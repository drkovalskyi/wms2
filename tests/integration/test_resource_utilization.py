"""Level 2 resource utilization tests — verify HTCondor actually uses requested CPUs.

Submits CPU-burning jobs to HTCondor and measures real CPU utilization
via /proc/stat sampling. This catches configuration issues where jobs
request N cores but only use 1.

Must run on a host where:
- HTCondor is running with available slots
- The 'wms2' user can submit jobs (root cannot submit directly)
"""

import os
import subprocess
import textwrap
import time
import uuid
from pathlib import Path

import pytest

pytestmark = [pytest.mark.level2]

SUBMIT_USER = "wms2"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_cpu_total():
    """Read total CPU jiffies from /proc/stat. Returns (busy, total)."""
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu "):
                parts = line.split()
                # user, nice, system, idle, iowait, irq, softirq, steal
                user, nice, system, idle, iowait, irq, softirq, steal = (
                    int(x) for x in parts[1:9]
                )
                busy = user + nice + system + irq + softirq + steal
                total = busy + idle + iowait
                return busy, total
    raise RuntimeError("Could not read /proc/stat")


def _measure_cpu_percent(duration_sec=5):
    """Measure system-wide CPU utilization over a window.

    Returns utilization as a percentage of total cores (e.g., 6400% = 64 cores busy).
    """
    b1, t1 = _read_cpu_total()
    time.sleep(duration_sec)
    b2, t2 = _read_cpu_total()
    delta_busy = b2 - b1
    delta_total = t2 - t1
    if delta_total == 0:
        return 0.0
    ncpus = os.cpu_count() or 1
    utilization = (delta_busy / delta_total) * ncpus * 100
    return utilization


def _run_as(user, cmd, cwd=None, timeout=30):
    """Run a command as a different user via su."""
    full_cmd = ["su", "-s", "/bin/bash", user, "-c", cmd]
    r = subprocess.run(full_cmd, capture_output=True, timeout=timeout, cwd=cwd)
    return r


def _submit_dag(dag_path: Path):
    """Submit a DAG file as wms2 user. Returns DAGMan cluster ID."""
    r = _run_as(
        SUBMIT_USER,
        f"condor_submit_dag {dag_path}",
        cwd=str(dag_path.parent),
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"condor_submit_dag failed (rc={r.returncode}): "
            f"{r.stderr.decode().strip()}"
        )
    # Parse "N job(s) submitted to cluster M." from condor_submit_dag output
    import re
    for line in r.stdout.decode().splitlines():
        m = re.search(r"submitted to cluster (\d+)", line)
        if m:
            return int(m.group(1))
    raise RuntimeError(f"Could not parse cluster ID from: {r.stdout.decode()}")


def _submit_job(sub_path: Path):
    """Submit a job file as wms2 user. Returns cluster ID."""
    r = _run_as(
        SUBMIT_USER,
        f"condor_submit {sub_path}",
        cwd=str(sub_path.parent),
    )
    if r.returncode != 0:
        raise RuntimeError(
            f"condor_submit failed (rc={r.returncode}): "
            f"{r.stderr.decode().strip()}"
        )
    # Parse "N job(s) submitted to cluster M."
    import re
    for line in r.stdout.decode().splitlines():
        m = re.search(r"submitted to cluster (\d+)", line)
        if m:
            return int(m.group(1))
    raise RuntimeError(f"Could not parse cluster ID from: {r.stdout.decode()}")


def _count_running_jobs(dagman_cluster=None, cluster_id=None):
    """Count running jobs via condor_q."""
    constraint = "JobStatus == 2"  # Running
    if dagman_cluster is not None:
        constraint += f" && DAGManJobId == {dagman_cluster}"
    elif cluster_id is not None:
        constraint += f" && ClusterId == {cluster_id}"
    r = subprocess.run(
        ["condor_q", "-constraint", constraint, "-af", "ClusterId"],
        capture_output=True, timeout=10,
    )
    if r.returncode != 0:
        return 0
    return len([l for l in r.stdout.decode().strip().splitlines() if l.strip()])


def _check_held_jobs(dagman_cluster=None, cluster_id=None):
    """Check for held jobs and return hold reason if any."""
    constraint = "JobStatus == 5"  # Held
    if dagman_cluster is not None:
        constraint += f" && DAGManJobId == {dagman_cluster}"
    elif cluster_id is not None:
        constraint += f" && ClusterId == {cluster_id}"
    r = subprocess.run(
        ["condor_q", "-constraint", constraint, "-af", "ClusterId", "HoldReason"],
        capture_output=True, timeout=10,
    )
    if r.returncode != 0:
        return None
    output = r.stdout.decode().strip()
    if output:
        return output
    return None


def _remove_jobs(cluster_id):
    """Remove a cluster and its children."""
    subprocess.run(
        ["condor_rm", str(cluster_id)],
        capture_output=True, timeout=10,
    )


def _count_all_jobs(dagman_cluster=None, cluster_id=None):
    """Count all jobs (any status) via condor_q."""
    constraint = "True"
    if dagman_cluster is not None:
        constraint = f"DAGManJobId == {dagman_cluster} || ClusterId == {dagman_cluster}"
    elif cluster_id is not None:
        constraint = f"ClusterId == {cluster_id}"
    r = subprocess.run(
        ["condor_q", "-constraint", constraint, "-af", "ClusterId", "JobStatus"],
        capture_output=True, timeout=10,
    )
    if r.returncode != 0:
        return 0
    return len([l for l in r.stdout.decode().strip().splitlines() if l.strip()])


def _wait_jobs_running(expected, dagman_cluster=None, cluster_id=None, timeout=120):
    """Wait until expected number of jobs are running. Fail fast on held jobs or DAG completion."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n = _count_running_jobs(dagman_cluster=dagman_cluster, cluster_id=cluster_id)
        if n >= expected:
            return n
        # Check for held jobs — fail fast
        held = _check_held_jobs(dagman_cluster=dagman_cluster, cluster_id=cluster_id)
        if held:
            raise RuntimeError(f"Jobs went to HOLD: {held}")
        # Check if DAGMan disappeared (completed or removed)
        total = _count_all_jobs(dagman_cluster=dagman_cluster, cluster_id=cluster_id)
        if total == 0 and (time.monotonic() - (deadline - timeout)) > 30:
            raise RuntimeError("All jobs disappeared from queue (DAG may have completed or failed)")
        time.sleep(2)
    return _count_running_jobs(dagman_cluster=dagman_cluster, cluster_id=cluster_id)


def _setup_work_dir(base_tmp: Path):
    """Create a work directory owned by the submit user.

    pytest tmp_path is under /tmp/pytest-of-root/ which other users can't
    traverse. Use /tmp directly and chown to SUBMIT_USER.
    """
    d = Path(f"/tmp/wms2_burn_{uuid.uuid4().hex[:8]}")
    d.mkdir()
    import pwd
    pw = pwd.getpwnam(SUBMIT_USER)
    os.chown(d, pw.pw_uid, pw.pw_gid)
    return d


def _cleanup_work_dir(work_dir: Path):
    """Remove a work directory and all contents."""
    import shutil
    try:
        shutil.rmtree(work_dir)
    except Exception:
        pass


def _write_burner_script(path: Path, ncpus: int, duration_sec: int):
    """Write a Python script that burns exactly ncpus cores for duration seconds."""
    script = textwrap.dedent(f"""\
        #!/usr/bin/env python3
        import multiprocessing, time, sys

        def burn(duration):
            end = time.monotonic() + duration
            while time.monotonic() < end:
                x = 0
                for _ in range(1_000_000):
                    x += 1

        if __name__ == "__main__":
            ncpus = {ncpus}
            duration = {duration_sec}
            procs = []
            for _ in range(ncpus):
                p = multiprocessing.Process(target=burn, args=(duration,))
                p.start()
                procs.append(p)
            for p in procs:
                p.join()
    """)
    path.write_text(script)
    path.chmod(0o755)
    _chown_file(path)


def _write_submit_file(path: Path, executable: str, ncpus: int):
    """Write an HTCondor submit file for the burner.

    Uses file transfer so the job doesn't depend on host paths.
    MOUNT_UNDER_SCRATCH = /tmp means jobs get a private /tmp,
    so we can't rely on host /tmp paths being visible inside jobs.
    """
    content = textwrap.dedent(f"""\
        universe = vanilla
        executable = {executable}
        request_cpus = {ncpus}
        request_memory = 100
        request_disk = 10000
        should_transfer_files = YES
        when_to_transfer_output = ON_EXIT
        output = burn_$(Cluster)_$(Process).out
        error = burn_$(Cluster)_$(Process).err
        log = burn.log
        queue 1
    """)
    path.write_text(content)
    _chown_file(path)


def _write_multi_submit_file(path: Path, executable: str, ncpus: int, njobs: int):
    """Write a submit file that queues njobs in a single cluster.

    All jobs enter the queue at once so HTCondor can match them
    in a single negotiation cycle.
    """
    content = textwrap.dedent(f"""\
        universe = vanilla
        executable = {executable}
        request_cpus = {ncpus}
        request_memory = 100
        request_disk = 10000
        should_transfer_files = YES
        when_to_transfer_output = ON_EXIT
        output = burn_$(Cluster)_$(Process).out
        error = burn_$(Cluster)_$(Process).err
        log = burn.log
        queue {njobs}
    """)
    path.write_text(content)
    _chown_file(path)


def _write_dag_file(path: Path, submit_file: str, njobs: int):
    """Write a DAG file that runs njobs independent burn jobs."""
    lines = []
    for i in range(njobs):
        lines.append(f"JOB burn_{i:03d} {submit_file}")
    path.write_text("\n".join(lines) + "\n")
    _chown_file(path)


def _chown_file(path: Path):
    """Change file ownership to SUBMIT_USER."""
    import pwd
    pw = pwd.getpwnam(SUBMIT_USER)
    os.chown(path, pw.pw_uid, pw.pw_gid)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCPUUtilization:
    """Verify HTCondor jobs actually use the CPUs they request."""

    def test_single_job_multicore(self, tmp_path):
        """One 8-core job should produce ~800% CPU utilization."""
        ncpus = 8
        burn_duration = 40

        work_dir = _setup_work_dir(tmp_path)
        burner = work_dir / "burn.py"
        _write_burner_script(burner, ncpus=ncpus, duration_sec=burn_duration)
        sub_file = work_dir / "burn.sub"
        _write_submit_file(sub_file, str(burner), ncpus=ncpus)

        cluster_id = _submit_job(sub_file)

        try:
            running = _wait_jobs_running(1, cluster_id=cluster_id, timeout=60)
            assert running >= 1, f"Job {cluster_id} did not start within 60s"

            # Let the job warm up
            time.sleep(5)

            # Measure CPU utilization
            utilization = _measure_cpu_percent(duration_sec=10)

            # Expect at least 75% efficiency on requested cores
            min_expected = ncpus * 75  # 600%
            assert utilization >= min_expected, (
                f"Expected >= {min_expected}% CPU ({ncpus} cores × 75%), "
                f"measured {utilization:.0f}%"
            )
            print(f"\n  Single {ncpus}-core job: {utilization:.0f}% CPU "
                  f"({utilization / ncpus:.0f}% efficiency per core)")
        finally:
            _remove_jobs(cluster_id)
            _cleanup_work_dir(work_dir)

    def test_multi_job_full_pool(self, tmp_path):
        """8 jobs × 8 cores = 64 cores should produce ~6400% CPU utilization."""
        njobs = 8
        ncpus = 8
        total_cpus = njobs * ncpus
        burn_duration = 120  # long enough for all jobs to overlap

        work_dir = _setup_work_dir(tmp_path)
        burner = work_dir / "burn.py"
        _write_burner_script(burner, ncpus=ncpus, duration_sec=burn_duration)

        # Submit njobs as "queue N" in a single cluster — all enter the
        # queue at once so HTCondor can schedule them in one negotiation cycle
        sub_file = work_dir / "burn.sub"
        _write_multi_submit_file(sub_file, str(burner), ncpus=ncpus, njobs=njobs)

        cluster_id = _submit_job(sub_file)

        try:
            running = _wait_jobs_running(
                njobs, cluster_id=cluster_id, timeout=120,
            )
            assert running >= njobs, (
                f"Only {running}/{njobs} jobs running after 120s"
            )

            # Let all jobs warm up
            time.sleep(5)

            # Measure CPU utilization over 10 seconds
            utilization = _measure_cpu_percent(duration_sec=10)

            min_expected = total_cpus * 75  # 4800%
            assert utilization >= min_expected, (
                f"Expected >= {min_expected}% CPU ({total_cpus} cores × 75%), "
                f"measured {utilization:.0f}%"
            )

            efficiency = utilization / (total_cpus * 100) * 100
            print(f"\n  {njobs} jobs × {ncpus} cores = {total_cpus} total")
            print(f"  Measured: {utilization:.0f}% CPU")
            print(f"  Efficiency: {efficiency:.1f}%")
        finally:
            _remove_jobs(cluster_id)
            time.sleep(3)
            _cleanup_work_dir(work_dir)
