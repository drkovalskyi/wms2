#!/usr/bin/env python3
"""E2E real CMSSW test via HTCondor DAGMan.

Fetches a real ReqMgr2 request, builds a sandbox with ConfigCache PSets,
plans a production DAG, and submits to HTCondor DAGMan.

Must run as the wms2 Linux user (HTCondor forbids root).

Usage:
    su - wms2 -c "/mnt/shared/work/wms2/.venv/bin/python \
        /mnt/shared/work/wms2/scripts/e2e_real_condor.py \
        --request <reqmgr2_request_name> --events 1000"
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import ssl
import shutil
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Ensure project is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wms2.adapters.condor import HTCondorAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner, PilotMetrics

# ── CPU Monitoring ─────────────────────────────────────────────

def read_cpu_jiffies():
    """Read total CPU jiffies from /proc/stat. Returns (busy, total)."""
    with open("/proc/stat") as f:
        for line in f:
            if line.startswith("cpu "):
                parts = line.split()
                user, nice, system, idle, iowait, irq, softirq, steal = (
                    int(x) for x in parts[1:9]
                )
                busy = user + nice + system + irq + softirq + steal
                total = busy + idle + iowait
                return busy, total
    return 0, 1


def cpu_utilization_percent(prev_busy, prev_total, cur_busy, cur_total):
    """Compute CPU utilization as % of all cores between two /proc/stat samples."""
    db = cur_busy - prev_busy
    dt = cur_total - prev_total
    if dt == 0:
        return 0.0
    ncpus = os.cpu_count() or 1
    return (db / dt) * ncpus * 100


# ── Configuration ──────────────────────────────────────────────

CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
POLL_INTERVAL = 10
MAX_WAIT = 7200  # 2 hours
X509_PROXY = os.environ.get("X509_USER_PROXY", "/mnt/creds/x509up")

REQMGR2_URL = "https://cmsweb.cern.ch/reqmgr2/data/request"
COUCHDB_URL = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache"


def make_ssl_context() -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(X509_PROXY, X509_PROXY)
    ca_dir = os.environ.get(
        "X509_CERT_DIR",
        "/cvmfs/grid.cern.ch/etc/grid-security/certificates",
    )
    if os.path.isdir(ca_dir):
        ctx.load_verify_locations(capath=ca_dir)
    else:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch_json(url: str) -> dict:
    ctx = make_ssl_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fetch_text(url: str) -> str:
    ctx = make_ssl_context()
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return resp.read().decode()


def fetch_request(request_name: str) -> dict:
    data = fetch_json(f"{REQMGR2_URL}/{request_name}")
    return list(data["result"][0].values())[0]


def fetch_pset(config_cache_id: str) -> str:
    return fetch_text(f"{COUCHDB_URL}/{config_cache_id}/configFile")


def _extract_pset_tier(pset_content: str, step_num: int) -> str:
    """Extract output data tier from a PSet by parsing the OutputModule name.

    CMSSW PSets define output modules like:
        process.MINIAODSIMoutput = cms.OutputModule("PoolOutputModule", ...)
    The tier is the module name with 'output' stripped: MINIAODSIM.
    For StepChain outputs, may include a numeric suffix: MINIAODSIM1.
    """
    tiers = []
    for line in pset_content.splitlines():
        m = re.search(r"process\.(\w+)output\s*=\s*cms\.OutputModule", line)
        if m:
            tiers.append(m.group(1))
    # If multiple output modules, prefer the non-LHE, non-RAWSIM one
    # (those are intermediate tiers, the kept output is usually the last/main one)
    for t in reversed(tiers):
        if t.upper() not in ("LHE", "RAWSIM", "PREMIXRAW"):
            return t
    return tiers[0] if tiers else ""


def build_sandbox(test_dir: Path, request: dict) -> tuple[str, dict[int, str]]:
    """Build sandbox with real ConfigCache PSets.

    Returns (sandbox_path, step_tiers) where step_tiers maps step number
    to the actual CMSSW data tier extracted from the PSet.
    """
    import tarfile
    import tempfile

    num_steps = int(request.get("StepChain", 1))

    # Build manifest
    steps = []
    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        arch = step.get("ScramArch") or request.get("ScramArch", "")
        if isinstance(arch, list):
            arch = arch[0]
        steps.append({
            "name": step.get("StepName", f"step{i}"),
            "cmssw_version": step.get("CMSSWVersion") or request.get("CMSSWVersion", ""),
            "scram_arch": arch,
            "global_tag": step.get("GlobalTag") or request.get("GlobalTag", ""),
            "pset": f"steps/step{i}/cfg.py",
            "multicore": int(step.get("Multicore") or request.get("Multicore", 1)),
            "memory_mb": int(step.get("Memory") or request.get("Memory", 2048)),
            "keep_output": step.get("KeepOutput", True),
            "input_step": step.get("InputStep", ""),
            "input_from_output_module": step.get("InputFromOutputModule", ""),
            "mc_pileup": step.get("MCPileup", ""),
            "data_pileup": step.get("DataPileup", ""),
            "event_streams": int(step.get("EventStreams") or request.get("EventStreams", 0)),
            "request_num_events": int(step.get("RequestNumEvents") or 0),
        })

    manifest = {"mode": "cmssw", "request_name": request.get("RequestName", ""), "steps": steps}

    # Fetch PSets and extract output tiers
    psets: dict[int, str] = {}
    step_tiers: dict[int, str] = {}
    for i in range(1, num_steps + 1):
        ccid = request[f"Step{i}"].get("ConfigCacheID", "")
        if ccid:
            psets[i] = fetch_pset(ccid)
            tier = _extract_pset_tier(psets[i], i)
            if tier:
                step_tiers[i] = tier
            print(f"    Step{i} PSet: {len(psets[i])} chars, tier={tier or '(none)'}")
        else:
            psets[i] = f"# Placeholder for step {i}\n"

    # Build tar.gz
    sandbox_path = str(test_dir / "sandbox.tar.gz")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))
        for step_idx, content in psets.items():
            pdir = tmp / "steps" / f"step{step_idx}"
            pdir.mkdir(parents=True, exist_ok=True)
            (pdir / "cfg.py").write_text(content)

        with tarfile.open(sandbox_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                tar.add(str(item), arcname=str(item.relative_to(tmp)))

    print(f"    Sandbox: {sandbox_path} ({os.path.getsize(sandbox_path):,} bytes)")
    return sandbox_path, step_tiers


def make_workflow(request: dict, sandbox_path: str, events_per_job: int,
                  num_jobs: int = 1,
                  step_tiers: dict[int, str] | None = None) -> MagicMock:
    """Create a mock workflow from the real request."""
    total_events = events_per_job * num_jobs
    st = step_tiers or {}
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = request.get("RequestName", "real-test")
    wf.input_dataset = ""  # GEN has no input dataset
    wf.splitting_algo = "EventBased"
    wf.splitting_params = {"events_per_job": events_per_job}
    wf.sandbox_url = sandbox_path
    wf.category_throttles = {"Processing": 100, "Merge": 10, "Cleanup": 10}

    multicore = int(request.get("Multicore", 8))
    memory_mb = int(request.get("Memory", 16000))

    # Find the kept output datasets — use actual DataTier from PSet or request
    output_datasets = []
    num_steps = int(request.get("StepChain", 1))
    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        if step.get("KeepOutput", True):
            prim = step.get("PrimaryDataset", "TestPrimary")
            acq = step.get("AcquisitionEra", "Test")
            proc = step.get("ProcessingString", "v1")
            ver = step.get("ProcessingVersion", 1)
            tier = step.get("DataTier") or st.get(i, f"STEP{i}")
            output_datasets.append({
                "dataset_name": f"/{prim}/{acq}-{proc}-v{ver}/{tier}",
                "merged_lfn_base": f"/store/mc/{acq}/{prim}/{tier}/{proc}-v{ver}",
                "data_tier": tier,
            })

    wf.config_data = {
        "sandbox_path": sandbox_path,
        "memory_mb": memory_mb,
        "multicore": multicore,
        "time_per_event": float(request.get("TimePerEvent", 1.0)),
        "size_per_event": float(request.get("SizePerEvent", 50.0)),
        "request_num_events": total_events,
        "_is_gen": True,  # tells planner to use GEN node planning
        "output_datasets": output_datasets,
    }
    return wf


def make_mock_repo():
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


async def run_test(request_name: str, events: int, num_jobs: int = 1):
    total_events = events * num_jobs
    print("=" * 70)
    print(f"WMS2 Real CMSSW HTCondor DAG Test")
    print(f"  Request: {request_name}")
    print(f"  Events:  {events}/job x {num_jobs} jobs = {total_events} total")
    print(f"  Condor:  {CONDOR_HOST}")
    print("=" * 70)

    test_dir = Path("/mnt/shared/work/wms2_real_condor_test")
    if test_dir.exists():
        for item in test_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    else:
        test_dir.mkdir(parents=True)

    output_dir = test_dir / "output"
    output_dir.mkdir()

    # 1. Fetch request
    print(f"\n[1/6] Fetching request from ReqMgr2...")
    request = fetch_request(request_name)
    num_steps = int(request.get("StepChain", 1))
    multicore = int(request.get("Multicore", 8))
    memory_mb = int(request.get("Memory", 16000))
    print(f"    {num_steps} steps, {multicore} cores, {memory_mb} MB")

    # 2. Build sandbox
    print(f"[2/6] Building sandbox with {num_steps} ConfigCache PSets...")
    sandbox_path, step_tiers = build_sandbox(test_dir, request)

    # 3. Connect to HTCondor
    print(f"[3/6] Connecting to HTCondor...")
    try:
        condor = HTCondorAdapter(CONDOR_HOST)
        print(f"    Connected to schedd")
    except Exception as e:
        print(f"    ERROR: {e}")
        return False

    submit_dir = str(test_dir / "submit")
    settings = Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        submit_base_dir=submit_dir,
        jobs_per_work_unit=max(num_jobs, 8),
        processing_executable="/bin/true",
        merge_executable="/bin/true",
        cleanup_executable="/bin/true",
        output_base_dir=str(output_dir),
        max_input_files=num_jobs,
    )

    repo = make_mock_repo()
    planner = DAGPlanner(
        repository=repo,
        dbs_adapter=MagicMock(),
        rucio_adapter=MagicMock(),
        condor_adapter=condor,
        settings=settings,
    )

    # 4. Plan and submit DAG
    print(f"[4/6] Planning and submitting production DAG...")
    wf = make_workflow(request, sandbox_path, events, num_jobs, step_tiers)
    metrics = PilotMetrics.from_request(wf.config_data)

    try:
        dag = await planner.plan_production_dag(wf, metrics=metrics)
    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Extract cluster info
    cluster_id = None
    schedd_name = None
    for call in repo.update_dag.call_args_list:
        kw = call[1]
        if "dagman_cluster_id" in kw:
            cluster_id = kw["dagman_cluster_id"]
            schedd_name = kw["schedd_name"]

    if not cluster_id:
        print("    ERROR: No cluster ID")
        return False

    wf_dir = Path(submit_dir) / str(wf.id)
    print(f"    Workflow ID: {wf.id}")
    print(f"    Cluster ID:  {cluster_id}")
    print(f"    Submit dir:  {wf_dir}")

    group_dirs = sorted(wf_dir.glob("mg_*"))
    print(f"    Merge groups: {len(group_dirs)}")
    for gd in group_dirs:
        proc_subs = list(gd.glob("proc_*.sub"))
        print(f"      {gd.name}: {len(proc_subs)} proc nodes")

    # 5. Poll with CPU monitoring
    print(f"\n[5/7] Polling DAGMan (max {MAX_WAIT}s)...")
    start_time = time.time()
    final_status = None
    cpu_samples = []  # list of (elapsed_sec, cpu_percent)
    prev_busy, prev_total = read_cpu_jiffies()

    while time.time() - start_time < MAX_WAIT:
        try:
            info = await condor.query_job(schedd_name, cluster_id)
        except Exception as e:
            print(f"    Query error: {e}")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        if info is None:
            completed = await condor.check_job_completed(cluster_id, schedd_name)
            final_status = "completed" if completed else "vanished"
            break

        job_status = int(info.get("JobStatus", 0))
        nodes_total = int(info.get("DAG_NodesTotal", 0))
        nodes_done = int(info.get("DAG_NodesDone", 0))
        nodes_failed = int(info.get("DAG_NodesFailed", 0))
        nodes_queued = int(info.get("DAG_NodesQueued", 0))
        nodes_ready = int(info.get("DAG_NodesReady", 0))

        # Sample CPU
        cur_busy, cur_total = read_cpu_jiffies()
        cpu_pct = cpu_utilization_percent(prev_busy, prev_total, cur_busy, cur_total)
        prev_busy, prev_total = cur_busy, cur_total

        elapsed = int(time.time() - start_time)
        cpu_samples.append((elapsed, cpu_pct))

        status_str = {1: "idle", 2: "running", 3: "removed", 4: "completed", 5: "held"}
        print(f"    [{elapsed:4d}s] status={status_str.get(job_status, job_status)} "
              f"nodes: {nodes_done}/{nodes_total} done, "
              f"{nodes_queued} queued, {nodes_ready} ready, "
              f"{nodes_failed} failed  "
              f"cpu: {cpu_pct:.0f}%")

        if job_status in (3, 4):
            final_status = "completed" if job_status == 4 else "removed"
            break

        await asyncio.sleep(POLL_INTERVAL)
    else:
        final_status = "timeout"

    elapsed_total = int(time.time() - start_time)
    print(f"\n    DAG finished: {final_status} in {elapsed_total}s ({elapsed_total/60:.1f} min)")

    # CPU utilization summary
    if cpu_samples:
        # Skip first sample (includes DAGMan startup overhead)
        active_samples = [pct for _, pct in cpu_samples[1:] if pct > 100]
        if active_samples:
            avg_cpu = sum(active_samples) / len(active_samples)
            peak_cpu = max(active_samples)
            expected_cpu = num_jobs * multicore * 100
            efficiency = avg_cpu / expected_cpu * 100 if expected_cpu else 0
            print(f"\n    CPU utilization (during active processing):")
            print(f"      Average: {avg_cpu:.0f}% ({avg_cpu/100:.1f} cores)")
            print(f"      Peak:    {peak_cpu:.0f}% ({peak_cpu/100:.1f} cores)")
            print(f"      Expected: {expected_cpu}% ({num_jobs}×{multicore} = {num_jobs*multicore} cores)")
            print(f"      Efficiency: {efficiency:.1f}%")

    # 6. Per-step efficiency
    print(f"\n[6/7] Per-step timing...")
    step_times: dict[str, list[float]] = {}  # step_name -> [wall_sec, ...]
    job_wall_times: list[float] = []

    for gd in group_dirs:
        for out_file in sorted(gd.glob("proc_*.out")):
            content = out_file.read_text()
            # Parse "  Step <name> completed in <N>s"
            for m in re.finditer(r"Step (\S+) completed in (\d+)s", content):
                name, secs = m.group(1), float(m.group(2))
                step_times.setdefault(name, []).append(secs)
            # Parse "wall_time_sec: <N>"
            m = re.search(r"wall_time_sec:\s*(\d+)", content)
            if m:
                job_wall_times.append(float(m.group(1)))

    if step_times:
        print(f"\n    {'Step':<25s} {'Avg':>6s} {'Min':>6s} {'Max':>6s}  (seconds, across {num_jobs} jobs)")
        print(f"    {'-'*25} {'-'*6} {'-'*6} {'-'*6}")
        for step_name, times in step_times.items():
            avg = sum(times) / len(times)
            print(f"    {step_name:<25s} {avg:6.0f} {min(times):6.0f} {max(times):6.0f}")
        if job_wall_times:
            avg_wall = sum(job_wall_times) / len(job_wall_times)
            step_total = sum(sum(t) / len(t) for t in step_times.values())
            overhead = avg_wall - step_total
            print(f"    {'--- Total cmsRun ---':<25s} {step_total:6.0f}")
            print(f"    {'--- Job wall clock ---':<25s} {avg_wall:6.0f}")
            print(f"    {'--- Overhead (setup) ---':<25s} {overhead:6.0f}")

    # 7. Check results
    print(f"\n[7/7] Checking results...")
    success = True

    for gd in group_dirs:
        proc_outs = sorted(gd.glob("proc_*.out"))
        proc_errs = sorted(gd.glob("proc_*.err"))
        print(f"    {gd.name}: {len(proc_outs)} .out, {len(proc_errs)} .err")

        # Show errors
        for err_file in proc_errs:
            content = err_file.read_text().strip()
            if content:
                lines = content.split("\n")
                # Only show lines with ERROR or last 3 lines
                error_lines = [l for l in lines if "ERROR" in l.upper()]
                if error_lines:
                    print(f"      {err_file.name} errors:")
                    for line in error_lines[:3]:
                        print(f"        {line}")

        # Check FJR files
        fjr_files = sorted(gd.glob("report_step*.xml"))
        if fjr_files:
            print(f"      FJR files: {[f.name for f in fjr_files]}")

        # Check .root output files
        root_files = sorted(gd.glob("*.root"))
        if root_files:
            total_root_bytes = sum(rf.stat().st_size for rf in root_files)
            print(f"      Output: {len(root_files)} .root files, "
                  f"{total_root_bytes / 1048576:.1f} MB total")

        # Merge output
        merge_out = gd / "merge.out"
        if merge_out.exists():
            content = merge_out.read_text().strip()
            if content:
                for line in content.split("\n")[-3:]:
                    print(f"      merge: {line}")

    # Check output directory
    merged_files = [f for f in output_dir.rglob("*") if f.is_file()]
    if merged_files:
        total_merged = sum(f.stat().st_size for f in merged_files)
        print(f"\n    Merged output: {len(merged_files)} files, "
              f"{total_merged / 1048576:.1f} MB in {output_dir}")

    rescue_dags = list(wf_dir.glob("workflow.dag.rescue*"))
    if rescue_dags:
        print(f"\n    WARNING: Rescue DAG(s): {[r.name for r in rescue_dags]}")
        success = False

    print(f"\n{'='*70}")
    if final_status == "completed" and success:
        print(f"RESULT: PASS — {elapsed_total}s ({elapsed_total/60:.1f} min)")
    elif final_status == "timeout":
        print(f"RESULT: TIMEOUT after {MAX_WAIT}s")
        try:
            await condor.remove_job(schedd_name, cluster_id)
        except Exception:
            pass
        success = False
    else:
        print(f"RESULT: {final_status.upper()}")
        success = False
    print("=" * 70)

    return success


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--events", type=int, default=1000,
                        help="Events per job (each job processes this many)")
    parser.add_argument("--jobs", type=int, default=1,
                        help="Number of parallel proc jobs (work unit size)")
    args = parser.parse_args()

    result = asyncio.run(run_test(args.request, args.events, args.jobs))
    sys.exit(0 if result else 1)
