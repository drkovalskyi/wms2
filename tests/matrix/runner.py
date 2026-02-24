"""MatrixRunner — generic execution engine for the test matrix.

Reuses the same code path as e2e_real_condor.py:
  - wms2.core.sandbox.create_sandbox() for sandbox building
  - wms2.core.dag_planner.DAGPlanner.plan_production_dag() for DAG + submit
  - wms2.adapters.condor.HTCondorAdapter for real HTCondor ops
  - wms2.core.dag_planner.PilotMetrics.from_request() for resource estimates
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import shutil
import subprocess
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from tests.matrix.definitions import WorkflowDef
from tests.matrix.environment import detect_capabilities, workflow_can_run
from tests.matrix.faults import inject_faults
from tests.matrix.sweeper import remove_dagman_jobs, sweep_post, sweep_pre

logger = logging.getLogger(__name__)

CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
POLL_INTERVAL = 5
RESULTS_DIR = Path("/mnt/shared/work/wms2_matrix/results")

# Disk space requirements per concurrent CMSSW job (GB).
# Measured from DY2L 5-step StepChain: sandbox extraction ~1 MB,
# CMSSW project area ~100 MB, MadGraph working dir ~500 MB,
# intermediate ROOT files across 5 steps ~300 MB, misc ~100 MB.
# Total ~1 GB; use 2 GB with safety margin.
DISK_GB_PER_JOB_EXECUTE = 2
# Per job output transferred back: ~300 MB ROOT + FJR + metrics.
DISK_GB_PER_JOB_OUTPUT = 0.5
# Minimum free space on /tmp for apptainer and system overhead.
DISK_GB_TMP_MIN = 1


def _check_disk_space(num_concurrent_jobs: int, output_dir: Path) -> None:
    """Pre-flight check: verify sufficient free disk space.

    Raises RuntimeError if any filesystem has insufficient space.
    """
    # Resolve HTCondor EXECUTE directory
    try:
        result = subprocess.run(
            ["condor_config_val", "EXECUTE"],
            capture_output=True, text=True, timeout=5,
        )
        execute_dir = Path(result.stdout.strip()) if result.returncode == 0 else Path("/var/lib/condor/execute")
    except Exception:
        execute_dir = Path("/var/lib/condor/execute")

    checks: list[tuple[str, Path, float]] = [
        ("EXECUTE", execute_dir, num_concurrent_jobs * DISK_GB_PER_JOB_EXECUTE),
        ("output", output_dir, num_concurrent_jobs * DISK_GB_PER_JOB_OUTPUT),
        ("/tmp", Path("/tmp"), DISK_GB_TMP_MIN),
    ]

    for label, path, required_gb in checks:
        try:
            stat = shutil.disk_usage(path)
        except OSError:
            stat = shutil.disk_usage(path.parent)
        free_gb = stat.free / (1024 ** 3)
        if free_gb < required_gb:
            raise RuntimeError(
                f"Insufficient disk space on {label} ({path}): "
                f"{free_gb:.1f} GB free, need {required_gb:.1f} GB "
                f"for {num_concurrent_jobs} concurrent jobs"
            )
        logger.info(
            "Disk check %s (%s): %.1f GB free, need %.1f GB — OK",
            label, path, free_gb, required_gb,
        )


# ── CPU monitoring ───────────────────────────────────────────

# HTCondor cgroup v2 CPU accounting — tracks only HTCondor job CPU,
# not system-wide utilization.  Safe to use with concurrent tests.
_HTCONDOR_CGROUP_CPU = "/sys/fs/cgroup/system.slice/htcondor/cpu.stat"


def _read_cgroup_cpu_usec() -> int:
    """Read usage_usec from HTCondor parent cgroup.

    Returns total CPU microseconds consumed by all HTCondor jobs.
    Falls back to 0 if cgroup file is unavailable.
    """
    try:
        with open(_HTCONDOR_CGROUP_CPU) as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return 0


def _read_cpu_jiffies() -> tuple[int, int]:
    """Read total CPU jiffies from /proc/stat.  Returns (busy, total).

    Legacy fallback — used only when cgroup accounting is unavailable.
    """
    try:
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
    except OSError:
        pass
    return 0, 1


def _cpu_percent(prev_busy: int, prev_total: int, cur_busy: int, cur_total: int) -> float:
    db = cur_busy - prev_busy
    dt = cur_total - prev_total
    if dt == 0:
        return 0.0
    ncpus = os.cpu_count() or 1
    return (db / dt) * ncpus * 100


def _cgroup_cpu_cores(prev_usec: int, cur_usec: int, interval_sec: float) -> float:
    """Convert cgroup CPU delta to cores (100% = 1 core, in % units)."""
    if interval_sec <= 0:
        return 0.0
    delta_usec = cur_usec - prev_usec
    # usec -> sec, then convert to % (1 core = 100%)
    return (delta_usec / 1_000_000) / interval_sec * 100


# ── Result tracking ──────────────────────────────────────────


@dataclass
class StepPerf:
    """Per-step performance aggregated across all jobs."""
    step_name: str
    n_jobs: int = 0           # How many jobs contributed data
    wall_sec_mean: float = 0.0
    wall_sec_stddev: float = 0.0
    wall_sec_min: float = 0.0
    wall_sec_max: float = 0.0
    cpu_eff_mean: float = 0.0   # 0–1 scale
    cpu_eff_stddev: float = 0.0
    rss_mb_mean: float = 0.0
    rss_mb_stddev: float = 0.0
    rss_mb_max: float = 0.0
    events_total: int = 0
    throughput_ev_s: float = 0.0

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, d: dict) -> StepPerf:
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class PerfData:
    """Aggregate performance metrics for a workflow run."""
    num_proc_jobs: int = 0
    steps: list[StepPerf] = field(default_factory=list)
    # CPU utilization (% of all cores, sampled during polling)
    cpu_avg_pct: float = 0.0
    cpu_peak_pct: float = 0.0
    cpu_expected_pct: float = 0.0
    # Output
    merged_files: int = 0
    merged_bytes: int = 0
    unmerged_cleaned: bool = False
    # Time breakdown (seconds) from DAGMan log
    time_processing: float = 0.0   # first proc EXECUTE to last proc TERMINATED
    time_merge: float = 0.0        # merge EXECUTE to merge TERMINATED
    time_cleanup: float = 0.0      # cleanup EXECUTE to cleanup TERMINATED
    time_overhead: float = 0.0     # gaps between phases (scheduling, transfer)
    # Raw per-job per-step data for re-analysis
    raw_job_step_data: dict = field(default_factory=dict)
    cpu_samples: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "steps"}
        d["steps"] = [s.to_dict() for s in self.steps]
        return d

    @classmethod
    def from_dict(cls, d: dict) -> PerfData:
        steps_data = d.pop("steps", [])
        obj = cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
        obj.steps = [StepPerf.from_dict(s) for s in steps_data]
        return obj


@dataclass
class WorkflowResult:
    wf_id: float
    title: str
    status: str = "pending"  # pending | running | passed | failed | skipped | timeout | error
    reason: str = ""
    elapsed_sec: float = 0.0
    dag_status: str = ""
    rescue_dags: int = 0
    cluster_id: str = ""
    submit_dir: str = ""
    perf: PerfData | None = None

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != "perf"}
        d["perf"] = self.perf.to_dict() if self.perf else None
        return d

    @classmethod
    def from_dict(cls, d: dict) -> WorkflowResult:
        perf_data = d.pop("perf", None)
        obj = cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})
        obj.perf = PerfData.from_dict(perf_data) if perf_data else None
        return obj


# ── Helpers ──────────────────────────────────────────────────


def _make_workflow_mock(wf: WorkflowDef, sandbox_path: str) -> MagicMock:
    """Build the mock Workflow object that DAGPlanner expects."""
    total_events = wf.events_per_job * wf.num_jobs
    mock = MagicMock()
    mock.id = uuid.uuid4()
    mock.request_name = wf.request_spec.get("RequestName", f"matrix_{wf.wf_id}")
    mock.input_dataset = ""
    mock.splitting_algo = "EventBased"
    mock.splitting_params = {"events_per_job": wf.events_per_job}
    mock.sandbox_url = sandbox_path
    mock.category_throttles = {"Processing": 100, "Merge": 10, "Cleanup": 10}
    mock.config_data = {
        "sandbox_path": sandbox_path,
        "memory_mb": wf.memory_mb,
        "multicore": wf.multicore,
        "time_per_event": float(wf.request_spec.get("TimePerEvent", 0.01)),
        "size_per_event": float(wf.request_spec.get("SizePerEvent", 1.0)),
        "request_num_events": total_events,
        "_is_gen": True,
        "output_datasets": wf.output_datasets,
    }
    return mock


def _make_mock_repo() -> MagicMock:
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


def _extract_cluster_info(repo_mock: MagicMock) -> tuple[str | None, str | None]:
    """Pull cluster_id and schedd_name from update_dag calls."""
    for call in repo_mock.update_dag.call_args_list:
        kw = call[1]
        if "dagman_cluster_id" in kw:
            return kw["dagman_cluster_id"], kw["schedd_name"]
    return None, None


# ── Verification ─────────────────────────────────────────────


def _verify(wf: WorkflowDef, result: WorkflowResult, wf_dir: Path) -> None:
    """Check actual outcomes against VerifySpec.  Mutates *result*."""
    v = wf.verify
    errors: list[str] = []

    # Rescue DAGs — check first since they determine actual success
    rescue_files = list(wf_dir.glob("**/workflow.dag.rescue*"))
    # Also check sub-DAG rescue files
    rescue_files.extend(wf_dir.glob("**/group.dag.rescue*"))
    result.rescue_dags = len(rescue_files)

    # DAGMan always exits with status=4 ("completed") even when nodes fail;
    # actual success = completed + no rescue DAGs anywhere.
    dag_finished = result.dag_status == "completed"
    actually_succeeded = dag_finished and result.rescue_dags == 0

    if v.expect_success and not actually_succeeded:
        if result.dag_status == "timeout":
            errors.append("DAG timed out")
        elif rescue_files:
            errors.append(
                f"DAG completed but {result.rescue_dags} rescue DAG(s) generated"
            )
        else:
            errors.append(f"expected success but dag_status={result.dag_status}")
    if not v.expect_success and actually_succeeded:
        errors.append("expected failure but DAG completed with all nodes successful")
    if v.expect_rescue_dag and not rescue_files:
        errors.append("expected rescue DAG but none found")
    if not v.expect_rescue_dag and rescue_files:
        errors.append(f"unexpected rescue DAG(s): {[r.name for r in rescue_files]}")

    # Merged outputs
    if v.expect_merged_outputs:
        has_merged = False
        for ds in wf.output_datasets:
            lfn_base = ds.get("merged_lfn_base", "")
            if lfn_base:
                pfn = Path("/mnt/shared") / lfn_base.lstrip("/")
                if pfn.exists() and any(pfn.rglob("*")):
                    has_merged = True
        if not has_merged and wf.output_datasets:
            errors.append("expected merged outputs but none found")

    # Cleanup
    if v.expect_cleanup_ran:
        for ds in wf.output_datasets:
            lfn_base = ds.get("unmerged_lfn_base", "")
            if lfn_base:
                pfn = Path("/mnt/shared") / lfn_base.lstrip("/")
                if pfn.exists() and list(pfn.rglob("*")):
                    errors.append("expected cleanup but unmerged files still exist")
                    break

    if errors:
        result.status = "failed"
        result.reason = "; ".join(errors)
    else:
        result.status = "passed"


# ── Performance collection ───────────────────────────────────


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    return math.sqrt(sum((x - m) ** 2 for x in values) / (len(values) - 1))


def _parse_fjr_xml(fjr_path: Path) -> dict | None:
    """Parse a single FJR XML file, returning key metrics."""
    try:
        tree = ET.parse(fjr_path)
        root = tree.getroot()
    except Exception:
        return None

    def get_metric(parent_name: str, metric_name: str) -> float | None:
        for ps in root.findall(".//PerformanceReport/PerformanceSummary"):
            if ps.get("Metric") == parent_name:
                for m in ps.findall("Metric"):
                    if m.get("Name") == metric_name:
                        try:
                            return float(m.get("Value", 0))
                        except (ValueError, TypeError):
                            return None
        return None

    wall = get_metric("Timing", "TotalJobTime")
    cpu = get_metric("Timing", "TotalJobCPU")
    nthreads = get_metric("Timing", "NumberOfThreads")
    throughput = get_metric("Timing", "EventThroughput")
    rss = get_metric("ApplicationMemory", "PeakValueRss")

    # Events: prefer NumberEvents from PerformanceSummary (physics events only),
    # not InputFile/EventsRead which overcounts for pileup steps.
    nevents = 0
    n = get_metric("ProcessingSummary", "NumberEvents")
    if n:
        nevents = int(n)
    if nevents == 0:
        for inp in root.findall(".//InputFile"):
            ev = inp.findtext("EventsRead")
            if ev:
                try:
                    nevents += int(ev)
                except ValueError:
                    pass

    cpu_eff = 0.0
    if wall and cpu and nthreads and wall > 0 and nthreads > 0:
        cpu_eff = cpu / (wall * nthreads)

    return {
        "wall": wall or 0.0,
        "cpu_eff": cpu_eff,
        "rss": rss or 0.0,
        "events": nevents,
        "throughput": throughput or 0.0,
    }


def _aggregate_step(
    step_name: str, samples: list[dict], events_per_job: int,
) -> StepPerf:
    """Aggregate per-job samples into a single StepPerf."""
    walls = [s["wall"] for s in samples if s["wall"] > 0]
    effs = [s["cpu_eff"] for s in samples if s["cpu_eff"] > 0]
    rsss = [s["rss"] for s in samples if s["rss"] > 0]
    tputs = [s["throughput"] for s in samples if s.get("throughput", 0) > 0]
    # Cap per-sample events to events_per_job (pileup steps overcount via InputFile)
    evts = sum(min(s.get("events", 0), events_per_job) for s in samples)
    if evts == 0:
        evts = events_per_job * len(samples)

    return StepPerf(
        step_name=step_name,
        n_jobs=len(samples),
        wall_sec_mean=_mean(walls),
        wall_sec_stddev=_stddev(walls),
        wall_sec_min=min(walls) if walls else 0,
        wall_sec_max=max(walls) if walls else 0,
        cpu_eff_mean=_mean(effs),
        cpu_eff_stddev=_stddev(effs),
        rss_mb_mean=_mean(rsss),
        rss_mb_stddev=_stddev(rsss),
        rss_mb_max=max(rsss) if rsss else 0,
        events_total=evts,
        throughput_ev_s=_mean(tputs),
    )


def _parse_dagman_times(wf_dir: Path, perf: PerfData) -> None:
    """Parse DAGMan log to get per-phase wall time breakdown."""
    from datetime import datetime

    # Find group.dag.dagman.out in any merge group
    for gd in sorted(wf_dir.glob("mg_*")):
        dagman_out = gd / "group.dag.dagman.out"
        if not dagman_out.exists():
            continue
        try:
            content = dagman_out.read_text()
        except OSError:
            continue

        # Extract EXECUTE/TERMINATED timestamps per node type
        # Format: "02/20/26 11:28:16 Event: ULOG_EXECUTE for HTCondor Node proc_000000 ..."
        # Timestamps in braces are the actual event times: {02/20/26 11:28:16}
        ts_pattern = re.compile(
            r"\{(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\}"
        )
        node_pattern = re.compile(
            r"ULOG_(EXECUTE|JOB_TERMINATED) for HTCondor Node (\S+)"
        )

        proc_starts: list[datetime] = []
        proc_ends: list[datetime] = []
        merge_start: datetime | None = None
        merge_end: datetime | None = None
        cleanup_start: datetime | None = None
        cleanup_end: datetime | None = None

        for line in content.splitlines():
            nm = node_pattern.search(line)
            tm = ts_pattern.search(line)
            if not nm or not tm:
                continue
            event_type, node_name = nm.group(1), nm.group(2)
            ts = datetime.strptime(tm.group(1), "%m/%d/%y %H:%M:%S")

            if node_name.startswith("proc_"):
                if event_type == "EXECUTE":
                    proc_starts.append(ts)
                else:
                    proc_ends.append(ts)
            elif node_name == "merge":
                if event_type == "EXECUTE":
                    merge_start = ts
                else:
                    merge_end = ts
            elif node_name == "cleanup":
                if event_type == "EXECUTE":
                    cleanup_start = ts
                else:
                    cleanup_end = ts

        if proc_starts and proc_ends:
            perf.time_processing = (max(proc_ends) - min(proc_starts)).total_seconds()
        if merge_start and merge_end:
            perf.time_merge = (merge_end - merge_start).total_seconds()
        if cleanup_start and cleanup_end:
            perf.time_cleanup = (cleanup_end - cleanup_start).total_seconds()

        # Overhead = gaps between phases
        total_phases = perf.time_processing + perf.time_merge + perf.time_cleanup
        if proc_starts and (cleanup_end or merge_end or proc_ends):
            last_ts = cleanup_end or merge_end or max(proc_ends)
            first_ts = min(proc_starts)
            total_span = (last_ts - first_ts).total_seconds()
            perf.time_overhead = total_span - total_phases

        break  # only first merge group for now


def _collect_perf(wf: WorkflowDef, wf_dir: Path, cpu_samples: list[float]) -> PerfData:
    """Extract performance metrics from DAG artifacts.

    Tries three data sources in order:
    1. proc_N_metrics.json files (structured, per-job, per-step)
    2. Proc stdout FJR metric lines: "Step N: wall=Xs cpu_eff=Y rss=ZMB events=N"
    3. FJR XML files (only last job) + proc stdout wall times (all jobs)
    """
    perf = PerfData()
    perf.num_proc_jobs = wf.num_jobs

    # CPU utilization from polling samples
    # Skip first sample (DAGMan startup overhead)
    active = [p for p in cpu_samples[1:] if p > 50]
    if active:
        perf.cpu_avg_pct = sum(active) / len(active)
        perf.cpu_peak_pct = max(active)
    perf.cpu_expected_pct = wf.num_jobs * wf.multicore * 100

    # Collect per-job per-step data: step_name -> [sample_dicts]
    job_step_data: dict[str, list[dict]] = {}

    # Strategy 1: proc_N_metrics.json (written by wms2_proc.sh FJR parser)
    # Only use files with numeric node indices (skip literal ${NODE_INDEX})
    metrics_pattern = re.compile(r"proc_(\d+)_metrics\.json$")
    for gd in sorted(wf_dir.glob("mg_*")):
        for mf in sorted(gd.glob("proc_*_metrics.json")):
            if not metrics_pattern.search(mf.name):
                continue
            try:
                data = json.loads(mf.read_text())
                for step_data in data:
                    if "step_index" not in step_data:
                        continue
                    si = step_data["step_index"]
                    name = f"Step {si + 1}"
                    job_step_data.setdefault(name, []).append({
                        "wall": step_data.get("wall_time_sec") or 0,
                        "cpu_eff": step_data.get("cpu_efficiency") or 0,
                        "rss": step_data.get("peak_rss_mb") or 0,
                        "events": step_data.get("events_processed") or 0,
                        "throughput": step_data.get("throughput_ev_s") or 0,
                    })
            except Exception as exc:
                logger.warning("Failed to read %s: %s", mf, exc)

    # Strategy 2: proc stdout FJR metric lines
    if not job_step_data:
        pattern = re.compile(
            r"Step (\d+): wall=([\d.]+)s cpu_eff=([\d.]+|None) "
            r"rss=([\d.]+|None)MB events=(\d+|None)"
        )
        for gd in sorted(wf_dir.glob("mg_*")):
            for out_file in sorted(gd.glob("proc_*.out")):
                try:
                    content = out_file.read_text()
                except OSError:
                    continue
                for m in pattern.finditer(content):
                    name = f"Step {m.group(1)}"
                    job_step_data.setdefault(name, []).append({
                        "wall": float(m.group(2)) if m.group(2) != "None" else 0,
                        "cpu_eff": float(m.group(3)) if m.group(3) != "None" else 0,
                        "rss": float(m.group(4)) if m.group(4) != "None" else 0,
                        "events": int(m.group(5)) if m.group(5) != "None" else 0,
                    })

    # Strategy 3: FJR XMLs (last surviving job) + proc stdout wall times (all jobs)
    if not job_step_data:
        # Parse FJR XMLs for representative CPU/RSS data
        fjr_data: dict[str, dict] = {}
        for gd in sorted(wf_dir.glob("mg_*")):
            for fjr in sorted(gd.glob("report_step*.xml")):
                match = re.search(r"report_step(\d+)", fjr.name)
                if match:
                    parsed = _parse_fjr_xml(fjr)
                    if parsed:
                        fjr_data[match.group(1)] = parsed

        # Parse wall times from all proc stdout (reliable across all jobs)
        # Map step appearance order -> list of wall times
        step_walls: dict[int, list[float]] = {}
        for gd in sorted(wf_dir.glob("mg_*")):
            for out_file in sorted(gd.glob("proc_*.out")):
                try:
                    content = out_file.read_text()
                except OSError:
                    continue
                matches = re.findall(r"Step \S+ completed in (\d+)s", content)
                for i, wall_str in enumerate(matches):
                    step_walls.setdefault(i, []).append(float(wall_str))

        # Combine: wall times from all jobs, FJR metrics from one job
        n_steps = max(len(fjr_data), len(step_walls))
        for i in range(n_steps):
            step_num = str(i + 1)
            name = f"Step {step_num}"
            fjr = fjr_data.get(step_num, {})
            walls = step_walls.get(i, [])

            # Build one sample per job for wall times
            if walls:
                for w in walls:
                    job_step_data.setdefault(name, []).append({
                        "wall": w,
                        "cpu_eff": fjr.get("cpu_eff", 0),
                        "rss": fjr.get("rss", 0),
                        "events": fjr.get("events", 0),
                        "throughput": fjr.get("throughput", 0),
                    })
            elif fjr:
                job_step_data.setdefault(name, []).append(fjr)

    # Save raw data for re-analysis without re-running
    perf.raw_job_step_data = job_step_data
    perf.cpu_samples = cpu_samples

    # Aggregate into StepPerf objects
    for step_name in sorted(job_step_data.keys()):
        samples = job_step_data[step_name]
        perf.steps.append(_aggregate_step(step_name, samples, wf.events_per_job))

    # Time breakdown from DAGMan log
    _parse_dagman_times(wf_dir, perf)

    # Output sizes
    pfn_prefix = "/mnt/shared"
    for ds in wf.output_datasets:
        merged_lfn = ds.get("merged_lfn_base", "")
        if merged_lfn:
            pfn = Path(pfn_prefix) / merged_lfn.lstrip("/")
            if pfn.exists():
                files = [f for f in pfn.rglob("*") if f.is_file()]
                perf.merged_files += len(files)
                perf.merged_bytes += sum(f.stat().st_size for f in files)
        unmerged_lfn = ds.get("unmerged_lfn_base", "")
        if unmerged_lfn:
            pfn = Path(pfn_prefix) / unmerged_lfn.lstrip("/")
            if not pfn.exists() or not list(pfn.rglob("*")):
                perf.unmerged_cleaned = True

    return perf


def _collect_adaptive_perf(
    wf: WorkflowDef, wf_dir: Path, cpu_samples: list[float]
) -> PerfData:
    """Collect performance separately from each work unit round.

    Stores per-round step data with prefixed keys ("Round 1: Step N")
    and tuning decisions in raw_job_step_data["_adaptive"].
    """
    perf = PerfData()
    perf.num_proc_jobs = wf.num_jobs * wf.num_work_units

    # CPU utilization from polling samples
    active = [p for p in cpu_samples[1:] if p > 50]
    if active:
        perf.cpu_avg_pct = sum(active) / len(active)
        perf.cpu_peak_pct = max(active)
    perf.cpu_expected_pct = wf.num_jobs * wf.multicore * 100

    job_step_data: dict[str, list[dict]] = {}
    metrics_pattern = re.compile(r"proc_(\d+)_metrics\.json$")

    # Read replan decisions — one per replan node (replan_0_decisions.json, etc.)
    # Also try legacy name (replan_decisions.json) for backward compatibility.
    all_decisions: list[dict] = []
    for dp in sorted(wf_dir.glob("replan_*_decisions.json")):
        try:
            all_decisions.append(json.loads(dp.read_text()))
        except Exception:
            pass
    if not all_decisions:
        legacy = wf_dir / "replan_decisions.json"
        if legacy.exists():
            try:
                all_decisions.append(json.loads(legacy.read_text()))
            except Exception:
                pass

    # Use first replan decisions as primary adaptive_info (for reporter compat)
    # and store full list for N-round reporting.  Copy to avoid circular ref
    # when we add _all_decisions below.
    adaptive_info: dict = dict(all_decisions[0]) if all_decisions else {}

    # Probe node index to exclude from R1 (WU0) performance data.
    # The probe runs a different config (2×(N/2)T) and would corrupt
    # the R1 baseline metrics used for throughput comparison.
    probe_node = adaptive_info.get("probe_node", "")
    probe_idx: int | None = None
    if probe_node:
        idx_match = re.search(r"proc_0*(\d+)$", probe_node)
        if idx_match:
            probe_idx = int(idx_match.group(1))

    mg_dirs = sorted(wf_dir.glob("mg_*"))
    for round_idx, gd in enumerate(mg_dirs):
        round_label = f"Round {round_idx + 1}"

        # Collect from group dir and unmerged storage
        search_dirs = [gd]
        output_info_path = gd / "output_info.json"
        if output_info_path.exists():
            try:
                oi = json.loads(output_info_path.read_text())
                pfx = oi.get("local_pfn_prefix", "")
                gi = oi.get("group_index", 0)
                for ds in oi.get("output_datasets", []):
                    ub = ds.get("unmerged_lfn_base", "")
                    if ub and pfx:
                        udir = Path(pfx) / ub.lstrip("/") / f"{gi:06d}"
                        if udir.is_dir():
                            search_dirs.append(udir)
            except Exception:
                pass

        seen_metrics: set[str] = set()  # deduplicate by filename
        for search_dir in search_dirs:
            for mf in sorted(search_dir.glob("proc_*_metrics.json")):
                if not metrics_pattern.search(mf.name):
                    continue
                if mf.name in seen_metrics:
                    continue
                # Exclude probe node from R1 — it ran a different config
                if round_idx == 0 and probe_idx is not None:
                    file_idx_match = metrics_pattern.search(mf.name)
                    if file_idx_match and int(file_idx_match.group(1)) == probe_idx:
                        logger.info("Excluding %s from R1 perf (probe node)", mf.name)
                        seen_metrics.add(mf.name)
                        continue
                try:
                    data = json.loads(mf.read_text())
                    seen_metrics.add(mf.name)
                    for step_data in data:
                        if "step_index" not in step_data:
                            continue
                        si = step_data["step_index"]
                        name = f"{round_label}: Step {si + 1}"
                        job_step_data.setdefault(name, []).append({
                            "wall": step_data.get("wall_time_sec") or 0,
                            "cpu_eff": step_data.get("cpu_efficiency") or 0,
                            "rss": step_data.get("peak_rss_mb") or 0,
                            "events": step_data.get("events_processed") or 0,
                            "throughput": step_data.get("throughput_ev_s") or 0,
                        })
                except Exception as exc:
                    logger.warning("Failed to read %s: %s", mf, exc)

    # adaptive_info already read above (needed probe_node for exclusion)

    perf.raw_job_step_data = job_step_data
    # Store primary decisions for backward compat, plus full list for N-round
    adaptive_info["_all_decisions"] = all_decisions
    perf.raw_job_step_data["_adaptive"] = adaptive_info
    perf.cpu_samples = cpu_samples

    # Aggregate into StepPerf objects
    for step_name in sorted(job_step_data.keys()):
        if step_name.startswith("_"):
            continue
        samples = job_step_data[step_name]
        perf.steps.append(_aggregate_step(step_name, samples, wf.events_per_job))

    # Time breakdown — parse from both merge groups
    for gd in sorted(wf_dir.glob("mg_*")):
        _parse_dagman_times_from_dir(gd, perf)

    # Output sizes
    pfn_prefix = "/mnt/shared"
    for ds in wf.output_datasets:
        merged_lfn = ds.get("merged_lfn_base", "")
        if merged_lfn:
            pfn = Path(pfn_prefix) / merged_lfn.lstrip("/")
            if pfn.exists():
                files = [f for f in pfn.rglob("*") if f.is_file()]
                perf.merged_files += len(files)
                perf.merged_bytes += sum(f.stat().st_size for f in files)
        unmerged_lfn = ds.get("unmerged_lfn_base", "")
        if unmerged_lfn:
            pfn = Path(pfn_prefix) / unmerged_lfn.lstrip("/")
            if not pfn.exists() or not list(pfn.rglob("*")):
                perf.unmerged_cleaned = True

    return perf


def _parse_dagman_times_from_dir(gd: Path, perf: PerfData) -> None:
    """Parse DAGMan log from a single merge group, accumulating into perf."""
    from datetime import datetime

    dagman_out = gd / "group.dag.dagman.out"
    if not dagman_out.exists():
        return
    try:
        content = dagman_out.read_text()
    except OSError:
        return

    ts_pattern = re.compile(r"\{(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\}")
    node_pattern = re.compile(
        r"ULOG_(EXECUTE|JOB_TERMINATED) for HTCondor Node (\S+)"
    )

    proc_starts: list[datetime] = []
    proc_ends: list[datetime] = []
    merge_start: datetime | None = None
    merge_end: datetime | None = None
    cleanup_start: datetime | None = None
    cleanup_end: datetime | None = None

    for line in content.splitlines():
        nm = node_pattern.search(line)
        tm = ts_pattern.search(line)
        if not nm or not tm:
            continue
        event_type, node_name = nm.group(1), nm.group(2)
        ts = datetime.strptime(tm.group(1), "%m/%d/%y %H:%M:%S")

        if node_name.startswith("proc_"):
            if event_type == "EXECUTE":
                proc_starts.append(ts)
            else:
                proc_ends.append(ts)
        elif node_name == "merge":
            if event_type == "EXECUTE":
                merge_start = ts
            else:
                merge_end = ts
        elif node_name == "cleanup":
            if event_type == "EXECUTE":
                cleanup_start = ts
            else:
                cleanup_end = ts

    if proc_starts and proc_ends:
        perf.time_processing += (max(proc_ends) - min(proc_starts)).total_seconds()
    if merge_start and merge_end:
        perf.time_merge += (merge_end - merge_start).total_seconds()
    if cleanup_start and cleanup_end:
        perf.time_cleanup += (cleanup_end - cleanup_start).total_seconds()


# ── Probe node injection ─────────────────────────────────────


def _inject_probe_node(wf: WorkflowDef, wu0_dir: Path) -> str | None:
    """Modify the last proc node in WU0 to run as a 2×(N/2)T probe.

    The probe splits step 0 into 2 parallel instances, each with half the
    cores, inside the same 8-core slot.  Its actual RSS becomes ground truth
    for R2 memory estimation (replacing the theoretical extrapolation).

    Steps:
    1. Read manifest.json from WU0 merge group dir
    2. Create manifest_tuned.json with steps[0].multicore = ncores//2,
       steps[0].n_parallel = 2, and split_tmpfs if enabled
    3. Patch ONLY the last proc submit file:
       - Add manifest_tuned.json to transfer_input_files
       - Set request_memory = max_memory_per_core × ncores
    4. Guard: skip if < 2 proc nodes

    Returns the probe node name (e.g. "proc_000001") or None if skipped.
    """
    # Find proc submit files
    proc_subs = sorted(wu0_dir.glob("proc_*.sub"))
    if len(proc_subs) < 2:
        logger.info("Probe split skipped: need >= 2 proc nodes, have %d", len(proc_subs))
        return None

    # Read manifest.json
    manifest_path = wu0_dir / "manifest.json"
    if not manifest_path.exists():
        logger.warning("Probe split skipped: no manifest.json in %s", wu0_dir)
        return None

    manifest = json.loads(manifest_path.read_text())
    steps = manifest.get("steps", [])
    if not steps:
        logger.warning("Probe split skipped: manifest has no steps")
        return None

    ncores = wf.multicore
    half_cores = max(ncores // 2, 2)

    # Create manifest_tuned.json for probe
    manifest["steps"][0]["multicore"] = half_cores
    manifest["steps"][0]["n_parallel"] = 2
    if wf.split_tmpfs:
        manifest["split_tmpfs"] = True
    tuned_path = wu0_dir / "manifest_tuned.json"
    tuned_path.write_text(json.dumps(manifest, indent=2))

    # Patch last proc submit file
    last_sub = proc_subs[-1]
    content = last_sub.read_text()

    # Add manifest_tuned.json to transfer_input_files
    tif_match = re.search(
        r"^(transfer_input_files\s*=\s*)(.+)$", content, re.MULTILINE
    )
    if tif_match:
        prefix = tif_match.group(1)
        files = tif_match.group(2).rstrip()
        new_line = f"{prefix}{files}, {tuned_path}"
        content = content[:tif_match.start()] + new_line + content[tif_match.end():]

    # Set request_memory = max_memory_per_core × ncores
    max_memory_mb = wf.max_memory_per_core_mb * ncores
    mem_match = re.search(
        r"^(request_memory\s*=\s*)(\d+)", content, re.MULTILINE
    )
    if mem_match:
        content = (content[:mem_match.start()]
                   + f"{mem_match.group(1)}{max_memory_mb}"
                   + content[mem_match.end():])

    last_sub.write_text(content)

    # Extract probe node name from filename (e.g. proc_000001.sub -> proc_000001)
    probe_node = last_sub.stem
    logger.info(
        "Probe node %s: 2×%dT, request_memory=%d MB, manifest_tuned.json injected",
        probe_node, half_cores, max_memory_mb,
    )
    return probe_node


# ── Main runner ──────────────────────────────────────────────


class MatrixRunner:
    """Execute workflow definitions against real HTCondor."""

    def __init__(self, caps: dict[str, bool] | None = None):
        self.caps = caps or detect_capabilities()
        self._condor = None
        self._settings = None

    def _ensure_infra(self):
        """Lazy-init HTCondor adapter and settings."""
        if self._condor is not None:
            return
        from wms2.adapters.condor import HTCondorAdapter
        from wms2.config import Settings

        self._condor = HTCondorAdapter(CONDOR_HOST)
        self._settings = Settings(
            database_url="postgresql+asyncpg://test:test@localhost:5432/test",
            submit_base_dir="",  # overridden per-workflow
            processing_executable="/bin/true",
            merge_executable="/bin/true",
            cleanup_executable="/bin/true",
            local_pfn_prefix="/mnt/shared",
            max_input_files=0,
        )

    async def run_workflow(self, wf: WorkflowDef) -> WorkflowResult:
        """Execute a single workflow definition end-to-end."""
        result = WorkflowResult(wf_id=wf.wf_id, title=wf.title)

        # 1. Pre-check capabilities
        can_run, reason = workflow_can_run(wf, self.caps)
        if not can_run:
            result.status = "skipped"
            result.reason = reason
            return result

        result.status = "running"
        start = time.monotonic()

        try:
            self._ensure_infra()
            result = await self._execute(wf, result)
        except Exception as exc:
            result.status = "error"
            result.reason = str(exc)
            logger.exception("Workflow %.1f error", wf.wf_id)
        finally:
            result.elapsed_sec = time.monotonic() - start

        return result

    async def _execute(self, wf: WorkflowDef, result: WorkflowResult) -> WorkflowResult:
        from wms2.core.dag_planner import DAGPlanner, PilotMetrics
        from wms2.core.sandbox import create_sandbox

        # 2. Pre-flight disk space check
        n_concurrent = wf.num_jobs * getattr(wf, "num_work_units", 1)
        _check_disk_space(n_concurrent, Path(f"/mnt/shared/work/wms2_matrix/wf_{wf.wf_id}"))

        # 3. Sweep (also cleans leftover LFN output paths)
        work_dir = sweep_pre(wf.wf_id, output_datasets=wf.output_datasets)
        submit_dir = str(work_dir / "submit")

        # 3. Sandbox
        sandbox_path = str(work_dir / "sandbox.tar.gz")
        if wf.sandbox_mode == "cached" and wf.cached_sandbox_path:
            shutil.copy2(wf.cached_sandbox_path, sandbox_path)
        else:
            create_sandbox(sandbox_path, wf.request_spec, mode=wf.sandbox_mode)

        # Dispatch to adaptive path if enabled
        if wf.adaptive:
            return await self._execute_adaptive(wf, result, work_dir, sandbox_path)

        # 4. Mock repo + planner
        mock_wf = _make_workflow_mock(wf, sandbox_path)
        repo = _make_mock_repo()

        # For multi-WU workflows, split into 1 job per WU so each gets its
        # own merge group (and its own manifest/tmpfs config).
        if wf.num_work_units > 1:
            jpwu = max(wf.num_jobs // wf.num_work_units, 1)
        else:
            jpwu = max(wf.num_jobs, 1)
        settings = self._settings.model_copy(
            update={
                "submit_base_dir": submit_dir,
                "jobs_per_work_unit": jpwu,
            }
        )

        planner = DAGPlanner(
            repository=repo,
            dbs_adapter=MagicMock(),
            rucio_adapter=MagicMock(),
            condor_adapter=self._condor,
            settings=settings,
        )

        # 5. Plan and submit
        metrics = PilotMetrics.from_request(mock_wf.config_data)
        await planner.plan_production_dag(mock_wf, metrics=metrics)

        cluster_id, schedd_name = _extract_cluster_info(repo)
        if not cluster_id:
            result.status = "error"
            result.reason = "no cluster_id after plan_production_dag"
            return result

        result.cluster_id = cluster_id
        wf_dir = Path(submit_dir) / str(mock_wf.id)
        result.submit_dir = str(wf_dir)

        # 5b. Inject split_tmpfs via manifest_tuned.json if requested.
        # Can't modify manifest.json directly — sandbox extraction overwrites it.
        # manifest_tuned.json takes precedence (the wrapper applies it after extraction).
        if wf.split_tmpfs:
            import json as _json
            import re as _re
            skip_below = getattr(wf, "split_tmpfs_from_wu", 0)
            for mg_dir in sorted(wf_dir.glob("mg_*")):
                # Extract WU index from dir name (mg_000000 → 0)
                wu_idx = int(mg_dir.name.split("_")[1])
                if wu_idx < skip_below:
                    continue
                mf = mg_dir / "manifest.json"
                if not mf.is_file():
                    continue
                try:
                    data = _json.loads(mf.read_text())
                    data["split_tmpfs"] = True
                    tuned = mg_dir / "manifest_tuned.json"
                    tuned.write_text(_json.dumps(data, indent=2))
                    # Add to transfer_input_files in each proc submit file
                    for sub in sorted(mg_dir.glob("proc_*.sub")):
                        content = sub.read_text()
                        tif = _re.search(r"^(transfer_input_files\s*=\s*)(.+)$",
                                         content, _re.MULTILINE)
                        if tif:
                            new = f"{tif.group(1)}{tif.group(2).rstrip()}, {tuned}"
                            content = content[:tif.start()] + new + content[tif.end():]
                            sub.write_text(content)
                except Exception:
                    pass

        # 5c. Per-WU memory overrides.
        if wf.memory_mb_per_wu:
            import re as _re2
            for mg_dir in sorted(wf_dir.glob("mg_*")):
                wu_idx = int(mg_dir.name.split("_")[1])
                if wu_idx not in wf.memory_mb_per_wu:
                    continue
                mem = wf.memory_mb_per_wu[wu_idx]
                for sub in sorted(mg_dir.glob("proc_*.sub")):
                    content = sub.read_text()
                    content = _re2.sub(
                        r"^(request_memory\s*=\s*)\d+",
                        f"\\g<1>{mem}",
                        content, flags=_re2.MULTILINE,
                    )
                    sub.write_text(content)

        # 6. Fault injection
        if wf.fault:
            patched = inject_faults(str(wf_dir), wf.fault)
            logger.info("Injected %d faults for wf %.1f", patched, wf.wf_id)

        # 7. Poll DAGMan (with CPU monitoring)
        result.dag_status, cpu_samples = await self._poll_dag(
            schedd_name, cluster_id, wf.timeout_sec
        )

        # 8. Collect performance metrics (before verify/sweep may clean artifacts)
        result.perf = _collect_perf(wf, wf_dir, cpu_samples)

        # 9. Verify
        _verify(wf, result, wf_dir)

        # 10. Sweep post — always keep artifacts for inspection
        sweep_post(wf.wf_id, keep_artifacts=True)

        return result

    async def _execute_adaptive(
        self,
        wf: WorkflowDef,
        result: WorkflowResult,
        work_dir: Path,
        sandbox_path: str,
    ) -> WorkflowResult:
        """Adaptive execution: N work units with replan nodes between them.

        Each replan node analyzes the previous WU's metrics and patches the
        next WU's manifest with per-step nThreads tuning.  Supports arbitrary
        num_work_units (2 = one replan, 3 = two replans, etc.).

        1. Plan with mock condor adapter (generates files, no submission)
        2. Generate replan_0..replan_{N-2}.sub (universe=local)
        3. Modify workflow.dag: add replan nodes between consecutive WUs
        4. Submit with real condor adapter
        5. Poll, collect per-round perf, verify, sweep
        """
        from wms2.core.dag_planner import DAGPlanner, PilotMetrics

        submit_dir = str(work_dir / "submit")

        # Compute memory from per-core fields
        round1_memory_mb = wf.memory_per_core_mb * wf.multicore
        max_memory_mb = wf.max_memory_per_core_mb * wf.multicore

        # Total events across all work units
        total_events = wf.events_per_job * wf.num_jobs * wf.num_work_units
        mock_wf = _make_workflow_mock(wf, sandbox_path)
        mock_wf.config_data["memory_mb"] = round1_memory_mb
        mock_wf.config_data["request_num_events"] = total_events
        mock_wf.splitting_params["events_per_job"] = wf.events_per_job

        repo = _make_mock_repo()

        settings = self._settings.model_copy(
            update={
                "submit_base_dir": submit_dir,
                "jobs_per_work_unit": max(wf.num_jobs, 1),
            }
        )

        # Step 1: Plan with MOCK condor adapter (generates files, no submission)
        mock_condor = MagicMock()
        mock_condor.submit_dag = AsyncMock(return_value=("0", "localhost"))

        planner = DAGPlanner(
            repository=repo,
            dbs_adapter=MagicMock(),
            rucio_adapter=MagicMock(),
            condor_adapter=mock_condor,
            settings=settings,
        )

        metrics = PilotMetrics.from_request(mock_wf.config_data)
        await planner.plan_production_dag(mock_wf, metrics=metrics)

        wf_dir = Path(submit_dir) / str(mock_wf.id)
        result.submit_dir = str(wf_dir)

        mg_dirs = sorted(wf_dir.glob("mg_*"))
        if len(mg_dirs) < 2:
            result.status = "error"
            result.reason = f"Expected {wf.num_work_units} merge groups, got {len(mg_dirs)}"
            return result

        logger.info("Planned %d merge groups for adaptive workflow", len(mg_dirs))

        # Step 1b: Inject probe node if enabled (only in first WU)
        probe_node_name: str | None = None
        if wf.probe_split:
            probe_node_name = _inject_probe_node(wf, mg_dirs[0])

        # Step 2: Generate replan nodes — one between each consecutive pair
        venv_python = str(Path(__file__).resolve().parents[2] / ".venv" / "bin" / "python")
        dag_path = wf_dir / "workflow.dag"
        dag_content = dag_path.read_text()
        dag_content += "\n# Adaptive replan nodes between work units\n"

        for replan_idx in range(len(mg_dirs) - 1):
            next_dir = mg_dirs[replan_idx + 1]
            node_name = f"replan_{replan_idx}"

            # Pass ALL prior dirs (rounds 0..replan_idx) for multi-round analysis
            prior_dirs = mg_dirs[:replan_idx + 1]
            prior_dirs_str = ",".join(str(d) for d in prior_dirs)

            replan_args = (
                f"-m tests.matrix.adaptive replan"
                f" --prior-wu-dirs {prior_dirs_str}"
                f" --wu1-dir {next_dir}"
                f" --ncores {wf.multicore}"
                f" --mem-per-core {wf.memory_per_core_mb}"
                f" --max-mem-per-core {wf.max_memory_per_core_mb}"
                f" --safety-margin {wf.safety_margin}"
                f" --overcommit-max {wf.overcommit_max}"
                f" --replan-index {replan_idx}"
                + (f" --no-split" if not wf.adaptive_split else "")
                + (f" --split-all-steps" if wf.split_all_steps else "")
                + (f" --uniform-threads" if wf.split_uniform_threads else "")
                + (f" --split-tmpfs" if wf.split_tmpfs else "")
                + (f" --job-split --events-per-job {wf.events_per_job}"
                   f" --num-jobs {wf.num_jobs}" if wf.job_split else "")
                # Probe node only for first replan (it's in WU0)
                + (f" --probe-node {probe_node_name}"
                   if probe_node_name and replan_idx == 0 else "")
            )

            prev_dir = mg_dirs[replan_idx]
            sub_path = wf_dir / f"{node_name}.sub"
            sub_path.write_text("\n".join([
                f"# Adaptive replan {replan_idx} (between {prev_dir.name} and {next_dir.name})",
                "universe = local",
                f"executable = {venv_python}",
                f"arguments = {replan_args}",
                f"output = {wf_dir}/{node_name}.out",
                f"error = {wf_dir}/{node_name}.err",
                f"log = {wf_dir}/{node_name}.log",
                "queue 1",
            ]) + "\n")

            dag_content += f"JOB {node_name} {sub_path}\n"
            dag_content += f"PARENT {prev_dir.name} CHILD {node_name}\n"
            dag_content += f"PARENT {node_name} CHILD {next_dir.name}\n"

        # Step 3: Write modified workflow.dag
        dag_path.write_text(dag_content)
        logger.info("Modified workflow.dag with %d replan node(s)", len(mg_dirs) - 1)

        # Step 4: Submit with REAL condor adapter
        cluster_id, schedd_name = await self._condor.submit_dag(str(dag_path))
        if not cluster_id:
            result.status = "error"
            result.reason = "no cluster_id after submit_dag"
            return result

        result.cluster_id = cluster_id
        logger.info("Submitted adaptive DAG, cluster_id=%s", cluster_id)

        # Step 5: Poll until completion
        result.dag_status, cpu_samples = await self._poll_dag(
            schedd_name, cluster_id, wf.timeout_sec
        )

        # Step 6: Collect per-round performance
        result.perf = _collect_adaptive_perf(wf, wf_dir, cpu_samples)

        # Step 7: Verify
        _verify(wf, result, wf_dir)

        # Step 8: Sweep — always keep artifacts for inspection
        sweep_post(wf.wf_id, keep_artifacts=True)

        return result

    async def _poll_dag(
        self, schedd_name: str, cluster_id: str, timeout_sec: int
    ) -> tuple[str, list[float]]:
        """Poll DAGMan until done, removed, or timeout.

        Returns (status_string, cpu_samples_percent).
        CPU samples use cgroup accounting (HTCondor jobs only) when available,
        falling back to system-wide /proc/stat if cgroup is unavailable.
        """
        start = time.monotonic()
        cpu_samples: list[float] = []

        # Prefer cgroup accounting — tracks only HTCondor job CPU
        use_cgroup = _read_cgroup_cpu_usec() > 0
        if use_cgroup:
            prev_cg_usec = _read_cgroup_cpu_usec()
            prev_time = time.monotonic()
        else:
            prev_busy, prev_total = _read_cpu_jiffies()

        while time.monotonic() - start < timeout_sec:
            try:
                info = await self._condor.query_job(schedd_name, cluster_id)
            except Exception as exc:
                logger.warning("Poll error: %s", exc)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Sample CPU
            if use_cgroup:
                cur_cg_usec = _read_cgroup_cpu_usec()
                cur_time = time.monotonic()
                cpu_pct = _cgroup_cpu_cores(prev_cg_usec, cur_cg_usec, cur_time - prev_time)
                prev_cg_usec = cur_cg_usec
                prev_time = cur_time
            else:
                cur_busy, cur_total = _read_cpu_jiffies()
                cpu_pct = _cpu_percent(prev_busy, prev_total, cur_busy, cur_total)
                prev_busy, prev_total = cur_busy, cur_total
            cpu_samples.append(cpu_pct)

            if info is None:
                completed = await self._condor.check_job_completed(
                    cluster_id, schedd_name
                )
                return ("completed" if completed else "vanished"), cpu_samples

            job_status = int(info.get("JobStatus", 0))
            nodes_done = int(info.get("DAG_NodesDone", 0))
            nodes_total = int(info.get("DAG_NodesTotal", 0))
            nodes_failed = int(info.get("DAG_NodesFailed", 0))
            elapsed = int(time.monotonic() - start)

            logger.debug(
                "wf poll [%ds] status=%d done=%d/%d failed=%d cpu=%.0f%%",
                elapsed, job_status, nodes_done, nodes_total, nodes_failed, cpu_pct,
            )

            if job_status == 4:  # completed
                return "completed", cpu_samples
            if job_status == 3:  # removed
                return "removed", cpu_samples

            await asyncio.sleep(POLL_INTERVAL)

        # Timeout — attempt removal
        try:
            await self._condor.remove_job(schedd_name, cluster_id)
        except Exception:
            pass
        return "timeout", cpu_samples

    async def run_many(self, workflows: list[WorkflowDef]) -> list[WorkflowResult]:
        """Run a list of workflows sequentially.

        Serial workflows always run one at a time.  Non-serial workflows
        could be parallelized in future but for now run sequentially to
        keep schedd load predictable.
        """
        results: list[WorkflowResult] = []
        for wf in workflows:
            result = await self.run_workflow(wf)
            results.append(result)
        return results


# ── Result persistence ──────────────────────────────────────


def save_results(results: list[WorkflowResult], tag: str = "") -> Path:
    """Save results to a timestamped JSON file.  Returns the file path."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    suffix = f"_{tag}" if tag else ""
    path = RESULTS_DIR / f"results_{ts}{suffix}.json"
    data = {
        "timestamp": ts,
        "tag": tag,
        "results": [r.to_dict() for r in results],
    }
    path.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Saved results to %s", path)
    return path


def load_results(path: Path | None = None) -> list[WorkflowResult]:
    """Load results from a JSON file.  If path is None, load the latest."""
    if path is None:
        files = sorted(RESULTS_DIR.glob("results_*.json"))
        if not files:
            raise FileNotFoundError(f"No saved results in {RESULTS_DIR}")
        path = files[-1]
    data = json.loads(path.read_text())
    return [WorkflowResult.from_dict(r) for r in data["results"]]


def list_saved_results() -> list[Path]:
    """Return all saved result files, newest last."""
    if not RESULTS_DIR.exists():
        return []
    return sorted(RESULTS_DIR.glob("results_*.json"))
