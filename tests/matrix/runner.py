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


# ── CPU monitoring ───────────────────────────────────────────


def _read_cpu_jiffies() -> tuple[int, int]:
    """Read total CPU jiffies from /proc/stat.  Returns (busy, total)."""
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

        # 2. Sweep (also cleans leftover LFN output paths)
        work_dir = sweep_pre(wf.wf_id, output_datasets=wf.output_datasets)
        submit_dir = str(work_dir / "submit")

        # 3. Sandbox
        sandbox_path = str(work_dir / "sandbox.tar.gz")
        if wf.sandbox_mode == "cached" and wf.cached_sandbox_path:
            import shutil
            shutil.copy2(wf.cached_sandbox_path, sandbox_path)
        else:
            create_sandbox(sandbox_path, wf.request_spec, mode=wf.sandbox_mode)

        # 4. Mock repo + planner
        mock_wf = _make_workflow_mock(wf, sandbox_path)
        repo = _make_mock_repo()

        settings = self._settings.model_copy(
            update={
                "submit_base_dir": submit_dir,
                "jobs_per_work_unit": max(wf.num_jobs, 1),
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

        # 10. Sweep post — keep artifacts on failure
        sweep_post(wf.wf_id, keep_artifacts=(result.status != "passed"))

        return result

    async def _poll_dag(
        self, schedd_name: str, cluster_id: str, timeout_sec: int
    ) -> tuple[str, list[float]]:
        """Poll DAGMan until done, removed, or timeout.

        Returns (status_string, cpu_samples_percent).
        """
        start = time.monotonic()
        cpu_samples: list[float] = []
        prev_busy, prev_total = _read_cpu_jiffies()

        while time.monotonic() - start < timeout_sec:
            try:
                info = await self._condor.query_job(schedd_name, cluster_id)
            except Exception as exc:
                logger.warning("Poll error: %s", exc)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # Sample CPU
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
