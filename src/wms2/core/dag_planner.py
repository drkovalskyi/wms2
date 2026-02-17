"""DAG Planner: pilot submission, production DAG planning, and DAG file generation."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wms2.adapters.base import CondorAdapter, DBSAdapter, RucioAdapter
from wms2.config import Settings
from wms2.core.splitters import DAGNodeSpec, InputFile, get_splitter
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, WorkflowStatus

logger = logging.getLogger(__name__)


# ── Data classes ────────────────────────────────────────────────


@dataclass
class PilotMetrics:
    """Performance metrics extracted from a pilot job's JSON report."""
    events_per_second: float = 1.0
    memory_peak_mb: int = 2000
    output_size_per_event_kb: float = 50.0
    time_per_event_sec: float = 1.0
    cpu_efficiency: float = 0.8

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> PilotMetrics:
        return cls(
            events_per_second=float(data.get("events_per_second", 1.0)),
            memory_peak_mb=int(data.get("memory_peak_mb", 2000)),
            output_size_per_event_kb=float(data.get("output_size_per_event_kb", 50.0)),
            time_per_event_sec=float(data.get("time_per_event_sec", 1.0)),
            cpu_efficiency=float(data.get("cpu_efficiency", 0.8)),
        )


@dataclass
class PlanningMergeGroup:
    """A merge group being assembled during planning."""
    group_index: int
    processing_nodes: list[DAGNodeSpec] = field(default_factory=list)
    estimated_output_kb: float = 0.0


# ── DAG Planner ────────────────────────────────────────────────


class DAGPlanner:
    def __init__(
        self,
        repository: Repository,
        dbs_adapter: DBSAdapter,
        rucio_adapter: RucioAdapter,
        condor_adapter: CondorAdapter,
        settings: Settings,
    ):
        self.db = repository
        self.dbs = dbs_adapter
        self.rucio = rucio_adapter
        self.condor = condor_adapter
        self.settings = settings

    # ── Pilot Phase ──────────────────────────────────────────

    async def submit_pilot(self, workflow) -> None:
        """Write a pilot submit file. No actual submission yet (Phase 3)."""
        submit_dir = os.path.join(
            self.settings.submit_base_dir, str(workflow.id), "pilot"
        )
        os.makedirs(submit_dir, exist_ok=True)

        # Fetch a small sample of files
        files = await self.dbs.get_files(workflow.input_dataset, limit=5)
        if not files:
            logger.warning("No files found for pilot of workflow %s", workflow.id)
            return

        pilot_sub = _generate_pilot_submit(
            submit_dir=submit_dir,
            sandbox_url=workflow.sandbox_url,
            input_files=[f["logical_file_name"] for f in files],
        )
        pilot_path = os.path.join(submit_dir, "pilot.sub")
        _write_file(pilot_path, pilot_sub)

        # Submit pilot to HTCondor
        cluster_id, schedd = await self.condor.submit_job(pilot_path)

        await self.db.update_workflow(
            workflow.id,
            status=WorkflowStatus.PILOT_RUNNING.value,
            pilot_output_path=submit_dir,
            pilot_cluster_id=cluster_id,
            pilot_schedd=schedd,
        )
        logger.info(
            "Pilot submitted for workflow %s: cluster=%s schedd=%s",
            workflow.id, cluster_id, schedd,
        )

    def _parse_pilot_report(self, path: str) -> PilotMetrics:
        """Parse pilot JSON report from disk."""
        with open(path) as f:
            data = json.load(f)
        return PilotMetrics.from_json(data)

    async def handle_pilot_completion(self, workflow, report_path: str) -> Any:
        """Parse pilot metrics and proceed to production DAG planning."""
        metrics = self._parse_pilot_report(report_path)
        await self.db.update_workflow(
            workflow.id,
            pilot_metrics={
                "events_per_second": metrics.events_per_second,
                "memory_peak_mb": metrics.memory_peak_mb,
                "output_size_per_event_kb": metrics.output_size_per_event_kb,
                "time_per_event_sec": metrics.time_per_event_sec,
                "cpu_efficiency": metrics.cpu_efficiency,
            },
        )
        return await self.plan_production_dag(workflow, metrics=metrics)

    # ── Production DAG ───────────────────────────────────────

    async def plan_production_dag(
        self, workflow, metrics: PilotMetrics | None = None
    ) -> Any:
        """Full pipeline: fetch files → split → merge groups → write DAG files → create DB row."""
        # 1. Resource parameters
        if metrics is None:
            metrics = PilotMetrics()  # defaults

        config = workflow.config_data or {}
        is_gen = config.get("_is_gen", False)

        if is_gen:
            # GEN workflow: no input files, create synthetic event-range nodes
            nodes = self._plan_gen_nodes(workflow, config)
        else:
            nodes = await self._plan_file_based_nodes(workflow)

        if not nodes:
            raise ValueError(
                f"No processing nodes generated for workflow {workflow.id}"
            )

        # Plan merge groups
        merge_groups = _plan_merge_groups(
            nodes,
            output_size_per_event_kb=metrics.output_size_per_event_kb,
            target_kb=self.settings.target_merged_size_kb,
        )

        # 7. Generate DAG files on disk
        submit_dir = os.path.join(self.settings.submit_base_dir, str(workflow.id))
        os.makedirs(submit_dir, exist_ok=True)
        executables = {
            "processing": self.settings.processing_executable,
            "merge": self.settings.merge_executable,
            "cleanup": self.settings.cleanup_executable,
        }
        # Extract output dataset info from workflow config_data
        config = workflow.config_data or {}
        output_datasets = config.get("output_datasets")
        # Extract sandbox path and resource params from config_data
        sandbox_path = config.get("sandbox_path")
        memory_mb = config.get("memory_mb", 0)
        resource_params = {}
        if memory_mb:
            resource_params["memory_mb"] = int(memory_mb)
        disk_kb = config.get("disk_kb", 0)
        if disk_kb:
            resource_params["disk_kb"] = int(disk_kb)

        dag_file_path = _generate_dag_files(
            submit_dir=submit_dir,
            workflow_id=str(workflow.id),
            merge_groups=merge_groups,
            sandbox_url=workflow.sandbox_url,
            category_throttles=workflow.category_throttles or {
                "Processing": 5000, "Merge": 100, "Cleanup": 50,
            },
            executables=executables,
            output_datasets=output_datasets,
            output_base_dir=self.settings.output_base_dir,
            sandbox_path=sandbox_path,
            resource_params=resource_params or None,
        )

        # 8. Count totals
        total_proc = sum(len(mg.processing_nodes) for mg in merge_groups)
        # Each group: 1 landing + N proc + 1 merge + 1 cleanup = N + 3
        total_nodes = sum(len(mg.processing_nodes) + 3 for mg in merge_groups)
        # Edges: landing→all_proc + all_proc→merge + merge→cleanup = N + N + 1 = 2N + 1 per group
        total_edges = sum(2 * len(mg.processing_nodes) + 1 for mg in merge_groups)

        # 9. Create DAG row
        dag = await self.db.create_dag(
            workflow_id=workflow.id,
            dag_file_path=dag_file_path,
            submit_dir=submit_dir,
            total_nodes=total_nodes,
            total_edges=total_edges,
            node_counts={
                "processing": total_proc,
                "merge": len(merge_groups),
                "cleanup": len(merge_groups),
                "landing": len(merge_groups),
            },
            total_work_units=len(merge_groups),
            status=DAGStatus.READY.value,
        )

        # 10. Submit DAG to HTCondor
        cluster_id, schedd = await self.condor.submit_dag(dag_file_path)
        now = datetime.now(timezone.utc)
        await self.db.update_dag(
            dag.id,
            dagman_cluster_id=cluster_id,
            schedd_name=schedd,
            status=DAGStatus.SUBMITTED.value,
            submitted_at=now,
        )

        await self.db.update_workflow(
            workflow.id,
            dag_id=dag.id,
            status=WorkflowStatus.ACTIVE.value,
            total_nodes=total_nodes,
        )

        logger.info(
            "Production DAG planned for workflow %s: %d groups, %d proc nodes, path=%s",
            workflow.id, len(merge_groups), total_proc, dag_file_path,
        )
        return dag

    async def _plan_file_based_nodes(self, workflow) -> list[DAGNodeSpec]:
        """Fetch input files from DBS and split them into processing nodes."""
        limit = self.settings.max_input_files if self.settings.max_input_files > 0 else 0
        raw_files = await self.dbs.get_files(workflow.input_dataset, limit=limit)
        if not raw_files:
            return []

        lfns = [f["logical_file_name"] for f in raw_files]
        try:
            replica_map = await self.rucio.get_replicas(lfns)
        except Exception as exc:
            logger.warning("Rucio replica lookup failed (%s), using empty locations", exc)
            replica_map = {}

        input_files = [
            InputFile(
                lfn=f["logical_file_name"],
                file_size=f.get("file_size", 0),
                event_count=f.get("event_count", 0),
                locations=replica_map.get(f["logical_file_name"], []),
            )
            for f in raw_files
        ]

        splitter = get_splitter(
            workflow.splitting_algo,
            workflow.splitting_params or {},
        )
        return splitter.split(input_files)

    def _plan_gen_nodes(self, workflow, config: dict) -> list[DAGNodeSpec]:
        """Create synthetic processing nodes for GEN workflows (no input files).

        Uses RequestNumEvents and EventsPerJob from the request to compute
        the number of event-generation jobs needed.
        """
        import math

        params = workflow.splitting_params or {}
        events_per_job = params.get("events_per_job") or params.get("eventsPerJob") or 100_000
        total_events = config.get("request_num_events") or 0
        max_files = self.settings.max_input_files

        if total_events <= 0:
            logger.warning("GEN workflow %s has no RequestNumEvents, using 1 node", workflow.id)
            total_events = events_per_job

        num_jobs = math.ceil(total_events / events_per_job)

        # Respect --max-files limit (reinterpreted as max jobs for GEN)
        if max_files > 0:
            num_jobs = min(num_jobs, max_files)

        nodes: list[DAGNodeSpec] = []
        for i in range(num_jobs):
            first_event = i * events_per_job + 1
            last_event = min((i + 1) * events_per_job, total_events)
            actual_events = last_event - first_event + 1

            # Create a synthetic InputFile so the rest of the pipeline works
            synthetic_file = InputFile(
                lfn=f"synthetic://gen/events_{first_event}_{last_event}",
                file_size=0,
                event_count=actual_events,
                locations=[],
            )
            nodes.append(
                DAGNodeSpec(
                    node_index=i,
                    input_files=[synthetic_file],
                    first_event=first_event,
                    last_event=last_event,
                    events_per_job=actual_events,
                )
            )

        logger.info(
            "GEN workflow %s: %d nodes, %d events/job, %d total events",
            workflow.id, num_jobs, events_per_job, total_events,
        )
        return nodes


# ── Merge Group Planning ───────────────────────────────────────


def _plan_merge_groups(
    nodes: list[DAGNodeSpec],
    output_size_per_event_kb: float,
    target_kb: int,
) -> list[PlanningMergeGroup]:
    """Accumulate processing nodes into merge groups by estimated output size."""
    if not nodes:
        return []

    groups: list[PlanningMergeGroup] = []
    current = PlanningMergeGroup(group_index=0)

    for node in nodes:
        node_output_kb = node.total_events * output_size_per_event_kb
        if current.processing_nodes and (
            current.estimated_output_kb + node_output_kb > target_kb
        ):
            groups.append(current)
            current = PlanningMergeGroup(group_index=len(groups))

        current.processing_nodes.append(node)
        current.estimated_output_kb += node_output_kb

    if current.processing_nodes:
        groups.append(current)

    return groups


# ── DAG File Generation (Appendix C format) ────────────────────


def _generate_dag_files(
    submit_dir: str,
    workflow_id: str,
    merge_groups: list[PlanningMergeGroup],
    sandbox_url: str,
    category_throttles: dict[str, int],
    executables: dict[str, str] | None = None,
    output_datasets: list[dict] | None = None,
    output_base_dir: str = "",
    sandbox_path: str | None = None,
    resource_params: dict[str, int] | None = None,
) -> str:
    """Generate all DAG files on disk. Returns path to outer workflow.dag."""
    submit_path = Path(submit_dir)

    # Write shared config and scripts
    _write_file(
        str(submit_path / "dagman.config"),
        "DAGMAN_MAX_RESCUE_NUM = 10\nDAGMAN_USER_LOG_SCAN_INTERVAL = 30\n",
    )
    _write_elect_site_script(str(submit_path / "elect_site.sh"))
    _write_pin_site_script(str(submit_path / "pin_site.sh"))
    _write_post_script(str(submit_path / "post_script.sh"))
    _write_proc_script(str(submit_path / "wms2_proc.sh"))
    _write_merge_script(str(submit_path / "wms2_merge.py"))

    # Generate outer DAG
    outer_lines = [
        f"# WMS2-generated DAG for workflow {workflow_id}",
        f"CONFIG {submit_path / 'dagman.config'}",
        f"NODE_STATUS_FILE {submit_path / 'workflow.dag.status'}",
        "",
    ]

    for mg in merge_groups:
        mg_name = f"mg_{mg.group_index:06d}"
        mg_dir = submit_path / mg_name
        mg_dir.mkdir(parents=True, exist_ok=True)

        outer_lines.append(f"SUBDAG EXTERNAL {mg_name} {mg_dir / 'group.dag'} DIR {mg_dir}")

        # Generate merge group sub-DAG
        _generate_group_dag(
            group_dir=mg_dir,
            submit_dir=submit_path,
            merge_group=mg,
            sandbox_url=sandbox_url,
            category_throttles=category_throttles,
            executables=executables,
            output_datasets=output_datasets,
            output_base_dir=output_base_dir,
            sandbox_path=sandbox_path,
            resource_params=resource_params,
        )

    # Category throttling for merge groups
    outer_lines.append("")
    for mg in merge_groups:
        mg_name = f"mg_{mg.group_index:06d}"
        outer_lines.append(f"CATEGORY {mg_name} MergeGroup")
    outer_lines.append("MAXJOBS MergeGroup 10")

    dag_path = str(submit_path / "workflow.dag")
    _write_file(dag_path, "\n".join(outer_lines) + "\n")
    return dag_path


def _generate_group_dag(
    group_dir: Path,
    submit_dir: Path,
    merge_group: PlanningMergeGroup,
    sandbox_url: str,
    category_throttles: dict[str, int],
    executables: dict[str, str] | None = None,
    output_datasets: list[dict] | None = None,
    output_base_dir: str = "",
    sandbox_path: str | None = None,
    resource_params: dict[str, int] | None = None,
) -> None:
    """Generate a single merge group sub-DAG (group.dag) + submit files."""
    exe = executables or {}
    proc_exe = exe.get("processing", "run_payload.sh")
    merge_exe = exe.get("merge", "run_merge.sh")
    cleanup_exe = exe.get("cleanup", "run_cleanup.sh")

    # In test mode (/bin/true), use the generated trivial scripts instead
    if proc_exe == "/bin/true":
        proc_exe = str(submit_dir / "wms2_proc.sh")
    if merge_exe == "/bin/true":
        merge_exe = str(submit_dir / "wms2_merge.py")

    # Resource parameters
    rp = resource_params or {}
    memory_mb = rp.get("memory_mb", 0)
    disk_kb = rp.get("disk_kb", 0)

    # Build transfer_input_files list for processing nodes
    proc_transfer_files: list[str] = []
    if sandbox_path and os.path.isfile(sandbox_path):
        proc_transfer_files.append(sandbox_path)

    proc_nodes = merge_group.processing_nodes
    lines: list[str] = [
        f"# Merge group {merge_group.group_index}",
        "",
    ]

    # Landing node
    _write_submit_file(
        str(group_dir / "landing.sub"),
        executable="/bin/true",
        arguments="",
        description="landing node",
    )
    lines.append("JOB landing landing.sub")
    lines.append(
        f"SCRIPT POST landing {submit_dir / 'elect_site.sh'} elected_site"
    )
    lines.append("")

    # Processing nodes
    sandbox_ref = os.path.basename(sandbox_path) if sandbox_path else sandbox_url
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        input_lfns = ",".join(f.lfn for f in node.input_files)

        # Build arguments with event range info
        proc_args = f"--sandbox {sandbox_ref} --input {input_lfns}"
        proc_args += f" --node-index {node.node_index}"
        if node.first_event > 0:
            proc_args += f" --first-event {node.first_event}"
        if node.last_event > 0:
            proc_args += f" --last-event {node.last_event}"
        if node.events_per_job > 0:
            proc_args += f" --events-per-job {node.events_per_job}"

        _write_submit_file(
            str(group_dir / f"{node_name}.sub"),
            executable=proc_exe,
            arguments=proc_args,
            description=f"processing node {node.node_index}",
            desired_sites=node.primary_location,
            memory_mb=memory_mb,
            disk_kb=disk_kb,
            transfer_input_files=proc_transfer_files or None,
        )
        lines.append(f"JOB {node_name} {node_name}.sub")
        lines.append(
            f"SCRIPT PRE {node_name} {submit_dir / 'pin_site.sh'} {node_name}.sub elected_site"
        )
        lines.append(
            f"SCRIPT POST {node_name} {submit_dir / 'post_script.sh'} {node_name} $RETURN"
        )
        lines.append("")

    # Merge node — write output info to a file, reference it by path in arguments
    merge_args = f"--sandbox {sandbox_ref}"
    if output_datasets and output_base_dir:
        output_info = {
            "output_datasets": [
                {
                    "dataset_name": d.get("dataset_name", ""),
                    "merged_lfn_base": d.get("merged_lfn_base", ""),
                    "data_tier": d.get("data_tier", ""),
                }
                for d in output_datasets
            ],
            "output_base_dir": output_base_dir,
            "group_index": merge_group.group_index,
        }
        output_info_path = str(group_dir / "output_info.json")
        _write_file(output_info_path, json.dumps(output_info, indent=2))
        merge_args += f" --output-info {output_info_path}"
    _write_submit_file(
        str(group_dir / "merge.sub"),
        executable=merge_exe,
        arguments=merge_args,
        description="merge node",
        memory_mb=memory_mb,
        disk_kb=disk_kb,
    )
    lines.append("JOB merge merge.sub")
    lines.append(
        f"SCRIPT PRE merge {submit_dir / 'pin_site.sh'} merge.sub elected_site"
    )
    lines.append("")

    # Cleanup node
    _write_submit_file(
        str(group_dir / "cleanup.sub"),
        executable=cleanup_exe,
        arguments="",
        description="cleanup node",
    )
    lines.append("JOB cleanup cleanup.sub")
    lines.append(
        f"SCRIPT PRE cleanup {submit_dir / 'pin_site.sh'} cleanup.sub elected_site"
    )
    lines.append("")

    # Retries
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        lines.append(f"RETRY {node_name} 3 UNLESS-EXIT 2")
    lines.append("RETRY merge 2 UNLESS-EXIT 2")
    lines.append("RETRY cleanup 1")
    lines.append("")

    # Dependencies
    proc_names = " ".join(f"proc_{n.node_index:06d}" for n in proc_nodes)
    lines.append(f"PARENT landing CHILD {proc_names}")
    lines.append(f"PARENT {proc_names} CHILD merge")
    lines.append("PARENT merge CHILD cleanup")
    lines.append("")

    # Categories
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        lines.append(f"CATEGORY {node_name} Processing")
    lines.append("CATEGORY merge Merge")
    lines.append("CATEGORY cleanup Cleanup")
    for cat, limit in category_throttles.items():
        lines.append(f"MAXJOBS {cat} {limit}")

    _write_file(str(group_dir / "group.dag"), "\n".join(lines) + "\n")


# ── Submit File + Script Generation ────────────────────────────


def _write_submit_file(
    path: str,
    executable: str,
    arguments: str,
    description: str,
    desired_sites: str = "",
    memory_mb: int = 0,
    disk_kb: int = 0,
    transfer_input_files: list[str] | None = None,
) -> None:
    lines = [
        f"# {description}",
        "universe = vanilla",
        f"executable = {executable}",
        f"arguments = {arguments}",
        f"output = {Path(path).stem}.out",
        f"error = {Path(path).stem}.err",
        f"log = {Path(path).stem}.log",
    ]
    if memory_mb > 0:
        lines.append(f"request_memory = {memory_mb}")
    if disk_kb > 0:
        lines.append(f"request_disk = {disk_kb}")
    lines.append("should_transfer_files = YES")
    lines.append("when_to_transfer_output = ON_EXIT")
    if transfer_input_files:
        lines.append(f"transfer_input_files = {','.join(transfer_input_files)}")
    if desired_sites:
        lines.append(f'+DESIRED_Sites = "{desired_sites}"')
    lines.append("queue 1")
    _write_file(path, "\n".join(lines) + "\n")


def _generate_pilot_submit(
    submit_dir: str,
    sandbox_url: str,
    input_files: list[str],
) -> str:
    input_str = ",".join(input_files)
    return "\n".join([
        "# WMS2 pilot job",
        "universe = vanilla",
        "executable = run_pilot.sh",
        f"arguments = --sandbox {sandbox_url} --input {input_str}",
        "output = pilot.out",
        "error = pilot.err",
        "log = pilot.log",
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT",
        "transfer_output_files = pilot_metrics.json",
        "queue 1",
    ]) + "\n"


def _write_elect_site_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# elect_site.sh — POST script for landing node
# Extracts GLIDEIN_CMSSite from the completed landing job.
# Falls back to "local" for dev environments without glidein infrastructure.
ELECTED_SITE_FILE=$1
CLUSTER_ID=$(condor_q -format "%d.0" ClusterId \\
    -constraint 'DAGNodeName=="landing"' 2>/dev/null)
SITE=$(condor_history $CLUSTER_ID -limit 1 -af MATCH_GLIDEIN_CMSSite 2>/dev/null)
if [ -n "$SITE" ] && [ "$SITE" != "undefined" ]; then
    echo "$SITE" > "$ELECTED_SITE_FILE"
else
    echo "local" > "$ELECTED_SITE_FILE"
fi
exit 0
""")
    os.chmod(path, 0o755)


def _write_pin_site_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# pin_site.sh — PRE script for processing/merge/cleanup nodes
SUBMIT_FILE=$1
SITE=$(cat "$2")
sed -i "s/+DESIRED_Sites = .*/+DESIRED_Sites = \\"${SITE}\\"/" "$SUBMIT_FILE"
exit 0
""")
    os.chmod(path, 0o755)


def _write_post_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# post_script.sh — POST script for processing nodes
NODE_NAME=$1
RETURN_CODE=$2
if [ "$RETURN_CODE" != "0" ]; then
    echo "Node $NODE_NAME failed with exit code $RETURN_CODE" >&2
fi
exit $RETURN_CODE
""")
    os.chmod(path, 0o755)


def _write_proc_script(path: str) -> None:
    """Generate a processing wrapper that supports CMSSW, synthetic, and pilot modes.

    Extracts sandbox, reads manifest.json, and dispatches to the appropriate mode.
    HTCondor captures stdout in proc_NNNNNN.out.  For CMSSW mode the real output
    files are ROOT files transferred back by HTCondor.
    """
    _write_file(path, r'''#!/bin/bash
# run_payload.sh — WMS2 processing job wrapper
# Supports CMSSW mode (cmsRun via CVMFS), synthetic mode (sized output),
# and pilot mode (iterative measurement).
set -euo pipefail

# ── Argument parsing ──────────────────────────────────────────
SANDBOX=""
INPUT_LFNS=""
FIRST_EVENT=0
LAST_EVENT=0
EVENTS_PER_JOB=0
NODE_INDEX=0
PILOT_MODE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sandbox)    SANDBOX="$2";        shift 2 ;;
        --input)      INPUT_LFNS="$2";     shift 2 ;;
        --first-event) FIRST_EVENT="$2";   shift 2 ;;
        --last-event)  LAST_EVENT="$2";    shift 2 ;;
        --events-per-job) EVENTS_PER_JOB="$2"; shift 2 ;;
        --node-index) NODE_INDEX="$2";     shift 2 ;;
        --pilot)      PILOT_MODE=true;     shift   ;;
        *)            echo "Unknown arg: $1" >&2; shift ;;
    esac
done

START_TIME=$(date +%s)
echo "=== WMS2 Processing Wrapper ==="
echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "host:      $(hostname)"
echo "pid:       $$"
echo "sandbox:   $SANDBOX"
echo "node:      $NODE_INDEX"
echo "pilot:     $PILOT_MODE"

# ── Sandbox extraction ────────────────────────────────────────
WORK_DIR=$(pwd)
if [[ -n "$SANDBOX" && -f "$SANDBOX" ]]; then
    echo "Extracting sandbox: $SANDBOX"
    tar xzf "$SANDBOX"
elif [[ -n "$SANDBOX" && ! -f "$SANDBOX" ]]; then
    echo "WARNING: Sandbox not found: $SANDBOX — running without sandbox"
fi

# ── Read manifest ─────────────────────────────────────────────
MODE="synthetic"
if [[ -f manifest.json ]]; then
    MODE=$(python3 -c "import json; print(json.load(open('manifest.json')).get('mode','synthetic'))")
    echo "manifest mode: $MODE"
else
    echo "No manifest.json found — defaulting to synthetic mode"
fi

# ── Helper: convert LFN to xrootd URL ────────────────────────
lfn_to_xrootd() {
    local lfn="$1"
    if [[ "$lfn" == root://* ]]; then
        echo "$lfn"
    elif [[ "$lfn" == synthetic://* ]]; then
        echo ""
    else
        echo "root://cms-xrd-global.cern.ch/$lfn"
    fi
}

# ── CMSSW mode ────────────────────────────────────────────────
run_cmssw_mode() {
    echo "--- CMSSW mode ---"

    # Read CMSSW params from manifest
    CMSSW_VERSION=$(python3 -c "import json; print(json.load(open('manifest.json'))['cmssw_version'])")
    SCRAM_ARCH=$(python3 -c "import json; print(json.load(open('manifest.json'))['scram_arch'])")
    NUM_STEPS=$(python3 -c "import json; print(len(json.load(open('manifest.json')).get('steps',[])))")

    echo "CMSSW:     $CMSSW_VERSION"
    echo "arch:      $SCRAM_ARCH"
    echo "steps:     $NUM_STEPS"

    # Check CVMFS
    if [[ ! -d /cvmfs/cms.cern.ch ]]; then
        echo "ERROR: /cvmfs/cms.cern.ch not available" >&2
        exit 1
    fi

    # Setup CMSSW environment
    export SCRAM_ARCH
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    scramv1 project CMSSW "$CMSSW_VERSION"
    cd "${CMSSW_VERSION}/src"
    eval $(scramv1 runtime -sh)
    cd "$WORK_DIR"

    echo "CMSSW environment ready"

    # Build input file list for first step
    IFS=',' read -ra LFN_ARRAY <<< "$INPUT_LFNS"
    XROOTD_INPUTS=""
    for lfn in "${LFN_ARRAY[@]}"; do
        url=$(lfn_to_xrootd "$lfn")
        if [[ -n "$url" ]]; then
            if [[ -n "$XROOTD_INPUTS" ]]; then
                XROOTD_INPUTS="${XROOTD_INPUTS},$url"
            else
                XROOTD_INPUTS="$url"
            fi
        fi
    done

    # Run each step
    PREV_OUTPUT=""
    for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
        step_num=$((step_idx + 1))
        STEP_NAME=$(python3 -c "import json; print(json.load(open('manifest.json'))['steps'][$step_idx]['name'])")
        PSET_PATH=$(python3 -c "import json; print(json.load(open('manifest.json'))['steps'][$step_idx]['pset'])")
        INPUT_FROM=$(python3 -c "import json; print(json.load(open('manifest.json'))['steps'][$step_idx].get('input_from_step',''))")

        echo ""
        echo "--- Step $step_num: $STEP_NAME ---"
        echo "  PSet: $PSET_PATH"

        CMSRUN_ARGS="-j report_step${step_num}.xml $PSET_PATH"

        # Determine input files for this step
        if [[ -n "$INPUT_FROM" && -n "$PREV_OUTPUT" ]]; then
            CMSRUN_ARGS="$CMSRUN_ARGS inputFiles=file:$PREV_OUTPUT"
        elif [[ -n "$XROOTD_INPUTS" && $step_idx -eq 0 ]]; then
            CMSRUN_ARGS="$CMSRUN_ARGS inputFiles=$XROOTD_INPUTS"
        fi

        # Event range
        if [[ "$EVENTS_PER_JOB" -gt 0 ]]; then
            CMSRUN_ARGS="$CMSRUN_ARGS maxEvents=$EVENTS_PER_JOB"
        fi
        if [[ "$FIRST_EVENT" -gt 0 ]]; then
            CMSRUN_ARGS="$CMSRUN_ARGS firstEvent=$FIRST_EVENT"
        fi

        echo "  cmsRun $CMSRUN_ARGS"
        STEP_START=$(date +%s)
        cmsRun $CMSRUN_ARGS
        STEP_RC=$?
        STEP_END=$(date +%s)
        STEP_WALL=$((STEP_END - STEP_START))

        if [[ $STEP_RC -ne 0 ]]; then
            echo "ERROR: Step $STEP_NAME (cmsRun) failed with exit code $STEP_RC" >&2
            exit $STEP_RC
        fi

        echo "  Step $STEP_NAME completed in ${STEP_WALL}s"

        # Parse FrameworkJobReport for output file
        if [[ -f "report_step${step_num}.xml" ]]; then
            PREV_OUTPUT=$(python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('report_step${step_num}.xml')
    for f in tree.findall('.//File'):
        pfn = f.findtext('PFN', '')
        if pfn:
            print(pfn)
            break
except Exception:
    pass
" 2>/dev/null || true)
            echo "  Output: ${PREV_OUTPUT:-none}"
        fi
    done

    # Collect output sizes
    echo ""
    echo "--- Output files ---"
    OUTPUT_SIZES=0
    for f in *.root; do
        if [[ -f "$f" ]]; then
            sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
            echo "  $f: $sz bytes"
            OUTPUT_SIZES=$((OUTPUT_SIZES + sz))
        fi
    done

    END_TIME=$(date +%s)
    WALL_TIME=$((END_TIME - START_TIME))
    echo ""
    echo "=== CMSSW mode complete ==="
    echo "wall_time_sec: $WALL_TIME"
    echo "output_bytes:  $OUTPUT_SIZES"
}

# ── Synthetic mode ────────────────────────────────────────────
run_synthetic_mode() {
    echo "--- Synthetic mode ---"

    # Read params from manifest (or use defaults)
    SIZE_PER_EVENT_KB=50.0
    TIME_PER_EVENT_SEC=0.5
    MEMORY_MB=2048

    if [[ -f manifest.json ]]; then
        SIZE_PER_EVENT_KB=$(python3 -c "import json; print(json.load(open('manifest.json')).get('size_per_event_kb', 50.0))")
        TIME_PER_EVENT_SEC=$(python3 -c "import json; print(json.load(open('manifest.json')).get('time_per_event_sec', 0.5))")
        MEMORY_MB=$(python3 -c "import json; print(json.load(open('manifest.json')).get('memory_mb', 2048))")
    fi

    # Calculate events to process
    EVENTS=$EVENTS_PER_JOB
    if [[ "$EVENTS" -le 0 ]]; then
        EVENTS=1000
    fi

    echo "events:         $EVENTS"
    echo "size/event(KB): $SIZE_PER_EVENT_KB"
    echo "time/event(s):  $TIME_PER_EVENT_SEC"

    # Calculate output size in bytes
    OUTPUT_BYTES=$(python3 -c "print(int($EVENTS * $SIZE_PER_EVENT_KB * 1024))")

    # Simulate processing time (capped at 300s for testing)
    SLEEP_TIME=$(python3 -c "print(min(300, $EVENTS * $TIME_PER_EVENT_SEC))")
    echo "sleep_time:     ${SLEEP_TIME}s"
    sleep "$SLEEP_TIME"

    # Create synthetic output file
    OUTPUT_FILE="proc_${NODE_INDEX}_output.root"
    dd if=/dev/urandom of="$OUTPUT_FILE" bs=1024 count="$(python3 -c "print(int($EVENTS * $SIZE_PER_EVENT_KB))")" 2>/dev/null
    ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo 0)
    echo "output_file:    $OUTPUT_FILE ($ACTUAL_SIZE bytes)"

    END_TIME=$(date +%s)
    WALL_TIME=$((END_TIME - START_TIME))

    echo ""
    echo "=== Synthetic mode complete ==="
    echo "wall_time_sec:      $WALL_TIME"
    echo "events_processed:   $EVENTS"
    echo "output_bytes:       $ACTUAL_SIZE"
}

# ── Pilot mode ────────────────────────────────────────────────
run_pilot_mode() {
    echo "--- Pilot mode ---"
    echo "Running iterative measurements..."

    EVENTS=$EVENTS_PER_JOB
    if [[ "$EVENTS" -le 0 ]]; then
        EVENTS=100
    fi

    PILOT_JSON="pilot_metrics.json"
    ITERATIONS="[]"

    for multiplier in 1 2 4; do
        iter_events=$((EVENTS * multiplier))
        echo ""
        echo "Pilot iteration: $iter_events events"
        ITER_START=$(date +%s)

        # Use the same mode dispatch but with modified event count
        EVENTS_PER_JOB=$iter_events

        if [[ "$MODE" == "cmssw" && -d /cvmfs/cms.cern.ch ]]; then
            run_cmssw_mode
        else
            run_synthetic_mode
        fi

        ITER_END=$(date +%s)
        ITER_WALL=$((ITER_END - ITER_START))

        # Collect output size
        ITER_OUTPUT_SIZE=0
        for f in *.root proc_*_output.root; do
            if [[ -f "$f" ]]; then
                sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
                ITER_OUTPUT_SIZE=$((ITER_OUTPUT_SIZE + sz))
            fi
        done

        # Measure peak RSS (from /proc/self)
        PEAK_RSS=$(python3 -c "
try:
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('VmHWM:'):
                print(int(line.split()[1]))
                break
except Exception:
    print(0)
")

        ITERATIONS=$(python3 -c "
import json
iters = json.loads('$ITERATIONS')
iters.append({
    'events': $iter_events,
    'wall_time_sec': $ITER_WALL,
    'output_bytes': $ITER_OUTPUT_SIZE,
    'peak_rss_kb': $PEAK_RSS,
})
print(json.dumps(iters))
")

        # Clean up for next iteration
        rm -f *.root proc_*_output.root
    done

    # Write pilot_metrics.json
    python3 -c "
import json

iterations = json.loads('$ITERATIONS')

# Calculate derived metrics from last iteration
last = iterations[-1] if iterations else {}
events = last.get('events', 1)
wall = last.get('wall_time_sec', 1) or 1
output_bytes = last.get('output_bytes', 0)
peak_rss = last.get('peak_rss_kb', 0)

metrics = {
    'events_per_second': events / wall,
    'time_per_event_sec': wall / events,
    'memory_peak_mb': peak_rss // 1024 if peak_rss else 2000,
    'output_size_per_event_kb': (output_bytes / 1024) / events if events else 50.0,
    'cpu_efficiency': 0.8,
    'iterations': iterations,
}
with open('$PILOT_JSON', 'w') as f:
    json.dump(metrics, f, indent=2)
print('Wrote:', '$PILOT_JSON')
"

    echo ""
    echo "=== Pilot mode complete ==="
}

# ── Main dispatch ─────────────────────────────────────────────
if [[ "$PILOT_MODE" == "true" ]]; then
    run_pilot_mode
elif [[ "$MODE" == "cmssw" && -d /cvmfs/cms.cern.ch ]]; then
    run_cmssw_mode
else
    if [[ "$MODE" == "cmssw" ]]; then
        echo "WARNING: CMSSW mode requested but /cvmfs/cms.cern.ch not available"
        echo "         Falling back to synthetic mode"
    fi
    run_synthetic_mode
fi
''')
    os.chmod(path, 0o755)


def _write_merge_script(path: str) -> None:
    """Generate a merge script that handles both ROOT files and text outputs.

    Detects output type: if .root files exist, uses hadd (from CMSSW env)
    for ROOT merging; otherwise falls back to text concatenation of proc_*.out.
    """
    _write_file(path, '''#!/usr/bin/env python3
"""wms2_merge.py — WMS2 merge job.

Detects output type and merges accordingly:
- ROOT files (.root): uses hadd from CMSSW environment
- Text files (proc_*.out): concatenates into merged text output
"""
import glob
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

# Parse arguments
output_info_path = None
sandbox_path = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--output-info" and i + 1 < len(sys.argv):
        output_info_path = sys.argv[i + 1]
        i += 2
    elif sys.argv[i] == "--sandbox" and i + 1 < len(sys.argv):
        sandbox_path = sys.argv[i + 1]
        i += 2
    else:
        i += 1

if not output_info_path:
    print("ERROR: --output-info not specified", file=sys.stderr)
    sys.exit(1)

with open(output_info_path) as f:
    info = json.load(f)

group_dir = os.path.dirname(output_info_path)
output_base_dir = info["output_base_dir"]
group_index = info["group_index"]
datasets = info.get("output_datasets", [])


def setup_cmssw_env():
    """Set up CMSSW environment for hadd if sandbox has manifest."""
    manifest_path = os.path.join(group_dir, "manifest.json")
    if not os.path.exists(manifest_path):
        return False

    with open(manifest_path) as f:
        manifest = json.load(f)

    if manifest.get("mode") != "cmssw":
        return False

    cmssw_version = manifest.get("cmssw_version", "")
    scram_arch = manifest.get("scram_arch", "")

    if not cmssw_version or not os.path.isdir("/cvmfs/cms.cern.ch"):
        return False

    # Source CMSSW environment and get hadd path
    setup_cmd = (
        f"source /cvmfs/cms.cern.ch/cmsset_default.sh && "
        f"export SCRAM_ARCH={scram_arch} && "
        f"scramv1 project CMSSW {cmssw_version} && "
        f"cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && "
        f"which hadd"
    )
    try:
        result = subprocess.run(
            ["bash", "-c", setup_cmd],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode == 0:
            os.environ["CMSSW_HADD"] = result.stdout.strip().split("\\n")[-1]
            return True
    except Exception as e:
        print(f"WARNING: CMSSW setup failed: {e}", file=sys.stderr)

    return False


def merge_root_files(ds, root_files, out_dir):
    """Merge ROOT files using hadd."""
    tier = ds.get("data_tier", "unknown")
    out_file = os.path.join(out_dir, f"merged_{tier}.root")

    hadd_cmd = os.environ.get("CMSSW_HADD", "hadd")
    cmd = [hadd_cmd, "-f", out_file] + root_files

    print(f"Merging {len(root_files)} ROOT files -> {out_file}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)

    if result.returncode != 0:
        print(f"ERROR: hadd failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    size = os.path.getsize(out_file) if os.path.exists(out_file) else 0
    print(f"Wrote: {out_file} ({size} bytes, {len(root_files)} inputs)")
    return out_file


def merge_text_files(datasets, text_files, output_base_dir, group_index):
    """Merge text files (original behavior)."""
    proc_entries = []
    for pf in text_files:
        node_name = os.path.splitext(os.path.basename(pf))[0]
        with open(pf) as f:
            content = f.read().strip()
        if content:
            proc_entries.append(f"{node_name} | {content}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for ds in datasets:
        lfn_base = ds.get("merged_lfn_base", "")
        tier = ds.get("data_tier", "unknown")
        ds_name = ds.get("dataset_name", "")

        if lfn_base.startswith("/store/"):
            relative = lfn_base[len("/store/"):]
        else:
            relative = lfn_base.lstrip("/")

        out_dir = os.path.join(output_base_dir, relative, f"{group_index:06d}")
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, "merged.txt")

        with open(out_file, "w") as fout:
            header = [
                "# WMS2 Merged Output",
                f"# Dataset:     {ds_name}",
                f"# Data tier:   {tier}",
                f"# Merge group: mg_{group_index:06d}",
                f"# Generated:   {now}",
                f"# Host:        {os.uname().nodename}",
                f"# Jobs merged: {len(proc_entries)}",
                "#",
            ]
            for line in header:
                fout.write(line + "\\n")
            for entry in proc_entries:
                fout.write(entry + "\\n")

        print(f"Wrote: {out_file} ({len(proc_entries)} entries)")


# Detect output type
root_files = sorted(glob.glob(os.path.join(group_dir, "*.root")))
# Also check for synthetic output pattern
root_files += sorted(glob.glob(os.path.join(group_dir, "proc_*_output.root")))
root_files = sorted(set(root_files))  # deduplicate

text_files = sorted(glob.glob(os.path.join(group_dir, "proc_*.out")))

if root_files:
    print(f"Detected {len(root_files)} ROOT file(s) — using ROOT merge mode")
    has_cmssw = setup_cmssw_env()

    for ds in datasets:
        lfn_base = ds.get("merged_lfn_base", "")
        tier = ds.get("data_tier", "unknown")

        if lfn_base.startswith("/store/"):
            relative = lfn_base[len("/store/"):]
        else:
            relative = lfn_base.lstrip("/")

        out_dir = os.path.join(output_base_dir, relative, f"{group_index:06d}")
        os.makedirs(out_dir, exist_ok=True)

        if has_cmssw:
            merge_root_files(ds, root_files, out_dir)
        else:
            # Without hadd, just copy/concatenate ROOT files
            print(f"WARNING: hadd not available, copying ROOT files to {out_dir}")
            import shutil
            for rf in root_files:
                dest = os.path.join(out_dir, os.path.basename(rf))
                shutil.copy2(rf, dest)
                print(f"  Copied: {dest}")
else:
    print(f"Detected {len(text_files)} text file(s) — using text merge mode")
    merge_text_files(datasets, text_files, output_base_dir, group_index)
''')
    os.chmod(path, 0o755)


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)
