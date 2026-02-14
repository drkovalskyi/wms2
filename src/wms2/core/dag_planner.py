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

        # 2. Fetch input files from DBS
        raw_files = await self.dbs.get_files(workflow.input_dataset)
        if not raw_files:
            raise ValueError(
                f"No input files for dataset {workflow.input_dataset}"
            )

        # 3. Get replica locations from Rucio
        lfns = [f["logical_file_name"] for f in raw_files]
        replica_map = await self.rucio.get_replicas(lfns)

        # 4. Convert to InputFile objects
        input_files = [
            InputFile(
                lfn=f["logical_file_name"],
                file_size=f.get("file_size", 0),
                event_count=f.get("event_count", 0),
                locations=replica_map.get(f["logical_file_name"], []),
            )
            for f in raw_files
        ]

        # 5. Split via appropriate splitter
        splitter = get_splitter(
            workflow.splitting_algo,
            workflow.splitting_params or {},
        )
        nodes = splitter.split(input_files)

        # 6. Plan merge groups
        merge_groups = _plan_merge_groups(
            nodes,
            output_size_per_event_kb=metrics.output_size_per_event_kb,
            target_kb=self.settings.target_merged_size_kb,
        )

        # 7. Generate DAG files on disk
        submit_dir = os.path.join(self.settings.submit_base_dir, str(workflow.id))
        os.makedirs(submit_dir, exist_ok=True)
        dag_file_path = _generate_dag_files(
            submit_dir=submit_dir,
            workflow_id=str(workflow.id),
            merge_groups=merge_groups,
            sandbox_url=workflow.sandbox_url,
            category_throttles=workflow.category_throttles or {
                "Processing": 5000, "Merge": 100, "Cleanup": 50,
            },
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

    # Generate outer DAG
    outer_lines = [
        f"# WMS2-generated DAG for workflow {workflow_id}",
        f"CONFIG {submit_path / 'dagman.config'}",
        "",
    ]

    for mg in merge_groups:
        mg_name = f"mg_{mg.group_index:06d}"
        mg_dir = submit_path / mg_name
        mg_dir.mkdir(parents=True, exist_ok=True)

        outer_lines.append(f"SUBDAG EXTERNAL {mg_name} {mg_dir / 'group.dag'}")

        # Generate merge group sub-DAG
        _generate_group_dag(
            group_dir=mg_dir,
            submit_dir=submit_path,
            merge_group=mg,
            sandbox_url=sandbox_url,
            category_throttles=category_throttles,
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
) -> None:
    """Generate a single merge group sub-DAG (group.dag) + submit files."""
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
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        input_lfns = ",".join(f.lfn for f in node.input_files)
        _write_submit_file(
            str(group_dir / f"{node_name}.sub"),
            executable="run_payload.sh",
            arguments=f"--sandbox {sandbox_url} --input {input_lfns}",
            description=f"processing node {node.node_index}",
            desired_sites=node.primary_location,
        )
        lines.append(f"JOB {node_name} {node_name}.sub")
        lines.append(
            f"SCRIPT PRE {node_name} {submit_dir / 'pin_site.sh'} {node_name}.sub elected_site"
        )
        lines.append(
            f"SCRIPT POST {node_name} {submit_dir / 'post_script.sh'} {node_name} $RETURN"
        )
        lines.append("")

    # Merge node
    _write_submit_file(
        str(group_dir / "merge.sub"),
        executable="run_merge.sh",
        arguments=f"--sandbox {sandbox_url}",
        description="merge node",
    )
    lines.append("JOB merge merge.sub")
    lines.append(
        f"SCRIPT PRE merge {submit_dir / 'pin_site.sh'} merge.sub elected_site"
    )
    lines.append("")

    # Cleanup node
    _write_submit_file(
        str(group_dir / "cleanup.sub"),
        executable="run_cleanup.sh",
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
) -> None:
    lines = [
        f"# {description}",
        "universe = vanilla",
        f"executable = {executable}",
        f"arguments = {arguments}",
        f"output = {Path(path).stem}.out",
        f"error = {Path(path).stem}.err",
        f"log = {Path(path).stem}.log",
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT",
    ]
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
ELECTED_SITE_FILE=$1
CLUSTER_ID=$(condor_q -format "%d.0" ClusterId \\
    -constraint 'DAGNodeName=="landing"' 2>/dev/null)
SITE=$(condor_history $CLUSTER_ID -limit 1 -af MATCH_GLIDEIN_CMSSite 2>/dev/null)
if [ -n "$SITE" ]; then
    echo "$SITE" > "$ELECTED_SITE_FILE"
    exit 0
else
    echo "ERROR: Could not determine site from landing job" >&2
    exit 1
fi
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


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)
