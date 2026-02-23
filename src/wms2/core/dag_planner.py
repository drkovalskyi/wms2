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
    steps: list[dict] | None = None
    per_output_module: dict | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> PilotMetrics:
        # New-format reports have a "summary" section; old-format has flat fields
        summary = data.get("summary", {})
        return cls(
            events_per_second=float(
                summary.get("events_per_second", data.get("events_per_second", 1.0))
            ),
            memory_peak_mb=int(
                summary.get("peak_rss_mb", data.get("memory_peak_mb", 2000))
            ),
            output_size_per_event_kb=float(
                summary.get("output_size_per_event_kb", data.get("output_size_per_event_kb", 50.0))
            ),
            time_per_event_sec=float(
                summary.get("total_time_per_event_sec", data.get("time_per_event_sec", 1.0))
            ),
            cpu_efficiency=float(data.get("cpu_efficiency", 0.8)),
            steps=data.get("steps"),
            per_output_module=summary.get("per_output_module"),
        )

    @classmethod
    def from_request(cls, config: dict) -> PilotMetrics:
        """Create metrics from request spec fields."""
        tpe = float(config.get("time_per_event", 1.0))
        return cls(
            time_per_event_sec=tpe,
            memory_peak_mb=int(config.get("memory_mb", 2000)),
            output_size_per_event_kb=float(config.get("size_per_event", 50.0)),
            events_per_second=1.0 / max(tpe, 0.001),
        )


@dataclass
class PlanningMergeGroup:
    """A merge group being assembled during planning."""
    group_index: int
    processing_nodes: list[DAGNodeSpec] = field(default_factory=list)


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
        """Write a pilot submit file and the pilot script, then submit."""
        from wms2.core.pilot_runner import write_pilot_script

        submit_dir = os.path.join(
            self.settings.submit_base_dir, str(workflow.id), "pilot"
        )
        os.makedirs(submit_dir, exist_ok=True)

        # Generate the standalone pilot script
        pilot_script_path = os.path.join(submit_dir, "wms2_pilot.py")
        write_pilot_script(pilot_script_path)

        # Fetch a small sample of files
        files = await self.dbs.get_files(workflow.input_dataset, limit=5)
        if not files:
            logger.warning("No files found for pilot of workflow %s", workflow.id)
            return

        # Resolve sandbox path from workflow config
        config = workflow.config_data or {}
        sandbox_path = config.get("sandbox_path", "")

        pilot_config = _compute_pilot_config(config, self.settings)

        pilot_sub = _generate_pilot_submit(
            submit_dir=submit_dir,
            sandbox_path=sandbox_path or workflow.sandbox_url,
            input_files=[f["logical_file_name"] for f in files],
            pilot_config=pilot_config,
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
        from wms2.core.pilot_runner import parse_pilot_report
        data = parse_pilot_report(path)
        return PilotMetrics.from_json(data)

    async def handle_pilot_completion(self, workflow, report_path: str) -> Any:
        """Parse pilot metrics and proceed to production DAG planning.

        Pilot results are stored for reference but don't drive merge group
        sizing — that uses fixed jobs_per_work_unit. Measured memory overrides
        the request hint if available.
        """
        from wms2.core.pilot_runner import parse_pilot_report

        config = workflow.config_data or {}
        metrics = PilotMetrics.from_request(config)

        # Store pilot results for reference; override memory if measured
        try:
            pilot_data = parse_pilot_report(report_path)
            summary = pilot_data.get("summary", {})
            if summary.get("peak_rss_mb", 0) > 0:
                metrics.memory_peak_mb = int(summary["peak_rss_mb"])
            if summary.get("per_output_module"):
                metrics.per_output_module = summary["per_output_module"]
            metrics.steps = pilot_data.get("steps")
        except Exception as exc:
            logger.warning("Pilot report parse failed (%s), using request hints", exc)
            pilot_data = {}

        await self.db.update_workflow(
            workflow.id,
            pilot_metrics=pilot_data,
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

        # Plan merge groups (fixed job count per work unit)
        merge_groups = _plan_merge_groups(
            nodes,
            jobs_per_group=self.settings.jobs_per_work_unit,
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
        ncpus = config.get("multicore", 0)
        if ncpus:
            resource_params["ncpus"] = int(ncpus)

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
            local_pfn_prefix=self.settings.local_pfn_prefix,
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
    jobs_per_group: int,
) -> list[PlanningMergeGroup]:
    """Group processing nodes into fixed-size merge groups."""
    if not nodes:
        return []

    groups: list[PlanningMergeGroup] = []
    for i in range(0, len(nodes), jobs_per_group):
        chunk = nodes[i:i + jobs_per_group]
        mg = PlanningMergeGroup(group_index=len(groups))
        mg.processing_nodes = chunk
        groups.append(mg)

    return groups


# ── Pilot Config from Request Hints ────────────────────────────


def _compute_pilot_config(config: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Compute pilot parameters from request performance hints.

    The pilot is a functional smoke test, not a measurement tool. All
    planning parameters come from the request spec. We just need enough
    events to verify the chain runs successfully.
    """
    time_per_event = float(config.get("time_per_event", 0))
    filter_eff = float(config.get("filter_efficiency", 1.0))

    # Enough events to get ~10 through the filter (functional test)
    if filter_eff > 0 and filter_eff < 1.0:
        initial_events = max(10, min(int(10 / filter_eff), 10000))
    else:
        initial_events = settings.pilot_initial_events

    # Timeout based on expected time, 5x safety margin
    if time_per_event > 0:
        timeout = max(600, int(initial_events * time_per_event * 5))
    else:
        timeout = settings.pilot_step_timeout

    return {
        "initial_events": initial_events,
        "timeout": timeout,
        "ncpus": int(config.get("multicore", 0)),
        "memory_mb": int(config.get("memory_mb", 0)),
    }


# ── DAG File Generation (Appendix C format) ────────────────────


def _generate_dag_files(
    submit_dir: str,
    workflow_id: str,
    merge_groups: list[PlanningMergeGroup],
    sandbox_url: str,
    category_throttles: dict[str, int],
    executables: dict[str, str] | None = None,
    output_datasets: list[dict] | None = None,
    local_pfn_prefix: str = "",
    sandbox_path: str | None = None,
    resource_params: dict[str, int] | None = None,
) -> str:
    """Generate all DAG files on disk. Returns path to outer workflow.dag."""
    submit_path = Path(submit_dir)

    # Write shared config and scripts
    _write_file(
        str(submit_path / "dagman.config"),
        "DAGMAN_MAX_RESCUE_NUM = 10\nDAGMAN_USER_LOG_SCAN_INTERVAL = 5\n",
    )
    _write_elect_site_script(str(submit_path / "elect_site.sh"))
    _write_pin_site_script(str(submit_path / "pin_site.sh"))
    _write_post_script(str(submit_path / "post_script.sh"))
    _write_proc_script(str(submit_path / "wms2_proc.sh"))
    _write_merge_script(str(submit_path / "wms2_merge.py"))
    _write_cleanup_script(str(submit_path / "wms2_cleanup.py"))

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
            local_pfn_prefix=local_pfn_prefix,
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
    local_pfn_prefix: str = "",
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
    if cleanup_exe == "/bin/true":
        cleanup_exe = str(submit_dir / "wms2_cleanup.py")

    # Resource parameters
    rp = resource_params or {}
    memory_mb = rp.get("memory_mb", 0)
    disk_kb = rp.get("disk_kb", 0)
    ncpus = rp.get("ncpus", 0)

    # Build transfer_input_files list for processing nodes
    proc_transfer_files: list[str] = []
    if sandbox_path and os.path.isfile(sandbox_path):
        proc_transfer_files.append(sandbox_path)
        # Extract manifest.json to group dir so merge job can find CMSSW info
        import tarfile
        try:
            with tarfile.open(sandbox_path, "r:gz") as tf:
                for member in tf.getmembers():
                    if member.name == "manifest.json" or member.name.endswith("/manifest.json"):
                        member.name = "manifest.json"
                        tf.extract(member, str(group_dir))
                        break
        except Exception:
            pass  # merge will fall back to copy mode without hadd

        # Override per-step multicore in manifest if ncpus differs from sandbox default
        manifest_path = group_dir / "manifest.json"
        if ncpus and manifest_path.is_file():
            try:
                import json as _json
                with open(manifest_path) as _f:
                    _manifest = _json.load(_f)
                changed = False
                for step in _manifest.get("steps", []):
                    if step.get("multicore", 0) != ncpus:
                        step["multicore"] = ncpus
                        changed = True
                if changed:
                    with open(manifest_path, "w") as _f:
                        _json.dump(_manifest, _f, indent=2)
            except Exception:
                pass

    # Pass X509 proxy to jobs if available
    proc_env: dict[str, str] = {}
    x509_proxy = os.environ.get("X509_USER_PROXY", "")
    if x509_proxy and os.path.isfile(x509_proxy):
        proc_env["X509_USER_PROXY"] = x509_proxy
    x509_cert_dir = os.environ.get("X509_CERT_DIR", "")
    if x509_cert_dir:
        proc_env["X509_CERT_DIR"] = x509_cert_dir
    siteconfig = os.environ.get("SITECONFIG_PATH", "")
    if siteconfig and os.path.isdir(siteconfig):
        proc_env["SITECONFIG_PATH"] = siteconfig

    # Write output_info.json — shared by proc (stage-out) and merge (read input/write output)
    output_info_path = None
    if output_datasets and local_pfn_prefix:
        output_info = {
            "output_datasets": [
                {
                    "dataset_name": d.get("dataset_name", ""),
                    "merged_lfn_base": d.get("merged_lfn_base", ""),
                    "unmerged_lfn_base": d.get("unmerged_lfn_base", ""),
                    "data_tier": d.get("data_tier", ""),
                }
                for d in output_datasets
            ],
            "local_pfn_prefix": local_pfn_prefix,
            "group_index": merge_group.group_index,
            "max_merge_size": rp.get("max_merge_size", 4 * 1024**3),
        }
        output_info_path = str(group_dir / "output_info.json")
        _write_file(output_info_path, json.dumps(output_info, indent=2))

    # Add output_info.json to proc transfer files so proc jobs can stage out
    if output_info_path and os.path.isfile(output_info_path):
        proc_transfer_files.append(output_info_path)

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
        if output_info_path:
            proc_args += f" --output-info {os.path.basename(output_info_path)}"
        if node.first_event > 0:
            proc_args += f" --first-event {node.first_event}"
        if node.last_event > 0:
            proc_args += f" --last-event {node.last_event}"
        if node.events_per_job > 0:
            proc_args += f" --events-per-job {node.events_per_job}"
        if ncpus > 0:
            proc_args += f" --ncpus {ncpus}"

        _write_submit_file(
            str(group_dir / f"{node_name}.sub"),
            executable=proc_exe,
            arguments=proc_args,
            description=f"processing node {node.node_index}",
            desired_sites=node.primary_location,
            memory_mb=memory_mb,
            disk_kb=disk_kb,
            ncpus=ncpus,
            transfer_input_files=proc_transfer_files or None,
            environment=proc_env or None,
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
    merge_args = f"--sandbox {sandbox_ref}"
    if output_info_path:
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
    cleanup_args = ""
    if output_info_path:
        cleanup_args = f"--output-info {output_info_path}"
    _write_submit_file(
        str(group_dir / "cleanup.sub"),
        executable=cleanup_exe,
        arguments=cleanup_args,
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
    ncpus: int = 0,
    transfer_input_files: list[str] | None = None,
    environment: dict[str, str] | None = None,
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
    if ncpus > 0:
        lines.append(f"request_cpus = {ncpus}")
    if memory_mb > 0:
        lines.append(f"request_memory = {memory_mb}")
    if disk_kb > 0:
        lines.append(f"request_disk = {disk_kb}")
    if environment:
        env_str = " ".join(f"{k}={v}" for k, v in environment.items())
        lines.append(f'environment = "{env_str}"')
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
    sandbox_path: str,
    input_files: list[str],
    pilot_config: dict[str, Any] | None = None,
) -> str:
    pc = pilot_config or {}
    input_str = ",".join(input_files)
    sandbox_basename = os.path.basename(sandbox_path) if sandbox_path else "sandbox.tar.gz"
    args = (
        f"--sandbox {sandbox_basename} --input {input_str}"
        f" --initial-events {pc.get('initial_events', 200)}"
        f" --timeout {pc.get('timeout', 900)}"
    )
    transfer_inputs = ["wms2_pilot.py"]
    if sandbox_path and os.path.isfile(sandbox_path):
        transfer_inputs.append(sandbox_path)
    lines = [
        "# WMS2 pilot job",
        "universe = vanilla",
        "executable = wms2_pilot.py",
        f"arguments = {args}",
        "output = pilot.out",
        "error = pilot.err",
        "log = pilot.log",
    ]
    ncpus = pc.get("ncpus", 0)
    if ncpus > 0:
        lines.append(f"request_cpus = {ncpus}")
    memory_mb = pc.get("memory_mb", 0)
    if memory_mb > 0:
        lines.append(f"request_memory = {memory_mb}")
    lines.extend([
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT",
        f"transfer_input_files = {','.join(transfer_inputs)}",
        "transfer_output_files = pilot_metrics.json",
        "queue 1",
    ])
    return "\n".join(lines) + "\n"


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
    CMSSW mode handles per-step CMSSW/ScramArch with apptainer container support
    for cross-OS execution (e.g. el8 CMSSW on el9 host).
    """
    _write_file(path, r'''#!/bin/bash
# wms2_proc.sh — WMS2 processing job wrapper
# Supports CMSSW mode (per-step cmsRun with apptainer), synthetic mode
# (sized output), and pilot mode (iterative measurement).
set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# ── Argument parsing ──────────────────────────────────────────
SANDBOX=""
INPUT_LFNS=""
FIRST_EVENT=0
LAST_EVENT=0
EVENTS_PER_JOB=0
NODE_INDEX=0
PILOT_MODE=false
OUTPUT_INFO=""
OVERRIDE_NCPUS=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sandbox)    SANDBOX="$2";        shift 2 ;;
        --input)      INPUT_LFNS="$2";     shift 2 ;;
        --first-event) FIRST_EVENT="$2";   shift 2 ;;
        --last-event)  LAST_EVENT="$2";    shift 2 ;;
        --events-per-job) EVENTS_PER_JOB="$2"; shift 2 ;;
        --node-index) NODE_INDEX="$2";     shift 2 ;;
        --output-info) OUTPUT_INFO="$2";   shift 2 ;;
        --pilot)      PILOT_MODE=true;     shift   ;;
        --ncpus)      OVERRIDE_NCPUS="$2"; shift 2 ;;
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

# Adaptive tuning: apply per-step nThreads from replan
if [[ -f manifest_tuned.json ]]; then
    echo "Applying adaptive manifest (per-step nThreads tuning)"
    cp manifest_tuned.json manifest.json
fi

# Override per-step multicore if --ncpus was passed
# Skip when adaptive tuning is active (manifest_tuned.json has per-step values)
if [[ "$OVERRIDE_NCPUS" -gt 0 && -f manifest.json && ! -f manifest_tuned.json ]]; then
    echo "Overriding manifest multicore to $OVERRIDE_NCPUS"
    python3 -c "
import json
m = json.load(open('manifest.json'))
for s in m.get('steps', []):
    s['multicore'] = $OVERRIDE_NCPUS
json.dump(m, open('manifest.json', 'w'), indent=2)
"
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

# ── Host OS detection ─────────────────────────────────────────
detect_host_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        case "$VERSION_ID" in
            8*) echo "el8" ;;
            9*) echo "el9" ;;
            *)  echo "unknown" ;;
        esac
    else
        echo "unknown"
    fi
}

# ── Container resolution ──────────────────────────────────────
resolve_container() {
    local scram_arch="$1"
    local arch_os="${scram_arch%%_*}"   # "el8" from "el8_amd64_gcc10"
    local HOST_OS
    HOST_OS=$(detect_host_os)

    if [[ "$arch_os" == "$HOST_OS" ]]; then
        echo ""  # native execution
        return
    fi

    # Find container image on CVMFS
    for variant in "x86_64" "amd64"; do
        local img="/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/${arch_os}:${variant}"
        if [[ -d "$img" ]]; then
            echo "$img"
            return
        fi
    done
    echo "ERROR: No container for $scram_arch (need $arch_os, host is $HOST_OS)" >&2
    return 1
}

# ── CMSSW step execution: native ──────────────────────────────
CURRENT_CMSSW=""
CURRENT_ARCH=""

run_step_native() {
    local cmssw="$1" arch="$2" cmsrun_args="$3"
    if [[ "$cmssw" != "$CURRENT_CMSSW" || "$arch" != "$CURRENT_ARCH" ]]; then
        export SCRAM_ARCH="$arch"
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        if [[ ! -d "$cmssw/src" ]]; then
            scramv1 project CMSSW "$cmssw"
        fi
        cd "$cmssw/src" && eval $(scramv1 runtime -sh) && cd "$WORK_DIR"
        # CMSSW >=14.x reads SITECONFIG_PATH; <14.x reads CMS_PATH
        local SITE_CFG="${SITECONFIG_PATH:-/opt/cms/siteconf}"
        export SITECONFIG_PATH="$SITE_CFG"
        export CMS_PATH="$SITE_CFG"
        CURRENT_CMSSW="$cmssw"
        CURRENT_ARCH="$arch"
    fi
    cmsRun $cmsrun_args
}

# ── CMSSW step execution: apptainer ───────────────────────────
run_step_apptainer() {
    local container="$1" cmssw="$2" arch="$3" cmsrun_args="$4"

    # Resolve siteconfig path: default to /opt/cms/siteconf if SITECONFIG_PATH unset
    local SITE_CFG="${SITECONFIG_PATH:-/opt/cms/siteconf}"

    # Copy siteconf files into the execute directory so they're available
    # inside the container via the working-directory bind mount.
    # Layout for SITECONFIG_PATH (CMSSW >=14.x): _siteconf/JobConfig/site-local-config.xml
    mkdir -p "$WORK_DIR/_siteconf/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
    [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
    [[ -f "$SITE_CFG/storage.json" ]] && cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
    # Layout for CMS_PATH (CMSSW <14.x): _siteconf/SITECONF/local/JobConfig/site-local-config.xml
    mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
    [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"

    cat > _step_runner.sh <<STEPEOF
#!/bin/bash
set -e
# CMSSW >=14.x reads SITECONFIG_PATH; <14.x reads CMS_PATH + /SITECONF/local/
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf

export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ ! -d $cmssw/src ]]; then
    scramv1 project CMSSW $cmssw
fi
cd $cmssw/src && eval \$(scramv1 runtime -sh) && cd $WORK_DIR
cmsRun $cmsrun_args
STEPEOF
    chmod +x _step_runner.sh

    # Bind paths: CVMFS, working dir, temp, and optional site config / credentials
    local BIND="/cvmfs,/tmp,/dev/shm,$(pwd)"
    [[ -d /mnt/creds ]]        && BIND="$BIND,/mnt/creds"
    [[ -d /mnt/shared ]]       && BIND="$BIND,/mnt/shared"
    [[ -d "$SITE_CFG" ]]       && BIND="$BIND,$SITE_CFG"
    export APPTAINER_BINDPATH="$BIND"
    apptainer exec --no-home "$container" bash "$(pwd)/_step_runner.sh"
}

# ── Stage-out to unmerged site storage ─────────────────────────
stage_out_to_unmerged() {
    if [[ -z "$OUTPUT_INFO" || ! -f "$OUTPUT_INFO" ]]; then
        echo "No output_info.json — skipping stage-out"
        return 0
    fi

    echo ""
    echo "--- Staging out to unmerged site storage ---"
    python3 -c "
import json, os, shutil, glob

with open('$OUTPUT_INFO') as f:
    info = json.load(f)

pfn_prefix = info.get('local_pfn_prefix', '/mnt/shared')
group_index = info['group_index']
datasets = info.get('output_datasets', [])

# Read output manifest to get file -> tier mapping
manifest_file = 'proc_${NODE_INDEX}_outputs.json'
if not os.path.isfile(manifest_file):
    # Synthetic mode: look for proc_*_output.root files
    synth_files = glob.glob('proc_${NODE_INDEX}_*.root')
    if not synth_files:
        print('No output manifest or synthetic files, skipping stage-out')
        exit(0)
    # Build a simple manifest for synthetic files
    file_map = {}
    for sf in synth_files:
        # Use first dataset's tier as default
        tier = datasets[0].get('data_tier', 'unknown') if datasets else 'unknown'
        file_map[sf] = {'tier': tier, 'step_index': -1}
else:
    with open(manifest_file) as f:
        file_map = json.load(f)

# Build tier -> unmerged LFN base lookup
tier_to_unmerged = {}
for ds in datasets:
    tier_to_unmerged[ds['data_tier']] = ds.get('unmerged_lfn_base', '')

for filename, finfo in file_map.items():
    if not os.path.isfile(filename):
        print(f'WARNING: {filename} not found, skipping')
        continue
    tier = finfo['tier'] if isinstance(finfo, dict) else finfo
    unmerged_base = tier_to_unmerged.get(tier, '')
    if not unmerged_base:
        # Try EDM variant: NANOEDMAODSIM files match NANOAODSIM dataset
        for ds_tier, base in tier_to_unmerged.items():
            if tier.upper().replace(\"EDM\", \"\") == ds_tier.upper():
                unmerged_base = base
                break
    if not unmerged_base:
        # Fallback: longer tier must contain shorter, with small length diff
        for ds_tier, base in tier_to_unmerged.items():
            t_up, d_up = tier.upper(), ds_tier.upper()
            longer, shorter = (t_up, d_up) if len(t_up) >= len(d_up) else (d_up, t_up)
            if shorter in longer and len(longer) - len(shorter) <= 3:
                unmerged_base = base
                break
    if not unmerged_base:
        print(f'WARNING: No unmerged LFN for tier {tier}, skipping {filename}')
        continue

    lfn_dir = f'{unmerged_base}/{group_index:06d}'
    pfn_dir = os.path.join(pfn_prefix, lfn_dir.lstrip('/'))
    os.makedirs(pfn_dir, exist_ok=True)
    dest = os.path.join(pfn_dir, filename)
    shutil.copy2(filename, dest)
    print(f'Staged: {filename} -> {dest}')

# Also stage the output manifest and metrics file
if os.path.isfile(manifest_file):
    for ds in datasets:
        unmerged_base = ds.get('unmerged_lfn_base', '')
        if unmerged_base:
            pfn_dir = os.path.join(pfn_prefix, f'{unmerged_base}/{group_index:06d}'.lstrip('/'))
            os.makedirs(pfn_dir, exist_ok=True)
            dest = os.path.join(pfn_dir, 'proc_${NODE_INDEX}_outputs.json')
            shutil.copy2(manifest_file, dest)
            # Stage metrics file alongside manifest
            metrics_file = 'proc_${NODE_INDEX}_metrics.json'
            if os.path.isfile(metrics_file):
                mdest = os.path.join(pfn_dir, metrics_file)
                shutil.copy2(metrics_file, mdest)
                print(f'Staged metrics: {metrics_file} -> {mdest}')
            break
"
}

# ── Parse FJR output file path ────────────────────────────────
parse_fjr_output() {
    local fjr_path="$1"
    python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('$fjr_path')
    candidates = []
    for f in tree.findall('.//File'):
        pfn = f.findtext('PFN', '')
        if pfn.startswith('file:'): pfn = pfn[5:]
        if not pfn: continue
        label = f.findtext('ModuleLabel', '')
        candidates.append((pfn, label))
    for pfn, label in candidates:
        if 'LHE' not in label.upper():
            print(pfn); break
    else:
        if candidates: print(candidates[0][0])
except Exception:
    pass
" 2>/dev/null
}

# ── PSet injection for parallel step 0 instances ──────────────
inject_pset_parallel() {
    local pset="$1" first_event="$2" max_events="$3" nthreads="$4"
    python3 -c "
pset_path = '$pset'
lines = ['', '# --- WMS2 parallel instance PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')
lines.append('process.maxEvents.input = cms.untracked.int32($max_events)')
lines.append('process.source.firstEvent = cms.untracked.uint32($first_event)')
lines.append('if hasattr(process, \"externalLHEProducer\"):')
lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32($max_events)')
if $nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32($nthreads)')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')
with open(pset_path, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"
}

# ── Parallel step 0 execution ─────────────────────────────────
run_step0_parallel() {
    local n_par="$1" nthreads="$2" cmssw="$3" arch="$4" pset="$5" step_name="$6" use_tmpfs="$7"
    local events_per_inst=$((EVENTS_PER_JOB / n_par))
    local remainder=$((EVENTS_PER_JOB % n_par))

    echo "  Parallel execution: $n_par instances, $events_per_inst events each"

    # Resolve container once
    local CONTAINER
    CONTAINER=$(resolve_container "$arch")

    # Setup CMSSW project once (before forking instances)
    if [[ -z "$CONTAINER" ]]; then
        # Native: setup env in current shell
        export SCRAM_ARCH="$arch"
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        if [[ ! -d "$WORK_DIR/$cmssw/src" ]]; then
            cd "$WORK_DIR" && scramv1 project CMSSW "$cmssw"
        fi
        cd "$WORK_DIR/$cmssw/src" && eval $(scramv1 runtime -sh) && cd "$WORK_DIR"
        export SITECONFIG_PATH="${SITECONFIG_PATH:-/opt/cms/siteconf}"
        export CMS_PATH="$SITECONFIG_PATH"
    else
        # Apptainer: prepare siteconf and create CMSSW project via container
        local SITE_CFG="${SITECONFIG_PATH:-/opt/cms/siteconf}"
        mkdir -p "$WORK_DIR/_siteconf/JobConfig"
        cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
        [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
        [[ -f "$SITE_CFG/storage.json" ]] && cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
        mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
        cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
        [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"

        cat > "$WORK_DIR/_setup_cmssw.sh" <<SETUPEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $WORK_DIR
if [[ ! -d $cmssw/src ]]; then
    scramv1 project CMSSW $cmssw
fi
SETUPEOF
        chmod +x "$WORK_DIR/_setup_cmssw.sh"
        local BIND="/cvmfs,/tmp,/dev/shm,$WORK_DIR"
        [[ -d /mnt/creds ]] && BIND="$BIND,/mnt/creds"
        [[ -d /mnt/shared ]] && BIND="$BIND,/mnt/shared"
        [[ -d "$SITE_CFG" ]] && BIND="$BIND,$SITE_CFG"
        export APPTAINER_BINDPATH="$BIND"
        echo "  Setting up CMSSW project via apptainer..."
        apptainer exec --no-home "$CONTAINER" bash "$WORK_DIR/_setup_cmssw.sh"
    fi

    # Instance base directory: tmpfs (/dev/shm) avoids virtiofs/FUSE
    # file descriptor limits during gridpack extraction (29K+ files each).
    # ExternalLHEProducer extracts gridpacks into cmsRun's CWD, so placing
    # the CWD on tmpfs keeps extraction entirely in RAM.
    local TMPFS_BASE
    if [[ "$use_tmpfs" == "true" ]]; then
        TMPFS_BASE="/dev/shm/wms2_step0_$$"
        echo "  Using tmpfs for parallel instances: $TMPFS_BASE"
    else
        TMPFS_BASE="$WORK_DIR/_step0_parallel"
        echo "  Using disk for parallel instances: $TMPFS_BASE"
    fi
    mkdir -p "$TMPFS_BASE"
    trap "rm -rf $TMPFS_BASE 2>/dev/null" EXIT

    # Create instance directories and fork
    local pids=()
    local pset_base
    pset_base=$(basename "$pset")
    for inst in $(seq 0 $((n_par - 1))); do
        local idir="$TMPFS_BASE/step1_inst${inst}"
        local odir="$WORK_DIR/step1_inst${inst}"
        mkdir -p "$idir" "$odir"
        # Symlink CMSSW project and archive files (gridpacks)
        ln -sf "$WORK_DIR/$cmssw" "$idir/$cmssw"
        ln -sf "$WORK_DIR"/*.xz "$WORK_DIR"/*.gz "$idir/" 2>/dev/null || true
        # Copy PSet (needs per-instance modification)
        cp "$WORK_DIR/$pset" "$idir/$pset_base"

        # Compute per-instance event range
        local inst_first=$((FIRST_EVENT + inst * events_per_inst))
        local inst_events=$events_per_inst
        [[ $inst -eq $((n_par - 1)) ]] && inst_events=$((events_per_inst + remainder))

        # Inject per-instance params into copied PSet
        inject_pset_parallel "$idir/$pset_base" "$inst_first" "$inst_events" "$nthreads"

        # Launch instance
        if [[ -z "$CONTAINER" ]]; then
            ( cd "$idir" && cmsRun -j report_step1.xml "$pset_base" ) &
        else
            cat > "$idir/_step_runner.sh" <<INSTEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $idir/$cmssw/src && eval \$(scramv1 runtime -sh) && cd $idir
cmsRun -j report_step1.xml $pset_base
INSTEOF
            chmod +x "$idir/_step_runner.sh"
            ( apptainer exec --no-home "$CONTAINER" bash "$idir/_step_runner.sh" ) &
        fi
        pids+=($!)
        echo "  Instance $inst: pid=${pids[-1]} firstEvent=$inst_first maxEvents=$inst_events"
    done

    # Wait for all instances
    local failed=0
    for pid in "${pids[@]}"; do
        wait "$pid" || failed=$((failed + 1))
    done
    if [[ $failed -gt 0 ]]; then
        echo "ERROR: $failed/$n_par parallel instances failed" >&2
        exit 1
    fi
    echo "  All $n_par instances completed successfully"

    # Collect outputs: copy from tmpfs back to WORK_DIR
    PREV_OUTPUT=""
    for inst in $(seq 0 $((n_par - 1))); do
        local idir="$TMPFS_BASE/step1_inst${inst}"
        local odir="$WORK_DIR/step1_inst${inst}"
        # Copy ROOT outputs and FJR from tmpfs to persistent storage
        cp "$idir"/*.root "$odir/" 2>/dev/null || true
        cp "$idir/report_step1.xml" "$odir/"
        local out
        out=$(parse_fjr_output "$odir/report_step1.xml")
        if [[ -n "$out" ]]; then
            [[ -n "$PREV_OUTPUT" ]] && PREV_OUTPUT="${PREV_OUTPUT},"
            PREV_OUTPUT="${PREV_OUTPUT}${odir}/${out}"
        fi
        cp "$odir/report_step1.xml" "$WORK_DIR/report_step1_inst${inst}.xml"
    done
    echo "  Collected outputs: $PREV_OUTPUT"

    # Clean up instance directories
    rm -rf "$TMPFS_BASE"
    echo "  Cleaned up parallel instance dirs"
}

# ── All-step pipeline split execution ─────────────────────────
run_all_steps_pipeline() {
    local n_pipelines="$1"
    local events_per_pipe=$((EVENTS_PER_JOB / n_pipelines))
    local remainder=$((EVENTS_PER_JOB % n_pipelines))

    echo "--- Pipeline mode: $n_pipelines pipelines ---"
    echo "  Events per pipeline: $events_per_pipe (remainder: $remainder)"

    NUM_STEPS=$(python3 -c "import json; print(len(json.load(open('manifest.json'))['steps']))")
    echo "  Steps: $NUM_STEPS"

    # Read step configs once (shared across pipelines)
    local step_names=()
    local step_cmssw=()
    local step_arch=()
    local step_pset=()
    local step_input_step=()
    local step_nthreads=()
    for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
        local sj
        sj=$(python3 -c "import json; s=json.load(open('manifest.json'))['steps'][$step_idx]; print(json.dumps(s))")
        step_names+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")")
        step_cmssw+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['cmssw_version'])")")
        step_arch+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['scram_arch'])")")
        step_pset+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['pset'])")")
        step_input_step+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin).get('input_step',''))")")
        step_nthreads+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin).get('multicore',1))")")
    done

    # Setup CMSSW project once (first step's version — all steps may share or re-setup)
    local first_cmssw="${step_cmssw[0]}"
    local first_arch="${step_arch[0]}"
    local CONTAINER
    CONTAINER=$(resolve_container "$first_arch")

    if [[ -z "$CONTAINER" ]]; then
        export SCRAM_ARCH="$first_arch"
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        if [[ ! -d "$WORK_DIR/$first_cmssw/src" ]]; then
            cd "$WORK_DIR" && scramv1 project CMSSW "$first_cmssw"
        fi
        export SITECONFIG_PATH="${SITECONFIG_PATH:-/opt/cms/siteconf}"
        export CMS_PATH="$SITECONFIG_PATH"
    else
        local SITE_CFG="${SITECONFIG_PATH:-/opt/cms/siteconf}"
        mkdir -p "$WORK_DIR/_siteconf/JobConfig"
        cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
        [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
        [[ -f "$SITE_CFG/storage.json" ]] && cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
        mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
        cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
        [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"

        # Setup all unique CMSSW versions
        local setup_versions=()
        for si in $(seq 0 $((NUM_STEPS - 1))); do
            local ver="${step_cmssw[$si]}"
            local arch="${step_arch[$si]}"
            local already=0
            for sv in "${setup_versions[@]}"; do
                [[ "$sv" == "$ver" ]] && already=1
            done
            if [[ $already -eq 0 ]]; then
                setup_versions+=("$ver")
                cat > "$WORK_DIR/_setup_cmssw_${ver}.sh" <<SETUPEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $WORK_DIR
if [[ ! -d $ver/src ]]; then
    scramv1 project CMSSW $ver
fi
SETUPEOF
                chmod +x "$WORK_DIR/_setup_cmssw_${ver}.sh"
                local BIND="/cvmfs,/tmp,$WORK_DIR"
                [[ -d /mnt/creds ]] && BIND="$BIND,/mnt/creds"
                [[ -d /mnt/shared ]] && BIND="$BIND,/mnt/shared"
                [[ -d "$SITE_CFG" ]] && BIND="$BIND,$SITE_CFG"
                export APPTAINER_BINDPATH="$BIND"
                echo "  Setting up CMSSW $ver via apptainer..."
                apptainer exec --no-home "$CONTAINER" bash "$WORK_DIR/_setup_cmssw_${ver}.sh"
            fi
        done
    fi

    # Fork N pipelines
    local pids=()
    for pipe in $(seq 0 $((n_pipelines - 1))); do
        local pdir="$WORK_DIR/pipeline_${pipe}"
        mkdir -p "$pdir"

        # Compute per-pipeline event range (step 0 firstEvent partitioning)
        local pipe_first=$((FIRST_EVENT + pipe * events_per_pipe))
        local pipe_events=$events_per_pipe
        [[ $pipe -eq $((n_pipelines - 1)) ]] && pipe_events=$((events_per_pipe + remainder))

        echo "  Pipeline $pipe: firstEvent=$pipe_first maxEvents=$pipe_events"

        # Symlink CMSSW projects and gridpack archives
        for si in $(seq 0 $((NUM_STEPS - 1))); do
            ln -sf "$WORK_DIR/${step_cmssw[$si]}" "$pdir/${step_cmssw[$si]}" 2>/dev/null || true
        done
        ln -sf "$WORK_DIR"/*.xz "$WORK_DIR"/*.gz "$pdir/" 2>/dev/null || true

        # Copy ALL PSet files into pipeline dir, preserving directory structure
        # (PSets may share the same basename, e.g. steps/step1/cfg.py, steps/step2/cfg.py)
        for si in $(seq 0 $((NUM_STEPS - 1))); do
            local pset_dir
            pset_dir=$(dirname "${step_pset[$si]}")
            mkdir -p "$pdir/$pset_dir"
            cp "$WORK_DIR/${step_pset[$si]}" "$pdir/${step_pset[$si]}"
        done

        # Launch pipeline subshell
        (
            cd "$pdir"
            local PREV_OUTPUT=""

            for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
                local step_num=$((step_idx + 1))
                local sname="${step_names[$step_idx]}"
                local cmssw="${step_cmssw[$step_idx]}"
                local arch="${step_arch[$step_idx]}"
                local pset_file="${step_pset[$step_idx]}"
                local input_step="${step_input_step[$step_idx]}"
                local nthreads="${step_nthreads[$step_idx]}"

                echo "  [pipe$pipe] Step $step_num: $sname (nThreads=$nthreads)"

                # Determine input files
                local INJECT_INPUT=""
                if [[ -n "$input_step" && -n "$PREV_OUTPUT" ]]; then
                    IFS=',' read -ra _PO_ARRAY <<< "$PREV_OUTPUT"
                    for _po in "${_PO_ARRAY[@]}"; do
                        [[ -n "$INJECT_INPUT" ]] && INJECT_INPUT="${INJECT_INPUT},"
                        INJECT_INPUT="${INJECT_INPUT}file:${_po}"
                    done
                fi

                # Inject runtime overrides into PSet
                python3 -c "
import os
pset = '$pset_file'
lines = ['', '# --- WMS2 pipeline PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')

inject_input = '$INJECT_INPUT'
if inject_input:
    file_list = inject_input.split(',')
    quoted = ', '.join(repr(f) for f in file_list)
    lines.append('process.source.fileNames = cms.untracked.vstring(' + quoted + ')')
    lines.append('process.source.secondaryFileNames = cms.untracked.vstring()')

step_idx = $step_idx
pipe_events = $pipe_events
pipe_first = $pipe_first
if step_idx == 0 and pipe_events > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(' + str(pipe_events) + ')')
    lines.append('if hasattr(process, \"externalLHEProducer\"):')
    lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32(' + str(pipe_events) + ')')
elif step_idx > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(-1)')
if step_idx == 0 and pipe_first > 0:
    lines.append('process.source.firstEvent = cms.untracked.uint32(' + str(pipe_first) + ')')

nthreads = $nthreads
if nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32(' + str(nthreads) + ')')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')

with open(pset, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"

                # Execute cmsRun
                local CMSRUN_ARGS="-j report_step${step_num}.xml $pset_file"
                local step_container
                step_container=$(resolve_container "$arch")
                local STEP_START
                STEP_START=$(date +%s)

                if [[ -z "$step_container" ]]; then
                    cd "$pdir/$cmssw/src" && eval $(scramv1 runtime -sh) && cd "$pdir"
                    cmsRun $CMSRUN_ARGS
                else
                    cat > "$pdir/_pipe${pipe}_step${step_num}.sh" <<PIPEEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $pdir/$cmssw/src && eval \$(scramv1 runtime -sh) && cd $pdir
cmsRun $CMSRUN_ARGS
PIPEEOF
                    chmod +x "$pdir/_pipe${pipe}_step${step_num}.sh"
                    apptainer exec --no-home "$step_container" bash "$pdir/_pipe${pipe}_step${step_num}.sh"
                fi

                local STEP_RC=$?
                local STEP_END
                STEP_END=$(date +%s)
                local STEP_WALL=$((STEP_END - STEP_START))

                if [[ $STEP_RC -ne 0 ]]; then
                    echo "  [pipe$pipe] ERROR: Step $sname failed with exit code $STEP_RC" >&2
                    exit $STEP_RC
                fi
                echo "  [pipe$pipe] Step $sname completed in ${STEP_WALL}s"

                # Parse FJR output for next step input
                PREV_OUTPUT=$(parse_fjr_output "$pdir/report_step${step_num}.xml")
                echo "  [pipe$pipe] Output: ${PREV_OUTPUT:-none}"
            done
        ) &
        pids+=($!)
    done

    # Wait for all pipelines
    local failed=0
    for pid in "${pids[@]}"; do
        wait "$pid" || failed=$((failed + 1))
    done
    if [[ $failed -gt 0 ]]; then
        echo "ERROR: $failed/$n_pipelines pipelines failed" >&2
        exit 1
    fi
    echo "  All $n_pipelines pipelines completed successfully"

    # Collect outputs: copy FJR + ROOT files from pipeline dirs to WORK_DIR
    for pipe in $(seq 0 $((n_pipelines - 1))); do
        local pdir="$WORK_DIR/pipeline_${pipe}"
        # Copy FJR files with pipeline suffix
        for fjr in "$pdir"/report_step*.xml; do
            if [[ -f "$fjr" ]]; then
                local base
                base=$(basename "$fjr" .xml)
                cp "$fjr" "$WORK_DIR/${base}_pipe${pipe}.xml"
            fi
        done
        # Copy ROOT files with pipeline prefix
        for rf in "$pdir"/*.root; do
            if [[ -f "$rf" ]]; then
                local base
                base=$(basename "$rf")
                cp "$rf" "$WORK_DIR/pipe${pipe}_${base}"
            fi
        done
    done
    echo "  Collected outputs from $n_pipelines pipelines"
}

# ── CMSSW mode (per-step CMSSW/ScramArch with apptainer) ─────
run_cmssw_mode() {
    echo "--- CMSSW mode ---"

    NUM_STEPS=$(python3 -c "import json; print(len(json.load(open('manifest.json'))['steps']))")
    echo "steps: $NUM_STEPS"

    # Check CVMFS
    if [[ ! -d /cvmfs/cms.cern.ch ]]; then
        echo "ERROR: /cvmfs/cms.cern.ch not available" >&2
        exit 1
    fi

    # Pipeline mode dispatch: n_pipelines > 1 runs all steps in parallel pipelines
    N_PIPELINES=$(python3 -c "import json; print(json.load(open('manifest.json')).get('n_pipelines', 1))")
    SPLIT_TMPFS=$(python3 -c "import json; print(str(json.load(open('manifest.json')).get('split_tmpfs', False)).lower())")
    if [[ "$N_PIPELINES" -gt 1 ]]; then
        echo "Pipeline mode: $N_PIPELINES pipelines"
        run_all_steps_pipeline "$N_PIPELINES"
        # Fall through to existing output rename + manifest + metrics code
    else

    # Build xrootd input file list for first step
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

    PREV_OUTPUT=""

    for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
        step_num=$((step_idx + 1))

        # Read step config from manifest
        STEP_JSON=$(python3 -c "import json; s=json.load(open('manifest.json'))['steps'][$step_idx]; print(json.dumps(s))")
        STEP_NAME=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")
        CMSSW_VER=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['cmssw_version'])")
        STEP_ARCH=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['scram_arch'])")
        PSET_PATH=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['pset'])")
        INPUT_STEP=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('input_step',''))")
        NTHREADS=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('multicore',1))")
        N_PARALLEL=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('n_parallel',1))")

        echo ""
        echo "--- Step $step_num: $STEP_NAME ---"
        echo "  CMSSW:  $CMSSW_VER"
        echo "  Arch:   $STEP_ARCH"
        echo "  PSet:   $PSET_PATH"

        # Parallel step 0: fork N_PARALLEL cmsRun instances with firstEvent partitioning
        if [[ $step_idx -eq 0 && "$N_PARALLEL" -gt 1 ]]; then
            echo "  n_parallel: $N_PARALLEL"
            STEP_START=$(date +%s)
            run_step0_parallel "$N_PARALLEL" "$NTHREADS" "$CMSSW_VER" "$STEP_ARCH" "$PSET_PATH" "$STEP_NAME" "$SPLIT_TMPFS"
            STEP_END=$(date +%s)
            STEP_WALL=$((STEP_END - STEP_START))
            echo "  Step $STEP_NAME completed in ${STEP_WALL}s (parallel, $N_PARALLEL instances)"
            continue
        fi

        # Build cmsRun command (no command-line overrides — ConfigCache PSets
        # ignore them; inject everything into PSet via python append).
        CMSRUN_ARGS="-j report_step${step_num}.xml $PSET_PATH"

        # Determine input files for PSet injection
        INJECT_INPUT=""
        if [[ -n "$INPUT_STEP" && -n "$PREV_OUTPUT" ]]; then
            # PREV_OUTPUT may be comma-separated (from parallel step 0)
            INJECT_INPUT=""
            IFS=',' read -ra _PO_ARRAY <<< "$PREV_OUTPUT"
            for _po in "${_PO_ARRAY[@]}"; do
                [[ -n "$INJECT_INPUT" ]] && INJECT_INPUT="${INJECT_INPUT},"
                INJECT_INPUT="${INJECT_INPUT}file:${_po}"
            done
        elif [[ $step_idx -eq 0 && -n "$XROOTD_INPUTS" ]]; then
            INJECT_INPUT="$XROOTD_INPUTS"
        fi

        # Inject runtime overrides into PSet (input files, maxEvents, nThreads)
        python3 -c "
import os
pset = '$PSET_PATH'
lines = ['', '# --- WMS2 runtime PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')

# Input files override
inject_input = '$INJECT_INPUT'
if inject_input:
    file_list = inject_input.split(',')
    quoted = ', '.join(repr(f) for f in file_list)
    lines.append('process.source.fileNames = cms.untracked.vstring(' + quoted + ')')
    lines.append('process.source.secondaryFileNames = cms.untracked.vstring()')

# maxEvents: step 1 uses EVENTS_PER_JOB; steps 2+ use -1 (all input)
# ConfigCache PSets have hardcoded maxEvents from cmsDriver that must be overridden.
step_idx = $step_idx
events_per_job = $EVENTS_PER_JOB
first_event = $FIRST_EVENT
if step_idx == 0 and events_per_job > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(' + str(events_per_job) + ')')
    # Also update ExternalLHEProducer.nEvents if present (wmLHE workflows)
    # The gridpack must produce exactly as many LHE events as cmsRun will consume,
    # otherwise ExternalLHEProducer throws a fatal error at end-of-run.
    lines.append('if hasattr(process, \"externalLHEProducer\"):')
    lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32(' + str(events_per_job) + ')')
elif step_idx > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(-1)')
if step_idx == 0 and first_event > 0:
    lines.append('process.source.firstEvent = cms.untracked.uint32(' + str(first_event) + ')')

# nThreads
nthreads = $NTHREADS
if nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32(' + str(nthreads) + ')')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')

with open(pset, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"
        if [[ -n "$INJECT_INPUT" ]]; then
            echo "  input: $INJECT_INPUT (injected into PSet)"
        fi
        if [[ $step_idx -eq 0 && "$EVENTS_PER_JOB" -gt 0 ]]; then
            echo "  maxEvents: $EVENTS_PER_JOB (injected into PSet)"
        fi
        if [[ "$NTHREADS" -gt 1 ]]; then
            echo "  nThreads: $NTHREADS (injected into PSet)"
        fi

        echo "  cmsRun $CMSRUN_ARGS"
        STEP_START=$(date +%s)

        # Tmpfs for non-parallel step 0: use /dev/shm for gridpack extraction
        # (same benefit as parallel split tmpfs, but for single-instance jobs)
        STEP0_TMPFS=""
        if [[ $step_idx -eq 0 && "$SPLIT_TMPFS" == "true" && "$N_PARALLEL" -le 1 ]]; then
            STEP0_TMPFS="/dev/shm/wms2_step0_$$"
            mkdir -p "$STEP0_TMPFS"
            echo "  Using tmpfs for step 0: $STEP0_TMPFS"
            # Copy PSet to tmpfs working directory
            cp "$PSET_PATH" "$STEP0_TMPFS/"
            # Update CMSRUN_ARGS to use tmpfs PSet path
            CMSRUN_ARGS="-j report_step${step_num}.xml $STEP0_TMPFS/$(basename $PSET_PATH)"
        fi

        # Execute: determine if we need apptainer
        CONTAINER=$(resolve_container "$STEP_ARCH")
        if [[ -n "$STEP0_TMPFS" ]]; then
            # Run from tmpfs directory — gridpack extracts to CWD
            ORIG_DIR="$PWD"
            cd "$STEP0_TMPFS"
            # Update FJR path to be absolute
            CMSRUN_ARGS="-j ${ORIG_DIR}/report_step${step_num}.xml $(basename $PSET_PATH)"
            # Override WORK_DIR so run_step_* functions cd back to tmpfs
            # after CMSSW setup (they do `cd $WORK_DIR` internally)
            SAVED_WORK_DIR="$WORK_DIR"
            WORK_DIR="$STEP0_TMPFS"
            if [[ -z "$CONTAINER" ]]; then
                echo "  execution: native (tmpfs CWD)"
                run_step_native "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS"
            else
                echo "  execution: apptainer (tmpfs CWD, $CONTAINER)"
                run_step_apptainer "$CONTAINER" "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS"
            fi
            STEP_RC=$?
            WORK_DIR="$SAVED_WORK_DIR"
            # Copy ROOT outputs back to work dir
            for rf in *.root; do
                [[ -f "$rf" ]] && cp "$rf" "$ORIG_DIR/"
            done
            cd "$ORIG_DIR"
            rm -rf "$STEP0_TMPFS"
            echo "  Cleaned tmpfs: $STEP0_TMPFS"
        else
            if [[ -z "$CONTAINER" ]]; then
                echo "  execution: native (host OS matches $STEP_ARCH)"
                run_step_native "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS"
            else
                echo "  execution: apptainer ($CONTAINER)"
                run_step_apptainer "$CONTAINER" "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS"
            fi
            STEP_RC=$?
        fi

        STEP_END=$(date +%s)
        STEP_WALL=$((STEP_END - STEP_START))

        if [[ $STEP_RC -ne 0 ]]; then
            echo "ERROR: Step $STEP_NAME (cmsRun) failed with exit code $STEP_RC" >&2
            # Clean up tmpfs on failure too
            [[ -n "$STEP0_TMPFS" && -d "$STEP0_TMPFS" ]] && rm -rf "$STEP0_TMPFS"
            exit $STEP_RC
        fi

        echo "  Step $STEP_NAME completed in ${STEP_WALL}s"

        # Parse FrameworkJobReport for output file (strip file: prefix)
        # When a step produces multiple outputs (e.g. GEN-SIM + LHE), prefer
        # the non-LHE output since the next step needs the physics data.
        PREV_OUTPUT=$(python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('report_step${step_num}.xml')
    candidates = []
    for f in tree.findall('.//File'):
        pfn = f.findtext('PFN', '')
        if pfn.startswith('file:'): pfn = pfn[5:]
        if not pfn: continue
        label = f.findtext('ModuleLabel', '')
        candidates.append((pfn, label))
    # Prefer non-LHE output for next step's input
    for pfn, label in candidates:
        if 'LHE' not in label.upper():
            print(pfn)
            break
    else:
        # All are LHE or no label — take first
        if candidates:
            print(candidates[0][0])
except Exception:
    pass
" 2>/dev/null || true)
        echo "  Output: ${PREV_OUTPUT:-none}"
    done

    fi  # end of sequential (non-pipeline) mode

    # Rename output ROOT files to include node index — prevents collisions
    # when HTCondor transfers outputs from multiple proc jobs to the same dir
    echo ""
    echo "--- Renaming outputs for merge disambiguation ---"
    for f in *.root; do
        if [[ -f "$f" ]]; then
            new_name="proc_${NODE_INDEX}_${f}"
            mv "$f" "$new_name"
            echo "  $f -> $new_name"
        fi
    done

    # Write output manifest — maps renamed files to their tiers and step indices
    # Parse FJRs (report_stepN.xml) to find each output file's data tier + step_index
    python3 -c "
import re, json, os, glob
import xml.etree.ElementTree as ET

# Build mapping: original_filename -> {tier, step_index} from FJR ModuleLabels
orig_to_info = {}
for fjr in sorted(glob.glob('report_step*.xml')):
    try:
        # Extract step number from filename (report_step1.xml -> step_index 0)
        step_num = int(re.search(r'report_step(\d+)', fjr).group(1))
        step_index = step_num - 1
        tree = ET.parse(fjr)
        for fnode in tree.findall('.//File'):
            pfn = fnode.findtext('PFN', '')
            if pfn.startswith('file:'): pfn = pfn[5:]
            pfn = os.path.basename(pfn)
            label = fnode.findtext('ModuleLabel', '')
            # ModuleLabel is like 'RAWSIMoutput', 'LHEoutput', 'MINIAODSIMoutput'
            tier = re.sub(r'output$', '', label, flags=re.IGNORECASE)
            if pfn and tier:
                orig_to_info[pfn] = {'tier': tier, 'step_index': step_index}
    except Exception:
        pass

# Map renamed files -> {tier, step_index}
files = {}
for f in sorted(os.listdir('.')):
    m = re.match(r'proc_\d+_(.+\.root)$', f)
    if m:
        orig = m.group(1)
        info = orig_to_info.get(orig)
        # Pipeline mode: orig may be 'pipe0_filename.root' — strip prefix
        if not info:
            pipe_m = re.match(r'pipe\d+_(.+)$', orig)
            if pipe_m:
                info = orig_to_info.get(pipe_m.group(1))
        if info:
            files[f] = info
        else:
            # Fallback: try to extract from filename pattern (stepN_TIER.root)
            m2 = re.match(r'step(\d+)_(.+)\.root', orig)
            if m2:
                files[f] = {'tier': m2.group(2), 'step_index': int(m2.group(1)) - 1}
            else:
                files[f] = {'tier': 'unknown', 'step_index': -1}

with open('proc_${NODE_INDEX}_outputs.json', 'w') as fh:
    json.dump(files, fh, indent=2)
print('Wrote proc_${NODE_INDEX}_outputs.json:', len(files), 'files')
for fn, info in files.items():
    print(f'  {fn} -> tier={info[\"tier\"]} step_index={info[\"step_index\"]}')
" 2>/dev/null || true

    # Extract per-step resource metrics from FJR XML files
    echo ""
    echo "--- Extracting per-step metrics from FJR ---"
    python3 -c "
import json, os, re
try:
    import xml.etree.ElementTree as ET
except ImportError:
    print('WARNING: xml.etree not available, skipping metrics')
    exit(0)

def parse_fjr_metrics(fjr_path, step_index):
    \"\"\"Extract resource metrics from a single FJR XML file.\"\"\"
    tree = ET.parse(fjr_path)
    root = tree.getroot()
    perf = root.find('.//PerformanceReport/PerformanceSummary')
    if perf is None:
        return None

    metrics = {'step': step_index + 1, 'step_index': step_index}

    # Helper to extract a metric value
    def get_metric(parent_name, metric_name, cast=float):
        for ps in root.findall('.//PerformanceReport/PerformanceSummary'):
            if ps.get('Metric') == parent_name:
                for m in ps.findall('Metric'):
                    if m.get('Name') == metric_name:
                        try:
                            return cast(m.get('Value', 0))
                        except (ValueError, TypeError):
                            return None
        return None

    # Timing
    wall = get_metric('Timing', 'TotalJobTime')
    cpu = get_metric('Timing', 'TotalJobCPU')
    nthreads = get_metric('Timing', 'NumberOfThreads', int)
    throughput = get_metric('Timing', 'EventThroughput')
    avg_ev_time = get_metric('Timing', 'AvgEventTime')

    metrics['wall_time_sec'] = round(wall, 2) if wall is not None else None
    metrics['cpu_time_sec'] = round(cpu, 2) if cpu is not None else None
    metrics['num_threads'] = nthreads
    metrics['throughput_ev_s'] = round(throughput, 4) if throughput is not None else None
    metrics['avg_event_time_sec'] = round(avg_ev_time, 4) if avg_ev_time is not None else None

    # CPU efficiency
    if wall and cpu and nthreads and wall > 0 and nthreads > 0:
        metrics['cpu_efficiency'] = round(cpu / (wall * nthreads), 4)
    else:
        metrics['cpu_efficiency'] = None

    # Memory
    metrics['peak_rss_mb'] = get_metric('ApplicationMemory', 'PeakValueRss')
    metrics['peak_vsize_mb'] = get_metric('ApplicationMemory', 'PeakValueVsize')

    # Events: prefer ProcessingSummary (physics events only, not pileup reads)
    nevents = get_metric('ProcessingSummary', 'NumberEvents', int)
    if nevents is None:
        nevents = 0
        for inp in root.findall('.//InputFile'):
            ev = inp.findtext('EventsRead')
            if ev:
                try:
                    nevents += int(ev)
                except ValueError:
                    pass
        nevents = nevents or None
    metrics['events_processed'] = nevents

    # Time per event
    if wall is not None and nevents and nevents > 0:
        metrics['time_per_event_sec'] = round(wall / nevents, 4)
    else:
        metrics['time_per_event_sec'] = None

    # Storage I/O
    read_mb = get_metric('StorageStatistics', 'Timing-file-read-totalMegabytes')
    write_mb = get_metric('StorageStatistics', 'Timing-file-write-totalMegabytes')
    metrics['read_mb'] = round(read_mb, 2) if read_mb is not None else None
    metrics['write_mb'] = round(write_mb, 2) if write_mb is not None else None

    return metrics

import glob as _gl

all_metrics = []
step_idx = 0
while True:
    fjr = f'report_step{step_idx + 1}.xml'
    inst_fjrs = sorted(_gl.glob(f'report_step{step_idx + 1}_inst*.xml'))
    pipe_fjrs = sorted(_gl.glob(f'report_step{step_idx + 1}_pipe*.xml'))
    if os.path.isfile(fjr):
        try:
            m = parse_fjr_metrics(fjr, step_idx)
            if m:
                all_metrics.append(m)
                print(f'  Step {step_idx + 1}: wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
        except Exception as e:
            print(f'  Step {step_idx + 1}: parse error: {e}')
    elif inst_fjrs:
        for ifjr in inst_fjrs:
            try:
                m = parse_fjr_metrics(ifjr, step_idx)
                if m:
                    all_metrics.append(m)
                    print(f'  Step {step_idx + 1} ({os.path.basename(ifjr)}): wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
            except Exception as e:
                print(f'  Step {step_idx + 1} ({os.path.basename(ifjr)}): parse error: {e}')
    elif pipe_fjrs:
        for pfjr in pipe_fjrs:
            try:
                m = parse_fjr_metrics(pfjr, step_idx)
                if m:
                    all_metrics.append(m)
                    print(f'  Step {step_idx + 1} ({os.path.basename(pfjr)}): wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
            except Exception as e:
                print(f'  Step {step_idx + 1} ({os.path.basename(pfjr)}): parse error: {e}')
    else:
        break
    step_idx += 1

if all_metrics:
    out = 'proc_${NODE_INDEX}_metrics.json'
    with open(out, 'w') as fh:
        json.dump(all_metrics, fh, indent=2)
    print(f'Wrote {out}: {len(all_metrics)} steps')
else:
    print('No FJR metrics extracted')
" 2>/dev/null || true

    # Collect output sizes
    echo ""
    echo "--- Output files ---"
    OUTPUT_SIZES=0
    for f in proc_*.root; do
        if [[ -f "$f" ]]; then
            sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
            echo "  $f: $sz bytes"
            OUTPUT_SIZES=$((OUTPUT_SIZES + sz))
        fi
    done

    # Stage out to unmerged site storage
    stage_out_to_unmerged

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

    # Calculate output size in KB (capped at 10 MB for dev/testing)
    MAX_OUTPUT_KB=10240
    OUTPUT_KB=$(python3 -c "print(min($MAX_OUTPUT_KB, int($EVENTS * $SIZE_PER_EVENT_KB)))")

    # Simulate processing time (capped at 10s for testing)
    SLEEP_TIME=$(python3 -c "print(min(10, $EVENTS * $TIME_PER_EVENT_SEC))")
    echo "output_kb:      $OUTPUT_KB (capped at ${MAX_OUTPUT_KB} KB)"
    echo "sleep_time:     ${SLEEP_TIME}s"
    sleep "$SLEEP_TIME"

    # Create synthetic output file
    OUTPUT_FILE="proc_${NODE_INDEX}_output.root"
    dd if=/dev/urandom of="$OUTPUT_FILE" bs=1024 count="$OUTPUT_KB" 2>/dev/null
    ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo 0)
    echo "output_file:    $OUTPUT_FILE ($ACTUAL_SIZE bytes)"

    # Write a simple output manifest for synthetic mode
    python3 -c "
import json, os
# Determine tier from output_info.json if available
tier = 'unknown'
if os.path.isfile('${OUTPUT_INFO:-}'):
    try:
        with open('${OUTPUT_INFO}') as f:
            info = json.load(f)
        datasets = info.get('output_datasets', [])
        if datasets:
            tier = datasets[0].get('data_tier', 'unknown')
    except Exception:
        pass
files = {}
for f in os.listdir('.'):
    if f.startswith('proc_') and f.endswith('.root'):
        files[f] = {'tier': tier, 'step_index': -1}
if files:
    with open('proc_${NODE_INDEX}_outputs.json', 'w') as fh:
        json.dump(files, fh, indent=2)
    print('Wrote proc_${NODE_INDEX}_outputs.json:', len(files), 'files')
" 2>/dev/null || true

    # Stage out to unmerged site storage
    stage_out_to_unmerged

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
# Trigger CVMFS autofs mount if needed (ls triggers automount, -d alone may not)
ls /cvmfs/cms.cern.ch/ > /dev/null 2>&1 || true

if [[ "$PILOT_MODE" == "true" ]]; then
    run_pilot_mode
elif [[ "$MODE" == "cmssw" ]]; then
    if [[ ! -d /cvmfs/cms.cern.ch ]]; then
        echo "ERROR: CMSSW mode requested but /cvmfs/cms.cern.ch not available" >&2
        exit 1
    fi
    run_cmssw_mode
elif [[ "$MODE" == "synthetic" ]]; then
    run_synthetic_mode
else
    echo "ERROR: Unknown mode: $MODE" >&2
    exit 1
fi
''')
    os.chmod(path, 0o755)


def _write_merge_script(path: str) -> None:
    """Generate a merge script using cmsRun + mergeProcess() for ROOT files.

    Reads proc outputs from unmerged site storage (staged by proc jobs),
    writes merged output to merged site storage. Uses LFN→PFN mapping:
    PFN = local_pfn_prefix + LFN.

    For ROOT files: uses cmsRun with Configuration.DataProcessing.Merge.mergeProcess()
    to produce proper EDM output with provenance. NanoAOD tiers use NanoAODOutputModule,
    DQMIO tiers use DQMRootOutputModule, others use PoolOutputModule with fast cloning.
    Merged output respects a target size limit (max_merge_size from output_info.json).
    Falls back to hadd if CMSSW setup fails.
    For text files: concatenates proc_*.out into merged text output.
    """
    _write_file(path, '''#!/usr/bin/env python3
"""wms2_merge.py — WMS2 merge job.

Reads proc outputs from unmerged site storage, merges them, writes to merged
site storage. LFN→PFN: PFN = local_pfn_prefix + LFN.

Merges using cmsRun + mergeProcess() for proper EDM provenance.
- Standard EDM tiers: PoolOutputModule with fastCloning (default)
- NanoAOD tiers: NanoAODOutputModule
- DQMIO tiers: DQMRootOutputModule
- Text files (proc_*.out): concatenates into merged text output

Respects max_merge_size — produces multiple output files when needed.
Falls back to hadd if cmsRun/mergeProcess is unavailable.
"""
import glob
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timezone

# ── Argument parsing ──────────────────────────────────────────

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
local_pfn_prefix = info.get("local_pfn_prefix", info.get("output_base_dir", "/mnt/shared"))
group_index = info["group_index"]
datasets = info.get("output_datasets", [])
max_merge_size = info.get("max_merge_size", 4 * 1024**3)


# ── Helpers ───────────────────────────────────────────────────

def lfn_to_pfn(pfn_prefix, lfn):
    """Convert LFN to local PFN by prepending site prefix."""
    return os.path.join(pfn_prefix, lfn.lstrip("/"))


def detect_host_os():
    """Detect host OS major version (el8, el9, etc.)."""
    try:
        with open("/etc/os-release") as f:
            for line in f:
                if line.startswith("VERSION_ID="):
                    ver = line.strip().split("=")[1].strip('"').strip("'")
                    major = ver.split(".")[0]
                    return f"el{major}"
    except Exception:
        pass
    return "unknown"


def resolve_container(scram_arch):
    """Find apptainer container for cross-OS execution. Returns path or empty string."""
    arch_os = scram_arch.split("_")[0]
    host_os = detect_host_os()
    if arch_os == host_os:
        return ""
    for variant in ("x86_64", "amd64"):
        img = f"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/{arch_os}:{variant}"
        if os.path.isdir(img):
            return img
    return ""


def batch_by_size(files, max_size):
    """Group files into batches, each <= max_size total bytes."""
    batches = []
    current_batch = []
    current_size = 0
    for f in files:
        fsize = os.path.getsize(f)
        if current_batch and current_size + fsize > max_size:
            batches.append(current_batch)
            current_batch = [f]
            current_size = fsize
        else:
            current_batch.append(f)
            current_size += fsize
    if current_batch:
        batches.append(current_batch)
    return batches


def write_merge_pset(pset_path, input_files, output_file, is_nano=False, is_dqmio=False):
    """Write a cmsRun merge PSet using Configuration.DataProcessing.Merge.mergeProcess()."""
    lines = [
        "import FWCore.ParameterSet.Config as cms",
        "from Configuration.DataProcessing.Merge import mergeProcess",
        "process = mergeProcess(",
    ]
    for f in input_files:
        pfn = f if f.startswith("file:") else f"file:{f}"
        lines.append(f"    {pfn!r},")
    out_pfn = output_file if output_file.startswith("file:") else f"file:{output_file}"
    lines.append(f"    output_file={out_pfn!r},")
    if is_nano:
        lines.append("    mergeNANO=True,")
    if is_dqmio:
        lines.append("    newDQMIO=True,")
    lines.append(")")
    with open(pset_path, "w") as fh:
        fh.write("\\n".join(lines) + "\\n")


def run_cmsrun(pset_path, cmssw_version, scram_arch, work_dir):
    """Run cmsRun with a merge PSet in the appropriate CMSSW environment.

    Returns True on success, False on failure.
    """
    container = resolve_container(scram_arch)

    if container:
        # Apptainer execution for cross-OS (e.g. el8 CMSSW on el9 host)
        site_cfg = os.environ.get("SITECONFIG_PATH", "/opt/cms/siteconf")
        local_siteconf = os.path.join(work_dir, "_siteconf", "JobConfig")
        os.makedirs(local_siteconf, exist_ok=True)
        try:
            shutil.copy2(
                os.path.join(site_cfg, "JobConfig", "site-local-config.xml"),
                local_siteconf,
            )
        except Exception:
            pass
        phedex_src = os.path.join(site_cfg, "PhEDEx")
        if os.path.isdir(phedex_src):
            shutil.copytree(phedex_src, os.path.join(work_dir, "_siteconf", "PhEDEx"),
                            dirs_exist_ok=True)
        storage_json = os.path.join(site_cfg, "storage.json")
        if os.path.isfile(storage_json):
            shutil.copy2(storage_json, os.path.join(work_dir, "_siteconf"))

        # Create SITECONF/local/ layout for CMS_PATH compatibility (CMSSW <14.x)
        local_cms_jobconfig = os.path.join(
            work_dir, "_siteconf", "SITECONF", "local", "JobConfig"
        )
        os.makedirs(local_cms_jobconfig, exist_ok=True)
        src_slc = os.path.join(site_cfg, "JobConfig", "site-local-config.xml")
        if os.path.isfile(src_slc):
            shutil.copy2(src_slc, local_cms_jobconfig)
        local_cms_phedex = os.path.join(
            work_dir, "_siteconf", "SITECONF", "local", "PhEDEx"
        )
        if os.path.isdir(phedex_src):
            shutil.copytree(phedex_src, local_cms_phedex, dirs_exist_ok=True)

        runner_path = os.path.join(work_dir, "_merge_runner.sh")
        with open(runner_path, "w") as f:
            f.write(f"""#!/bin/bash
set -e
export SITECONFIG_PATH={work_dir}/_siteconf
export CMS_PATH={work_dir}/_siteconf
export SCRAM_ARCH={scram_arch}
export X509_USER_PROXY={os.environ.get('X509_USER_PROXY', '')}
export X509_CERT_DIR={os.environ.get('X509_CERT_DIR', '/cvmfs/grid.cern.ch/etc/grid-security/certificates')}
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ ! -d {cmssw_version}/src ]]; then
    scramv1 project CMSSW {cmssw_version}
fi
cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && cd {work_dir}
cmsRun {pset_path}
""")
        os.chmod(runner_path, 0o755)

        bind_paths = "/cvmfs,/tmp," + work_dir
        if os.path.isdir("/mnt/creds"):
            bind_paths += ",/mnt/creds"
        if os.path.isdir("/mnt/shared"):
            bind_paths += ",/mnt/shared"
        if os.path.isdir(site_cfg):
            bind_paths += "," + site_cfg
        env = dict(os.environ, APPTAINER_BINDPATH=bind_paths)
        cmd = ["apptainer", "exec", "--no-home", container, "bash", runner_path]
    else:
        # Native execution
        setup_script = (
            f"source /cvmfs/cms.cern.ch/cmsset_default.sh && "
            f"export SCRAM_ARCH={scram_arch} && "
            f"if [[ ! -d {cmssw_version}/src ]]; then "
            f"scramv1 project CMSSW {cmssw_version}; fi && "
            f"cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && "
            f"cd {work_dir} && "
            f"export SITECONFIG_PATH=${{SITECONFIG_PATH:-/opt/cms/siteconf}} && "
            f"cmsRun {pset_path}"
        )
        cmd = ["bash", "-c", setup_script]
        env = None

    print(f"  Running: cmsRun {os.path.basename(pset_path)} (CMSSW {cmssw_version}, {scram_arch})")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200, env=env)

    if result.stdout:
        for line in result.stdout.strip().split("\\n")[-10:]:
            print(f"  [cmsRun] {line}")
    if result.returncode != 0:
        print(f"  ERROR: cmsRun failed (exit {result.returncode})", file=sys.stderr)
        if result.stderr:
            for line in result.stderr.strip().split("\\n")[-20:]:
                print(f"  [stderr] {line}", file=sys.stderr)
        return False

    return True


def merge_root_with_hadd(root_files, out_file, cmssw_version, scram_arch):
    """Fallback: merge ROOT files using hadd from CMSSW environment.

    Runs hadd inside the full CMSSW runtime environment (with apptainer
    when cross-OS) so that all shared libraries (libtbb, ROOT, etc.) are
    available.
    """
    file_args = " ".join(shlex.quote(f) for f in root_files)
    hadd_script = (
        f"source /cvmfs/cms.cern.ch/cmsset_default.sh && "
        f"export SCRAM_ARCH={scram_arch} && "
        f"if [[ ! -d {cmssw_version}/src ]]; then "
        f"scramv1 project CMSSW {cmssw_version}; fi && "
        f"cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && cd - >/dev/null && "
        f"hadd -f {shlex.quote(out_file)} {file_args}"
    )

    container = resolve_container(scram_arch)
    if container:
        bind_paths = "/cvmfs,/tmp," + os.getcwd()
        if os.path.isdir("/mnt/shared"):
            bind_paths += ",/mnt/shared"
        env = dict(os.environ, APPTAINER_BINDPATH=bind_paths)
        cmd = ["apptainer", "exec", "--no-home", container, "bash", "-c", hadd_script]
    else:
        cmd = ["bash", "-c", hadd_script]
        env = None

    print(f"  Fallback: hadd -f {out_file} ({len(root_files)} inputs)")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600, env=env)

    if result.stdout:
        for line in result.stdout.strip().split("\\n")[-5:]:
            print(f"  [hadd] {line}")
    if result.returncode != 0:
        print(f"  ERROR: hadd failed (exit {result.returncode})", file=sys.stderr)
        if result.stderr:
            for line in result.stderr.strip().split("\\n")[-10:]:
                print(f"  [stderr] {line}", file=sys.stderr)
        return False
    return True


def merge_root_tier(tier, tier_files, tier_step_index, out_dir, manifest, work_dir):
    """Merge ROOT files for a single tier using cmsRun + mergeProcess().

    Batches files by max_merge_size and produces multiple outputs if needed.
    Falls back to hadd if cmsRun fails.
    """
    # Determine CMSSW version/arch for this tier's step
    steps = manifest.get("steps", [])
    cmssw_version = ""
    scram_arch = ""
    if 0 <= tier_step_index < len(steps):
        cmssw_version = steps[tier_step_index].get("cmssw_version", "")
        scram_arch = steps[tier_step_index].get("scram_arch", "")
    # Fallback to last keep_output step or top-level manifest fields
    if not cmssw_version:
        for s in reversed(steps):
            if s.get("keep_output", False):
                cmssw_version = s.get("cmssw_version", "")
                scram_arch = s.get("scram_arch", "")
                if cmssw_version:
                    break
    if not cmssw_version:
        cmssw_version = manifest.get("cmssw_version", "")
        scram_arch = manifest.get("scram_arch", "")

    is_nano = "NANO" in tier.upper()
    is_dqmio = "DQMIO" in tier.upper()

    # NANOEDMAODSIM → NANOAODSIM: mergeNANO=True converts EDM nano to flat nano
    output_tier = tier
    if is_nano and "EDM" in tier.upper():
        output_tier = tier.replace("EDM", "").replace("edm", "")

    # Check if cmsRun merge is possible
    has_cvmfs = os.path.isdir("/cvmfs/cms.cern.ch")
    can_cmsrun = bool(cmssw_version) and has_cvmfs

    # Batch files by target merge size
    batches = batch_by_size(tier_files, max_merge_size)
    print(f"  {len(batches)} merge batch(es) for tier {tier} "
          f"(max_merge_size={max_merge_size / (1024**3):.1f} GB)")

    merged_files = []
    for batch_idx, batch in enumerate(batches):
        suffix = f"_{batch_idx}" if len(batches) > 1 else ""
        out_file = os.path.join(out_dir, f"merged_{output_tier}{suffix}.root")

        if len(batch) == 1:
            # Single file — just copy, no merge needed
            shutil.copy2(batch[0], out_file)
            size = os.path.getsize(out_file)
            print(f"  Batch {batch_idx}: single file copied -> {out_file} ({size} bytes)")
            merged_files.append(out_file)
            continue

        print(f"  Batch {batch_idx}: {len(batch)} files")

        if can_cmsrun:
            pset_path = os.path.join(work_dir, f"merge_cfg_{tier}_{batch_idx}.py")
            write_merge_pset(pset_path, batch, out_file, is_nano=is_nano, is_dqmio=is_dqmio)

            success = run_cmsrun(pset_path, cmssw_version, scram_arch, work_dir)
            if success and os.path.isfile(out_file):
                size = os.path.getsize(out_file)
                print(f"  Batch {batch_idx}: cmsRun merge -> {out_file} ({size} bytes)")
                merged_files.append(out_file)
                continue
            else:
                print(f"  WARNING: cmsRun merge failed for batch {batch_idx}, trying hadd fallback")

        # Fallback to hadd
        if cmssw_version and has_cvmfs:
            success = merge_root_with_hadd(batch, out_file, cmssw_version, scram_arch)
            if success and os.path.isfile(out_file):
                size = os.path.getsize(out_file)
                print(f"  Batch {batch_idx}: hadd fallback -> {out_file} ({size} bytes)")
                merged_files.append(out_file)
                continue

        # Last resort: copy files individually
        print(f"  WARNING: No merge method available, copying {len(batch)} files to {out_dir}")
        for rf in batch:
            dest = os.path.join(out_dir, os.path.basename(rf))
            shutil.copy2(rf, dest)
            print(f"    Copied: {dest}")
            merged_files.append(dest)

    return merged_files


def merge_text_files(datasets, text_files, pfn_prefix, group_index):
    """Merge text files — write to merged LFN paths."""
    proc_entries = []
    for pf in text_files:
        node_name = os.path.splitext(os.path.basename(pf))[0]
        with open(pf) as f:
            content = f.read().strip()
        if content:
            proc_entries.append(f"{node_name} | {content}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for ds in datasets:
        merged_base = ds.get("merged_lfn_base", "")
        tier = ds.get("data_tier", "unknown")
        ds_name = ds.get("dataset_name", "")

        merged_lfn_dir = f"{merged_base}/{group_index:06d}"
        out_dir = lfn_to_pfn(pfn_prefix, merged_lfn_dir)
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


# ── Main logic ────────────────────────────────────────────────

# Read proc output manifests and ROOT files from unmerged site storage
# Each dataset has an unmerged_lfn_base; files are at PFN = pfn_prefix + unmerged_lfn/{group:06d}/
unmerged_root_files = []  # (pfn_path, tier_info)
unmerged_manifests = []
unmerged_metrics = []
has_unmerged = any(ds.get("unmerged_lfn_base") for ds in datasets)

if has_unmerged:
    # Read from unmerged site storage
    for ds in datasets:
        unmerged_base = ds.get("unmerged_lfn_base", "")
        if not unmerged_base:
            continue
        unmerged_dir = lfn_to_pfn(local_pfn_prefix, f"{unmerged_base}/{group_index:06d}")
        if not os.path.isdir(unmerged_dir):
            print(f"WARNING: Unmerged dir not found: {unmerged_dir}")
            continue
        # Collect ROOT files
        for f in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_*.root"))):
            unmerged_root_files.append(f)
        if not unmerged_root_files:
            for f in sorted(glob.glob(os.path.join(unmerged_dir, "*.root"))):
                unmerged_root_files.append(f)
        # Collect output manifests
        for mf in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_outputs.json"))):
            unmerged_manifests.append(mf)
        # Collect metrics files
        for mf in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_metrics.json"))):
            unmerged_metrics.append(mf)

# Fallback: read from group dir (backward compat with old format)
if not unmerged_root_files:
    unmerged_root_files = sorted(glob.glob(os.path.join(group_dir, "proc_*_*.root")))
    if not unmerged_root_files:
        unmerged_root_files = sorted(glob.glob(os.path.join(group_dir, "*.root")))
    unmerged_manifests = sorted(glob.glob(os.path.join(group_dir, "proc_*_outputs.json")))
    unmerged_metrics = sorted(glob.glob(os.path.join(group_dir, "proc_*_metrics.json")))

# Also check group dir for text files (transferred by HTCondor)
text_files = sorted(glob.glob(os.path.join(group_dir, "proc_*.out")))

root_files = unmerged_root_files

if root_files:
    print(f"Detected {len(root_files)} ROOT file(s) — using cmsRun merge mode")

    # Load manifest for CMSSW version info
    manifest = {}
    manifest_path = os.path.join(group_dir, "manifest.json")
    if os.path.isfile(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

    # Build file-to-info mapping from proc output manifests
    # Supports both new format {"file": {"tier": "X", "step_index": N}}
    # and old format {"file": "TIER"} for backward compatibility
    file_info_map = {}  # filename -> {"tier": str, "step_index": int}
    for mf in unmerged_manifests:
        try:
            with open(mf) as f:
                raw = json.load(f)
            for fname, val in raw.items():
                if isinstance(val, dict):
                    file_info_map[fname] = val
                else:
                    # Old format: val is just the tier string
                    file_info_map[fname] = {"tier": val, "step_index": -1}
        except Exception:
            pass

    # Group files by tier
    tier_to_files = {}  # tier -> [paths]
    tier_to_step = {}   # tier -> step_index (first seen)
    for f in root_files:
        basename = os.path.basename(f)
        finfo = file_info_map.get(basename)
        if finfo:
            tier = finfo["tier"]
            step_idx = finfo.get("step_index", -1)
        else:
            # Fallback: extract from filename pattern proc_NNN_stepN_TIER.root
            m = re.match(r'proc_\\d+_step(\\d+)_(\\w+)\\.root', basename)
            if m:
                tier = m.group(2)
                step_idx = int(m.group(1)) - 1
            else:
                tier = "unknown"
                step_idx = -1
        tier_to_files.setdefault(tier, []).append(f)
        if tier not in tier_to_step:
            tier_to_step[tier] = step_idx

    print(f"Tier groups: { {k: len(v) for k, v in tier_to_files.items()} }")

    work_dir = os.getcwd()

    # Track unmerged directories for cleanup
    cleanup_dirs = set()

    for ds in datasets:
        merged_base = ds.get("merged_lfn_base", "")
        unmerged_base = ds.get("unmerged_lfn_base", "")
        ds_tier = ds.get("data_tier", "unknown")

        # Match dataset tier to discovered file tiers
        tier_files = tier_to_files.get(ds_tier, [])
        step_idx = tier_to_step.get(ds_tier, -1)
        if not tier_files:
            # Try EDM variant: NANOEDMAODSIM files match NANOAODSIM dataset
            for tier, files in tier_to_files.items():
                if tier.upper().replace("EDM", "") == ds_tier.upper():
                    tier_files = files
                    step_idx = tier_to_step.get(tier, -1)
                    break
        if not tier_files:
            # Fallback: longer tier name must contain shorter one
            # (match MINIAODSIM->MINIAODSIM but not AODSIM->NANOAODSIM)
            for tier, files in tier_to_files.items():
                t_up, d_up = tier.upper(), ds_tier.upper()
                longer, shorter = (t_up, d_up) if len(t_up) >= len(d_up) else (d_up, t_up)
                if shorter in longer and len(longer) - len(shorter) <= 3:
                    tier_files = files
                    step_idx = tier_to_step.get(tier, -1)
                    break

        if not tier_files:
            print(f"WARNING: No ROOT files matching tier {ds_tier}, skipping")
            continue

        # Merged output goes to merged site storage via LFN→PFN
        merged_lfn_dir = f"{merged_base}/{group_index:06d}"
        out_dir = lfn_to_pfn(local_pfn_prefix, merged_lfn_dir)

        print(f"Tier {ds_tier}: {len(tier_files)} files to merge (step_index={step_idx})")
        print(f"  Output: {out_dir}")
        os.makedirs(out_dir, exist_ok=True)

        if manifest.get("mode") == "cmssw":
            merge_root_tier(ds_tier, tier_files, step_idx, out_dir, manifest, work_dir)
        else:
            # Non-CMSSW mode: just copy files
            print(f"  Non-CMSSW mode: copying {len(tier_files)} ROOT files to {out_dir}")
            for rf in tier_files:
                dest = os.path.join(out_dir, os.path.basename(rf))
                shutil.copy2(rf, dest)
                print(f"    Copied: {dest}")

        # Track unmerged dir for cleanup
        if unmerged_base:
            unmerged_pfn_dir = lfn_to_pfn(local_pfn_prefix, f"{unmerged_base}/{group_index:06d}")
            cleanup_dirs.add(unmerged_pfn_dir)

    # Write cleanup_manifest.json for the cleanup job
    if cleanup_dirs:
        cleanup_manifest = {"unmerged_dirs": sorted(cleanup_dirs)}
        cleanup_path = os.path.join(group_dir, "cleanup_manifest.json")
        with open(cleanup_path, "w") as f:
            json.dump(cleanup_manifest, f, indent=2)
        print(f"Wrote cleanup manifest: {cleanup_path} ({len(cleanup_dirs)} dirs)")

    # Aggregate per-step resource metrics from proc jobs
    if unmerged_metrics:
        print(f"\\nAggregating metrics from {len(unmerged_metrics)} proc job(s)...")
        all_proc_metrics = []
        for mf in unmerged_metrics:
            try:
                with open(mf) as f:
                    all_proc_metrics.append(json.load(f))
            except Exception as e:
                print(f"  WARNING: Failed to read {mf}: {e}")

        if all_proc_metrics:
            # Group by step number
            step_data = {}  # step_num -> {metric_name -> [values]}
            for proc in all_proc_metrics:
                for step_m in proc:
                    step_num = step_m.get("step", 0)
                    if step_num not in step_data:
                        step_data[step_num] = {}
                    for key, val in step_m.items():
                        if key == "step":
                            continue
                        if val is not None:
                            step_data[step_num].setdefault(key, []).append(val)

            # Compute aggregates
            per_step = {}
            count_metrics = {"events_processed"}  # use total instead of mean
            for step_num, metrics in sorted(step_data.items()):
                agg = {"step": step_num, "num_jobs": len(all_proc_metrics)}
                for key, values in metrics.items():
                    if not values:
                        continue
                    if key in count_metrics:
                        agg[key] = {
                            "min": min(values),
                            "max": max(values),
                            "total": sum(values),
                        }
                    else:
                        agg[key] = {
                            "min": round(min(values), 4),
                            "max": round(max(values), 4),
                            "mean": round(sum(values) / len(values), 4),
                        }
                per_step[str(step_num)] = agg

            work_unit = {
                "group_index": group_index,
                "num_proc_jobs": len(all_proc_metrics),
                "per_step": per_step,
            }
            wu_path = os.path.join(group_dir, "work_unit_metrics.json")
            with open(wu_path, "w") as f:
                json.dump(work_unit, f, indent=2)
            print(f"Wrote work_unit_metrics.json: {len(per_step)} step(s), {len(all_proc_metrics)} jobs")
else:
    print(f"Detected {len(text_files)} text file(s) — using text merge mode")
    merge_text_files(datasets, text_files, local_pfn_prefix, group_index)
''')
    os.chmod(path, 0o755)


def _write_cleanup_script(path: str) -> None:
    """Generate a cleanup script that removes unmerged files from site storage."""
    _write_file(path, '''#!/usr/bin/env python3
"""wms2_cleanup.py — WMS2 cleanup job.

Reads cleanup_manifest.json (written by the merge job) and removes
unmerged directories from site storage.
"""
import json
import os
import shutil
import sys

# ── Argument parsing ──────────────────────────────────────────

output_info_path = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--output-info" and i + 1 < len(sys.argv):
        output_info_path = sys.argv[i + 1]
        i += 2
    else:
        i += 1

# Find cleanup_manifest.json in the group dir
cleanup_manifest_path = None
if output_info_path and os.path.isfile(output_info_path):
    group_dir = os.path.dirname(output_info_path)
    candidate = os.path.join(group_dir, "cleanup_manifest.json")
    if os.path.isfile(candidate):
        cleanup_manifest_path = candidate

# Also check current working directory
if not cleanup_manifest_path and os.path.isfile("cleanup_manifest.json"):
    cleanup_manifest_path = "cleanup_manifest.json"

if not cleanup_manifest_path:
    print("No cleanup_manifest.json found — nothing to clean up")
    sys.exit(0)

with open(cleanup_manifest_path) as f:
    manifest = json.load(f)

unmerged_dirs = manifest.get("unmerged_dirs", [])
if not unmerged_dirs:
    print("No unmerged directories to clean up")
    sys.exit(0)

print(f"Cleaning up {len(unmerged_dirs)} unmerged directories...")
for d in unmerged_dirs:
    if os.path.isdir(d):
        shutil.rmtree(d)
        print(f"  Removed: {d}")
    else:
        print(f"  Already gone: {d}")

print("Cleanup complete")
''')
    os.chmod(path, 0o755)


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)
