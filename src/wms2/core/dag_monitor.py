"""DAG Monitor: polls DAGMan status and detects completed work units."""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from wms2.adapters.base import CondorAdapter
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus

logger = logging.getLogger(__name__)


@dataclass
class NodeSummary:
    """Aggregate node status counts from a DAGMan status/metrics file."""
    idle: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    held: int = 0
    node_statuses: dict[str, str] = field(default_factory=dict)


@dataclass
class DAGPollResult:
    """Result of polling a single DAG."""
    dag_id: str
    status: DAGStatus
    nodes_idle: int = 0
    nodes_running: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_held: int = 0
    newly_completed_work_units: list[dict] = field(default_factory=list)


class DAGMonitor:
    """Monitors running DAGs by parsing .status files and querying HTCondor."""

    def __init__(self, repository: Repository, condor_adapter: CondorAdapter,
                 settings=None):
        self.db = repository
        self.condor = condor_adapter
        self.settings = settings

    async def poll_dag(self, dag) -> DAGPollResult:
        """Poll a DAG's status. Main entry point called by lifecycle manager."""
        # Check if DAGMan process is still alive
        try:
            job_info = await self.condor.query_job(
                schedd_name=dag.schedd_name,
                cluster_id=dag.dagman_cluster_id,
            )
        except Exception:
            logger.warning(
                "Cannot reach schedd %s for DAG %s — will retry next cycle",
                dag.schedd_name, dag.id,
            )
            return DAGPollResult(
                dag_id=str(dag.id),
                status=DAGStatus.RUNNING,
                nodes_idle=dag.nodes_idle or 0,
                nodes_running=dag.nodes_running or 0,
                nodes_done=dag.nodes_done or 0,
                nodes_failed=dag.nodes_failed or 0,
                nodes_held=dag.nodes_held or 0,
            )

        if job_info is None:
            # DAGMan process gone — read final metrics
            return await self._handle_dag_completion(dag)

        # DAGMan completed but still in queue (LeaveJobInQueue)
        job_status = int(job_info.get("JobStatus", 0))
        if job_status in (3, 4):  # 3=removed, 4=completed
            return await self._handle_dag_completion(dag)

        # DAGMan alive — parse .status file for progress
        status_file = self._resolve_dag_file(dag, ".status")
        summary = self._parse_dagman_status(status_file)
        logger.debug(
            "DAG %s outer status: done=%d failed=%d running=%d idle=%d, mg_nodes=%d",
            dag.id, summary.done, summary.failed, summary.running, summary.idle,
            sum(1 for n in summary.node_statuses if n.startswith("mg_")),
        )

        # Aggregate inner SUBDAG status files for actual node counts;
        # fall back to condor_q if inner status files don't exist yet
        inner_summary = self._aggregate_inner_status(dag, summary)
        if inner_summary is summary:
            # No inner status files found — try condor_q
            logger.debug("DAG %s: falling back to condor_q for node counts", dag.id)
            condor_counts = await self._count_jobs_from_condor(dag)

            if condor_counts is not None:
                inner_summary = condor_counts
                logger.debug(
                    "DAG %s condor_q counts: done=%d running=%d idle=%d failed=%d",
                    dag.id, condor_counts.done, condor_counts.running,
                    condor_counts.idle, condor_counts.failed,
                )

        # Detect newly completed work units
        newly_completed = self._detect_completed_work_units(dag, summary)

        # Update DAG row with current counts (use inner counts for accurate display)
        now = datetime.now(timezone.utc)
        live_total = (
            inner_summary.idle + inner_summary.running
            + inner_summary.done + inner_summary.failed
            + inner_summary.held
        )
        # Merge WU-level counts from outer summary into node_counts
        nc = dict(dag.node_counts or {})
        nc["wus_done"] = summary.done
        nc["wus_failed"] = summary.failed
        update_kwargs = {
            "nodes_idle": inner_summary.idle,
            "nodes_running": inner_summary.running,
            "nodes_done": inner_summary.done,
            "nodes_failed": inner_summary.failed,
            "nodes_held": inner_summary.held,
            "total_nodes": live_total,
            "node_counts": nc,
            "status": DAGStatus.RUNNING.value,
        }
        if newly_completed:
            existing = dag.completed_work_units or []
            update_kwargs["completed_work_units"] = existing + [
                wu["group_name"] for wu in newly_completed
            ]
        await self.db.update_dag(dag.id, **update_kwargs)

        # Handle cgroup OOM holds (HoldReasonCode 34)
        if inner_summary.held > 0 and dag.dagman_cluster_id:
            await self._handle_held_oom_jobs(dag)


        # Update workflow node counts (reuse live_total from DAG update above)
        if dag.workflow_id:
            await self.db.update_workflow(
                dag.workflow_id,
                nodes_done=inner_summary.done,
                nodes_failed=inner_summary.failed,
                nodes_running=inner_summary.running,
                nodes_queued=inner_summary.idle,
                total_nodes=live_total,
            )

        return DAGPollResult(
            dag_id=str(dag.id),
            status=DAGStatus.RUNNING,
            nodes_idle=inner_summary.idle,
            nodes_running=inner_summary.running,
            nodes_done=inner_summary.done,
            nodes_failed=inner_summary.failed,
            nodes_held=inner_summary.held,
            newly_completed_work_units=newly_completed,
        )

    @staticmethod
    def _resolve_dag_file(dag, suffix: str) -> str:
        """Resolve DAG auxiliary file path, preferring submit_dir.

        In spool mode the submit_dir (DAGMan's Iwd) may differ from the
        directory containing dag_file_path. DAGMan writes .status, .metrics,
        .dagman.out etc. in its Iwd, so prefer that when available.
        """
        dag_basename = os.path.basename(dag.dag_file_path)
        candidate = os.path.join(dag.submit_dir, dag_basename + suffix)
        if os.path.exists(candidate):
            return candidate
        return dag.dag_file_path + suffix

    # NodeStatus integer → string mapping for DAGMan NODE_STATUS_FILE
    _NODE_STATUS_MAP = {
        "0": "not_ready",
        "1": "ready",
        "2": "prerun",
        "3": "submitted",
        "4": "postrun",
        "5": "done",
        "6": "error",
        "7": "futile",
    }

    def _parse_dagman_status(self, status_file: str) -> NodeSummary:
        """Parse the DAGMan NODE_STATUS_FILE (ClassAd format).

        Format is a sequence of ClassAd blocks:
          [Type = "DagStatus"; NodesDone = N; ...]
          [Type = "NodeStatus"; Node = "mg_000000"; NodeStatus = 5; ...]
          [Type = "StatusEnd"; ...]
        """
        import re

        if not os.path.exists(status_file):
            logger.warning("Status file not found: %s", status_file)
            return NodeSummary()

        with open(status_file) as f:
            content = f.read()

        # Parse ClassAd blocks
        blocks: list[dict[str, str]] = []
        current_block: dict[str, str] = {}
        for line in content.splitlines():
            line = re.sub(r"/\*.*?\*/", "", line).strip()
            if line == "[":
                current_block = {}
            elif line in ("]", "];"):
                if current_block:
                    blocks.append(current_block)
                current_block = {}
            elif "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().rstrip(";").strip()
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                current_block[key] = value

        node_statuses: dict[str, str] = {}
        dag_block: dict[str, str] | None = None

        for block in blocks:
            btype = block.get("Type", "")
            if btype == "DagStatus":
                dag_block = block
            elif btype == "NodeStatus":
                name = block.get("Node", "")
                status_num = block.get("NodeStatus", "0")
                node_statuses[name] = self._NODE_STATUS_MAP.get(status_num, "unknown")

        if dag_block:
            done = int(dag_block.get("NodesDone", 0))
            failed = int(dag_block.get("NodesFailed", 0))
            held = int(dag_block.get("JobProcsHeld", 0))
            # NodesQueued = nodes submitted to HTCondor (includes idle + running + held).
            # Use JobProcsIdle to split correctly.
            nodes_submitted = int(dag_block.get("NodesQueued", 0))
            job_procs_idle = int(dag_block.get("JobProcsIdle", 0))
            running = max(0, nodes_submitted - job_procs_idle - held) + int(dag_block.get("NodesPost", 0))
            idle = (
                job_procs_idle
                + int(dag_block.get("NodesReady", 0))
                + int(dag_block.get("NodesUnready", 0))
                + int(dag_block.get("NodesPre", 0))
            )
        else:
            done = sum(1 for s in node_statuses.values() if s == "done")
            failed = sum(1 for s in node_statuses.values() if s in ("error", "futile"))
            running = sum(1 for s in node_statuses.values() if s in ("submitted", "postrun", "prerun"))
            idle = sum(1 for s in node_statuses.values() if s in ("ready", "not_ready"))
            held = 0

        return NodeSummary(
            idle=idle,
            running=running,
            done=done,
            failed=failed,
            held=held,
            node_statuses=node_statuses,
        )

    def _aggregate_inner_status(self, dag, outer_summary: NodeSummary) -> NodeSummary:
        """Aggregate node counts from inner SUBDAG status files.

        The outer DAG's status file only reports top-level nodes (mg_NNNNNN SUBDAGs).
        To get actual job-level counts, we parse each inner group.dag.status file
        and sum up all inner node counts across all merge groups.
        Falls back to outer summary if no inner status files are found.
        """
        total = NodeSummary()
        found_any = False
        mg_count = 0

        for node_name, status in outer_summary.node_statuses.items():
            if not node_name.startswith("mg_"):
                continue
            mg_count += 1
            inner_status_file = os.path.join(
                dag.submit_dir, node_name, "group.dag.status"
            )
            if not os.path.exists(inner_status_file):
                continue

            found_any = True
            inner = self._parse_dagman_status(inner_status_file)
            total.idle += inner.idle
            total.running += inner.running
            total.done += inner.done
            total.failed += inner.failed
            total.held += inner.held

        if not found_any:
            logger.debug(
                "No inner status files found for DAG %s (%d mg_ nodes in outer status)",
                dag.id, mg_count,
            )
            return outer_summary
        logger.debug(
            "Aggregated inner status for DAG %s: done=%d running=%d idle=%d failed=%d (from %d mg_ nodes)",
            dag.id, total.done, total.running, total.idle, total.failed, mg_count,
        )
        return total

    async def _count_jobs_from_condor(self, dag) -> NodeSummary | None:
        """Query condor_q for actual job counts under this DAG hierarchy.

        Used as fallback when inner SUBDAG status files are not available
        (e.g. DAGs submitted before NODE_STATUS_FILE was added to inner DAGs).
        """
        if not dag.dagman_cluster_id:
            return None
        try:
            counts = await self.condor.count_dag_jobs(
                dag.dagman_cluster_id, schedd_name=dag.schedd_name,
            )
        except Exception:
            logger.warning("Cannot query condor for DAG %s job counts", dag.id)
            return None
        if counts is None or counts["total"] == 0:
            return None
        return NodeSummary(
            idle=counts["idle"],
            running=counts["running"],
            done=counts["done"],
            failed=counts["failed"],
            held=counts["held"],
        )

    def _parse_dagman_metrics(self, metrics_file: str) -> NodeSummary:
        """Parse the DAGMan .metrics file for final counts."""
        if not os.path.exists(metrics_file):
            logger.warning("Metrics file not found: %s", metrics_file)
            return NodeSummary()

        with open(metrics_file) as f:
            data = json.load(f)

        return NodeSummary(
            idle=int(data.get("nodes_idle", 0)),
            running=int(data.get("nodes_running", 0)),
            done=int(data.get("nodes_done", 0)),
            failed=int(data.get("nodes_failed", 0)),
            held=int(data.get("nodes_held", 0)),
        )

    def _detect_completed_work_units(
        self, dag, summary: NodeSummary
    ) -> list[dict]:
        """Detect merge group SUBDAGs that have newly completed."""
        already_completed = set(dag.completed_work_units or [])
        newly_completed = []

        for node_name, status in summary.node_statuses.items():
            # Merge group nodes are named mg_NNNNNN in the outer DAG
            if not node_name.startswith("mg_"):
                continue
            if status in ("done", "success") and node_name not in already_completed:
                manifest = self._read_merge_manifest(dag, node_name)
                metrics = self._read_work_unit_metrics(dag, node_name)
                newly_completed.append({
                    "group_name": node_name,
                    "manifest": manifest,
                    "metrics": metrics,
                    "output_events": self._extract_output_events(metrics),
                })

        return newly_completed

    def _read_merge_manifest(self, dag, group_name: str) -> dict | None:
        """Read the merge output manifest for a completed work unit."""
        # Primary: WU subdirectory (spool mode with transfer_output_remaps)
        manifest_path = os.path.join(
            dag.submit_dir, group_name, "merge_output.json"
        )
        # Fallback: spool root (old DAGs without output remaps)
        if not os.path.exists(manifest_path):
            manifest_path = os.path.join(dag.submit_dir, "merge_output.json")
        if not os.path.exists(manifest_path):
            return None
        with open(manifest_path) as f:
            return json.load(f)

    def _read_work_unit_metrics(self, dag, group_name: str) -> dict | None:
        """Read the work_unit_metrics.json for a completed work unit."""
        # Primary: WU subdirectory (spool mode with transfer_output_remaps)
        metrics_path = os.path.join(dag.submit_dir, group_name, "work_unit_metrics.json")
        # Fallback: spool root (old DAGs without output remaps)
        if not os.path.exists(metrics_path):
            metrics_path = os.path.join(dag.submit_dir, "work_unit_metrics.json")
        if not os.path.exists(metrics_path):
            return None
        try:
            with open(metrics_path) as f:
                return json.load(f)
        except Exception:
            logger.warning("Failed to read work unit metrics: %s", metrics_path)
            return None

    @staticmethod
    def _extract_output_events(metrics: dict | None) -> int:
        """Extract total output events from work_unit_metrics.json.

        Reads the last step's events_written.total, falling back to
        events_processed.total if events_written is unavailable.
        """
        if not metrics:
            return 0
        per_step = metrics.get("per_step", {})
        if not per_step:
            return 0
        # Last step by step number
        last_step_key = max(per_step.keys(), key=lambda k: int(k))
        last_step = per_step[last_step_key]
        ew = last_step.get("events_written")
        if ew and isinstance(ew, dict):
            return ew.get("total", 0)
        ep = last_step.get("events_processed")
        if ep and isinstance(ep, dict):
            return ep.get("total", 0)
        return 0

    async def _handle_held_oom_jobs(self, dag) -> None:
        """Detect jobs held for cgroup OOM (HoldReasonCode 34), bump memory, release."""
        try:
            held_jobs = await self.condor.query_held_jobs(
                dag.dagman_cluster_id, schedd_name=dag.schedd_name,
            )
        except Exception:
            logger.warning("Cannot query held jobs for DAG %s", dag.id)
            return
        if not held_jobs:
            return

        max_memory_per_core = 4000  # default fallback
        if self.settings:
            max_memory_per_core = self.settings.max_memory_per_core

        for job in held_jobs:
            if job["hold_reason_code"] != 34:
                continue

            old_mem = job["request_memory"]
            cpus = job["request_cpus"]
            cap = max_memory_per_core * cpus
            new_mem = min(old_mem * 3 // 2, cap)

            if new_mem <= old_mem:
                logger.warning(
                    "OOM job %s.%s already at memory cap %d MB, cannot bump further",
                    job["cluster_id"], job["proc_id"], old_mem,
                )
                continue

            node_name = job["node_name"]
            cluster_id = job["cluster_id"]
            proc_id = job["proc_id"]
            constraint = f"ClusterId == {cluster_id} && ProcId == {proc_id}"

            # Edit the live job's RequestMemory
            await self.condor.edit_job_attr(
                constraint, "RequestMemory", str(new_mem),
                schedd_name=dag.schedd_name,
            )

            # Update the .sub file on disk so retries use the new value.
            # May fail on read-only mounts (remote spool via sshfs) — that's
            # acceptable, the live job is already updated.
            iwd = job.get("iwd", "")
            if iwd:
                sub_path = os.path.join(iwd, f"{node_name}.sub")
                self._update_sub_file_memory(sub_path, new_mem)

            # Release the held job
            await self.condor.release_jobs(
                constraint, schedd_name=dag.schedd_name,
            )

            logger.info(
                "OOM recovery: bumped %s (%d.%d) memory %d → %d MB (cap %d), released",
                node_name, cluster_id, proc_id, old_mem, new_mem, cap,
            )

    @staticmethod
    def _update_sub_file_memory(sub_path: str, new_mem: int) -> None:
        """Update request_memory in a .sub file on disk."""
        if not os.path.exists(sub_path):
            logger.warning("Sub file not found for memory update: %s", sub_path)
            return
        try:
            with open(sub_path) as f:
                content = f.read()
            updated = re.sub(
                r"request_memory\s*=\s*\d+",
                f"request_memory = {new_mem}",
                content,
            )
            if updated != content:
                with open(sub_path, "w") as f:
                    f.write(updated)
        except OSError:
            logger.debug("Cannot update sub file (read-only?): %s", sub_path)

    async def _handle_dag_completion(self, dag) -> DAGPollResult:
        """Handle a DAG whose DAGMan process has exited."""
        metrics_file = self._resolve_dag_file(dag, ".metrics")
        summary = self._parse_dagman_metrics(metrics_file)

        # Fall back to .status file if metrics file is missing
        if summary.done == 0 and summary.failed == 0:
            status_file = self._resolve_dag_file(dag, ".status")
            summary = self._parse_dagman_status(status_file)

        # Also parse .status to detect work units that completed since last poll
        status_file = self._resolve_dag_file(dag, ".status")
        status_summary = self._parse_dagman_status(status_file)
        newly_completed = self._detect_completed_work_units(dag, status_summary)

        # Determine final status (uses outer summary — 1 SUBDAG = 1 work unit)
        total = dag.total_work_units or (summary.done + summary.failed)
        if summary.failed == 0 and summary.done > 0:
            final_status = DAGStatus.COMPLETED
        elif summary.done > 0 and summary.failed > 0:
            final_status = DAGStatus.PARTIAL
        elif summary.failed > 0 and summary.done == 0:
            final_status = DAGStatus.FAILED
        else:
            # No done, no failed — status files likely unreadable (e.g. sshfs
            # mount down).  Keep DAG as RUNNING so we retry next poll cycle
            # instead of falsely marking it as FAILED.
            logger.warning(
                "DAG %s: DAGMan exited but status files show done=0 failed=0 "
                "— status files may be unavailable, will retry next cycle",
                dag.id,
            )
            return DAGPollResult(
                dag_id=str(dag.id),
                status=DAGStatus.RUNNING,
                nodes_idle=dag.nodes_idle or 0,
                nodes_running=dag.nodes_running or 0,
                nodes_done=dag.nodes_done or 0,
                nodes_failed=dag.nodes_failed or 0,
                nodes_held=dag.nodes_held or 0,
            )

        # Try inner SUBDAG counts; fall back to metrics/outer summary
        inner_summary = self._aggregate_inner_status(dag, status_summary)
        # Use inner counts if available, otherwise use the metrics-based summary
        final_done = inner_summary.done if inner_summary.done > 0 else summary.done
        final_failed = inner_summary.failed if inner_summary.done > 0 else summary.failed

        now = datetime.now(timezone.utc)
        nc = dict(dag.node_counts or {})
        nc["wus_done"] = summary.done
        nc["wus_failed"] = summary.failed
        update_kwargs = {
            "status": final_status.value,
            "nodes_done": final_done,
            "nodes_failed": final_failed,
            "nodes_running": 0,
            "nodes_idle": 0,
            "nodes_held": 0,
            "node_counts": nc,
            "completed_at": now,
        }
        if newly_completed:
            existing = dag.completed_work_units or []
            update_kwargs["completed_work_units"] = existing + [
                wu["group_name"] for wu in newly_completed
            ]

        await self.db.update_dag(dag.id, **update_kwargs)

        if dag.workflow_id:
            await self.db.update_workflow(
                dag.workflow_id,
                nodes_done=final_done,
                nodes_failed=final_failed,
                nodes_running=0,
                nodes_queued=0,
            )

        logger.info(
            "DAG %s completed: status=%s done=%d failed=%d newly_completed_wus=%d",
            dag.id, final_status.value, final_done, final_failed,
            len(newly_completed),
        )

        return DAGPollResult(
            dag_id=str(dag.id),
            status=final_status,
            nodes_done=final_done,
            nodes_failed=final_failed,
            newly_completed_work_units=newly_completed,
        )
