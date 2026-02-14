"""DAG Monitor: polls DAGMan status and detects completed work units."""

from __future__ import annotations

import json
import logging
import os
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

    def __init__(self, repository: Repository, condor_adapter: CondorAdapter):
        self.db = repository
        self.condor = condor_adapter

    async def poll_dag(self, dag) -> DAGPollResult:
        """Poll a DAG's status. Main entry point called by lifecycle manager."""
        # Check if DAGMan process is still alive
        job_info = await self.condor.query_job(
            schedd_name=dag.schedd_name,
            cluster_id=dag.dagman_cluster_id,
        )

        if job_info is None:
            # DAGMan process gone — read final metrics
            return await self._handle_dag_completion(dag)

        # DAGMan alive — parse .status file for progress
        status_file = dag.dag_file_path + ".status"
        summary = self._parse_dagman_status(status_file)

        # Detect newly completed work units
        newly_completed = self._detect_completed_work_units(dag, summary)

        # Update DAG row with current counts
        now = datetime.now(timezone.utc)
        update_kwargs = {
            "nodes_idle": summary.idle,
            "nodes_running": summary.running,
            "nodes_done": summary.done,
            "nodes_failed": summary.failed,
            "nodes_held": summary.held,
            "status": DAGStatus.RUNNING.value,
        }
        if newly_completed:
            existing = dag.completed_work_units or []
            update_kwargs["completed_work_units"] = existing + [
                wu["group_name"] for wu in newly_completed
            ]
        await self.db.update_dag(dag.id, **update_kwargs)

        # Update workflow node counts
        if dag.workflow_id:
            await self.db.update_workflow(
                dag.workflow_id,
                nodes_done=summary.done,
                nodes_failed=summary.failed,
                nodes_running=summary.running,
                nodes_queued=summary.idle,
            )

        return DAGPollResult(
            dag_id=str(dag.id),
            status=DAGStatus.RUNNING,
            nodes_idle=summary.idle,
            nodes_running=summary.running,
            nodes_done=summary.done,
            nodes_failed=summary.failed,
            nodes_held=summary.held,
            newly_completed_work_units=newly_completed,
        )

    def _parse_dagman_status(self, status_file: str) -> NodeSummary:
        """Parse the DAGMan .status JSON file."""
        if not os.path.exists(status_file):
            logger.warning("Status file not found: %s", status_file)
            return NodeSummary()

        with open(status_file) as f:
            data = json.load(f)

        node_statuses = {}
        counts = {"idle": 0, "running": 0, "done": 0, "failed": 0, "held": 0}

        # DAGMan status file has a "nodes" dict with node_name -> status info
        for node_name, info in data.get("nodes", {}).items():
            status = info.get("status", "idle").lower()
            node_statuses[node_name] = status
            if status in ("done", "success"):
                counts["done"] += 1
            elif status in ("error", "failed"):
                counts["failed"] += 1
            elif status in ("running", "submitted"):
                counts["running"] += 1
            elif status == "held":
                counts["held"] += 1
            else:
                counts["idle"] += 1

        return NodeSummary(
            idle=counts["idle"],
            running=counts["running"],
            done=counts["done"],
            failed=counts["failed"],
            held=counts["held"],
            node_statuses=node_statuses,
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
                newly_completed.append({
                    "group_name": node_name,
                    "manifest": manifest,
                })

        return newly_completed

    def _read_merge_manifest(self, dag, group_name: str) -> dict | None:
        """Read the merge output manifest for a completed work unit."""
        manifest_path = os.path.join(
            dag.submit_dir, group_name, "merge_output.json"
        )
        if not os.path.exists(manifest_path):
            return None
        with open(manifest_path) as f:
            return json.load(f)

    async def _handle_dag_completion(self, dag) -> DAGPollResult:
        """Handle a DAG whose DAGMan process has exited."""
        metrics_file = dag.dag_file_path + ".metrics"
        summary = self._parse_dagman_metrics(metrics_file)

        # Fall back to .status file if metrics file is missing
        if summary.done == 0 and summary.failed == 0:
            status_file = dag.dag_file_path + ".status"
            summary = self._parse_dagman_status(status_file)

        # Determine final status
        total = dag.total_work_units or (summary.done + summary.failed)
        if summary.failed == 0 and summary.done > 0:
            final_status = DAGStatus.COMPLETED
        elif summary.done > 0 and summary.failed > 0:
            final_status = DAGStatus.PARTIAL
        elif summary.failed > 0 and summary.done == 0:
            final_status = DAGStatus.FAILED
        else:
            # No done, no failed — could be removed/halted
            final_status = DAGStatus.FAILED

        now = datetime.now(timezone.utc)
        await self.db.update_dag(
            dag.id,
            status=final_status.value,
            nodes_done=summary.done,
            nodes_failed=summary.failed,
            nodes_running=0,
            nodes_idle=0,
            nodes_held=0,
            completed_at=now,
        )

        if dag.workflow_id:
            await self.db.update_workflow(
                dag.workflow_id,
                nodes_done=summary.done,
                nodes_failed=summary.failed,
                nodes_running=0,
                nodes_queued=0,
            )

        logger.info(
            "DAG %s completed: status=%s done=%d failed=%d",
            dag.id, final_status.value, summary.done, summary.failed,
        )

        return DAGPollResult(
            dag_id=str(dag.id),
            status=final_status,
            nodes_done=summary.done,
            nodes_failed=summary.failed,
        )
