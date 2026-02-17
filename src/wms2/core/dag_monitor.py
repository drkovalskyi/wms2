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
            running = int(dag_block.get("NodesQueued", 0)) + int(dag_block.get("NodesPost", 0))
            idle = (
                int(dag_block.get("NodesReady", 0))
                + int(dag_block.get("NodesUnready", 0))
                + int(dag_block.get("NodesPre", 0))
            )
            held = int(dag_block.get("JobProcsHeld", 0))
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

        # Also parse .status to detect work units that completed since last poll
        status_file = dag.dag_file_path + ".status"
        status_summary = self._parse_dagman_status(status_file)
        newly_completed = self._detect_completed_work_units(dag, status_summary)

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
        update_kwargs = {
            "status": final_status.value,
            "nodes_done": summary.done,
            "nodes_failed": summary.failed,
            "nodes_running": 0,
            "nodes_idle": 0,
            "nodes_held": 0,
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
                nodes_done=summary.done,
                nodes_failed=summary.failed,
                nodes_running=0,
                nodes_queued=0,
            )

        logger.info(
            "DAG %s completed: status=%s done=%d failed=%d newly_completed_wus=%d",
            dag.id, final_status.value, summary.done, summary.failed,
            len(newly_completed),
        )

        return DAGPollResult(
            dag_id=str(dag.id),
            status=final_status,
            nodes_done=summary.done,
            nodes_failed=summary.failed,
            newly_completed_work_units=newly_completed,
        )
