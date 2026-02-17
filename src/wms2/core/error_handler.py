"""Error Handler — workflow-level failure classification and recovery.

When a DAG completes with failures, the Error Handler classifies by failure
ratio and decides: auto-rescue, flag for review, or abort.
"""

import logging
import os

from wms2.adapters.base import CondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, WorkflowStatus

logger = logging.getLogger(__name__)


class ErrorHandler:
    def __init__(self, repository: Repository, condor_adapter: CondorAdapter, settings: Settings):
        self.db = repository
        self.condor = condor_adapter
        self.settings = settings

    async def handle_dag_failure(self, dag, request, workflow) -> str:
        """DAG with zero successes. Always aborts."""
        await self._record_event(dag, "dag_failed", {
            "nodes_done": dag.nodes_done,
            "nodes_failed": dag.nodes_failed,
            "total_nodes": dag.total_nodes,
        })
        await self.db.update_workflow(workflow.id, status=WorkflowStatus.FAILED.value)
        return "abort"

    async def handle_dag_partial_failure(self, dag, request, workflow) -> str:
        """DAG with some failures. Returns 'rescue', 'review', or 'abort'."""
        ratio = dag.nodes_failed / max(dag.total_nodes, 1)
        await self._record_event(dag, "dag_partial", {
            "failure_ratio": ratio,
            "nodes_done": dag.nodes_done,
            "nodes_failed": dag.nodes_failed,
            "total_nodes": dag.total_nodes,
        })

        # Check rescue attempt count
        rescue_count = await self._count_rescue_chain(dag)
        if rescue_count >= self.settings.error_max_rescue_attempts:
            logger.warning(
                "Max rescue attempts (%d) reached for %s",
                rescue_count, request.request_name,
            )
            await self.db.update_workflow(workflow.id, status=WorkflowStatus.FAILED.value)
            return "abort"

        if ratio < self.settings.error_auto_rescue_threshold:
            await self._prepare_rescue(dag, request, workflow)
            return "rescue"
        elif ratio < self.settings.error_abort_threshold:
            logger.error(
                "ALERT [partial_failure] %s: %.1f%% failed — flagged for review",
                request.request_name, ratio * 100,
            )
            return "review"
        else:
            await self.db.update_workflow(workflow.id, status=WorkflowStatus.FAILED.value)
            return "abort"

    async def _prepare_rescue(self, dag, request, workflow):
        """Create rescue DAG record, point workflow to it."""
        rescue_path = self._find_rescue_dag(dag)
        new_dag = await self.db.create_dag(
            workflow_id=workflow.id,
            dag_file_path=dag.dag_file_path,
            submit_dir=dag.submit_dir,
            rescue_dag_path=rescue_path,
            parent_dag_id=dag.id,
            total_nodes=dag.total_nodes,
            total_edges=dag.total_edges,
            node_counts=dag.node_counts,
            total_work_units=dag.total_work_units,
            completed_work_units=dag.completed_work_units,
            status=DAGStatus.READY.value,
        )
        await self.db.update_workflow(
            workflow.id, dag_id=new_dag.id, status=WorkflowStatus.RESUBMITTING.value
        )

    def _find_rescue_dag(self, dag) -> str:
        """Find highest-numbered rescue DAG file on disk."""
        for i in range(99, 0, -1):
            path = f"{dag.dag_file_path}.rescue{i:03d}"
            if os.path.exists(path):
                return path
        return f"{dag.dag_file_path}.rescue001"

    async def _count_rescue_chain(self, dag) -> int:
        """Count how many rescue DAGs exist in this workflow's chain."""
        count = 0
        current = dag
        while current.parent_dag_id:
            count += 1
            current = await self.db.get_dag(current.parent_dag_id)
            if not current:
                break
        return count

    async def _record_event(self, dag, event_type: str, detail: dict):
        await self.db.create_dag_history(
            dag_id=dag.id,
            event_type=event_type,
            from_status=dag.status,
            nodes_done=dag.nodes_done,
            nodes_failed=dag.nodes_failed,
            nodes_running=0,
            detail=detail,
        )
