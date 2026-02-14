"""Workflow Manager: imports requests from ReqMgr2, creates workflow rows."""

import logging
from typing import Any

from wms2.adapters.base import ReqMgrAdapter
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("InputDataset", "SplittingAlgo", "SandboxUrl")


class WorkflowManager:
    def __init__(self, repository: Repository, reqmgr_adapter: ReqMgrAdapter):
        self.db = repository
        self.reqmgr = reqmgr_adapter

    async def import_request(self, request_name: str) -> Any:
        """Fetch request from ReqMgr2, validate, create workflow row."""
        data = await self.reqmgr.get_request(request_name)

        # Validate required fields
        missing = [f for f in REQUIRED_FIELDS if not data.get(f)]
        if missing:
            raise ValueError(
                f"Request {request_name} missing required fields: {', '.join(missing)}"
            )

        workflow = await self.db.create_workflow(
            request_name=request_name,
            input_dataset=data["InputDataset"],
            splitting_algo=data["SplittingAlgo"],
            splitting_params=data.get("SplittingParams", {}),
            sandbox_url=data["SandboxUrl"],
            config_data={
                "campaign": data.get("Campaign"),
                "requestor": data.get("Requestor"),
                "priority": data.get("Priority"),
            },
        )

        logger.info(
            "Imported request %s as workflow %s (dataset=%s, splitting=%s)",
            request_name, workflow.id, data["InputDataset"], data["SplittingAlgo"],
        )
        return workflow

    async def get_workflow_status(self, workflow_id) -> dict[str, Any]:
        """Build a progress summary for a workflow."""
        workflow = await self.db.get_workflow(workflow_id)
        if not workflow:
            return {}

        result: dict[str, Any] = {
            "workflow_id": str(workflow.id),
            "request_name": workflow.request_name,
            "status": workflow.status,
            "total_nodes": workflow.total_nodes,
            "nodes_done": workflow.nodes_done,
            "nodes_failed": workflow.nodes_failed,
            "nodes_running": workflow.nodes_running,
        }

        if workflow.dag_id:
            dag = await self.db.get_dag(workflow.dag_id)
            if dag:
                result["dag"] = {
                    "dag_id": str(dag.id),
                    "status": dag.status,
                    "total_work_units": dag.total_work_units,
                }

        outputs = await self.db.get_output_datasets(workflow.id)
        result["outputs"] = [
            {"dataset": o.dataset_name, "status": o.status} for o in outputs
        ]

        return result
