"""Workflow Manager: imports requests from ReqMgr2, creates workflow rows."""

import logging
from dataclasses import asdict
from typing import Any

from wms2.adapters.base import ReqMgrAdapter
from wms2.core.stepchain import parse_stepchain
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
            config_data=self._build_config_data(data),
        )

        logger.info(
            "Imported request %s as workflow %s (dataset=%s, splitting=%s)",
            request_name, workflow.id, data["InputDataset"], data["SplittingAlgo"],
        )
        return workflow

    @staticmethod
    def _build_config_data(data: dict[str, Any]) -> dict[str, Any]:
        """Extract config_data fields from a ReqMgr2 request.

        For StepChain requests, parses per-step configs using the StepChain parser.
        Step-level always overrides top-level.
        """
        # Common metadata fields
        config_data: dict[str, Any] = {
            "campaign": data.get("Campaign"),
            "requestor": data.get("Requestor"),
            "priority": data.get("Priority"),
        }

        if data.get("StepChain"):
            spec = parse_stepchain(data)
            config_data["stepchain_spec"] = asdict(spec)
            # Flat fields for DAG planner backward compat
            config_data["memory_mb"] = max(s.memory_mb for s in spec.steps)
            config_data["multicore"] = max(s.multicore for s in spec.steps)
            config_data["time_per_event"] = spec.time_per_event
            config_data["size_per_event"] = spec.size_per_event
            config_data["filter_efficiency"] = spec.filter_efficiency
        else:
            # Non-StepChain: existing flat extraction
            config_data.update({
                "cmssw_version": data.get("CMSSWVersion"),
                "scram_arch": data.get("ScramArch"),
                "global_tag": data.get("GlobalTag"),
                "memory_mb": data.get("Memory", 2048),
                "multicore": data.get("Multicore", 1),
                "time_per_event": data.get("TimePerEvent", 1.0),
                "size_per_event": data.get("SizePerEvent", 1.5),
                "filter_efficiency": data.get("FilterEfficiency", 1.0),
                "event_streams": data.get("EventStreams", 0),
            })

        return config_data

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

        blocks = await self.db.get_processing_blocks(workflow.id)
        result["processing_blocks"] = [
            {"dataset": b.dataset_name, "status": b.status, "block_index": b.block_index}
            for b in blocks
        ]

        return result
