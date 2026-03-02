"""POST /api/v1/import — import a request from ReqMgr2 into WMS2."""

from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request as FastAPIRequest
from pydantic import BaseModel

from wms2.core.dag_planner import DAGPlanner
from wms2.core.output_lfn import derive_merged_lfn_bases, determine_merged_lfn_base
from wms2.core.sandbox import create_sandbox
from wms2.db.repository import Repository
from wms2.models.enums import RequestStatus

from .deps import get_repository, get_settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["import"])


class ImportBody(BaseModel):
    request_name: str
    sandbox_mode: str = "cmssw"
    test_fraction: float | None = None
    events_per_job: int | None = None
    files_per_job: int | None = None
    max_files: int | None = None
    dry_run: bool = False


def _normalize_request(reqdata: dict) -> dict:
    """Normalize a ReqMgr2 request dict (same logic as cli._normalize_request)."""
    rtype = reqdata.get("RequestType", "")

    if rtype == "StepChain":
        step1 = reqdata.get("Step1", {})
        if not reqdata.get("InputDataset"):
            outputs = reqdata.get("OutputDatasets", [])
            reqdata["InputDataset"] = outputs[0] if outputs else step1.get("PrimaryDataset", "")
        if not reqdata.get("SplittingAlgo"):
            reqdata["SplittingAlgo"] = step1.get("SplittingAlgo", "EventBased")
        if not reqdata.get("EventsPerJob"):
            reqdata["EventsPerJob"] = step1.get("EventsPerJob")
        if not reqdata.get("RequestNumEvents"):
            reqdata["RequestNumEvents"] = step1.get("RequestNumEvents")
    elif rtype == "TaskChain":
        task1 = reqdata.get("Task1", {})
        if not reqdata.get("InputDataset"):
            outputs = reqdata.get("OutputDatasets", [])
            reqdata["InputDataset"] = outputs[0] if outputs else task1.get("InputDataset", "")
        if not reqdata.get("SplittingAlgo"):
            reqdata["SplittingAlgo"] = task1.get("SplittingAlgo", "FileBased")

    if rtype == "StepChain":
        step1 = reqdata.get("Step1", {})
        if not step1.get("InputDataset") and not step1.get("InputFromOutputModule"):
            reqdata["_is_gen"] = True

    if not reqdata.get("SandboxUrl"):
        reqdata["SandboxUrl"] = "N/A"

    if not reqdata.get("SplittingParams"):
        params = {}
        key_map = {
            "FilesPerJob": "files_per_job",
            "EventsPerJob": "events_per_job",
            "LumisPerJob": "lumis_per_job",
        }
        for key, snake in key_map.items():
            val = reqdata.get(key)
            if val is not None:
                params[snake] = val
        if params:
            reqdata["SplittingParams"] = params

    return reqdata


@router.post("/import")
async def import_request(
    body: ImportBody,
    raw_request: FastAPIRequest,
    repo: Repository = Depends(get_repository),
    settings=Depends(get_settings),
):
    """Import a request from ReqMgr2: fetch spec, create request+workflow+DAG."""
    reqmgr = getattr(raw_request.app.state, "reqmgr", None)
    dbs = getattr(raw_request.app.state, "dbs", None)
    rucio = getattr(raw_request.app.state, "rucio", None)
    condor = getattr(raw_request.app.state, "condor", None)

    if reqmgr is None:
        raise HTTPException(status_code=503, detail="ReqMgr2 adapter not available")

    # Check if request already exists
    existing = await repo.get_request(body.request_name)
    if existing:
        raise HTTPException(status_code=409, detail="Request already exists in WMS2")

    # 1. Fetch from ReqMgr2
    try:
        reqdata = await reqmgr.get_request(body.request_name)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch from ReqMgr2: {exc}")

    reqdata = _normalize_request(reqdata)

    # 2. Create request row
    await repo.create_request(
        request_name=body.request_name,
        requestor=reqdata.get("Requestor", "unknown"),
        request_data=reqdata,
        input_dataset=reqdata.get("InputDataset"),
        campaign=reqdata.get("Campaign"),
        priority=reqdata.get("RequestPriority", reqdata.get("Priority", 100000)),
        status="submitted",
    )
    session = repo.session
    await session.flush()

    # 3. Determine MergedLFNBase and build config_data
    merged_lfn_base = await determine_merged_lfn_base(reqdata, dbs_adapter=dbs)
    reqdata["MergedLFNBase"] = merged_lfn_base
    output_datasets_info = derive_merged_lfn_bases(reqdata)
    config_data = {
        "campaign": reqdata.get("Campaign"),
        "requestor": reqdata.get("Requestor"),
        "priority": reqdata.get("RequestPriority"),
        "request_type": reqdata.get("RequestType"),
        "output_datasets": output_datasets_info,
        "merged_lfn_base": merged_lfn_base,
        "unmerged_lfn_base": reqdata.get("UnmergedLFNBase", "/store/unmerged"),
        "cmssw_version": reqdata.get("CMSSWVersion"),
        "scram_arch": reqdata.get("ScramArch"),
        "global_tag": reqdata.get("GlobalTag"),
        "memory_mb": reqdata.get("Memory", 2048),
        "multicore": reqdata.get("Multicore", 1),
        "time_per_event": reqdata.get("TimePerEvent", 1.0),
        "size_per_event": reqdata.get("SizePerEvent", 1.5),
        "filter_efficiency": reqdata.get("FilterEfficiency", 1.0),
    }
    if reqdata.get("_is_gen"):
        config_data["_is_gen"] = True
        config_data["request_num_events"] = reqdata.get("RequestNumEvents", 0)
    if body.test_fraction is not None:
        config_data["test_fraction"] = body.test_fraction

    # Create sandbox
    submit_dir = os.path.join(settings.submit_base_dir, body.request_name)
    os.makedirs(submit_dir, exist_ok=True)
    sandbox_path = os.path.join(submit_dir, "sandbox.tar.gz")
    try:
        create_sandbox(sandbox_path, reqdata, mode=body.sandbox_mode)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create sandbox: {exc}")
    config_data["sandbox_path"] = sandbox_path

    # Extract manifest steps for StepChain pileup
    if reqdata.get("RequestType") == "StepChain" and reqdata.get("StepChain"):
        from dataclasses import asdict as _asdict
        from wms2.core.stepchain import parse_stepchain
        spec = parse_stepchain(reqdata)
        config_data["manifest_steps"] = [_asdict(s) for s in spec.steps]
        config_data["filter_efficiency"] = spec.filter_efficiency

    workflow = await repo.create_workflow(
        request_name=body.request_name,
        input_dataset=reqdata["InputDataset"],
        splitting_algo=reqdata["SplittingAlgo"],
        splitting_params=reqdata.get("SplittingParams", {}),
        sandbox_url=reqdata.get("SandboxUrl", "N/A"),
        config_data=config_data,
    )

    # Override splitting params if provided
    if body.files_per_job is not None or body.events_per_job is not None:
        params = workflow.splitting_params or {}
        if body.files_per_job is not None:
            params["files_per_job"] = body.files_per_job
        if body.events_per_job is not None:
            params["events_per_job"] = body.events_per_job
        await repo.update_workflow(workflow.id, splitting_params=params)
        workflow = await repo.get_workflow(workflow.id)

    await session.flush()

    # 4. Plan + submit DAG (unless dry_run)
    dag_id = None
    message = f"Request {body.request_name} imported"

    if not body.dry_run and condor is not None:
        try:
            dp = DAGPlanner(repo, dbs, rucio, condor, settings)
            dag = await dp.plan_production_dag(workflow, adaptive=True)
            await session.flush()
            dag_id = str(dag.id)

            # Transition to ACTIVE
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            req_row = await repo.get_request(body.request_name)
            old_transitions = req_row.status_transitions or []
            new_transition = {
                "from": req_row.status,
                "to": RequestStatus.ACTIVE.value,
                "timestamp": now.isoformat(),
            }
            await repo.update_request(
                body.request_name,
                status=RequestStatus.ACTIVE.value,
                status_transitions=old_transitions + [new_transition],
            )
            message += f", DAG submitted (cluster {dag.dagman_cluster_id})"
        except Exception as exc:
            logger.exception("DAG planning/submission failed for %s", body.request_name)
            message += f" (DAG submission failed: {exc})"
    elif body.dry_run:
        message += " (dry run — no DAG submitted)"

    return {
        "request_name": body.request_name,
        "workflow_id": str(workflow.id),
        "dag_id": dag_id,
        "message": message,
    }
