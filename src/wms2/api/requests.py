import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request as FastAPIRequest
from pydantic import BaseModel

from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, RequestStatus
from wms2.models.request import Request, RequestCreate, RequestUpdate

from .deps import get_repository, get_settings

logger = logging.getLogger(__name__)

# Simple TTL cache for expensive filesystem-scanning endpoints.
# Both /errors and /site-performance scan sshfs-mounted spool directories
# which involves thousands of file operations with network latency.
# Results only change on round completion (~minutes), so a 30s TTL is safe.
_api_cache: dict[str, tuple[float, object]] = {}
_CACHE_TTL = 30  # seconds


def _cache_get(key: str) -> object | None:
    entry = _api_cache.get(key)
    if entry and time.monotonic() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value: object) -> None:
    _api_cache[key] = (time.monotonic(), value)


class StopRequest(BaseModel):
    reason: str = "Operator-initiated clean stop"


class PriorityProfileUpdate(BaseModel):
    high: int
    nominal: int
    switch_fraction: float

router = APIRouter(prefix="/requests", tags=["requests"])


@router.post("", response_model=Request, status_code=201)
async def create_request(
    body: RequestCreate,
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(body.request_name)
    if existing:
        raise HTTPException(status_code=409, detail="Request already exists")

    now = datetime.now(timezone.utc)
    request_data = body.model_dump(
        exclude={"request_name", "requestor", "requestor_dn", "campaign",
                 "priority", "urgent", "production_steps", "payload_config",
                 "input_dataset", "splitting_params", "cleanup_policy"}
    )
    row = await repo.create_request(
        request_name=body.request_name,
        requestor=body.requestor,
        requestor_dn=body.requestor_dn,
        request_data=request_data,
        payload_config=body.payload_config,
        splitting_params=body.splitting_params,
        input_dataset=body.input_dataset,
        campaign=body.campaign,
        priority=body.priority,
        urgent=body.urgent,
        production_steps=[s.model_dump() for s in body.production_steps],
        status=RequestStatus.NEW.value,
        status_transitions=[],
        cleanup_policy=body.cleanup_policy.value,
        created_at=now,
        updated_at=now,
    )
    return Request.model_validate(row)


@router.get("")
async def list_requests(
    status: str | None = None,
    campaign: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    rows = await repo.list_requests_with_progress(
        status=status, campaign=campaign, limit=limit, offset=offset,
    )
    result = []
    for item in rows:
        req = Request.model_validate(item["request"])
        d = req.model_dump(mode="json")
        ep = item["events_produced"] or 0
        te = item["target_events"] or 0
        d["events_produced"] = ep
        d["target_events"] = te
        d["progress_pct"] = (ep / te * 100) if te > 0 else None
        cd = item.get("config_data") or {}
        d["condor_pool"] = cd.get("condor_pool", "local")
        d["stageout_mode"] = cd.get("stageout_mode", "local")
        result.append(d)
    return result


@router.get("/{request_name}", response_model=Request)
async def get_request(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    row = await repo.get_request(request_name)
    if not row:
        raise HTTPException(status_code=404, detail="Request not found")
    return Request.model_validate(row)


@router.patch("/{request_name}", response_model=Request)
async def update_request(
    request_name: str,
    body: RequestUpdate,
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    updates = body.model_dump(exclude_none=True)
    if "production_steps" in updates:
        updates["production_steps"] = [s.model_dump() for s in body.production_steps]

    if not updates:
        return Request.model_validate(existing)

    row = await repo.update_request(request_name, **updates)
    return Request.model_validate(row)


@router.post("/{request_name}/abort", response_model=Request)
async def abort_request(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    terminal = (RequestStatus.COMPLETED.value, RequestStatus.FAILED.value, RequestStatus.ABORTED.value)
    if existing.status in terminal:
        raise HTTPException(status_code=400, detail=f"Cannot abort request in {existing.status} state")

    now = datetime.now(timezone.utc)
    old_transitions = existing.status_transitions or []
    new_transition = {
        "from": existing.status,
        "to": RequestStatus.ABORTED.value,
        "timestamp": now.isoformat(),
    }
    row = await repo.update_request(
        request_name,
        status=RequestStatus.ABORTED.value,
        status_transitions=old_transitions + [new_transition],
        updated_at=now,
    )
    return Request.model_validate(row)


@router.post("/{request_name}/stop", response_model=dict)
async def stop_request(
    request_name: str,
    body: StopRequest = StopRequest(),
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    stoppable = (RequestStatus.ACTIVE.value, RequestStatus.PILOT_RUNNING.value)
    if existing.status not in stoppable:
        raise HTTPException(
            status_code=400,
            detail=f"Can only stop requests in active or pilot_running state, "
                   f"current status: {existing.status}",
        )

    previous_status = existing.status

    # Mark current DAG as stop-requested and remove from condor
    workflow = await repo.get_workflow_by_request(request_name)
    if workflow and workflow.dag_id:
        now = datetime.now(timezone.utc)
        dag = await repo.get_dag(workflow.dag_id)
        if dag and dag.status in (DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value):
            try:
                condor = raw_request.app.state.condor
                await condor.remove_job(
                    schedd_name=dag.schedd_name,
                    cluster_id=dag.dagman_cluster_id,
                )
            except Exception:
                logger.warning(
                    "Failed to condor_rm DAG %s for %s — lifecycle manager will retry",
                    dag.dagman_cluster_id, request_name, exc_info=True,
                )
        await repo.update_dag(
            workflow.dag_id,
            stop_requested_at=now,
            stop_reason=body.reason,
        )

    # Transition to STOPPING
    now = datetime.now(timezone.utc)
    old_transitions = existing.status_transitions or []
    new_transition = {
        "from": previous_status,
        "to": RequestStatus.STOPPING.value,
        "timestamp": now.isoformat(),
    }
    await repo.update_request(
        request_name,
        status=RequestStatus.STOPPING.value,
        status_transitions=old_transitions + [new_transition],
        updated_at=now,
    )
    logger.info("Request %s: %s -> stopping (reason: %s)", request_name, previous_status, body.reason)

    return {
        "request_name": request_name,
        "status": "stopping",
        "previous_status": previous_status,
        "stop_reason": body.reason,
        "message": f"Clean stop initiated for {request_name}",
    }


@router.post("/{request_name}/release", response_model=dict)
async def release_request(
    request_name: str,
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
):
    """Release a HELD request back to queue, or resume a PAUSED request."""
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    releasable = (RequestStatus.HELD.value, RequestStatus.PAUSED.value)
    if existing.status not in releasable:
        raise HTTPException(
            status_code=400,
            detail=f"Can only release requests in held or paused state, "
                   f"current status: {existing.status}",
        )

    previous_status = existing.status
    # PAUSED needs recovery prep → RESUBMITTING; HELD goes straight to QUEUED
    if previous_status == RequestStatus.PAUSED.value:
        target_status = RequestStatus.RESUBMITTING.value
    else:
        target_status = RequestStatus.QUEUED.value

    now = datetime.now(timezone.utc)
    old_transitions = existing.status_transitions or []
    new_transition = {
        "from": previous_status,
        "to": target_status,
        "timestamp": now.isoformat(),
    }
    await repo.update_request(
        request_name,
        status=target_status,
        status_transitions=old_transitions + [new_transition],
        updated_at=now,
    )
    logger.info("Request %s: %s -> %s (operator release)", request_name, previous_status, target_status)

    return {
        "request_name": request_name,
        "status": target_status,
        "previous_status": previous_status,
        "message": f"Request {request_name} {'resumed' if previous_status == 'paused' else 'released to admission queue'}",
    }


@router.post("/{request_name}/fail", response_model=dict)
async def fail_request(
    request_name: str,
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
):
    """Fail a request: kill DAG and mark as failed.

    For running requests (active, pilot_running), goes through STOPPING first
    so condor_rm drains cleanly. The lifecycle manager's _handle_stopping
    recognises the "FAIL:" stop_reason prefix and transitions to FAILED
    instead of PAUSED once DAGMan exits.

    For already-stopped requests (held, partial, paused, stopping), marks
    everything failed immediately.
    """
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    allowed = (
        RequestStatus.HELD.value, RequestStatus.PARTIAL.value, RequestStatus.PAUSED.value,
        RequestStatus.ACTIVE.value, RequestStatus.PILOT_RUNNING.value, RequestStatus.STOPPING.value,
    )
    if existing.status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Can only fail requests in active, pilot_running, stopping, held, partial, or paused state, "
                   f"current status: {existing.status}",
        )

    previous_status = existing.status

    # For running requests, go through clean stop first
    running = (RequestStatus.ACTIVE.value, RequestStatus.PILOT_RUNNING.value)
    if previous_status in running:
        workflow = await repo.get_workflow_by_request(request_name)
        if workflow and workflow.dag_id:
            now = datetime.now(timezone.utc)
            dag = await repo.get_dag(workflow.dag_id)
            if dag and dag.status in (DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value):
                condor = getattr(raw_request.app.state, "condor", None)
                if condor:
                    try:
                        await condor.remove_job(
                            schedd_name=dag.schedd_name,
                            cluster_id=dag.dagman_cluster_id,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to condor_rm DAG %s for %s — lifecycle manager will retry",
                            dag.dagman_cluster_id, request_name, exc_info=True,
                        )
            # "FAIL:" prefix tells _handle_stopping to go to FAILED, not PAUSED
            await repo.update_dag(
                workflow.dag_id,
                stop_requested_at=now,
                stop_reason="FAIL: operator-initiated",
            )

        # Transition to STOPPING — lifecycle manager will finish the job
        now = datetime.now(timezone.utc)
        old_transitions = existing.status_transitions or []
        new_transition = {
            "from": previous_status,
            "to": RequestStatus.STOPPING.value,
            "timestamp": now.isoformat(),
        }
        await repo.update_request(
            request_name,
            status=RequestStatus.STOPPING.value,
            status_transitions=old_transitions + [new_transition],
            updated_at=now,
        )
        logger.info("Request %s: %s -> stopping (fail requested, waiting for DAGMan exit)", request_name, previous_status)

        return {
            "request_name": request_name,
            "status": "stopping",
            "previous_status": previous_status,
            "message": f"Fail initiated for {request_name} — removing condor jobs, will mark failed when done",
        }

    # For already-stopped requests (stopping, held, partial, paused):
    # If STOPPING, update the stop_reason so lifecycle manager transitions to FAILED
    if previous_status == RequestStatus.STOPPING.value:
        workflow = await repo.get_workflow_by_request(request_name)
        if workflow and workflow.dag_id:
            dag = await repo.get_dag(workflow.dag_id)
            if dag:
                await repo.update_dag(workflow.dag_id, stop_reason="FAIL: operator-initiated")
        return {
            "request_name": request_name,
            "status": "stopping",
            "previous_status": previous_status,
            "message": f"Request {request_name} already stopping — will mark failed when DAGMan exits",
        }

    # Immediate fail for held/partial/paused — no running DAG to drain
    workflow = await repo.get_workflow_by_request(request_name)
    if workflow:
        if workflow.dag_id:
            dag = await repo.get_dag(workflow.dag_id)
            if dag and dag.status in (DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value):
                condor = getattr(raw_request.app.state, "condor", None)
                if condor:
                    try:
                        await condor.remove_job(
                            schedd_name=dag.schedd_name,
                            cluster_id=dag.dagman_cluster_id,
                        )
                    except Exception:
                        logger.warning("Failed to remove DAG %s", dag.id)

        # Mark non-terminal DAGs as failed
        for dag in await repo.list_dags(workflow_id=workflow.id):
            if dag.status not in (DAGStatus.FAILED.value, DAGStatus.COMPLETED.value):
                await repo.update_dag(dag.id, status=DAGStatus.FAILED.value)

        # Mark open processing blocks as failed
        for block in await repo.get_processing_blocks(workflow.id):
            if block.status == "open":
                await repo.update_processing_block(block.id, status="failed")

        # Mark workflow as failed
        await repo.update_workflow(workflow.id, status="failed")

    # Transition to FAILED
    now = datetime.now(timezone.utc)
    old_transitions = existing.status_transitions or []
    new_transition = {
        "from": previous_status,
        "to": RequestStatus.FAILED.value,
        "timestamp": now.isoformat(),
    }
    await repo.update_request(
        request_name,
        status=RequestStatus.FAILED.value,
        status_transitions=old_transitions + [new_transition],
        updated_at=now,
    )
    logger.info("Request %s: %s -> failed (operator-initiated)", request_name, previous_status)

    return {
        "request_name": request_name,
        "status": "failed",
        "previous_status": previous_status,
        "message": f"Request {request_name} marked as failed",
    }


@router.delete("/{request_name}", response_model=dict)
async def delete_request(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    """Delete a failed or aborted request and all related rows."""
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    deletable = ("failed", "aborted")
    if existing.status not in deletable:
        raise HTTPException(
            status_code=400,
            detail=f"Can only delete requests in failed/aborted state, "
                   f"current status: {existing.status}",
        )

    workflow = await repo.get_workflow_by_request(request_name)
    if workflow:
        for dag in await repo.list_dags(workflow_id=workflow.id):
            await repo.delete_dag_history(dag.id)
            await repo.delete_dag(dag.id)
        for block in await repo.get_processing_blocks(workflow.id):
            await repo.delete_processing_block(block.id)
        await repo.delete_workflow(workflow.id)
    await repo.delete_request(request_name)
    logger.info("Deleted request %s (was %s)", request_name, existing.status)

    return {
        "request_name": request_name,
        "message": f"Request {request_name} deleted",
    }


@router.post("/{request_name}/restart", response_model=dict)
async def restart_request(
    request_name: str,
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
):
    """Kill+clone: create new request with incremented version, fail old."""
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    allowed = (
        RequestStatus.HELD.value, RequestStatus.PARTIAL.value,
        RequestStatus.ABORTED.value, RequestStatus.FAILED.value,
        RequestStatus.PAUSED.value,
    )
    if existing.status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Can only restart requests in held, partial, paused, aborted, or failed state, "
                   f"current status: {existing.status}",
        )

    lm = getattr(raw_request.app.state, "lifecycle_manager", None)
    if lm is None:
        raise HTTPException(status_code=503, detail="Lifecycle manager not available")

    new_name = await lm.restart_request(request_name)
    return {
        "request_name": request_name,
        "new_request_name": new_name,
        "status": "failed",
        "message": f"Request {request_name} restarted as {new_name}",
    }


@router.post("/{request_name}/clone", response_model=dict)
async def clone_request(
    request_name: str,
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
    settings=Depends(get_settings),
):
    """Kill, rename old request aside, and re-import with incremented version.

    Flow: rename old → {name}_Failed_vN, kill its DAG, import fresh with
    the original name. Old request is kept for reference (manual delete).
    """
    from wms2.api.import_endpoint import ImportBody, import_request

    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    reqdata = existing.request_data or {}
    workflow = await repo.get_workflow_by_request(request_name)
    config_data = (workflow.config_data if workflow else None) or {}

    # Determine versions
    current_version = reqdata.get("ProcessingVersion",
                                  reqdata.get("processing_version", 1))
    new_version = current_version + 1

    # 1. Rename old request out of the way
    old_name = f"{request_name}_Failed_v{current_version}"
    await repo.rename_request(request_name, old_name)
    await repo.session.flush()
    logger.info("Clone %s: renamed old request to %s", request_name, old_name)

    # 2. Kill running DAG on the old request
    if workflow and workflow.dag_id:
        dag = await repo.get_dag(workflow.dag_id)
        if dag and dag.status in (DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value):
            condor = getattr(raw_request.app.state, "condor", None)
            if condor:
                try:
                    await condor.remove_job(
                        schedd_name=dag.schedd_name,
                        cluster_id=dag.dagman_cluster_id,
                    )
                    logger.info("Clone %s: removed DAG %s from %s",
                                request_name, dag.dagman_cluster_id, dag.schedd_name)
                except Exception:
                    logger.warning("Clone %s: failed to remove DAG %s",
                                   request_name, dag.dagman_cluster_id, exc_info=True)

    # Mark old request as failed
    await repo.update_request(old_name, status="failed")
    await repo.session.flush()

    # 3. Import fresh with original name
    priority_profile = config_data.get("priority_profile", {})
    import_body = ImportBody(
        request_name=request_name,
        source_request_name=request_name,
        stageout_mode=config_data.get("stageout_mode", "test"),
        condor_pool=config_data.get("condor_pool", "global"),
        test_fraction=config_data.get("test_fraction"),
        processing_version=new_version,
        allowed_sites=config_data.get("allowed_sites", ""),
        work_units_per_round=config_data.get("work_units_per_round"),
        high_priority=priority_profile.get("high", 5),
        nominal_priority=priority_profile.get("nominal", 3),
        priority_switch_fraction=priority_profile.get("switch_fraction", 0.5),
        pilot_fraction=config_data.get("pilot_fraction"),
        pilot_throwaway=config_data.get("pilot_throwaway"),
    )

    try:
        result = await import_request(import_body, raw_request, repo, settings)
    except Exception:
        # Import failed — rename old request back so nothing is lost
        logger.warning("Clone %s: import failed, restoring old request", request_name)
        # Clean up any partial import records
        partial_wf = await repo.get_workflow_by_request(request_name)
        if partial_wf:
            for d in await repo.list_dags(workflow_id=partial_wf.id):
                await repo.delete_dag_history(d.id)
                await repo.delete_dag(d.id)
            for block in await repo.get_processing_blocks(partial_wf.id):
                await repo.delete_processing_block(block.id)
            await repo.delete_workflow(partial_wf.id)
        partial_req = await repo.get_request(request_name)
        if partial_req:
            await repo.delete_request(request_name)
        await repo.rename_request(old_name, request_name)
        await repo.session.flush()
        raise

    result["cloned_from_version"] = current_version
    result["new_version"] = new_version
    result["old_request_name"] = old_name
    logger.info("Clone %s: v%d → v%d (old kept as %s)",
                request_name, current_version, new_version, old_name)
    return result


@router.get("/{request_name}/errors", response_model=dict)
async def get_request_errors(
    request_name: str,
    raw_request: FastAPIRequest = None,
    repo: Repository = Depends(get_repository),
):
    """Get error summary for a request (any state)."""
    cached = _cache_get(f"errors:{request_name}")
    if cached is not None:
        return cached

    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    lm = getattr(raw_request.app.state, "lifecycle_manager", None)
    if lm is None:
        raise HTTPException(status_code=503, detail="Lifecycle manager not available")

    result = await lm.get_error_summary(request_name)
    _cache_set(f"errors:{request_name}", result)
    return result


@router.get("/{request_name}/versions", response_model=list[Request])
async def get_request_versions(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    row = await repo.get_request(request_name)
    if not row:
        raise HTTPException(status_code=404, detail="Request not found")

    versions = [Request.model_validate(row)]
    # Walk version chain backward
    current = row
    while current.previous_version_request:
        prev = await repo.get_request(current.previous_version_request)
        if not prev:
            break
        versions.insert(0, Request.model_validate(prev))
        current = prev
    # Walk version chain forward
    current = row
    while current.superseded_by_request:
        nxt = await repo.get_request(current.superseded_by_request)
        if not nxt:
            break
        versions.append(Request.model_validate(nxt))
        current = nxt

    return versions


@router.patch("/{request_name}/priority-profile", response_model=dict)
async def update_priority_profile(
    request_name: str,
    body: PriorityProfileUpdate,
    repo: Repository = Depends(get_repository),
):
    """Update the priority profile for a request (high, nominal, switch_fraction)."""
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    if body.switch_fraction < 0 or body.switch_fraction > 1:
        raise HTTPException(status_code=422, detail="switch_fraction must be between 0 and 1")

    profile = {"high": body.high, "nominal": body.nominal, "switch_fraction": body.switch_fraction}

    # Update request_data._priority_profile
    rd = dict(existing.request_data or {})
    # Preserve pilot from existing profile if present
    old_profile = rd.get("_priority_profile", {})
    profile["pilot"] = old_profile.get("pilot", body.high)
    rd["_priority_profile"] = profile
    await repo.update_request(request_name, request_data=rd)

    # Update workflow config_data.priority_profile
    workflow = await repo.get_workflow_by_request(request_name)
    if workflow:
        cd = dict(workflow.config_data or {})
        cd["priority_profile"] = profile
        await repo.update_workflow(workflow.id, config_data=cd)

    logger.info("Priority profile updated for %s: %s", request_name, profile)
    return {"request_name": request_name, "priority_profile": profile}


# ---------------------------------------------------------------------------
# Site performance
# ---------------------------------------------------------------------------

_NODE_COMPLETED_RE = re.compile(
    r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) Node (\S+) job completed"
)


def _scan_site_performance(dag_infos: list[dict]) -> list[dict]:
    """Scan spool directories for per-WU site and timing data (blocking I/O).

    dag_infos: [{"submit_dir": str, "round": int}, ...]
    Returns list of per-WU dicts.
    """
    results = []
    for dag_info in dag_infos:
        submit_dir = dag_info["submit_dir"]
        dag_round = dag_info["round"]
        if not os.path.isdir(submit_dir):
            continue
        # Find mg_* subdirectories
        try:
            entries = os.listdir(submit_dir)
        except OSError:
            continue
        mg_dirs = sorted(e for e in entries if e.startswith("mg_") and
                         os.path.isdir(os.path.join(submit_dir, e)))
        for mg_name in mg_dirs:
            mg_path = os.path.join(submit_dir, mg_name)
            # Get elected site
            site_file = os.path.join(submit_dir, f"{mg_name}_elected_site")
            if not os.path.isfile(site_file):
                continue
            try:
                site = open(site_file).read().strip()
            except OSError:
                continue
            if not site:
                continue

            # Check DagStatus — only include completed (5) WUs
            status_file = os.path.join(mg_path, "group.dag.status")
            if os.path.isfile(status_file):
                try:
                    content = open(status_file).read()
                    m = re.search(r"DagStatus\s*=\s*(\d+)", content)
                    dag_status = int(m.group(1)) if m else None
                except OSError:
                    dag_status = None
            else:
                dag_status = None

            # Parse dagman.out for completion timestamps
            dagman_out = os.path.join(mg_path, "group.dag.dagman.out")
            if not os.path.isfile(dagman_out):
                continue
            try:
                text = open(dagman_out).read()
            except OSError:
                continue

            landing_ts = None
            last_proc_ts = None
            merge_ts = None
            cleanup_ts = None
            for match in _NODE_COMPLETED_RE.finditer(text):
                ts_str, node = match.group(1), match.group(2)
                try:
                    ts = datetime.strptime(ts_str, "%m/%d/%y %H:%M:%S")
                except ValueError:
                    continue
                if node == "landing":
                    landing_ts = ts
                elif node.startswith("proc_"):
                    if last_proc_ts is None or ts > last_proc_ts:
                        last_proc_ts = ts
                elif node == "merge":
                    merge_ts = ts
                elif node == "cleanup":
                    cleanup_ts = ts

            # Compute durations (only if we have landing + last_proc at minimum)
            if not landing_ts or not last_proc_ts:
                continue
            proc_sec = (last_proc_ts - landing_ts).total_seconds()

            merge_sec = None
            if merge_ts and last_proc_ts:
                merge_sec = (merge_ts - last_proc_ts).total_seconds()

            total_sec = None
            if cleanup_ts:
                total_sec = (cleanup_ts - landing_ts).total_seconds()
            elif merge_ts:
                total_sec = (merge_ts - landing_ts).total_seconds()

            completed = dag_status == 5
            results.append({
                "site": site,
                "round": dag_round,
                "mg_name": mg_name,
                "proc_sec": proc_sec,
                "merge_sec": merge_sec,
                "total_sec": total_sec,
                "completed": completed,
            })
    return results


@router.get("/{request_name}/site-performance")
async def get_site_performance(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    """Site performance aggregated from DAG spool directories (excludes round 0)."""
    cached = _cache_get(f"site_perf:{request_name}")
    if cached is not None:
        return cached

    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")

    workflow = await repo.get_workflow_by_request(request_name)
    if not workflow:
        return {"sites": [], "total_wus": 0}

    dags = await repo.list_dags(workflow_id=workflow.id, limit=1000)
    # Sort by created_at ascending (list_dags returns desc)
    dags.sort(key=lambda d: d.created_at)

    # Build dag_infos, skip round 0 (first DAG)
    dag_infos = []
    for i, dag in enumerate(dags):
        if i == 0:
            continue  # skip round 0
        if not dag.submit_dir:
            continue
        dag_infos.append({"submit_dir": dag.submit_dir, "round": i})

    if not dag_infos:
        return {"sites": [], "total_wus": 0}

    # Run blocking I/O in thread pool
    loop = asyncio.get_running_loop()
    raw = await loop.run_in_executor(None, _scan_site_performance, dag_infos)

    # Aggregate by site (only completed WUs for timing averages)
    site_map: dict[str, dict] = {}
    for wu in raw:
        site = wu["site"]
        if site not in site_map:
            site_map[site] = {"wus_done": 0, "wus_running": 0,
                              "proc_secs": [], "merge_secs": [], "total_secs": []}
        entry = site_map[site]
        if wu["completed"]:
            entry["wus_done"] += 1
            entry["proc_secs"].append(wu["proc_sec"])
            if wu["merge_sec"] is not None:
                entry["merge_secs"].append(wu["merge_sec"])
            if wu["total_sec"] is not None:
                entry["total_secs"].append(wu["total_sec"])
        else:
            entry["wus_running"] += 1

    def _avg(lst):
        return round(sum(lst) / len(lst) / 60, 1) if lst else None

    sites = []
    for site, d in site_map.items():
        sites.append({
            "site": site,
            "wus_done": d["wus_done"],
            "wus_running": d["wus_running"],
            "avg_proc_min": _avg(d["proc_secs"]),
            "avg_merge_min": _avg(d["merge_secs"]),
            "avg_total_min": _avg(d["total_secs"]),
        })
    sites.sort(key=lambda s: -(s["wus_done"] + s["wus_running"]))

    total = sum(s["wus_done"] + s["wus_running"] for s in sites)
    result = {"sites": sites, "total_wus": total}
    _cache_set(f"site_perf:{request_name}", result)
    return result
