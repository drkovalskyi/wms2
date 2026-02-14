from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException

from wms2.db.repository import Repository
from wms2.models.enums import RequestStatus
from wms2.models.request import Request, RequestCreate, RequestUpdate

from .deps import get_repository

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


@router.get("", response_model=list[Request])
async def list_requests(
    status: str | None = None,
    campaign: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    rows = await repo.list_requests(status=status, campaign=campaign, limit=limit, offset=offset)
    return [Request.model_validate(r) for r in rows]


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


@router.delete("/{request_name}", response_model=Request)
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
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")
    if existing.status != RequestStatus.ACTIVE.value:
        raise HTTPException(status_code=400, detail="Can only stop active requests")
    # Clean stop would be initiated by the lifecycle manager
    return {"message": "Clean stop requested", "request_name": request_name}


@router.post("/{request_name}/restart", response_model=dict)
async def restart_request(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    existing = await repo.get_request(request_name)
    if not existing:
        raise HTTPException(status_code=404, detail="Request not found")
    return {"message": "Restart requested", "request_name": request_name}


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
