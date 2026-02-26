from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from wms2.api.deps import get_repository
from wms2.db.repository import Repository

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("")
async def list_workflows(
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    rows = await repo.list_workflows(status=status, limit=limit, offset=offset)
    return [
        {
            "id": str(r.id),
            "request_name": r.request_name,
            "input_dataset": r.input_dataset,
            "splitting_algo": r.splitting_algo,
            "status": r.status,
            "total_nodes": r.total_nodes,
            "nodes_done": r.nodes_done,
            "nodes_failed": r.nodes_failed,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


@router.get("/{workflow_id}")
async def get_workflow(
    workflow_id: str,
    repo: Repository = Depends(get_repository),
):
    try:
        wf_uuid = UUID(workflow_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")

    row = await repo.get_workflow(wf_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found")

    return {
        "id": str(row.id),
        "request_name": row.request_name,
        "input_dataset": row.input_dataset,
        "splitting_algo": row.splitting_algo,
        "splitting_params": row.splitting_params,
        "sandbox_url": row.sandbox_url,
        "config_data": row.config_data,
        "pilot_cluster_id": row.pilot_cluster_id,
        "pilot_schedd": row.pilot_schedd,
        "step_metrics": row.step_metrics,
        "current_round": row.current_round,
        "next_first_event": row.next_first_event,
        "file_offset": row.file_offset,
        "dag_id": str(row.dag_id) if row.dag_id else None,
        "category_throttles": row.category_throttles,
        "status": row.status,
        "total_nodes": row.total_nodes,
        "nodes_done": row.nodes_done,
        "nodes_failed": row.nodes_failed,
        "nodes_queued": row.nodes_queued,
        "nodes_running": row.nodes_running,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
