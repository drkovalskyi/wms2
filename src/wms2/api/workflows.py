from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from wms2.api.deps import get_repository
from wms2.db.repository import Repository

router = APIRouter(prefix="/workflows", tags=["workflows"])


def _workflow_summary(r):
    return {
        "id": str(r.id),
        "request_name": r.request_name,
        "input_dataset": r.input_dataset,
        "splitting_algo": r.splitting_algo,
        "status": r.status,
        "current_round": r.current_round,
        "total_nodes": r.total_nodes,
        "nodes_done": r.nodes_done,
        "nodes_failed": r.nodes_failed,
        "nodes_running": r.nodes_running,
        "nodes_queued": r.nodes_queued,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _workflow_detail(row):
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
        "pilot_output_path": row.pilot_output_path,
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


def _block_detail(b):
    return {
        "id": str(b.id),
        "workflow_id": str(b.workflow_id) if b.workflow_id else None,
        "block_index": b.block_index,
        "dataset_name": b.dataset_name,
        "total_work_units": b.total_work_units,
        "completed_work_units": b.completed_work_units,
        "dbs_block_name": b.dbs_block_name,
        "dbs_block_open": b.dbs_block_open,
        "dbs_block_closed": b.dbs_block_closed,
        "source_rule_ids": b.source_rule_ids,
        "tape_rule_id": b.tape_rule_id,
        "rucio_attempt_count": b.rucio_attempt_count,
        "rucio_last_error": b.rucio_last_error,
        "status": b.status,
        "created_at": b.created_at.isoformat() if b.created_at else None,
        "updated_at": b.updated_at.isoformat() if b.updated_at else None,
    }


@router.get("")
async def list_workflows(
    status: str | None = None,
    request_name: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    if request_name:
        row = await repo.get_workflow_by_request(request_name)
        return [_workflow_summary(row)] if row else []
    rows = await repo.list_workflows(status=status, limit=limit, offset=offset)
    return [_workflow_summary(r) for r in rows]


@router.get("/by-request/{request_name}")
async def get_workflow_by_request(
    request_name: str,
    repo: Repository = Depends(get_repository),
):
    """Look up the workflow for a given request name."""
    row = await repo.get_workflow_by_request(request_name)
    if not row:
        raise HTTPException(status_code=404, detail="No workflow found for this request")
    return _workflow_detail(row)


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

    return _workflow_detail(row)


@router.get("/{workflow_id}/blocks")
async def get_workflow_blocks(
    workflow_id: str,
    repo: Repository = Depends(get_repository),
):
    """Get processing blocks for a workflow."""
    try:
        wf_uuid = UUID(workflow_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")

    row = await repo.get_workflow(wf_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="Workflow not found")

    blocks = await repo.get_processing_blocks(wf_uuid)
    return [_block_detail(b) for b in blocks]
