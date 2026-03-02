from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request as FastAPIRequest

from wms2.api.deps import get_repository
from wms2.db.repository import Repository

router = APIRouter(prefix="/dags", tags=["dags"])


def _dag_summary(r):
    return {
        "id": str(r.id),
        "workflow_id": str(r.workflow_id) if r.workflow_id else None,
        "status": r.status,
        "total_nodes": r.total_nodes,
        "total_work_units": r.total_work_units,
        "nodes_done": r.nodes_done,
        "nodes_failed": r.nodes_failed,
        "nodes_running": r.nodes_running,
        "dag_file_path": r.dag_file_path,
        "dagman_cluster_id": r.dagman_cluster_id,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _dag_detail(row):
    return {
        "id": str(row.id),
        "workflow_id": str(row.workflow_id) if row.workflow_id else None,
        "dag_file_path": row.dag_file_path,
        "submit_dir": row.submit_dir,
        "rescue_dag_path": row.rescue_dag_path,
        "dagman_cluster_id": row.dagman_cluster_id,
        "schedd_name": row.schedd_name,
        "parent_dag_id": str(row.parent_dag_id) if row.parent_dag_id else None,
        "stop_requested_at": row.stop_requested_at.isoformat() if row.stop_requested_at else None,
        "stop_reason": row.stop_reason,
        "status": row.status,
        "total_nodes": row.total_nodes,
        "total_edges": row.total_edges,
        "node_counts": row.node_counts,
        "nodes_idle": row.nodes_idle,
        "nodes_running": row.nodes_running,
        "nodes_done": row.nodes_done,
        "nodes_failed": row.nodes_failed,
        "nodes_held": row.nodes_held,
        "total_work_units": row.total_work_units,
        "completed_work_units": row.completed_work_units,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }


@router.get("")
async def list_dags(
    status: str | None = None,
    workflow_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    wf_uuid = None
    if workflow_id:
        try:
            wf_uuid = UUID(workflow_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid workflow_id format")

    rows = await repo.list_dags(
        status=status, workflow_id=wf_uuid, limit=limit, offset=offset
    )
    return [_dag_summary(r) for r in rows]


@router.get("/{dag_id}")
async def get_dag(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    return _dag_detail(row)


@router.get("/{dag_id}/history")
async def get_dag_history(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    """Get the event history for a DAG (status transitions, node counts over time)."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    history = await repo.get_dag_history(d_uuid)
    return [
        {
            "id": h.id,
            "dag_id": str(h.dag_id) if h.dag_id else None,
            "event_type": h.event_type,
            "from_status": h.from_status,
            "to_status": h.to_status,
            "nodes_done": h.nodes_done,
            "nodes_failed": h.nodes_failed,
            "nodes_running": h.nodes_running,
            "detail": h.detail,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        }
        for h in history
    ]


@router.get("/{dag_id}/jobs")
async def get_dag_jobs(
    dag_id: str,
    raw_request: FastAPIRequest,
    repo: Repository = Depends(get_repository),
):
    """Get live HTCondor job details for all payload jobs in this DAG."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    if not row.dagman_cluster_id:
        return []

    condor = getattr(raw_request.app.state, "condor", None)
    if condor is None:
        return []

    jobs = await condor.query_dag_jobs(row.dagman_cluster_id)
    return jobs if jobs is not None else []
