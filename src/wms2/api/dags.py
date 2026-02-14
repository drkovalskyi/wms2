from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from wms2.api.deps import get_repository
from wms2.db.repository import Repository

router = APIRouter(prefix="/dags", tags=["dags"])


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
    return [
        {
            "id": str(r.id),
            "workflow_id": str(r.workflow_id) if r.workflow_id else None,
            "status": r.status,
            "total_nodes": r.total_nodes,
            "total_work_units": r.total_work_units,
            "dag_file_path": r.dag_file_path,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in rows
    ]


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

    return {
        "id": str(row.id),
        "workflow_id": str(row.workflow_id) if row.workflow_id else None,
        "dag_file_path": row.dag_file_path,
        "submit_dir": row.submit_dir,
        "rescue_dag_path": row.rescue_dag_path,
        "dagman_cluster_id": row.dagman_cluster_id,
        "schedd_name": row.schedd_name,
        "parent_dag_id": str(row.parent_dag_id) if row.parent_dag_id else None,
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
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }
