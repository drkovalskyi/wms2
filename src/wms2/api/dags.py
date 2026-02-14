from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/dags", tags=["dags"])


@router.get("")
async def list_dags():
    raise HTTPException(status_code=501, detail="Not implemented — Phase 2")


@router.get("/{dag_id}")
async def get_dag(dag_id: str):
    raise HTTPException(status_code=501, detail="Not implemented — Phase 2")
