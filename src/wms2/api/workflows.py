from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("")
async def list_workflows():
    raise HTTPException(status_code=501, detail="Not implemented — Phase 2")


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str):
    raise HTTPException(status_code=501, detail="Not implemented — Phase 2")
