from fastapi import APIRouter, Request

router = APIRouter(prefix="/lifecycle", tags=["lifecycle"])


@router.get("/status")
async def lifecycle_status(request: Request):
    lm = getattr(request.app.state, "lifecycle_manager", None)
    return {
        "running": lm is not None,
        "cycle_interval": request.app.state.settings.lifecycle_cycle_interval,
    }
