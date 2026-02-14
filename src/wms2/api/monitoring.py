from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse

from wms2 import __version__
from wms2.db.repository import Repository

from .deps import get_repository

router = APIRouter(tags=["monitoring"])


@router.get("/health")
async def health():
    return {"status": "ok", "version": __version__}


@router.get("/status")
async def status(repo: Repository = Depends(get_repository)):
    counts = await repo.count_requests_by_status()
    return {"requests_by_status": counts}


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics(repo: Repository = Depends(get_repository)):
    counts = await repo.count_requests_by_status()
    lines = []
    lines.append("# HELP wms2_requests_total Number of requests by status")
    lines.append("# TYPE wms2_requests_total gauge")
    for status_val, count in counts.items():
        lines.append(f'wms2_requests_total{{status="{status_val}"}} {count}')
    return "\n".join(lines) + "\n"
