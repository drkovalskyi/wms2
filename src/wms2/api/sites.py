from fastapi import APIRouter, Depends, HTTPException

from wms2.db.repository import Repository
from wms2.models.site import Site

from .deps import get_repository

router = APIRouter(prefix="/sites", tags=["sites"])


@router.get("", response_model=list[Site])
async def list_sites(repo: Repository = Depends(get_repository)):
    rows = await repo.list_sites()
    return [Site.model_validate(r) for r in rows]


@router.get("/{site_name}", response_model=Site)
async def get_site(site_name: str, repo: Repository = Depends(get_repository)):
    row = await repo.get_site(site_name)
    if not row:
        raise HTTPException(status_code=404, detail="Site not found")
    return Site.model_validate(row)
