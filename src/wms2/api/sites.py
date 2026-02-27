from fastapi import APIRouter, Depends, HTTPException

from wms2.config import Settings
from wms2.core.site_manager import SiteManager
from wms2.db.repository import Repository
from wms2.models.site import Site
from wms2.models.site_ban import SiteBan, SiteBanCreate

from .deps import get_repository, get_settings

router = APIRouter(prefix="/sites", tags=["sites"])


@router.get("", response_model=list[Site])
async def list_sites(repo: Repository = Depends(get_repository)):
    rows = await repo.list_sites()
    return [Site.model_validate(r) for r in rows]


@router.get("/bans/active", response_model=list[SiteBan])
async def list_active_bans(repo: Repository = Depends(get_repository)):
    """List all active site bans."""
    rows = await repo.list_all_active_bans()
    return [SiteBan.model_validate(r) for r in rows]


@router.get("/{site_name}", response_model=Site)
async def get_site(site_name: str, repo: Repository = Depends(get_repository)):
    row = await repo.get_site(site_name)
    if not row:
        raise HTTPException(status_code=404, detail="Site not found")
    return Site.model_validate(row)


@router.get("/{site_name}/bans", response_model=list[SiteBan])
async def get_site_bans(
    site_name: str, repo: Repository = Depends(get_repository),
):
    """List active bans for a specific site."""
    rows = await repo.get_active_bans_for_site(site_name)
    return [SiteBan.model_validate(r) for r in rows]


@router.post("/{site_name}/ban", response_model=SiteBan, status_code=201)
async def create_site_ban(
    site_name: str,
    body: SiteBanCreate,
    repo: Repository = Depends(get_repository),
    settings: Settings = Depends(get_settings),
):
    """Create a manual site ban (operator action)."""
    sm = SiteManager(repo, settings)
    ban = await sm.ban_site(
        site_name=site_name,
        workflow_id=body.workflow_id,
        reason=body.reason,
        duration_days=body.duration_days,
    )
    if not ban:
        raise HTTPException(status_code=404, detail="Site not found")
    return SiteBan.model_validate(ban)


@router.delete("/{site_name}/ban")
async def remove_site_ban(
    site_name: str,
    removed_by: str = "operator",
    repo: Repository = Depends(get_repository),
    settings: Settings = Depends(get_settings),
):
    """Remove all active bans for a site (operator override)."""
    sm = SiteManager(repo, settings)
    count = await sm.remove_ban(site_name, removed_by)
    return {"removed": count}
