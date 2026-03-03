import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request

from wms2.core.lifecycle_manager import RequestLifecycleManager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/lifecycle", tags=["lifecycle"])

# Fields that can be modified via PATCH /lifecycle/settings
EDITABLE_FIELDS = {
    "lifecycle_cycle_interval": (int, lambda v: v > 0),
    "max_active_dags": (int, lambda v: v > 0),
    "jobs_per_work_unit": (int, lambda v: v > 0),
    "first_round_work_units": (int, lambda v: v > 0),
    "work_units_per_round": (int, lambda v: v > 0),
    "target_merged_size_kb": (int, lambda v: v > 0),
    "error_hold_threshold": (float, lambda v: 0 < v <= 1),
    "error_max_rescue_attempts": (int, lambda v: v >= 0),
    "default_memory_per_core": (int, lambda v: v > 0),
    "max_memory_per_core": (int, lambda v: v > 0),
    "safety_margin": (float, lambda v: 0 <= v <= 1),
    "site_ban_duration_days": (int, lambda v: v >= 0),
    "site_ban_min_failures": (int, lambda v: v >= 1),
    "site_ban_failure_ratio": (float, lambda v: 0 < v <= 1),
    "log_level": (str, lambda v: v.upper() in ("DEBUG", "INFO", "WARNING", "ERROR")),
    "default_pilot_priority": (int, lambda v: True),
}


@router.get("/status")
async def lifecycle_status(request: Request):
    lm = getattr(request.app.state, "lifecycle_manager", None)
    return {
        "running": lm is not None,
        "cycle_interval": request.app.state.settings.lifecycle_cycle_interval,
    }


@router.get("/settings")
async def lifecycle_settings(request: Request):
    """Return key configuration settings for display."""
    s = request.app.state.settings
    return {
        "lifecycle_cycle_interval": s.lifecycle_cycle_interval,
        "max_active_dags": s.max_active_dags,
        "jobs_per_work_unit": s.jobs_per_work_unit,
        "first_round_work_units": s.first_round_work_units,
        "work_units_per_round": s.work_units_per_round,
        "target_merged_size_kb": s.target_merged_size_kb,
        "condor_host": s.condor_host or "(not set)",
        "schedd_name": s.schedd_name or "(auto)",
        "submit_base_dir": s.submit_base_dir,
        "local_pfn_prefix": s.local_pfn_prefix,
        "error_hold_threshold": s.error_hold_threshold,
        "error_max_rescue_attempts": s.error_max_rescue_attempts,
        "adaptive_mode": s.adaptive_mode,
        "default_memory_per_core": s.default_memory_per_core,
        "max_memory_per_core": s.max_memory_per_core,
        "safety_margin": s.safety_margin,
        "site_ban_duration_days": s.site_ban_duration_days,
        "site_ban_min_failures": s.site_ban_min_failures,
        "site_ban_failure_ratio": s.site_ban_failure_ratio,
        "log_level": s.log_level,
        "default_pilot_priority": s.default_pilot_priority,
        "database_url": _mask_password(s.database_url),
    }


@router.patch("/settings")
async def update_settings(request: Request):
    """Update editable configuration settings in memory."""
    body = await request.json()
    s = request.app.state.settings
    errors = []
    updated = {}
    for key, value in body.items():
        if key not in EDITABLE_FIELDS:
            errors.append(f"{key}: not an editable field")
            continue
        expected_type, validator = EDITABLE_FIELDS[key]
        try:
            cast_value = expected_type(value)
        except (TypeError, ValueError):
            errors.append(f"{key}: expected {expected_type.__name__}")
            continue
        if not validator(cast_value):
            errors.append(f"{key}: value {cast_value!r} out of range")
            continue
        setattr(s, key, cast_value)
        updated[key] = cast_value
    if errors:
        raise HTTPException(status_code=422, detail="; ".join(errors))
    logger.info("Settings updated: %s", updated)
    return updated


@router.post("/restart")
async def restart_lifecycle(request: Request):
    """Cancel and restart the lifecycle manager task."""
    app = request.app

    # Cancel existing task
    old_task = getattr(app.state, "lifecycle_task", None)
    if old_task and not old_task.done():
        old_task.cancel()
        try:
            await old_task
        except asyncio.CancelledError:
            pass
        logger.info("Cancelled existing lifecycle manager task")

    # Build fresh lifecycle manager and start it
    settings = app.state.settings
    session_factory = app.state.session_factory
    condor = app.state.condor
    reqmgr = getattr(app.state, "reqmgr", None)
    dbs = getattr(app.state, "dbs", None)
    rucio = getattr(app.state, "rucio", None)
    cric = getattr(app.state, "cric", None)

    async def run_lifecycle():
        lm = RequestLifecycleManager(
            session_factory, condor, settings,
            reqmgr=reqmgr, dbs=dbs, rucio=rucio, cric=cric,
        )
        app.state.lifecycle_manager = lm
        await lm.main_loop()

    new_task = asyncio.create_task(run_lifecycle())
    app.state.lifecycle_task = new_task
    logger.info("Restarted lifecycle manager task")

    return {"message": "Lifecycle manager restarted"}


def _mask_password(url: str) -> str:
    """Mask password in database URL for display."""
    # postgresql+asyncpg://user:pass@host:port/db -> mask the password
    import re
    return re.sub(r"://([^:]+):([^@]+)@", r"://\1:***@", url)
