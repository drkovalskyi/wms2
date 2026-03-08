import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from wms2 import __version__
from wms2.adapters.mock import (
    MockCondorAdapter,
    MockCRICAdapter,
    MockDBSAdapter,
    MockReqMgrAdapter,
    MockRucioAdapter,
)
from wms2.api.router import api_router
from wms2.config import Settings
from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.core.site_manager import SiteManager
from wms2.db.engine import create_engine, create_session_factory
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)

WATCHDOG_RESTART_DELAY = 10  # seconds before auto-restarting a dead task


def _make_task_watchdog(app, task_name: str, coro_factory):
    """Return a done callback that logs failures and auto-restarts the task.

    coro_factory is a zero-arg callable returning a coroutine to schedule.
    """
    def _on_done(task: asyncio.Task):
        exc = task.exception() if not task.cancelled() else None
        health = app.state.task_health[task_name]
        if exc:
            health["last_error"] = f"{type(exc).__name__}: {exc}"
            logger.error("Background task %s died: %s", task_name, exc, exc_info=exc)
        elif task.cancelled():
            logger.info("Background task %s cancelled", task_name)
            return  # don't restart on intentional cancellation

        # Schedule a restart after a short delay
        loop = asyncio.get_event_loop()
        def _restart():
            health["restarts"] += 1
            health["started_at"] = time.time()
            logger.info("Auto-restarting background task %s (restart #%d)",
                        task_name, health["restarts"])
            new_task = asyncio.create_task(coro_factory())
            new_task.add_done_callback(_make_task_watchdog(app, task_name, coro_factory))
            setattr(app.state, f"{task_name}_task", new_task)

        loop.call_later(WATCHDOG_RESTART_DELAY, _restart)

    return _on_done


def _build_condor(settings: Settings):
    """Build HTCondor adapter: real if condor_host is set, mock otherwise."""
    if settings.condor_host:
        from wms2.adapters.condor import HTCondorAdapter

        extra = [c.strip() for c in settings.extra_collectors.split(",") if c.strip()]
        return HTCondorAdapter(
            settings.condor_host, settings.schedd_name,
            sec_token_directory=settings.sec_token_directory,
            extra_collectors=extra or None,
        )
    return MockCondorAdapter()


def _build_adapters(settings: Settings):
    """Build adapters: real if cert is configured, mock otherwise."""
    condor = _build_condor(settings)

    proxy = settings.x509_proxy
    if proxy:
        import os
        import ssl

        from wms2.adapters.cric import CRICClient
        from wms2.adapters.dbs import DBSClient
        from wms2.adapters.reqmgr2 import ReqMgr2Client
        from wms2.adapters.rucio import RucioClient

        # Build SSL context with Grid CA certificates
        ca_path = settings.ssl_ca_path
        if os.path.isdir(ca_path):
            ssl_ctx = ssl.create_default_context(capath=ca_path)
        elif os.path.isfile(ca_path):
            ssl_ctx = ssl.create_default_context(cafile=ca_path)
        else:
            ssl_ctx = ssl.create_default_context()
        ssl_ctx.load_cert_chain(proxy, proxy)

        reqmgr = ReqMgr2Client(settings.reqmgr2_url, proxy, proxy, verify=ssl_ctx)
        dbs = DBSClient(settings.dbs_url, proxy, proxy, verify=ssl_ctx)
        rucio = RucioClient(
            settings.rucio_url, settings.rucio_account,
            proxy, proxy, verify=ssl_ctx,
        )
        cric = CRICClient(settings.cric_url, proxy, proxy, verify=ssl_ctx)
    else:
        reqmgr = MockReqMgrAdapter()
        dbs = MockDBSAdapter()
        rucio = MockRucioAdapter()
        cric = MockCRICAdapter()

    return condor, reqmgr, dbs, rucio, cric


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))

    # Database
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    app.state.engine = engine
    app.state.session_factory = session_factory

    # Build adapters (also stored on app.state for API endpoints)
    condor, reqmgr, dbs, rucio, cric = _build_adapters(settings)
    app.state.condor = condor
    app.state.reqmgr = reqmgr
    app.state.dbs = dbs
    app.state.rucio = rucio

    # Task health tracking
    now = time.time()
    app.state.task_health = {
        "lifecycle": {
            "started_at": now, "last_cycle_at": None,
            "cycle_count": 0, "last_error": None, "restarts": 0,
        },
        "cric_sync": {
            "started_at": now, "last_cycle_at": None,
            "cycle_count": 0, "last_error": None, "restarts": 0,
        },
    }

    # Initial CRIC sync (populate sites table at startup)
    try:
        async with session_factory() as session:
            repo = Repository(session)
            sm = SiteManager(repo, settings, cric_adapter=cric)
            stats = await sm.sync_from_cric()
            await session.commit()
            logger.info("Startup CRIC sync: %s", stats)
    except Exception:
        logger.exception("Startup CRIC sync failed — continuing without site data")

    # Periodic CRIC sync background task
    async def run_cric_sync():
        health = app.state.task_health["cric_sync"]
        interval = settings.cric_sync_interval
        if interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            try:
                async with session_factory() as session:
                    repo = Repository(session)
                    sm = SiteManager(repo, settings, cric_adapter=cric)
                    stats = await sm.sync_from_cric()
                    await session.commit()
                    logger.info("Periodic CRIC sync: %s", stats)
                health["last_cycle_at"] = time.time()
                health["cycle_count"] += 1
                health["last_error"] = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health["last_error"] = f"{type(exc).__name__}: {exc}"
                logger.exception("Periodic CRIC sync failed")

    cric_sync_task = asyncio.create_task(run_cric_sync())
    cric_sync_task.add_done_callback(
        _make_task_watchdog(app, "cric_sync", run_cric_sync))
    app.state.cric_sync_task = cric_sync_task

    # Lifecycle cycle callback — updates task_health from main_loop
    def on_lifecycle_cycle(exc):
        health = app.state.task_health["lifecycle"]
        health["last_cycle_at"] = time.time()
        health["cycle_count"] += 1
        if exc:
            health["last_error"] = f"{type(exc).__name__}: {exc}"
        else:
            health["last_error"] = None

    # Start lifecycle manager
    async def run_lifecycle():
        lm = RequestLifecycleManager(
            session_factory, condor, settings,
            reqmgr=reqmgr, dbs=dbs, rucio=rucio, cric=cric,
            on_cycle=on_lifecycle_cycle,
        )
        app.state.lifecycle_manager = lm
        await lm.main_loop()

    lifecycle_task = asyncio.create_task(run_lifecycle())
    lifecycle_task.add_done_callback(
        _make_task_watchdog(app, "lifecycle", run_lifecycle))
    app.state.lifecycle_task = lifecycle_task

    logger.info("WMS2 v%s started", __version__)

    yield

    # Shutdown — cancel current tasks (may differ from originals if watchdog restarted them)
    for task_attr in ("cric_sync_task", "lifecycle_task"):
        task = getattr(app.state, task_attr, None)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    await engine.dispose()
    logger.info("WMS2 shut down")


def create_app() -> FastAPI:
    settings = Settings()
    app = FastAPI(
        title="WMS2",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.include_router(api_router, prefix=settings.api_prefix)

    # Web UI
    from wms2.ui.routes import ui_router

    static_dir = Path(__file__).resolve().parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
    app.include_router(ui_router)

    @app.get("/")
    async def root_redirect():
        return RedirectResponse(url="/ui/")

    return app
