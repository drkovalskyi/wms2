import asyncio
import logging
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
from wms2.core.dag_monitor import DAGMonitor
from wms2.core.dag_planner import DAGPlanner
from wms2.core.error_handler import ErrorHandler
from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.core.output_manager import OutputManager
from wms2.core.site_manager import SiteManager
from wms2.core.workflow_manager import WorkflowManager
from wms2.db.engine import create_engine, create_session_factory
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)


def _build_condor(settings: Settings):
    """Build HTCondor adapter: real if condor_host is set, mock otherwise."""
    if settings.condor_host:
        from wms2.adapters.condor import HTCondorAdapter

        return HTCondorAdapter(settings.condor_host, settings.schedd_name)
    return MockCondorAdapter()


def _build_adapters(settings: Settings):
    """Build adapters: real if cert is configured, mock otherwise."""
    condor = _build_condor(settings)

    if settings.cert_file and settings.key_file:
        from wms2.adapters.cric import CRICClient
        from wms2.adapters.dbs import DBSClient
        from wms2.adapters.reqmgr2 import ReqMgr2Client
        from wms2.adapters.rucio import RucioClient

        reqmgr = ReqMgr2Client(settings.reqmgr2_url, settings.cert_file, settings.key_file)
        dbs = DBSClient(settings.dbs_url, settings.cert_file, settings.key_file)
        rucio = RucioClient(
            settings.rucio_url, settings.rucio_account,
            settings.cert_file, settings.key_file,
        )
        cric = CRICClient(settings.cric_url, settings.cert_file, settings.key_file)
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

    # Build adapters
    condor, reqmgr, dbs, rucio, cric = _build_adapters(settings)

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
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Periodic CRIC sync failed")

    cric_sync_task = asyncio.create_task(run_cric_sync())
    app.state.cric_sync_task = cric_sync_task

    # Start lifecycle manager
    async def run_lifecycle():
        async with session_factory() as session:
            repo = Repository(session)
            sm = SiteManager(repo, settings, cric_adapter=cric)
            wm = WorkflowManager(repo, reqmgr)
            dp = DAGPlanner(repo, dbs, rucio, condor, settings, site_manager=sm)
            dm = DAGMonitor(repo, condor)
            om = OutputManager(repo, dbs, rucio)
            eh = ErrorHandler(repo, condor, settings, site_manager=sm)
            lm = RequestLifecycleManager(
                repo, condor, settings,
                workflow_manager=wm,
                dag_planner=dp,
                dag_monitor=dm,
                output_manager=om,
                error_handler=eh,
            )
            app.state.lifecycle_manager = lm
            await lm.main_loop()

    lifecycle_task = asyncio.create_task(run_lifecycle())
    app.state.lifecycle_task = lifecycle_task

    logger.info("WMS2 v%s started", __version__)

    yield

    # Shutdown
    cric_sync_task.cancel()
    lifecycle_task.cancel()
    for task in (cric_sync_task, lifecycle_task):
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
