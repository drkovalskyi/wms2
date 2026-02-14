import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from wms2 import __version__
from wms2.adapters.mock import MockCondorAdapter
from wms2.api.router import api_router
from wms2.config import Settings
from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.db.engine import create_engine, create_session_factory
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = app.state.settings
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))

    # Database
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    app.state.engine = engine
    app.state.session_factory = session_factory

    # Start lifecycle manager
    async def run_lifecycle():
        async with session_factory() as session:
            repo = Repository(session)
            condor = MockCondorAdapter()
            lm = RequestLifecycleManager(repo, condor, settings)
            app.state.lifecycle_manager = lm
            await lm.main_loop()

    lifecycle_task = asyncio.create_task(run_lifecycle())
    app.state.lifecycle_task = lifecycle_task

    logger.info("WMS2 v%s started", __version__)

    yield

    # Shutdown
    lifecycle_task.cancel()
    try:
        await lifecycle_task
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
    return app
