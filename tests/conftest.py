import pytest
from httpx import ASGITransport, AsyncClient

from wms2.config import Settings
from wms2.main import create_app


@pytest.fixture
def settings():
    return Settings(
        database_url="postgresql+asyncpg://wms2test:wms2test@localhost:5433/wms2test",
        lifecycle_cycle_interval=1,
        max_active_dags=10,
    )


@pytest.fixture
def app(settings):
    application = create_app()
    application.state.settings = settings
    return application


@pytest.fixture
async def client(app):
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
