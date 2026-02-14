import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from wms2.db.base import Base
from wms2.db.engine import create_session_factory
from wms2.db.repository import Repository
from wms2.db.tables import (  # noqa: F401
    DAGHistoryRow,
    DAGRow,
    OutputDatasetRow,
    RequestRow,
    SiteRow,
    WorkflowRow,
)

TEST_DB_URL = "postgresql+asyncpg://wms2test:wms2test@localhost:5433/wms2test"


@pytest.fixture(scope="session")
def engine():
    return create_async_engine(TEST_DB_URL, echo=False)


@pytest.fixture(scope="session")
def session_factory(engine):
    return create_session_factory(engine)


@pytest.fixture(autouse=True)
async def setup_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest.fixture
async def session(session_factory):
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def repo(session):
    return Repository(session)
