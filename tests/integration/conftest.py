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

TEST_DB_URL = "postgresql+asyncpg://wms2test:wms2test@localhost:5432/wms2test"


@pytest.fixture
async def engine():
    eng = create_async_engine(TEST_DB_URL, echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest.fixture
async def session(engine):
    factory = create_session_factory(engine)
    async with factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def repo(session):
    return Repository(session)
