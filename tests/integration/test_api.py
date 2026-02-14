import pytest
from httpx import ASGITransport, AsyncClient

from wms2.config import Settings
from wms2.db.base import Base
from wms2.db.engine import create_engine, create_session_factory
from wms2.main import create_app

TEST_DB_URL = "postgresql+asyncpg://wms2test:wms2test@localhost:5432/wms2test"


@pytest.fixture
async def api_app():
    settings = Settings(
        database_url=TEST_DB_URL,
        lifecycle_cycle_interval=3600,  # Don't actually cycle during tests
        max_active_dags=10,
    )
    app = create_app()
    app.state.settings = settings

    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    app.state.engine = engine
    app.state.session_factory = session_factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield app

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
async def api_client(api_app):
    async with AsyncClient(
        transport=ASGITransport(app=api_app),
        base_url="http://test",
    ) as ac:
        yield ac


def _valid_request_body(**overrides):
    body = {
        "request_name": "api-test-001",
        "requestor": "testuser",
        "input_dataset": "/TestPrimary/TestProcessed/RECO",
        "cmssw_version": "CMSSW_14_0_0",
        "scram_arch": "el9_amd64_gcc12",
        "global_tag": "GT_TEST",
        "splitting_algo": "FileBased",
        "campaign": "TestCampaign",
        "processing_string": "TestProc",
        "acquisition_era": "Run2024",
    }
    body.update(overrides)
    return body


async def test_health_endpoint(api_client):
    resp = await api_client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "version" in data


async def test_create_and_get_request(api_client):
    resp = await api_client.post("/api/v1/requests", json=_valid_request_body())
    assert resp.status_code == 201
    data = resp.json()
    assert data["request_name"] == "api-test-001"
    assert data["status"] == "new"

    resp = await api_client.get("/api/v1/requests/api-test-001")
    assert resp.status_code == 200
    assert resp.json()["request_name"] == "api-test-001"


async def test_create_duplicate_request(api_client):
    await api_client.post("/api/v1/requests", json=_valid_request_body())
    resp = await api_client.post("/api/v1/requests", json=_valid_request_body())
    assert resp.status_code == 409


async def test_get_nonexistent_request(api_client):
    resp = await api_client.get("/api/v1/requests/does-not-exist")
    assert resp.status_code == 404


async def test_update_request(api_client):
    await api_client.post("/api/v1/requests", json=_valid_request_body())
    resp = await api_client.patch(
        "/api/v1/requests/api-test-001",
        json={"priority": 200000},
    )
    assert resp.status_code == 200
    assert resp.json()["priority"] == 200000


async def test_abort_request(api_client):
    await api_client.post("/api/v1/requests", json=_valid_request_body())
    resp = await api_client.delete("/api/v1/requests/api-test-001")
    assert resp.status_code == 200
    assert resp.json()["status"] == "aborted"


async def test_list_requests(api_client):
    for i in range(3):
        await api_client.post(
            "/api/v1/requests",
            json=_valid_request_body(request_name=f"list-test-{i}"),
        )
    resp = await api_client.get("/api/v1/requests")
    assert resp.status_code == 200
    assert len(resp.json()) == 3


async def test_validation_error(api_client):
    resp = await api_client.post("/api/v1/requests", json={"request_name": "bad"})
    assert resp.status_code == 422


async def test_status_endpoint(api_client):
    resp = await api_client.get("/api/v1/status")
    assert resp.status_code == 200
    assert "requests_by_status" in resp.json()
