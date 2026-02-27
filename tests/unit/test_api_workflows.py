"""Unit tests for workflow and DAG API endpoints."""

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from wms2.api.deps import get_repository
from wms2.api.router import api_router


def _make_app():
    """Create a minimal FastAPI app with just the API router for testing."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(api_router, prefix="/api/v1")
    return app


def _make_workflow_row(**overrides):
    row = MagicMock()
    row.id = overrides.get("id", uuid.uuid4())
    row.request_name = overrides.get("request_name", "test-request-001")
    row.input_dataset = overrides.get("input_dataset", "/Test/Input/RECO")
    row.splitting_algo = overrides.get("splitting_algo", "FileBased")
    row.splitting_params = overrides.get("splitting_params", {"files_per_job": 10})
    row.sandbox_url = overrides.get("sandbox_url", "https://example.com/sandbox.tar.gz")
    row.config_data = overrides.get("config_data", {})
    row.pilot_cluster_id = overrides.get("pilot_cluster_id", None)
    row.pilot_schedd = overrides.get("pilot_schedd", None)
    row.pilot_output_path = overrides.get("pilot_output_path", None)
    row.step_metrics = overrides.get("step_metrics", None)
    row.current_round = overrides.get("current_round", 0)
    row.next_first_event = overrides.get("next_first_event", 1)
    row.file_offset = overrides.get("file_offset", 0)
    row.dag_id = overrides.get("dag_id", None)
    row.category_throttles = overrides.get("category_throttles", {})
    row.status = overrides.get("status", "active")
    row.total_nodes = overrides.get("total_nodes", 100)
    row.nodes_done = overrides.get("nodes_done", 50)
    row.nodes_failed = overrides.get("nodes_failed", 2)
    row.nodes_queued = overrides.get("nodes_queued", 10)
    row.nodes_running = overrides.get("nodes_running", 38)
    row.created_at = overrides.get("created_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    row.updated_at = overrides.get("updated_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    return row


def _make_dag_row(**overrides):
    row = MagicMock()
    row.id = overrides.get("id", uuid.uuid4())
    row.workflow_id = overrides.get("workflow_id", uuid.uuid4())
    row.dag_file_path = overrides.get("dag_file_path", "/data/wms2/submit/dag.txt")
    row.submit_dir = overrides.get("submit_dir", "/data/wms2/submit")
    row.rescue_dag_path = overrides.get("rescue_dag_path", None)
    row.dagman_cluster_id = overrides.get("dagman_cluster_id", "12345")
    row.schedd_name = overrides.get("schedd_name", "schedd.example.com")
    row.parent_dag_id = overrides.get("parent_dag_id", None)
    row.stop_requested_at = overrides.get("stop_requested_at", None)
    row.stop_reason = overrides.get("stop_reason", None)
    row.status = overrides.get("status", "running")
    row.total_nodes = overrides.get("total_nodes", 100)
    row.total_edges = overrides.get("total_edges", 200)
    row.node_counts = overrides.get("node_counts", {"Processing": 80, "Merge": 10, "Cleanup": 10})
    row.nodes_idle = overrides.get("nodes_idle", 5)
    row.nodes_running = overrides.get("nodes_running", 30)
    row.nodes_done = overrides.get("nodes_done", 60)
    row.nodes_failed = overrides.get("nodes_failed", 3)
    row.nodes_held = overrides.get("nodes_held", 2)
    row.total_work_units = overrides.get("total_work_units", 10)
    row.completed_work_units = overrides.get("completed_work_units", ["mg_000000", "mg_000001"])
    row.created_at = overrides.get("created_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    row.submitted_at = overrides.get("submitted_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    row.completed_at = overrides.get("completed_at", None)
    return row


def _make_block_row(**overrides):
    row = MagicMock()
    row.id = overrides.get("id", uuid.uuid4())
    row.workflow_id = overrides.get("workflow_id", uuid.uuid4())
    row.block_index = overrides.get("block_index", 0)
    row.dataset_name = overrides.get("dataset_name", "/Test/Output/GEN-SIM")
    row.total_work_units = overrides.get("total_work_units", 10)
    row.completed_work_units = overrides.get("completed_work_units", [])
    row.dbs_block_name = overrides.get("dbs_block_name", None)
    row.dbs_block_open = overrides.get("dbs_block_open", False)
    row.dbs_block_closed = overrides.get("dbs_block_closed", False)
    row.source_rule_ids = overrides.get("source_rule_ids", {})
    row.tape_rule_id = overrides.get("tape_rule_id", None)
    row.rucio_attempt_count = overrides.get("rucio_attempt_count", 0)
    row.rucio_last_error = overrides.get("rucio_last_error", None)
    row.status = overrides.get("status", "open")
    row.created_at = overrides.get("created_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    row.updated_at = overrides.get("updated_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    return row


def _make_history_row(**overrides):
    row = MagicMock()
    row.id = overrides.get("id", 1)
    row.dag_id = overrides.get("dag_id", uuid.uuid4())
    row.event_type = overrides.get("event_type", "status_change")
    row.from_status = overrides.get("from_status", "submitted")
    row.to_status = overrides.get("to_status", "running")
    row.nodes_done = overrides.get("nodes_done", 10)
    row.nodes_failed = overrides.get("nodes_failed", 0)
    row.nodes_running = overrides.get("nodes_running", 20)
    row.detail = overrides.get("detail", None)
    row.created_at = overrides.get("created_at", datetime(2026, 2, 27, tzinfo=timezone.utc))
    return row


@pytest.fixture
def repo():
    r = MagicMock()
    r.list_workflows = AsyncMock(return_value=[])
    r.get_workflow = AsyncMock(return_value=None)
    r.get_workflow_by_request = AsyncMock(return_value=None)
    r.get_processing_blocks = AsyncMock(return_value=[])
    r.list_dags = AsyncMock(return_value=[])
    r.get_dag = AsyncMock(return_value=None)
    r.get_dag_history = AsyncMock(return_value=[])
    return r


@pytest.fixture
async def client(repo):
    app = _make_app()
    app.dependency_overrides[get_repository] = lambda: repo
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


# ── Workflow endpoints ──────────────────────────────────────


async def test_list_workflows(client, repo):
    wf = _make_workflow_row()
    repo.list_workflows.return_value = [wf]

    resp = await client.get("/api/v1/workflows")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["request_name"] == "test-request-001"
    assert data[0]["status"] == "active"
    assert data[0]["nodes_done"] == 50


async def test_list_workflows_filter_by_request_name(client, repo):
    wf = _make_workflow_row()
    repo.get_workflow_by_request.return_value = wf

    resp = await client.get("/api/v1/workflows?request_name=test-request-001")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    repo.get_workflow_by_request.assert_called_once_with("test-request-001")


async def test_list_workflows_filter_by_request_name_not_found(client, repo):
    repo.get_workflow_by_request.return_value = None

    resp = await client.get("/api/v1/workflows?request_name=missing")

    assert resp.status_code == 200
    assert resp.json() == []


async def test_get_workflow_by_id(client, repo):
    wf_id = uuid.uuid4()
    wf = _make_workflow_row(id=wf_id, current_round=2)
    repo.get_workflow.return_value = wf

    resp = await client.get(f"/api/v1/workflows/{wf_id}")

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == str(wf_id)
    assert data["current_round"] == 2
    assert "splitting_params" in data  # detail view has more fields


async def test_get_workflow_not_found(client, repo):
    resp = await client.get(f"/api/v1/workflows/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_get_workflow_invalid_uuid(client, repo):
    resp = await client.get("/api/v1/workflows/not-a-uuid")
    assert resp.status_code == 400


async def test_get_workflow_by_request(client, repo):
    wf = _make_workflow_row(request_name="my-request")
    repo.get_workflow_by_request.return_value = wf

    resp = await client.get("/api/v1/workflows/by-request/my-request")

    assert resp.status_code == 200
    data = resp.json()
    assert data["request_name"] == "my-request"
    assert "config_data" in data  # detail view


async def test_get_workflow_by_request_not_found(client, repo):
    resp = await client.get("/api/v1/workflows/by-request/nonexistent")
    assert resp.status_code == 404


async def test_get_workflow_blocks(client, repo):
    wf_id = uuid.uuid4()
    repo.get_workflow.return_value = _make_workflow_row(id=wf_id)
    block = _make_block_row(workflow_id=wf_id, dataset_name="/Test/Out/GEN-SIM")
    repo.get_processing_blocks.return_value = [block]

    resp = await client.get(f"/api/v1/workflows/{wf_id}/blocks")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["dataset_name"] == "/Test/Out/GEN-SIM"
    assert data[0]["status"] == "open"
    assert data[0]["total_work_units"] == 10


async def test_get_workflow_blocks_not_found(client, repo):
    resp = await client.get(f"/api/v1/workflows/{uuid.uuid4()}/blocks")
    assert resp.status_code == 404


async def test_get_workflow_blocks_empty(client, repo):
    wf_id = uuid.uuid4()
    repo.get_workflow.return_value = _make_workflow_row(id=wf_id)
    repo.get_processing_blocks.return_value = []

    resp = await client.get(f"/api/v1/workflows/{wf_id}/blocks")

    assert resp.status_code == 200
    assert resp.json() == []


# ── DAG endpoints ──────────────────────────────────────────


async def test_list_dags(client, repo):
    dag = _make_dag_row()
    repo.list_dags.return_value = [dag]

    resp = await client.get("/api/v1/dags")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["status"] == "running"
    assert data[0]["nodes_done"] == 60
    assert data[0]["dagman_cluster_id"] == "12345"


async def test_list_dags_filter_by_workflow(client, repo):
    wf_id = uuid.uuid4()
    dag = _make_dag_row(workflow_id=wf_id)
    repo.list_dags.return_value = [dag]

    resp = await client.get(f"/api/v1/dags?workflow_id={wf_id}")

    assert resp.status_code == 200
    repo.list_dags.assert_called_once()
    call_kwargs = repo.list_dags.call_args[1]
    assert call_kwargs["workflow_id"] == wf_id


async def test_get_dag_detail(client, repo):
    dag_id = uuid.uuid4()
    dag = _make_dag_row(id=dag_id, completed_work_units=["mg_000000"])
    repo.get_dag.return_value = dag

    resp = await client.get(f"/api/v1/dags/{dag_id}")

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == str(dag_id)
    assert data["completed_work_units"] == ["mg_000000"]
    assert data["submit_dir"] == "/data/wms2/submit"
    assert data["node_counts"] == {"Processing": 80, "Merge": 10, "Cleanup": 10}


async def test_get_dag_not_found(client, repo):
    resp = await client.get(f"/api/v1/dags/{uuid.uuid4()}")
    assert resp.status_code == 404


async def test_get_dag_history(client, repo):
    dag_id = uuid.uuid4()
    repo.get_dag.return_value = _make_dag_row(id=dag_id)
    h1 = _make_history_row(dag_id=dag_id, event_type="submitted", from_status="planning", to_status="submitted")
    h2 = _make_history_row(dag_id=dag_id, id=2, event_type="running", from_status="submitted", to_status="running")
    repo.get_dag_history.return_value = [h1, h2]

    resp = await client.get(f"/api/v1/dags/{dag_id}/history")

    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["event_type"] == "submitted"
    assert data[1]["event_type"] == "running"


async def test_get_dag_history_not_found(client, repo):
    resp = await client.get(f"/api/v1/dags/{uuid.uuid4()}/history")
    assert resp.status_code == 404


async def test_get_dag_history_empty(client, repo):
    dag_id = uuid.uuid4()
    repo.get_dag.return_value = _make_dag_row(id=dag_id)
    repo.get_dag_history.return_value = []

    resp = await client.get(f"/api/v1/dags/{dag_id}/history")

    assert resp.status_code == 200
    assert resp.json() == []
