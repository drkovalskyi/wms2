import json
import os
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from wms2.config import Settings
from wms2.core.error_handler import ErrorHandler
from wms2.models.enums import DAGStatus, WorkflowStatus

from .conftest import make_request_row


def _make_settings(**overrides):
    defaults = dict(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
    )
    defaults.update(overrides)
    return Settings(**defaults)


def _make_workflow(dag_id=None, **kwargs):
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = kwargs.get("request_name", "test-request-001")
    wf.dag_id = dag_id
    for k, v in kwargs.items():
        setattr(wf, k, v)
    return wf


def _make_dag(
    nodes_done=0,
    nodes_failed=0,
    total_nodes=20,
    parent_dag_id=None,
    dag_file_path="/tmp/submit/workflow.dag",
    submit_dir="/tmp/submit",
    **kwargs,
):
    dag = MagicMock()
    dag.id = uuid.uuid4()
    dag.workflow_id = uuid.uuid4()
    dag.dag_file_path = dag_file_path
    dag.submit_dir = submit_dir
    dag.status = "partial"
    dag.nodes_done = nodes_done
    dag.nodes_failed = nodes_failed
    dag.total_nodes = total_nodes
    dag.total_edges = 8
    dag.node_counts = {"processing": 16, "merge": 2, "cleanup": 2}
    dag.total_work_units = 2
    dag.completed_work_units = []
    dag.parent_dag_id = parent_dag_id
    dag.nodes_running = 0
    for k, v in kwargs.items():
        setattr(dag, k, v)
    return dag


@pytest.fixture
def repo():
    r = MagicMock()
    r.update_workflow = AsyncMock()
    r.create_dag = AsyncMock()
    r.create_dag_history = AsyncMock()
    r.get_dag = AsyncMock(return_value=None)
    return r


@pytest.fixture
def condor():
    return MagicMock()


@pytest.fixture
def settings():
    return _make_settings()


@pytest.fixture
def handler(repo, condor, settings):
    return ErrorHandler(repo, condor, settings)


# ── Classification Tests (single 20% threshold) ──────────────


async def test_low_failure_ratio_triggers_rescue(handler, repo):
    """1/100 failed (1%) → 'rescue', rescue DAG created."""
    dag = _make_dag(nodes_done=99, nodes_failed=1, total_nodes=100)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag()
    repo.create_dag.return_value = new_dag

    action = await handler.handle_dag_completion(dag, request, workflow)

    assert action == "rescue"
    repo.create_dag.assert_called_once()
    create_kwargs = repo.create_dag.call_args[1]
    assert create_kwargs["parent_dag_id"] == dag.id
    assert create_kwargs["status"] == DAGStatus.READY.value
    repo.update_workflow.assert_called_once()
    wf_kwargs = repo.update_workflow.call_args[1]
    assert wf_kwargs["status"] == WorkflowStatus.RESUBMITTING.value


async def test_below_threshold_triggers_rescue(handler, repo):
    """3/20 failed (15%) — below 20% threshold → 'rescue'."""
    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag()
    repo.create_dag.return_value = new_dag

    action = await handler.handle_dag_completion(dag, request, workflow)

    assert action == "rescue"
    repo.create_dag.assert_called_once()


async def test_at_threshold_holds(handler, repo):
    """4/20 failed (20%) — at threshold → 'hold'."""
    dag = _make_dag(nodes_done=16, nodes_failed=4, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_completion(dag, request, workflow)

    assert action == "hold"
    repo.create_dag.assert_not_called()


async def test_high_failure_ratio_holds(handler, repo):
    """8/20 failed (40%) — above threshold → 'hold'."""
    dag = _make_dag(nodes_done=12, nodes_failed=8, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_completion(dag, request, workflow)

    assert action == "hold"
    repo.create_dag.assert_not_called()


async def test_zero_successes_holds(handler, repo):
    """0/20 succeeded (100% failure) → 'hold' (not auto-abort)."""
    dag = _make_dag(nodes_done=0, nodes_failed=20, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_completion(dag, request, workflow)

    assert action == "hold"
    # FAILED is manual only — error handler never sets it
    repo.update_workflow.assert_not_called()


# ── Rescue DAG Path Tests ────────────────────────────────────


async def test_rescue_dag_path_found(handler, tmp_path):
    """Finds .rescue001 on disk."""
    dag_file = tmp_path / "workflow.dag"
    dag_file.touch()
    rescue = tmp_path / "workflow.dag.rescue001"
    rescue.touch()

    dag = _make_dag(dag_file_path=str(dag_file))
    result = handler._find_rescue_dag(dag)
    assert result == str(rescue)


async def test_rescue_dag_path_finds_highest(handler, tmp_path):
    """With .rescue001 + .rescue002, returns .rescue002."""
    dag_file = tmp_path / "workflow.dag"
    dag_file.touch()
    (tmp_path / "workflow.dag.rescue001").touch()
    (tmp_path / "workflow.dag.rescue002").touch()

    dag = _make_dag(dag_file_path=str(dag_file))
    result = handler._find_rescue_dag(dag)
    assert result == str(tmp_path / "workflow.dag.rescue002")


# ── Max Rescue Guard ─────────────────────────────────────────


async def test_max_rescue_attempts_exceeded(repo, condor):
    """Parent chain length >= max_rescue_attempts → 'hold' even at low ratio."""
    settings = _make_settings(error_max_rescue_attempts=2)
    handler = ErrorHandler(repo, condor, settings)

    # Build a chain: dag3 → dag2 → dag1 → None (2 ancestors = 2 rescues)
    dag1 = _make_dag(parent_dag_id=None)
    dag2 = _make_dag(parent_dag_id=dag1.id)
    dag3 = _make_dag(parent_dag_id=dag2.id, nodes_done=99, nodes_failed=1, total_nodes=100)

    repo.get_dag.side_effect = lambda did: {
        dag2.id: dag2,
        dag1.id: dag1,
    }.get(did)

    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag3.id)

    # ratio is 1% (below threshold), but max rescues blocks it
    action = await handler.handle_dag_completion(dag3, request, workflow)

    assert action == "hold"
    # No workflow status change — FAILED is manual only
    repo.update_workflow.assert_not_called()


# ── Audit Trail ──────────────────────────────────────────────


async def test_dag_history_recorded(handler, repo):
    """create_dag_history called with event_type and detail."""
    dag = _make_dag(nodes_done=16, nodes_failed=4, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    await handler.handle_dag_completion(dag, request, workflow)

    repo.create_dag_history.assert_called_once()
    call_kwargs = repo.create_dag_history.call_args[1]
    assert call_kwargs["event_type"] == "dag_completion"
    assert call_kwargs["dag_id"] == dag.id
    assert "failure_ratio" in call_kwargs["detail"]


# ── Custom Thresholds ────────────────────────────────────────


async def test_custom_threshold(repo, condor):
    """Custom error_hold_threshold=0.30 — 15% triggers rescue."""
    settings = _make_settings(error_hold_threshold=0.30)
    handler = ErrorHandler(repo, condor, settings)

    # 3/20 = 15% < 30% custom threshold → rescue
    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag()
    repo.create_dag.return_value = new_dag

    action = await handler.handle_dag_completion(dag, request, workflow)
    assert action == "rescue"


async def test_custom_threshold_holds_above(repo, condor):
    """Custom error_hold_threshold=0.10 — 15% triggers hold."""
    settings = _make_settings(error_hold_threshold=0.10)
    handler = ErrorHandler(repo, condor, settings)

    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_completion(dag, request, workflow)
    assert action == "hold"


# ── POST Data Reading ────────────────────────────────────────


async def test_read_post_data(handler, tmp_path):
    """read_post_data reads final post.json files from submit directory."""
    mg_dir = tmp_path / "mg_000000"
    mg_dir.mkdir()

    # Final post.json (should be included)
    final_post = {
        "node_name": "proc_000001",
        "final": True,
        "classification": {"category": "data"},
        "job": {"site": "T2_US_Purdue"},
    }
    (mg_dir / "proc_000001.post.json").write_text(json.dumps(final_post))

    # Non-final post.json (should be excluded)
    intermediate_post = {
        "node_name": "proc_000002",
        "final": False,
        "classification": {"category": "transient"},
        "job": {"site": "T1_US_FNAL"},
    }
    (mg_dir / "proc_000002.post.json").write_text(json.dumps(intermediate_post))

    results = handler.read_post_data(str(tmp_path))
    assert len(results) == 1
    assert results[0]["node_name"] == "proc_000001"
    assert results[0]["classification"]["category"] == "data"


async def test_read_post_data_empty_dir(handler, tmp_path):
    """Empty submit dir returns empty list."""
    results = handler.read_post_data(str(tmp_path))
    assert results == []


async def test_read_post_data_ignores_corrupt_json(handler, tmp_path):
    """Corrupt JSON files are skipped with warning."""
    (tmp_path / "bad.post.json").write_text("{corrupt")
    results = handler.read_post_data(str(tmp_path))
    assert results == []


# ── Site Failure Analysis ────────────────────────────────────


async def test_analyze_site_failures_detects_problem_site(handler):
    """Site with >50% failure rate and >=3 failures is flagged."""
    post_data = [
        {"job": {"site": "T2_US_Bad"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Bad"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Bad"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Bad"}, "classification": {"category": "data"}},
        {"job": {"site": "T1_US_Good"}, "classification": {"category": "success"}},
        {"job": {"site": "T1_US_Good"}, "classification": {"category": "success"}},
    ]
    problem = handler.analyze_site_failures(post_data)
    assert "T2_US_Bad" in problem
    assert "T1_US_Good" not in problem


async def test_analyze_site_failures_ignores_low_count(handler):
    """Site with high ratio but only 2 failures is not flagged."""
    post_data = [
        {"job": {"site": "T2_US_Few"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Few"}, "classification": {"category": "infrastructure"}},
    ]
    problem = handler.analyze_site_failures(post_data)
    assert problem == []


async def test_analyze_site_failures_ignores_low_ratio(handler):
    """Site with 3 failures but <50% ratio is not flagged."""
    post_data = [
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "infrastructure"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "success"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "success"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "success"}},
        {"job": {"site": "T2_US_Mix"}, "classification": {"category": "success"}},
    ]
    problem = handler.analyze_site_failures(post_data)
    assert problem == []
