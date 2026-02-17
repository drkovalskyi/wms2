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


# ── Classification Tests ──────────────────────────────────────


async def test_low_failure_ratio_triggers_rescue(handler, repo):
    """1/100 failed (1%) → 'rescue', rescue DAG created."""
    dag = _make_dag(nodes_done=99, nodes_failed=1, total_nodes=100)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag()
    repo.create_dag.return_value = new_dag

    action = await handler.handle_dag_partial_failure(dag, request, workflow)

    assert action == "rescue"
    repo.create_dag.assert_called_once()
    create_kwargs = repo.create_dag.call_args[1]
    assert create_kwargs["parent_dag_id"] == dag.id
    assert create_kwargs["status"] == DAGStatus.READY.value
    repo.update_workflow.assert_called_once()
    wf_kwargs = repo.update_workflow.call_args[1]
    assert wf_kwargs["status"] == WorkflowStatus.RESUBMITTING.value


async def test_medium_failure_ratio_flags_review(handler, repo):
    """3/20 failed (15%) → 'review', no rescue created."""
    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_partial_failure(dag, request, workflow)

    assert action == "review"
    repo.create_dag.assert_not_called()
    repo.update_workflow.assert_not_called()


async def test_high_failure_ratio_aborts(handler, repo):
    """8/20 failed (40%) → 'abort', workflow FAILED."""
    dag = _make_dag(nodes_done=12, nodes_failed=8, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_partial_failure(dag, request, workflow)

    assert action == "abort"
    repo.update_workflow.assert_called_once()
    wf_kwargs = repo.update_workflow.call_args[1]
    assert wf_kwargs["status"] == WorkflowStatus.FAILED.value


async def test_zero_successes_always_aborts(handler, repo):
    """handle_dag_failure → 'abort', workflow FAILED."""
    dag = _make_dag(nodes_done=0, nodes_failed=20, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    action = await handler.handle_dag_failure(dag, request, workflow)

    assert action == "abort"
    repo.update_workflow.assert_called_once()
    wf_kwargs = repo.update_workflow.call_args[1]
    assert wf_kwargs["status"] == WorkflowStatus.FAILED.value


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
    """Parent chain length >= max_rescue_attempts → 'abort' even at low ratio."""
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

    # ratio is 1% (below auto-rescue threshold), but max rescues blocks it
    action = await handler.handle_dag_partial_failure(dag3, request, workflow)

    assert action == "abort"
    repo.update_workflow.assert_called_once()
    wf_kwargs = repo.update_workflow.call_args[1]
    assert wf_kwargs["status"] == WorkflowStatus.FAILED.value


# ── Audit Trail ──────────────────────────────────────────────


async def test_dag_history_recorded(handler, repo):
    """create_dag_history called with event_type and detail."""
    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    await handler.handle_dag_partial_failure(dag, request, workflow)

    repo.create_dag_history.assert_called_once()
    call_kwargs = repo.create_dag_history.call_args[1]
    assert call_kwargs["event_type"] == "dag_partial"
    assert call_kwargs["dag_id"] == dag.id
    assert "failure_ratio" in call_kwargs["detail"]


# ── Custom Thresholds ────────────────────────────────────────


async def test_custom_thresholds(repo, condor):
    """Settings override defaults — 15% with threshold at 0.20 triggers rescue."""
    settings = _make_settings(
        error_auto_rescue_threshold=0.20,
        error_abort_threshold=0.50,
    )
    handler = ErrorHandler(repo, condor, settings)

    # 3/20 = 15% < 20% custom threshold → rescue
    dag = _make_dag(nodes_done=17, nodes_failed=3, total_nodes=20)
    request = make_request_row(status="active")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag()
    repo.create_dag.return_value = new_dag

    action = await handler.handle_dag_partial_failure(dag, request, workflow)
    assert action == "rescue"
