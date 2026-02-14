from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.models.enums import RequestStatus
from .conftest import make_request_row


@pytest.fixture
def lifecycle_manager(mock_repository, mock_condor, settings):
    return RequestLifecycleManager(
        repository=mock_repository,
        condor_adapter=mock_condor,
        settings=settings,
    )


async def test_dispatch_submitted(lifecycle_manager, mock_repository):
    """SUBMITTED without workflow_manager skips gracefully."""
    request = make_request_row(status="submitted")
    await lifecycle_manager.evaluate_request(request)
    # No workflow_manager → handler skips, no transition
    mock_repository.update_request.assert_not_called()


async def test_dispatch_resubmitting_to_queued(lifecycle_manager, mock_repository):
    """RESUBMITTING should transition to QUEUED."""
    request = make_request_row(status="resubmitting")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_called_once()
    call_kwargs = mock_repository.update_request.call_args
    assert call_kwargs[1]["status"] == RequestStatus.QUEUED.value


async def test_transition_records_audit_trail(lifecycle_manager, mock_repository):
    """transition() should append to status_transitions."""
    request = make_request_row(status="resubmitting", status_transitions=[])
    await lifecycle_manager.transition(request, RequestStatus.QUEUED)

    call_kwargs = mock_repository.update_request.call_args
    transitions = call_kwargs[1]["status_transitions"]
    assert len(transitions) == 1
    assert transitions[0]["from"] == "resubmitting"
    assert transitions[0]["to"] == "queued"
    assert "timestamp" in transitions[0]


async def test_stuck_detection(lifecycle_manager, settings):
    """Request stuck in SUBMITTED beyond timeout is detected."""
    old_time = datetime.now(timezone.utc) - timedelta(seconds=settings.timeout_submitted + 10)
    request = make_request_row(status="submitted", updated_at=old_time)
    assert lifecycle_manager._is_stuck(request) is True


async def test_not_stuck_within_timeout(lifecycle_manager, settings):
    """Request within timeout is not stuck."""
    recent = datetime.now(timezone.utc) - timedelta(seconds=10)
    request = make_request_row(status="submitted", updated_at=recent)
    assert lifecycle_manager._is_stuck(request) is False


async def test_stuck_submitted_transitions_to_failed(lifecycle_manager, mock_repository, settings):
    """Stuck SUBMITTED request → FAILED."""
    old_time = datetime.now(timezone.utc) - timedelta(seconds=settings.timeout_submitted + 10)
    request = make_request_row(status="submitted", updated_at=old_time)
    await lifecycle_manager.evaluate_request(request)
    call_kwargs = mock_repository.update_request.call_args
    assert call_kwargs[1]["status"] == RequestStatus.FAILED.value


async def test_handle_queued_no_dag_planner(lifecycle_manager, mock_repository):
    """QUEUED without dag_planner skips gracefully."""
    request = make_request_row(status="queued")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_handle_queued_no_capacity(lifecycle_manager, mock_repository, settings):
    """QUEUED with no capacity doesn't transition."""
    lifecycle_manager.dag_planner = AsyncMock()
    mock_repository.count_active_dags.return_value = settings.max_active_dags
    request = make_request_row(status="queued")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_handle_active_no_dag_monitor(lifecycle_manager, mock_repository):
    """ACTIVE without dag_monitor skips gracefully."""
    request = make_request_row(status="active")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_terminal_statuses_not_dispatched(lifecycle_manager, mock_repository):
    """Terminal statuses (completed, failed, aborted) have no handler."""
    for status in ("completed", "failed", "aborted"):
        request = make_request_row(status=status)
        await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()
