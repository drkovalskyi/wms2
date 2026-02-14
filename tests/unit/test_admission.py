from unittest.mock import MagicMock

import pytest

from wms2.core.admission_controller import AdmissionController
from .conftest import make_request_row


@pytest.fixture
def admission_controller(mock_repository, settings):
    return AdmissionController(mock_repository, settings)


async def test_has_capacity_when_below_limit(admission_controller, mock_repository):
    mock_repository.count_active_dags.return_value = 5
    assert await admission_controller.has_capacity() is True


async def test_no_capacity_at_limit(admission_controller, mock_repository, settings):
    mock_repository.count_active_dags.return_value = settings.max_active_dags
    assert await admission_controller.has_capacity() is False


async def test_get_next_pending_returns_highest_priority(admission_controller, mock_repository):
    high_priority = make_request_row(request_name="high", priority=200000, status="queued")
    mock_repository.get_queued_requests.return_value = [high_priority]
    result = await admission_controller.get_next_pending()
    assert result.request_name == "high"


async def test_get_next_pending_empty_queue(admission_controller, mock_repository):
    mock_repository.get_queued_requests.return_value = []
    result = await admission_controller.get_next_pending()
    assert result is None


async def test_queue_status(admission_controller, mock_repository, settings):
    mock_repository.count_active_dags.return_value = 3
    req = make_request_row(request_name="pending-1", status="queued")
    mock_repository.get_queued_requests.return_value = [req]

    status = await admission_controller.get_queue_status()
    assert status["active_dags"] == 3
    assert status["max_active_dags"] == settings.max_active_dags
    assert status["capacity_available"] is True
    assert status["queued_requests"] == 1
    assert status["next_pending"] == "pending-1"
