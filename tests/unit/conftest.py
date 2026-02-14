import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository


@pytest.fixture
def mock_repository():
    repo = MagicMock(spec=Repository)
    # Default async returns
    repo.get_non_terminal_requests = AsyncMock(return_value=[])
    repo.get_request = AsyncMock(return_value=None)
    repo.create_request = AsyncMock()
    repo.update_request = AsyncMock()
    repo.get_workflow_by_request = AsyncMock(return_value=None)
    repo.get_dag = AsyncMock(return_value=None)
    repo.count_active_dags = AsyncMock(return_value=0)
    repo.get_queued_requests = AsyncMock(return_value=[])
    repo.create_dag = AsyncMock()
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    repo.count_requests_by_status = AsyncMock(return_value={})
    return repo


@pytest.fixture
def mock_condor():
    return MockCondorAdapter()


@pytest.fixture
def settings():
    return Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
        lifecycle_cycle_interval=1,
        max_active_dags=10,
        timeout_submitted=60,
        timeout_queued=120,
        timeout_pilot_running=60,
        timeout_planning=60,
        timeout_active=120,
        timeout_stopping=60,
        timeout_resubmitting=60,
    )


def make_request_row(
    request_name="test-request-001",
    status="new",
    priority=100000,
    urgent=False,
    production_steps=None,
    **kwargs,
):
    """Helper to create a mock request row."""
    now = datetime.now(timezone.utc)
    row = MagicMock()
    row.id = uuid.uuid4()
    row.request_name = request_name
    row.requestor = "testuser"
    row.requestor_dn = "/CN=testuser"
    row.request_data = {}
    row.payload_config = {}
    row.splitting_params = {}
    row.input_dataset = "/TestPrimary/TestProcessed/RECO"
    row.campaign = "TestCampaign"
    row.priority = priority
    row.urgent = urgent
    row.production_steps = production_steps or []
    row.status = status
    row.status_transitions = []
    row.previous_version_request = None
    row.superseded_by_request = None
    row.cleanup_policy = "keep_until_replaced"
    row.created_at = now
    row.updated_at = now
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row
