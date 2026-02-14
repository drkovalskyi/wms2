"""Tests for WorkflowManager."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockReqMgrAdapter
from wms2.core.workflow_manager import WorkflowManager


@pytest.fixture
def workflow_manager(mock_repository, mock_reqmgr):
    return WorkflowManager(mock_repository, mock_reqmgr)


class TestImportRequest:
    async def test_import_creates_workflow(self, workflow_manager, mock_repository):
        wf_row = MagicMock()
        wf_row.id = uuid.uuid4()
        mock_repository.create_workflow = AsyncMock(return_value=wf_row)

        result = await workflow_manager.import_request("test-request-001")
        assert result == wf_row
        mock_repository.create_workflow.assert_called_once()
        call_kwargs = mock_repository.create_workflow.call_args[1]
        assert call_kwargs["request_name"] == "test-request-001"
        assert call_kwargs["input_dataset"] == "/TestPrimary/TestProcessed/RECO"
        assert call_kwargs["splitting_algo"] == "FileBased"

    async def test_import_validates_required_fields(self, mock_repository):
        """Missing required fields should raise ValueError."""
        bad_reqmgr = MagicMock()
        bad_reqmgr.get_request = AsyncMock(return_value={
            "RequestName": "test",
            # Missing InputDataset, SplittingAlgo, SandboxUrl
        })
        wm = WorkflowManager(mock_repository, bad_reqmgr)
        with pytest.raises(ValueError, match="missing required fields"):
            await wm.import_request("test")

    async def test_import_passes_splitting_params(self, workflow_manager, mock_repository):
        wf_row = MagicMock()
        wf_row.id = uuid.uuid4()
        mock_repository.create_workflow = AsyncMock(return_value=wf_row)

        await workflow_manager.import_request("test-request-001")
        call_kwargs = mock_repository.create_workflow.call_args[1]
        assert call_kwargs["splitting_params"] == {"files_per_job": 10}


class TestGetWorkflowStatus:
    async def test_missing_workflow(self, workflow_manager):
        result = await workflow_manager.get_workflow_status(uuid.uuid4())
        assert result == {}

    async def test_status_summary(self, workflow_manager, mock_repository):
        wf = MagicMock()
        wf.id = uuid.uuid4()
        wf.request_name = "test-req"
        wf.status = "active"
        wf.total_nodes = 100
        wf.nodes_done = 50
        wf.nodes_failed = 2
        wf.nodes_running = 10
        wf.dag_id = None
        mock_repository.get_workflow = AsyncMock(return_value=wf)

        result = await workflow_manager.get_workflow_status(wf.id)
        assert result["status"] == "active"
        assert result["total_nodes"] == 100
        assert result["nodes_done"] == 50
