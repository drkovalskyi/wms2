"""Unit tests for OutputManager."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockDBSAdapter, MockRucioAdapter
from wms2.config import Settings
from wms2.core.output_manager import OutputManager
from wms2.models.enums import OutputStatus


def _make_settings(**overrides):
    defaults = {
        "database_url": "postgresql+asyncpg://test:test@localhost:5433/test",
        "local_pfn_prefix": "/mnt/shared",
    }
    defaults.update(overrides)
    return Settings(**defaults)


_DEFAULT_CONFIG_DATA = {
    "output_datasets": [
        {
            "dataset_name": "/Primary/Era-Proc-v1/AODSIM",
            "merged_lfn_base": "/store/mc/Era/Primary/AODSIM/Proc-v1",
        },
        {
            "dataset_name": "/Primary/Era-Proc-v1/MINIAODSIM",
            "merged_lfn_base": "/store/mc/Era/Primary/MINIAODSIM/Proc-v1",
        },
    ],
    "merged_lfn_base": "/store/mc",
}


def _make_workflow(config_data=_DEFAULT_CONFIG_DATA):
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.config_data = config_data
    return wf


def _make_output_row(
    workflow_id=None, dataset_name="/Primary/Era-Proc-v1/AODSIM",
    status="pending", **kwargs,
):
    row = MagicMock()
    row.id = uuid.uuid4()
    row.workflow_id = workflow_id or uuid.uuid4()
    row.dataset_name = dataset_name
    row.status = status
    row.source_rule_id = kwargs.get("source_rule_id")
    row.transfer_rule_ids = kwargs.get("transfer_rule_ids", [])
    row.transfer_destinations = kwargs.get("transfer_destinations", [])
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.create_output_dataset = AsyncMock()
    repo.get_output_datasets = AsyncMock(return_value=[])
    repo.update_output_dataset = AsyncMock()
    return repo


@pytest.fixture
def mock_dbs():
    return MockDBSAdapter()


@pytest.fixture
def mock_rucio():
    return MockRucioAdapter()


@pytest.fixture
def output_manager(mock_repo, mock_dbs, mock_rucio):
    settings = _make_settings()
    return OutputManager(mock_repo, mock_dbs, mock_rucio, settings)


class TestHandleMergeCompletion:
    async def test_creates_records_for_each_dataset(self, output_manager, mock_repo):
        """N output datasets → N OutputDataset records created."""
        workflow = _make_workflow()
        merge_info = {"group_name": "mg_000000", "manifest": None}

        await output_manager.handle_merge_completion(workflow, merge_info)

        assert mock_repo.create_output_dataset.call_count == 2
        calls = mock_repo.create_output_dataset.call_args_list
        dataset_names = {c[1]["dataset_name"] for c in calls}
        assert "/Primary/Era-Proc-v1/AODSIM" in dataset_names
        assert "/Primary/Era-Proc-v1/MINIAODSIM" in dataset_names

    async def test_no_manifest_uses_config_data(self, output_manager, mock_repo):
        """With no manifest, output info is derived from workflow config_data."""
        workflow = _make_workflow()
        merge_info = {"group_name": "mg_000000", "manifest": None}

        await output_manager.handle_merge_completion(workflow, merge_info)
        assert mock_repo.create_output_dataset.call_count == 2

    async def test_no_output_datasets_skips(self, output_manager, mock_repo):
        """No output_datasets in config → no records created."""
        workflow = _make_workflow(config_data={})
        merge_info = {"group_name": "mg_000000", "manifest": None}

        await output_manager.handle_merge_completion(workflow, merge_info)
        mock_repo.create_output_dataset.assert_not_called()


class TestProcessOutputsPipeline:
    async def test_full_pipeline_mock_adapters(self, output_manager, mock_repo, mock_dbs, mock_rucio):
        """PENDING → DBS_REGISTERED → ... → ANNOUNCED in one cycle (mock adapters)."""
        workflow_id = uuid.uuid4()
        output_row = _make_output_row(workflow_id=workflow_id, status="pending")

        # get_output_datasets returns our output initially and on re-fetch
        mock_repo.get_output_datasets.return_value = [output_row]

        await output_manager.process_outputs_for_workflow(workflow_id)

        # DBS inject called
        assert any(c[0] == "inject_dataset" for c in mock_dbs.calls)
        # Rucio create_rule called (source protection + transfer)
        create_rule_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_rule_calls) >= 2
        # Rucio get_rule_status called (transfer check)
        assert any(c[0] == "get_rule_status" for c in mock_rucio.calls)
        # Rucio delete_rule called (source release)
        assert any(c[0] == "delete_rule" for c in mock_rucio.calls)

        # update_output_dataset called multiple times (one per state transition)
        assert mock_repo.update_output_dataset.call_count >= 5

    async def test_register_in_dbs_calls_adapter(self, output_manager, mock_repo, mock_dbs):
        """_register_in_dbs calls dbs.inject_dataset."""
        output = _make_output_row()
        await output_manager._register_in_dbs(output)

        assert len(mock_dbs.calls) == 1
        assert mock_dbs.calls[0][0] == "inject_dataset"
        mock_repo.update_output_dataset.assert_called_once()
        call_kwargs = mock_repo.update_output_dataset.call_args[1]
        assert call_kwargs["status"] == OutputStatus.DBS_REGISTERED.value
        assert call_kwargs["dbs_registered"] is True

    async def test_protect_source_calls_rucio(self, output_manager, mock_repo, mock_rucio):
        """_protect_source calls rucio.create_rule."""
        output = _make_output_row(status="dbs_registered")
        await output_manager._protect_source(output)

        assert len(mock_rucio.calls) == 1
        assert mock_rucio.calls[0][0] == "create_rule"
        call_kwargs = mock_repo.update_output_dataset.call_args[1]
        assert call_kwargs["status"] == OutputStatus.SOURCE_PROTECTED.value
        assert call_kwargs["source_rule_id"] == "rule-id-12345"

    async def test_request_transfers_calls_rucio(self, output_manager, mock_repo, mock_rucio):
        """_request_transfers calls rucio.create_rule for destinations."""
        output = _make_output_row(status="source_protected")
        await output_manager._request_transfers(output)

        assert len(mock_rucio.calls) == 1
        assert mock_rucio.calls[0][0] == "create_rule"
        call_kwargs = mock_repo.update_output_dataset.call_args[1]
        assert call_kwargs["status"] == OutputStatus.TRANSFERS_REQUESTED.value
        assert len(call_kwargs["transfer_rule_ids"]) == 1


class TestGetOutputSummary:
    async def test_counts_by_status(self, output_manager, mock_repo):
        """get_output_summary returns correct counts."""
        workflow_id = uuid.uuid4()
        mock_repo.get_output_datasets.return_value = [
            _make_output_row(status="announced"),
            _make_output_row(status="announced"),
            _make_output_row(status="pending"),
        ]
        summary = await output_manager.get_output_summary(workflow_id)
        assert summary == {"announced": 2, "pending": 1}


class TestValidateLocalOutput:
    def test_file_exists(self, output_manager, tmp_path):
        """validate_local_output returns True for existing file."""
        output_manager.settings.local_pfn_prefix = str(tmp_path)
        # Create a file at the expected PFN path (prefix + LFN)
        target = tmp_path / "store" / "mc" / "Era" / "Primary" / "TIER"
        target.mkdir(parents=True)
        (target / "merged.txt").touch()

        assert output_manager.validate_local_output("/store/mc/Era/Primary/TIER/merged.txt")

    def test_file_missing(self, output_manager):
        """validate_local_output returns False for missing file."""
        assert not output_manager.validate_local_output("/store/mc/no/such/file.root")
