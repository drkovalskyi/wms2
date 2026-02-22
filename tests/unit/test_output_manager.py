"""Unit tests for OutputManager (ProcessingBlock model)."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockDBSAdapter, MockRucioAdapter
from wms2.core.output_manager import OutputManager
from wms2.models.enums import BlockStatus


def _make_block_row(
    workflow_id=None,
    block_index=0,
    dataset_name="/Primary/Era-Proc-v1/AODSIM",
    total_work_units=3,
    completed_work_units=None,
    dbs_block_name=None,
    dbs_block_open=False,
    dbs_block_closed=False,
    source_rule_ids=None,
    tape_rule_id=None,
    last_rucio_attempt=None,
    rucio_attempt_count=0,
    rucio_last_error=None,
    status="open",
    **kwargs,
):
    row = MagicMock()
    row.id = uuid.uuid4()
    row.workflow_id = workflow_id or uuid.uuid4()
    row.block_index = block_index
    row.dataset_name = dataset_name
    row.total_work_units = total_work_units
    row.completed_work_units = completed_work_units or []
    row.dbs_block_name = dbs_block_name
    row.dbs_block_open = dbs_block_open
    row.dbs_block_closed = dbs_block_closed
    row.source_rule_ids = source_rule_ids or {}
    row.tape_rule_id = tape_rule_id
    row.last_rucio_attempt = last_rucio_attempt
    row.rucio_attempt_count = rucio_attempt_count
    row.rucio_last_error = rucio_last_error
    row.status = status
    row.created_at = datetime.now(timezone.utc)
    row.updated_at = datetime.now(timezone.utc)
    for k, v in kwargs.items():
        setattr(row, k, v)
    return row


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.get_processing_block = AsyncMock(return_value=None)
    repo.get_processing_blocks = AsyncMock(return_value=[])
    repo.update_processing_block = AsyncMock()
    repo.create_processing_block = AsyncMock()
    repo.count_processing_blocks_by_status = AsyncMock(return_value=0)
    return repo


@pytest.fixture
def mock_dbs():
    return MockDBSAdapter()


@pytest.fixture
def mock_rucio():
    return MockRucioAdapter()


@pytest.fixture
def output_manager(mock_repo, mock_dbs, mock_rucio):
    return OutputManager(mock_repo, mock_dbs, mock_rucio)


class TestHandleWorkUnitCompletion:
    async def test_first_wu_opens_dbs_block(self, output_manager, mock_repo, mock_dbs):
        """First work unit for a block should open the DBS block."""
        block = _make_block_row(
            total_work_units=3, dbs_block_open=False, dbs_block_name=None
        )
        mock_repo.get_processing_block.return_value = block

        merge_info = {
            "node_name": "mg_000000",
            "output_files": [{"lfn": "/store/mc/file1.root", "size": 1000}],
            "site": "T1_US_FNAL",
        }

        await output_manager.handle_work_unit_completion(
            block.workflow_id, block.id, merge_info
        )

        # DBS open_block called
        assert any(c[0] == "open_block" for c in mock_dbs.calls)

        # DB updated with block name and open flag
        update_calls = mock_repo.update_processing_block.call_args_list
        open_call = update_calls[0]
        assert open_call[1]["dbs_block_open"] is True
        assert "dbs_block_name" in open_call[1]

    async def test_registers_files_in_dbs(self, output_manager, mock_repo, mock_dbs):
        """Should register output files in the DBS block."""
        block = _make_block_row(
            total_work_units=3,
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
        )
        mock_repo.get_processing_block.return_value = block

        merge_info = {
            "node_name": "mg_000001",
            "output_files": [
                {"lfn": "/store/mc/file1.root", "size": 1000},
                {"lfn": "/store/mc/file2.root", "size": 2000},
            ],
            "site": "T2_US_MIT",
        }

        await output_manager.handle_work_unit_completion(
            block.workflow_id, block.id, merge_info
        )

        # DBS register_files called
        register_calls = [c for c in mock_dbs.calls if c[0] == "register_files"]
        assert len(register_calls) == 1
        assert len(register_calls[0][1][1]) == 2  # 2 files

    async def test_creates_source_protection_rule(
        self, output_manager, mock_repo, mock_rucio
    ):
        """Should create a Rucio source protection rule."""
        block = _make_block_row(
            total_work_units=3,
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
        )
        mock_repo.get_processing_block.return_value = block

        merge_info = {
            "node_name": "mg_000000",
            "output_files": [],
            "site": "T1_US_FNAL",
        }

        await output_manager.handle_work_unit_completion(
            block.workflow_id, block.id, merge_info
        )

        # Rucio create_rule called for source protection
        create_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_calls) == 1
        assert create_calls[0][1][1] == "T1_US_FNAL"  # destination = site

    async def test_updates_completed_work_units(self, output_manager, mock_repo):
        """Should add node_name to completed_work_units and source_rule_ids."""
        block = _make_block_row(
            total_work_units=3,
            completed_work_units=["mg_000000"],
            source_rule_ids={"mg_000000": "rule-abc"},
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
        )
        mock_repo.get_processing_block.return_value = block

        merge_info = {
            "node_name": "mg_000001",
            "output_files": [],
            "site": "T2_US_MIT",
        }

        await output_manager.handle_work_unit_completion(
            block.workflow_id, block.id, merge_info
        )

        # Check the update call for completed_work_units
        update_calls = mock_repo.update_processing_block.call_args_list
        wu_update = update_calls[-1]
        assert "mg_000001" in wu_update[1]["completed_work_units"]
        assert "mg_000001" in wu_update[1]["source_rule_ids"]

    async def test_completes_block_when_all_wus_done(
        self, output_manager, mock_repo, mock_dbs, mock_rucio
    ):
        """When all work units done, should close DBS block and create tape rule."""
        block = _make_block_row(
            total_work_units=2,
            completed_work_units=["mg_000000"],
            source_rule_ids={"mg_000000": "rule-abc"},
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
        )
        mock_repo.get_processing_block.return_value = block

        merge_info = {
            "node_name": "mg_000001",
            "output_files": [],
            "site": "T1_US_FNAL",
        }

        await output_manager.handle_work_unit_completion(
            block.workflow_id, block.id, merge_info
        )

        # DBS close_block called
        close_calls = [c for c in mock_dbs.calls if c[0] == "close_block"]
        assert len(close_calls) == 1

        # Rucio create_rule called twice: source protection + tape archival
        create_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_calls) == 2
        # Second call is tape archival
        assert create_calls[1][1][1] == "tier=1&type=TAPE"

        # Status updated to ARCHIVED (source protection + WU update + complete + tape)
        update_calls = mock_repo.update_processing_block.call_args_list
        statuses_set = [
            c[1].get("status") for c in update_calls if "status" in c[1]
        ]
        assert BlockStatus.COMPLETE.value in statuses_set
        assert BlockStatus.ARCHIVED.value in statuses_set

    async def test_block_not_found_logs_error(self, output_manager, mock_repo):
        """If block not found, should log error and return."""
        mock_repo.get_processing_block.return_value = None
        wf_id = uuid.uuid4()
        block_id = uuid.uuid4()

        await output_manager.handle_work_unit_completion(
            wf_id, block_id, {"node_name": "mg_000000"}
        )

        # No updates made
        mock_repo.update_processing_block.assert_not_called()


class TestCompleteBlock:
    async def test_closes_dbs_block_and_creates_tape_rule(
        self, output_manager, mock_repo, mock_dbs, mock_rucio
    ):
        """_complete_block should close DBS block and create tape archival rule."""
        block = _make_block_row(
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
            dbs_block_open=True,
            dbs_block_closed=False,
        )

        await output_manager._complete_block(block)

        # DBS close_block called
        assert any(c[0] == "close_block" for c in mock_dbs.calls)

        # Rucio tape rule created
        create_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_calls) == 1
        assert create_calls[0][1][1] == "tier=1&type=TAPE"

        # Status transitions: COMPLETE then ARCHIVED
        update_calls = mock_repo.update_processing_block.call_args_list
        assert update_calls[0][1]["status"] == BlockStatus.COMPLETE.value
        assert update_calls[0][1]["dbs_block_closed"] is True
        assert update_calls[1][1]["status"] == BlockStatus.ARCHIVED.value
        assert update_calls[1][1]["tape_rule_id"] == "rule-id-12345"


class TestProcessBlocksForWorkflow:
    async def test_retries_complete_blocks_missing_tape_rule(
        self, output_manager, mock_repo, mock_rucio
    ):
        """COMPLETE blocks should get tape archival retried."""
        block = _make_block_row(
            status=BlockStatus.COMPLETE.value,
            dataset_name="/Primary/Era-Proc-v1/AODSIM",
            rucio_attempt_count=1,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(minutes=10),
        )
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.process_blocks_for_workflow(block.workflow_id)

        # Rucio tape rule creation attempted
        create_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_calls) == 1

        # Status updated to ARCHIVED
        update_call = mock_repo.update_processing_block.call_args
        assert update_call[1]["status"] == BlockStatus.ARCHIVED.value

    async def test_retries_open_blocks_with_rucio_errors(
        self, output_manager, mock_repo, mock_rucio
    ):
        """OPEN blocks with rucio errors should get source protection retried."""
        block = _make_block_row(
            status=BlockStatus.OPEN.value,
            rucio_last_error="connection timeout",
            rucio_attempt_count=1,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(minutes=10),
        )
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.process_blocks_for_workflow(block.workflow_id)

        # Source protection retry attempted
        create_calls = [c for c in mock_rucio.calls if c[0] == "create_rule"]
        assert len(create_calls) == 1

    async def test_skips_archived_blocks(self, output_manager, mock_repo, mock_rucio):
        """ARCHIVED blocks should not be retried."""
        block = _make_block_row(status=BlockStatus.ARCHIVED.value)
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.process_blocks_for_workflow(block.workflow_id)

        assert len(mock_rucio.calls) == 0

    async def test_skips_during_backoff(self, output_manager, mock_repo, mock_rucio):
        """Should not retry if within backoff window."""
        block = _make_block_row(
            status=BlockStatus.COMPLETE.value,
            rucio_attempt_count=0,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(seconds=30),
            # First backoff is 60s, so 30s ago = still in backoff
        )
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.process_blocks_for_workflow(block.workflow_id)

        # No Rucio calls — within backoff
        assert len(mock_rucio.calls) == 0


class TestShouldRetry:
    def test_allows_first_retry(self, output_manager):
        """First retry (no last_rucio_attempt) should be allowed."""
        block = _make_block_row(last_rucio_attempt=None)
        assert output_manager._should_retry(block) is True

    def test_respects_backoff_schedule(self, output_manager):
        """Should not retry if within backoff window."""
        block = _make_block_row(
            rucio_attempt_count=0,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(seconds=30),
        )
        assert output_manager._should_retry(block) is False

    def test_allows_retry_after_backoff(self, output_manager):
        """Should allow retry after backoff elapsed."""
        block = _make_block_row(
            rucio_attempt_count=0,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        assert output_manager._should_retry(block) is True

    def test_marks_failed_after_max_duration(self, output_manager):
        """Should return False after max retry duration and log failure."""
        block = _make_block_row(
            rucio_attempt_count=10,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(hours=1),
            created_at=datetime.now(timezone.utc) - timedelta(days=4),
            rucio_last_error="persistent failure",
        )
        assert output_manager._should_retry(block) is False

    def test_uses_highest_backoff_for_many_attempts(self, output_manager):
        """Attempts beyond schedule length use last backoff value."""
        block = _make_block_row(
            rucio_attempt_count=100,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(hours=10),
        )
        # Last backoff is 8h, 10h ago > 8h, so should allow
        assert output_manager._should_retry(block) is True


class TestInvalidateBlocks:
    async def test_deletes_source_and_tape_rules(
        self, output_manager, mock_repo, mock_rucio, mock_dbs
    ):
        """Should delete source protection rules and tape rule."""
        block = _make_block_row(
            source_rule_ids={"mg_000000": "rule-1", "mg_000001": "rule-2"},
            tape_rule_id="tape-rule-1",
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
            status=BlockStatus.ARCHIVED.value,
        )
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.invalidate_blocks(block.workflow_id, "catastrophic recovery")

        # Source rules deleted
        delete_calls = [c for c in mock_rucio.calls if c[0] == "delete_rule"]
        assert len(delete_calls) == 3  # 2 source + 1 tape
        deleted_ids = {c[1][0] for c in delete_calls}
        assert "rule-1" in deleted_ids
        assert "rule-2" in deleted_ids
        assert "tape-rule-1" in deleted_ids

        # DBS block invalidated
        invalidate_calls = [c for c in mock_dbs.calls if c[0] == "invalidate_block"]
        assert len(invalidate_calls) == 1

        # Status set to INVALIDATED
        update_call = mock_repo.update_processing_block.call_args
        assert update_call[1]["status"] == BlockStatus.INVALIDATED.value

    async def test_handles_rucio_errors_gracefully(
        self, output_manager, mock_repo, mock_dbs
    ):
        """Should continue even if Rucio delete_rule fails."""
        # Use a custom rucio that raises
        failing_rucio = MagicMock()
        failing_rucio.delete_rule = AsyncMock(side_effect=Exception("Rucio down"))
        output_manager.rucio = failing_rucio

        block = _make_block_row(
            source_rule_ids={"mg_000000": "rule-1"},
            tape_rule_id="tape-rule-1",
            dbs_block_open=True,
            dbs_block_name="/Primary/Era-Proc-v1/AODSIM#block_0",
        )
        mock_repo.get_processing_blocks.return_value = [block]

        # Should not raise
        await output_manager.invalidate_blocks(block.workflow_id, "test")

        # Status still set to INVALIDATED despite errors
        update_call = mock_repo.update_processing_block.call_args
        assert update_call[1]["status"] == BlockStatus.INVALIDATED.value

    async def test_skips_blocks_without_dbs_block(
        self, output_manager, mock_repo, mock_dbs, mock_rucio
    ):
        """Blocks without open DBS block should not call invalidate_block."""
        block = _make_block_row(
            source_rule_ids={},
            tape_rule_id=None,
            dbs_block_open=False,
            dbs_block_name=None,
        )
        mock_repo.get_processing_blocks.return_value = [block]

        await output_manager.invalidate_blocks(block.workflow_id, "test")

        # No DBS invalidation
        invalidate_calls = [c for c in mock_dbs.calls if c[0] == "invalidate_block"]
        assert len(invalidate_calls) == 0

        # Status still set
        update_call = mock_repo.update_processing_block.call_args
        assert update_call[1]["status"] == BlockStatus.INVALIDATED.value


class TestAllBlocksArchived:
    async def test_true_when_all_archived(self, output_manager, mock_repo):
        """Returns True when all blocks have ARCHIVED status."""
        blocks = [
            _make_block_row(status=BlockStatus.ARCHIVED.value),
            _make_block_row(status=BlockStatus.ARCHIVED.value),
        ]
        mock_repo.get_processing_blocks.return_value = blocks

        result = await output_manager.all_blocks_archived(uuid.uuid4())
        assert result is True

    async def test_false_when_some_open(self, output_manager, mock_repo):
        """Returns False when some blocks are still OPEN."""
        blocks = [
            _make_block_row(status=BlockStatus.ARCHIVED.value),
            _make_block_row(status=BlockStatus.OPEN.value),
        ]
        mock_repo.get_processing_blocks.return_value = blocks

        result = await output_manager.all_blocks_archived(uuid.uuid4())
        assert result is False

    async def test_false_when_some_complete(self, output_manager, mock_repo):
        """Returns False when some blocks are COMPLETE (tape rule pending)."""
        blocks = [
            _make_block_row(status=BlockStatus.ARCHIVED.value),
            _make_block_row(status=BlockStatus.COMPLETE.value),
        ]
        mock_repo.get_processing_blocks.return_value = blocks

        result = await output_manager.all_blocks_archived(uuid.uuid4())
        assert result is False

    async def test_true_when_no_blocks(self, output_manager, mock_repo):
        """Returns True when workflow has no blocks (e.g. GEN-only)."""
        mock_repo.get_processing_blocks.return_value = []

        result = await output_manager.all_blocks_archived(uuid.uuid4())
        assert result is True

    async def test_false_when_failed(self, output_manager, mock_repo):
        """Returns False when a block is FAILED."""
        blocks = [
            _make_block_row(status=BlockStatus.ARCHIVED.value),
            _make_block_row(status=BlockStatus.FAILED.value),
        ]
        mock_repo.get_processing_blocks.return_value = blocks

        result = await output_manager.all_blocks_archived(uuid.uuid4())
        assert result is False
