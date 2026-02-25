"""Integration tests for processing block lifecycle with real DB.

Tests the OutputManager + Repository interaction end-to-end using a real
PostgreSQL database with mock external services (DBS, Rucio).
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter, MockDBSAdapter, MockRucioAdapter
from wms2.config import Settings
from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.core.output_manager import OutputManager
from wms2.models.enums import BlockStatus, DAGStatus, RequestStatus


# ── Helpers ──────────────────────────────────────────────────────


def _synthetic_merge_info(node_name: str, site: str = "T2_LOCAL_A", n_files: int = 2):
    """Build a merge_info dict with synthetic output files."""
    return {
        "node_name": node_name,
        "output_files": [
            {"lfn": f"/store/mc/Test/Primary/GEN-SIM/v1/0000/{node_name}_{i}.root",
             "size": 500_000 + i * 100_000}
            for i in range(n_files)
        ],
        "site": site,
    }


class TrackingRucioAdapter(MockRucioAdapter):
    """Mock Rucio that returns unique rule IDs for easier verification."""

    def __init__(self):
        super().__init__()
        self._rule_counter = 0

    async def create_rule(self, dataset, destination, **kwargs):
        self.calls.append(("create_rule", (dataset, destination), kwargs))
        self._rule_counter += 1
        return f"rule-{self._rule_counter:05d}"


class FailingRucioAdapter(TrackingRucioAdapter):
    """Mock Rucio that fails for specific destinations."""

    def __init__(self):
        super().__init__()
        self.fail_destinations: set[str] = set()

    async def create_rule(self, dataset, destination, **kwargs):
        self.calls.append(("create_rule", (dataset, destination), kwargs))
        if destination in self.fail_destinations:
            raise Exception(f"Rucio unavailable for {destination}")
        self._rule_counter += 1
        return f"rule-{self._rule_counter:05d}"


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def mock_dbs():
    return MockDBSAdapter()


@pytest.fixture
def tracking_rucio():
    return TrackingRucioAdapter()


@pytest.fixture
def output_manager(repo, mock_dbs, tracking_rucio):
    return OutputManager(repo, mock_dbs, tracking_rucio)


async def _create_request(repo, name="test-request-001", status="active"):
    now = datetime.now(timezone.utc)
    return await repo.create_request(
        request_name=name,
        requestor="testuser",
        request_data={},
        status=status,
        status_transitions=[],
        created_at=now,
        updated_at=now,
    )


async def _create_workflow(repo, request_name="test-request-001"):
    now = datetime.now(timezone.utc)
    return await repo.create_workflow(
        request_name=request_name,
        input_dataset="",
        splitting_algo="EventBased",
        splitting_params={"events_per_job": 100},
        config_data={"_is_gen": True},
        status="active",
        created_at=now,
        updated_at=now,
    )


async def _create_dag(repo, workflow_id, total_work_units=2, status="submitted"):
    return await repo.create_dag(
        workflow_id=workflow_id,
        dag_file_path="/tmp/submit/workflow.dag",
        submit_dir="/tmp/submit",
        total_nodes=10,
        total_work_units=total_work_units,
        completed_work_units=[],
        status=status,
    )


async def _create_block(
    repo,
    workflow_id,
    block_index=0,
    dataset_name="/TestPrimary/Test-v1/GEN-SIM",
    total_work_units=2,
    **kwargs,
):
    now = datetime.now(timezone.utc)
    return await repo.create_processing_block(
        workflow_id=workflow_id,
        block_index=block_index,
        dataset_name=dataset_name,
        total_work_units=total_work_units,
        completed_work_units=kwargs.get("completed_work_units", []),
        dbs_block_name=kwargs.get("dbs_block_name"),
        dbs_block_open=kwargs.get("dbs_block_open", False),
        dbs_block_closed=kwargs.get("dbs_block_closed", False),
        source_rule_ids=kwargs.get("source_rule_ids", {}),
        tape_rule_id=kwargs.get("tape_rule_id"),
        last_rucio_attempt=kwargs.get("last_rucio_attempt"),
        rucio_attempt_count=kwargs.get("rucio_attempt_count", 0),
        rucio_last_error=kwargs.get("rucio_last_error"),
        status=kwargs.get("status", "open"),
        created_at=now,
        updated_at=now,
    )


# ── Test 1: Full lifecycle with a single block ──────────────────


class TestFullLifecycleSingleBlock:
    """Happy path: one block, two work units, OPEN → COMPLETE → ARCHIVED."""

    async def test_full_lifecycle(self, repo, output_manager, mock_dbs, tracking_rucio):
        request = await _create_request(repo)
        workflow = await _create_workflow(repo, request.request_name)
        block = await _create_block(repo, workflow.id, total_work_units=2)

        # ── WU 1: first work unit opens the DBS block ───────────
        await output_manager.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000000", site="T2_LOCAL_A"),
        )

        # DBS: open_block + register_files
        assert any(c[0] == "open_block" for c in mock_dbs.calls)
        assert any(c[0] == "register_files" for c in mock_dbs.calls)

        # Rucio: one source protection rule created
        source_calls = [c for c in tracking_rucio.calls if c[0] == "create_rule"]
        assert len(source_calls) == 1
        assert source_calls[0][1][1] == "T2_LOCAL_A"  # destination = site

        # DB: block is still OPEN, 1 WU done
        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.OPEN.value
        assert refreshed.dbs_block_open is True
        assert len(refreshed.completed_work_units) == 1
        assert "mg_000000" in refreshed.completed_work_units
        assert len(refreshed.source_rule_ids) == 1

        # Not yet archived
        assert await output_manager.all_blocks_archived(workflow.id) is False

        # ── WU 2: completes the block ───────────────────────────
        mock_dbs.calls.clear()
        tracking_rucio.calls.clear()

        await output_manager.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000001", site="T2_LOCAL_B"),
        )

        # DBS: open_block NOT called again, register_files + close_block
        assert not any(c[0] == "open_block" for c in mock_dbs.calls)
        assert any(c[0] == "register_files" for c in mock_dbs.calls)
        assert any(c[0] == "close_block" for c in mock_dbs.calls)

        # Rucio: source protection + tape archival
        rucio_calls = [c for c in tracking_rucio.calls if c[0] == "create_rule"]
        assert len(rucio_calls) == 2
        destinations = {c[1][1] for c in rucio_calls}
        assert "T2_LOCAL_B" in destinations           # source protection
        assert "tier=1&type=TAPE" in destinations      # tape archival

        # DB: block is ARCHIVED
        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.ARCHIVED.value
        assert refreshed.dbs_block_closed is True
        assert refreshed.tape_rule_id is not None
        assert len(refreshed.completed_work_units) == 2
        assert len(refreshed.source_rule_ids) == 2

        # All archived
        assert await output_manager.all_blocks_archived(workflow.id) is True


# ── Test 2: Multi-block workflow ────────────────────────────────


class TestMultiBlockWorkflow:
    """Two blocks for different datasets. Both must reach ARCHIVED."""

    async def test_independent_block_progress(
        self, repo, output_manager, mock_dbs, tracking_rucio,
    ):
        request = await _create_request(repo, name="multi-block-001")
        workflow = await _create_workflow(repo, "multi-block-001")

        block_0 = await _create_block(
            repo, workflow.id, block_index=0,
            dataset_name="/TestPrimary/Test-v1/GEN-SIM",
            total_work_units=2,
        )
        block_1 = await _create_block(
            repo, workflow.id, block_index=1,
            dataset_name="/TestPrimary/Test-v1/AODSIM",
            total_work_units=2,
        )

        # ── Complete block 0 ────────────────────────────────────
        await output_manager.handle_work_unit_completion(
            workflow.id, block_0.id,
            _synthetic_merge_info("mg_000000"),
        )
        await output_manager.handle_work_unit_completion(
            workflow.id, block_0.id,
            _synthetic_merge_info("mg_000001"),
        )

        r0 = await repo.get_processing_block(block_0.id)
        assert r0.status == BlockStatus.ARCHIVED.value

        # Block 1 still OPEN → not all archived
        assert await output_manager.all_blocks_archived(workflow.id) is False

        # ── Complete block 1 ────────────────────────────────────
        await output_manager.handle_work_unit_completion(
            workflow.id, block_1.id,
            _synthetic_merge_info("mg_000002"),
        )
        await output_manager.handle_work_unit_completion(
            workflow.id, block_1.id,
            _synthetic_merge_info("mg_000003"),
        )

        r1 = await repo.get_processing_block(block_1.id)
        assert r1.status == BlockStatus.ARCHIVED.value

        # Now all archived
        assert await output_manager.all_blocks_archived(workflow.id) is True

        # Verify: 2 tape rules (one per block), 4 source rules (one per WU)
        tape_calls = [
            c for c in tracking_rucio.calls
            if c[0] == "create_rule" and c[1][1] == "tier=1&type=TAPE"
        ]
        assert len(tape_calls) == 2

        source_calls = [
            c for c in tracking_rucio.calls
            if c[0] == "create_rule" and c[1][1] != "tier=1&type=TAPE"
        ]
        assert len(source_calls) == 4

        # Verify: 2 DBS blocks opened, 2 closed
        open_calls = [c for c in mock_dbs.calls if c[0] == "open_block"]
        close_calls = [c for c in mock_dbs.calls if c[0] == "close_block"]
        assert len(open_calls) == 2
        assert len(close_calls) == 2


# ── Test 3: Source protection failure and retry ─────────────────


class TestRucioSourceProtectionRetry:
    """Source protection fails on WU completion. Retry succeeds via process_blocks."""

    async def test_failure_and_retry(self, repo, mock_dbs, session):
        failing_rucio = FailingRucioAdapter()
        failing_rucio.fail_destinations.add("T2_LOCAL_A")
        om = OutputManager(repo, mock_dbs, failing_rucio)

        request = await _create_request(repo, name="rucio-fail-001")
        workflow = await _create_workflow(repo, "rucio-fail-001")
        block = await _create_block(repo, workflow.id, total_work_units=2)

        # ── WU 1: source protection fails ───────────────────────
        await om.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000000", site="T2_LOCAL_A"),
        )

        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.OPEN.value
        assert refreshed.rucio_last_error is not None
        assert refreshed.rucio_attempt_count == 1
        # Work unit still recorded as completed (DBS reg succeeded)
        assert "mg_000000" in refreshed.completed_work_units
        # But no source rule for this WU (Rucio failed)
        assert "mg_000000" not in refreshed.source_rule_ids

        # ── Fix Rucio, retry via process_blocks ─────────────────
        failing_rucio.fail_destinations.clear()

        # Backoff: first attempt wait is 60s. Set last_rucio_attempt in the past.
        await repo.update_processing_block(
            block.id,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(minutes=5),
        )

        await om.process_blocks_for_workflow(workflow.id)

        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.rucio_last_error is None
        assert refreshed.rucio_attempt_count == 0


# ── Test 4: Tape archival failure and retry ─────────────────────


class TestTapeArchivalRetry:
    """All WUs complete, DBS closed, but tape rule fails. Retry succeeds."""

    async def test_tape_failure_and_retry(self, repo, mock_dbs, session):
        failing_rucio = FailingRucioAdapter()
        om = OutputManager(repo, mock_dbs, failing_rucio)

        request = await _create_request(repo, name="tape-fail-001")
        workflow = await _create_workflow(repo, "tape-fail-001")
        block = await _create_block(repo, workflow.id, total_work_units=2)

        # ── WU 1: succeeds ──────────────────────────────────────
        await om.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000000", site="T2_LOCAL_A"),
        )

        # ── WU 2: source protection succeeds, but tape fails ───
        # Tape destination is "tier=1&type=TAPE"
        failing_rucio.fail_destinations.add("tier=1&type=TAPE")

        await om.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000001", site="T2_LOCAL_B"),
        )

        # Block should be COMPLETE (DBS closed) but not ARCHIVED (tape failed)
        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.COMPLETE.value
        assert refreshed.dbs_block_closed is True
        assert refreshed.tape_rule_id is None
        assert refreshed.rucio_last_error is not None

        assert await om.all_blocks_archived(workflow.id) is False

        # ── Fix Rucio, retry via process_blocks ─────────────────
        failing_rucio.fail_destinations.clear()

        # Set last_rucio_attempt in the past to clear backoff
        await repo.update_processing_block(
            block.id,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(minutes=5),
        )

        await om.process_blocks_for_workflow(workflow.id)

        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.ARCHIVED.value
        assert refreshed.tape_rule_id is not None
        assert refreshed.rucio_last_error is None

        assert await om.all_blocks_archived(workflow.id) is True


# ── Test 5: Block-gated completion via lifecycle manager ────────


class TestBlockGatedCompletion:
    """DAG completes but blocks pending → request stays ACTIVE.
    Blocks archived → request COMPLETED on next cycle."""

    async def test_stays_active_until_blocks_archived(self, repo, mock_dbs, session):
        failing_rucio = FailingRucioAdapter()
        # Tape archival will fail — keeps block in COMPLETE
        failing_rucio.fail_destinations.add("tier=1&type=TAPE")

        om = OutputManager(repo, mock_dbs, failing_rucio)

        request = await _create_request(repo, name="gated-001", status="active")
        workflow = await _create_workflow(repo, "gated-001")
        dag = await _create_dag(repo, workflow.id, total_work_units=2)

        # Link DAG to workflow
        await repo.update_workflow(workflow.id, dag_id=dag.id)

        block = await _create_block(repo, workflow.id, total_work_units=2)

        # Complete both work units (tape will fail)
        await om.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000000"),
        )
        await om.handle_work_unit_completion(
            workflow.id, block.id,
            _synthetic_merge_info("mg_000001"),
        )

        # Block is COMPLETE, not ARCHIVED
        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.COMPLETE.value

        # ── Set up lifecycle manager with mock dag_monitor ──────
        mock_condor = MockCondorAdapter()
        settings = Settings(
            database_url="unused",
            lifecycle_cycle_interval=1,
        )

        # DAG monitor says: DAG completed, no new work units
        from wms2.core.dag_monitor import DAGPollResult
        mock_dag_monitor = MagicMock()
        mock_dag_monitor.poll_dag = AsyncMock(return_value=DAGPollResult(
            dag_id=str(dag.id),
            status=DAGStatus.COMPLETED,
            nodes_done=10,
        ))

        lm = RequestLifecycleManager(
            repository=repo,
            condor_adapter=mock_condor,
            settings=settings,
            dag_monitor=mock_dag_monitor,
            output_manager=om,
        )

        # ── Cycle 1: DAG COMPLETED but block not archived ───────
        # process_blocks will try to retry tape but it's within backoff
        # (last_rucio_attempt was just set by handle_work_unit_completion)
        req = await repo.get_request("gated-001")
        await lm.evaluate_request(req)

        req = await repo.get_request("gated-001")
        assert req.status == RequestStatus.ACTIVE.value  # Stayed ACTIVE

        # ── Fix Rucio and clear backoff ─────────────────────────
        failing_rucio.fail_destinations.clear()
        await repo.update_processing_block(
            block.id,
            last_rucio_attempt=datetime.now(timezone.utc) - timedelta(minutes=10),
        )

        # ── Cycle 2: process_blocks retries tape → ARCHIVED ────
        req = await repo.get_request("gated-001")
        await lm.evaluate_request(req)

        req = await repo.get_request("gated-001")
        assert req.status == RequestStatus.COMPLETED.value

        # Block is now ARCHIVED
        refreshed = await repo.get_processing_block(block.id)
        assert refreshed.status == BlockStatus.ARCHIVED.value


# ── Test 6: Catastrophic invalidation ───────────────────────────


class TestInvalidateBlocksCatastrophic:
    """Blocks in various states all get invalidated: rules deleted, DBS invalidated."""

    async def test_invalidate_all_blocks(
        self, repo, output_manager, mock_dbs, tracking_rucio,
    ):
        request = await _create_request(repo, name="catastrophic-001")
        workflow = await _create_workflow(repo, "catastrophic-001")

        # Block 0: OPEN with 1 WU done (has source rule, DBS block open)
        block_0 = await _create_block(
            repo, workflow.id, block_index=0,
            dataset_name="/TestPrimary/Test-v1/GEN-SIM",
            total_work_units=3,
        )
        await output_manager.handle_work_unit_completion(
            workflow.id, block_0.id,
            _synthetic_merge_info("mg_000000"),
        )

        r0 = await repo.get_processing_block(block_0.id)
        assert r0.status == BlockStatus.OPEN.value
        assert r0.dbs_block_open is True
        source_rule_0 = list(r0.source_rule_ids.values())[0]

        # Block 1: ARCHIVED (2 WUs done, tape rule exists)
        block_1 = await _create_block(
            repo, workflow.id, block_index=1,
            dataset_name="/TestPrimary/Test-v1/AODSIM",
            total_work_units=2,
        )
        await output_manager.handle_work_unit_completion(
            workflow.id, block_1.id,
            _synthetic_merge_info("mg_000001"),
        )
        await output_manager.handle_work_unit_completion(
            workflow.id, block_1.id,
            _synthetic_merge_info("mg_000002"),
        )

        r1 = await repo.get_processing_block(block_1.id)
        assert r1.status == BlockStatus.ARCHIVED.value
        tape_rule_1 = r1.tape_rule_id

        # Clear call logs before invalidation
        tracking_rucio.calls.clear()
        mock_dbs.calls.clear()

        # ── Invalidate all blocks ───────────────────────────────
        await output_manager.invalidate_blocks(workflow.id, "catastrophic recovery")

        # All Rucio rules deleted
        delete_calls = [c for c in tracking_rucio.calls if c[0] == "delete_rule"]
        deleted_ids = {c[1][0] for c in delete_calls}
        assert source_rule_0 in deleted_ids
        assert tape_rule_1 in deleted_ids
        # Block 1 had 2 source rules + 1 tape = 3 deletes for block 1
        # Block 0 had 1 source rule = 1 delete for block 0
        # Total: 4 delete_rule calls
        assert len(delete_calls) == 4

        # DBS blocks invalidated
        dbs_inv = [c for c in mock_dbs.calls if c[0] == "invalidate_block"]
        assert len(dbs_inv) == 2  # both blocks had dbs_block_open=True

        # All blocks → INVALIDATED
        for bid in (block_0.id, block_1.id):
            refreshed = await repo.get_processing_block(bid)
            assert refreshed.status == BlockStatus.INVALIDATED.value
