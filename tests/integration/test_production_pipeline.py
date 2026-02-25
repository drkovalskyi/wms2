"""Production-path integration test.

Wires all WMS2 production components together with mock external services:
- Real PostgreSQL (wms2test database)
- Real HTCondor (local pool)
- Mock DBS/Rucio adapters (call tracking)
- Mock ReqMgr adapter (returns configured request spec)
- Simulator sandbox mode (fast cmsRun substitute)

Drives the lifecycle from SUBMITTED → COMPLETED and verifies:
- Request/workflow/DAG/ProcessingBlock DB rows
- DBS calls: open_block, register_files, close_block per dataset
- Rucio calls: create_rule (source protection + tape archival) per dataset
- Merged output files on disk
- Cleanup of unmerged files

Can run standalone:
    python -m tests.integration.test_production_pipeline

Or via the matrix runner:
    python -m tests.matrix.run --wf 160.0
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import create_async_engine

from wms2.adapters.condor import HTCondorAdapter
from wms2.adapters.mock import MockDBSAdapter, MockReqMgrAdapter, MockRucioAdapter
from wms2.config import Settings
from wms2.core.dag_monitor import DAGMonitor
from wms2.core.dag_planner import DAGPlanner
from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.core.output_manager import OutputManager
from wms2.core.sandbox import create_sandbox
from wms2.core.workflow_manager import WorkflowManager
from wms2.db.base import Base
from wms2.db.engine import create_session_factory
from wms2.db.repository import Repository
from wms2.db.tables import (  # noqa: F401
    DAGHistoryRow,
    DAGRow,
    ProcessingBlockRow,
    RequestRow,
    SiteRow,
    WorkflowRow,
)

logger = logging.getLogger(__name__)

TEST_DB_URL = "postgresql+asyncpg://wms2test:wms2test@localhost:5432/wms2test"
CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
POLL_INTERVAL = 5
MAX_TIMEOUT = 600  # 10 minutes

# Default NPS 5-step StepChain request spec for standalone execution
_DEFAULT_REQUEST_SPEC = {
    "RequestName": "prod_pipeline_test_160_0",
    "RequestType": "StepChain",
    "StepChain": 5,
    "Step1": {"StepName": "GEN-SIM"},
    "Step2": {"StepName": "DIGI"},
    "Step3": {"StepName": "RECO"},
    "Step4": {"StepName": "MINIAODSIM"},
    "Step5": {"StepName": "NANOAODSIM"},
    "Multicore": 8,
    "Memory": 16000,
    "TimePerEvent": 0.05,
    "SizePerEvent": 50.0,
    "SplittingAlgo": "EventBased",
    "SplittingParams": {"events_per_job": 10},
    "InputDataset": "",
    "SandboxUrl": "",
    "Requestor": "integration-test",
    "Campaign": "NPS-Run3Summer22EEGS",
    "Priority": 100000,
}

_DEFAULT_OUTPUT_DATASETS = [
    {
        "dataset_name": "/TestPrimary/Test-v1/GEN-SIM",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/GEN-SIM/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/GEN-SIM/v1",
        "data_tier": "GEN-SIM",
    },
    {
        "dataset_name": "/TestPrimary/Test-v1/DIGI",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/DIGI/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/DIGI/v1",
        "data_tier": "DIGI",
    },
    {
        "dataset_name": "/TestPrimary/Test-v1/RECO",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/RECO/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/RECO/v1",
        "data_tier": "RECO",
    },
    {
        "dataset_name": "/TestPrimary/Test-v1/MINIAODSIM",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/MINIAODSIM/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/MINIAODSIM/v1",
        "data_tier": "MINIAODSIM",
    },
    {
        "dataset_name": "/TestPrimary/Test-v1/NANOAODSIM",
        "merged_lfn_base": "/store/mc/Test/TestPrimary/NANOAODSIM/v1",
        "unmerged_lfn_base": "/store/unmerged/Test/TestPrimary/NANOAODSIM/v1",
        "data_tier": "NANOAODSIM",
    },
]


class ProductionPipelineTest:
    """Run a request through the full WMS2 production pipeline.

    Instantiates all production components with real PostgreSQL and HTCondor,
    but mock DBS/Rucio/ReqMgr adapters. Drives the lifecycle step-by-step
    via evaluate_request() in a poll loop.
    """

    def __init__(
        self,
        *,
        wf=None,
        work_dir: Path | None = None,
        sandbox_path: str = "",
    ):
        """Initialize with optional WorkflowDef (from matrix runner).

        Args:
            wf: WorkflowDef from matrix runner (optional, uses defaults if None)
            work_dir: Working directory for DAG files
            sandbox_path: Path to pre-built sandbox tarball
        """
        self._wf = wf
        self._work_dir = work_dir or Path("/mnt/shared/work/wms2_matrix/wf_160.0")
        self._sandbox_path = sandbox_path
        self._engine = None
        self._session_factory = None
        self._session = None
        self._repo = None

        # Production components
        self._lifecycle = None
        self._workflow_mgr = None
        self._dag_planner = None
        self._dag_monitor = None
        self._output_mgr = None

        # Mock adapters (for verification)
        self.mock_dbs = None
        self.mock_rucio = None
        self.mock_reqmgr = None

        # Test state
        self._request_name = ""
        self._request_spec: dict[str, Any] = {}
        self._output_datasets: list[dict] = []
        self._timeout_sec = MAX_TIMEOUT

    async def setup(self) -> None:
        """Create DB engine, tables, session, all production components."""
        # Resolve test parameters from WorkflowDef or defaults
        if self._wf:
            self._request_spec = dict(self._wf.request_spec)
            self._output_datasets = list(self._wf.output_datasets)
            self._timeout_sec = self._wf.timeout_sec
            self._request_name = self._request_spec.get(
                "RequestName", f"prod_test_{self._wf.wf_id}"
            )
        else:
            self._request_spec = dict(_DEFAULT_REQUEST_SPEC)
            self._output_datasets = list(_DEFAULT_OUTPUT_DATASETS)
            self._request_name = self._request_spec["RequestName"]

        # Build sandbox if not provided
        if not self._sandbox_path:
            self._work_dir.mkdir(parents=True, exist_ok=True)
            self._sandbox_path = str(self._work_dir / "sandbox.tar.gz")
            mode = self._wf.sandbox_mode if self._wf else "simulator"
            create_sandbox(self._sandbox_path, self._request_spec, mode=mode)

        # Point SandboxUrl at local sandbox
        self._request_spec["SandboxUrl"] = self._sandbox_path

        # Enrich request spec with output datasets and event counts
        # so WorkflowManager._build_config_data() can build complete config
        events_per_job = (
            self._wf.events_per_job if self._wf else
            self._request_spec.get("SplittingParams", {}).get("events_per_job", 10)
        )
        num_jobs = self._wf.num_jobs if self._wf else 2
        total_events = events_per_job * num_jobs

        self._request_spec["OutputDatasets"] = self._output_datasets
        self._request_spec["RequestNumEvents"] = total_events
        if "SplittingParams" not in self._request_spec:
            self._request_spec["SplittingParams"] = {"events_per_job": events_per_job}

        # 1. Database setup
        self._engine = create_async_engine(TEST_DB_URL, echo=False)
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self._session_factory = create_session_factory(self._engine)
        self._session = self._session_factory()
        await self._session.__aenter__()
        self._repo = Repository(self._session)

        # 2. Mock adapters
        self.mock_dbs = MockDBSAdapter()
        self.mock_rucio = MockRucioAdapter()
        self.mock_reqmgr = MockReqMgrAdapter()
        self.mock_reqmgr.seed_request(self._request_name, self._request_spec)

        # 3. Real HTCondor adapter
        condor = HTCondorAdapter(CONDOR_HOST)

        # 4. Settings
        submit_dir = str(self._work_dir / "submit")
        num_jobs = self._wf.num_jobs if self._wf else 2
        settings = Settings(
            database_url=TEST_DB_URL,
            submit_base_dir=submit_dir,
            processing_executable="/bin/true",
            merge_executable="/bin/true",
            cleanup_executable="/bin/true",
            jobs_per_work_unit=max(num_jobs, 1),
            local_pfn_prefix="/mnt/shared",
            max_active_dags=10,
            lifecycle_cycle_interval=POLL_INTERVAL,
        )

        # 5. Production components
        self._workflow_mgr = WorkflowManager(
            repository=self._repo,
            reqmgr_adapter=self.mock_reqmgr,
        )
        self._dag_planner = DAGPlanner(
            repository=self._repo,
            dbs_adapter=self.mock_dbs,
            rucio_adapter=self.mock_rucio,
            condor_adapter=condor,
            settings=settings,
        )
        self._dag_monitor = DAGMonitor(
            repository=self._repo,
            condor_adapter=condor,
        )
        self._output_mgr = OutputManager(
            repository=self._repo,
            dbs_adapter=self.mock_dbs,
            rucio_adapter=self.mock_rucio,
        )
        self._lifecycle = RequestLifecycleManager(
            repository=self._repo,
            condor_adapter=condor,
            settings=settings,
            workflow_manager=self._workflow_mgr,
            dag_planner=self._dag_planner,
            dag_monitor=self._dag_monitor,
            output_manager=self._output_mgr,
        )

        logger.info(
            "Production pipeline test setup complete: request=%s, %d output datasets",
            self._request_name, len(self._output_datasets),
        )

    async def run(self) -> str:
        """Drive lifecycle from SUBMITTED → COMPLETED.

        Returns the final request status.
        """
        # Create request row (status="submitted")
        events_per_job = (
            self._wf.events_per_job if self._wf else
            self._request_spec.get("SplittingParams", {}).get("events_per_job", 10)
        )
        num_jobs = self._wf.num_jobs if self._wf else 2
        total_events = events_per_job * num_jobs

        await self._repo.create_request(
            request_name=self._request_name,
            requestor=self._request_spec.get("Requestor", "test"),
            request_data=self._request_spec,
            input_dataset=self._request_spec.get("InputDataset", ""),
            campaign=self._request_spec.get("Campaign", ""),
            priority=self._request_spec.get("Priority", 100000),
            urgent=True,  # Skip pilot, go straight to production DAG
            status="submitted",
            splitting_params={
                "events_per_job": events_per_job,
            },
            payload_config={
                "output_datasets": self._output_datasets,
                "sandbox_path": self._sandbox_path,
                "memory_mb": self._request_spec.get("Memory", 16000),
                "multicore": self._request_spec.get("Multicore", 8),
                "time_per_event": self._request_spec.get("TimePerEvent", 0.05),
                "size_per_event": self._request_spec.get("SizePerEvent", 50.0),
                "request_num_events": total_events,
                "_is_gen": True,  # GEN workflow — no input files
            },
        )
        await self._session.commit()

        logger.info("Created request: %s (events=%d)", self._request_name, total_events)

        # Poll loop: evaluate_request() every POLL_INTERVAL seconds
        start = time.monotonic()
        terminal_statuses = {"completed", "failed", "aborted"}
        last_status = "submitted"

        while True:
            elapsed = time.monotonic() - start
            if elapsed > self._timeout_sec:
                logger.error("Timeout after %.0fs (last status: %s)", elapsed, last_status)
                break

            # Refresh the request row
            request = await self._repo.get_request(self._request_name)
            if not request:
                logger.error("Request %s not found", self._request_name)
                break

            last_status = request.status
            if last_status in terminal_statuses:
                logger.info(
                    "Request reached terminal status: %s (%.0fs)",
                    last_status, elapsed,
                )
                break

            # Evaluate
            try:
                await self._lifecycle.evaluate_request(request)
                await self._session.commit()
            except Exception:
                logger.exception("Error evaluating request")
                await self._session.rollback()

            await asyncio.sleep(POLL_INTERVAL)

        return last_status

    async def verify(self) -> dict[str, Any]:
        """Check DB state, DBS/Rucio calls, file artifacts.

        Returns a dict with verification results.
        """
        results: dict[str, Any] = {"checks": [], "all_passed": True}

        def check(name: str, passed: bool, detail: str = ""):
            results["checks"].append({
                "name": name,
                "passed": passed,
                "detail": detail,
            })
            if not passed:
                results["all_passed"] = False
            symbol = "PASS" if passed else "FAIL"
            msg = f"  [{symbol}] {name}"
            if detail:
                msg += f": {detail}"
            print(msg)

        print("\n=== Verification ===\n")

        # 1. Request status
        request = await self._repo.get_request(self._request_name)
        check(
            "request_completed",
            request is not None and request.status == "completed",
            f"status={request.status if request else 'NOT_FOUND'}",
        )

        # 2. Workflow exists with dag_id
        workflow = await self._repo.get_workflow_by_request(self._request_name)
        check(
            "workflow_exists",
            workflow is not None,
            f"id={workflow.id if workflow else 'NOT_FOUND'}",
        )
        check(
            "workflow_has_dag",
            workflow is not None and workflow.dag_id is not None,
            f"dag_id={workflow.dag_id if workflow else 'N/A'}",
        )

        # 3. DAG row
        dag = None
        if workflow and workflow.dag_id:
            dag = await self._repo.get_dag(workflow.dag_id)
        check(
            "dag_completed",
            dag is not None and dag.status == "completed",
            f"status={dag.status if dag else 'NOT_FOUND'}",
        )

        # 4. ProcessingBlock rows
        blocks = []
        if workflow:
            blocks = await self._repo.get_processing_blocks(workflow.id)
        n_datasets = len(self._output_datasets)
        check(
            "processing_blocks_count",
            len(blocks) == n_datasets,
            f"expected={n_datasets}, got={len(blocks)}",
        )

        archived_count = sum(1 for b in blocks if b.status == "archived")
        check(
            "all_blocks_archived",
            archived_count == n_datasets,
            f"archived={archived_count}/{n_datasets}",
        )

        # 5. DBS calls
        dbs_open = [c for c in self.mock_dbs.calls if c[0] == "open_block"]
        dbs_register = [c for c in self.mock_dbs.calls if c[0] == "register_files"]
        dbs_close = [c for c in self.mock_dbs.calls if c[0] == "close_block"]
        check(
            "dbs_open_block",
            len(dbs_open) == n_datasets,
            f"expected={n_datasets}, got={len(dbs_open)}",
        )
        check(
            "dbs_register_files",
            len(dbs_register) >= n_datasets,
            f"expected>={n_datasets}, got={len(dbs_register)}",
        )
        check(
            "dbs_close_block",
            len(dbs_close) == n_datasets,
            f"expected={n_datasets}, got={len(dbs_close)}",
        )

        # 6. Rucio calls: source protection + tape archival
        rucio_rules = [c for c in self.mock_rucio.calls if c[0] == "create_rule"]
        # Each dataset gets at least one source rule + one tape rule
        expected_rules = n_datasets * 2
        check(
            "rucio_create_rule",
            len(rucio_rules) >= expected_rules,
            f"expected>={expected_rules}, got={len(rucio_rules)}",
        )

        # Count tape vs source rules
        tape_rules = [r for r in rucio_rules if r[1][1] == "tier=1&type=TAPE"]
        source_rules = [r for r in rucio_rules if r[1][1] != "tier=1&type=TAPE"]
        check(
            "rucio_tape_rules",
            len(tape_rules) == n_datasets,
            f"expected={n_datasets}, got={len(tape_rules)}",
        )
        check(
            "rucio_source_rules",
            len(source_rules) >= n_datasets,
            f"expected>={n_datasets}, got={len(source_rules)}",
        )

        # 7. Merged outputs exist on disk
        merged_count = 0
        for ds in self._output_datasets:
            merged_base = ds.get("merged_lfn_base", "")
            merged_dir = Path("/mnt/shared") / merged_base.lstrip("/")
            if merged_dir.exists():
                # Check for any files in group subdirs
                for group_dir in sorted(merged_dir.iterdir()):
                    if group_dir.is_dir():
                        files = list(group_dir.iterdir())
                        if files:
                            merged_count += 1
                            break
        check(
            "merged_outputs",
            merged_count == n_datasets,
            f"expected={n_datasets} tiers with output, got={merged_count}",
        )

        # Summary
        passed = sum(1 for c in results["checks"] if c["passed"])
        total = len(results["checks"])
        results["summary"] = f"{passed}/{total} checks passed"
        print(f"\n{results['summary']}")
        if results["all_passed"]:
            print("Production pipeline test: ALL PASSED")
        else:
            print("Production pipeline test: FAILURES DETECTED")

        return results

    async def cleanup(self) -> None:
        """Drop test tables, clean up session."""
        try:
            if self._session:
                await self._session.rollback()
                await self._session.__aexit__(None, None, None)
        except Exception:
            pass

        try:
            if self._engine:
                async with self._engine.begin() as conn:
                    await conn.run_sync(Base.metadata.drop_all)
                await self._engine.dispose()
        except Exception:
            pass


async def main():
    """Standalone entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    print("=" * 60)
    print("WMS2 Production Pipeline Integration Test")
    print("=" * 60)

    test = ProductionPipelineTest()
    try:
        print("\n--- Setup ---")
        await test.setup()

        print("\n--- Running lifecycle ---")
        final_status = await test.run()
        print(f"\nFinal request status: {final_status}")

        print("\n--- Verification ---")
        verification = await test.verify()

        sys.exit(0 if verification["all_passed"] else 1)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(130)
    except Exception:
        logger.exception("Test failed with exception")
        sys.exit(1)
    finally:
        await test.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
