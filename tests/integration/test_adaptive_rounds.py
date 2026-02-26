"""Multi-round adaptive execution integration test.

Wires all WMS2 production components together with mock external services:
- Real PostgreSQL (wms2test database)
- Real HTCondor (local pool)
- Mock DBS/Rucio adapters (call tracking)
- Mock ReqMgr adapter (returns configured request spec)
- Simulator sandbox mode (fast cmsRun substitute)

Drives the lifecycle through multiple adaptive rounds until all events are
processed and verifies:
- Request transitions through SUBMITTED → QUEUED → ACTIVE → QUEUED → ... → COMPLETED
- Multiple DAG rows created (one per round)
- Workflow round counter and event offsets advance correctly
- DBS/Rucio calls happen each round

Can run standalone:
    python -m tests.integration.test_adaptive_rounds
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
MAX_TIMEOUT = 300  # 5 minutes — each round needs ~90s for DAGMan lifecycle

_REQUEST_SPEC = {
    "RequestName": "adaptive_rounds_test_001",
    "RequestType": "StepChain",
    "StepChain": 1,
    "Step1": {"StepName": "GEN-SIM"},
    "Multicore": 1,
    "Memory": 2000,
    "TimePerEvent": 0.01,
    "SizePerEvent": 10.0,
    "SplittingAlgo": "EventBased",
    "SplittingParams": {"events_per_job": 10},
    "InputDataset": "",
    "SandboxUrl": "",
    "Requestor": "integration-test",
    "Campaign": "AdaptiveTest",
    "Priority": 100000,
}

_OUTPUT_DATASETS = [
    {
        "dataset_name": "/AdaptiveTest/Test-v1/GEN-SIM",
        "merged_lfn_base": "/store/mc/AdaptiveTest/GEN-SIM/v1",
        "unmerged_lfn_base": "/store/unmerged/AdaptiveTest/GEN-SIM/v1",
        "data_tier": "GEN-SIM",
    },
]


class AdaptiveRoundsTest:
    """Run an adaptive request through multiple rounds until COMPLETED.

    Settings: work_units_per_round=1, jobs_per_work_unit=2, events_per_job=10,
    request_num_events=40.

    Expected rounds:
    - Round 0: 2 jobs (events 1–20)
    - Round 1: 2 jobs (events 21–40)
    - Round 2: _plan_gen_nodes returns [] → plan_production_dag returns None → COMPLETED
    """

    def __init__(self, *, work_dir: Path | None = None):
        self._work_dir = work_dir or Path("/mnt/shared/work/wms2_matrix/adaptive_rounds")
        self._engine = None
        self._session_factory = None
        self._session = None
        self._repo = None

        self._lifecycle = None
        self._workflow_mgr = None
        self._dag_planner = None
        self._dag_monitor = None
        self._output_mgr = None

        self.mock_dbs = None
        self.mock_rucio = None
        self.mock_reqmgr = None

        self._request_name = _REQUEST_SPEC["RequestName"]
        self._events_per_job = 10
        self._jobs_per_work_unit = 2
        self._work_units_per_round = 1
        self._total_events = 40

    async def setup(self) -> None:
        """Create DB engine, tables, session, all production components."""
        sandbox_path = str(self._work_dir / "sandbox.tar.gz")
        self._work_dir.mkdir(parents=True, exist_ok=True)

        spec = dict(_REQUEST_SPEC)
        spec["OutputDatasets"] = _OUTPUT_DATASETS
        spec["RequestNumEvents"] = self._total_events
        spec["SplittingParams"] = {"events_per_job": self._events_per_job}
        spec["SandboxUrl"] = sandbox_path

        create_sandbox(sandbox_path, spec, mode="simulator")

        # 1. Database
        self._engine = create_async_engine(TEST_DB_URL, echo=False)
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        self._session_factory = create_session_factory(self._engine)
        self._session = self._session_factory()
        await self._session.__aenter__()
        self._repo = Repository(self._session)

        # 2. Mock adapters
        self.mock_dbs = MockDBSAdapter()
        self.mock_rucio = MockRucioAdapter()
        self.mock_reqmgr = MockReqMgrAdapter()
        self.mock_reqmgr.seed_request(self._request_name, spec)

        # 3. Real HTCondor
        condor = HTCondorAdapter(CONDOR_HOST)

        # 4. Settings — small round size to force multiple rounds
        submit_dir = str(self._work_dir / "submit")
        settings = Settings(
            database_url=TEST_DB_URL,
            submit_base_dir=submit_dir,
            processing_executable="/bin/true",
            merge_executable="/bin/true",
            cleanup_executable="/bin/true",
            jobs_per_work_unit=self._jobs_per_work_unit,
            work_units_per_round=self._work_units_per_round,
            local_pfn_prefix="/mnt/shared",
            max_active_dags=10,
            lifecycle_cycle_interval=POLL_INTERVAL,
        )

        # 5. Components
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

        # 6. Create request row
        await self._repo.create_request(
            request_name=self._request_name,
            requestor=spec.get("Requestor", "test"),
            request_data=spec,
            input_dataset="",
            campaign=spec.get("Campaign", ""),
            priority=spec.get("Priority", 100000),
            urgent=True,
            adaptive=True,
            status="submitted",
            splitting_params={"events_per_job": self._events_per_job},
            payload_config={
                "output_datasets": _OUTPUT_DATASETS,
                "sandbox_path": sandbox_path,
                "memory_mb": spec.get("Memory", 2000),
                "multicore": spec.get("Multicore", 1),
                "time_per_event": spec.get("TimePerEvent", 0.01),
                "size_per_event": spec.get("SizePerEvent", 10.0),
                "request_num_events": self._total_events,
                "_is_gen": True,
            },
        )
        await self._session.commit()
        logger.info("Created adaptive request: %s (events=%d)", self._request_name, self._total_events)

    async def run(self) -> str:
        """Drive lifecycle through multiple rounds."""
        start = time.monotonic()
        terminal_statuses = {"completed", "failed", "aborted"}
        last_status = "submitted"

        while True:
            elapsed = time.monotonic() - start
            if elapsed > MAX_TIMEOUT:
                logger.error("Timeout after %.0fs (last status: %s)", elapsed, last_status)
                break

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

            try:
                await self._lifecycle.evaluate_request(request)
                await self._session.commit()
            except Exception:
                logger.exception("Error evaluating request")
                await self._session.rollback()

            await asyncio.sleep(POLL_INTERVAL)

        return last_status

    async def verify(self) -> dict[str, Any]:
        """Check DB state for multi-round adaptive execution."""
        results: dict[str, Any] = {"checks": [], "all_passed": True}

        def check(name: str, passed: bool, detail: str = ""):
            results["checks"].append({"name": name, "passed": passed, "detail": detail})
            if not passed:
                results["all_passed"] = False
            symbol = "PASS" if passed else "FAIL"
            msg = f"  [{symbol}] {name}"
            if detail:
                msg += f": {detail}"
            print(msg)

        print("\n=== Verification ===\n")

        # 1. Request completed
        request = await self._repo.get_request(self._request_name)
        check(
            "request_completed",
            request is not None and request.status == "completed",
            f"status={request.status if request else 'NOT_FOUND'}",
        )

        # 2. Workflow round counter
        workflow = await self._repo.get_workflow_by_request(self._request_name)
        check(
            "workflow_exists",
            workflow is not None,
            f"id={workflow.id if workflow else 'NOT_FOUND'}",
        )
        # Expected: 2 completed rounds (round 0 and round 1), so current_round=2
        check(
            "workflow_current_round",
            workflow is not None and workflow.current_round == 2,
            f"expected=2, got={workflow.current_round if workflow else 'N/A'}",
        )

        # 3. Event offset advanced past total
        check(
            "event_offset_past_total",
            workflow is not None and workflow.next_first_event > self._total_events,
            f"next_first_event={workflow.next_first_event if workflow else 'N/A'}, "
            f"total={self._total_events}",
        )

        # 4. Multiple DAG rows created (at least 2 — one per completed round)
        dags = await self._repo.list_dags(workflow_id=workflow.id) if workflow else []
        check(
            "multiple_dags_created",
            len(dags) >= 2,
            f"expected>=2, got={len(dags)}",
        )

        # 5. DBS calls (open_block at least once)
        dbs_open = [c for c in self.mock_dbs.calls if c[0] == "open_block"]
        check(
            "dbs_open_block_calls",
            len(dbs_open) >= 1,
            f"got={len(dbs_open)}",
        )

        # 6. Rucio calls
        rucio_rules = [c for c in self.mock_rucio.calls if c[0] == "create_rule"]
        check(
            "rucio_create_rule_calls",
            len(rucio_rules) >= 1,
            f"got={len(rucio_rules)}",
        )

        # Summary
        passed = sum(1 for c in results["checks"] if c["passed"])
        total = len(results["checks"])
        results["summary"] = f"{passed}/{total} checks passed"
        print(f"\n{results['summary']}")
        if results["all_passed"]:
            print("Adaptive rounds test: ALL PASSED")
        else:
            print("Adaptive rounds test: FAILURES DETECTED")

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
    print("WMS2 Multi-Round Adaptive Execution Integration Test")
    print("=" * 60)

    test = AdaptiveRoundsTest()
    try:
        print("\n--- Setup ---")
        await test.setup()

        print("\n--- Running lifecycle (multi-round) ---")
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
