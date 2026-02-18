"""Integration tests for HTCondor submission — requires a running HTCondor pool.

Run with: pytest tests/integration/test_condor_submit.py -v -m level2
Skip:     pytest -m "not level2"
"""

import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

pytestmark = [pytest.mark.level2, pytest.mark.condor]


@pytest.fixture
def condor_host():
    return os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")


@pytest.fixture
def adapter(condor_host):
    from wms2.adapters.condor import HTCondorAdapter
    return HTCondorAdapter(condor_host)


@pytest.fixture
def dag_planner_with_condor(tmp_path, adapter):
    from wms2.adapters.mock import MockDBSAdapter, MockRucioAdapter
    from wms2.config import Settings
    from wms2.core.dag_planner import DAGPlanner

    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()

    settings = Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
        submit_base_dir=str(tmp_path),
        jobs_per_work_unit=8,
    )

    return DAGPlanner(
        repository=repo,
        dbs_adapter=MockDBSAdapter(),
        rucio_adapter=MockRucioAdapter(),
        condor_adapter=adapter,
        settings=settings,
    )


class TestCondorSubmission:
    async def test_submit_and_query_trivial_dag(self, adapter, tmp_path):
        """Submit a trivial DAG, query it, then remove it."""
        # Create a minimal DAG
        sub_file = tmp_path / "true.sub"
        sub_file.write_text(
            "universe = vanilla\n"
            "executable = /bin/true\n"
            "output = true.out\n"
            "error = true.err\n"
            "log = true.log\n"
            "queue 1\n"
        )
        dag_file = tmp_path / "test.dag"
        dag_file.write_text(
            f"JOB A {sub_file}\n"
        )

        cluster_id, schedd = await adapter.submit_dag(str(dag_file))
        assert cluster_id.isdigit()
        assert schedd

        # Query the job
        info = await adapter.query_job(schedd, cluster_id)
        assert info is not None
        assert int(info["ClusterId"]) == int(cluster_id)

        # Remove the job
        await adapter.remove_job(schedd, cluster_id)

        # After removal, query should return None (eventually)
        info = await adapter.query_job(schedd, cluster_id)
        # May or may not be None immediately; just verify no exception

    async def test_dag_planner_generates_and_submits(
        self, dag_planner_with_condor, tmp_path
    ):
        """End-to-end: DAGPlanner generates files and submits to real HTCondor."""
        wf = MagicMock()
        wf.id = uuid.uuid4()
        wf.request_name = "condor-test-001"
        wf.input_dataset = "/TestPrimary/TestProcessed/RECO"
        wf.splitting_algo = "FileBased"
        wf.splitting_params = {"files_per_job": 2}
        wf.sandbox_url = "https://example.com/sandbox.tar.gz"
        wf.category_throttles = {"Processing": 5000, "Merge": 100, "Cleanup": 50}

        dag = await dag_planner_with_condor.plan_production_dag(wf)

        # Verify DAG files exist
        submit_dir = Path(tmp_path / str(wf.id))
        assert (submit_dir / "workflow.dag").exists()

        # Verify submit_dag was called (via update_dag with cluster info)
        dp = dag_planner_with_condor
        dp.db.update_dag.assert_called()
        call_kwargs = dp.db.update_dag.call_args[1]
        assert call_kwargs["dagman_cluster_id"]
        assert call_kwargs["status"] == "submitted"

        # Clean up — remove submitted DAG
        cluster_id = call_kwargs["dagman_cluster_id"]
        schedd = call_kwargs["schedd_name"]
        await dp.condor.remove_job(schedd, cluster_id)
