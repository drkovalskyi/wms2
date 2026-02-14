"""Integration tests for DAG Planner: end-to-end from InputFiles to DAG files on disk."""

import os
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter, MockDBSAdapter, MockRucioAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner


def _make_workflow(workflow_id=None):
    wf = MagicMock()
    wf.id = workflow_id or uuid.uuid4()
    wf.request_name = "test-request-001"
    wf.input_dataset = "/TestPrimary/TestProcessed/RECO"
    wf.splitting_algo = "FileBased"
    wf.splitting_params = {"files_per_job": 2}
    wf.sandbox_url = "https://example.com/sandbox.tar.gz"
    wf.category_throttles = {"Processing": 5000, "Merge": 100, "Cleanup": 50}
    wf.pilot_output_path = None
    wf.pilot_cluster_id = None
    return wf


@pytest.fixture
def settings(tmp_path):
    return Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
        submit_base_dir=str(tmp_path),
        target_merged_size_kb=4 * 1024 * 1024,
    )


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


@pytest.fixture
def dag_planner(mock_repo, settings):
    return DAGPlanner(
        repository=mock_repo,
        dbs_adapter=MockDBSAdapter(),
        rucio_adapter=MockRucioAdapter(),
        condor_adapter=MockCondorAdapter(),
        settings=settings,
    )


class TestDAGPlannerEndToEnd:
    async def test_plan_production_dag_creates_files(self, dag_planner, tmp_path):
        wf = _make_workflow()
        dag = await dag_planner.plan_production_dag(wf)

        # Verify DAG row was created
        dag_planner.db.create_dag.assert_called_once()
        call_kwargs = dag_planner.db.create_dag.call_args[1]
        assert call_kwargs["status"] == "ready"
        assert call_kwargs["total_nodes"] > 0

        # Verify files on disk
        submit_dir = Path(tmp_path / str(wf.id))
        assert (submit_dir / "workflow.dag").exists()
        assert (submit_dir / "dagman.config").exists()
        assert (submit_dir / "elect_site.sh").exists()

    async def test_plan_production_dag_structure(self, dag_planner, tmp_path):
        wf = _make_workflow()
        await dag_planner.plan_production_dag(wf)

        submit_dir = Path(tmp_path / str(wf.id))
        outer = (submit_dir / "workflow.dag").read_text()
        assert "SUBDAG EXTERNAL mg_000000" in outer
        assert "MAXJOBS MergeGroup 10" in outer

        # Check merge group sub-DAG
        group_dag = submit_dir / "mg_000000" / "group.dag"
        assert group_dag.exists()
        content = group_dag.read_text()
        assert "JOB landing" in content
        assert "JOB merge" in content
        assert "JOB cleanup" in content

    async def test_submit_pilot_creates_file(self, dag_planner, tmp_path):
        wf = _make_workflow()
        await dag_planner.submit_pilot(wf)

        pilot_dir = Path(tmp_path / str(wf.id) / "pilot")
        assert (pilot_dir / "pilot.sub").exists()
        content = (pilot_dir / "pilot.sub").read_text()
        assert "run_pilot.sh" in content
        assert "pilot_metrics.json" in content
