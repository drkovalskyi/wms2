"""Unit tests for DAG Planner offset consumption and adaptive capping."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter, MockDBSAdapter, MockRucioAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner


def _make_settings(tmp_path, **overrides):
    defaults = dict(
        database_url="postgresql+asyncpg://test:test@localhost:5433/test",
        submit_base_dir=str(tmp_path),
        jobs_per_work_unit=8,
        work_units_per_round=10,
    )
    defaults.update(overrides)
    return Settings(**defaults)


def _make_workflow(**kwargs):
    wf = MagicMock()
    wf.id = kwargs.pop("workflow_id", uuid.uuid4())
    wf.request_name = kwargs.pop("request_name", "test-request-001")
    wf.input_dataset = kwargs.pop("input_dataset", "/TestPrimary/TestProcessed/RECO")
    wf.splitting_algo = kwargs.pop("splitting_algo", "FileBased")
    wf.splitting_params = kwargs.pop("splitting_params", {"files_per_job": 2})
    wf.sandbox_url = kwargs.pop("sandbox_url", "https://example.com/sandbox.tar.gz")
    wf.category_throttles = kwargs.pop("category_throttles", None)
    wf.config_data = kwargs.pop("config_data", {})
    wf.pilot_output_path = kwargs.pop("pilot_output_path", None)
    wf.pilot_cluster_id = kwargs.pop("pilot_cluster_id", None)
    wf.next_first_event = kwargs.pop("next_first_event", 1)
    wf.file_offset = kwargs.pop("file_offset", 0)
    wf.current_round = kwargs.pop("current_round", 0)
    for k, v in kwargs.items():
        setattr(wf, k, v)
    return wf


def _make_planner(tmp_path, **settings_overrides):
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    repo.create_processing_block = AsyncMock()

    settings = _make_settings(tmp_path, **settings_overrides)
    planner = DAGPlanner(
        repository=repo,
        dbs_adapter=MockDBSAdapter(),
        rucio_adapter=MockRucioAdapter(),
        condor_adapter=MockCondorAdapter(),
        settings=settings,
    )
    return planner


# ── GEN node offset tests ────────────────────────────────────────


class TestGenNodesOffset:
    def test_gen_nodes_uses_next_first_event(self, tmp_path):
        """next_first_event=500_001 → nodes start at event 500,001."""
        planner = _make_planner(tmp_path)
        wf = _make_workflow(
            config_data={"_is_gen": True, "request_num_events": 1_000_000},
            splitting_params={"events_per_job": 100_000},
            next_first_event=500_001,
        )

        nodes = planner._plan_gen_nodes(wf, wf.config_data)

        # 500,000 remaining events / 100,000 per job = 5 nodes
        assert len(nodes) == 5
        assert nodes[0].first_event == 500_001
        assert nodes[0].last_event == 600_000
        assert nodes[-1].first_event == 900_001
        assert nodes[-1].last_event == 1_000_000

    def test_gen_nodes_max_jobs_caps(self, tmp_path):
        """max_jobs=6, 10 remaining jobs → 6 nodes."""
        planner = _make_planner(tmp_path)
        wf = _make_workflow(
            config_data={"_is_gen": True, "request_num_events": 1_000_000},
            splitting_params={"events_per_job": 100_000},
            next_first_event=1,
        )

        nodes = planner._plan_gen_nodes(wf, wf.config_data, max_jobs=6)

        assert len(nodes) == 6
        assert nodes[0].first_event == 1
        assert nodes[-1].first_event == 500_001
        assert nodes[-1].last_event == 600_000

    def test_gen_nodes_all_done_returns_empty(self, tmp_path):
        """next_first_event past total → empty list."""
        planner = _make_planner(tmp_path)
        wf = _make_workflow(
            config_data={"_is_gen": True, "request_num_events": 1_000_000},
            splitting_params={"events_per_job": 100_000},
            next_first_event=1_000_001,
        )

        nodes = planner._plan_gen_nodes(wf, wf.config_data)

        assert nodes == []


# ── File-based node offset tests ─────────────────────────────────


class TestFileBasedNodesOffset:
    async def test_file_based_skips_offset(self, tmp_path):
        """file_offset=2, 5 files from DBS → only 3 files split."""
        planner = _make_planner(tmp_path)

        # MockDBSAdapter returns 5 files by default
        wf = _make_workflow(
            splitting_algo="FileBased",
            splitting_params={"files_per_job": 1},
            file_offset=2,
        )

        nodes = await planner._plan_file_based_nodes(wf)

        # 5 files - 2 offset = 3 files, 1 per job = 3 nodes
        assert len(nodes) == 3

    async def test_file_based_max_jobs_caps(self, tmp_path):
        """max_jobs=3 with files_per_job=1 → 3 files processed."""
        planner = _make_planner(tmp_path)

        wf = _make_workflow(
            splitting_algo="FileBased",
            splitting_params={"files_per_job": 1},
            file_offset=0,
        )

        nodes = await planner._plan_file_based_nodes(wf, max_jobs=3)

        assert len(nodes) == 3
