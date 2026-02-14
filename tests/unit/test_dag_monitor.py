"""Unit tests for DAGMonitor."""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter
from wms2.core.dag_monitor import DAGMonitor, DAGPollResult, NodeSummary
from wms2.models.enums import DAGStatus


def _make_dag_row(
    status="submitted",
    dag_file_path="/tmp/submit/workflow.dag",
    submit_dir="/tmp/submit",
    total_work_units=2,
    completed_work_units=None,
):
    dag = MagicMock()
    dag.id = uuid.uuid4()
    dag.workflow_id = uuid.uuid4()
    dag.dag_file_path = dag_file_path
    dag.submit_dir = submit_dir
    dag.dagman_cluster_id = "12346"
    dag.schedd_name = "schedd.example.com"
    dag.status = status
    dag.total_work_units = total_work_units
    dag.completed_work_units = completed_work_units or []
    dag.total_nodes = 10
    return dag


@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


@pytest.fixture
def mock_condor():
    return MockCondorAdapter()


@pytest.fixture
def monitor(mock_repo, mock_condor):
    return DAGMonitor(mock_repo, mock_condor)


class TestParseDagmanStatus:
    def test_parse_valid_status(self, monitor, tmp_path):
        status_data = {
            "nodes": {
                "mg_000000": {"status": "Done"},
                "mg_000001": {"status": "Running"},
                "mg_000002": {"status": "Idle"},
            }
        }
        status_file = tmp_path / "workflow.dag.status"
        status_file.write_text(json.dumps(status_data))

        result = monitor._parse_dagman_status(str(status_file))
        assert result.done == 1
        assert result.running == 1
        assert result.idle == 1
        assert result.failed == 0
        assert result.node_statuses["mg_000000"] == "done"

    def test_parse_missing_file(self, monitor):
        result = monitor._parse_dagman_status("/nonexistent/path.status")
        assert result.done == 0
        assert result.running == 0
        assert result.idle == 0

    def test_parse_failed_nodes(self, monitor, tmp_path):
        status_data = {
            "nodes": {
                "mg_000000": {"status": "Error"},
                "mg_000001": {"status": "Done"},
            }
        }
        status_file = tmp_path / "workflow.dag.status"
        status_file.write_text(json.dumps(status_data))

        result = monitor._parse_dagman_status(str(status_file))
        assert result.done == 1
        assert result.failed == 1

    def test_parse_held_nodes(self, monitor, tmp_path):
        status_data = {
            "nodes": {
                "mg_000000": {"status": "Held"},
            }
        }
        status_file = tmp_path / "workflow.dag.status"
        status_file.write_text(json.dumps(status_data))

        result = monitor._parse_dagman_status(str(status_file))
        assert result.held == 1


class TestParseDagmanMetrics:
    def test_parse_valid_metrics(self, monitor, tmp_path):
        metrics_data = {
            "nodes_done": 10,
            "nodes_failed": 2,
            "nodes_running": 0,
            "nodes_idle": 0,
            "nodes_held": 0,
        }
        metrics_file = tmp_path / "workflow.dag.metrics"
        metrics_file.write_text(json.dumps(metrics_data))

        result = monitor._parse_dagman_metrics(str(metrics_file))
        assert result.done == 10
        assert result.failed == 2

    def test_parse_missing_metrics(self, monitor):
        result = monitor._parse_dagman_metrics("/nonexistent/metrics")
        assert result.done == 0
        assert result.failed == 0


class TestDetectCompletedWorkUnits:
    def test_new_completed_detected(self, monitor):
        dag = _make_dag_row(completed_work_units=[])
        summary = NodeSummary(
            done=1,
            node_statuses={"mg_000000": "done", "mg_000001": "running"},
        )
        result = monitor._detect_completed_work_units(dag, summary)
        assert len(result) == 1
        assert result[0]["group_name"] == "mg_000000"

    def test_already_reported_not_reemitted(self, monitor):
        dag = _make_dag_row(completed_work_units=["mg_000000"])
        summary = NodeSummary(
            done=1,
            node_statuses={"mg_000000": "done", "mg_000001": "running"},
        )
        result = monitor._detect_completed_work_units(dag, summary)
        assert len(result) == 0

    def test_non_mg_nodes_ignored(self, monitor):
        dag = _make_dag_row(completed_work_units=[])
        summary = NodeSummary(
            done=1,
            node_statuses={"proc_000000": "done"},
        )
        result = monitor._detect_completed_work_units(dag, summary)
        assert len(result) == 0


class TestPollDag:
    async def test_dag_alive_returns_running(self, monitor, mock_condor, mock_repo, tmp_path):
        dag = _make_dag_row(dag_file_path=str(tmp_path / "workflow.dag"))
        # Write a status file
        status_data = {
            "nodes": {
                "mg_000000": {"status": "Done"},
                "mg_000001": {"status": "Running"},
            }
        }
        status_file = tmp_path / "workflow.dag.status"
        status_file.write_text(json.dumps(status_data))

        result = await monitor.poll_dag(dag)
        assert result.status == DAGStatus.RUNNING
        assert result.nodes_done == 1
        assert result.nodes_running == 1
        mock_repo.update_dag.assert_called_once()

    async def test_dag_gone_no_failures_returns_completed(
        self, monitor, mock_condor, mock_repo, tmp_path
    ):
        dag = _make_dag_row(dag_file_path=str(tmp_path / "workflow.dag"))
        # DAGMan gone from queue
        mock_condor.completed_jobs.add(dag.dagman_cluster_id)

        # Write metrics file
        metrics_data = {"nodes_done": 10, "nodes_failed": 0}
        metrics_file = tmp_path / "workflow.dag.metrics"
        metrics_file.write_text(json.dumps(metrics_data))

        result = await monitor.poll_dag(dag)
        assert result.status == DAGStatus.COMPLETED
        assert result.nodes_done == 10

    async def test_dag_gone_some_failures_returns_partial(
        self, monitor, mock_condor, mock_repo, tmp_path
    ):
        dag = _make_dag_row(dag_file_path=str(tmp_path / "workflow.dag"))
        mock_condor.completed_jobs.add(dag.dagman_cluster_id)

        metrics_data = {"nodes_done": 8, "nodes_failed": 2}
        metrics_file = tmp_path / "workflow.dag.metrics"
        metrics_file.write_text(json.dumps(metrics_data))

        result = await monitor.poll_dag(dag)
        assert result.status == DAGStatus.PARTIAL

    async def test_dag_gone_all_failures_returns_failed(
        self, monitor, mock_condor, mock_repo, tmp_path
    ):
        dag = _make_dag_row(dag_file_path=str(tmp_path / "workflow.dag"))
        mock_condor.completed_jobs.add(dag.dagman_cluster_id)

        metrics_data = {"nodes_done": 0, "nodes_failed": 5}
        metrics_file = tmp_path / "workflow.dag.metrics"
        metrics_file.write_text(json.dumps(metrics_data))

        result = await monitor.poll_dag(dag)
        assert result.status == DAGStatus.FAILED

    async def test_newly_completed_work_units_added(
        self, monitor, mock_condor, mock_repo, tmp_path
    ):
        dag = _make_dag_row(
            dag_file_path=str(tmp_path / "workflow.dag"),
            completed_work_units=[],
        )
        status_data = {
            "nodes": {
                "mg_000000": {"status": "Done"},
                "mg_000001": {"status": "Running"},
            }
        }
        status_file = tmp_path / "workflow.dag.status"
        status_file.write_text(json.dumps(status_data))

        result = await monitor.poll_dag(dag)
        assert len(result.newly_completed_work_units) == 1
        assert result.newly_completed_work_units[0]["group_name"] == "mg_000000"

        # Verify update_dag was called with completed_work_units
        call_kwargs = mock_repo.update_dag.call_args[1]
        assert "mg_000000" in call_kwargs["completed_work_units"]


class TestHandleDagCompletion:
    async def test_reads_metrics_file(self, monitor, mock_repo, tmp_path):
        dag = _make_dag_row(dag_file_path=str(tmp_path / "workflow.dag"))
        # Make DAGMan gone
        monitor.condor = MockCondorAdapter()
        monitor.condor.completed_jobs.add(dag.dagman_cluster_id)

        metrics_data = {"nodes_done": 5, "nodes_failed": 0}
        metrics_file = tmp_path / "workflow.dag.metrics"
        metrics_file.write_text(json.dumps(metrics_data))

        result = await monitor._handle_dag_completion(dag)
        assert result.status == DAGStatus.COMPLETED
        assert result.nodes_done == 5


class TestLifecycleIntegration:
    async def test_handle_active_calls_poll_dag(self, mock_repo, tmp_path):
        """Lifecycle _handle_active calls dag_monitor.poll_dag when DAG is SUBMITTED."""
        from wms2.config import Settings
        from wms2.core.lifecycle_manager import RequestLifecycleManager

        condor = MockCondorAdapter()
        settings = Settings(
            database_url="postgresql+asyncpg://test:test@localhost:5433/test",
            lifecycle_cycle_interval=1,
        )

        dag = _make_dag_row(
            status="submitted",
            dag_file_path=str(tmp_path / "workflow.dag"),
        )

        # Write a status file showing running state
        status_data = {"nodes": {"mg_000000": {"status": "Running"}}}
        (tmp_path / "workflow.dag.status").write_text(json.dumps(status_data))

        workflow = MagicMock()
        workflow.id = dag.workflow_id
        workflow.dag_id = dag.id

        mock_repo.get_workflow_by_request = AsyncMock(return_value=workflow)
        mock_repo.get_dag = AsyncMock(return_value=dag)

        dm = DAGMonitor(mock_repo, condor)
        lm = RequestLifecycleManager(
            mock_repo, condor, settings, dag_monitor=dm,
        )

        from tests.unit.conftest import make_request_row
        request = make_request_row(status="active")
        await lm.evaluate_request(request)

        # DAG should be updated to RUNNING
        mock_repo.update_dag.assert_called()
        call_kwargs = mock_repo.update_dag.call_args[1]
        assert call_kwargs["status"] == "running"
