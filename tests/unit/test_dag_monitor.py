"""Unit tests for DAGMonitor."""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.adapters.mock import MockCondorAdapter
from wms2.core.dag_monitor import DAGMonitor, DAGPollResult, NodeSummary
from wms2.models.enums import DAGStatus


# ── ClassAd status file helpers ──────────────────────────────────


# NodeStatus integer codes used by DAGMan
_NS_NOT_READY = 0
_NS_READY = 1
_NS_PRERUN = 2
_NS_SUBMITTED = 3
_NS_POSTRUN = 4
_NS_DONE = 5
_NS_ERROR = 6
_NS_FUTILE = 7


def _write_classad_status(path, nodes: dict[str, int], **dag_overrides):
    """Write a DAGMan NODE_STATUS_FILE in ClassAd format.

    nodes: {"mg_000000": _NS_DONE, "mg_000001": _NS_SUBMITTED, ...}
    dag_overrides: override DagStatus block fields (NodesDone, etc.)
    """
    # Compute defaults from node statuses
    done = sum(1 for s in nodes.values() if s == _NS_DONE)
    failed = sum(1 for s in nodes.values() if s == _NS_ERROR)
    futile = sum(1 for s in nodes.values() if s == _NS_FUTILE)
    queued = sum(1 for s in nodes.values() if s == _NS_SUBMITTED)
    ready = sum(1 for s in nodes.values() if s == _NS_READY)
    unready = sum(1 for s in nodes.values() if s == _NS_NOT_READY)
    pre = sum(1 for s in nodes.values() if s == _NS_PRERUN)
    post = sum(1 for s in nodes.values() if s == _NS_POSTRUN)
    total = len(nodes)
    held = dag_overrides.pop("JobProcsHeld", 0)

    lines = [
        "[",
        '  Type = "DagStatus";',
        f"  NodesTotal = {dag_overrides.get('NodesTotal', total)};",
        f"  NodesDone = {dag_overrides.get('NodesDone', done)};",
        f"  NodesPre = {dag_overrides.get('NodesPre', pre)};",
        f"  NodesQueued = {dag_overrides.get('NodesQueued', queued)};",
        f"  NodesPost = {dag_overrides.get('NodesPost', post)};",
        f"  NodesReady = {dag_overrides.get('NodesReady', ready)};",
        f"  NodesUnready = {dag_overrides.get('NodesUnready', unready)};",
        f"  NodesFailed = {dag_overrides.get('NodesFailed', failed)};",
        f"  NodesFutile = {dag_overrides.get('NodesFutile', futile)};",
        f"  JobProcsHeld = {held};",
        "]",
    ]

    for name, status in nodes.items():
        lines += [
            "[",
            '  Type = "NodeStatus";',
            f'  Node = "{name}";',
            f"  NodeStatus = {status};",
            "]",
        ]

    lines += [
        "[",
        '  Type = "StatusEnd";',
        "]",
    ]

    path.write_text("\n".join(lines) + "\n")


# ── Test fixtures ────────────────────────────────────────────────


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
        status_file = tmp_path / "workflow.dag.status"
        _write_classad_status(status_file, {
            "mg_000000": _NS_DONE,
            "mg_000001": _NS_SUBMITTED,
            "mg_000002": _NS_READY,
        })

        result = monitor._parse_dagman_status(str(status_file))
        assert result.done == 1
        assert result.running == 1
        assert result.idle == 1
        assert result.failed == 0
        assert result.node_statuses["mg_000000"] == "done"
        assert result.node_statuses["mg_000001"] == "submitted"

    def test_parse_missing_file(self, monitor):
        result = monitor._parse_dagman_status("/nonexistent/path.status")
        assert result.done == 0
        assert result.running == 0
        assert result.idle == 0

    def test_parse_failed_nodes(self, monitor, tmp_path):
        status_file = tmp_path / "workflow.dag.status"
        _write_classad_status(status_file, {
            "mg_000000": _NS_ERROR,
            "mg_000001": _NS_DONE,
        })

        result = monitor._parse_dagman_status(str(status_file))
        assert result.done == 1
        assert result.failed == 1

    def test_parse_held_nodes(self, monitor, tmp_path):
        status_file = tmp_path / "workflow.dag.status"
        _write_classad_status(status_file, {
            "mg_000000": _NS_SUBMITTED,
        }, JobProcsHeld=1)

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
        _write_classad_status(tmp_path / "workflow.dag.status", {
            "mg_000000": _NS_DONE,
            "mg_000001": _NS_SUBMITTED,
        })

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
        _write_classad_status(tmp_path / "workflow.dag.status", {
            "mg_000000": _NS_DONE,
            "mg_000001": _NS_SUBMITTED,
        })

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

    async def test_detects_final_work_units(self, monitor, mock_repo, tmp_path):
        """On DAG exit, newly completed work units from .status file are returned."""
        dag = _make_dag_row(
            dag_file_path=str(tmp_path / "workflow.dag"),
            completed_work_units=[],
        )
        monitor.condor = MockCondorAdapter()
        monitor.condor.completed_jobs.add(dag.dagman_cluster_id)

        # Metrics file
        metrics_data = {"nodes_done": 2, "nodes_failed": 0}
        (tmp_path / "workflow.dag.metrics").write_text(json.dumps(metrics_data))

        # Status file shows both merge groups completed
        _write_classad_status(tmp_path / "workflow.dag.status", {
            "mg_000000": _NS_DONE,
            "mg_000001": _NS_DONE,
        })

        result = await monitor._handle_dag_completion(dag)
        assert result.status == DAGStatus.COMPLETED
        assert len(result.newly_completed_work_units) == 2
        names = {wu["group_name"] for wu in result.newly_completed_work_units}
        assert names == {"mg_000000", "mg_000001"}

        # Verify update_dag received the completed_work_units
        call_kwargs = mock_repo.update_dag.call_args[1]
        assert "mg_000000" in call_kwargs["completed_work_units"]
        assert "mg_000001" in call_kwargs["completed_work_units"]

    async def test_dag_gone_with_status_file_detects_wus(
        self, monitor, mock_condor, mock_repo, tmp_path
    ):
        """Full poll_dag when DAGMan gone: uses .status to detect final work units."""
        dag = _make_dag_row(
            dag_file_path=str(tmp_path / "workflow.dag"),
            completed_work_units=["mg_000000"],  # One already known
        )
        mock_condor.completed_jobs.add(dag.dagman_cluster_id)

        metrics_data = {"nodes_done": 2, "nodes_failed": 0}
        (tmp_path / "workflow.dag.metrics").write_text(json.dumps(metrics_data))

        _write_classad_status(tmp_path / "workflow.dag.status", {
            "mg_000000": _NS_DONE,
            "mg_000001": _NS_DONE,
        })

        result = await monitor.poll_dag(dag)
        assert result.status == DAGStatus.COMPLETED
        # Only mg_000001 is newly completed (mg_000000 was already known)
        assert len(result.newly_completed_work_units) == 1
        assert result.newly_completed_work_units[0]["group_name"] == "mg_000001"


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
        _write_classad_status(tmp_path / "workflow.dag.status", {
            "mg_000000": _NS_SUBMITTED,
        })

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
