import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from wms2.core.lifecycle_manager import RequestLifecycleManager
from wms2.models.enums import DAGStatus, RequestStatus, WorkflowStatus
from .conftest import make_request_row


def _make_workflow(dag_id=None, pilot_cluster_id=None, pilot_schedd=None, **kwargs):
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = kwargs.get("request_name", "test-request-001")
    wf.dag_id = dag_id
    wf.pilot_cluster_id = pilot_cluster_id
    wf.pilot_schedd = pilot_schedd
    wf.pilot_output_path = None
    for k, v in kwargs.items():
        setattr(wf, k, v)
    return wf


def _make_dag(
    status="submitted",
    dag_file_path="/tmp/submit/workflow.dag",
    submit_dir="/tmp/submit",
    rescue_dag_path=None,
    parent_dag_id=None,
    **kwargs,
):
    dag = MagicMock()
    dag.id = uuid.uuid4()
    dag.workflow_id = uuid.uuid4()
    dag.dag_file_path = dag_file_path
    dag.submit_dir = submit_dir
    dag.dagman_cluster_id = "12346"
    dag.schedd_name = "schedd.example.com"
    dag.status = status
    dag.rescue_dag_path = rescue_dag_path
    dag.parent_dag_id = parent_dag_id
    dag.total_nodes = 10
    dag.total_edges = 8
    dag.node_counts = {"processing": 8, "merge": 1, "cleanup": 1}
    dag.total_work_units = 2
    dag.completed_work_units = []
    for k, v in kwargs.items():
        setattr(dag, k, v)
    return dag


@pytest.fixture
def lifecycle_manager(mock_repository, mock_condor, settings):
    return RequestLifecycleManager(
        repository=mock_repository,
        condor_adapter=mock_condor,
        settings=settings,
    )


async def test_dispatch_submitted(lifecycle_manager, mock_repository):
    """SUBMITTED without workflow_manager skips gracefully."""
    request = make_request_row(status="submitted")
    await lifecycle_manager.evaluate_request(request)
    # No workflow_manager → handler skips, no transition
    mock_repository.update_request.assert_not_called()


async def test_dispatch_resubmitting_to_queued(lifecycle_manager, mock_repository):
    """RESUBMITTING should transition to QUEUED."""
    request = make_request_row(status="resubmitting")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_called_once()
    call_kwargs = mock_repository.update_request.call_args
    assert call_kwargs[1]["status"] == RequestStatus.QUEUED.value


async def test_transition_records_audit_trail(lifecycle_manager, mock_repository):
    """transition() should append to status_transitions."""
    request = make_request_row(status="resubmitting", status_transitions=[])
    await lifecycle_manager.transition(request, RequestStatus.QUEUED)

    call_kwargs = mock_repository.update_request.call_args
    transitions = call_kwargs[1]["status_transitions"]
    assert len(transitions) == 1
    assert transitions[0]["from"] == "resubmitting"
    assert transitions[0]["to"] == "queued"
    assert "timestamp" in transitions[0]


async def test_stuck_detection(lifecycle_manager, settings):
    """Request stuck in SUBMITTED beyond timeout is detected."""
    old_time = datetime.now(timezone.utc) - timedelta(seconds=settings.timeout_submitted + 10)
    request = make_request_row(status="submitted", updated_at=old_time)
    assert lifecycle_manager._is_stuck(request) is True


async def test_not_stuck_within_timeout(lifecycle_manager, settings):
    """Request within timeout is not stuck."""
    recent = datetime.now(timezone.utc) - timedelta(seconds=10)
    request = make_request_row(status="submitted", updated_at=recent)
    assert lifecycle_manager._is_stuck(request) is False


async def test_stuck_submitted_transitions_to_failed(lifecycle_manager, mock_repository, settings):
    """Stuck SUBMITTED request → FAILED."""
    old_time = datetime.now(timezone.utc) - timedelta(seconds=settings.timeout_submitted + 10)
    request = make_request_row(status="submitted", updated_at=old_time)
    await lifecycle_manager.evaluate_request(request)
    call_kwargs = mock_repository.update_request.call_args
    assert call_kwargs[1]["status"] == RequestStatus.FAILED.value


async def test_handle_queued_no_dag_planner(lifecycle_manager, mock_repository):
    """QUEUED without dag_planner skips gracefully."""
    request = make_request_row(status="queued")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_handle_queued_no_capacity(lifecycle_manager, mock_repository, settings):
    """QUEUED with no capacity doesn't transition."""
    lifecycle_manager.dag_planner = AsyncMock()
    mock_repository.count_active_dags.return_value = settings.max_active_dags
    request = make_request_row(status="queued")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_handle_active_no_dag_monitor(lifecycle_manager, mock_repository):
    """ACTIVE without dag_monitor skips gracefully."""
    request = make_request_row(status="active")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_terminal_statuses_not_dispatched(lifecycle_manager, mock_repository):
    """Terminal statuses (completed, failed, aborted) have no handler."""
    for status in ("completed", "failed", "aborted"):
        request = make_request_row(status=status)
        await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


# ── Clean Stop Tests ──────────────────────────────────────────


async def test_initiate_clean_stop(lifecycle_manager, mock_repository, mock_condor):
    """initiate_clean_stop removes DAGMan job, updates DAG/workflow/request."""
    request = make_request_row(status="active")
    dag = _make_dag(status="running")
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    await lifecycle_manager.initiate_clean_stop("test-request-001", "test stop")

    # condor_rm called
    assert any(c[0] == "remove_job" for c in mock_condor.calls)

    # DAG updated with stop metadata
    mock_repository.update_dag.assert_called_once()
    dag_update = mock_repository.update_dag.call_args
    assert dag_update[1]["stop_reason"] == "test stop"
    assert "stop_requested_at" in dag_update[1]

    # Workflow status set to STOPPING
    mock_repository.update_workflow.assert_called_once()
    wf_update = mock_repository.update_workflow.call_args
    assert wf_update[1]["status"] == WorkflowStatus.STOPPING.value

    # Request transitions to STOPPING
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.STOPPING.value


async def test_handle_stopping_dagman_gone(lifecycle_manager, mock_repository, mock_condor):
    """STOPPING: DAGMan gone → DAG STOPPED, recovery prepared, request → RESUBMITTING."""
    request = make_request_row(status="stopping")
    dag = _make_dag(status="running")
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    # DAGMan process gone — query_job returns None
    mock_condor.removed_jobs.add(dag.dagman_cluster_id)

    # create_dag returns the new rescue DAG record
    new_dag = _make_dag(status="ready", rescue_dag_path=f"{dag.dag_file_path}.rescue001")
    mock_repository.create_dag.return_value = new_dag

    await lifecycle_manager.evaluate_request(request)

    # DAG marked as STOPPED
    dag_update_calls = mock_repository.update_dag.call_args_list
    assert any(c[1].get("status") == DAGStatus.STOPPED.value for c in dag_update_calls)

    # Recovery DAG created
    mock_repository.create_dag.assert_called_once()

    # Request eventually transitions to RESUBMITTING
    last_req_update = mock_repository.update_request.call_args
    assert last_req_update[1]["status"] == RequestStatus.RESUBMITTING.value


async def test_handle_stopping_dagman_alive(lifecycle_manager, mock_repository, mock_condor):
    """STOPPING: DAGMan still running → no transition, wait."""
    request = make_request_row(status="stopping")
    dag = _make_dag(status="running")
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    # DAGMan still alive — query_job returns a result (not None)
    # MockCondorAdapter returns a job dict by default unless in removed/completed sets

    await lifecycle_manager.evaluate_request(request)

    # No status transition
    mock_repository.update_request.assert_not_called()


async def test_prepare_recovery_creates_rescue_dag(lifecycle_manager, mock_repository):
    """_prepare_recovery creates a new DAG record with parent_dag_id and rescue path."""
    request = make_request_row(status="stopping")
    dag = _make_dag(status="stopped")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag(status="ready", rescue_dag_path=f"{dag.dag_file_path}.rescue001")
    mock_repository.create_dag.return_value = new_dag

    await lifecycle_manager._prepare_recovery(request, workflow, dag)

    create_call = mock_repository.create_dag.call_args
    assert create_call[1]["parent_dag_id"] == dag.id
    assert create_call[1]["rescue_dag_path"] == f"{dag.dag_file_path}.rescue001"
    assert create_call[1]["status"] == DAGStatus.READY.value

    # Workflow updated with new DAG id and resubmitting status
    wf_update = mock_repository.update_workflow.call_args
    assert wf_update[1]["dag_id"] == new_dag.id
    assert wf_update[1]["status"] == "resubmitting"


async def test_prepare_recovery_consumes_production_step(lifecycle_manager, mock_repository):
    """_prepare_recovery pops first production step and updates priority."""
    steps = [
        {"fraction": 0.5, "priority": 80000},
        {"fraction": 1.0, "priority": 100000},
    ]
    request = make_request_row(status="stopping", production_steps=steps)
    dag = _make_dag(status="stopped")
    workflow = _make_workflow(dag_id=dag.id)

    new_dag = _make_dag(status="ready")
    mock_repository.create_dag.return_value = new_dag

    await lifecycle_manager._prepare_recovery(request, workflow, dag)

    # update_request called twice: once for production_steps, once for transition
    req_calls = mock_repository.update_request.call_args_list
    # First call updates priority and production_steps
    step_update = req_calls[0]
    assert step_update[1]["priority"] == 80000
    assert len(step_update[1]["production_steps"]) == 1
    assert step_update[1]["production_steps"][0]["fraction"] == 1.0


async def test_handle_active_processes_work_units(mock_repository, mock_condor, settings):
    """_handle_active calls output_manager for each completed work unit."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    mock_output_manager = AsyncMock()

    dag = _make_dag(status="submitted")
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    # Mock a processing block that get_processing_blocks returns
    block = MagicMock()
    block.id = uuid.uuid4()
    block.dataset_name = "/TestPrimary/Test-v1/GEN-SIM"
    mock_repository.get_processing_blocks = AsyncMock(return_value=[block])

    # Mock dag_monitor to return newly completed work units with manifest
    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id),
        status=DAGStatus.RUNNING,
        nodes_done=1,
        newly_completed_work_units=[
            {
                "group_name": "mg_000000",
                "manifest": {
                    "site": "T2_US_MIT",
                    "datasets": {
                        "/TestPrimary/Test-v1/GEN-SIM": {
                            "files": ["/store/mc/test/output.root"],
                        },
                    },
                },
            },
        ],
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
        output_manager=mock_output_manager,
    )

    request = make_request_row(status="active")
    await lm.evaluate_request(request)

    # Output manager called for each block with data extracted from manifest
    mock_output_manager.handle_work_unit_completion.assert_called_once_with(
        workflow.id, block.id,
        {
            "output_files": ["/store/mc/test/output.root"],
            "site": "T2_US_MIT",
            "node_name": "mg_000000",
        },
    )
    mock_output_manager.process_blocks_for_workflow.assert_called_once_with(workflow.id)


async def test_handle_active_no_output_manager_skips(mock_repository, mock_condor, settings):
    """_handle_active with no output_manager skips output processing (backward compat)."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    dag = _make_dag(status="submitted")
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    block_id = uuid.uuid4()
    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id),
        status=DAGStatus.RUNNING,
        nodes_done=1,
        newly_completed_work_units=[
            {"block_id": block_id, "node_name": "mg_000000", "output_files": []},
        ],
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
        # No output_manager
    )

    request = make_request_row(status="active")
    # Should not raise
    await lm.evaluate_request(request)


# ── Error Handler Integration ────────────────────────────────


async def test_active_partial_with_rescue(mock_repository, mock_condor, settings):
    """PARTIAL + error_handler returns 'rescue' → request RESUBMITTING."""
    from wms2.core.dag_monitor import DAGPollResult

    mock_error_handler = AsyncMock()
    mock_error_handler.handle_dag_completion.return_value = "rescue"

    dag = _make_dag(status="submitted", nodes_done=19, nodes_failed=1)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.PARTIAL,
        nodes_done=19, nodes_failed=1,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
        error_handler=mock_error_handler,
    )

    request = make_request_row(status="active")
    await lm.evaluate_request(request)

    mock_error_handler.handle_dag_completion.assert_called_once_with(
        dag, request, workflow
    )
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.RESUBMITTING.value


async def test_active_partial_without_error_handler(mock_repository, mock_condor, settings):
    """PARTIAL without error_handler → request HELD."""
    from wms2.core.dag_monitor import DAGPollResult

    dag = _make_dag(status="submitted", nodes_done=19, nodes_failed=1)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.PARTIAL,
        nodes_done=19, nodes_failed=1,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
        # No error_handler
    )

    request = make_request_row(status="active")
    await lm.evaluate_request(request)

    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.HELD.value


async def test_active_failed_with_error_handler(mock_repository, mock_condor, settings):
    """FAILED + error_handler returns 'hold' → request HELD (held for operator)."""
    from wms2.core.dag_monitor import DAGPollResult

    mock_error_handler = AsyncMock()
    mock_error_handler.handle_dag_completion.return_value = "hold"

    dag = _make_dag(status="submitted", nodes_done=0, nodes_failed=20)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.FAILED,
        nodes_done=0, nodes_failed=20,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
        error_handler=mock_error_handler,
    )

    request = make_request_row(status="active")
    await lm.evaluate_request(request)

    mock_error_handler.handle_dag_completion.assert_called_once_with(
        dag, request, workflow
    )
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.HELD.value


async def test_handle_queued_rescue_dag_submits(lifecycle_manager, mock_repository, mock_condor):
    """QUEUED with rescue DAG → submit to HTCondor → ACTIVE."""
    lifecycle_manager.dag_planner = AsyncMock()

    request = make_request_row(status="queued")
    rescue_path = "/tmp/submit/workflow.dag.rescue001"
    dag = _make_dag(status="ready", rescue_dag_path=rescue_path)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.count_active_dags.return_value = 0
    mock_repository.get_queued_requests.return_value = [request]
    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    await lifecycle_manager.evaluate_request(request)

    # Rescue DAG submitted to HTCondor
    submit_calls = [c for c in mock_condor.calls if c[0] == "submit_dag"]
    assert len(submit_calls) == 1
    assert submit_calls[0][1] == (rescue_path,)

    # DAG record updated with new cluster_id and SUBMITTED status
    mock_repository.update_dag.assert_called_once()
    dag_update = mock_repository.update_dag.call_args
    assert dag_update[1]["status"] == DAGStatus.SUBMITTED.value
    assert "dagman_cluster_id" in dag_update[1]

    # Request transitions to ACTIVE
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.ACTIVE.value


# ── Adaptive Round Completion Tests ──────────────────────────


async def test_round_completion_gen_returns_to_queued(mock_repository, mock_condor, settings):
    """Adaptive GEN request with remaining events → QUEUED, offsets advanced."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    # GEN workflow: 1M total events, 100k per job, round 0 with 8 processing jobs
    dag = _make_dag(status="submitted", nodes_done=8, nodes_failed=0)
    workflow = _make_workflow(
        dag_id=dag.id,
        config_data={"_is_gen": True, "request_num_events": 1_000_000},
        splitting_params={"events_per_job": 100_000},
        next_first_event=1,
        file_offset=0,
        current_round=0,
        step_metrics=None,
        adaptive=True,
    )

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.COMPLETED,
        nodes_done=8, nodes_failed=0,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
    )

    request = make_request_row(status="active", adaptive=True)
    await lm.evaluate_request(request)

    # Workflow updated with new round and offset
    wf_update = mock_repository.update_workflow.call_args
    assert wf_update[1]["current_round"] == 1
    assert wf_update[1]["next_first_event"] == 800_001  # 1 + 8*100_000
    assert wf_update[1]["step_metrics"]["rounds_completed"] == 1
    assert wf_update[1]["step_metrics"]["cumulative_nodes_done"] == 8

    # Request transitions to QUEUED (not COMPLETED)
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.QUEUED.value


async def test_round_completion_gen_all_done(mock_repository, mock_condor, settings):
    """Adaptive GEN request where all events processed → COMPLETED."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    # 1M events total, already at event 900_001 (9 rounds done), 1 job of 100k left
    dag = _make_dag(
        status="submitted", nodes_done=1, nodes_failed=0,
        node_counts={"processing": 1, "merge": 1, "cleanup": 1},
    )
    workflow = _make_workflow(
        dag_id=dag.id,
        config_data={"_is_gen": True, "request_num_events": 1_000_000},
        splitting_params={"events_per_job": 100_000},
        next_first_event=900_001,
        file_offset=0,
        current_round=9,
        step_metrics={"rounds_completed": 9, "cumulative_nodes_done": 72},
        adaptive=True,
    )

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.COMPLETED,
        nodes_done=1, nodes_failed=0,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
    )

    request = make_request_row(status="active", adaptive=True)
    await lm.evaluate_request(request)

    # Workflow updated
    wf_update = mock_repository.update_workflow.call_args
    assert wf_update[1]["current_round"] == 10
    assert wf_update[1]["next_first_event"] == 1_000_001

    # Request transitions to COMPLETED (all done)
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.COMPLETED.value


async def test_round_completion_file_based_returns_to_queued(
    mock_repository, mock_condor, settings
):
    """Adaptive file-based request → QUEUED, file_offset advanced."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    dag = _make_dag(
        status="submitted", nodes_done=5, nodes_failed=0,
        node_counts={"processing": 5, "merge": 1, "cleanup": 1},
    )
    workflow = _make_workflow(
        dag_id=dag.id,
        config_data={"_is_gen": False},
        splitting_params={"files_per_job": 2},
        next_first_event=1,
        file_offset=0,
        current_round=0,
        step_metrics=None,
        adaptive=True,
    )

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.COMPLETED,
        nodes_done=5, nodes_failed=0,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
    )

    request = make_request_row(status="active", adaptive=True)
    await lm.evaluate_request(request)

    # Workflow updated with file_offset (not next_first_event)
    wf_update = mock_repository.update_workflow.call_args
    assert wf_update[1]["current_round"] == 1
    assert wf_update[1]["file_offset"] == 10  # 5 jobs * 2 files_per_job
    assert "next_first_event" not in wf_update[1]

    # Request transitions to QUEUED
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.QUEUED.value


async def test_non_adaptive_skips_round_handler(mock_repository, mock_condor, settings):
    """Non-adaptive request → COMPLETED directly (existing behavior)."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    dag = _make_dag(status="submitted", nodes_done=10, nodes_failed=0)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.COMPLETED,
        nodes_done=10, nodes_failed=0,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
    )

    request = make_request_row(status="active")
    await lm.evaluate_request(request)

    # Request transitions to COMPLETED (no round handler)
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.COMPLETED.value
    # update_workflow should NOT have been called with current_round
    for call in mock_repository.update_workflow.call_args_list:
        assert "current_round" not in call[1]


async def test_round_completion_priority_demotion(mock_repository, mock_condor, settings):
    """Adaptive request with production_steps → priority demoted when threshold crossed."""
    from wms2.core.dag_monitor import DAGPollResult
    from wms2.core.lifecycle_manager import RequestLifecycleManager

    # 1000 events, 100 per job, 5 done this round → offset moves to 501
    # progress = 500/1000 = 0.5, matches first step's fraction
    dag = _make_dag(
        status="submitted", nodes_done=5, nodes_failed=0,
        node_counts={"processing": 5, "merge": 1, "cleanup": 1},
    )
    workflow = _make_workflow(
        dag_id=dag.id,
        config_data={"_is_gen": True, "request_num_events": 1000},
        splitting_params={"events_per_job": 100},
        next_first_event=1,
        file_offset=0,
        current_round=0,
        step_metrics=None,
        adaptive=True,
    )

    mock_repository.get_workflow_by_request = AsyncMock(return_value=workflow)
    mock_repository.get_dag = AsyncMock(return_value=dag)

    mock_dag_monitor = AsyncMock()
    mock_dag_monitor.poll_dag.return_value = DAGPollResult(
        dag_id=str(dag.id), status=DAGStatus.COMPLETED,
        nodes_done=5, nodes_failed=0,
    )

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_monitor=mock_dag_monitor,
    )

    steps = [
        {"fraction": 0.5, "priority": 80000},
        {"fraction": 1.0, "priority": 100000},
    ]
    request = make_request_row(status="active", adaptive=True, production_steps=steps)
    await lm.evaluate_request(request)

    # update_request called for priority demotion AND for transition
    req_calls = mock_repository.update_request.call_args_list
    # Priority demotion call (first)
    priority_call = req_calls[0]
    assert priority_call[1]["priority"] == 80000
    assert len(priority_call[1]["production_steps"]) == 1
    assert priority_call[1]["production_steps"][0]["fraction"] == 1.0

    # Transition call (second) — QUEUED
    transition_call = req_calls[1]
    assert transition_call[1]["status"] == RequestStatus.QUEUED.value


async def test_queued_round2_skips_pilot(mock_repository, mock_condor, settings):
    """Round 2+ (current_round=1) → plan_production_dag called directly, no pilot."""
    mock_dag_planner = AsyncMock()

    workflow = _make_workflow(
        current_round=1,
        next_first_event=500_001,
        file_offset=0,
    )

    mock_repository.count_active_dags.return_value = 0
    request = make_request_row(status="queued", adaptive=True)
    mock_repository.get_queued_requests.return_value = [request]
    mock_repository.get_workflow_by_request.return_value = workflow

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_planner=mock_dag_planner,
    )

    await lm.evaluate_request(request)

    # plan_production_dag called with adaptive=True, NOT submit_pilot
    mock_dag_planner.plan_production_dag.assert_called_once_with(
        workflow, adaptive=True,
    )
    mock_dag_planner.submit_pilot.assert_not_called()

    # Request transitions to ACTIVE
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.ACTIVE.value


async def test_queued_round2_all_done_completes(mock_repository, mock_condor, settings):
    """Round 2+ where plan_production_dag returns None → COMPLETED."""
    mock_dag_planner = AsyncMock()
    mock_dag_planner.plan_production_dag.return_value = None  # All work done

    workflow = _make_workflow(
        current_round=2,
        next_first_event=1_000_001,
        file_offset=0,
    )

    mock_repository.count_active_dags.return_value = 0
    request = make_request_row(status="queued", adaptive=True)
    mock_repository.get_queued_requests.return_value = [request]
    mock_repository.get_workflow_by_request.return_value = workflow

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_planner=mock_dag_planner,
    )

    await lm.evaluate_request(request)

    # plan_production_dag called with adaptive=True
    mock_dag_planner.plan_production_dag.assert_called_once_with(
        workflow, adaptive=True,
    )

    # Request transitions to COMPLETED (not ACTIVE)
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.COMPLETED.value


async def test_queued_urgent_adaptive_all_done_completes(mock_repository, mock_condor, settings):
    """Urgent adaptive round 0 where plan_production_dag returns None → COMPLETED."""
    mock_dag_planner = AsyncMock()
    mock_dag_planner.plan_production_dag.return_value = None

    workflow = _make_workflow(current_round=0)

    mock_repository.count_active_dags.return_value = 0
    request = make_request_row(status="queued", urgent=True, adaptive=True)
    mock_repository.get_queued_requests.return_value = [request]
    mock_repository.get_workflow_by_request.return_value = workflow

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_planner=mock_dag_planner,
    )

    await lm.evaluate_request(request)

    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.COMPLETED.value


async def test_queued_round0_runs_pilot(mock_repository, mock_condor, settings):
    """Round 0, non-urgent → pilot submitted (existing behavior)."""
    mock_dag_planner = AsyncMock()

    workflow = _make_workflow(current_round=0)

    mock_repository.count_active_dags.return_value = 0
    request = make_request_row(status="queued", urgent=False)
    mock_repository.get_queued_requests.return_value = [request]
    mock_repository.get_workflow_by_request.return_value = workflow

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        dag_planner=mock_dag_planner,
    )

    await lm.evaluate_request(request)

    # submit_pilot called, NOT plan_production_dag
    mock_dag_planner.submit_pilot.assert_called_once_with(workflow)
    mock_dag_planner.plan_production_dag.assert_not_called()

    # Request transitions to PILOT_RUNNING
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.PILOT_RUNNING.value


async def test_aggregate_round_metrics():
    """Unit test for _aggregate_round_metrics()."""
    from wms2.core.lifecycle_manager import _aggregate_round_metrics

    dag = MagicMock()
    dag.nodes_done = 10
    dag.nodes_failed = 2
    dag.total_work_units = 3

    # First round — no prior metrics
    result = _aggregate_round_metrics(None, dag, 0)
    assert result["rounds_completed"] == 1
    assert result["cumulative_nodes_done"] == 10
    assert result["cumulative_nodes_failed"] == 2
    assert result["last_round_nodes_done"] == 10
    assert result["last_round_work_units"] == 3

    # Second round — accumulate
    dag2 = MagicMock()
    dag2.nodes_done = 8
    dag2.nodes_failed = 1
    dag2.total_work_units = 2

    result2 = _aggregate_round_metrics(result, dag2, 1)
    assert result2["rounds_completed"] == 2
    assert result2["cumulative_nodes_done"] == 18  # 10 + 8
    assert result2["cumulative_nodes_failed"] == 3  # 2 + 1
    assert result2["last_round_nodes_done"] == 8
    assert result2["last_round_work_units"] == 2


# ── HELD State and Operator Action Tests ─────────────────────


async def test_held_no_automatic_transition(lifecycle_manager, mock_repository):
    """HELD handler is a no-op — no state transition."""
    request = make_request_row(status="held")
    await lifecycle_manager.evaluate_request(request)
    mock_repository.update_request.assert_not_called()


async def test_held_no_timeout(lifecycle_manager, settings):
    """Request HELD for a long time is NOT considered stuck (no timeout)."""
    old_time = datetime.now(timezone.utc) - timedelta(days=365)
    request = make_request_row(status="held", updated_at=old_time)
    assert lifecycle_manager._is_stuck(request) is False


async def test_release_held_request(mock_repository, mock_condor, settings):
    """release_held_request: HELD → QUEUED."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="held")
    mock_repository.get_request.return_value = request

    await lm.release_held_request("test-request-001")

    call_kwargs = mock_repository.update_request.call_args
    assert call_kwargs[1]["status"] == RequestStatus.QUEUED.value


async def test_release_rejects_non_held(mock_repository, mock_condor, settings):
    """release_held_request rejects non-HELD request."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="active")
    mock_repository.get_request.return_value = request

    with pytest.raises(ValueError, match="must be held"):
        await lm.release_held_request("test-request-001")


async def test_fail_request_cleanup(mock_repository, mock_condor, settings):
    """fail_request: condor_rm + DAGs→FAILED + blocks→failed + request→FAILED."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="held")
    mock_repository.get_request.return_value = request

    dag = _make_dag(status="running")
    workflow = _make_workflow(dag_id=dag.id)
    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    # list_dags returns the running dag plus one ready dag
    ready_dag = _make_dag(status="ready")
    mock_repository.list_dags.return_value = [dag, ready_dag]

    # get_processing_blocks returns one open block
    block = MagicMock()
    block.id = uuid.uuid4()
    block.status = "open"
    mock_repository.get_processing_blocks.return_value = [block]

    await lm.fail_request("test-request-001")

    # condor_rm called for running DAG
    assert any(c[0] == "remove_job" for c in mock_condor.calls)

    # Both non-terminal DAGs marked FAILED
    dag_updates = mock_repository.update_dag.call_args_list
    assert len(dag_updates) == 2
    for call in dag_updates:
        assert call[1]["status"] == "failed"

    # Open block marked failed
    mock_repository.update_processing_block.assert_called_once()
    block_update = mock_repository.update_processing_block.call_args
    assert block_update[1]["status"] == "failed"

    # Request transitioned to FAILED
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.FAILED.value


async def test_fail_request_no_workflow(mock_repository, mock_condor, settings):
    """fail_request with no workflow → just transition to FAILED (no cleanup errors)."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="held")
    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = None

    await lm.fail_request("test-request-001")

    # No condor_rm, no DAG updates, no block updates
    assert len(mock_condor.calls) == 0
    mock_repository.update_dag.assert_not_called()
    mock_repository.update_processing_block.assert_not_called()

    # Request still transitioned to FAILED
    req_update = mock_repository.update_request.call_args
    assert req_update[1]["status"] == RequestStatus.FAILED.value


async def test_fail_rejects_active(mock_repository, mock_condor, settings):
    """fail_request rejects ACTIVE request."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="active")
    mock_repository.get_request.return_value = request

    with pytest.raises(ValueError, match="must be held or partial"):
        await lm.fail_request("test-request-001")


async def test_restart_creates_clone(mock_repository, mock_condor, settings):
    """restart_request: new request with version+1, old linked, old failed."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(
        status="held",
        request_data={"processing_version": 1, "some_param": "value"},
    )
    # get_request is called multiple times: once for restart, once for fail
    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = None  # No workflow

    new_name = await lm.restart_request("test-request-001")

    assert new_name == "test-request-001_v2"

    # create_request called with cloned data
    create_call = mock_repository.create_request.call_args
    assert create_call[1]["request_name"] == "test-request-001_v2"
    assert create_call[1]["request_data"]["processing_version"] == 2
    assert create_call[1]["request_data"]["some_param"] == "value"
    assert create_call[1]["previous_version_request"] == "test-request-001"
    assert create_call[1]["status"] == RequestStatus.SUBMITTED.value

    # Old request linked to new
    link_call = mock_repository.update_request.call_args_list[0]
    assert link_call[1]["superseded_by_request"] == "test-request-001_v2"

    # Old request transitioned to FAILED (via fail_request)
    last_update = mock_repository.update_request.call_args
    assert last_update[1]["status"] == RequestStatus.FAILED.value


async def test_restart_preserves_fields(mock_repository, mock_condor, settings):
    """restart_request clones requestor, campaign, payload_config, etc."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(
        status="held",
        request_name="my-req",
        request_data={},
        campaign="MyCampaign",
        priority=50000,
    )
    request.payload_config = {"step1": "config"}
    request.splitting_params = {"files_per_job": 5}
    request.input_dataset = "/Data/Run/RECO"
    request.urgent = False
    request.adaptive = True
    request.production_steps = [{"fraction": 0.5, "priority": 80000}]
    request.cleanup_policy = "immediate_cleanup"

    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = None

    await lm.restart_request("my-req")

    create_call = mock_repository.create_request.call_args
    assert create_call[1]["requestor"] == "testuser"
    assert create_call[1]["campaign"] == "MyCampaign"
    assert create_call[1]["priority"] == 50000
    assert create_call[1]["payload_config"] == {"step1": "config"}
    assert create_call[1]["splitting_params"] == {"files_per_job": 5}
    assert create_call[1]["input_dataset"] == "/Data/Run/RECO"
    assert create_call[1]["adaptive"] is True
    assert create_call[1]["cleanup_policy"] == "immediate_cleanup"


async def test_get_error_summary(mock_repository, mock_condor, settings):
    """get_error_summary aggregates POST data: categories, sites, bad files."""
    mock_error_handler = MagicMock()
    mock_error_handler.read_post_data.return_value = [
        {
            "classification": {"category": "infrastructure", "bad_input_file": "/store/bad.root"},
            "job": {"site": "T2_US_MIT"},
        },
        {
            "classification": {"category": "infrastructure"},
            "job": {"site": "T2_US_MIT"},
        },
        {
            "classification": {"category": "success"},
            "job": {"site": "T2_DE_DESY"},
        },
    ]

    lm = RequestLifecycleManager(
        mock_repository, mock_condor, settings,
        error_handler=mock_error_handler,
    )

    request = make_request_row(status="held")
    dag = _make_dag(status="failed", nodes_done=1, nodes_failed=2)
    workflow = _make_workflow(dag_id=dag.id)

    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = workflow
    mock_repository.get_dag.return_value = dag

    result = await lm.get_error_summary("test-request-001")

    assert result["request_name"] == "test-request-001"
    assert result["status"] == "held"
    assert result["nodes_failed"] == 2
    assert result["error_summary"]["infrastructure"] == 2
    assert result["error_summary"]["success"] == 1
    assert result["site_summary"]["T2_US_MIT"]["total"] == 2
    assert result["site_summary"]["T2_US_MIT"]["failed"] == 2
    assert result["site_summary"]["T2_DE_DESY"]["total"] == 1
    assert result["site_summary"]["T2_DE_DESY"]["failed"] == 0
    assert result["bad_input_files"] == ["/store/bad.root"]


async def test_get_error_summary_no_workflow(mock_repository, mock_condor, settings):
    """get_error_summary with no workflow returns empty summary."""
    lm = RequestLifecycleManager(mock_repository, mock_condor, settings)

    request = make_request_row(status="held")
    mock_repository.get_request.return_value = request
    mock_repository.get_workflow_by_request.return_value = None

    result = await lm.get_error_summary("test-request-001")

    assert result["request_name"] == "test-request-001"
    assert result["dag_id"] is None
    assert result["error_summary"] == {}
    assert result["site_summary"] == {}
    assert result["bad_input_files"] == []
