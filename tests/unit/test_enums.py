from wms2.models.enums import (
    BlockStatus,
    CleanupPolicy,
    DAGStatus,
    NodeRole,
    RequestStatus,
    SiteStatus,
    SplittingAlgo,
    WorkflowStatus,
)


def test_request_status_values():
    assert len(RequestStatus) == 12
    assert RequestStatus.NEW == "new"
    assert RequestStatus.SUBMITTED == "submitted"
    assert RequestStatus.QUEUED == "queued"
    assert RequestStatus.PILOT_RUNNING == "pilot_running"
    assert RequestStatus.PLANNING == "planning"
    assert RequestStatus.ACTIVE == "active"
    assert RequestStatus.STOPPING == "stopping"
    assert RequestStatus.RESUBMITTING == "resubmitting"
    assert RequestStatus.COMPLETED == "completed"
    assert RequestStatus.PARTIAL == "partial"
    assert RequestStatus.FAILED == "failed"
    assert RequestStatus.ABORTED == "aborted"


def test_workflow_status_values():
    assert len(WorkflowStatus) == 10
    assert WorkflowStatus.NEW == "new"
    assert WorkflowStatus.COMPLETED == "completed"


def test_dag_status_values():
    assert len(DAGStatus) == 10
    assert DAGStatus.PLANNING == "planning"
    assert DAGStatus.READY == "ready"
    assert DAGStatus.STOPPED == "stopped"


def test_block_status_values():
    assert len(BlockStatus) == 5
    assert BlockStatus.OPEN == "open"
    assert BlockStatus.COMPLETE == "complete"
    assert BlockStatus.ARCHIVED == "archived"
    assert BlockStatus.FAILED == "failed"
    assert BlockStatus.INVALIDATED == "invalidated"


def test_site_status_values():
    assert len(SiteStatus) == 4


def test_splitting_algo_values():
    assert len(SplittingAlgo) == 4
    assert SplittingAlgo.FILE_BASED == "FileBased"
    assert SplittingAlgo.EVENT_AWARE_LUMI == "EventAwareLumiBased"


def test_cleanup_policy_values():
    assert len(CleanupPolicy) == 2
    assert CleanupPolicy.KEEP_UNTIL_REPLACED == "keep_until_replaced"


def test_node_role_values():
    assert len(NodeRole) == 3
    assert NodeRole.PROCESSING == "Processing"
    assert NodeRole.MERGE == "Merge"
    assert NodeRole.CLEANUP == "Cleanup"


def test_enum_serialization():
    assert RequestStatus.ACTIVE.value == "active"
    assert str(RequestStatus.ACTIVE) == "RequestStatus.ACTIVE"
    assert RequestStatus("active") == RequestStatus.ACTIVE
