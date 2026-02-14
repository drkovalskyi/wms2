from enum import Enum


class RequestStatus(str, Enum):
    NEW = "new"
    SUBMITTED = "submitted"
    QUEUED = "queued"
    PILOT_RUNNING = "pilot_running"
    PLANNING = "planning"
    ACTIVE = "active"
    STOPPING = "stopping"
    RESUBMITTING = "resubmitting"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


class WorkflowStatus(str, Enum):
    NEW = "new"
    PILOT_RUNNING = "pilot_running"
    PLANNING = "planning"
    ACTIVE = "active"
    STOPPING = "stopping"
    RESUBMITTING = "resubmitting"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


class DAGStatus(str, Enum):
    PLANNING = "planning"
    READY = "ready"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    REMOVED = "removed"
    HALTED = "halted"
    STOPPED = "stopped"


class OutputStatus(str, Enum):
    PENDING = "pending"
    DBS_REGISTERED = "dbs_registered"
    SOURCE_PROTECTED = "source_protected"
    TRANSFERS_REQUESTED = "transfers_requested"
    TRANSFERRING = "transferring"
    TRANSFERRED = "transferred"
    SOURCE_RELEASED = "source_released"
    ANNOUNCED = "announced"
    INVALIDATED = "invalidated"
    FAILED = "failed"


class SiteStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    DRAINING = "draining"
    MAINTENANCE = "maintenance"


class SplittingAlgo(str, Enum):
    FILE_BASED = "FileBased"
    EVENT_BASED = "EventBased"
    LUMI_BASED = "LumiBased"
    EVENT_AWARE_LUMI = "EventAwareLumiBased"


class CleanupPolicy(str, Enum):
    KEEP_UNTIL_REPLACED = "keep_until_replaced"
    IMMEDIATE_CLEANUP = "immediate_cleanup"


class NodeRole(str, Enum):
    PROCESSING = "Processing"
    MERGE = "Merge"
    CLEANUP = "Cleanup"
