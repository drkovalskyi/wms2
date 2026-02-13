# WMS2 — CMS Workload Management System v2

**OpenSpec v1.0**

| Field | Value |
|---|---|
| **Spec Version** | 2.3.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-13 |
| **Authors** | CMS Computing |
| **Supersedes** | WMCore / WMAgent |

---

## 1. Overview

### 1.1 Purpose

WMS2 is a modern, maintainable Workload Management system designed to replace WMCore. It leverages existing ReqMgr2 schemas and workflows while simplifying the architecture, improving observability, and enabling faster development cycles.

A key architectural decision is the delegation of all job-level execution orchestration to **HTCondor DAGMan**, eliminating the need for WMS2 to track, retry, or manage individual job lifecycles. WMS2 is a thin orchestration layer; HTCondor is the engine.

### 1.2 Core Goals

- **Simplicity**: Replace complex multi-component architecture with a streamlined, modern design.
- **Compatibility**: Support existing ReqMgr2 workflow schemas and CMS service integrations.
- **Scalability**: Handle CMS production workloads (millions of jobs across hundreds of sites).
- **Maintainability**: Clean codebase with clear separation of concerns.
- **Observability**: First-class monitoring, logging, and debugging capabilities.
- **Delegation**: Let HTCondor DAGMan own job-level orchestration (submission, retry, dependency management).
- **Payload Agnosticism**: WMS2 is agnostic to what runs on the worker node. Whether a job runs one CMSSW step or five (StepChain), the DAG structure is identical. The worker-node payload is an opaque unit managed by the sandbox/wrapper.

### 1.3 Design Philosophy

WMS2 delegates all job-level concerns to HTCondor. Where HTCondor lacks needed capabilities, the resolution path is collaboration with the HTCondor team, not workarounds in WMS2. This ensures that improvements benefit the entire HTCondor community rather than being confined to CMS.

### 1.4 Non-Goals (MVP Phase)

- Full feature parity with WMCore (incremental migration).
- Monte Carlo generation workflows (focus on data processing first).
- Data management beyond output registration (full Rucio integration comes later).
- Per-job tracking or state management in WMS2 (delegated to DAGMan).
- Accounting and resource usage reporting (handled by existing external systems).

---

## 2. Architecture

### 2.1 Current WMCore Architecture (Reference)

```
ReqMgr2 ──► WorkQueue ──► WMAgent ──► HTCondor
   │            │            │
   │            │            ├── JobCreator
   │            │            ├── JobSubmitter
   │            │            ├── JobAccountant
   │            │            ├── ErrorHandler
   │            │            ├── RetryManager
   │            │            └── ... (15+ components)
   │            │
CouchDB    CouchDB/MySQL    MySQL/Oracle
```

**Problems**: Complex multi-tier architecture; multiple databases with sync issues; 15+ agent components running as threads; Python 2 legacy code mixed with Python 3; difficult to debug and maintain; CouchDB scaling limitations.

### 2.2 WMS2 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          WMS2 Architecture                          │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                     WMS2 API (FastAPI)                       │   │
│   │  /requests  /workflows  /dags  /sites  /monitoring          │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│   ┌──────────────────────────┼──────────────────────────────────┐   │
│   │                    Core Engine                               │   │
│   │                                                              │   │
│   │  ┌────────────────────────────────────────────────────────┐  │   │
│   │  │           Request Lifecycle Manager                     │  │   │
│   │  │  Single owner of request state machine — evaluates     │  │   │
│   │  │  all non-terminal requests and dispatches to workers   │  │   │
│   │  └───────────────────────┬────────────────────────────────┘  │   │
│   │                          │ calls                              │   │
│   │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │   │
│   │  │  Workflow     │ │  Admission   │ │  DAG         │         │   │
│   │  │  Manager      │ │  Controller  │ │  Planner     │         │   │
│   │  └──────────────┘ └──────────────┘ └──────────────┘         │   │
│   │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐         │   │
│   │  │  DAG         │ │  Output      │ │  Site        │         │   │
│   │  │  Monitor     │ │  Manager     │ │  Manager     │         │   │
│   │  └──────────────┘ └──────────────┘ └──────────────┘         │   │
│   │  ┌──────────────┐ ┌──────────────┐                           │   │
│   │  │  Error       │ │  Metrics     │                           │   │
│   │  │  Handler     │ │  Collector   │                           │   │
│   │  └──────────────┘ └──────────────┘                           │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│   ┌──────────────────────────┼──────────────────────────────────┐   │
│   │                    External Adapters                          │   │
│   │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │   │
│   │  │ ReqMgr2  │ │  DBS     │ │ HTCondor │ │  Rucio   │        │   │
│   │  │ Adapter  │ │ Adapter  │ │ /DAGMan  │ │ Adapter  │        │   │
│   │  └──────────┘ └──────────┘ │ Adapter  │ └──────────┘        │   │
│   │                            └──────────┘                      │   │
│   └──────────────────────────┬──────────────────────────────────┘   │
│                              │                                      │
│   ┌──────────────────────────┴──────────────────────────────────┐   │
│   │                    Data Layer                                │   │
│   │  ┌──────────────────────────────────────────────────────┐    │   │
│   │  │                   PostgreSQL                          │    │   │
│   │  │  Workflows, DAGs, Output Datasets, Sites, History     │    │   │
│   │  └──────────────────────────────────────────────────────┘    │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                HTCondor / DAGMan (External)                  │   │
│   │  Owns: job submission, per-node retry, dependency ordering,  │   │
│   │        resource negotiation, per-node error handling         │   │
│   └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

**Key architectural principle**: WMS2 operates at the *workflow* and *DAG* level. The **Request Lifecycle Manager** is the single owner of every request's state machine — it runs a continuous loop over all non-terminal requests, evaluates each one, and dispatches work to the appropriate component worker (Admission Controller, DAG Planner, DAG Monitor, Output Manager). This replaces distributed independent polling loops per component, eliminating the risk of requests silently stalling when any single loop hiccups. Once a DAG is submitted to HTCondor DAGMan, HTCondor owns the individual job lifecycle — submission, retry, hold/release, and dependency resolution. WMS2 monitors DAG-level progress and intervenes only at the workflow level (abort, priority changes, clean stop, catastrophic recovery).

### 2.3 Execution Model

WMS2 treats the worker-node payload as opaque. Whether the job runs a single cmsRun (ReReco-style) or chains multiple steps (StepChain-style), the DAG structure is identical. The complexity of multi-step execution lives entirely within the sandbox/wrapper.

```
From WMS2's perspective, a workflow is a set of independent merge groups:

  ┌─ Merge Group 0: [Landing] → [Processing Nodes] → [Merge] → [Cleanup]
  ├─ Merge Group 1: [Landing] → [Processing Nodes] → [Merge] → [Cleanup]
  ├─ Merge Group 2: [Landing] → [Processing Nodes] → [Merge] → [Cleanup]
  └─ ...

Each merge group is a SUBDAG EXTERNAL within one top-level DAG.
The landing node lets HTCondor pick the site; all other nodes in the
group are pinned to that site. Groups are independent and run in
parallel (throttled by MAXJOBS).

What happens inside each processing node is opaque:

  Single-step:    input → cmsRun → output
  Multi-step:     input → cmsRun(DIGI) → cmsRun(RECO) → cmsRun(MINIAOD) → output

WMS2 doesn't distinguish between these. The sandbox/wrapper handles
step orchestration, resource optimization, and intermediate data flow
on the worker node.
```

The sandbox/wrapper is part of the WMS2 system but its development is decoupled — it can be improved independently without changes to WMS2 core or DAGMan.

### 2.4 Capacity Planning

Expected operating parameters:

| Parameter | Expected Value |
|---|---|
| Nodes per workflow (typical) | ~10,000 |
| Nodes per workflow (large) | up to ~50,000–100,000 |
| Concurrent active workflows | ~300 |
| Total managed job population | ~10M |
| Idle job budget (negotiator safe range) | ~1M |
| Running jobs at any time | ~hundreds of thousands |
| Schedds | horizontally scalable, as many as needed |

Per-DAG throttling via `DAGMAN_MAX_JOBS_IDLE` and `DAGMAN_MAX_JOBS_SUBMITTED` provides basic pacing. Global idle throttling across all concurrent DAGs is a required HTCondor feature (see Section 12). If not available at launch, WMS2 will use conservative static per-DAG limits as an interim measure.

The negotiator scales with the number of idle + running jobs (not total managed population). Schedds scale with total managed jobs but can be added horizontally. DAGMan processes are lightweight (~few MB each for 10K-node DAGs).

---

## 3. Data Models

### 3.1 Core Entities (MVP)

#### 3.1.1 Request (Compatible with ReqMgr2 Schema)

```python
class Request(BaseModel):
    """
    Workflow request — compatible with ReqMgr2 schema.
    Top-level entity submitted by users.
    """
    # Identity
    request_name: str                # Auto-generated unique name

    # Ownership
    requestor: str                   # Username
    requestor_dn: str               # X.509 DN
    group: str                      # CMS group

    # Processing Parameters
    input_dataset: str              # /Primary/Processed/Tier
    secondary_input_dataset: Optional[str]
    output_datasets: List[str]      # Expected outputs

    # Software Configuration
    cmssw_version: str              # e.g. "CMSSW_14_0_0"
    scram_arch: str                 # e.g. "el9_amd64_gcc12"
    global_tag: str                 # Conditions DB tag

    # Resource Hints (used for urgent workflows without pilot)
    memory_mb: int = 2048
    time_per_event_sec: float = 1.0
    size_per_event_kb: float = 1.5

    # Site Configuration
    site_whitelist: List[str] = []
    site_blacklist: List[str] = []

    # Splitting Configuration
    splitting_algo: SplittingAlgo
    splitting_params: Dict[str, Any]

    # Metadata
    campaign: str
    processing_string: str
    acquisition_era: str
    processing_version: int
    priority: int = 100000
    urgent: bool = False            # Skip pilot if True

    # Payload Configuration
    payload_config: Dict[str, Any]  # Opaque to WMS2, passed to sandbox

    # Version Linkage (for catastrophic failure recovery)
    previous_version_request: Optional[str]    # request_name of v(N-1)
    superseded_by_request: Optional[str]       # request_name of v(N+1)
    cleanup_policy: CleanupPolicy = CleanupPolicy.KEEP_UNTIL_REPLACED

    # State
    status: RequestStatus
    status_transitions: List[StatusTransition]

    # Timestamps
    created_at: datetime
    updated_at: datetime


class RequestStatus(str, Enum):
    NEW = "new"                    # Requestor's workspace, WMS2 ignores
    SUBMITTED = "submitted"        # Requestor signals ready, WMS2 validates
    QUEUED = "queued"              # In admission queue, waiting for capacity
    PILOT_RUNNING = "pilot_running"  # Pilot job executing
    PLANNING = "planning"          # DAG Planner building production DAG
    ACTIVE = "active"              # DAG submitted to DAGMan
    STOPPING = "stopping"          # Clean stop in progress (condor_rm issued)
    RESUBMITTING = "resubmitting"  # Recovery DAG prepared, re-entering queue
    COMPLETED = "completed"        # All nodes succeeded, outputs registered
    PARTIAL = "partial"            # DAG finished with some failures, recoverable
    FAILED = "failed"              # Catastrophic failure or retries exhausted
    ABORTED = "aborted"            # Manually cancelled


class CleanupPolicy(str, Enum):
    """Controls handling of outputs from previous version during catastrophic recovery."""
    KEEP_UNTIL_REPLACED = "keep_until_replaced"    # Previous outputs remain until explicitly cleaned
    IMMEDIATE_CLEANUP = "immediate_cleanup"         # Previous outputs invalidated when new version completes


class SplittingAlgo(str, Enum):
    FILE_BASED = "FileBased"
    EVENT_BASED = "EventBased"
    LUMI_BASED = "LumiBased"
    EVENT_AWARE_LUMI = "EventAwareLumiBased"
```

Note: `RequestType` (ReReco, StepChain, etc.) is **not** a WMS2 concept. It is stored as metadata in `payload_config` and meaningful only to ReqMgr2 and the sandbox/wrapper. WMS2 treats all requests identically.

#### 3.1.2 Workflow

```python
class Workflow(BaseModel):
    """
    Internal workflow representation.
    Created from a Request after submission.
    """
    id: UUID
    request_name: str

    # Input/Output
    input_dataset: str
    input_fileset_id: Optional[UUID]
    output_fileset_id: Optional[UUID]

    # Splitting
    splitting_algo: SplittingAlgo
    splitting_params: Dict[str, Any]

    # Execution Config
    sandbox_url: str
    config_cache_id: str

    # Pilot tracking (set when pilot is submitted, cleared after completion)
    pilot_cluster_id: Optional[str]         # HTCondor cluster ID of pilot job
    pilot_schedd: Optional[str]             # Schedd that owns the pilot job
    pilot_output_path: Optional[str]        # Path to pilot metrics report

    # Pilot Results (populated after pilot completes)
    pilot_metrics: Optional[PilotMetrics]

    # DAG Reference (set after DAG submission)
    dag_id: Optional[UUID]

    # Category throttles (global defaults with optional per-workflow override)
    category_throttles: Dict[str, int] = {
        "Processing": 5000,
        "Merge": 100,
        "Cleanup": 50,
        "MergeGroup": 10,          # Concurrent merge groups (staggered site selection)
    }

    # Aggregate counters (populated from DAGMan status)
    total_nodes: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_queued: int = 0
    nodes_running: int = 0

    # State
    status: WorkflowStatus

    # Timestamps
    created_at: datetime
    updated_at: datetime


class WorkflowStatus(str, Enum):
    NEW = "new"
    PILOT_RUNNING = "pilot_running"
    PLANNING = "planning"
    ACTIVE = "active"
    STOPPING = "stopping"              # Clean stop in progress
    RESUBMITTING = "resubmitting"      # Recovery DAG prepared
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


class PilotMetrics(BaseModel):
    """
    Performance measurements from the pilot job.
    Used by DAG Planner for resource estimation and merge planning.
    """
    time_per_event_sec: float       # Measured across full step chain
    memory_peak_mb: int             # High-water mark across all steps
    output_size_per_event_kb: float # Final output size ratio
    cpu_efficiency: float           # CPU time / wall time
    events_processed: int           # Total events the pilot processed
    steps_profiled: List[StepProfile]  # Per-step breakdown

    # Recommended settings derived from measurements
    recommended_events_per_job: int
    recommended_memory_mb: int
    recommended_time_per_job_sec: int


class StepProfile(BaseModel):
    """Per-step performance from pilot."""
    step_name: str
    time_per_event_sec: float
    memory_peak_mb: int
    output_size_per_event_kb: float
    threads_used: int
```

#### 3.1.3 DAG (DAGMan Submission Unit)

WMS2 does **not** model individual HTCondor jobs as first-class entities. Instead, it models the **DAG** — the unit submitted to `condor_submit_dag`. Individual node status is read from DAGMan's own status files and logs rather than being tracked in the WMS2 database.

```python
class DAG(BaseModel):
    """
    A DAGMan DAG — the unit of submission to HTCondor.
    Each DAG corresponds to one workflow.
    WMS2 submits the DAG and monitors it; DAGMan owns the node lifecycle.
    """
    id: UUID
    workflow_id: UUID

    # DAGMan file paths (on submit host filesystem)
    dag_file_path: str
    submit_dir: str
    rescue_dag_path: Optional[str]

    # HTCondor tracking
    dagman_cluster_id: Optional[str]
    schedd_name: Optional[str]

    # Recovery linkage
    parent_dag_id: Optional[UUID]        # DAG this is a recovery of
    stop_requested_at: Optional[datetime]
    stop_reason: Optional[str]

    # DAG structure summary
    total_nodes: int
    total_edges: int = 0

    # Node breakdown by role
    node_counts: Dict[str, int]    # e.g. {"Processing": 9500, "Merge": 50, "Cleanup": 50}

    # Aggregate status (updated from DAGMan status polling)
    nodes_idle: int = 0
    nodes_running: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_held: int = 0

    # Merge group tracking — set of SUBDAG node names already reported
    # as completed. Used by DAG Monitor to avoid re-reporting.
    reported_merge_groups: List[str] = []

    # DAG-level state
    status: DAGStatus

    # Timestamps
    created_at: datetime
    submitted_at: Optional[datetime]
    completed_at: Optional[datetime]


class DAGStatus(str, Enum):
    PLANNING = "planning"
    READY = "ready"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL = "partial"            # Finished with some failures
    FAILED = "failed"
    REMOVED = "removed"
    HALTED = "halted"
    STOPPED = "stopped"            # Clean stop completed — eligible for rescue DAG recovery


class DAGNodeSpec(BaseModel):
    """
    Specification for a single node in the DAG file.
    Transient planning object — NOT stored in the DB per-node.
    Used during DAG file generation only.
    """
    node_name: str                 # Unique within DAG (e.g. "proc_000001")
    submit_file: str               # Path to .sub file
    node_role: NodeRole            # Processing, Merge, Cleanup

    # Input data for this node
    input_files: List[InputFile]
    input_events: int
    input_lumis: List[LumiRange]

    # Execution mask
    first_event: Optional[int]
    last_event: Optional[int]
    first_lumi: Optional[int]
    last_lumi: Optional[int]
    first_run: Optional[int]
    last_run: Optional[int]

    # Resource estimates (from pilot or requestor)
    estimated_time_sec: int
    estimated_disk_kb: int
    estimated_memory_mb: int
    cores: int = 1

    # Site constraints
    possible_sites: List[str]

    # Retry (DAGMan RETRY directive)
    max_retries: int = 3

    # POST script for per-node error handling
    post_script: str               # WMS2-provided recovery script

    # Dependencies
    parent_nodes: List[str] = []
    child_nodes: List[str] = []


class NodeRole(str, Enum):
    """Role of a node within the DAG structure."""
    PROCESSING = "Processing"
    MERGE = "Merge"
    CLEANUP = "Cleanup"


class InputFile(BaseModel):
    lfn: str
    size_bytes: int
    events: int
    checksums: Dict[str, str]
    locations: List[str]
    parent_lfns: List[str] = []


class LumiRange(BaseModel):
    run: int
    lumi_start: int
    lumi_end: int
```

#### 3.1.4 Output Dataset

```python
class OutputDataset(BaseModel):
    """
    Tracks an output dataset through the registration and transfer pipeline.
    Created when merge nodes complete.
    """
    id: UUID
    workflow_id: UUID
    dataset_name: str              # /Primary/Processed/Tier

    # DBS registration
    dbs_registered: bool = False
    dbs_registered_at: Optional[datetime]

    # Rucio source protection
    source_site: str
    source_rule_id: Optional[str]
    source_protected: bool = False

    # Rucio transfer
    transfer_rule_ids: List[str] = []
    transfer_destinations: List[str] = []
    transfers_complete: bool = False
    transfers_complete_at: Optional[datetime]
    last_transfer_check: Optional[datetime]  # Rate-limit transfer status polling

    # Source cleanup
    source_released: bool = False

    # Invalidation tracking (for catastrophic failure recovery)
    invalidated: bool = False
    invalidated_at: Optional[datetime]
    invalidation_reason: Optional[str]
    dbs_invalidated: bool = False
    rucio_rules_deleted: bool = False

    # State
    status: OutputStatus

    # Timestamps
    created_at: datetime
    updated_at: datetime


class OutputStatus(str, Enum):
    PENDING = "pending"
    DBS_REGISTERED = "dbs_registered"
    SOURCE_PROTECTED = "source_protected"
    TRANSFERS_REQUESTED = "transfers_requested"
    TRANSFERRING = "transferring"
    TRANSFERRED = "transferred"
    SOURCE_RELEASED = "source_released"
    ANNOUNCED = "announced"
    INVALIDATED = "invalidated"        # Outputs invalidated during catastrophic recovery
    FAILED = "failed"
```

#### 3.1.5 Site

```python
class Site(BaseModel):
    name: str                      # CMS site name (T1_US_FNAL, etc.)
    total_slots: int
    running_slots: int
    pending_slots: int
    status: SiteStatus
    drain_until: Optional[datetime]
    thresholds: Dict[str, int]
    schedd_name: str
    collector_host: str
    storage_element: str
    storage_path: str
    updated_at: datetime


class SiteStatus(str, Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    DRAINING = "draining"
    MAINTENANCE = "maintenance"
```

### 3.2 Database Schema (PostgreSQL)

```sql
CREATE TABLE requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_name VARCHAR(255) UNIQUE NOT NULL,
    requestor VARCHAR(100) NOT NULL,
    requestor_dn VARCHAR(500),
    request_data JSONB NOT NULL,
    payload_config JSONB DEFAULT '{}',
    splitting_params JSONB,
    input_dataset VARCHAR(500),
    campaign VARCHAR(100),
    priority INTEGER DEFAULT 100000,
    urgent BOOLEAN DEFAULT FALSE,
    status VARCHAR(50) NOT NULL DEFAULT 'new',
    status_transitions JSONB DEFAULT '[]',
    -- Version linkage (catastrophic failure recovery)
    previous_version_request VARCHAR(255) REFERENCES requests(request_name),
    superseded_by_request VARCHAR(255),
    cleanup_policy VARCHAR(50) DEFAULT 'keep_until_replaced',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE workflows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    request_name VARCHAR(255) REFERENCES requests(request_name),
    input_dataset VARCHAR(500),
    splitting_algo VARCHAR(50) NOT NULL,
    splitting_params JSONB,
    sandbox_url TEXT,
    config_data JSONB,
    pilot_cluster_id VARCHAR(50),
    pilot_schedd VARCHAR(255),
    pilot_output_path TEXT,
    pilot_metrics JSONB,
    dag_id UUID,
    category_throttles JSONB DEFAULT '{"Processing": 5000, "Merge": 100, "Cleanup": 50}',
    total_nodes INTEGER DEFAULT 0,
    nodes_done INTEGER DEFAULT 0,
    nodes_failed INTEGER DEFAULT 0,
    nodes_queued INTEGER DEFAULT 0,
    nodes_running INTEGER DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE dags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES workflows(id),
    dag_file_path TEXT NOT NULL,
    submit_dir TEXT NOT NULL,
    rescue_dag_path TEXT,
    dagman_cluster_id VARCHAR(50),
    schedd_name VARCHAR(255),
    -- Recovery linkage
    parent_dag_id UUID REFERENCES dags(id),
    stop_requested_at TIMESTAMPTZ,
    stop_reason TEXT,
    total_nodes INTEGER NOT NULL,
    total_edges INTEGER DEFAULT 0,
    node_counts JSONB DEFAULT '{}',
    nodes_idle INTEGER DEFAULT 0,
    nodes_running INTEGER DEFAULT 0,
    nodes_done INTEGER DEFAULT 0,
    nodes_failed INTEGER DEFAULT 0,
    nodes_held INTEGER DEFAULT 0,
    reported_merge_groups JSONB DEFAULT '[]',
    status VARCHAR(50) NOT NULL DEFAULT 'planning',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    submitted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE dag_history (
    id BIGSERIAL PRIMARY KEY,
    dag_id UUID REFERENCES dags(id),
    event_type VARCHAR(50) NOT NULL,
    from_status VARCHAR(50),
    to_status VARCHAR(50),
    nodes_done INTEGER,
    nodes_failed INTEGER,
    nodes_running INTEGER,
    detail JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE output_datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES workflows(id),
    dataset_name VARCHAR(500) NOT NULL,
    source_site VARCHAR(100),
    dbs_registered BOOLEAN DEFAULT FALSE,
    dbs_registered_at TIMESTAMPTZ,
    source_rule_id VARCHAR(100),
    source_protected BOOLEAN DEFAULT FALSE,
    transfer_rule_ids JSONB DEFAULT '[]',
    transfer_destinations JSONB DEFAULT '[]',
    transfers_complete BOOLEAN DEFAULT FALSE,
    transfers_complete_at TIMESTAMPTZ,
    last_transfer_check TIMESTAMPTZ,
    source_released BOOLEAN DEFAULT FALSE,
    -- Invalidation tracking (catastrophic failure recovery)
    invalidated BOOLEAN DEFAULT FALSE,
    invalidated_at TIMESTAMPTZ,
    invalidation_reason TEXT,
    dbs_invalidated BOOLEAN DEFAULT FALSE,
    rucio_rules_deleted BOOLEAN DEFAULT FALSE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE sites (
    name VARCHAR(100) PRIMARY KEY,
    total_slots INTEGER,
    running_slots INTEGER DEFAULT 0,
    pending_slots INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'enabled',
    drain_until TIMESTAMPTZ,
    thresholds JSONB DEFAULT '{}',
    schedd_name VARCHAR(255),
    collector_host VARCHAR(255),
    storage_element VARCHAR(255),
    storage_path TEXT,
    config_data JSONB DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX idx_dags_workflow ON dags(workflow_id);
CREATE INDEX idx_dags_status ON dags(status);
CREATE INDEX idx_dags_schedd ON dags(schedd_name) WHERE dagman_cluster_id IS NOT NULL;
CREATE INDEX idx_dags_parent ON dags(parent_dag_id) WHERE parent_dag_id IS NOT NULL;
CREATE INDEX idx_workflows_request ON workflows(request_name);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_requests_status ON requests(status);
CREATE INDEX idx_requests_campaign ON requests(campaign);
CREATE INDEX idx_requests_priority ON requests(priority);
CREATE INDEX idx_requests_non_terminal ON requests(status)
    WHERE status NOT IN ('completed', 'failed', 'aborted');
CREATE INDEX idx_requests_version_link ON requests(previous_version_request)
    WHERE previous_version_request IS NOT NULL;
CREATE INDEX idx_dag_history_dag ON dag_history(dag_id);
CREATE INDEX idx_output_datasets_workflow ON output_datasets(workflow_id);
CREATE INDEX idx_output_datasets_status ON output_datasets(status);
```

---

## 4. Core Components

### 4.1 Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      WMS2 Core Components                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Request Lifecycle Manager                                   │
│     - Single owner of the request state machine                │
│     - Main loop evaluates all non-terminal requests            │
│     - Dispatches work to component workers (2–9 below)         │
│     - Owns state transitions, timeout detection, recovery      │
│                                                                 │
│  2. Workflow Manager                                            │
│     - Import requests from ReqMgr2                              │
│     - Create internal workflows                                 │
│     - Manage workflow lifecycle                                 │
│                                                                 │
│  3. Admission Controller                                        │
│     - FIFO queue with priority ordering                         │
│     - Limit concurrent active DAGs                              │
│     - Prevent resource starvation and long tails                │
│                                                                 │
│  4. DAG Planner                                                 │
│     - Run pilot job (or use requestor estimates for urgent)     │
│     - Split input data into DAG node specs using pilot metrics  │
│     - Plan merge nodes based on actual output sizes             │
│     - Generate DAGMan .dag and .sub files                       │
│     - Submit DAGs via condor_submit_dag                         │
│                                                                 │
│  5. DAG Monitor                                                 │
│     - Poll DAGMan status files / condor_q for a single DAG     │
│     - Update aggregate DAG/workflow counters                    │
│     - Detect DAG completion, partial failure, or failure        │
│     - Return results to Lifecycle Manager for routing           │
│                                                                 │
│  6. Output Manager                                              │
│     - Register outputs in DBS                                   │
│     - Create Rucio source protection rules                      │
│     - Request Rucio transfers to final destinations             │
│     - Poll Rucio until transfers complete                       │
│     - Release source protection                                 │
│     - Invalidate outputs for catastrophic failure recovery      │
│     - Rate-limit all DBS/Rucio API calls globally               │
│     - Evaluate existing CMS micro-services before building      │
│                                                                 │
│  7. Site Manager                                                │
│     - Track site resources                                      │
│     - Manage site status (drain, disable)                       │
│     - Sync with CRIC                                            │
│                                                                 │
│  8. Error Handler (workflow-level only)                          │
│     - Classify DAG-level failures                               │
│     - Decide: submit rescue DAG, flag for review, or abort      │
│     - Per-node retry handled by DAGMan RETRY + POST scripts     │
│                                                                 │
│  9. Metrics Collector                                           │
│     - Workflow/DAG lifecycle metrics                             │
│     - Admission queue state                                     │
│     - Output Manager progress                                   │
│     - WMS2 service health                                       │
│     - Note: per-job and pool metrics handled externally          │
│                                                                 │
│  NOTE: Components 2–9 are workers called by the Request         │
│  Lifecycle Manager. They do not run independent polling loops.   │
│  Individual job submission, retry, hold/release, and dependency  │
│  sequencing are delegated entirely to HTCondor DAGMan. WMS2     │
│  does NOT track individual job state in its DB.                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Request Lifecycle Manager

The Request Lifecycle Manager is the single owner of every request's state machine. It runs a continuous main loop that queries all non-terminal requests from the database, evaluates each one based on its current status, and dispatches work to the appropriate component worker. This design:

- **Eliminates silent stalling**: Every non-terminal request is evaluated every cycle. If a component fails to make progress, the timeout detection catches it.
- **Simplifies restart recovery**: On startup, the main loop simply re-evaluates all non-terminal requests from the database. No in-memory state is required beyond the current cycle.
- **Centralizes state transitions**: All status changes go through `transition()`, producing a complete audit trail.

```python
class RequestLifecycleManager:
    """
    Single owner of the request state machine. Runs a continuous loop that
    evaluates all non-terminal requests and dispatches work to the appropriate
    component workers. Replaces the distributed polling loops that previously
    ran independently in AdmissionController, DAGMonitor, and OutputManager.

    Restart recovery is trivial: on startup, the main loop simply re-evaluates
    all non-terminal requests from the database. No in-memory state is required.
    """

    # Configurable timeout thresholds per status (seconds)
    STATUS_TIMEOUTS = {
        RequestStatus.SUBMITTED: 3600,          # 1 hour to validate and queue
        RequestStatus.QUEUED: 86400 * 7,        # 7 days in admission queue
        RequestStatus.PILOT_RUNNING: 86400,     # 24 hours for pilot
        RequestStatus.PLANNING: 3600,           # 1 hour for DAG planning
        RequestStatus.ACTIVE: 86400 * 30,       # 30 days for DAG execution
        RequestStatus.STOPPING: 3600,           # 1 hour for clean stop
        RequestStatus.RESUBMITTING: 600,        # 10 minutes to prepare recovery
    }

    def __init__(self, cycle_interval: int = 60):
        self.cycle_interval = cycle_interval

    # ── Main Loop ───────────────────────────────────────────────

    async def main_loop(self):
        """Main loop: query all non-terminal requests, evaluate each."""
        while True:
            try:
                requests = await self.db.get_non_terminal_requests()
                for request in requests:
                    try:
                        await self.evaluate_request(request)
                    except Exception as e:
                        logger.error(f"Error evaluating {request.request_name}: {e}")
                await asyncio.sleep(self.cycle_interval)
            except Exception as e:
                logger.error(f"Lifecycle manager cycle error: {e}")
                await asyncio.sleep(self.cycle_interval)

    async def evaluate_request(self, request: Request):
        """Match on current status, dispatch to the appropriate handler."""
        # Check for stuck states first
        if self._is_stuck(request):
            await self._handle_stuck(request)
            return

        handler = {
            RequestStatus.SUBMITTED: self._handle_submitted,
            RequestStatus.QUEUED: self._handle_queued,
            RequestStatus.PILOT_RUNNING: self._handle_pilot_running,
            RequestStatus.ACTIVE: self._handle_active,
            RequestStatus.STOPPING: self._handle_stopping,
            RequestStatus.RESUBMITTING: self._handle_resubmitting,
            RequestStatus.PARTIAL: self._handle_partial,
        }.get(request.status)

        if handler:
            await handler(request)

    # ── State Handlers ──────────────────────────────────────────

    async def _handle_submitted(self, request: Request):
        """Validate request and move to admission queue."""
        workflow = await self.workflow_manager.import_request(request.request_name)
        await self.transition(request, RequestStatus.QUEUED)

    async def _handle_queued(self, request: Request):
        """Check admission capacity and start pilot or planning."""
        if not await self.admission_controller.has_capacity():
            return  # Wait for capacity

        next_pending = await self.admission_controller.get_next_pending()
        if next_pending and next_pending.request_name != request.request_name:
            return  # Not this request's turn

        workflow = await self.db.get_workflow_by_request(request.request_name)
        if request.urgent:
            await self.dag_planner.plan_and_submit(workflow)
            await self.transition(request, RequestStatus.ACTIVE)
        else:
            await self.dag_planner.submit_pilot(workflow)
            await self.transition(request, RequestStatus.PILOT_RUNNING)

    async def _handle_pilot_running(self, request: Request):
        """Poll pilot status, trigger DAG planning on completion."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        completed = await self.condor_adapter.check_job_completed(
            workflow.pilot_cluster_id, workflow.pilot_schedd
        )
        if completed:
            await self.dag_planner.handle_pilot_completion(
                workflow, workflow.pilot_output_path
            )
            await self.transition(request, RequestStatus.ACTIVE)

    async def _handle_active(self, request: Request):
        """Poll DAG status, register/protect outputs as merge groups complete."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        if dag.status in (DAGStatus.SUBMITTED, DAGStatus.RUNNING):
            # DAG still running — poll it
            result = await self.dag_monitor.poll_dag(dag)

            if result.status == DAGStatus.RUNNING:
                for merge_info in result.newly_completed_merges:
                    await self.output_manager.handle_merge_completion(
                        workflow.id, merge_info
                    )
            elif result.status == DAGStatus.PARTIAL:
                await self.transition(request, RequestStatus.PARTIAL)
            elif result.status == DAGStatus.FAILED:
                await self.transition(request, RequestStatus.FAILED)

        # Process outputs every cycle — don't wait for DAG to finish.
        # As each merge group completes, its output needs DBS registration
        # and Rucio source protection immediately to prevent data loss.
        await self.output_manager.process_outputs_for_workflow(workflow.id)

        # Check if everything is done (DAG complete + all outputs announced)
        if dag.status == DAGStatus.COMPLETED:
            if await self._all_outputs_announced(workflow.id):
                await self.transition(request, RequestStatus.COMPLETED)
                # Clean up previous version outputs if this is a version increment
                if request.previous_version_request:
                    await self.output_manager.cleanup_previous_version_outputs(
                        request.previous_version_request, request.cleanup_policy
                    )

    async def _handle_stopping(self, request: Request):
        """Monitor clean stop progress, prepare recovery when done."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        dag = await self.db.get_dag(workflow.dag_id)

        # Check if condor_rm has completed (DAGMan process gone)
        dagman_status = await self.condor_adapter.query_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id
        )
        if dagman_status is None:
            # DAGMan process is gone — stop is complete
            await self.db.update_dag(dag.id, status=DAGStatus.STOPPED)
            await self._prepare_recovery(request, workflow, dag)

    async def _handle_resubmitting(self, request: Request):
        """Recovery DAG prepared, move back to admission queue."""
        await self.transition(request, RequestStatus.QUEUED)

    async def _handle_partial(self, request: Request):
        """Handle partial DAG completion — delegate to Error Handler."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        dag = await self.db.get_dag(workflow.dag_id)
        await self.error_handler.handle_dag_partial_failure(dag)

    # ── Clean Stop ──────────────────────────────────────────────

    async def initiate_clean_stop(self, request_name: str, reason: str):
        """
        Operator-triggered clean stop. Issues condor_rm to DAGMan,
        which writes a rescue DAG before exiting. The request transitions
        to STOPPING and will be picked up by _handle_stopping on next cycle.
        """
        request = await self.db.get_request(request_name)
        workflow = await self.db.get_workflow_by_request(request_name)
        dag = await self.db.get_dag(workflow.dag_id)

        # Remove the DAGMan job — DAGMan writes rescue DAG on exit
        await self.condor_adapter.remove_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id
        )
        await self.db.update_dag(dag.id,
            stop_requested_at=datetime.utcnow(), stop_reason=reason
        )
        await self.transition(request, RequestStatus.STOPPING)
        await self.db.update_workflow(workflow.id, status=WorkflowStatus.STOPPING)

    async def _prepare_recovery(self, request: Request, workflow, dag):
        """
        After clean stop completes, create a new DAG record pointing to
        the rescue DAG path. Transition to RESUBMITTING → QUEUED so the
        admission controller re-admits it.
        """
        rescue_path = f"{dag.dag_file_path}.rescue001"
        new_dag = DAG(
            workflow_id=workflow.id,
            dag_file_path=dag.dag_file_path,
            submit_dir=dag.submit_dir,
            rescue_dag_path=rescue_path,
            parent_dag_id=dag.id,
            total_nodes=dag.total_nodes,
            total_edges=dag.total_edges,
            node_counts=dag.node_counts,
            status=DAGStatus.READY,
        )
        await self.db.create_dag(new_dag)
        await self.db.update_workflow(workflow.id,
            dag_id=new_dag.id, status=WorkflowStatus.RESUBMITTING
        )
        await self.transition(request, RequestStatus.RESUBMITTING)

    # ── Catastrophic Failure Recovery ───────────────────────────

    async def handle_catastrophic_failure(self, request_name: str,
                                          cleanup_policy: CleanupPolicy):
        """
        Handle permanent schedd/infrastructure loss. Creates a new request
        with incremented processing_version and links it to the failed one.
        Outputs from the failed version are invalidated or preserved based
        on cleanup_policy.
        """
        request = await self.db.get_request(request_name)

        # Invalidate outputs from failed version
        workflow = await self.db.get_workflow_by_request(request_name)
        await self.output_manager.invalidate_outputs(
            workflow.id, reason=f"Catastrophic failure of {request_name}"
        )

        # Create version increment
        new_request = await self._create_version_increment(request, cleanup_policy)

        # Mark old request as failed
        await self.transition(request, RequestStatus.FAILED)

        return new_request

    async def _create_version_increment(self, request: Request,
                                        cleanup_policy: CleanupPolicy) -> Request:
        """
        Create a new request with processing_version+1. Link the old and
        new requests via previous_version_request / superseded_by_request.
        """
        new_version = request.processing_version + 1
        new_request_name = request.request_name.replace(
            f"_v{request.processing_version}_",
            f"_v{new_version}_"
        )

        new_request = Request(
            request_name=new_request_name,
            processing_version=new_version,
            previous_version_request=request.request_name,
            cleanup_policy=cleanup_policy,
            # Copy remaining fields from original request
            requestor=request.requestor,
            requestor_dn=request.requestor_dn,
            group=request.group,
            input_dataset=request.input_dataset,
            output_datasets=request.output_datasets,
            cmssw_version=request.cmssw_version,
            scram_arch=request.scram_arch,
            global_tag=request.global_tag,
            campaign=request.campaign,
            processing_string=request.processing_string,
            acquisition_era=request.acquisition_era,
            splitting_algo=request.splitting_algo,
            splitting_params=request.splitting_params,
            payload_config=request.payload_config,
            priority=request.priority,
            urgent=request.urgent,
            status=RequestStatus.SUBMITTED,
        )
        await self.db.create_request(new_request)

        # Link old request to new one
        await self.db.update_request(request.request_name,
            superseded_by_request=new_request_name
        )

        return new_request

    # ── Timeout Detection ───────────────────────────────────────

    def _is_stuck(self, request: Request) -> bool:
        """Check if a request has been in its current status too long."""
        timeout = self.STATUS_TIMEOUTS.get(request.status)
        if timeout is None:
            return False
        elapsed = (datetime.utcnow() - request.updated_at).total_seconds()
        return elapsed > timeout

    async def _handle_stuck(self, request: Request):
        """
        Handle a request that has exceeded its status timeout.
        Action depends on the status the request is stuck in:

        - SUBMITTED/PLANNING/RESUBMITTING: Internal operations that should be
          fast. Likely a bug or infrastructure issue. Retry the operation once,
          then fail if still stuck.
        - QUEUED: Normal — admission queue can be slow. Just alert.
        - PILOT_RUNNING: Pilot may have failed silently. Check if the job
          still exists; if not, resubmit or fail.
        - ACTIVE: DAG may be making slow progress (normal) or schedd may be
          unreachable (catastrophic). Probe the schedd before escalating.
        - STOPPING: condor_rm may have failed. Retry the removal.
        """
        elapsed = datetime.utcnow() - request.updated_at
        logger.warning(
            f"Request {request.request_name} stuck in {request.status} "
            f"for {elapsed.total_seconds():.0f}s"
        )
        self.metrics.increment("wms2_request_stuck_total",
            labels={"status": request.status.value}
        )

        if request.status in (RequestStatus.SUBMITTED, RequestStatus.PLANNING,
                               RequestStatus.RESUBMITTING):
            # Internal operations — should be fast. Mark as failed so operator
            # can investigate. These statuses don't involve external systems
            # where retry would help.
            await self.transition(request, RequestStatus.FAILED)

        elif request.status == RequestStatus.QUEUED:
            # Admission queue timeout — unusual but not catastrophic.
            # Alert only; operator may need to adjust capacity or priority.
            await self._send_alert(request, "stuck_in_queue",
                f"Request queued for {elapsed.total_seconds() / 86400:.1f} days")

        elif request.status == RequestStatus.PILOT_RUNNING:
            # Check if pilot job still exists in the schedd
            workflow = await self.db.get_workflow_by_request(request.request_name)
            job_exists = await self.condor_adapter.query_job(
                schedd_name=workflow.pilot_schedd,
                cluster_id=workflow.pilot_cluster_id,
            )
            if job_exists is None:
                # Pilot vanished without completion — fail the request
                await self.transition(request, RequestStatus.FAILED)

        elif request.status == RequestStatus.ACTIVE:
            # The critical case: is the schedd reachable?
            workflow = await self.db.get_workflow_by_request(request.request_name)
            dag = await self.db.get_dag(workflow.dag_id)
            schedd_reachable = await self.condor_adapter.ping_schedd(dag.schedd_name)

            if schedd_reachable:
                # Schedd is fine, DAG is just slow. Alert but don't intervene.
                await self._send_alert(request, "slow_dag",
                    f"DAG running for {elapsed.total_seconds() / 86400:.1f} days")
            else:
                # Schedd unreachable — potential catastrophic failure.
                # Don't auto-trigger catastrophic recovery; escalate to operator.
                # Operator can call POST /api/v1/requests/{name}/restart
                # with a cleanup_policy to confirm catastrophic recovery.
                await self._send_alert(request, "schedd_unreachable",
                    f"Schedd {dag.schedd_name} unreachable for "
                    f"{elapsed.total_seconds():.0f}s — may need catastrophic recovery")

        elif request.status == RequestStatus.STOPPING:
            # condor_rm may have failed — retry the removal
            await self.initiate_clean_stop(request, reason="retry after stuck stop")

    async def _send_alert(self, request: Request, alert_type: str, message: str):
        """Send an operator alert via the configured notification channel."""
        logger.error(f"ALERT [{alert_type}] {request.request_name}: {message}")
        self.metrics.increment("wms2_alerts_total",
            labels={"type": alert_type}
        )
        # TODO: integrate with operator notification (email, Slack, PagerDuty)

    # ── Helpers ─────────────────────────────────────────────────

    async def _all_outputs_announced(self, workflow_id: UUID) -> bool:
        """Check if all outputs for a workflow have reached ANNOUNCED status."""
        outputs = await self.db.get_output_datasets(workflow_id)
        return all(o.status == OutputStatus.ANNOUNCED for o in outputs)

    # ── State Transition ────────────────────────────────────────

    async def transition(self, request: Request, new_status: RequestStatus):
        """Record a state transition with timestamp."""
        await self.db.update_request(request.request_name,
            status=new_status,
            status_transitions=request.status_transitions + [{
                "from": request.status.value,
                "to": new_status.value,
                "timestamp": datetime.utcnow().isoformat(),
            }],
            updated_at=datetime.utcnow(),
        )
```

### 4.3 Workflow Manager

```python
class WorkflowManager:
    async def import_request(self, request_name: str) -> Workflow:
        request_data = await self.reqmgr_adapter.get_request(request_name)
        self._validate_request(request_data)

        workflow = Workflow(
            request_name=request_name,
            input_dataset=request_data["InputDataset"],
            splitting_algo=request_data["SplittingAlgo"],
            splitting_params=request_data.get("SplittingParams", {}),
            sandbox_url=request_data["SandboxUrl"],
            status=WorkflowStatus.NEW,
        )
        await self.db.create_workflow(workflow)
        return workflow

    async def get_workflow_status(self, workflow_id: UUID) -> WorkflowStatusReport:
        workflow = await self.db.get_workflow(workflow_id)
        dag = await self.db.get_dag(workflow.dag_id) if workflow.dag_id else None
        outputs = await self.db.get_output_datasets(workflow_id)
        return WorkflowStatusReport(
            workflow=workflow, dag=dag, outputs=outputs,
            progress_percent=self._calc_progress(dag, outputs),
            estimated_completion=self._estimate_completion(workflow, dag, outputs),
        )
```

### 4.4 Admission Controller

```python
class AdmissionController:
    """
    Controls the rate of DAG submissions to prevent resource starvation.
    Priority-ordered FIFO with a concurrency limit.

    Called by the Request Lifecycle Manager — not an independent loop.
    """
    def __init__(self, max_active_dags: int = 300):
        self.max_active_dags = max_active_dags

    async def has_capacity(self) -> bool:
        """Check if there is capacity to admit another DAG."""
        active_count = await self.db.count_active_dags()
        return active_count < self.max_active_dags

    async def get_next_pending(self) -> Optional[Workflow]:
        """Get the next workflow to admit, ordered by priority then age."""
        pending = await self.db.get_queued_workflows(
            limit=1,
            order_by=["priority DESC", "created_at ASC"],
        )
        return pending[0] if pending else None
```

### 4.5 DAG Planner

```python
class DAGPlanner:
    """
    Plans and submits DAGMan DAGs for workflows.
    Two-phase model: pilot first, then production DAG.
    """
    SPLITTERS = {
        SplittingAlgo.FILE_BASED: FileBasedSplitter,
        SplittingAlgo.EVENT_BASED: EventBasedSplitter,
        SplittingAlgo.LUMI_BASED: LumiBasedSplitter,
        SplittingAlgo.EVENT_AWARE_LUMI: EventAwareLumiSplitter,
    }

    # ── Pilot Phase ──────────────────────────────────────────────

    async def submit_pilot(self, workflow: Workflow):
        """Submit pilot job that measures performance iteratively."""
        input_files = await self.dbs_adapter.get_files(
            dataset=workflow.input_dataset, limit=5,
        )
        pilot_dir = self._create_submit_dir(workflow, pilot=True)
        pilot_sub = self._write_pilot_submit_file(workflow, input_files, pilot_dir)
        cluster_id, schedd = await self.condor_adapter.submit_job(pilot_sub)
        pilot_output_path = f"{pilot_dir}/pilot_metrics.json"
        await self.db.update_workflow(workflow.id,
            pilot_cluster_id=cluster_id,
            pilot_schedd=schedd,
            pilot_output_path=pilot_output_path,
            status=WorkflowStatus.PILOT_RUNNING,
        )

    async def handle_pilot_completion(self, workflow: Workflow, pilot_output_path: str):
        """Parse pilot metrics and trigger production DAG planning."""
        metrics = self._parse_pilot_report(pilot_output_path)
        await self.db.update_workflow(workflow.id, pilot_metrics=metrics, status=WorkflowStatus.PLANNING)
        await self.plan_and_submit(workflow, metrics=metrics)

    def _parse_pilot_report(self, pilot_output_path: str) -> PilotMetrics:
        """
        Parse the pilot's JSON performance report into PilotMetrics.

        The pilot (running inside the sandbox in pilot mode) writes a
        structured JSON report — see Section 5.2 for the schema. The
        "recommended" block is computed by the pilot itself based on
        convergence of its iterative measurements. We trust the pilot's
        recommendations for events_per_job, memory_mb, and time_per_job.

        The key value for merge group planning is output_size_per_event_kb:
        this drives _plan_merge_groups() to compute how many processing
        nodes can feed one merge node without exceeding the target merged
        file size (default 4 GB).
        """
        with open(pilot_output_path) as f:
            data = json.load(f)

        recommended = data["recommended"]
        last_iteration = data["iterations"][-1]

        steps = [
            StepProfile(
                step_name=s["step"],
                time_per_event_sec=s["time_per_event"],
                memory_peak_mb=s["memory_mb"],
                output_size_per_event_kb=s.get("output_size_per_event_kb", 0),
                threads_used=s.get("threads", 1),
            )
            for s in data.get("per_step", [])
        ]

        return PilotMetrics(
            time_per_event_sec=last_iteration["wall_time_sec"] / last_iteration["events"],
            memory_peak_mb=max(it["memory_mb"] for it in data["iterations"]),
            output_size_per_event_kb=recommended["output_size_per_event_kb"],
            cpu_efficiency=last_iteration.get("cpu_efficiency", 0.9),
            events_processed=sum(it["events"] for it in data["iterations"]),
            steps_profiled=steps,
            recommended_events_per_job=recommended["events_per_job"],
            recommended_memory_mb=recommended["memory_mb"],
            recommended_time_per_job_sec=recommended["time_per_job_sec"],
        )

    # ── Production DAG Phase ─────────────────────────────────────

    async def plan_and_submit(self, workflow: Workflow, metrics: Optional[PilotMetrics] = None) -> DAG:
        """Build and submit the production DAG."""
        # 1. Determine resource parameters
        if metrics:
            events_per_job = metrics.recommended_events_per_job
            memory_mb = metrics.recommended_memory_mb
            time_per_job = metrics.recommended_time_per_job_sec
            output_size_per_event = metrics.output_size_per_event_kb
        else:
            request = await self.db.get_request(workflow.request_name)
            events_per_job = request.splitting_params.get("events_per_job", 10000)
            memory_mb = request.memory_mb
            time_per_job = int(request.time_per_event_sec * events_per_job)
            output_size_per_event = request.size_per_event_kb

        # 2. Fetch input files and locations
        input_files = await self.dbs_adapter.get_files(
            dataset=workflow.input_dataset,
            run_whitelist=workflow.splitting_params.get("run_whitelist"),
            lumi_mask=workflow.splitting_params.get("lumi_mask"),
        )
        locations = await self.rucio_adapter.get_replicas(
            lfns=[f.lfn for f in input_files]
        )
        for f in input_files:
            f.locations = locations.get(f.lfn, [])

        # 3. Split into processing nodes
        splitter_cls = self.SPLITTERS[workflow.splitting_algo]
        splitter = splitter_cls(**workflow.splitting_params)
        processing_nodes = splitter.split(
            files=input_files, workflow=workflow,
            memory_mb=memory_mb, time_per_job=time_per_job,
        )

        # 4. Plan merge groups (each group = processing nodes + merge + cleanup)
        merge_groups = self._plan_merge_groups(
            processing_nodes, workflow, output_size_per_event
        )

        total_nodes = sum(g.total_nodes for g in merge_groups)

        # 5. Generate DAG files (outer DAG + per-group sub-DAGs)
        submit_dir = self._create_submit_dir(workflow)
        dag_file_path = self._generate_dag_files(merge_groups, submit_dir, workflow)

        # 6. Create and submit DAG
        dag = DAG(
            workflow_id=workflow.id, dag_file_path=dag_file_path,
            submit_dir=submit_dir, total_nodes=total_nodes,
            total_edges=sum(len(g.processing_nodes) for g in merge_groups),
            node_counts={
                "Processing": sum(len(g.processing_nodes) for g in merge_groups),
                "Merge": len(merge_groups),
                "Cleanup": len(merge_groups),
                "Landing": len(merge_groups),
            },
            status=DAGStatus.READY,
        )
        await self.db.create_dag(dag)
        dagman_cluster_id, schedd_name = await self._submit_dag(dag)

        await self.db.update_dag(dag.id,
            dagman_cluster_id=dagman_cluster_id, schedd_name=schedd_name,
            status=DAGStatus.SUBMITTED, submitted_at=datetime.utcnow(),
        )
        await self.db.update_workflow(workflow.id,
            dag_id=dag.id, total_nodes=total_nodes, status=WorkflowStatus.ACTIVE,
        )
        await self.db.update_request(workflow.request_name, status=RequestStatus.ACTIVE)
        return dag

    # ── Merge Group Planning ─────────────────────────────────────

    def _plan_merge_groups(self, processing_nodes, workflow, output_size_per_event_kb,
                           target_merged_size_kb=4_000_000) -> List[MergeGroup]:
        """
        Group processing nodes into merge groups based on expected output size.
        Each merge group becomes a SUBDAG EXTERNAL containing:
          landing node → processing nodes → merge node → cleanup node
        Group composition is fixed at planning time (from pilot metrics).
        Site assignment is deferred to runtime (landing node mechanism).
        """
        groups = []
        current_procs = []
        current_size = 0

        for proc in processing_nodes:
            est_output = proc.input_events * output_size_per_event_kb
            if current_size + est_output > target_merged_size_kb and current_procs:
                groups.append(MergeGroup(
                    group_index=len(groups),
                    processing_nodes=current_procs,
                    estimated_output_kb=int(current_size),
                ))
                current_procs, current_size = [], 0
            current_procs.append(proc)
            current_size += est_output

        if current_procs:
            groups.append(MergeGroup(
                group_index=len(groups),
                processing_nodes=current_procs,
                estimated_output_kb=int(current_size),
            ))
        return groups


class MergeGroup(BaseModel):
    """A self-contained group of processing nodes + merge + cleanup."""
    group_index: int
    processing_nodes: List[DAGNodeSpec]
    estimated_output_kb: int

    @property
    def total_nodes(self) -> int:
        return len(self.processing_nodes) + 3  # + landing + merge + cleanup

    # ── DAG File Generation ──────────────────────────────────────

    def _generate_dag_files(self, merge_groups, submit_dir, workflow) -> str:
        """
        Generate the outer DAG (SUBDAG EXTERNAL entries) and per-group
        sub-DAG files. Each group directory contains its own .dag file,
        submit files, and site-pinning scripts.
        """
        dag_file_path = os.path.join(submit_dir, "workflow.dag")

        # Generate per-group sub-DAG files
        for group in merge_groups:
            self._generate_group_dag(group, submit_dir, workflow)

        # Write shared site-pinning scripts
        self._write_site_scripts(submit_dir)

        # Generate outer DAG
        with open(dag_file_path, "w") as f:
            f.write(f"CONFIG {os.path.join(submit_dir, 'dagman.config')}\n\n")

            for group in merge_groups:
                group_dir = os.path.join(submit_dir, f"mg_{group.group_index:06d}")
                f.write(f"SUBDAG EXTERNAL mg_{group.group_index:06d} "
                        f"{os.path.join(group_dir, 'group.dag')}\n")
            f.write("\n")

            # Throttle concurrent merge groups to enable staggered site selection
            for group in merge_groups:
                f.write(f"CATEGORY mg_{group.group_index:06d} MergeGroup\n")
            f.write("\n")
            f.write(f"MAXJOBS MergeGroup {workflow.category_throttles.get('MergeGroup', 10)}\n")

        self._write_dagman_config(submit_dir)
        return dag_file_path

    def _generate_group_dag(self, group: MergeGroup, submit_dir, workflow):
        """Generate a self-contained sub-DAG for one merge group."""
        group_dir = os.path.join(submit_dir, f"mg_{group.group_index:06d}")
        os.makedirs(group_dir, exist_ok=True)

        group_dag_path = os.path.join(group_dir, "group.dag")
        gidx = group.group_index

        # Write submit files for all nodes in the group
        landing_sub = os.path.join(group_dir, "landing.sub")
        self._write_landing_submit_file(landing_sub)

        for proc in group.processing_nodes:
            sub_path = os.path.join(group_dir, f"{proc.node_name}.sub")
            proc.submit_file = sub_path
            self._write_submit_file(proc, sub_path, workflow)

        merge_sub = os.path.join(group_dir, "merge.sub")
        self._write_merge_submit_file(merge_sub, group, workflow)

        cleanup_sub = os.path.join(group_dir, "cleanup.sub")
        self._write_cleanup_submit_file(cleanup_sub, group, workflow)

        # Write group DAG file
        proc_names = [p.node_name for p in group.processing_nodes]

        with open(group_dag_path, "w") as f:
            # Landing node — HTCondor picks the site
            f.write(f"JOB landing {landing_sub}\n")
            f.write(f"SCRIPT POST landing {submit_dir}/elect_site.sh "
                    f"{group_dir}/elected_site\n\n")

            # Processing nodes — PRE script pins to elected site
            for proc in group.processing_nodes:
                f.write(f"JOB {proc.node_name} {proc.submit_file}\n")
                f.write(f"SCRIPT PRE {proc.node_name} {submit_dir}/pin_site.sh "
                        f"{proc.submit_file} {group_dir}/elected_site\n")
                if proc.post_script:
                    f.write(f"SCRIPT POST {proc.node_name} "
                            f"{os.path.join(submit_dir, proc.post_script)} "
                            f"{proc.node_name} $RETURN\n")
            f.write("\n")

            # Merge node
            f.write(f"JOB merge {merge_sub}\n")
            f.write(f"SCRIPT PRE merge {submit_dir}/pin_site.sh "
                    f"{merge_sub} {group_dir}/elected_site\n\n")

            # Cleanup node
            f.write(f"JOB cleanup {cleanup_sub}\n")
            f.write(f"SCRIPT PRE cleanup {submit_dir}/pin_site.sh "
                    f"{cleanup_sub} {group_dir}/elected_site\n\n")

            # Retries
            for proc in group.processing_nodes:
                f.write(f"RETRY {proc.node_name} {proc.max_retries} UNLESS-EXIT 2\n")
            f.write("RETRY merge 2 UNLESS-EXIT 2\n")
            f.write("RETRY cleanup 1\n\n")

            # Dependencies: landing → processing → merge → cleanup
            f.write(f"PARENT landing CHILD {' '.join(proc_names)}\n")
            f.write(f"PARENT {' '.join(proc_names)} CHILD merge\n")
            f.write(f"PARENT merge CHILD cleanup\n\n")

            # Category throttles within the group
            for proc in group.processing_nodes:
                f.write(f"CATEGORY {proc.node_name} Processing\n")
            f.write("CATEGORY merge Merge\n")
            f.write("CATEGORY cleanup Cleanup\n\n")
            for role, limit in workflow.category_throttles.items():
                if role != "MergeGroup":
                    f.write(f"MAXJOBS {role} {limit}\n")

    def _write_landing_submit_file(self, path):
        """Landing node: trivial job that lets HTCondor pick the site."""
        content = """universe = vanilla
executable = /bin/true
request_memory = 1
request_disk = 1
+WMS2_LandingNode = true
output = landing.out
error = landing.err
log = cluster.log
queue 1
"""
        with open(path, "w") as f:
            f.write(content)

    def _write_site_scripts(self, submit_dir):
        """Write the shared site-election and site-pinning scripts."""
        # elect_site.sh — POST script for landing node
        # Reads MATCH_GLIDEIN_CMSSite from the completed job's classad
        elect_script = os.path.join(submit_dir, "elect_site.sh")
        with open(elect_script, "w") as f:
            f.write("""#!/bin/bash
# elect_site.sh — called as POST script after landing node completes
# $1 = path to write elected site
ELECTED_SITE_FILE=$1
CLUSTER_ID=$(condor_q -format "%d.0" ClusterId -constraint 'DAGNodeName=="landing"' 2>/dev/null)
SITE=$(condor_history $CLUSTER_ID -limit 1 -af MATCH_GLIDEIN_CMSSite 2>/dev/null)
if [ -n "$SITE" ]; then
    echo "$SITE" > "$ELECTED_SITE_FILE"
    exit 0
else
    echo "ERROR: Could not determine site from landing job" >&2
    exit 1
fi
""")
        os.chmod(elect_script, 0o755)

        # pin_site.sh — PRE script for processing/merge/cleanup nodes
        # Rewrites +DESIRED_Sites in the submit file to the elected site
        pin_script = os.path.join(submit_dir, "pin_site.sh")
        with open(pin_script, "w") as f:
            f.write("""#!/bin/bash
# pin_site.sh — called as PRE script before each node in the group
# $1 = submit file path, $2 = elected site file path
SUBMIT_FILE=$1
ELECTED_SITE_FILE=$2
SITE=$(cat "$ELECTED_SITE_FILE")
sed -i "s/+DESIRED_Sites = .*/+DESIRED_Sites = \\"${SITE}\\"/" "$SUBMIT_FILE"
exit 0
""")
        os.chmod(pin_script, 0o755)

    def _write_submit_file(self, node, path, workflow):
        content = f"""universe = vanilla
executable = {workflow.sandbox_url}/run.sh
arguments = {node.node_name}

request_memory = {node.estimated_memory_mb}
request_disk = {node.estimated_disk_kb}
request_cpus = {node.cores}
+MaxWallTimeMins = {node.estimated_time_sec // 60 + 1}

+DESIRED_Sites = "{','.join(node.possible_sites)}"
+CMS_RequestName = "{workflow.request_name}"
+CMS_WorkflowID = "{workflow.id}"
+CMS_NodeRole = "{node.node_role.value}"

transfer_input_files = {workflow.sandbox_url}
should_transfer_files = YES
when_to_transfer_output = ON_EXIT

output = {node.node_name}.out
error = {node.node_name}.err
log = cluster.log

queue 1
"""
        with open(path, "w") as f:
            f.write(content)

    def _write_dagman_config(self, submit_dir):
        config_path = os.path.join(submit_dir, "dagman.config")
        with open(config_path, "w") as f:
            f.write("DAGMAN_MAX_SUBMITS_PER_INTERVAL = 100\n")
            f.write("DAGMAN_USER_LOG_SCAN_INTERVAL = 5\n")

    async def _submit_dag(self, dag):
        return await self.condor_adapter.submit_dag(
            dag_file=dag.dag_file_path, submit_dir=dag.submit_dir,
        )
```

#### 4.5.1 Splitting Algorithms

```python
class FileBasedSplitter:
    def __init__(self, files_per_job: int = 5, **kwargs):
        self.files_per_job = files_per_job

    def split(self, files, workflow, **kwargs) -> List[DAGNodeSpec]:
        by_location = self._group_by_location(files)
        nodes = []
        idx = 0
        for site, site_files in by_location.items():
            for i in range(0, len(site_files), self.files_per_job):
                batch = site_files[i : i + self.files_per_job]
                nodes.append(DAGNodeSpec(
                    node_name=f"proc_{idx:06d}", submit_file="",
                    node_role=NodeRole.PROCESSING, input_files=batch,
                    input_events=sum(f.events for f in batch), input_lumis=[],
                    estimated_time_sec=kwargs.get("time_per_job", 3600),
                    estimated_disk_kb=sum(f.size_bytes // 1024 for f in batch) * 2,
                    estimated_memory_mb=kwargs.get("memory_mb", 2048),
                    cores=1, possible_sites=[site], max_retries=3,
                    post_script="post_script.sh",
                ))
                idx += 1
        return nodes


class EventBasedSplitter:
    def __init__(self, events_per_job: int = 100000, **kwargs):
        self.events_per_job = events_per_job

    def split(self, files, workflow, **kwargs) -> List[DAGNodeSpec]:
        nodes = []
        idx = 0
        for file in files:
            events_processed = 0
            while events_processed < file.events:
                n = min(self.events_per_job, file.events - events_processed)
                nodes.append(DAGNodeSpec(
                    node_name=f"proc_{idx:06d}", submit_file="",
                    node_role=NodeRole.PROCESSING, input_files=[file],
                    input_events=n, input_lumis=[],
                    first_event=events_processed + 1,
                    last_event=events_processed + n,
                    estimated_time_sec=kwargs.get("time_per_job", 3600),
                    estimated_disk_kb=file.size_bytes // 1024 * 2,
                    estimated_memory_mb=kwargs.get("memory_mb", 2048),
                    cores=1, possible_sites=file.locations, max_retries=3,
                    post_script="post_script.sh",
                ))
                events_processed += n
                idx += 1
        return nodes
```

### 4.6 DAG Monitor

The DAG Monitor reads DAGMan's status file to determine per-node and aggregate progress. For merge group detection, it tracks which SUBDAG nodes have completed since the last poll by comparing against a persistent set of already-reported completions stored in the database.

DAGMan writes a JSON status file (`<dagfile>.status`) that includes per-node status. The top-level DAG's nodes include the SUBDAG EXTERNAL entries — each merge group appears as a single node. When a SUBDAG node transitions to `STATUS_DONE`, its merge output is ready for the Output Manager.

```python
class NodeSummary(BaseModel):
    """Aggregate and per-node status parsed from DAGMan's .status file."""
    # Aggregates
    idle: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    held: int = 0
    # Per-node detail (node_name → status string)
    node_statuses: Dict[str, str] = {}


class DAGPollResult(BaseModel):
    """Result of polling a single DAG — returned to Lifecycle Manager."""
    dag_id: UUID
    status: DAGStatus
    nodes_idle: int = 0
    nodes_running: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_held: int = 0
    newly_completed_merges: List[Dict] = []


class DAGMonitor:
    """
    Polls DAGMan status for a single DAG and returns results.
    Called by the Request Lifecycle Manager — not an independent loop.

    Merge group detection: each merge group is a SUBDAG EXTERNAL node in the
    top-level DAG. When a SUBDAG node status changes to STATUS_DONE, the
    group's merge output is ready. The monitor compares current SUBDAG node
    completions against a DB-stored set (dag.reported_merge_groups) to find
    newly completed groups, avoiding duplicate reports across polling cycles.
    """

    async def poll_dag(self, dag: DAG) -> DAGPollResult:
        """
        Poll a single DAG's status from DAGMan. Returns a DAGPollResult
        instead of having side effects — the Lifecycle Manager handles
        routing results to the appropriate component.
        """
        dagman_status = await self.condor_adapter.query_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id,
        )
        if dagman_status is None:
            return await self._handle_dag_completion(dag)

        status_file = f"{dag.dag_file_path}.status"
        summary = await self._parse_dagman_status(status_file)

        await self.db.update_dag(dag.id,
            nodes_idle=summary.idle, nodes_running=summary.running,
            nodes_done=summary.done, nodes_failed=summary.failed,
            nodes_held=summary.held, status=DAGStatus.RUNNING,
        )
        await self.db.update_workflow(dag.workflow_id,
            nodes_done=summary.done, nodes_failed=summary.failed,
            nodes_running=summary.running, nodes_queued=summary.idle,
        )

        # Detect merge groups that completed since the last poll
        newly_completed_merges = await self._detect_completed_merges(dag, summary)

        return DAGPollResult(
            dag_id=dag.id, status=DAGStatus.RUNNING,
            nodes_idle=summary.idle, nodes_running=summary.running,
            nodes_done=summary.done, nodes_failed=summary.failed,
            nodes_held=summary.held,
            newly_completed_merges=newly_completed_merges,
        )

    async def _detect_completed_merges(self, dag: DAG,
                                       summary: NodeSummary) -> List[Dict]:
        """
        Compare current per-node statuses against the set of merge groups
        already reported as completed (stored in dag.reported_merge_groups).

        SUBDAG EXTERNAL nodes are named "MergeGroup_<N>" by the DAG Planner
        (see Section 4.5). When such a node reaches STATUS_DONE, we read its
        sub-DAG's output manifest to get the dataset name and site, then
        record it as reported so it's not re-emitted on the next cycle.

        Returns a list of dicts suitable for OutputManager.handle_merge_completion().
        """
        already_reported = set(dag.reported_merge_groups or [])
        newly_completed = []

        for node_name, status in summary.node_statuses.items():
            if not node_name.startswith("MergeGroup_"):
                continue
            if status != "STATUS_DONE":
                continue
            if node_name in already_reported:
                continue

            # Read the merge group's output manifest written by its cleanup node.
            # The manifest contains the dataset name and site where the merged
            # output was produced (written by POST script of the merge node).
            manifest = await self._read_merge_manifest(dag, node_name)
            if manifest:
                newly_completed.append({
                    "node_name": node_name,
                    "dataset_name": manifest["dataset_name"],
                    "site": manifest["site"],
                    "merge_output_lfn": manifest.get("output_lfn"),
                })
            already_reported.add(node_name)

        # Persist so we don't re-report on the next poll cycle
        if newly_completed:
            await self.db.update_dag(dag.id,
                reported_merge_groups=list(already_reported),
            )

        return newly_completed

    async def _read_merge_manifest(self, dag: DAG, group_node_name: str) -> Optional[Dict]:
        """
        Read the output manifest for a completed merge group.

        Each merge group's cleanup node writes a JSON manifest at a
        predictable path: <submit_dir>/<group_name>/merge_output.json
        containing: dataset_name, site, output_lfn, file_count, total_bytes.
        """
        manifest_path = f"{dag.submit_dir}/{group_node_name}/merge_output.json"
        try:
            content = await aiofiles.read(manifest_path)
            return json.loads(content)
        except FileNotFoundError:
            logger.warning(f"Merge manifest not found: {manifest_path}")
            return None

    async def _handle_dag_completion(self, dag: DAG) -> DAGPollResult:
        """DAGMan process is gone — read final metrics to determine outcome."""
        metrics_file = f"{dag.dag_file_path}.metrics"
        final = await self._parse_dagman_metrics(metrics_file)

        if final.nodes_failed == 0:
            new_status = DAGStatus.COMPLETED
        elif final.nodes_done > 0:
            new_status = DAGStatus.PARTIAL
        else:
            new_status = DAGStatus.FAILED

        await self.db.update_dag(dag.id,
            status=new_status, nodes_done=final.nodes_done,
            nodes_failed=final.nodes_failed, completed_at=datetime.utcnow(),
        )

        return DAGPollResult(
            dag_id=dag.id, status=new_status,
            nodes_done=final.nodes_done, nodes_failed=final.nodes_failed,
        )

    async def _parse_dagman_status(self, status_file: str) -> NodeSummary:
        """
        Parse DAGMan's .status file (JSON format).

        DAGMan writes this file periodically. Structure:
            {
                "DagStatus": {
                    "NodesTotal": 100,
                    "NodesDone": 42,
                    "NodesFailed": 0,
                    "NodesIdle": 30,
                    "NodesRunning": 28,
                    "NodesHeld": 0
                },
                "Nodes": {
                    "MergeGroup_0": {"NodeStatus": "STATUS_DONE"},
                    "MergeGroup_1": {"NodeStatus": "STATUS_RUNNING"},
                    ...
                }
            }
        """
        content = await aiofiles.read(status_file)
        data = json.loads(content)

        dag_status = data.get("DagStatus", {})
        nodes = data.get("Nodes", {})

        node_statuses = {
            name: info.get("NodeStatus", "STATUS_NOT_READY")
            for name, info in nodes.items()
        }

        return NodeSummary(
            idle=dag_status.get("NodesIdle", 0),
            running=dag_status.get("NodesRunning", 0),
            done=dag_status.get("NodesDone", 0),
            failed=dag_status.get("NodesFailed", 0),
            held=dag_status.get("NodesHeld", 0),
            node_statuses=node_statuses,
        )

    async def _parse_dagman_metrics(self, metrics_file: str) -> NodeSummary:
        """
        Parse the DAGMan .metrics file written at DAG completion.
        Same structure as .status but guaranteed to be final.
        """
        content = await aiofiles.read(metrics_file)
        data = json.loads(content)
        dag_status = data.get("DagStatus", {})
        return NodeSummary(
            done=dag_status.get("NodesDone", 0),
            failed=dag_status.get("NodesFailed", 0),
        )
```

### 4.7 Output Manager

The Output Manager handles the post-compute pipeline: DBS registration, Rucio source protection, transfer requests, transfer monitoring, and source release. This is the one area where WMS2 cannot be thin — it owns a long-lived, stateful process that outlives DAG execution and may take hours or days.

**Important**: Before building this component, existing CMS micro-services that handle output registration and data placement should be evaluated. If they are reusable with clean APIs, the Output Manager becomes a thin orchestrator calling those services.

```python
class OutputManager:
    """
    Handles the post-compute pipeline: DBS registration, Rucio source
    protection, transfer requests, transfer monitoring, and source release.

    Called by the Request Lifecycle Manager — not an independent loop.
    """

    async def handle_merge_completion(self, workflow_id, merge_info):
        output = OutputDataset(
            workflow_id=workflow_id,
            dataset_name=merge_info["dataset_name"],
            source_site=merge_info["site"],
            status=OutputStatus.PENDING,
        )
        await self.db.create_output_dataset(output)

    # Transfer status polling interval — avoid hammering Rucio every cycle
    TRANSFER_POLL_INTERVAL = timedelta(minutes=5)

    async def process_outputs_for_workflow(self, workflow_id: UUID):
        """
        Process all outputs for a single workflow through the output pipeline.

        Priority ordering: DBS registration and Rucio source protection are
        urgent (prevent data loss if the source site cleans up scratch space).
        Transfer requests are next. Transfer status polling is lowest priority
        and rate-limited to avoid excessive Rucio API calls.

        Skips terminal outputs (ANNOUNCED, INVALIDATED, SOURCE_RELEASED).
        """
        outputs = await self.db.get_output_datasets(workflow_id)

        # Partition into priority tiers — process urgent work first
        urgent = []        # PENDING, DBS_REGISTERED (need protection ASAP)
        transfers = []     # SOURCE_PROTECTED (need transfer rules created)
        polling = []       # TRANSFERS_REQUESTED, TRANSFERRING (can wait)
        for output in outputs:
            if output.status in (OutputStatus.PENDING, OutputStatus.DBS_REGISTERED):
                urgent.append(output)
            elif output.status == OutputStatus.SOURCE_PROTECTED:
                transfers.append(output)
            elif output.status in (OutputStatus.TRANSFERS_REQUESTED,
                                   OutputStatus.TRANSFERRING):
                polling.append(output)
            # Skip terminal: ANNOUNCED, INVALIDATED, SOURCE_RELEASED

        # Tier 1: DBS registration + source protection (highest priority)
        for output in urgent:
            try:
                if output.status == OutputStatus.PENDING:
                    await self._register_in_dbs(output)
                elif output.status == OutputStatus.DBS_REGISTERED:
                    await self._protect_source(output)
            except RateLimitExceeded:
                return  # Back off entirely — retry next cycle
            except Exception as e:
                logger.error(f"Output {output.id} protection failed: {e}")

        # Tier 2: Create transfer rules
        for output in transfers:
            try:
                await self._request_transfers(output)
            except RateLimitExceeded:
                return
            except Exception as e:
                logger.error(f"Output {output.id} transfer request failed: {e}")

        # Tier 3: Poll transfer status (rate-limited per output)
        now = datetime.utcnow()
        for output in polling:
            if (output.last_transfer_check and
                    now - output.last_transfer_check < self.TRANSFER_POLL_INTERVAL):
                continue  # Cooldown not elapsed — skip this output
            try:
                all_complete = await self._check_transfer_status(output)
                if all_complete:
                    await self._release_source_protection(output)
                    await self.db.update_output_dataset(
                        output.id, status=OutputStatus.ANNOUNCED
                    )
            except RateLimitExceeded:
                return
            except Exception as e:
                logger.error(f"Output {output.id} transfer poll failed: {e}")

    async def invalidate_outputs(self, workflow_id: UUID, reason: str):
        """Invalidate all outputs for a workflow — DBS invalidation + Rucio rule deletion."""
        outputs = await self.db.get_output_datasets(workflow_id)
        for output in outputs:
            # Invalidate in DBS
            if output.dbs_registered:
                await self.dbs_adapter.invalidate_dataset(output.dataset_name)

            # Delete Rucio rules
            rules_deleted = False
            if output.source_rule_id:
                await self.rucio_adapter.delete_rule(output.source_rule_id)
                rules_deleted = True
            for rule_id in output.transfer_rule_ids:
                await self.rucio_adapter.delete_rule(rule_id)
                rules_deleted = True

            await self.db.update_output_dataset(output.id,
                invalidated=True,
                invalidated_at=datetime.utcnow(),
                invalidation_reason=reason,
                dbs_invalidated=output.dbs_registered,
                rucio_rules_deleted=rules_deleted,
                status=OutputStatus.INVALIDATED,
            )

    async def cleanup_previous_version_outputs(self, prev_request_name: str,
                                               cleanup_policy: CleanupPolicy):
        """
        Called when a version-incremented request completes successfully.
        Handles outputs from the previous version based on cleanup_policy.
        """
        prev_workflow = await self.db.get_workflow_by_request(prev_request_name)
        if not prev_workflow:
            return

        if cleanup_policy == CleanupPolicy.IMMEDIATE_CLEANUP:
            await self.invalidate_outputs(
                prev_workflow.id,
                reason=f"Superseded by new version, immediate cleanup"
            )
        elif cleanup_policy == CleanupPolicy.KEEP_UNTIL_REPLACED:
            # Outputs remain valid until explicitly cleaned up
            logger.info(
                f"Previous version {prev_request_name} outputs kept "
                f"(keep_until_replaced policy)"
            )

    async def _register_in_dbs(self, output):
        await self.dbs_adapter.inject_dataset(output.dataset_name, output.source_site)
        await self.db.update_output_dataset(output.id,
            dbs_registered=True, dbs_registered_at=datetime.utcnow(),
            status=OutputStatus.DBS_REGISTERED,
        )

    async def _protect_source(self, output):
        rule_id = await self.rucio_adapter.create_rule(
            dataset=output.dataset_name, site=output.source_site, lifetime=None,
        )
        await self.db.update_output_dataset(output.id,
            source_rule_id=rule_id, source_protected=True,
            status=OutputStatus.SOURCE_PROTECTED,
        )

    async def _request_transfers(self, output):
        destinations = await self._determine_destinations(output)
        rule_ids = []
        for dest in destinations:
            rule_ids.append(await self.rucio_adapter.create_rule(
                dataset=output.dataset_name, site=dest,
            ))
        await self.db.update_output_dataset(output.id,
            transfer_rule_ids=rule_ids, transfer_destinations=destinations,
            status=OutputStatus.TRANSFERS_REQUESTED,
        )

    async def _check_transfer_status(self, output) -> bool:
        all_ok = True
        for rule_id in output.transfer_rule_ids:
            if await self.rucio_adapter.get_rule_status(rule_id) != "OK":
                all_ok = False
                break
        await self.db.update_output_dataset(output.id,
            status=OutputStatus.TRANSFERRING if not all_ok else output.status,
            last_transfer_check=datetime.utcnow(),
        )
        return all_ok

    async def _determine_destinations(self, output) -> List[str]:
        """
        Determine where to place replicas of a completed output dataset.

        CMS data placement follows Rucio subscription rules defined by
        Physics and Computing coordination. WMS2 does NOT decide placement
        policy — it queries Rucio for existing subscriptions that match the
        dataset pattern and creates transfer rules accordingly.

        If no Rucio subscriptions match (e.g., test workflows or custom
        user datasets), the output stays at the source site only.
        """
        # Query Rucio for subscriptions matching this dataset pattern
        subscriptions = await self.rucio_adapter.list_matching_subscriptions(
            dataset=output.dataset_name,
        )

        destinations = set()
        for sub in subscriptions:
            # Each subscription specifies RSE expressions (e.g., "tier=1")
            # Resolve to concrete sites
            rses = await self.rucio_adapter.resolve_rse_expression(sub.rse_expression)
            destinations.update(rses)

        # Never transfer to the source site (already has a replica)
        destinations.discard(output.source_site)
        return list(destinations)

    async def _release_source_protection(self, output):
        await self.rucio_adapter.delete_rule(output.source_rule_id)
        await self.db.update_output_dataset(output.id,
            source_released=True, status=OutputStatus.SOURCE_RELEASED,
        )
```

### 4.8 Error Handler (Workflow-Level)

Per-node retries are handled by DAGMan's `RETRY` directive combined with a WMS2-provided POST script. The POST script inspects exit codes, classifies errors, implements cool-off delays, and returns an appropriate exit code to DAGMan. This script is part of the sandbox and can be iterated independently.

```python
class ErrorHandler:
    async def handle_dag_partial_failure(self, dag):
        failure_ratio = dag.nodes_failed / max(dag.total_nodes, 1)
        if failure_ratio < 0.05:
            logger.info(f"DAG {dag.id}: {failure_ratio:.1%} failed, submitting rescue")
            await self._submit_rescue_dag(dag)
        elif failure_ratio < 0.30:
            logger.warning(f"DAG {dag.id}: {failure_ratio:.1%} failed, needs review")
            await self._flag_for_review(dag)
            await self.db.update_request_by_dag(dag.id, status=RequestStatus.PARTIAL)
        else:
            logger.error(f"DAG {dag.id}: {failure_ratio:.1%} failed, aborting")
            await self._abort_workflow(dag)

    async def handle_dag_failure(self, dag):
        await self._abort_workflow(dag)

    async def _submit_rescue_dag(self, dag):
        dagman_cluster_id, schedd_name = await self.condor_adapter.submit_dag(
            dag_file=dag.dag_file_path, submit_dir=dag.submit_dir,
        )
        await self.db.update_dag(dag.id,
            dagman_cluster_id=dagman_cluster_id, schedd_name=schedd_name,
            rescue_dag_path=f"{dag.dag_file_path}.rescue001",
            status=DAGStatus.SUBMITTED, submitted_at=datetime.utcnow(),
        )

    async def _abort_workflow(self, dag):
        await self.db.update_dag(dag.id, status=DAGStatus.FAILED)
        await self.db.update_workflow(dag.workflow_id, status=WorkflowStatus.FAILED)
        await self.db.update_request_by_dag(dag.id, status=RequestStatus.FAILED)
```

### 4.9 Site Manager

```python
class SiteManager:
    async def sync_from_cric(self):
        cric_sites = await self.cric_adapter.get_sites()
        for site_data in cric_sites:
            await self.db.upsert_site(Site(**site_data))

    async def drain_site(self, site_name, until=None):
        """Mark site as draining. For already-submitted DAGs, dynamic classad
        updates are an HTCondor feature requirement (see Section 12)."""
        await self.db.update_site(site_name, status=SiteStatus.DRAINING, drain_until=until)

    async def get_available_sites(self) -> List[Site]:
        return await self.db.get_sites_by_status(SiteStatus.ENABLED)
```

### 4.10 Metrics Collector

```python
class MetricsCollector:
    """Per-job and pool metrics handled by existing external systems."""

    async def collect(self):
        for status in WorkflowStatus:
            count = await self.db.count_workflows_by_status(status)
            self.gauge("wms2_workflows", count, labels={"status": status.value})

        queued = await self.db.count_requests_by_status(RequestStatus.QUEUED)
        self.gauge("wms2_admission_queue_depth", queued)

        for dag in await self.db.get_active_dags():
            progress = dag.nodes_done / max(dag.total_nodes, 1)
            self.gauge("wms2_dag_progress", progress, labels={"dag_id": str(dag.id)})

        for status in OutputStatus:
            count = await self.db.count_output_datasets_by_status(status)
            self.gauge("wms2_outputs", count, labels={"status": status.value})
```

---

## 5. Pilot Execution Model

### 5.1 Overview

The pilot is a single job that runs the full payload chain with iteratively increasing input sizes to measure performance. It produces real output data (not wasted work) and a structured performance report that the DAG Planner uses to build an optimally configured production DAG.

### 5.2 Pilot Behavior

```
Pilot job lands on worker node with allocated resources.

Iteration 1: Process N events through full chain → measure
Iteration 2: Process 2N events through full chain → measure
Iteration 3: Process 4N events through full chain → measure
...
Stop when: time budget exhausted OR measurements converged

Output:
  - Real processed data (feeds into final merge)
  - Performance report (JSON):
    {
      "iterations": [
        {"events": 1000, "wall_time_sec": 120, "memory_mb": 1800, ...},
        {"events": 2000, "wall_time_sec": 235, "memory_mb": 1850, ...},
        {"events": 4000, "wall_time_sec": 470, "memory_mb": 1900, ...}
      ],
      "per_step": [
        {"step": "DIGI", "time_per_event": 0.05, "memory_mb": 1200, ...},
        {"step": "RECO", "time_per_event": 0.08, "memory_mb": 1900, ...}
      ],
      "recommended": {
        "events_per_job": 25000,
        "memory_mb": 2048,
        "time_per_job_sec": 28800,
        "output_size_per_event_kb": 1.8
      }
    }
```

### 5.3 Pilot vs Urgent Workflows

| Aspect | Normal (with pilot) | Urgent (no pilot) |
|---|---|---|
| Performance data | Measured | Requestor-provided estimates |
| Resource accuracy | Optimal | Potentially suboptimal |
| Merge planning | Based on actual output sizes | Based on estimated sizes |
| Time to first DAG | ~8 hours (pilot duration) | Immediate |
| Use case | Standard campaigns | Time-critical reprocessing |

### 5.4 Pilot as Part of the Sandbox

The pilot is implemented within the sandbox/wrapper — the same executable as production jobs, running in a "pilot mode" that iterates and measures. WMS2 only needs to submit the pilot job with the right configuration, detect completion, parse the performance report, and feed metrics to the DAG Planner.

### 5.5 Pilot → Production Pipeline

The pilot's measurements flow through a concrete pipeline to produce the production DAG. Each step is owned by a specific component:

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. Lifecycle Manager: _handle_queued()  [Section 4.2]             │
│     Calls dag_planner.submit_pilot(workflow)                       │
│     Stores pilot_cluster_id, pilot_schedd, pilot_output_path       │
│     on the Workflow record. Transitions to PILOT_RUNNING.          │
│                                                                     │
│  2. HTCondor runs the pilot job (sandbox in pilot mode)            │
│     Pilot iterates: N, 2N, 4N events until convergence            │
│     Writes pilot_metrics.json to pilot_output_path                 │
│                                                                     │
│  3. Lifecycle Manager: _handle_pilot_running()  [Section 4.2]      │
│     Polls pilot_cluster_id on pilot_schedd until completed         │
│     Calls dag_planner.handle_pilot_completion(workflow, path)      │
│                                                                     │
│  4. DAG Planner: _parse_pilot_report()  [Section 4.5]             │
│     Reads pilot_metrics.json                                        │
│     Maps JSON → PilotMetrics (Section 3.1.2)                       │
│     Key value: output_size_per_event_kb                            │
│                                                                     │
│  5. DAG Planner: plan_and_submit()  [Section 4.5]                  │
│     Uses PilotMetrics for resource sizing:                          │
│       - events_per_job → how many events per processing node       │
│       - memory_mb → HTCondor request_memory                        │
│       - time_per_job_sec → max_retries / timeout                   │
│                                                                     │
│  6. DAG Planner: _plan_merge_groups()  [Section 4.5]               │
│     Uses output_size_per_event_kb to determine group composition:  │
│       group_size = target_merged_size / (events_per_job × kb/evt)  │
│     Each group: N processing nodes feeding 1 merge node            │
│     This is the critical pilot-dependent calculation.               │
│                                                                     │
│  7. DAG Planner: _generate_dag_files()  [Section 4.5]             │
│     Each group → SUBDAG EXTERNAL with landing node                 │
│     Submits to HTCondor. Transitions to ACTIVE.                    │
└─────────────────────────────────────────────────────────────────────┘
```

**Why this pipeline matters**: Without the pilot, merge group composition is based on estimates. If the estimate for `output_size_per_event_kb` is wrong by 2x, merge outputs will be half or double the target size — either wasting storage (too small) or creating downstream problems (too large). The pilot's iterative convergence gives us a reliable measurement.

---

## 6. Recovery Model

### 6.1 Multi-Level Recovery

**Level 1: Per-Node (owned by DAGMan + POST script)**
- DAGMan's `RETRY` directive retries failed nodes automatically
- WMS2-provided POST script runs after each node completion/failure
- POST script classifies errors, implements cool-off delays, controls retry
- Lives in the sandbox, can be iterated independently

**Level 2: DAG-wide (owned by HTCondor — feature requirement)**
- Detect correlated failure patterns across nodes
- Make DAG-level decisions: halt submission, avoid specific sites
- Cannot be implemented in individual POST scripts
- See Section 12 for HTCondor feature requirements

**Level 3: Workflow-level (owned by Request Lifecycle Manager)**
- The Lifecycle Manager owns all workflow-level recovery decisions
- When a DAG terminates with failures, it decides: rescue, review, or abort
- Based on failure ratio thresholds (< 5% auto-rescue, 5–30% review, > 30% abort)
- Rescue DAGs automatically skip completed nodes
- Also handles clean stop recovery and catastrophic failure recovery

### 6.2 Clean Stop and Recovery Flow

Operators can gracefully stop a running DAG and resume it later through the admission queue. This is useful for schedd maintenance, priority reshuffling, or resource rebalancing.

**State flow**: `ACTIVE → STOPPING → RESUBMITTING → QUEUED → ACTIVE`

```
Operator calls POST /api/v1/requests/{name}/stop
  │
  ▼
Lifecycle Manager: initiate_clean_stop()
  ├── condor_rm on DAGMan job
  ├── DAGMan writes rescue DAG before exiting
  ├── Request → STOPPING, Workflow → STOPPING
  │
  ▼ (next cycle: _handle_stopping)
DAGMan process gone, stop complete
  ├── DAG → STOPPED
  ├── Create new DAG record pointing to rescue DAG
  ├── Request → RESUBMITTING
  │
  ▼ (next cycle: _handle_resubmitting)
Request → QUEUED
  │
  ▼ (normal admission flow)
Admission Controller admits, DAG Planner submits rescue DAG
  ├── Request → ACTIVE
  └── DAG resumes from where it left off (rescue DAG skips completed nodes)
```

The `STOPPED` DAG status is distinct from `FAILED` — it indicates a controlled shutdown that is eligible for rescue DAG recovery without going through the Error Handler's failure ratio evaluation.

### 6.3 Catastrophic Failure Recovery

When a schedd is permanently lost (hardware failure, unrecoverable corruption), the DAG and all its in-flight state are gone. The recovery path uses **dataset versioning**: increment `processing_version` and resubmit from scratch.

**State flow**: Failed request → new request with `processing_version + 1`

```
Operator calls POST /api/v1/requests/{name}/restart
  │
  ▼
Lifecycle Manager: handle_catastrophic_failure()
  ├── Invalidate outputs from failed version:
  │     ├── DBS: invalidate dataset entries
  │     └── Rucio: delete protection and transfer rules
  ├── Create new request with processing_version + 1
  │     ├── Link: new.previous_version_request = old.request_name
  │     └── Link: old.superseded_by_request = new.request_name
  ├── Old request → FAILED
  └── New request → SUBMITTED (enters normal pipeline)
```

**Cleanup policies** control what happens to outputs from the previous version when the new version completes successfully:

| Policy | Behavior | Use Case |
|---|---|---|
| `keep_until_replaced` | Previous outputs remain valid and accessible until explicitly cleaned up | Safe default; allows manual verification before removing old data |
| `immediate_cleanup` | Previous outputs are invalidated as soon as the new version completes | When storage pressure is high or outputs are known to be corrupt |

**Output invalidation rollback table** — every forward output state has a corresponding undo operation:

| Forward State | Undo Operation |
|---|---|
| `PENDING` | Delete record (no external state) |
| `DBS_REGISTERED` | `dbs_adapter.invalidate_dataset()` |
| `SOURCE_PROTECTED` | `rucio_adapter.delete_rule(source_rule_id)` |
| `TRANSFERS_REQUESTED` | `rucio_adapter.delete_rule()` for each transfer rule |
| `TRANSFERRING` | `rucio_adapter.delete_rule()` for each transfer rule |
| `TRANSFERRED` | `rucio_adapter.delete_rule()` for source + transfer rules |
| `SOURCE_RELEASED` | No source rule to delete; delete transfer rules |
| `ANNOUNCED` | `dbs_adapter.invalidate_dataset()` + delete remaining rules |

### 6.4 POST Script Design

```bash
#!/bin/bash
# post_script.sh — per-node error handler
# Arguments: $1 = node_name, $2 = exit_code ($RETURN from DAGMan)

NODE_NAME=$1
EXIT_CODE=$2

if [ "$EXIT_CODE" -eq 0 ]; then
    exit 0  # Success
fi

ERROR_CLASS=$(python3 classify_error.py $NODE_NAME $EXIT_CODE)

case $ERROR_CLASS in
    "transient")
        RETRY_COUNT=$(cat ${NODE_NAME}.retry_count 2>/dev/null || echo 0)
        SLEEP_TIME=$((60 * (2 ** RETRY_COUNT)))
        echo $((RETRY_COUNT + 1)) > ${NODE_NAME}.retry_count
        sleep $SLEEP_TIME
        exit 1  # Failed, eligible for RETRY
        ;;
    "permanent")
        exit 2  # Failed, do not retry (UNLESS-EXIT 2)
        ;;
    "site_issue")
        sleep 300
        exit 1  # Retry, may land on different site
        ;;
esac
```

---

## 7. API Design

### 7.1 REST API Endpoints (MVP)

```
Requests
────────
POST   /api/v1/requests                    Import request from ReqMgr2
GET    /api/v1/requests                    List requests (filterable)
GET    /api/v1/requests/{name}             Get request details
PATCH  /api/v1/requests/{name}             Update request (priority, status)
DELETE /api/v1/requests/{name}             Cancel/abort request
POST   /api/v1/requests/{name}/stop        Initiate clean stop
POST   /api/v1/requests/{name}/restart     Resubmit with version increment
GET    /api/v1/requests/{name}/versions    Version history

Workflows
─────────
GET    /api/v1/workflows                   List workflows
GET    /api/v1/workflows/{id}              Get workflow details
GET    /api/v1/workflows/{id}/status       Get workflow status summary

DAGs
────
GET    /api/v1/dags                        List DAGs (filterable)
GET    /api/v1/dags/{id}                   Get DAG details & aggregate node status
GET    /api/v1/dags/{id}/nodes             Get node-level status (live from DAGMan)
GET    /api/v1/dags/{id}/logs              Get DAGMan log location
POST   /api/v1/dags/{id}/hold              Hold DAGMan job (pause DAG)
POST   /api/v1/dags/{id}/release           Release held DAGMan job
POST   /api/v1/dags/{id}/remove            Remove DAG from queue
POST   /api/v1/dags/{id}/rescue            Submit rescue DAG

Admission
─────────
GET    /api/v1/admission/queue             Get admission queue status
PATCH  /api/v1/admission/config            Update admission limits

Sites
─────
GET    /api/v1/sites                       List sites
GET    /api/v1/sites/{name}                Get site details
PATCH  /api/v1/sites/{name}                Update site config
POST   /api/v1/sites/{name}/drain          Start draining site
POST   /api/v1/sites/{name}/enable         Enable site

Lifecycle
─────────
GET    /api/v1/lifecycle/status             Lifecycle Manager health

Monitoring
──────────
GET    /api/v1/metrics                     Prometheus metrics endpoint
GET    /api/v1/health                      Health check
GET    /api/v1/status                      System status summary
```

### 7.2 Example API Interactions

```
# Import a request
POST /api/v1/requests
{
    "request_name": "user_Run2024A_v1_250130_123456",
    "source": "reqmgr2"
}

Response:
{
    "request_name": "user_Run2024A_v1_250130_123456",
    "status": "submitted",
    "workflow": {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "status": "new"
    }
}

# Check admission queue
GET /api/v1/admission/queue

Response:
{
    "active_dags": 285,
    "max_active_dags": 300,
    "queued_workflows": 42,
    "next_in_queue": {
        "request_name": "user_Run2024A_v1_250130_123456",
        "priority": 100000,
        "queued_since": "2026-01-30T10:00:00Z"
    }
}

# Get workflow status
GET /api/v1/workflows/550e8400-e29b-41d4-a716-446655440000/status

Response:
{
    "workflow_id": "550e8400-e29b-41d4-a716-446655440000",
    "request_name": "user_Run2024A_v1_250130_123456",
    "status": "active",
    "pilot_metrics": {
        "time_per_event_sec": 0.12,
        "memory_peak_mb": 1900,
        "output_size_per_event_kb": 1.8,
        "recommended_events_per_job": 25000
    },
    "dag": {
        "id": "660e8400-e29b-41d4-a716-446655440001",
        "status": "running",
        "total_nodes": 9560,
        "node_counts": {"Processing": 9500, "Merge": 50, "Cleanup": 10},
        "nodes_idle": 3000,
        "nodes_running": 2500,
        "nodes_done": 4000,
        "nodes_failed": 60
    },
    "outputs": [
        {
            "dataset_name": "/Run2024A/RECO/v1",
            "status": "transferring",
            "transfers_complete": false
        }
    ],
    "progress_percent": 41.8,
    "estimated_completion": "2026-01-31T15:30:00Z"
}

# Initiate clean stop
POST /api/v1/requests/user_Run2024A_v1_250130_123456/stop
{
    "reason": "Schedd maintenance window"
}

Response:
{
    "request_name": "user_Run2024A_v1_250130_123456",
    "status": "stopping",
    "previous_status": "active",
    "stop_reason": "Schedd maintenance window",
    "message": "Clean stop initiated. DAG will be stopped and a rescue DAG prepared for re-admission."
}

# Get version history
GET /api/v1/requests/user_Run2024A_v2_250130_123456/versions

Response:
{
    "current": {
        "request_name": "user_Run2024A_v2_250130_123456",
        "processing_version": 2,
        "status": "active",
        "cleanup_policy": "keep_until_replaced"
    },
    "versions": [
        {
            "request_name": "user_Run2024A_v1_250130_123456",
            "processing_version": 1,
            "status": "failed",
            "superseded_by": "user_Run2024A_v2_250130_123456",
            "failure_reason": "Schedd permanently lost"
        }
    ]
}
```

---

## 8. Authentication and Authorization

### 8.1 MVP: X.509 Certificates

- **API access**: X.509 client certificates with VOMS proxy delegation.
- **HTCondor credentials**: VOMS proxy certificates managed by HTCondor credential infrastructure.
- **DAG lifetime constraint**: DAGs are bounded by proxy validity (~24 hours). Workflows requiring longer execution use sequential DAG submissions with rescue DAG continuity.
- **Service-to-service**: Robot certificates for DBS, Rucio, ReqMgr2, CRIC communication.

### 8.2 Target: WLCG Tokens

- **API access**: OIDC/OAuth2 tokens via WLCG token infrastructure.
- **HTCondor credentials**: OAuth2 refresh tokens stored on schedd, automatic access token provisioning to jobs.
- **DAG lifetime**: Unlimited — refresh tokens handle credential renewal transparently.
- **Transition**: Configuration change, not architectural change. WMS2 code is auth-mechanism-agnostic.

Token support is an external dependency (HTCondor + WLCG infrastructure), not a WMS2 deliverable.

---

## 9. Technology Stack

### 9.1 Core Technologies

| Component | Technology | Rationale |
|---|---|---|
| Language | Python 3.11+ | Team expertise, CMS ecosystem |
| API Framework | FastAPI | Modern, async, auto-docs |
| Database | PostgreSQL 15 | ACID, JSONB, mature, handles queues |
| ORM | SQLAlchemy 2.0 | Async support, mature |
| Validation | Pydantic 2.0 | Type safety, FastAPI integration |
| Task Queue | PostgreSQL-based | Simple, no additional infrastructure |

### 9.2 Infrastructure

| Component | Technology | Rationale |
|---|---|---|
| Container | Docker | Standard containerization |
| Orchestration | Kubernetes | CMS infrastructure standard |
| CI/CD | GitLab CI | CMS standard |
| Monitoring | Prometheus + Grafana | CMS standard |
| Logging | Loki | Integrates with Grafana |
| Tracing | OpenTelemetry | Standard observability |

### 9.3 External Integrations

| Service | Integration Method |
|---|---|
| ReqMgr2 | REST API |
| DBS | REST API (read + write for output registration) |
| Rucio | REST API (replica lookup, rule creation/status/deletion) |
| HTCondor | Python bindings + CLI (`condor_submit_dag`, `condor_q`, `condor_hold`, `condor_release`, `condor_rm`) |
| DAGMan | `.dag` file generation + `.status` / `.metrics` file parsing |
| CRIC | REST API |

---

## 10. MVP Implementation Plan

### 10.1 Phase 1: Foundation (Weeks 1–4)

**Goal**: Basic infrastructure, data models, and Lifecycle Manager framework.

| Component | Description | Priority |
|---|---|---|
| Database Schema | PostgreSQL tables, indexes, migrations | P0 |
| Data Models | Pydantic models, validation | P0 |
| API Framework | FastAPI setup, auth, middleware | P0 |
| Lifecycle Manager | Main loop + state evaluation framework | P0 |
| Config Management | Settings, environment handling | P0 |
| Logging / Metrics | Structured logging, Prometheus | P1 |

**Deliverables**: Database migrations working; API skeleton with health endpoints; Lifecycle Manager main loop evaluating requests; Docker compose for local dev; basic test framework.

### 10.2 Phase 2: Pilot + DAG Planning Pipeline (Weeks 5–8)

**Goal**: Request → Pilot → DAG file generation.

| Component | Description | Priority |
|---|---|---|
| ReqMgr2 Adapter | Fetch requests, validate schema | P0 |
| DBS Adapter | Query datasets, get file lists | P0 |
| Workflow Manager | Create workflows from requests | P0 |
| Admission Controller | Capacity check and priority ordering | P0 |
| Pilot Submission | Submit pilot jobs, parse results | P0 |
| File-Based Splitter | File-based splitting → DAGNodeSpecs | P0 |
| Event-Based Splitter | Event-based splitting | P1 |
| Merge Planner | Plan merge nodes from pilot metrics | P0 |
| DAG File Generator | Generate .dag and .sub files | P0 |

**Deliverables**: Can import request from ReqMgr2; pilot job runs and reports metrics; DAG files generated with accurate resource estimates and merge plan; unit tests for splitters and DAG generation.

**Action item**: Evaluate existing CMS micro-services for output registration during this phase.

### 10.3 Phase 3: HTCondor DAGMan Integration (Weeks 9–12)

**Goal**: DAG submission, monitoring, recovery, and clean stop.

| Component | Description | Priority |
|---|---|---|
| Condor Adapter | Submit DAGs, query DAGMan jobs | P0 |
| DAG Monitor | Poll DAGMan status, return results to Lifecycle Manager | P0 |
| POST Script | Per-node error classification and retry | P0 |
| Error Handler | Rescue DAG submission, abort logic | P0 |
| Clean Stop Flow | Operator-triggered stop → rescue DAG → re-admission | P0 |
| Catastrophic Recovery | Version increment, output invalidation, resubmit | P1 |
| DAG Hold/Release | Operator controls for DAGs | P1 |

**Deliverables**: DAGs submit to HTCondor DAGMan; status monitoring working; POST script error handling; automatic rescue DAG submission; clean stop and resume working; end-to-end test with real workflow.

**Milestone**: Load test with realistic concurrency (~300 DAGs × ~10K nodes) on test infrastructure with HTCondor team.

### 10.4 Phase 4: Output Pipeline + Productionization (Weeks 13–16)

**Goal**: Complete pipeline and ready for pilot testing.

| Component | Description | Priority |
|---|---|---|
| Output Manager | DBS registration, Rucio transfers | P0 |
| Rucio Adapter | Rule creation, status, deletion | P0 |
| Output Invalidation | DBS invalidation + Rucio rule cleanup for catastrophic recovery | P1 |
| Site Manager | CRIC sync, site control | P0 |
| Dashboard API | Frontend data endpoints | P1 |
| Observability | Grafana dashboards | P1 |
| Documentation | Ops guide, API docs | P1 |

**Deliverables**: Full request lifecycle working (submit → pilot → DAG → outputs registered → transfers complete); clean stop and catastrophic recovery tested; site management; production-like deployment; monitoring dashboards; operator documentation.

---

## 11. Migration Strategy

### 11.1 Coexistence Phase

```
ReqMgr2
   │
   ├──► WMCore Agent (existing production)
   │       └── Handles: all current workflow types
   │
   └──► WMS2 (new system)
           └── Handles: selected workflows for testing
                └── Delegates to DAGMan for job orchestration

Selection Criteria (in ReqMgr2):
  request.wms_backend = "wms2" | "wmcore"
  Gradual migration by campaign / workflow complexity
```

### 11.2 Migration Phases

| Phase | Timeline | Scope |
|---|---|---|
| Pilot | Month 1–2 | Test workflows, non-production |
| Limited | Month 3–4 | Selected campaigns |
| Expanded | Month 5–8 | More campaigns, broader testing |
| Full | Month 9–12 | All workflows, WMCore deprecated |

---

## 12. HTCondor Feature Requirements

The following capabilities are required from HTCondor / DAGMan and will be developed in collaboration with the HTCondor team. WMS2 is designed to use these features when available, with interim workarounds where noted.

| Requirement | Description | Interim Workaround |
|---|---|---|
| **Global idle throttling** | A pool-level or multi-DAG mechanism to cap total idle jobs across all concurrent DAGMan instances (budget: ~1M idle from ~10M total) | Conservative static per-DAG `DAGMAN_MAX_JOBS_IDLE` limits |
| **Dynamic classad updates** | Ability to update `+DESIRED_Sites` and other classads on queued/un-submitted DAG nodes when site status changes | Manual `condor_qedit` by operators; new DAGs use updated site lists |
| **DAG-wide priority propagation** | Update priority of an entire DAG (including un-submitted nodes) via a single operation | Remove and resubmit DAG with updated priority (rescue DAG continuity) |
| **DAG-level failure pattern detection** | DAGMan detects correlated failures (many nodes, same error, same site) and can halt/adjust automatically | POST scripts report to WMS2 API; operators monitor and intervene manually |
| **Site-affine job groups** | A set of independent jobs that the negotiator matches as a unit to one site, accounting for the group's total resource commitment when deciding placement. Eliminates the need for landing nodes and staggered submission. | SUBDAG EXTERNAL per merge group with a landing node (trivial `/bin/true` job); POST script reads `MATCH_GLIDEIN_CMSSite` classad to elect site; PRE scripts pin remaining nodes via `+DESIRED_Sites`. Staggered via `MAXJOBS MergeGroup` to create backpressure between batches. |

---

## 13. Success Criteria

### 13.1 MVP Success Metrics

| Metric | Target | Measurement |
|---|---|---|
| DAG Submission Rate | 50 DAGs/hour (covering 10,000+ nodes/hour) | Throughput test |
| DAG Tracking Latency | < 60 seconds | Time from DAGMan status change to WMS2 update |
| API Response Time | p99 < 500ms | API latency monitoring |
| System Uptime | 99.5% | Availability monitoring |
| Node Success Rate | > 95% | DAGMan metrics |
| Pilot Accuracy | Resource estimates within 20% of actual | Compare pilot predictions to production metrics |

### 13.2 Comparison with WMCore

| Aspect | WMCore | WMS2 Target |
|---|---|---|
| Codebase Size | ~500K lines | < 30K lines |
| Components | 15+ threads | 5–8 async workers |
| Databases | 3 (MySQL, Oracle, CouchDB) | 1 (PostgreSQL) |
| Job Tracking | Per-job in WMS DB | Delegated to DAGMan |
| Resource Estimation | Manual (requestor guesses) | Automated (pilot measurement) |
| Setup Time | Hours | Minutes |
| Debug Difficulty | High | Low (single service + DAGMan logs) |

---

## 14. Risks and Mitigations

| Risk | Impact | Likelihood | Mitigation |
|---|---|---|---|
| DAGMan scalability at 10M jobs | Medium | Low | Multiple schedds, HTCondor team collaboration, per-DAG throttling, load testing in Phase 3 |
| HTCondor API changes | High | Low | Abstract behind adapter, version pinning |
| ReqMgr2 schema changes | Medium | Medium | Flexible JSONB storage, validation layer |
| DAGMan status file parsing brittleness | Medium | Medium | Use condor_q as fallback; version-pin HTCondor |
| Existing micro-services not reusable for Output Manager | Medium | Medium | Evaluate early (Phase 2); budget time for building if needed |
| Output Manager complexity | Medium | Medium | Rate limiting, robust error handling, clear state machine |
| Pilot not representative of full dataset | Low | Medium | Allow manual override; use conservative estimates for merge planning |
| Feature gaps discovered late | High | Medium | Close collaboration with operations team |
| Team bandwidth | Medium | High | Phased approach, MVP focus |
| Silent request stalling | High | Medium | Lifecycle Manager timeout detection alerts when any request exceeds its expected duration in a given status; no request can be "forgotten" |
| Schedd permanent loss | High | Low | Dataset versioning: increment processing_version, resubmit from scratch, invalidate or preserve partial outputs based on cleanup policy |
| DAG stop leaves orphaned state | Medium | Low | Clean stop flow ensures DAGMan writes rescue DAG before exit; new DAG record created with recovery linkage; request re-enters admission queue |

---

## 15. Open Questions

1. **Sandbox/Wrapper Design**: How does the per-node manifest communicate input data to the job? What format? This is tightly coupled to sandbox architecture and needs dedicated design discussion.
2. **Sandbox Scope**: Exact boundary between WMS2 and sandbox responsibilities — manifest generation, pilot mode, POST script logic.
3. **Existing Micro-Services**: Which CMS micro-services currently handle output registration and data placement? Are they reusable with clean APIs?
4. **DAG Size Partitioning**: If a workflow exceeds practical DAG size limits (discovered during load testing), what's the partitioning strategy?
5. **DAGMan Status Polling vs. Event-Based**: Should WMS2 poll `.status` files or use event log callbacks? Performance implications at scale. The Lifecycle Manager's main loop uses polling by default; event-based callbacks could replace the poll_dag() call if HTCondor supports efficient event delivery.
6. **Shared Filesystem**: Is a shared filesystem between submit host and WMS2 service guaranteed, or do DAG files need to be transferred?
7. **GPU Jobs**: Include `request_gpus` support in submit files for MVP?
8. **Lifecycle Manager Cycle Interval**: What is the right cycle interval for the main loop? Too fast wastes resources polling unchanged state; too slow delays reaction to completions. Initial default is 60 seconds — needs tuning under production load.
9. **Catastrophic Failure Detection**: How to distinguish temporary schedd unavailability (network blip, restart) from permanent loss (hardware failure, corruption)? Threshold-based: if schedd is unreachable for N minutes, escalate to operator for catastrophic recovery decision.
10. **Version Cleanup Timing**: After v2 completes, how long should v1 outputs be kept under `keep_until_replaced` policy? Should there be a configurable grace period before automatic cleanup?
11. **Concurrent Version Limit**: Should there be a maximum number of version increments for a single request (e.g., max 3 retries) to prevent infinite retry loops from catastrophic infrastructure issues?
12. **Merge Group Stagger Depth**: How many merge groups should run concurrently (`MAXJOBS MergeGroup`)? Too few underutilizes the pool; too many causes landing nodes to cluster on the same site before processing backpressure takes effect. Needs tuning with real pool behavior.
13. **Landing Node Site Discovery**: Is `MATCH_GLIDEIN_CMSSite` reliably set across all CMS glidein configurations? Are there worker node environments where this classad is missing or named differently?

---

## 16. Design Decisions

This section captures significant design decisions and the reasoning behind them. Each entry records *what* was decided, *why*, and *what alternatives were rejected*. This serves as the authoritative rationale — if the spec needs to be rebuilt or re-evaluated, start here.

### DD-1: Single Lifecycle Manager instead of independent polling loops

**Decision**: One component (Request Lifecycle Manager) owns all request state transitions via a single main loop, calling other components as stateless workers.

**Why**: With independent loops per component (Admission Controller loop, DAG Monitor loop, Output Manager loop), a request can silently stall if any single loop hiccups — there is no single place that notices "this request hasn't made progress." A single loop over all non-terminal requests makes stuck-state detection trivial and provides one place to add timeouts and observability.

**Rejected alternative**: Event-driven architecture where components emit events and subscribe to each other. Too complex for the number of components; harder to reason about ordering; WMS2's scale (hundreds of workflows, not millions) doesn't justify it.

### DD-2: Merge groups as SUBDAG EXTERNAL with landing nodes

**Decision**: Each merge group (processing nodes + merge + cleanup) is a self-contained sub-DAG declared via `SUBDAG EXTERNAL`. A trivial `/bin/true` landing node per group lets HTCondor pick the site; POST script reads `MATCH_GLIDEIN_CMSSite` and writes it to a file; PRE scripts on remaining nodes read that file and set `+DESIRED_Sites`.

**Why**: Processing jobs feeding a merge job must run on the same site to avoid cross-site transfers before merging. But WMS2 should not do site selection — that's HTCondor's job. The landing node lets HTCondor's negotiator pick the site through normal matchmaking, and then pins the group. SUBDAG EXTERNAL keeps the sub-DAG internal to DAGMan; WMS2 sees only one top-level DAG.

**Rejected alternatives**:
- *WMS2 assigns sites at planning time*: Prevents HTCondor from load balancing. WMS2 would need to replicate the negotiator's logic.
- *Dynamic merge creation based on where processing outputs land*: Violates the principle of fixing merge group composition at planning time from pilot metrics. Also much more complex.
- *PRE script site selection with WMS2 API calls*: Creates races between groups; effectively WMS2 doing load balancing through the back door.

### DD-3: Staggered merge group concurrency via MAXJOBS

**Decision**: Limit concurrent merge groups using DAGMan's `MAXJOBS MergeGroup` category. This creates backpressure so early groups' processing jobs fill site queues before later groups' landing nodes are scheduled.

**Why**: Without staggering, all landing nodes are trivial `/bin/true` jobs that could all land on the same site since they create no backpressure. With staggering, the first few groups' processing jobs create real resource demand, so later landing nodes naturally spread across sites where capacity remains.

**Rejected alternative**: No throttling, rely on HTCondor. Risk of poor site distribution is too high for merge-heavy workflows.

### DD-4: Output processing starts immediately as merge groups complete

**Decision**: The Lifecycle Manager calls `process_outputs_for_workflow()` every cycle for ACTIVE requests, not just after the DAG finishes. When a merge group completes, its output enters the pipeline immediately for DBS registration and Rucio source protection.

**Why**: Merge group outputs sit on the source site's scratch space. If we wait until the entire DAG completes (which could be hours or days later), the source site may clean up that scratch space. DBS registration and Rucio source protection are urgent — they must happen as soon as the merge output exists.

**Rejected alternative**: Process outputs only after DAG completion. Simpler but risks data loss for early-completing merge groups.

### DD-5: Priority-tiered output processing with transfer polling cooldown

**Decision**: `process_outputs_for_workflow()` partitions outputs into three priority tiers: (1) DBS registration + source protection (urgent), (2) transfer rule creation, (3) transfer status polling (rate-limited to every 5 minutes per output).

**Why**: With 300 workflows × dozens of outputs each, the Lifecycle Manager calls the Output Manager many times per cycle. Not all output work is equally urgent. DBS registration and source protection prevent data loss; transfer polling is informational and can tolerate minutes of staleness. Polling Rucio for every in-flight transfer every 60-second cycle would generate excessive API load for no benefit.

**Rejected alternative**: Flat processing order (iterate all outputs, handle each based on status). Risks spending the cycle budget on transfer polling while newly-completed merge groups wait for protection.

### DD-6: Dataset versioning for catastrophic failure recovery

**Decision**: When a schedd is permanently lost, increment `processing_version`, create a new request linked to the old one, resubmit from scratch. Old outputs are handled per `CleanupPolicy` (immediate cleanup or keep until replaced).

**Why**: When a schedd dies permanently, the DAG state is gone. There's no rescue DAG, no node status — nothing to resume from. Starting over is the only option. Version linkage preserves the audit trail and allows the new version to clean up old partial outputs once it succeeds.

**Rejected alternative**: Attempt to reconstruct DAG state from output records. Fragile, incomplete (we don't track individual job state), and the processing outputs may themselves be corrupt if the schedd died mid-write.

### DD-7: Payload agnosticism — WMS2 doesn't know what jobs run

**Decision**: WMS2 treats the worker-node payload as opaque. Whether the job runs a single cmsRun or chains multiple steps (StepChain), the DAG structure and WMS2 logic are identical.

**Why**: WMCore's tight coupling to payload internals (knowing about DIGI, RECO, MINIAOD steps) is a major source of complexity and maintenance burden. Decoupling means WMS2 can support new workflow types without code changes. The sandbox/wrapper handles payload execution and can be iterated independently.

### DD-8: No per-job tracking

**Decision**: WMS2 tracks requests, workflows, and DAGs. Individual job state (running, held, idle, completed) is owned entirely by HTCondor/DAGMan. WMS2 never queries `condor_q` for individual job status.

**Why**: WMCore's per-job tracking database tables are the single largest source of scale problems: millions of rows, expensive status updates, complex state machines per job. HTCondor already tracks this. Duplicating it in WMS2 creates consistency problems and O(jobs) database load. WMS2 needs only DAG-level aggregates (nodes done, nodes failed) which DAGMan provides via the `.status` file.

### DD-9: Rucio subscriptions drive data placement, not WMS2

**Decision**: `_determine_destinations()` queries Rucio for existing subscriptions matching the dataset pattern. WMS2 does not maintain its own placement policy.

**Why**: CMS data placement policy is managed by Physics and Computing coordination through Rucio subscriptions. These policies change frequently (new tiers, quota adjustments, campaign-specific rules). If WMS2 duplicated this logic, it would need constant synchronization. Instead, WMS2 creates the source protection rule (preventing premature cleanup) and then lets Rucio's subscription mechanism handle the rest.

### DD-10: Stuck request handling is status-aware, not one-size-fits-all

**Decision**: `_handle_stuck()` takes different actions based on which status the request is stuck in. Fast internal operations (SUBMITTED, PLANNING) are failed immediately. Slow external operations (ACTIVE with unreachable schedd) escalate to the operator rather than auto-triggering catastrophic recovery.

**Why**: A request stuck in PLANNING for an hour is a bug — fail fast and alert. A request stuck in ACTIVE for 30 days may just be a large workflow — don't abort it. A request stuck in ACTIVE with an unreachable schedd may be catastrophic — but auto-recovery could destroy data if the schedd is temporarily down (network blip). The operator must confirm before triggering version increment and output invalidation.

---

## Appendix A: WMCore Component Mapping

| WMCore Component | WMS2 Equivalent | Notes |
|---|---|---|
| — (distributed loops) | Request Lifecycle Manager | New: replaces independent polling loops with single state machine owner |
| WorkQueue | Workflow Manager + Admission Controller | Simplified with FIFO throttling |
| JobCreator | DAG Planner (splitting) | Produces DAGNodeSpecs, not Jobs |
| JobSubmitter | DAG Planner (DAG submission) | Uses `condor_submit_dag` |
| BossAir | HTCondor/DAGMan Adapter | DAGMan owns job lifecycle |
| StatusPoller | DAG Monitor | Monitors DAGMan status, not individual jobs |
| JobAccountant | DAG Monitor + Output Manager | Split between monitoring and registration |
| ErrorHandler | POST script + Error Handler | Per-node in POST script, workflow-level in WMS2 |
| RetryManager | DAGMan RETRY + POST script | Fully delegated with intelligent POST script |
| ResourceControl | Site Manager + DAGMan MAXJOBS + Admission Controller | Multi-level throttling |
| WMBS | PostgreSQL | No separate DB; no per-job rows |
| — (manual) | Pilot job | New: automated performance measurement |

---

## Appendix B: ReqMgr2 Schema Reference

Key fields from ReqMgr2 that WMS2 must support:

```python
REQMGR2_CORE_FIELDS = [
    # Identity
    "RequestName", "Requestor", "RequestorDN",
    # Processing
    "InputDataset", "SecondaryInputDataset", "OutputDatasets",
    "CMSSWVersion", "ScramArch", "GlobalTag",
    # Splitting
    "SplittingAlgo", "EventsPerJob", "FilesPerJob", "LumisPerJob",
    # Resources
    "Memory", "TimePerEvent", "SizePerEvent", "Multicore",
    # Sites
    "SiteWhitelist", "SiteBlacklist",
    # Metadata
    "Campaign", "ProcessingString", "AcquisitionEra",
    "ProcessingVersion", "Priority",
    # State
    "RequestStatus", "RequestTransition",
    # Payload (opaque to WMS2)
    "PayloadConfig",
]
```

Note: `RequestType` is stored in `PayloadConfig` as metadata. WMS2 does not act on it.

---

## Appendix C: DAGMan File Format Reference

### C.1 Outer DAG (workflow.dag)

WMS2 submits ONE DAG per workflow. Each merge group is a `SUBDAG EXTERNAL` — DAGMan manages the sub-DAGs internally. WMS2 monitors only the top-level DAGMan process.

```
# WMS2-generated DAG for workflow 550e8400-...
# Each merge group is a self-contained sub-DAG with site-affine execution.
CONFIG /submit/550e8400/dagman.config

# Merge groups — each is a SUBDAG containing landing + processing + merge + cleanup
SUBDAG EXTERNAL mg_000000 /submit/550e8400/mg_000000/group.dag
SUBDAG EXTERNAL mg_000001 /submit/550e8400/mg_000001/group.dag
SUBDAG EXTERNAL mg_000002 /submit/550e8400/mg_000002/group.dag

# Stagger merge groups to create backpressure for site distribution.
# Active groups' processing jobs consume real slots, so the negotiator
# sees genuine load when placing the next batch of landing nodes.
CATEGORY mg_000000 MergeGroup
CATEGORY mg_000001 MergeGroup
CATEGORY mg_000002 MergeGroup
MAXJOBS MergeGroup 10
```

### C.2 Merge Group Sub-DAG (mg_000000/group.dag)

Each merge group is a self-contained DAG. The landing node lets HTCondor pick the site via normal negotiation. Its POST script reads `MATCH_GLIDEIN_CMSSite` from the completed job's classad and writes the elected site to a file. PRE scripts on subsequent nodes read that file and rewrite `+DESIRED_Sites` to pin them to the elected site.

```
# Merge group 0 — landing node elects site, all other nodes pinned to it

# Landing node: trivial /bin/true job, HTCondor picks site via normal matching
JOB landing landing.sub
SCRIPT POST landing /submit/550e8400/elect_site.sh elected_site

# Processing nodes: PRE script pins +DESIRED_Sites to elected site
JOB proc_000000 proc_000000.sub
SCRIPT PRE proc_000000 /submit/550e8400/pin_site.sh proc_000000.sub elected_site
SCRIPT POST proc_000000 /submit/550e8400/post_script.sh proc_000000 $RETURN

JOB proc_000001 proc_000001.sub
SCRIPT PRE proc_000001 /submit/550e8400/pin_site.sh proc_000001.sub elected_site
SCRIPT POST proc_000001 /submit/550e8400/post_script.sh proc_000001 $RETURN

JOB proc_000002 proc_000002.sub
SCRIPT PRE proc_000002 /submit/550e8400/pin_site.sh proc_000002.sub elected_site
SCRIPT POST proc_000002 /submit/550e8400/post_script.sh proc_000002 $RETURN

# Merge node: also pinned to elected site
JOB merge merge.sub
SCRIPT PRE merge /submit/550e8400/pin_site.sh merge.sub elected_site

# Cleanup node
JOB cleanup cleanup.sub
SCRIPT PRE cleanup /submit/550e8400/pin_site.sh cleanup.sub elected_site

# Retries
RETRY proc_000000 3 UNLESS-EXIT 2
RETRY proc_000001 3 UNLESS-EXIT 2
RETRY proc_000002 3 UNLESS-EXIT 2
RETRY merge 2 UNLESS-EXIT 2
RETRY cleanup 1

# Dependencies: landing → processing → merge → cleanup
PARENT landing CHILD proc_000000 proc_000001 proc_000002
PARENT proc_000000 proc_000001 proc_000002 CHILD merge
PARENT merge CHILD cleanup

# Category throttles within the group
CATEGORY proc_000000 Processing
CATEGORY proc_000001 Processing
CATEGORY proc_000002 Processing
CATEGORY merge Merge
CATEGORY cleanup Cleanup
MAXJOBS Processing 5000
MAXJOBS Merge 100
MAXJOBS Cleanup 50
```

### C.3 Site-Pinning Scripts

```bash
#!/bin/bash
# elect_site.sh — POST script for landing node
# Reads the CMS site name from the completed job's matched classad.
# $1 = path to write elected site file
ELECTED_SITE_FILE=$1
CLUSTER_ID=$(condor_q -format "%d.0" ClusterId \
    -constraint 'DAGNodeName=="landing"' 2>/dev/null)
SITE=$(condor_history $CLUSTER_ID -limit 1 -af MATCH_GLIDEIN_CMSSite 2>/dev/null)
if [ -n "$SITE" ]; then
    echo "$SITE" > "$ELECTED_SITE_FILE"
    exit 0
else
    echo "ERROR: Could not determine site from landing job" >&2
    exit 1
fi
```

```bash
#!/bin/bash
# pin_site.sh — PRE script for processing/merge/cleanup nodes
# Rewrites +DESIRED_Sites in the submit file to the elected site.
# $1 = submit file path, $2 = elected site file path
SUBMIT_FILE=$1
SITE=$(cat "$2")
sed -i "s/+DESIRED_Sites = .*/+DESIRED_Sites = \"${SITE}\"/" "$SUBMIT_FILE"
exit 0
```
