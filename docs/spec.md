# WMS2 — CMS Workload Management System v2

**OpenSpec v1.0**

| Field | Value |
|---|---|
| **Spec Version** | 2.7.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-23 |
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

**Work unit**: The atomic unit of progress in WMS2. A work unit is a self-contained set of processing jobs whose outputs feed a single merge job, producing one usable merged output. Each work unit is implemented as a merge group — a SUBDAG EXTERNAL containing a landing node, processing nodes, a merge node, and a cleanup node. WMS2 measures progress, calculates partial production fractions, and reports completion in terms of work units. A DAG consists of many work units running concurrently (throttled by `MAXJOBS`).

**Processing block**: A group of work units that form the unit of DBS block registration and tape archival. When a request is split into work units, those work units are grouped into processing blocks based on expected output size and tape-friendliness criteria. A processing block maps 1:1 to a DBS block and a tape archival unit. As individual work units within a block complete, their files are registered in DBS (opening the block on first completion) and protected at the source site via Rucio. When all work units in a block complete, the DBS block is closed and a tape archival rule is created for the entire block as a single unit — ensuring contiguous tape writes and efficient recall. WMS2 prioritizes completing in-progress blocks before starting new ones.

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

    # Resource Hints (initial defaults for first DAG round)
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
    urgent: bool = False            # Prioritize in admission queue

    # Partial Production — fair-share scheduling: produce a fraction of work
    # units at each priority level before yielding to other requests.
    # Steps are consumed front-to-back. After the last step triggers, the
    # request runs to completion at that step's priority.
    # Example: [{"fraction": 0.1, "priority": 80000}, {"fraction": 0.5, "priority": 50000}]
    #   → first 10% of work units at original priority, next 40% at 80k, rest at 50k
    production_steps: List[ProductionStep] = []

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
    PLANNING = "planning"          # DAG Planner building production DAG
    ACTIVE = "active"              # DAG submitted to DAGMan
    STOPPING = "stopping"          # Clean stop in progress (condor_rm issued)
    RESUBMITTING = "resubmitting"  # Recovery DAG prepared, re-entering queue
    COMPLETED = "completed"        # All nodes succeeded, outputs registered
    PARTIAL = "partial"            # DAG finished with some failures, recoverable
    FAILED = "failed"              # Catastrophic failure or retries exhausted
    ABORTED = "aborted"            # Manually cancelled


class ProductionStep(BaseModel):
    """A single step in the partial production schedule."""
    fraction: float    # Cumulative fraction of work units to complete (0.0, 1.0)
    priority: int      # Priority for the remainder after this step triggers
    # Validation: fractions must be strictly increasing across steps;
    # priorities must be strictly decreasing. Mutually exclusive with urgent=True.


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

    # Accumulated per-step FJR metrics from completed jobs (populated after
    # recovery rounds). Passed to sandbox via step_profile.json for adaptive
    # per-step optimization. Structure defined in Section 5.6.
    step_metrics: Optional[Dict[str, Any]]

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
    PLANNING = "planning"
    ACTIVE = "active"
    STOPPING = "stopping"              # Clean stop in progress
    RESUBMITTING = "resubmitting"      # Recovery DAG prepared
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    ABORTED = "aborted"


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

    # Work unit tracking
    total_work_units: int = 0                    # Total work units in this DAG
    completed_work_units: List[str] = []         # SUBDAG node names reported as completed

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

    # Resource estimates (from request hints or step_metrics)
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

#### 3.1.4 Processing Block

```python
class ProcessingBlock(BaseModel):
    """
    A group of work units that form the unit of DBS block registration and
    tape archival. Maps 1:1 to a DBS block and a tape archival unit.

    Lifecycle:
      OPEN → work units complete, files registered in DBS, source protection rules created
      COMPLETE → all work units done, DBS block closed, tape archival rule created
      ARCHIVED → tape rule created successfully (terminal)
      FAILED → Rucio call failed after all retries exhausted
    """
    id: UUID
    workflow_id: UUID
    block_index: int                         # 0-based index within the workflow
    dataset_name: str                        # /Primary/Processed/Tier

    # Work unit tracking
    total_work_units: int                    # Number of work units in this block
    completed_work_units: List[str] = []     # Node names of completed work units

    # DBS block
    dbs_block_name: Optional[str]            # DBS block name (set on first file registration)
    dbs_block_open: bool = False             # Block created in DBS
    dbs_block_closed: bool = False           # Block closed in DBS (all files registered)

    # Rucio source protection (one rule per completed work unit)
    source_rule_ids: Dict[str, str] = {}     # work_unit_node_name → rule_id

    # Rucio tape archival (created when block completes)
    tape_rule_id: Optional[str]              # Rucio rule ID for tape archival

    # Retry tracking for Rucio failures
    last_rucio_attempt: Optional[datetime]
    rucio_attempt_count: int = 0
    rucio_last_error: Optional[str]

    # State
    status: BlockStatus

    # Timestamps
    created_at: datetime
    updated_at: datetime


class BlockStatus(str, Enum):
    OPEN = "open"                  # Accepting work unit completions
    COMPLETE = "complete"          # All work units done, ready for tape archival
    ARCHIVED = "archived"          # Tape archival rule created (terminal)
    FAILED = "failed"              # Rucio call failed after max retries
    INVALIDATED = "invalidated"    # Outputs invalidated during catastrophic recovery
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
    production_steps JSONB DEFAULT '[]',
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
    step_metrics JSONB,
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
    total_work_units INTEGER DEFAULT 0,
    completed_work_units JSONB DEFAULT '[]',
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

CREATE TABLE processing_blocks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_id UUID REFERENCES workflows(id),
    block_index INTEGER NOT NULL,
    dataset_name VARCHAR(500) NOT NULL,
    -- Work unit tracking
    total_work_units INTEGER NOT NULL,
    completed_work_units JSONB DEFAULT '[]',
    -- DBS block
    dbs_block_name VARCHAR(500),
    dbs_block_open BOOLEAN DEFAULT FALSE,
    dbs_block_closed BOOLEAN DEFAULT FALSE,
    -- Rucio source protection (work_unit_name → rule_id)
    source_rule_ids JSONB DEFAULT '{}',
    -- Rucio tape archival
    tape_rule_id VARCHAR(100),
    -- Retry tracking for Rucio failures
    last_rucio_attempt TIMESTAMPTZ,
    rucio_attempt_count INTEGER DEFAULT 0,
    rucio_last_error TEXT,
    -- State
    status VARCHAR(50) NOT NULL DEFAULT 'open',
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
CREATE INDEX idx_processing_blocks_workflow ON processing_blocks(workflow_id);
CREATE INDEX idx_processing_blocks_status ON processing_blocks(status);
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
│     - Split input data into DAG node specs                      │
│     - Use request hints (round 1) or step_metrics (round 2+)   │
│     - Plan merge nodes based on output size estimates           │
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
│     - Register output files in DBS as work units complete       │
│     - Create Rucio source protection rules per work unit        │
│     - Close DBS block and create tape archival rule when        │
│       processing block completes                                │
│     - Retry Rucio calls with backoff on failure                 │
│     - Invalidate outputs for catastrophic failure recovery      │
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
│     - Processing block progress                                 │
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
        """Check admission capacity and start DAG planning or rescue DAG."""
        if not await self.admission_controller.has_capacity():
            return  # Wait for capacity

        next_pending = await self.admission_controller.get_next_pending()
        if next_pending and next_pending.request_name != request.request_name:
            return  # Not this request's turn

        workflow = await self.db.get_workflow_by_request(request.request_name)

        # Rescue DAG re-admission: submit existing rescue DAG directly
        dag = await self.db.get_dag(workflow.dag_id) if workflow.dag_id else None
        if dag and dag.rescue_dag_path and dag.status == DAGStatus.READY:
            dagman_id, schedd = await self.dag_planner.submit_rescue_dag(dag)
            await self.db.update_dag(dag.id,
                dagman_cluster_id=dagman_id, schedd_name=schedd,
                status=DAGStatus.SUBMITTED, submitted_at=datetime.utcnow(),
            )
            await self.db.update_workflow(workflow.id, status=WorkflowStatus.ACTIVE)
            await self.transition(request, RequestStatus.ACTIVE)
            return

        # Fresh submission: plan and submit DAG using request hints
        await self.dag_planner.plan_and_submit(workflow)
        await self.transition(request, RequestStatus.ACTIVE)

    async def _handle_active(self, request: Request):
        """Poll DAG status, register/protect outputs as work units complete."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        if dag.status in (DAGStatus.SUBMITTED, DAGStatus.RUNNING):
            # DAG still running — poll it
            result = await self.dag_monitor.poll_dag(dag)

            if result.status == DAGStatus.RUNNING:
                for work_unit in result.newly_completed_work_units:
                    await self.output_manager.handle_work_unit_completion(
                        workflow.id, work_unit["block_id"], work_unit
                    )

                # Partial production: auto-stop when next step's fraction threshold reached
                if request.production_steps and dag.status == DAGStatus.RUNNING:
                    fraction_done = len(dag.completed_work_units) / max(dag.total_work_units, 1)
                    next_step = request.production_steps[0]
                    if fraction_done >= next_step.fraction:
                        await self.initiate_clean_stop(
                            request.request_name,
                            reason=f"Partial production step: {fraction_done:.1%} complete, "
                                   f"deprioritizing remainder to {next_step.priority}"
                        )
                        return

            elif result.status == DAGStatus.PARTIAL:
                await self.transition(request, RequestStatus.PARTIAL)
            elif result.status == DAGStatus.FAILED:
                await self.transition(request, RequestStatus.FAILED)

        # Process blocks every cycle — retry failed Rucio calls
        await self.output_manager.process_blocks_for_workflow(workflow.id)

        # Check if everything is done (DAG complete + all blocks archived)
        if dag.status == DAGStatus.COMPLETED:
            if await self._all_blocks_archived(workflow.id):
                await self.transition(request, RequestStatus.COMPLETED)
                # Clean up previous version blocks if this is a version increment
                if request.previous_version_request:
                    if request.cleanup_policy == CleanupPolicy.IMMEDIATE_CLEANUP:
                        prev_wf = await self.db.get_workflow_by_request(
                            request.previous_version_request)
                        if prev_wf:
                            await self.output_manager.invalidate_blocks(
                                prev_wf.id,
                                reason=f"Superseded by new version, immediate cleanup"
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
        After clean stop completes, aggregate per-step FJR metrics from
        completed work units and create a new DAG record pointing to
        the rescue DAG path. Transition to RESUBMITTING → QUEUED so the
        admission controller re-admits it.
        """
        # Aggregate per-step FJR metrics from completed work units.
        # These are stored on the workflow so the next DAG round can
        # use them for adaptive optimization (see Section 5).
        step_metrics = await self._aggregate_step_metrics(dag)
        if step_metrics:
            await self.db.update_workflow(workflow.id, step_metrics=step_metrics)

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
            total_work_units=dag.total_work_units,
            completed_work_units=dag.completed_work_units,
            status=DAGStatus.READY,
        )
        await self.db.create_dag(new_dag)
        await self.db.update_workflow(workflow.id,
            dag_id=new_dag.id, status=WorkflowStatus.RESUBMITTING
        )

        # Partial production: consume step, demote priority
        if request.production_steps:
            step = request.production_steps[0]
            remaining_steps = request.production_steps[1:]
            await self.db.update_request(request.request_name,
                priority=step.priority,
                production_steps=remaining_steps,
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

        # Invalidate processing blocks from failed version
        workflow = await self.db.get_workflow_by_request(request_name)
        await self.output_manager.invalidate_blocks(
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

    async def _all_blocks_archived(self, workflow_id: UUID) -> bool:
        """Check if all processing blocks for a workflow have reached ARCHIVED status."""
        blocks = await self.db.get_processing_blocks(workflow_id)
        return all(b.status == BlockStatus.ARCHIVED for b in blocks)

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
        blocks = await self.db.get_processing_blocks(workflow_id)
        return WorkflowStatusReport(
            workflow=workflow, dag=dag, blocks=blocks,
            progress_percent=self._calc_progress(dag, blocks),
            estimated_completion=self._estimate_completion(workflow, dag, blocks),
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
    Adaptive model: first round uses request resource hints; subsequent
    rounds (after recovery) use accumulated step_metrics from FJR data.
    See Section 5 for the adaptive execution model.
    """
    SPLITTERS = {
        SplittingAlgo.FILE_BASED: FileBasedSplitter,
        SplittingAlgo.EVENT_BASED: EventBasedSplitter,
        SplittingAlgo.LUMI_BASED: LumiBasedSplitter,
        SplittingAlgo.EVENT_AWARE_LUMI: EventAwareLumiSplitter,
    }

    # ── DAG Planning and Submission ──────────────────────────────

    async def plan_and_submit(self, workflow: Workflow) -> DAG:
        """
        Build and submit the production DAG.

        Round 1 (no step_metrics): uses request resource hints directly.
        Round 2+ (step_metrics populated from previous round's FJR data):
        uses measured peak RSS for memory sizing. Writes step_profile.json
        to submit dir so sandbox can apply per-step adaptive optimization.
        """
        request = await self.db.get_request(workflow.request_name)

        # Determine resource parameters from step_metrics or request hints
        if workflow.step_metrics:
            # Recovery round — use measured metrics for sizing
            sm = workflow.step_metrics
            memory_mb = max(
                s["rss_mb"] for s in sm["steps"].values()
            ) + 512  # headroom
            events_per_job = request.splitting_params.get("events_per_job", 10000)
            time_per_job = int(request.time_per_event_sec * events_per_job)
            output_size_per_event = request.size_per_event_kb
        else:
            # First round — use request defaults
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

        # Write step_profile.json if step_metrics exist (recovery round).
        # The sandbox reads this file for per-step adaptive optimization
        # (see Section 5.4). Absent on round 1 — sandbox uses defaults.
        if workflow.step_metrics:
            profile_path = os.path.join(submit_dir, "step_profile.json")
            with open(profile_path, "w") as f:
                json.dump(workflow.step_metrics, f)

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
            total_work_units=len(merge_groups),
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
        Group processing nodes into work units based on expected output size.
        Each work unit becomes a SUBDAG EXTERNAL (merge group) containing:
          landing node → processing nodes → merge node → cleanup node
        Group composition is fixed at planning time (from request hints
        or step_metrics). Site assignment is deferred to runtime (landing
        node mechanism).
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
    """A work unit — a self-contained merge group implementing one unit of progress."""
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

    async def submit_rescue_dag(self, dag: DAG) -> Tuple[str, str]:
        """
        Submit a rescue DAG for a previously stopped DAG. Thin wrapper
        over condor_submit_dag — the rescue DAG file already exists
        (written by DAGMan on clean stop). DAGMan automatically skips
        nodes marked as done in the rescue file.
        """
        return await self.condor_adapter.submit_dag(
            dag_file=dag.dag_file_path, submit_dir=dag.submit_dir,
            rescue_dag=dag.rescue_dag_path,
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
    newly_completed_work_units: List[Dict] = []


class DAGMonitor:
    """
    Polls DAGMan status for a single DAG and returns results.
    Called by the Request Lifecycle Manager — not an independent loop.

    Work unit detection: each work unit is a SUBDAG EXTERNAL node in the
    top-level DAG. When a SUBDAG node status changes to STATUS_DONE, the
    work unit's merge output is ready. The monitor compares current SUBDAG
    node completions against a DB-stored set (dag.completed_work_units) to
    find newly completed work units, avoiding duplicate reports across
    polling cycles.
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

        # Detect work units that completed since the last poll
        newly_completed_work_units = await self._detect_completed_work_units(dag, summary)

        return DAGPollResult(
            dag_id=dag.id, status=DAGStatus.RUNNING,
            nodes_idle=summary.idle, nodes_running=summary.running,
            nodes_done=summary.done, nodes_failed=summary.failed,
            nodes_held=summary.held,
            newly_completed_work_units=newly_completed_work_units,
        )

    async def _detect_completed_work_units(self, dag: DAG,
                                           summary: NodeSummary) -> List[Dict]:
        """
        Compare current per-node statuses against the set of work units
        already reported as completed (stored in dag.completed_work_units).

        SUBDAG EXTERNAL nodes are named "mg_<N>" by the DAG Planner
        (see Section 4.5). When such a node reaches STATUS_DONE, we read its
        output manifest to get the dataset name and site, then record it as
        completed so it's not re-emitted on the next cycle.

        Returns a list of dicts suitable for OutputManager.handle_work_unit_completion().
        """
        already_completed = set(dag.completed_work_units or [])
        newly_completed = []

        for node_name, status in summary.node_statuses.items():
            if not node_name.startswith("mg_"):
                continue
            if status != "STATUS_DONE":
                continue
            if node_name in already_completed:
                continue

            # Read the work unit's output manifest written by its cleanup node.
            # The manifest contains the dataset name and site where the merged
            # output was produced (written by POST script of the merge node).
            manifest = await self._read_merge_manifest(dag, node_name)
            if manifest:
                newly_completed.append({
                    "node_name": node_name,
                    "block_id": manifest["block_id"],
                    "dataset_name": manifest["dataset_name"],
                    "site": manifest["site"],
                    "output_files": manifest.get("output_files", []),
                })
            already_completed.add(node_name)

        # Persist so we don't re-report on the next poll cycle
        if newly_completed:
            await self.db.update_dag(dag.id,
                completed_work_units=list(already_completed),
            )

        return newly_completed

    async def _read_merge_manifest(self, dag: DAG, group_node_name: str) -> Optional[Dict]:
        """
        Read the output manifest for a completed merge group.

        Each merge group's cleanup node writes a JSON manifest at a
        predictable path: <submit_dir>/<group_name>/merge_output.json
        containing: block_id, dataset_name, site, output_files (list of
        {lfn, size, checksum}), file_count, total_bytes.
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

The Output Manager handles the post-compute data pipeline: DBS file registration, Rucio source protection, DBS block closure, and tape archival rule creation. It operates at the **processing block** level — groups of work units that map 1:1 to DBS blocks and tape archival units.

The design is fire-and-forget: WMS2 creates Rucio rules and moves on. It does not poll transfer status, manage source protection lifecycle, or track tape completion. Once a rule is created, Rucio owns the outcome. WMS2's only follow-up responsibility is retry with backoff if a Rucio call fails.

```python
class OutputManager:
    """
    Handles the post-compute data pipeline at the processing block level.

    Three interactions per work unit completion:
      1. Register files in DBS (open block if first work unit)
      2. Create Rucio source protection rule

    One interaction per block completion:
      3. Close DBS block, create tape archival rule

    Called by the Request Lifecycle Manager — not an independent loop.
    """

    # Retry configuration for Rucio failures
    RUCIO_MAX_RETRY_DURATION = timedelta(days=3)    # Keep retrying for up to 3 days
    RUCIO_BACKOFF_SCHEDULE = [60, 300, 1800, 7200, 14400, 28800]  # seconds: 1m, 5m, 30m, 2h, 4h, 8h

    async def handle_work_unit_completion(self, workflow_id: UUID, block_id: UUID,
                                          merge_info: dict):
        """
        Called when a work unit completes. Registers output files in DBS
        and creates a Rucio source protection rule.
        """
        block = await self.db.get_processing_block(block_id)

        # 1. Register files in DBS (opens DBS block on first work unit)
        if not block.dbs_block_open:
            block_name = await self.dbs_adapter.open_block(
                block.dataset_name, block.block_index,
            )
            await self.db.update_processing_block(block.id,
                dbs_block_name=block_name, dbs_block_open=True,
            )

        await self.dbs_adapter.register_files(
            block_name=block.dbs_block_name or block_name,
            files=merge_info["output_files"],
        )

        # 2. Create Rucio source protection rule
        rule_id = await self._rucio_call_with_retry(
            block, "create_rule",
            self.rucio_adapter.create_rule,
            dataset=block.dataset_name,
            site=merge_info["site"],
            lifetime=None,
        )

        # 3. Update block state
        node_name = merge_info["node_name"]
        source_rules = dict(block.source_rule_ids)
        source_rules[node_name] = rule_id
        completed = list(block.completed_work_units) + [node_name]

        await self.db.update_processing_block(block.id,
            completed_work_units=completed,
            source_rule_ids=source_rules,
        )

        # 4. Check if block is now complete
        if len(completed) >= block.total_work_units:
            await self._complete_block(block)

    async def _complete_block(self, block: ProcessingBlock):
        """
        All work units in the block are done. Close the DBS block
        and create a tape archival rule for the entire block.
        """
        # Close DBS block
        await self.dbs_adapter.close_block(block.dbs_block_name)
        await self.db.update_processing_block(block.id,
            dbs_block_closed=True, status=BlockStatus.COMPLETE,
        )

        # Create tape archival rule for the whole block
        tape_rule_id = await self._rucio_call_with_retry(
            block, "create_tape_rule",
            self.rucio_adapter.create_rule,
            dataset=block.dataset_name,
            rse_expression="tier=1&type=TAPE",  # Tape RSE expression
        )

        await self.db.update_processing_block(block.id,
            tape_rule_id=tape_rule_id, status=BlockStatus.ARCHIVED,
        )

    async def process_blocks_for_workflow(self, workflow_id: UUID):
        """
        Process any blocks that need retry for failed Rucio calls.
        Called by the Lifecycle Manager every cycle.
        """
        blocks = await self.db.get_processing_blocks(workflow_id)
        for block in blocks:
            if block.status == BlockStatus.COMPLETE:
                # Block complete but tape rule not yet created — retry
                await self._retry_tape_archival(block)
            elif block.status == BlockStatus.OPEN and block.rucio_last_error:
                # Source protection failed for a work unit — retry
                await self._retry_source_protection(block)

    async def _retry_tape_archival(self, block: ProcessingBlock):
        """Retry tape archival rule creation with backoff."""
        if not self._should_retry(block):
            return
        try:
            tape_rule_id = await self.rucio_adapter.create_rule(
                dataset=block.dataset_name,
                rse_expression="tier=1&type=TAPE",
            )
            await self.db.update_processing_block(block.id,
                tape_rule_id=tape_rule_id, status=BlockStatus.ARCHIVED,
                rucio_last_error=None, rucio_attempt_count=0,
            )
        except Exception as e:
            await self._record_rucio_failure(block, e)

    async def _rucio_call_with_retry(self, block, operation, func, **kwargs):
        """
        Attempt a Rucio call. On failure, record the error for retry
        on the next Lifecycle Manager cycle.
        """
        try:
            result = await func(**kwargs)
            return result
        except Exception as e:
            await self._record_rucio_failure(block, e)
            raise

    def _should_retry(self, block: ProcessingBlock) -> bool:
        """Check if retry is allowed (within max duration, respecting backoff)."""
        if not block.last_rucio_attempt:
            return True

        # Check max duration
        elapsed = datetime.utcnow() - block.created_at
        if elapsed > self.RUCIO_MAX_RETRY_DURATION:
            logger.error(
                f"Block {block.id} ({block.dataset_name}): Rucio retry "
                f"exhausted after {elapsed}. Last error: {block.rucio_last_error}. "
                f"Marking FAILED. Manual intervention required."
            )
            # Dump full context for operator
            self._dump_failure_context(block)
            asyncio.create_task(self.db.update_processing_block(
                block.id, status=BlockStatus.FAILED,
            ))
            return False

        # Check backoff schedule
        attempt = min(block.rucio_attempt_count, len(self.RUCIO_BACKOFF_SCHEDULE) - 1)
        backoff = timedelta(seconds=self.RUCIO_BACKOFF_SCHEDULE[attempt])
        if datetime.utcnow() - block.last_rucio_attempt < backoff:
            return False  # Backoff not elapsed

        return True

    async def _record_rucio_failure(self, block, error):
        """Record a Rucio call failure for later retry."""
        await self.db.update_processing_block(block.id,
            last_rucio_attempt=datetime.utcnow(),
            rucio_attempt_count=block.rucio_attempt_count + 1,
            rucio_last_error=str(error),
        )
        logger.warning(
            f"Block {block.id} ({block.dataset_name}): Rucio call failed "
            f"(attempt {block.rucio_attempt_count + 1}): {error}"
        )

    def _dump_failure_context(self, block: ProcessingBlock):
        """Dump full block context for operator diagnosis."""
        logger.error(
            f"RUCIO FAILURE CONTEXT for block {block.id}:\n"
            f"  dataset: {block.dataset_name}\n"
            f"  block_index: {block.block_index}\n"
            f"  work_units: {len(block.completed_work_units)}/{block.total_work_units}\n"
            f"  dbs_block: {block.dbs_block_name} (closed={block.dbs_block_closed})\n"
            f"  source_rules: {block.source_rule_ids}\n"
            f"  tape_rule: {block.tape_rule_id}\n"
            f"  attempts: {block.rucio_attempt_count}\n"
            f"  last_error: {block.rucio_last_error}\n"
            f"  first_attempt: {block.created_at}\n"
            f"  last_attempt: {block.last_rucio_attempt}"
        )

    async def invalidate_blocks(self, workflow_id: UUID, reason: str):
        """
        Invalidate all processing blocks for a workflow during catastrophic
        recovery. Deletes Rucio rules and invalidates DBS entries.
        """
        blocks = await self.db.get_processing_blocks(workflow_id)
        for block in blocks:
            # Delete Rucio source protection rules
            for node_name, rule_id in block.source_rule_ids.items():
                try:
                    await self.rucio_adapter.delete_rule(rule_id)
                except Exception as e:
                    logger.warning(f"Failed to delete source rule {rule_id}: {e}")

            # Delete tape archival rule
            if block.tape_rule_id:
                try:
                    await self.rucio_adapter.delete_rule(block.tape_rule_id)
                except Exception as e:
                    logger.warning(f"Failed to delete tape rule {block.tape_rule_id}: {e}")

            # Invalidate DBS block
            if block.dbs_block_open:
                try:
                    await self.dbs_adapter.invalidate_block(block.dbs_block_name)
                except Exception as e:
                    logger.warning(f"Failed to invalidate DBS block {block.dbs_block_name}: {e}")

            await self.db.update_processing_block(block.id,
                status=BlockStatus.INVALIDATED,
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

        for status in BlockStatus:
            count = await self.db.count_processing_blocks_by_status(status)
            self.gauge("wms2_blocks", count, labels={"status": status.value})
```

---

## 5. Adaptive Execution Model

### 5.1 Overview

WMS2 does not run a separate pilot job. Instead, the first production DAG uses resource hints from the request (TimePerEvent, Memory, SizePerEvent) as defaults. As work units complete, their Framework Job Report (FJR) data — per-step CPU efficiency, peak RSS, wall time — is collected by the sandbox and written to merge manifests. When a recovery round occurs (rescue DAG after clean stop, partial failure, or partial production step), the Lifecycle Manager aggregates this FJR data into `step_metrics` on the workflow, and the DAG Planner writes it to `step_profile.json` in the submit directory. The sandbox reads this file and applies per-step adaptive optimization.

This design eliminates the latency of a dedicated pilot phase (~8 hours) while converging to optimal resource estimates asymptotically through production data. The existing recovery/rescue DAG mechanism is the natural re-optimization point — no new states, no new components. See `docs/adaptive.md` for the complete algorithm specification covering thread count rounding, memory source hierarchy, probe node design, job split, pipeline split, and multi-round convergence.

### 5.2 Memory-Per-Core Window

CMS sites advertise resources as memory per core. There is a minimum that most sites offer, but no fixed maximum — requesting more memory per core narrows the pool of matching sites. WMS2 defines two operational parameters to bound the adaptive algorithm:

- **`default_memory_per_core`** (MB): The baseline memory per core. Matches the widest set of sites. Round 1 production jobs use this value. Example: 2000 MB/core.
- **`max_memory_per_core`** (MB): The upper bound the adaptive algorithm may request. Beyond this, too few sites match to be practical. Probe jobs and memory-constrained Round 2+ jobs may use up to this value. Example: 3000 MB/core.

These are operational knobs — set by the WMS2 deployment, not per-request. They reflect the site landscape and can be tuned as site capabilities change.

**Request validation**: A request arrives with `Multicore` (thread count) and `Memory` (total MB). The DAG Planner checks that the request's memory requirement fits within the window:

```
request_memory_per_core = request.Memory / request.Multicore

if request_memory_per_core > max_memory_per_core:
    reject — request cannot be satisfied
if request_memory_per_core > default_memory_per_core:
    Round 1 uses request_memory_per_core (accepted, but fewer sites)
else:
    Round 1 uses default_memory_per_core × Multicore
```

### 5.3 Execution Rounds

**Round 1** (fresh submission, no `step_profile.json`):
- **Production jobs**: Use `default_memory_per_core × Multicore` as `request_memory` (unless the request itself demands more — see Section 5.2). This maximizes the site pool.
- **Probe jobs** (optional, 1–2 jobs): Use `max_memory_per_core × Multicore` as `request_memory`. Probes test splitting configurations (e.g., 2 parallel instances within one slot) that need headroom to avoid OOM. The wider memory envelope is acceptable because only 1–2 jobs are affected. Probe results (cgroup memory peaks, CPU utilization) feed the Round 2 adaptive sizing.
- DAG Planner uses request resource hints: `Memory`, `TimePerEvent`, `SizePerEvent`
- Sandbox runs each job with default resource allocation
- FJR data is collected per work unit but not yet aggregated

**Round 2+** (rescue DAG after clean stop or partial failure):
- Lifecycle Manager aggregates FJR data from completed work units into `workflow.step_metrics`
- DAG Planner writes `step_profile.json` to submit directory
- Sandbox reads `step_profile.json` and applies per-step optimization:
  - Memory sizing based on measured data + 20% safety margin (Section 5.5), within the `[default, max] × Multicore` window
  - Per-step splitting for CPU-inefficient steps (Section 5.4)
- Rescue DAG skips completed nodes — only remaining work uses updated parameters

```
Round 1: Request hints → DAG → jobs run → FJR data collected
              │         (production: default mem/core)
              │         (probes: max mem/core)
              │
              ▼ (clean stop, partial failure, or production step)
         Lifecycle Manager: _prepare_recovery()
              │  Aggregates FJR data → workflow.step_metrics
              ▼
Round 2: step_metrics → DAG Planner writes step_profile.json
              │  Sandbox applies per-step optimization
              │  Memory sized to measured data + 20%, within [default, max] window
              ▼
         Rescue DAG resumes with adaptive parameters
              │
              ▼ (if another recovery round)
Round 3+: Refined step_metrics → further optimization (diminishing returns)
```

### 5.4 Per-Step Splitting Optimization

Per-step splitting is a sandbox-owned optimization. When `step_profile.json` is present, the sandbox can split CPU-inefficient steps into N parallel `cmsRun` processes within a single slot, using `skipEvents`/`maxEvents` partitioning.

**Decision rule**: If a step's CPU efficiency (CPU time / wall time / threads) is below a threshold (e.g., 50%), splitting may help. The number of parallel instances N is constrained by:
- Memory: N instances must fit within the slot's `request_memory`
- CPU: N should not exceed `request_cpus` (the allocated cores)

**Example**: A GEN-SIM step running on 8 cores at 20% CPU efficiency (effectively using 1.6 cores). With measured RSS of 800 MB per instance and 4 GB slot memory, the sandbox could run 4 parallel single-threaded cmsRun processes (4 × 800 MB = 3.2 GB < 4 GB), each processing 1/4 of the events via `skipEvents`/`maxEvents`. This is entirely the sandbox's decision — WMS2 only provides the metrics.

**CPU overcommit** is a complementary optimization: instead of (or in addition to) splitting, give each cmsRun instance more threads than its proportional core share. The extra threads fill I/O bubbles — when some threads are waiting on disk or network, the overcommitted threads can use the idle CPU. For example, on 8 allocated cores with 2 parallel step 0 instances, each could get 5 threads (10 total, 25% overcommit) instead of 4. Steps 1+ running sequentially could get 10 threads instead of 8. Overcommit is optional (off by default, `overcommit_max=1.0`) and memory-safe: the projected RSS including per-thread overhead (conservatively 250 MB/thread for CMSSW) must fit within `request_memory`. Only steps with moderate CPU efficiency (50–90%) benefit from overcommit — low-efficiency steps need splitting, and high-efficiency steps are already saturating their threads. The scheduler-visible `request_cpus` is unchanged; overcommit operates entirely within the sandbox.

### 5.5 Memory Sizing

The adaptive algorithm sizes `request_memory` for Round 2+ based on measured data from Round 1 (or earlier rounds). All measured values include a **20% safety margin** (default, configurable) to account for:
- Memory leak accumulation over longer production runs (probes/early jobs process fewer events)
- Event-to-event variation in RSS across different input data
- Page cache pressure under concurrent load

**Data sources** (in order of preference):

1. **Cgroup peak** (from probe jobs): HTCondor's cgroup accounting captures the full memory footprint — process RSS, tmpfs (e.g., gridpack extraction), and page cache. This is the most accurate measurement because it reflects what the cgroup OOM killer actually enforces. Available when probe jobs run in Round 1.
2. **FJR peak RSS** (from completed work units): The Framework Job Report's `PeakValueRss` measures process RSS only — it misses tmpfs and page cache. Available from Round 2+ via `step_metrics` aggregation. Median of all sampled jobs, robust against outliers.

**Sizing formula**:

```
measured_memory = best available measurement (cgroup or FJR RSS)
request_memory  = measured_memory × 1.20
request_memory  = clamp(request_memory, default_memory_per_core × cores, max_memory_per_core × cores)
```

If the clamped value at `max_memory_per_core × cores` is still below what the measured data requires, the adaptive algorithm must reduce the number of parallel instances (fewer splits) rather than exceed the memory ceiling.

**Thread count extrapolation**: Memory usage varies with thread count. Two data points enable a linear model:

```
RSS(threads) = base + per_thread × threads
```

- **Point 1**: Round 1 measurement (e.g., 8 threads, 2400 MB)
- **Point 2**: Round 2 measurement at a different thread count (if the sandbox adjusts threads)

With only one data point (common case), the 20% safety margin on the measured value absorbs the extrapolation uncertainty — a percentage scales naturally with job size (unlike a fixed MB buffer).

### 5.6 Metric Aggregation

The Lifecycle Manager aggregates per-step FJR data from completed work units during `_prepare_recovery()`. The aggregation uses medians to be robust against outliers (e.g., one job hitting a slow node).

**`step_metrics` structure**:

```json
{
  "round": 1,
  "jobs_sampled": 42,
  "steps": {
    "DIGI": {
      "cpu_efficiency": 0.85,
      "rss_mb": 1800,
      "wall_sec_per_event": 0.05,
      "threads": 8
    },
    "RECO": {
      "cpu_efficiency": 0.72,
      "rss_mb": 2400,
      "wall_sec_per_event": 0.08,
      "threads": 8
    },
    "MINIAODSIM": {
      "cpu_efficiency": 0.18,
      "rss_mb": 800,
      "wall_sec_per_event": 0.12,
      "threads": 8
    }
  }
}
```

The `round` counter increments with each recovery. On round 2+, previous round metrics are merged (weighted by `jobs_sampled`) so the model accumulates data across rounds.

### 5.7 Convergence

- **Round 1**: Request defaults. May be suboptimal but functional.
- **Round 2**: Most gains realized — memory sized to actual RSS, per-step splitting applied to inefficient steps.
- **Round 3+**: Diminishing returns. Metrics refined with more data points but parameters are already near-optimal.

Partial production steps (Section 6.2.1) are natural re-optimization points: after the first 10% completes, the rescue DAG for the remaining 90% carries accumulated metrics. This means the bulk of every workflow benefits from measured data, even on the first submission.

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

#### 6.2.1 Partial Production Flow

Partial production is a fair-share scheduling mechanism: when many requests compete for resources, every request gets at least a bare-minimum fraction of its results quickly, then the remainder is deprioritized so other requests get their share. This prevents a single large request from monopolizing the system while others wait.

The mechanism reuses the clean stop + rescue DAG flow (Section 6.2) — no new states, no new DAG types. The `production_steps` field on the request defines a list of fraction thresholds with corresponding demoted priorities. As the DAG runs, the Lifecycle Manager checks the fraction of completed work units against the next step's threshold. When the threshold is reached, a clean stop is triggered automatically.

**Multi-step example**: A request with priority 100000 and `production_steps = [{"fraction": 0.1, "priority": 80000}, {"fraction": 0.5, "priority": 50000}]` runs as follows:

1. First 10% of work units execute at priority 100000 (original)
2. At 10% complete → auto clean stop → priority demoted to 80000 → step consumed
3. Next 40% of work units (10%→50%) execute at priority 80000
4. At 50% complete → auto clean stop → priority demoted to 50000 → step consumed
5. Remaining 50% of work units execute at priority 50000 → run to completion

```
Request submitted with production_steps = [{0.1, 80000}, {0.5, 50000}]
  │
  ▼
ACTIVE at priority 100000
  │  work units complete...
  │  fraction_done >= 0.1
  ▼
Auto clean stop (same as operator stop)
  ├── STOPPING → RESUBMITTING → QUEUED
  ├── _prepare_recovery(): consume step, set priority=80000
  │
  ▼
ACTIVE at priority 80000 (rescue DAG, carries step_metrics)
  │  work units complete...
  │  fraction_done >= 0.5
  ▼
Auto clean stop
  ├── STOPPING → RESUBMITTING → QUEUED
  ├── _prepare_recovery(): consume step, set priority=50000
  │
  ▼
ACTIVE at priority 50000 (rescue DAG, refined step_metrics)
  │  production_steps is now empty → run to completion
  ▼
COMPLETED
```

**Adaptive optimization on rescue DAG**: When a request re-enters QUEUED after a clean stop, the Lifecycle Manager has aggregated per-step FJR metrics from completed work units into `workflow.step_metrics` (see Section 5.6). The rescue DAG carries these accumulated metrics, and the sandbox uses them for per-step optimization (memory sizing, CPU-efficiency-based splitting). Each successive rescue round refines the estimates further.

**In-flight waste**: When the clean stop triggers, work units already in flight continue briefly until `condor_rm` takes effect. The waste is bounded by `MAXJOBS MergeGroup` (typically ~10 work units) — the actual fraction may slightly overshoot the requested threshold.

**Output continuity**: Work units that completed before the stop already have their files registered in DBS and source-protected in Rucio (per DD-4). Completed processing blocks have their tape archival rules created. The rescue DAG skips these completed work units. No outputs are lost or duplicated across stops.

### 6.3 Catastrophic Failure Recovery

When a schedd is permanently lost (hardware failure, unrecoverable corruption), the DAG and all its in-flight state are gone. The recovery path uses **dataset versioning**: increment `processing_version` and resubmit from scratch.

**State flow**: Failed request → new request with `processing_version + 1`

```
Operator calls POST /api/v1/requests/{name}/restart
  │
  ▼
Lifecycle Manager: handle_catastrophic_failure()
  ├── Invalidate processing blocks from failed version:
  │     ├── DBS: invalidate block entries
  │     └── Rucio: delete source protection and tape archival rules
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

**Block invalidation rollback table** — every forward block state has a corresponding undo operation:

| Forward State | Undo Operation |
|---|---|
| `OPEN` | Delete Rucio source protection rules for completed work units; invalidate DBS block if open |
| `COMPLETE` | Delete Rucio source protection rules; invalidate DBS block |
| `ARCHIVED` | Delete Rucio source protection rules + tape archival rule; invalidate DBS block |

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
    "step_metrics": {
        "round": 1,
        "jobs_sampled": 42,
        "steps": {
            "DIGI": {"cpu_efficiency": 0.85, "rss_mb": 1800, "wall_sec_per_event": 0.05, "threads": 8},
            "RECO": {"cpu_efficiency": 0.72, "rss_mb": 2400, "wall_sec_per_event": 0.08, "threads": 8}
        }
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
    "blocks": [
        {
            "dataset_name": "/Run2024A/RECO/v1",
            "block_index": 0,
            "status": "open",
            "work_units": "12/25"
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

# Submit with partial production (single step: 10% at full priority, rest at 80k)
POST /api/v1/requests
{
    "request_name": "user_Run2024B_v1_250210_654321",
    "source": "reqmgr2",
    "production_steps": [
        {"fraction": 0.1, "priority": 80000}
    ]
}

# Submit with multi-step partial production
POST /api/v1/requests
{
    "request_name": "user_Run2024C_v1_250211_111111",
    "source": "reqmgr2",
    "production_steps": [
        {"fraction": 0.1, "priority": 80000},
        {"fraction": 0.5, "priority": 50000}
    ]
}

Response:
{
    "request_name": "user_Run2024C_v1_250211_111111",
    "status": "submitted",
    "production_steps": [
        {"fraction": 0.1, "priority": 80000},
        {"fraction": 0.5, "priority": 50000}
    ],
    "workflow": {
        "id": "770e8400-e29b-41d4-a716-446655440002",
        "status": "new"
    }
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

### 10.2 Phase 2: DAG Planning Pipeline + Adaptive Metrics (Weeks 5–8)

**Goal**: Request → DAG file generation with adaptive optimization.

| Component | Description | Priority |
|---|---|---|
| ReqMgr2 Adapter | Fetch requests, validate schema | P0 |
| DBS Adapter | Query datasets, get file lists | P0 |
| Workflow Manager | Create workflows from requests | P0 |
| Admission Controller | Capacity check and priority ordering | P0 |
| File-Based Splitter | File-based splitting → DAGNodeSpecs | P0 |
| Event-Based Splitter | Event-based splitting | P1 |
| Merge Planner | Plan merge nodes from request hints / step_metrics | P0 |
| DAG File Generator | Generate .dag and .sub files | P0 |
| Step Metrics Aggregation | Aggregate FJR data from completed work units | P0 |
| step_profile.json Writer | Write step metrics to submit dir for sandbox | P0 |

**Deliverables**: Can import request from ReqMgr2; DAG files generated with resource estimates from request hints; step metrics aggregation tested; step_profile.json written for recovery rounds; unit tests for splitters and DAG generation.

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
| Output Manager | DBS registration, Rucio source protection, tape archival | P0 |
| Rucio Adapter | Rule creation, deletion | P0 |
| Block Invalidation | DBS invalidation + Rucio rule cleanup for catastrophic recovery | P1 |
| Site Manager | CRIC sync, site control | P0 |
| Dashboard API | Frontend data endpoints | P1 |
| Observability | Grafana dashboards | P1 |
| Documentation | Ops guide, API docs | P1 |

**Deliverables**: Full request lifecycle working (submit → DAG → outputs registered → blocks archived → adaptive optimization on recovery); clean stop and catastrophic recovery tested; site management; production-like deployment; monitoring dashboards; operator documentation.

**Testing**: See `docs/testing.md` for the complete testing specification covering test levels, matrix runner, cmsRun simulator, adaptive testing, fault injection, and environment verification.

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
| Adaptive Convergence | Resource estimates within 20% of actual by round 2 | Compare round 2 step_metrics-based sizing to measured usage |

### 13.2 Comparison with WMCore

| Aspect | WMCore | WMS2 Target |
|---|---|---|
| Codebase Size | ~500K lines | < 30K lines |
| Components | 15+ threads | 5–8 async workers |
| Databases | 3 (MySQL, Oracle, CouchDB) | 1 (PostgreSQL) |
| Job Tracking | Per-job in WMS DB | Delegated to DAGMan |
| Resource Estimation | Manual (requestor guesses) | Adaptive (converges from FJR data) |
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
| Processing block sizing | Medium | Medium | Block too small → tape fragmentation; block too large → data sits on disk too long. Needs tuning with real production patterns |
| First-round resource estimates inaccurate | Low | Medium | Auto-corrects from round 2 via step_metrics; conservative memory headroom on round 1 |
| Feature gaps discovered late | High | Medium | Close collaboration with operations team |
| Team bandwidth | Medium | High | Phased approach, MVP focus |
| Silent request stalling | High | Medium | Lifecycle Manager timeout detection alerts when any request exceeds its expected duration in a given status; no request can be "forgotten" |
| Schedd permanent loss | High | Low | Dataset versioning: increment processing_version, resubmit from scratch, invalidate or preserve partial outputs based on cleanup policy |
| DAG stop leaves orphaned state | Medium | Low | Clean stop flow ensures DAGMan writes rescue DAG before exit; new DAG record created with recovery linkage; request re-enters admission queue |
| Partial production fraction overshoot | Low | Medium | Actual fraction may slightly exceed requested threshold because in-flight work units complete after clean stop triggers; bounded by `MAXJOBS MergeGroup` (~10 work units) |

---

## 15. Open Questions

1. **Sandbox/Wrapper Design**: How does the per-node manifest communicate input data to the job? What format? This is tightly coupled to sandbox architecture and needs dedicated design discussion.
2. **Sandbox Scope**: Exact boundary between WMS2 and sandbox responsibilities — manifest generation, adaptive per-step optimization (reading `step_profile.json`, per-step splitting), POST script logic, FJR metric extraction.
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
14. **Work Unit Granularity**: With few work units (e.g., a workflow with only 5 merge groups), the actual fraction may differ significantly from the requested fraction. A `production_steps` entry of `{"fraction": 0.1}` cannot be honored — the closest achievable step is 20% (1 out of 5). Should WMS2 warn at submission time or silently round?
15. **Partial Production on ACTIVE Requests**: Can `production_steps` be set via `PATCH /api/v1/requests/{name}` on an already-ACTIVE request? This could trigger an immediate clean stop on the next Lifecycle Manager cycle if the fraction threshold is already met. Should this be allowed, or should `production_steps` be immutable after submission?
16. **Default Thread Count for First Round**: What default thread count should the sandbox use when no `step_profile.json` exists? Should it match the request's `Multicore` field, or should WMS2 suggest a conservative default? Per-step CPU efficiency data suggests some steps are highly inefficient at high thread counts.
17. **Metric Aggregation Strategy**: Should `step_metrics` use median or p90 for RSS? Median is robust to outliers but may underestimate memory needs for skewed distributions. P90 is safer but wastes memory for most jobs.
18. **Minimum Sample Size for Reliable step_metrics**: How many completed work units are needed before `step_metrics` are considered reliable? With only 2–3 work units, one outlier can skew medians significantly. Should there be a minimum sample threshold below which the DAG Planner ignores step_metrics and falls back to request hints?
19. **Processing Block Sizing**: What criteria determine how many work units go into a processing block? Options include: target total output size (e.g., 1–2 TB per block for efficient tape writes), fixed work unit count, or input-data-driven boundaries (e.g., one block per input DBS block). Too small → tape fragmentation; too large → data sits on disk unprotected by tape for too long. The right size depends on tape capacity (18 TB), expected file sizes (2–4 GB), and production timelines.
20. **Block Completion Priority**: Should WMS2 actively prioritize completing in-progress blocks before starting new ones? This could mean biasing work unit scheduling within a DAG to finish partial blocks first, or limiting how many blocks can be open simultaneously. Tradeoff: strict block completion ordering may reduce site utilization if a block's remaining work units are waiting for specific sites.
21. **Rucio Retry Max Duration**: What is the right default for `RUCIO_MAX_RETRY_DURATION`? Currently 3 days (covers a weekend outage). Should this be configurable per request? A campaign deadline might require shorter timeout with faster operator escalation.

---

## 16. Design Decisions

This section captures significant design decisions and the reasoning behind them. Each entry records *what* was decided, *why*, and *what alternatives were rejected*. This serves as the authoritative rationale — if the spec needs to be rebuilt or re-evaluated, start here.

### DD-1: Single Lifecycle Manager instead of independent polling loops

**Decision**: One component (Request Lifecycle Manager) owns all request state transitions via a single main loop, calling other components as stateless workers.

**Why**: With independent loops per component (Admission Controller loop, DAG Monitor loop, Output Manager loop), a request can silently stall if any single loop hiccups — there is no single place that notices "this request hasn't made progress." A single loop over all non-terminal requests makes stuck-state detection trivial and provides one place to add timeouts and observability.

**Rejected alternative**: Event-driven architecture where components emit events and subscribe to each other. Too complex for the number of components; harder to reason about ordering; WMS2's scale (hundreds of workflows, not millions) doesn't justify it.

### DD-2: Work units implemented as merge group SUBDAGs with landing nodes

**Decision**: Each work unit (processing nodes + merge + cleanup) is a self-contained sub-DAG declared via `SUBDAG EXTERNAL`. A trivial `/bin/true` landing node per work unit lets HTCondor pick the site; POST script reads `MATCH_GLIDEIN_CMSSite` and writes it to a file; PRE scripts on remaining nodes read that file and set `+DESIRED_Sites`. The work unit is the atomic unit of progress — WMS2 measures completion, calculates partial production fractions, and reports progress in terms of work units.

**Why**: Processing jobs feeding a merge job must run on the same site to avoid cross-site transfers before merging. But WMS2 should not do site selection — that's HTCondor's job. The landing node lets HTCondor's negotiator pick the site through normal matchmaking, and then pins the work unit. SUBDAG EXTERNAL keeps the sub-DAG internal to DAGMan; WMS2 sees only one top-level DAG.

**Rejected alternatives**:
- *WMS2 assigns sites at planning time*: Prevents HTCondor from load balancing. WMS2 would need to replicate the negotiator's logic.
- *Dynamic merge creation based on where processing outputs land*: Violates the principle of fixing merge group composition at planning time from resource estimates. Also much more complex.
- *PRE script site selection with WMS2 API calls*: Creates races between groups; effectively WMS2 doing load balancing through the back door.

### DD-3: Staggered merge group concurrency via MAXJOBS

**Decision**: Limit concurrent merge groups using DAGMan's `MAXJOBS MergeGroup` category. This creates backpressure so early groups' processing jobs fill site queues before later groups' landing nodes are scheduled.

**Why**: Without staggering, all landing nodes are trivial `/bin/true` jobs that could all land on the same site since they create no backpressure. With staggering, the first few groups' processing jobs create real resource demand, so later landing nodes naturally spread across sites where capacity remains.

**Rejected alternative**: No throttling, rely on HTCondor. Risk of poor site distribution is too high for merge-heavy workflows.

### DD-4: Output processing starts immediately as work units complete

**Decision**: The Lifecycle Manager calls `handle_work_unit_completion()` as soon as a work unit is detected as complete. DBS file registration and Rucio source protection happen immediately — they do not wait for the full processing block or DAG to complete.

**Why**: Work unit outputs sit on the source site's local storage. If we wait until the entire processing block or DAG completes (which could be hours or days later), the source site may clean up that space. DBS registration and Rucio source protection are urgent — they must happen as soon as the merge output exists. This also ensures that partial production stops do not lose already-completed outputs.

**Rejected alternative**: Process outputs only after block or DAG completion. Simpler but risks data loss for early-completing work units.

### DD-5: Processing blocks as the unit of DBS blocks and tape archival

**Decision**: Work units are grouped into processing blocks at DAG planning time. Each processing block maps 1:1 to a DBS block and a tape archival unit. Individual work unit completions trigger DBS file registration and source protection (urgent). Block completion triggers DBS block closure and a single tape archival rule for the entire block.

**Why**: Writing thousands of 2–4 GB files individually to tape causes dramatic fragmentation — files scatter across many 18 TB tapes, making dataset recall extremely slow (requires mounting dozens of tapes). Production can stretch over weeks or months (partial production at high priority, remainder via rescue DAG at lower priority), so tape system buffers (1–2 days) cannot consolidate on their own. Grouping work units into blocks ensures contiguous tape writes and efficient recall. The block also serves as a natural DBS block boundary, giving downstream consumers a meaningful unit of data availability.

**Rejected alternatives**:
- *Tape archival per work unit*: Causes tape fragmentation. Each 2–4 GB merged output would be written to tape independently, scattering across tapes.
- *Tape archival per entire request*: Delays archival too long — a request with thousands of work units may take months. Data sits on disk unprotected by tape for the entire duration.
- *Let Rucio/tape system consolidate*: Tape buffers only handle 1–2 day delays, not weeks/months. Rucio has no concept of "wait until related files accumulate."

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

### DD-9: Fire-and-forget Rucio interaction

**Decision**: WMS2 creates Rucio rules (source protection per work unit, tape archival per block) and does not track their progress. Once a rule is created, Rucio owns the outcome — transfers, source cleanup, tape writes, and subscription-driven distribution are all Rucio's responsibility. WMS2's only follow-up is retry with backoff if a rule creation call fails.

**Why**: WMS2 is an orchestrator, not a data management system. Rucio already handles transfer scheduling, status tracking, subscription-driven placement, and cleanup. Duplicating any of this in WMS2 (polling transfer status, managing source protection lifecycle, determining placement destinations) would create a fragile shadow of Rucio's own state machine. The previous design had WMS2 polling Rucio transfer status every 5 minutes across hundreds of workflows, managing an 8-state output lifecycle, and querying subscriptions to determine destinations — all of which is Rucio's job. The simplified design reduces WMS2's Rucio interaction to two API calls: `create_rule()` for source protection (per work unit) and `create_rule()` for tape archival (per block).

**Rejected alternative**: WMS2 tracks transfer completion, manages source protection lifecycle, queries Rucio subscriptions for placement. Over-engineered — WMS2 was doing Rucio's job.

### DD-10: Stuck request handling is status-aware, not one-size-fits-all

**Decision**: `_handle_stuck()` takes different actions based on which status the request is stuck in. Fast internal operations (SUBMITTED, PLANNING) are failed immediately. Slow external operations (ACTIVE with unreachable schedd) escalate to the operator rather than auto-triggering catastrophic recovery.

**Why**: A request stuck in PLANNING for an hour is a bug — fail fast and alert. A request stuck in ACTIVE for 30 days may just be a large workflow — don't abort it. A request stuck in ACTIVE with an unreachable schedd may be catastrophic — but auto-recovery could destroy data if the schedd is temporarily down (network blip). The operator must confirm before triggering version increment and output invalidation.

### DD-11: Partial production as fair-share scheduling via clean stop

**Decision**: Partial production uses the existing clean stop + rescue DAG mechanism to implement fair-share scheduling. A `production_steps` list on the request defines fraction thresholds; when the Lifecycle Manager detects that enough work units have completed, it auto-triggers a clean stop, consumes the step, demotes the request's priority, and re-enters the admission queue. The rescue DAG carries accumulated step_metrics for adaptive optimization.

**Why**: Computing resources cannot sit idle, and users should request their full needs to maximize utilization. But when many requests compete, one large request can monopolize the system while others wait. Partial production ensures every request gets at least a bare-minimum fraction of its results quickly (e.g., 10% for validation), then the remainder is deprioritized so other requests get their share. This is a fairness mechanism, not a scheduling optimization.

The design reuses the clean stop flow entirely — no new request states, no new DAG types, no new components. The `production_steps` list is extensible: adding more steps or changing thresholds requires no schema migration (JSONB). Work units are the natural fraction unit because they are already tracked by the DAG Monitor as SUBDAG EXTERNAL completions.

**Rejected alternatives**:
- *DAG partitioning at planning time* (split workflow into multiple smaller DAGs per priority tier): Complex, inflexible — the fraction boundary is hard-coded at planning time and can't adapt. Also requires managing multiple DAGs per workflow, violating the one-DAG-per-workflow invariant.
- *New request status (e.g., DEPRIORITIZED)*: Unnecessary — the existing STOPPING → RESUBMITTING → QUEUED → ACTIVE flow handles everything. Adding states increases the state machine complexity for no benefit.
- *Separate partial production queue*: Over-engineering. The admission queue already handles priority ordering; demoting the request's priority achieves the same effect.

### DD-12: Adaptive execution instead of dedicated pilot

**Decision**: WMS2 does not run a separate pilot job to measure performance before the production DAG. Instead, the first production DAG uses resource hints from the request (TimePerEvent, Memory, SizePerEvent). As work units complete, per-step FJR data (CPU efficiency, peak RSS, wall time) is aggregated by the Lifecycle Manager. When a recovery round occurs (rescue DAG after clean stop, partial failure, or partial production step), the accumulated metrics are written to `step_profile.json` for the sandbox to use in per-step optimization.

**Why**: A dedicated pilot phase adds ~8 hours of latency before the first production job runs, and the pilot's measurements may not represent the full dataset (different input files, different sites). The adaptive model starts producing real results immediately and converges to optimal parameters asymptotically — most gains are realized by round 2. Partial production steps (Section 6.2.1) are natural re-optimization points, so the bulk of every workflow benefits from measured data even on first submission. This also simplifies the state machine (no `PILOT_RUNNING` status) and removes pilot-specific tracking fields.

**Rejected alternative**: *Dedicated pilot job with iterative measurement*. Adds latency, complexity (extra request/workflow states, pilot tracking fields, pilot parsing logic), and may not be representative. The adaptive approach achieves the same goal (accurate resource estimates) without a separate phase, using production data that is inherently representative.

### DD-13: Memory-per-core window with 20% safety margin

**Decision**: The adaptive algorithm operates within a `[default_memory_per_core, max_memory_per_core]` window. Round 1 production jobs use `default_memory_per_core` (maximizing site pool), probe jobs use `max_memory_per_core` (headroom for split testing), and Round 2+ sizes memory to measured data + 20% safety margin, clamped within the window. This replaces the previous fixed 512 MB headroom buffer.

**Why**: CMS sites advertise resources as memory per core with a practical minimum. Requesting more than the minimum narrows the pool of matching sites, creating a tradeoff between accurate memory sizing and scheduling breadth. The two-knob window makes this tradeoff explicit and tunable. The 20% percentage-based margin (rather than fixed MB) scales naturally with job size — it provides proportionally more headroom for memory-heavy jobs and avoids being either too generous for small jobs (512 MB on a 2 GB job is 25%) or too thin for large jobs (512 MB on a 20 GB job is 2.5%). The margin accounts for memory leak accumulation (probes process fewer events than production), event-to-event RSS variation, and page cache pressure under concurrent load.

**Rejected alternative**: *Fixed MB headroom* (512 MB above measured peak). Doesn't scale — too large for small jobs, too small for large ones. Also conflated two sources of uncertainty (measurement noise vs. extrapolation error) into a single buffer.

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
| JobAccountant | DAG Monitor + Output Manager | Split between monitoring and DBS/Rucio registration |
| ErrorHandler | POST script + Error Handler | Per-node in POST script, workflow-level in WMS2 |
| RetryManager | DAGMan RETRY + POST script | Fully delegated with intelligent POST script |
| ResourceControl | Site Manager + DAGMan MAXJOBS + Admission Controller | Multi-level throttling |
| WMBS | PostgreSQL | No separate DB; no per-job rows |
| — (manual) | Adaptive execution (step_metrics) | New: automated performance measurement via production FJR data |

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
