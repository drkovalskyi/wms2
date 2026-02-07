# WMS2 — CMS Workload Management System v2

**OpenSpec v1.0**

| Field | Value |
|---|---|
| **Spec Version** | 2.0.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-07 |
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

**Key architectural principle**: WMS2 operates at the *workflow* and *DAG* level. Once a DAG is submitted to HTCondor DAGMan, HTCondor owns the individual job lifecycle — submission, retry, hold/release, and dependency resolution. WMS2 monitors DAG-level progress and intervenes only at the workflow level (abort, priority changes).

### 2.3 Execution Model

WMS2 treats the worker-node payload as opaque. Whether the job runs a single cmsRun (ReReco-style) or chains multiple steps (StepChain-style), the DAG structure is identical: a flat set of processing nodes followed by merge and cleanup nodes. The complexity of multi-step execution lives entirely within the sandbox/wrapper.

```
From WMS2's perspective, all workflows have the same shape:

  [Processing Nodes] → [Merge Nodes] → [Cleanup Nodes]

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
    COMPLETED = "completed"        # All nodes succeeded, outputs registered
    PARTIAL = "partial"            # DAG finished with some failures, recoverable
    FAILED = "failed"              # Catastrophic failure or retries exhausted
    ABORTED = "aborted"            # Manually cancelled


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

    # Pilot Results (populated after pilot completes)
    pilot_metrics: Optional[PilotMetrics]

    # DAG Reference (set after DAG submission)
    dag_id: Optional[UUID]

    # Category throttles (global defaults with optional per-workflow override)
    category_throttles: Dict[str, int] = {
        "Processing": 5000,
        "Merge": 100,
        "Cleanup": 50,
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

    # Source cleanup
    source_released: bool = False

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
    total_nodes INTEGER NOT NULL,
    total_edges INTEGER DEFAULT 0,
    node_counts JSONB DEFAULT '{}',
    nodes_idle INTEGER DEFAULT 0,
    nodes_running INTEGER DEFAULT 0,
    nodes_done INTEGER DEFAULT 0,
    nodes_failed INTEGER DEFAULT 0,
    nodes_held INTEGER DEFAULT 0,
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
    source_released BOOLEAN DEFAULT FALSE,
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
CREATE INDEX idx_workflows_request ON workflows(request_name);
CREATE INDEX idx_workflows_status ON workflows(status);
CREATE INDEX idx_requests_status ON requests(status);
CREATE INDEX idx_requests_campaign ON requests(campaign);
CREATE INDEX idx_requests_priority ON requests(priority);
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
│  1. Workflow Manager                                            │
│     - Import requests from ReqMgr2                              │
│     - Create internal workflows                                 │
│     - Manage workflow lifecycle                                 │
│                                                                 │
│  2. Admission Controller                                        │
│     - FIFO queue with priority ordering                         │
│     - Limit concurrent active DAGs                              │
│     - Prevent resource starvation and long tails                │
│                                                                 │
│  3. DAG Planner                                                 │
│     - Run pilot job (or use requestor estimates for urgent)     │
│     - Split input data into DAG node specs using pilot metrics  │
│     - Plan merge nodes based on actual output sizes             │
│     - Generate DAGMan .dag and .sub files                       │
│     - Submit DAGs via condor_submit_dag                         │
│                                                                 │
│  4. DAG Monitor                                                 │
│     - Poll DAGMan status files / condor_q                       │
│     - Update aggregate DAG/workflow counters                    │
│     - Detect DAG completion, partial failure, or failure        │
│     - Trigger Output Manager for completed merge outputs        │
│                                                                 │
│  5. Output Manager                                              │
│     - Register outputs in DBS                                   │
│     - Create Rucio source protection rules                      │
│     - Request Rucio transfers to final destinations             │
│     - Poll Rucio until transfers complete                       │
│     - Release source protection                                 │
│     - Rate-limit all DBS/Rucio API calls globally               │
│     - Evaluate existing CMS micro-services before building      │
│                                                                 │
│  6. Site Manager                                                │
│     - Track site resources                                      │
│     - Manage site status (drain, disable)                       │
│     - Sync with CRIC                                            │
│                                                                 │
│  7. Error Handler (workflow-level only)                          │
│     - Classify DAG-level failures                               │
│     - Decide: submit rescue DAG, flag for review, or abort      │
│     - Per-node retry handled by DAGMan RETRY + POST scripts     │
│                                                                 │
│  8. Metrics Collector                                           │
│     - Workflow/DAG lifecycle metrics                             │
│     - Admission queue state                                     │
│     - Output Manager progress                                   │
│     - WMS2 service health                                       │
│     - Note: per-job and pool metrics handled externally          │
│                                                                 │
│  NOTE: Individual job submission, retry, hold/release, and      │
│  dependency sequencing are delegated entirely to HTCondor        │
│  DAGMan. WMS2 does NOT track individual job state in its DB.    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Workflow Manager

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
        await self.admission_controller.enqueue(workflow)
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

### 4.3 Admission Controller

```python
class AdmissionController:
    """
    Controls the rate of DAG submissions to prevent resource starvation.
    Priority-ordered FIFO with a concurrency limit.
    """
    def __init__(self, max_active_dags: int = 300):
        self.max_active_dags = max_active_dags

    async def enqueue(self, workflow: Workflow):
        await self.db.update_request(workflow.request_name, status=RequestStatus.QUEUED)

    async def admission_loop(self):
        while True:
            try:
                await self.process_queue()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Admission controller error: {e}")
                await asyncio.sleep(self.error_backoff)

    async def process_queue(self):
        active_count = await self.db.count_active_dags()
        available_slots = self.max_active_dags - active_count
        if available_slots <= 0:
            return

        pending = await self.db.get_queued_workflows(
            limit=available_slots,
            order_by=["priority DESC", "created_at ASC"],
        )
        for workflow in pending:
            request = await self.db.get_request(workflow.request_name)
            if request.urgent:
                await self.dag_planner.plan_and_submit(workflow)
            else:
                await self.dag_planner.submit_pilot(workflow)
```

### 4.4 DAG Planner

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
        await self.db.update_workflow(workflow.id, status=WorkflowStatus.PILOT_RUNNING)
        await self.db.update_request(workflow.request_name, status=RequestStatus.PILOT_RUNNING)

    async def handle_pilot_completion(self, workflow: Workflow, pilot_output_path: str):
        """Parse pilot metrics and trigger production DAG planning."""
        metrics = self._parse_pilot_report(pilot_output_path)
        await self.db.update_workflow(workflow.id, pilot_metrics=metrics, status=WorkflowStatus.PLANNING)
        await self.plan_and_submit(workflow, metrics=metrics)

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

        # 4. Plan merge nodes from pilot metrics
        merge_nodes = self._plan_merge_nodes(processing_nodes, workflow, output_size_per_event)

        # 5. Plan cleanup nodes
        cleanup_nodes = self._plan_cleanup_nodes(processing_nodes, workflow)

        all_nodes = processing_nodes + merge_nodes + cleanup_nodes

        # 6. Wire up dependencies
        self._set_dependencies(processing_nodes, merge_nodes, cleanup_nodes)

        # 7. Generate DAG files
        submit_dir = self._create_submit_dir(workflow)
        dag_file_path = self._generate_dag_files(all_nodes, submit_dir, workflow)

        # 8. Create and submit DAG
        dag = DAG(
            workflow_id=workflow.id, dag_file_path=dag_file_path,
            submit_dir=submit_dir, total_nodes=len(all_nodes),
            total_edges=sum(len(n.parent_nodes) for n in all_nodes),
            node_counts=self._count_by_role(all_nodes), status=DAGStatus.READY,
        )
        await self.db.create_dag(dag)
        dagman_cluster_id, schedd_name = await self._submit_dag(dag)

        await self.db.update_dag(dag.id,
            dagman_cluster_id=dagman_cluster_id, schedd_name=schedd_name,
            status=DAGStatus.SUBMITTED, submitted_at=datetime.utcnow(),
        )
        await self.db.update_workflow(workflow.id,
            dag_id=dag.id, total_nodes=dag.total_nodes, status=WorkflowStatus.ACTIVE,
        )
        await self.db.update_request(workflow.request_name, status=RequestStatus.ACTIVE)
        return dag

    # ── Merge Planning ───────────────────────────────────────────

    def _plan_merge_nodes(self, processing_nodes, workflow, output_size_per_event_kb,
                          target_merged_size_kb=4_000_000) -> List[DAGNodeSpec]:
        """Group processing outputs into merge nodes based on size threshold."""
        merge_nodes = []
        current_group = []
        current_size = 0
        idx = 0

        for proc in processing_nodes:
            est_output = proc.input_events * output_size_per_event_kb
            if current_size + est_output > target_merged_size_kb and current_group:
                merge_nodes.append(DAGNodeSpec(
                    node_name=f"merge_{idx:06d}", submit_file="",
                    node_role=NodeRole.MERGE, input_files=[], input_lumis=[],
                    input_events=sum(n.input_events for n in current_group),
                    estimated_time_sec=3600, estimated_disk_kb=int(current_size * 2),
                    estimated_memory_mb=2048, cores=1, possible_sites=[],
                    max_retries=2, post_script="post_script.sh",
                    parent_nodes=[n.node_name for n in current_group],
                ))
                idx += 1
                current_group, current_size = [], 0
            current_group.append(proc)
            current_size += est_output

        if current_group:
            merge_nodes.append(DAGNodeSpec(
                node_name=f"merge_{idx:06d}", submit_file="",
                node_role=NodeRole.MERGE, input_files=[], input_lumis=[],
                input_events=sum(n.input_events for n in current_group),
                estimated_time_sec=3600, estimated_disk_kb=int(current_size * 2),
                estimated_memory_mb=2048, cores=1, possible_sites=[],
                max_retries=2, post_script="post_script.sh",
                parent_nodes=[n.node_name for n in current_group],
            ))
        return merge_nodes

    # ── DAG File Generation ──────────────────────────────────────

    def _generate_dag_files(self, nodes, submit_dir, workflow) -> str:
        dag_file_path = os.path.join(submit_dir, "workflow.dag")

        for node in nodes:
            sub_path = os.path.join(submit_dir, f"{node.node_name}.sub")
            node.submit_file = sub_path
            self._write_submit_file(node, sub_path, workflow)

        with open(dag_file_path, "w") as f:
            f.write(f"CONFIG {os.path.join(submit_dir, 'dagman.config')}\n\n")

            for node in nodes:
                f.write(f"JOB {node.node_name} {node.submit_file}\n")
            f.write("\n")

            for node in nodes:
                if node.max_retries > 0:
                    f.write(f"RETRY {node.node_name} {node.max_retries} UNLESS-EXIT 2\n")
            f.write("\n")

            for node in nodes:
                if node.post_script:
                    f.write(f"SCRIPT POST {node.node_name} "
                            f"{os.path.join(submit_dir, node.post_script)} "
                            f"{node.node_name} $RETURN\n")
            f.write("\n")

            for node in nodes:
                if node.parent_nodes:
                    parents = " ".join(node.parent_nodes)
                    f.write(f"PARENT {parents} CHILD {node.node_name}\n")
            f.write("\n")

            for node in nodes:
                f.write(f"CATEGORY {node.node_name} {node.node_role.value}\n")
            f.write("\n")

            for role, limit in workflow.category_throttles.items():
                f.write(f"MAXJOBS {role} {limit}\n")

        self._write_dagman_config(submit_dir)
        return dag_file_path

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

#### 4.4.1 Splitting Algorithms

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

### 4.5 DAG Monitor

```python
class DAGMonitor:
    async def poll_loop(self):
        while True:
            try:
                await self.poll_pilots()
                await self.poll_all_dags()
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                logger.error(f"DAG monitor error: {e}")
                await asyncio.sleep(self.error_backoff)

    async def poll_pilots(self):
        pilots = await self.db.get_workflows_by_status(WorkflowStatus.PILOT_RUNNING)
        for workflow in pilots:
            completed = await self.condor_adapter.check_job_completed(
                workflow.pilot_cluster_id, workflow.pilot_schedd
            )
            if completed:
                await self.dag_planner.handle_pilot_completion(
                    workflow, workflow.pilot_output_path
                )

    async def poll_all_dags(self):
        active_dags = await self.db.get_dags_by_status(
            [DAGStatus.SUBMITTED, DAGStatus.RUNNING]
        )
        tasks = [self.poll_dag(dag) for dag in active_dags]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def poll_dag(self, dag):
        dagman_status = await self.condor_adapter.query_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id,
        )
        if dagman_status is None:
            await self._handle_dag_completion(dag)
            return

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

        # Check for completed merge nodes → trigger Output Manager
        newly_completed_merges = await self._detect_completed_merges(dag, summary)
        for merge_info in newly_completed_merges:
            await self.output_manager.handle_merge_completion(dag.workflow_id, merge_info)

    async def _handle_dag_completion(self, dag):
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

        if new_status == DAGStatus.PARTIAL:
            await self.error_handler.handle_dag_partial_failure(dag)
        elif new_status == DAGStatus.FAILED:
            await self.error_handler.handle_dag_failure(dag)

    async def _parse_dagman_status(self, status_file):
        content = await aiofiles.read(status_file)
        data = json.loads(content)
        return NodeSummary(
            idle=data.get("NodesIdle", 0), running=data.get("NodesRunning", 0),
            done=data.get("NodesDone", 0), failed=data.get("NodesFailed", 0),
            held=data.get("NodesHeld", 0),
        )
```

### 4.6 Output Manager

The Output Manager handles the post-compute pipeline: DBS registration, Rucio source protection, transfer requests, transfer monitoring, and source release. This is the one area where WMS2 cannot be thin — it owns a long-lived, stateful process that outlives DAG execution and may take hours or days.

**Important**: Before building this component, existing CMS micro-services that handle output registration and data placement should be evaluated. If they are reusable with clean APIs, the Output Manager becomes a thin orchestrator calling those services.

```python
class OutputManager:
    async def processing_loop(self):
        while True:
            try:
                await self.process_pending_outputs()
                await self.poll_active_transfers()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Output manager error: {e}")
                await asyncio.sleep(self.error_backoff)

    async def handle_merge_completion(self, workflow_id, merge_info):
        output = OutputDataset(
            workflow_id=workflow_id,
            dataset_name=merge_info["dataset_name"],
            source_site=merge_info["site"],
            status=OutputStatus.PENDING,
        )
        await self.db.create_output_dataset(output)

    async def process_pending_outputs(self):
        pending = await self.db.get_output_datasets_by_status(
            [OutputStatus.PENDING, OutputStatus.DBS_REGISTERED, OutputStatus.SOURCE_PROTECTED]
        )
        for output in pending:
            try:
                if output.status == OutputStatus.PENDING:
                    await self._register_in_dbs(output)
                elif output.status == OutputStatus.DBS_REGISTERED:
                    await self._protect_source(output)
                elif output.status == OutputStatus.SOURCE_PROTECTED:
                    await self._request_transfers(output)
            except RateLimitExceeded:
                break
            except Exception as e:
                logger.error(f"Output {output.id} failed: {e}")

    async def poll_active_transfers(self):
        transferring = await self.db.get_output_datasets_by_status(
            [OutputStatus.TRANSFERS_REQUESTED, OutputStatus.TRANSFERRING]
        )
        for output in transferring:
            all_complete = await self._check_transfer_status(output)
            if all_complete:
                await self._release_source_protection(output)
                await self.db.update_output_dataset(output.id, status=OutputStatus.ANNOUNCED)
                await self._check_workflow_completion(output.workflow_id)

    async def _check_workflow_completion(self, workflow_id):
        outputs = await self.db.get_output_datasets(workflow_id)
        if all(o.status == OutputStatus.ANNOUNCED for o in outputs):
            await self.db.update_workflow(workflow_id, status=WorkflowStatus.COMPLETED)
            await self.db.update_request_by_workflow(workflow_id, status=RequestStatus.COMPLETED)

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
        for rule_id in output.transfer_rule_ids:
            if await self.rucio_adapter.get_rule_status(rule_id) != "OK":
                await self.db.update_output_dataset(output.id, status=OutputStatus.TRANSFERRING)
                return False
        return True

    async def _release_source_protection(self, output):
        await self.rucio_adapter.delete_rule(output.source_rule_id)
        await self.db.update_output_dataset(output.id,
            source_released=True, status=OutputStatus.SOURCE_RELEASED,
        )
```

### 4.7 Error Handler (Workflow-Level)

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

### 4.8 Site Manager

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

### 4.9 Metrics Collector

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

---

## 6. Auto-Recovery Model

### 6.1 Two-Level Recovery

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

**Level 3: Workflow-level (owned by WMS2 Error Handler)**
- When a DAG terminates with failures, WMS2 decides: rescue, review, or abort
- Based on failure ratio thresholds (< 5% auto-rescue, 5–30% review, > 30% abort)
- Rescue DAGs automatically skip completed nodes

### 6.2 POST Script Design

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

**Goal**: Basic infrastructure and data models.

| Component | Description | Priority |
|---|---|---|
| Database Schema | PostgreSQL tables, indexes, migrations | P0 |
| Data Models | Pydantic models, validation | P0 |
| API Framework | FastAPI setup, auth, middleware | P0 |
| Config Management | Settings, environment handling | P0 |
| Logging / Metrics | Structured logging, Prometheus | P1 |

**Deliverables**: Database migrations working; API skeleton with health endpoints; Docker compose for local dev; basic test framework.

### 10.2 Phase 2: Pilot + DAG Planning Pipeline (Weeks 5–8)

**Goal**: Request → Pilot → DAG file generation.

| Component | Description | Priority |
|---|---|---|
| ReqMgr2 Adapter | Fetch requests, validate schema | P0 |
| DBS Adapter | Query datasets, get file lists | P0 |
| Workflow Manager | Create workflows from requests | P0 |
| Admission Controller | FIFO queue with concurrency limit | P0 |
| Pilot Submission | Submit pilot jobs, parse results | P0 |
| File-Based Splitter | File-based splitting → DAGNodeSpecs | P0 |
| Event-Based Splitter | Event-based splitting | P1 |
| Merge Planner | Plan merge nodes from pilot metrics | P0 |
| DAG File Generator | Generate .dag and .sub files | P0 |

**Deliverables**: Can import request from ReqMgr2; pilot job runs and reports metrics; DAG files generated with accurate resource estimates and merge plan; unit tests for splitters and DAG generation.

**Action item**: Evaluate existing CMS micro-services for output registration during this phase.

### 10.3 Phase 3: HTCondor DAGMan Integration (Weeks 9–12)

**Goal**: DAG submission, monitoring, and recovery.

| Component | Description | Priority |
|---|---|---|
| Condor Adapter | Submit DAGs, query DAGMan jobs | P0 |
| DAG Monitor | Poll DAGMan status, update aggregates | P0 |
| POST Script | Per-node error classification and retry | P0 |
| Error Handler | Rescue DAG submission, abort logic | P0 |
| DAG Hold/Release | Operator controls for DAGs | P1 |

**Deliverables**: DAGs submit to HTCondor DAGMan; status monitoring working; POST script error handling; automatic rescue DAG submission; end-to-end test with real workflow.

**Milestone**: Load test with realistic concurrency (~300 DAGs × ~10K nodes) on test infrastructure with HTCondor team.

### 10.4 Phase 4: Output Pipeline + Productionization (Weeks 13–16)

**Goal**: Complete pipeline and ready for pilot testing.

| Component | Description | Priority |
|---|---|---|
| Output Manager | DBS registration, Rucio transfers | P0 |
| Rucio Adapter | Rule creation, status, deletion | P0 |
| Site Manager | CRIC sync, site control | P0 |
| Dashboard API | Frontend data endpoints | P1 |
| Observability | Grafana dashboards | P1 |
| Documentation | Ops guide, API docs | P1 |

**Deliverables**: Full request lifecycle working (submit → pilot → DAG → outputs registered → transfers complete); site management; production-like deployment; monitoring dashboards; operator documentation.

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

---

## 15. Open Questions

1. **Sandbox/Wrapper Design**: How does the per-node manifest communicate input data to the job? What format? This is tightly coupled to sandbox architecture and needs dedicated design discussion.
2. **Sandbox Scope**: Exact boundary between WMS2 and sandbox responsibilities — manifest generation, pilot mode, POST script logic.
3. **Existing Micro-Services**: Which CMS micro-services currently handle output registration and data placement? Are they reusable with clean APIs?
4. **DAG Size Partitioning**: If a workflow exceeds practical DAG size limits (discovered during load testing), what's the partitioning strategy?
5. **DAGMan Status Polling vs. Event-Based**: Should WMS2 poll `.status` files or use event log callbacks? Performance implications at scale.
6. **Shared Filesystem**: Is a shared filesystem between submit host and WMS2 service guaranteed, or do DAG files need to be transferred?
7. **GPU Jobs**: Include `request_gpus` support in submit files for MVP?

---

## Appendix A: WMCore Component Mapping

| WMCore Component | WMS2 Equivalent | Notes |
|---|---|---|
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

Example generated `.dag` file for a workflow with processing, merge, and cleanup nodes:

```
# WMS2-generated DAG for workflow 550e8400-...
CONFIG /submit/550e8400/dagman.config

# Processing nodes
JOB proc_000000 /submit/550e8400/proc_000000.sub
JOB proc_000001 /submit/550e8400/proc_000001.sub
JOB proc_000002 /submit/550e8400/proc_000002.sub

# Merge nodes (sized from pilot metrics)
JOB merge_000000 /submit/550e8400/merge_000000.sub

# Cleanup nodes
JOB cleanup_000000 /submit/550e8400/cleanup_000000.sub

# Retries with POST script error classification
# exit 2 = permanent failure, do not retry
RETRY proc_000000 3 UNLESS-EXIT 2
RETRY proc_000001 3 UNLESS-EXIT 2
RETRY proc_000002 3 UNLESS-EXIT 2
RETRY merge_000000 2 UNLESS-EXIT 2
RETRY cleanup_000000 1

# POST scripts for per-node error handling and cool-off
SCRIPT POST proc_000000 /submit/550e8400/post_script.sh proc_000000 $RETURN
SCRIPT POST proc_000001 /submit/550e8400/post_script.sh proc_000001 $RETURN
SCRIPT POST proc_000002 /submit/550e8400/post_script.sh proc_000002 $RETURN
SCRIPT POST merge_000000 /submit/550e8400/post_script.sh merge_000000 $RETURN

# Dependencies
PARENT proc_000000 proc_000001 proc_000002 CHILD merge_000000
PARENT merge_000000 CHILD cleanup_000000

# Categories for throttling
CATEGORY proc_000000 Processing
CATEGORY proc_000001 Processing
CATEGORY proc_000002 Processing
CATEGORY merge_000000 Merge
CATEGORY cleanup_000000 Cleanup

# Category throttles
MAXJOBS Processing 5000
MAXJOBS Merge 100
MAXJOBS Cleanup 50
```
