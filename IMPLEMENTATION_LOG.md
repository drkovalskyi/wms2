# WMS2 — Implementation Log

**Date**: 2026-02-14
**Spec Version**: 2.4.0
**Phase**: 1 — Project Scaffold and Foundation

---

## What Was Built

### Project Scaffold
- `pyproject.toml` — PEP 621, src layout, hatchling build backend
- `Dockerfile` — Multi-stage Python 3.11-slim with uvicorn entrypoint
- `docker-compose.yml` — PostgreSQL 15 (port 5432) + WMS2 app (port 8000)
- `docker-compose.test.yml` — Isolated test PostgreSQL (port 5433)
- `alembic.ini` — Async Alembic configuration
- `.env.example` — All `WMS2_` prefixed env vars documented
- `.gitignore` — Python, IDE, env, testing, cache patterns

### Dependencies
- **Runtime**: fastapi, uvicorn[standard], sqlalchemy[asyncio], asyncpg, pydantic, pydantic-settings, alembic, httpx, prometheus-client
- **Dev**: pytest, pytest-asyncio, pytest-cov, httpx, ruff, mypy

### Pydantic Models (`src/wms2/models/`)
| File | Contents |
|---|---|
| `enums.py` | 8 enums: RequestStatus (12), WorkflowStatus (10), DAGStatus (10), OutputStatus (10), SiteStatus (4), SplittingAlgo (4), CleanupPolicy (2), NodeRole (3) |
| `common.py` | StatusTransition |
| `request.py` | ProductionStep, RequestCreate (with validators), RequestUpdate, Request |
| `workflow.py` | StepProfile, PilotMetrics, Workflow |
| `dag.py` | LumiRange, InputFile, MergeGroup, DAGNodeSpec, DAG |
| `output_dataset.py` | OutputDataset |
| `site.py` | Site |

**Validators on RequestCreate**:
- `urgent` and `production_steps` are mutually exclusive
- `production_steps` fractions must be strictly increasing
- `production_steps` priorities must be strictly decreasing
- `ProductionStep.fraction` bounded to (0, 1) exclusive

### SQLAlchemy Tables (`src/wms2/db/tables.py`)
6 table classes matching spec Section 3.2:
1. `RequestRow` — 19 columns, JSONB for request_data/payload_config/splitting_params/production_steps/status_transitions
2. `WorkflowRow` — 21 columns, JSONB for splitting_params/config_data/pilot_metrics/category_throttles
3. `DAGRow` — 24 columns, JSONB for node_counts/completed_work_units, self-referential FK for parent_dag_id
4. `DAGHistoryRow` — 10 columns, BIGSERIAL PK, JSONB for detail
5. `OutputDatasetRow` — 22 columns, JSONB for transfer_rule_ids/transfer_destinations
6. `SiteRow` — 13 columns, name as PK (not UUID), JSONB for thresholds/config_data

### Alembic Migration (`001_initial_schema.py`)
Single migration creating all 6 tables + 14 indexes from spec:
- Standard indexes: idx_dags_workflow, idx_dags_status, idx_workflows_request, idx_workflows_status, idx_requests_status, idx_requests_campaign, idx_requests_priority, idx_dag_history_dag, idx_output_datasets_workflow, idx_output_datasets_status
- Partial indexes: idx_dags_schedd (WHERE dagman_cluster_id IS NOT NULL), idx_dags_parent (WHERE parent_dag_id IS NOT NULL), idx_requests_non_terminal (WHERE status NOT IN ('completed','failed','aborted')), idx_requests_version_link (WHERE previous_version_request IS NOT NULL)

### Repository (`src/wms2/db/repository.py`)
Single `Repository` class with `AsyncSession` injection:
- **Requests**: create, get by name, list (with status/campaign filters), update, get_non_terminal, count_by_status
- **Workflows**: create, get by id, get by request_name, update, get_queued_requests (ORDER BY priority DESC, created_at ASC)
- **DAGs**: create, get, update, count_active (WHERE status IN submitted/running)
- **DAG History**: create, get by dag_id
- **Output Datasets**: create, get by workflow_id, update
- **Sites**: upsert (ON CONFLICT DO UPDATE), get by name, list

### Adapter Interfaces (`src/wms2/adapters/base.py`)
5 abstract base classes:
1. `CondorAdapter` — submit_job, submit_dag, query_job, check_job_completed, remove_job, ping_schedd
2. `ReqMgrAdapter` — get_request
3. `DBSAdapter` — get_files, inject_dataset, invalidate_dataset
4. `RucioAdapter` — get_replicas, create_rule, get_rule_status, delete_rule
5. `CRICAdapter` — get_sites

Mock implementations in `adapters/mock.py` with call history tracking for test assertions.

### Core Business Logic

**RequestLifecycleManager** (`src/wms2/core/lifecycle_manager.py`):
- Constructor injection of Repository, CondorAdapter, Settings, and optional Phase 2+ components
- `main_loop()` — async infinite loop with CancelledError handling for graceful shutdown
- `evaluate_request()` — stuck check + dispatch table
- 7 handlers: _handle_submitted, _handle_queued, _handle_pilot_running, _handle_active, _handle_stopping, _handle_resubmitting, _handle_partial
- Phase 2+ components (workflow_manager, dag_planner, dag_monitor, output_manager, error_handler) checked for None; log+skip when unavailable
- `transition()` — appends to status_transitions JSONB with from/to/timestamp
- `_is_stuck()` / `_handle_stuck()` — per-status timeouts from Settings
- `initiate_clean_stop()` — condor_rm + STOPPING transition
- `_prepare_recovery()` — creates rescue DAG record, consumes production_step, demotes priority

**AdmissionController** (`src/wms2/core/admission_controller.py`):
- `has_capacity()` — active DAGs < max_active_dags
- `get_next_pending()` — highest priority, oldest first
- `get_queue_status()` — summary dict for API

### FastAPI Application

**App factory** (`src/wms2/main.py`):
- `create_app()` returns configured FastAPI instance
- Lifespan context manager: creates engine, session factory, starts Lifecycle Manager as asyncio.Task
- Graceful shutdown: cancels lifecycle task, disposes engine

**23 API routes** under `/api/v1`:
| Route | Method | Description |
|---|---|---|
| /requests | POST | Create request (201) |
| /requests | GET | List requests (filter by status, campaign) |
| /requests/{name} | GET | Get request by name |
| /requests/{name} | PATCH | Partial update |
| /requests/{name} | DELETE | Abort request |
| /requests/{name}/stop | POST | Request clean stop |
| /requests/{name}/restart | POST | Request restart |
| /requests/{name}/versions | GET | Version chain |
| /workflows | GET | 501 stub |
| /workflows/{id} | GET | 501 stub |
| /dags | GET | 501 stub |
| /dags/{id} | GET | 501 stub |
| /sites | GET | List sites |
| /sites/{name} | GET | Get site |
| /admission/queue | GET | Queue status |
| /health | GET | Health check |
| /status | GET | Request counts by status |
| /metrics | GET | Prometheus format metrics |
| /lifecycle/status | GET | Lifecycle manager status |

### Tests

**32 unit tests — all passing** (no database required):

| File | Tests | Coverage |
|---|---|---|
| `test_enums.py` | 10 | All 8 enums: value counts, string values, serialization |
| `test_models.py` | 8 | RequestCreate validation: valid, production_steps, mutual exclusion, ordering, bounds |
| `test_lifecycle.py` | 10 | Dispatch routing, RESUBMITTING→QUEUED, audit trail, stuck detection, None component handling |
| `test_admission.py` | 5 | Capacity check, priority ordering, empty queue, queue status |

**Integration tests** (require PostgreSQL on port 5433):
- `test_repository.py` — CRUD round-trips, status filters, JSONB fields, non-terminal query, active DAG count, site upsert
- `test_api.py` — POST/GET/PATCH/DELETE requests, health endpoint, 404/409/422 error handling

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/ -v` — 32 tests pass
3. `python -c "from wms2.main import create_app; create_app()"` — app creates successfully with 23 routes
4. For integration tests: `docker compose -f docker-compose.test.yml up -d` then `pytest tests/integration/ -v`
5. For full app: `docker compose up -d db` then `alembic upgrade head` then `uvicorn wms2.main:create_app --factory`

## Design Decisions

- **Repository pattern**: Single class with AsyncSession injection rather than per-entity repositories. Simpler for Phase 1; can split later if needed.
- **Phase 2+ components as optional constructor args**: Lifecycle manager checks for None and skips gracefully, allowing incremental development.
- **Enums stored as strings in DB**: VARCHAR columns with string enum values, not PostgreSQL ENUM types. Easier to migrate when adding new statuses.
- **JSONB for flexible fields**: status_transitions, production_steps, node_counts, etc. use JSONB. Avoids extra tables for nested/variable structures.
- **Upsert for sites**: Sites use ON CONFLICT DO UPDATE since they come from external CRIC sync.

## What's Next (Phase 2)

- Workflow Manager — ReqMgr2 import
- DAG Planner — pilot submission, splitting, DAG file generation
- DAG Monitor — DAGMan status polling
- Output Manager — DBS registration, Rucio transfers
- Error Handler — failure classification, rescue DAG decisions
- Site Manager — CRIC sync
- Real CondorAdapter (htcondor Python bindings)

---

# Phase 2 — Pilot + DAG Planning Pipeline

**Date**: 2026-02-14
**Spec Version**: 2.4.0
**Phase**: 2 — Request → Pilot → DAG File Generation

---

## What Was Built

### Config Expansion (`config.py`)
New settings for external services and DAG submission:
- `reqmgr2_url`, `dbs_url`, `rucio_url`, `rucio_account` — External service endpoints
- `agent_name` — WMS2 instance identity in ReqMgr2
- `submit_base_dir` — Root directory for DAG file output
- `target_merged_size_kb` — Target merged output size (default 4 GB)
- `cert_file`, `key_file` — X.509 certificate paths (None = use mock adapters)

### Adapter Signature Updates (`adapters/base.py`, `adapters/mock.py`)
- `ReqMgrAdapter.get_assigned_requests(agent_name)` — Fetch requests assigned to agent
- `DBSAdapter.get_files()` — Added `run_whitelist` and `lumi_mask` optional params
- `RucioAdapter.get_replicas(lfns: list[str]) -> dict[str, list[str]]` — Changed from dataset-level to per-LFN replica lookup
- All mock adapters updated to match new signatures

### Real Adapters (3 new files)

**ReqMgr2Client** (`adapters/reqmgr2.py`):
- httpx async client with X.509 cert auth
- `get_request(name)` — GET single request, unwraps ReqMgr2 response envelope
- `get_assigned_requests(agent_name)` — GET requests with status=assigned&team=agent
- 3 retries with exponential backoff on HTTP/transport errors

**DBSClient** (`adapters/dbs.py`):
- httpx async client with X.509 cert auth
- `get_files(dataset, limit, run_whitelist, lumi_mask)` — GET /files?dataset=&detail=1
- `inject_dataset()`, `invalidate_dataset()` — POST/PUT endpoints
- 3 retries with exponential backoff

**RucioClient** (`adapters/rucio.py`):
- httpx async client with X-Rucio-Account header
- `get_replicas(lfns)` — POST /replicas/list, returns {LFN → [site_name]}
- RSE name normalization: strips `_Disk`/`_Tape`/`_Test`/`_Temp` suffixes, excludes tape-only
- `create_rule()`, `get_rule_status()`, `delete_rule()` — Rucio rule management
- 3 retries with exponential backoff

### Workflow Manager (`core/workflow_manager.py`)
- `import_request(request_name)` — Fetches from ReqMgr2, validates required fields (InputDataset, SplittingAlgo, SandboxUrl), creates workflow row
- `get_workflow_status(workflow_id)` — Builds progress summary (workflow + DAG + outputs)
- Called by lifecycle manager `_handle_submitted`

### Splitters (`core/splitters.py`)
Data classes:
- `InputFile` — LFN, size, event count, replica locations
- `DAGNodeSpec` — Node index, input files, event range, primary location

Splitter implementations (pure stateless computation):
- **FileBasedSplitter** — Groups files by primary location for data locality, chunks into batches of `files_per_job`
- **EventBasedSplitter** — Splits across files by event count, handles files spanning multiple jobs
- `get_splitter(algo, params)` — Factory function, maps FileBased/LumiBased → FileBasedSplitter, EventBased/EventAwareLumiBased → EventBasedSplitter

### DAG Planner (`core/dag_planner.py`)

**Data classes**:
- `PilotMetrics` — events_per_second, memory_peak_mb, output_size_per_event_kb, time_per_event_sec, cpu_efficiency; parses from JSON
- `PlanningMergeGroup` — group_index, processing_nodes, estimated_output_kb

**Pilot phase**:
- `submit_pilot(workflow)` — Fetches 5 sample files, writes `pilot.sub` to submit_dir/pilot/
- `_parse_pilot_report(path)` — Reads JSON → PilotMetrics
- `handle_pilot_completion(workflow, path)` — Parses metrics, calls plan_production_dag()

**Production DAG** (`plan_production_dag`):
1. Resource params from pilot metrics or defaults
2. Fetch all input files from DBS
3. Get replica locations from Rucio
4. Convert to InputFile objects with locations
5. Split via appropriate splitter
6. Plan merge groups by output size accumulation
7. Generate DAG files (Appendix C format)
8. Create DAG row with status=READY

**Merge group planning** (`_plan_merge_groups`):
- Accumulates processing nodes into groups by estimated output size
- Starts new group when adding a node would exceed `target_merged_size_kb`

**DAG file generation** (Appendix C format):
- Outer `workflow.dag`: CONFIG + SUBDAG EXTERNAL per group + CATEGORY + MAXJOBS MergeGroup 10
- Per-group `mg_NNNNNN/group.dag`:
  - JOB: landing, proc_NNNNNN (×N), merge, cleanup
  - SCRIPT POST landing → elect_site.sh
  - SCRIPT PRE proc/merge/cleanup → pin_site.sh
  - SCRIPT POST proc → post_script.sh
  - RETRY: proc×3, merge×2, cleanup×1 (UNLESS-EXIT 2)
  - PARENT/CHILD: landing→all_proc→merge→cleanup
  - CATEGORY: Processing, Merge, Cleanup with MAXJOBS throttles
- Submit files: landing.sub, proc_NNNNNN.sub, merge.sub, cleanup.sub (vanilla universe)
- Scripts: elect_site.sh (reads MATCH_GLIDEIN_CMSSite), pin_site.sh (sed DESIRED_Sites), post_script.sh

### Component Wiring

**`main.py`** — `_build_adapters(settings)`:
- If `cert_file` and `key_file` set → real ReqMgr2Client, DBSClient, RucioClient
- Otherwise → mock adapters
- Instantiates WorkflowManager + DAGPlanner, passes to lifecycle manager

**`lifecycle_manager.py`** — Updated handlers:
- `_handle_submitted` → `workflow_manager.import_request()` → QUEUED
- `_handle_queued` → `dag_planner.plan_production_dag()` (urgent) or `dag_planner.submit_pilot()` (normal)
- `_handle_pilot_running` → checks for pilot_metrics.json, calls `handle_pilot_completion()` or `plan_production_dag()` with defaults → ACTIVE

### API Endpoints (replaced 501 stubs)

| Route | Method | Description |
|---|---|---|
| /workflows | GET | List workflows (filter by status, paginate) |
| /workflows/{id} | GET | Full workflow detail |
| /dags | GET | List DAGs (filter by status, workflow_id, paginate) |
| /dags/{id} | GET | Full DAG detail |

### Repository Additions (`db/repository.py`)
- `list_workflows(status, limit, offset)` — ORDER BY created_at DESC
- `list_dags(status, workflow_id, limit, offset)` — ORDER BY created_at DESC

### Tests

**35 new tests (all passing)**:

| File | Tests | What |
|---|---|---|
| `test_splitters.py` | 11 | File grouping, partial batches, single file, empty, files_per_job=1, site grouping, event splitting |
| `test_merge_groups.py` | 5 | Single group, multiple groups, exact boundaries, empty, sequential indices |
| `test_dag_generator.py` | 8 | Outer DAG, group DAG, submit files, site scripts, dagman.config, Appendix C structure verification |
| `test_workflow_manager.py` | 5 | Workflow creation, field validation, splitting params, missing workflow, status summary |
| `test_pilot_metrics.py` | 4 | Complete JSON, defaults, partial, file parsing |
| `test_dag_planner.py` (integration) | 3 | End-to-end: plan → files on disk, DAG structure, pilot submit |

**Total: 70 tests passing** (67 unit + 3 integration)

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/ -v` — 67 tests pass
3. `pytest tests/integration/test_dag_planner.py -v` — 3 tests pass
4. `pytest tests/unit/ tests/integration/test_dag_planner.py -v` — 70 tests pass

## Design Decisions

- **Real adapters use httpx with X.509 certs**: Same async HTTP client as test dependencies, consistent retry logic across all external services
- **Mock-by-default**: When no cert configured, all adapters are mocks — enables local development and testing without CMS infrastructure
- **Splitters are pure functions**: No DB or IO access; take InputFiles, return DAGNodeSpecs. Easy to test and reason about
- **Merge group planning by output size**: Accumulates nodes until estimated output exceeds target_merged_size_kb, then starts new group. Simple greedy algorithm
- **DAG file generation follows Appendix C exactly**: Outer DAG with SUBDAG EXTERNAL per merge group, inner DAGs with landing→proc→merge→cleanup chain, site election via scripts
- **Per-LFN replica lookup**: Changed Rucio adapter from dataset-level to per-LFN to enable file-level data locality in splitters

## What's Next (Phase 3)

- Real CondorAdapter — condor_submit_dag, condor_q, condor_rm via htcondor Python bindings
- DAG Monitor — Poll DAGMan status, parse .dagman.out, update node counts
- DAG Submission — Move DAGs from READY → SUBMITTED → RUNNING
- Output Manager — Detect completed merge groups, DBS registration, Rucio transfers
- Error Handler — Failure classification, rescue DAG decisions
