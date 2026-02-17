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

---

# Phase 3 — HTCondor Submission + DAG Monitoring

**Date**: 2026-02-14
**Spec Version**: 2.4.0
**Phase**: 3 — Real HTCondor Adapter + DAG Monitor

---

## What Was Built

### Real HTCondor Adapter (`adapters/condor.py`)
Wraps synchronous `htcondor2` Python bindings in `asyncio.to_thread()`:
- `submit_job(submit_file)` — Reads submit file, creates `htcondor2.Submit`, submits via `Schedd.submit()`
- `submit_dag(dag_file)` — Uses `htcondor2.Submit.from_dag()` + `Schedd.submit()`
- `query_job(schedd_name, cluster_id)` — `Schedd.query()` with DAG status projection (DAG_NodesTotal, DAG_NodesDone, etc.)
- `check_job_completed(cluster_id, schedd_name)` — Checks queue first, then history for `JobStatus==4`
- `remove_job(schedd_name, cluster_id)` — `Schedd.act(JobAction.Remove, ...)`
- `ping_schedd(schedd_name)` — Lightweight `query(constraint="false", limit=1)`

Constructor: `Collector(condor_host)` → `locate(DaemonType.Schedd)` → `Schedd(ad)`.

### DAG Monitor (`core/dag_monitor.py`)

**Data classes**:
- `NodeSummary` — Aggregate counts: idle, running, done, failed, held + per-node status map
- `DAGPollResult` — dag_id, status, node counts, newly_completed_work_units

**Main entry point** `poll_dag(dag)`:
1. Query HTCondor for DAGMan process (`condor.query_job`)
2. If alive → parse `.status` JSON file for node progress, detect newly completed merge group SUBDAGs, update DAG + workflow rows
3. If gone → read `.metrics` file for final counts, determine terminal status (COMPLETED / PARTIAL / FAILED), update rows with `completed_at` timestamp

**Status file parsing**:
- `_parse_dagman_status(path)` — Reads `.status` JSON, maps node statuses to counts (done/success → done, error/failed → failed, running/submitted → running, held → held, else → idle)
- `_parse_dagman_metrics(path)` — Reads `.metrics` JSON for final aggregate counts

**Work unit detection**:
- `_detect_completed_work_units(dag, summary)` — Finds `mg_NNNNNN` nodes newly in "done" state, reads merge manifest if available
- `_read_merge_manifest(dag, group_name)` — Reads `mg_NNNNNN/merge_output.json`

**Completion handling** `_handle_dag_completion(dag)`:
- Falls back from metrics to status file if metrics missing
- Terminal status logic: no failures → COMPLETED, mixed → PARTIAL, all failed → FAILED

### Lifecycle Manager Integration
- `_handle_active` calls `dag_monitor.poll_dag()` when DAG exists
- DAG Monitor passed as constructor arg alongside other Phase 2+ components

### Tests

**26 new tests (all passing)**:

| File | Tests | What |
|---|---|---|
| `test_condor_adapter.py` | 10 | submit_dag, submit_job, query (found/not), check_completed (running/history/gone), remove, ping (success/fail) — all with mocked htcondor2 |
| `test_dag_monitor.py` | 16 | Status parsing (valid/missing/failed/held), metrics parsing, work unit detection (new/already-reported/non-mg), poll_dag (alive→running, gone→completed/partial/failed), newly completed units, handle_dag_completion, lifecycle integration |
| `test_condor_submit.py` (integration) | 2 | Submit+query+remove trivial DAG, DAGPlanner end-to-end with real condor — requires `WMS2_CONDOR_HOST` env var |

**Total: 96 tests passing** (93 unit + 3 integration)

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/ -v` — 93 tests pass
3. `pytest tests/integration/test_dag_planner.py -v` — 3 tests pass
4. `pytest tests/unit/ tests/integration/test_dag_planner.py -v` — 96 tests pass
5. HTCondor integration tests: `WMS2_CONDOR_HOST=localhost:9618 pytest tests/integration/test_condor_submit.py -v`

## Design Decisions

- **asyncio.to_thread() for htcondor2**: The Python bindings are synchronous. Wrapping in `to_thread()` keeps the main event loop responsive while HTCondor calls block.
- **DAGMan status via .status file**: Parsing the JSON status file is more reliable than querying individual node statuses through HTCondor. The file is updated by DAGMan itself.
- **Completion detection is two-tier**: Check queue first (fast), then history (authoritative). If not in either, treat as completed — DAGMan may have exited before history was written.
- **Work unit granularity**: Only `mg_NNNNNN` nodes in the outer DAG represent work units. Inner nodes (landing, proc, merge, cleanup) are DAGMan's concern.

---

# Phase 4 — CLI Runner + Real Service Integration

**Date**: 2026-02-17
**Spec Version**: 2.4.0
**Phase**: 4 — CLI, Adapter Fixes for Production Services, Dev Infrastructure

---

## What Was Built

### CLI Runner (`cli.py`, `__main__.py`)

**Entry point**: `python -m wms2 import <request_name> [options]`

**Options**:
| Flag | Default | Description |
|---|---|---|
| `--proxy` | `/tmp/x509up_u$UID` | X.509 proxy cert (used as both cert and key) |
| `--cert`, `--key` | — | Separate cert/key paths (alternative to --proxy) |
| `--condor-host` | `localhost:9618` | HTCondor collector address |
| `--schedd-name` | auto-discover | Explicit schedd name |
| `--submit-dir` | `/tmp/wms2` | DAG file output directory |
| `--max-files N` | `0` (all) | Limit DBS file query |
| `--files-per-job N` | from request | Override splitting parameter |
| `--dry-run` | off | Plan DAG but don't submit to HTCondor |
| `--db-url` | from settings | Override database URL |
| `--poll-interval` | `10` | Monitoring poll interval in seconds |
| `--log-level` | `INFO` | Logging verbosity |

**Pipeline** (`run_import`):
1. Resolve X.509 credentials (proxy auto-detection or explicit cert/key)
2. Build SSL context with CERN Grid CA (`/etc/grid-security/certificates`)
3. Connect to PostgreSQL, build real adapters (ReqMgr2, DBS, Rucio, HTCondor)
4. Fetch request from ReqMgr2, normalize StepChain/TaskChain fields
5. Create request + workflow rows in DB
6. Plan production DAG (DBS file fetch → splitting → DAG file generation)
7. If `--dry-run`: stop here with summary
8. Submit DAG to HTCondor, monitor with `DAGMonitor.poll_dag()` until terminal

**StepChain/TaskChain normalization** (`_normalize_request`):
- StepChain: `InputDataset` from Step1 or first `OutputDatasets` entry; `SplittingAlgo` from Step1
- TaskChain: `InputDataset` from Task1 or first `OutputDatasets` entry; `SplittingAlgo` from Task1
- Default `SandboxUrl` to "N/A" if missing
- Build `SplittingParams` from scattered fields (`FilesPerJob`, `EventsPerJob`, `LumisPerJob`)

**Executables overridden to `/bin/true`** in CLI mode — all jobs are trivial for local testing.

### Adapter Fixes for Real CMS Services

**ReqMgr2Client** (`adapters/reqmgr2.py`):
- Fixed URL path duplication: base URL `https://cmsweb.cern.ch/reqmgr2` already has `/reqmgr2`, so request paths changed from `/reqmgr2/data/request/` to `/data/request/`
- Fixed response unwrapping: ReqMgr2 returns `{result: [{request_name: {fields}}]}` — must extract inner dict by checking `if request_name in row`
- Added `verify` parameter for custom SSL context

**DBSClient** (`adapters/dbs.py`):
- Removed `limit` from DBS query params — DBS `/files` endpoint returns HTTP 400 if `limit` is passed. Files are now sliced in Python after fetch.
- Added `verify` parameter for custom SSL context

**RucioClient** (`adapters/rucio.py`):
- Added `verify` parameter for custom SSL context
- Note: Rucio auth still fails with proxy cert DN. Adapter falls back to empty replica map in DAG planner.

### Config Additions (`config.py`)

```python
# Job executables (override to /bin/true for local testing)
processing_executable: str = "run_payload.sh"
merge_executable: str = "run_merge.sh"
cleanup_executable: str = "run_cleanup.sh"

# Input file limit (0 = no limit, >0 = cap DBS file query)
max_input_files: int = 0

# SSL
ssl_ca_path: str = "/etc/grid-security/certificates"
```

### DAG Planner Changes (`core/dag_planner.py`)
- Executables threaded from `settings` through `plan_production_dag()` → `_generate_dag_files()` → `_generate_group_dag()`
- `max_input_files` passed to DBS query with Python-side slicing
- Rucio failures caught with try/except, falls back to empty replica map (non-fatal)

### Dev Infrastructure Files

**`condor/config`** — HTCondor container config:
- Fast negotiation intervals (10s) for testing
- `ALLOW_WRITE/READ/ADMINISTRATOR/NEGOTIATOR = *`
- `SEC_DEFAULT_AUTHENTICATION = OPTIONAL` (required for unauthenticated host submissions)
- `DAGMAN_USE_DIRECT_SUBMIT = True`

**`docker-compose.yml`** — Service definitions:
- `db`: PostgreSQL 15, port 5432, healthcheck
- `condor`: htcondor/mini, port 9618, mounts `condor/config` as `99-wms2.conf`
- `wms2`: App build, port 8000, depends on db

**`install.sh`** — Native setup script (RHEL 9):
- HTCondor 25.x from official repo (EPEL + CRB required)
- Personal condor config (all daemons on localhost, shared port 9618)
- PostgreSQL setup (commented out — instructions for manual steps)

---

## Real Service Testing

Tested with workflow `cmsunified_task_TSG-Run3Summer23BPixGS-00097__v1_T_231129_092644_4472`:

**Dry-run (verified working)**:
```
python -m wms2 import cmsunified_task_TSG-Run3Summer23BPixGS-00097__v1_T_231129_092644_4472 \
  --dry-run --max-files 10 --proxy /tmp/x509up_u11792
```
- ReqMgr2 fetch: OK (StepChain, normalized successfully)
- DBS files: OK (10 files fetched, sliced in Python)
- Rucio: Falls back to empty map (auth issue)
- DAG generation: OK (submit files with `executable = /bin/true`)

**Full submit**: NOT YET TESTED — was blocked on HTCondor container auth on vocms118.

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/ tests/integration/test_dag_planner.py -v` — 96 tests pass
3. Dry-run: `python -m wms2 import <request_name> --dry-run --max-files 10`
4. Full run: `python -m wms2 import <request_name> --max-files 10` (requires working HTCondor)

## Key Bugs Fixed

| Bug | Root Cause | Fix |
|---|---|---|
| ReqMgr2 404 | URL path doubled (`/reqmgr2/reqmgr2/data/...`) | Changed paths to `/data/...` (base URL includes `/reqmgr2`) |
| ReqMgr2 wrong data | Response is `{result: [{name: {fields}}]}` | Unwrap inner dict by name key |
| DBS 400 | `/files` endpoint rejects `limit` query param | Removed from params, slice in Python |
| Rucio 400 | Proxy cert DN not recognized for token auth | Non-fatal fallback to empty replica map |
| StepChain missing fields | `InputDataset`, `SplittingAlgo` in Step1 not top-level | `_normalize_request()` extracts from Step1/Task1 |
| rucio_account garbage | Config had Unicode `\ufffd` char | Fixed to `"wms2"` |
| condor_dagman not in PATH | `Submit.from_dag()` needs binary on PATH | Shim: `echo '#!/bin/sh' > /tmp/condor_dagman` |

## Design Decisions

- **CLI creates rows directly**: Rather than calling `WorkflowManager.import_request()` (which re-fetches and lacks normalization), the CLI creates request + workflow rows directly with normalized data.
- **Executables as settings, not hardcoded**: `processing_executable`, `merge_executable`, `cleanup_executable` in `Settings` allow override to `/bin/true` for testing without changing DAG generation logic.
- **SSL context passed as `verify` param**: All HTTP adapters accept `verify=ssl.SSLContext` for CERN Grid CA. This is the httpx pattern for custom trust stores.
- **Rucio failure is non-fatal**: In `plan_production_dag`, Rucio errors are caught and logged. The planner proceeds with empty site locations — jobs will use AAA (remote data access) instead of data locality.

## What's Next

- ~~Full HTCondor submission test on new dev VM~~ — Done (Phase 5)
- ~~Output Manager — DBS registration, Rucio rule creation for completed merge groups~~ — Done (Phase 5)
- Error Handler — Failure classification, rescue DAG decision logic
- Site Manager — CRIC sync for site status and capacity

---

# Phase 5 — Output Manager + Trivial Test Scripts + End-to-End Pipeline

**Date**: 2026-02-17
**Spec Version**: 2.4.0
**Phase**: 5 — Output Management, DAG Fixes, Real E2E Verification

---

## What Was Built

### Output Manager (`core/output_manager.py`)

Full post-compute pipeline with 8-state machine:
```
PENDING → DBS_REGISTERED → SOURCE_PROTECTED → TRANSFERS_REQUESTED
→ TRANSFERRING → TRANSFERRED → SOURCE_RELEASED → ANNOUNCED
```

- `handle_merge_completion(workflow, merge_info)` — Creates PENDING OutputDataset records (one per output dataset × per merge group)
- `process_outputs_for_workflow(workflow_id)` — Advances all outputs through the pipeline
- `_advance_output(output)` — Drives state transitions with local variable tracking for multi-step advancement in one call
- `_register_in_dbs()` → `_protect_source()` → `_request_transfers()` → `_check_transfer_status()` → `_release_source_protection()` → `_announce()`
- `validate_local_output(lfn)` — Checks file existence on local SE
- `get_output_summary(workflow_id)` — Counts by status for display

All DBS/Rucio calls go through adapter interfaces. Mock adapters make every step succeed immediately — the full state machine runs in a single `process_outputs_for_workflow()` call. When real adapters are wired in, transfers will take real time.

### LFN Derivation Helpers (`core/output_lfn.py`)

CMS LFN convention utilities:
- `derive_merged_lfn_bases(request_data)` — Parses `OutputDatasets` from ReqMgr2 response, extracts PrimaryDataset/AcquisitionEra/ProcessingString/DataTier, builds merged LFN base paths
- `_parse_dataset_name(name)` — Regex extraction from DBS format `/{Primary}/{Era}-{Proc}-v{N}/{Tier}`
- `merged_lfn_for_group(base, index, filename)` — Constructs `{base}/{NNNNNN}/{filename}`
- `local_output_path(output_base_dir, lfn)` — Converts `/store/mc/...` to local path

### Trivial Test Scripts

Processing and merge jobs now produce real output instead of running `/bin/true`:

**`wms2_proc.sh`** — Generated in submit dir, writes one line of job metadata to stdout:
```
2026-02-17T15:32:58Z | pid=54134 | host=localhost.localdomain | args: --sandbox N/A --input synthetic://gen/events_1_100000
```
HTCondor captures stdout as `proc_NNNNNN.out`.

**`wms2_merge.py`** — Generated in submit dir, reads all `proc_*.out` files from the merge group directory, writes one `merged.txt` per output dataset to the local SE:
```
# WMS2 Merged Output
# Dataset:     /QCD_.../AODSIM
# Data tier:   AODSIM
# Merge group: mg_000000
# Generated:   2026-02-17T15:34:30Z
# Host:        localhost.localdomain
# Jobs merged: 1
#
proc_000000 | 2026-02-17T15:32:58Z | pid=54134 | host=localhost.localdomain | args: --sandbox N/A --input synthetic://gen/events_1_100000
```

Output files follow CMS LFN conventions on the local SE:
```
/mnt/shared/store/mc/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-vN/{NNNNNN}/merged.txt
```

**Auto-substitution**: When executables are `/bin/true` (test mode), `_generate_group_dag()` automatically uses the generated scripts instead. Production executables are used as-is.

### DAG Monitor Fixes (`core/dag_monitor.py`)

**ClassAd parser rewrite**: DAGMan's `NODE_STATUS_FILE` uses ClassAd format, not JSON. Completely rewrote `_parse_dagman_status()`:
- Parses ClassAd blocks (`[ Type = "DagStatus"; NodesDone = N; ... ]`)
- Handles `DagStatus` block for aggregate counts and `NodeStatus` blocks for per-node status
- Maps NodeStatus integers: 0=not_ready, 1=ready, 2=prerun, 3=submitted, 4=postrun, 5=done, 6=error, 7=futile

**Work unit detection on DAG exit**: Fixed `_handle_dag_completion()` to also parse the `.status` file and call `_detect_completed_work_units()`. Previously, merge groups that completed between the last poll and DAGMan exit were lost.

### DAG Planner Fixes (`core/dag_planner.py`)

| Fix | Problem | Solution |
|---|---|---|
| SUBDAG EXTERNAL DIR | Sub-DAGMan couldn't find relative-path submit files | Added `DIR {mg_dir}` to each SUBDAG EXTERNAL line |
| NODE_STATUS_FILE | No `.status` file produced by DAGMan | Added `NODE_STATUS_FILE workflow.dag.status` directive |
| elect_site.sh fallback | Script failed on local dev (no GLIDEIN_CMSSite) | Fall back to "local" and always exit 0 |
| Merge arguments quoting | Embedded JSON double quotes rejected by HTCondor | Write `output_info.json` file, pass path in arguments |
| output_info.json format | Only had flat `output_lfn_bases` list | Now includes `output_datasets` with dataset_name, merged_lfn_base, data_tier |
| GEN workflow support | GEN workflows have no input files | `_plan_gen_nodes()` creates synthetic event-range nodes |

### Lifecycle Manager Wiring (`core/lifecycle_manager.py`)

- `_handle_active()` calls `output_manager.handle_merge_completion()` for each newly completed work unit
- Calls `output_manager.process_outputs_for_workflow()` to advance outputs through pipeline
- OutputManager passed as optional constructor arg (backward-compatible)

### CLI Enhancements (`cli.py`)

- Stores output dataset metadata in workflow `config_data` (from `derive_merged_lfn_bases`)
- Creates OutputManager with mock DBS/Rucio adapters
- Processes work unit completions through output pipeline during monitoring loop
- Prints output summary (counts by status) and lists merged output files on disk with sizes
- GEN workflow detection and handling

### Config (`config.py`)

```python
output_base_dir: str = "/mnt/shared/store"   # Local SE staging area
```

### Tests

**42 new tests (138 total, all passing)**:

| File | New Tests | What |
|---|---|---|
| `test_output_lfn.py` | 9 | LFN derivation, dataset parsing, block dirs, local path conversion |
| `test_output_manager.py` | 9 | Merge completion records, full pipeline, DBS/Rucio adapter calls, output summary, file validation |
| `test_dag_monitor.py` | 10 (rewritten) | ClassAd format parsing, completion detection, work unit tracking on DAG exit |
| `test_dag_generator.py` | 4 | Trivial script generation, test mode substitution, output_info.json format |
| `test_lifecycle.py` | 2 | Output manager wiring, backward compat without output manager |
| `test_api.py` | 2 | Clean stop endpoint, stop non-active request |
| Integration tests | 6 | DAG planner e2e, API endpoints |

---

## Real End-to-End Verification

Tested with real ReqMgr2 workflow `cmsunified_task_GEN-RunIII2024Summer24GS-00002__v1_T_260204_161305_1359`:

```
su - wms2 -s /bin/bash -c 'source .venv/bin/activate && python -m wms2 import \
  cmsunified_task_GEN-RunIII2024Summer24GS-00002__v1_T_260204_161305_1359 \
  --max-files 5 --proxy /tmp/x509up_wms2 --submit-dir /mnt/shared/wms2_submit \
  --poll-interval 5 --ca-bundle /tmp/cern-ca-bundle.pem \
  --db-url "postgresql+asyncpg://wms2:wms2dev@localhost:5432/wms2"'
```

**Result**: All 5 work units completed, 15 output records (3 datasets × 5 merge groups), all ANNOUNCED.

**Output files on disk** (15 files in `/mnt/shared/store/`):
```
mc/RunIII2024Summer24DRPremix/QCD_.../AODSIM/140X_...-v2/000000/merged.txt     (434 bytes)
mc/RunIII2024Summer24DRPremix/QCD_.../AODSIM/140X_...-v2/000001/merged.txt     (439 bytes)
...
mc/RunIII2024Summer24MiniAODv6/QCD_.../MINIAODSIM/150X_...-v2/000000/merged.txt (442 bytes)
...
mc/RunIII2024Summer24NanoAODv15/QCD_.../NANOAODSIM/150X_...-v2/000000/merged.txt (443 bytes)
...
```

Each file contains a header (dataset, tier, merge group, timestamp, host, job count) followed by one line per processing job with PID, hostname, timestamp, and arguments.

---

## Key Bugs Fixed During E2E Testing

| Bug | Symptom | Root Cause | Fix |
|---|---|---|---|
| No status file | DAG monitor warns "Status file not found" | DAGMan only produces `.status` if `NODE_STATUS_FILE` directive present | Added directive to outer DAG |
| Sub-DAG failures | All 5 merge group SUBDAGs fail immediately | `SUBDAG EXTERNAL` without `DIR` — sub-DAGMan can't find relative-path submit files | Added `DIR {mg_dir}` |
| elect_site.sh exit 1 | POST script failure kills sub-DAG | `MATCH_GLIDEIN_CMSSite` undefined on local dev | Fall back to "local", always exit 0 |
| JSON parse error | `json.JSONDecodeError` on `.status` file | NODE_STATUS_FILE is ClassAd format, not JSON | Rewrote parser for ClassAd blocks |
| HTCondor quote error | "illegal unescaped double-quote" in merge.sub | JSON with double quotes embedded in `arguments` line | Write to `output_info.json` file, pass path |

## Design Decisions

- **Mock adapters for default output lifecycle**: With mock DBS/Rucio, the full 8-state pipeline runs in one call. When real adapters are configured, transfers will take time and `TRANSFERRING` state will persist across multiple poll cycles.
- **Script generation over script installation**: `wms2_proc.sh` and `wms2_merge.py` are generated in the submit directory alongside the DAG files, not installed as package scripts. This keeps the submit directory self-contained.
- **ClassAd parser instead of JSON**: DAGMan's NODE_STATUS_FILE uses ClassAd format with `[key = value;]` blocks. The parser handles `DagStatus` (aggregate counts) and `NodeStatus` (per-node) block types.
- **Output info as separate file**: Complex merge arguments (JSON with dataset info) can't be safely embedded in HTCondor submit file `arguments` lines. Writing to `output_info.json` and passing the file path avoids quoting issues.
- **One merged.txt per dataset per merge group**: Each output dataset tier (AODSIM, MINIAODSIM, NANOAODSIM) gets its own merged file at its own LFN path. This matches the CMS convention where each data tier is a separate dataset in DBS.

## What's Next

- Error Handler — Failure classification, rescue DAG decision logic
- Clean Stop — ACTIVE → STOPPING → RESUBMITTING → QUEUED rescue DAG flow
- Site Manager — CRIC sync for site status and capacity
- Continuous lifecycle loop — FastAPI daemon polling all active requests
- Real DBS/Rucio adapters — actual service calls for output registration and transfers

---

# Phase 6 — Processing Wrapper + Sandbox Builder

**Date**: 2026-02-17
**Spec Version**: 2.4.0
**Phase**: 6 — Real Payload Execution Support

---

## What Was Built

### Sandbox Builder (`core/sandbox.py`)

Creates `sandbox.tar.gz` with manifest + PSet configs:

- `create_sandbox(output_path, request_data, mode)` — Builds tarball with `manifest.json` + per-step PSet configs
- Three modes: `auto` (detects CMSSW from request data), `synthetic`, `cmssw`
- `_build_manifest()` — Generates manifest from ReqMgr2 request data:
  - Synthetic: `size_per_event_kb`, `time_per_event_sec`, `memory_mb`, `output_tiers`
  - CMSSW: `cmssw_version`, `scram_arch`, `global_tag`, `multicore`, `memory_mb`, plus per-step info
- `_extract_steps()` — Handles StepChain multi-step requests (step chaining via `input_from_step`)
- `_create_test_pset()` — Generates minimal working CMSSW PSet.py files for testing

**Sandbox format**:
```
sandbox.tar.gz
├── manifest.json           # Job config
└── steps/                  # Per-step CMSSW configs (CMSSW mode only)
    ├── step1/PSet.py
    ├── step2/PSet.py
    └── ...
```

### Processing Wrapper — Rewritten `_write_proc_script()` in `dag_planner.py`

Replaced one-liner with ~200-line bash wrapper (`run_payload.sh`):

**Argument parsing**: `--sandbox`, `--input`, `--first-event`, `--last-event`, `--events-per-job`, `--node-index`, `--pilot`

**CMSSW mode** (`run_cmssw_mode`):
- Sources CVMFS: `/cvmfs/cms.cern.ch/cmsset_default.sh`
- Sets up CMSSW via `scramv1 project` + `scramv1 runtime`
- Converts LFNs to xrootd URLs (`root://cms-xrd-global.cern.ch/`)
- Runs multi-step `cmsRun` with FrameworkJobReport parsing
- Chains step outputs (step N output → step N+1 input)
- Falls back to synthetic mode if CVMFS unavailable

**Synthetic mode** (`run_synthetic_mode`):
- Reads `size_per_event_kb` and `time_per_event_sec` from manifest
- Creates sized output file via `dd if=/dev/urandom`
- Simulates processing time with proportional `sleep` (capped at 300s)

**Pilot mode** (`run_pilot_mode`):
- Iterative measurement: runs N, 2N, 4N events
- Collects wall time, output size, peak RSS per iteration
- Writes `pilot_metrics.json` with derived metrics

### Merge Wrapper — Enhanced `_write_merge_script()` in `dag_planner.py`

Added ROOT file handling alongside existing text mode:

- Detects output type: `.root` files → ROOT merge, `proc_*.out` → text merge
- **ROOT mode**: Sets up CMSSW environment, uses `hadd` for ROOT file merging
- **Text mode**: Unchanged — concatenates `proc_*.out` into `merged.txt`
- Falls back to copying ROOT files if `hadd` not available

### Submit File Updates — `_write_submit_file()` in `dag_planner.py`

New parameters:
- `memory_mb` — `request_memory` in submit file (omitted if 0)
- `disk_kb` — `request_disk` in submit file (omitted if 0)
- `transfer_input_files` — HTCondor file transfer list (for sandbox)

### Group DAG Updates — `_generate_group_dag()` in `dag_planner.py`

- Processing node arguments now include: `--node-index`, `--first-event`, `--last-event`, `--events-per-job`
- Sandbox tarball added to `transfer_input_files` for processing nodes
- Resource parameters (memory, disk) passed through to submit files
- Sandbox reference uses basename (for file transfer) instead of full URL

### DAG Files Signature — `_generate_dag_files()` in `dag_planner.py`

New parameters:
- `sandbox_path` — Local path to `sandbox.tar.gz` (passed through to groups)
- `resource_params` — `{"memory_mb": N, "disk_kb": N}` (passed through to submit files)

### CLI Updates (`cli.py`)

- New `--sandbox-mode` flag: `auto` (default), `synthetic`, `cmssw`
- Creates sandbox during import (`create_sandbox()`)
- Stores CMSSW metadata in workflow `config_data`: `cmssw_version`, `scram_arch`, `global_tag`, `memory_mb`, `multicore`, `time_per_event`, `size_per_event`
- Prints CMSSW info (version, arch, global tag) when available
- Output file detection includes `.root` files alongside `.txt`

### Workflow Manager Updates (`core/workflow_manager.py`)

- `import_request()` now stores CMSSW fields in `config_data`

### Tests

**21 new tests (157 total, all passing)**:

| File | New Tests | What |
|---|---|---|
| `test_sandbox.py` | 9 | Manifest building (synthetic/CMSSW/StepChain), PSet generation, tarball creation, auto-detection |
| `test_dag_generator.py` | 12 | Processing wrapper content (mode dispatch, sandbox extraction, pilot mode), merge wrapper (ROOT handling, text fallback), resource requests, transfer_input_files, event range args |

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/test_sandbox.py -v` — 9 tests pass
3. `pytest tests/unit/test_dag_generator.py -v` — 21 tests pass
4. `pytest tests/unit/ tests/integration/ -v -k "not condor"` — 157 tests pass

## Design Decisions

- **Sandbox is a tarball, not a pickle**: Unlike WMCore's WMSandbox (pickled Python objects), WMS2's sandbox uses JSON manifest + ready-to-use PSet.py files. No WMCore dependency on worker nodes.
- **Wrapper lives in submit_dir, not sandbox**: The processing wrapper is generated alongside the DAG files. The sandbox only contains config and PSet files. This keeps the wrapper updatable without re-creating sandboxes.
- **CMSSW fallback to synthetic**: If CMSSW mode is requested but `/cvmfs/cms.cern.ch` is unavailable, the wrapper falls back to synthetic mode with a warning. This enables testing on non-CVMFS hosts.
- **Resource requests optional**: `request_memory` and `request_disk` are omitted from submit files when set to 0. This avoids over-constraining jobs on development pools.
- **Transfer sandbox via HTCondor**: The sandbox tarball is included in `transfer_input_files` so HTCondor handles delivery to worker nodes. No separate file staging needed.

## HTCondor E2E Verification (Synthetic Mode)

Re-ran with same real ReqMgr2 workflow after capping synthetic output sizes:

```
su -c '.venv/bin/python -m wms2 import \
  cmsunified_task_GEN-RunIII2024Summer24GS-00002__v1_T_260204_161305_1359 \
  --max-files 5 --proxy /tmp/x509up_wms2 --submit-dir /mnt/shared/wms2_submit \
  --poll-interval 5 --ca-bundle /tmp/cern-ca-bundle.pem --sandbox-mode synthetic \
  --db-url "postgresql+asyncpg://wms2:wms2dev@localhost:5432/wms2"' wms2
```

**Result**: All 5 work units completed, 15 output records (3 datasets × 5 groups), all ANNOUNCED.

**Processing wrapper verified**:
- Sandbox extracted, manifest parsed, synthetic mode detected
- Output capped at 10MB per file (was ~58GB uncapped — fixed)
- Sleep capped at 10s
- Each proc node produces `proc_N_output.root` (10,485,760 bytes)

**Merge wrapper verified**:
- ROOT files detected, `hadd` unavailable (no CMSSW) so fallback to copy
- Files copied to correct CMS LFN paths on local SE

**15 output files on disk** (150MB total in `/mnt/shared/store/`):
```
mc/RunIII2024Summer24DRPremix/QCD_.../AODSIM/140X_...-v2/000000/proc_0_output.root  (10MB)
mc/RunIII2024Summer24MiniAODv6/QCD_.../MINIAODSIM/150X_...-v2/000000/proc_0_output.root  (10MB)
mc/RunIII2024Summer24NanoAODv15/QCD_.../NANOAODSIM/150X_...-v2/000000/proc_0_output.root  (10MB)
... (×5 merge groups)
```

**Bug fixed**: CLI output file detection glob was looking for `merged_*.root` but actual files are `proc_N_output.root`. Changed to `*.root`.

## What's Next

- CVMFS setup on dev VM — Enable real CMSSW execution via CVMFS client
- HTCondor e2e (CMSSW) — Once CVMFS available, verify real `cmsRun` execution
- Error Handler — Failure classification, rescue DAG decisions
- Clean Stop — Full clean stop flow with rescue DAGs
