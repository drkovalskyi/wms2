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
- Site Manager — CRIC sync for site status and capacity
- Real DBS/Rucio adapters — actual service calls for output registration and transfers

---

# Phase 7 — Error Handler: Failure Classification and Recovery

**Date**: 2026-02-17
**Spec Version**: 2.4.0
**Phase**: 7 — Error Handler (Spec Section 4.8, Section 6)

---

## What Was Built

### Error Handler (`core/error_handler.py`)

Workflow-level failure classification and recovery. When a DAG completes with failures, the Error Handler classifies by failure ratio and decides: auto-rescue, flag for review, or abort.

**`handle_dag_failure(dag, request, workflow) → "abort"`**:
- DAG with zero successes — always aborts
- Sets workflow status to FAILED
- Records `dag_failed` event in DAG history

**`handle_dag_partial_failure(dag, request, workflow) → "rescue" | "review" | "abort"`**:
- Computes `failure_ratio = nodes_failed / total_nodes`
- Checks rescue attempt count against max before classification
- Classification thresholds:
  - `ratio < error_auto_rescue_threshold` (default 0.05 = 5%) → auto-rescue
  - `ratio < error_abort_threshold` (default 0.30 = 30%) → flag for review (operator alert logged)
  - `ratio >= error_abort_threshold` → abort
- Records `dag_partial` event with failure ratio in DAG history

**`_prepare_rescue(dag, request, workflow)`**:
- Finds highest-numbered rescue DAG file on disk (`.rescue001` through `.rescue099`)
- Creates new DAG record with `parent_dag_id` pointing to failed DAG, status=READY
- Updates workflow with new DAG id and RESUBMITTING status
- Reuses the same pattern as `_prepare_recovery()` in lifecycle manager (clean stop)

**`_count_rescue_chain(dag) → int`**:
- Walks `parent_dag_id` chain to count how many rescue attempts have been made
- Used as guard against infinite rescue loops

**Return value convention**: Methods return `"rescue"`, `"review"`, or `"abort"`. The lifecycle manager uses this to decide the state transition — it still owns the state machine.

### Config Settings (`config.py`)

```python
error_auto_rescue_threshold: float = 0.05   # Below 5% → auto-rescue
error_abort_threshold: float = 0.30          # Above 30% → abort
error_max_rescue_attempts: int = 3           # Max rescue chain length
```

### Lifecycle Manager Integration (`core/lifecycle_manager.py`)

**`_handle_active()` — PARTIAL branch**:
- If error_handler present: calls `handle_dag_partial_failure()`
  - `"rescue"` → request transitions to RESUBMITTING (→ QUEUED → ACTIVE via rescue DAG)
  - `"abort"` → request transitions to FAILED
  - `"review"` → falls through to PARTIAL (operator intervention needed)
- If no error_handler → request transitions to PARTIAL (backward compatible)

**`_handle_active()` — FAILED branch**:
- If error_handler present: calls `handle_dag_failure()` (records event, sets workflow FAILED)
- Request always transitions to FAILED regardless

**Both poll-result and cached-status code paths** updated — error handler is called whether the DAG status comes from a fresh `poll_dag()` result or from a previously-stored DAG status.

**`_handle_partial()` — simplified to stub**:
- Previous implementation called `error_handler.handle_dag_partial_failure(dag)` with wrong signature
- Now a no-op stub — initial classification happens in `_handle_active()` when DAG first reaches PARTIAL
- Can be enhanced later with manual retry support (operator triggers re-evaluation)

### FastAPI Wiring (`main.py`)

ErrorHandler created and injected into lifecycle manager alongside other components:
```python
eh = ErrorHandler(repo, condor, settings)
lm = RequestLifecycleManager(..., error_handler=eh)
```

### Tests

**12 new tests (169 total, all passing)**:

| File | New Tests | What |
|---|---|---|
| `test_error_handler.py` | 9 | Low ratio → rescue, medium → review, high → abort, zero successes → abort, rescue DAG path discovery (single + highest), max rescue exceeded, DAG history recorded, custom thresholds |
| `test_lifecycle.py` | 3 | PARTIAL + rescue → RESUBMITTING, PARTIAL without handler → PARTIAL, FAILED + handler called → FAILED |

---

## Recovery Flow

The full rescue flow after a partial DAG failure:

```
DAG finishes PARTIAL (some nodes failed)
  → Error Handler classifies failure ratio
    → If < 5%: "rescue"
      → _prepare_rescue creates new DAG record (status=READY, parent_dag_id=old)
      → Workflow updated to point to new DAG
      → Request: ACTIVE → RESUBMITTING → QUEUED
      → _handle_queued detects rescue DAG (dag.rescue_dag_path + status=READY)
      → Rescue DAG submitted to HTCondor
      → Request: QUEUED → ACTIVE (monitoring resumes)
    → If 5-30%: "review"
      → Request stays PARTIAL, operator alert logged
    → If > 30%: "abort"
      → Workflow set to FAILED
      → Request: ACTIVE → FAILED
```

This reuses the same rescue DAG mechanism as clean stop recovery (Phase 1), but triggered automatically by failure ratio instead of operator request.

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/test_error_handler.py -v` — 9 tests pass
3. `pytest tests/unit/test_lifecycle.py -v` — 21 tests pass (18 existing + 3 new)
4. `pytest tests/unit/ tests/integration/ -v -k "not condor"` — 169 tests pass

## Design Decisions

- **Error handler returns action strings, not transitions**: The error handler returns `"rescue"`, `"review"`, or `"abort"` — the lifecycle manager decides the actual state transition. This preserves the single-owner-of-state-machine invariant.
- **Rescue chain count via parent_dag_id traversal**: Rather than storing a counter, we walk the `parent_dag_id` chain. This is accurate even if records are created by different code paths (clean stop vs error handler).
- **Thresholds are < (strict), not <=**: Exactly 5% failure ratio goes to "review", not "rescue". This is conservative — borderline cases get human review.
- **_handle_partial simplified to stub**: The old implementation had a broken call signature. Classification now happens once when the DAG first reaches PARTIAL status (in `_handle_active`). Re-evaluation of PARTIAL requests can be added later for manual retry support.
- **CLI not wired**: The CLI runs a direct monitoring loop without the lifecycle manager, so the error handler is not needed there. Error handling in CLI mode is manual (operator sees the final DAG status).

## What's Next

- Site Manager — CRIC sync for site status and capacity
- CVMFS setup on dev VM — Enable real CMSSW execution
- Real DBS/Rucio adapters — actual service calls for output registration and transfers
- Continuous lifecycle loop testing — multi-request concurrent processing

---

# Phase 8 — Real CMSSW StepChain Execution via HTCondor

**Date**: 2026-02-18
**Spec Version**: 2.4.0
**Phase**: 8 — Real CMSSW Payload Verification

---

## What Was Built

### Real CMSSW StepChain Support — `_write_proc_script()` rewrite in `dag_planner.py`

Major rewrite of the processing wrapper to handle real multi-step CMSSW StepChain execution. The wrapper now has two execution paths:

**`run_step_native()`** — Direct execution on hosts with CVMFS:
- Sources `/cvmfs/cms.cern.ch/cmsset_default.sh`
- Creates CMSSW project via `scramv1 project`
- Evaluates runtime environment via `scramv1 runtime -sh`
- Overrides `CMS_PATH` with local siteconf symlinks to `/opt/cms/siteconf/` (or `$SITECONFIG_PATH`)
- Runs `cmsRun` with step-specific PSet.py

**`run_step_apptainer()`** — Container execution for cross-architecture (el8 on el9):
- Uses `apptainer exec` with `/cvmfs/unpacked.cern.ch` singularity images
- Binds CVMFS, siteconf, and X509 credentials into the container
- Passes `SCRAM_ARCH` from manifest for architecture selection

**Step chaining logic**:
- Step 1 gets `--firstEvent`, `--lastEvent`, `--nThreads`, `--customise_commands` for maxEvents
- Steps 2+ get `--filein file:` pointing to previous step's output, `--nThreads`, `--customise_commands` for maxEvents=-1
- GEN steps: uses `--customise_commands 'process.maxEvents.input=...'` since there's no input file
- Non-GEN steps: output file names derived from step config in manifest

**PSet maxEvents injection**:
- Production PSets from ConfigCache have hardcoded `maxEvents` from cmsDriver (e.g., 100 events)
- Steps 2+ need `maxEvents=-1` injected to process all input from previous step
- Added `process.maxEvents.input = cms.untracked.int32(-1)` append to PSet.py for steps 2+

### HTCondor Environment Passthrough — `_write_submit_file()` and `_generate_group_dag()`

HTCondor sanitizes job environments by default. Added explicit environment passthrough:

**`_write_submit_file()`** — New `environment: dict[str, str] | None` parameter:
```python
if environment:
    env_str = " ".join(f"{k}={v}" for k, v in environment.items())
    lines.append(f'environment = "{env_str}"')
```

**`_generate_group_dag()`** — Passes three env vars from submitter to proc jobs:
- `X509_USER_PROXY` — Required for xrootd pileup access via `root://cms-xrd-global.cern.ch/`
- `X509_CERT_DIR` — CA certificate directory for grid authentication
- `SITECONFIG_PATH` — Path to CMS site-local-config (defaults to `/opt/cms/siteconf/`)

### StepChain Module (`core/stepchain.py`)

New module for CMSSW StepChain request handling:
- `extract_steps_from_request()` — Parses ReqMgr2 StepChain request into ordered list of step dicts
- `get_step_output_modules()` — Determines output module names and dataset tiers per step
- `resolve_input_chain()` — Maps step N input to step N-1 output for file chaining

### PSet Generator (`core/pset_generator.py`)

New module for downloading and processing ConfigCache PSets:
- `fetch_config_cache_pset()` — Downloads PSet.py from CouchDB ConfigCache
- `inject_max_events()` — Appends `maxEvents=-1` for steps 2+

### Pilot Runner (`core/pilot_runner.py`)

Standalone pilot execution without full DAG:
- `run_pilot()` — Executes a single processing node with small event count
- Measures wall time, RSS, output size per step
- Returns structured pilot metrics

### End-to-End Test Script (`scripts/e2e_real_condor.py`)

New script for real CMSSW HTCondor verification:
- Fetches request from ReqMgr2 (HTTPS with CA bundle)
- Downloads ConfigCache PSets from CouchDB
- Builds sandbox with real PSets
- Creates mock workflow matching request parameters
- Submits production DAG via DAGPlanner
- Polls DAGMan to completion with progress display
- `--events N` — Events per job (default 1000)
- `--jobs N` — Number of parallel jobs (default 1)
- `--request` — ReqMgr2 request name

### Unit Tests

| File | Tests | What |
|---|---|---|
| `test_stepchain.py` | 6 | Step extraction, output module resolution, input chaining |
| `test_pilot_runner.py` | 4 | Pilot metric collection, error handling |

---

## Verification — 8-Job Real CMSSW StepChain via HTCondor

**Request**: `cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54`
**Chain**: 5 steps — GEN-SIM → DRPremix → HLT → MiniAOD → NanoAOD
**CMSSW versions**: 12_4_16 (steps 1-2), 12_4_14_patch3 (step 3), 13_0_23 (step 4), 13_0_19 (step 5)
**Architecture**: el8 via apptainer on el9 host
**Resources**: 8 cores/job, 16 GB memory

```
python scripts/e2e_real_condor.py \
  --request cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54 \
  --events 50 --jobs 8
```

**Result**: 8/8 jobs completed all 5 steps in ~25 minutes. Full 64-core utilization.

```
[5/6] Polling DAGMan (max 7200s)...
    [   0s] status=running nodes: 0/0 done, 0 queued, 0 ready, 0 failed
    ...
    [1526s] status=running nodes: 8/8 done, 0 queued, 0 ready, 0 failed
    DAGMan finished successfully

[6/6] Results
    Status:  PASS
    Time:    1526s (25.4 min)
    Outputs: 8 × 5-step StepChain = 40 cmsRun executions, all succeeded
```

**Key milestones verified**:
- xrootd pileup access works (MCPileup for DRPremix step via `root://cms-xrd-global.cern.ch/`)
- Cross-CMSSW-version step chaining works (5 different releases in one job)
- el8/el9 cross-architecture execution via apptainer works
- Site-local-config override via CMS_PATH works across all steps
- ConfigCache PSets with maxEvents injection work
- 8 parallel jobs × 8 cores = 64 cores fully utilized

---

## Bugs Fixed

1. **xrootd authentication failure in HTCondor jobs**: Step 2 (DRPremix) crashed with `[FATAL] Auth failed` trying to access MCPileup files. Root cause: `X509_USER_PROXY` not set in HTCondor job environment. Fixed by adding `environment` dict to submit files with proxy path passthrough.

2. **Site-local-config not found in native execution**: Step 4 (MiniAOD, CMSSW_13_0_23) crashed with "Valid site-local-config not found at /cvmfs/cms.cern.ch/SITECONF/local/". Root cause: `scramv1 runtime` sets `CMS_PATH=/cvmfs/cms.cern.ch` which overrides any prior setting. Fixed by adding `CMS_PATH` override *after* `eval $(scramv1 runtime -sh)` in `run_step_native()`, symlinking to local siteconf.

3. **Hardcoded maxEvents in ConfigCache PSets**: Production PSets from CouchDB had `process.maxEvents.input = cms.untracked.int32(100)` from cmsDriver. Steps 2+ would only process 100 events regardless of input. Fixed by appending `process.maxEvents.input = cms.untracked.int32(-1)` to PSet.py for all non-first steps.

4. **Single-job bottleneck**: Initial test used 1 job × 1000 events (only 8 of 64 cores). Changed to support `--jobs N` for parallel execution within one merge group.

## Design Decisions

- **Environment passthrough is explicit, not blanket**: Only three specific env vars are forwarded (X509_USER_PROXY, X509_CERT_DIR, SITECONFIG_PATH). Using `getenv = true` in submit files would leak the full submitter environment, which is fragile and can conflict with CMSSW's `scramv1 runtime`.
- **CMS_PATH override after scramv1**: The siteconf symlink approach (creating `$WORK_DIR/_cms/SITECONF/local/` with links to real config) is more robust than modifying `/cvmfs/` or relying on `$SITECONF` — it works for all CMSSW versions and both native/apptainer paths.
- **maxEvents=-1 injected at PSet level, not cmsRun CLI**: Using `--customise_commands` for maxEvents on step 1 (GEN) and PSet append for steps 2+. CLI `--number` flag only works for step 1; steps 2+ must read all input from previous step, which requires PSet-level override.
- **50 events/job × 8 jobs for e2e testing**: Much faster iteration than 1000 events × 1 job (25 min vs 70+ min). Uses all available cores. Sufficient to verify the full pipeline without waiting for production-scale event counts.

## What's Next

- Multi-merge-group DAG — Submit workflow with multiple work units (currently only one merge group tested)
- Output registration — Wire merge outputs to Output Manager for DBS/Rucio registration
- Pilot integration — Use pilot_runner to measure real metrics before production DAG
- Site Manager — CRIC sync for site status and capacity
- Real DBS/Rucio adapters — actual service calls for output registration and transfers

---

# Phase 9 — Environment Test Infrastructure

**Date**: 2026-02-18
**Spec Version**: 2.4.0
**Phase**: 9 — Test Environment Gating

---

## What Was Built

### Test Levels

Four hierarchical test levels, each gating the next:

| Level | Scope | Checks |
|-------|-------|--------|
| 0 | Unit tests | Python >= 3.11, 8 core packages importable, PostgreSQL reachable |
| 1 | Local real CMSSW | CVMFS mounted, CMSSW releases available, valid X509 proxy, siteconf exists, apptainer available |
| 2 | Local HTCondor | HTCondor CLI tools, schedd reachable, execution slots available, htcondor2 Python bindings |
| 3 | Production CMS services | ReqMgr2 API, DBS API, CouchDB/ConfigCache (HTTPS + X509 auth) |

Levels are cumulative: a test marked `level2` requires both level 1 and level 2 checks to pass.

### Environment Check Module (`tests/environment/checks.py`)

Reusable check functions that return `(ok: bool, detail: str)`:
- Level 0: `check_python_version()`, `check_package_importable()`, `check_postgres_reachable()`
- Level 1: `check_cvmfs_mounted()`, `check_cmssw_release_available()`, `check_x509_proxy()`, `check_siteconf()`, `check_apptainer()`
- Level 2: `check_condor_binaries()`, `check_condor_schedd()`, `check_condor_slots()`, `check_condor_python_bindings()`
- Level 3: `check_reqmgr2_reachable()`, `check_dbs_reachable()`, `check_couchdb_reachable()`
- `run_level_checks(level)` — aggregate runner returning all results for a level

Level 3 checks use `curl` subprocess instead of httpx because Python's ssl module does not handle X509 proxy certificate chains correctly (sends only the proxy cert, not the full chain, resulting in HTTP 401).

### Environment Test Files (`tests/environment/test_level{0-3}.py`)

Each level has its own test file. Running `pytest tests/environment/` serves as a pre-flight check before any real tests.

### Gating Mechanism (`tests/conftest.py`)

- `pytest_configure()` — registers `level1`, `level2`, `level3` markers
- `pytest_collection_modifyitems()` — runs environment checks once per session (cached), auto-skips tests whose level prerequisites are not met
- Skip messages include the specific failure: `"Level 2 environment not ready: condor_schedd: ..."`

### Marker Registration (`pyproject.toml`)

Added `[tool.pytest.ini_options].markers` for `level1`, `level2`, `level3`, and legacy `condor`.

### Migration of Existing Tests

Updated `tests/integration/test_condor_submit.py`: replaced ad-hoc `WMS2_CONDOR_HOST` env var gate with `@pytest.mark.level2`.

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/environment/ -v` — 22 checks pass (all levels available on dev VM)
3. `pytest tests/ -v -k "not condor"` — 238 tests pass (22 environment + 216 existing)

## Design Decisions

- **Checks return (ok, detail), not raise**: Check functions are pure predicates. They never throw — this lets the gating logic collect all failures before deciding to skip, and gives clear diagnostic messages.
- **curl for Level 3 instead of httpx**: Python's ssl context doesn't forward the full X509 proxy certificate chain, causing HTTP 401 from CMS services. `curl` handles this correctly. This is the right tool for the job — the check is about the environment, not about Python's TLS stack.
- **Levels are cumulative**: A `level2` test auto-requires `level1`. This prevents confusing failures where HTCondor is available but CVMFS isn't.
- **Environment tests are not markers on themselves**: The `tests/environment/test_level*.py` files don't carry level markers — they *are* the environment checks. The markers are for gating actual functional tests.

---

## Phase 10 — Merge Mechanism Fix & Real CMSSW StepChain Verification (2026-02-19)

### Problem
When multiple proc jobs in a merge group produced output ROOT files, HTCondor transferred all
outputs to the same group directory (`mg_000000/`). Files with identical names from different
proc jobs (e.g., `B2G-RunIII2024Summer24wmLHEGS-00642.root`) overwrote each other — only the
last proc job's outputs survived. The merge job had no way to distinguish which files came from
which proc job, or which output tier each file belonged to.

### Changes

#### 1. Proc output rename (`dag_planner.py` — proc wrapper)
After all CMSSW steps complete, the proc wrapper renames every `*.root` file to
`proc_{NODE_INDEX}_{original}.root`. This prevents file collisions when HTCondor transfers
outputs from multiple proc jobs to the same merge group directory.

#### 2. Proc output manifest (`dag_planner.py` — proc wrapper)
After renaming, the proc wrapper parses all FrameworkJobReport XML files (`report_stepN.xml`)
to build a `proc_{NODE_INDEX}_outputs.json` manifest mapping each renamed file to its data
tier (extracted from the FJR `ModuleLabel` field, e.g., `MINIAODSIMoutput` → `MINIAODSIM`).

#### 3. Merge script tier-aware file grouping (`dag_planner.py` — merge script)
The merge script reads all `proc_*_outputs.json` manifests to build a tier→files mapping.
Each output dataset is matched to the correct subset of ROOT files by tier name, with
case-insensitive substring fallback. Only matching files are merged/copied to each dataset's
output directory.

#### 4. FJR output selection for step chaining (`dag_planner.py` — proc wrapper)
When a CMSSW step produces multiple output files (e.g., GEN-SIM + LHE), the proc wrapper now
prefers the non-LHE output for the next step's input. Previously it took the first FJR `<File>`
entry, which could be the LHE output — causing the next step to crash with `ProductNotFound`
for `generatorSmeared` HepMCProduct.

#### 5. ExternalLHEProducer nEvents sync (`dag_planner.py` — PSet injection)
When injecting `maxEvents` into the step 1 PSet, also update `ExternalLHEProducer.nEvents` to
match. Otherwise the gridpack produces more LHE events than cmsRun consumes, and
ExternalLHEProducer throws a fatal error at end-of-run.

#### 6. SITECONFIG_PATH fix (`dag_planner.py` — CMSSW execution)
CMSSW 14.x+ uses `SITECONFIG_PATH` (not `CMS_PATH`) to find `site-local-config.xml`. Updated
both native and apptainer execution paths. For apptainer, siteconf files are copied into the
execute directory (since CVMFS bind mounts are read-only).

#### 7. E2e test tier extraction (`e2e_real_condor.py`)
Fixed `_extract_pset_tier()` to parse output module names from the PSet
(`process.MINIAODSIMoutput = cms.OutputModule(...)`) instead of looking for filename patterns.

### Verification

**Real CMSSW 5-step StepChain test** (wmLHEGS → DRPremix → DRPremix → MiniAODv6 → NanoAODv15):
```
Request:  cmsunified_task_B2G-RunIII2024Summer24wmLHEGS-00642__v1_T_251007_083425_3739
Events:   50/job × 2 jobs = 100 total
Result:   PASS — 842s (14.0 min)
CPU:      Peak 1672% (16.7 cores), Expected 1600% (2×8)
Output:   12 .root files (6 per proc), 411 MB total
Merged:   4 files (2 MINIAODSIM1 + 2 NANOEDMAODSIM1), 22.9 MB
```

- All 5 FJR files produced per proc job
- Proc output rename: `proc_0_*.root` and `proc_1_*.root` (no collisions)
- Manifest correctly maps files to tiers from FJR ModuleLabels
- Merge correctly routes files to output datasets by tier
- 208 unit tests pass

### Key Discoveries

- **CMSSW SITECONFIG_PATH**: CMSSW 14.x uses `std::getenv("SITECONFIG_PATH")` in
  `SiteLocalConfigService.cc`, NOT `CMS_PATH`. Confirmed from CMSSW source code.
- **ExternalLHEProducer nEvents**: Must match `maxEvents`, otherwise the gridpack generates
  excess LHE events and CMSSW throws `EventGenerationFailure` at end-of-run.
- **StepChain output modules**: Multi-step PSets use numbered suffixes like `MINIAODSIM1output`
  (not just `MINIAODSIMoutput`). The `1` suffix is from CMSSW's StepChain output numbering.
- **FJR output ordering**: LHE output may be listed before GEN-SIM output in the FJR. Must
  filter by ModuleLabel to get the correct input for the next step.

## What's Next

- Mark existing and future tests with appropriate level markers as they're written
- Add checks for new requirements as they're discovered during debugging
- Multi-merge-group DAG — Submit workflow with multiple work units
- Output registration — Wire merge outputs to Output Manager
- Site Manager — CRIC sync for site status and capacity

---

# Phase 11 — LFN-Based Output Paths: Unmerged Staging, Merged Output, Real Cleanup

**Date**: 2026-02-19
**Spec Version**: 2.4.0
**Phase**: 11 — Correct Output Data Flow

---

## Problem

Output paths were broken in multiple ways:

1. **Proc outputs stayed in HTCondor sandbox** — temporary storage, lost after job exit. No staging to persistent site storage.
2. **`output_base_dir` semantic mismatch** — Config was `/mnt/shared/store` with `local_output_path()` stripping `/store/` prefix. This caused double `/store/` in paths (e.g., `/mnt/shared/store/store/mc/...`).
3. **No `UnmergedLFNBase`** — Never read from request, nowhere in the system. Proc outputs should go to `/store/unmerged/...` before merging.
4. **Merge script read from wrong place** — Read proc outputs from group directory via glob. Should read from unmerged site storage where proc jobs stage their outputs.
5. **No real cleanup** — Cleanup job was a no-op (`/bin/true` or generated script that did nothing). Unmerged files were never removed after merge.
6. **`lstrip("/")` path bug** — `lfn_base.lstrip("/")` in the merge script could eat extra characters, and the overall path construction was fragile.

### Correct Data Flow (WMAgent convention)

```
Proc job → stages to unmerged:  PFN = /mnt/shared + /store/unmerged/Era/Primary/TIER/Proc-v1/000000/file.root
Merge job → reads unmerged, writes merged: PFN = /mnt/shared + /store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root
Cleanup job → removes unmerged directory
```

LFN→PFN mapping: `PFN = local_pfn_prefix + LFN` (simple concatenation).

## What Was Changed

### Config (`config.py`)

Renamed `output_base_dir: str = "/mnt/shared/store"` → `local_pfn_prefix: str = "/mnt/shared"`.

The prefix is now a pure LFN→PFN mapping site prefix, not a partial path with `/store` baked in.

### LFN Helpers (`core/output_lfn.py`)

- **`lfn_to_pfn(local_pfn_prefix, lfn)`** — New canonical path function: `os.path.join(prefix, lfn.lstrip("/"))`. Replaces the old `local_output_path()` which stripped `/store/` prefix.
- **`local_output_path()`** — Kept as backward-compatible alias for `lfn_to_pfn()`.
- **`derive_merged_lfn_bases()`** — Now reads `UnmergedLFNBase` from request (default `/store/unmerged`) and produces `unmerged_lfn_base` per dataset alongside the existing `merged_lfn_base`.
- **`unmerged_lfn_for_group()`** — New function, parallel to `merged_lfn_for_group()`, builds unmerged LFN paths per merge group.

### CLI (`cli.py`)

- Threads `unmerged_lfn_base` from request's `UnmergedLFNBase` field into workflow `config_data`.
- Updated `_print_output_files()` to use `local_pfn_prefix` instead of `output_base_dir`.

### Output Manager (`core/output_manager.py`)

- Updated imports: `local_output_path` → `lfn_to_pfn`.
- Updated all references: `self.settings.output_base_dir` → `self.settings.local_pfn_prefix`.

### DAG Planner (`core/dag_planner.py`) — Major Changes

**`output_info.json` format change**:
- Renamed `output_base_dir` → `local_pfn_prefix`
- Added `unmerged_lfn_base` per dataset entry
- Backward compat: merge script reads either `local_pfn_prefix` or `output_base_dir`

**Proc jobs**:
- `output_info.json` added to `transfer_input_files` list
- `--output-info output_info.json` added to proc job arguments
- Proc wrapper parses `--output-info` argument

**Proc wrapper — new `stage_out_to_unmerged()` function**:
- After CMSSW/synthetic execution and output renaming, reads `output_info.json`
- For each output file, looks up the unmerged LFN base by tier
- Copies file to unmerged PFN: `local_pfn_prefix + unmerged_lfn_base/{group_index:06d}/{filename}`
- Also stages the output manifest (`proc_N_outputs.json`) for merge to read
- Graceful no-op if no `output_info.json` (backward compat)
- Called from both `run_cmssw_mode()` and `run_synthetic_mode()`

**Merge script — reads from unmerged site storage**:
- New `lfn_to_pfn()` helper inline (same logic as `output_lfn.py`)
- Reads proc ROOT files and output manifests from unmerged PFN paths instead of group directory glob
- Falls back to group directory glob if no `unmerged_lfn_base` (backward compat with old format)
- Writes merged output to merged PFN paths via `lfn_to_pfn()`
- Writes `cleanup_manifest.json` listing unmerged directories for cleanup
- Text file merge (`merge_text_files()`) updated to use `lfn_to_pfn()` for output paths

**Cleanup script — new `_write_cleanup_script()` generating `wms2_cleanup.py`**:
- Reads `cleanup_manifest.json` from group directory (written by merge job)
- Removes all listed unmerged directories via `shutil.rmtree()`
- Graceful no-op if no manifest found
- In test mode (`/bin/true`), cleanup now uses generated script (same pattern as proc/merge)

**Cleanup submit file**:
- Now receives `--output-info` argument to find the group directory
- Uses generated `wms2_cleanup.py` instead of `/bin/true` in test mode

### Tests

**`test_output_lfn.py`** — 18 tests (was 11):
- New `TestLfnToPfn` class: store_mc, store_unmerged, no_double_slash, trailing_slash, backward_compat_alias
- New `TestUnmergedLfnForGroup` class: directory_only, with_filename, large_group_index
- `TestDeriveMergedLfnBases`: added `test_unmerged_lfn_base_derived`, `test_default_unmerged_lfn_base`

**`test_dag_generator.py`** — 37 tests (was 28):
- `test_output_info_json_format`: updated to assert `local_pfn_prefix` and `unmerged_lfn_base`, assert no `output_base_dir`
- New: `test_proc_transfer_includes_output_info`, `test_proc_args_include_output_info`
- New: `test_proc_script_has_stage_out`, `test_proc_script_output_info_arg`
- New: `test_merge_script_uses_lfn_to_pfn`, `test_merge_script_reads_from_unmerged`, `test_merge_script_writes_cleanup_manifest`
- New: `TestCleanupWrapper` class: `test_cleanup_script_generated`, `test_cleanup_script_reads_manifest`, `test_cleanup_submit_has_output_info`
- Updated: `test_test_mode_uses_wrapper_scripts` — cleanup now also uses generated script
- Updated: `test_wrapper_scripts_generated` — includes `wms2_cleanup.py`

**`test_output_manager.py`** — Updated for renamed config field (`local_pfn_prefix`) and new PFN path structure.

**Total: 272 tests passing** (excluding 2 pre-existing condor_submit failures unrelated to this change).

---

## Verification Steps

1. `source .venv/bin/activate`
2. `pytest tests/unit/test_output_lfn.py -v` — 18 tests pass
3. `pytest tests/unit/test_dag_generator.py -v` — 37 tests pass
4. `pytest tests/ -v --ignore=tests/integration/test_condor_submit.py` — 272 tests pass

## Design Decisions

- **`local_pfn_prefix` not `output_base_dir`**: The old name implied a directory containing outputs. The new name makes clear it's a site-specific prefix for LFN→PFN mapping. Default `/mnt/shared` means PFN = `/mnt/shared/store/mc/...`.
- **Stage-out in proc wrapper, not HTCondor transfer**: HTCondor's `transfer_output_files` moves files to the group directory (DAGMan's working directory). Stage-out to site storage must happen *inside* the proc job, before HTCondor cleans up the sandbox.
- **Cleanup manifest written by merge, read by cleanup**: The merge job knows which unmerged directories it read from. Writing a `cleanup_manifest.json` decouples cleanup from knowing the full LFN structure — it just removes what merge tells it to.
- **Backward compatibility**: Both merge script and proc wrapper handle missing `unmerged_lfn_base` gracefully (fall back to group directory reads). The merge script reads either `local_pfn_prefix` or `output_base_dir` from `output_info.json`.
- **`lfn_to_pfn()` inline in merge script**: The merge script runs standalone on worker nodes without access to the `wms2` package. The 2-line helper is duplicated rather than imported.

---

## Phase 12 — cmsRun Merge Fix & NANOAODSIM Conversion (2026-02-19)

**Date**: 2026-02-19
**Spec Version**: 2.4.0
**Phase**: 12 — Working cmsRun Merge

---

### Problem

The Phase 11 E2E test completed the full pipeline (8 proc jobs → merge → cleanup) but with two critical defects:

1. **Files not actually merged.** cmsRun merge failed with `LogicalFileNameNotFound` (missing `file:` prefix on local paths), hadd fallback failed with `libtbb.so.12: cannot open shared object` (ran bare binary outside CMSSW env on el9), so the merge job fell back to copying 8 individual files. A work unit should produce **one merged file per tier**.

2. **NANOEDMAODSIM instead of NANOAODSIM.** Proc steps produce EDM-format nano (`NANOEDMAODSIM`). The merge step converts to flat ROOT ntuples (`NANOAODSIM`) via `mergeNANO=True` + `NanoAODOutputModule`. Since merge fell back to copy, no conversion happened.

### Root Causes

1. **`write_merge_pset()`** wrote bare paths like `'/mnt/shared/store/.../file.root'`. CMSSW's PoolSource requires `'file:/mnt/shared/store/.../file.root'` for local PFN.

2. **`merge_root_with_hadd()`** found hadd binary path via `which hadd` inside CMSSW env, then ran the bare binary in a separate subprocess without CMSSW runtime libraries. On el9 host with el8 CMSSW, the binary needs the container and `LD_LIBRARY_PATH` from `scramv1 runtime -sh`.

3. **`run_cmsrun()`** was missing `$SITE_CFG` in apptainer bind paths, missing `CMS_PATH` export (needed by CMSSW <14.x), and missing `SITECONF/local/` directory layout.

4. **Merged output filename** used the input tier (`NANOEDMAODSIM`) rather than the converted tier (`NANOAODSIM`).

### Changes (`src/wms2/core/dag_planner.py`)

#### 1. `write_merge_pset()` — add `file:` prefix
Input file paths and output file path now get `file:` prefix so CMSSW PoolSource treats them as local PFN, not LFN. Idempotent — skips if prefix already present.

#### 2. `merge_root_with_hadd()` — run inside CMSSW env
Replaced two-step approach (find hadd path, run bare binary) with a single bash command that sources `scramv1 runtime -sh` before running hadd. Uses apptainer via `resolve_container()` when cross-OS (el8 CMSSW on el9 host). Added `shlex.quote()` for safe shell quoting of file paths.

#### 3. `run_cmsrun()` — site config fixes
- **Bind `$SITE_CFG`** into apptainer: `bind_paths += "," + site_cfg`
- **Export `CMS_PATH`** alongside `SITECONFIG_PATH` in runner script for CMSSW <14.x compat
- **Create `SITECONF/local/` layout**: Copies `site-local-config.xml` and `PhEDEx/` into `_siteconf/SITECONF/local/` (the directory structure `CMS_PATH` expects)

#### 4. `merge_root_tier()` — NANOEDMAODSIM → NANOAODSIM naming
When the input tier contains "EDM" and "NANO" (i.e., `NANOEDMAODSIM`), the merged output filename uses the converted tier (`NANOAODSIM`). The `mergeNANO=True` flag in the PSet produces flat ROOT ntuples via `NanoAODOutputModule`, so the filename should reflect the actual output format.

### Tests (`tests/unit/test_dag_generator.py`)

3 new tests (40 total, was 37):

- **`test_merge_pset_adds_file_prefix`** — Verifies `file:` prefix logic in generated `write_merge_pset()`
- **`test_hadd_runs_inside_cmssw_env`** — Verifies hadd uses `scramv1 runtime` and apptainer support
- **`test_cmsrun_merge_binds_site_cfg`** — Verifies site_cfg binding, `CMS_PATH` export, and `SITECONF/local/` layout

### E2E Verification

**Real CMSSW 5-step NPS StepChain** (GEN-SIM → DRPremix × 2 → MiniAOD → NanoAOD):
```
Request:  cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54
Events:   50/job × 8 jobs = 400 total (8 cores each, 64 cores total)
Result:   PASS — 1093s (18.2 min)
```

**Before (Phase 11):**
- cmsRun merge: FAILED (LogicalFileNameNotFound)
- hadd fallback: FAILED (libtbb.so.12 not found)
- Result: 8 individual copied files per tier (no merge)

**After (Phase 12):**
- cmsRun merge: SUCCESS
- MINIAODSIM: 8 files → **1 merged file** (`merged_MINIAODSIM.root`, 27.5 MB)
- NANOAODSIM: 8 files → **1 merged file** (`merged_NANOAODSIM.root`, 3.0 MB, flat ntuple format)
- Unmerged directories: cleaned up
- No rescue DAGs, no hadd fallback needed

**Output paths:**
```
/mnt/shared/store/mc/Run3Summer22EEMiniAODv4/.../MINIAODSIM/.../000000/merged_MINIAODSIM.root
/mnt/shared/store/mc/Run3Summer22EENanoAODv12/.../NANOAODSIM/.../000000/merged_NANOAODSIM.root
```

### Key Discoveries

- **CMSSW `file:` prefix**: CMSSW PoolSource interprets bare paths as LFNs (logical file names resolved via catalog). Local files must have `file:` prefix to be treated as PFNs. This is the same issue proc jobs had (already documented in MEMORY.md) — now also fixed for merge.
- **hadd shared library dependency**: hadd from el8 CMSSW requires `libtbb.so.12` and other libraries from the CMSSW runtime. Running the binary outside `scramv1 runtime -sh` or outside the apptainer container fails on el9 hosts.
- **CMS_PATH vs SITECONFIG_PATH**: CMSSW >=14.x uses `SITECONFIG_PATH` (flat layout: `$SITECONFIG_PATH/JobConfig/site-local-config.xml`). CMSSW <14.x uses `CMS_PATH` (nested layout: `$CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml`). Both must be set for compatibility.

### Dev VM Setup Script (`scripts/setup-dev-vm.sh`)

Added a provisioning script for fresh AlmaLinux 9 VMs. Addresses the recurring friction of running as root — HTCondor rejects root job submissions, file ownership conflicts require constant `chown`, and postgres config edits break ownership.

The script creates an `agent` user that owns the repo, submits to HTCondor, and runs all dev tasks. 10 idempotent steps:

1. Create `agent` user
2. Sudoers — limited `systemctl`, `dnf`, `chown`, `condor_reconfig`, `psql`
3. System packages — python3.12, git, postgresql-server, condor, jq
4. PostgreSQL — init, scram-sha-256 auth, wms2 role + databases + pgcrypto
5. HTCondor — dev config with `ALLOW_ADMINISTRATOR = *` (fixes condor_reconfig denied)
6. CMS siteconf — `/opt/cms/siteconf/` with site-local-config, storage.json, PhEDEx
7. Directory ownership — `/mnt/shared/work`, `/mnt/shared/store`, repo dir
8. Python venv — `.venv/` with all deps + htcondor pip bindings
9. Shell profile — `.bashrc` auto-cd, activate venv, set `SITECONFIG_PATH`/`CMS_PATH`/`X509_*`
10. Verification — smoke-test PostgreSQL, HTCondor, Python, CVMFS

Usage: `sudo bash scripts/setup-dev-vm.sh`, then `su - agent` for all work.

---

## Phase 14 — Per-Step Resource Utilization Metrics

**Date**: 2026-02-20

### What Was Built

Per-step resource metrics collection pipeline: proc jobs extract metrics from CMSSW FrameworkJobReport (FJR) XML files, stage them alongside outputs, and merge jobs aggregate them into per-work-unit summaries. This enables resource right-sizing (memory, CPU threads) and throughput calibration for future DAG planning.

### Changes

#### 1. Proc wrapper — FJR metrics extraction (`dag_planner.py`)

Added embedded Python in `_write_proc_script()` (after output manifest, before output size collection) that parses each `report_stepN.xml` and writes `proc_N_metrics.json`:

**Metrics extracted per step (12 fields):**
- `wall_time_sec` — `Timing/TotalJobTime`
- `cpu_time_sec` — `Timing/TotalJobCPU`
- `num_threads` — `Timing/NumberOfThreads`
- `cpu_efficiency` — computed: `cpu_time / (wall_time × num_threads)`
- `peak_rss_mb` — `ApplicationMemory/PeakValueRss`
- `peak_vsize_mb` — `ApplicationMemory/PeakValueVsize`
- `events_processed` — from `InputFile/EventsRead` (sum), GEN fallback via `ProcessingSummary/NumberEvents`
- `throughput_ev_s` — `Timing/EventThroughput`
- `avg_event_time_sec` — `Timing/AvgEventTime`
- `time_per_event_sec` — computed: `wall_time / events`
- `read_mb`, `write_mb` — `StorageStatistics/Timing-file-{read,write}-totalMegabytes`

Guarded with `2>/dev/null || true` — metrics extraction failure does not affect job success.

#### 2. Proc wrapper — metrics staging (`dag_planner.py`)

In `stage_out_to_unmerged()`, added block that copies `proc_N_metrics.json` to the unmerged directory alongside the output manifest. Only stages if the file exists (no-op for synthetic mode or FJR parse failures).

#### 3. Merge script — metrics aggregation (`dag_planner.py`)

In `_write_merge_script()`:
- **Collection**: Added `unmerged_metrics` list, collected from unmerged dirs (and group dir fallback) using `proc_*_metrics.json` glob pattern
- **Aggregation**: After cleanup manifest, reads all per-proc metrics, groups by step number, computes per-step min/max/mean (or min/max/total for event counts), writes `work_unit_metrics.json` to the group directory

#### 4. E2E test — structured metrics display (`e2e_real_condor.py`)

Replaced `[6/7] Per-step timing` with `[6/7] Per-step resource utilization` that reads `work_unit_metrics.json` and displays a formatted table with Wall(s), CPU Eff, RSS(MB), Events, Tput(ev/s) including min-max ranges. Falls back to old text-parsing approach when no structured metrics file exists.

#### 5. Unit tests (`test_dag_generator.py`)

Added `TestMetrics` class with 4 tests:
- `test_proc_script_has_metrics_extraction` — FJR parsing present with all key metric names
- `test_proc_script_stages_metrics_file` — metrics staging in stage-out function
- `test_merge_script_aggregates_metrics` — collection and aggregation references
- `test_merge_script_metrics_has_aggregates` — min/max/mean structure in aggregation output

### Verification

- `pytest tests/unit/test_dag_generator.py -v` — 44/44 pass (40 existing + 4 new)
- `pytest tests/unit/ -v` — 233/233 pass

### Design Decisions

- **Metrics are best-effort**: Extraction uses `2>/dev/null || true`, staging is conditional on file existence, aggregation skips unreadable files. A metrics failure never blocks job completion.
- **Aggregation in merge script, not WMS2 core**: Keeps WMS2's "no per-job tracking" invariant — the merge job (which already reads per-proc manifests) does the aggregation as a side effect. WMS2 can later read `work_unit_metrics.json` for resource calibration without needing per-job data.
- **Events use total, not mean**: `events_processed` aggregation reports min/max/total (not mean) since total events per step across all proc jobs is more useful for throughput analysis.

---

## Phase 14 — Test Matrix Framework

**Date**: 2026-02-20
**Commit**: `fc69159`

### What Was Built

A declarative test matrix module (`tests/matrix/`, 11 files, ~1200 lines) inspired by CMSSW's `runTheMatrix.py`. Provides numbered workflow definitions, predefined sets, environment auto-detection, fault injection, a generic runner reusing the real DAGPlanner + HTCondor, and per-step performance reporting with statistical analysis.

#### Module Structure

| File | Purpose |
|---|---|
| `definitions.py` | Core dataclasses: `WorkflowDef`, `FaultSpec`, `VerifySpec` (all frozen) |
| `derive.py` | `derive()` helper for creating workflow variants |
| `environment.py` | Capability detection wrapping `tests/environment/checks.py` |
| `catalog.py` | 6 workflows: 100.x synthetic, 300.x real CMSSW, 500.x fault injection |
| `sets.py` | Predefined sets: smoke, integration, full, faults, synthetic |
| `sweeper.py` | Pre/post cleanup of work dirs and LFN output paths |
| `faults.py` | Post-submission fault injection via `.sub` file patching |
| `runner.py` | `MatrixRunner` execution engine, perf collection, result persistence |
| `reporter.py` | Pass/fail table + per-step performance report |
| `__main__.py` | CLI entry point with `--list`, `--sets`, `--report`, `--results` |
| `__init__.py` | Package init |

#### Workflow Catalog

| ID | Mode | Title |
|---|---|---|
| 100.0 | synthetic | Synthetic 1-job smoke test |
| 100.1 | synthetic | Synthetic 2-job parallel |
| 300.0 | cached | NPS 5-step StepChain, 1 work unit (8 jobs × 50 ev) |
| 500.0 | synthetic | Fault: proc exit 1 (expect retry + rescue DAG) |
| 501.0 | synthetic | Fault: proc exit 2 (UNLESS-EXIT, no retry) |
| 510.0 | synthetic | Fault: merge exit 1 (expect rescue DAG) |

#### Performance Collection — 3-Tier Strategy

1. **`proc_N_metrics.json`** — Structured per-job per-step data written by `wms2_proc.sh` FJR parser. Each proc job writes a uniquely-named file (no overwrites). Contains `step_index`, `wall_time_sec`, `cpu_efficiency`, `peak_rss_mb`, `events_processed`, `throughput_ev_s`.

2. **Proc stdout FJR metric lines** — Pattern: `Step N: wall=Xs cpu_eff=Y rss=ZMB events=N`. Available from all proc jobs (each has its own stdout file). Parsed when no metrics JSON files exist.

3. **FJR XML + stdout wall times** — Fallback: parse `report_step*.xml` (only last job's survive due to filename collisions) for CPU eff/RSS, combine with wall times from all proc stdout (`Step X completed in Ns` pattern).

All strategies compute mean ± stddev across jobs using sample standard deviation.

#### Time Breakdown from DAGMan Log

Parses `group.dag.dagman.out` for `ULOG_EXECUTE`/`ULOG_JOB_TERMINATED` timestamps per node type to produce:
- Processing time (first proc start → last proc end)
- Merge time (merge start → merge end)
- Cleanup time
- Overhead (scheduling gaps between phases)

#### Result Persistence

- Results saved as JSON to `/mnt/shared/work/wms2_matrix/results/` after each run
- `WorkflowResult`, `PerfData`, `StepPerf` all have `to_dict()`/`from_dict()` serialization
- Raw per-job per-step data and CPU samples preserved for re-analysis
- `--report` re-renders from saved data without re-running
- `--results` lists saved result files

#### Report Output

```
Performance: 300.0 — NPS 5-step StepChain, 1 work unit (8 jobs x 50 ev)
  Jobs: 8    Wall: 1230s
  CPU:  avg 2825% (28.2 cores)  peak 3050% (30.5 cores)  expected 6400%  efficiency 44%

  Step        Wall(s)         CPU Eff           RSS(MB)   Events   Tput(ev/s)    n
  Step 1  326 ±2 [323-328]   66.1% ±1.1    1870 ±9/1886     400            -    8
  Step 2  318 ±21 [294-349]  51.1% ±8.8  7280 ±133/7478     400            -    8
  Step 3  174 ±9 [162-190]   53.0% ±3.7   3901 ±72/4017     400            -    8
  Step 4  67 ±3 [64-71]      22.5% ±1.0   2336 ±15/2356     400            -    8
  Step 5  44 ±2 [42-47]      14.8% ±0.2   1706 ±11/1719     400            -    8
  Total                929s/job

  Overall:
    CMSSW threading: 52.9% (4.2 / 8 cores used by cmsRun)
    System-level:    44.1% (28.2 / 64 allocated cores over 1230s)
    Peak RSS:        7478 MB

  Time breakdown:
    Processing: 1012s
    Merge:      89s
    Overhead:   43s (scheduling, transfer, DAGMan gaps)

  Output: 2 merged files, 29.1 MB  (unmerged cleaned)
```

### dag_planner.py Fixes

- **`proc_\${NODE_INDEX}_metrics.json` → `proc_${NODE_INDEX}_metrics.json`**: The backslash-escaped `\$` prevented bash variable expansion inside the `python3 -c "..."` block. All proc jobs wrote to the same literal filename instead of `proc_0_metrics.json`, `proc_1_metrics.json`, etc.
- **Added `step_index` field** to metrics JSON entries (was only `step` with 1-based numbering).
- **Events counting**: Changed FJR parser to prefer `ProcessingSummary/NumberEvents` over summing `InputFile/EventsRead`. The latter overcounts for DRPremix steps where pileup InputFile entries inflate the count (e.g., 100 instead of 50 for a 50-event job).

### DAGMan Verification Fix

DAGMan always exits with `job_status=4` ("completed") regardless of whether nodes failed — it writes rescue DAGs instead. Fixed `_verify()` to check `actually_succeeded = dag_completed AND rescue_dags == 0`. Rescue DAG search covers both `workflow.dag.rescue*` and `group.dag.rescue*`.

### Verification

- `python -m tests.matrix --list` — Lists all 6 workflows with environment status
- `python -m tests.matrix -l smoke` — 4/4 passed (100.0, 500.0, 501.0, 510.0)
- `python -m tests.matrix -l 300.0` — Passed (8 jobs × 50 events × 5 steps, ~20 min)
- `python -m tests.matrix --report` — Re-renders from saved JSON without re-running

### Design Decisions

- **Declarative over imperative**: Workflow definitions are frozen dataclasses, not procedural test functions. Adding a new test = adding a `WorkflowDef` to the catalog. The runner is generic.
- **Same code path as production**: Uses `DAGPlanner.plan_production_dag()` and `HTCondorAdapter` directly, not test-specific submission logic. Only mocks are the repository (no DB needed) and DBS/Rucio adapters.
- **Results persisted, not ephemeral**: Every run saves full results (including raw per-job data) to JSON. Iterating on the report format never requires re-running workflows.
- **Three efficiency metrics, not one**: CMSSW threading (FJR-based, per-step), system-level CPU (ground truth from `/proc/stat`), and time breakdown (DAGMan log timestamps). Each measures something different; they are not combined into a misleading single number.
- **Fault injection via .sub patching**: Rather than building fault modes into the wrapper script, faults are injected by rewriting `.sub` files after submission. DAGMan re-reads `.sub` at node start time, so patches take effect on the next execution of that node.

## What's Next

- Multi-merge-group DAG — Submit workflow with multiple work units
- Output registration — Wire merge outputs to Output Manager (DBS/Rucio)
- Site Manager — CRIC sync for site status and capacity
- AODSIM tier handling — proc stages AODSIM to unmerged but no dataset config for it; decide whether to merge or discard
- Resource calibration — Use `work_unit_metrics.json` to feed back into DAG planning (memory/CPU right-sizing)

---

## Phase 15 — Spec Update: Adaptive Execution Model (2026-02-20)

**Spec Version**: 2.4.0 → 2.5.0

### What Changed

Replaced the dedicated pilot job execution model with an adaptive execution model. The key insight: eliminate the separate pilot entirely — let the first production jobs serve as calibration, and use the existing recovery/rescue DAG mechanism as the natural re-optimization point.

### Spec Changes Summary

| Area | Change |
|---|---|
| **RequestStatus enum** | Removed `PILOT_RUNNING` |
| **WorkflowStatus enum** | Removed `PILOT_RUNNING` |
| **Workflow model** | Removed `pilot_cluster_id`, `pilot_schedd`, `pilot_output_path`, `pilot_metrics`; removed `PilotMetrics` and `StepProfile` classes; added `step_metrics: Optional[Dict[str, Any]]` |
| **Database schema** | Replaced 4 pilot columns with single `step_metrics JSONB` column |
| **Component Overview (4.1)** | DAG Planner description updated: request hints or step_metrics instead of pilot |
| **Lifecycle Manager (4.2)** | Removed `PILOT_RUNNING` from timeouts, dispatch, stuck handler; simplified `_handle_queued` to go directly to DAG planning; removed `_handle_pilot_running`; added step_metrics aggregation to `_prepare_recovery` |
| **DAG Planner (4.5)** | Removed pilot phase methods (`submit_pilot`, `handle_pilot_completion`, `_parse_pilot_report`); updated `plan_and_submit` to use step_metrics or request hints; added `step_profile.json` writing for sandbox |
| **Section 5** | Full rewrite: "Pilot Execution Model" → "Adaptive Execution Model" with 6 subsections covering execution rounds, per-step splitting, memory calibration, metric aggregation, and convergence |
| **Section 6.2.1** | Updated partial production flow: rescue DAGs carry step_metrics instead of "skipping pilot" |
| **Section 7 API** | Replaced `pilot_metrics` with `step_metrics` in workflow status response |
| **Section 10** | Phase 2 title/content updated; Phase 4 deliverables updated |
| **Section 13** | "Pilot Accuracy" → "Adaptive Convergence" |
| **Section 14** | "Pilot not representative" → "First-round estimates inaccurate" |
| **Section 15** | Updated OQ-2; added OQ-16 (default threads), OQ-17 (median vs p90), OQ-18 (min sample size) |
| **Section 16** | Added DD-12: Adaptive execution instead of dedicated pilot |
| **Appendix A** | Updated pilot row to adaptive execution |

### Design Rationale

A dedicated pilot phase adds ~8 hours of latency before the first production job runs, and measurements may not represent the full dataset. The adaptive model:

1. Starts producing real results immediately (no latency)
2. Converges to optimal parameters by round 2 (most gains realized)
3. Uses production data that is inherently representative
4. Simplifies the state machine (no `PILOT_RUNNING` status, no pilot tracking fields)
5. Reuses existing recovery/rescue DAG mechanism — no new components

Partial production steps are natural re-optimization points: after the first 10% completes, the rescue DAG for the remaining 90% carries accumulated metrics.

### Verification

- `grep -i pilot docs/spec.md` — remaining references are only in DD-12 (rejected alternative context), migration phase naming ("Pilot" = system pilot testing), and Section 5.1 (explaining what was eliminated)
- All `PILOT_RUNNING` enum values removed from both `RequestStatus` and `WorkflowStatus`
- All pilot tracking fields removed from Workflow model and database schema
- `step_metrics` field consistently referenced across model, schema, lifecycle manager, DAG planner, API, and Section 5

---

## Phase 16 — Parallel Step 0 Execution (Adaptive) (2026-02-20)

### What Was Built

Implemented parallel cmsRun execution for step 0 (GEN steps) in the adaptive execution model. When replan analysis shows step 0 has low CPU efficiency (e.g., single-threaded LHE generators using 2 of 8 cores), the system now forks multiple parallel cmsRun instances to fill all allocated cores. Each instance processes a subset of events via firstEvent partitioning.

**Key constraint**: Only step 0 is eligible for splitting. Steps 1+ run sequentially with the full original nThreads — reducing their threads would just leave cores idle with nothing to fill them.

### Files Changed

| File | Change |
|---|---|
| `tests/matrix/adaptive.py` | New file: adaptive replan logic — `analyze_wu_metrics()`, `compute_per_step_nthreads()` (with `request_cpus` and `request_memory_mb` for memory-capped n_parallel), `patch_wu_manifests()`, CLI entry point |
| `src/wms2/core/dag_planner.py` | Added `parse_fjr_output()`, `inject_pset_parallel()`, `run_step0_parallel()` bash functions; step loop branch for parallel step 0; comma-separated PREV_OUTPUT handling for multi-file input to step 1; metrics glob updated for instance FJRs |
| `tests/matrix/runner.py` | Added adaptive execution mode (`_execute_adaptive()`): two-round DAG with replan node between work units; passes `--request-cpus` and `--request-memory` to replan |
| `tests/matrix/reporter.py` | Added adaptive comparison report (`_print_adaptive_comparison()`): per-step tuning table with n_par column, round-over-round performance comparison |
| `tests/matrix/catalog.py` | Added adaptive workflow definitions (350.x series) and `WorkflowDef.memory_mb` field |
| `tests/matrix/definitions.py` | Added `memory_mb` field to `WorkflowDef` dataclass |
| `tests/matrix/sets.py` | Added `adaptive` test set |

### Architecture

#### Execution Flow (Round 2, after replan)
```
Step 0: fork N instances (firstEvent partitioning) → N output ROOT files
Step 1: sequential cmsRun, reads all N files as input, original nThreads
Step 2+: sequential, reads previous step output, original nThreads
Final: rename, metrics extraction, stage out
```

#### Instance Subdirectories
Each parallel instance runs in its own subdir. CMSSW project and sandbox content are symlinked; only the PSet is copied (needs per-instance modification for firstEvent/maxEvents/nThreads):
```
WORK_DIR/step1_inst0/  step1_inst1/  step1_inst2/  step1_inst3/
```

#### Memory-Capped n_parallel
```
n_parallel = min(request_cpus // tuned_nthreads,
                 request_memory_mb // (step0_avg_rss + 512 MB headroom))
```

### Design Decisions

- **Only step 0 eligible for splitting**: GEN steps are the primary use case (single-threaded LHE generators). Splitting subsequent steps would require cross-step output chaining between variable-parallel instances — dramatically more complex for marginal benefit.
- **nThreads unchanged for steps 1+**: Reducing threads for sequential steps just leaves cores idle. The freed cores have nothing to fill them since only one cmsRun runs at a time for steps 1+.
- **n_parallel=1 fallback**: If memory cap or CPU computation yields n_parallel=1, step 0 keeps original nThreads and runs the existing sequential code path. No behavioral change for non-adaptive workflows.
- **Power-of-2 thread counts**: tuned nThreads uses nearest power of 2 (geometric midpoint rounding) to match CMSSW's preferred thread counts.

### Verification

- Test 350.1 (10 ev/job, 8 cores): replan correctly computes n_parallel=4, step 0 tuned to 2 threads, steps 2-5 keep 8 threads. All rounds pass.
- Test 350.0 (50 ev/job, 8 cores): replan computes n_parallel=2 (higher CPU eff = 65.6% → 4 effective cores → 4 tuned threads). All rounds pass.
- Test 300.0 (non-adaptive): unaffected, existing code path unchanged.
- Performance note: test workflows show limited speedup because CMSSW setup overhead (~120s for gridpack extraction) dominates event processing time. Real production workflows with longer GEN steps will benefit more.

---

## Phase 17 — Optional CPU Overcommit in Adaptive Execution (2026-02-20)

### What Was Built

Optional CPU overcommit for the adaptive execution model. When `overcommit_max > 1.0`, each cmsRun step can receive more threads than its proportional core share, filling I/O bubbles with extra runnable threads. The feature is off by default (`overcommit_max=1.0`) and memory-safe.

### How It Works

**Step 0 overcommit**: After computing `tuned` nThreads and `n_par` for parallel splitting, if overcommit is enabled, each instance gets up to `tuned * overcommit_max` threads. The number of parallel instances stays unchanged — overcommit adds threads per instance, not more instances. Memory check: `n_par * (avg_rss + extra_threads * 250MB) <= request_memory_mb`.

**Steps 1+ overcommit**: Sequential steps with moderate CPU efficiency (50-90%) get up to `original_nthreads * overcommit_max` threads. Low efficiency steps (<50%) are better served by splitting (step 0) or are left as-is (steps 1+ can't be split). High efficiency steps (>=90%) are already saturating their threads.

**Memory safety**: Per-thread overhead is conservatively estimated at 250 MB (typical for CMSSW). If the projected RSS exceeds `request_memory_mb`, the system backs off to the maximum safe thread count.

### Files Changed

| File | Change |
|---|---|
| `tests/matrix/definitions.py` | Added `overcommit_max: float = 1.0` and `adaptive_split: bool = True` to `WorkflowDef` |
| `tests/matrix/adaptive.py` | Added `overcommit_max` and `split` params to `compute_per_step_nthreads()`, step 0 and steps 1+ overcommit logic, `overcommit_applied`/`projected_rss_mb` tracking fields, `--overcommit-max` and `--no-split` CLI args, `[OC]` flag in replan output |
| `tests/matrix/runner.py` | Passes `--overcommit-max` and `--no-split` to replan args |
| `tests/matrix/catalog.py` | Added `_WF_360_0` (overcommit only), `_WF_370_0` (split + overcommit) |
| `tests/matrix/sets.py` | Added 360.0 and 370.0 to `_INTEGRATION_IDS` |
| `tests/matrix/reporter.py` | Shows overcommit summary line when `overcommit_max > 1.0` |
| `docs/spec.md` | Added CPU overcommit paragraph to Section 5.3 |

### Test Workflows

Split and overcommit are independent controls, tested in isolation:

| ID | Split | Overcommit | What it tests |
|---|---|---|---|
| 350.0 | yes | no (1.0) | Step 0 parallel splitting only |
| 360.0 | no | yes (1.25) | CPU overcommit only, all steps n_par=1 |
| 370.0 | yes | yes (1.25) | Both combined |

### Design Decisions

- **Off by default**: `overcommit_max=1.0` means no overcommit. Existing workflows are unaffected. Only workflows explicitly configured with `overcommit_max > 1.0` get the feature.
- **Independent controls**: `adaptive_split` and `overcommit_max` are orthogonal. When `adaptive_split=False`, step 0 is treated like steps 1+ for overcommit (moderate efficiency band gets extra threads, but no parallel instances). This allows testing each optimization's contribution independently.
- **Conservative per-thread overhead**: 250 MB/thread is a safe default for CMSSW. Real overhead varies by step type but this ensures no OOM.
- **Moderate efficiency band (50-90%)**: Steps below 50% have fundamental parallelism issues that extra threads won't fix. Steps above 90% are already efficient — marginal benefit from overcommit.
- **request_cpus unchanged**: Overcommit operates entirely within the sandbox. The scheduler sees the same resource request. This avoids slot fragmentation.
- **n_par unchanged for step 0**: Overcommit adds threads per instance, not more instances. Adding instances would compound memory pressure.

### Verification

- Workflow 360.0 (overcommit only, prior to split isolation): passed in 2556s. Overcommit applied to 3/5 steps (step 0 at 65% eff: 8→5T×2par with OC, steps 1-2 at 50-54% eff: 8→10T). RSS increased as expected (step 2: 7333→8330 MB). No OOM, no rescue DAGs.
- Throughput impact: overcommit did not improve wall time for this NPS workload — steps are memory-bandwidth bound, not I/O-bubble bound. Step 2 was 15% slower with 10 threads vs 8. This confirms overcommit is workload-dependent and validates the need for per-workflow configurability.
- Unit tests confirm all three configurations produce expected tuning:
  - 350.0: step 0 gets 4T×2par, steps 1+ keep 8T
  - 360.0: all steps n_par=1, steps in 50-90% band get 10T [OC]
  - 370.0: step 0 gets 5T×2par [OC], steps 1+ in band get 10T [OC]

---

# Phase 10 — All-Step Pipeline Split (380.0)

**Date**: 2026-02-21
**Spec Version**: 2.4.0

---

## What Was Built

### All-Step Pipeline Split Algorithm (`tests/matrix/adaptive.py`)

New function `compute_all_step_split()` that forks N complete StepChain pipelines within one sandbox, each processing `events/N` events through all steps with per-step optimized nThreads. This is a separate code path from the existing step 0 split (`compute_per_step_nthreads`).

Algorithm:
1. For each step: compute `ideal_threads = nearest_power_of_2(eff_cores)`, capped to original nThreads
2. Iterate `n_pipelines` from highest feasible down to 1:
   - `threads_cap = request_cpus // n_pipelines`
   - Per-step: `tuned = min(ideal_threads, threads_cap)`
   - Per-step: `projected_rss = measured_rss - (original - tuned) * 250 MB`, floor 500 MB
   - Memory check: `n_pipelines * max(projected_rss across steps) <= request_memory_mb`
   - Take first (highest) n_pipelines that fits memory
3. Returns `{n_pipelines, per_step: {tuned_nthreads, projected_rss_mb, ...}}`

### Pipeline Mode Proc Script (`src/wms2/core/dag_planner.py`)

New bash function `run_all_steps_pipeline()` in the proc script, dispatched from `run_cmssw_mode()` when `manifest.json` contains `n_pipelines > 1`.

Pipeline execution pattern:
- CMSSW project setup happens once (shared across pipelines)
- N subshells fork, each running the complete step chain (steps 0 through N-1)
- Each pipeline gets a non-overlapping firstEvent range (same pattern as `run_step0_parallel`)
- Steps 1+ within each pipeline consume only that pipeline's previous step output
- After all pipelines complete: FJR files are copied as `report_stepN_pipeM.xml`, ROOT files as `pipeM_filename.root`
- Both pipeline and sequential paths converge at the existing output rename, manifest, and metrics extraction code

### Extended Metrics and Output Manifest

- Metrics extraction now looks for `report_step*_pipe*.xml` alongside existing `_inst*` pattern
- Output manifest handles `pipe*_` prefix stripping when mapping renamed files to tiers

### Files Changed

| File | Changes |
|---|---|
| `tests/matrix/definitions.py` | Added `split_all_steps: bool = False` field (supersedes `adaptive_split` when True) |
| `tests/matrix/adaptive.py` | New `compute_all_step_split()`, `--split-all-steps` CLI flag, `patch_wu_manifests()` accepts `n_pipelines`, writes `n_pipelines` to manifest and `replan_decisions.json`, `[PIPE]` flag in replan output |
| `src/wms2/core/dag_planner.py` | New `run_all_steps_pipeline()` bash function, pipeline dispatch in `run_cmssw_mode()`, extended metrics extraction for pipeline FJRs, output manifest handles `pipe*_` prefix |
| `tests/matrix/catalog.py` | Added `_WF_380_0` (adaptive all-step pipeline split) |
| `tests/matrix/sets.py` | Added 380.0 to `_INTEGRATION_IDS` |
| `tests/matrix/runner.py` | Passes `--split-all-steps` to replan args when `wf.split_all_steps` is True |
| `tests/matrix/reporter.py` | Shows `Pipeline split: N pipelines` when `n_pipelines > 1` in adaptive data |

### Test Workflow

| ID | Split | Pipeline | Overcommit | What it tests |
|---|---|---|---|---|
| 380.0 | no | yes | no (1.0) | All-step pipeline split — N pipelines each running full StepChain |

### Design Decisions

- **Separate function, not a mode flag on `compute_per_step_nthreads`**: Pipeline split is a fundamentally different execution model (all steps in parallel vs just step 0). Keeping it as a separate function (`compute_all_step_split`) makes each algorithm self-contained and independently testable.
- **`n_pipelines` in manifest**: The proc script reads `n_pipelines` from `manifest.json` to decide the execution path. Per-step `n_parallel` stays 1 because parallelism is at the pipeline level, not the step level.
- **Memory model**: Uses the same conservative 250 MB/thread overhead as the existing split. Projects RSS down when reducing threads, with a 500 MB floor. Memory check uses `max(projected_rss across all steps)` since all steps of one pipeline run sequentially but all pipelines run simultaneously.
- **Event numbering uniqueness**: Each pipeline's step 0 gets a non-overlapping firstEvent range. Steps 1+ within each pipeline consume only that pipeline's output, so event numbers propagate naturally with no cross-pipeline contamination.
- **`adaptive_split=False` for 380.0**: Pipeline mode supersedes step 0 splitting. The two are not combined — pipeline mode already optimizes all steps including step 0.

### Bug Fix During Verification

Initial run failed: all pipelines crashed immediately on CMSSW NANO process validation. Root cause: all step PSets shared the same basename (`cfg.py`) in different subdirectories (`steps/step1/cfg.py`, `steps/step2/cfg.py`, etc.). The pipeline code copied them flat to the pipeline directory, causing each to overwrite the previous — the last one (NANO) won, so ALL steps tried to run the NANO config.

Fix: preserve directory structure when copying PSets into pipeline dirs (`mkdir -p "$pdir/$pset_dir"` + copy with full relative path).

### Verification

Workflow 380.0 passed in 2821s. Verification results:

1. **replan_decisions.json**: `n_pipelines: 2`, per-step tuned threads [4, 4, 4, 2, 1]
2. **replan.out**: `[PIPE]` flags on all 5 steps, `mode: all-step pipeline split`
3. **Round 2 proc output**: "Pipeline mode: 2 pipelines", both pipelines completed all 5 steps
4. **Per-step efficiency comparison**:

| Step | R1 Wall | R2 Wall | R1 CpuEff | R2 CpuEff | R1 RSS | R2 RSS |
|---|---|---|---|---|---|---|
| 1 (GEN) | 334s | 371s | 64.6% | 69.7% | 1870 MB | 1488 MB |
| 2 (DR) | 383s | 462s | 58.3% | 69.0% | 7218 MB | 4886 MB |
| 3 (DRpremix) | 187s | 272s | 55.7% | 52.8% | 3843 MB | 3325 MB |
| 4 (MINI) | 67s | 78s | 25.2% | 62.8% | 2326 MB | 2078 MB |
| 5 (NANO) | 48s | 46s | 15.2% | 94.7% | 1696 MB | 1539 MB |

5. **DAG completed**, all outputs merged and cleaned, no OOM, no rescue DAGs
6. **16 FJR samples in Round 2** (8 jobs × 2 pipelines), confirming all pipelines ran

### Analysis

CPU efficiency improved for 4 of 5 steps — especially step 4 (MINI: 25% → 63%) and step 5 (NANO: 15% → 95%) where fewer threads closely match actual parallelism. Per-pipeline wall time increased ~20% (expected from shared memory bandwidth contention), but 2 simultaneous pipelines yield ~1.6× total throughput over the single-pipeline baseline. Memory stayed well within the 16 GB limit (peak 5005 MB per pipeline for step 2, vs projected 6218 MB).

---

## 2026-02-22 — DY2L Filtered Workflow (301.0) and X509 Proxy Fix

### What Was Built

#### New Test Workflow: 301.0 (DY2L with Jet Matching Filter)

Added a second real CMSSW workflow to the test matrix: DYto2L-4Jets (Drell-Yan to 2 leptons + up to 4 jets) with MadGraph MLM jet matching. This workflow differs from NPS in two important ways:

1. **ExternalLHEProducer with gridpack**: Step 1 runs MadGraph event generation via a gridpack, which is **single-threaded**. With 8 allocated cores, only 1 is active during the ~50 min MadGraph phase (12.5% utilization).

2. **~10.7% filter efficiency**: The Pythia8ConcurrentHadronizerFilter with jet matching rejects ~90% of generated events. 500 LHE events per job → ~54 surviving to RAWSIM output → steps 2-5 process only 54 events each.

Source request: `cmsunified_task_GEN-Run3Summer22EEwmLHEGS-00600__v1_T_250902_211552_8573` (5-step StepChain, Run3Summer22EE campaign, same pileup as NPS).

**Files:**
- `tests/matrix/catalog.py` — Added `_WF_301_0` and `_GEN_DY2L_OUTPUT_DATASETS`
- `tests/matrix/sets.py` — Added 301.0 to integration set
- Sandbox: `/mnt/shared/work/wms2_real_condor_test/sandbox_gen_dy2l.tar.gz`

#### X509 Proxy Permission Fix in Proc Script

xrootd 5.4.x (used by CMSSW_12_4_16 in DY2L step 2) rejects X509 proxy files with permissions more permissive than 600. The proxy at `/mnt/creds/x509up` has 644 permissions (not changeable). NPS uses CMSSW_12_4_25 with xrootd 5.7.2, which doesn't enforce this check.

**Fix**: All three step runner templates in `dag_planner.py` now copy the proxy to the working directory with 600 permissions before running cmsRun:

```bash
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
```

This works for any xrootd version and any original proxy permissions/location, since the working directory is always bind-mounted into the apptainer container.

### Verification

301.0 passed in 5121s (85 min):

| Step | Wall(s) | ev/s | CPU | RSS(MB) | Events | n |
|---|---|---|---|---|---|---|
| Step 1 (GEN+SIM) | 4044 | 0.12 | 30% | 1750 | 4000 | 8 |
| Step 2 (DRPremix) | 461 | 0.12 | 62% | 7617 | 429 | 8 |
| Step 3 (RECO) | 301 | 0.18 | 60% | 4029 | 429 | 8 |
| Step 4 (MiniAOD) | 107 | 0.50 | 28% | 2466 | 429 | 8 |
| Step 5 (NanoAOD) | 47 | 1.14 | 17% | 1735 | 429 | 8 |

- Overall: 34% CPU efficiency, 45 ev/core-hour, 59.5 GB machine peak RSS
- Filter: 4000 generated → 429 output (10.7% efficiency, matching request's 10.2%)
- Output: 2 merged files, 56.2 MB

### Analysis

Step 1 dominates wall time (81.5%) at only 30% CPU efficiency. The 30% is a weighted average: MadGraph runs single-threaded (~50 min at 12.5%) followed by multi-threaded Pythia+SIM (~17 min at ~80%). This workflow is the ideal candidate for step 0 splitting — parallelizing MadGraph across all allocated cores would reduce step 1 wall time by up to 8×.

### Known Issues

- The `events_per_job` in the report header counts generated (input) events, not filtered (output) events. For filtered workflows this is misleading — 301.0 reports "4000 events" but only 429 survive to output.

## 2026-02-22 — Spec v2.6.0: Processing Blocks and Output Manager Redesign

### Context

The Output Manager design needed to address a fundamental tape archival problem: CMS workflows produce thousands of 2-4GB merged files over weeks or months of production. Tape drives write 18TB tapes — if files trickle in individually, they scatter across tape cartridges causing severe fragmentation and slow recall. Tape buffers compensate for 1-2 day transfer delays, not months-long production.

### What Changed (spec v2.5.0 → v2.6.0)

#### New Concept: Processing Block

A **processing block** is a group of work units that forms the atomic unit of DBS block registration and tape archival. Requests are split into processing blocks at DAG planning time. Each block maps 1:1 to a DBS block and a tape archival rule.

- Work unit completes → immediate DBS file registration + Rucio source protection rule (urgent, protects local storage)
- Processing block completes (all work units done) → DBS block closure + single tape archival rule for the entire block
- After creating rules, WMS2 is done — Rucio owns the outcome (fire-and-forget)

#### Replaced OutputDataset with ProcessingBlock

- **Old**: `OutputDataset` model with 10-state `OutputStatus` enum tracking registration, announcement, invalidation stages
- **New**: `ProcessingBlock` model with 5-state `BlockStatus` enum (OPEN → COMPLETE → ARCHIVED, plus FAILED and INVALIDATED)
- SQL table `output_datasets` → `processing_blocks` with fields for DBS state, Rucio rule tracking, and retry metadata

#### Rewrote Output Manager (Section 4.7)

Simplified from complex multi-stage pipeline to focused responsibilities:

1. `handle_work_unit_completion()` — DBS file registration + source protection (per work unit)
2. `_complete_block()` — DBS block closure + tape archival rule creation (per block)
3. `process_blocks_for_workflow()` — retry loop for previously failed Rucio calls
4. `invalidate_blocks()` — catastrophic recovery cleanup (delete rules, invalidate in DBS)

#### Retry with Backoff

Rucio call failures use exponential backoff: 1m → 5m → 30m → 2h → 4h → 8h, bounded by configurable `max_rucio_retry_duration` (default 3 days). On exhaustion, logs a full diagnostic dump and marks block as FAILED.

#### Other Spec Changes

- Section 2.3: Added processing block definition
- Section 4.2: Updated Lifecycle Manager `_handle_active` to use new Output Manager methods
- Section 4.6: Updated DAG Monitor merge manifest to include `block_id` and `output_files`
- Section 6.2.1: Updated output continuity note for clean stop
- Section 6.3: Simplified catastrophic recovery invalidation table (3 states instead of 8)
- Section 7: Updated API example response to show blocks
- Section 10.4: Updated Phase 4 deliverables
- Section 14: Replaced Output Manager risks with processing block sizing risk
- Section 15: Added OQ-19 (block sizing), OQ-20 (block completion priority), OQ-21 (retry max duration)
- Section 16: Replaced DD-4 (immediate output processing), DD-5 (processing blocks), DD-9 (fire-and-forget Rucio)
- Appendix A: Updated WMCore component mapping

### Design Decisions

1. **Fire-and-forget Rucio interaction**: WMS2 creates rules and does not track their progress. Rucio owns transfer execution, retry, and completion. WMS2's only follow-up is retrying rule creation on temporary failures.

2. **Immediate source protection**: Source protection rules are created per work unit immediately on completion, not batched with block completion. Output files sit on the producing site's local storage which could be reclaimed.

3. **Processing blocks as DBS/tape unit**: Grouping work units into blocks ensures contiguous tape writes. A single tape rule per block (containing all the block's files) lets Rucio write them together on tape, avoiding fragmentation.

### Rucio Installation

Also set up a local Rucio 39.3.0 instance for future integration testing:
- Separate venv at `/mnt/shared/rucio/venv/`
- PostgreSQL database `rucio` on existing pg16 instance
- 3 POSIX RSEs: T2_LOCAL_A, T2_LOCAL_B, T2_LOCAL_C
- Userpass auth (ddmlab/secret), no X.509 needed
- Server management: `/mnt/shared/rucio/rucio-server.sh {start|stop|status}`
- No automatic transfers (no FTS) — manual replica placement + `rucio-judge-evaluator --run-once` for rule evaluation

---

## Output Manager Rewrite — ProcessingBlock Model

**Date**: 2026-02-22
**Spec Version**: 2.6.0

### What Changed

Replaced the old 10-state `OutputDataset` lifecycle with a 5-state `ProcessingBlock` model per spec v2.6.0. The old design had WMS2 tracking transfers, polling Rucio status, managing source protection lifecycle, and announcing datasets — duplicating Rucio's job. The new design uses fire-and-forget Rucio interaction.

### Files Modified (17 total)

**Models & Enums (3 files)**
- `src/wms2/models/enums.py` — `OutputStatus` (10 states) → `BlockStatus` (5: OPEN, COMPLETE, ARCHIVED, FAILED, INVALIDATED)
- `src/wms2/models/processing_block.py` — New model: block_index, total_work_units, completed_work_units, dbs_block_name/open/closed, source_rule_ids, tape_rule_id, rucio retry fields
- `src/wms2/models/output_dataset.py` — Deleted
- `src/wms2/models/__init__.py` — Updated exports

**Database (4 files)**
- `src/wms2/db/tables.py` — `OutputDatasetRow` → `ProcessingBlockRow`
- `src/wms2/db/repository.py` — 3 old methods → 5 new methods (added `get_processing_block`, `count_processing_blocks_by_status`)
- `src/wms2/db/migrations/versions/001_initial_schema.py` — `output_datasets` table → `processing_blocks` table
- `src/wms2/db/migrations/env.py` — Updated import

**Adapters (3 files)**
- `src/wms2/adapters/base.py` — Added 4 abstract methods: `open_block`, `register_files`, `close_block`, `invalidate_block`
- `src/wms2/adapters/dbs.py` — Real httpx implementations for block operations
- `src/wms2/adapters/mock.py` — Mock implementations for block operations

**Core Logic (3 files)**
- `src/wms2/core/output_manager.py` — Complete rewrite with fire-and-forget Rucio, backoff retry, failure dump
- `src/wms2/core/lifecycle_manager.py` — `_handle_active` uses `handle_work_unit_completion()`, `process_blocks_for_workflow()`, `all_blocks_archived()`
- `src/wms2/core/workflow_manager.py` — `get_output_datasets()` → `get_processing_blocks()`

**Tests & Fixtures (4 files)**
- `tests/unit/test_output_manager.py` — Complete rewrite: 24 tests across 6 classes
- `tests/unit/test_enums.py` — `OutputStatus` → `BlockStatus` assertions
- `tests/unit/test_lifecycle.py` — Updated work unit completion mock expectations
- `tests/unit/conftest.py` — Updated mock repository methods
- `tests/integration/conftest.py` — Updated import

### Key Design Decisions

1. **Fire-and-forget Rucio**: `handle_work_unit_completion()` creates source protection rules immediately. `_complete_block()` creates tape archival rules. Neither polls for completion — Rucio owns the outcome.

2. **Retry with backoff**: Failed Rucio calls are retried on subsequent lifecycle cycles with exponential backoff (1m → 5m → 30m → 2h → 4h → 8h). After 3 days, blocks are marked FAILED with full context dump.

3. **Block-gated completion**: DAG COMPLETED no longer immediately transitions to request COMPLETED. The lifecycle manager checks `all_blocks_archived()` first — if blocks are still COMPLETE (pending tape rule) or OPEN (pending source protection), the request stays ACTIVE and retries on the next cycle.

4. **DBS block lifecycle**: First work unit opens the DBS block. Each work unit registers files. Block completion closes the DBS block. Catastrophic recovery invalidates blocks.

### NOT Changed

Files using `output_datasets`/`OutputDatasets` as a ReqMgr2 field name (list of dataset names from request config): `cli.py`, `output_lfn.py`, `dag_planner.py`, `sandbox.py`, `request.py`, and all matrix test files. These refer to a different concept.

### Verification

- 247/247 unit tests pass
- DB migration runs cleanly: `processing_blocks` table created, no `output_datasets` table
- No stale references to old model names in imports or core code

---

## 2026-02-22 — Fix Memory Model for Adaptive Step 0 Split

### Context

Running 351.0 (DY2L, 8 cores, 16 GB, 50 ev/job) with the old memory model caused all 8 Round 2 proc jobs to be held by HTCondor for exceeding the 16 GB cgroup memory limit (peak 15,511 MB). The old algorithm projected step 0 RSS at 2T as `max(1767 - 6×250, 500) = 500 MB`, leading to `4 × (500 + 1500 tmpfs) = 8000 MB` — appeared safe but actual consumption was ~15.5 GB because:

1. **Per-thread reduction was wrong for GEN**: The 250 MB/thread reduction assumes CMSSW thread-scaling overhead. GEN step memory is dominated by the gridpack/MadGraph runtime, which is thread-independent. Reducing threads doesn't reduce RSS.
2. **Missing sandbox overhead**: CMSSW project area, shared libs, and ROOT framework consume ~3 GB as a constant base, regardless of how many cmsRun instances run.

### What Changed

#### Memory Model (`tests/matrix/adaptive.py`)

Replaced the aggressive per-thread reduction with a conservative model:

```
Total = SANDBOX_OVERHEAD_MB (3000) + n_par × (measured_RSS + TMPFS_PER_INSTANCE_MB)
```

- **SANDBOX_OVERHEAD_MB = 3000**: CMSSW project area, shared libs, ROOT — loaded once regardless of instances
- **measured_RSS**: Used directly, NOT reduced by thread count for GEN step
- **TMPFS_PER_INSTANCE_MB = 1500**: Gridpack extraction on /dev/shm per instance

Even-division preference: the algorithm tries n_par values that evenly divide request_cpus first (no wasted cores), then falls back to non-even values. This avoids configurations like n_par=3 on 8 cores (2 cores wasted).

Ideal memory reporting: `compute_per_step_nthreads()` now returns `ideal_n_parallel` and `ideal_memory_mb` in step 0 data, so the reporter can show what would be optimal without the memory cap.

#### Per-Core Memory Inputs

Replaced `memory_mb` (single total) and `request_memory`/`request_cpus` CLI args with three clean per-core inputs:

- `memory_per_core_mb` — MB per core for Round 1 request_memory (default 2000)
- `max_memory_per_core_mb` — max MB per core for Round 2 request_memory (default 2000)
- CLI: `--ncores`, `--mem-per-core`, `--max-mem-per-core`

Total memory is derived: `round1_memory = mem_per_core × ncores`, `max_memory = max_mem_per_core × ncores`.

#### Manifest Patching (`patch_wu_manifests`)

Simplified from computing `extra_memory = n_par × TMPFS` and adding to base request_memory (which double-counted) to simply setting `request_memory = max_memory_mb` when parallel splitting is active. The algorithm already validated that instances fit within this limit.

Return value changed from `int` (patched count) to `dict` with `patched`, `ideal_memory_mb`, `actual_memory_mb`.

#### Proc Script (`src/wms2/core/dag_planner.py`)

- Added `/dev/shm` to apptainer bind mounts
- Parallel instance working directories now use tmpfs (`/dev/shm/wms2_step0_$$`) instead of the virtiofs-backed work directory, avoiding file descriptor limits during gridpack extraction (29K+ files each)
- CVMFS autofs mount trigger before mode dispatch (`ls /cvmfs/cms.cern.ch/`)
- Mode dispatch is now explicit (`cmssw` / `synthetic` / error) instead of falling back silently

#### Runner (`tests/matrix/runner.py`)

- Pre-flight disk space check (`_check_disk_space`) verifying EXECUTE, output, and /tmp
- Computes Round 1 and max Round 2 memory from per-core fields
- Always keeps artifacts (`keep_artifacts=True`) for both pass and fail

#### Reporter (`tests/matrix/reporter.py`)

- Shows memory limits: `Memory: 2000 MB/core (R1), max 2000 MB/core (R2)`
- Shows ideal vs actual when constrained: `Memory needed: 16290 MB (2036 MB/core) — capped at 16000 MB`
- Shows step 0 split info: `Step 0 split: 2 instances (ideal: 4, memory-constrained)`

#### Catalog (`tests/matrix/catalog.py`)

- Added 351.0 (DY2L, 50 ev/job) as fast iteration test; renumbered old 351.0 to 351.1 (500 ev/job)
- All 8 adaptive entries converted from `memory_mb=16000` to `memory_per_core_mb=2000, max_memory_per_core_mb=2000`

### Files Changed (7 files, 334 insertions, 67 deletions)

| File | Changes |
|---|---|
| `tests/matrix/adaptive.py` | New memory model with SANDBOX_OVERHEAD, even-division preference, ideal memory tracking, per-core CLI args, `patch_wu_manifests` returns dict with memory info |
| `tests/matrix/definitions.py` | Added `memory_per_core_mb`, `max_memory_per_core_mb` fields |
| `tests/matrix/catalog.py` | Added 351.0 (50 ev), renumbered old to 351.1, all entries use per-core fields |
| `tests/matrix/runner.py` | Disk space checks, per-core memory computation, always keep artifacts |
| `tests/matrix/reporter.py` | Memory limits display, ideal vs actual, step 0 split info |
| `tests/matrix/sets.py` | Added 351.1 to integration set |
| `src/wms2/core/dag_planner.py` | tmpfs for parallel instances, /dev/shm bind, CVMFS trigger, explicit mode dispatch |

### Verification

351.0 passed in 3256s. Replan decisions: step 0 → n_parallel=2 at 4T (ideal was 4, needs 16290 MB > 16000 MB limit).

| | Round 1 | Round 2 | Change |
|---|---|---|---|
| Events | 400 | 400 | |
| Processing wall | 1620s | 1262s | |
| Throughput (ev/s) | 0.25 | 0.32 | **+28.4%** |
| Ev/core-hour | 14 | 18 | **+28.4%** |

Per-step comparison:

| Step | R1 nT | R2 nT | R1 Wall | R2 Wall | R1 CPU | R2 CPU | R1 RSS | R2 RSS |
|---|---|---|---|---|---|---|---|---|
| 1 (GEN) | 8 | 4 | 1116s | 777s | 18% | 18% | 1891 MB | 1547 MB |
| 2 (SIM/DIGI) | 8 | 8 | 210s | 205s | 25% | 18% | 7125 MB | 6192 MB |
| 3 (RECO) | 8 | 8 | 149s | 149s | 23% | 24% | 3838 MB | 3681 MB |
| 4 (MINI) | 8 | 8 | 95s | 80s | 15% | 17% | 2427 MB | 2413 MB |
| 5 (NANO) | 8 | 8 | 50s | 52s | 14% | 14% | 1703 MB | 1701 MB |

- **Memory stayed within limits**: 6192 MB peak RSS, well under 16 GB cgroup limit. Zero held jobs.
- **Step 0 (GEN) wall time**: 1116s → 777s (30% faster from 2 parallel instances at 4T)
- **Overall +28.4% throughput**: GEN dominates wall time (69% of R1), so halving it with 2 instances gives the biggest single improvement
- **CPU efficiency flat** (19% → 18%): expected since total core-hours are the same, just overlapping idle cores during GEN
- Output: 4 merged files (MINIAODSIM + NANOAODSIM × 2 work units), 23.4 MB total

---

## Phase: NanoAOD Merge Fix & split_tmpfs Switch

**Date**: 2026-02-23
**Commit**: `0f043b1`

### Problem: Broken NanoAOD Output

The merged NanoAOD from 351.0 contained only HLT trigger branches (616 of 624 total branches). Physics objects (Muon, Electron, Jet, MET, Photon) were completely absent.

### Root Cause

Two tier-matching bugs in `dag_planner.py`:

**1. Staging code (embedded Python in bash `python3 -c "..."`):**
The staging code maps per-job output files to unmerged store paths by matching file tiers (from FJR) to dataset tiers (from output_info.json). The NANOEDMAODSIM→NANOAODSIM EDM variant match used `"EDM"` in a `replace()` call, but inside a bash double-quoted heredoc, the quotes were interpreted by bash, producing `NameError: name 'EDM' is not defined`. All proc jobs failed at staging, never reaching the merge step.

**2. Merge code (standalone Python `wms2_merge.py`):**
The merge config selected input files by matching dataset tier to discovered file tiers. Loose bidirectional substring match (`ds_tier in tier or tier in ds_tier`) caused `"AODSIM" in "NANOAODSIM"` → True, picking step 3 AODSIM files instead of step 5 NANOEDMAODSIM files. The merge ran `mergeNANO=True` on AODSIM files, producing a flat NanoAOD tree with only trigger info surviving the format conversion.

### Fix

1. **Staging code**: Escaped quotes `\"EDM\"` for bash context
2. **Merge code**: Two-phase matching:
   - First try EDM variant: `tier.replace("EDM","") == ds_tier` (catches NANOEDMAODSIM→NANOAODSIM)
   - Fallback: substring match with length-difference guard (≤3 chars), preventing AODSIM→NANOAODSIM (diff=4)

### split_tmpfs Switch

Made tmpfs usage for parallel split instances configurable:
- New field `split_tmpfs: bool = False` in `WorkflowDef`
- Passed through runner → replan CLI → manifest → proc script
- `run_step0_parallel()` now takes `use_tmpfs` parameter; uses `/dev/shm` when true, disk when false
- Enabled for 351.0 and 351.1 (gridpack-based GEN benefits from RAM-disk I/O)
- Off by default for other workflows

### Verification

Test 300.0 (NPS 5-step StepChain, 8 jobs × 50 events):

| Metric | Before Fix | After Fix |
|---|---|---|
| Status | failed (staging NameError) | **passed** |
| Total branches | 624 | **1675** |
| HLT branches | 616 | 616 |
| Muon branches | 0 | **60** |
| Electron branches | 0 | **55** |
| Jet branches | 0 | **46** |
| MET branches | 0 | **12** |
| Photon branches | 0 | **47** |
| Events | — | 400 |
| Merged output | — | 2 files, 30.4 MB |

Performance (300.0, single round, no adaptation):
- 0.37 ev/s, 21 ev/core-hour
- 1078s processing, 55s merge, 1270s total
- Peak RSS: 7348 MB (step 2 DRPremix)

---

## Adaptive Job Split (391.0/391.1)

**Date**: 2026-02-23

### Motivation

351.0/351.1 results showed step 0 (GEN) is the bottleneck: 86% of wall time, only 27% CPU efficiency at 8 threads (effective 2.2 cores). The previous adaptive model (350.x/351.x) splits step 0 into parallel instances *within* each job, but has two drawbacks:
1. **Tail effect**: Slower instance dictates wall time, idle cores on fast instance
2. **Resource lock-in**: All 8 cores stay reserved even when only 2 are needed

### Design: Job-level splitting

Instead of splitting step 0 inside the sandbox, split the jobs themselves — run N× more jobs with N× fewer cores each. Same total core allocation, but jobs finish independently and release resources faster.

Example: 8 jobs × 8 cores → 32 jobs × 2 cores, each processing 12 events instead of 50.

### Algorithm: `compute_job_split()`

Based on Round 1 step 0 metrics:
1. `eff_cores = cpu_eff_step0 × original_nthreads` (e.g., 0.27 × 8 = 2.18)
2. `tuned_threads = nearest_power_of_2(eff_cores)` → 2, capped to [1, original]
3. `job_multiplier = original_nthreads // tuned_threads` → 4
4. All steps run with `tuned_threads` (request_cpus = tuned_threads)
5. `new_events_per_job = original_events_per_job // job_multiplier`
6. `new_num_jobs = original_num_jobs × job_multiplier`
7. `new_request_memory = tuned_threads × max_memory_per_core_mb`

### Key implementation: `rewrite_wu_for_job_split()`

The replan node rewrites WU1's DAG files between work units:
- Parses existing proc_*.sub as template
- Generates new proc submit files with tuned resources
- Rewrites group.dag with new proc node entries + dependencies
- Writes manifest_tuned.json with all steps at tuned_threads

This works because DAGMan reads SUBDAG EXTERNAL files when the node is ready to execute, not at outer DAG submission.

### Tmpfs for non-parallel step 0

Added tmpfs support to the regular step 0 path in dag_planner.py (not just the parallel split path). When `SPLIT_TMPFS=true` and `N_PARALLEL<=1`, step 0 runs from /dev/shm to avoid virtiofs overhead for gridpack extraction.

### Files Changed

| File | Change |
|---|---|
| `tests/matrix/definitions.py` | Added `job_split: bool = False` field |
| `tests/matrix/adaptive.py` | New `compute_job_split()`, `rewrite_wu_for_job_split()`, `--job-split` CLI flag |
| `src/wms2/core/dag_planner.py` | Tmpfs support for non-parallel step 0 |
| `tests/matrix/catalog.py` | Added 391.0 and 391.1 workflow definitions |
| `tests/matrix/sets.py` | Added 391.0 to integration set |
| `tests/matrix/runner.py` | Wired `--job-split`, `--events-per-job`, `--num-jobs` flags |
| `tests/matrix/reporter.py` | Job split display in adaptive report (R2 job count, memory) |

### Workflow Definitions

| WF | Title | Events/job | Jobs | Timeout |
|---|---|---|---|---|
| 391.0 | DY2L adaptive job split (50 ev) | 50 | 8 | 7200s |
| 391.1 | DY2L adaptive job split (500 ev) | 500 | 8 | 10800s |

### Expected Behavior (391.0)

- Round 1: 8 jobs × 8 cores × 50 events (same as 351.0)
- Replan: step 0 eff=27% → ideal 2T → multiplier=4 → 32 jobs × 2 cores × 12 events
- Round 2: 32 jobs at 2 cores each, all steps at 2 threads, tmpfs for step 0

### Verification

- `python -m tests.matrix -l 391.0` — lists workflow, shows runnable
- `compute_job_split()` unit test: 27% eff × 8T → 2T, 4× multiplier, 32 jobs × 12 ev
- Full run 391.0: **PASSED** (4436s)

### Bugs Found and Fixed During 391.0 Testing

**Bug 1: tmpfs CWD not propagated to apptainer step runner**
- `run_step_apptainer()` creates `_step_runner.sh` with `cd $WORK_DIR`, but when running
  from tmpfs, WORK_DIR still pointed to the original execute directory
- cmsRun couldn't find `cfg.py` (copied to tmpfs but cmsRun ran from original dir)
- Fix: temporarily override `WORK_DIR` to tmpfs path before calling `run_step_apptainer()`,
  restore after. Also added `/dev/shm` to apptainer bind mounts.

**Bug 2: `--input none` treated as real LFN**
- `rewrite_wu_for_job_split()` wrote `--input none` in proc submit files
- `lfn_to_xrootd("none")` converted to `root://cms-xrd-global.cern.ch/none`
- This got injected as `fileNames` into GEN's `EmptySource` → CMSSW Configuration error
- Fix: use `synthetic://gen/events_{fe}_{le}` instead of `none`

**Bug 3: Memory formula underestimated at low nthreads**
- Formula `tuned_nthreads × max_memory_per_core_mb` → 1 × 2500 = 2500 MB
- But CMSSW base memory (~70%) doesn't scale with threads; step 1 (SIM) needs ~4000+ MB even at 1T
- All 64 single-threaded jobs hit cgroup OOM (2560 MB limit)
- Fix: floor `tuned_nthreads` at 2 (1T too aggressive for memory), and use
  `max(formula, peak_observed_rss × 1.1)` as memory request

### 391.0 Results (8 jobs × 8 cores baseline)

| Metric | Round 1 (8T, 8 jobs) | Round 2 (2T, 32 jobs) | Change |
|--------|---------------------|----------------------|--------|
| Events | 400 | 384 | |
| Processing wall | 1508s | 1764s | +17% |
| CPU efficiency | 17% | 62% | **+45pp** |
| Peak RSS/job | 6491 MB | 3655 MB | -44% |

Round 2 wall time higher because 6 of 32 jobs queued for memory (128 GB VM limit).
In a real cluster, all 32 would run concurrently.

### Catalog Changes: Plumbing Test Optimization

Reduced num_jobs for .0 plumbing tests to allow parallel test execution on the 64-core VM:
- 300.0: 8 → 2 jobs (16 cores, was 64)
- 350.0: 8 → 2 jobs
- 351.0: 8 → 2 jobs
- 391.0: 8 → 2 jobs
- 351.1: 8 → 4 jobs (performance test, moderate reduction)

---

## Probe Split + Events Count Change

**Date**: 2026-02-23

### Problem

351.1 (adaptive step 0 split, 4 jobs × 500 ev) OOM'd in R2 — jobs hit 18.9 GB against 20 GB cgroup limit. The replan node estimated R2 memory theoretically (SANDBOX_OVERHEAD + n_par × (R1_RSS + TMPFS)), but FJR RSS only measures process RSS (~1550 MB), not tmpfs (~3500 MB from gridpack extraction). The actual cgroup footprint was ~5000 MB/instance, not the estimated 3050 MB.

### Solution: Probe Node

Run one R1 job as a "probe" — split it into 2×(N/2)T instances within the same slot, with `max_memory_per_core × ncores` reservation. The probe's HTCondor cgroup peak (Image size) becomes ground truth for R2 memory estimation.

### Key Design Decisions

- **Probe is the last proc node**: Doesn't extend critical path — other jobs run in parallel
- **request_cpus stays at 8**: Same slot size, no extra concurrent jobs
- **Cgroup peak > RSS**: HTCondor's `ImageSize` captures RSS + tmpfs + page cache — what the cgroup actually enforces. FJR's `PeakValueRss` only measures process RSS.
- **10% safety margin on cgroup data** (vs larger margins for theoretical estimates)

### Files Changed

| File | Change |
|---|---|
| `definitions.py` | Added `probe_split: bool` field |
| `catalog.py` | Events 50→40, 500→400; `probe_split=True` on 351.0/351.1 |
| `runner.py` | `_inject_probe_node()`, cgroup CPU monitoring, probe wiring |
| `adaptive.py` | `analyze_probe_metrics()`, cgroup-aware memory model |
| `reporter.py` | Probe data and operator guidance in report |

### Events Change

Changed event counts to avoid splitting edge cases (N must be divisible by n_parallel):
- 50→40: all `.0` cached workflows
- 500→400: all `.1` cached workflows
- Unchanged: 350.1 (10 ev), 300.1 (1000 ev)

### Memory Model (3-tier)

```
1. probe_cgroup (best):  instance_mem = cgroup_per_instance × 1.10
2. probe_rss (fallback): instance_mem = rss + TMPFS_ESTIMATE
3. theoretical:          instance_mem = R1_avg_rss + TMPFS_ESTIMATE
total = SANDBOX_OVERHEAD + n_parallel × instance_mem
```

### Cgroup CPU Monitoring

Replaced system-wide `/proc/stat` CPU tracking with HTCondor cgroup-based monitoring at `/sys/fs/cgroup/system.slice/htcondor/cpu.stat`. Enables meaningful CPU timeline plots even when running concurrent workflows.

### Verification Results

**351.0** (2 jobs × 40 ev): PASSED, 2708s
- Probe cgroup: 9957 MB → 4978 MB/instance
- R2 n_parallel=2 (correctly sized, not 4)
- R2 peak memory: 5492 MB/job (within 20 GB)

**351.1** (4 jobs × 400 ev): PASSED, 7783s
- Probe cgroup: 10308 MB → 5154 MB/instance
- R2 n_parallel=2 (ideal=4 needs 25677 MB, capped at 20000 MB)
- R2 peak memory: 7606 MB/job (within 20 GB)

| Metric | R1 (3 jobs, 1×8T) | R2 (4 jobs, 2×4T) | Change |
|--------|-------------------|-------------------|--------|
| Step 0 wall | 3405s | 2310s | -32% |
| Total wall/job | 3951s | 2828s | -28% |
| Ev/core-hour | 45.6 | 63.6 | **+40%** |
| Peak memory/job | 6795 MB | 7606 MB | +12% |

### Operator Guidance in Report

Report now shows actionable memory info:
```
Memory needed: 25677 MB (3209 MB/core) — capped at 20000 MB
Step 0 split: 2 instances (ideal: 4, memory-constrained)
Probe: 2 instances, 5154 MB/instance cgroup, 1663 MB/instance RSS  [probe_cgroup]
To split 4×: need 3209 MB/core (current max: 2500 MB/core)
```

---

## Spec v2.7.0: Memory-Per-Core Window and Adaptive Sizing

**Date**: 2026-02-23

### Problem

The spec's adaptive memory model (Section 5.4) used a fixed 512 MB headroom buffer above measured peak RSS. This doesn't scale — 512 MB is 25% overhead on a 2 GB job but only 2.5% on a 20 GB job. The spec also had no concept of site-level memory constraints: CMS sites advertise memory per core with a practical minimum, and requesting more narrows the site pool. The adaptive algorithm needed explicit bounds.

### Design Decisions

- **Memory-per-core window**: Two operational parameters — `default_memory_per_core` (baseline, widest site pool) and `max_memory_per_core` (upper bound). Round 1 production jobs use default, probe jobs use max, Round 2+ sizes within the window based on measured data.
- **20% safety margin**: Replaces fixed 512 MB buffer. Percentage-based margin scales naturally with job size. Accounts for memory leak accumulation (probes process fewer events), event-to-event RSS variation, and page cache pressure.
- **Request validation**: Requests where `Memory / Multicore > max_memory_per_core` are rejected — they cannot be satisfied.
- **Memory-constrained splits**: If measured data + 20% exceeds `max_memory_per_core × cores`, reduce parallel instances rather than exceed the ceiling.

### Spec Changes (v2.6.0 → v2.7.0)

| Section | Change |
|---|---|
| 5.2 (new) | Memory-Per-Core Window — defines default/max parameters, request validation |
| 5.3 (was 5.2) | Execution Rounds — probe jobs use max mem/core, production uses default |
| 5.5 (new, replaces 5.4) | Memory Sizing — 20% margin, data source hierarchy (cgroup > FJR RSS), sizing formula with clamp |
| 5.4, 5.6, 5.7 | Renumbered from 5.3, 5.5, 5.6; cross-references updated |
| DD-13 (new) | Rationale for memory-per-core window and percentage-based margin |

### Adaptive Algorithm Alignment

Updated the three adaptive algorithms in `tests/matrix/adaptive.py` to match the spec v2.7.0 memory model.

**Changes:**

| Function | Before | After |
|---|---|---|
| `compute_per_step_nthreads` (probe cgroup) | `× 1.10` | `× (1 + safety_margin)` |
| `compute_per_step_nthreads` (probe RSS) | `× 1.10 + TMPFS` | `× (1 + safety_margin) + TMPFS` |
| `compute_per_step_nthreads` (theoretical) | `avg_rss + TMPFS` (no margin) | `avg_rss × (1 + safety_margin) + TMPFS` |
| `compute_job_split` | `max(tuned × max_per_core, rss × 1.1)` | `clamp(rss × (1 + margin), default × tuned, max × tuned)` |
| `compute_all_step_split` | No margin on projected RSS | `proj_rss × (1 + safety_margin)` |

**New parameter:** `safety_margin` (default 0.20) added to `WorkflowDef`, all three compute functions, and the CLI (`--safety-margin`). Recorded in `replan_decisions.json` for traceability.

---

## Testing Specification

**Date**: 2026-02-24

### What Was Built

Created `docs/testing.md` (597 lines) — a formal testing specification document following the same OpenSpec v1.0 format as the main spec.

### Motivation

The testing infrastructure had grown to ~13K lines across unit tests, integration tests, the matrix runner, cmsRun simulator, adaptive analysis, fault injection, and environment verification — all undocumented. The main spec mentions testing only in passing. Following the project principle that "the spec is the source of truth — always rebuildable," the testing infrastructure needed its own specification.

### Document Structure

| Section | Content |
|---|---|
| §1 Overview | Purpose, relationship to main spec, production code reuse principle |
| §2 Test Levels | Unit (18 files, ~5.3K lines), integration (6 files, ~1.6K lines), matrix (~5.3K lines), environment (7 files, ~400 lines) |
| §3 Test Matrix System | Architecture, execution flow, full 26-workflow catalog table, 7 named sets, CLI reference |
| §4 Execution Modes | Synthetic, simulator (Amdahl's law resource models), real CMSSW |
| §5 Adaptive Execution Testing | Two-round model, 6 adaptive modes, probe split, multi-round convergence, simulator integration |
| §6 Fault Injection | Post-DAG injection mechanism, FaultSpec/VerifySpec data structures, 3 fault workflows |
| §7 Verification Model | DAG status, rescue DAG, output checks, performance metrics, adaptive convergence |
| §8 Unit Test Coverage | Mock strategy (5 fixture types), test file → component map with spec cross-references |
| §9 Integration Test Coverage | 6 integration test files with infrastructure requirements |
| §10 Environment Verification | 4 capability levels (L0–L3), matrix capability mapping |
| §11 Performance Baselines | Main spec §13.1 success criteria mapped to test mechanisms |
| §12 Future Work | CI/CD, load testing, regression detection, simulator profile library |

### Spec Changes

- `docs/spec.md` §10.4: Added one-line reference to `docs/testing.md` after Phase 4 deliverables

### Verification

- All 37 referenced source files confirmed to exist
- All 26 catalog entries match actual `CATALOG` dictionary in `catalog.py`
- Section numbering is sequential (§1–§12 with subsections)
- Formatting follows main spec conventions (tables, inline code, `##`/`###` hierarchy)

**Key fix:** `compute_job_split` previously used `max_memory_per_core` as the per-core formula base. Now uses `memory_per_core` (default) as floor and `max_memory_per_core` as ceiling, matching the spec's clamp semantics.

---

## 2026-02-24 — Cgroup memory monitor for accurate memory estimation

### Problem

FJR `PeakValueRss` only measures the main cmsRun process via `/proc/PID/stat`. For GEN workflows with `ExternalLHEProducer`, the gridpack subprocess memory is invisible to FJR. Verified experimentally with workflow 392.1:

| Metric | FJR | Memory monitor (cgroup) |
|---|---|---|
| Step 0 peak RSS | 1241 MB | 3753 MB (anon) |
| Peak shmem (tmpfs) | not measured | 1494 MB |
| Peak non-reclaimable (anon+shmem) | not measured | 5126 MB |

The adaptive algorithm was using FJR RSS + hardcoded constants (`TMPFS_PER_INSTANCE_MB=2000`, `CGROUP_HEADROOM_MB=1000`), which produced wrong memory estimates and caused systematic OOM when tmpfs was enabled.

### Root cause analysis

CMSSW's `SimpleMemoryCheck` service (`FWCore/Services/plugins/ProcInfoFetcher.cc`) reads `/proc/<pid>/stat` field 24 (rss in pages) for the main cmsRun process only. `ExternalLHEProducer` spawns a child process to run the gridpack — its memory is invisible to this measurement. The cgroup, however, captures all processes.

Controlled 3-round experiment (392.1):
- **R1** (no tmpfs, 4337 MB): OK. Peak anon=3754, shmem=0. Cgroup hit limit but kernel reclaimed file cache.
- **R2** (tmpfs, 6000 MB): OK. Peak anon=3755, shmem=1494. Peak non-reclaimable=5126 MB.
- **R3** (tmpfs, 4337 MB): OOM at ~4285 MB. Non-reclaimable (5126) far exceeds limit (4337).

### Solution

Use the existing memory monitor (already sampling `memory.stat` every 2 seconds) to produce structured cgroup data for the adaptive algorithm.

**Wrapper (`dag_planner.py`)**:
- After killing the memory monitor, compute peak non-reclaimable split by `tmpfs_present` flag using awk
- Write `proc_${NODE_INDEX}_cgroup.json` with: `peak_anon_mb`, `peak_shmem_mb`, `peak_nonreclaim_mb`, `tmpfs_peak_nonreclaim_mb`, `no_tmpfs_peak_anon_mb`
- Stage cgroup file alongside metrics during output transfer

**Adaptive (`adaptive.py`)**:
- New `load_cgroup_metrics()` function reads `proc_*_cgroup.json` files, aggregates peaks across jobs
- `analyze_wu_metrics()` attaches cgroup data to return dict
- `merge_round_metrics()` propagates cgroup from latest round
- `compute_job_split()` and `compute_per_step_nthreads()` gain `cgroup` parameter — when available, use measured `peak_nonreclaim_mb` instead of FJR RSS + hardcoded constants
- Existing FJR-based logic remains as fallback for jobs without cgroup data

### Memory formula

```
binding_constraint = max(
    tmpfs_peak_nonreclaim_mb,    # step 0: anon + shmem (both non-reclaimable)
    no_tmpfs_peak_anon_mb,       # other steps: anon only (file cache reclaimable)
)
request_memory = binding_constraint × (1 + safety_margin)
```

With 392.1 R2 data and 10% margin: `max(5126, 2990) × 1.10 = 5638 MB` — correctly covers the 5126 MB non-reclaimable peak. The old FJR-based estimate would have been ~4633 MB, still causing OOM.

### Verification

- `proc_0_cgroup.json` (R1, no tmpfs): `peak_anon=3754, shmem=0, tmpfs_peak_nr=0`
- `proc_1_cgroup.json` (R2, tmpfs): `peak_anon=3755, shmem=1494, tmpfs_peak_nr=5126, no_tmpfs_peak=2990`
- Adaptive algorithm correctly selects `cgroup_measured` source and computes 5638 MB
- Backward compatible: jobs without `proc_*_cgroup.json` fall through to existing FJR+estimate logic

---

## N-Round Adaptive Execution and Simulator Job-Split Workflow

**Date**: 2026-02-24

### What was built

Generalized the adaptive execution framework from a fixed 2 work units to N work units, with replan nodes between each consecutive pair. Added workflow 155.2 as the simulator equivalent of 392.0 (3-round adaptive job split).

### Runner changes (`tests/matrix/runner.py`)

**N-round adaptive support**:
- `_execute_adaptive()` now generates `replan_0..replan_{N-2}` submit files and DAG nodes dynamically based on `num_work_units`
- Each replan node receives `--prior-wu-dirs` (comma-separated list of all completed WU dirs) and `--replan-index` for multi-round metric averaging
- Probe node injection remains WU0-only; probe exclusion from R1 performance data uses the `probe_node` field from replan decisions

**Per-WU configuration**:
- `split_tmpfs` injection via `manifest_tuned.json` in each merge group, respecting `split_tmpfs_from_wu` threshold
- `memory_mb_per_wu` overrides: per-WU `request_memory` patching in proc submit files
- Multi-WU job distribution: `jobs_per_work_unit = num_jobs // num_work_units` ensures each WU gets its own merge group

**Performance collection**:
- `_collect_adaptive_perf()` reads `replan_*_decisions.json` files (one per replan node) and stores them as `_all_decisions` list
- Probe node excluded from R1 step data to avoid corrupting baseline metrics

### Reporter changes (`tests/matrix/reporter.py`)

**N-round dynamic reporting**:
- `_print_adaptive_report()` discovers rounds dynamically from step name prefixes (`Round 1:`, `Round 2:`, etc.)
- Builds per-round metadata from `_all_decisions` list: job count, request_cpus, job_multiplier, n_pipelines, per-step tuning
- Throughput comparison table shows all rounds side-by-side with pairwise deltas (Δ1→2, Δ2→3, etc.)
- CPU efficiency summary shows all rounds (e.g., `R1 81%  R2 83%  R3 85%  (+4pp)`)

**Per-round peak memory**:
- New summary line: `Peak memory per job: R1 4200 MB (8.2 GB × 2 jobs)  R2 4200 MB (8.2 GB × 2 jobs)  R3 3000 MB (11.7 GB × 4 jobs)`
- Shows per-job RSS and total machine memory footprint per round

### Data model changes (`tests/matrix/definitions.py`)

| Field | Type | Purpose |
|---|---|---|
| `split_tmpfs_from_wu` | `int` | Only inject tmpfs for WU >= this index (default 0) |
| `memory_mb_per_wu` | `dict[int, int]` | Per-WU request_memory overrides |

### New workflows (`tests/matrix/catalog.py`)

| ID | Title | Mode | Key parameters |
|---|---|---|---|
| 155.2 | Simulator adaptive job split 3-round, 3 GB/core | simulator | 2 jobs × 8T, num_work_units=3, job_split, probe_split |
| 392.1 | DY2L fixed 2T memory test (3-round tmpfs) | cached | 3 jobs × 2T, split_tmpfs_from_wu=1, memory_mb_per_wu |

**155.2 design**: Simulator equivalent of 392.0. GEN-SIM serial_fraction=0.30 at 8T gives ~5.9 effective cores, which is above the geometric-mean midpoint (√(4×8)=5.66), so replan_0 does NOT split (stays at 8T). Only replan_1 triggers the job split after averaging R1+R2 metrics (cpu_eff converges → 5.2 eff cores → below 5.66 → rounds to 4T → job_multiplier=2).

### Verification

155.2 run result (312s):

```
Throughput Comparison
                         Round 1     Round 2     Round 3      Δ1→2      Δ2→3
  Events                      20          20          20
  Jobs × Cores              2×8T        2×8T        4×4T
  Ev/core-hour             22500       28125       28125    +25.0%     +0.0%

CPU efficiency per job:  R1 81%  R2 83%  R3 85%  (+4pp)
Peak memory per job: R1 4200 MB (8.2 GB × 2 jobs)  R2 4200 MB (8.2 GB × 2 jobs)  R3 3000 MB (11.7 GB × 4 jobs)
```

- R1→R2: No split (5.9 eff cores > 5.66 midpoint), stays at 2×8T
- R2→R3: Job split triggered (averaged metrics → 5.2 eff cores < 5.66 → 4T → 4 jobs × 4T)
- Per-job memory correctly reduced from 4200→3000 MB after split
- Machine total increased from 8.2→11.7 GB (more jobs, less per job)

---

## Adaptive Execution Algorithm Specification

**Date**: 2026-02-24
**Spec Version**: 2.7.0
**Commit**: ebdf7af

### What Was Built

Created `docs/adaptive.md` (1139 lines) — a standalone algorithm specification documenting the concrete algorithms behind main spec Section 5. This is a companion document to `docs/spec.md`, following the same OpenSpec v1.0 format as `docs/testing.md`.

### Document Structure

| Section | Lines | Content |
|---|---|---|
| 1. Overview | ~25 | Purpose, relationship to main spec, scope |
| 2. Core Concepts | ~70 | Effective cores, geometric mean thread rounding, work unit rounds, three optimization modes |
| 3. Metric Collection | ~90 | `analyze_wu_metrics()`, `load_cgroup_metrics()`, `merge_round_metrics()` with normalization formula |
| 4. Probe Node Design | ~80 | Configuration (2×(N/2)T on last WU0 proc), metrics extraction, marginal memory model |
| 5. Memory Sizing Model | ~90 | 4-level source hierarchies for each mode, constants table |
| 6. Cgroup and Tmpfs | ~35 | Why cgroup matters, tmpfs optimization, tmpfs-aware binding selection |
| 7. Per-Step nThreads Tuning | ~140 | Step 0 splitting, memory-aware instance reduction, CPU overcommit, output format |
| 8. Adaptive Job Split | ~100 | Core algorithm, DAG rewriting, event range redistribution |
| 9. Pipeline Split | ~55 | Algorithm, uniform vs per-step threads |
| 10. Manifest Patching | ~35 | Behavior, sandbox contract |
| 11. Multi-Round Convergence | ~60 | Round progression, orchestration, convergence properties |
| 12. Worked Examples | ~90 | Geometric mean table, 3-round job split trace, memory source fallback scenarios |
| 13. CLI Reference | ~85 | Arguments table, decision JSON schema, DAGMan integration |
| Appendix A | ~15 | Constants reference (consolidated) |
| Appendix B | ~50 | Decision JSON schema (full field listing) |

### Design Decisions

- **Separate document rather than inline in spec.md**: The main spec Section 5 (~155 lines) covers the adaptive model at an architectural level. The algorithm details (1139 lines) would overwhelm the main spec. A companion document keeps the main spec readable while providing the algorithm-level detail needed for reimplementation.
- **Memory source hierarchy tables**: The priority order for per-step tuning differs from job split mode (probe_peak > cgroup_measured > probe_rss > theoretical vs probe_peak > cgroup_measured > probe_rss > prior_rss). Documenting these as explicit tables makes the non-obvious priority differences visible.
- **Worked examples**: The 3-round job split trace (§12.2) demonstrates the critical geometric mean boundary at 5.657 and how multi-round merging can shift the efficiency estimate across that boundary.

### Verification

- All 10 constants verified against `tests/matrix/adaptive.py` values (SANDBOX_OVERHEAD=3000, TMPFS_PER_INSTANCE=1500/2000, CGROUP_HEADROOM=1000, PER_THREAD_OVERHEAD=250, MAX_PARALLEL=4, safety_margin=0.20, marginal floor=500, pipeline RSS floor=500, power-of-2 max=64)
- Memory source hierarchy priority order matches code: `compute_per_step_nthreads()` lines 585–601, `compute_job_split()` lines 818–857
- Spec.md §5.1–§5.5 cross-references verified against actual section locations
- Cross-reference sentence added to `docs/spec.md` Section 5.1

---

## Propagate Spec v2.8.0 Adaptive/Round-Tracking Fields into Code

**Date**: 2026-02-25
**Spec Version**: 2.8.0
**Commit**: 3640b64

### What Was Built

Propagated new fields from spec v2.8.0 (multi-round adaptive lifecycle) into the application code. Renamed the `pilot_metrics` DB/model field to `step_metrics` to match spec terminology, and added round-tracking fields for multi-round execution.

### Changes

| File | Change |
|---|---|
| `src/wms2/models/request.py` | Added `adaptive: bool = True` to `RequestCreate`, `RequestUpdate`, `Request` |
| `src/wms2/models/workflow.py` | Renamed `pilot_metrics` → `step_metrics`, added `current_round`, `next_first_event`, `file_cursor` |
| `src/wms2/db/tables.py` | Added `adaptive` to `RequestRow`, renamed + added fields to `WorkflowRow` |
| `src/wms2/db/migrations/versions/002_adaptive_round_fields.py` | New Alembic migration (add_column, alter_column rename, with downgrade) |
| `src/wms2/core/dag_planner.py` | Renamed `pilot_metrics=` → `step_metrics=` in `update_workflow()` call |
| `src/wms2/api/workflows.py` | Renamed field in serialization, added `current_round`, `next_first_event`, `file_cursor` |
| `tests/unit/test_models.py` | Added 3 tests: adaptive default/override, workflow round-tracking defaults |

### Design Decisions

- **Keep `pilot_cluster_id`, `pilot_schedd`, `pilot_output_path` unchanged**: These track the pilot HTCondor job (cluster ID, schedd, output directory) — a different concern than `step_metrics` (aggregated FJR data). Renaming them would break working code for no spec benefit; the spec doesn't prescribe these implementation details.
- **Rename only `pilot_metrics` → `step_metrics`**: This is the one field the spec explicitly names in the new terminology, and the code already uses it the same way (aggregated metrics from completed work units). The `PilotMetrics` class name stays — it's still a valid description of the data structure.
- **`adaptive` defaults to `True`**: Matches spec intent that multi-round is the default execution mode. Non-adaptive (single-pass) is the exception.
- **`pilot_metrics.json` file name unchanged**: The on-disk JSON file written by pilot_runner.py and transferred by HTCondor keeps its name. Only the DB column and model field were renamed.

### Verification

- Alembic migration 002 applied successfully against local DB
- DB schema verified: `requests.adaptive` (boolean, default true), `workflows.step_metrics` (renamed), `workflows.current_round` (int, default 0), `workflows.next_first_event` (int, default 1), `workflows.file_cursor` (int, default 0)
- 275/276 unit tests pass (1 pre-existing failure in `test_handle_active_processes_work_units`)
- 8/8 integration tests pass
- No stale `pilot_metrics` references in `src/` outside of file-on-disk names (pilot_runner.py, dag_planner HTCondor submit templates) and migration files
