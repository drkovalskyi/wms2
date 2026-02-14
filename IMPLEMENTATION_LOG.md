# WMS2 Phase 1 — Implementation Log

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
