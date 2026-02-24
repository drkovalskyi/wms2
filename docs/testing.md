# WMS2 Testing Specification

**OpenSpec v1.0**

| Field | Value |
|---|---|
| **Spec Version** | 1.0.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-24 |
| **Authors** | CMS Computing |
| **Parent Spec** | docs/spec.md (WMS2 v2.7.0) |

---

## 1. Overview

### 1.1 Purpose

This document specifies the WMS2 test infrastructure, strategy, and verification model. It is the source of truth for how WMS2 is tested — from isolated unit tests through full end-to-end workflow execution on a live HTCondor pool.

### 1.2 Relationship to Main Spec

Testing validates the design described in `docs/spec.md`. The main spec defines *what* WMS2 does; this document defines *how we verify it does so correctly*. Cross-references to main spec sections use the notation `§N` (e.g., `§4.5` for the DAG Planner).

### 1.3 Design Principle: Production Code Reuse

The test matrix calls the same production code paths used by WMS2 itself:

- `wms2.core.sandbox.create_sandbox()` for sandbox building
- `wms2.core.dag_planner.DAGPlanner.plan_production_dag()` for DAG planning and submit file generation
- `wms2.adapters.condor.HTCondorAdapter` for real HTCondor operations
- `wms2.core.dag_planner.PilotMetrics.from_request()` for resource estimates

This ensures tests exercise actual production logic, not test-specific reimplementations. Only external service adapters (DBS, Rucio, ReqMgr2) are mocked at the boundary.

---

## 2. Test Levels

WMS2 uses four test levels, each with different scope, speed, and infrastructure requirements.

| Level | Scope | Speed | Infrastructure | Location |
|---|---|---|---|---|
| Unit | Individual functions/classes | Seconds | Python only | `tests/unit/` (18 files, ~5.3K lines) |
| Integration | Cross-component with real DB/services | Seconds–minutes | PostgreSQL, optionally HTCondor | `tests/integration/` (6 files, ~1.6K lines) |
| Matrix | End-to-end workflows through HTCondor | Minutes | HTCondor local pool | `tests/matrix/` (~5.3K lines) |
| Environment | Capability verification | Seconds | Varies by level | `tests/environment/` (7 files, ~400 lines) |

### 2.1 Unit Tests

Unit tests verify individual components in isolation. External dependencies (HTCondor, PostgreSQL, DBS, Rucio) are replaced with mocks from `tests/unit/conftest.py`. These tests run in seconds with no infrastructure beyond Python.

**What they catch**: Logic errors in state machines, incorrect DAG file generation, broken parsing, invalid data model validation, wrong resource calculations.

**How to run**: `pytest tests/unit/`

### 2.2 Integration Tests

Integration tests verify cross-component interactions with real services. Most require a running PostgreSQL instance; some (`test_condor_submit.py`, `test_resource_utilization.py`) require a running HTCondor pool and are gated by `@pytest.mark.level2`.

**What they catch**: SQL schema mismatches, API endpoint regressions, DAG planner end-to-end failures, repository CRUD bugs.

**How to run**: `pytest tests/integration/`

### 2.3 Matrix Tests

Matrix tests execute complete workflows through the full production pipeline: sandbox creation, DAG planning, HTCondor submission, monitoring, verification, and reporting. This is the primary end-to-end validation mechanism.

**What they catch**: Integration failures between WMS2 and HTCondor DAGMan, sandbox packaging errors, merge/cleanup chain breakage, adaptive algorithm regressions, fault recovery failures.

**How to run**: `python -m tests.matrix -l smoke` (see §3.4 for full CLI usage)

### 2.4 Environment Tests

Environment tests verify that infrastructure prerequisites are available before running higher-level tests. They are organized by capability level and used by both the test suite and `conftest.py` to gate tests.

**What they catch**: Missing services, expired credentials, unavailable CVMFS, broken HTCondor pool.

**How to run**: `pytest tests/environment/`

---

## 3. Test Matrix System

The test matrix is the core end-to-end testing infrastructure. It submits real workflows to a local HTCondor pool and verifies the complete output pipeline.

### 3.1 Architecture

The matrix system consists of these modules:

| Module | Role |
|---|---|
| `runner.py` | Orchestration engine: sandbox creation, DAG planning, HTCondor submission, monitoring, verification, reporting |
| `catalog.py` | Workflow definitions — the single source of numbered `WorkflowDef` entries |
| `definitions.py` | Data structures: `WorkflowDef`, `FaultSpec`, `VerifySpec` |
| `sets.py` | Named workflow groupings (smoke, integration, synthetic, simulator, faults, adaptive, full) |
| `reporter.py` | Pass/fail tables, per-step performance metrics, throughput comparison, CPU timeline plots |
| `sweeper.py` | Pre/post cleanup — removes work directories, LFN output paths, DAGMan jobs |
| `environment.py` | Capability detection — wraps `tests/environment/checks.py` for workflow prerequisite gating |
| `__main__.py` | CLI entry point for `python -m tests.matrix` |

#### Execution Flow

```
┌─────────────────────────────────────────────────────┐
│  MatrixRunner.run_many(workflows)                   │
│                                                     │
│  for each workflow:                                 │
│    1. sweep_pre()         — clean work dir + LFNs   │
│    2. create_sandbox()    — build sandbox tarball    │
│    3. plan_production_dag() — generate .dag + .sub   │
│    4. inject_faults()     — if fault spec present    │
│    5. condor_submit_dag   — submit to HTCondor       │
│    6. poll DAGMan status  — until done or timeout    │
│    7. verify outcomes     — check VerifySpec         │
│    8. collect perf data   — parse FJR/metrics        │
│    9. sweep_post()        — cleanup on success       │
│                                                     │
│  save_results() → print_summary()                   │
└─────────────────────────────────────────────────────┘
```

For adaptive workflows, steps 2–8 repeat per work unit with a replan phase between rounds (see §5).

### 3.2 Workflow Catalog

Workflows are numbered with an ID scheme that encodes the execution mode and purpose:

| Range | Mode | Purpose |
|---|---|---|
| 100.x | Synthetic | Fast baseline — no CMSSW, tests DAG plumbing |
| 150.x | Simulator | Real ROOT/FJR artifacts via cmsRun simulator, no CMSSW |
| 300.x | Real CMSSW | Production-like StepChain execution via cached sandbox |
| 350.x | Adaptive | Adaptive nThreads tuning (per-step split, overcommit, pipeline) |
| 360.x | Adaptive | Overcommit-only mode |
| 370.x | Adaptive | Split + overcommit combined |
| 380.x | Adaptive | All-step pipeline split |
| 391.x–392.x | Adaptive | Job split (more jobs, fewer cores per job in Round 2) |
| 500.x | Fault injection | Process exits, SIGKILL, merge failures |

The variant convention is: `x00.0` = plumbing (1–2 jobs), `x00.1` = performance (4–8 jobs).

#### Full Catalog

| ID | Title | Mode | Jobs | WUs | Cores | Requires | Purpose |
|---|---|---|---|---|---|---|---|
| 100.0 | Synthetic baseline, 1 job | synthetic | 1 | 1 | 1 | condor | DAG plumbing smoke test |
| 100.1 | Synthetic baseline, 8 jobs | synthetic | 8 | 1 | 1 | condor | Multi-job merge chain |
| 150.0 | Simulator single-step, 1 job | simulator | 1 | 1 | 4 | condor | Simulator plumbing |
| 150.1 | Simulator single-step, 4 jobs | simulator | 4 | 1 | 4 | condor | Multi-job simulator |
| 151.0 | Simulator 3-step StepChain, 2 jobs | simulator | 2 | 1 | 8 | condor | Multi-step simulator |
| 152.0 | Simulator high-memory profile | simulator | 2 | 1 | 8 | condor | High-memory resource model |
| 155.0 | Simulator adaptive 2-WU probe split | simulator | 2 | 2 | 8 | condor | Adaptive probe with simulator |
| 155.1 | Simulator adaptive job split | simulator | 2 | 2 | 8 | condor | Job split with simulator |
| 300.0 | NPS 5-step StepChain, 1 WU | cached | 2 | 1 | 8 | condor, cvmfs, siteconf, apptainer | Real CMSSW baseline |
| 300.1 | NPS 5-step StepChain, 16-core | cached | 4 | 1 | 16 | condor, cvmfs, siteconf, apptainer | High-core count |
| 301.0 | DY2L 5-step StepChain, 8 jobs | cached | 8 | 1 | 8 | condor, cvmfs, siteconf, apptainer | Large real CMSSW |
| 350.0 | Adaptive nThreads tuning, 40 ev/job | cached | 2 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Basic adaptive |
| 350.1 | Adaptive nThreads tuning, 10 ev/job | cached | 8 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Fast adaptive iteration |
| 351.0 | DY2L adaptive step 1 split, 2 jobs | cached | 2 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Per-step split with probe |
| 351.1 | DY2L adaptive step 1 split, 4 jobs | cached | 4 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Per-step split at scale |
| 360.0 | Adaptive overcommit only | cached | 8 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Overcommit without split |
| 370.0 | Adaptive split + overcommit (1.25x) | cached | 8 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Combined modes |
| 380.0 | Adaptive all-step pipeline split | cached | 8 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Pipeline parallelism |
| 380.1 | Pipeline split, uniform threads | cached | 8 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Uniform nThreads pipeline |
| 391.0 | DY2L adaptive job split, 2 jobs | cached | 2 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Job split (small) |
| 391.1 | DY2L adaptive job split, 4 jobs | cached | 4 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Job split (medium) |
| 391.2 | DY2L adaptive job split, 3 GB/core | cached | 4 | 2 | 8 | condor, cvmfs, siteconf, apptainer | Job split high memory |
| 392.0 | DY2L adaptive job split 3-round | cached | 2 | 3 | 8 | condor, cvmfs, siteconf, apptainer | 3-round convergence |
| 500.0 | Fault: proc exits 1 | synthetic | 1 | 1 | 1 | condor | Retry exhaustion → rescue DAG |
| 501.0 | Fault: proc SIGKILL | synthetic | 2 | 1 | 1 | condor | Signal-based failure |
| 510.0 | Fault: merge exits 1 | synthetic | 2 | 1 | 1 | condor | Merge failure → rescue DAG |

### 3.3 Workflow Sets

Named sets group workflows for different testing scenarios:

| Set | Workflows | Purpose | When to Use |
|---|---|---|---|
| `smoke` | 100.0, 150.0, 500.0, 501.0, 510.0 | Quick sanity check | After any code change; CI gate |
| `synthetic` | All `sandbox_mode="synthetic"` | DAG plumbing without CMSSW | Testing DAG structure changes |
| `simulator` | All `sandbox_mode="simulator"` | Full pipeline with simulated physics | Testing output pipeline, metrics |
| `faults` | All workflows with `fault != None` | Recovery path validation | After error handler changes |
| `adaptive` | All workflows with `adaptive=True` | Adaptive algorithm validation | After adaptive/replan changes |
| `integration` | All except scale-only variants | Comprehensive validation | Pre-merge validation |
| `full` | All catalog entries | Complete regression | Release validation |

### 3.4 Running the Matrix

CLI entry point: `python -m tests.matrix`

```
# Run a named set
python -m tests.matrix -l smoke
python -m tests.matrix -l adaptive

# Run specific workflow IDs
python -m tests.matrix -l 100.0,500.0

# Run an ID range
python -m tests.matrix -l 500-530

# List workflows and environment status
python -m tests.matrix --list
python -m tests.matrix --list -l integration

# List available sets
python -m tests.matrix --sets

# Re-render report from saved results
python -m tests.matrix --report
python -m tests.matrix --report /path/to/results.json

# List saved result files
python -m tests.matrix --results

# Enable debug logging
python -m tests.matrix -l smoke -v
```

Results are saved to `/mnt/shared/work/wms2_matrix/results/` and can be re-rendered at any time with `--report`.

---

## 4. Execution Modes

Three sandbox/payload modes provide different fidelity levels. Each uses the same DAG planning and submission infrastructure — only the payload inside each job differs.

### 4.1 Synthetic Mode

**Sandbox mode**: `synthetic`

Synthetic mode creates trivially sized output files without any real processing. The executable is a simple shell script that touches files in the expected locations.

- **Tests**: DAG structure, merge chain correctness, cleanup execution, stage-out paths
- **Speed**: Seconds per workflow (dominated by HTCondor scheduling overhead)
- **Infrastructure**: HTCondor only
- **Use case**: DAG plumbing validation, fault injection targets

### 4.2 Simulator Mode

**Sandbox mode**: `simulator`

The cmsRun simulator (`src/wms2/core/simulator.py`) replaces real CMSSW with a lightweight Python script that produces identical artifacts in seconds:

- **ROOT files**: Valid ROOT files with `Events` TTree via `uproot`, sized to match expected output
- **FJR XML**: Framework Job Report XML compatible with all WMS2 parsers (timing, memory, throughput, storage metrics)
- **Memory simulation**: Allocates real memory via `bytearray` with page-touching for accurate HTCondor `ImageSize` reporting
- **CPU simulation**: Threaded busy loops matching the Amdahl's law resource profile for accurate `cpu.stat` accounting
- **Step chaining**: Output of step N is input to step N+1, matching real StepChain behavior

#### Resource Models

The simulator uses physics-based resource profiles per step type:

**CPU efficiency** — Amdahl's law:
```
eff_cores = serial_fraction + (1 - serial_fraction) * ncpus
cpu_efficiency = eff_cores / ncpus
```

**Memory** — linear model:
```
peak_rss_mb = base_memory_mb + marginal_per_thread_mb * ncpus
```

**Wall time**:
```
wall_sec = events * wall_sec_per_event / eff_cores
```

Each step type (GEN-SIM, DIGI, RECO, NANO) has its own `serial_fraction`, `base_memory_mb`, `marginal_per_thread_mb`, and `wall_sec_per_event` parameters configured in the manifest.

- **Tests**: Full output pipeline, metrics extraction, adaptive algorithm feedback loops
- **Speed**: Seconds per job (wall time dominated by HTCondor scheduling + simulated processing)
- **Infrastructure**: HTCondor only (no CVMFS, no apptainer)
- **Use case**: Rapid adaptive algorithm iteration, output pipeline validation

### 4.3 Real CMSSW Mode

**Sandbox mode**: `cached`

Real CMSSW execution via pre-built sandbox tarballs. Jobs run actual `cmsRun` inside apptainer containers with CMSSW from CVMFS.

- **Tests**: Real physics processing, site-local configuration, proxy authentication, actual resource consumption
- **Speed**: Minutes to hours per workflow depending on step count and event count
- **Infrastructure**: HTCondor, CVMFS, apptainer, siteconf, optionally x509 proxy
- **Use case**: Production-fidelity validation, adaptive algorithm calibration against real workloads

---

## 5. Adaptive Execution Testing

The adaptive testing infrastructure (`tests/matrix/adaptive.py`) validates the adaptive execution model described in main spec §5. It demonstrates that WMS2 can converge resource estimates from initial guesses to measured values within a small number of work unit rounds.

### 5.1 Two-Round Execution Model

The basic adaptive pattern uses two work units (WU0 and WU1):

```
┌──────────────────────────────────────────────┐
│  Round 1 (WU0): Baseline execution           │
│    - Execute at nominal nThreads             │
│    - Collect work_unit_metrics.json           │
│                                              │
│  Replan:                                     │
│    - Read WU0 metrics                        │
│    - Compute per-step optimal nThreads       │
│    - Write manifest_tuned.json for WU1       │
│                                              │
│  Round 2 (WU1): Tuned execution              │
│    - Execute with optimized parameters       │
│    - Collect metrics for comparison           │
│                                              │
│  Verify:                                     │
│    - Compare R1 vs R2 efficiency             │
│    - Check convergence criterion             │
└──────────────────────────────────────────────┘
```

The `analyze_wu_metrics()` function reads `proc_*_metrics.json` files from a completed merge group and aggregates per-step metrics (wall time, CPU efficiency, peak RSS). The `compute_per_step_nthreads()` function derives optimal nThreads for each step from observed CPU efficiency. The `patch_wu_manifests()` function writes `manifest_tuned.json` and updates proc submit files.

### 5.2 Adaptive Modes

The catalog covers several adaptive strategies:

| Mode | Workflows | Description |
|---|---|---|
| Per-step nThreads tuning | 350.x | Reduce nThreads for low-efficiency steps |
| Step 0 parallel split | 351.x | Run step 0 as N parallel instances at reduced threads |
| Overcommit | 360.0 | Allow CPU overcommit up to a configured ratio |
| Split + overcommit | 370.0 | Combine parallel split with overcommit |
| Pipeline split | 380.x | Split all steps into concurrent pipeline stages |
| Job split | 391.x, 392.x | More jobs with fewer cores per job in Round 2 |

### 5.3 Probe Split Testing

Probe split workflows (those with `probe_split=True`) run one WU0 processing job as 2 instances at half-threads. This provides:

- **cgroup memory measurement**: Accurate per-instance memory from HTCondor cgroup accounting
- **R2 sizing input**: Measured memory informs `manifest_tuned.json` for Round 2
- **Split feasibility**: Determines whether parallel splitting is memory-safe

### 5.4 Multi-Round Convergence

Workflow 392.0 tests 3-round convergence (`num_work_units=3`), validating that the adaptive algorithm stabilizes rather than oscillating. The reporter dynamically discovers rounds from step name prefixes and shows pairwise throughput comparison.

### 5.5 Simulator + Adaptive Integration

The simulator responds to `ncpus` changes via Amdahl's law, enabling deterministic adaptive testing:

- Known `serial_fraction` per step produces predictable efficiency curves
- Simulator 155.0 tests the full adaptive loop without CMSSW
- Enables rapid iteration on adaptive algorithm changes

The success criterion from main spec §13: *"resource estimates within 20% of actual by round 2"*.

---

## 6. Fault Injection

The fault injection system (`tests/matrix/faults.py`) validates WMS2's recovery paths by introducing controlled failures into DAG execution.

### 6.1 Injection Mechanism

Faults are injected **after** DAG file generation but **before** HTCondor submission. The `inject_faults()` function patches `.sub` files to replace the real executable with a fault wrapper script. DAGMan reads `.sub` files at node start time (not submission time), so patches applied after `condor_submit_dag` are picked up correctly.

### 6.2 Fault Types

Faults are specified via `FaultSpec`:

```python
@dataclass(frozen=True)
class FaultSpec:
    target: str          # "proc" | "merge" | "cleanup"
    node_indices: tuple[int, ...] | None = None  # None = all nodes of that type
    exit_code: int = 0   # Non-zero exit code
    signal: int = 0      # Signal number (e.g., 9 for SIGKILL)
    delay_sec: int = 0   # Delay before fault
    skip_output: bool = False
```

| Fault | Workflow | Target | Effect | Expected Recovery |
|---|---|---|---|---|
| Process exit 1 | 500.0 | proc | All proc nodes exit with code 1 | DAGMan retries exhaust → rescue DAG |
| Process SIGKILL | 501.0 | proc | All proc nodes killed by signal 9 | Rescue DAG |
| Merge exit 1 | 510.0 | merge | Merge node exits with code 1 | Rescue DAG |

### 6.3 Verification

Each fault workflow has a `VerifySpec` that declares expected outcomes:

```python
@dataclass(frozen=True)
class VerifySpec:
    expect_success: bool = True
    expect_rescue_dag: bool = False
    expect_merged_outputs: bool = True
    expect_cleanup_ran: bool = True
    expect_dag_status: str | None = None
```

For fault workflows, `expect_success=False` and `expect_rescue_dag=True` — the test passes when DAGMan correctly produces a rescue DAG rather than reporting success.

---

## 7. Verification Model

The matrix runner verifies each workflow against its `VerifySpec` after execution completes. Verification covers:

### 7.1 DAG Status

- **Success**: DAGMan reports completed status
- **Failure**: DAGMan reports failed status (expected for fault workflows)
- Checked against `VerifySpec.expect_success` and `VerifySpec.expect_dag_status`

### 7.2 Rescue DAG

- Presence or absence of rescue DAG file in the submit directory
- Expected for fault workflows (`expect_rescue_dag=True`)
- Absence expected for normal workflows

### 7.3 Output Verification

- **Merged outputs**: Merged output files exist at expected LFN paths (`expect_merged_outputs`)
- **Cleanup execution**: Unmerged intermediate files have been removed (`expect_cleanup_ran`)

### 7.4 Performance Metrics

For passing workflows, the reporter collects:

- **Per-step metrics**: Wall time, CPU efficiency, peak RSS, event throughput
- **Throughput**: Events per core-hour computed from actual wall time and core allocation
- **Time breakdown**: Processing, merge, cleanup, and overhead phases from DAGMan log
- **CPU timeline**: Sampled CPU usage over time (cgroup-based or `/proc/stat` fallback)
- **Adaptive comparison**: Round-over-round throughput and efficiency deltas

### 7.5 Adaptive Convergence

For adaptive workflows, the reporter compares consecutive rounds:

- **Events per core-hour**: Should improve from Round 1 to Round 2
- **CPU efficiency per job**: Should increase as nThreads are tuned to match actual parallelism
- **Memory sizing**: R2 `request_memory` should reflect measured RSS from R1

---

## 8. Unit Test Coverage

Unit tests (`tests/unit/`) cover all core WMS2 components using mock fixtures defined in `conftest.py`.

### 8.1 Mock Strategy

`tests/unit/conftest.py` provides pytest fixtures that replace external dependencies:

| Fixture | Replaces | Strategy |
|---|---|---|
| `mock_repository` | `Repository` (PostgreSQL) | `MagicMock(spec=Repository)` with `AsyncMock` returns |
| `mock_reqmgr` | `MockReqMgrAdapter` | In-memory adapter from `wms2.adapters.mock` |
| `mock_dbs` | `MockDBSAdapter` | In-memory adapter |
| `mock_rucio` | `MockRucioAdapter` | In-memory adapter |
| `mock_condor` | `MockCondorAdapter` | In-memory adapter |
| `settings` | `Settings` | Test configuration with short timeouts |

The `make_request_row()` helper creates mock request database rows with sensible defaults.

### 8.2 Test File Map

| File | Component (spec §) | What It Tests |
|---|---|---|
| `test_lifecycle.py` | Lifecycle Manager (§4.2) | Request state machine transitions, timeout handling, lifecycle handlers |
| `test_workflow_manager.py` | Workflow Manager (§4.3) | Workflow import, creation, request-to-workflow conversion |
| `test_admission.py` | Admission Controller (§4.4) | Capacity checking, priority ordering |
| `test_dag_generator.py` | DAG Planner (§4.5) | DAG file generation, SUBDAG EXTERNAL syntax, merge group structure |
| `test_merge_groups.py` | DAG Planner (§4.5) | Merge group planning with fixed job count allocations |
| `test_splitters.py` | DAG Planner (§4.5) | Event-based and file-based job splitting |
| `test_dag_monitor.py` | DAG Monitor (§4.6) | DAG status polling, node status parsing, ClassAd parsing |
| `test_output_manager.py` | Output Manager (§4.7) | ProcessingBlock lifecycle, DBS/Rucio integration |
| `test_error_handler.py` | Error Handler (§4.8) | Error classification, rescue DAG submission, abort logic |
| `test_condor_adapter.py` | HTCondor Adapter | htcondor2 API calls (mocked) |
| `test_models.py` | Data Models (§3) | Pydantic validation, enum values, request schema |
| `test_enums.py` | Enums (§3) | RequestStatus, WorkflowStatus, DAGStatus, BlockStatus, CleanupPolicy, NodeRole |
| `test_stepchain.py` | StepChain Parser | StepChain request parsing, manifest generation |
| `test_sandbox.py` | Sandbox Builder | Sandbox creation, test pset generation |
| `test_output_lfn.py` | Output LFN Helpers | LFN-to-PFN conversion, merged/unmerged LFN generation |
| `test_pilot_runner.py` | Pilot Runner (§5) | FJR parsing, summary computation, script generation |
| `test_pilot_metrics.py` | Pilot Metrics (§5) | Pilot metrics JSON parsing (events/sec, memory, output size) |
| `test_simulator.py` | cmsRun Simulator | CPU burning, memory allocation, ROOT file creation, FJR XML generation |

---

## 9. Integration Test Coverage

Integration tests (`tests/integration/`) validate cross-component interactions with real services.

| File | Infrastructure | What It Tests |
|---|---|---|
| `test_repository.py` | PostgreSQL | CRUD operations on requests, workflows, processing blocks, DAGs |
| `test_api.py` | PostgreSQL | FastAPI HTTP endpoints with real DB, mocked external services |
| `test_dag_planner.py` | Python | End-to-end DAG planning: InputFiles → splitting → DAG files on disk |
| `test_condor_submit.py` | HTCondor (Level 2) | Real DAG submission and execution on a live pool |
| `test_resource_utilization.py` | HTCondor (Level 2) | CPU utilization verification via `/proc/stat` sampling |
| `test_processing_blocks.py` | PostgreSQL | OutputManager + Repository end-to-end with real DB, mock DBS/Rucio |

Integration tests requiring HTCondor are gated by `@pytest.mark.level2` and are skipped when no pool is available.

---

## 10. Environment Verification

Environment checks (`tests/environment/checks.py`) are organized into four capability levels. Each check function returns `(ok: bool, detail: str)`.

### 10.1 Level 0 — Python and Packages

| Check | Function | What |
|---|---|---|
| Python version | `check_python_version()` | Python >= 3.11 |
| Core packages | `check_package_importable()` | fastapi, sqlalchemy, pydantic, pydantic_settings, httpx, pytest, alembic, asyncpg |
| PostgreSQL | `check_postgres_reachable()` | Database accepts connections |

### 10.2 Level 1 — CMSSW Prerequisites

| Check | Function | What |
|---|---|---|
| CVMFS | `check_cvmfs_mounted()` | `/cvmfs/cms.cern.ch` mounted |
| CMSSW releases | `check_cmssw_release_available()` | At least one release directory exists |
| X509 proxy | `check_x509_proxy()` | Valid, non-expired proxy certificate |
| Siteconf | `check_siteconf()` | `site-local-config.xml` exists |
| Apptainer | `check_apptainer()` | `apptainer` or `singularity` in PATH |

### 10.3 Level 2 — HTCondor

| Check | Function | What |
|---|---|---|
| CLI tools | `check_condor_binaries()` | condor_status, condor_submit, condor_q, condor_submit_dag |
| Schedd | `check_condor_schedd()` | Scheduler daemon reachable |
| Slots | `check_condor_slots()` | Pool has available execution slots |
| Python bindings | `check_condor_python_bindings()` | `htcondor2` importable |

### 10.4 Level 3 — Production Services

| Check | Function | What |
|---|---|---|
| ReqMgr2 | `check_reqmgr2_reachable()` | CMS Request Manager API responds |
| DBS | `check_dbs_reachable()` | Data Bookkeeping System API responds |
| CouchDB | `check_couchdb_reachable()` | ConfigCache API responds |

Level 3 checks use `curl` with X509 proxy authentication because Python's `ssl` module does not handle X509 proxy certificate chains correctly.

### 10.5 Matrix Environment Detection

The matrix runner uses `tests/matrix/environment.py` to map these checks into capabilities:

| Capability | Required Checks |
|---|---|
| `condor` | schedd reachable, slots available, Python bindings |
| `cvmfs` | CVMFS mounted |
| `siteconf` | site-local-config exists |
| `apptainer` | apptainer/singularity available |
| `x509` | valid proxy |
| `postgres` | PostgreSQL reachable |
| `reqmgr2` | ReqMgr2 API responds |
| `dbs` | DBS API responds |

Each `WorkflowDef` declares its `requires` tuple. The runner skips workflows whose requirements are not met, reporting them as "skipped" with the reason.

---

## 11. Performance Baselines

The matrix reporter tracks performance metrics that correspond to the success criteria in main spec §13.1:

| Metric | Target | How Tested |
|---|---|---|
| DAG submission rate | 50 DAGs/hour (10K+ nodes/hour) | Scale test with 300.x workflows |
| Tracking latency | < 60 seconds | Timing from DAGMan status change to WMS2 update |
| API response time | p99 < 500ms | Integration test `test_api.py` timing |
| Adaptive convergence | Within 20% by round 2 | Adaptive workflows 350.x–392.x, R1 vs R2 comparison |

The reporter computes **events per core-hour** as the primary throughput metric:

```
core_hours = request_cpus * sum(per_job_wall_sec) / 3600
ev_core_hour = total_events / core_hours
```

For adaptive workflows, this is computed per round, with delta percentages showing improvement.

---

## 12. Future Work

- **Local Rucio/DBS adapters**: Enable full output chain testing (DBS registration, Rucio rule creation) without production service access
- **CI/CD integration**: GitHub Actions workflow with HTCondor local pool for automated smoke set on every PR
- **Load testing framework**: Scale test with 300 DAGs x 10K nodes to validate main spec §13.1 targets
- **Performance regression detection**: Track ev/core-hour across runs, alert on significant regressions
- **Simulator profile library**: Curated step profiles for common workflow types (GEN-SIM, DIGI-RECO, NanoAOD) calibrated against production measurements
