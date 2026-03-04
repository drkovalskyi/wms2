# WMS2 Planning

<!-- Everything above and including the "---" divider is human-owned; Claude must not edit it.
     Everything below the "---" is Claude-owned; update as work progresses. -->

## Objectives

We are commissioning WMS2 as a service with real CMSSW requests taking
configuration from ReqMgr2. All processing is done in a fully
automated mode. We need to test for failures and make sure that DAG
configuration is optimized after the first pilot round of processing.
Keep track of all issues - we need to fix them. We need reliable
solutions, not kludges.

To speed things up we are using test fraction.

### Service mode

Build the service that manages requests autonomously:

- Requests are injected into WMS2 DB (via API or import tool)
- Lifecycle manager service runs continuously, discovers active requests,
  polls DAGs, handles round transitions, adaptive optimization
- CLI becomes a thin client: import + optionally tail logs
- The lifecycle manager already has the core logic; needs to be wired into
  a long-running service loop

### Monitoring

Build observability for WMS2:

- Dashboard showing active requests, workflow status, round progress
- Per-workflow metrics: events_produced vs target, current round, job counts
- Per-step performance: CPU efficiency, memory usage, throughput
- Alerting on stuck/failed workflows
- HTCondor queue overview (running/idle/held by workflow)
- Technology TBD (Prometheus + Grafana, or simple web UI, or CLI status command)

## Develop Next

- Add configuration control to enable tmpfs for gridpacks in UI.
- Make sure that work group size is optimized once after the pilot round to
  get optimal merged output size.
- Add support for TaskChain. Same approach as for StepChain, i.e. build work
  units representing one merge group. The only difference is that the dagman
  will now have instead of single layer of processing nodes, a chain of nodes
  representing each task.

## Future improvements (not fixing now)

- **Pileup (secondary input) site selection** — configure CMSSW to prefer
  local/nearby replicas or provide a site-filtered pileup file list
- **Intra-DAG replan nodes** — replan between WU0 and WU1 within a single DAG
- **Probe nodes** — modified last proc node in WU0 for memory measurement
- **Pipeline split mode** — code moved but not wired in yet

## After every failure

Review how error handling performed: check POST script exit codes,
retry behavior, early abort, failure ratio computation, and final
request status. Confirm no time was wasted on unnecessary retries. If
error handling misbehaved, fix it before re-running.

---

## Technical debt

- **"Workflow" naming confusion** — WMS2 internally uses "workflow" for the
  execution record of a request (DB table, API endpoints, data model, core
  components). This clashes with ReqMgr2's use of "workflow" for the request
  itself. The web UI now says "Processing Details" but the internal name is
  still `workflow` everywhere: database table, `Workflow` model, `/api/v1/workflows/`
  endpoints, `WorkflowManager`, repository methods, spec document. A full rename
  (e.g. to `execution` or `processing`) would require a DB migration, API version
  bump, and ~100 references in the spec. Low priority but worth resolving for
  clarity before onboarding other developers.

## Claude Status

### Current status

**Service mode works end-to-end** with real CMSSW workflows. The lifecycle manager
runs as an autonomous service: CLI imports with `--no-monitor`, service handles
DAG monitoring, round completion, adaptive optimization, and multi-round planning.

Tested with `cmsunified_task_GEN-Run3Summer23wmLHEGS-00058` (5-step StepChain,
test_fraction=0.01): round 0 completed autonomously, adaptive optimization reduced
memory 7900 → 5672 MB, round 1 automatically planned (10 WUs, 80 jobs).

### Verified working

- Import from ReqMgr2, DAG planning, job submission, processing (5-step StepChain)
- Seed randomization (each job gets a unique random seed)
- Merge (AODSIM, MINIAODSIM, NANOAODSIM via cmsRun)
- Cleanup of unmerged files after merge
- Round completion with metrics aggregation, adaptive optimization, events tracking
- Error handling: retry, rescue DAG, early abort, site exclusion
- Shared `complete_round()` logic between CLI and lifecycle manager
- Tmpfs gridpack extraction with split_tmpfs=true (via apptainer)
- Multi-round adaptive optimization (8T → 4T → 2T with memory tuning)
- **Service mode**: per-cycle DB sessions, explicit commit/rollback, CLI `--no-monitor`
- **Multi-round service autonomy**: round 0 → adaptive optimization → round 1 planning
- **Grid stageout**: xrdcp-based stageout via storage.json LFN→PFN resolution
  - `WMS2_STAGEOUT_MODE=grid` — proc/merge/cleanup use XRootD (or gfal-copy/WebDAV)
  - `WMS2_STAGEOUT_MODE=local` (default) — filesystem copy (backward compatible)
  - Supports CMS storage.json formats: prefix, rules, chained rules
  - Self-contained `wms2_stageout.py` utility transferred to worker nodes
  - Local XRootD server at T2_LOCAL_DEV for integration testing

### Test commands

Service mode (start service + import via CLI):
```bash
# Terminal 1: start service
unset WMS2_CERT_FILE WMS2_KEY_FILE
WMS2_CONDOR_HOST="localhost:9618" WMS2_LIFECYCLE_CYCLE_INTERVAL=30 \
  uvicorn wms2.main:create_app --factory --host 0.0.0.0 --port 8080

# Terminal 2: import request (exits immediately)
wms2 import cmsunified_task_GEN-Run3Summer23wmLHEGS-00058__v1_T_230922_115553_5657 \
  --sandbox-mode cmssw --test-fraction 0.01 --no-monitor
```

Real CMSSW workflow (CLI monitoring mode):
```bash
wms2 import cmsunified_task_B2G-Run3Summer23BPixwmLHEGS-06000__v1_T_250628_211038_1313 \
  --sandbox-mode cmssw
```

Matrix smoke tests:
```bash
python -m tests.matrix -l smoke
```

Adaptive 3-round test:
```bash
python -m tests.matrix -l 391.4
```

### Known issues

- NanoAOD Rivet segfault on 0 events (CMSSW_10_6_47 bug, not WMS2)

### Historical issues (fixed)

1. Wrong failure ratio — was using inner node count instead of work units
2. `read_post_data()` missed early-aborted nodes (filtered on `final=True`)
3. Rescue DAG submission crashed — no `Force` option for `from_dag()`
4. 2-node sub-DAGs couldn't reach early abort threshold (hardcoded at 3)
5. Infrastructure errors retried same broken site 3x with 300s cooloff
6. Rescue DAG landed on same broken site — no site exclusion mechanism
7. SplittingParams key mismatch — `EventsPerJob` lowercased to `eventsperjob` instead of `events_per_job`
8. Missing filter_efficiency — StepChain Step1.FilterEfficiency not used when top-level absent
9. `set -euo pipefail` killed script before STEP_RC captured
10. GenFilter events_per_job double-inflation
11. Premature completion — offset-based termination vs production-based
12. Merge job not merging — manifest.json missing from transfer_input_files
13. Duplicate physics events — seeds not randomized
14. events_produced=0 after round completion — fixed with disk fallback
15. step_metrics=NULL — now stores WU performance data
16. Cleanup job can't find cleanup_manifest.json — not in transfer_input_files
17. Matrix mock missing adaptive fields — MagicMock returned mocks instead of ints
18. CLI duplicated round-completion logic — refactored to shared `complete_round()`
19. Apptainer `/dev/null: Permission denied` with split_tmpfs — `cd /dev/shm` before launching apptainer caused container's `/dev` mount conflict; fixed by cd-ing to tmpfs inside the container after `cmsset_default.sh`
