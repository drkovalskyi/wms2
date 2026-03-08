# WMS2 Planning

<!-- Everything above and including the "---" divider is human-owned; Claude must not edit it.
     Everything below the "---" is Claude-owned; update as work progresses. -->

## Objectives

We are commissioning WMS2 as a service with real CMSSW requests using
ReqMgr2 as the configuration source. All processing is
done in a fully automated mode. We need to test for failures and make
sure that DAG configuration is optimized after the first pilot round
of processing.  Keep track of all issues - we need to fix
them. Solutions need to be reliable, not workarounds unless confirmed
to be acceptable.

### Immediate Goal

Make sure that running in the global pool works reliably. Focus on
debugging.

### Things to review

- pilot concept needs to be clarified. there should be no "pilot",
  round 0 in a request processing is the pilot.

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

### Output registration pipeline (not commissioned)

DBS write operations (open_block, register_files, close_block) and
Rucio rule creation (source protection, tape archival) are scaffolded
in the OutputManager but must not run against production services yet.
The dataset creation step (inject_dataset) is missing from the DBS
pipeline, so writes always fail. DBS reads (get_files, list_files) and
Rucio reads (get_replicas) are fine — used by DAG planner. Currently
the OutputManager receives mock adapters for both DBS and Rucio to
suppress writes. Commission the full output registration pipeline
(DBS dataset creation → block management → Rucio rules) against test
instances before enabling.

## Develop Next

- Comission execution at other sites. We should follow closely
  CRABServer implementation since they use DAGMan and more modern solutions.
- Add support for TaskChain. Same approach as for StepChain, i.e. build work
  units representing one merge group. The only difference is that the dagman
  will now have instead of single layer of processing nodes, a chain of nodes
  representing each task.
- Add configuration control to enable tmpfs for gridpacks in UI.
- We hardcoded some site restrictions. This needs to be revised and handled properly

## Future improvements (not fixing now)

- **Pileup (secondary input) site selection** — configure CMSSW to prefer
  local/nearby replicas or provide a site-filtered pileup file list
- **Intra-DAG replan nodes** — replan between WU0 and WU1 within a single DAG
- **Probe nodes** — modified last proc node in WU0 for memory measurement
- **Pipeline split mode** — code moved but not wired in yet
- **Remote schedd rescue DAG** — copy `.rescue001` from sshfs mount into local
  submit_dir, apply site exclusions locally, then re-spool. Currently rescue
  resubmission doesn't use spool mode.
- **Chirp-based landing optimization** — eliminate the trivial `/bin/true`
  landing node by making proc_000000 elect the site. proc_000000 starts,
  immediately calls `condor_chirp set_job_attr WMS2_ElectedSite` with
  MATCH_GLIDEIN_CMSSite, then proceeds with real CMSSW processing. Other proc
  nodes have no DAG parent dependency on proc_000000; instead, their PRE scripts
  poll `condor_q` for proc_000000's WMS2_ElectedSite attribute, rewrite their
  submit files with site pinning, and exit. This saves ~1-2 min of landing
  overhead while proc_000000 does useful work during site election. Current
  `/bin/true` landing adds ~60s — this is a minor optimization for later.
  **Concerns:** (1) PRE scripts run on the schedd (vocms047), need to know
  proc_000000's cluster ID to poll — requires DAGMan variable substitution or
  a shared file; (2) `condor_chirp` availability varies across grid sites —
  not all worker node environments expose it; (3) if proc_000000 fails before
  chirping (e.g. CVMFS unavailable), other PRE scripts need timeout/fallback
  logic to avoid blocking forever; (4) DAG structure changes fundamentally —
  all procs become root nodes with PRE scripts instead of children of landing,
  making rescue DAG behavior less predictable; (5) the real cost of the current
  landing is not the ~1s `/bin/true` execution but the ~100s wait for an
  8-core/16GB slot to match (since the landing now requests full proc
  resources) — chirp doesn't eliminate this wait, it just hides it inside
  proc_000000's startup; (6) current approach is simple, reliable, and
  well-tested across multiple sites.

## After every failure

Review how error handling performed: check POST script exit codes,
retry behavior, early abort, failure ratio computation, and final
request status. Confirm no time was wasted on unnecessary retries. If
error handling misbehaved, fix it before re-running.

---

## Stageout modes

Three stageout modes, selected at import time (`--stageout-mode`):

| | local | test | production |
|---|---|---|---|
| Stageout | filesystem copy | xrdcp via storage.json | xrdcp via storage.json |
| LFN prefix | N/A | `/store/temp/user/dmytro.wms2.*` | auto from ReqMgr2 (`/store/mc/...`) |
| Rucio scope | N/A | `user.dmytro` | `cms` |
| RSE mapping | N/A | site → `_Temp` (e.g. `T2_CH_CERN_Temp`) | site → as-is (e.g. `T2_CH_CERN`) |
| DID tier | N/A | `/USER#block` | `/AODSIM#block` |
| Consolidation RSE | N/A | `T2_CH_CERN_Temp` | `T2_CH_CERN` |
| Rucio account | N/A | `dmytro` (user, no admin) | service account (admin) |
| Condor pool | local | global (remote schedd) | global (remote schedd) |

## Rucio output consolidation (intermediate)

Rucio DID registration and consolidation rule support is implemented as an
intermediate solution. When `consolidation_rse` is set (per-request via
`config_data` or globally via `WMS2_CONSOLIDATION_RSE`), the OutputManager:

1. Registers merged output files as Rucio DIDs at the execution site RSE
2. Creates a consolidation replication rule to move files to the target RSE

This is separate from the full DBS+Rucio pipeline (DD-4, DD-5, DD-9) in the
spec. DBS writes remain disabled.

**Rucio permissions (CMS policy):**
- `add_replicas`: requires `admin` attribute OR `_Temp` RSE
- `add_did` (dataset): non-cms scope requires `/USER#` in name
- `add_did` (container): non-cms scope requires name ending with `/USER`
- Account `dmytro`: 1 TB at `T2_CH_CERN`, 10 TB at `T2_CH_CERN_Temp`
- Auth: X.509 proxy → token exchange via `cms-rucio-auth.cern.ch`

**Pending:**
- **Refactor `RucioClient` to use native Rucio Python client** — The current
  httpx-based adapter cannot authenticate to CMS Rucio. CMS Rucio requires a
  two-step flow: X.509 proxy → token exchange via `cms-rucio-auth.cern.ch`,
  then token-based API calls. httpx doesn't present proxy certs correctly
  ("Cannot get DN"). The native `rucio.client.Client` (already used for
  pileup file listing) handles auth correctly. Refactor all `RucioClient`
  methods to wrap it instead of raw httpx.
- **CMS DID naming** — CMS Rucio enforces a schema: dataset DIDs must be
  blocks (`/Primary/Processed/TIER#blockname`). File DIDs cannot be created
  by user accounts. Replica registration (`add_replicas`) requires a service
  account or `_Temp` RSE. Adjust `_register_files_in_rucio()` for CMS
  conventions — in test mode use `_Temp` RSEs with explicit PFN.
- **Rucio account** — User account `dmytro` can register replicas at `_Temp`
  RSEs and create rules. Production mode needs service account with `admin`
  attribute (e.g. `wma_prod`, `wmcore_output`).
- **Scope** — Production uses `cms` scope; user testing uses `user.dmytro`.
  Make scope configurable.

## Known bugs

- **Local pool memory overcommit** — The local HTCondor pool reports 166 GB
  slot memory but the machine has only 128 GB physical RAM. With multiple WUs
  running concurrently, HTCondor schedules more jobs than the machine can
  handle, causing SIGBUS (bus error) in memory-heavy steps like DRPremix
  (pileup mixing). Fix: set `MEMORY = 128000` (or actual physical RAM) in
  HTCondor config so the partitionable slot doesn't overcommit.

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

**Global pool commissioning validated.** Two requests running in the CMS global
pool (00058 at 94.7%, 00060 at 29.0%) with zero failures. The full pipeline
works end-to-end: spool-mode DAG submission to remote schedd, grid stageout,
multi-round adaptive optimization, error recovery (rescue chain + HELD + fresh
replan). Merges verified at T2_CH_CERN (prefix storage.json) and T1_US_FNAL
(rules storage.json). No architectural issues found — all bugs were
implementation-level fixes.

Four active requests:
- **00058**: round 17, 56840/60000 events (94.7%) — global pool, near completion
- **00060**: round 1, 17400/60000 events (29.0%) — global pool, fresh DAG after rescue exhaustion
- **00057**: round 7, 19960/30000 events (66.5%) — local pool
- **00059**: completed, 12000/12000 events (100%)

### Global pool commissioning

**Validated:**
- Spool-mode DAG submission to remote schedd (vocms047.cern.ch)
- Landing node site election + site pinning (elect_site.sh / pin_site.sh)
- Grid stageout via storage.json LFN→PFN resolution (xrdcp)
- Merge at T2_CH_CERN (prefix format, `root://eoscms.cern.ch`)
- Merge at T1_US_FNAL (rules format, `root://cmseos.fnal.gov`)
- Cleanup of unmerged files on grid storage
- Rescue DAG submission in spool mode
- Rescue chain exhaustion → HELD → fresh replan with updated code
- Multi-round adaptive optimization across 17+ rounds
- Autonomous lifecycle manager operation in global pool

**Not yet validated:**
- Merge at European sites using `davs://` protocol (KIT, NCBJ, DESY) —
  `proc_node_indices` probe fallback deployed but untested at those sites
- Production-scale requests (current tests use test_fraction=0.01)
- TaskChain request type in global pool

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
- **Global pool**: spool-mode submission, site election, grid merge/cleanup at CERN + FNAL

### Test commands

Service mode with global pool:
```bash
# Terminal 1: start service
WMS2_CONDOR_HOST="localhost:9618" \
  WMS2_EXTRA_COLLECTORS="cmsgwms-collector-global.cern.ch:9620" \
  WMS2_LIFECYCLE_CYCLE_INTERVAL=30 \
  WMS2_SPOOL_MOUNT="/mnt/remote_spool" \
  WMS2_REMOTE_SPOOL_PREFIX="/data/srv/glidecondor/condor_local/spool" \
  WMS2_REMOTE_SCHEDD="vocms047.cern.ch" \
  WMS2_SEC_TOKEN_DIRECTORY="/mnt/creds/tokens.d" \
  uvicorn wms2.main:create_app --factory --host 0.0.0.0 --port 8080

# Terminal 2: import request (exits immediately)
wms2 import <request_name> --sandbox-mode cmssw --test-fraction 0.01 --no-monitor
```

Local pool mode:
```bash
WMS2_CONDOR_HOST="localhost:9618" WMS2_LIFECYCLE_CYCLE_INTERVAL=30 \
  uvicorn wms2.main:create_app --factory --host 0.0.0.0 --port 8080
```

Matrix smoke tests:
```bash
python -m tests.matrix -l smoke
```

### Known issues

- NanoAOD Rivet segfault on 0 events (CMSSW_10_6_47 bug, not WMS2)
- Grid listing via `gfal-ls` on `davs://` returns empty at T1_DE_KIT and
  T1_PL_NCBJ — `proc_node_indices` probe fallback deployed but not yet
  validated at those sites

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
20. Workflow status stuck at "resubmitting" after rescue DAG submission — `transition()` only updated request status; fixed to also reset workflow status to ACTIVE
21. `nodes_done > total_nodes` — total_nodes was stale from DAG creation; fixed with live total computed from inner summary each poll cycle
22. Stale status files in spool mode — `dag_file_path + ".status"` pointed to old spool dir; fixed with `_resolve_dag_file()` that checks `submit_dir` first
23. Merge crash on grid workers when gfal-ls returns empty — fell to text merge path, crashed on `os.makedirs("/mnt/shared")` (read-only); fixed with guard to exit with error if no ROOT and no text files
24. Grid listing empty at T1_DE_KIT/T1_PL_NCBJ — `gfal-ls` on `davs://` returned empty; added `proc_node_indices` probe fallback in merge script
