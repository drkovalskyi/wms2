# Current Objectives

Main objective: make sure we can process a real workflow
(cmsunified_task_BPH-RunIISummer20UL18GEN-00292__v1_T_250801_104414_1441)
using WMS2 in a fully automated mode from scratch using default
splitting. To speed things up we are using test fraction of 0.05. The
first round should have only one work unit created automatically,
which measures optimal parameters for the jobs. Next round should
optimize. Keep track of all issues starting and running the test - we
need to fix them.

Verify that each job is using a new seed for event generation.

Make sure that outpus is properly merged in each work unit.

## Status

Fresh re-import running (previous rounds had duplicate physics due to issue #13).
Round 0 in progress: 7 of 8 proc jobs completed, proc_000004 retrying (attempt 1
of 3) after XRootD pileup file timeout. Seed randomization fix verified — all jobs
have unique RandomNumberGeneratorService seeds.

## Command

```bash
wms2 import cmsunified_task_BPH-RunIISummer20UL18GEN-00292__v1_T_250801_104414_1441 \
  --sandbox-mode cmssw \
  --test-fraction 0.05
```

## Known Issues (fixed)

1. Wrong failure ratio — was using inner node count instead of work units
2. `read_post_data()` missed early-aborted nodes (filtered on `final=True`)
3. Rescue DAG submission crashed — no `Force` option for `from_dag()`
4. 2-node sub-DAGs couldn't reach early abort threshold (hardcoded at 3)
5. Infrastructure errors retried same broken site 3x with 300s cooloff
6. Rescue DAG landed on same broken site — no site exclusion mechanism
7. SplittingParams key mismatch — `EventsPerJob` lowercased to `eventsperjob` instead of `events_per_job`
8. Missing filter_efficiency — StepChain Step1.FilterEfficiency not used when top-level absent
9. `set -euo pipefail` killed script before STEP_RC captured — `run_step` non-zero exit triggered `set -e` before `STEP_RC=$?`
10. GenFilter events_per_job double-inflation — events_per_job is *generated* events (not output); PSet injection must NOT divide by filter_eff; total_events must be inflated by 1/filter_eff at planning time
11. Premature completion — offset-based termination used assignment offsets for accounting; GenFilter round 0 offset exceeded request_num_events despite producing only ~90 output events vs 250K target
12. Merge job not merging — manifest.json missing from merge job's transfer_input_files; merge POST script fell back to file copy instead of cmsRun merge
13. Duplicate physics events — RandomNumberGeneratorService seeds not randomized; all GEN jobs used identical hardcoded seeds from IOMC_cff, producing bit-for-bit identical events. Fixed by calling RandomNumberServiceHelper.populate() (same as WMAgent's AutomaticSeeding)

## Potential issues

- NanoAOD Rivet segfault on 0 events (CMSSW_10_6_47 bug, not WMS2) — only affects 0-event case
- With test_fraction=0.05, each job produces ~10 output events. NanoAOD should work since events > 0.

## Future improvements (not fixing now)

- **Pileup (secondary input) site selection** — CMSSW reads pileup files via AAA
  (XRootD federation), which can pick a bad replica at a remote/unreachable site.
  proc_000004 failed on Step 3 (DIGIPremix) because AAA routed to an unresponsive
  site. DAGMan retry handled it (restarts from scratch, likely gets a different
  replica). A future improvement would be to configure CMSSW to prefer local/nearby
  replicas for secondary input, or to provide a site-filtered pileup file list.
  Details of the failure:
  - **File**: `/store/mc/RunIISummer20ULPrePremix/Neutrino_E-10_gun/PREMIX/BParking_106X_upgrade2018_realistic_v16_L1v1-v1/80020/5ADFEA0E-6F73-9645-A309-FA7DDE767EBF.root`
  - **Site**: `se01.indiacms.res.in:1094`
  - **Error**: XRootD `kXR_open` timeout (two attempts, ~3 min each), then CMSSW `FileOpenError` exit code 84
  - **Step**: BPH-RunIISummer20UL18DIGIPremix-00476_0
  - **Resolution**: DAGMan retry #1 of 3; job restarts from GEN (loses ~2h of work)

## After every failure

Review how error handling performed: check POST script exit codes, retry behavior, early abort, failure ratio computation, and final request status. Confirm no time was wasted on unnecessary retries. If error handling misbehaved, fix it before re-running.

## Definition of done

Workflow completes end-to-end with all outputs registered, no manual intervention needed.
