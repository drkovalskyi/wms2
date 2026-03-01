# Current Objectives

Main objective: make sure we can process a real workflow
(cmsunified_task_BPH-RunIISummer20UL18GEN-00292__v1_T_250801_104414_1441)
using WMS2 in a fully automated mode from scratch using default
splitting. To speed things up we are using test fraction of 0.05. The
first round should have only one work unit created automatically,
which measures optimal parameters for the jobs. Next round should
optimize. Keep track of all issues starting and running the test - we
need to fix them.

## Status

Round 1 running (10 work units, 80 proc jobs). Round 0 completed successfully
with 1 work unit (8 proc jobs). Two issues found and fixed this session:

1. **Premature completion** — offset-based termination declared COMPLETED after
   round 0 because generated-event offset exceeded request_num_events. Fixed by
   tracking actual output events (events_produced vs target_events). Backfilled
   target_events=250000 on the running workflow.

2. **Merge job not merging** — manifest.json was not transferred to the merge job
   sandbox, so the merge POST script couldn't detect cmssw mode and fell back to
   copying 8 unmerged files instead of merging into 1. Fixed by adding manifest.json
   to merge transfer_input_files. Round 1 was planned before this fix, so it will
   still copy. The fix takes effect from the next planned round onward.

Waiting for round 1 to complete to verify production tracking and merge fix.

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

- **Rounds 0 and 1 have duplicate physics** — all jobs generated identical events due to missing seed randomization (issue #13). Fix takes effect from next round onward. Previous rounds are scientifically invalid.
- Round 1 (currently running) was planned before the merge fix — will still copy instead of merge
- NanoAOD Rivet segfault on 0 events (CMSSW_10_6_47 bug, not WMS2) — only affects 0-event case
- With test_fraction=0.05, each job produces ~10 output events. NanoAOD should work since events > 0.

## After every failure

Review how error handling performed: check POST script exit codes, retry behavior, early abort, failure ratio computation, and final request status. Confirm no time was wasted on unnecessary retries. If error handling misbehaved, fix it before re-running.

## Definition of done

Workflow completes end-to-end with all outputs registered, no manual intervention needed.
