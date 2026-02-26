# WMS2 — Error Handling Specification

| Field | Value |
|---|---|
| **Version** | 1.0.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-26 |
| **Parent Spec** | `docs/spec.md` v2.9.0 |

---

## 1. Overview

WMS2 uses a **two-level error handling model** that separates fast, per-node recovery from slow, workflow-level decisions:

| Level | Owner | Scope | Speed | Mechanism |
|---|---|---|---|---|
| **Level 1: Immediate** | POST script + DAGMan RETRY | Per-node | Seconds–minutes | Classify error, retry or stop |
| **Level 2: Delayed** | WMS2 Error Handler + rescue DAG | Per-round | Minutes–hours | Aggregate failures, rescue or hold |

**Design principles:**

1. **POST script is the brain, RETRY is the loop.** DAGMan's RETRY directive is primitive — no delays, no conditionals beyond a single UNLESS-EXIT code. All classification intelligence lives in the POST script. RETRY provides fast in-DAG resubmission.
2. **WMS2 never tracks individual jobs.** Error handling operates at the work-unit and round level. Per-node data is collected by POST scripts into side files that WMS2 reads after DAG completion.
3. **Fail fast, recover slowly.** Transient errors are retried immediately (Level 1). Persistent patterns are surfaced to operators (Level 2) rather than silently retried forever.
4. **Rescue first, then next round.** When a DAG finishes with failures, rescue handles transient issues (cooloff, site flaps). A new round is planned only after rescue attempts are exhausted.

**Relationship to spec Section 6:** The main spec (`docs/spec.md` Section 6) defines the recovery model at a high level — three conceptual recovery levels, clean stop flow, catastrophic failure recovery, and POST script sketch. This document expands Level 1 and Level 3 into a complete, implementable design. Level 2 (DAG-wide correlated failure detection) remains an HTCondor feature request (spec Section 12) — the practical model uses POST scripts (Level 1) and WMS2 rescue decisions (Level 2 in this document) as the two operational levels.

---

## 2. Level 1: Immediate Error Handling (POST Script + DAGMan RETRY)

### 2.1 DAGMan RETRY Mechanics

Every node in the DAG has a `RETRY` directive that allows DAGMan to resubmit the node without WMS2 involvement:

```
# In the .dag file
RETRY proc_000001 3 UNLESS-EXIT 42
RETRY proc_000002 3 UNLESS-EXIT 42
RETRY merge_grp_001 2 UNLESS-EXIT 42
```

- **Retry count**: 2–3 attempts for processing nodes, 1–2 for merge nodes. The POST script controls whether a retry actually happens by choosing its exit code.
- **UNLESS-EXIT**: A designated exit code (e.g., 42) that tells DAGMan "do not retry — this failure is permanent." The POST script exits with this code when it classifies an error as non-retryable.
- **Division of responsibility**: RETRY provides the resubmission loop. The POST script provides the decision logic (classify, adjust, allow/block retry).

### 2.2 POST Script Design

The POST script runs on the **submit host** after each job attempt (success or failure). It receives information about the completed/failed job and produces a classification that controls DAGMan's next action.

**Invocation**: DAGMan calls the POST script with macros that provide context:

```
# In the .dag file
SCRIPT POST proc_000001 post_script.sh $JOB $RETURN $RETRY $MAX_RETRIES $DAG_STATUS $FAILED_COUNT
```

| Macro | Meaning |
|---|---|
| `$JOB` | Node name (e.g., `proc_000001`) |
| `$RETURN` | Job exit code (0 = success, non-zero = failure) |
| `$RETRY` | Current retry attempt (0-indexed) |
| `$MAX_RETRIES` | Maximum retries configured for this node |
| `$DAG_STATUS` | Overall DAG status at time of POST execution |
| `$FAILED_COUNT` | Number of failed nodes in the DAG so far |

**Exit code conventions:**

| POST Exit Code | DAGMan Behavior | Meaning |
|---|---|---|
| 0 | Node marked successful | Job succeeded, or POST absorbed the failure |
| Non-zero (not UNLESS-EXIT) | Node marked failed, eligible for RETRY | Retryable error — DAGMan resubmits if retries remain |
| UNLESS-EXIT code (42) | Node marked failed, **no more retries** | Permanent failure — stop retrying this node |

### 2.3 POST Script Data Collection

The POST script writes a JSON side file per node attempt in the submit directory:

```
{submit_dir}/{node_name}.post.json
```

This file is the primary data exchange between Level 1 (POST script) and Level 2 (WMS2 Error Handler). WMS2 reads these files after DAG termination to make workflow-level decisions.

**Contents:**

```json
{
  "node_name": "proc_000001",
  "timestamp": "2026-02-26T14:30:00Z",
  "attempt": 2,
  "max_retries": 3,
  "final": true,

  "job": {
    "exit_code": 8021,
    "exit_signal": null,
    "hold_reason": null,
    "wall_time_sec": 3600,
    "cpu_time_sec": 3200,
    "memory_mb": 4096,
    "site": "T2_US_Purdue",
    "schedd": "schedd01.fnal.gov"
  },

  "cmssw": {
    "exit_code": 8021,
    "steps": {
      "cmsRun1": {"status": 1, "exit_code": 8021},
      "stageOut1": {"status": 0, "exit_code": 0}
    },
    "error_message": "FileReadError: Unable to read /store/data/file_001.root",
    "input_files": ["/store/data/file_001.root", "/store/data/file_002.root"],
    "output_files": [],
    "performance": {
      "peak_rss_mb": 3800,
      "avg_event_time_sec": 1.2,
      "events_read": 15000,
      "events_written": 0
    }
  },

  "classification": {
    "category": "data",
    "retryable": false,
    "bad_input_files": ["/store/data/file_001.root"],
    "action": "permanent_failure"
  },

  "log_tail": "... last 200 lines of stderr ..."
}
```

**Data sources:**

| Field Group | Source | How POST Script Obtains It |
|---|---|---|
| `job.*` | HTCondor job classads | `condor_q -json` or job ad file in submit directory |
| `job.site` | `MATCH_GLIDEIN_CMSSite` classad | From job ad or userlog |
| `cmssw.*` | Framework Job Report (FJR) | Parse `FrameworkJobReport.xml` from job sandbox |
| `cmssw.exit_code` | FJR `ExitCode` element | Direct FJR parse |
| `cmssw.input_files` | FJR `<InputFile>` entries | Direct FJR parse |
| `cmssw.error_message` | FJR `<FrameworkError>` or log patterns | FJR parse + grep |
| `classification.*` | POST script logic | Error classification algorithm (Section 2.4) |
| `log_tail` | Job stderr | Last 200 lines of `_condor_stderr` from sandbox |

**Important**: The `final` field indicates whether this is the last attempt for this node (either succeeded, or retries exhausted, or classified as permanent). WMS2 only reads the final post.json — intermediate attempts are for POST script internal bookkeeping.

### 2.4 POST Script Classification and Actions

The POST script classifies each failure into one of four categories and takes an appropriate action:

| Category | Criteria | POST Exit | Effect |
|---|---|---|---|
| **Transient** | Known temporary errors (network timeout, temporary file access failure, schedd flap) | Non-zero (not 42) | Allow RETRY; optional short cooloff via `sleep` |
| **Permanent** | Non-retryable errors (CMSSW config error, missing software, authentication failure) | 42 (UNLESS-EXIT) | Stop retrying this node |
| **Data** | Input file is unreadable or corrupt (FileReadError, checksum mismatch) | 42 (UNLESS-EXIT) | Stop retrying; record bad file in `classification.bad_input_files` |
| **Infrastructure** | Site-level issue (storage failure, worker node misconfiguration) | Non-zero (not 42) | Allow RETRY with cooloff; job may land on different site |

**Classification logic** (simplified):

```python
def classify_error(cmssw_exit_code, job_exit_code, error_message, site):
    # CMSSW-specific codes
    if cmssw_exit_code == 8021:  # FileReadError
        return "data"
    if cmssw_exit_code == 8028:  # FileOpenError (corrupt/missing)
        return "data"
    if cmssw_exit_code in (65, 66, 67):  # Config/software errors
        return "permanent"

    # HTCondor-level failures
    if job_exit_code == 0 and cmssw_exit_code != 0:
        return "transient"  # Job wrapper succeeded but CMSSW failed
    if "held" in str(job_exit_code):
        return "infrastructure"

    # Default: assume transient
    return "transient"
```

**Parameter adjustment for retries**: The POST script can modify the submit file for the next retry attempt. For example, if a job was killed for exceeding memory, the POST script can increase `request_memory` in the `.sub` file before exiting with a retryable code:

```bash
# If memory exceeded, bump request_memory by 50%
if [ "$CMSSW_EXIT" -eq 50660 ]; then
    current_mem=$(grep request_memory ${NODE_NAME}.sub | awk '{print $3}')
    new_mem=$((current_mem * 3 / 2))
    sed -i "s/request_memory = .*/request_memory = ${new_mem}/" ${NODE_NAME}.sub
    exit 1  # Retryable
fi
```

### 2.5 ABORT-DAG-ON

DAGMan's `ABORT-DAG-ON` directive provides a circuit breaker for catastrophic errors. If any node's POST script exits with a designated abort code, DAGMan kills the entire DAG immediately:

```
# In the .dag file
ABORT-DAG-ON proc_000001 43 RETURN 1
```

This is reserved for errors where continuing the DAG is pointless — for example, a missing dataset that affects all nodes, or a global configuration error. The POST script exits with code 43 only when it identifies such a condition.

**When to use ABORT-DAG-ON vs. UNLESS-EXIT:**
- `UNLESS-EXIT 42`: Stop retrying *this node* but let the rest of the DAG continue.
- `ABORT-DAG-ON 43`: Stop the *entire DAG* immediately. Used for errors that affect all nodes (e.g., dataset deleted, global config broken).

### 2.6 Merge Group Behavior

Work units are implemented as SUBDAG EXTERNAL merge groups (see spec DD-2). The merge node depends on all processing nodes in the group:

```
# Inside merge group sub-DAG
PARENT proc_001 proc_002 ... proc_N CHILD merge_grp_001
PARENT merge_grp_001 CHILD cleanup_grp_001
```

**Failure propagation within a merge group:**

1. If a processing node fails permanently (exhausts retries or hits UNLESS-EXIT), DAGMan marks it FAILED.
2. The merge node's `PARENT` dependency is never satisfied — DAGMan will not run the merge node.
3. The entire merge group (work unit) fails. The cleanup node also does not run.
4. The failed work unit is reflected in the DAG's final status and reported to WMS2.

**Future option — POST absorb for partial merges**: A POST script could absorb a processing node failure (exit 0) and let the merge node run on partial input. This would allow merge groups to produce output even when some processing jobs fail. This pattern is documented here as a future option but is **not part of the initial implementation** — it requires careful handling of output completeness metadata and downstream validation.

---

## 3. Level 2: Delayed Error Handling (WMS2 + Rescue DAG)

### 3.1 When It Triggers

Level 2 error handling engages when a DAG terminates with failures — meaning some nodes exhausted their RETRY attempts at Level 1 and remain in a failed state.

**Sequence:**

1. DAG finishes execution (some nodes DONE, some FAILED, some potentially unrun due to dependency failures).
2. DAG Monitor detects DAG termination via status file polling (spec Section 4.6).
3. DAG Monitor reports DAG status as PARTIAL (some failures) or FAILED (all failed) to the Lifecycle Manager.
4. Lifecycle Manager calls the Error Handler.
5. Error Handler reads POST script side files (`{node_name}.post.json`) from the submit directory.
6. Error Handler aggregates failure data and applies the threshold decision.

### 3.2 Failure Threshold

The Error Handler uses a **per-round** failure ratio to decide the next action:

```
failure_ratio = failed_work_units / total_work_units_in_round
```

| Condition | Action | New Status |
|---|---|---|
| `failure_ratio < 0.20` AND `rescue_count < max_rescue_attempts` | Submit rescue DAG | DAG → SUBMITTED (rescue) |
| `failure_ratio >= 0.20` | Hold for operator | Request → HELD |
| `rescue_count >= max_rescue_attempts` (regardless of ratio) | Hold for operator | Request → HELD |

**Key design decisions:**

- **20% threshold** replaces the three-tier model (5%/30%) from the initial spec sketch. A single threshold is simpler to reason about and configure. It will evolve with operational experience.
- **HELD** replaces the "review" and "abort" split. There is one operator-attention state, not two.
- **FAILED is manual only.** The Error Handler never sets a request to FAILED automatically. Only an operator or the requestor can make that decision (see Section 4.2).
- **Max rescue attempts** (default 3) is a separate guard. Even if the failure ratio stays low, endless rescue loops are prevented.

**Counting**: `failed_work_units` counts merge group SUBDAGs that ended in a failed state. A work unit is "failed" if any of its processing nodes failed permanently (preventing the merge node from running). Work units that completed successfully in the original DAG run or in previous rescue attempts are not counted — they are marked DONE in the rescue DAG and skipped.

### 3.3 Rescue DAG Mechanics

When a DAG finishes with failures, DAGMan writes a **rescue DAG** file (`.rescue001`, `.rescue002`, etc.) that records which nodes completed successfully:

```
# example.dag.rescue001
# Rescue DAG file — marks completed nodes as DONE
DONE proc_000001
DONE proc_000002
DONE merge_grp_001
DONE cleanup_grp_001
# proc_000003, merge_grp_002, etc. are NOT listed → will be retried
```

**Submitting the rescue DAG**: WMS2 resubmits the *original* `.dag` file. DAGMan automatically detects the `.rescue001` file in the same directory and skips all nodes marked DONE. Failed nodes and their blocked dependents are retried.

**Properties:**

- **Cumulative**: Each rescue file marks all previously completed nodes. The rescue chain is: `.rescue001` (first failure), `.rescue002` (second failure), etc. Each successive file is a superset of the previous.
- **Schedd-portable**: Rescue DAG files are plain text on the shared filesystem. They contain no schedd-specific state (no cluster IDs, no job IDs). A rescue DAG can be submitted on a different schedd than the original — this enables schedd drain/decommission scenarios.
- **WMS2 bookkeeping**: When submitting a rescue DAG, WMS2 creates a new DAG record with `parent_dag_id` pointing to the previous DAG. The rescue count is tracked as the number of DAG records in the chain.

### 3.4 Adaptive Round Recovery Flow

For adaptive workflows (multi-round execution, see `docs/processing.md`), error recovery interacts with round advancement:

```
Round N DAG finishes with partial failures
  │
  ├─ failure_ratio < 0.20 AND rescue_count < max_rescue_attempts?
  │   │
  │   YES → Submit rescue DAG
  │   │     (handles transient issues: cooloff effects, site flaps, temporary outages)
  │   │
  │   ├── Rescue succeeds → Round N complete → advance to Round N+1
  │   └── Rescue still has failures → loop back to threshold check
  │
  └─ NO (threshold exceeded or rescues exhausted)
      │
      └── Request → HELD (operator attention)
          Operator can:
          ├── Release → credit completed work, advance offsets, plan Round N+1
          ├── Release with modifications (ban sites, exclude files, adjust resources)
          ├── Fail → FAILED (return to requestor)
          └── Kill and clone (increment processing_version, start over)
```

**Critical rule: one round at a time.** WMS2 never plans Round N+1 while Round N (or any of its rescue attempts) is still in progress. The round must fully resolve — either all work units complete, or the request is held for operator decision — before the next round begins.

**Credit on release from HELD**: When an operator releases a HELD request, WMS2 credits all work units that completed successfully (across the original DAG and its rescue attempts). For workflows without primary input, the event offset advances past all planned events for the round (see Section 5.1). For workflows with primary input, completed files are marked as processed and failed files are handled per Section 5.2.

---

## 4. Request States for Error Handling

### 4.1 HELD State

HELD is a new request status indicating that the request is valid but an external problem prevents continued processing. It requires operator attention.

**Entry conditions:**
- Failure ratio >= 20% after a DAG round completes
- Max rescue attempts exhausted (regardless of failure ratio)

**Properties:**
- Request is *not* terminal — it can be resumed.
- All completed work is preserved (DBS registrations, Rucio rules, processing block state).
- The request does not consume admission capacity while HELD.

**Operator actions on a HELD request:**

| Action | API Call | Effect |
|---|---|---|
| **Release** | `POST /api/v1/requests/{name}/release` | Request → QUEUED. Completed work preserved. Next admission cycle plans the next action (rescue or new round). |
| **Release with modifications** | `POST /api/v1/requests/{name}/release` with body | Same as release, but applies modifications first: site bans, memory overrides, retry limit changes, file exclusions. |
| **Fail** | `POST /api/v1/requests/{name}/fail` | Request → FAILED. Terminal. Output invalidation triggered (see Section 4.2). |
| **Kill and clone** | `POST /api/v1/requests/{name}/restart` | Old request → FAILED. New request created with `processing_version + 1`. See Section 4.3. |
| **Inspect** | `GET /api/v1/requests/{name}/errors` | View aggregated POST script data: failure counts by category, by site, bad input files, log tails. No state change. |

### 4.2 FAILED State

FAILED is a terminal state. Once a request is FAILED, it cannot be resumed.

**Entry**: Manual only — set by an operator (`fail` action on a HELD request) or by the requestor cancelling. The Error Handler **never** sets FAILED automatically.

**Rationale**: Automatic failure is dangerous. A transient infrastructure issue (site outage, network partition) could cause 100% failure in a round, but the request itself is valid. Requiring human judgment before declaring failure prevents premature data loss and unnecessary resubmission.

**On entering FAILED:**

1. If any DAG is still running, issue `condor_rm` to stop it.
2. Mark all associated DAGs as FAILED.
3. Mark all open processing blocks as FAILED.
4. Trigger output invalidation:
   - **For now**: Write an invalidation manifest file listing all DBS blocks and Rucio rules that need cleanup. Operators process this manually or via a separate tool.
   - **Future**: Call DBS API to invalidate blocks, call Rucio API to delete source protection and tape archival rules.
5. Update request status in ReqMgr2 (if applicable).

### 4.3 Kill and Clone

Kill and clone is the catastrophic recovery mechanism (spec Section 6.3). It creates a new request that supersedes the old one.

**Flow:**

```
Operator calls POST /api/v1/requests/{name}/restart
  │
  ├── Old request → FAILED (output invalidation deferred — see below)
  ├── New request created:
  │     ├── Clone of old request with processing_version + 1
  │     ├── previous_version_request = old.request_name
  │     └── New request → SUBMITTED (enters normal pipeline)
  └── Old request: superseded_by_request = new.request_name
```

**Output lifecycle during kill and clone:**

Old outputs are **not** immediately invalidated. Users may be consuming partial results from the old request, and invalidating them before the new request produces replacements would cause unnecessary disruption.

| Output State | Meaning | Transition |
|---|---|---|
| **Active** | Old request's outputs, still valid and accessible | Initial state after kill and clone |
| **Superseded** | New request has completed — old outputs have replacements | When new request reaches COMPLETED |
| **Invalidated** | Old outputs removed from DBS, Rucio rules deleted | Per cleanup policy (immediate or manual) |

The `cleanup_policy` on the request (spec Section 3.1.1) controls the Active → Superseded → Invalidated transition:
- `keep_until_replaced`: Old outputs stay Active until the new request reaches COMPLETED, then move to Superseded. Invalidation is manual.
- `immediate_cleanup`: Old outputs are invalidated as soon as the new request reaches COMPLETED.

---

## 5. Workflow-Type-Specific Error Recovery

WMS2 handles two fundamentally different workflow types with different recovery strategies.

### 5.1 Workflows Without Primary Input (MC Generation)

Monte Carlo generation workflows produce events from scratch — there is no input dataset. The workflow tracks progress via an **event offset** (`workflow.next_first_event`) that advances monotonically.

**Recovery model: offset advance, no gap tracking.**

When a round finishes (with or without failures), the event offset advances past **all** planned events for the round — not just the events from completed work units. Failed event ranges are abandoned.

```
Example:
  Round 0: events 1–10,000 planned (10 WU × 1000 events)
    - 8 WUs complete, 2 WUs fail
    - Event offset advances to 10,001 (past ALL planned events)
    - Events from failed WUs (e.g., 7001–8000, 9001–10000) are abandoned

  Round 1: events 10,001–110,000 planned
    - Plans enough new WUs to produce the remaining needed events
    - No attempt to "fill in" gaps from Round 0
```

**Why no gap tracking:**
- Event numbers need only be **unique**, not sequential. CMSSW uses event numbers for reproducibility (same number → same random seed → same output). Gaps in the sequence are harmless.
- 64-bit unsigned integer event numbers provide effectively unlimited space. Even at 10^12 events per request, there is no risk of exhaustion.
- Gap tracking would add complexity (tracking which ranges failed, replanning specific ranges) for no benefit.
- Uniqueness is guaranteed by always moving the offset forward.

**Completion condition**: `next_first_event >= request.total_events`. The request has produced enough events when the offset has advanced past the total requested, regardless of gaps.

### 5.2 Workflows With Primary Input

Input-driven workflows process a fixed set of files from an input dataset. Each file must be processed exactly once (or explicitly excluded). The recovery model tracks per-file state.

**File states:**

| State | Meaning | Priority |
|---|---|---|
| **Not yet processed** | Available for next round. Never been in a failed work unit. | Processed first |
| **Attempted** | Was in a failed work unit, but not identified as the cause of failure. | Processed after all fresh files |
| **Processed** | Part of a successful work unit. Done. | — |
| **Excluded** | Identified as unprocessable by POST script (bad data). Skipped permanently. | — |

**State transitions after a round with failures:**

```
Round N completes with some work units failed
  │
  For each failed work unit:
  │   ├── Read {node_name}.post.json
  │   ├── Files in classification.bad_input_files → Excluded
  │   └── Other files from this work unit → Attempted
  │
  For each successful work unit:
  │   └── All files → Processed
  │
  Next round planning:
  │   1. First: all "Not yet processed" files (fresh files, highest priority)
  │   2. Then: "Attempted" files (only after all fresh files are consumed)
  │   3. Skip: "Processed" and "Excluded" files
```

**Rationale for deferred retry of "Attempted" files**: If a work unit fails and the POST script identifies a specific bad file, only that file is excluded. The other files in the same work unit were not necessarily the problem — but they might have been. By processing all fresh files first, WMS2 maximizes throughput with known-good data. Attempted files are retried later, and if they fail again, the POST script has another chance to identify the actual problem.

**Run-aware grouping**: When planning work units for input-driven workflows, the DAG Planner makes a **best-effort** attempt to group files from the same CMS run together. CMSSW's `beginRun`/`endRun` processing is expensive — processing files from many different runs in a single job causes unnecessary run transitions. Grouping by run reduces this overhead. This is best-effort, not a hard constraint: if run grouping would leave some runs with very few files (below the work unit target), files are grouped by proximity instead.

**Completion condition**: All files are either Processed or Excluded. The request is COMPLETED when no files remain in "Not yet processed" or "Attempted" state.

---

## 6. Site Banning

### 6.1 Detection

After a DAG round completes (including rescue attempts), WMS2 aggregates POST script failure data by site:

```python
def analyze_site_failures(post_data_files):
    """Aggregate failures by site from POST script side files."""
    site_failures = defaultdict(int)
    site_total = defaultdict(int)
    for post_data in post_data_files:
        site = post_data["job"]["site"]
        site_total[site] += 1
        if post_data["classification"]["category"] in ("infrastructure", "data"):
            site_failures[site] += 1

    problem_sites = []
    for site, failures in site_failures.items():
        ratio = failures / site_total[site]
        if ratio > 0.5 and failures >= 3:  # >50% failure rate, minimum sample
            problem_sites.append(site)
    return problem_sites
```

A site is considered problematic for a workflow when it has both a high failure rate (>50%) and a minimum number of failures (to avoid banning on a single unlucky job).

### 6.2 Two-Level Banning

Site banning operates at two levels:

| Level | Scope | Trigger | Effect |
|---|---|---|---|
| **Per-workflow** | One workflow at one site | High failure rate for this workflow at this site | This workflow's future DAGs exclude the site |
| **System-wide** | All workflows at one site | Multiple independent workflows ban the same site | All new DAGs exclude the site |

**Promotion**: When `N` or more independent workflows have active per-workflow bans on the same site (where `N` = `site_ban_promotion_threshold`, default 3), WMS2 promotes the ban to system-wide. This indicates a site-level infrastructure problem rather than a workflow-specific issue.

### 6.3 Properties

- **Time-limited**: Default ban duration is 7 days (configurable via `site_ban_duration_days`). After expiration, the site is automatically eligible again.
- **Operator override**: An operator can remove any ban at any time via the API (`DELETE /api/v1/sites/{name}/ban`).
- **Not retroactive**: Banning a site does not affect already-running DAGs. It only affects future DAG planning and rescue DAG submissions.
- **Logged**: All ban events (creation, expiration, manual removal, promotion to system-wide) are logged for operational visibility.

### 6.4 Storage

Per-workflow bans and system-wide bans are stored in the database:

```sql
CREATE TABLE site_bans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    site_name VARCHAR(100) REFERENCES sites(name),
    workflow_id UUID REFERENCES workflows(id),  -- NULL for system-wide bans
    reason TEXT,
    failure_data JSONB,          -- Summary of failures that triggered the ban
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    removed_at TIMESTAMPTZ,      -- Set when manually removed
    removed_by VARCHAR(100)      -- Operator who removed the ban
);

CREATE INDEX idx_site_bans_active ON site_bans(site_name, expires_at)
    WHERE removed_at IS NULL;
CREATE INDEX idx_site_bans_workflow ON site_bans(workflow_id)
    WHERE workflow_id IS NOT NULL AND removed_at IS NULL;
```

### 6.5 DAG Planner Integration

When planning a DAG or submitting a rescue DAG, the DAG Planner queries active bans:

1. **System-wide bans**: All sites with active system-wide bans are excluded from the workflow's site list.
2. **Per-workflow bans**: Sites with active per-workflow bans for this specific workflow are excluded.

The excluded sites are written into submit files as negative `Requirements` expressions:

```
# In .sub file
Requirements = (TARGET.GLIDEIN_CMSSite =!= "T2_US_Purdue") && \
               (TARGET.GLIDEIN_CMSSite =!= "T1_DE_KIT") && \
               (other requirements...)
```

Or equivalently, if the workflow has a site whitelist, banned sites are simply removed from `DESIRED_Sites`.

---

## 7. Clean Stop Interaction

Clean stop (spec Section 6.2) interacts with error handling in two scenarios.

### 7.1 Between Rounds

When a clean stop is requested between rounds (no DAG running), the interaction is trivial:
- The request is in QUEUED or PLANNING state.
- No DAG to stop. Request stays in its current state.
- The operator can stop admission by moving the request to a held/paused state via API.

### 7.2 Mid-Round

When a clean stop is requested while a DAG is running:

```
Operator calls POST /api/v1/requests/{name}/stop
  │
  ├── condor_rm on DAGMan job
  │     DAGMan writes rescue DAG (.rescue00N) before exiting
  │     All completed nodes are marked DONE in rescue file
  │
  ├── WMS2 creates new DAG record (parent_dag_id = old DAG)
  │     Rescue DAG is the recovery artifact
  │
  ├── Request → STOPPING → RESUBMITTING → QUEUED
  │     (standard clean stop flow per spec Section 6.2)
  │
  └── On re-admission:
        Submit rescue DAG on same or different schedd
        (rescue DAG is schedd-portable — no schedd-specific state)
```

**Stop-rescues don't count against max rescue attempts**: The `max_rescue_attempts` limit (Section 3.2) is for failure-rescues only. Operator-initiated clean stops produce rescue DAGs as a mechanism for preserving progress, not as error recovery. The rescue count distinguishes between the two:

- **Failure-rescue**: DAG terminated due to node failures → counts against limit.
- **Stop-rescue**: DAG terminated due to `condor_rm` (clean stop) → does not count.

WMS2 distinguishes these by checking the DAG record: if `stop_requested_at` is set, it's a stop-rescue.

### 7.3 DAGMan Rescue DAG Is Cumulative

Rescue DAGs are cumulative — each rescue file marks all previously completed nodes as DONE, regardless of whether they completed in the original run, a failure-rescue, or a stop-rescue.

```
Original DAG: 100 WUs
  ├── 60 complete, 40 fail → .rescue001 (60 DONE)
  │
  Rescue 1 (failure): submit original .dag, DAGMan reads .rescue001
  ├── 30 more complete, 10 fail → .rescue002 (90 DONE)
  │
  Clean stop: condor_rm → .rescue003 (90 DONE, same as .rescue002)
  │
  Resume after stop: submit original .dag, DAGMan reads .rescue003
  ├── 10 remaining complete → COMPLETED (100 DONE)
```

This means the DAG file itself never changes — all state is in the rescue files and the DAGMan status tracking.

---

## 8. Configuration

### 8.1 Error Handler Settings

| Parameter | Default | Description |
|---|---|---|
| `error_hold_threshold` | 0.20 | Failure ratio above which request is HELD (replaces the three-tier rescue/review/abort split) |
| `error_max_rescue_attempts` | 3 | Maximum failure-rescue attempts before holding regardless of ratio |
| `site_ban_duration_days` | 7 | Default duration for automatic site bans |
| `site_ban_promotion_threshold` | 3 | Number of independent per-workflow bans before promoting to system-wide |
| `site_ban_min_failures` | 3 | Minimum failures at a site before it can be banned (avoids banning on one unlucky job) |
| `site_ban_failure_ratio` | 0.50 | Failure ratio at a site above which it is banned |

### 8.2 POST Script Settings

These are written into the `.dag` file by the DAG Planner:

| Parameter | Default | Description |
|---|---|---|
| `retry_count` (processing) | 3 | `RETRY proc_NNNNNN 3 UNLESS-EXIT 42` |
| `retry_count` (merge) | 2 | `RETRY merge_grp_NNN 2 UNLESS-EXIT 42` |
| `unless_exit_code` | 42 | Exit code for permanent failures |
| `abort_dag_code` | 43 | Exit code for DAG-wide abort (`ABORT-DAG-ON`) |
| `cooloff_base_sec` | 60 | Base sleep duration for transient retries (exponential backoff: `60 * 2^attempt`) |

### 8.3 Per-Request Overrides (Future)

In the future, operators may set per-request overrides when releasing a HELD request:

```json
{
  "retry_count": 5,
  "error_hold_threshold": 0.30,
  "site_bans": ["T2_US_Purdue"],
  "excluded_files": ["/store/data/bad_file.root"],
  "memory_mb": 6144
}
```

These overrides would be stored in the request's `request_data` JSONB field and applied by the DAG Planner when planning the next round or rescue. This mechanism is documented but not part of the initial implementation.

---

## Appendix: Design Decisions Summary

This appendix summarizes the key design decisions in this document and their rationale. These complement the design decisions in the main spec (Section 16).

| # | Decision | Rationale |
|---|---|---|
| EH-1 | Two-level model (POST+RETRY, WMS2+rescue), not three | Level 2 (DAG-wide HTCondor feature) is aspirational. Practical model uses POST scripts and rescue DAGs. |
| EH-2 | POST script is the brain, RETRY is the loop | DAGMan RETRY has no delays, no conditionals beyond UNLESS-EXIT. All intelligence in POST script. |
| EH-3 | HELD replaces "review" and "abort" | Single operator-attention state. Simpler state machine, same operational capability. |
| EH-4 | FAILED is manual only | Automatic failure is dangerous — transient infrastructure issues could cause 100% round failure on valid requests. Human judgment required. |
| EH-5 | 20% per-round threshold | Simple, configurable. Replaces three-tier (5%/30%) split. Will evolve with experience. |
| EH-6 | Rescue first, then next round | Rescue handles transient issues (cooloff, site flaps). Don't skip to next round prematurely. |
| EH-7 | No gap tracking for MC generation | Event offset always advances past all planned events. 64-bit numbers, no exhaustion risk. Gaps are harmless. |
| EH-8 | Per-file state tracking for input workflows | Four states (not yet processed / attempted / processed / excluded). Deferred retry of attempted files maximizes throughput. |
| EH-9 | Site banning is two-level (per-workflow + system-wide) | Per-workflow catches workflow-specific issues. Promotion to system-wide catches infrastructure problems. Time-limited with operator override. |
| EH-10 | Stop-rescues don't count against max rescue attempts | Operator-initiated stops are not errors. Rescue DAGs from stops preserve progress, not recover from failure. |
