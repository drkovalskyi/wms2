# WMS2 Processing Logic Specification

**OpenSpec v1.0**

| Field | Value |
|---|---|
| **Spec Version** | 0.1.0 |
| **Status** | DRAFT |
| **Date** | 2026-02-25 |
| **Authors** | CMS Computing |
| **Parent Spec** | docs/spec.md (WMS2 v2.7.0) |
| **Related** | docs/adaptive.md (Adaptive Execution v1.0.0) |

---

## 1. Overview

### 1.1 Purpose

This document specifies the processing logic that transforms a request into an executable DAG — the pipeline from "I want to process this dataset" to "here are the HTCondor submit files." It covers three sequential decisions:

1. **Splitting**: How many processing jobs, and what does each job process?
2. **Merge group formation**: How are those jobs grouped into work units?
3. **Processing block formation**: How are work units grouped for DBS/Rucio?

These decisions are interrelated and determine the fundamental structure of every DAG WMS2 produces. Getting them right is critical for resource utilization, tape efficiency, progress tracking granularity, and adaptive execution convergence.

### 1.2 Relationship to Other Specs

- **Main spec** (docs/spec.md): Defines the architectural framework — work units as SUBDAGs, processing blocks for DBS/tape, the lifecycle state machine. This document specifies the algorithms that determine *how many* of each and *what goes in them*.
- **Adaptive spec** (docs/adaptive.md): Defines how resource parameters are tuned across rounds. This document specifies how those tuned parameters feed back into splitting and grouping decisions.

### 1.3 Scope

**In scope**: Splitting algorithms, merge group sizing, processing block boundaries, resource parameter flow from request through DAG planning, interaction between splitting and adaptive execution, GEN vs. file-based workflow differences.

**Out of scope**: DAG file generation (main spec §4.5), sandbox internals, HTCondor scheduling, merge script implementation, site selection (landing nodes).

---

## 2. Input Parameters

Every processing decision starts from the request's parameters. These arrive from ReqMgr2 and are stored in the workflow row.

### 2.1 Splitting Parameters

| Parameter | Source | Description |
|---|---|---|
| `SplittingAlgo` | Request | Algorithm name: `EventBased`, `FileBased`, `LumiBased`, `EventAwareLumi` |
| `splitting_params` | Request | Algorithm-specific parameters (dict) |
| `InputDataset` | Request | DBS dataset path, empty for GEN workflows |
| `RequestNumEvents` | Request | Total events to generate (GEN only) |

**`splitting_params` by algorithm:**

| Algorithm | Key Parameters | Defaults |
|---|---|---|
| `EventBased` | `events_per_job` | 100,000 |
| `FileBased` | `files_per_job` | 5 |
| `LumiBased` | `lumis_per_job` | TBD |
| `EventAwareLumi` | `events_per_job`, `lumi_mask` | TBD |

### 2.2 Resource Parameters

| Parameter | Source | Round 1 | Round 2+ |
|---|---|---|---|
| `Memory` (total MB) | Request | Used directly | Replaced by measured + safety margin |
| `Multicore` | Request | Used directly | May be adjusted by job split |
| `TimePerEvent` | Request | Used for wall time estimate | Replaced by measured |
| `SizePerEvent` (KB) | Request | Used for `request_disk` estimate | Replaced by measured |
| `FilterEfficiency` | Request / StepChain | Adjusts effective event count | Replaced by measured |

**`SizePerEvent` clarification**: In WMCore/ReqMgr2, `SizePerEvent` is the estimated **total disk usage per event** (all output tiers combined), in kilobytes. It is used exclusively for HTCondor `request_disk` estimation: `request_disk = events_per_job × SizePerEvent`. It is NOT a per-tier output size and is NOT used for merge group sizing in WMAgent. Default: 512 KB/event. Typical range: 100–1500 KB/event.

WMS2 uses `SizePerEvent` the same way — for `request_disk` estimation only. Merge group sizing uses measured output sizes from Round 1 (see §4).

### 2.3 Output Parameters

| Parameter | Source | Description |
|---|---|---|
| `OutputDatasets` | Request / StepChain | List of output dataset specs (one per data tier) |

Note: Per-tier output sizes are NOT known at request time. They are measured from Round 1 output and used for Round 2 merge group sizing.

### 2.4 Merge Parameters

Configurable parameters for merge sizing, following WMCore conventions:

| Parameter | Default | Description |
|---|---|---|
| `min_merge_size` | 2 GB | Minimum merged output file size (per tier) |
| `max_merge_size` | 4 GB | Maximum merged output file size (per tier) |

These are operational settings, not per-request. They define the target window for merged file sizes. In non-adaptive workflows, they are not used (fixed `jobs_per_group`). In adaptive workflows, Round 2 uses them with measured per-tier output sizes to compute `jobs_per_group`.

### 2.5 Operational Parameters

WMS2-wide settings, not per-request:

| Parameter | Default | Description |
|---|---|---|
| `default_memory_per_core` | 2000 MB | Round 1 memory-per-core for maximum site pool |
| `max_memory_per_core` | 3000 MB | Upper bound for adaptive sizing |
| `safety_margin` | 0.20 | Percentage added to measured memory |
| `jobs_per_work_unit` | 8 | Fixed jobs per merge group (non-adaptive Round 0 and non-adaptive workflows) |
| `work_units_per_round` | 10 | Work units per processing round (adaptive workflows) |
| `target_wall_time_hours` | 8 | Target wall time per processing job (for Round 1+ events_per_job tuning) |
| `target_block_size_tb` | 1.0 | Target total output per processing block |

---

## 3. Stage 1: Splitting — Request to Processing Nodes

Splitting transforms a request's input (dataset or event count) into a list of processing nodes, where each node is one HTCondor job.

### 3.1 GEN Workflows (Event-Based, No Input Files)

GEN workflows generate events from scratch — there are no input files. The number of jobs is determined purely by the total event count and events per job.

**Inputs:**
- `RequestNumEvents` (total events to generate)
- `events_per_job` (from `splitting_params`)

**Algorithm:**
```
num_jobs = ceil(RequestNumEvents / events_per_job)

for i in 0..num_jobs-1:
    first_event = i × events_per_job + 1
    last_event  = min((i + 1) × events_per_job, RequestNumEvents)

    create DAGNodeSpec(
        node_index = i,
        first_event = first_event,
        last_event = last_event,
        events_per_job = last_event - first_event + 1,
        input_files = [synthetic placeholder],
    )
```

**Key property**: Every job gets the same number of events (except the last one, which gets the remainder). Jobs are independent — no data locality concerns.

**Example**: `RequestNumEvents=1000, events_per_job=100` → 10 jobs, each generating 100 events.

### 3.2 File-Based Workflows

File-based workflows process an existing dataset. Input files are fetched from DBS and grouped into jobs.

**Inputs:**
- `InputDataset` → DBS query → list of `InputFile(lfn, size, event_count, locations)`
- `files_per_job` (from `splitting_params`)

**Algorithm:**
```
1. Fetch files from DBS for InputDataset
2. Fetch replica locations from Rucio for each file
3. Group files by primary location (first replica site)
4. Within each site group, batch files into chunks of files_per_job

for each site_group:
    for i in 0..ceil(len(files)/files_per_job)-1:
        batch = files[i*files_per_job : (i+1)*files_per_job]
        create DAGNodeSpec(
            node_index = global_idx++,
            input_files = batch,
            primary_location = site,
        )
```

**Key property**: Files from the same site are grouped together for data locality. The landing node mechanism may override this at runtime — `primary_location` is a hint, not a hard assignment.

### 3.3 Event-Based Splitting (with Input Files)

For workflows that need precise event boundaries rather than file boundaries. Splits files across job boundaries by event count.

**Inputs:**
- `InputDataset` → list of `InputFile`
- `events_per_job` (from `splitting_params`)

**Algorithm:**
```
Walk through files sequentially.
Accumulate events until events_per_job is reached.
A file may be split across two jobs (via first_event/last_event offsets).
```

### 3.4 Lumi-Based Splitting

**Status: Not yet specified.** Required for reprocessing workflows where specific luminosity sections must be processed. The splitter would use a lumi mask (run:lumi ranges) to partition input files.

### 3.5 Splitting Output

All splitters produce a list of `DAGNodeSpec`:

```python
@dataclass
class DAGNodeSpec:
    node_index: int              # Sequential index (0, 1, 2, ...)
    input_files: list[InputFile] # Files this job processes (may be synthetic for GEN)
    first_event: int = 0         # Event range start (GEN and EventBased)
    last_event: int = 0          # Event range end
    events_per_job: int = 0      # Events this job processes
    primary_location: str = ""   # Preferred site (hint, not hard constraint)
```

**Invariant**: The union of all nodes' input files (or event ranges) must cover the entire input dataset (or total event count) exactly once. No gaps, no overlaps.

### 3.6 Lumi-Section Assignment

For GEN workflows, WMS2 assigns lumi-section numbers at DAG planning time. Each lumi-section must be unique within a given run number. The lumi assignment determines the granularity of data accounting downstream — a lumi-section is the atomic unit that cannot be split across multiple output files.

**Two granularities:**

| Mode | Assignment | Output Files per WU per Tier | Use Case |
|---|---|---|---|
| `per_job` (default) | Each job gets a unique lumi number | N files (one per job, merged into one) | Normal workflows |
| `per_work_unit` | All jobs in a WU share one lumi number | 1 file (cannot split a lumi) | Very low filter efficiency |

**`per_job` mode** (default):

```
WU 0: Job 0 → lumi 1, Job 1 → lumi 2, ..., Job 7 → lumi 8
WU 1: Job 8 → lumi 9, Job 9 → lumi 10, ..., Job 15 → lumi 16
...
```

Each job produces output with its own lumi. After merge, the merged file contains N lumis. Safe and flexible — merged files can be logically partitioned by lumi for downstream processing.

**`per_work_unit` mode:**

```
WU 0: Job 0 → lumi 1, Job 1 → lumi 1, ..., Job 7 → lumi 1
WU 1: Job 8 → lumi 2, Job 9 → lumi 2, ..., Job 15 → lumi 2
...
```

All jobs in a work unit share one lumi. After merge, the merged output for that WU is a single file per tier (the lumi cannot be split across files). This forces one-file-per-tier-per-WU, which is suboptimal for large tiers but necessary for very low filter efficiency workflows.

**Why per-WU lumi exists**: At extreme filter efficiencies (1e-5 to 1e-6), each job produces very few output events — often zero. Empty lumi-sections are not invalid — they represent part of the cross-section of the physical process and must be tracked and accounted for. But having thousands of nearly-empty lumis creates a bookkeeping burden: DBS must catalog each lumi, analysis frameworks must iterate over them, and lumi-based data accounting becomes dominated by empty entries. Per-WU lumi aggregates all input events under one lumi, reducing the total lumi count by the number of jobs per group (e.g., 8×) and producing lumis with more reasonable event counts.

Even per-WU lumi may not be enough: at filter efficiency 1e-6 with 80K input events per WU (8 jobs × 10K events), the expected output is 0.08 events per WU. Reaching 1 output event per lumi requires ~1M input events per lumi, which means either very large `events_per_job` or very large `jobs_per_work_unit`.

**Adaptive lumi decision**: Default to `per_job`. After the pilot round, if measured filter efficiency is below a threshold (e.g., 1e-3), Round 2 can switch to `per_work_unit`. This is a flag in the DAG planning parameters, passed to the sandbox via the manifest.

**Lumi number assignment**: Sequential integers starting from 1, unique within the run number. For `per_job` mode: `lumi = job_index + 1`. For `per_work_unit` mode: `lumi = work_unit_index + 1`. The run number is assigned per request (from ReqMgr2 or generated by WMS2).

---

## 4. Stage 2: Merge Group Formation — Processing Nodes to Work Units

Processing nodes are grouped into **merge groups** (work units). Each merge group becomes a SUBDAG EXTERNAL containing a landing node, processing nodes, a merge node, and a cleanup node.

### 4.1 Design Goals

Merge group sizing must balance several competing concerns:

1. **Merged output size**: Each merge group produces one merged output file per data tier. Merged files should be 2–4 GB for efficient storage, transfer, and tape writing. Too small → file proliferation and metadata overhead. Too large → long merge times and large disk footprint during merge.

2. **Progress granularity**: Work units are the atomic unit of progress reporting and partial production. Too few work units → coarse progress tracking and poor partial production control. Too many → overhead from landing nodes and merge operations.

3. **Tape efficiency**: Processing blocks (groups of work units) are the tape archival unit. Merge group size affects how many work units fit in a block, which affects tape write size.

4. **Site utilization**: Each merge group runs at one site. Groups should have enough jobs to meaningfully use site resources, but not so many that one group monopolizes a site.

5. **Adaptive convergence**: The first work unit (WU0) provides metrics for tuning subsequent work units. If WU0 has too few jobs, the metrics sample is small. If WU0 has too many jobs, most of Round 1 runs with untuned parameters.

### 4.2 Sizing Strategy

**Resolved: Two-mode approach based on `adaptive` flag.**

Merge group sizing depends on whether the workflow uses adaptive execution:

#### Non-Adaptive Workflows (`adaptive=False`, default)

Single DAG, fixed `jobs_per_group` from config:

```
jobs_per_group = config.jobs_per_work_unit  # default: 8
```

Simple, predictable, no estimates needed. Suitable for small requests, simple workflows, and testing. The merged output size varies with the workflow, but this is acceptable when optimization is not a goal.

#### Adaptive Workflows (`adaptive=True`)

Two-DAG lifecycle. Round 1 uses the same fixed `jobs_per_group` as non-adaptive. Round 2 computes `jobs_per_group` from **measured output sizes** — not estimates.

**Round 1 (pilot DAG):**
```
jobs_per_group = config.jobs_per_work_unit  # same fixed default
num_work_units = config.pilot_work_units    # default: 10
total_pilot_jobs = num_work_units × jobs_per_group
```

Round 1 processes a small fraction of the total work (e.g., 80 jobs out of 10,000). The goal is not optimal output — it's collecting measured data: per-tier output sizes, memory, CPU efficiency, wall time.

**Round 2 (production DAG):**

After Round 1 completes, actual per-tier output sizes are known from the merged output files. The DAG Planner computes `jobs_per_group` to hit the `[min_merge_size, max_merge_size]` window for the largest-output tier:

```
measured_output_per_job = actual_merged_size / jobs_in_group  # from Round 1, largest tier
target_merged_size = (min_merge_size + max_merge_size) / 2   # midpoint, e.g., 3 GB

jobs_per_group = round(target_merged_size / measured_output_per_job)
jobs_per_group = clamp(jobs_per_group, 1, max_jobs_per_group)
```

Round 2 plans a **new DAG** (not a rescue DAG) for the remaining work with the optimized `jobs_per_group`, plus tuned memory, thread counts, and other adaptive parameters.

**Which tier drives sizing?** The largest-output tier (typically GEN-SIM or RECO). Smaller tiers (MINIAODSIM, NANOAODSIM) accept undersized merged files. This is acceptable:
- Merge for small files is trivial
- DBS/Rucio handle any file size
- Tape write granularity depends on the block (group of work units), not individual files

**Why measured data, not estimates?** `SizePerEvent` from the request is total disk usage across all tiers — not per-tier output size. There is no reliable per-tier size estimate at request time. Round 1 produces actual files whose sizes we can measure directly. This is analogous to how WMAgent's merge algorithm (`WMBSMergeBySize`) uses actual file sizes rather than predictions.

#### Design Decision: DD-P1

**Decision**: Merge group sizing uses a fixed default for non-adaptive workflows and measured-data-based sizing for adaptive workflows (Round 2). The `adaptive` flag on the request controls which mode is used.

**Why**: Per-tier output sizes are not known at request time. `SizePerEvent` is an aggregate disk estimate, not a per-tier output prediction. Rather than building complex estimation logic for Round 1, we accept that Round 1 uses a simple fixed default and optimize in Round 2 with real data. This matches the adaptive philosophy: don't try to predict, measure and adjust.

**Why not estimate-based for Round 1**: WMAgent's approach (merge after the fact from actual file sizes) works because WMAgent dynamically creates merge jobs. WMS2 pre-plans merge groups into SUBDAGs. Without per-tier size data, any Round 1 sizing is a guess. A fixed default is honest about that.

### 4.3 Group Formation Algorithm

The formation algorithm is a simple sequential partitioning:

```python
def form_merge_groups(nodes: list[DAGNodeSpec], jobs_per_group: int) -> list[MergeGroup]:
    groups = []
    for i in range(0, len(nodes), jobs_per_group):
        chunk = nodes[i : i + jobs_per_group]
        groups.append(MergeGroup(
            group_index=len(groups),
            processing_nodes=chunk,
        ))
    return groups
```

**Key invariant**: Group composition is fixed within a DAG. It never changes during DAG execution. The same jobs always feed the same merge node.

**Between DAGs**: When `adaptive=True`, Round 2's DAG has different group composition than Round 1's. This is a new DAG, not a modification of the old one. DAGMan requires all SUBDAG EXTERNAL entries to be defined at submission time — you cannot add, remove, or restructure nodes in a running DAG. A rescue DAG reuses the exact same DAG file. Changing `jobs_per_group` requires a new DAG.

### 4.4 Work Unit Structure

Each merge group produces one SUBDAG EXTERNAL containing:

```
Landing (1 job)  →  Processing (N jobs)  →  Merge (1 job)  →  Cleanup (1 job)
```

- **Landing**: `/bin/true`, elects site via HTCondor negotiation
- **Processing**: N jobs running the payload (cmsRun or simulator)
- **Merge**: Combines outputs from N processing jobs (hadd for ROOT, concatenation for other formats)
- **Cleanup**: Removes unmerged intermediate files, writes output manifest

**Node count per group**: N + 3 (N processing + landing + merge + cleanup)
**Edge count per group**: 2N + 1 (landing→all_proc + all_proc→merge + merge→cleanup)

### 4.5 Total Job Count Derivation

The total number of processing jobs in a DAG is fully determined by splitting:

| Workflow Type | Formula | Example |
|---|---|---|
| GEN | `ceil(RequestNumEvents / events_per_job)` | 100K events / 100 epj = 1000 jobs |
| FileBased | `ceil(len(input_files) / files_per_job)` | 500 files / 5 fpj = 100 jobs |
| EventBased | `ceil(total_events / events_per_job)` | 10M events / 100K epj = 100 jobs |

The total number of work units is then:
```
num_work_units = ceil(num_processing_jobs / jobs_per_group)
```

The total DAG nodes:
```
total_nodes = num_work_units × (jobs_per_group_actual + 3)
```
where `jobs_per_group_actual` varies for the last group if `num_jobs % jobs_per_group != 0`.

---

## 5. Stage 3: Processing Block Formation — Work Units to Blocks

Processing blocks group work units for DBS block registration and tape archival.

### 5.1 Current Design: One Block per Output Dataset

The current implementation creates one processing block per output dataset, with all work units in that block:

```python
for i, dataset in enumerate(output_datasets):
    create_processing_block(
        workflow_id=workflow.id,
        block_index=i,
        dataset_name=dataset["dataset_name"],
        total_work_units=len(merge_groups),  # ALL work units
    )
```

For a 5-step StepChain with 100 work units, this creates 5 blocks × 100 WUs each.

### 5.2 Block Lifecycle

```
OPEN: First work unit in the block completes.
      → DBS: open_block() called
      → DBS: register_files() for each completing work unit
      → Rucio: create_rule() source protection per work unit

COMPLETE: All work units in the block have completed.
      → DBS: close_block() called
      → Rucio: create_rule() tape archival for entire block

ARCHIVED: Tape archival rule confirmed created.
```

### 5.3 Block Sizing Considerations

**OQ-P2: Should processing blocks contain all work units, or should large workflows be split into multiple blocks per dataset?**

The current "all WUs in one block" approach has a problem for large workflows: if a workflow has 1000 work units and each produces 4 GB merged output (per the largest tier), the block contains 4 TB. This is fine for tape (18 TB tapes), but:

- DBS block closure is delayed until ALL 1000 work units complete. If the workflow takes weeks (with partial production pauses), the block stays open for weeks — data sits on source sites unprotected by tape.
- If one work unit fails and blocks completion, the entire block is stuck.

**Possible block sizing strategies:**

| Strategy | Block Size | Pros | Cons |
|---|---|---|---|
| All-in-one | 1 block per dataset | Simplest | Large blocks, delayed tape archival |
| Fixed WU count | e.g., 50 WUs per block | Predictable | Doesn't adapt to output size |
| Target output size | e.g., 1 TB per block | Tape-friendly | Depends on size estimates |
| Time-based | Close block after N hours | Limits exposure | Arbitrary, size varies |

**Tape considerations** (from main spec §DD-5):
- 18 TB tape capacity
- 2–4 GB merged files
- Want contiguous writes for efficient recall
- Tape buffers handle 1–2 day consolidation windows
- Target: blocks large enough for efficient tape writes (~1 TB), small enough to close within days

### 5.4 Pilot Round and Processing Blocks

In adaptive workflows, Round 1 (pilot) produces fewer work units than a typical production round. Its processing blocks are smaller — e.g., 10 WUs producing ~40 GB vs. hundreds of WUs producing ~1 TB in Round 2 blocks.

**Decision (DD-P2)**: Pilot WUs go into normal processing blocks. Each DAG creates its own self-contained blocks. No cross-DAG block state.

- Round 1 creates blocks with `total_work_units = pilot_work_units`. When all pilot WUs complete, blocks close normally → DBS block closure → tape archival rule.
- Round 2 creates separate blocks for its work units.

This keeps the OutputManager completely unaware of the two-DAG lifecycle. The invariant holds: one DAG's blocks are self-contained.

The small pilot block is slightly tape-inefficient (writing ~40 GB to an 18 TB tape). This is acceptable — it's one block out of potentially dozens, and the data gets taped quickly.

**Future optimization**: A separate tape consolidation service could batch small processing blocks from different workflows/rounds into combined archival requests, improving tape write efficiency without adding complexity to WMS2's core output pipeline. This is out of scope for now.

---

## 6. Resource Parameter Flow

Resource parameters flow from the request through the DAG Planner to HTCondor submit files. The multi-round adaptive lifecycle can change every parameter between rounds.

### 6.1 Non-Adaptive Workflow (Single DAG)

```
Request Spec                         DAG Planner                    Submit File
─────────────                        ───────────                    ───────────
Memory: 16000 MB          →    request_memory = max(             →  request_memory = 16000
Multicore: 8                       Memory,
                                   default_mem_per_core × Multicore)

TimePerEvent: 12 sec       →    wall_time = TimePerEvent ×        →  +MaxWallTimeMins = 334
                                   events_per_job

SizePerEvent: 512 KB       →    request_disk = SizePerEvent ×     →  request_disk = 52428800
                                   events_per_job

SplittingAlgo: EventBased  →    events_per_job = 100K             →  (affects number of jobs)
events_per_job: 100000           num_jobs = ceil(N / 100K)
                                 jobs_per_group = config default (8)
```

All parameters come from request hints. `SizePerEvent` is used only for `request_disk`. Merge group sizing uses the fixed `jobs_per_work_unit` config value.

### 6.2 Adaptive Workflow — Multi-Round Lifecycle

Adaptive workflows process work in fixed-size rounds. Each round is a separate DAG with `work_units_per_round` WUs (default 10). After each round completes, the request returns to the admission queue. The dispatcher decides when and at what priority the next round runs.

**Round 0 (pilot):**

Same parameter flow as non-adaptive. Uses request hints, fixed `jobs_per_work_unit`. The DAG is small:

```
total_pilot_jobs = work_units_per_round × jobs_per_work_unit  # 10 × 8 = 80
pilot_events = total_pilot_jobs × events_per_job               # 80 × 10K = 800K
```

Round 0's purpose is metrics collection: memory, CPU efficiency, time-per-event, per-tier output sizes. When it completes, the Lifecycle Manager aggregates metrics and returns the request to the queue.

**Round 1+ (production rounds):**

After Round 0, measured data is available. Each subsequent round is planned with optimized parameters:

```
Measured Data                        DAG Planner                    Submit File
────────────────                     ───────────                    ───────────
peak_rss_mb: 12000           →  request_memory =                  →  request_memory = 14400
                                  peak_rss × (1 + safety_margin)
                                  clamped to [default, max] window

measured_time_per_event      →  events_per_job =                   →  (affects job count)
                                  target_wall_time / measured_tpe

measured_output_per_job      →  jobs_per_group = target /          →  (affects DAG structure)
  (per tier, from real files)     measured_output_per_job

cpu_efficiency: 0.65         →  (passed to sandbox via            →  (sandbox reads
effective_cores: 5.2              step_profile.json)                  step_profile.json)
```

Each production round processes at most `work_units_per_round` WUs of remaining work. If remaining work is less, the round is smaller (final round).

**Optimization is continuous.** Each round's metrics are merged with previous rounds. If Round 1 reveals something Round 0 didn't (e.g., different input data characteristics for file-based workflows), Round 2 can re-tune. In practice, parameters stabilize after Round 0 — production rounds use the same parameters unless metrics diverge.

### 6.3 Round Lifecycle

```
adaptive=true:

  Round 0: QUEUED → ACTIVE (pilot, 10 WUs, request defaults) → complete
           → collect metrics → QUEUED (back to admission queue)

  Round 1: QUEUED → ACTIVE (10 WUs, optimized params) → complete
           → update metrics → QUEUED

  Round 2: QUEUED → ACTIVE (10 WUs, same or re-tuned params) → complete
           → QUEUED

  ...

  Round K: QUEUED → ACTIVE (remaining ≤10 WUs) → complete
           → all work done → COMPLETED

adaptive=false:

  QUEUED → ACTIVE (single DAG, all work, request defaults) → COMPLETED
```

**Key properties:**

- **Rounds run to completion.** Once a round starts, it finishes — no priority-based preemption mid-round. This prevents the starvation problem where partially-processed requests hang because their jobs lost priority.
- **FIFO within a round, priority between rounds.** The admission controller dispatches the next round based on priority. A low-priority request waits its turn after each round completes.
- **Fixed round size limits resource commitment.** 10 WUs × optimized jobs_per_group jobs is a bounded commitment. Other requests get their turn after each round.
- **Partial production is subsumed.** Priority demotion (`production_steps`) applies between rounds when the request re-enters the queue. No need for clean stop — the round boundary is the natural scheduling point.
- **Each DAG is self-contained.** Processing blocks, output management, and metrics are per-DAG. No cross-DAG state.

The workflow row's `dag_id` points to the current DAG. Previous DAGs remain in the database as history. Completed work units from earlier rounds have already been processed through OutputManager — they are safe regardless of subsequent rounds.

For GEN workflows, event ranges are non-overlapping by construction (each round starts where the previous ended). For file-based workflows, files processed in earlier rounds are excluded from subsequent rounds' splitting input.

### 6.4 Adaptive Job Split

The adaptive system may decide to run **more jobs with fewer cores** in Round 1+ (see adaptive.md §4.2). Since every round is a new DAG, this is naturally supported:

```
Round 0: 8 cores/job, 80 jobs (10 WUs × 8 jobs), request defaults
Round 1: 4 cores/job, different jobs_per_group from measured data
         New DAG, fully replanned
```

No conflict with merge group composition — each round's groups are planned fresh. Sandbox-level per-step splitting (running multiple cmsRun within one slot) is a complementary optimization that works within all rounds.

### 6.5 DAG Construction per Round

Each round's DAG is built from scratch containing **only** the work units for that round. The DAG Planner does not build a large DAG and throttle it — it builds exactly `work_units_per_round` WUs (or fewer for the final round).

**Design decision (DD-P3):** Build only the current round's WUs in each DAG. The workflow row tracks cumulative progress across rounds. Each DAG runs to natural completion — no `condor_rm`, no clean stop, no rescue DAG.

**Why not build all remaining WUs and throttle?**
- Generates DAG files for WUs that won't run this round (wasted I/O)
- Requires `condor_rm` to stop the DAG after 10 WUs complete, adding clean stop complexity
- Rescue DAG semantics conflict with the "always new DAG" design
- Parameters may change between rounds (optimization is continuous) — pre-planned WUs would use stale parameters

**Round bookkeeping — GEN workflows:**

The workflow row tracks a single counter: `next_first_event`.

```
Round 0: next_first_event = 1
         Plan 10 WUs: events 1–800,000
         After completion: next_first_event = 800,001

Round 1: next_first_event = 800,001
         Plan 10 WUs: events 800,001–1,952,000
         After completion: next_first_event = 1,952,001

...
```

The DAG Planner reads `next_first_event` and `RequestNumEvents` to determine remaining work. If `next_first_event > RequestNumEvents`, all work is done.

**Round bookkeeping — file-based workflows:**

The workflow row tracks which input files have been processed. Two approaches:

1. **Cursor-based**: Input files are ordered deterministically (e.g., by LFN). Track the index of the next unprocessed file. Simple but assumes file list doesn't change between rounds.
2. **Set-based**: Track the set of processed LFNs. More robust (handles file list changes between rounds, e.g., new replicas) but requires more storage.

For MVP, cursor-based is sufficient. The file list is fetched once from DBS at request submission and stored on the workflow. Each round's splitter starts at the cursor position.

```
Round 0: cursor = 0, files_per_job = 5, jobs_per_work_unit = 8
         Process files 0–399 (80 jobs × 5 files)
         After completion: cursor = 400

Round 1: cursor = 400, optimized files_per_job and jobs_per_group
         Process files 400–499 (next batch)
         ...
```

**Remaining work calculation:**

```python
# GEN
remaining_events = request_num_events - workflow.next_first_event + 1
remaining_jobs = ceil(remaining_events / events_per_job)
round_work_units = min(work_units_per_round, ceil(remaining_jobs / jobs_per_group))
round_jobs = round_work_units * jobs_per_group  # last WU may have fewer

# File-based
remaining_files = total_files - workflow.file_cursor
remaining_jobs = ceil(remaining_files / files_per_job)
round_work_units = min(work_units_per_round, ceil(remaining_jobs / jobs_per_group))
```

---

## 7. End-to-End Flow

### 7.1 Non-Adaptive GEN Workflow

```
RequestNumEvents: 1,000,000    adaptive: false
events_per_job: 10,000         Multicore: 8
SizePerEvent: 512 KB           Memory: 16000
OutputDatasets: [GEN-SIM, DIGI, RECO, MINIAODSIM, NANOAODSIM]
```

**Single DAG:**
```
Splitting: num_jobs = ceil(1,000,000 / 10,000) = 100 processing jobs
Grouping:  jobs_per_group = 8 (config default)
           num_work_units = ceil(100 / 8) = 13
Blocks:    5 output datasets → 5 processing blocks, each with 13 WUs
Totals:    100 proc + 13 landing + 13 merge + 13 cleanup = 139 nodes
```

### 7.2 Adaptive GEN Workflow

Same request, but `adaptive: true`:

```
RequestNumEvents: 10,000,000   adaptive: true
events_per_job: 10,000         Multicore: 8
SizePerEvent: 512 KB           Memory: 16000
OutputDatasets: [GEN-SIM, DIGI, RECO, MINIAODSIM, NANOAODSIM]
Total jobs at request params: ceil(10,000,000 / 10,000) = 1000
```

**Round 0 (pilot):**
```
work_units_per_round = 10, jobs_per_work_unit = 8
total_pilot_jobs = 80, pilot_events = 800,000

DAG 0: 80 proc + 10 landing + 10 merge + 10 cleanup = 110 nodes
Processes events 1–800,000 (8% of total)
→ complete → collect metrics → back to queue
```

**After Round 0 — measured data:**
```
Actual GEN-SIM output per job: 620 MB (from merged files)
Measured time_per_event: 0.5 sec (request said 1.0)
Measured peak RSS: 12,000 MB
Measured CPU efficiency: 0.65
```

**Round 1 (first production round):**
```
Optimized events_per_job:
  target_wall_time = 8h = 28,800 sec
  events_per_job = floor(28,800 / 0.5) = 57,600

Optimized jobs_per_group:
  measured_output_per_job = 620 MB × (57,600 / 10,000) = 3,571 MB
  target = (2 GB + 4 GB) / 2 = 3 GB
  jobs_per_group = round(3,000 / 3,571) = 1  → clamp to min 2
  jobs_per_group = 2

work_units_per_round = 10 → 20 jobs × 57,600 events = 1,152,000 events
request_memory = 12,000 × 1.20 = 14,400 MB

DAG 1: 20 proc + 10 landing + 10 merge + 10 cleanup = 50 nodes
Processes events 800,001–1,952,000
→ complete → back to queue
```

**Round 2–7:** Same parameters, 10 WUs each, ~1.15M events per round.

**Round 8 (final):** Remaining ~740K events → 13 jobs → 7 WUs (< 10, final round).

**Totals across all rounds:**
```
Round 0:   80 jobs, 800K events    (unoptimized, pilot)
Round 1-7: 140 jobs, 8.06M events  (optimized, 7 rounds × 20 jobs)
Round 8:   13 jobs, 740K events    (optimized, final)
Total:     233 jobs, 10M events    (vs. 1000 jobs at original events_per_job)
```

Note: Optimizing `events_per_job` from 10K to 57.6K reduced total jobs from 1000 to 233 — fewer, longer jobs are more efficient.

### 7.3 File-Based Reprocessing (Non-Adaptive)

```
InputDataset: /PrimaryDS/Campaign/RAW (500 files, ~2 GB each, ~50K events each)
files_per_job: 5               adaptive: false
SizePerEvent: 1500 KB          Multicore: 4
Memory: 8000
OutputDatasets: [RECO, MINIAODSIM]
```

**Single DAG:**
```
Splitting: num_jobs = ceil(500 / 5) = 100 processing jobs
Grouping:  jobs_per_group = 8 (config default)
           num_work_units = ceil(100 / 8) = 13
Blocks:    2 output datasets → 2 processing blocks, each with 13 WUs
```

### 7.4 Small Test Workflow

```
RequestNumEvents: 40           adaptive: false
events_per_job: 10             jobs_per_work_unit: 2 (test override)
OutputDatasets: [GEN-SIM, DIGI, RECO, MINIAODSIM, NANOAODSIM]
```

```
Splitting: num_jobs = ceil(40 / 10) = 4
Grouping:  jobs_per_group = 2 (overridden for testing)
           num_work_units = 2
Blocks:    5 datasets → 5 processing blocks, each with 2 WUs
```

For integration tests that need to exercise multi-WU features (progress tracking, block gating, output pipeline), override `jobs_per_work_unit` to produce multiple work units from a small job count.

---

## 8. Open Questions and Resolved Decisions

### RESOLVED: OQ-P1 — Merge Group Sizing Strategy

**Decision**: Two-mode approach controlled by `adaptive` flag. See §4.2 (DD-P1).

- **Non-adaptive** (`adaptive=false`): Fixed `jobs_per_work_unit` from config (default 8). Simple, no estimates.
- **Adaptive** (`adaptive=true`): Round 1 uses fixed default (pilot). Round 2 computes `jobs_per_group` from measured per-tier output sizes, targeting the `[min_merge_size, max_merge_size]` window (default 2–4 GB).

### RESOLVED: OQ-P3 — Job Split and Merge Group Composition

**Decision**: Not a problem. Since every round is a new DAG, job split is naturally supported — each round's DAG is planned from scratch with different job count, core count, and group sizes. See §6.4.

### RESOLVED: OQ-P4 — Which Tier Drives Merge Group Sizing

**Decision**: Largest-output tier (typically GEN-SIM or RECO). Smaller tiers accept undersized merged files. See §4.2.

### RESOLVED: OQ-P5 — First Work Unit Sizing

**Decision**: All Round 0 work units have the same `jobs_per_group` (fixed default). Probe nodes within WU0 provide high-quality metrics with higher memory allowance (adaptive.md §5). No special sizing for WU0.

### RESOLVED: OQ-P6 — Events-Per-Job Tuning Across Rounds

**Decision**: `events_per_job` DOES change in Round 1+ for adaptive workflows. The target is a configurable wall time per job (default 8 hours). Production rounds compute:

```
events_per_job = floor(target_wall_time / measured_time_per_event)
```

**Why**: There is no special meaning to the request's `events_per_job` — it's an initial estimate. The goal is jobs big enough to maximize efficiency (amortize startup overhead, fill the slot) but not so big that wall time becomes problematic. An 8-hour target balances these concerns: long enough for efficiency, short enough for reasonable turnaround and preemption recovery.

**Interaction with lumi-section assignment**: Changing `events_per_job` changes the number of events per lumi (in `per_job` mode) or per work unit (in `per_work_unit` mode). For low filter efficiency workflows, the adaptive system considers both `events_per_job` and lumi granularity together — see §3.6.

**Interaction with merge group sizing**: More events per job → larger output per job → fewer jobs per merge group (to stay in the 2–4 GB window). Fewer events per job → smaller output per job → more jobs per merge group. Both `events_per_job` and `jobs_per_group` are recomputed together from measured data.

### RESOLVED: OQ-P8 — Round Sizing

**Decision**: Fixed `work_units_per_round` (default 10) for all rounds, including Round 0 (pilot). The admission controller controls pacing between rounds via the queue. See §6.3.

If remaining work is less than `work_units_per_round`, the final round is smaller. If a non-adaptive workflow's total work fits in one round, it runs as a single DAG.

### RESOLVED: OQ-P9 — Partial Production Interaction

**Decision**: Partial production is subsumed by the multi-round lifecycle. Every round returns the request to the admission queue. Priority demotion (`production_steps`) is applied when the request re-enters the queue based on cumulative events completed. No clean stop needed — round boundaries are the natural scheduling points. See §6.3.

Fractions are tracked in events (GEN) or files (file-based), accumulated across all rounds. When the cumulative fraction crosses a `production_steps` threshold, the admission controller applies the priority demotion before the next round is dispatched.

### OQ-P2: Processing Block Boundaries

Should large workflows split into multiple blocks per dataset? See §5.3.

**Current**: All work units in a round → one block per dataset per round (DD-P2).
**Note**: With multi-round lifecycle, blocks are naturally bounded by round size (10 WUs per round). This may be sufficient for tape efficiency without additional block splitting logic.

**Deferred**: Revisit if 10-WU blocks are too small for efficient tape writes. May need to consolidate blocks across rounds via a separate tape optimization service (see §5.4).

**Related**: Main spec OQ-19, OQ-20.

### OQ-P7: Max Jobs per Workflow

Is there a practical upper limit on total processing jobs per DAG?

With `work_units_per_round=10`, each DAG has at most 10 × `jobs_per_group` processing nodes. At the default `jobs_per_work_unit=8`, that's 80 processing nodes per DAG — well within DAGMan's comfort zone. Even with optimized `jobs_per_group` in production rounds (e.g., 50 jobs per group), a 10-WU DAG has 500 processing nodes — trivial for DAGMan.

**Largely resolved by multi-round design.** The fixed round size naturally caps DAG size. The total workflow may have millions of events, but each DAG is small.

**Remaining concern**: Non-adaptive workflows (`adaptive=false`) run as a single DAG with all work. Very large non-adaptive workflows could hit DAGMan limits. This is acceptable because non-adaptive is for simple/small requests. Large production workflows should use `adaptive=true`.

**Related**: Main spec OQ-4.

---

## 9. Implementation Status

### 9.1 What Exists

| Component | Status | Notes |
|---|---|---|
| `FileBasedSplitter` | Implemented | `src/wms2/core/splitters.py` |
| `EventBasedSplitter` | Implemented | `src/wms2/core/splitters.py` |
| `LumiBasedSplitter` | Stub only | Raises `NotImplementedError` |
| `EventAwareLumiSplitter` | Stub only | Raises `NotImplementedError` |
| GEN node generation | Implemented | `dag_planner.py:_plan_gen_nodes()` |
| File-based node generation | Implemented | `dag_planner.py:_plan_file_based_nodes()` |
| Merge group formation | Implemented (fixed count) | `dag_planner.py:_plan_merge_groups()` |
| Processing block creation | Implemented (all-in-one) | `dag_planner.py:plan_production_dag()` |
| Merge group DAG generation | Implemented | `dag_planner.py:_generate_dag_files()` |
| `SizePerEvent` → `request_disk` | Implemented | `dag_planner.py` submit file generation |
| Multi-round adaptive lifecycle | **Not implemented** | Round 0 pilot → Round 1+ production → queue between rounds |
| Round bookkeeping | **Not implemented** | `next_first_event` (GEN) / `file_cursor` (file-based) on workflow row |
| Measured-output merge sizing | **Not implemented** | Round 1+ `jobs_per_group` from real file sizes |
| Lumi-section assignment | **Not implemented** | Per-job (default) or per-WU mode |
| `events_per_job` tuning | **Not implemented** | Round 1+ recomputes from measured time-per-event |
| Multi-block per dataset | **Not implemented** | Single block per dataset (deferred) |

### 9.2 Known Gaps

1. **No multi-round lifecycle**. The Lifecycle Manager treats every DAG as the only DAG for a workflow. It has no concept of "round completed, collect metrics, return to queue, plan next round." This is the primary implementation gap.

2. **No measured-output-based merge group sizing**. After Round 0 completes, there is no code to measure per-tier output sizes from merged files and compute optimized `jobs_per_group` for production rounds.

3. **No `events_per_job` recomputation**. `events_per_job` is fixed from the request. No code computes `target_wall_time / measured_time_per_event` for production rounds.

4. **No lumi-section assignment**. The DAG planner does not assign lumi numbers to jobs. The sandbox/cmsRun uses CMSSW's default lumi numbering.

5. **`SizePerEvent` is used for merge group sizing in the main spec** (§4.5) but should only be used for `request_disk`. The main spec needs updating to match the resolved design.

6. **Test matrix bypasses production split logic**. The test matrix's `WorkflowDef` manually specifies `num_jobs`, `events_per_job`, and `num_work_units` — the production `_plan_gen_nodes()` + `_plan_merge_groups()` path is only exercised when `production_path=True`.
