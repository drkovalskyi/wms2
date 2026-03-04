# WMS2 Adaptive Execution Algorithm Specification

**OpenSpec v2.0**

| Field | Value |
|---|---|
| **Spec Version** | 2.0.0 |
| **Status** | DRAFT |
| **Date** | 2026-03-04 |
| **Authors** | CMS Computing |
| **Parent Spec** | docs/spec.md (WMS2 v2.7.0) |

---

## 1. Overview

### 1.1 Purpose

This document specifies the algorithms behind the WMS2 adaptive
execution model described in main spec §5. The main spec defines
*what* the adaptive system does at an architectural level —
memory-per-core windows, execution rounds, per-step splitting, metric
aggregation, convergence. This document defines *how* — the concrete
algorithms, data structures, formulas, and constants sufficient to
reimplement the system from scratch.

### 1.2 Relationship to Main Spec

The main spec (§5, §DD-12, §DD-13) is authoritative for architectural decisions:

- WMS2 does not run separate pilot jobs; the first production DAG provides baseline metrics
- The adaptive algorithm operates within a `[default_memory_per_core, max_memory_per_core]` window
- Per-step splitting is a sandbox-owned optimization; WMS2 only provides metrics
- Memory sizing uses measured data + 20% safety margin, clamped within the window

This document specifies the algorithms that implement these
architectural decisions. Cross-references use the notation `main spec
§N` (e.g., `main spec §5.4` for per-step splitting).

### 1.3 Scope

**In scope**: Composable optimization dimensions (memory sizing, job
  splitting, internal parallelism, work group sizing), metric
  collection and aggregation, multi-round convergence, manifest
  patching, integration points.

**Out of scope**: Sandbox internals (cmsRun forking, event
  partitioning, gridpack extraction), HTCondor scheduling behavior,
  Framework Job Report (FJR) parsing, cgroup monitor implementation,
  simulator profiles for testing.

---

## 2. Inter-Round Optimization Model

### 2.1 Round Progression

The adaptive system uses multiple processing rounds, each as a
separate DAG. Round 0 is the probe — it runs with request defaults and
produces baseline metrics. Round 1+ is optimized based on measured
data.

```
Round 0: Request defaults → pilot DAG (1 WU) → metrics collected
              │
              ▼ (DAG completes, advance offsets)
         Lifecycle Manager: aggregate metrics → step_metrics
              │   compute_round_optimization() → adaptive_params
              ▼
Round 1: Optimized params → production DAG (10 WUs) → metrics refined
              │
              ▼ (if work remains, advance offsets)
Round K: Remaining work (≤10 WUs) → final DAG → COMPLETED
```

**Round 0** (`current_round=0`):
- Uses request resource defaults (Memory, TimePerEvent, Multicore) extracted from ReqMgr2
- Fixed `jobs_per_work_unit` (default 8) for merge group sizing
- Produces `first_round_work_units` work units (default 1) — small probe to validate the pipeline quickly before committing more resources
- Collects FJR metrics: per-step RSS, CPU efficiency, wall time, per-tier output sizes
- On completion: metrics aggregated into `workflow.step_metrics`, optimization computed for round 1+

**Round 1+** (`current_round>=1`):
- Memory sized to measured peak + safety margin. Peak value is tracked for all rounds, not only the latest.
- `request_cpus` may be reduced (job splitting) based on CPU efficiency, but has a limit on how small it can be to prevent HTCondor pool fragmentation.
- `jobs_per_work_unit` sized from measured per-tier output to target merged file size
- Each round builds only its share of work units

### 2.2 The Optimization Function

The orchestrator `compute_round_optimization()` composes independent
optimization dimensions rather than dispatching to one exclusive mode:

1. **Collect and merge metrics** from completed work units
2. **Size memory** from unified source hierarchy (cgroup → FJR RSS → default)
3. **Decide job splitting** (effective cores → `request_cpus` reduction, clamped to `min_request_cpus`)
4. **Compute internal parallelism** (step 0 instances at the new `request_cpus`)
5. **Return unified output** with all dimensions

Each dimension is independent: job splitting can combine with internal
parallelism, memory sizing applies regardless of splitting decisions,
and work group sizing is computed separately in the DAG planner.

### 2.3 Effective Cores

The fundamental metric driving splitting decisions is **effective
cores** — the number of CPU cores a step actually utilizes:

```
effective_cores = weighted_cpu_eff × nthreads
```

Where `weighted_cpu_eff` is the wall-time-weighted CPU efficiency across all steps:

```
weighted_cpu_eff = Σ(mean_eff_i × mean_wall_i) / Σ(mean_wall_i)
```

This weighting ensures that long-running steps (which dominate job
wall time) have proportionally more influence on the aggregate
efficiency. A 4-minute GEN step at 55% efficiency matters more than a
10-second NANO step at 15%.

The connection to Amdahl's law is direct: each CMSSW step has a serial
fraction (GEN ~30%, DIGI/RECO ~10%, NANO ~5%) that limits how much
parallelism the step can exploit. Effective cores measures the
realized parallelism — the input to all splitting and thread-tuning
decisions.

### 2.4 Thread Count Rounding — Power-of-2

Thread counts are always rounded to the nearest power of 2, using
**geometric mean midpoints** rather than arithmetic midpoints. The
function `_nearest_power_of_2(n)` implements this:

```
For consecutive powers p and 2p:
    midpoint = sqrt(p × 2p) = p × sqrt(2)
    if n <= midpoint: return p
    if n > midpoint:  return 2p
```

Minimum: 1. Maximum: 64.

**Geometric mean rounding table:**

| Input range | Midpoint | Output |
|---|---|---|
| 0.0–1.0 | — | 1 |
| 1.0–1.414 | sqrt(1×2) = 1.414 | 1 |
| 1.415–2.828 | sqrt(2×4) = 2.828 | 2 |
| 2.829–5.657 | sqrt(4×8) = 5.657 | 4 |
| 5.658–11.314 | sqrt(8×16) = 11.314 | 8 |
| 11.315–22.627 | sqrt(16×32) = 22.627 | 16 |
| 22.628–45.255 | sqrt(32×64) = 45.255 | 32 |
| 45.256+ | — | 64 |

**Rationale**: Geometric mean rounding is appropriate because thread
  counts are multiplicative — doubling from 4 to 8 threads is the same
  proportional change as doubling from 8 to 16. Arithmetic midpoints
  (e.g., 6 between 4 and 8) would bias toward larger values. The
  boundary at 5.657 means an effective core count of 5.6 rounds to 4,
  not 8 — this is conservative and avoids allocating threads that
  won't be productively used.

---

## 3. Metric Collection and Aggregation

### 3.1 Per-Work-Unit Metrics

The function `analyze_wu_metrics(group_dir, exclude_nodes)` reads metrics from a completed merge group.

**Input search paths** (in order, first with results wins):
1. The merge group directory itself (`mg_NNNNNN/`)
2. Unmerged storage paths derived from `output_info.json` — `{local_pfn_prefix}/{unmerged_lfn_base}/{group_index:06d}/`

**File pattern**: `proc_*_metrics.json` (regex: `proc_(\d+)_metrics\.json$`)

**Node exclusion**: When `exclude_nodes` is provided, matching files
  are skipped. Node names are normalized to integer indices for
  comparison — `proc_000001` and `proc_1_metrics.json` both resolve to
  index 1.

**Per-step field collection**: Each metrics file contains a list of
  step entries. For each step (keyed by `step_index`), the following
  fields are collected into lists:

| Field | Source key | Description |
|---|---|---|
| `wall_sec` | `wall_time_sec` | Wall clock time (seconds) |
| `cpu_eff` | `cpu_efficiency` | CPU efficiency (0.0–1.0) |
| `peak_rss_mb` | `peak_rss_mb` | Peak RSS in MB |
| `events` | `events_processed` | Events processed |
| `throughput` | `throughput_ev_s` | Throughput (events/sec) |
| `cpu_time_sec` | `cpu_time_sec` | CPU time (seconds) |
| `nthreads` | `num_threads` | Thread count used |

**Cross-step aggregation**:

```
peak_rss_mb     = max across all steps, all jobs (max of max)
weighted_cpu_eff = Σ(mean_eff_i × mean_wall_i) / Σ(mean_wall_i)
effective_cores  = weighted_cpu_eff × nthreads
```

Where `nthreads` is the maximum `num_threads` observed across all steps.

**Cgroup attachment**: After collecting FJR-based metrics,
  `load_cgroup_metrics()` is called on the same directory. If cgroup
  data exists, it is attached under the `"cgroup"` key.

**Return structure**:

```json
{
  "steps": {
    0: {"wall_sec": [...], "cpu_eff": [...], "peak_rss_mb": [...], ...},
    1: {"wall_sec": [...], "cpu_eff": [...], "peak_rss_mb": [...], ...}
  },
  "peak_rss_mb": 2400.0,
  "weighted_cpu_eff": 0.65,
  "effective_cores": 5.2,
  "num_jobs": 4,
  "nthreads": 8,
  "cgroup": { ... }
}
```

### 3.2 Cgroup Memory Metrics

The function `load_cgroup_metrics(group_dir, exclude_nodes)` reads
cgroup monitor data from `proc_*_cgroup.json` files.

**Why cgroup matters**: The FJR `PeakValueRss` measures only the main
  cmsRun process RSS (`/proc/PID/stat`). For workflows with
  `ExternalLHEProducer`, the gridpack subprocess memory is invisible
  to the FJR. The cgroup monitor reads `memory.stat` from the HTCondor
  cgroup, which captures ALL processes: cmsRun, subprocesses, tmpfs
  allocations, and kernel overhead. This is the same metric the cgroup
  OOM killer enforces against.

**File pattern**: `proc_*_cgroup.json` (regex: `proc_(\d+)_cgroup\.json$`)

**Aggregation**: Peak (max) across all proc jobs for each field:

| Field | Description |
|---|---|
| `peak_anon_mb` | Peak anonymous memory (heap, stack, mmap) |
| `peak_shmem_mb` | Peak shared memory / tmpfs usage |
| `peak_nonreclaim_mb` | Peak non-reclaimable memory (anon + shmem) |
| `tmpfs_peak_nonreclaim_mb` | Peak non-reclaimable during tmpfs phase (step 0 with gridpack) |
| `no_tmpfs_peak_anon_mb` | Peak anonymous-only after tmpfs phase (steps 1+, shmem reclaimed) |

Returns `None` if no cgroup files are found. The `num_jobs` field records how many files contributed.

### 3.3 Multi-Round Merging

The function `merge_round_metrics(round_metrics, original_nthreads)`
combines metrics from multiple rounds into a single aggregated result.

**Problem**: When a later round runs at reduced thread count (e.g., 2T instead of 8T), raw `cpu_eff` overstates effective cores:

```
# WRONG: raw eff at 2T applied to 8T original
eff_cores = 0.95 × 8 = 7.6  (but step actually uses ~1.9 cores)

# CORRECT: normalize first
normalized_eff = 0.95 × 2 / 8 = 0.2375
eff_cores = 0.2375 × 8 = 1.9
```

**Normalization formula**:

```
normalized_eff = raw_eff × actual_nthreads / original_nthreads
```

Where `actual_nthreads` is the per-step average from that round's
`nthreads` list, falling back to the round's overall `nthreads` if
per-step data is unavailable.

**Merge strategy**:

| Field | Strategy | Rationale |
|---|---|---|
| `cpu_eff` | Concatenate from ALL rounds (after normalization) | More data points = better estimate of true efficiency |
| `peak_rss_mb` | Latest round only | Best predictor at current thread count |
| `wall_sec` | Latest round only | Reflects current configuration |
| `events`, `throughput`, `cpu_time_sec` | Latest round only | Reflects current configuration |
| `nthreads` | Set to `original_nthreads` | Normalized reference frame |
| `cgroup` | Latest round only | Most recent measurement |

### 3.4 Output Size Metrics

Per-work-unit metrics include `write_mb` per output tier (AODSIM,
MINIAODSIM, NANOAODSIM, etc.). The DAG planner uses the smallest
tier's `write_mb.mean` to compute optimal `jobs_per_work_unit` for
merged file sizing (§7).

---

## 4. Memory Sizing

### 4.1 Principles

Memory sizing follows main spec §5.5:

1. **Safety margin**: All measured values are multiplied by `(1 + safety_margin)` (default 20%) to account for memory leak accumulation over longer runs, event-to-event RSS variation, and page cache pressure under concurrent load.
2. **Clamping**: The result is clamped to `[floor, ceiling]` where:
   - `floor = MIN_MEMORY_MB` (4000 MB hard minimum)
   - `ceiling = max_memory_per_core × ncores`
3. **Instance reduction over ceiling breach**: If the measured-based estimate exceeds the ceiling, reduce the number of parallel instances rather than exceeding the memory limit.

### 4.2 Source Hierarchy (Unified)

Memory sizing uses a single priority hierarchy regardless of
optimization dimension. The first available source is used:

| Priority | Source | Condition | Formula |
|---|---|---|---|
| 1 | `cgroup` | `cgroup.peak_nonreclaim_mb > 0` | `measured_memory × (1 + margin)` |
| 2 | `fjr_rss` | `peak_rss_mb > 0` | `peak_rss × (1 + margin)` |
| 3 | `default` | (fallback) | `default_memory_per_core × ncores` |

Cgroup data is preferred because it captures the full memory footprint
(process RSS, tmpfs, subprocesses, page cache), which is the same
metric the cgroup OOM killer enforces. FJR RSS only measures the main
cmsRun process.

When job splitting reduces `request_cpus`, memory is resized using
`compute_job_split()`'s own memory model which accounts for the
reduced resource footprint.

### 4.3 Gridpack Memory

Workflows with gridpacks (`has_gridpack` derived from
`ExternalLHEProducer` in the workflow config) have additional memory
requirements:

- **Tmpfs extraction**: When `split_tmpfs` is enabled, the gridpack is
    extracted to `/dev/shm` (tmpfs) instead of the working
    directory. This avoids FUSE I/O bottlenecks but charges the
    extraction to the cgroup's memory accounting as non-reclaimable
    shared memory.
- **Phase-aware binding**: During step 0 (with tmpfs), non-reclaimable
    memory includes both anonymous memory and shared memory (the
    gridpack). After step 0 completes, the gridpack tmpfs can be
    reclaimed, so steps 1+ only need anonymous memory. The memory
    model takes the max of both phases.

The `split_tmpfs` flag is a workflow property set during import, not
an algorithm mode — it controls whether the sandbox uses tmpfs for
gridpack extraction. The adaptive algorithm accounts for its memory
impact in sizing.

### 4.4 Constants and Rationale

| Constant | Value | Unit | Rationale |
|---|---|---|---|
| `MIN_MEMORY_MB` | 4000 | MB | Hard minimum prevents pathologically small allocations |
| `SANDBOX_OVERHEAD` | 3000 | MB | CMSSW project area + shared libs + ROOT: ~3 GB loaded once regardless of instances |
| `TMPFS_PER_INSTANCE` (per-step) | 1500 | MB | Gridpack extraction on /dev/shm: measured ~1.4 GB, rounded up |
| `TMPFS_PER_INSTANCE` (job split) | 2000 | MB | Higher estimate for single-instance jobs: full tmpfs lifecycle + concurrent I/O |
| `CGROUP_HEADROOM` | 1000 | MB | Absolute minimum headroom covering page cache, kernel overhead, cgroup artifacts |
| `PER_THREAD_OVERHEAD` | 250 | MB | Conservative CMSSW per-thread cost: stacks, TLS, per-thread caches, ROOT buffers |
| `MAX_PARALLEL` | 4 | instances | Limits tmpfs memory for gridpack extraction (4 × ~1.4 GB = 5.6 GB) |
| `safety_margin` (default) | 0.20 | fraction | 20% buffer for leak accumulation, event variation, page cache pressure |
| Marginal floor | 500 | MB | Minimum per-instance memory; prevents unrealistic estimates |
| Power-of-2 max | 64 | threads | Upper bound for `_nearest_power_of_2()`; no CMS workflow uses more than 64 threads |

---

## 5. Job Splitting

### 5.1 When to Split

Job splitting is decided by CPU efficiency. When step 0's effective
cores are significantly below `request_cpus`, it's more efficient to
run more jobs with fewer cores. `request_cpus` is the real CPU knob —
reducing nThreads below available CPUs is not useful; CMSSW should use
all available cores within a job.

**Decision rule**: If `_nearest_power_of_2(eff_cores) < request_cpus`, split.

### 5.2 Minimum request_cpus Floor

The `min_request_cpus` setting (default 4) prevents pool
fragmentation. Jobs with very few cores are harder to schedule and
fragment the pool by consuming memory/disk resources disproportionate
to their CPU usage.

The effective minimum is `max(min_threads, min_request_cpus)` where
`min_threads` is a legacy per-call parameter (default 2).

### 5.3 Algorithm

The function `compute_job_split()` implements:

```
function compute_job_split(metrics, original_nthreads, ...):
    # 1. Effective cores from step 0
    eff_cores = mean(step0.cpu_eff) × original_nthreads

    # 2. Tuned thread count
    tuned = nearest_power_of_2(eff_cores)
    tuned = clamp(tuned, effective_min, original_nthreads)

    # 3. Job multiplier
    job_multiplier = original_nthreads // tuned
    if job_multiplier < 1: job_multiplier = 1

    # 4. New event distribution
    new_events_per_job = events_per_job // job_multiplier
    if new_events_per_job < 1:
        new_events_per_job = 1
        job_multiplier = events_per_job

    # 5. New job count and resources
    new_num_jobs = num_jobs_wu × job_multiplier
    new_request_cpus = tuned
    new_request_memory = sized_from_hierarchy(§4.2)
```

### 5.4 Memory Sizing for Split Jobs

Job split uses the unified source hierarchy (§4.2) with per-core bounds scaled to `tuned_threads`, not `original_nthreads`:

```
floor   = tuned_threads × memory_per_core
ceiling = tuned_threads × max_memory_per_core
result  = max(floor, min(measured_estimate, ceiling))
```

This reflects the reduced resource footprint of each split job.

### 5.5 DAG Rewriting

The function `rewrite_wu_for_job_split(group_dir, split_result, ...)` modifies the target WU's merge group for the new job configuration:

1. **Parse template**: Read the first existing `proc_*.sub` as a template
2. **Extract event range**: Parse `--first-event`, `--last-event` from all existing proc submit files
3. **Write manifest_tuned.json**: Set all steps to `tuned_nthreads`, `n_parallel=1`
4. **Delete old proc submit files**: Remove all `proc_*.sub`
5. **Generate new proc submit files**: For each of `new_num_jobs` jobs with new event ranges, CPUs, and memory
6. **Rewrite group.dag**: Regenerate the DAG file preserving landing, merge, cleanup structure

This rewrite is safe because DAGMan reads `SUBDAG EXTERNAL` files when the node becomes ready to execute, not at outer DAG submission time.

---

## 6. Internal Parallelism

### 6.1 Step 0 Splitting

The function `compute_per_step_nthreads()` reduces wasted CPU by matching each step's thread count to its actual parallelism, then filling freed cores with parallel instances of step 0.

**Key invariant**: When used alone (no job split), the scheduler-visible resource footprint (`request_cpus`, `num_jobs`, `events_per_job`) is unchanged. Optimization happens entirely within the sandbox.

**Algorithm**:

```
function compute_step0_split(step0, original_nthreads, request_cpus):
    eff_cores = mean(step0.cpu_eff) × original_nthreads
    tuned = nearest_power_of_2(eff_cores)
    tuned = clamp(tuned, 2, original_nthreads)
    n_parallel = request_cpus // tuned
    n_parallel = clamp(n_parallel, 1, MAX_PARALLEL)
    return tuned, n_parallel
```

**Minimum 2 threads**: Each parallel instance extracts a full gridpack (~1.4 GB tmpfs). Single-threaded instances would have the same base memory cost (~70% of CMSSW memory is thread-independent) while providing only half the parallelism of 2-thread instances.

**MAX_PARALLEL = 4**: Each instance needs ~1.4 GB tmpfs for gridpack extraction. At 4 instances, tmpfs alone consumes 5.6 GB — already a significant fraction of typical slot memory (16–24 GB).

**Worked example** (8-core job, step 0 at 55% efficiency):
1. `eff_cores = 0.55 × 8 = 4.4`
2. `tuned = nearest_power_of_2(4.4) = 4` (boundary 5.657, 4.4 < 5.657)
3. `n_parallel = 8 // 4 = 2`
4. Result: 2 instances of step 0, each with 4 threads

### 6.2 All-Step Splitting (Pipeline Split)

The function `compute_all_step_split()` forks N complete StepChain pipelines within one sandbox. Unlike step 0-only splitting, pipeline split runs ALL steps with optimized thread counts, with each pipeline processing `events/N` events through the full chain.

This is appropriate for workflows where submitting 1-core jobs would fragment the pool but steps are most efficient at low thread counts. The job keeps its original `request_cpus` but runs multiple independent pipelines inside.

**Algorithm**: Iterates from maximum feasible `n_pipelines` downward, taking the first (highest) that fits in memory:

```
for n_pipe in request_cpus..1 (descending):
    threads_cap = request_cpus // n_pipe
    for each step:
        tuned = min(ideal[step], threads_cap)
        projected_rss = (measured_rss - thread_reduction × PER_THREAD_OVERHEAD) × (1 + margin)
        projected_rss = max(projected_rss, 500)
    if n_pipe × max(projected_rss) <= request_memory_mb:
        return n_pipe
```

### 6.3 Memory-Aware Instance Reduction

After computing the ideal `n_parallel` and `tuned`, the algorithm checks whether the total memory fits:

```
total = SANDBOX_OVERHEAD + n_parallel × instance_mem

if total > max_memory_mb:
    # Try even-division candidates first (no wasted cores)
    for p in n_parallel..2 where cpus % p == 0 (descending):
        if SANDBOX_OVERHEAD + p × instance_mem <= max_memory_mb:
            accept (p, cpus // p)
    # Fallback: any n_parallel; then single instance
```

Even divisors are preferred because they avoid wasted cores (e.g., 8 cpus / 3 = 2 threads with 2 cores idle).

### 6.4 CPU Overcommit

CPU overcommit adds extra threads beyond the proportional core share, filling I/O bubbles when threads are waiting on disk or network.

**Step 0 overcommit** (when splitting is active, `n_parallel > 1`):

```
tuned_oc = min(round(tuned × overcommit_max), round(original_nthreads × overcommit_max))
projected_mem = instance_mem + (tuned_oc - tuned) × PER_THREAD_OVERHEAD
total = SANDBOX_OVERHEAD + n_parallel × projected_mem
if total <= max_memory_mb: accept overcommit
```

**Steps 1+ overcommit** (sequential execution): Only applied when CPU efficiency is in the moderate range (50–90%). Below 50%: fundamentally serial, more threads won't help. Above 90%: already saturating threads.

### 6.5 Composability

Internal parallelism composes with job splitting. When job splitting reduces `request_cpus` from 8 to 4, `compute_per_step_nthreads()` is then called with `request_cpus=4` and computes internal parallelism at the new core count. The result is unified: the job has fewer cores AND internal optimization within those cores.

---

## 7. Work Group Sizing

### 7.1 Target Merged File Size

The function `_compute_jobs_per_wu_from_write_mb()` (in `dag_planner.py`) computes optimal `jobs_per_work_unit` from measured output sizes:

```
target_mb = target_merged_size_kb / 1024 × test_fraction
optimal_jobs = int(target_mb / min_write_mb)
result = clamp(optimal_jobs, 1, max_jobs_per_work_unit)
```

Where `min_write_mb` is the smallest output tier's mean write size per job from the most recent round's metrics.

### 7.2 Test Fraction Scaling

When `test_fraction < 1`, the target merged size is scaled down proportionally (e.g., 4 GB × 0.01 = 40 MB) so that merged files stay proportional to the test run. Without this scaling, test runs would produce enormous merged files relative to their total output.

---

## 8. Multi-Round Convergence

### 8.1 Round 0 as Measurement

Round 0 serves as the measurement phase. Controlled by
`first_round_work_units` (default 1), it produces a small number of
work units using request defaults. The outputs are real production
data (registered in DBS, archived to tape), not throwaway
measurements.

After Round 0 completes, `complete_round()` in the lifecycle manager:
1. Reads work unit metrics from disk
2. Counts output events
3. Aggregates step_metrics
4. Calls `compute_round_optimization()` to produce `adaptive_params`
5. Stores `adaptive_params` in `workflow.step_metrics` for the DAG planner to apply in round 1+

### 8.2 Convergence Properties

**Data accumulates**: Multi-round merging (§3.3) concatenates
  normalized `cpu_eff` from all rounds. With more data points, the
  mean efficiency estimate becomes more robust, reducing the impact of
  outlier jobs.

**No oscillation**: Thread counts are quantized to powers of 2
  (§2.4). The geometric mean rounding creates discrete, stable
  states. A step at 4T will stay at 4T unless its efficiency changes
  enough to cross the 5.657 boundary — and since the data is
  accumulating (not replacing), the mean is increasingly stable.

**Diminishing returns**: Round 1 captures most gains — memory is sized
  to actual measurements, job splitting is applied to inefficient
  steps. Round 2+ refines the efficiency estimate with more data but
  the parameters are already near-optimal. The discrete power-of-2
  states mean that small efficiency changes don't alter the tuning.

---

## 9. Worked Examples

### 9.1 Geometric Mean Rounding Table

Complete mapping from `_nearest_power_of_2()`:

| Input | Nearest lower power | Nearest upper power | Geometric midpoint | Output |
|---|---|---|---|---|
| 0.5 | — | — | — | 1 |
| 1.0 | 1 | 2 | 1.414 | 1 |
| 1.4 | 1 | 2 | 1.414 | 1 |
| 1.5 | 1 | 2 | 1.414 | 2 |
| 2.0 | 2 | 4 | 2.828 | 2 |
| 2.8 | 2 | 4 | 2.828 | 2 |
| 3.0 | 2 | 4 | 2.828 | 4 |
| 4.0 | 4 | 8 | 5.657 | 4 |
| 4.4 | 4 | 8 | 5.657 | 4 |
| 5.6 | 4 | 8 | 5.657 | 4 |
| 5.7 | 4 | 8 | 5.657 | 8 |
| 6.0 | 4 | 8 | 5.657 | 8 |
| 8.0 | 8 | 16 | 11.314 | 8 |
| 11.3 | 8 | 16 | 11.314 | 8 |
| 11.4 | 8 | 16 | 11.314 | 16 |
| 16.0 | 16 | 32 | 22.627 | 16 |

### 9.2 Inter-Round Optimization Trace

**Scenario**: 8-core GEN-SIM StepChain, `default_memory_per_core=2000`, `max_memory_per_core=3000`, `events_per_job=5000`, `jobs_per_wu=8`.

**Round 0** (1 WU, 8T baseline):
- Step 0 cpu_eff: [0.52, 0.55, 0.54, 0.53] → mean = 0.535
- Peak RSS: 5200 MB, Cgroup peak_nonreclaim: 5800 MB
- `eff_cores = 0.535 × 8 = 4.28`

**Optimization computed** (`compute_round_optimization()`):
1. Memory: `cgroup 5800 × 1.20 = 6960 → clamp(4000, 6960, 24000) = 6960 MB` [source: cgroup]
2. Job split: `_nearest_power_of_2(4.28) = 4` → `tuned = max(4, 4) = 4` → `multiplier = 8/4 = 2`
   - `new_events_per_job = 5000/2 = 2500`, `new_request_cpus = 4`
   - Job split memory at 4 cores: `clamp(measured, 4×2000, 4×3000) = clamp(6960, 8000, 12000) = 8000 MB`
3. Internal parallelism at 4 cpus: `eff_cores = 0.535 × 8 = 4.28`, at 4 cpus → `n_parallel = 4/4 = 1`
4. Result: `{tuned_request_cpus: 4, tuned_memory_mb: 8000, job_multiplier: 2, per_step: {...}}`

**Round 1** (10 WUs, 4T, 2× jobs):
- Each WU now has 16 jobs × 2500 events, 4 cores each
- Total core-hours unchanged (16×4 = 8×8 = 64 core-slots)

### 9.3 Memory Source Scenarios

**Scenario A**: Cgroup available (best case)

| Available data | Selected source | Calculation |
|---|---|---|
| cgroup: `peak_nonreclaim = 5800 MB` | `cgroup` (priority 1) | `5800 × 1.20 = 6960 MB` |
| FJR: `peak_rss = 5200 MB` | — (lower priority) | — |

**Scenario B**: No cgroup, FJR available

| Available data | Selected source | Calculation |
|---|---|---|
| FJR: `peak_rss = 5200 MB` | `fjr_rss` (priority 2) | `5200 × 1.20 = 6240 MB` |

**Scenario C**: No measurements (round 0 with no data)

| Available data | Selected source | Calculation |
|---|---|---|
| (none) | `default` (priority 3) | `2000 × 8 = 16000 MB` |

### 9.4 Work Group Sizing Example

**Scenario**: After round 0, smallest output tier (NANOAODSIM) has `write_mb.mean = 25 MB/job`. Target merged size = 4 GB. Test fraction = 0.01.

```
target_mb = 4 × 1024 × 0.01 = 40.96 MB
optimal_jobs = int(40.96 / 25) = 1
result = clamp(1, 1, 100) = 1 job per WU
```

At full scale (test_fraction = 1.0):
```
target_mb = 4 × 1024 = 4096 MB
optimal_jobs = int(4096 / 25) = 163
result = clamp(163, 1, 100) = 100 jobs per WU  (capped at max)
```

---

## 10. Integration Points

### 10.1 Lifecycle Manager

The lifecycle manager calls `compute_round_optimization()` in `complete_round()` after each DAG completes:

```python
adaptive_params = compute_round_optimization(
    submit_dir=dag.submit_dir,
    completed_wus=list(dag.completed_work_units),
    original_nthreads=multicore,
    request_cpus=multicore,
    default_memory_per_core=settings.default_memory_per_core,
    max_memory_per_core=settings.max_memory_per_core,
    safety_margin=settings.safety_margin,
    events_per_job=events_per_job,
    jobs_per_wu=settings.jobs_per_work_unit,
    min_request_cpus=settings.min_request_cpus,
)
```

The result is stored in `workflow.step_metrics["adaptive_params"]`.

### 10.2 DAG Planner

When planning round 1+ DAGs, the planner reads `adaptive_params` from `workflow.step_metrics` and applies:

- `tuned_memory_mb` → `resource_params["memory_mb"]`
- `tuned_request_cpus` → `resource_params["ncpus"]`
- `tuned_events_per_job` → `splitting_params["events_per_job"]`
- `job_multiplier` → `jobs_per_work_unit` override

Output-size-based WU sizing (`_compute_jobs_per_wu_from_write_mb()`) runs independently if no explicit `jobs_per_wu` override is present.

### 10.3 Output Schema

The `adaptive_params` dict returned by `compute_round_optimization()`:

| Field | Type | Description |
|---|---|---|
| `tuned_nthreads` | int | Thread count (= `tuned_request_cpus`) |
| `tuned_memory_mb` | int | Optimal memory (MB) |
| `tuned_request_cpus` | int | New `request_cpus` for jobs |
| `job_multiplier` | int | Job count multiplication factor (1 = no split) |
| `memory_source` | string | `"cgroup"`, `"fjr_rss"`, or `"default"` |
| `per_step` | dict[str, StepTuning] | Per-step tuning parameters |
| `metrics_summary` | dict | Observed metrics summary |
| `tuned_events_per_job` | int | (present if `job_multiplier > 1`) New events per job |

**StepTuning object**:

| Field | Type | Description |
|---|---|---|
| `tuned_nthreads` | int | Thread count for this step |
| `n_parallel` | int | Parallel instances (>1 only for step 0) |
| `cpu_eff` | float | Mean CPU efficiency (normalized if multi-round) |
| `effective_cores` | float | `cpu_eff × original_nthreads` |
| `overcommit_applied` | bool | Whether CPU overcommit was applied |
| `projected_rss_mb` | float or null | Projected RSS with overcommit threads |

---

## Appendix A: Constants Reference

| Constant | Value | Unit | Used in | Rationale |
|---|---|---|---|---|
| `MIN_MEMORY_MB` | 4000 | MB | Memory sizing (§4) | Hard minimum prevents pathologically small allocations |
| `SANDBOX_OVERHEAD` | 3000 | MB | Internal parallelism (§6) | CMSSW project area + shared libraries + ROOT framework, loaded once per job |
| `TMPFS_PER_INSTANCE` (per-step) | 1500 | MB | Internal parallelism (§6.1) | Gridpack extraction on /dev/shm: measured ~1.4 GB, rounded up |
| `TMPFS_PER_INSTANCE` (job split) | 2000 | MB | Job splitting (§5.4) | Higher estimate for single-instance: full tmpfs lifecycle + concurrent I/O |
| `CGROUP_HEADROOM` | 1000 | MB | Job splitting (§5.4, prior_rss fallback) | Absolute minimum headroom covering page cache, kernel overhead |
| `PER_THREAD_OVERHEAD` | 250 | MB | Overcommit (§6.4), Pipeline split (§6.2) | Conservative CMSSW per-thread cost: stacks, TLS, caches, ROOT buffers |
| `MAX_PARALLEL` | 4 | instances | Internal parallelism (§6.1) | Limits tmpfs memory (4 × ~1.4 GB = 5.6 GB) |
| `safety_margin` (default) | 0.20 | fraction | All dimensions | 20% buffer for leak accumulation, event variation, page cache pressure |
| Marginal floor | 500 | MB | Internal parallelism (§6.1), Job split (§5.4) | Minimum per-instance memory; prevents unrealistic estimates |
| Pipeline RSS floor | 500 | MB | Pipeline split (§6.2) | Minimum projected RSS per pipeline |
| `min_request_cpus` (default) | 4 | cores | Job splitting (§5.2) | Prevents pool fragmentation from many small jobs |
| Power-of-2 max | 64 | threads | Thread rounding (§2.4) | Upper bound for `_nearest_power_of_2()` |
