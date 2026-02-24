# WMS2 Adaptive Execution Algorithm Specification

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

This document specifies the algorithms behind the WMS2 adaptive execution model described in main spec §5. The main spec defines *what* the adaptive system does at an architectural level — memory-per-core windows, execution rounds, per-step splitting, metric aggregation, convergence. This document defines *how* — the concrete algorithms, data structures, formulas, and constants sufficient to reimplement the system from scratch.

### 1.2 Relationship to Main Spec

The main spec (§5, §DD-12, §DD-13) is authoritative for architectural decisions:

- WMS2 does not run separate pilot jobs; the first production DAG provides baseline metrics
- The adaptive algorithm operates within a `[default_memory_per_core, max_memory_per_core]` window
- Per-step splitting is a sandbox-owned optimization; WMS2 only provides metrics
- Memory sizing uses measured data + 20% safety margin, clamped within the window

This document specifies the algorithms that implement these architectural decisions. Cross-references use the notation `main spec §N` (e.g., `main spec §5.4` for per-step splitting).

### 1.3 Scope

**In scope**: Replan algorithms (per-step tuning, job split, pipeline split), metric collection and aggregation, memory sizing model, probe node design, DAG modification, manifest patching, multi-round convergence, CLI interface.

**Out of scope**: Sandbox internals (cmsRun forking, event partitioning, gridpack extraction), HTCondor scheduling behavior, Framework Job Report (FJR) parsing, cgroup monitor implementation, simulator profiles for testing.

---

## 2. Core Concepts

### 2.1 Effective Cores

The fundamental metric driving all adaptive decisions is **effective cores** — the number of CPU cores a step actually utilizes:

```
effective_cores = weighted_cpu_eff × nthreads
```

Where `weighted_cpu_eff` is the wall-time-weighted CPU efficiency across all steps:

```
weighted_cpu_eff = Σ(mean_eff_i × mean_wall_i) / Σ(mean_wall_i)
```

This weighting ensures that long-running steps (which dominate job wall time) have proportionally more influence on the aggregate efficiency. A 4-minute GEN step at 55% efficiency matters more than a 10-second NANO step at 15%.

The connection to Amdahl's law is direct: each CMSSW step has a serial fraction (GEN ~30%, DIGI/RECO ~10%, NANO ~5%) that limits how much parallelism the step can exploit. Effective cores measures the realized parallelism — the input to all splitting and thread-tuning decisions.

### 2.2 Thread Count Rounding

Thread counts are always rounded to the nearest power of 2, using **geometric mean midpoints** rather than arithmetic midpoints. The function `_nearest_power_of_2(n)` implements this:

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

**Rationale**: Geometric mean rounding is appropriate because thread counts are multiplicative — doubling from 4 to 8 threads is the same proportional change as doubling from 8 to 16. Arithmetic midpoints (e.g., 6 between 4 and 8) would bias toward larger values. The boundary at 5.657 means an effective core count of 5.6 rounds to 4, not 8 — this is conservative and avoids allocating threads that won't be productively used.

### 2.3 Work Unit Rounds

The adaptive system operates across multiple **work unit rounds**. Each work unit is a self-contained merge group (main spec §2) executed as a SUBDAG EXTERNAL:

- **Round 1 (WU0)**: Runs with request defaults (nThreads from request `Multicore`, memory from `default_memory_per_core × Multicore`). Produces baseline metrics.
- **Replan 0**: Analyzes WU0 metrics, computes tuned parameters, patches WU1's manifest and/or DAG.
- **Round 2 (WU1)**: Runs with tuned parameters. Produces refined metrics.
- **Replan 1**: Analyzes WU0 + WU1 metrics (merged), patches WU2.
- **Round 3+ (WU2+)**: Further refinement with accumulated data. Diminishing returns.

Work unit directories map to merge group directories (`mg_000000`, `mg_000001`, etc.) within the workflow submit directory. Replan nodes are `universe=local` HTCondor jobs that execute between consecutive SUBDAGs via DAG dependency edges.

### 2.4 Three Optimization Modes

The replan algorithm supports three mutually exclusive optimization modes:

| Mode | Flag | Description | Use case |
|---|---|---|---|
| **Per-step tuning** | (default) | Reduce step 0 nThreads, split into N parallel instances within one job | Steps with low CPU efficiency; memory allows multiple instances |
| **Job split** | `--job-split` | Multiply job count, reduce cores per job | Production scaling; avoid tail effects from large jobs |
| **Pipeline split** | `--split-all-steps` | Fork N complete StepChain pipelines within one sandbox | All steps have low efficiency; uniform per-step optimization |

Per-step tuning and job split both use step 0 efficiency as the primary driver. Pipeline split considers all steps and finds the highest number of pipelines that fits in memory. Sections 7–9 detail each mode.

---

## 3. Metric Collection and Aggregation

### 3.1 Per-Work-Unit Metrics

The function `analyze_wu_metrics(group_dir, exclude_nodes)` reads metrics from a completed merge group.

**Input search paths** (in order, first with results wins):
1. The merge group directory itself (`mg_NNNNNN/`)
2. Unmerged storage paths derived from `output_info.json` — `{local_pfn_prefix}/{unmerged_lfn_base}/{group_index:06d}/`

**File pattern**: `proc_*_metrics.json` (regex: `proc_(\d+)_metrics\.json$`)

**Node exclusion**: When `exclude_nodes` is provided (e.g., `{"proc_000001"}` for the probe node), matching files are skipped. Node names are normalized to integer indices for comparison — `proc_000001` and `proc_1_metrics.json` both resolve to index 1.

**Per-step field collection**: Each metrics file contains a list of step entries. For each step (keyed by `step_index`), the following fields are collected into lists:

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

**Cgroup attachment**: After collecting FJR-based metrics, `load_cgroup_metrics()` is called on the same directory. If cgroup data exists, it is attached under the `"cgroup"` key.

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

The function `load_cgroup_metrics(group_dir, exclude_nodes)` reads cgroup monitor data from `proc_*_cgroup.json` files.

**Why cgroup matters**: The FJR `PeakValueRss` measures only the main cmsRun process RSS (`/proc/PID/stat`). For workflows with `ExternalLHEProducer`, the gridpack subprocess memory is invisible to the FJR. The cgroup monitor reads `memory.stat` from the HTCondor cgroup, which captures ALL processes: cmsRun, subprocesses, tmpfs allocations, and kernel overhead. This is the same metric the cgroup OOM killer enforces against.

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

The function `merge_round_metrics(round_metrics, original_nthreads)` combines metrics from multiple rounds into a single aggregated result.

**Problem**: When Round 2 runs at reduced thread count (e.g., 2T instead of 8T), raw `cpu_eff` overstates effective cores:

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

Where `actual_nthreads` is the per-step average from that round's `nthreads` list, falling back to the round's overall `nthreads` if per-step data is unavailable.

**Merge strategy**:

| Field | Strategy | Rationale |
|---|---|---|
| `cpu_eff` | Concatenate from ALL rounds (after normalization) | More data points = better estimate of true efficiency |
| `peak_rss_mb` | Latest round only | Best predictor at current thread count |
| `wall_sec` | Latest round only | Reflects current configuration |
| `events`, `throughput`, `cpu_time_sec` | Latest round only | Reflects current configuration |
| `nthreads` | Set to `original_nthreads` | Normalized reference frame |
| `cgroup` | Latest round only | Most recent measurement |

**Pseudocode**:

```
function merge_round_metrics(round_metrics, original_nthreads):
    if single round:
        # Normalize in place
        for each step:
            step_nt = avg(step.nthreads) or round.nthreads
            ratio = step_nt / original_nthreads
            step.cpu_eff = [e × ratio for e in step.cpu_eff]
        recompute weighted_cpu_eff, effective_cores
        return

    # Multiple rounds:
    # Initialize from latest round (RSS, wall, events, throughput)
    merged = copy_non_eff_fields(latest_round)

    # Collect normalized cpu_eff from ALL rounds
    for each round:
        for each step:
            step_nt = avg(step.nthreads) or round.nthreads
            ratio = step_nt / original_nthreads
            merged[step].cpu_eff.extend(e × ratio for e in step.cpu_eff)

    # Recompute aggregates
    weighted_cpu_eff = wall-time-weighted mean of merged cpu_eff
    effective_cores = weighted_cpu_eff × original_nthreads
    propagate cgroup from latest round
```

---

## 4. Probe Node Design

### 4.1 Purpose

The probe node provides ground-truth per-instance memory measurement, eliminating the need for theoretical memory estimation when computing parallel split parameters. Without probe data, memory estimates rely on FJR RSS (which misses subprocess memory and tmpfs) plus hardcoded overhead constants — a fragile approach that either wastes memory or risks OOM.

### 4.2 Configuration

The probe modifies the **last** proc node in WU0 to run step 0 as **2 parallel instances, each with half the cores**: `2×(N/2)T`.

- **Why the last node**: Earlier nodes have already been submitted or are running. The last node is most likely to still be pending when the probe injection runs during DAG planning.
- **Why 2 instances**: The minimum needed to measure per-instance marginal cost. More instances would reduce per-instance thread count further, making the measurement less representative of the target configuration.
- **Minimum half_cores**: `max(ncores // 2, 2)` — at least 2 threads per instance.

**Guard conditions** (skip probe if):
- Fewer than 2 proc nodes in WU0 (need at least one non-probe node for baseline metrics)
- No `manifest.json` in the merge group directory
- No steps in the manifest

### 4.3 Memory Allocation

The probe node's `request_memory` is set to `max_memory_per_core × ncores` — the maximum available memory. This ensures the probe has headroom for 2 concurrent instances without risk of OOM, even though production jobs use `default_memory_per_core`.

### 4.4 Metrics Extraction

The function `analyze_probe_metrics(group_dir, probe_node_name)` reads the probe's metrics file.

**Node name resolution**: The probe node name (e.g., `proc_000001`) is converted to the metrics filename pattern (e.g., `proc_1_metrics.json`) by extracting the integer index. The wrapper writes metrics using the `--node-index` integer, not the 6-digit padded name.

**FJR per-instance RSS**: The probe metrics file contains two entries with `step_index=0` (one per parallel instance). Each has its own `peak_rss_mb`. The function extracts all step 0 RSS values.

**HTCondor event 006 job peak**: The function parses the probe node's HTCondor job log (`{probe_node_name}.log`) for event 006 image size updates:

```
006 (...) Image size of job updated: <ImageSizeKb>
    <MemoryUsage_MB>  -  MemoryUsage of job (MB)
    <ResidentSetSize_KB>  -  ResidentSetSize of job (KB)
```

The peak `MemoryUsage` across all samples is extracted as `job_peak_mb`. This is the HTCondor-reported ceiling (RSS at each sample, not cgroup watermark) and captures the combined memory of all processes in the job.

**Return structure**:

```json
{
  "per_instance_rss_mb": [1200.0, 1150.0],
  "max_instance_rss_mb": 1200.0,
  "num_instances": 2,
  "job_peak_mb": 6200.0,
  "per_instance_peak_mb": 3100.0
}
```

`per_instance_peak_mb = job_peak_mb / num_instances` is a simple average — it does not separate shared overhead from marginal cost (that's done by the memory model in §5).

### 4.5 Marginal Memory Model

The memory model separates **shared overhead** from **marginal per-instance cost**:

```
total_memory = SANDBOX_OVERHEAD + n_instances × marginal_per_instance
```

Where:
- `SANDBOX_OVERHEAD` = 3000 MB — CMSSW project area, shared libraries, ROOT framework, loaded once regardless of instance count
- `marginal_per_instance = (job_peak_total - SANDBOX_OVERHEAD) / n_probe_instances`

**Why not simple division**: Dividing `job_peak_total / n_instances` amortizes the shared overhead into each instance. When projecting for a different number of instances, adding the shared overhead back would double-count it. The marginal model avoids this:

```
# Probe: 2 instances, total 6200 MB
marginal = (6200 - 3000) / 2 = 1600 MB/instance

# Project for 4 instances:
projected = 3000 + 4 × 1600 = 9400 MB  (correct)

# Simple division would give:
per_inst = 6200 / 2 = 3100 MB
projected = 3000 + 4 × 3100 = 15400 MB  (inflated)
```

The marginal value has a floor of 500 MB — if the computation yields less, the probe data is suspect and the floor prevents unrealistically low memory estimates.

---

## 5. Memory Sizing Model

### 5.1 Principles

Memory sizing follows main spec §5.5:

1. **Safety margin**: All measured values are multiplied by `(1 + safety_margin)` (default 20%) to account for memory leak accumulation over longer runs, event-to-event RSS variation, and page cache pressure under concurrent load.
2. **Clamping**: The result is clamped to `[floor, ceiling]` where:
   - `floor = memory_per_core × ncores` (or `memory_per_core × tuned_cores` for job split)
   - `ceiling = max_memory_per_core × ncores` (or `max_memory_per_core × tuned_cores` for job split)
3. **Instance reduction over ceiling breach**: If the measured-based estimate exceeds the ceiling, reduce the number of parallel instances rather than exceeding the memory limit.

### 5.2 Source Hierarchy — Per-Step Tuning Mode

Memory sizing in `compute_per_step_nthreads()` for step 0 parallel splitting uses a 4-level priority hierarchy. The first available source is used:

| Priority | Source | Condition | Formula | Notes |
|---|---|---|---|---|
| 1 | `probe_peak` | `probe_job_peak_total_mb > 0` | `marginal = (total - 3000) / n_instances`; `instance_mem = marginal × (1 + margin)` | Marginal model avoids double-counting shared overhead |
| 2 | `cgroup_measured` | `cgroup.tmpfs_peak_nonreclaim_mb > 0` | `instance_mem = cgroup.tmpfs_peak_nonreclaim_mb × (1 + margin)` | Cgroup captures all processes including subprocesses |
| 3 | `probe_rss` | `probe_rss_mb > 0` | `instance_mem = probe_rss × (1 + margin) + 1500` | FJR RSS + tmpfs estimate (1500 MB) |
| 4 | `theoretical` | (fallback) | `instance_mem = avg_rss × (1 + margin) + 1500` | R1 average RSS + tmpfs estimate |

**Total memory projection**: `SANDBOX_OVERHEAD + n_parallel × instance_mem`

**Memory-aware instance reduction**: If the total exceeds `max_memory_mb`, reduce `n_parallel`:

```
function reduce_instances(n_par, request_cpus, instance_mem, max_memory_mb):
    # Prefer even divisors of cpus (no wasted cores)
    candidates = [p for p in n_par..2 if cpus % p == 0] (descending)
    candidates += [p for p in n_par..2 if cpus % p != 0] (descending, fallback)
    for p in candidates:
        tuned = max(cpus // p, 2)
        total = SANDBOX_OVERHEAD + p × instance_mem
        if total <= max_memory_mb:
            return p, tuned
    return 1, original_nthreads  # no splitting possible
```

### 5.3 Source Hierarchy — Job Split Mode

Memory sizing in `compute_job_split()` uses a different 4-level hierarchy optimized for single-instance jobs at reduced thread count:

| Priority | Source | Condition | Formula | Notes |
|---|---|---|---|---|
| 1 | `probe_peak` | `probe_job_peak_total_mb > 0` | `mem = (SANDBOX_OVERHEAD + marginal) × (1 + margin)` | Single instance: overhead + 1 marginal |
| 2 | `cgroup_measured` | `cgroup.peak_nonreclaim_mb > 0` | `mem = binding × (1 + margin)` (see §6.3) | Tmpfs-aware binding selection |
| 3 | `probe_rss` | `probe_rss_mb > 0` | `mem = probe_rss × (1 + margin) + TMPFS_PER_INSTANCE` | RSS + 2000 MB tmpfs |
| 4 | `prior_rss` | `peak_rss_mb > 0` | `mem = max(effective_peak × (1 + margin), effective_peak + CGROUP_HEADROOM)` | FJR RSS + hardcoded estimates |

Where:
- `TMPFS_PER_INSTANCE` = 2000 MB (job split fallback, higher than per-step tuning's 1500)
- `CGROUP_HEADROOM` = 1000 MB (absolute minimum headroom)
- `effective_peak` includes tmpfs estimation when `split_tmpfs` is enabled: `max(peak_rss, step0_avg_rss + TMPFS_PER_INSTANCE)`

**Clamping**:

```
floor   = tuned_threads × memory_per_core
ceiling = tuned_threads × max_memory_per_core
result  = max(floor, min(measured_estimate, ceiling))
```

Note: Job split uses per-core bounds scaled to `tuned_threads`, not `original_nthreads`. This reflects the reduced resource footprint of each split job.

### 5.4 Source Hierarchy — Pipeline Split Mode

Memory sizing in `compute_all_step_split()` uses RSS projection based on thread reduction:

```
projected_rss = (measured_rss - thread_reduction × PER_THREAD_OVERHEAD) × (1 + margin)
projected_rss = max(projected_rss, 500)  # floor at 500 MB
```

Where `thread_reduction = original_nthreads - tuned_nthreads` and `PER_THREAD_OVERHEAD` = 250 MB.

The memory check for N pipelines:

```
total_memory = n_pipelines × max(projected_rss across all steps)
if total_memory <= request_memory_mb: accept
```

Pipeline split iterates from the maximum feasible `n_pipelines` downward and takes the first (highest) that fits in memory.

### 5.5 Constants and Rationale

| Constant | Value | Unit | Used in | Rationale |
|---|---|---|---|---|
| `SANDBOX_OVERHEAD` | 3000 | MB | Per-step tuning, job split | CMSSW project area + shared libs + ROOT: ~3 GB loaded once regardless of instances |
| `TMPFS_PER_INSTANCE` (per-step) | 1500 | MB | Per-step tuning | Gridpack extraction on /dev/shm: measured ~1.4 GB per instance, rounded up |
| `TMPFS_PER_INSTANCE` (job split) | 2000 | MB | Job split | Higher estimate for single-instance jobs to account for peak tmpfs + concurrent I/O |
| `CGROUP_HEADROOM` | 1000 | MB | Job split (prior_rss fallback) | Absolute minimum headroom when no cgroup data: covers page cache + kernel overhead |
| `PER_THREAD_OVERHEAD` | 250 | MB | Overcommit, pipeline split | Conservative CMSSW per-thread memory cost (thread stacks, TLS, per-thread caches) |
| `MAX_PARALLEL` | 4 | — | Per-step tuning | Cap parallel instances to limit tmpfs memory (4 × 1.4 GB = 5.6 GB tmpfs budget) |
| `safety_margin` | 0.20 | fraction | All modes | 20% buffer for leak accumulation, event variation, page cache pressure |
| Marginal floor | 500 | MB | Per-step tuning, job split | Minimum per-instance cost; prevents unrealistic estimates from suspect probe data |

---

## 6. Cgroup and Tmpfs Interactions

### 6.1 Why Cgroup Matters

The FJR `PeakValueRss` only measures the main cmsRun process (`/proc/PID/stat`). For GEN workflows using `ExternalLHEProducer`, the gridpack subprocess spawns separate processes whose memory is invisible to the FJR. The cgroup monitor reads `memory.stat` from the HTCondor job cgroup, which captures:

- All process RSS (cmsRun + subprocesses)
- tmpfs allocations (gridpack on /dev/shm)
- Page cache charged to the cgroup
- Kernel data structures (slab, etc.)

This is the same metric the cgroup OOM killer enforces against, making it the most accurate basis for memory sizing.

### 6.2 Tmpfs Optimization

When `split_tmpfs` is enabled, the gridpack is extracted to `/dev/shm` (tmpfs) instead of the working directory. This avoids FUSE file descriptor limits that can cause failures with CVMFS-backed storage. Each instance's gridpack extraction consumes approximately 1.4 GB of tmpfs space, which is charged to the cgroup's memory accounting as non-reclaimable shared memory.

The trade-off: tmpfs eliminates I/O bottlenecks but increases memory pressure. The adaptive algorithm accounts for this in memory sizing (§5.2, §5.3).

### 6.3 Tmpfs-Aware Binding Selection

When computing memory for job split mode with `split_tmpfs` enabled, the cgroup-measured source uses a phase-aware selection:

```
if split_tmpfs and cgroup.tmpfs_peak_nonreclaim_mb > 0:
    binding = max(
        cgroup.tmpfs_peak_nonreclaim_mb,   # step 0 phase: anon + shmem
        cgroup.no_tmpfs_peak_anon_mb,      # steps 1+ phase: anon only, shmem reclaimed
    )
else:
    binding = cgroup.peak_nonreclaim_mb
```

**Rationale**: During step 0 (with tmpfs), non-reclaimable memory includes both anonymous memory and shared memory (the gridpack). After step 0 completes, the gridpack tmpfs can be reclaimed, so steps 1+ only need anonymous memory. Taking the max of both phases ensures the memory allocation is sufficient for whichever phase has the higher peak.

---

## 7. Per-Step nThreads Tuning

### 7.1 Overview

The function `compute_per_step_nthreads()` is the default optimization mode. Its goal is to reduce wasted CPU by matching each step's thread count to its actual parallelism, then filling freed cores with parallel instances of step 0.

**Key invariant**: The scheduler-visible resource footprint (`request_cpus`, `num_jobs`, `events_per_job`) is unchanged. Optimization happens entirely within the sandbox.

### 7.2 Step 0 Parallel Splitting

When splitting is enabled (`split=True`) and step 0 data is available:

```
function compute_step0_split(step0, original_nthreads, request_cpus):
    eff_cores = mean(step0.cpu_eff) × original_nthreads
    tuned = nearest_power_of_2(eff_cores)
    tuned = clamp(tuned, 2, original_nthreads)
    n_parallel = request_cpus // tuned
    n_parallel = clamp(n_parallel, 1, MAX_PARALLEL)
    return tuned, n_parallel
```

**Minimum 2 threads**: Each parallel instance extracts a full gridpack (~1.4 GB tmpfs). Single-threaded instances would have the same base memory cost (~70% of CMSSW memory is thread-independent) while providing only half the parallelism of 2-thread instances. The 2-thread minimum balances parallelism against memory efficiency.

**MAX_PARALLEL = 4**: Each instance needs ~1.4 GB tmpfs for gridpack extraction. At 4 instances, tmpfs alone consumes 5.6 GB — already a significant fraction of typical slot memory (16–24 GB). More instances would leave insufficient headroom for process RSS.

**Worked example** (8-core job, step 0 at 55% efficiency):
1. `eff_cores = 0.55 × 8 = 4.4`
2. `tuned = nearest_power_of_2(4.4) = 4` (boundary 5.657, 4.4 < 5.657)
3. `n_parallel = 8 // 4 = 2`
4. Result: 2 instances of step 0, each with 4 threads

### 7.3 Memory-Aware Instance Reduction

After computing the ideal `n_parallel` and `tuned`, the algorithm checks whether the total memory fits:

```
total = SANDBOX_OVERHEAD + n_parallel × instance_mem

if total > max_memory_mb:
    # Try even-division candidates first (no wasted cores)
    for p in n_parallel..2 where cpus % p == 0 (descending):
        t = max(cpus // p, 2)
        if SANDBOX_OVERHEAD + p × instance_mem <= max_memory_mb:
            accept (p, t)

    # Fallback: any n_parallel
    for p in n_parallel..2 where cpus % p != 0 (descending):
        t = max(cpus // p, 2)
        if SANDBOX_OVERHEAD + p × instance_mem <= max_memory_mb:
            accept (p, t)

    # Nothing fits: no splitting
    n_parallel = 1
    tuned = original_nthreads
```

Even divisors are preferred because they avoid wasted cores (e.g., 8 cpus / 3 = 2 threads with 2 cores idle).

If `n_parallel` is reduced to 1, the algorithm reverts to original nThreads — splitting with a single instance provides no benefit.

### 7.4 CPU Overcommit

CPU overcommit adds extra threads beyond the proportional core share, filling I/O bubbles when threads are waiting on disk or network.

**Step 0 overcommit** (when splitting is active, `n_parallel > 1`):

```
tuned_oc = min(
    round(tuned × overcommit_max),
    round(original_nthreads × overcommit_max)
)
extra_threads = tuned_oc - tuned
projected_mem = instance_mem + extra_threads × PER_THREAD_OVERHEAD
total = SANDBOX_OVERHEAD + n_parallel × projected_mem

if total <= max_memory_mb:
    tuned = tuned_oc  # accept overcommit
else:
    # Back off to max safe value
    avail = max_memory_mb - SANDBOX_OVERHEAD
    safe_per_inst = avail / n_parallel
    safe_extra = floor((safe_per_inst - instance_mem) / PER_THREAD_OVERHEAD)
    if safe_extra > 0:
        tuned = tuned + safe_extra  # partial overcommit
```

**Steps 1+ overcommit** (sequential execution, no splitting):

Only applied when CPU efficiency is in the moderate range (50–90%):
- Below 50%: Low efficiency means the step is fundamentally serial; more threads won't help.
- Above 90%: Already saturating available threads; overcommit would only add memory pressure.

```
if 0.50 <= mean_eff < 0.90 and overcommit_max > 1.0:
    oc_threads = round(original_nthreads × overcommit_max)
    extra = oc_threads - original_nthreads
    projected_rss = avg_rss + extra × PER_THREAD_OVERHEAD
    if projected_rss <= max_memory_mb:
        tuned = oc_threads
    else:
        safe_extra = floor((max_memory_mb - avg_rss) / PER_THREAD_OVERHEAD)
        if safe_extra > 0:
            tuned = original_nthreads + safe_extra
```

### 7.5 Output Format

`compute_per_step_nthreads()` returns:

```json
{
  "original_nthreads": 8,
  "per_step": {
    "0": {
      "tuned_nthreads": 4,
      "n_parallel": 2,
      "cpu_eff": 0.55,
      "effective_cores": 4.4,
      "overcommit_applied": false,
      "projected_rss_mb": null,
      "ideal_n_parallel": 2,
      "ideal_memory_mb": 6200,
      "memory_source": "probe_peak",
      "instance_mem_mb": 1600
    },
    "1": {
      "tuned_nthreads": 8,
      "n_parallel": 1,
      "cpu_eff": 0.85,
      "effective_cores": 6.8,
      "overcommit_applied": false,
      "projected_rss_mb": null
    }
  }
}
```

Step 0 includes additional fields when splitting is active: `ideal_n_parallel` (before memory reduction), `ideal_memory_mb` (memory needed for ideal), `memory_source` (which data source was used), `instance_mem_mb` (per-instance memory estimate).

---

## 8. Adaptive Job Split

### 8.1 Overview

The function `compute_job_split()` implements an alternative to per-step tuning: instead of splitting step 0 into parallel instances within one job, split the jobs themselves — run N× more jobs with N× fewer cores. Each job processes fewer events and finishes independently, eliminating tail effects and releasing resources sooner.

### 8.2 Core Algorithm

```
function compute_job_split(metrics, original_nthreads, ...):
    # 1. Effective cores from step 0
    eff_cores = mean(step0.cpu_eff) × original_nthreads

    # 2. Tuned thread count
    tuned = nearest_power_of_2(eff_cores)
    tuned = clamp(tuned, 2, original_nthreads)

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
    new_request_memory = sized_from_hierarchy(§5.3)

    return result
```

**Floor at 2T rationale**: Single-threaded CMSSW has similar base memory to multi-threaded (~70% of memory is thread-independent: framework, geometry, conditions, etc.). Going to 1T would multiply job count without proportional memory savings, causing memory pressure without meaningful throughput gain.

### 8.3 Memory Sizing

Job split uses the 4-source hierarchy from §5.3. The key difference from per-step tuning: each split job runs a single instance at reduced thread count, so the memory model does not include `n_parallel × instance_mem` — it's simply the total memory for one job at `tuned` threads.

The `prior_rss` fallback uses a dual formula to ensure sufficient headroom:

```
memory_from_measured = max(
    effective_peak × (1 + safety_margin),     # percentage-based
    effective_peak + CGROUP_HEADROOM           # absolute minimum 1 GB headroom
)
```

The max of percentage and absolute headroom prevents the pathological case where a low-RSS workflow gets insufficient absolute headroom from the percentage alone.

### 8.4 DAG Rewriting

The function `rewrite_wu_for_job_split(group_dir, split_result, ...)` modifies the target WU's merge group for the new job configuration.

**Steps**:

1. **Parse template**: Read the first existing `proc_*.sub` as a template. Extract `executable`, `transfer_input_files`, `environment`, `request_disk`, `+DESIRED_Sites`, `--sandbox`, and `--output-info` from the template.

2. **Extract event range**: Parse `--first-event`, `--last-event`, and `--events-per-job` from all existing proc submit file arguments. Compute `wu_first_event = min(first_events)` and `wu_last_event = max(last_events)`.

3. **Write manifest_tuned.json**: Set all steps to `tuned_nthreads`, `n_parallel=1`. Add `split_tmpfs: true` if enabled.

4. **Delete old proc submit files**: Remove all `proc_*.sub` from the merge group directory.

5. **Generate new proc submit files**: For each of `new_num_jobs` jobs:
   - Compute event range: `first_event = wu_first_event + i × new_events_per_job`
   - Use `synthetic://gen/events_{fe}_{le}` as input LFN for GEN workflows (EmptySource rejects fileNames, so the synthetic prefix ensures `lfn_to_xrootd()` returns empty)
   - Set `request_cpus = new_request_cpus`, `request_memory = new_request_memory_mb`
   - Include `manifest_tuned.json` in `transfer_input_files`

6. **Rewrite group.dag**: Regenerate the entire `group.dag` file preserving:
   - Landing node (`JOB landing landing.sub`, `SCRIPT POST landing ...`)
   - Merge node (`JOB merge ...`, `SCRIPT PRE merge ...`)
   - Cleanup node (`JOB cleanup ...`, `SCRIPT PRE cleanup ...`)
   - RETRY directives: `proc_* 3 UNLESS-EXIT 2`, `merge 2 UNLESS-EXIT 2`, `cleanup 1`
   - Dependencies: `landing → proc_* → merge → cleanup`
   - Categories: proc nodes → `Processing`, merge → `Merge`, cleanup → `Cleanup`
   - MAXJOBS settings from original

**DAGMan lazy SUBDAG reading**: This rewrite is safe because DAGMan reads `SUBDAG EXTERNAL` files when the node becomes ready to execute, not at outer DAG submission. Since the replan node runs between WU0 and WU1, the rewritten `group.dag` will be read when WU1's SUBDAG starts.

### 8.5 Event Range Redistribution

Events are distributed evenly across split jobs:

```
for i in 0..new_num_jobs-1:
    first_event = wu_first_event + i × new_events_per_job
    last_event  = first_event + new_events_per_job - 1
    last_event  = min(last_event, wu_last_event)
```

For GEN workflows, input LFNs use the `synthetic://` scheme:

```
synthetic://gen/events_{first_event}_{last_event}
```

This is a sentinel value: the wrapper's `lfn_to_xrootd()` function returns empty for `synthetic://` prefixes, which is required because GEN workflows use `EmptySource` (CMSSW's input source for generation-only workflows that rejects explicit file names).

---

## 9. Pipeline Split

### 9.1 Overview

The function `compute_all_step_split()` implements pipeline split — forking N complete StepChain pipelines within one sandbox. Unlike per-step tuning (which only splits step 0), pipeline split runs ALL steps with optimized thread counts, with each pipeline processing `events/N` events through the full chain.

This mode is appropriate when multiple steps have low efficiency, making step 0-only splitting insufficient.

### 9.2 Algorithm

```
function compute_all_step_split(metrics, original_nthreads, request_cpus,
                                 request_memory_mb, uniform, safety_margin):
    PER_THREAD_OVERHEAD = 250
    margin_mult = 1.0 + safety_margin

    # Step 1: compute ideal threads per step
    for each step:
        eff_cores = mean(step.cpu_eff) × original_nthreads
        ideal = nearest_power_of_2(eff_cores) if eff_cores > 0 else original_nthreads
        ideal = min(ideal, original_nthreads)

    # Step 2: iterate n_pipelines from max feasible downward
    for n_pipe in request_cpus..1 (descending):
        threads_cap = request_cpus // n_pipe
        if threads_cap < 1: continue

        max_proj_rss = 0
        for each step:
            if uniform:
                tuned = threads_cap
            else:
                tuned = min(ideal[step], threads_cap)
            tuned = max(tuned, 1)

            # Project RSS with thread reduction
            thread_reduction = original_nthreads - tuned
            proj_rss = measured_rss - thread_reduction × PER_THREAD_OVERHEAD
            proj_rss = max(proj_rss, 500)  # floor
            proj_rss = proj_rss × margin_mult
            max_proj_rss = max(max_proj_rss, proj_rss)

        # Memory check: all pipelines share memory simultaneously
        if n_pipe × max_proj_rss <= request_memory_mb:
            return n_pipe  # first (highest) that fits

    return 1  # fallback: single pipeline
```

### 9.3 Uniform vs Per-Step Threads

| Mode | Flag | Thread assignment | When to use |
|---|---|---|---|
| **Per-step** | (default) | `tuned = min(ideal, threads_cap)` | Steps have varying efficiency; optimize each individually |
| **Uniform** | `--uniform-threads` | `tuned = threads_cap` | Steps have similar efficiency; simpler configuration |

With uniform threads, all steps in each pipeline use the same thread count (`request_cpus // n_pipelines`). This simplifies the sandbox configuration but may over-allocate threads to low-efficiency steps.

With per-step threads, each step uses the minimum of its ideal thread count and the per-pipeline thread cap. This optimizes per-step but requires the sandbox to change thread counts between steps.

---

## 10. Manifest Patching

### 10.1 Purpose

The function `patch_wu_manifests(group_dir, per_step, n_pipelines, max_memory_mb, split_tmpfs)` bridges the gap between algorithm output and sandbox execution. It translates the computed tuning parameters into the format the wrapper script expects.

### 10.2 Behavior

1. **Read** `manifest.json` from the merge group directory (placed during DAG planning)
2. **Modify** per-step fields:
   - `steps[i].multicore` = `per_step[i].tuned_nthreads`
   - `steps[i].n_parallel` = `per_step[i].n_parallel`
3. **Add** top-level fields if applicable:
   - `n_pipelines` (if > 1, for pipeline split mode)
   - `split_tmpfs: true` (if enabled)
4. **Write** `manifest_tuned.json` to the merge group directory
5. **Patch** each `proc_*.sub` submit file:
   - Add `manifest_tuned.json` to `transfer_input_files`
   - If parallel splitting is active (`max n_parallel > 1`) and `max_memory_mb > 0`: update `request_memory` to `max_memory_mb` (only if the new value exceeds the current value)

### 10.3 Sandbox Contract

The proc wrapper script applies the tuned manifest after sandbox extraction:

```bash
if [[ -f manifest_tuned.json ]]; then
    cp manifest_tuned.json manifest.json
fi
```

The sandbox then reads `manifest.json` and applies:
- `steps[i].multicore` → `--nThreads` for each cmsRun invocation
- `steps[i].n_parallel` → number of parallel cmsRun instances for that step (with `skipEvents`/`maxEvents` partitioning)
- `n_pipelines` → number of complete StepChain pipelines to fork
- `split_tmpfs` → extract gridpack to `/dev/shm` instead of working directory

---

## 11. Multi-Round Convergence

### 11.1 Round Progression

```
R1 (WU0): request defaults → baseline metrics
    │
    ├─ replan_0 (analyzes WU0, patches WU1)
    │
R2 (WU1): tuned params → refined metrics
    │
    ├─ replan_1 (analyzes WU0 + WU1, patches WU2)
    │
R3 (WU2): further refined → diminishing returns
    │
    ...
```

Each replan node receives all preceding WU directories via `--prior-wu-dirs`. For replan_0: `mg_000000`. For replan_1: `mg_000000,mg_000001`. This accumulates data across rounds.

### 11.2 Orchestration

The runner (`_execute_adaptive()`) generates replan nodes between consecutive work units:

```
for replan_idx in 0..num_work_units-2:
    # Prior dirs: all WUs from 0 to replan_idx
    prior_dirs = mg_000000,...,mg_{replan_idx}

    # CLI arguments
    args = "replan --prior-wu-dirs {prior_dirs} --wu1-dir {mg_{replan_idx+1}}"
           + " --ncores ... --mem-per-core ... --max-mem-per-core ..."
           + " --safety-margin ... --overcommit-max ..."
           + " --replan-index {replan_idx}"
           + mode-specific flags

    # Probe only in replan_0 (probe node is in WU0)
    if replan_idx == 0 and probe_node:
        args += " --probe-node {probe_node_name}"

    # Submit file: universe=local
    write replan_{replan_idx}.sub

    # DAG dependencies:
    PARENT mg_{replan_idx} CHILD replan_{replan_idx}
    PARENT replan_{replan_idx} CHILD mg_{replan_idx+1}
```

The `universe=local` submit universe ensures replan nodes execute on the submit host (where they have filesystem access to WU directories), not on worker nodes.

### 11.3 Convergence Properties

**Data accumulates**: Multi-round merging (§3.3) concatenates normalized `cpu_eff` from all rounds. With more data points, the mean efficiency estimate becomes more robust, reducing the impact of outlier jobs.

**No oscillation**: Thread counts are quantized to powers of 2 (§2.2). The geometric mean rounding creates discrete, stable states. A step at 4T will stay at 4T unless its efficiency changes enough to cross the 5.657 boundary — and since the data is accumulating (not replacing), the mean is increasingly stable.

**Diminishing returns**: Round 2 captures most gains — memory is sized to actual measurements, splitting is applied to inefficient steps. Round 3+ refines the efficiency estimate with more data but the parameters are already near-optimal. The discrete power-of-2 states mean that small efficiency changes don't alter the tuning.

---

## 12. Worked Examples

### 12.1 Geometric Mean Rounding Table

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

### 12.2 Three-Round Job Split Trace

**Scenario**: Workflow 155.2, 8-core GEN-SIM StepChain, `memory_per_core=2000`, `max_memory_per_core=2500`.

**Round 1** (WU0, 8T baseline):
- Step 0 cpu_eff samples: [0.78, 0.82, 0.84, 0.80] → mean = 0.81
- `eff_cores = 0.81 × 8 = 6.48`
- `tuned = nearest_power_of_2(6.48) = 8` (boundary 5.657, 6.48 > 5.657)
- `job_multiplier = 8 // 8 = 1` → no split
- WU1 runs at same configuration as WU0

**Round 2** (WU1, still 8T):
- Step 0 cpu_eff samples: [0.79, 0.81, 0.83, 0.80] → mean = 0.808
- Round 1 nthreads = 8, Round 2 nthreads = 8 → normalization ratio = 1.0
- Merged cpu_eff (8 samples): mean = 0.809
- `eff_cores = 0.809 × 8 = 6.47`
- `tuned = nearest_power_of_2(6.47) = 8` → still no split
- WU2 runs at same configuration

**Round 3** (WU2, still 8T):
- Step 0 cpu_eff samples: [0.73, 0.75, 0.72, 0.74] → mean = 0.735 (lower this round)
- Merged cpu_eff (12 samples total): mean = 0.784
- `eff_cores = 0.784 × 8 = 6.27`
- `tuned = nearest_power_of_2(6.27) = 8` (still above 5.657 boundary)
- No split — 3 rounds of data confirms the workflow is efficient enough at 8T

**Contrast**: If Round 1 had shown cpu_eff = 0.65:
- `eff_cores = 0.65 × 8 = 5.2`
- `tuned = nearest_power_of_2(5.2) = 4` (5.2 < 5.657)
- `job_multiplier = 8 // 4 = 2` → split to 2× more 4T jobs

### 12.3 Memory Source Fallback Scenarios

**Scenario A**: Probe + cgroup available (best case for per-step tuning)

| Available data | Selected source | Calculation |
|---|---|---|
| probe: `job_peak = 6200 MB, 2 instances` | `probe_peak` (per-step tuning, priority 1) | `marginal = (6200-3000)/2 = 1600`; `instance = 1600 × 1.20 = 1920 MB` |
| cgroup: `tmpfs_peak_nonreclaim = 4500 MB` | — (lower priority) | — |
| FJR: `avg_rss = 1800 MB` | — (lower priority) | — |

**Scenario B**: Probe available, no cgroup (common in early deployment)

| Available data | Selected source | Calculation |
|---|---|---|
| probe: `job_peak = 6200 MB, 2 instances` | `probe_peak` (priority 1) | `marginal = (6200-3000)/2 = 1600`; `instance = 1600 × 1.20 = 1920 MB` |
| FJR: `avg_rss = 1800 MB` | — (lower priority) | — |

**Scenario C**: No probe, no cgroup (fallback)

| Available data | Selected source | Calculation |
|---|---|---|
| FJR: `avg_rss = 1800 MB` | `theoretical` (per-step tuning) | `1800 × 1.20 + 1500 = 3660 MB` |

**Scenario D**: Job split with cgroup, tmpfs enabled

| Available data | Selected source | Calculation |
|---|---|---|
| cgroup: `tmpfs_peak_nonreclaim = 4500 MB, no_tmpfs_peak_anon = 3200 MB` | `cgroup_measured` | `binding = max(4500, 3200) = 4500`; `4500 × 1.20 = 5400 MB` |

**Scenario E**: Job split, no probe, no cgroup, tmpfs enabled

| Available data | Selected source | Calculation |
|---|---|---|
| FJR: `peak_rss = 1800 MB`, step0 avg = 1500 MB | `prior_rss` | `effective_peak = max(1800, 1500+2000) = 3500`; `max(3500×1.20, 3500+1000) = max(4200, 4500) = 4500 MB` |

---

## 13. CLI Reference

### 13.1 Arguments

The replan CLI is invoked as `python -m tests.matrix.adaptive replan` with the following arguments:

| Argument | Type | Required | Default | Description |
|---|---|---|---|---|
| `--prior-wu-dirs` | string | Yes* | `""` | Comma-separated list of prior WU directory paths (all completed rounds) |
| `--wu0-dir` | string | No | `""` | Legacy: single completed WU0 dir (use `--prior-wu-dirs` instead) |
| `--wu1-dir` | string | Yes | — | Target WU directory to patch with tuned parameters |
| `--ncores` | int | Yes | — | Cores per job (`request_cpus`) |
| `--mem-per-core` | int | Yes | — | Default MB per core for Round 1 memory floor |
| `--max-mem-per-core` | int | Yes | — | Maximum MB per core for memory ceiling |
| `--safety-margin` | float | No | 0.20 | Fractional safety margin on measured memory |
| `--overcommit-max` | float | No | 1.0 | Max CPU overcommit ratio (1.0 = disabled) |
| `--no-split` | flag | No | false | Disable step 0 parallel splitting |
| `--split-all-steps` | flag | No | false | Enable pipeline split (supersedes `--no-split`) |
| `--uniform-threads` | flag | No | false | Uniform nThreads across pipeline split steps |
| `--split-tmpfs` | flag | No | false | Enable tmpfs for parallel split instance working dirs |
| `--job-split` | flag | No | false | Enable adaptive job split mode |
| `--events-per-job` | int | No | 0 | Events per job (required with `--job-split`) |
| `--num-jobs` | int | No | 0 | Jobs per work unit (required with `--job-split`) |
| `--probe-node` | string | No | `""` | Probe node name (e.g., `proc_000001`) for memory estimation |
| `--replan-index` | int | No | 0 | Replan index (0-based) for decision file naming |

*Either `--prior-wu-dirs` or `--wu0-dir` is required. `--prior-wu-dirs` takes precedence.

### 13.2 Decision JSON

Each replan node writes a decision file at `{wf_dir}/replan_{index}_decisions.json`. This file records all inputs and outputs of the replan algorithm for debugging and performance reporting.

**Key fields**:

| Field | Type | Description |
|---|---|---|
| `original_nthreads` | int | Original thread count from manifest |
| `overcommit_max` | float | CPU overcommit ratio used |
| `safety_margin` | float | Memory safety margin used |
| `n_pipelines` | int | Number of pipelines (1 for per-step tuning and job split) |
| `memory_per_core_mb` | int | Default memory per core |
| `max_memory_per_core_mb` | int | Max memory per core |
| `ideal_memory_mb` | int | Memory the algorithm ideally wants (before capping) |
| `actual_memory_mb` | int | Memory actually allocated (after capping) |
| `rounds_analyzed` | int | Number of prior rounds merged |
| `per_round_nthreads` | list[int] | nThreads from each analyzed round |
| `per_step` | dict | Per-step tuning details (see §7.5) |
| `probe_data` | dict | Probe metrics (if probe was used, see §4.4) |
| `probe_node` | string | Probe node name (if probe was used) |

**Job split additional fields**:

| Field | Type | Description |
|---|---|---|
| `job_multiplier` | int | Factor by which job count was multiplied |
| `tuned_nthreads` | int | New thread count per job |
| `new_num_jobs` | int | New total job count |
| `new_events_per_job` | int | New events per job |
| `new_request_cpus` | int | New request_cpus per job |
| `new_request_memory_mb` | int | New request_memory per job |
| `memory_source` | string | Which memory source was selected |

### 13.3 DAGMan Integration

Replan nodes are submitted as `universe=local` HTCondor jobs:

```
universe = local
executable = /path/to/.venv/bin/python
arguments = -m tests.matrix.adaptive replan --prior-wu-dirs ... --wu1-dir ...
output = replan_0.out
error = replan_0.err
log = replan_0.log
queue 1
```

**Why `universe=local`**: Replan nodes need filesystem access to WU directories (to read metrics and patch submit files). Local universe jobs run on the submit host where the submit directory is on the local filesystem.

**DAG dependency chain**:

```
PARENT mg_000000 CHILD replan_0
PARENT replan_0 CHILD mg_000001
PARENT mg_000001 CHILD replan_1
PARENT replan_1 CHILD mg_000002
```

This ensures each replan runs after its predecessor WU completes and before its successor WU starts. DAGMan enforces strict ordering via these parent-child edges.

**Lazy SUBDAG file reading**: The merge group directories are `SUBDAG EXTERNAL` nodes in the top-level workflow DAG. DAGMan reads the `group.dag` file for each SUBDAG when the node becomes ready to execute, not at outer DAG submission time. This means the replan node can safely rewrite `group.dag` (as in job split mode, §8.4) before DAGMan reads it.

---

## Appendix A: Constants Reference

| Constant | Value | Unit | Used in | Rationale |
|---|---|---|---|---|
| `SANDBOX_OVERHEAD` | 3000 | MB | Per-step tuning (§7), Job split (§8) | CMSSW project area + shared libraries + ROOT framework, loaded once per job regardless of parallel instances |
| `TMPFS_PER_INSTANCE` (per-step) | 1500 | MB | Per-step tuning (§5.2) | Gridpack extraction on /dev/shm: measured ~1.4 GB, rounded up for margin |
| `TMPFS_PER_INSTANCE` (job split) | 2000 | MB | Job split (§5.3) | Higher estimate for single-instance: full tmpfs lifecycle + concurrent I/O overhead |
| `CGROUP_HEADROOM` | 1000 | MB | Job split (§5.3, prior_rss fallback) | Absolute minimum headroom covering page cache, kernel overhead, and cgroup accounting artifacts |
| `PER_THREAD_OVERHEAD` | 250 | MB | Overcommit (§7.4), Pipeline split (§9) | Conservative CMSSW per-thread cost: thread stacks, TLS, per-thread caches, ROOT per-thread buffers |
| `MAX_PARALLEL` | 4 | instances | Per-step tuning (§7.2) | Limits tmpfs memory for gridpack extraction (4 × ~1.4 GB = 5.6 GB) |
| `safety_margin` (default) | 0.20 | fraction | All modes | 20% buffer for memory leak accumulation, event-to-event RSS variation, page cache pressure |
| Marginal floor | 500 | MB | Per-step tuning (§5.2), Job split (§5.3) | Minimum per-instance memory; prevents unrealistic estimates from suspect or noisy probe data |
| Pipeline RSS floor | 500 | MB | Pipeline split (§9.2) | Minimum projected RSS per pipeline; prevents negative values from thread reduction projection |
| Power-of-2 max | 64 | threads | Thread rounding (§2.2) | Upper bound for `_nearest_power_of_2()`; no CMS workflow uses more than 64 threads |

---

## Appendix B: Decision JSON Schema

Full field listing for `replan_{index}_decisions.json`:

**Common fields** (all modes):

| Field | Type | Required | Description |
|---|---|---|---|
| `original_nthreads` | int | Yes | Original thread count from WU1's manifest |
| `overcommit_max` | float | Yes | CPU overcommit ratio (1.0 = disabled) |
| `safety_margin` | float | Yes | Memory safety margin fraction |
| `n_pipelines` | int | Yes | Pipeline count (1 for per-step tuning and job split) |
| `memory_per_core_mb` | int | Yes | Default memory per core (Round 1 floor) |
| `max_memory_per_core_mb` | int | Yes | Maximum memory per core (ceiling) |
| `rounds_analyzed` | int | Yes | Number of prior rounds whose metrics were merged |
| `per_round_nthreads` | list[int] | Yes | Thread count from each analyzed round (for normalization audit) |
| `per_step` | dict[str, StepTuning] | Yes | Per-step tuning parameters (keys are string step indices) |

**Per-step tuning mode additional fields**:

| Field | Type | Required | Description |
|---|---|---|---|
| `ideal_memory_mb` | int | Yes | Memory the algorithm ideally wants (before capping) |
| `actual_memory_mb` | int | Yes | Memory actually allocated (after cap/floor) |
| `probe_data` | ProbeResult | No | Probe metrics if probe was used |
| `probe_node` | string | No | Probe node name if probe was used |

**Job split mode additional fields**:

| Field | Type | Required | Description |
|---|---|---|---|
| `job_multiplier` | int | Yes | Factor by which job count was multiplied |
| `tuned_nthreads` | int | Yes | New thread count per job |
| `new_num_jobs` | int | Yes | New total job count for this WU |
| `new_events_per_job` | int | Yes | New events per job |
| `new_request_cpus` | int | Yes | New request_cpus per job |
| `new_request_memory_mb` | int | Yes | New request_memory per job |
| `memory_source` | string | Yes | Selected memory source: `probe_peak`, `cgroup_measured`, `probe_rss`, `prior_rss`, `default` |

**StepTuning object**:

| Field | Type | Description |
|---|---|---|
| `tuned_nthreads` | int | Thread count for this step |
| `n_parallel` | int | Parallel instances for this step (>1 only for step 0 in per-step mode) |
| `cpu_eff` | float | Mean CPU efficiency (0.0–1.0, normalized if multi-round) |
| `effective_cores` | float | `cpu_eff × original_nthreads` |
| `overcommit_applied` | bool | Whether CPU overcommit was applied to this step |
| `projected_rss_mb` | float or null | Projected RSS with overcommit threads (null if no overcommit) |
| `ideal_n_parallel` | int | (step 0 only) Ideal parallel count before memory reduction |
| `ideal_memory_mb` | int | (step 0 only) Memory needed for ideal configuration |
| `memory_source` | string | (step 0 only) Memory data source used |
| `instance_mem_mb` | int | (step 0 only) Per-instance memory estimate |
