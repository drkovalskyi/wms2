"""Adaptive execution — analyze completed work unit metrics and tune parameters.

Core adaptive algorithms for inter-round optimization. Given metrics from
completed work units, these functions compute optimal resource parameters
for the next production round.

Optimization is composable across independent dimensions:
  1. Memory sizing   — cgroup/FJR RSS → request_memory with safety margin
  2. Job splitting   — effective cores → request_cpus reduction, job multiplication
  3. Internal parallelism — step 0 splitting (N instances) or all-step pipeline split
  4. Work group sizing — measured output sizes → jobs_per_work_unit

Round 0 is the probe — controlled by first_round_work_units (default 1).
No separate probe node design needed.

Functions:
  analyze_wu_metrics       — read proc_*_metrics.json from a completed merge group
  load_cgroup_metrics      — read proc_*_cgroup.json for subprocess memory data
  merge_round_metrics      — merge metrics across rounds with normalization
  compute_per_step_nthreads — derive nThreads per step from CPU efficiency
  compute_job_split        — adaptive job split (more jobs, fewer cores)
  compute_all_step_split   — N-pipeline split for all steps
  patch_wu_manifests       — write manifest_tuned.json into merge group dirs
  rewrite_wu_for_job_split — rewrite WU DAG for job split
  compute_round_optimization — orchestrator composing all dimensions
"""

from __future__ import annotations

import json
import logging
import math
import re
from pathlib import Path

logger = logging.getLogger(__name__)


# ── Metrics analysis ─────────────────────────────────────────


def analyze_wu_metrics(group_dir: Path, exclude_nodes: set[str] | None = None) -> dict:
    """Read proc_*_metrics.json from a completed merge group.

    Returns aggregated per-step metrics:
      {
        "steps": {
          0: {"wall_sec": [...], "cpu_eff": [...], "peak_rss_mb": [...], ...},
          ...
        },
        "peak_rss_mb": float,          # max across all steps/jobs
        "weighted_cpu_eff": float,      # wall-time-weighted CPU efficiency
        "effective_cores": float,       # weighted_cpu_eff * nthreads
        "num_jobs": int,
        "nthreads": int,
      }
    """
    metrics_pattern = re.compile(r"proc_(\d+)_metrics\.json$")
    steps: dict[int, dict[str, list]] = {}
    num_jobs = 0
    nthreads = 1

    # Also check unmerged storage for metrics files
    search_dirs = [group_dir]
    output_info_path = group_dir / "output_info.json"
    if output_info_path.exists():
        try:
            oi = json.loads(output_info_path.read_text())
            pfx = oi.get("local_pfn_prefix", "")
            gi = oi.get("group_index", 0)
            for ds in oi.get("output_datasets", []):
                ub = ds.get("unmerged_lfn_base", "")
                if ub and pfx:
                    udir = Path(pfx) / ub.lstrip("/") / f"{gi:06d}"
                    if udir.is_dir():
                        search_dirs.append(udir)
        except Exception:
            pass

    for search_dir in search_dirs:
        for mf in sorted(search_dir.glob("proc_*_metrics.json")):
            if not metrics_pattern.search(mf.name):
                continue
            # Skip excluded nodes (e.g. probe node with different config).
            # Node names are "proc_000001" but files are "proc_1_metrics.json",
            # so normalize both to the integer index for comparison.
            if exclude_nodes:
                file_idx_match = re.search(r"proc_(\d+)_metrics", mf.name)
                if file_idx_match:
                    file_idx = int(file_idx_match.group(1))
                    excluded_indices = set()
                    for en in exclude_nodes:
                        en_idx_match = re.search(r"proc_0*(\d+)$", en)
                        if en_idx_match:
                            excluded_indices.add(int(en_idx_match.group(1)))
                    if file_idx in excluded_indices:
                        logger.info("Excluding %s from WU metrics (probe node)", mf.name)
                        continue
            try:
                data = json.loads(mf.read_text())
            except Exception as exc:
                logger.warning("Failed to read %s: %s", mf, exc)
                continue

            num_jobs += 1
            for step_data in data:
                si = step_data.get("step_index")
                if si is None:
                    continue
                s = steps.setdefault(si, {
                    "wall_sec": [], "cpu_eff": [], "peak_rss_mb": [],
                    "events": [], "throughput": [], "cpu_time_sec": [],
                    "nthreads": [],
                })
                if step_data.get("wall_time_sec"):
                    s["wall_sec"].append(step_data["wall_time_sec"])
                if step_data.get("cpu_efficiency"):
                    s["cpu_eff"].append(step_data["cpu_efficiency"])
                if step_data.get("peak_rss_mb"):
                    s["peak_rss_mb"].append(step_data["peak_rss_mb"])
                if step_data.get("events_processed"):
                    s["events"].append(step_data["events_processed"])
                if step_data.get("throughput_ev_s"):
                    s["throughput"].append(step_data["throughput_ev_s"])
                if step_data.get("cpu_time_sec"):
                    s["cpu_time_sec"].append(step_data["cpu_time_sec"])
                nt = step_data.get("num_threads")
                if nt:
                    s["nthreads"].append(nt)
                    if nt > nthreads:
                        nthreads = nt

        if num_jobs > 0:
            break  # found metrics in this dir, don't look further

    if num_jobs == 0:
        raise ValueError(f"No proc_*_metrics.json found in {group_dir}")

    # Aggregate
    peak_rss_all = 0.0
    weighted_eff_num = 0.0
    weighted_eff_den = 0.0

    for si in sorted(steps):
        s = steps[si]
        rss_vals = s["peak_rss_mb"]
        if rss_vals:
            peak_rss_all = max(peak_rss_all, max(rss_vals))
        wall_vals = s["wall_sec"]
        eff_vals = s["cpu_eff"]
        if wall_vals and eff_vals:
            mean_wall = sum(wall_vals) / len(wall_vals)
            mean_eff = sum(eff_vals) / len(eff_vals)
            weighted_eff_num += mean_eff * mean_wall
            weighted_eff_den += mean_wall

    weighted_cpu_eff = weighted_eff_num / weighted_eff_den if weighted_eff_den > 0 else 0.5
    effective_cores = weighted_cpu_eff * nthreads

    result = {
        "steps": steps,
        "peak_rss_mb": peak_rss_all,
        "weighted_cpu_eff": weighted_cpu_eff,
        "effective_cores": effective_cores,
        "num_jobs": num_jobs,
        "nthreads": nthreads,
    }
    # Attach cgroup monitor data if available (captures subprocess memory
    # that FJR PeakValueRss misses)
    cgroup = load_cgroup_metrics(group_dir, exclude_nodes=exclude_nodes)
    if cgroup:
        result["cgroup"] = cgroup
    return result


def merge_round_metrics(round_metrics: list[dict], original_nthreads: int) -> dict:
    """Merge metrics from multiple rounds, normalizing cpu_eff to original_nthreads.

    When a round ran at reduced thread count (e.g. 2T instead of 8T),
    its raw cpu_eff overstates effective cores:
        eff_cores = raw_eff × original_nt   (WRONG if run at 2T)
    Normalization: normalized_eff = raw_eff × actual_nt / original_nt
    so that:  eff_cores = normalized_eff × original_nt = raw_eff × actual_nt  (CORRECT)

    Strategy:
    - cpu_eff: Normalize each round's values using per-step nthreads,
      then concatenate from ALL rounds (more data = better estimate).
    - RSS, wall, events, throughput, cpu_time: Most recent round only
      (best predictor at the current thread count).
    - nthreads: Set to original_nthreads (matches normalized reference frame).
    - Recompute weighted_cpu_eff and effective_cores from normalized values.
    """
    if not round_metrics:
        raise ValueError("No round metrics to merge")

    # Single round: normalize in place and return
    if len(round_metrics) == 1:
        m = round_metrics[0]
        round_nt = m["nthreads"]
        for si, s in m["steps"].items():
            step_nts = s.get("nthreads", [])
            step_nt = (sum(step_nts) / len(step_nts)) if step_nts else round_nt
            if step_nt != original_nthreads and original_nthreads > 0:
                ratio = step_nt / original_nthreads
                s["cpu_eff"] = [e * ratio for e in s["cpu_eff"]]
        # Recompute aggregates
        m["nthreads"] = original_nthreads
        weighted_num = 0.0
        weighted_den = 0.0
        for si in sorted(m["steps"]):
            s = m["steps"][si]
            wall_vals = s["wall_sec"]
            eff_vals = s["cpu_eff"]
            if wall_vals and eff_vals:
                mean_wall = sum(wall_vals) / len(wall_vals)
                mean_eff = sum(eff_vals) / len(eff_vals)
                weighted_num += mean_eff * mean_wall
                weighted_den += mean_wall
        m["weighted_cpu_eff"] = weighted_num / weighted_den if weighted_den > 0 else 0.5
        m["effective_cores"] = m["weighted_cpu_eff"] * original_nthreads
        # job_peak_mb already present if collected
        return m

    # Multiple rounds: merge cpu_eff from all, other fields from latest
    latest = round_metrics[-1]
    merged_steps: dict[int, dict[str, list]] = {}

    # Initialize from latest round (RSS, wall, events, throughput, cpu_time)
    for si, s in latest["steps"].items():
        merged_steps[si] = {
            "wall_sec": list(s["wall_sec"]),
            "cpu_eff": [],  # will be filled from all rounds
            "peak_rss_mb": list(s["peak_rss_mb"]),
            "events": list(s["events"]),
            "throughput": list(s["throughput"]),
            "cpu_time_sec": list(s["cpu_time_sec"]),
            "nthreads": [],
        }

    # Collect normalized cpu_eff and peak RSS from ALL rounds/WUs
    for rm in round_metrics:
        round_nt = rm["nthreads"]
        for si, s in rm["steps"].items():
            if si not in merged_steps:
                merged_steps[si] = {
                    "wall_sec": list(s["wall_sec"]),
                    "cpu_eff": [],
                    "peak_rss_mb": list(s["peak_rss_mb"]),
                    "events": list(s["events"]),
                    "throughput": list(s["throughput"]),
                    "cpu_time_sec": list(s["cpu_time_sec"]),
                    "nthreads": [],
                }
            else:
                # Extend RSS from all entries (not just latest) so max()
                # covers all WUs, not just the last one.
                merged_steps[si]["peak_rss_mb"].extend(s["peak_rss_mb"])
            step_nts = s.get("nthreads", [])
            step_nt = (sum(step_nts) / len(step_nts)) if step_nts else round_nt
            ratio = step_nt / original_nthreads if original_nthreads > 0 else 1.0
            merged_steps[si]["cpu_eff"].extend(e * ratio for e in s["cpu_eff"])

    # Recompute aggregates
    peak_rss_all = 0.0
    weighted_num = 0.0
    weighted_den = 0.0
    for si in sorted(merged_steps):
        s = merged_steps[si]
        rss_vals = s["peak_rss_mb"]
        if rss_vals:
            peak_rss_all = max(peak_rss_all, max(rss_vals))
        wall_vals = s["wall_sec"]
        eff_vals = s["cpu_eff"]
        if wall_vals and eff_vals:
            mean_wall = sum(wall_vals) / len(wall_vals)
            mean_eff = sum(eff_vals) / len(eff_vals)
            weighted_num += mean_eff * mean_wall
            weighted_den += mean_wall

    weighted_cpu_eff = weighted_num / weighted_den if weighted_den > 0 else 0.5

    result = {
        "steps": merged_steps,
        "peak_rss_mb": peak_rss_all,
        "weighted_cpu_eff": weighted_cpu_eff,
        "effective_cores": weighted_cpu_eff * original_nthreads,
        "num_jobs": latest["num_jobs"],
        "nthreads": original_nthreads,
    }
    # Propagate cgroup data — use max peak_nonreclaim_mb across ALL entries,
    # not just the latest.  Each entry is one work unit; completion order is
    # arbitrary, so taking the last one's cgroup would pick a random WU's peak
    # instead of the true worst-case.
    best_cgroup = None
    best_peak = 0
    for rm in round_metrics:
        cg = rm.get("cgroup")
        if cg and cg.get("peak_nonreclaim_mb", 0) > best_peak:
            best_peak = cg["peak_nonreclaim_mb"]
            best_cgroup = cg
    if best_cgroup:
        result["cgroup"] = best_cgroup
    return result


# ── Cgroup memory metrics ────────────────────────────────────


def load_cgroup_metrics(
    group_dir: Path, exclude_nodes: set[str] | None = None,
) -> dict | None:
    """Load cgroup monitor data from proc_*_cgroup.json files.

    These files are generated by the wrapper's memory monitor and contain
    peak non-reclaimable memory (anon + shmem) measured from cgroup
    memory.stat.  Unlike FJR PeakValueRss, this captures ALL processes
    in the cgroup (including ExternalLHEProducer subprocesses).

    Returns aggregated peaks across all proc jobs, or None if no files found.
    """
    cgroup_pattern = re.compile(r"proc_(\d+)_cgroup\.json$")

    # Search same dirs as analyze_wu_metrics
    search_dirs = [group_dir]
    output_info_path = group_dir / "output_info.json"
    if output_info_path.exists():
        try:
            oi = json.loads(output_info_path.read_text())
            pfx = oi.get("local_pfn_prefix", "")
            gi = oi.get("group_index", 0)
            for ds in oi.get("output_datasets", []):
                ub = ds.get("unmerged_lfn_base", "")
                if ub and pfx:
                    udir = Path(pfx) / ub.lstrip("/") / f"{gi:06d}"
                    if udir.is_dir():
                        search_dirs.append(udir)
        except Exception:
            pass

    num_jobs = 0
    agg = {
        "peak_anon_mb": 0,
        "peak_shmem_mb": 0,
        "peak_nonreclaim_mb": 0,
        "tmpfs_peak_nonreclaim_mb": 0,
        "no_tmpfs_peak_anon_mb": 0,
        "gridpack_disk_mb": 0,
    }

    for search_dir in search_dirs:
        for cf in sorted(search_dir.glob("proc_*_cgroup.json")):
            if not cgroup_pattern.search(cf.name):
                continue
            # Apply same node exclusion as analyze_wu_metrics
            if exclude_nodes:
                file_idx_match = re.search(r"proc_(\d+)_cgroup", cf.name)
                if file_idx_match:
                    file_idx = int(file_idx_match.group(1))
                    excluded_indices = set()
                    for en in exclude_nodes:
                        en_idx_match = re.search(r"proc_0*(\d+)$", en)
                        if en_idx_match:
                            excluded_indices.add(int(en_idx_match.group(1)))
                    if file_idx in excluded_indices:
                        continue
            try:
                data = json.loads(cf.read_text())
            except Exception as exc:
                logger.warning("Failed to read %s: %s", cf, exc)
                continue

            num_jobs += 1
            for key in agg:
                agg[key] = max(agg[key], data.get(key, 0))

        if num_jobs > 0:
            break  # found files in this dir, don't look further

    if num_jobs == 0:
        return None

    agg["num_jobs"] = num_jobs
    return agg


# ── Probe metrics analysis ───────────────────────────────────


# ── Per-step nThreads tuning ─────────────────────────────────


def _nearest_power_of_2(n: float) -> int:
    """Round to the nearest power of 2, minimum 1, maximum 64.

    Uses geometric mean as midpoint: e.g. between 4 and 8,
    the midpoint is sqrt(4*8)=5.66, so 4.3 rounds to 4.
    """
    if n <= 1:
        return 1
    p = 1
    while p < 64:
        next_p = p * 2
        if n <= math.sqrt(p * next_p):
            return p
        p = next_p
    return 64


def compute_per_step_nthreads(
    metrics: dict,
    original_nthreads: int,
    request_cpus: int = 0,
    default_memory_mb: int = 0,
    max_memory_mb: int = 0,
    overcommit_max: float = 1.0,
    split: bool = True,
    probe_rss_mb: float = 0,
    probe_job_peak_total_mb: float = 0,
    probe_num_instances: int = 0,
    safety_margin: float = 0.20,
    cgroup: dict | None = None,
    min_threads: int = 2,
) -> dict:
    """Derive optimal nThreads for step 0 parallel splitting and optional overcommit.

    Key inputs:
      request_cpus        — max cores per job (ncores)
      default_memory_mb   — floor for request_memory (default_mem_per_core * ncores)
      max_memory_mb       — ceiling for request_memory (max_mem_per_core * ncores)
      overcommit_max      — max CPU overcommit ratio (1.0 = disabled)
      safety_margin       — fractional margin on measured memory (0.20 = 20%)
      probe_job_peak_total_mb — total job peak from probe (not per-instance)
      probe_num_instances — number of instances in the probe job

    Memory sizing follows spec Section 5.5: measured data × (1 + safety_margin),
    clamped to [default_memory_mb, max_memory_mb].

    Returns dict with per-step tuning details.
    """
    margin_mult = 1.0 + safety_margin
    # Conservative per-thread memory overhead for CMSSW (MB)
    PER_THREAD_OVERHEAD_MB = 250

    per_step = {}
    for si in sorted(metrics["steps"]):
        step = metrics["steps"][si]
        eff_vals = step["cpu_eff"]
        rss_vals = step["peak_rss_mb"]
        if eff_vals:
            mean_eff = sum(eff_vals) / len(eff_vals)
            eff_cores = mean_eff * original_nthreads
        else:
            mean_eff = 0.0
            eff_cores = 0.0
        avg_rss = sum(rss_vals) / len(rss_vals) if rss_vals else 0

        # Parallel splitting: only step 0 is eligible
        n_par = 1
        tuned = original_nthreads
        overcommit_applied = False
        projected_rss_mb = None
        ideal_n_par = 1
        ideal_memory = max_memory_mb

        if si == 0 and split and request_cpus > 0 and eff_cores > 0:
            TMPFS_PER_INSTANCE_MB = 1500
            tuned = _nearest_power_of_2(eff_cores)
            tuned = min(tuned, original_nthreads)
            tuned = max(tuned, min_threads)
            n_par = request_cpus // tuned
            # Cap parallel instances to limit tmpfs usage for gridpack
            # extraction (each uses ~1.4 GB counted against job memory).
            MAX_PARALLEL = 4
            n_par = min(n_par, MAX_PARALLEL)
            n_par = max(n_par, 1)

            ideal_n_par = n_par
            ideal_tuned = tuned

            # Per-instance memory for the memory model (spec Section 5.5).
            SANDBOX_OVERHEAD_MB = 3000
            if probe_job_peak_total_mb > 0 and probe_num_instances > 0:
                marginal = (probe_job_peak_total_mb - SANDBOX_OVERHEAD_MB) / probe_num_instances
                marginal = max(marginal, 500)
                instance_mem = marginal * margin_mult
                memory_source = "probe_peak"
            elif cgroup is not None and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0:
                instance_mem = cgroup["tmpfs_peak_nonreclaim_mb"] * margin_mult
                instance_mem = max(instance_mem, 500)
                memory_source = "cgroup_measured"
            elif probe_rss_mb > 0:
                instance_mem = probe_rss_mb * margin_mult + TMPFS_PER_INSTANCE_MB
                memory_source = "probe"
            else:
                instance_mem = avg_rss * margin_mult + TMPFS_PER_INSTANCE_MB
                memory_source = "theoretical"

            # Memory-aware reduction
            effective_max = max_memory_mb if max_memory_mb > 0 else 0
            if effective_max > 0 and n_par > 1 and instance_mem > 0:
                candidates = sorted(
                    [p for p in range(n_par, 1, -1) if request_cpus % p == 0],
                    reverse=True,
                )
                candidates += sorted(
                    [p for p in range(n_par, 1, -1) if request_cpus % p != 0],
                    reverse=True,
                )
                found = False
                for p in candidates:
                    t = max(request_cpus // p, min_threads)
                    total = SANDBOX_OVERHEAD_MB + p * instance_mem
                    if total <= effective_max:
                        n_par = p
                        tuned = t
                        found = True
                        break
                if not found:
                    n_par = 1

            if n_par <= 1:
                tuned = original_nthreads
                n_par = 1

            # Compute ideal memory for reporting
            if ideal_n_par > 1 and instance_mem > 0:
                ideal_memory = SANDBOX_OVERHEAD_MB + ideal_n_par * instance_mem
            else:
                ideal_memory = max_memory_mb

            # Step 0 overcommit
            if overcommit_max > 1.0 and n_par > 1 and instance_mem > 0:
                tuned_oc = min(
                    round(tuned * overcommit_max),
                    int(original_nthreads * overcommit_max),
                )
                extra_threads = tuned_oc - tuned
                if extra_threads > 0 and max_memory_mb > 0:
                    proj_mem = instance_mem + extra_threads * PER_THREAD_OVERHEAD_MB
                    total_proj = SANDBOX_OVERHEAD_MB + n_par * proj_mem
                    if total_proj <= max_memory_mb:
                        projected_rss_mb = proj_mem
                        tuned = tuned_oc
                        overcommit_applied = True
                    else:
                        avail = max_memory_mb - SANDBOX_OVERHEAD_MB
                        safe_per_inst = avail / n_par if avail > 0 else 0
                        safe_extra = max(0, int((safe_per_inst - instance_mem) / PER_THREAD_OVERHEAD_MB))
                        if safe_extra > 0:
                            tuned_safe = tuned + safe_extra
                            projected_rss_mb = instance_mem + safe_extra * PER_THREAD_OVERHEAD_MB
                            tuned = tuned_safe
                            overcommit_applied = True

        elif (si > 0 or not split) and overcommit_max > 1.0 and eff_vals and avg_rss > 0:
            if 0.50 <= mean_eff < 0.90:
                oc_threads = round(original_nthreads * overcommit_max)
                extra_threads = oc_threads - original_nthreads
                if extra_threads > 0 and max_memory_mb > 0:
                    proj_rss = avg_rss + extra_threads * PER_THREAD_OVERHEAD_MB
                    if proj_rss <= max_memory_mb:
                        tuned = oc_threads
                        projected_rss_mb = proj_rss
                        overcommit_applied = True
                    else:
                        safe_extra = max(0, int((max_memory_mb - avg_rss) / PER_THREAD_OVERHEAD_MB))
                        if safe_extra > 0 and original_nthreads + safe_extra > original_nthreads:
                            tuned = original_nthreads + safe_extra
                            projected_rss_mb = avg_rss + safe_extra * PER_THREAD_OVERHEAD_MB
                            overcommit_applied = True

        entry = {
            "tuned_nthreads": tuned,
            "n_parallel": n_par,
            "cpu_eff": mean_eff,
            "effective_cores": eff_cores,
            "overcommit_applied": overcommit_applied,
            "projected_rss_mb": projected_rss_mb,
        }
        if si == 0 and split:
            entry["ideal_n_parallel"] = ideal_n_par
            entry["ideal_memory_mb"] = round(ideal_memory)
            entry["memory_source"] = memory_source
            entry["instance_mem_mb"] = round(instance_mem)
        per_step[si] = entry

    return {
        "original_nthreads": original_nthreads,
        "per_step": per_step,
    }


# ── Adaptive job split ───────────────────────────────────────


def compute_job_split(
    metrics: dict,
    original_nthreads: int,
    request_cpus: int,
    memory_per_core_mb: int,
    max_memory_per_core_mb: int,
    events_per_job: int,
    num_jobs_wu: int,
    safety_margin: float = 0.20,
    probe_job_peak_total_mb: float = 0,
    probe_num_instances: int = 0,
    probe_rss_mb: float = 0,
    split_tmpfs: bool = False,
    cgroup: dict | None = None,
    last_round_nthreads: int = 0,
    min_threads: int = 2,
    min_request_cpus: int = 4,
) -> dict:
    """Compute adaptive job split: more jobs with fewer cores per job.

    Instead of splitting step 0 into parallel instances within one job,
    split the jobs themselves — run N× more jobs with N× fewer cores.
    Same total core allocation, but each job finishes independently
    (no tail effect, resources released sooner).

    The min_request_cpus floor (default 4) prevents pool fragmentation —
    jobs with very few cores are harder to schedule and fragment the pool.

    Memory follows spec Section 5.5: clamp(measured × (1 + safety_margin),
    default_per_core × tuned_cores, max_per_core × tuned_cores).
    """
    step0 = metrics["steps"].get(0)
    if not step0:
        return {
            "tuned_nthreads": original_nthreads,
            "job_multiplier": 1,
            "new_num_jobs": num_jobs_wu,
            "new_events_per_job": events_per_job,
            "new_request_cpus": request_cpus,
            "new_request_memory_mb": memory_per_core_mb * request_cpus,
            "per_step": {},
        }

    eff_vals = step0["cpu_eff"]
    mean_eff = sum(eff_vals) / len(eff_vals) if eff_vals else 0.5
    eff_cores = mean_eff * original_nthreads

    tuned = _nearest_power_of_2(eff_cores)
    # Enforce both legacy min_threads and new min_request_cpus floor
    effective_min = max(min_threads, min_request_cpus)
    tuned = max(tuned, effective_min)
    tuned = min(tuned, original_nthreads)

    job_multiplier = original_nthreads // tuned
    if job_multiplier < 1:
        job_multiplier = 1

    has_tmpfs_data = (cgroup is not None
                      and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0)
    has_gridpack_size = (cgroup is not None
                         and cgroup.get("gridpack_disk_mb", 0) > 0)
    if split_tmpfs and not has_tmpfs_data:
        if has_gridpack_size:
            est_mem = int((cgroup["peak_anon_mb"] + cgroup["gridpack_disk_mb"])
                         * (1.0 + safety_margin))
            while tuned < original_nthreads:
                if tuned * max_memory_per_core_mb >= est_mem:
                    break
                tuned *= 2
            tuned = max(tuned, effective_min)
            tuned = min(tuned, original_nthreads)
            job_multiplier = original_nthreads // tuned
        else:
            prev_nt = last_round_nthreads if last_round_nthreads > 0 else original_nthreads
            min_tuned = max(prev_nt // 2, effective_min)
            if tuned < min_tuned:
                tuned = min_tuned
                job_multiplier = original_nthreads // tuned

    new_events_per_job = events_per_job // job_multiplier
    if new_events_per_job < 1:
        new_events_per_job = 1
        job_multiplier = events_per_job

    new_num_jobs = num_jobs_wu * job_multiplier
    new_request_cpus = tuned

    # Memory sizing (spec Section 5.5)
    SANDBOX_OVERHEAD_MB = 3000
    TMPFS_PER_INSTANCE_MB = 2000
    CGROUP_HEADROOM_MB = 1000
    margin_mult = 1.0 + safety_margin

    peak_rss_mb = 0
    for si in metrics["steps"]:
        step = metrics["steps"][si]
        step_rss = step["peak_rss_mb"]
        if step_rss:
            avg = sum(step_rss) / len(step_rss)
            peak_rss_mb = max(peak_rss_mb, avg)

    if probe_job_peak_total_mb > 0 and probe_num_instances > 0:
        marginal = (probe_job_peak_total_mb - SANDBOX_OVERHEAD_MB) / probe_num_instances
        marginal = max(marginal, 500)
        memory_from_measured = int((SANDBOX_OVERHEAD_MB + marginal) * margin_mult)
        memory_source = "probe_peak"
    elif cgroup is not None and cgroup.get("peak_nonreclaim_mb", 0) > 0:
        if split_tmpfs and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0:
            binding = max(
                cgroup["tmpfs_peak_nonreclaim_mb"],
                cgroup.get("no_tmpfs_peak_anon_mb", 0),
            )
            memory_from_measured = int(binding * margin_mult)
            memory_source = "cgroup_measured"
        elif split_tmpfs and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) == 0:
            gridpack_mb = cgroup.get("gridpack_disk_mb", 0)
            if gridpack_mb > 0:
                binding = cgroup["peak_anon_mb"] + gridpack_mb
                memory_from_measured = int(binding * margin_mult)
                memory_source = "cgroup_gridpack_estimated"
            else:
                memory_from_measured = tuned * max_memory_per_core_mb
                memory_source = "cgroup_no_tmpfs_data"
        else:
            binding = cgroup["peak_nonreclaim_mb"]
            memory_from_measured = int(binding * margin_mult)
            memory_source = "cgroup_measured"
    elif probe_rss_mb > 0:
        memory_from_measured = int(probe_rss_mb * margin_mult + TMPFS_PER_INSTANCE_MB)
        memory_source = "probe_rss"
    elif peak_rss_mb > 0:
        effective_peak = peak_rss_mb
        if split_tmpfs:
            step0_data = metrics["steps"].get(0)
            if step0_data and step0_data["peak_rss_mb"]:
                s0_avg = sum(step0_data["peak_rss_mb"]) / len(step0_data["peak_rss_mb"])
                s0_cgroup_est = s0_avg + TMPFS_PER_INSTANCE_MB
                effective_peak = max(effective_peak, s0_cgroup_est)
        memory_from_measured = max(
            int(effective_peak * margin_mult),
            int(effective_peak) + CGROUP_HEADROOM_MB,
        )
        memory_source = "prior_rss"
    else:
        memory_from_measured = 0
        memory_source = "default"

    memory_floor = tuned * memory_per_core_mb
    memory_ceiling = tuned * max_memory_per_core_mb
    new_request_memory_mb = max(memory_floor, min(memory_from_measured, memory_ceiling))
    if memory_from_measured > memory_ceiling:
        new_request_memory_mb = memory_ceiling

    # Build per-step info
    per_step = {}
    for si in sorted(metrics["steps"]):
        step = metrics["steps"][si]
        step_eff = step["cpu_eff"]
        step_rss = step["peak_rss_mb"]
        step_mean_eff = sum(step_eff) / len(step_eff) if step_eff else 0
        step_eff_cores = step_mean_eff * original_nthreads
        avg_rss = sum(step_rss) / len(step_rss) if step_rss else 0
        per_step[si] = {
            "tuned_nthreads": tuned,
            "cpu_eff": step_mean_eff,
            "effective_cores": step_eff_cores,
            "avg_rss_mb": avg_rss,
            "n_parallel": 1,
            "overcommit_applied": False,
            "projected_rss_mb": None,
        }

    return {
        "tuned_nthreads": tuned,
        "job_multiplier": job_multiplier,
        "new_num_jobs": new_num_jobs,
        "new_events_per_job": new_events_per_job,
        "new_request_cpus": new_request_cpus,
        "new_request_memory_mb": new_request_memory_mb,
        "memory_source": memory_source,
        "per_step": per_step,
    }


def rewrite_wu_for_job_split(
    group_dir: Path,
    split_result: dict,
    max_memory_per_core_mb: int,
    split_tmpfs: bool = False,
) -> dict:
    """Rewrite WU1's DAG for adaptive job split.

    After compute_job_split(), this rewrites the merge group:
    1. Parse one existing proc_*.sub as template
    2. Find WU1's event range from existing submit files
    3. Generate new_num_jobs proc submit files with tuned resources
    4. Delete old proc submit files
    5. Rewrite group.dag with new proc node entries
    6. Write manifest_tuned.json with all steps set to tuned_threads
    """
    tuned_nt = split_result["tuned_nthreads"]
    job_multiplier = split_result["job_multiplier"]
    new_num_jobs = split_result["new_num_jobs"]
    new_events_per_job = split_result["new_events_per_job"]
    new_request_cpus = split_result["new_request_cpus"]
    new_request_memory_mb = split_result["new_request_memory_mb"]

    # 1. Find existing proc submit files and parse one as template
    old_sub_files = sorted(group_dir.glob("proc_*.sub"))
    if not old_sub_files:
        raise FileNotFoundError(f"No proc_*.sub found in {group_dir}")

    template_content = old_sub_files[0].read_text()

    # 2. Extract event range from existing submit files
    first_events = []
    last_events = []
    events_per_jobs = []
    for sub_file in old_sub_files:
        content = sub_file.read_text()
        args_match = re.search(r"^arguments\s*=\s*(.+)$", content, re.MULTILINE)
        if args_match:
            args_str = args_match.group(1)
            fe_match = re.search(r"--first-event\s+(\d+)", args_str)
            le_match = re.search(r"--last-event\s+(\d+)", args_str)
            epj_match = re.search(r"--events-per-job\s+(\d+)", args_str)
            if fe_match:
                first_events.append(int(fe_match.group(1)))
            if le_match:
                last_events.append(int(le_match.group(1)))
            if epj_match:
                events_per_jobs.append(int(epj_match.group(1)))

    if first_events and last_events:
        wu_first_event = min(first_events)
        wu_last_event = max(last_events)
    else:
        wu_first_event = 1
        wu_last_event = len(old_sub_files) * new_events_per_job * job_multiplier

    total_events = wu_last_event - wu_first_event + 1

    # 3. Write manifest_tuned.json
    manifest_path = group_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {group_dir}")
    manifest = json.loads(manifest_path.read_text())
    for step in manifest.get("steps", []):
        step["multicore"] = tuned_nt
        step["n_parallel"] = 1
    if split_tmpfs:
        manifest["split_tmpfs"] = True
    tuned_path = group_dir / "manifest_tuned.json"
    tuned_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote %s", tuned_path)

    # Extract common fields from template
    exe_match = re.search(r"^executable\s*=\s*(.+)$", template_content, re.MULTILINE)
    executable = exe_match.group(1).strip() if exe_match else "wms2_proc.sh"

    tif_match = re.search(r"^transfer_input_files\s*=\s*(.+)$", template_content, re.MULTILINE)
    transfer_files = tif_match.group(1).strip() if tif_match else ""
    if transfer_files:
        transfer_files = f"{transfer_files}, {tuned_path}"
    else:
        transfer_files = str(tuned_path)

    env_match = re.search(r'^environment\s*=\s*"(.+)"$', template_content, re.MULTILINE)
    env_str = env_match.group(1).strip() if env_match else ""

    disk_match = re.search(r"^request_disk\s*=\s*(\d+)", template_content, re.MULTILINE)
    disk_kb = int(disk_match.group(1)) if disk_match else 0

    desired_sites_match = re.search(r'^\+DESIRED_Sites\s*=\s*"(.+)"$', template_content, re.MULTILINE)
    desired_sites = desired_sites_match.group(1) if desired_sites_match else ""

    args_match = re.search(r"^arguments\s*=\s*(.+)$", template_content, re.MULTILINE)
    template_args = args_match.group(1).strip() if args_match else ""
    sandbox_match = re.search(r"--sandbox\s+(\S+)", template_args)
    sandbox_ref = sandbox_match.group(1) if sandbox_match else "sandbox.tar.gz"
    oi_match = re.search(r"--output-info\s+(\S+)", template_args)
    output_info_ref = oi_match.group(1) if oi_match else ""
    pileup_remote_read = "--pileup-remote-read" in template_args

    # Extract SCRIPT PRE/POST patterns from group.dag
    dag_path = group_dir / "group.dag"
    dag_content = dag_path.read_text()

    pre_script_match = re.search(
        r"^SCRIPT PRE proc_\d+ (.+)$", dag_content, re.MULTILINE
    )
    post_script_match = re.search(
        r"^SCRIPT POST proc_\d+ (.+\s+)\S+ \$RETURN$", dag_content, re.MULTILINE
    )
    pre_script_base = ""
    pre_script_arg = ""
    if pre_script_match:
        pre_parts = pre_script_match.group(1).strip()
        pre_base = re.match(r"(\S+)\s+\S+\s+(\S+)", pre_parts)
        if pre_base:
            pre_script_base = pre_base.group(1)
            pre_script_arg = pre_base.group(2)
    post_script_base = ""
    if post_script_match:
        post_script_base = post_script_match.group(1).strip()

    # 4. Delete old proc submit files
    for sub_file in old_sub_files:
        sub_file.unlink()
    logger.info("Deleted %d old proc submit files", len(old_sub_files))

    # 5. Generate new proc submit files
    for i in range(new_num_jobs):
        node_index = i
        node_name = f"proc_{node_index:06d}"

        fe = wu_first_event + i * new_events_per_job
        le = fe + new_events_per_job - 1
        if le > wu_last_event:
            le = wu_last_event

        input_lfn = f"synthetic://gen/events_{fe}_{le}"
        proc_args = f"--sandbox {sandbox_ref} --input {input_lfn}"
        proc_args += f" --node-index {node_index}"
        if output_info_ref:
            proc_args += f" --output-info {output_info_ref}"
        proc_args += f" --first-event {fe}"
        proc_args += f" --last-event {le}"
        proc_args += f" --events-per-job {new_events_per_job}"
        proc_args += f" --ncpus {new_request_cpus}"
        if pileup_remote_read:
            proc_args += " --pileup-remote-read"

        lines = [
            f"# processing node {node_index} (job split)",
            "universe = vanilla",
            f"executable = {executable}",
            f"arguments = {proc_args}",
            f"output = {node_name}.out",
            f"error = {node_name}.err",
            f"log = {node_name}.log",
            f"request_cpus = {new_request_cpus}",
            f"request_memory = {new_request_memory_mb}",
        ]
        if disk_kb > 0:
            lines.append(f"request_disk = {disk_kb}")
        if env_str:
            lines.append(f'environment = "{env_str}"')
        lines.append("should_transfer_files = YES")
        lines.append("when_to_transfer_output = ON_EXIT")
        lines.append(f"transfer_input_files = {transfer_files}")
        if desired_sites:
            lines.append(f'+DESIRED_Sites = "{desired_sites}"')
        lines.append("queue 1")

        sub_path = group_dir / f"{node_name}.sub"
        sub_path.write_text("\n".join(lines) + "\n")

    logger.info("Generated %d new proc submit files "
                "(%d cpus, %d ev/job, %d MB)",
                new_num_jobs, new_request_cpus, new_events_per_job,
                new_request_memory_mb)

    # 6. Rewrite group.dag
    new_dag_lines = [f"# Merge group (job split: {job_multiplier}x)", ""]

    landing_match = re.search(
        r"^JOB landing .+$", dag_content, re.MULTILINE
    )
    if landing_match:
        new_dag_lines.append(landing_match.group(0))
    else:
        new_dag_lines.append("JOB landing landing.sub")
    landing_post = re.search(
        r"^SCRIPT POST landing .+$", dag_content, re.MULTILINE
    )
    if landing_post:
        new_dag_lines.append(landing_post.group(0))
    new_dag_lines.append("")

    proc_names = []
    for i in range(new_num_jobs):
        node_name = f"proc_{i:06d}"
        proc_names.append(node_name)
        new_dag_lines.append(f"JOB {node_name} {node_name}.sub")
        if pre_script_base:
            new_dag_lines.append(
                f"SCRIPT PRE {node_name} {pre_script_base} {node_name}.sub {pre_script_arg}"
            )
        if post_script_base:
            new_dag_lines.append(
                f"SCRIPT POST {node_name} {post_script_base} {node_name} $RETURN"
            )
        new_dag_lines.append("")

    for pattern in [r"^JOB merge .+$", r"^SCRIPT PRE merge .+$"]:
        m = re.search(pattern, dag_content, re.MULTILINE)
        if m:
            new_dag_lines.append(m.group(0))
    new_dag_lines.append("")

    for pattern in [r"^JOB cleanup .+$", r"^SCRIPT PRE cleanup .+$"]:
        m = re.search(pattern, dag_content, re.MULTILINE)
        if m:
            new_dag_lines.append(m.group(0))
    new_dag_lines.append("")

    for name in proc_names:
        new_dag_lines.append(f"RETRY {name} 3 UNLESS-EXIT 42")
    new_dag_lines.append("RETRY merge 2 UNLESS-EXIT 42")
    new_dag_lines.append("RETRY cleanup 1")
    new_dag_lines.append("")

    for name in proc_names:
        new_dag_lines.append(f"ABORT-DAG-ON {name} 43 RETURN 1")
    new_dag_lines.append("ABORT-DAG-ON merge 43 RETURN 1")
    new_dag_lines.append("")

    proc_str = " ".join(proc_names)
    new_dag_lines.append(f"PARENT landing CHILD {proc_str}")
    new_dag_lines.append(f"PARENT {proc_str} CHILD merge")
    new_dag_lines.append("PARENT merge CHILD cleanup")
    new_dag_lines.append("")

    for name in proc_names:
        new_dag_lines.append(f"CATEGORY {name} Processing")
    new_dag_lines.append("CATEGORY merge Merge")
    new_dag_lines.append("CATEGORY cleanup Cleanup")
    for m in re.finditer(r"^MAXJOBS .+$", dag_content, re.MULTILINE):
        new_dag_lines.append(m.group(0))

    dag_path.write_text("\n".join(new_dag_lines) + "\n")
    logger.info("Rewrote %s (%d proc nodes)", dag_path, new_num_jobs)

    return {
        "proc_files_written": new_num_jobs,
        "old_proc_files_deleted": len(old_sub_files),
    }


# ── All-step pipeline split ──────────────────────────────────


def compute_all_step_split(
    metrics: dict,
    original_nthreads: int,
    request_cpus: int,
    request_memory_mb: int,
    uniform: bool = False,
    safety_margin: float = 0.20,
) -> dict:
    """Derive N-pipeline split where each pipeline runs ALL steps with tuned nThreads."""
    PER_THREAD_OVERHEAD_MB = 250

    step_ideals = {}
    step_rss = {}
    for si in sorted(metrics["steps"]):
        step = metrics["steps"][si]
        eff_vals = step["cpu_eff"]
        rss_vals = step["peak_rss_mb"]
        if eff_vals:
            mean_eff = sum(eff_vals) / len(eff_vals)
            eff_cores = mean_eff * original_nthreads
        else:
            mean_eff = 0.0
            eff_cores = 0.0
        avg_rss = sum(rss_vals) / len(rss_vals) if rss_vals else 0
        ideal = _nearest_power_of_2(eff_cores) if eff_cores > 0 else original_nthreads
        ideal = min(ideal, original_nthreads)
        step_ideals[si] = {"ideal": ideal, "cpu_eff": mean_eff, "eff_cores": eff_cores}
        step_rss[si] = avg_rss

    margin_mult = 1.0 + safety_margin
    max_pipelines = request_cpus
    best_n = 1
    best_per_step = {}

    for n_pipe in range(max_pipelines, 0, -1):
        threads_cap = request_cpus // n_pipe
        if threads_cap < 1:
            continue

        per_step = {}
        max_proj_rss = 0
        for si in sorted(step_ideals):
            tuned = threads_cap if uniform else min(step_ideals[si]["ideal"], threads_cap)
            tuned = max(tuned, 1)
            measured = step_rss[si]
            thread_reduction = original_nthreads - tuned
            proj_rss = measured - thread_reduction * PER_THREAD_OVERHEAD_MB
            proj_rss = max(proj_rss, 500)
            proj_rss = proj_rss * margin_mult
            max_proj_rss = max(max_proj_rss, proj_rss)
            per_step[si] = {
                "tuned_nthreads": tuned,
                "n_parallel": 1,
                "cpu_eff": step_ideals[si]["cpu_eff"],
                "effective_cores": step_ideals[si]["eff_cores"],
                "projected_rss_mb": proj_rss,
                "overcommit_applied": False,
            }

        total_mem = n_pipe * max_proj_rss
        if total_mem <= request_memory_mb:
            best_n = n_pipe
            best_per_step = per_step
            break

    return {
        "original_nthreads": original_nthreads,
        "n_pipelines": best_n,
        "per_step": best_per_step,
    }


# ── Patch manifests ──────────────────────────────────────────


def patch_wu_manifests(
    group_dir: Path, per_step: dict, n_pipelines: int = 1,
    max_memory_mb: int = 0, split_tmpfs: bool = False,
) -> dict:
    """Create manifest_tuned.json with per-step nThreads and add to proc submit files.

    Reads manifest.json from the group dir (extracted at planning time),
    modifies the per-step 'multicore' values, writes manifest_tuned.json,
    and adds it to transfer_input_files in each proc_*.sub.

    Returns number of proc submit files patched.
    """
    manifest_path = group_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {group_dir}")

    manifest = json.loads(manifest_path.read_text())

    if n_pipelines > 1:
        manifest["n_pipelines"] = n_pipelines
    if split_tmpfs:
        manifest["split_tmpfs"] = True

    for si_str, tuning in per_step.items():
        si = int(si_str)
        if si < len(manifest.get("steps", [])):
            manifest["steps"][si]["multicore"] = tuning["tuned_nthreads"]
            n_par = tuning.get("n_parallel", 1)
            manifest["steps"][si]["n_parallel"] = n_par

    tuned_path = group_dir / "manifest_tuned.json"
    tuned_path.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote %s", tuned_path)

    # Determine if parallel splitting is in use
    max_n_par = max(
        (t.get("n_parallel", 1) for t in per_step.values()), default=1
    )

    step0_data = per_step.get("0") or per_step.get(0) or {}
    ideal_memory_mb = step0_data.get("ideal_memory_mb", 0)
    actual_memory_mb = max_memory_mb if max_memory_mb > 0 else 0

    patched = 0
    for sub_file in sorted(group_dir.glob("proc_*.sub")):
        content = sub_file.read_text()
        tif_match = re.search(
            r"^(transfer_input_files\s*=\s*)(.+)$", content, re.MULTILINE
        )
        if tif_match:
            prefix = tif_match.group(1)
            files = tif_match.group(2).rstrip()
            new_line = f"{prefix}{files}, {tuned_path}"
            content = content[:tif_match.start()] + new_line + content[tif_match.end():]
        if max_n_par > 1 and max_memory_mb > 0:
            mem_match = re.search(
                r"^(request_memory\s*=\s*)(\d+)", content, re.MULTILINE
            )
            if mem_match:
                old_mem = int(mem_match.group(2))
                new_mem = max_memory_mb
                if new_mem > old_mem:
                    content = (content[:mem_match.start()]
                               + f"{mem_match.group(1)}{new_mem}"
                               + content[mem_match.end():])
        sub_file.write_text(content)
        patched += 1

    return {
        "patched": patched,
        "ideal_memory_mb": ideal_memory_mb,
        "actual_memory_mb": actual_memory_mb,
    }


# ── Inter-round optimization orchestrator ────────────────────


def compute_round_optimization(
    submit_dir: str,
    completed_wus: list[str],
    original_nthreads: int,
    request_cpus: int,
    default_memory_per_core: int,
    max_memory_per_core: int,
    safety_margin: float = 0.20,
    events_per_job: int = 0,
    jobs_per_wu: int = 8,
    min_request_cpus: int = 4,
    # Deprecated — ignored, kept for call-site compatibility
    adaptive_mode: str = "",
    historical_peak_rss_mb: float = 0.0,
) -> dict:
    """Orchestrate inter-round adaptive optimization.

    Composes independent optimization dimensions rather than dispatching to
    one exclusive mode:

      1. Collect and merge metrics from completed work units
      2. Size memory from unified source hierarchy (cgroup → FJR RSS → default)
      3. Decide job splitting (effective cores → request_cpus, clamped to min_request_cpus)
      4. Compute internal parallelism (step 0 instances at the new request_cpus)
      5. Return unified output dict with all dimensions

    Args:
        submit_dir: Path to the DAG's submit directory
        completed_wus: List of completed work unit names (e.g. ["mg_000000"])
        original_nthreads: Original nThreads from request spec
        request_cpus: CPUs allocated per job
        default_memory_per_core: Floor memory per core (MB)
        max_memory_per_core: Ceiling memory per core (MB)
        safety_margin: Fractional margin on measured memory
        events_per_job: Current events per job (for job splitting)
        jobs_per_wu: Jobs per work unit (for job splitting)
        min_request_cpus: Floor for request_cpus to avoid pool fragmentation
        historical_peak_rss_mb: Max peak RSS observed across all previous rounds.
            Memory sizing uses max(current, historical) to prevent regression.

    Returns:
        Dict with tuning results including:
        - tuned_nthreads: Optimal thread count (original unless job split)
        - tuned_memory_mb: Optimal memory (MB)
        - tuned_request_cpus: New request_cpus if job split applied
        - per_step: Per-step tuning details
        - metrics_summary: Summary of observed metrics
        - job_multiplier: Job multiplication factor (1 if no split)
        - memory_source: Where memory estimate came from
    """
    submit_path = Path(submit_dir)

    # ── 1. Collect metrics from all completed work units ──
    round_metrics = []
    for wu_name in completed_wus:
        wu_dir = submit_path / wu_name
        if not wu_dir.is_dir():
            logger.warning("WU dir not found: %s", wu_dir)
            continue
        try:
            metrics = analyze_wu_metrics(wu_dir)
            round_metrics.append(metrics)
            logger.info(
                "WU %s: %d jobs, cpu_eff=%.1f%%, peak_rss=%.0f MB",
                wu_name, metrics["num_jobs"],
                metrics["weighted_cpu_eff"] * 100,
                metrics["peak_rss_mb"],
            )
        except ValueError as exc:
            logger.warning("No metrics for WU %s: %s", wu_name, exc)

    if not round_metrics:
        logger.warning("No WU metrics found — using defaults")
        return {
            "tuned_nthreads": original_nthreads,
            "tuned_memory_mb": default_memory_per_core * request_cpus,
            "tuned_request_cpus": request_cpus,
            "job_multiplier": 1,
            "per_step": {},
            "metrics_summary": None,
            "memory_source": "default",
        }

    # ── Merge metrics across work units ──
    merged = merge_round_metrics(round_metrics, original_nthreads)
    cgroup = merged.get("cgroup")

    default_memory_mb = default_memory_per_core * request_cpus
    max_memory_mb = max_memory_per_core * request_cpus

    result = {
        "metrics_summary": {
            "weighted_cpu_eff": merged["weighted_cpu_eff"],
            "effective_cores": merged["effective_cores"],
            "peak_rss_mb": merged["peak_rss_mb"],
            "num_jobs": merged["num_jobs"],
            "nthreads": merged["nthreads"],
        },
    }

    # ── 2. Size memory from unified source hierarchy ──
    # Spec: "Peak value is tracked for all rounds, not only the latest."
    # Use max(current round peak, historical peak) to prevent regression.
    MIN_MEMORY_MB = 4000
    peak_rss = merged["peak_rss_mb"]
    if historical_peak_rss_mb > 0 and historical_peak_rss_mb > peak_rss:
        logger.info(
            "Historical peak RSS %.0f MB > current %.0f MB — using historical",
            historical_peak_rss_mb, peak_rss,
        )
        peak_rss = historical_peak_rss_mb

    if cgroup and cgroup.get("peak_nonreclaim_mb", 0) > 0:
        cgroup_peak = cgroup["peak_nonreclaim_mb"]
        # Also enforce historical floor on cgroup measurement
        if historical_peak_rss_mb > cgroup_peak:
            cgroup_peak = historical_peak_rss_mb
        measured_memory_mb = round(cgroup_peak)
        measured_mem = int(cgroup_peak * (1.0 + safety_margin))
        memory_source = "cgroup"
    elif peak_rss > 0:
        measured_memory_mb = round(peak_rss)
        measured_mem = int(peak_rss * (1.0 + safety_margin))
        memory_source = "fjr_rss"
    else:
        measured_memory_mb = 0
        measured_mem = default_memory_mb
        memory_source = "default"

    tuned_memory_mb = max(MIN_MEMORY_MB, min(measured_mem, max_memory_mb))

    # ── 3. Decide job splitting ──
    tuned_request_cpus = request_cpus
    job_multiplier = 1
    tuned_events_per_job = events_per_job

    if events_per_job > 0:
        job_split = compute_job_split(
            merged, original_nthreads,
            request_cpus=request_cpus,
            memory_per_core_mb=default_memory_per_core,
            max_memory_per_core_mb=max_memory_per_core,
            events_per_job=events_per_job,
            num_jobs_wu=jobs_per_wu,
            safety_margin=safety_margin,
            cgroup=cgroup,
            min_request_cpus=min_request_cpus,
        )
        if job_split["job_multiplier"] > 1:
            tuned_request_cpus = job_split["new_request_cpus"]
            job_multiplier = job_split["job_multiplier"]
            tuned_events_per_job = job_split["new_events_per_job"]
            # Job split provides its own memory sizing at tuned_cpus
            tuned_memory_mb = job_split["new_request_memory_mb"]
            memory_source = job_split.get("memory_source", memory_source)

    # ── 4. Compute internal parallelism (per-step nThreads) ──
    tuning = compute_per_step_nthreads(
        merged, original_nthreads,
        request_cpus=tuned_request_cpus,
        default_memory_mb=default_memory_per_core * tuned_request_cpus,
        max_memory_mb=max_memory_per_core * tuned_request_cpus,
        safety_margin=safety_margin,
        cgroup=cgroup,
    )

    # ── 5. Build unified result ──
    result["tuned_nthreads"] = tuned_request_cpus
    result["tuned_memory_mb"] = tuned_memory_mb
    result["tuned_request_cpus"] = tuned_request_cpus
    result["job_multiplier"] = job_multiplier
    result["memory_source"] = memory_source
    result["measured_memory_mb"] = measured_memory_mb
    result["per_step"] = {str(k): v for k, v in tuning["per_step"].items()}

    if job_multiplier > 1:
        result["tuned_events_per_job"] = tuned_events_per_job

    logger.info(
        "Adaptive optimization: cpus=%d→%d, memory=%d MB [%s], "
        "job_multiplier=%d, cpu_eff=%.1f%%",
        request_cpus, tuned_request_cpus,
        tuned_memory_mb, memory_source,
        job_multiplier,
        merged["weighted_cpu_eff"] * 100,
    )

    return result
