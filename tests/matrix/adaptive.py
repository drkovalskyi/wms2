"""Adaptive execution — analyze completed work unit metrics and tune per-step nThreads.

Demonstrates the adaptive execution model from spec Section 5: a replan node
between work units reads WU0 metrics, computes per-step optimal nThreads for
cmsRun, and patches WU1's manifest with the tuned values.

The key insight: CMSSW step efficiency varies widely.  A GEN step may use 55%
of its 8 allocated threads (4.4 effective cores) while a NANO step uses only
15% (1.2 effective cores).  Rather than changing the scheduler-visible resource
footprint (which causes fragmentation), we optimize inside the sandbox by
telling each cmsRun step to use fewer threads matching its actual parallelism.

Three main functions:
  analyze_wu_metrics       — read proc_*_metrics.json from a completed merge group
  compute_per_step_nthreads — derive nThreads per step from observed CPU efficiency
  patch_wu_manifests       — write manifest_tuned.json and add to proc submit files

CLI entry point:
  python -m tests.matrix.adaptive replan --wu0-dir ... --wu1-dir ...
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

    # Collect normalized cpu_eff from ALL rounds
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
    # Propagate cgroup data from latest round (most recent measurement)
    if "cgroup" in latest:
        result["cgroup"] = latest["cgroup"]
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


def analyze_probe_metrics(
    group_dir: Path, probe_node_name: str,
) -> dict | None:
    """Read the probe node's metrics and extract per-instance RSS.

    The probe runs step 0 as 2×(N/2)T parallel instances.  Its metrics file
    contains TWO entries with step_index=0, each from one instance, with its
    own peak_rss_mb.

    Returns:
        {
            "per_instance_rss_mb": [rss1, rss2],
            "max_instance_rss_mb": float,
            "num_instances": int,
        }
    or None if probe metrics aren't found.
    """
    # Node name is "proc_000001" but metrics file is "proc_1_metrics.json"
    # (the wrapper uses the integer --node-index, not the 6-digit padded name)
    idx_match = re.search(r"proc_0*(\d+)$", probe_node_name)
    node_idx = idx_match.group(1) if idx_match else probe_node_name.replace("proc_", "")
    probe_file = group_dir / f"proc_{node_idx}_metrics.json"

    # Also check unmerged storage
    if not probe_file.exists():
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
                        candidate = udir / f"proc_{node_idx}_metrics.json"
                        if candidate.exists():
                            probe_file = candidate
                            break
            except Exception:
                pass

    if not probe_file.exists():
        logger.warning("Probe metrics file not found: %s", probe_file)
        return None

    try:
        data = json.loads(probe_file.read_text())
    except Exception as exc:
        logger.warning("Failed to read probe metrics %s: %s", probe_file, exc)
        return None

    # Extract step 0 entries (one per parallel instance)
    step0_rss = []
    for entry in data:
        if entry.get("step_index") == 0 and entry.get("peak_rss_mb"):
            step0_rss.append(entry["peak_rss_mb"])

    if not step0_rss:
        logger.warning("No step 0 RSS data in probe metrics %s", probe_file)
        return None

    # Read peak MemoryUsage from HTCondor job log.
    # The event 006 format is:
    #   006 (...) Image size of job updated: <ImageSizeKb>
    #       <MemoryUsage_MB>  -  MemoryUsage of job (MB)
    #       <ResidentSetSize_KB>  -  ResidentSetSize of job (KB)
    # ImageSize is virtual memory (irrelevant).  MemoryUsage is
    # ceil(RSS/1024) at each sample — the best per-sample metric we
    # have from the log.  Note: condor_history MemoryUsage (cgroup
    # watermark) can be higher due to transient peaks between samples.
    job_peak_mb = 0.0
    log_file = group_dir / f"{probe_node_name}.log"
    if log_file.exists():
        try:
            content = log_file.read_text()
            for m in re.finditer(
                r"(\d+)\s+-\s+MemoryUsage of job \(MB\)", content
            ):
                mb = int(m.group(1))
                job_peak_mb = max(job_peak_mb, mb)
        except Exception:
            pass

    result = {
        "per_instance_rss_mb": step0_rss,
        "max_instance_rss_mb": max(step0_rss),
        "num_instances": len(step0_rss),
    }
    if job_peak_mb > 0:
        result["job_peak_mb"] = job_peak_mb
        # Per-instance peak memory = total / num_instances
        result["per_instance_peak_mb"] = job_peak_mb / len(step0_rss)
    return result


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

    The scheduler-visible resource footprint (request_cpus, num_jobs,
    events_per_job) stays unchanged.  Only step 0's nThreads is reduced —
    freed cores are filled by running n_parallel cmsRun instances.

    CPU overcommit (overcommit_max > 1.0): give each step MORE threads than
    its proportional core share, filling I/O bubbles with extra runnable threads.
    This is memory-safe — extra threads are only added if the projected RSS
    fits within max_memory_mb.

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
            #
            # Memory model: SANDBOX_OVERHEAD + n_par × marginal_instance_mem
            # - SANDBOX_OVERHEAD: CMSSW project area, shared libs, ROOT
            #   (~3 GB constant, loaded once regardless of instances)
            # - marginal_instance_mem: memory cost of adding one more instance
            #
            # Three data sources, best to worst:
            # 1. probe job peak: HTCondor-reported peak memory from probe,
            #    subtract shared overhead to get marginal per-instance cost.
            #    This avoids double-counting — dividing total by n_instances
            #    would amortize the shared overhead into each instance, then
            #    adding it back inflates the estimate.
            # 2. probe RSS: FJR peak_rss_mb from probe (RSS only, add TMPFS estimate)
            # 3. theoretical: R1 avg_rss at original nThreads (add TMPFS estimate)
            #
            # All sources apply safety_margin to account for memory leak
            # accumulation, event-to-event variation, and page cache pressure.
            SANDBOX_OVERHEAD_MB = 3000
            if probe_job_peak_total_mb > 0 and probe_num_instances > 0:
                # Marginal cost = (total - shared overhead) / n_probe_instances
                marginal = (probe_job_peak_total_mb - SANDBOX_OVERHEAD_MB) / probe_num_instances
                marginal = max(marginal, 500)  # floor: at least 500 MB/instance
                instance_mem = marginal * margin_mult
                memory_source = "probe_peak"
            elif cgroup is not None and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0:
                # Measured cgroup data: peak non-reclaimable per instance
                instance_mem = cgroup["tmpfs_peak_nonreclaim_mb"] * margin_mult
                instance_mem = max(instance_mem, 500)
                memory_source = "cgroup_measured"
            elif probe_rss_mb > 0:
                instance_mem = probe_rss_mb * margin_mult + TMPFS_PER_INSTANCE_MB
                memory_source = "probe"
            else:
                instance_mem = avg_rss * margin_mult + TMPFS_PER_INSTANCE_MB
                memory_source = "theoretical"

            # Memory-aware reduction: each instance needs instance_mem.
            # Reduce n_parallel until total fits within max_memory_mb.
            effective_max = max_memory_mb if max_memory_mb > 0 else 0
            if effective_max > 0 and n_par > 1 and instance_mem > 0:
                # Prefer n_par that evenly divides cpus (no wasted cores).
                # Try even-division candidates first, then any n_par as fallback.
                candidates = sorted(
                    [p for p in range(n_par, 1, -1) if request_cpus % p == 0],
                    reverse=True,
                )
                # Fallback: any n_par if no even-division candidate fits
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

            # If splitting isn't possible, keep original nThreads
            if n_par <= 1:
                tuned = original_nthreads
                n_par = 1

            # Compute ideal memory for reporting (what we'd want without cap)
            if ideal_n_par > 1 and instance_mem > 0:
                ideal_memory = SANDBOX_OVERHEAD_MB + ideal_n_par * instance_mem
            else:
                ideal_memory = max_memory_mb

            # Step 0 overcommit: add threads per instance (not more instances)
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
                        # Back off to max safe value
                        avail = max_memory_mb - SANDBOX_OVERHEAD_MB
                        safe_per_inst = avail / n_par if avail > 0 else 0
                        safe_extra = max(0, int((safe_per_inst - instance_mem) / PER_THREAD_OVERHEAD_MB))
                        if safe_extra > 0:
                            tuned_safe = tuned + safe_extra
                            projected_rss_mb = instance_mem + safe_extra * PER_THREAD_OVERHEAD_MB
                            tuned = tuned_safe
                            overcommit_applied = True

        elif (si > 0 or not split) and overcommit_max > 1.0 and eff_vals and avg_rss > 0:
            # Steps 1+ overcommit: add threads to sequential step
            # Only for moderate efficiency (50-90%) — low eff steps won't
            # benefit, high eff steps are already efficient
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
                        # Back off to max safe value
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
) -> dict:
    """Compute adaptive job split: more jobs with fewer cores per job.

    Instead of splitting step 0 into parallel instances within one job,
    split the jobs themselves — run N× more jobs with N× fewer cores.
    Same total core allocation, but each job finishes independently
    (no tail effect, resources released sooner).

    Memory follows spec Section 5.5: clamp(measured × (1 + safety_margin),
    default_per_core × tuned_cores, max_per_core × tuned_cores).

    The cgroup enforces against total memory (RSS + page cache + tmpfs +
    kernel overhead), not just process RSS.  When step 0 uses tmpfs for
    gridpack extraction, TMPFS_PER_INSTANCE_MB of non-reclaimable content
    is charged to the cgroup, making step 0's total cgroup usage potentially
    comparable to the highest-RSS step.  Under tight limits, memory pressure
    amplifies overhead further.  CGROUP_HEADROOM_MB provides minimum
    absolute headroom to prevent this feedback loop.

    Algorithm (based on Round 1 step 0 metrics):
    1. eff_cores = cpu_eff_step0 × original_nthreads
    2. tuned_threads = nearest_power_of_2(eff_cores), capped to [2, original]
    3. job_multiplier = original_nthreads // tuned_threads
    4. new_events_per_job = events_per_job // job_multiplier
    5. new_num_jobs = num_jobs_wu × job_multiplier
    6. new_request_memory = clamp(peak_cgroup_est × (1 + margin),
                                  tuned × memory_per_core_mb,
                                  tuned × max_memory_per_core_mb)
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
    tuned = max(tuned, min_threads)
    tuned = min(tuned, original_nthreads)

    job_multiplier = original_nthreads // tuned
    if job_multiplier < 1:
        job_multiplier = 1
    # Cap step-down when tmpfs will be enabled but we lack actual tmpfs
    # cgroup data.  Two cases:
    # (a) Have gridpack size from xz -l: estimate tmpfs memory as
    #     peak_anon + gridpack_disk_mb, then find minimum threads where
    #     tuned × max_memory_per_core_mb >= estimated_memory.
    # (b) No data at all: blind ×2 cap (conservative fallback).
    has_tmpfs_data = (cgroup is not None
                      and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0)
    has_gridpack_size = (cgroup is not None
                         and cgroup.get("gridpack_disk_mb", 0) > 0)
    if split_tmpfs and not has_tmpfs_data:
        if has_gridpack_size:
            # Memory-aware cap: find minimum threads where memory fits
            est_mem = int((cgroup["peak_anon_mb"] + cgroup["gridpack_disk_mb"])
                         * (1.0 + safety_margin))
            while tuned < original_nthreads:
                if tuned * max_memory_per_core_mb >= est_mem:
                    break
                tuned *= 2
            tuned = max(tuned, min_threads)
            tuned = min(tuned, original_nthreads)
            job_multiplier = original_nthreads // tuned
        else:
            # No data at all — blind ×2 step-down cap
            prev_nt = last_round_nthreads if last_round_nthreads > 0 else original_nthreads
            min_tuned = max(prev_nt // 2, min_threads)
            if tuned < min_tuned:
                tuned = min_tuned
                job_multiplier = original_nthreads // tuned

    new_events_per_job = events_per_job // job_multiplier
    if new_events_per_job < 1:
        new_events_per_job = 1
        job_multiplier = events_per_job

    new_num_jobs = num_jobs_wu * job_multiplier
    new_request_cpus = tuned

    # Memory sizing (spec Section 5.5):
    # R2 jobs run 1 instance at tuned threads.  Three data sources, best
    # to worst:
    # 1. Probe job peak: HTCondor-reported peak from probe (ran n_instances
    #    at fewer threads).  Marginal model: memory = SANDBOX_OVERHEAD +
    #    1 × marginal, where marginal = (total_peak - overhead) / n_instances.
    # 2. Probe RSS: FJR peak_rss_mb from probe × margin + tmpfs estimate.
    # 3. Prior RSS: peak RSS from prior rounds × margin (fallback).
    # All clamped to [tuned × memory_per_core, tuned × max_memory_per_core].
    #
    # IMPORTANT: FJR PeakValueRss only measures the main cmsRun process
    # (/proc/PID/stat).  For GEN workflows with ExternalLHEProducer, the
    # gridpack subprocess memory is invisible to FJR.  When cgroup monitor
    # data is available (proc_*_cgroup.json), use peak non-reclaimable
    # memory (anon + shmem from memory.stat) which captures ALL processes.
    # Fall back to FJR + hardcoded estimates when cgroup data is absent.
    SANDBOX_OVERHEAD_MB = 3000
    TMPFS_PER_INSTANCE_MB = 2000   # fallback when no cgroup data
    CGROUP_HEADROOM_MB = 1000      # fallback when no cgroup data
    margin_mult = 1.0 + safety_margin

    peak_rss_mb = 0
    for si in metrics["steps"]:
        step = metrics["steps"][si]
        step_rss = step["peak_rss_mb"]
        if step_rss:
            avg = sum(step_rss) / len(step_rss)
            peak_rss_mb = max(peak_rss_mb, avg)

    if probe_job_peak_total_mb > 0 and probe_num_instances > 0:
        # Marginal cost per instance from probe, then project for 1 instance
        marginal = (probe_job_peak_total_mb - SANDBOX_OVERHEAD_MB) / probe_num_instances
        marginal = max(marginal, 500)
        memory_from_measured = int((SANDBOX_OVERHEAD_MB + marginal) * margin_mult)
        memory_source = "probe_peak"
    elif cgroup is not None and cgroup.get("peak_nonreclaim_mb", 0) > 0:
        # Measured cgroup data: peak non-reclaimable (anon + shmem).
        # This captures subprocess memory that FJR PeakValueRss misses.
        if split_tmpfs and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) > 0:
            # Have tmpfs measurement: use max of tmpfs-phase peak
            # (step 0: anon+shmem) vs non-tmpfs peak (other steps: anon only)
            binding = max(
                cgroup["tmpfs_peak_nonreclaim_mb"],
                cgroup.get("no_tmpfs_peak_anon_mb", 0),
            )
            memory_from_measured = int(binding * margin_mult)
            memory_source = "cgroup_measured"
        elif split_tmpfs and cgroup.get("tmpfs_peak_nonreclaim_mb", 0) == 0:
            # Prior round ran WITHOUT tmpfs but next round WILL use tmpfs.
            gridpack_mb = cgroup.get("gridpack_disk_mb", 0)
            if gridpack_mb > 0:
                # Have gridpack uncompressed size from xz -l: estimate tmpfs
                # impact as peak_anon + gridpack (gridpack extraction goes to
                # /dev/shm as shmem, process RSS stays as anon).
                binding = cgroup["peak_anon_mb"] + gridpack_mb
                memory_from_measured = int(binding * margin_mult)
                memory_source = "cgroup_gridpack_estimated"
            else:
                # No gridpack size, no tmpfs data — cannot safely estimate.
                # Jump to max memory allowed so the job survives.
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
        # Fallback: FJR RSS + hardcoded estimates (no cgroup data available).
        effective_peak = peak_rss_mb
        if split_tmpfs:
            step0 = metrics["steps"].get(0)
            if step0 and step0["peak_rss_mb"]:
                s0_avg = sum(step0["peak_rss_mb"]) / len(step0["peak_rss_mb"])
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

    # Build per-step info for reporting
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

    DAGMan reads SUBDAG EXTERNAL files when the node is ready to execute,
    not at outer DAG submission. Since replan runs AFTER WU0 and BEFORE WU1,
    DAGMan will see the rewritten group.dag.
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
        # Parse arguments line for --first-event, --last-event, --events-per-job
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
        # Fallback: compute from events_per_job and num_jobs
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
    print(f"Wrote {tuned_path}")

    # Extract common fields from template
    # Find executable, transfer_input_files, environment, etc.
    exe_match = re.search(r"^executable\s*=\s*(.+)$", template_content, re.MULTILINE)
    executable = exe_match.group(1).strip() if exe_match else "wms2_proc.sh"

    tif_match = re.search(r"^transfer_input_files\s*=\s*(.+)$", template_content, re.MULTILINE)
    transfer_files = tif_match.group(1).strip() if tif_match else ""
    # Add manifest_tuned.json to transfer files
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

    # Extract --sandbox and --output-info from template args
    args_match = re.search(r"^arguments\s*=\s*(.+)$", template_content, re.MULTILINE)
    template_args = args_match.group(1).strip() if args_match else ""
    sandbox_match = re.search(r"--sandbox\s+(\S+)", template_args)
    sandbox_ref = sandbox_match.group(1) if sandbox_match else "sandbox.tar.gz"
    oi_match = re.search(r"--output-info\s+(\S+)", template_args)
    output_info_ref = oi_match.group(1) if oi_match else ""

    # Extract SCRIPT PRE/POST patterns from group.dag
    dag_path = group_dir / "group.dag"
    dag_content = dag_path.read_text()

    # Find PRE and POST script patterns from existing proc nodes
    pre_script_match = re.search(
        r"^SCRIPT PRE proc_\d+ (.+)$", dag_content, re.MULTILINE
    )
    post_script_match = re.search(
        r"^SCRIPT POST proc_\d+ (.+\s+)\S+ \$RETURN$", dag_content, re.MULTILINE
    )
    # Extract the script path portion
    pre_script_base = ""
    if pre_script_match:
        pre_parts = pre_script_match.group(1).strip()
        # Pattern: /path/to/pin_site.sh proc_NNNNNN.sub elected_site
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
    print(f"Deleted {len(old_sub_files)} old proc submit files")

    # 5. Generate new proc submit files
    for i in range(new_num_jobs):
        node_index = i
        node_name = f"proc_{node_index:06d}"

        # Compute event range for this job
        fe = wu_first_event + i * new_events_per_job
        le = fe + new_events_per_job - 1
        if le > wu_last_event:
            le = wu_last_event

        # Build arguments — use synthetic:// so lfn_to_xrootd() returns empty
        # (GEN workflows use EmptySource which rejects fileNames)
        input_lfn = f"synthetic://gen/events_{fe}_{le}"
        proc_args = f"--sandbox {sandbox_ref} --input {input_lfn}"
        proc_args += f" --node-index {node_index}"
        if output_info_ref:
            proc_args += f" --output-info {output_info_ref}"
        proc_args += f" --first-event {fe}"
        proc_args += f" --last-event {le}"
        proc_args += f" --events-per-job {new_events_per_job}"
        proc_args += f" --ncpus {new_request_cpus}"

        # Write submit file
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

    print(f"Generated {new_num_jobs} new proc submit files "
          f"({new_request_cpus} cpus, {new_events_per_job} ev/job, "
          f"{new_request_memory_mb} MB)")

    # 6. Rewrite group.dag
    new_dag_lines = [f"# Merge group (job split: {job_multiplier}x)", ""]

    # Landing node (unchanged)
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

    # New proc nodes
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

    # Merge node (unchanged)
    for pattern in [r"^JOB merge .+$", r"^SCRIPT PRE merge .+$"]:
        m = re.search(pattern, dag_content, re.MULTILINE)
        if m:
            new_dag_lines.append(m.group(0))
    new_dag_lines.append("")

    # Cleanup node (unchanged)
    for pattern in [r"^JOB cleanup .+$", r"^SCRIPT PRE cleanup .+$"]:
        m = re.search(pattern, dag_content, re.MULTILINE)
        if m:
            new_dag_lines.append(m.group(0))
    new_dag_lines.append("")

    # Retries
    for name in proc_names:
        new_dag_lines.append(f"RETRY {name} 3 UNLESS-EXIT 42")
    new_dag_lines.append("RETRY merge 2 UNLESS-EXIT 42")
    new_dag_lines.append("RETRY cleanup 1")
    new_dag_lines.append("")

    # Abort-DAG-on (catastrophic failure circuit breaker)
    for name in proc_names:
        new_dag_lines.append(f"ABORT-DAG-ON {name} 43 RETURN 1")
    new_dag_lines.append("ABORT-DAG-ON merge 43 RETURN 1")
    new_dag_lines.append("")

    # Dependencies
    proc_str = " ".join(proc_names)
    new_dag_lines.append(f"PARENT landing CHILD {proc_str}")
    new_dag_lines.append(f"PARENT {proc_str} CHILD merge")
    new_dag_lines.append("PARENT merge CHILD cleanup")
    new_dag_lines.append("")

    # Categories
    for name in proc_names:
        new_dag_lines.append(f"CATEGORY {name} Processing")
    new_dag_lines.append("CATEGORY merge Merge")
    new_dag_lines.append("CATEGORY cleanup Cleanup")
    # Preserve MAXJOBS from original
    for m in re.finditer(r"^MAXJOBS .+$", dag_content, re.MULTILINE):
        new_dag_lines.append(m.group(0))

    dag_path.write_text("\n".join(new_dag_lines) + "\n")
    print(f"Rewrote {dag_path} ({new_num_jobs} proc nodes)")

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
    """Derive N-pipeline split where each pipeline runs ALL steps with tuned nThreads.

    Instead of splitting only step 0, this forks N complete StepChain pipelines
    within one sandbox. Each processes events/N events through all steps with
    per-step optimized nThreads.

    Algorithm:
    1. For each step: ideal_threads = nearest_power_of_2(eff_cores), capped to original
    2. Iterate n_pipelines from highest feasible down to 1:
       - threads_cap = request_cpus // n_pipelines
       - Per-step: tuned = min(ideal_threads, threads_cap)
       - Per-step: proj_rss = (measured_rss - (original - tuned) * 250) × (1 + margin)
       - Memory check: n_pipelines * max(proj_rss) <= request_memory_mb
       - Take first (highest) n_pipelines that fits
    3. Return dict with n_pipelines and per-step tuning details.
    """
    PER_THREAD_OVERHEAD_MB = 250

    # Step 1: compute ideal threads per step
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

    # Step 2: iterate n_pipelines from highest feasible down
    margin_mult = 1.0 + safety_margin
    max_pipelines = request_cpus  # theoretical max (1 thread each)
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
            # Project RSS: fewer threads => less memory, then apply safety margin
            measured = step_rss[si]
            thread_reduction = original_nthreads - tuned
            proj_rss = measured - thread_reduction * PER_THREAD_OVERHEAD_MB
            proj_rss = max(proj_rss, 500)  # floor at 500 MB
            proj_rss = proj_rss * margin_mult
            max_proj_rss = max(max_proj_rss, proj_rss)
            per_step[si] = {
                "tuned_nthreads": tuned,
                "n_parallel": 1,  # parallelism is at pipeline level
                "cpu_eff": step_ideals[si]["cpu_eff"],
                "effective_cores": step_ideals[si]["eff_cores"],
                "projected_rss_mb": proj_rss,
                "overcommit_applied": False,
            }

        # Memory check: all pipelines run simultaneously
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

    The proc script applies the tuned manifest after sandbox extraction:
        if [[ -f manifest_tuned.json ]]; then
            cp manifest_tuned.json manifest.json
        fi

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
            old = manifest["steps"][si].get("multicore", "?")
            manifest["steps"][si]["multicore"] = tuning["tuned_nthreads"]
            n_par = tuning.get("n_parallel", 1)
            manifest["steps"][si]["n_parallel"] = n_par
            par_str = f" n_parallel={n_par}" if n_par > 1 else ""
            print(f"  Step {si}: multicore {old} -> {tuning['tuned_nthreads']}{par_str}")

    tuned_path = group_dir / "manifest_tuned.json"
    tuned_path.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote {tuned_path}")

    # Determine if parallel splitting is in use
    max_n_par = max(
        (t.get("n_parallel", 1) for t in per_step.values()), default=1
    )

    # Get ideal memory from algorithm (stored in step 0 per_step data)
    step0_data = per_step.get("0") or per_step.get(0) or {}
    ideal_memory_mb = step0_data.get("ideal_memory_mb", 0)
    actual_memory_mb = max_memory_mb if max_memory_mb > 0 else 0

    # Add manifest_tuned.json to transfer_input_files in proc submit files.
    # When parallel splitting is active, set request_memory = max_memory_mb
    # (the algorithm already validated that parallel instances fit within this).
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
        # Set request_memory to max when parallel splitting needs extra memory
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
                if patched == 0:
                    if ideal_memory_mb > actual_memory_mb:
                        print(f"  request_memory: {old_mem} -> {new_mem} MB"
                              f" (ideal: {ideal_memory_mb} MB, capped at max: {max_memory_mb} MB)")
                    elif new_mem > old_mem:
                        print(f"  request_memory: {old_mem} -> {new_mem} MB")
                    else:
                        print(f"  request_memory: {old_mem} MB (unchanged)")
        sub_file.write_text(content)
        patched += 1

    return {
        "patched": patched,
        "ideal_memory_mb": ideal_memory_mb,
        "actual_memory_mb": actual_memory_mb,
    }


# ── CLI entry point ──────────────────────────────────────────


def _replan_cli(args: list[str]) -> None:
    """Called by the DAGMan replan job: analyze WU0, patch WU1 manifests."""
    import argparse

    parser = argparse.ArgumentParser(description="WMS2 adaptive replan")
    parser.add_argument("--wu0-dir", required=False, default="",
                        help="Completed WU0 group dir (legacy, use --prior-wu-dirs)")
    parser.add_argument("--prior-wu-dirs", default="",
                        help="Comma-separated prior WU dirs (all rounds)")
    parser.add_argument("--wu1-dir", required=True, help="WU1 group dir to patch")
    parser.add_argument("--ncores", type=int, required=True,
                        help="Cores per job")
    parser.add_argument("--mem-per-core", type=int, required=True,
                        help="MB per core for Round 1")
    parser.add_argument("--max-mem-per-core", type=int, required=True,
                        help="Max MB per core for Round 2")
    parser.add_argument("--overcommit-max", type=float, default=1.0,
                        help="Max CPU overcommit ratio (1.0 = disabled)")
    parser.add_argument("--no-split", action="store_true", default=False,
                        help="Disable step 0 parallel splitting")
    parser.add_argument("--split-all-steps", action="store_true", default=False,
                        help="All-step pipeline split (supersedes --no-split)")
    parser.add_argument("--uniform-threads", action="store_true", default=False,
                        help="Use uniform nThreads across all steps (with --split-all-steps)")
    parser.add_argument("--split-tmpfs", action="store_true", default=False,
                        help="Use tmpfs for parallel split instance working directories")
    parser.add_argument("--job-split", action="store_true", default=False,
                        help="Adaptive job split: more jobs with fewer cores")
    parser.add_argument("--events-per-job", type=int, default=0,
                        help="Events per job (for job split)")
    parser.add_argument("--num-jobs", type=int, default=0,
                        help="Number of jobs per work unit (for job split)")
    parser.add_argument("--probe-node", type=str, default="",
                        help="Probe node name (e.g. proc_000001) for R2 memory estimation")
    parser.add_argument("--safety-margin", type=float, default=0.20,
                        help="Safety margin on measured memory (0.20 = 20%%)")
    parser.add_argument("--replan-index", type=int, default=0,
                        help="Replan index (0-based) for decision file naming")
    parser.add_argument("--min-threads", type=int, default=2,
                        help="Minimum threads per job (floor for adaptive tuning)")
    opts = parser.parse_args(args)

    # Resolve prior dirs: --prior-wu-dirs takes precedence, --wu0-dir as fallback
    if opts.prior_wu_dirs:
        prior_dirs = [Path(p.strip()) for p in opts.prior_wu_dirs.split(",") if p.strip()]
    elif opts.wu0_dir:
        prior_dirs = [Path(opts.wu0_dir)]
    else:
        parser.error("Either --prior-wu-dirs or --wu0-dir is required")
    wu0_dir = prior_dirs[0]  # first dir for probe analysis
    wu1_dir = Path(opts.wu1_dir)

    # Derive total memory from per-core inputs
    ncores = opts.ncores
    mem_per_core = opts.mem_per_core
    max_mem_per_core = opts.max_mem_per_core
    safety_margin = opts.safety_margin
    default_memory_mb = mem_per_core * ncores
    max_memory_mb = max_mem_per_core * ncores

    print(f"=== WMS2 Adaptive Replan ===")
    print(f"Prior dirs: {[str(d) for d in prior_dirs]}")
    print(f"WU1 dir: {wu1_dir}")

    # Read original nthreads from WU1's manifest
    manifest_path = wu1_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json not found in {wu1_dir}")
    manifest = json.loads(manifest_path.read_text())
    steps = manifest.get("steps", [])
    if steps:
        original_nthreads = steps[0].get("multicore", 1)
    else:
        original_nthreads = 1
    print(f"Original nThreads: {original_nthreads}")

    # 1a. Analyze probe metrics (if probe node was used — always in first dir)
    probe_data = None
    probe_rss_mb = 0.0
    probe_job_peak_total = 0.0
    probe_num_instances = 0
    exclude_nodes: set[str] | None = None
    if opts.probe_node:
        print(f"\n--- Analyzing probe metrics ({opts.probe_node}) ---")
        probe_data = analyze_probe_metrics(wu0_dir, opts.probe_node)
        if probe_data:
            probe_rss_mb = probe_data["max_instance_rss_mb"]
            probe_num_instances = probe_data["num_instances"]
            print(f"Probe instances: {probe_num_instances}")
            print(f"Per-instance RSS: {probe_data['per_instance_rss_mb']}")
            print(f"Max instance RSS: {probe_rss_mb:.0f} MB")
            if "job_peak_mb" in probe_data:
                probe_job_peak_total = probe_data["job_peak_mb"]
                marginal = (probe_job_peak_total - 3000) / probe_num_instances
                print(f"Job peak: {probe_job_peak_total:.0f} MB total"
                      f" (marginal: {marginal:.0f} MB/instance after 3 GB shared overhead)")
            exclude_nodes = {opts.probe_node}
        else:
            print(f"WARNING: Probe metrics not found, falling back to theoretical")

    # 1b. Analyze metrics from all prior rounds, merge with normalization
    print(f"\n--- Analyzing prior round metrics ({len(prior_dirs)} round(s)) ---")
    round_metrics = []
    for rd_idx, rd_dir in enumerate(prior_dirs):
        # Exclude probe node only from first round (WU0)
        excl = exclude_nodes if rd_idx == 0 else None
        rm = analyze_wu_metrics(rd_dir, exclude_nodes=excl)
        round_metrics.append(rm)
        print(f"  Round {rd_idx + 1}: {rm['num_jobs']} jobs, "
              f"cpu_eff={rm['weighted_cpu_eff']:.1%}, "
              f"nthreads={rm['nthreads']}")

    metrics = merge_round_metrics(round_metrics, original_nthreads)
    print(f"Merged: Peak RSS: {metrics['peak_rss_mb']:.0f} MB")
    print(f"Merged: Weighted CPU efficiency: {metrics['weighted_cpu_eff']:.1%} (normalized)")
    print(f"Merged: Effective cores: {metrics['effective_cores']:.1f} / {original_nthreads}")
    print(f"Rounds analyzed: {len(round_metrics)}")
    cgroup = metrics.get("cgroup")
    if cgroup:
        print(f"Cgroup: peak_nonreclaim={cgroup['peak_nonreclaim_mb']} MB"
              f"  (anon={cgroup['peak_anon_mb']} shmem={cgroup['peak_shmem_mb']})"
              f"  tmpfs_phase={cgroup.get('tmpfs_peak_nonreclaim_mb', 0)}"
              f"  no_tmpfs={cgroup.get('no_tmpfs_peak_anon_mb', 0)}")
        gridpack_mb = cgroup.get("gridpack_disk_mb", 0)
        if gridpack_mb > 0:
            anon_mb = cgroup.get("peak_anon_mb", 0)
            print(f"Gridpack uncompressed: {gridpack_mb} MB"
                  f"  (estimated tmpfs total: {anon_mb} + {gridpack_mb}"
                  f" = {anon_mb + gridpack_mb} MB)")
            # Disable tmpfs if gridpack extraction is too large for RAM
            TMPFS_MAX_GRIDPACK_MB = 4000
            if opts.split_tmpfs and gridpack_mb > TMPFS_MAX_GRIDPACK_MB:
                print(f"  WARNING: gridpack {gridpack_mb} MB exceeds tmpfs"
                      f" threshold ({TMPFS_MAX_GRIDPACK_MB} MB) — disabling tmpfs")
                opts.split_tmpfs = False

    # 2. Compute per-step optimal nThreads
    print(f"\n--- Computing per-step nThreads ---")
    print(f"ncores: {ncores}  mem/core: {mem_per_core} MB  max mem/core: {max_mem_per_core} MB  margin: {safety_margin:.0%}")

    n_pipelines = 1
    job_multiplier = 1

    if opts.job_split:
        print(f"mode: adaptive job split")
        print(f"events_per_job: {opts.events_per_job}  num_jobs: {opts.num_jobs}")
        last_round_nt = round_metrics[-1]["nthreads"] if round_metrics else 0
        split_result = compute_job_split(
            metrics, original_nthreads,
            request_cpus=ncores,
            memory_per_core_mb=mem_per_core,
            max_memory_per_core_mb=max_mem_per_core,
            events_per_job=opts.events_per_job,
            num_jobs_wu=opts.num_jobs,
            safety_margin=safety_margin,
            probe_job_peak_total_mb=probe_job_peak_total,
            probe_num_instances=probe_num_instances,
            probe_rss_mb=probe_rss_mb,
            split_tmpfs=opts.split_tmpfs,
            cgroup=cgroup,
            last_round_nthreads=last_round_nt,
            min_threads=opts.min_threads,
        )
        job_multiplier = split_result["job_multiplier"]
        tuned_nt = split_result["tuned_nthreads"]
        print(f"  step 0 cpu_eff={split_result['per_step'].get(0, {}).get('cpu_eff', 0):.1%}"
              f"  eff_cores={split_result['per_step'].get(0, {}).get('effective_cores', 0):.1f}")
        print(f"  tuned_nthreads: {tuned_nt}")
        print(f"  job_multiplier: {job_multiplier}")
        print(f"  {opts.num_jobs} jobs x {opts.events_per_job} ev"
              f" -> {split_result['new_num_jobs']} jobs x {split_result['new_events_per_job']} ev")
        print(f"  request_cpus: {ncores} -> {split_result['new_request_cpus']}")
        print(f"  request_memory: {split_result['new_request_memory_mb']} MB"
              f"  [{split_result.get('memory_source', 'unknown')}]")

        # Rewrite WU1's DAG
        print(f"\n--- Rewriting WU1 DAG ---")
        rewrite_result = rewrite_wu_for_job_split(
            wu1_dir, split_result,
            max_memory_per_core_mb=max_mem_per_core,
            split_tmpfs=opts.split_tmpfs,
        )

        # Write replan decisions
        decisions = {
            "original_nthreads": original_nthreads,
            "overcommit_max": opts.overcommit_max,
            "safety_margin": safety_margin,
            "n_pipelines": 1,
            "job_multiplier": job_multiplier,
            "tuned_nthreads": tuned_nt,
            "new_num_jobs": split_result["new_num_jobs"],
            "new_events_per_job": split_result["new_events_per_job"],
            "new_request_cpus": split_result["new_request_cpus"],
            "new_request_memory_mb": split_result["new_request_memory_mb"],
            "memory_source": split_result.get("memory_source", "unknown"),
            "memory_per_core_mb": mem_per_core,
            "max_memory_per_core_mb": max_mem_per_core,
            "min_threads": opts.min_threads,
            "split_tmpfs": opts.split_tmpfs,
            "gridpack_disk_mb": cgroup.get("gridpack_disk_mb", 0) if cgroup else 0,
            "rounds_analyzed": len(round_metrics),
            "per_round_nthreads": [rm["nthreads"] for rm in round_metrics],
            "per_step": {
                str(si): t for si, t in split_result["per_step"].items()
            },
        }
        decisions_path = wu1_dir.parent / f"replan_{opts.replan_index}_decisions.json"
        decisions_path.write_text(json.dumps(decisions, indent=2))
        print(f"\nWrote {decisions_path}")
        print(f"=== Replan complete ===")
        return

    elif opts.split_all_steps:
        mode_desc = "all-step pipeline split"
        if opts.uniform_threads:
            mode_desc += " (uniform threads)"
        print(f"mode: {mode_desc}")
        tuning = compute_all_step_split(
            metrics, original_nthreads,
            request_cpus=ncores,
            request_memory_mb=max_memory_mb,
            uniform=opts.uniform_threads,
            safety_margin=safety_margin,
        )
        n_pipelines = tuning.get("n_pipelines", 1)
        print(f"n_pipelines: {n_pipelines}")
        for si in sorted(tuning["per_step"]):
            t = tuning["per_step"][si]
            proj_str = ""
            if t.get("projected_rss_mb") is not None:
                proj_str = f"  proj_rss={t['projected_rss_mb']:.0f}MB"
            print(f"  Step {si}: cpu_eff={t['cpu_eff']:.1%}"
                  f"  eff_cores={t['effective_cores']:.1f}"
                  f"  -> nThreads={t['tuned_nthreads']}  [PIPE]{proj_str}")
    else:
        if opts.overcommit_max > 1.0:
            print(f"overcommit_max: {opts.overcommit_max}")
        if opts.no_split:
            print(f"step 0 splitting: disabled")
        tuning = compute_per_step_nthreads(
            metrics, original_nthreads,
            request_cpus=ncores,
            default_memory_mb=default_memory_mb,
            max_memory_mb=max_memory_mb,
            overcommit_max=opts.overcommit_max,
            split=not opts.no_split,
            probe_rss_mb=probe_rss_mb,
            probe_job_peak_total_mb=probe_job_peak_total,
            probe_num_instances=probe_num_instances,
            safety_margin=safety_margin,
            cgroup=cgroup,
            min_threads=opts.min_threads,
        )
        for si in sorted(tuning["per_step"]):
            t = tuning["per_step"][si]
            par_str = f"  n_parallel={t['n_parallel']}" if t.get("n_parallel", 1) > 1 else ""
            oc_str = "  [OC]" if t.get("overcommit_applied") else ""
            proj_str = ""
            if t.get("projected_rss_mb") is not None:
                proj_str = f"  proj_rss={t['projected_rss_mb']:.0f}MB"
            ideal_str = ""
            if t.get("ideal_n_parallel") and t["ideal_n_parallel"] > t.get("n_parallel", 1):
                ideal_str = f"  (ideal: n_par={t['ideal_n_parallel']}, needs {t['ideal_memory_mb']} MB)"
            mem_src = ""
            if t.get("memory_source"):
                mem_src = f"  [{t['memory_source']}]"
            print(f"  Step {si}: cpu_eff={t['cpu_eff']:.1%}"
                  f"  eff_cores={t['effective_cores']:.1f}"
                  f"  -> nThreads={t['tuned_nthreads']}{par_str}{oc_str}{proj_str}{ideal_str}{mem_src}")

    # 3. Patch WU1 manifests
    print(f"\n--- Patching WU1 manifests ---")
    patch_result = patch_wu_manifests(
        wu1_dir, tuning["per_step"], n_pipelines=n_pipelines,
        max_memory_mb=max_memory_mb,
        split_tmpfs=opts.split_tmpfs,
    )
    print(f"Patched {patch_result['patched']} proc submit files")

    # 4. Write replan decisions for the performance report
    decisions = {
        "original_nthreads": original_nthreads,
        "overcommit_max": opts.overcommit_max,
        "safety_margin": safety_margin,
        "n_pipelines": n_pipelines,
        "memory_per_core_mb": mem_per_core,
        "max_memory_per_core_mb": max_mem_per_core,
        "ideal_memory_mb": patch_result.get("ideal_memory_mb", 0),
        "actual_memory_mb": patch_result.get("actual_memory_mb", 0),
        "rounds_analyzed": len(round_metrics),
        "per_round_nthreads": [rm["nthreads"] for rm in round_metrics],
        "per_step": {
            str(si): t for si, t in tuning["per_step"].items()
        },
    }
    if probe_data:
        decisions["probe_data"] = probe_data
    if opts.probe_node:
        decisions["probe_node"] = opts.probe_node
    decisions_path = wu1_dir.parent / f"replan_{opts.replan_index}_decisions.json"
    decisions_path.write_text(json.dumps(decisions, indent=2))
    print(f"\nWrote {decisions_path}")
    print(f"=== Replan complete ===")


def main():
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m tests.matrix.adaptive replan --wu0-dir ... --wu1-dir ...")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "replan":
        _replan_cli(sys.argv[2:])
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
