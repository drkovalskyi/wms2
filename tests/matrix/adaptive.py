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


def analyze_wu_metrics(group_dir: Path) -> dict:
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
                if nt and nt > nthreads:
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

    return {
        "steps": steps,
        "peak_rss_mb": peak_rss_all,
        "weighted_cpu_eff": weighted_cpu_eff,
        "effective_cores": effective_cores,
        "num_jobs": num_jobs,
        "nthreads": nthreads,
    }


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
    request_memory_mb: int = 0,
    overcommit_max: float = 1.0,
    split: bool = True,
) -> dict:
    """Derive optimal nThreads for step 0 parallel splitting and optional overcommit.

    The scheduler-visible resource footprint (request_cpus, request_memory,
    num_jobs, events_per_job) stays unchanged.  Only step 0's nThreads is
    reduced — freed cores are filled by running n_parallel cmsRun instances.

    CPU overcommit (overcommit_max > 1.0): give each step MORE threads than
    its proportional core share, filling I/O bubbles with extra runnable threads.
    This is memory-safe — extra threads are only added if the projected RSS
    fits within request_memory_mb.

    Memory-capped: each parallel instance needs step0_rss + 512 MB headroom.

    Returns dict with per-step tuning details.
    """
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

        if si == 0 and split and request_cpus > 0 and eff_cores > 0:
            tuned = _nearest_power_of_2(eff_cores)
            tuned = min(tuned, original_nthreads)
            tuned = max(tuned, 1)
            n_par = request_cpus // tuned
            # Memory cap: each instance needs step0_rss + headroom
            if request_memory_mb > 0 and avg_rss > 0:
                mem_cap = request_memory_mb // int(avg_rss + 512)
                n_par = min(n_par, mem_cap)
            n_par = max(n_par, 1)
            # If splitting isn't possible, keep original nThreads
            if n_par <= 1:
                tuned = original_nthreads
                n_par = 1

            # Step 0 overcommit: add threads per instance (not more instances)
            if overcommit_max > 1.0 and n_par > 1 and avg_rss > 0:
                tuned_oc = min(
                    round(tuned * overcommit_max),
                    int(original_nthreads * overcommit_max),
                )
                extra_threads = tuned_oc - tuned
                if extra_threads > 0 and request_memory_mb > 0:
                    proj_rss = avg_rss + extra_threads * PER_THREAD_OVERHEAD_MB
                    total_proj = n_par * proj_rss
                    if total_proj <= request_memory_mb:
                        projected_rss_mb = proj_rss
                        tuned = tuned_oc
                        overcommit_applied = True
                    else:
                        # Back off to max safe value
                        safe_per_inst = request_memory_mb / n_par
                        safe_extra = max(0, int((safe_per_inst - avg_rss) / PER_THREAD_OVERHEAD_MB))
                        if safe_extra > 0:
                            tuned_safe = tuned + safe_extra
                            projected_rss_mb = avg_rss + safe_extra * PER_THREAD_OVERHEAD_MB
                            tuned = tuned_safe
                            overcommit_applied = True

        elif (si > 0 or not split) and overcommit_max > 1.0 and eff_vals and avg_rss > 0:
            # Steps 1+ overcommit: add threads to sequential step
            # Only for moderate efficiency (50-90%) — low eff steps won't
            # benefit, high eff steps are already efficient
            if 0.50 <= mean_eff < 0.90:
                oc_threads = round(original_nthreads * overcommit_max)
                extra_threads = oc_threads - original_nthreads
                if extra_threads > 0 and request_memory_mb > 0:
                    proj_rss = avg_rss + extra_threads * PER_THREAD_OVERHEAD_MB
                    if proj_rss <= request_memory_mb:
                        tuned = oc_threads
                        projected_rss_mb = proj_rss
                        overcommit_applied = True
                    else:
                        # Back off to max safe value
                        safe_extra = max(0, int((request_memory_mb - avg_rss) / PER_THREAD_OVERHEAD_MB))
                        if safe_extra > 0 and original_nthreads + safe_extra > original_nthreads:
                            tuned = original_nthreads + safe_extra
                            projected_rss_mb = avg_rss + safe_extra * PER_THREAD_OVERHEAD_MB
                            overcommit_applied = True

        per_step[si] = {
            "tuned_nthreads": tuned,
            "n_parallel": n_par,
            "cpu_eff": mean_eff,
            "effective_cores": eff_cores,
            "overcommit_applied": overcommit_applied,
            "projected_rss_mb": projected_rss_mb,
        }

    return {
        "original_nthreads": original_nthreads,
        "per_step": per_step,
    }


# ── Patch manifests ──────────────────────────────────────────


def patch_wu_manifests(group_dir: Path, per_step: dict) -> int:
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

    # Add manifest_tuned.json to transfer_input_files in proc submit files
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
        sub_file.write_text(content)
        patched += 1

    return patched


# ── CLI entry point ──────────────────────────────────────────


def _replan_cli(args: list[str]) -> None:
    """Called by the DAGMan replan job: analyze WU0, patch WU1 manifests."""
    import argparse

    parser = argparse.ArgumentParser(description="WMS2 adaptive replan")
    parser.add_argument("--wu0-dir", required=True, help="Completed WU0 group dir")
    parser.add_argument("--wu1-dir", required=True, help="WU1 group dir to patch")
    parser.add_argument("--request-cpus", type=int, default=0,
                        help="Job request_cpus (for n_parallel computation)")
    parser.add_argument("--request-memory", type=int, default=0,
                        help="Job request_memory in MB (for memory-capped n_parallel)")
    parser.add_argument("--overcommit-max", type=float, default=1.0,
                        help="Max CPU overcommit ratio (1.0 = disabled)")
    parser.add_argument("--no-split", action="store_true", default=False,
                        help="Disable step 0 parallel splitting")
    opts = parser.parse_args(args)

    wu0_dir = Path(opts.wu0_dir)
    wu1_dir = Path(opts.wu1_dir)

    print(f"=== WMS2 Adaptive Replan ===")
    print(f"WU0 dir: {wu0_dir}")
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

    # 1. Analyze WU0 metrics
    print(f"\n--- Analyzing WU0 metrics ---")
    metrics = analyze_wu_metrics(wu0_dir)
    print(f"Peak RSS: {metrics['peak_rss_mb']:.0f} MB")
    print(f"Weighted CPU efficiency: {metrics['weighted_cpu_eff']:.1%}")
    print(f"Effective cores: {metrics['effective_cores']:.1f} / {metrics['nthreads']}")
    print(f"Jobs analyzed: {metrics['num_jobs']}")

    # 2. Compute per-step optimal nThreads
    print(f"\n--- Computing per-step nThreads ---")
    if opts.request_cpus > 0:
        print(f"request_cpus: {opts.request_cpus}")
    if opts.request_memory > 0:
        print(f"request_memory: {opts.request_memory} MB")
    if opts.overcommit_max > 1.0:
        print(f"overcommit_max: {opts.overcommit_max}")
    if opts.no_split:
        print(f"step 0 splitting: disabled")
    tuning = compute_per_step_nthreads(
        metrics, original_nthreads,
        request_cpus=opts.request_cpus,
        request_memory_mb=opts.request_memory,
        overcommit_max=opts.overcommit_max,
        split=not opts.no_split,
    )
    for si in sorted(tuning["per_step"]):
        t = tuning["per_step"][si]
        par_str = f"  n_parallel={t['n_parallel']}" if t.get("n_parallel", 1) > 1 else ""
        oc_str = "  [OC]" if t.get("overcommit_applied") else ""
        proj_str = ""
        if t.get("projected_rss_mb") is not None:
            proj_str = f"  proj_rss={t['projected_rss_mb']:.0f}MB"
        print(f"  Step {si}: cpu_eff={t['cpu_eff']:.1%}"
              f"  eff_cores={t['effective_cores']:.1f}"
              f"  -> nThreads={t['tuned_nthreads']}{par_str}{oc_str}{proj_str}")

    # 3. Patch WU1 manifests
    print(f"\n--- Patching WU1 manifests ---")
    patched = patch_wu_manifests(wu1_dir, tuning["per_step"])
    print(f"Patched {patched} proc submit files")

    # 4. Write replan decisions for the performance report
    decisions = {
        "original_nthreads": original_nthreads,
        "overcommit_max": opts.overcommit_max,
        "per_step": {
            str(si): t for si, t in tuning["per_step"].items()
        },
    }
    decisions_path = wu1_dir.parent / "replan_decisions.json"
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
