"""Adaptive execution — CLI wrapper for WMS2 adaptive algorithms.

Thin wrapper around wms2.core.adaptive — all algorithm functions live
in the core module. This file provides the CLI entry point for intra-DAG
replan (called by DAGMan replan nodes between work units).

CLI entry point:
  python -m tests.matrix.adaptive replan --wu0-dir ... --wu1-dir ...
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

# Re-export all algorithm functions from core for backward compatibility
from wms2.core.adaptive import (
    _nearest_power_of_2,
    analyze_wu_metrics,
    compute_all_step_split,
    compute_job_split,
    compute_per_step_nthreads,
    compute_round_optimization,
    load_cgroup_metrics,
    merge_round_metrics,
    patch_wu_manifests,
    rewrite_wu_for_job_split,
)

logger = logging.getLogger(__name__)


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

    # Probe node analysis removed — round 0 is the probe, cgroup/FJR data
    # from normal jobs is sufficient. --probe-node arg kept for backward compat.
    probe_rss_mb = 0.0
    probe_job_peak_total = 0.0
    probe_num_instances = 0
    probe_data = None
    if opts.probe_node:
        print(f"NOTE: --probe-node is deprecated (round 0 is the probe)")

    # 1. Analyze metrics from all prior rounds, merge with normalization
    print(f"\n--- Analyzing prior round metrics ({len(prior_dirs)} round(s)) ---")
    round_metrics = []
    for rd_idx, rd_dir in enumerate(prior_dirs):
        excl = None
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
