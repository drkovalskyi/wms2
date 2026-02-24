"""Pass/fail summary table with performance metrics for matrix test results."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import TextIO

from tests.matrix.runner import POLL_INTERVAL, PerfData, WorkflowResult

# ANSI color codes (disabled if not a tty)
_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_RESET = "\033[0m"
_BOLD = "\033[1m"

STATUS_COLORS = {
    "passed": _GREEN,
    "failed": _RED,
    "error": _RED,
    "skipped": _YELLOW,
    "timeout": _YELLOW,
}


def _color(text: str, status: str, use_color: bool) -> str:
    if not use_color:
        return text
    c = STATUS_COLORS.get(status, "")
    return f"{c}{text}{_RESET}" if c else text


def print_summary(
    results: list[WorkflowResult],
    out: TextIO = sys.stdout,
    use_color: bool | None = None,
    out_dir: Path | None = None,
) -> None:
    """Print a pass/fail table with timing."""
    if use_color is None:
        use_color = hasattr(out, "isatty") and out.isatty()

    # Header
    hdr = f"{'ID':>7s}  {'Status':<8s}  {'Time':>7s}  {'Title'}"
    sep = "-" * max(len(hdr) + 20, 70)
    out.write(f"\n{sep}\n")
    if use_color:
        out.write(f"{_BOLD}{hdr}{_RESET}\n")
    else:
        out.write(f"{hdr}\n")
    out.write(f"{sep}\n")

    # Rows
    for r in results:
        time_str = f"{r.elapsed_sec:.1f}s" if r.elapsed_sec > 0 else "-"
        status_str = _color(f"{r.status:<8s}", r.status, use_color)
        line = f"{r.wf_id:>7.1f}  {status_str}  {time_str:>7s}  {r.title}"
        out.write(f"{line}\n")
        if r.reason:
            out.write(f"{'':>7s}  {'':8s}  {'':>7s}  \u2514\u2500 {r.reason}\n")

    out.write(f"{sep}\n")

    # Totals
    passed = sum(1 for r in results if r.status == "passed")
    failed = sum(1 for r in results if r.status in ("failed", "error"))
    skipped = sum(1 for r in results if r.status == "skipped")
    timed_out = sum(1 for r in results if r.status == "timeout")
    total_time = sum(r.elapsed_sec for r in results)

    parts = [f"{passed} passed"]
    if failed:
        parts.append(_color(f"{failed} failed", "failed", use_color))
    if skipped:
        parts.append(_color(f"{skipped} skipped", "skipped", use_color))
    if timed_out:
        parts.append(_color(f"{timed_out} timeout", "timeout", use_color))
    parts.append(f"{total_time:.1f}s total")

    summary = "  ".join(parts)
    out.write(f"\n{summary}\n\n")

    # Performance details for workflows that have perf data
    perf_results = [r for r in results if r.perf and r.perf.steps]
    if perf_results:
        for r in perf_results:
            _print_perf_detail(r, out, use_color, out_dir=out_dir)


# ── Throughput computation ───────────────────────────────────


def _compute_round_metrics(steps, n_jobs, request_cpus, n_parallel=1):
    """Compute throughput metrics based on actual core-time committed.

    Core-seconds = sum of (per-job wall time × request_cpus) across all jobs.
    Uses StepPerf.n_jobs × StepPerf.wall_sec_mean per step to get the sum
    directly, which is more accurate than n_jobs × mean when job counts
    vary per step (e.g. some jobs failed a later step).

    When n_parallel > 1 (step 0 parallel split), StepPerf.n_jobs for step 0
    counts individual instances (e.g. 16 = 4 jobs × 4 instances).  But the
    actual core commitment is only 4 HTCondor slots × 8 cores.  We divide
    step 0's instance count by n_parallel to get real job count.

    Args:
        steps: list of StepPerf objects for one round
        n_jobs: number of HTCondor jobs (for output metadata)
        request_cpus: cores allocated per job
        n_parallel: parallel instances per job in step 0 (default 1)

    Returns dict with events, n_jobs, request_cpus, core_hours, ev_ch.
    """
    if not steps or request_cpus <= 0:
        return {}
    total_events = max(s.events_total for s in steps) if steps else 0
    # Sum of all per-job wall seconds across all steps.
    # For steps with parallel instances, divide n_jobs by n_parallel
    # to count actual HTCondor slots, not individual instances.
    total_job_wall_sec = 0
    for s in steps:
        if n_parallel > 1 and s.n_jobs > n_jobs:
            # This step has parallel instances — use max wall time per job
            # (instances run concurrently, so wall = max, not sum)
            actual_jobs = s.n_jobs // n_parallel
            total_job_wall_sec += actual_jobs * s.wall_sec_max
        else:
            total_job_wall_sec += s.n_jobs * s.wall_sec_mean
    if total_job_wall_sec <= 0 or total_events <= 0:
        return {}
    total_core_sec = request_cpus * total_job_wall_sec
    core_hours = total_core_sec / 3600
    ev_ch = total_events / core_hours
    return {
        "events": total_events,
        "n_jobs": n_jobs,
        "request_cpus": request_cpus,
        "core_hours": core_hours,
        "ev_ch": ev_ch,
    }


# ── Per-step table ───────────────────────────────────────────


def _print_step_table(steps, nthreads_map, out, show_nthreads=True,
                      n_concurrent=1, n_jobs=0, request_cpus=0):
    """Print a per-step performance table with per-job metrics.

    All metrics are per-job (one HTCondor slot):
      - ev/s:    events_per_job / wall_seconds  (throughput per job)
      - CPU:     effective_cores / request_cpus  (CPU efficiency per job)
      - RSS(MB): peak_rss * n_concurrent         (total memory per job)

    Args:
        steps: list of StepPerf objects
        nthreads_map: dict {step_index: nthreads} or int for uniform
        out: output stream
        show_nthreads: whether to include the nT column
        n_concurrent: concurrent cmsRun processes per job (1 or n_pipelines)
        n_jobs: number of concurrent HTCondor jobs
        request_cpus: allocated cores per job
    """
    if isinstance(nthreads_map, int):
        nt_val = nthreads_map
        nthreads_map = {i: nt_val for i in range(len(steps))}

    if request_cpus <= 0:
        request_cpus = max(nthreads_map.values()) * n_concurrent if nthreads_map else 8

    # Header
    nt_col = "  nT" if show_nthreads else ""
    out.write(f"  {'Step':<8s}{nt_col}"
              f"  {'Wall(s)':>8s}"
              f"  {'ev/s':>7s}"
              f"  {'CPU':>8s}"
              f"  {'RSS(MB)':>8s}"
              f"  {'n':>3s}\n")
    nt_sep = "  \u2500\u2500" if show_nthreads else ""
    out.write(f"  {'\u2500' * 8}{nt_sep}"
              f"  {'\u2500' * 8}"
              f"  {'\u2500' * 7}"
              f"  {'\u2500' * 8}"
              f"  {'\u2500' * 8}"
              f"  {'\u2500' * 3}\n")

    total_wall = 0
    total_events = 0
    weighted_eff_num = 0.0
    weighted_eff_den = 0.0
    peak_job_rss = 0.0

    for i, s in enumerate(steps):
        nt = nthreads_map.get(i, 8)

        # Per-job throughput: events_per_job / wall
        events_per_job = s.events_total / n_jobs if n_jobs > 0 else s.events_total
        job_ev_s = events_per_job / s.wall_sec_max if s.wall_sec_max > 0 else 0

        # Per-job CPU efficiency: effective_cores / request_cpus
        eff_cores = s.cpu_eff_mean * nt * n_concurrent
        job_cpu_pct = eff_cores / request_cpus * 100 if request_cpus > 0 else 0

        # Per-job RSS: peak across cmsRun instances * concurrent processes
        job_rss = s.rss_mb_max * n_concurrent

        nt_str = f"  {nt:>2d}" if show_nthreads else ""
        ev_s_str = f"{job_ev_s:.2f}" if job_ev_s > 0 else "-"
        cpu_str = f"{job_cpu_pct:.0f}%" if job_cpu_pct > 0 else "-"
        rss_str = f"{job_rss:.0f}" if job_rss > 0 else "-"

        step_label = s.step_name
        if ": " in step_label:
            step_label = step_label.split(": ", 1)[1]

        out.write(f"  {step_label:<8s}{nt_str}"
                  f"  {s.wall_sec_max:>8.0f}"
                  f"  {ev_s_str:>7s}"
                  f"  {cpu_str:>8s}"
                  f"  {rss_str:>8s}"
                  f"  {s.n_jobs:>3d}\n")

        total_wall += s.wall_sec_max
        total_events = max(total_events, s.events_total)
        peak_job_rss = max(peak_job_rss, job_rss)
        if job_cpu_pct > 0 and s.wall_sec_max > 0:
            weighted_eff_num += job_cpu_pct * s.wall_sec_max
            weighted_eff_den += s.wall_sec_max

    # Total row
    events_per_job = total_events / n_jobs if n_jobs > 0 else total_events
    total_ev_s = events_per_job / total_wall if total_wall > 0 else 0
    overall_cpu_pct = weighted_eff_num / weighted_eff_den if weighted_eff_den > 0 else 0
    nt_pad = "    " if show_nthreads else ""
    out.write(f"  {'\u2500' * 8}  {nt_pad}{'\u2500' * 8}  {'\u2500' * 7}"
              f"  {'\u2500' * 8}  {'\u2500' * 8}\n")
    cpu_total_str = f"{overall_cpu_pct:.0f}%" if overall_cpu_pct > 0 else ""
    out.write(f"  {'Total':<8s}  {nt_pad}{total_wall:>8.0f}"
              f"  {total_ev_s:>7.2f}"
              f"  {cpu_total_str:>8s}"
              f"  {peak_job_rss:>8.0f}\n")

    # Machine total memory
    if peak_job_rss > 0 and n_jobs > 0:
        machine_gb = peak_job_rss * n_jobs / 1024
        out.write(f"\n  Memory: {peak_job_rss:.0f} MB/job peak  |  "
                  f"{machine_gb:.1f} GB machine total ({n_jobs} jobs)\n")

    return total_events, total_wall, overall_cpu_pct, peak_job_rss


# ── Performance detail ───────────────────────────────────────


def _print_perf_detail(
    result: WorkflowResult,
    out: TextIO,
    use_color: bool,
    out_dir: Path | None = None,
) -> None:
    """Print performance report for a single workflow."""
    perf = result.perf
    if not perf:
        return

    out.write(f"{'\u2500' * 90}\n")
    title = f"Performance: {result.wf_id:.1f} \u2014 {result.title}"
    if use_color:
        out.write(f"{_BOLD}{title}{_RESET}\n")
    else:
        out.write(f"{title}\n")

    adaptive = perf.raw_job_step_data.get("_adaptive")
    if adaptive:
        _print_adaptive_report(result, perf, adaptive, out, use_color)
    else:
        _print_standard_report(result, perf, out, use_color)

    # Output info
    if perf.merged_files > 0:
        size_mb = perf.merged_bytes / (1024 * 1024)
        out.write(f"\n  Output: {perf.merged_files} merged files, {size_mb:.1f} MB")
        if perf.unmerged_cleaned:
            out.write("  (unmerged cleaned)")
        out.write("\n")

    # CPU timeline plot
    if out_dir and perf.cpu_samples and len(perf.cpu_samples) > 10:
        plot_path = out_dir / f"cpu_{result.wf_id:.1f}.png"
        saved = _save_cpu_plot(result, perf, plot_path)
        if saved:
            out.write(f"\n  CPU plot: {saved}\n")

    out.write("\n")


def _print_standard_report(
    result: WorkflowResult,
    perf: PerfData,
    out: TextIO,
    use_color: bool,
) -> None:
    """Print report for non-adaptive workflows."""
    ncpus = perf.cpu_expected_pct / (perf.num_proc_jobs * 100) if perf.num_proc_jobs else 1
    nthreads = int(round(ncpus))

    # Throughput headline
    n_jobs = perf.num_proc_jobs
    m = _compute_round_metrics(perf.steps, n_jobs, nthreads)
    if m:
        out.write(f"\n  Throughput: {m['ev_ch']:.0f} ev/core-hour  |  "
                  f"{m['events']} events, {m['core_hours']:.2f} core-hours "
                  f"({n_jobs} jobs \u00d7 {nthreads}T)\n")

    # Per-step table
    if perf.steps:
        out.write("\n")
        _print_step_table(perf.steps, nthreads, out, n_concurrent=1,
                          n_jobs=n_jobs, request_cpus=nthreads)

    # Time breakdown
    _print_time_breakdown(result, perf, out)


def _print_adaptive_report(
    result: WorkflowResult,
    perf: PerfData,
    adaptive: dict,
    out: TextIO,
    use_color: bool,
) -> None:
    """Print report for adaptive workflows with throughput comparison.

    Supports N rounds: discovers rounds dynamically from step name prefixes,
    reads per-replan decisions from _all_decisions list, and shows pairwise
    throughput comparison for consecutive rounds.
    """
    orig_nt = adaptive.get("original_nthreads", 8)
    all_decisions = adaptive.get("_all_decisions", [adaptive])

    # Discover rounds dynamically from step name prefixes
    round_nums: set[int] = set()
    for s in perf.steps:
        m = re.match(r"Round (\d+):", s.step_name)
        if m:
            round_nums.add(int(m.group(1)))
    n_rounds = max(round_nums) if round_nums else 0

    # Build per-round step lists and metadata
    rounds: list[dict] = []  # index 0 = Round 1, etc.
    for ri in range(1, n_rounds + 1):
        prefix = f"Round {ri}:"
        steps = [s for s in perf.steps if s.step_name.startswith(prefix)]

        # Decisions for the replan *producing* this round (replan_0 -> Round 2)
        dec_idx = ri - 2  # Round 2 -> decisions[0], Round 3 -> decisions[1]
        dec = all_decisions[dec_idx] if 0 <= dec_idx < len(all_decisions) else {}

        job_multiplier = dec.get("job_multiplier", 1)
        n_pipelines = dec.get("n_pipelines", 1)
        per_step_tuning = dec.get("per_step", {})

        # Determine job count for this round
        if steps and steps[0].n_jobs > 0:
            n_jobs = steps[0].n_jobs
        elif ri == 1:
            # Round 1: baseline
            n_jobs = perf.num_proc_jobs // n_rounds if n_rounds else 0
            # When probe is excluded, infer from step data
            if adaptive.get("probe_node") and steps:
                non_step0 = [s for s in steps if not s.step_name.endswith("Step 1")]
                if non_step0:
                    n_jobs = non_step0[0].n_jobs
                elif steps:
                    n_jobs = steps[0].n_jobs
        else:
            n_jobs = perf.num_proc_jobs // n_rounds if n_rounds else 0

        request_cpus = dec.get("new_request_cpus", orig_nt) if job_multiplier > 1 else orig_nt
        step0_n_par = per_step_tuning.get("0", {}).get("n_parallel", 1)

        rounds.append({
            "num": ri,
            "steps": steps,
            "decisions": dec,
            "n_jobs": n_jobs,
            "request_cpus": request_cpus,
            "job_multiplier": job_multiplier,
            "n_pipelines": n_pipelines,
            "per_step_tuning": per_step_tuning,
            "step0_n_par": step0_n_par,
        })

    # Compute per-round throughput
    round_metrics = []
    for rd in rounds:
        n_par = rd["step0_n_par"] if rd["num"] > 1 else 1
        m = _compute_round_metrics(rd["steps"], rd["n_jobs"], rd["request_cpus"],
                                   n_parallel=n_par)
        round_metrics.append(m)

    # Throughput comparison table (all rounds side by side)
    valid_metrics = [m for m in round_metrics if m]
    if len(valid_metrics) >= 2:
        out.write("\n")
        hdr = "  Throughput Comparison"
        if use_color:
            out.write(f"{_BOLD}{hdr}{_RESET}\n")
        else:
            out.write(f"{hdr}\n")

        # Header row
        out.write(f"\n    {'':22s}")
        for i, m in enumerate(round_metrics):
            if m:
                out.write(f"  {'Round ' + str(i + 1):>10s}")
        # Change columns for consecutive pairs
        for i in range(1, len(round_metrics)):
            if round_metrics[i] and round_metrics[i - 1]:
                out.write(f"  {'\u0394' + str(i) + '\u2192' + str(i + 1):>8s}")
        out.write("\n")

        # Events row
        out.write(f"    {'Events':22s}")
        for m in round_metrics:
            if m:
                out.write(f"  {m['events']:>10d}")
        out.write("\n")

        # Jobs × Cores row
        out.write(f"    {'Jobs \u00d7 Cores':22s}")
        for m in round_metrics:
            if m:
                jc = f"{m['n_jobs']}\u00d7{m['request_cpus']}T"
                out.write(f"  {jc:>10s}")
        out.write("\n")

        # Core-hours row
        out.write(f"    {'Core-hours':22s}")
        for m in round_metrics:
            if m:
                out.write(f"  {m['core_hours']:>10.2f}")
        out.write("\n")

        # Ev/core-hour row with deltas
        out.write(f"    {'Ev/core-hour':22s}")
        for m in round_metrics:
            if m:
                out.write(f"  {m['ev_ch']:>10.0f}")
        for i in range(1, len(round_metrics)):
            prev, cur = round_metrics[i - 1], round_metrics[i]
            if prev and cur and prev['ev_ch'] > 0:
                delta = (cur['ev_ch'] - prev['ev_ch']) / prev['ev_ch'] * 100
                out.write(f"  {delta:>+7.1f}%")
        out.write("\n")

    elif valid_metrics:
        m = valid_metrics[0]
        out.write(f"\n  Throughput (Round 1): {m['ev_ch']:.0f} ev/core-hour  |  "
                  f"{m['events']} events, {m['core_hours']:.2f} core-hours\n")

    # Tuning summary (from first replan decisions)
    first_dec = all_decisions[0] if all_decisions else {}
    first_jm = first_dec.get("job_multiplier", 1)
    if first_jm > 1 and len(rounds) >= 2:
        r1_jobs = rounds[0]["n_jobs"]
        r2_jobs = rounds[1]["n_jobs"]
        r2_cpus = rounds[1]["request_cpus"]
        out.write(f"\n  Job split: {r1_jobs} \u2192 {r2_jobs} jobs"
                  f" (\u00d7{first_jm}), {r2_cpus}T\n")
    else:
        n_pipelines = first_dec.get("n_pipelines", 1)
        per_step_tuning = first_dec.get("per_step", {})
        out.write(f"\n  Tuning: {orig_nt}T baseline")
        if n_pipelines > 1:
            out.write(f" \u2192 {n_pipelines} pipelines")
        tuned_vals = [t.get("tuned_nthreads", orig_nt) for t in per_step_tuning.values()]
        if tuned_vals and len(set(tuned_vals)) == 1:
            out.write(f" \u00d7 {tuned_vals[0]}T (uniform)")
        elif tuned_vals:
            out.write(f", per-step: [{', '.join(str(v) for v in tuned_vals)}]")
        out.write("\n")

    # Memory limits
    mem_per_core = adaptive.get("memory_per_core_mb", 0)
    max_mem_per_core = adaptive.get("max_memory_per_core_mb", 0)
    ideal_mem = adaptive.get("ideal_memory_mb", 0)
    actual_mem = adaptive.get("actual_memory_mb", 0)
    if mem_per_core > 0:
        out.write(f"  Memory: {mem_per_core} MB/core (R1)")
        if max_mem_per_core > 0:
            out.write(f", max {max_mem_per_core} MB/core")
        if first_jm > 1:
            r2_mem = first_dec.get("new_request_memory_mb", 0)
            mem_src = first_dec.get("memory_source", "")
            if r2_mem > 0:
                src_str = f"  [{mem_src}]" if mem_src else ""
                out.write(f"\n  R2 request_memory: {r2_mem} MB"
                          f" ({first_dec.get('new_request_cpus', orig_nt)} cores){src_str}")
        elif ideal_mem > 0 and actual_mem > 0 and ideal_mem > actual_mem:
            out.write(f"\n  Memory needed: {ideal_mem} MB"
                      f" ({ideal_mem // orig_nt if orig_nt else 0} MB/core)"
                      f" — capped at {actual_mem} MB")
        elif actual_mem > 0 and actual_mem > mem_per_core * orig_nt:
            out.write(f"\n  R2 request_memory: {actual_mem} MB")
        out.write("\n")

    # Ideal n_parallel (if memory-constrained, not for job split)
    if first_jm <= 1:
        step0_tuning = first_dec.get("per_step", {}).get("0", {})
        ideal_n_par = step0_tuning.get("ideal_n_parallel", 0)
        actual_n_par = step0_tuning.get("n_parallel", 1)
        if ideal_n_par > actual_n_par:
            out.write(f"  Step 0 split: {actual_n_par} instances"
                      f" (ideal: {ideal_n_par}, memory-constrained)\n")

    # Probe data (if probe split was used)
    probe_data = adaptive.get("probe_data", {})
    if probe_data:
        pk = probe_data.get("per_instance_peak_mb", 0)
        # Backward compat: try old key name too
        if not pk:
            pk = probe_data.get("per_instance_cgroup_mb", 0)
        rss = probe_data.get("max_instance_rss_mb", 0)
        n_inst = probe_data.get("num_instances", 0)
        step0_tuning = first_dec.get("per_step", {}).get("0", {})
        mem_src = step0_tuning.get("memory_source", "")
        inst_mem = step0_tuning.get("instance_mem_mb", 0)
        out.write(f"  Probe: {n_inst} instances,"
                  f" {round(pk)} MB/instance peak,"
                  f" {round(rss)} MB/instance RSS"
                  f"  [{mem_src}]\n")
        if inst_mem > 0 and ideal_mem > 0 and orig_nt > 0:
            out.write(f"  To split {ideal_n_par}×: need"
                      f" {ideal_mem // orig_nt} MB/core"
                      f" (current max: {max_mem_per_core} MB/core)\n")

    # Per-round tables
    round_cpu_pcts: list[float] = []
    round_peak_rss: list[float] = []
    for rd in rounds:
        if not rd["steps"]:
            round_cpu_pcts.append(0.0)
            round_peak_rss.append(0.0)
            continue

        ri = rd["num"]
        n_jobs = rd["n_jobs"]
        request_cpus = rd["request_cpus"]
        n_pipelines = rd["n_pipelines"]
        per_step_tuning = rd["per_step_tuning"]

        if ri == 1:
            # Round 1: baseline
            out.write(f"\n  Round {ri} (baseline, {orig_nt}T, {n_jobs} jobs):\n")
            _, _, cpu_pct, peak_rss = _print_step_table(
                rd["steps"], orig_nt, out, show_nthreads=False,
                n_concurrent=1, n_jobs=n_jobs, request_cpus=orig_nt)
        else:
            # Build nthreads map for tuned rounds
            nt_map = {}
            for i in range(len(rd["steps"])):
                si_str = str(i)
                if si_str in per_step_tuning:
                    nt_map[i] = per_step_tuning[si_str].get("tuned_nthreads", orig_nt)
                else:
                    nt_map[i] = orig_nt

            desc_parts = []
            jm = rd["job_multiplier"]
            if jm > 1:
                desc_parts.append(f"{n_jobs} jobs")
                desc_parts.append(f"{request_cpus}T")
            else:
                if n_pipelines > 1:
                    desc_parts.append(f"{n_pipelines} pipelines")
                tuned_unique = sorted(set(nt_map.values()))
                if len(tuned_unique) == 1:
                    desc_parts.append(f"{tuned_unique[0]}T")
                else:
                    desc_parts.append("per-step tuned")

            out.write(f"\n  Round {ri} ({', '.join(desc_parts)}):\n")
            _, _, cpu_pct, peak_rss = _print_step_table(
                rd["steps"], nt_map, out, show_nthreads=True,
                n_concurrent=n_pipelines, n_jobs=n_jobs, request_cpus=request_cpus)

        round_cpu_pcts.append(cpu_pct)
        round_peak_rss.append(peak_rss)

    # Overall CPU efficiency summary
    active_pcts = [(i + 1, p) for i, p in enumerate(round_cpu_pcts) if p > 0]
    if active_pcts:
        out.write("\n  CPU efficiency per job:")
        for ri, p in active_pcts:
            out.write(f"  R{ri} {p:.0f}%")
        # Show delta between first and last
        if len(active_pcts) >= 2:
            delta = active_pcts[-1][1] - active_pcts[0][1]
            out.write(f"  ({delta:+.0f}pp)")
        out.write("\n")

    # Per-round peak memory summary
    active_rss = [(i + 1, r) for i, r in enumerate(round_peak_rss) if r > 0]
    if active_rss:
        out.write("  Peak memory per job: ")
        parts = []
        for ri, rss in active_rss:
            n_jobs = rounds[ri - 1]["n_jobs"]
            machine_gb = rss * n_jobs / 1024
            parts.append(f"R{ri} {rss:.0f} MB ({machine_gb:.1f} GB × {n_jobs} jobs)")
        out.write("  ".join(parts))
        out.write("\n")

    # Time breakdown
    _print_time_breakdown(result, perf, out)


def _print_time_breakdown(
    result: WorkflowResult,
    perf: PerfData,
    out: TextIO,
) -> None:
    """Print time breakdown from DAGMan log."""
    if perf.time_processing > 0:
        out.write(f"\n  Time: processing {perf.time_processing:.0f}s")
        if perf.time_merge > 0:
            out.write(f"  merge {perf.time_merge:.0f}s")
        if perf.time_cleanup > 0:
            out.write(f"  cleanup {perf.time_cleanup:.0f}s")
        if perf.time_overhead > 0:
            out.write(f"  overhead {perf.time_overhead:.0f}s")
        out.write(f"  total {result.elapsed_sec:.0f}s\n")


# ── CPU timeline plot ────────────────────────────────────────


def _rolling_mean(data: list[float], window: int) -> list[float]:
    """Simple moving average, same length as input (edges use smaller windows)."""
    n = len(data)
    result = []
    for i in range(n):
        lo = max(0, i - window // 2)
        hi = min(n, i + window // 2 + 1)
        result.append(sum(data[lo:hi]) / (hi - lo))
    return result


def _annotate_phases(ax, perf: PerfData) -> None:
    """Shade workflow phase regions on the plot."""
    colors = {
        "Processing": ("#2196F3", 0.08),
        "Merge": ("#FF9800", 0.12),
        "Cleanup": ("#4CAF50", 0.12),
    }

    t_cursor = 0.0
    if perf.time_processing > 0:
        c, a = colors["Processing"]
        ax.axvspan(t_cursor, t_cursor + perf.time_processing,
                   color=c, alpha=a, label="Processing")
        t_cursor += perf.time_processing
    if perf.time_overhead > 0:
        t_cursor += perf.time_overhead
    if perf.time_merge > 0:
        c, a = colors["Merge"]
        ax.axvspan(t_cursor, t_cursor + perf.time_merge,
                   color=c, alpha=a, label="Merge")
        t_cursor += perf.time_merge
    if perf.time_cleanup > 0:
        c, a = colors["Cleanup"]
        ax.axvspan(t_cursor, t_cursor + perf.time_cleanup,
                   color=c, alpha=a, label="Cleanup")


def _find_r1r2_boundary(cores: list[float], perf: PerfData) -> int | None:
    """Estimate R1/R2 boundary sample index for adaptive workflows.

    Looks for the deepest sustained dip in CPU usage (the replan gap)
    between the two processing bursts.  Only searches the middle 80%
    of the timeline.
    """
    n = len(cores)
    if n < 40:
        return None
    # Search in the middle 60% of the timeline
    lo = n // 5
    hi = 4 * n // 5
    # Smooth heavily to find the valley
    window = max(12, n // 20)
    smoothed = _rolling_mean(cores, window)
    # Find the minimum in the search window
    min_val = float("inf")
    min_idx = lo
    for i in range(lo, hi):
        if smoothed[i] < min_val:
            min_val = smoothed[i]
            min_idx = i
    # Only report if the dip is significant (below 50% of expected)
    expected = perf.cpu_expected_pct / 100.0
    if expected > 0 and min_val < expected * 0.5:
        return min_idx
    return None


def _save_cpu_plot(
    result: WorkflowResult, perf: PerfData, out_path: Path,
) -> Path | None:
    """Generate CPU timeline PNG for a workflow run."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    samples = perf.cpu_samples
    if len(samples) < 10:
        return None

    # Time axis
    t = [i * POLL_INTERVAL for i in range(len(samples))]
    # Convert CPU% to cores (100% = 1 core)
    cores = [s / 100.0 for s in samples]

    # Smoothing: rolling average (window = ~30s = 6 samples)
    window = min(6, len(cores) // 4)
    if window > 1:
        smoothed = _rolling_mean(cores, window)
    else:
        smoothed = cores

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.fill_between(t, smoothed, alpha=0.3, color="#1976D2")
    ax.plot(t, smoothed, linewidth=0.8, color="#1976D2")
    ax.set_xlabel("Time (s)")
    ax.set_ylabel("CPU cores")
    ax.set_title(f"{result.wf_id:.1f} \u2014 {result.title}")

    # Expected cores line
    expected = perf.cpu_expected_pct / 100.0
    if expected > 0:
        ax.axhline(y=expected, color="red", linestyle="--", alpha=0.5,
                    label=f"Allocated ({expected:.0f} cores)")

    # Phase annotations
    _annotate_phases(ax, perf)

    # Adaptive R1/R2 boundary
    adaptive = perf.raw_job_step_data.get("_adaptive")
    if adaptive:
        boundary = _find_r1r2_boundary(cores, perf)
        if boundary is not None:
            ax.axvline(x=t[boundary], color="#9C27B0", linestyle=":",
                       alpha=0.7, label="R1\u2192R2 replan")

    ax.legend(loc="upper right", fontsize=8)
    ax.set_xlim(0, t[-1])
    ax.set_ylim(0, None)
    fig.tight_layout()
    fig.savefig(str(out_path), dpi=100)
    plt.close(fig)
    return out_path


# ── Catalog listing ──────────────────────────────────────────


def print_catalog_list(
    workflows: list,
    caps: dict[str, bool] | None = None,
    out: TextIO = sys.stdout,
    use_color: bool | None = None,
) -> None:
    """Print a table listing workflow definitions (for --list mode)."""
    from tests.matrix.environment import workflow_can_run

    if use_color is None:
        use_color = hasattr(out, "isatty") and out.isatty()

    hdr = f"{'ID':>7s}  {'Mode':<10s}  {'Jobs':>4s}  {'Size':<6s}  {'Timeout':>7s}  {'Runnable':>8s}  {'Title'}"
    sep = "-" * max(len(hdr) + 10, 80)
    out.write(f"\n{sep}\n{hdr}\n{sep}\n")

    for wf in sorted(workflows, key=lambda w: w.wf_id):
        if caps is not None:
            can_run, reason = workflow_can_run(wf, caps)
            runnable = _color("yes", "passed", use_color) if can_run else _color("no", "failed", use_color)
        else:
            runnable = "?"

        timeout_str = f"{wf.timeout_sec}s"
        fault_mark = " [F]" if wf.fault else ""
        out.write(
            f"{wf.wf_id:>7.1f}  {wf.sandbox_mode:<10s}  {wf.num_jobs:>4d}  "
            f"{wf.size:<6s}  {timeout_str:>7s}  {runnable:>8s}  "
            f"{wf.title}{fault_mark}\n"
        )

    out.write(f"{sep}\n")
    out.write(f"{len(workflows)} workflows\n\n")
