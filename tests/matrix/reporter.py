"""Pass/fail summary table with performance metrics for matrix test results."""

from __future__ import annotations

import re
import sys
from typing import TextIO

from tests.matrix.runner import PerfData, WorkflowResult

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
            _print_perf_detail(r, out, use_color)


# ── Throughput computation ───────────────────────────────────


def _compute_round_metrics(steps, total_cores):
    """Compute aggregate throughput metrics for a list of StepPerf objects.

    Args:
        steps: list of StepPerf objects for one round
        total_cores: total allocated cores across all concurrent jobs

    Returns dict with events, proc_wall, ev_s, ev_ch.
    """
    if not steps:
        return {}
    total_events = max(s.events_total for s in steps) if steps else 0
    proc_wall = sum(s.wall_sec_max for s in steps)
    if proc_wall <= 0 or total_events <= 0:
        return {}
    ev_s = total_events / proc_wall
    ev_ch = total_events / (total_cores * proc_wall / 3600)
    return {
        "events": total_events,
        "proc_wall": proc_wall,
        "ev_s": ev_s,
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
    total_cores = nthreads * n_jobs
    m = _compute_round_metrics(perf.steps, total_cores)
    if m:
        out.write(f"\n  Throughput: {m['ev_s']:.2f} ev/s  |  "
                  f"{m['ev_ch']:.0f} ev/core-hour  |  "
                  f"{m['events']} events in {m['proc_wall']:.0f}s\n")

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
    """Print report for adaptive workflows with throughput comparison."""
    orig_nt = adaptive.get("original_nthreads", 8)
    n_pipelines = adaptive.get("n_pipelines", 1)
    per_step_tuning = adaptive.get("per_step", {})

    # Split steps by round
    round1_steps = [s for s in perf.steps if s.step_name.startswith("Round 1:")]
    round2_steps = [s for s in perf.steps if s.step_name.startswith("Round 2:")]

    # Jobs per round (adaptive always has 2 work units)
    jobs_per_round = perf.num_proc_jobs // 2 if perf.num_proc_jobs else 0
    total_cores = orig_nt * jobs_per_round

    # Compute per-round throughput using total allocated cores
    r1 = _compute_round_metrics(round1_steps, total_cores)
    r2 = _compute_round_metrics(round2_steps, total_cores)

    # Throughput comparison headline
    if r1 and r2:
        out.write("\n")
        hdr = "  Throughput Comparison"
        if use_color:
            out.write(f"{_BOLD}{hdr}{_RESET}\n")
        else:
            out.write(f"{hdr}\n")

        delta_ev_s = (r2['ev_s'] - r1['ev_s']) / r1['ev_s'] * 100 if r1['ev_s'] > 0 else 0
        delta_ev_ch = (r2['ev_ch'] - r1['ev_ch']) / r1['ev_ch'] * 100 if r1['ev_ch'] > 0 else 0

        out.write(f"\n    {'':22s}  {'Round 1':>10s}  {'Round 2':>10s}  {'Change':>8s}\n")
        out.write(f"    {'Events':22s}  {r1['events']:>10d}  {r2['events']:>10d}\n")
        out.write(f"    {'Processing wall':22s}  {r1['proc_wall']:>9.0f}s  {r2['proc_wall']:>9.0f}s\n")
        out.write(f"    {'Throughput (ev/s)':22s}  {r1['ev_s']:>10.2f}  {r2['ev_s']:>10.2f}  {delta_ev_s:>+7.1f}%\n")
        out.write(f"    {'Ev/core-hour':22s}  {r1['ev_ch']:>10.0f}  {r2['ev_ch']:>10.0f}  {delta_ev_ch:>+7.1f}%\n")
    elif r1:
        out.write(f"\n  Throughput (Round 1): {r1['ev_s']:.2f} ev/s  |  "
                  f"{r1['ev_ch']:.0f} ev/core-hour  |  "
                  f"{r1['events']} events in {r1['proc_wall']:.0f}s\n")

    # Tuning summary
    out.write(f"\n  Tuning: {orig_nt}T baseline")
    if n_pipelines > 1:
        out.write(f" \u2192 {n_pipelines} pipelines")
    tuned_vals = [t.get("tuned_nthreads", orig_nt) for t in per_step_tuning.values()]
    if tuned_vals and len(set(tuned_vals)) == 1:
        out.write(f" \u00d7 {tuned_vals[0]}T (uniform)")
    elif tuned_vals:
        out.write(f", per-step: [{', '.join(str(v) for v in tuned_vals)}]")
    out.write("\n")

    # Round 1 table
    r1_cpu_pct = 0.0
    if round1_steps:
        out.write(f"\n  Round 1 (baseline, {orig_nt}T):\n")
        _, _, r1_cpu_pct, _ = _print_step_table(
            round1_steps, orig_nt, out, show_nthreads=False,
            n_concurrent=1, n_jobs=jobs_per_round, request_cpus=orig_nt)

    # Round 2 table
    r2_cpu_pct = 0.0
    if round2_steps:
        # Build nthreads map for R2
        r2_nt_map = {}
        for i in range(len(round2_steps)):
            si_str = str(i)
            if si_str in per_step_tuning:
                r2_nt_map[i] = per_step_tuning[si_str].get("tuned_nthreads", orig_nt)
            else:
                r2_nt_map[i] = orig_nt

        desc_parts = []
        if n_pipelines > 1:
            desc_parts.append(f"{n_pipelines} pipelines")
        tuned_unique = sorted(set(r2_nt_map.values()))
        if len(tuned_unique) == 1:
            desc_parts.append(f"{tuned_unique[0]}T")
        else:
            desc_parts.append("per-step tuned")

        out.write(f"\n  Round 2 ({', '.join(desc_parts)}):\n")
        _, _, r2_cpu_pct, _ = _print_step_table(
            round2_steps, r2_nt_map, out, show_nthreads=True,
            n_concurrent=n_pipelines, n_jobs=jobs_per_round, request_cpus=orig_nt)

    # Overall CPU efficiency summary
    if r1_cpu_pct > 0:
        out.write(f"\n  CPU efficiency per job: R1 {r1_cpu_pct:.0f}%")
        if r2_cpu_pct > 0:
            delta = r2_cpu_pct - r1_cpu_pct
            out.write(f"  R2 {r2_cpu_pct:.0f}%  ({delta:+.0f}pp)")
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
