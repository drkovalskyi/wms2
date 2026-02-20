"""Pass/fail summary table with performance metrics for matrix test results."""

from __future__ import annotations

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
            out.write(f"{'':>7s}  {'':8s}  {'':>7s}  └─ {r.reason}\n")

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


def _fmt_val_sd(mean: float, sd: float, fmt: str = ".0f", pct: bool = False) -> str:
    """Format mean ± stddev, or just mean if no stddev."""
    scale = 100 if pct else 1
    m = mean * scale
    s = sd * scale
    suffix = "%" if pct else ""
    if s > 0:
        return f"{m:{fmt}}{suffix} \u00b1{s:{fmt}}"
    return f"{m:{fmt}}{suffix}"


def _print_perf_detail(
    result: WorkflowResult,
    out: TextIO,
    use_color: bool,
) -> None:
    """Print per-step performance table for a single workflow."""
    perf = result.perf
    if not perf:
        return

    out.write(f"{'─' * 90}\n")
    title = f"Performance: {result.wf_id:.1f} — {result.title}"
    if use_color:
        out.write(f"{_BOLD}{title}{_RESET}\n")
    else:
        out.write(f"{title}\n")

    out.write(f"  Jobs: {perf.num_proc_jobs}    Wall: {result.elapsed_sec:.0f}s\n")

    # CPU utilization
    if perf.cpu_avg_pct > 0:
        eff = perf.cpu_avg_pct / perf.cpu_expected_pct * 100 if perf.cpu_expected_pct else 0
        out.write(
            f"  CPU:  avg {perf.cpu_avg_pct:.0f}% ({perf.cpu_avg_pct / 100:.1f} cores)"
            f"  peak {perf.cpu_peak_pct:.0f}% ({perf.cpu_peak_pct / 100:.1f} cores)"
            f"  expected {perf.cpu_expected_pct:.0f}%"
            f"  efficiency {eff:.0f}%\n"
        )

    # Per-step table
    if perf.steps:
        # Determine if we have multi-job data (stddev available)
        has_sd = any(s.n_jobs > 1 for s in perf.steps)

        out.write(f"\n  {'Step':<8s}"
                  f"  {'Wall(s)':>19s}"
                  f"  {'CPU Eff':>14s}"
                  f"  {'RSS(MB)':>16s}"
                  f"  {'Events':>7s}"
                  f"  {'Tput(ev/s)':>11s}"
                  f"  {'n':>3s}\n")
        out.write(f"  {'─' * 8}"
                  f"  {'─' * 19}"
                  f"  {'─' * 14}"
                  f"  {'─' * 16}"
                  f"  {'─' * 7}"
                  f"  {'─' * 11}"
                  f"  {'─' * 3}\n")
        total_wall_mean = 0.0
        total_events = 0
        for s in perf.steps:
            # Wall: mean ± sd [min-max]
            if has_sd and s.wall_sec_stddev > 0:
                wall_str = f"{s.wall_sec_mean:.0f} \u00b1{s.wall_sec_stddev:.0f}"
            else:
                wall_str = f"{s.wall_sec_mean:.0f}"
            if s.wall_sec_min != s.wall_sec_max:
                wall_str += f" [{s.wall_sec_min:.0f}-{s.wall_sec_max:.0f}]"

            # CPU efficiency: mean ± sd (as percentage)
            if s.cpu_eff_mean > 0:
                eff_str = _fmt_val_sd(s.cpu_eff_mean, s.cpu_eff_stddev, ".1f", pct=True)
            else:
                eff_str = "-"

            # RSS: mean ± sd / max
            if s.rss_mb_max > 0:
                if has_sd and s.rss_mb_stddev > 0:
                    rss_str = f"{s.rss_mb_mean:.0f} \u00b1{s.rss_mb_stddev:.0f}/{s.rss_mb_max:.0f}"
                else:
                    rss_str = f"{s.rss_mb_mean:.0f}/{s.rss_mb_max:.0f}"
            else:
                rss_str = "-"

            evts_str = str(s.events_total) if s.events_total else "-"
            tput_str = f"{s.throughput_ev_s:.2f}" if s.throughput_ev_s else "-"
            n_str = str(s.n_jobs)

            out.write(f"  {s.step_name:<8s}"
                      f"  {wall_str:>19s}"
                      f"  {eff_str:>14s}"
                      f"  {rss_str:>16s}"
                      f"  {evts_str:>7s}"
                      f"  {tput_str:>11s}"
                      f"  {n_str:>3s}\n")
            total_wall_mean += s.wall_sec_mean
            total_events = max(total_events, s.events_total)
        out.write(f"  {'─' * 8}  {'─' * 19}\n")
        out.write(f"  {'Total':<8s}  {total_wall_mean:>19.0f}s/job\n")

    # Overall efficiency summary
    if perf.steps:
        # Wall-time-weighted CPU efficiency across all steps
        weighted_eff_num = sum(s.cpu_eff_mean * s.wall_sec_mean for s in perf.steps)
        weighted_eff_den = sum(s.wall_sec_mean for s in perf.steps)
        overall_cpu_eff = weighted_eff_num / weighted_eff_den if weighted_eff_den > 0 else 0

        total_wall_per_job = sum(s.wall_sec_mean for s in perf.steps)
        peak_rss = max(s.rss_mb_max for s in perf.steps) if any(s.rss_mb_max for s in perf.steps) else 0

        out.write(f"\n  Overall:\n")

        ncpus = perf.cpu_expected_pct / (perf.num_proc_jobs * 100) if perf.num_proc_jobs else 0
        allocated_cores = ncpus * perf.num_proc_jobs

        if overall_cpu_eff > 0:
            eff_cores = overall_cpu_eff * ncpus
            out.write(f"    CMSSW threading: {overall_cpu_eff * 100:.1f}%"
                      f" ({eff_cores:.1f} / {ncpus:.0f} cores used by cmsRun)\n")

        # System-level: actual CPU utilization measured via /proc/stat
        if perf.cpu_avg_pct > 0 and allocated_cores > 0:
            system_eff = perf.cpu_avg_pct / (allocated_cores * 100)
            effective_cores = perf.cpu_avg_pct / 100
            out.write(f"    System-level:    {system_eff * 100:.1f}%"
                      f" ({effective_cores:.1f} / {allocated_cores:.0f} allocated cores"
                      f" over {result.elapsed_sec:.0f}s)\n")

        if peak_rss > 0:
            out.write(f"    Peak RSS:        {peak_rss:.0f} MB\n")

    # Time breakdown from DAGMan log
    if perf.time_processing > 0:
        out.write(f"\n  Time breakdown:\n")
        out.write(f"    Processing: {perf.time_processing:.0f}s\n")
        if perf.time_merge > 0:
            out.write(f"    Merge:      {perf.time_merge:.0f}s\n")
        if perf.time_cleanup > 0:
            out.write(f"    Cleanup:    {perf.time_cleanup:.0f}s\n")
        if perf.time_overhead > 0:
            out.write(f"    Overhead:   {perf.time_overhead:.0f}s"
                      f" (scheduling, transfer, DAGMan gaps)\n")

    # Output
    if perf.merged_files > 0:
        size_mb = perf.merged_bytes / (1024 * 1024)
        out.write(f"\n  Output: {perf.merged_files} merged files, {size_mb:.1f} MB")
        if perf.unmerged_cleaned:
            out.write("  (unmerged cleaned)")
        out.write("\n")

    out.write("\n")


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
