"""Pilot runner: per-step CMSSW pilot with watchdog and FJR parsing.

Provides:
- write_pilot_script(path) — writes the standalone wms2_pilot.py to disk
- parse_fjr(xml_path) — parses a FrameworkJobReport XML
- parse_pilot_report(path) — reads pilot_metrics.json from disk
- compute_summary(step_results, initial_events) — aggregates per-step metrics
"""

from __future__ import annotations

import json
import os
import textwrap
import xml.etree.ElementTree as ET
from typing import Any


def parse_fjr(xml_path: str) -> dict[str, Any]:
    """Parse a CMSSW FrameworkJobReport XML file.

    Returns:
        dict with events_read, events_written, output_modules, wall_time_sec,
        peak_rss_kb, n_threads, n_streams
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # Events read — sum across all InputFile elements
    events_read = 0
    for inp in root.findall(".//InputFile"):
        er = inp.findtext("EventsRead")
        if er is not None:
            events_read += int(er)

    # Events processed — from ProcessingSummary (covers GEN steps with no input)
    events_processed = 0
    for ps in root.findall(".//PerformanceSummary"):
        if ps.get("Metric") == "ProcessingSummary":
            for m in ps.findall("Metric"):
                if m.get("Name") == "NumberEvents":
                    events_processed = int(m.get("Value", 0))

    # For GEN steps there are no InputFile elements; use events_processed
    if events_read == 0 and events_processed > 0:
        events_read = events_processed

    # Output modules
    output_modules: list[dict[str, Any]] = []
    for f in root.findall(".//File"):
        module = f.findtext("ModuleLabel", "")
        total_events = f.findtext("TotalEvents", "0")
        pfn = f.findtext("PFN", "")

        # Resolve file path — PFN may have "file:" prefix
        real_path = pfn
        if real_path.startswith("file:"):
            real_path = real_path[5:]

        size_bytes = 0
        if real_path and os.path.isfile(real_path):
            size_bytes = os.path.getsize(real_path)

        output_modules.append({
            "module": module,
            "events_written": int(total_events),
            "size_bytes": size_bytes,
            "file_path": pfn,
        })

    # Wall time and thread count from Timing
    wall_time_sec = 0.0
    n_threads = 1
    n_streams = 1
    for ps in root.findall(".//PerformanceSummary"):
        if ps.get("Metric") == "Timing":
            for m in ps.findall("Metric"):
                name = m.get("Name", "")
                val = m.get("Value", "0")
                if name == "TotalJobTime":
                    wall_time_sec = float(val)
                elif name == "NumberOfThreads":
                    n_threads = int(val)
                elif name == "NumberOfStreams":
                    n_streams = int(val)

    # Peak RSS — CMSSW reports PeakValueRss in MB; convert to KB
    peak_rss_kb = 0
    for ps in root.findall(".//PerformanceSummary"):
        if ps.get("Metric") == "ApplicationMemory":
            for m in ps.findall("Metric"):
                if m.get("Name") == "PeakValueRss":
                    peak_rss_mb = float(m.get("Value", 0))
                    peak_rss_kb = int(peak_rss_mb * 1024)

    return {
        "events_read": events_read,
        "output_modules": output_modules,
        "wall_time_sec": wall_time_sec,
        "peak_rss_kb": peak_rss_kb,
        "n_threads": n_threads,
        "n_streams": n_streams,
    }


def compute_summary(
    step_results: list[dict[str, Any]], initial_events: int
) -> dict[str, Any]:
    """Aggregate per-step results into summary metrics.

    Args:
        step_results: list of step dicts with events_read, wall_time_sec,
                      peak_rss_kb, output_modules
        initial_events: events requested in step 1 (denominator for sizing)

    Returns:
        dict with total_time_per_event_sec, peak_rss_mb, events_per_second,
        output_size_per_event_kb, per_output_module
    """
    if not step_results or initial_events <= 0:
        return {
            "total_time_per_event_sec": 0.0,
            "peak_rss_mb": 0,
            "events_per_second": 0.0,
            "output_size_per_event_kb": 0.0,
            "per_output_module": {},
        }

    # time_per_event = sum of (step wall time / step events_read)
    total_time_per_event = 0.0
    for step in step_results:
        er = step.get("events_read", 0)
        if er > 0:
            total_time_per_event += step["wall_time_sec"] / er

    # peak RSS across all steps
    peak_rss_kb = max(step.get("peak_rss_kb", 0) for step in step_results)
    peak_rss_mb = peak_rss_kb / 1024

    # events_per_second = inverse of total_time_per_event
    events_per_second = 1.0 / total_time_per_event if total_time_per_event > 0 else 0.0

    # Output sizing from the last step's output modules
    last_step = step_results[-1]
    last_modules = last_step.get("output_modules", [])
    total_output_bytes = sum(m.get("size_bytes", 0) for m in last_modules)
    output_size_per_event_kb = (total_output_bytes / 1024) / initial_events

    # Per-output-module detail from last step
    per_output_module: dict[str, Any] = {}
    for m in last_modules:
        module_name = m.get("module", "unknown")
        size_per_event_kb = (m.get("size_bytes", 0) / 1024) / initial_events
        filtering_ratio = m.get("events_written", 0) / initial_events
        per_output_module[module_name] = {
            "size_per_event_kb": round(size_per_event_kb, 3),
            "filtering_ratio": round(filtering_ratio, 6),
        }

    return {
        "total_time_per_event_sec": round(total_time_per_event, 6),
        "peak_rss_mb": round(peak_rss_mb, 1),
        "events_per_second": round(events_per_second, 6),
        "output_size_per_event_kb": round(output_size_per_event_kb, 3),
        "per_output_module": per_output_module,
    }


def parse_pilot_report(path: str) -> dict[str, Any]:
    """Read pilot_metrics.json from disk."""
    with open(path) as f:
        return json.load(f)


def write_pilot_script(path: str) -> None:
    """Write the standalone wms2_pilot.py script to disk.

    This script runs on a worker node using only stdlib. It executes each
    CMSSW step with a single timeout (no retry loop). The pilot is a
    functional smoke test — planning parameters come from the request.
    """
    script = textwrap.dedent('''\
        #!/usr/bin/env python3
        """wms2_pilot.py — Per-step CMSSW pilot with watchdog.

        Runs each step independently, parsing FJR after each.
        A watchdog kills cmsRun if it exceeds the timeout.
        """
        import argparse
        import json
        import os
        import signal
        import subprocess
        import sys
        import xml.etree.ElementTree as ET


        def parse_fjr(xml_path):
            """Parse FrameworkJobReport XML, return dict of metrics."""
            tree = ET.parse(xml_path)
            root = tree.getroot()

            events_read = 0
            for inp in root.findall(".//InputFile"):
                er = inp.findtext("EventsRead")
                if er is not None:
                    events_read += int(er)

            # Events processed from ProcessingSummary (covers GEN with no input)
            events_processed = 0
            for ps in root.findall(".//PerformanceSummary"):
                if ps.get("Metric") == "ProcessingSummary":
                    for m in ps.findall("Metric"):
                        if m.get("Name") == "NumberEvents":
                            events_processed = int(m.get("Value", 0))
            if events_read == 0 and events_processed > 0:
                events_read = events_processed

            output_modules = []
            for f in root.findall(".//File"):
                module = f.findtext("ModuleLabel", "")
                total_events = f.findtext("TotalEvents", "0")
                pfn = f.findtext("PFN", "")
                real_path = pfn
                if real_path.startswith("file:"):
                    real_path = real_path[5:]
                size_bytes = 0
                if real_path and os.path.isfile(real_path):
                    size_bytes = os.path.getsize(real_path)
                output_modules.append({
                    "module": module,
                    "events_written": int(total_events),
                    "size_bytes": size_bytes,
                    "file_path": pfn,
                })

            wall_time_sec = 0.0
            n_threads = 1
            n_streams = 1
            for ps in root.findall(".//PerformanceSummary"):
                if ps.get("Metric") == "Timing":
                    for m in ps.findall("Metric"):
                        name = m.get("Name", "")
                        val = m.get("Value", "0")
                        if name == "TotalJobTime":
                            wall_time_sec = float(val)
                        elif name == "NumberOfThreads":
                            n_threads = int(val)
                        elif name == "NumberOfStreams":
                            n_streams = int(val)

            peak_rss_kb = 0
            for ps in root.findall(".//PerformanceSummary"):
                if ps.get("Metric") == "ApplicationMemory":
                    for m in ps.findall("Metric"):
                        if m.get("Name") == "PeakValueRss":
                            peak_rss_mb = float(m.get("Value", 0))
                            peak_rss_kb = int(peak_rss_mb * 1024)

            return {
                "events_read": events_read,
                "output_modules": output_modules,
                "wall_time_sec": wall_time_sec,
                "peak_rss_kb": peak_rss_kb,
                "n_threads": n_threads,
                "n_streams": n_streams,
            }


        def run_step_with_watchdog(cmd, timeout_sec, work_dir):
            """Run a command with a watchdog timeout.

            Returns True on success, False on failure/timeout.
            On timeout, kills the process group.
            """
            proc = subprocess.Popen(
                cmd,
                cwd=work_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid,
            )
            try:
                stdout, stderr = proc.communicate(timeout=timeout_sec)
                if stdout:
                    sys.stdout.buffer.write(stdout)
                if stderr:
                    sys.stderr.buffer.write(stderr)
                return proc.returncode == 0
            except subprocess.TimeoutExpired:
                # Kill the entire process group
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except OSError:
                    pass
                proc.wait()
                return False


        def setup_cmssw(cmssw_version, scram_arch, work_dir):
            """Set up CMSSW environment and return env dict."""
            setup_cmd = (
                f"source /cvmfs/cms.cern.ch/cmsset_default.sh && "
                f"export SCRAM_ARCH={scram_arch} && "
                f"cd {work_dir} && "
                f"scramv1 project CMSSW {cmssw_version} && "
                f"cd {cmssw_version}/src && "
                f"eval $(scramv1 runtime -sh) && "
                f"env"
            )
            result = subprocess.run(
                ["bash", "-c", setup_cmd],
                capture_output=True, text=True, timeout=300,
            )
            if result.returncode != 0:
                print(f"CMSSW setup failed: {result.stderr}", file=sys.stderr)
                sys.exit(1)

            env = {}
            for line in result.stdout.splitlines():
                if "=" in line:
                    key, _, val = line.partition("=")
                    env[key] = val
            return env


        def main():
            parser = argparse.ArgumentParser(description="WMS2 pilot smoke test")
            parser.add_argument("--sandbox", required=True)
            parser.add_argument("--input", default="")
            parser.add_argument("--initial-events", type=int, default=200)
            parser.add_argument("--timeout", type=int, default=900)
            args = parser.parse_args()

            work_dir = os.getcwd()

            # Extract sandbox
            sandbox = args.sandbox
            if os.path.isfile(sandbox):
                subprocess.run(["tar", "xzf", sandbox], check=True, cwd=work_dir)

            # Read manifest
            manifest_path = os.path.join(work_dir, "manifest.json")
            if not os.path.isfile(manifest_path):
                print("ERROR: manifest.json not found", file=sys.stderr)
                sys.exit(1)
            with open(manifest_path) as f:
                manifest = json.load(f)

            steps = manifest.get("steps", [])
            if not steps:
                print("ERROR: no steps in manifest", file=sys.stderr)
                sys.exit(1)

            cmssw_version = manifest.get("cmssw_version", "")
            scram_arch = manifest.get("scram_arch", "")

            # Setup CMSSW environment
            cmssw_env = None
            if cmssw_version and os.path.isdir("/cvmfs/cms.cern.ch"):
                cmssw_env = setup_cmssw(cmssw_version, scram_arch, work_dir)

            # Build initial input files string
            input_files_str = args.input

            step_results = []
            prev_output_files = []  # output files from previous step

            for step_idx, step_def in enumerate(steps):
                step_name = step_def.get("name", f"step{step_idx + 1}")
                pset = step_def.get("pset", "")
                report_xml = os.path.join(work_dir, f"report_step{step_idx + 1}.xml")

                print(f"\\n=== Step {step_idx + 1}: {step_name} ===")

                # Build cmsRun command
                cmd = ["cmsRun", "-j", report_xml, pset]

                if step_idx == 0:
                    # Step 1: use initial input files and event count
                    if input_files_str:
                        cmd.append(f"inputFiles={input_files_str}")
                    cmd.append(f"maxEvents={args.initial_events}")
                else:
                    # Step 2+: input is previous step output
                    if not prev_output_files:
                        print(f"ERROR: No output from previous step for {step_name}",
                              file=sys.stderr)
                        sys.exit(1)
                    input_arg = "inputFiles=" + ",".join(
                        f"file:{fp}" for fp in prev_output_files
                    )
                    cmd.append(input_arg)

                print(f"  Running: {' '.join(cmd)}")
                success = run_step_with_watchdog(cmd, args.timeout, work_dir)

                if not success:
                    print(f"ERROR: Step {step_name} failed or timed out",
                          file=sys.stderr)
                    # Write partial results before exiting
                    if step_results:
                        initial_events = step_results[0]["events_read"] if step_results else 0
                        summary = compute_summary(step_results, initial_events)
                        report = {"steps": step_results, "summary": summary,
                                  "failed_step": step_name}
                        with open("pilot_metrics.json", "w") as f:
                            json.dump(report, f, indent=2)
                    sys.exit(1)

                # Parse FJR
                fjr = parse_fjr(report_xml)
                fjr["name"] = step_name
                fjr["events_requested"] = (
                    args.initial_events if step_idx == 0 else fjr["events_read"]
                )

                step_results.append(fjr)

                # Chain outputs: collect output file paths for next step
                prev_output_files = [
                    m["file_path"]
                    for m in fjr.get("output_modules", [])
                    if m.get("file_path") and os.path.isfile(m["file_path"])
                ]

                print(f"  Events read: {fjr['events_read']}")
                for m in fjr.get("output_modules", []):
                    print(f"  Output: {m['module']} — "
                          f"{m['events_written']} events, "
                          f"{m['size_bytes']} bytes")

            # Compute summary
            initial_events = step_results[0]["events_read"] if step_results else 0
            summary = compute_summary(step_results, initial_events)

            report = {"steps": step_results, "summary": summary}
            with open("pilot_metrics.json", "w") as f:
                json.dump(report, f, indent=2)
            print(f"\\nWrote pilot_metrics.json")


        def compute_summary(step_results, initial_events):
            """Aggregate per-step results into summary."""
            if not step_results or initial_events <= 0:
                return {}

            total_time_per_event = 0.0
            for step in step_results:
                er = step.get("events_read", 0)
                if er > 0:
                    total_time_per_event += step["wall_time_sec"] / er

            peak_rss_kb = max(step.get("peak_rss_kb", 0) for step in step_results)
            peak_rss_mb = round(peak_rss_kb / 1024, 1)
            events_per_second = round(1.0 / total_time_per_event, 6) if total_time_per_event > 0 else 0.0

            last_step = step_results[-1]
            last_modules = last_step.get("output_modules", [])
            total_output_bytes = sum(m.get("size_bytes", 0) for m in last_modules)
            output_size_per_event_kb = round(
                (total_output_bytes / 1024) / initial_events, 3
            )

            per_output_module = {}
            for m in last_modules:
                name = m.get("module", "unknown")
                per_output_module[name] = {
                    "size_per_event_kb": round(
                        (m.get("size_bytes", 0) / 1024) / initial_events, 3
                    ),
                    "filtering_ratio": round(
                        m.get("events_written", 0) / initial_events, 6
                    ),
                }

            return {
                "total_time_per_event_sec": round(total_time_per_event, 6),
                "peak_rss_mb": peak_rss_mb,
                "events_per_second": events_per_second,
                "output_size_per_event_kb": output_size_per_event_kb,
                "per_output_module": per_output_module,
            }


        if __name__ == "__main__":
            main()
    ''')

    with open(path, "w") as f:
        f.write(script)
    os.chmod(path, 0o755)
