#!/usr/bin/env python3
"""WMS2 cmsRun Simulator — produces identical artifacts to real cmsRun in seconds.

Standalone script (no WMS2 imports). Included in sandbox tarball, runs inside
HTCondor jobs. For each step in a StepChain it:

  - Allocates real memory (bytearray, page-touched) for accurate HTCondor ImageSize
  - Burns real CPU (threaded busy loop) for accurate cpu.stat
  - Creates valid ROOT files via uproot with an Events TTree
  - Writes FJR XML (report_stepN.xml) compatible with all pipeline parsers
  - Chains steps (output of step N is input to step N+1)
  - Models resource scaling as a function of nThreads using Amdahl's law

Usage:
    python3 simulator.py --manifest manifest.json --node-index N \\
        --events-per-job E --first-event F [--ncpus C]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import threading
import time
import xml.etree.ElementTree as ET


# ── Resource models ──────────────────────────────────────────────


def compute_resource_profile(
    step: dict, ncpus: int, events: int
) -> dict:
    """Compute resource usage for a step given actual ncpus and event count.

    Uses Amdahl's law for CPU efficiency and a linear model for memory.

    Returns dict with:
        eff_cores      - effective parallel cores (Amdahl)
        cpu_efficiency - eff_cores / ncpus
        peak_rss_mb    - base + marginal * ncpus
        wall_sec       - events * wall_per_event / eff_cores
        cpu_time_sec   - wall * eff_cores (total CPU seconds consumed)
    """
    serial_fraction = step.get("serial_fraction", 0.10)
    base_memory_mb = step.get("base_memory_mb", 1500)
    marginal_per_thread_mb = step.get("marginal_per_thread_mb", 250)
    wall_sec_per_event = step.get("wall_sec_per_event", 0.05)

    # Amdahl's law
    eff_cores = serial_fraction + (1 - serial_fraction) * ncpus
    cpu_efficiency = eff_cores / ncpus

    # Memory: base + marginal per thread
    peak_rss_mb = base_memory_mb + marginal_per_thread_mb * ncpus

    # Wall time: events * time_per_event / effective_cores
    wall_sec = events * wall_sec_per_event / eff_cores if eff_cores > 0 else 0

    # CPU time: wall * effective_cores (total CPU seconds consumed)
    cpu_time_sec = wall_sec * eff_cores

    return {
        "eff_cores": eff_cores,
        "cpu_efficiency": cpu_efficiency,
        "peak_rss_mb": peak_rss_mb,
        "wall_sec": wall_sec,
        "cpu_time_sec": cpu_time_sec,
    }


# ── Memory simulation ───────────────────────────────────────────


def allocate_memory(size_mb: float) -> bytearray:
    """Allocate size_mb of memory and touch every page for accurate RSS."""
    size_bytes = int(size_mb * 1024 * 1024)
    buf = bytearray(size_bytes)
    # Touch every page (4096 bytes) to force RSS commitment
    for i in range(0, size_bytes, 4096):
        buf[i] = 1
    return buf


# ── CPU simulation ───────────────────────────────────────────────


def burn_cpu(wall_sec: float, eff_cores: float, ncpus: int) -> None:
    """Burn CPU using threaded busy loops matching the resource profile.

    Spawns round(eff_cores) busy threads + remaining idle threads
    for wall_sec seconds.
    """
    if wall_sec <= 0:
        return

    n_busy = max(1, round(eff_cores))
    n_idle = max(0, ncpus - n_busy)
    stop_event = threading.Event()

    def busy_loop():
        x = 0.0
        while not stop_event.is_set():
            x += math.sin(x + 1.0)

    def idle_loop():
        stop_event.wait()

    threads = []
    for _ in range(n_busy):
        t = threading.Thread(target=busy_loop, daemon=True)
        t.start()
        threads.append(t)
    for _ in range(n_idle):
        t = threading.Thread(target=idle_loop, daemon=True)
        t.start()
        threads.append(t)

    time.sleep(wall_sec)
    stop_event.set()
    for t in threads:
        t.join(timeout=5)


# ── ROOT file creation ──────────────────────────────────────────


def create_root_file(
    output_path: str, events: int, target_size_kb: float
) -> int:
    """Create a ROOT file with an Events TTree using uproot.

    Contains run, event, luminosityBlock branches plus padding to reach
    target size.  Returns actual file size in bytes.
    """
    import numpy as np
    import uproot

    run = np.ones(events, dtype=np.uint32)
    event = np.arange(1, events + 1, dtype=np.uint64)
    lumi = np.ones(events, dtype=np.uint32)

    # Estimate base size and compute padding needed
    base_size_kb = 1 + (events * 16) / 1024
    padding_kb = max(0, target_size_kb - base_size_kb)
    padding_per_event = int(padding_kb * 1024 / max(events, 1))

    data: dict = {
        "run": run,
        "event": event,
        "luminosityBlock": lumi,
    }
    if padding_per_event > 0:
        cols = max(1, padding_per_event // 4)
        data["padding"] = np.random.randint(
            0, 255, size=(events, cols), dtype=np.int32
        )

    with uproot.recreate(output_path) as f:
        f["Events"] = data

    return os.path.getsize(output_path)


# ── FJR XML generation ──────────────────────────────────────────


def write_fjr_xml(
    fjr_path: str,
    step_config: dict,
    profile: dict,
    events: int,
    output_path: str,
    input_path: str | None = None,
    step_num: int = 1,
) -> None:
    """Write a FrameworkJobReport XML file compatible with all WMS2 parsers.

    Produces the same XML structure that cmsRun generates, with:
      - InputFile (omitted for GEN steps)
      - File (output file with PFN, ModuleLabel, TotalEvents)
      - PerformanceReport (Timing, ApplicationMemory, ProcessingSummary,
        StorageStatistics)
    """
    root = ET.Element("FrameworkJobReport")

    # InputFile — omitted for GEN steps (no input data)
    if input_path:
        inp = ET.SubElement(root, "InputFile")
        ET.SubElement(inp, "LFN").text = ""
        ET.SubElement(inp, "PFN").text = f"file:{input_path}"
        ET.SubElement(inp, "EventsRead").text = str(events)

    # Output File
    out = ET.SubElement(root, "File")
    ET.SubElement(out, "LFN").text = ""
    ET.SubElement(out, "PFN").text = f"file:{output_path}"
    ET.SubElement(out, "TotalEvents").text = str(events)
    ET.SubElement(out, "OutputModuleClass").text = "PoolOutputModule"

    output_module = step_config.get("output_module", f"step{step_num}output")
    ET.SubElement(out, "ModuleLabel").text = output_module

    # PerformanceReport
    perf_report = ET.SubElement(root, "PerformanceReport")

    wall = profile["wall_sec"]
    cpu = profile["cpu_time_sec"]
    ncpus = step_config.get("multicore", 1)
    throughput = events / wall if wall > 0 else 0
    avg_event_time = wall / events if events > 0 else 0

    # Timing
    timing = ET.SubElement(
        perf_report, "PerformanceSummary", Metric="Timing"
    )
    for name, value in [
        ("TotalJobTime", f"{wall:.4f}"),
        ("TotalJobCPU", f"{cpu:.4f}"),
        ("NumberOfThreads", str(ncpus)),
        ("EventThroughput", f"{throughput:.6f}"),
        ("AvgEventTime", f"{avg_event_time:.6f}"),
    ]:
        ET.SubElement(timing, "Metric", Name=name, Value=value)

    # ApplicationMemory — PeakValueRss is in MB (matches CMSSW FJR convention)
    memory = ET.SubElement(
        perf_report, "PerformanceSummary", Metric="ApplicationMemory"
    )
    peak_rss = profile["peak_rss_mb"]
    peak_vsize = peak_rss * 1.2
    ET.SubElement(
        memory, "Metric", Name="PeakValueRss", Value=f"{peak_rss:.2f}"
    )
    ET.SubElement(
        memory, "Metric", Name="PeakValueVsize", Value=f"{peak_vsize:.2f}"
    )

    # ProcessingSummary
    proc_summary = ET.SubElement(
        perf_report, "PerformanceSummary", Metric="ProcessingSummary"
    )
    ET.SubElement(
        proc_summary, "Metric", Name="NumberEvents", Value=str(events)
    )

    # StorageStatistics
    storage = ET.SubElement(
        perf_report, "PerformanceSummary", Metric="StorageStatistics"
    )
    output_size_mb = (
        os.path.getsize(output_path) / (1024 * 1024)
        if os.path.isfile(output_path)
        else 0
    )
    input_size_mb = (
        os.path.getsize(input_path) / (1024 * 1024)
        if input_path and os.path.isfile(input_path)
        else 0
    )
    ET.SubElement(
        storage,
        "Metric",
        Name="Timing-file-read-totalMegabytes",
        Value=f"{input_size_mb:.2f}",
    )
    ET.SubElement(
        storage,
        "Metric",
        Name="Timing-file-write-totalMegabytes",
        Value=f"{output_size_mb:.2f}",
    )

    # Write formatted XML
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(fjr_path, encoding="unicode", xml_declaration=True)


# ── Main simulator logic ────────────────────────────────────────


def run_simulator(
    manifest_path: str,
    node_index: int,
    events_per_job: int,
    first_event: int = 0,
    ncpus_override: int = 0,
) -> int:
    """Run the simulator for all steps in the manifest.

    Returns 0 on success, non-zero on failure.
    """
    with open(manifest_path) as f:
        manifest = json.load(f)

    steps = manifest.get("steps", [])
    if not steps:
        print("ERROR: No steps in manifest", flush=True)
        return 1

    prev_output = None

    for step_idx, step in enumerate(steps):
        step_num = step_idx + 1
        step_name = step.get("name", f"step{step_num}")

        # Determine actual ncpus
        ncpus = (
            ncpus_override
            if ncpus_override > 0
            else step.get("multicore", 1)
        )

        print(f"\n--- Step {step_num}: {step_name} (ncpus={ncpus}) ---", flush=True)

        # Compute resource profile for actual ncpus
        profile = compute_resource_profile(step, ncpus, events_per_job)
        print(f"  eff_cores:  {profile['eff_cores']:.2f}", flush=True)
        print(f"  cpu_eff:    {profile['cpu_efficiency']:.4f}", flush=True)
        print(f"  peak_rss:   {profile['peak_rss_mb']:.0f} MB", flush=True)
        print(f"  wall_time:  {profile['wall_sec']:.2f} s", flush=True)
        print(f"  cpu_time:   {profile['cpu_time_sec']:.2f} s", flush=True)

        # Update step with actual ncpus for FJR
        step_with_ncpus = dict(step, multicore=ncpus)

        # Allocate memory (page-touched for RSS accuracy)
        print(
            f"  Allocating {profile['peak_rss_mb']:.0f} MB memory...",
            flush=True,
        )
        mem_buf = allocate_memory(profile["peak_rss_mb"])

        # Burn CPU
        print(
            f"  Burning CPU for {profile['wall_sec']:.2f}s "
            f"({profile['eff_cores']:.1f} busy threads)...",
            flush=True,
        )
        burn_cpu(profile["wall_sec"], profile["eff_cores"], ncpus)

        # Create ROOT output file
        data_tier = step.get("data_tier", f"STEP{step_num}")
        output_size_kb = events_per_job * step.get(
            "output_size_per_event_kb", 50
        )
        output_filename = f"step{step_num}_{data_tier}_output.root"

        print(
            f"  Creating ROOT file: {output_filename} "
            f"(~{output_size_kb:.0f} KB)...",
            flush=True,
        )
        actual_size = create_root_file(
            output_filename, events_per_job, output_size_kb
        )
        print(f"  Created: {output_filename} ({actual_size} bytes)", flush=True)

        # Write FJR XML
        fjr_path = f"report_step{step_num}.xml"

        # GEN steps have no input
        is_gen = step_name.upper().startswith("GEN") or (
            step_idx == 0 and prev_output is None
        )
        input_for_fjr = None if is_gen else prev_output

        write_fjr_xml(
            fjr_path=fjr_path,
            step_config=step_with_ncpus,
            profile=profile,
            events=events_per_job,
            output_path=output_filename,
            input_path=input_for_fjr,
            step_num=step_num,
        )
        print(f"  Wrote FJR: {fjr_path}", flush=True)

        # Release memory
        del mem_buf

        # Track output for step chaining
        prev_output = output_filename

    print("\n=== Simulator complete ===", flush=True)
    return 0


def main():
    parser = argparse.ArgumentParser(description="WMS2 cmsRun Simulator")
    parser.add_argument(
        "--manifest", required=True, help="Path to manifest.json"
    )
    parser.add_argument(
        "--node-index", type=int, required=True, help="DAG node index"
    )
    parser.add_argument(
        "--events-per-job", type=int, required=True, help="Events per job"
    )
    parser.add_argument(
        "--first-event", type=int, default=0, help="First event number"
    )
    parser.add_argument(
        "--ncpus",
        type=int,
        default=0,
        help="Override ncpus (0 = use manifest value)",
    )
    args = parser.parse_args()

    return run_simulator(
        manifest_path=args.manifest,
        node_index=args.node_index,
        events_per_job=args.events_per_job,
        first_event=args.first_event,
        ncpus_override=args.ncpus,
    )


if __name__ == "__main__":
    exit(main())
