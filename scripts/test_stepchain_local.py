#!/usr/bin/env python3
"""Local test for StepChain processing: 4-step NPS chain.

Tests the full chain:
  GEN-SIM → DIGI+RECO → MiniAOD → NanoAOD

Usage:
    python scripts/test_stepchain_local.py [--events N] [--skip-pset-gen]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from wms2.core.pset_generator import generate_psets_from_mcm
from wms2.core.sandbox import create_sandbox_from_spec
from wms2.core.stepchain import StepChainSpec, StepSpec, to_manifest

# Real NPS chain prepids
CHAIN_PREPIDS = [
    "NPS-Run3Summer22EEGS-00049",        # GEN-SIM: CMSSW_12_4_25 / el8_amd64_gcc10
    "NPS-Run3Summer22EEDRPremix-00840",   # DIGI+RECO: CMSSW_12_4_25 / el8_amd64_gcc10
    "NPS-Run3Summer22EEMiniAODv4-00015",  # MiniAOD: CMSSW_13_0_23 / el8_amd64_gcc11
    "NPS-Run3Summer22EENanoAODv12-00015",  # NanoAOD: CMSSW_13_0_23 / el8_amd64_gcc11
]
DEFAULT_EVENTS = 10  # 10% of 100 nominal


def build_hardcoded_spec(events: int) -> StepChainSpec:
    """Build a hardcoded spec for the NPS chain (fallback if McM fetch fails)."""
    return StepChainSpec(
        request_name="NPS-Run3Summer22EEGS-00049",
        steps=[
            StepSpec(
                name="GEN-SIM",
                cmssw_version="CMSSW_12_4_25",
                scram_arch="el8_amd64_gcc10",
                global_tag="124X_mcRun3_2022_realistic_postEE_v1",
                pset="steps/step1/cfg.py",
                multicore=4,
                memory_mb=8000,
                keep_output=True,
                input_step="",
                input_from_output_module="",
                mc_pileup="",
                data_pileup="",
                event_streams=0,
                request_num_events=events,
            ),
            StepSpec(
                name="DIGI-RECO",
                cmssw_version="CMSSW_12_4_25",
                scram_arch="el8_amd64_gcc10",
                global_tag="124X_mcRun3_2022_realistic_postEE_v1",
                pset="steps/step2/cfg.py",
                multicore=4,
                memory_mb=8000,
                keep_output=True,
                input_step="GEN-SIM",
                input_from_output_module="RAWSIMoutput",
                mc_pileup="/MinBias_TuneCP5_13p6TeV-pythia8/Run3Summer22EEGS-124X_mcRun3_2022_realistic_postEE_v1-v1/GEN-SIM",
                data_pileup="",
                event_streams=0,
                request_num_events=0,
            ),
            StepSpec(
                name="MiniAOD",
                cmssw_version="CMSSW_13_0_23",
                scram_arch="el8_amd64_gcc11",
                global_tag="130X_mcRun3_2022_realistic_postEE_v6",
                pset="steps/step3/cfg.py",
                multicore=2,
                memory_mb=6000,
                keep_output=True,
                input_step="DIGI-RECO",
                input_from_output_module="AODSIMoutput",
                mc_pileup="",
                data_pileup="",
                event_streams=0,
                request_num_events=0,
            ),
            StepSpec(
                name="NanoAOD",
                cmssw_version="CMSSW_13_0_23",
                scram_arch="el8_amd64_gcc11",
                global_tag="130X_mcRun3_2022_realistic_postEE_v6",
                pset="steps/step4/cfg.py",
                multicore=2,
                memory_mb=4000,
                keep_output=True,
                input_step="MiniAOD",
                input_from_output_module="MINIAODSIMoutput",
                mc_pileup="",
                data_pileup="",
                event_streams=0,
                request_num_events=0,
            ),
        ],
        time_per_event=5.0,
        size_per_event=2.5,
        filter_efficiency=1.0,
    )


def run_processing(sandbox_path: str, events: int, work_dir: str) -> bool:
    """Run wms2_proc.sh with the sandbox and verify results."""
    # Copy the proc script from the DAG planner
    from wms2.core.dag_planner import _write_proc_script, _write_file
    proc_script = os.path.join(work_dir, "wms2_proc.sh")
    _write_proc_script(proc_script)

    cmd = [
        "bash", proc_script,
        "--sandbox", sandbox_path,
        "--events-per-job", str(events),
        "--node-index", "0",
    ]

    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"Working dir: {work_dir}")
    print(f"{'='*60}\n")

    result = subprocess.run(
        cmd,
        cwd=work_dir,
        timeout=3600,
    )

    return result.returncode == 0


def verify_outputs(work_dir: str, num_steps: int) -> bool:
    """Verify that all expected outputs exist."""
    work = Path(work_dir)
    success = True

    print(f"\n{'='*60}")
    print("Verification")
    print(f"{'='*60}")

    # Check FJR files
    for i in range(1, num_steps + 1):
        fjr = work / f"report_step{i}.xml"
        if fjr.exists():
            print(f"  [OK] report_step{i}.xml ({fjr.stat().st_size} bytes)")
        else:
            print(f"  [FAIL] report_step{i}.xml missing")
            success = False

    # Check for .root output files
    root_files = list(work.glob("*.root"))
    if root_files:
        for rf in root_files:
            print(f"  [OK] {rf.name} ({rf.stat().st_size} bytes)")
    else:
        print("  [WARN] No .root files found")

    return success


def main():
    parser = argparse.ArgumentParser(description="Test StepChain processing locally")
    parser.add_argument("--events", type=int, default=DEFAULT_EVENTS,
                        help=f"Events to process (default: {DEFAULT_EVENTS})")
    parser.add_argument("--skip-pset-gen", action="store_true",
                        help="Use hardcoded spec with placeholder PSets")
    parser.add_argument("--work-dir", type=str, default=None,
                        help="Working directory (default: temp dir)")
    args = parser.parse_args()

    print(f"StepChain Local Test")
    print(f"  Events: {args.events}")
    print(f"  Skip PSet generation: {args.skip_pset_gen}")
    print()

    # Set up working directory
    if args.work_dir:
        work_dir = args.work_dir
        os.makedirs(work_dir, exist_ok=True)
    else:
        work_dir = tempfile.mkdtemp(prefix="wms2_stepchain_test_")

    print(f"Working directory: {work_dir}")

    # Step 1: Get spec + PSets
    pset_paths: dict[str, str] = {}
    if args.skip_pset_gen:
        print("\nUsing hardcoded spec with placeholder PSets")
        spec = build_hardcoded_spec(args.events)
    else:
        print(f"\nFetching McM setups for {len(CHAIN_PREPIDS)} prepids...")
        try:
            pset_dir = Path(work_dir) / "psets"
            spec, pset_path_objs = generate_psets_from_mcm(
                CHAIN_PREPIDS, pset_dir, events=args.events
            )
            pset_paths = {k: str(v) for k, v in pset_path_objs.items()}
            print(f"  Generated {len(pset_paths)} PSets")
        except Exception as e:
            print(f"  McM fetch failed: {e}")
            print("  Falling back to hardcoded spec")
            spec = build_hardcoded_spec(args.events)

    # Step 2: Create sandbox
    sandbox_path = os.path.join(work_dir, "sandbox.tar.gz")
    print(f"\nCreating sandbox: {sandbox_path}")
    create_sandbox_from_spec(sandbox_path, spec, pset_paths)
    print(f"  Sandbox size: {os.path.getsize(sandbox_path)} bytes")

    # Step 3: Write manifest for reference
    manifest = to_manifest(spec)
    manifest_path = os.path.join(work_dir, "manifest_reference.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Manifest written: {manifest_path}")

    # Step 4: Run processing
    run_dir = os.path.join(work_dir, "run")
    os.makedirs(run_dir, exist_ok=True)
    success = run_processing(sandbox_path, args.events, run_dir)

    if not success:
        print("\n[FAIL] Processing failed!")
        sys.exit(1)

    # Step 5: Verify
    ok = verify_outputs(run_dir, len(spec.steps))

    if ok:
        print(f"\n[OK] All verifications passed!")
    else:
        print(f"\n[WARN] Some verifications failed")

    print(f"\nOutputs in: {run_dir}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
