"""Sandbox builder: creates WMS2 sandbox tarballs with manifest + PSet configs."""

from __future__ import annotations

import json
import logging
import os
import tarfile
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def create_sandbox(output_path: str, request_data: dict[str, Any], mode: str = "auto") -> str:
    """Create a WMS2 sandbox tar.gz with manifest.json + PSet configs.

    mode = "auto": CMSSW if CMSSWVersion present, else synthetic
    mode = "synthetic": always synthetic
    mode = "cmssw": requires CMSSWVersion + PSet configs

    Returns path to created tar.gz.
    """
    if mode == "auto":
        mode = "cmssw" if request_data.get("CMSSWVersion") else "synthetic"

    manifest = _build_manifest(request_data, mode)

    # Build sandbox in a temp directory, then tar it
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Write manifest
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # For simulator mode, include simulator.py in sandbox
        if mode == "simulator":
            sim_src = Path(__file__).parent / "simulator.py"
            if sim_src.is_file():
                import shutil
                shutil.copy2(str(sim_src), str(tmp / "simulator.py"))
            else:
                logger.warning("simulator.py not found at %s", sim_src)

        # For CMSSW mode, generate test PSets per step
        if mode == "cmssw":
            # For StepChain manifests (new format), get global_tag from first step
            if "steps" in manifest and manifest["steps"]:
                first_step = manifest["steps"][0]
                global_tag = first_step.get("global_tag", manifest.get("global_tag", ""))
            else:
                global_tag = manifest.get("global_tag", "")
            for step in manifest.get("steps", []):
                pset_rel = step["pset"]
                pset_path = tmp / pset_rel
                pset_path.parent.mkdir(parents=True, exist_ok=True)
                step_gt = step.get("global_tag", global_tag)
                pset_path.write_text(_create_test_pset(step["name"], step_gt))

        # Create tar.gz
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with tarfile.open(output_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                arcname = str(item.relative_to(tmp))
                tar.add(str(item), arcname=arcname)

    logger.info("Created sandbox: %s (mode=%s)", output_path, mode)
    return output_path


def create_sandbox_from_spec(
    output_path: str,
    spec: Any,
    pset_paths: dict[str, str],
) -> str:
    """Create sandbox from a StepChainSpec + pre-generated PSets.

    pset_paths maps step name to a local file path containing the PSet.
    These are generated externally (from ConfigCache download or cmsDriver).

    Returns path to created tar.gz.
    """
    from wms2.core.stepchain import StepChainSpec, to_manifest

    if not isinstance(spec, StepChainSpec):
        raise TypeError(f"Expected StepChainSpec, got {type(spec).__name__}")

    manifest = to_manifest(spec)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Write manifest
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # Copy PSet files into sandbox at the expected paths
        for step in spec.steps:
            pset_rel = step.pset  # e.g. "steps/step1/cfg.py"
            pset_dest = tmp / pset_rel
            pset_dest.parent.mkdir(parents=True, exist_ok=True)

            if step.name in pset_paths:
                pset_src = Path(pset_paths[step.name])
                pset_dest.write_text(pset_src.read_text())
            else:
                # Generate a placeholder test PSet if no real one provided
                pset_dest.write_text(_create_test_pset(step.name, step.global_tag))

        # Create tar.gz
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with tarfile.open(output_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                arcname = str(item.relative_to(tmp))
                tar.add(str(item), arcname=arcname)

    logger.info("Created sandbox from spec: %s (%d steps)", output_path, len(spec.steps))
    return output_path


def _build_manifest(request_data: dict[str, Any], mode: str) -> dict[str, Any]:
    """Build manifest.json from ReqMgr2 request data."""
    if mode == "synthetic":
        return {
            "mode": "synthetic",
            "size_per_event_kb": float(request_data.get("SizePerEvent", 50.0)),
            "time_per_event_sec": float(request_data.get("TimePerEvent", 0.5)),
            "memory_mb": int(request_data.get("Memory", 2048)),
            "output_tiers": _extract_output_tiers(request_data),
        }

    if mode == "simulator":
        return {
            "mode": "simulator",
            "steps": _build_simulator_steps(request_data),
        }

    # CMSSW mode — use StepChain parser for StepChain requests
    if request_data.get("RequestType") == "StepChain" and request_data.get("StepChain"):
        from wms2.core.stepchain import parse_stepchain, to_manifest

        spec = parse_stepchain(request_data)
        return to_manifest(spec)

    # Legacy single-step CMSSW mode
    cmssw_version = request_data.get("CMSSWVersion", "")
    scram_arch = request_data.get("ScramArch", "")
    global_tag = request_data.get("GlobalTag", "")
    multicore = int(request_data.get("Multicore", 1))
    memory_mb = int(request_data.get("Memory", 2048))

    steps = _extract_steps(request_data)

    return {
        "mode": "cmssw",
        "cmssw_version": cmssw_version,
        "scram_arch": scram_arch,
        "global_tag": global_tag,
        "multicore": multicore,
        "memory_mb": memory_mb,
        "steps": steps,
    }


def _extract_steps(request_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract step definitions from a non-StepChain request."""
    steps: list[dict[str, Any]] = []

    # Single step (ReReco, MonteCarlo, etc.)
    steps.append({
        "name": "processing",
        "pset": "steps/step1/PSet.py",
        "output_modules": ["output"],
        "keep_output": True,
    })

    return steps


def _extract_output_tiers(request_data: dict[str, Any]) -> list[str]:
    """Extract output data tiers from request data."""
    tiers: list[str] = []
    output_datasets = request_data.get("OutputDatasets", [])
    for ds in output_datasets:
        # Dataset format: /Primary/Era-Proc-v1/TIER
        parts = ds.split("/")
        if len(parts) >= 4:
            tiers.append(parts[3])
    if not tiers:
        tiers = ["AODSIM"]
    return tiers


# Default physics-based resource profiles per step type
_SIMULATOR_PROFILES: dict[str, dict[str, float]] = {
    "GEN": {
        "serial_fraction": 0.30,
        "base_memory_mb": 1500,
        "marginal_per_thread_mb": 250,
        "wall_sec_per_event": 0.05,
        "output_size_per_event_kb": 50,
    },
    "DIGI": {
        "serial_fraction": 0.10,
        "base_memory_mb": 1800,
        "marginal_per_thread_mb": 300,
        "wall_sec_per_event": 0.03,
        "output_size_per_event_kb": 30,
    },
    "RECO": {
        "serial_fraction": 0.10,
        "base_memory_mb": 1800,
        "marginal_per_thread_mb": 300,
        "wall_sec_per_event": 0.03,
        "output_size_per_event_kb": 20,
    },
    "MINI": {
        "serial_fraction": 0.08,
        "base_memory_mb": 1600,
        "marginal_per_thread_mb": 200,
        "wall_sec_per_event": 0.02,
        "output_size_per_event_kb": 15,
    },
    "NANO": {
        "serial_fraction": 0.05,
        "base_memory_mb": 1200,
        "marginal_per_thread_mb": 150,
        "wall_sec_per_event": 0.01,
        "output_size_per_event_kb": 5,
    },
}


def _get_simulator_profile(step_name: str) -> dict[str, float]:
    """Get resource profile defaults for a step type.

    Matches by prefix: GEN-SIM → GEN, DIGI-RAW → DIGI, etc.
    Falls back to DIGI profile for unknown step types.
    """
    name_upper = step_name.upper()
    for prefix in ("GEN", "NANO", "MINI", "RECO", "DIGI"):
        if name_upper.startswith(prefix):
            return dict(_SIMULATOR_PROFILES[prefix])
    return dict(_SIMULATOR_PROFILES["DIGI"])


def _build_simulator_steps(request_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Build per-step simulator config with physics-based resource parameters.

    Extracts step names from StepChain request data, assigns default profiles
    based on step type, and sets output_module to data_tier + "output".
    """
    multicore = int(request_data.get("Multicore", 1))
    n_steps = int(request_data.get("StepChain", 1))

    steps: list[dict[str, Any]] = []
    for i in range(1, n_steps + 1):
        step_key = f"Step{i}"
        step_data = request_data.get(step_key, {})
        step_name = step_data.get("StepName", f"STEP{i}")

        profile = _get_simulator_profile(step_name)

        # Use step name as data tier (e.g. "GEN-SIM", "DIGI", "RECO")
        data_tier = step_name

        # output_module: data_tier + "output" for direct tier matching
        output_module = data_tier + "output"

        # Last step keeps output by default; intermediate steps don't
        keep_output = i == n_steps

        steps.append({
            "name": step_name,
            "serial_fraction": profile["serial_fraction"],
            "base_memory_mb": profile["base_memory_mb"],
            "marginal_per_thread_mb": profile["marginal_per_thread_mb"],
            "wall_sec_per_event": profile["wall_sec_per_event"],
            "output_size_per_event_kb": profile["output_size_per_event_kb"],
            "multicore": multicore,
            "output_module": output_module,
            "data_tier": data_tier,
            "keep_output": keep_output,
        })

    return steps


def _create_test_pset(step_name: str, global_tag: str) -> str:
    """Generate a minimal working PSet.py for testing.

    Reads input, applies global tag, writes output.
    For real production, PSets come from ConfigCache (future).
    """
    return f'''\
# Auto-generated test PSet for step: {step_name}
# Global tag: {global_tag}
import FWCore.ParameterSet.Config as cms

process = cms.Process("{step_name.upper()}")

process.source = cms.Source("PoolSource",
    fileNames=cms.untracked.vstring()
)

process.maxEvents = cms.untracked.PSet(
    input=cms.untracked.int32(-1)
)

process.load("Configuration.StandardSequences.FrontierConditions_GlobalTag_cff")
process.GlobalTag.globaltag = "{global_tag}"

process.output = cms.OutputModule("PoolOutputModule",
    fileName=cms.untracked.string("{step_name}_output.root"),
    outputCommands=cms.untracked.vstring("keep *")
)

process.out_step = cms.EndPath(process.output)
'''
