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

        # For CMSSW mode, generate test PSets per step
        if mode == "cmssw":
            global_tag = manifest.get("global_tag", "")
            for step in manifest.get("steps", []):
                pset_rel = step["pset"]
                pset_path = tmp / pset_rel
                pset_path.parent.mkdir(parents=True, exist_ok=True)
                pset_path.write_text(_create_test_pset(step["name"], global_tag))

        # Create tar.gz
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with tarfile.open(output_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                arcname = str(item.relative_to(tmp))
                tar.add(str(item), arcname=arcname)

    logger.info("Created sandbox: %s (mode=%s)", output_path, mode)
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

    # CMSSW mode
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
    """Extract step definitions from a StepChain/TaskChain request."""
    steps: list[dict[str, Any]] = []
    request_type = request_data.get("RequestType", "")

    if request_type == "StepChain":
        num_steps = int(request_data.get("StepChain", 1))
        for i in range(1, num_steps + 1):
            step_data = request_data.get(f"Step{i}", {})
            step_name = step_data.get("StepName", f"step{i}")
            output_modules = step_data.get("KeepOutput", True)

            step_info: dict[str, Any] = {
                "name": step_name,
                "pset": f"steps/step{i}/PSet.py",
                "output_modules": [f"{step_name}output"],
                "keep_output": bool(output_modules) if isinstance(output_modules, bool) else True,
            }

            # Chain steps: step2+ gets input from previous step
            if i > 1:
                prev_step = request_data.get(f"Step{i-1}", {})
                prev_name = prev_step.get("StepName", f"step{i-1}")
                step_info["input_from_step"] = prev_name
                step_info["output_from_module"] = f"{prev_name}output"

            steps.append(step_info)
    else:
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
