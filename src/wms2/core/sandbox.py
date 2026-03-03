"""Sandbox builder: creates WMS2 sandbox tarballs with manifest + PSet configs."""

from __future__ import annotations

import json
import logging
import os
import ssl
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def create_sandbox(
    output_path: str,
    request_data: dict[str, Any],
    mode: str = "synthetic",
    ssl_context: ssl.SSLContext | None = None,
    configcache_url: str | None = None,
) -> str:
    """Create a WMS2 sandbox tar.gz with manifest.json + PSet configs.

    mode = "synthetic": always synthetic (sized output, no CMSSW)
    mode = "cmssw": requires CMSSWVersion + PSet configs (real cmsRun)
    mode = "simulator": realistic artifacts without real physics

    If ssl_context and configcache_url are provided, CMSSW-mode steps with a
    config_cache_id will fetch real PSets from ConfigCache instead of generating
    test stubs.

    Returns path to created tar.gz.
    """

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

        # For CMSSW mode, fetch real PSets from ConfigCache (required)
        if mode == "cmssw":
            if not ssl_context or not configcache_url:
                raise RuntimeError(
                    "CMSSW sandbox mode requires SSL credentials and ConfigCache URL. "
                    "Set WMS2_CERT_FILE/WMS2_KEY_FILE environment variables or use "
                    "--sandbox-mode simulator for testing without credentials."
                )
            for step in manifest.get("steps", []):
                pset_rel = step["pset"]
                pset_path = tmp / pset_rel
                pset_path.parent.mkdir(parents=True, exist_ok=True)

                cc_id = step.get("config_cache_id", "")
                if not cc_id:
                    raise RuntimeError(
                        f"Step {step['name']} has no config_cache_id — "
                        f"cannot fetch PSet from ConfigCache"
                    )
                pset_content = _fetch_configcache_pset(
                    configcache_url, cc_id, ssl_context, step["name"]
                )
                if pset_content is None:
                    raise RuntimeError(
                        f"Failed to fetch PSet from ConfigCache for step "
                        f"{step['name']} (config_cache_id={cc_id})"
                    )
                pset_path.write_text(pset_content)

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
    from wms2.core.stepchain import _normalize_arch

    cmssw_version = request_data.get("CMSSWVersion", "")
    scram_arch = _normalize_arch(request_data.get("ScramArch", ""))
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
    based on step type, and maps data tiers from OutputDatasets so that the
    simulator FJR ModuleLabels match the real data tiers (AODSIM, etc.).
    """
    multicore = int(request_data.get("Multicore", 1))
    n_steps = int(request_data.get("StepChain", 1))

    # Extract data tiers from OutputDatasets (last path component)
    # OutputDatasets entries correspond 1:1 with kept steps, in order.
    output_tiers: list[str] = []
    for ds in request_data.get("OutputDatasets", []):
        parts = ds.strip("/").split("/")
        if len(parts) >= 3:
            output_tiers.append(parts[-1])  # e.g. "AODSIM"

    # First pass: collect steps and mark which ones keep output
    raw_steps: list[tuple[str, dict, dict, bool]] = []
    for i in range(1, n_steps + 1):
        step_data = request_data.get(f"Step{i}", {})
        step_name = step_data.get("StepName", f"STEP{i}")
        profile = _get_simulator_profile(step_name)
        keep_output = step_data.get("KeepOutput", i == n_steps)
        raw_steps.append((step_name, step_data, profile, keep_output))

    # Map kept steps to OutputDatasets tiers
    tier_idx = 0
    steps: list[dict[str, Any]] = []
    for step_name, step_data, profile, keep_output in raw_steps:
        if keep_output and tier_idx < len(output_tiers):
            data_tier = output_tiers[tier_idx]
            tier_idx += 1
        else:
            # Non-kept step or no OutputDatasets: use step name as fallback
            data_tier = step_name

        output_module = data_tier + "output"

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


def _fetch_configcache_pset(
    configcache_url: str,
    config_cache_id: str,
    ssl_context: ssl.SSLContext,
    step_name: str,
) -> str | None:
    """Fetch a PSet from CMS ConfigCache (CouchDB).

    Returns the PSet source text, or None on failure.
    """
    url = f"{configcache_url}/{config_cache_id}/configFile"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ssl_context, timeout=30) as resp:
            content = resp.read().decode("utf-8")
        logger.info("Fetched PSet from ConfigCache for step %s (%s)", step_name, config_cache_id)
        return content
    except Exception as exc:
        logger.error(
            "Failed to fetch PSet from ConfigCache for step %s (%s): %s",
            step_name, config_cache_id, exc,
        )
        return None


def _create_test_pset(step_name: str, global_tag: str, has_input: bool = True) -> str:
    """Generate a minimal working PSet.py for testing (simulator/unit tests only).

    For GEN steps (has_input=False): uses EmptySource so cmsRun generates events.
    For later steps (has_input=True): uses PoolSource; fileNames injected at runtime.

    NOT used for real CMSSW production — those PSets come from ConfigCache.
    """
    import re
    # CMSSW Process name must be alphanumeric only (no hyphens, dots, etc.)
    proc_name = re.sub(r"[^A-Za-z0-9]", "", step_name.upper())
    if not proc_name:
        proc_name = "PROCESS"

    if has_input:
        source_block = '''\
process.source = cms.Source("PoolSource",
    fileNames=cms.untracked.vstring()
)'''
    else:
        # GEN step: EmptySource generates events from nothing.
        # firstEvent, firstLuminosityBlock, and maxEvents are injected at runtime.
        source_block = '''\
process.source = cms.Source("EmptySource",
    firstRun=cms.untracked.uint32(1),
    firstLuminosityBlock=cms.untracked.uint32(1),
    firstEvent=cms.untracked.uint32(1),
    numberEventsInLuminosityBlock=cms.untracked.uint32(100)
)'''

    return f'''\
# Auto-generated test PSet for step: {step_name}
# Global tag: {global_tag}
import FWCore.ParameterSet.Config as cms

process = cms.Process("{proc_name}")

{source_block}

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
