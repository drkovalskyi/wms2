"""PSet generator: fetches McM setups and generates PSets via cmsDriver.

This is a development/testing utility. In production, PSets come from ConfigCache.
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from wms2.core.stepchain import StepChainSpec, StepSpec

logger = logging.getLogger(__name__)

MCM_PUBLIC_URL = "https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_setup"


def parse_mcm_setup(setup_script: str) -> dict[str, Any]:
    """Parse a McM setup script to extract execution parameters.

    Returns dict with: scram_arch, cmssw_version, cmsdriver_commands (list),
    fragment_url (if any).
    """
    result: dict[str, Any] = {
        "scram_arch": "",
        "cmssw_version": "",
        "cmsdriver_commands": [],
        "fragment_url": "",
    }

    for line in setup_script.splitlines():
        line = line.strip()

        # SCRAM_ARCH
        m = re.match(r'export\s+SCRAM_ARCH=(\S+)', line)
        if m:
            result["scram_arch"] = m.group(1)
            continue

        # CMSSW version from scram project
        m = re.match(r'.*scram\s+p(?:roject)?\s+(?:CMSSW\s+)?(\S+)', line)
        if m and line.startswith("scram") or "scram p" in line:
            val = m.group(1)
            if val.startswith("CMSSW_"):
                result["cmssw_version"] = val
            continue

        # cmsDriver.py command
        if "cmsDriver.py" in line and not line.startswith("#"):
            result["cmsdriver_commands"].append(line)
            continue

        # Fragment URL (curl download)
        m = re.match(r'curl\s.*-o\s+(\S+)\s.*?(https?://\S+)', line)
        if not m:
            m = re.match(r'curl\s.*?(https?://\S+).*-o\s+(\S+)', line)
        if m:
            result["fragment_url"] = m.group(2) if "http" in (m.group(2) or "") else m.group(1)

    return result


def fetch_mcm_setup(prepid: str) -> str:
    """Fetch setup script from McM public API for a given prepid.

    Returns the setup script as a string.
    """
    import urllib.request

    url = f"{MCM_PUBLIC_URL}/{prepid}"
    logger.info("Fetching McM setup for %s", prepid)

    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())

    # McM returns {"results": "script content"} or similar
    if isinstance(data, dict):
        return data.get("results", "")
    return str(data)


def generate_psets_from_mcm(
    mcm_prepids: list[str],
    output_dir: Path,
    events: int = 10,
) -> tuple[StepChainSpec, dict[str, Path]]:
    """Fetch McM setups, generate PSets, return spec + PSet paths.

    For each prepid:
    1. Fetch setup script from McM public API
    2. Parse to extract cmsDriver command(s)
    3. Run cmsDriver.py inside appropriate apptainer container
    4. Return generated PSet file path

    Returns (StepChainSpec, dict mapping step name -> PSet path).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    steps: list[StepSpec] = []
    pset_paths: dict[str, Path] = {}

    for i, prepid in enumerate(mcm_prepids, 1):
        step_dir = output_dir / f"step{i}"
        step_dir.mkdir(parents=True, exist_ok=True)

        # Fetch and parse McM setup
        setup_script = fetch_mcm_setup(prepid)
        parsed = parse_mcm_setup(setup_script)

        cmssw_version = parsed["cmssw_version"]
        scram_arch = parsed["scram_arch"]
        step_name = f"step{i}"

        logger.info(
            "Step %d (%s): CMSSW=%s, Arch=%s, %d cmsDriver commands",
            i, prepid, cmssw_version, scram_arch, len(parsed["cmsdriver_commands"]),
        )

        # Generate PSet via cmsDriver
        if parsed["cmsdriver_commands"]:
            pset_path = _run_cmsdriver(
                cmsdriver_cmd=parsed["cmsdriver_commands"][0],
                cmssw_version=cmssw_version,
                scram_arch=scram_arch,
                work_dir=step_dir,
                events=events,
                fragment_url=parsed.get("fragment_url", ""),
            )
        else:
            logger.warning("No cmsDriver command found for %s, generating placeholder", prepid)
            pset_path = step_dir / "cfg.py"
            pset_path.write_text(
                f"# Placeholder PSet for {prepid}\n"
                f"# No cmsDriver command found in McM setup\n"
            )

        pset_paths[step_name] = pset_path

        # Build StepSpec
        input_step = steps[-1].name if steps else ""
        steps.append(StepSpec(
            name=step_name,
            cmssw_version=cmssw_version,
            scram_arch=scram_arch,
            global_tag=_extract_global_tag(parsed["cmsdriver_commands"][0] if parsed["cmsdriver_commands"] else ""),
            pset=f"steps/step{i}/cfg.py",
            multicore=_extract_nthreads(parsed["cmsdriver_commands"][0] if parsed["cmsdriver_commands"] else ""),
            memory_mb=4000,
            keep_output=True,
            input_step=input_step,
            input_from_output_module="",
            mc_pileup="",
            data_pileup="",
            event_streams=0,
            request_num_events=events if i == 1 else 0,
        ))

    spec = StepChainSpec(
        request_name=mcm_prepids[0] if mcm_prepids else "",
        steps=steps,
        time_per_event=1.0,
        size_per_event=1.5,
        filter_efficiency=1.0,
    )

    return spec, pset_paths


def _run_cmsdriver(
    cmsdriver_cmd: str,
    cmssw_version: str,
    scram_arch: str,
    work_dir: Path,
    events: int,
    fragment_url: str = "",
) -> Path:
    """Run cmsDriver.py to generate a PSet, using apptainer if needed.

    Returns path to the generated PSet file.
    """
    # Modify cmsDriver command to only generate the PSet (--no_exec)
    cmd = cmsdriver_cmd.strip()
    if "--no_exec" not in cmd:
        cmd += " --no_exec"

    # Override event count
    cmd = re.sub(r'-n\s+\d+', f'-n {events}', cmd)
    if f'-n {events}' not in cmd and '-n ' not in cmd:
        cmd += f" -n {events}"

    # Build the full script
    script_lines = [
        "#!/bin/bash",
        "set -e",
        f"export SCRAM_ARCH={scram_arch}",
        "source /cvmfs/cms.cern.ch/cmsset_default.sh",
        f"if [[ ! -d {cmssw_version}/src ]]; then",
        f"    scramv1 project CMSSW {cmssw_version}",
        "fi",
        f"cd {cmssw_version}/src",
        "eval $(scramv1 runtime -sh)",
    ]

    # Download fragment if needed
    if fragment_url:
        script_lines.extend([
            "mkdir -p Configuration/GenProduction/python/",
            f"curl -s -L {fragment_url} -o Configuration/GenProduction/python/fragment.py",
            "scram b",
        ])

    script_lines.extend([
        f"cd {work_dir}",
        cmd,
    ])

    script_content = "\n".join(script_lines) + "\n"
    script_path = work_dir / "_generate_pset.sh"
    script_path.write_text(script_content)
    script_path.chmod(0o755)

    # Determine if we need apptainer
    host_os = _detect_host_os()
    arch_os = scram_arch.split("_")[0] if scram_arch else "unknown"

    if arch_os != host_os and os.path.isdir("/cvmfs/unpacked.cern.ch"):
        _run_in_apptainer(script_path, arch_os, work_dir)
    else:
        subprocess.run(
            ["bash", str(script_path)],
            cwd=str(work_dir),
            check=True,
            timeout=600,
        )

    # Find generated PSet file
    for candidate in work_dir.glob("*_cfg.py"):
        return candidate
    for candidate in work_dir.glob("*.py"):
        if candidate.name != "_generate_pset.sh":
            return candidate

    # Fallback: create placeholder
    pset_path = work_dir / "cfg.py"
    if not pset_path.exists():
        pset_path.write_text(f"# cmsDriver output not found\n# Command: {cmd}\n")
    return pset_path


def _run_in_apptainer(script_path: Path, arch_os: str, work_dir: Path) -> None:
    """Run a script inside an apptainer container."""
    container = None
    for variant in ["x86_64", "amd64"]:
        img = f"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/cmssw/{arch_os}:{variant}"
        if os.path.isdir(img):
            container = img
            break

    if not container:
        raise RuntimeError(f"No apptainer container found for {arch_os}")

    env = os.environ.copy()
    env["APPTAINER_BINDPATH"] = f"/cvmfs,{work_dir}"

    subprocess.run(
        ["apptainer", "exec", "--no-home", container, "bash", str(script_path)],
        cwd=str(work_dir),
        check=True,
        timeout=600,
        env=env,
    )


def _detect_host_os() -> str:
    """Detect host OS version."""
    try:
        with open("/etc/os-release") as f:
            for line in f:
                if line.startswith("VERSION_ID="):
                    ver = line.split("=", 1)[1].strip().strip('"')
                    if ver.startswith("8"):
                        return "el8"
                    if ver.startswith("9"):
                        return "el9"
    except FileNotFoundError:
        pass
    return "unknown"


def _extract_global_tag(cmsdriver_cmd: str) -> str:
    """Extract global tag from cmsDriver command."""
    m = re.search(r'--conditions\s+(\S+)', cmsdriver_cmd)
    return m.group(1) if m else ""


def _extract_nthreads(cmsdriver_cmd: str) -> int:
    """Extract nThreads from cmsDriver command."""
    m = re.search(r'--nThreads\s+(\d+)', cmsdriver_cmd)
    return int(m.group(1)) if m else 1
