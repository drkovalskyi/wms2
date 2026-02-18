#!/usr/bin/env python3
"""Run a real CMSSW StepChain locally (no HTCondor).

Fetches ConfigCache PSets from CouchDB, builds a sandbox with the real
manifest + PSets, then executes wms2_proc.sh in the working directory.

Usage:
    .venv/bin/python scripts/run_real_local.py \
        --request cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54 \
        --events 1000
"""
from __future__ import annotations

import argparse
import json
import os
import ssl
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from wms2.core.dag_planner import _write_proc_script

# ── Configuration ────────────────────────────────────────────
REQMGR2_URL = "https://cmsweb.cern.ch/reqmgr2/data/request"
COUCHDB_URL = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache"
X509_PROXY = os.environ.get("X509_USER_PROXY", "/mnt/creds/x509up")


def make_ssl_context() -> ssl.SSLContext:
    """Create SSL context with X509 proxy for CMS services."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_cert_chain(X509_PROXY, X509_PROXY)
    ca_dir = os.environ.get(
        "X509_CERT_DIR",
        "/cvmfs/grid.cern.ch/etc/grid-security/certificates",
    )
    if os.path.isdir(ca_dir):
        ctx.load_verify_locations(capath=ca_dir)
    else:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch_request(request_name: str) -> dict:
    """Fetch full request spec from ReqMgr2."""
    url = f"{REQMGR2_URL}/{request_name}"
    print(f"  Fetching request from ReqMgr2...")
    ctx = make_ssl_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    return list(data["result"][0].values())[0]


def fetch_pset(config_cache_id: str) -> str:
    """Fetch PSet config from CouchDB ConfigCache."""
    url = f"{COUCHDB_URL}/{config_cache_id}/configFile"
    ctx = make_ssl_context()
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return resp.read().decode()


def build_manifest(request: dict) -> dict:
    """Build manifest.json from the ReqMgr2 request."""
    num_steps = int(request.get("StepChain", 1))
    steps = []

    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        # ScramArch may be a list
        arch = step.get("ScramArch") or request.get("ScramArch", "")
        if isinstance(arch, list):
            arch = arch[0]

        steps.append({
            "name": step.get("StepName", f"step{i}"),
            "cmssw_version": step.get("CMSSWVersion") or request.get("CMSSWVersion", ""),
            "scram_arch": arch,
            "global_tag": step.get("GlobalTag") or request.get("GlobalTag", ""),
            "pset": f"steps/step{i}/cfg.py",
            "multicore": int(step.get("Multicore") or request.get("Multicore", 1)),
            "memory_mb": int(step.get("Memory") or request.get("Memory", 2048)),
            "keep_output": step.get("KeepOutput", True),
            "input_step": step.get("InputStep", ""),
            "input_from_output_module": step.get("InputFromOutputModule", ""),
            "mc_pileup": step.get("MCPileup", ""),
            "data_pileup": step.get("DataPileup", ""),
            "event_streams": int(step.get("EventStreams") or request.get("EventStreams", 0)),
            "request_num_events": int(step.get("RequestNumEvents") or 0),
        })

    return {
        "mode": "cmssw",
        "request_name": request.get("RequestName", ""),
        "steps": steps,
    }


def build_sandbox(
    output_path: str,
    manifest: dict,
    psets: dict[int, str],
) -> None:
    """Create sandbox tar.gz with manifest + PSets."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)

        # Write manifest
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # Write PSets
        for step_idx, content in psets.items():
            pset_dir = tmp / "steps" / f"step{step_idx}"
            pset_dir.mkdir(parents=True, exist_ok=True)
            (pset_dir / "cfg.py").write_text(content)

        # Create tar.gz
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with tarfile.open(output_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                arcname = str(item.relative_to(tmp))
                tar.add(str(item), arcname=arcname)

    print(f"  Sandbox: {output_path} ({os.path.getsize(output_path)} bytes)")


def run_proc_script(
    sandbox_path: str,
    events: int,
    work_dir: str,
    nthreads: int,
) -> int:
    """Run wms2_proc.sh locally."""
    # Generate the proc script
    proc_script = os.path.join(work_dir, "wms2_proc.sh")
    _write_proc_script(proc_script)

    cmd = [
        "bash", proc_script,
        "--sandbox", sandbox_path,
        "--events-per-job", str(events),
        "--node-index", "0",
    ]

    print(f"\n{'='*70}")
    print(f"Running: {' '.join(cmd)}")
    print(f"Working dir: {work_dir}")
    print(f"nThreads: {nthreads} (set per-step in manifest)")
    print(f"{'='*70}\n")

    env = os.environ.copy()
    # Ensure CMS env vars are set
    env.setdefault("SITECONFIG_PATH", "/opt/cms/siteconf")
    env.setdefault("X509_USER_PROXY", X509_PROXY)
    env.setdefault(
        "X509_CERT_DIR",
        "/cvmfs/grid.cern.ch/etc/grid-security/certificates",
    )

    result = subprocess.run(cmd, cwd=work_dir, env=env)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="Run real CMSSW StepChain locally")
    parser.add_argument("--request", required=True, help="ReqMgr2 request name")
    parser.add_argument("--events", type=int, default=1000, help="Events to process")
    parser.add_argument("--work-dir", type=str, default=None, help="Working directory")
    args = parser.parse_args()

    print(f"{'='*70}")
    print(f"WMS2 Real CMSSW Local Run")
    print(f"  Request: {args.request}")
    print(f"  Events:  {args.events}")
    print(f"{'='*70}")

    # 1. Fetch request
    print(f"\n[1/4] Fetching request spec...")
    request = fetch_request(args.request)
    num_steps = int(request.get("StepChain", 1))
    multicore = int(request.get("Multicore", 1))
    print(f"  StepChain: {num_steps} steps")
    print(f"  Multicore: {multicore}")
    print(f"  TimePerEvent: {request.get('TimePerEvent', '?')} sec")
    print(f"  Memory: {request.get('Memory', '?')} MB")

    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        arch = step.get("ScramArch", request.get("ScramArch", "?"))
        if isinstance(arch, list):
            arch = arch[0]
        print(f"  Step{i}: {step.get('StepName', '?')} "
              f"({step.get('CMSSWVersion', '?')}/{arch}) "
              f"ConfigCache={step.get('ConfigCacheID', '?')[:12]}...")

    # 2. Fetch PSets from ConfigCache
    print(f"\n[2/4] Fetching {num_steps} PSets from ConfigCache...")
    psets: dict[int, str] = {}
    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        ccid = step.get("ConfigCacheID", "")
        if not ccid:
            print(f"  Step{i}: no ConfigCacheID, using placeholder")
            psets[i] = f"# Placeholder PSet for step {i}\n"
            continue
        pset_content = fetch_pset(ccid)
        psets[i] = pset_content
        print(f"  Step{i}: {len(pset_content)} chars ({ccid[:12]}...)")

    # 3. Build sandbox
    print(f"\n[3/4] Building sandbox...")
    manifest = build_manifest(request)

    if args.work_dir:
        base_dir = Path(args.work_dir)
    else:
        base_dir = Path("/mnt/shared/work/wms2_real_test")
    base_dir.mkdir(parents=True, exist_ok=True)

    sandbox_path = str(base_dir / "sandbox.tar.gz")
    build_sandbox(sandbox_path, manifest, psets)

    # Save manifest for reference
    manifest_ref = base_dir / "manifest_reference.json"
    manifest_ref.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest reference: {manifest_ref}")

    # Save request for reference
    request_ref = base_dir / "request.json"
    request_ref.write_text(json.dumps(request, indent=2))

    # 4. Run
    print(f"\n[4/4] Running {num_steps}-step chain with {args.events} events...")
    run_dir = str(base_dir / "run")
    os.makedirs(run_dir, exist_ok=True)

    start_time = time.time()
    rc = run_proc_script(sandbox_path, args.events, run_dir, multicore)
    elapsed = time.time() - start_time

    # Results
    print(f"\n{'='*70}")
    print(f"Exit code: {rc}")
    print(f"Wall time: {elapsed:.1f}s ({elapsed/60:.1f} min)")

    # Check outputs
    run_path = Path(run_dir)
    root_files = list(run_path.glob("*.root"))
    if root_files:
        print(f"\nOutput files:")
        for f in sorted(root_files):
            print(f"  {f.name}: {f.stat().st_size:,} bytes")

    fjr_files = sorted(run_path.glob("report_step*.xml"))
    if fjr_files:
        print(f"\nFrameworkJobReports:")
        for f in fjr_files:
            print(f"  {f.name}: {f.stat().st_size:,} bytes")

    print(f"{'='*70}")

    return rc


if __name__ == "__main__":
    sys.exit(main())
