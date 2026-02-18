#!/usr/bin/env python3
"""E2E real CMSSW test via HTCondor DAGMan.

Fetches a real ReqMgr2 request, builds a sandbox with ConfigCache PSets,
plans a production DAG, and submits to HTCondor DAGMan.

Must run as the wms2 Linux user (HTCondor forbids root).

Usage:
    su - wms2 -c "/mnt/shared/work/wms2/.venv/bin/python \
        /mnt/shared/work/wms2/scripts/e2e_real_condor.py \
        --request <reqmgr2_request_name> --events 1000"
"""
from __future__ import annotations

import asyncio
import json
import os
import ssl
import shutil
import sys
import time
import urllib.request
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Ensure project is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wms2.adapters.condor import HTCondorAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner, PilotMetrics

# ── Configuration ──────────────────────────────────────────────

CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
POLL_INTERVAL = 10
MAX_WAIT = 7200  # 2 hours
X509_PROXY = os.environ.get("X509_USER_PROXY", "/mnt/creds/x509up")

REQMGR2_URL = "https://cmsweb.cern.ch/reqmgr2/data/request"
COUCHDB_URL = "https://cmsweb.cern.ch/couchdb/reqmgr_config_cache"


def make_ssl_context() -> ssl.SSLContext:
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


def fetch_json(url: str) -> dict:
    ctx = make_ssl_context()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return json.loads(resp.read().decode())


def fetch_text(url: str) -> str:
    ctx = make_ssl_context()
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
        return resp.read().decode()


def fetch_request(request_name: str) -> dict:
    data = fetch_json(f"{REQMGR2_URL}/{request_name}")
    return list(data["result"][0].values())[0]


def fetch_pset(config_cache_id: str) -> str:
    return fetch_text(f"{COUCHDB_URL}/{config_cache_id}/configFile")


def build_sandbox(test_dir: Path, request: dict) -> str:
    """Build sandbox with real ConfigCache PSets."""
    import tarfile
    import tempfile

    num_steps = int(request.get("StepChain", 1))

    # Build manifest
    steps = []
    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
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

    manifest = {"mode": "cmssw", "request_name": request.get("RequestName", ""), "steps": steps}

    # Fetch PSets
    psets: dict[int, str] = {}
    for i in range(1, num_steps + 1):
        ccid = request[f"Step{i}"].get("ConfigCacheID", "")
        if ccid:
            psets[i] = fetch_pset(ccid)
            print(f"    Step{i} PSet: {len(psets[i])} chars")
        else:
            psets[i] = f"# Placeholder for step {i}\n"

    # Build tar.gz
    sandbox_path = str(test_dir / "sandbox.tar.gz")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        (tmp / "manifest.json").write_text(json.dumps(manifest, indent=2))
        for step_idx, content in psets.items():
            pdir = tmp / "steps" / f"step{step_idx}"
            pdir.mkdir(parents=True, exist_ok=True)
            (pdir / "cfg.py").write_text(content)

        with tarfile.open(sandbox_path, "w:gz") as tar:
            for item in tmp.rglob("*"):
                tar.add(str(item), arcname=str(item.relative_to(tmp)))

    print(f"    Sandbox: {sandbox_path} ({os.path.getsize(sandbox_path):,} bytes)")
    return sandbox_path


def make_workflow(request: dict, sandbox_path: str, events_per_job: int,
                  num_jobs: int = 1) -> MagicMock:
    """Create a mock workflow from the real request."""
    total_events = events_per_job * num_jobs
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = request.get("RequestName", "real-test")
    wf.input_dataset = ""  # GEN has no input dataset
    wf.splitting_algo = "EventBased"
    wf.splitting_params = {"events_per_job": events_per_job}
    wf.sandbox_url = sandbox_path
    wf.category_throttles = {"Processing": 100, "Merge": 10, "Cleanup": 10}

    multicore = int(request.get("Multicore", 8))
    memory_mb = int(request.get("Memory", 16000))

    # Find the kept output datasets
    output_datasets = []
    num_steps = int(request.get("StepChain", 1))
    for i in range(1, num_steps + 1):
        step = request[f"Step{i}"]
        if step.get("KeepOutput", True):
            prim = step.get("PrimaryDataset", "TestPrimary")
            acq = step.get("AcquisitionEra", "Test")
            proc = step.get("ProcessingString", "v1")
            ver = step.get("ProcessingVersion", 1)
            output_datasets.append({
                "dataset_name": f"/{prim}/{acq}-{proc}-v{ver}/STEP{i}",
                "merged_lfn_base": f"/store/mc/{acq}/{prim}/STEP{i}/{proc}-v{ver}",
                "data_tier": f"STEP{i}",
            })

    wf.config_data = {
        "sandbox_path": sandbox_path,
        "memory_mb": memory_mb,
        "multicore": multicore,
        "time_per_event": float(request.get("TimePerEvent", 1.0)),
        "size_per_event": float(request.get("SizePerEvent", 50.0)),
        "request_num_events": total_events,
        "_is_gen": True,  # tells planner to use GEN node planning
        "output_datasets": output_datasets,
    }
    return wf


def make_mock_repo():
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


async def run_test(request_name: str, events: int, num_jobs: int = 1):
    total_events = events * num_jobs
    print("=" * 70)
    print(f"WMS2 Real CMSSW HTCondor DAG Test")
    print(f"  Request: {request_name}")
    print(f"  Events:  {events}/job x {num_jobs} jobs = {total_events} total")
    print(f"  Condor:  {CONDOR_HOST}")
    print("=" * 70)

    test_dir = Path("/mnt/shared/work/wms2_real_condor_test")
    if test_dir.exists():
        for item in test_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    else:
        test_dir.mkdir(parents=True)

    output_dir = test_dir / "output"
    output_dir.mkdir()

    # 1. Fetch request
    print(f"\n[1/6] Fetching request from ReqMgr2...")
    request = fetch_request(request_name)
    num_steps = int(request.get("StepChain", 1))
    multicore = int(request.get("Multicore", 8))
    memory_mb = int(request.get("Memory", 16000))
    print(f"    {num_steps} steps, {multicore} cores, {memory_mb} MB")

    # 2. Build sandbox
    print(f"[2/6] Building sandbox with {num_steps} ConfigCache PSets...")
    sandbox_path = build_sandbox(test_dir, request)

    # 3. Connect to HTCondor
    print(f"[3/6] Connecting to HTCondor...")
    try:
        condor = HTCondorAdapter(CONDOR_HOST)
        print(f"    Connected to schedd")
    except Exception as e:
        print(f"    ERROR: {e}")
        return False

    submit_dir = str(test_dir / "submit")
    settings = Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        submit_base_dir=submit_dir,
        jobs_per_work_unit=max(num_jobs, 8),
        processing_executable="/bin/true",
        merge_executable="/bin/true",
        cleanup_executable="/bin/true",
        output_base_dir=str(output_dir),
        max_input_files=num_jobs,
    )

    repo = make_mock_repo()
    planner = DAGPlanner(
        repository=repo,
        dbs_adapter=MagicMock(),
        rucio_adapter=MagicMock(),
        condor_adapter=condor,
        settings=settings,
    )

    # 4. Plan and submit DAG
    print(f"[4/6] Planning and submitting production DAG...")
    wf = make_workflow(request, sandbox_path, events, num_jobs)
    metrics = PilotMetrics.from_request(wf.config_data)

    try:
        dag = await planner.plan_production_dag(wf, metrics=metrics)
    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Extract cluster info
    cluster_id = None
    schedd_name = None
    for call in repo.update_dag.call_args_list:
        kw = call[1]
        if "dagman_cluster_id" in kw:
            cluster_id = kw["dagman_cluster_id"]
            schedd_name = kw["schedd_name"]

    if not cluster_id:
        print("    ERROR: No cluster ID")
        return False

    wf_dir = Path(submit_dir) / str(wf.id)
    print(f"    Workflow ID: {wf.id}")
    print(f"    Cluster ID:  {cluster_id}")
    print(f"    Submit dir:  {wf_dir}")

    group_dirs = sorted(wf_dir.glob("mg_*"))
    print(f"    Merge groups: {len(group_dirs)}")
    for gd in group_dirs:
        proc_subs = list(gd.glob("proc_*.sub"))
        print(f"      {gd.name}: {len(proc_subs)} proc nodes")

    # 5. Poll
    print(f"\n[5/6] Polling DAGMan (max {MAX_WAIT}s)...")
    start_time = time.time()
    final_status = None

    while time.time() - start_time < MAX_WAIT:
        try:
            info = await condor.query_job(schedd_name, cluster_id)
        except Exception as e:
            print(f"    Query error: {e}")
            await asyncio.sleep(POLL_INTERVAL)
            continue

        if info is None:
            completed = await condor.check_job_completed(cluster_id, schedd_name)
            final_status = "completed" if completed else "vanished"
            break

        job_status = int(info.get("JobStatus", 0))
        nodes_total = int(info.get("DAG_NodesTotal", 0))
        nodes_done = int(info.get("DAG_NodesDone", 0))
        nodes_failed = int(info.get("DAG_NodesFailed", 0))
        nodes_queued = int(info.get("DAG_NodesQueued", 0))
        nodes_ready = int(info.get("DAG_NodesReady", 0))

        status_str = {1: "idle", 2: "running", 3: "removed", 4: "completed", 5: "held"}
        elapsed = int(time.time() - start_time)
        print(f"    [{elapsed:4d}s] status={status_str.get(job_status, job_status)} "
              f"nodes: {nodes_done}/{nodes_total} done, "
              f"{nodes_queued} queued, {nodes_ready} ready, "
              f"{nodes_failed} failed")

        if job_status in (3, 4):
            final_status = "completed" if job_status == 4 else "removed"
            break

        await asyncio.sleep(POLL_INTERVAL)
    else:
        final_status = "timeout"

    elapsed_total = int(time.time() - start_time)
    print(f"\n    DAG finished: {final_status} in {elapsed_total}s ({elapsed_total/60:.1f} min)")

    # 6. Check results
    print(f"\n[6/6] Checking results...")
    success = True

    for gd in group_dirs:
        proc_outs = sorted(gd.glob("proc_*.out"))
        proc_errs = sorted(gd.glob("proc_*.err"))
        print(f"    {gd.name}: {len(proc_outs)} .out, {len(proc_errs)} .err")

        for err_file in proc_errs:
            content = err_file.read_text().strip()
            if content:
                lines = content.split("\n")
                print(f"      {err_file.name} (last 3 lines):")
                for line in lines[-3:]:
                    print(f"        {line}")

        for out_file in proc_outs:
            content = out_file.read_text().strip()
            if content:
                lines = content.split("\n")
                # Show key lines
                for line in lines:
                    if any(kw in line for kw in ["completed in", "Output:", "wall_time", "ERROR", "output_bytes"]):
                        print(f"      {line.strip()}")

        # Check FJR files
        fjr_files = sorted(gd.glob("report_step*.xml"))
        if fjr_files:
            print(f"      FJR files: {[f.name for f in fjr_files]}")

        # Check .root output files
        root_files = sorted(gd.glob("*.root"))
        if root_files:
            for rf in root_files:
                print(f"      {rf.name}: {rf.stat().st_size:,} bytes")

        merge_out = gd / "merge.out"
        if merge_out.exists():
            content = merge_out.read_text().strip()
            if content:
                for line in content.split("\n")[-5:]:
                    print(f"      merge: {line}")

    # Check output directory
    merged_files = list(output_dir.rglob("*"))
    if merged_files:
        print(f"\n    Output directory:")
        for f in sorted(merged_files):
            if f.is_file():
                print(f"      {f.relative_to(output_dir)}: {f.stat().st_size:,} bytes")

    rescue_dags = list(wf_dir.glob("workflow.dag.rescue*"))
    if rescue_dags:
        print(f"\n    WARNING: Rescue DAG(s): {[r.name for r in rescue_dags]}")
        success = False

    print(f"\n{'='*70}")
    if final_status == "completed" and success:
        print(f"RESULT: PASS — Real CMSSW DAG completed in {elapsed_total}s ({elapsed_total/60:.1f} min)")
    elif final_status == "timeout":
        print(f"RESULT: TIMEOUT after {MAX_WAIT}s")
        try:
            await condor.remove_job(schedd_name, cluster_id)
        except Exception:
            pass
        success = False
    else:
        print(f"RESULT: {final_status.upper()}")
        success = False
    print("=" * 70)

    return success


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True)
    parser.add_argument("--events", type=int, default=1000,
                        help="Events per job (each job processes this many)")
    parser.add_argument("--jobs", type=int, default=1,
                        help="Number of parallel proc jobs (work unit size)")
    args = parser.parse_args()

    result = asyncio.run(run_test(args.request, args.events, args.jobs))
    sys.exit(0 if result else 1)
