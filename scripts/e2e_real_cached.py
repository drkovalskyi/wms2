#!/usr/bin/env python3
"""E2E real CMSSW test reusing a cached sandbox.

Runs 8 proc jobs (50 events each, 400 total) through the full 5-step NPS
StepChain with proper unmerged→merged→cleanup output pipeline.
This exercises one full work unit: 8 jobs x 8 cores = 64 cores.

Must run as the wms2 Linux user.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wms2.adapters.condor import HTCondorAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner, PilotMetrics

# ── Configuration ──────────────────────────────────────────────

CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
PFN_PREFIX = "/mnt/shared"
POLL_INTERVAL = 10
MAX_WAIT = 7200  # 2 hours

# Cached sandbox from previous fetch (has all 5 ConfigCache PSets)
CACHED_SANDBOX = "/mnt/shared/work/wms2_real_condor_test/sandbox.tar.gz"

REQUEST_NAME = "cmsunified_task_NPS-Run3Summer22EEGS-00049__v1_T_260126_110934_54"
EVENTS_PER_JOB = 50
NUM_JOBS = 8

# Output datasets — kept outputs are Step4 (MINIAODSIM) and Step5 (NANOEDMAODSIM)
OUTPUT_DATASETS = [
    {
        "dataset_name": "/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2/MINIAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EEMiniAODv4/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EEMiniAODv4/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/MINIAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "MINIAODSIM",
    },
    {
        "dataset_name": "/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/Run3Summer22EENanoAODv12-130X_mcRun3_2022_realistic_postEE_v6-v2/NANOAODSIM",
        "merged_lfn_base": "/store/mc/Run3Summer22EENanoAODv12/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "unmerged_lfn_base": "/store/unmerged/Run3Summer22EENanoAODv12/ADD-monojet_N-6_MD-6000_TuneCP5_13p6TeV_pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2",
        "data_tier": "NANOEDMAODSIM",
    },
]

TEST_DIR = Path("/mnt/shared/work/wms2_real_output_test")


def setup():
    if TEST_DIR.exists():
        for item in TEST_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    else:
        TEST_DIR.mkdir(parents=True)
    # Clean previous LFN outputs
    for ds in OUTPUT_DATASETS:
        for key in ("merged_lfn_base", "unmerged_lfn_base"):
            pfn = os.path.join(PFN_PREFIX, ds[key].lstrip("/"))
            if os.path.isdir(pfn):
                shutil.rmtree(pfn)


def make_workflow(sandbox_path: str) -> MagicMock:
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = REQUEST_NAME
    wf.input_dataset = ""
    wf.splitting_algo = "EventBased"
    wf.splitting_params = {"events_per_job": EVENTS_PER_JOB}
    wf.sandbox_url = sandbox_path
    wf.category_throttles = {"Processing": 100, "Merge": 10, "Cleanup": 10}
    wf.config_data = {
        "sandbox_path": sandbox_path,
        "memory_mb": 16000,
        "multicore": 8,
        "time_per_event": 1.0,
        "size_per_event": 50.0,
        "request_num_events": EVENTS_PER_JOB * NUM_JOBS,
        "_is_gen": True,
        "output_datasets": OUTPUT_DATASETS,
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


async def run():
    total_events = EVENTS_PER_JOB * NUM_JOBS
    print("=" * 70)
    print("WMS2 Real CMSSW E2E Test — Output Pipeline")
    print(f"  Request:      {REQUEST_NAME}")
    print(f"  Events:       {EVENTS_PER_JOB}/job x {NUM_JOBS} jobs = {total_events}")
    print(f"  Steps:        5 (GEN-SIM → DRPremix x2 → MiniAOD → NanoAOD)")
    print(f"  PFN prefix:   {PFN_PREFIX}")
    print(f"  Condor:       {CONDOR_HOST}")
    print(f"  Sandbox:      {CACHED_SANDBOX}")
    for ds in OUTPUT_DATASETS:
        print(f"  Output:       {ds['dataset_name']}")
    print("=" * 70)

    # 1. Setup
    print("\n[1/7] Setting up...")
    setup()

    # Verify sandbox exists
    if not os.path.isfile(CACHED_SANDBOX):
        print(f"  ERROR: Cached sandbox not found: {CACHED_SANDBOX}")
        return False

    # Copy sandbox to test dir
    sandbox_path = str(TEST_DIR / "sandbox.tar.gz")
    shutil.copy2(CACHED_SANDBOX, sandbox_path)
    print(f"  Sandbox: {sandbox_path} ({os.path.getsize(sandbox_path):,} bytes)")

    # 2. Connect to HTCondor
    print("[2/7] Connecting to HTCondor...")
    try:
        condor = HTCondorAdapter(CONDOR_HOST)
        print("  Connected")
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

    submit_dir = str(TEST_DIR / "submit")
    settings = Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        submit_base_dir=submit_dir,
        jobs_per_work_unit=8,
        processing_executable="/bin/true",
        merge_executable="/bin/true",
        cleanup_executable="/bin/true",
        local_pfn_prefix=PFN_PREFIX,
    )

    repo = make_mock_repo()
    planner = DAGPlanner(
        repository=repo,
        dbs_adapter=MagicMock(),
        rucio_adapter=MagicMock(),
        condor_adapter=condor,
        settings=settings,
    )

    # 3. Plan and submit
    print("[3/7] Planning and submitting DAG...")
    wf = make_workflow(sandbox_path)
    metrics = PilotMetrics.from_request(wf.config_data)

    try:
        dag = await planner.plan_production_dag(wf, metrics=metrics)
    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

    cluster_id = None
    schedd_name = None
    for call in repo.update_dag.call_args_list:
        kw = call[1]
        if "dagman_cluster_id" in kw:
            cluster_id = kw["dagman_cluster_id"]
            schedd_name = kw["schedd_name"]

    if not cluster_id:
        print("  ERROR: No cluster ID")
        return False

    wf_dir = Path(submit_dir) / str(wf.id)
    group_dirs = sorted(wf_dir.glob("mg_*"))
    print(f"  Workflow:     {wf.id}")
    print(f"  Cluster:      {cluster_id}")
    print(f"  Merge groups: {len(group_dirs)}")
    for gd in group_dirs:
        procs = list(gd.glob("proc_*.sub"))
        print(f"    {gd.name}: {len(procs)} proc node(s)")

    # 4. Poll
    print(f"\n[4/7] Polling DAGMan (max {MAX_WAIT//60} min)...")
    start_time = time.time()
    final_status = None

    while time.time() - start_time < MAX_WAIT:
        try:
            info = await condor.query_job(schedd_name, cluster_id)
        except Exception as e:
            print(f"  Query error: {e}")
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

        status_str = {1: "idle", 2: "running", 3: "removed", 4: "completed", 5: "held"}
        elapsed = int(time.time() - start_time)
        print(f"  [{elapsed:4d}s] {status_str.get(job_status, job_status)} "
              f"done={nodes_done}/{nodes_total} queued={nodes_queued} failed={nodes_failed}")

        if job_status in (3, 4):
            final_status = "completed" if job_status == 4 else "removed"
            break

        await asyncio.sleep(POLL_INTERVAL)
    else:
        final_status = "timeout"

    elapsed_total = int(time.time() - start_time)
    print(f"\n  DAG {final_status} in {elapsed_total}s ({elapsed_total/60:.1f} min)")

    # 5. Per-step timing
    print(f"\n[5/7] Per-step timing...")
    for gd in group_dirs:
        for out_file in sorted(gd.glob("proc_*.out")):
            content = out_file.read_text()
            for m in re.finditer(r"Step (\S+) completed in (\d+)s", content):
                print(f"  {m.group(1)}: {m.group(2)}s")
            wm = re.search(r"wall_time_sec:\s*(\d+)", content)
            if wm:
                print(f"  Total wall: {wm.group(1)}s")

    # 6. Check job logs
    print(f"\n[6/7] Job logs...")
    success = True
    for gd in group_dirs:
        print(f"\n  --- {gd.name} ---")

        # Proc stdout — stage-out lines
        for pout in sorted(gd.glob("proc_*.out")):
            content = pout.read_text()
            stage_lines = [l for l in content.splitlines()
                           if "Staged:" in l or "stage-out" in l.lower() or "output_file" in l]
            print(f"  {pout.name}:")
            for sl in stage_lines:
                print(f"    {sl.strip()}")

        # Proc stderr — errors only
        for perr in sorted(gd.glob("proc_*.err")):
            content = perr.read_text().strip()
            if content:
                error_lines = [l for l in content.splitlines() if "error" in l.lower() or "fatal" in l.lower()]
                if error_lines:
                    print(f"  {perr.name} ERRORS:")
                    for el in error_lines[:5]:
                        print(f"    {el.strip()}")

        # FJR
        fjrs = sorted(gd.glob("report_step*.xml"))
        if fjrs:
            print(f"  FJR: {[f.name for f in fjrs]}")

        # Merge
        merge_out = gd / "merge.out"
        if merge_out.exists():
            content = merge_out.read_text()
            for line in content.splitlines():
                if any(kw in line.lower() for kw in ["detected", "tier", "output:", "wrote",
                    "copied", "cleanup", "merged", "batch"]):
                    print(f"  merge: {line.strip()}")
        else:
            print("  WARNING: merge.out missing")
            success = False

        # Cleanup
        cleanup_out = gd / "cleanup.out"
        if cleanup_out.exists():
            content = cleanup_out.read_text().strip()
            for line in content.splitlines():
                print(f"  cleanup: {line.strip()}")

    # 7. Final output files
    print(f"\n[7/7] Final output files:")
    for ds in OUTPUT_DATASETS:
        tier = ds["data_tier"]
        for label, key in [("merged", "merged_lfn_base"), ("unmerged", "unmerged_lfn_base")]:
            lfn = ds[key]
            pfn_dir = os.path.join(PFN_PREFIX, lfn.lstrip("/"))
            if os.path.isdir(pfn_dir):
                files = [f for f in Path(pfn_dir).rglob("*") if f.is_file()]
                if files:
                    total = sum(f.stat().st_size for f in files)
                    print(f"\n  {label}/{tier}: {len(files)} files, {total / 1048576:.1f} MB")
                    for f in sorted(files):
                        print(f"    {f}  ({f.stat().st_size:,} bytes)")
                else:
                    tag = "(cleaned up)" if label == "unmerged" else "(empty)"
                    print(f"\n  {label}/{tier}: {tag}")
            else:
                tag = "(cleaned up)" if label == "unmerged" else "(NOT FOUND)"
                print(f"\n  {label}/{tier}: {tag}")
                if label == "merged":
                    success = False

    rescue_dags = list(wf_dir.glob("workflow.dag.rescue*"))
    if rescue_dags:
        print(f"\n  WARNING: Rescue DAG(s): {[r.name for r in rescue_dags]}")
        success = False

    print(f"\n{'=' * 70}")
    print(f"Input request: {REQUEST_NAME}")
    if final_status == "completed" and success:
        print("RESULT: PASS")
    elif final_status == "timeout":
        print(f"RESULT: TIMEOUT after {MAX_WAIT}s")
        try:
            await condor.remove_job(schedd_name, cluster_id)
        except Exception:
            pass
        success = False
    else:
        print(f"RESULT: {final_status.upper()}" + (" (with issues)" if not success else ""))
        if not success and final_status == "completed":
            success = False
    print("=" * 70)
    return success


if __name__ == "__main__":
    result = asyncio.run(run())
    sys.exit(0 if result else 1)
