#!/usr/bin/env python3
"""E2E reduced-scale test: one work unit with synthetic processing.

Creates a sandbox, plans a production DAG with 1 work unit (5 processing
nodes at 10% of nominal events), submits to HTCondor DAGMan, and polls
until completion.

Must run as the wms2 Linux user (HTCondor forbids root).

Usage:
    su - wms2 -c "/mnt/shared/work/wms2/.venv/bin/python \
        /mnt/shared/work/wms2/scripts/e2e_reduced_test.py"
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Ensure project is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from wms2.adapters.condor import HTCondorAdapter
from wms2.config import Settings
from wms2.core.dag_planner import DAGPlanner, PilotMetrics
from wms2.core.sandbox import create_sandbox
from wms2.core.splitters import DAGNodeSpec, InputFile, get_splitter


# ── Configuration ──────────────────────────────────────────────

TEST_DIR = Path("/mnt/shared/work/wms2_e2e_test")
CONDOR_HOST = os.environ.get("WMS2_CONDOR_HOST", "localhost:9618")
POLL_INTERVAL = 5   # seconds between DAGMan polls
MAX_WAIT = 600      # max seconds to wait for DAG completion

# 10% of nominal: 1000 events/file instead of 10000
EVENTS_PER_FILE = 1000
NUM_FILES = 5        # 5 files → 5 proc nodes → 1 work unit (< 8)
FILES_PER_JOB = 1    # 1 file per proc node


def setup_test_dir() -> Path:
    """Create a clean test directory."""
    if TEST_DIR.exists():
        # Clean contents but keep the directory (may be owned by different user)
        for item in TEST_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    else:
        TEST_DIR.mkdir(parents=True)
    return TEST_DIR


def create_test_sandbox(test_dir: Path) -> str:
    """Create a synthetic-mode sandbox for testing."""
    sandbox_path = str(test_dir / "sandbox.tar.gz")
    request_data = {
        "SizePerEvent": 5.0,        # 5 KB/event — small synthetic output
        "TimePerEvent": 0.001,       # 1ms/event — fast
        "Memory": 2048,
    }
    create_sandbox(sandbox_path, request_data, mode="synthetic")
    print(f"  Sandbox created: {sandbox_path}")
    return sandbox_path


def make_workflow(test_dir: Path, sandbox_path: str) -> MagicMock:
    """Create a mock workflow object for the DAG planner."""
    wf = MagicMock()
    wf.id = uuid.uuid4()
    wf.request_name = "e2e-reduced-test-001"
    wf.input_dataset = "/TestPrimary/TestProcessed/RECO"
    wf.splitting_algo = "FileBased"
    wf.splitting_params = {"files_per_job": FILES_PER_JOB}
    wf.sandbox_url = sandbox_path
    wf.category_throttles = {"Processing": 100, "Merge": 10, "Cleanup": 10}
    wf.config_data = {
        "sandbox_path": sandbox_path,
        "memory_mb": 2048,
        "multicore": 1,
        "time_per_event": 0.001,
        "size_per_event": 5.0,
        "output_datasets": [
            {
                "dataset_name": "/TestPrimary/E2ETest-v1/AODSIM",
                "merged_lfn_base": "/store/mc/E2ETest/TestPrimary/AODSIM/Test-v1",
                "data_tier": "AODSIM",
            },
        ],
    }
    return wf


def make_input_files() -> list[InputFile]:
    """Create synthetic input files (no real DBS query)."""
    return [
        InputFile(
            lfn=f"/store/data/e2e_test_file_{i}.root",
            file_size=100_000_000,  # 100 MB
            event_count=EVENTS_PER_FILE,
            locations=["T2_LOCAL_DEV"],
        )
        for i in range(NUM_FILES)
    ]


def make_processing_nodes() -> list[DAGNodeSpec]:
    """Split input files into processing nodes."""
    input_files = make_input_files()
    splitter = get_splitter("FileBased", {"files_per_job": FILES_PER_JOB})
    return splitter.split(input_files)


def make_mock_repo() -> MagicMock:
    """Create a mock repository (DAG planner needs it for DB updates)."""
    repo = MagicMock()
    dag_row = MagicMock()
    dag_row.id = uuid.uuid4()
    repo.create_dag = AsyncMock(return_value=dag_row)
    repo.update_dag = AsyncMock()
    repo.update_workflow = AsyncMock()
    return repo


def make_mock_dbs(nodes: list[DAGNodeSpec]) -> MagicMock:
    """Create a mock DBS adapter that returns our synthetic files."""
    dbs = MagicMock()

    async def get_files(dataset, limit=0, **kw):
        return [
            {
                "logical_file_name": f.lfn,
                "file_size": f.file_size,
                "event_count": f.event_count,
            }
            for f in make_input_files()[:limit if limit else NUM_FILES]
        ]

    dbs.get_files = get_files
    return dbs


def make_mock_rucio() -> MagicMock:
    """Create a mock Rucio adapter returning synthetic replicas."""
    rucio = MagicMock()

    async def get_replicas(lfns):
        return {lfn: ["T2_LOCAL_DEV"] for lfn in lfns}

    rucio.get_replicas = get_replicas
    return rucio


async def run_test():
    """Main test sequence."""
    print("=" * 60)
    print("WMS2 E2E Reduced-Scale Test")
    print(f"  {NUM_FILES} files × {EVENTS_PER_FILE} events = "
          f"{NUM_FILES * EVENTS_PER_FILE} total events")
    print(f"  {FILES_PER_JOB} file(s)/job → {NUM_FILES} proc nodes → 1 work unit")
    print(f"  Condor: {CONDOR_HOST}")
    print("=" * 60)

    # 1. Setup
    print("\n[1/6] Setting up test directory...")
    test_dir = setup_test_dir()
    submit_dir = test_dir / "submit"
    output_dir = test_dir / "output"
    output_dir.mkdir()

    # 2. Create sandbox
    print("[2/6] Creating synthetic sandbox...")
    sandbox_path = create_test_sandbox(test_dir)

    # 3. Build DAG planner with real HTCondor
    print("[3/6] Connecting to HTCondor...")
    try:
        condor = HTCondorAdapter(CONDOR_HOST)
        print(f"  Connected to schedd")
    except Exception as e:
        print(f"  ERROR: Cannot connect to HTCondor at {CONDOR_HOST}: {e}")
        print("  Make sure HTCondor is running and WMS2_CONDOR_HOST is set.")
        return False

    settings = Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        submit_base_dir=str(submit_dir),
        jobs_per_work_unit=8,
        processing_executable="/bin/true",  # triggers wms2_proc.sh
        merge_executable="/bin/true",       # triggers wms2_merge.py
        cleanup_executable="/bin/true",
        output_base_dir=str(output_dir),
        max_input_files=NUM_FILES,
    )

    repo = make_mock_repo()
    planner = DAGPlanner(
        repository=repo,
        dbs_adapter=make_mock_dbs([]),
        rucio_adapter=make_mock_rucio(),
        condor_adapter=condor,
        settings=settings,
    )

    # 4. Plan and submit DAG
    print("[4/6] Planning and submitting production DAG...")
    wf = make_workflow(test_dir, sandbox_path)
    metrics = PilotMetrics.from_request(wf.config_data)

    try:
        dag = await planner.plan_production_dag(wf, metrics=metrics)
    except Exception as e:
        print(f"  ERROR: DAG planning/submission failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    # Extract cluster info from the mock repo calls
    update_dag_calls = repo.update_dag.call_args_list
    cluster_id = None
    schedd_name = None
    for call in update_dag_calls:
        kw = call[1]
        if "dagman_cluster_id" in kw:
            cluster_id = kw["dagman_cluster_id"]
            schedd_name = kw["schedd_name"]

    if not cluster_id:
        print("  ERROR: No cluster ID found — DAG was not submitted?")
        return False

    # Show what was generated
    wf_dir = submit_dir / str(wf.id)
    dag_file = wf_dir / "workflow.dag"
    print(f"  Workflow ID:  {wf.id}")
    print(f"  DAG file:     {dag_file}")
    print(f"  Cluster ID:   {cluster_id}")
    print(f"  Schedd:       {schedd_name}")

    # List generated files
    group_dirs = sorted(wf_dir.glob("mg_*"))
    print(f"  Merge groups: {len(group_dirs)}")
    for gd in group_dirs:
        proc_subs = list(gd.glob("proc_*.sub"))
        print(f"    {gd.name}: {len(proc_subs)} proc nodes")

    # 5. Poll until completion
    print(f"\n[5/6] Polling DAGMan (max {MAX_WAIT}s)...")
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
            # DAGMan no longer in queue — check history
            completed = await condor.check_job_completed(cluster_id, schedd_name)
            if completed:
                final_status = "completed"
                break
            else:
                final_status = "vanished"
                break

        # Parse DAGMan status
        job_status = int(info.get("JobStatus", 0))
        nodes_total = int(info.get("DAG_NodesTotal", 0))
        nodes_done = int(info.get("DAG_NodesDone", 0))
        nodes_failed = int(info.get("DAG_NodesFailed", 0))
        nodes_queued = int(info.get("DAG_NodesQueued", 0))
        nodes_ready = int(info.get("DAG_NodesReady", 0))

        status_str = {1: "idle", 2: "running", 3: "removed", 4: "completed", 5: "held"}
        elapsed = int(time.time() - start_time)

        print(f"  [{elapsed:3d}s] DAGMan status={status_str.get(job_status, job_status)} "
              f"nodes: {nodes_done}/{nodes_total} done, "
              f"{nodes_queued} queued, {nodes_ready} ready, "
              f"{nodes_failed} failed")

        if job_status in (3, 4):  # removed or completed
            final_status = "completed" if job_status == 4 else "removed"
            break

        await asyncio.sleep(POLL_INTERVAL)
    else:
        final_status = "timeout"

    elapsed_total = int(time.time() - start_time)
    print(f"\n  DAG finished: {final_status} in {elapsed_total}s")

    # 6. Check results
    print(f"\n[6/6] Checking results...")
    success = True

    # Check DAGMan log for node completion
    dag_status_file = wf_dir / "workflow.dag.status"
    if dag_status_file.exists():
        status_content = dag_status_file.read_text()
        print(f"  DAG status file: {len(status_content)} bytes")
    else:
        print("  WARNING: No DAG status file found")

    # Check for output files in merge group directories
    for gd in group_dirs:
        # Proc outputs
        proc_outs = list(gd.glob("proc_*.out"))
        proc_errs = list(gd.glob("proc_*.err"))
        print(f"  {gd.name}: {len(proc_outs)} proc .out files, {len(proc_errs)} proc .err files")

        # Check for any error content
        for err_file in proc_errs:
            content = err_file.read_text().strip()
            if content:
                print(f"    {err_file.name}: {content[:200]}")

        # Check merge output
        merge_out = gd / "merge.out"
        merge_err = gd / "merge.err"
        if merge_out.exists():
            content = merge_out.read_text().strip()
            print(f"  {gd.name}/merge.out: {len(content)} chars")
            if content:
                # Show last few lines
                lines = content.split("\n")
                for line in lines[-5:]:
                    print(f"    {line}")
        else:
            print(f"  WARNING: {gd.name}/merge.out not found")
            success = False

        # Landing node
        landing_out = gd / "landing.out"
        if landing_out.exists():
            print(f"  {gd.name}/landing.out: exists")
        else:
            print(f"  WARNING: {gd.name}/landing.out not found")

    # Check merged output directory
    merged_files = list(output_dir.rglob("*"))
    if merged_files:
        print(f"\n  Output directory ({output_dir}):")
        for f in merged_files:
            if f.is_file():
                size = f.stat().st_size
                print(f"    {f.relative_to(output_dir)}: {size} bytes")
    else:
        print(f"\n  Output directory empty — merge may have found no proc outputs")

    # Check DAGMan rescue DAG (indicates failures)
    rescue_dags = list(wf_dir.glob("workflow.dag.rescue*"))
    if rescue_dags:
        print(f"\n  WARNING: Rescue DAG(s) found: {[r.name for r in rescue_dags]}")
        success = False

    # Summary
    print("\n" + "=" * 60)
    if final_status == "completed" and success:
        print("RESULT: PASS — DAG completed successfully")
    elif final_status == "completed":
        print("RESULT: PARTIAL — DAG completed but some checks failed")
    elif final_status == "timeout":
        print(f"RESULT: TIMEOUT — DAG did not complete within {MAX_WAIT}s")
        # Clean up
        print(f"  Removing DAG cluster {cluster_id}...")
        try:
            await condor.remove_job(schedd_name, cluster_id)
        except Exception:
            pass
        success = False
    else:
        print(f"RESULT: FAIL — DAG status: {final_status}")
        success = False
    print("=" * 60)

    return success


if __name__ == "__main__":
    result = asyncio.run(run_test())
    sys.exit(0 if result else 1)
