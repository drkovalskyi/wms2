"""CLI runner: import a real workflow, plan DAG, submit to HTCondor, monitor."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import ssl
import sys
from typing import Any

from wms2.config import Settings
from wms2.core.dag_monitor import DAGMonitor
from wms2.core.dag_planner import DAGPlanner
from wms2.core.workflow_manager import WorkflowManager
from wms2.db.engine import create_engine, create_session_factory
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus

logger = logging.getLogger(__name__)

DEFAULT_PROXY = "/tmp/x509up_u{uid}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wms2",
        description="WMS2 CLI — import and run real workflows",
    )
    sub = parser.add_subparsers(dest="command")

    imp = sub.add_parser("import", help="Import a request from ReqMgr2 and run it")
    imp.add_argument("request_name", help="ReqMgr2 request name")
    imp.add_argument("--proxy", default=None, help="X.509 proxy cert (default: /tmp/x509up_u$UID)")
    imp.add_argument("--cert", default=None, help="X.509 cert path (alternative to --proxy)")
    imp.add_argument("--key", default=None, help="X.509 key path (alternative to --proxy)")
    imp.add_argument("--condor-host", default="localhost:9618", help="HTCondor collector")
    imp.add_argument("--schedd-name", default=None, help="Explicit schedd name")
    imp.add_argument("--submit-dir", default="/tmp/wms2", help="DAG file output directory")
    imp.add_argument("--max-files", type=int, default=0, help="Limit DBS file query (0=all)")
    imp.add_argument("--files-per-job", type=int, default=None, help="Override splitting param")
    imp.add_argument("--dry-run", action="store_true", help="Plan DAG but don't submit")
    imp.add_argument("--db-url", default=None, help="Override database URL")
    imp.add_argument("--poll-interval", type=int, default=10, help="Monitoring poll interval (seconds)")
    imp.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    return parser


def _resolve_cert(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve X.509 cert+key from --proxy or --cert/--key."""
    if args.proxy:
        return args.proxy, args.proxy
    if args.cert and args.key:
        return args.cert, args.key
    # Default: grid proxy
    proxy_path = DEFAULT_PROXY.format(uid=os.getuid())
    if os.path.exists(proxy_path):
        return proxy_path, proxy_path
    raise SystemExit(
        f"No X.509 credential found. Tried: {proxy_path}\n"
        "Use --proxy /path/to/proxy or --cert/--key."
    )


def _make_ssl_context(cert_file: str, key_file: str, ca_path: str) -> ssl.SSLContext:
    """Build an SSL context with CERN Grid CA + client cert."""
    ctx = ssl.create_default_context(capath=ca_path)
    ctx.load_cert_chain(cert_file, key_file)
    return ctx


def build_settings(args: argparse.Namespace, cert_file: str, key_file: str) -> Settings:
    """Build Settings from CLI args, overriding executables to /bin/true."""
    overrides: dict = {
        "cert_file": cert_file,
        "key_file": key_file,
        "condor_host": args.condor_host,
        "submit_base_dir": args.submit_dir,
        "max_input_files": args.max_files,
        "processing_executable": "/bin/true",
        "merge_executable": "/bin/true",
        "cleanup_executable": "/bin/true",
        "log_level": args.log_level,
    }
    if args.schedd_name:
        overrides["schedd_name"] = args.schedd_name
    if args.db_url:
        overrides["database_url"] = args.db_url
    return Settings(**overrides)


def _build_adapters(settings: Settings, ssl_ctx: ssl.SSLContext):
    """Build real adapters for CLI use."""
    from wms2.adapters.condor import HTCondorAdapter
    from wms2.adapters.dbs import DBSClient
    from wms2.adapters.reqmgr2 import ReqMgr2Client
    from wms2.adapters.rucio import RucioClient

    reqmgr = ReqMgr2Client(
        settings.reqmgr2_url, settings.cert_file, settings.key_file,
        verify=ssl_ctx,
    )
    dbs = DBSClient(
        settings.dbs_url, settings.cert_file, settings.key_file,
        verify=ssl_ctx,
    )
    rucio = RucioClient(
        settings.rucio_url, settings.rucio_account,
        settings.cert_file, settings.key_file,
        verify=ssl_ctx,
    )
    condor = HTCondorAdapter(settings.condor_host, settings.schedd_name)
    return reqmgr, dbs, rucio, condor


def _normalize_request(reqdata: dict[str, Any]) -> dict[str, Any]:
    """Normalize a ReqMgr2 request dict so top-level fields are always present.

    For StepChain/TaskChain requests, extract InputDataset, SplittingAlgo, etc.
    from the first step/task.
    """
    rtype = reqdata.get("RequestType", "")

    # For StepChain, pull fields from Step1
    if rtype == "StepChain":
        step1 = reqdata.get("Step1", {})
        if not reqdata.get("InputDataset"):
            # GEN-SIM: no input dataset — use first OutputDataset if available
            outputs = reqdata.get("OutputDatasets", [])
            reqdata["InputDataset"] = outputs[0] if outputs else step1.get("PrimaryDataset", "")
        if not reqdata.get("SplittingAlgo"):
            reqdata["SplittingAlgo"] = step1.get("SplittingAlgo", "EventBased")
        if not reqdata.get("EventsPerJob"):
            reqdata["EventsPerJob"] = step1.get("EventsPerJob")
        if not reqdata.get("RequestNumEvents"):
            reqdata["RequestNumEvents"] = step1.get("RequestNumEvents")

    # For TaskChain, pull from Task1
    elif rtype == "TaskChain":
        task1 = reqdata.get("Task1", {})
        if not reqdata.get("InputDataset"):
            outputs = reqdata.get("OutputDatasets", [])
            reqdata["InputDataset"] = outputs[0] if outputs else task1.get("InputDataset", "")
        if not reqdata.get("SplittingAlgo"):
            reqdata["SplittingAlgo"] = task1.get("SplittingAlgo", "FileBased")

    # Default SandboxUrl if missing
    if not reqdata.get("SandboxUrl"):
        reqdata["SandboxUrl"] = "N/A"

    # Build SplittingParams from scattered fields if not present
    if not reqdata.get("SplittingParams"):
        params = {}
        for key in ("FilesPerJob", "EventsPerJob", "LumisPerJob"):
            val = reqdata.get(key)
            if val is not None:
                params[key.lower()] = val
        if params:
            reqdata["SplittingParams"] = params

    return reqdata


async def run_import(args: argparse.Namespace) -> None:
    cert_file, key_file = _resolve_cert(args)
    settings = build_settings(args, cert_file, key_file)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )

    request_name = args.request_name
    dry_run = args.dry_run

    print(f"=== WMS2 CLI: importing {request_name} ===")
    print(f"  cert:         {cert_file}")
    print(f"  condor_host:  {settings.condor_host}")
    print(f"  submit_dir:   {settings.submit_base_dir}")
    print(f"  max_files:    {settings.max_input_files or 'all'}")
    print(f"  executables:  /bin/true (test mode)")
    print(f"  dry_run:      {dry_run}")
    print()

    # SSL context for CERN Grid services
    ssl_ctx = _make_ssl_context(cert_file, key_file, settings.ssl_ca_path)

    # Database
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    # Adapters
    reqmgr, dbs, rucio, condor = _build_adapters(settings, ssl_ctx)

    try:
        async with session_factory() as session:
            repo = Repository(session)

            # 1. Fetch from ReqMgr2
            print("[1/5] Fetching request from ReqMgr2...")
            reqdata = await reqmgr.get_request(request_name)
            reqdata = _normalize_request(reqdata)

            print(f"      type:        {reqdata.get('RequestType')}")
            print(f"      dataset:     {reqdata.get('InputDataset')}")
            print(f"      splitting:   {reqdata.get('SplittingAlgo')}")

            # Create request row (workflow FK requires it)
            await repo.create_request(
                request_name=request_name,
                requestor=reqdata.get("Requestor", "unknown"),
                request_data=reqdata,
                input_dataset=reqdata.get("InputDataset"),
                campaign=reqdata.get("Campaign"),
                priority=reqdata.get("RequestPriority", reqdata.get("Priority", 100000)),
                status="submitted",
            )
            await session.commit()

            # Create workflow via WorkflowManager (uses the same reqmgr to re-fetch)
            # But WorkflowManager.import_request calls reqmgr.get_request again,
            # which is fine — it's idempotent. However, it won't have our normalization.
            # So create the workflow row directly instead.
            workflow = await repo.create_workflow(
                request_name=request_name,
                input_dataset=reqdata["InputDataset"],
                splitting_algo=reqdata["SplittingAlgo"],
                splitting_params=reqdata.get("SplittingParams", {}),
                sandbox_url=reqdata.get("SandboxUrl", "N/A"),
                config_data={
                    "campaign": reqdata.get("Campaign"),
                    "requestor": reqdata.get("Requestor"),
                    "priority": reqdata.get("RequestPriority"),
                    "request_type": reqdata.get("RequestType"),
                },
            )

            # Override splitting_params if --files-per-job given
            if args.files_per_job is not None:
                params = workflow.splitting_params or {}
                params["files_per_job"] = args.files_per_job
                await repo.update_workflow(workflow.id, splitting_params=params)
                workflow = await repo.get_workflow(workflow.id)

            await session.commit()
            print(f"      workflow_id: {workflow.id}")
            print()

            # 2. Plan production DAG
            dp = DAGPlanner(repo, dbs, rucio, condor, settings)

            if dry_run:
                print("[2/5] Planning production DAG (dry-run)...")
                from wms2.core.dag_planner import PilotMetrics, _generate_dag_files, _plan_merge_groups
                from wms2.core.splitters import InputFile, get_splitter

                metrics = PilotMetrics()
                limit = settings.max_input_files if settings.max_input_files > 0 else 0
                raw_files = await dbs.get_files(workflow.input_dataset, limit=limit)
                if not raw_files:
                    print("ERROR: No input files found for dataset")
                    return

                lfns = [f["logical_file_name"] for f in raw_files]
                print(f"      input files: {len(raw_files)}")

                try:
                    replica_map = await rucio.get_replicas(lfns)
                except Exception as exc:
                    logger.warning("Rucio replica lookup failed (%s), using empty locations", exc)
                    print("      (Rucio unavailable — using empty site locations)")
                    replica_map = {}
                input_files = [
                    InputFile(
                        lfn=f["logical_file_name"],
                        file_size=f.get("file_size", 0),
                        event_count=f.get("event_count", 0),
                        locations=replica_map.get(f["logical_file_name"], []),
                    )
                    for f in raw_files
                ]

                splitter = get_splitter(
                    workflow.splitting_algo,
                    workflow.splitting_params or {},
                )
                nodes = splitter.split(input_files)
                merge_groups = _plan_merge_groups(
                    nodes,
                    output_size_per_event_kb=metrics.output_size_per_event_kb,
                    target_kb=settings.target_merged_size_kb,
                )

                submit_dir = os.path.join(settings.submit_base_dir, str(workflow.id))
                os.makedirs(submit_dir, exist_ok=True)
                executables = {
                    "processing": settings.processing_executable,
                    "merge": settings.merge_executable,
                    "cleanup": settings.cleanup_executable,
                }
                dag_file_path = _generate_dag_files(
                    submit_dir=submit_dir,
                    workflow_id=str(workflow.id),
                    merge_groups=merge_groups,
                    sandbox_url=workflow.sandbox_url,
                    category_throttles=workflow.category_throttles or {
                        "Processing": 5000, "Merge": 100, "Cleanup": 50,
                    },
                    executables=executables,
                )

                total_proc = sum(len(mg.processing_nodes) for mg in merge_groups)
                await session.commit()

                print(f"      proc nodes:  {total_proc}")
                print(f"      merge grps:  {len(merge_groups)}")
                print(f"      dag file:    {dag_file_path}")
                print()
                print("[3/5] Skipping HTCondor submission (dry-run)")
                print("[4/5] Skipping monitoring (dry-run)")
                print()
                print("=== Dry-run complete ===")
                return

            print("[2/5] Planning production DAG + submitting to HTCondor...")
            dag = await dp.plan_production_dag(workflow)
            await session.commit()

            print(f"      dag_id:      {dag.id}")
            print(f"      dag_file:    {dag.dag_file_path}")
            print(f"      cluster_id:  {dag.dagman_cluster_id}")
            print(f"      schedd:      {dag.schedd_name}")
            print(f"      nodes:       {dag.total_nodes}")
            print(f"      work_units:  {dag.total_work_units}")
            print()

            # 3. Monitor
            print(f"[3/5] Monitoring DAG (poll every {args.poll_interval}s)...")
            dm = DAGMonitor(repo, condor)
            terminal = {DAGStatus.COMPLETED, DAGStatus.FAILED, DAGStatus.PARTIAL}

            while True:
                await asyncio.sleep(args.poll_interval)
                dag = await repo.get_dag(dag.id)
                result = await dm.poll_dag(dag)
                await session.commit()

                print(
                    f"  [{result.status.value}] "
                    f"done={result.nodes_done} running={result.nodes_running} "
                    f"idle={result.nodes_idle} failed={result.nodes_failed} "
                    f"held={result.nodes_held}"
                )

                if result.newly_completed_work_units:
                    for wu in result.newly_completed_work_units:
                        print(f"    completed work unit: {wu['group_name']}")

                if result.status in terminal:
                    break

            print()
            print(f"=== DAG finished: {result.status.value} ===")
            print(f"  done={result.nodes_done} failed={result.nodes_failed}")

    finally:
        await engine.dispose()


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "import":
        asyncio.run(run_import(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
