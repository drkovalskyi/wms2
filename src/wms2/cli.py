"""CLI runner: import a real workflow, plan DAG, submit to HTCondor, monitor."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import ssl
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from wms2.adapters.mock import MockDBSAdapter as _MockDBS, MockRucioAdapter as _MockRucio
from wms2.config import Settings
from wms2.core.dag_monitor import DAGMonitor
from wms2.core.dag_planner import DAGPlanner
from wms2.core.error_handler import ErrorHandler
from wms2.core.lifecycle_manager import complete_round
from wms2.core.output_lfn import derive_merged_lfn_bases, determine_merged_lfn_base
from wms2.core.output_manager import OutputManager
from wms2.core.sandbox import create_sandbox
from wms2.core.workflow_manager import WorkflowManager
from wms2.db.engine import create_engine, create_session_factory
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, RequestStatus

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
    imp.add_argument("--submit-dir", default="/mnt/shared/tmp/wms2", help="DAG file output directory")
    imp.add_argument("--max-files", type=int, default=0, help="Limit DBS file query (0=all)")
    imp.add_argument("--files-per-job", type=int, default=None, help="Override splitting param")
    imp.add_argument("--events-per-job", type=int, default=None, help="Override events per job (GEN workflows)")
    imp.add_argument("--dry-run", action="store_true", help="Plan DAG but don't submit")
    imp.add_argument("--sandbox-mode", required=True, choices=["synthetic", "cmssw", "simulator"],
                     help="Sandbox mode: synthetic (sized output), cmssw (real cmsRun), simulator (realistic artifacts)")
    imp.add_argument("--ca-bundle", default=None, help="CA bundle file for CERN Grid verification")
    imp.add_argument("--db-url", default=None, help="Override database URL")
    imp.add_argument("--poll-interval", type=int, default=10, help="Monitoring poll interval (seconds)")
    imp.add_argument("--test-fraction", type=float, default=None,
                     help="Process only this fraction of events per job (e.g. 0.01 = 1%%). "
                          "DAG structure unchanged; jobs finish faster. Not for production.")
    imp.add_argument("--no-monitor", action="store_true",
                     help="Import and submit DAG but skip monitoring (for service mode)")
    imp.add_argument("--merged-lfn-base", default=None,
                     help="Override MergedLFNBase (e.g. /store/mc or /store/data). "
                          "Auto-determined from input dataset if not specified.")
    imp.add_argument("--stageout-mode", required=True, choices=["local", "test", "production"],
                     help="Stageout mode: local (filesystem copy), test (grid, /store/temp/, _Temp RSEs), "
                          "production (grid, /store/, real RSEs)")
    imp.add_argument("--processing-version", type=int, default=None,
                     help="Override ProcessingVersion (changes -vN in output dataset paths)")
    imp.add_argument("--work-units-per-round", type=int, default=None,
                     help="Work units per production round (round 1+). Default: global setting.")
    imp.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    # ── delete subcommand ──
    dele = sub.add_parser("delete", help="Delete a request and all its data from the DB")
    dele.add_argument("request_name", help="Request name to delete")
    dele.add_argument("--db-url", default=None, help="Override database URL")
    dele.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")
    imp.add_argument("--high-priority", type=int, default=5,
                     help="HTCondor job priority for early production rounds (default: 5)")
    imp.add_argument("--nominal-priority", type=int, default=3,
                     help="HTCondor job priority after switch fraction (default: 3)")
    imp.add_argument("--priority-switch-fraction", type=float, default=0.5,
                     help="Progress fraction at which to switch from high to nominal priority (default: 0.5)")

    return parser


def _resolve_cert(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve X.509 cert+key from --proxy or --cert/--key or env vars."""
    if args.proxy:
        return args.proxy, args.proxy
    if args.cert and args.key:
        return args.cert, args.key
    # Check X509_USER_PROXY env var
    env_proxy = os.environ.get("X509_USER_PROXY", "")
    if env_proxy and os.path.exists(env_proxy):
        return env_proxy, env_proxy
    # Default: grid proxy at /tmp/x509up_u$UID
    proxy_path = DEFAULT_PROXY.format(uid=os.getuid())
    if os.path.exists(proxy_path):
        return proxy_path, proxy_path
    raise SystemExit(
        f"No X.509 credential found. Tried: $X509_USER_PROXY, {proxy_path}\n"
        "Use --proxy /path/to/proxy or --cert/--key."
    )


def _make_ssl_context(cert_file: str, key_file: str, ca_path: str) -> ssl.SSLContext:
    """Build an SSL context with CERN Grid CA + client cert.

    Tries ca_path as a directory (capath=) first, then as a file (cafile=).
    Falls back to the system default CA bundle if neither works.
    """
    import os

    if os.path.isdir(ca_path):
        ctx = ssl.create_default_context(capath=ca_path)
    elif os.path.isfile(ca_path):
        ctx = ssl.create_default_context(cafile=ca_path)
    else:
        # Fall back to system defaults
        ctx = ssl.create_default_context()
    ctx.load_cert_chain(cert_file, key_file)
    return ctx


def build_settings(args: argparse.Namespace, cert_file: str, key_file: str) -> Settings:
    """Build Settings from CLI args. Uses wrapper scripts by default."""
    overrides: dict = {
        "x509_proxy": cert_file,
        "condor_host": args.condor_host,
        "submit_base_dir": args.submit_dir,
        "max_input_files": args.max_files,
        "processing_executable": "/bin/true",
        "merge_executable": "/bin/true",
        "cleanup_executable": "/bin/true",
        "log_level": args.log_level,
    }
    if args.ca_bundle:
        overrides["ssl_ca_path"] = args.ca_bundle
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

    proxy = settings.x509_proxy
    reqmgr = ReqMgr2Client(
        settings.reqmgr2_url, proxy, proxy,
        verify=ssl_ctx,
    )
    dbs = DBSClient(
        settings.dbs_url, proxy, proxy,
        verify=ssl_ctx,
    )
    rucio = RucioClient(
        settings.rucio_url, settings.rucio_account,
        proxy, proxy,
        verify=ssl_ctx,
    )
    condor = HTCondorAdapter(
        settings.condor_host, settings.schedd_name,
        sec_token_directory=settings.sec_token_directory,
    )
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

    # Detect GEN workflows (no real input dataset)
    if rtype == "StepChain":
        step1 = reqdata.get("Step1", {})
        if not step1.get("InputDataset") and not step1.get("InputFromOutputModule"):
            reqdata["_is_gen"] = True

    # Default SandboxUrl if missing
    if not reqdata.get("SandboxUrl"):
        reqdata["SandboxUrl"] = "N/A"

    # Build SplittingParams from scattered fields if not present
    if not reqdata.get("SplittingParams"):
        params = {}
        _key_map = {
            "FilesPerJob": "files_per_job",
            "EventsPerJob": "events_per_job",
            "LumisPerJob": "lumis_per_job",
        }
        for key, snake in _key_map.items():
            val = reqdata.get(key)
            if val is not None:
                params[snake] = val
        if params:
            reqdata["SplittingParams"] = params

    return reqdata


def _print_output_files(output_base_dir: str) -> None:
    """Find and display merged output files on disk."""
    import glob

    # Look for all output files (text and ROOT)
    txt_files = sorted(glob.glob(os.path.join(output_base_dir, "**", "merged.txt"), recursive=True))
    root_files = sorted(glob.glob(os.path.join(output_base_dir, "**", "*.root"), recursive=True))
    files = txt_files + root_files
    if not files:
        print("[5/5] No merged output files found on disk")
        return

    print(f"[5/5] Merged output files ({len(files)} files):")
    for f in files:
        size = os.path.getsize(f)
        rel = os.path.relpath(f, output_base_dir)
        print(f"  {rel}  ({size} bytes)")


async def _transition_request(
    repo: Repository, request_name: str, old_status: str, new_status: RequestStatus
) -> None:
    """Record a state transition on a request (same pattern as LifecycleManager.transition)."""
    now = datetime.now(timezone.utc)
    request = await repo.get_request(request_name)
    old_transitions = (request.status_transitions or []) if request else []
    new_transition = {
        "from": old_status,
        "to": new_status.value,
        "timestamp": now.isoformat(),
    }
    await repo.update_request(
        request_name,
        status=new_status.value,
        status_transitions=old_transitions + [new_transition],
    )
    logger.info("Request %s: %s -> %s", request_name, old_status, new_status.value)


def _print_error_summary(post_data: list[dict]) -> None:
    """Print failure categories, problem sites, and sample error messages."""
    if not post_data:
        print("  No POST data found (DAGMan may have died before jobs ran)")
        return

    # Aggregate by category
    categories: dict[str, int] = defaultdict(int)
    sites: dict[str, int] = defaultdict(int)
    sample_messages: list[str] = []
    for entry in post_data:
        cat = entry.get("classification", {}).get("category", "unknown")
        if cat != "success":
            categories[cat] += 1
        site = entry.get("job", {}).get("site", "unknown")
        if cat != "success":
            sites[site] += 1
        msg = entry.get("cmssw", {}).get("error_message", "")
        if msg and len(sample_messages) < 3:
            sample_messages.append(msg[:200])

    print("  Failure categories:")
    for cat, count in sorted(categories.items()):
        print(f"    {cat} = {count}")
    if sites:
        print("  Problem sites:")
        for site, count in sorted(sites.items(), key=lambda x: -x[1]):
            print(f"    {site} = {count}")
    if sample_messages:
        print("  Sample error messages:")
        for msg in sample_messages:
            print(f"    {msg}")


async def run_import(args: argparse.Namespace) -> None:
    cert_file, key_file = _resolve_cert(args)
    settings = build_settings(args, cert_file, key_file)
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )

    request_name = args.request_name
    dry_run = args.dry_run

    sandbox_mode = args.sandbox_mode

    print(f"=== WMS2 CLI: importing {request_name} ===")
    print(f"  cert:          {cert_file}")
    print(f"  condor_host:   {settings.condor_host}")
    print(f"  submit_dir:    {settings.submit_base_dir}")
    print(f"  max_files:     {settings.max_input_files or 'all'}")
    print(f"  sandbox_mode:  {sandbox_mode}")
    print(f"  dry_run:       {dry_run}")
    if args.test_fraction is not None:
        print(f"  test_fraction: {args.test_fraction}")
        print("  *** TEST MODE: each job will process only "
              f"{args.test_fraction*100:.1f}% of nominal events ***")
    print()

    # SSL context for CERN Grid services
    ssl_ctx = _make_ssl_context(cert_file, key_file, settings.ssl_ca_path)

    # Database
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    # Set RUCIO_HOME for native rucio-clients (pileup file queries)
    os.environ.setdefault("RUCIO_HOME", settings.rucio_home)

    # Adapters
    reqmgr, dbs, rucio, condor = _build_adapters(settings, ssl_ctx)

    try:
        async with session_factory() as session:
            repo = Repository(session)

            # 1. Fetch from ReqMgr2
            print("[1/5] Fetching request from ReqMgr2...")
            reqdata = await reqmgr.get_request(request_name)
            reqdata = _normalize_request(reqdata)

            # Override processing version if requested
            if args.processing_version is not None:
                from wms2.api.import_endpoint import _apply_processing_version
                _apply_processing_version(reqdata, args.processing_version)
                print(f"      proc_ver:    v{args.processing_version} (CLI override)")

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
            # Determine MergedLFNBase based on stageout mode
            stageout_mode = args.stageout_mode
            if args.merged_lfn_base:
                merged_lfn_base = args.merged_lfn_base
                print(f"      lfn_base:    {merged_lfn_base} (CLI override)")
            elif stageout_mode == "test":
                if not settings.test_lfn_user:
                    print("ERROR: WMS2_TEST_LFN_USER must be set for test stageout mode")
                    sys.exit(1)
                merged_lfn_base = f"/store/temp/user/{settings.test_lfn_user}.merged"
                reqdata["UnmergedLFNBase"] = f"/store/temp/user/{settings.test_lfn_user}.unmerged"
                print(f"      lfn_base:    {merged_lfn_base} (test mode)")
            else:
                merged_lfn_base = await determine_merged_lfn_base(reqdata, dbs_adapter=dbs)
                print(f"      lfn_base:    {merged_lfn_base} (auto-determined)")
            reqdata["MergedLFNBase"] = merged_lfn_base

            output_datasets_info = derive_merged_lfn_bases(reqdata)
            config_data = {
                "campaign": reqdata.get("Campaign"),
                "requestor": reqdata.get("Requestor"),
                "priority": reqdata.get("RequestPriority"),
                "request_type": reqdata.get("RequestType"),
                "output_datasets": output_datasets_info,
                "merged_lfn_base": merged_lfn_base,
                "unmerged_lfn_base": reqdata.get("UnmergedLFNBase", "/store/unmerged"),
                # CMSSW metadata
                "cmssw_version": reqdata.get("CMSSWVersion"),
                "scram_arch": reqdata.get("ScramArch"),
                "global_tag": reqdata.get("GlobalTag"),
                "memory_mb": reqdata.get("Memory", 2048),
                "multicore": reqdata.get("Multicore", 1),
                "time_per_event": reqdata.get("TimePerEvent", 1.0),
                "size_per_event": reqdata.get("SizePerEvent", 1.5),
                "filter_efficiency": reqdata.get("FilterEfficiency", 1.0),
            }
            if reqdata.get("_is_gen"):
                config_data["_is_gen"] = True
                config_data["request_num_events"] = reqdata.get("RequestNumEvents", 0)
            if args.test_fraction is not None:
                config_data["test_fraction"] = args.test_fraction

            # Stageout mode and condor pool
            config_data["stageout_mode"] = stageout_mode
            config_data["condor_pool"] = "local" if stageout_mode == "local" else "global"
            if stageout_mode == "test":
                config_data["consolidation_rse"] = "T2_CH_CERN_Temp"
                config_data["rucio_test_account"] = settings.rucio_test_account
            if args.processing_version is not None:
                config_data["processing_version"] = args.processing_version
            if args.work_units_per_round is not None:
                config_data["work_units_per_round"] = args.work_units_per_round

            # Priority profile
            config_data["priority_profile"] = {
                "pilot": settings.default_pilot_priority,
                "high": args.high_priority,
                "nominal": args.nominal_priority,
                "switch_fraction": args.priority_switch_fraction,
            }

            # Create sandbox
            submit_dir = os.path.join(settings.submit_base_dir, request_name)
            os.makedirs(submit_dir, exist_ok=True)
            sandbox_path = os.path.join(submit_dir, "sandbox.tar.gz")
            create_sandbox(sandbox_path, reqdata, mode=sandbox_mode,
                          ssl_context=ssl_ctx, configcache_url=settings.configcache_url)
            config_data["sandbox_path"] = sandbox_path

            # Extract manifest steps (mc_pileup/data_pileup) for pileup resolution
            if reqdata.get("RequestType") == "StepChain" and reqdata.get("StepChain"):
                from dataclasses import asdict as _asdict
                from wms2.core.stepchain import parse_stepchain
                _spec = parse_stepchain(reqdata)
                config_data["manifest_steps"] = [_asdict(s) for s in _spec.steps]
                # Use parsed filter_efficiency (handles Step1 fallback)
                config_data["filter_efficiency"] = _spec.filter_efficiency

            # Print CMSSW info if available
            cmssw_ver = reqdata.get("CMSSWVersion")
            if cmssw_ver:
                print(f"      cmssw:       {cmssw_ver}")
                print(f"      scram_arch:  {reqdata.get('ScramArch', 'N/A')}")
                print(f"      global_tag:  {reqdata.get('GlobalTag', 'N/A')}")
            print(f"      sandbox:     {sandbox_path} ({sandbox_mode})")
            workflow = await repo.create_workflow(
                request_name=request_name,
                input_dataset=reqdata["InputDataset"],
                splitting_algo=reqdata["SplittingAlgo"],
                splitting_params=reqdata.get("SplittingParams", {}),
                sandbox_url=reqdata.get("SandboxUrl", "N/A"),
                config_data=config_data,
            )
            if output_datasets_info:
                print(f"      outputs:     {len(output_datasets_info)} datasets")
                for ods in output_datasets_info:
                    print(f"                   {ods['dataset_name']}")
            else:
                print("      outputs:     (none detected from request)")

            # Override splitting_params if --files-per-job or --events-per-job given
            if args.files_per_job is not None or args.events_per_job is not None:
                params = workflow.splitting_params or {}
                if args.files_per_job is not None:
                    params["files_per_job"] = args.files_per_job
                if args.events_per_job is not None:
                    params["events_per_job"] = args.events_per_job
                await repo.update_workflow(workflow.id, splitting_params=params)
                workflow = await repo.get_workflow(workflow.id)

            await session.commit()
            print(f"      workflow_id: {workflow.id}")
            print()

            # 2. Plan production DAG
            dp = DAGPlanner(repo, dbs, rucio, condor, settings)

            # When condor_pool=global, the CLI lacks spool env vars
            # (WMS2_SPOOL_MOUNT, WMS2_REMOTE_SPOOL_PREFIX, WMS2_REMOTE_SCHEDD).
            # Leave request as "queued" and let the lifecycle manager (which has
            # the correct env) handle DAG planning + spool-mode submission.
            if config_data.get("condor_pool") == "global" and not settings.spool_mount:
                await _transition_request(repo, request_name, "submitted", RequestStatus.QUEUED)
                await session.commit()
                print("[2/5] Global pool — skipping local DAG submission")
                print("      Lifecycle manager will plan and submit with spool mode.")
                print()
                print("=== Request queued — lifecycle manager will handle submission ===")
                return

            if dry_run:
                print("[2/5] Planning production DAG (dry-run, no HTCondor)...")
                # Use a mock condor adapter for dry-run so plan_production_dag
                # writes DAG files but doesn't actually submit
                from wms2.adapters.mock import MockCondorAdapter as _MockCondor

                dp_dry = DAGPlanner(repo, dbs, rucio, _MockCondor(), settings)
                dag = await dp_dry.plan_production_dag(workflow, adaptive=True)
                await session.commit()

                print(f"      dag_file:    {dag.dag_file_path}")
                print(f"      nodes:       {dag.total_nodes}")
                print(f"      work_units:  {dag.total_work_units}")
                print()
                print("[3/5] Skipping HTCondor submission (dry-run)")
                print("[4/5] Skipping monitoring (dry-run)")
                print()
                print("=== Dry-run complete ===")
                return

            print("[2/5] Planning production DAG + submitting to HTCondor...")
            dag = await dp.plan_production_dag(workflow, adaptive=True)
            await session.commit()

            print(f"      dag_id:      {dag.id}")
            print(f"      dag_file:    {dag.dag_file_path}")
            print(f"      cluster_id:  {dag.dagman_cluster_id}")
            print(f"      schedd:      {dag.schedd_name}")
            print(f"      nodes:       {dag.total_nodes}")
            print(f"      work_units:  {dag.total_work_units}")
            current_round = getattr(workflow, "current_round", 0) or 0
            print(f"      round:       {current_round}")
            print()

            # Transition request to ACTIVE — a DAG is now running
            await _transition_request(repo, request_name, "submitted", RequestStatus.ACTIVE)
            await session.commit()

            if args.no_monitor:
                print("[3/5] Skipping monitoring (--no-monitor)")
                print("[4/5] Skipping output summary (--no-monitor)")
                print("[5/5] Skipping output files (--no-monitor)")
                print()
                print(f"=== DAG submitted — lifecycle manager will handle monitoring ===")
                return

            # 3. Monitor (with rescue loop + multi-round adaptive lifecycle)
            print(f"[3/5] Monitoring DAG (poll every {args.poll_interval}s)...")
            dm = DAGMonitor(repo, condor, settings=settings)
            # OutputManager with mock DBS/Rucio for output lifecycle
            om = OutputManager(repo, _MockDBS(), _MockRucio())
            eh = ErrorHandler(repo, condor, settings)
            terminal = {DAGStatus.COMPLETED, DAGStatus.FAILED, DAGStatus.PARTIAL}
            held = False  # break out of both loops on HELD

            while True:  # outer loop: rounds
                while True:  # inner loop: poll + rescue within a round
                    await asyncio.sleep(args.poll_interval)
                    dag = await repo.get_dag(dag.id)
                    workflow = await repo.get_workflow(workflow.id)
                    result = await dm.poll_dag(dag)

                    # Process completed work units through output manager
                    if result.newly_completed_work_units:
                        blocks = await repo.get_processing_blocks(workflow.id)
                        for wu in result.newly_completed_work_units:
                            print(f"    completed work unit: {wu['group_name']}")
                            manifest = wu.get("manifest") or {}
                            datasets_info = manifest.get("datasets", {})
                            for block in blocks:
                                ds_info = datasets_info.get(block.dataset_name, {})
                                await om.handle_work_unit_completion(
                                    workflow.id, block.id, {
                                        "output_files": ds_info.get("files", []),
                                        "site": manifest.get("site", "local"),
                                        "node_name": wu["group_name"],
                                    }
                                )
                        await om.process_blocks_for_workflow(workflow.id)

                    await session.commit()

                    print(
                        f"  [{result.status.value}] "
                        f"done={result.nodes_done} running={result.nodes_running} "
                        f"idle={result.nodes_idle} failed={result.nodes_failed} "
                        f"held={result.nodes_held}"
                    )

                    if result.status not in terminal:
                        continue

                    # Process any final outputs
                    if result.newly_completed_work_units:
                        await om.process_blocks_for_workflow(workflow.id)
                        await session.commit()

                    # ── Terminal status handling ──

                    if result.status == DAGStatus.COMPLETED:
                        break  # exit inner loop → handle round completion

                    # PARTIAL or FAILED — invoke error handler
                    dag = await repo.get_dag(dag.id)
                    request = await repo.get_request(request_name)
                    workflow = await repo.get_workflow(workflow.id)

                    from wms2.core.error_handler import CompletionResult
                    result = await eh.handle_dag_completion(dag, request, workflow)
                    await session.commit()

                    if result.action == "rescue":
                        # ErrorHandler._prepare_rescue() already created the rescue DAG
                        # record and pointed the workflow at it. Submit it.
                        workflow = await repo.get_workflow(workflow.id)
                        rescue_dag = await repo.get_dag(workflow.dag_id)

                        if result.problem_sites:
                            from wms2.core.error_handler import ErrorHandler as EH
                            EH.apply_site_exclusions(dag.submit_dir, result.problem_sites)
                            print(f"  Excluded sites from rescue: {', '.join(result.problem_sites)}")

                        print(f"\n  Submitting rescue DAG (attempt from {rescue_dag.rescue_dag_path})...")
                        # Submit the *original* DAG file — DAGMan's AutoRescue
                        # finds and applies the rescue file automatically.
                        cluster_id, schedd = await condor.submit_dag(
                            rescue_dag.dag_file_path,
                            force=True,
                        )
                        await repo.update_dag(
                            rescue_dag.id,
                            dagman_cluster_id=cluster_id,
                            schedd_name=schedd,
                            status=DAGStatus.SUBMITTED.value,
                        )
                        await _transition_request(repo, request_name, "active", RequestStatus.RESUBMITTING)
                        await _transition_request(repo, request_name, "resubmitting", RequestStatus.ACTIVE)
                        await session.commit()

                        # Update dag reference for the monitoring loop
                        dag = rescue_dag
                        print(f"  Rescue DAG submitted: cluster_id={cluster_id}")
                        print(f"  Continuing monitoring...\n")
                        continue  # re-enter inner monitoring loop

                    # result.action == "hold"
                    await _transition_request(repo, request_name, "active", RequestStatus.HELD)
                    await session.commit()

                    print(f"\n=== Request HELD — operator intervention required ===")
                    post_data = eh.read_post_data(dag.submit_dir)
                    _print_error_summary(post_data)
                    held = True
                    break  # exit inner loop

                if held:
                    break  # exit outer round loop

                # ── Round completed successfully — advance to next round ──
                workflow = await repo.get_workflow(workflow.id)
                current_round = getattr(workflow, "current_round", 0) or 0

                result = await complete_round(repo, settings, workflow, dag)
                await session.commit()

                next_round = result["new_round"]
                proc_jobs = result["proc_jobs"]
                events_from_wus = result["events_from_wus"]
                adaptive_params = result["adaptive_params"]

                print(f"\n  Round {current_round} complete ({proc_jobs} proc jobs, {events_from_wus} events).")
                if adaptive_params:
                    print(f"  Adaptive: tuned_memory_mb={adaptive_params.get('tuned_memory_mb', '?')}")
                print(f"  Advancing to round {next_round}...")

                # Plan next round
                workflow = await repo.get_workflow(workflow.id)
                next_dag = await dp.plan_production_dag(workflow, adaptive=True)

                if next_dag is None:
                    # All work done
                    print(f"  All work consumed — no more rounds needed.")
                    await _transition_request(repo, request_name, "active", RequestStatus.COMPLETED)
                    await repo.update_workflow(workflow.id, status="completed")
                    await session.commit()
                    break

                await session.commit()
                dag = next_dag
                print(f"  Round {next_round}: dag_id={dag.id}, "
                      f"nodes={dag.total_nodes}, work_units={dag.total_work_units}")
                print(f"  Continuing monitoring...\n")
                # continue outer loop → monitor the new DAG

            # Print output summary
            print()
            blocks = await repo.get_processing_blocks(workflow.id)
            if blocks:
                print(f"[4/5] Output summary ({len(blocks)} blocks):")
                for block in blocks:
                    wus = len(block.completed_work_units or [])
                    print(f"  {block.dataset_name}: {block.status} "
                          f"({wus}/{block.total_work_units} work units)")
            else:
                print("[4/5] No processing blocks created")

            # List merged output files on disk
            print()
            _print_output_files(settings.local_pfn_prefix)

            # Final status line
            request = await repo.get_request(request_name)
            final_status = request.status if request else "unknown"
            dag = await repo.get_dag(dag.id)
            dag_status = dag.status if dag else "unknown"
            print()
            print(f"=== DAG finished: {dag_status} | request: {final_status} ===")

    finally:
        await engine.dispose()


async def run_delete(args: argparse.Namespace) -> None:
    """Delete a request and all associated data (workflow, DAGs, blocks)."""
    overrides = {}
    if args.db_url:
        overrides["database_url"] = args.db_url
    settings = Settings(**overrides)
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)

    try:
        async with session_factory() as session:
            repo = Repository(session)
            existing = await repo.get_request(args.request_name)
            if not existing:
                print(f"Request not found: {args.request_name}")
                sys.exit(1)

            # Show what will be deleted
            workflow = await repo.get_workflow_by_request(args.request_name)
            dag_count = 0
            if workflow:
                dags = await repo.list_dags(workflow_id=workflow.id)
                dag_count = len(dags)
            print(f"Request:  {existing.request_name}")
            print(f"Status:   {existing.status}")
            if workflow:
                print(f"Workflow: {workflow.id} (round {workflow.current_round})")
            print(f"DAGs:     {dag_count}")

            if not args.yes:
                answer = input("\nDelete this request and all its data? [y/N] ")
                if answer.lower() not in ("y", "yes"):
                    print("Cancelled.")
                    return

            # Cascading delete
            if workflow:
                for dag in await repo.list_dags(workflow_id=workflow.id):
                    await repo.delete_dag_history(dag.id)
                    await repo.delete_dag(dag.id)
                for block in await repo.get_processing_blocks(workflow.id):
                    await repo.delete_processing_block(block.id)
                await repo.delete_workflow(workflow.id)
            await repo.delete_request(args.request_name)
            await session.commit()
            print(f"Deleted request {args.request_name}")
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
    elif args.command == "delete":
        asyncio.run(run_delete(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
