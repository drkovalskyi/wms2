import asyncio
import json
import logging
import os
from datetime import datetime, timezone

from wms2.adapters.base import CondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, RequestStatus, WorkflowStatus

logger = logging.getLogger(__name__)


def _aggregate_round_metrics(existing_metrics, dag, round_number,
                              wu_metrics_list=None):
    """Merge this round's DAG stats and WU performance data into step_metrics.

    When wu_metrics_list is provided (list of work_unit_metrics.json dicts),
    store them under a 'rounds' key keyed by round number. This preserves
    real per-step performance data that the adaptive algorithm needs.
    """
    prior = existing_metrics or {}
    result = {
        **prior,
        "rounds_completed": round_number + 1,
        "cumulative_nodes_done": prior.get("cumulative_nodes_done", 0) + (dag.nodes_done or 0),
        "cumulative_nodes_failed": prior.get("cumulative_nodes_failed", 0) + (dag.nodes_failed or 0),
        "last_round_nodes_done": dag.nodes_done or 0,
        "last_round_work_units": dag.total_work_units or 0,
    }

    # Store WU-level performance data for adaptive analysis
    if wu_metrics_list:
        rounds = prior.get("rounds", {})
        rounds[str(round_number)] = {
            "wu_metrics": wu_metrics_list,
            "nodes_done": dag.nodes_done or 0,
            "nodes_failed": dag.nodes_failed or 0,
            "work_units": dag.total_work_units or 0,
        }
        result["rounds"] = rounds

    return result


def _count_events_from_disk(submit_dir, completed_wus):
    """Read output events from work_unit_metrics.json on disk.

    Safety net for when events_produced is 0 in the DB despite work
    units having completed (session timing / commit ordering issue).
    """
    total = 0
    for wu_name in (completed_wus or []):
        metrics_path = os.path.join(submit_dir, wu_name, "work_unit_metrics.json")
        if not os.path.exists(metrics_path):
            continue
        try:
            wu_metrics = json.load(open(metrics_path))
            per_step = wu_metrics.get("per_step", {})
            if not per_step:
                continue
            last_key = max(per_step.keys(), key=lambda k: int(k))
            last_step = per_step[last_key]
            ew = last_step.get("events_written")
            if ew and isinstance(ew, dict):
                total += ew.get("total", 0)
            else:
                ep = last_step.get("events_processed")
                if ep and isinstance(ep, dict):
                    total += ep.get("total", 0)
        except Exception:
            logger.warning("Failed to read WU metrics: %s", metrics_path)
    return total


def _compute_adaptive_params(config, dag, workflow, new_metrics, settings):
    """Run adaptive optimization if metrics are available.

    Returns adaptive_params dict or None if optimization can't run.
    """
    if not dag.submit_dir or not dag.completed_work_units:
        return None

    original_nthreads = int(config.get("multicore", 0))
    if original_nthreads <= 0:
        return None

    try:
        from wms2.core.adaptive import compute_round_optimization
    except ImportError:
        logger.warning("wms2.core.adaptive not available — skipping optimization")
        return None

    params = workflow.splitting_params or {}
    events_per_job = (params.get("events_per_job")
                      or params.get("eventsPerJob") or 0)

    try:
        return compute_round_optimization(
            submit_dir=dag.submit_dir,
            completed_wus=list(dag.completed_work_units),
            original_nthreads=original_nthreads,
            request_cpus=original_nthreads,
            default_memory_per_core=settings.default_memory_per_core,
            max_memory_per_core=settings.max_memory_per_core,
            safety_margin=settings.safety_margin,
            adaptive_mode=settings.adaptive_mode,
            events_per_job=events_per_job,
            jobs_per_wu=settings.jobs_per_work_unit,
        )
    except Exception:
        logger.exception("Adaptive optimization failed — using defaults")
        return None


async def complete_round(repo, settings, workflow, dag):
    """Shared round-completion logic for CLI and lifecycle manager.

    Reads WU metrics from disk, counts output events, aggregates step_metrics,
    runs adaptive optimization, advances the round offset, and updates the
    workflow in the DB.

    Returns a dict with:
      - new_round: int
      - events_from_wus: int
      - step_metrics: dict (enriched)
      - adaptive_params: dict | None
      - proc_jobs: int
    """
    config = workflow.config_data or {}
    is_gen = config.get("_is_gen", False)
    params = workflow.splitting_params or {}
    current_round = getattr(workflow, "current_round", 0) or 0

    # ── Count proc jobs ──
    node_counts = dag.node_counts or {}
    proc_jobs = node_counts.get("processing", dag.nodes_done)

    # ── events_produced fix: read from disk if DB shows 0 ──
    completed_wus = dag.completed_work_units or []
    events_from_wus = 0
    if is_gen and (workflow.events_produced or 0) == 0 and completed_wus:
        events_from_wus = _count_events_from_disk(dag.submit_dir, completed_wus)
        if events_from_wus > 0:
            logger.info(
                "Workflow %s: events_produced=0 in DB but %d from disk — fixing",
                workflow.request_name, events_from_wus,
            )
            await repo.update_workflow(
                workflow.id, events_produced=events_from_wus
            )

    # ── Collect WU metrics from disk ──
    wu_metrics_list = []
    if dag.submit_dir and completed_wus:
        for wu_name in completed_wus:
            metrics_path = os.path.join(
                dag.submit_dir, wu_name, "work_unit_metrics.json"
            )
            if os.path.exists(metrics_path):
                try:
                    wu_metrics_list.append(json.load(open(metrics_path)))
                except Exception:
                    logger.warning("Failed to read WU metrics: %s", metrics_path)

    # ── Aggregate step_metrics ──
    new_metrics = _aggregate_round_metrics(
        workflow.step_metrics, dag, current_round,
        wu_metrics_list=wu_metrics_list or None,
    )

    # ── Adaptive optimization ──
    adaptive_params = _compute_adaptive_params(
        config, dag, workflow, new_metrics, settings
    )
    if adaptive_params:
        new_metrics["adaptive_params"] = adaptive_params
        logger.info(
            "Workflow %s: adaptive optimization — nthreads=%d, memory=%d MB, "
            "mode=%s, cpu_eff=%.1f%%",
            workflow.request_name,
            adaptive_params.get("tuned_nthreads", 0),
            adaptive_params.get("tuned_memory_mb", 0),
            adaptive_params.get("mode", "?"),
            (adaptive_params.get("metrics_summary", {}) or {}).get(
                "weighted_cpu_eff", 0
            ) * 100,
        )

    # ── Advance offset and round ──
    new_round = current_round + 1
    update_kwargs = {
        "current_round": new_round,
        "step_metrics": new_metrics,
    }
    if is_gen:
        events_per_job = (params.get("events_per_job")
                          or params.get("eventsPerJob") or 100_000)
        new_offset = (workflow.next_first_event or 1) + proc_jobs * events_per_job
        update_kwargs["next_first_event"] = new_offset
    else:
        files_per_job = (params.get("files_per_job")
                         or params.get("filesPerJob") or 1)
        new_offset = (workflow.file_offset or 0) + proc_jobs * files_per_job
        update_kwargs["file_offset"] = new_offset

    await repo.update_workflow(workflow.id, **update_kwargs)

    return {
        "new_round": new_round,
        "events_from_wus": events_from_wus,
        "step_metrics": new_metrics,
        "adaptive_params": adaptive_params,
        "proc_jobs": proc_jobs,
    }


class RequestLifecycleManager:
    """
    Single owner of the request state machine. Runs a continuous loop that
    evaluates all non-terminal requests and dispatches work to the appropriate
    component workers.

    Accepts a session_factory and creates a fresh DB session per cycle so that:
    - Each cycle sees the latest DB state (including API-injected requests)
    - Changes are committed after each request evaluation
    - Crashes don't lose previously committed state

    Adapter instances (condor, reqmgr, dbs, rucio, cric) are long-lived and
    shared across cycles. Worker components (DAGMonitor, DAGPlanner, etc.)
    are rebuilt per-cycle with the fresh repository.
    """

    def __init__(
        self,
        session_factory_or_repo=None,
        condor_adapter: CondorAdapter = None,
        settings: Settings = None,
        *,
        # Service mode: pass adapters, workers built per-cycle
        reqmgr=None,
        dbs=None,
        rucio=None,
        cric=None,
        # Test/legacy mode: pass pre-built workers directly
        repository=None,
        workflow_manager=None,
        dag_planner=None,
        dag_monitor=None,
        output_manager=None,
        error_handler=None,
    ):
        self.condor = condor_adapter
        self.settings = settings

        # Resolve: keyword `repository=` forces repo mode; otherwise check
        # if the first positional arg is a session_factory (async_sessionmaker)
        # or a repository-like object (has get_request method).
        if repository is not None:
            # Explicit keyword: always repo mode
            self.session_factory = None
            self.db = repository
        elif session_factory_or_repo is not None and hasattr(
            session_factory_or_repo, "get_request"
        ):
            # Repository-like (real Repository or MagicMock with get_request)
            self.session_factory = None
            self.db = session_factory_or_repo
        else:
            # Service mode: session_factory (async_sessionmaker) passed
            self.session_factory = session_factory_or_repo
            self.db = None

        self.reqmgr = reqmgr
        self.dbs = dbs
        self.rucio = rucio
        self.cric = cric
        self.workflow_manager = workflow_manager
        self.dag_planner = dag_planner
        self.dag_monitor = dag_monitor
        self.output_manager = output_manager
        self.error_handler = error_handler

        self.status_timeouts = {
            RequestStatus.SUBMITTED: settings.timeout_submitted,
            RequestStatus.QUEUED: settings.timeout_queued,
            RequestStatus.PILOT_RUNNING: settings.timeout_pilot_running,
            RequestStatus.PLANNING: settings.timeout_planning,
            RequestStatus.ACTIVE: settings.timeout_active,
            RequestStatus.STOPPING: settings.timeout_stopping,
            RequestStatus.RESUBMITTING: settings.timeout_resubmitting,
        }

        self._dispatch = {
            RequestStatus.SUBMITTED: self._handle_submitted,
            RequestStatus.QUEUED: self._handle_queued,
            RequestStatus.PILOT_RUNNING: self._handle_pilot_running,
            RequestStatus.ACTIVE: self._handle_active,
            RequestStatus.STOPPING: self._handle_stopping,
            RequestStatus.RESUBMITTING: self._handle_resubmitting,
            RequestStatus.HELD: self._handle_held,
            RequestStatus.PARTIAL: self._handle_partial,
        }

    def _build_workers(self, repo: Repository):
        """Build per-cycle worker components with a fresh repository."""
        from wms2.core.dag_monitor import DAGMonitor
        from wms2.core.dag_planner import DAGPlanner
        from wms2.core.error_handler import ErrorHandler
        from wms2.core.output_manager import OutputManager
        from wms2.core.site_manager import SiteManager
        from wms2.core.workflow_manager import WorkflowManager

        sm = SiteManager(repo, self.settings, cric_adapter=self.cric)
        self.db = repo
        self.workflow_manager = WorkflowManager(repo, self.reqmgr) if self.reqmgr else None
        self.dag_planner = DAGPlanner(
            repo, self.dbs, self.rucio, self.condor, self.settings, site_manager=sm,
        )
        self.dag_monitor = DAGMonitor(repo, self.condor)
        self.output_manager = OutputManager(repo, self.dbs, self.rucio)
        self.error_handler = ErrorHandler(repo, self.condor, self.settings, site_manager=sm)

    # ── Main Loop ───────────────────────────────────────────────

    async def main_loop(self):
        """Main loop: create fresh session per cycle, evaluate all requests."""
        while True:
            try:
                async with self.session_factory() as session:
                    repo = Repository(session)
                    self._build_workers(repo)

                    requests = await repo.get_non_terminal_requests()
                    for request in requests:
                        req_name = request.request_name  # capture before try
                        try:
                            await self.evaluate_request(request)
                            await session.commit()
                        except Exception:
                            logger.exception("Error evaluating %s", req_name)
                            await session.rollback()
                            # After rollback, ORM objects in the requests list
                            # are expired. Accessing their attributes would
                            # trigger a lazy load that fails with
                            # MissingGreenlet. Break and restart the cycle.
                            break

                    if not requests:
                        logger.debug("No non-terminal requests")

                await asyncio.sleep(self.settings.lifecycle_cycle_interval)
            except asyncio.CancelledError:
                logger.info("Lifecycle manager shutting down")
                break
            except Exception:
                logger.exception("Lifecycle manager cycle error")
                await asyncio.sleep(self.settings.lifecycle_cycle_interval)

    async def evaluate_request(self, request):
        """Match on current status, dispatch to the appropriate handler."""
        status = RequestStatus(request.status)

        if self._is_stuck(request):
            await self._handle_stuck(request)
            return

        handler = self._dispatch.get(status)
        if handler:
            await handler(request)

    # ── State Handlers ──────────────────────────────────────────

    async def _handle_submitted(self, request):
        """Validate request and move to admission queue."""
        if self.workflow_manager is None:
            logger.debug("Skipping _handle_submitted: workflow_manager not available")
            return
        await self.workflow_manager.import_request(request.request_name)
        await self.transition(request, RequestStatus.QUEUED)

    async def _handle_queued(self, request):
        """Check admission capacity and start pilot or planning."""
        if self.dag_planner is None:
            logger.debug("Skipping _handle_queued: dag_planner not available")
            return

        active_count = await self.db.count_active_dags()
        if active_count >= self.settings.max_active_dags:
            return

        next_pending = await self.db.get_queued_requests(limit=1)
        if next_pending and next_pending[0].request_name != request.request_name:
            return

        workflow = await self.db.get_workflow_by_request(request.request_name)
        if not workflow:
            return

        # Check for rescue DAG re-admission
        if workflow.dag_id:
            dag = await self.db.get_dag(workflow.dag_id)
            if dag and dag.rescue_dag_path and dag.status == DAGStatus.READY.value:
                cluster_id, schedd = await self.condor.submit_dag(
                    dag.rescue_dag_path or dag.dag_file_path
                )
                await self.db.update_dag(
                    dag.id,
                    dagman_cluster_id=cluster_id,
                    schedd_name=schedd,
                    status=DAGStatus.SUBMITTED.value,
                )
                await self.transition(request, RequestStatus.ACTIVE)
                return

        current_round = getattr(workflow, "current_round", 0) or 0
        is_adaptive = getattr(request, "adaptive", False)

        if current_round > 0:
            # Round 2+: skip pilot, go straight to production (always adaptive)
            dag = await self.dag_planner.plan_production_dag(workflow, adaptive=True)
            if dag is None:
                await self.transition(request, RequestStatus.COMPLETED)
            else:
                await self.transition(request, RequestStatus.ACTIVE)
        elif request.urgent:
            # Skip pilot, go straight to production DAG
            dag = await self.dag_planner.plan_production_dag(
                workflow, adaptive=is_adaptive,
            )
            if is_adaptive and dag is None:
                await self.transition(request, RequestStatus.COMPLETED)
            else:
                await self.transition(request, RequestStatus.ACTIVE)
        else:
            await self.dag_planner.submit_pilot(workflow)
            await self.transition(request, RequestStatus.PILOT_RUNNING)

    async def _handle_pilot_running(self, request):
        """Poll pilot status, trigger DAG planning on completion."""
        if self.dag_planner is None:
            logger.debug("Skipping _handle_pilot_running: dag_planner not available")
            return

        workflow = await self.db.get_workflow_by_request(request.request_name)
        if not workflow or not workflow.pilot_cluster_id:
            return

        completed = await self.condor.check_job_completed(
            workflow.pilot_cluster_id, workflow.pilot_schedd
        )
        if completed:
            is_adaptive = getattr(request, "adaptive", False)
            report_path = os.path.join(
                workflow.pilot_output_path or "", "pilot_metrics.json"
            )
            if os.path.exists(report_path):
                await self.dag_planner.handle_pilot_completion(
                    workflow, report_path, adaptive=is_adaptive,
                )
            else:
                # No pilot metrics — plan with defaults
                await self.dag_planner.plan_production_dag(
                    workflow, adaptive=is_adaptive,
                )
            await self.transition(request, RequestStatus.ACTIVE)

    async def _handle_active(self, request):
        """Poll DAG status, process outputs."""
        if self.dag_monitor is None:
            logger.debug("Skipping _handle_active: dag_monitor not available")
            return

        workflow = await self.db.get_workflow_by_request(request.request_name)
        if not workflow or not workflow.dag_id:
            return

        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        if dag.status in (DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value):
            result = await self.dag_monitor.poll_dag(dag)

            # Process completed work units through output manager
            if result.newly_completed_work_units and self.output_manager:
                try:
                    blocks = await self.db.get_processing_blocks(workflow.id)
                    for wu in result.newly_completed_work_units:
                        manifest = wu.get("manifest") or {}
                        datasets_info = manifest.get("datasets", {})
                        for block in blocks:
                            ds_info = datasets_info.get(block.dataset_name, {})
                            await self.output_manager.handle_work_unit_completion(
                                workflow.id, block.id, {
                                    "output_files": ds_info.get("files", []),
                                    "site": manifest.get("site", "local"),
                                    "node_name": wu["group_name"],
                                }
                            )
                except Exception:
                    logger.warning(
                        "Output registration failed for %s — will retry next cycle",
                        request.request_name, exc_info=True,
                    )

            # Accumulate production counters from completed work units
            if result.newly_completed_work_units:
                total_new_events = sum(
                    wu.get("output_events", 0) for wu in result.newly_completed_work_units
                )
                if total_new_events > 0:
                    wf = await self.db.get_workflow_by_request(request.request_name)
                    if wf:
                        await self.db.update_workflow(
                            wf.id,
                            events_produced=(wf.events_produced or 0) + total_new_events,
                        )

            # Every cycle: retry failed Rucio calls
            if self.output_manager:
                try:
                    await self.output_manager.process_blocks_for_workflow(workflow.id)
                except Exception:
                    logger.warning(
                        "Output block processing failed for %s — will retry next cycle",
                        request.request_name, exc_info=True,
                    )

            if result.status == DAGStatus.COMPLETED:
                # Check blocks before transitioning
                if self.output_manager:
                    try:
                        if not await self.output_manager.all_blocks_archived(workflow.id):
                            # Stay ACTIVE, retry on next cycle
                            return
                    except Exception:
                        logger.warning(
                            "Block archive check failed for %s — proceeding with completion",
                            request.request_name, exc_info=True,
                        )
                # Check adaptive round completion
                if getattr(request, "adaptive", False):
                    # Re-fetch dag — poll_dag updated DB but our object is stale
                    dag = await self.db.get_dag(workflow.dag_id)
                    await self._handle_round_completion(request, workflow, dag)
                    return
                await self.transition(request, RequestStatus.COMPLETED)
                return
            elif result.status in (DAGStatus.PARTIAL, DAGStatus.FAILED):
                if self.error_handler:
                    completion = await self.error_handler.handle_dag_completion(
                        dag, request, workflow
                    )
                    if completion.action == "rescue":
                        if completion.problem_sites:
                            from wms2.core.error_handler import ErrorHandler
                            ErrorHandler.apply_site_exclusions(
                                dag.submit_dir, completion.problem_sites
                            )
                        await self.transition(request, RequestStatus.RESUBMITTING)
                        return
                # No error_handler or completion.action == "hold"
                await self.transition(request, RequestStatus.HELD)
                return
            # RUNNING — no transition, will poll again next cycle
            return

        if dag.status == DAGStatus.COMPLETED.value:
            if self.output_manager:
                try:
                    if not await self.output_manager.all_blocks_archived(workflow.id):
                        return
                except Exception:
                    logger.warning(
                        "Block archive check failed for %s — proceeding with completion",
                        request.request_name, exc_info=True,
                    )
            if getattr(request, "adaptive", False):
                await self._handle_round_completion(request, workflow, dag)
                return
            await self.transition(request, RequestStatus.COMPLETED)
        elif dag.status in (DAGStatus.PARTIAL.value, DAGStatus.FAILED.value):
            if self.error_handler:
                result = await self.error_handler.handle_dag_completion(
                    dag, request, workflow
                )
                if result.action == "rescue":
                    if result.problem_sites:
                        from wms2.core.error_handler import ErrorHandler
                        ErrorHandler.apply_site_exclusions(
                            dag.submit_dir, result.problem_sites
                        )
                    await self.transition(request, RequestStatus.RESUBMITTING)
                    return
            # No error_handler or result.action == "hold"
            await self.transition(request, RequestStatus.HELD)

    async def _handle_stopping(self, request):
        """Monitor clean stop progress."""
        workflow = await self.db.get_workflow_by_request(request.request_name)
        if not workflow or not workflow.dag_id:
            return

        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        dagman_status = await self.condor.query_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id
        )
        if dagman_status is None:
            # DAGMan process gone — stop complete
            await self.db.update_dag(dag.id, status=DAGStatus.STOPPED.value)
            await self._prepare_recovery(request, workflow, dag)

    async def _handle_resubmitting(self, request):
        """Recovery DAG prepared, move back to admission queue."""
        await self.transition(request, RequestStatus.QUEUED)

    async def _handle_held(self, request):
        """HELD: stable state waiting for operator action. No-op."""
        pass

    async def _handle_partial(self, request):
        """Handle partial DAG completion — re-evaluation on subsequent cycles.

        Legacy state kept for backward compatibility with existing DB rows.
        New transitions use HELD instead. No-op.
        """
        pass

    # ── Operator Actions ─────────────────────────────────────────

    async def release_held_request(self, request_name: str):
        """Release a HELD request back to the admission queue.

        The next lifecycle cycle will handle it: rescue DAG if one exists,
        or a new round.
        """
        request = await self.db.get_request(request_name)
        if not request:
            raise ValueError(f"Request {request_name} not found")
        if request.status != RequestStatus.HELD.value:
            raise ValueError(
                f"Cannot release request in {request.status} state; must be held"
            )
        await self.transition(request, RequestStatus.QUEUED)

    async def fail_request(self, request_name: str):
        """Fail a HELD or PARTIAL request: kill running DAG, mark DAGs/blocks
        as failed, transition request to FAILED."""
        request = await self.db.get_request(request_name)
        if not request:
            raise ValueError(f"Request {request_name} not found")
        allowed = (RequestStatus.HELD.value, RequestStatus.PARTIAL.value)
        if request.status not in allowed:
            raise ValueError(
                f"Cannot fail request in {request.status} state; "
                f"must be held or partial"
            )

        workflow = await self.db.get_workflow_by_request(request_name)
        if workflow:
            # 1. condor_rm on running DAG (swallow errors)
            if workflow.dag_id:
                dag = await self.db.get_dag(workflow.dag_id)
                if dag and dag.status in (
                    DAGStatus.SUBMITTED.value, DAGStatus.RUNNING.value
                ):
                    try:
                        await self.condor.remove_job(
                            schedd_name=dag.schedd_name,
                            cluster_id=dag.dagman_cluster_id,
                        )
                    except Exception:
                        logger.warning(
                            "Failed to remove DAG %s for %s",
                            dag.id, request_name,
                        )

            # 2. Mark all non-terminal DAGs as FAILED
            for dag in await self.db.list_dags(workflow_id=workflow.id):
                if dag.status not in (
                    DAGStatus.FAILED.value, DAGStatus.COMPLETED.value
                ):
                    await self.db.update_dag(dag.id, status=DAGStatus.FAILED.value)

            # 3. Mark open processing blocks as "failed"
            for block in await self.db.get_processing_blocks(workflow.id):
                if block.status == "open":
                    await self.db.update_processing_block(
                        block.id, status="failed"
                    )

        # 4. Transition to FAILED
        logger.info(
            "Operator-initiated fail for %s (output invalidation deferred)",
            request_name,
        )
        await self.transition(request, RequestStatus.FAILED)

    async def restart_request(self, request_name: str) -> str:
        """Kill+clone: create new request with incremented processing_version,
        fail the old one. Returns the new request name."""
        request = await self.db.get_request(request_name)
        if not request:
            raise ValueError(f"Request {request_name} not found")
        allowed = (RequestStatus.HELD.value, RequestStatus.PARTIAL.value)
        if request.status not in allowed:
            raise ValueError(
                f"Cannot restart request in {request.status} state; "
                f"must be held or partial"
            )

        # 1. Compute new version
        request_data = request.request_data or {}
        current_version = request_data.get("processing_version", 1)
        new_version = current_version + 1
        new_name = f"{request_name}_v{new_version}"

        # 2. Clone request with incremented version
        new_data = {**request_data, "processing_version": new_version}
        now = datetime.now(timezone.utc)
        await self.db.create_request(
            request_name=new_name,
            requestor=request.requestor,
            requestor_dn=request.requestor_dn,
            request_data=new_data,
            payload_config=request.payload_config,
            splitting_params=request.splitting_params,
            input_dataset=request.input_dataset,
            campaign=request.campaign,
            priority=request.priority,
            urgent=request.urgent,
            adaptive=request.adaptive,
            production_steps=request.production_steps or [],
            previous_version_request=request_name,
            cleanup_policy=request.cleanup_policy,
            status=RequestStatus.SUBMITTED.value,
            status_transitions=[],
            created_at=now,
            updated_at=now,
        )

        # 3. Link old → new
        await self.db.update_request(
            request_name, superseded_by_request=new_name
        )

        # 4. Fail old request (condor_rm, mark DAGs/blocks, → FAILED)
        await self.fail_request(request_name)

        logger.info(
            "Restarted %s → %s (processing_version=%d)",
            request_name, new_name, new_version,
        )
        return new_name

    async def get_error_summary(self, request_name: str) -> dict:
        """Read-only error inspection. Aggregates POST data from the
        current DAG's submit directory."""
        request = await self.db.get_request(request_name)
        if not request:
            raise ValueError(f"Request {request_name} not found")

        result = {
            "request_name": request_name,
            "status": request.status,
            "dag_id": None,
            "nodes_done": 0,
            "nodes_failed": 0,
            "total_nodes": 0,
            "error_summary": {},
            "site_summary": {},
            "bad_input_files": [],
        }

        workflow = await self.db.get_workflow_by_request(request_name)
        if not workflow or not workflow.dag_id:
            return result

        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return result

        result["dag_id"] = str(dag.id)
        result["nodes_done"] = dag.nodes_done or 0
        result["nodes_failed"] = dag.nodes_failed or 0
        result["total_nodes"] = dag.total_nodes or 0

        if not self.error_handler or not dag.submit_dir:
            return result

        post_data = self.error_handler.read_post_data(dag.submit_dir)
        if not post_data:
            return result

        # Aggregate by category
        category_counts = {}
        site_counts = {}
        bad_files = set()
        for entry in post_data:
            cat = entry.get("classification", {}).get("category", "unknown")
            category_counts[cat] = category_counts.get(cat, 0) + 1

            site = entry.get("job", {}).get("site", "unknown")
            sc = site_counts.setdefault(site, {"total": 0, "failed": 0})
            sc["total"] += 1
            if cat != "success":
                sc["failed"] += 1

            bad_file = entry.get("classification", {}).get("bad_input_file")
            if bad_file:
                bad_files.add(bad_file)

        result["error_summary"] = category_counts
        result["site_summary"] = site_counts
        result["bad_input_files"] = sorted(bad_files)
        return result

    # ── Adaptive Round Completion ──────────────────────────────

    async def _handle_round_completion(self, request, workflow, dag):
        """Handle completion of one adaptive round. Delegates shared logic
        to the module-level complete_round(), then handles termination
        checks and state transitions.
        """
        # Re-fetch workflow to get the latest state
        workflow = await self.db.get_workflow_by_request(request.request_name)

        # Shared logic: metrics, adaptive, offset advancement
        result = await complete_round(self.db, self.settings, workflow, dag)
        new_round = result["new_round"]

        # Re-fetch workflow to get latest production counters
        workflow = await self.db.get_workflow_by_request(request.request_name)
        config = workflow.config_data or {}
        is_gen = config.get("_is_gen", False)

        # ── Termination check: based on actual production ──
        if is_gen:
            target = workflow.target_events or 0
            produced = workflow.events_produced or 0
            if target > 0 and produced >= target:
                logger.info(
                    "Request %s: COMPLETED — produced %d >= target %d output events",
                    request.request_name, produced, target,
                )
                await self.transition(request, RequestStatus.COMPLETED)
                return

            # Safety net: if generated-event ranges are exhausted but target
            # not met, complete with warning (avoid infinite loops)
            total_gen_events = config.get("request_num_events", 0)
            filter_eff = float(config.get("filter_efficiency", 1.0))
            if filter_eff > 0 and filter_eff < 1.0 and total_gen_events > 0:
                total_gen_events = int(total_gen_events / filter_eff)
            new_offset = workflow.next_first_event or 1
            remaining_gen = total_gen_events - new_offset + 1
            if remaining_gen <= 0:
                logger.warning(
                    "Request %s: generated-event ranges exhausted "
                    "(produced %d / target %d). Completing.",
                    request.request_name, produced, target,
                )
                await self.transition(request, RequestStatus.COMPLETED)
                return
        else:
            total_files = workflow.total_input_files or 0
            processed = workflow.files_processed or 0
            if total_files > 0 and processed >= total_files:
                logger.info(
                    "Request %s: COMPLETED — processed %d / %d input files",
                    request.request_name, processed, total_files,
                )
                await self.transition(request, RequestStatus.COMPLETED)
                return

        # Apply production_steps priority demotion between rounds
        steps = request.production_steps or []
        if steps and is_gen:
            target = workflow.target_events or 0
            produced = workflow.events_produced or 0
            if target > 0:
                progress = produced / target
                step = steps[0]
                fraction = step["fraction"] if isinstance(step, dict) else step.fraction
                if progress >= fraction:
                    priority = step["priority"] if isinstance(step, dict) else step.priority
                    remaining_steps = steps[1:]
                    await self.db.update_request(
                        request.request_name,
                        priority=priority,
                        production_steps=[
                            s if isinstance(s, dict)
                            else {"fraction": s.fraction, "priority": s.priority}
                            for s in remaining_steps
                        ],
                    )

        logger.info(
            "Request %s: round %d complete, advancing to round %d "
            "(produced=%s, target=%s)",
            request.request_name, workflow.current_round - 1, new_round,
            workflow.events_produced if is_gen else workflow.files_processed,
            workflow.target_events if is_gen else workflow.total_input_files,
        )

        # Return to admission queue for next round
        await self.transition(request, RequestStatus.QUEUED)

    # ── Clean Stop ──────────────────────────────────────────────

    async def initiate_clean_stop(self, request_name: str, reason: str):
        request = await self.db.get_request(request_name)
        if not request:
            return
        workflow = await self.db.get_workflow_by_request(request_name)
        if not workflow:
            return

        now = datetime.now(timezone.utc)

        # PILOT_RUNNING: remove pilot job, no DAG to stop
        if request.status == RequestStatus.PILOT_RUNNING.value and workflow.pilot_cluster_id:
            await self.condor.remove_job(
                schedd_name=workflow.pilot_schedd, cluster_id=workflow.pilot_cluster_id
            )
            await self.db.update_workflow(workflow.id, status=WorkflowStatus.STOPPING.value)
            await self.transition(request, RequestStatus.STOPPING)
            return

        # ACTIVE: remove DAGMan job
        if not workflow.dag_id:
            return
        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        await self.condor.remove_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id
        )
        await self.db.update_dag(dag.id, stop_requested_at=now, stop_reason=reason)
        await self.db.update_workflow(workflow.id, status=WorkflowStatus.STOPPING.value)
        await self.transition(request, RequestStatus.STOPPING)

    async def _prepare_recovery(self, request, workflow, dag):
        """After clean stop, create recovery DAG record and transition."""
        rescue_path = f"{dag.dag_file_path}.rescue001"
        new_dag = await self.db.create_dag(
            workflow_id=workflow.id,
            dag_file_path=dag.dag_file_path,
            submit_dir=dag.submit_dir,
            rescue_dag_path=rescue_path,
            parent_dag_id=dag.id,
            total_nodes=dag.total_nodes,
            total_edges=dag.total_edges,
            node_counts=dag.node_counts,
            total_work_units=dag.total_work_units,
            completed_work_units=dag.completed_work_units,
            status=DAGStatus.READY.value,
        )
        await self.db.update_workflow(
            workflow.id, dag_id=new_dag.id, status="resubmitting"
        )

        # Partial production: consume step, demote priority
        steps = request.production_steps or []
        if steps:
            step = steps[0]
            remaining = steps[1:]
            await self.db.update_request(
                request.request_name,
                priority=step["priority"] if isinstance(step, dict) else step.priority,
                production_steps=[
                    s if isinstance(s, dict) else {"fraction": s.fraction, "priority": s.priority}
                    for s in remaining
                ],
            )

        await self.transition(request, RequestStatus.RESUBMITTING)

    # ── Timeout Detection ───────────────────────────────────────

    def _is_stuck(self, request) -> bool:
        status = RequestStatus(request.status)
        timeout = self.status_timeouts.get(status)
        if timeout is None:
            return False
        elapsed = (datetime.now(timezone.utc) - request.updated_at).total_seconds()
        return elapsed > timeout

    async def _handle_stuck(self, request):
        status = RequestStatus(request.status)
        elapsed = datetime.now(timezone.utc) - request.updated_at
        logger.warning(
            "Request %s stuck in %s for %.0fs",
            request.request_name, status.value, elapsed.total_seconds(),
        )

        if status in (
            RequestStatus.SUBMITTED,
            RequestStatus.PLANNING,
            RequestStatus.RESUBMITTING,
        ):
            await self.transition(request, RequestStatus.FAILED)

        elif status == RequestStatus.QUEUED:
            logger.error(
                "ALERT [stuck_in_queue] %s: queued for %.1f days",
                request.request_name, elapsed.total_seconds() / 86400,
            )

        elif status == RequestStatus.PILOT_RUNNING:
            workflow = await self.db.get_workflow_by_request(request.request_name)
            if workflow and workflow.pilot_cluster_id:
                job_exists = await self.condor.query_job(
                    schedd_name=workflow.pilot_schedd,
                    cluster_id=workflow.pilot_cluster_id,
                )
                if job_exists is None:
                    await self.transition(request, RequestStatus.FAILED)

        elif status == RequestStatus.ACTIVE:
            workflow = await self.db.get_workflow_by_request(request.request_name)
            if workflow and workflow.dag_id:
                dag = await self.db.get_dag(workflow.dag_id)
                if dag:
                    reachable = await self.condor.ping_schedd(dag.schedd_name)
                    if reachable:
                        logger.error(
                            "ALERT [slow_dag] %s: running for %.1f days",
                            request.request_name, elapsed.total_seconds() / 86400,
                        )
                    else:
                        logger.error(
                            "ALERT [schedd_unreachable] %s: schedd %s unreachable",
                            request.request_name, dag.schedd_name,
                        )

        elif status == RequestStatus.STOPPING:
            await self.initiate_clean_stop(
                request.request_name, reason="retry after stuck stop"
            )

    # ── State Transition ────────────────────────────────────────

    async def transition(self, request, new_status: RequestStatus):
        """Record a state transition with timestamp."""
        now = datetime.now(timezone.utc)
        old_transitions = request.status_transitions or []
        new_transition = {
            "from": request.status if isinstance(request.status, str) else request.status.value,
            "to": new_status.value,
            "timestamp": now.isoformat(),
        }
        await self.db.update_request(
            request.request_name,
            status=new_status.value,
            status_transitions=old_transitions + [new_transition],
            updated_at=now,
        )
        logger.info(
            "Request %s: %s -> %s",
            request.request_name,
            request.status if isinstance(request.status, str) else request.status.value,
            new_status.value,
        )
