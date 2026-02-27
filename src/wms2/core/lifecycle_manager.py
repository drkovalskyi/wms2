import asyncio
import logging
import os
from datetime import datetime, timezone

from wms2.adapters.base import CondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, RequestStatus, WorkflowStatus

logger = logging.getLogger(__name__)


def _aggregate_round_metrics(existing_metrics, dag, round_number):
    """Merge this round's DAG stats into cumulative step_metrics."""
    prior = existing_metrics or {}
    return {
        **prior,
        "rounds_completed": round_number + 1,
        "cumulative_nodes_done": prior.get("cumulative_nodes_done", 0) + (dag.nodes_done or 0),
        "cumulative_nodes_failed": prior.get("cumulative_nodes_failed", 0) + (dag.nodes_failed or 0),
        "last_round_nodes_done": dag.nodes_done or 0,
        "last_round_work_units": dag.total_work_units or 0,
    }


class RequestLifecycleManager:
    """
    Single owner of the request state machine. Runs a continuous loop that
    evaluates all non-terminal requests and dispatches work to the appropriate
    component workers.

    Phase 2+ components (workflow_manager, dag_planner, dag_monitor,
    output_manager, error_handler) are optional — when None, the corresponding
    handler logs a skip and returns.
    """

    def __init__(
        self,
        repository: Repository,
        condor_adapter: CondorAdapter,
        settings: Settings,
        *,
        workflow_manager=None,
        dag_planner=None,
        dag_monitor=None,
        output_manager=None,
        error_handler=None,
    ):
        self.db = repository
        self.condor = condor_adapter
        self.settings = settings
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

    # ── Main Loop ───────────────────────────────────────────────

    async def main_loop(self):
        """Main loop: query all non-terminal requests, evaluate each."""
        while True:
            try:
                requests = await self.db.get_non_terminal_requests()
                for request in requests:
                    try:
                        await self.evaluate_request(request)
                    except Exception:
                        logger.exception("Error evaluating %s", request.request_name)
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

            # Every cycle: retry failed Rucio calls
            if self.output_manager:
                await self.output_manager.process_blocks_for_workflow(workflow.id)

            if result.status == DAGStatus.COMPLETED:
                # Check blocks before transitioning
                if self.output_manager:
                    if not await self.output_manager.all_blocks_archived(workflow.id):
                        # Stay ACTIVE, retry on next cycle
                        return
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
                    action = await self.error_handler.handle_dag_completion(
                        dag, request, workflow
                    )
                    if action == "rescue":
                        await self.transition(request, RequestStatus.RESUBMITTING)
                        return
                # No error_handler or action == "hold"
                await self.transition(request, RequestStatus.HELD)
                return
            # RUNNING — no transition, will poll again next cycle
            return

        if dag.status == DAGStatus.COMPLETED.value:
            if self.output_manager:
                if not await self.output_manager.all_blocks_archived(workflow.id):
                    return
            if getattr(request, "adaptive", False):
                await self._handle_round_completion(request, workflow, dag)
                return
            await self.transition(request, RequestStatus.COMPLETED)
        elif dag.status in (DAGStatus.PARTIAL.value, DAGStatus.FAILED.value):
            if self.error_handler:
                action = await self.error_handler.handle_dag_completion(
                    dag, request, workflow
                )
                if action == "rescue":
                    await self.transition(request, RequestStatus.RESUBMITTING)
                    return
            # No error_handler or action == "hold"
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
        """Handle completion of one adaptive round. Advance progress offsets,
        aggregate metrics, and either complete the request or return it to the queue."""
        config = workflow.config_data or {}
        is_gen = config.get("_is_gen", False)
        params = workflow.splitting_params or {}

        # Compute how many events/files this round processed.
        # dag.nodes_done counts outer-DAG nodes (SUBDAGs), not processing
        # jobs.  node_counts["processing"] is the actual job count set at
        # planning time and is correct for completed DAGs.
        node_counts = dag.node_counts or {}
        total_jobs = node_counts.get("processing", dag.nodes_done)

        if is_gen:
            events_per_job = (params.get("events_per_job")
                              or params.get("eventsPerJob") or 100_000)
            events_this_round = total_jobs * events_per_job
            new_offset = workflow.next_first_event + events_this_round
            total_events = config.get("request_num_events", 0)
            remaining = total_events - new_offset + 1
        else:
            files_per_job = (params.get("files_per_job")
                             or params.get("FilesPerJob") or 1)
            files_this_round = total_jobs * files_per_job
            new_offset = workflow.file_offset + files_this_round
            remaining = -1  # file-based: check via DBS in planner

        # Aggregate step_metrics from completed DAG
        new_metrics = _aggregate_round_metrics(
            workflow.step_metrics, dag, workflow.current_round
        )

        new_round = workflow.current_round + 1

        # Update workflow with new offsets and round
        update_kwargs = {
            "current_round": new_round,
            "step_metrics": new_metrics,
        }
        if is_gen:
            update_kwargs["next_first_event"] = new_offset
        else:
            update_kwargs["file_offset"] = new_offset

        await self.db.update_workflow(workflow.id, **update_kwargs)

        # Check if all work is done
        if is_gen and remaining <= 0:
            await self.transition(request, RequestStatus.COMPLETED)
            return

        # Apply production_steps priority demotion between rounds
        steps = request.production_steps or []
        if steps:
            if is_gen and total_events > 0:
                progress = (new_offset - 1) / total_events
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
            "(%s offset: %s, remaining: %s)",
            request.request_name, workflow.current_round, new_round,
            "event" if is_gen else "file",
            new_offset,
            remaining if remaining >= 0 else "unknown",
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
