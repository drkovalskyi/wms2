import asyncio
import logging
from datetime import datetime, timezone

from wms2.adapters.base import CondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, RequestStatus

logger = logging.getLogger(__name__)


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
                await self.transition(request, RequestStatus.ACTIVE)
                return

        if request.urgent:
            await self.transition(request, RequestStatus.ACTIVE)
        else:
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
            # DAG still active — would poll via dag_monitor in Phase 2
            pass

        if dag.status == DAGStatus.COMPLETED.value:
            await self.transition(request, RequestStatus.COMPLETED)
        elif dag.status == DAGStatus.PARTIAL.value:
            await self.transition(request, RequestStatus.PARTIAL)
        elif dag.status == DAGStatus.FAILED.value:
            await self.transition(request, RequestStatus.FAILED)

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

    async def _handle_partial(self, request):
        """Handle partial DAG completion."""
        if self.error_handler is None:
            logger.debug("Skipping _handle_partial: error_handler not available")
            return

        workflow = await self.db.get_workflow_by_request(request.request_name)
        if not workflow or not workflow.dag_id:
            return
        dag = await self.db.get_dag(workflow.dag_id)
        if dag:
            await self.error_handler.handle_dag_partial_failure(dag)

    # ── Clean Stop ──────────────────────────────────────────────

    async def initiate_clean_stop(self, request_name: str, reason: str):
        request = await self.db.get_request(request_name)
        if not request:
            return
        workflow = await self.db.get_workflow_by_request(request_name)
        if not workflow or not workflow.dag_id:
            return
        dag = await self.db.get_dag(workflow.dag_id)
        if not dag:
            return

        await self.condor.remove_job(
            schedd_name=dag.schedd_name, cluster_id=dag.dagman_cluster_id
        )
        now = datetime.now(timezone.utc)
        await self.db.update_dag(dag.id, stop_requested_at=now, stop_reason=reason)
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
