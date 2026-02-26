"""Error Handler — workflow-level failure classification and recovery.

When a DAG completes with failures, the Error Handler reads POST script
side files, aggregates failure data, and decides: auto-rescue or hold.

Uses a single 20% threshold (error_hold_threshold):
- Below threshold + rescue attempts remaining → submit rescue DAG
- At or above threshold, or rescues exhausted → hold for operator

See docs/error_handling.md Section 3 for the full design.
"""

import glob
import json
import logging
import os
from collections import defaultdict

from wms2.adapters.base import CondorAdapter
from wms2.config import Settings
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, WorkflowStatus

logger = logging.getLogger(__name__)


class ErrorHandler:
    def __init__(self, repository: Repository, condor_adapter: CondorAdapter, settings: Settings):
        self.db = repository
        self.condor = condor_adapter
        self.settings = settings

    async def handle_dag_completion(self, dag, request, workflow) -> str:
        """Handle DAG that completed with failures. Returns 'rescue' or 'hold'."""
        ratio = dag.nodes_failed / max(dag.total_nodes, 1)
        await self._record_event(dag, "dag_completion", {
            "failure_ratio": ratio,
            "nodes_done": dag.nodes_done,
            "nodes_failed": dag.nodes_failed,
            "total_nodes": dag.total_nodes,
        })

        # Read POST script side files for failure analysis
        post_data = self.read_post_data(dag.submit_dir)
        if post_data:
            self._log_failure_summary(post_data, request.request_name)
            problem_sites = self.analyze_site_failures(post_data)
            if problem_sites:
                logger.warning(
                    "Problem sites for %s: %s",
                    request.request_name, ", ".join(problem_sites),
                )

        # Check rescue attempt count
        rescue_count = await self._count_rescue_chain(dag)
        if rescue_count >= self.settings.error_max_rescue_attempts:
            logger.warning(
                "Max rescue attempts (%d) reached for %s",
                rescue_count, request.request_name,
            )
            return "hold"

        if ratio < self.settings.error_hold_threshold:
            await self._prepare_rescue(dag, request, workflow)
            return "rescue"
        else:
            logger.error(
                "ALERT [dag_completion] %s: %.1f%% failed — held for operator",
                request.request_name, ratio * 100,
            )
            return "hold"

    def read_post_data(self, submit_dir: str) -> list[dict]:
        """Read all final post.json files from submit directory tree."""
        post_files = glob.glob(
            os.path.join(submit_dir, "**", "*.post.json"), recursive=True
        )
        results = []
        for path in post_files:
            try:
                with open(path) as f:
                    data = json.load(f)
                    if data.get("final"):
                        results.append(data)
            except (json.JSONDecodeError, OSError):
                logger.warning("Failed to read post data: %s", path)
        return results

    def analyze_site_failures(self, post_data: list[dict]) -> list[str]:
        """Aggregate failures by site. Returns list of problem sites.

        A site is problematic when it has >50% failure rate with at least
        3 failures (to avoid banning on a single unlucky job).
        """
        site_failures: dict[str, int] = defaultdict(int)
        site_total: dict[str, int] = defaultdict(int)
        for entry in post_data:
            site = entry.get("job", {}).get("site", "unknown")
            site_total[site] += 1
            category = entry.get("classification", {}).get("category", "")
            if category in ("infrastructure", "infrastructure_memory", "data"):
                site_failures[site] += 1

        problem_sites = []
        for site, failures in site_failures.items():
            total = site_total[site]
            if total > 0 and failures / total > 0.5 and failures >= 3:
                problem_sites.append(site)
        return problem_sites

    def _log_failure_summary(self, post_data: list[dict], request_name: str):
        """Log aggregated failure summary from POST data."""
        categories: dict[str, int] = defaultdict(int)
        for entry in post_data:
            cat = entry.get("classification", {}).get("category", "unknown")
            if cat != "success":
                categories[cat] += 1
        if categories:
            summary = ", ".join(f"{cat}={count}" for cat, count in sorted(categories.items()))
            logger.info("Failure summary for %s: %s", request_name, summary)

    async def _prepare_rescue(self, dag, request, workflow):
        """Create rescue DAG record, point workflow to it."""
        rescue_path = self._find_rescue_dag(dag)
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
            workflow.id, dag_id=new_dag.id, status=WorkflowStatus.RESUBMITTING.value
        )

    def _find_rescue_dag(self, dag) -> str:
        """Find highest-numbered rescue DAG file on disk."""
        for i in range(99, 0, -1):
            path = f"{dag.dag_file_path}.rescue{i:03d}"
            if os.path.exists(path):
                return path
        return f"{dag.dag_file_path}.rescue001"

    async def _count_rescue_chain(self, dag) -> int:
        """Count how many rescue DAGs exist in this workflow's chain."""
        count = 0
        current = dag
        while current.parent_dag_id:
            count += 1
            current = await self.db.get_dag(current.parent_dag_id)
            if not current:
                break
        return count

    async def _record_event(self, dag, event_type: str, detail: dict):
        await self.db.create_dag_history(
            dag_id=dag.id,
            event_type=event_type,
            from_status=dag.status,
            nodes_done=dag.nodes_done,
            nodes_failed=dag.nodes_failed,
            nodes_running=0,
            detail=detail,
        )
