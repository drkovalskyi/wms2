import asyncio
import logging

import sqlalchemy
from fastapi import APIRouter, Depends, Request as FastAPIRequest
from fastapi.responses import PlainTextResponse

from wms2 import __version__
from wms2.db.repository import Repository

from .deps import get_repository

log = logging.getLogger(__name__)

router = APIRouter(tags=["monitoring"])


@router.get("/health")
async def health(raw_request: FastAPIRequest):
    checks = {}
    degraded = False

    # DB connectivity
    try:
        session_factory = getattr(raw_request.app.state, "session_factory", None)
        if session_factory:
            async with session_factory() as session:
                await asyncio.wait_for(
                    session.execute(sqlalchemy.text("SELECT 1")), timeout=5.0,
                )
            checks["database"] = "ok"
        else:
            checks["database"] = "unavailable"
            degraded = True
    except Exception as exc:
        checks["database"] = f"error: {exc}"
        degraded = True

    # Background task liveness
    for task_name in ("lifecycle", "cric_sync"):
        task = getattr(raw_request.app.state, f"{task_name}_task", None)
        if task is not None and not task.done():
            checks[task_name] = "running"
        else:
            checks[task_name] = "dead"
            degraded = True

    return {
        "status": "degraded" if degraded else "ok",
        "version": __version__,
        "checks": checks,
    }


@router.get("/status")
async def status(repo: Repository = Depends(get_repository)):
    counts = await repo.count_requests_by_status()
    return {"requests_by_status": counts}


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics(repo: Repository = Depends(get_repository)):
    counts = await repo.count_requests_by_status()
    lines = []
    lines.append("# HELP wms2_requests_total Number of requests by status")
    lines.append("# TYPE wms2_requests_total gauge")
    for status_val, count in counts.items():
        lines.append(f'wms2_requests_total{{status="{status_val}"}} {count}')
    return "\n".join(lines) + "\n"


@router.get("/htcondor-overview")
async def htcondor_overview(
    raw_request: FastAPIRequest,
    repo: Repository = Depends(get_repository),
):
    """Live HTCondor job counts grouped by request and site.

    For each active request with a running DAG, queries HTCondor for
    payload job status and MATCH_GLIDEIN_CMSSite, then aggregates
    running/idle/held counts per site.
    """
    condor = getattr(raw_request.app.state, "condor", None)

    # Find all non-terminal requests that have an active workflow + DAG
    requests = await repo.get_non_terminal_requests()
    request_entries = []
    grand_totals = {"running": 0, "idle": 0, "held": 0}
    grand_by_site: dict[str, dict[str, int]] = {}

    async def _query_one(req_row):
        """Query HTCondor for one request's active DAG."""
        wf = await repo.get_workflow_by_request(req_row.request_name)
        if not wf or not wf.dag_id:
            return None
        dag = await repo.get_dag(wf.dag_id)
        if not dag or not dag.dagman_cluster_id:
            return None
        if dag.status not in ("submitted", "running"):
            return None

        sites: dict[str, dict[str, int]] = {}
        if condor:
            try:
                sites = await condor.query_dag_site_summary(
                    dag.dagman_cluster_id, schedd_name=dag.schedd_name,
                )
            except Exception:
                log.debug("HTCondor site query failed for DAG %s", dag.id)

        totals = {"running": 0, "idle": 0, "held": 0}
        for counts in sites.values():
            for k in totals:
                totals[k] += counts.get(k, 0)

        return {
            "request_name": req_row.request_name,
            "request_status": req_row.status,
            "round": wf.current_round,
            "dag_status": dag.status,
            "dagman_cluster_id": dag.dagman_cluster_id,
            "schedd_name": dag.schedd_name,
            "sites": sites,
            "totals": totals,
        }

    # Query all active DAGs concurrently
    tasks = [_query_one(r) for r in requests]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if res is None or isinstance(res, Exception):
            continue
        request_entries.append(res)
        for k in grand_totals:
            grand_totals[k] += res["totals"][k]
        for site, counts in res["sites"].items():
            if site not in grand_by_site:
                grand_by_site[site] = {"running": 0, "idle": 0, "held": 0}
            for k in counts:
                grand_by_site[site][k] += counts[k]

    return {
        "requests": request_entries,
        "totals": {
            **grand_totals,
            "by_site": grand_by_site,
        },
    }
