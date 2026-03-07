import asyncio
import glob
import json
import logging
import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request as FastAPIRequest

from wms2.api.deps import get_repository
from wms2.db.repository import Repository

log = logging.getLogger(__name__)

router = APIRouter(prefix="/dags", tags=["dags"])


def _dag_summary(r):
    return {
        "id": str(r.id),
        "workflow_id": str(r.workflow_id) if r.workflow_id else None,
        "status": r.status,
        "total_nodes": r.total_nodes,
        "total_work_units": r.total_work_units,
        "nodes_done": r.nodes_done,
        "nodes_failed": r.nodes_failed,
        "nodes_running": r.nodes_running,
        "dag_file_path": r.dag_file_path,
        "dagman_cluster_id": r.dagman_cluster_id,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _dag_detail(row):
    return {
        "id": str(row.id),
        "workflow_id": str(row.workflow_id) if row.workflow_id else None,
        "dag_file_path": row.dag_file_path,
        "submit_dir": row.submit_dir,
        "rescue_dag_path": row.rescue_dag_path,
        "dagman_cluster_id": row.dagman_cluster_id,
        "schedd_name": row.schedd_name,
        "parent_dag_id": str(row.parent_dag_id) if row.parent_dag_id else None,
        "stop_requested_at": row.stop_requested_at.isoformat() if row.stop_requested_at else None,
        "stop_reason": row.stop_reason,
        "status": row.status,
        "total_nodes": row.total_nodes,
        "total_edges": row.total_edges,
        "node_counts": row.node_counts,
        "nodes_idle": row.nodes_idle,
        "nodes_running": row.nodes_running,
        "nodes_done": row.nodes_done,
        "nodes_failed": row.nodes_failed,
        "nodes_held": row.nodes_held,
        "total_work_units": row.total_work_units,
        "completed_work_units": row.completed_work_units,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "submitted_at": row.submitted_at.isoformat() if row.submitted_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
    }


@router.get("")
async def list_dags(
    status: str | None = None,
    workflow_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    repo: Repository = Depends(get_repository),
):
    wf_uuid = None
    if workflow_id:
        try:
            wf_uuid = UUID(workflow_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid workflow_id format")

    rows = await repo.list_dags(
        status=status, workflow_id=wf_uuid, limit=limit, offset=offset
    )
    return [_dag_summary(r) for r in rows]


@router.get("/{dag_id}")
async def get_dag(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    result = _dag_detail(row)

    # DB counts are already accurate — lifecycle manager aggregates
    # inner SUBDAG status files and updates these each cycle.
    return result


@router.get("/{dag_id}/history")
async def get_dag_history(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    """Get the event history for a DAG (status transitions, node counts over time)."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    history = await repo.get_dag_history(d_uuid)
    return [
        {
            "id": h.id,
            "dag_id": str(h.dag_id) if h.dag_id else None,
            "event_type": h.event_type,
            "from_status": h.from_status,
            "to_status": h.to_status,
            "nodes_done": h.nodes_done,
            "nodes_failed": h.nodes_failed,
            "nodes_running": h.nodes_running,
            "detail": h.detail,
            "created_at": h.created_at.isoformat() if h.created_at else None,
        }
        for h in history
    ]


@router.get("/{dag_id}/jobs")
async def get_dag_jobs(
    dag_id: str,
    raw_request: FastAPIRequest,
    repo: Repository = Depends(get_repository),
):
    """Get live HTCondor job details for all payload jobs in this DAG."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    if not row.dagman_cluster_id:
        return []

    condor = getattr(raw_request.app.state, "condor", None)
    if condor is None:
        return []

    try:
        jobs = await condor.query_dag_jobs(
            row.dagman_cluster_id, schedd_name=row.schedd_name,
        )
        return jobs if jobs is not None else []
    except Exception:
        log.debug("HTCondor job query failed for DAG %s", dag_id)
        return []


# ---------------------------------------------------------------------------
# Node Event Log — parse DAGMan node log files
# ---------------------------------------------------------------------------

# Event types we care about (skip noisy IMAGE_SIZE, JOB_AD_INFORMATION, etc.)
_INTERESTING_EVENTS: set[str] = {
    "SUBMIT",
    "EXECUTE",
    "JOB_TERMINATED",
    "JOB_HELD",
    "JOB_RELEASED",
    "JOB_EVICTED",
    "JOB_ABORTED",
    "POST_SCRIPT_TERMINATED",
    "SHADOW_EXCEPTION",
}

# Human-friendly short names for the UI
_EVENT_NAMES: dict[str, str] = {
    "SUBMIT": "submit",
    "EXECUTE": "execute",
    "JOB_TERMINATED": "terminated",
    "JOB_HELD": "held",
    "JOB_RELEASED": "released",
    "JOB_EVICTED": "evicted",
    "JOB_ABORTED": "aborted",
    "POST_SCRIPT_TERMINATED": "post_script",
    "SHADOW_EXCEPTION": "shadow_exception",
}


def _parse_node_log(path: str) -> list[dict]:
    """Parse a single group.dag.nodes.log file (blocking I/O)."""
    import htcondor2

    events: list[dict] = []
    # Map cluster.proc -> node name (learned from SUBMIT events' LogNotes)
    cluster_to_node: dict[str, str] = {}

    try:
        jel = htcondor2.JobEventLog(path)
        for ev in jel.events(0):
            type_name = ev.type.name  # e.g. "SUBMIT", "EXECUTE"
            if type_name not in _INTERESTING_EVENTS:
                continue

            cluster_proc = f"{ev.cluster}.{ev.proc}"

            # Learn node name from SUBMIT events
            if type_name == "SUBMIT":
                log_notes = ev.get("LogNotes", "")
                # LogNotes looks like "DAG Node: proc_000003"
                if "DAG Node:" in log_notes:
                    node_name = log_notes.split("DAG Node:")[-1].strip()
                    cluster_to_node[cluster_proc] = node_name

            node_name = cluster_to_node.get(cluster_proc, cluster_proc)

            # Build detail string
            detail = _extract_detail(ev, type_name)

            # Format timestamp
            ts = ev.timestamp
            if isinstance(ts, (int, float)):
                ts_str = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            elif isinstance(ts, datetime):
                ts_str = ts.isoformat()
            else:
                ts_str = str(ts)

            events.append({
                "node": node_name,
                "event": _EVENT_NAMES.get(type_name, type_name.lower()),
                "timestamp": ts_str,
                "detail": detail,
            })
    except Exception as exc:
        log.warning("Failed to parse node log %s: %s", path, exc)

    return events


def _extract_detail(ev, type_name: str) -> str:
    """Extract a short human-readable detail string from an event."""
    try:
        if type_name == "SUBMIT":
            return ""

        if type_name == "EXECUTE":
            slot = ev.get("SlotName", "") or ev.get("ExecuteHost", "")
            return str(slot).strip("<>") if slot else ""

        if type_name == "JOB_TERMINATED":
            rc = ev.get("ReturnValue", "?")
            mem = ev.get("MemoryUsage", None)
            # TimeExecuteUsage may be in different keys
            wall = ev.get("RemoteWallClockTime", None)
            parts = [f"exit {rc}"]
            if mem is not None:
                parts.append(f"{mem} MB")
            if wall is not None:
                parts.append(f"{int(float(wall))}s")
            return ", ".join(parts)

        if type_name == "JOB_HELD":
            reason = str(ev.get("HoldReason", ""))
            return reason[:120] if reason else ""

        if type_name == "JOB_RELEASED":
            reason = str(ev.get("Reason", ev.get("ReleaseReason", "")))
            return reason[:120] if reason else ""

        if type_name == "JOB_EVICTED":
            reason = str(ev.get("Reason", ""))
            return reason[:120] if reason else ""

        if type_name == "JOB_ABORTED":
            reason = str(ev.get("Reason", ""))
            return reason[:120] if reason else ""

        if type_name == "POST_SCRIPT_TERMINATED":
            rc = ev.get("ReturnValue", "?")
            return f"exit {rc}"

        if type_name == "SHADOW_EXCEPTION":
            msg = str(ev.get("Message", ""))
            return msg[:120] if msg else ""

    except Exception:
        pass
    return ""


@router.get("/{dag_id}/node-log")
async def get_dag_node_log(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    """Parse DAGMan node log files and return the event history."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    if not row.submit_dir:
        return []

    # Find all group.dag.nodes.log files under merge-group subdirs
    pattern = f"{row.submit_dir}/mg_*/group.dag.nodes.log"
    log_files = sorted(glob.glob(pattern))
    if not log_files:
        return []

    # Parse each log file in a thread executor (blocking I/O)
    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(None, _parse_node_log, f) for f in log_files]
    results = await asyncio.gather(*tasks)

    # Merge all events and sort by timestamp
    all_events: list[dict] = []
    for event_list in results:
        all_events.extend(event_list)

    all_events.sort(key=lambda e: e["timestamp"])
    return all_events


    # ---------------------------------------------------------------------------
    # Performance Summary — DAGMan metrics + step_metrics from workflow
    # ---------------------------------------------------------------------------


def _read_json(path: str) -> dict | None:
    """Read a JSON file, return None on any error."""
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _collect_dag_metrics(submit_dir: str) -> list[dict]:
    """Read group.dag.metrics files from merge-group subdirs (blocking I/O)."""
    mg_dirs = sorted(glob.glob(os.path.join(submit_dir, "mg_*")))
    result = []
    for mg_dir in mg_dirs:
        m = _read_json(os.path.join(mg_dir, "group.dag.metrics"))
        if m:
            result.append({
                "merge_group": os.path.basename(mg_dir),
                "duration_sec": m.get("duration"),
                "nodes": m.get("total_nodes"),
                "nodes_succeeded": m.get("nodes_succeeded"),
                "nodes_failed": m.get("nodes_failed"),
                "jobs_submitted": m.get("jobs_submitted"),
                "jobs_succeeded": m.get("jobs_succeeded"),
                "jobs_failed": m.get("jobs_failed"),
                "exitcode": m.get("exitcode"),
            })
    return result


@router.get("/{dag_id}/performance")
async def get_dag_performance(
    dag_id: str,
    repo: Repository = Depends(get_repository),
):
    """Get DAGMan metrics and per-step performance for this DAG's round."""
    try:
        d_uuid = UUID(dag_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid DAG ID format")

    row = await repo.get_dag(d_uuid)
    if not row:
        raise HTTPException(status_code=404, detail="DAG not found")

    # DAGMan metrics from disk
    dag_metrics = []
    if row.submit_dir:
        loop = asyncio.get_running_loop()
        dag_metrics = await loop.run_in_executor(
            None, _collect_dag_metrics, row.submit_dir
        )

    # Step metrics from workflow DB — determine this DAG's round number
    step_data = {}
    round_number = None
    if row.workflow_id:
        workflow = await repo.get_workflow(row.workflow_id)
        if workflow and workflow.step_metrics:
            sm = workflow.step_metrics
            if isinstance(sm, str):
                sm = json.loads(sm)

            # Find round number: DAGs ordered by created_at
            all_dags = await repo.list_dags(
                workflow_id=row.workflow_id, limit=1000
            )
            dag_ids_ordered = [
                str(d.id) for d in sorted(all_dags, key=lambda d: d.created_at)
            ]
            try:
                round_number = dag_ids_ordered.index(str(row.id))
            except ValueError:
                round_number = None

            rounds = sm.get("rounds", {})
            if round_number is not None and str(round_number) in rounds:
                step_data = rounds[str(round_number)]

    return {
        "dag_metrics": dag_metrics,
        "round_number": round_number,
        "step_data": step_data,
    }
