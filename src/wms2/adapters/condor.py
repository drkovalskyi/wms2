"""Real HTCondor adapter using htcondor2 Python bindings."""

from __future__ import annotations

import asyncio
import logging
from functools import partial
from typing import Any

import htcondor2

from .base import CondorAdapter

logger = logging.getLogger(__name__)


class HTCondorAdapter(CondorAdapter):
    """HTCondor adapter that wraps synchronous htcondor2 calls in asyncio.to_thread()."""

    def __init__(self, condor_host: str, schedd_name: str | None = None):
        collector = htcondor2.Collector(condor_host)
        if schedd_name:
            schedd_ad = collector.locate(
                htcondor2.DaemonType.Schedd, schedd_name
            )
        else:
            schedd_ad = collector.locate(htcondor2.DaemonType.Schedd)
        self._schedd = htcondor2.Schedd(schedd_ad)
        self._schedd_name = schedd_ad.get("Name", schedd_name or "")
        logger.info("Connected to schedd %s via %s", self._schedd_name, condor_host)

    def _submit_job_sync(self, submit_file: str) -> tuple[str, str]:
        with open(submit_file) as f:
            text = f.read()
        sub = htcondor2.Submit(text)
        result = self._schedd.submit(sub)
        cluster_id = str(result.cluster())
        return (cluster_id, self._schedd_name)

    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        return await asyncio.to_thread(self._submit_job_sync, submit_file)

    def _submit_dag_sync(self, dag_file: str, force: bool = False) -> tuple[str, str]:
        options = {"Force": True} if force else {}
        dag_submit = htcondor2.Submit.from_dag(str(dag_file), options=options)
        result = self._schedd.submit(dag_submit)
        cluster_id = str(result.cluster())
        return (cluster_id, self._schedd_name)

    async def submit_dag(self, dag_file: str, force: bool = False) -> tuple[str, str]:
        return await asyncio.to_thread(self._submit_dag_sync, dag_file, force)

    def _query_job_sync(self, cluster_id: str) -> dict[str, Any] | None:
        constraint = f"ClusterId == {cluster_id}"
        projection = [
            "ClusterId", "ProcId", "JobStatus", "DAG_NodesTotal",
            "DAG_NodesDone", "DAG_NodesFailed", "DAG_NodesQueued",
            "DAG_NodesReady", "DAG_NodesUnready", "DAG_Status",
        ]
        ads = self._schedd.query(constraint=constraint, projection=projection)
        if not ads:
            return None
        ad = ads[0]
        return {k: ad[k] for k in ad.keys()}

    async def query_job(self, schedd_name: str, cluster_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._query_job_sync, cluster_id)

    def _check_job_completed_sync(self, cluster_id: str) -> bool:
        # First check if still in the queue
        constraint = f"ClusterId == {cluster_id}"
        ads = self._schedd.query(constraint=constraint, projection=["JobStatus"])
        if ads:
            # Job still in queue — not completed yet
            return False
        # Not in queue — check history for completed status (JobStatus==4)
        for ad in self._schedd.history(
            constraint=constraint,
            projection=["JobStatus"],
            match=1,
        ):
            return int(ad["JobStatus"]) == 4
        # Not in queue or history — treat as completed (dagman exited)
        return True

    async def check_job_completed(self, cluster_id: str, schedd_name: str) -> bool:
        return await asyncio.to_thread(self._check_job_completed_sync, cluster_id)

    def _remove_job_sync(self, cluster_id: str) -> None:
        self._schedd.act(
            htcondor2.JobAction.Remove,
            f"ClusterId == {cluster_id}",
        )

    async def remove_job(self, schedd_name: str, cluster_id: str) -> None:
        await asyncio.to_thread(self._remove_job_sync, cluster_id)

    def _ping_schedd_sync(self) -> bool:
        try:
            self._schedd.query(constraint="false", projection=["ClusterId"], limit=1)
            return True
        except Exception:
            return False

    async def ping_schedd(self, schedd_name: str) -> bool:
        return await asyncio.to_thread(self._ping_schedd_sync)

    _JOB_STATUS_MAP = {
        1: "idle",
        2: "running",
        3: "removed",
        4: "completed",
        5: "held",
    }

    def _query_dag_jobs_sync(self, cluster_id: str) -> list[dict]:
        """Query per-job details for payload jobs under a DAGMan hierarchy."""
        # Find sub-DAGMan cluster IDs (inner DAGs)
        sub_dagman_ads = self._schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=["ClusterId"],
        )
        if not sub_dagman_ads:
            return []

        # Build constraint for payload jobs under all inner DAGMans
        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7"

        projection = [
            "DAGNodeName", "JobStatus", "ResidentSetSize_RAW",
            "CumulativeRemoteUserCpu", "CumulativeRemoteSysCpu",
            "RequestCpus", "RequestMemory", "JobStartDate", "ServerTime",
            "MATCH_GLIDEIN_CMSSite",
        ]
        ads = self._schedd.query(constraint=constraint, projection=projection)

        jobs = []
        for ad in ads:
            status_int = int(ad.get("JobStatus", 0))
            status = self._JOB_STATUS_MAP.get(status_int, f"unknown({status_int})")

            start = ad.get("JobStartDate")
            server_time = ad.get("ServerTime")
            wall_time = None
            if start is not None and server_time is not None:
                wall_time = max(0, int(server_time) - int(start))

            rss_kb = ad.get("ResidentSetSize_RAW")
            memory_mb = int(rss_kb) // 1024 if rss_kb is not None else None

            cpu_user = float(ad.get("CumulativeRemoteUserCpu", 0))
            cpu_sys = float(ad.get("CumulativeRemoteSysCpu", 0))
            cpus = int(ad.get("RequestCpus", 1))

            cpu_efficiency = None
            if wall_time and wall_time > 0 and cpus > 0:
                cpu_efficiency = round(
                    100.0 * (cpu_user + cpu_sys) / (wall_time * cpus), 1
                )

            jobs.append({
                "name": ad.get("DAGNodeName", "?"),
                "status": status,
                "wall_time": wall_time,
                "memory_mb": memory_mb,
                "cpu_user": round(cpu_user, 1),
                "cpu_sys": round(cpu_sys, 1),
                "cpu_efficiency": cpu_efficiency,
                "cpus": cpus,
                "request_memory": int(ad.get("RequestMemory", 0)),
                "site": ad.get("MATCH_GLIDEIN_CMSSite"),
            })

        jobs.sort(key=lambda j: j["name"])
        return jobs

    async def query_dag_jobs(self, cluster_id: str) -> list[dict] | None:
        return await asyncio.to_thread(self._query_dag_jobs_sync, cluster_id)

    def _count_dag_jobs_sync(self, cluster_id: str) -> dict[str, int]:
        """Count nodes by status across all inner sub-DAGMans.

        Uses DAGMan ClassAds for done/failed/total (authoritative) and
        queries actual payload job statuses for the running/idle/held
        breakdown of in-queue nodes.
        """
        # Find sub-DAGMan jobs (inner DAGs) under the outer DAGMan
        sub_dagman_ads = self._schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=[
                "ClusterId", "DAG_NodesTotal", "DAG_NodesDone",
                "DAG_NodesFailed", "DAG_NodesReady", "DAG_NodesUnready",
            ],
        )

        if not sub_dagman_ads:
            return {"idle": 0, "running": 0, "done": 0, "held": 0, "failed": 0, "total": 0}

        # done/failed/total from DAGMan ClassAds (authoritative for these)
        counts = {"idle": 0, "running": 0, "done": 0, "held": 0, "failed": 0, "total": 0}
        for ad in sub_dagman_ads:
            counts["done"] += int(ad.get("DAG_NodesDone", 0))
            counts["failed"] += int(ad.get("DAG_NodesFailed", 0))
            counts["total"] += int(ad.get("DAG_NodesTotal", 0))
            # Nodes not yet submitted to schedd count as idle
            counts["idle"] += (
                int(ad.get("DAG_NodesReady", 0))
                + int(ad.get("DAG_NodesUnready", 0))
            )

        # Query actual payload job statuses for in-queue breakdown
        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7"
        job_ads = self._schedd.query(
            constraint=constraint, projection=["JobStatus"],
        )
        for ad in job_ads:
            status = int(ad.get("JobStatus", 0))
            if status == 1:
                counts["idle"] += 1
            elif status == 2:
                counts["running"] += 1
            elif status == 5:
                counts["held"] += 1
        return counts

    async def count_dag_jobs(self, cluster_id: str) -> dict[str, int] | None:
        return await asyncio.to_thread(self._count_dag_jobs_sync, cluster_id)
