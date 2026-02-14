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

    def _submit_dag_sync(self, dag_file: str) -> tuple[str, str]:
        dag_submit = htcondor2.Submit.from_dag(str(dag_file))
        result = self._schedd.submit(dag_submit)
        cluster_id = str(result.cluster())
        return (cluster_id, self._schedd_name)

    async def submit_dag(self, dag_file: str) -> tuple[str, str]:
        return await asyncio.to_thread(self._submit_dag_sync, dag_file)

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
