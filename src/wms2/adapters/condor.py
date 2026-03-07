"""Real HTCondor adapter using htcondor2 Python bindings."""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any

import htcondor2

from .base import CondorAdapter

logger = logging.getLogger(__name__)


class HTCondorAdapter(CondorAdapter):
    """HTCondor adapter that wraps synchronous htcondor2 calls in asyncio.to_thread().

    Supports multiple schedds: the default schedd is located at init time,
    additional schedds are connected lazily when queried by name.
    """

    def __init__(self, condor_host: str, schedd_name: str | None = None,
                 sec_token_directory: str = "",
                 extra_collectors: list[str] | None = None):
        # Set IDTOKEN directory before any connections (for remote schedd auth)
        if sec_token_directory:
            htcondor2.param["SEC_TOKEN_DIRECTORY"] = sec_token_directory

        self._collector = htcondor2.Collector(condor_host)
        self._extra_collectors: list[htcondor2.Collector] = [
            htcondor2.Collector(addr) for addr in (extra_collectors or [])
        ]

        # Locate and cache the default schedd
        if schedd_name:
            schedd_ad = self._collector.locate(
                htcondor2.DaemonType.Schedd, schedd_name
            )
        else:
            schedd_ad = self._collector.locate(htcondor2.DaemonType.Schedd)
        if schedd_ad is None:
            raise RuntimeError(
                f"Default schedd not found via collector {condor_host}"
            )
        default_name = schedd_ad.get("Name", schedd_name or "")
        self._default_schedd_name = default_name
        self._schedds: dict[str, htcondor2.Schedd] = {
            default_name: htcondor2.Schedd(schedd_ad)
        }
        logger.info("Default schedd: %s via %s", default_name, condor_host)

        # Full proxy delegation (following CRABServer approach)
        htcondor2.param["DELEGATE_FULL_JOB_GSI_CREDENTIALS"] = "true"
        htcondor2.param["DELEGATE_JOB_GSI_CREDENTIALS_LIFETIME"] = "0"

    def _get_schedd(self, name: str | None = None) -> htcondor2.Schedd:
        """Get a cached schedd connection, connecting lazily if needed.

        Tries the configured collector first; falls back to extra collectors
        so that DAGs on different pools (e.g. local + remote) can coexist.
        Note: htcondor2 Collector.locate() returns None (not an exception)
        when a daemon is not found, and Schedd(None) silently connects to
        the local schedd. We must check for None explicitly.
        """
        name = name or self._default_schedd_name
        if name not in self._schedds:
            schedd_ad = None
            try:
                schedd_ad = self._collector.locate(
                    htcondor2.DaemonType.Schedd, name
                )
            except Exception:
                pass
            if schedd_ad is None:
                # Schedd not in primary collector — try extra collectors
                for coll in self._extra_collectors:
                    try:
                        schedd_ad = coll.locate(
                            htcondor2.DaemonType.Schedd, name
                        )
                    except Exception:
                        continue
                    if schedd_ad is not None:
                        break
            if schedd_ad is None:
                raise RuntimeError(f"Schedd {name!r} not found in any collector")
            self._schedds[name] = htcondor2.Schedd(schedd_ad)
            logger.info("Connected to schedd %s", name)
        return self._schedds[name]

    @staticmethod
    def _set_proxy(sub: htcondor2.Submit) -> None:
        """Set x509userproxy on a Submit object if a proxy file exists.

        This is where the local proxy path (e.g. /mnt/creds/x509up) is
        resolved — at submit time on the submit machine. HTCondor reads
        the proxy and delegates it to the schedd/execute node. The path
        never appears in .sub files that DAGMan reads on the schedd.
        """
        proxy = os.environ.get("X509_USER_PROXY", "")
        if proxy and os.path.isfile(proxy):
            sub["x509userproxy"] = proxy

    def _submit_job_sync(self, submit_file: str) -> tuple[str, str]:
        schedd = self._get_schedd()
        with open(submit_file) as f:
            text = f.read()
        sub = htcondor2.Submit(text)
        self._set_proxy(sub)
        result = schedd.submit(sub)
        cluster_id = str(result.cluster())
        return (cluster_id, self._default_schedd_name)

    async def submit_job(self, submit_file: str) -> tuple[str, str]:
        return await asyncio.to_thread(self._submit_job_sync, submit_file)

    @staticmethod
    def _collect_dag_files(dag_file: str) -> list[str]:
        """Collect all files under the DAG's submit directory.

        Returns paths relative to submit_dir for use with
        preserve_relative_paths + transfer_input_files when spooling.
        """
        submit_dir = Path(dag_file).parent
        files = []
        for f in submit_dir.rglob("*"):
            if f.is_file():
                files.append(str(f.relative_to(submit_dir)))
        return files

    def _submit_dag_sync(self, dag_file: str, force: bool = False,
                         spool: bool = False,
                         schedd_name: str | None = None) -> tuple[str, str]:
        schedd = self._get_schedd(schedd_name)
        options = {"Force": True} if force else {}
        submit_dir = Path(dag_file).parent
        dag_basename = Path(dag_file).name

        prev_cwd = os.getcwd()
        try:
            os.chdir(str(submit_dir))

            if spool:
                # Spool mode: use relative DAG path so from_dag() generates
                # relative arguments. DAGMan's CWD = spool subdir on schedd.
                dag_submit = htcondor2.Submit.from_dag(dag_basename, options=options)
                self._set_proxy(dag_submit)

                # Don't transfer our condor_dagman binary — use the
                # remote schedd's own /usr/bin/condor_dagman.
                dag_submit["transfer_executable"] = "false"

                # from_dag() copies our local env via getenv, which exports
                # PATH/HOME from the submit machine. Disable getenv so the
                # scheduler universe job inherits the schedd's environment.
                dag_submit["getenv"] = "false"

                # from_dag() embeds our local condor version in -CsdVersion.
                # If the remote schedd runs a different version, DAGMan
                # refuses to start. Add -AllowVersionMismatch.
                # The arguments value is double-quoted: "-f ... -force",
                # so insert before the closing quote.
                args = dag_submit.get("arguments", "")
                if "-AllowVersionMismatch" not in args:
                    if args.endswith('"'):
                        args = args[:-1] + ' -AllowVersionMismatch"'
                    else:
                        args = args + " -AllowVersionMismatch"
                    dag_submit["arguments"] = args

                # Merge dagman config env vars with those from_dag() already
                # set (e.g. _CONDOR_DAGMAN_LOG, _CONDOR_MAX_DAGMAN_LOG).
                # DAGMAN_GENERATE_SUBDAG_SUBMITS=false: we pre-generate
                # .condor.sub files for sub-DAGs (the remote schedd's
                # condor_submit_dag may not work in the spool context).
                existing_env = dag_submit.get("environment", "").strip('"')
                extra_env = (
                    "_CONDOR_DAGMAN_USE_STRICT=0 "
                    "_CONDOR_DAGMAN_MAX_RESCUE_NUM=10 "
                    "_CONDOR_DAGMAN_USER_LOG_SCAN_INTERVAL=5 "
                    "_CONDOR_DAGMAN_GENERATE_SUBDAG_SUBMITS=false"
                )
                merged = f"{existing_env} {extra_env}".strip()
                dag_submit["environment"] = f'"{merged}"'

                transfer_files = self._collect_dag_files(dag_file)
                dag_submit["transfer_input_files"] = ", ".join(transfer_files)
                dag_submit["preserve_relative_paths"] = "true"
                result = schedd.submit(dag_submit, spool=True)
                schedd.spool(result)
            else:
                # Local mode: absolute DAG path so DAGMan (scheduler universe,
                # forked by schedd with schedd's CWD) can find everything.
                abs_dag = str(submit_dir.resolve() / dag_basename)
                dag_submit = htcondor2.Submit.from_dag(abs_dag, options=options)
                self._set_proxy(dag_submit)
                result = schedd.submit(dag_submit)
        finally:
            os.chdir(prev_cwd)

        cluster_id = str(result.cluster())
        actual_name = schedd_name or self._default_schedd_name
        return (cluster_id, actual_name)

    async def submit_dag(self, dag_file: str, force: bool = False,
                         spool: bool = False,
                         schedd_name: str | None = None) -> tuple[str, str]:
        return await asyncio.to_thread(
            self._submit_dag_sync, dag_file, force, spool, schedd_name
        )

    def _query_job_sync(self, cluster_id: str,
                        schedd_name: str | None = None) -> dict[str, Any] | None:
        schedd = self._get_schedd(schedd_name)
        constraint = f"ClusterId == {cluster_id}"
        projection = [
            "ClusterId", "ProcId", "JobStatus", "DAG_NodesTotal",
            "DAG_NodesDone", "DAG_NodesFailed", "DAG_NodesQueued",
            "DAG_NodesReady", "DAG_NodesUnready", "DAG_Status",
        ]
        ads = schedd.query(constraint=constraint, projection=projection)
        if not ads:
            return None
        ad = ads[0]
        return {k: ad[k] for k in ad.keys()}

    async def query_job(self, schedd_name: str, cluster_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._query_job_sync, cluster_id, schedd_name)

    def _check_job_completed_sync(self, cluster_id: str,
                                  schedd_name: str | None = None) -> bool:
        schedd = self._get_schedd(schedd_name)
        constraint = f"ClusterId == {cluster_id}"
        ads = schedd.query(constraint=constraint, projection=["JobStatus"])
        if ads:
            return False
        for ad in schedd.history(
            constraint=constraint,
            projection=["JobStatus"],
            match=1,
        ):
            return int(ad["JobStatus"]) == 4
        return True

    async def check_job_completed(self, cluster_id: str, schedd_name: str) -> bool:
        return await asyncio.to_thread(self._check_job_completed_sync, cluster_id, schedd_name)

    def _remove_job_sync(self, cluster_id: str,
                         schedd_name: str | None = None) -> None:
        schedd = self._get_schedd(schedd_name)
        schedd.act(
            htcondor2.JobAction.Remove,
            f"ClusterId == {cluster_id}",
        )

    async def remove_job(self, schedd_name: str, cluster_id: str) -> None:
        await asyncio.to_thread(self._remove_job_sync, cluster_id, schedd_name)

    def _ping_schedd_sync(self, schedd_name: str | None = None) -> bool:
        try:
            schedd = self._get_schedd(schedd_name)
            schedd.query(constraint="false", projection=["ClusterId"], limit=1)
            return True
        except Exception:
            return False

    async def ping_schedd(self, schedd_name: str) -> bool:
        return await asyncio.to_thread(self._ping_schedd_sync, schedd_name)

    _JOB_STATUS_MAP = {
        1: "idle",
        2: "running",
        3: "removed",
        4: "completed",
        5: "held",
    }

    def _query_dag_jobs_sync(self, cluster_id: str,
                             schedd_name: str | None = None) -> list[dict]:
        """Query per-job details for payload jobs under a DAGMan hierarchy."""
        schedd = self._get_schedd(schedd_name)
        sub_dagman_ads = schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=["ClusterId"],
        )
        if not sub_dagman_ads:
            return []

        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7"

        projection = [
            "DAGNodeName", "JobStatus", "ResidentSetSize_RAW",
            "CumulativeRemoteUserCpu", "CumulativeRemoteSysCpu",
            "RequestCpus", "RequestMemory", "JobStartDate", "ServerTime",
            "MATCH_GLIDEIN_CMSSite", "JobPrio",
            "HoldReason", "HoldReasonCode",
        ]
        ads = schedd.query(constraint=constraint, projection=projection)

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

            job = {
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
                "priority": int(ad.get("JobPrio", 0)),
            }
            if status_int == 5:
                job["hold_reason"] = str(ad.get("HoldReason", ""))
                job["hold_reason_code"] = int(ad.get("HoldReasonCode", 0))
            jobs.append(job)

        jobs.sort(key=lambda j: j["name"])
        return jobs

    async def query_dag_jobs(self, cluster_id: str,
                             schedd_name: str | None = None) -> list[dict] | None:
        return await asyncio.to_thread(self._query_dag_jobs_sync, cluster_id, schedd_name)

    def _count_dag_jobs_sync(self, cluster_id: str,
                             schedd_name: str | None = None) -> dict[str, int]:
        """Count nodes by status across all inner sub-DAGMans."""
        schedd = self._get_schedd(schedd_name)
        sub_dagman_ads = schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=[
                "ClusterId", "DAG_NodesTotal", "DAG_NodesDone",
                "DAG_NodesFailed", "DAG_NodesReady", "DAG_NodesUnready",
            ],
        )

        if not sub_dagman_ads:
            return {"idle": 0, "running": 0, "done": 0, "held": 0, "failed": 0, "total": 0}

        counts = {"idle": 0, "running": 0, "done": 0, "held": 0, "failed": 0, "total": 0}
        for ad in sub_dagman_ads:
            counts["done"] += int(ad.get("DAG_NodesDone", 0))
            counts["failed"] += int(ad.get("DAG_NodesFailed", 0))
            counts["total"] += int(ad.get("DAG_NodesTotal", 0))
            counts["idle"] += (
                int(ad.get("DAG_NodesReady", 0))
                + int(ad.get("DAG_NodesUnready", 0))
            )

        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7"
        job_ads = schedd.query(
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

    async def count_dag_jobs(self, cluster_id: str,
                             schedd_name: str | None = None) -> dict[str, int] | None:
        return await asyncio.to_thread(self._count_dag_jobs_sync, cluster_id, schedd_name)

    def _query_held_jobs_sync(self, cluster_id: str,
                              schedd_name: str | None = None) -> list[dict]:
        """Query held payload jobs under a DAGMan hierarchy."""
        schedd = self._get_schedd(schedd_name)
        sub_dagman_ads = schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=["ClusterId"],
        )
        if not sub_dagman_ads:
            return []

        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7 && JobStatus == 5"

        projection = [
            "ClusterId", "ProcId", "DAGNodeName",
            "HoldReasonCode", "HoldReason",
            "RequestMemory", "RequestCpus", "Iwd",
        ]
        ads = schedd.query(constraint=constraint, projection=projection)

        return [
            {
                "cluster_id": int(ad["ClusterId"]),
                "proc_id": int(ad.get("ProcId", 0)),
                "node_name": ad.get("DAGNodeName", ""),
                "hold_reason_code": int(ad.get("HoldReasonCode", 0)),
                "hold_reason": str(ad.get("HoldReason", "")),
                "request_memory": int(ad.get("RequestMemory", 0)),
                "request_cpus": int(ad.get("RequestCpus", 1)),
                "iwd": str(ad.get("Iwd", "")),
            }
            for ad in ads
        ]

    async def query_held_jobs(self, cluster_id: str,
                              schedd_name: str | None = None) -> list[dict]:
        return await asyncio.to_thread(self._query_held_jobs_sync, cluster_id, schedd_name)

    def _edit_job_attr_sync(self, constraint: str, attr: str, value: str,
                            schedd_name: str | None = None) -> None:
        schedd = self._get_schedd(schedd_name)
        schedd.edit(constraint, attr, value)

    async def edit_job_attr(self, constraint: str, attr: str, value: str,
                            schedd_name: str | None = None) -> None:
        return await asyncio.to_thread(self._edit_job_attr_sync, constraint, attr, value, schedd_name)

    def _release_jobs_sync(self, constraint: str,
                           schedd_name: str | None = None) -> None:
        schedd = self._get_schedd(schedd_name)
        schedd.act(htcondor2.JobAction.Release, constraint)

    async def release_jobs(self, constraint: str,
                           schedd_name: str | None = None) -> None:
        await asyncio.to_thread(self._release_jobs_sync, constraint, schedd_name)

    def _get_job_iwd_sync(self, cluster_id: str,
                          schedd_name: str | None = None) -> str | None:
        """Query a job's Iwd (initial working directory) classad."""
        schedd = self._get_schedd(schedd_name)
        constraint = f"ClusterId == {cluster_id}"
        ads = schedd.query(constraint=constraint, projection=["Iwd"])
        if not ads:
            return None
        return str(ads[0].get("Iwd", ""))

    async def get_job_iwd(self, cluster_id: str,
                          schedd_name: str | None = None) -> str | None:
        return await asyncio.to_thread(self._get_job_iwd_sync, cluster_id, schedd_name)

    def _query_dag_site_summary_sync(
        self, cluster_id: str, schedd_name: str | None = None,
    ) -> dict[str, dict[str, int]]:
        """Per-site job status counts for payload jobs under a DAGMan hierarchy."""
        schedd = self._get_schedd(schedd_name)
        sub_dagman_ads = schedd.query(
            constraint=f"DAGManJobId == {cluster_id} && JobUniverse == 7",
            projection=["ClusterId"],
        )
        if not sub_dagman_ads:
            return {}

        sub_ids = [str(int(ad["ClusterId"])) for ad in sub_dagman_ads]
        parts = " || ".join(f"DAGManJobId == {sid}" for sid in sub_ids)
        constraint = f"({parts}) && JobUniverse =!= 7"

        ads = schedd.query(
            constraint=constraint,
            projection=["JobStatus", "MATCH_GLIDEIN_CMSSite", "DESIRED_Sites"],
        )

        is_local = schedd_name in (None, "localhost", self._default_schedd_name)

        result: dict[str, dict[str, int]] = {}
        for ad in ads:
            status_int = int(ad.get("JobStatus", 0))
            site = ad.get("MATCH_GLIDEIN_CMSSite") or ""
            if not site:
                if is_local:
                    site = "local"
                else:
                    # Use DESIRED_Sites for pinned idle/held jobs
                    desired = ad.get("DESIRED_Sites") or ""
                    if desired:
                        site = desired
                    elif status_int == 1:
                        site = "pending"
            if site not in result:
                result[site] = {"running": 0, "idle": 0, "held": 0}
            if status_int == 1:
                result[site]["idle"] += 1
            elif status_int == 2:
                result[site]["running"] += 1
            elif status_int == 5:
                result[site]["held"] += 1
        return result

    async def query_dag_site_summary(
        self, cluster_id: str, schedd_name: str | None = None,
    ) -> dict[str, dict[str, int]]:
        return await asyncio.to_thread(
            self._query_dag_site_summary_sync, cluster_id, schedd_name
        )
