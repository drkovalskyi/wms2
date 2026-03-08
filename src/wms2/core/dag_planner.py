"""DAG Planner: pilot submission, production DAG planning, and DAG file generation."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from wms2.adapters.base import CondorAdapter, DBSAdapter, RucioAdapter
from wms2.config import Settings
from wms2.core.splitters import DAGNodeSpec, InputFile, get_splitter
from wms2.db.repository import Repository
from wms2.models.enums import DAGStatus, WorkflowStatus

logger = logging.getLogger(__name__)

# Module-level cache for pileup file lists.
# Key: dataset name, Value: (timestamp, file_list)
_pileup_cache: dict[str, tuple[float, list[str]]] = {}
PILEUP_CACHE_TTL = 24 * 3600  # 24 hours


def _effective_stageout(mode: str) -> str:
    """Normalize stageout_mode for DAG generation.

    Both 'test' and 'production' use grid stageout (xrdcp via storage.json).
    The DAG planner only cares about 'local' vs 'grid'.
    """
    if mode in ("test", "production", "grid"):
        return "grid"
    return "local"


# ── Data classes ────────────────────────────────────────────────


@dataclass
class PilotMetrics:
    """Performance metrics extracted from a pilot job's JSON report."""
    events_per_second: float = 1.0
    memory_peak_mb: int = 2000
    output_size_per_event_kb: float = 50.0
    time_per_event_sec: float = 1.0
    cpu_efficiency: float = 0.8
    steps: list[dict] | None = None
    per_output_module: dict | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> PilotMetrics:
        # New-format reports have a "summary" section; old-format has flat fields
        summary = data.get("summary", {})
        return cls(
            events_per_second=float(
                summary.get("events_per_second", data.get("events_per_second", 1.0))
            ),
            memory_peak_mb=int(
                summary.get("peak_rss_mb", data.get("memory_peak_mb", 2000))
            ),
            output_size_per_event_kb=float(
                summary.get("output_size_per_event_kb", data.get("output_size_per_event_kb", 50.0))
            ),
            time_per_event_sec=float(
                summary.get("total_time_per_event_sec", data.get("time_per_event_sec", 1.0))
            ),
            cpu_efficiency=float(data.get("cpu_efficiency", 0.8)),
            steps=data.get("steps"),
            per_output_module=summary.get("per_output_module"),
        )

    @classmethod
    def from_request(cls, config: dict) -> PilotMetrics:
        """Create metrics from request spec fields."""
        tpe = float(config.get("time_per_event", 1.0))
        return cls(
            time_per_event_sec=tpe,
            memory_peak_mb=int(config.get("memory_mb", 2000)),
            output_size_per_event_kb=float(config.get("size_per_event", 50.0)),
            events_per_second=1.0 / max(tpe, 0.001),
        )


@dataclass
class PlanningMergeGroup:
    """A merge group being assembled during planning."""
    group_index: int
    processing_nodes: list[DAGNodeSpec] = field(default_factory=list)


# ── DAG Planner ────────────────────────────────────────────────


class DAGPlanner:
    def __init__(
        self,
        repository: Repository,
        dbs_adapter: DBSAdapter,
        rucio_adapter: RucioAdapter,
        condor_adapter: CondorAdapter,
        settings: Settings,
        site_manager=None,
    ):
        self.db = repository
        self.dbs = dbs_adapter
        self.rucio = rucio_adapter
        self.condor = condor_adapter
        self.settings = settings
        self.site_manager = site_manager

    # ── Pilot Phase ──────────────────────────────────────────

    async def submit_pilot(self, workflow) -> None:
        """Write a pilot submit file and the pilot script, then submit."""
        from wms2.core.pilot_runner import write_pilot_script

        submit_dir = os.path.join(
            self.settings.submit_base_dir, str(workflow.id), "pilot"
        )
        os.makedirs(submit_dir, exist_ok=True)

        # Generate the standalone pilot script
        pilot_script_path = os.path.join(submit_dir, "wms2_pilot.py")
        write_pilot_script(pilot_script_path)

        # Fetch a small sample of files
        files = await self.dbs.get_files(workflow.input_dataset, limit=5)
        if not files:
            logger.warning("No files found for pilot of workflow %s", workflow.id)
            return

        # Resolve sandbox path from workflow config
        config = workflow.config_data or {}
        sandbox_path = config.get("sandbox_path", "")

        pilot_config = _compute_pilot_config(config, self.settings)

        pilot_sub = _generate_pilot_submit(
            submit_dir=submit_dir,
            sandbox_path=sandbox_path or workflow.sandbox_url,
            input_files=[f["logical_file_name"] for f in files],
            pilot_config=pilot_config,
        )
        pilot_path = os.path.join(submit_dir, "pilot.sub")
        _write_file(pilot_path, pilot_sub)

        # Submit pilot to HTCondor
        cluster_id, schedd = await self.condor.submit_job(pilot_path)

        await self.db.update_workflow(
            workflow.id,
            status=WorkflowStatus.PILOT_RUNNING.value,
            pilot_output_path=submit_dir,
            pilot_cluster_id=cluster_id,
            pilot_schedd=schedd,
        )
        logger.info(
            "Pilot submitted for workflow %s: cluster=%s schedd=%s",
            workflow.id, cluster_id, schedd,
        )

    def _parse_pilot_report(self, path: str) -> PilotMetrics:
        """Parse pilot JSON report from disk."""
        from wms2.core.pilot_runner import parse_pilot_report
        data = parse_pilot_report(path)
        return PilotMetrics.from_json(data)

    async def handle_pilot_completion(
        self, workflow, report_path: str, *, adaptive: bool = False,
    ) -> Any:
        """Parse pilot metrics and proceed to production DAG planning.

        Pilot results are stored for reference but don't drive merge group
        sizing — that uses fixed jobs_per_work_unit. Measured memory overrides
        the request hint if available.
        """
        from wms2.core.pilot_runner import parse_pilot_report

        config = workflow.config_data or {}
        metrics = PilotMetrics.from_request(config)

        # Store pilot results for reference; override memory if measured
        try:
            pilot_data = parse_pilot_report(report_path)
            summary = pilot_data.get("summary", {})
            if summary.get("peak_rss_mb", 0) > 0:
                metrics.memory_peak_mb = int(summary["peak_rss_mb"])
            if summary.get("per_output_module"):
                metrics.per_output_module = summary["per_output_module"]
            metrics.steps = pilot_data.get("steps")
        except Exception as exc:
            logger.warning("Pilot report parse failed (%s), using request hints", exc)
            pilot_data = {}

        await self.db.update_workflow(
            workflow.id,
            step_metrics=pilot_data,
        )
        return await self.plan_production_dag(
            workflow, metrics=metrics, adaptive=adaptive,
        )

    # ── Production DAG ───────────────────────────────────────

    async def plan_production_dag(
        self, workflow, metrics: PilotMetrics | None = None,
        *, adaptive: bool = False,
    ) -> Any:
        """Full pipeline: fetch files → split → merge groups → write DAG files → create DB row.

        When *adaptive* is True the planner caps each round to
        ``settings.work_units_per_round * settings.jobs_per_work_unit`` jobs
        and reads ``workflow.next_first_event`` / ``workflow.file_offset`` to
        skip already-processed data.
        """
        # 1. Resource parameters
        if metrics is None:
            metrics = PilotMetrics()  # defaults

        config = workflow.config_data or {}
        is_gen = config.get("_is_gen", False)

        current_round = getattr(workflow, "current_round", 0) or 0

        # Adaptive cap: limit the number of processing jobs per round
        if adaptive:
            if current_round == 0:
                wus = self.settings.first_round_work_units
            else:
                wus = config.get("work_units_per_round") or self.settings.work_units_per_round
            max_jobs = wus * self.settings.jobs_per_work_unit
        else:
            max_jobs = 0

        t_plan = time.monotonic()
        if is_gen:
            # GEN workflow: no input files, create synthetic event-range nodes
            nodes = self._plan_gen_nodes(workflow, config, max_jobs=max_jobs)
        else:
            nodes = await self._plan_file_based_nodes(workflow, max_jobs=max_jobs)
        logger.info("Plan %s round %d: %d proc nodes planned in %.1fs",
                     workflow.request_name, current_round, len(nodes) if nodes else 0,
                     time.monotonic() - t_plan)

        if not nodes:
            if adaptive:
                return None  # All work done — caller handles completion
            raise ValueError(
                f"No processing nodes generated for workflow {workflow.id}"
            )

        # Plan merge groups — deferred until after resource_params is built
        # (adaptive job_split may override jobs_per_wu via _jobs_per_wu_override)

        # 7. Generate DAG files on disk
        # Round-aware submit directory for round 2+
        base_submit_dir = os.path.join(self.settings.submit_base_dir, str(workflow.id))
        if current_round > 0:
            submit_dir = os.path.join(base_submit_dir, f"round_{current_round:03d}")
        else:
            submit_dir = base_submit_dir
        os.makedirs(submit_dir, exist_ok=True)
        executables = {
            "processing": self.settings.processing_executable,
            "merge": self.settings.merge_executable,
            "cleanup": self.settings.cleanup_executable,
        }
        # Extract output dataset info from workflow config_data
        config = workflow.config_data or {}
        output_datasets = config.get("output_datasets")
        # Extract sandbox path and resource params from config_data
        sandbox_path = config.get("sandbox_path")
        memory_mb = config.get("memory_mb", 0)
        resource_params = {}
        if memory_mb:
            resource_params["memory_mb"] = int(memory_mb)
        disk_kb = config.get("disk_kb", 0)
        if disk_kb:
            resource_params["disk_kb"] = int(disk_kb)
        ncpus = config.get("multicore", 0)
        if ncpus:
            resource_params["ncpus"] = int(ncpus)
        test_fraction = config.get("test_fraction")
        if test_fraction:
            resource_params["test_fraction"] = float(test_fraction)
        # filter_efficiency is used at planning time (inflating total_events in
        # _plan_gen_nodes) but not passed to the processing wrapper.

        # ── Apply adaptive params from prior round optimization ──
        if adaptive and current_round > 0:
            step_metrics = getattr(workflow, "step_metrics", None)
            adaptive_params = (
                step_metrics.get("adaptive_params")
                if isinstance(step_metrics, dict) else None
            )
            if isinstance(adaptive_params, dict):
                tuned_memory = adaptive_params.get("tuned_memory_mb")
                if tuned_memory and tuned_memory > 0:
                    old_mem = resource_params.get("memory_mb", memory_mb)
                    resource_params["memory_mb"] = int(tuned_memory)
                    logger.info(
                        "Adaptive: memory %s -> %d MB [%s]",
                        old_mem, tuned_memory,
                        adaptive_params.get("memory_source", "?"),
                    )

                # Apply job splitting (unified — no mode dispatch)
                tuned_cpus = adaptive_params.get("tuned_request_cpus")
                if tuned_cpus and tuned_cpus > 0:
                    resource_params["ncpus"] = int(tuned_cpus)
                tuned_epj = adaptive_params.get("tuned_events_per_job")
                if tuned_epj and tuned_epj > 0:
                    params = workflow.splitting_params or {}
                    params["events_per_job"] = tuned_epj
                multiplier = adaptive_params.get("job_multiplier", 1)
                if multiplier > 1:
                    resource_params["_jobs_per_wu_override"] = (
                        self.settings.jobs_per_work_unit * multiplier
                    )

                # Per-step tuning (n_parallel, per-step nthreads)
                per_step = adaptive_params.get("per_step")
                if isinstance(per_step, dict):
                    resource_params["_per_step_tuning"] = per_step

        # Plan merge groups (after resource_params is fully built)
        jobs_per_wu = resource_params.pop("_jobs_per_wu_override", None)

        # Output-size-based WU sizing for rounds 1+ (when no explicit override)
        if adaptive and current_round > 0 and jobs_per_wu is None:
            step_metrics = getattr(workflow, "step_metrics", None)
            if isinstance(step_metrics, dict):
                computed = _compute_jobs_per_wu_from_write_mb(
                    step_metrics,
                    self.settings.target_merged_size_kb,
                    self.settings.max_jobs_per_work_unit,
                    test_fraction=config.get("test_fraction", 1.0),
                )
                if computed is not None:
                    jobs_per_wu = computed
                    logger.info(
                        "Output-size WU sizing: %d jobs/WU (target %d KB merged)",
                        computed, self.settings.target_merged_size_kb,
                    )

        merge_groups = _plan_merge_groups(
            nodes,
            jobs_per_group=jobs_per_wu or self.settings.jobs_per_work_unit,
        )

        # Query banned sites for this workflow
        banned_sites: list[str] = []
        if self.site_manager:
            banned_sites = await self.site_manager.get_banned_sites(
                workflow_id=workflow.id
            )

        # Resolve pileup file availability via Rucio (with 24h cache)
        t_pileup = time.monotonic()
        pileup_files: dict[str, list[str]] = {}
        manifest_steps = config.get("manifest_steps", [])
        if self.rucio and manifest_steps:
            now_ts = time.time()
            for step in manifest_steps:
                for pu_field in ("mc_pileup", "data_pileup"):
                    ds = step.get(pu_field, "")
                    if ds and ds not in pileup_files:
                        # Check cache first
                        cached = _pileup_cache.get(ds)
                        if cached and (now_ts - cached[0]) < PILEUP_CACHE_TTL:
                            pileup_files[ds] = cached[1]
                            logger.info("Pileup %s: %d files (cached)", ds, len(cached[1]))
                            continue
                        t_pu = time.monotonic()
                        try:
                            preferred = [
                                r.strip()
                                for r in (self.settings.pileup_preferred_rses or "").split(",")
                                if r.strip()
                            ] or None
                            files = await self.rucio.get_available_pileup_files(ds, preferred_rses=preferred)
                            pileup_files[ds] = files
                            _pileup_cache[ds] = (now_ts, files)
                            logger.info("Pileup %s: %d files in %.1fs%s",
                                        ds, len(files), time.monotonic() - t_pu,
                                        f" (preferred RSEs {preferred})" if preferred else "")
                        except Exception:
                            logger.warning(
                                "Pileup %s: Rucio query failed after %.1fs",
                                ds, time.monotonic() - t_pu, exc_info=True,
                            )
        pileup_elapsed = time.monotonic() - t_pileup
        if pileup_elapsed > 1.0:
            logger.info("Plan %s: pileup resolution took %.1fs", workflow.request_name, pileup_elapsed)

        # Determine job priority from priority profile
        priority_profile = config.get("priority_profile", {})
        if current_round == 0:
            job_priority = priority_profile.get("pilot", self.settings.default_pilot_priority)
        else:
            progress = (workflow.events_produced or 0) / max(workflow.target_events or 1, 1)
            switch_at = priority_profile.get("switch_fraction", 0.5)
            if progress < switch_at:
                job_priority = priority_profile.get("high", 5)
            else:
                job_priority = priority_profile.get("nominal", 3)
        if job_priority:
            logger.info("Round %d: job priority = %d", current_round, job_priority)

        # Store current priority on request row for UI display
        try:
            req_row = await self.repo.get_request(workflow.request_name)
            if req_row:
                rd = dict(req_row.request_data or {})
                rd["_current_job_priority"] = job_priority
                await self.repo.update_request(workflow.request_name, request_data=rd)
        except Exception:
            logger.debug("Could not store job priority on request row", exc_info=True)

        # Determine if we should use spool mode (remote schedd).
        # Per-request condor_pool overrides the global setting.
        condor_pool = config.get("condor_pool", "local")
        has_spool_config = bool(self.settings.spool_mount and self.settings.remote_spool_prefix)
        use_spool = condor_pool == "global" and has_spool_config

        # Fetch all CMS site names for DESIRED_Sites (required by GlideinWMS).
        # When allowed_sites is configured, only those are used; otherwise
        # all CRIC sites are listed so glideins at any site can match.
        all_site_names: list[str] = []
        if use_spool:
            try:
                site_rows = await self.db.list_sites()
                all_site_names = [s.name for s in site_rows]
            except Exception:
                logger.warning("Could not fetch site list for DESIRED_Sites")

        t_gen = time.monotonic()
        effective_stageout = _effective_stageout(
            config.get("stageout_mode", self.settings.stageout_mode)
        )
        effective_allowed = (
            [s.strip() for s in config.get("allowed_sites", "").split(",") if s.strip()]
            or [s.strip() for s in self.settings.allowed_sites.split(",") if s.strip()]
            or None
        )

        # In test stageout mode, restrict to sites with _Temp RSE entries
        # so that Rucio registration and consolidation can succeed.
        raw_stageout = config.get("stageout_mode", self.settings.stageout_mode)
        if raw_stageout == "test" and use_spool:
            from wms2.core.output_manager import TEMP_RSE_PFN_PREFIXES
            rucio_sites = {
                rse.replace("_Temp", "")
                for rse in TEMP_RSE_PFN_PREFIXES
            }
            if rucio_sites:
                if effective_allowed:
                    effective_allowed = [s for s in effective_allowed if s in rucio_sites]
                else:
                    effective_allowed = sorted(rucio_sites)
                logger.info("Test mode: restricted to Rucio-enabled sites: %s", effective_allowed)

        dag_file_path = _generate_dag_files(
            submit_dir=submit_dir,
            workflow_id=str(workflow.id),
            merge_groups=merge_groups,
            sandbox_url=workflow.sandbox_url,
            category_throttles=workflow.category_throttles or {
                "Processing": 5000, "Merge": 100, "Cleanup": 50,
            },
            executables=executables,
            output_datasets=output_datasets,
            local_pfn_prefix=self.settings.local_pfn_prefix,
            sandbox_path=sandbox_path,
            resource_params=resource_params or None,
            banned_sites=banned_sites or None,
            pileup_files=pileup_files or None,
            job_priority=job_priority,
            extra_classads=config.get("extra_classads"),
            stageout_mode=effective_stageout,
            pileup_remote_read=self.settings.pileup_remote_read,
            spool_mode=use_spool,
            allowed_sites=effective_allowed,
            all_sites=all_site_names or None,
        )

        logger.info("Plan %s: DAG files generated in %.1fs (%d WUs, %s)",
                     workflow.request_name, time.monotonic() - t_gen,
                     len(merge_groups), "spool" if use_spool else "local")

        # 8. Count totals
        stageout_mode = _effective_stageout(config.get("stageout_mode", self.settings.stageout_mode))
        skip_site_pinning = (stageout_mode == "grid")
        total_proc = sum(len(mg.processing_nodes) for mg in merge_groups)
        if skip_site_pinning:
            # No landing node: N proc + 1 merge + 1 cleanup = N + 2
            total_nodes = sum(len(mg.processing_nodes) + 2 for mg in merge_groups)
            # proc→merge + merge→cleanup = N + 1
            total_edges = sum(len(mg.processing_nodes) + 1 for mg in merge_groups)
        else:
            # With landing: 1 landing + N proc + 1 merge + 1 cleanup = N + 3
            total_nodes = sum(len(mg.processing_nodes) + 3 for mg in merge_groups)
            # landing→all_proc + all_proc→merge + merge→cleanup = 2N + 1
            total_edges = sum(2 * len(mg.processing_nodes) + 1 for mg in merge_groups)

        # 9. Create DAG row (paths may be updated after spool submission)
        node_counts = {
            "processing": total_proc,
            "merge": len(merge_groups),
            "cleanup": len(merge_groups),
        }
        if not skip_site_pinning:
            node_counts["landing"] = len(merge_groups)
        dag = await self.db.create_dag(
            workflow_id=workflow.id,
            dag_file_path=dag_file_path,
            submit_dir=submit_dir,
            total_nodes=total_nodes,
            total_edges=total_edges,
            node_counts=node_counts,
            total_work_units=len(merge_groups),
            status=DAGStatus.READY.value,
        )

        # 10. Create processing blocks for output management (round 0 only —
        #     subsequent rounds reuse existing blocks)
        if output_datasets and current_round == 0:
            for i, ds in enumerate(output_datasets):
                await self.db.create_processing_block(
                    workflow_id=workflow.id,
                    block_index=i,
                    dataset_name=ds.get("dataset_name", ""),
                    total_work_units=len(merge_groups),
                )

        # 10b. Set production targets on round 0
        if current_round == 0:
            target_kwargs = {}
            if is_gen:
                request_num_events = int(config.get("request_num_events") or 0)
                tf = float(config.get("test_fraction") or 1.0)
                target = int(request_num_events * tf)
                if target > 0:
                    target_kwargs["target_events"] = target
            else:
                # File-based: total input files from what DBS returned
                # (already filtered by limit/availability in _plan_file_based_nodes)
                total_files = 0
                if self.dbs and workflow.input_dataset:
                    limit = self.settings.max_input_files if self.settings.max_input_files > 0 else 0
                    all_files = await self.dbs.get_files(workflow.input_dataset, limit=limit)
                    total_files = len(all_files)
                if total_files > 0:
                    target_kwargs["total_input_files"] = total_files
            if target_kwargs:
                await self.db.update_workflow(workflow.id, **target_kwargs)

        # 11. Submit DAG to HTCondor
        t_submit = time.monotonic()
        remote_schedd = self.settings.remote_schedd if use_spool else None
        cluster_id, schedd = await self.condor.submit_dag(
            dag_file_path, force=True, spool=use_spool,
            schedd_name=remote_schedd,
        )
        logger.info("Plan %s: HTCondor submit in %.1fs (cluster=%s, schedd=%s)",
                     workflow.request_name, time.monotonic() - t_submit, cluster_id, schedd)
        now = datetime.now(timezone.utc)

        # For spool mode, map the remote spool Iwd to local sshfs mount
        # so monitoring can read status files, metrics, manifests.
        if use_spool:
            iwd = await self.condor.get_job_iwd(cluster_id, schedd)
            if iwd:
                mapped_dir = iwd.replace(
                    self.settings.remote_spool_prefix,
                    self.settings.spool_mount, 1,
                )
                submit_dir = mapped_dir
                dag_file_path = os.path.join(mapped_dir, "workflow.dag")
                await self.db.update_dag(
                    dag.id, submit_dir=submit_dir, dag_file_path=dag_file_path,
                )
                logger.info("Spool mode: mapped submit_dir=%s", submit_dir)

        await self.db.update_dag(
            dag.id,
            dagman_cluster_id=cluster_id,
            schedd_name=schedd,
            status=DAGStatus.SUBMITTED.value,
            submitted_at=now,
        )

        await self.db.update_workflow(
            workflow.id,
            dag_id=dag.id,
            status=WorkflowStatus.ACTIVE.value,
            total_nodes=total_nodes,
        )

        logger.info(
            "Production DAG planned for workflow %s (round %d): "
            "%d groups, %d proc nodes, path=%s, spool=%s",
            workflow.id, current_round, len(merge_groups), total_proc,
            dag_file_path, use_spool,
        )
        return dag

    async def _plan_file_based_nodes(
        self, workflow, *, max_jobs: int = 0,
    ) -> list[DAGNodeSpec]:
        """Fetch input files from DBS and split them into processing nodes.

        For adaptive rounds, ``workflow.file_offset`` skips files processed
        in prior rounds and ``max_jobs`` caps the number of jobs planned.
        """
        limit = self.settings.max_input_files if self.settings.max_input_files > 0 else 0
        raw_files = await self.dbs.get_files(workflow.input_dataset, limit=limit)
        if not raw_files:
            return []

        # Adaptive offset: skip files already processed in prior rounds
        file_offset = getattr(workflow, "file_offset", 0) or 0
        if file_offset > 0:
            raw_files = raw_files[file_offset:]
        if not raw_files:
            return []

        # Adaptive cap: limit files to max_jobs * files_per_job
        if max_jobs > 0:
            params = workflow.splitting_params or {}
            files_per_job = (
                params.get("files_per_job")
                or params.get("FilesPerJob")
                or 1
            )
            max_files_needed = max_jobs * files_per_job
            raw_files = raw_files[:max_files_needed]

        lfns = [f["logical_file_name"] for f in raw_files]
        try:
            replica_map = await self.rucio.get_replicas(lfns)
        except Exception as exc:
            logger.warning("Rucio replica lookup failed (%s), using empty locations", exc)
            replica_map = {}

        input_files = [
            InputFile(
                lfn=f["logical_file_name"],
                file_size=f.get("file_size", 0),
                event_count=f.get("event_count", 0),
                locations=replica_map.get(f["logical_file_name"], []),
            )
            for f in raw_files
        ]

        splitter = get_splitter(
            workflow.splitting_algo,
            workflow.splitting_params or {},
        )
        return splitter.split(input_files)

    def _plan_gen_nodes(
        self, workflow, config: dict, *, max_jobs: int = 0,
    ) -> list[DAGNodeSpec]:
        """Create synthetic processing nodes for GEN workflows (no input files).

        Uses RequestNumEvents and EventsPerJob from the request to compute
        the number of event-generation jobs needed.  For adaptive rounds,
        ``workflow.next_first_event`` skips already-processed events and
        ``max_jobs`` caps the batch size.

        Convention (matches WMAgent): EventsPerJob and TimePerEvent are in
        terms of *generated* events.  RequestNumEvents is the desired *output*
        count, so for GenFilter workflows we inflate it by 1/filter_efficiency
        to get the total number of generated events needed.
        """
        import math

        params = workflow.splitting_params or {}
        events_per_job = params.get("events_per_job") or params.get("eventsPerJob") or 100_000
        total_events = int(config.get("request_num_events") or 0)

        # For GenFilter workflows, RequestNumEvents is desired *output* events
        # but events_per_job is *generated* events per job.  Inflate total to
        # match, just like WMAgent's modifyJobSplitting does.
        filter_eff = float(config.get("filter_efficiency", 1.0))
        if filter_eff > 0 and filter_eff < 1.0 and total_events > 0:
            total_events = int(total_events / filter_eff)
            logger.info(
                "GenFilter workflow %s: inflated total_events to %d "
                "(requested_output / filter_eff=%.6f)",
                workflow.id, total_events, filter_eff,
            )

        if total_events <= 0:
            logger.warning("GEN workflow %s has no RequestNumEvents, using 1 node", workflow.id)
            total_events = events_per_job

        # Adaptive offset: skip events already processed in prior rounds
        start_event = getattr(workflow, "next_first_event", 1) or 1
        remaining_events = total_events - start_event + 1
        if remaining_events <= 0:
            return []

        num_jobs = math.ceil(remaining_events / events_per_job)

        # Adaptive cap: limit batch size per round
        if max_jobs > 0:
            num_jobs = min(num_jobs, max_jobs)

        nodes: list[DAGNodeSpec] = []
        for i in range(num_jobs):
            first_event = start_event + i * events_per_job
            last_event = min(start_event + (i + 1) * events_per_job - 1, total_events)
            actual_events = last_event - first_event + 1

            # Create a synthetic InputFile so the rest of the pipeline works
            synthetic_file = InputFile(
                lfn=f"synthetic://gen/events_{first_event}_{last_event}",
                file_size=0,
                event_count=actual_events,
                locations=[],
            )
            nodes.append(
                DAGNodeSpec(
                    node_index=i,
                    input_files=[synthetic_file],
                    first_event=first_event,
                    last_event=last_event,
                    events_per_job=actual_events,
                )
            )

        logger.info(
            "GEN workflow %s: %d nodes, %d events/job, start_event=%d, total=%d",
            workflow.id, num_jobs, events_per_job, start_event, total_events,
        )
        return nodes


# ── Output-Size WU Sizing ─────────────────────────────────────


def _compute_jobs_per_wu_from_write_mb(
    step_metrics: dict,
    target_merged_size_kb: int,
    max_jobs: int,
    test_fraction: float = 1.0,
) -> int | None:
    """Compute optimal jobs_per_work_unit from measured write_mb in step_metrics.

    Uses the smallest output tier's write_mb.mean to calculate how many jobs
    produce a target merged file size. When test_fraction < 1, the target is
    scaled down proportionally (e.g. 4 GB * 0.01 = 40 MB) so that merged files
    stay proportional to the test run. Returns None if no write_mb data found.
    """
    rounds = step_metrics.get("rounds", {})
    if not rounds:
        return None

    # Find the minimum write_mb.mean across all steps from the most recent round
    min_write_mb = None
    for round_num in sorted(rounds.keys(), key=int, reverse=True):
        round_data = rounds[round_num]
        wu_metrics = round_data.get("wu_metrics", [])
        for wu in wu_metrics:
            per_step = wu.get("per_step", {})
            for step_data in per_step.values():
                write_info = step_data.get("write_mb", {})
                mean_write = write_info.get("mean")
                if mean_write and mean_write > 0:
                    if min_write_mb is None or mean_write < min_write_mb:
                        min_write_mb = mean_write
        if min_write_mb is not None:
            break  # Use most recent round only

    if min_write_mb is None or min_write_mb <= 0:
        return None

    # target_merged_size_kb is in KB, write_mb is in MB
    # Scale target by test_fraction so test runs get proportionally smaller merges
    target_mb = target_merged_size_kb / 1024.0 * test_fraction
    optimal = int(target_mb / min_write_mb)

    # Clamp to [1, max_jobs]
    return max(1, min(optimal, max_jobs))


# ── Merge Group Planning ───────────────────────────────────────


def _plan_merge_groups(
    nodes: list[DAGNodeSpec],
    jobs_per_group: int,
) -> list[PlanningMergeGroup]:
    """Group processing nodes into fixed-size merge groups."""
    if not nodes:
        return []

    groups: list[PlanningMergeGroup] = []
    for i in range(0, len(nodes), jobs_per_group):
        chunk = nodes[i:i + jobs_per_group]
        mg = PlanningMergeGroup(group_index=len(groups))
        mg.processing_nodes = chunk
        groups.append(mg)

    return groups


# ── Pilot Config from Request Hints ────────────────────────────


def _compute_pilot_config(config: dict[str, Any], settings: Settings) -> dict[str, Any]:
    """Compute pilot parameters from request performance hints.

    The pilot is a functional smoke test, not a measurement tool. All
    planning parameters come from the request spec. We just need enough
    events to verify the chain runs successfully.
    """
    time_per_event = float(config.get("time_per_event", 0))
    filter_eff = float(config.get("filter_efficiency", 1.0))

    # Enough events to get ~10 through the filter (functional test)
    if filter_eff > 0 and filter_eff < 1.0:
        initial_events = max(10, min(int(10 / filter_eff), 10000))
    else:
        initial_events = settings.pilot_initial_events

    # Timeout based on expected time, 5x safety margin
    if time_per_event > 0:
        timeout = max(600, int(initial_events * time_per_event * 5))
    else:
        timeout = settings.pilot_step_timeout

    return {
        "initial_events": initial_events,
        "timeout": timeout,
        "ncpus": int(config.get("multicore", 0)),
        "memory_mb": int(config.get("memory_mb", 0)),
    }


# ── DAG File Generation (Appendix C format) ────────────────────


def _generate_dag_files(
    submit_dir: str,
    workflow_id: str,
    merge_groups: list[PlanningMergeGroup],
    sandbox_url: str,
    category_throttles: dict[str, int],
    executables: dict[str, str] | None = None,
    output_datasets: list[dict] | None = None,
    local_pfn_prefix: str = "",
    sandbox_path: str | None = None,
    resource_params: dict[str, int] | None = None,
    banned_sites: list[str] | None = None,
    pileup_files: dict[str, list[str]] | None = None,
    job_priority: int = 0,
    extra_classads: dict[str, str] | None = None,
    stageout_mode: str = "local",
    pileup_remote_read: bool = True,
    spool_mode: bool = False,
    allowed_sites: list[str] | None = None,
    all_sites: list[str] | None = None,
) -> str:
    """Generate all DAG files on disk. Returns path to outer workflow.dag."""
    submit_path = Path(submit_dir)

    # Write shared config and scripts
    _write_file(
        str(submit_path / "dagman.config"),
        "DAGMAN_USE_STRICT = 0\nDAGMAN_MAX_RESCUE_NUM = 10\nDAGMAN_USER_LOG_SCAN_INTERVAL = 5\n",
    )
    _write_elect_site_script(str(submit_path / "elect_site.sh"))
    _write_pin_site_script(str(submit_path / "pin_site.sh"))
    _write_post_script(str(submit_path / "post_script.sh"))
    _write_post_collector(str(submit_path / "wms2_post_collect.py"))
    _write_proc_script(str(submit_path / "wms2_proc.sh"))
    _write_merge_script(str(submit_path / "wms2_merge.py"))
    _write_cleanup_script(str(submit_path / "wms2_cleanup.py"))
    _write_stageout_utility(str(submit_path / "wms2_stageout.py"))

    # Symlink X509 proxy into submit_dir so .sub files can reference it
    # with a relative path (../x509_proxy from group_dir). The proxy's
    # actual location is irrelevant — only the symlink needs to be here.
    x509_proxy = os.environ.get("X509_USER_PROXY", "")
    if x509_proxy and os.path.isfile(x509_proxy):
        proxy_link = submit_path / "x509_proxy"
        if not proxy_link.exists():
            os.symlink(os.path.abspath(x509_proxy), str(proxy_link))

    # Generate outer DAG.
    # Local mode: absolute paths (scheduler universe inherits schedd's CWD).
    # Spool mode: relative paths (DAGMan CWD = spool subdir on remote schedd).
    if spool_mode:
        outer_lines = [
            f"# WMS2-generated DAG for workflow {workflow_id}",
            "NODE_STATUS_FILE workflow.dag.status",
            "",
        ]
    else:
        abs_submit = str(submit_path.resolve())
        outer_lines = [
            f"# WMS2-generated DAG for workflow {workflow_id}",
            f"CONFIG {abs_submit}/dagman.config",
            f"NODE_STATUS_FILE {abs_submit}/workflow.dag.status",
            "",
        ]

    for mg in merge_groups:
        mg_name = f"mg_{mg.group_index:06d}"
        mg_dir = submit_path / mg_name
        mg_dir.mkdir(parents=True, exist_ok=True)

        if spool_mode:
            # Spool mode: no DIR keyword — the inner DAGMan inherits the
            # outer DAG's CWD (spool root) and references files via
            # mg_name/ prefix.  No symlinks (they don't transfer in spool).
            outer_lines.append(
                f"SUBDAG EXTERNAL {mg_name} {mg_name}/group.dag"
            )
        else:
            # Local mode: symlink shared scripts into mg_dir so inner DAG
            # can reference them as ./script.sh (DIR sets IWD to mg_dir).
            for script in ("elect_site.sh", "pin_site.sh", "post_script.sh",
                            "wms2_post_collect.py"):
                link = mg_dir / script
                if not link.exists():
                    os.symlink(f"../{script}", str(link))

            abs_mg = f"{abs_submit}/{mg_name}"
            outer_lines.append(
                f"SUBDAG EXTERNAL {mg_name} {abs_mg}/group.dag DIR {abs_mg}"
            )

        # Write pileup_files.json if pileup datasets were resolved
        if pileup_files:
            _write_file(
                str(mg_dir / "pileup_files.json"),
                json.dumps(pileup_files, indent=2),
            )

        # Generate merge group sub-DAG
        _generate_group_dag(
            group_dir=mg_dir,
            submit_dir=submit_path,
            merge_group=mg,
            sandbox_url=sandbox_url,
            category_throttles=category_throttles,
            executables=executables,
            output_datasets=output_datasets,
            local_pfn_prefix=local_pfn_prefix,
            sandbox_path=sandbox_path,
            resource_params=resource_params,
            banned_sites=banned_sites,
            pileup_files=pileup_files,
            job_priority=job_priority,
            extra_classads=extra_classads,
            stageout_mode=stageout_mode,
            pileup_remote_read=pileup_remote_read,
            allowed_sites=allowed_sites,
            spool_mode=spool_mode,
            all_sites=all_sites,
        )

        if spool_mode:
            # Pre-generate .condor.sub for the sub-DAG so the remote
            # schedd doesn't need to run condor_submit_dag.
            import subprocess
            subprocess.run(
                ["condor_submit_dag", "-no_submit", "-force",
                 f"{mg_name}/group.dag"],
                cwd=str(submit_path),
                capture_output=True,
            )

    # Category throttling for merge groups
    outer_lines.append("")
    for mg in merge_groups:
        mg_name = f"mg_{mg.group_index:06d}"
        outer_lines.append(f"CATEGORY {mg_name} MergeGroup")
    outer_lines.append("MAXJOBS MergeGroup 10")

    dag_path = str(submit_path / "workflow.dag")
    _write_file(dag_path, "\n".join(outer_lines) + "\n")
    return dag_path


def _generate_group_dag(
    group_dir: Path,
    submit_dir: Path,
    merge_group: PlanningMergeGroup,
    sandbox_url: str,
    category_throttles: dict[str, int],
    executables: dict[str, str] | None = None,
    output_datasets: list[dict] | None = None,
    local_pfn_prefix: str = "",
    sandbox_path: str | None = None,
    resource_params: dict[str, int] | None = None,
    banned_sites: list[str] | None = None,
    pileup_files: dict[str, list[str]] | None = None,
    job_priority: int = 0,
    extra_classads: dict[str, str] | None = None,
    stageout_mode: str = "local",
    pileup_remote_read: bool = True,
    allowed_sites: list[str] | None = None,
    spool_mode: bool = False,
    all_sites: list[str] | None = None,
) -> None:
    """Generate a single merge group sub-DAG (group.dag) + submit files."""
    exe = executables or {}
    proc_exe = exe.get("processing", "run_payload.sh")
    merge_exe = exe.get("merge", "run_merge.sh")
    cleanup_exe = exe.get("cleanup", "run_cleanup.sh")

    # In spool mode (no DIR), inner DAGMan CWD = spool root, so:
    #   - per-group files use mg_name/ prefix
    #   - shared scripts/executables are in the root (no ../ prefix)
    # In local mode (DIR sets CWD to group_dir):
    #   - per-group files are referenced directly (landing.sub)
    #   - shared scripts/executables use ../ prefix
    mg_name = group_dir.name  # e.g. "mg_000000"
    if spool_mode:
        # File prefix for per-group files in the DAG
        fp = f"{mg_name}/"
        proc_exe = "wms2_proc.sh"
        merge_exe = "wms2_merge.py"
        cleanup_exe = "wms2_cleanup.py"
    else:
        fp = ""
        proc_exe = "../wms2_proc.sh"
        merge_exe = "../wms2_merge.py"
        cleanup_exe = "../wms2_cleanup.py"

    # Resource parameters
    rp = resource_params or {}
    memory_mb = rp.get("memory_mb", 0)
    disk_kb = rp.get("disk_kb", 0)
    ncpus = rp.get("ncpus", 0)

    # Build transfer_input_files list for processing nodes.
    # In local mode paths are relative to group_dir (../ prefix for parent).
    # In spool mode paths are relative to spool root (no ../ prefix).
    proc_transfer_files: list[str] = []
    if sandbox_path and os.path.isfile(sandbox_path):
        # Symlink sandbox into submit_dir so it's co-located with DAG files
        sandbox_link = submit_dir / os.path.basename(sandbox_path)
        if not sandbox_link.exists():
            os.symlink(os.path.abspath(sandbox_path), str(sandbox_link))
        if spool_mode:
            proc_transfer_files.append(sandbox_link.name)
        else:
            proc_transfer_files.append(f"../{sandbox_link.name}")
        # Extract manifest.json to group dir so merge job can find CMSSW info
        import tarfile
        try:
            with tarfile.open(sandbox_path, "r:gz") as tf:
                for member in tf.getmembers():
                    if member.name == "manifest.json" or member.name.endswith("/manifest.json"):
                        member.name = "manifest.json"
                        tf.extract(member, str(group_dir))
                        break
        except Exception:
            pass  # merge will fall back to copy mode without hadd

        # Override per-step multicore and n_parallel in manifest
        manifest_path = group_dir / "manifest.json"
        per_step_tuning = rp.get("_per_step_tuning")
        if manifest_path.is_file() and (ncpus or per_step_tuning):
            try:
                import json as _json
                with open(manifest_path) as _f:
                    _manifest = _json.load(_f)
                changed = False
                for i, step in enumerate(_manifest.get("steps", [])):
                    step_key = str(i)
                    st = per_step_tuning.get(step_key) if per_step_tuning else None
                    if st:
                        tuned_nt = st.get("tuned_nthreads")
                        n_par = st.get("n_parallel", 1)
                        if tuned_nt and tuned_nt != step.get("multicore"):
                            step["multicore"] = tuned_nt
                            changed = True
                        if n_par > 1 and step.get("n_parallel", 1) != n_par:
                            step["n_parallel"] = n_par
                            changed = True
                    elif ncpus and step.get("multicore", 0) != ncpus:
                        step["multicore"] = ncpus
                        changed = True
                if changed:
                    with open(manifest_path, "w") as _f:
                        _json.dump(_manifest, _f, indent=2)
            except Exception:
                pass

    # Job environment — X509 proxy is delegated by HTCondor via
    # use_x509userproxy (set in _write_submit_file), not passed via
    # environment. SITECONFIG_PATH is not set here: at remote sites the
    # glidein provides it; locally the worker scripts fall back to
    # /opt/cms/siteconf.
    proc_env: dict[str, str] = {}
    x509_cert_dir = os.environ.get("X509_CERT_DIR", "")
    if x509_cert_dir:
        proc_env["X509_CERT_DIR"] = x509_cert_dir
    # Pass GLIDEIN_CMSSite from the matched slot to the job environment.
    # This lets merge/cleanup jobs find the correct site-specific siteconf
    # at /cvmfs/cms.cern.ch/SITECONF/<site>/. The $$() syntax is evaluated
    # by HTCondor at match time.
    if stageout_mode == "grid":
        proc_env["GLIDEIN_CMSSite"] = "$$([GLIDEIN_CMSSite])"

    # Write output_info.json — shared by proc (stage-out) and merge (read input/write output)
    output_info_path = None
    if output_datasets and local_pfn_prefix:
        output_info = {
            "output_datasets": [
                {
                    "dataset_name": d.get("dataset_name", ""),
                    "merged_lfn_base": d.get("merged_lfn_base", ""),
                    "unmerged_lfn_base": d.get("unmerged_lfn_base", ""),
                    "data_tier": d.get("data_tier", ""),
                }
                for d in output_datasets
            ],
            "local_pfn_prefix": local_pfn_prefix,
            "stageout_mode": stageout_mode,
            "group_index": merge_group.group_index,
            "max_merge_size": rp.get("max_merge_size", 4 * 1024**3),
            "proc_node_indices": [n.node_index for n in merge_group.processing_nodes],
        }
        output_info_path = str(group_dir / "output_info.json")
        _write_file(output_info_path, json.dumps(output_info, indent=2))

    # Add output_info.json to proc transfer files so proc jobs can stage out
    if output_info_path and os.path.isfile(output_info_path):
        proc_transfer_files.append(f"{fp}output_info.json")

    # Add wms2_stageout.py to transfer files for grid mode
    stageout_utility_path = str(submit_dir / "wms2_stageout.py")
    if stageout_mode == "grid" and os.path.isfile(stageout_utility_path):
        if spool_mode:
            proc_transfer_files.append("wms2_stageout.py")
        else:
            proc_transfer_files.append("../wms2_stageout.py")

    # Add pileup_files.json to proc transfer files for PSet injection
    pileup_json_path = str(group_dir / "pileup_files.json")
    if pileup_files and os.path.isfile(pileup_json_path):
        proc_transfer_files.append(f"{fp}pileup_files.json")

    proc_nodes = merge_group.processing_nodes

    # Site pinning is always needed: even in grid stageout mode, merge/cleanup
    # jobs must run at the same site as proc jobs because they resolve the
    # storage endpoint from site-specific storage.json. Without pinning, a
    # merge job could land at a different site and use the wrong endpoint.
    skip_site_pinning = False

    # In spool mode, elected_site needs a per-group name to avoid clashes
    # between merge groups sharing the same CWD (spool root).
    elected_site = f"{mg_name}_elected_site" if spool_mode else "elected_site"

    lines: list[str] = [
        f"# Merge group {merge_group.group_index}",
        f"NODE_STATUS_FILE {fp}group.dag.status",
        "",
    ]

    # DESIRED_Sites tells GlideinWMS which sites this job can run at.
    # GlideinWMS START expression requires DESIRED_Sites to be defined;
    # if allowed_sites is set use those, otherwise list all CRIC sites.
    if allowed_sites:
        all_desired = ",".join(allowed_sites)
    elif all_sites:
        all_desired = ",".join(all_sites)
    else:
        all_desired = ""
    out_dir = mg_name if spool_mode else ""

    if not skip_site_pinning:
        # Landing node: /bin/true job with free site selection.
        # Its POST script (elect_site.sh) extracts MATCH_GLIDEIN_CMSSite
        # so remaining nodes can be pinned to the same site.
        # Request the same resources as proc jobs so that the landing only
        # matches at sites that can actually run the heavy processing.
        landing_priority = max(job_priority, 5) + 10
        landing_classads = dict(extra_classads or {})
        landing_classads["WMS2_QuickJob"] = "True"
        _write_submit_file(
            str(group_dir / "landing.sub"),
            executable="/bin/true",
            arguments="",
            description="landing node",
            desired_sites=all_desired,
            banned_sites=banned_sites,
            allowed_sites=allowed_sites,
            memory_mb=memory_mb,
            ncpus=ncpus,
            disk_kb=disk_kb,
            priority=landing_priority,
            extra_classads=landing_classads,
            output_dir=out_dir,
            transfer_executable=False,
        )
        lines.append(f"JOB landing {fp}landing.sub")
        landing_log = f"{fp}landing.log"
        lines.append(
            f"SCRIPT POST landing elect_site.sh {elected_site} {landing_log}"
        )
        lines.append("")

    # Processing nodes
    sandbox_ref = os.path.basename(sandbox_path) if sandbox_path else sandbox_url
    post_args = f"$JOB $RETURN $RETRY $MAX_RETRIES {mg_name}" if spool_mode else "$JOB $RETURN $RETRY $MAX_RETRIES"
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        input_lfns = ",".join(f.lfn for f in node.input_files)

        # Build arguments with event range info
        proc_args = f"--sandbox {sandbox_ref} --input {input_lfns}"
        proc_args += f" --node-index {node.node_index}"
        if output_info_path:
            proc_args += f" --output-info {os.path.basename(output_info_path)}"
        if node.first_event > 0:
            proc_args += f" --first-event {node.first_event}"
        if node.last_event > 0:
            proc_args += f" --last-event {node.last_event}"
        if node.events_per_job > 0:
            proc_args += f" --events-per-job {node.events_per_job}"
        if ncpus > 0:
            proc_args += f" --ncpus {ncpus}"
        if rp.get("test_fraction"):
            proc_args += f" --test-fraction {rp['test_fraction']}"
        # filter_efficiency is handled at planning time (inflating total_events),
        # not at execution time. No --filter-eff arg needed.
        if pileup_remote_read:
            proc_args += " --pileup-remote-read"

        _write_submit_file(
            str(group_dir / f"{node_name}.sub"),
            executable=proc_exe,
            arguments=proc_args,
            description=f"processing node {node.node_index}",
            desired_sites=all_desired if skip_site_pinning else (node.primary_location or ""),
            memory_mb=memory_mb,
            disk_kb=disk_kb,
            ncpus=ncpus,
            transfer_input_files=proc_transfer_files or None,
            environment=proc_env or None,
            priority=job_priority,
            extra_classads=extra_classads,
            output_dir=out_dir,
        )
        lines.append(f"JOB {node_name} {fp}{node_name}.sub")
        if not skip_site_pinning:
            lines.append(
                f"SCRIPT PRE {node_name} pin_site.sh {fp}{node_name}.sub {elected_site}"
            )
        lines.append(
            f"SCRIPT POST {node_name} post_script.sh {post_args}"
        )
        lines.append("")

    # Merge node
    merge_args = f"--sandbox {sandbox_ref}"
    merge_transfer: list[str] = []
    if output_info_path:
        merge_args += f" --output-info {os.path.basename(output_info_path)}"
        merge_transfer.append(f"{fp}output_info.json")
    manifest_path = str(group_dir / "manifest.json")
    if os.path.isfile(manifest_path):
        merge_transfer.append(f"{fp}manifest.json")
    if stageout_mode == "grid" and os.path.isfile(stageout_utility_path):
        if spool_mode:
            merge_transfer.append("wms2_stageout.py")
        else:
            merge_transfer.append("../wms2_stageout.py")
    _write_submit_file(
        str(group_dir / "merge.sub"),
        executable=merge_exe,
        arguments=merge_args,
        description="merge node",
        desired_sites=all_desired if skip_site_pinning else "",
        memory_mb=memory_mb,
        disk_kb=disk_kb,
        transfer_input_files=merge_transfer or None,
        environment=proc_env or None,
        priority=job_priority,
        extra_classads=extra_classads,
        output_dir=out_dir,
    )
    lines.append(f"JOB merge {fp}merge.sub")
    if not skip_site_pinning:
        lines.append(
            f"SCRIPT PRE merge pin_site.sh {fp}merge.sub {elected_site}"
        )
    lines.append(
        f"SCRIPT POST merge post_script.sh {post_args}"
    )
    lines.append("")

    # Cleanup node
    cleanup_args = ""
    cleanup_transfer: list[str] = []
    if output_info_path:
        cleanup_args = f"--output-info {os.path.basename(output_info_path)}"
        cleanup_transfer.append(f"{fp}output_info.json")
    # cleanup_manifest.json is written by the merge job. In spool mode,
    # HTCondor transfers merge output back to the spool root (not mg_xxx/).
    # In local mode, it's in the group directory.
    if spool_mode:
        cleanup_transfer.append("cleanup_manifest.json")
    else:
        cleanup_transfer.append(f"{fp}cleanup_manifest.json")
    if stageout_mode == "grid" and os.path.isfile(stageout_utility_path):
        if spool_mode:
            cleanup_transfer.append("wms2_stageout.py")
        else:
            cleanup_transfer.append("../wms2_stageout.py")
    _write_submit_file(
        str(group_dir / "cleanup.sub"),
        executable=cleanup_exe,
        arguments=cleanup_args,
        description="cleanup node",
        desired_sites=all_desired if skip_site_pinning else "",
        transfer_input_files=cleanup_transfer or None,
        environment=proc_env or None,
        priority=job_priority,
        extra_classads=extra_classads,
        output_dir=out_dir,
    )
    lines.append(f"JOB cleanup {fp}cleanup.sub")
    if not skip_site_pinning:
        lines.append(
            f"SCRIPT PRE cleanup pin_site.sh {fp}cleanup.sub {elected_site}"
        )
    lines.append("")

    # Retries
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        lines.append(f"RETRY {node_name} 5 UNLESS-EXIT 42")
    lines.append("RETRY merge 2 UNLESS-EXIT 42")
    lines.append("RETRY cleanup 1")
    lines.append("")

    # Abort-DAG-on (catastrophic failure circuit breaker)
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        lines.append(f"ABORT-DAG-ON {node_name} 43 RETURN 1")
    lines.append("ABORT-DAG-ON merge 43 RETURN 1")
    lines.append("")

    # Dependencies
    proc_names = " ".join(f"proc_{n.node_index:06d}" for n in proc_nodes)
    if not skip_site_pinning:
        lines.append(f"PARENT landing CHILD {proc_names}")
    lines.append(f"PARENT {proc_names} CHILD merge")
    lines.append("PARENT merge CHILD cleanup")
    lines.append("")

    # Categories
    for node in proc_nodes:
        node_name = f"proc_{node.node_index:06d}"
        lines.append(f"CATEGORY {node_name} Processing")
    lines.append("CATEGORY merge Merge")
    lines.append("CATEGORY cleanup Cleanup")
    for cat, limit in category_throttles.items():
        lines.append(f"MAXJOBS {cat} {limit}")

    _write_file(str(group_dir / "group.dag"), "\n".join(lines) + "\n")


# ── Submit File + Script Generation ────────────────────────────


def _write_submit_file(
    path: str,
    executable: str,
    arguments: str,
    description: str,
    desired_sites: str = "",
    memory_mb: int = 0,
    disk_kb: int = 0,
    ncpus: int = 0,
    transfer_input_files: list[str] | None = None,
    environment: dict[str, str] | None = None,
    banned_sites: list[str] | None = None,
    allowed_sites: list[str] | None = None,
    priority: int = 0,
    extra_classads: dict[str, str] | None = None,
    output_dir: str = "",
    transfer_executable: bool = True,
) -> None:
    stem = Path(path).stem
    out_prefix = f"{output_dir}/" if output_dir else ""
    lines = [
        f"# {description}",
        "universe = vanilla",
        f"executable = {executable}",
        f"arguments = {arguments}",
        f"output = {out_prefix}{stem}.out",
        f"error = {out_prefix}{stem}.err",
        f"log = {out_prefix}{stem}.log",
    ]
    if ncpus > 0:
        lines.append(f"request_cpus = {ncpus}")
    if memory_mb > 0:
        lines.append(f"request_memory = {memory_mb}")
    if disk_kb > 0:
        lines.append(f"request_disk = {disk_kb}")
    if environment:
        env_str = " ".join(f"{k}={v}" for k, v in environment.items())
        lines.append(f'environment = "{env_str}"')
    if not transfer_executable:
        lines.append("transfer_executable = false")
    lines.append("should_transfer_files = YES")
    lines.append("when_to_transfer_output = ON_EXIT")
    # X509 proxy delegation (following CRABServer approach):
    # use_x509userproxy tells HTCondor to delegate the proxy to each job
    # and set X509_USER_PROXY in the job environment. The proxy file is
    # symlinked into submit_dir at planning time (see _generate_dag_files).
    # In local mode (inner DAG with DIR), proxy is at ../x509_proxy.
    # In spool mode (no DIR), proxy is at x509_proxy (spool root).
    x509_proxy = os.environ.get("X509_USER_PROXY", "")
    if x509_proxy and os.path.isfile(x509_proxy):
        lines.append("use_x509userproxy = true")
        proxy_ref = "x509_proxy" if output_dir else "../x509_proxy"
        lines.append(f"x509userproxy = {proxy_ref}")
    if transfer_input_files:
        lines.append(f"transfer_input_files = {','.join(transfer_input_files)}")
    if desired_sites:
        lines.append(f'+DESIRED_Sites = "{desired_sites}"')
    # CMS GlideinWMS classads — required for glidein provisioning and matching.
    # MaxWallTimeMins: glidein START checks remaining lifetime against this.
    # Without it, the default (16h) rejects many short-lived glideins.
    lines.append("+MaxWallTimeMins = 1440")
    lines.append('+CMS_Type = "production"')
    lines.append('+CMS_JobType = "Processing"')
    # CMS_MATCH_MICROARCH: glidein START expression evaluates the slot's own
    # CMS_MATCH_MICROARCH which compares int(REQUIRED_MINIMUM_MICROARCH) against
    # the slot's Microarch level. Must be an integer (2 = x86_64-v2, 3 = v3).
    lines.append("+CMS_MATCH_MICROARCH = true")
    lines.append('+CMS_ALLOW_OVERFLOW = "True"')
    lines.append("+REQUIRED_MINIMUM_MICROARCH = 2")
    # CMS accounting group: the global pool has three groups:
    # highprio, production, analysis. Use "production" to match Tier-0 jobs.
    lines.append("accounting_group = production")
    lines.append("accounting_group_user = dmytro")
    req_parts = []
    if allowed_sites:
        pos = " || ".join(
            f'(TARGET.GLIDEIN_CMSSite == "{s}")' for s in allowed_sites
        )
        req_parts.append(f"({pos})")
    if banned_sites:
        for s in banned_sites:
            req_parts.append(f'(TARGET.GLIDEIN_CMSSite =!= "{s}")')
    if req_parts:
        lines.append(f"Requirements = {' && '.join(req_parts)}")
    if priority:
        lines.append(f"priority = {priority}")
    if extra_classads:
        for key, value in extra_classads.items():
            lines.append(f"+{key} = {value}")
    lines.append("queue 1")
    _write_file(path, "\n".join(lines) + "\n")


def _generate_pilot_submit(
    submit_dir: str,
    sandbox_path: str,
    input_files: list[str],
    pilot_config: dict[str, Any] | None = None,
) -> str:
    pc = pilot_config or {}
    input_str = ",".join(input_files)
    sandbox_basename = os.path.basename(sandbox_path) if sandbox_path else "sandbox.tar.gz"
    args = (
        f"--sandbox {sandbox_basename} --input {input_str}"
        f" --initial-events {pc.get('initial_events', 200)}"
        f" --timeout {pc.get('timeout', 900)}"
    )
    transfer_inputs = ["wms2_pilot.py"]
    if sandbox_path and os.path.isfile(sandbox_path):
        transfer_inputs.append(sandbox_path)
    lines = [
        "# WMS2 pilot job",
        "universe = vanilla",
        "executable = wms2_pilot.py",
        f"arguments = {args}",
        "output = pilot.out",
        "error = pilot.err",
        "log = pilot.log",
    ]
    ncpus = pc.get("ncpus", 0)
    if ncpus > 0:
        lines.append(f"request_cpus = {ncpus}")
    memory_mb = pc.get("memory_mb", 0)
    if memory_mb > 0:
        lines.append(f"request_memory = {memory_mb}")
    x509_proxy = os.environ.get("X509_USER_PROXY", "")
    lines.extend([
        "should_transfer_files = YES",
        "when_to_transfer_output = ON_EXIT",
    ])
    # Proxy delegation: the condor adapter sets x509userproxy on the
    # Submit object at submit time (from the local X509_USER_PROXY path).
    if x509_proxy and os.path.isfile(x509_proxy):
        lines.append("use_x509userproxy = true")
    lines.extend([
        f"transfer_input_files = {','.join(transfer_inputs)}",
        "transfer_output_files = pilot_metrics.json",
        "queue 1",
    ])
    return "\n".join(lines) + "\n"


def _write_elect_site_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# elect_site.sh — POST script for landing node
# Extracts GLIDEIN_CMSSite from the completed landing job.
# Falls back to "local" for dev environments without glidein infrastructure.
ELECTED_SITE_FILE=$1
LOG_FILE=$2
# Extract the cluster ID from the landing node's HTCondor log.
# The log contains "Job submitted from host" with (cluster.proc.subproc) pattern.
CLUSTER_ID=$(grep -o '([0-9]*\\.' "$LOG_FILE" 2>/dev/null | tail -1 | tr -d '(.')
if [ -n "$CLUSTER_ID" ]; then
    SITE=$(condor_history "${CLUSTER_ID}.0" -limit 1 -af MATCH_GLIDEIN_CMSSite 2>/dev/null)
fi
if [ -n "$SITE" ] && [ "$SITE" != "undefined" ]; then
    echo "$SITE" > "$ELECTED_SITE_FILE"
else
    echo "local" > "$ELECTED_SITE_FILE"
fi
exit 0
""")
    os.chmod(path, 0o755)


def _write_pin_site_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# pin_site.sh — PRE script for processing/merge/cleanup nodes
# Pins the job to the elected site by setting Requirements and DESIRED_Sites.
# Skips pinning when site is "local" (dev environments without glidein).
SUBMIT_FILE=$1
SITE=$(cat "$2")
if [ "$SITE" = "local" ]; then
    exit 0
fi
if grep -q '+DESIRED_Sites' "$SUBMIT_FILE"; then
    sed -i "s/+DESIRED_Sites = .*/+DESIRED_Sites = \\"${SITE}\\"/" "$SUBMIT_FILE"
else
    sed -i "/^queue/i +DESIRED_Sites = \\"${SITE}\\"" "$SUBMIT_FILE"
fi
if grep -q '^Requirements' "$SUBMIT_FILE"; then
    sed -i "s/^Requirements = .*/Requirements = (TARGET.GLIDEIN_CMSSite == \\"${SITE}\\")/" "$SUBMIT_FILE"
else
    sed -i "/^queue/i Requirements = (TARGET.GLIDEIN_CMSSite == \\"${SITE}\\")" "$SUBMIT_FILE"
fi
exit 0
""")
    os.chmod(path, 0o755)


def _write_post_script(path: str) -> None:
    _write_file(path, """\
#!/bin/bash
# post_script.sh — POST script for processing/merge nodes
# Called by DAGMan: SCRIPT POST <node> post_script.sh $JOB $RETURN $RETRY $MAX_RETRIES [group_dir]
# group_dir: optional prefix for file paths (spool mode without DIR).
NODE_NAME=$1; EXIT_CODE=$2; RETRY_NUM=${3:-0}; MAX_RETRIES=${4:-3}; GROUP_DIR=${5:-.}
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

UNLESS_EXIT=42
ABORT_EXIT=43

# Run collector — writes post.json, prints classification category to stdout
CLASSIFICATION=$(python3 "$SCRIPT_DIR/wms2_post_collect.py" \
    "$NODE_NAME" "$EXIT_CODE" "$RETRY_NUM" "$MAX_RETRIES" "$GROUP_DIR" 2>/dev/null || echo "transient")

if [ "$EXIT_CODE" -eq 0 ]; then
    exit 0
fi

# Memory adjustment: if OOM, bump request_memory by 50%
if [ "$CLASSIFICATION" = "infrastructure_memory" ]; then
    SUB_FILE="${GROUP_DIR}/${NODE_NAME}.sub"
    if [ -f "$SUB_FILE" ]; then
        current_mem=$(grep -oP 'request_memory\\s*=\\s*\\K\\d+' "$SUB_FILE")
        if [ -n "$current_mem" ]; then
            new_mem=$((current_mem * 3 / 2))
            sed -i "s/request_memory = .*/request_memory = ${new_mem}/" "$SUB_FILE"
        fi
    fi
fi

# --- Early abort on repeated identical failures ---
# If N different proc nodes failed with the same exit code, this is systemic.
# Abort the sub-DAG instead of burning through all retries on remaining nodes.
# Threshold scales to group size: min(proc_count, 3) so a 2-node group
# aborts after 2 matching failures instead of never reaching 3.
PROC_COUNT=$(ls "${GROUP_DIR}"/proc_*.sub 2>/dev/null | wc -l)
IDENTICAL_THRESHOLD=$((PROC_COUNT < 3 ? PROC_COUNT : 3))
CURRENT_EXIT="$EXIT_CODE"
match_count=0
for pj in "${GROUP_DIR}"/proc_*.post.json; do
    [ -f "$pj" ] || continue
    pj_exit=$(python3 -c "import json,sys; d=json.load(open(sys.argv[1])); print(d.get('job',{}).get('exit_code',''))" "$pj" 2>/dev/null)
    if [ "$pj_exit" = "$CURRENT_EXIT" ]; then
        match_count=$((match_count + 1))
    fi
done
if [ "$match_count" -ge "$IDENTICAL_THRESHOLD" ]; then
    echo "ABORT: $match_count nodes failed with exit code $CURRENT_EXIT — systemic failure" >&2
    exit $ABORT_EXIT
fi

case "$CLASSIFICATION" in
    transient)
        SLEEP_TIME=$((60 * (2 ** RETRY_NUM)))
        [ "$SLEEP_TIME" -gt 600 ] && SLEEP_TIME=600
        sleep $SLEEP_TIME
        exit 1 ;;
    permanent|data)
        exit $UNLESS_EXIT ;;
    infrastructure)
        # Site-pinned sub-DAG: retrying on same broken site is pointless.
        # Exit with UNLESS_EXIT (42) so DAGMan skips retries — Level 2
        # recovery (rescue DAG) will pick a different site.
        exit $UNLESS_EXIT ;;
    infrastructure_memory)
        # Memory bump already applied to .sub file above — retry on same
        # site with more memory is valid.
        sleep 60
        exit 1 ;;
    catastrophic)
        exit $ABORT_EXIT ;;
    *)
        exit 1 ;;
esac
""")
    os.chmod(path, 0o755)


def _write_post_collector(path: str) -> None:
    from wms2.core.post_classifier import generate_collector_script
    _write_file(path, generate_collector_script())
    os.chmod(path, 0o755)


def _write_proc_script(path: str) -> None:
    """Generate a processing wrapper that supports CMSSW, synthetic, and pilot modes.

    Extracts sandbox, reads manifest.json, and dispatches to the appropriate mode.
    CMSSW mode handles per-step CMSSW/ScramArch with apptainer container support
    for cross-OS execution (e.g. el8 CMSSW on el9 host).
    """
    # Embed the venv Python path for simulator mode (needs numpy/uproot).
    # At planning time sys.executable is the venv Python; at execution time
    # the HTCondor job may only have system Python (no numpy/uproot).
    _venv_python = sys.executable
    _script = r'''#!/bin/bash
# wms2_proc.sh — WMS2 processing job wrapper
# Supports CMSSW mode (per-step cmsRun with apptainer), synthetic mode
# (sized output), and pilot mode (iterative measurement).
set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# ── Argument parsing ──────────────────────────────────────────
SANDBOX=""
INPUT_LFNS=""
FIRST_EVENT=0
LAST_EVENT=0
EVENTS_PER_JOB=0
NODE_INDEX=0
PILOT_MODE=false
OUTPUT_INFO=""
OVERRIDE_NCPUS=0
TEST_FRACTION=0
FILTER_EFF=1.0  # kept for backward compat; no longer used in PSet injection
PILEUP_REMOTE_READ=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --sandbox)    SANDBOX="$2";        shift 2 ;;
        --input)      INPUT_LFNS="$2";     shift 2 ;;
        --first-event) FIRST_EVENT="$2";   shift 2 ;;
        --last-event)  LAST_EVENT="$2";    shift 2 ;;
        --events-per-job) EVENTS_PER_JOB="$2"; shift 2 ;;
        --node-index) NODE_INDEX="$2";     shift 2 ;;
        --output-info) OUTPUT_INFO="$2";   shift 2 ;;
        --pilot)      PILOT_MODE=true;     shift   ;;
        --ncpus)      OVERRIDE_NCPUS="$2"; shift 2 ;;
        --test-fraction) TEST_FRACTION="$2"; shift 2 ;;
        --filter-eff)  FILTER_EFF="$2";    shift 2 ;;
        --pileup-remote-read) PILEUP_REMOTE_READ=true; shift ;;
        *)            echo "Unknown arg: $1" >&2; shift ;;
    esac
done

# ── Test fraction: reduce events per job for fast testing ─────
if [[ "$TEST_FRACTION" != "0" && "$EVENTS_PER_JOB" -gt 0 ]]; then
    ORIG_EVENTS=$EVENTS_PER_JOB
    EVENTS_PER_JOB=$(python3 -c "print(max(1, int($EVENTS_PER_JOB * $TEST_FRACTION)))")
    echo "TEST MODE: events_per_job reduced from $ORIG_EVENTS to $EVENTS_PER_JOB (fraction=$TEST_FRACTION)"
fi

START_TIME=$(date +%s)
echo "=== WMS2 Processing Wrapper ==="
echo "timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "host:      $(hostname)"
echo "pid:       $$"
echo "sandbox:   $SANDBOX"
echo "node:      $NODE_INDEX"
echo "pilot:     $PILOT_MODE"

# ── Sandbox extraction ────────────────────────────────────────
WORK_DIR=$(pwd)
if [[ -n "$SANDBOX" && -f "$SANDBOX" ]]; then
    echo "Extracting sandbox: $SANDBOX"
    tar xzf "$SANDBOX"
elif [[ -n "$SANDBOX" && ! -f "$SANDBOX" ]]; then
    echo "WARNING: Sandbox not found: $SANDBOX — running without sandbox"
fi

# Adaptive tuning: apply per-step nThreads from replan
if [[ -f manifest_tuned.json ]]; then
    echo "Applying adaptive manifest (per-step nThreads tuning)"
    cp manifest_tuned.json manifest.json
fi

# Override per-step multicore if --ncpus was passed
# Skip when adaptive tuning is active (manifest_tuned.json has per-step values)
if [[ "$OVERRIDE_NCPUS" -gt 0 && -f manifest.json && ! -f manifest_tuned.json ]]; then
    echo "Overriding manifest multicore to $OVERRIDE_NCPUS"
    python3 -c "
import json
m = json.load(open('manifest.json'))
for s in m.get('steps', []):
    s['multicore'] = $OVERRIDE_NCPUS
json.dump(m, open('manifest.json', 'w'), indent=2)
"
fi

# ── Read manifest ─────────────────────────────────────────────
MODE="synthetic"
if [[ -f manifest.json ]]; then
    MODE=$(python3 -c "import json; print(json.load(open('manifest.json')).get('mode','synthetic'))")
    echo "manifest mode: $MODE"
else
    echo "No manifest.json found — defaulting to synthetic mode"
fi

# ── Helper: convert LFN to xrootd URL ────────────────────────
lfn_to_xrootd() {
    local lfn="$1"
    if [[ "$lfn" == root://* ]]; then
        echo "$lfn"
    elif [[ "$lfn" == synthetic://* ]]; then
        echo ""
    else
        echo "root://cms-xrd-global.cern.ch/$lfn"
    fi
}

# ── cmssw-env delegation ──────────────────────────────────────
# CMS production wrapper: handles container resolution, bind mounts,
# OS normalization, locale, architecture detection, and runtime selection.
# See https://github.com/cms-sw/cmssw/blob/master/Utilities/ReleaseScripts/scripts/cmssw-env
CMSSW_ENV="/cvmfs/cms.cern.ch/common/cmssw-env"

cmssw_env_exec() {
    # Usage: cmssw_env_exec <scram_arch> [extra apptainer flags...] -- <command> [args...]
    local scram_arch="$1"
    shift
    local arch_os="${scram_arch%%_*}"   # "el8" from "el8_amd64_gcc10"

    # If already inside a container (glidein-provided), skip cmssw-env to avoid
    # nested singularity/apptainer which fails on many grid sites.
    # Detection: APPTAINER_CONTAINER or SINGULARITY_CONTAINER env vars, or /.singularity.d
    if [[ -n "${APPTAINER_CONTAINER:-}" || -n "${SINGULARITY_CONTAINER:-}" || -d "/.singularity.d" ]]; then
        echo "  [cmssw_env_exec] Already inside container, running directly"
        export SCRAM_ARCH="$scram_arch"
        # cmsset_default.sh expects VO_CMS_SW_DIR, CMS_PATH, SITECONFIG_PATH;
        # set if missing to avoid "unbound variable" errors under set -u
        # in glidein containers.
        export VO_CMS_SW_DIR="${VO_CMS_SW_DIR:-/cvmfs/cms.cern.ch}"
        export CMS_PATH="${CMS_PATH:-/cvmfs/cms.cern.ch}"
        export SITECONFIG_PATH="${SITECONFIG_PATH:-/cvmfs/cms.cern.ch/SITECONF/local}"
        # Temporarily disable nounset — cmsset_default.sh references many
        # potentially-unset vars (CVSROOT, MANPATH, etc.) that we can't predict.
        set +u
        source /cvmfs/cms.cern.ch/cmsset_default.sh
        set -u
        "$@"
    elif [[ -x "$CMSSW_ENV" ]]; then
        "$CMSSW_ENV" --cmsos "$arch_os" -- "$@"
    else
        echo "ERROR: cmssw-env not found at $CMSSW_ENV (is CVMFS mounted?)" >&2
        return 1
    fi
}

# ── CMSSW step execution via cmssw-env ─────────────────────────
run_step() {
    local cmssw="$1" arch="$2" cmsrun_args="$3" cmsrun_cwd="${4:-}"

    # Resolve siteconfig path: default to /opt/cms/siteconf if SITECONFIG_PATH unset
    local SITE_CFG="${SITECONFIG_PATH:-}"
    if [[ -z "$SITE_CFG" || ! -d "$SITE_CFG/JobConfig" ]]; then
        for _try in /opt/cms/siteconf /cvmfs/cms.cern.ch/SITECONF/local; do
            if [[ -f "$_try/JobConfig/site-local-config.xml" ]]; then
                SITE_CFG="$_try"
                break
            fi
        done
    fi
    echo "siteconf: ${SITE_CFG:-NOT FOUND}"

    # Copy siteconf files into the execute directory so they're available
    # inside the container via the working-directory bind mount.
    # Layout for SITECONFIG_PATH (CMSSW >=14.x): _siteconf/JobConfig/site-local-config.xml
    mkdir -p "$WORK_DIR/_siteconf/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
    # Copy PhEDEx — check current dir and parent (for subsite siteconfs)
    if [[ -d "$SITE_CFG/PhEDEx" ]]; then
        cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
    elif [[ -d "$SITE_CFG/../PhEDEx" ]]; then
        cp -r "$SITE_CFG/../PhEDEx" "$WORK_DIR/_siteconf/"
    fi
    # Copy storage.json — check current dir and parent (for subsite siteconfs)
    if [[ -f "$SITE_CFG/storage.json" ]]; then
        cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
    elif [[ -f "$SITE_CFG/../storage.json" ]]; then
        cp "$SITE_CFG/../storage.json" "$WORK_DIR/_siteconf/"
    fi
    # Also place at $WORK_DIR/ — CMSSW subsites look at SITECONFIG_PATH/../storage.json
    [[ -f "$WORK_DIR/_siteconf/storage.json" ]] && cp "$WORK_DIR/_siteconf/storage.json" "$WORK_DIR/"
    # Layout for CMS_PATH (CMSSW <14.x): _siteconf/SITECONF/local/JobConfig/site-local-config.xml
    mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
    if [[ -d "$SITE_CFG/PhEDEx" ]]; then
        cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"
    elif [[ -d "$SITE_CFG/../PhEDEx" ]]; then
        cp -r "$SITE_CFG/../PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"
    fi
    # Rewrite catalog URLs in copied siteconf to use absolute paths within _siteconf.
    # TrivialFileCatalog resolves relative to CWD, not SITECONFIG_PATH, so we must
    # make the PhEDEx/storage.xml reference absolute to where we copied it.
    for slc in "$WORK_DIR/_siteconf/JobConfig/site-local-config.xml" \
               "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/site-local-config.xml"; do
        [[ -f "$slc" ]] && sed -i "s|trivialcatalog_file:[^?]*storage.xml|trivialcatalog_file:$WORK_DIR/_siteconf/PhEDEx/storage.xml|g" "$slc"
    done

    # Build step runner with optional tmpfs CWD.
    # IMPORTANT: Do NOT cd to /dev/shm before launching cmssw-env (apptainer).
    # Apptainer bind-mounts the CWD into the container; when CWD is under /dev/shm,
    # the /dev/shm bind conflicts with the container's /dev mount, breaking /dev/null.
    # Instead, launch from WORK_DIR and cd to tmpfs INSIDE the container after
    # cmsset_default.sh has been sourced.
    local _cwd_line=""
    local _copyback_line=""
    if [[ -n "$cmsrun_cwd" ]]; then
        _cwd_line="cd $cmsrun_cwd"
        _copyback_line="cp *.root $WORK_DIR/ 2>/dev/null || true"
    fi

    cat > _step_runner.sh <<STEPEOF
#!/bin/bash
set -e
# CMSSW >=14.x reads SITECONFIG_PATH; <14.x reads CMS_PATH + /SITECONF/local/
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf

export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ ! -d $cmssw/src ]]; then
    scramv1 project CMSSW $cmssw
fi
cd $cmssw/src && eval \$(scramv1 runtime -sh) && cd $WORK_DIR
$_cwd_line
cmsRun $cmsrun_args
$_copyback_line
STEPEOF
    chmod +x _step_runner.sh

    cmssw_env_exec "$arch" bash "$(pwd)/_step_runner.sh"
}

# ── Stage-out to unmerged site storage ─────────────────────────
stage_out_to_unmerged() {
    if [[ -z "$OUTPUT_INFO" || ! -f "$OUTPUT_INFO" ]]; then
        echo "No output_info.json — skipping stage-out"
        return 0
    fi

    echo ""
    echo "--- Staging out to unmerged site storage ---"
    python3 -c "
import json, os, shutil, glob

with open('$OUTPUT_INFO') as f:
    info = json.load(f)

pfn_prefix = info.get('local_pfn_prefix', '/mnt/shared')
stageout_mode = info.get('stageout_mode', 'local')
group_index = info['group_index']
datasets = info.get('output_datasets', [])

# Grid mode: import stageout utility and resolve write protocol
grid_cmd = None
siteconfig = None
if stageout_mode == 'grid':
    import wms2_stageout
    siteconfig = os.environ.get('SITECONFIG_PATH', '/opt/cms/siteconf')
    # Use _siteconf copy if available (inside container working dir)
    if os.path.isdir('_siteconf') and os.path.isfile('_siteconf/storage.json'):
        siteconfig = os.path.abspath('_siteconf')
    print(f'Grid stageout via siteconfig: {siteconfig}')

def stage_file(local_path, lfn):
    \"\"\"Stage a local file to its LFN destination.\"\"\"
    if stageout_mode == 'grid':
        pfn, cmd = wms2_stageout.resolve_write_pfn(lfn, siteconfig)
        pfn_dir = os.path.dirname(pfn)
        wms2_stageout.mkdir_p(pfn_dir, cmd)
        wms2_stageout.upload_file(local_path, pfn, cmd)
    else:
        pfn_dir = os.path.join(pfn_prefix, os.path.dirname(lfn).lstrip('/'))
        os.makedirs(pfn_dir, exist_ok=True)
        dest = os.path.join(pfn_dir, os.path.basename(lfn))
        shutil.copy2(local_path, dest)
        print(f'Staged: {local_path} -> {dest}')

# Read output manifest to get file -> tier mapping
manifest_file = 'proc_${NODE_INDEX}_outputs.json'
if not os.path.isfile(manifest_file):
    # Synthetic mode: look for proc_*_output.root files
    synth_files = glob.glob('proc_${NODE_INDEX}_*.root')
    if not synth_files:
        print('No output manifest or synthetic files, skipping stage-out')
        exit(0)
    # Build a simple manifest for synthetic files
    file_map = {}
    for sf in synth_files:
        # Use first dataset's tier as default
        tier = datasets[0].get('data_tier', 'unknown') if datasets else 'unknown'
        file_map[sf] = {'tier': tier, 'step_index': -1}
else:
    with open(manifest_file) as f:
        file_map = json.load(f)

# Build tier -> unmerged LFN base lookup
tier_to_unmerged = {}
for ds in datasets:
    tier_to_unmerged[ds['data_tier']] = ds.get('unmerged_lfn_base', '')

for filename, finfo in file_map.items():
    if not os.path.isfile(filename):
        print(f'WARNING: {filename} not found, skipping')
        continue
    tier = finfo['tier'] if isinstance(finfo, dict) else finfo
    unmerged_base = tier_to_unmerged.get(tier, '')
    if not unmerged_base:
        # Try EDM variant + strip numeric suffix:
        # NANOEDMAODSIM1 -> NANOAODSIM matches NANOAODSIM
        import re as _re
        tier_norm = _re.sub(r'(EDM)?(\\d+)$', '', tier.upper())
        for ds_tier, base in tier_to_unmerged.items():
            ds_norm = _re.sub(r'\\d+$', '', ds_tier.upper())
            if tier_norm == ds_norm:
                unmerged_base = base
                break
    if not unmerged_base:
        # Fallback: longer tier must contain shorter, with small length diff
        for ds_tier, base in tier_to_unmerged.items():
            t_up, d_up = tier.upper(), ds_tier.upper()
            longer, shorter = (t_up, d_up) if len(t_up) >= len(d_up) else (d_up, t_up)
            if shorter in longer and len(longer) - len(shorter) <= 3:
                unmerged_base = base
                break
    if not unmerged_base:
        print(f'WARNING: No unmerged LFN for tier {tier}, skipping {filename}')
        continue

    lfn = f'{unmerged_base}/{group_index:06d}/{filename}'
    stage_file(filename, lfn)

# Also stage the output manifest and metrics file
if os.path.isfile(manifest_file):
    for ds in datasets:
        unmerged_base = ds.get('unmerged_lfn_base', '')
        if unmerged_base:
            lfn_base = f'{unmerged_base}/{group_index:06d}'
            stage_file(manifest_file, f'{lfn_base}/proc_${NODE_INDEX}_outputs.json')
            # Stage metrics file alongside manifest
            metrics_file = 'proc_${NODE_INDEX}_metrics.json'
            if os.path.isfile(metrics_file):
                stage_file(metrics_file, f'{lfn_base}/{metrics_file}')
            # Stage cgroup metrics file
            cgroup_file = 'proc_${NODE_INDEX}_cgroup.json'
            if os.path.isfile(cgroup_file):
                stage_file(cgroup_file, f'{lfn_base}/{cgroup_file}')
            break
"
}

# ── Parse FJR output file path ────────────────────────────────
parse_fjr_output() {
    local fjr_path="$1"
    python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('$fjr_path')
    candidates = []
    for f in tree.findall('.//File'):
        pfn = f.findtext('PFN', '')
        if pfn.startswith('file:'): pfn = pfn[5:]
        if not pfn: continue
        label = f.findtext('ModuleLabel', '')
        candidates.append((pfn, label))
    for pfn, label in candidates:
        if 'LHE' not in label.upper():
            print(pfn); break
    else:
        if candidates: print(candidates[0][0])
except Exception:
    pass
" 2>/dev/null
}

# ── PSet injection for parallel step 0 instances ──────────────
inject_pset_parallel() {
    local pset="$1" first_event="$2" max_events="$3" nthreads="$4"
    # max_events is already in terms of generated events (no filter_eff adjustment needed)
    python3 -c "
pset_path = '$pset'
lines = ['', '# --- WMS2 parallel instance PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')
lines.append('process.maxEvents.input = cms.untracked.int32($max_events)')
lines.append('process.source.firstEvent = cms.untracked.uint32($first_event)')
lines.append('if hasattr(process, \"externalLHEProducer\"):')
lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32($max_events)')
# Randomize seeds for parallel GEN instances (same as main path)
lines.append('if hasattr(process, \"RandomNumberGeneratorService\"):')
lines.append('    from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper')
lines.append('    _rng_helper = RandomNumberServiceHelper(process.RandomNumberGeneratorService)')
lines.append('    _rng_helper.populate()')
if $nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32($nthreads)')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')
with open(pset_path, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"
}

# ── Parallel step 0 execution ─────────────────────────────────
run_step0_parallel() {
    local n_par="$1" nthreads="$2" cmssw="$3" arch="$4" pset="$5" step_name="$6" use_tmpfs="$7"
    local events_per_inst=$((EVENTS_PER_JOB / n_par))
    local remainder=$((EVENTS_PER_JOB % n_par))

    echo "  Parallel execution: $n_par instances, $events_per_inst events each"

    # Prepare siteconf for container access
    local SITE_CFG="${SITECONFIG_PATH:-}"
    if [[ -z "$SITE_CFG" || ! -d "$SITE_CFG/JobConfig" ]]; then
        for _try in /opt/cms/siteconf /cvmfs/cms.cern.ch/SITECONF/local; do
            if [[ -f "$_try/JobConfig/site-local-config.xml" ]]; then
                SITE_CFG="$_try"
                break
            fi
        done
    fi
    echo "siteconf: ${SITE_CFG:-NOT FOUND}"
    mkdir -p "$WORK_DIR/_siteconf/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
    if [[ -d "$SITE_CFG/PhEDEx" ]]; then
        cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
    elif [[ -d "$SITE_CFG/../PhEDEx" ]]; then
        cp -r "$SITE_CFG/../PhEDEx" "$WORK_DIR/_siteconf/"
    fi
    if [[ -f "$SITE_CFG/storage.json" ]]; then
        cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
    elif [[ -f "$SITE_CFG/../storage.json" ]]; then
        cp "$SITE_CFG/../storage.json" "$WORK_DIR/_siteconf/"
    fi
    [[ -f "$WORK_DIR/_siteconf/storage.json" ]] && cp "$WORK_DIR/_siteconf/storage.json" "$WORK_DIR/"
    mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
    if [[ -d "$SITE_CFG/PhEDEx" ]]; then
        cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"
    elif [[ -d "$SITE_CFG/../PhEDEx" ]]; then
        cp -r "$SITE_CFG/../PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"
    fi
    for slc in "$WORK_DIR/_siteconf/JobConfig/site-local-config.xml" \
               "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/site-local-config.xml"; do
        [[ -f "$slc" ]] && sed -i "s|trivialcatalog_file:[^?]*storage.xml|trivialcatalog_file:$WORK_DIR/_siteconf/PhEDEx/storage.xml|g" "$slc"
    done

    # Setup CMSSW project once (before forking instances) via cmssw-env
    cat > "$WORK_DIR/_setup_cmssw.sh" <<SETUPEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $WORK_DIR
if [[ ! -d $cmssw/src ]]; then
    scramv1 project CMSSW $cmssw
fi
SETUPEOF
    chmod +x "$WORK_DIR/_setup_cmssw.sh"
    echo "  Setting up CMSSW project via cmssw-env..."
    cmssw_env_exec "$arch" bash "$WORK_DIR/_setup_cmssw.sh"

    # Instance base directory: tmpfs (/dev/shm) avoids virtiofs/FUSE
    # file descriptor limits during gridpack extraction (29K+ files each).
    # ExternalLHEProducer extracts gridpacks into cmsRun's CWD, so placing
    # the CWD on tmpfs keeps extraction entirely in RAM.
    local TMPFS_BASE
    if [[ "$use_tmpfs" == "true" ]]; then
        TMPFS_BASE="/dev/shm/wms2_step0_$$"
        echo "  Using tmpfs for parallel instances: $TMPFS_BASE"
    else
        TMPFS_BASE="$WORK_DIR/_step0_parallel"
        echo "  Using disk for parallel instances: $TMPFS_BASE"
    fi
    mkdir -p "$TMPFS_BASE"
    trap "rm -rf $TMPFS_BASE 2>/dev/null" EXIT

    # Create instance directories and fork
    local pids=()
    local pset_base
    pset_base=$(basename "$pset")
    for inst in $(seq 0 $((n_par - 1))); do
        local idir="$TMPFS_BASE/step1_inst${inst}"
        local odir="$WORK_DIR/step1_inst${inst}"
        mkdir -p "$idir" "$odir"
        # Symlink CMSSW project and archive files (gridpacks)
        ln -sf "$WORK_DIR/$cmssw" "$idir/$cmssw"
        ln -sf "$WORK_DIR"/*.xz "$WORK_DIR"/*.gz "$idir/" 2>/dev/null || true
        # Copy PSet (needs per-instance modification)
        cp "$WORK_DIR/$pset" "$idir/$pset_base"

        # Compute per-instance event range
        local inst_first=$((FIRST_EVENT + inst * events_per_inst))
        local inst_events=$events_per_inst
        [[ $inst -eq $((n_par - 1)) ]] && inst_events=$((events_per_inst + remainder))

        # Inject per-instance params into copied PSet
        inject_pset_parallel "$idir/$pset_base" "$inst_first" "$inst_events" "$nthreads"

        # Launch instance via cmssw-env
        cat > "$idir/_step_runner.sh" <<INSTEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $idir/$cmssw/src && eval \$(scramv1 runtime -sh) && cd $idir
cmsRun -j report_step1.xml $pset_base
INSTEOF
        chmod +x "$idir/_step_runner.sh"
        ( cmssw_env_exec "$arch" bash "$idir/_step_runner.sh" ) &
        pids+=($!)
        echo "  Instance $inst: pid=${pids[-1]} firstEvent=$inst_first maxEvents=$inst_events"
    done

    # Wait for all instances
    local failed=0
    for pid in "${pids[@]}"; do
        wait "$pid" || failed=$((failed + 1))
    done
    if [[ $failed -gt 0 ]]; then
        echo "ERROR: $failed/$n_par parallel instances failed" >&2
        exit 1
    fi
    echo "  All $n_par instances completed successfully"

    # Collect outputs: copy from tmpfs back to WORK_DIR
    PREV_OUTPUT=""
    for inst in $(seq 0 $((n_par - 1))); do
        local idir="$TMPFS_BASE/step1_inst${inst}"
        local odir="$WORK_DIR/step1_inst${inst}"
        # Copy ROOT outputs and FJR from tmpfs to persistent storage
        cp "$idir"/*.root "$odir/" 2>/dev/null || true
        cp "$idir/report_step1.xml" "$odir/"
        local out
        out=$(parse_fjr_output "$odir/report_step1.xml")
        if [[ -n "$out" ]]; then
            [[ -n "$PREV_OUTPUT" ]] && PREV_OUTPUT="${PREV_OUTPUT},"
            PREV_OUTPUT="${PREV_OUTPUT}${odir}/${out}"
        fi
        cp "$odir/report_step1.xml" "$WORK_DIR/report_step1_inst${inst}.xml"
    done
    echo "  Collected outputs: $PREV_OUTPUT"

    # Clean up instance directories
    rm -rf "$TMPFS_BASE"
    echo "  Cleaned up parallel instance dirs"
}

# ── All-step pipeline split execution ─────────────────────────
run_all_steps_pipeline() {
    local n_pipelines="$1"
    local events_per_pipe=$((EVENTS_PER_JOB / n_pipelines))
    local remainder=$((EVENTS_PER_JOB % n_pipelines))

    echo "--- Pipeline mode: $n_pipelines pipelines ---"
    echo "  Events per pipeline: $events_per_pipe (remainder: $remainder)"

    NUM_STEPS=$(python3 -c "import json; print(len(json.load(open('manifest.json'))['steps']))")
    echo "  Steps: $NUM_STEPS"

    # Read step configs once (shared across pipelines)
    local step_names=()
    local step_cmssw=()
    local step_arch=()
    local step_pset=()
    local step_input_step=()
    local step_nthreads=()
    for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
        local sj
        sj=$(python3 -c "import json; s=json.load(open('manifest.json'))['steps'][$step_idx]; print(json.dumps(s))")
        step_names+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")")
        step_cmssw+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['cmssw_version'])")")
        step_arch+=("$(echo "$sj" | python3 -c "import sys,json; a=json.load(sys.stdin)['scram_arch']; print(a[0] if isinstance(a,list) else a)")")
        step_pset+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin)['pset'])")")
        step_input_step+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin).get('input_step',''))")")
        step_nthreads+=("$(echo "$sj" | python3 -c "import sys,json; print(json.load(sys.stdin).get('multicore',1))")")
    done

    # Prepare siteconf for container access
    local SITE_CFG="${SITECONFIG_PATH:-}"
    if [[ -z "$SITE_CFG" || ! -d "$SITE_CFG/JobConfig" ]]; then
        for _try in /opt/cms/siteconf /cvmfs/cms.cern.ch/SITECONF/local; do
            if [[ -f "$_try/JobConfig/site-local-config.xml" ]]; then
                SITE_CFG="$_try"
                break
            fi
        done
    fi
    echo "siteconf: ${SITE_CFG:-NOT FOUND}"
    mkdir -p "$WORK_DIR/_siteconf/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/JobConfig/" 2>/dev/null || true
    [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/"
    [[ -f "$SITE_CFG/storage.json" ]] && cp "$SITE_CFG/storage.json" "$WORK_DIR/_siteconf/"
    mkdir -p "$WORK_DIR/_siteconf/SITECONF/local/JobConfig"
    cp "$SITE_CFG/JobConfig/site-local-config.xml" "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/" 2>/dev/null || true
    [[ -d "$SITE_CFG/PhEDEx" ]] && cp -r "$SITE_CFG/PhEDEx" "$WORK_DIR/_siteconf/SITECONF/local/"
    for slc in "$WORK_DIR/_siteconf/JobConfig/site-local-config.xml" \
               "$WORK_DIR/_siteconf/SITECONF/local/JobConfig/site-local-config.xml"; do
        [[ -f "$slc" ]] && sed -i "s|trivialcatalog_file:[^?]*storage.xml|trivialcatalog_file:$WORK_DIR/_siteconf/PhEDEx/storage.xml|g" "$slc"
    done

    # Setup all unique CMSSW versions via cmssw-env
    local setup_versions=()
    for si in $(seq 0 $((NUM_STEPS - 1))); do
        local ver="${step_cmssw[$si]}"
        local arch="${step_arch[$si]}"
        local already=0
        for sv in "${setup_versions[@]}"; do
            [[ "$sv" == "$ver" ]] && already=1
        done
        if [[ $already -eq 0 ]]; then
            setup_versions+=("$ver")
            cat > "$WORK_DIR/_setup_cmssw_${ver}.sh" <<SETUPEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $WORK_DIR
if [[ ! -d $ver/src ]]; then
    scramv1 project CMSSW $ver
fi
SETUPEOF
            chmod +x "$WORK_DIR/_setup_cmssw_${ver}.sh"
            echo "  Setting up CMSSW $ver via cmssw-env..."
            cmssw_env_exec "$arch" bash "$WORK_DIR/_setup_cmssw_${ver}.sh"
        fi
    done

    # Fork N pipelines
    local pids=()
    for pipe in $(seq 0 $((n_pipelines - 1))); do
        local pdir="$WORK_DIR/pipeline_${pipe}"
        mkdir -p "$pdir"

        # Compute per-pipeline event range (step 0 firstEvent partitioning)
        local pipe_first=$((FIRST_EVENT + pipe * events_per_pipe))
        local pipe_events=$events_per_pipe
        [[ $pipe -eq $((n_pipelines - 1)) ]] && pipe_events=$((events_per_pipe + remainder))

        echo "  Pipeline $pipe: firstEvent=$pipe_first maxEvents=$pipe_events"

        # Symlink CMSSW projects and gridpack archives
        for si in $(seq 0 $((NUM_STEPS - 1))); do
            ln -sf "$WORK_DIR/${step_cmssw[$si]}" "$pdir/${step_cmssw[$si]}" 2>/dev/null || true
        done
        ln -sf "$WORK_DIR"/*.xz "$WORK_DIR"/*.gz "$pdir/" 2>/dev/null || true

        # Copy ALL PSet files into pipeline dir, preserving directory structure
        # (PSets may share the same basename, e.g. steps/step1/cfg.py, steps/step2/cfg.py)
        for si in $(seq 0 $((NUM_STEPS - 1))); do
            local pset_dir
            pset_dir=$(dirname "${step_pset[$si]}")
            mkdir -p "$pdir/$pset_dir"
            cp "$WORK_DIR/${step_pset[$si]}" "$pdir/${step_pset[$si]}"
        done

        # Launch pipeline subshell
        (
            cd "$pdir"
            local PREV_OUTPUT=""

            for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
                local step_num=$((step_idx + 1))
                local sname="${step_names[$step_idx]}"
                local cmssw="${step_cmssw[$step_idx]}"
                local arch="${step_arch[$step_idx]}"
                local pset_file="${step_pset[$step_idx]}"
                local input_step="${step_input_step[$step_idx]}"
                local nthreads="${step_nthreads[$step_idx]}"

                echo "  [pipe$pipe] Step $step_num: $sname (nThreads=$nthreads)"

                # Determine input files
                local INJECT_INPUT=""
                if [[ -n "$input_step" && -n "$PREV_OUTPUT" ]]; then
                    IFS=',' read -ra _PO_ARRAY <<< "$PREV_OUTPUT"
                    for _po in "${_PO_ARRAY[@]}"; do
                        [[ -n "$INJECT_INPUT" ]] && INJECT_INPUT="${INJECT_INPUT},"
                        INJECT_INPUT="${INJECT_INPUT}file:${_po}"
                    done
                fi

                # Inject runtime overrides into PSet
                python3 -c "
import os
pset = '$pset_file'
lines = ['', '# --- WMS2 pipeline PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')

inject_input = '$INJECT_INPUT'
if inject_input:
    file_list = inject_input.split(',')
    quoted = ', '.join(repr(f) for f in file_list)
    lines.append('process.source.fileNames = cms.untracked.vstring(' + quoted + ')')
    lines.append('process.source.secondaryFileNames = cms.untracked.vstring()')

step_idx = $step_idx
pipe_events = $pipe_events
pipe_first = $pipe_first
if step_idx == 0 and pipe_events > 0:
    gen_events = pipe_events
    lines.append('process.maxEvents.input = cms.untracked.int32(' + str(gen_events) + ')')
    lines.append('if hasattr(process, \"externalLHEProducer\"):')
    lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32(' + str(gen_events) + ')')
elif step_idx > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(-1)')
if step_idx == 0 and pipe_first > 0:
    lines.append('process.source.firstEvent = cms.untracked.uint32(' + str(pipe_first) + ')')

nthreads = $nthreads
if nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32(' + str(nthreads) + ')')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')

# Override pileup file list with Rucio-resolved on-disk files.
# At planning time we determine which pileup dataset this step needs.
# We emit PSet code that reads pileup_files.json at cmsRun time and
# appends LFNs one-by-one (avoids Python's 255-argument call limit).
import json as _json
_pu_found = False
for _pu_candidate in [os.path.join(os.path.dirname(pset), '..', 'pileup_files.json'), 'pileup_files.json']:
    if os.path.isfile(_pu_candidate):
        _pu_found = True
        break
if _pu_found:
    _manifest_path = os.path.join(os.path.dirname(pset), '..', 'manifest.json')
    if not os.path.isfile(_manifest_path):
        _manifest_path = 'manifest.json'
    if os.path.isfile(_manifest_path):
        _manifest = _json.load(open(_manifest_path))
        _step = _manifest.get('steps', [{}])[step_idx] if step_idx < len(_manifest.get('steps', [])) else {}
        _ds = _step.get('mc_pileup') or _step.get('data_pileup', '')
        if _ds:
            lines.append('import json as _pujson, os as _puos, random as _purandom')
            lines.append('_pu_json = None')
            lines.append('for _pp in [_puos.path.join(_puos.path.dirname(_puos.path.abspath(\"' + pset + '\")), \"..\", \"pileup_files.json\"), \"pileup_files.json\"]:')
            lines.append('    if _puos.path.isfile(_pp):')
            lines.append('        _pu_json = _pp')
            lines.append('        break')
            lines.append('if _pu_json:')
            lines.append('    _pu_data = _pujson.load(open(_pu_json))')
            lines.append('    _pu_list = _pu_data.get(\"' + _ds + '\", [])')
            lines.append('    if _pu_list:')
            lines.append('        _purandom.shuffle(_pu_list)')
            lines.append('        _pu_remote = (\"$PILEUP_REMOTE_READ\" == \"true\")')
            lines.append('        if _pu_remote:')
            lines.append('            _pu_prefix = \"root://cms-xrd-global.cern.ch/\"')
            lines.append('            _pu_pfns = [(_pu_prefix + f if not f.startswith(\"root://\") else f) for f in _pu_list]')
            lines.append('        else:')
            lines.append('            _pu_pfns = _pu_list')
            lines.append('        for _mixer in [\"mixData\", \"mix\"]:')
            lines.append('            _mobj = getattr(process, _mixer, None)')
            lines.append('            if _mobj is None: continue')
            lines.append('            _inp = getattr(_mobj, \"input\", None) or getattr(_mobj, \"secsource\", None)')
            lines.append('            if _inp is None: continue')
            lines.append('            _inp.fileNames = cms.untracked.vstring()')
            lines.append('            for _pfn in _pu_pfns: _inp.fileNames.append(str(_pfn))')
            lines.append('            _how = (\"via global redirector\" if _pu_remote else \"via local catalog\")')
            lines.append('            print(\"WMS2: Overrode \" + _mixer + \".input.fileNames with \" + str(len(_pu_pfns)) + \" pileup files \" + _how)')
else:
    # No pileup_files.json — prefix existing pileup LFNs with AAA global redirector
    lines.append('if (\"$PILEUP_REMOTE_READ\" == \"true\"):')
    lines.append('    _pu_prefix = \"root://cms-xrd-global.cern.ch/\"')
    lines.append('    for _mixer in [\"mixData\", \"mix\"]:')
    lines.append('        _mobj = getattr(process, _mixer, None)')
    lines.append('        if _mobj is None: continue')
    lines.append('        _inp = getattr(_mobj, \"input\", None) or getattr(_mobj, \"secsource\", None)')
    lines.append('        if _inp is None: continue')
    lines.append('        _old = list(_inp.fileNames)')
    lines.append('        if _old:')
    lines.append('            _inp.fileNames = cms.untracked.vstring()')
    lines.append('            for _fn in _old:')
    lines.append('                _pfn = (_pu_prefix + _fn) if not _fn.startswith(\"root://\") else _fn')
    lines.append('                _inp.fileNames.append(str(_pfn))')
    lines.append('            print(\"WMS2: Prefixed \" + str(len(_old)) + \" existing pileup LFNs in \" + _mixer + \" with AAA global redirector\")')

with open(pset, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"

                # Execute cmsRun
                local CMSRUN_ARGS="-j report_step${step_num}.xml $pset_file"
                local STEP_START
                STEP_START=$(date +%s)

                # Execute via cmssw-env
                cat > "$pdir/_pipe${pipe}_step${step_num}.sh" <<PIPEEOF
#!/bin/bash
set -e
export SITECONFIG_PATH=$WORK_DIR/_siteconf
export CMS_PATH=$WORK_DIR/_siteconf
export SCRAM_ARCH=$arch
export X509_CERT_DIR="${X509_CERT_DIR:-/cvmfs/grid.cern.ch/etc/grid-security/certificates}"
# Copy proxy to working dir with 600 perms (xrootd 5.4.x rejects world-readable certs)
if [[ -n "${X509_USER_PROXY:-}" && -f "${X509_USER_PROXY}" ]]; then
    cp "$X509_USER_PROXY" "$WORK_DIR/_x509up"
    chmod 600 "$WORK_DIR/_x509up"
    export X509_USER_PROXY="$WORK_DIR/_x509up"
fi
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd $pdir/$cmssw/src && eval \$(scramv1 runtime -sh) && cd $pdir
cmsRun $CMSRUN_ARGS
PIPEEOF
                chmod +x "$pdir/_pipe${pipe}_step${step_num}.sh"
                cmssw_env_exec "$arch" bash "$pdir/_pipe${pipe}_step${step_num}.sh"

                local STEP_RC=$?
                local STEP_END
                STEP_END=$(date +%s)
                local STEP_WALL=$((STEP_END - STEP_START))

                if [[ $STEP_RC -ne 0 ]]; then
                    echo "  [pipe$pipe] ERROR: Step $sname failed with exit code $STEP_RC" >&2
                    exit $STEP_RC
                fi
                echo "  [pipe$pipe] Step $sname completed in ${STEP_WALL}s"

                # Parse FJR output for next step input
                PREV_OUTPUT=$(parse_fjr_output "$pdir/report_step${step_num}.xml")
                echo "  [pipe$pipe] Output: ${PREV_OUTPUT:-none}"
            done
        ) &
        pids+=($!)
    done

    # Wait for all pipelines
    local failed=0
    for pid in "${pids[@]}"; do
        wait "$pid" || failed=$((failed + 1))
    done
    if [[ $failed -gt 0 ]]; then
        echo "ERROR: $failed/$n_pipelines pipelines failed" >&2
        exit 1
    fi
    echo "  All $n_pipelines pipelines completed successfully"

    # Collect outputs: copy FJR + ROOT files from pipeline dirs to WORK_DIR
    for pipe in $(seq 0 $((n_pipelines - 1))); do
        local pdir="$WORK_DIR/pipeline_${pipe}"
        # Copy FJR files with pipeline suffix
        for fjr in "$pdir"/report_step*.xml; do
            if [[ -f "$fjr" ]]; then
                local base
                base=$(basename "$fjr" .xml)
                cp "$fjr" "$WORK_DIR/${base}_pipe${pipe}.xml"
            fi
        done
        # Copy ROOT files with pipeline prefix
        for rf in "$pdir"/*.root; do
            if [[ -f "$rf" ]]; then
                local base
                base=$(basename "$rf")
                cp "$rf" "$WORK_DIR/pipe${pipe}_${base}"
            fi
        done
    done
    echo "  Collected outputs from $n_pipelines pipelines"
}

# ── CMSSW mode (per-step CMSSW/ScramArch with apptainer) ─────
run_cmssw_mode() {
    echo "--- CMSSW mode ---"

    NUM_STEPS=$(python3 -c "import json; print(len(json.load(open('manifest.json'))['steps']))")
    echo "steps: $NUM_STEPS"

    # Check CVMFS
    if [[ ! -d /cvmfs/cms.cern.ch ]]; then
        echo "ERROR: /cvmfs/cms.cern.ch not available" >&2
        exit 1
    fi

    # Pipeline mode dispatch: n_pipelines > 1 runs all steps in parallel pipelines
    N_PIPELINES=$(python3 -c "import json; print(json.load(open('manifest.json')).get('n_pipelines', 1))")
    SPLIT_TMPFS=$(python3 -c "import json; print(str(json.load(open('manifest.json')).get('split_tmpfs', False)).lower())")
    if [[ "$N_PIPELINES" -gt 1 ]]; then
        echo "Pipeline mode: $N_PIPELINES pipelines"
        run_all_steps_pipeline "$N_PIPELINES"
        # Fall through to existing output rename + manifest + metrics code
    else

    # Build xrootd input file list for first step
    IFS=',' read -ra LFN_ARRAY <<< "$INPUT_LFNS"
    XROOTD_INPUTS=""
    for lfn in "${LFN_ARRAY[@]}"; do
        url=$(lfn_to_xrootd "$lfn")
        if [[ -n "$url" ]]; then
            if [[ -n "$XROOTD_INPUTS" ]]; then
                XROOTD_INPUTS="${XROOTD_INPUTS},$url"
            else
                XROOTD_INPUTS="$url"
            fi
        fi
    done

    PREV_OUTPUT=""

    # ── Background memory monitor ──────────────────────────────
    # Samples cgroup memory every 2 seconds to capture transient peaks
    # that condor's 5-minute updates and FJR event-boundary sampling miss.
    # Output: memory_monitor.log columns:
    #   ts_sec cgroup_mb anon_mb file_mb shmem_mb tmpfs_present
    # - cgroup_mb: total from memory.current (what cgroup limit enforces)
    # - anon_mb:   anonymous pages from memory.stat (process RSS)
    # - file_mb:   file-backed pages from memory.stat (page cache, reclaimable)
    # - shmem_mb:  shared/tmpfs pages from memory.stat (non-reclaimable)
    # - tmpfs_present: 1 if /dev/shm/wms2_step0_$$ exists, else 0
    MEMMON_LOG="$WORK_DIR/memory_monitor.log"
    # Locate cgroup v2 memory file for this job's cgroup
    _memmon_cgroup_file=""
    _memmon_stat_file=""
    _cg_path=$(cat /proc/self/cgroup 2>/dev/null | sed 's/^0:://')
    if [[ -n "$_cg_path" ]]; then
        _candidate="/sys/fs/cgroup${_cg_path}/memory.current"
        if [[ -r "$_candidate" ]]; then
            _memmon_cgroup_file="$_candidate"
            _stat_candidate="/sys/fs/cgroup${_cg_path}/memory.stat"
            [[ -r "$_stat_candidate" ]] && _memmon_stat_file="$_stat_candidate"
        fi
    fi
    # Fallback: cgroup v2 root or v1
    if [[ -z "$_memmon_cgroup_file" ]]; then
        if [[ -r /sys/fs/cgroup/memory.current ]]; then
            _memmon_cgroup_file="/sys/fs/cgroup/memory.current"
            [[ -r /sys/fs/cgroup/memory.stat ]] && _memmon_stat_file="/sys/fs/cgroup/memory.stat"
        elif [[ -r /sys/fs/cgroup/memory/memory.usage_in_bytes ]]; then
            _memmon_cgroup_file="/sys/fs/cgroup/memory/memory.usage_in_bytes"
        fi
    fi
    echo "Memory monitor cgroup file: ${_memmon_cgroup_file:-none}"
    echo "Memory monitor stat file:   ${_memmon_stat_file:-none}"
    # Save wrapper PID for tmpfs detection inside the subshell
    # ($$ doesn't change in subshells, but be explicit)
    _wrapper_pid=$$
    (
        echo "# ts_sec cgroup_mb anon_mb file_mb shmem_mb tmpfs_present" > "$MEMMON_LOG"
        while true; do
            ts=$(date +%s)
            # Cgroup total memory (RSS + page cache + tmpfs + kernel)
            cg=0
            if [[ -n "$_memmon_cgroup_file" && -r "$_memmon_cgroup_file" ]]; then
                cg_bytes=$(cat "$_memmon_cgroup_file" 2>/dev/null || echo 0)
                cg=$((cg_bytes / 1048576))
            fi
            # Breakdown from memory.stat (cgroup v2)
            anon=0; file_mb=0; shmem=0
            if [[ -n "$_memmon_stat_file" && -r "$_memmon_stat_file" ]]; then
                eval "$(awk '/^anon / {printf "anon=%d;",int($2/1048576)} /^file / {printf "file_mb=%d;",int($2/1048576)} /^shmem / {printf "shmem=%d;",int($2/1048576)}' "$_memmon_stat_file" 2>/dev/null)"
            fi
            # Check if tmpfs step0 dir exists
            tmpfs_flag=0
            [[ -d /dev/shm/wms2_step0_${_wrapper_pid} ]] && tmpfs_flag=1
            echo "$ts $cg $anon $file_mb $shmem $tmpfs_flag" >> "$MEMMON_LOG"
            sleep 2
        done
    ) &
    MEMMON_PID=$!
    echo "Memory monitor started (pid $MEMMON_PID, log: memory_monitor.log)"

    GRIDPACK_DISK_MB=0
    for step_idx in $(seq 0 $((NUM_STEPS - 1))); do
        step_num=$((step_idx + 1))

        # Read step config from manifest
        STEP_JSON=$(python3 -c "import json; s=json.load(open('manifest.json'))['steps'][$step_idx]; print(json.dumps(s))")
        STEP_NAME=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['name'])")
        CMSSW_VER=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['cmssw_version'])")
        STEP_ARCH=$(echo "$STEP_JSON" | python3 -c "import sys,json; a=json.load(sys.stdin)['scram_arch']; print(a[0] if isinstance(a,list) else a)")
        PSET_PATH=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['pset'])")
        INPUT_STEP=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('input_step',''))")
        NTHREADS=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('multicore',1))")
        N_PARALLEL=$(echo "$STEP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin).get('n_parallel',1))")

        echo ""
        echo "--- Step $step_num: $STEP_NAME ---"
        echo "  CMSSW:  $CMSSW_VER"
        echo "  Arch:   $STEP_ARCH"
        echo "  PSet:   $PSET_PATH"

        # Parallel step 0: fork N_PARALLEL cmsRun instances with firstEvent partitioning
        if [[ $step_idx -eq 0 && "$N_PARALLEL" -gt 1 ]]; then
            echo "  n_parallel: $N_PARALLEL"
            STEP_START=$(date +%s)
            run_step0_parallel "$N_PARALLEL" "$NTHREADS" "$CMSSW_VER" "$STEP_ARCH" "$PSET_PATH" "$STEP_NAME" "$SPLIT_TMPFS"
            STEP_END=$(date +%s)
            STEP_WALL=$((STEP_END - STEP_START))
            echo "  Step $STEP_NAME completed in ${STEP_WALL}s (parallel, $N_PARALLEL instances)"
            continue
        fi

        # Build cmsRun command (no command-line overrides — ConfigCache PSets
        # ignore them; inject everything into PSet via python append).
        CMSRUN_ARGS="-j report_step${step_num}.xml $PSET_PATH"

        # Determine input files for PSet injection
        INJECT_INPUT=""
        if [[ -n "$INPUT_STEP" && -n "$PREV_OUTPUT" ]]; then
            # PREV_OUTPUT may be comma-separated (from parallel step 0)
            INJECT_INPUT=""
            IFS=',' read -ra _PO_ARRAY <<< "$PREV_OUTPUT"
            for _po in "${_PO_ARRAY[@]}"; do
                [[ -n "$INJECT_INPUT" ]] && INJECT_INPUT="${INJECT_INPUT},"
                INJECT_INPUT="${INJECT_INPUT}file:${_po}"
            done
        elif [[ $step_idx -eq 0 && -n "$XROOTD_INPUTS" ]]; then
            INJECT_INPUT="$XROOTD_INPUTS"
        fi

        # Inject runtime overrides into PSet (input files, maxEvents, nThreads)
        python3 -c "
import os
pset = '$PSET_PATH'
lines = ['', '# --- WMS2 runtime PSet injection ---']
lines.append('import FWCore.ParameterSet.Config as cms')

# Input files override
inject_input = '$INJECT_INPUT'
if inject_input:
    file_list = inject_input.split(',')
    quoted = ', '.join(repr(f) for f in file_list)
    lines.append('process.source.fileNames = cms.untracked.vstring(' + quoted + ')')
    lines.append('process.source.secondaryFileNames = cms.untracked.vstring()')

# maxEvents: step 1 uses EVENTS_PER_JOB (generated events); steps 2+ use -1 (all input).
# ConfigCache PSets have hardcoded maxEvents from cmsDriver that must be overridden.
# Convention: events_per_job is already in terms of *generated* events (matching
# WMAgent's calcEvtsPerJobLumi = target_job_length / TimePerEvent). No division
# by filter_efficiency — that inflation is applied to total_events at planning time.
step_idx = $step_idx
events_per_job = $EVENTS_PER_JOB
first_event = $FIRST_EVENT
if step_idx == 0 and events_per_job > 0:
    gen_events = events_per_job
    lines.append('process.maxEvents.input = cms.untracked.int32(' + str(gen_events) + ')')
    # Also update ExternalLHEProducer.nEvents if present (wmLHE workflows)
    # The gridpack must produce exactly as many LHE events as cmsRun will consume,
    # otherwise ExternalLHEProducer throws a fatal error at end-of-run.
    lines.append('if hasattr(process, \"externalLHEProducer\"):')
    lines.append('    process.externalLHEProducer.nEvents = cms.untracked.uint32(' + str(gen_events) + ')')
elif step_idx > 0:
    lines.append('process.maxEvents.input = cms.untracked.int32(-1)')
if step_idx == 0 and first_event > 0:
    lines.append('process.source.firstEvent = cms.untracked.uint32(' + str(first_event) + ')')

# Randomize seeds for GEN step — without this, all jobs use the same hardcoded
# seeds from IOMC.RandomEngine.IOMC_cff and generate identical physics events.
# Uses CMSSW's built-in RandomNumberServiceHelper.populate() which draws from
# /dev/urandom (same mechanism WMAgent uses via AutomaticSeeding).
if step_idx == 0:
    lines.append('if hasattr(process, \"RandomNumberGeneratorService\"):')
    lines.append('    from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper')
    lines.append('    _rng_helper = RandomNumberServiceHelper(process.RandomNumberGeneratorService)')
    lines.append('    _rng_helper.populate()')

# nThreads
nthreads = $NTHREADS
if nthreads > 1:
    lines.append('if not hasattr(process, \"options\"):')
    lines.append('    process.options = cms.untracked.PSet()')
    lines.append('process.options.numberOfThreads = cms.untracked.uint32(' + str(nthreads) + ')')
    lines.append('process.options.numberOfStreams = cms.untracked.uint32(0)')

# Override pileup file list with Rucio-resolved on-disk files.
# At planning time we determine which pileup dataset this step needs.
# We emit PSet code that reads pileup_files.json at cmsRun time and
# appends LFNs one-by-one (avoids Python's 255-argument call limit).
import json as _json
_pu_found = False
for _pu_candidate in [os.path.join(os.path.dirname(pset), '..', 'pileup_files.json'), 'pileup_files.json']:
    if os.path.isfile(_pu_candidate):
        _pu_found = True
        break
if _pu_found:
    _manifest_path = os.path.join(os.path.dirname(pset), '..', 'manifest.json')
    if not os.path.isfile(_manifest_path):
        _manifest_path = 'manifest.json'
    if os.path.isfile(_manifest_path):
        _manifest = _json.load(open(_manifest_path))
        _step = _manifest.get('steps', [{}])[step_idx] if step_idx < len(_manifest.get('steps', [])) else {}
        _ds = _step.get('mc_pileup') or _step.get('data_pileup', '')
        if _ds:
            lines.append('import json as _pujson, os as _puos, random as _purandom')
            lines.append('_pu_json = None')
            lines.append('for _pp in [_puos.path.join(_puos.path.dirname(_puos.path.abspath(\"' + pset + '\")), \"..\", \"pileup_files.json\"), \"pileup_files.json\"]:')
            lines.append('    if _puos.path.isfile(_pp):')
            lines.append('        _pu_json = _pp')
            lines.append('        break')
            lines.append('if _pu_json:')
            lines.append('    _pu_data = _pujson.load(open(_pu_json))')
            lines.append('    _pu_list = _pu_data.get(\"' + _ds + '\", [])')
            lines.append('    if _pu_list:')
            lines.append('        _purandom.shuffle(_pu_list)')
            lines.append('        _pu_remote = (\"$PILEUP_REMOTE_READ\" == \"true\")')
            lines.append('        if _pu_remote:')
            lines.append('            _pu_prefix = \"root://cms-xrd-global.cern.ch/\"')
            lines.append('            _pu_pfns = [(_pu_prefix + f if not f.startswith(\"root://\") else f) for f in _pu_list]')
            lines.append('        else:')
            lines.append('            _pu_pfns = _pu_list')
            lines.append('        for _mixer in [\"mixData\", \"mix\"]:')
            lines.append('            _mobj = getattr(process, _mixer, None)')
            lines.append('            if _mobj is None: continue')
            lines.append('            _inp = getattr(_mobj, \"input\", None) or getattr(_mobj, \"secsource\", None)')
            lines.append('            if _inp is None: continue')
            lines.append('            _inp.fileNames = cms.untracked.vstring()')
            lines.append('            for _pfn in _pu_pfns: _inp.fileNames.append(str(_pfn))')
            lines.append('            _how = (\"via global redirector\" if _pu_remote else \"via local catalog\")')
            lines.append('            print(\"WMS2: Overrode \" + _mixer + \".input.fileNames with \" + str(len(_pu_pfns)) + \" pileup files \" + _how)')
else:
    # No pileup_files.json — prefix existing pileup LFNs with AAA global redirector
    lines.append('if (\"$PILEUP_REMOTE_READ\" == \"true\"):')
    lines.append('    _pu_prefix = \"root://cms-xrd-global.cern.ch/\"')
    lines.append('    for _mixer in [\"mixData\", \"mix\"]:')
    lines.append('        _mobj = getattr(process, _mixer, None)')
    lines.append('        if _mobj is None: continue')
    lines.append('        _inp = getattr(_mobj, \"input\", None) or getattr(_mobj, \"secsource\", None)')
    lines.append('        if _inp is None: continue')
    lines.append('        _old = list(_inp.fileNames)')
    lines.append('        if _old:')
    lines.append('            _inp.fileNames = cms.untracked.vstring()')
    lines.append('            for _fn in _old:')
    lines.append('                _pfn = (_pu_prefix + _fn) if not _fn.startswith(\"root://\") else _fn')
    lines.append('                _inp.fileNames.append(str(_pfn))')
    lines.append('            print(\"WMS2: Prefixed \" + str(len(_old)) + \" existing pileup LFNs in \" + _mixer + \" with AAA global redirector\")')

with open(pset, 'a') as f:
    f.write(chr(10).join(lines) + chr(10))
"
        if [[ -n "$INJECT_INPUT" ]]; then
            echo "  input: $INJECT_INPUT (injected into PSet)"
        fi
        if [[ $step_idx -eq 0 && "$EVENTS_PER_JOB" -gt 0 ]]; then
            echo "  maxEvents: $EVENTS_PER_JOB (generated events, injected into PSet)"
        fi
        if [[ "$NTHREADS" -gt 1 ]]; then
            echo "  nThreads: $NTHREADS (injected into PSet)"
        fi

        echo "  cmsRun $CMSRUN_ARGS"
        STEP_START=$(date +%s)

        # Pre-measure gridpack uncompressed size from CVMFS (before step 0)
        # Parse ExternalLHEProducer.args from PSet to find gridpack path,
        # then use xz/gzip headers to get uncompressed size without extracting.
        if [[ $step_idx -eq 0 ]]; then
            GRIDPACK_DISK_MB=$(python3 -c "
import re, subprocess, os
pset = '${PSET_PATH}'
try:
    text = open(pset).read()
    # CMS gridpacks are always on /cvmfs/ — match the path directly
    m = re.search(r'(/cvmfs/\S+\.tar\.\w+)', text)
    if not m:
        print(0)
        raise SystemExit
    gp = m.group(1)
    if not os.path.isfile(gp):
        print(0)
        raise SystemExit
    if gp.endswith('.tar.xz') or gp.endswith('.xz'):
        r = subprocess.run(['xz', '-l', '--robot', gp], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=10)
        for line in r.stdout.strip().split(chr(10)):
            if line.startswith('totals'):
                parts = line.split()
                print(int(int(parts[4]) / 1048576))
                raise SystemExit
    elif gp.endswith('.tar.gz') or gp.endswith('.tgz'):
        sz = os.path.getsize(gp)
        # gzip -l unreliable for >4GB; estimate 10x ratio for gridpacks
        print(int(sz * 10 / 1048576))
        raise SystemExit
    print(0)
except SystemExit:
    pass
except Exception:
    print(0)
" 2>/dev/null)
            GRIDPACK_DISK_MB=${GRIDPACK_DISK_MB:-0}
            if [[ "$GRIDPACK_DISK_MB" -gt 0 ]]; then
                echo "  gridpack_uncompressed_mb: $GRIDPACK_DISK_MB"
            fi
        fi

        # Tmpfs for non-parallel step 0: use /dev/shm for gridpack extraction
        # (same benefit as parallel split tmpfs, but for single-instance jobs)
        STEP0_TMPFS=""
        if [[ $step_idx -eq 0 && "$SPLIT_TMPFS" == "true" && "$N_PARALLEL" -le 1 ]]; then
            STEP0_TMPFS="/dev/shm/wms2_step0_$$"
            mkdir -p "$STEP0_TMPFS"
            echo "  Using tmpfs for step 0: $STEP0_TMPFS"
            # Copy PSet to tmpfs working directory
            cp "$PSET_PATH" "$STEP0_TMPFS/"
        fi

        # Execute via cmssw-env (handles container resolution + bind mounts)
        if [[ -n "$STEP0_TMPFS" ]]; then
            # Pass tmpfs path to run_step — it cd's there INSIDE the container
            # after cmsset_default.sh (avoids apptainer /dev/null breakage).
            # FJR path is absolute to WORK_DIR; PSet path is absolute to tmpfs.
            CMSRUN_ARGS="-j $(pwd)/report_step${step_num}.xml $STEP0_TMPFS/$(basename $PSET_PATH)"
            echo "  execution: cmssw-env (tmpfs CWD)"
            run_step "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS" "$STEP0_TMPFS" && STEP_RC=0 || STEP_RC=$?
            # Measure tmpfs usage before cleanup
            TMPFS_MB=$(du -sm "$STEP0_TMPFS" 2>/dev/null | awk '{print $1}')
            echo "  tmpfs_usage_mb: ${TMPFS_MB:-0}"
            rm -rf "$STEP0_TMPFS"
            echo "  Cleaned tmpfs: $STEP0_TMPFS"
        else
            echo "  execution: cmssw-env ($STEP_ARCH)"
            run_step "$CMSSW_VER" "$STEP_ARCH" "$CMSRUN_ARGS" && STEP_RC=0 || STEP_RC=$?
        fi

        STEP_END=$(date +%s)
        STEP_WALL=$((STEP_END - STEP_START))

        if [[ $STEP_RC -ne 0 ]]; then
            echo "ERROR: Step $STEP_NAME (cmsRun) failed with exit code $STEP_RC" >&2
            # Clean up tmpfs on failure too
            [[ -n "$STEP0_TMPFS" && -d "$STEP0_TMPFS" ]] && rm -rf "$STEP0_TMPFS"
            exit $STEP_RC
        fi

        echo "  Step $STEP_NAME completed in ${STEP_WALL}s"

        # Parse FrameworkJobReport for output file (strip file: prefix)
        # When a step produces multiple outputs (e.g. GEN-SIM + LHE), prefer
        # the non-LHE output since the next step needs the physics data.
        PREV_OUTPUT=$(python3 -c "
import xml.etree.ElementTree as ET
try:
    tree = ET.parse('report_step${step_num}.xml')
    candidates = []
    for f in tree.findall('.//File'):
        pfn = f.findtext('PFN', '')
        if pfn.startswith('file:'): pfn = pfn[5:]
        if not pfn: continue
        label = f.findtext('ModuleLabel', '')
        candidates.append((pfn, label))
    # Prefer non-LHE output for next step's input
    for pfn, label in candidates:
        if 'LHE' not in label.upper():
            print(pfn)
            break
    else:
        # All are LHE or no label — take first
        if candidates:
            print(candidates[0][0])
except Exception:
    pass
" 2>/dev/null || true)
        # Fix output path: if FJR points to tmpfs (cleaned up), use WORK_DIR copy
        if [[ -n "$PREV_OUTPUT" && ! -f "$PREV_OUTPUT" ]]; then
            _PREV_BN=$(basename "$PREV_OUTPUT")
            if [[ -f "$_PREV_BN" ]]; then
                PREV_OUTPUT="$(pwd)/$_PREV_BN"
            fi
        fi
        echo "  Output: ${PREV_OUTPUT:-none}"

        # Safety net: detect 0-event output from step 1 (GenFilter rejected all)
        if [[ $step_idx -eq 0 ]]; then
            EVENTS_WRITTEN=$(python3 -c "
import xml.etree.ElementTree as ET
total = 0
try:
    tree = ET.parse('report_step${step_num}.xml')
    for f in tree.findall('.//File'):
        te = f.findtext('TotalEvents', '0')
        total += int(te)
except Exception:
    pass
print(total)
" 2>/dev/null || echo "0")
            if [[ "$EVENTS_WRITTEN" == "0" ]]; then
                echo "ERROR: Step 1 produced 0 output events (GenFilter rejected all)." >&2
                echo "  maxEvents=$EVENTS_PER_JOB (generated events)" >&2
                exit 80
            fi
            echo "  events_written: $EVENTS_WRITTEN"
        fi
    done

    fi  # end of sequential (non-pipeline) mode

    # Stop memory monitor
    if [[ -n "${MEMMON_PID:-}" ]]; then
        kill "$MEMMON_PID" 2>/dev/null || true
        wait "$MEMMON_PID" 2>/dev/null || true
        if [[ -f "$MEMMON_LOG" ]]; then
            SAMPLES=$(awk 'NR>1' "$MEMMON_LOG" | wc -l)
            PEAK_CG=$(awk 'NR>1 && $2+0 > mx {mx=$2+0} END {print mx+0}' "$MEMMON_LOG")
            PEAK_ANON=$(awk 'NR>1 && $3+0 > mx {mx=$3+0} END {print mx+0}' "$MEMMON_LOG")
            PEAK_FILE=$(awk 'NR>1 && $4+0 > mx {mx=$4+0} END {print mx+0}' "$MEMMON_LOG")
            PEAK_SHMEM=$(awk 'NR>1 && $5+0 > mx {mx=$5+0} END {print mx+0}' "$MEMMON_LOG")
            # Peak non-reclaimable (anon+shmem) split by tmpfs phase
            TMPFS_PEAK_NR=$(awk 'NR>1 && $6==1 {v=$3+$5; if(v>mx) mx=v} END {print mx+0}' "$MEMMON_LOG")
            NO_TMPFS_PEAK_ANON=$(awk 'NR>1 && $6==0 {v=$3+0; if(v>mx) mx=v} END {print mx+0}' "$MEMMON_LOG")
            PEAK_NR=$(awk 'NR>1 {v=$3+$5; if(v>mx) mx=v} END {print mx+0}' "$MEMMON_LOG")
            echo "Memory monitor: ${SAMPLES} samples"
            echo "  peak cgroup=${PEAK_CG} MB  (anon=${PEAK_ANON} file=${PEAK_FILE} shmem=${PEAK_SHMEM})"
            echo "  peak non-reclaimable=${PEAK_NR} MB  (tmpfs_phase=${TMPFS_PEAK_NR} no_tmpfs=${NO_TMPFS_PEAK_ANON})"
            # Write cgroup metrics JSON for adaptive algorithm
            python3 -c "
import json
data = {
    'peak_anon_mb': int('${PEAK_ANON}'),
    'peak_shmem_mb': int('${PEAK_SHMEM}'),
    'peak_nonreclaim_mb': int('${PEAK_NR}'),
    'tmpfs_peak_nonreclaim_mb': int('${TMPFS_PEAK_NR}'),
    'no_tmpfs_peak_anon_mb': int('${NO_TMPFS_PEAK_ANON}'),
    'num_samples': int('${SAMPLES}'),
    'gridpack_disk_mb': int('${GRIDPACK_DISK_MB}'),
}
out = 'proc_${NODE_INDEX}_cgroup.json'
with open(out, 'w') as f:
    json.dump(data, f, indent=2)
print(f'Wrote {out}')
" 2>/dev/null || true
        fi
    fi

    process_outputs

    END_TIME=$(date +%s)
    WALL_TIME=$((END_TIME - START_TIME))
    echo ""
    echo "=== CMSSW mode complete ==="
    echo "wall_time_sec: $WALL_TIME"
    echo "output_bytes:  $OUTPUT_SIZES"
}

# ── Shared output processing (FJR parse, manifest, metrics, stage-out) ──
process_outputs() {
    # Rename output ROOT files to include node index — prevents collisions
    # when HTCondor transfers outputs from multiple proc jobs to the same dir
    echo ""
    echo "--- Renaming outputs for merge disambiguation ---"
    for f in *.root; do
        if [[ -f "$f" ]]; then
            new_name="proc_${NODE_INDEX}_${f}"
            mv "$f" "$new_name"
            echo "  $f -> $new_name"
        fi
    done

    # Write output manifest — maps renamed files to their tiers and step indices
    # Parse FJRs (report_stepN.xml) to find each output file's data tier + step_index
    python3 -c "
import re, json, os, glob
import xml.etree.ElementTree as ET

# Build mapping: original_filename -> {tier, step_index} from FJR ModuleLabels
orig_to_info = {}
for fjr in sorted(glob.glob('report_step*.xml')):
    try:
        # Extract step number from filename (report_step1.xml -> step_index 0)
        step_num = int(re.search(r'report_step(\d+)', fjr).group(1))
        step_index = step_num - 1
        tree = ET.parse(fjr)
        for fnode in tree.findall('.//File'):
            pfn = fnode.findtext('PFN', '')
            if pfn.startswith('file:'): pfn = pfn[5:]
            pfn = os.path.basename(pfn)
            label = fnode.findtext('ModuleLabel', '')
            # ModuleLabel is like 'RAWSIMoutput', 'LHEoutput', 'MINIAODSIMoutput'
            tier = re.sub(r'output$', '', label, flags=re.IGNORECASE)
            if pfn and tier:
                orig_to_info[pfn] = {'tier': tier, 'step_index': step_index}
    except Exception:
        pass

# Map renamed files -> {tier, step_index}
files = {}
for f in sorted(os.listdir('.')):
    m = re.match(r'proc_\d+_(.+\.root)$', f)
    if m:
        orig = m.group(1)
        info = orig_to_info.get(orig)
        # Pipeline mode: orig may be 'pipe0_filename.root' — strip prefix
        if not info:
            pipe_m = re.match(r'pipe\d+_(.+)$', orig)
            if pipe_m:
                info = orig_to_info.get(pipe_m.group(1))
        if info:
            files[f] = info
        else:
            # Fallback: try to extract from filename pattern (stepN_TIER.root)
            m2 = re.match(r'step(\d+)_(.+)\.root', orig)
            if m2:
                files[f] = {'tier': m2.group(2), 'step_index': int(m2.group(1)) - 1}
            else:
                files[f] = {'tier': 'unknown', 'step_index': -1}

with open('proc_${NODE_INDEX}_outputs.json', 'w') as fh:
    json.dump(files, fh, indent=2)
print('Wrote proc_${NODE_INDEX}_outputs.json:', len(files), 'files')
for fn, info in files.items():
    print(f'  {fn} -> tier={info[\"tier\"]} step_index={info[\"step_index\"]}')
" 2>/dev/null || true

    # Extract per-step resource metrics from FJR XML files
    echo ""
    echo "--- Extracting per-step metrics from FJR ---"
    python3 -c "
import json, os, re
try:
    import xml.etree.ElementTree as ET
except ImportError:
    print('WARNING: xml.etree not available, skipping metrics')
    exit(0)

def parse_fjr_metrics(fjr_path, step_index):
    \"\"\"Extract resource metrics from a single FJR XML file.\"\"\"
    tree = ET.parse(fjr_path)
    root = tree.getroot()
    perf = root.find('.//PerformanceReport/PerformanceSummary')
    if perf is None:
        return None

    metrics = {'step': step_index + 1, 'step_index': step_index}

    # Helper to extract a metric value
    def get_metric(parent_name, metric_name, cast=float):
        for ps in root.findall('.//PerformanceReport/PerformanceSummary'):
            if ps.get('Metric') == parent_name:
                for m in ps.findall('Metric'):
                    if m.get('Name') == metric_name:
                        try:
                            return cast(m.get('Value', 0))
                        except (ValueError, TypeError):
                            return None
        return None

    # Timing
    wall = get_metric('Timing', 'TotalJobTime')
    cpu = get_metric('Timing', 'TotalJobCPU')
    nthreads = get_metric('Timing', 'NumberOfThreads', int)
    throughput = get_metric('Timing', 'EventThroughput')
    avg_ev_time = get_metric('Timing', 'AvgEventTime')

    metrics['wall_time_sec'] = round(wall, 2) if wall is not None else None
    metrics['cpu_time_sec'] = round(cpu, 2) if cpu is not None else None
    metrics['num_threads'] = nthreads
    metrics['throughput_ev_s'] = round(throughput, 4) if throughput is not None else None
    metrics['avg_event_time_sec'] = round(avg_ev_time, 4) if avg_ev_time is not None else None

    # CPU efficiency
    if wall and cpu and nthreads and wall > 0 and nthreads > 0:
        metrics['cpu_efficiency'] = round(cpu / (wall * nthreads), 4)
    else:
        metrics['cpu_efficiency'] = None

    # Memory
    metrics['peak_rss_mb'] = get_metric('ApplicationMemory', 'PeakValueRss')
    metrics['peak_vsize_mb'] = get_metric('ApplicationMemory', 'PeakValueVsize')

    # Events: prefer ProcessingSummary (physics events only, not pileup reads)
    nevents = get_metric('ProcessingSummary', 'NumberEvents', int)
    if nevents is None:
        nevents = 0
        for inp in root.findall('.//InputFile'):
            ev = inp.findtext('EventsRead')
            if ev:
                try:
                    nevents += int(ev)
                except ValueError:
                    pass
        nevents = nevents or None
    metrics['events_processed'] = nevents

    # Output events: sum TotalEvents from all <File> entries
    events_written = 0
    for f_elem in root.findall('.//File'):
        te = f_elem.findtext('TotalEvents', '0')
        try:
            events_written += int(te)
        except ValueError:
            pass
    metrics['events_written'] = events_written if events_written > 0 else nevents

    # Time per event
    if wall is not None and nevents and nevents > 0:
        metrics['time_per_event_sec'] = round(wall / nevents, 4)
    else:
        metrics['time_per_event_sec'] = None

    # Storage I/O
    read_mb = get_metric('StorageStatistics', 'Timing-file-read-totalMegabytes')
    write_mb = get_metric('StorageStatistics', 'Timing-file-write-totalMegabytes')
    metrics['read_mb'] = round(read_mb, 2) if read_mb is not None else None
    metrics['write_mb'] = round(write_mb, 2) if write_mb is not None else None

    return metrics

import glob as _gl

all_metrics = []
step_idx = 0
while True:
    fjr = f'report_step{step_idx + 1}.xml'
    inst_fjrs = sorted(_gl.glob(f'report_step{step_idx + 1}_inst*.xml'))
    pipe_fjrs = sorted(_gl.glob(f'report_step{step_idx + 1}_pipe*.xml'))
    if os.path.isfile(fjr):
        try:
            m = parse_fjr_metrics(fjr, step_idx)
            if m:
                all_metrics.append(m)
                print(f'  Step {step_idx + 1}: wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
        except Exception as e:
            print(f'  Step {step_idx + 1}: parse error: {e}')
    elif inst_fjrs:
        for ifjr in inst_fjrs:
            try:
                m = parse_fjr_metrics(ifjr, step_idx)
                if m:
                    all_metrics.append(m)
                    print(f'  Step {step_idx + 1} ({os.path.basename(ifjr)}): wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
            except Exception as e:
                print(f'  Step {step_idx + 1} ({os.path.basename(ifjr)}): parse error: {e}')
    elif pipe_fjrs:
        for pfjr in pipe_fjrs:
            try:
                m = parse_fjr_metrics(pfjr, step_idx)
                if m:
                    all_metrics.append(m)
                    print(f'  Step {step_idx + 1} ({os.path.basename(pfjr)}): wall={m[\"wall_time_sec\"]}s cpu_eff={m[\"cpu_efficiency\"]} rss={m[\"peak_rss_mb\"]}MB events={m[\"events_processed\"]}')
            except Exception as e:
                print(f'  Step {step_idx + 1} ({os.path.basename(pfjr)}): parse error: {e}')
    else:
        break
    step_idx += 1

if all_metrics:
    out = 'proc_${NODE_INDEX}_metrics.json'
    with open(out, 'w') as fh:
        json.dump(all_metrics, fh, indent=2)
    print(f'Wrote {out}: {len(all_metrics)} steps')
else:
    print('No FJR metrics extracted')
" 2>/dev/null || true

    # Collect output sizes
    echo ""
    echo "--- Output files ---"
    OUTPUT_SIZES=0
    for f in proc_*.root; do
        if [[ -f "$f" ]]; then
            sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
            echo "  $f: $sz bytes"
            OUTPUT_SIZES=$((OUTPUT_SIZES + sz))
        fi
    done

    # Stage out to unmerged site storage
    stage_out_to_unmerged
}

# ── Simulator mode ────────────────────────────────────────────
run_simulator_mode() {
    echo "--- Simulator mode ---"

    # Run the simulator script (included in sandbox)
    # Uses venv Python (embedded at DAG planning time) for numpy/uproot deps
    __VENV_PYTHON__ simulator.py \
        --manifest manifest.json \
        --node-index "$NODE_INDEX" \
        --events-per-job "$EVENTS_PER_JOB" \
        --first-event "$FIRST_EVENT"

    SIM_RC=$?
    if [[ $SIM_RC -ne 0 ]]; then
        echo "ERROR: Simulator failed with exit code $SIM_RC" >&2
        exit $SIM_RC
    fi

    # Shared output processing: rename, build manifest, extract metrics, stage-out
    process_outputs

    END_TIME=$(date +%s)
    WALL_TIME=$((END_TIME - START_TIME))
    echo ""
    echo "=== Simulator mode complete ==="
    echo "wall_time_sec: $WALL_TIME"
    echo "output_bytes:  $OUTPUT_SIZES"
}

# ── Synthetic mode ────────────────────────────────────────────
run_synthetic_mode() {
    echo "--- Synthetic mode ---"

    # Read params from manifest (or use defaults)
    SIZE_PER_EVENT_KB=50.0
    TIME_PER_EVENT_SEC=0.5
    MEMORY_MB=2048

    if [[ -f manifest.json ]]; then
        SIZE_PER_EVENT_KB=$(python3 -c "import json; print(json.load(open('manifest.json')).get('size_per_event_kb', 50.0))")
        TIME_PER_EVENT_SEC=$(python3 -c "import json; print(json.load(open('manifest.json')).get('time_per_event_sec', 0.5))")
        MEMORY_MB=$(python3 -c "import json; print(json.load(open('manifest.json')).get('memory_mb', 2048))")
    fi

    # Calculate events to process
    EVENTS=$EVENTS_PER_JOB
    if [[ "$EVENTS" -le 0 ]]; then
        EVENTS=1000
    fi

    echo "events:         $EVENTS"
    echo "size/event(KB): $SIZE_PER_EVENT_KB"
    echo "time/event(s):  $TIME_PER_EVENT_SEC"

    # Calculate output size in KB (capped at 10 MB for dev/testing)
    MAX_OUTPUT_KB=10240
    OUTPUT_KB=$(python3 -c "print(min($MAX_OUTPUT_KB, int($EVENTS * $SIZE_PER_EVENT_KB)))")

    # Simulate processing time (capped at 10s for testing)
    SLEEP_TIME=$(python3 -c "print(min(10, $EVENTS * $TIME_PER_EVENT_SEC))")
    echo "output_kb:      $OUTPUT_KB (capped at ${MAX_OUTPUT_KB} KB)"
    echo "sleep_time:     ${SLEEP_TIME}s"
    sleep "$SLEEP_TIME"

    # Create synthetic output file
    OUTPUT_FILE="proc_${NODE_INDEX}_output.root"
    dd if=/dev/urandom of="$OUTPUT_FILE" bs=1024 count="$OUTPUT_KB" 2>/dev/null
    ACTUAL_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || echo 0)
    echo "output_file:    $OUTPUT_FILE ($ACTUAL_SIZE bytes)"

    # Write a simple output manifest for synthetic mode
    python3 -c "
import json, os
# Determine tier from output_info.json if available
tier = 'unknown'
if os.path.isfile('${OUTPUT_INFO:-}'):
    try:
        with open('${OUTPUT_INFO}') as f:
            info = json.load(f)
        datasets = info.get('output_datasets', [])
        if datasets:
            tier = datasets[0].get('data_tier', 'unknown')
    except Exception:
        pass
files = {}
for f in os.listdir('.'):
    if f.startswith('proc_') and f.endswith('.root'):
        files[f] = {'tier': tier, 'step_index': -1}
if files:
    with open('proc_${NODE_INDEX}_outputs.json', 'w') as fh:
        json.dump(files, fh, indent=2)
    print('Wrote proc_${NODE_INDEX}_outputs.json:', len(files), 'files')
" 2>/dev/null || true

    # Stage out to unmerged site storage
    stage_out_to_unmerged

    END_TIME=$(date +%s)
    WALL_TIME=$((END_TIME - START_TIME))

    echo ""
    echo "=== Synthetic mode complete ==="
    echo "wall_time_sec:      $WALL_TIME"
    echo "events_processed:   $EVENTS"
    echo "output_bytes:       $ACTUAL_SIZE"
}

# ── Pilot mode ────────────────────────────────────────────────
run_pilot_mode() {
    echo "--- Pilot mode ---"
    echo "Running iterative measurements..."

    EVENTS=$EVENTS_PER_JOB
    if [[ "$EVENTS" -le 0 ]]; then
        EVENTS=100
    fi

    PILOT_JSON="pilot_metrics.json"
    ITERATIONS="[]"

    for multiplier in 1 2 4; do
        iter_events=$((EVENTS * multiplier))
        echo ""
        echo "Pilot iteration: $iter_events events"
        ITER_START=$(date +%s)

        # Use the same mode dispatch but with modified event count
        EVENTS_PER_JOB=$iter_events

        if [[ "$MODE" == "cmssw" && -d /cvmfs/cms.cern.ch ]]; then
            run_cmssw_mode
        else
            run_synthetic_mode
        fi

        ITER_END=$(date +%s)
        ITER_WALL=$((ITER_END - ITER_START))

        # Collect output size
        ITER_OUTPUT_SIZE=0
        for f in *.root proc_*_output.root; do
            if [[ -f "$f" ]]; then
                sz=$(stat -c%s "$f" 2>/dev/null || echo 0)
                ITER_OUTPUT_SIZE=$((ITER_OUTPUT_SIZE + sz))
            fi
        done

        # Measure peak RSS (from /proc/self)
        PEAK_RSS=$(python3 -c "
try:
    with open('/proc/self/status') as f:
        for line in f:
            if line.startswith('VmHWM:'):
                print(int(line.split()[1]))
                break
except Exception:
    print(0)
")

        ITERATIONS=$(python3 -c "
import json
iters = json.loads('$ITERATIONS')
iters.append({
    'events': $iter_events,
    'wall_time_sec': $ITER_WALL,
    'output_bytes': $ITER_OUTPUT_SIZE,
    'peak_rss_kb': $PEAK_RSS,
})
print(json.dumps(iters))
")

        # Clean up for next iteration
        rm -f *.root proc_*_output.root
    done

    # Write pilot_metrics.json
    python3 -c "
import json

iterations = json.loads('$ITERATIONS')

# Calculate derived metrics from last iteration
last = iterations[-1] if iterations else {}
events = last.get('events', 1)
wall = last.get('wall_time_sec', 1) or 1
output_bytes = last.get('output_bytes', 0)
peak_rss = last.get('peak_rss_kb', 0)

metrics = {
    'events_per_second': events / wall,
    'time_per_event_sec': wall / events,
    'memory_peak_mb': peak_rss // 1024 if peak_rss else 2000,
    'output_size_per_event_kb': (output_bytes / 1024) / events if events else 50.0,
    'cpu_efficiency': 0.8,
    'iterations': iterations,
}
with open('$PILOT_JSON', 'w') as f:
    json.dump(metrics, f, indent=2)
print('Wrote:', '$PILOT_JSON')
"

    echo ""
    echo "=== Pilot mode complete ==="
}

# ── Main dispatch ─────────────────────────────────────────────
# Trigger CVMFS autofs mount if needed (ls triggers automount, -d alone may not)
ls /cvmfs/cms.cern.ch/ > /dev/null 2>&1 || true

if [[ "$PILOT_MODE" == "true" ]]; then
    run_pilot_mode
elif [[ "$MODE" == "cmssw" ]]; then
    if [[ ! -d /cvmfs/cms.cern.ch ]]; then
        echo "ERROR: CMSSW mode requested but /cvmfs/cms.cern.ch not available" >&2
        exit 1
    fi
    run_cmssw_mode
elif [[ "$MODE" == "simulator" ]]; then
    run_simulator_mode
elif [[ "$MODE" == "synthetic" ]]; then
    run_synthetic_mode
else
    echo "ERROR: Unknown mode: $MODE" >&2
    exit 1
fi
'''
    _write_file(path, _script.replace("__VENV_PYTHON__", _venv_python))
    os.chmod(path, 0o755)


def _write_merge_script(path: str) -> None:
    """Generate a merge script using cmsRun + mergeProcess() for ROOT files.

    Reads proc outputs from unmerged site storage (staged by proc jobs),
    writes merged output to merged site storage. Uses LFN→PFN mapping:
    PFN = local_pfn_prefix + LFN.

    For ROOT files: uses cmsRun with Configuration.DataProcessing.Merge.mergeProcess()
    to produce proper EDM output with provenance. NanoAOD tiers use NanoAODOutputModule,
    DQMIO tiers use DQMRootOutputModule, others use PoolOutputModule with fast cloning.
    Merged output respects a target size limit (max_merge_size from output_info.json).
    Falls back to hadd if CMSSW setup fails.
    For text files: concatenates proc_*.out into merged text output.
    """
    # Use /usr/bin/env python3 for portability (grid WNs don't have the venv).
    # Simulator merge (numpy/uproot) only works locally where the venv is available.
    _venv_python = sys.executable
    _script = '''#!/usr/bin/env python3
"""wms2_merge.py — WMS2 merge job.

Reads proc outputs from unmerged site storage, merges them, writes to merged
site storage. LFN→PFN: PFN = local_pfn_prefix + LFN.

Merges using cmsRun + mergeProcess() for proper EDM provenance.
- Standard EDM tiers: PoolOutputModule with fastCloning (default)
- NanoAOD tiers: NanoAODOutputModule
- DQMIO tiers: DQMRootOutputModule
- Text files (proc_*.out): concatenates into merged text output

Respects max_merge_size — produces multiple output files when needed.
Falls back to hadd if cmsRun/mergeProcess is unavailable.
"""
import glob
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import zlib
from datetime import datetime, timezone

# ── Argument parsing ──────────────────────────────────────────

output_info_path = None
sandbox_path = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--output-info" and i + 1 < len(sys.argv):
        output_info_path = sys.argv[i + 1]
        i += 2
    elif sys.argv[i] == "--sandbox" and i + 1 < len(sys.argv):
        sandbox_path = sys.argv[i + 1]
        i += 2
    else:
        i += 1

if not output_info_path:
    print("ERROR: --output-info not specified", file=sys.stderr)
    sys.exit(1)

with open(output_info_path) as f:
    info = json.load(f)

group_dir = os.path.dirname(output_info_path)
local_pfn_prefix = info.get("local_pfn_prefix", info.get("output_base_dir", "/mnt/shared"))
stageout_mode = info.get("stageout_mode", "local")
group_index = info["group_index"]
datasets = info.get("output_datasets", [])
max_merge_size = info.get("max_merge_size", 4 * 1024**3)
_glidein_site = os.environ.get("GLIDEIN_CMSSite", "")

# Grid mode: import stageout utility
stageout = None
siteconfig = None
grid_write_cmd = None
if stageout_mode == "grid":
    try:
        import wms2_stageout
        stageout = wms2_stageout
        # Resolve siteconf: prefer _siteconf copy, then GLIDEIN_CMSSite, then env, then CVMFS
        if os.path.isdir("_siteconf") and os.path.isfile("_siteconf/storage.json"):
            siteconfig = os.path.abspath("_siteconf")
        else:
            # GLIDEIN_CMSSite is passed via $$() in submit file — most reliable on grid
            _glidein_site = os.environ.get("GLIDEIN_CMSSite", "")
            if _glidein_site:
                _site_path = f"/cvmfs/cms.cern.ch/SITECONF/{_glidein_site}"
                if os.path.isfile(os.path.join(_site_path, "storage.json")):
                    siteconfig = _site_path
            if not siteconfig:
                siteconfig = os.environ.get("SITECONFIG_PATH", "")
            if not siteconfig or not os.path.isfile(os.path.join(siteconfig, "storage.json")):
                for _try in ["/opt/cms/siteconf", "/cvmfs/cms.cern.ch/SITECONF/local"]:
                    if os.path.isfile(os.path.join(_try, "storage.json")):
                        siteconfig = _try
                        break
        # Probe the write command once
        _probe_lfn = datasets[0].get("unmerged_lfn_base", "/store/test") + "/probe"
        _, grid_write_cmd = stageout.resolve_write_pfn(_probe_lfn, siteconfig)
        print(f"Grid stageout mode: command={grid_write_cmd}, siteconfig={siteconfig}")
    except Exception as e:
        print(f"WARNING: Grid stageout init failed ({e}), falling back to local", file=sys.stderr)
        stageout_mode = "local"


# ── Helpers ───────────────────────────────────────────────────

def lfn_to_pfn(pfn_prefix, lfn):
    """Convert LFN to local PFN by prepending site prefix."""
    return os.path.join(pfn_prefix, lfn.lstrip("/"))


CMSSW_ENV_PATH = "/cvmfs/cms.cern.ch/common/cmssw-env"


def cmssw_env_cmd(scram_arch, inner_cmd):
    """Build command list to run inner_cmd via cmssw-env.

    Returns a list suitable for subprocess.run().
    scram_arch may be a string or list (first element used).
    inner_cmd is a list of strings (the command to run inside the container).
    """
    if isinstance(scram_arch, list):
        scram_arch = scram_arch[0] if scram_arch else ""
    arch_os = scram_arch.split("_")[0]
    return [CMSSW_ENV_PATH, "--cmsos", arch_os, "--"] + list(inner_cmd)


def batch_by_size(files, max_size, file_sizes=None):
    """Group files into batches, each <= max_size total bytes.

    file_sizes: optional dict mapping filename to size (for remote files).
    """
    batches = []
    current_batch = []
    current_size = 0
    for f in files:
        if file_sizes and f in file_sizes:
            fsize = file_sizes[f]
        elif os.path.isfile(f):
            fsize = os.path.getsize(f)
        else:
            fsize = 0  # Unknown size for remote files; put all in one batch
        if current_batch and current_size + fsize > max_size:
            batches.append(current_batch)
            current_batch = [f]
            current_size = fsize
        else:
            current_batch.append(f)
            current_size += fsize
    if current_batch:
        batches.append(current_batch)
    return batches


def write_merge_pset(pset_path, input_files, output_file, is_nano=False, is_dqmio=False):
    """Write a cmsRun merge PSet using Configuration.DataProcessing.Merge.mergeProcess()."""
    lines = [
        "import FWCore.ParameterSet.Config as cms",
        "from Configuration.DataProcessing.Merge import mergeProcess",
        "process = mergeProcess(",
    ]
    for f in input_files:
        # root:// URLs are passed as-is (CMSSW reads XRootD natively)
        pfn = f if (f.startswith("file:") or f.startswith("root://")) else f"file:{f}"
        lines.append(f"    {pfn!r},")
    out_pfn = output_file if output_file.startswith("file:") else f"file:{output_file}"
    lines.append(f"    output_file={out_pfn!r},")
    if is_nano:
        lines.append("    mergeNANO=True,")
    if is_dqmio:
        lines.append("    newDQMIO=True,")
    lines.append(")")
    with open(pset_path, "w") as fh:
        fh.write("\\n".join(lines) + "\\n")


def _resolve_siteconf():
    """Find the siteconf directory on this host."""
    env_val = os.environ.get("SITECONFIG_PATH", "")
    if env_val and os.path.isdir(os.path.join(env_val, "JobConfig")):
        return env_val
    for candidate in ["/opt/cms/siteconf", "/cvmfs/cms.cern.ch/SITECONF/local"]:
        if os.path.isfile(os.path.join(candidate, "JobConfig", "site-local-config.xml")):
            return candidate
    return os.environ.get("SITECONFIG_PATH", "/opt/cms/siteconf")


def _prepare_siteconf(work_dir):
    """Copy site-local-config into work_dir/_siteconf for container access."""
    site_cfg = _resolve_siteconf()
    # Layout for SITECONFIG_PATH (CMSSW >=14.x)
    local_siteconf = os.path.join(work_dir, "_siteconf", "JobConfig")
    os.makedirs(local_siteconf, exist_ok=True)
    try:
        shutil.copy2(
            os.path.join(site_cfg, "JobConfig", "site-local-config.xml"),
            local_siteconf,
        )
    except Exception:
        pass
    phedex_src = os.path.join(site_cfg, "PhEDEx")
    phedex_dst = os.path.join(work_dir, "_siteconf", "PhEDEx")
    if os.path.isdir(phedex_src):
        if os.path.isdir(phedex_dst):
            shutil.rmtree(phedex_dst)
        shutil.copytree(phedex_src, phedex_dst)
    storage_json = os.path.join(site_cfg, "storage.json")
    if os.path.isfile(storage_json):
        shutil.copy2(storage_json, os.path.join(work_dir, "_siteconf"))
    # Layout for CMS_PATH (CMSSW <14.x): SITECONF/local/
    local_cms_jobconfig = os.path.join(
        work_dir, "_siteconf", "SITECONF", "local", "JobConfig"
    )
    os.makedirs(local_cms_jobconfig, exist_ok=True)
    src_slc = os.path.join(site_cfg, "JobConfig", "site-local-config.xml")
    if os.path.isfile(src_slc):
        shutil.copy2(src_slc, local_cms_jobconfig)
    local_cms_phedex = os.path.join(
        work_dir, "_siteconf", "SITECONF", "local", "PhEDEx"
    )
    if os.path.isdir(phedex_src):
        if os.path.isdir(local_cms_phedex):
            shutil.rmtree(local_cms_phedex)
        shutil.copytree(phedex_src, local_cms_phedex)


def run_cmsrun(pset_path, cmssw_version, scram_arch, work_dir):
    """Run cmsRun with a merge PSet via cmssw-env.

    Returns True on success, False on failure.
    """
    if isinstance(scram_arch, list):
        scram_arch = scram_arch[0] if scram_arch else ""

    _prepare_siteconf(work_dir)

    runner_path = os.path.join(work_dir, "_merge_runner.sh")
    with open(runner_path, "w") as f:
        f.write(f"""#!/bin/bash
set -e
export SITECONFIG_PATH={work_dir}/_siteconf
export CMS_PATH={work_dir}/_siteconf
export SCRAM_ARCH={scram_arch}
export X509_USER_PROXY={os.environ.get('X509_USER_PROXY', '')}
export X509_CERT_DIR={os.environ.get('X509_CERT_DIR', '/cvmfs/grid.cern.ch/etc/grid-security/certificates')}
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ ! -d {cmssw_version}/src ]]; then
    scramv1 project CMSSW {cmssw_version}
fi
cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && cd {work_dir}
cmsRun {pset_path}
""")
    os.chmod(runner_path, 0o755)

    cmd = cmssw_env_cmd(scram_arch, ["bash", runner_path])

    print(f"  Running: cmsRun {os.path.basename(pset_path)} (CMSSW {cmssw_version}, {scram_arch})")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=7200)

    if result.stdout:
        for line in result.stdout.strip().split("\\n")[-10:]:
            print(f"  [cmsRun] {line}")
    if result.returncode != 0:
        print(f"  ERROR: cmsRun failed (exit {result.returncode})", file=sys.stderr)
        if result.stderr:
            for line in result.stderr.strip().split("\\n")[-20:]:
                print(f"  [stderr] {line}", file=sys.stderr)
        return False

    return True


def merge_root_with_hadd(root_files, out_file, cmssw_version, scram_arch):
    """Fallback: merge ROOT files using hadd from CMSSW environment.

    Runs hadd inside the full CMSSW runtime environment via cmssw-env
    so that all shared libraries (libtbb, ROOT, etc.) are available.
    """
    if isinstance(scram_arch, list):
        scram_arch = scram_arch[0] if scram_arch else ""
    file_args = " ".join(shlex.quote(f) for f in root_files)
    hadd_script = (
        f"source /cvmfs/cms.cern.ch/cmsset_default.sh && "
        f"export SCRAM_ARCH={scram_arch} && "
        f"if [[ ! -d {cmssw_version}/src ]]; then "
        f"scramv1 project CMSSW {cmssw_version}; fi && "
        f"cd {cmssw_version}/src && eval $(scramv1 runtime -sh) && cd - >/dev/null && "
        f"hadd -f {shlex.quote(out_file)} {file_args}"
    )

    cmd = cmssw_env_cmd(scram_arch, ["bash", "-c", hadd_script])

    print(f"  Fallback: hadd -f {out_file} ({len(root_files)} inputs)")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=3600)

    if result.stdout:
        for line in result.stdout.strip().split("\\n")[-5:]:
            print(f"  [hadd] {line}")
    if result.returncode != 0:
        print(f"  ERROR: hadd failed (exit {result.returncode})", file=sys.stderr)
        if result.stderr:
            for line in result.stderr.strip().split("\\n")[-10:]:
                print(f"  [stderr] {line}", file=sys.stderr)
        return False
    return True


def merge_root_with_uproot(root_files, out_file):
    """Merge ROOT files using uproot — for simulator mode when CVMFS unavailable."""
    try:
        import uproot
        import numpy as np
    except ImportError:
        print("  ERROR: uproot/numpy not available for merge", file=sys.stderr)
        return False

    all_data = {}
    for rf in root_files:
        try:
            with uproot.open(rf) as f:
                tree = f["Events"]
                # Read all fields as awkward arrays, then convert each to numpy
                # (tree-level arrays(library="np") fails on 2D fields in RNTuple)
                ak_arrays = tree.arrays()
                for key in tree.keys():
                    all_data.setdefault(key, []).append(
                        np.asarray(ak_arrays[key])
                    )
        except Exception as e:
            print(f"  WARNING: Failed to read {rf}: {e}", file=sys.stderr)
            return False

    if not all_data:
        print("  WARNING: No data read from ROOT files", file=sys.stderr)
        return False

    merged = {}
    for k, v in all_data.items():
        try:
            merged[k] = np.concatenate(v)
        except ValueError:
            # Skip fields with incompatible shapes (e.g. padding with
            # different column counts across files)
            pass
    with uproot.recreate(out_file) as f:
        f["Events"] = merged
    return True


def merge_root_tier(tier, tier_files, tier_step_index, out_dir, manifest, work_dir):
    """Merge ROOT files for a single tier using cmsRun + mergeProcess().

    Batches files by max_merge_size and produces multiple outputs if needed.
    Falls back to hadd if cmsRun fails, then to uproot merge for simulator mode.
    """
    # Determine CMSSW version/arch for this tier's step
    steps = manifest.get("steps", [])
    cmssw_version = ""
    scram_arch = ""
    if 0 <= tier_step_index < len(steps):
        cmssw_version = steps[tier_step_index].get("cmssw_version", "")
        scram_arch = steps[tier_step_index].get("scram_arch", "")
    # Fallback to last keep_output step or top-level manifest fields
    if not cmssw_version:
        for s in reversed(steps):
            if s.get("keep_output", False):
                cmssw_version = s.get("cmssw_version", "")
                scram_arch = s.get("scram_arch", "")
                if cmssw_version:
                    break
    if not cmssw_version:
        cmssw_version = manifest.get("cmssw_version", "")
        scram_arch = manifest.get("scram_arch", "")

    is_nano = "NANO" in tier.upper()
    is_dqmio = "DQMIO" in tier.upper()

    # NANOEDMAODSIM → NANOAODSIM: mergeNANO=True converts EDM nano to flat nano
    output_tier = tier
    if is_nano and "EDM" in tier.upper():
        output_tier = tier.replace("EDM", "").replace("edm", "")

    # Check if cmsRun merge is possible
    has_cvmfs = os.path.isdir("/cvmfs/cms.cern.ch")
    can_cmsrun = bool(cmssw_version) and has_cvmfs

    # Batch files by target merge size
    batches = batch_by_size(tier_files, max_merge_size)
    print(f"  {len(batches)} merge batch(es) for tier {tier} "
          f"(max_merge_size={max_merge_size / (1024**3):.1f} GB)")

    merged_files = []
    for batch_idx, batch in enumerate(batches):
        suffix = f"_{batch_idx}" if len(batches) > 1 else ""
        out_file = os.path.join(out_dir, f"merged_{output_tier}{suffix}.root")

        if len(batch) == 1:
            # Single file — just copy/download, no merge needed
            src = batch[0]
            if src.startswith("root://") or src.startswith("davs://"):
                # Remote file: download via stageout utility
                stageout.download_file(src, out_file, grid_write_cmd)
            else:
                shutil.copy2(src, out_file)
            size = os.path.getsize(out_file)
            print(f"  Batch {batch_idx}: single file copied -> {out_file} ({size} bytes)")
            merged_files.append(out_file)
            continue

        print(f"  Batch {batch_idx}: {len(batch)} files")

        if can_cmsrun:
            pset_path = os.path.join(work_dir, f"merge_cfg_{tier}_{batch_idx}.py")
            write_merge_pset(pset_path, batch, out_file, is_nano=is_nano, is_dqmio=is_dqmio)

            success = run_cmsrun(pset_path, cmssw_version, scram_arch, work_dir)
            if success and os.path.isfile(out_file):
                size = os.path.getsize(out_file)
                print(f"  Batch {batch_idx}: cmsRun merge -> {out_file} ({size} bytes)")
                merged_files.append(out_file)
                continue
            else:
                print(f"  WARNING: cmsRun merge failed for batch {batch_idx}, trying hadd fallback")

        # Fallback to hadd
        if cmssw_version and has_cvmfs:
            success = merge_root_with_hadd(batch, out_file, cmssw_version, scram_arch)
            if success and os.path.isfile(out_file):
                size = os.path.getsize(out_file)
                print(f"  Batch {batch_idx}: hadd fallback -> {out_file} ({size} bytes)")
                merged_files.append(out_file)
                continue

        # Uproot merge fallback for simulator mode
        if manifest.get("mode") == "simulator":
            success = merge_root_with_uproot(batch, out_file)
            if success and os.path.isfile(out_file):
                size = os.path.getsize(out_file)
                print(f"  Batch {batch_idx}: uproot merge -> {out_file} ({size} bytes)")
                merged_files.append(out_file)
                continue

        # Last resort: copy files individually
        print(f"  WARNING: No merge method available, copying {len(batch)} files to {out_dir}")
        for rf in batch:
            dest = os.path.join(out_dir, os.path.basename(rf))
            shutil.copy2(rf, dest)
            print(f"    Copied: {dest}")
            merged_files.append(dest)

    return merged_files


def merge_text_files(datasets, text_files, pfn_prefix, group_index):
    """Merge text files — write to merged LFN paths."""
    proc_entries = []
    for pf in text_files:
        node_name = os.path.splitext(os.path.basename(pf))[0]
        with open(pf) as f:
            content = f.read().strip()
        if content:
            proc_entries.append(f"{node_name} | {content}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for ds in datasets:
        merged_base = ds.get("merged_lfn_base", "")
        tier = ds.get("data_tier", "unknown")
        ds_name = ds.get("dataset_name", "")

        merged_lfn_dir = f"{merged_base}/{group_index:06d}"
        out_dir = lfn_to_pfn(pfn_prefix, merged_lfn_dir)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, "merged.txt")

        with open(out_file, "w") as fout:
            header = [
                "# WMS2 Merged Output",
                f"# Dataset:     {ds_name}",
                f"# Data tier:   {tier}",
                f"# Merge group: mg_{group_index:06d}",
                f"# Generated:   {now}",
                f"# Host:        {os.uname().nodename}",
                f"# Jobs merged: {len(proc_entries)}",
                "#",
            ]
            for line in header:
                fout.write(line + "\\n")
            for entry in proc_entries:
                fout.write(entry + "\\n")

        print(f"Wrote: {out_file} ({len(proc_entries)} entries)")


# ── Main logic ────────────────────────────────────────────────

# Read proc output manifests and ROOT files from unmerged site storage
# Each dataset has an unmerged_lfn_base; files are at PFN = pfn_prefix + unmerged_lfn/{group:06d}/
unmerged_root_files = []  # local paths or root:// URLs
unmerged_manifests = []
unmerged_metrics = []
has_unmerged = any(ds.get("unmerged_lfn_base") for ds in datasets)

if has_unmerged and stageout_mode == "grid" and stageout is not None:
    # Grid mode: discover files via remote listing, download JSON, resolve ROOT file URLs
    proc_node_indices = info.get("proc_node_indices", [])
    for ds in datasets:
        unmerged_base = ds.get("unmerged_lfn_base", "")
        if not unmerged_base:
            continue
        unmerged_lfn_dir = f"{unmerged_base}/{group_index:06d}"
        write_pfn, write_cmd = stageout.resolve_write_pfn(unmerged_lfn_dir + "/probe", siteconfig)
        write_pfn_dir = os.path.dirname(write_pfn)
        print(f"Grid listing: {write_pfn_dir} (cmd={write_cmd})")
        entries = []
        try:
            entries = stageout.list_dir(write_pfn_dir, write_cmd)
        except Exception as e:
            print(f"WARNING: Grid list failed for {write_pfn_dir}: {e}")

        # Fallback: if listing returned empty but we know proc indices,
        # probe for expected files directly (works around gfal-ls failures)
        if not entries and proc_node_indices:
            print(f"  Grid listing empty, probing {len(proc_node_indices)} proc outputs...")
            for ni in proc_node_indices:
                # Try to download the outputs manifest for this proc node
                manifest_name = f"proc_{ni}_outputs.json"
                lfn = f"{unmerged_lfn_dir}/{manifest_name}"
                local = os.path.join(os.getcwd(), manifest_name)
                try:
                    pfn, cmd = stageout.resolve_write_pfn(lfn, siteconfig)
                    stageout.download_file(pfn, local, cmd)
                    unmerged_manifests.append(local)
                    # Parse the manifest to discover ROOT file names
                    with open(local) as _mf:
                        _manifest_data = json.load(_mf)
                    for fname in _manifest_data:
                        if fname.endswith(".root"):
                            entries.append(fname)
                except Exception as _e:
                    print(f"  Probe failed for proc_{ni}: {_e}")
                # Also try metrics
                for suffix in ("_metrics.json", "_cgroup.json"):
                    aux_name = f"proc_{ni}{suffix}"
                    aux_lfn = f"{unmerged_lfn_dir}/{aux_name}"
                    aux_local = os.path.join(os.getcwd(), aux_name)
                    try:
                        pfn, cmd = stageout.resolve_write_pfn(aux_lfn, siteconfig)
                        stageout.download_file(pfn, aux_local, cmd)
                        if suffix == "_metrics.json":
                            unmerged_metrics.append(aux_local)
                    except Exception:
                        pass
            if entries:
                print(f"  Probe discovered {len(entries)} ROOT files")
        else:
            # Download JSON files (manifests, metrics) to local working dir
            for entry in entries:
                if entry.endswith("_outputs.json"):
                    lfn = f"{unmerged_lfn_dir}/{entry}"
                    pfn, cmd = stageout.resolve_write_pfn(lfn, siteconfig)
                    local = os.path.join(os.getcwd(), entry)
                    stageout.download_file(pfn, local, cmd)
                    unmerged_manifests.append(local)
                elif entry.endswith("_metrics.json") or entry.endswith("_cgroup.json"):
                    lfn = f"{unmerged_lfn_dir}/{entry}"
                    pfn, cmd = stageout.resolve_write_pfn(lfn, siteconfig)
                    local = os.path.join(os.getcwd(), entry)
                    stageout.download_file(pfn, local, cmd)
                    if entry.endswith("_metrics.json"):
                        unmerged_metrics.append(local)

        # Resolve ROOT file read URLs (prefer XRootD for direct CMSSW access)
        for entry in entries:
            if entry.endswith(".root"):
                lfn = f"{unmerged_lfn_dir}/{entry}"
                try:
                    read_pfn, is_direct = stageout.resolve_read_pfn(lfn, siteconfig)
                    if is_direct:
                        # CMSSW can read root:// directly — no download needed
                        unmerged_root_files.append(read_pfn)
                    else:
                        # Download to local temp dir
                        local = os.path.join(os.getcwd(), entry)
                        stageout.download_file(read_pfn, local, grid_write_cmd)
                        unmerged_root_files.append(local)
                except Exception as e:
                    print(f"WARNING: Could not resolve read PFN for {lfn}: {e}")

elif has_unmerged:
    # Local mode: read from unmerged site storage via filesystem
    for ds in datasets:
        unmerged_base = ds.get("unmerged_lfn_base", "")
        if not unmerged_base:
            continue
        unmerged_dir = lfn_to_pfn(local_pfn_prefix, f"{unmerged_base}/{group_index:06d}")
        if not os.path.isdir(unmerged_dir):
            print(f"WARNING: Unmerged dir not found: {unmerged_dir}")
            continue
        # Collect ROOT files
        for f in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_*.root"))):
            unmerged_root_files.append(f)
        if not unmerged_root_files:
            for f in sorted(glob.glob(os.path.join(unmerged_dir, "*.root"))):
                unmerged_root_files.append(f)
        # Collect output manifests
        for mf in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_outputs.json"))):
            unmerged_manifests.append(mf)
        # Collect metrics files
        for mf in sorted(glob.glob(os.path.join(unmerged_dir, "proc_*_metrics.json"))):
            unmerged_metrics.append(mf)

# Fallback: read from group dir (backward compat with old format)
if not unmerged_root_files:
    unmerged_root_files = sorted(glob.glob(os.path.join(group_dir, "proc_*_*.root")))
    if not unmerged_root_files:
        unmerged_root_files = sorted(glob.glob(os.path.join(group_dir, "*.root")))
    unmerged_manifests = sorted(glob.glob(os.path.join(group_dir, "proc_*_outputs.json")))
    unmerged_metrics = sorted(glob.glob(os.path.join(group_dir, "proc_*_metrics.json")))

# Also check group dir for text files (transferred by HTCondor)
text_files = sorted(glob.glob(os.path.join(group_dir, "proc_*.out")))

root_files = unmerged_root_files

if root_files:
    print(f"Detected {len(root_files)} ROOT file(s) — using cmsRun merge mode")

    # Load manifest for CMSSW version info
    manifest = {}
    manifest_path = os.path.join(group_dir, "manifest.json")
    if os.path.isfile(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)

    # Build file-to-info mapping from proc output manifests
    # Supports both new format {"file": {"tier": "X", "step_index": N}}
    # and old format {"file": "TIER"} for backward compatibility
    file_info_map = {}  # filename -> {"tier": str, "step_index": int}
    for mf in unmerged_manifests:
        try:
            with open(mf) as f:
                raw = json.load(f)
            for fname, val in raw.items():
                if isinstance(val, dict):
                    file_info_map[fname] = val
                else:
                    # Old format: val is just the tier string
                    file_info_map[fname] = {"tier": val, "step_index": -1}
        except Exception:
            pass

    # Group files by tier
    tier_to_files = {}  # tier -> [paths]
    tier_to_step = {}   # tier -> step_index (first seen)
    for f in root_files:
        basename = os.path.basename(f)
        finfo = file_info_map.get(basename)
        if finfo:
            tier = finfo["tier"]
            step_idx = finfo.get("step_index", -1)
        else:
            # Fallback: extract from filename pattern proc_NNN_stepN_TIER.root
            m = re.match(r'proc_\\d+_step(\\d+)_(\\w+)\\.root', basename)
            if m:
                tier = m.group(2)
                step_idx = int(m.group(1)) - 1
            else:
                tier = "unknown"
                step_idx = -1
        tier_to_files.setdefault(tier, []).append(f)
        if tier not in tier_to_step:
            tier_to_step[tier] = step_idx

    print(f"Tier groups: { {k: len(v) for k, v in tier_to_files.items()} }")

    work_dir = os.getcwd()

    # Track unmerged directories for cleanup
    cleanup_dirs = set()

    for ds in datasets:
        merged_base = ds.get("merged_lfn_base", "")
        unmerged_base = ds.get("unmerged_lfn_base", "")
        ds_tier = ds.get("data_tier", "unknown")

        # Match dataset tier to discovered file tiers
        tier_files = tier_to_files.get(ds_tier, [])
        step_idx = tier_to_step.get(ds_tier, -1)
        if not tier_files:
            # Try EDM variant + strip numeric suffix:
            # NANOEDMAODSIM1 -> NANOAODSIM matches NANOAODSIM
            import re as _re
            ds_norm = _re.sub(r'\\d+$', '', ds_tier.upper())
            for tier, files in tier_to_files.items():
                tier_norm = _re.sub(r'(EDM)?(\\d+)$', '', tier.upper())
                if tier_norm == ds_norm:
                    tier_files = files
                    step_idx = tier_to_step.get(tier, -1)
                    break
        if not tier_files:
            # Fallback: longer tier name must contain shorter one
            # (match MINIAODSIM->MINIAODSIM but not AODSIM->NANOAODSIM)
            for tier, files in tier_to_files.items():
                t_up, d_up = tier.upper(), ds_tier.upper()
                longer, shorter = (t_up, d_up) if len(t_up) >= len(d_up) else (d_up, t_up)
                if shorter in longer and len(longer) - len(shorter) <= 3:
                    tier_files = files
                    step_idx = tier_to_step.get(tier, -1)
                    break

        if not tier_files:
            print(f"WARNING: No ROOT files matching tier {ds_tier}, skipping")
            continue

        # Merged output goes to merged site storage via LFN→PFN
        merged_lfn_dir = f"{merged_base}/{group_index:06d}"

        if stageout_mode == "grid" and stageout is not None:
            # Grid mode: write to local temp dir, then upload
            out_dir = os.path.join(work_dir, f"_merged_{ds_tier}")
            os.makedirs(out_dir, exist_ok=True)
            print(f"Tier {ds_tier}: {len(tier_files)} files to merge (step_index={step_idx})")
            print(f"  Local output: {out_dir} (will upload to grid)")

            if manifest.get("mode") in ("cmssw", "simulator"):
                merge_root_tier(ds_tier, tier_files, step_idx, out_dir, manifest, work_dir)
            else:
                print(f"  Non-CMSSW mode: downloading {len(tier_files)} ROOT files to {out_dir}")
                for rf in tier_files:
                    dest = os.path.join(out_dir, os.path.basename(rf))
                    if rf.startswith("root://") or rf.startswith("davs://"):
                        stageout.download_file(rf, dest, grid_write_cmd)
                    elif os.path.isfile(rf):
                        shutil.copy2(rf, dest)
                    print(f"    Fetched: {dest}")

            # Upload merged files to grid
            print(f"  Uploading merged output to grid...")
            for fname in sorted(os.listdir(out_dir)):
                local_file = os.path.join(out_dir, fname)
                if os.path.isfile(local_file):
                    lfn = f"{merged_lfn_dir}/{fname}"
                    pfn, cmd = stageout.resolve_write_pfn(lfn, siteconfig)
                    pfn_dir = os.path.dirname(pfn)
                    stageout.mkdir_p(pfn_dir, cmd)
                    stageout.upload_file(local_file, pfn, cmd)
        else:
            # Local mode: write directly to local PFN path
            out_dir = lfn_to_pfn(local_pfn_prefix, merged_lfn_dir)
            print(f"Tier {ds_tier}: {len(tier_files)} files to merge (step_index={step_idx})")
            print(f"  Output: {out_dir}")
            os.makedirs(out_dir, exist_ok=True)

            if manifest.get("mode") in ("cmssw", "simulator"):
                merge_root_tier(ds_tier, tier_files, step_idx, out_dir, manifest, work_dir)
            else:
                # Synthetic mode: just copy files
                print(f"  Non-CMSSW mode: copying {len(tier_files)} ROOT files to {out_dir}")
                for rf in tier_files:
                    dest = os.path.join(out_dir, os.path.basename(rf))
                    shutil.copy2(rf, dest)
                    print(f"    Copied: {dest}")

        # Track unmerged dir for cleanup (store LFN paths for portability)
        if unmerged_base:
            cleanup_dirs.add(f"{unmerged_base}/{group_index:06d}")

    # Write cleanup_manifest.json for the cleanup job (always, even if empty,
    # because cleanup.sub references it in transfer_input_files)
    cleanup_manifest = {
        "unmerged_dirs": sorted(cleanup_dirs),
        "stageout_mode": stageout_mode,
    }
    cleanup_path = os.path.join(group_dir, "cleanup_manifest.json")
    with open(cleanup_path, "w") as f:
        json.dump(cleanup_manifest, f, indent=2)
    print(f"Wrote cleanup manifest: {cleanup_path} ({len(cleanup_dirs)} dirs)")

    # Aggregate per-step resource metrics from proc jobs
    if unmerged_metrics:
        print(f"\\nAggregating metrics from {len(unmerged_metrics)} proc job(s)...")
        all_proc_metrics = []
        for mf in unmerged_metrics:
            try:
                with open(mf) as f:
                    all_proc_metrics.append(json.load(f))
            except Exception as e:
                print(f"  WARNING: Failed to read {mf}: {e}")

        if all_proc_metrics:
            # Group by step number
            step_data = {}  # step_num -> {metric_name -> [values]}
            for proc in all_proc_metrics:
                for step_m in proc:
                    step_num = step_m.get("step", 0)
                    if step_num not in step_data:
                        step_data[step_num] = {}
                    for key, val in step_m.items():
                        if key == "step":
                            continue
                        if val is not None:
                            step_data[step_num].setdefault(key, []).append(val)

            # Compute aggregates
            per_step = {}
            count_metrics = {"events_processed", "events_written"}  # use total instead of mean
            for step_num, metrics in sorted(step_data.items()):
                agg = {"step": step_num, "num_jobs": len(all_proc_metrics)}
                for key, values in metrics.items():
                    if not values:
                        continue
                    if key in count_metrics:
                        agg[key] = {
                            "min": min(values),
                            "max": max(values),
                            "total": sum(values),
                        }
                    else:
                        agg[key] = {
                            "min": round(min(values), 4),
                            "max": round(max(values), 4),
                            "mean": round(sum(values) / len(values), 4),
                        }
                per_step[str(step_num)] = agg

            work_unit = {
                "group_index": group_index,
                "num_proc_jobs": len(all_proc_metrics),
                "per_step": per_step,
            }
            wu_path = os.path.join(group_dir, "work_unit_metrics.json")
            with open(wu_path, "w") as f:
                json.dump(work_unit, f, indent=2)
            print(f"Wrote work_unit_metrics.json: {len(per_step)} step(s), {len(all_proc_metrics)} jobs")

    # Write merge_output.json for DAGMonitor → OutputManager bridge
    merge_output = {
        "group_index": group_index,
        "site": _glidein_site if _glidein_site else "local",
        "datasets": {},
    }
    for ds in datasets:
        tier = ds.get("data_tier", "")
        merged_base = ds.get("merged_lfn_base", "")
        ds_name = ds.get("dataset_name", "")
        # In grid mode, merged files are in local temp dir; in local mode, at PFN path
        if stageout_mode == "grid":
            merged_dir = os.path.join(work_dir, f"_merged_{tier}")
        else:
            merged_dir = lfn_to_pfn(local_pfn_prefix, f"{merged_base}/{group_index:06d}")
        if os.path.isdir(merged_dir):
            files = []
            for f in sorted(os.listdir(merged_dir)):
                fp = os.path.join(merged_dir, f)
                if os.path.isfile(fp):
                    # Compute adler32 checksum for Rucio registration
                    cksum = 1  # adler32 initial value
                    with open(fp, "rb") as cf:
                        while True:
                            chunk = cf.read(1024 * 1024)
                            if not chunk:
                                break
                            cksum = zlib.adler32(chunk, cksum) & 0xFFFFFFFF
                    files.append({
                        "lfn": f"{merged_base}/{group_index:06d}/{f}",
                        "size": os.path.getsize(fp),
                        "adler32": "%08x" % cksum,
                    })
            merge_output["datasets"][ds_name] = {
                "data_tier": tier,
                "files": files,
            }
    mo_path = os.path.join(group_dir, "merge_output.json")
    with open(mo_path, "w") as f:
        json.dump(merge_output, f, indent=2)
    print(f"Wrote merge_output.json: {len(merge_output['datasets'])} dataset(s)")

else:
    if text_files:
        print(f"Detected {len(text_files)} text file(s) — using text merge mode")
        merge_text_files(datasets, text_files, local_pfn_prefix, group_index)
    else:
        print("WARNING: No ROOT files and no text files found — nothing to merge")
        sys.exit(1)
'''
    _write_file(path, _script)
    os.chmod(path, 0o755)


def _write_cleanup_script(path: str) -> None:
    """Generate a cleanup script that removes unmerged files from site storage."""
    _write_file(path, '''#!/usr/bin/env python3
"""wms2_cleanup.py — WMS2 cleanup job.

Reads cleanup_manifest.json (written by the merge job) and removes
unmerged directories from site storage. Supports both local (filesystem)
and grid (xrdcp/gfal-copy) modes.

Cleanup manifest paths may be:
- LFN paths (/store/...) — resolved via local_pfn_prefix or grid stageout
- Legacy absolute PFN paths (/mnt/shared/...) — used directly (backward compat)
"""
import json
import os
import shutil
import sys

# ── Argument parsing ──────────────────────────────────────────

output_info_path = None
i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--output-info" and i + 1 < len(sys.argv):
        output_info_path = sys.argv[i + 1]
        i += 2
    else:
        i += 1

# Load output_info.json for stageout config
local_pfn_prefix = "/mnt/shared"
stageout_mode = "local"
if output_info_path and os.path.isfile(output_info_path):
    with open(output_info_path) as f:
        output_info = json.load(f)
    local_pfn_prefix = output_info.get("local_pfn_prefix", "/mnt/shared")
    stageout_mode = output_info.get("stageout_mode", "local")

# Find cleanup_manifest.json in the group dir
cleanup_manifest_path = None
if output_info_path and os.path.isfile(output_info_path):
    group_dir = os.path.dirname(output_info_path)
    candidate = os.path.join(group_dir, "cleanup_manifest.json")
    if os.path.isfile(candidate):
        cleanup_manifest_path = candidate

# Also check current working directory
if not cleanup_manifest_path and os.path.isfile("cleanup_manifest.json"):
    cleanup_manifest_path = "cleanup_manifest.json"

if not cleanup_manifest_path:
    print("No cleanup_manifest.json found — nothing to clean up")
    sys.exit(0)

with open(cleanup_manifest_path) as f:
    manifest = json.load(f)

unmerged_dirs = manifest.get("unmerged_dirs", [])
# Override stageout_mode from manifest if present (written by merge)
stageout_mode = manifest.get("stageout_mode", stageout_mode)

if not unmerged_dirs:
    print("No unmerged directories to clean up")
    sys.exit(0)

# Grid mode: import stageout utility
stageout = None
siteconfig = None
if stageout_mode == "grid":
    try:
        import wms2_stageout
        stageout = wms2_stageout
        # Resolve siteconf: prefer _siteconf copy, then GLIDEIN_CMSSite, then env, then CVMFS
        if os.path.isdir("_siteconf") and os.path.isfile("_siteconf/storage.json"):
            siteconfig = os.path.abspath("_siteconf")
        else:
            _glidein_site = os.environ.get("GLIDEIN_CMSSite", "")
            if _glidein_site:
                _site_path = f"/cvmfs/cms.cern.ch/SITECONF/{_glidein_site}"
                if os.path.isfile(os.path.join(_site_path, "storage.json")):
                    siteconfig = _site_path
            if not siteconfig:
                siteconfig = os.environ.get("SITECONFIG_PATH", "")
            if not siteconfig or not os.path.isfile(os.path.join(siteconfig, "storage.json")):
                for _try in ["/opt/cms/siteconf", "/cvmfs/cms.cern.ch/SITECONF/local"]:
                    if os.path.isfile(os.path.join(_try, "storage.json")):
                        siteconfig = _try
                        break
        print(f"Grid cleanup mode: siteconfig={siteconfig}")
    except Exception as e:
        print(f"WARNING: Grid stageout init failed ({e}), falling back to local", file=sys.stderr)
        stageout_mode = "local"


def is_lfn(path):
    """Check if path is an LFN (starts with /store/)."""
    return path.startswith("/store/")


print(f"Cleaning up {len(unmerged_dirs)} unmerged directories (mode={stageout_mode})...")
for d in unmerged_dirs:
    if stageout_mode == "grid" and stageout is not None and is_lfn(d):
        # Grid mode: resolve LFN → PFN and remove remotely
        try:
            pfn, cmd = stageout.resolve_write_pfn(d + "/probe", siteconfig)
            pfn_dir = os.path.dirname(pfn)
            stageout.remove_path(pfn_dir, cmd, recursive=True)
            print(f"  Removed (grid): {pfn_dir}")
        except Exception as e:
            print(f"  WARNING: Grid cleanup failed for {d}: {e}")
    elif is_lfn(d):
        # Local mode with LFN path: resolve to local PFN
        local_dir = os.path.join(local_pfn_prefix, d.lstrip("/"))
        if os.path.isdir(local_dir):
            shutil.rmtree(local_dir)
            print(f"  Removed: {local_dir}")
        else:
            print(f"  Already gone: {local_dir}")
    else:
        # Legacy absolute PFN path (backward compat)
        if os.path.isdir(d):
            shutil.rmtree(d)
            print(f"  Removed: {d}")
        else:
            print(f"  Already gone: {d}")

print("Cleanup complete")
''')
    os.chmod(path, 0o755)


def _write_stageout_utility(path: str) -> None:
    """Generate wms2_stageout.py — self-contained stageout utility for worker nodes.

    This module is transferred to worker nodes and provides:
    - CMS storage.json parsing (prefix and rules formats)
    - site-local-config.xml <stage-out> parsing
    - LFN→PFN resolution for read and write protocols
    - File operations (upload/download/list/remove/mkdir) via xrdcp/gfal-copy/cp
    """
    _write_file(path, '''#!/usr/bin/env python3
"""wms2_stageout.py — Self-contained stageout utility for WMS2 worker nodes.

Resolves LFN→PFN using CMS storage.json and site-local-config.xml, then performs
file operations via the appropriate protocol command (xrdcp, gfal-copy, or cp).

No WMS2 imports — stdlib only. Transferred to worker nodes by DAG planner.
"""
import json
import os
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


# ── Storage.json parsing ──────────────────────────────────────

def parse_storage_json(path):
    """Parse CMS storage.json. Returns list of volume dicts.

    Each volume has: site, volume, protocols, type, rse.
    Each protocol has: protocol, access, prefix (and optionally rules/chain).
    """
    with open(path) as f:
        return json.load(f)


def _resolve_prefix_format(lfn, prefix):
    """Resolve LFN using simple prefix format: PFN = prefix + LFN."""
    # Prefix may or may not end with /; LFN starts with /store/...
    if prefix.endswith("/"):
        return prefix + lfn.lstrip("/")
    return prefix + lfn


def _resolve_rules_format(lfn, rules, all_protocols=None):
    """Resolve LFN using regex rules format (FNAL-style).

    rules is a list of {"lfn": regex, "pfn": replacement, "chain": optional} dicts.
    Replacement uses $1, $2, etc. for capture groups (CMS convention).
    all_protocols: full protocol list from same volume (for chain resolution).
    """
    for rule in rules:
        pattern = rule.get("lfn", "")
        replacement = rule.get("pfn", "")
        chain_target = rule.get("chain")
        if not pattern:
            continue

        # If this rule has a chain, first resolve through the chain target
        input_path = lfn
        if chain_target and all_protocols:
            chained = _resolve_via_chain(lfn, chain_target, all_protocols)
            if chained:
                input_path = chained
            else:
                continue  # Chain resolution failed, skip this rule

        m = re.match(pattern, input_path)
        if m:
            result = replacement
            groups = m.groups()
            for i, g in enumerate(groups, 1):
                result = result.replace(f"${i}", g)
            return result
    return None


def _resolve_via_chain(lfn, chain_protocol, all_protocols):
    """Resolve LFN through a chain target protocol (e.g., 'pnfs' virtual protocol)."""
    for proto in all_protocols:
        if proto.get("protocol") == chain_protocol:
            if "rules" in proto:
                return _resolve_rules_format(lfn, proto["rules"])
            elif "prefix" in proto:
                return _resolve_prefix_format(lfn, proto["prefix"])
    return None


def resolve_lfn_to_pfn(lfn, protocol_entry, all_protocols=None):
    """Resolve LFN→PFN using a single protocol entry from storage.json.

    Handles 'prefix' format (simple concatenation), 'rules' format (regex-based),
    and chained rules. Returns PFN string or None if no match.
    all_protocols: full protocol list from same volume (needed for chain resolution).
    """
    if "prefix" in protocol_entry:
        return _resolve_prefix_format(lfn, protocol_entry["prefix"])
    if "rules" in protocol_entry:
        return _resolve_rules_format(lfn, protocol_entry["rules"], all_protocols)
    return None


# ── Site-local-config.xml parsing ─────────────────────────────

def parse_stageout_config(slc_path):
    """Parse <stage-out> section from site-local-config.xml.

    Returns list of dicts: [{volume, protocol, command}, ...]
    """
    try:
        tree = ET.parse(slc_path)
        methods = []
        for method in tree.findall(".//stage-out/method"):
            protocol = method.get("protocol", "")
            # Default command: gfal-copy for WebDAV, xrdcp for XRootD
            default_cmd = "gfal-copy" if "WebDAV" in protocol else "xrdcp"
            methods.append({
                "volume": method.get("volume", ""),
                "protocol": protocol,
                "command": method.get("command", default_cmd),
            })
        return methods
    except Exception:
        return []


# ── High-level PFN resolution ─────────────────────────────────

def _find_siteconf_files(siteconfig_path):
    """Locate storage.json and site-local-config.xml from SITECONFIG_PATH."""
    storage_json = os.path.join(siteconfig_path, "storage.json")
    slc = os.path.join(siteconfig_path, "JobConfig", "site-local-config.xml")
    return storage_json, slc


def resolve_write_pfn(lfn, siteconfig_path):
    """Resolve LFN to write PFN using site-local-config <stage-out> + storage.json.

    Returns (pfn, command) tuple. command is one of: xrdcp, gfal-copy, cp.
    Raises ValueError if resolution fails.
    """
    storage_json_path, slc_path = _find_siteconf_files(siteconfig_path)
    volumes = parse_storage_json(storage_json_path)
    stageout_methods = parse_stageout_config(slc_path)

    if not stageout_methods:
        raise ValueError(f"No <stage-out> methods in {slc_path}")

    for method in stageout_methods:
        target_volume = method["volume"]
        target_protocol = method["protocol"]
        command = method["command"]

        # Find matching volume and protocol in storage.json
        for vol in volumes:
            if vol.get("volume") != target_volume:
                continue
            all_protos = vol.get("protocols", [])
            for proto in all_protos:
                if proto.get("protocol") != target_protocol:
                    continue
                # Prefer writable access levels
                access = proto.get("access", "")
                if "rw" in access or "write" in access or "local" in access:
                    pfn = resolve_lfn_to_pfn(lfn, proto, all_protos)
                    if pfn:
                        return pfn, command

        # Fallback: try any protocol in the volume matching the name
        for vol in volumes:
            if vol.get("volume") != target_volume:
                continue
            all_protos = vol.get("protocols", [])
            for proto in all_protos:
                if proto.get("protocol") != target_protocol:
                    continue
                pfn = resolve_lfn_to_pfn(lfn, proto, all_protos)
                if pfn:
                    return pfn, command

    raise ValueError(
        f"Could not resolve write PFN for {lfn} "
        f"(methods={stageout_methods}, volumes={[v.get('volume') for v in volumes]})"
    )


def resolve_read_pfn(lfn, siteconfig_path):
    """Resolve LFN to read PFN, preferring XRootD protocol for direct CMSSW access.

    Returns (pfn, is_direct) tuple. is_direct=True means CMSSW can read it
    natively (root:// URL) without downloading first.
    """
    storage_json_path, slc_path = _find_siteconf_files(siteconfig_path)
    volumes = parse_storage_json(storage_json_path)
    stageout_methods = parse_stageout_config(slc_path)

    # Get the stageout volume name to search the right volume
    target_volume = stageout_methods[0]["volume"] if stageout_methods else None

    # Search for an XRootD protocol in the same volume (any access level)
    for vol in volumes:
        if target_volume and vol.get("volume") != target_volume:
            continue
        all_protos = vol.get("protocols", [])
        for proto in all_protos:
            if proto.get("protocol") == "XRootD":
                pfn = resolve_lfn_to_pfn(lfn, proto, all_protos)
                if pfn and pfn.startswith("root://"):
                    return pfn, True

    # Fallback: use the write protocol (will need download)
    try:
        pfn, _cmd = resolve_write_pfn(lfn, siteconfig_path)
        is_direct = pfn.startswith("root://")
        return pfn, is_direct
    except ValueError:
        raise ValueError(f"Could not resolve read PFN for {lfn}")


# ── File operations ───────────────────────────────────────────

def _run_cmd(cmd, retries=3, retry_delay=10, description=""):
    """Run a shell command with retries."""
    for attempt in range(retries):
        try:
            result = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, timeout=3600,
            )
            if result.returncode == 0:
                return result
            msg = f"{description} failed (attempt {attempt + 1}/{retries}): {result.stderr.strip()}"
            print(msg, file=sys.stderr)
        except subprocess.TimeoutExpired:
            msg = f"{description} timed out (attempt {attempt + 1}/{retries})"
            print(msg, file=sys.stderr)
        except Exception as e:
            msg = f"{description} error (attempt {attempt + 1}/{retries}): {e}"
            print(msg, file=sys.stderr)
        if attempt < retries - 1:
            time.sleep(retry_delay * (attempt + 1))
    raise RuntimeError(f"{description} failed after {retries} attempts")


def upload_file(local_path, pfn, command):
    """Upload local file to remote PFN."""
    if command == "cp":
        dest_dir = os.path.dirname(pfn)
        os.makedirs(dest_dir, exist_ok=True)
        shutil.copy2(local_path, pfn)
        print(f"  cp: {local_path} -> {pfn}")
    elif command == "xrdcp":
        if pfn.startswith("davs://") or pfn.startswith("https://"):
            # xrdcp can't reliably handle davs/https — use gfal-copy
            src = f"file://{os.path.abspath(local_path)}"
            _run_cmd(
                ["gfal-copy", "-f", src, pfn],
                description=f"gfal-copy upload {os.path.basename(local_path)}",
            )
            print(f"  gfal-copy: {local_path} -> {pfn}")
        else:
            _run_cmd(
                ["xrdcp", "-f", local_path, pfn],
                description=f"xrdcp upload {os.path.basename(local_path)}",
            )
            print(f"  xrdcp: {local_path} -> {pfn}")
    elif command == "gfal-copy":
        src = f"file://{os.path.abspath(local_path)}"
        _run_cmd(
            ["gfal-copy", "-f", src, pfn],
            description=f"gfal-copy upload {os.path.basename(local_path)}",
        )
        print(f"  gfal-copy: {local_path} -> {pfn}")
    else:
        raise ValueError(f"Unknown stageout command: {command}")


def download_file(pfn, local_path, command):
    """Download remote PFN to local file."""
    if command == "cp":
        shutil.copy2(pfn, local_path)
    elif command == "xrdcp":
        if pfn.startswith("davs://") or pfn.startswith("https://"):
            # xrdcp can't read davs/https — use gfal-copy
            dest = f"file://{os.path.abspath(local_path)}"
            _run_cmd(
                ["gfal-copy", "-f", pfn, dest],
                description=f"gfal-copy download {os.path.basename(pfn)}",
            )
        else:
            _run_cmd(
                ["xrdcp", "-f", pfn, local_path],
                description=f"xrdcp download {os.path.basename(pfn)}",
            )
    elif command == "gfal-copy":
        dest = f"file://{os.path.abspath(local_path)}"
        _run_cmd(
            ["gfal-copy", "-f", pfn, dest],
            description=f"gfal-copy download {os.path.basename(pfn)}",
        )
    else:
        raise ValueError(f"Unknown stageout command: {command}")


def list_dir(pfn_dir, command):
    """List files in a remote directory. Returns list of basenames."""
    if command == "cp":
        if os.path.isdir(pfn_dir):
            return sorted(os.listdir(pfn_dir))
        return []
    elif command == "xrdcp":
        # xrdfs only supports root:// URLs; for davs/https, fall through to gfal-ls
        m = re.match(r"(root://[^/]+/)(/.*)", pfn_dir)
        if m:
            host_prefix, path = m.group(1), m.group(2)
            result = _run_cmd(
                ["xrdfs", host_prefix.rstrip("/"), "ls", path],
                retries=2, description=f"xrdfs ls {path}",
            )
            entries = []
            for line in result.stdout.strip().split("\\n"):
                line = line.strip()
                if line:
                    entries.append(os.path.basename(line))
            return sorted(entries)
        # davs:// or https:// — use gfal-ls instead
        result = _run_cmd(
            ["gfal-ls", pfn_dir],
            retries=2, description=f"gfal-ls {pfn_dir}",
        )
        entries = []
        for line in result.stdout.strip().split("\\n"):
            line = line.strip()
            if line:
                entries.append(os.path.basename(line))
        return sorted(entries)
    elif command == "gfal-copy":
        result = _run_cmd(
            ["gfal-ls", pfn_dir],
            retries=2, description=f"gfal-ls {pfn_dir}",
        )
        entries = []
        for line in result.stdout.strip().split("\\n"):
            line = line.strip()
            if line:
                entries.append(os.path.basename(line))
        return sorted(entries)
    return []


def remove_path(pfn, command, recursive=False):
    """Remove a remote file or directory."""
    if command == "cp":
        if recursive and os.path.isdir(pfn):
            shutil.rmtree(pfn)
        elif os.path.exists(pfn):
            os.remove(pfn)
    elif command == "xrdcp":
        m = re.match(r"(root://[^/]+/)(/.*)", pfn)
        if not m:
            # davs:// or https:// — use gfal-rm
            cmd = ["gfal-rm"]
            if recursive:
                cmd.append("-r")
            cmd.append(pfn)
            try:
                _run_cmd(cmd, retries=2, description=f"gfal-rm {pfn}")
            except RuntimeError:
                pass  # Best effort
            return
        host, path = m.group(1).rstrip("/"), m.group(2)
        if recursive:
            # List and remove each file, then rmdir
            try:
                result = _run_cmd(
                    ["xrdfs", host, "ls", path],
                    retries=1, description=f"xrdfs ls {path}",
                )
                for line in result.stdout.strip().split("\\n"):
                    entry = line.strip()
                    if entry:
                        _run_cmd(
                            ["xrdfs", host, "rm", entry],
                            retries=1, description=f"xrdfs rm {entry}",
                        )
                _run_cmd(
                    ["xrdfs", host, "rmdir", path],
                    retries=1, description=f"xrdfs rmdir {path}",
                )
            except RuntimeError:
                pass  # Best effort
        else:
            _run_cmd(
                ["xrdfs", host, "rm", path],
                retries=2, description=f"xrdfs rm {path}",
            )
    elif command == "gfal-copy":
        cmd = ["gfal-rm"]
        if recursive:
            cmd.append("-r")
        cmd.append(pfn)
        _run_cmd(cmd, retries=2, description=f"gfal-rm {pfn}")


def mkdir_p(pfn, command):
    """Create remote directory (including parents)."""
    if command == "cp":
        os.makedirs(pfn, exist_ok=True)
    elif command == "xrdcp":
        m = re.match(r"(root://[^/]+/)(/.*)", pfn)
        if m:
            host, path = m.group(1).rstrip("/"), m.group(2)
            _run_cmd(
                ["xrdfs", host, "mkdir", "-p", path],
                retries=2, description=f"xrdfs mkdir -p {path}",
            )
        else:
            # davs:// or https:// — use gfal-mkdir
            _run_cmd(
                ["gfal-mkdir", "-p", pfn],
                retries=2, description=f"gfal-mkdir -p {pfn}",
            )
    elif command == "gfal-copy":
        _run_cmd(
            ["gfal-mkdir", "-p", pfn],
            retries=2, description=f"gfal-mkdir -p {pfn}",
        )


if __name__ == "__main__":
    # Simple test: resolve a write PFN
    if len(sys.argv) >= 3:
        lfn = sys.argv[1]
        siteconf = sys.argv[2]
        pfn, cmd = resolve_write_pfn(lfn, siteconf)
        print(f"LFN:     {lfn}")
        print(f"PFN:     {pfn}")
        print(f"Command: {cmd}")
''')
    os.chmod(path, 0o755)


def _write_file(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)
