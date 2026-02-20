"""Pre/post cleanup for matrix test runs.

Removes leftover directories and condor jobs from previous runs.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

MATRIX_BASE = "/mnt/shared/work/wms2_matrix"
PFN_PREFIX = "/mnt/shared"


def work_dir_for(wf_id: float) -> Path:
    """Return the per-workflow working directory."""
    return Path(MATRIX_BASE) / f"wf_{wf_id}"


def sweep_pre(wf_id: float, output_datasets: list[dict] | None = None) -> Path:
    """Clean and recreate the work directory for a workflow.  Returns the path.

    Also removes any leftover LFN output paths from previous runs so
    verification is not confused by stale files.
    """
    wd = work_dir_for(wf_id)
    if wd.exists():
        shutil.rmtree(wd)
        logger.debug("Removed %s", wd)
    wd.mkdir(parents=True)

    # Clean LFN output directories
    for ds in output_datasets or []:
        for key in ("merged_lfn_base", "unmerged_lfn_base"):
            lfn_base = ds.get(key, "")
            if lfn_base:
                pfn = os.path.join(PFN_PREFIX, lfn_base.lstrip("/"))
                if os.path.isdir(pfn):
                    shutil.rmtree(pfn)
                    logger.debug("Cleaned LFN output %s", pfn)
    return wd


def sweep_post(wf_id: float, keep_artifacts: bool = False) -> None:
    """Clean up after a workflow run.

    If *keep_artifacts* is True (e.g. on failure), the directory is left
    for debugging.
    """
    if keep_artifacts:
        logger.info("Keeping artifacts for wf %.1f at %s", wf_id, work_dir_for(wf_id))
        return
    wd = work_dir_for(wf_id)
    if wd.exists():
        shutil.rmtree(wd)
        logger.debug("Cleaned %s", wd)


async def remove_dagman_jobs(condor_adapter, schedd_name: str, cluster_id: str) -> None:
    """Best-effort removal of a DAGMan cluster."""
    try:
        await condor_adapter.remove_job(schedd_name, cluster_id)
        logger.debug("Removed DAGMan cluster %s", cluster_id)
    except Exception as exc:
        logger.warning("Failed to remove cluster %s: %s", cluster_id, exc)
