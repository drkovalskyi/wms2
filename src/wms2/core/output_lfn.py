"""LFN derivation helpers for CMS output datasets.

CMS LFN conventions:
  Unmerged: /store/unmerged/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-v{ProcessingVersion}
  Merged:   /store/mc/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-v{ProcessingVersion}/{block}/{file}
  DBS name: /{PrimaryDataset}/{AcquisitionEra}-{ProcessingString}-v{ProcessingVersion}/{DataTier}

LFN→PFN mapping for local storage:
  PFN = local_pfn_prefix + LFN
  e.g. /mnt/shared + /store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root
     → /mnt/shared/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)


async def determine_merged_lfn_base(request_data: dict, dbs_adapter=None) -> str:
    """Determine the correct MergedLFNBase for a request.

    Logic (mirrors WMAgent Unified assignor.py):
    - StoreResults: use request's explicit MergedLFNBase
    - GEN workflows (no InputDataset): return /store/mc
    - Workflows with InputDataset: query DBS for a file, extract LFN base
      (first 3 path components, e.g. /store/mc or /store/data)
    - Fallback: /store/mc
    """
    request_type = request_data.get("RequestType", "")

    # StoreResults: use explicit value
    if request_type == "StoreResults":
        return request_data.get("MergedLFNBase", "/store/mc")

    # Check for input dataset
    input_dataset = request_data.get("InputDataset", "")
    if not input_dataset:
        # GEN workflow — check Step1 for input
        step1 = request_data.get("Step1", {})
        input_dataset = step1.get("InputDataset", "")

    if not input_dataset or request_data.get("_is_gen"):
        return "/store/mc"

    # Query DBS for a file in the input dataset to determine LFN base
    if dbs_adapter is not None:
        try:
            files = await dbs_adapter.list_files(input_dataset, limit=1)
            if files:
                lfn = files[0].get("logical_file_name", "")
                if lfn:
                    # Extract first 3 path components: /store/mc or /store/data
                    parts = lfn.split("/")
                    if len(parts) >= 3:
                        base = "/" + "/".join(parts[1:3])
                        logger.info(
                            "Determined MergedLFNBase=%s from input dataset %s",
                            base, input_dataset,
                        )
                        return base
        except Exception:
            logger.warning(
                "Failed to query DBS for input dataset %s, using default /store/mc",
                input_dataset, exc_info=True,
            )

    return "/store/mc"


def lfn_to_pfn(local_pfn_prefix: str, lfn: str) -> str:
    """Convert an LFN to a local PFN by prepending the site prefix.

    Example: lfn_to_pfn("/mnt/shared", "/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root")
           → "/mnt/shared/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root"
    """
    return os.path.join(local_pfn_prefix, lfn.lstrip("/"))


def local_output_path(local_pfn_prefix: str, lfn: str) -> str:
    """Backward-compatible alias for lfn_to_pfn()."""
    return lfn_to_pfn(local_pfn_prefix, lfn)


def derive_merged_lfn_bases(request_data: dict) -> list[dict]:
    """Derive merged and unmerged LFN bases from a ReqMgr2 request.

    Uses OutputModulesLFNBases + OutputDatasets + MergedLFNBase + UnmergedLFNBase
    to produce a list of:
      {"dataset_name": "/Primary/Era-Proc-vN/Tier",
       "merged_lfn_base": "/store/mc/Era/Primary/Tier/Proc-vN",
       "unmerged_lfn_base": "/store/unmerged/Era/Primary/Tier/Proc-vN"}

    Only returns outputs that appear in OutputDatasets (KeepOutput=true steps).
    """
    output_datasets = request_data.get("OutputDatasets", [])
    merged_lfn_base_root = request_data.get("MergedLFNBase", "/store/mc")
    unmerged_lfn_base_root = request_data.get("UnmergedLFNBase", "/store/unmerged")

    if not output_datasets:
        return []

    # Parse OutputDatasets to extract components
    # DBS format: /{PrimaryDataset}/{AcquisitionEra}-{ProcessingString}-v{ProcessingVersion}/{DataTier}
    results = []
    for ds_name in output_datasets:
        parsed = _parse_dataset_name(ds_name)
        if not parsed:
            logger.warning("Could not parse dataset name: %s", ds_name)
            continue

        primary, acq_era, proc_string, proc_version, data_tier = parsed
        # Build merged LFN base
        # /store/mc/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-v{ProcessingVersion}
        merged_base = (
            f"{merged_lfn_base_root}/{acq_era}/{primary}/{data_tier}/"
            f"{proc_string}-v{proc_version}"
        )
        # Build unmerged LFN base (same structure, different root)
        unmerged_base = (
            f"{unmerged_lfn_base_root}/{acq_era}/{primary}/{data_tier}/"
            f"{proc_string}-v{proc_version}"
        )
        results.append({
            "dataset_name": ds_name,
            "merged_lfn_base": merged_base,
            "unmerged_lfn_base": unmerged_base,
            "primary_dataset": primary,
            "acquisition_era": acq_era,
            "processing_string": proc_string,
            "processing_version": proc_version,
            "data_tier": data_tier,
        })

    return results


def _parse_dataset_name(dataset_name: str) -> tuple[str, str, str, str, str] | None:
    """Parse a DBS dataset name into components.

    Format: /{PrimaryDataset}/{AcquisitionEra}-{ProcessingString}-v{ProcessingVersion}/{DataTier}
    Returns: (primary, acq_era, proc_string, proc_version, data_tier) or None.
    """
    # Match: /Primary/Era-ProcString-vN/Tier
    m = re.match(
        r"^/([^/]+)/([^/]+)-([^/]+)-v(\d+)/([^/]+)$",
        dataset_name,
    )
    if m:
        return m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)

    # Fallback: simpler format /Primary/Processing/Tier (2-component middle)
    parts = dataset_name.strip("/").split("/")
    if len(parts) == 3:
        primary = parts[0]
        data_tier = parts[2]
        # Try to parse the middle part
        middle = parts[1]
        m2 = re.match(r"^(.+?)-(.+)-v(\d+)$", middle)
        if m2:
            return primary, m2.group(1), m2.group(2), m2.group(3), data_tier

    return None


def merged_lfn_for_group(
    merged_lfn_base: str,
    group_index: int,
    filename: str = "merged.txt",
) -> str:
    """Build full merged LFN for a specific merge group.

    Returns: {merged_lfn_base}/{group_index:06d}/{filename}
    """
    return f"{merged_lfn_base}/{group_index:06d}/{filename}"


def unmerged_lfn_for_group(
    unmerged_lfn_base: str,
    group_index: int,
    filename: str = "",
) -> str:
    """Build full unmerged LFN directory (or file) for a specific merge group.

    Returns: {unmerged_lfn_base}/{group_index:06d}[/{filename}]
    """
    base = f"{unmerged_lfn_base}/{group_index:06d}"
    if filename:
        return f"{base}/{filename}"
    return base
