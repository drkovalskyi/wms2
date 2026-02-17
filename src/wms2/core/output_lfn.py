"""LFN derivation helpers for CMS output datasets.

CMS LFN conventions:
  Unmerged: /store/unmerged/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-v{ProcessingVersion}
  Merged:   /store/mc/{AcquisitionEra}/{PrimaryDataset}/{DataTier}/{ProcessingString}-v{ProcessingVersion}/{block}/{file}
  DBS name: /{PrimaryDataset}/{AcquisitionEra}-{ProcessingString}-v{ProcessingVersion}/{DataTier}
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)


def derive_merged_lfn_bases(request_data: dict) -> list[dict]:
    """Derive merged LFN bases from a ReqMgr2 request.

    Uses OutputModulesLFNBases + OutputDatasets + MergedLFNBase
    to produce a list of:
      {"dataset_name": "/Primary/Era-Proc-vN/Tier",
       "merged_lfn_base": "/store/mc/Era/Primary/Tier/Proc-vN"}

    Only returns outputs that appear in OutputDatasets (KeepOutput=true steps).
    """
    output_datasets = request_data.get("OutputDatasets", [])
    merged_lfn_base_root = request_data.get("MergedLFNBase", "/store/mc")

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
        results.append({
            "dataset_name": ds_name,
            "merged_lfn_base": merged_base,
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


def local_output_path(output_base_dir: str, lfn: str) -> str:
    """Convert an LFN to a local file path.

    /store/mc/Era/... → {output_base_dir}/mc/Era/...
    """
    # Strip the /store prefix from the LFN
    if lfn.startswith("/store/"):
        relative = lfn[len("/store/"):]
    elif lfn.startswith("/store"):
        relative = lfn[len("/store"):]
    else:
        relative = lfn.lstrip("/")

    return f"{output_base_dir}/{relative}"
