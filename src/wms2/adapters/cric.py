"""Real CRIC adapter using httpx with X.509 certificate authentication."""

import asyncio
import logging
from typing import Any

import httpx

from .base import CRICAdapter

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1.0

# CRIC API endpoint for CMS sites
SITES_PATH = "/api/cms/site/query/"


class CRICClient(CRICAdapter):
    def __init__(self, base_url: str, cert_file: str, key_file: str, verify: bool = True):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            cert=(cert_file, key_file),
            verify=verify,
            timeout=60.0,
        )

    async def close(self):
        await self._client.aclose()

    async def _get(self, path: str, params: dict | None = None) -> Any:
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = await self._client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES - 1:
                    wait = BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "CRIC request %s failed (attempt %d/%d): %s, retrying in %.1fs",
                        path, attempt + 1, MAX_RETRIES, exc, wait,
                    )
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore[misc]

    async def get_sites(self) -> list[dict[str, Any]]:
        """Fetch all CMS sites from CRIC and map to SiteRow-compatible dicts.

        CRIC returns a dict keyed by site name, e.g.:
        {
            "T1_US_FNAL": {
                "name": "T1_US_FNAL",
                "facility": "US_FNAL",
                "tier_level": 1,
                "rcsite_state": "ACTIVE",
                ...
            },
            ...
        }

        We extract a flat list of dicts with the columns the sites table needs.
        """
        raw = await self._get(SITES_PATH, params={"json": "", "rcsite_state": "ANY"})

        # CRIC returns a dict keyed by site name
        if not isinstance(raw, dict):
            logger.warning("CRIC returned unexpected type %s, expected dict", type(raw).__name__)
            return []

        sites: list[dict[str, Any]] = []
        for site_name, info in raw.items():
            if not isinstance(info, dict):
                continue
            sites.append(_map_cric_site(site_name, info))

        logger.info("Fetched %d sites from CRIC", len(sites))
        return sites


def _map_cric_site(site_name: str, info: dict[str, Any]) -> dict[str, Any]:
    """Map a single CRIC site entry to SiteRow columns.

    Tolerant of missing fields — extracts what's available, skips the rest.
    The full CRIC response is stored in config_data for future use.
    """
    # Map CRIC state → WMS2 status
    cric_state = (info.get("rcsite_state") or "").upper()
    if cric_state == "ACTIVE":
        status = "enabled"
    elif cric_state in ("STANDBY", "WAITING"):
        status = "drain"
    else:
        status = "disabled"

    return {
        "name": site_name,
        "total_slots": info.get("total_slots"),
        "status": status,
        "schedd_name": info.get("schedd_name"),
        "collector_host": info.get("collector_host"),
        "storage_element": info.get("se"),
        "storage_path": info.get("storage_path"),
        "config_data": info,
    }
