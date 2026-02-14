"""Real Rucio adapter using httpx with X.509 certificate authentication."""

import logging
import re
from typing import Any

import httpx

from .base import RucioAdapter

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1.0

# Strip _Disk/_Tape suffixes from RSE names to get CMS site names
_RSE_SUFFIX_RE = re.compile(r"_(Disk|Tape|Test|Temp)$")


class RucioClient(RucioAdapter):
    def __init__(self, base_url: str, account: str, cert_file: str, key_file: str):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            cert=(cert_file, key_file),
            verify=True,
            timeout=60.0,
            headers={"X-Rucio-Account": account},
        )

    async def close(self):
        await self._client.aclose()

    async def _post(self, path: str, json_data: Any = None) -> Any:
        url = f"{self._base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            try:
                resp = await self._client.post(url, json=json_data)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPStatusError, httpx.TransportError) as exc:
                last_exc = exc
                if attempt < MAX_RETRIES - 1:
                    import asyncio
                    wait = BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "Rucio request %s failed (attempt %d/%d): %s, retrying in %.1fs",
                        path, attempt + 1, MAX_RETRIES, exc, wait,
                    )
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore[misc]

    @staticmethod
    def _rse_to_site(rse: str) -> str | None:
        """Convert RSE name to CMS site name, excluding tape-only RSEs."""
        if rse.endswith("_Tape"):
            return None
        return _RSE_SUFFIX_RE.sub("", rse)

    async def get_replicas(self, lfns: list[str]) -> dict[str, list[str]]:
        payload = {"dids": [{"scope": "cms", "name": lfn} for lfn in lfns]}
        data = await self._post("/replicas/list", json_data=payload)
        result: dict[str, list[str]] = {lfn: [] for lfn in lfns}
        if isinstance(data, list):
            for entry in data:
                lfn = entry.get("name", "")
                rses = entry.get("rses", {})
                sites = set()
                for rse_name in rses:
                    site = self._rse_to_site(rse_name)
                    if site:
                        sites.add(site)
                if lfn in result:
                    result[lfn] = sorted(sites)
        return result

    async def create_rule(self, dataset: str, destination: str, **kwargs: Any) -> str:
        payload = {
            "dids": [{"scope": "cms", "name": dataset}],
            "rse_expression": destination,
            "copies": 1,
            **kwargs,
        }
        url = f"{self._base_url}/rules/"
        resp = await self._client.post(url, json=payload)
        resp.raise_for_status()
        rule_ids = resp.json()
        return rule_ids[0] if rule_ids else ""

    async def get_rule_status(self, rule_id: str) -> dict[str, Any]:
        url = f"{self._base_url}/rules/{rule_id}"
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def delete_rule(self, rule_id: str) -> None:
        url = f"{self._base_url}/rules/{rule_id}"
        resp = await self._client.delete(url)
        resp.raise_for_status()
