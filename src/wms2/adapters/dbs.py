"""Real DBS adapter using httpx with X.509 certificate authentication."""

import logging
from typing import Any

import httpx

from .base import DBSAdapter

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1.0


class DBSClient(DBSAdapter):
    def __init__(self, base_url: str, cert_file: str, key_file: str):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            cert=(cert_file, key_file),
            verify=True,
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
                    import asyncio
                    wait = BACKOFF_BASE * (2 ** attempt)
                    logger.warning(
                        "DBS request %s failed (attempt %d/%d): %s, retrying in %.1fs",
                        path, attempt + 1, MAX_RETRIES, exc, wait,
                    )
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore[misc]

    async def get_files(
        self,
        dataset: str,
        limit: int = 0,
        run_whitelist: list[int] | None = None,
        lumi_mask: dict[str, list[list[int]]] | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"dataset": dataset, "detail": 1}
        if limit:
            params["limit"] = limit
        if run_whitelist:
            params["run_num"] = run_whitelist
        data = await self._get("/files", params=params)
        # DBS returns a flat list of file dicts
        return data if isinstance(data, list) else []

    async def inject_dataset(self, dataset_info: dict[str, Any]) -> None:
        url = f"{self._base_url}/datasets"
        resp = await self._client.post(url, json=dataset_info)
        resp.raise_for_status()

    async def invalidate_dataset(self, dataset_name: str, reason: str) -> None:
        url = f"{self._base_url}/datasets"
        payload = {
            "dataset": dataset_name,
            "dataset_access_type": "INVALID",
        }
        resp = await self._client.put(url, json=payload)
        resp.raise_for_status()
