"""Real ReqMgr2 adapter using httpx with X.509 certificate authentication."""

import logging
from typing import Any

import httpx

from .base import ReqMgrAdapter

logger = logging.getLogger(__name__)

# Max retries with exponential backoff
MAX_RETRIES = 3
BACKOFF_BASE = 1.0


class ReqMgr2Client(ReqMgrAdapter):
    def __init__(self, base_url: str, cert_file: str, key_file: str):
        self._base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(
            cert=(cert_file, key_file),
            verify=True,
            timeout=30.0,
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
                        "ReqMgr2 request %s failed (attempt %d/%d): %s, retrying in %.1fs",
                        path, attempt + 1, MAX_RETRIES, exc, wait,
                    )
                    await asyncio.sleep(wait)
        raise last_exc  # type: ignore[misc]

    async def get_request(self, request_name: str) -> dict[str, Any]:
        data = await self._get(f"/reqmgr2/data/request/{request_name}")
        # ReqMgr2 wraps the response in {"result": [{...}]}
        results = data.get("result", [])
        if not results:
            raise ValueError(f"Request {request_name} not found in ReqMgr2")
        return results[0]

    async def get_assigned_requests(self, agent_name: str) -> list[dict[str, Any]]:
        data = await self._get(
            "/reqmgr2/data/request",
            params={"status": "assigned", "team": agent_name},
        )
        results = data.get("result", [])
        if results and isinstance(results[0], dict):
            # Response format: {"result": [{"request_name": {...}, ...}]}
            return list(results[0].values()) if results[0] else []
        return []
