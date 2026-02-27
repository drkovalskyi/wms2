"""Site Manager — site banning, promotion, and CRIC sync.

Manages site bans created automatically by the Error Handler (per-workflow)
or manually by operators (system-wide). Promotes per-workflow bans to
system-wide when a site is banned across enough workflows.

See docs/error_handling.md Section 6 for the full design.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from wms2.adapters.base import CRICAdapter
from wms2.config import Settings
from wms2.db.repository import Repository

logger = logging.getLogger(__name__)


class SiteManager:
    def __init__(
        self,
        repository: Repository,
        settings: Settings,
        cric_adapter: CRICAdapter | None = None,
    ):
        self.db = repository
        self.settings = settings
        self.cric_adapter = cric_adapter

    async def ban_site(
        self,
        site_name: str,
        workflow_id=None,
        reason: str = "",
        failure_data: dict | None = None,
        duration_days: int | None = None,
    ):
        """Create a site ban. Returns the ban row, or None if site doesn't exist."""
        site = await self.db.get_site(site_name)
        if not site:
            logger.warning("Cannot ban unknown site %s — not in sites table", site_name)
            return None

        days = duration_days or self.settings.site_ban_duration_days
        expires_at = datetime.now(timezone.utc) + timedelta(days=days)

        ban = await self.db.create_site_ban(
            site_name=site_name,
            workflow_id=workflow_id,
            reason=reason,
            failure_data=failure_data,
            expires_at=expires_at,
        )
        scope = f"workflow {workflow_id}" if workflow_id else "system-wide"
        logger.info(
            "Banned site %s (%s) for %d days: %s",
            site_name, scope, days, reason,
        )

        if workflow_id is not None:
            await self._check_promotion(site_name)

        return ban

    async def _check_promotion(self, site_name: str) -> None:
        """Promote per-workflow bans to system-wide if threshold is reached."""
        count = await self.db.count_active_workflow_bans_for_site(site_name)
        if count < self.settings.site_ban_promotion_threshold:
            return

        # Check for existing active system-wide ban
        existing = await self.db.get_active_bans_for_site(site_name)
        for ban in existing:
            if ban.workflow_id is None:
                logger.debug(
                    "System-wide ban already exists for %s, skipping promotion",
                    site_name,
                )
                return

        days = self.settings.site_ban_duration_days
        expires_at = datetime.now(timezone.utc) + timedelta(days=days)
        await self.db.create_site_ban(
            site_name=site_name,
            workflow_id=None,
            reason=f"Promoted: {count} workflows banned",
            expires_at=expires_at,
        )
        logger.warning(
            "Promoted site %s to system-wide ban (%d workflow bans)",
            site_name, count,
        )

    async def get_banned_sites(self, workflow_id=None) -> list[str]:
        """Return deduplicated sorted list of banned site names."""
        system_bans = await self.db.get_active_system_bans()
        sites = {ban.site_name for ban in system_bans}

        if workflow_id is not None:
            wf_bans = await self.db.get_active_workflow_bans(workflow_id)
            sites.update(ban.site_name for ban in wf_bans)

        return sorted(sites)

    async def remove_ban(
        self, site_name: str, removed_by: str, workflow_id=None,
    ) -> int:
        """Remove active bans for a site. Returns count of bans removed."""
        return await self.db.remove_active_bans_for_site(
            site_name, removed_by, workflow_id=workflow_id,
        )

    async def get_active_bans(self, site_name: str | None = None):
        """Return active bans, optionally filtered by site."""
        if site_name:
            return await self.db.get_active_bans_for_site(site_name)
        return await self.db.list_all_active_bans()

    async def sync_from_cric(self) -> dict[str, Any]:
        """Sync site information from CRIC into the sites table.

        Returns a stats dict: {"added": N, "updated": N, "total": N, "errors": N}.
        If no cric_adapter is configured, returns zeros and logs a warning.
        """
        if self.cric_adapter is None:
            logger.info("CRIC sync skipped — no cric_adapter configured")
            return {"added": 0, "updated": 0, "total": 0, "errors": 0}

        try:
            sites = await self.cric_adapter.get_sites()
        except Exception:
            logger.exception("CRIC sync failed — could not fetch sites")
            return {"added": 0, "updated": 0, "total": 0, "errors": 1}

        added = 0
        updated = 0
        errors = 0
        for site_data in sites:
            site_name = site_data.get("name")
            if not site_name:
                errors += 1
                continue
            try:
                existing = await self.db.get_site(site_name)
                await self.db.upsert_site(**site_data)
                if existing is None:
                    added += 1
                else:
                    updated += 1
            except Exception:
                logger.exception("Failed to upsert site %s", site_name)
                errors += 1

        logger.info(
            "CRIC sync complete: %d added, %d updated, %d total, %d errors",
            added, updated, len(sites), errors,
        )
        return {"added": added, "updated": updated, "total": len(sites), "errors": errors}
