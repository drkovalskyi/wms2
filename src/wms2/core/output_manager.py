"""Output Manager: post-compute pipeline for processing blocks.

Handles the output lifecycle at the processing block level: DBS file
registration, Rucio source protection, DBS block closure, and tape archival
rule creation.

Rucio registration is FATAL for non-local stageout modes: if registration
fails, the error propagates to the lifecycle manager which holds the request.
There is no point producing more data if we cannot register and transfer it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

from wms2.adapters.base import DBSAdapter, RucioAdapter
from wms2.db.repository import Repository
from wms2.models.enums import BlockStatus

logger = logging.getLogger(__name__)

# Module-level cache: tracks (block_id, rse) pairs that have been successfully
# registered in Rucio this process lifetime. Prevents redundant idempotent calls.
_rucio_registered_cache: set[tuple] = set()


class RucioRegistrationError(Exception):
    """Raised when Rucio DID registration fails fatally."""


class OutputManager:
    """Manages the output lifecycle for processing blocks.

    Three interactions per work unit completion:
      1. Register files in DBS (open block if first work unit)
      2. Create Rucio source protection rule

    One interaction per block completion:
      3. Close DBS block, create tape archival rule

    Called by the Request Lifecycle Manager — not an independent loop.
    """

    RUCIO_MAX_RETRY_DURATION = timedelta(days=3)
    RUCIO_BACKOFF_SCHEDULE = [60, 300, 1800, 7200, 14400, 28800]  # 1m, 5m, 30m, 2h, 4h, 8h

    def __init__(
        self,
        repository: Repository,
        dbs_adapter: DBSAdapter,
        rucio_adapter: RucioAdapter,
        site_manager=None,
    ):
        self.db = repository
        self.dbs = dbs_adapter
        self.rucio = rucio_adapter
        self.site_manager = site_manager

    async def handle_work_unit_completion(
        self, workflow_id: UUID, block_id: UUID, merge_info: dict,
        config_data: dict | None = None,
    ) -> None:
        """Called when a work unit completes.

        Registers output files in DBS and Rucio, creates source protection rule.
        Raises RucioRegistrationError if Rucio registration fails for a
        non-local stageout mode — caller should hold the request.
        """
        block = await self.db.get_processing_block(block_id)
        if not block:
            logger.error("Processing block %s not found", block_id)
            return

        # 1. Open DBS block on first work unit
        block_name = block.dbs_block_name
        site = merge_info.get("site", "local")
        if not block.dbs_block_open:
            block_name = await self.dbs.open_block(
                block.dataset_name, block.block_index,
                origin_site_name=site,
            )
            await self.db.update_processing_block(
                block.id, dbs_block_name=block_name, dbs_block_open=True
            )

        # 2. Register files in DBS
        output_files = merge_info.get("output_files", [])
        if output_files:
            await self.dbs.register_files(
                block_name=block_name or block.dbs_block_name,
                files=output_files,
            )

        # 3. Update block state FIRST — record completion + files before Rucio
        #    so data is persisted even if Rucio fails
        node_name = merge_info.get("node_name", "unknown")
        source_rules = dict(block.source_rule_ids or {})
        completed = list(block.completed_work_units or []) + [node_name]

        # Annotate files with site for retry
        annotated_files = []
        for f in output_files:
            af = dict(f)
            af["site"] = site
            annotated_files.append(af)

        accumulated_files = list(block.output_files or []) + annotated_files

        await self.db.update_processing_block(
            block.id,
            completed_work_units=completed,
            source_rule_ids=source_rules,
            output_files=accumulated_files,
            site=site,
        )

        # 4. Register files as Rucio DIDs — FATAL for non-local stageout
        stageout_mode = (config_data or {}).get("stageout_mode", "local")
        if output_files and site != "local" and stageout_mode != "local":
            await self._register_files_in_rucio(
                block, output_files, site, config_data=config_data,
            )
            # No try/except — let exceptions propagate to hold the request

        # 5. Create Rucio source protection rule (production only)
        if stageout_mode == "production":
            try:
                rule_id = await self.rucio.create_rule(
                    dataset=block.dataset_name,
                    destination=merge_info.get("site", "local"),
                )
                source_rules[node_name] = rule_id
                await self.db.update_processing_block(
                    block.id, source_rule_ids=source_rules,
                )
            except Exception as e:
                await self._record_rucio_failure(block, e)
                raise RucioRegistrationError(
                    f"Source protection failed for block {block.id}: {e}"
                ) from e

        # 6. Check if block is now complete
        if len(completed) >= block.total_work_units:
            await self._complete_block(block, config_data=config_data)

    async def _register_files_in_rucio(
        self, block, output_files: list[dict], site: str,
        config_data: dict | None = None,
    ) -> None:
        """Register merged output files as Rucio DIDs at the site RSE.

        Stageout mode determines naming conventions:
        - test:  scope=user.<rucio_test_account>, RSE=<site>_Temp, /USER DID tier
        - production: scope=cms, RSE=<site>, standard DID naming

        Rucio derives PFN from RSE protocol config (deterministic naming).
        Raises on failure — caller decides whether to hold the request.
        """
        cd = config_data or {}
        stageout_mode = cd.get("stageout_mode", "local")
        if stageout_mode == "local":
            return

        # Determine scope and RSE based on stageout mode
        if stageout_mode == "test":
            rucio_account = cd.get("rucio_test_account", "")
            if not rucio_account:
                raise RucioRegistrationError(
                    "rucio_test_account not set in config_data for test stageout mode"
                )
            scope = f"user.{rucio_account}"
            if self.site_manager:
                rse = self.site_manager.get_temp_rse_name(site)
                if not rse:
                    logger.info(
                        "Skipping Rucio registration for site %s — "
                        "no _Temp RSE in Rucio (files exist on grid but not in Rucio)",
                        site,
                    )
                    return
            else:
                rse = f"{site}_Temp" if not site.endswith("_Temp") else site
        else:
            scope = "cms"
            rse = site

        # Build Rucio DID names from CMS dataset name
        ds = block.dataset_name
        parts = ds.strip("/").split("/")
        if len(parts) != 3:
            raise RucioRegistrationError(
                f"Cannot parse dataset name for Rucio: {ds}"
            )
        primary, processing, tier = parts

        if stageout_mode == "test":
            container_name = f"/{primary}/{processing}-{tier}/USER"
            block_did = f"{container_name}#block_{block.block_index:03d}"
        else:
            block_did = f"{ds}#block_{block.block_index:03d}"
            container_name = None

        # Build replica list with PFN for non-deterministic _Temp RSEs
        pfn_prefix = None
        if self.site_manager and stageout_mode == "test":
            pfn_prefix = self.site_manager.get_temp_rse_pfn_prefix(rse)

        replicas = []
        file_dids = []
        for f in output_files:
            lfn = f.get("lfn", "")
            size = f.get("size", 0)
            adler32 = f.get("adler32", "")
            if not lfn:
                continue
            replica = {
                "scope": scope,
                "name": lfn,
                "bytes": size,
            }
            if adler32:
                replica["adler32"] = adler32
            if pfn_prefix:
                # _Temp RSE prefix maps /store/temp/ → PFN base path
                # LFN: /store/temp/user/... → suffix: user/...
                lfn_suffix = lfn.removeprefix("/store/temp/")
                replica["pfn"] = pfn_prefix + lfn_suffix
            replicas.append(replica)
            file_dids.append({"scope": scope, "name": lfn})

        if not replicas:
            return

        # Register replicas (creates file DIDs implicitly)
        await self.rucio.register_replicas(rse, replicas)

        # Create container and block DIDs, attach files
        if container_name:
            await self.rucio.add_did(scope, container_name, "CONTAINER")
            await self.rucio.add_did(scope, block_did, "DATASET")
            await self.rucio.attach_dids(
                scope, container_name,
                [{"scope": scope, "name": block_did}],
            )
        else:
            await self.rucio.add_did(scope, block_did, "DATASET")

        # Attach files to block
        await self.rucio.attach_dids(scope, block_did, file_dids)

        _rucio_registered_cache.add((block.id, rse))
        logger.info(
            "Registered %d files in Rucio at %s scope=%s block=%s",
            len(replicas), rse, scope, block_did,
        )

    async def _complete_block(self, block, config_data: dict | None = None) -> None:
        """All work units done. Close DBS block and create tape archival rule."""
        block_name = block.dbs_block_name

        # Close DBS block
        if block_name and not block.dbs_block_closed:
            await self.dbs.close_block(block_name)

        await self.db.update_processing_block(
            block.id,
            dbs_block_closed=True,
            status=BlockStatus.COMPLETE.value,
        )

        # Create tape archival rule (production only)
        stageout_mode = (config_data or {}).get("stageout_mode", "local")
        if stageout_mode == "production":
            try:
                tape_rule_id = await self.rucio.create_rule(
                    dataset=block.dataset_name,
                    destination="tier=1&type=TAPE",
                )
                await self.db.update_processing_block(
                    block.id,
                    tape_rule_id=tape_rule_id,
                    status=BlockStatus.ARCHIVED.value,
                    rucio_last_error=None,
                    rucio_attempt_count=0,
                )
            except Exception as e:
                await self._record_rucio_failure(block, e)
                raise RucioRegistrationError(
                    f"Tape archival rule failed for block {block.id}: {e}"
                ) from e

        # Create consolidation rule (move all files to one target RSE)
        cd = config_data or {}
        consolidation_rse = cd.get("consolidation_rse", "")
        if consolidation_rse:
            # Skip consolidation if no files were registered in Rucio
            # (e.g. execution site has no _Temp RSE in test mode)
            file_sites = {f.get("site", "") for f in (block.output_files or [])}
            has_rucio_files = (
                not self.site_manager
                or any(self.site_manager.has_temp_rse(s) for s in file_sites if s)
            )
            if not has_rucio_files and cd.get("stageout_mode") == "test":
                logger.info(
                    "Skipping consolidation for block %s — "
                    "no files registered in Rucio (execution sites: %s)",
                    block.id, file_sites,
                )
                return
            stageout_mode = cd.get("stageout_mode", "local")
            ds = block.dataset_name
            parts = ds.strip("/").split("/")
            if stageout_mode == "test" and len(parts) == 3:
                primary, processing, tier = parts
                rule_did = f"/{primary}/{processing}-{tier}/USER"
            else:
                rule_did = ds
            rucio_account = cd.get("rucio_test_account", "")
            scope = f"user.{rucio_account}" if stageout_mode == "test" else "cms"
            try:
                rule_id = await self.rucio.create_rule(
                    dataset=rule_did,
                    destination=consolidation_rse,
                    scope=scope,
                )
                await self.db.update_processing_block(
                    block.id, consolidation_rule_id=rule_id,
                )
                logger.info(
                    "Consolidation rule created for block %s → %s: %s",
                    block.id, consolidation_rse, rule_id,
                )
            except Exception as e:
                err_str = str(e).lower()
                if "duplicate" in err_str or "already exists" in err_str:
                    logger.info(
                        "Consolidation rule already exists for block %s → %s",
                        block.id, consolidation_rse,
                    )
                    await self.db.update_processing_block(
                        block.id, consolidation_rule_id="existing",
                    )
                    return
                await self._record_rucio_failure(block, e)
                raise RucioRegistrationError(
                    f"Consolidation rule failed for block {block.id} → {consolidation_rse}: {e}"
                ) from e

    async def process_blocks_for_workflow(
        self, workflow_id: UUID, config_data: dict | None = None,
    ) -> None:
        """Retry blocks with failed Rucio calls.

        Called by the Lifecycle Manager every cycle.
        Raises RucioRegistrationError if a retry fails — caller should
        hold the request.
        """
        cd = config_data or {}
        stageout_mode = cd.get("stageout_mode", "local")
        blocks = await self.db.get_processing_blocks(workflow_id)
        for block in blocks:
            # Retry replica registration for files not yet in Rucio.
            # register_replicas / add_did / attach_dids are idempotent,
            # so safe to call again even if some files were already registered.
            if (stageout_mode != "local"
                    and block.output_files
                    and self.site_manager):
                files_by_site: dict[str, list[dict]] = {}
                for f in block.output_files:
                    fsite = f.get("site", "")
                    if fsite and fsite != "local":
                        files_by_site.setdefault(fsite, []).append(f)
                for fsite, files in files_by_site.items():
                    rse = self.site_manager.get_temp_rse_name(fsite)
                    if rse and (block.id, rse) not in _rucio_registered_cache:
                        try:
                            await self._register_files_in_rucio(
                                block, files, fsite, config_data=config_data,
                            )
                        except Exception:
                            logger.warning(
                                "Rucio registration retry failed for block %s site %s",
                                block.id, fsite, exc_info=True,
                            )

            if stageout_mode == "production":
                if block.status == BlockStatus.COMPLETE.value:
                    await self._retry_tape_archival(block)
                elif block.status == BlockStatus.OPEN.value and block.rucio_last_error:
                    await self._retry_source_protection(block)

            # Retry consolidation rule if configured but missing
            consolidation_rse = cd.get("consolidation_rse", "")
            if (consolidation_rse
                    and block.status in (BlockStatus.COMPLETE.value,
                                         BlockStatus.ARCHIVED.value)
                    and not getattr(block, "consolidation_rule_id", None)):
                await self._retry_consolidation(
                    block, consolidation_rse, config_data,
                )

    async def _retry_tape_archival(self, block) -> None:
        """Retry tape archival rule creation with backoff."""
        if not self._should_retry(block):
            return
        try:
            tape_rule_id = await self.rucio.create_rule(
                dataset=block.dataset_name,
                destination="tier=1&type=TAPE",
            )
            await self.db.update_processing_block(
                block.id,
                tape_rule_id=tape_rule_id,
                status=BlockStatus.ARCHIVED.value,
                rucio_last_error=None,
                rucio_attempt_count=0,
            )
        except Exception as e:
            await self._record_rucio_failure(block, e)
            raise RucioRegistrationError(
                f"Tape archival retry failed for block {block.id}: {e}"
            ) from e

    async def _retry_source_protection(self, block) -> None:
        """Retry failed source protection rule creation with backoff."""
        if not self._should_retry(block):
            return
        try:
            rule_id = await self.rucio.create_rule(
                dataset=block.dataset_name,
                destination="local",
            )
            await self.db.update_processing_block(
                block.id,
                rucio_last_error=None,
                rucio_attempt_count=0,
            )
            logger.info(
                "Source protection retry succeeded for block %s: rule=%s",
                block.id, rule_id,
            )
        except Exception as e:
            await self._record_rucio_failure(block, e)
            raise RucioRegistrationError(
                f"Source protection retry failed for block {block.id}: {e}"
            ) from e

    async def _retry_consolidation(
        self, block, consolidation_rse: str,
        config_data: dict | None = None,
    ) -> None:
        """Retry consolidation rule creation with backoff.

        Re-registers DIDs (idempotent) before creating the rule,
        since the initial registration may have failed.
        Groups files by site to register at the correct RSE.
        """
        if not self._should_retry(block):
            return

        # Skip if no files are at Rucio-enabled sites (test mode)
        cd_check = config_data or {}
        if cd_check.get("stageout_mode") == "test" and self.site_manager:
            file_sites = {f.get("site", "") for f in (block.output_files or [])}
            has_rucio_files = any(
                self.site_manager.has_temp_rse(s)
                for s in file_sites if s
            )
            if not has_rucio_files:
                logger.info(
                    "Skipping consolidation retry for block %s — "
                    "no files at Rucio-enabled sites",
                    block.id,
                )
                return

        # Re-register DIDs grouped by site
        if block.output_files:
            files_by_site: dict[str, list[dict]] = {}
            for f in block.output_files:
                fsite = f.get("site", block.site or "unknown")
                files_by_site.setdefault(fsite, []).append(f)

            for fsite, files in files_by_site.items():
                await self._register_files_in_rucio(
                    block, files, fsite, config_data=config_data,
                )

        # Create consolidation rule on the container DID
        cd = config_data or {}
        stageout_mode = cd.get("stageout_mode", "local")
        ds = block.dataset_name
        parts = ds.strip("/").split("/")
        if stageout_mode == "test" and len(parts) == 3:
            primary, processing, tier = parts
            rule_did = f"/{primary}/{processing}-{tier}/USER"
        else:
            rule_did = ds
        rucio_account = cd.get("rucio_test_account", "")
        scope = f"user.{rucio_account}" if stageout_mode == "test" else "cms"
        try:
            rule_id = await self.rucio.create_rule(
                dataset=rule_did,
                destination=consolidation_rse,
                scope=scope,
            )
            await self.db.update_processing_block(
                block.id,
                consolidation_rule_id=rule_id,
                rucio_last_error=None,
                rucio_attempt_count=0,
            )
            logger.info(
                "Consolidation rule retry succeeded for block %s → %s: %s",
                block.id, consolidation_rse, rule_id,
            )
        except Exception as e:
            err_str = str(e).lower()
            if "duplicate" in err_str or "already exists" in err_str:
                # Rule already exists — treat as success
                logger.info(
                    "Consolidation rule already exists for block %s → %s (idempotent)",
                    block.id, consolidation_rse,
                )
                await self.db.update_processing_block(
                    block.id,
                    consolidation_rule_id="existing",
                    rucio_last_error=None,
                    rucio_attempt_count=0,
                )
                return
            await self._record_rucio_failure(block, e)
            raise RucioRegistrationError(
                f"Consolidation rule retry failed for block {block.id} → {consolidation_rse}: {e}"
            ) from e

    def _should_retry(self, block) -> bool:
        """Check if retry is allowed (within max duration, respecting backoff)."""
        if not block.last_rucio_attempt:
            return True

        now = datetime.now(timezone.utc)
        created_at = block.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        elapsed = now - created_at
        if elapsed > self.RUCIO_MAX_RETRY_DURATION:
            logger.error(
                "Block %s (%s): Rucio retry exhausted after %s. "
                "Last error: %s. Marking FAILED.",
                block.id, block.dataset_name, elapsed, block.rucio_last_error,
            )
            self._dump_failure_context(block)
            return False

        last_attempt = block.last_rucio_attempt
        if last_attempt.tzinfo is None:
            last_attempt = last_attempt.replace(tzinfo=timezone.utc)

        attempt = min(
            block.rucio_attempt_count,
            len(self.RUCIO_BACKOFF_SCHEDULE) - 1,
        )
        backoff = timedelta(seconds=self.RUCIO_BACKOFF_SCHEDULE[attempt])
        if now - last_attempt < backoff:
            return False

        return True

    async def _record_rucio_failure(self, block, error: Exception) -> None:
        """Record a Rucio call failure for later retry."""
        now = datetime.now(timezone.utc)
        await self.db.update_processing_block(
            block.id,
            last_rucio_attempt=now,
            rucio_attempt_count=(block.rucio_attempt_count or 0) + 1,
            rucio_last_error=str(error),
        )
        logger.warning(
            "Block %s (%s): Rucio call failed (attempt %d): %s",
            block.id, block.dataset_name,
            (block.rucio_attempt_count or 0) + 1, error,
        )

    def _dump_failure_context(self, block) -> None:
        """Dump full block context for operator diagnosis."""
        logger.error(
            "RUCIO FAILURE CONTEXT for block %s:\n"
            "  dataset: %s\n"
            "  block_index: %s\n"
            "  work_units: %d/%d\n"
            "  dbs_block: %s (closed=%s)\n"
            "  source_rules: %s\n"
            "  tape_rule: %s\n"
            "  attempts: %s\n"
            "  last_error: %s\n"
            "  first_attempt: %s\n"
            "  last_attempt: %s",
            block.id, block.dataset_name, block.block_index,
            len(block.completed_work_units or []), block.total_work_units,
            block.dbs_block_name, block.dbs_block_closed,
            block.source_rule_ids, block.tape_rule_id,
            block.rucio_attempt_count, block.rucio_last_error,
            block.created_at, block.last_rucio_attempt,
        )

    async def invalidate_blocks(self, workflow_id: UUID, reason: str) -> None:
        """Invalidate all blocks for catastrophic recovery.

        Deletes Rucio rules and invalidates DBS blocks.
        """
        blocks = await self.db.get_processing_blocks(workflow_id)
        for block in blocks:
            # Delete source protection rules
            for node_name, rule_id in (block.source_rule_ids or {}).items():
                try:
                    await self.rucio.delete_rule(rule_id)
                except Exception as e:
                    logger.warning("Failed to delete source rule %s: %s", rule_id, e)

            # Delete tape archival rule
            if block.tape_rule_id:
                try:
                    await self.rucio.delete_rule(block.tape_rule_id)
                except Exception as e:
                    logger.warning(
                        "Failed to delete tape rule %s: %s", block.tape_rule_id, e
                    )

            # Delete consolidation rule
            consolidation_rule = getattr(block, "consolidation_rule_id", None)
            if consolidation_rule:
                try:
                    await self.rucio.delete_rule(consolidation_rule)
                except Exception as e:
                    logger.warning(
                        "Failed to delete consolidation rule %s: %s",
                        consolidation_rule, e,
                    )

            # Invalidate DBS block
            if block.dbs_block_open and block.dbs_block_name:
                try:
                    await self.dbs.invalidate_block(block.dbs_block_name)
                except Exception as e:
                    logger.warning(
                        "Failed to invalidate DBS block %s: %s",
                        block.dbs_block_name, e,
                    )

            await self.db.update_processing_block(
                block.id, status=BlockStatus.INVALIDATED.value
            )

        logger.info(
            "Invalidated %d blocks for workflow %s: %s",
            len(blocks), workflow_id, reason,
        )

    async def all_blocks_archived(self, workflow_id: UUID) -> bool:
        """True if every block for this workflow has status ARCHIVED."""
        blocks = await self.db.get_processing_blocks(workflow_id)
        if not blocks:
            return True
        return all(b.status == BlockStatus.ARCHIVED.value for b in blocks)
