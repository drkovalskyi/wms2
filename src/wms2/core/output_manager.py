"""Output Manager: post-compute pipeline for processing blocks.

Handles the output lifecycle at the processing block level: DBS file
registration, Rucio source protection, DBS block closure, and tape archival
rule creation.

Fire-and-forget design: WMS2 creates Rucio rules and moves on. It does not
poll transfer status, manage source protection lifecycle, or track tape
completion. Once a rule is created, Rucio owns the outcome. WMS2's only
follow-up responsibility is retry with backoff if a Rucio call fails.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from uuid import UUID

from wms2.adapters.base import DBSAdapter, RucioAdapter
from wms2.db.repository import Repository
from wms2.models.enums import BlockStatus

logger = logging.getLogger(__name__)


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
    ):
        self.db = repository
        self.dbs = dbs_adapter
        self.rucio = rucio_adapter

    async def handle_work_unit_completion(
        self, workflow_id: UUID, block_id: UUID, merge_info: dict
    ) -> None:
        """Called when a work unit completes.

        Registers output files in DBS and creates a Rucio source protection rule.
        """
        block = await self.db.get_processing_block(block_id)
        if not block:
            logger.error("Processing block %s not found", block_id)
            return

        # 1. Open DBS block on first work unit
        block_name = block.dbs_block_name
        if not block.dbs_block_open:
            block_name = await self.dbs.open_block(
                block.dataset_name, block.block_index
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

        # 3. Create Rucio source protection rule (no lifetime — permanent)
        rule_id = None
        try:
            rule_id = await self.rucio.create_rule(
                dataset=block.dataset_name,
                destination=merge_info.get("site", "local"),
            )
        except Exception as e:
            await self._record_rucio_failure(block, e)
            logger.warning(
                "Source protection failed for block %s, will retry: %s",
                block.id, e,
            )

        # 4. Update block state
        node_name = merge_info.get("node_name", "unknown")
        source_rules = dict(block.source_rule_ids or {})
        if rule_id:
            source_rules[node_name] = rule_id
        completed = list(block.completed_work_units or []) + [node_name]

        await self.db.update_processing_block(
            block.id,
            completed_work_units=completed,
            source_rule_ids=source_rules,
        )

        # 5. Check if block is now complete
        if len(completed) >= block.total_work_units:
            await self._complete_block(block)

    async def _complete_block(self, block) -> None:
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

        # Create tape archival rule
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
            logger.warning(
                "Tape archival rule failed for block %s, will retry: %s",
                block.id, e,
            )

    async def process_blocks_for_workflow(self, workflow_id: UUID) -> None:
        """Retry blocks with failed Rucio calls.

        Called by the Lifecycle Manager every cycle.
        """
        blocks = await self.db.get_processing_blocks(workflow_id)
        for block in blocks:
            try:
                if block.status == BlockStatus.COMPLETE.value:
                    # Tape rule not yet created — retry
                    await self._retry_tape_archival(block)
                elif block.status == BlockStatus.OPEN.value and block.rucio_last_error:
                    # Source protection failed — retry
                    await self._retry_source_protection(block)
            except Exception:
                logger.exception("Error retrying block %s", block.id)

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

    async def _retry_source_protection(self, block) -> None:
        """Retry failed source protection rule creation with backoff."""
        if not self._should_retry(block):
            return
        try:
            rule_id = await self.rucio.create_rule(
                dataset=block.dataset_name,
                destination="local",
            )
            # Clear error state on success
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
            # Mark as failed — can't use create_task in sync, caller handles
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
