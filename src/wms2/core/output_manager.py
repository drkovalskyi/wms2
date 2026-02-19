"""Output Manager: post-compute pipeline for completed merge group outputs.

Owns the full output lifecycle: local file validation, DBS registration,
Rucio source protection, transfer requests, and announcement.

All DBS/Rucio calls go through adapter interfaces — mock adapters make every
step trivial (immediate success), real adapters call the actual services.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from wms2.adapters.base import DBSAdapter, RucioAdapter
from wms2.config import Settings
from wms2.core.output_lfn import derive_merged_lfn_bases, lfn_to_pfn, merged_lfn_for_group
from wms2.db.repository import Repository
from wms2.models.enums import OutputStatus

logger = logging.getLogger(__name__)


class OutputManager:
    """Manages the output lifecycle for completed merge groups."""

    def __init__(
        self,
        repository: Repository,
        dbs_adapter: DBSAdapter,
        rucio_adapter: RucioAdapter,
        settings: Settings,
    ):
        self.db = repository
        self.dbs = dbs_adapter
        self.rucio = rucio_adapter
        self.settings = settings

    async def handle_merge_completion(self, workflow, merge_info: dict) -> None:
        """Called when a merge group SUBDAG completes.

        Creates PENDING OutputDataset records — one per output dataset
        (per KeepOutput=true step).

        merge_info = {"group_name": "mg_000000", "manifest": dict|None}
        """
        group_name = merge_info["group_name"]
        # Extract group index from name (mg_NNNNNN → int)
        group_index = int(group_name.split("_")[1])

        # Get output dataset info from workflow config_data
        config = workflow.config_data or {}
        output_datasets = config.get("output_datasets", [])

        if not output_datasets:
            logger.warning(
                "No output_datasets in workflow %s config_data, skipping output creation",
                workflow.id,
            )
            return

        for ds_info in output_datasets:
            dataset_name = ds_info["dataset_name"]
            merged_base = ds_info["merged_lfn_base"]

            # Build the merged LFN for this group
            lfn = merged_lfn_for_group(merged_base, group_index)
            local_path = lfn_to_pfn(self.settings.local_pfn_prefix, lfn)

            await self.db.create_output_dataset(
                workflow_id=workflow.id,
                dataset_name=dataset_name,
                source_site="local",
                status=OutputStatus.PENDING.value,
            )
            logger.info(
                "Created output record: workflow=%s dataset=%s group=%s lfn=%s",
                workflow.id, dataset_name, group_name, lfn,
            )

    async def process_outputs_for_workflow(self, workflow_id) -> None:
        """Advance all outputs through the full pipeline.

        Called every lifecycle cycle. With mock adapters, all steps succeed
        immediately — PENDING → ... → ANNOUNCED in one call.

        Priority tiers (per spec Section 4.7):
          Tier 1 (urgent): PENDING → DBS_REGISTERED → SOURCE_PROTECTED
          Tier 2 (transfers): SOURCE_PROTECTED → TRANSFERS_REQUESTED
          Tier 3 (polling): TRANSFERS_REQUESTED → TRANSFERRING → TRANSFERRED
                            → SOURCE_RELEASED → ANNOUNCED
        """
        outputs = await self.db.get_output_datasets(workflow_id)

        for output in outputs:
            try:
                await self._advance_output(output)
            except Exception:
                logger.exception("Error advancing output %s", output.id)

    async def _advance_output(self, output) -> None:
        """Advance a single output through all applicable state transitions.

        Uses local variables to track state through the pipeline so multiple
        transitions can happen in one call (important for mock adapters where
        every step succeeds immediately).
        """
        status = OutputStatus(output.status)
        transfer_rule_ids: list[str] = list(output.transfer_rule_ids or [])
        source_rule_id: str | None = output.source_rule_id

        if status == OutputStatus.PENDING:
            await self._register_in_dbs(output)
            status = OutputStatus.DBS_REGISTERED

        if status == OutputStatus.DBS_REGISTERED:
            source_rule_id = await self._protect_source(output)
            status = OutputStatus.SOURCE_PROTECTED

        if status == OutputStatus.SOURCE_PROTECTED:
            transfer_rule_ids = await self._request_transfers(output)
            status = OutputStatus.TRANSFERS_REQUESTED

        if status == OutputStatus.TRANSFERS_REQUESTED:
            transferred = await self._check_transfer_status(output, transfer_rule_ids)
            status = OutputStatus.TRANSFERRED if transferred else OutputStatus.TRANSFERRING

        if status == OutputStatus.TRANSFERRING:
            transferred = await self._check_transfer_status(output, transfer_rule_ids)
            if not transferred:
                return
            status = OutputStatus.TRANSFERRED

        if status == OutputStatus.TRANSFERRED:
            await self._release_source_protection(output, source_rule_id)
            status = OutputStatus.SOURCE_RELEASED

        if status == OutputStatus.SOURCE_RELEASED:
            await self._announce(output)

    async def _register_in_dbs(self, output) -> None:
        """PENDING → DBS_REGISTERED. Calls dbs.inject_dataset()."""
        now = datetime.now(timezone.utc)
        await self.dbs.inject_dataset({
            "dataset_name": output.dataset_name,
            "workflow_id": str(output.workflow_id),
        })
        await self.db.update_output_dataset(
            output.id,
            status=OutputStatus.DBS_REGISTERED.value,
            dbs_registered=True,
            dbs_registered_at=now,
        )
        logger.info("DBS registered: %s", output.dataset_name)

    async def _protect_source(self, output) -> str:
        """DBS_REGISTERED → SOURCE_PROTECTED. Calls rucio.create_rule() for source.

        Returns the source rule_id for immediate use by the caller.
        """
        rule_id = await self.rucio.create_rule(
            dataset=output.dataset_name,
            destination="local",
            lifetime=86400 * 30,  # 30 days
        )
        await self.db.update_output_dataset(
            output.id,
            status=OutputStatus.SOURCE_PROTECTED.value,
            source_protected=True,
            source_rule_id=rule_id,
        )
        logger.info("Source protected: %s rule=%s", output.dataset_name, rule_id)
        return rule_id

    async def _request_transfers(self, output) -> list[str]:
        """SOURCE_PROTECTED → TRANSFERS_REQUESTED. Calls rucio.create_rule() per destination.

        Returns the list of rule_ids for immediate use by the caller.
        """
        # For now, use a single default destination
        destinations = ["T1_US_FNAL_Disk"]
        rule_ids = []
        for dest in destinations:
            rule_id = await self.rucio.create_rule(
                dataset=output.dataset_name,
                destination=dest,
            )
            rule_ids.append(rule_id)

        await self.db.update_output_dataset(
            output.id,
            status=OutputStatus.TRANSFERS_REQUESTED.value,
            transfer_rule_ids=rule_ids,
            transfer_destinations=destinations,
        )
        logger.info(
            "Transfers requested: %s → %s", output.dataset_name, destinations
        )
        return rule_ids

    async def _check_transfer_status(self, output, rule_ids: list[str]) -> bool:
        """TRANSFERS_REQUESTED/TRANSFERRING → TRANSFERRED. Calls rucio.get_rule_status().

        Returns True if all transfers are complete.
        """
        all_ok = True
        for rule_id in rule_ids:
            status = await self.rucio.get_rule_status(rule_id)
            if status.get("state") != "OK":
                all_ok = False
                break

        now = datetime.now(timezone.utc)
        if all_ok:
            await self.db.update_output_dataset(
                output.id,
                status=OutputStatus.TRANSFERRED.value,
                transfers_complete=True,
                transfers_complete_at=now,
                last_transfer_check=now,
            )
            logger.info("Transfers complete: %s", output.dataset_name)
        else:
            await self.db.update_output_dataset(
                output.id,
                status=OutputStatus.TRANSFERRING.value,
                last_transfer_check=now,
            )
        return all_ok

    async def _release_source_protection(self, output, source_rule_id: str | None = None) -> None:
        """TRANSFERRED → SOURCE_RELEASED. Calls rucio.delete_rule()."""
        rule_id = source_rule_id or output.source_rule_id
        if rule_id:
            await self.rucio.delete_rule(rule_id)
        await self.db.update_output_dataset(
            output.id,
            status=OutputStatus.SOURCE_RELEASED.value,
            source_released=True,
        )
        logger.info("Source released: %s", output.dataset_name)

    async def _announce(self, output) -> None:
        """SOURCE_RELEASED → ANNOUNCED. Terminal success state."""
        await self.db.update_output_dataset(
            output.id,
            status=OutputStatus.ANNOUNCED.value,
        )
        logger.info("Announced: %s", output.dataset_name)

    def validate_local_output(self, output_lfn: str) -> bool:
        """Check if output file exists at local_pfn_prefix + lfn."""
        path = lfn_to_pfn(self.settings.local_pfn_prefix, output_lfn)
        return os.path.exists(path)

    async def get_output_summary(self, workflow_id) -> dict[str, int]:
        """Return output counts by status for display."""
        outputs = await self.db.get_output_datasets(workflow_id)
        summary: dict[str, int] = {}
        for output in outputs:
            summary[output.status] = summary.get(output.status, 0) + 1
        return summary
