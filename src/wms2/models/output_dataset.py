from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from .enums import OutputStatus


class OutputDataset(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    workflow_id: UUID
    dataset_name: str
    source_site: Optional[str] = None
    dbs_registered: bool = False
    dbs_registered_at: Optional[datetime] = None
    source_rule_id: Optional[str] = None
    source_protected: bool = False
    transfer_rule_ids: list[str] = []
    transfer_destinations: list[str] = []
    transfers_complete: bool = False
    transfers_complete_at: Optional[datetime] = None
    last_transfer_check: Optional[datetime] = None
    source_released: bool = False
    invalidated: bool = False
    invalidated_at: Optional[datetime] = None
    invalidation_reason: Optional[str] = None
    dbs_invalidated: bool = False
    rucio_rules_deleted: bool = False
    status: OutputStatus = OutputStatus.PENDING
    created_at: datetime
    updated_at: datetime
