from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from .enums import BlockStatus


class ProcessingBlock(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    workflow_id: UUID
    block_index: int
    dataset_name: str
    total_work_units: int
    completed_work_units: list[str] = []
    dbs_block_name: Optional[str] = None
    dbs_block_open: bool = False
    dbs_block_closed: bool = False
    source_rule_ids: dict[str, str] = {}
    tape_rule_id: Optional[str] = None
    last_rucio_attempt: Optional[datetime] = None
    rucio_attempt_count: int = 0
    rucio_last_error: Optional[str] = None
    status: BlockStatus = BlockStatus.OPEN
    created_at: datetime
    updated_at: datetime
