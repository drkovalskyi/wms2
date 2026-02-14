from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from .enums import SiteStatus


class Site(BaseModel):
    model_config = {"from_attributes": True}

    name: str
    total_slots: Optional[int] = None
    running_slots: int = 0
    pending_slots: int = 0
    status: SiteStatus = SiteStatus.ENABLED
    drain_until: Optional[datetime] = None
    thresholds: dict[str, Any] = {}
    schedd_name: Optional[str] = None
    collector_host: Optional[str] = None
    storage_element: Optional[str] = None
    storage_path: Optional[str] = None
    config_data: dict[str, Any] = {}
    updated_at: datetime
