from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class SiteBan(BaseModel):
    """API response model for a site ban."""

    model_config = {"from_attributes": True}

    id: UUID
    site_name: str
    workflow_id: Optional[UUID] = None
    reason: Optional[str] = None
    failure_data: Optional[dict] = None
    created_at: datetime
    expires_at: datetime
    removed_at: Optional[datetime] = None
    removed_by: Optional[str] = None


class SiteBanCreate(BaseModel):
    """API request body for creating a manual site ban."""

    reason: str
    workflow_id: Optional[UUID] = None
    duration_days: Optional[int] = None
