from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from .common import StatusTransition
from .enums import CleanupPolicy, RequestStatus, SplittingAlgo


class ProductionStep(BaseModel):
    fraction: float = Field(gt=0, lt=1)
    priority: int


class RequestCreate(BaseModel):
    """API input for creating a request."""

    request_name: str
    requestor: str
    requestor_dn: str = ""
    group: str = ""
    input_dataset: str
    secondary_input_dataset: Optional[str] = None
    output_datasets: list[str] = []
    cmssw_version: str
    scram_arch: str
    global_tag: str
    memory_mb: int = 2048
    time_per_event_sec: float = 1.0
    size_per_event_kb: float = 1.5
    site_whitelist: list[str] = []
    site_blacklist: list[str] = []
    splitting_algo: SplittingAlgo
    splitting_params: dict[str, Any] = {}
    campaign: str
    processing_string: str
    acquisition_era: str
    processing_version: int = 1
    priority: int = 100000
    urgent: bool = False
    production_steps: list[ProductionStep] = []
    adaptive: bool = True
    payload_config: dict[str, Any] = {}
    cleanup_policy: CleanupPolicy = CleanupPolicy.KEEP_UNTIL_REPLACED

    @model_validator(mode="after")
    def validate_production_steps(self):
        if self.urgent and self.production_steps:
            raise ValueError("urgent and production_steps are mutually exclusive")
        if len(self.production_steps) >= 2:
            fractions = [s.fraction for s in self.production_steps]
            if fractions != sorted(fractions) or len(set(fractions)) != len(fractions):
                raise ValueError("production_steps fractions must be strictly increasing")
            priorities = [s.priority for s in self.production_steps]
            if priorities != sorted(priorities, reverse=True) or len(set(priorities)) != len(
                priorities
            ):
                raise ValueError("production_steps priorities must be strictly decreasing")
        return self


class RequestUpdate(BaseModel):
    """Partial update for a request."""

    priority: Optional[int] = None
    site_whitelist: Optional[list[str]] = None
    site_blacklist: Optional[list[str]] = None
    production_steps: Optional[list[ProductionStep]] = None
    adaptive: Optional[bool] = None
    urgent: Optional[bool] = None


class Request(BaseModel):
    """Full request model returned from DB."""

    model_config = {"from_attributes": True}

    id: UUID
    request_name: str
    requestor: str
    requestor_dn: Optional[str] = None
    request_data: dict[str, Any] = {}
    payload_config: dict[str, Any] = {}
    splitting_params: Optional[dict[str, Any]] = None
    input_dataset: Optional[str] = None
    campaign: Optional[str] = None
    priority: int = 100000
    urgent: bool = False
    production_steps: list[ProductionStep] = []
    adaptive: bool = True
    status: RequestStatus = RequestStatus.NEW
    status_transitions: list[StatusTransition] = []
    previous_version_request: Optional[str] = None
    superseded_by_request: Optional[str] = None
    cleanup_policy: CleanupPolicy = CleanupPolicy.KEEP_UNTIL_REPLACED
    created_at: datetime
    updated_at: datetime
