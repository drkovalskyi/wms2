from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from .enums import SplittingAlgo, WorkflowStatus


class StepProfile(BaseModel):
    step_name: str
    time_per_event_sec: float
    memory_peak_mb: int
    output_size_per_event_kb: float
    threads_used: int = 1


class PilotMetrics(BaseModel):
    time_per_event_sec: float
    memory_peak_mb: int
    output_size_per_event_kb: float
    cpu_efficiency: float
    events_processed: int
    steps_profiled: list[StepProfile] = []
    recommended_events_per_job: int
    recommended_memory_mb: int
    recommended_time_per_job_sec: int


class Workflow(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    request_name: str
    input_dataset: Optional[str] = None
    splitting_algo: Optional[SplittingAlgo] = None
    splitting_params: Optional[dict[str, Any]] = None
    sandbox_url: Optional[str] = None
    config_data: Optional[dict[str, Any]] = None
    pilot_cluster_id: Optional[str] = None
    pilot_schedd: Optional[str] = None
    pilot_output_path: Optional[str] = None
    step_metrics: Optional[PilotMetrics] = None
    current_round: int = 0
    next_first_event: int = 1
    file_cursor: int = 0
    dag_id: Optional[UUID] = None
    category_throttles: dict[str, int] = {
        "Processing": 5000,
        "Merge": 100,
        "Cleanup": 50,
    }
    total_nodes: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_queued: int = 0
    nodes_running: int = 0
    status: WorkflowStatus = WorkflowStatus.NEW
    created_at: datetime
    updated_at: datetime
