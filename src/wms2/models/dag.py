from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from .enums import DAGStatus, NodeRole


class LumiRange(BaseModel):
    run: int
    lumi_start: int
    lumi_end: int


class InputFile(BaseModel):
    lfn: str
    size_bytes: int
    events: int
    checksums: dict[str, str] = {}
    locations: list[str] = []
    parent_lfns: list[str] = []


class MergeGroup(BaseModel):
    """A set of processing nodes feeding one merge node — one SUBDAG EXTERNAL."""

    group_index: int
    processing_nodes: list[str] = []
    merge_node: Optional[str] = None
    cleanup_node: Optional[str] = None
    landing_node: Optional[str] = None


class DAGNodeSpec(BaseModel):
    """Transient planning object for DAG file generation."""

    node_name: str
    submit_file: str
    node_role: NodeRole
    input_files: list[InputFile] = []
    input_events: int = 0
    input_lumis: list[LumiRange] = []
    first_event: Optional[int] = None
    last_event: Optional[int] = None
    first_lumi: Optional[int] = None
    last_lumi: Optional[int] = None
    first_run: Optional[int] = None
    last_run: Optional[int] = None
    estimated_time_sec: int = 0
    estimated_disk_kb: int = 0
    estimated_memory_mb: int = 2048
    cores: int = 1
    possible_sites: list[str] = []
    max_retries: int = 3
    post_script: str = ""
    parent_nodes: list[str] = []
    child_nodes: list[str] = []


class DAG(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    workflow_id: UUID
    dag_file_path: str
    submit_dir: str
    rescue_dag_path: Optional[str] = None
    dagman_cluster_id: Optional[str] = None
    schedd_name: Optional[str] = None
    parent_dag_id: Optional[UUID] = None
    stop_requested_at: Optional[datetime] = None
    stop_reason: Optional[str] = None
    total_nodes: int
    total_edges: int = 0
    node_counts: dict[str, int] = {}
    nodes_idle: int = 0
    nodes_running: int = 0
    nodes_done: int = 0
    nodes_failed: int = 0
    nodes_held: int = 0
    total_work_units: int = 0
    completed_work_units: list[str] = []
    status: DAGStatus = DAGStatus.PLANNING
    created_at: datetime
    submitted_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
