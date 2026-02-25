import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


def _utcnow():
    return datetime.now(timezone.utc)


def _new_uuid():
    return uuid.uuid4()


class RequestRow(Base):
    __tablename__ = "requests"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    request_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    requestor: Mapped[str] = mapped_column(String(100), nullable=False)
    requestor_dn: Mapped[str | None] = mapped_column(String(500))
    request_data: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    payload_config: Mapped[dict] = mapped_column(JSONB, default=dict)
    splitting_params: Mapped[dict | None] = mapped_column(JSONB)
    input_dataset: Mapped[str | None] = mapped_column(String(500))
    campaign: Mapped[str | None] = mapped_column(String(100))
    priority: Mapped[int] = mapped_column(Integer, default=100000)
    urgent: Mapped[bool] = mapped_column(Boolean, default=False)
    production_steps: Mapped[list] = mapped_column(JSONB, default=list)
    adaptive: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="new")
    status_transitions: Mapped[list] = mapped_column(JSONB, default=list)
    previous_version_request: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("requests.request_name")
    )
    superseded_by_request: Mapped[str | None] = mapped_column(String(255))
    cleanup_policy: Mapped[str] = mapped_column(String(50), default="keep_until_replaced")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class WorkflowRow(Base):
    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    request_name: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("requests.request_name")
    )
    input_dataset: Mapped[str | None] = mapped_column(String(500))
    splitting_algo: Mapped[str | None] = mapped_column(String(50))
    splitting_params: Mapped[dict | None] = mapped_column(JSONB)
    sandbox_url: Mapped[str | None] = mapped_column(Text)
    config_data: Mapped[dict | None] = mapped_column(JSONB)
    pilot_cluster_id: Mapped[str | None] = mapped_column(String(50))
    pilot_schedd: Mapped[str | None] = mapped_column(String(255))
    pilot_output_path: Mapped[str | None] = mapped_column(Text)
    step_metrics: Mapped[dict | None] = mapped_column(JSONB)
    current_round: Mapped[int] = mapped_column(Integer, default=0)
    next_first_event: Mapped[int] = mapped_column(Integer, default=1)
    file_cursor: Mapped[int] = mapped_column(Integer, default=0)
    dag_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    category_throttles: Mapped[dict] = mapped_column(
        JSONB, default=lambda: {"Processing": 5000, "Merge": 100, "Cleanup": 50}
    )
    total_nodes: Mapped[int] = mapped_column(Integer, default=0)
    nodes_done: Mapped[int] = mapped_column(Integer, default=0)
    nodes_failed: Mapped[int] = mapped_column(Integer, default=0)
    nodes_queued: Mapped[int] = mapped_column(Integer, default=0)
    nodes_running: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="new")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class DAGRow(Base):
    __tablename__ = "dags"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflows.id")
    )
    dag_file_path: Mapped[str] = mapped_column(Text, nullable=False)
    submit_dir: Mapped[str] = mapped_column(Text, nullable=False)
    rescue_dag_path: Mapped[str | None] = mapped_column(Text)
    dagman_cluster_id: Mapped[str | None] = mapped_column(String(50))
    schedd_name: Mapped[str | None] = mapped_column(String(255))
    parent_dag_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dags.id")
    )
    stop_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stop_reason: Mapped[str | None] = mapped_column(Text)
    total_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    total_edges: Mapped[int] = mapped_column(Integer, default=0)
    node_counts: Mapped[dict] = mapped_column(JSONB, default=dict)
    nodes_idle: Mapped[int] = mapped_column(Integer, default=0)
    nodes_running: Mapped[int] = mapped_column(Integer, default=0)
    nodes_done: Mapped[int] = mapped_column(Integer, default=0)
    nodes_failed: Mapped[int] = mapped_column(Integer, default=0)
    nodes_held: Mapped[int] = mapped_column(Integer, default=0)
    total_work_units: Mapped[int] = mapped_column(Integer, default=0)
    completed_work_units: Mapped[list] = mapped_column(JSONB, default=list)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="planning")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DAGHistoryRow(Base):
    __tablename__ = "dag_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    dag_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("dags.id")
    )
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    from_status: Mapped[str | None] = mapped_column(String(50))
    to_status: Mapped[str | None] = mapped_column(String(50))
    nodes_done: Mapped[int | None] = mapped_column(Integer)
    nodes_failed: Mapped[int | None] = mapped_column(Integer)
    nodes_running: Mapped[int | None] = mapped_column(Integer)
    detail: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class ProcessingBlockRow(Base):
    __tablename__ = "processing_blocks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflows.id")
    )
    block_index: Mapped[int] = mapped_column(Integer, nullable=False)
    dataset_name: Mapped[str] = mapped_column(String(500), nullable=False)
    total_work_units: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_work_units: Mapped[list] = mapped_column(JSONB, default=list)
    dbs_block_name: Mapped[str | None] = mapped_column(String(500))
    dbs_block_open: Mapped[bool] = mapped_column(Boolean, default=False)
    dbs_block_closed: Mapped[bool] = mapped_column(Boolean, default=False)
    source_rule_ids: Mapped[dict] = mapped_column(JSONB, default=dict)
    tape_rule_id: Mapped[str | None] = mapped_column(String(100))
    last_rucio_attempt: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rucio_attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    rucio_last_error: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="open")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class SiteRow(Base):
    __tablename__ = "sites"

    name: Mapped[str] = mapped_column(String(100), primary_key=True)
    total_slots: Mapped[int | None] = mapped_column(Integer)
    running_slots: Mapped[int] = mapped_column(Integer, default=0)
    pending_slots: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(50), default="enabled")
    drain_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    thresholds: Mapped[dict] = mapped_column(JSONB, default=dict)
    schedd_name: Mapped[str | None] = mapped_column(String(255))
    collector_host: Mapped[str | None] = mapped_column(String(255))
    storage_element: Mapped[str | None] = mapped_column(String(255))
    storage_path: Mapped[str | None] = mapped_column(Text)
    config_data: Mapped[dict] = mapped_column(JSONB, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
