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
    pilot_metrics: Mapped[dict | None] = mapped_column(JSONB)
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


class OutputDatasetRow(Base):
    __tablename__ = "output_datasets"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=_new_uuid)
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflows.id")
    )
    dataset_name: Mapped[str] = mapped_column(String(500), nullable=False)
    source_site: Mapped[str | None] = mapped_column(String(100))
    dbs_registered: Mapped[bool] = mapped_column(Boolean, default=False)
    dbs_registered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_rule_id: Mapped[str | None] = mapped_column(String(100))
    source_protected: Mapped[bool] = mapped_column(Boolean, default=False)
    transfer_rule_ids: Mapped[list] = mapped_column(JSONB, default=list)
    transfer_destinations: Mapped[list] = mapped_column(JSONB, default=list)
    transfers_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    transfers_complete_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_transfer_check: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_released: Mapped[bool] = mapped_column(Boolean, default=False)
    invalidated: Mapped[bool] = mapped_column(Boolean, default=False)
    invalidated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    invalidation_reason: Mapped[str | None] = mapped_column(Text)
    dbs_invalidated: Mapped[bool] = mapped_column(Boolean, default=False)
    rucio_rules_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
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
