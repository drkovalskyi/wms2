"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-02-14
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # requests
    op.create_table(
        "requests",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("request_name", sa.String(255), unique=True, nullable=False),
        sa.Column("requestor", sa.String(100), nullable=False),
        sa.Column("requestor_dn", sa.String(500)),
        sa.Column("request_data", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("payload_config", JSONB, server_default=sa.text("'{}'::jsonb")),
        sa.Column("splitting_params", JSONB),
        sa.Column("input_dataset", sa.String(500)),
        sa.Column("campaign", sa.String(100)),
        sa.Column("priority", sa.Integer, server_default="100000"),
        sa.Column("urgent", sa.Boolean, server_default="false"),
        sa.Column("production_steps", JSONB, server_default=sa.text("'[]'::jsonb")),
        sa.Column("status", sa.String(50), nullable=False, server_default="new"),
        sa.Column("status_transitions", JSONB, server_default=sa.text("'[]'::jsonb")),
        sa.Column("previous_version_request", sa.String(255), sa.ForeignKey("requests.request_name")),
        sa.Column("superseded_by_request", sa.String(255)),
        sa.Column("cleanup_policy", sa.String(50), server_default="keep_until_replaced"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # workflows
    op.create_table(
        "workflows",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("request_name", sa.String(255), sa.ForeignKey("requests.request_name")),
        sa.Column("input_dataset", sa.String(500)),
        sa.Column("splitting_algo", sa.String(50)),
        sa.Column("splitting_params", JSONB),
        sa.Column("sandbox_url", sa.Text),
        sa.Column("config_data", JSONB),
        sa.Column("pilot_cluster_id", sa.String(50)),
        sa.Column("pilot_schedd", sa.String(255)),
        sa.Column("pilot_output_path", sa.Text),
        sa.Column("pilot_metrics", JSONB),
        sa.Column("dag_id", UUID(as_uuid=True)),
        sa.Column("category_throttles", JSONB, server_default=sa.text("'{\"Processing\": 5000, \"Merge\": 100, \"Cleanup\": 50}'::jsonb")),
        sa.Column("total_nodes", sa.Integer, server_default="0"),
        sa.Column("nodes_done", sa.Integer, server_default="0"),
        sa.Column("nodes_failed", sa.Integer, server_default="0"),
        sa.Column("nodes_queued", sa.Integer, server_default="0"),
        sa.Column("nodes_running", sa.Integer, server_default="0"),
        sa.Column("status", sa.String(50), nullable=False, server_default="new"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # dags
    op.create_table(
        "dags",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("workflow_id", UUID(as_uuid=True), sa.ForeignKey("workflows.id")),
        sa.Column("dag_file_path", sa.Text, nullable=False),
        sa.Column("submit_dir", sa.Text, nullable=False),
        sa.Column("rescue_dag_path", sa.Text),
        sa.Column("dagman_cluster_id", sa.String(50)),
        sa.Column("schedd_name", sa.String(255)),
        sa.Column("parent_dag_id", UUID(as_uuid=True), sa.ForeignKey("dags.id")),
        sa.Column("stop_requested_at", sa.DateTime(timezone=True)),
        sa.Column("stop_reason", sa.Text),
        sa.Column("total_nodes", sa.Integer, nullable=False),
        sa.Column("total_edges", sa.Integer, server_default="0"),
        sa.Column("node_counts", JSONB, server_default=sa.text("'{}'::jsonb")),
        sa.Column("nodes_idle", sa.Integer, server_default="0"),
        sa.Column("nodes_running", sa.Integer, server_default="0"),
        sa.Column("nodes_done", sa.Integer, server_default="0"),
        sa.Column("nodes_failed", sa.Integer, server_default="0"),
        sa.Column("nodes_held", sa.Integer, server_default="0"),
        sa.Column("total_work_units", sa.Integer, server_default="0"),
        sa.Column("completed_work_units", JSONB, server_default=sa.text("'[]'::jsonb")),
        sa.Column("status", sa.String(50), nullable=False, server_default="planning"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("submitted_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )

    # dag_history
    op.create_table(
        "dag_history",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("dag_id", UUID(as_uuid=True), sa.ForeignKey("dags.id")),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("from_status", sa.String(50)),
        sa.Column("to_status", sa.String(50)),
        sa.Column("nodes_done", sa.Integer),
        sa.Column("nodes_failed", sa.Integer),
        sa.Column("nodes_running", sa.Integer),
        sa.Column("detail", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # output_datasets
    op.create_table(
        "output_datasets",
        sa.Column("id", UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), primary_key=True),
        sa.Column("workflow_id", UUID(as_uuid=True), sa.ForeignKey("workflows.id")),
        sa.Column("dataset_name", sa.String(500), nullable=False),
        sa.Column("source_site", sa.String(100)),
        sa.Column("dbs_registered", sa.Boolean, server_default="false"),
        sa.Column("dbs_registered_at", sa.DateTime(timezone=True)),
        sa.Column("source_rule_id", sa.String(100)),
        sa.Column("source_protected", sa.Boolean, server_default="false"),
        sa.Column("transfer_rule_ids", JSONB, server_default=sa.text("'[]'::jsonb")),
        sa.Column("transfer_destinations", JSONB, server_default=sa.text("'[]'::jsonb")),
        sa.Column("transfers_complete", sa.Boolean, server_default="false"),
        sa.Column("transfers_complete_at", sa.DateTime(timezone=True)),
        sa.Column("last_transfer_check", sa.DateTime(timezone=True)),
        sa.Column("source_released", sa.Boolean, server_default="false"),
        sa.Column("invalidated", sa.Boolean, server_default="false"),
        sa.Column("invalidated_at", sa.DateTime(timezone=True)),
        sa.Column("invalidation_reason", sa.Text),
        sa.Column("dbs_invalidated", sa.Boolean, server_default="false"),
        sa.Column("rucio_rules_deleted", sa.Boolean, server_default="false"),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # sites
    op.create_table(
        "sites",
        sa.Column("name", sa.String(100), primary_key=True),
        sa.Column("total_slots", sa.Integer),
        sa.Column("running_slots", sa.Integer, server_default="0"),
        sa.Column("pending_slots", sa.Integer, server_default="0"),
        sa.Column("status", sa.String(50), server_default="enabled"),
        sa.Column("drain_until", sa.DateTime(timezone=True)),
        sa.Column("thresholds", JSONB, server_default=sa.text("'{}'::jsonb")),
        sa.Column("schedd_name", sa.String(255)),
        sa.Column("collector_host", sa.String(255)),
        sa.Column("storage_element", sa.String(255)),
        sa.Column("storage_path", sa.Text),
        sa.Column("config_data", JSONB, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # Indexes (14 from spec)
    op.create_index("idx_dags_workflow", "dags", ["workflow_id"])
    op.create_index("idx_dags_status", "dags", ["status"])
    op.create_index(
        "idx_dags_schedd", "dags", ["schedd_name"],
        postgresql_where=sa.text("dagman_cluster_id IS NOT NULL"),
    )
    op.create_index(
        "idx_dags_parent", "dags", ["parent_dag_id"],
        postgresql_where=sa.text("parent_dag_id IS NOT NULL"),
    )
    op.create_index("idx_workflows_request", "workflows", ["request_name"])
    op.create_index("idx_workflows_status", "workflows", ["status"])
    op.create_index("idx_requests_status", "requests", ["status"])
    op.create_index("idx_requests_campaign", "requests", ["campaign"])
    op.create_index("idx_requests_priority", "requests", ["priority"])
    op.create_index(
        "idx_requests_non_terminal", "requests", ["status"],
        postgresql_where=sa.text("status NOT IN ('completed', 'failed', 'aborted')"),
    )
    op.create_index(
        "idx_requests_version_link", "requests", ["previous_version_request"],
        postgresql_where=sa.text("previous_version_request IS NOT NULL"),
    )
    op.create_index("idx_dag_history_dag", "dag_history", ["dag_id"])
    op.create_index("idx_output_datasets_workflow", "output_datasets", ["workflow_id"])
    op.create_index("idx_output_datasets_status", "output_datasets", ["status"])


def downgrade() -> None:
    op.drop_table("dag_history")
    op.drop_table("output_datasets")
    op.drop_table("dags")
    op.drop_table("workflows")
    op.drop_table("sites")
    op.drop_table("requests")
