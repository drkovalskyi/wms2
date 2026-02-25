"""Add adaptive flag and round-tracking fields

Revision ID: 002
Revises: 001
Create Date: 2026-02-25
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # requests: add adaptive flag
    op.add_column("requests", sa.Column("adaptive", sa.Boolean, server_default="true"))

    # workflows: rename pilot_metrics → step_metrics
    op.alter_column("workflows", "pilot_metrics", new_column_name="step_metrics")

    # workflows: add round-tracking fields
    op.add_column("workflows", sa.Column("current_round", sa.Integer, server_default="0"))
    op.add_column("workflows", sa.Column("next_first_event", sa.Integer, server_default="1"))
    op.add_column("workflows", sa.Column("file_cursor", sa.Integer, server_default="0"))


def downgrade() -> None:
    op.drop_column("workflows", "file_cursor")
    op.drop_column("workflows", "next_first_event")
    op.drop_column("workflows", "current_round")
    op.alter_column("workflows", "step_metrics", new_column_name="pilot_metrics")
    op.drop_column("requests", "adaptive")
