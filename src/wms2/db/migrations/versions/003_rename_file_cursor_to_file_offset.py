"""Rename file_cursor to file_offset

Revision ID: 003
Revises: 002
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("workflows", "file_cursor", new_column_name="file_offset")


def downgrade() -> None:
    op.alter_column("workflows", "file_offset", new_column_name="file_cursor")
