"""Patch 02 §A — reframe users as principals (semantic only, no behavior change).

Adds:
  - account_type ('principal' | 'system')
  - timezone (default America/Los_Angeles)
  - idx_users_created_at
Renames:
  - role -> default_session_role (and makes nullable)

Revision ID: 004
Revises: 003
Create Date: 2026-04-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("account_type", sa.Text, nullable=False, server_default="principal"),
        schema="orchestrator",
    )
    op.create_check_constraint(
        "users_account_type_check",
        "users",
        "account_type IN ('principal', 'system')",
        schema="orchestrator",
    )

    op.alter_column(
        "users",
        "role",
        new_column_name="default_session_role",
        existing_type=sa.Text,
        nullable=True,
        schema="orchestrator",
    )

    op.add_column(
        "users",
        sa.Column(
            "timezone",
            sa.Text,
            nullable=False,
            server_default="America/Los_Angeles",
        ),
        schema="orchestrator",
    )

    op.create_index(
        "idx_users_created_at",
        "users",
        ["created_at"],
        schema="orchestrator",
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_users_created_at",
        table_name="users",
        schema="orchestrator",
        if_exists=True,
    )
    op.drop_column("users", "timezone", schema="orchestrator")
    op.alter_column(
        "users",
        "default_session_role",
        new_column_name="role",
        existing_type=sa.Text,
        nullable=False,
        schema="orchestrator",
    )
    op.drop_constraint("users_account_type_check", "users", schema="orchestrator")
    op.drop_column("users", "account_type", schema="orchestrator")
