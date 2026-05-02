"""multi-user tables: users, user_contexts, user_case_registry (per Patch 01)

Revision ID: 002
Revises: 001
Create Date: 2026-04-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "orchestrator"


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("identifier", sa.Text, nullable=False, unique=True),
        sa.Column("display_name", sa.Text, nullable=False),
        sa.Column("preferred_name", sa.Text, nullable=True),
        sa.Column("primary_email", sa.Text, nullable=True),
        sa.Column("primary_phone", sa.Text, nullable=True),
        sa.Column("role", sa.Text, nullable=False),
        sa.Column("authorization_context", sa.Text, nullable=True),
        sa.Column("signature_block", sa.Text, nullable=True),
        sa.Column("primary_sending_email", sa.Text, nullable=True),
        sa.Column(
            "default_cc",
            postgresql.ARRAY(sa.Text),
            nullable=False,
            server_default=sa.text("ARRAY[]::TEXT[]"),
        ),
        sa.Column("signature_image_ace_doc_id", sa.Text, nullable=True),
        sa.Column("signing_script_ace_doc_id", sa.Text, nullable=True),
        sa.Column(
            "preferences",
            postgresql.JSONB,
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "privileged_contacts",
            postgresql.ARRAY(sa.Text),
            nullable=False,
            server_default=sa.text("ARRAY[]::TEXT[]"),
        ),
        sa.Column("active", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema=SCHEMA,
    )
    op.create_index("idx_users_active", "users", ["active"], schema=SCHEMA)

    op.create_table(
        "user_contexts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("context_type", sa.Text, nullable=False),
        sa.Column("key", sa.Text, nullable=False),
        sa.Column("content", postgresql.JSONB, nullable=False),
        sa.Column("priority", sa.Integer, nullable=False, server_default="50"),
        sa.Column("active", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint(
            "user_id", "context_type", "key", name="uniq_user_contexts_key"
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_user_contexts_user", "user_contexts", ["user_id", "active"], schema=SCHEMA
    )

    op.create_table(
        "user_case_registry",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("case_id", sa.Text, nullable=False),
        sa.Column("caption", sa.Text, nullable=True),
        sa.Column("court", sa.Text, nullable=True),
        sa.Column("judge", sa.Text, nullable=True),
        sa.Column("tribunal", sa.Text, nullable=True),
        sa.Column("role", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("opposing_counsel", postgresql.JSONB, nullable=True),
        sa.Column("named_defendants_include", postgresql.JSONB, nullable=True),
        sa.Column("subject", sa.Text, nullable=True),
        sa.Column("active_workstream", sa.Text, nullable=True),
        sa.Column("next_deadline", sa.Date, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("priority", sa.Integer, nullable=False, server_default="50"),
        sa.Column(
            "context_refs",
            postgresql.ARRAY(sa.Text),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("user_id", "case_id", name="pk_user_case_registry"),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_user_case_registry_status",
        "user_case_registry",
        ["user_id", "status", sa.text("priority DESC")],
        schema=SCHEMA,
    )
    op.execute(
        f"""
        CREATE INDEX idx_user_case_registry_deadline
        ON {SCHEMA}.user_case_registry (user_id, next_deadline)
        WHERE next_deadline IS NOT NULL
        """
    )

    op.add_column(
        "conversations",
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.users.id"),
            nullable=True,
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_conversations_user",
        "conversations",
        ["user_id", sa.text("updated_at DESC")],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("idx_conversations_user", table_name="conversations", schema=SCHEMA)
    op.drop_column("conversations", "user_id", schema=SCHEMA)
    op.execute(f"DROP INDEX IF EXISTS {SCHEMA}.idx_user_case_registry_deadline")
    op.drop_index(
        "idx_user_case_registry_status", table_name="user_case_registry", schema=SCHEMA
    )
    op.drop_table("user_case_registry", schema=SCHEMA)
    op.drop_index("idx_user_contexts_user", table_name="user_contexts", schema=SCHEMA)
    op.drop_table("user_contexts", schema=SCHEMA)
    op.drop_index("idx_users_active", table_name="users", schema=SCHEMA)
    op.drop_table("users", schema=SCHEMA)
