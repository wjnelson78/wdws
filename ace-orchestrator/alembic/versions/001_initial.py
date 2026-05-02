"""initial orchestrator schema

Revision ID: 001
Revises:
Create Date: 2026-04-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "orchestrator"


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    op.create_table(
        "conversations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("title", sa.String, nullable=True),
        sa.Column("active_case_id", sa.String, nullable=True),
        sa.Column("domain_module", sa.String, nullable=False, server_default="legal"),
        sa.Column("mode", sa.String, nullable=False, server_default="default"),
        sa.Column("model", sa.String, nullable=False),
        sa.Column("system_prompt_version", sa.String, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("last_message_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("archived", sa.Boolean, nullable=False, server_default=sa.false()),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_conversations_updated", "conversations", [sa.text("updated_at DESC")], schema=SCHEMA
    )
    op.create_index("idx_conversations_case", "conversations", ["active_case_id"], schema=SCHEMA)

    op.create_table(
        "messages",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "conversation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String, nullable=False),
        sa.Column("content", postgresql.JSONB, nullable=False),
        sa.Column("input_tokens", sa.Integer, nullable=True),
        sa.Column("output_tokens", sa.Integer, nullable=True),
        sa.Column("cache_creation_tokens", sa.Integer, nullable=True),
        sa.Column("cache_read_tokens", sa.Integer, nullable=True),
        sa.Column("model", sa.String, nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_messages_conversation",
        "messages",
        ["conversation_id", "created_at"],
        schema=SCHEMA,
    )

    op.create_table(
        "tool_invocations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "message_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("server_name", sa.String, nullable=False),
        sa.Column("tool_name", sa.String, nullable=False),
        sa.Column("input", postgresql.JSONB, nullable=True),
        sa.Column("output", postgresql.JSONB, nullable=True),
        sa.Column("duration_ms", sa.Integer, nullable=True),
        sa.Column("succeeded", sa.Boolean, nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_tool_invocations_server_tool",
        "tool_invocations",
        ["server_name", "tool_name"],
        schema=SCHEMA,
    )

    op.create_table(
        "system_prompts",
        sa.Column("version", sa.String, primary_key=True),
        sa.Column("source_doc_id", sa.String, nullable=False),
        sa.Column("content", sa.String, nullable=False),
        sa.Column("active", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        schema=SCHEMA,
    )

    op.create_table(
        "system_prompt_blocks",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "version",
            sa.String,
            sa.ForeignKey(f"{SCHEMA}.system_prompts.version"),
            nullable=False,
        ),
        sa.Column("block_name", sa.String, nullable=False),
        sa.Column("content", sa.String, nullable=False),
        sa.Column("token_count", sa.Integer, nullable=True),
        sa.Column("cache_ttl", sa.String, nullable=False, server_default="1h"),
        sa.Column("sort_order", sa.Integer, nullable=True),
        sa.UniqueConstraint("version", "block_name", name="uq_prompt_blocks_version_name"),
        schema=SCHEMA,
    )

    op.create_table(
        "case_preloads",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("case_id", sa.String, nullable=False),
        sa.Column("content_hash", sa.String, nullable=False),
        sa.Column("assembled_text", sa.String, nullable=False),
        sa.Column("token_count", sa.Integer, nullable=False),
        sa.Column("document_count", sa.Integer, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column(
            "last_used_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        sa.Column("stale", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.UniqueConstraint("case_id", "content_hash", name="uniq_case_preloads_case_hash"),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_case_preloads_case",
        "case_preloads",
        ["case_id", sa.text("created_at DESC")],
        schema=SCHEMA,
    )

    op.create_table(
        "cache_telemetry",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "conversation_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.conversations.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("model", sa.String, nullable=False),
        sa.Column("input_tokens", sa.Integer, nullable=False),
        sa.Column("cache_creation_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("cache_read_tokens", sa.Integer, nullable=False, server_default="0"),
        sa.Column("output_tokens", sa.Integer, nullable=False),
        sa.Column(
            "recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
        ),
        schema=SCHEMA,
    )
    op.create_index(
        "idx_cache_telemetry_recorded",
        "cache_telemetry",
        [sa.text("recorded_at DESC")],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("idx_cache_telemetry_recorded", table_name="cache_telemetry", schema=SCHEMA)
    op.drop_table("cache_telemetry", schema=SCHEMA)
    op.drop_index("idx_case_preloads_case", table_name="case_preloads", schema=SCHEMA)
    op.drop_table("case_preloads", schema=SCHEMA)
    op.drop_table("system_prompt_blocks", schema=SCHEMA)
    op.drop_table("system_prompts", schema=SCHEMA)
    op.drop_index("idx_tool_invocations_server_tool", table_name="tool_invocations", schema=SCHEMA)
    op.drop_table("tool_invocations", schema=SCHEMA)
    op.drop_index("idx_messages_conversation", table_name="messages", schema=SCHEMA)
    op.drop_table("messages", schema=SCHEMA)
    op.drop_index("idx_conversations_case", table_name="conversations", schema=SCHEMA)
    op.drop_index("idx_conversations_updated", table_name="conversations", schema=SCHEMA)
    op.drop_table("conversations", schema=SCHEMA)
