from datetime import date, datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from orchestrator.db.base import Base

SCHEMA = "orchestrator"


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        Index("idx_users_active", "active"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    identifier: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    account_type: Mapped[str] = mapped_column(Text, default="principal", nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    preferred_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_phone: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_session_role: Mapped[str | None] = mapped_column(Text, nullable=True)
    timezone: Mapped[str] = mapped_column(Text, default="America/Los_Angeles", nullable=False)
    authorization_context: Mapped[str | None] = mapped_column(Text, nullable=True)
    signature_block: Mapped[str | None] = mapped_column(Text, nullable=True)
    primary_sending_email: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_cc: Mapped[list[str]] = mapped_column(
        ARRAY(Text), default=list, nullable=False
    )
    signature_image_ace_doc_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    signing_script_ace_doc_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    preferences: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    privileged_contacts: Mapped[list[str]] = mapped_column(
        ARRAY(Text), default=list, nullable=False
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    contexts: Mapped[list["UserContext"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    case_registry: Mapped[list["UserCaseRegistry"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    conversations: Mapped[list["Conversation"]] = relationship(back_populates="user")

    @property
    def role(self) -> str:
        """Patch 02 §A.4 deprecated alias. Falls back to 'principal' if column null."""
        return self.default_session_role or "principal"


class UserContext(Base):
    __tablename__ = "user_contexts"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "context_type", "key", name="uniq_user_contexts_key"
        ),
        Index("idx_user_contexts_user", "user_id", "active"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    context_type: Mapped[str] = mapped_column(Text, nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[dict] = mapped_column(JSONB, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=50, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    user: Mapped[User] = relationship(back_populates="contexts")


class UserCaseRegistry(Base):
    __tablename__ = "user_case_registry"
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "case_id", name="pk_user_case_registry"),
        Index(
            "idx_user_case_registry_status",
            "user_id",
            "status",
            "priority",
        ),
        {"schema": SCHEMA},
    )

    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    case_id: Mapped[str] = mapped_column(Text, nullable=False)
    caption: Mapped[str | None] = mapped_column(Text, nullable=True)
    court: Mapped[str | None] = mapped_column(Text, nullable=True)
    judge: Mapped[str | None] = mapped_column(Text, nullable=True)
    tribunal: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    opposing_counsel: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    named_defendants_include: Mapped[dict | list | None] = mapped_column(JSONB, nullable=True)
    subject: Mapped[str | None] = mapped_column(Text, nullable=True)
    active_workstream: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_deadline: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, default=50, nullable=False)
    context_refs: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    user: Mapped[User] = relationship(back_populates="case_registry")


class Conversation(Base):
    __tablename__ = "conversations"
    __table_args__ = (
        Index("idx_conversations_updated", "updated_at"),
        Index("idx_conversations_case", "active_case_id"),
        Index("idx_conversations_user", "user_id"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.users.id"),
        nullable=True,
    )
    title: Mapped[str | None] = mapped_column(String, nullable=True)
    active_case_id: Mapped[str | None] = mapped_column(String, nullable=True)
    domain_module: Mapped[str] = mapped_column(String, default="legal", nullable=False)
    mode: Mapped[str] = mapped_column(String, default="default", nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    system_prompt_version: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    last_message_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    archived: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    user: Mapped["User | None"] = relationship(back_populates="conversations")
    messages: Mapped[list["Message"]] = relationship(
        back_populates="conversation", cascade="all, delete-orphan", order_by="Message.created_at"
    )


class Message(Base):
    __tablename__ = "messages"
    __table_args__ = (
        Index("idx_messages_conversation", "conversation_id", "created_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    conversation_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.conversations.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cache_creation_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cache_read_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    model: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    conversation: Mapped[Conversation] = relationship(back_populates="messages")
    tool_invocations: Mapped[list["ToolInvocation"]] = relationship(
        back_populates="message", cascade="all, delete-orphan"
    )


class ToolInvocation(Base):
    __tablename__ = "tool_invocations"
    __table_args__ = (
        Index("idx_tool_invocations_server_tool", "server_name", "tool_name"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    message_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.messages.id", ondelete="CASCADE"),
        nullable=False,
    )
    server_name: Mapped[str] = mapped_column(String, nullable=False)
    tool_name: Mapped[str] = mapped_column(String, nullable=False)
    input: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    output: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    succeeded: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    message: Mapped[Message] = relationship(back_populates="tool_invocations")


class SystemPrompt(Base):
    __tablename__ = "system_prompts"
    __table_args__ = ({"schema": SCHEMA},)

    version: Mapped[str] = mapped_column(String, primary_key=True)
    source_doc_id: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[str] = mapped_column(String, nullable=False)
    active: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    blocks: Mapped[list["SystemPromptBlock"]] = relationship(
        back_populates="prompt", cascade="all, delete-orphan", order_by="SystemPromptBlock.sort_order"
    )


class SystemPromptBlock(Base):
    __tablename__ = "system_prompt_blocks"
    __table_args__ = (
        UniqueConstraint("version", "block_name", name="uq_prompt_blocks_version_name"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    version: Mapped[str] = mapped_column(
        String, ForeignKey(f"{SCHEMA}.system_prompts.version"), nullable=False
    )
    block_name: Mapped[str] = mapped_column(String, nullable=False)
    content: Mapped[str] = mapped_column(String, nullable=False)
    token_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cache_ttl: Mapped[str] = mapped_column(String, default="1h", nullable=False)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)

    prompt: Mapped[SystemPrompt] = relationship(back_populates="blocks")


class CasePreload(Base):
    __tablename__ = "case_preloads"
    __table_args__ = (
        UniqueConstraint("case_id", "content_hash", name="uniq_case_preloads_case_hash"),
        Index("idx_case_preloads_case", "case_id", "created_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    case_id: Mapped[str] = mapped_column(String, nullable=False)
    content_hash: Mapped[str] = mapped_column(String, nullable=False)
    assembled_text: Mapped[str] = mapped_column(String, nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, nullable=False)
    document_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    last_used_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    stale: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class CacheTelemetry(Base):
    __tablename__ = "cache_telemetry"
    __table_args__ = (
        Index("idx_cache_telemetry_recorded", "recorded_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    model: Mapped[str] = mapped_column(String, nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    cache_creation_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cache_read_tokens: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
