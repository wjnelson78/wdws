"""SQLAlchemy models for the `memory` schema (Phase 2)."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PgUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from orchestrator.db.base import Base

SCHEMA = "memory"


class Episode(Base):
    __tablename__ = "episodes"
    __table_args__ = (
        Index("idx_episodes_user_time", "user_id", "occurred_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    conversation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    message_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.messages.id", ondelete="SET NULL"),
        nullable=True,
    )
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    participants: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    full_text: Mapped[str] = mapped_column(Text, nullable=False)
    linked_case_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    linked_entity_ids: Mapped[list[UUID] | None] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), nullable=True
    )
    salience: Mapped[float] = mapped_column(Numeric(4, 3), default=0.5, nullable=False)
    emotional_valence: Mapped[float | None] = mapped_column(Numeric(4, 3), default=0)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    retention_tier: Mapped[str] = mapped_column(Text, default="full", nullable=False)
    last_accessed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    access_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    shared_with_users: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), default=list, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class SemanticEntity(Base):
    __tablename__ = "semantic_entities"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "entity_type", "canonical_name", name="uniq_entities_user_canonical"
        ),
        Index("idx_entities_type", "user_id", "entity_type"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False)
    aliases: Mapped[list[str]] = mapped_column(ARRAY(Text), default=list, nullable=False)
    attributes: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_reinforced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    reference_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class SemanticClaim(Base):
    __tablename__ = "semantic_claims"
    __table_args__ = (
        Index("idx_claims_subject", "subject_id", "predicate"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.semantic_entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    predicate: Mapped[str] = mapped_column(Text, nullable=False)
    object_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.semantic_entities.id", ondelete="CASCADE"),
        nullable=True,
    )
    object_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float] = mapped_column(Numeric(4, 3), default=0.5, nullable=False)
    source_episode_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), default=list, nullable=False
    )
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    valid_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    superseded_by: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.semantic_claims.id"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class Procedure(Base):
    __tablename__ = "procedures"
    __table_args__ = (
        Index("idx_procedures_user_active", "user_id", "active", "confidence"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    trigger_conditions: Mapped[dict] = mapped_column(JSONB, nullable=False)
    steps: Mapped[list | dict] = mapped_column(JSONB, nullable=False)
    source_episode_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), default=list, nullable=False
    )
    observed_count: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    confidence: Mapped[float] = mapped_column(Numeric(4, 3), default=0.5, nullable=False)
    last_reinforced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class Affect(Base):
    __tablename__ = "affects"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "subject_type", "subject_ref", "affect_type", name="uniq_affects_subject"
        ),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    subject_type: Mapped[str] = mapped_column(Text, nullable=False)
    subject_ref: Mapped[str] = mapped_column(Text, nullable=False)
    affect_type: Mapped[str] = mapped_column(Text, nullable=False)
    intensity: Mapped[float] = mapped_column(Numeric(4, 3), default=0.5, nullable=False)
    direction: Mapped[float] = mapped_column(Numeric(4, 3), default=0, nullable=False)
    source_episode_ids: Mapped[list[UUID]] = mapped_column(
        ARRAY(PgUUID(as_uuid=True)), default=list, nullable=False
    )
    confidence: Mapped[float] = mapped_column(Numeric(4, 3), default=0.5, nullable=False)
    last_reinforced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class DreamReport(Base):
    __tablename__ = "dream_reports"
    __table_args__ = (
        Index("idx_dreams_user_time", "user_id", "created_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    episodes_processed: Mapped[int] = mapped_column(Integer, nullable=False)
    patterns_extracted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    entities_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    entities_reinforced: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    claims_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    claims_superseded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    procedures_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    procedures_reinforced: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    affects_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    episodes_compressed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    episodes_deleted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    hypotheses: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    significant_hypotheses_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )
    surfaced_to_user_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    model: Mapped[str | None] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    narrative: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class CouncilSession(Base):
    __tablename__ = "council_sessions"
    __table_args__ = (
        Index("idx_council_user_time", "user_id", "created_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    conversation_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    message_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.messages.id", ondelete="SET NULL"),
        nullable=True,
    )
    tier: Mapped[str] = mapped_column(Text, nullable=False)
    trigger_reason: Mapped[str] = mapped_column(Text, nullable=False)
    personas_convened: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    framing_prompt: Mapped[str | None] = mapped_column(Text, nullable=True)
    rounds: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    synthesis: Mapped[str | None] = mapped_column(Text, nullable=True)
    recommendation: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_accepted: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    user_response: Mapped[str | None] = mapped_column(Text, nullable=True)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 4), nullable=True)
    input_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    output_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class CouncilGateLog(Base):
    __tablename__ = "council_gate_log"
    __table_args__ = (
        Index("idx_gate_log_user_time", "user_id", "recorded_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    message_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    classifier_score: Mapped[float | None] = mapped_column(Numeric(4, 3), nullable=True)
    tier_recommended: Mapped[str | None] = mapped_column(Text, nullable=True)
    tier_actual: Mapped[str | None] = mapped_column(Text, nullable=True)
    override_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    classifier_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    council_session_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.council_sessions.id", ondelete="SET NULL"),
        nullable=True,
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class AdaptationReport(Base):
    __tablename__ = "adaptation_reports"
    __table_args__ = ({"schema": SCHEMA},)

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    metrics: Mapped[dict] = mapped_column(JSONB, nullable=False)
    observations: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    autonomous_changes: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    recommendations: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    questions_for_principal: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    watch_list: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    report_markdown: Mapped[str] = mapped_column(Text, nullable=False)
    ace_document_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    surfaced_to_user_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class AdaptationChange(Base):
    __tablename__ = "adaptation_changes"
    __table_args__ = (
        Index("idx_adaptation_changes_user", "user_id", "applied_at"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    report_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.adaptation_reports.id", ondelete="SET NULL"),
        nullable=True,
    )
    parameter_path: Mapped[str] = mapped_column(Text, nullable=False)
    before_value: Mapped[Any] = mapped_column(JSONB, nullable=False)
    after_value: Mapped[Any] = mapped_column(JSONB, nullable=False)
    rationale: Mapped[str] = mapped_column(Text, nullable=False)
    authority: Mapped[str] = mapped_column(Text, nullable=False)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    reverted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AdaptationRecommendation(Base):
    __tablename__ = "adaptation_recommendations"
    __table_args__ = (
        Index("idx_adapt_recs_pending", "user_id", "status"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    report_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.adaptation_reports.id", ondelete="CASCADE"),
        nullable=False,
    )
    proposal: Mapped[dict] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, default="pending", nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )


class SystemConfig(Base):
    __tablename__ = "system_config"
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "parameter_path", name="pk_system_config"),
        {"schema": SCHEMA},
    )

    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("orchestrator.users.id", ondelete="CASCADE"),
        nullable=False,
    )
    parameter_path: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[Any] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
