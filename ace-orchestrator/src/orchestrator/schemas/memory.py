from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EpisodeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    conversation_id: UUID | None = None
    message_id: UUID | None = None
    occurred_at: datetime
    event_type: str
    participants: list[Any] = Field(default_factory=list)
    domain: str | None = None
    summary: str
    full_text: str
    linked_case_id: str | None = None
    salience: float
    emotional_valence: float | None = None
    retention_tier: str
    last_accessed_at: datetime | None = None
    access_count: int
    created_at: datetime


class EpisodeListItem(BaseModel):
    """Lighter-weight list response — drops full_text + embedding."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    occurred_at: datetime
    event_type: str
    domain: str | None = None
    summary: str
    linked_case_id: str | None = None
    salience: float
    retention_tier: str
    access_count: int


class ExplicitSaveIn(BaseModel):
    user_identifier: str | None = None
    summary: str = Field(..., min_length=1)
    full_text: str = Field(..., min_length=1)
    domain: str | None = None
    linked_case_id: str | None = None
    salience: float = Field(0.8, ge=0.0, le=1.0)
    emotional_valence: float = Field(0.0, ge=-1.0, le=1.0)
    participants: list[str] = Field(default_factory=list)


class EntityOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    entity_type: str
    canonical_name: str
    aliases: list[str]
    attributes: dict[str, Any]
    first_seen_at: datetime
    last_reinforced_at: datetime | None = None
    reference_count: int


class ClaimOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    subject_id: UUID
    predicate: str
    object_id: UUID | None = None
    object_value: str | None = None
    confidence: float
    source_episode_ids: list[UUID]
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    superseded_by: UUID | None = None
    created_at: datetime


class ConsolidateIn(BaseModel):
    user_identifier: str | None = None
    kind: str = Field("on_demand", pattern="^(nightly|weekly_deep|on_demand)$")
    window_start: datetime | None = None
    window_end: datetime | None = None
    force_metadata_only: bool = False


class DreamReportListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    kind: str
    window_start: datetime
    window_end: datetime
    episodes_processed: int
    significant_hypotheses_count: int
    cost_usd: float | None = None
    duration_ms: int | None = None
    created_at: datetime


class DreamReportOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    kind: str
    window_start: datetime
    window_end: datetime
    episodes_processed: int
    patterns_extracted: int
    entities_created: int
    entities_reinforced: int
    claims_added: int
    claims_superseded: int
    procedures_added: int
    procedures_reinforced: int
    affects_added: int
    episodes_compressed: int
    episodes_deleted: int
    hypotheses: list[Any]
    significant_hypotheses_count: int
    surfaced_to_user_at: datetime | None = None
    cost_usd: float | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    model: str | None = None
    duration_ms: int | None = None
    narrative: str | None = None
    created_at: datetime
