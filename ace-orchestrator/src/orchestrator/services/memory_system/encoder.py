"""Post-turn memory encoder.

Milestone 2.1 scope: build an episode record + embedding from one turn.
Entity / claim / procedure / affect extraction lands in Milestone 2.2.

Runs as a background task — must not block the user's response. Failures log
loudly but never raise to the calling chat handler.
"""
from __future__ import annotations

import asyncio
import json
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from anthropic import AsyncAnthropic
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import text
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from orchestrator.config import get_settings
from orchestrator.db.memory_models import Episode
from orchestrator.db.session import MemorySessionLocal
from orchestrator.logging_config import get_logger
from orchestrator.services.memory_system.embedding_client import (
    EmbeddingUnavailable,
    get_embedding_client,
)
from orchestrator.services.memory_system.semantic_resolver import (
    CandidateClaim,
    CandidateEntity,
    ResolutionResult,
    resolve_turn,
)

_log = get_logger(__name__)

_PROMPT_PATH = pathlib.Path(__file__).parent / "prompts" / "encoder.md"
_ENCODER_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")


class EncodedEntity(BaseModel):
    entity_type: str = Field(..., min_length=1)
    canonical_name: str = Field(..., min_length=1)
    aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class EncodedClaim(BaseModel):
    subject: str = Field(..., min_length=1)
    subject_type: str = Field(..., min_length=1)
    predicate: str = Field(..., min_length=1)
    object: str | None = None
    object_type: str | None = None
    object_value: str | None = None
    confidence: float = Field(0.5, ge=0.0, le=1.0)


class EncodedEpisode(BaseModel):
    summary: str = Field(..., min_length=1)
    full_text: str = Field(..., min_length=1)
    domain: str | None = None
    linked_case_id: str | None = None
    salience: float = Field(..., ge=0.0, le=1.0)
    emotional_valence: float = Field(0.0, ge=-1.0, le=1.0)
    participants: list[str] = Field(default_factory=list)
    entities: list[EncodedEntity] = Field(default_factory=list)
    claims: list[EncodedClaim] = Field(default_factory=list)


@dataclass
class EncoderInput:
    user_id: UUID
    user_identifier: str
    conversation_id: UUID
    message_id: UUID | None
    user_message: str
    assistant_response: str
    domain: str | None
    active_case_id: str | None
    categories: list[str]
    tool_invocations: list[dict[str, Any]] | None = None
    occurred_at: datetime | None = None


class MemoryEncoder:
    """Sonnet-backed episode encoder."""

    def __init__(self, api_key: str | None = None) -> None:
        settings = get_settings()
        self._client = AsyncAnthropic(api_key=api_key or settings.anthropic_api_key)
        self._model = settings.encoder_model
        self._timeout = settings.encoder_timeout_seconds
        self._max_attempts = settings.encoder_retry_max_attempts

    async def encode_and_persist(self, payload: EncoderInput) -> Episode | None:
        """Run the encoder + embed + write the episode. Never raises."""
        try:
            encoded = await self._call_encoder(payload)
        except Exception as exc:
            _log.warning(
                "encoder_call_failed",
                conversation_id=str(payload.conversation_id),
                error=type(exc).__name__,
                detail=str(exc)[:300],
            )
            return None

        try:
            embedding = await get_embedding_client().embed(encoded.full_text)
        except EmbeddingUnavailable as exc:
            _log.warning(
                "encoder_embedding_unavailable",
                conversation_id=str(payload.conversation_id),
                error=str(exc)[:200],
            )
            embedding = None

        try:
            episode, resolution = await self._persist(payload, encoded, embedding)
        except Exception as exc:
            _log.exception(
                "encoder_persist_failed",
                conversation_id=str(payload.conversation_id),
                error=str(exc),
            )
            return None

        log_kwargs = {
            "episode_id": str(episode.id),
            "conversation_id": str(payload.conversation_id),
            "salience": float(episode.salience),
            "domain": episode.domain,
            "embedded": embedding is not None,
        }
        if resolution is not None:
            log_kwargs.update(
                {
                    "entities_inserted": resolution.entity_inserts,
                    "entities_reinforced": resolution.entity_reinforcements,
                    "claims_inserted": resolution.claim_inserts,
                    "claims_reinforced": resolution.claim_reinforcements,
                    "claims_superseded": resolution.claim_supersessions,
                }
            )
        _log.info("encoder_episode_written", **log_kwargs)
        return episode

    async def _call_encoder(self, payload: EncoderInput) -> EncodedEpisode:
        user_block = self._build_user_block(payload)

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(min=0.5, max=4.0),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        ):
            with attempt:
                resp = await asyncio.wait_for(
                    self._client.messages.create(
                        model=self._model,
                        max_tokens=2000,
                        system=_ENCODER_PROMPT,
                        messages=[{"role": "user", "content": user_block}],
                    ),
                    timeout=self._timeout,
                )
        text_blocks = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        raw = "\n".join(text_blocks).strip()
        return self._parse_encoder_output(raw)

    @staticmethod
    def _parse_encoder_output(raw: str) -> EncodedEpisode:
        # Strip optional ```json fences.
        cleaned = raw
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError(f"encoder returned non-JSON: {raw[:200]}") from exc
        try:
            return EncodedEpisode(**data)
        except ValidationError as exc:
            raise ValueError(f"encoder JSON failed validation: {exc}") from exc

    @staticmethod
    def _build_user_block(payload: EncoderInput) -> str:
        parts = [
            "## Turn metadata",
            f"- user_identifier: {payload.user_identifier}",
            f"- conversation_id: {payload.conversation_id}",
            f"- domain: {payload.domain or '(none)'}",
            f"- active_case_id: {payload.active_case_id or '(none)'}",
            f"- categories: {', '.join(payload.categories) if payload.categories else '(none)'}",
        ]
        parts.append("")
        parts.append("## User message")
        parts.append("")
        parts.append(payload.user_message.strip())
        parts.append("")
        parts.append("## Athena response")
        parts.append("")
        parts.append(payload.assistant_response.strip())
        if payload.tool_invocations:
            parts.append("")
            parts.append("## Tool invocations")
            parts.append("")
            for ti in payload.tool_invocations[:10]:
                parts.append(f"- {ti.get('server_name', '?')}.{ti.get('tool_name', '?')}")
        parts.append("")
        parts.append("Output the JSON episode now. Nothing else.")
        return "\n".join(parts)

    @staticmethod
    async def _persist(
        payload: EncoderInput,
        encoded: EncodedEpisode,
        embedding: list[float] | None,
    ) -> tuple[Episode, ResolutionResult | None]:
        async with MemorySessionLocal() as session:
            episode = Episode(
                user_id=payload.user_id,
                conversation_id=payload.conversation_id,
                message_id=payload.message_id,
                occurred_at=payload.occurred_at or datetime.now(timezone.utc),
                event_type="conversation_turn",
                participants=encoded.participants
                or [f"user:{payload.user_identifier}", "athena"],
                domain=encoded.domain or payload.domain,
                summary=encoded.summary,
                full_text=encoded.full_text,
                linked_case_id=encoded.linked_case_id or payload.active_case_id,
                salience=encoded.salience,
                emotional_valence=encoded.emotional_valence,
                embedding=embedding,
                retention_tier="full",
            )
            session.add(episode)
            await session.flush()
            episode_id = episode.id

            resolution: ResolutionResult | None = None
            if encoded.entities or encoded.claims:
                candidate_entities = [
                    CandidateEntity(
                        entity_type=e.entity_type,
                        canonical_name=e.canonical_name,
                        aliases=list(e.aliases),
                        attributes=dict(e.attributes),
                    )
                    for e in encoded.entities
                ]
                candidate_claims = [
                    CandidateClaim(
                        subject_name=c.subject,
                        subject_type=c.subject_type,
                        predicate=c.predicate,
                        object_name=c.object,
                        object_type=c.object_type,
                        object_value=c.object_value,
                        confidence=c.confidence,
                    )
                    for c in encoded.claims
                ]
                resolution = await resolve_turn(
                    session,
                    user_id=payload.user_id,
                    candidate_entities=candidate_entities,
                    candidate_claims=candidate_claims,
                    source_episode_id=episode_id,
                )
                # Link episode → entities resolved during this turn.
                if resolution.entities:
                    episode.linked_entity_ids = [
                        e.id for e in resolution.entities.values()
                    ]
                    await session.flush()

            await session.commit()
            ep = await _refetch_episode(session, episode_id)
            return ep, resolution


async def _refetch_episode(session, episode_id: UUID) -> Episode:
    return await session.get(Episode, episode_id)


_encoder_singleton: MemoryEncoder | None = None


def get_memory_encoder() -> MemoryEncoder:
    global _encoder_singleton
    if _encoder_singleton is None:
        _encoder_singleton = MemoryEncoder()
    return _encoder_singleton


async def maybe_encode_turn(payload: EncoderInput) -> None:
    """Background-task entry point. Honors the kill switch."""
    settings = get_settings()
    if not settings.memory_encoder_enabled:
        return
    encoder = get_memory_encoder()
    await encoder.encode_and_persist(payload)
