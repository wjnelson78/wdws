"""Phase 2 §8.4: entity resolution + claim resolution helpers.

Pure async functions that take an open SQLAlchemy session and a list of
candidate entities/claims, and return the persisted IDs. Designed to run
inside the encoder's transaction so a turn's entities/claims/episode
all commit atomically.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.db.memory_models import SemanticClaim, SemanticEntity
from orchestrator.logging_config import get_logger
from orchestrator.services.memory_system.embedding_client import (
    EmbeddingUnavailable,
    get_embedding_client,
)

_log = get_logger(__name__)


@dataclass
class CandidateEntity:
    entity_type: str
    canonical_name: str
    aliases: list[str]
    attributes: dict[str, Any]


@dataclass
class CandidateClaim:
    subject_name: str
    subject_type: str
    predicate: str
    object_name: str | None
    object_type: str | None
    object_value: str | None
    confidence: float


@dataclass
class ResolvedEntity:
    id: UUID
    canonical_name: str
    entity_type: str
    inserted: bool


@dataclass
class ResolvedClaim:
    id: UUID
    is_new: bool
    superseded_claim_id: UUID | None


@dataclass
class ResolutionResult:
    entities: dict[tuple[str, str], ResolvedEntity]  # (entity_type, canonical_name)
    claims: list[ResolvedClaim]
    entity_inserts: int
    entity_reinforcements: int
    claim_inserts: int
    claim_reinforcements: int
    claim_supersessions: int


def _normalize_predicate(p: str) -> str:
    return p.strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_name(name: str) -> str:
    return name.strip()


async def resolve_entities(
    session: AsyncSession,
    user_id: UUID,
    candidates: list[CandidateEntity],
) -> tuple[dict[tuple[str, str], ResolvedEntity], int, int]:
    """Lookup-or-insert each candidate entity. Merge aliases on hit."""
    resolved: dict[tuple[str, str], ResolvedEntity] = {}
    inserts = 0
    reinforces = 0

    for cand in candidates:
        entity_type = cand.entity_type.strip().lower()
        canonical = _normalize_name(cand.canonical_name)
        if not canonical:
            continue
        key = (entity_type, canonical)
        if key in resolved:
            continue

        existing = await session.execute(
            select(SemanticEntity).where(
                SemanticEntity.user_id == user_id,
                SemanticEntity.entity_type == entity_type,
                func.lower(SemanticEntity.canonical_name) == canonical.lower(),
            )
        )
        row = existing.scalar_one_or_none()

        if row is not None:
            # Merge aliases + reinforce.
            new_aliases = sorted(
                {*(row.aliases or []), *(a.strip() for a in cand.aliases if a.strip())}
            )
            row.aliases = new_aliases
            existing_attrs = dict(row.attributes or {})
            for k, v in (cand.attributes or {}).items():
                if v is None:
                    continue
                existing_attrs.setdefault(k, v)
            row.attributes = existing_attrs
            row.reference_count = (row.reference_count or 0) + 1
            row.last_reinforced_at = datetime.now(timezone.utc)
            row.updated_at = datetime.now(timezone.utc)
            await session.flush()
            resolved[key] = ResolvedEntity(
                id=row.id, canonical_name=canonical, entity_type=entity_type, inserted=False
            )
            reinforces += 1
        else:
            # Embed canonical_name + aliases for similarity search later.
            embed_text = ", ".join([canonical, *cand.aliases]) if cand.aliases else canonical
            try:
                embedding = await get_embedding_client().embed(embed_text)
            except EmbeddingUnavailable:
                embedding = None
            ent = SemanticEntity(
                user_id=user_id,
                entity_type=entity_type,
                canonical_name=canonical,
                aliases=[a.strip() for a in cand.aliases if a.strip()],
                attributes=cand.attributes or {},
                first_seen_at=datetime.now(timezone.utc),
                last_reinforced_at=datetime.now(timezone.utc),
                reference_count=1,
                embedding=embedding,
            )
            session.add(ent)
            await session.flush()
            resolved[key] = ResolvedEntity(
                id=ent.id, canonical_name=canonical, entity_type=entity_type, inserted=True
            )
            inserts += 1

    return resolved, inserts, reinforces


async def resolve_claims(
    session: AsyncSession,
    user_id: UUID,
    candidates: list[CandidateClaim],
    entity_index: dict[tuple[str, str], ResolvedEntity],
    source_episode_id: UUID,
) -> tuple[list[ResolvedClaim], int, int, int]:
    """Dedup, supersede, or insert each candidate claim.

    Dedup rule: same (user_id, subject_id, predicate, object_id_or_value).
    Supersession rule: same (user_id, subject_id, predicate) + different object.
    """
    resolved: list[ResolvedClaim] = []
    inserts = 0
    reinforces = 0
    supersessions = 0
    now = datetime.now(timezone.utc)

    for cand in candidates:
        predicate = _normalize_predicate(cand.predicate)
        if not predicate:
            continue

        subj_key = (cand.subject_type.strip().lower(), _normalize_name(cand.subject_name))
        subj = entity_index.get(subj_key)
        if subj is None:
            # Subject wasn't in this turn's entities; try the DB.
            existing_subj = await session.execute(
                select(SemanticEntity).where(
                    SemanticEntity.user_id == user_id,
                    SemanticEntity.entity_type == subj_key[0],
                    func.lower(SemanticEntity.canonical_name) == subj_key[1].lower(),
                )
            )
            subj_row = existing_subj.scalar_one_or_none()
            if subj_row is None:
                _log.info(
                    "claim_skipped_unknown_subject",
                    subject=cand.subject_name,
                    predicate=predicate,
                )
                continue
            subj = ResolvedEntity(
                id=subj_row.id,
                canonical_name=subj_row.canonical_name,
                entity_type=subj_row.entity_type,
                inserted=False,
            )

        object_id: UUID | None = None
        object_value: str | None = None
        if cand.object_name and cand.object_type:
            obj_key = (cand.object_type.strip().lower(), _normalize_name(cand.object_name))
            obj = entity_index.get(obj_key)
            if obj is None:
                existing_obj = await session.execute(
                    select(SemanticEntity).where(
                        SemanticEntity.user_id == user_id,
                        SemanticEntity.entity_type == obj_key[0],
                        func.lower(SemanticEntity.canonical_name) == obj_key[1].lower(),
                    )
                )
                obj_row = existing_obj.scalar_one_or_none()
                if obj_row is not None:
                    object_id = obj_row.id
            else:
                object_id = obj.id

        if object_id is None and cand.object_value is not None:
            object_value = str(cand.object_value).strip() or None

        if object_id is None and object_value is None:
            # Predicate w/ neither object_id nor object_value isn't useful.
            continue

        # Dedup: exact match.
        dedup_q = select(SemanticClaim).where(
            SemanticClaim.user_id == user_id,
            SemanticClaim.subject_id == subj.id,
            SemanticClaim.predicate == predicate,
            SemanticClaim.valid_to.is_(None),
        )
        if object_id is not None:
            dedup_q = dedup_q.where(SemanticClaim.object_id == object_id)
        else:
            dedup_q = dedup_q.where(
                SemanticClaim.object_id.is_(None),
                SemanticClaim.object_value == object_value,
            )
        existing_match = (await session.execute(dedup_q)).scalar_one_or_none()
        if existing_match is not None:
            new_sources = list(existing_match.source_episode_ids or [])
            if source_episode_id not in new_sources:
                new_sources.append(source_episode_id)
            existing_match.source_episode_ids = new_sources
            if cand.confidence and cand.confidence > float(existing_match.confidence or 0):
                existing_match.confidence = cand.confidence
            existing_match.updated_at = now
            await session.flush()
            resolved.append(
                ResolvedClaim(id=existing_match.id, is_new=False, superseded_claim_id=None)
            )
            reinforces += 1
            continue

        # Supersession: same subject+predicate, different object.
        contradiction_q = select(SemanticClaim).where(
            SemanticClaim.user_id == user_id,
            SemanticClaim.subject_id == subj.id,
            SemanticClaim.predicate == predicate,
            SemanticClaim.valid_to.is_(None),
        )
        contradictions = list(
            (await session.execute(contradiction_q)).scalars().all()
        )

        new_claim = SemanticClaim(
            user_id=user_id,
            subject_id=subj.id,
            predicate=predicate,
            object_id=object_id,
            object_value=object_value,
            confidence=cand.confidence,
            source_episode_ids=[source_episode_id],
            valid_from=now,
            valid_to=None,
        )
        session.add(new_claim)
        await session.flush()

        superseded_id = None
        for old in contradictions:
            old.valid_to = now
            old.superseded_by = new_claim.id
            old.updated_at = now
            superseded_id = old.id
            supersessions += 1
        await session.flush()
        resolved.append(
            ResolvedClaim(
                id=new_claim.id, is_new=True, superseded_claim_id=superseded_id
            )
        )
        inserts += 1

    return resolved, inserts, reinforces, supersessions


async def resolve_turn(
    session: AsyncSession,
    user_id: UUID,
    candidate_entities: list[CandidateEntity],
    candidate_claims: list[CandidateClaim],
    source_episode_id: UUID,
) -> ResolutionResult:
    entity_index, e_inserts, e_reinforces = await resolve_entities(
        session, user_id, candidate_entities
    )
    claims, c_inserts, c_reinforces, c_supersessions = await resolve_claims(
        session, user_id, candidate_claims, entity_index, source_episode_id
    )
    return ResolutionResult(
        entities=entity_index,
        claims=claims,
        entity_inserts=e_inserts,
        entity_reinforcements=e_reinforces,
        claim_inserts=c_inserts,
        claim_reinforcements=c_reinforces,
        claim_supersessions=c_supersessions,
    )
