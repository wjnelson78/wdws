"""Read endpoints for memory.episodes + explicit-save POST.

Milestone 2.1 scope. Entity / claim / procedure / affect / dream endpoints
land in subsequent milestones.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.api.deps import get_db, require_api_key
from orchestrator.config import get_settings
from orchestrator.db.memory_models import (
    DreamReport,
    Episode,
    SemanticClaim,
    SemanticEntity,
)
from orchestrator.db.models import User
from orchestrator.db.session import MemorySessionLocal
from orchestrator.schemas.memory import (
    ClaimOut,
    ConsolidateIn,
    DreamReportListItem,
    DreamReportOut,
    EntityOut,
    EpisodeListItem,
    EpisodeOut,
    ExplicitSaveIn,
)
from orchestrator.services.memory_system.embedding_client import (
    EmbeddingUnavailable,
    get_embedding_client,
)

router = APIRouter(
    prefix="/v1/memory",
    tags=["memory"],
    dependencies=[Depends(require_api_key)],
)


async def _resolve_user_id(identifier: str, db: AsyncSession) -> UUID:
    result = await db.execute(select(User.id).where(User.identifier == identifier))
    user_id = result.scalar_one_or_none()
    if user_id is None:
        raise HTTPException(status_code=404, detail=f"user {identifier!r} not found")
    return user_id


@router.get("/episodes", response_model=list[EpisodeListItem])
async def list_episodes(
    user: str | None = None,
    since: datetime | None = None,
    domain: str | None = None,
    case_id: str | None = None,
    limit: int = 50,
    include_deleted: bool = False,
    db: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    identifier = user or settings.default_user_identifier
    user_id = await _resolve_user_id(identifier, db)
    async with MemorySessionLocal() as msession:
        stmt = select(Episode).where(Episode.user_id == user_id)
        if not include_deleted:
            stmt = stmt.where(Episode.retention_tier != "deleted")
        if since is not None:
            stmt = stmt.where(Episode.occurred_at >= since)
        if domain:
            stmt = stmt.where(Episode.domain == domain)
        if case_id:
            stmt = stmt.where(Episode.linked_case_id == case_id)
        stmt = stmt.order_by(Episode.occurred_at.desc()).limit(min(limit, 500))
        result = await msession.execute(stmt)
        rows = list(result.scalars().all())
    return rows


@router.get("/episodes/{episode_id}", response_model=EpisodeOut)
async def get_episode(episode_id: UUID):
    async with MemorySessionLocal() as msession:
        ep = await msession.get(Episode, episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="episode not found")
    return ep


@router.get("/entities", response_model=list[EntityOut])
async def list_entities(
    user: str | None = None,
    type: str | None = None,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    identifier = user or settings.default_user_identifier
    user_id = await _resolve_user_id(identifier, db)
    async with MemorySessionLocal() as msession:
        stmt = select(SemanticEntity).where(SemanticEntity.user_id == user_id)
        if type:
            stmt = stmt.where(SemanticEntity.entity_type == type)
        stmt = stmt.order_by(SemanticEntity.last_reinforced_at.desc()).limit(min(limit, 500))
        result = await msession.execute(stmt)
        return list(result.scalars().all())


@router.get("/entities/{entity_id}", response_model=EntityOut)
async def get_entity(entity_id: UUID):
    async with MemorySessionLocal() as msession:
        ent = await msession.get(SemanticEntity, entity_id)
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    return ent


@router.get("/entities/{entity_id}/claims", response_model=list[ClaimOut])
async def list_entity_claims(entity_id: UUID, include_superseded: bool = False):
    async with MemorySessionLocal() as msession:
        stmt = select(SemanticClaim).where(
            (SemanticClaim.subject_id == entity_id)
            | (SemanticClaim.object_id == entity_id)
        )
        if not include_superseded:
            stmt = stmt.where(SemanticClaim.valid_to.is_(None))
        stmt = stmt.order_by(SemanticClaim.confidence.desc(), SemanticClaim.updated_at.desc())
        result = await msession.execute(stmt)
        return list(result.scalars().all())


@router.post(
    "/episodes/save", response_model=EpisodeOut, status_code=status.HTTP_201_CREATED
)
async def explicit_save(payload: ExplicitSaveIn, db: AsyncSession = Depends(get_db)):
    settings = get_settings()
    identifier = payload.user_identifier or settings.default_user_identifier
    user_id = await _resolve_user_id(identifier, db)

    embedding: list[float] | None = None
    try:
        embedding = await get_embedding_client().embed(payload.full_text)
    except EmbeddingUnavailable:
        embedding = None

    async with MemorySessionLocal() as msession:
        ep = Episode(
            user_id=user_id,
            occurred_at=datetime.utcnow(),
            event_type="explicit_save",
            participants=payload.participants or [f"user:{identifier}"],
            domain=payload.domain,
            summary=payload.summary,
            full_text=payload.full_text,
            linked_case_id=payload.linked_case_id,
            salience=payload.salience,
            emotional_valence=payload.emotional_valence,
            embedding=embedding,
            retention_tier="full",
        )
        msession.add(ep)
        await msession.flush()
        ep_id = ep.id
        await msession.commit()
        result = await msession.execute(select(Episode).where(Episode.id == ep_id))
        return result.scalar_one()


@router.post("/consolidate", response_model=DreamReportOut)
async def trigger_consolidation(
    payload: ConsolidateIn, db: AsyncSession = Depends(get_db)
):
    from orchestrator.services.memory_system.consolidator import get_consolidator

    settings = get_settings()
    identifier = payload.user_identifier or settings.default_user_identifier
    user_id = await _resolve_user_id(identifier, db)
    consolidator = get_consolidator()
    try:
        outcome = await consolidator.run(
            user_id=user_id,
            kind=payload.kind,
            window_start=payload.window_start,
            window_end=payload.window_end,
            force_metadata_only=payload.force_metadata_only,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    async with MemorySessionLocal() as msession:
        report = await msession.get(DreamReport, outcome.dream_report_id)
    if report is None:
        raise HTTPException(status_code=500, detail="dream report missing after run")
    return report


@router.get("/dreams", response_model=list[DreamReportListItem])
async def list_dreams(
    user: str | None = None,
    kind: str | None = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    settings = get_settings()
    identifier = user or settings.default_user_identifier
    user_id = await _resolve_user_id(identifier, db)
    async with MemorySessionLocal() as msession:
        stmt = select(DreamReport).where(DreamReport.user_id == user_id)
        if kind:
            stmt = stmt.where(DreamReport.kind == kind)
        stmt = stmt.order_by(DreamReport.created_at.desc()).limit(min(limit, 200))
        result = await msession.execute(stmt)
        return list(result.scalars().all())


@router.get("/dreams/{dream_id}", response_model=DreamReportOut)
async def get_dream(dream_id: UUID):
    async with MemorySessionLocal() as msession:
        report = await msession.get(DreamReport, dream_id)
    if report is None:
        raise HTTPException(status_code=404, detail="dream report not found")
    return report
