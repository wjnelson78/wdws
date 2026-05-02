"""Phase 2 §8.7: dreaming engine — nightly + weekly consolidation pass.

Five-stage pipeline:
  1. Replay   — load episodes in window, render compact representation for Opus
  2. Pattern  — Opus call extracts entities, claims, procedures, affects, hypotheses
  3. Forgetting — apply per-domain retention policy
  4. Reinforcement — bump salience on accessed episodes; bump entity reference counts
  5. Hypothesis scoring — promote significant hypotheses for morning surfacing

Persists a row to ``memory.dream_reports`` with full stats.

Budget enforcement: before the Opus call, check spend so far today against
``dreaming.daily_budget_usd`` from ConfigResolver. If insufficient, downgrade
to "metadata-only" run (stages 3+4 only — no Opus call, no extracted artifacts).
"""
from __future__ import annotations

import asyncio
import json
import math
import pathlib
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

from anthropic import AsyncAnthropic
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import and_, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.config import get_settings
from orchestrator.db.memory_models import (
    Affect,
    DreamReport,
    Episode,
    Procedure,
    SemanticEntity,
)
from orchestrator.db.session import MemorySessionLocal
from orchestrator.logging_config import get_logger
from orchestrator.services.config_resolver import get_config_resolver
from orchestrator.services.memory_system.semantic_resolver import (
    CandidateClaim,
    CandidateEntity,
    resolve_turn,
)

_log = get_logger(__name__)

_PROMPT_PATH = pathlib.Path(__file__).parent / "prompts" / "consolidator.md"
_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")

DreamKind = Literal["nightly", "weekly_deep", "on_demand"]

# Conservative per-1k token rates (USD) for budget estimates. Off by a few cents
# is fine; the resolver uses these only to gate downgrades, not to bill.
_OPUS_INPUT_PER_1K = 0.015
_OPUS_OUTPUT_PER_1K = 0.075


class _DreamHypothesis(BaseModel):
    text: str = Field(..., min_length=1)
    significance: float = Field(0.0, ge=0.0, le=1.0)
    rationale: str = ""


class _DreamEntity(BaseModel):
    entity_type: str
    canonical_name: str
    aliases: list[str] = Field(default_factory=list)
    attributes: dict[str, Any] = Field(default_factory=dict)


class _DreamClaim(BaseModel):
    subject: str
    subject_type: str
    predicate: str
    object: str | None = None
    object_type: str | None = None
    object_value: str | None = None
    confidence: float = Field(0.5, ge=0.0, le=1.0)


class _DreamProcedure(BaseModel):
    name: str
    description: str = ""
    domain: str | None = None
    trigger_conditions: dict[str, Any] = Field(default_factory=dict)
    steps: list[str] = Field(default_factory=list)
    confidence: float = Field(0.5, ge=0.0, le=1.0)


class _DreamAffect(BaseModel):
    subject_type: str
    subject_ref: str
    affect_type: str
    intensity: float = Field(0.5, ge=0.0, le=1.0)
    direction: float = Field(0.0, ge=-1.0, le=1.0)
    confidence: float = Field(0.5, ge=0.0, le=1.0)


class _DreamResult(BaseModel):
    narrative: str = ""
    new_entities: list[_DreamEntity] = Field(default_factory=list)
    new_claims: list[_DreamClaim] = Field(default_factory=list)
    new_procedures: list[_DreamProcedure] = Field(default_factory=list)
    new_affects: list[_DreamAffect] = Field(default_factory=list)
    hypotheses: list[_DreamHypothesis] = Field(default_factory=list)


@dataclass
class ConsolidationOutcome:
    dream_report_id: UUID
    kind: DreamKind
    metadata_only: bool
    episodes_processed: int
    entities_created: int
    entities_reinforced: int
    claims_added: int
    claims_superseded: int
    procedures_added: int
    affects_added: int
    episodes_compressed: int
    episodes_deleted: int
    significant_hypotheses_count: int
    cost_usd: float
    duration_ms: int


@dataclass
class _RetentionTotals:
    compressed: int = 0
    deleted: int = 0


class Consolidator:
    """Run a single consolidation pass for one principal."""

    def __init__(self, api_key: str | None = None) -> None:
        settings = get_settings()
        self._client = AsyncAnthropic(api_key=api_key or settings.anthropic_api_key)
        self._model = settings.dreaming_model

    async def run(
        self,
        *,
        user_id: UUID,
        kind: DreamKind = "nightly",
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        force_metadata_only: bool = False,
    ) -> ConsolidationOutcome:
        settings = get_settings()
        if not settings.dreaming_enabled:
            raise RuntimeError("dreaming_disabled by kill switch")

        started = time.monotonic()
        now = datetime.now(timezone.utc)

        if window_end is None:
            window_end = now
        if window_start is None:
            window_start = now - (timedelta(days=7) if kind == "weekly_deep" else timedelta(days=1))

        episodes = await self._load_episodes(user_id, window_start, window_end, kind)
        episode_count = len(episodes)
        _log.info(
            "consolidator_loaded_episodes",
            user_id=str(user_id),
            kind=kind,
            count=episode_count,
            window_start=window_start.isoformat(),
            window_end=window_end.isoformat(),
        )

        # Budget check
        resolver = get_config_resolver()
        daily_budget = float(await resolver.get(user_id, "dreaming.daily_budget_usd"))
        spend_today = await self._spend_today(user_id, now)
        estimated_cost = self._estimate_cost(episode_count)
        metadata_only = (
            force_metadata_only
            or episode_count == 0
            or (spend_today + estimated_cost) > daily_budget
        )
        if metadata_only and not force_metadata_only:
            _log.info(
                "consolidator_metadata_only",
                user_id=str(user_id),
                spend_today=round(spend_today, 4),
                budget=daily_budget,
                estimated_cost=estimated_cost,
                episode_count=episode_count,
            )

        # Stage 2: pattern extraction (skipped for metadata-only)
        result = _DreamResult()
        input_tokens = 0
        output_tokens = 0
        cost = 0.0
        if not metadata_only:
            try:
                result, input_tokens, output_tokens = await self._extract_patterns(
                    user_id, episodes
                )
                cost = self._cost_for(input_tokens, output_tokens)
            except Exception as exc:
                _log.exception(
                    "consolidator_extraction_failed",
                    user_id=str(user_id),
                    error=str(exc),
                )
                result = _DreamResult(narrative=f"Extraction failed: {exc}")

        # Persist new artifacts
        ent_inserts, ent_reinforces, claim_inserts, claim_supersessions = 0, 0, 0, 0
        proc_added, affect_added = 0, 0
        if not metadata_only and (
            result.new_entities or result.new_claims or result.new_procedures or result.new_affects
        ):
            (
                ent_inserts,
                ent_reinforces,
                claim_inserts,
                claim_supersessions,
                proc_added,
                affect_added,
            ) = await self._persist_artifacts(user_id, result)

        # Stage 3: forgetting
        retention = await self._apply_retention(user_id, now)

        # Stage 4: reinforcement
        await self._apply_reinforcement(user_id, window_start, window_end)

        # Stage 5: hypothesis scoring (already provided as significance by Opus;
        # we just count the >= threshold ones and persist all)
        significance_threshold = float(
            await resolver.get(user_id, "dreaming.significance_threshold")
        )
        hyp_dumps = [h.model_dump() for h in result.hypotheses]
        sig_count = sum(1 for h in result.hypotheses if h.significance >= significance_threshold)

        report = await self._write_report(
            user_id=user_id,
            kind=kind,
            window_start=window_start,
            window_end=window_end,
            episode_count=episode_count,
            patterns_extracted=len(result.new_entities)
            + len(result.new_claims)
            + len(result.new_procedures)
            + len(result.new_affects)
            + len(result.hypotheses),
            entities_created=ent_inserts,
            entities_reinforced=ent_reinforces,
            claims_added=claim_inserts,
            claims_superseded=claim_supersessions,
            procedures_added=proc_added,
            affects_added=affect_added,
            episodes_compressed=retention.compressed,
            episodes_deleted=retention.deleted,
            hypotheses=hyp_dumps,
            significant_hypotheses_count=sig_count,
            narrative=result.narrative,
            cost_usd=cost,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            model=None if metadata_only else self._model,
            duration_ms=int((time.monotonic() - started) * 1000),
        )

        return ConsolidationOutcome(
            dream_report_id=report.id,
            kind=kind,
            metadata_only=metadata_only,
            episodes_processed=episode_count,
            entities_created=ent_inserts,
            entities_reinforced=ent_reinforces,
            claims_added=claim_inserts,
            claims_superseded=claim_supersessions,
            procedures_added=proc_added,
            affects_added=affect_added,
            episodes_compressed=retention.compressed,
            episodes_deleted=retention.deleted,
            significant_hypotheses_count=sig_count,
            cost_usd=cost,
            duration_ms=report.duration_ms or 0,
        )

    @staticmethod
    async def _load_episodes(
        user_id: UUID,
        window_start: datetime,
        window_end: datetime,
        kind: DreamKind,
    ) -> list[Episode]:
        async with MemorySessionLocal() as session:
            stmt = (
                select(Episode)
                .where(
                    Episode.user_id == user_id,
                    Episode.retention_tier != "deleted",
                    Episode.occurred_at >= window_start,
                    Episode.occurred_at <= window_end,
                )
                .order_by(Episode.occurred_at)
            )
            result = await session.execute(stmt)
            episodes = list(result.scalars().all())

            if kind == "weekly_deep":
                # Also pull older "untouched" full-tier episodes for deeper patterns.
                older_q = (
                    select(Episode)
                    .where(
                        Episode.user_id == user_id,
                        Episode.retention_tier == "full",
                        Episode.occurred_at < window_start,
                        # untouched: no access in last 30 days OR never accessed
                        (
                            Episode.last_accessed_at.is_(None)
                            | (Episode.last_accessed_at < window_start - timedelta(days=30))
                        ),
                    )
                    .order_by(Episode.occurred_at.desc())
                    .limit(20)
                )
                older = list((await session.execute(older_q)).scalars().all())
                episodes = list(reversed(older)) + episodes
            return episodes

    @staticmethod
    async def _spend_today(user_id: UUID, now: datetime) -> float:
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        async with MemorySessionLocal() as session:
            stmt = select(func.coalesce(func.sum(DreamReport.cost_usd), 0)).where(
                DreamReport.user_id == user_id,
                DreamReport.created_at >= start_of_day,
            )
            result = await session.execute(stmt)
            return float(result.scalar_one() or 0)

    @staticmethod
    def _estimate_cost(episode_count: int) -> float:
        # Rough: 400 input tokens per episode + 1k system prompt + 800 output tokens.
        in_tokens = 1000 + episode_count * 400
        out_tokens = 800
        return (in_tokens / 1000) * _OPUS_INPUT_PER_1K + (out_tokens / 1000) * _OPUS_OUTPUT_PER_1K

    @staticmethod
    def _cost_for(in_tokens: int, out_tokens: int) -> float:
        return (in_tokens / 1000) * _OPUS_INPUT_PER_1K + (out_tokens / 1000) * _OPUS_OUTPUT_PER_1K

    async def _extract_patterns(
        self, user_id: UUID, episodes: list[Episode]
    ) -> tuple[_DreamResult, int, int]:
        replay = self._render_replay(episodes)
        # Top entities snapshot (helps Opus avoid duplicating known ones)
        async with MemorySessionLocal() as session:
            top_entities = (
                await session.execute(
                    select(SemanticEntity)
                    .where(SemanticEntity.user_id == user_id)
                    .order_by(SemanticEntity.reference_count.desc())
                    .limit(40)
                )
            ).scalars().all()

        snapshot = "\n".join(
            f"- ({e.entity_type}) {e.canonical_name} — references={e.reference_count}"
            for e in top_entities
        )

        user_block = (
            "## Episode window\n\n"
            f"{replay}\n\n"
            "## Existing top entities (snapshot)\n\n"
            f"{snapshot or '(none)'}\n\n"
            "Output the JSON dream record now. Nothing else."
        )

        resp = await self._client.messages.create(
            model=self._model,
            max_tokens=4000,
            system=_PROMPT,
            messages=[{"role": "user", "content": user_block}],
        )
        text_blocks = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
        raw = "\n".join(text_blocks).strip()
        result = self._parse_dream_output(raw)
        usage = getattr(resp, "usage", None)
        in_tokens = getattr(usage, "input_tokens", 0) or 0
        out_tokens = getattr(usage, "output_tokens", 0) or 0
        return result, in_tokens, out_tokens

    @staticmethod
    def _parse_dream_output(raw: str) -> _DreamResult:
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise ValueError(f"dream output not valid JSON: {raw[:200]}") from exc
        try:
            return _DreamResult(**data)
        except ValidationError as exc:
            raise ValueError(f"dream output failed validation: {exc}") from exc

    @staticmethod
    def _render_replay(episodes: list[Episode]) -> str:
        if not episodes:
            return "(no episodes in window)"
        lines: list[str] = []
        for ep in episodes:
            ts = ep.occurred_at.strftime("%Y-%m-%d %H:%M")
            domain = ep.domain or "—"
            case = f" case={ep.linked_case_id}" if ep.linked_case_id else ""
            lines.append(
                f"### {ts} [{domain}] salience={float(ep.salience):.2f}{case}\n"
                f"summary: {ep.summary.strip()}\n"
                f"detail: {ep.full_text.strip()}"
            )
        return "\n\n".join(lines)

    async def _persist_artifacts(
        self, user_id: UUID, result: _DreamResult
    ) -> tuple[int, int, int, int, int, int]:
        # Use a synthetic source-episode for resolver bookkeeping. Dream-derived
        # claims need a source_episode_ids entry; we use the most recent episode
        # as the anchor since it represents the "moment of consolidation."
        async with MemorySessionLocal() as session:
            anchor_q = (
                select(Episode.id)
                .where(Episode.user_id == user_id)
                .order_by(Episode.occurred_at.desc())
                .limit(1)
            )
            anchor_id = (await session.execute(anchor_q)).scalar_one_or_none()
            if anchor_id is None:
                # No episodes yet — skip resolver-routed inserts.
                return 0, 0, 0, 0, 0, 0

            cand_entities = [
                CandidateEntity(
                    entity_type=e.entity_type,
                    canonical_name=e.canonical_name,
                    aliases=list(e.aliases),
                    attributes=dict(e.attributes),
                )
                for e in result.new_entities
            ]
            cand_claims = [
                CandidateClaim(
                    subject_name=c.subject,
                    subject_type=c.subject_type,
                    predicate=c.predicate,
                    object_name=c.object,
                    object_type=c.object_type,
                    object_value=c.object_value,
                    confidence=c.confidence,
                )
                for c in result.new_claims
            ]
            res = await resolve_turn(
                session,
                user_id=user_id,
                candidate_entities=cand_entities,
                candidate_claims=cand_claims,
                source_episode_id=anchor_id,
            )

            now = datetime.now(timezone.utc)
            proc_added = 0
            for proc in result.new_procedures:
                # Naive insert — M2.4 will add procedure dedup/reinforcement.
                session.add(
                    Procedure(
                        user_id=user_id,
                        name=proc.name,
                        description=proc.description,
                        domain=proc.domain,
                        trigger_conditions=proc.trigger_conditions or {},
                        steps=proc.steps or [],
                        source_episode_ids=[anchor_id],
                        observed_count=1,
                        confidence=proc.confidence,
                        last_reinforced_at=now,
                        active=True,
                    )
                )
                proc_added += 1

            affect_added = 0
            for af in result.new_affects:
                # affects has a UNIQUE (user_id, subject_type, subject_ref, affect_type).
                # Use ON CONFLICT to upsert.
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                stmt = pg_insert(Affect.__table__).values(
                    user_id=user_id,
                    subject_type=af.subject_type,
                    subject_ref=af.subject_ref,
                    affect_type=af.affect_type,
                    intensity=af.intensity,
                    direction=af.direction,
                    source_episode_ids=[anchor_id],
                    confidence=af.confidence,
                    last_reinforced_at=now,
                    active=True,
                )
                stmt = stmt.on_conflict_do_update(
                    constraint="uniq_affects_subject",
                    set_={
                        "intensity": stmt.excluded.intensity,
                        "direction": stmt.excluded.direction,
                        "confidence": stmt.excluded.confidence,
                        "last_reinforced_at": now,
                        "active": True,
                        "updated_at": text("now()"),
                    },
                )
                await session.execute(stmt)
                affect_added += 1

            await session.commit()
            return (
                res.entity_inserts,
                res.entity_reinforcements,
                res.claim_inserts,
                res.claim_supersessions,
                proc_added,
                affect_added,
            )

    async def _apply_retention(self, user_id: UUID, now: datetime) -> _RetentionTotals:
        """Stage 3 — apply per-domain retention policies via ConfigResolver."""
        totals = _RetentionTotals()
        resolver = get_config_resolver()

        # Domains we know about → policy lookup path under retention.
        domain_policy_paths = {
            "legal": "retention.legal.policy",
            "medical": "retention.medical.policy",
            "casual": "retention.casual.policy",
            "dev": "retention.dev.policy",
            None: "retention.default.policy",
        }

        async with MemorySessionLocal() as session:
            for domain, path in domain_policy_paths.items():
                policy = await resolver.get(user_id, path)
                if policy == "preserve":
                    continue
                if policy == "compress_30":
                    cutoff = now - timedelta(days=30)
                    where_compress = and_(
                        Episode.user_id == user_id,
                        Episode.domain == domain if domain else Episode.domain.is_(None),
                        Episode.retention_tier == "full",
                        Episode.occurred_at < cutoff,
                        Episode.salience < 0.3,
                    )
                    upd = (
                        update(Episode)
                        .where(where_compress)
                        .values(retention_tier="summary")
                        .returning(Episode.id)
                    )
                    rows = (await session.execute(upd)).scalars().all()
                    totals.compressed += len(rows)
                elif policy == "compress_30_delete_90":
                    cutoff_compress = now - timedelta(days=30)
                    cutoff_delete = now - timedelta(days=90)
                    upd1 = (
                        update(Episode)
                        .where(
                            Episode.user_id == user_id,
                            Episode.domain == domain if domain else Episode.domain.is_(None),
                            Episode.retention_tier == "full",
                            Episode.occurred_at < cutoff_compress,
                            Episode.salience < 0.3,
                        )
                        .values(retention_tier="summary")
                        .returning(Episode.id)
                    )
                    rows1 = (await session.execute(upd1)).scalars().all()
                    totals.compressed += len(rows1)
                    upd2 = (
                        update(Episode)
                        .where(
                            Episode.user_id == user_id,
                            Episode.domain == domain if domain else Episode.domain.is_(None),
                            Episode.retention_tier.in_(["full", "summary"]),
                            Episode.occurred_at < cutoff_delete,
                            Episode.salience < 0.2,
                        )
                        .values(retention_tier="deleted")
                        .returning(Episode.id)
                    )
                    rows2 = (await session.execute(upd2)).scalars().all()
                    totals.deleted += len(rows2)
                else:
                    _log.warning(
                        "consolidator_unknown_retention_policy",
                        user_id=str(user_id),
                        domain=domain,
                        policy=policy,
                    )
            await session.commit()
        return totals

    @staticmethod
    async def _apply_reinforcement(
        user_id: UUID, window_start: datetime, window_end: datetime
    ) -> None:
        """Stage 4 — bump salience on episodes accessed during the window."""
        async with MemorySessionLocal() as session:
            accessed = await session.execute(
                select(Episode).where(
                    Episode.user_id == user_id,
                    Episode.access_count > 0,
                    Episode.last_accessed_at.between(window_start, window_end),
                )
            )
            for ep in accessed.scalars().all():
                bump = 0.02 * math.log(max(1, ep.access_count))
                new_sal = min(1.0, float(ep.salience) + bump)
                ep.salience = new_sal
            await session.commit()

    @staticmethod
    async def _write_report(**kwargs: Any) -> DreamReport:
        async with MemorySessionLocal() as session:
            report = DreamReport(**kwargs)
            session.add(report)
            await session.flush()
            rid = report.id
            await session.commit()
            return await session.get(DreamReport, rid)


_singleton: Consolidator | None = None


def get_consolidator() -> Consolidator:
    global _singleton
    if _singleton is None:
        _singleton = Consolidator()
    return _singleton
