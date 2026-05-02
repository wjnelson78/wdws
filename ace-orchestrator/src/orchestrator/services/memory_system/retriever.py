"""Build the MEMORY_CONTEXT block injected into the system prompt.

Milestone 2.1 scope: recent + similarity-based retrieval over `memory.episodes`.
Semantic claims / procedures / affects come in 2.2+.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import bindparam, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.config import get_settings
from orchestrator.db.memory_models import Episode, SemanticClaim, SemanticEntity
from orchestrator.db.session import AceSessionLocal, MemorySessionLocal
from orchestrator.logging_config import get_logger
from orchestrator.services.memory_system.embedding_client import (
    EmbeddingUnavailable,
    get_embedding_client,
)

_log = get_logger(__name__)

# Conservative stop list — long enough that single-word matches like "the"
# don't trigger unrelated entity hits, short enough that proper nouns slip through.
_STOPWORDS = frozenset(
    {
        "the", "a", "an", "and", "or", "but", "for", "to", "of", "in", "on",
        "at", "by", "with", "from", "as", "is", "are", "was", "were", "be",
        "been", "being", "this", "that", "these", "those", "it", "its", "i",
        "you", "we", "they", "he", "she", "him", "her", "them", "us", "my",
        "your", "our", "their", "his", "hers",
        "what", "when", "where", "who", "whom", "whose", "why", "how",
        "do", "does", "did", "doing", "have", "has", "had", "having",
        "can", "could", "should", "would", "may", "might", "must",
        "about", "into", "out", "up", "down", "any", "all", "no", "not",
        "now", "then", "if", "so", "than", "such", "very", "just",
    }
)


def _query_tokens(text: str) -> list[str]:
    """Split a user message into proper-noun-ish tokens for entity matching."""
    import re

    raw = re.findall(r"[A-Za-z][A-Za-z0-9_'\-]{2,}", text)
    out: list[str] = []
    seen: set[str] = set()
    for tok in raw:
        lower = tok.lower()
        if lower in _STOPWORDS:
            continue
        if lower in seen:
            continue
        seen.add(lower)
        out.append(tok)
    return out[:24]


@dataclass
class CuratedMemory:
    """Row from MCP-side ``core.memories`` (William's curated facts)."""

    memory_id: int
    memory_type: str
    content: str
    importance: float
    similarity: float
    created_at: datetime


@dataclass
class VoiceRule:
    """A feedback or preference memory that applies universally to every turn."""

    memory_id: int
    memory_type: str  # "feedback" or "preference"
    content: str
    importance: float
    created_at: datetime


@dataclass
class RetrievedMemory:
    block: str
    episodes_returned: int
    similar_episodes_returned: int
    claims_returned: int
    curated_returned: int
    block_token_estimate: int
    cache_key: str


class MemoryRetriever:
    """Query-time memory injector with a small in-memory dedupe cache."""

    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, RetrievedMemory]] = {}
        self._cache_ttl_seconds = 60.0
        self._max_cache_entries = 256

    async def build_memory_context(
        self,
        *,
        user_id: UUID,
        user_message: str,
        active_case_id: str | None,
        max_recent: int = 10,
        max_similar: int = 10,
        max_claims: int = 12,
        max_curated: int = 8,
        token_budget: int = 4000,
    ) -> RetrievedMemory:
        settings = get_settings()
        if not settings.memory_retrieval_enabled:
            return RetrievedMemory(
                block="", episodes_returned=0, similar_episodes_returned=0,
                claims_returned=0, curated_returned=0,
                block_token_estimate=0, cache_key="disabled",
            )

        cache_key = self._cache_key(user_id, active_case_id, user_message)
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and (now - cached[0]) < self._cache_ttl_seconds:
            return cached[1]

        recent_task = asyncio.create_task(
            self._fetch_recent(user_id, active_case_id, max_recent)
        )
        similar_task = asyncio.create_task(
            self._fetch_similar(user_id, user_message, max_similar)
        )
        claims_task = asyncio.create_task(
            self._fetch_relevant_claims(user_id, user_message, max_claims)
        )
        curated_task = asyncio.create_task(
            self._fetch_curated_memories(user_message, max_curated)
        )
        recent, similar, claims, curated = await asyncio.gather(
            recent_task, similar_task, claims_task, curated_task
        )

        seen: set[UUID] = set()
        deduped_recent: list[Episode] = []
        for ep in recent:
            if ep.id not in seen:
                deduped_recent.append(ep)
                seen.add(ep.id)
        deduped_similar: list[Episode] = []
        for ep in similar:
            if ep.id not in seen:
                deduped_similar.append(ep)
                seen.add(ep.id)

        block, used_recent, used_similar, used_claims, used_curated = self._render(
            deduped_recent, deduped_similar, claims, curated, token_budget
        )

        if used_recent or used_similar:
            await self._touch_episodes(
                [ep.id for ep in deduped_recent[:used_recent] + deduped_similar[:used_similar]]
            )

        result = RetrievedMemory(
            block=block,
            episodes_returned=used_recent,
            similar_episodes_returned=used_similar,
            claims_returned=used_claims,
            curated_returned=used_curated,
            block_token_estimate=len(block) // 4,
            cache_key=cache_key,
        )
        self._store_in_cache(cache_key, now, result)
        _log.info(
            "memory_retrieved",
            user_id=str(user_id),
            episodes_returned=used_recent,
            similar_returned=used_similar,
            claims_returned=used_claims,
            curated_returned=used_curated,
            token_estimate=result.block_token_estimate,
        )
        return result

    @staticmethod
    async def _fetch_recent(
        user_id: UUID, active_case_id: str | None, limit: int
    ) -> list[Episode]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        async with MemorySessionLocal() as session:
            stmt = (
                select(Episode)
                .where(
                    Episode.user_id == user_id,
                    Episode.retention_tier != "deleted",
                    Episode.occurred_at >= cutoff,
                )
                .order_by(Episode.occurred_at.desc())
                .limit(limit)
            )
            if active_case_id:
                # Boost case-linked episodes by querying them first if any exist.
                case_stmt = (
                    select(Episode)
                    .where(
                        Episode.user_id == user_id,
                        Episode.retention_tier != "deleted",
                        Episode.linked_case_id == active_case_id,
                    )
                    .order_by(Episode.occurred_at.desc())
                    .limit(limit)
                )
                case_result = await session.execute(case_stmt)
                case_eps = list(case_result.scalars().all())
                recent_result = await session.execute(stmt)
                recent_eps = list(recent_result.scalars().all())
                seen = {e.id for e in case_eps}
                merged = case_eps + [e for e in recent_eps if e.id not in seen]
                return merged[:limit]
            result = await session.execute(stmt)
            return list(result.scalars().all())

    @staticmethod
    async def _fetch_similar(user_id: UUID, query_text: str, limit: int) -> list[Episode]:
        if not query_text.strip():
            return []
        try:
            query_vec = await get_embedding_client().embed(query_text)
        except EmbeddingUnavailable as exc:
            _log.warning("retriever_embedding_unavailable", error=str(exc)[:200])
            return []
        async with MemorySessionLocal() as session:
            stmt = (
                select(Episode)
                .where(
                    Episode.user_id == user_id,
                    Episode.retention_tier != "deleted",
                    Episode.embedding.is_not(None),
                )
                .order_by(Episode.embedding.cosine_distance(query_vec))
                .limit(limit)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    @staticmethod
    async def _fetch_relevant_claims(
        user_id: UUID, query_text: str, limit: int
    ) -> list[tuple[SemanticClaim, SemanticEntity, SemanticEntity | None]]:
        """Pull active claims whose subject (or object) entity name appears in the query.

        Returns rows of (claim, subject_entity, object_entity_or_None) sorted by
        descending confidence then recency. Capped at ``limit``.
        """
        if not query_text.strip():
            return []
        async with MemorySessionLocal() as session:
            tokens = _query_tokens(query_text)
            if not tokens:
                return []
            # Find candidate entities by name OR alias overlap with the message.
            entity_q = select(SemanticEntity).where(
                SemanticEntity.user_id == user_id,
                # tsvector-style fast path would be nicer; for M2.2 we OR a small set of ILIKE.
            )
            ilike_clauses = []
            for tok in tokens:
                pattern = f"%{tok}%"
                ilike_clauses.append(SemanticEntity.canonical_name.ilike(pattern))
            from sqlalchemy import or_

            entity_q = entity_q.where(or_(*ilike_clauses))
            entity_rows = list((await session.execute(entity_q)).scalars().all())
            if not entity_rows:
                return []

            entity_ids = [e.id for e in entity_rows]
            entity_by_id = {e.id: e for e in entity_rows}

            claim_q = (
                select(SemanticClaim)
                .where(
                    SemanticClaim.user_id == user_id,
                    SemanticClaim.valid_to.is_(None),
                    or_(
                        SemanticClaim.subject_id.in_(entity_ids),
                        SemanticClaim.object_id.in_(entity_ids),
                    ),
                )
                .order_by(
                    SemanticClaim.confidence.desc(),
                    SemanticClaim.updated_at.desc(),
                )
                .limit(limit)
            )
            claim_rows = list((await session.execute(claim_q)).scalars().all())

            # Hydrate object entities not already in entity_by_id.
            need_object_ids: set[UUID] = set()
            for c in claim_rows:
                if c.object_id is not None and c.object_id not in entity_by_id:
                    need_object_ids.add(c.object_id)
                if c.subject_id not in entity_by_id:
                    need_object_ids.add(c.subject_id)
            if need_object_ids:
                more = await session.execute(
                    select(SemanticEntity).where(SemanticEntity.id.in_(list(need_object_ids)))
                )
                for e in more.scalars().all():
                    entity_by_id[e.id] = e

            results: list[tuple[SemanticClaim, SemanticEntity, SemanticEntity | None]] = []
            for c in claim_rows:
                subj = entity_by_id.get(c.subject_id)
                obj = entity_by_id.get(c.object_id) if c.object_id else None
                if subj is None:
                    continue
                results.append((c, subj, obj))
            return results

    async def build_voice_context(
        self,
        *,
        user_id: UUID,
        max_rules: int = 12,
        token_budget: int = 1500,
    ) -> str:
        """Render William's universal voice rules (feedback + high-importance prefs).

        Query-independent: these rules apply on every turn regardless of topic.
        Pulled from MCP-side ``core.memories`` where memory_type is 'feedback' or
        'preference'. Ranked by importance DESC, then by recency. Returns "" when
        retrieval is disabled or no rules are present.

        Cached briefly under user_id since voice rules change rarely (records
        flow in via William's explicit ``record_feedback`` calls, not per-turn).
        """
        settings = get_settings()
        if not settings.memory_retrieval_enabled:
            return ""

        cache_key = f"voice:{user_id}"
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and (now - cached[0]) < self._cache_ttl_seconds:
            block_value = cached[1]
            return block_value.block if isinstance(block_value, RetrievedMemory) else ""

        rules = await self._fetch_voice_rules(max_rules)
        if not rules:
            self._store_in_cache(
                cache_key, now,
                RetrievedMemory(
                    block="", episodes_returned=0, similar_episodes_returned=0,
                    claims_returned=0, curated_returned=0,
                    block_token_estimate=0, cache_key=cache_key,
                ),
            )
            return ""

        chars_budget = token_budget * 4
        lines = ["## Voice & Behavior Rules", ""]
        lines.append(
            "These are William's standing rules and preferences. Apply them every turn "
            "regardless of topic. Listed by importance."
        )
        lines.append("")
        used = 0
        for r in rules:
            tag = "**feedback**" if r.memory_type == "feedback" else "preference"
            imp_pct = int(round(r.importance * 100))
            line = f"- [{tag} · imp {imp_pct}%] {r.content.strip()}"
            trial = "\n".join(lines + [line])
            if len(trial) > chars_budget:
                break
            lines.append(line)
            used += 1

        if used == 0:
            return ""
        block = "\n".join(lines).rstrip() + "\n"

        result = RetrievedMemory(
            block=block, episodes_returned=0, similar_episodes_returned=0,
            claims_returned=0, curated_returned=used,
            block_token_estimate=len(block) // 4, cache_key=cache_key,
        )
        self._store_in_cache(cache_key, now, result)
        _log.info("voice_retrieved", user_id=str(user_id), rules_returned=used,
                  token_estimate=result.block_token_estimate)
        return block

    @staticmethod
    async def _fetch_voice_rules(limit: int) -> list[VoiceRule]:
        async with AceSessionLocal() as session:
            rows = (
                await session.execute(
                    text(
                        """
                        SELECT id, memory_type, content, importance, created_at
                          FROM core.memories
                         WHERE is_active = true
                           AND superseded_by IS NULL
                           AND memory_type IN ('feedback', 'preference')
                         ORDER BY (memory_type = 'feedback') DESC,
                                  importance DESC,
                                  created_at DESC
                         LIMIT :lim
                        """
                    ),
                    {"lim": limit},
                )
            ).mappings().all()
        return [
            VoiceRule(
                memory_id=int(r["id"]),
                memory_type=str(r["memory_type"]),
                content=str(r["content"]),
                importance=float(r["importance"]) if r["importance"] is not None else 0.5,
                created_at=r["created_at"],
            )
            for r in rows
        ]

    @staticmethod
    async def _fetch_curated_memories(
        query_text: str, limit: int
    ) -> list[CuratedMemory]:
        """Pull query-relevant curated facts from MCP-side ``core.memories``.

        Bridges the wdws-side semantic memory store into the orchestrator's
        retrieval path. Excludes ``memory_type IN ('feedback', 'preference')``
        because those are universal rules that belong in the Voice block, not
        in query-relevant retrieval. Returns rows ranked by
        ``0.7·similarity + 0.3·importance``.
        """
        if not query_text.strip():
            return []
        try:
            query_vec = await get_embedding_client().embed(query_text)
        except EmbeddingUnavailable as exc:
            _log.warning("retriever_curated_embedding_unavailable", error=str(exc)[:200])
            return []

        # Inline the vector literal — built from validated floats so no SQL injection
        # surface. SQLAlchemy's named-parameter parser conflicts with `::halfvec(N)`
        # casts when the param sits adjacent to the cast operator, so inlining the
        # vector is cleaner than acrobatic bindparam typing.
        vec_literal = "'[" + ",".join(f"{v:.6f}" for v in query_vec) + "]'::halfvec(1024)"
        candidate_pool = max(limit * 4, 16)
        async with AceSessionLocal() as session:
            rows = (
                await session.execute(
                    text(
                        f"""
                        SELECT id, memory_type, content, importance, created_at,
                               (embedding <=> {vec_literal}) AS distance
                          FROM core.memories
                         WHERE is_active = true
                           AND superseded_by IS NULL
                           AND embedding IS NOT NULL
                           AND memory_type NOT IN ('feedback', 'preference')
                           AND COALESCE(source, '') <> 'morning_briefing'
                           AND COALESCE(metadata->>'type', '') <> 'daily_briefing'
                         ORDER BY embedding <=> {vec_literal}
                         LIMIT :pool
                        """
                    ),
                    {"pool": candidate_pool},
                )
            ).mappings().all()

        scored: list[CuratedMemory] = []
        for r in rows:
            similarity = max(0.0, 1.0 - float(r["distance"]))
            importance = float(r["importance"]) if r["importance"] is not None else 0.5
            scored.append(
                CuratedMemory(
                    memory_id=int(r["id"]),
                    memory_type=str(r["memory_type"]),
                    content=str(r["content"]),
                    importance=importance,
                    similarity=similarity,
                    created_at=r["created_at"],
                )
            )
        scored.sort(
            key=lambda c: 0.7 * c.similarity + 0.3 * c.importance, reverse=True
        )
        return scored[:limit]

    @staticmethod
    async def _touch_episodes(episode_ids: list[UUID]) -> None:
        if not episode_ids:
            return
        async with MemorySessionLocal() as session:
            await session.execute(
                text(
                    "UPDATE memory.episodes "
                    "SET access_count = access_count + 1, last_accessed_at = now() "
                    "WHERE id = ANY(:ids)"
                ).bindparams(bindparam("ids", value=episode_ids, expanding=False)),
                {"ids": episode_ids},
            )
            await session.commit()

    @staticmethod
    def _render(
        recent: list[Episode],
        similar: list[Episode],
        claims: list[tuple[SemanticClaim, SemanticEntity, SemanticEntity | None]],
        curated: list[CuratedMemory],
        token_budget: int,
    ) -> tuple[str, int, int, int, int]:
        """Return (block, used_recent, used_similar, used_claims, used_curated).

        Token-budget strategy: curated facts and feedback first (highest signal —
        these are deliberately recorded by William), then claims (consolidated
        triples), then similar episodes (most relevant raw history), then recent
        (least dense signal). Drop from the bottom up.
        """
        if not recent and not similar and not claims and not curated:
            return "", 0, 0, 0, 0

        chars_budget = token_budget * 4

        def _episode_lines(ep: Episode) -> list[str]:
            ts = ep.occurred_at.strftime("%Y-%m-%d %H:%M")
            domain = f" [{ep.domain}]" if ep.domain else ""
            case = f" (case {ep.linked_case_id})" if ep.linked_case_id else ""
            return [
                f"- **{ts}{domain}{case}**  salience={float(ep.salience):.2f}",
                f"  {ep.summary.strip()}",
            ]

        def _claim_line(
            claim: SemanticClaim,
            subj: SemanticEntity,
            obj: SemanticEntity | None,
        ) -> str:
            if obj is not None:
                target = f"{obj.canonical_name} ({obj.entity_type})"
            else:
                target = claim.object_value or "?"
            conf_pct = int(round(float(claim.confidence) * 100))
            return f"- **{subj.canonical_name}** ({subj.entity_type}) — `{claim.predicate}` — {target}  · conf {conf_pct}%"

        def _curated_line(c: CuratedMemory) -> str:
            imp_pct = int(round(c.importance * 100))
            type_label = c.memory_type
            if type_label == "feedback":
                type_label = "**feedback**"
            return f"- [{type_label}] {c.content.strip()}  · imp {imp_pct}%"

        rendered: list[str] = ["## Memory Context", ""]

        used_curated = 0
        if curated:
            section = ["### William's curated facts & feedback", ""]
            current = "\n".join(rendered)
            for c in curated:
                trial = section + [_curated_line(c)]
                trial_block = current + "\n" + "\n".join(trial)
                if len(trial_block) > chars_budget:
                    break
                section = trial
                used_curated += 1
            if used_curated > 0:
                section.append("")
                rendered.extend(section)

        used_claims = 0
        if claims:
            section = ["### Relevant facts (consolidated)", ""]
            current = "\n".join(rendered)
            for c, s, o in claims:
                trial = section + [_claim_line(c, s, o)]
                trial_block = current + "\n" + "\n".join(trial)
                if len(trial_block) > chars_budget:
                    break
                section = trial
                used_claims += 1
            if used_claims > 0:
                section.append("")
                rendered.extend(section)

        sections: list[tuple[str, list[Episode]]] = []
        if similar:
            sections.append(("### Relevant prior episodes (similarity)", similar))
        if recent:
            sections.append(("### Recent episodes (last 48h)", recent))

        used_counts: dict[str, int] = {h: 0 for h, _ in sections}
        for header, eps in sections:
            section_lines = [header, ""]
            current = "\n".join(rendered)
            for ep in eps:
                trial = section_lines + _episode_lines(ep)
                trial_block = current + "\n" + "\n".join(trial)
                if len(trial_block) > chars_budget:
                    break
                section_lines = trial
                used_counts[header] += 1
            if used_counts[header] > 0:
                section_lines.append("")
                rendered.extend(section_lines)

        used_similar = used_counts.get("### Relevant prior episodes (similarity)", 0)
        used_recent = used_counts.get("### Recent episodes (last 48h)", 0)

        if used_claims == 0 and used_similar == 0 and used_recent == 0 and used_curated == 0:
            return "", 0, 0, 0, 0

        block = "\n".join(rendered).rstrip() + "\n"
        return block, used_recent, used_similar, used_claims, used_curated

    @staticmethod
    def _cache_key(user_id: UUID, active_case_id: str | None, message: str) -> str:
        h = hashlib.sha256()
        h.update(str(user_id).encode())
        h.update(b"|")
        h.update((active_case_id or "").encode())
        h.update(b"|")
        h.update(message.encode())
        return h.hexdigest()[:32]

    def _store_in_cache(self, key: str, ts: float, value: RetrievedMemory) -> None:
        if len(self._cache) >= self._max_cache_entries:
            # Drop oldest. Simple LRU-ish eviction.
            oldest = min(self._cache.items(), key=lambda kv: kv[1][0])
            self._cache.pop(oldest[0], None)
        self._cache[key] = (ts, value)

    def invalidate_user(self, user_id: UUID) -> None:
        prefix = self._cache_key(user_id, None, "")[:16]
        # The cache key isn't strictly per-user-prefixed; clear everything that
        # might involve this user by hashing again with empty extras.
        # Cheap: just clear all entries — memory cache is small.
        self._cache.clear()
        _log.info("memory_retriever_cache_invalidated", user_id=str(user_id))


_retriever_singleton: MemoryRetriever | None = None


def get_memory_retriever() -> MemoryRetriever:
    global _retriever_singleton
    if _retriever_singleton is None:
        _retriever_singleton = MemoryRetriever()
    return _retriever_singleton
