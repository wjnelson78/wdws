"""Assemble the system-message block list for a single chat request.

Loads parsed v2.0 blocks from `orchestrator.system_prompt_blocks` at startup,
then composes the per-request block list according to Patch 01 §7. Output is a
list of `AssembledBlock` records with metadata; the cache strategist attaches
the actual `cache_control` markers afterward.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.db.models import SystemPromptBlock
from orchestrator.logging_config import get_logger
from orchestrator.services.role_overlay import select_role_overlay_block

_log = get_logger(__name__)

DomainModule = Literal["legal", "comms", "medical", "osint", "dochandling"]
DOMAIN_TO_BLOCK: dict[DomainModule, str] = {
    "legal": "DOMAIN_LEGAL",
    "comms": "DOMAIN_COMMS",
    "medical": "DOMAIN_MEDICAL",
    "osint": "DOMAIN_OSINT",
    "dochandling": "DOMAIN_DOCHANDLING",
}

# Match {{ALL_CAPS_TOKEN}} only — leave {{user_profile.preferred_name}}-style
# instructional refs as literal text per v2.0 design.
_TOKEN_RE = re.compile(r"\{\{([A-Z][A-Z0-9_]*)\}\}")


@dataclass(frozen=True)
class AssembledBlock:
    """One slot in the final system-message array.

    cacheable: whether the strategist may attach cache_control here.
    cache_key_part: extra string the strategist mixes into its cache-key tracking
        for telemetry (not the cache marker itself; Anthropic handles content hashing).
    """

    name: str
    text: str
    cacheable: bool
    cache_ttl: str
    cache_key_part: str = ""


class PromptAssembler:
    """In-process cache of parsed v2.0 blocks + per-request assembly."""

    def __init__(self) -> None:
        self._blocks: dict[str, SystemPromptBlock] = {}
        self._version: str | None = None

    async def load(self, session: AsyncSession, version: str) -> None:
        """Populate the in-memory cache from system_prompt_blocks for `version`."""
        result = await session.execute(
            select(SystemPromptBlock)
            .where(SystemPromptBlock.version == version)
            .order_by(SystemPromptBlock.sort_order)
        )
        rows = result.scalars().all()
        if not rows:
            raise RuntimeError(
                f"No system_prompt_blocks found for version {version!r}; "
                "did the bootstrap script run?"
            )
        self._blocks = {b.block_name: b for b in rows}
        self._version = version
        _log.info(
            "prompt_assembler_loaded",
            version=version,
            block_count=len(rows),
            block_names=list(self._blocks.keys()),
        )

    @property
    def version(self) -> str:
        if self._version is None:
            raise RuntimeError("PromptAssembler.load() has not been called")
        return self._version

    def _get(self, name: str) -> str:
        block = self._blocks.get(name)
        if block is None:
            raise KeyError(f"system_prompt_block {name!r} not loaded for version {self._version!r}")
        return block.content

    def _get_ttl(self, name: str) -> str:
        block = self._blocks.get(name)
        return block.cache_ttl if block is not None else "1h"

    def assemble(
        self,
        *,
        domain: DomainModule | None = None,
        domains: list[DomainModule] | None = None,
        role: str | None,
        user_profile_block: str | None,
        active_case_context_block: str | None,
        case_file_block: str | None,
        runtime_context: dict[str, str],
        substitutions: dict[str, str] | None = None,
        user_cache_key: str = "",
        memory_context_block: str | None = None,
        voice_context_block: str | None = None,
        is_onboarding: bool = False,
    ) -> list[AssembledBlock]:
        """Build the ordered list of system blocks for one request.

        Patch 01 §7 order:
            SYSTEM_CORE → ROLE_OVERLAY_<role> → DOMAIN_<selected> → DOMAIN_COMMS
            → USER_PROFILE → ACTIVE_CASE_CONTEXT → CASE_FILE?
            → VOICE_CONTEXT → MEMORY_CONTEXT → RUNTIME_CONTEXT

        VOICE_CONTEXT carries William's standing voice/behavior rules
        (record_feedback memories + preferences). It precedes MEMORY_CONTEXT
        because voice rules apply universally; query-relevant memories follow.
        """
        subs = dict(substitutions or {})
        out: list[AssembledBlock] = []

        out.append(
            AssembledBlock(
                name="SYSTEM_CORE",
                text=self._render(self._get("SYSTEM_CORE"), subs),
                cacheable=True,
                cache_ttl="",
            )
        )

        role_block_name = select_role_overlay_block(role)
        out.append(
            AssembledBlock(
                name=role_block_name,
                text=self._render(self._get(role_block_name), subs),
                cacheable=True,
                cache_ttl="",
            )
        )

        # Patch 02 §C.3: ONBOARDING_OVERLAY sits between role overlay and domains.
        if is_onboarding:
            try:
                onboarding_text = self._get("ONBOARDING_OVERLAY")
            except KeyError:
                _log.warning("onboarding_overlay_block_missing")
                onboarding_text = None
            if onboarding_text:
                out.append(
                    AssembledBlock(
                        name="ONBOARDING_OVERLAY",
                        text=self._render(onboarding_text, subs),
                        cacheable=True,
                        cache_ttl="",
                    )
                )

        # Patch 02 §D: accept up to 2 domains; DOMAIN_COMMS always added alongside.
        selected_domains: list[DomainModule] = []
        if domains:
            for d in domains:
                if d in DOMAIN_TO_BLOCK and d not in selected_domains:
                    selected_domains.append(d)
                if len(selected_domains) >= 2:
                    break
        elif domain is not None:
            selected_domains.append(domain)
        else:
            selected_domains.append("legal")

        # Sort for cache stability (Patch 02 §D.3).
        emit_domains = sorted(selected_domains, key=lambda d: DOMAIN_TO_BLOCK[d])
        for d in emit_domains:
            block_name = DOMAIN_TO_BLOCK[d]
            out.append(
                AssembledBlock(
                    name=block_name,
                    text=self._render(self._get(block_name), subs),
                    cacheable=True,
                    cache_ttl="",
                )
            )

        # DOMAIN_COMMS always included unless it's already one of the selected domains.
        if "comms" not in selected_domains:
            out.append(
                AssembledBlock(
                    name="DOMAIN_COMMS",
                    text=self._render(self._get("DOMAIN_COMMS"), subs),
                    cacheable=True,
                    cache_ttl="",
                )
            )

        if user_profile_block:
            out.append(
                AssembledBlock(
                    name="USER_PROFILE",
                    text=self._render(user_profile_block, subs),
                    cacheable=True,
                    cache_ttl="",
                    cache_key_part=user_cache_key,
                )
            )

        if active_case_context_block:
            out.append(
                AssembledBlock(
                    name="ACTIVE_CASE_CONTEXT",
                    text=self._render(active_case_context_block, subs),
                    cacheable=True,
                    cache_ttl="",
                    cache_key_part=user_cache_key,
                )
            )

        if case_file_block:
            out.append(
                AssembledBlock(
                    name="CASE_FILE",
                    text=case_file_block,
                    cacheable=True,
                    cache_ttl="",
                )
            )

        if voice_context_block:
            # Voice rules change rarely (only on record_feedback) but invalidate on edit.
            # Use the user-scoped cache key so two users don't share each other's voice.
            out.append(
                AssembledBlock(
                    name="VOICE_CONTEXT",
                    text=voice_context_block,
                    cacheable=True,
                    cache_ttl="1h",
                    cache_key_part=user_cache_key,
                )
            )

        if memory_context_block:
            # Phase 2 §11.1: 5m TTL because memory updates every turn invalidate this.
            out.append(
                AssembledBlock(
                    name="MEMORY_CONTEXT",
                    text=memory_context_block,
                    cacheable=True,
                    cache_ttl="5m",
                )
            )

        out.append(
            AssembledBlock(
                name="RUNTIME_CONTEXT",
                text=self._render_runtime_context(runtime_context),
                cacheable=False,
                cache_ttl="",
            )
        )

        return out

    @staticmethod
    def _render(text: str, subs: dict[str, str]) -> str:
        if not subs:
            return text

        def repl(match: re.Match[str]) -> str:
            return subs.get(match.group(1), match.group(0))

        return _TOKEN_RE.sub(repl, text)

    @staticmethod
    def _render_runtime_context(ctx: dict[str, str]) -> str:
        if not ctx:
            return "## Runtime Context\n\n(no runtime context)\n"
        lines = ["## Runtime Context", ""]
        for k, v in ctx.items():
            lines.append(f"- **{k}:** {v}")
        return "\n".join(lines) + "\n"


def select_domain_blocks(
    candidates: list[tuple[str, float]], *, max_domains: int = 2, min_secondary_confidence: float = 0.4
) -> list[DomainModule]:
    """Patch 02 §D.4: pick up to max_domains by confidence.

    candidates: ordered list of (domain_name, confidence_score) pairs.
    Returns: list of valid DomainModule values, deduped, in input order.
    """
    seen: set[str] = set()
    chosen: list[DomainModule] = []
    for d, conf in candidates:
        if d not in DOMAIN_TO_BLOCK or d in seen:
            continue
        if len(chosen) >= max_domains:
            break
        if len(chosen) > 0 and conf < min_secondary_confidence:
            break
        chosen.append(d)  # type: ignore[arg-type]
        seen.add(d)
    return chosen


def detect_domain(
    *, active_case_id: str | None, message_text: str
) -> DomainModule:
    """Heuristic domain detection per spec §7.2 + Patch 01.

    Active case → legal. Otherwise keyword sniff; default to legal.
    """
    if active_case_id:
        return "legal"
    text = message_text.lower()
    if any(
        kw in text
        for kw in (
            "medical",
            "doctor",
            "appointment",
            "provider",
            "diagnosis",
            "prescription",
            "mrn",
        )
    ):
        return "medical"
    if any(
        kw in text
        for kw in ("investigate", "background check", "osint", "person lookup", "property research")
    ):
        return "osint"
    if any(
        kw in text
        for kw in ("upload", "convert", "merge pdf", "split pdf", "sign docx", "extract from")
    ):
        return "dochandling"
    if any(
        kw in text
        for kw in (
            "case",
            "motion",
            "pleading",
            "filing",
            "court",
            "discovery",
            "complaint",
            "deposition",
        )
    ):
        return "legal"
    if any(kw in text for kw in ("email", "send", "reply", "draft a message")):
        return "comms"
    return "legal"


def default_runtime_context(now: datetime | None = None) -> dict[str, str]:
    now = now or datetime.now()
    return {
        "CURRENT_DATE": now.strftime("%Y-%m-%d"),
        "CURRENT_DATETIME": now.isoformat(timespec="seconds"),
    }
