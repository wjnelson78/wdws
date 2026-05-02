"""Load and render per-user context blocks for the system prompt.

Pulls the user row, active user_contexts, and active user_case_registry entries
and renders them as markdown ready for the assembler's USER_PROFILE and
ACTIVE_CASE_CONTEXT slots.

Implements the Patch 01 §8 spec: priority-sorted within each context group,
YAML flattened into readable prose/bullets (not fenced code), no loader-level
redaction (disclosure is governed by the prompt itself).

Caching: in-memory per-user, keyed on (user_id, content_hash). TTL controlled
by ``settings.user_context_cache_ttl_seconds``. Hash is derived from the users'
``updated_at`` column plus max(updated_at) of contexts and case registry —
any edit via SQL invalidates cleanly.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.config import get_settings
from orchestrator.db.models import User, UserCaseRegistry, UserContext
from orchestrator.logging_config import get_logger
from orchestrator.services.onboarding import detect_onboarding

_log = get_logger(__name__)


@dataclass
class RenderedUserContext:
    user_id: UUID
    identifier: str
    display_name: str
    preferred_name: str | None
    role: str  # convenience alias of default_session_role; kept for downstream consumers
    timezone: str
    is_onboarding: bool
    user_profile_block: str
    active_case_context_block: str
    content_hash: str
    rendered_at: datetime
    raw_user: User
    raw_contexts: list[UserContext]
    raw_cases: list[UserCaseRegistry]


class UserContextLoader:
    """In-process cache + renderer for per-user context blocks."""

    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, RenderedUserContext]] = {}

    async def load(
        self, session: AsyncSession, user_identifier: str
    ) -> RenderedUserContext:
        settings = get_settings()
        ttl = settings.user_context_cache_ttl_seconds

        user = await self._fetch_user(session, user_identifier)
        contexts = await self._fetch_contexts(session, user.id)
        cases = await self._fetch_cases(session, user.id)
        onboarding_state = await detect_onboarding(session, user)

        content_hash = _hash_state(user, contexts, cases, onboarding_state.is_onboarding)
        cache_key = f"{user.id}:{content_hash}"
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and (now - cached[0]) < ttl:
            return cached[1]

        rendered = RenderedUserContext(
            user_id=user.id,
            identifier=user.identifier,
            display_name=user.display_name,
            preferred_name=user.preferred_name,
            role=user.role,
            timezone=user.timezone,
            is_onboarding=onboarding_state.is_onboarding,
            user_profile_block=_render_user_profile(user, contexts),
            active_case_context_block=_render_active_cases(cases),
            content_hash=content_hash,
            rendered_at=datetime.now(),
            raw_user=user,
            raw_contexts=contexts,
            raw_cases=cases,
        )
        self._cache[cache_key] = (now, rendered)
        _log.info(
            "user_context_rendered",
            user_identifier=user_identifier,
            user_id=str(user.id),
            contexts=len(contexts),
            cases=len(cases),
            content_hash=content_hash,
        )
        return rendered

    def invalidate(self, user_id: UUID) -> None:
        drop = [k for k in self._cache if k.startswith(f"{user_id}:")]
        for k in drop:
            del self._cache[k]
        _log.info("user_context_cache_invalidated", user_id=str(user_id), entries=len(drop))

    @staticmethod
    async def _fetch_user(session: AsyncSession, identifier: str) -> User:
        result = await session.execute(
            select(User).where(User.identifier == identifier)
        )
        user = result.scalar_one_or_none()
        if user is None:
            raise LookupError(f"user {identifier!r} not found in orchestrator.users")
        if not user.active:
            raise LookupError(f"user {identifier!r} is inactive")
        return user

    @staticmethod
    async def _fetch_contexts(
        session: AsyncSession, user_id: UUID
    ) -> list[UserContext]:
        result = await session.execute(
            select(UserContext)
            .where(UserContext.user_id == user_id, UserContext.active.is_(True))
            .order_by(UserContext.priority.desc(), UserContext.key.asc())
        )
        return list(result.scalars().all())

    @staticmethod
    async def _fetch_cases(
        session: AsyncSession, user_id: UUID
    ) -> list[UserCaseRegistry]:
        result = await session.execute(
            select(UserCaseRegistry)
            .where(
                UserCaseRegistry.user_id == user_id,
                UserCaseRegistry.status.in_(["active", "in_preparation"]),
            )
            .order_by(
                UserCaseRegistry.priority.desc(),
                func.coalesce(
                    UserCaseRegistry.next_deadline, date(9999, 12, 31)
                ).asc(),
            )
        )
        return list(result.scalars().all())


def _hash_state(
    user: User,
    contexts: list[UserContext],
    cases: list[UserCaseRegistry],
    is_onboarding: bool = False,
) -> str:
    h = hashlib.sha256()
    h.update(str(user.updated_at).encode())
    h.update(b"|onboarding=" + (b"1" if is_onboarding else b"0"))
    for c in contexts:
        h.update(str(c.id).encode())
        h.update(str(c.updated_at).encode())
    for k in cases:
        h.update(k.case_id.encode())
        h.update(str(k.updated_at).encode())
    return h.hexdigest()[:16]


def _render_user_profile(user: User, contexts: list[UserContext]) -> str:
    lines: list[str] = [
        "## User Profile",
        "",
        f"- **Identifier:** `{user.identifier}`",
        f"- **Display name:** {user.display_name}",
    ]
    if user.preferred_name:
        lines.append(f"- **Preferred name:** {user.preferred_name}")
    if user.primary_email:
        lines.append(f"- **Primary email:** {user.primary_email}")
    if user.primary_phone:
        lines.append(f"- **Primary phone:** {user.primary_phone}")
    lines.append(f"- **Role:** {user.role}")
    if user.primary_sending_email:
        lines.append(f"- **Primary sending email:** {user.primary_sending_email}")
    if user.default_cc:
        lines.append(f"- **Default CC:** {', '.join(user.default_cc)}")
    if user.privileged_contacts:
        lines.append(
            f"- **Privileged contacts:** {', '.join(user.privileged_contacts)}"
        )
    if user.signature_image_ace_doc_id:
        lines.append(
            f"- **Signature image asset (ACE):** `{user.signature_image_ace_doc_id}`"
        )
    if user.signing_script_ace_doc_id:
        lines.append(
            f"- **Signing script asset (ACE):** `{user.signing_script_ace_doc_id}`"
        )

    if user.authorization_context:
        lines.append("")
        lines.append("### Authorization context")
        lines.append("")
        lines.append(user.authorization_context.rstrip())

    if user.signature_block:
        lines.append("")
        lines.append("### Signature block")
        lines.append("")
        lines.append("```")
        lines.append(user.signature_block.rstrip())
        lines.append("```")

    if user.preferences:
        lines.append("")
        lines.append("### Preferences")
        lines.append("")
        lines.extend(_render_mapping_as_bullets(user.preferences))

    grouped: dict[str, list[UserContext]] = {}
    for c in contexts:
        grouped.setdefault(c.context_type, []).append(c)

    if grouped:
        lines.append("")
        lines.append("### Contexts")
    for ctype in sorted(grouped.keys()):
        items = grouped[ctype]
        lines.append("")
        lines.append(f"#### {ctype} ({len(items)})")
        lines.append("")
        for c in items:
            lines.append(f"- **{c.key}** (priority {c.priority})")
            lines.extend(
                "  " + l for l in _render_mapping_as_bullets(c.content, depth=0)
            )

    return "\n".join(lines) + "\n"


def _render_active_cases(cases: list[UserCaseRegistry]) -> str:
    if not cases:
        return "## Active Matters\n\n_No active matters for this user._\n"

    lines: list[str] = ["## Active Matters", ""]
    for c in cases:
        lines.append(f"### {c.case_id} — {c.caption or '(no caption)'}")
        lines.append("")
        if c.court:
            lines.append(f"- **Court:** {c.court}")
        if c.tribunal:
            lines.append(f"- **Tribunal:** {c.tribunal}")
        if c.judge:
            lines.append(f"- **Judge:** {c.judge}")
        if c.role:
            lines.append(f"- **Role:** {c.role}")
        lines.append(f"- **Status:** {c.status}")
        if c.priority is not None:
            lines.append(f"- **Priority:** {c.priority}")
        if c.next_deadline:
            lines.append(f"- **Next deadline:** {c.next_deadline.isoformat()}")
        if c.subject:
            lines.append(f"- **Subject:** {c.subject}")
        if c.active_workstream:
            lines.append("- **Active workstream:**")
            lines.extend(
                f"  {line}" for line in c.active_workstream.strip().splitlines()
            )
        if c.opposing_counsel:
            lines.append("- **Opposing counsel:**")
            lines.extend(
                "  " + line
                for line in _render_mapping_as_bullets(c.opposing_counsel)
            )
        if c.named_defendants_include:
            lines.append("- **Named defendants include:**")
            lines.extend(
                "  " + line
                for line in _render_mapping_as_bullets(c.named_defendants_include)
            )
        if c.context_refs:
            lines.append(
                f"- **Context refs:** {', '.join(c.context_refs)}"
            )
        if c.notes:
            lines.append("- **Notes:**")
            lines.extend(f"  {line}" for line in c.notes.strip().splitlines())
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _render_mapping_as_bullets(value: Any, depth: int = 0) -> list[str]:
    indent = "  " * depth
    out: list[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(v, (dict, list)):
                out.append(f"{indent}- **{k}:**")
                out.extend(_render_mapping_as_bullets(v, depth + 1))
            elif isinstance(v, str) and "\n" in v:
                out.append(f"{indent}- **{k}:**")
                cont_indent = "  " * (depth + 1)
                out.extend(f"{cont_indent}{line}" for line in v.rstrip().splitlines())
            else:
                out.append(f"{indent}- **{k}:** {_scalar(v)}")
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, (dict, list)):
                out.append(f"{indent}-")
                out.extend(_render_mapping_as_bullets(item, depth + 1))
            elif isinstance(item, str) and "\n" in item:
                out.append(f"{indent}-")
                cont_indent = "  " * (depth + 1)
                out.extend(f"{cont_indent}{line}" for line in item.rstrip().splitlines())
            else:
                out.append(f"{indent}- {_scalar(item)}")
    else:
        out.append(f"{indent}- {_scalar(value)}")
    return out


def _scalar(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(value)
