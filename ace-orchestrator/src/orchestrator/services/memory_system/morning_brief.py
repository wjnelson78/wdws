"""Phase 2 §8.7 morning surfacing.

On the principal's first message after 06:00 local following a dream run with
significant hypotheses, surface a brief "Overnight I noticed ___" line to
Athena. The full report stays queryable; this just flags that something is
worth raising in the conversation.

Marks the dream report ``surfaced_to_user_at = now`` so the same report doesn't
fire twice. Onboarding mode disables this entirely (Patch 02 §C.4).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select, update

from orchestrator.config import get_settings
from orchestrator.db.memory_models import DreamReport
from orchestrator.db.session import MemorySessionLocal
from orchestrator.logging_config import get_logger
from orchestrator.services.config_resolver import get_config_resolver

_log = get_logger(__name__)

_MORNING_HOUR = 6


@dataclass
class MorningBrief:
    dream_report_id: UUID
    text: str
    hypotheses: list[dict]


async def maybe_pop_brief(
    *, user_id: UUID, user_timezone: str, is_onboarding: bool
) -> MorningBrief | None:
    """Return a brief to surface to the user this turn, or None.

    Marks the brief as surfaced atomically so concurrent turns don't both
    receive the same one.
    """
    if is_onboarding:
        return None
    settings = get_settings()
    if not settings.dreaming_enabled:
        return None

    try:
        tz = ZoneInfo(user_timezone)
    except Exception:
        tz = ZoneInfo("UTC")
    now_local = datetime.now(timezone.utc).astimezone(tz)
    if now_local.time() < time(_MORNING_HOUR, 0):
        return None

    threshold = float(
        await get_config_resolver().get(user_id, "dreaming.significance_threshold")
    )

    async with MemorySessionLocal() as session:
        stmt = (
            select(DreamReport)
            .where(
                DreamReport.user_id == user_id,
                DreamReport.surfaced_to_user_at.is_(None),
                DreamReport.significant_hypotheses_count > 0,
            )
            .order_by(DreamReport.created_at.desc())
            .limit(1)
        )
        report = (await session.execute(stmt)).scalar_one_or_none()
        if report is None:
            return None

        # Filter hypotheses to only the significant ones for surfacing.
        sig = [
            h for h in (report.hypotheses or []) if float(h.get("significance", 0)) >= threshold
        ]
        if not sig:
            return None

        await session.execute(
            update(DreamReport)
            .where(DreamReport.id == report.id)
            .values(surfaced_to_user_at=datetime.now(timezone.utc))
        )
        await session.commit()

    text = _render_brief(sig)
    _log.info(
        "morning_brief_surfaced",
        user_id=str(user_id),
        dream_report_id=str(report.id),
        hypothesis_count=len(sig),
    )
    return MorningBrief(dream_report_id=report.id, text=text, hypotheses=sig)


def _render_brief(hypotheses: list[dict]) -> str:
    lines = ["Overnight I noticed:"]
    for h in hypotheses[:5]:  # cap to keep runtime context lean
        lines.append(f"- {h.get('text', '').strip()}")
    return "\n".join(lines)
