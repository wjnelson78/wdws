"""Patch 02 §C: onboarding-mode detection.

A principal is onboarding when ANY of:
  1. created_at < 14 days ago AND total session count < 10
  2. preferences.onboarding_incomplete is true
  3. fewer than 3 of (signature_block, primary_sending_email, default_cc, primary_phone)
     are populated
  4. user_case_registry is empty AND user_contexts count < 5

Computed at session start; cached for the session by the chat handler.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.db.models import (
    Conversation,
    User,
    UserCaseRegistry,
    UserContext,
)


@dataclass(frozen=True)
class OnboardingState:
    is_onboarding: bool
    reasons: tuple[str, ...]


REQUIRED_PROFILE_FIELDS = (
    "signature_block",
    "primary_sending_email",
    "default_cc",
    "primary_phone",
)


def _has_field(user: User, field: str) -> bool:
    value = getattr(user, field, None)
    if value is None:
        return False
    if isinstance(value, (list, tuple)):
        return len(value) > 0
    if isinstance(value, str):
        return value.strip() != ""
    return True


async def detect_onboarding(session: AsyncSession, user: User) -> OnboardingState:
    """Patch 02 §C.1 + §C.5 + §C.6:

    - ``preferences.onboarding_incomplete = false`` is an explicit declaration
      that the principal has completed onboarding — short-circuit to False.
      This is what §C.6 relies on for admin-provisioned full-context accounts.
    - ``preferences.onboarding_incomplete = true`` forces onboarding on.
    - Otherwise apply the §C.1 detection rules.
    """
    prefs = user.preferences or {}
    explicit = prefs.get("onboarding_incomplete")
    if explicit is False:
        return OnboardingState(is_onboarding=False, reasons=())

    reasons: list[str] = []
    if explicit is True:
        reasons.append("preferences.onboarding_incomplete")

    if user.created_at and user.created_at >= datetime.now(timezone.utc) - timedelta(days=14):
        session_count = await session.scalar(
            select(func.count(Conversation.id)).where(Conversation.user_id == user.id)
        )
        if (session_count or 0) < 10:
            reasons.append("recent_account_low_session_count")

    populated_required = sum(1 for field in REQUIRED_PROFILE_FIELDS if _has_field(user, field))
    if populated_required < 3:
        reasons.append("profile_underpopulated")

    case_count = await session.scalar(
        select(func.count(UserCaseRegistry.case_id)).where(UserCaseRegistry.user_id == user.id)
    )
    context_count = await session.scalar(
        select(func.count(UserContext.id)).where(UserContext.user_id == user.id)
    )
    if (case_count or 0) == 0 and (context_count or 0) < 5:
        reasons.append("context_underpopulated")

    return OnboardingState(is_onboarding=bool(reasons), reasons=tuple(reasons))
