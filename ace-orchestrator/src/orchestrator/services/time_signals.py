"""Patch 02 §E: opt-in time-aware runtime signals.

Default off for every principal. Reads ``preferences.time_signals`` from the
user row to decide whether to emit signals into the RUNTIME_CONTEXT block.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from zoneinfo import ZoneInfo

from orchestrator.db.models import User
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)


@dataclass(frozen=True)
class TimeSignals:
    enabled: bool = False
    out_of_hours: str | None = None
    weekend: str | None = None
    morning_brief: str | None = None

    def as_runtime_lines(self) -> list[tuple[str, str]]:
        """Return runtime-context (key, value) pairs for the signals that fired."""
        lines: list[tuple[str, str]] = []
        if not self.enabled:
            return lines
        if self.out_of_hours:
            lines.append(("TIME_SIGNAL_HOURS", self.out_of_hours))
        if self.weekend:
            lines.append(("TIME_SIGNAL_WEEKEND", self.weekend))
        if self.morning_brief:
            lines.append(("TIME_SIGNAL_MORNING", self.morning_brief))
        return lines


def _parse_hhmm(s: str) -> time | None:
    try:
        hh, mm = s.split(":", 1)
        return time(int(hh), int(mm))
    except Exception:
        return None


def evaluate_time_signals(user: User, *, now_utc: datetime | None = None) -> TimeSignals:
    """Read user.preferences.time_signals; emit signals based on local clock.

    Returns a TimeSignals instance. ``enabled=False`` means no signals will be
    emitted; emitter callers should still check this at the top of their loop.
    """
    prefs = user.preferences or {}
    cfg = prefs.get("time_signals") or {}
    if not cfg.get("enabled"):
        return TimeSignals(enabled=False)

    try:
        tz = ZoneInfo(user.timezone)
    except Exception:
        _log.warning("time_signals_bad_tz", tz=user.timezone)
        tz = ZoneInfo("UTC")

    now_utc = now_utc or datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    local = now_utc.astimezone(tz)

    out_of_hours = None
    hours_cfg = cfg.get("preferred_working_hours") or {}
    start = _parse_hhmm(hours_cfg.get("start", "08:00"))
    end = _parse_hhmm(hours_cfg.get("end", "20:00"))
    if start and end and not (start <= local.time() <= end):
        out_of_hours = (
            f"Request arrived at {local.strftime('%H:%M %Z')}, outside the principal's "
            f"preferred working hours ({start.strftime('%H:%M')}–{end.strftime('%H:%M')}). "
            "If this request looks fatigue-influenced (rambling, contradictory, abnormally "
            "emotional, or asking for an action that could be deferred), gently note this "
            "rather than execute reflexively."
        )

    weekend = None
    if cfg.get("weekend_noteworthy") and local.weekday() >= 5:
        weekend = (
            f"Today is {local.strftime('%A, %Y-%m-%d')} (weekend). The principal has flagged "
            "weekends as noteworthy."
        )

    return TimeSignals(
        enabled=True,
        out_of_hours=out_of_hours,
        weekend=weekend,
        morning_brief=None,  # populated by chat handler when a dream report is queued
    )
