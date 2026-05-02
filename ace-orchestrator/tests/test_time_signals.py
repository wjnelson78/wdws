"""Patch 02 §E: opt-in time signals."""
from datetime import datetime
from zoneinfo import ZoneInfo

from orchestrator.db.models import User
from orchestrator.services.time_signals import evaluate_time_signals


def _user(prefs: dict, *, tz: str = "America/Los_Angeles") -> User:
    u = User(
        identifier="x",
        display_name="X",
        default_session_role="principal",
    )
    u.preferences = prefs
    u.timezone = tz
    return u


def test_disabled_by_default_yields_no_signals():
    u = _user({})
    ts = evaluate_time_signals(u, now_utc=datetime(2026, 4, 21, 23, 0, tzinfo=ZoneInfo("UTC")))
    assert ts.enabled is False
    assert ts.as_runtime_lines() == []


def test_enabled_explicit_off_still_no_signals():
    u = _user({"time_signals": {"enabled": False}})
    ts = evaluate_time_signals(u)
    assert ts.enabled is False


def test_out_of_hours_emits_signal_late_night():
    u = _user(
        {
            "time_signals": {
                "enabled": True,
                "preferred_working_hours": {"start": "08:00", "end": "20:00"},
            }
        }
    )
    # 02:00 UTC = 18:00 PT prior day OR 19:00 PT — pick a clearly-late time:
    # 06:00 UTC = 23:00 PT (PDT in April)
    late_utc = datetime(2026, 4, 22, 6, 0, tzinfo=ZoneInfo("UTC"))
    ts = evaluate_time_signals(u, now_utc=late_utc)
    assert ts.enabled is True
    assert ts.out_of_hours is not None
    assert "outside the principal's preferred working hours" in ts.out_of_hours


def test_in_hours_no_out_of_hours_signal():
    u = _user(
        {
            "time_signals": {
                "enabled": True,
                "preferred_working_hours": {"start": "08:00", "end": "20:00"},
            }
        }
    )
    midday_utc = datetime(2026, 4, 21, 19, 0, tzinfo=ZoneInfo("UTC"))  # 12:00 PT
    ts = evaluate_time_signals(u, now_utc=midday_utc)
    assert ts.out_of_hours is None


def test_weekend_signal_only_when_flagged():
    u_off = _user(
        {
            "time_signals": {
                "enabled": True,
                "preferred_working_hours": {"start": "08:00", "end": "20:00"},
                "weekend_noteworthy": False,
            }
        }
    )
    saturday_midday_utc = datetime(2026, 4, 25, 19, 0, tzinfo=ZoneInfo("UTC"))
    ts_off = evaluate_time_signals(u_off, now_utc=saturday_midday_utc)
    assert ts_off.weekend is None

    u_on = _user(
        {
            "time_signals": {
                "enabled": True,
                "preferred_working_hours": {"start": "08:00", "end": "20:00"},
                "weekend_noteworthy": True,
            }
        }
    )
    ts_on = evaluate_time_signals(u_on, now_utc=saturday_midday_utc)
    assert ts_on.weekend is not None
    assert "Saturday" in ts_on.weekend


def test_weekday_no_weekend_signal_even_when_flagged():
    u = _user(
        {
            "time_signals": {
                "enabled": True,
                "preferred_working_hours": {"start": "08:00", "end": "20:00"},
                "weekend_noteworthy": True,
            }
        }
    )
    tuesday_midday_utc = datetime(2026, 4, 21, 19, 0, tzinfo=ZoneInfo("UTC"))
    ts = evaluate_time_signals(u, now_utc=tuesday_midday_utc)
    assert ts.weekend is None
