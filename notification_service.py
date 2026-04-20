#!/usr/bin/env python3
"""
Athena Cognitive Engine — Notification Service

Delivers notifications to William via multiple channels:
  - ntfy.sh push notifications (phone)
  - Email via Microsoft Graph API
  - Console/log (fallback)

Monitors:
  - ops.notification_queue for pending deliveries
  - core.tasks for reminders
  - legal.case_deadlines for deadline alerts
  - core.calendar_events for upcoming events

Usage:
    python notification_service.py              # Run as daemon
    python notification_service.py --send "Test message"   # Send test notification
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

# Load .env
_env_file = Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

try:
    from athena_logging import setup_logging, get_logger
    setup_logging("notifications")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("notifications")

import asyncpg
import httpx

DATABASE_URL = os.environ["DATABASE_URL"]
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "athena-william")
NTFY_URL = os.getenv("NTFY_URL", "https://ntfy.sh")
CHECK_INTERVAL = 60  # Check every minute


async def send_push(title: str, message: str, priority: str = "default",
                    tags: str = "bell", click_url: str = "") -> bool:
    """Send push notification via ntfy.sh."""
    if not NTFY_TOPIC:
        log.warning("NTFY_TOPIC not configured, skipping push")
        return False

    priority_map = {"critical": "5", "high": "4", "normal": "3", "low": "2", "default": "3"}
    headers = {
        "Title": title[:250],
        "Priority": priority_map.get(priority, "3"),
        "Tags": tags,
    }
    if click_url:
        headers["Click"] = click_url

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{NTFY_URL}/{NTFY_TOPIC}",
                headers=headers,
                content=message[:4000],
            )
            resp.raise_for_status()
            log.info("Push sent: %s", title[:80])
            return True
    except Exception as e:
        log.error("Push failed: %s", e)
        return False


async def check_task_reminders(pool: asyncpg.Pool):
    """Check for tasks needing reminders right now."""
    now = datetime.now()
    today = now.date()
    current_time = now.time()

    rows = await pool.fetch("""
        SELECT id, title, description, due_date, due_time, priority, category,
               reminder_minutes
        FROM core.tasks
        WHERE status = 'pending'
          AND due_date IS NOT NULL
          AND due_date >= $1
    """, today)

    for r in rows:
        if not r["due_time"]:
            # All-day task — remind at 8 AM on the due date
            if r["due_date"] == today and current_time.hour == 8 and current_time.minute < 2:
                await send_push(
                    f"📋 Task Due Today: {r['title']}",
                    r["description"] or f"Category: {r['category']}",
                    priority=r["priority"],
                    tags="clipboard",
                )
            continue

        due_dt = datetime.combine(r["due_date"], r["due_time"])
        minutes_until = (due_dt - now).total_seconds() / 60

        for remind_min in (r["reminder_minutes"] or [30]):
            if abs(minutes_until - remind_min) < 1.5:
                time_str = f"in {remind_min} minutes" if remind_min > 0 else "NOW"
                await send_push(
                    f"⏰ Reminder: {r['title']}",
                    f"Due {time_str} — {r['description'] or r['category']}",
                    priority="high" if remind_min <= 5 else r["priority"],
                    tags="alarm_clock",
                )


async def check_calendar_alerts(pool: asyncpg.Pool):
    """Alert on upcoming calendar events."""
    now = datetime.now(timezone.utc)
    # Check for events starting in 15 or 60 minutes
    for minutes_ahead in [15, 60]:
        window_start = now + timedelta(minutes=minutes_ahead - 1)
        window_end = now + timedelta(minutes=minutes_ahead + 1)

        rows = await pool.fetch("""
            SELECT id, subject, start_time, location, organizer
            FROM core.calendar_events
            WHERE start_time BETWEEN $1 AND $2
              AND NOT is_cancelled
        """, window_start, window_end)

        for r in rows:
            loc = f"\n📍 {r['location']}" if r["location"] else ""
            await send_push(
                f"📅 {r['subject']} in {minutes_ahead}min",
                f"Starts at {r['start_time'].strftime('%I:%M %p')}{loc}",
                priority="high" if minutes_ahead <= 15 else "normal",
                tags="calendar",
            )


async def check_deadline_alerts(pool: asyncpg.Pool):
    """Check legal deadlines — runs once per hour, alerts on matching reminder days."""
    today = date.today()
    now = datetime.now()

    # Only run the daily check once (at 8 AM)
    if now.hour != 8 or now.minute > 1:
        return

    rows = await pool.fetch("""
        SELECT d.id, d.title, d.deadline_date, d.deadline_type,
               d.priority, d.reminder_days,
               c.case_number, c.case_title
        FROM legal.case_deadlines d
        JOIN legal.cases c ON d.case_id = c.id
        WHERE d.status = 'pending'
    """)

    for r in rows:
        days_until = (r["deadline_date"] - today).days
        reminder_days = r["reminder_days"] or [7, 3, 1]

        if days_until < 0:
            await send_push(
                f"🔴 OVERDUE: {r['title']}",
                f"Case: {r['case_number']} — {r['case_title']}\n"
                f"Was due: {r['deadline_date']} ({abs(days_until)} days ago)\n"
                f"Type: {r['deadline_type']}",
                priority="critical",
                tags="rotating_light,law",
            )
        elif days_until in reminder_days or days_until == 0:
            emoji = "🔴" if days_until <= 1 else "🟡" if days_until <= 3 else "📅"
            await send_push(
                f"{emoji} Deadline in {days_until}d: {r['title']}",
                f"Case: {r['case_number']} — {r['case_title']}\n"
                f"Due: {r['deadline_date']}\n"
                f"Type: {r['deadline_type']}",
                priority="critical" if days_until <= 1 else "high",
                tags="calendar,law",
            )


async def deliver_queued_notifications(pool: asyncpg.Pool):
    """Deliver pending notifications from ops.notification_queue."""
    rows = await pool.fetch("""
        SELECT id, subject, sections, severity
        FROM ops.notification_queue
        WHERE delivered_at IS NULL
        ORDER BY created_at
        LIMIT 10
    """)

    for r in rows:
        sections = r["sections"] if isinstance(r["sections"], list) else json.loads(r["sections"] or "[]")
        body_parts = []
        for s in sections:
            if s.get("heading"):
                body_parts.append(s["heading"])
            if s.get("content"):
                body_parts.append(s["content"][:500])

        body = "\n".join(body_parts)[:4000]
        priority = "high" if r["severity"] in ("critical", "error") else "normal"

        sent = await send_push(
            r["subject"] or "Athena Notification",
            body,
            priority=priority,
            tags="robot",
        )

        if sent:
            await pool.execute(
                "UPDATE ops.notification_queue SET delivered_at = now() WHERE id = $1",
                r["id"],
            )


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--send", type=str, help="Send a test notification")
    args = parser.parse_args()

    if args.send:
        ok = await send_push("🧪 Athena Test", args.send, priority="normal", tags="test_tube")
        print(f"Sent: {ok}")
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    log.info("Notification service started (interval=%ds, topic=%s)", CHECK_INTERVAL, NTFY_TOPIC)

    while True:
        try:
            await check_task_reminders(pool)
            await check_calendar_alerts(pool)
            await check_deadline_alerts(pool)
            await deliver_queued_notifications(pool)
        except Exception as e:
            log.error("Notification check failed: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
