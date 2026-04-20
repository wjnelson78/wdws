#!/usr/bin/env python3
"""
Athena Cognitive Engine — Morning Briefing

Generates and delivers William's daily briefing at 7 AM:
  - Today's calendar events
  - Upcoming deadlines (next 7 days)
  - Emails needing response
  - Active tasks/reminders
  - Case status summary
  - Weather (optional)

Runs daily via cron or systemd timer.

Usage:
    python morning_briefing.py              # Generate and send briefing now
    python morning_briefing.py --preview    # Print to console without sending
"""

import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_env_file = Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

try:
    from athena_logging import setup_logging, get_logger
    setup_logging("morning-briefing")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("morning-briefing")

import asyncpg
import httpx

DATABASE_URL = os.environ["DATABASE_URL"]
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "athena-william")
NTFY_URL = os.getenv("NTFY_URL", "https://ntfy.sh")
TZ_OFFSET = timedelta(hours=-7)  # PDT


async def get_todays_events(pool: asyncpg.Pool) -> list[dict]:
    """Get today's calendar events."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0)
    today_end = today_start + timedelta(days=1)

    rows = await pool.fetch("""
        SELECT subject, start_time, end_time, location, importance
        FROM core.calendar_events
        WHERE start_time BETWEEN $1 AND $2
          AND NOT is_cancelled
        ORDER BY start_time
    """, today_start, today_end)

    return [{"subject": r["subject"],
             "time": (r["start_time"] + TZ_OFFSET).strftime("%I:%M %p"),
             "location": r["location"] or "",
             "importance": r["importance"]} for r in rows]


async def get_upcoming_deadlines(pool: asyncpg.Pool) -> list[dict]:
    """Get deadlines in next 7 days."""
    today = date.today()
    cutoff = today + timedelta(days=7)

    rows = await pool.fetch("""
        SELECT d.title, d.deadline_date, d.deadline_type, d.priority,
               c.case_number, c.case_title
        FROM legal.case_deadlines d
        JOIN legal.cases c ON d.case_id = c.id
        WHERE d.status = 'pending'
          AND d.deadline_date BETWEEN $1 AND $2
        ORDER BY d.deadline_date
    """, today, cutoff)

    return [{"title": r["title"],
             "date": str(r["deadline_date"]),
             "days": (r["deadline_date"] - today).days,
             "type": r["deadline_type"],
             "priority": r["priority"],
             "case": r["case_number"]} for r in rows]


async def get_pending_tasks(pool: asyncpg.Pool) -> list[dict]:
    """Get pending tasks due today or overdue."""
    today = date.today()
    rows = await pool.fetch("""
        SELECT title, due_date, priority, category
        FROM core.tasks
        WHERE status = 'pending'
          AND (due_date IS NULL OR due_date <= $1)
        ORDER BY COALESCE(due_date, '2099-12-31'), priority
        LIMIT 10
    """, today + timedelta(days=1))

    return [{"title": r["title"],
             "due": str(r["due_date"]) if r["due_date"] else "no date",
             "priority": r["priority"],
             "category": r["category"]} for r in rows]


async def get_recent_important_emails(pool: asyncpg.Pool) -> list[dict]:
    """Get important emails from last 24 hours."""
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)

    rows = await pool.fetch("""
        SELECT em.sender, em.subject, em.date_sent,
               d.document_type
        FROM legal.email_metadata em
        JOIN core.documents d ON em.document_id = d.id
        WHERE em.date_sent >= $1
          AND em.direction = 'inbound'
        ORDER BY em.date_sent DESC
        LIMIT 10
    """, yesterday)

    return [{"from": r["sender"],
             "subject": r["subject"],
             "time": r["date_sent"].strftime("%I:%M %p") if r["date_sent"] else ""} for r in rows]


async def get_case_summary(pool: asyncpg.Pool) -> list[dict]:
    """Get active case status overview."""
    rows = await pool.fetch("""
        SELECT c.case_number, c.case_title, c.status,
               COUNT(cd.document_id) as doc_count,
               MAX(d.created_at) as last_filing
        FROM legal.cases c
        LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
        LEFT JOIN core.documents d ON cd.document_id = d.id
        WHERE c.status IS NULL OR c.status != 'closed'
        GROUP BY c.id, c.case_number, c.case_title, c.status
        ORDER BY last_filing DESC NULLS LAST
        LIMIT 5
    """)

    return [{"number": r["case_number"],
             "name": r["case_title"] or r["case_number"],
             "docs": r["doc_count"],
             "last_activity": str(r["last_filing"].date()) if r["last_filing"] else "none"} for r in rows]


def format_briefing(events, deadlines, tasks, emails, cases) -> str:
    """Format the morning briefing as readable text."""
    today = date.today()
    lines = [
        f"☀️ Good morning, William",
        f"📅 {today.strftime('%A, %B %d, %Y')}",
        "",
    ]

    # Calendar
    if events:
        lines.append("━━━ TODAY'S SCHEDULE ━━━")
        for e in events:
            loc = f" 📍 {e['location']}" if e["location"] else ""
            lines.append(f"  {e['time']}  {e['subject']}{loc}")
        lines.append("")
    else:
        lines.append("📅 No events scheduled today\n")

    # Deadlines
    if deadlines:
        lines.append("━━━ UPCOMING DEADLINES ━━━")
        for d in deadlines:
            emoji = "🔴" if d["days"] <= 1 else "🟡" if d["days"] <= 3 else "📋"
            lines.append(f"  {emoji} {d['date']} ({d['days']}d) — {d['title']}")
            lines.append(f"     Case: {d['case']}")
        lines.append("")

    # Tasks
    if tasks:
        lines.append("━━━ TASKS ━━━")
        for t in tasks:
            lines.append(f"  ☐ {t['title']} [{t['category']}]")
        lines.append("")

    # Emails
    if emails:
        lines.append(f"━━━ NEW EMAIL ({len(emails)}) ━━━")
        for e in emails[:5]:
            sender = e["from"].split("<")[0].strip() if "<" in e["from"] else e["from"]
            lines.append(f"  📧 {sender[:30]}: {e['subject'][:50]}")
        lines.append("")

    # Cases
    if cases:
        lines.append("━━━ ACTIVE CASES ━━━")
        for c in cases:
            lines.append(f"  ⚖️ {c['number']} — {c['name'][:40]}")
            lines.append(f"     {c['docs']} docs, last activity: {c['last_activity']}")
        lines.append("")

    lines.append("— Athena 🏛️")
    return "\n".join(lines)


async def generate_and_send(preview: bool = False):
    """Generate and optionally send the morning briefing."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        events, deadlines, tasks, emails, cases = await asyncio.gather(
            get_todays_events(pool),
            get_upcoming_deadlines(pool),
            get_pending_tasks(pool),
            get_recent_important_emails(pool),
            get_case_summary(pool),
        )

        briefing = format_briefing(events, deadlines, tasks, emails, cases)

        if preview:
            print(briefing)
            return

        # Send via ntfy
        if NTFY_TOPIC:
            try:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.post(
                        f"{NTFY_URL}/{NTFY_TOPIC}",
                        headers={
                            "Title": f"☀️ Morning Briefing — {date.today().strftime('%A %b %d')}",
                            "Priority": "3",
                            "Tags": "sunrise,clipboard",
                        },
                        content=briefing,
                    )
                log.info("Morning briefing sent via ntfy")
            except Exception as e:
                log.error("Failed to send briefing via ntfy: %s", e)

        # Also store as a memory
        try:
            await pool.execute("""
                INSERT INTO core.memories
                    (memory_type, content, source, importance, metadata)
                VALUES ('event', $1, 'morning_briefing', 0.3,
                        $2::jsonb)
            """,
                f"Morning briefing for {date.today()}: "
                f"{len(events)} events, {len(deadlines)} deadlines, "
                f"{len(tasks)} tasks, {len(emails)} new emails",
                json.dumps({
                    "date": str(date.today()),
                    "type": "daily_briefing",
                    "semantic": False,
                    "embedding_policy": "skip",
                }),
            )
        except Exception as e:
            log.warning("Failed to store briefing memory: %s", e)

    finally:
        await pool.close()


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", action="store_true", help="Preview without sending")
    args = parser.parse_args()

    await generate_and_send(preview=args.preview)


if __name__ == "__main__":
    asyncio.run(main())
