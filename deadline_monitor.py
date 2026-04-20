#!/usr/bin/env python3
"""
Athena Cognitive Engine — Legal Deadline Monitor

Runs continuously, checking for upcoming and overdue deadlines.
Sends alerts via logging (and optionally ntfy push notifications).

Usage:
    python deadline_monitor.py                  # Run as daemon
    python deadline_monitor.py --check-once     # Single check and exit
"""

import asyncio
import json
import logging
import os
import sys
from datetime import date, timedelta
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
    setup_logging("deadline-monitor")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("deadline-monitor")

import asyncpg
import httpx

DATABASE_URL = os.environ["DATABASE_URL"]
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "")  # e.g., "athena-alerts"
NTFY_URL = os.getenv("NTFY_URL", "https://ntfy.sh")
CHECK_INTERVAL = int(os.getenv("DEADLINE_CHECK_INTERVAL", "3600"))  # 1 hour


async def check_deadlines(pool: asyncpg.Pool) -> list[dict]:
    """Check for deadlines needing alerts."""
    today = date.today()
    alerts = []

    # Upcoming deadlines where today matches a reminder day
    rows = await pool.fetch("""
        SELECT d.id, d.title, d.deadline_date, d.deadline_type,
               d.priority, d.description, d.reminder_days,
               c.case_number, c.case_title
        FROM legal.case_deadlines d
        JOIN legal.cases c ON d.case_id = c.id
        WHERE d.status = 'pending'
    """)

    for r in rows:
        days_until = (r["deadline_date"] - today).days
        reminder_days = r["reminder_days"] or [7, 3, 1]

        if days_until < 0:
            # Overdue
            alerts.append({
                "deadline_id": r["id"],
                "case_number": r["case_number"],
                "case_title": r["case_title"] or r["case_number"],
                "title": f"OVERDUE ({abs(days_until)}d): {r['title']}",
                "deadline_date": str(r["deadline_date"]),
                "days_remaining": days_until,
                "priority": "critical",
                "type": r["deadline_type"],
            })
        elif days_until in reminder_days or days_until == 0:
            alerts.append({
                "deadline_id": r["id"],
                "case_number": r["case_number"],
                "case_title": r["case_title"] or r["case_number"],
                "title": r["title"],
                "deadline_date": str(r["deadline_date"]),
                "days_remaining": days_until,
                "priority": "critical" if days_until <= 1 else r["priority"],
                "type": r["deadline_type"],
            })

    return sorted(alerts, key=lambda a: a["days_remaining"])


async def send_ntfy_alert(alert: dict):
    """Send push notification via ntfy.sh."""
    if not NTFY_TOPIC:
        return

    priority_map = {"critical": "5", "high": "4", "normal": "3", "low": "2"}
    ntfy_priority = priority_map.get(alert["priority"], "3")

    title = f"[{alert['case_number']}] {alert['title']}"
    body = f"Deadline: {alert['deadline_date']} ({alert['days_remaining']}d)\nType: {alert['type']}"
    if alert.get("case_title"):
        body += f"\nCase: {alert['case_title']}"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{NTFY_URL}/{NTFY_TOPIC}",
                headers={
                    "Title": title,
                    "Priority": ntfy_priority,
                    "Tags": "calendar,law",
                },
                content=body,
            )
        log.info("Sent ntfy alert for deadline %d", alert["deadline_id"])
    except Exception as e:
        log.error("Failed to send ntfy alert: %s", e)


async def run_check(pool: asyncpg.Pool):
    """Run a single deadline check cycle."""
    alerts = await check_deadlines(pool)

    if not alerts:
        log.info("No deadline alerts")
        return

    log.warning("Found %d deadline alerts:", len(alerts))
    for alert in alerts:
        emoji = "🔴" if alert["priority"] == "critical" else "🟡"
        log.warning(
            "  %s [%s] %s — %s (%dd)",
            emoji, alert["case_number"], alert["title"],
            alert["deadline_date"], alert["days_remaining"],
        )
        await send_ntfy_alert(alert)


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-once", action="store_true")
    args = parser.parse_args()

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    log.info("Deadline monitor started (interval=%ds)", CHECK_INTERVAL)

    if args.check_once:
        await run_check(pool)
        await pool.close()
        return

    while True:
        try:
            await run_check(pool)
        except Exception as e:
            log.error("Deadline check failed: %s", e)

        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
