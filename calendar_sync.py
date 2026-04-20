#!/usr/bin/env python3
"""
Athena Cognitive Engine — Microsoft Graph Calendar Sync

Syncs William's Outlook/Microsoft 365 calendar to PostgreSQL.
Runs on a schedule (every 15 minutes) and on startup.

Usage:
    python calendar_sync.py              # Run as daemon (15-min interval)
    python calendar_sync.py --once       # Single sync and exit
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
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
    setup_logging("calendar-sync")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    log = logging.getLogger("calendar-sync")

import asyncpg
import httpx

DATABASE_URL = os.environ["DATABASE_URL"]
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
CALENDAR_USER = os.getenv("CALENDAR_USER", "william@seattleseahawks.me")
SYNC_INTERVAL = 900  # 15 minutes
LOOKAHEAD_DAYS = 90

_access_token = ""
_token_expires = 0.0


async def get_graph_token() -> str:
    """Get Microsoft Graph access token via client credentials."""
    global _access_token, _token_expires

    if _access_token and time.time() < _token_expires:
        return _access_token

    if not all([GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET]):
        raise RuntimeError("Graph credentials not configured")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token",
            data={
                "client_id": GRAPH_CLIENT_ID,
                "client_secret": GRAPH_CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        _access_token = data["access_token"]
        _token_expires = time.time() + data.get("expires_in", 3600) - 300
        return _access_token


async def fetch_calendar_events(days_ahead: int = LOOKAHEAD_DAYS) -> list[dict]:
    """Fetch calendar events from Microsoft Graph."""
    token = await get_graph_token()
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days_ahead)

    events = []
    url = (
        f"https://graph.microsoft.com/v1.0/users/{CALENDAR_USER}/calendarview"
        f"?startdatetime={now.isoformat()}"
        f"&enddatetime={end.isoformat()}"
        f"&$top=100"
        f"&$orderby=start/dateTime"
        f"&$select=id,subject,bodyPreview,start,end,location,organizer,attendees,"
        f"isAllDay,isCancelled,webLink,categories,importance"
    )

    async with httpx.AsyncClient(timeout=30) as client:
        while url:
            resp = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "10"))
                log.warning("Rate limited, waiting %ds", retry_after)
                await asyncio.sleep(retry_after)
                continue

            resp.raise_for_status()
            data = resp.json()
            events.extend(data.get("value", []))
            url = data.get("@odata.nextLink")

    return events


def parse_graph_event(event: dict) -> dict:
    """Parse a Graph API event into our schema."""
    start = event.get("start", {})
    end = event.get("end", {})

    # Parse datetime — Graph returns ISO format with timezone
    start_dt = datetime.fromisoformat(start.get("dateTime", "").replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end.get("dateTime", "").replace("Z", "+00:00"))

    # If no timezone info, assume UTC
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    location = event.get("location", {})
    loc_str = location.get("displayName", "") if isinstance(location, dict) else str(location)

    organizer = event.get("organizer", {})
    org_email = ""
    if isinstance(organizer, dict):
        org_email = organizer.get("emailAddress", {}).get("address", "")

    attendees = []
    for a in event.get("attendees", []):
        if isinstance(a, dict):
            attendees.append({
                "name": a.get("emailAddress", {}).get("name", ""),
                "email": a.get("emailAddress", {}).get("address", ""),
                "status": a.get("status", {}).get("response", ""),
            })

    return {
        "id": event["id"],
        "subject": event.get("subject", ""),
        "body_preview": (event.get("bodyPreview") or "")[:1000],
        "start_time": start_dt,
        "end_time": end_dt,
        "location": loc_str,
        "organizer": org_email,
        "attendees": json.dumps(attendees),
        "is_all_day": event.get("isAllDay", False),
        "is_cancelled": event.get("isCancelled", False),
        "web_link": event.get("webLink", ""),
        "categories": event.get("categories", []),
        "importance": event.get("importance", "normal"),
    }


async def sync_events(pool: asyncpg.Pool, days_ahead: int = LOOKAHEAD_DAYS) -> int:
    """Sync calendar events from Graph API to PostgreSQL."""
    log.info("Syncing calendar events (%d days ahead)...", days_ahead)

    try:
        events = await fetch_calendar_events(days_ahead)
    except Exception as e:
        log.error("Failed to fetch calendar events: %s", e)
        return 0

    synced = 0
    for event in events:
        try:
            parsed = parse_graph_event(event)
            await pool.execute("""
                INSERT INTO core.calendar_events
                    (id, subject, body_preview, start_time, end_time, location,
                     organizer, attendees, is_all_day, is_cancelled, web_link,
                     categories, importance, synced_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13, now())
                ON CONFLICT (id) DO UPDATE SET
                    subject = EXCLUDED.subject,
                    body_preview = EXCLUDED.body_preview,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    location = EXCLUDED.location,
                    organizer = EXCLUDED.organizer,
                    attendees = EXCLUDED.attendees,
                    is_all_day = EXCLUDED.is_all_day,
                    is_cancelled = EXCLUDED.is_cancelled,
                    importance = EXCLUDED.importance,
                    synced_at = now()
            """,
                parsed["id"], parsed["subject"], parsed["body_preview"],
                parsed["start_time"], parsed["end_time"], parsed["location"],
                parsed["organizer"], parsed["attendees"],
                parsed["is_all_day"], parsed["is_cancelled"],
                parsed["web_link"], parsed["categories"], parsed["importance"],
            )
            synced += 1
        except Exception as e:
            log.error("Failed to sync event %s: %s", event.get("subject", "?"), e)

    log.info("Synced %d/%d calendar events", synced, len(events))
    return synced


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Sync once and exit")
    args = parser.parse_args()

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    if args.once:
        count = await sync_events(pool)
        print(f"Synced {count} events")
        await pool.close()
        return

    log.info("Calendar sync started (interval=%ds, user=%s)", SYNC_INTERVAL, CALENDAR_USER)

    while True:
        try:
            await sync_events(pool)
        except Exception as e:
            log.error("Calendar sync failed: %s", e)

        await asyncio.sleep(SYNC_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
