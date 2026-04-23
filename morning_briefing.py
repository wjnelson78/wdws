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
from typing import Optional

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

from core_safety import build_morning_brief_safety_filter

DATABASE_URL = os.environ["DATABASE_URL"]

_BRIEF_CALLER = {
    'tool': 'morning_briefing',
    'agent_id': 'morning_briefing',
    'user': 'will',
}
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
    """Get important emails from last 24 hours.

    Metadata-only per §5.5: surfaces sender, subject, date — never email body
    or attachment content. The brief-specific safety filter applies stricter
    defaults than interactive retrieval: heightened-protection categories
    (42 CFR Part 2, psychotherapy notes) and minor-patient documents are
    hard-excluded regardless of authorization, because the brief is an SMTP
    surface and those content classes should never reach it.
    """
    yesterday = datetime.now(timezone.utc) - timedelta(hours=24)

    async with pool.acquire() as conn:
        clause, params, log_cb = await build_morning_brief_safety_filter(
            conn=conn,
            domain='legal',
            caller_context={**_BRIEF_CALLER, 'tool_frame': 'get_recent_important_emails'},
            table_alias='d',
            next_param_index=2,  # $1=yesterday, $2+=filter
        )
        rows = await conn.fetch(f"""
            SELECT d.id, em.sender, em.subject, em.date_sent,
                   d.document_type
            FROM legal.email_metadata em
            JOIN core.documents d ON em.document_id = d.id
            WHERE em.date_sent >= $1
              AND em.direction = 'inbound'
              AND {clause}
            ORDER BY em.date_sent DESC
            LIMIT 10
        """, yesterday, *params)
        await log_cb(rows)

    return [{"from": r["sender"],
             "subject": r["subject"],
             "time": r["date_sent"].strftime("%I:%M %p") if r["date_sent"] else ""} for r in rows]


async def get_case_summary(pool: asyncpg.Pool) -> list[dict]:
    """Get active case status overview.

    Count-only per §5.6: surfaces case number, title, status, document count,
    and last filing date — never privileged filing titles or content. The
    brief's safety filter is applied in count-only mode to the case-document
    join, so privileged docs are *counted* in doc_count but never surface
    here as identifiable rows.
    """
    async with pool.acquire() as conn:
        # Count-only clause is "TRUE" — privileged/PHI docs included in the
        # count but the filter's audit log records aggregate only (no IDs).
        clause, params, log_cb = await build_morning_brief_safety_filter(
            conn=conn,
            domain='legal',
            caller_context={**_BRIEF_CALLER, 'tool_frame': 'get_case_summary'},
            mode='count_only',
            table_alias='d',
            next_param_index=1,
        )
        rows = await conn.fetch(f"""
            SELECT c.case_number, c.case_title, c.status,
                   COUNT(cd.document_id) as doc_count,
                   MAX(d.created_at) as last_filing
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            LEFT JOIN core.documents d ON cd.document_id = d.id
            WHERE (c.status IS NULL OR c.status != 'closed')
              AND {clause}
            GROUP BY c.id, c.case_number, c.case_title, c.status
            ORDER BY last_filing DESC NULLS LAST
            LIMIT 5
        """, *params)
        await log_cb([{'count': sum(r['doc_count'] or 0 for r in rows),
                       'domain': 'legal'}])

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


async def get_sprint_a_task4_progress(pool: asyncpg.Pool) -> Optional[dict]:
    """Task 4 progress block per §10 rule 8 (Sprint A edition).

    Returns None if Task 4 is not yet active (no staging-table rows).
    Otherwise returns a dict the format_briefing can render into a block
    with per-domain counts, confidence-bucket distribution (last 24h),
    review queue depth, failed classifications, heightened-category hits,
    throughput stats, and transparent ETA math.
    """
    async with pool.acquire() as conn:
        # Is Task 4 active?
        total_staged = await conn.fetchval(
            "SELECT COUNT(*) FROM core.documents_backfill_staging")
        if not total_staged:
            return None

        # Per-domain total corpus + classified counts
        per_domain = {}
        for domain in ('legal', 'medical'):
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM core.documents WHERE domain = $1", domain)
            if domain == 'legal':
                classified = await conn.fetchval(
                    "SELECT COUNT(*) FROM core.documents "
                    " WHERE domain = $1 AND privilege IS NOT NULL", domain)
            else:
                classified = await conn.fetchval(
                    "SELECT COUNT(*) FROM core.documents "
                    " WHERE domain = $1 AND phi_status IS NOT NULL", domain)
            per_domain[domain] = {'total': total, 'classified': classified}

        # Confidence bucket distribution, last 24h
        buckets_legal = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE confidence >= 0.9) AS high,
                COUNT(*) FILTER (WHERE confidence >= 0.7 AND confidence < 0.9) AS medium,
                COUNT(*) FILTER (WHERE confidence < 0.7) AS low
              FROM core.documents_backfill_staging
             WHERE domain = 'legal'
               AND created_at >= now() - interval '24 hours'
               AND classifier_version = 'agent_athena_v1'
        """)
        buckets_medical = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE confidence >= 0.9) AS high,
                COUNT(*) FILTER (WHERE confidence >= 0.7 AND confidence < 0.9) AS medium,
                COUNT(*) FILTER (WHERE confidence < 0.7) AS low
              FROM core.documents_backfill_staging
             WHERE domain = 'medical'
               AND created_at >= now() - interval '24 hours'
               AND classifier_version = 'agent_athena_v1'
        """)

        # Review queue depth
        review_depth = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE decision_type = 'sprint_a_t4_classification_review'
                                  AND approved IS NULL) AS classification_review,
                COUNT(*) FILTER (WHERE decision_type = 'sprint_a_t4_heightened_review'
                                  AND approved IS NULL) AS heightened_review
              FROM ops.pending_approvals
        """)

        # Heightened-category hits, last 24h
        heightened = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE 'psychotherapy_notes' = ANY(proposed_phi_categories)) AS psychotherapy,
                COUNT(*) FILTER (WHERE 'sud_42_cfr_part_2' = ANY(proposed_phi_categories)) AS part2,
                COUNT(*) FILTER (WHERE 'mental_health' = ANY(proposed_phi_categories)) AS mental_health,
                COUNT(*) FILTER (WHERE 'genetic_gina' = ANY(proposed_phi_categories)) AS genetic,
                COUNT(*) FILTER (WHERE 'hiv_aids' = ANY(proposed_phi_categories)) AS hiv
              FROM core.documents_backfill_staging
             WHERE created_at >= now() - interval '24 hours'
        """)

        # Failed classifications, last 24h
        failed_24h = await conn.fetchval("""
            SELECT COUNT(*) FROM core.documents_backfill_staging
             WHERE status = 'rejected'
               AND created_at >= now() - interval '24 hours'
        """)

        # Throughput: docs/hr averages last 24h, split by business hours
        throughput = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE classifier_version = 'agent_athena_v1')::float
                    / GREATEST(EXTRACT(EPOCH FROM (now() - MIN(created_at))) / 3600.0, 1.0) AS avg_24h,
                COUNT(*) FILTER (WHERE classifier_version = 'agent_athena_v1'
                                  AND EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Los_Angeles') BETWEEN 9 AND 18)::float
                    / GREATEST(10.0, 1.0) AS business_hours_avg
              FROM core.documents_backfill_staging
             WHERE created_at >= now() - interval '24 hours'
        """)

        # ETA at current 24h rate
        total_to_classify = sum(
            per_domain[d]['total'] - per_domain[d]['classified']
            for d in ('legal', 'medical')
        )
        avg_24h = float(throughput['avg_24h'] or 0)
        eta_days = total_to_classify / (avg_24h * 24) if avg_24h > 0 else None

        # T4_SAMPLE_REVIEW status
        t4_sample = await conn.fetchrow("""
            SELECT approved, approved_at FROM ops.pending_approvals
             WHERE decision_type = 'sprint_a_t4_sample_review'
             ORDER BY created_at DESC LIMIT 1
        """)

        return {
            'per_domain': per_domain,
            'buckets_legal': dict(buckets_legal),
            'buckets_medical': dict(buckets_medical),
            'review_queue': dict(review_depth),
            'heightened_24h': dict(heightened),
            'failed_24h': failed_24h,
            'throughput_avg_24h': avg_24h,
            'throughput_business_hours_avg': float(throughput['business_hours_avg'] or 0),
            'eta_days': eta_days,
            'total_remaining': total_to_classify,
            't4_sample_review': (
                'cleared' if (t4_sample and t4_sample['approved'])
                else ('pending' if t4_sample else 'not yet created')
            ),
        }


def format_task4_block(progress: dict) -> list[str]:
    """Render a 10-line Task 4 progress block for the brief."""
    d = progress
    lines = ["━━━ SPRINT A TASK 4 PROGRESS ━━━"]
    for domain in ('legal', 'medical'):
        pd = d['per_domain'][domain]
        pct = 100.0 * pd['classified'] / pd['total'] if pd['total'] else 0
        lines.append(f"  {domain}: {pd['classified']}/{pd['total']} ({pct:.1f}%)")
    bl = d['buckets_legal']; bm = d['buckets_medical']
    lines.append(f"  confidence buckets (24h) — legal: high={bl['high']} med={bl['medium']} low={bl['low']}; "
                 f"medical: high={bm['high']} med={bm['medium']} low={bm['low']}")
    q = d['review_queue']
    lines.append(f"  review queues: classification={q['classification_review']}, heightened={q['heightened_review']}")
    h = d['heightened_24h']
    if any(h.values()):
        lines.append(f"  heightened categories hit (24h): "
                     f"psych={h['psychotherapy']}, part2={h['part2']}, "
                     f"mental={h['mental_health']}, genetic={h['genetic']}, hiv={h['hiv']}")
    lines.append(f"  failed classifications (24h): {d['failed_24h']}")
    lines.append(f"  throughput avg: {d['throughput_avg_24h']:.1f}/hr (business-hours avg {d['throughput_business_hours_avg']:.1f}/hr)")
    if d['eta_days'] is not None:
        lines.append(f"  ETA to 100% classified: {d['eta_days']:.1f} days "
                     f"at current {d['throughput_avg_24h']:.0f}/hr extrapolated to 24h")
    else:
        lines.append(f"  ETA: n/a (no throughput in last 24h)")
    lines.append(f"  T4_SAMPLE_REVIEW: {d['t4_sample_review']}")
    lines.append("")
    return lines


async def generate_and_send(preview: bool = False):
    """Generate and optionally send the morning briefing."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        events, deadlines, tasks, emails, cases, task4 = await asyncio.gather(
            get_todays_events(pool),
            get_upcoming_deadlines(pool),
            get_pending_tasks(pool),
            get_recent_important_emails(pool),
            get_case_summary(pool),
            get_sprint_a_task4_progress(pool),
        )

        briefing = format_briefing(events, deadlines, tasks, emails, cases)
        if task4 is not None:
            briefing = briefing + "\n" + "\n".join(format_task4_block(task4))

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
