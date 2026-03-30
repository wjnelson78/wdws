"""
📬 Daily Digest Agent — One Email Per Day
══════════════════════════════════════════════════════════
Collects every queued notification from the previous 24 hours
and sends a single, well-structured daily report at 7 AM.

Critical-severity events are sent immediately by each agent
and never appear in this digest.  Everything else (Code Doctor
fixes, Watchdog warnings, DBA notes, Athena morning brief,
quality eval reports, scorecards) arrives here.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

from framework import BaseAgent, RunContext
from config import (
    GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
    GRAPH_SENDER_EMAIL, ALERT_EMAIL,
)
from email_util import send_email, build_notification_html

_EMAIL_AVAILABLE = all([
    GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
    GRAPH_SENDER_EMAIL, ALERT_EMAIL,
])

# Agent display names for the digest
_AGENT_LABELS = {
    "code-doctor":        "🩺 Code Doctor",
    "watchdog":           "🐕 Watchdog",
    "athena":             "🏛️ Athena",
    "software-engineer":  "💻 Software Engineer",
    "dba":                "🗄️ DBA",
    "quality-eval":       "✅ Quality Eval",
    "scorecard":          "📊 Scorecard",
    "orchestrator":       "🎯 Orchestrator",
    "security-sentinel":  "🔒 Security",
    "data-quality":       "📋 Data Quality",
    "self-healing":       "🔧 Self-Healing",
    "db-tuner":           "⚡ DB Tuner",
}

# Section type → human priority sort order (lower = more prominent)
_TYPE_PRIORITY = {
    "error":   0,
    "warning": 1,
    "success": 2,
    "code":    3,
    "info":    4,
}


class DailyDigestAgent(BaseAgent):
    agent_id   = "daily-digest"
    name       = "Daily Digest"
    version    = "1.0.0"
    schedule   = "0 7 * * *"   # 7:00 AM every day
    priority   = 2
    instructions = """You manage the daily platform report email. Your job is to collect all
queued agent notifications and send a single, well-structured daily digest.
You never modify agent behaviour — you only read the notification queue and
compose the email."""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {"queued_items": 0, "agents_represented": 0, "email_sent": False}

        p = await ctx.db()

        # ── 1. Claim all pending rows (atomic to avoid double-send) ──────
        now = datetime.now(timezone.utc)
        window_start = now - timedelta(hours=25)  # generous window: 25 h

        rows = await p.fetch("""
            UPDATE ops.notification_queue
            SET    digest_sent_at = $1
            WHERE  digest_sent_at IS NULL
              AND  immediate_sent = FALSE
              AND  created_at     >= $2
            RETURNING id, agent_id, subject, sections, severity, created_at
        """, now, window_start)

        if not rows:
            await ctx.action("No pending notifications — skipping digest")
            return {"summary": "Daily Digest: nothing queued — no email sent", "metrics": metrics}

        metrics["queued_items"] = len(rows)

        # ── 2. Group rows by agent ────────────────────────────────────────
        by_agent: dict[str, list] = defaultdict(list)
        for row in rows:
            sections = row["sections"]
            if isinstance(sections, str):
                try:
                    sections = json.loads(sections)
                except Exception:
                    sections = [{"heading": row["subject"], "content": str(sections), "type": "info"}]
            by_agent[row["agent_id"]].extend(sections)

        metrics["agents_represented"] = len(by_agent)

        # ── 3. Compute top-line summary ───────────────────────────────────
        total_sections = sum(len(v) for v in by_agent.values())
        warning_count  = sum(
            1 for secs in by_agent.values()
            for s in secs if s.get("type") in ("error", "warning")
        )
        fix_count = sum(
            1 for secs in by_agent.values()
            for s in secs if "auto-fix" in s.get("heading", "").lower()
        )

        if warning_count == 0 and fix_count == 0:
            headline = "All systems ran cleanly. No issues or repairs to report."
            headline_type = "success"
        elif fix_count > 0 and warning_count == 0:
            headline = (
                f"{fix_count} bug{'s were' if fix_count > 1 else ' was'} automatically "
                f"repaired by Code Doctor. No unresolved issues."
            )
            headline_type = "success"
        elif warning_count > 0 and fix_count == 0:
            headline = (
                f"{warning_count} item{'s need' if warning_count > 1 else ' needs'} your attention."
            )
            headline_type = "warning"
        else:
            headline = (
                f"{fix_count} repair{'s' if fix_count > 1 else ''} applied automatically. "
                f"{warning_count} item{'s' if warning_count > 1 else ''} awaiting your review."
            )
            headline_type = "warning"

        # ── 4. Build digest sections ──────────────────────────────────────
        digest_sections = []

        # Overview card (plain_summary = the headline; expandable stats)
        digest_sections.append({
            "heading":       "📋 Today's Platform Summary",
            "plain_summary": headline,
            "content": (
                f"Date:                {now.strftime('%A, %B %-d, %Y')}\n"
                f"Agents Reporting:    {metrics['agents_represented']}\n"
                f"Total Events:        {total_sections}\n"
                f"Issues / Warnings:   {warning_count}\n"
                f"Auto-Repairs:        {fix_count}"
            ),
            "type": headline_type,
        })

        # One section group per agent, sorted by priority (errors first)
        agent_order = sorted(by_agent.keys(), key=lambda a: (
            # Push agents with errors/warnings to the top
            min((_TYPE_PRIORITY.get(s.get("type", "info"), 4) for s in by_agent[a]), default=4),
            a,
        ))

        for agent_id in agent_order:
            sections = by_agent[agent_id]
            label = _AGENT_LABELS.get(agent_id, f"🤖 {agent_id.title()}")

            # Agent header — no content, just a visual separator
            digest_sections.append({
                "heading": label,
                "content": "",
                "type":    "code",
            })

            # Sort this agent's sections: errors → warnings → success → info
            sorted_secs = sorted(
                sections,
                key=lambda s: _TYPE_PRIORITY.get(s.get("type", "info"), 4),
            )
            digest_sections.extend(sorted_secs)

        # ── 5. Send the digest ────────────────────────────────────────────
        if not _EMAIL_AVAILABLE:
            await ctx.action("Email not configured — digest built but not sent")
            return {"summary": "Daily Digest: email not configured", "metrics": metrics}

        date_str   = now.strftime("%B %-d, %Y")
        email_subj = f"Athena Daily Report — {date_str}"

        body_html = build_notification_html(
            title=email_subj,
            sections=digest_sections,
            footer="Athena Cognitive Platform - Developed by William Nelson 2016",
        )

        result = await send_email(
            tenant_id=GRAPH_TENANT_ID,
            client_id=GRAPH_CLIENT_ID,
            client_secret=GRAPH_CLIENT_SECRET,
            sender=GRAPH_SENDER_EMAIL,
            to_recipients=[ALERT_EMAIL],
            subject=email_subj,
            body_html=body_html,
            importance="normal",
        )

        metrics["email_sent"] = result["status"] == "sent"

        if result["status"] == "sent":
            await ctx.action(
                f"Daily digest sent — {metrics['queued_items']} events from "
                f"{metrics['agents_represented']} agent(s)"
            )
        else:
            # Roll back the digest_sent_at so items are retried tomorrow
            await p.execute("""
                UPDATE ops.notification_queue
                SET    digest_sent_at = NULL
                WHERE  digest_sent_at = $1
            """, now)
            await ctx.finding("warning", "digest",
                "Daily digest email failed to send",
                result.get("error", "unknown error"),
                {"result": result})

        return {
            "summary": (
                f"Daily Digest: {metrics['queued_items']} events from "
                f"{metrics['agents_represented']} agents — "
                f"{'sent' if metrics['email_sent'] else 'FAILED'}"
            ),
            "metrics": metrics,
        }
