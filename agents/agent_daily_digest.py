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
    "dba":                "🗄️ DBA",
    "quality-eval":       "✅ Quality Eval",
    "orchestrator":       "🎯 Orchestrator",
    "security-sentinel":  "🔒 Security",
    "data-quality":       "📋 Data Quality",
    "self-healing":       "🔧 Self-Healing",
    "case-strategy":      "⚖️ Case Strategy",
    "retention":          "🗑️ Retention",
    "query-insight":      "🔍 Query Insight",
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
    agent_id    = "daily-digest"
    agent_name  = "Daily Digest"
    version     = "1.0.0"
    schedule    = "0 7 * * *"   # 7:00 AM every day
    priority    = 2
    instructions = """You manage the daily platform report email. Your job is to collect all
queued agent notifications and send a single, well-structured daily digest.
On Mondays, a weekly scorecard section is appended with quality, reliability,
and safety metrics. You never modify agent behaviour — you only read the
notification queue and compose the email."""

    async def _weekly_scorecard_sections(self, p) -> list[dict]:
        """Build scorecard sections (called on Mondays only)."""
        sections = []

        # Quality eval pass rates (last 30 days)
        eval_runs = await p.fetch("""
            SELECT metrics FROM ops.agent_runs
            WHERE agent_id = 'quality-eval'
              AND started_at > now() - interval '30 days'
            ORDER BY started_at DESC LIMIT 5
        """)
        pass_rates = []
        for r in eval_runs:
            m = r["metrics"] or {}
            if isinstance(m, str):
                try: m = json.loads(m)
                except Exception: m = {}
            pr = m.get("pass_rate")
            if pr is not None:
                pass_rates.append(float(pr))
        avg_pass_rate = round(sum(pass_rates) / len(pass_rates), 3) if pass_rates else None

        # Agent reliability (last 7 days)
        agent_stats = await p.fetchrow("""
            SELECT COUNT(*) AS total_runs,
                   COUNT(*) FILTER (WHERE status = 'error') AS errors,
                   AVG(duration_ms)::int AS avg_ms,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms
            FROM ops.agent_runs WHERE started_at > now() - interval '7 days'
        """)

        # MCP reliability (last 7 days)
        mcp_stats = await p.fetchrow("""
            SELECT COUNT(*) AS total_calls,
                   COUNT(*) FILTER (WHERE error IS NOT NULL) AS errors,
                   AVG(duration_ms)::int AS avg_ms,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99_ms
            FROM ops.mcp_query_log WHERE created_at > now() - interval '7 days'
        """)

        # Open findings
        findings = await p.fetch("""
            SELECT severity, agent_id, title, created_at
            FROM ops.agent_findings
            WHERE status = 'open' AND created_at > now() - interval '7 days'
            ORDER BY CASE severity WHEN 'critical' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,
                     created_at DESC
            LIMIT 10
        """)

        # Top error agents
        top_errors = await p.fetch("""
            SELECT agent_id, COUNT(*) AS errors FROM ops.agent_runs
            WHERE status = 'error' AND started_at > now() - interval '7 days'
            GROUP BY agent_id ORDER BY errors DESC LIMIT 5
        """)

        # Build sections
        total_agent = agent_stats["total_runs"] or 0
        agent_errors = agent_stats["errors"] or 0
        agent_error_rate = round(agent_errors / max(total_agent, 1), 3)
        total_mcp = mcp_stats["total_calls"] or 0
        mcp_errors = mcp_stats["errors"] or 0
        mcp_error_rate = round(mcp_errors / max(total_mcp, 1), 3)

        sections.append({
            "heading": "Weekly Scorecard — Quality",
            "content": (
                f"Eval avg pass rate (30d): {avg_pass_rate if avg_pass_rate is not None else 'N/A'}\n"
                f"Eval runs analyzed: {len(eval_runs)}"
            ),
            "type": "success" if avg_pass_rate and avg_pass_rate > 0.8 else "warning",
        })
        sections.append({
            "heading": "Weekly Scorecard — Reliability",
            "content": (
                f"Agent runs: {total_agent} | errors: {agent_errors} | error rate: {agent_error_rate}\n"
                f"Agent p95: {int(agent_stats['p95_ms'] or 0)}ms | avg: {int(agent_stats['avg_ms'] or 0)}ms\n"
                f"MCP calls: {total_mcp} | errors: {mcp_errors} | error rate: {mcp_error_rate}\n"
                f"MCP p95: {int(mcp_stats['p95_ms'] or 0)}ms | p99: {int(mcp_stats['p99_ms'] or 0)}ms"
            ),
            "type": "info",
        })
        if findings:
            lines = [f"[{f['severity']}] {f['agent_id']}: {f['title']} ({f['created_at'].date()})"
                     for f in findings]
            sections.append({
                "heading": "Weekly Scorecard — Open Findings",
                "content": "\n".join(lines),
                "type": "warning" if any(f["severity"] == "critical" for f in findings) else "info",
            })
        if top_errors:
            lines = [f"{t['agent_id']}: {t['errors']} error(s)" for t in top_errors]
            sections.append({
                "heading": "Weekly Scorecard — Top Error Agents",
                "content": "\n".join(lines),
                "type": "warning",
            })

        return sections

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

        # ── 3b. Weekly scorecard (Mondays only) ─────────────────────────
        is_monday = now.weekday() == 0
        scorecard_sections = []
        if is_monday:
            try:
                scorecard_sections = await self._weekly_scorecard_sections(p)
                metrics["scorecard_included"] = True
            except Exception as e:
                ctx.log.warning("Scorecard generation failed: %s", e)

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

        # Append weekly scorecard on Mondays
        if scorecard_sections:
            digest_sections.append({
                "heading": "📊 Weekly Scorecard",
                "content": "",
                "type": "code",
            })
            digest_sections.extend(scorecard_sections)

        # ── 5. Send the digest ────────────────────────────────────────────
        if not _EMAIL_AVAILABLE:
            await ctx.action("Email not configured — digest built but not sent")
            return {"summary": "Daily Digest: email not configured", "metrics": metrics}

        date_str   = now.strftime("%B %-d, %Y")
        suffix     = " + Weekly Scorecard" if scorecard_sections else ""
        email_subj = f"Athena Daily Report — {date_str}{suffix}"

        body_html = build_notification_html(
            title=email_subj,
            sections=digest_sections,
            footer="Athena Cognitive Engine - Developed by William Nelson 2016",
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
