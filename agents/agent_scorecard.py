"""
📈 Weekly Scorecard Agent — Quality & Reliability Summary
═════════════════════════════════════════════════════════════
Sends a weekly executive summary of quality, reliability, and safety
metrics via Microsoft Graph API.
"""
import json
from datetime import datetime, timezone

from framework import BaseAgent, RunContext
from config import (
    GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
    GRAPH_SENDER_EMAIL, SCORECARD_EMAIL,
)
from email_util import send_email, build_notification_html


class ScorecardAgent(BaseAgent):
    agent_id = "scorecard"
    agent_name = "Scorecard"
    description = "Weekly quality + reliability scorecard email"
    version = "1.0.0"
    schedule = "0 7 * * 1"  # Mondays @ 7:00am
    priority = 3
    capabilities = ["reporting", "scorecard", "quality-metrics", "reliability"]

    instructions = "You generate weekly quality and reliability summaries."

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}
        p = await ctx.db()

        # ── Quality Eval (last 30 days) ─────────────────────
        eval_runs = await p.fetch("""
            SELECT id, status, started_at, summary, metrics
            FROM ops.agent_runs
            WHERE agent_id = 'quality-eval'
              AND started_at > now() - interval '30 days'
            ORDER BY started_at DESC
            LIMIT 5
        """)

        latest_eval = None
        pass_rates = []
        for r in eval_runs:
            m = r["metrics"] or {}
            if isinstance(m, str):
                try:
                    m = json.loads(m)
                except Exception:
                    m = {}
            pass_rate = m.get("pass_rate")
            if pass_rate is not None:
                pass_rates.append(float(pass_rate))
            if latest_eval is None:
                latest_eval = {"status": r["status"], "started_at": r["started_at"], "metrics": m, "summary": r["summary"]}

        avg_pass_rate = round(sum(pass_rates) / len(pass_rates), 3) if pass_rates else None

        # ── Reliability (last 7 days) ───────────────────────
        agent_stats = await p.fetchrow("""
            SELECT COUNT(*) AS total_runs,
                   COUNT(*) FILTER (WHERE status = 'error') AS errors,
                   AVG(duration_ms)::int AS avg_ms,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms
            FROM ops.agent_runs
            WHERE started_at > now() - interval '7 days'
        """)

        mcp_stats = await p.fetchrow("""
            SELECT COUNT(*) AS total_calls,
                   COUNT(*) FILTER (WHERE error IS NOT NULL) AS errors,
                   AVG(duration_ms)::int AS avg_ms,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
                   PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99_ms
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '7 days'
        """)

        # ── Open Findings (last 7 days) ────────────────────
        findings = await p.fetch("""
            SELECT severity, agent_id, title, created_at
            FROM ops.agent_findings
            WHERE status = 'open'
              AND created_at > now() - interval '7 days'
            ORDER BY
              CASE severity WHEN 'critical' THEN 3 WHEN 'warning' THEN 2 ELSE 1 END DESC,
              created_at DESC
            LIMIT 10
        """)

        # ── Top Error Agents (last 7 days) ──────────────────
        top_errors = await p.fetch("""
            SELECT agent_id, COUNT(*) AS errors
            FROM ops.agent_runs
            WHERE status = 'error'
              AND started_at > now() - interval '7 days'
            GROUP BY agent_id
            ORDER BY errors DESC
            LIMIT 5
        """)

        # ── Build email sections ────────────────────────────
        sections = []

        if latest_eval:
            m = latest_eval["metrics"] or {}
            sections.append({
                "heading": "Quality (Eval Results)",
                "content": (
                    f"Latest Eval: {latest_eval['summary'] or '—'}\n"
                    f"Pass rate: {m.get('pass_rate', '—')}\n"
                    f"Passed: {m.get('passed', 0)} | Failed: {m.get('failed', 0)} | Skipped: {m.get('skipped', 0)}\n"
                    f"Avg results: {m.get('avg_results', '—')}\n"
                    f"Avg pass rate (30d): {avg_pass_rate if avg_pass_rate is not None else '—'}"
                ),
                "type": "success" if (m.get("failed", 0) == 0) else "warning",
            })
        else:
            sections.append({
                "heading": "Quality (Eval Results)",
                "content": "No eval runs in the last 30 days.",
                "type": "warning",
            })

        total_agent = agent_stats["total_runs"] or 0
        agent_errors = agent_stats["errors"] or 0
        agent_error_rate = round(agent_errors / max(total_agent, 1), 3)

        total_mcp = mcp_stats["total_calls"] or 0
        mcp_errors = mcp_stats["errors"] or 0
        mcp_error_rate = round(mcp_errors / max(total_mcp, 1), 3)

        sections.append({
            "heading": "Reliability (last 7 days)",
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
                "heading": "Open Findings (last 7 days)",
                "content": "\n".join(lines),
                "type": "warning" if any(f["severity"] == "critical" for f in findings) else "info",
            })
        else:
            sections.append({
                "heading": "Open Findings (last 7 days)",
                "content": "No open findings this week.",
                "type": "success",
            })

        if top_errors:
            lines = [f"{t['agent_id']}: {t['errors']} error(s)" for t in top_errors]
            sections.append({
                "heading": "Top Error Agents (last 7 days)",
                "content": "\n".join(lines),
                "type": "warning",
            })

        subject = "Athena Cognitive Platform — Weekly Quality Scorecard"
        body_html = build_notification_html(
            title="Weekly Quality & Reliability Scorecard",
            sections=sections,
            footer="Athena Cognitive Platform — Scorecard Agent",
        )

        result = await send_email(
            tenant_id=GRAPH_TENANT_ID,
            client_id=GRAPH_CLIENT_ID,
            client_secret=GRAPH_CLIENT_SECRET,
            sender=GRAPH_SENDER_EMAIL,
            to_recipients=[SCORECARD_EMAIL],
            subject=subject,
            body_html=body_html,
            importance="normal",
        )

        # Log email in DB
        await p.execute("""
            INSERT INTO ops.email_notifications
                (agent_id, run_id, recipient, subject, body_html, status, error, graph_msg_id, sent_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, self.agent_id, self._run_id, SCORECARD_EMAIL, subject, body_html,
            result["status"], result.get("error"), result.get("message_id"),
            datetime.now(timezone.utc) if result["status"] == "sent" else None)

        await ctx.log_comms("email", "scorecard",
            f"Scorecard email {result['status']} to {SCORECARD_EMAIL}",
            {"result": result})

        metrics.update({
            "eval_avg_pass_rate": avg_pass_rate,
            "eval_latest_pass_rate": (latest_eval or {}).get("metrics", {}).get("pass_rate") if latest_eval else None,
            "agent_error_rate": agent_error_rate,
            "mcp_error_rate": mcp_error_rate,
            "email_status": result["status"],
        })

        summary = f"Scorecard sent: {result['status']} (eval avg pass={avg_pass_rate})"
        return {"summary": summary, "metrics": metrics}
