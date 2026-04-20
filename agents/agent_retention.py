"""
🗑️ Retention Agent — Operational Data Governance
═════════════════════════════════════════════════
Enforces retention policies for operational data and detects PII exposure:
  - Litigation hold verification (read-only)
  - PII pattern detection in document previews
  - Expired operational data purging (health checks, old logs, agent runs)
  - Weekly compliance summary
"""
import json
from framework import BaseAgent, RunContext


class RetentionAgent(BaseAgent):
    agent_id = "retention"
    agent_name = "Retention"
    description = "Purges expired operational data, detects PII exposure, verifies litigation holds"
    version = "1.1.0"
    schedule = "0 3 * * 0"  # Weekly, Sunday 3 AM
    priority = 4
    capabilities = ["retention-policy", "pii-detection", "litigation-hold", "compliance-report"]

    instructions = """You are the Retention Agent for the Athena Cognitive Engine.

OPERATIONAL RETENTION POLICIES (what this agent actually enforces):
- Health check metrics: purge after 90 days
- MCP query logs: purge after 1 year
- Agent run history: purge after 6 months

LITIGATION HOLD (read-only verification):
- Documents linked to active cases are protected — this agent verifies
  the hold is in place but does NOT modify case documents.

PII DETECTION (detection only — no redaction):
- Scans document previews for SSN and credit card patterns
- Flags documents with PII for manual review

RULES:
- Never delete case-related documents
- Log everything that is purged with counts
- Generate a weekly compliance summary"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {"scanned": 0, "holds": 0, "pii_found": 0, "purged": 0}

        # ── Check litigation holds ───────────────────────────
        active_cases = await ctx.query("""
            SELECT c.id, c.case_number, c.status,
                   COUNT(cd.document_id) as doc_count
            FROM legal.cases c
            JOIN legal.case_documents cd ON c.id = cd.case_id
            WHERE c.status NOT IN ('closed', 'dismissed')
            GROUP BY c.id, c.case_number, c.status
        """)
        metrics["holds"] = len(active_cases)
        held_doc_count = sum(c["doc_count"] for c in active_cases)
        metrics["docs_under_hold"] = held_doc_count

        await ctx.finding("info", "compliance",
            f"Litigation hold: {held_doc_count} documents across {len(active_cases)} active cases",
            "All documents linked to active cases are protected from deletion",
            {"cases": [c["case_number"] for c in active_cases]})

        # ── Scan for PII in document previews ────────────────
        pii_patterns = [
            (r'\d{3}-\d{2}-\d{4}', 'SSN'),
            (r'\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}', 'Credit Card'),
            (r'\b\d{9,12}\b', 'Account Number'),
        ]

        pii_docs = await ctx.query("""
            SELECT id, title, domain,
                   LEFT(full_content, 2000) as preview
            FROM core.documents
            WHERE full_content IS NOT NULL
              AND (full_content ~ '\\d{3}-\\d{2}-\\d{4}'
                   OR full_content ~ '\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}')
            LIMIT 50
        """)
        metrics["pii_found"] = len(pii_docs)

        if pii_docs:
            await ctx.finding("warning", "privacy",
                f"PII detected in {len(pii_docs)} document previews",
                "SSN or card number patterns found in full_content fields",
                {"doc_ids": [str(d["id"]) for d in pii_docs[:10]]})

        # ── Check for expired data ───────────────────────────
        # MCP query logs > 1 year
        old_logs = await ctx.query("""
            SELECT COUNT(*) as count
            FROM ops.mcp_query_log
            WHERE created_at < now() - interval '1 year'
        """)
        old_log_count = old_logs[0]["count"] if old_logs else 0

        # Health checks > 90 days
        old_health = await ctx.query("""
            SELECT COUNT(*) as count
            FROM ops.health_checks
            WHERE checked_at < now() - interval '90 days'
        """)
        old_health_count = old_health[0]["count"] if old_health else 0

        # Agent runs > 6 months
        old_runs = await ctx.query("""
            SELECT COUNT(*) as count
            FROM ops.agent_runs
            WHERE started_at < now() - interval '6 months'
        """)
        old_run_count = old_runs[0]["count"] if old_runs else 0

        if old_log_count + old_health_count + old_run_count > 0:
            await ctx.finding("info", "retention",
                f"Expired data: {old_log_count} old query logs, {old_health_count} old health checks, {old_run_count} old agent runs",
                "These records exceed retention policy and can be purged")

            # Purge expired operational data (safe — not case data)
            if old_health_count > 0:
                await ctx.execute(
                    "DELETE FROM ops.health_checks WHERE checked_at < now() - interval '90 days'")
                metrics["purged"] += old_health_count
                await ctx.action(f"Purged {old_health_count} health check records > 90 days")

            if old_log_count > 0:
                await ctx.execute(
                    "DELETE FROM ops.mcp_query_log WHERE created_at < now() - interval '1 year'")
                metrics["purged"] += old_log_count
                await ctx.action(f"Purged {old_log_count} MCP query log records > 1 year")

            if old_run_count > 0:
                await ctx.execute(
                    "DELETE FROM ops.agent_runs WHERE started_at < now() - interval '6 months'")
                metrics["purged"] += old_run_count
                await ctx.action(f"Purged {old_run_count} agent run records > 6 months")

        # ── Compliance summary ───────────────────────────────
        summary_data = {
            "active_litigation_holds": len(active_cases),
            "documents_under_hold": held_doc_count,
            "pii_exposures_found": len(pii_docs),
            "expired_data_found": old_log_count + old_health_count + old_run_count,
            "records_purged": metrics["purged"],
        }

        compliance_report = await ctx.ask_llm(f"""Generate a brief weekly compliance report:

Data:
{json.dumps(summary_data, indent=2)}

Include:
1. Compliance status (COMPLIANT / ACTION NEEDED)
2. Key findings
3. Recommended actions
4. HIPAA considerations for medical data""")

        await ctx.finding("info", "compliance-report",
            "Weekly Compliance Report",
            compliance_report,
            summary_data)

        return {
            "summary": f"Retention: {metrics['holds']} holds, {metrics['pii_found']} PII docs, {metrics['purged']} purged",
            "metrics": metrics,
        }
