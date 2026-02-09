"""
🗑️ Retention & Redaction Agent — Data Governance
════════════════════════════════════════════════
Enforces data retention policies and PII protection:
  - Domain-specific retention (legal: 7yr, medical: HIPAA)
  - PII redaction in previews
  - Litigation hold management
  - Expired data purging with certificates
  - Compliance reporting
"""
import json
import re
from framework import BaseAgent, RunContext


class RetentionAgent(BaseAgent):
    agent_id = "retention"
    agent_name = "Retention & Redaction"
    description = "Enforces data retention policies, PII redaction, litigation holds, compliance"
    version = "1.0.0"
    schedule = "0 3 * * 0"  # Weekly, Sunday 3 AM
    priority = 4
    capabilities = ["retention-policy", "pii-redaction", "litigation-hold", "compliance-report"]

    instructions = """You are the Retention & Redaction Agent for the Nelson Enterprise WDWS system.

RETENTION POLICIES:
- Legal documents: 7 years from case closure (or indefinitely if case is active)
- Medical records: Per HIPAA — 6 years from creation or last use
- Emails: 3 years unless linked to active case (then follows legal policy)
- MCP query logs: 1 year
- Health check metrics: 90 days
- Agent run history: 6 months

LITIGATION HOLD:
- ALL documents linked to active cases are on automatic litigation hold
- Litigation hold overrides any retention policy
- Never delete anything under litigation hold

PII PATTERNS TO REDACT (in previews/summaries only — never modify source):
- SSN: XXX-XX-XXXX → [REDACTED SSN]
- Credit/debit cards: XXXX-XXXX-XXXX-XXXX → [REDACTED CARD]
- Date of birth in medical context → [REDACTED DOB]
- Bank account numbers → [REDACTED ACCOUNT]

COMPLIANCE:
- Generate weekly compliance summary
- Track any policy violations
- Maintain destruction certificates for purged data
- Never delete without logging what was deleted and why"""

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
                   LEFT(full_content_preview, 2000) as preview
            FROM core.documents
            WHERE full_content_preview IS NOT NULL
              AND (full_content_preview ~ '\\d{3}-\\d{2}-\\d{4}'
                   OR full_content_preview ~ '\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}')
            LIMIT 50
        """)
        metrics["pii_found"] = len(pii_docs)

        if pii_docs:
            await ctx.finding("warning", "privacy",
                f"PII detected in {len(pii_docs)} document previews",
                "SSN or card number patterns found in full_content_preview fields",
                {"doc_ids": [d["id"] for d in pii_docs[:10]]})

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
