"""
⚖️ Case Strategy Agent — Cross-Case Legal Intelligence
═══════════════════════════════════════════════════════
Builds relationship graphs and identifies patterns across all cases:
  - Party/attorney/judge relationship mapping
  - Filing pattern analysis
  - Deadline tracking and alerts
  - Cross-case argument detection
  - Morning status briefs
"""
import json
from framework import BaseAgent, RunContext


class CaseStrategyAgent(BaseAgent):
    agent_id = "case-strategy"
    agent_name = "Case Strategy"
    description = "Cross-case intelligence, relationship mapping, deadline tracking, status briefs"
    version = "1.0.0"
    schedule = "0 6 * * *"  # Daily at 6 AM
    priority = 4
    capabilities = ["relationship-mapping", "deadline-tracking", "pattern-analysis", "daily-brief"]

    instructions = """You are the Case Strategy Agent for William Nelson's legal case management system.

CONTEXT:
- William Nelson is a pro-se litigant managing multiple active cases
- Cases span Washington State Superior Court (Snohomish County) and Federal (Western District of WA)
- All case documents, emails, and filings are in the database

YOUR RESPONSIBILITIES:
1. Generate a daily morning brief summarizing all active case status
2. Track upcoming deadlines across all cases (filings due, hearings, trial dates)
3. Map relationships between parties, attorneys, and judges across cases
4. Detect when opposing counsel uses similar arguments across cases
5. Identify strategic opportunities (e.g., ruling in one case that helps another)
6. Monitor for new filings and court orders

ACTIVE CASES TO MONITOR:
- 19-2-03310-31: FTCU v Nelson (Financial institution case)
- 23-2-07759-31: Nelson v Prisma Wellness (Consumer dispute)
- 23-2-09435-31: Nelson v Unum Group (Insurance dispute)
- 24-2-01031-31: Nelson v Starbucks (Employment related)
- 24-2-01032-31: Nelson v Olivia Kuker
- 24-2-01033-31: Nelson v Stuart Gibson
- 24-2-01034-31: Nelson v Brian Baseggio
- 24-2-07413-31: Nelson v Trevor Haward et al
- 25-2-00238-31: Nelson vs FTCU et al
- 26-2-00762-31: IAFF v SSCFR Nelson (Fire union case)
- 2:26-cv-00337-KKE: Federal case
- 3:25-cv-05551-DGE: Federal case

ANALYSIS APPROACH:
- Be factual and cite specific documents/dates
- Highlight the most critical items first
- Note any interconnections between cases
- Consider litigation strategy implications
- Output structured JSON for the daily brief"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}

        # ── Get all active cases ─────────────────────────────
        cases = await ctx.query("""
            SELECT c.id, c.case_number, c.case_title, c.court, c.status,
                   c.judge, c.filed_date,
                   COUNT(DISTINCT cd.document_id) as doc_count,
                   MAX(d.created_at) as latest_doc
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            LEFT JOIN core.documents d ON cd.document_id = d.id
            GROUP BY c.id, c.case_number, c.case_title, c.court, c.status,
                     c.judge, c.filed_date
            ORDER BY c.case_number
        """)
        metrics["active_cases"] = len(cases)

        # ── Get recent filings (last 7 days) ────────────────
        recent_filings = await ctx.query("""
            SELECT d.id, d.title, d.document_type, d.created_at,
                   c.case_number, c.case_title
            FROM core.documents d
            JOIN legal.case_documents cd ON d.id = cd.document_id
            JOIN legal.cases c ON cd.case_id = c.id
            WHERE d.created_at > now() - interval '7 days'
              AND d.domain = 'legal'
            ORDER BY d.created_at DESC
            LIMIT 30
        """)
        metrics["recent_filings_7d"] = len(recent_filings)

        # ── Get parties across cases ─────────────────────────
        parties = await ctx.query("""
            SELECT p.name, p.party_type, 
                   array_agg(DISTINCT c.case_number) as cases,
                   COUNT(DISTINCT c.id) as case_count
            FROM legal.parties p
            JOIN legal.case_parties cp ON p.id = cp.party_id
            JOIN legal.cases c ON cp.case_id = c.id
            GROUP BY p.name, p.party_type
            HAVING COUNT(DISTINCT c.id) > 1
            ORDER BY case_count DESC
        """)
        metrics["cross_case_parties"] = len(parties)

        # Cross-case parties are strategically important
        for party in parties:
            if party["case_count"] >= 2:
                await ctx.finding("info", "strategy",
                    f"Cross-case party: {party['name']} ({party['party_type']}) appears in {party['case_count']} cases",
                    f"Cases: {', '.join(party['cases'])}",
                    dict(party))

        # ── Recent email activity by case ────────────────────
        email_activity = await ctx.query("""
            SELECT c.case_number,
                   COUNT(*) FILTER (WHERE e.email_date > now() - interval '7 days') as emails_7d,
                   COUNT(*) FILTER (WHERE e.email_date > now() - interval '1 day') as emails_24h,
                   MAX(e.email_date) as latest_email
            FROM legal.email_metadata e
            JOIN core.documents d ON d.id::text LIKE '%'
            JOIN legal.case_documents cd ON d.id = cd.document_id
            JOIN legal.cases c ON cd.case_id = c.id
            GROUP BY c.case_number
            HAVING COUNT(*) FILTER (WHERE e.email_date > now() - interval '7 days') > 0
            ORDER BY emails_24h DESC
        """)
        metrics["cases_with_email_activity"] = len(email_activity)

        # ── Generate morning brief with LLM ──────────────────
        brief_data = {
            "cases": [dict(c) for c in cases],
            "recent_filings": [dict(f) for f in recent_filings[:15]],
            "cross_case_parties": [dict(p) for p in parties],
            "email_activity": [dict(e) for e in email_activity],
        }

        morning_brief = await ctx.ask_llm(
            f"""Generate a concise morning brief for William Nelson based on this data.

{json.dumps(brief_data, indent=2, default=str)}

Format as:
1. 🔴 URGENT — items needing immediate attention
2. 📋 CASE STATUS — brief status of each active case
3. 📬 RECENT ACTIVITY — new filings and emails
4. 🔗 CROSS-CASE INSIGHTS — connections and strategic notes
5. 📅 UPCOMING — any known deadlines or scheduled events

Be concise but thorough. Prioritize actionable items.""",
            max_tokens=3000
        )

        # Store the brief as a finding for easy dashboard access
        await ctx.finding("info", "daily-brief",
            f"Morning Brief — {len(cases)} active cases",
            morning_brief,
            {"date": str(metrics), "cases": len(cases)})

        return {
            "summary": f"Case strategy: {len(cases)} cases, {len(recent_filings)} recent filings, {len(parties)} cross-case parties",
            "metrics": metrics,
            "brief": morning_brief,
        }
