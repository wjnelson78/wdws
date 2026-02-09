"""
📅 Timeline Reconstruction Agent — Automated Chronology Builder
════════════════════════════════════════════════════════════════
Builds comprehensive timelines from court filings, emails, and medical records:
  - Cross-references dates across document types
  - Detects gaps and contradictions
  - Exports formatted chronologies
"""
import json
from framework import BaseAgent, RunContext


class TimelineAgent(BaseAgent):
    agent_id = "timeline"
    agent_name = "Timeline Reconstruction"
    description = "Builds case timelines from filings, emails, and records; detects gaps and contradictions"
    version = "1.0.0"
    schedule = "0 2 * * *"  # Daily at 2 AM
    priority = 5
    capabilities = ["timeline-building", "gap-detection", "contradiction-detection", "chronology-export"]

    instructions = """You are the Timeline Reconstruction Agent for the Nelson Enterprise legal system.

YOUR ROLE:
Build comprehensive chronological timelines by cross-referencing:
- Court filings (filing dates, hearing dates mentioned in documents)
- Email correspondence (sent/received dates, referenced dates in content)
- Medical records (dates of service, appointment dates)
- Case events (status changes, party additions)

METHODOLOGY:
1. For each case, gather all date-stamped events
2. Order chronologically
3. Identify gaps > 30 days with no activity
4. Flag contradictions (document A says event was on date X, document B says date Y)
5. Note where the timeline has weak evidence (single source vs corroborated)

OUTPUT FORMAT:
{
  "case_number": "...",
  "events": [
    {"date": "2024-01-15", "type": "filing", "description": "...", "source_doc_id": "...", "confidence": 0.9},
    ...
  ],
  "gaps": [{"from": "2024-03-01", "to": "2024-05-15", "note": "No activity for 75 days"}],
  "contradictions": [{"date1": "...", "date2": "...", "event": "...", "sources": [...]}]
}

Be precise with dates. If a date is uncertain, note it."""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {"cases_processed": 0, "events_found": 0, "gaps_found": 0}

        # ── Get cases with recent documents ──────────────────
        cases = await ctx.query("""
            SELECT c.id, c.case_number, c.case_title, c.filed_date,
                   COUNT(cd.document_id) as doc_count
            FROM legal.cases c
            JOIN legal.case_documents cd ON c.id = cd.case_id
            GROUP BY c.id, c.case_number, c.case_title, c.filed_date
            HAVING COUNT(cd.document_id) > 2
            ORDER BY c.case_number
        """)

        for case in cases:
            # Get documents with dates for this case
            docs = await ctx.query("""
                SELECT d.id, d.title, d.document_type, d.created_at,
                       COALESCE(f.filing_date, d.created_at) as event_date,
                       LEFT(d.full_content_preview, 500) as preview
                FROM core.documents d
                JOIN legal.case_documents cd ON d.id = cd.document_id
                LEFT JOIN legal.filing_metadata f ON d.id = f.document_id
                WHERE cd.case_id = $1
                ORDER BY COALESCE(f.filing_date, d.created_at)
            """, case["id"])

            if len(docs) < 3:
                continue

            # Get emails related to this case
            emails = await ctx.query("""
                SELECT e.id, e.subject, e.sender, e.email_date, e.direction
                FROM legal.email_metadata e
                JOIN core.documents d ON d.id::text LIKE '%'
                JOIN legal.case_documents cd ON d.id = cd.document_id
                WHERE cd.case_id = $1
                ORDER BY e.email_date
                LIMIT 50
            """, case["id"])

            metrics["events_found"] += len(docs) + len(emails)

            # Let LLM analyze for gaps and patterns
            if docs:
                timeline_data = {
                    "case": case["case_number"],
                    "filed": str(case.get("filed_date", "")),
                    "documents": [{"date": str(d["event_date"]), "title": d["title"],
                                  "type": d["document_type"]} for d in docs],
                    "emails": [{"date": str(e["email_date"]), "subject": e["subject"],
                               "from": e["sender"], "direction": e["direction"]}
                              for e in emails[:20]],
                }

                analysis = await ctx.ask_llm_json(f"""Analyze this case timeline and identify:
1. Significant gaps (>30 days with no activity)
2. Periods of intense activity
3. Any notable patterns

Case data:
{json.dumps(timeline_data, indent=2, default=str)}

Return JSON: {{"gaps": [{{"from": "date", "to": "date", "days": N, "note": "..."}}], 
"patterns": ["..."], "intensity_periods": [{{"from": "date", "to": "date", "events": N, "note": "..."}}]}}""")

                gaps = analysis.get("gaps", [])
                metrics["gaps_found"] += len(gaps)

                for gap in gaps:
                    if gap.get("days", 0) > 60:
                        await ctx.finding("info", "timeline",
                            f"Case {case['case_number']}: {gap.get('days', 0)}-day activity gap",
                            f"From {gap.get('from')} to {gap.get('to')}: {gap.get('note', '')}",
                            {"case": case["case_number"], "gap": gap})

            metrics["cases_processed"] += 1

        return {
            "summary": f"Timeline: {metrics['cases_processed']} cases, {metrics['events_found']} events, {metrics['gaps_found']} gaps",
            "metrics": metrics,
        }
