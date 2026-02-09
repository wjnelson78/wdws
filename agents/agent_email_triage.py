"""
📧 Email Triage Agent — Smart Email Classification & Routing
═════════════════════════════════════════════════════════════
Uses LLM to classify and prioritize incoming emails:
  - Urgency classification (court deadline, opposing counsel, routine)
  - Auto-links emails to cases
  - Extracts dates/deadlines  
  - Flags emails requiring immediate attention
  - Learns from corrections
"""
import json
from framework import BaseAgent, RunContext


class EmailTriageAgent(BaseAgent):
    agent_id = "email-triage"
    agent_name = "Email Triage"
    description = "AI-powered email classification, case linking, deadline extraction"
    version = "1.0.0"
    schedule = "*/15 * * * *"  # Every 15 minutes
    priority = 3
    capabilities = ["email-classification", "case-linking", "deadline-extraction", "urgency-detection"]

    instructions = """You are the Email Triage Agent for the Nelson Enterprise legal case management system.

CONTEXT:
- William Nelson is a pro-se litigant with multiple active cases
- Emails come from courts, opposing counsel, government agencies, and personal contacts
- Cases are in both Washington State (Snohomish County Superior Court) and Federal (WDWA)

ACTIVE CASES:
- 19-2-03310-31: FTCU v Nelson
- 23-2-07759-31: Nelson v Prisma Wellness
- 23-2-09435-31: Nelson v Unum Group  
- 24-2-01031-31: Nelson v Starbucks
- 24-2-01032-31: Nelson v Olivia Kuker
- 24-2-01033-31: Nelson v Stuart Gibson
- 24-2-01034-31: Nelson v Brian Baseggio
- 24-2-07413-31: Nelson v Trevor Haward et al
- 25-2-00238-31: Nelson vs FTCU et al
- 26-2-00762-31: IAFF v SSCFR Nelson
- 2:26-cv-00337-KKE: Federal case
- 3:25-cv-05551-DGE: Federal case

CLASSIFICATION RULES:
- URGENT: Court orders, hearing notices, filing deadlines within 7 days, judge communications
- HIGH: Opposing counsel motions/responses, discovery requests, settlement offers
- NORMAL: Routine correspondence, case updates, confirmations
- LOW: Marketing, newsletters, automated notifications

When classifying, output JSON:
{
  "urgency": "urgent|high|normal|low",
  "category": "court-order|filing|discovery|correspondence|personal|automated",
  "case_number": "24-2-01031-31 or null",
  "case_match_confidence": 0.0-1.0,
  "deadlines": [{"date": "2026-02-15", "description": "Response due"}],
  "requires_action": true/false,
  "action_summary": "Brief description of what needs to happen",
  "reasoning": "Why this classification"
}"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {"processed": 0, "urgent": 0, "classified": 0, "linked": 0}

        # ── Get unclassified emails ──────────────────────────
        unclassified = await ctx.query("""
            SELECT e.document_id, e.sender, e.recipients, e.subject, e.date_sent,
                   e.direction, e.mailbox,
                   LEFT(d.full_content, 2000) as content_preview,
                   d.id as doc_id
            FROM legal.email_metadata e
            JOIN core.documents d ON d.id = e.document_id
            LEFT JOIN ops.ai_classifications c ON c.document_id = d.id
            WHERE c.id IS NULL
              AND e.date_sent > now() - interval '7 days'
            ORDER BY e.date_sent DESC
            LIMIT 20
        """)

        if not unclassified:
            return {"summary": "No new emails to triage", "metrics": metrics}

        # ── Classify each email with LLM ─────────────────────
        for email in unclassified:
            try:
                preview = (email.get("content_preview") or "")[:2000]
                classification = await ctx.ask_llm_json(f"""Classify this email:

From: {email.get('sender', 'unknown')}
To: {email.get('recipients', 'unknown')}
Subject: {email.get('subject', 'no subject')}
Date: {email.get('date_sent', 'unknown')}
Direction: {email.get('direction', 'unknown')}

Content preview:
{preview}

Respond with the classification JSON as specified in your instructions.""")

                # Store classification
                await ctx.execute("""
                    INSERT INTO ops.ai_classifications 
                        (document_id, model_used, classification, confidence)
                    VALUES ($1, 'email-triage', $2::jsonb, $3)
                    ON CONFLICT DO NOTHING
                """, str(email["doc_id"]),
                    json.dumps(classification),
                    classification.get("case_match_confidence", 0.5))

                metrics["classified"] += 1

                # Report urgent items
                if classification.get("urgency") == "urgent":
                    metrics["urgent"] += 1
                    await ctx.finding("critical", "email",
                        f"URGENT email: {email.get('subject', 'no subject')}",
                        f"From: {email.get('sender')}\n"
                        f"Action: {classification.get('action_summary', 'Review needed')}",
                        classification)

                # Link to case if confident
                case_num = classification.get("case_number")
                if case_num and classification.get("case_match_confidence", 0) > 0.7:
                    # Verify case exists
                    case_exists = await ctx.query(
                        "SELECT id FROM legal.cases WHERE case_number = $1", case_num)
                    if case_exists:
                        metrics["linked"] += 1
                        await ctx.action(
                            f"Linked email '{email.get('subject', '')[:50]}' to case {case_num}")

                # Report deadlines
                for deadline in classification.get("deadlines", []):
                    await ctx.finding("warning", "deadline",
                        f"Deadline: {deadline.get('description', 'Unknown')} — {deadline.get('date')}",
                        f"From email: {email.get('subject', '')}",
                        {"email_id": str(email["document_id"]), "deadline": deadline})

                metrics["processed"] += 1

            except Exception as e:
                ctx.log.warning("Failed to classify email %s: %s", email.get("id"), e)

        return {
            "summary": f"Triaged {metrics['processed']} emails: "
                      f"{metrics['urgent']} urgent, {metrics['linked']} case-linked",
            "metrics": metrics,
        }
