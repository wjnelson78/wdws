#!/usr/bin/env python3
"""
Send the 6 AM morning brief with Athena's status report.
"""
import asyncio
import sys
from datetime import datetime, timezone
from email_util import send_email, build_notification_html
from config import (
    GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
    GRAPH_SENDER_EMAIL, ALERT_EMAIL,
)


async def send_morning_brief():
    """Send the 6 AM morning brief with Athena's status report."""

    sections = [
        {
            "heading": "📋 What Athena Did (Last 24h)",
            "plain_summary": "Delegated MCP fix to Code Doctor (SUCCESS), extended watchdog exclusions, maintained monitoring on schema drift and CPU spikes",
            "content": """• Delegated MCP server fix ('Response' object has no attribute 'get') to code-doctor — outcome: SUCCESS
• Extended watchdog latency exclusion list to include quality-eval (config_change, verified stable)
• Maintained monitoring stance on 37 schema-drift tables and 2615 self-healing false-positive mismatch signals
• Sustained no_action on 6 recurring CPU spikes (96.3%–99.6%) that each self-resolved within minutes""",
            "type": "success",
        },
        {
            "heading": "🔴 OPEN ISSUES — AWAITING YOUR DECISION",
            "plain_summary": "3 items require your attention: pending schema approval, CPU spike investigation, and schema drift handling",
            "content": """1. ops.pending_approvals id=1 — CREATE TABLE ops.notification_queue
   Queued: 2026-03-07 | Expired: 2026-03-09 | Now: 31+ days overdue
   To approve: UPDATE ops.pending_approvals SET status='approved' WHERE id=1
   To reject: UPDATE ops.pending_approvals SET status='rejected' WHERE id=1

2. CPU Spikes: 6 spikes >95% in last 24h (most recent: 99.6% at 05:56 UTC)
   Each resolved without intervention
   Action: If you want root-cause investigation, authorize Athena to run process snapshot during next spike

3. Schema Drift: 37 chat.* and web.* tables defined in migrations but absent from DB
   Athena has not auto-created these (exceeds additive-only bounds)
   Action: Confirm whether tables are needed or retire migrations""",
            "type": "warning",
        },
        {
            "heading": "📊 SYSTEM HEALTH SUMMARY",
            "plain_summary": "All systems nominal. CPU 23.6%, Memory 45.7%, DB healthy with 5/200 connections",
            "content": """Resources:
  CPU: 23.6% (nominal)
  Memory: 45.7%
  Disk: 40.3% (62.6 GB free)
  Swap: 16.7%

Database:
  Connections: 5/200 active
  Deadlocks: 0
  Size: 1141 MB

Services:
  Tunnel: healthy
  OpenAI API: operational
  Agent fleet: all healthy except quality-eval (absent from 2h window)

Code Doctor Performance:
  Successes: 17 | Errors: 2 (2h window, ~10.5% error rate)""",
            "type": "info",
        },
        {
            "heading": "🎯 WHAT ATHENA PLANS NEXT",
            "plain_summary": "Continue monitoring CPU pattern, allow quality-eval false positive to age out, await schema decision",
            "content": """• Continue monitoring CPU spike pattern; escalate to security-sentinel if spike sustains >10 minutes
• Allow Quality Eval fleet-health false positive to age out of orchestrator tracking window
• Await your decision on ops.pending_approvals id=1 before taking any schema action
• Monitor code-doctor timeout rate; escalate if error rate exceeds 25%""",
            "type": "info",
        }
    ]

    now = datetime.now(timezone.utc)
    date_str = now.strftime("%B %-d, %Y")
    email_subject = f"Athena 6 AM Morning Brief — {date_str}"

    body_html = build_notification_html(
        title=email_subject,
        sections=sections,
        footer="Athena Cognitive Engine - Developed by William Nelson 2016"
    )

    result = await send_email(
        tenant_id=GRAPH_TENANT_ID,
        client_id=GRAPH_CLIENT_ID,
        client_secret=GRAPH_CLIENT_SECRET,
        sender=GRAPH_SENDER_EMAIL,
        to_recipients=[ALERT_EMAIL],
        subject=email_subject,
        body_html=body_html,
        importance="high",
    )

    if result["status"] == "sent":
        print(f"✅ Morning brief sent to {ALERT_EMAIL}")
        return 0
    else:
        print(f"❌ Failed to send morning brief: {result.get('error', 'unknown error')}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(send_morning_brief())
    sys.exit(exit_code)
