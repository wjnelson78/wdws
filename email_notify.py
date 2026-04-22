#!/usr/bin/env python3
"""
Email-arrival → SMS notification hook.

Called by email_sync.py after each newly-ingested email. Decides whether
the email warrants an SMS to William, and if so inserts an ops.notification_queue
row with channel='sms'. The actual send happens in notification_service.py
which reads the queue, enforces quiet hours + rate limiting, and dispatches
via telnyx_sms.

Filter triggers an SMS when ANY of these hold:
  - Sender's domain matches a hard-coded VIP domain list (courts, counsel,
    medical providers, insurance carriers)
  - Sender's email matches a VIP-specific contact (from email_sync_config
    TARGET_SPECIFIC_EMAILS)
  - Email's importance flag is "high"
  - Subject contains URGENT / ASAP / COURT ORDER / etc.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Optional

import asyncpg

log = logging.getLogger("email-notify")

# ---------------------------------------------------------------------------
# Filter configuration
# ---------------------------------------------------------------------------

# Always notify when sender's domain matches one of these.
SMS_TRIGGER_DOMAINS = {
    # Courts
    "courts.wa.gov", "snoco.org", "co.snohomish.wa.us", "co.chelan.wa.us",
    "ca9.uscourts.gov",  # 9th Circuit
    # Opposing counsel
    "workerlaw.com", "csdlaw.com", "cabornelaw.com", "clearpathpllc.com",
    "fwwlaw.com", "kantorlaw.net", "bn-lawyers.com",
    "ogletree.com", "ogletreedeakins.com", "lewisbrisbois.com", "grsm.com",
    "insleebest.com", "jmblawyers.com", "uscanadalaw.com", "favros.com",
    "cozen.com", "slwsd.com", "lldkb.com", "mhb.com",
    # Medical / insurance
    "evergreenhealthcare.org", "evergreenhealth.com", "wahbexchange.org",
    "providence.org", "uwmedicine.org", "swedish.org", "workwellfoundation.org",
    # Claims + benefits
    "sedgwick.com", "unum.com",
    # Starbucks (employer correspondence)
    "starbucks.com",
}

# Catch any *.uscourts.gov (federal court systems).
USCOURTS_RE = re.compile(r"\.uscourts\.gov$", re.IGNORECASE)

# Always notify when sender matches one of these exactly (from email_sync_config).
SMS_TRIGGER_SENDERS = {
    "fayejwonglawfirm@gmail.com",  # GAL Faye Wong
    "sara.c.murray@gmail.com",     # Sara Murray
}

# Urgency keywords (case-insensitive substring) in subject.
URGENCY_KEYWORDS = [
    "URGENT", "ASAP", "COURT ORDER", "HEARING", "SUBPOENA", "MOTION",
    "TRO", "TEMPORARY RESTRAINING", "EMERGENCY", "TIME SENSITIVE",
    "DEADLINE", "RESPONSE DUE", "OBJECTION", "APPEAL",
]

_URGENCY_RE = re.compile("|".join(re.escape(k) for k in URGENCY_KEYWORDS), re.IGNORECASE)


# ---------------------------------------------------------------------------
# Filter + format
# ---------------------------------------------------------------------------

def _extract_sender_email(raw_sender: str) -> str:
    """Pull the bare email from a 'Name <email>' or 'email' string."""
    m = re.search(r"<([^>]+)>", raw_sender or "")
    if m:
        return m.group(1).strip().lower()
    return (raw_sender or "").strip().lower()


def _sender_domain(raw_sender: str) -> str:
    em = _extract_sender_email(raw_sender)
    if "@" in em:
        return em.split("@", 1)[1]
    return ""


def _sender_display(raw_sender: str) -> str:
    """Short label: display name if present, else the email, truncated."""
    raw = (raw_sender or "").strip()
    # 'Name <email>' → take Name
    m = re.match(r'^"?([^"<]+?)"?\s*<[^>]+>$', raw)
    if m:
        return m.group(1).strip()[:30]
    em = _extract_sender_email(raw)
    return em[:40]


def should_sms(*, sender: str, subject: str, importance: Optional[str] = None) -> tuple[bool, str]:
    """Return (trigger, reason). Never sends SMS for promo/auto/support bots."""
    domain = _sender_domain(sender)
    email_lc = _extract_sender_email(sender)

    # Never notify for automated / no-reply senders, even if their domain matches
    if any(bot in email_lc for bot in (
        "noreply@", "no-reply@", "donotreply@", "do-not-reply@",
        "auto@", "automated@", "mailer-daemon@", "postmaster@",
    )):
        return False, "auto_sender_suppressed"

    if email_lc in SMS_TRIGGER_SENDERS:
        return True, f"vip_sender:{email_lc}"

    if domain in SMS_TRIGGER_DOMAINS:
        return True, f"trigger_domain:{domain}"
    if USCOURTS_RE.search(domain):
        return True, f"federal_court:{domain}"

    if (importance or "").lower() == "high":
        return True, "importance_high"

    if _URGENCY_RE.search(subject or ""):
        return True, "urgency_keyword"

    return False, "no_match"


def format_sms(*, sender: str, subject: str, received_at: Optional[datetime] = None) -> str:
    """Build the SMS body. Target ≤ 160 chars for single-part GSM-7 delivery."""
    who = _sender_display(sender)
    subj = (subject or "(no subject)").strip()
    # Collapse whitespace + strip non-printable
    subj = re.sub(r"\s+", " ", subj)
    # When received — short local time
    t = ""
    if received_at:
        try:
            t = f" @ {received_at.strftime('%H:%M')}"
        except Exception:
            t = ""

    prefix = "[ATHENA] "
    # Budget: 160 - len(prefix) - len(who) - 2 (": ") - len(t)
    reserve = len(prefix) + len(who) + 2 + len(t)
    avail = max(20, 160 - reserve)
    if len(subj) > avail:
        subj = subj[: avail - 1] + "…"
    return f"{prefix}{who}: {subj}{t}"


# ---------------------------------------------------------------------------
# Queue insert (called from email_sync)
# ---------------------------------------------------------------------------

async def queue_email_sms(
    pool: asyncpg.Pool,
    *,
    sender: str,
    subject: str,
    received_at: Optional[datetime] = None,
    importance: Optional[str] = None,
    graph_id: Optional[str] = None,
    mailbox: Optional[str] = None,
) -> Optional[str]:
    """If the email matches the SMS filter, insert a queue row.
    Returns the match reason (or None if skipped)."""

    trigger, reason = should_sms(sender=sender, subject=subject, importance=importance)
    if not trigger:
        return None

    body = format_sms(sender=sender, subject=subject, received_at=received_at)
    sections = [
        {
            "heading": "New priority email",
            "content": body,
            "meta": {
                "sender": sender,
                "subject": subject,
                "received_at": received_at.isoformat() if received_at else None,
                "importance": importance,
                "graph_id": graph_id,
                "mailbox": mailbox,
                "trigger_reason": reason,
            },
        }
    ]

    await pool.execute(
        """
        INSERT INTO ops.notification_queue
            (agent_id, subject, sections, severity, channel, priority)
        VALUES ($1, $2, $3::jsonb, $4, $5, $6)
        """,
        "email_notify",
        body,  # subject column holds the SMS text (dispatcher uses this verbatim)
        json.dumps(sections),
        "info",
        "sms",
        "immediate",
    )
    log.info(f"SMS queued for email from {sender[:40]}: {reason}")
    return reason


if __name__ == "__main__":
    # Quick smoke test of the filter logic
    cases = [
        ("Sara Murray <sara.c.murray@gmail.com>", "Re: Strategic Update"),
        ("noreply@starbucks.com", "Your order"),
        ("clerk@courts.wa.gov", "Order on Motion"),
        ("Bobby Hair <bobby@playfasa.com>", "Website issue"),
        ("random@google.com", "Notification"),
        ("counsel@ogletree.com", "Deposition scheduling"),
        ("anyone@somewhere.com", "URGENT: hearing tomorrow"),
    ]
    for sender, subject in cases:
        trig, reason = should_sms(sender=sender, subject=subject)
        msg = format_sms(sender=sender, subject=subject) if trig else "—"
        print(f"{'✓' if trig else '✗'} [{reason:30s}]  {sender[:45]:45s}  {subject[:40]}")
        if trig:
            print(f"   SMS: {msg}")
