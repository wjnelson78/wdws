#!/usr/bin/env python3
"""
Email-arrival → SMS notification hook.

Called by email_sync.py after each newly-ingested email. Decides whether
the email warrants an SMS to William, and if so inserts an ops.notification_queue
row with channel='sms'. The actual send happens in notification_service.py
which reads the queue, enforces quiet hours + rate limiting, and dispatches
via telnyx_sms.

Trigger rule (as of 2026-04-22, widened from a VIP-only allowlist):

  Fire SMS when ALL hold:
    1. Recipient mailbox matches SMS_MAILBOX (default: athena@seattleseahawks.me).
       Mail to William's personal mailbox (william@...) is intentionally silent
       — his phone is his own inbox, he doesn't need push-to-phone for it.
    2. Sender is a real human (not a noreply / auto / mailer-daemon bot).

  That's it. Every inbound human email to Athena gets a heads-up SMS; William
  can always respond to noise by adding specific senders to the bot list.

The VIP domain/sender lists are kept below for historical reference and in
case we want to restore tier-based priorities later, but they no longer gate
SMS.
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

# Only fire SMS for inbound email addressed to this mailbox. Anything to
# William's personal inbox stays silent.
SMS_MAILBOX = os.getenv("SMS_MAILBOX", "athena@seattleseahawks.me").lower()

# Noreply / bot senders that never warrant a phone ping, regardless of where
# they're addressed. Deliberately conservative — add more here (e.g. a
# specific newsletter that's pestering you) rather than inventing broad
# patterns that could suppress legitimate senders like "alerts@uscourts.gov".
SMS_SUPPRESS_SENDER_PATTERNS = (
    "noreply@", "no-reply@", "donotreply@", "do-not-reply@",
    "auto@", "automated@", "mailer-daemon@", "postmaster@",
)

# The following VIP lists are retained for reference and potential future use
# (tier-based urgency, prioritized queue order, etc.), but they no longer gate
# whether an SMS fires — any inbound-to-athena human email triggers now.
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


# Max age of an email before we stop firing SMS for it. Covers the common
# "delta bootstrap re-walks 1,754 inbox messages" case — we don't want to
# text William for every month-old mail that gets re-ingested.
SMS_MAX_AGE_MINUTES = 15


def should_sms(
    *,
    sender: str,
    subject: str,
    importance: Optional[str] = None,
    mailbox: Optional[str] = None,
    received_at: Optional[datetime] = None,
) -> tuple[bool, str]:
    """Return (trigger, reason).

    Rule: fire iff
      - addressed to the Athena mailbox, AND
      - sender is not a bot / auto-sender, AND
      - received within the last SMS_MAX_AGE_MINUTES (so bootstrap/backfill
        re-ingests of historical mail stay silent).
    """
    mbox = (mailbox or "").strip().lower()
    if not mbox:
        return False, "no_mailbox"
    if mbox != SMS_MAILBOX:
        return False, f"not_athena_mailbox:{mbox}"

    email_lc = _extract_sender_email(sender)
    if any(bot in email_lc for bot in SMS_SUPPRESS_SENDER_PATTERNS):
        return False, "auto_sender_suppressed"

    # Recency gate — only alert on actually-new mail.
    if received_at is not None:
        try:
            now_utc = datetime.now(received_at.tzinfo) if received_at.tzinfo else datetime.utcnow()
            age_min = (now_utc - received_at).total_seconds() / 60.0
            if age_min > SMS_MAX_AGE_MINUTES:
                return False, f"stale:{int(age_min)}min"
        except Exception:
            # If we can't compute age, err toward silence (don't spam).
            return False, "received_at_unparseable"
    else:
        return False, "no_received_at"

    return True, "athena_inbound"


def _first_two_lines(body: str) -> str:
    """Pull up to the first two non-empty, non-quoted lines of an email body.

    Skips leading blank lines, collapsed whitespace, common reply-header
    giveaways ('On ... wrote:', '>', 'From:', 'Sent:', etc.) so the preview
    shows actual new content rather than quoted history.
    """
    if not body:
        return ""
    out: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(">"):
            continue
        if re.match(r"^(on\s.*\swrote:|from:|sent:|to:|subject:|cc:|bcc:|date:)",
                    line, re.IGNORECASE):
            continue
        line = re.sub(r"\s+", " ", line)
        out.append(line)
        if len(out) == 2:
            break
    return " · ".join(out)


def format_sms(
    *,
    sender: str,
    subject: str,
    received_at: Optional[datetime] = None,
    body_preview: Optional[str] = None,
) -> str:
    """Build the SMS body.

    Format (multi-part; Telnyx handles concatenation):
        [ATHENA] Sender Name @ 14:23
        Subject line here
        First line of body · Second line
    """
    who = _sender_display(sender)
    subj = (subject or "(no subject)").strip()
    subj = re.sub(r"\s+", " ", subj)

    t = ""
    if received_at:
        try:
            t = f" @ {received_at.strftime('%H:%M')}"
        except Exception:
            t = ""

    # Line 1: header
    lines = [f"[ATHENA] {who}{t}"]
    # Line 2: subject (cap to ~100 chars)
    lines.append(subj[:100] + ("…" if len(subj) > 100 else ""))
    # Line 3: body preview (cap to ~200 chars — keeps total under 2 SMS parts)
    if body_preview:
        preview = _first_two_lines(body_preview)
        if preview:
            if len(preview) > 200:
                preview = preview[:199] + "…"
            lines.append(preview)
    return "\n".join(lines)


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
    body_preview: Optional[str] = None,
) -> Optional[str]:
    """If the email matches the SMS filter, insert a queue row.
    Returns the match reason (or None if skipped)."""

    trigger, reason = should_sms(
        sender=sender, subject=subject, importance=importance,
        mailbox=mailbox, received_at=received_at,
    )
    if not trigger:
        return None

    body = format_sms(
        sender=sender, subject=subject,
        received_at=received_at, body_preview=body_preview,
    )
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
    # Smoke test — exercises the new athena-mailbox-only rule plus the bot
    # suppression filter. Each case is (sender, subject, mailbox).
    ATHENA = "athena@seattleseahawks.me"
    WILLIAM = "william@seattleseahawks.me"
    cases = [
        # Human senders to athena — should ALL trigger
        ("Sara Murray <sara.c.murray@gmail.com>", "Re: Strategic Update", ATHENA),
        ("Bobby Hair <bobby@playfasa.com>", "Website issue", ATHENA),
        ("counsel@ogletree.com", "Deposition scheduling", ATHENA),
        ("random@google.com", "Notification", ATHENA),
        ("anyone@somewhere.com", "URGENT: hearing tomorrow", ATHENA),
        ("clerk@courts.wa.gov", "Order on Motion", ATHENA),
        # Bot / no-reply senders — should be suppressed even to athena
        ("noreply@starbucks.com", "Your order", ATHENA),
        ("news@nytimes.com", "Daily digest", ATHENA),
        # Anything to William's personal mailbox — should be silent
        ("Sara Murray <sara.c.murray@gmail.com>", "Personal note", WILLIAM),
        ("counsel@ogletree.com", "Deposition scheduling", WILLIAM),
    ]
    for sender, subject, mailbox in cases:
        trig, reason = should_sms(sender=sender, subject=subject, mailbox=mailbox)
        msg = format_sms(sender=sender, subject=subject) if trig else "—"
        print(f"{'✓' if trig else '✗'} [{reason:28s}] to={mailbox[:20]:20s}  "
              f"{sender[:40]:40s}  {subject[:35]}")
        if trig:
            print(f"   SMS: {msg}")
