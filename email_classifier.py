#!/usr/bin/env python3
"""
Email / document classifier — rule-based first pass.

Populates `core.documents.classification` with one of:

  content        — default legit content worth preserving
  privileged     — attorney-client communication, work product, confidential
                   legal correspondence. MUST never auto-purge (retention
                   sweep also respects legal_hold separately for matters
                   under active litigation).
  transactional  — auto-generated receipts, confirmations, OTPs, password
                   resets, calendar invites. Short retention acceptable.
  spam           — promotional / marketing / noise. Shortest retention.
  unknown        — couldn't decide; retention falls back to longest
                   preservation window (treat like content).

Design:
  - Rule-based, no ML yet. Each rule is an independent predicate returning
    (bool match, classification, reason). First match wins; order matters.
  - For emails, decision factors: sender domain, sender local-part, subject
    keywords, body text head.
  - For non-email documents (court_filing, medical_record, email_attachment,
    etc.), default to `content`. Attachments inherit from their parent
    email's classification when available.

Usage:
  python email_classifier.py                     # backfill unclassified docs
  python email_classifier.py --limit 500         # small batch
  python email_classifier.py --document UUID     # single doc
  python email_classifier.py --reclassify        # re-run on all docs (not
                                                 # just NULL classification)
  python email_classifier.py --dry-run           # log only, don't write
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import uuid
from pathlib import Path
from typing import Any, Optional

import asyncpg

# Env loader — keep independent of email_sync.py
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())


log = logging.getLogger("classifier")

DATABASE_URL = os.environ["DATABASE_URL"]


# ============================================================
# Rule: priviledged legal correspondence
# ============================================================
#
# Mirror and extend the domain list used by email_notify.py — sender
# domains that are known counsel / courts get a strong prior toward
# "privileged" when combined with subject / body cues.

_LEGAL_DOMAINS = {
    # Counsel
    "workerlaw.com", "csdlaw.com", "cabornelaw.com", "clearpathpllc.com",
    "fwwlaw.com", "kantorlaw.net", "bn-lawyers.com",
    "ogletree.com", "ogletreedeakins.com", "lewisbrisbois.com", "grsm.com",
    "insleebest.com", "jmblawyers.com", "uscanadalaw.com", "favros.com",
    "cozen.com", "slwsd.com", "lldkb.com", "mhb.com",
    "fayejwonglawfirm.com",
}

_COURT_DOMAINS = {
    "courts.wa.gov", "snoco.org", "co.snohomish.wa.us", "co.chelan.wa.us",
    "ca9.uscourts.gov",
}
_USCOURTS_RE = re.compile(r"\.uscourts\.gov$", re.IGNORECASE)

_PRIVILEGED_MARKERS = re.compile(
    r"attorney[-\s]client\s+privilege|attorney\s+work\s+product|"
    r"privileged\s+and\s+confidential|confidential\s+communication",
    re.IGNORECASE,
)


# ============================================================
# Rule: transactional / automated
# ============================================================

_TRANSACTIONAL_LOCAL_PARTS = (
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "auto", "automated", "mailer-daemon", "postmaster",
    "notifications", "notification", "support", "help",
    "receipts", "receipt", "billing", "orders", "verify",
)

_TRANSACTIONAL_SUBJECT_RE = re.compile(
    r"\b(receipt|order\s+confirmation|shipping\s+notification|"
    r"password\s+reset|verification\s+code|one[-\s]time\s+(passcode|password)|"
    r"otp|two[-\s]factor|security\s+alert|login\s+alert|"
    r"account\s+(activity|notification)|calendar\s+invite|"
    r"statement\s+available|payment\s+(received|due|confirmation))",
    re.IGNORECASE,
)


# ============================================================
# Rule: spam / marketing
# ============================================================

_MARKETING_SUBJECT_RE = re.compile(
    r"(\b\d{1,2}%\s*off\b|\bsave\s+(up\s+to\s+)?\$?\d|\bdeals?\b|"
    r"\blimited[-\s]time\b|\bflash\s+sale\b|\bexclusive\s+offer\b|"
    r"\blast\s+chance\b|\bunsubscribe\b|\bnewsletter\b|\bdigest\b|"
    r"\bwebinar\b|\bfree\s+(trial|gift|shipping)\b)",
    re.IGNORECASE,
)

_MARKETING_LOCAL_PARTS = (
    "news", "newsletter", "marketing", "promo", "promos", "promotions",
    "deals", "offers", "updates",
)


# ============================================================
# Rule: medical / insurance
# ============================================================
# These are content for retention purposes — medical records and insurance
# correspondence both have real legal value in personal-injury / disability
# matters. Classifying them explicitly ensures they never accidentally fall
# into transactional / spam.

_MEDICAL_DOMAINS = {
    "evergreenhealthcare.org", "evergreenhealth.com",
    "providence.org", "uwmedicine.org", "swedish.org",
    "workwellfoundation.org", "wahbexchange.org",
}
_INSURANCE_DOMAINS = {"sedgwick.com", "unum.com"}


# ============================================================
# Classification
# ============================================================

def _extract_sender_email(raw_sender: str) -> str:
    m = re.search(r"<([^>]+)>", raw_sender or "")
    if m:
        return m.group(1).strip().lower()
    return (raw_sender or "").strip().lower()


def _domain_of(raw_sender: str) -> str:
    e = _extract_sender_email(raw_sender)
    return e.split("@", 1)[1] if "@" in e else ""


def _local_of(raw_sender: str) -> str:
    e = _extract_sender_email(raw_sender)
    return e.split("@", 1)[0] if "@" in e else e


def classify_email(
    sender: str,
    subject: str = "",
    body_head: str = "",
) -> tuple[str, str]:
    """Apply rules in order; return (classification, reason)."""
    sender = sender or ""
    subject = subject or ""
    body_head = body_head or ""

    domain = _domain_of(sender)
    local = _local_of(sender)

    # 1. Privileged — legal domain + explicit privilege marker in
    # subject or body → privileged. Legal domain alone is still legal but
    # not necessarily privileged (e.g. admin court clerk notification).
    is_legal_domain = domain in _LEGAL_DOMAINS
    if _PRIVILEGED_MARKERS.search(subject) or _PRIVILEGED_MARKERS.search(body_head):
        if is_legal_domain:
            return "privileged", f"privilege_marker+legal_domain:{domain}"
        # Marker alone is also a strong signal (opposing counsel's own
        # correspondence often carries the marker even from alternate domains)
        return "privileged", "privilege_marker"

    # Legal-domain mail without explicit markers → content (preserve, but
    # don't assert privilege)
    if is_legal_domain or domain in _COURT_DOMAINS or _USCOURTS_RE.search(domain):
        return "content", f"legal_or_court_domain:{domain}"

    # 2. Medical / insurance — content
    if domain in _MEDICAL_DOMAINS:
        return "content", f"medical_domain:{domain}"
    if domain in _INSURANCE_DOMAINS:
        return "content", f"insurance_domain:{domain}"

    # 3. Transactional — local-part bot prefixes first (cheapest check)
    if any(local == b or local.startswith(f"{b}-") or local.startswith(f"{b}.")
           for b in _TRANSACTIONAL_LOCAL_PARTS):
        return "transactional", f"auto_local_part:{local[:30]}"
    if _TRANSACTIONAL_SUBJECT_RE.search(subject):
        return "transactional", "transactional_subject"

    # 4. Marketing / spam — subject keyword or local-part
    if _MARKETING_SUBJECT_RE.search(subject):
        return "spam", "marketing_subject"
    if any(local == m or local.startswith(f"{m}-") or local.startswith(f"{m}.")
           for m in _MARKETING_LOCAL_PARTS):
        return "spam", f"marketing_local_part:{local[:30]}"

    # 5. Fall-through: content. Deliberately conservative — we prefer to
    # retain something as "content" than to misclassify as spam and purge.
    return "content", "default_fallthrough"


def classify_document(
    domain: str,
    document_type: str,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    body_head: Optional[str] = None,
    parent_classification: Optional[str] = None,
) -> tuple[str, str]:
    """Route to the right classifier by document_type.

    For email + email_attachment, use the email classifier.
    Attachments inherit their parent's classification when known (privileged
    email → privileged attachment).
    Everything else defaults to 'content'.
    """
    if document_type == "email":
        return classify_email(sender or "", subject or "", body_head or "")

    if document_type == "email_attachment":
        if parent_classification:
            return parent_classification, f"inherit_parent:{parent_classification}"
        # No parent linkage — fall through to email classifier using whatever
        # we have (may produce 'content' from the default fallthrough).
        return classify_email(sender or "", subject or "", body_head or "")

    # Court filings, medical records, arbitrary files — all content by default.
    return "content", f"default_for_type:{document_type}"


# ============================================================
# DB integration
# ============================================================

_LOOKUP_SQL = """
SELECT d.id,
       d.domain,
       d.document_type,
       d.full_content,
       em.sender,
       em.subject,
       /* For attachments, find parent email's classification */
       (SELECT parent.classification
          FROM core.document_relationships r
          JOIN core.documents parent
            ON parent.id = r.source_document_id
         WHERE r.target_document_id = d.id
           AND r.relationship_type = 'has_attachment'
         LIMIT 1) AS parent_classification
  FROM core.documents d
  LEFT JOIN legal.email_metadata em ON em.document_id = d.id
 WHERE d.id = $1
"""


async def classify_single(
    pool: asyncpg.Pool, document_id: str, dry_run: bool = False,
) -> Optional[tuple[str, str]]:
    row = await pool.fetchrow(_LOOKUP_SQL, uuid.UUID(document_id))
    if not row:
        log.warning("document %s not found", document_id)
        return None

    body_head = (row["full_content"] or "")[:2000]
    cls, reason = classify_document(
        domain=row["domain"],
        document_type=row["document_type"],
        sender=row["sender"],
        subject=row["subject"],
        body_head=body_head,
        parent_classification=row["parent_classification"],
    )

    if dry_run:
        log.info("[dry-run] %s → %s (%s)", document_id, cls, reason)
        return cls, reason

    await pool.execute(
        """
        UPDATE core.documents
           SET classification = $1,
               classified_at = now()
         WHERE id = $2
        """,
        cls, uuid.UUID(document_id),
    )
    return cls, reason


async def backfill(
    pool: asyncpg.Pool,
    *,
    limit: int,
    reclassify: bool,
    dry_run: bool,
) -> dict[str, int]:
    """Backfill classification for existing docs.

    `reclassify=True` includes rows that already have classification set;
    default only touches NULL classification rows.
    """
    where = "classification IS NULL"
    if reclassify:
        where = "TRUE"

    ids = await pool.fetch(
        f"SELECT id FROM core.documents WHERE {where} "
        f"ORDER BY created_at DESC LIMIT $1",
        limit,
    )
    counts: dict[str, int] = {}
    for row in ids:
        doc_id = str(row["id"])
        try:
            result = await classify_single(pool, doc_id, dry_run=dry_run)
        except Exception as e:
            log.exception("classify failed for %s", doc_id)
            counts["error"] = counts.get("error", 0) + 1
            continue
        if result is None:
            continue
        cls, reason = result
        counts[cls] = counts.get(cls, 0) + 1
    return counts


# ============================================================
# CLI
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--document", help="Classify one document by UUID")
    ap.add_argument("--limit", type=int, default=1000,
                    help="Backfill batch size (default 1000)")
    ap.add_argument("--reclassify", action="store_true",
                    help="Include already-classified docs (default: only NULL)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Log decisions but do not write to DB")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def go() -> int:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        try:
            if args.document:
                result = await classify_single(pool, args.document, args.dry_run)
                if result:
                    log.info("%s → %s (%s)", args.document, *result)
                return 0

            counts = await backfill(
                pool,
                limit=args.limit,
                reclassify=args.reclassify,
                dry_run=args.dry_run,
            )
            total = sum(counts.values())
            log.info("Classified %d docs: %s", total, dict(sorted(counts.items())))
            return 0
        finally:
            await pool.close()

    return asyncio.run(go())


if __name__ == "__main__":
    sys.exit(main())
