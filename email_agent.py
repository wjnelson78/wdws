#!/usr/bin/env python3
"""
WDWS AI Email Classification Agent v2 — GPT-4o with Attachment Intelligence

Deep legal analysis powered by GPT-4o (not mini):
  • Reads FULL email body (up to 20K chars, not 4K)
  • Processes attachment text when available
  • Structured legal analysis (deadlines, risk, procedural posture)
  • Entity extraction with relationship context
  • Case association with legal reasoning
  • Urgency scoring calibrated for legal practice

Also includes attachment extraction pipeline:
  • Re-parses .eml files to extract attachment content
  • OCR for PDF attachments, text decode for text files
  • Stores extracted text in legal.email_attachments
  • Concatenates to email_metadata.attachment_text for search

Usage:
    python email_agent.py                        # Classify 100 unclassified
    python email_agent.py --backfill 500         # Classify up to 500
    python email_agent.py --doc-id UUID          # Classify specific document
    python email_agent.py --reclassify           # Re-classify all
    python email_agent.py --extract-attachments  # Extract text from attachments
    python email_agent.py --full-pipeline 500    # Extract + classify in one pass
"""

import os
import sys
import json
import asyncio
import logging
import time
import argparse
import hashlib
import uuid as _uuid
import re
import subprocess
import tempfile
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from email import policy
import email as email_lib

import httpx
import asyncpg

# ============================================================
# Configuration
# ============================================================

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws"
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")

# GPT-4o for serious legal analysis — NOT mini
CLASSIFICATION_MODEL = os.getenv("CLASSIFICATION_MODEL", "gpt-4o")

# Content limits
MAX_BODY_CHARS = 20_000       # 4x more than v1
MAX_ATTACHMENT_CHARS = 15_000  # attachment text budget
MAX_TOTAL_CHARS = 30_000      # total prompt content budget

# Source data paths (for re-parsing .eml files)
EML_DIRS = [
    Path("/opt/wdws/data/emails/athena_seattleseahawks.me"),
    Path("/opt/wdws/data/emails/william_seattleseahawks.me"),
]

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_agent")

# ============================================================
# Data classes
# ============================================================

@dataclass
class EmailClassification:
    case_associations: List[Dict] = field(default_factory=list)
    entities: List[Dict] = field(default_factory=list)
    summary: str = ""
    urgency: str = "low"
    categories: List[str] = field(default_factory=list)
    key_dates: List[Dict] = field(default_factory=list)
    suggested_tags: List[str] = field(default_factory=list)
    confidence: float = 0.0
    legal_analysis: Dict = field(default_factory=dict)
    attachment_summary: str = ""
    token_usage: Dict = field(default_factory=dict)


# ============================================================
# Attachment text extraction
# ============================================================

def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    """OCR a PDF from raw bytes using poppler + tesseract."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = Path(tmpdir) / "attachment.pdf"
        pdf_path.write_bytes(pdf_bytes)

        # Convert PDF to images
        try:
            result = subprocess.run(
                ["pdftoppm", "-png", "-r", "150",
                 str(pdf_path), str(Path(tmpdir) / "page")],
                capture_output=True, timeout=60,
            )
            if result.returncode != 0:
                log.debug(f"pdftoppm failed: {result.stderr[:200]}")
                return ""
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return ""

        # OCR each page image
        pages = sorted(Path(tmpdir).glob("page-*.png"))
        text_parts = []
        for page_img in pages[:20]:  # Cap at 20 pages
            try:
                result = subprocess.run(
                    ["tesseract", str(page_img), "stdout", "--psm", "6"],
                    capture_output=True, text=True, timeout=30,
                )
                if result.returncode == 0 and result.stdout.strip():
                    text_parts.append(result.stdout.strip())
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        return "\n\n".join(text_parts)


def extract_text_from_attachment(
    filename: str, content_type: str, payload_bytes: bytes,
) -> Tuple[str, str]:
    """
    Extract text from an attachment. Returns (text, method).

    Handles: PDF (OCR), plain text, HTML (strip tags), CSV, JSON, DOCX, images.
    """
    if not payload_bytes:
        return "", "none"

    fname_lower = (filename or "").lower()
    ct_lower = (content_type or "").lower()

    # PDF -> OCR
    if "pdf" in ct_lower or fname_lower.endswith(".pdf"):
        text = ocr_pdf_bytes(payload_bytes)
        return text, "ocr" if text else "none"

    # Plain text files
    if any(ext in fname_lower for ext in
           [".txt", ".csv", ".json", ".xml", ".log", ".md"]):
        try:
            text = payload_bytes.decode("utf-8", errors="ignore")
            return text[:50000], "text_decode"
        except Exception:
            return "", "none"

    # HTML
    if ("html" in ct_lower or fname_lower.endswith(".html")
            or fname_lower.endswith(".htm")):
        try:
            text = payload_bytes.decode("utf-8", errors="ignore")
            clean = re.sub(
                r'<style[^>]*>.*?</style>', '', text,
                flags=re.DOTALL | re.IGNORECASE,
            )
            clean = re.sub(
                r'<script[^>]*>.*?</script>', '', clean,
                flags=re.DOTALL | re.IGNORECASE,
            )
            clean = re.sub(r'<[^>]+>', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean).strip()
            return clean[:50000], "html_strip"
        except Exception:
            return "", "none"

    # Word documents (.docx)
    if fname_lower.endswith(".docx"):
        try:
            import zipfile
            import io
            with zipfile.ZipFile(io.BytesIO(payload_bytes)) as zf:
                if "word/document.xml" in zf.namelist():
                    xml_content = zf.read("word/document.xml").decode(
                        "utf-8", errors="ignore",
                    )
                    text = re.sub(r'<[^>]+>', ' ', xml_content)
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text[:50000], "docx_extract"
        except Exception:
            return "", "none"

    # Images -> OCR directly
    if any(ext in fname_lower for ext in
           [".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"]):
        suffix = fname_lower[fname_lower.rfind('.'):]
        with tempfile.NamedTemporaryFile(
            suffix=suffix, delete=False,
        ) as tf:
            tf.write(payload_bytes)
            tf_path = tf.name
        try:
            result = subprocess.run(
                ["tesseract", tf_path, "stdout", "--psm", "6"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip(), "ocr"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        finally:
            Path(tf_path).unlink(missing_ok=True)
        return "", "none"

    return "", "none"


def parse_eml_with_attachments(eml_path: Path) -> Optional[Dict]:
    """
    Parse .eml file and extract BOTH body text AND attachment content.

    Returns dict with headers, body, full_text, and attachments list.
    """
    try:
        with open(eml_path, "rb") as f:
            msg = email_lib.message_from_binary_file(f, policy=policy.default)

        headers = {
            "message_id": str(msg.get("Message-ID", "")).strip(),
            "from": str(msg.get("From", "")),
            "to": str(msg.get("To", "")),
            "cc": str(msg.get("Cc", "")),
            "subject": str(msg.get("Subject", "")),
            "date": str(msg.get("Date", "")),
        }

        body = ""
        attachments = []

        if msg.is_multipart():
            for part in msg.walk():
                ct = part.get_content_type()
                disp = part.get("Content-Disposition", "")
                filename = part.get_filename()

                # Attachment
                if ("attachment" in disp.lower()
                        or (filename and filename.strip())):
                    payload = part.get_payload(decode=True)
                    if payload and filename:
                        text, method = extract_text_from_attachment(
                            filename, ct, payload,
                        )
                        attachments.append({
                            "filename": filename,
                            "content_type": ct,
                            "size": len(payload),
                            "text": text,
                            "method": method,
                        })
                # Body text
                elif ct == "text/plain":
                    try:
                        body += part.get_content()
                    except Exception:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body += payload.decode("utf-8", errors="ignore")
        else:
            try:
                body = msg.get_content()
            except Exception:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="ignore")

        full_text = (
            f"Subject: {headers['subject']}\n"
            f"From: {headers['from']}\n"
            f"To: {headers['to']}\n"
            f"Date: {headers['date']}\n\n"
            f"{body}"
        )

        return {
            "headers": headers,
            "body": body,
            "full_text": full_text,
            "attachments": attachments,
        }
    except Exception as e:
        log.error(f"Failed to parse {eml_path.name}: {e}")
        return None


# ============================================================
# Attachment extraction pipeline
# ============================================================

async def extract_attachments_from_eml_files(
    pool: asyncpg.Pool, limit: int = 500,
):
    """
    Find emails with has_attachments=true, locate the original .eml file,
    re-parse it to extract attachment content, and store in DB.
    """
    log.info("═══════════════════════════════════════════════════════")
    log.info("  ATTACHMENT EXTRACTION PIPELINE")
    log.info("═══════════════════════════════════════════════════════")

    # Find emails with attachments that haven't been processed yet
    rows = await pool.fetch("""
        SELECT d.id, d.source_path, d.filename, em.has_attachments,
               em.attachment_text
        FROM core.documents d
        JOIN legal.email_metadata em ON d.id = em.document_id
        WHERE em.has_attachments = true
          AND (em.attachment_text IS NULL OR em.attachment_text = '')
        ORDER BY d.created_at DESC
        LIMIT $1
    """, limit)

    log.info(f"Found {len(rows)} emails with unprocessed attachments")

    if not rows:
        log.info("Nothing to extract.")
        return

    # Build a lookup of .eml files by filename
    eml_index: Dict[str, Path] = {}
    for eml_dir in EML_DIRS:
        if eml_dir.exists():
            for eml_file in eml_dir.glob("*.eml"):
                eml_index[eml_file.name] = eml_file
    log.info(f"Indexed {len(eml_index)} .eml files from source dirs")

    stats = {
        "processed": 0, "attachments_found": 0, "text_extracted": 0,
        "skipped_no_file": 0, "errors": 0,
    }

    for i, row in enumerate(rows, 1):
        doc_id = row["id"]
        source_path = row["source_path"] or ""
        filename = row["filename"] or ""

        # Try to find the original .eml file
        eml_path = None

        # Method 1: source_path is a real file path
        if source_path and not source_path.startswith("graph://"):
            candidate = Path(source_path)
            if candidate.exists():
                eml_path = candidate
            else:
                base = candidate.name
                if base in eml_index:
                    eml_path = eml_index[base]

        # Method 2: Match by filename
        if not eml_path and filename:
            if filename in eml_index:
                eml_path = eml_index[filename]
            else:
                prefix = filename.replace(".eml", "")[:30]
                for name, path in eml_index.items():
                    if prefix in name:
                        eml_path = path
                        break

        if not eml_path:
            stats["skipped_no_file"] += 1
            continue

        # Parse the .eml file with attachment extraction
        try:
            parsed = parse_eml_with_attachments(eml_path)
            if not parsed or not parsed["attachments"]:
                stats["skipped_no_file"] += 1
                continue

            all_attachment_text = []

            for att in parsed["attachments"]:
                att_text = att["text"]
                method = att["method"]

                # Store individual attachment record
                await pool.execute("""
                    INSERT INTO legal.email_attachments
                        (email_doc_id, filename, content_type, file_size,
                         extracted_text, extraction_method, is_processed)
                    VALUES ($1, $2, $3, $4, $5, $6, true)
                """,
                    doc_id, att["filename"], att["content_type"],
                    att["size"], att_text if att_text else None, method,
                )

                stats["attachments_found"] += 1
                if att_text:
                    all_attachment_text.append(
                        f"[Attachment: {att['filename']}]\n{att_text}"
                    )
                    stats["text_extracted"] += 1

            # Concatenate and store on email_metadata for search/classify
            combined = (
                "\n\n---\n\n".join(all_attachment_text)
                if all_attachment_text else ""
            )
            await pool.execute("""
                UPDATE legal.email_metadata
                SET attachment_text = $1
                WHERE document_id = $2
            """, combined if combined else "", doc_id)

            stats["processed"] += 1

            if i <= 10 or i % 50 == 0:
                att_count = len(parsed["attachments"])
                text_count = sum(1 for a in parsed["attachments"] if a["text"])
                log.info(
                    f"  [{i}/{len(rows)}] "
                    f"{filename[:45]} — "
                    f"{att_count} att, {text_count} with text"
                )

        except Exception as e:
            stats["errors"] += 1
            log.error(f"  [{i}/{len(rows)}] {filename[:45]} — {e}")

    log.info("")
    log.info(f"  Processed:      {stats['processed']}")
    log.info(f"  Attachments:    {stats['attachments_found']}")
    log.info(f"  Text extracted: {stats['text_extracted']}")
    log.info(f"  No file found:  {stats['skipped_no_file']}")
    log.info(f"  Errors:         {stats['errors']}")


# ============================================================
# Database helpers
# ============================================================

async def get_known_cases(pool: asyncpg.Pool) -> List[Dict]:
    """Fetch all known legal cases for classification context."""
    rows = await pool.fetch("""
        SELECT case_number, case_title, court, jurisdiction, status,
               case_type, filing_date
        FROM legal.cases ORDER BY case_number
    """)
    return [dict(r) for r in rows]


async def get_known_parties(pool: asyncpg.Pool) -> List[str]:
    """Fetch known party names."""
    rows = await pool.fetch(
        "SELECT DISTINCT name FROM legal.parties ORDER BY name LIMIT 200"
    )
    return [r["name"] for r in rows]


# ============================================================
# AI Classification — GPT-4o Deep Analysis
# ============================================================

def build_system_prompt(
    known_cases: List[Dict], known_parties: List[str],
) -> str:
    """Build a rich system prompt with full case context."""
    cases_ctx = "\n".join([
        f"  {c['case_number']}: {c.get('case_title', '')} "
        f"({c.get('court', '')}, {c.get('case_type', '')}, "
        f"status: {c.get('status', '')})"
        for c in known_cases
    ]) or "  (no cases registered)"

    parties_ctx = (
        ", ".join(known_parties[:50]) if known_parties else "(none)"
    )

    return f"""You are an expert legal AI assistant for William Nelson's Athena Cognitive Platform.
You perform deep analysis of legal emails and their attachments.

William Nelson is the plaintiff/petitioner in most of these cases. He is pro se
(self-represented) in some cases and represented by counsel in others.

KNOWN LEGAL CASES:
{cases_ctx}

KNOWN PARTIES:
{parties_ctx}

YOUR ANALYSIS MUST INCLUDE:

1. CASE ASSOCIATIONS — Match to known cases with high precision. Explain WHY.
   Only associate when the content clearly relates to that specific case.

2. ENTITIES — Extract ALL people, organizations, courts, judges, attorneys,
   law firms, and government agencies. Note their ROLE (sender, recipient,
   mentioned, attorney, judge, opposing_counsel, witness, etc.)

3. LEGAL ANALYSIS — This is the most important part:
   - procedural_posture: What stage is this at?
   - deadlines: Extract ALL dates with legal significance and applicable rules
   - legal_issues: What substantive issues are discussed?
   - risk_assessment: What are the risks if this is ignored?
   - recommended_actions: What should William Nelson do in response?
   - opposing_counsel_tone: neutral, aggressive, cooperative, or threatening?

4. URGENCY — Be precise:
   - critical: Court deadlines within 72 hours, emergency motions, TROs
   - high: Response deadlines within 2 weeks, motions to compel, sanctions
   - medium: Routine legal correspondence, scheduling, standard deadlines
   - low: Administrative, informational, newsletters, receipts

5. SUMMARY — 3-5 sentences: what it is, who from, what action needed, when.

6. ATTACHMENT ANALYSIS — If attachment text is provided, analyze it with the
   same depth as the email body. Attachments often contain the most critical
   content (court orders, motions, declarations, exhibits)."""


def build_user_prompt(
    subject: str, body: str, sender: str, recipients: str,
    attachment_text: str = "",
) -> str:
    """Build the user prompt with email content + attachment text."""
    body_budget = MAX_BODY_CHARS
    att_budget = MAX_ATTACHMENT_CHARS

    if len(attachment_text) < att_budget // 2:
        body_budget = MAX_TOTAL_CHARS - len(attachment_text)

    parts = [
        f"FROM: {sender}",
        f"TO: {recipients}",
        f"SUBJECT: {subject}",
        "",
        "--- EMAIL BODY ---",
        body[:body_budget],
    ]

    if attachment_text and attachment_text.strip():
        parts.extend([
            "",
            "--- ATTACHMENT CONTENT ---",
            "(Text extracted from email attachments — PDFs, documents, etc. "
            "Analyze this with equal or greater importance than the email body.)",
            "",
            attachment_text[:att_budget],
        ])

    schema = {
        "case_associations": [
            {"case_number": "XX-X-XXXXX-XX", "confidence": 0.9,
             "reason": "Specific reason linking email to case"}
        ],
        "entities": [
            {"name": "Full Name", "type": "person", "role": "sender"}
        ],
        "summary": "3-5 sentence analysis of email and legal significance",
        "urgency": "low|medium|high|critical",
        "categories": ["correspondence"],
        "key_dates": [
            {"date": "2025-01-15", "description": "hearing",
             "rule": "CR 56(c)"}
        ],
        "suggested_tags": ["tag1"],
        "confidence": 0.85,
        "legal_analysis": {
            "procedural_posture": "Current stage of proceedings",
            "deadlines": [
                {"date": "2025-02-01", "type": "response_due",
                 "rule": "CR 26(c)", "consequence": "sanctions"}
            ],
            "legal_issues": ["discovery dispute"],
            "risk_assessment": "Description of risk if not addressed",
            "recommended_actions": ["File response by X date"],
            "opposing_counsel_tone": "neutral",
        },
        "attachment_summary": "Summary of attachments and their significance",
    }

    parts.extend([
        "",
        "--- RESPOND WITH JSON ---",
        "Respond with ONLY valid JSON matching this schema:",
        json.dumps(schema, indent=2),
        "",
        "Valid entity types: person, organization, court, judge, attorney, "
        "government_agency, law_firm, medical_provider",
        "Valid categories: legal_filing, discovery, scheduling, "
        "administrative, correspondence, financial, medical_related, "
        "settlement, court_order, notification, guardian_ad_litem, "
        "procedural, appeal, enforcement, contempt, protective_order, "
        "sanctions, motion, declaration, subpoena",
    ])

    return "\n".join(parts)


async def classify_email(
    subject: str,
    body: str,
    sender: str,
    recipients: str,
    known_cases: List[Dict],
    known_parties: List[str] = None,
    attachment_text: str = "",
    api_key: str = None,
    model: str = None,
) -> Optional[EmailClassification]:
    """Use GPT-4o to deeply analyze and classify an email."""
    api_key = api_key or OPENAI_API_KEY
    model = model or CLASSIFICATION_MODEL
    known_parties = known_parties or []

    system_prompt = build_system_prompt(known_cases, known_parties)
    user_prompt = build_user_prompt(
        subject, body, sender, recipients, attachment_text,
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                    "max_tokens": 2000,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            result = json.loads(content)

            return EmailClassification(
                case_associations=result.get("case_associations", []),
                entities=result.get("entities", []),
                summary=result.get("summary", ""),
                urgency=result.get("urgency", "low"),
                categories=result.get("categories", []),
                key_dates=result.get("key_dates", []),
                suggested_tags=result.get("suggested_tags", []),
                confidence=result.get("confidence", 0.5),
                legal_analysis=result.get("legal_analysis", {}),
                attachment_summary=result.get("attachment_summary", ""),
                token_usage={
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0),
                },
            )
    except httpx.HTTPStatusError as e:
        log.error(
            f"OpenAI API error {e.response.status_code}: "
            f"{e.response.text[:300]}"
        )
        return None
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse classification JSON: {e}")
        return None
    except Exception as e:
        log.error(f"Classification error: {e}")
        return None


# ============================================================
# Database storage
# ============================================================

async def store_classification(
    pool: asyncpg.Pool,
    document_id: str,
    classification: EmailClassification,
    model: str = None,
):
    """Store GPT-4o classification with legal analysis."""
    model = model or CLASSIFICATION_MODEL
    doc_uuid = (
        _uuid.UUID(document_id)
        if isinstance(document_id, str) else document_id
    )

    class_json = {
        "case_associations": classification.case_associations,
        "entities": classification.entities,
        "summary": classification.summary,
        "urgency": classification.urgency,
        "categories": classification.categories,
        "key_dates": classification.key_dates,
        "suggested_tags": classification.suggested_tags,
    }

    await pool.execute("""
        INSERT INTO ops.ai_classifications
            (document_id, model_used, classification, confidence,
             legal_analysis, attachment_summary, token_usage)
        VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6, $7::jsonb)
        ON CONFLICT (document_id) DO UPDATE SET
            model_used = EXCLUDED.model_used,
            classification = EXCLUDED.classification,
            confidence = EXCLUDED.confidence,
            legal_analysis = EXCLUDED.legal_analysis,
            attachment_summary = EXCLUDED.attachment_summary,
            token_usage = EXCLUDED.token_usage,
            processed_at = now()
    """,
        doc_uuid, model, json.dumps(class_json), classification.confidence,
        json.dumps(classification.legal_analysis)
        if classification.legal_analysis else None,
        classification.attachment_summary or None,
        json.dumps(classification.token_usage)
        if classification.token_usage else None,
    )


async def write_entities(
    pool: asyncpg.Pool,
    document_id: str,
    entities: List[Dict],
):
    """Write extracted entities to core.entities + core.entity_mentions."""
    doc_uuid = (
        _uuid.UUID(document_id)
        if isinstance(document_id, str) else document_id
    )

    for ent in entities:
        name = ent.get("name", "").strip()
        ent_type = ent.get("type", "unknown").lower()
        role = ent.get("role", "mentioned")

        if not name or len(name) < 2:
            continue

        normalized = name.lower().strip()

        try:
            entity_id = await pool.fetchval("""
                INSERT INTO core.entities
                    (name, entity_type, normalized_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (normalized_name, entity_type) DO UPDATE
                    SET name = EXCLUDED.name
                RETURNING id
            """, name, ent_type, normalized)

            existing = await pool.fetchval("""
                SELECT COUNT(*) FROM core.entity_mentions
                WHERE entity_id = $1 AND document_id = $2 AND role = $3
            """, entity_id, doc_uuid, role)

            if existing == 0:
                await pool.execute("""
                    INSERT INTO core.entity_mentions
                        (entity_id, document_id, role)
                    VALUES ($1, $2, $3)
                """, entity_id, doc_uuid, role)
        except Exception as e:
            log.debug(f"Entity write error for '{name}': {e}")


async def link_cases_from_classification(
    pool: asyncpg.Pool,
    document_id: str,
    case_associations: List[Dict],
    min_confidence: float = 0.6,
):
    """Link document to cases based on classification."""
    doc_uuid = (
        _uuid.UUID(document_id)
        if isinstance(document_id, str) else document_id
    )
    linked = 0

    for assoc in case_associations:
        case_num = assoc.get("case_number", "")
        confidence = assoc.get("confidence", 0)

        if confidence < min_confidence or not case_num:
            continue

        row = await pool.fetchrow(
            "SELECT id FROM legal.cases WHERE case_number = $1",
            case_num,
        )
        if row:
            await pool.execute("""
                INSERT INTO legal.case_documents (case_id, document_id)
                VALUES ($1, $2) ON CONFLICT DO NOTHING
            """, row["id"], doc_uuid)
            linked += 1

    return linked


# ============================================================
# Full pipeline: classify -> store -> entities -> case links
# ============================================================

async def classify_and_store(
    pool: asyncpg.Pool,
    document_id: str,
    subject: str,
    body: str,
    sender: str,
    recipients: str,
    known_cases: List[Dict],
    known_parties: List[str] = None,
    attachment_text: str = "",
) -> Optional[EmailClassification]:
    """Full classification pipeline for a single email."""
    result = await classify_email(
        subject, body, sender, recipients, known_cases,
        known_parties=known_parties,
        attachment_text=attachment_text,
    )

    if not result:
        return None

    try:
        await store_classification(pool, document_id, result)
        await write_entities(pool, document_id, result.entities)
        await link_cases_from_classification(
            pool, document_id, result.case_associations,
        )
    except Exception as e:
        log.error(f"Storage error for {document_id}: {e}")

    return result


# ============================================================
# Batch processing / backfill
# ============================================================

async def backfill_classifications(
    limit: int = 100,
    reclassify: bool = False,
    doc_id: str = None,
):
    """Classify emails with GPT-4o, including attachment content."""
    log.info("")
    log.info("=" * 64)
    log.info("  WDWS AI Email Classification Agent v2")
    log.info("  Model: GPT-4o (deep legal analysis)")
    log.info("=" * 64)
    log.info(f"  Limit:      {limit}")
    log.info(f"  Reclassify: {reclassify}")
    log.info(f"  Doc ID:     {doc_id or 'all'}")
    log.info("=" * 64)
    log.info("")

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # Load context
    known_cases = await get_known_cases(pool)
    known_parties = await get_known_parties(pool)
    log.info(f"Context: {len(known_cases)} cases, {len(known_parties)} parties")
    for c in known_cases:
        log.info(f"  - {c['case_number']}: {c.get('case_title', '')[:50]}")

    # Build query — now includes attachment_text
    if doc_id:
        rows = await pool.fetch("""
            SELECT d.id, d.title, d.full_content,
                   em.sender, em.recipients, em.has_attachments,
                   COALESCE(em.attachment_text, '') as attachment_text
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE d.id = $1
        """, _uuid.UUID(doc_id))
    elif reclassify:
        rows = await pool.fetch("""
            SELECT d.id, d.title, d.full_content,
                   em.sender, em.recipients, em.has_attachments,
                   COALESCE(em.attachment_text, '') as attachment_text
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            ORDER BY d.created_at DESC
            LIMIT $1
        """, limit)
    else:
        rows = await pool.fetch("""
            SELECT d.id, d.title, d.full_content,
                   em.sender, em.recipients, em.has_attachments,
                   COALESCE(em.attachment_text, '') as attachment_text
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN ops.ai_classifications ac ON d.id = ac.document_id
            WHERE ac.id IS NULL
            ORDER BY d.created_at DESC
            LIMIT $1
        """, limit)

    log.info(f"\nFound {len(rows)} emails to classify")
    with_att = sum(1 for r in rows if r["attachment_text"])
    log.info(f"  {with_att} have attachment text available")
    log.info("")

    if not rows:
        log.info("Nothing to classify.")
        await pool.close()
        return

    stats = {
        "ok": 0, "errors": 0, "entities": 0, "cases_linked": 0,
        "with_attachments": 0, "total_tokens": 0,
    }
    urgency_counts = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    cost_total = 0.0
    start = time.time()

    for i, row in enumerate(rows, 1):
        title = (row["title"] or "No Subject")[:50]
        has_att_text = bool(row["attachment_text"])

        try:
            result = await classify_and_store(
                pool, str(row["id"]),
                row["title"] or "",
                row["full_content"] or "",
                row["sender"] or "",
                row["recipients"] or "",
                known_cases,
                known_parties=known_parties,
                attachment_text=row["attachment_text"] or "",
            )

            if result:
                stats["ok"] += 1
                stats["entities"] += len(result.entities)
                if has_att_text:
                    stats["with_attachments"] += 1

                # Track tokens and cost (GPT-4o pricing)
                prompt_tok = result.token_usage.get("prompt_tokens", 0)
                compl_tok = result.token_usage.get("completion_tokens", 0)
                total_tok = prompt_tok + compl_tok
                stats["total_tokens"] += total_tok

                # GPT-4o: $2.50/1M input, $10.00/1M output
                email_cost = (
                    (prompt_tok * 2.50 + compl_tok * 10.00) / 1_000_000
                )
                cost_total += email_cost

                urg = result.urgency
                urgency_counts[urg] = urgency_counts.get(urg, 0) + 1

                urgency_icon = {
                    "critical": "!!!", "high": "!! ",
                    "medium": "!  ", "low": "   ",
                }.get(urg, "   ")

                cases_str = ", ".join(
                    a["case_number"]
                    for a in result.case_associations[:3]
                ) or "none"

                att_flag = " +att" if has_att_text else ""

                elapsed_so_far = time.time() - start
                rate = i / elapsed_so_far if elapsed_so_far > 0 else 0
                eta = (len(rows) - i) / rate if rate > 0 else 0
                eta_str = (
                    f"{eta:.0f}s" if eta < 60 else f"{eta / 60:.1f}m"
                )

                log.info(
                    f"  [{i}/{len(rows)} {i*100//len(rows)}%] "
                    f"{urgency_icon} {title}{att_flag} "
                    f"-> {urg}, cases: {cases_str}, "
                    f"ent: {len(result.entities)}, "
                    f"tok: {total_tok}, ${email_cost:.4f}  "
                    f"[{rate:.1f}/s, ETA {eta_str}]"
                )

                # Log legal analysis highlights for critical/high
                if urg in ("critical", "high") and result.legal_analysis:
                    la = result.legal_analysis
                    if la.get("risk_assessment"):
                        log.info(
                            f"       RISK: {la['risk_assessment'][:80]}"
                        )
                    if la.get("deadlines"):
                        for dl in la["deadlines"][:2]:
                            log.info(
                                f"       DEADLINE: {dl.get('date', '?')}"
                                f" - {dl.get('type', '')}"
                            )
                    if la.get("recommended_actions"):
                        for action in la["recommended_actions"][:2]:
                            log.info(f"       ACTION: {action[:80]}")
            else:
                stats["errors"] += 1
                log.warning(
                    f"  [{i}/{len(rows)}] FAIL {title} "
                    f"- classification failed"
                )

        except Exception as e:
            stats["errors"] += 1
            log.error(f"  [{i}/{len(rows)}] ERROR {title} - {e}")

        # Rate limit: GPT-4o is slower, 1.5s between calls
        if i < len(rows):
            await asyncio.sleep(1.5)

    elapsed = time.time() - start

    # Get totals
    total_classified = await pool.fetchval(
        "SELECT COUNT(*) FROM ops.ai_classifications"
    )
    total_entities = await pool.fetchval(
        "SELECT COUNT(*) FROM core.entities"
    )

    log.info("")
    log.info("=" * 64)
    log.info("  CLASSIFICATION COMPLETE")
    log.info("=" * 64)
    log.info(f"  Model:            GPT-4o (deep analysis)")
    log.info(f"  Classified:       {stats['ok']}")
    log.info(f"  With attachments: {stats['with_attachments']}")
    log.info(f"  Errors:           {stats['errors']}")
    log.info(f"  Entities found:   {stats['entities']}")
    log.info(f"  Time:             {elapsed:.1f}s")
    rate_str = f"{stats['ok']/max(elapsed,1):.1f} emails/s"
    log.info(f"  Rate:             {rate_str}")
    log.info("-" * 64)
    log.info(f"  Urgency breakdown:")
    log.info(f"    Critical: {urgency_counts.get('critical', 0)}")
    log.info(f"    High:     {urgency_counts.get('high', 0)}")
    log.info(f"    Medium:   {urgency_counts.get('medium', 0)}")
    log.info(f"    Low:      {urgency_counts.get('low', 0)}")
    log.info("-" * 64)
    tokens_str = f"{stats['total_tokens']:,}"
    cost_str = f"${cost_total:.4f}"
    log.info(f"  Total tokens:     {tokens_str}")
    log.info(f"  Total cost:       {cost_str}")
    log.info("-" * 64)
    log.info(f"  DB classified:    {total_classified}")
    log.info(f"  DB entities:      {total_entities}")
    log.info("=" * 64)

    await pool.close()


# ============================================================
# Full pipeline: extract attachments then classify
# ============================================================

async def full_pipeline(limit: int = 100, reclassify: bool = False):
    """Extract attachments first, then classify with attachment context."""
    log.info("=== PHASE 1: ATTACHMENT EXTRACTION ===")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    await extract_attachments_from_eml_files(pool, limit=9999)
    await pool.close()

    log.info("")
    log.info("=== PHASE 2: GPT-4o CLASSIFICATION ===")
    await backfill_classifications(limit=limit, reclassify=reclassify)


# ============================================================
# CLI entry point
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="WDWS AI Email Classification Agent v2 (GPT-4o)"
    )
    parser.add_argument(
        "--backfill", "-n", type=int, default=100,
        help="Max emails to classify (default: 100)",
    )
    parser.add_argument(
        "--reclassify", action="store_true",
        help="Re-classify already classified emails",
    )
    parser.add_argument(
        "--doc-id",
        help="Classify a specific document by UUID",
    )
    parser.add_argument(
        "--extract-attachments", action="store_true",
        help="Extract text from email attachments (no classification)",
    )
    parser.add_argument(
        "--full-pipeline", type=int, metavar="N",
        help="Extract attachments then classify N emails",
    )
    args = parser.parse_args()

    if args.extract_attachments:
        async def _extract():
            pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=5,
            )
            await extract_attachments_from_eml_files(pool, limit=9999)
            await pool.close()
        asyncio.run(_extract())
    elif args.full_pipeline is not None:
        asyncio.run(full_pipeline(
            limit=args.full_pipeline,
            reclassify=args.reclassify,
        ))
    else:
        asyncio.run(backfill_classifications(
            limit=args.backfill,
            reclassify=args.reclassify,
            doc_id=args.doc_id,
        ))
