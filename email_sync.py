#!/usr/bin/env python3
"""
Graph API Email Sync for ACP Enterprise PostgreSQL.

Connects to Microsoft Graph API using app-only (client credentials) auth,
fetches emails to/from specified domains for both william@ and athena@
mailboxes, and ingests them directly into the enterprise PostgreSQL database
with embeddings.

Usage:
    python email_sync.py                          # Sync snoco emails from both mailboxes
    python email_sync.py --domains snoco.org co.snohomish.wa.us
    python email_sync.py --mailbox william@seattleseahawks.me
    python email_sync.py --since 2024-01-01
    python email_sync.py --dry-run                # Show what would be fetched
"""
import os
import sys
sys.path.insert(0, "/opt/wdws")
import json
import importlib.util
import re
import uuid
import hashlib
import asyncio
import logging
import time
import argparse
import subprocess
import tempfile
import traceback
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from email import policy
import email as email_lib

import httpx
import asyncpg


def _ensure_project_venv_for_embeddings() -> None:
    """Re-exec under the project venv when embedding deps are missing.

    Operators sometimes invoke this script with the system `python3`, but the
    local BGE-M3 stack lives in `/opt/wdws/venv`. When that happens the sync can
    still ingest emails/OCR attachments yet fall back to zero vectors once
    `sentence_transformers` is needed. Detect that early and transparently
    relaunch the same command under the project venv.
    """
    if os.getenv("WDWS_SKIP_VENV_REEXEC") == "1":
        return

    venv_python = Path(os.getenv("WDWS_VENV_PYTHON", "/opt/wdws/venv/bin/python3"))
    if not venv_python.exists():
        return

    current_python = Path(sys.executable).resolve()
    target_python = venv_python.resolve()
    if current_python == target_python:
        return

    if importlib.util.find_spec("sentence_transformers") is not None:
        return

    print(
        f"[email_sync] sentence_transformers missing in {current_python}; "
        f"relaunching with {target_python}",
        file=sys.stderr,
        flush=True,
    )
    os.execv(str(target_python), [str(target_python), str(Path(__file__).resolve()), *sys.argv[1:]])

from email_sync_config import (
    MAILBOXES,
    TARGET_DOMAINS,
    TARGET_SPECIFIC_EMAILS,
    resolve_sync_configuration,
)
from embedding_service import (
    embed_texts_sync, embed_query_sync,
    _vec_literal as _embedding_vec_literal,
    EMBEDDING_DIMENSIONS, EMBEDDING_MODEL,
)
from contextual_retrieval import generate_context_sync, enrich_chunks
from email_chunker import (
    EmailAwareChunker,
    ChunkResult,
    PartType,
    TextChunker,  # back-compat alias; EmailAwareChunker is the real class
)
from ingestion_pipeline import (
    DocumentSpec,
    Attachment as PipelineAttachment,
    ingest as pipeline_ingest,
)
import blob_storage
from ingest_attachments import extract_xlsx_text as _extract_xlsx_text, extract_xls_text as _extract_xls_text
from agents.email_util import (
    build_notification_html as build_alert_notification_html,
    send_email as send_alert_email,
)

# ============================================================
# Configuration
# ============================================================
# Load .env
from pathlib import Path as _Path
_env_file = _Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            import os as _os
            _os.environ.setdefault(_k.strip(), _v.strip())


_ensure_project_venv_for_embeddings()


DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL — check /opt/wdws/.env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
# OpenAI API key no longer required — embeddings use local BGE-M3 model

# Graph API credentials
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
if not GRAPH_CLIENT_ID:
    raise RuntimeError("Missing GRAPH_CLIENT_ID — check /opt/wdws/.env")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
if not GRAPH_CLIENT_SECRET:
    raise RuntimeError("Missing GRAPH_CLIENT_SECRET — check /opt/wdws/.env")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
if not GRAPH_TENANT_ID:
    raise RuntimeError("Missing GRAPH_TENANT_ID — check /opt/wdws/.env")
GRAPH_SENDER_EMAIL = (
    os.getenv("GRAPH_SENDER_EMAIL", "athena@seattleseahawks.me")
    or "athena@seattleseahawks.me"
).strip()
ATHENA_ALERT_EMAIL = (
    os.getenv("ATHENA_ALERT_EMAIL", "athena@seattleseahawks.me")
    or "athena@seattleseahawks.me"
).strip()

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

# EMBEDDING_MODEL and EMBEDDING_DIMENSIONS imported from embedding_service
# (BGE-M3 local model, 1024 dimensions)
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMAILS_DIR = Path(os.getenv("EMAILS_DIR", "/opt/wdws/data/emails"))
RAW_EMAILS_DIR = EMAILS_DIR / "emails"
ATTACHMENTS_DIR = EMAILS_DIR / "attachments"

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_sync")

# ============================================================
# Case number extraction (shared with ingest.py)
# ============================================================

STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
FEDERAL_CASE_RE = re.compile(
    r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?', re.IGNORECASE
)


def extract_case_numbers_from_text(text: str) -> List[str]:
    """Extract and normalize case numbers from text content."""
    found = set()
    for m in FEDERAL_CASE_RE.finditer(text):
        court, year, seq, judge = m.groups()
        normalized = f"{court}{year}CV{seq}"
        if judge:
            normalized += judge.upper()
        found.add(normalized.upper())
    for m in STATE_CASE_RE.finditer(text):
        found.add(m.group(0))
    return list(found)


# ============================================================
# Attachment OCR / text extraction
# ============================================================

EXTRACT_DIR = Path("/opt/wdws/data/extracted_attachments")
OCR_DPI = 150

# Skip these content types — they are not real attachments
_SKIP_CONTENT_TYPES = {
    "application/pkcs7-signature",
    "application/x-pkcs7-signature",
    "application/pgp-signature",
    "application/ms-tnef",
    "text/calendar",
}

# Skip tiny files (signatures, logos, spacer pixels)
_MIN_ATTACHMENT_SIZE = 512   # bytes


def _try_pdftotext_native(data: bytes) -> Optional[str]:
    """Fast-path extraction for native-text PDFs via pdftotext.

    Most court filings, letters, and reports are digital PDFs with a real
    text layer — pdftotext reads that layer in <1s with zero OCR cost and
    perfect fidelity. This helper returns the extracted text if it looks
    substantial, else None so the caller falls through to remote OCR.

    Heuristics for "substantial":
      - At least 200 characters returned
      - Average word length ≤ 40 chars (scanned-PDF garbage tends to come
        back as long unbroken runs with no whitespace)
    """
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", "-", "-"],
            input=data,
            capture_output=True,
            timeout=30,
        )
    except Exception:
        return None
    if result.returncode != 0:
        return None
    text = result.stdout.decode("utf-8", errors="ignore")
    if len(text) < 200:
        return None
    spaces = text.count(" ") + text.count("\n")
    non_ws = len(text) - spaces
    if spaces == 0 or non_ws / max(spaces, 1) > 40:
        return None
    return text


def _extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF using OCR — page-by-page."""
    try:
        import pytesseract
        from pdf2image import convert_from_path, pdfinfo_from_path
    except ImportError:
        log.warning("pytesseract/pdf2image not installed — skipping PDF OCR")
        return ""
    try:
        try:
            info = pdfinfo_from_path(str(pdf_path))
            num_pages = info.get("Pages", 0)
        except Exception:
            num_pages = 0

        parts = []
        if num_pages > 0:
            for pn in range(1, num_pages + 1):
                try:
                    images = convert_from_path(
                        str(pdf_path), dpi=OCR_DPI,
                        first_page=pn, last_page=pn, thread_count=1,
                    )
                    if images:
                        page_text = pytesseract.image_to_string(images[0])
                        if page_text and page_text.strip():
                            parts.append(f"[Page {pn}]\n{page_text}")
                        del images
                except Exception as e:
                    log.debug(f"  OCR page {pn} failed: {e}")
        else:
            images = convert_from_path(str(pdf_path), dpi=OCR_DPI, thread_count=1)
            for pn, img in enumerate(images, 1):
                page_text = pytesseract.image_to_string(img)
                if page_text and page_text.strip():
                    parts.append(f"[Page {pn}]\n{page_text}")
        return "\n\n".join(parts)
    except Exception as e:
        log.warning(f"OCR failed for {pdf_path.name}: {e}, trying pdftotext")
        try:
            result = subprocess.run(
                ["pdftotext", "-layout", str(pdf_path), "-"],
                capture_output=True, text=True, timeout=30,
            )
            return result.stdout
        except Exception:
            return ""


def _extract_image_text(image_path: Path) -> str:
    """Extract text from image using OCR."""
    try:
        import pytesseract
        from PIL import Image
        img = Image.open(image_path)
        return pytesseract.image_to_string(img)
    except Exception as e:
        log.error(f"Image OCR failed for {image_path.name}: {e}")
        return ""


def _extract_docx_text(docx_path: Path) -> str:
    """Extract text from DOCX file."""
    try:
        with zipfile.ZipFile(docx_path) as z:
            with z.open("word/document.xml") as f:
                tree = ET.parse(f)
                root = tree.getroot()
                ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                texts = []
                for p in root.findall(".//w:p", ns):
                    line = "".join(
                        (r.text or "") for r in p.findall(".//w:t", ns)
                    )
                    if line:
                        texts.append(line)
                return "\n".join(texts)
    except Exception as e:
        log.error(f"DOCX extraction failed for {docx_path.name}: {e}")
        return ""


    except Exception as e:
        log.error(f"XLS extraction failed for {xls_path.name}: {e}")
        return ""


_OCR_ENDPOINT_URL = os.getenv("OCR_ENDPOINT_URL", "").rstrip("/")
_OCR_ENDPOINT_TOKEN = os.getenv("OCR_ENDPOINT_TOKEN", "")
_OCR_ENDPOINT_TIMEOUT = float(os.getenv("OCR_ENDPOINT_TIMEOUT", "28800"))  # 8h

# Images under this size are almost certainly email signature / tracking
# pixels / Outlook auto-embedded decorations (image001.png, logo.png, etc.).
# Routing them to Falcon burns ~10s per image for 0-1 chars of noise.
# Keep them on the cheap local tesseract path instead.
_MIN_IMAGE_FOR_REMOTE_OCR = int(os.getenv("OCR_IMAGE_MIN_REMOTE", "30000"))  # 30 KB

# Filename patterns that are Outlook-auto-embedded junk regardless of size
_NOISE_IMAGE_PATTERNS = re.compile(
    r"^(image|thumbnail|tracker|spacer|pixel|logo|sig|signature)"
    r"[-_]?\d*\.(png|gif|jpg|jpeg|bmp)$",
    re.IGNORECASE,
)


def _looks_like_noise_image(filename: str, size: int, content_type: str) -> bool:
    """Is this attachment a likely email-signature / decoration / tracking
    pixel that we should not waste Falcon cycles on?"""
    ct = (content_type or "").lower()
    if not ct.startswith("image/"):
        return False
    if size < _MIN_IMAGE_FOR_REMOTE_OCR:
        return True
    if _NOISE_IMAGE_PATTERNS.match((filename or "").strip()):
        return True
    return False


# Health-check interval used during long OCRs. If Falcon hasn't touched
# a page in this many seconds, treat as stuck and give up.
_OCR_STUCK_THRESHOLD_SEC = float(os.getenv("OCR_STUCK_THRESHOLD_SEC", "300"))


def _ocr_is_stuck() -> bool:
    """Query /activity; return True if ANY current job looks stalled.

    Stuck heuristic: the job's last update is older than
    _OCR_STUCK_THRESHOLD_SEC AND older than 3× its last_page_took_sec.
    A first-page compile that takes a few minutes isn't stuck — it's slow.
    A job sitting for 5+ min with no page advance IS.
    """
    if not (_OCR_ENDPOINT_URL and _OCR_ENDPOINT_TOKEN):
        return False
    try:
        with httpx.Client(timeout=10) as c:
            r = c.get(
                f"{_OCR_ENDPOINT_URL}/activity",
                headers={"Authorization": f"Bearer {_OCR_ENDPOINT_TOKEN}"},
            )
        if r.status_code != 200:
            return False
        for j in r.json().get("jobs", []):
            stale = j.get("last_update_sec_ago", 0)
            last_page = j.get("last_page_took_sec") or 0
            # Stuck if no progress for _OCR_STUCK_THRESHOLD_SEC AND that's
            # noticeably longer than a typical page on this hardware.
            if stale > _OCR_STUCK_THRESHOLD_SEC and stale > max(last_page, 30) * 3:
                return True
    except Exception:
        pass
    return False


_SONNET_OCR_MODEL = "claude-sonnet-4-6"
_SONNET_OCR_ENABLED = os.getenv("SONNET_OCR_ENABLED", "1") != "0"
_SONNET_OCR_DPI = int(os.getenv("SONNET_OCR_DPI", "200"))
_SONNET_OCR_MAX_PAGES = int(os.getenv("SONNET_OCR_MAX_PAGES", "200"))
_SONNET_OCR_PROMPT = (
    "Transcribe all text from this page verbatim. Preserve line breaks, "
    "paragraphs, bullet structure, and table layout. Do not summarize, "
    "interpret, or add commentary. Output only the raw text as it appears "
    "on the page. If the page is truly blank, output exactly: [BLANK PAGE]"
)


def _ocr_via_sonnet(
    filename: str, content_type: str, data: bytes,
) -> Optional[Tuple[str, str]]:
    """Send a PDF or image to Claude Sonnet for OCR.

    Returns (text, method) on success, None on any failure (caller falls back
    to Falcon remote). Intended as the primary OCR path for new ingests when
    pdftotext fails to extract substantive native text.
    """
    if not _SONNET_OCR_ENABLED:
        return None
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None
    try:
        import base64 as _b64
        import io as _io
        from anthropic import Anthropic
    except ImportError:
        return None

    suffix = Path(filename).suffix.lower() if filename else ""
    ct = (content_type or "").lower()
    is_pdf = ct == "application/pdf" or suffix == ".pdf"
    is_image = ct.startswith("image/") or suffix in (
        ".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif",
    )
    if not (is_pdf or is_image):
        return None

    try:
        from PIL import Image  # noqa: F401
        if is_pdf:
            from pdf2image import convert_from_bytes
            images = convert_from_bytes(data, dpi=_SONNET_OCR_DPI, fmt="png")
            if len(images) > _SONNET_OCR_MAX_PAGES:
                log.warning("Sonnet OCR: skipping %s (%d pages exceeds cap %d)",
                            filename, len(images), _SONNET_OCR_MAX_PAGES)
                return None
        else:
            from PIL import Image as _PIL
            img = _PIL.open(_io.BytesIO(data))
            if img.mode != "RGB":
                img = img.convert("RGB")
            images = [img]
    except Exception as e:
        log.warning("Sonnet OCR render-failed for %s: %s", filename, str(e)[:100])
        return None

    try:
        client = Anthropic(api_key=api_key)
        parts: list[str] = []
        for img in images:
            buf = _io.BytesIO()
            img.save(buf, format="PNG", optimize=True)
            b64 = _b64.standard_b64encode(buf.getvalue()).decode("ascii")
            msg = client.messages.create(
                model=_SONNET_OCR_MODEL,
                max_tokens=4000,
                messages=[{
                    "role": "user",
                    "content": [
                        {"type": "image",
                         "source": {"type": "base64", "media_type": "image/png",
                                    "data": b64}},
                        {"type": "text", "text": _SONNET_OCR_PROMPT},
                    ],
                }],
            )
            if msg.content:
                parts.append(msg.content[0].text or "")
        text = "\n\n".join(p for p in parts if p and p.strip())
        if not text.strip():
            return None
        method = "sonnet_pdf" if is_pdf else "sonnet_image"
        return text, method
    except Exception as e:
        log.warning("Sonnet OCR API-failed for %s: %s", filename, str(e)[:150])
        return None


def _ocr_via_remote_endpoint(
    filename: str, content_type: str, data: bytes,
) -> Optional[Tuple[str, str]]:
    """Try the remote Falcon-OCR service (saturn) for this blob.

    The server side has no timeout — a multi-hundred-page PDF can legitimately
    take hours on a T4. We give up ONLY when _ocr_is_stuck() reports no page
    progress, not on a blunt wall-clock cap.

    Returns (text, method) on success, None on failure/not-configured.
    Caller falls back to local tesseract on None.
    """
    if not (_OCR_ENDPOINT_URL and _OCR_ENDPOINT_TOKEN):
        return None
    try:
        with httpx.Client(timeout=_OCR_ENDPOINT_TIMEOUT) as c:
            resp = c.post(
                f"{_OCR_ENDPOINT_URL}/ocr",
                headers={"Authorization": f"Bearer {_OCR_ENDPOINT_TOKEN}"},
                files={"file": (filename or "upload.bin",
                                data,
                                content_type or "application/octet-stream")},
                data={"mode": "plain"},
            )
        if resp.status_code != 200:
            log.warning("OCR endpoint %s returned %d: %s",
                        _OCR_ENDPOINT_URL, resp.status_code,
                        resp.text[:160])
            return None
        body = resp.json()
        text = body.get("text", "") or ""
        method = body.get("method") or "falcon_remote"
        pages = body.get("page_count", 0)
        log.info("OCR via %s: %d pages → %d chars in %.1fs",
                 _OCR_ENDPOINT_URL, pages, len(text),
                 body.get("elapsed_sec", -1))
        return text, method
    except httpx.ReadTimeout:
        # Hit the client-side ceiling (default 8h). At this point assume the
        # server is stuck — check and log for operator.
        stuck = _ocr_is_stuck()
        log.warning("OCR endpoint read timeout after %ds (stuck=%s); "
                    "falling back to local", _OCR_ENDPOINT_TIMEOUT, stuck)
        return None
    except Exception as e:
        log.warning("OCR endpoint call failed (%s): %s; falling back to local",
                    type(e).__name__, e)
        return None


def extract_text_from_binary(filename: str, content_type: str, data: bytes) -> Tuple[str, str]:
    """
    Extract text from a binary attachment.
    Returns (extracted_text, extraction_method).

    Priority:
      1. Remote Falcon-OCR endpoint (if OCR_ENDPOINT_URL set) — for PDFs and
         images. Falls back to local tesseract on any failure.
      2. Local extraction (tesseract OCR, python-docx, raw decode).

    The Falcon-OCR path produces markdown-structured output with proper
    headings, preserved reading order, and better table handling than
    tesseract. Local path remains as the reliable fallback.
    """
    if not data:
        return "", "empty"

    suffix = Path(filename).suffix.lower() if filename else ""
    ct = (content_type or "").lower()

    is_image = ct.startswith("image/") or suffix in (
        ".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif",
    )
    is_pdf = ct == "application/pdf" or suffix == ".pdf"

    # Fast-path: try pdftotext on PDFs before any OCR. Digital/native-text
    # PDFs (most court filings, letters, reports) have an embedded text layer
    # that pdftotext extracts instantly with perfect fidelity. Only scanned
    # image-based PDFs fall through to vision OCR.
    if is_pdf:
        native = _try_pdftotext_native(data)
        if native:
            return native, "pdftotext_native"

    # Vision-OCR fallbacks (scanned PDFs + non-noise images):
    #   1. Sonnet (primary — highest quality, paid per page)
    #   2. Falcon remote (backup — free, on saturn T4)
    #   3. local tesseract (last resort)
    want_ocr = is_pdf or (is_image and not _looks_like_noise_image(
        filename, len(data), content_type))
    if want_ocr:
        sonnet = _ocr_via_sonnet(filename, content_type, data)
        if sonnet is not None and sonnet[0].strip():
            return sonnet
        remote = _ocr_via_remote_endpoint(filename, content_type, data)
        if remote is not None and remote[0].strip():
            return remote

    # Write to temp file for local OCR tools that need a path
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r'[^\w\-\.]', '_', filename or "attachment")
    tmp_path = EXTRACT_DIR / f"_tmp_{uuid.uuid4().hex[:8]}_{safe_name}"
    try:
        tmp_path.write_bytes(data)

        if ct == "application/pdf" or suffix == ".pdf":
            text = _extract_pdf_text(tmp_path)
            method = "ocr_pdf_150dpi"
        elif ct.startswith("image/") or suffix in (".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".gif"):
            text = _extract_image_text(tmp_path)
            method = "ocr_image"
        elif ct == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" or suffix == ".docx":
            text = _extract_docx_text(tmp_path)
            method = "docx_xml"
        elif ct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or suffix == ".xlsx":
            text = _extract_xlsx_text(tmp_path)
            method = "xlsx_openpyxl"
        elif ct == "application/vnd.ms-excel" or suffix == ".xls":
            text = _extract_xls_text(tmp_path)
            method = "xls_soffice"
        elif ct in ("text/plain", "text/csv", "text/html") or suffix in (".txt", ".csv", ".html", ".htm"):
            text = data.decode("utf-8", errors="ignore")
            method = "text_direct"
        elif suffix in (".doc", ".ppt", ".pptx"):
            # Try as PDF fallback first (some .doc are actually PDFs)
            text = _extract_pdf_text(tmp_path)
            method = "ocr_pdf_fallback" if text.strip() else "unsupported"
        else:
            # Unknown — try PDF then image OCR
            text = _extract_pdf_text(tmp_path)
            if not text.strip():
                text = _extract_image_text(tmp_path)
                method = "ocr_image_fallback"
            else:
                method = "ocr_pdf_fallback"

        return text.strip() if text else "", method
    except Exception as e:
        log.error(f"  Text extraction failed for {filename}: {e}")
        return "", f"error: {str(e)[:80]}"
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


# ============================================================
# Data classes
# ============================================================

@dataclass
class DocumentChunk:
    content: str
    metadata: Dict
    chunk_index: int
    total_chunks: int


@dataclass
class IngestedDocument:
    doc_id: str
    domain: str
    source_path: str
    filename: str
    document_type: str
    title: str
    content_hash: str
    full_content: str
    metadata: Dict
    chunks: List[DocumentChunk] = field(default_factory=list)
    case_numbers: List[str] = field(default_factory=list)
    email_meta: Optional[Dict] = None


@dataclass
class AlertIssueBucket:
    severity: str
    heading: str
    count: int = 0
    details: List[str] = field(default_factory=list)


class SyncAlertCollector:
    def __init__(self, run_label: str):
        self.run_label = run_label
        self._buckets: Dict[Tuple[str, str], AlertIssueBucket] = {}

    def record(self, severity: str, heading: str, detail: str) -> None:
        severity = (severity or "warning").strip().lower()
        heading = (heading or "Unhandled sync issue").strip()
        detail = (detail or "").strip()
        key = (severity, heading)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = AlertIssueBucket(severity=severity, heading=heading)
            self._buckets[key] = bucket
        bucket.count += 1
        if detail and detail not in bucket.details and len(bucket.details) < 5:
            bucket.details.append(detail[:4000])

    def has_issues(self) -> bool:
        return bool(self._buckets)

    # Suppress resending an identical alert within this window. Counts still
    # accrue in ops.sync_alert_dedup so we can see how often the same
    # condition was observed.
    DEDUP_WINDOW_HOURS = 6

    def _content_fingerprint(self) -> str:
        """Fingerprint the alert body so runs producing the same set of
        (severity, heading, detail) tuples collapse into one notification.

        Deliberately excludes run-label, host, python, timestamp — those
        change every run. Error counts are included so an alert expanding
        from 18 -> 19 warnings does notify. Detail text is hashed raw so
        the fingerprint flips the moment a new failing URL appears.
        """
        parts: List[str] = []
        for key in sorted(self._buckets.keys()):
            bucket = self._buckets[key]
            details_joined = "\n".join(sorted(bucket.details))
            parts.append(
                f"{bucket.severity}|{bucket.heading}|{bucket.count}|{details_joined}"
            )
        blob = "\n---\n".join(parts).encode("utf-8", errors="replace")
        return hashlib.sha256(blob).hexdigest()

    async def _should_send(self, content_hash: str) -> bool:
        """Return True if this fingerprint hasn't been sent inside the dedup
        window. Updates ops.sync_alert_dedup either way. Fails open: any DB
        error logs and still sends (noisy is better than silent)."""
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=1)
        except Exception as e:
            log.warning(f"Alert dedup: DB connect failed ({e}); sending anyway")
            return True
        try:
            row = await pool.fetchrow(
                """
                SELECT last_sent_at,
                       (now() - last_sent_at) < ($2 * INTERVAL '1 hour') AS within_window
                FROM ops.sync_alert_dedup
                WHERE run_label = $1 AND content_hash = $3
                """,
                self.run_label, self.DEDUP_WINDOW_HOURS, content_hash,
            )
            if row and row["within_window"]:
                await pool.execute(
                    """
                    UPDATE ops.sync_alert_dedup
                       SET last_seen_at   = now(),
                           suppress_count = suppress_count + 1
                     WHERE run_label = $1 AND content_hash = $2
                    """,
                    self.run_label, content_hash,
                )
                return False
            # First time seeing this fingerprint, or outside the window: send.
            await pool.execute(
                """
                INSERT INTO ops.sync_alert_dedup
                    (run_label, content_hash, last_sent_at, last_seen_at, send_count)
                VALUES ($1, $2, now(), now(), 1)
                ON CONFLICT (run_label, content_hash) DO UPDATE
                SET last_sent_at = now(),
                    last_seen_at = now(),
                    send_count   = ops.sync_alert_dedup.send_count + 1
                """,
                self.run_label, content_hash,
            )
            return True
        except Exception as e:
            log.warning(f"Alert dedup: query failed ({e}); sending anyway")
            return True
        finally:
            try:
                await pool.close()
            except Exception:
                pass

    async def flush_email(self) -> None:
        if not self._buckets:
            return

        errors = sum(bucket.count for bucket in self._buckets.values() if bucket.severity == "error")
        warnings = sum(bucket.count for bucket in self._buckets.values() if bucket.severity != "error")
        subject = (
            f"Athena Alert — {self.run_label}: {errors} error(s), {warnings} warning(s)"
        )

        content_hash = self._content_fingerprint()
        should_send = await self._should_send(content_hash)
        if not should_send:
            log.info(
                "Alert suppressed (identical fingerprint sent within last %sh): %s",
                self.DEDUP_WINDOW_HOURS, subject,
            )
            return

        sections = [{
            "heading": "Run summary",
            "type": "info" if errors == 0 else "error",
            "plain_summary": (
                f"{self.run_label} completed with {errors} error(s) and {warnings} warning(s)."
            ),
            "content": "\n".join([
                f"Run label: {self.run_label}",
                f"Errors: {errors}",
                f"Warnings: {warnings}",
                f"Host: {os.uname().nodename}",
                f"Python: {sys.executable}",
                f"Time (UTC): {datetime.now(timezone.utc).isoformat()}",
            ]),
        }]

        for bucket in sorted(
            self._buckets.values(),
            key=lambda item: (0 if item.severity == "error" else 1, item.heading.lower()),
        ):
            sections.append({
                "heading": bucket.heading,
                "type": "error" if bucket.severity == "error" else "warning",
                "plain_summary": f"{bucket.count} occurrence(s)",
                "content": "\n\n".join(bucket.details) if bucket.details else "No details captured.",
            })

        body_html = build_alert_notification_html(subject, sections)
        result = await send_alert_email(
            tenant_id=GRAPH_TENANT_ID,
            client_id=GRAPH_CLIENT_ID,
            client_secret=GRAPH_CLIENT_SECRET,
            sender=GRAPH_SENDER_EMAIL,
            to_recipients=[ATHENA_ALERT_EMAIL],
            subject=subject,
            body_html=body_html,
            importance="high" if errors else "normal",
        )
        if result.get("status") == "sent":
            log.info(
                "Sent Athena alert email for %s (%s error(s), %s warning(s))",
                self.run_label,
                errors,
                warnings,
            )
        else:
            log.error("Failed sending Athena alert email: %s", result.get("error", "unknown error"))


_ACTIVE_ALERT_COLLECTOR: Optional[SyncAlertCollector] = None


def _record_run_issue(severity: str, heading: str, detail: str) -> None:
    if _ACTIVE_ALERT_COLLECTOR is not None:
        _ACTIVE_ALERT_COLLECTOR.record(severity, heading, detail)


async def _run_with_error_alerts(run_label: str, coro):
    global _ACTIVE_ALERT_COLLECTOR
    collector = SyncAlertCollector(run_label)
    _ACTIVE_ALERT_COLLECTOR = collector
    try:
        return await coro
    except Exception:
        collector.record("error", f"{run_label} crashed", traceback.format_exc())
        raise
    finally:
        _ACTIVE_ALERT_COLLECTOR = None
        try:
            await collector.flush_email()
        except Exception as e:
            log.error("Failed to flush Athena alert email: %s", e)


# ============================================================
# MIME-fetch failure tracking (dead-letter)
# ============================================================
#
# Graph /$value returns HTTP 500 for items with no RFC-822 MIME
# representation — meeting responses (IPM.Schedule.Meeting.Resp.*),
# meeting cancellations, Teams chat items, voicemail, and occasional
# corrupt items. These failures repeat every sync, producing endless
# identical operator alerts. We detect HTTP 500 from /$value as a
# permanent condition and persist the graph_id to ops.email_mime_failures
# so future runs skip it. (Graph v1.0 does not expose itemClass on the
# Message resource's $select, so class-based pre-filtering is not
# available — failure-based marking is what we use instead.)


class MimeFailureTracker:
    """Tracks Graph MIME fetch failures across a sync run.

    Loads the set of (mailbox, graph_id) pairs that should be skipped
    (permanent or attempted >= 3 times). Callers check should_skip()
    before the MIME call; after a MIME failure they record() to update
    the counter. flush() upserts all new/updated failures back to the DB.
    """

    SKIP_AFTER_ATTEMPTS = 3

    def __init__(self) -> None:
        # (mailbox, graph_id) already known to be unfetchable — skip entirely.
        self._skip: Set[Tuple[str, str]] = set()
        # (mailbox, graph_id) -> pending upsert payload for this run.
        self._pending: Dict[Tuple[str, str], Dict] = {}

    async def load(self, pool) -> None:
        try:
            rows = await pool.fetch(
                "SELECT mailbox, graph_id FROM ops.email_mime_failures "
                "WHERE permanent = TRUE OR attempt_count >= $1",
                self.SKIP_AFTER_ATTEMPTS,
            )
            self._skip = {(r["mailbox"], r["graph_id"]) for r in rows}
            if self._skip:
                log.info(f"MIME failure tracker: skipping {len(self._skip)} known-bad message IDs")
        except Exception as e:
            # Table may not exist yet on first-deploy — fail open (don't block sync).
            log.warning(f"MIME failure tracker: load failed ({e}); continuing with empty skip set")
            self._skip = set()

    def should_skip(self, mailbox: str, graph_id: str) -> bool:
        return (mailbox, graph_id) in self._skip

    def record(
        self,
        mailbox: str,
        graph_id: str,
        *,
        item_class: Optional[str] = None,
        subject: Optional[str] = None,
        http_status: Optional[int] = None,
        error: Optional[str] = None,
        permanent: bool = False,
    ) -> None:
        key = (mailbox, graph_id)
        existing = self._pending.get(key)
        if existing is None:
            self._pending[key] = {
                "item_class": item_class,
                "subject": (subject or "")[:500],
                "http_status": http_status,
                "error": (error or "")[:2000] or None,
                "permanent": permanent,
            }
        else:
            # Upgrade with any additional info seen this run.
            if item_class and not existing.get("item_class"):
                existing["item_class"] = item_class
            if subject and not existing.get("subject"):
                existing["subject"] = subject[:500]
            if http_status is not None:
                existing["http_status"] = http_status
            if error:
                existing["error"] = error[:2000]
            if permanent:
                existing["permanent"] = True

    async def flush(self, pool) -> None:
        if not self._pending:
            return
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for (mailbox, graph_id), info in self._pending.items():
                        await conn.execute(
                            """
                            INSERT INTO ops.email_mime_failures
                                (mailbox, graph_id, item_class, subject,
                                 last_http_status, last_error, permanent)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (mailbox, graph_id) DO UPDATE
                            SET last_failed_at   = now(),
                                attempt_count    = ops.email_mime_failures.attempt_count + 1,
                                item_class       = COALESCE(EXCLUDED.item_class, ops.email_mime_failures.item_class),
                                subject          = COALESCE(EXCLUDED.subject, ops.email_mime_failures.subject),
                                last_http_status = COALESCE(EXCLUDED.last_http_status, ops.email_mime_failures.last_http_status),
                                last_error       = COALESCE(EXCLUDED.last_error, ops.email_mime_failures.last_error),
                                permanent        = ops.email_mime_failures.permanent OR EXCLUDED.permanent
                            """,
                            mailbox, graph_id,
                            info.get("item_class"),
                            info.get("subject"),
                            info.get("http_status"),
                            info.get("error"),
                            info.get("permanent", False),
                        )
            log.info(f"MIME failure tracker: recorded {len(self._pending)} failure(s) to ops.email_mime_failures")
        except Exception as e:
            log.error(f"MIME failure tracker: flush failed ({e}); failures will be retried next run")


_ACTIVE_MIME_TRACKER: Optional[MimeFailureTracker] = None


def _mime_tracker() -> Optional[MimeFailureTracker]:
    return _ACTIVE_MIME_TRACKER


async def _finalize_sync_pool(pool) -> None:
    """Flush the MIME failure tracker, then close the DB pool.

    Used at every exit path in main() so we always persist newly-seen
    Graph MIME failures before tearing down.
    """
    global _ACTIVE_MIME_TRACKER
    if _ACTIVE_MIME_TRACKER is not None:
        try:
            await _ACTIVE_MIME_TRACKER.flush(pool)
        except Exception as e:
            log.error(f"MIME failure tracker flush error on finalize: {e}")
        _ACTIVE_MIME_TRACKER = None
    # NOTE: call the pool's own close() here, not the helper — self-recursion.
    await asyncpg.pool.Pool.close(pool)


# ============================================================
# Text chunker — imported from email_chunker module.
# See: TextChunker is a back-compat alias for EmailAwareChunker.
# Email body ingest should call .split_email_body() to get structure-aware
# chunking; .split() remains available for generic (non-email) text.
# ============================================================


# ============================================================
# Embedding client
# ============================================================

class EmbeddingClient:
    """Local BGE-M3 embedding client with batching (delegates to embedding_service)."""

    def __init__(self, **kwargs):
        self.total_tokens = 0
        self.total_requests = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        result = embed_texts_sync(texts)
        self.total_requests += 1
        return result

    async def close(self):
        pass  # No HTTP client to close


# ============================================================
# Graph API client
# ============================================================

class GraphClient:
    """Microsoft Graph API client using app-only (client credentials) auth."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    async def authenticate(self) -> bool:
        """Get app-only access token using client credentials."""
        try:
            resp = await self.client.post(
                TOKEN_URL,
                data={
                    "client_id": GRAPH_CLIENT_ID,
                    "client_secret": GRAPH_CLIENT_SECRET,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            self.access_token = data["access_token"]
            self.token_expires_at = time.time() + data.get("expires_in", 3600) - 300
            log.info("✓ Graph API authenticated (app-only, client credentials)")
            return True
        except Exception as e:
            log.error(f"✗ Graph API authentication failed: {e}")
            _record_run_issue("error", "Graph API authentication failed", str(e))
            return False

    async def _ensure_token(self):
        """Refresh token if expired."""
        if time.time() >= self.token_expires_at:
            await self.authenticate()

    async def _get(self, url: str, params: dict = None) -> Optional[dict]:
        """Make authenticated GET request with iterative rate-limit retry."""
        for attempt in range(5):
            await self._ensure_token()
            try:
                resp = await self.client.get(
                    url,
                    headers={"Authorization": f"Bearer {self.access_token}"},
                    params=params,
                )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < 4:
                    retry_after = int(e.response.headers.get("Retry-After", "10"))
                    log.warning(f"  Rate limited (attempt {attempt+1}/5), waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    continue
                log.error(f"  Graph API error {e.response.status_code}: {e.response.text[:200]}")
                _record_run_issue(
                    "warning",
                    "Graph API request failed",
                    f"URL: {url}\nHTTP {e.response.status_code}\n{e.response.text[:500]}",
                )
                return None
            except Exception as e:
                log.error(f"  Graph API request failed: {e}")
                _record_run_issue("warning", "Graph API request failed", f"URL: {url}\n{e}")
                return None
        return None

    async def _get_raw(self, url: str) -> Tuple[Optional[bytes], Optional[Dict]]:
        """Make authenticated GET request returning raw bytes (for MIME).

        Returns (bytes, None) on success, or (None, error_info) on failure.
        error_info is a dict with keys: status (int|None), message (str),
        permanent (bool — True when the error is known-permanent, e.g. 500
        from /$value indicating a non-MIMEable item class).
        """
        for attempt in range(5):
            await self._ensure_token()
            try:
                resp = await self.client.get(
                    url,
                    headers={"Authorization": f"Bearer {self.access_token}"},
                )
                resp.raise_for_status()
                return resp.content, None
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429 and attempt < 4:
                    retry_after = int(e.response.headers.get("Retry-After", "10"))
                    log.warning(f"  Rate limited (attempt {attempt+1}/5), waiting {retry_after}s...")
                    await asyncio.sleep(retry_after)
                    continue
                log.error(f"  MIME fetch error {status}")
                _record_run_issue(
                    "warning",
                    "Graph MIME fetch failed",
                    f"URL: {url}\nHTTP {status}",
                )
                # HTTP 500 from /$value on a specific message is Graph's
                # signal that the item has no RFC-822 MIME representation
                # (meeting responses, Teams items, voicemail). Mark these
                # permanent so we never retry them.
                permanent = status == 500 and url.endswith("/$value")
                return None, {"status": status, "message": f"HTTP {status}", "permanent": permanent}
            except Exception as e:
                log.error(f"  MIME fetch failed: {e}")
                _record_run_issue("warning", "Graph MIME fetch failed", f"URL: {url}\n{e}")
                return None, {"status": None, "message": str(e), "permanent": False}
        return None, {"status": None, "message": "exhausted retries", "permanent": False}

    async def get_user_info(self, mailbox: str) -> Optional[dict]:
        """Get user profile info."""
        return await self._get(f"{GRAPH_BASE_URL}/users/{mailbox}")

    async def list_messages(
        self,
        mailbox: str,
        filter_expr: str = None,
        search: str = None,
        top: int = 100,
        skip: int = 0,
        order_by: str = "receivedDateTime desc",
        select: str = None,
    ) -> Optional[dict]:
        """List messages from a mailbox with optional filter/search."""
        params = {"$top": top}
        if select:
            params["$select"] = select
        if filter_expr:
            params["$filter"] = filter_expr
        if search:
            params["$search"] = f'"{search}"'
        else:
            # $orderby not compatible with $search
            if order_by:
                params["$orderby"] = order_by
        if skip > 0:
            params["$skip"] = skip

        return await self._get(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages",
            params=params,
        )

    async def get_message_mime(
        self, mailbox: str, message_id: str
    ) -> Tuple[Optional[bytes], Optional[Dict]]:
        """Get message in MIME (.eml) format.

        Returns (mime_bytes, None) on success or (None, error_info) on
        failure. See _get_raw for the error_info shape.
        """
        return await self._get_raw(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/$value"
        )

    async def close(self):
        await self.client.aclose()


# ============================================================
# Email MIME parsing
# ============================================================

def parse_mime_bytes(mime_data: bytes) -> Optional[Dict]:
    """Parse MIME bytes into structured email data, including attachments.
    
    Returns dict with keys: headers, body, full_text, attachments.
    attachments is a list of (filename, content_type, raw_bytes) tuples.
    """
    try:
        msg = email_lib.message_from_bytes(mime_data, policy=policy.default)

        headers = {
            "message_id": str(msg.get("Message-ID", "")).strip(),
            "from": str(msg.get("From", "")),
            "to": str(msg.get("To", "")),
            "cc": str(msg.get("Cc", "")),
            "bcc": str(msg.get("Bcc", "")),
            "subject": str(msg.get("Subject", "")),
            "date": str(msg.get("Date", "")),
            "in_reply_to": str(msg.get("In-Reply-To", "")).strip(),
            "references": str(msg.get("References", "")).strip(),
        }

        body = ""
        has_attachments = False
        attachments: List[Tuple[str, str, bytes]] = []

        if msg.is_multipart():
            for part in msg.walk():
                disp = str(part.get("Content-Disposition", ""))
                fname = part.get_filename()

                if "attachment" in disp.lower() or fname:
                    has_attachments = True

                    # Extract the attachment binary
                    ct = part.get_content_type() or "application/octet-stream"

                    # Skip signatures, calendar invites, and other non-document types
                    if ct.lower() in _SKIP_CONTENT_TYPES:
                        continue

                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    # Skip tiny files (logos, spacer pixels, tracking pixels)
                    if len(payload) < _MIN_ATTACHMENT_SIZE:
                        continue

                    if not fname:
                        ext = {
                            "application/pdf": ".pdf",
                            "image/png": ".png",
                            "image/jpeg": ".jpg",
                            "text/plain": ".txt",
                            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                        }.get(ct, ".bin")
                        fname = f"attachment{ext}"

                    attachments.append((fname, ct, payload))
                    continue

                if part.get_content_type() == "text/plain":
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

        headers["has_attachments"] = has_attachments

        full_text = (
            f"Subject: {headers['subject']}\n"
            f"From: {headers['from']}\n"
            f"To: {headers['to']}\n"
            f"Cc: {headers['cc']}\n"
            f"Bcc: {headers['bcc']}\n"
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
        log.error(f"MIME parse failed: {e}")
        _record_run_issue("error", "MIME parse failed", str(e))
        return None


def determine_email_direction(from_addr: str, mailbox: str) -> str:
    """Determine if email is inbound or outbound relative to our mailbox."""
    if mailbox.lower() in from_addr.lower():
        return "outbound"
    return "inbound"


def _extract_address_domain(address: str) -> str:
    """Extract normalized domain portion from an email header/address string."""
    value = (address or "").strip().lower()
    if not value:
        return ""

    match = re.search(r"<([^>]+)>", value)
    if match:
        value = match.group(1).strip().lower()

    if "@" not in value:
        return ""
    return value.rsplit("@", 1)[-1]


def _domain_pattern_matches(address_domain: str, pattern: str) -> bool:
    """Match exact domains and wildcard patterns like *.uscourts.gov."""
    address_domain = (address_domain or "").strip().lower()
    pattern = (pattern or "").strip().lower()
    if not address_domain or not pattern:
        return False

    if pattern.startswith("*."):
        base = pattern[2:]
        return address_domain == base or address_domain.endswith(f".{base}")

    return address_domain == pattern


def address_matches_domains(address: str, domains: List[str]) -> bool:
    """Check if an email address belongs to one of the target domains."""
    addr_domain = _extract_address_domain(address)
    for domain in domains:
        if _domain_pattern_matches(addr_domain, domain):
            return True
    return False


def address_matches_specific(address: str, specific_emails: List[str]) -> bool:
    """Check if an email address matches one of the specific target addresses."""
    addr_lower = address.lower().strip()
    return addr_lower in {e.lower() for e in specific_emails}


def email_involves_targets(
    msg_data: dict,
    domains: List[str],
    specific_emails: Optional[List[str]] = None,
) -> bool:
    """Check if any sender or recipient matches a target domain or specific address."""
    specific_emails = specific_emails or []

    def _matches(addr: str) -> bool:
        return address_matches_domains(addr, domains) or address_matches_specific(addr, specific_emails)

    # Check from
    from_addr = msg_data.get("from", {}).get("emailAddress", {}).get("address", "")
    if _matches(from_addr):
        return True

    # Check to recipients
    for recip in msg_data.get("toRecipients", []):
        addr = recip.get("emailAddress", {}).get("address", "")
        if _matches(addr):
            return True

    # Check cc recipients
    for recip in msg_data.get("ccRecipients", []):
        addr = recip.get("emailAddress", {}).get("address", "")
        if _matches(addr):
            return True

    # Check bcc recipients
    for recip in msg_data.get("bccRecipients", []):
        addr = recip.get("emailAddress", {}).get("address", "")
        if _matches(addr):
            return True

    return False


# Keep old name as alias for backward compat
def email_involves_domains(msg_data: dict, domains: List[str]) -> bool:
    return email_involves_targets(msg_data, domains, TARGET_SPECIFIC_EMAILS)


# ============================================================
# Helper: vec literal for halfvec
# ============================================================

def _vec_literal(embedding: List[float]) -> str:
    """Convert embedding list to PostgreSQL halfvec literal."""
    return _embedding_vec_literal(embedding)


def _parse_email_date(date_str: str) -> Optional[datetime]:
    """Parse email date string to datetime."""
    if not date_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str)
    except Exception:
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None


def _safe_mailbox_dir_name(mailbox: str) -> str:
    """Convert an email address into the historical mailbox directory name."""
    mailbox = (mailbox or "unknown").strip().lower()
    return re.sub(r"[^a-z0-9._-]", "_", mailbox.replace("@", "_"))


def _safe_subject_fragment(subject: str, max_len: int = 80) -> str:
    """Create a filesystem-safe, human-readable subject fragment."""
    text = (subject or "No Subject").strip()
    text = re.sub(r"[^\w\s\-.,()]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return (text[:max_len].rstrip(" .-_")) or "No Subject"


def _safe_attachment_filename(filename: str) -> str:
    """Create a filesystem-safe attachment filename while preserving extension."""
    name = Path(filename or "attachment.bin").name.strip() or "attachment.bin"
    name = re.sub(r"[^\w\s\-.,()]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:180].rstrip(" .-_") or "attachment.bin"


def persist_graph_mime_copy(
    mailbox: str,
    mime_data: bytes,
    subject: str,
    date_hint: Optional[str],
) -> Optional[str]:
    """Save a Graph-downloaded MIME message to the local historical .eml archive."""
    if not mime_data:
        return None

    mailbox_dir = RAW_EMAILS_DIR / _safe_mailbox_dir_name(mailbox)
    mailbox_dir.mkdir(parents=True, exist_ok=True)

    date_prefix = (date_hint or datetime.now(timezone.utc).strftime("%Y-%m-%d"))[:10]
    base_name = f"{date_prefix} - {_safe_subject_fragment(subject)}"
    base_name = base_name[:180].rstrip(" .-_") or f"{date_prefix} - email"

    candidate = mailbox_dir / f"{base_name}.eml"
    digest = hashlib.sha256(mime_data).hexdigest()[:10]
    counter = 1

    while candidate.exists():
        try:
            if candidate.read_bytes() == mime_data:
                return str(candidate)
        except Exception:
            pass
        candidate = mailbox_dir / f"{base_name}_{counter}_{digest}.eml"
        counter += 1

    candidate.write_bytes(mime_data)
    return str(candidate)


def persist_graph_attachment_copy(
    mailbox: str,
    filename: str,
    data: bytes,
    email_subject: str,
    date_hint: Optional[str],
) -> Optional[str]:
    """Save an attachment binary to the local historical archive."""
    if not data:
        return None

    mailbox_dir = ATTACHMENTS_DIR / _safe_mailbox_dir_name(mailbox)
    mailbox_dir.mkdir(parents=True, exist_ok=True)

    date_prefix = (date_hint or datetime.now(timezone.utc).strftime("%Y-%m-%d"))[:10]
    subject_dir = f"{date_prefix} - {_safe_subject_fragment(email_subject, max_len=60)}"
    subject_dir = subject_dir[:180].rstrip(" .-_") or f"{date_prefix} - attachments"

    attachment_dir = mailbox_dir / subject_dir
    attachment_dir.mkdir(parents=True, exist_ok=True)

    safe_name = _safe_attachment_filename(filename)
    candidate = attachment_dir / safe_name
    digest = hashlib.sha256(data).hexdigest()[:10]
    counter = 1

    while candidate.exists():
        try:
            if candidate.read_bytes() == data:
                return str(candidate)
        except Exception:
            pass

        stem = candidate.stem[:120].rstrip(" .-_") or "attachment"
        suffix = candidate.suffix
        candidate = attachment_dir / f"{stem}_{counter}_{digest}{suffix}"
        counter += 1

    candidate.write_bytes(data)
    return str(candidate)


# ============================================================
# Database writer
# ============================================================

def _match_existing_attachment_record(
    existing_rows: List[Dict],
    filename: str,
    content_type: str,
    file_size: int,
) -> Tuple[Optional[Dict], Optional[int]]:
    """Find the best existing attachment row for a MIME attachment."""
    norm_name = (filename or "").strip().lower()
    norm_type = (content_type or "").strip().lower()

    best_idx = None
    best_row = None
    best_score = -1

    for idx, row in enumerate(existing_rows):
        row_name = (row.get("filename") or "").strip().lower()
        row_type = (row.get("content_type") or "").strip().lower()
        row_size = row.get("file_size") or 0

        score = -1
        if norm_name and row_name == norm_name and norm_type and row_type == norm_type and row_size == file_size:
            score = 4
        elif norm_name and row_name == norm_name and row_size == file_size:
            score = 3
        elif norm_name and row_name == norm_name:
            score = 2
        elif norm_type and row_type == norm_type and row_size == file_size:
            score = 1

        if score > best_score:
            best_score = score
            best_idx = idx
            best_row = row

    if best_score < 0:
        return None, None
    return best_row, best_idx


async def process_email_attachments(
    pool: asyncpg.Pool,
    embedder: EmbeddingClient,
    chunker: TextChunker,
    email_doc_id: str,
    email_source_path: str,
    email_subject: str,
    attachments: List[Tuple[str, str, bytes]],
    existing_attachment_rows: Optional[List[Dict]] = None,
    mailbox: Optional[str] = None,
    email_date: Optional[str] = None,
) -> int:
    """
    Process and ingest attachments extracted from a MIME email.
    
    For each attachment:
      1. OCR / extract text
      2. Create child document in core.documents + chunks with embeddings
      3. Insert into legal.email_attachments linking parent email
      4. Create core.document_relationships record
    
    Returns number of attachments successfully processed.
    """
    if not attachments:
        return 0

    processed = 0
    existing_attachment_rows = [dict(r) for r in (existing_attachment_rows or [])]

    for fname, content_type, data in attachments:
        file_size = len(data)
        existing_row, existing_idx = _match_existing_attachment_record(
            existing_attachment_rows,
            fname,
            content_type,
            file_size,
        )
        if existing_idx is not None:
            existing_attachment_rows.pop(existing_idx)

        if existing_row and existing_row.get("child_doc_id"):
            log.info(f"    📎 {fname} ({file_size:,} bytes) — already linked, skipping")
            continue

        att_id = existing_row.get("id") if existing_row else uuid.uuid4()
        local_attachment_path = persist_graph_attachment_copy(
            mailbox=mailbox or "unknown",
            filename=fname,
            data=data,
            email_subject=email_subject,
            date_hint=email_date,
        )
        att_source_path = local_attachment_path or f"{email_source_path}/attachment/{fname}"

        try:
            # 1. OCR / extract text (run in thread to avoid blocking event loop).
            # Binary extraction stays here — it's a source-side concern.
            extracted_text, extraction_method = await asyncio.to_thread(
                extract_text_from_binary, fname, content_type, data
            )

            # Build content for indexing — real extracted text if we got it,
            # otherwise a searchable placeholder so the filename still surfaces.
            if not extracted_text or len(extracted_text.strip()) < 20:
                log.info(f"    📎 {fname} ({file_size:,} bytes) — no extractable text")
                full_content = (
                    f"Attachment filename: {fname}\n"
                    f"Content-Type: {content_type or 'application/octet-stream'}\n"
                    f"File size: {file_size}\n"
                    f"Extraction method: {extraction_method}\n"
                    f"Original binary stored locally for retrieval."
                )
            else:
                full_content = extracted_text

            title = f"{fname}"
            if email_subject:
                title = f"{fname} (from: {email_subject[:80]})"

            # Stamp ocr_version so the Sonnet backfill worker treats this doc
            # as already up-to-date. Any extraction method in the current tree
            # (pdftotext_native, sonnet_pdf, sonnet_image, docx_xml, text_direct)
            # produces primary-quality text; stamping avoids the backfill
            # re-running on freshly-ingested attachments.
            _SONNET_V1_METHODS = {
                "pdftotext_native", "sonnet_pdf", "sonnet_image",
                "docx_xml", "xlsx_openpyxl", "xls_soffice", "text_direct",
            }
            metadata = {
                "parent_email_doc_id": email_doc_id,
                "attachment_id": str(att_id),
                "extraction_method": extraction_method,
                "source": "graph_api",
                "parent_email_source_path": email_source_path,
                "local_attachment_path": local_attachment_path,
                "original_filename": fname,
                "content_type": content_type,
            }
            if extraction_method in _SONNET_V1_METHODS:
                metadata["ocr_version"] = "sonnet-v1"

            # Hand off to the unified pipeline. It handles chunking +
            # enrichment + embedding + core.documents + core.document_chunks
            # + legal.case_documents (from auto-extracted case numbers) +
            # core.document_relationships (parent email → attachment) +
            # legal.email_attachments (via attachment_meta), all in one txn.
            # Upload original attachment bytes to MinIO. If this fails (or
            # isn't configured) we still ingest — the extracted_text and FS
            # copy remain the source of truth for the row until a future
            # backfill can re-upload.
            att_blob = blob_storage.upload_attachment(
                data,
                parent_doc_id=email_doc_id,
                filename=fname,
                content_type=content_type or "",
            )

            att_spec = DocumentSpec(
                domain="legal",
                document_type="email_attachment",
                source_path=att_source_path,
                title=title,
                filename=fname,
                full_content=full_content,
                metadata=metadata,
                parent_doc_id=email_doc_id,
                relationship_type="has_attachment",
                attachment_meta={
                    "id": str(att_id),
                    "filename": fname,
                    "content_type": content_type,
                    "file_size": file_size,
                    "extracted_text": extracted_text or "",
                    "extraction_method": extraction_method,
                },
                storage_backend=att_blob.storage_backend if att_blob else None,
                storage_uri=att_blob.storage_uri if att_blob else None,
                storage_sha256=att_blob.storage_sha256 if att_blob else None,
                storage_size_bytes=att_blob.storage_size_bytes if att_blob else None,
            )

            result = await pipeline_ingest(att_spec, pool=pool)

            if result.error:
                log.error(f"    ✗ Attachment ingest failed: {fname}: {result.error}")
                _record_run_issue(
                    "error",
                    "Attachment ingest failed",
                    f"attachment: {fname}\nparent_email: {email_subject[:200]}\n"
                    f"error: {result.error}",
                )
                continue

            if result.was_existing:
                log.info(f"    📎 {fname} ({file_size:,} bytes) — already ingested, skipping")
            else:
                text_len = len(full_content)
                log.info(
                    f"    📎 {fname} ({file_size:,} bytes) → "
                    f"{text_len:,} chars, {result.chunk_count} chunks [{extraction_method}]"
                )
            processed += 1

        except Exception as e:
            log.error(f"    ✗ Attachment failed: {fname}: {e}")
            _record_run_issue(
                "error",
                "Attachment ingest failed",
                f"attachment: {fname}\nparent_email: {email_subject[:200]}\nerror: {e}",
            )

    await refresh_email_attachment_text(pool, email_doc_id)

    return processed


async def refresh_email_attachment_text(pool: asyncpg.Pool, email_doc_id: str) -> None:
    """Refresh legacy attachment_text cache on the parent email row.

    Attachment child documents and embeddings are the primary source of truth,
    but older Athena/email-agent paths still consult legal.email_metadata
    attachment_text directly. Keep that field synchronized from the extracted
    attachment text we already persist in legal.email_attachments.
    """
    email_uuid = uuid.UUID(str(email_doc_id))

    async with pool.acquire() as conn:
        combined = await conn.fetchval(
            """
            SELECT COALESCE(
                STRING_AGG(formatted_text, E'\n\n---\n\n' ORDER BY filename, attachment_id),
                ''
            )
            FROM (
                SELECT id AS attachment_id,
                       filename,
                       CASE
                           WHEN COALESCE(BTRIM(extracted_text), '') = '' THEN NULL
                           ELSE '[Attachment: ' || filename || E']\n' || extracted_text
                       END AS formatted_text
                FROM legal.email_attachments
                WHERE email_doc_id = $1::uuid
            ) att
            WHERE formatted_text IS NOT NULL
            """,
            email_uuid,
        )

        await conn.execute(
            """
            UPDATE legal.email_metadata
            SET attachment_text = $2
            WHERE document_id = $1::uuid
            """,
            email_uuid,
            combined or "",
        )


# ============================================================
# Main sync logic
# ============================================================

async def get_existing_message_ids(pool: asyncpg.Pool) -> Set[str]:
    """Get all message_ids already in the DB for deduplication."""
    rows = await pool.fetch(
        "SELECT message_id FROM legal.email_metadata WHERE message_id IS NOT NULL"
    )
    return {r["message_id"] for r in rows}


async def _fetch_recent_target_emails(
    graph: GraphClient,
    mailbox: str,
    domains: List[str],
    specific_emails: List[str],
    since: str,
    existing_msg_ids: Set[str],
) -> List[Tuple[dict, bytes]]:
    """Fetch recent mailbox messages and filter locally across from/to/cc/bcc."""
    results: List[Tuple[dict, bytes]] = []
    seen_ids: Set[str] = set()
    skipped_existing = 0
    matched = 0
    scanned = 0
    page_count = 0

    next_link = None
    filter_expr = f"receivedDateTime ge {since}T00:00:00Z"
    select = (
        "id,subject,from,toRecipients,ccRecipients,bccRecipients,"
        "receivedDateTime,bodyPreview,hasAttachments,internetMessageId"
    )
    tracker = _mime_tracker()
    skipped_known_failed = 0

    log.info(f"  📧 Scanning recent mail in {mailbox} since {since} across from/to/cc/bcc...")

    while True:
        if next_link:
            data = await graph._get(next_link)
        else:
            data = await graph.list_messages(
                mailbox=mailbox,
                filter_expr=filter_expr,
                top=250,
                order_by="receivedDateTime desc",
                select=select,
            )

        if not data:
            break

        messages = data.get("value", [])
        if not messages:
            break

        for msg in messages:
            scanned += 1
            graph_id = msg.get("id", "")
            inet_msg_id = msg.get("internetMessageId", "")

            if graph_id in seen_ids:
                continue
            seen_ids.add(graph_id)

            if inet_msg_id and inet_msg_id in existing_msg_ids:
                skipped_existing += 1
                continue

            if not email_involves_targets(msg, domains, specific_emails):
                continue

            if tracker is not None and tracker.should_skip(mailbox, graph_id):
                skipped_known_failed += 1
                continue

            mime, mime_error = await graph.get_message_mime(mailbox, graph_id)
            if mime:
                results.append((msg, mime))
                matched += 1
            else:
                if tracker is not None:
                    tracker.record(
                        mailbox, graph_id,
                        subject=msg.get("subject"),
                        http_status=(mime_error or {}).get("status"),
                        error=(mime_error or {}).get("message") or "Graph /$value returned no bytes",
                        permanent=bool(mime_error and mime_error.get("permanent")),
                    )

            if matched % 50 == 0 and matched > 0:
                await asyncio.sleep(1)

        page_count += 1
        next_link = data.get("@odata.nextLink")
        if not next_link:
            break

        log.info(f"    Page {page_count}: {len(messages)} messages, {matched} matches so far...")

    log.info(
        f"    ✓ mailbox scan complete: {matched} new matching emails fetched"
        f" from {scanned} recent messages"
        + (f", {skipped_existing} already in DB" if skipped_existing else "")
        + (f", {skipped_known_failed} skipped (known MIME failures)" if skipped_known_failed else "")
    )

    return results


async def fetch_target_emails(
    graph: GraphClient,
    mailbox: str,
    domains: List[str],
    specific_emails: Optional[List[str]] = None,
    since: Optional[str] = None,
    existing_msg_ids: Set[str] = None,
) -> List[Tuple[dict, bytes]]:
    """
    Fetch all emails involving target domains or specific email addresses.

    Uses Graph API $search for each configured target, then verifies the
    message actually matches the configured domains / email addresses.
    """
    existing_msg_ids = existing_msg_ids or set()
    specific_emails = specific_emails or []

    if since:
        return await _fetch_recent_target_emails(
            graph=graph,
            mailbox=mailbox,
            domains=domains,
            specific_emails=specific_emails,
            since=since,
            existing_msg_ids=existing_msg_ids,
        )

    results: List[Tuple[dict, bytes]] = []  # (metadata, mime_bytes)
    seen_ids: Set[str] = set()

    search_targets: List[Tuple[str, str]] = []
    seen_search_terms: Set[Tuple[str, str]] = set()

    for domain in domains:
        key = ("domain", domain.lower().strip())
        if key[1] and key not in seen_search_terms:
            seen_search_terms.add(key)
            search_targets.append(("domain", domain.strip()))

    for email_addr in specific_emails:
        key = ("email_address", email_addr.lower().strip())
        if key[1] and key not in seen_search_terms:
            seen_search_terms.add(key)
            search_targets.append(("email_address", email_addr.strip()))

    for target_type, target_value in search_targets:
        search_value = target_value[2:] if target_type == "domain" and target_value.startswith("*.") else target_value

        if target_type == "domain":
            log.info(f"  📧 Searching {mailbox} for *@{target_value}...")
        else:
            log.info(f"  📧 Searching {mailbox} for {target_value}...")

        page_count = 0
        fetched_this_target = 0
        skipped_existing = 0
        skipped_known_failed = 0
        next_link = None
        tracker = _mime_tracker()

        while True:
            if next_link:
                # Follow @odata.nextLink for pagination
                data = await graph._get(next_link)
            else:
                # Search looks at from, to, cc, subject, and body
                data = await graph.list_messages(
                    mailbox=mailbox,
                    search=search_value,
                    top=250,
                    select="id,subject,from,toRecipients,ccRecipients,bccRecipients,receivedDateTime,bodyPreview,hasAttachments,internetMessageId",
                )

            if not data:
                break

            messages = data.get("value", [])
            if not messages:
                break

            for msg in messages:
                graph_id = msg.get("id", "")
                inet_msg_id = msg.get("internetMessageId", "")

                # Skip if we've seen this in another target search
                if graph_id in seen_ids:
                    continue
                seen_ids.add(graph_id)

                # Skip if already in DB
                if inet_msg_id and inet_msg_id in existing_msg_ids:
                    skipped_existing += 1
                    continue

                # Verify this email actually involves a target domain or specific address
                if not email_involves_targets(msg, domains, specific_emails):
                    continue

                # Apply date filter if specified
                if since:
                    recv_date = msg.get("receivedDateTime", "")[:10]
                    if recv_date and recv_date < since:
                        continue

                # Skip IDs that have already failed MIME fetch enough times
                if tracker is not None and tracker.should_skip(mailbox, graph_id):
                    skipped_known_failed += 1
                    continue

                # Download MIME content
                mime, mime_error = await graph.get_message_mime(mailbox, graph_id)
                if mime:
                    results.append((msg, mime))
                    fetched_this_target += 1
                else:
                    if tracker is not None:
                        tracker.record(
                            mailbox, graph_id,
                            subject=msg.get("subject"),
                            http_status=(mime_error or {}).get("status"),
                            error=(mime_error or {}).get("message") or "Graph /$value returned no bytes",
                            permanent=bool(mime_error and mime_error.get("permanent")),
                        )

                # Brief throttle to avoid hitting rate limits
                if fetched_this_target % 50 == 0 and fetched_this_target > 0:
                    await asyncio.sleep(1)

            # Check for next page
            next_link = data.get("@odata.nextLink")
            page_count += 1

            if not next_link:
                break

            log.info(f"    Page {page_count}: {len(messages)} messages, {fetched_this_target} new so far...")

        log.info(
            f"    ✓ {target_value}: {fetched_this_target} new emails fetched"
            + (f", {skipped_existing} already in DB" if skipped_existing else "")
            + (f", {skipped_known_failed} skipped (known MIME failures)" if skipped_known_failed else "")
        )

    return results


async def fetch_all_mailbox_emails(
    graph: GraphClient,
    mailbox: str,
    since: Optional[str] = None,
    existing_msg_ids: Optional[Set[str]] = None,
    max_pages: int = 50,
) -> List[Tuple[dict, bytes]]:
    """
    Full-mailbox fetch: ingest every message regardless of sender/domain.

    Used for mailboxes with ingest_strategy='full' (e.g. athena@), where the
    domain allowlist would otherwise drop legitimate correspondence from
    senders we haven't pre-registered. Walks /users/{mb}/messages newest-first
    with pagination, deduping against existing_msg_ids.
    """
    existing_msg_ids = existing_msg_ids or set()
    results: List[Tuple[dict, bytes]] = []
    seen_ids: Set[str] = set()

    log.info(f"  📧 Full-mailbox sync of {mailbox} (ingest_strategy=full)...")

    fetched = 0
    skipped_existing = 0
    skipped_known_failed = 0
    next_link: Optional[str] = None
    page_count = 0
    tracker = _mime_tracker()

    while page_count < max_pages:
        if next_link:
            data = await graph._get(next_link)
        else:
            data = await graph.list_messages(
                mailbox=mailbox,
                top=250,
                order_by="receivedDateTime desc",
                select="id,subject,from,toRecipients,ccRecipients,bccRecipients,receivedDateTime,bodyPreview,hasAttachments,internetMessageId",
            )

        if not data:
            break

        messages = data.get("value", [])
        if not messages:
            break

        for msg in messages:
            graph_id = msg.get("id", "")
            inet_msg_id = msg.get("internetMessageId", "")

            if graph_id in seen_ids:
                continue
            seen_ids.add(graph_id)

            if inet_msg_id and inet_msg_id in existing_msg_ids:
                skipped_existing += 1
                continue

            if since:
                recv_date = msg.get("receivedDateTime", "")[:10]
                if recv_date and recv_date < since:
                    continue

            if tracker is not None and tracker.should_skip(mailbox, graph_id):
                skipped_known_failed += 1
                continue

            mime, mime_error = await graph.get_message_mime(mailbox, graph_id)
            if mime:
                results.append((msg, mime))
                fetched += 1
            else:
                if tracker is not None:
                    tracker.record(
                        mailbox, graph_id,
                        subject=msg.get("subject"),
                        http_status=(mime_error or {}).get("status"),
                        error=(mime_error or {}).get("message") or "Graph /$value returned no bytes",
                        permanent=bool(mime_error and mime_error.get("permanent")),
                    )

            if fetched % 50 == 0 and fetched > 0:
                await asyncio.sleep(1)

        next_link = data.get("@odata.nextLink")
        page_count += 1

        if not next_link:
            break

        log.info(
            f"    Page {page_count}: {fetched} new, {skipped_existing} already in DB..."
        )

    capped = page_count >= max_pages and bool(next_link)
    log.info(
        f"  ✓ {mailbox}: {fetched} new emails fetched"
        + (f", {skipped_existing} already in DB" if skipped_existing else "")
        + (f", {skipped_known_failed} skipped (known MIME failures)" if skipped_known_failed else "")
        + (f" — page cap {max_pages} hit; more pages remain" if capped else "")
    )
    if capped:
        _record_run_issue(
            "warning",
            "Full-mailbox sync page cap hit",
            f"{mailbox}: walked {max_pages} pages × 250 msgs and more remained. "
            "Increase max_pages or run a one-off backfill.",
        )

    return results


async def fetch_domain_emails(
    graph: GraphClient,
    mailbox: str,
    domains: List[str],
    since: Optional[str] = None,
    existing_msg_ids: Set[str] = None,
) -> List[Tuple[dict, bytes]]:
    """Backward-compatible wrapper for older callers."""
    return await fetch_target_emails(
        graph,
        mailbox,
        domains,
        specific_emails=TARGET_SPECIFIC_EMAILS,
        since=since,
        existing_msg_ids=existing_msg_ids,
    )


async def process_and_ingest_email(
    pool: asyncpg.Pool,
    embedder: EmbeddingClient,
    chunker: TextChunker,
    msg_meta: dict,
    mime_data: bytes,
    mailbox: str,
    seen_message_ids: Set[str],
) -> Optional[IngestedDocument]:
    """Parse a single email, chunk, embed, and write to PostgreSQL."""
    parsed = parse_mime_bytes(mime_data)
    if not parsed or not parsed["full_text"].strip():
        return None

    h = parsed["headers"]
    msg_id = h["message_id"]

    # Dedup by Message-ID
    if msg_id and msg_id in seen_message_ids:
        return None
    if msg_id:
        seen_message_ids.add(msg_id)

    direction = determine_email_direction(h["from"], mailbox)

    graph_cc = ", ".join(
        f"{r.get('emailAddress', {}).get('name', '')} <{r.get('emailAddress', {}).get('address', '')}>".strip()
        for r in (msg_meta.get("ccRecipients") or [])
        if r.get("emailAddress", {}).get("address")
    )
    graph_bcc = ", ".join(
        f"{r.get('emailAddress', {}).get('name', '')} <{r.get('emailAddress', {}).get('address', '')}>".strip()
        for r in (msg_meta.get("bccRecipients") or [])
        if r.get("emailAddress", {}).get("address")
    )
    if graph_cc and f"Cc: {graph_cc}" not in parsed["full_text"]:
        parsed["full_text"] = parsed["full_text"].replace(
            f"Date: {h['date']}\n\n",
            f"Cc: {graph_cc}\nDate: {h['date']}\n\n",
        )
    if graph_bcc and f"Bcc: {graph_bcc}" not in parsed["full_text"]:
        parsed["full_text"] = parsed["full_text"].replace(
            f"Date: {h['date']}\n\n",
            f"Bcc: {graph_bcc}\nDate: {h['date']}\n\n",
        )

    case_nums = extract_case_numbers_from_text(parsed["full_text"][:5000])
    content_hash = hashlib.sha256(parsed["full_text"].encode()).hexdigest()[:16]

    # Filename from subject + date
    subject = h["subject"] or "No Subject"
    date_str = msg_meta.get("receivedDateTime", "")[:10]
    safe_subject = re.sub(r'[^\w\s-]', '', subject)[:60].strip()
    filename = f"{date_str} - {safe_subject}.eml"

    # Create a stable source_path based on mailbox + message_id.
    # The pipeline uses this as its idempotency key.
    source_path = f"graph://{mailbox}/{msg_id or msg_meta.get('id', '')}"

    local_eml_path = persist_graph_mime_copy(
        mailbox=mailbox,
        mime_data=mime_data,
        subject=subject,
        date_hint=date_str,
    )

    # Upload the raw .eml to MinIO. Gives us a durable archival copy + the
    # storage_uri we persist on the core.documents row so MCP downloads and
    # retention_sweep can find it later. FS copy above stays in place for
    # legacy reads until a backfill pass moves existing 2.3 GB into MinIO.
    eml_blob = blob_storage.upload_eml(
        mime_data,
        mailbox=mailbox,
        graph_message_id=msg_meta.get("id", "") or msg_id or "unknown",
    )
    if eml_blob is None:
        log.debug("  MinIO upload skipped for %s (not configured or failed)",
                  filename)

    # Pre-parse the date for consistency; the pipeline accepts either string
    # or datetime but pre-parsed avoids a second parse in the SMS hook below.
    date_sent_dt = _parse_email_date(h["date"])

    email_meta_dict = {
        "subject": subject,
        "message_id": msg_id,
        "in_reply_to": h["in_reply_to"],
        "thread_id": h.get("references", "").split()[0] if h.get("references") else msg_id,
        "sender": h["from"],
        "recipients": h["to"],
        "cc": h["cc"],
        "date_sent": date_sent_dt,
        "direction": direction,
        "mailbox": mailbox,
        "has_attachments": h.get("has_attachments", False),
    }

    metadata_dict = {
        "file_type": "eml",
        "mailbox": mailbox,
        "source": "graph_api",
        "graph_message_id": msg_meta.get("id", ""),
        "received_date": date_str,
        "local_eml_path": local_eml_path,
    }

    # Hand off to the unified ingestion pipeline. The pipeline owns:
    #   - chunker routing (email body → split_email_body)
    #   - dedup via source_path
    #   - Contextual Retrieval + embedding of body chunks only
    #   - core.documents / core.document_chunks / case_documents /
    #     email_metadata / document_tags writes in one transaction
    # Attachments still run through the domain-specific process_email_attachments
    # below because it also writes legal.email_attachments (not yet in pipeline).
    spec = DocumentSpec(
        domain="legal",
        document_type="email",
        source_path=source_path,
        title=subject,
        filename=filename,
        full_content=parsed["full_text"],
        content_hash=content_hash,
        metadata=metadata_dict,
        case_numbers=case_nums,
        email_meta=email_meta_dict,
        storage_backend=eml_blob.storage_backend if eml_blob else None,
        storage_uri=eml_blob.storage_uri if eml_blob else None,
        storage_sha256=eml_blob.storage_sha256 if eml_blob else None,
        storage_size_bytes=eml_blob.storage_size_bytes if eml_blob else None,
    )

    result = await pipeline_ingest(spec, pool=pool)

    if result.was_existing:
        # Pipeline dedup on source_path is the designed behavior — auto-handled,
        # not an alertable condition. Keep the log line for forensics; don't
        # surface to the run-summary email. (Mirrors attachment-dedup pattern
        # at process_email_attachments.)
        log.info(f"  Duplicate graph source_path already exists, skipping {source_path}")
        return None

    if result.error:
        log.error(f"  Ingest error for {filename}: {result.error}")
        _record_run_issue(
            "error",
            "Email ingest failed",
            f"filename: {filename}\nsource_path: {source_path}\nerror: {result.error}",
        )
        return None

    # Build a legacy IngestedDocument for downstream callers
    # (process_email_attachments needs doc_id; the rest is for logging).
    doc = IngestedDocument(
        doc_id=result.doc_id,
        domain="legal",
        source_path=source_path,
        filename=filename,
        document_type="email",
        title=subject,
        content_hash=content_hash,
        full_content=parsed["full_text"],
        metadata=metadata_dict,
        case_numbers=case_nums or [],
        email_meta=email_meta_dict,
    )

    # SMS notification hook — previously lived inside write_email_document's
    # transaction. Moved outside because queue_email_sms is already a
    # fire-and-forget (swallows its own exceptions), and keeping it separate
    # from the ingest transaction is cleaner.
    if direction == "inbound":
        try:
            from email_notify import queue_email_sms
            await queue_email_sms(
                pool,
                sender=email_meta_dict.get("sender", "") or "",
                subject=subject,
                received_at=date_sent_dt,
                importance=msg_meta.get("importance"),
                graph_id=msg_id,
                mailbox=mailbox,
                body_preview=parsed.get("full_text") or "",
            )
        except Exception as sms_e:
            log.warning(f"SMS queue failed for {email_meta_dict.get('sender','?')}: {sms_e}")

    # Process attachments inline
    att_list = parsed.get("attachments", [])
    if att_list:
        try:
            att_count = await process_email_attachments(
                pool, embedder, chunker,
                email_doc_id=doc.doc_id,
                email_source_path=source_path,
                email_subject=subject,
                attachments=att_list,
                mailbox=mailbox,
                email_date=date_str,
            )
            if att_count > 0:
                doc.metadata["attachments_processed"] = att_count
        except Exception as e:
            log.error(f"  Attachment processing error for {filename}: {e}")
            _record_run_issue(
                "error",
                "Attachment processing failed",
                f"filename: {filename}\nsource_path: {source_path}\nerror: {e}",
            )

    return doc


# ============================================================
# Main entry point
# ============================================================

async def main(
    mailboxes: List[str] = None,
    domains: List[str] = None,
    since: Optional[str] = None,
    dry_run: bool = False,
    audit_config: bool = False,
):
    # Connect to database
    log.info("Connecting to PostgreSQL...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # Load known-bad MIME graph IDs so we don't retry them forever.
    global _ACTIVE_MIME_TRACKER
    _ACTIVE_MIME_TRACKER = MimeFailureTracker()
    await _ACTIVE_MIME_TRACKER.load(pool)

    # Read sync config from database (if management tables exist)
    db_mailboxes = []
    db_rules = []
    mailbox_strategies: Dict[str, str] = {}
    try:
        db_mbs = await pool.fetch(
            "SELECT email, ingest_strategy FROM ops.sync_mailboxes "
            "WHERE is_active = true ORDER BY id"
        )
        db_mailboxes = [r["email"] for r in db_mbs]
        for r in db_mbs:
            strategy = (r["ingest_strategy"] or "filtered").strip().lower()
            mailbox_strategies[r["email"].strip().lower()] = strategy
        db_rules = await pool.fetch(
            "SELECT pattern, rule_type FROM ops.sync_rules WHERE is_active = true "
            "AND rule_type IN ('domain', 'email_address') ORDER BY priority DESC"
        )
    except Exception:
        log.info("  Using default mailboxes and domains (DB config tables not found)")

    config = resolve_sync_configuration(
        default_mailboxes=MAILBOXES,
        default_domains=TARGET_DOMAINS,
        default_specific_emails=TARGET_SPECIFIC_EMAILS,
        cli_mailboxes=mailboxes,
        cli_domains=domains,
        db_mailboxes=db_mailboxes,
        db_rules=db_rules,
    )
    mailboxes = config.mailboxes
    domains = config.domains
    specific_emails = config.specific_emails

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  ACP Graph API Email Sync                               ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  Mailboxes:  {f'{len(mailboxes)} configured':<43}║")
    log.info(f"║  Domains:    {f'{len(domains)} configured':<43}║")
    log.info(f"║  Since:      {since or 'all time':<43}║")
    log.info(f"║  Dry run:    {str(dry_run):<43}║")
    log.info(f"║  Audit only: {str(audit_config):<43}║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("")
    log.info(f"  📫 Effective mailboxes: {', '.join(mailboxes)}")
    log.info(f"  🌐 Effective domains ({len(domains)}): {', '.join(domains)}")

    if config.db_mailboxes:
        if config.explicit_mailboxes:
            log.info(f"  📋 Explicit --mailbox override: using {mailboxes} (DB config ignored)")
        else:
            log.info(f"  📋 Loaded {len(config.db_mailboxes)} mailboxes from DB config")

    if config.db_domains:
        if config.explicit_domains:
            log.info(f"  📋 Explicit --domains override: using {domains} (DB config ignored)")
        else:
            log.info(f"  📋 Loaded {len(config.db_domains)} domain rules from DB config")

    if config.db_specific_emails:
        log.info(f"  📋 Loaded {len(config.db_specific_emails)} specific email rules from DB config")

    if config.code_only_domains:
        log.warning(
            "  ⚠ Active DB sync_rules is missing %d code default domains: %s",
            len(config.code_only_domains),
            ", ".join(config.code_only_domains),
        )
    if config.db_only_domains:
        log.warning(
            "  ⚠ Active DB sync_rules includes %d domains not present in code defaults: %s",
            len(config.db_only_domains),
            ", ".join(config.db_only_domains),
        )
    if config.db_domains and not config.code_only_domains and not config.db_only_domains:
        log.info("  ✅ Code defaults and active DB domain rules are in sync")

    if audit_config:
        log.info("Configuration audit complete — not contacting Graph API or ingesting emails.")
        await _finalize_sync_pool(pool)
        return 0

    # Get existing message IDs for dedup
    existing_msg_ids = await get_existing_message_ids(pool)
    log.info(f"Found {len(existing_msg_ids)} existing emails in DB (will skip)")

    # Get current counts
    doc_count = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    chunk_count = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")
    log.info(f"DB has {doc_count} documents, {chunk_count} chunks")

    # Authenticate with Graph API
    graph = GraphClient()
    if not await graph.authenticate():
        log.error("Cannot continue without Graph API authentication")
        _record_run_issue("error", "Email sync aborted", "Graph API authentication failed before mailbox scan.")
        await _finalize_sync_pool(pool)
        return

    # Test access to each mailbox
    for mb in mailboxes:
        user = await graph.get_user_info(mb)
        if user:
            log.info(f"✓ Access confirmed: {user.get('displayName', mb)} ({mb})")
        else:
            log.error(f"✗ Cannot access mailbox: {mb}")
            _record_run_issue("error", "Mailbox access failed", mb)
            await graph.close()
            await _finalize_sync_pool(pool)
            return

    # Fetch emails from all mailboxes
    all_emails: List[Tuple[dict, bytes, str]] = []  # (metadata, mime, mailbox)

    for mb in mailboxes:
        log.info("")
        log.info(f"{'='*60}")
        log.info(f"  Fetching from {mb}")
        log.info(f"{'='*60}")

        strategy = mailbox_strategies.get(mb.strip().lower(), "filtered")
        log.info(f"  Strategy: {strategy}")

        if strategy == "full":
            emails = await fetch_all_mailbox_emails(
                graph, mb,
                since=since,
                existing_msg_ids=existing_msg_ids,
            )
        else:
            emails = await fetch_target_emails(
                graph, mb, domains,
                specific_emails=specific_emails,
                since=since,
                existing_msg_ids=existing_msg_ids,
            )

        for meta, mime in emails:
            all_emails.append((meta, mime, mb))

        log.info(f"  Total from {mb}: {len(emails)} new emails")

    log.info("")
    log.info(f"Total emails to ingest: {len(all_emails)}")

    if dry_run:
        log.info("DRY RUN — not writing to database")
        # Show sample
        for meta, mime, mb in all_emails[:20]:
            subj = meta.get("subject", "?")[:60]
            date = meta.get("receivedDateTime", "")[:10]
            from_addr = meta.get("from", {}).get("emailAddress", {}).get("address", "?")
            log.info(f"  {date} | {from_addr:<35} | {subj}")
        if len(all_emails) > 20:
            log.info(f"  ... and {len(all_emails) - 20} more")
        await graph.close()
        await _finalize_sync_pool(pool)
        return

    if not all_emails:
        log.info("No new emails to ingest")
        # Record the sync even when empty for auditability
        try:
            await pool.execute("""
                INSERT INTO ops.sync_runs
                    (trigger_type, mailboxes_scanned, emails_found,
                     completed_at, status, emails_ingested, emails_skipped,
                     emails_failed, duration_seconds)
                VALUES ('timer', $1, 0, now(), 'completed', 0, 0, 0, 0)
            """, mailboxes)
        except Exception as e:
            log.error(f"AUDIT FAILURE - sync_runs INSERT (empty run): {e}")
            _record_run_issue("warning", "Audit insert failed", f"empty sync run insert\n{e}")
        # Always update mailbox last-checked timestamps
        try:
            for mb in mailboxes:
                await pool.execute(
                    "UPDATE ops.sync_mailboxes SET last_sync_at = now() WHERE email = $1",
                    mb)
        except Exception as e:
            log.error(f"AUDIT FAILURE - mailbox timestamp update: {e}")
            _record_run_issue("warning", "Audit mailbox timestamp update failed", str(e))
        await graph.close()
        await _finalize_sync_pool(pool)
        return

    # Record sync run in database
    sync_run_id = None
    try:
        sync_run_id = await pool.fetchval("""
            INSERT INTO ops.sync_runs
                (trigger_type, mailboxes_scanned, emails_found)
            VALUES ('api', $1, $2) RETURNING id
        """, mailboxes, len(all_emails))
    except Exception as e:
        log.error(f"AUDIT FAILURE: {e}")
        _record_run_issue("warning", "Audit insert failed", str(e))

    # Ingest emails
    log.info("")
    log.info("────────────────────────────────────────────────────────────")
    log.info(f"  Ingesting {len(all_emails)} emails → PostgreSQL")
    log.info("────────────────────────────────────────────────────────────")

    embedder = EmbeddingClient()
    chunker = TextChunker()
    seen_message_ids: Set[str] = set(existing_msg_ids)

    stats = {"ok": 0, "skipped": 0, "errors": 0, "chunks": 0, "attachments": 0}
    start_time = time.time()

    for i, (meta, mime, mb) in enumerate(all_emails, 1):
        subj = (meta.get("subject") or "?")[:50]
        date = (meta.get("receivedDateTime") or "")[:10]

        try:
            doc = await process_and_ingest_email(
                pool, embedder, chunker, meta, mime, mb, seen_message_ids,
            )
            if doc:
                stats["ok"] += 1
                stats["chunks"] += len(doc.chunks)
                att_count = doc.metadata.get("attachments_processed", 0)
                stats["attachments"] += att_count
                icon = "✓"
                detail = f"→ {len(doc.chunks)} chunks"
                if att_count:
                    detail += f" + {att_count} attachments"
            else:
                stats["skipped"] += 1
                icon = "⊘"
                detail = "skipped"
        except Exception as e:
            stats["errors"] += 1
            icon = "✗"
            detail = str(e)[:60]
            log.error(f"  Error processing email: {e}")
            _record_run_issue("error", "Email processing loop failure", str(e))

        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        eta = (len(all_emails) - i) / rate if rate > 0 else 0
        eta_str = f"{eta:.0f}s" if eta < 60 else f"{eta/60:.1f}m"

        log.info(
            f"  [{i}/{len(all_emails)} {i*100//len(all_emails)}%] "
            f"{icon} {date} {subj} {detail}  [{rate:.1f}/s, ETA {eta_str}]"
        )

        if i % 20 == 0:
            log.info(
                f"  ── Progress: {stats['ok']} ok / {stats['skipped']} skip / "
                f"{stats['errors']} err / {stats['chunks']} chunks"
            )

    # Final summary
    elapsed = time.time() - start_time
    final_docs = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    final_chunks = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")
    email_count = await pool.fetchval(
        "SELECT COUNT(*) FROM legal.email_metadata WHERE mailbox = ANY($1::text[])",
        mailboxes,
    )

    att_db_count = await pool.fetchval(
        "SELECT COUNT(*) FROM legal.email_attachments WHERE email_doc_id IN "
        "(SELECT id FROM core.documents WHERE source_path LIKE 'graph://%')"
    )

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  EMAIL SYNC COMPLETE                                     ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  New emails:      {stats['ok']:<38}║")
    log.info(f"║  Attachments:     {stats['attachments']:<38}║")
    log.info(f"║  Skipped:         {stats['skipped']:<38}║")
    log.info(f"║  Errors:          {stats['errors']:<38}║")
    log.info(f"║  New chunks:      {stats['chunks']:<38}║")
    log.info(f"║  Time:            {elapsed:.1f}s{' '*(36-len(f'{elapsed:.1f}s'))}║")
    log.info(f"║  Embed tokens:    {embedder.total_tokens:<38}║")
    log.info(f"║  Est. cost:       ${embedder.total_tokens * 0.00013 / 1000:.4f}{' '*(35-len(f'${embedder.total_tokens * 0.00013 / 1000:.4f}'))}║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  DB total docs:   {final_docs:<38}║")
    log.info(f"║  DB total chunks: {final_chunks:<38}║")
    log.info(f"║  Mailbox emails:  {email_count:<38}║")
    log.info(f"║  Attachments (DB):{att_db_count:<38}║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    # Finalize sync run record
    if sync_run_id:
        try:
            await pool.execute("""
                UPDATE ops.sync_runs SET
                    completed_at = now(), status = 'completed',
                    emails_ingested = $2, emails_skipped = $3,
                    emails_failed = $4, chunks_created = $5,
                    embedding_tokens = $6, duration_seconds = $7
                WHERE id = $1
            """, sync_run_id, stats["ok"], stats["skipped"],
                stats["errors"], stats["chunks"],
                embedder.total_tokens, elapsed)
        except Exception as e:
            log.error(f"AUDIT FAILURE - sync_runs UPDATE: {e}")
            _record_run_issue("warning", "Audit sync_run update failed", str(e))

    # Update mailbox sync timestamps
    try:
        for mb in mailboxes:
            mb_count = sum(1 for _, _, m in all_emails if m == mb)
            if mb_count > 0:
                await pool.execute("""
                    UPDATE ops.sync_mailboxes SET
                        last_sync_at = now(),
                        total_synced = total_synced + $2
                    WHERE email = $1
                """, mb, mb_count)
            else:
                await pool.execute(
                    "UPDATE ops.sync_mailboxes SET last_sync_at = now() WHERE email = $1",
                    mb)
    except Exception as e:
        log.error(f"AUDIT FAILURE: {e}")
        _record_run_issue("warning", "Audit mailbox update failed", str(e))

    await embedder.close()
    await graph.close()
    await _finalize_sync_pool(pool)


# ============================================================
# Backfill attachments for existing Graph API emails
# ============================================================

async def _load_backfill_mime(
    row: asyncpg.Record,
    graph: Optional[GraphClient],
) -> Tuple[Optional[bytes], Optional[str]]:
    """Load MIME bytes for an existing email document during attachment backfill."""
    source_path = row["source_path"] or ""
    subject = row["subject"] or "No Subject"
    metadata = row["metadata"] or {}

    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except Exception:
            metadata = {}

    local_eml_path = metadata.get("local_eml_path") if isinstance(metadata, dict) else None
    if local_eml_path:
        local_path = Path(local_eml_path)
        if local_path.exists():
            try:
                return local_path.read_bytes(), None
            except Exception as e:
                return None, f"Failed reading local MIME file from metadata: {e}"

    if source_path.startswith("graph://"):
        if graph is None:
            return None, "Graph client unavailable"

        # Format: graph://mailbox/message_id (internet message-id, not Graph internal id)
        parts = source_path.replace("graph://", "").split("/", 1)
        if len(parts) != 2:
            return None, f"Bad graph source_path: {source_path}"

        mb, inet_msg_id = parts
        search_result = await graph._get(
            f"{GRAPH_BASE_URL}/users/{mb}/messages",
            params={
                "$filter": f"internetMessageId eq '{inet_msg_id}'",
                "$select": "id,hasAttachments",
                "$top": "1",
            },
        )

        if not search_result or not search_result.get("value"):
            return None, f"Cannot find message in Graph API: {subject[:50]}"

        graph_id = search_result["value"][0]["id"]
        mime_data, mime_err = await graph.get_message_mime(mb, graph_id)
        if not mime_data:
            err_detail = (mime_err or {}).get("message", "unknown") if mime_err else "empty response"
            return None, f"MIME download failed ({err_detail}): {subject[:50]}"
        return mime_data, None

    local_path = Path(source_path)
    if not local_path.exists():
        return None, f"Local MIME file not found: {source_path}"

    try:
        return local_path.read_bytes(), None
    except Exception as e:
        return None, f"Failed reading local MIME file: {e}"

async def backfill_attachments(
    mailboxes: List[str] = None,
    limit: int = 0,
):
    """
    Load MIME data for existing emails that have has_attachments=true but no
    complete records in legal.email_attachments, then extract & process attachments.
    """
    mailboxes = mailboxes or MAILBOXES

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  ATTACHMENT BACKFILL — Existing Email Records            ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  Mailboxes: {', '.join(mailboxes):<44}║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("")

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # Find existing emails with has_attachments but no attachment records
    rows = await pool.fetch("""
         SELECT d.id AS doc_id, d.source_path, d.metadata, em.subject, em.mailbox,
             em.message_id, em.date_sent
        FROM legal.email_metadata em
        JOIN core.documents d ON d.id = em.document_id
        WHERE em.has_attachments = true
          AND d.document_type = 'email'
          AND (
              NOT EXISTS (
                  SELECT 1 FROM legal.email_attachments ea
                  WHERE ea.email_doc_id = d.id
              )
              OR EXISTS (
                  SELECT 1 FROM legal.email_attachments ea
                  WHERE ea.email_doc_id = d.id
                    AND ea.child_doc_id IS NULL
              )
          )
        ORDER BY em.date_sent DESC
    """)

    if not rows:
        log.info("No emails need attachment backfill — all caught up.")
        await _finalize_sync_pool(pool)
        return

    total = len(rows)
    if limit > 0:
        rows = rows[:limit]
        log.info(f"Processing {len(rows)} of {total} emails (--limit {limit})")
    else:
        log.info(f"Processing {total} emails with missing attachments")

    graph = None
    if any((row["source_path"] or "").startswith("graph://") for row in rows):
        graph = GraphClient()
        if not await graph.authenticate():
            log.error("Cannot continue without Graph API authentication")
            _record_run_issue("error", "Attachment backfill aborted", "Graph API authentication failed before MIME backfill.")
            await _finalize_sync_pool(pool)
            return

    embedder = EmbeddingClient()
    chunker = TextChunker()

    stats = {"processed": 0, "attachments": 0, "errors": 0, "no_attachments": 0}
    start_time = time.time()

    for i, row in enumerate(rows, 1):
        doc_id = str(row["doc_id"])
        source_path = row["source_path"]
        subject = row["subject"] or "No Subject"
        metadata = row["metadata"] or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}
        mime_data, load_error = await _load_backfill_mime(row, graph)
        if not mime_data:
            log.warning(f"  ⊘ {load_error}")
            stats["errors"] += 1
            _record_run_issue(
                "warning",
                "Attachment backfill MIME load failed",
                f"subject: {subject[:200]}\nsource_path: {source_path}\nerror: {load_error}",
            )
            continue

        if source_path.startswith("graph://") and not metadata.get("local_eml_path"):
            local_eml_path = persist_graph_mime_copy(
                mailbox=row["mailbox"],
                mime_data=mime_data,
                subject=subject,
                date_hint=(row["date_sent"].isoformat()[:10] if row["date_sent"] else None),
            )
            if local_eml_path:
                metadata["local_eml_path"] = local_eml_path
                await pool.execute(
                    "UPDATE core.documents SET metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb WHERE id = $1::uuid",
                    uuid.UUID(doc_id),
                    json.dumps({"local_eml_path": local_eml_path}),
                )

        # Parse MIME to extract attachments
        parsed = parse_mime_bytes(mime_data)
        if not parsed:
            stats["errors"] += 1
            continue

        att_list = parsed.get("attachments", [])
        if not att_list:
            log.info(f"  [{i}/{len(rows)}] ⊘ {subject[:50]} — no extractable attachments in MIME")
            stats["no_attachments"] += 1
            continue

        existing_attachment_rows = await pool.fetch("""
            SELECT id, filename, content_type, file_size, child_doc_id, is_processed
            FROM legal.email_attachments
            WHERE email_doc_id = $1
            ORDER BY created_at
        """, uuid.UUID(doc_id))

        # Process attachments
        try:
            att_count = await process_email_attachments(
                pool, embedder, chunker,
                email_doc_id=doc_id,
                email_source_path=source_path,
                email_subject=subject,
                attachments=att_list,
                existing_attachment_rows=[dict(r) for r in existing_attachment_rows],
                mailbox=row["mailbox"],
                email_date=(row["date_sent"].isoformat()[:10] if row["date_sent"] else None),
            )
            stats["processed"] += 1
            stats["attachments"] += att_count
            log.info(
                f"  [{i}/{len(rows)}] ✓ {subject[:50]} — {att_count} attachments"
            )
        except Exception as e:
            log.error(f"  [{i}/{len(rows)}] ✗ {subject[:50]}: {e}")
            stats["errors"] += 1
            _record_run_issue(
                "error",
                "Attachment backfill processing failed",
                f"subject: {subject[:200]}\nsource_path: {source_path}\nerror: {e}",
            )

        # Throttle to avoid rate limits
        if i % 10 == 0:
            await asyncio.sleep(1)

    elapsed = time.time() - start_time
    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  ATTACHMENT BACKFILL COMPLETE                            ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  Emails processed:  {stats['processed']:<36}║")
    log.info(f"║  Attachments:       {stats['attachments']:<36}║")
    log.info(f"║  No attachments:    {stats['no_attachments']:<36}║")
    log.info(f"║  Errors:            {stats['errors']:<36}║")
    log.info(f"║  Time:              {elapsed:.1f}s{' '*(34-len(f'{elapsed:.1f}s'))}║")
    log.info(f"║  Embed tokens:      {embedder.total_tokens:<36}║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    await embedder.close()
    if graph:
        await graph.close()
    await _finalize_sync_pool(pool)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ACP Graph API Email Sync")
    parser.add_argument(
        "--mailbox", "-m", action="append",
        help="Mailbox to scan (can specify multiple). Default: both william@ and athena@"
    )
    parser.add_argument(
        "--domains", "-d", nargs="+",
        default=None,
        help="Target domains to filter for (overrides DB sync_rules config)"
    )
    parser.add_argument(
        "--since", "-s",
        help="Only fetch emails after this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be fetched without writing to DB"
    )
    parser.add_argument(
        "--audit-config", action="store_true",
        help="Show the resolved mailbox/domain config and drift warnings, then exit"
    )
    parser.add_argument(
        "--backfill-attachments", action="store_true",
        help="Load MIME and process attachments for existing emails with missing attachment records"
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Limit number of emails to process (for backfill, 0=all)"
    )

    args = parser.parse_args()

    if args.backfill_attachments:
        exit_code = asyncio.run(_run_with_error_alerts(
            "Email attachment backfill",
            backfill_attachments(
                mailboxes=args.mailbox,
                limit=args.limit,
            ),
        ))
    else:
        exit_code = asyncio.run(_run_with_error_alerts(
            "Email sync",
            main(
                mailboxes=args.mailbox,
                domains=args.domains,
                since=args.since,
                dry_run=args.dry_run,
                audit_config=args.audit_config,
            ),
        ))

    raise SystemExit(exit_code or 0)
