#!/usr/bin/env python3
"""
WDWS Enterprise Ingest Pipeline v1.0

Fresh ingest from source files → OCR → chunk → embed → PostgreSQL.
Runs on the datacenter server against /opt/wdws/data/.

Domains:
  legal   → cases/ (PDFs, DOCX) + emails/ (.eml files)
  medical → medical/records/ (CCDA XML, PDFs, images)

Usage:
    python ingest.py --domain all
    python ingest.py --domain legal --skip-emails
    python ingest.py --domain medical
    python ingest.py --domain legal --dry-run
"""

import argparse
import asyncio
from concurrent.futures import ProcessPoolExecutor, as_completed
import email as email_lib
from email import policy
import hashlib
import json
import logging
import multiprocessing
import os
import re
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple

import asyncpg
import httpx
import magic
import pytesseract
from pdf2image import convert_from_path, pdfinfo_from_path
from PIL import Image

# OCR worker count — each worker spawns pdftoppm + tesseract subprocesses,
# so real CPU usage = workers × 2-3. Keep this conservative to avoid
# overloading the system (e.g., 8 workers × 3 subprocs = 24 on 32 cores).
OCR_WORKERS = min(8, max(1, multiprocessing.cpu_count() // 4))

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
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 3072

DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/wdws/data"))
CASES_DIR   = DATA_DIR / "cases"
EMAILS_DIR  = DATA_DIR / "emails"
INBOX_DIR   = DATA_DIR / "inbox"
MEDICAL_DIR = DATA_DIR / "medical" / "records"

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
OCR_DPI = 150
EMBEDDING_BATCH_SIZE = 100   # OpenAI supports up to 2048 per request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("ingest")


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
    """A fully processed source file ready for PostgreSQL."""
    doc_id: str                      # UUID
    domain: str
    source_path: str
    filename: str
    document_type: str
    title: str
    content_hash: str
    full_content: str
    metadata: Dict
    chunks: List[DocumentChunk] = field(default_factory=list)

    # Legal enrichment
    case_numbers: List[str] = field(default_factory=list)
    email_meta: Optional[Dict] = None

    # Medical enrichment
    patient_name: str = ""
    provider: str = ""
    facility: str = ""
    date_of_service: Optional[str] = None
    record_type: str = ""


# ============================================================
# Text chunker
# ============================================================

class TextChunker:
    """Recursive character text splitting — same algorithm as LangChain."""

    def __init__(self, chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = ["\n\n", "\n", ". ", " ", ""]

    def split(self, text: str) -> List[str]:
        if not text:
            return []
        return self._split_text(text, self.separators)

    def _split_text(self, text: str, separators: list) -> list:
        final_chunks = []
        separator = separators[-1]
        new_separators = []

        for i, sep in enumerate(separators):
            if sep == "" or sep in text:
                separator = sep
                new_separators = separators[i + 1:]
                break

        splits = text.split(separator) if separator else list(text)
        good_splits = []
        sep_len = len(separator)

        for s in splits:
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    merged = self._merge_splits(good_splits, separator)
                    final_chunks.extend(merged)
                    good_splits = []
                if new_separators:
                    final_chunks.extend(self._split_text(s, new_separators))
                else:
                    final_chunks.append(s)

        if good_splits:
            merged = self._merge_splits(good_splits, separator)
            final_chunks.extend(merged)

        return final_chunks

    def _merge_splits(self, splits: list, separator: str) -> list:
        docs = []
        current = []
        total = 0

        for s in splits:
            s_len = len(s)
            if total + s_len + (len(separator) if current else 0) > self.chunk_size:
                if current:
                    docs.append(separator.join(current))
                    while total > self.chunk_overlap and len(current) > 1:
                        total -= len(current[0]) + len(separator)
                        current.pop(0)
                current = [s]
                total = s_len
            else:
                current.append(s)
                total += s_len + (len(separator) if len(current) > 1 else 0)

        if current:
            docs.append(separator.join(current))
        return docs


# ============================================================
# OCR & text extraction
# ============================================================

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF using OCR — page-by-page to limit memory."""
    try:
        # Get page count first
        try:
            info = pdfinfo_from_path(str(pdf_path))
            num_pages = info.get("Pages", 0)
        except Exception:
            num_pages = 0  # will fall through to single-call

        if num_pages > 0:
            # Process one page at a time — keeps memory bounded
            parts = []
            for page_num in range(1, num_pages + 1):
                try:
                    images = convert_from_path(
                        str(pdf_path), dpi=OCR_DPI,
                        first_page=page_num, last_page=page_num,
                        thread_count=1
                    )
                    if images:
                        page_text = pytesseract.image_to_string(images[0])
                        if page_text and page_text.strip():
                            parts.append(f"[Page {page_num}]\n{page_text}")
                        # Free memory immediately
                        del images
                except Exception as e:
                    log.debug(f"  OCR page {page_num}/{num_pages} failed for {pdf_path.name}: {e}")
            return "\n\n".join(parts)
        else:
            # Fallback: load all at once (small files or no pdfinfo)
            images = convert_from_path(str(pdf_path), dpi=OCR_DPI, thread_count=1)
            parts = []
            for page_num, img in enumerate(images, 1):
                page_text = pytesseract.image_to_string(img)
                if page_text and page_text.strip():
                    parts.append(f"[Page {page_num}]\n{page_text}")
            return "\n\n".join(parts)
    except Exception as e:
        log.warning(f"OCR failed for {pdf_path.name}: {e}, trying pdftotext")
        try:
            import subprocess
            result = subprocess.run(
                ["pdftotext", "-layout", str(pdf_path), "-"],
                capture_output=True, text=True, timeout=30
            )
            return result.stdout
        except Exception as e2:
            log.error(f"All extraction failed for {pdf_path.name}: {e2}")
            return ""


def extract_image_text(image_path: Path) -> str:
    """Extract text from image using OCR."""
    try:
        img = Image.open(image_path)
        return pytesseract.image_to_string(img)
    except Exception as e:
        log.error(f"Image OCR failed for {image_path.name}: {e}")
        return ""


def extract_docx_text(docx_path: Path) -> str:
    """Extract text from DOCX file."""
    try:
        import zipfile
        with zipfile.ZipFile(docx_path) as z:
            with z.open("word/document.xml") as f:
                tree = ET.parse(f)
                root = tree.getroot()
                ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                paragraphs = root.findall(".//w:p", ns)
                texts = []
                for p in paragraphs:
                    runs = p.findall(".//w:t", ns)
                    line = "".join(r.text or "" for r in runs)
                    if line:
                        texts.append(line)
                return "\n".join(texts)
    except Exception as e:
        log.error(f"DOCX extraction failed for {docx_path.name}: {e}")
        return ""


# ============================================================
# Case number extraction
# ============================================================

# State case format: 24-2-01031-31, 26-2-00762-31
STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
# Federal: 3:25-cv-05551-DGE, 325cv05551dge, 2:26-cv-00337-KKE
FEDERAL_CASE_RE = re.compile(
    r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?', re.IGNORECASE
)

KNOWN_CASE_MAP = {
    # directory name → (case_number, court, case_title, jurisdiction)
    "19-2-03310-31 - FTCU v Nelson": ("19-2-03310-31", "Snohomish County Superior Court", "FTCU v Nelson", "state"),
    "226cv00337kke": ("226CV00337KKE", "US District Court WDWA", "Nelson v Silver Lake et al", "federal"),
    "23-2-07759-31 - Nelson v Prisma Wellness": ("23-2-07759-31", "Snohomish County Superior Court", "Nelson v Prisma Wellness", "state"),
    "23-2-09435-31 - Nelson v Unum Group": ("23-2-09435-31", "Snohomish County Superior Court", "Nelson v Unum Group", "state"),
    "24-2-01031-31 - Nelson v Starbucks": ("24-2-01031-31", "Snohomish County Superior Court", "Nelson v Starbucks", "state"),
    "24-2-01032-31 - Nelson v Olivia Kuker": ("24-2-01032-31", "Snohomish County Superior Court", "Nelson v Olivia Kuker", "state"),
    "24-2-01033-31 - Nelson v Stuart Gibson": ("24-2-01033-31", "Snohomish County Superior Court", "Nelson v Stuart Gibson", "state"),
    "24-2-01034-31 - Nelson v Brian Baseggio": ("24-2-01034-31", "Snohomish County Superior Court", "Nelson v Brian Baseggio", "state"),
    "24-2-07413-31 - Nelson v Trevor Haward et al": ("24-2-07413-31", "Snohomish County Superior Court", "Nelson v Trevor Haward et al", "state"),
    "25-2-00238-31 - Nelson vs FTCU et al": ("25-2-00238-31", "Snohomish County Superior Court", "Nelson vs FTCU et al", "state"),
    "26-2-00762-31 - IAFF v SSCFR Nelson": ("26-2-00762-31", "Snohomish County Superior Court", "IAFF v SSCFR Nelson", "state"),
    "325cv05551dge": ("325CV05551DGE", "US District Court WDWA", "Nelson v BIIA et al", "federal"),
}


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


def case_number_from_path(file_path: Path) -> Tuple[List[str], Optional[str]]:
    """Get case numbers from parent directory name."""
    for parent in file_path.parents:
        dir_name = parent.name
        if dir_name in KNOWN_CASE_MAP:
            info = KNOWN_CASE_MAP[dir_name]
            return [info[0]], dir_name
        # Try pattern matching on directory name
        m = STATE_CASE_RE.search(dir_name)
        if m:
            return [m.group(0)], dir_name
        m = FEDERAL_CASE_RE.search(dir_name)
        if m:
            court, year, seq, judge = m.groups()
            normalized = f"{court}{year}CV{seq}"
            if judge:
                normalized += judge.upper()
            return [normalized.upper()], dir_name
    return [], None


# ============================================================
# Email parsing
# ============================================================

def parse_eml(eml_path: Path) -> Optional[Dict]:
    """Parse an .eml file and extract content + metadata."""
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
            "in_reply_to": str(msg.get("In-Reply-To", "")).strip(),
            "references": str(msg.get("References", "")).strip(),
        }

        body = ""
        has_attachments = False
        if msg.is_multipart():
            for part in msg.walk():
                disp = part.get("Content-Disposition", "")
                if "attachment" in disp.lower() or (part.get_filename() and part.get_filename()):
                    has_attachments = True
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

        # Build full searchable text
        full_text = (
            f"Subject: {headers['subject']}\n"
            f"From: {headers['from']}\n"
            f"To: {headers['to']}\n"
            f"Date: {headers['date']}\n\n"
            f"{body}"
        )

        return {"headers": headers, "body": body, "full_text": full_text}
    except Exception as e:
        log.error(f"Failed to parse {eml_path.name}: {e}")
        return None


def determine_email_direction(from_addr: str, mailbox: str) -> str:
    """Determine if email is inbound or outbound."""
    from_lower = from_addr.lower()
    if mailbox:
        mailbox_email = mailbox.replace("_", "@")
        if mailbox_email in from_lower:
            return "outbound"
    return "inbound"


# ============================================================
# Medical metadata extraction
# ============================================================

CCDA_NS = {"cda": "urn:hl7-org:v3", "sdtc": "urn:hl7-org:sdtc"}

MEDICAL_CATEGORY_PATTERNS = {
    "lab_results": [r"laboratory\s*result", r"lab\s*report", r"blood\s*test",
                    r"metabolic\s*panel", r"CBC", r"lipid\s*panel", r"urinalysis"],
    "imaging": [r"x-?ray", r"radiolog", r"MRI", r"CT\s*scan", r"ultrasound",
                r"mammogram", r"IMPRESSION:"],
    "clinical_notes": [r"office\s*visit", r"chief\s*complaint",
                       r"history\s*of\s*present", r"SOAP\s*note", r"assessment\s*and\s*plan"],
    "discharge_summary": [r"discharge\s*summar", r"discharge\s*instruction"],
    "operative_report": [r"operative\s*report", r"surgical\s*note", r"procedure\s*report"],
    "prescription": [r"prescription", r"medication\s*list", r"refill"],
    "immunization": [r"immunization", r"vaccine", r"vaccination"],
    "pathology": [r"pathology", r"biopsy", r"histolog"],
}


def auto_categorize_medical(text: str, filename: str = "") -> str:
    """Auto-categorize medical document based on content patterns."""
    combined = (text[:3000] + " " + filename).lower()
    for category, patterns in MEDICAL_CATEGORY_PATTERNS.items():
        for p in patterns:
            if re.search(p, combined, re.IGNORECASE):
                return category
    return "other"


def extract_provider_name(text: str) -> Optional[str]:
    patterns = [
        r"(?:Provider|Physician|Doctor|Dr\.?)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+),?\s*(?:MD|DO|NP|PA|RN)",
        r"(?:Prepared by|Signed by)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            return m.group(1).strip()
    return None


def extract_facility_name(text: str) -> Optional[str]:
    patterns = [
        r"(?:Hospital|Medical Center|Clinic|Healthcare)[:\s]*([^\n]+)",
        r"^([A-Z][A-Za-z\s]+(?:Hospital|Medical Center|Clinic|Healthcare|Health System))",
    ]
    for p in patterns:
        m = re.search(p, text, re.MULTILINE)
        if m:
            return m.group(1).strip()[:100]
    return None


def extract_date_from_text(text: str, filename: str = "") -> Optional[str]:
    """Extract a date from text or filename → YYYY-MM-DD."""
    patterns = [
        (r"(\d{4})-(\d{2})-(\d{2})", "ymd"),
        (r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", "mdy"),
    ]
    for source in [filename, text[:2000]]:
        for pat, fmt in patterns:
            m = re.search(pat, source)
            if m:
                g = m.groups()
                if fmt == "ymd":
                    return f"{g[0]}-{g[1]}-{g[2]}"
                elif fmt == "mdy":
                    return f"{g[2]}-{g[0].zfill(2)}-{g[1].zfill(2)}"
    return None


def parse_ccda(xml_path: Path) -> Optional[Dict]:
    """Parse CCDA XML and return structured content."""
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Patient name
        patient_name = ""
        patient = root.find(".//cda:patient/cda:name", CCDA_NS)
        if patient is not None:
            given = patient.find("cda:given", CCDA_NS)
            family = patient.find("cda:family", CCDA_NS)
            parts = []
            if given is not None and given.text:
                parts.append(given.text)
            if family is not None and family.text:
                parts.append(family.text)
            patient_name = " ".join(parts)

        # Document date
        doc_date = ""
        eff = root.find(".//cda:effectiveTime", CCDA_NS)
        if eff is not None:
            val = eff.get("value", "")
            if len(val) >= 8:
                doc_date = f"{val[:4]}-{val[4:6]}-{val[6:8]}"

        # Sections
        sections = []
        for section in root.findall(".//cda:section", CCDA_NS):
            title_el = section.find("cda:title", CCDA_NS)
            text_el = section.find("cda:text", CCDA_NS)
            title = title_el.text if title_el is not None and title_el.text else "Untitled"
            content_parts = []
            if text_el is not None:
                for elem in text_el.iter():
                    if elem.text:
                        content_parts.append(elem.text.strip())
                    if elem.tail:
                        content_parts.append(elem.tail.strip())
            content = " ".join(filter(None, content_parts))
            if content:
                sections.append({"title": title, "content": content})

        # Build full text
        full_text = "\n\n".join(
            f"## {s['title']}\n{s['content']}" for s in sections
        )

        return {
            "patient_name": patient_name,
            "document_date": doc_date,
            "sections": sections,
            "full_text": full_text,
        }
    except Exception as e:
        log.error(f"CCDA parse failed for {xml_path.name}: {e}")
        return None


# ============================================================
# Embedding client
# ============================================================

class EmbeddingClient:
    """Async OpenAI embedding client with batching."""

    def __init__(self, api_key: str = OPENAI_API_KEY, model: str = EMBEDDING_MODEL):
        self.api_key = api_key
        self.model = model
        self.client = httpx.AsyncClient(timeout=60.0)
        self.total_tokens = 0
        self.total_requests = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of texts. Returns list of 3072-dim vectors."""
        if not texts:
            return []
        # Truncate to 8191 tokens (OpenAI limit) — rough char estimate
        truncated = [t[:30000] for t in texts]

        resp = await self.client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"input": truncated, "model": self.model, "dimensions": EMBEDDING_DIMENSIONS},
        )
        resp.raise_for_status()
        data = resp.json()
        self.total_tokens += data.get("usage", {}).get("total_tokens", 0)
        self.total_requests += 1
        return [d["embedding"] for d in data["data"]]

    async def close(self):
        await self.client.aclose()


# ============================================================
# Progress tracker
# ============================================================

class ProgressTracker:
    """Real-time progress tracking with ETA."""

    def __init__(self, total: int, label: str):
        self.total = total
        self.label = label
        self.done = 0
        self.ok = 0
        self.skipped = 0
        self.errors = 0
        self.chunks_written = 0
        self.start_time = time.time()

    def tick(self, success: bool = True, chunks: int = 0, filename: str = "", detail: str = ""):
        self.done += 1
        if success:
            self.ok += 1
            self.chunks_written += chunks
        elif detail == "skipped":
            self.skipped += 1
        else:
            self.errors += 1

        elapsed = time.time() - self.start_time
        rate = self.done / elapsed if elapsed > 0 else 0
        eta = (self.total - self.done) / rate if rate > 0 else 0
        pct = (self.done / self.total * 100) if self.total > 0 else 100

        # Status emoji
        icon = "✓" if success else ("⊘" if detail == "skipped" else "✗")

        # Per-document line
        msg = f"  [{self.done}/{self.total} {pct:.0f}%] {icon} {filename}"
        if chunks:
            msg += f" → {chunks} chunks"
        if detail and detail != "skipped":
            msg += f" ({detail})"
        msg += f"  [{rate:.1f}/min, ETA {self._fmt_time(eta)}]"
        log.info(msg)

        # Periodic aggregate every 10 docs
        if self.done % 10 == 0 or self.done == self.total:
            log.info(
                f"  ── {self.label} progress: {self.ok} ok / {self.skipped} skip / "
                f"{self.errors} err / {self.chunks_written} chunks / "
                f"{self._fmt_time(elapsed)} elapsed / ETA {self._fmt_time(eta)}"
            )

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        rate = self.ok / (elapsed / 60) if elapsed > 0 else 0
        return (
            f"{self.label}: {self.ok} ok, {self.skipped} skipped, {self.errors} errors, "
            f"{self.chunks_written} chunks in {self._fmt_time(elapsed)} ({rate:.1f} docs/min)"
        )

    @staticmethod
    def _fmt_time(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            return f"{h}h{m:02d}m"


# ============================================================
# File discovery
# ============================================================

def find_files(base_dir: Path, extensions: List[str]) -> List[Path]:
    """Recursively find files with given extensions (case-insensitive)."""
    if not base_dir.exists():
        return []
    found = []
    ext_set = {e.lower() for e in extensions}
    for f in base_dir.rglob("*"):
        if f.is_file() and f.suffix.lower() in ext_set and not f.name.startswith("."):
            found.append(f)
    return sorted(found)


def find_ccda_files(base_dir: Path) -> List[Path]:
    """Find CCDA XML files."""
    result = []
    for f in base_dir.rglob("*.XML"):
        try:
            with open(f, "r") as fh:
                header = fh.read(500)
                if "ClinicalDocument" in header or "urn:hl7-org:v3" in header:
                    result.append(f)
        except Exception:
            continue
    # Also .xml lowercase
    for f in base_dir.rglob("*.xml"):
        if f not in result:
            try:
                with open(f, "r") as fh:
                    header = fh.read(500)
                    if "ClinicalDocument" in header or "urn:hl7-org:v3" in header:
                        result.append(f)
            except Exception:
                continue
    return sorted(result)


# ============================================================
# Processing pipeline — Legal domain
# ============================================================

def process_legal_pdf(pdf_path: Path, chunker: TextChunker) -> Optional[IngestedDocument]:
    """Process a legal PDF (court filing) → IngestedDocument."""
    text = extract_pdf_text(pdf_path)
    if not text or len(text.strip()) < 50:
        log.warning(f"  No text extracted from {pdf_path.name}")
        return None

    case_nums, dir_name = case_number_from_path(pdf_path)
    text_case_nums = extract_case_numbers_from_text(text[:5000])
    all_case_nums = list(set(case_nums + text_case_nums))

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    chunks_text = chunker.split(text)

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="legal",
        source_path=str(pdf_path),
        filename=pdf_path.name,
        document_type="court_filing",
        title=pdf_path.stem.replace("_", " ").replace("-", " "),
        content_hash=content_hash,
        full_content=text,
        metadata={"file_type": "pdf", "dir_name": dir_name or ""},
        case_numbers=all_case_nums,
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text,
            metadata={"case_numbers": all_case_nums},
            chunk_index=i,
            total_chunks=len(chunks_text),
        ))

    return doc


def process_legal_email(eml_path: Path, chunker: TextChunker, seen_message_ids: Set[str]) -> Optional[IngestedDocument]:
    """Process a legal email (.eml) → IngestedDocument."""
    parsed = parse_eml(eml_path)
    if not parsed or not parsed["full_text"].strip():
        return None

    h = parsed["headers"]
    msg_id = h["message_id"]

    # Dedup by Message-ID
    if msg_id and msg_id in seen_message_ids:
        return None
    if msg_id:
        seen_message_ids.add(msg_id)

    mailbox = eml_path.parent.name
    direction = determine_email_direction(h["from"], mailbox)

    # Case numbers from subject + body
    case_nums = extract_case_numbers_from_text(parsed["full_text"][:5000])

    content_hash = hashlib.sha256(parsed["full_text"].encode()).hexdigest()[:16]
    chunks_text = chunker.split(parsed["full_text"])

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="legal",
        source_path=str(eml_path),
        filename=eml_path.name,
        document_type="email",
        title=h["subject"] or eml_path.stem,
        content_hash=content_hash,
        full_content=parsed["full_text"],
        metadata={"file_type": "eml", "mailbox": mailbox},
        case_numbers=case_nums,
        email_meta={
            "subject": h["subject"] or eml_path.stem,
            "message_id": msg_id,
            "in_reply_to": h["in_reply_to"],
            "thread_id": h.get("references", "").split()[0] if h.get("references") else msg_id,
            "sender": h["from"],
            "recipients": h["to"],
            "cc": h["cc"],
            "date_sent": h["date"],
            "direction": direction,
            "mailbox": mailbox,
            "has_attachments": h.get("has_attachments", False),
        },
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text,
            metadata={"case_numbers": case_nums},
            chunk_index=i,
            total_chunks=len(chunks_text),
        ))

    return doc


def process_legal_docx(docx_path: Path, chunker: TextChunker) -> Optional[IngestedDocument]:
    """Process a legal DOCX → IngestedDocument."""
    text = extract_docx_text(docx_path)
    if not text or len(text.strip()) < 50:
        return None

    case_nums, dir_name = case_number_from_path(docx_path)
    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    chunks_text = chunker.split(text)

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="legal",
        source_path=str(docx_path),
        filename=docx_path.name,
        document_type="court_filing",
        title=docx_path.stem,
        content_hash=content_hash,
        full_content=text,
        metadata={"file_type": "docx", "dir_name": dir_name or ""},
        case_numbers=case_nums,
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text,
            metadata={"case_numbers": case_nums},
            chunk_index=i,
            total_chunks=len(chunks_text),
        ))

    return doc


# ============================================================
# Processing pipeline — Medical domain
# ============================================================

def process_medical_pdf(pdf_path: Path, chunker: TextChunker) -> Optional[IngestedDocument]:
    """Process a medical PDF → IngestedDocument."""
    text = extract_pdf_text(pdf_path)
    if not text or len(text.strip()) < 50:
        return None

    record_type = auto_categorize_medical(text, pdf_path.name)
    provider = extract_provider_name(text)
    facility = extract_facility_name(text)
    dos = extract_date_from_text(text, pdf_path.name)
    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    chunks_text = chunker.split(text)

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="medical",
        source_path=str(pdf_path),
        filename=pdf_path.name,
        document_type=record_type,
        title=pdf_path.stem,
        content_hash=content_hash,
        full_content=text,
        metadata={"file_type": "pdf"},
        patient_name="William Nelson",
        provider=provider or "",
        facility=facility or "",
        date_of_service=dos,
        record_type=record_type,
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text, metadata={},
            chunk_index=i, total_chunks=len(chunks_text),
        ))

    return doc


def process_medical_ccda(xml_path: Path, chunker: TextChunker) -> Optional[IngestedDocument]:
    """Process a CCDA XML → IngestedDocument."""
    data = parse_ccda(xml_path)
    if not data or not data["full_text"]:
        return None

    content_hash = hashlib.sha256(data["full_text"].encode()).hexdigest()[:16]
    chunks_text = chunker.split(data["full_text"])

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="medical",
        source_path=str(xml_path),
        filename=xml_path.name,
        document_type="medical_export",
        title=f"CCDA Export - {data['patient_name']}",
        content_hash=content_hash,
        full_content=data["full_text"],
        metadata={"file_type": "ccda", "sections": len(data["sections"])},
        patient_name=data["patient_name"] or "William Nelson",
        date_of_service=data["document_date"],
        record_type="medical_export",
    )

    for i, chunk_text in enumerate(chunks_text):
        # Try to identify section
        section_title = ""
        for s in data["sections"]:
            if chunk_text[:100] in f"## {s['title']}\n{s['content']}"[:200]:
                section_title = s["title"]
                break
        doc.chunks.append(DocumentChunk(
            content=chunk_text,
            metadata={"section_title": section_title},
            chunk_index=i,
            total_chunks=len(chunks_text),
        ))

    return doc


def process_medical_image(img_path: Path, chunker: TextChunker) -> Optional[IngestedDocument]:
    """Process a medical image (scanned doc) → IngestedDocument."""
    text = extract_image_text(img_path)
    if not text or len(text.strip()) < 30:
        return None

    record_type = auto_categorize_medical(text, img_path.name)
    dos = extract_date_from_text(text, img_path.name)
    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    chunks_text = chunker.split(text)

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="medical",
        source_path=str(img_path),
        filename=img_path.name,
        document_type=record_type,
        title=img_path.stem,
        content_hash=content_hash,
        full_content=text,
        metadata={"file_type": "image"},
        patient_name="William Nelson",
        date_of_service=dos,
        record_type=record_type,
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text, metadata={},
            chunk_index=i, total_chunks=len(chunks_text),
        ))

    return doc


# ============================================================
# Database writer
# ============================================================

def _vec_literal(embedding: List[float]) -> str:
    """Convert embedding list to PostgreSQL halfvec literal."""
    return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"


async def ensure_cases_exist(pool: asyncpg.Pool):
    """Populate legal.cases from KNOWN_CASE_MAP."""
    for dir_name, (case_num, court, title, jurisdiction) in KNOWN_CASE_MAP.items():
        await pool.execute("""
            INSERT INTO legal.cases (case_number, court, case_title, jurisdiction)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (case_number) DO NOTHING
        """, case_num, court, title, jurisdiction)
    log.info(f"Ensured {len(KNOWN_CASE_MAP)} legal cases exist")


async def ensure_patient_exists(pool: asyncpg.Pool, name: str) -> int:
    """Get or create a patient, return ID."""
    row = await pool.fetchrow(
        "SELECT id FROM medical.patients WHERE name = $1", name
    )
    if row:
        return row["id"]
    row = await pool.fetchrow(
        "INSERT INTO medical.patients (name) VALUES ($1) RETURNING id", name
    )
    return row["id"]


async def ensure_provider_exists(pool: asyncpg.Pool, name: str, facility: str = None) -> Optional[int]:
    """Get or create a provider, return ID."""
    if not name:
        return None
    row = await pool.fetchrow(
        "SELECT id FROM medical.providers WHERE name = $1 AND COALESCE(facility, '') = COALESCE($2, '')",
        name, facility
    )
    if row:
        return row["id"]
    row = await pool.fetchrow(
        "INSERT INTO medical.providers (name, facility) VALUES ($1, $2) RETURNING id",
        name, facility
    )
    return row["id"]


def _parse_email_date(date_str: str) -> Optional[datetime]:
    """Parse an email Date header into datetime."""
    if not date_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str)
    except Exception:
        pass
    try:
        from dateutil.parser import parse as dp
        return dp(date_str)
    except Exception:
        return None


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse a YYYY-MM-DD date string."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except Exception:
        return None


async def write_document(pool: asyncpg.Pool, doc: IngestedDocument, embeddings: List[List[float]]):
    """Write a fully processed document + chunks + enrichment to PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. core.documents
            await conn.execute("""
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (id) DO NOTHING
            """,
                uuid.UUID(doc.doc_id), doc.domain, doc.source_path,
                doc.filename, doc.document_type, doc.title,
                doc.content_hash, len(doc.chunks),
                doc.full_content[:500000],  # Safety cap
                json.dumps(doc.metadata),
            )

            # 2. core.document_chunks
            for chunk, emb in zip(doc.chunks, embeddings):
                chunk_id = hashlib.md5(
                    f"{doc.source_path}:{chunk.chunk_index}".encode()
                ).hexdigest()
                await conn.execute("""
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                    ON CONFLICT (id) DO NOTHING
                """,
                    chunk_id, uuid.UUID(doc.doc_id),
                    chunk.chunk_index, chunk.total_chunks,
                    chunk.content, _vec_literal(emb),
                    json.dumps(chunk.metadata),
                )

            # 3. Legal enrichment
            if doc.domain == "legal":
                # Link to cases
                for case_num in doc.case_numbers:
                    row = await conn.fetchrow(
                        "SELECT id FROM legal.cases WHERE case_number = $1",
                        case_num
                    )
                    if row:
                        await conn.execute("""
                            INSERT INTO legal.case_documents (case_id, document_id)
                            VALUES ($1, $2) ON CONFLICT DO NOTHING
                        """, row["id"], uuid.UUID(doc.doc_id))

                # Email metadata
                if doc.email_meta:
                    em = doc.email_meta
                    await conn.execute("""
                        INSERT INTO legal.email_metadata
                            (document_id, message_id, in_reply_to, thread_id,
                             sender, recipients, cc, date_sent, direction,
                             mailbox, has_attachments, subject)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (document_id) DO NOTHING
                    """,
                        uuid.UUID(doc.doc_id),
                        em.get("message_id"), em.get("in_reply_to"),
                        em.get("thread_id"),
                        em.get("sender"), em.get("recipients"), em.get("cc"),
                        _parse_email_date(em.get("date_sent", "")),
                        em.get("direction"), em.get("mailbox"),
                        em.get("has_attachments", False),
                        em.get("subject"),
                    )

            # 4. Medical enrichment
            elif doc.domain == "medical":
                patient_id = None
                if doc.patient_name:
                    patient_id = await ensure_patient_exists(conn, doc.patient_name)

                provider_id = None
                if doc.provider:
                    provider_id = await ensure_provider_exists(
                        conn, doc.provider, doc.facility
                    )

                await conn.execute("""
                    INSERT INTO medical.record_metadata
                        (document_id, patient_id, provider_id, record_type,
                         date_of_service, facility, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                    ON CONFLICT (document_id) DO NOTHING
                """,
                    uuid.UUID(doc.doc_id),
                    patient_id, provider_id,
                    doc.record_type,
                    _parse_date(doc.date_of_service),
                    doc.facility,
                    json.dumps({}),
                )


async def ensure_patient_exists(conn, name: str) -> int:
    row = await conn.fetchrow("SELECT id FROM medical.patients WHERE name = $1", name)
    if row:
        return row["id"]
    row = await conn.fetchrow(
        "INSERT INTO medical.patients (name) VALUES ($1) ON CONFLICT (name) DO NOTHING RETURNING id",
        name
    )
    if row:
        return row["id"]
    row = await conn.fetchrow("SELECT id FROM medical.patients WHERE name = $1", name)
    return row["id"]


async def ensure_provider_exists(conn, name: str, facility: str = None) -> Optional[int]:
    if not name:
        return None
    row = await conn.fetchrow("""
        SELECT id FROM medical.providers
        WHERE name = $1 AND COALESCE(facility, '') = COALESCE($2, '')
    """, name, facility or "")
    if row:
        return row["id"]
    try:
        row = await conn.fetchrow(
            "INSERT INTO medical.providers (name, facility) VALUES ($1, $2) RETURNING id",
            name, facility
        )
        return row["id"]
    except Exception:
        row = await conn.fetchrow("""
            SELECT id FROM medical.providers
            WHERE name = $1 AND COALESCE(facility, '') = COALESCE($2, '')
        """, name, facility or "")
        return row["id"] if row else None


# ============================================================
# Multiprocessing OCR worker (must be top-level for pickle)
# ============================================================

def _process_legal_pdf_worker(pdf_path: Path) -> Optional[IngestedDocument]:
    """Worker function for parallel OCR processing."""
    import time
    start = time.time()
    # Get page count for logging
    try:
        info = pdfinfo_from_path(str(pdf_path))
        num_pages = info.get("Pages", "?")
    except Exception:
        num_pages = "?"
    log.info(f"  ⚙ OCR start: {pdf_path.name} ({num_pages} pages)")
    chunker = TextChunker()
    result = process_legal_pdf(pdf_path, chunker)
    elapsed = time.time() - start
    if result:
        log.info(f"  ✓ OCR done:  {pdf_path.name} → {len(result.chunks)} chunks in {elapsed:.1f}s")
    else:
        log.info(f"  ⊘ OCR empty: {pdf_path.name} in {elapsed:.1f}s")
    return result


def _process_medical_pdf_worker(pdf_path: Path) -> Optional[IngestedDocument]:
    """Worker function for parallel medical PDF OCR."""
    import time
    start = time.time()
    try:
        info = pdfinfo_from_path(str(pdf_path))
        num_pages = info.get("Pages", "?")
    except Exception:
        num_pages = "?"
    log.info(f"  ⚙ OCR start: {pdf_path.name} ({num_pages} pages)")
    chunker = TextChunker()
    result = process_medical_pdf(pdf_path, chunker)
    elapsed = time.time() - start
    if result:
        log.info(f"  ✓ OCR done:  {pdf_path.name} → {len(result.chunks)} chunks in {elapsed:.1f}s")
    else:
        log.info(f"  ⊘ OCR empty: {pdf_path.name} in {elapsed:.1f}s")
    return result


# ============================================================
# Main pipeline
# ============================================================

async def get_ingested_source_paths(pool: asyncpg.Pool, domain: str) -> Set[str]:
    """Get all source_path values already in the DB for a given domain."""
    rows = await pool.fetch(
        "SELECT source_path FROM core.documents WHERE domain = $1", domain
    )
    return {r["source_path"] for r in rows}


async def embed_and_write_single(
    pool: asyncpg.Pool, embedder: EmbeddingClient,
    doc: IngestedDocument, stats: Dict,
):
    """Embed and write a single document immediately to PostgreSQL."""
    chunk_texts = [c.content for c in doc.chunks]
    if not chunk_texts:
        return

    # Embed all chunks for this one document
    try:
        embeddings = await embedder.embed_batch(chunk_texts)
    except Exception as e:
        log.error(f"  Embedding failed for {doc.filename}: {e}")
        # Use zero vectors as fallback so the document is still written
        embeddings = [[0.0] * EMBEDDING_DIMENSIONS] * len(chunk_texts)

    try:
        await write_document(pool, doc, embeddings)
        stats["chunks"] += len(doc.chunks)
    except Exception as e:
        log.error(f"  DB write error for {doc.filename}: {e}")
        stats["errors"] += 1


async def ingest_legal(pool: asyncpg.Pool, embedder: EmbeddingClient,
                       skip_emails: bool = False, dry_run: bool = False) -> Dict:
    """Ingest all legal domain files — incremental with resume support."""
    stats = {"pdfs": 0, "emails": 0, "docx": 0, "chunks": 0, "errors": 0, "skipped": 0}
    chunker = TextChunker()

    await ensure_cases_exist(pool)

    # Discover files
    pdfs = find_files(CASES_DIR, [".pdf"])
    docx_files = find_files(CASES_DIR, [".docx"])
    eml_files = find_files(EMAILS_DIR, [".eml"]) if not skip_emails else []

    total_files = len(pdfs) + len(eml_files) + len(docx_files)
    log.info(f"Legal domain: {len(pdfs)} PDFs, {len(eml_files)} emails, {len(docx_files)} DOCX = {total_files} files")

    if dry_run:
        return stats

    # Resume support: load already-ingested paths
    already_done = await get_ingested_source_paths(pool, "legal")
    if already_done:
        log.info(f"Resume mode: {len(already_done)} documents already in DB, will skip them")

    # Filter out already-ingested files
    pdfs_todo = [p for p in pdfs if str(p) not in already_done]
    docx_todo = [d for d in docx_files if str(d) not in already_done]
    eml_todo = [e for e in eml_files if str(e) not in already_done]
    skipped_resume = (len(pdfs) - len(pdfs_todo)) + (len(docx_files) - len(docx_todo)) + (len(eml_files) - len(eml_todo))
    if skipped_resume:
        log.info(f"Skipping {skipped_resume} already-ingested files ({len(pdfs_todo)} PDFs, {len(docx_todo)} DOCX, {len(eml_todo)} emails remaining)")

    seen_message_ids: Set[str] = set()

    # Parallel OCR for PDFs — embed+write each as it completes
    if pdfs_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  PDF OCR: {len(pdfs_todo)} files × {OCR_WORKERS} workers @ {OCR_DPI} DPI")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(pdfs_todo), "Legal PDFs")
        with ProcessPoolExecutor(max_workers=OCR_WORKERS) as executor:
            futures = {executor.submit(_process_legal_pdf_worker, pdf): pdf for pdf in pdfs_todo}
            for future in as_completed(futures):
                pdf = futures[future]
                try:
                    doc = future.result()
                    if doc:
                        await embed_and_write_single(pool, embedder, doc, stats)
                        stats["pdfs"] += 1
                        tracker.tick(success=True, chunks=len(doc.chunks), filename=pdf.name)
                    else:
                        stats["skipped"] += 1
                        tracker.tick(success=False, filename=pdf.name, detail="skipped")
                except Exception as e:
                    log.error(f"  OCR exception for {pdf.name}: {e}")
                    stats["errors"] += 1
                    tracker.tick(success=False, filename=pdf.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    if docx_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  DOCX: {len(docx_todo)} files")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(docx_todo), "Legal DOCX")
        for docx in docx_todo:
            try:
                doc = process_legal_docx(docx, chunker)
                if doc:
                    await embed_and_write_single(pool, embedder, doc, stats)
                    stats["docx"] += 1
                    tracker.tick(success=True, chunks=len(doc.chunks), filename=docx.name)
                else:
                    stats["skipped"] += 1
                    tracker.tick(success=False, filename=docx.name, detail="skipped")
            except Exception as e:
                log.error(f"  ✗ {docx.name}: {e}")
                stats["errors"] += 1
                tracker.tick(success=False, filename=docx.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    if eml_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  Emails: {len(eml_todo)} files")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(eml_todo), "Legal Emails")
        for eml in eml_todo:
            try:
                doc = process_legal_email(eml, chunker, seen_message_ids)
                if doc:
                    await embed_and_write_single(pool, embedder, doc, stats)
                    stats["emails"] += 1
                    tracker.tick(success=True, chunks=len(doc.chunks), filename=eml.name)
                else:
                    stats["skipped"] += 1
                    tracker.tick(success=False, filename=eml.name, detail="skipped")
            except Exception as e:
                log.error(f"  ✗ {eml.name}: {e}")
                stats["errors"] += 1
                tracker.tick(success=False, filename=eml.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    return stats


async def ingest_medical(pool: asyncpg.Pool, embedder: EmbeddingClient,
                         dry_run: bool = False) -> Dict:
    """Ingest all medical domain files — incremental with resume support."""
    stats = {"pdfs": 0, "ccda": 0, "images": 0, "chunks": 0, "errors": 0, "skipped": 0}
    chunker = TextChunker()

    # Discover files
    pdfs = find_files(MEDICAL_DIR, [".pdf"])
    ccda_files = find_ccda_files(MEDICAL_DIR)
    images = find_files(MEDICAL_DIR, [".png", ".jpg", ".jpeg", ".tiff", ".tif"])

    total_files = len(pdfs) + len(ccda_files) + len(images)
    log.info(f"Medical domain: {len(pdfs)} PDFs, {len(ccda_files)} CCDA, {len(images)} images = {total_files} files")

    if dry_run:
        return stats

    # Resume support: load already-ingested paths
    already_done = await get_ingested_source_paths(pool, "medical")
    if already_done:
        log.info(f"Resume mode: {len(already_done)} documents already in DB, will skip them")

    ccda_todo = [f for f in ccda_files if str(f) not in already_done]
    pdfs_todo = [f for f in pdfs if str(f) not in already_done]
    images_todo = [f for f in images if str(f) not in already_done]
    skipped_resume = total_files - len(ccda_todo) - len(pdfs_todo) - len(images_todo)
    if skipped_resume:
        log.info(f"Skipping {skipped_resume} already-ingested files ({len(pdfs_todo)} PDFs, {len(ccda_todo)} CCDA, {len(images_todo)} images remaining)")

    if ccda_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  CCDA XML: {len(ccda_todo)} files")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(ccda_todo), "Medical CCDA")
        for xml_path in ccda_todo:
            try:
                doc = process_medical_ccda(xml_path, chunker)
                if doc:
                    await embed_and_write_single(pool, embedder, doc, stats)
                    stats["ccda"] += 1
                    tracker.tick(success=True, chunks=len(doc.chunks), filename=xml_path.name)
                else:
                    stats["skipped"] += 1
                    tracker.tick(success=False, filename=xml_path.name, detail="skipped")
            except Exception as e:
                log.error(f"  ✗ CCDA {xml_path.name}: {e}")
                stats["errors"] += 1
                tracker.tick(success=False, filename=xml_path.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    if pdfs_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  Medical PDF OCR: {len(pdfs_todo)} files × {OCR_WORKERS} workers @ {OCR_DPI} DPI")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(pdfs_todo), "Medical PDFs")
        with ProcessPoolExecutor(max_workers=OCR_WORKERS) as executor:
            futures = {executor.submit(_process_medical_pdf_worker, pdf): pdf for pdf in pdfs_todo}
            for future in as_completed(futures):
                pdf = futures[future]
                try:
                    doc = future.result()
                    if doc:
                        await embed_and_write_single(pool, embedder, doc, stats)
                        stats["pdfs"] += 1
                        tracker.tick(success=True, chunks=len(doc.chunks), filename=pdf.name)
                    else:
                        stats["skipped"] += 1
                        tracker.tick(success=False, filename=pdf.name, detail="skipped")
                except Exception as e:
                    log.error(f"  ✗ medical PDF {pdf.name}: {e}")
                    stats["errors"] += 1
                    tracker.tick(success=False, filename=pdf.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    if images_todo:
        log.info(f"")
        log.info(f"{'─'*60}")
        log.info(f"  Medical Images OCR: {len(images_todo)} files")
        log.info(f"{'─'*60}")
        tracker = ProgressTracker(len(images_todo), "Medical Images")
        for img in images_todo:
            try:
                doc = process_medical_image(img, chunker)
                if doc:
                    await embed_and_write_single(pool, embedder, doc, stats)
                    stats["images"] += 1
                    tracker.tick(success=True, chunks=len(doc.chunks), filename=img.name)
                else:
                    stats["skipped"] += 1
                    tracker.tick(success=False, filename=img.name, detail="skipped")
            except Exception as e:
                log.error(f"  ✗ {img.name}: {e}")
                stats["errors"] += 1
                tracker.tick(success=False, filename=img.name, detail=str(e)[:80])
        log.info(f"  ✔ {tracker.summary()}")

    return stats


async def record_ingestion_job(pool: asyncpg.Pool, domain: str, stats: Dict, elapsed: float):
    """Record ingestion job in ops.ingestion_jobs."""
    await pool.execute("""
        INSERT INTO ops.ingestion_jobs
            (domain, source, status, documents_processed, chunks_processed,
             documents_failed, completed_at, metadata)
        VALUES ($1, 'fresh_ingest', 'completed', $2, $3, $4, now(), $5::jsonb)
    """,
        domain,
        sum(v for k, v in stats.items() if k not in ("chunks", "errors", "skipped")),
        stats.get("chunks", 0),
        stats.get("errors", 0),
        json.dumps({"elapsed_seconds": round(elapsed, 1), **stats}),
    )


# ============================================================
# CLI
# ============================================================

async def main():
    parser = argparse.ArgumentParser(description="WDWS Enterprise Ingest Pipeline")
    parser.add_argument("--domain", choices=["legal", "medical", "all"], default="all")
    parser.add_argument("--skip-emails", action="store_true", help="Skip email ingestion")
    parser.add_argument("--dry-run", action="store_true", help="Count files only, don't ingest")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()

    db_url = args.database_url or DATABASE_URL
    log.info(f"Connecting to PostgreSQL...")
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=8)

    version = await pool.fetchval("SELECT version()")
    existing_docs = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    existing_chunks = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")

    log.info(f"")
    log.info(f"╔{'═'*58}╗")
    log.info(f"║  WDWS Enterprise Ingest Pipeline v1.0{' '*20}║")
    log.info(f"╠{'═'*58}╣")
    log.info(f"║  Server:     {version[:44]}║")
    log.info(f"║  CPU cores:  {multiprocessing.cpu_count():<45}║")
    log.info(f"║  OCR workers:{OCR_WORKERS:<45}║")
    log.info(f"║  OCR DPI:    {OCR_DPI:<45}║")
    log.info(f"║  Chunk size: {CHUNK_SIZE} chars, {CHUNK_OVERLAP} overlap{' '*24}║")
    log.info(f"║  Embed model:{EMBEDDING_MODEL:<45}║")
    log.info(f"║  DB docs:    {existing_docs:<45}║")
    log.info(f"║  DB chunks:  {existing_chunks:<45}║")
    log.info(f"║  Domain:     {args.domain:<45}║")
    log.info(f"║  Dry run:    {str(args.dry_run):<45}║")
    log.info(f"╚{'═'*58}╝")
    log.info(f"")

    embedder = EmbeddingClient()
    results = {}

    try:
        if args.domain in ("legal", "all"):
            log.info("=" * 60)
            log.info("INGESTING: Legal Domain")
            log.info("=" * 60)
            t0 = time.time()
            results["legal"] = await ingest_legal(
                pool, embedder,
                skip_emails=args.skip_emails,
                dry_run=args.dry_run,
            )
            elapsed = time.time() - t0
            if not args.dry_run:
                await record_ingestion_job(pool, "legal", results["legal"], elapsed)
            log.info(f"Legal domain completed in {elapsed:.1f}s")

        if args.domain in ("medical", "all"):
            log.info("=" * 60)
            log.info("INGESTING: Medical Domain")
            log.info("=" * 60)
            t0 = time.time()
            results["medical"] = await ingest_medical(
                pool, embedder,
                dry_run=args.dry_run,
            )
            elapsed = time.time() - t0
            if not args.dry_run:
                await record_ingestion_job(pool, "medical", results["medical"], elapsed)
            log.info(f"Medical domain completed in {elapsed:.1f}s")

    finally:
        pass

    # Final DB counts
    final_docs = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    final_chunks = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")
    final_legal = await pool.fetchval("SELECT COUNT(*) FROM core.documents WHERE domain = 'legal'")
    final_medical = await pool.fetchval("SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'")
    embed_cost = embedder.total_tokens / 1_000_000 * 0.13  # $0.13 per 1M tokens

    await embedder.close()
    await pool.close()

    # Summary
    log.info(f"")
    log.info(f"╔{'═'*58}╗")
    log.info(f"║  INGEST COMPLETE{' '*41}║")
    log.info(f"╠{'═'*58}╣")
    for domain, s in results.items():
        log.info(f"║  {domain.upper():<56}║")
        for k, v in s.items():
            log.info(f"║    {k:>14}: {str(v):<40}║")
    log.info(f"╠{'═'*58}╣")
    log.info(f"║  DATABASE TOTALS{' '*41}║")
    log.info(f"║    Documents:     {final_docs:<40}║")
    log.info(f"║    Chunks:        {final_chunks:<40}║")
    log.info(f"║    Legal docs:    {final_legal:<40}║")
    log.info(f"║    Medical docs:  {final_medical:<40}║")
    log.info(f"╠{'═'*58}╣")
    log.info(f"║  EMBEDDING COST{' '*42}║")
    log.info(f"║    Tokens:        {embedder.total_tokens:,}{' '*(40-len(f'{embedder.total_tokens:,}'))}║")
    log.info(f"║    Requests:      {embedder.total_requests:<40}║")
    log.info(f"║    Est. cost:     ${embed_cost:.4f}{' '*(39-len(f'${embed_cost:.4f}'))}║")
    log.info(f"╚{'═'*58}╝")


if __name__ == "__main__":
    asyncio.run(main())
