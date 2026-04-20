#!/usr/bin/env python3
"""
One-shot script: Download attachment from Graph API for a specific email
and ingest it into WDWS (legal.email_attachments + core.documents + chunks).

Usage:
    python fix_graph_attachment.py
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

import asyncpg
import httpx

# ── Config ──
DATABASE_URL = os.environ["DATABASE_URL"]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMS = 3072
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# The email we need to process
EMAIL_DOC_ID = "8d64fc4c-0628-4fff-86bb-aaf8225e687c"
GRAPH_MESSAGE_ID = "AAMkAGRlZjg2NTcwLThmYTAtNDJhMy04OWYxLTJlNDVmNzczNDc0ZQBGAAAAAAB8wdKevo5NR4RYRG2OJKouBwA1lmu87wJpRqBfSCGMoGM-AAAAAAEMAAA1lmu87wJpRqBfSCGMoGM-AAHyS09pAAA="
MAILBOX = "athena@seattleseahawks.me"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fix-att")


def _require_env(name: str, value: Optional[str]) -> str:
    if value:
        return value
    raise RuntimeError(f"{name} environment variable is required")

STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
FEDERAL_CASE_RE = re.compile(r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?', re.IGNORECASE)


def extract_case_numbers(text: str) -> List[str]:
    found = set()
    for m in FEDERAL_CASE_RE.finditer(text):
        court, year, seq, judge = m.groups()
        n = f"{court}{year}CV{seq}"
        if judge:
            n += judge.upper()
        found.add(n.upper())
    for m in STATE_CASE_RE.finditer(text):
        found.add(m.group(0))
    return list(found)


def _vec_literal(emb: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in emb) + "]"


class TextChunker:
    def __init__(self, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.overlap = overlap
        self.seps = ["\n\n", "\n", ". ", " ", ""]

    def split(self, text: str) -> List[str]:
        if not text:
            return []
        return self._split(text, self.seps)

    def _split(self, text, seps):
        final = []
        sep = seps[-1]
        new_seps = []
        for i, s in enumerate(seps):
            if s == "" or s in text:
                sep = s
                new_seps = seps[i + 1:]
                break
        parts = text.split(sep) if sep else list(text)
        good = []
        for p in parts:
            if len(p) < self.chunk_size:
                good.append(p)
            else:
                if good:
                    final.extend(self._merge(good, sep))
                    good = []
                if new_seps:
                    final.extend(self._split(p, new_seps))
                else:
                    final.append(p)
        if good:
            final.extend(self._merge(good, sep))
        return final

    def _merge(self, splits, sep):
        docs = []
        current = []
        total = 0
        for s in splits:
            sl = len(s)
            if total + sl + (len(sep) if current else 0) > self.chunk_size:
                if current:
                    docs.append(sep.join(current))
                    while total > self.overlap and len(current) > 1:
                        total -= len(current[0]) + len(sep)
                        current.pop(0)
                current = [s]
                total = sl
            else:
                current.append(s)
                total += sl + (len(sep) if len(current) > 1 else 0)
        if current:
            docs.append(sep.join(current))
        return docs


def extract_pdf_text(pdf_path: Path) -> str:
    """OCR PDF page by page."""
    try:
        import pytesseract
        from pdf2image import convert_from_path, pdfinfo_from_path

        try:
            info = pdfinfo_from_path(str(pdf_path))
            num_pages = info.get("Pages", 0)
        except Exception:
            num_pages = 0

        parts = []
        if num_pages > 0:
            for pg in range(1, num_pages + 1):
                try:
                    images = convert_from_path(str(pdf_path), dpi=150,
                                               first_page=pg, last_page=pg, thread_count=1)
                    if images:
                        t = pytesseract.image_to_string(images[0])
                        if t and t.strip():
                            parts.append(f"[Page {pg}]\n{t}")
                        del images
                except Exception as e:
                    log.warning(f"OCR page {pg} failed: {e}")
        else:
            images = convert_from_path(str(pdf_path), dpi=150, thread_count=1)
            for pg, img in enumerate(images, 1):
                t = pytesseract.image_to_string(img)
                if t and t.strip():
                    parts.append(f"[Page {pg}]\n{t}")

        return "\n\n".join(parts)
    except Exception as e:
        log.warning(f"OCR failed: {e}, trying pdftotext")
        try:
            r = subprocess.run(["pdftotext", "-layout", str(pdf_path), "-"],
                               capture_output=True, text=True, timeout=30)
            return r.stdout
        except Exception as e2:
            log.error(f"All extraction failed: {e2}")
            return ""


async def get_graph_token(client: httpx.AsyncClient) -> str:
    resp = await client.post(TOKEN_URL, data={
        "client_id": GRAPH_CLIENT_ID,
        "client_secret": GRAPH_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


async def get_attachments_from_graph(client: httpx.AsyncClient, token: str) -> List[dict]:
    """List attachments on the Graph message."""
    url = f"{GRAPH_BASE_URL}/users/{MAILBOX}/messages/{GRAPH_MESSAGE_ID}/attachments"
    resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.json().get("value", [])


async def download_attachment(client: httpx.AsyncClient, token: str, att_id: str) -> bytes:
    """Download attachment content from Graph API."""
    url = f"{GRAPH_BASE_URL}/users/{MAILBOX}/messages/{GRAPH_MESSAGE_ID}/attachments/{att_id}/$value"
    resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    return resp.content


async def embed_batch(client: httpx.AsyncClient, texts: List[str]) -> List[List[float]]:
    if not texts:
        return []
    truncated = [t[:30000] for t in texts]
    resp = await client.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json={"input": truncated, "model": EMBEDDING_MODEL, "dimensions": EMBEDDING_DIMS},
        timeout=60,
    )
    resp.raise_for_status()
    return [d["embedding"] for d in resp.json()["data"]]


async def main():
    _require_env("OPENAI_API_KEY", OPENAI_API_KEY)
    _require_env("GRAPH_CLIENT_ID", GRAPH_CLIENT_ID)
    _require_env("GRAPH_CLIENT_SECRET", GRAPH_CLIENT_SECRET)
    _require_env("GRAPH_TENANT_ID", GRAPH_TENANT_ID)

    log.info("=" * 60)
    log.info("Fix Graph Attachment: Download & ingest attachment")
    log.info(f"Email: {EMAIL_DOC_ID}")
    log.info(f"Mailbox: {MAILBOX}")
    log.info("=" * 60)

    async with httpx.AsyncClient(timeout=60) as client:
        # 1. Get Graph API token
        log.info("Step 1: Authenticating with Graph API...")
        token = await get_graph_token(client)
        log.info("  ✓ Authenticated")

        # 2. List attachments on this message
        log.info("Step 2: Listing attachments from Graph API...")
        attachments = await get_attachments_from_graph(client, token)
        log.info(f"  ✓ Found {len(attachments)} attachment(s)")

        if not attachments:
            log.error("No attachments found on this message!")
            return

        for att in attachments:
            log.info(f"  - {att.get('name')} ({att.get('contentType')}, {att.get('size', 0)} bytes)")

        # 3. Connect to DB
        log.info("Step 3: Connecting to PostgreSQL...")
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        log.info("  ✓ Connected")

        chunker = TextChunker()
        email_doc_uuid = uuid.UUID(EMAIL_DOC_ID)

        for att in attachments:
            att_name = att.get("name", "unknown")
            att_ct = att.get("contentType", "application/octet-stream")
            att_size = att.get("size", 0)
            att_graph_id = att.get("id", "")

            # Check if contentBytes is inline (small attachments)
            if att.get("contentBytes"):
                import base64
                att_data = base64.b64decode(att["contentBytes"])
                log.info(f"  ✓ Got inline attachment: {att_name} ({len(att_data)} bytes)")
            else:
                # Download separately
                log.info(f"  Downloading {att_name}...")
                att_data = await download_attachment(client, token, att_graph_id)
                log.info(f"  ✓ Downloaded {len(att_data)} bytes")

            # 4. Save to temp file for OCR
            suffix = Path(att_name).suffix or ".bin"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(att_data)
                tmp_path = Path(tmp.name)

            # 5. Extract text
            log.info(f"Step 4: Extracting text from {att_name}...")
            if att_ct == "application/pdf" or suffix.lower() == ".pdf":
                text = extract_pdf_text(tmp_path)
                method = "ocr_pdf_150dpi"
            elif att_ct.startswith("image/"):
                try:
                    import pytesseract
                    from PIL import Image
                    text = pytesseract.image_to_string(Image.open(tmp_path))
                    method = "ocr_image"
                except Exception as e:
                    log.error(f"Image OCR failed: {e}")
                    text = ""
                    method = "error"
            elif "word" in att_ct or suffix.lower() in (".docx", ".doc"):
                try:
                    import zipfile
                    import xml.etree.ElementTree as ET
                    with zipfile.ZipFile(tmp_path) as z:
                        with z.open("word/document.xml") as f:
                            tree = ET.parse(f)
                            root = tree.getroot()
                            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                            paragraphs = root.findall(".//w:p", ns)
                            lines = []
                            for p in paragraphs:
                                runs = p.findall(".//w:t", ns)
                                line = "".join(r.text or "" for r in runs)
                                if line:
                                    lines.append(line)
                            text = "\n".join(lines)
                    method = "docx_xml"
                except Exception as e:
                    log.error(f"DOCX extraction failed: {e}")
                    text = ""
                    method = "error"
            elif att_ct == "text/plain" or suffix.lower() == ".txt":
                text = tmp_path.read_text(errors="ignore")
                method = "text_direct"
            else:
                # Try PDF first, then image
                text = extract_pdf_text(tmp_path)
                method = "ocr_pdf_fallback" if text.strip() else "unknown"

            log.info(f"  ✓ Extracted {len(text)} chars via {method}")

            # Clean up temp file
            tmp_path.unlink(missing_ok=True)

            if not text or len(text.strip()) < 10:
                log.warning(f"  ⊘ No usable text from {att_name}, storing metadata only")
                # Still create the email_attachments record
                att_db_id = uuid.uuid4()
                await pool.execute("""
                    INSERT INTO legal.email_attachments
                        (id, email_doc_id, filename, content_type, file_size,
                         extracted_text, extraction_method, is_processed)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT DO NOTHING
                """, att_db_id, email_doc_uuid, att_name, att_ct, att_size,
                    text[:100000] if text else None, method, True)
                log.info(f"  ✓ Created email_attachments record (no chunks)")
                continue

            # 6. Chunk the text
            chunks_text = chunker.split(text)
            log.info(f"Step 5: Chunked into {len(chunks_text)} chunks")

            # 7. Embed
            log.info(f"Step 6: Embedding {len(chunks_text)} chunks...")
            try:
                all_embeddings = []
                for i in range(0, len(chunks_text), 100):
                    batch = chunks_text[i:i + 100]
                    embs = await embed_batch(client, batch)
                    all_embeddings.extend(embs)
                log.info(f"  ✓ Generated {len(all_embeddings)} embeddings")
            except Exception as e:
                log.error(f"  ✗ Embedding failed: {e}")
                all_embeddings = [[0.0] * EMBEDDING_DIMS] * len(chunks_text)

            # 8. Extract case numbers
            case_numbers = extract_case_numbers(text[:5000])
            log.info(f"  Case numbers found: {case_numbers}")

            # 9. Write to database
            log.info("Step 7: Writing to database...")
            doc_id = uuid.uuid4()
            att_db_id = uuid.uuid4()
            content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
            email_title = "ADA accommodation for 26-2-00762-31: review and decision"
            title = f"{att_name} (from: {email_title})"
            source_path = f"graph://{MAILBOX}/{GRAPH_MESSAGE_ID}"

            async with pool.acquire() as conn:
                async with conn.transaction():
                    # a. core.documents FIRST (FK target for email_attachments.child_doc_id)
                    await conn.execute("""
                        INSERT INTO core.documents
                            (id, domain, source_path, filename, document_type, title,
                             content_hash, total_chunks, full_content, metadata)
                        VALUES ($1, 'legal', $2, $3, 'email_attachment', $4,
                                $5, $6, $7, $8::jsonb)
                        ON CONFLICT (id) DO NOTHING
                    """, doc_id, source_path, att_name, title,
                        content_hash, len(chunks_text), text[:500000],
                        json.dumps({
                            "parent_email_doc_id": EMAIL_DOC_ID,
                            "attachment_id": str(att_db_id),
                            "extraction_method": method,
                        }))

                    # b. email_attachments record (references core.documents via child_doc_id)
                    await conn.execute("""
                        INSERT INTO legal.email_attachments
                            (id, email_doc_id, filename, content_type, file_size,
                             extracted_text, extraction_method, is_processed, child_doc_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT DO NOTHING
                    """, att_db_id, email_doc_uuid, att_name, att_ct, att_size,
                        text[:100000], method, True, doc_id)

                    # c. document_chunks with embeddings
                    for i, (chunk_text, emb) in enumerate(zip(chunks_text, all_embeddings)):
                        chunk_id = hashlib.md5(f"att:{att_db_id}:{i}".encode()).hexdigest()
                        await conn.execute("""
                            INSERT INTO core.document_chunks
                                (id, document_id, chunk_index, total_chunks,
                                 content, embedding, metadata)
                            VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                            ON CONFLICT (id) DO NOTHING
                        """, chunk_id, doc_id, i, len(chunks_text),
                            chunk_text, _vec_literal(emb),
                            json.dumps({"case_numbers": case_numbers}))

                    # d. Link to cases
                    for cn in case_numbers:
                        row = await conn.fetchrow(
                            "SELECT id FROM legal.cases WHERE case_number = $1", cn)
                        if row:
                            await conn.execute("""
                                INSERT INTO legal.case_documents (case_id, document_id)
                                VALUES ($1, $2) ON CONFLICT DO NOTHING
                            """, row["id"], doc_id)
                            log.info(f"  ✓ Linked to case {cn}")

                    # e. Document relationship: email → attachment
                    await conn.execute("""
                        INSERT INTO core.document_relationships
                            (source_document_id, target_document_id, relationship_type)
                        VALUES ($1, $2, 'has_attachment')
                        ON CONFLICT DO NOTHING
                    """, email_doc_uuid, doc_id)

            log.info(f"  ✓ Created document {doc_id}")
            log.info(f"  ✓ Created {len(chunks_text)} chunks with embeddings")
            log.info(f"  ✓ Created email_attachments record {att_db_id}")
            log.info(f"  ✓ Created document relationship (has_attachment)")

        await pool.close()

    log.info("")
    log.info("=" * 60)
    log.info("DONE — Attachment ingested successfully!")
    log.info("AI agents can now search and review this document.")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
