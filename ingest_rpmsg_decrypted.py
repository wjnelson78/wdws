#!/usr/bin/env python3
"""
Ingest .rpmsg-decrypted email bodies + their embedded attachments into Athena.

Scope:
  1. Every `<stem>.decrypted.html` file alongside a `.rpmsg` on disk
     (produced by batch_decrypt_rpmsg.py).
  2. Every `<stem>.decrypted.att##_<filename>` extracted alongside.
  3. The Workwell CPET dataset in data/medical/records/records/workwell_cpet_2026_03_23/.

Each decrypted file becomes a core.documents row with:
  - domain inferred (medical for CPET/health, legal for case correspondence,
    email otherwise)
  - BGE-M3 embeddings + contextual-retrieval enrichment
  - metadata.source = 'rpmsg_decrypt'
  - metadata.parent_rpmsg_path / .pl_owner preserved for provenance
  - medical.record_metadata populated for medical-domain items

Idempotent: keyed on source_path (skip if already in DB).
"""

import asyncio
import csv
import hashlib
import html
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import asyncpg
import pypdf

sys.path.insert(0, "/opt/wdws")
from dropbox_watcher import (
    TextChunker,
    extract_pdf_text,
    ensure_patient_exists,
)
from contextual_retrieval import generate_context_sync, enrich_chunks
from embedding_service import embed_texts_sync

DATABASE_URL = os.environ["DATABASE_URL"]

DECRYPT_REPORT = Path("/opt/wdws/data/emails/attachments/rpmsg_decrypt_report.csv")
WORKWELL_DIR = Path("/opt/wdws/data/medical/records/records/workwell_cpet_2026_03_23")
NEW_RPMSG_FROM_EML = Path("/opt/wdws/data/emails/rpmsg_from_eml")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("rpmsg-ingest")


def _vec_literal(e):
    return "[" + ",".join(f"{v:.6f}" for v in e) + "]"


def html_to_text(raw: str) -> str:
    t = re.sub(r"<style.*?</style>", "", raw, flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r"<script.*?</script>", "", t, flags=re.DOTALL | re.IGNORECASE)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def extract_pdf_fast(pdf_path: Path) -> str:
    """pypdf first (fast), fall back to OCR if empty."""
    try:
        r = pypdf.PdfReader(str(pdf_path))
        parts = []
        for i, p in enumerate(r.pages, 1):
            try:
                t = p.extract_text() or ""
            except Exception:
                t = ""
            if t.strip():
                parts.append(f"[Page {i}]\n{t}")
        text = "\n\n".join(parts)
        if len(text.strip()) > 100:
            return text
    except Exception:
        pass
    # Fallback to OCR (from dropbox_watcher)
    return extract_pdf_text(pdf_path)


def infer_domain(path: Path, body_text: str) -> str:
    p = str(path).lower()
    t = body_text[:3000].lower()
    if "medical/records" in p or "cpet" in p or "workwell" in p:
        return "medical"
    if any(k in t for k in ["cpet", "bike report", "chronic fatigue", "post-exertional", "me/cfs", "long covid", "prognosis", "pulmonary"]):
        return "medical"
    if any(k in t for k in ["motion to compel", "declaration", "ada accommodation", "nelson v.", "settlement", "attorney", "plaintiff", "defendant", "case no"]):
        return "legal"
    return "legal"  # correspondence around cases defaults to legal


def infer_record_type(domain: str, body_text: str) -> str:
    t = body_text[:2000].lower()
    if domain == "medical":
        if "cpet" in t or "bike report" in t:
            return "radiology"  # closest existing — test report
        return "progress_note"
    # domain == legal/email: existing convention is "other"
    return "other"


async def already_ingested(pool, source_path: str) -> bool:
    row = await pool.fetchrow(
        "SELECT id FROM core.documents WHERE source_path = $1", source_path
    )
    return row is not None


async def write_doc(
    pool: asyncpg.Pool,
    source_path: Path,
    doc_type: str,
    title: str,
    text: str,
    domain: str,
    extra_meta: dict,
    medical_meta: Optional[dict] = None,
) -> str:
    """Chunk + embed + insert a single document."""
    if len(text.strip()) < 20:
        return "empty"

    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        return "empty"

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    doc_id = str(uuid.uuid4())

    cr_context = generate_context_sync(
        title=title,
        domain=domain,
        document_type=doc_type,
        content_preview=text[:3000],
        case_number=None,
    )
    enriched_texts = enrich_chunks(cr_context, chunks)
    all_embs: List[List[float]] = []
    for i in range(0, len(enriched_texts), 100):
        all_embs.extend(embed_texts_sync(enriched_texts[i : i + 100]))

    metadata = {
        "file_type": source_path.suffix.lstrip(".") or "txt",
        "source": "rpmsg_decrypt",
        "ingested_at": datetime.now().isoformat(),
        **extra_meta,
    }

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                uuid.UUID(doc_id), domain, str(source_path),
                source_path.name, doc_type, title,
                content_hash, len(chunks), text[:500000],
                json.dumps(metadata),
            )

            for i, (chunk_text, emb) in enumerate(zip(chunks, all_embs)):
                chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
                await conn.execute(
                    """
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, embedded_content, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7, $8::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    chunk_id, uuid.UUID(doc_id),
                    i, len(chunks),
                    chunk_text, _vec_literal(emb),
                    enriched_texts[i],
                    json.dumps({}),
                )

            if domain == "medical" and medical_meta:
                patient_id = await ensure_patient_exists(conn, medical_meta["patient_name"])
                dos = medical_meta.get("date_of_service")
                dos_date = None
                if dos:
                    try:
                        dos_date = datetime.strptime(dos, "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        pass
                await conn.execute(
                    """
                    INSERT INTO medical.record_metadata
                        (document_id, patient_id, provider_id, record_type,
                         date_of_service, facility, metadata)
                    VALUES ($1, $2, NULL, $3, $4, $5, $6::jsonb)
                    ON CONFLICT (document_id) DO NOTHING
                    """,
                    uuid.UUID(doc_id), patient_id,
                    medical_meta["record_type"], dos_date,
                    medical_meta.get("facility", ""),
                    json.dumps({"source": "rpmsg_decrypt", **medical_meta.get("extra", {})}),
                )
    return "ok"


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    before = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE metadata->>'source'='rpmsg_decrypt'"
    )
    log.info(f"rpmsg-decrypt docs before: {before}")

    # ── 1. Collect all decrypted files ──
    targets: list[tuple[Path, str, dict]] = []  # (path, kind, extra_meta)

    # From the first-batch CSV report
    if DECRYPT_REPORT.exists():
        with open(DECRYPT_REPORT) as f:
            for row in csv.DictReader(f):
                if row["status"] != "ok":
                    continue
                body = Path(row["output"])
                if not body.exists():
                    continue
                extra = {
                    "parent_rpmsg_path": row["rpmsg_path"],
                    "pl_owner": row["pl_owner"],
                    "body_type": row["body_type"],
                    "rpmsg_source": "first_batch",
                }
                targets.append((body, "html_body", extra))
                # Sibling attachments (.attNN_*)
                stem = body.stem.removesuffix(".decrypted")
                for att in body.parent.glob(f"{stem}.decrypted.att*"):
                    targets.append((att, "attachment", {**extra, "att_of": str(body)}))

    # From the 2026-03-23 extraction (new)
    for p in NEW_RPMSG_FROM_EML.glob("*/body.html"):
        targets.append((p, "html_body", {
            "parent_rpmsg_path": str(p.parent / "message.rpmsg"),
            "pl_owner": "athena@seattleseahawks.me",
            "body_type": "HTML",
            "rpmsg_source": "eml_extracted",
        }))

    # CPET dataset — treated as medical priority
    if WORKWELL_DIR.exists():
        for p in WORKWELL_DIR.glob("*"):
            if p.is_file():
                targets.append((p, "cpet", {
                    "parent_rpmsg_path": "/opt/wdws/data/emails/emails/william_seattleseahawks.me/2026-03-24 - RE post-test questionnaires - Workwell.eml",
                    "pl_owner": "athena@seattleseahawks.me",
                    "rpmsg_source": "eml_extracted_workwell_cpet",
                    "workwell_cpet": True,
                }))

    log.info(f"targets: {len(targets)}")

    # ── 2. Process each ──
    stats = {"ok": 0, "skip": 0, "empty": 0, "fail": 0}
    for i, (path, kind, extra) in enumerate(targets, 1):
        source_path = str(path)
        if await already_ingested(pool, source_path):
            stats["skip"] += 1
            continue

        try:
            # Extract text
            if path.suffix.lower() in (".html", ".htm"):
                raw = path.read_text(errors="replace")
                text = html_to_text(raw)
            elif path.suffix.lower() == ".pdf":
                text = extract_pdf_fast(path)
            elif path.suffix.lower() in (".png", ".jpg", ".jpeg", ".gif"):
                # Skip tiny inline email-signature images
                if path.stat().st_size < 30_000:
                    stats["skip"] += 1
                    continue
                # OCR larger images via tesseract
                import pytesseract
                from PIL import Image
                try:
                    text = pytesseract.image_to_string(Image.open(path))
                except Exception:
                    text = ""
            else:
                text = path.read_text(errors="replace")

            domain = infer_domain(path, text)
            record_type = infer_record_type(domain, text)
            doc_type = record_type
            title = path.stem.replace("_", " ")[:200]

            medical_meta = None
            if domain == "medical":
                dos = None
                if "2026-03-23" in str(path) or "2026-03-23" in text[:5000]:
                    dos = "2026-03-23"
                medical_meta = {
                    "patient_name": "William Nelson",
                    "record_type": record_type,
                    "date_of_service": dos,
                    "facility": "Workwell Foundation" if extra.get("workwell_cpet") else "",
                    "extra": {"kind": kind, **extra},
                }

            result = await write_doc(
                pool, path, doc_type, title, text, domain,
                {**extra, "kind": kind}, medical_meta=medical_meta,
            )
            stats[result] += 1
            mark = {"ok": "✔", "empty": "∅"}.get(result, "?")
            log.info(f"[{i:3d}/{len(targets)}] {mark} {domain:8s} {record_type:15s} {path.name[:60]}")

        except Exception as e:
            log.error(f"[{i:3d}/{len(targets)}] ✗ {path.name}: {e}")
            stats["fail"] += 1

    after = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE metadata->>'source'='rpmsg_decrypt'"
    )
    log.info("=" * 60)
    log.info(f"ingested: {stats['ok']}  skipped: {stats['skip']}  empty: {stats['empty']}  failed: {stats['fail']}")
    log.info(f"rpmsg-decrypt docs: {before} → {after}")
    log.info("=" * 60)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
