#!/usr/bin/env python3
"""
Batch ingest UW Medicine HealthSummary export (Mar 17, 2026).

Received via email from william@seattleseahawks.me to athena@seattleseahawks.me
on 2026-03-17T22:06:57Z, subject "UW Medical Files".

Contains 7 CCDA XML documents + 1 PDF.
Uses DB-based setId dedup (no dependency on old files on disk).
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import asyncpg
import httpx

sys.path.insert(0, "/opt/wdws")
from dropbox_watcher import (
    extract_pdf_text,
    extract_ccda_text,
    TextChunker,
    auto_categorize_medical,
    extract_date_from_text,
    EMBEDDING_MODEL,
    EMBEDDING_DIMENSIONS,
)
from ingest_uw_2026 import (
    get_ccda_set_id,
    get_ccda_version,
    get_ccda_effective_time,
    delete_document,
    _vec_literal,
    EmbeddingClient,
)
from contextual_retrieval import generate_context_sync, enrich_chunks

DATABASE_URL = os.environ["DATABASE_URL"]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

EXPORT_LABEL = "HealthSummary_Mar_17_2026"
NEW_EXPORT = Path(
    "/opt/wdws/data/dropbox/medical/HealthSummary_Mar_17_2026/IHE_XDM/William1"
)
PDF_DIR = Path(
    "/opt/wdws/data/dropbox/medical/HealthSummary_Mar_17_2026"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("uw-ingest-mar17")


async def build_old_setid_map_from_db(pool: asyncpg.Pool) -> Dict[str, dict]:
    """Build {setId: {doc_uuid, filename}} from existing DB records."""
    rows = await pool.fetch("""
        SELECT id, filename, metadata->>'ccda_set_id' AS set_id
        FROM core.documents
        WHERE domain = 'medical'
          AND metadata->>'ccda_set_id' IS NOT NULL
    """)
    mapping = {}
    for r in rows:
        mapping[r["set_id"]] = {
            "doc_uuid": str(r["id"]),
            "old_filename": r["filename"],
        }
    log.info(f"Loaded {len(mapping)} existing setIds from DB")
    return mapping


async def write_doc(conn, doc_id, source_path, filename, doc_type, title,
                    content_hash, full_content, metadata, chunks, embeddings,
                    medical_meta=None, enriched_texts=None):
    await conn.execute("""
        INSERT INTO core.documents
            (id, domain, source_path, filename, document_type, title,
             content_hash, total_chunks, full_content, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        ON CONFLICT (id) DO NOTHING
    """,
        uuid.UUID(doc_id), "medical", source_path,
        filename, doc_type, title,
        content_hash, len(chunks),
        full_content[:500000],
        json.dumps(metadata),
    )

    for i, (chunk_text, emb) in enumerate(zip(chunks, embeddings)):
        chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
        embedded_content = enriched_texts[i] if enriched_texts else chunk_text
        await conn.execute("""
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks,
                 content, embedding, embedded_content, metadata)
            VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7, $8::jsonb)
            ON CONFLICT (id) DO NOTHING
        """,
            chunk_id, uuid.UUID(doc_id),
            i, len(chunks),
            chunk_text, _vec_literal(emb),
            embedded_content,
            json.dumps({}),
        )

    if medical_meta:
        from dropbox_watcher import ensure_patient_exists, ensure_provider_exists

        patient_id = None
        if medical_meta.get("patient_name"):
            patient_id = await ensure_patient_exists(conn, medical_meta["patient_name"])

        provider_id = None
        if medical_meta.get("provider"):
            provider_id = await ensure_provider_exists(
                conn, medical_meta["provider"], medical_meta.get("facility")
            )

        dos = medical_meta.get("date_of_service")
        dos_date = None
        if dos:
            try:
                dos_date = datetime.strptime(dos, "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass

        await conn.execute("""
            INSERT INTO medical.record_metadata
                (document_id, patient_id, provider_id, record_type,
                 date_of_service, facility, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (document_id) DO NOTHING
        """,
            uuid.UUID(doc_id),
            patient_id, provider_id,
            medical_meta.get("record_type", "other"),
            dos_date,
            medical_meta.get("facility", ""),
            json.dumps({
                "source": "email_attachment",
                "export": EXPORT_LABEL,
                "email_from": "william@seattleseahawks.me",
                "email_received": "2026-03-17T22:06:57Z",
                "ingested_at": datetime.now().isoformat(),
            }),
        )


async def ingest_ccda(pool, embedder, xml_path, old_map, stats):
    set_id = get_ccda_set_id(xml_path)
    version = get_ccda_version(xml_path) or 1
    eff_time = get_ccda_effective_time(xml_path) or ""
    source_path = str(xml_path)
    filename = xml_path.name

    text = extract_ccda_text(xml_path)
    if not text or len(text.strip()) < 20:
        log.warning(f"  ✗ No text: {filename}")
        stats["failed"] += 1
        return

    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        log.warning(f"  ✗ No chunks: {filename}")
        stats["failed"] += 1
        return

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    doc_type = auto_categorize_medical(text, filename)
    title = Path(filename).stem.replace("_", " ")
    doc_id = str(uuid.uuid4())

    # Contextual Retrieval — enrich chunks before embedding
    cr_context = generate_context_sync(
        title=title,
        domain="medical",
        document_type=doc_type,
        content_preview=text[:3000],
        case_number=None,
    )
    enriched_texts = enrich_chunks(cr_context, chunks)

    all_embeddings = []
    for i in range(0, len(enriched_texts), 100):
        batch = enriched_texts[i:i + 100]
        embs = await embedder.embed_batch(batch)
        all_embeddings.extend(embs)

    metadata = {
        "file_type": "ccda",
        "ccda_set_id": set_id,
        "ccda_version": version,
        "effective_time": eff_time,
        "sections": len(chunks),
        "source": "email_attachment",
        "export": EXPORT_LABEL,
        "email_from": "william@seattleseahawks.me",
        "dropped_at": datetime.now().isoformat(),
    }

    medical_meta = {
        "patient_name": "William Nelson",
        "record_type": doc_type,
        "date_of_service": extract_date_from_text(text, filename),
        "provider": "",
        "facility": "UW Medicine",
    }

    action = "NEW"
    async with pool.acquire() as conn:
        async with conn.transaction():
            if set_id and set_id in old_map:
                old_info = old_map[set_id]
                await delete_document(conn, old_info["doc_uuid"])
                action = "UPDATE"
                stats["updated"] += 1
                log.info(
                    f"  ↻ {filename} replaces {old_info['old_filename']} "
                    f"(setId …{set_id[-8:]})"
                )
            else:
                stats["new"] += 1
                log.info(f"  ★ {filename} — new record (setId …{(set_id or 'none')[-8:]})")

            await write_doc(
                conn, doc_id, source_path, filename,
                doc_type, title, content_hash, text,
                metadata, chunks, all_embeddings, medical_meta,
                enriched_texts=enriched_texts,
            )

    stats["total"] += 1


async def ingest_pdf(pool, embedder, pdf_path, stats):
    source_path = str(pdf_path)
    filename = pdf_path.name

    existing = await pool.fetchrow(
        "SELECT id FROM core.documents WHERE source_path = $1", source_path
    )
    if existing:
        log.info(f"  Already ingested: {filename}")
        return

    log.info(f"  OCR extracting: {filename} ({pdf_path.stat().st_size:,} bytes)")
    text = extract_pdf_text(pdf_path)

    if not text or len(text.strip()) < 20:
        log.warning(f"  ✗ No text from OCR: {filename}")
        stats["failed"] += 1
        return

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    dup_row = await pool.fetchrow(
        "SELECT id, filename FROM core.documents WHERE content_hash = $1 AND domain = 'medical'",
        content_hash,
    )
    if dup_row:
        log.info(f"  ⊘ Content duplicate of {dup_row['filename']}: {filename}")
        stats["skipped_dup"] += 1
        return

    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        log.warning(f"  ✗ No chunks: {filename}")
        stats["failed"] += 1
        return

    doc_type = auto_categorize_medical(text, filename)
    title = pdf_path.stem.replace("_", " ").replace("-", " ")
    doc_id = str(uuid.uuid4())

    # Contextual Retrieval — enrich chunks before embedding
    cr_context = generate_context_sync(
        title=title,
        domain="medical",
        document_type=doc_type,
        content_preview=text[:3000],
        case_number=None,
    )
    enriched_texts = enrich_chunks(cr_context, chunks)

    log.info(f"  {len(chunks)} chunks, embedding...")
    all_embeddings = []
    for i in range(0, len(enriched_texts), 100):
        batch = enriched_texts[i:i + 100]
        try:
            embs = await embedder.embed_batch(batch)
        except Exception as batch_err:
            log.warning(f"  Batch {i//100} failed ({batch_err}), falling back to per-item...")
            embs = []
            for chunk in batch:
                try:
                    single = await embedder.embed_batch([chunk[:15000]])
                    embs.extend(single)
                except Exception:
                    embs.append([0.0] * EMBEDDING_DIMENSIONS)
        all_embeddings.extend(embs)

    metadata = {
        "file_type": "pdf",
        "ocr": True,
        "source": "email_attachment",
        "export": EXPORT_LABEL,
        "email_from": "william@seattleseahawks.me",
        "dropped_at": datetime.now().isoformat(),
    }

    medical_meta = {
        "patient_name": "William Nelson",
        "record_type": doc_type,
        "date_of_service": extract_date_from_text(text, filename),
        "provider": "",
        "facility": "UW Medicine",
    }

    async with pool.acquire() as conn:
        async with conn.transaction():
            await write_doc(
                conn, doc_id, source_path, filename,
                doc_type, title, content_hash, text,
                metadata, chunks, all_embeddings, medical_meta,
                enriched_texts=enriched_texts,
            )

    stats["total"] += 1
    stats["new"] += 1
    log.info(f"  ✔ PDF ingested: {filename} ({len(chunks)} chunks)")


async def main():
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        sys.exit(1)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    embedder = EmbeddingClient()

    before_count = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'"
    )
    log.info(f"Medical docs before: {before_count}")

    old_map = await build_old_setid_map_from_db(pool)

    new_xmls = sorted(NEW_EXPORT.glob("DOC*.XML"))
    log.info(f"New CCDA files to process: {len(new_xmls)}")

    pdfs = sorted(PDF_DIR.glob("*.PDF")) + sorted(PDF_DIR.glob("*.pdf"))
    log.info(f"PDFs to process: {len(pdfs)}")

    stats = {"total": 0, "updated": 0, "new": 0, "failed": 0, "skipped_dup": 0}

    log.info("=" * 60)
    log.info("Ingesting CCDA XMLs...")
    log.info("=" * 60)
    for xml_path in new_xmls:
        try:
            await ingest_ccda(pool, embedder, xml_path, old_map, stats)
        except Exception as e:
            log.error(f"  ✗ {xml_path.name}: {e}")
            stats["failed"] += 1

    log.info("=" * 60)
    log.info("Ingesting PDFs...")
    log.info("=" * 60)
    for pdf_path in pdfs:
        try:
            await ingest_pdf(pool, embedder, pdf_path, stats)
        except Exception as e:
            log.error(f"  ✗ {pdf_path.name}: {e}")
            stats["failed"] += 1

    after_count = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'"
    )

    await embedder.close()
    await pool.close()

    log.info("=" * 60)
    log.info(f"DONE — medical docs: {before_count} → {after_count}")
    log.info(f"  updated (replaced): {stats['updated']}")
    log.info(f"  new:                {stats['new']}")
    log.info(f"  skipped (dup):      {stats['skipped_dup']}")
    log.info(f"  failed:             {stats['failed']}")
    log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
