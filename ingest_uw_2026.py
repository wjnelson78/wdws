#!/usr/bin/env python3
"""
Batch ingest UW Medicine HealthSummary export (Mar 06, 2026).

Handles:
  - 101 CCDA XML docs with setId-based dedup against 98 existing records
    → 97 updates (DELETE old → INSERT new), 4 truly new
  - 2 PDFs with page-at-a-time OCR (no full-document load)
  - Skips non-document files (HTML, CSS, images, XSL, METADATA.XML, etc.)
  - Stores ccda_set_id in metadata for future dedup
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
from typing import Dict, List, Optional, Tuple

import asyncpg
import httpx

# ── reuse extraction / chunking from dropbox_watcher ──
sys.path.insert(0, "/opt/wdws")
from dropbox_watcher import (
    extract_pdf_text,
    extract_ccda_text,
    extract_image_text,
    TextChunker,
    auto_categorize_medical,
    extract_date_from_text,
    EMBEDDING_MODEL,
    EMBEDDING_DIMENSIONS,
    CHUNK_SIZE,
    CHUNK_OVERLAP,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

NEW_EXPORT = Path(
    "/opt/wdws/data/dropbox/medical/HealthSummary_Mar_06_2026/IHE_XDM/William1"
)
OLD_EXPORT = Path(
    "/opt/wdws/data/medical/records/records/exports/IHE_XDM/William1"
)
PDF_DIR = Path(
    "/opt/wdws/data/dropbox/medical/HealthSummary_Mar_06_2026"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("uw-ingest")


# ============================================================
# Helpers
# ============================================================

HL7_NS = {"hl7": "urn:hl7-org:v3"}


def get_ccda_set_id(xml_path: Path) -> Optional[str]:
    """Extract setId extension attribute from a CCDA document."""
    try:
        tree = ET.parse(str(xml_path))
        root = tree.getroot()
        sid = root.find("hl7:setId", HL7_NS)
        if sid is not None:
            # extension is the unique per-document UUID; root is the system OID
            return sid.get("extension") or sid.get("root")
    except Exception:
        pass
    return None


def get_ccda_version(xml_path: Path) -> Optional[int]:
    """Extract versionNumber from CCDA."""
    try:
        tree = ET.parse(str(xml_path))
        root = tree.getroot()
        v = root.find("hl7:versionNumber", HL7_NS)
        if v is not None:
            return int(v.get("value", "1"))
    except Exception:
        pass
    return None


def get_ccda_effective_time(xml_path: Path) -> Optional[str]:
    """Extract effectiveTime value."""
    try:
        tree = ET.parse(str(xml_path))
        root = tree.getroot()
        et = root.find("hl7:effectiveTime", HL7_NS)
        if et is not None:
            return et.get("value", "")
    except Exception:
        pass
    return None


def _vec_literal(embedding: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"


class EmbeddingClient:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=120.0)
        self.total_tokens = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        truncated = [t[:30000] for t in texts]
        resp = await self.client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "input": truncated,
                "model": EMBEDDING_MODEL,
                "dimensions": EMBEDDING_DIMENSIONS,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self.total_tokens += data.get("usage", {}).get("total_tokens", 0)
        return [d["embedding"] for d in data["data"]]

    async def close(self):
        await self.client.aclose()


# ============================================================
# Build setId → DB document_id mapping for OLD records
# ============================================================

async def build_old_setid_map(pool: asyncpg.Pool) -> Dict[str, dict]:
    """
    Returns {setId: {"doc_uuid": ..., "old_source": ..., "old_filename": ...}}
    by cross-referencing old XML files on disk with DB records.
    """
    mapping: Dict[str, dict] = {}

    old_files = sorted(OLD_EXPORT.glob("DOC*.XML"))
    log.info(f"Scanning {len(old_files)} old CCDA files for setIds...")

    for xml_path in old_files:
        set_id = get_ccda_set_id(xml_path)
        if not set_id:
            continue

        source_path = str(xml_path)
        row = await pool.fetchrow(
            "SELECT id FROM core.documents WHERE source_path = $1",
            source_path,
        )
        if row:
            mapping[set_id] = {
                "doc_uuid": row["id"],
                "old_source": source_path,
                "old_filename": xml_path.name,
            }
        else:
            log.debug(f"  Old file {xml_path.name} not in DB (set_id={set_id[:12]}…)")

    log.info(f"Mapped {len(mapping)} old setIds to DB records")
    return mapping


# ============================================================
# Delete old document (cascading chunks + medical metadata)
# ============================================================

async def delete_document(conn, doc_uuid):
    """Delete a document and its chunks/metadata."""
    await conn.execute(
        "DELETE FROM core.document_chunks WHERE document_id = $1", doc_uuid
    )
    await conn.execute(
        "DELETE FROM medical.record_metadata WHERE document_id = $1", doc_uuid
    )
    await conn.execute("DELETE FROM core.documents WHERE id = $1", doc_uuid)


# ============================================================
# Write a new document (with ccda_set_id in metadata)
# ============================================================

async def write_doc(conn, doc_id: str, domain: str, source_path: str,
                    filename: str, doc_type: str, title: str,
                    content_hash: str, full_content: str,
                    metadata: dict, chunks: List[str],
                    embeddings: List[List[float]],
                    medical_meta: Optional[dict] = None):
    """Insert document + chunks + medical enrichment in one transaction."""

    await conn.execute("""
        INSERT INTO core.documents
            (id, domain, source_path, filename, document_type, title,
             content_hash, total_chunks, full_content, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        ON CONFLICT (id) DO NOTHING
    """,
        uuid.UUID(doc_id), domain, source_path,
        filename, doc_type, title,
        content_hash, len(chunks),
        full_content[:500000],
        json.dumps(metadata),
    )

    for i, (chunk_text, emb) in enumerate(zip(chunks, embeddings)):
        chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
        await conn.execute("""
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks,
                 content, embedding, metadata)
            VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
            ON CONFLICT (id) DO NOTHING
        """,
            chunk_id, uuid.UUID(doc_id),
            i, len(chunks),
            chunk_text, _vec_literal(emb),
            json.dumps({}),
        )

    if domain == "medical" and medical_meta:
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
                "source": "dropbox",
                "export": "HealthSummary_Mar_06_2026",
                "ingested_at": datetime.now().isoformat(),
            }),
        )


# ============================================================
# Ingest one CCDA file
# ============================================================

async def ingest_ccda(pool: asyncpg.Pool, embedder: EmbeddingClient,
                      xml_path: Path, old_map: Dict[str, dict],
                      stats: dict):
    """Ingest a single CCDA XML, updating if setId overlaps."""

    set_id = get_ccda_set_id(xml_path)
    version = get_ccda_version(xml_path) or 1
    eff_time = get_ccda_effective_time(xml_path) or ""
    source_path = str(xml_path)
    filename = xml_path.name

    # Extract text
    text = extract_ccda_text(xml_path)
    if not text or len(text.strip()) < 20:
        log.warning(f"  ✗ No text: {filename}")
        stats["failed"] += 1
        return

    # Chunk + embed
    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        log.warning(f"  ✗ No chunks: {filename}")
        stats["failed"] += 1
        return

    all_embeddings = []
    for i in range(0, len(chunks), 100):
        batch = chunks[i : i + 100]
        embs = await embedder.embed_batch(batch)
        all_embeddings.extend(embs)

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    doc_type = auto_categorize_medical(text, filename)
    title = Path(filename).stem.replace("_", " ")
    doc_id = str(uuid.uuid4())

    metadata = {
        "file_type": "ccda",
        "ccda_set_id": set_id,
        "ccda_version": version,
        "effective_time": eff_time,
        "sections": len(chunks),
        "source": "samba_dropbox",
        "export": "HealthSummary_Mar_06_2026",
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
            # Check for overlap — if setId matches an old record, delete old first
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
                conn, doc_id, "medical", source_path, filename,
                doc_type, title, content_hash, text,
                metadata, chunks, all_embeddings, medical_meta,
            )

    stats["total"] += 1


# ============================================================
# Ingest PDFs (with page-at-a-time OCR)
# ============================================================

async def ingest_pdf(pool: asyncpg.Pool, embedder: EmbeddingClient,
                     pdf_path: Path, stats: dict):
    """Ingest a PDF using page-at-a-time OCR."""
    source_path = str(pdf_path)
    filename = pdf_path.name

    # Check if already ingested by source_path
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

    # Check content hash against existing PDFs
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

    log.info(f"  {len(chunks)} chunks, embedding...")

    all_embeddings = []
    for i in range(0, len(chunks), 100):
        batch = chunks[i : i + 100]
        embs = await embedder.embed_batch(batch)
        all_embeddings.extend(embs)

    doc_type = auto_categorize_medical(text, filename)
    title = pdf_path.stem.replace("_", " ").replace("-", " ")
    doc_id = str(uuid.uuid4())

    metadata = {
        "file_type": "pdf",
        "ocr": True,
        "source": "samba_dropbox",
        "export": "HealthSummary_Mar_06_2026",
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
                conn, doc_id, "medical", source_path, filename,
                doc_type, title, content_hash, text,
                metadata, chunks, all_embeddings, medical_meta,
            )

    stats["total"] += 1
    stats["new"] += 1
    log.info(f"  ✔ PDF ingested: {filename} ({len(chunks)} chunks)")


# ============================================================
# Main
# ============================================================

async def main():
    if not OPENAI_API_KEY:
        log.error("OPENAI_API_KEY not set")
        sys.exit(1)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    embedder = EmbeddingClient()

    # ── Count existing records ──
    before_count = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'"
    )
    log.info(f"Medical docs before: {before_count}")

    # ── Build old setId map ──
    old_map = await build_old_setid_map(pool)

    # ── Gather new CCDA files ──
    new_xmls = sorted(NEW_EXPORT.glob("DOC*.XML"))
    log.info(f"New CCDA files to process: {len(new_xmls)}")

    # ── Gather PDFs ──
    pdfs = sorted(PDF_DIR.glob("*.PDF")) + sorted(PDF_DIR.glob("*.pdf"))
    log.info(f"PDFs to process: {len(pdfs)}")

    stats = {"total": 0, "updated": 0, "new": 0, "failed": 0, "skipped_dup": 0}

    # ── Ingest CCDAs ──
    log.info("=" * 60)
    log.info("Ingesting CCDA XMLs...")
    log.info("=" * 60)
    for xml_path in new_xmls:
        try:
            await ingest_ccda(pool, embedder, xml_path, old_map, stats)
        except Exception as e:
            log.error(f"  ✗ {xml_path.name}: {e}")
            stats["failed"] += 1

    # ── Ingest PDFs ──
    log.info("=" * 60)
    log.info("Ingesting PDFs (page-at-a-time OCR)...")
    log.info("=" * 60)
    for pdf_path in pdfs:
        try:
            await ingest_pdf(pool, embedder, pdf_path, stats)
        except Exception as e:
            log.error(f"  ✗ {pdf_path.name}: {e}")
            stats["failed"] += 1

    # ── Summary ──
    after_count = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'"
    )

    log.info("=" * 60)
    log.info("INGESTION COMPLETE")
    log.info(f"  Documents processed : {stats['total']}")
    log.info(f"    Updated (replaced): {stats['updated']}")
    log.info(f"    New (inserted)    : {stats['new']}")
    log.info(f"    Failed            : {stats['failed']}")
    log.info(f"    Skipped (dup hash): {stats['skipped_dup']}")
    log.info(f"  DB medical docs     : {before_count} → {after_count}")
    log.info(f"  Embedding tokens    : {embedder.total_tokens:,}")
    log.info("=" * 60)

    await embedder.close()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
