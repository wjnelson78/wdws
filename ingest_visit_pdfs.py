#!/usr/bin/env python3
"""
Ingest per-visit PDFs split from Epic "My Health Summary" monolith exports.

Replaces the prior whole-PDF ingestion for:
  - Providence Swedish Apr 20 2026 (2 monolith PDFs → 107 per-visit PDFs)
  - UW Medicine Feb 6 2026       (1 monolith PDF  →  98 per-visit PDFs)

Deletes the monolith rows from core.documents (keeps the file on disk).
Each visit PDF is linked back to its parent monolith via
  metadata.parent_pdf_path / parent_pdf_pages so the user can open the
  original for a given slice when needed.

CCDA XML rows are left alone — they provide structured data the PDFs lack
and the PDFs provide narrative notes the CCDAs lack, so both coexist.
"""

import asyncio
import csv
import hashlib
import json
import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import asyncpg
import pypdf

sys.path.insert(0, "/opt/wdws")
from dropbox_watcher import (
    TextChunker,
    ensure_patient_exists,
)
from contextual_retrieval import generate_context_sync, enrich_chunks
from embedding_service import embed_texts_sync

# Map Epic encounter_type strings to record_type values already used in
# medical.record_metadata (progress_note, prescription, radiology, lab_result,
# other, medical_export). Unknown types fall through to "other".
ENCOUNTER_TYPE_TO_RECORD_TYPE = {
    "Emergency": "progress_note",
    "Hospital Encounter": "progress_note",
    "Office Visit": "progress_note",
    "Virtual Office Visit": "progress_note",
    "Telemedicine": "progress_note",
    "Home Care Visit": "progress_note",
    "Surgery": "progress_note",
    "Procedure": "progress_note",
    "Anesthesia": "progress_note",
    "Appointment": "progress_note",
    "Urgent Care": "progress_note",
    "Observation": "progress_note",
    "Admission": "progress_note",
    "Consult": "progress_note",
    "Infusion": "progress_note",
    "Sleep Study": "progress_note",
    "Dialysis": "progress_note",
    "Nurse Triage": "progress_note",
    "Clinical Support": "progress_note",
    "Clinic Outreach": "progress_note",
    "Video Visit": "progress_note",
    "Telephone": "other",
    "Phone Call": "other",
    "Patient Message": "other",
    "Patient E-mail": "other",
    "E-Visit": "other",
    "Refill": "prescription",
    "Refill Request": "prescription",
    "Imaging": "radiology",
    "Scan": "radiology",
    "Lab Results": "lab_result",
}

# Doc-types for the two rollup summary PDFs at the head of each export.
# These aren't individual visits — they're snapshots of current state.
ROLLUP_DOC_TYPES = {"Patient Health Summary", "Continuity of Care Document"}


def record_type_for_visit(visit: dict) -> str:
    """Decide the medical.record_metadata.record_type for a visit row."""
    if visit.get("doc_type") in ROLLUP_DOC_TYPES:
        return "medical_export"
    enc = (visit.get("encounter_type") or "").strip()
    return ENCOUNTER_TYPE_TO_RECORD_TYPE.get(enc, "other")


_FACILITY_STOPWORDS = {"and", "of", "the", "in", "at", "for", "to", "with", "on"}
_FACILITY_KEEP_UPPER = {
    # Washington-area health-system acronyms we see in these exports
    "UW", "UWMC", "HMC", "PMG", "SHS", "ED", "ER", "ICU", "OR",
    "CT", "MRI", "XRAY", "MP", "CORP", "HC", "BIIA",
}


def normalize_facility(raw: str, default: str) -> str:
    """Title-case all-caps facility strings while preserving known acronyms."""
    if not raw:
        return default
    s = raw.strip()
    if not s:
        return default
    if s != s.upper():
        return s  # already mixed case — leave alone
    out = []
    for t in s.split():
        if t in _FACILITY_KEEP_UPPER:
            out.append(t)
        elif t.lower() in _FACILITY_STOPWORDS:
            out.append(t.lower())
        else:
            out.append(t.capitalize())
    return " ".join(out)

DATABASE_URL = os.environ["DATABASE_URL"]

SOURCES = [
    {
        "label": "HealthSummary_Apr_20_2026",
        "facility_default": "Providence Swedish",
        "visits_dir": Path(
            "/opt/wdws/data/medical/records/records/swedish_providence_2026_04_20/visits"
        ),
        "parents": {
            "part-1": "/opt/wdws/data/medical/records/records/swedish_providence_2026_04_20/1 of 2 - My Health Summary.PDF",
            "part-2": "/opt/wdws/data/medical/records/records/swedish_providence_2026_04_20/2 of 2 - My Health Summary.PDF",
        },
    },
    {
        "label": "UW_HealthSummary_Feb_06_2026",
        "facility_default": "UW Medicine",
        "visits_dir": Path("/opt/wdws/data/medical/records/records/exports/visits"),
        "parents": {
            "uw-medicine-2026-02-06": "/opt/wdws/data/medical/records/records/exports/1 of 1 - My Health Summary.PDF",
        },
    },
    {
        "label": "UW_HealthSummary_Apr_21_2026",
        "facility_default": "UW Medicine",
        "visits_dir": Path(
            "/opt/wdws/data/medical/records/records/uw_medicine_2026_04_21/visits"
        ),
        "parents": {
            "part-1": "/opt/wdws/data/medical/records/records/uw_medicine_2026_04_21/1 of 2 - My Health Summary.PDF",
            "part-2": "/opt/wdws/data/medical/records/records/uw_medicine_2026_04_21/2 of 2 - My Health Summary.PDF",
        },
    },
    {
        "label": "EvergreenHealth_Apr_21_2026",
        "facility_default": "EvergreenHealth",
        "visits_dir": Path(
            "/opt/wdws/data/medical/records/records/evergreenhealth_2026_04_21/visits"
        ),
        "parents": {
            "part-1": "/opt/wdws/data/medical/records/records/evergreenhealth_2026_04_21/1 of 2 - My Health Summary.PDF",
            "part-2": "/opt/wdws/data/medical/records/records/evergreenhealth_2026_04_21/2 of 2 - My Health Summary.PDF",
        },
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("visit-ingest")


def _vec_literal(emb: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in emb) + "]"


def extract_pdf_text_fast(pdf_path: Path) -> str:
    """Direct text-layer extraction. These Epic PDFs carry text, no OCR needed."""
    r = pypdf.PdfReader(str(pdf_path))
    parts = []
    for i, p in enumerate(r.pages, 1):
        try:
            t = p.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            parts.append(f"[Page {i}]\n{t}")
    return "\n\n".join(parts)


async def delete_monolith(pool, source_path: str) -> Optional[str]:
    """Delete an existing monolith PDF row + its chunks + medical metadata."""
    row = await pool.fetchrow(
        "SELECT id, filename FROM core.documents WHERE source_path = $1",
        source_path,
    )
    if not row:
        return None
    doc_id = row["id"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM core.document_chunks WHERE document_id = $1", doc_id
            )
            await conn.execute(
                "DELETE FROM medical.record_metadata WHERE document_id = $1", doc_id
            )
            await conn.execute("DELETE FROM core.documents WHERE id = $1", doc_id)
    log.info(f"  ✗ removed monolith: {row['filename']} ({doc_id})")
    return str(doc_id)


def build_title(visit: dict, source: dict) -> str:
    dt = visit.get("doc_type") or "Summary of Care"
    if dt in ROLLUP_DOC_TYPES:
        return f"{dt} — {source['facility_default']} — {visit.get('encounter_date') or ''}".rstrip(" —")
    date = visit.get("encounter_date") or "unknown-date"
    enc = visit.get("encounter_type") or ""
    fac = normalize_facility(visit.get("facility") or "", source["facility_default"])
    pieces = [p for p in (date, enc, fac) if p]
    return " — ".join(pieces)


async def ingest_visit(
    pool: asyncpg.Pool,
    visit: dict,
    pdf_path: Path,
    source: dict,
) -> str:
    """Ingest a single per-visit PDF. Returns one of: ok, skip, empty, fail."""
    source_path = str(pdf_path)

    # Idempotency: if already ingested at this source_path, skip
    existing = await pool.fetchrow(
        "SELECT id FROM core.documents WHERE source_path = $1", source_path
    )
    if existing:
        return "skip"

    text = extract_pdf_text_fast(pdf_path)
    if not text or len(text.strip()) < 20:
        return "empty"

    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        return "empty"

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    record_type = record_type_for_visit(visit)
    # core.documents.document_type carries the same value — medical domain uses
    # record_type conventions across the whole system.
    doc_type = record_type
    title = build_title(visit, source)
    doc_id = str(uuid.uuid4())

    facility_for_provider = normalize_facility(
        visit.get("facility") or "", source["facility_default"]
    )

    # Contextual retrieval enrichment
    cr_context = generate_context_sync(
        title=title,
        domain="medical",
        document_type=doc_type,
        content_preview=text[:3000],
        case_number=None,
    )
    enriched_texts = enrich_chunks(cr_context, chunks)

    # Embed
    all_embeddings: List[List[float]] = []
    for i in range(0, len(enriched_texts), 100):
        batch = enriched_texts[i : i + 100]
        embs = embed_texts_sync(batch)
        all_embeddings.extend(embs)

    # Metadata: link back to parent monolith + manifest row
    parent_key = visit.get("source_pdf") or ""
    parent_path = source["parents"].get(parent_key) or next(
        iter(source["parents"].values())
    )
    is_rollup = visit.get("doc_type") in ROLLUP_DOC_TYPES
    metadata = {
        "file_type": "pdf",
        "visit_pdf": True,
        "is_rollup_summary": is_rollup,
        "export": source["label"],
        "source": "split_from_monolith",
        "parent_pdf_path": parent_path,
        "parent_pdf_pages": [
            int(visit.get("source_page_start") or 0),
            int(visit.get("source_page_end") or 0),
        ],
        "visit_seq": visit.get("seq"),
        "doc_type_label": visit.get("doc_type"),
        "encounter_date": visit.get("encounter_date") or None,
        "encounter_type": visit.get("encounter_type") or None,
        "facility_raw": visit.get("facility") or None,
        "facility_normalized": facility_for_provider,
        "ingested_at": datetime.now().isoformat(),
    }

    # Medical metadata
    dos = visit.get("encounter_date")
    dos_date = None
    if dos:
        try:
            dos_date = datetime.strptime(dos, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            dos_date = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            patient_id = await ensure_patient_exists(conn, "William Nelson")

            provider_id = None
            # We don't parse provider off the visit PDF yet — leave null rather
            # than inventing one. Provider can be backfilled from CCDA.

            await conn.execute(
                """
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                uuid.UUID(doc_id),
                "medical",
                source_path,
                pdf_path.name,
                doc_type,
                title,
                content_hash,
                len(chunks),
                text[:500000],
                json.dumps(metadata),
            )

            for i, (chunk_text, emb) in enumerate(zip(chunks, all_embeddings)):
                chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
                embedded_content = (
                    enriched_texts[i] if enriched_texts else chunk_text
                )
                await conn.execute(
                    """
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, embedded_content, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7, $8::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    chunk_id,
                    uuid.UUID(doc_id),
                    i,
                    len(chunks),
                    chunk_text,
                    _vec_literal(emb),
                    embedded_content,
                    json.dumps({}),
                )

            await conn.execute(
                """
                INSERT INTO medical.record_metadata
                    (document_id, patient_id, provider_id, record_type,
                     date_of_service, facility, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                ON CONFLICT (document_id) DO NOTHING
                """,
                uuid.UUID(doc_id),
                patient_id,
                provider_id,
                record_type,
                dos_date,
                facility_for_provider,
                json.dumps(
                    {
                        "source": "visit_pdf",
                        "export": source["label"],
                        "encounter_type": visit.get("encounter_type"),
                        "parent_pdf_path": parent_path,
                        "parent_pdf_pages": [
                            int(visit.get("source_page_start") or 0),
                            int(visit.get("source_page_end") or 0),
                        ],
                    }
                ),
            )

    return "ok"


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    before = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain='medical'"
    )
    log.info(f"medical docs before: {before}")

    # Step 1 — delete monolith PDF rows (file on disk is preserved)
    log.info("=" * 60)
    log.info("Removing monolith PDF rows from DB...")
    log.info("=" * 60)
    removed = 0
    for source in SOURCES:
        for parent_path in source["parents"].values():
            if await delete_monolith(pool, parent_path):
                removed += 1
    log.info(f"removed {removed} monolith rows")

    # Step 2 — ingest per-visit PDFs from each manifest
    log.info("=" * 60)
    log.info("Ingesting per-visit PDFs...")
    log.info("=" * 60)

    stats = {"ok": 0, "skip": 0, "empty": 0, "fail": 0}

    for source in SOURCES:
        manifest = source["visits_dir"] / "visits_manifest.csv"
        if not manifest.exists():
            log.warning(f"manifest missing: {manifest}")
            continue
        with open(manifest) as f:
            rows = list(csv.DictReader(f))
        log.info(f"[{source['label']}] {len(rows)} visits")
        for j, row in enumerate(rows, 1):
            pdf_path = source["visits_dir"] / row["output_file"]
            if not pdf_path.exists():
                log.warning(f"  missing file: {pdf_path}")
                stats["fail"] += 1
                continue
            try:
                result = await ingest_visit(pool, row, pdf_path, source)
                stats[result] += 1
                mark = {"ok": "✔", "skip": "⊘", "empty": "∅", "fail": "✗"}[result]
                log.info(
                    f"  {mark} [{j:3d}/{len(rows)}] {row['output_file']}"
                )
            except Exception as e:
                log.error(f"  ✗ [{j:3d}/{len(rows)}] {row['output_file']}: {e}")
                stats["fail"] += 1

    after = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain='medical'"
    )

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  ingested:        {stats['ok']}")
    log.info(f"  skipped (dup):   {stats['skip']}")
    log.info(f"  empty text:      {stats['empty']}")
    log.info(f"  failed:          {stats['fail']}")
    log.info(f"  medical docs:    {before} → {after}")
    log.info("=" * 60)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
