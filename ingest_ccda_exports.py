#!/usr/bin/env python3
"""
Ingest CCDA XML exports into core.documents + medical.record_metadata.

Registry-based: add new Epic exports to EXPORTS below and re-run.
Uses setId dedup — if a setId already exists in the DB (medical domain),
the old row is deleted and the new version replaces it.

This supersedes the per-export scripts
  (ingest_providence_swedish_2026_04_20.py,
   ingest_uw_2026.py,
   ingest_uw_20260317.py)
for future exports. Existing records stay in place; re-running this does
not re-process setIds unless their version has changed (setId dedup only
triggers on explicit match → delete-then-insert).

Usage:
    python ingest_ccda_exports.py                # all exports in registry
    python ingest_ccda_exports.py <key> [<key>]  # selected keys only
"""

import asyncio
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

sys.path.insert(0, "/opt/wdws")
from dropbox_watcher import (
    TextChunker,
    extract_ccda_text,
    extract_date_from_text,
    ensure_patient_exists,
)
from contextual_retrieval import generate_context_sync, enrich_chunks
from embedding_service import embed_texts_sync
from ingest_uw_2026 import (
    get_ccda_set_id,
    get_ccda_version,
    get_ccda_effective_time,
)

DATABASE_URL = os.environ["DATABASE_URL"]

EXPORTS: Dict[str, dict] = {
    "uw_medicine_2026_04_21": {
        "label": "UW_HealthSummary_Apr_21_2026",
        "facility_default": "UW Medicine",
        "ccda_dir": Path(
            "/opt/wdws/data/medical/records/records/uw_medicine_2026_04_21/IHE_XDM/William1"
        ),
        "patient_name": "William Nelson",
        "provenance": {
            "source": "local_zip",
            "zip_filename": "HealthSummary_Apr_21_2026.zip",
        },
    },
    "evergreenhealth_2026_04_21": {
        "label": "EvergreenHealth_Apr_21_2026",
        "facility_default": "EvergreenHealth",
        "ccda_dir": Path(
            "/opt/wdws/data/medical/records/records/evergreenhealth_2026_04_21/IHE_XDM/William1"
        ),
        "patient_name": "William Nelson",
        "provenance": {
            "source": "local_zip",
            "zip_filename": "HealthSummary_Apr_21_2026_Evergeenhealth.zip",
        },
    },
}

# All CCDAs from a healthcare-system export are visit-level Summary of Care
# documents. We map them to "progress_note" (the record_type already used for
# clinical visit notes in the DB) rather than the misleading values
# auto_categorize_medical tends to return on CCDA narrative blocks.
CCDA_RECORD_TYPE = "progress_note"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ccda-ingest")


def _vec_literal(emb: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in emb) + "]"


async def build_setid_map(pool) -> Dict[str, dict]:
    """Return {setId: {doc_uuid, filename}} for medical CCDAs already in DB."""
    rows = await pool.fetch(
        """
        SELECT id, filename, metadata->>'ccda_set_id' AS set_id
        FROM core.documents
        WHERE domain = 'medical'
          AND metadata->>'ccda_set_id' IS NOT NULL
        """
    )
    return {
        r["set_id"]: {"doc_uuid": str(r["id"]), "old_filename": r["filename"]}
        for r in rows
        if r["set_id"]
    }


async def delete_document(conn, doc_uuid):
    doc_uuid = uuid.UUID(doc_uuid) if isinstance(doc_uuid, str) else doc_uuid
    await conn.execute(
        "DELETE FROM core.document_chunks WHERE document_id = $1", doc_uuid
    )
    await conn.execute(
        "DELETE FROM medical.record_metadata WHERE document_id = $1", doc_uuid
    )
    await conn.execute("DELETE FROM core.documents WHERE id = $1", doc_uuid)


async def ingest_ccda(
    pool: asyncpg.Pool,
    xml_path: Path,
    setid_map: Dict[str, dict],
    export: dict,
    stats: dict,
) -> str:
    """Ingest a single CCDA XML. Returns: new, updated, empty, fail."""
    set_id = get_ccda_set_id(xml_path)
    version = get_ccda_version(xml_path) or 1
    eff_time = get_ccda_effective_time(xml_path) or ""
    source_path = str(xml_path)
    filename = xml_path.name

    text = extract_ccda_text(xml_path)
    if not text or len(text.strip()) < 20:
        return "empty"

    chunker = TextChunker()
    chunks = chunker.split(text)
    if not chunks:
        return "empty"

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    record_type = CCDA_RECORD_TYPE
    doc_type = record_type
    title = Path(filename).stem.replace("_", " ")
    doc_id = str(uuid.uuid4())

    cr_context = generate_context_sync(
        title=title,
        domain="medical",
        document_type=doc_type,
        content_preview=text[:3000],
        case_number=None,
    )
    enriched_texts = enrich_chunks(cr_context, chunks)

    all_embeddings: List[List[float]] = []
    for i in range(0, len(enriched_texts), 100):
        embs = embed_texts_sync(enriched_texts[i : i + 100])
        all_embeddings.extend(embs)

    dos = extract_date_from_text(text, filename)
    dos_date = None
    if dos:
        try:
            dos_date = datetime.strptime(dos, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            pass

    metadata = {
        "file_type": "ccda",
        "ccda_set_id": set_id,
        "ccda_version": version,
        "effective_time": eff_time,
        "sections": len(chunks),
        "export": export["label"],
        "ingested_at": datetime.now().isoformat(),
        **export.get("provenance", {}),
    }

    medical_meta_extra = {
        "source": "ccda_xml",
        "export": export["label"],
        "ccda_set_id": set_id,
        **export.get("provenance", {}),
    }

    action = "new"
    async with pool.acquire() as conn:
        async with conn.transaction():
            if set_id and set_id in setid_map:
                old = setid_map[set_id]
                await delete_document(conn, old["doc_uuid"])
                action = "updated"

            patient_id = await ensure_patient_exists(conn, export["patient_name"])

            await conn.execute(
                """
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, 'medical', $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                uuid.UUID(doc_id), source_path, filename, doc_type, title,
                content_hash, len(chunks), text[:500000], json.dumps(metadata),
            )

            for i, (chunk_text, emb) in enumerate(zip(chunks, all_embeddings)):
                chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
                embedded_content = enriched_texts[i] if enriched_texts else chunk_text
                await conn.execute(
                    """
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, embedded_content, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7, $8::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    chunk_id, uuid.UUID(doc_id), i, len(chunks),
                    chunk_text, _vec_literal(emb), embedded_content,
                    json.dumps({}),
                )

            await conn.execute(
                """
                INSERT INTO medical.record_metadata
                    (document_id, patient_id, provider_id, record_type,
                     date_of_service, facility, metadata)
                VALUES ($1, $2, NULL, $3, $4, $5, $6::jsonb)
                ON CONFLICT (document_id) DO NOTHING
                """,
                uuid.UUID(doc_id), patient_id, record_type,
                dos_date, export["facility_default"],
                json.dumps(medical_meta_extra),
            )

    stats[action] = stats.get(action, 0) + 1
    return action


async def ingest_export(pool, key: str, export: dict):
    ccda_dir = export["ccda_dir"]
    xmls = sorted(ccda_dir.glob("DOC*.XML"))
    if not xmls:
        log.warning(f"[{key}] no DOC*.XML found in {ccda_dir}")
        return
    log.info(f"[{key}] {len(xmls)} CCDA files")
    setid_map = await build_setid_map(pool)
    log.info(f"[{key}] {len(setid_map)} existing CCDAs in DB for dedup")

    stats: Dict[str, int] = {"new": 0, "updated": 0, "empty": 0, "fail": 0}
    for j, xml_path in enumerate(xmls, 1):
        try:
            result = await ingest_ccda(pool, xml_path, setid_map, export, stats)
            mark = {"new": "★", "updated": "↻", "empty": "∅", "fail": "✗"}[result]
            set_id = get_ccda_set_id(xml_path) or "none"
            log.info(
                f"  {mark} [{j:3d}/{len(xmls)}] {xml_path.name} "
                f"(setId …{set_id[-8:] if set_id and set_id != 'none' else 'none'})"
            )
        except Exception as e:
            log.error(f"  ✗ [{j:3d}/{len(xmls)}] {xml_path.name}: {e}")
            stats["fail"] += 1

    log.info(
        f"[{key}] done: new={stats['new']} updated={stats['updated']} "
        f"empty={stats['empty']} fail={stats['fail']}"
    )


async def main():
    keys = sys.argv[1:] if len(sys.argv) > 1 else list(EXPORTS.keys())
    unknown = [k for k in keys if k not in EXPORTS]
    if unknown:
        log.error(f"unknown key(s): {unknown}. known: {list(EXPORTS.keys())}")
        sys.exit(2)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    before = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain='medical'"
    )
    log.info(f"medical docs before: {before}")

    for k in keys:
        await ingest_export(pool, k, EXPORTS[k])

    after = await pool.fetchval(
        "SELECT COUNT(*) FROM core.documents WHERE domain='medical'"
    )
    log.info(f"medical docs: {before} → {after}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
