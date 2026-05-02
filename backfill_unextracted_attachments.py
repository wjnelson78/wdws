#!/usr/bin/env python3
"""backfill_unextracted_attachments.py

Re-extract + re-embed specific attachment rows where extraction yielded
zero text. Uses the new xlsx/xls handlers in email_sync.extract_text_from_binary
and the existing PDF path (which now prefers sonnet_pdf over local OCR).

Targets: attachment IDs passed via --att-ids OR --mailbox + auto-discovery
of unextracted xlsx/xls/pdf rows for that mailbox.

For each target:
  1. Read binary from local_attachment_path (in child-doc metadata)
  2. Run extract_text_from_binary
  3. UPDATE legal.email_attachments (extracted_text, extraction_method, is_processed)
  4. UPDATE core.documents (full_content, content_hash, metadata.extraction_method)
  5. DELETE old core.document_chunks for child_doc_id
  6. Re-chunk + embed + insert new chunks
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import List, Optional

import asyncpg

sys.path.insert(0, "/opt/wdws")

from email_sync import extract_text_from_binary  # noqa: E402
from ingest import TextChunker  # noqa: E402
from embedding_service import (  # noqa: E402
    embed_texts_sync,
    get_or_create_embedding_model_id,
    _vec_literal as _embedding_vec_literal,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

DATABASE_URL = os.environ["DATABASE_URL"]


async def fetch_targets(
    conn: asyncpg.Connection,
    att_ids: Optional[List[str]],
    mailbox: Optional[str],
) -> list[dict]:
    """Resolve target attachments by explicit ID list or mailbox sweep."""
    if att_ids:
        rows = await conn.fetch(
            """
            SELECT ea.id AS att_id, ea.email_doc_id, ea.child_doc_id,
                   ea.filename, ea.content_type, ea.file_size,
                   cd.metadata->>'local_attachment_path' AS local_path,
                   cd.storage_uri,
                   cd.title AS child_title
            FROM legal.email_attachments ea
            JOIN core.documents cd ON cd.id = ea.child_doc_id
            WHERE ea.id = ANY($1::uuid[])
            """,
            [uuid.UUID(x) for x in att_ids],
        )
    elif mailbox:
        rows = await conn.fetch(
            """
            SELECT ea.id AS att_id, ea.email_doc_id, ea.child_doc_id,
                   ea.filename, ea.content_type, ea.file_size,
                   cd.metadata->>'local_attachment_path' AS local_path,
                   cd.storage_uri,
                   cd.title AS child_title
            FROM legal.email_attachments ea
            JOIN legal.email_metadata em ON em.document_id = ea.email_doc_id
            JOIN core.documents cd ON cd.id = ea.child_doc_id
            WHERE em.mailbox = $1
              AND (ea.extracted_text IS NULL OR LENGTH(ea.extracted_text) = 0)
              AND (
                ea.content_type IN (
                  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                  'application/vnd.ms-excel',
                  'application/pdf'
                )
              )
            ORDER BY ea.created_at
            """,
            mailbox,
        )
    else:
        return []
    return [dict(r) for r in rows]


def read_attachment_bytes(local_path: Optional[str]) -> Optional[bytes]:
    if not local_path:
        return None
    p = Path(local_path)
    if not p.exists():
        return None
    return p.read_bytes()


def parse_path_overrides(items: Optional[List[str]]) -> dict:
    """Accept ['att_id=/abs/path', ...] and return {att_id: path}."""
    out = {}
    for item in items or []:
        if "=" not in item:
            raise SystemExit(f"--path-override must be att_id=path, got: {item}")
        k, v = item.split("=", 1)
        out[k.strip()] = v.strip()
    return out


async def reembed_chunks(
    conn: asyncpg.Connection,
    child_doc_id: uuid.UUID,
    full_content: str,
    source_path: str,
    chunker: TextChunker,
    model_id: int,
) -> int:
    """Delete old chunks for child_doc_id and write new ones from full_content."""
    await conn.execute(
        "DELETE FROM core.document_chunks WHERE document_id = $1", child_doc_id
    )

    chunks = chunker.split(full_content)
    if not chunks:
        return 0

    embeddings = embed_texts_sync(chunks)
    total = len(chunks)
    for i, (text, emb) in enumerate(zip(chunks, embeddings)):
        chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
        emb_param = _embedding_vec_literal(emb) if emb is not None else None
        meta = {
            "source_part": "attachment_body",
            "is_embedded": True,
            "case_numbers": [],
            "backfill": "xlsx_pdf_reextract_2026_04_28",
        }
        await conn.execute(
            """
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks,
                 content, embedded_content, embedding, embedding_model_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec, $8, $9::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                content = EXCLUDED.content,
                embedded_content = EXCLUDED.embedded_content,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata
            """,
            chunk_id, child_doc_id, i, total, text, None,
            emb_param, model_id, json.dumps(meta),
        )
    return total


async def process_target(
    conn: asyncpg.Connection,
    target: dict,
    chunker: TextChunker,
    model_id: int,
    dry_run: bool,
    path_overrides: dict,
) -> dict:
    """Returns dict {att_id, status, chars, method, chunks}."""
    att_id = target["att_id"]
    filename = target["filename"]
    content_type = target["content_type"]
    local_path = target["local_path"]
    child_doc_id = target["child_doc_id"]

    override = path_overrides.get(str(att_id))
    if override:
        log.info("→ %s  %s  (%s)  [override path]", att_id, filename, content_type)
        local_path = override
    else:
        log.info("→ %s  %s  (%s)", att_id, filename, content_type)

    data = read_attachment_bytes(local_path)
    if data is None:
        return {"att_id": str(att_id), "status": "missing_binary",
                "chars": 0, "method": "", "chunks": 0,
                "filename": filename, "local_path": local_path}

    text, method = extract_text_from_binary(filename, content_type, data)
    chars = len(text or "")
    log.info("    extract: method=%s chars=%d", method, chars)

    if dry_run:
        return {"att_id": str(att_id), "status": "dry_run",
                "chars": chars, "method": method, "chunks": 0,
                "filename": filename}

    # Truncate to 100k for the attachment row (matches existing pipeline cap)
    truncated = (text or "")[:100000]

    async with conn.transaction():
        await conn.execute(
            """
            UPDATE legal.email_attachments
               SET extracted_text = $2,
                   extraction_method = $3,
                   is_processed = TRUE
             WHERE id = $1
            """,
            att_id, truncated, method,
        )

        # Update child doc full_content + metadata.extraction_method
        await conn.execute(
            """
            UPDATE core.documents
               SET full_content = $2,
                   content_hash = $3,
                   metadata = COALESCE(metadata, '{}'::jsonb) ||
                              jsonb_build_object(
                                'extraction_method', $4::text,
                                'reextracted_at', now()::text
                              ),
                   updated_at = now()
             WHERE id = $1
            """,
            child_doc_id,
            text or "",
            hashlib.sha256((text or "").encode("utf-8", errors="ignore")).hexdigest(),
            method,
        )

        # Re-chunk + re-embed
        source_path_row = await conn.fetchrow(
            "SELECT source_path FROM core.documents WHERE id = $1", child_doc_id
        )
        source_path = source_path_row["source_path"] if source_path_row else str(child_doc_id)
        n_chunks = await reembed_chunks(
            conn, child_doc_id, text or "", source_path, chunker, model_id
        )
        await conn.execute(
            "UPDATE core.documents SET total_chunks = $2 WHERE id = $1",
            child_doc_id, n_chunks,
        )

    return {"att_id": str(att_id), "status": "ok",
            "chars": chars, "method": method, "chunks": n_chunks,
            "filename": filename}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mailbox", help="Sweep all unextracted xlsx/xls/pdf for a mailbox")
    parser.add_argument("--att-ids", nargs="+", help="Specific attachment UUIDs")
    parser.add_argument("--path-override", action="append",
                        help="att_id=/abs/path to override the binary source for that att (repeatable)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.mailbox and not args.att_ids:
        parser.error("Pass --mailbox or --att-ids")

    path_overrides = parse_path_overrides(args.path_override)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    chunker = TextChunker()

    async with pool.acquire() as conn:
        targets = await fetch_targets(conn, args.att_ids, args.mailbox)
        log.info("Targets: %d", len(targets))

        model_id = await get_or_create_embedding_model_id(conn)
        log.info("Embedding model_id: %d", model_id)

        results = []
        for t in targets:
            try:
                r = await process_target(conn, t, chunker, model_id, args.dry_run,
                                         path_overrides)
            except Exception as e:
                log.exception("Failed on %s: %s", t.get("att_id"), e)
                r = {"att_id": str(t.get("att_id")), "status": "error",
                     "chars": 0, "method": "", "chunks": 0,
                     "filename": t.get("filename"), "error": str(e)[:200]}
            results.append(r)

    await pool.close()

    log.info("\n=== SUMMARY ===")
    for r in results:
        log.info("  %s  %s  chars=%d  method=%s  chunks=%d  %s",
                 r["status"], r["att_id"], r["chars"], r["method"],
                 r.get("chunks", 0), r.get("filename", ""))

    # Counts
    by_status = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    log.info("\nStatus counts: %s", by_status)


if __name__ == "__main__":
    asyncio.run(main())
