#!/usr/bin/env python3
"""backfill_one_doc.py — re-extract one core.documents row from an override path.

For docs that aren't email attachments (where backfill_unextracted_attachments.py
applies). Single-doc tool; takes a doc_id + a known-clean source path.
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
log = logging.getLogger("backfill1")

DATABASE_URL = os.environ["DATABASE_URL"]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc-id", required=True)
    parser.add_argument("--source-path", required=True, help="Local file to extract from")
    parser.add_argument("--content-type", default="application/pdf")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    src = Path(args.source_path)
    if not src.exists():
        raise SystemExit(f"Source not found: {src}")
    data = src.read_bytes()
    log.info("Source: %s (%d bytes)", src, len(data))

    text, method = extract_text_from_binary(src.name, args.content_type, data)
    log.info("Extracted: method=%s chars=%d", method, len(text or ""))
    if not (text or "").strip():
        raise SystemExit("Extraction produced no text — aborting.")

    if args.dry_run:
        log.info("--- first 600 chars ---\n%s", (text or "")[:600])
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    chunker = TextChunker()
    doc_id = uuid.UUID(args.doc_id)

    async with pool.acquire() as conn:
        model_id = await get_or_create_embedding_model_id(conn)
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE core.documents
                   SET full_content = $2,
                       content_hash = $3,
                       metadata = COALESCE(metadata, '{}'::jsonb) ||
                                  jsonb_build_object(
                                    'extraction_method', $4::text,
                                    'reextracted_at', now()::text,
                                    'reextract_source_path', $5::text
                                  ),
                       updated_at = now()
                 WHERE id = $1
                """,
                doc_id, text,
                hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest(),
                method, str(src),
            )

            await conn.execute(
                "DELETE FROM core.document_chunks WHERE document_id = $1", doc_id
            )

            chunks = chunker.split(text)
            embeddings = embed_texts_sync(chunks)
            row = await conn.fetchrow(
                "SELECT source_path FROM core.documents WHERE id = $1", doc_id
            )
            source_path = row["source_path"] if row else str(doc_id)

            total = len(chunks)
            for i, (t, emb) in enumerate(zip(chunks, embeddings)):
                chunk_id = hashlib.md5(f"{source_path}:{i}".encode()).hexdigest()
                emb_param = _embedding_vec_literal(emb) if emb is not None else None
                meta = {
                    "source_part": "body",
                    "is_embedded": True,
                    "case_numbers": [],
                    "backfill": "single_doc_reextract_2026_04_28",
                }
                await conn.execute(
                    """
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedded_content, embedding, embedding_model_id, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec, $8, $9::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata
                    """,
                    chunk_id, doc_id, i, total, t, None,
                    emb_param, model_id, json.dumps(meta),
                )

            await conn.execute(
                "UPDATE core.documents SET total_chunks = $2 WHERE id = $1",
                doc_id, total,
            )

        log.info("Done: doc_id=%s chunks=%d method=%s", doc_id, len(chunks), method)

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
