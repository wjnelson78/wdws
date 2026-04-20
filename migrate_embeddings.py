#!/usr/bin/env python3
"""
Athena Cognitive Engine — Embedding Migration
==============================================
Migrates from OpenAI text-embedding-3-large (3072-dim) to
BGE-M3 (1024-dim) with Contextual Retrieval.

Process:
  1. Generate document-level context summaries via Claude Sonnet
  2. Prepend context to each chunk
  3. Re-embed with BGE-M3 (1024 dims, local CPU)
  4. Update pgvector column + rebuild HNSW indexes

Usage:
    # Dry run — show what would be done
    python migrate_embeddings.py --dry-run

    # Full migration
    python migrate_embeddings.py

    # Resume after interruption (skips already-migrated docs)
    python migrate_embeddings.py --resume

    # Only generate contexts (no re-embedding)
    python migrate_embeddings.py --contexts-only

    # Only re-embed (contexts already generated)
    python migrate_embeddings.py --embed-only
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/wdws")

# Load .env
_env = Path("/opt/wdws/.env")
if _env.exists():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

import asyncpg
from embedding_service import (
    embed_texts_sync, embed_query_sync, _vec_literal,
    EMBEDDING_DIMENSIONS, EMBEDDING_MODEL_NAME,
)
from contextual_retrieval import (
    generate_context, prepend_context, generate_contexts_batch,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger("migrate")

DATABASE_URL = os.environ["DATABASE_URL"]

# ── Migration State Table ────────────────────────────────────
MIGRATION_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS ops.embedding_migration (
    document_id UUID PRIMARY KEY,
    context_summary TEXT,
    context_generated_at TIMESTAMPTZ,
    embedded_at TIMESTAMPTZ,
    old_model TEXT DEFAULT 'text-embedding-3-large',
    new_model TEXT DEFAULT 'BAAI/bge-m3',
    old_dims INT DEFAULT 3072,
    new_dims INT DEFAULT 1024,
    chunk_count INT,
    status TEXT DEFAULT 'pending'
);
"""


async def setup_migration_table(pool):
    """Create migration tracking table if it doesn't exist."""
    await pool.execute(MIGRATION_TABLE_DDL)
    log.info("Migration tracking table ready")


async def get_documents(pool, resume: bool = False):
    """Get all documents that need migration."""
    if resume:
        # Skip documents already fully migrated
        rows = await pool.fetch("""
            SELECT d.id, d.title, d.domain, d.document_type,
                   LEFT(d.full_content, 3000) as content_preview,
                   c.case_number,
                   COUNT(ch.id) as chunk_count
            FROM core.documents d
            LEFT JOIN legal.case_documents cd ON d.id = cd.document_id
            LEFT JOIN legal.cases c ON cd.case_id = c.id
            LEFT JOIN core.document_chunks ch ON d.id = ch.document_id
            WHERE d.id NOT IN (
                SELECT document_id FROM ops.embedding_migration
                WHERE status = 'complete'
            )
            GROUP BY d.id, d.title, d.domain, d.document_type,
                     d.full_content, c.case_number
            ORDER BY d.created_at
        """)
    else:
        rows = await pool.fetch("""
            SELECT d.id, d.title, d.domain, d.document_type,
                   LEFT(d.full_content, 3000) as content_preview,
                   c.case_number,
                   COUNT(ch.id) as chunk_count
            FROM core.documents d
            LEFT JOIN legal.case_documents cd ON d.id = cd.document_id
            LEFT JOIN legal.cases c ON cd.case_id = c.id
            LEFT JOIN core.document_chunks ch ON d.id = ch.document_id
            GROUP BY d.id, d.title, d.domain, d.document_type,
                     d.full_content, c.case_number
            ORDER BY d.created_at
        """)
    return [dict(r) for r in rows]


async def phase1_generate_contexts(pool, documents, concurrency=3):
    """Phase 1: Generate context summaries for all documents via Claude Sonnet."""
    log.info("=" * 60)
    log.info("PHASE 1: Generating context summaries (%d documents)", len(documents))
    log.info("Using Claude Sonnet (via CLI) with %d concurrent calls", concurrency)
    log.info("=" * 60)

    # Check which docs already have contexts
    existing = await pool.fetch("""
        SELECT document_id FROM ops.embedding_migration
        WHERE context_summary IS NOT NULL
    """)
    existing_ids = {str(r["document_id"]) for r in existing}

    to_process = [d for d in documents if str(d["id"]) not in existing_ids]
    log.info("Skipping %d docs with existing contexts, processing %d",
             len(existing_ids), len(to_process))

    if not to_process:
        log.info("All contexts already generated")
        return

    t0 = time.time()
    processed = 0
    errors = 0

    async def progress(completed, total, title):
        nonlocal processed
        processed = completed
        elapsed = time.time() - t0
        rate = completed / elapsed if elapsed > 0 else 0
        eta = (total - completed) / rate if rate > 0 else 0
        log.info("Context [%d/%d] (%.1f/min, ETA %.0fm): %s",
                 completed, total, rate * 60, eta / 60, title)

    # Process in batches to save memory and allow checkpointing
    batch_size = 50
    for batch_start in range(0, len(to_process), batch_size):
        batch = to_process[batch_start:batch_start + batch_size]
        contexts = await generate_contexts_batch(
            batch, concurrency=concurrency, callback=progress
        )

        # Save to migration table
        for doc in batch:
            doc_id = doc["id"]
            context = contexts.get(doc_id, "")
            if context:
                await pool.execute("""
                    INSERT INTO ops.embedding_migration
                        (document_id, context_summary, context_generated_at,
                         chunk_count, status)
                    VALUES ($1, $2, now(), $3, 'context_ready')
                    ON CONFLICT (document_id) DO UPDATE SET
                        context_summary = $2,
                        context_generated_at = now(),
                        status = 'context_ready'
                """, doc_id, context, doc.get("chunk_count", 0))
            else:
                errors += 1

    elapsed = time.time() - t0
    log.info("Phase 1 complete: %d contexts in %.1f minutes (%d errors)",
             processed, elapsed / 60, errors)


async def phase2_reembed(pool, batch_size=64):
    """Phase 2: Re-embed all chunks with BGE-M3 using contextual content.

    Uses batch SQL updates (UNNEST arrays) instead of per-chunk UPDATEs
    for ~10-20x throughput improvement.
    """
    log.info("=" * 60)
    log.info("PHASE 2: Re-embedding with BGE-M3 (%d-dim) — BATCH MODE", EMBEDDING_DIMENSIONS)
    log.info("=" * 60)

    SQL_BATCH_SIZE = 100  # Chunks per SQL UPDATE call

    # Get all documents with contexts ready
    docs = await pool.fetch("""
        SELECT em.document_id, em.context_summary
        FROM ops.embedding_migration em
        WHERE em.status = 'context_ready'
        ORDER BY em.document_id
    """)

    if not docs:
        log.info("No documents ready for re-embedding")
        return

    log.info("Re-embedding %d documents (%d-chunk SQL batches)",
             len(docs), SQL_BATCH_SIZE)
    t0 = time.time()
    total_chunks = 0
    doc_count = 0

    # Accumulate chunks across documents for batch SQL writes
    pending_ids = []
    pending_vecs = []
    pending_contents = []
    pending_meta = []
    pending_doc_ids = set()

    async def flush_batch():
        """Write accumulated chunks to DB in a single UNNEST-based UPDATE."""
        nonlocal pending_ids, pending_vecs, pending_contents, pending_meta, pending_doc_ids
        if not pending_ids:
            return

        await pool.execute("""
            UPDATE core.document_chunks AS c
            SET embedding = v.vec::halfvec,
                embedding_model_id = 2,
                embedded_content = v.econtent,
                embedded_at = now(),
                metadata = COALESCE(c.metadata, '{}'::jsonb) || v.meta::jsonb
            FROM (
                SELECT unnest($1::text[]) AS cid,
                       unnest($2::text[]) AS vec,
                       unnest($3::text[]) AS econtent,
                       unnest($4::text[]) AS meta
            ) AS v
            WHERE c.id = v.cid
        """, pending_ids, pending_vecs, pending_contents, pending_meta)

        # Mark flushed documents as complete
        for did in pending_doc_ids:
            await pool.execute("""
                UPDATE ops.embedding_migration
                SET status = 'complete', embedded_at = now(),
                    new_model = $2, new_dims = $3
                WHERE document_id = $1
            """, did, EMBEDDING_MODEL_NAME, EMBEDDING_DIMENSIONS)

        pending_ids = []
        pending_vecs = []
        pending_contents = []
        pending_meta = []
        pending_doc_ids = set()

    for doc in docs:
        doc_id = doc["document_id"]
        context = doc["context_summary"] or ""

        # Get all chunks for this document
        chunks = await pool.fetch("""
            SELECT id, chunk_index, content
            FROM core.document_chunks
            WHERE document_id = $1
            ORDER BY chunk_index
        """, doc_id)

        if not chunks:
            await pool.execute("""
                UPDATE ops.embedding_migration
                SET status = 'complete', embedded_at = now()
                WHERE document_id = $1
            """, doc_id)
            doc_count += 1
            continue

        # Prepend context to each chunk
        enriched_texts = []
        chunk_ids = []
        for ch in chunks:
            content = ch["content"] or ""
            enriched = prepend_context(context, content)
            enriched_texts.append(enriched)
            chunk_ids.append(ch["id"])

        # Embed with BGE-M3
        embeddings = embed_texts_sync(enriched_texts, batch_size=batch_size)

        # Build metadata JSON once per document
        meta_json = json.dumps({
            "embedding_model": EMBEDDING_MODEL_NAME,
            "contextual": True,
            "context_preview": context[:100],
        })

        # Accumulate for batch SQL write
        for i, (cid, emb) in enumerate(zip(chunk_ids, embeddings)):
            pending_ids.append(cid)
            pending_vecs.append(_vec_literal(emb))
            pending_contents.append(enriched_texts[i])
            pending_meta.append(meta_json)

        pending_doc_ids.add(doc_id)
        total_chunks += len(chunks)
        doc_count += 1

        # Flush when batch is full
        if len(pending_ids) >= SQL_BATCH_SIZE:
            await flush_batch()

        if doc_count % 50 == 0:
            elapsed = time.time() - t0
            rate = total_chunks / elapsed if elapsed > 0 else 0
            remaining = 50916 - total_chunks  # approximate
            eta = remaining / rate if rate > 0 else 0
            log.info("Embedded %d docs / %d chunks (%.0f chunks/min, ETA %.0fm)",
                     doc_count, total_chunks, rate * 60, eta / 60)

    # Flush any remaining
    await flush_batch()

    elapsed = time.time() - t0
    log.info("Phase 2 complete: %d docs, %d chunks in %.1f minutes (%.0f chunks/min)",
             doc_count, total_chunks, elapsed / 60,
             total_chunks / elapsed * 60 if elapsed > 0 else 0)


async def phase3_rebuild_indexes(pool):
    """Phase 3: Rebuild HNSW indexes for the new 1024-dim vectors."""
    log.info("=" * 60)
    log.info("PHASE 3: Rebuilding pgvector indexes (1024-dim)")
    log.info("=" * 60)

    # Drop old indexes and create new ones for 1024 dims
    index_cmds = [
        # Drop old HNSW index if exists
        "DROP INDEX IF EXISTS core.idx_document_chunks_embedding_hnsw",
        "DROP INDEX IF EXISTS core.document_chunks_embedding_idx",
        # Create new HNSW index for 1024-dim halfvec
        """CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chunks_embedding_hnsw
           ON core.document_chunks
           USING hnsw ((embedding::halfvec(1024)) halfvec_cosine_ops)
           WITH (m = 16, ef_construction = 200)""",
    ]

    for cmd in index_cmds:
        try:
            log.info("Executing: %s", cmd[:100])
            await pool.execute(cmd)
            log.info("  Done")
        except Exception as e:
            log.warning("  Index command failed (may be expected): %s", e)

    log.info("Phase 3 complete: indexes rebuilt")


async def run_migration(
    dry_run: bool = False,
    resume: bool = False,
    contexts_only: bool = False,
    embed_only: bool = False,
    concurrency: int = 3,
    batch_size: int = 64,
):
    """Run the full embedding migration."""
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)

    try:
        await setup_migration_table(pool)

        # Get documents
        documents = await get_documents(pool, resume=resume)
        total_chunks = sum(d.get("chunk_count", 0) for d in documents)

        log.info("=" * 60)
        log.info("  EMBEDDING MIGRATION")
        log.info("  Documents: %d", len(documents))
        log.info("  Total chunks: %d", total_chunks)
        log.info("  Old model: text-embedding-3-large (3072-dim)")
        log.info("  New model: %s (%d-dim)", EMBEDDING_MODEL_NAME, EMBEDDING_DIMENSIONS)
        log.info("  Context model: Claude Sonnet (via CLI)")
        log.info("  Resume: %s", resume)
        log.info("=" * 60)

        if dry_run:
            log.info("DRY RUN — no changes will be made")

            # Show sample context for first doc
            if documents:
                doc = documents[0]
                log.info("\nSample document: %s", doc.get("title", "Untitled")[:80])
                context = await generate_context(
                    title=doc.get("title", ""),
                    domain=doc.get("domain", ""),
                    document_type=doc.get("document_type", ""),
                    content_preview=doc.get("content_preview", ""),
                    case_number=doc.get("case_number"),
                )
                log.info("Generated context:\n  %s", context)

                # Show embedding dimensions
                enriched = prepend_context(context, "Sample chunk text here.")
                vec = embed_query_sync(enriched)
                log.info("Embedding dimensions: %d", len(vec))

            # Estimate time
            est_context_min = len(documents) * 8 / 60  # ~8s per doc
            est_embed_min = total_chunks * 0.05 / 60   # ~50ms per chunk
            log.info("\nEstimated time:")
            log.info("  Context generation: ~%.0f minutes", est_context_min)
            log.info("  Re-embedding: ~%.0f minutes", est_embed_min)
            log.info("  Total: ~%.0f minutes", est_context_min + est_embed_min)
            return

        # Phase 1: Generate contexts
        if not embed_only:
            await phase1_generate_contexts(pool, documents, concurrency=concurrency)

        # Phase 2: Re-embed with BGE-M3
        if not contexts_only:
            await phase2_reembed(pool, batch_size=batch_size)

            # Phase 3: Rebuild indexes
            await phase3_rebuild_indexes(pool)

        # Summary
        stats = await pool.fetchrow("""
            SELECT COUNT(*) FILTER (WHERE status = 'complete') as completed,
                   COUNT(*) FILTER (WHERE status = 'context_ready') as context_only,
                   COUNT(*) FILTER (WHERE status = 'pending') as pending,
                   COUNT(*) as total
            FROM ops.embedding_migration
        """)

        log.info("=" * 60)
        log.info("  MIGRATION SUMMARY")
        log.info("  Completed: %d", stats["completed"])
        log.info("  Context only: %d", stats["context_only"])
        log.info("  Pending: %d", stats["pending"])
        log.info("  Total: %d", stats["total"])
        log.info("=" * 60)

    finally:
        await pool.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate embeddings to BGE-M3 + Contextual Retrieval")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without making changes")
    parser.add_argument("--resume", action="store_true", help="Resume interrupted migration")
    parser.add_argument("--contexts-only", action="store_true", help="Only generate context summaries")
    parser.add_argument("--embed-only", action="store_true", help="Only re-embed (contexts already done)")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent Claude CLI calls (default: 3)")
    parser.add_argument("--batch-size", type=int, default=64, help="Embedding batch size (default: 64)")

    args = parser.parse_args()
    asyncio.run(run_migration(
        dry_run=args.dry_run,
        resume=args.resume,
        contexts_only=args.contexts_only,
        embed_only=args.embed_only,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
    ))


if __name__ == "__main__":
    main()
