#!/usr/bin/env python3
"""Fast Phase 2: Batch re-embed all context_ready chunks with BGE-M3.
Fully synchronous — no asyncio, no thread pools. Avoids torch deadlock under nohup.
"""
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
os.environ["OMP_NUM_THREADS"] = "8"

import json, sys, time
sys.path.insert(0, "/opt/wdws")

for line in open("/opt/wdws/.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())

import psycopg2
from psycopg2.extras import execute_values
from embedding_service import embed_texts_sync, _vec_literal, EMBEDDING_MODEL_NAME, EMBEDDING_DIMENSIONS
from contextual_retrieval import prepend_context

SQL_BATCH = 100
DB_URL = os.environ["DATABASE_URL"]


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor()

    # Get docs to re-embed
    cur.execute("""
        SELECT em.document_id, em.context_summary
        FROM ops.embedding_migration em
        WHERE em.status = 'context_ready'
        ORDER BY em.document_id
    """)
    docs = cur.fetchall()
    print(f"Phase 2: {len(docs)} docs to re-embed", flush=True)

    t0 = time.time()
    total_chunks = 0
    doc_count = 0
    batch_rows = []
    batch_doc_ids = set()

    def flush():
        nonlocal batch_rows, batch_doc_ids
        if not batch_rows:
            return
        # Unzip batch rows into parallel arrays for UNNEST
        cids, vecs, econtents, metas = zip(*batch_rows)
        cur.execute("""
            UPDATE core.document_chunks AS c
            SET embedding = v.vec::halfvec,
                embedding_model_id = 2,
                embedded_content = v.econtent,
                embedded_at = now(),
                metadata = COALESCE(c.metadata, '{}'::jsonb) || v.meta::jsonb
            FROM (
                SELECT unnest(%s::text[]) AS cid,
                       unnest(%s::text[]) AS vec,
                       unnest(%s::text[]) AS econtent,
                       unnest(%s::text[]) AS meta
            ) AS v
            WHERE c.id = v.cid
        """, (list(cids), list(vecs), list(econtents), list(metas)))
        for did in batch_doc_ids:
            cur.execute(
                "UPDATE ops.embedding_migration SET status='complete', embedded_at=now() WHERE document_id=%s",
                (did,))
        batch_rows = []
        batch_doc_ids = set()

    for doc_id, context in docs:
        context = context or ""

        cur.execute(
            "SELECT id, content FROM core.document_chunks WHERE document_id=%s ORDER BY chunk_index",
            (doc_id,))
        chunks = cur.fetchall()

        if not chunks:
            cur.execute(
                "UPDATE ops.embedding_migration SET status='complete', embedded_at=now() WHERE document_id=%s",
                (doc_id,))
            doc_count += 1
            continue

        ids = [ch[0] for ch in chunks]
        enriched = [prepend_context(context, ch[1] or "") for ch in chunks]

        vecs = embed_texts_sync(enriched)

        meta_json = json.dumps({
            "embedding_model": EMBEDDING_MODEL_NAME,
            "contextual": True,
            "context_preview": context[:100],
        })

        for i, (cid, emb) in enumerate(zip(ids, vecs)):
            batch_rows.append((cid, _vec_literal(emb), enriched[i], meta_json))
        batch_doc_ids.add(doc_id)
        total_chunks += len(chunks)
        doc_count += 1

        if len(batch_rows) >= SQL_BATCH:
            flush()

        if doc_count % 50 == 0:
            elapsed = time.time() - t0
            rate = total_chunks / elapsed * 60 if elapsed > 0 else 0
            remaining = 36000 - total_chunks
            eta = remaining / (total_chunks / elapsed) / 60 if total_chunks > 0 else 0
            print(f"[{doc_count}/{len(docs)}] {total_chunks} chunks | {rate:.0f}/min | ETA {eta:.1f}h",
                  flush=True)

    flush()
    elapsed = time.time() - t0
    print(f"\nDone: {doc_count} docs, {total_chunks} chunks in {elapsed/60:.1f} min "
          f"({total_chunks/elapsed*60:.0f} chunks/min)", flush=True)

    # Phase 3: rebuild HNSW index
    print("Rebuilding HNSW index...", flush=True)
    cur.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_chunks_embedding_hnsw
        ON core.document_chunks
        USING hnsw ((embedding::halfvec(1024)) halfvec_cosine_ops)
        WITH (m = 16, ef_construction = 200)
    """)
    print("HNSW index built", flush=True)

    cur.execute("SELECT COUNT(*) FILTER (WHERE embedding IS NOT NULL), COUNT(*) FROM core.document_chunks")
    emb, total = cur.fetchone()
    print(f"Final: {emb}/{total} chunks embedded", flush=True)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
