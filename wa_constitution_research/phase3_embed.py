#!/usr/bin/env python3
"""Phase 3 — BGE-M3 embedding generation for wa_constitution.sections.

Pattern follows wa_court_rules/phase4_ingest.py exactly:
  - chunk_text(full_text, breadcrumb) → list of ~6000-char chunks
  - hf_embed_batch(texts) → 1024-dim vectors via HF endpoint
  - INSERT into core.document_chunks (DELETE + INSERT for idempotency)
  - UPDATE core.documents.total_chunks

Falls back to local CPU via embedding_service.embed_texts_sync if HF endpoint
unhealthy. Per §K canon: log fallback at WARN if it triggers (avoid
silent-fallback-defeats-halt-on-error pattern, feedback memory).
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/wdws/.env")
except ImportError:
    pass

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase3_embed.log")

HF_ENDPOINT = os.environ.get("HF_ENDPOINT_URL", "http://172.16.81.187:9098")
HF_API_TOKEN = os.environ.get("HF_API_TOKEN", "")
EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
EMBEDDING_DIMS = 1024
CHUNK_TARGET_CHARS = 6000
BATCH_SIZE = 32

CPU_FALLBACK_USED = 0  # counter; surfaced at end


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def hf_client() -> httpx.Client:
    headers = {"Authorization": f"Bearer {HF_API_TOKEN}"} if HF_API_TOKEN else {}
    return httpx.Client(headers=headers, timeout=60.0)


def hf_embed_batch(client: httpx.Client, texts: list[str]) -> list[list[float]]:
    """Embed via HF endpoint; fall back to CPU on failure (logged WARN)."""
    global CPU_FALLBACK_USED
    try:
        r = client.post(f"{HF_ENDPOINT}/embed", json={"inputs": texts})
        r.raise_for_status()
        return r.json()
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        log(f"  WARN: HF embed failed ({type(e).__name__}: {e}); falling back to local CPU")
        CPU_FALLBACK_USED += len(texts)
        from embedding_service import embed_texts_sync
        return embed_texts_sync(texts)


def vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"


def chunk_text(full_text: str, breadcrumb: str) -> list[str]:
    if not full_text or not full_text.strip():
        return []
    full = f"{breadcrumb}\n\n{full_text}"
    if len(full) <= CHUNK_TARGET_CHARS:
        return [full]
    out = []
    paras = full_text.split("\n\n")
    buf = ""
    overhead = len(breadcrumb) + 4
    target = CHUNK_TARGET_CHARS - overhead
    for p in paras:
        if len(buf) + len(p) + 2 <= target:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                out.append(f"{breadcrumb}\n\n{buf}")
                buf = ""
            if len(p) > target:
                # Hard slice
                for i in range(0, len(p), target):
                    out.append(f"{breadcrumb}\n\n{p[i:i+target]}")
            else:
                buf = p
    if buf:
        out.append(f"{breadcrumb}\n\n{buf}")
    return out


def main() -> int:
    log("=== Phase 3 embedding begin ===")
    log(f"HF endpoint: {HF_ENDPOINT}")
    log(f"Model: BGE-M3 (id={EMBEDDING_MODEL_ID}, dim={EMBEDDING_DIMS})")
    log(f"Chunk target chars: {CHUNK_TARGET_CHARS}")

    # Load all sections + their parent article info for breadcrumb
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id, s.document_id, s.canonical_citation, s.section_heading,
                       s.full_text, a.article_number, a.article_title
                FROM wa_constitution.sections s
                JOIN wa_constitution.articles a ON s.article_id = a.id
                ORDER BY a.article_number_arabic, s.section_number
            """)
            sections = cur.fetchall()
        log(f"Loaded {len(sections)} sections from DB")

        client = hf_client()
        try:
            total_chunks = 0
            sections_with_chunks = 0
            sections_skipped = 0
            t0 = time.time()
            for i, (sec_id, doc_id, citation, heading, full_text, art_num, art_title) in enumerate(sections):
                if not full_text or not full_text.strip():
                    sections_skipped += 1
                    # Set total_chunks=0 for empty-text sections
                    with conn.cursor() as cur:
                        cur.execute("UPDATE core.documents SET total_chunks=0 WHERE id=%s", (doc_id,))
                    continue

                breadcrumb = (
                    f"Wash. Const. art. {art_num} ({art_title}) — {citation}"
                    + (f": {heading}" if heading else "")
                )
                chunks = chunk_text(full_text, breadcrumb)
                if not chunks:
                    sections_skipped += 1
                    continue

                # Embed in batches
                vecs = []
                for batch_start in range(0, len(chunks), BATCH_SIZE):
                    batch = chunks[batch_start:batch_start+BATCH_SIZE]
                    batch_vecs = hf_embed_batch(client, batch)
                    vecs.extend(batch_vecs)
                if len(vecs) != len(chunks):
                    raise HaltError(f"Embed mismatch for {citation}: {len(chunks)} chunks → {len(vecs)} vecs")

                # DELETE existing chunks for this document_id (idempotency)
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM core.document_chunks WHERE document_id=%s", (doc_id,))
                    chunk_rows = []
                    for j, (ctext, vec) in enumerate(zip(chunks, vecs)):
                        chunk_id = f"{doc_id}:{j}"
                        chunk_rows.append((
                            chunk_id, doc_id, j, len(chunks), ctext,
                            vec_literal(vec), EMBEDDING_MODEL_ID,
                            len(ctext.split()), ctext, dt.datetime.now(dt.timezone.utc),
                        ))
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO core.document_chunks
                            (id, document_id, chunk_index, total_chunks, content,
                             embedding, embedding_model_id, token_count, embedded_content,
                             embedded_at)
                           VALUES %s""",
                        chunk_rows,
                        template="(%s, %s, %s, %s, %s, %s::halfvec(1024), %s, %s, %s, %s)",
                    )
                    cur.execute("UPDATE core.documents SET total_chunks=%s WHERE id=%s",
                                (len(chunks), doc_id))
                total_chunks += len(chunks)
                sections_with_chunks += 1

                if (i+1) % 25 == 0:
                    elapsed = time.time() - t0
                    rate = (i+1) / elapsed if elapsed > 0 else 0
                    log(f"  progress: {i+1}/{len(sections)} sections, {total_chunks} chunks, {rate:.1f} sec/sec")

            log(f"--- Phase 3 summary ---")
            log(f"  Sections processed:        {len(sections)}")
            log(f"  Sections with chunks:      {sections_with_chunks}")
            log(f"  Sections skipped (no text): {sections_skipped}")
            log(f"  Total chunks inserted:     {total_chunks}")
            log(f"  Avg chunks/section:        {total_chunks/sections_with_chunks if sections_with_chunks else 0:.2f}")
            log(f"  CPU fallback chunks:       {CPU_FALLBACK_USED} (count of chunks that fell back)")
            log(f"  Wall time:                 {time.time()-t0:.1f}s")

            # Post-flight sanity
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE embedding IS NOT NULL)
                    FROM core.document_chunks c
                    JOIN core.documents d ON c.document_id = d.id
                    WHERE d.document_type = 'constitution-section'
                """)
                (db_total, db_embedded) = cur.fetchone()
            log(f"  DB total wa-const chunks: {db_total} (embedded: {db_embedded})")

            if db_total != total_chunks:
                log(f"  WARN: DB count ({db_total}) != insert count ({total_chunks}) — investigate")
            if db_embedded != db_total:
                raise HaltError(f"Post-flight: {db_total - db_embedded} chunks lack embedding")

            conn.commit()
            log("=== Phase 3 embedding commit ===")
            return 0
        finally:
            client.close()
    except HaltError as e:
        conn.rollback()
        log(f"HALT: {e} (rolled back)")
        return 2
    except Exception as e:
        conn.rollback()
        log(f"ERROR: {type(e).__name__}: {e} (rolled back)")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
