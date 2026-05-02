#!/usr/bin/env python3
"""Phase 4: extract canonical text from case_law.opinions, chunk, embed via BGE-M3.

For each row in case_law.opinions:
  1. Extract canonical text:
       plain_text (preferred) → html_with_citations (CAP TEI XML, tags stripped)
       → xml_harvard (TEI XML, tags stripped). Pick first non-empty.
  2. Insert one core.documents row (domain='legal', document_type='case-opinion').
  3. Update case_law.opinions.document_id, extracted_text, text_source, char_count.
  4. Chunk text at paragraph boundaries (CHUNK_TARGET_CHARS=6000), insert into
     core.document_chunks with embedding=NULL initially.
  5. Stage 2: batch-embed all chunks where embedding IS NULL via BGE-M3
     (embedding_service.embed_texts_sync, batch=64).

Idempotency:
  - Skip opinions with document_id already populated.
  - Skip chunks with embedding IS NOT NULL during Stage 2.
  - source_path is per-opinion: 'cl-opinion-{cl_opinion_id}' → unique.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import time
import uuid
from typing import Optional

import psycopg2 as psycopg

sys.path.insert(0, "/opt/wdws")
from embedding_service import embed_texts_sync, EMBEDDING_DIMENSIONS  # noqa: E402

assert EMBEDDING_DIMENSIONS == 1024, f"unexpected embedding dim {EMBEDDING_DIMENSIONS}"

EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
CHUNK_TARGET_CHARS = 6000
EMBED_BATCH_SIZE = 64

# CL TEI XML tag pattern + entity decode
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")
_NL_RE = re.compile(r"\n{3,}")


def strip_tei_xml(s: str) -> str:
    """Strip CAP TEI XML tags + page-number markers; preserve paragraph breaks."""
    if not s:
        return ""
    s = re.sub(r"</p>", "\n\n", s)
    s = re.sub(r"</author>", "\n\n", s)
    s = re.sub(r"</opinion>", "\n\n", s)
    s = _TAG_RE.sub("", s)
    s = (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
           .replace("&quot;", '"').replace("&#x27;", "'").replace("&apos;", "'")
           .replace("&nbsp;", " "))
    s = _WS_RE.sub(" ", s)
    s = _NL_RE.sub("\n\n", s)
    return s.strip()


def extract_text(plain_text: Optional[str], html_with_citations: Optional[str],
                 xml_harvard: Optional[str]) -> tuple[str, str]:
    """Return (text, source). source is 'plain_text'|'html_with_citations'|'xml_harvard'|''."""
    if plain_text and plain_text.strip():
        return plain_text.strip(), "plain_text"
    if html_with_citations and html_with_citations.strip():
        return strip_tei_xml(html_with_citations), "html_with_citations"
    if xml_harvard and xml_harvard.strip():
        return strip_tei_xml(xml_harvard), "xml_harvard"
    return "", ""


def _hard_slice(s: str, n: int) -> list[str]:
    return [s[i:i+n] for i in range(0, len(s), n)]


def chunk_text(text: str, breadcrumb: str) -> list[str]:
    """Paragraph-boundary chunking targeting CHUNK_TARGET_CHARS, with breadcrumb prefix."""
    if not text.strip():
        return []
    full = f"{breadcrumb}\n\n{text}"
    if len(full) <= CHUNK_TARGET_CHARS:
        return [full]
    paras = text.split("\n\n")
    out: list[str] = []
    buf = ""
    bc_overhead = len(breadcrumb) + 4
    target = CHUNK_TARGET_CHARS - bc_overhead
    for p in paras:
        if len(buf) + len(p) + 2 <= target:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                out.append(f"{breadcrumb}\n\n{buf}")
            if len(p) > target:
                for sub in _hard_slice(p, target):
                    out.append(f"{breadcrumb}\n\n{sub}")
                buf = ""
            else:
                buf = p
    if buf:
        out.append(f"{breadcrumb}\n\n{buf}")
    return out


def build_breadcrumb(case_name: str, court_id: str, date_filed: str,
                     opinion_type: str, citation: str) -> str:
    parts = []
    if case_name:
        parts.append(case_name.strip())
    if citation:
        parts.append(citation)
    if date_filed:
        parts.append(f"({date_filed})")
    if court_id:
        court = {"wash": "Wash. Sup. Ct.", "washctapp": "Wash. Ct. App."}.get(court_id, court_id)
        parts.append(court)
    if opinion_type:
        type_label = {
            "020lead": "Lead Opinion",
            "040dissent": "Dissent",
            "060concurrence": "Concurrence",
            "010combined": "Per Curiam",
            "030concurrenceinpart": "Concurrence in Part",
            "050addendum": "Addendum",
        }.get(opinion_type, opinion_type)
        parts.append(type_label)
    return " | ".join(parts)


def normalize_citation(volume: Optional[str], reporter: Optional[str], page: Optional[str]) -> Optional[str]:
    """Normalize CL reporter form 'Wash. 2d' → 'Wn.2d', etc."""
    if not (volume and reporter and page):
        return None
    r = reporter.replace("Wash. App. 2d", "Wn. App. 2d") \
                .replace("Wash. App.", "Wn. App.") \
                .replace("Wash. 2d", "Wn.2d") \
                .replace("Wash.", "Wn.")
    return f"{volume} {r} {page}"


def stage1_extract_and_chunk(conn, *, limit: Optional[int] = None) -> int:
    """Extract text + create core.documents + chunks (no embeddings) for case_law.opinions."""
    with conn.cursor() as cur:
        sql = """
            SELECT o.id, o.cluster_id, o.type, o.plain_text, o.html_with_citations, o.xml_harvard,
                   c.case_name, c.date_filed, d.court_id,
                   (SELECT canonical FROM case_law.citations WHERE cluster_id=c.id
                    ORDER BY type LIMIT 1) AS citation
              FROM case_law.opinions o
              JOIN case_law.opinion_clusters c ON c.id = o.cluster_id
              JOIN case_law.dockets d          ON d.id = c.docket_id
             WHERE o.document_id IS NULL
             ORDER BY o.id
        """
        if limit:
            sql += f" LIMIT {limit}"
        cur.execute(sql)
        rows = cur.fetchall()
    print(f"[stage1] {len(rows):,} opinions to extract", flush=True)

    n_done = 0
    n_skipped = 0
    n_chunks_total = 0
    t0 = time.time()
    for r in rows:
        (op_id, cluster_id, op_type, plain_text, html, xml, case_name, date_filed,
         court_id, citation) = r
        text, source = extract_text(plain_text, html, xml)
        if not text or len(text) < 50:
            n_skipped += 1
            continue
        breadcrumb = build_breadcrumb(case_name or "", court_id or "", str(date_filed or ""),
                                       op_type or "", citation or "")
        chunks = chunk_text(text, breadcrumb)
        if not chunks:
            n_skipped += 1
            continue

        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        doc_id = str(uuid.uuid4())
        title = breadcrumb[:500]
        source_path = f"cl-opinion-{op_id}"
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO core.documents (id, domain, source_path, document_type, title,
                                            content_hash, total_chunks, full_content, metadata,
                                            classification, classified_at)
                VALUES (%s, 'legal', %s, 'case-opinion', %s, %s, %s, %s,
                        jsonb_build_object('cl_opinion_id', %s, 'cl_cluster_id', %s,
                                           'court_id', %s, 'opinion_type', %s,
                                           'date_filed', %s::text, 'text_source', %s,
                                           'canonical_citation', %s),
                        'public', now())
                ON CONFLICT DO NOTHING
                RETURNING id
            """, (doc_id, source_path, title, content_hash, len(chunks), text,
                  op_id, cluster_id, court_id, op_type, date_filed, source, citation))
            res = cur.fetchone()
            if res is None:
                # source_path collision (shouldn't happen — opinion ids are unique)
                cur.execute("SELECT id FROM core.documents WHERE source_path=%s", (source_path,))
                doc_id = cur.fetchone()[0]
            else:
                doc_id = res[0]

            # Insert chunks (no embedding yet)
            for i, ch in enumerate(chunks):
                chunk_id = f"{doc_id}_{i}"
                cur.execute("""
                    INSERT INTO core.document_chunks (id, document_id, chunk_index, total_chunks,
                                                      content, embedding_model_id, token_count,
                                                      embedded_content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (document_id, chunk_index) DO NOTHING
                """, (chunk_id, doc_id, i, len(chunks), ch, EMBEDDING_MODEL_ID,
                      len(ch.split()), ch))
            cur.execute("""
                UPDATE case_law.opinions
                   SET document_id = %s, extracted_text = %s, text_source = %s,
                       char_count = %s
                 WHERE id = %s
            """, (doc_id, text, source, len(text), op_id))
        conn.commit()
        n_done += 1
        n_chunks_total += len(chunks)
        if n_done % 1000 == 0:
            elapsed = time.time() - t0
            rate = n_done / elapsed if elapsed > 0 else 0
            print(f"[stage1] {n_done:,}/{len(rows):,} extracted ({n_skipped:,} skipped, "
                  f"{n_chunks_total:,} chunks, {rate:.0f}/sec)", flush=True)

    elapsed = time.time() - t0
    print(f"[stage1] DONE: {n_done:,} extracted, {n_skipped:,} skipped, "
          f"{n_chunks_total:,} chunks, elapsed={elapsed:.0f}s", flush=True)
    return n_chunks_total


def stage2_embed_chunks(conn, *, batch_size: int = EMBED_BATCH_SIZE) -> int:
    """Batch-embed all case-law chunks where embedding IS NULL.

    Fetches N×batch_size rows per round (N = embed pool size), splits into N
    sub-batches, and submits them to all endpoints concurrently via
    ThreadPoolExecutor — true N-GPU parallelism instead of alternating.
    """
    import sys
    sys.path.insert(0, "/opt/wdws")
    from embedding_service import _EMBED_POOL, _embed_hf_sync_at, _embed_cpu_sync

    from concurrent.futures import ThreadPoolExecutor, as_completed

    n_workers = max(len(_EMBED_POOL), 1)
    fetch_size = batch_size * n_workers  # fetch enough for all GPUs at once

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
              FROM core.document_chunks ch
              JOIN core.documents d ON d.id = ch.document_id
             WHERE d.document_type = 'case-opinion'
               AND ch.embedding IS NULL
        """)
        total = cur.fetchone()[0]
    print(f"[stage2] {total:,} chunks to embed (batch={batch_size}, workers={n_workers}, "
          f"pool={_EMBED_POOL})", flush=True)
    if total == 0:
        return 0

    def embed_sub_batch(url_idx_texts):
        url, sub_ids, sub_texts = url_idx_texts
        try:
            return sub_ids, _embed_hf_sync_at(url, sub_texts)
        except Exception as e:
            print(f"[stage2] endpoint {url} failed ({e}) — falling back to CPU", flush=True)
            return sub_ids, _embed_cpu_sync(sub_texts)

    n_done = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        while True:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ch.id, ch.embedded_content
                      FROM core.document_chunks ch
                      JOIN core.documents d ON d.id = ch.document_id
                     WHERE d.document_type = 'case-opinion' AND ch.embedding IS NULL
                     ORDER BY ch.id
                     LIMIT %s
                """, (fetch_size,))
                rows = cur.fetchall()
            if not rows:
                break

            # Split into N sub-batches, one per GPU endpoint
            sub_batches = []
            for i, url in enumerate(_EMBED_POOL):
                chunk = rows[i * batch_size:(i + 1) * batch_size]
                if chunk:
                    sub_batches.append((url, [r[0] for r in chunk], [r[1] for r in chunk]))

            futures = {pool.submit(embed_sub_batch, sb): sb for sb in sub_batches}
            results = {}
            for f in as_completed(futures):
                sub_ids, vecs = f.result()
                results[sub_ids[0]] = (sub_ids, vecs)

            with conn.cursor() as cur:
                for sub_ids, vecs in results.values():
                    for cid, vec in zip(sub_ids, vecs):
                        cur.execute("""
                            UPDATE core.document_chunks
                               SET embedding = %s::halfvec(1024), embedded_at = now()
                             WHERE id = %s
                        """, (vec, cid))
            conn.commit()

            n_done += len(rows)
            elapsed = time.time() - t0
            rate = n_done / elapsed if elapsed > 0 else 0
            eta = (total - n_done) / rate if rate > 0 else 0
            if n_done % (fetch_size * 5) == 0 or n_done >= total:
                print(f"[stage2] {n_done:,}/{total:,} embedded ({rate:.1f}/s, ETA {eta:.0f}s)",
                      flush=True)

    elapsed = time.time() - t0
    print(f"[stage2] DONE: {n_done:,} embedded, elapsed={elapsed:.0f}s", flush=True)
    return n_done


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["1", "2", "all"], default="all")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    assert db_url, "DATABASE_URL not set"

    conn = psycopg.connect(db_url)
    conn.autocommit = False
    try:
        if args.stage in ("1", "all"):
            stage1_extract_and_chunk(conn, limit=args.limit)
        if args.stage in ("2", "all"):
            stage2_embed_chunks(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
