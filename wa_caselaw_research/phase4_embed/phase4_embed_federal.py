#!/usr/bin/env python3
"""Phase 4 (federal): extract canonical text from federal case_law.opinions,
chunk, embed via BGE-M3 across the 4-T4 GPU pool.

Mirrors phase4_embed_opinions.py (WA) with these federal-specific changes:

  1. WHERE clause filters to non-WA courts:
       d.court_id NOT IN ('wash','washctapp','washterr')
  2. 4-column COALESCE for text (adds extracted_text after xml_harvard).
  3. Skip threshold raised from <50 chars (WA precedent) to <100 chars.
     Operator-authorized deviation 2026-05-04 (agent note 56).
  4. Federal court label map for breadcrumbs.
  5. All-4 pool saturation (9098, 9099, 9100, 9101) with multi-in-flight.
     Operator-authorized 2026-05-04 (agent note 56) override of the
     9100/9101-federal-dedicated vs 9098/9099-Athena/WA-shared split.
  6. Halt-loud on any per-pool failure (no silent CPU fallback).
  7. --shard K:N for stage 1 sharding (parallel worker fleet).
  8. --in-flight N for stage 2 multi-in-flight per pool.
  9. Halt-loud guardrails: failure_rate>5%, chunks/op outside [1,50],
     disk free<50GB, any pool unresponsive.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import psycopg2 as psycopg

sys.path.insert(0, "/opt/wdws")
from embedding_service import (  # noqa: E402
    EMBEDDING_DIMENSIONS,
    _EMBED_POOL,
    _embed_hf_sync_at,
)

assert EMBEDDING_DIMENSIONS == 1024, f"unexpected embedding dim {EMBEDDING_DIMENSIONS}"

EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
CHUNK_TARGET_CHARS = 6000
# EMBED_BATCH_SIZE history (per-pool BGE_EMBED_BATCH_MAX limits at server side):
#   - 64: original WA-precedent value; would 413 on all 4 federal pools as
#         configured (9098/9099 cap=32, 9100/9101 cap=16). WA succeeded only
#         because its silent CPU fallback caught the 413s.
#   - 16: pre-realignment floor across all 4 pools (2026-05-04 ~21:00 UTC).
#   - 32: post-realignment floor (2026-05-04 ~21:30 UTC; operator-authorized,
#         agent note 62). bge-federal-cuda0/1 services restarted with explicit
#         BGE_EMBED_BATCH_MAX=32 in their env overlays. Operator authorized
#         this in the same authorization that dropped the
#         core.document_chunks HNSW index (Path B mitigation).
EMBED_BATCH_SIZE = 32

# Federal-specific skip threshold: <100 chars (vs WA's <50).
# Operator-authorized deviation 2026-05-04 (agent note 56).
SKIP_THRESHOLD_CHARS = 100

# Halt-loud guardrails
MIN_DISK_FREE_GB = 50
MAX_CHUNKS_PER_OPINION = 200  # bumped from 50 after smoke-test on op_id=88
                              # (410 KB opinion legitimately chunks to 81)
MIN_CHUNKS_PER_OPINION = 1
MAX_FAILURE_RATE = 0.05
DISK_CHECK_EVERY_N_OPINIONS = 500
PG_DATA_PATH = "/var/lib/postgresql"

# CL TEI XML tag pattern + entity decode
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]+")
_NL_RE = re.compile(r"\n{3,}")

# Federal court label map. courts.court_id values per CourtListener.
FED_COURT_LABELS = {
    "scotus": "U.S.",
    "ca1": "1st Cir.", "ca2": "2d Cir.", "ca3": "3d Cir.", "ca4": "4th Cir.",
    "ca5": "5th Cir.", "ca6": "6th Cir.", "ca7": "7th Cir.", "ca8": "8th Cir.",
    "ca9": "9th Cir.", "ca10": "10th Cir.", "ca11": "11th Cir.",
    "cadc": "D.C. Cir.", "cafc": "Fed. Cir.",
    "dcd": "D.D.C.", "nyed": "E.D.N.Y.", "nysd": "S.D.N.Y.", "nynd": "N.D.N.Y.",
    "nywd": "W.D.N.Y.", "cand": "N.D. Cal.", "cacd": "C.D. Cal.",
    "caed": "E.D. Cal.", "casd": "S.D. Cal.", "wawd": "W.D. Wash.",
    "waed": "E.D. Wash.", "txed": "E.D. Tex.", "txnd": "N.D. Tex.",
    "txsd": "S.D. Tex.", "txwd": "W.D. Tex.", "ord": "D. Or.",
    "ilnd": "N.D. Ill.", "ilcd": "C.D. Ill.", "ilsd": "S.D. Ill.",
    "tax": "T.C.", "cit": "Ct. Int'l Trade", "uscfc": "Fed. Cl.",
}


class HaltLoud(Exception):
    """Raised by guardrails to halt the run loudly."""


def disk_free_gb(path: str = PG_DATA_PATH) -> float:
    return shutil.disk_usage(path).free / (1024**3)


def strip_tei_xml(s: str) -> str:
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


def extract_text(plain_text, html_with_citations, xml_harvard, extracted_text):
    """4-column COALESCE: plain > html > xml > extracted. Returns (text, source)."""
    if plain_text and plain_text.strip():
        return plain_text.strip(), "plain_text"
    if html_with_citations and html_with_citations.strip():
        return strip_tei_xml(html_with_citations), "html_with_citations"
    if xml_harvard and xml_harvard.strip():
        return strip_tei_xml(xml_harvard), "xml_harvard"
    if extracted_text and extracted_text.strip():
        return extracted_text.strip(), "extracted_text"
    return "", ""


def _hard_slice(s: str, n: int) -> list[str]:
    return [s[i:i+n] for i in range(0, len(s), n)]


def chunk_text(text: str, breadcrumb: str) -> list[str]:
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


def build_breadcrumb(case_name, court_id, date_filed, opinion_type, citation):
    parts = []
    if case_name:
        parts.append(case_name.strip())
    if citation:
        parts.append(citation)
    if date_filed:
        parts.append(f"({date_filed})")
    if court_id:
        parts.append(FED_COURT_LABELS.get(court_id, court_id))
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


# ── Stage 1: extract + chunk + insert documents (no embeddings) ──────

FEDERAL_WHERE = "d.court_id NOT IN ('wash','washctapp','washterr')"


def stage1_extract_and_chunk(conn_r, conn_w, *, limit=None, sample=None,
                             shard_k=None, shard_n=None,
                             tag="stage1") -> dict:
    """Extract text + create core.documents + chunks (no embeddings).

    --shard K:N filter: only process opinions where (o.id % N) = K.
    Used by parallel worker fleet for stage 1 horizontal scaling.
    """
    select_cols = """
        o.id, o.cluster_id, o.type,
        o.plain_text, o.html_with_citations, o.xml_harvard, o.extracted_text,
        c.case_name, c.date_filed, d.court_id,
        (SELECT canonical FROM case_law.citations WHERE cluster_id=c.id
         ORDER BY type LIMIT 1) AS citation
    """
    base_join = """
        FROM case_law.opinions o
        JOIN case_law.opinion_clusters c ON c.id = o.cluster_id
        JOIN case_law.dockets d          ON d.id = c.docket_id
    """
    shard_clause = ""
    if shard_n is not None:
        shard_clause = f" AND (o.id % {shard_n}) = {shard_k}"

    if sample:
        sql = f"""
            SELECT {select_cols}
            {base_join}
            WHERE o.document_id IS NULL
              AND {FEDERAL_WHERE}
              {shard_clause}
            ORDER BY random()
            LIMIT {sample}
        """
    else:
        sql = f"""
            SELECT {select_cols}
            {base_join}
            WHERE o.document_id IS NULL
              AND {FEDERAL_WHERE}
              {shard_clause}
            ORDER BY o.id
        """
        if limit:
            sql += f" LIMIT {limit}"

    # Server-side cursor on conn_r (read-only) to avoid loading 1M rows into memory.
    # All INSERTs/UPDATEs go through conn_w to avoid invalidating the named cursor.
    cursor_name = f"fed_stage1_{shard_k or 0}_{int(time.time())}"
    with conn_r.cursor(name=cursor_name) as cur:
        cur.itersize = 200
        cur.execute(sql)

        n_done = 0
        n_attempted = 0
        n_failures = 0
        skip_reasons: dict[str, int] = defaultdict(int)
        n_chunks_total = 0
        per_op_ms: list[float] = []
        t0 = time.time()
        chunks_per_opinion: dict[str, int] = defaultdict(int)

        for r in cur:
            n_attempted += 1
            op_t0 = time.time()
            (op_id, cluster_id, op_type, plain_text, html, xml, extracted,
             case_name, date_filed, court_id, citation) = r
            text, source = extract_text(plain_text, html, xml, extracted)
            if not text:
                skip_reasons["no_text"] += 1
                continue
            if len(text) < SKIP_THRESHOLD_CHARS:
                skip_reasons["below_threshold"] += 1
                continue
            breadcrumb = build_breadcrumb(case_name or "", court_id or "",
                                          str(date_filed or ""), op_type or "",
                                          citation or "")
            chunks = chunk_text(text, breadcrumb)
            if not chunks:
                skip_reasons["no_chunks"] += 1
                continue
            if len(chunks) > MAX_CHUNKS_PER_OPINION or len(chunks) < MIN_CHUNKS_PER_OPINION:
                # Skip-and-log instead of halt: a single mega-opinion (e.g.
                # 2.78 MB en banc consolidated case → 534 chunks) shouldn't
                # take down a worker. Operator-acknowledged 2026-05-04.
                skip_reasons[f"chunks_outside_range:{len(chunks)}"] += 1
                print(f"[{tag}] op_id={op_id} skipped: {len(chunks)} chunks "
                      f"(text_len={len(text)}) outside "
                      f"[{MIN_CHUNKS_PER_OPINION},{MAX_CHUNKS_PER_OPINION}]",
                      flush=True)
                continue

            content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
            doc_id = str(uuid.uuid4())
            title = breadcrumb[:500]
            source_path = f"cl-opinion-{op_id}"

            # All writes via conn_w (avoids invalidating the named cursor on conn_r)
            try:
                with conn_w.cursor() as wcur:
                    wcur.execute("""
                        INSERT INTO core.documents
                            (id, domain, source_path, document_type, title,
                             content_hash, total_chunks, full_content, metadata,
                             classification, classified_at,
                             privilege, confidentiality,
                             privilege_classified_at, privilege_classified_by)
                        VALUES (%s, 'legal', %s, 'case-opinion', %s, %s, %s, %s,
                                jsonb_build_object('cl_opinion_id', %s, 'cl_cluster_id', %s,
                                                   'court_id', %s, 'opinion_type', %s,
                                                   'date_filed', %s::text, 'text_source', %s,
                                                   'canonical_citation', %s),
                                'public', now(),
                                'none', 'public', now(), 'rule:public_record')
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """, (doc_id, source_path, title, content_hash, len(chunks), text,
                          op_id, cluster_id, court_id, op_type, date_filed, source, citation))
                    res = wcur.fetchone()
                    if res is None:
                        wcur.execute("SELECT id FROM core.documents WHERE source_path=%s",
                                     (source_path,))
                        doc_id = wcur.fetchone()[0]
                    else:
                        doc_id = res[0]

                    for i, ch in enumerate(chunks):
                        chunk_id = f"{doc_id}_{i}"
                        wcur.execute("""
                            INSERT INTO core.document_chunks
                                (id, document_id, chunk_index, total_chunks,
                                 content, embedding_model_id, token_count,
                                 embedded_content)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (document_id, chunk_index) DO NOTHING
                        """, (chunk_id, doc_id, i, len(chunks), ch, EMBEDDING_MODEL_ID,
                              len(ch.split()), ch))
                    wcur.execute("""
                        UPDATE case_law.opinions
                           SET document_id = %s, extracted_text = %s, text_source = %s,
                               char_count = %s
                         WHERE id = %s
                    """, (doc_id, text, source, len(text), op_id))
                conn_w.commit()
            except HaltLoud:
                raise
            except Exception as exc:
                conn_w.rollback()
                n_failures += 1
                skip_reasons[f"db_error:{type(exc).__name__}"] += 1
                print(f"[{tag}] op_id={op_id} INSERT failed: {exc}", flush=True)
                # halt-loud on excessive failure rate
                if n_attempted >= 100 and n_failures / n_attempted > MAX_FAILURE_RATE:
                    raise HaltLoud(
                        f"[{tag}] failure rate {n_failures}/{n_attempted} "
                        f"= {n_failures/n_attempted:.1%} > {MAX_FAILURE_RATE:.0%}"
                    )
                continue

            n_done += 1
            n_chunks_total += len(chunks)
            per_op_ms.append((time.time() - op_t0) * 1000)
            chunks_per_opinion[op_id] = len(chunks)

            if n_done % 1000 == 0:
                elapsed = time.time() - t0
                rate = n_done / elapsed if elapsed > 0 else 0
                print(f"[{tag}] {n_done:,} done, {n_chunks_total:,} chunks, "
                      f"{rate:.1f} ops/s, fails={n_failures}", flush=True)

            if n_attempted % DISK_CHECK_EVERY_N_OPINIONS == 0:
                free = disk_free_gb()
                if free < MIN_DISK_FREE_GB:
                    raise HaltLoud(
                        f"[{tag}] disk free {free:.1f} GB < {MIN_DISK_FREE_GB} GB"
                    )

    elapsed = time.time() - t0
    skipped_total = sum(skip_reasons.values())
    print(f"[{tag}] DONE: {n_done:,} created, {skipped_total:,} skipped, "
          f"{n_chunks_total:,} chunks, fails={n_failures}, "
          f"elapsed={elapsed:.1f}s", flush=True)
    return {
        "documents_created": n_done,
        "chunks_created": n_chunks_total,
        "opinions_skipped": skipped_total,
        "opinions_attempted": n_attempted,
        "opinions_failed": n_failures,
        "skip_reasons": dict(skip_reasons),
        "per_op_ms": per_op_ms,
        "wall_seconds": elapsed,
    }


# ── Stage 2: multi-in-flight 4-pool saturation embedding (halt-loud) ─


def _summarize_latencies(samples):
    if not samples:
        return {"n": 0, "min_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    ss = sorted(samples)
    n = len(ss)
    def pct(p):
        if n == 1:
            return ss[0]
        idx = max(0, min(n - 1, int(round(p / 100.0 * (n - 1)))))
        return ss[idx]
    return {
        "n": n,
        "min_ms": round(ss[0], 1),
        "p50_ms": round(pct(50), 1),
        "p95_ms": round(pct(95), 1),
        "max_ms": round(ss[-1], 1),
    }


def stage2_embed_chunks(conn, *, batch_size=EMBED_BATCH_SIZE, in_flight=1,
                        only_federal=True, follow=False,
                        follow_idle_sleep=10, follow_max_idle=600,
                        tag="stage2") -> dict:
    """Multi-in-flight stage 2 with halt-loud and follow mode.

    in_flight: concurrent batches per pool (e.g., 4 means 4 batches in flight per pool,
        4 pools = 16 concurrent HTTP calls per round).
    follow: when True, when no chunks remain, sleep briefly and re-poll until
        follow_max_idle seconds have elapsed with no work. Used to run alongside
        stage 1 workers that are still producing chunks.
    """
    n_pools = max(len(_EMBED_POOL), 1)
    total_concurrent = n_pools * in_flight
    fetch_size = batch_size * total_concurrent

    federal_filter = """
        AND d.metadata->>'court_id' NOT IN ('wash','washctapp','washterr')
    """ if only_federal else ""

    print(f"[{tag}] starting batch={batch_size} in_flight={in_flight} "
          f"total_concurrent={total_concurrent} fetch_size={fetch_size} "
          f"pool={_EMBED_POOL}", flush=True)

    per_pool_dispatches: dict[str, int] = defaultdict(int)
    per_pool_latencies: dict[str, list[float]] = defaultdict(list)
    fetch_latencies_ms: list[float] = []
    pool_max_latencies_ms: list[float] = []
    update_latencies_ms: list[float] = []

    def embed_sub_batch(arg):
        url, sub_ids, sub_texts = arg
        t0 = time.time()
        # Resilience: one retry-with-backoff on 500-class errors (transient
        # CUDA blips on overloaded pools). After retry exhausted, halt-loud.
        # NOT a silent fallback — the halt still fires if retry also fails.
        last_exc = None
        for attempt in (1, 2):
            try:
                vecs = _embed_hf_sync_at(url, sub_texts)
                latency_ms = (time.time() - t0) * 1000
                return url, sub_ids, vecs, latency_ms
            except Exception as exc:
                last_exc = exc
                if attempt == 1:
                    print(f"[s2] {url} attempt 1 failed ({exc}) — backoff 2s, retry",
                          flush=True)
                    time.sleep(2)
                continue
        raise last_exc

    n_done = 0
    n_failures = 0
    idle_seconds = 0
    t0 = time.time()
    last_disk_check = 0

    with ThreadPoolExecutor(max_workers=total_concurrent) as pool:
        while True:
            f0 = time.time()
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT ch.id, ch.embedded_content
                      FROM core.document_chunks ch
                      JOIN core.documents d ON d.id = ch.document_id
                     WHERE d.document_type = 'case-opinion'
                       AND ch.embedding IS NULL
                       {federal_filter}
                     ORDER BY ch.id
                     LIMIT %s
                """, (fetch_size,))
                rows = cur.fetchall()
            fetch_latencies_ms.append((time.time() - f0) * 1000)
            if not rows:
                if not follow:
                    break
                # Follow mode: wait for stage 1 to produce more chunks
                idle_seconds += follow_idle_sleep
                if idle_seconds >= follow_max_idle:
                    print(f"[{tag}] follow-mode idle {idle_seconds}s — exiting",
                          flush=True)
                    break
                print(f"[{tag}] follow-mode no chunks ({idle_seconds}s idle)",
                      flush=True)
                time.sleep(follow_idle_sleep)
                continue
            idle_seconds = 0

            # Round-robin assign URLs to sub-batches
            sub_batches = []
            for i in range(0, len(rows), batch_size):
                batch_rows = rows[i:i + batch_size]
                if not batch_rows:
                    continue
                url = _EMBED_POOL[(i // batch_size) % n_pools]
                sub_batches.append((url, [r[0] for r in batch_rows],
                                    [r[1] for r in batch_rows]))

            pool_t0 = time.time()
            try:
                futures = {pool.submit(embed_sub_batch, sb): sb for sb in sub_batches}
                results = []
                for f in as_completed(futures):
                    url, sub_ids, vecs, latency_ms = f.result()  # raises on pool fail
                    per_pool_dispatches[url] += len(sub_ids)
                    per_pool_latencies[url].append(latency_ms)
                    results.append((sub_ids, vecs))
            except Exception as exc:
                n_failures += len(sub_batches)
                # Halt-loud on any pool failure (no degrade-to-N-1 pools)
                raise HaltLoud(f"[{tag}] pool dispatch failed: {exc}") from exc
            pool_max_latencies_ms.append((time.time() - pool_t0) * 1000)

            u0 = time.time()
            # Batched UPDATE via TEMP TABLE + UPDATE FROM. ~50× faster than
            # 256 sequential UPDATEs per round. Avoids the per-row buffer-page
            # serialization that caused folio_wait_bit kernel stalls observed
            # on cold-cache restarts (2026-05-08 night).
            # Convert vec (list of floats) to halfvec literal format "[v1,v2,...]"
            # because psycopg2's default list→postgres-array conversion uses {} braces
            # which halfvec doesn't accept.
            all_pairs = [
                (cid, "[" + ",".join(f"{v:.6f}" for v in vec) + "]")
                for sub_ids, vecs in results
                for cid, vec in zip(sub_ids, vecs)
            ]
            if all_pairs:
                from psycopg2.extras import execute_values
                with conn.cursor() as cur:
                    cur.execute("CREATE TEMP TABLE IF NOT EXISTS _tmp_emb "
                                "(id text PRIMARY KEY, vec text) ON COMMIT DROP")
                    execute_values(
                        cur,
                        "INSERT INTO _tmp_emb (id, vec) VALUES %s",
                        all_pairs,
                        template="(%s, %s)",
                        page_size=len(all_pairs),
                    )
                    cur.execute("""
                        UPDATE core.document_chunks AS ch
                           SET embedding = t.vec::halfvec(1024),
                               embedded_at = now()
                          FROM _tmp_emb t
                         WHERE ch.id = t.id
                    """)
            conn.commit()
            update_latencies_ms.append((time.time() - u0) * 1000)

            n_done += len(rows)
            elapsed = time.time() - t0
            rate = n_done / elapsed if elapsed > 0 else 0

            if n_done - last_disk_check >= 5000:
                free = disk_free_gb()
                if free < MIN_DISK_FREE_GB:
                    raise HaltLoud(
                        f"[{tag}] disk free {free:.1f} GB < {MIN_DISK_FREE_GB} GB"
                    )
                last_disk_check = n_done

            if n_done % (fetch_size * 5) == 0 or len(rows) < fetch_size:
                print(f"[{tag}] {n_done:,} embedded ({rate:.1f}/s)", flush=True)

    elapsed = time.time() - t0
    print(f"[{tag}] DONE: {n_done:,} embedded, fails={n_failures}, "
          f"elapsed={elapsed:.1f}s", flush=True)
    return {
        "chunks_embedded": n_done,
        "chunks_failed": n_failures,
        "per_pool_dispatches": dict(per_pool_dispatches),
        "per_pool_latencies_ms": {k: list(v) for k, v in per_pool_latencies.items()},
        "wall_seconds": elapsed,
        "fetch_latencies_ms": fetch_latencies_ms,
        "pool_max_latencies_ms": pool_max_latencies_ms,
        "update_latencies_ms": update_latencies_ms,
    }


# ── Reporting ─────────────────────────────────────────────────────────


def render_report(stage1, stage2, *, sample_chunks):
    s2_dispatches = stage2.get("per_pool_dispatches", {})
    s2_latencies = stage2.get("per_pool_latencies_ms", {})
    pool_summary = {
        url: {
            "dispatches": s2_dispatches.get(url, 0),
            "latency_ms": _summarize_latencies(s2_latencies.get(url, [])),
        }
        for url in _EMBED_POOL
    }
    op_lat = _summarize_latencies(stage1.get("per_op_ms", []))
    return json.dumps({
        "stage1": {
            "documents_created": stage1.get("documents_created", 0),
            "chunks_created": stage1.get("chunks_created", 0),
            "opinions_attempted": stage1.get("opinions_attempted", 0),
            "opinions_skipped": stage1.get("opinions_skipped", 0),
            "opinions_failed": stage1.get("opinions_failed", 0),
            "skip_reasons": stage1.get("skip_reasons", {}),
            "wall_seconds": round(stage1.get("wall_seconds", 0), 1),
            "per_opinion_ms": op_lat,
        },
        "stage2": {
            "chunks_embedded": stage2.get("chunks_embedded", 0),
            "chunks_failed": stage2.get("chunks_failed", 0),
            "wall_seconds": round(stage2.get("wall_seconds", 0), 1),
            "pool_size": len(_EMBED_POOL),
            "per_pool": pool_summary,
            "fetch_round_latency_ms": _summarize_latencies(stage2.get("fetch_latencies_ms", [])),
            "pool_round_latency_ms": _summarize_latencies(stage2.get("pool_max_latencies_ms", [])),
            "pg_update_round_latency_ms": _summarize_latencies(stage2.get("update_latencies_ms", [])),
            "rounds_completed": len(stage2.get("update_latencies_ms", [])),
        },
        "sample_chunks": sample_chunks,
    }, indent=2, default=str)


def fetch_sample_chunks(conn, n=5):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ch.id, ch.document_id::text, ch.chunk_index, ch.total_chunks,
                   LEFT(ch.content, 500), ch.token_count,
                   (ch.embedding::text)::text AS emb_repr,
                   d.metadata
              FROM core.document_chunks ch
              JOIN core.documents d ON d.id = ch.document_id
             WHERE d.document_type = 'case-opinion'
               AND d.metadata->>'court_id' NOT IN ('wash','washctapp','washterr')
               AND ch.embedding IS NOT NULL
             ORDER BY ch.embedded_at DESC NULLS LAST
             LIMIT %s
        """, (n,))
        rows = cur.fetchall()
    samples = []
    for (cid, did, idx, total, content_500, tokens, emb_repr, meta) in rows:
        try:
            inner = emb_repr.strip("[]")
            first5 = [float(x) for x in inner.split(",")[:5]]
        except Exception:
            first5 = []
        samples.append({
            "chunk_id": cid,
            "document_id": did,
            "chunk_index": idx,
            "total_chunks": total,
            "content_preview_500": content_500,
            "token_count": tokens,
            "embedding_first5": first5,
            "metadata": meta if isinstance(meta, dict) else json.loads(meta),
        })
    return samples


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", choices=["1", "2", "all"], default="all")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap number of opinions (ordered by id)")
    ap.add_argument("--sample", type=int, default=None,
                    help="random sample N opinions (vs --limit)")
    ap.add_argument("--shard", default=None,
                    help="K:N — process opinions where (id mod N) == K (stage 1 only)")
    ap.add_argument("--in-flight", type=int, default=1,
                    help="concurrent batches per pool in stage 2 (default 1)")
    ap.add_argument("--follow", action="store_true",
                    help="stage 2: follow-mode — wait for new chunks (run alongside stage 1)")
    ap.add_argument("--follow-max-idle", type=int, default=600,
                    help="seconds of no new chunks before exiting follow-mode (default 600)")
    ap.add_argument("--report-file", default=None)
    ap.add_argument("--sample-chunks", type=int, default=5)
    ap.add_argument("--tag", default=None,
                    help="log prefix for this worker (e.g. 'stage1.shard3')")
    args = ap.parse_args()

    if args.sample and args.limit:
        ap.error("--sample and --limit are mutually exclusive")

    shard_k = shard_n = None
    if args.shard:
        try:
            sk, sn = args.shard.split(":")
            shard_k, shard_n = int(sk), int(sn)
            assert 0 <= shard_k < shard_n
        except Exception:
            ap.error(f"--shard must be K:N with 0<=K<N (got {args.shard})")

    db_url = os.environ.get("DATABASE_URL")
    assert db_url, "DATABASE_URL not set"

    tag1 = args.tag or ("stage1" + (f".shard{shard_k}of{shard_n}" if shard_n else ""))
    tag2 = args.tag or "stage2"

    print(f"[bootstrap] embed pool ({len(_EMBED_POOL)}): {_EMBED_POOL}", flush=True)
    print(f"[bootstrap] disk free: {disk_free_gb():.1f} GB", flush=True)

    stage1 = {}
    stage2 = {}
    halt_exc = None
    sample_chunks = []
    try:
        if args.stage in ("1", "all"):
            conn_r = psycopg.connect(db_url)
            conn_w = psycopg.connect(db_url)
            conn_r.autocommit = False
            conn_w.autocommit = False
            try:
                stage1 = stage1_extract_and_chunk(
                    conn_r, conn_w, limit=args.limit, sample=args.sample,
                    shard_k=shard_k, shard_n=shard_n, tag=tag1)
            finally:
                conn_r.close()
                conn_w.close()
        if args.stage in ("2", "all"):
            conn = psycopg.connect(db_url)
            conn.autocommit = False
            try:
                stage2 = stage2_embed_chunks(
                    conn, in_flight=args.in_flight, only_federal=True,
                    follow=args.follow, follow_max_idle=args.follow_max_idle,
                    tag=tag2)
            finally:
                conn.close()
        # Sample chunks for the report (separate connection)
        try:
            sconn = psycopg.connect(db_url)
            try:
                sample_chunks = fetch_sample_chunks(sconn, n=args.sample_chunks)
            finally:
                sconn.close()
        except Exception:
            sample_chunks = []
    except HaltLoud as exc:
        halt_exc = exc
        print(f"\n!!! HALT-LOUD: {exc}", flush=True)
        try:
            sconn = psycopg.connect(db_url)
            try:
                sample_chunks = fetch_sample_chunks(sconn, n=args.sample_chunks)
            finally:
                sconn.close()
        except Exception:
            sample_chunks = []

    report_json = render_report(stage1, stage2, sample_chunks=sample_chunks)
    print("\n=== RUN REPORT ===\n" + report_json, flush=True)
    if args.report_file:
        with open(args.report_file, "w") as f:
            f.write(report_json)
        print(f"\n[report] written to {args.report_file}", flush=True)
    if halt_exc:
        sys.exit(2)


if __name__ == "__main__":
    main()
