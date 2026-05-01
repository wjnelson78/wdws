"""Promote Sonnet OCR to primary for the existing corpus.

Two candidate classes:

  1. Sidecar-promote: doc already has metadata.claude_sonnet_ocr_v1.text
     (from the pilot). Move that text into full_content, regenerate chunks,
     re-embed. Zero Sonnet API cost.

  2. OCR-fresh: doc has storage_uri + PDF/image but no Sonnet sidecar. Run
     the same extraction tree as live ingest (pdftotext → Sonnet → Falcon →
     Tesseract) via email_sync.extract_text_from_binary, write full_content,
     regenerate chunks, re-embed.

Marks each doc with metadata.ocr_version='sonnet-v1' so runs are idempotent.

Safe against the existing Falcon reprocess worker (hardened earlier to skip
claude_sonnet_ocr_v1 entries) and the live delta worker (writes to fresh
docs, ignores already-sonnet'd).

Usage:
    python sonnet_backfill_worker.py --limit 5 --dry-run
    python sonnet_backfill_worker.py --limit 100 --concurrency 3
    python sonnet_backfill_worker.py --all --concurrency 3
    python sonnet_backfill_worker.py --report
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.extras
import httpx

sys.path.insert(0, "/opt/wdws")
import blob_storage  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("sonnet-backfill")

DATABASE_URL = ""
EMBED_URL = ""
EMBED_TOKEN = ""
with open("/opt/wdws/.env") as f:
    for line in f:
        if line.startswith("DATABASE_URL="):
            DATABASE_URL = line.split("=", 1)[1].strip()
        elif line.startswith("HF_ENDPOINT_URL="):
            EMBED_URL = line.split("=", 1)[1].strip()
        elif line.startswith("HF_API_TOKEN="):
            EMBED_TOKEN = line.split("=", 1)[1].strip()

OCR_VERSION = "sonnet-v1"
CHUNK_TARGET_CHARS = 1500
CHUNK_OVERLAP = 200
EMBED_BATCH = 16


def fetch_candidates(conn, limit: int) -> list[dict]:
    """Pick docs needing Sonnet promotion or fresh Sonnet OCR."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
              d.id::text AS id,
              d.filename,
              d.storage_uri,
              d.document_type,
              d.classification,
              (d.metadata ? 'claude_sonnet_ocr_v1') AS has_sidecar,
              COALESCE(d.metadata->'claude_sonnet_ocr_v1'->>'error', '') AS sidecar_error,
              COALESCE(d.storage_size_bytes, 0) AS size
            FROM core.documents d
            WHERE d.legal_hold = false
              AND d.purged_at IS NULL
              AND d.storage_backend = 'minio'
              AND d.storage_uri IS NOT NULL
              AND d.filename ~* '\\.(pdf|png|jpe?g|tiff?|gif|bmp)$'
              AND COALESCE(d.metadata->>'ocr_version', '') != %s
              AND COALESCE(d.classification, 'unknown') != 'spam'
              -- skip tiny noise images (same threshold as ingest path)
              AND NOT (
                d.filename ~* '\\.(png|jpe?g|gif|bmp)$'
                AND COALESCE(d.storage_size_bytes, 0) < 30000
              )
            ORDER BY
              -- prefer sidecar-promote first (cheapest, fastest)
              (d.metadata ? 'claude_sonnet_ocr_v1') DESC,
              RANDOM()
            LIMIT %s
            """,
            (OCR_VERSION, limit),
        )
        return cur.fetchall()


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Call saturn BGE service synchronously.

    Sends every chunk in a single HTTP request — saturn batches internally
    at MAX_BATCH=32. This lets a single doc's embeddings acquire the server
    GPU lock once instead of N times, and fills the GPU more efficiently.
    For the rare truly-huge doc (1000+ chunks), we fall back to chunk-batches
    of 512 to bound memory on both sides.
    """
    if not texts:
        return []
    out: list[list[float]] = []
    headers = {
        "Authorization": f"Bearer {EMBED_TOKEN}",
        "Content-Type": "application/json",
    }
    HARD_CAP = 512
    with httpx.Client(timeout=300) as client:
        for i in range(0, len(texts), HARD_CAP):
            batch = texts[i:i + HARD_CAP]
            r = client.post(f"{EMBED_URL}/embed", json={"inputs": batch},
                            headers=headers)
            r.raise_for_status()
            out.extend(r.json())
    return out


def chunk_text(text: str) -> list[str]:
    """Naive char-count chunker with overlap. EmailAwareChunker is great for
    email bodies but overkill for attachment OCR text; a simple splitter
    matches what the existing pipeline produces for non-email docs."""
    if not text:
        return []
    text = text.strip()
    if len(text) <= CHUNK_TARGET_CHARS:
        return [text]
    out: list[str] = []
    i = 0
    while i < len(text):
        end = min(i + CHUNK_TARGET_CHARS, len(text))
        # Try to break on a newline or space near the end
        if end < len(text):
            for sep in ("\n\n", "\n", ". ", " "):
                found = text.rfind(sep, i + CHUNK_TARGET_CHARS // 2, end)
                if found > 0:
                    end = found + len(sep)
                    break
        out.append(text[i:end].strip())
        i = end - CHUNK_OVERLAP if end - CHUNK_OVERLAP > i else end
    return [c for c in out if c]


def vec_to_literal(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def promote_from_sidecar(conn, row: dict) -> dict:
    """Sidecar-promote path — zero API cost."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metadata->'claude_sonnet_ocr_v1'->>'text' AS text "
            "FROM core.documents WHERE id = %s",
            (row["id"],),
        )
        text = (cur.fetchone() or [""])[0] or ""
    if not text.strip():
        return {"status": "empty_sidecar"}
    return _apply_text(conn, row, text, method="sonnet_promoted_from_sidecar")


def ocr_fresh_and_write(conn, row: dict) -> dict:
    """Full OCR via the live ingest tree (pdftotext → Sonnet → Falcon → Tesseract)."""
    from email_sync import extract_text_from_binary  # noqa: E402
    blob = blob_storage.get_bytes(row["storage_uri"])
    if not blob:
        return {"status": "blob_missing"}
    if row["filename"].lower().endswith(".pdf") and not blob.startswith(b"%PDF-"):
        return {"status": "not_a_pdf"}
    text, method = extract_text_from_binary(
        row["filename"] or "attachment",
        "application/pdf" if row["filename"].lower().endswith(".pdf") else "image/png",
        blob,
    )
    if not text or not text.strip():
        return {"status": "extraction_empty", "method": method}
    return _apply_text(conn, row, text, method=method)


def _apply_text(conn, row: dict, text: str, method: str) -> dict:
    """Write full_content, regenerate chunks, re-embed. Per-doc transaction."""
    chunks = chunk_text(text)
    if not chunks:
        return {"status": "no_chunks"}
    embeddings = embed_texts(chunks)
    if len(embeddings) != len(chunks):
        return {"status": "embed_mismatch"}

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE core.documents
               SET full_content = %s,
                   total_chunks = %s,
                   metadata = jsonb_set(
                     jsonb_set(
                       COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                       '{ocr_version}', %s::jsonb, true
                     ),
                     '{ocr_method_sonnet_v1}', %s::jsonb, true
                   )
               WHERE id = %s""",
            (
                text, len(chunks),
                json.dumps({"sonnet_v1_at": now}),
                json.dumps(OCR_VERSION),
                json.dumps(method),
                row["id"],
            ),
        )
        cur.execute(
            "DELETE FROM core.document_chunks WHERE document_id = %s",
            (row["id"],),
        )
        for idx, (chunk, vec) in enumerate(zip(chunks, embeddings)):
            cur.execute(
                """INSERT INTO core.document_chunks
                     (id, document_id, chunk_index, total_chunks, content,
                      embedding, embedding_model_id, token_count)
                   VALUES (%s, %s, %s, %s, %s, %s::halfvec(1024), %s, %s)""",
                (
                    str(uuid.uuid4()),
                    row["id"], idx, len(chunks),
                    chunk, vec_to_literal(vec), 1, len(chunk) // 4,
                ),
            )
    conn.commit()
    return {"status": "ok", "method": method, "chars": len(text), "chunks": len(chunks)}


def report(conn) -> None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
              COUNT(*) FILTER (WHERE metadata->>'ocr_version' = %s) AS sonnet_promoted,
              COUNT(*) FILTER (WHERE metadata ? 'claude_sonnet_ocr_v1'
                               AND metadata->>'ocr_version' != %s) AS sidecar_only_pending,
              COUNT(*) FILTER (WHERE storage_backend = 'minio'
                               AND storage_uri IS NOT NULL
                               AND filename ~* '\\.(pdf|png|jpe?g|tiff?|gif|bmp)$'
                               AND COALESCE(metadata->>'ocr_version','') != %s) AS fresh_ocr_pending
            FROM core.documents
            WHERE legal_hold = false AND purged_at IS NULL
        """, (OCR_VERSION, OCR_VERSION, OCR_VERSION))
        s = cur.fetchone() or {}
    print("=" * 60)
    print(f"Sonnet-primary docs:        {s.get('sonnet_promoted') or 0}")
    print(f"Sidecar only (to promote):  {s.get('sidecar_only_pending') or 0}")
    print(f"Fresh OCR pending:          {s.get('fresh_ocr_pending') or 0}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--concurrency", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", action="store_true")
    args = ap.parse_args()

    if not EMBED_URL or not EMBED_TOKEN:
        log.error("EMBED_URL/EMBED_TOKEN not configured")
        return 1

    with psycopg2.connect(DATABASE_URL) as conn:
        if args.report:
            report(conn)
            return 0

        limit = 1_000_000 if args.all else args.limit
        candidates = fetch_candidates(conn, limit)
        if not candidates:
            log.info("nothing to do — all eligible docs already at %s", OCR_VERSION)
            return 0

        sidecar_count = sum(1 for c in candidates if c["has_sidecar"])
        log.info("Selected %d candidates (%d sidecar-promote, %d fresh-ocr), "
                 "concurrency=%d",
                 len(candidates), sidecar_count,
                 len(candidates) - sidecar_count, args.concurrency)
        if args.dry_run:
            for i, row in enumerate(candidates[:20], start=1):
                tag = "PROMOTE" if row["has_sidecar"] else "FRESH_OCR"
                log.info("  [DRY %d] %s %s %s", i, tag, row["id"][:8],
                         row["filename"][:60])
            return 0

        totals = {"ok": 0, "fail": 0, "promoted": 0, "ocr_fresh": 0, "cost_hint": 0.0}
        db_lock = threading.Lock()
        t_start = time.time()

        def _worker(idx: int, row: dict) -> None:
            try:
                if row["has_sidecar"] and not row["sidecar_error"]:
                    res = promote_from_sidecar(conn, row)
                    kind = "promote"
                else:
                    res = ocr_fresh_and_write(conn, row)
                    kind = "fresh_ocr"
            except Exception as e:
                res = {"status": f"exception:{type(e).__name__}:{str(e)[:100]}"}
                kind = "error"

            with db_lock:
                if res.get("status") == "ok":
                    totals["ok"] += 1
                    if kind == "promote":
                        totals["promoted"] += 1
                    else:
                        totals["ocr_fresh"] += 1
                    log.info(
                        "[%d/%d] OK(%s) %s method=%s chars=%d chunks=%d "
                        "(cum ok=%d fail=%d)",
                        idx, len(candidates), kind, row["id"][:8],
                        res.get("method", "?"), res.get("chars", 0),
                        res.get("chunks", 0),
                        totals["ok"], totals["fail"],
                    )
                else:
                    totals["fail"] += 1
                    log.warning(
                        "[%d/%d] FAIL %s %s: %s (cum ok=%d fail=%d)",
                        idx, len(candidates), row["id"][:8],
                        row["filename"][:40], res.get("status"),
                        totals["ok"], totals["fail"],
                    )

        if args.concurrency <= 1:
            for i, row in enumerate(candidates, start=1):
                _worker(i, row)
        else:
            with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
                futures = {ex.submit(_worker, i + 1, r): r
                           for i, r in enumerate(candidates)}
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        log.exception("worker crashed: %s", e)

        elapsed = time.time() - t_start
        log.info("Done. ok=%d (promoted=%d fresh_ocr=%d) fail=%d elapsed=%.1fs (%.1f min)",
                 totals["ok"], totals["promoted"], totals["ocr_fresh"],
                 totals["fail"], elapsed, elapsed / 60)
        report(conn)
    return 0


if __name__ == "__main__":
    sys.exit(main())
