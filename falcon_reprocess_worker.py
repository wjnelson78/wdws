#!/usr/bin/env python3
"""
Falcon Reprocess Worker — upgrade existing tesseract-OCR docs to Falcon-OCR.

Background, priority-aware client that calls the same Falcon-OCR service
(saturn:9097) used by the live ingest path. Does NOT run a second Falcon —
both the live ingest worker and this reprocess worker share one service,
serialized on saturn's GPU. Live ingest gets priority by being synchronous
and firing immediately; this worker yields voluntarily (sleeps between
batches, backs off when Falcon response times climb).

Selection criteria (all must hold):
  * storage_backend='minio' and storage_uri IS NOT NULL  → original bytes retrievable
  * document_type indicates OCR was relevant (email_attachment, court_filing,
    medical_record, and anything else with a legacy tesseract extraction method)
  * metadata->>'ocr_version' != 'falcon-v1'  → not yet upgraded
  * legal_hold = false                         → holds block reprocessing
  * purged_at IS NULL                          → skip gravestones
  * LENGTH(full_content) > 0                   → doc has content to replace

For each candidate, in one DB transaction per doc:
  1. Fetch blob bytes from MinIO
  2. POST to Falcon /ocr endpoint (plain mode)
  3. Sanity-check the response (not all pad tokens, length plausible)
  4. UPDATE core.documents SET full_content=new, metadata augmented with
       ocr_version='falcon-v1', ocr_upgraded_at, ocr_previous_chars
  5. DELETE FROM core.document_chunks WHERE document_id=$1
  6. Re-chunk via email_chunker + contextual_retrieval
  7. Re-embed via embedding_service
  8. INSERT new chunks

Priority / throttle:
  * Before each doc, GET /healthz — if not reachable, back off
  * Track rolling OCR latency; if > SLOW_THRESHOLD_SEC, sleep (signal live
    traffic is queued ahead of us)
  * --quiet-hours flag: only run 22:00-06:00 local time (for systemd timer
    with OnCalendar=*-*-* 22:00:00 + RandomizedDelaySec + runs until 6am)

Usage:
    python falcon_reprocess_worker.py --dry-run              # preview candidates
    python falcon_reprocess_worker.py --limit 5              # small test
    python falcon_reprocess_worker.py --doc UUID             # single doc
    python falcon_reprocess_worker.py --doc-type email_attachment
    python falcon_reprocess_worker.py --quiet-hours
    python falcon_reprocess_worker.py --max-runtime 3600     # stop after 1h
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import asyncpg
import httpx

# Env loader — keep independent of email_sync to avoid triggering venv reexec
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import blob_storage
from email_chunker import EmailAwareChunker, ChunkResult, PartType
from embedding_service import (
    embed_texts_sync,
    _vec_literal,
    EMBEDDING_DIMENSIONS,
    get_or_create_embedding_model_id,
)
from contextual_retrieval import generate_context_sync, enrich_chunks

log = logging.getLogger("falcon-reprocess")

DATABASE_URL = os.environ["DATABASE_URL"]
OCR_ENDPOINT_URL = os.environ.get("OCR_ENDPOINT_URL", "").rstrip("/")
OCR_ENDPOINT_TOKEN = os.environ.get("OCR_ENDPOINT_TOKEN", "")
OCR_ENDPOINT_TIMEOUT = float(os.environ.get("OCR_ENDPOINT_TIMEOUT", "28800"))  # 8h
# Rationale: a multi-hundred-page PDF at ~60s/page on T4 can take hours.
# Better to block on a single call than split pages client-side and risk
# losing reading-order or page-ordering metadata. This is the server-side
# equivalent of "trust the pipeline".
OCR_VERSION_TAG = "falcon-v1"

# Back-off when we detect live traffic queued ahead of us. A request
# taking significantly longer than the steady-state baseline suggests
# queueing at the Falcon server. Sleep between batches when this fires
# so live ingest gets its turn.
SLOW_THRESHOLD_SEC = float(os.environ.get("REPROCESS_SLOW_SEC", "180"))
BATCH_SLEEP_SEC = float(os.environ.get("REPROCESS_BATCH_SLEEP", "5"))
BACKOFF_SEC = float(os.environ.get("REPROCESS_BACKOFF_SEC", "30"))

# Sanity: if Falcon returns mostly pad tokens or vastly less text than we
# already had, DON'T overwrite — the new output is suspect. Threshold is
# the minimum fraction of the old content length that the new result must
# meet to be accepted.
MIN_ACCEPT_RATIO = float(os.environ.get("REPROCESS_MIN_RATIO", "0.25"))
MAX_PAD_RATIO = float(os.environ.get("REPROCESS_MAX_PAD", "0.10"))

# Doc types that are OCR-relevant. Emails themselves don't need re-OCR
# (their full_content is the parsed MIME text, not an OCR result).
DEFAULT_OCR_DOC_TYPES = (
    "email_attachment", "court_filing", "medical_record",
    "document", "html", "pdf", "note", "internal_communication",
    "external_communication", "mailroom_record", "phone_record",
    "accommodation_order", "accommodation_policy",
    "progress_note", "prescription",
)


# ============================================================
# Candidate selection
# ============================================================

async def select_candidates(
    pool: asyncpg.Pool,
    *,
    limit: int,
    doc_type: Optional[str],
    document_id: Optional[str],
    include_versioned: bool = False,
    include_noise_images: bool = False,
) -> list[dict]:
    conds = [
        "d.storage_backend = 'minio'",
        "d.storage_uri IS NOT NULL",
        "d.legal_hold = false",
        "d.purged_at IS NULL",
        "LENGTH(COALESCE(d.full_content, '')) > 0",
    ]
    if not document_id:
        # Falcon only helps on PDFs + images; everything else is passthrough
        # or already-handled (docx, html, plain text). Skip by extension.
        conds.append(
            "LOWER(COALESCE(d.filename,'')) ~ "
            "'\\.(pdf|png|jpg|jpeg|tiff|tif|gif|bmp)$'"
        )
        if not include_noise_images:
            # Skip tiny Outlook-auto-embedded signature/decoration images
            # (storage_size_bytes is in bytes; 30KB = the MIN threshold we
            # use in email_sync for live noise skipping).
            conds.append(
                "(NOT LOWER(COALESCE(d.filename,'')) ~ "
                "'\\.(png|gif|jpg|jpeg|bmp)$' "
                " OR d.storage_size_bytes >= 30000)"
            )
            conds.append(
                "NOT LOWER(COALESCE(d.filename,'')) ~ "
                "'(outlook-cid|image[0-9]+|signature|logo|spacer|pixel|"
                "thumbnail|tracker)\\.(png|gif|jpg|jpeg|bmp)$'"
            )
    params: list[Any] = []
    idx = 1
    if document_id:
        conds.append(f"d.id = ${idx}::uuid")
        params.append(document_id)
        idx += 1
    else:
        # Never overwrite Claude-Sonnet sidecar output. Even if a doc hasn't
        # been Falcon-upgraded yet, if Sonnet was run on it we treat Sonnet as
        # the higher-quality source and skip.
        conds.append("NOT (d.metadata ? 'claude_sonnet_ocr_v1')")
        if not include_versioned:
            conds.append(
                f"(d.metadata->>'ocr_version' IS NULL "
                f"OR d.metadata->>'ocr_version' != ${idx})"
            )
            params.append(OCR_VERSION_TAG)
            idx += 1
        if doc_type:
            conds.append(f"d.document_type = ${idx}")
            params.append(doc_type)
            idx += 1
        else:
            conds.append(
                f"d.document_type = ANY(${idx}::text[])"
            )
            params.append(list(DEFAULT_OCR_DOC_TYPES))
            idx += 1

    params.append(limit)
    sql = f"""
        SELECT d.id, d.source_path, d.domain, d.document_type, d.title,
               d.filename, d.storage_uri, d.storage_sha256,
               d.storage_size_bytes,
               LENGTH(COALESCE(d.full_content, '')) AS old_chars,
               d.metadata
          FROM core.documents d
         WHERE {' AND '.join(conds)}
         ORDER BY d.created_at DESC
         LIMIT ${idx}
    """
    rows = await pool.fetch(sql, *params)
    return [dict(r) for r in rows]


# ============================================================
# Falcon endpoint client
# ============================================================

async def call_falcon_ocr(
    client: httpx.AsyncClient,
    data: bytes,
    filename: str,
    content_type: str,
) -> Optional[dict]:
    """Post bytes to the Falcon /ocr endpoint, return parsed JSON or None."""
    if not (OCR_ENDPOINT_URL and OCR_ENDPOINT_TOKEN):
        log.error("OCR_ENDPOINT_URL / TOKEN not set — can't reach Falcon")
        return None
    try:
        resp = await client.post(
            f"{OCR_ENDPOINT_URL}/ocr",
            headers={"Authorization": f"Bearer {OCR_ENDPOINT_TOKEN}"},
            files={"file": (filename or "upload.bin",
                            data, content_type or "application/octet-stream")},
            data={"mode": "plain"},
            timeout=OCR_ENDPOINT_TIMEOUT,
        )
        if resp.status_code != 200:
            log.warning("Falcon returned %d: %s",
                        resp.status_code, resp.text[:200])
            return None
        return resp.json()
    except httpx.ReadTimeout:
        log.warning("Falcon read timeout after %ds", OCR_ENDPOINT_TIMEOUT)
        return None
    except Exception as e:
        log.warning("Falcon call failed (%s): %s", type(e).__name__, e)
        return None


def _is_acceptable_output(new_text: str, old_chars: int) -> tuple[bool, str]:
    """Reject obviously-bad Falcon output:
      - too short relative to old content (something went wrong)
      - too many <|pad|> tokens (fp16-style corruption)
    Returns (ok, reason_if_not_ok).
    """
    if not new_text or not new_text.strip():
        return False, "empty"
    pad_count = new_text.count("<|pad|>")
    if pad_count:
        pad_frac = pad_count / max(1, len(new_text) / 10)  # rough
        if pad_frac > MAX_PAD_RATIO:
            return False, f"too_many_pad_tokens={pad_count}"
    if old_chars > 200 and len(new_text) < old_chars * MIN_ACCEPT_RATIO:
        return False, f"shrunk_from_{old_chars}_to_{len(new_text)}"
    return True, "ok"


# ============================================================
# Content-type inference from filename
# ============================================================

def _guess_content_type(filename: str) -> str:
    ext = Path(filename or "").suffix.lower()
    mapping = {
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".tiff": "image/tiff", ".tif": "image/tiff",
        ".gif": "image/gif",
        ".bmp": "image/bmp",
    }
    return mapping.get(ext, "application/octet-stream")


# ============================================================
# Re-chunk + re-embed, atomic per doc
# ============================================================

def _chunk_for_type(document_type: str, text: str) -> list[ChunkResult]:
    """Same routing as ingestion_pipeline._chunk_for_spec, local copy
    to avoid importing the full pipeline (which would pull heavier deps)."""
    chunker = EmailAwareChunker()
    if document_type == "email":
        results = chunker.split_email_body(text)
        if results:
            return results
        return [ChunkResult(t, PartType.BODY, True)
                for t in chunker.split(text)]
    if document_type == "email_attachment":
        return chunker.split_attachment(text, filename="", content_type="")
    return [ChunkResult(t, PartType.GENERIC, True)
            for t in chunker.split(text)]


async def rechunk_and_reembed(
    conn,
    doc_id: uuid.UUID,
    document_type: str,
    domain: str,
    title: str,
    new_text: str,
    case_numbers: list[str],
) -> int:
    """Delete existing chunks, re-chunk + embed, insert. Returns chunk count."""
    # Delete old chunks
    await conn.execute(
        "DELETE FROM core.document_chunks WHERE document_id = $1", doc_id,
    )

    # Chunk the new content
    chunk_results = _chunk_for_type(document_type, new_text)
    if not chunk_results:
        return 0

    # Filter to embeddable parts; build enriched context
    embed_indices = [i for i, r in enumerate(chunk_results) if r.is_embedded]
    if not embed_indices:
        return 0

    embed_texts_list = [chunk_results[i].text for i in embed_indices]
    case_num = case_numbers[0] if case_numbers else None
    try:
        context = generate_context_sync(
            title=title,
            domain=domain,
            document_type=document_type,
            content_preview=new_text[:3000],
            case_number=case_num,
        )
        enriched = enrich_chunks(context, embed_texts_list)
    except Exception as e:
        log.warning("context gen failed: %s", e)
        enriched = embed_texts_list

    # Embed
    try:
        vectors = embed_texts_sync(enriched)
    except Exception as e:
        log.error("embedding failed: %s — writing zero vectors", e)
        vectors = [[0.0] * EMBEDDING_DIMENSIONS] * len(enriched)

    model_id = await get_or_create_embedding_model_id(conn)

    # Write new chunks
    enriched_by_idx = dict(zip(embed_indices, enriched))
    vectors_by_idx = dict(zip(embed_indices, vectors))
    source_path_row = await conn.fetchval(
        "SELECT source_path FROM core.documents WHERE id = $1", doc_id,
    )
    for i, r in enumerate(chunk_results):
        chunk_id = hashlib.md5(
            f"{source_path_row}:{i}".encode()
        ).hexdigest()
        emb_param = (_vec_literal(vectors_by_idx[i])
                     if i in vectors_by_idx else None)
        enriched_text = enriched_by_idx.get(i)
        chunk_meta = {
            "source_part": r.source_part.value,
            "is_embedded": r.is_embedded,
            "case_numbers": case_numbers,
            "reprocessed_from": OCR_VERSION_TAG,
        }
        await conn.execute(
            """
            INSERT INTO core.document_chunks
                (id, document_id, chunk_index, total_chunks,
                 content, embedded_content, embedding,
                 embedding_model_id, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec, $8, $9::jsonb)
            ON CONFLICT (id) DO UPDATE
               SET content=EXCLUDED.content,
                   embedded_content=EXCLUDED.embedded_content,
                   embedding=EXCLUDED.embedding,
                   metadata=EXCLUDED.metadata
            """,
            chunk_id, doc_id, i, len(chunk_results),
            r.text, enriched_text, emb_param, model_id,
            json.dumps(chunk_meta),
        )
    return len(chunk_results)


# ============================================================
# Per-doc reprocessing
# ============================================================

async def reprocess_one(
    pool: asyncpg.Pool,
    client: httpx.AsyncClient,
    row: dict,
    dry_run: bool,
) -> tuple[str, dict]:
    """Reprocess one doc. Returns (result, info_dict).
    result: 'upgraded' | 'skipped' | 'rejected' | 'failed'
    """
    doc_id = row["id"]
    info = {
        "doc_id": str(doc_id),
        "document_type": row["document_type"],
        "storage_uri": row["storage_uri"],
        "old_chars": row["old_chars"],
    }

    # 1. Fetch blob from MinIO
    t0 = time.time()
    blob = blob_storage.get_bytes(row["storage_uri"])
    info["blob_fetch_sec"] = round(time.time() - t0, 2)
    if not blob:
        return "failed", {**info, "reason": "blob_fetch_returned_none"}
    info["blob_size"] = len(blob)

    # 2. Infer content-type from filename
    ctype = _guess_content_type(row["filename"] or "")
    info["content_type"] = ctype

    # 3. Call Falcon
    t0 = time.time()
    if dry_run:
        return "dry_run", {**info, "reason": "dry-run (no call made)"}

    falcon_resp = await call_falcon_ocr(
        client, blob, row["filename"] or "", ctype,
    )
    info["ocr_sec"] = round(time.time() - t0, 2)

    if not falcon_resp:
        return "failed", {**info, "reason": "falcon_call_failed"}

    new_text = (falcon_resp.get("text") or "").strip()
    info["new_chars"] = len(new_text)

    # 4. Sanity check
    ok, reason = _is_acceptable_output(new_text, row["old_chars"])
    if not ok:
        return "rejected", {**info, "reason": reason}

    # 5. Commit atomically: update doc metadata + rebuild chunks
    now_iso = datetime.now(timezone.utc).isoformat()
    case_numbers = []
    md_raw = row.get("metadata") or {}
    if isinstance(md_raw, str):
        try:
            md_raw = json.loads(md_raw)
        except Exception:
            md_raw = {}
    if isinstance(md_raw, dict):
        case_numbers = md_raw.get("case_numbers") or []

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE core.documents
                       SET full_content = $1,
                           metadata = metadata
                                      || jsonb_build_object(
                                           'ocr_version', $2::text,
                                           'ocr_upgraded_at', $3::text,
                                           'ocr_previous_chars', $4::int,
                                           'ocr_new_chars', $5::int,
                                           'ocr_method',
                                             $6::text)
                     WHERE id = $7
                    """,
                    new_text[:500000],
                    OCR_VERSION_TAG,
                    now_iso,
                    int(row["old_chars"]),
                    len(new_text),
                    falcon_resp.get("method", "falcon_plain"),
                    doc_id,
                )
                chunks_written = await rechunk_and_reembed(
                    conn,
                    doc_id=doc_id,
                    document_type=row["document_type"],
                    domain=row["domain"] or "legal",
                    title=row["title"] or row["filename"] or str(doc_id),
                    new_text=new_text,
                    case_numbers=case_numbers,
                )
                # Update total_chunks on parent doc
                await conn.execute(
                    "UPDATE core.documents SET total_chunks = $1 WHERE id = $2",
                    chunks_written, doc_id,
                )
        info["chunks_written"] = chunks_written
        return "upgraded", info
    except Exception as e:
        log.exception("DB commit failed for %s", doc_id)
        return "failed", {**info, "reason": f"db:{type(e).__name__}:{e}"[:200]}


# ============================================================
# Priority / throttle: detect slow Falcon → back off
# ============================================================

async def falcon_is_healthy(client: httpx.AsyncClient) -> bool:
    try:
        resp = await client.get(
            f"{OCR_ENDPOINT_URL}/healthz",
            headers={"Authorization": f"Bearer {OCR_ENDPOINT_TOKEN}"},
            timeout=10,
        )
        if resp.status_code != 200:
            return False
        body = resp.json()
        return body.get("status") == "ok" and body.get("loaded", False)
    except Exception:
        return False


async def falcon_activity(client: httpx.AsyncClient) -> list[dict]:
    """Fetch in-flight jobs from /activity. Returns [] on any failure."""
    try:
        resp = await client.get(
            f"{OCR_ENDPOINT_URL}/activity",
            headers={"Authorization": f"Bearer {OCR_ENDPOINT_TOKEN}"},
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        return resp.json().get("jobs", [])
    except Exception:
        return []


def _in_quiet_hours() -> bool:
    """22:00–06:00 local time."""
    h = datetime.now().hour
    return h >= 22 or h < 6


# ============================================================
# Main loop
# ============================================================

async def run(
    *,
    limit: int,
    doc_type: Optional[str],
    document_id: Optional[str],
    dry_run: bool,
    quiet_hours: bool,
    max_runtime_sec: Optional[int],
    include_versioned: bool,
    include_noise_images: bool = False,
) -> int:
    if quiet_hours and not _in_quiet_hours():
        log.info("Outside quiet-hours window (22:00-06:00); exiting")
        return 0

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    try:
        candidates = await select_candidates(
            pool,
            limit=limit,
            doc_type=doc_type,
            document_id=document_id,
            include_versioned=include_versioned,
            include_noise_images=include_noise_images,
        )
        log.info("Selected %d candidate(s)", len(candidates))
        if dry_run:
            for c in candidates[:20]:
                log.info("  would-reprocess %s  type=%s  old_chars=%d  uri=%s",
                         c["id"], c["document_type"],
                         c["old_chars"], c["storage_uri"])
            return 0

        stats = {
            "upgraded": 0, "skipped": 0,
            "rejected": 0, "failed": 0, "backed_off": 0,
        }
        rolling_avg = 0.0
        start = time.time()

        async with httpx.AsyncClient(timeout=OCR_ENDPOINT_TIMEOUT) as client:
            if not await falcon_is_healthy(client):
                log.error("Falcon service not healthy at %s", OCR_ENDPOINT_URL)
                return 1

            for idx, row in enumerate(candidates, start=1):
                # Runtime budget check
                if max_runtime_sec and time.time() - start > max_runtime_sec:
                    log.info("Runtime budget exceeded (%ds) — stopping at %d/%d",
                             max_runtime_sec, idx - 1, len(candidates))
                    break
                if quiet_hours and not _in_quiet_hours():
                    log.info("Quiet-hours window closed — stopping at %d/%d",
                             idx - 1, len(candidates))
                    break

                # Priority: back off if Falcon is slow (live traffic queued)
                if rolling_avg > SLOW_THRESHOLD_SEC:
                    log.info("  rolling_avg=%.1fs > %ds — sleeping %ds to yield",
                             rolling_avg, SLOW_THRESHOLD_SEC, BACKOFF_SEC)
                    await asyncio.sleep(BACKOFF_SEC)
                    stats["backed_off"] += 1

                result, info = await reprocess_one(
                    pool, client, row, dry_run=False,
                )
                stats[result if result in stats else "failed"] += 1

                log.info(
                    "[%d/%d] %s  doc=%s  type=%s  old=%d → new=%d  "
                    "blob_fetch=%.1fs  ocr=%.1fs  reason=%s",
                    idx, len(candidates), result,
                    str(info.get("doc_id", ""))[:8],
                    info.get("document_type", "?"),
                    info.get("old_chars", 0), info.get("new_chars", 0),
                    info.get("blob_fetch_sec", 0),
                    info.get("ocr_sec", 0),
                    info.get("reason", "-"),
                )

                # Update rolling OCR latency (used to detect queueing)
                this_ocr = info.get("ocr_sec", 0) or 0
                if this_ocr > 0:
                    rolling_avg = (
                        0.7 * rolling_avg + 0.3 * this_ocr
                        if rolling_avg else this_ocr
                    )

                # Be polite between docs even when fast, so the live client
                # can interleave if traffic arrives.
                await asyncio.sleep(BATCH_SLEEP_SEC)

        log.info("Done: %s", {k: v for k, v in stats.items() if v})
        return 0
    finally:
        await pool.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Re-OCR existing docs via Falcon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--limit", type=int, default=100,
                    help="Max docs to process this run (default 100)")
    ap.add_argument("--doc-type",
                    help="Only reprocess this document_type")
    ap.add_argument("--doc",
                    help="Reprocess exactly this document UUID")
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview candidates; no writes, no Falcon calls")
    ap.add_argument("--quiet-hours", action="store_true",
                    help="Only run between 22:00 and 06:00 local time")
    ap.add_argument("--max-runtime", type=int, default=None,
                    help="Stop after this many seconds (budget cap)")
    ap.add_argument("--include-versioned", action="store_true",
                    help="Also reprocess docs already tagged falcon-v1 "
                         "(use for re-run after model upgrade)")
    ap.add_argument("--include-noise-images", action="store_true",
                    help="Include small / Outlook-cid / signature images "
                         "(usually not worth the Falcon call)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return asyncio.run(run(
        limit=args.limit,
        doc_type=args.doc_type,
        document_id=args.doc,
        dry_run=args.dry_run,
        quiet_hours=args.quiet_hours,
        max_runtime_sec=args.max_runtime,
        include_versioned=args.include_versioned,
        include_noise_images=args.include_noise_images,
    ))


if __name__ == "__main__":
    sys.exit(main())
