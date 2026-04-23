#!/usr/bin/env python3
"""
Retention sweep — compute retain_until clocks and/or purge eligible docs.

Two related operations, gated by --mode:

  --mode=compute
      For every document, compute retain_until from its classification and
      lifecycle stamps (source_deleted_at, created_at) per the RETENTION
      policy table below. Idempotent and side-effect-light: only touches
      the retain_until column.

  --mode=purge   (default; ALSO dry-run by default)
      For each eligible document (retain_until < now() AND NOT legal_hold
      AND purged_at IS NULL), log what would be purged. Only writes when
      --apply is passed.
      Purging = delete blob bytes (MinIO object / FS file), NULL out
      full_content and raw_content, delete all chunks, stamp purged_at and
      purge_reason. The core.documents row persists as an audit record
      (filename, source_path, doc type, dates, classification) so we can
      later prove WHAT was destroyed WHEN.

Retention policy (per classification, starting point if caller doesn't
override via --policy-json):

  classification  | retention window | starts from
  ----------------+------------------+--------------------------------------
  spam            | 90d              | created_at   (hard cap, source or not)
  transactional   | 1y               | created_at
  content         | 5y               | source_deleted_at  (never purge while
                                     |                     still at source)
  privileged      | never auto       | legal_hold required for any purge
  unknown         | 5y               | created_at   (conservative)

Safety:
  - --apply required to write anything in purge mode.
  - Legal hold is hard-enforced in SQL (indexed partial).
  - Each purge is its own transaction — partial-run failures don't
    cascade. If a blob delete fails, the DB row is NOT stamped purged_at.

Usage:
  python retention_sweep.py --mode=compute                 # set retain_until
  python retention_sweep.py --mode=purge                   # dry-run sweep
  python retention_sweep.py --mode=purge --apply           # real sweep
  python retention_sweep.py --mode=purge --apply --limit=100
  python retention_sweep.py --mode=purge --document UUID   # single doc
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import asyncpg

# Env loader
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())


log = logging.getLogger("retention-sweep")

DATABASE_URL = os.environ["DATABASE_URL"]
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_INGEST_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_INGEST_SECRET_KEY")


# ============================================================
# Policy
# ============================================================

# Retention window per classification. None = never auto-purge.
_POLICY: dict[str, Optional[timedelta]] = {
    "spam":          timedelta(days=90),
    "transactional": timedelta(days=365),
    "content":       timedelta(days=5 * 365),
    "privileged":    None,
    "unknown":       timedelta(days=5 * 365),
}

# Which lifecycle stamp starts the clock. 'created' = created_at always;
# 'source_deleted' = source_deleted_at (NULL → no clock, never purge).
# IMPORTANT: unclassified docs are treated like content — kept indefinitely
# while they still exist at the source, with a 5-year grace period after the
# upstream mailbox deletes them. Purging an unclassified doc while it's still
# in the email server would be surprising data loss; the server itself is the
# backup of record.
_CLOCK_SOURCE: dict[str, str] = {
    "spam":          "created",
    "transactional": "created",
    "content":       "source_deleted",
    "privileged":    "never",
    "unknown":       "source_deleted",
}


def compute_retain_until(
    classification: Optional[str],
    created_at: datetime,
    source_deleted_at: Optional[datetime],
) -> Optional[datetime]:
    """Apply policy to return a retain_until timestamp, or None if the doc
    should never be auto-purged under current policy.

    An unclassified (classification IS NULL) doc is treated as 'unknown'.
    """
    cls = classification or "unknown"
    window = _POLICY.get(cls)
    clock = _CLOCK_SOURCE.get(cls, "never")

    if window is None or clock == "never":
        return None

    if clock == "created":
        return created_at + window

    if clock == "source_deleted":
        if source_deleted_at is None:
            return None  # still present at source — upstream is backup
        return source_deleted_at + window

    return None


# ============================================================
# Compute mode — recompute retain_until for every doc
# ============================================================

_COMPUTE_SELECT = """
SELECT id, classification, created_at, retain_until,
       (SELECT source_deleted_at
          FROM legal.email_metadata em
         WHERE em.document_id = d.id) AS source_deleted_at
  FROM core.documents d
 WHERE ($1::boolean OR retain_until IS NULL)
 ORDER BY created_at DESC
 LIMIT $2
"""


async def run_compute(
    pool: asyncpg.Pool, *, limit: int, overwrite: bool, dry_run: bool,
) -> dict[str, int]:
    rows = await pool.fetch(_COMPUTE_SELECT, overwrite, limit)
    stats = {"examined": 0, "updated": 0, "set_null": 0, "unchanged": 0}

    for r in rows:
        stats["examined"] += 1
        current = r["retain_until"]
        new_ru = compute_retain_until(
            r["classification"],
            r["created_at"],
            r["source_deleted_at"],
        )
        if current == new_ru:
            stats["unchanged"] += 1
            continue

        if dry_run:
            log.info("[dry-run] doc=%s retain_until: %s → %s",
                     r["id"], current, new_ru)
            stats["updated"] += 1
            continue

        await pool.execute(
            "UPDATE core.documents SET retain_until = $1 WHERE id = $2",
            new_ru, r["id"],
        )
        if new_ru is None:
            stats["set_null"] += 1
        else:
            stats["updated"] += 1

    return stats


# ============================================================
# Purge mode — delete eligible docs
# ============================================================

_ELIGIBLE_SELECT = """
SELECT d.id, d.source_path, d.filename, d.document_type, d.domain,
       d.created_at, d.retain_until, d.classification,
       d.storage_backend, d.storage_uri, d.storage_size_bytes
  FROM core.documents d
 WHERE d.purged_at IS NULL
   AND d.legal_hold = false
   AND d.retain_until IS NOT NULL
   AND d.retain_until < now()
 ORDER BY d.retain_until
 LIMIT $1
"""


def _bucket_and_key_from_uri(uri: str) -> Optional[tuple[str, str]]:
    """Parse s3://bucket/key → (bucket, key). Returns None on malformed input."""
    if not uri or not uri.startswith("s3://"):
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    return bucket, key


def _minio_client():
    import boto3
    from botocore.client import Config
    if not (MINIO_ACCESS_KEY and MINIO_SECRET_KEY):
        raise RuntimeError(
            "MINIO_INGEST_ACCESS_KEY / MINIO_INGEST_SECRET_KEY missing in .env"
        )
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO default, ignored on self-hosted
    )


def _delete_blob(
    backend: str, uri: str,
) -> tuple[bool, Optional[str]]:
    """Delete the physical blob. Returns (ok, error_message)."""
    if not backend or not uri:
        return True, None  # nothing to delete

    if backend == "minio" or backend == "s3":
        parsed = _bucket_and_key_from_uri(uri)
        if not parsed:
            return False, f"malformed s3 uri: {uri!r}"
        bucket, key = parsed
        try:
            client = _minio_client()
            # delete_object is a no-op if key doesn't exist; safe to call.
            client.delete_object(Bucket=bucket, Key=key)
            return True, None
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"[:400]

    if backend == "fs":
        try:
            p = Path(uri)
            if p.exists():
                p.unlink()
            return True, None
        except Exception as e:
            return False, f"fs delete failed: {e}"[:400]

    return False, f"unknown storage_backend {backend!r}"


async def _purge_one(
    pool: asyncpg.Pool, row: dict, dry_run: bool,
) -> tuple[bool, str]:
    """Purge a single document. Returns (succeeded, reason)."""
    doc_id = row["id"]
    backend = row["storage_backend"]
    uri = row["storage_uri"]

    if dry_run:
        return True, (
            f"would_purge backend={backend or 'none'} uri={uri or 'none'} "
            f"class={row['classification']} retain_until={row['retain_until']}"
        )

    # 1. Delete the blob first (if any). If this fails, do NOT stamp
    # purged_at — we want the sweep to retry next run.
    if backend and uri:
        ok, err = _delete_blob(backend, uri)
        if not ok:
            return False, f"blob_delete_failed: {err}"

    # 2. NULL content + delete chunks + stamp purged_at, all in one tx.
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM core.document_chunks WHERE document_id = $1",
                doc_id,
            )
            await conn.execute(
                """
                UPDATE core.documents
                   SET full_content = NULL,
                       raw_content = NULL,
                       storage_uri = NULL,
                       storage_sha256 = NULL,
                       storage_size_bytes = NULL,
                       purged_at = now(),
                       purge_reason = $1
                 WHERE id = $2
                """,
                f"retention_sweep:class={row['classification']}",
                doc_id,
            )
    return True, "purged"


async def run_purge(
    pool: asyncpg.Pool,
    *,
    limit: int,
    apply: bool,
    document: Optional[str],
) -> dict[str, int]:
    dry_run = not apply
    stats = {"examined": 0, "purged": 0, "failed": 0}

    if document:
        # Single-doc mode: fetch that doc even if it doesn't meet retain_until
        # (useful for manual cleanup after legal_hold is released)
        r = await pool.fetchrow(
            """
            SELECT id, source_path, filename, document_type, domain,
                   created_at, retain_until, classification,
                   storage_backend, storage_uri, storage_size_bytes,
                   legal_hold, purged_at
              FROM core.documents WHERE id = $1
            """,
            uuid.UUID(document),
        )
        if not r:
            log.error("document %s not found", document)
            return stats
        if r["legal_hold"]:
            log.error("document %s is on legal_hold — refusing to purge", document)
            return stats
        if r["purged_at"]:
            log.info("document %s already purged at %s", document, r["purged_at"])
            return stats
        rows = [r]
    else:
        rows = await pool.fetch(_ELIGIBLE_SELECT, limit)

    log.info("%d candidate document(s) for purge (dry_run=%s)",
             len(rows), dry_run)
    for r in rows:
        stats["examined"] += 1
        ok, msg = await _purge_one(pool, dict(r), dry_run=dry_run)
        if ok:
            stats["purged"] += 1
            log.info("  %s  doc=%s class=%s  %s",
                     "[dry-run]" if dry_run else "PURGED",
                     r["id"], r["classification"], msg)
        else:
            stats["failed"] += 1
            log.error("  FAILED  doc=%s  %s", r["id"], msg)

    return stats


# ============================================================
# CLI
# ============================================================

def main() -> int:
    ap = argparse.ArgumentParser(
        description="Retention computation + purge sweep",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "--mode", choices=("compute", "purge"), default="purge",
        help="compute: recompute retain_until clocks. "
             "purge: sweep eligible docs (dry-run unless --apply)",
    )
    ap.add_argument("--limit", type=int, default=500,
                    help="Max docs to process per run (default 500)")
    ap.add_argument("--apply", action="store_true",
                    help="Actually write changes (purge mode default is dry-run)")
    ap.add_argument("--overwrite", action="store_true",
                    help="[compute] Recompute retain_until even if already set")
    ap.add_argument("--document",
                    help="[purge] Operate on a single document UUID only")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def go() -> int:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        try:
            if args.mode == "compute":
                stats = await run_compute(
                    pool, limit=args.limit,
                    overwrite=args.overwrite,
                    dry_run=not args.apply,
                )
                log.info("compute done%s: %s",
                         " (dry-run)" if not args.apply else "",
                         dict(sorted(stats.items())))
            else:
                stats = await run_purge(
                    pool,
                    limit=args.limit,
                    apply=args.apply,
                    document=args.document,
                )
                log.info("purge done%s: %s",
                         " (dry-run)" if not args.apply else "",
                         dict(sorted(stats.items())))
            return 0
        finally:
            await pool.close()

    return asyncio.run(go())


if __name__ == "__main__":
    sys.exit(main())
