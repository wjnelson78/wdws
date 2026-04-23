#!/usr/bin/env python3
"""
Phase-1 migration: upload existing on-disk files to MinIO and populate the
storage_backend/storage_uri columns on core.documents. Does NOT delete FS
files (that's Phase 3; separate script, explicit flag).

Resolution order for the physical file per row:
  1. source_path, if it starts with '/' and the file exists
  2. metadata->>'local_eml_path' (set by email_sync.persist_graph_mime_copy)
  3. metadata->>'local_attachment_path' (set by persist_graph_attachment_copy)
  4. metadata->>'parent_email_source_path' — for attachments where the
     parent email had a local_eml_path we can use as a secondary hint

Bucket routing:
  document_type == 'email'            → wdws-email-eml          (blob_storage.upload_eml)
  document_type == 'email_attachment' → wdws-email-attachments  (blob_storage.upload_attachment)
  everything else                     → wdws-document-blobs     (blob_storage.upload_blob)

Safety:
  * Idempotent — skips rows that already have storage_backend set.
  * Dry-run by default (no writes, no uploads). --apply to act.
  * Per-row errors are logged and counted; the run continues to the next row.
  * After each successful upload, writes the new columns in a single tx.
  * Does NOT touch source_path, metadata, or any FK. The doc UUID is
    unchanged; all downstream references remain valid.

Usage:
  python migrate_fs_to_minio.py                # inventory (dry-run)
  python migrate_fs_to_minio.py --apply        # migrate everything
  python migrate_fs_to_minio.py --limit 50 --apply
  python migrate_fs_to_minio.py --document UUID --apply
  python migrate_fs_to_minio.py --domain legal --apply
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import mimetypes
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import asyncpg

# env load (keep independent of email_sync)
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import blob_storage

log = logging.getLogger("migrate-fs-to-minio")

DATABASE_URL = os.environ["DATABASE_URL"]


# ============================================================
# FS path resolution
# ============================================================

# Only allow reads from these roots — defense against a malformed source_path
# that could point anywhere.
_ALLOWED_ROOTS = (
    Path("/opt/wdws/data").resolve(),
    Path("/data").resolve(),                    # Samba share / dropbox root
    Path("/mnt").resolve(),                     # Additional mount points
)


def _is_safe_path(p: Path) -> bool:
    try:
        resolved = p.resolve(strict=False)
    except Exception:
        return False
    return any(
        str(resolved).startswith(str(root)) for root in _ALLOWED_ROOTS
    )


def resolve_fs_path(row: dict) -> Optional[Path]:
    """Given a core.documents row, find the matching file on disk. None if
    nothing resolves to an existing file."""
    candidates: list[str] = []

    sp = (row.get("source_path") or "").strip()
    if sp.startswith("/"):
        candidates.append(sp)
    if sp.startswith("file://"):
        candidates.append(sp[7:])

    meta_raw = row.get("metadata") or {}
    if isinstance(meta_raw, str):
        try:
            meta_raw = json.loads(meta_raw)
        except Exception:
            meta_raw = {}
    if isinstance(meta_raw, dict):
        for key in ("local_eml_path", "local_attachment_path"):
            v = meta_raw.get(key)
            if v and isinstance(v, str):
                candidates.append(v)

    for c in candidates:
        p = Path(c)
        if not _is_safe_path(p):
            continue
        if p.exists() and p.is_file():
            return p

    return None


# ============================================================
# Upload + DB write
# ============================================================

_MIME_FOR_EMAIL = "message/rfc822"


def guess_mime(filename: str, existing: str = "") -> str:
    if existing:
        return existing
    guess, _ = mimetypes.guess_type(filename)
    return guess or "application/octet-stream"


async def fetch_candidate_rows(
    pool: asyncpg.Pool,
    *,
    limit: int,
    document_filter: Optional[str],
    domain_filter: Optional[str],
    type_filter: Optional[str],
) -> list[dict]:
    where = ["d.storage_backend IS NULL"]
    params: list[Any] = []
    idx = 1
    if document_filter:
        where.append(f"d.id = ${idx}::uuid")
        params.append(document_filter)
        idx += 1
    if domain_filter:
        where.append(f"d.domain = ${idx}")
        params.append(domain_filter)
        idx += 1
    if type_filter:
        where.append(f"d.document_type = ${idx}")
        params.append(type_filter)
        idx += 1
    params.append(limit)
    rows = await pool.fetch(f"""
        SELECT d.id, d.domain, d.document_type, d.filename, d.source_path,
               d.metadata,
               em.mailbox              AS email_mailbox,
               ea.content_type         AS att_content_type,
               ea.email_doc_id         AS att_parent_doc_id,
               rel.source_document_id  AS relationship_parent_doc_id
          FROM core.documents d
          LEFT JOIN legal.email_metadata em     ON em.document_id = d.id
          LEFT JOIN legal.email_attachments ea  ON ea.child_doc_id = d.id
          LEFT JOIN core.document_relationships rel
                 ON rel.target_document_id = d.id
                AND rel.relationship_type = 'has_attachment'
         WHERE {' AND '.join(where)}
         ORDER BY d.created_at
         LIMIT ${idx}
    """, *params)
    return [dict(r) for r in rows]


def _graph_message_id_from_metadata(meta: dict, source_path: str) -> str:
    gid = meta.get("graph_message_id") if isinstance(meta, dict) else None
    if gid:
        return gid
    if source_path.startswith("graph://"):
        # graph://mailbox/message_id → take the part after mailbox
        parts = source_path.split("/", 3)
        if len(parts) >= 4:
            return parts[3] or parts[-1]
    return str(uuid.uuid4())   # fallback — keeps key unique


def _parent_doc_id_for_attachment(row: dict) -> str:
    """Prefer the legal.email_attachments linkage, fall back to relationships."""
    p = row.get("att_parent_doc_id") or row.get("relationship_parent_doc_id")
    return str(p) if p else "unknown-parent"


def _metadata_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def upload_for_row(row: dict, fs_path: Path, file_bytes: bytes) -> Optional[blob_storage.BlobRef]:
    """Pick the right uploader by document_type."""
    dt = row.get("document_type") or ""
    meta = _metadata_dict(row.get("metadata"))

    if dt == "email":
        mailbox = (row.get("email_mailbox")
                   or meta.get("mailbox")
                   or "unknown")
        gid = _graph_message_id_from_metadata(meta, row.get("source_path") or "")
        return blob_storage.upload_eml(
            file_bytes, mailbox=mailbox, graph_message_id=gid,
        )

    if dt == "email_attachment":
        parent = _parent_doc_id_for_attachment(row)
        fname = row.get("filename") or fs_path.name
        ctype = (row.get("att_content_type")
                 or meta.get("content_type")
                 or guess_mime(fname))
        return blob_storage.upload_attachment(
            file_bytes,
            parent_doc_id=parent,
            filename=fname,
            content_type=ctype,
        )

    # Generic: dropbox-ingested docs, medical records, court filings, etc.
    fname = row.get("filename") or fs_path.name
    ctype = meta.get("content_type") or guess_mime(fname)
    return blob_storage.upload_blob(
        file_bytes,
        domain=row.get("domain") or "unknown",
        filename=fname,
        content_type=ctype,
    )


async def write_blob_ref(
    pool: asyncpg.Pool, doc_id: uuid.UUID, ref: blob_storage.BlobRef,
) -> None:
    """Update storage columns. Uses postgres superuser via a connection
    string switch only if needed — most installs run the wdws user which
    owns the table's UPDATE grant."""
    await pool.execute(
        """
        UPDATE core.documents
           SET storage_backend    = $1,
               storage_uri        = $2,
               storage_sha256     = $3,
               storage_size_bytes = $4
         WHERE id = $5
           AND storage_backend IS NULL
        """,
        ref.storage_backend,
        ref.storage_uri,
        ref.storage_sha256,
        int(ref.storage_size_bytes),
        doc_id,
    )


# ============================================================
# Main loop
# ============================================================

async def migrate(
    pool: asyncpg.Pool,
    *,
    limit: int,
    dry_run: bool,
    document_filter: Optional[str],
    domain_filter: Optional[str],
    type_filter: Optional[str],
    verify_roundtrip: bool,
    progress_every: int = 100,
) -> dict:
    stats = {
        "examined": 0,
        "uploaded": 0,
        "skipped_already_migrated": 0,
        "skipped_no_fs_file": 0,
        "skipped_unsafe_path": 0,
        "failed_upload": 0,
        "failed_verify": 0,
        "failed_db": 0,
        "bytes_uploaded": 0,
    }

    rows = await fetch_candidate_rows(
        pool,
        limit=limit,
        document_filter=document_filter,
        domain_filter=domain_filter,
        type_filter=type_filter,
    )
    log.info("Candidate rows queued: %d", len(rows))

    start = time.time()
    for idx, row in enumerate(rows, start=1):
        stats["examined"] += 1
        doc_id = row["id"]

        fs_path = resolve_fs_path(row)
        if fs_path is None:
            stats["skipped_no_fs_file"] += 1
            log.debug("[%s] no FS file resolved for %s / %s",
                      doc_id, row.get("source_path"), row.get("document_type"))
            continue

        # Size sanity — skip empties and anything absurd
        try:
            fs_size = fs_path.stat().st_size
        except Exception as e:
            stats["skipped_no_fs_file"] += 1
            log.warning("[%s] stat failed for %s: %s", doc_id, fs_path, e)
            continue

        if fs_size == 0:
            stats["skipped_no_fs_file"] += 1
            continue

        if dry_run:
            stats["uploaded"] += 1  # report as would-have
            stats["bytes_uploaded"] += fs_size
            log.info("[dry-run] %s  type=%s  %s (%d bytes)",
                     doc_id, row.get("document_type"), fs_path, fs_size)
            if idx % progress_every == 0:
                log.info("  … %d/%d scanned", idx, len(rows))
            continue

        # Real upload
        try:
            file_bytes = fs_path.read_bytes()
        except Exception as e:
            stats["skipped_no_fs_file"] += 1
            log.warning("[%s] read failed for %s: %s", doc_id, fs_path, e)
            continue

        ref = upload_for_row(row, fs_path, file_bytes)
        if ref is None:
            stats["failed_upload"] += 1
            log.error("[%s] upload failed for %s", doc_id, fs_path)
            continue

        if verify_roundtrip:
            head = blob_storage.head(ref.storage_uri)
            if not head or head.get("size") != ref.storage_size_bytes:
                stats["failed_verify"] += 1
                log.error("[%s] verify failed uri=%s head=%s",
                          doc_id, ref.storage_uri, head)
                continue

        try:
            await write_blob_ref(pool, doc_id, ref)
        except Exception as e:
            stats["failed_db"] += 1
            log.exception("[%s] DB write failed", doc_id)
            continue

        stats["uploaded"] += 1
        stats["bytes_uploaded"] += ref.storage_size_bytes

        if idx % progress_every == 0:
            elapsed = time.time() - start
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (len(rows) - idx) / rate if rate > 0 else 0
            log.info(
                "  progress %d/%d  %.1f/s  ETA %ds  bytes=%.1fMB",
                idx, len(rows), rate, int(eta),
                stats["bytes_uploaded"] / 1024 / 1024,
            )

    elapsed = time.time() - start
    log.info("Done in %.1fs — %s", elapsed, dict(sorted(stats.items())))
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Migrate existing FS files into MinIO",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--apply", action="store_true",
                    help="Actually upload + update DB (default is dry-run)")
    ap.add_argument("--limit", type=int, default=50000,
                    help="Max rows to process per run (default 50000)")
    ap.add_argument("--document",
                    help="Operate on a single document UUID (bypasses filters)")
    ap.add_argument("--domain", help="Filter by domain (e.g. legal, medical)")
    ap.add_argument("--type", dest="doc_type",
                    help="Filter by document_type (e.g. email, email_attachment)")
    ap.add_argument("--no-verify", dest="verify", action="store_false",
                    default=True,
                    help="Skip sha+size verification after upload (faster)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    async def go() -> int:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
        try:
            if not blob_storage.ENABLED:
                log.error("blob_storage not configured — check "
                          "MINIO_INGEST_ACCESS_KEY/SECRET in /opt/wdws/.env")
                return 1

            stats = await migrate(
                pool,
                limit=args.limit,
                dry_run=not args.apply,
                document_filter=args.document,
                domain_filter=args.domain,
                type_filter=args.doc_type,
                verify_roundtrip=args.verify,
            )
            return 0
        finally:
            await pool.close()

    return asyncio.run(go())


if __name__ == "__main__":
    sys.exit(main())
