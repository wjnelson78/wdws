"""Audit .pdf documents in MinIO for content-type mismatches.

For every core.documents row where filename ~* '\\.pdf$' and storage_backend='minio',
range-read the first 32 bytes from MinIO and classify by magic:
  - valid_pdf:  starts with %PDF-
  - eml_mime:   starts with 'Received:' or 'From:' or 'Return-Path:' (raw RFC 822)
  - html:       starts with '<html' or '<!DOCTYPE'
  - png/jpg/gif: image magic bytes
  - zip/docx:   PK zip magic
  - empty:      0 bytes
  - unknown:    anything else (first 32 bytes logged)

Writes findings to core.documents.metadata.blob_audit_v1 so the scan is resumable
and queryable. Does NOT delete, reclassify, or mutate full_content / blobs.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Optional

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/opt/wdws")
import blob_storage  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("pdf-scan")

DATABASE_URL = os.environ.get("DATABASE_URL") or ""
if not DATABASE_URL:
    with open("/opt/wdws/.env") as f:
        for line in f:
            if line.startswith("DATABASE_URL="):
                DATABASE_URL = line.split("=", 1)[1].strip()
                break


def classify(first32: bytes) -> str:
    if not first32:
        return "empty"
    if first32[:5] == b"%PDF-":
        return "valid_pdf"
    head = first32[:40].decode("utf-8", errors="ignore").lstrip()
    lower = head.lower()
    if (
        lower.startswith("received:")
        or lower.startswith("from:")
        or lower.startswith("return-path:")
        or lower.startswith("delivered-to:")
        or lower.startswith("message-id:")
    ):
        return "eml_mime"
    if lower.startswith("<html") or lower.startswith("<!doctype"):
        return "html"
    if first32[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if first32[:3] == b"\xff\xd8\xff":
        return "jpg"
    if first32[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if first32[:2] == b"PK":
        return "zip_or_docx"
    return "unknown"


def peek(storage_uri: str, n: int = 32) -> Optional[bytes]:
    """Range-GET first n bytes from MinIO. Returns None if failed."""
    if not blob_storage.ENABLED:
        return None
    parsed = blob_storage.parse_s3_uri(storage_uri)
    if not parsed:
        return None
    bucket, key = parsed
    client = blob_storage._get_client()
    if client is None:
        return None
    try:
        resp = client.get_object(
            Bucket=bucket, Key=key, Range=f"bytes=0-{n - 1}"
        )
        return resp["Body"].read()
    except Exception as e:
        log.warning("  peek failed %s: %s", storage_uri, str(e)[:100])
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="0 = all")
    parser.add_argument("--reaudit", action="store_true", help="redo docs already audited")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        where = ["d.filename ~* '\\.pdf$'",
                 "d.storage_backend = 'minio'",
                 "d.storage_uri IS NOT NULL",
                 "d.legal_hold = false",
                 "d.purged_at IS NULL"]
        if not args.reaudit:
            where.append("NOT (d.metadata ? 'blob_audit_v1')")
        limit_sql = f"LIMIT {args.limit}" if args.limit > 0 else ""
        cur.execute(
            f"""SELECT d.id::text AS id, d.filename, d.storage_uri
                FROM core.documents d
                WHERE {' AND '.join(where)}
                {limit_sql}"""
        )
        rows = cur.fetchall()

    log.info("Auditing %d .pdf document(s)", len(rows))
    counts: dict[str, int] = {}
    mismatches: list[dict] = []

    for i, row in enumerate(rows, start=1):
        first32 = peek(row["storage_uri"]) or b""
        kind = classify(first32)
        counts[kind] = counts.get(kind, 0) + 1

        with conn.cursor() as wcur:
            wcur.execute(
                """UPDATE core.documents
                   SET metadata = jsonb_set(
                       COALESCE(metadata, '{}'::jsonb),
                       '{blob_audit_v1}',
                       %s::jsonb, true)
                   WHERE id = %s""",
                (json.dumps({
                    "classified_as": kind,
                    "first32_hex": first32.hex(),
                    "first32_text": first32.decode("utf-8", errors="replace")[:40],
                }), row["id"]),
            )
        if i % 100 == 0:
            conn.commit()
            log.info("  %d/%d scanned — %s", i, len(rows), counts)
        if kind != "valid_pdf":
            mismatches.append({
                "id": row["id"][:8],
                "filename": row["filename"][:60],
                "classified_as": kind,
                "first32": first32[:40].decode("utf-8", errors="replace"),
            })
    conn.commit()

    print("=" * 60)
    print(f"Total .pdf docs scanned:  {len(rows)}")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {k:15s}  {v:5d}  ({100*v/max(len(rows),1):.1f}%)")
    print()
    print(f"Mismatches (not_a_pdf): {len(mismatches)}")
    if mismatches and args.limit <= 500:
        print("\nFirst 20 mismatches:")
        for m in mismatches[:20]:
            print(f"  {m['id']}  [{m['classified_as']:10s}]  {m['filename']}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
