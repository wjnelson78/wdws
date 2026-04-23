"""Repair 217 docs where the parent email's MIME was accidentally stored in place
of the extracted PDF attachment.

For each doc flagged blob_audit_v1.classified_as='eml_mime':
  1. Read the current (wrong) blob — it's a full RFC822 email
  2. Parse with email.message_from_bytes
  3. Walk parts, match on decoded filename (Content-Disposition) vs the doc's filename
  4. If found: re-upload the correct PDF bytes to MinIO
     - reuse the same storage_uri (overwrite in place)
     - update storage_sha256 and storage_size_bytes
     - clear full_content, full_content_tsv regenerates automatically
     - delete chunks (so they'll be regenerated on next OCR)
     - record in metadata.blob_repair_v1
  5. If not found: mark as unrepairable with reason, don't touch blob

Does NOT re-OCR — the pilot/worker picks these up afterward.
"""
from __future__ import annotations

import argparse
import email
import hashlib
import json
import logging
import os
import sys
from email import policy
from typing import Optional
from unicodedata import normalize as unicode_normalize

import psycopg2
import psycopg2.extras

sys.path.insert(0, "/opt/wdws")
import blob_storage  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("eml-repair")

DATABASE_URL = ""
with open("/opt/wdws/.env") as f:
    for line in f:
        if line.startswith("DATABASE_URL="):
            DATABASE_URL = line.split("=", 1)[1].strip()
            break


def _normalize_name(s: str) -> str:
    """Case-fold + strip space/underscore/dash differences so we can match
    'Case_2-25-cv-00127-Notice_Of_Appearance_By_Defendant.pdf' against the
    same title in its native capitalization.
    """
    if not s:
        return ""
    s = unicode_normalize("NFKC", s).casefold()
    # drop common filename punctuation that differs across storage vs email
    return "".join(c for c in s if c.isalnum())


def find_matching_attachment(mime_bytes: bytes, want_filename: str) -> Optional[bytes]:
    """Walk MIME parts looking for an attachment whose decoded filename matches."""
    msg = email.message_from_bytes(mime_bytes, policy=policy.default)
    want = _normalize_name(want_filename)
    best_any_pdf: Optional[bytes] = None
    for part in msg.walk():
        fn = part.get_filename()
        if not fn:
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue
        # exact-ish match on normalized filename
        if _normalize_name(fn) == want:
            return payload
        # fallback: remember the first application/pdf we see in case the name
        # differs (punctuation/unicode) — many of these emails only have 1 PDF
        if part.get_content_type() == "application/pdf" and best_any_pdf is None:
            best_any_pdf = payload
    return best_any_pdf


def repair_row(conn, row: dict, dry_run: bool = False) -> str:
    """Return status string: 'repaired' | 'no_match' | 'not_pdf' | 'error'."""
    old_blob = blob_storage.get_bytes(row["storage_uri"])
    if not old_blob:
        return "error:blob_missing"

    recovered = find_matching_attachment(old_blob, row["filename"])
    if not recovered:
        return "no_match"
    if not recovered.startswith(b"%PDF-"):
        return "not_pdf"

    new_sha = hashlib.sha256(recovered).hexdigest()
    new_size = len(recovered)

    if dry_run:
        log.info("  [DRY] would repair %s %s: old=%d → new=%d bytes (new_sha=%s...)",
                 row["id"][:8], row["filename"][:40], len(old_blob), new_size, new_sha[:10])
        return "repaired"

    # Overwrite the blob at the same S3 URI
    parsed = blob_storage.parse_s3_uri(row["storage_uri"])
    if not parsed:
        return "error:bad_uri"
    bucket, key = parsed
    client = blob_storage._get_client()
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=recovered,
            ContentType="application/pdf",
        )
    except Exception as e:
        log.error("  %s put failed: %s", row["id"][:8], e)
        return "error:put_failed"

    # Update DB: clear full_content, update blob metadata, delete chunks,
    # stamp a repair marker. Next reprocess pass rebuilds full_content + chunks.
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE core.documents
               SET storage_sha256 = %s,
                   storage_size_bytes = %s,
                   full_content = NULL,
                   total_chunks = 0,
                   metadata = jsonb_set(
                       COALESCE(metadata, '{}'::jsonb) || %s::jsonb,
                       '{blob_audit_v1,classified_as}',
                       '"valid_pdf_repaired"'::jsonb,
                       true)
               WHERE id = %s""",
            (
                new_sha, new_size,
                json.dumps({
                    "blob_repair_v1": {
                        "repaired_at": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "old_size": len(old_blob),
                        "new_size": new_size,
                        "old_classification": "eml_mime",
                    },
                }),
                row["id"],
            ),
        )
        cur.execute("DELETE FROM core.document_chunks WHERE document_id = %s",
                    (row["id"],))
    conn.commit()
    return "repaired"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="only report matches, don't mutate blobs/DB")
    parser.add_argument("--limit", type=int, default=0, help="0 = all 217")
    args = parser.parse_args()

    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        limit_sql = f"LIMIT {args.limit}" if args.limit > 0 else ""
        cur.execute(f"""
            SELECT id::text AS id, filename, storage_uri
            FROM core.documents
            WHERE metadata->'blob_audit_v1'->>'classified_as' = 'eml_mime'
              AND legal_hold = false AND purged_at IS NULL
            {limit_sql}
        """)
        rows = cur.fetchall()

    log.info("Processing %d bogus .pdf row(s), dry_run=%s", len(rows), args.dry_run)
    counts: dict[str, int] = {}
    for i, row in enumerate(rows, start=1):
        result = repair_row(conn, row, dry_run=args.dry_run)
        counts[result] = counts.get(result, 0) + 1
        if i % 25 == 0 or i == len(rows):
            log.info("  %d/%d — %s", i, len(rows), counts)

    print()
    print("=" * 60)
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {k:25s}  {v:5d}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
