"""
Blob storage adapter — uploads raw bytes to MinIO and reads them back.

One seam between the rest of the system and object storage. Callers pass
bytes in and get back a storage URI + sha256 + size that they persist into
`core.documents.storage_backend / storage_uri / storage_sha256 /
storage_size_bytes`. Later, retention_sweep.py and the MCP download tools
read from here to access or delete those bytes.

Design:
  * Scoped service credentials — reads MINIO_INGEST_ACCESS_KEY /
    MINIO_INGEST_SECRET_KEY from env. NOT the root user.
  * Singleton client per process — boto3 creates real TCP connections;
    reuse them.
  * Key naming is deterministic and namespaced by content type:
        wdws-email-eml          :  <mailbox>/<sha256-prefix>/<graph-id>.eml
        wdws-email-attachments  :  <parent-doc>/<sha256-prefix>/<safe-fn>
        wdws-document-blobs     :  <domain>/<sha256-prefix>/<safe-fn>
    Rationale: sha256-prefix in the key means accidental duplicates
    collide on the same object (versioning keeps both); accidental
    re-ingests don't create N copies of the same bytes.
  * Returns None on any upload failure — caller decides whether to proceed
    without a blob (falls back to old FS behavior) or to abort.

Environment:
  MINIO_ENDPOINT             http://127.0.0.1:9000
  MINIO_INGEST_ACCESS_KEY    scoped key (see MinIO setup)
  MINIO_INGEST_SECRET_KEY    scoped secret
  MINIO_BUCKET_EML           default: wdws-email-eml
  MINIO_BUCKET_ATTACHMENTS   default: wdws-email-attachments
  MINIO_BUCKET_BLOBS         default: wdws-document-blobs
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

# Load /opt/wdws/.env BEFORE module-level constants read env vars, so
# callers that just `import blob_storage` pick up the right credentials.
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

log = logging.getLogger("blob-storage")


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_INGEST_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.environ.get("MINIO_INGEST_SECRET_KEY", "")

BUCKET_EML = os.environ.get("MINIO_BUCKET_EML", "wdws-email-eml")
BUCKET_ATTACHMENTS = os.environ.get(
    "MINIO_BUCKET_ATTACHMENTS", "wdws-email-attachments",
)
BUCKET_BLOBS = os.environ.get("MINIO_BUCKET_BLOBS", "wdws-document-blobs")

# Callers check this to know whether blob storage is even configured.
ENABLED = bool(MINIO_ACCESS_KEY and MINIO_SECRET_KEY)


# ============================================================
# Result type
# ============================================================

@dataclass
class BlobRef:
    """What the pipeline persists alongside the core.documents row."""
    storage_backend: str        # 'minio'
    storage_uri: str            # 's3://bucket/key'
    storage_sha256: str         # hex-encoded sha256 of the payload
    storage_size_bytes: int


# ============================================================
# Client singleton
# ============================================================

_client = None
_client_lock = threading.Lock()


def _get_client():
    """Lazy boto3 S3 client. Returns None if blob storage isn't configured."""
    global _client
    if not ENABLED:
        return None
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:
            return _client
        try:
            import boto3
            from botocore.client import Config
            _client = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 3, "mode": "standard"},
                ),
                region_name="us-east-1",  # MinIO ignores this but boto3 needs it
            )
        except Exception as e:
            log.error("Failed to initialize MinIO client: %s", e)
            _client = None
    return _client


# ============================================================
# Key derivation
# ============================================================

_SAFE_FN_RE = re.compile(r"[^\w.\-]+")


def _safe_path_fragment(raw: str, max_len: int = 80) -> str:
    """Make a string safe for use as an S3 key fragment."""
    s = _SAFE_FN_RE.sub("_", (raw or "").strip())
    s = s.strip("_")
    return (s or "untitled")[:max_len]


def _prefix_from_sha256(digest: str) -> str:
    """Two-level directory split from the sha256 hex to avoid hot directories.
    `fa/3b/<sha>` — similar to git's object store layout.
    """
    return f"{digest[:2]}/{digest[2:4]}"


def _eml_key(mailbox: str, graph_message_id: str, sha256: str) -> str:
    mbox = _safe_path_fragment(mailbox, 60)
    gid = _safe_path_fragment(graph_message_id, 80)
    return f"{mbox}/{_prefix_from_sha256(sha256)}/{gid}.eml"


def _attachment_key(parent_doc_id: str, filename: str, sha256: str) -> str:
    parent = _safe_path_fragment(parent_doc_id, 36)
    fn = _safe_path_fragment(filename, 80)
    return f"{parent}/{_prefix_from_sha256(sha256)}/{fn}"


def _blob_key(domain: str, filename: str, sha256: str) -> str:
    dom = _safe_path_fragment(domain, 40)
    fn = _safe_path_fragment(filename, 80)
    return f"{dom}/{_prefix_from_sha256(sha256)}/{fn}"


# ============================================================
# Upload API
# ============================================================

def _sha256_and_size(data: bytes) -> tuple[str, int]:
    return hashlib.sha256(data).hexdigest(), len(data)


def _put_object(
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
) -> bool:
    """Upload bytes to a bucket/key. Returns True on success, False on failure."""
    client = _get_client()
    if client is None:
        return False
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type or "application/octet-stream",
            ChecksumAlgorithm="SHA256",
        )
        return True
    except Exception as e:
        log.error("MinIO put failed (bucket=%s key=%s): %s", bucket, key, e)
        return False


def upload_eml(
    mime_bytes: bytes,
    mailbox: str,
    graph_message_id: str,
) -> Optional[BlobRef]:
    """Upload an .eml MIME payload to the email bucket. Idempotent: same sha256
    → same key; MinIO bucket versioning keeps prior copies if the content
    differs (shouldn't happen in practice)."""
    if not ENABLED or not mime_bytes:
        return None
    sha, size = _sha256_and_size(mime_bytes)
    key = _eml_key(mailbox, graph_message_id, sha)
    if _put_object(BUCKET_EML, key, mime_bytes, "message/rfc822"):
        return BlobRef(
            storage_backend="minio",
            storage_uri=f"s3://{BUCKET_EML}/{key}",
            storage_sha256=sha,
            storage_size_bytes=size,
        )
    return None


def upload_attachment(
    data: bytes,
    parent_doc_id: str,
    filename: str,
    content_type: str = "",
) -> Optional[BlobRef]:
    if not ENABLED or not data:
        return None
    sha, size = _sha256_and_size(data)
    key = _attachment_key(parent_doc_id, filename, sha)
    if _put_object(BUCKET_ATTACHMENTS, key, data,
                   content_type or "application/octet-stream"):
        return BlobRef(
            storage_backend="minio",
            storage_uri=f"s3://{BUCKET_ATTACHMENTS}/{key}",
            storage_sha256=sha,
            storage_size_bytes=size,
        )
    return None


def upload_blob(
    data: bytes,
    domain: str,
    filename: str,
    content_type: str = "",
) -> Optional[BlobRef]:
    """Catch-all for non-email blobs (PDFs, medical records, etc.)."""
    if not ENABLED or not data:
        return None
    sha, size = _sha256_and_size(data)
    key = _blob_key(domain, filename, sha)
    if _put_object(BUCKET_BLOBS, key, data,
                   content_type or "application/octet-stream"):
        return BlobRef(
            storage_backend="minio",
            storage_uri=f"s3://{BUCKET_BLOBS}/{key}",
            storage_sha256=sha,
            storage_size_bytes=size,
        )
    return None


# ============================================================
# Download / metadata API
# ============================================================

def parse_s3_uri(uri: str) -> Optional[tuple[str, str]]:
    """'s3://bucket/key' → (bucket, key). Returns None on malformed input."""
    if not uri or not uri.startswith("s3://"):
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    return bucket, key


def get_bytes(storage_uri: str) -> Optional[bytes]:
    """Fetch the blob bytes for an s3:// URI. Returns None if missing / error."""
    if not ENABLED:
        return None
    parsed = parse_s3_uri(storage_uri)
    if not parsed:
        log.error("Malformed storage_uri: %r", storage_uri)
        return None
    bucket, key = parsed

    client = _get_client()
    if client is None:
        return None
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()
    except Exception as e:
        log.error("MinIO get failed (bucket=%s key=%s): %s", bucket, key, e)
        return None


def head(storage_uri: str) -> Optional[dict]:
    """HEAD a blob — returns {size, content_type, etag, last_modified} or None."""
    if not ENABLED:
        return None
    parsed = parse_s3_uri(storage_uri)
    if not parsed:
        return None
    bucket, key = parsed
    client = _get_client()
    if client is None:
        return None
    try:
        resp = client.head_object(Bucket=bucket, Key=key)
        return {
            "size": resp.get("ContentLength"),
            "content_type": resp.get("ContentType"),
            "etag": resp.get("ETag", "").strip('"'),
            "last_modified": resp.get("LastModified"),
        }
    except Exception as e:
        log.debug("MinIO head failed (bucket=%s key=%s): %s", bucket, key, e)
        return None


def delete_blob(storage_uri: str) -> bool:
    """Delete a blob by URI. Idempotent (returns True if already absent).
    Used by retention_sweep."""
    if not ENABLED:
        return False
    parsed = parse_s3_uri(storage_uri)
    if not parsed:
        return False
    bucket, key = parsed
    client = _get_client()
    if client is None:
        return False
    try:
        client.delete_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        log.error("MinIO delete failed (bucket=%s key=%s): %s", bucket, key, e)
        return False


# ============================================================
# CLI self-test
# ============================================================

if __name__ == "__main__":
    # Env is already loaded at module-top; just set up logging here.
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    print(f"ENABLED: {ENABLED}")
    print(f"MINIO_ENDPOINT: {MINIO_ENDPOINT}")
    print(f"Buckets: eml={BUCKET_EML}  att={BUCKET_ATTACHMENTS}  blobs={BUCKET_BLOBS}")

    sample = b"sample eml blob for blob_storage selftest\n" * 40  # ~1.5KB
    ref = upload_eml(sample, "test@example.com", "selftest-graph-id")
    print(f"upload_eml → {ref}")
    if ref:
        got = get_bytes(ref.storage_uri)
        print(f"get_bytes → {len(got) if got else 'None'} bytes, match={got == sample}")
        info = head(ref.storage_uri)
        print(f"head → {info}")
        deleted = delete_blob(ref.storage_uri)
        print(f"delete_blob → {deleted}")
