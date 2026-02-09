#!/usr/bin/env python3
"""
Graph API Email Sync for WDWS Enterprise PostgreSQL.

Connects to Microsoft Graph API using app-only (client credentials) auth,
fetches emails to/from specified domains for both william@ and athena@
mailboxes, and ingests them directly into the enterprise PostgreSQL database
with embeddings.

Usage:
    python email_sync.py                          # Sync snoco emails from both mailboxes
    python email_sync.py --domains snoco.org co.snohomish.wa.us
    python email_sync.py --mailbox william@seattleseahawks.me
    python email_sync.py --since 2024-01-01
    python email_sync.py --dry-run                # Show what would be fetched
"""
import os
import sys
import json
import re
import uuid
import hashlib
import asyncio
import logging
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Set, Tuple
from email import policy
import email as email_lib

import httpx
import asyncpg

# ============================================================
# Configuration
# ============================================================
# Load .env
from pathlib import Path as _Path
_env_file = _Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            import os as _os
            _os.environ.setdefault(_k.strip(), _v.strip())


DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL — check /opt/wdws/.env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY — check /opt/wdws/.env")

# Graph API credentials
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
if not GRAPH_CLIENT_ID:
    raise RuntimeError("Missing GRAPH_CLIENT_ID — check /opt/wdws/.env")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
if not GRAPH_CLIENT_SECRET:
    raise RuntimeError("Missing GRAPH_CLIENT_SECRET — check /opt/wdws/.env")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
if not GRAPH_TENANT_ID:
    raise RuntimeError("Missing GRAPH_TENANT_ID — check /opt/wdws/.env")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMENSIONS = 3072
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# Mailboxes to scan
MAILBOXES = [
    "william@seattleseahawks.me",
    "athena@seattleseahawks.me",
]

# Target domains — emails to/from these domains
TARGET_DOMAINS = [
    "snoco.org",
    "co.snohomish.wa.us",
]

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_sync")

# ============================================================
# Case number extraction (shared with ingest.py)
# ============================================================

STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
FEDERAL_CASE_RE = re.compile(
    r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?', re.IGNORECASE
)


def extract_case_numbers_from_text(text: str) -> List[str]:
    """Extract and normalize case numbers from text content."""
    found = set()
    for m in FEDERAL_CASE_RE.finditer(text):
        court, year, seq, judge = m.groups()
        normalized = f"{court}{year}CV{seq}"
        if judge:
            normalized += judge.upper()
        found.add(normalized.upper())
    for m in STATE_CASE_RE.finditer(text):
        found.add(m.group(0))
    return list(found)


# ============================================================
# Data classes
# ============================================================

@dataclass
class DocumentChunk:
    content: str
    metadata: Dict
    chunk_index: int
    total_chunks: int


@dataclass
class IngestedDocument:
    doc_id: str
    domain: str
    source_path: str
    filename: str
    document_type: str
    title: str
    content_hash: str
    full_content: str
    metadata: Dict
    chunks: List[DocumentChunk] = field(default_factory=list)
    case_numbers: List[str] = field(default_factory=list)
    email_meta: Optional[Dict] = None


# ============================================================
# Text chunker
# ============================================================

class TextChunker:
    def __init__(self, chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.separators = ["\n\n", "\n", ". ", " ", ""]

    def split(self, text: str) -> List[str]:
        if not text:
            return []
        return self._split_text(text, self.separators)

    def _split_text(self, text: str, separators: list) -> list:
        final_chunks = []
        separator = separators[-1]
        new_separators = []
        for i, sep in enumerate(separators):
            if sep == "" or sep in text:
                separator = sep
                new_separators = separators[i + 1:]
                break
        splits = text.split(separator) if separator else list(text)
        good_splits = []
        for s in splits:
            if len(s) < self.chunk_size:
                good_splits.append(s)
            else:
                if good_splits:
                    merged = self._merge_splits(good_splits, separator)
                    final_chunks.extend(merged)
                    good_splits = []
                if new_separators:
                    final_chunks.extend(self._split_text(s, new_separators))
                else:
                    final_chunks.append(s)
        if good_splits:
            merged = self._merge_splits(good_splits, separator)
            final_chunks.extend(merged)
        return final_chunks

    def _merge_splits(self, splits: list, separator: str) -> list:
        docs = []
        current = []
        total = 0
        for s in splits:
            s_len = len(s)
            if total + s_len + (len(separator) if current else 0) > self.chunk_size:
                if current:
                    docs.append(separator.join(current))
                    while total > self.chunk_overlap and len(current) > 1:
                        total -= len(current[0]) + len(separator)
                        current.pop(0)
                current = [s]
                total = s_len
            else:
                current.append(s)
                total += s_len + (len(separator) if len(current) > 1 else 0)
        if current:
            docs.append(separator.join(current))
        return docs


# ============================================================
# Embedding client
# ============================================================

class EmbeddingClient:
    def __init__(self, api_key: str = OPENAI_API_KEY, model: str = EMBEDDING_MODEL):
        self.api_key = api_key
        self.model = model
        self.client = httpx.AsyncClient(timeout=60.0)
        self.total_tokens = 0
        self.total_requests = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []
        truncated = [t[:30000] for t in texts]
        resp = await self.client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={"input": truncated, "model": self.model, "dimensions": EMBEDDING_DIMENSIONS},
        )
        resp.raise_for_status()
        data = resp.json()
        self.total_tokens += data.get("usage", {}).get("total_tokens", 0)
        self.total_requests += 1
        return [d["embedding"] for d in data["data"]]

    async def close(self):
        await self.client.aclose()


# ============================================================
# Graph API client
# ============================================================

class GraphClient:
    """Microsoft Graph API client using app-only (client credentials) auth."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    async def authenticate(self) -> bool:
        """Get app-only access token using client credentials."""
        try:
            resp = await self.client.post(
                TOKEN_URL,
                data={
                    "client_id": GRAPH_CLIENT_ID,
                    "client_secret": GRAPH_CLIENT_SECRET,
                    "scope": "https://graph.microsoft.com/.default",
                    "grant_type": "client_credentials",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            self.access_token = data["access_token"]
            self.token_expires_at = time.time() + data.get("expires_in", 3600) - 300
            log.info("✓ Graph API authenticated (app-only, client credentials)")
            return True
        except Exception as e:
            log.error(f"✗ Graph API authentication failed: {e}")
            return False

    async def _ensure_token(self):
        """Refresh token if expired."""
        if time.time() >= self.token_expires_at:
            await self.authenticate()

    async def _get(self, url: str, params: dict = None) -> Optional[dict]:
        """Make authenticated GET request."""
        await self._ensure_token()
        try:
            resp = await self.client.get(
                url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                params=params,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # Rate limited — wait and retry
                retry_after = int(e.response.headers.get("Retry-After", "10"))
                log.warning(f"  Rate limited, waiting {retry_after}s...")
                await asyncio.sleep(retry_after)
                return await self._get(url, params)
            log.error(f"  Graph API error {e.response.status_code}: {e.response.text[:200]}")
            return None
        except Exception as e:
            log.error(f"  Graph API request failed: {e}")
            return None

    async def _get_raw(self, url: str) -> Optional[bytes]:
        """Make authenticated GET request returning raw bytes (for MIME)."""
        await self._ensure_token()
        try:
            resp = await self.client.get(
                url,
                headers={"Authorization": f"Bearer {self.access_token}"},
            )
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", "10"))
                log.warning(f"  Rate limited, waiting {retry_after}s...")
                await asyncio.sleep(retry_after)
                return await self._get_raw(url)
            log.error(f"  MIME fetch error {e.response.status_code}")
            return None
        except Exception as e:
            log.error(f"  MIME fetch failed: {e}")
            return None

    async def get_user_info(self, mailbox: str) -> Optional[dict]:
        """Get user profile info."""
        return await self._get(f"{GRAPH_BASE_URL}/users/{mailbox}")

    async def list_messages(
        self,
        mailbox: str,
        filter_expr: str = None,
        search: str = None,
        top: int = 100,
        skip: int = 0,
        order_by: str = "receivedDateTime desc",
        select: str = None,
    ) -> Optional[dict]:
        """List messages from a mailbox with optional filter/search."""
        params = {"$top": top}
        if select:
            params["$select"] = select
        if filter_expr:
            params["$filter"] = filter_expr
        if search:
            params["$search"] = f'"{search}"'
        else:
            # $orderby not compatible with $search
            if order_by:
                params["$orderby"] = order_by
        if skip > 0:
            params["$skip"] = skip

        return await self._get(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages",
            params=params,
        )

    async def get_message_mime(self, mailbox: str, message_id: str) -> Optional[bytes]:
        """Get message in MIME (.eml) format."""
        return await self._get_raw(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/$value"
        )

    async def close(self):
        await self.client.aclose()


# ============================================================
# Email MIME parsing
# ============================================================

def parse_mime_bytes(mime_data: bytes) -> Optional[Dict]:
    """Parse MIME bytes into structured email data."""
    try:
        msg = email_lib.message_from_bytes(mime_data, policy=policy.default)

        headers = {
            "message_id": str(msg.get("Message-ID", "")).strip(),
            "from": str(msg.get("From", "")),
            "to": str(msg.get("To", "")),
            "cc": str(msg.get("Cc", "")),
            "subject": str(msg.get("Subject", "")),
            "date": str(msg.get("Date", "")),
            "in_reply_to": str(msg.get("In-Reply-To", "")).strip(),
            "references": str(msg.get("References", "")).strip(),
        }

        body = ""
        has_attachments = False
        if msg.is_multipart():
            for part in msg.walk():
                disp = part.get("Content-Disposition", "")
                if "attachment" in disp.lower() or (part.get_filename()):
                    has_attachments = True
                    continue
                if part.get_content_type() == "text/plain":
                    try:
                        body += part.get_content()
                    except Exception:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body += payload.decode("utf-8", errors="ignore")
        else:
            try:
                body = msg.get_content()
            except Exception:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="ignore")

        headers["has_attachments"] = has_attachments

        full_text = (
            f"Subject: {headers['subject']}\n"
            f"From: {headers['from']}\n"
            f"To: {headers['to']}\n"
            f"Date: {headers['date']}\n\n"
            f"{body}"
        )

        return {"headers": headers, "body": body, "full_text": full_text}
    except Exception as e:
        log.error(f"MIME parse failed: {e}")
        return None


def determine_email_direction(from_addr: str, mailbox: str) -> str:
    """Determine if email is inbound or outbound relative to our mailbox."""
    if mailbox.lower() in from_addr.lower():
        return "outbound"
    return "inbound"


def address_matches_domains(address: str, domains: List[str]) -> bool:
    """Check if an email address belongs to one of the target domains."""
    addr_lower = address.lower()
    for domain in domains:
        if f"@{domain.lower()}" in addr_lower:
            return True
    return False


def email_involves_domains(msg_data: dict, domains: List[str]) -> bool:
    """Check if any sender or recipient is from a target domain."""
    # Check from
    from_addr = msg_data.get("from", {}).get("emailAddress", {}).get("address", "")
    if address_matches_domains(from_addr, domains):
        return True

    # Check to recipients
    for recip in msg_data.get("toRecipients", []):
        addr = recip.get("emailAddress", {}).get("address", "")
        if address_matches_domains(addr, domains):
            return True

    # Check cc recipients
    for recip in msg_data.get("ccRecipients", []):
        addr = recip.get("emailAddress", {}).get("address", "")
        if address_matches_domains(addr, domains):
            return True

    return False


# ============================================================
# Helper: vec literal for halfvec
# ============================================================

def _vec_literal(embedding: List[float]) -> str:
    """Convert embedding list to PostgreSQL halfvec literal."""
    return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"


def _parse_email_date(date_str: str) -> Optional[datetime]:
    """Parse email date string to datetime."""
    if not date_str:
        return None
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(date_str)
    except Exception:
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None


# ============================================================
# Database writer
# ============================================================

async def write_email_document(pool: asyncpg.Pool, doc: IngestedDocument, embeddings: List[List[float]]):
    """Write email document + chunks + email metadata to PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            # 1. core.documents
            await conn.execute("""
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (id) DO NOTHING
            """,
                uuid.UUID(doc.doc_id), doc.domain, doc.source_path,
                doc.filename, doc.document_type, doc.title,
                doc.content_hash, len(doc.chunks),
                doc.full_content[:500000],
                json.dumps(doc.metadata),
            )

            # 2. core.document_chunks
            for chunk, emb in zip(doc.chunks, embeddings):
                chunk_id = hashlib.md5(
                    f"{doc.source_path}:{chunk.chunk_index}".encode()
                ).hexdigest()
                await conn.execute("""
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                    ON CONFLICT (id) DO NOTHING
                """,
                    chunk_id, uuid.UUID(doc.doc_id),
                    chunk.chunk_index, chunk.total_chunks,
                    chunk.content, _vec_literal(emb),
                    json.dumps(chunk.metadata),
                )

            # 3. Link to legal cases
            for case_num in doc.case_numbers:
                row = await conn.fetchrow(
                    "SELECT id FROM legal.cases WHERE case_number = $1",
                    case_num,
                )
                if row:
                    await conn.execute("""
                        INSERT INTO legal.case_documents (case_id, document_id)
                        VALUES ($1, $2) ON CONFLICT DO NOTHING
                    """, row["id"], uuid.UUID(doc.doc_id))

            # 4. Email metadata
            if doc.email_meta:
                em = doc.email_meta
                await conn.execute("""
                    INSERT INTO legal.email_metadata
                        (document_id, message_id, in_reply_to, thread_id,
                         sender, recipients, cc, date_sent, direction,
                         mailbox, has_attachments, subject)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (document_id) DO NOTHING
                """,
                    uuid.UUID(doc.doc_id),
                    em.get("message_id"), em.get("in_reply_to"),
                    em.get("thread_id"),
                    em.get("sender"), em.get("recipients"), em.get("cc"),
                    _parse_email_date(em.get("date_sent", "")),
                    em.get("direction"), em.get("mailbox"),
                    em.get("has_attachments", False),
                    em.get("subject"),
                )


# ============================================================
# Main sync logic
# ============================================================

async def get_existing_message_ids(pool: asyncpg.Pool) -> Set[str]:
    """Get all message_ids already in the DB for deduplication."""
    rows = await pool.fetch(
        "SELECT message_id FROM legal.email_metadata WHERE message_id IS NOT NULL"
    )
    return {r["message_id"] for r in rows}


async def fetch_domain_emails(
    graph: GraphClient,
    mailbox: str,
    domains: List[str],
    since: Optional[str] = None,
    existing_msg_ids: Set[str] = None,
) -> List[Tuple[dict, bytes]]:
    """
    Fetch all emails involving target domains from a mailbox.

    Uses Graph API $filter with OR conditions on sender/recipient domains.
    Falls back to iterating through all messages if filter fails.
    """
    existing_msg_ids = existing_msg_ids or set()
    results: List[Tuple[dict, bytes]] = []  # (metadata, mime_bytes)
    seen_ids: Set[str] = set()

    # Build search queries for each domain
    # Graph API $search is the most reliable way to find emails by domain
    for domain in domains:
        log.info(f"  📧 Searching {mailbox} for *@{domain}...")

        page_count = 0
        fetched_this_domain = 0
        skipped_existing = 0
        next_link = None

        while True:
            if next_link:
                # Follow @odata.nextLink for pagination
                data = await graph._get(next_link)
            else:
                # Use $search to find emails involving this domain
                # Search looks at from, to, cc, subject, and body
                data = await graph.list_messages(
                    mailbox=mailbox,
                    search=domain,
                    top=250,
                    select="id,subject,from,toRecipients,ccRecipients,receivedDateTime,bodyPreview,hasAttachments,internetMessageId",
                )

            if not data:
                break

            messages = data.get("value", [])
            if not messages:
                break

            for msg in messages:
                graph_id = msg.get("id", "")
                inet_msg_id = msg.get("internetMessageId", "")

                # Skip if we've seen this in another domain search
                if graph_id in seen_ids:
                    continue
                seen_ids.add(graph_id)

                # Skip if already in DB
                if inet_msg_id and inet_msg_id in existing_msg_ids:
                    skipped_existing += 1
                    continue

                # Verify this email actually involves a target domain
                if not email_involves_domains(msg, domains):
                    continue

                # Apply date filter if specified
                if since:
                    recv_date = msg.get("receivedDateTime", "")[:10]
                    if recv_date and recv_date < since:
                        continue

                # Download MIME content
                mime = await graph.get_message_mime(mailbox, graph_id)
                if mime:
                    results.append((msg, mime))
                    fetched_this_domain += 1

                # Brief throttle to avoid hitting rate limits
                if fetched_this_domain % 50 == 0 and fetched_this_domain > 0:
                    await asyncio.sleep(1)

            # Check for next page
            next_link = data.get("@odata.nextLink")
            page_count += 1

            if not next_link:
                break

            log.info(f"    Page {page_count}: {len(messages)} messages, {fetched_this_domain} new so far...")

        log.info(
            f"    ✓ {domain}: {fetched_this_domain} new emails fetched"
            + (f", {skipped_existing} already in DB" if skipped_existing else "")
        )

    return results


async def process_and_ingest_email(
    pool: asyncpg.Pool,
    embedder: EmbeddingClient,
    chunker: TextChunker,
    msg_meta: dict,
    mime_data: bytes,
    mailbox: str,
    seen_message_ids: Set[str],
) -> Optional[IngestedDocument]:
    """Parse a single email, chunk, embed, and write to PostgreSQL."""
    parsed = parse_mime_bytes(mime_data)
    if not parsed or not parsed["full_text"].strip():
        return None

    h = parsed["headers"]
    msg_id = h["message_id"]

    # Dedup by Message-ID
    if msg_id and msg_id in seen_message_ids:
        return None
    if msg_id:
        seen_message_ids.add(msg_id)

    direction = determine_email_direction(h["from"], mailbox)
    case_nums = extract_case_numbers_from_text(parsed["full_text"][:5000])
    content_hash = hashlib.sha256(parsed["full_text"].encode()).hexdigest()[:16]

    # Create a stable source_path based on mailbox + message_id
    source_path = f"graph://{mailbox}/{msg_id or msg_meta.get('id', '')}"

    # Filename from subject + date
    subject = h["subject"] or "No Subject"
    date_str = msg_meta.get("receivedDateTime", "")[:10]
    safe_subject = re.sub(r'[^\w\s-]', '', subject)[:60].strip()
    filename = f"{date_str} - {safe_subject}.eml"

    chunks_text = chunker.split(parsed["full_text"])

    doc = IngestedDocument(
        doc_id=str(uuid.uuid4()),
        domain="legal",
        source_path=source_path,
        filename=filename,
        document_type="email",
        title=subject,
        content_hash=content_hash,
        full_content=parsed["full_text"],
        metadata={
            "file_type": "eml",
            "mailbox": mailbox,
            "source": "graph_api",
            "graph_message_id": msg_meta.get("id", ""),
            "received_date": date_str,
        },
        case_numbers=case_nums,
        email_meta={
            "subject": subject,
            "message_id": msg_id,
            "in_reply_to": h["in_reply_to"],
            "thread_id": h.get("references", "").split()[0] if h.get("references") else msg_id,
            "sender": h["from"],
            "recipients": h["to"],
            "cc": h["cc"],
            "date_sent": h["date"],
            "direction": direction,
            "mailbox": mailbox,
            "has_attachments": h.get("has_attachments", False),
        },
    )

    for i, chunk_text in enumerate(chunks_text):
        doc.chunks.append(DocumentChunk(
            content=chunk_text,
            metadata={"case_numbers": case_nums},
            chunk_index=i,
            total_chunks=len(chunks_text),
        ))

    # Embed
    chunk_texts = [c.content for c in doc.chunks]
    if not chunk_texts:
        return None

    try:
        embeddings = await embedder.embed_batch(chunk_texts)
    except Exception as e:
        log.error(f"  Embedding failed for {filename}: {e}")
        embeddings = [[0.0] * EMBEDDING_DIMENSIONS] * len(chunk_texts)

    # Write to DB
    try:
        await write_email_document(pool, doc, embeddings)
    except Exception as e:
        log.error(f"  DB write error for {filename}: {e}")
        return None

    return doc


# ============================================================
# Main entry point
# ============================================================

async def main(
    mailboxes: List[str] = None,
    domains: List[str] = None,
    since: Optional[str] = None,
    dry_run: bool = False,
):
    mailboxes = mailboxes or MAILBOXES
    domains = domains or TARGET_DOMAINS

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  WDWS Graph API Email Sync                              ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  Mailboxes:  {', '.join(mailboxes):<43}║")
    log.info(f"║  Domains:    {', '.join(domains):<43}║")
    log.info(f"║  Since:      {since or 'all time':<43}║")
    log.info(f"║  Dry run:    {str(dry_run):<43}║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("")

    # Connect to database
    log.info("Connecting to PostgreSQL...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # Read sync config from database (if management tables exist)
    try:
        db_mbs = await pool.fetch(
            "SELECT email FROM ops.sync_mailboxes WHERE is_active = true ORDER BY id"
        )
        if db_mbs:
            mailboxes = [r["email"] for r in db_mbs]
            log.info(f"  \U0001f4cb Loaded {len(mailboxes)} mailboxes from DB config")
        db_rules = await pool.fetch(
            "SELECT pattern FROM ops.sync_rules WHERE is_active = true "
            "AND rule_type = 'domain' ORDER BY priority DESC"
        )
        if db_rules:
            domains = [r["pattern"] for r in db_rules]
            log.info(f"  \U0001f4cb Loaded {len(domains)} domain rules from DB config")
    except Exception:
        log.info("  Using default mailboxes and domains (DB config tables not found)")

    # Get existing message IDs for dedup
    existing_msg_ids = await get_existing_message_ids(pool)
    log.info(f"Found {len(existing_msg_ids)} existing emails in DB (will skip)")

    # Get current counts
    doc_count = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    chunk_count = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")
    log.info(f"DB has {doc_count} documents, {chunk_count} chunks")

    # Authenticate with Graph API
    graph = GraphClient()
    if not await graph.authenticate():
        log.error("Cannot continue without Graph API authentication")
        await pool.close()
        return

    # Test access to each mailbox
    for mb in mailboxes:
        user = await graph.get_user_info(mb)
        if user:
            log.info(f"✓ Access confirmed: {user.get('displayName', mb)} ({mb})")
        else:
            log.error(f"✗ Cannot access mailbox: {mb}")
            await graph.close()
            await pool.close()
            return

    # Fetch emails from all mailboxes
    all_emails: List[Tuple[dict, bytes, str]] = []  # (metadata, mime, mailbox)

    for mb in mailboxes:
        log.info("")
        log.info(f"{'='*60}")
        log.info(f"  Fetching from {mb}")
        log.info(f"{'='*60}")

        emails = await fetch_domain_emails(
            graph, mb, domains,
            since=since,
            existing_msg_ids=existing_msg_ids,
        )

        for meta, mime in emails:
            all_emails.append((meta, mime, mb))

        log.info(f"  Total from {mb}: {len(emails)} new emails")

    log.info("")
    log.info(f"Total emails to ingest: {len(all_emails)}")

    if dry_run:
        log.info("DRY RUN — not writing to database")
        # Show sample
        for meta, mime, mb in all_emails[:20]:
            subj = meta.get("subject", "?")[:60]
            date = meta.get("receivedDateTime", "")[:10]
            from_addr = meta.get("from", {}).get("emailAddress", {}).get("address", "?")
            log.info(f"  {date} | {from_addr:<35} | {subj}")
        if len(all_emails) > 20:
            log.info(f"  ... and {len(all_emails) - 20} more")
        await graph.close()
        await pool.close()
        return

    if not all_emails:
        log.info("No new emails to ingest")
        # Record the sync even when empty for auditability
        try:
            await pool.execute("""
                INSERT INTO ops.sync_runs
                    (trigger_type, mailboxes_scanned, emails_found,
                     completed_at, status, emails_ingested, emails_skipped,
                     emails_failed, duration_seconds)
                VALUES ('timer', $1, 0, now(), 'completed', 0, 0, 0, 0)
            """, mailboxes)
        except Exception as e:
            log.error(f"AUDIT FAILURE - sync_runs INSERT (empty run): {e}")
        # Always update mailbox last-checked timestamps
        try:
            for mb in mailboxes:
                await pool.execute(
                    "UPDATE ops.sync_mailboxes SET last_sync_at = now() WHERE email = $1",
                    mb)
        except Exception as e:
            log.error(f"AUDIT FAILURE - mailbox timestamp update: {e}")
        await graph.close()
        await pool.close()
        return

    # Record sync run in database
    sync_run_id = None
    try:
        sync_run_id = await pool.fetchval("""
            INSERT INTO ops.sync_runs
                (trigger_type, mailboxes_scanned, emails_found)
            VALUES ('api', $1, $2) RETURNING id
        """, mailboxes, len(all_emails))
    except Exception as e:
        log.error(f"AUDIT FAILURE: {e}")

    # Ingest emails
    log.info("")
    log.info("────────────────────────────────────────────────────────────")
    log.info(f"  Ingesting {len(all_emails)} emails → PostgreSQL")
    log.info("────────────────────────────────────────────────────────────")

    embedder = EmbeddingClient()
    chunker = TextChunker()
    seen_message_ids: Set[str] = set(existing_msg_ids)

    stats = {"ok": 0, "skipped": 0, "errors": 0, "chunks": 0}
    start_time = time.time()

    for i, (meta, mime, mb) in enumerate(all_emails, 1):
        subj = meta.get("subject", "?")[:50]
        date = meta.get("receivedDateTime", "")[:10]

        try:
            doc = await process_and_ingest_email(
                pool, embedder, chunker, meta, mime, mb, seen_message_ids,
            )
            if doc:
                stats["ok"] += 1
                stats["chunks"] += len(doc.chunks)
                icon = "✓"
                detail = f"→ {len(doc.chunks)} chunks"
            else:
                stats["skipped"] += 1
                icon = "⊘"
                detail = "skipped"
        except Exception as e:
            stats["errors"] += 1
            icon = "✗"
            detail = str(e)[:60]
            log.error(f"  Error processing email: {e}")

        elapsed = time.time() - start_time
        rate = i / elapsed if elapsed > 0 else 0
        eta = (len(all_emails) - i) / rate if rate > 0 else 0
        eta_str = f"{eta:.0f}s" if eta < 60 else f"{eta/60:.1f}m"

        log.info(
            f"  [{i}/{len(all_emails)} {i*100//len(all_emails)}%] "
            f"{icon} {date} {subj} {detail}  [{rate:.1f}/s, ETA {eta_str}]"
        )

        if i % 20 == 0:
            log.info(
                f"  ── Progress: {stats['ok']} ok / {stats['skipped']} skip / "
                f"{stats['errors']} err / {stats['chunks']} chunks"
            )

    # Final summary
    elapsed = time.time() - start_time
    final_docs = await pool.fetchval("SELECT COUNT(*) FROM core.documents")
    final_chunks = await pool.fetchval("SELECT COUNT(*) FROM core.document_chunks")
    email_count = await pool.fetchval(
        "SELECT COUNT(*) FROM legal.email_metadata WHERE mailbox = ANY($1::text[])",
        mailboxes,
    )

    log.info("")
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  EMAIL SYNC COMPLETE                                     ║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  New emails:      {stats['ok']:<38}║")
    log.info(f"║  Skipped:         {stats['skipped']:<38}║")
    log.info(f"║  Errors:          {stats['errors']:<38}║")
    log.info(f"║  New chunks:      {stats['chunks']:<38}║")
    log.info(f"║  Time:            {elapsed:.1f}s{' '*(36-len(f'{elapsed:.1f}s'))}║")
    log.info(f"║  Embed tokens:    {embedder.total_tokens:<38}║")
    log.info(f"║  Est. cost:       ${embedder.total_tokens * 0.00013 / 1000:.4f}{' '*(35-len(f'${embedder.total_tokens * 0.00013 / 1000:.4f}'))}║")
    log.info("╠══════════════════════════════════════════════════════════╣")
    log.info(f"║  DB total docs:   {final_docs:<38}║")
    log.info(f"║  DB total chunks: {final_chunks:<38}║")
    log.info(f"║  Mailbox emails:  {email_count:<38}║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    # Finalize sync run record
    if sync_run_id:
        try:
            await pool.execute("""
                UPDATE ops.sync_runs SET
                    completed_at = now(), status = 'completed',
                    emails_ingested = $2, emails_skipped = $3,
                    emails_failed = $4, chunks_created = $5,
                    embedding_tokens = $6, duration_seconds = $7
                WHERE id = $1
            """, sync_run_id, stats["ok"], stats["skipped"],
                stats["errors"], stats["chunks"],
                embedder.total_tokens, elapsed)
        except Exception as e:
            log.error(f"AUDIT FAILURE - sync_runs UPDATE: {e}")

    # Update mailbox sync timestamps
    try:
        for mb in mailboxes:
            mb_count = sum(1 for _, _, m in all_emails if m == mb)
            if mb_count > 0:
                await pool.execute("""
                    UPDATE ops.sync_mailboxes SET
                        last_sync_at = now(),
                        total_synced = total_synced + $2
                    WHERE email = $1
                """, mb, mb_count)
            else:
                await pool.execute(
                    "UPDATE ops.sync_mailboxes SET last_sync_at = now() WHERE email = $1",
                    mb)
    except Exception as e:
        log.error(f"AUDIT FAILURE: {e}")

    await embedder.close()
    await graph.close()
    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WDWS Graph API Email Sync")
    parser.add_argument(
        "--mailbox", "-m", action="append",
        help="Mailbox to scan (can specify multiple). Default: both william@ and athena@"
    )
    parser.add_argument(
        "--domains", "-d", nargs="+",
        default=TARGET_DOMAINS,
        help="Target domains to filter for"
    )
    parser.add_argument(
        "--since", "-s",
        help="Only fetch emails after this date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be fetched without writing to DB"
    )

    args = parser.parse_args()

    asyncio.run(main(
        mailboxes=args.mailbox or MAILBOXES,
        domains=args.domains,
        since=args.since,
        dry_run=args.dry_run,
    ))
