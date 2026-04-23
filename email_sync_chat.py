#!/usr/bin/env python3
"""
O365 Email Sync for Athena Chat Database (athena_chat).

Hybrid Architecture (Option C):
  - Server syncs emails via Graph API app-only (client credentials)
  - Stores in athena_chat.chat.email_messages with pgvector embeddings
  - App reads emails via delegated auth for real-time UX
  - App calls /api/email/enrich for AI metadata overlay

Usage:
    python email_sync_chat.py                             # Sync all active accounts
    python email_sync_chat.py --account william@seattleseahawks.me
    python email_sync_chat.py --since 2025-01-01
    python email_sync_chat.py --classify                  # Also run AI classification
    python email_sync_chat.py --dry-run
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
from typing import List, Dict, Optional, Set, Tuple

import httpx
import asyncpg

# ── Load .env ─────────────────────────────────────────────────
_env_file = Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Config ────────────────────────────────────────────────────
# Chat database (NOT the MCP/wdws database)
CHAT_DATABASE_URL = os.getenv("CHAT_DATABASE_URL", "")
if not CHAT_DATABASE_URL:
    raise RuntimeError("Missing CHAT_DATABASE_URL — check /opt/wdws/.env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
if not OPENAI_API_KEY:
    raise RuntimeError("Missing OPENAI_API_KEY — check /opt/wdws/.env")

# Graph API credentials (app-only / client credentials)
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
if not all([GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_TENANT_ID]):
    raise RuntimeError("Missing GRAPH_CLIENT_ID/SECRET/TENANT_ID — check /opt/wdws/.env")

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

import sys
sys.path.insert(0, "/opt/wdws")
from embedding_service import EMBEDDING_MODEL, EMBEDDING_DIMENSIONS
from contextual_retrieval import generate_context_sync, enrich_chunks
from email_chunker import (
    EmailAwareChunker,
    ChunkResult,
    PartType,
    TextChunker,  # back-compat alias → EmailAwareChunker
)
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBEDDING_BATCH_SIZE = 20

# Classification model
CLASSIFY_MODEL = os.getenv("CLASSIFY_MODEL", "gpt-5.4")

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("email_sync_chat")


# ══════════════════════════════════════════════════════════════
# Graph API Client
# ══════════════════════════════════════════════════════════════

class GraphClient:
    """Microsoft Graph API client using app-only (client credentials) auth."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        self.access_token: Optional[str] = None
        self.token_expires_at: float = 0

    async def authenticate(self) -> bool:
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
            log.info("✓ Graph API authenticated (app-only)")
            return True
        except Exception as e:
            log.error("✗ Graph API auth failed: %s", e)
            return False

    async def _ensure_token(self):
        if time.time() >= self.token_expires_at:
            await self.authenticate()

    async def _get(self, url: str, params: dict = None) -> Optional[dict]:
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
                retry_after = int(e.response.headers.get("Retry-After", "10"))
                log.warning("Rate limited, waiting %ds...", retry_after)
                await asyncio.sleep(retry_after)
                return await self._get(url, params)
            log.error("Graph API %d: %s", e.response.status_code, e.response.text[:200])
            return None
        except Exception as e:
            log.error("Graph API request failed: %s", e)
            return None

    async def _post(self, url: str, json_data: dict) -> Optional[dict]:
        await self._ensure_token()
        try:
            resp = await self.client.post(
                url,
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "Content-Type": "application/json",
                },
                json=json_data,
            )
            resp.raise_for_status()
            return resp.json() if resp.content else {}
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", "10"))
                log.warning("Rate limited, waiting %ds...", retry_after)
                await asyncio.sleep(retry_after)
                return await self._post(url, json_data)
            log.error("Graph POST %d: %s", e.response.status_code, e.response.text[:200])
            return None
        except Exception as e:
            log.error("Graph POST failed: %s", e)
            return None

    async def get_user(self, upn: str) -> Optional[dict]:
        return await self._get(f"{GRAPH_BASE_URL}/users/{upn}")

    async def list_messages(
        self,
        mailbox: str,
        folder: str = "inbox",
        *,
        top: int = 100,
        skip: int = 0,
        select: str = None,
        filter_expr: str = None,
        search: str = None,
        order_by: str = "receivedDateTime desc",
        next_link: str = None,
    ) -> Optional[dict]:
        """List messages from a mailbox folder."""
        if next_link:
            return await self._get(next_link)

        # Map folder names to well-known names
        folder_map = {
            "inbox": "inbox",
            "sentitems": "sentitems",
            "sent": "sentitems",
            "drafts": "drafts",
            "deleteditems": "deleteditems",
            "archive": "archive",
        }
        folder_path = folder_map.get(folder.lower(), folder)

        params = {"$top": top}
        if select:
            params["$select"] = select
        if filter_expr:
            params["$filter"] = filter_expr
        if search:
            params["$search"] = f'"{search}"'
        elif order_by:
            params["$orderby"] = order_by
        if skip > 0:
            params["$skip"] = skip

        return await self._get(
            f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders/{folder_path}/messages",
            params=params,
        )

    async def get_message(self, mailbox: str, message_id: str) -> Optional[dict]:
        """Get full message by ID."""
        return await self._get(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}",
            params={"$select": "id,subject,from,toRecipients,ccRecipients,bccRecipients,"
                    "replyTo,receivedDateTime,sentDateTime,createdDateTime,"
                    "lastModifiedDateTime,body,bodyPreview,internetMessageId,"
                    "conversationId,isRead,isDraft,importance,flag,hasAttachments,"
                    "internetMessageHeaders,categories,webLink,parentFolderId,"
                    "inferenceClassification"},
        )

    async def list_attachments(self, mailbox: str, message_id: str) -> Optional[dict]:
        """List attachments for a message."""
        return await self._get(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/attachments",
        )

    async def send_mail(self, mailbox: str, message: dict) -> Optional[dict]:
        """Send an email."""
        return await self._post(
            f"{GRAPH_BASE_URL}/users/{mailbox}/sendMail",
            {"message": message, "saveToSentItems": True},
        )

    async def reply_to_message(self, mailbox: str, message_id: str, comment: str) -> Optional[dict]:
        """Reply to a message."""
        return await self._post(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/reply",
            {"comment": comment},
        )

    async def create_subscription(
        self, resource: str, change_type: str, notification_url: str, expiration: str
    ) -> Optional[dict]:
        """Create a Graph webhook subscription for change notifications."""
        return await self._post(
            f"{GRAPH_BASE_URL}/subscriptions",
            {
                "changeType": change_type,
                "notificationUrl": notification_url,
                "resource": resource,
                "expirationDateTime": expiration,
                "clientState": hashlib.sha256(GRAPH_CLIENT_SECRET.encode()).hexdigest()[:32],
            },
        )

    async def close(self):
        await self.client.aclose()


# ══════════════════════════════════════════════════════════════
# Embedding Client
# ══════════════════════════════════════════════════════════════

class EmbeddingClient:
    """Local BGE-M3 embedding client (replaces OpenAI API calls)."""
    def __init__(self):
        self.total_tokens = 0

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        from embedding_service import embed_texts_sync
        return embed_texts_sync(texts)

    async def embed_single(self, text: str) -> List[float]:
        from embedding_service import embed_query_sync
        return embed_query_sync(text)

    async def close(self):
        pass


# ══════════════════════════════════════════════════════════════
# Text Chunker — imported from email_chunker module.
# TextChunker is a back-compat alias for EmailAwareChunker.
# Email body ingest should call .split_email_body() for structure-aware
# chunking; .split() remains available for generic (non-email) text.
# ══════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════

def _vec_literal(embedding: List[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in embedding) + "]"


def _extract_recipients(recipients: list) -> list:
    """Convert Graph recipient objects to [{email, name}]."""
    return [
        {
            "email": r.get("emailAddress", {}).get("address", ""),
            "name": r.get("emailAddress", {}).get("name", ""),
        }
        for r in (recipients or [])
    ]


def _determine_direction(sender_email: str, mailbox: str) -> str:
    if mailbox.lower() in (sender_email or "").lower():
        return "outbound"
    return "inbound"


def _parse_graph_datetime(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


# Case number extraction
STATE_CASE_RE = re.compile(r'(\d{2})-(\d)-(\d{5})-(\d{2})')
FEDERAL_CASE_RE = re.compile(
    r'(\d{1,2})[-:]?(\d{2})[-:]?cv[-:]?(\d{5})[-:]?(\w{2,4})?', re.IGNORECASE
)


def extract_case_numbers(text: str) -> List[str]:
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


# ══════════════════════════════════════════════════════════════
# Core Sync Logic
# ══════════════════════════════════════════════════════════════

async def sync_account(
    pool: asyncpg.Pool,
    graph: GraphClient,
    embedder: EmbeddingClient,
    account: dict,
    *,
    since: Optional[str] = None,
    dry_run: bool = False,
    classify: bool = False,
) -> dict:
    """Sync a single email account from Graph API to the chat database."""
    account_id = account["id"]
    email_addr = account["email_address"]
    folders = account.get("sync_folders") or ["inbox", "sentitems"]
    domains = account.get("sync_domains")  # None means all
    max_msgs = account.get("max_messages", 10000)

    log.info("══════════════════════════════════════════════════")
    log.info("  Syncing: %s", email_addr)
    log.info("  Folders: %s", ", ".join(folders))
    if domains:
        log.info("  Domains: %s", ", ".join(domains))
    log.info("══════════════════════════════════════════════════")

    # Start sync run
    run_id = None
    if not dry_run:
        run_id = await pool.fetchval("""
            INSERT INTO chat.email_sync_runs (account_id, trigger_type, folder_name)
            VALUES ($1, 'manual', $2)
            RETURNING id
        """, account_id, ",".join(folders))

    stats = {
        "messages_fetched": 0, "messages_new": 0, "messages_updated": 0,
        "messages_skipped": 0, "messages_failed": 0,
        "chunks_created": 0, "embeddings_created": 0,
    }
    chunker = TextChunker()
    start_time = time.time()

    # Get existing graph_message_ids for dedup
    existing = set()
    rows = await pool.fetch(
        "SELECT graph_message_id FROM chat.email_messages WHERE account_id = $1",
        account_id,
    )
    existing = {r["graph_message_id"] for r in rows}
    log.info("  Existing messages in DB: %d", len(existing))

    # Build date filter
    since_dt = since or (account.get("sync_since") or "").isoformat() if account.get("sync_since") else None
    if not since_dt:
        since_dt = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00Z")

    # Sync each folder
    total_synced = 0
    for folder in folders:
        log.info("  📁 Folder: %s", folder)
        next_link = None
        page = 0

        while total_synced < max_msgs:
            data = await graph.list_messages(
                mailbox=email_addr,
                folder=folder,
                top=100,
                select="id,subject,from,toRecipients,ccRecipients,bccRecipients,replyTo,"
                       "receivedDateTime,sentDateTime,createdDateTime,lastModifiedDateTime,"
                       "body,bodyPreview,internetMessageId,conversationId,"
                       "isRead,isDraft,importance,flag,hasAttachments,categories,webLink",
                filter_expr=f"receivedDateTime ge {since_dt}" if not next_link else None,
                next_link=next_link,
            )
            if not data:
                break

            messages = data.get("value", [])
            if not messages:
                break

            page += 1
            log.info("    Page %d: %d messages", page, len(messages))

            # Process messages
            pending_chunks: List[Tuple[str, str, int, str, Optional[str]]] = []  # (msg_uuid, text, idx, source, att_id)
            pending_embeddings: List[str] = []  # texts to embed
            pending_chunk_ids: List[Tuple[str, int]] = []  # (msg_uuid, chunk_idx) parallel to pending_embeddings

            for msg in messages:
                graph_msg_id = msg.get("id", "")
                stats["messages_fetched"] += 1

                # Skip already synced
                if graph_msg_id in existing:
                    stats["messages_skipped"] += 1
                    continue

                # Extract data
                sender_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "")
                sender_name = msg.get("from", {}).get("emailAddress", {}).get("name", "")
                to_recips = _extract_recipients(msg.get("toRecipients", []))
                cc_recips = _extract_recipients(msg.get("ccRecipients", []))
                bcc_recips = _extract_recipients(msg.get("bccRecipients", []))
                reply_to = _extract_recipients(msg.get("replyTo", []))

                # Domain filter
                if domains:
                    all_addrs = [sender_addr] + [r["email"] for r in to_recips + cc_recips]
                    if not any(
                        any(f"@{d.lower()}" in addr.lower() for d in domains)
                        for addr in all_addrs if addr
                    ):
                        stats["messages_skipped"] += 1
                        continue

                body_obj = msg.get("body", {})
                body_text = ""
                body_html = ""
                if body_obj.get("contentType") == "text":
                    body_text = body_obj.get("content", "")
                elif body_obj.get("contentType") == "html":
                    body_html = body_obj.get("content", "")
                    # Strip HTML for plain text
                    import html as _html
                    body_text = re.sub(r'<[^>]+>', ' ', body_html)
                    body_text = _html.unescape(body_text)
                    body_text = re.sub(r'\s+', ' ', body_text).strip()

                subject = msg.get("subject", "")
                content_hash = hashlib.sha256(
                    (subject + body_text[:5000]).encode()
                ).hexdigest()

                direction = _determine_direction(sender_addr, email_addr)
                importance = (msg.get("importance") or "normal").lower()
                if importance not in ("low", "normal", "high"):
                    importance = "normal"

                flag_status = msg.get("flag", {}).get("flagStatus", "notFlagged")
                is_flagged = flag_status == "flagged"

                msg_uuid = str(uuid.uuid4())

                if dry_run:
                    log.info("    [DRY] %s → %s: %s", sender_addr, direction, subject[:60])
                    stats["messages_new"] += 1
                    continue

                try:
                    await pool.execute("""
                        INSERT INTO chat.email_messages (
                            id, account_id, graph_message_id, graph_conversation_id,
                            internet_message_id, subject, sender_email, sender_name,
                            to_recipients, cc_recipients, bcc_recipients, reply_to,
                            direction, folder_name, body_preview, body_text, body_html,
                            content_hash, is_read, is_flagged, is_draft, importance,
                            has_attachments, categories, received_at, sent_at,
                            graph_created_at, graph_modified_at, web_link, status
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8,
                            $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb,
                            $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
                            $23, $24, $25, $26, $27, $28, $29, 'pending'
                        )
                    """,
                        uuid.UUID(msg_uuid), account_id, graph_msg_id,
                        msg.get("conversationId"),
                        msg.get("internetMessageId"),
                        subject, sender_addr, sender_name,
                        json.dumps(to_recips), json.dumps(cc_recips),
                        json.dumps(bcc_recips), json.dumps(reply_to),
                        direction, folder, msg.get("bodyPreview", ""),
                        body_text[:500000] if body_text else None,
                        body_html[:500000] if body_html else None,
                        content_hash, msg.get("isRead", False),
                        is_flagged, msg.get("isDraft", False), importance,
                        msg.get("hasAttachments", False),
                        msg.get("categories") or [],
                        _parse_graph_datetime(msg.get("receivedDateTime")),
                        _parse_graph_datetime(msg.get("sentDateTime")),
                        _parse_graph_datetime(msg.get("createdDateTime")),
                        _parse_graph_datetime(msg.get("lastModifiedDateTime")),
                        msg.get("webLink"),
                    )
                    stats["messages_new"] += 1
                    existing.add(graph_msg_id)

                    # Structure-aware chunking: split body into body / quoted /
                    # signature / disclaimer parts. Only 'body' is embedded;
                    # the rest are stored with chunk_source != 'body' and may
                    # be re-embedded later or used for FTS only.
                    full_text = f"Subject: {subject}\nFrom: {sender_name} <{sender_addr}>\n\n{body_text}"
                    chunk_results = chunker.split_email_body(full_text[:50000])

                    # Defensive fallback for extremely short / malformed bodies
                    if not chunk_results:
                        generic = chunker.split(full_text[:50000])
                        chunk_results = [
                            ChunkResult(text=t, source_part=PartType.BODY, is_embedded=True)
                            for t in generic
                        ]

                    body_texts_for_embed = [r.text for r in chunk_results if r.is_embedded]

                    if body_texts_for_embed:
                        cr_context = generate_context_sync(
                            title=subject,
                            domain="email",
                            document_type="email",
                            content_preview=full_text[:3000],
                            case_number=None,
                        )
                        enriched_body = enrich_chunks(cr_context, body_texts_for_embed)
                    else:
                        enriched_body = []

                    body_iter = iter(enriched_body)
                    for ci, r in enumerate(chunk_results):
                        if r.is_embedded:
                            enriched_text = next(body_iter)
                            pending_chunks.append(
                                (msg_uuid, r.text, ci, r.source_part.value, None)
                            )
                            pending_embeddings.append(enriched_text)
                            pending_chunk_ids.append((msg_uuid, ci))
                            stats["chunks_created"] += 1
                        else:
                            # Non-body chunks are skipped here — they'd either
                            # duplicate an already-indexed parent email (quoted)
                            # or be boilerplate (signature / disclaimer). If
                            # FTS-only storage of these is desired later, add
                            # an INSERT here with NULL embedding.
                            pass

                except Exception as e:
                    log.error("    Failed to insert message %s: %s", graph_msg_id[:20], e)
                    stats["messages_failed"] += 1

            # Batch embed
            if pending_embeddings and not dry_run:
                try:
                    for batch_start in range(0, len(pending_embeddings), EMBEDDING_BATCH_SIZE):
                        batch_texts = pending_embeddings[batch_start:batch_start + EMBEDDING_BATCH_SIZE]
                        batch_ids = pending_chunk_ids[batch_start:batch_start + EMBEDDING_BATCH_SIZE]
                        batch_chunks = pending_chunks[batch_start:batch_start + EMBEDDING_BATCH_SIZE]

                        embeddings = await embedder.embed_batch(batch_texts)

                        for (msg_uuid, chunk_text, ci, source, att_id), emb in zip(batch_chunks, embeddings):
                            await pool.execute("""
                                INSERT INTO chat.email_message_chunks
                                    (message_id, chunk_index, chunk_text, chunk_source,
                                     attachment_id, token_count, embedding)
                                VALUES ($1, $2, $3, $4, $5, $6, $7::halfvec)
                                ON CONFLICT (message_id, chunk_index) DO NOTHING
                            """,
                                uuid.UUID(msg_uuid), ci, chunk_text, source,
                                uuid.UUID(att_id) if att_id else None,
                                len(chunk_text.split()),
                                _vec_literal(emb),
                            )
                            stats["embeddings_created"] += 1

                        # Update status to indexed
                        msg_uuids_in_batch = list(set(mid for mid, _ in batch_ids))
                        for mid in msg_uuids_in_batch:
                            await pool.execute(
                                "UPDATE chat.email_messages SET status = 'indexed', indexed_at = now() WHERE id = $1",
                                uuid.UUID(mid),
                            )

                except Exception as e:
                    log.error("    Embedding batch failed: %s", e)

            total_synced += len(messages)

            # Next page
            next_link = data.get("@odata.nextLink")
            if not next_link:
                break

    # Finalize sync run
    duration = time.time() - start_time
    if run_id and not dry_run:
        await pool.execute("""
            UPDATE chat.email_sync_runs SET
                status = 'completed', completed_at = now(),
                messages_fetched = $2, messages_new = $3,
                messages_updated = $4, messages_skipped = $5,
                messages_failed = $6, chunks_created = $7,
                embeddings_created = $8, duration_seconds = $9
            WHERE id = $1
        """, run_id, stats["messages_fetched"], stats["messages_new"],
            stats["messages_updated"], stats["messages_skipped"],
            stats["messages_failed"], stats["chunks_created"],
            stats["embeddings_created"], duration,
        )

    # Update account stats
    if not dry_run:
        counts = await pool.fetchrow("""
            SELECT COUNT(*) AS total, 
                   COUNT(*) FILTER (WHERE status = 'indexed') AS indexed
            FROM chat.email_messages WHERE account_id = $1
        """, account_id)
        await pool.execute("""
            UPDATE chat.email_accounts SET
                last_sync_at = now(),
                last_sync_count = $2,
                total_messages = $3,
                total_indexed = $4
            WHERE id = $1
        """, account_id, stats["messages_new"],
            counts["total"], counts["indexed"],
        )

    log.info("  ✓ Done in %.1fs — %d new, %d skipped, %d failed, %d embeddings",
             duration, stats["messages_new"], stats["messages_skipped"],
             stats["messages_failed"], stats["embeddings_created"])

    # Classify if requested
    if classify and not dry_run and stats["messages_new"] > 0:
        await classify_unclassified(pool, limit=stats["messages_new"])

    return stats


# ══════════════════════════════════════════════════════════════
# AI Classification
# ══════════════════════════════════════════════════════════════

CLASSIFY_PROMPT = """You are an email classification agent for a pro-se legal litigant.

ACTIVE CASES:
- 19-2-03310-31: FTCU v Nelson
- 23-2-07759-31: Nelson v Prisma Wellness
- 23-2-09435-31: Nelson v Unum Group
- 24-2-01031-31: Nelson v Starbucks
- 24-2-01032-31: Nelson v Olivia Kuker
- 24-2-01033-31: Nelson v Stuart Gibson
- 24-2-01034-31: Nelson v Brian Baseggio
- 24-2-07413-31: Nelson v Trevor Haward et al
- 25-2-00238-31: Nelson vs FTCU et al
- 26-2-00762-31: IAFF v SSCFR Nelson
- 2:26-cv-00337-KKE: Federal case
- 3:25-cv-05551-DGE: Federal case

CLASSIFICATION RULES:
- URGENT: Court orders, hearing notices, filing deadlines within 7 days, judge communications
- HIGH: Opposing counsel motions/responses, discovery requests, settlement offers
- NORMAL: Routine correspondence, case updates, confirmations
- LOW: Marketing, newsletters, automated notifications

Output valid JSON only:
{
  "urgency": "urgent|high|normal|low",
  "category": "court-order|filing|discovery|correspondence|personal|automated|marketing",
  "case_number": "24-2-01031-31 or null",
  "case_confidence": 0.0-1.0,
  "deadlines": [{"date": "2026-02-15", "description": "Response due"}],
  "requires_action": true/false,
  "action_summary": "Brief description",
  "reasoning": "Why this classification"
}"""


async def classify_unclassified(pool: asyncpg.Pool, limit: int = 50) -> int:
    """Run AI classification on unclassified emails."""
    rows = await pool.fetch("""
        SELECT em.id, em.subject, em.sender_email, em.sender_name,
               em.received_at, em.direction, em.body_preview,
               LEFT(em.body_text, 3000) AS body_excerpt
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE aic.id IS NULL AND em.status = 'indexed'
        ORDER BY em.received_at DESC
        LIMIT $1
    """, limit)

    if not rows:
        log.info("  No unclassified emails to process")
        return 0

    log.info("  🤖 Classifying %d emails...", len(rows))
    classified = 0

    async with httpx.AsyncClient(timeout=60.0) as client:
        for row in rows:
            email_text = (
                f"Subject: {row['subject']}\n"
                f"From: {row['sender_name']} <{row['sender_email']}>\n"
                f"Date: {row['received_at']}\n"
                f"Direction: {row['direction']}\n\n"
                f"{row['body_excerpt'] or row['body_preview'] or '(no content)'}"
            )

            try:
                resp = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                    json={
                        "model": CLASSIFY_MODEL,
                        "messages": [
                            {"role": "system", "content": CLASSIFY_PROMPT},
                            {"role": "user", "content": email_text},
                        ],
                        "temperature": 0.1,
                        "response_format": {"type": "json_object"},
                    },
                )
                resp.raise_for_status()
                result = json.loads(resp.json()["choices"][0]["message"]["content"])

                await pool.execute("""
                    INSERT INTO chat.email_ai_classifications
                        (message_id, urgency, category, case_number, case_confidence,
                         deadlines, requires_action, action_summary, reasoning, model_used)
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10)
                    ON CONFLICT (message_id) DO UPDATE SET
                        urgency = EXCLUDED.urgency,
                        category = EXCLUDED.category,
                        case_number = EXCLUDED.case_number,
                        case_confidence = EXCLUDED.case_confidence,
                        deadlines = EXCLUDED.deadlines,
                        requires_action = EXCLUDED.requires_action,
                        action_summary = EXCLUDED.action_summary,
                        reasoning = EXCLUDED.reasoning,
                        model_used = EXCLUDED.model_used,
                        updated_at = now()
                """,
                    row["id"],
                    result.get("urgency", "normal"),
                    result.get("category"),
                    result.get("case_number"),
                    result.get("case_confidence"),
                    json.dumps(result.get("deadlines", [])),
                    result.get("requires_action", False),
                    result.get("action_summary"),
                    result.get("reasoning"),
                    CLASSIFY_MODEL,
                )
                classified += 1

            except Exception as e:
                log.error("    Classification failed for %s: %s", row["id"], e)

    log.info("  ✓ Classified %d/%d emails", classified, len(rows))
    return classified


# ══════════════════════════════════════════════════════════════
# Priority Indexing (called by app when user views an email)
# ══════════════════════════════════════════════════════════════

async def priority_index_message(
    pool: asyncpg.Pool,
    graph: GraphClient,
    embedder: EmbeddingClient,
    *,
    mailbox: str,
    graph_message_id: str,
) -> Optional[str]:
    """Fast-track indexing of a single message the user just viewed.
    
    Returns the message UUID if indexed, None if already exists or failed.
    """
    # Check if already indexed
    existing = await pool.fetchval(
        "SELECT id FROM chat.email_messages WHERE graph_message_id = $1",
        graph_message_id,
    )
    if existing:
        return str(existing)

    # Fetch from Graph
    msg = await graph.get_message(mailbox, graph_message_id)
    if not msg:
        return None

    # Find account
    account_id = await pool.fetchval(
        "SELECT id FROM chat.email_accounts WHERE email_address = $1",
        mailbox,
    )
    if not account_id:
        log.warning("No account registered for mailbox: %s", mailbox)
        return None

    # Extract and store (simplified single-message version)
    sender_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "")
    sender_name = msg.get("from", {}).get("emailAddress", {}).get("name", "")
    subject = msg.get("subject", "")
    body_obj = msg.get("body", {})
    body_text = ""
    if body_obj.get("contentType") == "text":
        body_text = body_obj.get("content", "")
    elif body_obj.get("contentType") == "html":
        import html as _html
        body_text = re.sub(r'<[^>]+>', ' ', body_obj.get("content", ""))
        body_text = _html.unescape(body_text)
        body_text = re.sub(r'\s+', ' ', body_text).strip()

    msg_uuid = str(uuid.uuid4())
    direction = _determine_direction(sender_addr, mailbox)

    try:
        await pool.execute("""
            INSERT INTO chat.email_messages (
                id, account_id, graph_message_id, graph_conversation_id,
                internet_message_id, subject, sender_email, sender_name,
                to_recipients, cc_recipients, direction, folder_name,
                body_preview, body_text, content_hash, is_read,
                importance, has_attachments, received_at, sent_at,
                web_link, status
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9::jsonb, $10::jsonb, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21, 'pending'
            )
        """,
            uuid.UUID(msg_uuid), account_id, msg.get("id"),
            msg.get("conversationId"), msg.get("internetMessageId"),
            subject, sender_addr, sender_name,
            json.dumps(_extract_recipients(msg.get("toRecipients", []))),
            json.dumps(_extract_recipients(msg.get("ccRecipients", []))),
            direction, "inbox",
            msg.get("bodyPreview", ""),
            body_text[:500000] if body_text else None,
            hashlib.sha256((subject + body_text[:5000]).encode()).hexdigest(),
            msg.get("isRead", False),
            (msg.get("importance") or "normal").lower(),
            msg.get("hasAttachments", False),
            _parse_graph_datetime(msg.get("receivedDateTime")),
            _parse_graph_datetime(msg.get("sentDateTime")),
            msg.get("webLink"),
        )

        # Embed — structure-aware chunking; only body parts get embedded.
        full_text = f"Subject: {subject}\nFrom: {sender_name} <{sender_addr}>\n\n{body_text}"
        chunker = EmailAwareChunker()
        chunk_results = chunker.split_email_body(full_text[:50000])
        if not chunk_results:
            # Defensive fallback for short / malformed bodies
            chunk_results = [
                ChunkResult(text=t, source_part=PartType.BODY, is_embedded=True)
                for t in chunker.split(full_text[:50000])
            ]
        body_chunks = [r for r in chunk_results if r.is_embedded][:10]
        if body_chunks:
            embeddings = await embedder.embed_batch([r.text for r in body_chunks])
            for ci, (r, emb) in enumerate(zip(body_chunks, embeddings)):
                await pool.execute("""
                    INSERT INTO chat.email_message_chunks
                        (message_id, chunk_index, chunk_text, chunk_source, embedding)
                    VALUES ($1, $2, $3, $4, $5::halfvec)
                    ON CONFLICT (message_id, chunk_index) DO NOTHING
                """, uuid.UUID(msg_uuid), ci, r.text, r.source_part.value,
                     _vec_literal(emb))

        await pool.execute(
            "UPDATE chat.email_messages SET status = 'indexed', indexed_at = now() WHERE id = $1",
            uuid.UUID(msg_uuid),
        )
        return msg_uuid

    except Exception as e:
        log.error("Priority index failed for %s: %s", graph_message_id[:20], e)
        return None


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════

async def main():
    parser = argparse.ArgumentParser(description="ACP Email Sync → athena_chat DB")
    parser.add_argument("--account", help="Sync specific account email address")
    parser.add_argument("--since", help="Sync since date (YYYY-MM-DD)")
    parser.add_argument("--classify", action="store_true", help="Run AI classification after sync")
    parser.add_argument("--classify-only", action="store_true", help="Only run classification (no sync)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced")
    parser.add_argument("--limit", type=int, default=50, help="Classification batch limit")
    args = parser.parse_args()

    pool = await asyncpg.create_pool(CHAT_DATABASE_URL, min_size=2, max_size=10)
    graph = GraphClient()
    embedder = EmbeddingClient()

    try:
        # Classification only
        if args.classify_only:
            n = await classify_unclassified(pool, limit=args.limit)
            log.info("Classified %d emails", n)
            return

        # Authenticate
        if not await graph.authenticate():
            log.error("Cannot continue without Graph API authentication")
            sys.exit(1)

        # Get accounts to sync
        if args.account:
            accounts = await pool.fetch(
                "SELECT * FROM chat.email_accounts WHERE email_address = $1 AND is_active = true",
                args.account,
            )
        else:
            accounts = await pool.fetch(
                "SELECT * FROM chat.email_accounts WHERE is_active = true"
            )

        if not accounts:
            log.warning("No active email accounts found")
            return

        log.info("╔══════════════════════════════════════════════════╗")
        log.info("║  Athena Email Sync → Chat DB                    ║")
        log.info("║  Accounts: %-36s  ║", len(accounts))
        log.info("╚══════════════════════════════════════════════════╝")

        total_stats = {"messages_new": 0, "messages_failed": 0}
        for acct in accounts:
            result = await sync_account(
                pool, graph, embedder, dict(acct),
                since=args.since, dry_run=args.dry_run, classify=args.classify,
            )
            total_stats["messages_new"] += result.get("messages_new", 0)
            total_stats["messages_failed"] += result.get("messages_failed", 0)

        log.info("═══════════════════════════════════════════════════")
        log.info("  TOTAL: %d new, %d failed", total_stats["messages_new"], total_stats["messages_failed"])
        log.info("═══════════════════════════════════════════════════")

    finally:
        await graph.close()
        await embedder.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
