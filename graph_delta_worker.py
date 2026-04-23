#!/usr/bin/env python3
"""
Graph Delta Worker — source-deletion tombstone tracker.

Polls Microsoft Graph's delta endpoint for each configured mailbox/folder,
detects `@removed` entries (messages the user/owner deleted upstream), and
stamps `legal.email_metadata.source_deleted_at` on any document we already
ingested. That column is the clock that drives eventual retention-based
purging.

What this worker does NOT do:
  - Ingest new messages. email_sync.py still owns the ingest path (it uses
    `$search`-filtered fetches, not delta). The delta worker is the
    lightweight "did anything disappear upstream" sidekick.
  - Purge local data. That's retention_sweep.py (a separate, dry-run-default
    job that reads source_deleted_at + classification + legal_hold to decide
    what's eligible for deletion and when).
  - Track changes to existing messages (body edits, flag changes). Graph
    delta returns those too, but we don't act on them — the doc_id stays
    valid and the ingest already captured body/attachments.

Bootstrap semantics:
  On first run for a (mailbox, folder), delta_link is NULL in
  ops.mailbox_delta_state. We call Graph's initial delta endpoint and page
  through every current message in the folder (the initial call returns the
  full set) to establish a cursor. We DO NOT stamp source_deleted_at on
  this bootstrap pass — we only record the delta_link for subsequent
  incremental runs.

Usage:
  python graph_delta_worker.py                # poll all active mailboxes
  python graph_delta_worker.py --mailbox X    # just one mailbox
  python graph_delta_worker.py --dry-run      # log what would change,
                                              # don't write source_deleted_at
                                              # or advance the cursor
  python graph_delta_worker.py --reset MB     # clear cursor for a mailbox
                                              # (forces a fresh bootstrap)

Runs under /opt/wdws/venv. Typical cadence: every 15 min (see systemd unit).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import asyncpg
import httpx

# Local .env loader — kept independent of email_sync to avoid pulling in
# that module's (heavier) import-time side effects.
_ENV_PATH = Path("/opt/wdws/.env")
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())


log = logging.getLogger("graph-delta-worker")

DATABASE_URL = os.environ["DATABASE_URL"]
GRAPH_CLIENT_ID = os.environ["GRAPH_CLIENT_ID"]
GRAPH_CLIENT_SECRET = os.environ["GRAPH_CLIENT_SECRET"]
GRAPH_TENANT_ID = os.environ["GRAPH_TENANT_ID"]
GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

# Folders to poll per mailbox. Inbox covers incoming mail; start conservative
# and add more (sentitems, custom folders) only if we decide we care about
# retention tracking there too.
DEFAULT_FOLDERS = ["inbox", "sentitems"]

# Per-run safety: a single poll won't page forever. If Graph keeps returning
# @odata.nextLink without ever emitting @odata.deltaLink, bail out.
MAX_PAGES_PER_RUN = 200

# Max concurrent ingests per run. IMPORTANT: Falcon-OCR serializes on the
# single T4 GPU — concurrency > 1 doesn't improve throughput, it just queues
# requests at the Falcon server where they can time out. We saw 9 timeouts /
# 5 successes at concurrency=4 with compile-cache VRAM drift. Keep at 1.
# If Falcon is upgraded to Ampere or we add a multi-GPU setup, bump this.
INGEST_CONCURRENCY = int(os.getenv("DELTA_INGEST_CONCURRENCY", "1"))

# Delta $select — include everything process_and_ingest_email needs AND enough
# for per-mailbox sender filtering without a second /messages/{id} call.
_DELTA_SELECT = (
    "id,subject,receivedDateTime,from,toRecipients,ccRecipients,"
    "bccRecipients,hasAttachments,internetMessageId,importance"
)


# ============================================================
# Graph auth (lightweight client-credentials flow)
# ============================================================

class _GraphAuth:
    """Minimal app-only auth — caches token, refreshes when close to expiry."""

    def __init__(self, client: httpx.AsyncClient):
        self._client = client
        self._token: Optional[str] = None
        self._expires_at: float = 0.0

    async def token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at - 60:
            return self._token
        resp = await self._client.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": GRAPH_CLIENT_ID,
                "client_secret": GRAPH_CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
        self._expires_at = now + int(body.get("expires_in", 3600))
        log.debug("Graph token refreshed; expires in %ds", body.get("expires_in"))
        return self._token


# ============================================================
# Delta-state helpers
# ============================================================

async def load_delta_link(
    pool: asyncpg.Pool, mailbox: str, folder: str,
) -> Optional[str]:
    return await pool.fetchval(
        "SELECT delta_link FROM ops.mailbox_delta_state "
        "WHERE mailbox = $1 AND folder = $2",
        mailbox, folder,
    )


async def upsert_delta_state(
    pool: asyncpg.Pool,
    mailbox: str,
    folder: str,
    *,
    delta_link: Optional[str],
    messages_seen: int,
    removals: int,
    error: Optional[str] = None,
) -> None:
    """Upsert the state row, stamping last_polled_at always and last_success_at
    only when error is None. Counter increments are additive (accumulate total
    work over the worker's lifetime)."""
    is_success = error is None
    await pool.execute(
        """
        INSERT INTO ops.mailbox_delta_state
            (mailbox, folder, delta_link, last_polled_at, last_success_at,
             last_error, last_error_at, messages_seen_total, removals_total)
        VALUES ($1, $2, $3, now(),
                CASE WHEN $4 THEN now() ELSE NULL END,
                $5,
                CASE WHEN $4 THEN NULL ELSE now() END,
                $6, $7)
        ON CONFLICT (mailbox, folder) DO UPDATE SET
            delta_link = CASE
                WHEN EXCLUDED.delta_link IS NOT NULL THEN EXCLUDED.delta_link
                ELSE ops.mailbox_delta_state.delta_link
            END,
            last_polled_at = now(),
            last_success_at = CASE WHEN $4 THEN now()
                                    ELSE ops.mailbox_delta_state.last_success_at END,
            last_error = $5,
            last_error_at = CASE WHEN $4 THEN ops.mailbox_delta_state.last_error_at
                                   ELSE now() END,
            messages_seen_total = ops.mailbox_delta_state.messages_seen_total
                                 + $6,
            removals_total = ops.mailbox_delta_state.removals_total + $7
        """,
        mailbox, folder, delta_link, is_success, error,
        int(messages_seen), int(removals),
    )


async def reset_delta_state(pool: asyncpg.Pool, mailbox: str) -> int:
    """Clear cursors for a mailbox — forces a fresh bootstrap on next run."""
    result = await pool.execute(
        "DELETE FROM ops.mailbox_delta_state WHERE mailbox = $1",
        mailbox,
    )
    # asyncpg returns 'DELETE N'
    try:
        return int(result.split()[-1])
    except Exception:
        return 0


# ============================================================
# Source-deletion stamp
# ============================================================

async def stamp_source_deleted(
    pool: asyncpg.Pool, graph_message_id: str,
) -> Optional[str]:
    """Mark the matching legal.email_metadata row as source-deleted if we have
    a local document for it. Returns the document_id we stamped, or None if
    we never ingested this message (no local row to update).

    Match is via core.documents.metadata->>'graph_message_id' since that's
    how email_sync.py stores the Graph ID. legal.email_metadata.message_id
    holds the RFC-5322 Message-ID header, which is different.
    """
    row = await pool.fetchrow(
        """
        UPDATE legal.email_metadata em
           SET source_deleted_at = now()
          FROM core.documents d
         WHERE em.document_id = d.id
           AND d.metadata->>'graph_message_id' = $1
           AND em.source_deleted_at IS NULL
        RETURNING em.document_id
        """,
        graph_message_id,
    )
    return str(row["document_id"]) if row else None


# ============================================================
# Per-mailbox ingest strategy + filter matching
# ============================================================

async def load_mailbox_config(
    pool: asyncpg.Pool, mailbox: str,
) -> tuple[str, dict]:
    """Return (strategy, filter_config) for a mailbox.

    Strategy is 'full' | 'filtered' | 'disabled' — see migration 025.
    filter_config is a dict with 'domains', 'domain_patterns', 'emails' lists.
    """
    row = await pool.fetchrow(
        "SELECT ingest_strategy, COALESCE(filter_config, '{}'::jsonb) AS filter_config "
        "FROM ops.sync_mailboxes WHERE email = $1",
        mailbox,
    )
    if not row:
        # Unknown mailbox — default to filtered with empty allowlist = skip all
        return "filtered", {}
    fc = row["filter_config"]
    if isinstance(fc, str):
        try:
            fc = json.loads(fc)
        except Exception:
            fc = {}
    return row["ingest_strategy"], fc or {}


def _extract_sender_email(msg: dict) -> str:
    """Pull the sender address from a Graph message's `from` field."""
    frm = msg.get("from") or {}
    ea = frm.get("emailAddress") or {}
    return (ea.get("address") or "").strip().lower()


def _domain_pattern_matches(domain: str, pattern: str) -> bool:
    """'*.uscourts.gov' → matches any subdomain of uscourts.gov."""
    pattern = pattern.lower().strip()
    domain = domain.lower().strip()
    if pattern.startswith("*."):
        suffix = pattern[2:]
        return domain == suffix or domain.endswith("." + suffix)
    return domain == pattern


def matches_filter(sender_email: str, filter_config: dict) -> bool:
    """Return True if this sender matches the mailbox's filter allowlist."""
    if not sender_email:
        return False
    email_lc = sender_email.lower()
    if email_lc in [e.lower() for e in (filter_config.get("emails") or [])]:
        return True
    if "@" not in email_lc:
        return False
    domain = email_lc.split("@", 1)[1]
    for d in filter_config.get("domains") or []:
        if domain == d.lower():
            return True
    for p in filter_config.get("domain_patterns") or []:
        if _domain_pattern_matches(domain, p):
            return True
    return False


async def is_already_ingested(
    pool: asyncpg.Pool, mailbox: str, graph_id: str,
) -> bool:
    """Has this graph_id already been ingested?

    email_sync.process_and_ingest_email builds source_path from the RFC-5322
    Message-ID header (only known AFTER fetching MIME), so we can't rely on
    source_path for a pre-fetch check. But it also stamps
    metadata.graph_message_id with the Graph API ID — which we DO have from
    delta — so we can dedup cheaply without pulling the MIME.
    """
    existing = await pool.fetchval(
        """
        SELECT 1
          FROM core.documents
         WHERE metadata->>'graph_message_id' = $1
            OR source_path = $2
         LIMIT 1
        """,
        graph_id, f"graph://{mailbox}/{graph_id}",
    )
    return existing is not None


# ============================================================
# Ingest one message via email_sync.process_and_ingest_email
# ============================================================

async def ingest_graph_message(
    pool: asyncpg.Pool,
    mailbox: str,
    delta_item: dict,
    seen_message_ids: set,
    embedder,
    chunker,
    graph_client,
    dry_run: bool,
) -> tuple[bool, Optional[str]]:
    """Fetch MIME + call email_sync.process_and_ingest_email for this message.

    Returns (ingested: bool, error_or_None).
    Errors are logged and counted but do not abort the caller's loop.
    """
    from email_sync import process_and_ingest_email

    graph_id = delta_item.get("id") or ""
    subject = (delta_item.get("subject") or "")[:80]

    if dry_run:
        log.info("    [dry-run] would ingest %s  subj=%r  from=%s",
                 graph_id[:20], subject,
                 _extract_sender_email(delta_item) or "(unknown)")
        return True, None

    # Fetch MIME bytes
    mime_data, err = await graph_client.get_message_mime(mailbox, graph_id)
    if err or not mime_data:
        msg = (err or {}).get("error") if isinstance(err, dict) else str(err)
        log.warning("    MIME fetch failed for %s: %s", graph_id[:20], msg)
        return False, f"mime_fetch:{msg}"

    try:
        doc = await process_and_ingest_email(
            pool=pool,
            embedder=embedder,
            chunker=chunker,
            msg_meta=delta_item,
            mime_data=mime_data,
            mailbox=mailbox,
            seen_message_ids=seen_message_ids,
        )
        if doc is None:
            # Duplicate or chunking failure — not an error, just a no-op
            return False, None
        return True, None
    except Exception as e:
        log.exception("    ingest exception for %s", graph_id[:20])
        return False, f"{type(e).__name__}: {str(e)[:200]}"


# ============================================================
# Folder enumeration (recursive)
# ============================================================

# Folders we always skip — pre-staging folders where messages are transient or
# duplicate (Drafts is staging, Outbox is in-flight, Conversation History is
# Teams chat artifacts, Sync Issues is Outlook internal).
_SKIP_FOLDER_NAMES = {
    "Drafts", "Outbox", "Conversation History",
    "Sync Issues", "Local Failures", "Server Failures",
    "RSS Feeds", "RSS Subscriptions",
}

# Map Graph display names to well-known names — preserves existing delta
# cursors that were recorded against "inbox" / "sentitems" string keys.
_WELL_KNOWN_BY_NAME = {
    "Inbox": "inbox",
    "Sent Items": "sentitems",
    "Deleted Items": "deleteditems",
    "Junk Email": "junkemail",
    "Archive": "archive",
}


async def list_all_folders(
    client: httpx.AsyncClient,
    auth: _GraphAuth,
    mailbox: str,
) -> list[dict]:
    """Recursively enumerate every mail folder for a mailbox.

    Returns a list of {key, display_name, total_count, parent_path} where
    `key` is either a well-known name (inbox, sentitems, etc.) or a folder ID
    for custom folders. Empty folders are skipped. Excluded folders
    (_SKIP_FOLDER_NAMES) are pruned even if they contain messages.
    """
    out: list[dict] = []

    async def _walk(base_url: str, path_prefix: str) -> None:
        url = (f"{base_url}?$select=id,displayName,totalItemCount,"
               "childFolderCount&$top=100")
        while url:
            token = await auth.token()
            r = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            if r.status_code != 200:
                log.warning("  folder-list %s → HTTP %d", url, r.status_code)
                return
            body = r.json()
            for f in body.get("value", []):
                dn = f.get("displayName") or ""
                fid = f.get("id") or ""
                total = int(f.get("totalItemCount") or 0)
                childcount = int(f.get("childFolderCount") or 0)
                path = f"{path_prefix}/{dn}" if path_prefix else dn

                if dn in _SKIP_FOLDER_NAMES:
                    log.debug("  skip folder by name: %s", path)
                elif total > 0:
                    key = _WELL_KNOWN_BY_NAME.get(dn, fid)
                    out.append({
                        "key": key,
                        "display_name": dn,
                        "total_count": total,
                        "parent_path": path,
                    })
                if childcount > 0:
                    child_url = f"{GRAPH_BASE}/users/{mailbox}/mailFolders/{fid}/childFolders"
                    await _walk(child_url, path)
            url = body.get("@odata.nextLink")

    await _walk(f"{GRAPH_BASE}/users/{mailbox}/mailFolders", "")
    return out


# ============================================================
# Delta polling loop
# ============================================================

async def poll_folder(
    client: httpx.AsyncClient,
    auth: _GraphAuth,
    pool: asyncpg.Pool,
    mailbox: str,
    folder: str,
    strategy: str,
    filter_config: dict,
    dry_run: bool,
    embedder=None,
    chunker=None,
    graph_client=None,
    ingest_sem: Optional[asyncio.Semaphore] = None,
    max_ingests: Optional[int] = None,
) -> tuple[int, int, int, Optional[str], Optional[str]]:
    """Poll one (mailbox, folder) pair. Returns
    (messages_seen, removals_stamped, messages_ingested,
     new_delta_link, error_or_None).
    """
    delta_link = await load_delta_link(pool, mailbox, folder)
    bootstrap = delta_link is None

    if bootstrap:
        url: Optional[str] = (
            f"{GRAPH_BASE}/users/{mailbox}/mailFolders/{folder}/messages/delta"
            f"?$select={_DELTA_SELECT}&$top=200"
        )
        log.info("  [%s/%s] BOOTSTRAP (strategy=%s) — walking current state",
                 mailbox, folder, strategy)
    else:
        url = delta_link
        log.info("  [%s/%s] resuming from cursor (strategy=%s)",
                 mailbox, folder, strategy)

    if strategy == "disabled":
        log.info("  [%s/%s] strategy=disabled — skipping entirely",
                 mailbox, folder)
        return 0, 0, 0, delta_link, None

    messages_seen = 0
    removals = 0
    ingested = 0
    ingest_failed = 0
    skipped_filter = 0
    skipped_already = 0
    seen_message_ids: set = set()
    # Collect messages to ingest per-page, then fan out with the semaphore
    final_delta_link: Optional[str] = None
    pages = 0

    async def _ingest_one(item):
        """Semaphore-bounded wrapper for a single ingest."""
        nonlocal ingested, ingest_failed
        async with ingest_sem:
            ok, err = await ingest_graph_message(
                pool, mailbox, item, seen_message_ids,
                embedder, chunker, graph_client, dry_run,
            )
            if ok:
                ingested += 1
            elif err:
                ingest_failed += 1

    while url and pages < MAX_PAGES_PER_RUN:
        pages += 1
        token = await auth.token()
        resp = await client.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Prefer": 'odata.maxpagesize=200',
            },
            timeout=60,
        )
        if resp.status_code == 429:
            retry = int(resp.headers.get("Retry-After", "30"))
            log.warning("  [%s/%s] Graph 429 — sleeping %ds",
                        mailbox, folder, retry)
            await asyncio.sleep(retry)
            continue
        if resp.status_code == 410:
            # Cursor expired (> 30 days). Force re-bootstrap next run by
            # clearing delta_link.
            log.warning("  [%s/%s] delta cursor expired (HTTP 410) — "
                        "clearing to re-bootstrap", mailbox, folder)
            return messages_seen, removals, ingested, None, "cursor_expired"
        resp.raise_for_status()
        body = resp.json()

        ingest_tasks = []
        for item in body.get("value", []):
            messages_seen += 1
            graph_id = item.get("id") or ""

            # ── Removed at source (tombstone) ──────────────────────────
            if "@removed" in item:
                if not graph_id:
                    continue
                if dry_run:
                    doc_id = await pool.fetchval(
                        "SELECT d.id FROM core.documents d "
                        "WHERE d.metadata->>'graph_message_id' = $1",
                        graph_id,
                    )
                    if doc_id:
                        log.info("    [dry-run] would tombstone %s "
                                 "(graph_id=%s)", doc_id, graph_id[:20])
                    continue
                if bootstrap:
                    # Bootstrap: @removed items reflect prior deletions,
                    # not fresh ones. Skip stamping on initial walk.
                    continue
                stamped = await stamp_source_deleted(pool, graph_id)
                if stamped:
                    removals += 1
                continue

            # ── New / changed message → candidate for ingest ───────────
            # Strategy gating
            if strategy == "filtered":
                sender = _extract_sender_email(item)
                if not matches_filter(sender, filter_config):
                    skipped_filter += 1
                    continue

            # Dedup: have we seen this graph_id already in this mailbox?
            if await is_already_ingested(pool, mailbox, graph_id):
                skipped_already += 1
                continue

            # Max-ingests cap (for sliced test runs)
            if max_ingests is not None and (
                ingested + len(ingest_tasks) >= max_ingests
            ):
                log.info("  [%s/%s] max-ingests cap %d reached, stopping",
                         mailbox, folder, max_ingests)
                # Drain queued tasks then exit the page loop
                if ingest_tasks:
                    await asyncio.gather(*ingest_tasks, return_exceptions=True)
                return messages_seen, removals, ingested, delta_link, None

            # Fan out ingestion with semaphore bound
            ingest_tasks.append(asyncio.create_task(_ingest_one(item)))

        # Process this page's ingest queue before fetching the next page.
        # Keeps memory + Graph concurrency bounded and makes progress visible.
        if ingest_tasks:
            log.info("  [%s/%s] page %d: queued %d ingests",
                     mailbox, folder, pages, len(ingest_tasks))
            await asyncio.gather(*ingest_tasks, return_exceptions=True)
            log.info("  [%s/%s] page %d: ingested=%d failed=%d "
                     "(cum: ingested=%d failed=%d skipped_filter=%d "
                     "skipped_already=%d)",
                     mailbox, folder, pages, len(ingest_tasks),
                     ingest_failed, ingested, ingest_failed,
                     skipped_filter, skipped_already)

        # Next page OR end-of-batch deltaLink
        next_link = body.get("@odata.nextLink")
        delta_link_next = body.get("@odata.deltaLink")
        if delta_link_next:
            final_delta_link = delta_link_next
            url = None
            break
        url = next_link

    if pages >= MAX_PAGES_PER_RUN and final_delta_link is None:
        log.warning("  [%s/%s] hit MAX_PAGES_PER_RUN without deltaLink — "
                    "will continue next run", mailbox, folder)
        return messages_seen, removals, ingested, None, "max_pages_reached"

    return messages_seen, removals, ingested, final_delta_link, None


# ============================================================
# Orchestration
# ============================================================

async def active_mailboxes(pool: asyncpg.Pool) -> list[str]:
    """Prefer ops.sync_mailboxes (runtime-configurable), fall back to the two
    hardcoded production mailboxes if the table is empty."""
    try:
        rows = await pool.fetch(
            "SELECT email FROM ops.sync_mailboxes "
            "WHERE is_active = true ORDER BY id"
        )
        if rows:
            return [r["email"] for r in rows]
    except Exception:
        pass
    return ["william@seattleseahawks.me", "athena@seattleseahawks.me"]


async def run(
    mailbox_filter: Optional[str],
    dry_run: bool,
    reset_mailbox: Optional[str],
    max_ingests: Optional[int] = None,
) -> int:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)

    try:
        if reset_mailbox:
            deleted = await reset_delta_state(pool, reset_mailbox)
            log.info("Reset delta state for %s — %d rows deleted",
                     reset_mailbox, deleted)
            return 0

        mailboxes = await active_mailboxes(pool)
        if mailbox_filter:
            mailboxes = [m for m in mailboxes if m == mailbox_filter]
            if not mailboxes:
                log.error("No active mailbox matches %r", mailbox_filter)
                return 1

        log.info("Polling %d mailbox(es): %s", len(mailboxes), mailboxes)

        # Build shared resources used for ingestion
        from email_sync import GraphClient, EmbeddingClient, TextChunker
        embedder = EmbeddingClient()
        chunker = TextChunker()
        graph_client = GraphClient()
        if not await graph_client.authenticate():
            log.error("Graph authentication failed")
            return 1
        ingest_sem = asyncio.Semaphore(INGEST_CONCURRENCY)

        try:
            async with httpx.AsyncClient(timeout=60) as client:
                auth = _GraphAuth(client)
                total_seen = 0
                total_removals = 0
                total_ingested = 0
                for mailbox in mailboxes:
                    strategy, filter_config = await load_mailbox_config(
                        pool, mailbox,
                    )
                    log.info("[%s] strategy=%s filter_domains=%d emails=%d",
                             mailbox, strategy,
                             len(filter_config.get("domains") or []),
                             len(filter_config.get("emails") or []))

                    # Enumerate every non-empty folder in this mailbox so we
                    # capture Archive, Junk, Deleted Items, and user-created
                    # subfolders — not just inbox/sentitems.
                    try:
                        discovered = await list_all_folders(client, auth, mailbox)
                    except Exception:
                        log.exception("[%s] folder enumeration failed — "
                                      "falling back to DEFAULT_FOLDERS", mailbox)
                        discovered = [{"key": f, "display_name": f,
                                       "total_count": 0, "parent_path": f}
                                      for f in DEFAULT_FOLDERS]

                    log.info("[%s] %d non-empty folder(s) to poll: %s",
                             mailbox, len(discovered),
                             ", ".join(f"{f['display_name']}({f['total_count']})"
                                       for f in discovered))

                    for folder_info in discovered:
                        folder = folder_info["key"]
                        try:
                            remaining = (max_ingests - total_ingested
                                         if max_ingests is not None else None)
                            if remaining is not None and remaining <= 0:
                                log.info("[%s/%s] global max-ingests reached",
                                         mailbox, folder_info["display_name"])
                                break
                            seen, removed, ing, new_link, err = await poll_folder(
                                client, auth, pool, mailbox, folder,
                                strategy, filter_config, dry_run,
                                embedder=embedder,
                                chunker=chunker,
                                graph_client=graph_client,
                                ingest_sem=ingest_sem,
                                max_ingests=remaining,
                            )
                        except Exception as e:
                            log.exception("[%s/%s] poll failed", mailbox, folder)
                            if not dry_run:
                                await upsert_delta_state(
                                    pool, mailbox, folder,
                                    delta_link=None,
                                    messages_seen=0, removals=0,
                                    error=f"{type(e).__name__}: {e}"[:500],
                                )
                            continue

                        total_seen += seen
                        total_removals += removed
                        total_ingested += ing

                        if dry_run:
                            log.info("  [%s/%s] dry-run done: seen=%d "
                                     "would-tombstone=%d would-ingest=%d",
                                     mailbox, folder, seen, removed, ing)
                            continue

                        await upsert_delta_state(
                            pool, mailbox, folder,
                            delta_link=new_link,
                            messages_seen=seen,
                            removals=removed,
                            error=err,
                        )
                        log.info("  [%s/%s] done: seen=%d tombstoned=%d "
                                 "ingested=%d",
                                 mailbox, folder, seen, removed, ing)

                log.info("TOTAL: seen=%d tombstoned=%d ingested=%d",
                         total_seen, total_removals, total_ingested)
        finally:
            await graph_client.close()
            await embedder.close()
        return 0
    finally:
        await pool.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Graph delta-poll worker")
    ap.add_argument("--mailbox", help="Poll only this mailbox")
    ap.add_argument("--dry-run", action="store_true",
                    help="Log would-be stamps; do not update DB or advance cursor")
    ap.add_argument("--reset",
                    help="Clear cursors for a mailbox (forces fresh bootstrap)")
    ap.add_argument("--max-ingests", type=int, default=None,
                    help="Cap total messages ingested in this run "
                         "(for sliced test runs)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    return asyncio.run(run(args.mailbox, args.dry_run, args.reset,
                           max_ingests=args.max_ingests))


if __name__ == "__main__":
    sys.exit(main())
