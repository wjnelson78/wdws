#!/usr/bin/env python3
"""
Athena Email API — REST endpoints for O365 email integration.

Provides:
  • Email account management (list, register)
  • Email listing and search (backed by chat DB, not live Graph)
  • Semantic search across emails (pgvector RAG)
  • AI enrichment overlay (urgency, case, deadlines)
  • Space ↔ email linking
  • Priority indexing trigger (app notifies server about viewed emails)
  • Send / reply via Graph API
  • Sync status and triggers

These routes are mounted into the main chat_api.py Starlette app.
"""
import json
import hashlib
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import asyncpg
import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse

log = logging.getLogger("email_api")

# ── Config (inherited from chat_api env) ──────────────────────
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMS = 3072

GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token"

# Cached Graph token
_graph_token_cache = {"access_token": None, "expires_at": 0}


# ── Helpers ───────────────────────────────────────────────────

def _json(data: Any, status: int = 200) -> JSONResponse:
    """JSON response with UUID/datetime handling."""
    import decimal
    import datetime as _dt

    class _Enc(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, uuid.UUID):
                return str(obj)
            if isinstance(obj, (_dt.datetime, _dt.date)):
                return obj.isoformat()
            if isinstance(obj, _dt.timedelta):
                return obj.total_seconds()
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            if isinstance(obj, bytes):
                import base64
                return base64.b64encode(obj).decode()
            return super().default(obj)

    body = json.dumps(data, cls=_Enc)
    return JSONResponse(content=json.loads(body), status_code=status)


def _json_error(msg: str, status: int = 400) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=status)


def _row(record: asyncpg.Record) -> dict:
    return json.loads(json.dumps(dict(record), cls=_json.__code__.co_consts[1] if False else type(
        '_E', (json.JSONEncoder,), {
            'default': lambda self, o: (
                str(o) if isinstance(o, uuid.UUID) else
                o.isoformat() if hasattr(o, 'isoformat') else
                float(o) if isinstance(o, __import__('decimal').Decimal) else
                super().default(o)
            )
        }
    )))


def _ser(record: asyncpg.Record) -> dict:
    """Safely serialize an asyncpg Record."""
    import decimal as _dec
    import datetime as _dt
    result = {}
    for k, v in dict(record).items():
        if isinstance(v, uuid.UUID):
            result[k] = str(v)
        elif isinstance(v, (_dt.datetime, _dt.date)):
            result[k] = v.isoformat()
        elif isinstance(v, _dt.timedelta):
            result[k] = v.total_seconds()
        elif isinstance(v, _dec.Decimal):
            result[k] = float(v)
        elif isinstance(v, bytes):
            import base64
            result[k] = base64.b64encode(v).decode()
        else:
            result[k] = v
    return result


async def _read_json(request: Request) -> dict:
    try:
        return await request.json()
    except Exception:
        return {}


async def _embed_query(text: str) -> list[float]:
    """Get embedding vector for a search query."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={"model": EMBEDDING_MODEL, "input": text, "dimensions": EMBEDDING_DIMS},
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


async def _get_graph_token() -> str:
    """Get or refresh app-only Graph token."""
    global _graph_token_cache
    if _graph_token_cache["access_token"] and time.time() < _graph_token_cache["expires_at"] - 300:
        return _graph_token_cache["access_token"]

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
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
    _graph_token_cache["access_token"] = data["access_token"]
    _graph_token_cache["expires_at"] = time.time() + data.get("expires_in", 3600)
    return _graph_token_cache["access_token"]


async def _graph_get(url: str, params: dict = None) -> Optional[dict]:
    """Authenticated Graph GET request."""
    token = await _get_graph_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


async def _graph_post(url: str, json_data: dict) -> Optional[dict]:
    """Authenticated Graph POST request."""
    token = await _get_graph_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=json_data,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}


# ══════════════════════════════════════════════════════════════
# ACCOUNT ENDPOINTS
# ══════════════════════════════════════════════════════════════

async def list_email_accounts(request: Request) -> JSONResponse:
    """GET /api/email/accounts — List registered email accounts + sync status."""
    from chat_api import get_pool
    p = await get_pool()
    rows = await p.fetch("""
        SELECT a.*,
            (SELECT COUNT(*) FROM chat.email_messages m WHERE m.account_id = a.id) AS message_count,
            (SELECT COUNT(*) FROM chat.email_messages m 
             WHERE m.account_id = a.id AND m.status = 'indexed') AS indexed_count,
            (SELECT COUNT(*) FROM chat.email_ai_classifications aic
             JOIN chat.email_messages m ON m.id = aic.message_id
             WHERE m.account_id = a.id AND aic.urgency = 'urgent') AS urgent_count
        FROM chat.email_accounts a
        ORDER BY a.email_address
    """)
    return _json({"accounts": [_ser(r) for r in rows]})


async def register_email_account(request: Request) -> JSONResponse:
    """POST /api/email/accounts — Register a new email account for sync."""
    data = await _read_json(request)
    email_address = (data.get("email_address") or "").strip().lower()
    if not email_address or "@" not in email_address:
        return _json_error("Valid email_address required")

    from chat_api import get_pool
    p = await get_pool()
    try:
        row = await p.fetchrow("""
            INSERT INTO chat.email_accounts (email_address, display_name, label, sync_folders, sync_domains)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (email_address) DO UPDATE SET
                display_name = COALESCE(EXCLUDED.display_name, chat.email_accounts.display_name),
                label = COALESCE(EXCLUDED.label, chat.email_accounts.label),
                updated_at = now()
            RETURNING *
        """,
            email_address,
            data.get("display_name"),
            data.get("label"),
            data.get("sync_folders", ["inbox", "sentitems"]),
            data.get("sync_domains"),
        )
        return _json({"account": _ser(row)}, 201)
    except Exception as e:
        return _json_error(str(e), 500)


# ══════════════════════════════════════════════════════════════
# EMAIL LISTING / DETAIL
# ══════════════════════════════════════════════════════════════

async def list_emails(request: Request) -> JSONResponse:
    """GET /api/email/messages — List synced emails with filtering.
    
    Query params:
      account_id, folder, direction, importance, is_flagged,
      sender, since, until, has_attachments, limit, offset
    """
    from chat_api import get_pool
    p = await get_pool()

    qp = request.query_params
    conditions = []
    params = []
    idx = 1

    if qp.get("account_id"):
        conditions.append(f"em.account_id = ${idx}")
        params.append(int(qp["account_id"]))
        idx += 1
    if qp.get("folder"):
        conditions.append(f"em.folder_name = ${idx}")
        params.append(qp["folder"])
        idx += 1
    if qp.get("direction"):
        conditions.append(f"em.direction = ${idx}")
        params.append(qp["direction"])
        idx += 1
    if qp.get("importance"):
        conditions.append(f"em.importance = ${idx}")
        params.append(qp["importance"])
        idx += 1
    if qp.get("is_flagged") == "true":
        conditions.append("em.is_flagged = true")
    if qp.get("sender"):
        conditions.append(f"em.sender_email ILIKE ${idx}")
        params.append(f"%{qp['sender']}%")
        idx += 1
    if qp.get("since"):
        conditions.append(f"em.received_at >= ${idx}::timestamptz")
        params.append(qp["since"])
        idx += 1
    if qp.get("until"):
        conditions.append(f"em.received_at <= ${idx}::timestamptz")
        params.append(qp["until"] + "T23:59:59Z")
        idx += 1
    if qp.get("has_attachments") == "true":
        conditions.append("em.has_attachments = true")

    limit = min(int(qp.get("limit", "50")), 200)
    offset = int(qp.get("offset", "0"))

    where = " AND ".join(conditions) if conditions else "true"

    rows = await p.fetch(f"""
        SELECT em.id, em.account_id, em.graph_message_id, em.subject,
               em.sender_email, em.sender_name, em.to_recipients, em.cc_recipients,
               em.direction, em.folder_name, em.body_preview, em.importance,
               em.is_read, em.is_flagged, em.has_attachments, em.categories,
               em.received_at, em.sent_at, em.web_link, em.status,
               aic.urgency, aic.category, aic.case_number, aic.requires_action,
               aic.action_summary, aic.deadlines
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE {where}
        ORDER BY em.received_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """, *params, limit, offset)

    total = await p.fetchval(f"""
        SELECT COUNT(*) FROM chat.email_messages em WHERE {where}
    """, *params)

    return _json({
        "messages": [_ser(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    })


async def get_email(request: Request) -> JSONResponse:
    """GET /api/email/messages/{message_id} — Full email detail."""
    message_id = request.path_params["message_id"]
    from chat_api import get_pool
    p = await get_pool()

    row = await p.fetchrow("""
        SELECT em.*, 
               aic.urgency, aic.category, aic.case_number, aic.case_confidence,
               aic.requires_action, aic.action_summary, aic.deadlines, aic.reasoning,
               a.email_address AS mailbox
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        LEFT JOIN chat.email_accounts a ON a.id = em.account_id
        WHERE em.id = $1
    """, uuid.UUID(message_id))

    if not row:
        return _json_error("Email not found", 404)

    result = _ser(row)

    # Get attachments
    attachments = await p.fetch(
        "SELECT * FROM chat.email_attachments WHERE message_id = $1",
        uuid.UUID(message_id),
    )
    result["attachments"] = [_ser(a) for a in attachments]

    return _json(result)


async def get_email_thread(request: Request) -> JSONResponse:
    """GET /api/email/threads/{conversation_id} — Full email thread."""
    conv_id = request.path_params["conversation_id"]
    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT em.id, em.subject, em.sender_email, em.sender_name,
               em.to_recipients, em.cc_recipients, em.direction,
               em.body_preview, em.body_text, em.received_at, em.sent_at,
               em.has_attachments, em.web_link,
               aic.urgency, aic.category, aic.case_number
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE em.graph_conversation_id = $1
        ORDER BY em.received_at ASC
    """, conv_id)

    return _json({
        "conversation_id": conv_id,
        "messages": [_ser(r) for r in rows],
        "count": len(rows),
    })


# ══════════════════════════════════════════════════════════════
# SEARCH ENDPOINTS
# ══════════════════════════════════════════════════════════════

async def search_emails(request: Request) -> JSONResponse:
    """GET /api/email/search — Full-text search across synced emails.
    
    Query params: q (search query), limit, offset
    """
    qp = request.query_params
    query = (qp.get("q") or "").strip()
    if not query:
        return _json_error("Query parameter 'q' required")

    limit = min(int(qp.get("limit", "20")), 100)
    offset = int(qp.get("offset", "0"))

    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT em.id, em.subject, em.sender_email, em.sender_name,
               em.direction, em.body_preview, em.received_at, em.has_attachments,
               em.web_link, em.importance,
               aic.urgency, aic.category, aic.case_number, aic.requires_action,
               ts_rank(
                   to_tsvector('english',
                       coalesce(em.subject, '') || ' ' ||
                       coalesce(em.sender_name, '') || ' ' ||
                       coalesce(em.sender_email, '') || ' ' ||
                       coalesce(em.body_text, '')
                   ),
                   websearch_to_tsquery('english', $1)
               ) AS rank
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE to_tsvector('english',
                   coalesce(em.subject, '') || ' ' ||
                   coalesce(em.sender_name, '') || ' ' ||
                   coalesce(em.sender_email, '') || ' ' ||
                   coalesce(em.body_text, '')
              ) @@ websearch_to_tsquery('english', $1)
        ORDER BY rank DESC
        LIMIT $2 OFFSET $3
    """, query, limit, offset)

    return _json({
        "query": query,
        "results": [_ser(r) for r in rows],
        "count": len(rows),
    })


async def semantic_search_emails(request: Request) -> JSONResponse:
    """GET /api/email/semantic-search — AI semantic search using pgvector.
    
    Query params: q (natural language query), limit, space_id (optional scope)
    """
    qp = request.query_params
    query = (qp.get("q") or "").strip()
    if not query:
        return _json_error("Query parameter 'q' required")

    limit = min(int(qp.get("limit", "15")), 50)
    space_id = qp.get("space_id")

    from chat_api import get_pool
    p = await get_pool()

    try:
        embedding = await _embed_query(query)
        vec_literal = "[" + ",".join(str(x) for x in embedding) + "]"

        # If space_id, only search emails linked to that space
        if space_id:
            rows = await p.fetch(f"""
                SELECT DISTINCT ON (em.id)
                    em.id, em.subject, em.sender_email, em.sender_name,
                    em.direction, em.body_preview, em.received_at,
                    em.has_attachments, em.web_link,
                    aic.urgency, aic.category, aic.case_number,
                    emc.chunk_text,
                    emc.embedding <=> $1::halfvec({EMBEDDING_DIMS}) AS distance
                FROM chat.email_message_chunks emc
                JOIN chat.email_messages em ON em.id = emc.message_id
                LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
                WHERE em.account_id IN (
                    SELECT account_id FROM chat.space_email_links 
                    WHERE space_id = $2 AND account_id IS NOT NULL
                    UNION
                    SELECT em2.account_id FROM chat.space_email_links sel
                    JOIN chat.email_messages em2 ON em2.id = sel.message_id
                    WHERE sel.space_id = $2 AND sel.message_id IS NOT NULL
                )
                AND emc.embedding IS NOT NULL
                ORDER BY em.id, emc.embedding <=> $1::halfvec({EMBEDDING_DIMS})
                LIMIT $3
            """, vec_literal, uuid.UUID(space_id), limit)
        else:
            rows = await p.fetch(f"""
                SELECT DISTINCT ON (em.id)
                    em.id, em.subject, em.sender_email, em.sender_name,
                    em.direction, em.body_preview, em.received_at,
                    em.has_attachments, em.web_link,
                    aic.urgency, aic.category, aic.case_number,
                    emc.chunk_text,
                    emc.embedding <=> $1::halfvec({EMBEDDING_DIMS}) AS distance
                FROM chat.email_message_chunks emc
                JOIN chat.email_messages em ON em.id = emc.message_id
                LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
                WHERE emc.embedding IS NOT NULL
                ORDER BY em.id, emc.embedding <=> $1::halfvec({EMBEDDING_DIMS})
                LIMIT $2
            """, vec_literal, limit)

        results = []
        for r in sorted(rows, key=lambda x: x["distance"]):
            item = _ser(r)
            item["similarity"] = round(1 - float(r["distance"]), 4)
            item["matched_chunk"] = r["chunk_text"][:300]
            results.append(item)

        return _json({
            "query": query,
            "results": results,
            "count": len(results),
        })
    except Exception as e:
        log.error("Semantic search error: %s", e)
        return _json_error(str(e), 500)


# ══════════════════════════════════════════════════════════════
# ENRICHMENT ENDPOINT (called by app for AI overlay)
# ══════════════════════════════════════════════════════════════

async def enrich_emails(request: Request) -> JSONResponse:
    """POST /api/email/enrich — Get AI classifications for a batch of emails.
    
    The app calls this with Graph message IDs to get urgency badges,
    case links, deadlines, etc. for overlay on the email list view.

    Body: { "message_ids": ["AAMk..."], "mailbox": "william@..." }
    Response: { "results": { "AAMk...": { urgency, category, case, ... } } }
    """
    data = await _read_json(request)
    message_ids = data.get("message_ids", [])
    if not message_ids:
        return _json_error("message_ids array required")

    from chat_api import get_pool
    p = await get_pool()

    # Look up by graph_message_id
    rows = await p.fetch("""
        SELECT em.graph_message_id,
               aic.urgency, aic.category, aic.case_number, aic.case_confidence,
               aic.requires_action, aic.action_summary, aic.deadlines,
               em.status AS index_status
        FROM chat.email_messages em
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE em.graph_message_id = ANY($1)
    """, message_ids)

    results = {}
    for r in rows:
        results[r["graph_message_id"]] = {
            "urgency": r["urgency"],
            "category": r["category"],
            "case_number": r["case_number"],
            "case_confidence": float(r["case_confidence"]) if r["case_confidence"] else None,
            "requires_action": r["requires_action"],
            "action_summary": r["action_summary"],
            "deadlines": r["deadlines"],
            "indexed": r["index_status"] == "indexed",
        }

    # Report which ones are unknown (not yet synced)
    unknown = [mid for mid in message_ids if mid not in results]

    return _json({
        "results": results,
        "found": len(results),
        "unknown": unknown,
        "unknown_count": len(unknown),
    })


# ══════════════════════════════════════════════════════════════
# PRIORITY INDEXING (app notifies server to fast-track an email)
# ══════════════════════════════════════════════════════════════

async def priority_index(request: Request) -> JSONResponse:
    """POST /api/email/priority-index — Fast-track index a specific email.
    
    Called when the user opens an email in the app that hasn't been synced yet.
    Body: { "graph_message_id": "AAMk...", "mailbox": "william@..." }
    """
    data = await _read_json(request)
    graph_message_id = data.get("graph_message_id")
    mailbox = data.get("mailbox")
    if not graph_message_id or not mailbox:
        return _json_error("graph_message_id and mailbox required")

    from email_sync_chat import priority_index_message, GraphClient, EmbeddingClient
    from chat_api import get_pool

    p = await get_pool()
    graph = GraphClient()
    embedder = EmbeddingClient()

    try:
        if not await graph.authenticate():
            return _json_error("Graph API authentication failed", 500)

        msg_uuid = await priority_index_message(
            p, graph, embedder,
            mailbox=mailbox, graph_message_id=graph_message_id,
        )
        if msg_uuid:
            return _json({"indexed": True, "message_id": msg_uuid})
        else:
            return _json({"indexed": False, "message": "Already indexed or failed"})
    finally:
        await graph.close()
        await embedder.close()


# ══════════════════════════════════════════════════════════════
# SEND / REPLY (via Graph API)
# ══════════════════════════════════════════════════════════════

async def send_email(request: Request) -> JSONResponse:
    """POST /api/email/send — Send an email via Graph API.
    
    Body: {
      "mailbox": "william@seattleseahawks.me",
      "to": ["recipient@example.com"],
      "cc": [],
      "subject": "...",
      "body_html": "<p>...</p>",
      "importance": "normal"
    }
    """
    data = await _read_json(request)
    mailbox = data.get("mailbox")
    to_addrs = data.get("to", [])
    subject = data.get("subject", "")
    body_html = data.get("body_html", "")

    if not mailbox or not to_addrs or not subject:
        return _json_error("mailbox, to, and subject required")

    message = {
        "subject": subject,
        "body": {"contentType": "HTML", "content": body_html},
        "toRecipients": [{"emailAddress": {"address": a}} for a in to_addrs],
        "importance": data.get("importance", "normal"),
    }
    if data.get("cc"):
        message["ccRecipients"] = [{"emailAddress": {"address": a}} for a in data["cc"]]

    try:
        await _graph_post(
            f"{GRAPH_BASE_URL}/users/{mailbox}/sendMail",
            {"message": message, "saveToSentItems": True},
        )
        return _json({"sent": True, "to": to_addrs, "subject": subject})
    except Exception as e:
        return _json_error(f"Send failed: {e}", 500)


async def reply_to_email(request: Request) -> JSONResponse:
    """POST /api/email/messages/{message_id}/reply — Reply to an email.
    
    Body: { "comment": "<p>reply text</p>" }
    """
    message_id = request.path_params["message_id"]
    data = await _read_json(request)
    comment = data.get("comment", "")
    if not comment:
        return _json_error("comment required")

    from chat_api import get_pool
    p = await get_pool()

    row = await p.fetchrow("""
        SELECT em.graph_message_id, a.email_address
        FROM chat.email_messages em
        JOIN chat.email_accounts a ON a.id = em.account_id
        WHERE em.id = $1
    """, uuid.UUID(message_id))

    if not row:
        return _json_error("Message not found", 404)

    try:
        await _graph_post(
            f"{GRAPH_BASE_URL}/users/{row['email_address']}/messages/{row['graph_message_id']}/reply",
            {"comment": comment},
        )
        return _json({"replied": True, "message_id": message_id})
    except Exception as e:
        return _json_error(f"Reply failed: {e}", 500)


# ══════════════════════════════════════════════════════════════
# SPACE ↔ EMAIL LINKING
# ══════════════════════════════════════════════════════════════

async def list_space_email_links(request: Request) -> JSONResponse:
    """GET /api/email/spaces/{space_id}/links — List email links for a space."""
    space_id = request.path_params["space_id"]
    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT sel.*, 
               a.email_address AS account_email,
               em.subject AS message_subject
        FROM chat.space_email_links sel
        LEFT JOIN chat.email_accounts a ON a.id = sel.account_id
        LEFT JOIN chat.email_messages em ON em.id = sel.message_id
        WHERE sel.space_id = $1
        ORDER BY sel.linked_at DESC
    """, uuid.UUID(space_id))

    return _json({"links": [_ser(r) for r in rows]})


async def link_email_to_space(request: Request) -> JSONResponse:
    """POST /api/email/spaces/{space_id}/links — Link emails to a space.
    
    Body: one of:
      { "account_id": 1 }                               — all emails from account
      { "account_id": 1, "domain_filter": "snoco.org" } — filtered
      { "message_id": "uuid" }                           — single email
    """
    space_id = request.path_params["space_id"]
    data = await _read_json(request)

    from chat_api import get_pool
    p = await get_pool()

    account_id = data.get("account_id")
    message_id = data.get("message_id")

    if not account_id and not message_id:
        return _json_error("account_id or message_id required")

    try:
        if message_id:
            row = await p.fetchrow("""
                INSERT INTO chat.space_email_links (space_id, message_id, linked_by)
                VALUES ($1, $2, NULL)
                ON CONFLICT (space_id, message_id) DO NOTHING
                RETURNING *
            """, uuid.UUID(space_id), uuid.UUID(message_id))
        else:
            row = await p.fetchrow("""
                INSERT INTO chat.space_email_links 
                    (space_id, account_id, domain_filter, sender_filter, keyword_filter, linked_by)
                VALUES ($1, $2, $3, $4, $5, NULL)
                ON CONFLICT DO NOTHING
                RETURNING *
            """,
                uuid.UUID(space_id), int(account_id),
                data.get("domain_filter"),
                data.get("sender_filter"),
                data.get("keyword_filter"),
            )

        if row:
            return _json({"link": _ser(row)}, 201)
        else:
            return _json({"message": "Link already exists"})
    except Exception as e:
        return _json_error(str(e), 500)


async def list_space_emails(request: Request) -> JSONResponse:
    """GET /api/email/spaces/{space_id}/messages — Emails linked to a space."""
    space_id = request.path_params["space_id"]
    qp = request.query_params
    limit = min(int(qp.get("limit", "50")), 200)
    offset = int(qp.get("offset", "0"))

    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT DISTINCT em.id, em.subject, em.sender_email, em.sender_name,
               em.to_recipients, em.direction, em.body_preview,
               em.received_at, em.has_attachments, em.importance,
               em.is_flagged, em.web_link,
               aic.urgency, aic.category, aic.case_number,
               aic.requires_action, aic.action_summary, aic.deadlines
        FROM chat.space_email_links sel
        LEFT JOIN chat.email_messages em ON (
            em.id = sel.message_id OR
            (sel.account_id IS NOT NULL AND em.account_id = sel.account_id
             AND (sel.domain_filter IS NULL OR em.sender_email ILIKE '%%' || sel.domain_filter)
             AND (sel.sender_filter IS NULL OR em.sender_email ILIKE '%%' || sel.sender_filter || '%%')
             AND (sel.keyword_filter IS NULL OR em.subject ILIKE '%%' || sel.keyword_filter || '%%')
            )
        )
        LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE sel.space_id = $1 AND em.id IS NOT NULL
        ORDER BY em.received_at DESC
        LIMIT $2 OFFSET $3
    """, uuid.UUID(space_id), limit, offset)

    return _json({
        "messages": [_ser(r) for r in rows],
        "count": len(rows),
        "space_id": space_id,
    })


# ══════════════════════════════════════════════════════════════
# SYNC STATUS / TRIGGER
# ══════════════════════════════════════════════════════════════

async def get_sync_status(request: Request) -> JSONResponse:
    """GET /api/email/sync — Overview of sync status for all accounts."""
    from chat_api import get_pool
    p = await get_pool()

    # Account stats
    accounts = await p.fetch("""
        SELECT a.id, a.email_address, a.label, a.is_active, a.total_messages,
               a.total_indexed, a.last_sync_at, a.last_sync_count, a.last_sync_error
        FROM chat.email_accounts a
        ORDER BY a.email_address
    """)

    # Recent sync runs
    runs = await p.fetch("""
        SELECT sr.*, a.email_address
        FROM chat.email_sync_runs sr
        JOIN chat.email_accounts a ON a.id = sr.account_id
        ORDER BY sr.started_at DESC
        LIMIT 10
    """)

    # Overall stats
    overall = await p.fetchrow("""
        SELECT 
            COUNT(*) AS total_messages,
            COUNT(*) FILTER (WHERE status = 'indexed') AS indexed,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            MIN(received_at) AS earliest,
            MAX(received_at) AS latest
        FROM chat.email_messages
    """)

    return _json({
        "accounts": [_ser(a) for a in accounts],
        "recent_runs": [_ser(r) for r in runs],
        "overall": _ser(overall) if overall else {},
    })


async def trigger_sync(request: Request) -> JSONResponse:
    """POST /api/email/sync — Trigger a sync run.
    
    Body: { "account_id": 1 } or {} for all accounts.
    Note: This spawns the sync in the background.
    """
    data = await _read_json(request)
    account_id = data.get("account_id")

    import subprocess
    cmd = ["python3", "/opt/wdws/email_sync_chat.py", "--classify"]
    if account_id:
        from chat_api import get_pool
        p = await get_pool()
        acct = await p.fetchval(
            "SELECT email_address FROM chat.email_accounts WHERE id = $1", int(account_id)
        )
        if acct:
            cmd.extend(["--account", acct])

    # Fire and forget
    subprocess.Popen(
        cmd,
        stdout=open("/tmp/email_sync_chat.log", "a"),
        stderr=subprocess.STDOUT,
        cwd="/opt/wdws",
    )

    return _json({"triggered": True, "command": " ".join(cmd)})


# ══════════════════════════════════════════════════════════════
# GRAPH WEBHOOKS (Change Notifications)
# ══════════════════════════════════════════════════════════════

# Client state secret — Graph sends this back so we can verify authenticity
_WEBHOOK_CLIENT_STATE = hashlib.sha256(
    (GRAPH_CLIENT_SECRET or "fallback").encode()
).hexdigest()[:32]

WEBHOOK_NOTIFICATION_URL = os.getenv(
    "WEBHOOK_NOTIFICATION_URL",
    "https://athena-ai.12432.net/api/email/webhooks/notifications",
)

# Debounce: track recently triggered sync per account to avoid duplicates.
# Key = account_id, Value = timestamp of last trigger.
_webhook_sync_last_trigger: dict[int, float] = {}
_WEBHOOK_DEBOUNCE_SECONDS = 30  # ignore duplicate notifications within this window


async def webhook_validation_or_notification(request: Request) -> JSONResponse:
    """POST /api/email/webhooks/notifications — Graph webhook handler.

    Graph API calls this endpoint in two scenarios:
    1. Validation: POST with ?validationToken=<token> — we must echo the token.
    2. Notification: POST with JSON body containing change notifications.
    """
    # ── Scenario 1: Subscription validation ────────────────────
    validation = request.query_params.get("validationToken")
    if validation:
        log.info("[webhook] Subscription validation — returning token")
        from starlette.responses import PlainTextResponse
        return PlainTextResponse(validation, status_code=200)

    # ── Scenario 2: Change notification ───────────────────────
    try:
        body = await request.json()
    except Exception:
        return _json_error("Invalid JSON", 400)

    notifications = body.get("value", [])
    if not notifications:
        return _json({"ok": True, "processed": 0})

    from chat_api import get_pool
    p = await get_pool()

    synced_accounts = set()
    for notif in notifications:
        # Verify client state
        if notif.get("clientState") != _WEBHOOK_CLIENT_STATE:
            log.warning("[webhook] Invalid clientState — skipping")
            continue

        resource = notif.get("resource", "")        # e.g. Users('x@y.com')/Messages('AAMk...')
        change_type = notif.get("changeType", "")   # created | updated | deleted
        subscription_id = notif.get("subscriptionId", "")

        log.info("[webhook] %s on %s (sub=%s)", change_type, resource[:80], subscription_id[:12])

        # Look up account from subscription ID first (most reliable),
        # then fall back to extracting mailbox from resource string
        account_row = await p.fetchrow("""
            SELECT w.account_id, a.email_address
            FROM chat.email_webhooks w
            JOIN chat.email_accounts a ON a.id = w.account_id
            WHERE w.subscription_id = $1 AND w.is_active
        """, subscription_id)

        if account_row:
            account_id = account_row["account_id"]
            mailbox = account_row["email_address"]
        else:
            mailbox = _extract_mailbox_from_resource(resource)
            if not mailbox:
                log.warning("[webhook] Could not resolve account for sub=%s resource=%s", subscription_id[:12], resource[:100])
                continue
            account_id = await p.fetchval(
                "SELECT id FROM chat.email_accounts WHERE email_address = $1 AND is_active",
                mailbox.lower(),
            )
            if not account_id:
                log.warning("[webhook] No active account for %s", mailbox)
                continue

        # Deduplicate — sync once per account per webhook batch + debounce window
        if account_id in synced_accounts:
            continue
        last_trigger = _webhook_sync_last_trigger.get(account_id, 0)
        if time.time() - last_trigger < _WEBHOOK_DEBOUNCE_SECONDS:
            log.info("[webhook] Debounced sync for %s (last triggered %.0fs ago)", mailbox, time.time() - last_trigger)
            continue
        synced_accounts.add(account_id)
        _webhook_sync_last_trigger[account_id] = time.time()

        # Fire a background sync for just this account
        import subprocess
        cmd = ["python3", "/opt/wdws/email_sync_chat.py", "--classify", "--account", mailbox]
        subprocess.Popen(
            cmd,
            stdout=open("/var/log/athena-email-sync.log", "a"),
            stderr=subprocess.STDOUT,
            cwd="/opt/wdws",
        )
        log.info("[webhook] Triggered sync for %s", mailbox)

    # Graph expects 202 within 30s or it retries
    return JSONResponse({"ok": True, "processed": len(synced_accounts)}, status_code=202)


def _extract_mailbox_from_resource(resource: str) -> Optional[str]:
    """Extract email address from Graph resource string.

    Handles formats like:
      Users('william@example.com')/Messages('AAMk...')
      Users/william@example.com/Messages/AAMk...
      users('william@example.com')/mailFolders('inbox')/messages
    """
    # Pattern 1: Users('email@domain')
    m = re.search(r"[Uu]sers\(['\"]([^'\"]+)['\"]\)", resource)
    if m:
        return m.group(1).lower()
    # Pattern 2: Users/email@domain/
    m = re.search(r"[Uu]sers/([^/]+@[^/]+)", resource)
    if m:
        return m.group(1).lower()
    return None


async def list_webhook_subscriptions(request: Request) -> JSONResponse:
    """GET /api/email/webhooks — List all active webhook subscriptions."""
    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT w.*, a.email_address
        FROM chat.email_webhooks w
        JOIN chat.email_accounts a ON a.id = w.account_id
        WHERE w.is_active = true
        ORDER BY w.created_at DESC
    """)
    return _json({"subscriptions": [_ser(r) for r in rows]})


async def create_webhook_subscription(request: Request) -> JSONResponse:
    """POST /api/email/webhooks — Create Graph webhook subscription(s).

    Body: { "account_id": 1 }  — creates for that account
    Body: {}                   — creates for all active accounts
    """
    data = await _read_json(request)
    target_id = data.get("account_id")

    from chat_api import get_pool
    p = await get_pool()

    if target_id:
        accounts = await p.fetch(
            "SELECT * FROM chat.email_accounts WHERE id = $1 AND is_active", int(target_id)
        )
    else:
        accounts = await p.fetch("SELECT * FROM chat.email_accounts WHERE is_active ORDER BY id")

    if not accounts:
        return _json_error("No active accounts found")

    results = []
    from datetime import timedelta
    # Graph allows max 4230 minutes (≈2.94 days) for mail subscriptions
    expiration = datetime.now(timezone.utc) + timedelta(minutes=4200)
    expiration_str = expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

    token = await _get_graph_token()

    for acct in accounts:
        email = acct["email_address"]
        resource = f"users/{email}/messages"

        # Check if subscription already exists
        existing = await p.fetchval("""
            SELECT subscription_id FROM chat.email_webhooks
            WHERE account_id = $1 AND is_active AND expiration_at > now()
        """, acct["id"])

        if existing:
            results.append({"email": email, "status": "already_active", "subscription_id": existing})
            continue

        # Create Graph subscription
        payload = {
            "changeType": "created,updated",
            "notificationUrl": WEBHOOK_NOTIFICATION_URL,
            "resource": resource,
            "expirationDateTime": expiration_str,
            "clientState": _WEBHOOK_CLIENT_STATE,
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{GRAPH_BASE_URL}/subscriptions",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json=payload,
                )
                resp.raise_for_status()
                sub = resp.json()

            # Store in DB
            await p.execute("""
                INSERT INTO chat.email_webhooks (account_id, subscription_id, resource, change_type, expiration_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (subscription_id) DO UPDATE SET
                    expiration_at = EXCLUDED.expiration_at,
                    renewed_at = now(),
                    is_active = true
            """, acct["id"], sub["id"], resource, "created,updated", expiration)

            results.append({
                "email": email,
                "status": "created",
                "subscription_id": sub["id"],
                "expires": expiration_str,
            })
            log.info("[webhook] Created subscription %s for %s (expires %s)", sub["id"], email, expiration_str)

        except httpx.HTTPStatusError as e:
            err_body = e.response.text[:500]
            log.error("[webhook] Failed to create subscription for %s: %s", email, err_body)
            results.append({"email": email, "status": "error", "error": err_body})
        except Exception as e:
            log.error("[webhook] Failed to create subscription for %s: %s", email, str(e))
            results.append({"email": email, "status": "error", "error": str(e)})

    return _json({"subscriptions": results})


async def renew_webhook_subscriptions(request: Request) -> JSONResponse:
    """POST /api/email/webhooks/renew — Renew all expiring subscriptions.

    Graph mail subscriptions expire after max ~3 days.
    This extends them by another 4200 minutes.
    """
    from chat_api import get_pool
    p = await get_pool()

    # Find subscriptions expiring within 24 hours
    rows = await p.fetch("""
        SELECT w.*, a.email_address
        FROM chat.email_webhooks w
        JOIN chat.email_accounts a ON a.id = w.account_id
        WHERE w.is_active AND w.expiration_at < now() + interval '24 hours'
    """)

    if not rows:
        return _json({"renewed": 0, "message": "No subscriptions need renewal"})

    from datetime import timedelta
    new_expiration = datetime.now(timezone.utc) + timedelta(minutes=4200)
    new_expiration_str = new_expiration.strftime("%Y-%m-%dT%H:%M:%S.0000000Z")

    token = await _get_graph_token()
    results = []

    for row in rows:
        sub_id = row["subscription_id"]
        email = row["email_address"]

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.patch(
                    f"{GRAPH_BASE_URL}/subscriptions/{sub_id}",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"expirationDateTime": new_expiration_str},
                )
                resp.raise_for_status()

            await p.execute("""
                UPDATE chat.email_webhooks
                SET expiration_at = $1, renewed_at = now()
                WHERE subscription_id = $2
            """, new_expiration, sub_id)

            results.append({"subscription_id": sub_id, "email": email, "status": "renewed"})
            log.info("[webhook] Renewed %s for %s → %s", sub_id, email, new_expiration_str)

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Subscription gone — mark inactive, it will be recreated
                await p.execute(
                    "UPDATE chat.email_webhooks SET is_active = false WHERE subscription_id = $1", sub_id
                )
                results.append({"subscription_id": sub_id, "email": email, "status": "expired_removed"})
                log.warning("[webhook] Subscription %s expired/gone — marked inactive", sub_id)
            else:
                results.append({"subscription_id": sub_id, "email": email, "status": "error", "error": e.response.text[:300]})
        except Exception as e:
            results.append({"subscription_id": sub_id, "email": email, "status": "error", "error": str(e)})

    return _json({"renewed": len([r for r in results if r["status"] == "renewed"]), "results": results})


async def delete_webhook_subscription(request: Request) -> JSONResponse:
    """DELETE /api/email/webhooks/{subscription_id} — Delete a subscription."""
    sub_id = request.path_params["subscription_id"]

    from chat_api import get_pool
    p = await get_pool()

    token = await _get_graph_token()

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.delete(
                f"{GRAPH_BASE_URL}/subscriptions/{sub_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            # 204 = success, 404 = already gone — both fine
            if resp.status_code not in (204, 404):
                resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        log.warning("[webhook] Graph delete failed for %s: %s", sub_id, e.response.text[:200])

    await p.execute(
        "UPDATE chat.email_webhooks SET is_active = false WHERE subscription_id = $1", sub_id
    )
    return _json({"deleted": True, "subscription_id": sub_id})


# ══════════════════════════════════════════════════════════════
# ACTION REQUIRED SUMMARY
# ══════════════════════════════════════════════════════════════

async def action_required(request: Request) -> JSONResponse:
    """GET /api/email/action-required — Emails flagged by AI as needing action."""
    from chat_api import get_pool
    p = await get_pool()

    rows = await p.fetch("""
        SELECT em.id, em.subject, em.sender_email, em.sender_name,
               em.received_at, em.web_link, em.direction,
               aic.urgency, aic.category, aic.case_number,
               aic.action_summary, aic.deadlines,
               a.email_address AS mailbox
        FROM chat.email_messages em
        JOIN chat.email_accounts a ON a.id = em.account_id
        JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
        WHERE aic.requires_action = true
        ORDER BY 
            CASE aic.urgency 
                WHEN 'urgent' THEN 1 WHEN 'high' THEN 2 
                WHEN 'normal' THEN 3 ELSE 4 END,
            em.received_at DESC
        LIMIT 50
    """)

    return _json({
        "emails": [_ser(r) for r in rows],
        "count": len(rows),
    })


# ══════════════════════════════════════════════════════════════
# ROUTE DEFINITIONS (imported by chat_api.py)
# ══════════════════════════════════════════════════════════════

def get_email_routes():
    """Return Starlette Route objects for the email API."""
    from starlette.routing import Route

    return [
        # Accounts
        Route("/api/email/accounts", list_email_accounts, methods=["GET"]),
        Route("/api/email/accounts", register_email_account, methods=["POST"]),

        # Messages
        Route("/api/email/messages", list_emails, methods=["GET"]),
        Route("/api/email/messages/{message_id}", get_email, methods=["GET"]),
        Route("/api/email/messages/{message_id}/reply", reply_to_email, methods=["POST"]),

        # Threads
        Route("/api/email/threads/{conversation_id}", get_email_thread, methods=["GET"]),

        # Search
        Route("/api/email/search", search_emails, methods=["GET"]),
        Route("/api/email/semantic-search", semantic_search_emails, methods=["GET"]),

        # AI enrichment (for app overlay)
        Route("/api/email/enrich", enrich_emails, methods=["POST"]),

        # Priority indexing
        Route("/api/email/priority-index", priority_index, methods=["POST"]),

        # Send
        Route("/api/email/send", send_email, methods=["POST"]),

        # Space linking
        Route("/api/email/spaces/{space_id}/links", list_space_email_links, methods=["GET"]),
        Route("/api/email/spaces/{space_id}/links", link_email_to_space, methods=["POST"]),
        Route("/api/email/spaces/{space_id}/messages", list_space_emails, methods=["GET"]),

        # Sync
        Route("/api/email/sync", get_sync_status, methods=["GET"]),
        Route("/api/email/sync", trigger_sync, methods=["POST"]),

        # Webhooks (Graph change notifications)
        Route("/api/email/webhooks/notifications", webhook_validation_or_notification, methods=["POST"]),
        Route("/api/email/webhooks", list_webhook_subscriptions, methods=["GET"]),
        Route("/api/email/webhooks", create_webhook_subscription, methods=["POST"]),
        Route("/api/email/webhooks/renew", renew_webhook_subscriptions, methods=["POST"]),
        Route("/api/email/webhooks/{subscription_id}", delete_webhook_subscription, methods=["DELETE"]),

        # Action required
        Route("/api/email/action-required", action_required, methods=["GET"]),
    ]
