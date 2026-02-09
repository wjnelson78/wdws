#!/usr/bin/env python3
"""
Athena Cognitive Platform MCP Server v2.0 — PostgreSQL-native with OAuth 2.0.

Single MCP server consolidating all domains:
  • Legal cases (court filings, emails, attachments)
  • Medical records (clinical notes, lab results, imaging)
  • Paperless-ngx documents
  • Write operations (notes, tags, documents, relationships)

OAuth 2.0 with per-client credentials:
  • Claude Desktop  → client_id: claude-desktop
  • ChatGPT         → client_id: chatgpt-connector

Connects directly to PostgreSQL + pgvector on localhost.
Supports both stdio (Claude Desktop) and SSE/Streamable HTTP transport.

Run:
  stdio:  /opt/wdws/venv/bin/python3 /opt/wdws/mcp-server/mcp_server.py
  http:   /opt/wdws/venv/bin/python3 /opt/wdws/mcp-server/mcp_server.py --http --port 9200
"""

import os
import sys
import json
import asyncio
import hashlib
import hmac
import logging
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, timezone
from typing import Optional, Any, Generic, TypeVar

import asyncpg
import bcrypt
import httpx
import jwt
from mcp.server.fastmcp import FastMCP
from mcp.server.auth.provider import (
    OAuthAuthorizationServerProvider,
    AuthorizationParams,
)
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions, RevocationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("athena-mcp")

# ── Config ───────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")
EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_DIMS = 3072

# JWT signing key (generated on first run, persisted in DB)
JWT_SECRET = os.getenv("JWT_SECRET", "wdws-mcp-jwt-" + secrets.token_hex(16))
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_TTL = 3600 * 8    # 8 hours
REFRESH_TOKEN_TTL = 3600 * 24 * 30  # 30 days

# ── Pre-registered OAuth Clients ─────────────────────────────
# These get inserted into ops.oauth_clients on startup if missing.
# Secrets are bcrypt-hashed in the DB; plain values here are for
# initial registration only.
PRECONFIGURED_CLIENTS = [
    {
        "client_id": "claude-desktop",
        "client_name": "Claude Desktop",
        "client_secret": "wdws-claude-" + hashlib.sha256(b"claude-desktop-wdws-2026").hexdigest()[:32],
        "scopes": ["read", "write", "search", "admin"],
    },
    {
        "client_id": "chatgpt-connector",
        "client_name": "ChatGPT",
        "client_secret": "wdws-chatgpt-" + hashlib.sha256(b"chatgpt-connector-wdws-2026").hexdigest()[:32],
        "scopes": ["read", "write", "search"],
    },
]

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://172.16.32.207:9200")

# ── Connection Pool ──────────────────────────────────────────
pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=10, command_timeout=30
        )
        log.info("PostgreSQL pool created")
    return pool


async def close_pool():
    global pool
    if pool:
        await pool.close()
        pool = None


# ── JSON helpers ─────────────────────────────────────────────
def _ser(val: Any) -> Any:
    """Serialize PostgreSQL types to JSON-safe values."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, dict):
        return val
    return str(val)


def _row_dict(row, keys=None) -> dict:
    """Convert asyncpg Record to dict."""
    if keys:
        return {k: _ser(row[k]) for k in keys if k in row.keys()}
    return {k: _ser(v) for k, v in dict(row).items()}


# ── Embedding helper ─────────────────────────────────────────
async def _embed_query(text: str) -> list[float]:
    """Get embedding vector for a search query via OpenAI API."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={"model": EMBEDDING_MODEL, "input": text, "dimensions": EMBEDDING_DIMS},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


# ── Access logging ───────────────────────────────────────────
async def _log_access(tool_name: str, query: str, result_count: int, client_id: str = "unknown"):
    """Record tool usage for audit trail."""
    try:
        p = await get_pool()
        await p.execute(
            """INSERT INTO ops.search_analytics (query, tool_name, result_count, searched_at)
               VALUES ($1, $2, $3, now())""",
            (query or "")[:500], tool_name, result_count,
        )
    except Exception:
        pass  # Don't let logging failures break tools


# ══════════════════════════════════════════════════════════════
#  OAUTH 2.0 PROVIDER (PostgreSQL-backed)
# ══════════════════════════════════════════════════════════════
class WDWSOAuthProvider(OAuthAuthorizationServerProvider[str, str, str]):
    """
    OAuth 2.0 Authorization Server backed by PostgreSQL.
    Supports authorization_code flow with PKCE for MCP clients.
    Each client (Claude Desktop, ChatGPT) gets unique client_id + secret.
    """

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        p = await get_pool()
        row = await p.fetchrow(
            "SELECT * FROM ops.oauth_clients WHERE client_id = $1 AND is_active = true",
            client_id,
        )
        if not row:
            return None
        return OAuthClientInformationFull(
            client_id=row["client_id"],
            client_secret=row["client_secret_hash"],  # hashed
            client_name=row["client_name"],
            redirect_uris=row["redirect_uris"] or [],
            scope=" ".join(row["scopes"] or []),
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            token_endpoint_auth_method="client_secret_post",
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        """Dynamic client registration (we pre-register, but support it)."""
        p = await get_pool()
        secret_hash = bcrypt.hashpw(
            (client_info.client_secret or "").encode(), bcrypt.gensalt()
        ).decode()
        await p.execute("""
            INSERT INTO ops.oauth_clients (client_id, client_name, client_secret_hash,
                                           redirect_uris, scopes, is_active, created_at)
            VALUES ($1, $2, $3, $4, $5, true, now())
            ON CONFLICT (client_id) DO NOTHING
        """, client_info.client_id, client_info.client_name or client_info.client_id,
            secret_hash, list(client_info.redirect_uris) if client_info.redirect_uris else [],
            list(client_info.scope.split()) if client_info.scope else [])
        log.info("Registered OAuth client: %s", client_info.client_id)

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        """Issue authorization code."""
        code = secrets.token_urlsafe(48)
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.oauth_tokens (token, token_type, client_id, scopes, redirect_uri,
                                          code_challenge, expires_at, created_at)
            VALUES ($1, 'authorization_code', $2, $3, $4, $5, now() + interval '10 minutes', now())
        """, code, client.client_id, params.scopes or [],
            str(params.redirect_uri), params.code_challenge)
        log.info("Issued auth code for client %s", client.client_id)
        return code

    async def load_authorization_code(self, client: OAuthClientInformationFull, authorization_code: str) -> str | None:
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT token FROM ops.oauth_tokens
            WHERE token = $1 AND token_type = 'authorization_code'
              AND client_id = $2 AND expires_at > now()
        """, authorization_code, client.client_id)
        return row["token"] if row else None

    async def exchange_authorization_code(self, client: OAuthClientInformationFull, authorization_code: str) -> OAuthToken:
        """Exchange auth code for access + refresh tokens."""
        p = await get_pool()
        # Delete the used auth code
        await p.execute(
            "DELETE FROM ops.oauth_tokens WHERE token = $1 AND token_type = 'authorization_code'",
            authorization_code,
        )
        return await self._issue_tokens(client)

    async def load_access_token(self, token: str) -> str | None:
        """Validate an access token (JWT-based)."""
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            if payload.get("exp", 0) < time.time():
                return None
            return token
        except jwt.InvalidTokenError:
            return None

    async def load_refresh_token(self, client: OAuthClientInformationFull, refresh_token: str) -> str | None:
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT token FROM ops.oauth_tokens
            WHERE token = $1 AND token_type = 'refresh_token'
              AND client_id = $2 AND expires_at > now()
        """, refresh_token, client.client_id)
        return row["token"] if row else None

    async def exchange_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str, scopes: list[str]
    ) -> OAuthToken:
        """Exchange refresh token for new access + refresh tokens."""
        p = await get_pool()
        await p.execute(
            "DELETE FROM ops.oauth_tokens WHERE token = $1 AND token_type = 'refresh_token'",
            refresh_token,
        )
        return await self._issue_tokens(client, scopes)

    async def revoke_token(self, token: str) -> None:
        p = await get_pool()
        await p.execute("DELETE FROM ops.oauth_tokens WHERE token = $1", token)

    async def _issue_tokens(self, client: OAuthClientInformationFull, scopes: list[str] | None = None) -> OAuthToken:
        """Create JWT access token + opaque refresh token."""
        now = time.time()
        resolved_scopes = scopes or (client.scope.split() if client.scope else ["read", "search"])
        access_payload = {
            "sub": client.client_id,
            "client_name": client.client_name,
            "scopes": resolved_scopes,
            "iat": int(now),
            "exp": int(now + ACCESS_TOKEN_TTL),
            "jti": str(uuid.uuid4()),
        }
        access_token = jwt.encode(access_payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

        refresh_token = secrets.token_urlsafe(64)
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.oauth_tokens (token, token_type, client_id, scopes, expires_at, created_at)
            VALUES ($1, 'refresh_token', $2, $3, now() + interval '30 days', now())
        """, refresh_token, client.client_id, resolved_scopes)

        # Update last_used
        await p.execute(
            "UPDATE ops.oauth_clients SET last_used = now() WHERE client_id = $1",
            client.client_id,
        )
        log.info("Issued tokens for client %s (scopes: %s)", client.client_id, resolved_scopes)

        return OAuthToken(
            access_token=access_token,
            token_type="Bearer",
            expires_in=ACCESS_TOKEN_TTL,
            refresh_token=refresh_token,
            scope=" ".join(resolved_scopes),
        )


# ══════════════════════════════════════════════════════════════
#  DB SETUP: Ensure OAuth tables exist + pre-register clients
# ══════════════════════════════════════════════════════════════
async def _ensure_oauth_schema():
    """Create token storage table and pre-register clients on startup."""
    p = await get_pool()

    # Tokens table (auth codes, refresh tokens)
    await p.execute("""
        CREATE TABLE IF NOT EXISTS ops.oauth_tokens (
            token       TEXT PRIMARY KEY,
            token_type  TEXT NOT NULL,
            client_id   TEXT NOT NULL,
            scopes      TEXT[] DEFAULT '{}',
            redirect_uri TEXT,
            code_challenge TEXT,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now()
        )
    """)

    # Clean expired tokens
    await p.execute("DELETE FROM ops.oauth_tokens WHERE expires_at < now()")

    # Pre-register clients
    for client_cfg in PRECONFIGURED_CLIENTS:
        existing = await p.fetchrow(
            "SELECT client_id FROM ops.oauth_clients WHERE client_id = $1",
            client_cfg["client_id"],
        )
        if not existing:
            secret_hash = bcrypt.hashpw(
                client_cfg["client_secret"].encode(), bcrypt.gensalt()
            ).decode()
            await p.execute("""
                INSERT INTO ops.oauth_clients (client_id, client_name, client_secret_hash, scopes, is_active, created_at)
                VALUES ($1, $2, $3, $4, true, now())
            """, client_cfg["client_id"], client_cfg["client_name"], secret_hash, client_cfg["scopes"])
            log.info("Pre-registered OAuth client: %s (secret: %s...)",
                     client_cfg["client_id"], client_cfg["client_secret"][:20])
        else:
            log.info("OAuth client already registered: %s", client_cfg["client_id"])

    log.info("OAuth schema ready — %d clients registered", len(PRECONFIGURED_CLIENTS))


# ══════════════════════════════════════════════════════════════
#  MCP SERVER INSTANCE
# ══════════════════════════════════════════════════════════════
# ══════════════════════════════════════════════════════════════
#  LIFESPAN: Init DB pool + OAuth on startup, cleanup on shutdown
# ══════════════════════════════════════════════════════════════
@asynccontextmanager
async def app_lifespan(app):
    """Initialize database pool and OAuth schema."""
    await get_pool()
    await _ensure_oauth_schema()
    log.info("Athena MCP Server v2.0 ready")
    yield {}
    await close_pool()
    log.info("Athena MCP Server shutdown")


oauth_provider = WDWSOAuthProvider()

auth_settings = AuthSettings(
    issuer_url=MCP_SERVER_URL,
    resource_server_url=MCP_SERVER_URL,
    service_documentation_url=None,
    client_registration_options=ClientRegistrationOptions(
        enabled=True,
        valid_scopes=["read", "write", "search", "admin"],
        default_scopes=["read", "search"],
    ),
    revocation_options=RevocationOptions(enabled=True),
    required_scopes=["read"],
)

mcp = FastMCP(
    "Athena Cognitive Platform",
    instructions=(
        "William Nelson's unified document intelligence server. "
        "Provides full CRUD access to legal cases, court filings, emails, "
        "medical records, and paperless documents via PostgreSQL + pgvector. "
        "Supports semantic search, full-text search, RAG queries, document "
        "creation, tagging, annotation, and file management."
    ),
    auth_server_provider=oauth_provider,
    auth=auth_settings,
    lifespan=app_lifespan,
    host="0.0.0.0",
    port=9200,
)


# ╔══════════════════════════════════════════════════════════════╗
# ║                    SEARCH TOOLS                              ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def semantic_search(
    query: str,
    domain: Optional[str] = None,
    document_type: Optional[str] = None,
    limit: int = 8,
) -> str:
    """Search all documents using AI semantic similarity (pgvector).

    Finds conceptually related content even without exact keyword matches.
    Use this for questions, concepts, or when you're unsure of exact wording.

    Args:
        query: Natural language search query
        domain: Filter by domain — "legal", "medical", or omit for all
        document_type: Filter by type — "email", "court_filing", "email_attachment", "medical_export"
        limit: Max results (1-20, default 8)
    """
    limit = min(max(limit, 1), 20)
    try:
        embedding = await _embed_query(query)
        vec_literal = "[" + ",".join(str(x) for x in embedding) + "]"
        p = await get_pool()

        conditions = ["c.embedding IS NOT NULL"]
        params: list = [vec_literal]
        idx = 2

        if domain:
            conditions.append(f"d.domain = ${idx}")
            params.append(domain)
            idx += 1
        if document_type:
            conditions.append(f"d.document_type = ${idx}")
            params.append(document_type)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT c.id AS chunk_id, c.content, c.chunk_index,
                   d.id AS doc_id, d.title, d.filename, d.domain, d.document_type,
                   c.embedding <=> $1::halfvec({EMBEDDING_DIMS}) AS distance
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE {where}
            ORDER BY c.embedding <=> $1::halfvec({EMBEDDING_DIMS})
            LIMIT ${idx}
        """, *params)

        results = []
        for r in rows:
            sim = round(1 - float(r["distance"]), 4)
            results.append({
                "document_id": str(r["doc_id"]),
                "chunk_id": r["chunk_id"],
                "title": r["title"] or r["filename"],
                "domain": r["domain"],
                "type": r["document_type"],
                "similarity": sim,
                "excerpt": r["content"][:800],
            })

        await _log_access("semantic_search", query, len(results))
        return json.dumps({"results": results, "count": len(results), "query": query})
    except Exception as e:
        log.error("semantic_search error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def fulltext_search(
    query: str,
    domain: Optional[str] = None,
    document_type: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search documents using PostgreSQL full-text search (exact keywords).

    Best for finding specific names, case numbers, dates, or known phrases.

    Args:
        query: Search terms (supports AND/OR/NOT, e.g. "Nelson AND Starbucks")
        domain: Filter by domain — "legal", "medical", or omit for all
        document_type: Filter by type
        limit: Max results (1-30, default 10)
    """
    limit = min(max(limit, 1), 30)
    p = await get_pool()
    try:
        conditions = ["c.content_tsv @@ websearch_to_tsquery('english', $1)"]
        params: list = [query]
        idx = 2

        if domain:
            conditions.append(f"d.domain = ${idx}")
            params.append(domain)
            idx += 1
        if document_type:
            conditions.append(f"d.document_type = ${idx}")
            params.append(document_type)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT c.id AS chunk_id, c.content, d.id AS doc_id,
                   d.title, d.filename, d.domain, d.document_type,
                   ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', $1))::FLOAT AS rank
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE {where}
            ORDER BY rank DESC LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["doc_id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "rank": round(r["rank"], 4),
            "excerpt": r["content"][:800],
        } for r in rows]

        await _log_access("fulltext_search", query, len(results))
        return json.dumps({"results": results, "count": len(results), "query": query})
    except Exception as e:
        log.error("fulltext_search error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def search_emails(
    query: str = "",
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    mailbox: Optional[str] = None,
    case_number: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    has_attachments: Optional[bool] = None,
    direction: Optional[str] = None,
    limit: int = 15,
) -> str:
    """Search the email archive with rich filtering.

    Args:
        query: Full-text search within email content
        sender: Filter by sender email/name (partial match)
        recipient: Filter by recipient email/name (partial match)
        mailbox: Filter by mailbox name (e.g. "william@seattleseahawks.me")
        case_number: Filter to emails about a specific case
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        has_attachments: Filter to emails with/without attachments
        direction: Filter by "inbound" or "outbound"
        limit: Max results (1-50, default 15)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        conditions = ["d.document_type IN ('email','eml')"]
        params: list = []
        idx = 1

        if query:
            conditions.append(f"d.full_content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query)
            idx += 1
        if sender:
            conditions.append(f"em.sender ILIKE ${idx}")
            params.append(f"%{sender}%")
            idx += 1
        if recipient:
            conditions.append(f"(em.recipients ILIKE ${idx} OR em.cc ILIKE ${idx})")
            params.append(f"%{recipient}%")
            idx += 1
        if mailbox:
            conditions.append(f"em.mailbox ILIKE ${idx}")
            params.append(f"%{mailbox}%")
            idx += 1
        if case_number:
            conditions.append(f"(d.title ILIKE ${idx} OR d.full_content ILIKE ${idx})")
            params.append(f"%{case_number}%")
            idx += 1
        if date_from:
            conditions.append(f"em.date_sent >= ${idx}::timestamptz")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"em.date_sent <= ${idx}::timestamptz")
            params.append(date_to + "T23:59:59")
            idx += 1
        if has_attachments is not None:
            conditions.append(f"em.has_attachments = ${idx}")
            params.append(has_attachments)
            idx += 1
        if direction:
            conditions.append(f"em.direction = ${idx}")
            params.append(direction)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, em.sender, em.recipients, em.cc,
                   em.date_sent, em.direction, em.mailbox, em.subject,
                   em.has_attachments, em.in_reply_to, em.thread_id,
                   LEFT(d.full_content, 500) AS excerpt
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE {where}
            ORDER BY em.date_sent DESC NULLS LAST
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "subject": r["subject"] or r["title"] or r["filename"],
            "from": r["sender"],
            "to": r["recipients"],
            "cc": r["cc"],
            "date": _ser(r["date_sent"]),
            "direction": r["direction"],
            "mailbox": r["mailbox"],
            "has_attachments": r["has_attachments"],
            "thread_id": r["thread_id"],
            "excerpt": r["excerpt"],
        } for r in rows]

        await _log_access("search_emails", query or sender or "browse", len(results))
        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_emails error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                  DOCUMENT RETRIEVAL TOOLS                    ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def get_document(document_id: str) -> str:
    """Retrieve full content of a document by ID.

    Returns the complete document text plus metadata. For emails,
    also returns sender, recipients, date, and attachments info.

    Args:
        document_id: UUID of the document
    """
    p = await get_pool()
    try:
        doc = await p.fetchrow("""
            SELECT d.*, em.sender, em.recipients, em.cc,
                   em.date_sent, em.direction, em.mailbox, em.subject,
                   em.has_attachments, em.attachment_text, em.in_reply_to
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE d.id = $1::uuid
        """, document_id)

        if not doc:
            return json.dumps({"error": f"Document {document_id} not found"})

        content = doc["full_content"] or ""
        if len(content) > 12000:
            content = content[:12000] + "\n\n... [truncated — use get_document_chunk for remaining content]"

        result = {
            "id": str(doc["id"]),
            "title": doc["title"],
            "filename": doc["filename"],
            "domain": doc["domain"],
            "document_type": doc["document_type"],
            "source_path": doc["source_path"],
            "content": content,
            "total_chunks": doc["total_chunks"],
            "metadata": doc["metadata"],
            "created_at": _ser(doc["created_at"]),
            "updated_at": _ser(doc["updated_at"]),
        }

        if doc["sender"]:
            result["email"] = {
                "subject": doc["subject"],
                "from": doc["sender"],
                "to": doc["recipients"],
                "cc": doc["cc"],
                "date": _ser(doc["date_sent"]),
                "direction": doc["direction"],
                "mailbox": doc["mailbox"],
                "has_attachments": doc["has_attachments"],
                "attachment_text": (doc["attachment_text"] or "")[:2000] if doc["attachment_text"] else None,
            }

        med = await p.fetchrow(
            "SELECT * FROM medical.record_metadata WHERE document_id = $1::uuid",
            document_id,
        )
        if med:
            result["medical"] = {
                "record_type": med["record_type"],
                "date_of_service": _ser(med["date_of_service"]),
                "facility": med["facility"],
                "section_title": med["section_title"],
            }

        tags = await p.fetch("""
            SELECT t.name, t.category FROM core.tags t
            JOIN core.document_tags dt ON t.id = dt.tag_id
            WHERE dt.document_id = $1::uuid
        """, document_id)
        if tags:
            result["tags"] = [{"name": t["name"], "category": t["category"]} for t in tags]

        await _log_access("get_document", document_id, 1)
        return json.dumps(result)
    except Exception as e:
        log.error("get_document error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_document_chunk(document_id: str, chunk_index: int = 0) -> str:
    """Retrieve a specific chunk of a document (for large documents).

    Args:
        document_id: UUID of the document
        chunk_index: 0-based chunk index (default 0)
    """
    p = await get_pool()
    try:
        row = await p.fetchrow("""
            SELECT c.content, c.chunk_index, c.metadata,
                   d.title, d.total_chunks
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.document_id = $1::uuid AND c.chunk_index = $2
        """, document_id, chunk_index)

        if not row:
            return json.dumps({"error": f"Chunk {chunk_index} not found for document {document_id}"})

        return json.dumps({
            "document_id": document_id,
            "title": row["title"],
            "chunk_index": row["chunk_index"],
            "total_chunks": row["total_chunks"],
            "content": row["content"],
        })
    except Exception as e:
        log.error("get_document_chunk error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_email_attachments(email_document_id: str) -> str:
    """List all attachments for a specific email.

    Args:
        email_document_id: UUID of the email document
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT ea.id, ea.filename, ea.content_type, ea.file_size,
                   ea.extraction_method, ea.is_processed, ea.child_doc_id,
                   LEFT(ea.extracted_text, 500) AS text_preview
            FROM legal.email_attachments ea
            WHERE ea.email_doc_id = $1::uuid
            ORDER BY ea.filename
        """, email_document_id)

        attachments = [{
            "attachment_id": str(r["id"]),
            "filename": r["filename"],
            "content_type": r["content_type"],
            "file_size": r["file_size"],
            "extraction_method": r["extraction_method"],
            "is_processed": r["is_processed"],
            "child_document_id": str(r["child_doc_id"]) if r["child_doc_id"] else None,
            "text_preview": r["text_preview"],
        } for r in rows]

        return json.dumps({"email_document_id": email_document_id, "attachments": attachments, "count": len(attachments)})
    except Exception as e:
        log.error("list_email_attachments error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                     LEGAL CASE TOOLS                         ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def lookup_case(case_number: str) -> str:
    """Look up a legal case by case number, returning parties and recent documents.

    Args:
        case_number: Full or partial case number (e.g. "26-2-00762-31" or "Nelson v Starbucks")
    """
    p = await get_pool()
    try:
        case = await p.fetchrow("""
            SELECT c.*, COUNT(cd.document_id) AS doc_count
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            WHERE c.case_number ILIKE $1 OR c.case_title ILIKE $1
            GROUP BY c.id
        """, f"%{case_number.strip()}%")

        if not case:
            return json.dumps({"error": f"Case '{case_number}' not found"})

        parties = await p.fetch("""
            SELECT p.name, cp.role, p.party_type
            FROM legal.case_parties cp
            JOIN legal.parties p ON cp.party_id = p.id
            WHERE cp.case_id = $1
        """, case["id"])

        docs = await p.fetch("""
            SELECT d.id, d.title, d.filename, d.document_type, d.created_at
            FROM legal.case_documents cd
            JOIN core.documents d ON cd.document_id = d.id
            WHERE cd.case_id = $1
            ORDER BY d.created_at DESC LIMIT 25
        """, case["id"])

        result = {
            "case_number": case["case_number"],
            "court": case["court"],
            "title": case["case_title"],
            "type": case["case_type"],
            "status": case["status"],
            "jurisdiction": case["jurisdiction"],
            "date_filed": _ser(case["date_filed"]),
            "date_closed": _ser(case["date_closed"]),
            "doc_count": case["doc_count"],
            "parties": [{"name": p["name"], "role": p["role"], "type": p["party_type"]} for p in parties],
            "recent_documents": [{
                "id": str(d["id"]),
                "title": d["title"] or d["filename"],
                "type": d["document_type"],
                "date": _ser(d["created_at"]),
            } for d in docs],
        }

        await _log_access("lookup_case", case_number, 1)
        return json.dumps(result)
    except Exception as e:
        log.error("lookup_case error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_cases(status: Optional[str] = None) -> str:
    """List all legal cases with document counts.

    Args:
        status: Filter by status — "active", "closed", or omit for all
    """
    p = await get_pool()
    try:
        if status and status != "all":
            rows = await p.fetch("""
                SELECT c.case_number, c.case_title, c.court, c.status,
                       c.jurisdiction, c.date_filed, c.case_type,
                       COUNT(cd.document_id) AS doc_count
                FROM legal.cases c
                LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
                WHERE c.status = $1
                GROUP BY c.id ORDER BY c.date_filed DESC NULLS LAST
            """, status)
        else:
            rows = await p.fetch("""
                SELECT c.case_number, c.case_title, c.court, c.status,
                       c.jurisdiction, c.date_filed, c.case_type,
                       COUNT(cd.document_id) AS doc_count
                FROM legal.cases c
                LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
                GROUP BY c.id ORDER BY c.date_filed DESC NULLS LAST
            """)

        cases = [{
            "case_number": r["case_number"],
            "title": r["case_title"],
            "court": r["court"],
            "status": r["status"],
            "jurisdiction": r["jurisdiction"],
            "filed": _ser(r["date_filed"]),
            "type": r["case_type"],
            "documents": r["doc_count"],
        } for r in rows]

        await _log_access("list_cases", status or "all", len(cases))
        return json.dumps({"cases": cases, "count": len(cases)})
    except Exception as e:
        log.error("list_cases error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def search_court_filings(
    query: str = "",
    case_number: Optional[str] = None,
    filing_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search court filings (motions, orders, declarations, etc.).

    Args:
        query: Full-text search query
        case_number: Filter to specific case
        filing_type: Filter by type (e.g. "motion", "order", "declaration")
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        limit: Max results (1-30, default 10)
    """
    limit = min(max(limit, 1), 30)
    p = await get_pool()
    try:
        conditions = ["d.document_type = 'court_filing'"]
        params: list = []
        idx = 1

        if query:
            conditions.append(f"d.full_content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query)
            idx += 1
        if case_number:
            conditions.append(f"""d.id IN (
                SELECT cd.document_id FROM legal.case_documents cd
                JOIN legal.cases c ON cd.case_id = c.id
                WHERE c.case_number ILIKE ${idx}
            )""")
            params.append(f"%{case_number}%")
            idx += 1
        if filing_type:
            conditions.append(f"fm.filing_type ILIKE ${idx}")
            params.append(f"%{filing_type}%")
            idx += 1
        if date_from:
            conditions.append(f"COALESCE(fm.date_filed, d.created_at::date) >= ${idx}::date")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"COALESCE(fm.date_filed, d.created_at::date) <= ${idx}::date")
            params.append(date_to)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, d.created_at,
                   fm.filing_type, fm.filed_by, fm.date_filed, fm.docket_number,
                   LEFT(d.full_content, 500) AS excerpt
            FROM core.documents d
            LEFT JOIN legal.filing_metadata fm ON d.id = fm.document_id
            WHERE {where}
            ORDER BY COALESCE(fm.date_filed, d.created_at::date) DESC
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "filing_type": r["filing_type"],
            "filed_by": r["filed_by"],
            "date_filed": _ser(r["date_filed"]),
            "docket_number": r["docket_number"],
            "excerpt": r["excerpt"],
        } for r in rows]

        await _log_access("search_court_filings", query or case_number or "browse", len(results))
        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_court_filings error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def search_by_case(case_number: str, document_type: Optional[str] = None, limit: int = 25) -> str:
    """Find all documents related to a specific case number.

    Args:
        case_number: Full or partial case number
        document_type: Filter to "email", "court_filing", "email_attachment", or omit for all
        limit: Max results (default 25)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        conditions = ["(c.case_number ILIKE $1 OR d.title ILIKE $1 OR d.source_path ILIKE $1)"]
        params: list = [f"%{case_number.strip()}%"]
        idx = 2

        if document_type:
            conditions.append(f"d.document_type = ${idx}")
            params.append(document_type)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT DISTINCT d.id, d.title, d.filename, d.domain,
                   d.document_type, d.created_at,
                   em.sender, em.date_sent, em.direction, em.subject
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN legal.case_documents cd ON d.id = cd.document_id
            LEFT JOIN legal.cases c ON cd.case_id = c.id
            WHERE {where}
            ORDER BY d.created_at DESC NULLS LAST
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "date": _ser(r["created_at"]),
            "sender": r["sender"],
            "subject": r["subject"],
            "email_date": _ser(r["date_sent"]),
            "direction": r["direction"],
        } for r in rows]

        await _log_access("search_by_case", case_number, len(results))
        return json.dumps({"results": results, "count": len(results), "case_number": case_number})
    except Exception as e:
        log.error("search_by_case error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                    MEDICAL RECORD TOOLS                      ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def search_medical_records(
    query: str,
    record_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    facility: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search medical records including clinical notes, lab results, and imaging.

    Args:
        query: Search terms (symptoms, diagnoses, medications, etc.)
        record_type: Filter by type (e.g. "lab_result", "clinical_note", "imaging")
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        facility: Filter by facility name
        limit: Max results (1-20, default 10)
    """
    limit = min(max(limit, 1), 20)
    p = await get_pool()
    try:
        conditions = ["d.domain = 'medical'"]
        params: list = []
        idx = 1

        if query:
            conditions.append(f"c.content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query)
            idx += 1
        if record_type:
            conditions.append(f"rm.record_type ILIKE ${idx}")
            params.append(f"%{record_type}%")
            idx += 1
        if date_from:
            conditions.append(f"rm.date_of_service >= ${idx}::date")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"rm.date_of_service <= ${idx}::date")
            params.append(date_to)
            idx += 1
        if facility:
            conditions.append(f"rm.facility ILIKE ${idx}")
            params.append(f"%{facility}%")
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT DISTINCT ON (d.id)
                   d.id, d.title, d.filename, d.created_at,
                   rm.record_type, rm.date_of_service, rm.facility, rm.section_title,
                   LEFT(c.content, 600) AS excerpt
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            LEFT JOIN medical.record_metadata rm ON d.id = rm.document_id
            WHERE {where}
            ORDER BY d.id, rm.date_of_service DESC NULLS LAST
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "record_type": r["record_type"],
            "date_of_service": _ser(r["date_of_service"]),
            "facility": r["facility"],
            "section": r["section_title"],
            "excerpt": r["excerpt"],
        } for r in rows]

        await _log_access("search_medical_records", query, len(results))
        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_medical_records error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                      RAG / AI TOOLS                          ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def rag_query(question: str, domain: Optional[str] = None, n_results: int = 6) -> str:
    """Answer a question using RAG — searches documents then synthesizes an answer with AI.

    Use this when the user wants an analyzed answer, not just raw search results.

    Args:
        question: The question to answer
        domain: Restrict to "legal" or "medical" or omit for all
        n_results: Number of source chunks to retrieve (default 6)
    """
    n_results = min(max(n_results, 3), 12)
    try:
        embedding = await _embed_query(question)
        vec_literal = "[" + ",".join(str(x) for x in embedding) + "]"
        p = await get_pool()

        if domain:
            rows = await p.fetch(f"""
                SELECT c.content, d.title, d.filename, d.domain, d.document_type,
                       c.embedding <=> $1::halfvec({EMBEDDING_DIMS}) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.embedding IS NOT NULL AND d.domain = $2
                ORDER BY c.embedding <=> $1::halfvec({EMBEDDING_DIMS})
                LIMIT $3
            """, vec_literal, domain, n_results)
        else:
            rows = await p.fetch(f"""
                SELECT c.content, d.title, d.filename, d.domain, d.document_type,
                       c.embedding <=> $1::halfvec({EMBEDDING_DIMS}) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.embedding IS NOT NULL
                ORDER BY c.embedding <=> $1::halfvec({EMBEDDING_DIMS})
                LIMIT $2
            """, vec_literal, n_results)

        if not rows:
            return json.dumps({"answer": "No relevant documents found.", "sources": []})

        context_parts = []
        sources = []
        for i, r in enumerate(rows, 1):
            label = r["title"] or r["filename"]
            context_parts.append(f"[Source {i}: {label} ({r['domain']}/{r['document_type']})]:\n{r['content']}")
            sources.append({
                "title": label,
                "domain": r["domain"],
                "type": r["document_type"],
                "similarity": round(1 - float(r["distance"]), 4),
            })

        context = "\n\n---\n\n".join(context_parts)

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={
                    "model": "gpt-4o",
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You are a research assistant for William Nelson. "
                                "Answer the question based ONLY on the provided source documents. "
                                "Cite sources by number. If the documents don't contain enough "
                                "information, say so clearly."
                            ),
                        },
                        {
                            "role": "user",
                            "content": f"Source Documents:\n\n{context}\n\n---\n\nQuestion: {question}",
                        },
                    ],
                    "temperature": 0.2,
                    "max_tokens": 1500,
                },
                timeout=30,
            )
            resp.raise_for_status()
            answer = resp.json()["choices"][0]["message"]["content"]

        await _log_access("rag_query", question, len(sources))
        return json.dumps({"answer": answer, "sources": sources, "question": question})
    except Exception as e:
        log.error("rag_query error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                     WRITE / CREATE TOOLS                     ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def create_note(
    title: str,
    content: str,
    domain: str = "legal",
    document_type: str = "note",
    case_number: Optional[str] = None,
    tags: Optional[list[str]] = None,
) -> str:
    """Create a new note/document in the database.

    Use this to write research notes, case summaries, meeting notes, etc.

    Args:
        title: Title of the note
        content: Full text content of the note
        domain: "legal" or "medical" (default "legal")
        document_type: Type — "note", "memo", "summary", "research" (default "note")
        case_number: Optionally associate with a case
        tags: Optional list of tag names to apply
    """
    p = await get_pool()
    try:
        doc_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        await p.execute("""
            INSERT INTO core.documents (id, domain, filename, document_type, title,
                                        content_hash, total_chunks, full_content, metadata, created_at, updated_at)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, 1, $7, $8::jsonb, now(), now())
        """, doc_id, domain, f"{title.replace(' ', '_')}.md", document_type, title,
            content_hash, content, json.dumps({"created_by": "mcp", "source": "user_created"}))

        chunk_id = f"note-{doc_id}-0"
        await p.execute("""
            INSERT INTO core.document_chunks (id, document_id, chunk_index, content, content_tsv)
            VALUES ($1, $2::uuid, 0, $3, to_tsvector('english', $3))
        """, chunk_id, doc_id, content)

        try:
            embedding = await _embed_query(content[:8000])
            vec_literal = "[" + ",".join(str(x) for x in embedding) + "]"
            await p.execute(f"""
                UPDATE core.document_chunks SET embedding = $1::halfvec({EMBEDDING_DIMS})
                WHERE id = $2
            """, vec_literal, chunk_id)
        except Exception as emb_err:
            log.warning("Failed to embed note: %s", emb_err)

        await p.execute("""
            UPDATE core.documents SET full_content_tsv = to_tsvector('english', $1) WHERE id = $2::uuid
        """, content, doc_id)

        if case_number:
            case = await p.fetchrow(
                "SELECT id FROM legal.cases WHERE case_number ILIKE $1",
                f"%{case_number}%",
            )
            if case:
                await p.execute(
                    "INSERT INTO legal.case_documents (case_id, document_id) VALUES ($1, $2::uuid) ON CONFLICT DO NOTHING",
                    case["id"], doc_id,
                )

        if tags:
            for tag_name in tags:
                tag = await p.fetchrow("SELECT id FROM core.tags WHERE name = $1", tag_name)
                if not tag:
                    tag = await p.fetchrow(
                        "INSERT INTO core.tags (name, category) VALUES ($1, 'user') RETURNING id",
                        tag_name,
                    )
                await p.execute(
                    "INSERT INTO core.document_tags (document_id, tag_id) VALUES ($1::uuid, $2) ON CONFLICT DO NOTHING",
                    doc_id, tag["id"],
                )

        await _log_access("create_note", title, 1)
        return json.dumps({"success": True, "document_id": doc_id, "title": title})
    except Exception as e:
        log.error("create_note error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def update_document_metadata(
    document_id: str,
    title: Optional[str] = None,
    metadata_updates: Optional[dict] = None,
) -> str:
    """Update metadata on an existing document.

    Args:
        document_id: UUID of the document
        title: New title (optional)
        metadata_updates: Dict of key-value pairs to merge into existing metadata
    """
    p = await get_pool()
    try:
        doc = await p.fetchrow("SELECT id, title, metadata FROM core.documents WHERE id = $1::uuid", document_id)
        if not doc:
            return json.dumps({"error": "Document not found"})

        if title:
            await p.execute("UPDATE core.documents SET title = $1, updated_at = now() WHERE id = $2::uuid", title, document_id)

        if metadata_updates:
            existing = doc["metadata"] or {}
            existing.update(metadata_updates)
            await p.execute(
                "UPDATE core.documents SET metadata = $1::jsonb, updated_at = now() WHERE id = $2::uuid",
                json.dumps(existing), document_id,
            )

        await _log_access("update_document_metadata", document_id, 1)
        return json.dumps({"success": True, "document_id": document_id})
    except Exception as e:
        log.error("update_document_metadata error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def tag_document(document_id: str, tags: list[str], remove: bool = False) -> str:
    """Add or remove tags on a document.

    Args:
        document_id: UUID of the document
        tags: List of tag names to add or remove
        remove: If True, remove the tags; if False (default), add them
    """
    p = await get_pool()
    try:
        doc = await p.fetchrow("SELECT id FROM core.documents WHERE id = $1::uuid", document_id)
        if not doc:
            return json.dumps({"error": "Document not found"})

        results = []
        for tag_name in tags:
            if remove:
                tag = await p.fetchrow("SELECT id FROM core.tags WHERE name = $1", tag_name)
                if tag:
                    await p.execute(
                        "DELETE FROM core.document_tags WHERE document_id = $1::uuid AND tag_id = $2",
                        document_id, tag["id"],
                    )
                    results.append({"tag": tag_name, "action": "removed"})
            else:
                tag = await p.fetchrow("SELECT id FROM core.tags WHERE name = $1", tag_name)
                if not tag:
                    tag = await p.fetchrow(
                        "INSERT INTO core.tags (name, category) VALUES ($1, 'user') RETURNING id",
                        tag_name,
                    )
                await p.execute(
                    "INSERT INTO core.document_tags (document_id, tag_id) VALUES ($1::uuid, $2) ON CONFLICT DO NOTHING",
                    document_id, tag["id"],
                )
                results.append({"tag": tag_name, "action": "added"})

        await _log_access("tag_document", f"{document_id}: {tags}", len(results))
        return json.dumps({"success": True, "document_id": document_id, "changes": results})
    except Exception as e:
        log.error("tag_document error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def link_document_to_case(document_id: str, case_number: str) -> str:
    """Link a document to a legal case.

    Args:
        document_id: UUID of the document
        case_number: Case number to link to
    """
    p = await get_pool()
    try:
        case = await p.fetchrow(
            "SELECT id, case_number, case_title FROM legal.cases WHERE case_number ILIKE $1",
            f"%{case_number}%",
        )
        if not case:
            return json.dumps({"error": f"Case '{case_number}' not found"})

        await p.execute(
            "INSERT INTO legal.case_documents (case_id, document_id) VALUES ($1, $2::uuid) ON CONFLICT DO NOTHING",
            case["id"], document_id,
        )

        return json.dumps({
            "success": True,
            "document_id": document_id,
            "case_number": case["case_number"],
            "case_title": case["case_title"],
        })
    except Exception as e:
        log.error("link_document_to_case error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def create_document_relationship(
    source_document_id: str,
    target_document_id: str,
    relationship_type: str,
    confidence: float = 1.0,
    notes: Optional[str] = None,
) -> str:
    """Create a relationship between two documents.

    Args:
        source_document_id: UUID of the source document
        target_document_id: UUID of the target document
        relationship_type: Type — "references", "responds_to", "amends", "supersedes", "relates_to", "attachment_of"
        confidence: Confidence score 0.0-1.0 (default 1.0)
        notes: Optional notes about the relationship
    """
    p = await get_pool()
    try:
        meta = {"notes": notes} if notes else {}
        await p.execute("""
            INSERT INTO core.document_relationships (source_document_id, target_document_id,
                                                      relationship_type, confidence, metadata, created_at)
            VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, now())
        """, source_document_id, target_document_id, relationship_type, confidence, json.dumps(meta))

        return json.dumps({
            "success": True,
            "source": source_document_id,
            "target": target_document_id,
            "type": relationship_type,
        })
    except Exception as e:
        log.error("create_document_relationship error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def delete_document(document_id: str, confirm: bool = False) -> str:
    """Delete a document and all its chunks, tags, and relationships.

    WARNING: This is destructive and cannot be undone.

    Args:
        document_id: UUID of the document to delete
        confirm: Must be True to proceed (safety check)
    """
    if not confirm:
        return json.dumps({"error": "Set confirm=True to delete. This cannot be undone."})

    p = await get_pool()
    try:
        doc = await p.fetchrow("SELECT id, title, domain FROM core.documents WHERE id = $1::uuid", document_id)
        if not doc:
            return json.dumps({"error": "Document not found"})

        # Delete in order of dependencies
        await p.execute("DELETE FROM core.document_tags WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM core.entity_mentions WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM core.document_relationships WHERE source_document_id = $1::uuid OR target_document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM legal.case_documents WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM legal.email_metadata WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM legal.filing_metadata WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM medical.record_metadata WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM core.document_chunks WHERE document_id = $1::uuid", document_id)
        await p.execute("DELETE FROM core.documents WHERE id = $1::uuid", document_id)

        await _log_access("delete_document", f"DELETED: {doc['title']}", 1)
        return json.dumps({"success": True, "deleted": document_id, "title": doc["title"]})
    except Exception as e:
        log.error("delete_document error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                    ENTITY / PEOPLE TOOLS                     ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def search_entities(
    name: Optional[str] = None,
    entity_type: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search for people, organizations, and other entities mentioned in documents.

    Args:
        name: Search by name (partial match)
        entity_type: Filter by type — "person", "organization", "court", "location"
        limit: Max results (default 20)
    """
    p = await get_pool()
    try:
        conditions = []
        params: list = []
        idx = 1

        if name:
            conditions.append(f"(e.name ILIKE ${idx} OR e.normalized_name ILIKE ${idx})")
            params.append(f"%{name}%")
            idx += 1
        if entity_type:
            conditions.append(f"e.entity_type = ${idx}")
            params.append(entity_type)
            idx += 1

        params.append(limit)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = await p.fetch(f"""
            SELECT e.id, e.name, e.entity_type, e.normalized_name, e.metadata,
                   COUNT(em.id) AS mention_count
            FROM core.entities e
            LEFT JOIN core.entity_mentions em ON e.id = em.entity_id
            {where}
            GROUP BY e.id
            ORDER BY mention_count DESC
            LIMIT ${idx}
        """, *params)

        results = [{
            "entity_id": r["id"],
            "name": r["name"],
            "type": r["entity_type"],
            "normalized_name": r["normalized_name"],
            "metadata": r["metadata"],
            "mention_count": r["mention_count"],
        } for r in rows]

        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_entities error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_entity_documents(entity_id: int, limit: int = 20) -> str:
    """Get all documents that mention a specific entity.

    Args:
        entity_id: ID of the entity
        limit: Max results (default 20)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT DISTINCT d.id, d.title, d.filename, d.domain, d.document_type,
                   d.created_at, em.role, em.mention_context
            FROM core.entity_mentions em
            JOIN core.documents d ON em.document_id = d.id
            WHERE em.entity_id = $1
            ORDER BY d.created_at DESC
            LIMIT $2
        """, entity_id, limit)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "date": _ser(r["created_at"]),
            "role": r["role"],
            "context": r["mention_context"],
        } for r in rows]

        return json.dumps({"entity_id": entity_id, "documents": results, "count": len(results)})
    except Exception as e:
        log.error("get_entity_documents error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                   EMAIL THREAD TOOLS                         ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def get_email_thread(message_id: Optional[str] = None, thread_id: Optional[str] = None) -> str:
    """Retrieve a full email thread/conversation.

    Args:
        message_id: Message-ID of any email in the thread
        thread_id: Thread ID to look up
    """
    p = await get_pool()
    try:
        if not thread_id and message_id:
            row = await p.fetchrow(
                "SELECT thread_id FROM legal.email_metadata WHERE message_id = $1",
                message_id,
            )
            if row:
                thread_id = row["thread_id"]

        if not thread_id:
            return json.dumps({"error": "Could not find thread. Provide message_id or thread_id."})

        rows = await p.fetch("""
            SELECT d.id, d.title, d.filename,
                   em.sender, em.recipients, em.cc, em.date_sent,
                   em.direction, em.subject, em.message_id, em.in_reply_to,
                   LEFT(d.full_content, 1000) AS excerpt
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE em.thread_id = $1
            ORDER BY em.date_sent ASC
        """, thread_id)

        messages = [{
            "document_id": str(r["id"]),
            "subject": r["subject"] or r["title"],
            "from": r["sender"],
            "to": r["recipients"],
            "cc": r["cc"],
            "date": _ser(r["date_sent"]),
            "direction": r["direction"],
            "message_id": r["message_id"],
            "in_reply_to": r["in_reply_to"],
            "excerpt": r["excerpt"],
        } for r in rows]

        return json.dumps({"thread_id": thread_id, "messages": messages, "count": len(messages)})
    except Exception as e:
        log.error("get_email_thread error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_mailboxes() -> str:
    """List all email mailboxes and their message counts."""
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT em.mailbox, COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE em.direction = 'inbound') AS inbound,
                   COUNT(*) FILTER (WHERE em.direction = 'outbound') AS outbound,
                   MIN(em.date_sent) AS earliest,
                   MAX(em.date_sent) AS latest
            FROM legal.email_metadata em
            WHERE em.mailbox IS NOT NULL
            GROUP BY em.mailbox
            ORDER BY total DESC
        """)

        return json.dumps({"mailboxes": [{
            "mailbox": r["mailbox"],
            "total": r["total"],
            "inbound": r["inbound"],
            "outbound": r["outbound"],
            "earliest": _ser(r["earliest"]),
            "latest": _ser(r["latest"]),
        } for r in rows]})
    except Exception as e:
        log.error("list_mailboxes error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                   ANALYTICS / STATS TOOLS                    ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def get_database_stats() -> str:
    """Get statistics about the document database — total counts by domain and type."""
    p = await get_pool()
    try:
        total = await p.fetchrow("SELECT COUNT(*) AS cnt FROM core.documents")
        by_domain = await p.fetch(
            "SELECT domain, COUNT(*) AS cnt FROM core.documents GROUP BY domain ORDER BY domain"
        )
        by_type = await p.fetch(
            "SELECT domain, document_type, COUNT(*) AS cnt FROM core.documents GROUP BY domain, document_type ORDER BY domain, cnt DESC"
        )
        chunks = await p.fetchrow("SELECT COUNT(*) AS cnt, COUNT(embedding) AS embedded FROM core.document_chunks")
        cases = await p.fetchrow("SELECT COUNT(*) AS cnt FROM legal.cases")
        tags = await p.fetchrow("SELECT COUNT(*) AS cnt FROM core.tags")
        entities = await p.fetchrow("SELECT COUNT(*) AS cnt FROM core.entities")

        return json.dumps({
            "total_documents": total["cnt"],
            "total_chunks": chunks["cnt"],
            "chunks_with_embeddings": chunks["embedded"],
            "total_cases": cases["cnt"],
            "total_tags": tags["cnt"],
            "total_entities": entities["cnt"],
            "by_domain": {r["domain"]: r["cnt"] for r in by_domain},
            "by_type": [{
                "domain": r["domain"],
                "type": r["document_type"],
                "count": r["cnt"],
            } for r in by_type],
        })
    except Exception as e:
        log.error("get_database_stats error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def recent_activity(days: int = 7, domain: Optional[str] = None, limit: int = 20) -> str:
    """Get recent document activity — new filings, emails, ingestions.

    Args:
        days: Look back this many days (default 7)
        domain: Filter by domain or omit for all
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        if domain:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.created_at, em.sender, em.date_sent
                FROM core.documents d
                LEFT JOIN legal.email_metadata em ON d.id = em.document_id
                WHERE d.created_at >= now() - ($1 || ' days')::interval
                  AND d.domain = $2
                ORDER BY d.created_at DESC LIMIT $3
            """, str(days), domain, limit)
        else:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.created_at, em.sender, em.date_sent
                FROM core.documents d
                LEFT JOIN legal.email_metadata em ON d.id = em.document_id
                WHERE d.created_at >= now() - ($1 || ' days')::interval
                ORDER BY d.created_at DESC LIMIT $2
            """, str(days), limit)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "created": _ser(r["created_at"]),
            "sender": r["sender"],
            "email_date": _ser(r["date_sent"]),
        } for r in rows]

        return json.dumps({"results": results, "count": len(results), "days": days})
    except Exception as e:
        log.error("recent_activity error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_search_history(days: int = 7, limit: int = 50) -> str:
    """View recent search analytics — what's been searched and by whom.

    Args:
        days: Look back this many days (default 7)
        limit: Max results (default 50)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT query, tool_name, result_count, searched_at
            FROM ops.search_analytics
            WHERE searched_at >= now() - ($1 || ' days')::interval
            ORDER BY searched_at DESC
            LIMIT $2
        """, str(days), limit)

        return json.dumps({"searches": [{
            "query": r["query"],
            "tool": r["tool_name"],
            "results": r["result_count"],
            "timestamp": _ser(r["searched_at"]),
        } for r in rows], "count": len(rows)})
    except Exception as e:
        log.error("get_search_history error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def get_ingestion_status(limit: int = 10) -> str:
    """Check the status of document ingestion jobs.

    Args:
        limit: Number of recent jobs to return (default 10)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT id, domain, source, status, documents_processed,
                   chunks_processed, documents_failed, started_at, completed_at
            FROM ops.ingestion_jobs
            ORDER BY started_at DESC
            LIMIT $1
        """, limit)

        return json.dumps({"jobs": [{
            "id": r["id"],
            "domain": r["domain"],
            "source": r["source"],
            "status": r["status"],
            "docs_processed": r["documents_processed"],
            "chunks_processed": r["chunks_processed"],
            "docs_failed": r["documents_failed"],
            "started": _ser(r["started_at"]),
            "completed": _ser(r["completed_at"]),
        } for r in rows]})
    except Exception as e:
        log.error("get_ingestion_status error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                   TAG MANAGEMENT TOOLS                       ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def list_tags(category: Optional[str] = None) -> str:
    """List all available tags.

    Args:
        category: Filter by category or omit for all
    """
    p = await get_pool()
    try:
        if category:
            rows = await p.fetch("""
                SELECT t.id, t.name, t.category, COUNT(dt.document_id) AS usage_count
                FROM core.tags t
                LEFT JOIN core.document_tags dt ON t.id = dt.tag_id
                WHERE t.category = $1
                GROUP BY t.id ORDER BY usage_count DESC
            """, category)
        else:
            rows = await p.fetch("""
                SELECT t.id, t.name, t.category, COUNT(dt.document_id) AS usage_count
                FROM core.tags t
                LEFT JOIN core.document_tags dt ON t.id = dt.tag_id
                GROUP BY t.id ORDER BY usage_count DESC
            """)

        return json.dumps({"tags": [{
            "id": r["id"],
            "name": r["name"],
            "category": r["category"],
            "usage_count": r["usage_count"],
        } for r in rows]})
    except Exception as e:
        log.error("list_tags error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def search_by_tag(tag_name: str, limit: int = 20) -> str:
    """Find all documents with a specific tag.

    Args:
        tag_name: Tag name to search for
        limit: Max results (default 20)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT d.id, d.title, d.filename, d.domain, d.document_type, d.created_at
            FROM core.documents d
            JOIN core.document_tags dt ON d.id = dt.document_id
            JOIN core.tags t ON dt.tag_id = t.id
            WHERE t.name ILIKE $1
            ORDER BY d.created_at DESC
            LIMIT $2
        """, f"%{tag_name}%", limit)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "date": _ser(r["created_at"]),
        } for r in rows]

        return json.dumps({"tag": tag_name, "results": results, "count": len(results)})
    except Exception as e:
        log.error("search_by_tag error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                   DOCUMENT RELATIONSHIPS                     ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def get_related_documents(document_id: str) -> str:
    """Get all documents related to a given document.

    Args:
        document_id: UUID of the document
    """
    p = await get_pool()
    try:
        outgoing = await p.fetch("""
            SELECT dr.relationship_type, dr.confidence, dr.metadata,
                   d.id, d.title, d.filename, d.domain, d.document_type
            FROM core.document_relationships dr
            JOIN core.documents d ON dr.target_document_id = d.id
            WHERE dr.source_document_id = $1::uuid
        """, document_id)

        incoming = await p.fetch("""
            SELECT dr.relationship_type, dr.confidence, dr.metadata,
                   d.id, d.title, d.filename, d.domain, d.document_type
            FROM core.document_relationships dr
            JOIN core.documents d ON dr.source_document_id = d.id
            WHERE dr.target_document_id = $1::uuid
        """, document_id)

        return json.dumps({
            "document_id": document_id,
            "outgoing": [{
                "relationship": r["relationship_type"],
                "confidence": r["confidence"],
                "document_id": str(r["id"]),
                "title": r["title"] or r["filename"],
                "domain": r["domain"],
                "type": r["document_type"],
            } for r in outgoing],
            "incoming": [{
                "relationship": r["relationship_type"],
                "confidence": r["confidence"],
                "document_id": str(r["id"]),
                "title": r["title"] or r["filename"],
                "domain": r["domain"],
                "type": r["document_type"],
            } for r in incoming],
        })
    except Exception as e:
        log.error("get_related_documents error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                    TIMELINE / SUMMARY                        ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def case_timeline(case_number: str, limit: int = 50) -> str:
    """Get a chronological timeline of all events in a case.

    Args:
        case_number: Case number to get timeline for
        limit: Max events (default 50)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT d.id, d.title, d.filename, d.document_type,
                   COALESCE(em.date_sent, fm.date_filed::timestamptz, d.created_at) AS event_date,
                   em.sender, em.direction, fm.filing_type, fm.filed_by
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN legal.filing_metadata fm ON d.id = fm.document_id
            LEFT JOIN legal.case_documents cd ON d.id = cd.document_id
            LEFT JOIN legal.cases c ON cd.case_id = c.id
            WHERE c.case_number ILIKE $1 OR d.title ILIKE $1 OR d.source_path ILIKE $1
            ORDER BY COALESCE(em.date_sent, fm.date_filed::timestamptz, d.created_at) ASC
            LIMIT $2
        """, f"%{case_number}%", limit)

        events = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "type": r["document_type"],
            "date": _ser(r["event_date"]),
            "sender": r["sender"],
            "direction": r["direction"],
            "filing_type": r["filing_type"],
            "filed_by": r["filed_by"],
        } for r in rows]

        await _log_access("case_timeline", case_number, len(events))
        return json.dumps({"case_number": case_number, "events": events, "count": len(events)})
    except Exception as e:
        log.error("case_timeline error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                      ADMIN / SYSTEM                          ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def list_oauth_clients() -> str:
    """List registered OAuth clients and their last activity (admin only)."""
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT client_id, client_name, scopes, is_active, created_at, last_used
            FROM ops.oauth_clients
            ORDER BY created_at
        """)

        return json.dumps({"clients": [{
            "client_id": r["client_id"],
            "client_name": r["client_name"],
            "scopes": r["scopes"],
            "is_active": r["is_active"],
            "created_at": _ser(r["created_at"]),
            "last_used": _ser(r["last_used"]),
        } for r in rows]})
    except Exception as e:
        log.error("list_oauth_clients error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def system_health() -> str:
    """Check system health — database connectivity, pool stats, server info."""
    p = await get_pool()
    try:
        db_time = await p.fetchrow("SELECT now() AS ts, version() AS ver")
        pool_size = p.get_size()
        pool_free = p.get_idle_size()

        return json.dumps({
            "status": "healthy",
            "server": "Athena Cognitive Platform MCP Server v2.0",
            "database_time": _ser(db_time["ts"]),
            "postgres_version": db_time["ver"][:50],
            "pool_size": pool_size,
            "pool_idle": pool_free,
            "transport": "http/sse" if "--http" in sys.argv else "stdio",
        })
    except Exception as e:
        return json.dumps({"status": "unhealthy", "error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                     MCP RESOURCES                            ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.resource("wdws://stats")
async def resource_stats() -> str:
    """Database statistics overview."""
    return await get_database_stats()


@mcp.resource("wdws://cases")
async def resource_cases() -> str:
    """List of all legal cases."""
    return await list_cases()


@mcp.resource("wdws://mailboxes")
async def resource_mailboxes() -> str:
    """Email mailbox overview."""
    return await list_mailboxes()


# ╔══════════════════════════════════════════════════════════════╗
# ║                      MCP PROMPTS                             ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.prompt()
async def case_briefing(case_number: str) -> str:
    """Generate a comprehensive briefing prompt for a legal case."""
    return f"""Please provide a comprehensive briefing on case {case_number}. Include:
1. Case overview (parties, court, status, key dates)
2. Summary of all court filings in chronological order
3. Key email communications related to this case
4. Any pending deadlines or action items
5. Relationships to other cases if any

Use the lookup_case, search_by_case, case_timeline, and search_emails tools."""


@mcp.prompt()
async def email_summary(days: int = 7) -> str:
    """Generate a prompt to summarize recent email activity."""
    return f"""Please provide a summary of email activity over the last {days} days. Include:
1. Total emails received and sent
2. Key correspondences by case
3. Any urgent items requiring response
4. Notable attachments received

Use the search_emails and recent_activity tools."""


@mcp.prompt()
async def medical_summary() -> str:
    """Generate a prompt to summarize medical records."""
    return """Please provide a comprehensive summary of all medical records. Include:
1. List of providers/facilities
2. Recent visits and dates of service
3. Key diagnoses and conditions
4. Current medications if mentioned
5. Upcoming appointments if any

Use the search_medical_records tool."""


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Athena Cognitive Platform MCP Server v2.0")
    parser.add_argument("--http", action="store_true", help="Run as HTTP/SSE server")
    parser.add_argument("--port", type=int, default=9200, help="HTTP port (default 9200)")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    args = parser.parse_args()

    if args.http:
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        log.info("Starting Athena MCP Server v2.0 (HTTP/SSE + OAuth) on %s:%d", args.host, args.port)
        mcp.run(transport="sse")
    else:
        log.info("Starting Athena MCP Server v2.0 (stdio)")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
