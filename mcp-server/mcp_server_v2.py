#!/usr/bin/env python3
"""
WDWS Unified MCP Server v2 — PostgreSQL + OAuth 2.0

Single MCP server consolidating all domains:
  • Legal cases (court filings, emails, attachments)
  • Medical records (clinical notes, lab results, imaging)
  • Paperless-ngx documents

Features:
  • Full OAuth 2.0 (RFC 6749) with per-client credentials
  • PostgreSQL-backed client registration, tokens, auth codes
  • pgvector semantic search + PostgreSQL full-text search
  • Read, write, upload, tag, annotate, delete operations
  • Exposed via Cloudflare Tunnel at https://klunky.12432.net

Clients:
  • Claude Desktop  — own client_id / client_secret
  • ChatGPT         — own client_id / client_secret

Transport:
  stdio:  /opt/wdws/venv/bin/python3 mcp_server_v2.py
  http:   /opt/wdws/venv/bin/python3 mcp_server_v2.py --http --port 9200
"""

import os
import sys
import json
import asyncio
import base64
import hashlib
import logging
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, timezone
from typing import Any, AsyncIterator, Optional

import asyncpg
import httpx
from pydantic import AnyUrl
from cryptography.fernet import Fernet, InvalidToken

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    RegistrationError,
    TokenError,
    construct_redirect_uri,
)
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions, RevocationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from mcp.server.auth.middleware.auth_context import get_access_token as _get_auth_token
import functools

# ── Tool Annotations ─────────────────────────────────────────
# Tell clients which tools are safe to run without confirmation
READ_ONLY = ToolAnnotations(readOnlyHint=True)
WRITE_OP = ToolAnnotations(readOnlyHint=False)
DESTRUCTIVE = ToolAnnotations(destructiveHint=True)

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("wdws-mcp")

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
BASE_URL = os.getenv("MCP_BASE_URL", "https://klunky.12432.net")
OAUTH_CLIENT_SECRET_KEY = os.getenv("OAUTH_CLIENT_SECRET_KEY", "").strip()

# ── OAuth Client Secrets (env-based, for SDK client_secret validation) ───
# Keyed by client_id. The SDK compares these against the secret
# submitted in the /token POST body (client_secret_post method).
CLIENT_SECRETS: dict[str, str] = {}
for _env_key, _env_val in os.environ.items():
    if _env_key.endswith("_CLIENT_SECRET"):
        _prefix = _env_key.replace("_CLIENT_SECRET", "")
        _cid = os.getenv(f"{_prefix}_CLIENT_ID", "").strip()
        if _cid:
            CLIENT_SECRETS[_cid] = _env_val
# Direct fallback mapping
if "claude-desktop" not in CLIENT_SECRETS:
    CLIENT_SECRETS["claude-desktop"] = os.getenv("CLAUDE_CLIENT_SECRET", "")
if "chatgpt" not in CLIENT_SECRETS:
    CLIENT_SECRETS["chatgpt"] = os.getenv("CHATGPT_CLIENT_SECRET", "")
CLIENT_SECRETS = {k: v for k, v in CLIENT_SECRETS.items() if v}

if OAUTH_CLIENT_SECRET_KEY:
    log.info("OAuth client secret encryption enabled")
else:
    log.warning("OAUTH_CLIENT_SECRET_KEY not set; client secrets will only be read from env")

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
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, dict):
        return val
    if isinstance(val, uuid.UUID):
        return str(val)
    return str(val)


def _row_dict(row, keys=None) -> dict:
    if keys:
        return {k: _ser(row[k]) for k in keys if k in row.keys()}
    return {k: _ser(v) for k, v in dict(row).items()}




# ── Tool-call logging ────────────────────────────────────────

async def _log_tool_call(tool_name: str, arguments: dict, result: str,
                         duration_ms: int, error: str = None):
    """Log every MCP tool call to ops.mcp_query_log for analytics."""
    try:
        token_info = _get_auth_token()
        client_id = token_info.client_id if token_info else "anonymous"
    except Exception:
        client_id = "anonymous"

    # Build result summary (truncate large results)
    try:
        parsed = json.loads(result) if isinstance(result, str) else result
        if isinstance(parsed, dict):
            result_count = parsed.get("count", parsed.get("total", 0))
            # Keep summary small
            summary = {k: v for k, v in parsed.items()
                       if k in ("count", "total", "query", "domain", "case_number",
                                "tool", "type", "message", "id", "title")}
        elif isinstance(parsed, list):
            result_count = len(parsed)
            summary = {"count": result_count}
        else:
            result_count = 1 if parsed else 0
            summary = {"preview": str(parsed)[:200]}
    except Exception:
        result_count = 0
        summary = {"preview": str(result)[:500]}

    # Store raw result (truncate to avoid bloating DB)
    max_raw_chars = int(os.getenv("MCP_LOG_RAW_MAX_CHARS", "20000"))
    raw_text = ""
    try:
        if result is not None:
            if isinstance(result, str):
                raw_text = result
            else:
                raw_text = json.dumps(result, default=str)
    except Exception:
        raw_text = str(result)

    if raw_text and len(raw_text) > max_raw_chars:
        raw_text = raw_text[:max_raw_chars] + "... (truncated)"

    try:
        p = await get_pool()
        # Get client name
        name_row = await p.fetchrow(
            "SELECT client_name FROM ops.oauth_clients WHERE client_id = $1",
            client_id,
        )
        client_name = name_row["client_name"] if name_row else client_id

        # Truncate arguments for storage (remove huge embedding arrays etc)
        safe_args = {}
        for k, v in arguments.items():
            if isinstance(v, str) and len(v) > 500:
                safe_args[k] = v[:500] + "..."
            else:
                safe_args[k] = v

        await p.execute("""
            INSERT INTO ops.mcp_query_log
                (client_id, client_name, tool_name, arguments, result_summary,
                 result_count, duration_ms, error, result_raw)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, client_id, client_name, tool_name,
            json.dumps(safe_args), json.dumps(summary),
            result_count, duration_ms, error, raw_text)

        # Update client stats
        await p.execute("""
            UPDATE ops.oauth_clients
            SET last_used = now(), total_tool_calls = COALESCE(total_tool_calls, 0) + 1
            WHERE client_id = $1
        """, client_id)

        log.debug("Logged tool call: %s by %s (%dms)", tool_name, client_id, duration_ms)
    except Exception as e:
        log.warning("Failed to log tool call: %s", e)


def logged_tool(func):
    """Decorator that wraps an MCP tool function to log its invocation."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        tool_name = func.__name__
        t0 = time.time()
        error = None
        result = ""
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            error = str(e)
            raise
        finally:
            duration_ms = int((time.time() - t0) * 1000)
            # kwargs are the tool arguments
            tool_args = {k: v for k, v in kwargs.items() if v is not None}
            asyncio.create_task(
                _log_tool_call(tool_name, tool_args, result or "", duration_ms, error)
            )
    return wrapper

# ── Embedding helper ─────────────────────────────────────────
async def _embed_query(text: str) -> list[float]:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={"model": EMBEDDING_MODEL, "input": text, "dimensions": EMBEDDING_DIMS},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


# ══════════════════════════════════════════════════════════════
#  OAUTH 2.0 PROVIDER — PostgreSQL-backed
# ══════════════════════════════════════════════════════════════

OAUTH_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ops.oauth_authorization_codes (
    code            TEXT PRIMARY KEY,
    client_id       TEXT NOT NULL,
    scopes          TEXT[] DEFAULT '{}',
    code_challenge  TEXT NOT NULL,
    redirect_uri    TEXT NOT NULL,
    redirect_uri_provided_explicitly BOOLEAN DEFAULT TRUE,
    resource        TEXT,
    expires_at      DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.oauth_access_tokens (
    token       TEXT PRIMARY KEY,
    client_id   TEXT NOT NULL,
    scopes      TEXT[] DEFAULT '{}',
    resource    TEXT,
    expires_at  BIGINT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.oauth_refresh_tokens (
    token       TEXT PRIMARY KEY,
    client_id   TEXT NOT NULL,
    scopes      TEXT[] DEFAULT '{}',
    expires_at  BIGINT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE IF EXISTS ops.oauth_clients
    ADD COLUMN IF NOT EXISTS client_secret_enc TEXT;
"""


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _get_fernet() -> Optional[Fernet]:
    if not OAUTH_CLIENT_SECRET_KEY:
        return None
    try:
        raw = OAUTH_CLIENT_SECRET_KEY.encode()
        # Accept raw passphrase or pre-generated Fernet key
        if len(raw) == 44:
            key = raw
        else:
            key = base64.urlsafe_b64encode(hashlib.sha256(raw).digest())
        return Fernet(key)
    except Exception as e:
        log.warning("Invalid OAUTH_CLIENT_SECRET_KEY: %s", e)
        return None


def _encrypt_client_secret(secret: str) -> Optional[str]:
    f = _get_fernet()
    if not f:
        return None
    return f.encrypt(secret.encode()).decode()


def _decrypt_client_secret(token: str) -> Optional[str]:
    f = _get_fernet()
    if not f:
        return None
    try:
        return f.decrypt(token.encode()).decode()
    except InvalidToken:
        log.warning("Failed to decrypt client secret (invalid token)")
        return None


class PostgresOAuthProvider:
    """
    Full OAuth 2.0 Authorization Server Provider backed by PostgreSQL.
    Implements the MCP SDK OAuthAuthorizationServerProvider protocol.
    """

    def __init__(self):
        self._schema_ready = False

    async def _ensure_schema(self):
        if self._schema_ready:
            return
        p = await get_pool()
        await p.execute(OAUTH_SCHEMA_SQL)
        self._schema_ready = True
        log.info("OAuth schema ensured")

    # ── Client management ────────────────────────────────────

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        await self._ensure_schema()
        p = await get_pool()
        row = await p.fetchrow(
            "SELECT * FROM ops.oauth_clients WHERE client_id = $1 AND is_active = TRUE",
            client_id,
        )
        if not row:
            return None

        # Prefer encrypted secret in DB, fall back to env
        enc_secret = row["client_secret_enc"]
        secret = _decrypt_client_secret(enc_secret) if enc_secret else None
        if not secret:
            secret = CLIENT_SECRETS.get(row["client_id"])

        # If env has secret and DB doesn't, seed encrypted secret
        if secret and not enc_secret:
            enc = _encrypt_client_secret(secret)
            if enc:
                await p.execute(
                    "UPDATE ops.oauth_clients SET client_secret_enc = $1 WHERE client_id = $2",
                    enc,
                    client_id,
                )

        if secret:
            auth_method = "client_secret_post"
        else:
            auth_method = "none"
            log.warning("No client_secret for %s — public (PKCE-only) auth", client_id)

        return OAuthClientInformationFull(
            client_id=row["client_id"],
            client_secret=secret,
            client_name=row["client_name"],
            redirect_uris=row["redirect_uris"] or [f"{BASE_URL}/callback"],
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            token_endpoint_auth_method=auth_method,
            scope=" ".join(row["scopes"] or ["read", "write", "admin"]),
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        await self._ensure_schema()
        p = await get_pool()
        scopes = client_info.scope.split() if client_info.scope else ["read", "write"]
        redirect_uris = [str(u) for u in (client_info.redirect_uris or [])]
        secret_hash = _hash_secret(client_info.client_secret) if client_info.client_secret else None
        secret_enc = _encrypt_client_secret(client_info.client_secret) if client_info.client_secret else None

        await p.execute("""
            INSERT INTO ops.oauth_clients (client_id, client_name, client_secret_hash, client_secret_enc, redirect_uris, scopes)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (client_id) DO UPDATE SET
                client_name = EXCLUDED.client_name,
                client_secret_hash = COALESCE(EXCLUDED.client_secret_hash, ops.oauth_clients.client_secret_hash),
                client_secret_enc = COALESCE(EXCLUDED.client_secret_enc, ops.oauth_clients.client_secret_enc),
                redirect_uris = EXCLUDED.redirect_uris,
                scopes = EXCLUDED.scopes
        """, client_info.client_id, client_info.client_name or client_info.client_id,
             secret_hash, secret_enc, redirect_uris, scopes)
        log.info("Registered client: %s", client_info.client_id)

    # ── Authorization ────────────────────────────────────────

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        await self._ensure_schema()
        p = await get_pool()

        # Generate authorization code (256 bits of entropy)
        code = secrets.token_urlsafe(32)
        expires_at = time.time() + 600  # 10 min

        await p.execute("""
            INSERT INTO ops.oauth_authorization_codes
                (code, client_id, scopes, code_challenge, redirect_uri,
                 redirect_uri_provided_explicitly, resource, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, code, client.client_id, params.scopes or [],
             params.code_challenge, str(params.redirect_uri),
             params.redirect_uri_provided_explicitly,
             params.resource, expires_at)

        # Auto-approve — redirect back with the code immediately
        redirect_uri = construct_redirect_uri(
            str(params.redirect_uri),
            code=code,
            state=params.state,
        )
        log.info("Authorized client %s, redirecting", client.client_id)
        return redirect_uri

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        await self._ensure_schema()
        p = await get_pool()
        row = await p.fetchrow(
            "SELECT * FROM ops.oauth_authorization_codes WHERE code = $1 AND client_id = $2",
            authorization_code, client.client_id,
        )
        if not row:
            return None
        if row["expires_at"] < time.time():
            await p.execute("DELETE FROM ops.oauth_authorization_codes WHERE code = $1", authorization_code)
            return None
        return AuthorizationCode(
            code=row["code"],
            client_id=row["client_id"],
            scopes=list(row["scopes"] or []),
            code_challenge=row["code_challenge"],
            redirect_uri=row["redirect_uri"],
            redirect_uri_provided_explicitly=row["redirect_uri_provided_explicitly"],
            resource=row["resource"],
            expires_at=row["expires_at"],
        )

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        await self._ensure_schema()
        p = await get_pool()

        # Delete the used auth code
        await p.execute(
            "DELETE FROM ops.oauth_authorization_codes WHERE code = $1",
            authorization_code.code,
        )

        # Generate tokens
        access_token = secrets.token_urlsafe(48)
        refresh_token = secrets.token_urlsafe(48)
        expires_in = 86400  # 24 hours
        now_ts = int(time.time())

        await p.execute("""
            INSERT INTO ops.oauth_access_tokens (token, client_id, scopes, resource, expires_at)
            VALUES ($1, $2, $3, $4, $5)
        """, access_token, client.client_id, authorization_code.scopes,
             authorization_code.resource, now_ts + expires_in)

        await p.execute("""
            INSERT INTO ops.oauth_refresh_tokens (token, client_id, scopes, expires_at)
            VALUES ($1, $2, $3, $4)
        """, refresh_token, client.client_id, authorization_code.scopes,
             now_ts + 86400 * 30)  # 30 day refresh

        # Update last_used
        await p.execute(
            "UPDATE ops.oauth_clients SET last_used = now() WHERE client_id = $1",
            client.client_id,
        )

        log.info("Issued tokens for client %s", client.client_id)
        return OAuthToken(
            access_token=access_token,
            token_type="Bearer",
            expires_in=expires_in,
            scope=" ".join(authorization_code.scopes) if authorization_code.scopes else None,
            refresh_token=refresh_token,
        )

    # ── Token loading / refresh ──────────────────────────────

    async def load_access_token(self, token: str) -> AccessToken | None:
        await self._ensure_schema()
        p = await get_pool()
        row = await p.fetchrow(
            "SELECT * FROM ops.oauth_access_tokens WHERE token = $1", token
        )
        if not row:
            return None
        if row["expires_at"] and row["expires_at"] < int(time.time()):
            await p.execute("DELETE FROM ops.oauth_access_tokens WHERE token = $1", token)
            return None
        return AccessToken(
            token=row["token"],
            client_id=row["client_id"],
            scopes=list(row["scopes"] or []),
            resource=row["resource"],
            expires_at=row["expires_at"],
        )

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        await self._ensure_schema()
        p = await get_pool()
        row = await p.fetchrow(
            "SELECT * FROM ops.oauth_refresh_tokens WHERE token = $1 AND client_id = $2",
            refresh_token, client.client_id,
        )
        if not row:
            return None
        if row["expires_at"] and row["expires_at"] < int(time.time()):
            await p.execute("DELETE FROM ops.oauth_refresh_tokens WHERE token = $1", refresh_token)
            return None
        return RefreshToken(
            token=row["token"],
            client_id=row["client_id"],
            scopes=list(row["scopes"] or []),
            expires_at=row["expires_at"],
        )

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        await self._ensure_schema()
        p = await get_pool()

        # Revoke old refresh token
        await p.execute("DELETE FROM ops.oauth_refresh_tokens WHERE token = $1", refresh_token.token)

        # New tokens
        new_access = secrets.token_urlsafe(48)
        new_refresh = secrets.token_urlsafe(48)
        use_scopes = scopes if scopes else refresh_token.scopes
        expires_in = 86400
        now_ts = int(time.time())

        await p.execute("""
            INSERT INTO ops.oauth_access_tokens (token, client_id, scopes, expires_at)
            VALUES ($1, $2, $3, $4)
        """, new_access, client.client_id, use_scopes, now_ts + expires_in)

        await p.execute("""
            INSERT INTO ops.oauth_refresh_tokens (token, client_id, scopes, expires_at)
            VALUES ($1, $2, $3, $4)
        """, new_refresh, client.client_id, use_scopes, now_ts + 86400 * 30)

        await p.execute(
            "UPDATE ops.oauth_clients SET last_used = now() WHERE client_id = $1",
            client.client_id,
        )

        log.info("Refreshed tokens for client %s", client.client_id)
        return OAuthToken(
            access_token=new_access,
            token_type="Bearer",
            expires_in=expires_in,
            scope=" ".join(use_scopes) if use_scopes else None,
            refresh_token=new_refresh,
        )

    # ── Revocation ───────────────────────────────────────────

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        await self._ensure_schema()
        p = await get_pool()
        await p.execute("DELETE FROM ops.oauth_access_tokens WHERE token = $1 OR client_id = $2", token.token, token.client_id)
        await p.execute("DELETE FROM ops.oauth_refresh_tokens WHERE token = $1 OR client_id = $2", token.token, token.client_id)
        log.info("Revoked tokens for client %s", token.client_id)


# ══════════════════════════════════════════════════════════════
#  LIFESPAN — initialize pool + ensure OAuth schema
# ══════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict]:
    log.info("WDWS MCP Server v2 starting up...")
    p = await get_pool()
    await p.execute(OAUTH_SCHEMA_SQL)
    log.info("OAuth schema ready, pool connected")
    try:
        yield {}
    finally:
        await close_pool()
        log.info("WDWS MCP Server v2 shut down")


# ══════════════════════════════════════════════════════════════
#  CREATE THE SERVER
# ══════════════════════════════════════════════════════════════

oauth_provider = PostgresOAuthProvider()

mcp = FastMCP(
    name="WDWS Legal & Medical Platform",
    instructions="""You are Athena AI, an elite Legal Analyst and Investigator connected to the WDWS 
Enterprise Legal & Medical Database. You serve as William Nelson's dedicated legal research 
assistant with expertise in civil litigation, employment law, and insurance disputes.

═══════════════════════════════════════════════════════════════════════════════════════════
DATABASE TECHNOLOGY
═══════════════════════════════════════════════════════════════════════════════════════════
**PostgreSQL 17** - NOT SQLite!
• When writing SQL queries, use PostgreSQL syntax (NOT SQLite syntax)
• PostgreSQL-specific features available: JSONB, arrays, CTEs, window functions, pgvector
• Full-text search: Use PostgreSQL's tsvector/tsquery (NOT SQLite FTS)
• Schema-qualified table names required (e.g., legal.cases, core.documents)
• For SQL queries, use execute_sql() but prefer specialized tools when available

═══════════════════════════════════════════════════════════════════════════════════════════
IDENTITY & EXPERTISE
═══════════════════════════════════════════════════════════════════════════════════════════
You are a Senior Legal Analyst and Investigative Specialist with deep expertise in:
• Civil litigation procedure and strategy
• Employment law (wrongful termination, discrimination, retaliation)
• Insurance bad faith and ERISA claims  
• Evidence analysis and timeline reconstruction
• Legal document review and pattern recognition
• Deposition and discovery analysis

═══════════════════════════════════════════════════════════════════════════════════════════
INVESTIGATION METHODOLOGY
═══════════════════════════════════════════════════════════════════════════════════════════
1. **START EVERY SESSION**: Call get_agent_context('athena') to load your memories and context
2. **DOCUMENT EVERYTHING**: Use create_note() to record findings, questions, and tasks
3. **WORK ITERATIVELY**: Break complex analysis into steps, documenting progress
4. **USE BULK TOOLS**: Prefer get_case_report() and get_all_cases_summary() over multiple queries
5. **TRACK INVESTIGATIONS**: Use get_investigation_summary() to see status and open items
6. **PERSIST KNOWLEDGE**: Use save_memory() for insights that should survive across sessions

═══════════════════════════════════════════════════════════════════════════════════════════
NOTE TYPES FOR DOCUMENTATION
═══════════════════════════════════════════════════════════════════════════════════════════
• 'finding' - Evidence or facts discovered during analysis
• 'task' - Action items that need follow-up
• 'question' - Unresolved questions needing research
• 'evidence' - Specific evidence relevant to a case
• 'analysis' - Your analytical conclusions and reasoning

═══════════════════════════════════════════════════════════════════════════════════════════
WORKFLOW EXAMPLE
═══════════════════════════════════════════════════════════════════════════════════════════
Session Start:
  1. get_agent_context('athena') → Load memories and profile
  2. get_investigation_summary('athena') → See open tasks and recent work

During Investigation:
  3. get_case_report(case_number) → Get full case details
  4. rag_query/semantic_search → Find relevant documents
  5. create_note(..., note_type='finding') → Document what you discover
  6. create_note(..., note_type='task') → Track follow-up items

Session End:
  7. save_memory('athena', 'session_summary', {...}) → Persist key insights

═══════════════════════════════════════════════════════════════════════════════════════════
AVAILABLE DATA
═══════════════════════════════════════════════════════════════════════════════════════════
• Court filings, emails, attachments across multiple active cases
• Medical records and correspondence
• Email archives (inbox triage and categorization)
• Embedded document chunks for semantic search

═══════════════════════════════════════════════════════════════════════════════════════════
CORE PRINCIPLES
═══════════════════════════════════════════════════════════════════════════════════════════
• NEVER fabricate information - if unsure, search for evidence
• ALWAYS document reasoning when making analytical conclusions
• PRIORITIZE accuracy over speed - double-check important facts
• MAINTAIN attorney-client privilege awareness in all communications
• FLAG potential deadline issues immediately with high-priority notes""",
    auth_server_provider=oauth_provider,
    auth=AuthSettings(
        issuer_url=BASE_URL,
        resource_server_url=BASE_URL,
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["read", "write", "admin", "search", "medical"],
            default_scopes=["read", "search"],
        ),
        revocation_options=RevocationOptions(enabled=True),
        required_scopes=["read"],
    ),
    host="0.0.0.0",
    port=9200,
    streamable_http_path="/mcp/sse",
    lifespan=lifespan,
)


# ══════════════════════════════════════════════════════════════
#  SEARCH TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(name="search", annotations=READ_ONLY)
@logged_tool
async def search(query: str) -> dict:
    """Search for documents (ChatGPT connector-compatible).

    Returns an object with a single key "results" containing items with:
    id, title, url.

    Args:
        query: Search query text
    """
    limit = 10
    p = await get_pool()
    try:
        vec = await _embed_query(query)
        vec_str = "[" + ",".join(str(v) for v in vec) + "]"

        rows = await p.fetch("""
            SELECT c.document_id, d.title, d.filename, d.source_path,
                   d.domain, d.document_type,
                   (c.embedding::halfvec(3072) <=> $1::halfvec(3072)) AS distance
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.embedding IS NOT NULL
            ORDER BY c.embedding::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT $2
        """, vec_str, limit * 3)

        seen_docs: set[str] = set()
        results = []
        for r in rows:
            doc_id = str(r["document_id"])
            if doc_id in seen_docs:
                continue
            seen_docs.add(doc_id)

            title = r["title"] or r["filename"] or f"Document {doc_id}"
            source_path = r["source_path"]
            url = source_path or f"{BASE_URL}/documents/{doc_id}"

            results.append({
                "id": doc_id,
                "title": title,
                "url": url,
            })
            if len(results) >= limit:
                break

        return {"results": results}
    except Exception as e:
        log.error("search error: %s", e)
        return {"error": str(e)}


@mcp.tool(name="fetch", annotations=READ_ONLY)
@logged_tool
async def fetch(id: str) -> dict:
    """Fetch full content for a document (ChatGPT connector-compatible).

    Args:
        id: Document ID from search results
    """
    p = await get_pool()
    try:
        doc = await p.fetchrow(
            "SELECT * FROM core.documents WHERE id = $1::uuid",
            id,
        )
        if not doc:
            return {"error": f"Document {id} not found"}

        title = doc["title"] or doc["filename"] or f"Document {id}"
        source_path = doc["source_path"]
        url = source_path or f"{BASE_URL}/documents/{id}"

        return {
            "id": str(doc["id"]),
            "title": title,
            "text": doc["full_content"] or "",
            "url": url,
            "metadata": {
                "domain": doc["domain"],
                "document_type": doc["document_type"],
                "source_path": doc["source_path"],
                "created_at": _ser(doc["created_at"]),
                "updated_at": _ser(doc["updated_at"]),
            },
        }
    except Exception as e:
        log.error("fetch error: %s", e)
        return {"error": str(e)}

@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def semantic_search(query: str, domain: Optional[str] = None,
                          case_number: Optional[str] = None, limit: int = 20) -> str:
    """PRIMARY TOOL for finding documents. Uses AI vector similarity to find relevant content.

    USE THIS (not SQL) to find documents matching natural language queries like:
    - "emergency motion in firefighter case"
    - "emails about settlement"
    - "medical records from 2024"

    Returns the most relevant documents ranked by semantic similarity.

    Args:
        query: Natural language search query (describe what you're looking for)
        domain: Filter by domain (legal, medical, paperless) or omit for all
        case_number: Filter to a specific case (partial match, e.g. "26-2-00762")
        limit: Max results (1-50, default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        vec = await _embed_query(query)
        vec_str = "[" + ",".join(str(v) for v in vec) + "]"

        # Build dynamic WHERE clause
        conditions = []
        params: list[Any] = [vec_str]
        idx = 2

        if domain:
            conditions.append(f"d.domain = ${idx}")
            params.append(domain)
            idx += 1
        if case_number:
            conditions.append(f"""d.id IN (
                SELECT cd.document_id FROM legal.case_documents cd
                JOIN legal.cases lc ON cd.case_id = lc.id
                WHERE lc.case_number ILIKE ${idx}
            )""")
            params.append(f"%{case_number}%")
            idx += 1

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        params.append(limit * 3)  # fetch extra to deduplicate

        # Fetch top chunks, then deduplicate to best-per-document
        rows = await p.fetch(f"""
            SELECT c.id AS chunk_id, d.id AS doc_id, d.title, d.filename,
                   d.domain, d.document_type, d.source_path,
                   c.content, c.chunk_index,
                   (c.embedding::halfvec(3072) <=> $1::halfvec(3072)) AS distance,
                   em.sender, em.subject, em.date_sent
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            {where}
            ORDER BY c.embedding::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT ${idx}
        """, *params)

        # Deduplicate: keep best chunk per document
        seen_docs: dict[str, dict] = {}
        for r in rows:
            doc_id = str(r["doc_id"])
            if doc_id not in seen_docs:
                entry = {
                    "document_id": doc_id,
                    "title": r["title"] or r["filename"],
                    "domain": r["domain"],
                    "type": r["document_type"],
                    "similarity": round(1 - r["distance"], 4),
                    "chunk_index": r["chunk_index"],
                    "content_preview": r["content"][:500],
                    "source_path": r["source_path"],
                }
                if r["sender"]:
                    entry["sender"] = r["sender"]
                    entry["subject"] = r["subject"]
                    entry["date_sent"] = _ser(r["date_sent"])
                seen_docs[doc_id] = entry
                if len(seen_docs) >= limit:
                    break

        results = list(seen_docs.values())
        return json.dumps({"query": query, "results": results, "count": len(results)})
    except Exception as e:
        log.error("semantic_search error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def fulltext_search(query: str, domain: Optional[str] = None, limit: int = 20) -> str:
    """Search documents using PostgreSQL full-text search (tsquery).

    Best for exact terms, names, case numbers, legal phrases.

    Args:
        query: Search terms (supports & for AND, | for OR)
        domain: Filter by domain or omit for all
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        if domain:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.source_path, d.created_at,
                       ts_rank(d.full_content_tsv, websearch_to_tsquery('english', $1)) AS rank,
                       ts_headline('english', d.full_content, websearch_to_tsquery('english', $1),
                                   'MaxWords=60, MinWords=20, StartSel=**, StopSel=**') AS headline
                FROM core.documents d
                WHERE d.full_content_tsv @@ websearch_to_tsquery('english', $1)
                  AND d.domain = $3
                ORDER BY rank DESC LIMIT $2
            """, query, limit, domain)
        else:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.source_path, d.created_at,
                       ts_rank(d.full_content_tsv, websearch_to_tsquery('english', $1)) AS rank,
                       ts_headline('english', d.full_content, websearch_to_tsquery('english', $1),
                                   'MaxWords=60, MinWords=20, StartSel=**, StopSel=**') AS headline
                FROM core.documents d
                WHERE d.full_content_tsv @@ websearch_to_tsquery('english', $1)
                ORDER BY rank DESC LIMIT $2
            """, query, limit)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "rank": round(float(r["rank"]), 4),
            "headline": r["headline"],
            "created_at": _ser(r["created_at"]),
        } for r in rows]

        return json.dumps({"query": query, "results": results, "count": len(results)})
    except Exception as e:
        log.error("fulltext_search error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_emails(
    query: Optional[str] = None,
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    case_number: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    has_attachments: Optional[bool] = None,
    limit: int = 20,
) -> str:
    """Search email messages with flexible filters.

    Args:
        query: Full-text search in email content and subject
        sender: Filter by sender email/name (partial match)
        recipient: Filter by recipient (partial match)
        case_number: Filter by associated case number
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        has_attachments: Filter emails with/without attachments
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        conditions = ["d.document_type = 'email'"]
        params: list[Any] = []
        idx = 1

        if query:
            conditions.append(f"d.full_content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query)
            idx += 1
        if sender:
            sender = sender.strip()
            sender_terms = [sender]
            if "," not in sender:
                parts = [p for p in sender.split() if p]
                if len(parts) >= 2:
                    sender_terms.append(f"{parts[-1]}, {' '.join(parts[:-1])}")

            if len(sender_terms) == 1:
                conditions.append(f"em.sender ILIKE ${idx}")
                params.append(f"%{sender_terms[0]}%")
                idx += 1
            else:
                term_conditions = []
                for term in sender_terms:
                    term_conditions.append(f"em.sender ILIKE ${idx}")
                    params.append(f"%{term}%")
                    idx += 1
                conditions.append("(" + " OR ".join(term_conditions) + ")")
        if recipient:
            conditions.append(f"(em.recipients ILIKE ${idx} OR em.cc ILIKE ${idx})")
            params.append(f"%{recipient}%")
            idx += 1
        if case_number:
            conditions.append(f"""d.id IN (
                SELECT cd.document_id FROM legal.case_documents cd
                JOIN legal.cases c ON cd.case_id = c.id
                WHERE c.case_number ILIKE ${idx}
            )""")
            params.append(f"%{case_number}%")
            idx += 1
        if start_date:
            conditions.append(f"em.date_sent >= ${idx}::timestamptz")
            params.append(start_date)
            idx += 1
        if end_date:
            conditions.append(f"em.date_sent <= ${idx}::timestamptz + interval '1 day'")
            params.append(end_date)
            idx += 1
        if has_attachments is not None:
            conditions.append(f"em.has_attachments = ${idx}")
            params.append(has_attachments)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, d.source_path, d.created_at,
                   em.sender, em.recipients, em.cc, em.subject,
                   em.date_sent, em.direction, em.has_attachments, em.thread_id,
                   LEFT(d.full_content, 500) AS preview
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE {where}
            ORDER BY em.date_sent DESC NULLS LAST
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "subject": r["subject"],
            "sender": r["sender"],
            "recipients": r["recipients"],
            "cc": r["cc"],
            "date_sent": _ser(r["date_sent"]),
            "direction": r["direction"],
            "has_attachments": r["has_attachments"],
            "thread_id": r["thread_id"],
            "preview": r["preview"],
        } for r in rows]

        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_emails error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_by_case(case_number: str, document_type: Optional[str] = None, limit: int = 50) -> str:
    """Get all documents for a specific legal case.

    Args:
        case_number: Case number (partial match OK, e.g. "26-2-00762")
        document_type: Filter by type (email, court_filing, email_attachment)
        limit: Max results (default 50)
    """
    limit = min(max(limit, 1), 100)
    p = await get_pool()
    try:
        case_row = await p.fetchrow(
            "SELECT * FROM legal.cases WHERE case_number ILIKE $1",
            f"%{case_number}%",
        )

        conditions = ["c.case_number ILIKE $1"]
        params: list[Any] = [f"%{case_number}%"]
        idx = 2

        if document_type:
            conditions.append(f"d.document_type = ${idx}")
            params.append(document_type)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                   d.created_at, d.source_path,
                   em.sender, em.date_sent, em.subject,
                   fm.filing_type, fm.filed_by, fm.date_ingested
            FROM core.documents d
            JOIN legal.case_documents cd ON d.id = cd.document_id
            JOIN legal.cases c ON cd.case_id = c.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN legal.filing_metadata fm ON d.id = fm.document_id
            WHERE {where}
            ORDER BY COALESCE(em.date_sent, d.created_at) DESC
            LIMIT ${idx}
        """, *params)

        case_info = None
        if case_row:
            case_info = {
                "case_number": case_row["case_number"],
                "case_title": case_row["case_title"],
                "court": case_row["court"],
                "status": case_row["status"],
                "date_filed": _ser(case_row["date_filed"]),
                "jurisdiction": case_row["jurisdiction"],
            }

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "type": r["document_type"],
            "created_at": _ser(r["created_at"]),
            "sender": r["sender"],
            "date_sent": _ser(r["date_sent"]),
            "subject": r["subject"],
            "filing_type": r["filing_type"],
            "filed_by": r["filed_by"],
            "date_ingested": _ser(r["date_ingested"]),
        } for r in rows]

        return json.dumps({"case": case_info, "documents": results, "count": len(results)})
    except Exception as e:
        log.error("search_by_case error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def case_timeline(case_number: str, start_date: Optional[str] = None,
                        end_date: Optional[str] = None, limit: int = 50) -> str:
    """Get chronological timeline of ALL activity for a case — emails, filings,
    attachments — with sender, subject, and content preview. Essential for
    understanding what happened and when in a case.

    Args:
        case_number: Case number (partial match, e.g. "26-2-00762")
        start_date: Start date filter (YYYY-MM-DD)
        end_date: End date filter (YYYY-MM-DD)
        limit: Max results (default 50)
    """
    limit = min(max(limit, 1), 200)
    p = await get_pool()
    try:
        conditions = ["c.case_number ILIKE $1"]
        params: list[Any] = [f"%{case_number}%"]
        idx = 2

        if start_date:
            conditions.append(f"COALESCE(em.date_sent, d.created_at) >= ${idx}::timestamptz")
            params.append(start_date)
            idx += 1
        if end_date:
            conditions.append(f"COALESCE(em.date_sent, d.created_at) <= ${idx}::timestamptz + interval '1 day'")
            params.append(end_date)
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, d.document_type,
                   em.sender, em.recipients, em.cc, em.date_sent, em.subject,
                   em.direction, em.has_attachments,
                   fm.filing_type, fm.filed_by, fm.docket_number,
                   LEFT(d.full_content, 400) AS preview,
                   COALESCE(em.date_sent, d.created_at) AS event_date
            FROM core.documents d
            JOIN legal.case_documents cd ON d.id = cd.document_id
            JOIN legal.cases c ON cd.case_id = c.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN legal.filing_metadata fm ON d.id = fm.document_id
            WHERE {where}
            ORDER BY event_date ASC
            LIMIT ${idx}
        """, *params)

        events = []
        for r in rows:
            evt = {
                "document_id": str(r["id"]),
                "date": _ser(r["event_date"]),
                "type": r["document_type"],
                "title": r["title"] or r["filename"],
            }
            if r["document_type"] == "email":
                evt["sender"] = r["sender"]
                evt["recipients"] = r["recipients"]
                evt["subject"] = r["subject"]
                evt["direction"] = r["direction"]
                evt["has_attachments"] = r["has_attachments"]
            elif r["document_type"] == "court_filing":
                evt["filing_type"] = r["filing_type"]
                evt["filed_by"] = r["filed_by"]
            evt["preview"] = r["preview"]
            events.append(evt)

        return json.dumps({
            "case_number": case_number,
            "timeline": events,
            "count": len(events),
            "instruction": "This is a chronological timeline. Earlier events are first. Pay attention to sender and direction to distinguish incoming court responses from outgoing filings."
        })
    except Exception as e:
        log.error("case_timeline error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_medical(
    query: Optional[str] = None,
    record_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    provider: Optional[str] = None,
    facility: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search medical records with flexible filters.

    Args:
        query: Full-text search in medical record content
        record_type: Filter by record type (e.g. clinical_note, lab_result, imaging)
        start_date: Service date start (YYYY-MM-DD)
        end_date: Service date end (YYYY-MM-DD)
        provider: Filter by provider name (partial match)
        facility: Filter by facility name (partial match)
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        conditions = ["d.domain = 'medical'"]
        params: list[Any] = []
        idx = 1

        if query:
            conditions.append(f"d.full_content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query)
            idx += 1
        if record_type:
            conditions.append(f"rm.record_type = ${idx}")
            params.append(record_type)
            idx += 1
        if start_date:
            conditions.append(f"rm.date_of_service >= ${idx}::date")
            params.append(start_date)
            idx += 1
        if end_date:
            conditions.append(f"rm.date_of_service <= ${idx}::date")
            params.append(end_date)
            idx += 1
        if provider:
            conditions.append(f"prov.name ILIKE ${idx}")
            params.append(f"%{provider}%")
            idx += 1
        if facility:
            conditions.append(f"rm.facility ILIKE ${idx}")
            params.append(f"%{facility}%")
            idx += 1

        params.append(limit)
        where = " AND ".join(conditions)

        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, d.document_type, d.created_at,
                   rm.record_type, rm.date_of_service, rm.facility, rm.section_title,
                   prov.name AS provider_name,
                   LEFT(d.full_content, 500) AS preview
            FROM core.documents d
            LEFT JOIN medical.record_metadata rm ON d.id = rm.document_id
            LEFT JOIN medical.providers prov ON rm.provider_id = prov.id
            WHERE {where}
            ORDER BY rm.date_of_service DESC NULLS LAST
            LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "record_type": r["record_type"],
            "date_of_service": _ser(r["date_of_service"]),
            "facility": r["facility"],
            "provider": r["provider_name"],
            "section": r["section_title"],
            "preview": r["preview"],
        } for r in rows]

        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_medical error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_entities(
    name: Optional[str] = None,
    entity_type: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search for entities (people, organizations, courts, etc.) mentioned in documents.

    Args:
        name: Entity name to search for (partial match with trigram similarity)
        entity_type: Filter by type (person, organization, court, location, etc.)
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        conditions = []
        params: list[Any] = []
        idx = 1

        if name:
            conditions.append(f"e.name ILIKE ${idx}")
            params.append(f"%{name}%")
            idx += 1
        if entity_type:
            conditions.append(f"e.entity_type = ${idx}")
            params.append(entity_type)
            idx += 1

        params.append(limit)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""

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
            "mentions": r["mention_count"],
            "metadata": r["metadata"],
        } for r in rows]

        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_entities error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_by_date_range(
    start_date: str,
    end_date: str,
    domain: Optional[str] = None,
    document_type: Optional[str] = None,
    limit: int = 50,
) -> str:
    """Search documents by date range.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        domain: Filter by domain (legal, medical, paperless)
        document_type: Filter by type (email, court_filing, email_attachment, note, etc.)
        limit: Max results (default 50)
    """
    limit = min(max(limit, 1), 100)
    p = await get_pool()
    try:
        conditions = ["d.created_at >= $1::timestamptz", "d.created_at <= $2::timestamptz + interval '1 day'"]
        params: list[Any] = [start_date, end_date]
        idx = 3

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
            SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                   d.created_at, d.source_path,
                   em.sender, em.date_sent, em.subject
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE {where}
            ORDER BY d.created_at DESC LIMIT ${idx}
        """, *params)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "created_at": _ser(r["created_at"]),
            "sender": r["sender"],
            "subject": r["subject"],
            "date_sent": _ser(r["date_sent"]),
        } for r in rows]

        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        log.error("search_by_date_range error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  READ / FETCH TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def fetch_document(document_id: str, include_chunks: bool = False) -> str:
    """Fetch a document's full content and metadata by ID.

    Args:
        document_id: UUID of the document
        include_chunks: Whether to include all chunk texts (default False)
    """
    p = await get_pool()
    try:
        doc = await p.fetchrow(
            "SELECT * FROM core.documents WHERE id = $1::uuid", document_id
        )
        if not doc:
            return json.dumps({"error": f"Document {document_id} not found"})

        result = {
            "document_id": str(doc["id"]),
            "domain": doc["domain"],
            "title": doc["title"],
            "filename": doc["filename"],
            "document_type": doc["document_type"],
            "source_path": doc["source_path"],
            "content_hash": doc["content_hash"],
            "total_chunks": doc["total_chunks"],
            "metadata": doc["metadata"],
            "created_at": _ser(doc["created_at"]),
            "updated_at": _ser(doc["updated_at"]),
            "full_content": doc["full_content"],
        }

        em = await p.fetchrow(
            "SELECT * FROM legal.email_metadata WHERE document_id = $1::uuid", document_id
        )
        if em:
            result["email"] = {
                "sender": em["sender"], "recipients": em["recipients"],
                "cc": em["cc"], "subject": em["subject"],
                "date_sent": _ser(em["date_sent"]), "direction": em["direction"],
                "thread_id": em["thread_id"], "has_attachments": em["has_attachments"],
            }

        fm = await p.fetchrow(
            "SELECT * FROM legal.filing_metadata WHERE document_id = $1::uuid", document_id
        )
        if fm:
            result["filing"] = {
                "filing_type": fm["filing_type"], "filed_by": fm["filed_by"],
                "date_ingested": _ser(fm["date_ingested"]),
                "docket_number": fm["docket_number"],
            }

        tags = await p.fetch("""
            SELECT t.name, t.category FROM core.tags t
            JOIN core.document_tags dt ON t.id = dt.tag_id
            WHERE dt.document_id = $1::uuid
        """, document_id)
        result["tags"] = [{"name": t["name"], "category": t["category"]} for t in tags]

        if include_chunks:
            chunks = await p.fetch("""
                SELECT chunk_index, content, token_count
                FROM core.document_chunks
                WHERE document_id = $1::uuid
                ORDER BY chunk_index
            """, document_id)
            result["chunks"] = [{"index": c["chunk_index"], "content": c["content"],
                                 "tokens": c["token_count"]} for c in chunks]

        return json.dumps(result)
    except Exception as e:
        log.error("fetch_document error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def fetch_email_thread(thread_id: str) -> str:
    """Fetch all emails in a conversation thread, ordered chronologically.

    Args:
        thread_id: The email thread ID
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT d.id, d.title, d.full_content,
                   em.sender, em.recipients, em.cc, em.subject,
                   em.date_sent, em.direction, em.has_attachments
            FROM core.documents d
            JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE em.thread_id = $1
            ORDER BY em.date_sent ASC
        """, thread_id)

        messages = [{
            "document_id": str(r["id"]),
            "subject": r["subject"],
            "sender": r["sender"],
            "recipients": r["recipients"],
            "cc": r["cc"],
            "date_sent": _ser(r["date_sent"]),
            "direction": r["direction"],
            "has_attachments": r["has_attachments"],
            "content": r["full_content"],
        } for r in rows]

        return json.dumps({"thread_id": thread_id, "messages": messages, "count": len(messages)})
    except Exception as e:
        log.error("fetch_email_thread error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def fetch_case_details(case_number: str) -> str:
    """Fetch detailed information about a legal case including parties and document counts.

    Args:
        case_number: Case number (partial match OK)
    """
    p = await get_pool()
    try:
        case = await p.fetchrow(
            "SELECT * FROM legal.cases WHERE case_number ILIKE $1",
            f"%{case_number}%",
        )
        if not case:
            return json.dumps({"error": f"Case matching '{case_number}' not found"})

        doc_counts = await p.fetch("""
            SELECT d.document_type, COUNT(*) AS cnt
            FROM core.documents d
            JOIN legal.case_documents cd ON d.id = cd.document_id
            WHERE cd.case_id = $1
            GROUP BY d.document_type ORDER BY cnt DESC
        """, case["id"])

        parties = await p.fetch("""
            SELECT cp.role, p.name, p.party_type, p.metadata
            FROM legal.case_parties cp
            JOIN legal.parties p ON cp.party_id = p.id
            WHERE cp.case_id = $1
        """, case["id"])

        result = {
            "case_number": case["case_number"],
            "case_title": case["case_title"],
            "court": case["court"],
            "case_type": case["case_type"],
            "status": case["status"],
            "jurisdiction": case["jurisdiction"],
            "date_filed": _ser(case["date_filed"]),
            "date_closed": _ser(case["date_closed"]),
            "metadata": case["metadata"],
            "document_counts": {r["document_type"]: r["cnt"] for r in doc_counts},
            "total_documents": sum(r["cnt"] for r in doc_counts),
            "parties": [{"role": p["role"], "name": p["name"], "type": p["party_type"]}
                        for p in parties],
        }
        return json.dumps(result)
    except Exception as e:
        log.error("fetch_case_details error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def fetch_attachments(email_document_id: str) -> str:
    """Fetch all attachments for a specific email.

    Args:
        email_document_id: UUID of the email document
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT ea.id, ea.filename, ea.content_type, ea.file_size,
                   ea.extracted_text, ea.extraction_method, ea.is_processed,
                   ea.child_doc_id
            FROM legal.email_attachments ea
            WHERE ea.email_doc_id = $1::uuid
            ORDER BY ea.filename
        """, email_document_id)

        results = [{
            "attachment_id": str(r["id"]),
            "filename": r["filename"],
            "content_type": r["content_type"],
            "file_size": r["file_size"],
            "is_processed": r["is_processed"],
            "extraction_method": r["extraction_method"],
            "child_document_id": str(r["child_doc_id"]) if r["child_doc_id"] else None,
            "extracted_text_preview": r["extracted_text"][:500] if r["extracted_text"] else None,
        } for r in rows]

        return json.dumps({"email_document_id": email_document_id, "attachments": results, "count": len(results)})
    except Exception as e:
        log.error("fetch_attachments error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_cases(status: Optional[str] = None) -> str:
    """List all legal cases with document counts.

    Args:
        status: Filter by status (active, closed, pending) or omit for all
    """
    p = await get_pool()
    try:
        if status:
            rows = await p.fetch("""
                SELECT c.*, COUNT(cd.document_id) AS doc_count
                FROM legal.cases c
                LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
                WHERE c.status = $1
                GROUP BY c.id ORDER BY c.date_filed DESC NULLS LAST
            """, status)
        else:
            rows = await p.fetch("""
                SELECT c.*, COUNT(cd.document_id) AS doc_count
                FROM legal.cases c
                LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
                GROUP BY c.id ORDER BY c.date_filed DESC NULLS LAST
            """)

        results = [{
            "case_id": r["id"],
            "case_number": r["case_number"],
            "case_title": r["case_title"],
            "court": r["court"],
            "status": r["status"],
            "date_filed": _ser(r["date_filed"]),
            "document_count": r["doc_count"],
        } for r in rows]

        return json.dumps({"cases": results, "count": len(results)})
    except Exception as e:
        log.error("list_cases error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_filings(
    case_number: Optional[str] = None,
    filing_type: Optional[str] = None,
    docket_number: Optional[str] = None,
    limit: int = 50
) -> str:
    """Search court filing metadata (docket entries) for a case.

    Returns structured filing information including docket numbers, filing types,
    and associated document IDs.

    NOTE: date_ingested is when the document was added to the database, NOT the
    actual court filing date. Use docket_number order or document content for
    chronological ordering.

    Args:
        case_number: Filter by case number (e.g., "24-2-01031-31") — required unless docket_number given
        filing_type: Filter by filing type substring (e.g., "Motion", "Order", "Declaration")
        docket_number: Filter by specific docket number (e.g., "50")
        limit: Max results (default 50)
    """
    p = await get_pool()
    limit = min(max(limit, 1), 200)
    try:
        conditions = []
        params: list[Any] = []
        idx = 1

        if case_number:
            conditions.append(f"c.case_number ILIKE ${idx}")
            params.append(f"%{case_number}%")
            idx += 1

        if filing_type:
            conditions.append(f"fm.filing_type ILIKE ${idx}")
            params.append(f"%{filing_type}%")
            idx += 1

        if docket_number:
            conditions.append(f"fm.docket_number = ${idx}")
            params.append(docket_number)
            idx += 1

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        rows = await p.fetch(f"""
            SELECT
                fm.document_id,
                fm.docket_number,
                fm.filing_type,
                fm.filed_by,
                fm.date_ingested,
                fm.court_document_number,
                c.case_number,
                c.case_title,
                d.title as document_title
            FROM legal.filing_metadata fm
            JOIN legal.case_documents cd ON cd.document_id = fm.document_id
            JOIN legal.cases c ON c.id = cd.case_id
            JOIN core.documents d ON d.id = fm.document_id
            WHERE {where_clause}
            ORDER BY c.case_number, fm.docket_number::int NULLS LAST
            LIMIT ${idx}
        """, *params, limit)

        results = [{
            "document_id": str(r["document_id"]),
            "docket_number": r["docket_number"],
            "filing_type": r["filing_type"],
            "filed_by": r["filed_by"],
            "date_ingested": _ser(r["date_ingested"]),
            "court_document_number": r["court_document_number"],
            "case_number": r["case_number"],
            "case_title": r["case_title"],
            "document_title": r["document_title"],
        } for r in rows]

        return json.dumps({"filings": results, "count": len(results)})
    except Exception as e:
        log.error("search_filings error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  RAG (Retrieval-Augmented Generation) TOOL
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def rag_query(question: str, domain: Optional[str] = None,
                    case_number: Optional[str] = None, top_k: int = 15) -> str:
    """PRIMARY TOOL for answering questions. ALWAYS USE THIS instead of execute_sql.

    Uses hybrid RAG (vector + full-text search) to find the most relevant context
    for answering questions about cases, emails, filings, medical records, etc.

    Examples - USE rag_query for:
    - "What happened in the Starbucks case?"
    - "Who are the parties in case 24-2-01031?"
    - "What motions were filed?"
    - "Summarize the FTCU litigation"

    Returns relevant document chunks that contain the answer.

    Args:
        question: The question to answer (natural language)
        domain: Restrict to domain (legal, medical, paperless) or omit for all
        case_number: Filter to a specific case (partial match, e.g. "26-2-00762")
        top_k: Number of context chunks to retrieve (default 15)
    """
    top_k = min(max(top_k, 1), 30)
    p = await get_pool()
    try:
        vec = await _embed_query(question)
        vec_str = "[" + ",".join(str(v) for v in vec) + "]"

        # Build WHERE clauses
        sem_conditions = []
        ft_conditions = []
        sem_params: list[Any] = [vec_str]
        ft_params: list[Any] = [question]
        sem_idx = 2
        ft_idx = 2

        if domain:
            sem_conditions.append(f"d.domain = ${sem_idx}")
            sem_params.append(domain)
            sem_idx += 1
            ft_conditions.append(f"d.domain = ${ft_idx}")
            ft_params.append(domain)
            ft_idx += 1
        if case_number:
            case_filter = """d.id IN (
                SELECT cd.document_id FROM legal.case_documents cd
                JOIN legal.cases lc ON cd.case_id = lc.id
                WHERE lc.case_number ILIKE ${idx}
            )"""
            sem_conditions.append(case_filter.replace("${idx}", f"${sem_idx}"))
            sem_params.append(f"%{case_number}%")
            sem_idx += 1
            ft_conditions.append(case_filter.replace("${idx}", f"${ft_idx}"))
            ft_params.append(f"%{case_number}%")
            ft_idx += 1

        sem_where = (" AND " + " AND ".join(sem_conditions)) if sem_conditions else ""
        ft_where = (" AND " + " AND ".join(ft_conditions)) if ft_conditions else ""
        fetch_count = top_k * 3  # over-fetch for merging

        sem_params.append(fetch_count)
        ft_params.append(fetch_count)

        # 1) Semantic search
        sem_rows = await p.fetch(f"""
            SELECT c.id AS chunk_id, c.document_id, c.content, c.chunk_index,
                   d.title, d.filename, d.document_type, d.domain,
                   (c.embedding::halfvec(3072) <=> $1::halfvec(3072)) AS distance,
                   em.sender, em.date_sent, em.subject
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE c.embedding IS NOT NULL {sem_where}
            ORDER BY c.embedding::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT ${sem_idx}
        """, *sem_params)

        # 2) Full-text search on chunks
        ft_rows = await p.fetch(f"""
            SELECT c.id AS chunk_id, c.document_id, c.content, c.chunk_index,
                   d.title, d.filename, d.document_type, d.domain,
                   ts_rank(c.content_tsv, websearch_to_tsquery('english', $1)) AS ft_rank,
                   em.sender, em.date_sent, em.subject
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', $1) {ft_where}
            ORDER BY ft_rank DESC
            LIMIT ${ft_idx}
        """, *ft_params)

        # 3) Reciprocal Rank Fusion (RRF) with k=60
        rrf_k = 60
        chunk_scores: dict[str, float] = {}
        chunk_data: dict[str, dict] = {}

        for rank, r in enumerate(sem_rows):
            cid = r["chunk_id"]
            chunk_scores[cid] = chunk_scores.get(cid, 0) + 1.0 / (rrf_k + rank + 1)
            if cid not in chunk_data:
                chunk_data[cid] = dict(r)

        for rank, r in enumerate(ft_rows):
            cid = r["chunk_id"]
            chunk_scores[cid] = chunk_scores.get(cid, 0) + 1.0 / (rrf_k + rank + 1)
            if cid not in chunk_data:
                chunk_data[cid] = dict(r)

        # Sort by RRF score, deduplicate per document, take top_k
        sorted_chunks = sorted(chunk_scores.items(), key=lambda x: x[1], reverse=True)
        seen_docs: set[str] = set()
        contexts = []
        for cid, score in sorted_chunks:
            if len(contexts) >= top_k:
                break
            r = chunk_data[cid]
            doc_id = str(r["document_id"])
            # Allow up to 2 chunks per document for thread context
            doc_count = sum(1 for c in contexts if c.get("_doc_id") == doc_id)
            if doc_count >= 2:
                continue

            ctx = f"[{r['domain']}/{r['document_type']}] {r['title'] or r['filename']}"
            if r.get("sender"):
                ctx += f"\nFrom: {r['sender']} | Date: {_ser(r['date_sent'])} | Subject: {r['subject']}"
            ctx += f"\n\n{r['content']}"
            contexts.append({
                "source": r["title"] or r["filename"],
                "type": r["document_type"],
                "domain": r["domain"],
                "rrf_score": round(score, 6),
                "text": ctx,
                "_doc_id": doc_id,
            })

        # Remove internal field
        for c in contexts:
            c.pop("_doc_id", None)

        return json.dumps({
            "question": question,
            "context_chunks": contexts,
            "count": len(contexts),
            "search_method": "hybrid_rrf (semantic + fulltext)",
            "instruction": "Use the above context chunks to answer the question. Cite sources. Pay close attention to email sender, date, and subject to distinguish replies from original messages.",
        })
    except Exception as e:
        log.error("rag_query error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  WRITE / MODIFY TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def add_tag(document_id: str, tag_name: str, category: Optional[str] = None) -> str:
    """Add a tag to a document. Creates the tag if it doesn't exist.

    Args:
        document_id: UUID of the document to tag
        tag_name: Tag name (e.g. "important", "follow-up", "privileged")
        category: Optional tag category (e.g. "priority", "status", "classification")
    """
    p = await get_pool()
    try:
        tag = await p.fetchrow("""
            INSERT INTO core.tags (name, category)
            VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET category = COALESCE(EXCLUDED.category, core.tags.category)
            RETURNING id
        """, tag_name.lower().strip(), category)

        await p.execute("""
            INSERT INTO core.document_tags (document_id, tag_id)
            VALUES ($1::uuid, $2)
            ON CONFLICT DO NOTHING
        """, document_id, tag["id"])

        return json.dumps({"success": True, "document_id": document_id, "tag": tag_name, "tag_id": tag["id"]})
    except Exception as e:
        log.error("add_tag error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def remove_tag(document_id: str, tag_name: str) -> str:
    """Remove a tag from a document.

    Args:
        document_id: UUID of the document
        tag_name: Tag name to remove
    """
    p = await get_pool()
    try:
        await p.execute("""
            DELETE FROM core.document_tags
            WHERE document_id = $1::uuid
              AND tag_id = (SELECT id FROM core.tags WHERE name = $2)
        """, document_id, tag_name.lower().strip())

        return json.dumps({"success": True, "document_id": document_id, "tag_removed": tag_name})
    except Exception as e:
        log.error("remove_tag error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_tags(document_id: Optional[str] = None) -> str:
    """List all tags, or tags for a specific document.

    Args:
        document_id: Optional UUID — if provided, lists tags for that document only
    """
    p = await get_pool()
    try:
        if document_id:
            rows = await p.fetch("""
                SELECT t.id, t.name, t.category
                FROM core.tags t
                JOIN core.document_tags dt ON t.id = dt.tag_id
                WHERE dt.document_id = $1::uuid
                ORDER BY t.name
            """, document_id)
        else:
            rows = await p.fetch("""
                SELECT t.id, t.name, t.category, COUNT(dt.document_id) AS doc_count
                FROM core.tags t
                LEFT JOIN core.document_tags dt ON t.id = dt.tag_id
                GROUP BY t.id ORDER BY doc_count DESC
            """)

        results = [_row_dict(r) for r in rows]
        return json.dumps({"tags": results, "count": len(results)})
    except Exception as e:
        log.error("list_tags error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def create_note(
    title: str,
    content: str,
    domain: str = "legal",
    case_number: Optional[str] = None,
    tags: Optional[str] = None,
) -> str:
    """Create a new note/document in the database.

    Args:
        title: Note title
        content: Note content (text/markdown)
        domain: Domain (legal, medical, paperless) — default legal
        case_number: Optional case number to associate with
        tags: Comma-separated tags to apply
    """
    p = await get_pool()
    try:
        doc_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        await p.execute("""
            INSERT INTO core.documents (id, domain, document_type, title, full_content, content_hash, metadata)
            VALUES ($1::uuid, $2, 'note', $3, $4, $5, $6)
        """, doc_id, domain, title, content, content_hash,
             json.dumps({"source": "mcp_server", "created_by": "ai_assistant"}))

        if case_number:
            case = await p.fetchrow(
                "SELECT id FROM legal.cases WHERE case_number ILIKE $1",
                f"%{case_number}%",
            )
            if case:
                await p.execute(
                    "INSERT INTO legal.case_documents (case_id, document_id) VALUES ($1, $2::uuid)",
                    case["id"], doc_id,
                )

        if tags:
            for tag_name in tags.split(","):
                tag_name = tag_name.strip().lower()
                if tag_name:
                    tag = await p.fetchrow("""
                        INSERT INTO core.tags (name) VALUES ($1)
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                    """, tag_name)
                    await p.execute("""
                        INSERT INTO core.document_tags (document_id, tag_id)
                        VALUES ($1::uuid, $2) ON CONFLICT DO NOTHING
                    """, doc_id, tag["id"])

        try:
            vec = await _embed_query(f"{title}\n\n{content}")
            vec_str = "[" + ",".join(str(v) for v in vec) + "]"
            chunk_id = f"{doc_id}_0"
            await p.execute("""
                INSERT INTO core.document_chunks (id, document_id, chunk_index, total_chunks, content, embedding, token_count)
                VALUES ($1, $2::uuid, 0, 1, $3, $4::halfvec(3072), $5)
            """, chunk_id, doc_id, content, vec_str, len(content.split()))
            await p.execute("UPDATE core.documents SET total_chunks = 1 WHERE id = $1::uuid", doc_id)
        except Exception as embed_err:
            log.warning("Note created but embedding failed: %s", embed_err)

        return json.dumps({"success": True, "document_id": doc_id, "title": title})
    except Exception as e:
        log.error("create_note error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def update_document_metadata(document_id: str, metadata_updates: str) -> str:
    """Update a document's metadata (JSONB merge).

    Args:
        document_id: UUID of the document
        metadata_updates: JSON string of metadata to merge (e.g. '{"priority": "high", "reviewed": true}')
    """
    p = await get_pool()
    try:
        updates = json.loads(metadata_updates)
        await p.execute("""
            UPDATE core.documents
            SET metadata = metadata || $2::jsonb
            WHERE id = $1::uuid
        """, document_id, json.dumps(updates))

        return json.dumps({"success": True, "document_id": document_id, "merged": updates})
    except json.JSONDecodeError:
        return json.dumps({"error": "Invalid JSON in metadata_updates"})
    except Exception as e:
        log.error("update_document_metadata error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def update_case_status(case_number: str, status: str, notes: Optional[str] = None) -> str:
    """Update the status of a legal case.

    Args:
        case_number: Case number
        status: New status (active, closed, pending, settled, dismissed, appealed)
        notes: Optional notes about the status change
    """
    valid_statuses = {"active", "closed", "pending", "settled", "dismissed", "appealed"}
    if status.lower() not in valid_statuses:
        return json.dumps({"error": f"Invalid status. Must be one of: {valid_statuses}"})

    p = await get_pool()
    try:
        case = await p.fetchrow(
            "SELECT * FROM legal.cases WHERE case_number ILIKE $1",
            f"%{case_number}%",
        )
        if not case:
            return json.dumps({"error": f"Case matching '{case_number}' not found"})

        old_status = case["status"]
        meta_update = {"status_history": [{"from": old_status, "to": status, "date": datetime.now(timezone.utc).isoformat()}]}
        if notes:
            meta_update["status_notes"] = notes

        await p.execute("""
            UPDATE legal.cases
            SET status = $1, metadata = metadata || $2::jsonb,
                date_closed = CASE WHEN $1 IN ('closed', 'settled', 'dismissed') THEN now()::date ELSE date_closed END
            WHERE id = $3
        """, status.lower(), json.dumps(meta_update), case["id"])

        return json.dumps({
            "success": True,
            "case_number": case["case_number"],
            "old_status": old_status,
            "new_status": status,
        })
    except Exception as e:
        log.error("update_case_status error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def create_document_relationship(
    source_document_id: str,
    target_document_id: str,
    relationship_type: str,
    confidence: float = 1.0,
) -> str:
    """Create a relationship between two documents.

    Args:
        source_document_id: UUID of the source document
        target_document_id: UUID of the target document
        relationship_type: Type (reply_to, attachment_of, references, supersedes, related_to)
        confidence: Confidence score 0.0-1.0 (default 1.0)
    """
    p = await get_pool()
    try:
        await p.execute("""
            INSERT INTO core.document_relationships (source_document_id, target_document_id, relationship_type, confidence)
            VALUES ($1::uuid, $2::uuid, $3, $4)
            ON CONFLICT (source_document_id, target_document_id, relationship_type) DO UPDATE
            SET confidence = EXCLUDED.confidence
        """, source_document_id, target_document_id, relationship_type, confidence)

        return json.dumps({
            "success": True,
            "source": source_document_id,
            "target": target_document_id,
            "relationship": relationship_type,
        })
    except Exception as e:
        log.error("create_document_relationship error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=DESTRUCTIVE)
@logged_tool
async def delete_document(document_id: str, confirm: bool = False) -> str:
    """Delete a document and all its associated data (chunks, metadata, tags).

    Args:
        document_id: UUID of the document to delete
        confirm: Must be True to actually delete (safety check)
    """
    if not confirm:
        return json.dumps({"error": "Set confirm=True to delete. This action is irreversible."})

    p = await get_pool()
    try:
        doc = await p.fetchrow("SELECT id, title, domain, document_type FROM core.documents WHERE id = $1::uuid", document_id)
        if not doc:
            return json.dumps({"error": f"Document {document_id} not found"})

        await p.execute("DELETE FROM core.documents WHERE id = $1::uuid", document_id)

        return json.dumps({
            "success": True,
            "deleted": {
                "document_id": document_id,
                "title": doc["title"],
                "domain": doc["domain"],
                "type": doc["document_type"],
            },
        })
    except Exception as e:
        log.error("delete_document error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_document_relationships(document_id: str) -> str:
    """Find all documents related to a given document (replies, attachments, references).

    Args:
        document_id: UUID of the document to find relationships for
    """
    p = await get_pool()
    try:
        outgoing = await p.fetch("""
            SELECT dr.relationship_type, dr.confidence,
                   d.id, d.title, d.filename, d.domain, d.document_type
            FROM core.document_relationships dr
            JOIN core.documents d ON dr.target_document_id = d.id
            WHERE dr.source_document_id = $1::uuid
        """, document_id)

        incoming = await p.fetch("""
            SELECT dr.relationship_type, dr.confidence,
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
                "target_id": str(r["id"]),
                "title": r["title"] or r["filename"],
                "type": r["document_type"],
            } for r in outgoing],
            "incoming": [{
                "relationship": r["relationship_type"],
                "confidence": r["confidence"],
                "source_id": str(r["id"]),
                "title": r["title"] or r["filename"],
                "type": r["document_type"],
            } for r in incoming],
        })
    except Exception as e:
        log.error("search_document_relationships error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  ADMIN / STATS TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def get_database_stats() -> str:
    """Get comprehensive database statistics — document counts, case counts, storage info."""
    p = await get_pool()
    try:
        total = await p.fetchrow("SELECT COUNT(*) AS cnt FROM core.documents")
        chunks = await p.fetchrow("SELECT COUNT(*) AS cnt FROM core.document_chunks")
        cases = await p.fetchrow("SELECT COUNT(*) AS cnt FROM legal.cases")

        by_type = await p.fetch("""
            SELECT domain || '/' || document_type AS dtype, COUNT(*) AS cnt
            FROM core.documents GROUP BY domain, document_type ORDER BY cnt DESC
        """)

        recent = await p.fetchrow("""
            SELECT MIN(created_at) AS oldest, MAX(created_at) AS newest
            FROM core.documents
        """)

        email_stats = await p.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE direction = 'inbound') AS inbound,
                   COUNT(*) FILTER (WHERE direction = 'outbound') AS outbound,
                   MIN(date_sent) AS earliest,
                   MAX(date_sent) AS latest
            FROM legal.email_metadata
        """)

        db_size = await p.fetchrow("SELECT pg_size_pretty(pg_database_size(current_database())) AS size")

        return json.dumps({
            "total_documents": total["cnt"],
            "total_chunks": chunks["cnt"],
            "total_cases": cases["cnt"],
            "by_type": {r["dtype"]: r["cnt"] for r in by_type},
            "date_range": {
                "oldest": _ser(recent["oldest"]),
                "newest": _ser(recent["newest"]),
            },
            "email_stats": {
                "total": email_stats["total"],
                "inbound": email_stats["inbound"],
                "outbound": email_stats["outbound"],
                "earliest": _ser(email_stats["earliest"]),
                "latest": _ser(email_stats["latest"]),
            },
            "database_size": db_size["size"],
        })
    except Exception as e:
        log.error("get_database_stats error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
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


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def ingestion_status(limit: int = 10) -> str:
    """Check the status of document ingestion jobs.

    Args:
        limit: Number of recent jobs to show (default 10)
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT id, domain, source, status, documents_processed,
                   chunks_processed, documents_failed, started_at, completed_at
            FROM ops.ingestion_jobs
            ORDER BY started_at DESC LIMIT $1
        """, limit)

        jobs = [{
            "job_id": r["id"],
            "domain": r["domain"],
            "source": r["source"],
            "status": r["status"],
            "docs_processed": r["documents_processed"],
            "chunks_processed": r["chunks_processed"],
            "docs_failed": r["documents_failed"],
            "started": _ser(r["started_at"]),
            "completed": _ser(r["completed_at"]),
        } for r in rows]

        return json.dumps({"jobs": jobs, "count": len(jobs)})
    except Exception as e:
        log.error("ingestion_status error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def execute_sql(query: str) -> str:
    """LOW-LEVEL PostgreSQL database access. DO NOT USE for answering questions about documents.

    ⚠️  DATABASE: PostgreSQL 17 (NOT SQLite) - Use PostgreSQL syntax and features
    
    ⚠️  PREFER these tools instead:
    - rag_query: for answering questions about case content
    - semantic_search: for finding relevant documents
    - search_emails, search_filings, list_cases: for structured queries

    Only use execute_sql for:
    - Database statistics (COUNT, aggregate queries)
    - Schema exploration (PostgreSQL information_schema queries)
    - Advanced PostgreSQL queries the other tools cannot handle
    - Queries requiring PostgreSQL-specific features (CTEs, window functions, JSONB, etc.)

    PostgreSQL Features Available:
    - Full-text search (tsvector, tsquery, to_tsquery)
    - JSONB operations and indexing
    - pgvector for semantic search
    - Window functions, CTEs, advanced joins
    - Array operations
    - PostgreSQL-specific types and functions

    Only SELECT queries allowed. Tables: core.documents, legal.cases, etc.
    Use schema-qualified names (schema.table) for all queries.

    Args:
        query: PostgreSQL SELECT query (schema.table format required)
    """
    normalized = query.strip().upper()
    if not normalized.startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed"})

    dangerous = ["DROP", "DELETE", "TRUNCATE", "ALTER", "INSERT", "UPDATE", "CREATE", "GRANT", "REVOKE"]
    for keyword in dangerous:
        if keyword in normalized:
            return json.dumps({"error": f"Query contains forbidden keyword: {keyword}"})

    p = await get_pool()
    try:
        rows = await p.fetch(query)
        results = [_row_dict(r) for r in rows[:100]]
        return json.dumps({"results": results, "count": len(results), "truncated": len(rows) > 100})
    except Exception as e:
        log.error("execute_sql error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  MEMORY MANAGEMENT TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=WRITE_OP)
async def save_memory(agent_id: str, key: str, value: str) -> str:
    """
    Save or update a memory for an agent. Use this to persist important context,
    user preferences, learned patterns, or any information that should survive
    across conversations.
    
    - agent_id: Your agent identifier (e.g., 'athena', 'case-strategy')
    - key: A descriptive key for the memory (e.g., 'user_preferences', 'case_context')
    - value: JSON string containing the memory data
    
    The memory is stored as JSONB and can be retrieved later with get_memory.
    If a memory with the same agent_id and key exists, it will be updated.
    """
    p = await get_pool()
    try:
        # Parse value as JSON to validate it
        try:
            json_value = json.loads(value)
        except json.JSONDecodeError:
            # If not valid JSON, wrap as string
            json_value = {"text": value}
        
        await p.execute("""
            INSERT INTO ops.agent_memory (agent_id, key, value, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (agent_id, key) DO UPDATE SET
                value = $3,
                updated_at = NOW()
        """, agent_id, key, json.dumps(json_value))
        
        return json.dumps({"success": True, "agent_id": agent_id, "key": key, "message": "Memory saved"})
    except Exception as e:
        log.error("save_memory error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def get_memory(agent_id: str, key: str) -> str:
    """
    Retrieve a specific memory for an agent. Use this to recall previously
    saved context, preferences, or learned information.
    
    - agent_id: Your agent identifier (e.g., 'athena', 'case-strategy')
    - key: The key of the memory to retrieve
    
    Returns the memory value or null if not found.
    """
    p = await get_pool()
    try:
        row = await p.fetchrow("""
            SELECT value, updated_at FROM ops.agent_memory
            WHERE agent_id = $1 AND key = $2
        """, agent_id, key)
        
        if row:
            return json.dumps({
                "found": True,
                "agent_id": agent_id,
                "key": key,
                "value": row["value"],
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None
            })
        else:
            return json.dumps({"found": False, "agent_id": agent_id, "key": key})
    except Exception as e:
        log.error("get_memory error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def list_memories(agent_id: str) -> str:
    """
    List all memories stored for an agent. Use this to see what context
    and information you have saved for later recall.
    
    - agent_id: Your agent identifier (e.g., 'athena', 'case-strategy')
    
    Returns a list of all memory keys and their last update times.
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT key, updated_at, 
                   CASE WHEN jsonb_typeof(value) = 'object' 
                        THEN (SELECT array_agg(k) FROM jsonb_object_keys(value) k)
                        ELSE NULL END as value_keys
            FROM ops.agent_memory
            WHERE agent_id = $1
            ORDER BY updated_at DESC
        """, agent_id)
        
        memories = [{
            "key": r["key"],
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            "value_keys": r["value_keys"]
        } for r in rows]
        
        return json.dumps({
            "agent_id": agent_id,
            "count": len(memories),
            "memories": memories
        })
    except Exception as e:
        log.error("list_memories error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=DESTRUCTIVE)
async def delete_memory(agent_id: str, key: str) -> str:
    """
    Delete a specific memory for an agent. Use with caution - this permanently
    removes the saved information.
    
    - agent_id: Your agent identifier (e.g., 'athena', 'case-strategy')
    - key: The key of the memory to delete
    """
    p = await get_pool()
    try:
        result = await p.execute("""
            DELETE FROM ops.agent_memory
            WHERE agent_id = $1 AND key = $2
        """, agent_id, key)
        
        deleted = result.split()[-1] != '0'
        return json.dumps({
            "success": True,
            "deleted": deleted,
            "agent_id": agent_id,
            "key": key
        })
    except Exception as e:
        log.error("delete_memory error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def get_agent_context(agent_id: str) -> str:
    """
    **CALL THIS FIRST AT CONVERSATION START** - Load all stored memories and
    context for an agent in a single call. Use this at the beginning of each
    conversation to restore your persistent knowledge.
    
    - agent_id: Your agent identifier (e.g., 'athena', 'case-strategy')
    
    Returns: All memories with their full values, plus agent profile and
    recent activity summary. This enables cross-conversation reflection.
    """
    p = await get_pool()
    try:
        # Get agent profile
        agent_row = await p.fetchrow("""
            SELECT id, name, description, capabilities, config, 
                   last_run_at, run_count, last_status
            FROM ops.agent_registry
            WHERE id = $1
        """, agent_id)
        
        profile = _row_dict(agent_row) if agent_row else {"id": agent_id, "name": agent_id}
        
        # Get all memories with full values
        memory_rows = await p.fetch("""
            SELECT key, value, updated_at
            FROM ops.agent_memory
            WHERE agent_id = $1
            ORDER BY updated_at DESC
        """, agent_id)
        
        memories = {r["key"]: {
            "value": r["value"],
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None
        } for r in memory_rows}
        
        # Get recent session activity (last 5 sessions)
        session_rows = await p.fetch("""
            SELECT id, started_at, ended_at, summary
            FROM ops.agent_sessions
            WHERE agent_id = $1
            ORDER BY started_at DESC
            LIMIT 5
        """, agent_id)
        
        recent_sessions = [_row_dict(r) for r in session_rows]
        
        # Get recent findings/insights
        finding_rows = await p.fetch("""
            SELECT category, severity, title, created_at
            FROM ops.agent_findings
            WHERE agent_id = $1
            ORDER BY created_at DESC
            LIMIT 10
        """, agent_id)
        
        recent_findings = [_row_dict(r) for r in finding_rows]
        
        return json.dumps({
            "profile": profile,
            "memories": memories,
            "memory_count": len(memories),
            "recent_sessions": recent_sessions,
            "recent_findings": recent_findings,
            "loaded_at": datetime.utcnow().isoformat()
        }, default=str)
    except Exception as e:
        log.error("get_agent_context error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  NOTE MANAGEMENT TOOLS (Investigation & Documentation)
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=WRITE_OP)
async def create_note(
    agent_id: str,
    title: str,
    content: str,
    note_type: str = "general",
    case_number: str = None,
    tags: str = None,
    priority: int = 5
) -> str:
    """
    Create a note to document findings, questions, or tasks during an investigation.
    Use this to track your analysis, record evidence, or create follow-up tasks.
    
    - agent_id: Your identifier (e.g., 'athena')
    - title: Brief descriptive title
    - content: Full note content (can be detailed)
    - note_type: One of 'general', 'finding', 'task', 'question', 'evidence', 'analysis'
    - case_number: Optional case number to link the note to
    - tags: Optional comma-separated tags (e.g., 'urgent,discovery,witness')
    - priority: 1 (highest) to 10 (lowest), default 5
    
    Returns the created note ID for future reference.
    """
    p = await get_pool()
    try:
        tag_array = [t.strip() for t in tags.split(',')] if tags else []
        
        row = await p.fetchrow("""
            INSERT INTO ops.agent_notes 
            (agent_id, title, content, note_type, case_number, tags, priority)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, created_at
        """, agent_id, title, content, note_type, case_number, tag_array, priority)
        
        return json.dumps({
            "success": True,
            "note_id": row["id"],
            "created_at": row["created_at"].isoformat(),
            "message": f"Note '{title}' created successfully"
        })
    except Exception as e:
        log.error("create_note error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def get_notes(
    agent_id: str,
    note_type: str = None,
    case_number: str = None,
    status: str = None,
    search_query: str = None,
    limit: int = 20
) -> str:
    """
    Retrieve notes for review or reference. Filter by type, case, status, or search text.
    Use this to recall your previous findings, check task status, or review evidence.
    
    - agent_id: Your identifier (e.g., 'athena')
    - note_type: Filter by type (general, finding, task, question, evidence, analysis)
    - case_number: Filter by case
    - status: Filter by status (open, in_progress, resolved, archived)
    - search_query: Full-text search in title and content
    - limit: Maximum notes to return (default 20)
    
    Returns notes sorted by most recent first.
    """
    p = await get_pool()
    try:
        conditions = ["agent_id = $1"]
        params = [agent_id]
        idx = 2
        
        if note_type:
            conditions.append(f"note_type = ${idx}")
            params.append(note_type)
            idx += 1
        
        if case_number:
            conditions.append(f"case_number = ${idx}")
            params.append(case_number)
            idx += 1
            
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
            
        if search_query:
            conditions.append(f"search_vector @@ plainto_tsquery('english', ${idx})")
            params.append(search_query)
            idx += 1
        
        params.append(limit)
        
        query = f"""
            SELECT id, note_type, title, content, case_number, tags, 
                   priority, status, created_at, updated_at
            FROM ops.agent_notes
            WHERE {' AND '.join(conditions)}
            ORDER BY created_at DESC
            LIMIT ${idx}
        """
        
        rows = await p.fetch(query, *params)
        notes = [_row_dict(r) for r in rows]
        
        return json.dumps({
            "count": len(notes),
            "notes": notes
        }, default=str)
    except Exception as e:
        log.error("get_notes error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
async def update_note(
    note_id: int,
    content: str = None,
    status: str = None,
    priority: int = None,
    tags: str = None
) -> str:
    """
    Update an existing note - add content, change status, adjust priority, or add tags.
    Use this to mark tasks complete, add follow-up information, or reprioritize.
    
    - note_id: The ID of the note to update
    - content: New content (appends with timestamp if note has existing content)
    - status: New status (open, in_progress, resolved, archived)
    - priority: New priority (1-10)
    - tags: New comma-separated tags (replaces existing)
    """
    p = await get_pool()
    try:
        updates = []
        params = []
        idx = 1
        
        if content:
            # Append to existing content with timestamp
            updates.append(f"content = content || E'\\n\\n--- Update ' || NOW()::text || ' ---\\n' || ${idx}")
            params.append(content)
            idx += 1
            
        if status:
            updates.append(f"status = ${idx}")
            params.append(status)
            idx += 1
            
        if priority:
            updates.append(f"priority = ${idx}")
            params.append(priority)
            idx += 1
            
        if tags:
            tag_array = [t.strip() for t in tags.split(',')]
            updates.append(f"tags = ${idx}")
            params.append(tag_array)
            idx += 1
        
        if not updates:
            return json.dumps({"error": "No updates provided"})
            
        updates.append("updated_at = NOW()")
        params.append(note_id)
        
        result = await p.execute(f"""
            UPDATE ops.agent_notes 
            SET {', '.join(updates)}
            WHERE id = ${idx}
        """, *params)
        
        updated = result.split()[-1] != '0'
        return json.dumps({
            "success": updated,
            "note_id": note_id,
            "message": "Note updated" if updated else "Note not found"
        })
    except Exception as e:
        log.error("update_note error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def get_investigation_summary(agent_id: str, case_number: str = None) -> str:
    """
    Get a comprehensive summary of your investigation status - all notes, tasks,
    findings, and open questions organized by type and priority.
    
    - agent_id: Your identifier (e.g., 'athena')
    - case_number: Optional - filter to specific case
    
    Use this at the start of a session to see where you left off, or to
    generate a status report of your investigation progress.
    """
    p = await get_pool()
    try:
        case_filter = "AND case_number = $2" if case_number else ""
        params = [agent_id, case_number] if case_number else [agent_id]
        
        # Get counts by type and status
        stats_query = f"""
            SELECT note_type, status, COUNT(*) as count
            FROM ops.agent_notes
            WHERE agent_id = $1 {case_filter}
            GROUP BY note_type, status
            ORDER BY note_type, status
        """
        stats_rows = await p.fetch(stats_query, *params)
        
        # Get high priority open items
        urgent_query = f"""
            SELECT id, note_type, title, case_number, priority, created_at
            FROM ops.agent_notes
            WHERE agent_id = $1 AND status IN ('open', 'in_progress') AND priority <= 3
            {case_filter}
            ORDER BY priority, created_at DESC
            LIMIT 10
        """
        urgent_rows = await p.fetch(urgent_query, *params)
        
        # Get recent activity
        recent_query = f"""
            SELECT id, note_type, title, status, updated_at
            FROM ops.agent_notes
            WHERE agent_id = $1 {case_filter}
            ORDER BY updated_at DESC
            LIMIT 5
        """
        recent_rows = await p.fetch(recent_query, *params)
        
        # Get open tasks
        tasks_query = f"""
            SELECT id, title, priority, case_number, created_at
            FROM ops.agent_notes
            WHERE agent_id = $1 AND note_type = 'task' AND status = 'open'
            {case_filter}
            ORDER BY priority, created_at
            LIMIT 10
        """
        task_rows = await p.fetch(tasks_query, *params)
        
        # Organize stats
        stats = {}
        for r in stats_rows:
            note_type = r["note_type"]
            if note_type not in stats:
                stats[note_type] = {}
            stats[note_type][r["status"]] = r["count"]
        
        return json.dumps({
            "case_number": case_number,
            "stats_by_type": stats,
            "urgent_items": [_row_dict(r) for r in urgent_rows],
            "open_tasks": [_row_dict(r) for r in task_rows],
            "recent_activity": [_row_dict(r) for r in recent_rows]
        }, default=str)
    except Exception as e:
        log.error("get_investigation_summary error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  BULK REPORT TOOLS (reduce iteration counts)
# ══════════════════════════════════════════════════════════════

@mcp.tool(annotations=READ_ONLY)
async def get_case_report(case_number: str) -> str:
    """
    **RECOMMENDED FOR CASE RESEARCH** - Get a comprehensive report for a case
    in a single call. This combines case details, all filings, key dates,
    and parties into one response - much more efficient than multiple queries.
    
    - case_number: The case number (e.g., '24-2-01031-31')
    
    Returns: Complete case information including metadata, all filings list,
    party information, and timeline of key events.
    """
    p = await get_pool()
    try:
        # Get case metadata from legal.cases
        case_row = await p.fetchrow("""
            SELECT case_number, case_title, court, case_type, status,
                   date_filed, date_closed, jurisdiction, metadata
            FROM legal.cases
            WHERE case_number ILIKE $1 OR case_title ILIKE '%' || $1 || '%'
            LIMIT 1
        """, case_number)
        
        if not case_row:
            return json.dumps({"error": f"Case not found: {case_number}"})
        
        case_data = _row_dict(case_row)
        actual_case_number = case_data["case_number"]
        
        # Get parties for this case
        party_rows = await p.fetch("""
            SELECT p.name, p.party_type, cp.role
            FROM legal.case_parties cp
            JOIN legal.parties p ON cp.party_id = p.id
            JOIN legal.cases c ON cp.case_id = c.id
            WHERE c.case_number = $1
        """, actual_case_number)
        
        parties = [_row_dict(r) for r in party_rows]
        
        # Get all filings/documents for this case
        filing_rows = await p.fetch("""
            SELECT d.id, d.title, d.document_type, d.filename, d.created_at,
                   fm.filing_type, fm.filed_by, fm.date_ingested, fm.actual_filing_date
            FROM core.documents d
            JOIN legal.case_documents cd ON d.id = cd.document_id
            JOIN legal.cases c ON cd.case_id = c.id
            LEFT JOIN legal.filing_metadata fm ON d.id = fm.document_id
            WHERE c.case_number = $1
            ORDER BY COALESCE(fm.actual_filing_date, fm.date_ingested, d.created_at) DESC
        """, actual_case_number)
        
        filings = [_row_dict(r) for r in filing_rows]
        
        # Compile report
        report = {
            "case": case_data,
            "parties": {
                "count": len(parties),
                "items": parties
            },
            "filings": {
                "count": len(filings),
                "items": filings
            },
            "summary": {
                "total_filings": len(filings),
                "total_parties": len(parties),
                "status": case_data.get("status", "Unknown")
            }
        }
        
        return json.dumps(report, default=str)
    except Exception as e:
        log.error("get_case_report error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
async def get_all_cases_summary() -> str:
    """
    **START HERE FOR CASE OVERVIEW** - Get a summary of all cases in the system
    in a single call. Returns case numbers, titles, status, and filing counts.
    
    Much more efficient than querying cases one at a time.
    """
    p = await get_pool()
    try:
        rows = await p.fetch("""
            SELECT c.case_number, c.case_title, c.court, c.case_type, 
                   c.status, c.date_filed, c.jurisdiction,
                   COUNT(DISTINCT cd.document_id) as filing_count,
                   COUNT(DISTINCT cp.party_id) as party_count
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            LEFT JOIN legal.case_parties cp ON c.id = cp.case_id
            GROUP BY c.id, c.case_number, c.case_title, c.court, c.case_type,
                     c.status, c.date_filed, c.jurisdiction
            ORDER BY c.date_filed DESC NULLS LAST
        """)
        
        cases = [_row_dict(r) for r in rows]
        
        return json.dumps({
            "total_cases": len(cases),
            "cases": cases
        }, default=str)
    except Exception as e:
        log.error("get_all_cases_summary error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

def main():
    import argparse
    parser = argparse.ArgumentParser(description="WDWS Unified MCP Server v2")
    parser.add_argument("--http", action="store_true", help="Run as HTTP/SSE server (default: stdio)")
    parser.add_argument("--port", type=int, default=9200, help="HTTP port (default 9200)")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    args = parser.parse_args()

    if args.http:
        log.info("Starting WDWS MCP Server v2 (Streamable HTTP + OAuth) on %s:%d", args.host, args.port)
        log.info("OAuth issuer: %s", BASE_URL)
        mcp.run(transport="streamable-http")
    else:
        log.info("Starting WDWS MCP Server v2 (stdio)")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
