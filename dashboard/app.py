#!/usr/bin/env python3
"""
Athena Cognitive Platform Dashboard v2.0 — PostgreSQL-native.

Direct connection to the enterprise PostgreSQL database.
Provides:
  • Real-time telemetry & system stats
  • Full audit trail (access logs, search analytics)
  • Ingestion job monitoring & live progress
  • Document browsing & search across all domains
  • Legal case overview with doc counts
  • Agent registry & session tracking
  • Database health & table statistics

Runs on uvicorn, default port 9100.
"""

import os, io, json, time, secrets, subprocess, asyncio, hashlib, mimetypes, re, shutil, sys, uuid, zipfile
import email as email_lib
import base64 as _b64
from email import policy
from pathlib import Path, PurePosixPath
from typing import Optional, List, Dict, Any
from urllib.parse import quote

import asyncpg
import httpx
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, StreamingResponse, FileResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
import uvicorn

# OSINT Investigation Database
import sys
sys.path.insert(0, '/opt/wdws/dashboard')
from osint_api import get_osint_routes, get_osint_pool

# Sprint A Task 3: retrieval-safety helper. Propagated to the 8 LLM-exposed
# chat tools registered in CHAT_TOOLS. HTTP-only endpoints (api_documents,
# api_document_detail, api_search, api_audit_*, etc.) are tracked in the
# T3_REVIEW manifest for propagation in a follow-up Sprint A commit before
# closeout — they require count-only mode for audit surfaces per §5.6.
sys.path.insert(0, '/opt/wdws')
from core_safety import (  # noqa: E402
    build_document_safety_filter,
    fetch_safe_document,
    PrivilegeDeniedException,
    PHIAccessDeniedException,
)


def _dashboard_caller_context(tool_name: str, request=None, extra: Optional[dict] = None) -> dict:
    """Build caller_context for Sprint A retrieval-safety logging.

    Dashboard calls generally have a session and user. When a request is
    passed, the user is extracted from Starlette session state; otherwise
    'dashboard' is the default attribution.
    """
    user = 'dashboard'
    session_id = None
    if request is not None:
        try:
            user = request.session.get('user') or user
        except Exception:
            pass
    ctx = {
        'tool': f"dashboard.{tool_name}",
        'agent_id': 'dashboard_chat',
        'user': user,
        'session_id': session_id,
    }
    if extra:
        ctx.update(extra)
    return ctx

# ── Config ────────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
DASHBOARD_USERS = {
    "admin":   "2$NEL2233obs",
    "wnelson": "NEL2233obs",
}
USER_DISPLAY_NAMES = {
    "admin":   "Admin",
    "wnelson": "William",
}
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "9100"))
INGEST_LOG     = os.getenv("INGEST_LOG", "/opt/wdws/ingest_full.log")
SYNC_LOG       = os.getenv("SYNC_LOG", "/opt/wdws/email_sync.log")
AGENT_LOG      = os.getenv("AGENT_LOG", "/opt/wdws/email_agent.log")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5.4")
sys.path.insert(0, "/opt/wdws")
from embedding_service import (
    embed_query as _embed_query_local,
    _vec_literal,
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL,
)
from email_sync_config import audit_sync_configuration_health
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token" if GRAPH_TENANT_ID else ""
CHAT_UPLOAD_DIR = Path(os.getenv("CHAT_UPLOAD_DIR", "/opt/wdws/data/chat_uploads"))
CHAT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MEDICAL_DROPBOX_DIR = Path(os.getenv("MEDICAL_DROPBOX_DIR", "/opt/wdws/data/dropbox/medical"))
MEDICAL_DROPBOX_DIR.mkdir(parents=True, exist_ok=True)
MEDICAL_ZIP_MAX_UPLOAD_BYTES = int(os.getenv("MEDICAL_ZIP_MAX_UPLOAD_BYTES", str(250 * 1024 * 1024)))
MEDICAL_ZIP_MAX_MEMBERS = int(os.getenv("MEDICAL_ZIP_MAX_MEMBERS", "5000"))
MEDICAL_ZIP_MAX_UNCOMPRESSED_BYTES = int(os.getenv("MEDICAL_ZIP_MAX_UNCOMPRESSED_BYTES", str(1024 * 1024 * 1024)))
SUPPORTED_MEDICAL_IMPORT_EXTENSIONS = {
  ".pdf", ".docx", ".doc", ".txt", ".eml",
  ".png", ".jpg", ".jpeg", ".tiff", ".tif",
  ".xml", ".csv", ".rtf", ".xlsx", ".xls",
}
AVAILABLE_MODELS = [
  {"id": "auto",            "model": "gpt-5.4",                     "provider": "openai",    "label": "⚡ Auto (GPT-5.4)",       "reasoning_effort": None,      "max_tool_rounds": 12, "description": "GPT-5.4 — automatically decides when to think deeper"},
  {"id": "claude-sonnet",   "model": "claude-sonnet-4-6",           "provider": "anthropic", "label": "🟣 Claude Sonnet 4.6",    "reasoning_effort": None,      "max_tool_rounds": 14, "description": "Claude Sonnet — fast, capable, great for most tasks"},
  {"id": "claude-opus",     "model": "claude-opus-4-6",             "provider": "anthropic", "label": "🔮 Claude Opus 4.6",      "reasoning_effort": None,      "max_tool_rounds": 18, "description": "Claude Opus — most capable, deep legal reasoning"},
  {"id": "claude-haiku",    "model": "claude-haiku-4-5-20251001",   "provider": "anthropic", "label": "⚡ Claude Haiku 4.5",     "reasoning_effort": None,      "max_tool_rounds": 10, "description": "Claude Haiku — fastest, great for simple questions"},
  {"id": "think-low",       "model": "gpt-5.4",                     "provider": "openai",    "label": "💭 GPT Think Low",        "reasoning_effort": "low",     "max_tool_rounds": 8,  "description": "GPT-5.4 light reasoning — faster responses"},
  {"id": "think-med",       "model": "gpt-5.4",                     "provider": "openai",    "label": "💭 GPT Think Med",        "reasoning_effort": "medium",  "max_tool_rounds": 10, "description": "GPT-5.4 balanced reasoning and speed"},
  {"id": "think-high",      "model": "gpt-5.4",                     "provider": "openai",    "label": "🧠 GPT Think High",       "reasoning_effort": "high",    "max_tool_rounds": 14, "description": "GPT-5.4 deep reasoning — most thorough"},
]

pool: Optional[asyncpg.Pool] = None
_graph_access_token: Optional[str] = None
_graph_token_expires_at: float = 0.0


async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=8, command_timeout=30
        )
    return pool


# ── Auth (PostgreSQL-backed sessions — persist across restarts) ─
SESSION_TIMEOUT = 14400  # 4 hours in seconds

_SESSION_TABLE_READY = False

async def _ensure_session_table():
    """Create ops.dashboard_sessions if it doesn't exist (runs once)."""
    global _SESSION_TABLE_READY
    if _SESSION_TABLE_READY:
        return
    p = await get_pool()
    await p.execute("""
        CREATE TABLE IF NOT EXISTS ops.dashboard_sessions (
            token       TEXT PRIMARY KEY,
            username    TEXT NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT now()
        )
    """)
    # Clean up expired on startup
    await p.execute("DELETE FROM ops.dashboard_sessions WHERE expires_at < now()")
    _SESSION_TABLE_READY = True


async def _get_session_user(tok: str) -> str | None:
    """Look up a session token, return username or None."""
    if not tok:
        return None
    await _ensure_session_table()
    p = await get_pool()
    row = await p.fetchrow(
        "SELECT username FROM ops.dashboard_sessions WHERE token = $1 AND expires_at > now()",
        tok,
    )
    return row["username"] if row else None


async def _create_session(username: str) -> tuple[str, float]:
    """Create a new session, return (token, expires_epoch)."""
    await _ensure_session_table()
    p = await get_pool()
    tok = secrets.token_hex(24)
    expires = time.time() + SESSION_TIMEOUT
    await p.execute(
        "INSERT INTO ops.dashboard_sessions (token, username, expires_at) VALUES ($1, $2, to_timestamp($3))",
        tok, username, expires,
    )
    # Prune expired sessions occasionally (non-blocking)
    await p.execute("DELETE FROM ops.dashboard_sessions WHERE expires_at < now()")
    return tok, expires


async def _delete_session(tok: str):
    """Remove a session token."""
    if not tok:
        return
    await _ensure_session_table()
    p = await get_pool()
    await p.execute("DELETE FROM ops.dashboard_sessions WHERE token = $1", tok)


async def _check_auth(request: Request):
    await _ensure_session_table()
    tok = request.cookies.get("dash_session")
    username = await _get_session_user(tok)
    if username:
        return None  # Authenticated
    import base64
    auth = request.headers.get("authorization", "")
    if auth.startswith("Basic "):
        try:
            u, p = base64.b64decode(auth[6:]).decode().split(":", 1)
            if DASHBOARD_USERS.get(u) == p:
                return None
        except Exception:
            pass
    return JSONResponse({"error": "unauthorized"}, status_code=401)


async def api_login(request: Request):
    data = await request.json()
    username = data.get("username", "")
    password = data.get("password", "")
    if DASHBOARD_USERS.get(username) == password:
        tok, _ = await _create_session(username)
        display = USER_DISPLAY_NAMES.get(username, username)
        resp = JSONResponse({"ok": True, "user": username, "display_name": display, "token": tok})
        resp.set_cookie(
            "dash_session", tok, httponly=True,
            max_age=SESSION_TIMEOUT, samesite="lax"
        )
        return resp
    return JSONResponse({"error": "bad credentials"}, status_code=401)


async def api_logout(request: Request):
    tok = request.cookies.get("dash_session")
    await _delete_session(tok)
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("dash_session")
    return resp


async def api_me(request: Request):
    """Return current user info for an active session."""
    tok = request.cookies.get("dash_session")
    username = await _get_session_user(tok)
    if not username:
        # Try Basic auth
        import base64
        auth = request.headers.get("authorization", "")
        if auth.startswith("Basic "):
            try:
                u, p = base64.b64decode(auth[6:]).decode().split(":", 1)
                if DASHBOARD_USERS.get(u) == p:
                    username = u
            except Exception:
                pass
    if not username:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    return JSONResponse({
        "username": username,
        "display_name": USER_DISPLAY_NAMES.get(username, username),
    })


# ── Helpers ───────────────────────────────────────────────────
def _ser(val):
    """Make a DB value JSON-serialisable."""
    if val is None:
        return None
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, (dict, list)):
        return val
    return val


def _row_dict(row, keys=None):
    """Convert an asyncpg Record to a plain dict."""
    d = dict(row)
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            d[k] = v.isoformat()
        elif isinstance(v, bytes):
            d[k] = None  # skip binary (embeddings, etc.)
    # Parse JSONB strings
    for k in (keys or []):
        if k in d and isinstance(d[k], str):
            try:
                d[k] = json.loads(d[k])
            except Exception:
                pass
    return d


def _guess_mime_type(filename: Optional[str], fallback: str = "application/octet-stream") -> str:
    if filename:
        guessed, _ = mimetypes.guess_type(filename)
        if guessed:
            return guessed
    return fallback


async def _get_graph_access_token() -> str:
    global _graph_access_token, _graph_token_expires_at

    if not all([GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_TENANT_ID, GRAPH_TOKEN_URL]):
        raise RuntimeError("Graph credentials are not configured for REST document downloads")

    if _graph_access_token and time.time() < _graph_token_expires_at:
        return _graph_access_token

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            GRAPH_TOKEN_URL,
            data={
                "client_id": GRAPH_CLIENT_ID,
                "client_secret": GRAPH_CLIENT_SECRET,
                "scope": "https://graph.microsoft.com/.default",
                "grant_type": "client_credentials",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        _graph_access_token = data["access_token"]
        _graph_token_expires_at = time.time() + data.get("expires_in", 3600) - 300
        return _graph_access_token


async def _graph_get_json(url: str, params: Optional[dict] = None) -> dict:
    token = await _get_graph_access_token()
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


async def _graph_get_bytes(url: str) -> bytes:
    token = await _get_graph_access_token()
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        return resp.content


async def _graph_resolve_message_graph_id(mailbox: str, message_ref: str) -> str:
    if not message_ref:
        raise RuntimeError("Missing Graph message reference")

    if message_ref.startswith("<") or "@" in message_ref:
        data = await _graph_get_json(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages",
            params={
                "$filter": f"internetMessageId eq '{message_ref}'",
                "$select": "id,internetMessageId",
                "$top": "1",
            },
        )
        values = data.get("value") or []
        if values:
            return values[0]["id"]

    try:
        data = await _graph_get_json(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_ref}",
            params={"$select": "id"},
        )
        if data.get("id"):
            return data["id"]
    except Exception:
        pass

    raise RuntimeError(f"Unable to resolve Graph message ID for mailbox {mailbox}")


def _extract_all_mime_attachments(mime_data: bytes) -> List[Dict[str, Any]]:
    msg = email_lib.message_from_bytes(mime_data, policy=policy.default)
    attachments: List[Dict[str, Any]] = []

    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        disp = str(part.get("Content-Disposition", ""))
        fname = part.get_filename()
        if not fname and "attachment" not in disp.lower():
            continue
        payload = part.get_payload(decode=True)
        if payload is None:
            continue
        attachments.append({
            "filename": fname or "attachment.bin",
            "content_type": part.get_content_type() or "application/octet-stream",
            "data": payload,
            "file_size": len(payload),
        })
    return attachments


def _match_attachment_bytes(candidates: List[Dict[str, Any]], filename: str, content_type: Optional[str], file_size: Optional[int]) -> Optional[Dict[str, Any]]:
    norm_name = (filename or "").strip().lower()
    norm_type = (content_type or "").strip().lower()

    best = None
    best_score = -1
    for item in candidates:
        score = -1
        item_name = (item.get("filename") or "").strip().lower()
        item_type = (item.get("content_type") or "").strip().lower()
        item_size = item.get("file_size") or 0

        if norm_name and item_name == norm_name and norm_type and item_type == norm_type and file_size and item_size == file_size:
            score = 4
        elif norm_name and item_name == norm_name and file_size and item_size == file_size:
            score = 3
        elif norm_name and item_name == norm_name:
            score = 2
        elif norm_type and item_type == norm_type and file_size and item_size == file_size:
            score = 1

        if score > best_score:
            best = item
            best_score = score

    return best if best_score >= 0 else None


def _content_disposition_value(filename: str, disposition: str = "attachment") -> str:
    safe_name = (filename or "download.bin").replace("\r", " ").replace("\n", " ").replace('"', "'")
    return f"{disposition}; filename=\"{safe_name}\"; filename*=UTF-8''{quote(safe_name)}"


def _health_status_code(status: str) -> int:
  return 200 if status == "healthy" else (207 if status == "degraded" else 503)


async def _resolve_original_document_download(
    document_id: str,
    *,
    request: Optional[Request] = None,
    include_privileged: bool = False,
    purpose_of_use: Optional[str] = None,
    authorization_id: Optional[uuid.UUID] = None,
) -> Dict[str, Any]:
    """Resolve the original bytes for a document download.

    Sprint A: gates by privilege + PHI via fetch_safe_document before
    returning any bytes. Raises RuntimeError on denial (mapped to 403 by
    the caller). caller_context is derived from request if present.
    """
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            _ = await fetch_safe_document(
                document_id, conn=conn,
                caller_context=_dashboard_caller_context(
                    '_resolve_original_document_download', request=request,
                ),
                include_privileged=include_privileged,
                purpose_of_use=purpose_of_use,
                authorization_id=authorization_id,
            )
        except PrivilegeDeniedException:
            raise RuntimeError("privilege_denied: document is privileged; caller not authorized")
        except PHIAccessDeniedException as e:
            raise RuntimeError(f"phi_access_denied: {e}")

        # Access authorized; fetch joined metadata for the download path.
        doc = await conn.fetchrow("""
            SELECT d.id, d.title, d.filename, d.domain, d.document_type, d.source_path,
                   d.full_content, d.metadata,
                   em.message_id, em.subject, em.mailbox
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON em.document_id = d.id
            WHERE d.id = $1::uuid
        """, document_id)

    if not doc:
        raise RuntimeError(f"Document {document_id} not found")

    source_path = doc["source_path"] or ""
    filename = doc["filename"] or doc["title"] or f"document-{document_id}"

    if source_path.startswith("graph://"):
        if doc["document_type"] in ("email", "eml"):
            graph_ref = source_path.replace("graph://", "", 1)
            mailbox, message_ref = graph_ref.split("/", 1)
            graph_id = await _graph_resolve_message_graph_id(mailbox, message_ref)
            mime_bytes = await _graph_get_bytes(f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_id}/$value")
            out_name = filename if filename.lower().endswith(".eml") else f"{filename}.eml"
            return {
                "document_id": str(doc["id"]),
                "filename": out_name,
                "mime_type": "message/rfc822",
                "source_kind": "graph_email",
                "original_available": True,
                "content_bytes": mime_bytes,
            }

        att = await p.fetchrow("""
            SELECT ea.id, ea.filename, ea.content_type, ea.file_size,
                   parent.source_path AS parent_source_path
            FROM legal.email_attachments ea
            JOIN core.documents parent ON parent.id = ea.email_doc_id
            WHERE ea.child_doc_id = $1::uuid
        """, document_id)
        if att:
            parent_ref = (att["parent_source_path"] or "").replace("graph://", "", 1)
            mailbox, message_ref = parent_ref.split("/", 1)
            graph_id = await _graph_resolve_message_graph_id(mailbox, message_ref)
            mime_bytes = await _graph_get_bytes(f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_id}/$value")
            attachments = _extract_all_mime_attachments(mime_bytes)
            matched = _match_attachment_bytes(attachments, att["filename"], att["content_type"], att["file_size"])
            if not matched:
                raise RuntimeError(f"Attachment bytes not found in parent email for document {document_id}")
            return {
                "document_id": str(doc["id"]),
                "filename": att["filename"] or filename,
                "mime_type": att["content_type"] or _guess_mime_type(att["filename"] or filename),
                "source_kind": "graph_attachment",
                "original_available": True,
                "content_bytes": matched["data"],
            }

    local_path = source_path
    if local_path.startswith("file://"):
        local_path = local_path[7:]
    if local_path and os.path.exists(local_path):
        local_name = os.path.basename(local_path) or filename
        return {
            "document_id": str(doc["id"]),
            "filename": local_name,
            "mime_type": _guess_mime_type(local_name),
            "source_kind": "filesystem",
            "original_available": True,
            "local_path": local_path,
        }

    if doc["full_content"]:
        export_name = filename if "." in filename else f"{filename}.txt"
        return {
            "document_id": str(doc["id"]),
            "filename": export_name,
            "mime_type": "text/plain; charset=utf-8",
            "source_kind": "database_export",
            "original_available": False,
            "content_bytes": (doc["full_content"] or "").encode("utf-8"),
        }

    raise RuntimeError(f"No downloadable source found for document {document_id}")


# ══════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ══════════════════════════════════════════════════════════════

# ── System Stats ──────────────────────────────────────────────
async def api_stats(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    row = await p.fetchval("SELECT ops.get_system_stats()")
    return JSONResponse(json.loads(row))


# ── Domain Breakdown ──────────────────────────────────────────
async def api_domain_stats(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT d.domain,
               COUNT(DISTINCT d.id)             AS documents,
               COALESCE(SUM(d.total_chunks), 0)  AS chunks,
               COUNT(DISTINCT c.id) FILTER (WHERE c.embedding IS NOT NULL)
                                                  AS embedded_chunks,
               MIN(d.created_at) AS first_ingested,
               MAX(d.created_at) AS last_ingested
        FROM core.documents d
        LEFT JOIN core.document_chunks c ON d.id = c.document_id
        GROUP BY d.domain ORDER BY d.domain
    """)
    return JSONResponse([{
        "domain": r["domain"],
        "documents": r["documents"],
        "chunks": r["chunks"],
        "embedded_chunks": r["embedded_chunks"],
        "first_ingested": _ser(r["first_ingested"]),
        "last_ingested": _ser(r["last_ingested"]),
    } for r in rows])


# ── Doc-Type Breakdown ────────────────────────────────────────
async def api_doctype_stats(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT domain, document_type, COUNT(*) AS count
        FROM core.documents GROUP BY domain, document_type
        ORDER BY count DESC
    """)
    return JSONResponse([dict(r) for r in rows])


# ── Ingestion Jobs ────────────────────────────────────────────
async def api_ingestion_jobs(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT id, domain, source, status, documents_processed,
               chunks_processed, documents_failed,
               started_at, completed_at, metadata
        FROM ops.ingestion_jobs ORDER BY started_at DESC LIMIT 50
    """)
    result = []
    for r in rows:
        d = _row_dict(r, ["metadata"])
        result.append(d)
    return JSONResponse(result)


# ── Live Ingestion Progress ───────────────────────────────────
async def api_ingestion_live(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    lines = int(request.query_params.get("lines", "40"))
    log_path = Path(INGEST_LOG)
    tail = ""
    if log_path.exists():
        try:
            result = subprocess.run(
                ["tail", f"-{lines}", str(log_path)],
                capture_output=True, text=True, timeout=5,
            )
            tail = result.stdout
        except Exception as e:
            tail = f"Error reading log: {e}"
    running = False
    try:
        result = subprocess.run(
            ["pgrep", "-f", "ingest.py"],
            capture_output=True, text=True, timeout=5,
        )
        running = result.returncode == 0
    except Exception:
        pass
    return JSONResponse({"running": running, "log": tail})


# ── Documents Browse ──────────────────────────────────────────
async def api_documents(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    params = request.query_params
    domain = params.get("domain")
    doc_type = params.get("type")
    case_number = params.get("case_number")
    tag_slug = params.get("tag")  # filter by tag slug (includes descendants)
    limit = min(int(params.get("limit", "50")), 200)
    offset = int(params.get("offset", "0"))

    where, args, idx = [], [], 1
    if domain:
        where.append(f"d.domain = ${idx}"); args.append(domain); idx += 1
    if doc_type:
        where.append(f"d.document_type = ${idx}"); args.append(doc_type); idx += 1
    if case_number:
        where.append(f"lc.case_number = ${idx}"); args.append(case_number); idx += 1
    if tag_slug:
        where.append(f"EXISTS (SELECT 1 FROM core.document_tags dt_f WHERE dt_f.document_id = d.id AND dt_f.tag_id IN (SELECT tag_id FROM core.get_tag_descendants(${idx})))")
        args.append(tag_slug); idx += 1
    async with p.acquire() as conn:
        safety_domain = domain if domain in ('legal', 'medical') else None
        safety_clause, safety_params, log_cb = await build_document_safety_filter(
            conn=conn, domain=safety_domain,
            caller_context=_dashboard_caller_context(
                'api_documents', request=request,
                extra={'query_domain': domain, 'doc_type': doc_type, 'case_number': case_number},
            ),
            table_alias='d', next_param_index=idx,
        )
        idx += len(safety_params)
        args.extend(safety_params)
        where.append(safety_clause)
        wc = " AND ".join(where)

        total = await conn.fetchval(f"""
            SELECT COUNT(DISTINCT d.id)
            FROM core.documents d
            LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
            LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
            WHERE {wc}
        """, *args)

        rows = await conn.fetch(f"""
            SELECT d.id, d.domain, d.document_type, d.title, d.filename,
                   d.source_path, d.total_chunks, d.content_hash,
                   d.created_at, d.metadata,
                   d.privilege, d.phi_status,
                   lc.case_number, lc.case_title,
                   em.sender, em.direction, em.date_sent, em.mailbox,
                   rm.record_type AS medical_type, rm.date_of_service
            FROM core.documents d
            LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
            LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN medical.record_metadata rm ON d.id = rm.document_id
            WHERE {wc}
            ORDER BY d.created_at DESC
            LIMIT ${idx} OFFSET ${idx + 1}
        """, *args, limit, offset)
        await log_cb(rows)

    docs = []
    for r in rows:
        d = _row_dict(r, ["metadata"])
        d["id"] = str(d["id"])
        docs.append(d)
    return JSONResponse({"total": total, "documents": docs})


# ── Document Detail ───────────────────────────────────────────
async def api_document_detail(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    doc_id = request.path_params["doc_id"]
    # Optional query params let authorized viewers request privileged/PHI.
    include_privileged = request.query_params.get("include_privileged", "").lower() in ("1","true","yes")
    purpose_of_use = request.query_params.get("purpose_of_use") or None
    auth_id_str = request.query_params.get("authorization_id")
    p = await get_pool()
    async with p.acquire() as conn:
        try:
            auth_uuid = uuid.UUID(auth_id_str) if auth_id_str else None
        except (ValueError, AttributeError):
            return JSONResponse({"error": "invalid authorization_id UUID"}, status_code=400)
        # Gate access first via fetch_safe_document — raises on denial.
        try:
            _ = await fetch_safe_document(
                doc_id, conn=conn,
                caller_context=_dashboard_caller_context('api_document_detail', request=request),
                include_privileged=include_privileged,
                purpose_of_use=purpose_of_use,
                authorization_id=auth_uuid,
            )
        except PrivilegeDeniedException:
            return JSONResponse({"error": "privilege_denied"}, status_code=403)
        except PHIAccessDeniedException as e:
            return JSONResponse({"error": "phi_access_denied", "detail": str(e)}, status_code=403)
        # Access authorized; fetch the joined metadata view.
        doc = await conn.fetchrow("""
            SELECT d.*, lc.case_number, lc.case_title, lc.court,
                   em.sender, em.recipients, em.cc, em.date_sent,
                   em.direction, em.mailbox, em.message_id, em.thread_id,
                   rm.record_type, rm.date_of_service, rm.facility,
                   pt.name AS patient_name, pr.name AS provider_name
            FROM core.documents d
            LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
            LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN medical.record_metadata rm ON d.id = rm.document_id
            LEFT JOIN medical.patients pt ON rm.patient_id = pt.id
            LEFT JOIN medical.providers pr ON rm.provider_id = pr.id
            WHERE d.id = $1
        """, doc_id)
    if not doc:
        return JSONResponse({"error": "not found"}, status_code=404)

    chunks = await p.fetch("""
        SELECT id, chunk_index, total_chunks, content,
               (embedding IS NOT NULL) AS has_embedding,
               metadata, created_at
        FROM core.document_chunks WHERE document_id = $1
        ORDER BY chunk_index
    """, doc_id)

    dd = _row_dict(doc, ["metadata"])
    dd["id"] = str(dd["id"])
    dd["download_url"] = f"/api/documents/{doc_id}/download"
    dd["download_api_url"] = f"/api/documents/{doc_id}/download"
    # Truncate full content for the preview
    if "full_content" in dd and dd["full_content"]:
        dd["full_content_preview"] = dd["full_content"][:2000]
    dd.pop("full_content", None)
    # Remove binary embedding column if present
    dd.pop("embedding", None)

    return JSONResponse({
        "document": dd,
        "chunks": [{
            "id": c["id"],
            "chunk_index": c["chunk_index"],
            "total_chunks": c["total_chunks"],
            "content": c["content"][:500] if c["content"] else "",
            "has_embedding": c["has_embedding"],
            "metadata": json.loads(c["metadata"]) if isinstance(c["metadata"], str) else (c["metadata"] or {}),
        } for c in chunks],
    })


async def api_document_download(request: Request):
    err = await _check_auth(request)
    if err:
        return err

    doc_id = request.path_params["doc_id"]
    disposition = request.query_params.get("disposition", "attachment").lower()
    if disposition not in {"attachment", "inline"}:
        disposition = "attachment"

    # Optional query params let authorized downloaders request privileged/PHI.
    include_privileged = request.query_params.get("include_privileged", "").lower() in ("1","true","yes")
    purpose_of_use = request.query_params.get("purpose_of_use") or None
    auth_id_str = request.query_params.get("authorization_id")
    try:
        auth_uuid = uuid.UUID(auth_id_str) if auth_id_str else None
    except (ValueError, AttributeError):
        return JSONResponse({"error": "invalid authorization_id UUID"}, status_code=400)
    try:
        download = await _resolve_original_document_download(
            doc_id, request=request,
            include_privileged=include_privileged,
            purpose_of_use=purpose_of_use,
            authorization_id=auth_uuid,
        )
    except RuntimeError as exc:
        message = str(exc)
        if message.startswith("privilege_denied") or message.startswith("phi_access_denied"):
            return JSONResponse({"error": message}, status_code=403)
        status = 404 if "not found" in message.lower() or "no downloadable source" in message.lower() else 502
        return JSONResponse({"error": message}, status_code=status)
    except Exception as exc:
        return JSONResponse({"error": f"download failed: {exc}"}, status_code=500)

    headers = {
        "Content-Disposition": _content_disposition_value(download["filename"], disposition),
        "X-Document-Id": download["document_id"],
        "X-Original-Available": "true" if download.get("original_available") else "false",
        "X-Source-Kind": download.get("source_kind", "unknown"),
    }

    local_path = download.get("local_path")
    if local_path:
        return FileResponse(
            local_path,
            media_type=download["mime_type"],
            filename=download["filename"],
            headers=headers,
            content_disposition_type=disposition,
        )

    content_bytes = download.get("content_bytes", b"")
    headers["Content-Length"] = str(len(content_bytes))
    return StreamingResponse(
        io.BytesIO(content_bytes),
        media_type=download["mime_type"],
        headers=headers,
    )


# ── Legal Cases ───────────────────────────────────────────────
async def api_cases(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT c.id, c.case_number, c.court, c.case_title, c.status,
               c.jurisdiction, c.date_filed, c.date_closed,
               COUNT(lcd.document_id) AS doc_count,
               COUNT(lcd.document_id) FILTER
                   (WHERE d.document_type = 'email') AS email_count,
               COUNT(lcd.document_id) FILTER
                   (WHERE d.document_type = 'court_filing') AS filing_count
        FROM legal.cases c
        LEFT JOIN legal.case_documents lcd ON c.id = lcd.case_id
        LEFT JOIN core.documents d ON lcd.document_id = d.id
        GROUP BY c.id ORDER BY c.case_number
    """)
    return JSONResponse([{
        "id": r["id"],
        "case_number": r["case_number"],
        "court": r["court"],
        "case_title": r["case_title"],
        "status": r["status"],
        "jurisdiction": r["jurisdiction"],
        "date_filed": _ser(r["date_filed"]),
        "doc_count": r["doc_count"],
        "email_count": r["email_count"],
        "filing_count": r["filing_count"],
    } for r in rows])


# ── Audit – Access Log ────────────────────────────────────────
async def api_audit_access(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    params = request.query_params
    limit = min(int(params.get("limit", "100")), 500)
    agent_id = params.get("agent_id")
    tool = params.get("tool")

    where, args, idx = [], [], 1
    if agent_id:
        where.append(f"agent_id = ${idx}"); args.append(agent_id); idx += 1
    if tool:
        where.append(f"tool_name = ${idx}"); args.append(tool); idx += 1
    wc = " AND ".join(where) if where else "TRUE"

    rows = await p.fetch(f"""
        SELECT id, timestamp, agent_id, session_id, tool_name,
               action, domain, query, result_count, duration_ms, metadata
        FROM ops.access_log
        WHERE {wc}
        ORDER BY timestamp DESC LIMIT ${idx}
    """, *args, limit)
    return JSONResponse([_row_dict(r, ["metadata"]) for r in rows])


# ── Audit – Search Analytics ──────────────────────────────────
async def api_audit_searches(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    limit = min(int(request.query_params.get("limit", "100")), 500)
    rows = await p.fetch("""
        SELECT id, timestamp, query_text, search_type, domain,
               result_count, top_score, duration_ms, agent_id
        FROM ops.search_analytics ORDER BY timestamp DESC LIMIT $1
    """, limit)
    return JSONResponse([_row_dict(r) for r in rows])


# ── Agent Registry ────────────────────────────────────────────
async def api_agents(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT ar.id, ar.name, ar.description, ar.capabilities,
               ar.is_active, ar.created_at, ar.schedule, ar.priority,
               ar.last_run_at, ar.last_status, ar.run_count, ar.error_count,
               ar.agent_type, ar.version
        FROM ops.agent_registry ar
        ORDER BY ar.priority NULLS LAST, ar.name
    """)
    return JSONResponse([_row_dict(r, ["capabilities"]) for r in rows])


# ── Agent Sessions ────────────────────────────────────────────
async def api_agent_sessions(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    agent_id = request.query_params.get("agent_id")
    limit = min(int(request.query_params.get("limit", "50")), 200)
    if agent_id:
        rows = await p.fetch("""
            SELECT id, agent_id, started_at, ended_at, context, summary
            FROM ops.agent_sessions WHERE agent_id = $1
            ORDER BY started_at DESC LIMIT $2
        """, agent_id, limit)
    else:
        rows = await p.fetch("""
            SELECT id, agent_id, started_at, ended_at, context, summary
            FROM ops.agent_sessions ORDER BY started_at DESC LIMIT $1
        """, limit)
    return JSONResponse([_row_dict(r, ["context"]) for r in rows])




# ── Agent Runs (new agent system) ─────────────────────────────
async def api_agent_runs(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    agent_id = request.query_params.get("agent_id")
    limit = min(int(request.query_params.get("limit", "50")), 200)
    if agent_id:
        rows = await p.fetch("""
            SELECT id, agent_id, status, started_at, ended_at as finished_at,
                   duration_ms, summary, metrics, error as error_message
            FROM ops.agent_runs WHERE agent_id = $1
            ORDER BY started_at DESC LIMIT $2
        """, agent_id, limit)
    else:
        rows = await p.fetch("""
            SELECT id, agent_id, status, started_at, ended_at as finished_at,
                   duration_ms, summary, metrics, error as error_message
            FROM ops.agent_runs ORDER BY started_at DESC LIMIT $1
        """, limit)
    return JSONResponse([_row_dict(r, ["metrics"]) for r in rows])


# ── Agent Findings ────────────────────────────────────────────
async def api_agent_findings(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    status_filter = request.query_params.get("status", "open")
    severity = request.query_params.get("severity")
    limit = min(int(request.query_params.get("limit", "100")), 500)

    q = """SELECT id, agent_id, severity, category, title, detail as description,
                  status, evidence as details, created_at, resolved_at
           FROM ops.agent_findings WHERE 1=1"""
    args = []
    n = 0
    if status_filter and status_filter != "all":
        n += 1; q += f" AND status = ${n}"; args.append(status_filter)
    if severity:
        n += 1; q += f" AND severity = ${n}"; args.append(severity)
    q += f" ORDER BY created_at DESC LIMIT ${n+1}"
    args.append(limit)
    rows = await p.fetch(q, *args)
    return JSONResponse([_row_dict(r, ["details"]) for r in rows])


async def api_agent_finding_resolve(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    fid = request.path_params["id"]
    p = await get_pool()
    await p.execute("""
        UPDATE ops.agent_findings SET status = 'resolved', resolved_at = now()
        WHERE id = $1
    """, int(fid))
    return JSONResponse({"ok": True})


# ── Agent Trigger (run on demand) ─────────────────────────────
async def api_agent_trigger(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    agent_id = request.path_params["agent_id"]
    import subprocess
    try:
        result = subprocess.Popen(
            ["/opt/wdws/venv/bin/python3", "/opt/wdws/agents/run.py", "--once", agent_id],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        return JSONResponse({"ok": True, "message": f"Agent {agent_id} triggered"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ── Agent Toggle (enable/disable) ────────────────────────────
async def api_agent_toggle(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    agent_id = request.path_params["agent_id"]
    p = await get_pool()
    row = await p.fetchrow("SELECT is_active FROM ops.agent_registry WHERE id = $1", agent_id)
    if not row:
        return JSONResponse({"error": "Agent not found"}, status_code=404)
    new_val = not row["is_active"]
    await p.execute("UPDATE ops.agent_registry SET is_active = $1 WHERE id = $2", new_val, agent_id)
    return JSONResponse({"ok": True, "is_active": new_val})


# ── ACL Blacklist / Whitelist ────────────────────────────────
async def api_acl_blacklist(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT id, ip_address::text AS ip_address, source, reason,
               blocked_at, expires_at, is_active, added_by,
               attack_count, last_seen
        FROM ops.acl_blacklist
        ORDER BY last_seen DESC NULLS LAST, blocked_at DESC
        LIMIT 500
    """)
    return JSONResponse([_row_dict(r) for r in rows])


async def api_acl_blacklist_toggle(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    bl_id = request.path_params["id"]
    p = await get_pool()
    row = await p.fetchrow(
        "SELECT is_active FROM ops.acl_blacklist WHERE id = $1", bl_id)
    if not row:
        return JSONResponse({"error": "Not found"}, status_code=404)
    new_val = not row["is_active"]
    await p.execute(
        "UPDATE ops.acl_blacklist SET is_active = $1 WHERE id = $2",
        new_val, bl_id)
    return JSONResponse({"ok": True, "is_active": new_val})


async def api_acl_blacklist_add(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    data = await request.json()
    ip = data.get("ip_address", "").strip()
    reason = data.get("reason", "Manual block")
    if not ip:
        return JSONResponse({"error": "ip_address required"}, status_code=400)
    p = await get_pool()
    await p.execute("""
        INSERT INTO ops.acl_blacklist (ip_address, source, reason, added_by)
        VALUES ($1::inet, 'manual', $2, 'dashboard')
        ON CONFLICT (ip_address) DO UPDATE SET
            is_active = true, reason = $2, last_seen = now()
    """, ip, reason)
    return JSONResponse({"ok": True})


async def api_acl_blacklist_delete(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    bl_id = request.path_params["id"]
    p = await get_pool()
    await p.execute("DELETE FROM ops.acl_blacklist WHERE id = $1", bl_id)
    return JSONResponse({"ok": True})


async def api_acl_whitelist(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    if request.method == "POST":
        data = await request.json()
        ip = data.get("ip_address", "").strip()
        label = data.get("label", "")
        if not ip:
            return JSONResponse({"error": "ip_address required"},
                                status_code=400)
        await p.execute("""
            INSERT INTO ops.acl_whitelist (ip_address, label, added_by)
            VALUES ($1::inet, $2, 'dashboard')
            ON CONFLICT (ip_address) DO UPDATE SET label = $2
        """, ip, label)
        # Also deactivate from blacklist if present
        await p.execute("""
            UPDATE ops.acl_blacklist SET is_active = false
            WHERE ip_address = $1::inet
        """, ip)
        return JSONResponse({"ok": True})
    # GET
    rows = await p.fetch("""
        SELECT id, ip_address::text AS ip_address, label, added_by, created_at
        FROM ops.acl_whitelist ORDER BY created_at DESC
    """)
    return JSONResponse([_row_dict(r) for r in rows])


async def api_acl_whitelist_delete(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    wl_id = request.path_params["id"]
    p = await get_pool()
    await p.execute("DELETE FROM ops.acl_whitelist WHERE id = $1", wl_id)
    return JSONResponse({"ok": True})


# ── Agent Detail (single agent deep view) ─────────────────────
async def api_agent_comms(request: Request):
    """Unified agent communications: logs, messages, fixes, emails."""
    err = await _check_auth(request)
    if err:
        return err
    view = request.query_params.get("view", "timeline")
    agent_filter = request.query_params.get("agent", "")
    level_filter = request.query_params.get("level", "")
    limit = min(int(request.query_params.get("limit", "100")), 500)

    p = await get_pool()
    async with p.acquire() as conn:
        stats = {}
        try:
            stats["total_logs"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ops.agent_logs WHERE created_at > now() - interval '24 hours'") or 0
        except Exception:
            stats["total_logs"] = 0
        stats["total_messages"] = await conn.fetchval(
            "SELECT COUNT(*) FROM ops.agent_messages WHERE created_at > now() - interval '24 hours'") or 0
        try:
            stats["total_fixes"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ops.code_fixes WHERE created_at > now() - interval '7 days'") or 0
            stats["fixes_applied"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ops.code_fixes WHERE applied = true AND created_at > now() - interval '7 days'") or 0
        except Exception:
            stats["total_fixes"] = 0
            stats["fixes_applied"] = 0
        try:
            stats["total_emails"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ops.email_notifications WHERE created_at > now() - interval '7 days'") or 0
        except Exception:
            stats["total_emails"] = 0
        try:
            stats["reasoning_entries"] = await conn.fetchval(
                "SELECT COUNT(*) FROM ops.agent_logs WHERE level = 'reasoning' AND created_at > now() - interval '24 hours'") or 0
        except Exception:
            stats["reasoning_entries"] = 0

        agents = await conn.fetch("SELECT id, name FROM ops.agent_registry ORDER BY name")
        stats["agents"] = [{"id": a["id"], "name": a["name"]} for a in agents]

        data = []

        if view == "timeline":
            where_clauses = ["l.created_at > now() - interval '24 hours'"]
            params = []
            idx = 1
            if agent_filter:
                where_clauses.append(f"l.agent_id = ${idx}")
                params.append(agent_filter)
                idx += 1
            if level_filter:
                where_clauses.append(f"l.level = ${idx}")
                params.append(level_filter)
                idx += 1
            where_sql = " AND ".join(where_clauses)

            try:
                logs = await conn.fetch(f"""
                    SELECT l.id, l.agent_id, l.run_id, l.level, l.category,
                           l.message, l.context, l.created_at,
                           a.name as agent_name
                    FROM ops.agent_logs l
                    LEFT JOIN ops.agent_registry a ON l.agent_id = a.id
                    WHERE {where_sql}
                    ORDER BY l.created_at DESC
                    LIMIT ${idx}
                """, *params, limit)
            except Exception:
                logs = []

            for row in logs:
                ctx = row["context"]
                if isinstance(ctx, str):
                    try:
                        ctx = json.loads(ctx)
                    except Exception:
                        ctx = {}
                data.append({
                    "type": "log",
                    "id": row["id"],
                    "agent_id": row["agent_id"],
                    "agent_name": row["agent_name"] or row["agent_id"],
                    "run_id": row["run_id"],
                    "level": row["level"],
                    "category": row["category"],
                    "message": row["message"],
                    "context": ctx,
                    "timestamp": row["created_at"].isoformat() if row["created_at"] else None,
                })

            msg_where = ["m.created_at > now() - interval '24 hours'"]
            msg_params = []
            msg_idx = 1
            if agent_filter:
                msg_where.append(f"(m.from_agent = ${msg_idx} OR m.to_agent = ${msg_idx})")
                msg_params.append(agent_filter)
                msg_idx += 1
            msg_sql = " AND ".join(msg_where)
            messages = await conn.fetch(f"""
                SELECT m.id, m.from_agent, m.to_agent, m.msg_type, m.subject,
                       m.body, m.priority, m.status, m.created_at,
                       fa.name as from_name, ta.name as to_name
                FROM ops.agent_messages m
                LEFT JOIN ops.agent_registry fa ON m.from_agent = fa.id
                LEFT JOIN ops.agent_registry ta ON m.to_agent = ta.id
                WHERE {msg_sql}
                ORDER BY m.created_at DESC
                LIMIT ${msg_idx}
            """, *msg_params, 50)
            for row in messages:
                body = row["body"]
                if isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except Exception:
                        body = {}
                data.append({
                    "type": "message",
                    "id": row["id"],
                    "from_agent": row["from_agent"],
                    "from_name": row["from_name"] or row["from_agent"],
                    "to_agent": row["to_agent"],
                    "to_name": row["to_name"] or row["to_agent"],
                    "msg_type": row["msg_type"],
                    "subject": row["subject"],
                    "body": body,
                    "priority": row["priority"],
                    "status": row["status"],
                    "timestamp": row["created_at"].isoformat() if row["created_at"] else None,
                })
            data.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
            data = data[:limit]

        elif view == "messages":
            where_parts = ["m.created_at > now() - interval '7 days'"]
            params = []
            idx = 1
            if agent_filter:
                where_parts.append(f"(m.from_agent = ${idx} OR m.to_agent = ${idx})")
                params.append(agent_filter)
                idx += 1
            where_sql = " AND ".join(where_parts)
            messages = await conn.fetch(f"""
                SELECT m.*, fa.name as from_name, ta.name as to_name
                FROM ops.agent_messages m
                LEFT JOIN ops.agent_registry fa ON m.from_agent = fa.id
                LEFT JOIN ops.agent_registry ta ON m.to_agent = ta.id
                WHERE {where_sql}
                ORDER BY m.created_at DESC LIMIT ${idx}
            """, *params, limit)
            for row in messages:
                body = row["body"]
                if isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except Exception:
                        body = {}
                data.append({
                    "id": row["id"], "from_agent": row["from_agent"],
                    "from_name": row["from_name"] or row["from_agent"],
                    "to_agent": row["to_agent"],
                    "to_name": row["to_name"] or row["to_agent"],
                    "msg_type": row["msg_type"], "subject": row["subject"],
                    "body": body, "priority": row["priority"],
                    "status": row["status"],
                    "timestamp": row["created_at"].isoformat() if row["created_at"] else None,
                })

        elif view == "fixes":
            try:
                where_parts = ["cf.created_at > now() - interval '30 days'"]
                params = []
                idx = 1
                if agent_filter:
                    where_parts.append(f"cf.agent_id = ${idx}")
                    params.append(agent_filter)
                    idx += 1
                where_sql = " AND ".join(where_parts)
                fixes = await conn.fetch(f"""
                    SELECT cf.id, cf.agent_id, cf.run_id, cf.target_file, cf.fix_type,
                           cf.original_error, cf.diagnosis, cf.patch, cf.applied,
                           cf.verified, cf.model_used, cf.created_at
                    FROM ops.code_fixes cf
                    WHERE {where_sql}
                    ORDER BY cf.created_at DESC LIMIT ${idx}
                """, *params, limit)
                for row in fixes:
                    patch = row["patch"]
                    if isinstance(patch, str):
                        try:
                            patch = json.loads(patch)
                        except Exception:
                            patch = {}
                    data.append({
                        "id": row["id"], "agent_id": row["agent_id"],
                        "run_id": row["run_id"], "target_file": row["target_file"],
                        "fix_type": row["fix_type"],
                        "original_error": row["original_error"][:500] if row["original_error"] else "",
                        "diagnosis": row["diagnosis"],
                        "patch": patch,
                        "applied": row["applied"], "verified": row["verified"],
                        "model_used": row["model_used"],
                        "timestamp": row["created_at"].isoformat() if row["created_at"] else None,
                    })
            except Exception as e:
                data = [{"error": str(e)}]

        elif view == "emails":
            try:
                where_parts = ["en.created_at > now() - interval '30 days'"]
                params = []
                idx = 1
                if agent_filter:
                    where_parts.append(f"en.agent_id = ${idx}")
                    params.append(agent_filter)
                    idx += 1
                where_sql = " AND ".join(where_parts)
                emails = await conn.fetch(f"""
                    SELECT en.id, en.agent_id, en.run_id, en.recipient, en.subject,
                           en.status, en.error, en.graph_msg_id, en.created_at, en.sent_at
                    FROM ops.email_notifications en
                    WHERE {where_sql}
                    ORDER BY en.created_at DESC LIMIT ${idx}
                """, *params, limit)
                for row in emails:
                    data.append({
                        "id": row["id"], "agent_id": row["agent_id"],
                        "run_id": row["run_id"], "recipient": row["recipient"],
                        "subject": row["subject"], "status": row["status"],
                        "error": row["error"],
                        "timestamp": row["created_at"].isoformat() if row["created_at"] else None,
                        "sent_at": row["sent_at"].isoformat() if row["sent_at"] else None,
                    })
            except Exception as e:
                data = [{"error": str(e)}]

    return JSONResponse({"stats": stats, "data": data, "view": view})


# ── Agent Detail (single agent deep view) ─────────────────────
async def api_agent_detail(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    agent_id = request.path_params["agent_id"]
    p = await get_pool()

    # Agent registry
    agent = await p.fetchrow("""
        SELECT id, name, description, capabilities, is_active, created_at,
               schedule, priority, last_run_at, last_status, run_count,
               error_count, agent_type, version, config
        FROM ops.agent_registry WHERE id = $1
    """, agent_id)
    if not agent:
        return JSONResponse({"error": "Agent not found"}, status_code=404)

    # Recent runs
    runs = await p.fetch("""
        SELECT id, status, started_at, ended_at as finished_at,
               duration_ms, summary, metrics, error as error_message,
               trigger, findings, actions
        FROM ops.agent_runs WHERE agent_id = $1
        ORDER BY started_at DESC LIMIT 20
    """, agent_id)

    # Recent findings
    findings = await p.fetch("""
        SELECT id, severity, category, title, detail as description,
               evidence as details, status, created_at, resolved_at
        FROM ops.agent_findings WHERE agent_id = $1
        ORDER BY created_at DESC LIMIT 30
    """, agent_id)

    # Memory
    memory = await p.fetch("""
        SELECT key, value, updated_at
        FROM ops.agent_memory WHERE agent_id = $1
        ORDER BY updated_at DESC
    """, agent_id)

    # Messages (sent and received)
    messages = await p.fetch("""
        SELECT id, from_agent, to_agent, msg_type, subject, body,
               priority, status, created_at, processed_at
        FROM ops.agent_messages
        WHERE from_agent = $1 OR to_agent = $1
        ORDER BY created_at DESC LIMIT 30
    """, agent_id)

    return JSONResponse({
        "agent": _row_dict(agent, ["capabilities", "config"]),
        "runs": [_row_dict(r, ["metrics"]) for r in runs],
        "findings": [_row_dict(r, ["details"]) for r in findings],
        "memory": [_row_dict(r, ["value"]) for r in memory],
        "messages": [_row_dict(r, ["body"]) for r in messages],
    })


# ── Full-Text Search ──────────────────────────────────────────
async def api_search(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    data = await request.json()
    query = data.get("query", "").strip()
    domain = data.get("domain")
    tag_slug = data.get("tag")  # filter by tag slug (includes descendants)
    limit = min(int(data.get("limit", 20)), 100)
    if not query:
        return JSONResponse({"error": "query required"}, status_code=400)

    p = await get_pool()
    t0 = time.monotonic()

    tag_filter = ""
    extra_args = []
    if tag_slug:
        tag_filter = " AND d.id IN (SELECT dt_f.document_id FROM core.document_tags dt_f WHERE dt_f.tag_id IN (SELECT tag_id FROM core.get_tag_descendants($%d)))"

    async with p.acquire() as conn:
        where_parts = ["c.content_tsv @@ websearch_to_tsquery('english', $1)"]
        args = [query]
        idx = 2
        if domain:
            where_parts.append(f"d.domain = ${idx}")
            args.append(domain); idx += 1
        if tag_slug:
            where_parts.append(f"d.id IN (SELECT dt_f.document_id FROM core.document_tags dt_f WHERE dt_f.tag_id IN (SELECT tag_id FROM core.get_tag_descendants(${idx})))")
            args.append(tag_slug); idx += 1

        safety_domain = domain if domain in ('legal', 'medical') else None
        safety_clause, safety_params, log_cb = await build_document_safety_filter(
            conn=conn, domain=safety_domain,
            caller_context=_dashboard_caller_context(
                'api_search', request=request,
                extra={'query_domain': domain, 'tag': tag_slug},
            ),
            table_alias='d', next_param_index=idx,
        )
        idx += len(safety_params)
        args.extend(safety_params)
        where_parts.append(safety_clause)

        where_clause = " AND ".join(where_parts)
        args.append(limit)
        rows = await conn.fetch(f"""
            SELECT c.id AS chunk_id, c.document_id, c.content,
                   ts_rank_cd(c.content_tsv,
                       websearch_to_tsquery('english', $1))::FLOAT AS rank,
                   d.id, d.domain, d.document_type, d.title, d.filename,
                   d.privilege, d.phi_status,
                   ts_headline('english', c.content,
                       websearch_to_tsquery('english', $1),
                       'MaxWords=60, MinWords=20, StartSel=<mark>, StopSel=</mark>'
                   ) AS headline
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE {where_clause}
            ORDER BY rank DESC LIMIT ${idx}
        """, *args)
        await log_cb(rows)

    dur_ms = (time.monotonic() - t0) * 1000

    # Log to analytics
    try:
        await p.execute("""
            INSERT INTO ops.search_analytics
                (query_text, search_type, domain, result_count, top_score, duration_ms)
            VALUES ($1, 'fulltext', $2, $3, $4, $5)
        """, query, domain, len(rows),
             float(rows[0]["rank"]) if rows else 0.0, dur_ms)
    except Exception:
        pass

    return JSONResponse({
        "query": query,
        "result_count": len(rows),
        "duration_ms": round(dur_ms, 1),
        "results": [{
            "chunk_id": r["chunk_id"],
            "document_id": str(r["document_id"]),
            "domain": r["domain"],
            "document_type": r["document_type"],
            "title": r["title"],
            "filename": r["filename"],
            "rank": round(r["rank"], 4),
            "headline": r["headline"],
        } for r in rows],
    })


# ── ADA Tagging System ───────────────────────────────────────

async def api_ada_tags(request: Request):
    """List all ADA tags with hierarchy and document counts."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        WITH RECURSIVE tag_tree AS (
            SELECT id, name, slug, category, description, parent_id, 0 AS depth
            FROM core.tags WHERE slug = 'ada'
            UNION ALL
            SELECT t.id, t.name, t.slug, t.category, t.description, t.parent_id, tt.depth + 1
            FROM core.tags t JOIN tag_tree tt ON t.parent_id = tt.id
        )
        SELECT tt.id, tt.name, tt.slug, tt.category, tt.description,
               tt.parent_id, tt.depth,
               COUNT(DISTINCT dt.document_id) AS document_count
        FROM tag_tree tt
        LEFT JOIN core.document_tags dt ON tt.id = dt.tag_id
        GROUP BY tt.id, tt.name, tt.slug, tt.category, tt.description,
                 tt.parent_id, tt.depth
        ORDER BY tt.depth, tt.name
    """)
    return JSONResponse([{
        "id": r["id"], "name": r["name"], "slug": r["slug"],
        "category": r["category"], "description": r["description"],
        "parent_id": r["parent_id"], "depth": r["depth"],
        "document_count": r["document_count"],
    } for r in rows])


async def api_ada_agency_summary(request: Request):
    """Per-agency ADA document summary for systemic analysis."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT * FROM core.v_ada_agency_summary
    """)
    return JSONResponse([{
        "agency": r["agency"], "slug": r["slug"],
        "description": r["description"],
        "document_count": r["document_count"],
        "earliest_document": _ser(r["earliest_document"]),
        "latest_document": _ser(r["latest_document"]),
    } for r in rows])


async def api_ada_search(request: Request):
    """Search documents by ADA tag slug. Includes all child tags.

    Query params:
      tag — tag slug (e.g. wa:ada, wa:ada:dshs, ada:type:request)
      limit — max results (default 50, max 200)
      offset — pagination offset
    """
    err = await _check_auth(request)
    if err:
        return err
    tag_slug = request.query_params.get("tag", "wa:ada")
    limit = min(int(request.query_params.get("limit", "50")), 200)
    offset = int(request.query_params.get("offset", "0"))
    p = await get_pool()
    async with p.acquire() as conn:
        # core.search_by_tag() is a stored function — we can't compose the
        # safety filter into it. Post-filter via a JOIN against core.documents
        # with the safety clause. Cross-domain (any domain might be in the
        # tag's set).
        safety_clause, safety_params, log_cb = await build_document_safety_filter(
            conn=conn, domain=None,
            caller_context=_dashboard_caller_context(
                'api_ada_search', request=request, extra={'tag': tag_slug},
            ),
            table_alias='d', next_param_index=4,  # $1=tag, $2=limit, $3=offset
        )
        rows = await conn.fetch(f"""
            SELECT t.*
            FROM core.search_by_tag($1, $2, $3) t
            JOIN core.documents d ON d.id = t.document_id
            WHERE {safety_clause}
        """, tag_slug, limit, offset, *safety_params)
        await log_cb(rows)

        # Total applies the same filter so the caller sees accessible count only.
        total = await conn.fetchval(f"""
            SELECT COUNT(DISTINCT dt.document_id)
            FROM core.document_tags dt
            JOIN core.documents d ON d.id = dt.document_id
            WHERE dt.tag_id IN (SELECT tag_id FROM core.get_tag_descendants($1))
              AND {safety_clause}
        """, tag_slug, *safety_params)

    return JSONResponse({
        "tag": tag_slug,
        "total": total,
        "limit": limit,
        "offset": offset,
        "documents": [{
            "document_id": str(r["document_id"]),
            "title": r["title"],
            "domain": r["domain"],
            "document_type": r["document_type"],
            "filename": r["filename"],
            "created_at": _ser(r["created_at"]),
            "tag_names": r["tag_names"],
            "case_number": r["case_number"],
            "sender": r["sender"],
            "direction": r["direction"],
            "date_sent": _ser(r["date_sent"]),
        } for r in rows],
    })


async def api_ada_tag_document(request: Request):
    """Tag a document with one or more ADA tags.

    POST body: { "document_id": "uuid", "tags": ["wa:ada:dshs", "ada:type:request"] }
    """
    err = await _check_auth(request)
    if err:
        return err
    data = await request.json()
    doc_id = data.get("document_id")
    tags = data.get("tags", [])
    if not doc_id or not tags:
        return JSONResponse({"error": "document_id and tags required"}, status_code=400)
    p = await get_pool()
    count = await p.fetchval("""
        SELECT core.tag_document_multi($1::uuid, $2::text[])
    """, doc_id, tags)
    return JSONResponse({"document_id": doc_id, "tags_applied": count})


async def api_ada_auto_tag(request: Request):
    """Run auto-tagging on untagged documents.

    POST body: { "limit": 1000 } (optional)
    """
    err = await _check_auth(request)
    if err:
        return err
    data = await request.json()
    limit = min(int(data.get("limit", 1000)), 5000)
    p = await get_pool()
    rows = await p.fetch("""
        SELECT * FROM core.auto_tag_all_untagged($1)
    """, limit)
    results = [{"document_id": str(r["document_id"]), "tags_applied": r["tags_applied"]}
               for r in rows if r["tags_applied"]]
    return JSONResponse({
        "documents_scanned": len(rows),
        "documents_tagged": len(results),
        "results": results,
    })


async def api_ada_auto_tag_single(request: Request):
    """Run auto-tagging on a single document.

    POST body: { "document_id": "uuid" }
    """
    err = await _check_auth(request)
    if err:
        return err
    data = await request.json()
    doc_id = data.get("document_id")
    if not doc_id:
        return JSONResponse({"error": "document_id required"}, status_code=400)
    p = await get_pool()
    tags = await p.fetchval("""
        SELECT core.auto_tag_document($1::uuid)
    """, doc_id)
    return JSONResponse({"document_id": doc_id, "tags_applied": tags or []})


async def api_ada_document_tags(request: Request):
    """Get all tags for a specific document."""
    err = await _check_auth(request)
    if err:
        return err
    doc_id = request.path_params["doc_id"]
    p = await get_pool()
    rows = await p.fetch("""
        SELECT t.id, t.name, t.slug, t.category, t.description,
               p.name AS parent_name, p.slug AS parent_slug
        FROM core.document_tags dt
        JOIN core.tags t ON dt.tag_id = t.id
        LEFT JOIN core.tags p ON t.parent_id = p.id
        WHERE dt.document_id = $1::uuid
        ORDER BY t.category, t.name
    """, doc_id)
    return JSONResponse([{
        "id": r["id"], "name": r["name"], "slug": r["slug"],
        "category": r["category"], "description": r["description"],
        "parent_name": r["parent_name"], "parent_slug": r["parent_slug"],
    } for r in rows])


async def api_ada_remove_tag(request: Request):
    """Remove a tag from a document.

    DELETE /api/ada/documents/{doc_id}/tags/{tag_slug}
    """
    err = await _check_auth(request)
    if err:
        return err
    doc_id = request.path_params["doc_id"]
    tag_slug = request.path_params["tag_slug"]
    p = await get_pool()
    await p.execute("""
        DELETE FROM core.document_tags
        WHERE document_id = $1::uuid
          AND tag_id = (SELECT id FROM core.tags WHERE slug = $2)
    """, doc_id, tag_slug)
    return JSONResponse({"ok": True})


async def api_ada_tag_rules(request: Request):
    """List all auto-tagging rules."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT tr.id, tr.rule_type, tr.pattern, tr.priority,
               tr.is_active, tr.description, tr.created_at,
               t.name AS tag_name, t.slug AS tag_slug
        FROM core.tag_rules tr
        JOIN core.tags t ON tr.tag_id = t.id
        ORDER BY t.slug, tr.priority
    """)
    return JSONResponse([{
        "id": r["id"], "rule_type": r["rule_type"], "pattern": r["pattern"],
        "priority": r["priority"], "is_active": r["is_active"],
        "description": r["description"], "created_at": _ser(r["created_at"]),
        "tag_name": r["tag_name"], "tag_slug": r["tag_slug"],
    } for r in rows])


async def api_ada_systemic_report(request: Request):
    """Generate a systemic ADA violation report across all WA agencies.

    Returns per-agency breakdown with document counts, date ranges,
    and document type distribution for building a pattern-and-practice case.
    """
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()

    # Per-agency stats
    agencies = await p.fetch("""SELECT * FROM core.v_ada_agency_summary""")

    # Doc-type distribution per agency
    type_dist = await p.fetch("""
        SELECT
            agency_t.name AS agency,
            agency_t.slug AS agency_slug,
            dtype.name AS doc_type,
            dtype.slug AS doc_type_slug,
            COUNT(DISTINCT dt.document_id) AS count
        FROM core.document_tags dt
        JOIN core.tags agency_t ON dt.tag_id = agency_t.id
        JOIN core.document_tags dt2 ON dt.document_id = dt2.document_id
        JOIN core.tags dtype ON dt2.tag_id = dtype.id AND dtype.category = 'ada-doc-type'
        WHERE agency_t.category IN ('wa-agency', 'wa-employer', 'wa-healthcare')
        GROUP BY agency_t.name, agency_t.slug, dtype.name, dtype.slug
        ORDER BY agency_t.name, count DESC
    """)

    # Total unique documents in WA ADA umbrella
    total_docs = await p.fetchval("""
        SELECT COUNT(DISTINCT dt.document_id)
        FROM core.document_tags dt
        WHERE dt.tag_id IN (SELECT tag_id FROM core.get_tag_descendants('wa:ada'))
    """)

    # Build type distribution by agency
    dist_by_agency = {}
    for r in type_dist:
        slug = r["agency_slug"]
        if slug not in dist_by_agency:
            dist_by_agency[slug] = []
        dist_by_agency[slug].append({
            "doc_type": r["doc_type"],
            "doc_type_slug": r["doc_type_slug"],
            "count": r["count"],
        })

    return JSONResponse({
        "title": "WA State ADA Systemic Violation Report",
        "total_documents": total_docs,
        "agencies": [{
            "agency": r["agency"],
            "slug": r["slug"],
            "description": r["description"],
            "document_count": r["document_count"],
            "earliest_document": _ser(r["earliest_document"]),
            "latest_document": _ser(r["latest_document"]),
            "document_types": dist_by_agency.get(r["slug"], []),
        } for r in agencies],
    })


# ── Timeline (recent activity) ────────────────────────────────
async def api_timeline(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    limit = min(int(request.query_params.get("limit", "30")), 100)
    rows = await p.fetch("""
        (SELECT 'document' AS event_type, domain,
                document_type AS detail,
                title AS description, filename, created_at AS ts
         FROM core.documents ORDER BY created_at DESC LIMIT $1)
        UNION ALL
        (SELECT 'ingestion', domain, status,
                source, NULL, started_at
         FROM ops.ingestion_jobs ORDER BY started_at DESC LIMIT $1)
        UNION ALL
        (SELECT 'search', domain, search_type,
                query_text, NULL, timestamp
         FROM ops.search_analytics ORDER BY timestamp DESC LIMIT $1)
        UNION ALL
        (SELECT 'access', domain, action,
                tool_name, query, timestamp
         FROM ops.access_log ORDER BY timestamp DESC LIMIT $1)
        ORDER BY ts DESC LIMIT $1
    """, limit)
    return JSONResponse([{
        "event_type": r["event_type"],
        "domain": r["domain"],
        "detail": r["detail"],
        "description": r["description"],
        "filename": r["filename"],
        "timestamp": _ser(r["ts"]),
    } for r in rows])


# ── DB Health ─────────────────────────────────────────────────
async def api_db_health(request: Request):
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    db_size = await p.fetchval(
        "SELECT pg_size_pretty(pg_database_size(current_database()))")
    version = await p.fetchval("SELECT version()")
    uptime = await p.fetchval(
        "SELECT now() - pg_postmaster_start_time()")
    tables = await p.fetch("""
        SELECT schemaname, relname AS tablename,
               n_live_tup AS row_count,
               pg_size_pretty(pg_total_relation_size(
                   schemaname || '.' || relname)) AS size
        FROM pg_stat_user_tables ORDER BY n_live_tup DESC
    """)
    return JSONResponse({
        "database_size": db_size,
        "version": version,
        "uptime": str(uptime),
        "tables": [{
            "schema": t["schemaname"],
            "table": t["tablename"],
            "rows": t["row_count"],
            "size": t["size"],
        } for t in tables],
    })


# ── Email Sync Management ────────────────────────────────

async def api_email_mailboxes(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    if request.method == "POST":
        data = await request.json()
        email = data.get("email", "").strip()
        if not email:
            return JSONResponse({"error": "email required"}, status_code=400)
        try:
            await p.execute(
                "INSERT INTO ops.sync_mailboxes (email, display_name) VALUES ($1, $2)",
                email, data.get("display_name", ""),
            )
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse({"ok": True})
    rows = await p.fetch("SELECT * FROM ops.sync_mailboxes ORDER BY id")
    return JSONResponse([{
        "id": r["id"], "email": r["email"],
        "display_name": r["display_name"],
        "is_active": r["is_active"],
        "last_sync_at": r["last_sync_at"].isoformat() if r["last_sync_at"] else None,
        "total_synced": r["total_synced"],
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
    } for r in rows])


async def api_email_mailbox_action(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    mid = int(request.path_params["id"])
    if request.method == "DELETE":
        await p.execute("DELETE FROM ops.sync_mailboxes WHERE id = $1", mid)
        return JSONResponse({"ok": True})
    data = await request.json()
    if "is_active" in data:
        await p.execute(
            "UPDATE ops.sync_mailboxes SET is_active = $2 WHERE id = $1",
            mid, data["is_active"],
        )
    if "email" in data:
        await p.execute(
            "UPDATE ops.sync_mailboxes SET email = $2, display_name = $3 WHERE id = $1",
            mid, data["email"], data.get("display_name", ""),
        )
    return JSONResponse({"ok": True})


async def api_email_rules(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    if request.method == "POST":
        data = await request.json()
        name = data.get("name", "").strip()
        rule_type = data.get("rule_type", "domain")
        pattern = data.get("pattern", "").strip()
        if not name or not pattern:
            return JSONResponse({"error": "name and pattern required"}, status_code=400)
        case_nums = data.get("case_numbers", [])
        if isinstance(case_nums, str):
            case_nums = [c.strip() for c in case_nums.split(",") if c.strip()]
        try:
            await p.execute("""
                INSERT INTO ops.sync_rules
                    (name, rule_type, pattern, case_numbers, description, priority)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, name, rule_type, pattern, case_nums,
                data.get("description", ""), data.get("priority", 0))
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse({"ok": True})
    rows = await p.fetch(
        "SELECT * FROM ops.sync_rules ORDER BY priority DESC, id"
    )
    return JSONResponse([{
        "id": r["id"], "name": r["name"],
        "rule_type": r["rule_type"], "pattern": r["pattern"],
        "case_numbers": list(r["case_numbers"]) if r["case_numbers"] else [],
        "is_active": r["is_active"],
        "priority": r["priority"],
        "description": r["description"],
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
    } for r in rows])


async def api_email_rule_action(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    rid = int(request.path_params["id"])
    if request.method == "DELETE":
        await p.execute("DELETE FROM ops.sync_rules WHERE id = $1", rid)
        return JSONResponse({"ok": True})
    data = await request.json()
    sets, vals, i = [], [rid], 2
    for fld in ["name", "rule_type", "pattern", "description", "is_active", "priority"]:
        if fld in data:
            sets.append(f"{fld} = ${i}")
            vals.append(data[fld])
            i += 1
    if "case_numbers" in data:
        cn = data["case_numbers"]
        if isinstance(cn, str):
            cn = [c.strip() for c in cn.split(",") if c.strip()]
        sets.append(f"case_numbers = ${i}")
        vals.append(cn)
        i += 1
    if sets:
        await p.execute(
            f"UPDATE ops.sync_rules SET {', '.join(sets)} WHERE id = $1",
            *vals,
        )
    return JSONResponse({"ok": True})


async def api_email_runs(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    limit = int(request.query_params.get("limit", "20"))
    rows = await p.fetch(
        "SELECT * FROM ops.sync_runs ORDER BY started_at DESC LIMIT $1", limit
    )
    return JSONResponse([{
        "id": r["id"],
        "started_at": r["started_at"].isoformat() if r["started_at"] else None,
        "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
        "status": r["status"],
        "trigger_type": r["trigger_type"],
        "mailboxes_scanned": list(r["mailboxes_scanned"]) if r["mailboxes_scanned"] else [],
        "emails_found": r["emails_found"],
        "emails_ingested": r["emails_ingested"],
        "emails_skipped": r["emails_skipped"],
        "emails_failed": r["emails_failed"],
        "chunks_created": r["chunks_created"],
        "ai_classified": r["ai_classified"],
        "duration_seconds": float(r["duration_seconds"]) if r["duration_seconds"] else None,
    } for r in rows])


async def api_email_trigger_sync(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    running = await p.fetchval(
        "SELECT COUNT(*) FROM ops.sync_runs WHERE status = 'running'"
    )
    if running > 0:
        return JSONResponse({"error": "Sync already running"}, status_code=409)
    try:
        log_file = open(SYNC_LOG, "a")
        subprocess.Popen(
            ["/opt/wdws/venv/bin/python3", "/opt/wdws/email_sync.py"],
            stdout=log_file, stderr=subprocess.STDOUT,
            cwd="/opt/wdws",
        )
        return JSONResponse({"ok": True, "message": "Email sync started"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_email_sync_live(request: Request):
    err = await _check_auth(request)
    if err: return err
    lines = int(request.query_params.get("lines", "40"))
    log_text = ""
    try:
        result = subprocess.run(
            ["tail", f"-{lines}", SYNC_LOG],
            capture_output=True, text=True, timeout=5,
        )
        log_text = result.stdout
    except Exception:
        log_text = "(sync log file not available)"
    p = await get_pool()
    running = await p.fetchval(
        "SELECT COUNT(*) FROM ops.sync_runs WHERE status = 'running'"
    )
    return JSONResponse({"log": log_text, "running": running > 0})


async def api_email_classifications(request: Request):
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    limit = int(request.query_params.get("limit", "50"))
    urgency = request.query_params.get("urgency")

    params = []
    where = ""
    if urgency:
        where = " WHERE ac.classification->>'urgency' = $1"
        params.append(urgency)

    param_idx = len(params) + 1
    rows = await p.fetch(f"""
        SELECT ac.id, ac.document_id, ac.model_used, ac.classification,
               ac.confidence, ac.processed_at,
               d.title, d.filename, d.created_at AS doc_created,
               em.sender, em.recipients, em.date_sent, em.direction, em.mailbox
        FROM ops.ai_classifications ac
        JOIN core.documents d ON ac.document_id = d.id
        LEFT JOIN legal.email_metadata em ON d.id = em.document_id
        {where}
        ORDER BY ac.processed_at DESC LIMIT ${param_idx}
    """, *params, limit)

    total = await p.fetchval("SELECT COUNT(*) FROM ops.ai_classifications")

    return JSONResponse({
        "total": total,
        "classifications": [{
            "id": r["id"],
            "document_id": str(r["document_id"]),
            "model": r["model_used"],
            "classification": r["classification"],
            "confidence": float(r["confidence"]) if r["confidence"] else None,
            "processed_at": r["processed_at"].isoformat() if r["processed_at"] else None,
            "title": r["title"],
            "filename": r["filename"],
            "sender": r["sender"],
            "recipients": r["recipients"],
            "direction": r["direction"],
            "mailbox": r["mailbox"],
        } for r in rows],
    })


async def api_email_trigger_classify(request: Request):
    err = await _check_auth(request)
    if err: return err
    data = await request.json()
    limit = data.get("limit", 50)
    try:
        log_file = open(AGENT_LOG, "a")
        cmd = [
            "/opt/wdws/venv/bin/python3", "/opt/wdws/email_agent.py",
            "--backfill", str(limit),
        ]
        if data.get("reclassify"):
            cmd.append("--reclassify")
        if data.get("doc_id"):
            cmd.extend(["--doc-id", data["doc_id"]])
        subprocess.Popen(
            cmd, stdout=log_file, stderr=subprocess.STDOUT, cwd="/opt/wdws",
        )
        return JSONResponse({"ok": True, "message": f"AI classification started (limit={limit})"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Chat-DB Email System (proxied to Athena Chat API 9350) ─────
CHAT_API_BASE = os.getenv("CHAT_API_URL", "http://127.0.0.1:9350")

async def _chat_api_proxy(path: str, method: str = "GET", json_body: dict = None) -> dict:
    """Proxy a request to the Athena Chat API."""
    async with httpx.AsyncClient(timeout=30) as client:
        url = f"{CHAT_API_BASE}{path}"
        if method == "GET":
            resp = await client.get(url)
        elif method == "POST":
            resp = await client.post(url, json=json_body or {})
        elif method == "DELETE":
            resp = await client.delete(url)
        elif method == "PATCH":
            resp = await client.patch(url, json=json_body or {})
        else:
            resp = await client.request(method, url, json=json_body)
        resp.raise_for_status()
        return resp.json()


async def api_chat_email_accounts(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/api/email/accounts")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_sync_status(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/api/email/sync")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_trigger_sync(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        body = await request.json() if request.method == "POST" else {}
        data = await _chat_api_proxy("/api/email/sync", "POST", body)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_messages(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        qs = str(request.query_params)
        path = f"/api/email/messages?{qs}" if qs else "/api/email/messages"
        data = await _chat_api_proxy(path)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_action_required(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/api/email/action-required")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_semantic_search(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        qs = str(request.query_params)
        path = f"/api/email/semantic-search?{qs}" if qs else "/api/email/semantic-search"
        data = await _chat_api_proxy(path)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_search(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        qs = str(request.query_params)
        path = f"/api/email/search?{qs}" if qs else "/api/email/search"
        data = await _chat_api_proxy(path)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_webhooks(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        if request.method == "POST":
            body = await request.json() if request.method == "POST" else {}
            data = await _chat_api_proxy("/api/email/webhooks", "POST", body)
        else:
            data = await _chat_api_proxy("/api/email/webhooks")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_webhooks_renew(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/api/email/webhooks/renew", "POST", {})
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_email_webhooks_delete(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        sub_id = request.path_params["sub_id"]
        data = await _chat_api_proxy(f"/api/email/webhooks/{sub_id}", "DELETE")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── OneDrive Integration (proxy to chat_api) ─────────────────

async def api_onedrive_stats(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/onedrive/stats")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_targets(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        data = await _chat_api_proxy("/onedrive/targets")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_target_toggle(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        tid = request.path_params["target_id"]
        data = await _chat_api_proxy(f"/onedrive/targets/{tid}/toggle", "POST", {})
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_files(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        qs = str(request.url.query)
        path = f"/onedrive/files?{qs}" if qs else "/onedrive/files"
        data = await _chat_api_proxy(path)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_search(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        qs = str(request.url.query)
        path = f"/onedrive/search?{qs}" if qs else "/onedrive/search"
        data = await _chat_api_proxy(path)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_sync(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        body = await request.json() if request.method == "POST" else {}
        data = await _chat_api_proxy("/onedrive/sync", "POST", body)
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_webhooks(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        if request.method == "POST":
            data = await _chat_api_proxy("/onedrive/webhooks", "POST", {})
        else:
            data = await _chat_api_proxy("/onedrive/webhooks")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_onedrive_webhook_delete(request: Request):
    err = await _check_auth(request)
    if err: return err
    try:
        sub_id = request.path_params["sub_id"]
        data = await _chat_api_proxy(f"/onedrive/webhooks/{sub_id}", "DELETE")
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ══════════════════════════════════════════════════════════════
#  CHAT ENGINE  — OpenAI + Tool Calling
# ══════════════════════════════════════════════════════════════

CHAT_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "semantic_search",
            "description": "Search the enterprise document database using semantic vector similarity. Use for finding documents related to concepts, topics, or natural language queries across all domains (legal, medical, paperless).",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural language search query"},
                    "domain": {"type": "string", "enum": ["legal", "medical", "paperless"], "description": "Optional domain filter"},
                    "tag": {"type": "string", "description": "Optional tag slug to restrict search to tagged documents (e.g. 'wa:ada', 'wa:ada:dshs', 'wa:ada:snoco-court')"},
                    "limit": {"type": "integer", "default": 8, "description": "Number of results (1-20)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "fulltext_search",
            "description": "Search documents using PostgreSQL full-text search. Good for exact phrases, case numbers, names, specific terms.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search keywords or phrases"},
                    "domain": {"type": "string", "enum": ["legal", "medical", "paperless"], "description": "Optional domain filter"},
                    "tag": {"type": "string", "description": "Optional tag slug to restrict search to tagged documents (e.g. 'wa:ada', 'wa:ada:dshs', 'wa:ada:snoco-court')"},
                    "limit": {"type": "integer", "default": 10}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "lookup_case",
            "description": "Look up a specific legal case by case number and get its details, parties, and associated documents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "case_number": {"type": "string", "description": "Case number e.g. '24-2-01031-31' or '325cv05551dge'"}
                },
                "required": ["case_number"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_document",
            "description": "Retrieve the full content and metadata of a specific document by its ID.",
            "parameters": {
                "type": "object",
                "properties": {
                    "document_id": {"type": "string", "description": "UUID of the document"}
                },
                "required": ["document_id"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_emails",
            "description": "Search email messages by sender, recipient, subject, or content. Can filter by mailbox, direction, and date range.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search terms for email content/subject"},
                    "sender": {"type": "string", "description": "Filter by sender email/name"},
                    "mailbox": {"type": "string", "description": "Filter by mailbox"},
                    "limit": {"type": "integer", "default": 10}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "list_cases",
            "description": "List all legal cases in the system with their status, court, and document counts.",
            "parameters": {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "description": "Filter by status: active, closed, all"},
                },
                "required": []
            }
        }
    },
    {
      "type": "function",
      "function": {
        "name": "list_case_documents",
        "description": "List documents for a specific case with optional document_type filter and pagination.",
        "parameters": {
          "type": "object",
          "properties": {
            "case_number": {"type": "string", "description": "Case number e.g. '24-2-01031-31' or '325cv05551dge'"},
            "document_type": {"type": "string", "description": "Optional document type filter (e.g., court_filing, email, eml)"},
            "limit": {"type": "integer", "default": 50, "description": "Max docs to return (1-200)"},
            "offset": {"type": "integer", "default": 0, "description": "Pagination offset"}
          },
          "required": ["case_number"]
        }
      }
    },
    {
      "type": "function",
      "function": {
        "name": "get_documents_bulk",
        "description": "Fetch multiple documents by ID in one call (content is truncated per document).",
        "parameters": {
          "type": "object",
          "properties": {
            "document_ids": {"type": "array", "items": {"type": "string"}, "description": "List of document UUIDs (max 20)"},
            "max_chars": {"type": "integer", "default": 1200, "description": "Max characters per document (200-4000)"}
          },
          "required": ["document_ids"]
        }
      }
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the internet using DuckDuckGo for current information, legal references, medical info, or anything not in the local database.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Web search query"},
                    "num_results": {"type": "integer", "default": 5}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "database_stats",
            "description": "Get current system statistics — total documents, chunks, embeddings, cases, entities, etc.",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "consult_agents",
            "description": "Consult the team of 12 specialist AI agents for expert analysis. Posts your question to the agent chatroom and collects responses from: Orchestrator, Watchdog, Code Doctor, Security Sentinel, Self-Healing, Data Quality, Email Triage, DB Tuner, Case Strategy, Retention, Timeline, and Query Insight. Use this when you need specialized domain expertise, cross-cutting analysis, or want multiple perspectives on a problem. Takes ~30-60 seconds as agents analyze and respond.",
            "parameters": {
                "type": "object",
                "properties": {
                    "question": {"type": "string", "description": "The question or analysis request to send to all agents"},
                    "context": {"type": "string", "description": "Optional additional context from the current conversation to help agents give better answers"}
                },
                "required": ["question"]
            }
        }
    },
]

SYSTEM_PROMPT = """You are **Athena AI**, an elite Legal Analyst and Investigative Specialist serving as William Nelson's dedicated research assistant.

## Identity & Expertise

You specialize in:
- **Civil Litigation**: Procedure, strategy, discovery, motions practice
- **Employment Law**: Wrongful termination, discrimination, retaliation, hostile work environment
- **Insurance**: Bad faith claims, ERISA, disability insurance disputes
- **Evidence Analysis**: Timeline reconstruction, document review, pattern recognition
- **Case Strategy**: Identifying strengths, weaknesses, opportunities, and risks

## Available Resources

1. **Enterprise Document Database** — Legal filings, court documents, emails, medical records with vector search
2. **Legal Case Management** — All court cases with parties, docket entries, and associated documents
3. **Email Archive** — Indexed emails with threading, sender/recipient metadata, AI classifications
4. **Web Search** — Internet search via DuckDuckGo for current information
5. **System Telemetry** — Database statistics and health
6. **Agent Team** — 12 specialist AI agents you can consult:
   - **Orchestrator**: Overall coordination and system health
   - **Watchdog**: System monitoring, alerts, anomaly detection
   - **Code Doctor**: Code quality, bug analysis, technical health
   - **Security Sentinel**: Security threats, vulnerabilities
   - **Self-Healing**: Automated recovery, error correction
   - **Data Quality**: Data integrity, validation, completeness
   - **Email Triage**: Email classification, priority, routing
   - **DB Tuner**: Database performance, query optimization
   - **Case Strategy**: Legal case analysis, strategy recommendations
   - **Retention**: Document retention, compliance, archival
   - **Timeline**: Event sequencing, deadline tracking
   - **Query Insight**: Query patterns, usage analytics

## Investigation Methodology

### Evidence Chain Process
1. Identify the claim or assertion being investigated
2. Search for supporting evidence in documents and emails
3. Search for contradicting evidence
4. Document findings with specific citations
5. Note gaps in the record

### Timeline Reconstruction
- Extract dates from documents systematically
- Build chronological sequences
- Identify inconsistencies
- Note missing time periods

### Pattern Recognition
- Compare similar documents across cases
- Identify recurring parties, behaviors, or language
- Flag anomalies or contradictions

## Core Principles

1. **NEVER fabricate** — If you cannot find evidence, say so clearly
2. **ALWAYS cite sources** — Reference document filenames, case numbers, and dates
3. **PRIORITIZE accuracy** — Double-check important facts before reporting
4. **MAINTAIN privilege** — Be aware of attorney-client privilege
5. **FLAG deadlines** — Immediately highlight time-sensitive items
6. **BE THOROUGH** — Use multiple tools to give complete answers

## Guidelines

- Always search the database before saying information doesn't exist
- For legal questions, search both court filings and emails for complete context
- Use semantic search for conceptual queries, fulltext search for exact terms/names
- When asked about a case, use lookup_case to get full details
- For large case reviews (e.g., “all pleadings”), use list_case_documents to inventory documents, then get_documents_bulk in batches to reduce tool calls
- Prefer batching and pagination over repeating the same tool call
- Use consult_agents when you need specialist analysis or domain expertise
- When consulting agents, summarize their responses clearly
- Think step-by-step through complex questions
- Present findings in organized, actionable format
"""


async def _embed_query(text: str) -> List[float]:
    """Get embedding vector for a search query using local BGE-M3 model."""
    return await _embed_query_local(text)


async def _tool_semantic_search(args: dict) -> str:
    """Semantic vector search against document chunks."""
    query = args["query"]
    domain = args.get("domain")
    tag_slug = args.get("tag")
    limit = min(args.get("limit", 8), 20)
    try:
        embedding = await _embed_query(query)
        vec_literal = _vec_literal(embedding)
        p = await get_pool()
        # Build WHERE clause dynamically based on filters
        where_parts = ["c.embedding IS NOT NULL"]
        q_args = [vec_literal]
        idx = 2
        if domain:
            where_parts.append(f"d.domain = ${idx}")
            q_args.append(domain)
            idx += 1
        if tag_slug:
            where_parts.append(
                f"c.document_id IN ("
                f"SELECT dt_f.document_id FROM core.document_tags dt_f "
                f"WHERE dt_f.tag_id IN (SELECT tag_id FROM core.get_tag_descendants(${idx}))"
                f")"
            )
            q_args.append(tag_slug)
            idx += 1
        async with p.acquire() as conn:
            safety_domain = domain if domain in ('legal', 'medical') else None
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain=safety_domain,
                caller_context=_dashboard_caller_context(
                    '_tool_semantic_search', extra={'query_domain': domain},
                ),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            q_args.extend(safety_params)
            where_parts.append(safety_clause)

            q_args.append(limit)
            where_clause = " AND ".join(where_parts)
            rows = await conn.fetch(f"""
                SELECT c.id, c.content, c.chunk_index,
                       d.id AS doc_id, d.id, d.title, d.filename, d.domain, d.document_type,
                       d.privilege, d.phi_status,
                       c.embedding <=> $1::halfvec({EMBEDDING_DIMENSIONS}) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE {where_clause}
                ORDER BY c.embedding <=> $1::halfvec({EMBEDDING_DIMENSIONS})
                LIMIT ${idx}
            """, *q_args)
            await log_cb(rows)
        results = []
        for r in rows:
            sim = round(1 - float(r["distance"]), 4)
            results.append({
                "document_id": str(r["doc_id"]),
                "title": r["title"] or r["filename"],
                "filename": r["filename"],
                "domain": r["domain"],
                "type": r["document_type"],
                "similarity": sim,
                "excerpt": r["content"][:600],
            })
        return json.dumps({"results": results, "count": len(results), "query": query})
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_fulltext_search(args: dict) -> str:
    """PostgreSQL full-text search."""
    query = args["query"]
    domain = args.get("domain")
    tag_slug = args.get("tag")
    limit = min(args.get("limit", 10), 30)
    p = await get_pool()
    try:
        where_parts = ["c.content_tsv @@ websearch_to_tsquery('english', $1)"]
        q_args = [query]
        idx = 2
        if domain:
            where_parts.append(f"d.domain = ${idx}")
            q_args.append(domain)
            idx += 1
        if tag_slug:
            where_parts.append(
                f"c.document_id IN ("
                f"SELECT dt_f.document_id FROM core.document_tags dt_f "
                f"WHERE dt_f.tag_id IN (SELECT tag_id FROM core.get_tag_descendants(${idx}))"
                f")"
            )
            q_args.append(tag_slug)
            idx += 1
        async with p.acquire() as conn:
            safety_domain = domain if domain in ('legal', 'medical') else None
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain=safety_domain,
                caller_context=_dashboard_caller_context(
                    '_tool_fulltext_search', extra={'query_domain': domain},
                ),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            q_args.extend(safety_params)
            where_parts.append(safety_clause)

            q_args.append(limit)
            where_clause = " AND ".join(where_parts)
            rows = await conn.fetch(f"""
                SELECT c.id, c.content, d.id AS doc_id, d.id, d.title, d.filename,
                       d.domain, d.document_type, d.privilege, d.phi_status,
                       ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', $1))::FLOAT AS rank
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE {where_clause}
                ORDER BY rank DESC LIMIT ${idx}
            """, *q_args)
            await log_cb(rows)
        results = [{"document_id": str(r["doc_id"]), "title": r["title"] or r["filename"],
                    "filename": r["filename"], "domain": r["domain"], "type": r["document_type"],
                    "rank": round(r["rank"], 4), "excerpt": r["content"][:600]}
                   for r in rows]
        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_lookup_case(args: dict) -> str:
    """Look up a legal case with parties and doc counts."""
    cn = args["case_number"].strip()
    p = await get_pool()
    try:
        case = await p.fetchrow("""
            SELECT c.*, COUNT(cd.document_id) AS doc_count
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            WHERE c.case_number ILIKE $1
            GROUP BY c.id
        """, f"%{cn}%")
        if not case:
            return json.dumps({"error": f"Case '{cn}' not found"})
        parties = await p.fetch("""
            SELECT p.name, cp.role, p.party_type
            FROM legal.case_parties cp
            JOIN legal.parties p ON cp.party_id = p.id
            WHERE cp.case_id = $1
        """, case["id"])
        async with p.acquire() as conn_docs:
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn_docs, domain='legal',
                caller_context=_dashboard_caller_context(
                    '_tool_lookup_case', extra={'case': cn},
                ),
                table_alias='d', next_param_index=2,
            )
            docs = await conn_docs.fetch(f"""
                SELECT d.id, d.title, d.filename, d.document_type, d.created_at
                FROM legal.case_documents cd
                JOIN core.documents d ON cd.document_id = d.id
                WHERE cd.case_id = $1 AND {safety_clause}
                ORDER BY d.created_at DESC LIMIT 20
            """, case["id"], *safety_params)
            await log_cb(docs)
        return json.dumps({
            "case_number": case["case_number"], "court": case["court"],
            "title": case["case_title"], "type": case["case_type"],
            "status": case["status"], "jurisdiction": case["jurisdiction"],
            "date_filed": case["date_filed"].isoformat() if case["date_filed"] else None,
            "doc_count": case["doc_count"],
            "parties": [{"name": p["name"], "role": p["role"], "type": p["party_type"]} for p in parties],
            "recent_documents": [{"id": str(d["id"]), "title": d["title"] or d["filename"],
                                   "type": d["document_type"],
                                   "date": d["created_at"].isoformat() if d["created_at"] else None}
                                  for d in docs],
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_get_document(args: dict) -> str:
    """Get full document content.

    Sprint A: uses fetch_safe_document to enforce privilege and PHI gating.
    If the document is privileged and include_privileged=False (default),
    returns a denial response object so the caller knows to ask explicitly.
    """
    doc_id = args["document_id"]
    include_privileged = bool(args.get("include_privileged", False))
    purpose_of_use = args.get("purpose_of_use")
    authorization_id = args.get("authorization_id")
    p = await get_pool()
    try:
        async with p.acquire() as conn:
            try:
                auth_uuid = uuid.UUID(authorization_id) if authorization_id else None
            except (ValueError, AttributeError):
                return json.dumps({"error": "invalid authorization_id UUID"})
            try:
                doc = await fetch_safe_document(
                    doc_id, conn=conn,
                    caller_context=_dashboard_caller_context('_tool_get_document'),
                    include_privileged=include_privileged,
                    purpose_of_use=purpose_of_use,
                    authorization_id=auth_uuid,
                )
            except PrivilegeDeniedException:
                return json.dumps({
                    "error": "privilege_denied",
                    "detail": "document is privileged; pass include_privileged=true to request access",
                })
            except PHIAccessDeniedException as e:
                return json.dumps({
                    "error": "phi_access_denied",
                    "detail": str(e),
                })
            # Augment with email metadata (same join as before)
            em = await conn.fetchrow("""
                SELECT sender, recipients, date_sent, direction, mailbox
                FROM legal.email_metadata WHERE document_id = $1::uuid
            """, doc_id)
        # Default email fields to None so downstream code can test them uniformly
        for k in ('sender', 'recipients', 'date_sent', 'direction', 'mailbox'):
            doc.setdefault(k, None)
        if em:
            doc['sender'] = em['sender']
            doc['recipients'] = em['recipients']
            doc['date_sent'] = em['date_sent']
            doc['direction'] = em['direction']
            doc['mailbox'] = em['mailbox']
        content = doc["full_content"] or ""
        if len(content) > 4000:
            content = content[:4000] + "\n... [truncated]"
        result = {
            "id": str(doc["id"]), "title": doc["title"], "filename": doc["filename"],
            "domain": doc["domain"], "type": doc["document_type"],
            "content": content, "created_at": _ser(doc["created_at"]),
        }
        if doc["sender"]:
            result["email"] = {"from": doc["sender"], "to": doc["recipients"],
                               "date": _ser(doc["date_sent"]), "direction": doc["direction"],
                               "mailbox": doc["mailbox"]}
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_search_emails(args: dict) -> str:
    """Search email archive."""
    query = args["query"]
    sender = args.get("sender")
    mailbox = args.get("mailbox")
    limit = min(args.get("limit", 10), 30)
    p = await get_pool()
    try:
        conditions = ["d.document_type IN ('email','eml')"]
        params: list = []
        idx = 1
        if query:
            conditions.append(f"d.full_content_tsv @@ websearch_to_tsquery('english', ${idx})")
            params.append(query); idx += 1
        if sender:
            conditions.append(f"em.sender ILIKE ${idx}")
            params.append(f"%{sender}%"); idx += 1
        if mailbox:
            conditions.append(f"em.mailbox ILIKE ${idx}")
            params.append(f"%{mailbox}%"); idx += 1
        async with p.acquire() as conn:
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain='legal',
                caller_context=_dashboard_caller_context(
                    '_tool_search_emails', extra={'sender': sender, 'mailbox': mailbox},
                ),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit); limit_idx = idx
            where = " AND ".join(conditions)
            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.privilege,
                       em.sender, em.recipients,
                       em.date_sent, em.direction, em.mailbox,
                       LEFT(d.full_content, 400) AS excerpt
                FROM core.documents d
                LEFT JOIN legal.email_metadata em ON d.id = em.document_id
                WHERE {where}
                ORDER BY em.date_sent DESC NULLS LAST
                LIMIT ${limit_idx}
            """, *params)
            await log_cb(rows)
        results = [{"document_id": str(r["id"]), "subject": r["title"] or r["filename"],
                    "from": r["sender"], "to": r["recipients"],
                    "date": _ser(r["date_sent"]), "direction": r["direction"],
                    "mailbox": r["mailbox"], "excerpt": r["excerpt"]}
                   for r in rows]
        return json.dumps({"results": results, "count": len(results)})
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_list_cases(args: dict) -> str:
    """List all cases."""
    p = await get_pool()
    status = args.get("status", "all")
    try:
        where = ""
        params: list = []
        if status and status != "all":
            where = "WHERE c.status = $1"
            params.append(status)
        rows = await p.fetch(f"""
            SELECT c.case_number, c.case_title, c.court, c.status,
                   c.jurisdiction, c.date_filed, c.case_type,
                   COUNT(cd.document_id) AS doc_count
            FROM legal.cases c
            LEFT JOIN legal.case_documents cd ON c.id = cd.case_id
            {where}
            GROUP BY c.id ORDER BY c.date_filed DESC NULLS LAST
        """, *params)
        return json.dumps({"cases": [{
            "case_number": r["case_number"], "title": r["case_title"],
            "court": r["court"], "status": r["status"],
            "jurisdiction": r["jurisdiction"],
            "filed": r["date_filed"].isoformat() if r["date_filed"] else None,
            "type": r["case_type"], "documents": r["doc_count"]
        } for r in rows], "count": len(rows)})
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_list_case_documents(args: dict) -> str:
  """List documents for a case with optional type filter and pagination."""
  case_number = args["case_number"].strip()
  doc_type = args.get("document_type")
  limit = min(int(args.get("limit", 50)), 200)
  offset = max(int(args.get("offset", 0)), 0)
  p = await get_pool()
  try:
    case = await p.fetchrow(
      "SELECT id, case_number, case_title FROM legal.cases WHERE case_number ILIKE $1",
      f"%{case_number}%",
    )
    if not case:
      return json.dumps({"error": f"Case '{case_number}' not found"})

    where = ["cd.case_id = $1"]
    params: list = [case["id"]]
    if doc_type:
      where.append(f"d.document_type = ${len(params) + 1}")
      params.append(doc_type)
    where_sql = " AND ".join(where)

    async with p.acquire() as conn:
      safety_clause, safety_params, log_cb = await build_document_safety_filter(
          conn=conn, domain='legal',
          caller_context=_dashboard_caller_context(
              '_tool_list_case_documents', extra={'case': case_number, 'doc_type': doc_type},
          ),
          table_alias='d', next_param_index=len(params) + 1,
      )
      params_with_filter = list(params) + list(safety_params)
      filter_idx_end = len(params_with_filter)

      # Total with safety filter applied — this is the count of docs the caller
      # is actually allowed to see, not the full case doc count
      total = await conn.fetchval(
          f"""
          SELECT COUNT(*)
          FROM legal.case_documents cd
          JOIN core.documents d ON cd.document_id = d.id
          WHERE {where_sql} AND {safety_clause}
          """,
          *params_with_filter,
      )

      limit_idx = filter_idx_end + 1
      offset_idx = filter_idx_end + 2
      rows = await conn.fetch(
          f"""
          SELECT d.id, d.title, d.filename, d.document_type, d.domain, d.created_at,
                 d.privilege
          FROM legal.case_documents cd
          JOIN core.documents d ON cd.document_id = d.id
          WHERE {where_sql} AND {safety_clause}
          ORDER BY d.created_at ASC
          LIMIT ${limit_idx} OFFSET ${offset_idx}
          """,
          *params_with_filter,
          limit,
          offset,
      )
      await log_cb(rows)

    return json.dumps({
      "case_number": case["case_number"],
      "case_title": case["case_title"],
      "document_type": doc_type,
      "total": total,
      "count": len(rows),
      "limit": limit,
      "offset": offset,
      "documents": [
        {
          "id": str(r["id"]),
          "title": r["title"] or r["filename"],
          "filename": r["filename"],
          "type": r["document_type"],
          "domain": r["domain"],
          "created_at": _ser(r["created_at"]),
        }
        for r in rows
      ],
    })
  except Exception as e:
    return json.dumps({"error": str(e)})


async def _tool_get_documents_bulk(args: dict) -> str:
  """Get multiple documents by ID in one call."""
  doc_ids = args.get("document_ids") or []
  if not isinstance(doc_ids, list) or not doc_ids:
    return json.dumps({"error": "document_ids must be a non-empty list"})

  max_chars = int(args.get("max_chars", 1200))
  max_chars = max(200, min(max_chars, 4000))

  truncated = False
  if len(doc_ids) > 20:
    doc_ids = doc_ids[:20]
    truncated = True

  p = await get_pool()
  try:
    async with p.acquire() as conn:
      # Cross-domain: each document id may be legal, medical, or out-of-scope.
      safety_clause, safety_params, log_cb = await build_document_safety_filter(
          conn=conn, domain=None,
          caller_context=_dashboard_caller_context(
              '_tool_get_documents_bulk', extra={'ids_count': len(doc_ids)},
          ),
          table_alias='d', next_param_index=2,
      )
      rows = await conn.fetch(
          f"""
          SELECT d.id, d.title, d.filename, d.domain, d.document_type, d.created_at,
                 d.privilege, d.phi_status,
                 d.full_content, em.sender, em.recipients, em.date_sent,
                 em.direction, em.mailbox
          FROM core.documents d
          LEFT JOIN legal.email_metadata em ON d.id = em.document_id
          WHERE d.id = ANY($1::uuid[]) AND {safety_clause}
          """,
          doc_ids, *safety_params,
      )
      await log_cb(rows)
    found_ids = {str(r["id"]) for r in rows}
    missing = [doc_id for doc_id in doc_ids if doc_id not in found_ids]

    results = []
    for r in rows:
      content = r["full_content"] or ""
      if len(content) > max_chars:
        content = content[:max_chars] + "\n... [truncated]"
      item = {
        "id": str(r["id"]),
        "title": r["title"] or r["filename"],
        "filename": r["filename"],
        "domain": r["domain"],
        "type": r["document_type"],
        "created_at": _ser(r["created_at"]),
        "content": content,
      }
      if r["sender"]:
        item["email"] = {
          "from": r["sender"],
          "to": r["recipients"],
          "date": _ser(r["date_sent"]),
          "direction": r["direction"],
          "mailbox": r["mailbox"],
        }
      results.append(item)

    return json.dumps({
      "count": len(results),
      "max_chars": max_chars,
      "truncated_ids": truncated,
      "missing_ids": missing,
      "documents": results,
    })
  except Exception as e:
    return json.dumps({"error": str(e)})


async def _tool_web_search(args: dict) -> str:
    """DuckDuckGo web search."""
    query = args["query"]
    num = min(args.get("num_results", 5), 10)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                headers={"User-Agent": "Mozilla/5.0 (compatible; NelsonBot/1.0)"},
                timeout=10, follow_redirects=True,
            )
            # Parse simple results from DDG HTML
            import re
            results = []
            # Extract result snippets
            for m in re.finditer(
                r'class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>.*?'
                r'class="result__snippet"[^>]*>(.*?)</(?:td|div)',
                resp.text, re.DOTALL
            ):
                if len(results) >= num:
                    break
                url = m.group(1)
                title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
                snippet = re.sub(r'<[^>]+>', '', m.group(3)).strip()
                if url.startswith("/l/?"):
                    # Extract actual URL from DDG redirect
                    url_match = re.search(r'uddg=([^&]+)', url)
                    if url_match:
                        from urllib.parse import unquote
                        url = unquote(url_match.group(1))
                results.append({"title": title, "url": url, "snippet": snippet})
            return json.dumps({"results": results, "count": len(results), "query": query})
    except Exception as e:
        return json.dumps({"error": str(e), "query": query})


async def _tool_database_stats(args: dict) -> str:
    """Get system stats."""
    p = await get_pool()
    try:
        s = await p.fetchrow("SELECT * FROM ops.get_system_stats()")
        return json.dumps({k: _ser(v) for k, v in dict(s).items()})
    except Exception as e:
        return json.dumps({"error": str(e)})


async def _tool_consult_agents(args: dict) -> str:
    """Post a question to the agent chatroom and collect expert responses."""
    import asyncio as _aio
    question = args.get("question", "").strip()
    context = args.get("context", "").strip()
    if not question:
        return json.dumps({"error": "No question provided"})

    full_msg = question
    if context:
        full_msg = f"{question}\n\nContext: {context}"

    p = await get_pool()
    try:
        # Post the question as athena-ai (from_agent LIKE 'human%' triggers agent response)
        row = await p.fetchrow("""
            INSERT INTO ops.agent_chat
                (channel_id, from_agent, content, mentions, reply_to, msg_type, metadata)
            VALUES ('general', 'human:athena-ai', $1, '{}', NULL, 'question', '{}')
            RETURNING id, created_at
        """, full_msg)
        msg_id = row["id"]

        # Poll for agent responses — agents wake within 5s, respond within 10-30s
        all_agent_ids = {
            'orchestrator', 'watchdog', 'code-doctor', 'security-sentinel',
            'self-healing', 'data-quality', 'dba', 'athena',
            'case-strategy', 'retention', 'query-insight', 'quality-eval',
            'daily-digest',
        }
        max_wait = 90  # seconds
        poll_interval = 5
        elapsed = 0
        responses = []

        while elapsed < max_wait:
            await _aio.sleep(poll_interval)
            elapsed += poll_interval

            replies = await p.fetch("""
                SELECT from_agent, content, created_at
                FROM ops.agent_chat
                WHERE reply_to = $1
                ORDER BY created_at ASC
            """, msg_id)

            responses = [
                {"agent": r["from_agent"], "response": r["content"],
                 "ts": r["created_at"].isoformat()}
                for r in replies
            ]

            responded_agents = {r["from_agent"] for r in replies}
            # Stop early if all agents have responded, or at least 8 have after 30s
            if responded_agents >= all_agent_ids:
                break
            if elapsed >= 30 and len(responded_agents) >= 8:
                break

        return json.dumps({
            "question_id": msg_id,
            "agents_responded": len(responses),
            "agents_total": len(all_agent_ids),
            "responses": responses,
            "note": "Responses are from specialist AI agents analyzing from their domain expertise."
        }, default=str)

    except Exception as e:
        return json.dumps({"error": str(e)})


TOOL_DISPATCH = {
    "semantic_search": _tool_semantic_search,
    "fulltext_search": _tool_fulltext_search,
    "lookup_case": _tool_lookup_case,
    "get_document": _tool_get_document,
    "search_emails": _tool_search_emails,
    "list_cases": _tool_list_cases,
    "list_case_documents": _tool_list_case_documents,
    "get_documents_bulk": _tool_get_documents_bulk,
    "web_search": _tool_web_search,
    "database_stats": _tool_database_stats,
    "consult_agents": _tool_consult_agents,
}


async def _call_openai(client, messages, api_model, reasoning_effort, tools=None):
    """Make an OpenAI API call and return normalized response."""
    payload = {"model": api_model, "messages": messages, "max_completion_tokens": 16384}
    if tools:
        payload["tools"] = tools
        payload["tool_choice"] = "auto"
    if reasoning_effort:
        payload["reasoning_effort"] = reasoning_effort
    resp = await client.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json=payload, timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    choice = data["choices"][0]
    msg = choice["message"]
    usage = data.get("usage", {})
    return {
        "content": msg.get("content", ""),
        "tool_calls": msg.get("tool_calls"),
        "usage": usage,
        "tokens_total": usage.get("total_tokens", 0),
    }

async def _call_anthropic(client, messages, api_model, tools=None):
    """Make an Anthropic API call and return normalized response (same shape as OpenAI)."""
    # Separate system prompt from messages
    system_text = ""
    anthropic_messages = []
    for m in messages:
        if m.get("role") == "system":
            system_text += m.get("content", "") + "\n"
        elif m.get("role") == "tool":
            anthropic_messages.append({
                "role": "user",
                "content": [{"type": "tool_result",
                             "tool_use_id": m.get("tool_call_id", ""),
                             "content": m.get("content", "")}],
            })
        elif m.get("role") == "assistant" and m.get("tool_calls"):
            # Convert OpenAI tool_calls format to Anthropic tool_use blocks
            content_blocks = []
            if m.get("content"):
                content_blocks.append({"type": "text", "text": m["content"]})
            for tc in m["tool_calls"]:
                fn = tc.get("function", {})
                args = fn.get("arguments", "{}")
                content_blocks.append({
                    "type": "tool_use",
                    "id": tc.get("id", ""),
                    "name": fn.get("name", ""),
                    "input": json.loads(args) if isinstance(args, str) else args,
                })
            anthropic_messages.append({"role": "assistant", "content": content_blocks})
        else:
            anthropic_messages.append({"role": m.get("role", "user"), "content": m.get("content", "")})

    body = {
        "model": api_model,
        "max_tokens": 16384,
        "messages": anthropic_messages,
    }
    if system_text.strip():
        body["system"] = system_text.strip()
    if tools:
        # Convert OpenAI tool format to Anthropic
        body["tools"] = [
            {"name": t["function"]["name"],
             "description": t["function"].get("description", ""),
             "input_schema": t["function"].get("parameters", {"type": "object", "properties": {}})}
            for t in tools
        ]

    import asyncio as _aio
    data = None
    for _attempt in range(4):
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body, timeout=120,
        )
        if resp.status_code == 429 and _attempt < 3:
            wait = int(resp.headers.get("retry-after", str(5 * (_attempt + 1))))
            print(f"[ANTHROPIC] Rate limited, waiting {wait}s (attempt {_attempt+1}/4)")
            await _aio.sleep(wait)
            continue
        resp.raise_for_status()
        data = resp.json()
        break
    if data is None:
        raise RuntimeError("Anthropic API rate limited after 4 attempts")

    usage = data.get("usage", {})
    content_blocks = data.get("content", [])

    # Extract text and tool_use blocks
    text_parts = []
    tool_calls = []
    for block in content_blocks:
        if block.get("type") == "text":
            text_parts.append(block.get("text", ""))
        elif block.get("type") == "tool_use":
            tool_calls.append({
                "id": block["id"],
                "type": "function",
                "function": {
                    "name": block["name"],
                    "arguments": json.dumps(block.get("input", {})),
                },
            })

    return {
        "content": " ".join(text_parts),
        "tool_calls": tool_calls if tool_calls else None,
        "usage": usage,
        "tokens_total": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
    }

def _claude_cli_env():
    """Clean env for Claude CLI — remove API keys so it uses its own OAuth auth."""
    import os as _os
    env = _os.environ.copy()
    env.pop("ANTHROPIC_API_KEY", None)
    env.pop("OPENAI_API_KEY", None)
    env.setdefault("HOME", "/root")
    return env

async def _call_claude_code(messages, api_model, tools=None):
    """Call Claude via Claude Code CLI (uses Max subscription auth)."""
    import asyncio as _aio

    # Convert model name to CLI alias
    model_map = {"claude-haiku-4-5-20251001": "haiku", "claude-sonnet-4-6": "sonnet", "claude-opus-4-6": "opus"}
    model_alias = model_map.get(api_model, api_model)

    # Build prompt from messages
    parts = []
    for m in messages:
        if m.get("role") == "user":
            parts.append(m["content"])
        elif m.get("role") == "assistant":
            parts.append(f"[Previous response: {m['content'][:200]}...]")
    prompt = "\n\n".join(parts) if parts else "Hello"

    # Get system prompt
    system_text = "\n".join(m["content"] for m in messages if m.get("role") == "system")

    cmd = ["/usr/local/bin/claude", "-p", "--model", model_alias, "--output-format", "text"]
    if system_text:
        cmd.extend(["--append-system-prompt", system_text])

    proc = await _aio.create_subprocess_exec(
        *cmd,
        stdin=_aio.subprocess.PIPE,
        stdout=_aio.subprocess.PIPE,
        stderr=_aio.subprocess.PIPE,
        env=_claude_cli_env(),
    )
    stdout, stderr = await _aio.wait_for(
        proc.communicate(input=prompt.encode("utf-8")),
        timeout=120,
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Claude Code CLI error: {stderr.decode()[:300]}")

    content = stdout.decode("utf-8", errors="replace").strip()

    return {
        "content": content,
        "tool_calls": None,
        "usage": {},
        "tokens_total": 0,
    }

async def _run_chat_turn(
    messages: List[Dict], conversation_id: str, model_id: str = "auto"
) -> Dict[str, Any]:
    """Run one full chat turn with tool calling loop. Supports OpenAI and Anthropic."""
    p = await get_pool()
    all_tool_results = []
    tool_cache: dict[str, str] = {}

    # Resolve model configuration
    model_cfg = next((m for m in AVAILABLE_MODELS if m["id"] == model_id), AVAILABLE_MODELS[0])
    api_model = model_cfg["model"]
    provider = model_cfg.get("provider", "openai")
    reasoning_effort = model_cfg["reasoning_effort"]

    max_tool_rounds = int(model_cfg.get("max_tool_rounds", 8))
    max_tool_rounds = max(1, min(max_tool_rounds, 20))

    async with httpx.AsyncClient() as client:
        for _iteration in range(max_tool_rounds):
          if provider == "anthropic":
              try:
                  result = await _call_claude_code(messages, api_model)
              except Exception as e:
                  print(f"[CHAT] Claude Code CLI failed: {e}, falling back to API")
                  result = await _call_anthropic(client, messages, api_model, tools=CHAT_TOOLS)
          else:
              result = await _call_openai(client, messages, api_model, reasoning_effort, tools=CHAT_TOOLS)

          # If no tool calls, we're done
          if not result["tool_calls"]:
            assistant_content = result["content"]
            await p.execute("""
              INSERT INTO ops.chat_messages
                (conversation_id, role, content, tokens_used, model)
              VALUES ($1, 'assistant', $2, $3, $4)
            """, conversation_id, assistant_content,
              result["tokens_total"], api_model)
            await p.execute("""
              UPDATE ops.chat_conversations SET updated_at = now(),
              title = CASE WHEN title = 'New Conversation'
                THEN LEFT($2, 80) ELSE title END
              WHERE id = $1
            """, conversation_id, assistant_content[:80] if assistant_content else "Chat")
            return {
              "content": assistant_content,
              "tool_calls_made": all_tool_results,
              "tokens": result["usage"],
              "model": api_model,
              "model_label": model_cfg["label"],
            }

          # Process tool calls
          msg_with_tools = {"role": "assistant", "content": result["content"], "tool_calls": result["tool_calls"]}
          messages.append(msg_with_tools)
          for tc in result["tool_calls"]:
            fn_name = tc["function"]["name"]
            fn_args = json.loads(tc["function"]["arguments"])
            handler = TOOL_DISPATCH.get(fn_name)
            cache_key = f"{fn_name}:{json.dumps(fn_args, sort_keys=True)}"
            if cache_key in tool_cache:
              result_str = tool_cache[cache_key]
            else:
              if handler:
                result_str = await handler(fn_args)
              else:
                result_str = json.dumps({"error": f"Unknown tool: {fn_name}"})
              tool_cache[cache_key] = result_str
            all_tool_results.append({
              "tool": fn_name, "args": fn_args,
              "result_preview": result_str[:200]
            })
            await p.execute("""
              INSERT INTO ops.chat_messages
                (conversation_id, role, content, tool_name)
              VALUES ($1, 'tool', $2, $3)
            """, conversation_id, result_str[:8000], fn_name)
            messages.append({
              "role": "tool",
              "tool_call_id": tc["id"],
              "content": result_str,
            })

        # Tool budget exhausted
        finalize_messages = messages + [{
          "role": "system",
          "content": (
            "Tool-call budget reached. Provide the best possible answer using the tool results "
            "already gathered. Be explicit about citations, note gaps, and propose the most "
            "targeted next steps to continue if needed."
          )
        }]
        if provider == "anthropic":
            try:
                result = await _call_claude_code(finalize_messages, api_model)
            except Exception as e:
                print(f"[CHAT] Claude Code CLI failed (finalize): {e}, falling back to API")
                result = await _call_anthropic(client, finalize_messages, api_model)
        else:
            payload = {"model": api_model, "messages": finalize_messages,
                       "tool_choice": "none", "max_completion_tokens": 8192}
            if reasoning_effort:
                payload["reasoning_effort"] = reasoning_effort
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json=payload, timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            result = {
                "content": data["choices"][0]["message"].get("content", ""),
                "usage": data.get("usage", {}),
                "tokens_total": data.get("usage", {}).get("total_tokens", 0),
            }

        assistant_content = result["content"] or ""
        await p.execute("""
          INSERT INTO ops.chat_messages
            (conversation_id, role, content, tokens_used, model)
          VALUES ($1, 'assistant', $2, $3, $4)
        """, conversation_id, assistant_content,
          result["tokens_total"], api_model)
        await p.execute("""
          UPDATE ops.chat_conversations SET updated_at = now(),
          title = CASE WHEN title = 'New Conversation'
            THEN LEFT($2, 80) ELSE title END
          WHERE id = $1
        """, conversation_id, assistant_content[:80] if assistant_content else "Chat")
        return {
          "content": assistant_content,
          "tool_calls_made": all_tool_results,
          "tokens": result["usage"],
          "model": api_model,
          "model_label": model_cfg["label"],
        }


# ── Chat API Endpoints ────────────────────────────────────────

async def api_chatroom_channels(request: Request):
    """List chat room channels."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT c.*, COALESCE(cnt.n, 0) AS message_count,
               cnt.latest
        FROM ops.chat_channels c
        LEFT JOIN (
            SELECT channel_id, COUNT(*) AS n, MAX(created_at) AS latest
            FROM ops.agent_chat
            GROUP BY channel_id
        ) cnt ON cnt.channel_id = c.name
        ORDER BY c.is_default DESC, c.name
    """)
    return JSONResponse([{
        "name": r["name"], "description": r["description"],
        "icon": r["icon"], "is_default": r["is_default"],
        "message_count": r["n"] if "n" in r.keys() else r["message_count"],
        "latest": r["latest"].isoformat() if r["latest"] else None
    } for r in rows])


async def api_chatroom_messages(request: Request):
    """Get chat room messages for a channel."""
    err = await _check_auth(request)
    if err:
        return err
    channel = request.query_params.get("channel", "general")
    limit = min(int(request.query_params.get("limit", "100")), 500)
    since_id = int(request.query_params.get("since_id", "0"))
    before_id = int(request.query_params.get("before_id", "0"))

    p = await get_pool()
    if before_id > 0:
        rows = await p.fetch("""
            SELECT id, channel_id, from_agent, content, mentions,
                   reply_to, msg_type, metadata, created_at
            FROM ops.agent_chat
            WHERE channel_id = $1 AND id < $2
            ORDER BY created_at DESC
            LIMIT $3
        """, channel, before_id, limit)
    elif since_id > 0:
        rows = await p.fetch("""
            SELECT id, channel_id, from_agent, content, mentions,
                   reply_to, msg_type, metadata, created_at
            FROM ops.agent_chat
            WHERE channel_id = $1 AND id > $2
            ORDER BY created_at ASC
            LIMIT $3
        """, channel, since_id, limit)
    else:
        rows = await p.fetch("""
            SELECT id, channel_id, from_agent, content, mentions,
                   reply_to, msg_type, metadata, created_at
            FROM ops.agent_chat
            WHERE channel_id = $1
            ORDER BY created_at DESC
            LIMIT $2
        """, channel, limit)

    messages = []
    for r in rows:
        messages.append({
            "id": r["id"], "channel": r["channel_id"],
            "from": r["from_agent"], "content": r["content"],
            "mentions": r["mentions"] or [],
            "reply_to": r["reply_to"],
            "type": r["msg_type"],
            "metadata": r["metadata"],
            "ts": r["created_at"].isoformat()
        })
    # Return in chronological order
    messages.sort(key=lambda m: m["ts"])
    return JSONResponse(messages)


async def api_chatroom_post(request: Request):
    """Post a human message to the chat room."""
    err = await _check_auth(request)
    if err:
        return err
    body = await request.json()
    channel = body.get("channel", "general")
    content_text = body.get("content", "").strip()
    reply_to = body.get("reply_to")
    msg_type = body.get("type", "chat")

    if not content_text:
        return JSONResponse({"error": "Empty message"}, status_code=400)

    # Determine user from session
    session = request.cookies.get("dash_session", "")
    user = "human"
    if session:
        import hashlib
        for uname in ["admin", "wnelson"]:
            if hashlib.sha256(uname.encode()).hexdigest()[:32] == session:
                user = f"human:{uname}"
                break

    # Extract @mentions
    import re as _re
    mentions = _re.findall(r'@([\w-]+)', content_text)

    p = await get_pool()
    row = await p.fetchrow("""
        INSERT INTO ops.agent_chat
            (channel_id, from_agent, content, mentions, reply_to, msg_type, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, created_at
    """, channel, user, content_text,
        mentions, reply_to, msg_type, '{}')

    return JSONResponse({
        "id": row["id"],
        "ts": row["created_at"].isoformat()
    })


async def api_chatroom_stats(request: Request):
    """Get chat room statistics."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    stats = {}
    stats["total_messages"] = await p.fetchval(
        "SELECT COUNT(*) FROM ops.agent_chat") or 0
    stats["messages_24h"] = await p.fetchval(
        "SELECT COUNT(*) FROM ops.agent_chat WHERE created_at > now() - interval '24 hours'") or 0
    stats["active_agents"] = await p.fetchval(
        "SELECT COUNT(DISTINCT from_agent) FROM ops.agent_chat WHERE created_at > now() - interval '24 hours'") or 0
    stats["channels_used"] = await p.fetchval(
        "SELECT COUNT(DISTINCT channel_id) FROM ops.agent_chat WHERE created_at > now() - interval '24 hours'") or 0

    recent_agents = await p.fetch("""
        SELECT from_agent, COUNT(*) AS n, MAX(created_at) AS last_seen
        FROM ops.agent_chat
        WHERE created_at > now() - interval '24 hours'
        GROUP BY from_agent
        ORDER BY n DESC
        LIMIT 20
    """)
    stats["recent_agents"] = [{"agent": r["from_agent"], "count": r["n"],
                                "last_seen": r["last_seen"].isoformat()} for r in recent_agents]
    return JSONResponse(stats)


async def api_chat_conversations(request: Request):
    """GET: list conversations, POST: create new."""
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    if request.method == "POST":
        row = await p.fetchrow("""
            INSERT INTO ops.chat_conversations DEFAULT VALUES
            RETURNING id, title, created_at
        """)
        return JSONResponse({
            "id": str(row["id"]), "title": row["title"],
            "created_at": row["created_at"].isoformat()
        })
    limit = int(request.query_params.get("limit", "50"))
    rows = await p.fetch("""
        SELECT cv.id, cv.title, cv.created_at, cv.updated_at,
               COUNT(cm.id) AS message_count
        FROM ops.chat_conversations cv
        LEFT JOIN ops.chat_messages cm ON cv.id = cm.conversation_id AND cm.role IN ('user','assistant')
        GROUP BY cv.id
        ORDER BY cv.updated_at DESC LIMIT $1
    """, limit)
    return JSONResponse([{
        "id": str(r["id"]), "title": r["title"],
        "created_at": r["created_at"].isoformat(),
        "updated_at": r["updated_at"].isoformat(),
        "message_count": r["message_count"],
    } for r in rows])


async def api_chat_conversation_detail(request: Request):
    """GET messages, DELETE conversation, PUT rename."""
    err = await _check_auth(request)
    if err: return err
    p = await get_pool()
    cid = request.path_params["cid"]
    if request.method == "DELETE":
        await p.execute("DELETE FROM ops.chat_conversations WHERE id = $1::uuid", cid)
        return JSONResponse({"ok": True})
    if request.method == "PUT":
        data = await request.json()
        if "archived" in data:
            await p.execute(
                "UPDATE ops.chat_conversations SET archived = $2 WHERE id = $1::uuid",
                cid, data["archived"]
            )
        if "title" in data:
            await p.execute(
                "UPDATE ops.chat_conversations SET title = $2 WHERE id = $1::uuid",
                cid, data["title"]
            )
        return JSONResponse({"ok": True})
    # GET — return messages
    rows = await p.fetch("""
        SELECT id, role, content, tool_calls, tool_name, tokens_used, model, created_at, attachments
        FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user', 'assistant')
        ORDER BY created_at
    """, cid)
    result = []
    for r in rows:
        msg = {
            "id": r["id"], "role": r["role"], "content": r["content"],
            "tool_name": r["tool_name"], "tokens_used": r["tokens_used"],
            "model": r["model"], "created_at": r["created_at"].isoformat(),
        }
        if r["attachments"]:
            att = json.loads(r["attachments"]) if isinstance(r["attachments"], str) else r["attachments"]
            msg["attachments"] = att
        result.append(msg)
    return JSONResponse(result)


# ── Chat File Upload ──────────────────────────────────────────
def _extract_text_basic(raw: bytes, content_type: str, file_name: str) -> str:
    """Best-effort text extraction for common file types."""
    ct = content_type.lower()
    fn = file_name.lower()
    # Plain text / code / CSV / markdown
    if ct.startswith("text/") or fn.endswith((".txt", ".csv", ".md", ".json", ".py", ".js", ".sql", ".log", ".xml", ".html", ".htm")):
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            pass
    # PDF
    if ct == "application/pdf" or fn.endswith(".pdf"):
        try:
            import fitz  # PyMuPDF
            doc = fitz.open(stream=raw, filetype="pdf")
            text = "\n".join(page.get_text() for page in doc)
            doc.close()
            return text.strip()
        except Exception:
            pass
    # DOCX
    if fn.endswith(".docx") or "wordprocessingml" in ct:
        try:
            import io
            from zipfile import ZipFile
            import xml.etree.ElementTree as ET
            zf = ZipFile(io.BytesIO(raw))
            xml_content = zf.read("word/document.xml")
            tree = ET.fromstring(xml_content)
            ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
            parts = [node.text for node in tree.iter(f"{{{ns['w']}}}t") if node.text]
            return " ".join(parts).strip()
        except Exception:
            pass
    return ""


def _medical_zip_slug(name: str) -> str:
    base = Path(name or "medical-import").stem
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-._")
    return (slug or "medical-import")[:64]


def _normalize_zip_member(name: str) -> PurePosixPath:
    member = PurePosixPath((name or "").replace("\\", "/"))
    parts = [part for part in member.parts if part not in ("", ".", "/")]
    if not parts or any(part == ".." for part in parts):
        raise ValueError(f"unsafe archive member: {name}")
    return PurePosixPath(*parts)


def _is_supported_medical_member(member: PurePosixPath) -> bool:
    return not member.name.startswith(".") and member.suffix.lower() in SUPPORTED_MEDICAL_IMPORT_EXTENSIONS


def _tail_lines(text: str, max_lines: int = 40) -> str:
    lines = (text or "").splitlines()
    return "\n".join(lines[-max_lines:])


async def _run_dropbox_ingest_once(timeout_seconds: int = 300) -> Dict[str, Any]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "/opt/wdws/dropbox_watcher.py",
        "--once",
        cwd="/opt/wdws",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        return {
            "completed": False,
            "exit_code": None,
            "processed_count": None,
            "log_tail": "Dropbox ingestion is still running in the background. Check the live ingestion log for progress.",
        }

    output = stdout.decode("utf-8", errors="replace") if stdout else ""
    match = re.search(r"Done\. Processed (\d+) file\(s\)\.", output)
    return {
        "completed": True,
        "exit_code": proc.returncode,
        "processed_count": int(match.group(1)) if match else None,
        "log_tail": _tail_lines(output),
    }


async def api_ingestion_medical_zip(request: Request):
    """Upload a medical ZIP, extract supported files into the medical dropbox, and trigger ingestion."""
    err = await _check_auth(request)
    if err:
        return err

    form = await request.form()
    upload = form.get("file")
    if upload is None:
        return JSONResponse({"error": "zip file is required"}, status_code=400)

    file_name = getattr(upload, "filename", "") or "medical-records.zip"
    content_type = (getattr(upload, "content_type", None) or mimetypes.guess_type(file_name)[0] or "application/octet-stream").lower()
    if not file_name.lower().endswith(".zip") and content_type not in {"application/zip", "application/x-zip-compressed", "multipart/x-zip"}:
        return JSONResponse({"error": "only .zip uploads are supported for medical import"}, status_code=400)

    raw = await upload.read()
    if not raw:
        return JSONResponse({"error": "uploaded zip is empty"}, status_code=400)
    if len(raw) > MEDICAL_ZIP_MAX_UPLOAD_BYTES:
        return JSONResponse({
            "error": f"zip exceeds max upload size of {MEDICAL_ZIP_MAX_UPLOAD_BYTES // (1024 * 1024)} MB"
        }, status_code=413)

    ingest_now = str(form.get("ingest_now", "true")).strip().lower() not in {"0", "false", "no"}
    staging_dir = MEDICAL_DROPBOX_DIR / f"portal_{time.strftime('%Y%m%d_%H%M%S')}_{_medical_zip_slug(file_name)}_{uuid.uuid4().hex[:8]}"
    staging_dir.mkdir(parents=True, exist_ok=True)

    extracted_files: List[str] = []
    skipped_files: List[Dict[str, str]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            members = [info for info in zf.infolist() if not info.is_dir()]
            if not members:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return JSONResponse({"error": "zip does not contain any files"}, status_code=400)
            if len(members) > MEDICAL_ZIP_MAX_MEMBERS:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return JSONResponse({"error": f"zip has too many files ({len(members)} > {MEDICAL_ZIP_MAX_MEMBERS})"}, status_code=400)

            total_uncompressed = sum(max(info.file_size, 0) for info in members)
            if total_uncompressed > MEDICAL_ZIP_MAX_UNCOMPRESSED_BYTES:
                shutil.rmtree(staging_dir, ignore_errors=True)
                return JSONResponse({
                    "error": f"zip expands beyond the {MEDICAL_ZIP_MAX_UNCOMPRESSED_BYTES // (1024 * 1024)} MB safety limit"
                }, status_code=400)

            for info in members:
                if info.flag_bits & 0x1:
                    shutil.rmtree(staging_dir, ignore_errors=True)
                    return JSONResponse({"error": "password-protected zip files are not supported"}, status_code=400)

                try:
                    member_path = _normalize_zip_member(info.filename)
                except ValueError as exc:
                    shutil.rmtree(staging_dir, ignore_errors=True)
                    return JSONResponse({"error": str(exc)}, status_code=400)

                if not _is_supported_medical_member(member_path):
                    skipped_files.append({"path": str(member_path), "reason": "unsupported file type"})
                    continue

                target_path = staging_dir.joinpath(*member_path.parts)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as src, open(target_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                extracted_files.append(str(target_path.relative_to(MEDICAL_DROPBOX_DIR)))
    except zipfile.BadZipFile:
        shutil.rmtree(staging_dir, ignore_errors=True)
        return JSONResponse({"error": "invalid or corrupt zip file"}, status_code=400)
    except Exception as exc:
        shutil.rmtree(staging_dir, ignore_errors=True)
        return JSONResponse({"error": f"failed to extract zip: {exc}"}, status_code=500)

    if not extracted_files:
        shutil.rmtree(staging_dir, ignore_errors=True)
        return JSONResponse({
            "error": "zip did not contain any supported medical files",
            "supported_extensions": sorted(SUPPORTED_MEDICAL_IMPORT_EXTENSIONS),
        }, status_code=400)

    response: Dict[str, Any] = {
        "ok": True,
        "uploaded_file": file_name,
        "staging_dir": str(staging_dir.relative_to(MEDICAL_DROPBOX_DIR.parent)),
        "extracted_count": len(extracted_files),
        "skipped_count": len(skipped_files),
        "extracted_files": extracted_files[:50],
        "skipped_files": skipped_files[:50],
        "supported_extensions": sorted(SUPPORTED_MEDICAL_IMPORT_EXTENSIONS),
    }

    if ingest_now:
        response["ingestion"] = await _run_dropbox_ingest_once()

    return JSONResponse(response, status_code=201)


async def api_chat_upload(request: Request):
    """POST: upload files to a chat conversation (multipart/form-data)."""
    err = await _check_auth(request)
    if err: return err

    form = await request.form()
    cid = form.get("conversation_id", "")
    files = form.getlist("files")

    if not files:
        return JSONResponse({"error": "no files provided"}, status_code=400)

    p = await get_pool()

    # Auto-create conversation if needed
    if not cid:
        row = await p.fetchrow(
            "INSERT INTO ops.chat_conversations DEFAULT VALUES RETURNING id")
        cid = str(row["id"])

    results = []
    for f in files:
        raw = await f.read()
        file_name = f.filename or "unnamed"
        content_type = f.content_type or mimetypes.guess_type(file_name)[0] or "application/octet-stream"
        ct_lower = content_type.lower()

        if ct_lower.startswith("image/"):
            asset_type = "image"
        elif ct_lower.startswith("audio/"):
            asset_type = "audio"
        elif ct_lower.startswith("video/"):
            asset_type = "video"
        else:
            asset_type = "file"

        sha = hashlib.sha256(raw).hexdigest()
        ext = Path(file_name).suffix or mimetypes.guess_extension(content_type) or ".bin"
        out_name = f"{uuid.uuid4().hex}{ext}"
        out_path = CHAT_UPLOAD_DIR / out_name
        out_path.write_bytes(raw)

        # Text extraction for documents
        extracted = ""
        if asset_type != "image":
            extracted = _extract_text_basic(raw, content_type, file_name)

        row = await p.fetchrow("""
            INSERT INTO ops.chat_attachments
                (conversation_id, file_name, content_type, size_bytes,
                 storage_path, asset_type, sha256, extracted_text)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id, file_name, content_type, size_bytes, asset_type
        """, cid, file_name, content_type, len(raw),
            str(out_path), asset_type, sha, extracted[:200_000] if extracted else None)

        results.append({
            "id": str(row["id"]),
            "file_name": row["file_name"],
            "content_type": row["content_type"],
            "size_bytes": row["size_bytes"],
            "asset_type": row["asset_type"],
            "extracted_length": len(extracted),
        })

    return JSONResponse({"conversation_id": cid, "attachments": results}, status_code=201)


async def api_chat_attachment_download(request: Request):
    """GET: download/serve a chat attachment by id."""
    err = await _check_auth(request)
    if err: return err
    aid = request.path_params["aid"]
    p = await get_pool()
    row = await p.fetchrow(
        "SELECT storage_path, file_name, content_type FROM ops.chat_attachments WHERE id = $1::uuid", aid)
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    fpath = Path(row["storage_path"])
    if not fpath.exists():
        return JSONResponse({"error": "file missing"}, status_code=404)
    ct = row["content_type"] or "application/octet-stream"
    if ct.startswith("image/") or ct == "application/pdf":
        return FileResponse(str(fpath), media_type=ct)
    return FileResponse(str(fpath), media_type=ct, filename=row["file_name"])


async def api_chat_send(request: Request):
    """POST: send a message (with optional attachments) and get AI response."""
    err = await _check_auth(request)
    if err: return err
    data = await request.json()
    cid = data.get("conversation_id")
    user_msg = data.get("message", "").strip()
    model_id = data.get("model", "auto")
    attachment_ids = data.get("attachment_ids", [])  # list of UUIDs from upload

    if not user_msg and not attachment_ids:
        return JSONResponse({"error": "message or attachments required"}, status_code=400)

    p = await get_pool()

    # Auto-create conversation if needed
    if not cid:
        row = await p.fetchrow(
            "INSERT INTO ops.chat_conversations DEFAULT VALUES RETURNING id")
        cid = str(row["id"])

    # Build attachment info
    attach_meta = []
    if attachment_ids:
        rows = await p.fetch("""
            SELECT id, file_name, content_type, size_bytes, asset_type,
                   storage_path, extracted_text
            FROM ops.chat_attachments
            WHERE id = ANY($1::uuid[]) AND conversation_id = $2::uuid
        """, attachment_ids, cid)
        for r in rows:
            attach_meta.append(dict(r))

    # Build content for the user message (JSON for attachments reference)
    attach_json = None
    if attach_meta:
        attach_json = json.dumps([{
            "id": str(a["id"]), "file_name": a["file_name"],
            "content_type": a["content_type"], "size_bytes": a["size_bytes"],
            "asset_type": a["asset_type"],
        } for a in attach_meta])

    display_content = user_msg or ""
    if attach_meta:
        fnames = ", ".join(a["file_name"] for a in attach_meta)
        if display_content:
            display_content += f"\n\n📎 Attached: {fnames}"
        else:
            display_content = f"📎 Attached: {fnames}"

    # Save user message with attachment refs
    await p.execute("""
        INSERT INTO ops.chat_messages (conversation_id, role, content, attachments)
        VALUES ($1::uuid, 'user', $2, $3::jsonb)
    """, cid, display_content, attach_json)

    # Link attachments to the message
    if attach_meta:
        msg_row = await p.fetchrow("""
            SELECT id FROM ops.chat_messages
            WHERE conversation_id = $1::uuid AND role = 'user'
            ORDER BY created_at DESC LIMIT 1
        """, cid)
        if msg_row:
            for a in attach_meta:
                await p.execute("""
                    UPDATE ops.chat_attachments SET message_id = $1
                    WHERE id = $2::uuid
                """, msg_row["id"], a["id"])

    # Build messages array from conversation history
    history = await p.fetch("""
        SELECT role, content, attachments FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user', 'assistant')
        ORDER BY created_at
    """, cid)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        if h["role"] == "user" and h.get("attachments"):
            # Check if this message has image attachments — use multimodal content
            h_attachments = json.loads(h["attachments"]) if isinstance(h["attachments"], str) else h["attachments"]
            content_parts = []
            if h["content"]:
                # Strip the "📎 Attached:" suffix for the API — cleaner prompt
                text = h["content"]
                content_parts.append({"type": "text", "text": text})
            for att_ref in (h_attachments or []):
                att_id = att_ref.get("id")
                if att_ref.get("asset_type") == "image":
                    # Look up the attachment for base64 image
                    att_row = await p.fetchrow(
                        "SELECT storage_path, content_type FROM ops.chat_attachments WHERE id = $1::uuid", att_id)
                    if att_row:
                        fpath = Path(att_row["storage_path"])
                        if fpath.exists() and fpath.stat().st_size < 20_000_000:  # 20MB limit
                            img_b64 = _b64.b64encode(fpath.read_bytes()).decode()
                            ct = att_row["content_type"] or "image/png"
                            content_parts.append({
                                "type": "image_url",
                                "image_url": {"url": f"data:{ct};base64,{img_b64}", "detail": "auto"}
                            })
                else:
                    # For documents, inject extracted text
                    att_row = await p.fetchrow(
                        "SELECT file_name, extracted_text FROM ops.chat_attachments WHERE id = $1::uuid", att_id)
                    if att_row and att_row["extracted_text"]:
                        content_parts.append({
                            "type": "text",
                            "text": f"\n\n--- File: {att_row['file_name']} ---\n{att_row['extracted_text'][:50000]}\n--- End file ---"
                        })
            if content_parts:
                messages.append({"role": "user", "content": content_parts})
            else:
                messages.append({"role": h["role"], "content": h["content"] or ""})
        else:
            messages.append({"role": h["role"], "content": h["content"] or ""})

    # Auto-title from first user message
    if len([h for h in history if h["role"] == "user"]) <= 1:
        title = (user_msg or "File upload")[:80]
        await p.execute(
            "UPDATE ops.chat_conversations SET title = $2 WHERE id = $1::uuid",
            cid, title)

    # Run the chat turn
    try:
        result = await _run_chat_turn(messages, cid, model_id=model_id)
        return JSONResponse({
            "conversation_id": cid,
            "response": result["content"],
            "tool_calls": result["tool_calls_made"],
            "tokens": result["tokens"],
            "model": result.get("model", ""),
            "model_label": result.get("model_label", ""),
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[CHAT SEND ERROR] model_id={model_id}, error={type(e).__name__}: {e}", flush=True)
        return JSONResponse({"error": str(e)}, status_code=500)


async def api_chat_retry(request: Request):
    """POST: retry the last assistant turn, optionally with a different model."""
    err = await _check_auth(request)
    if err: return err
    data = await request.json()
    cid = data.get("conversation_id")
    model_id = data.get("model", "auto")
    if not cid:
        return JSONResponse({"error": "conversation_id required"}, status_code=400)

    p = await get_pool()

    # Find the last assistant message and all tool messages after the last user message
    last_user = await p.fetchrow("""
        SELECT id, created_at FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role = 'user'
        ORDER BY created_at DESC LIMIT 1
    """, cid)
    if not last_user:
        return JSONResponse({"error": "No user message to retry"}, status_code=400)

    # Delete assistant + tool messages that came after the last user message
    deleted = await p.execute("""
        DELETE FROM ops.chat_messages
        WHERE conversation_id = $1::uuid
          AND role IN ('assistant', 'tool')
          AND created_at >= $2
    """, cid, last_user["created_at"])

    # Rebuild messages from conversation history
    history = await p.fetch("""
        SELECT role, content FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user', 'assistant')
        ORDER BY created_at
    """, cid)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": h["role"], "content": h["content"]})

    # Re-run the chat turn with the (possibly different) model
    try:
        result = await _run_chat_turn(messages, cid, model_id=model_id)
        return JSONResponse({
            "conversation_id": cid,
            "response": result["content"],
            "tool_calls": result["tool_calls_made"],
            "tokens": result["tokens"],
            "model": result.get("model", ""),
            "model_label": result.get("model_label", ""),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── Chat Models Endpoint ──────────────────────────────────────
async def api_chat_models(request: Request):
    """GET: list available chat models."""
    err = await _check_auth(request)
    if err: return err
    return JSONResponse({"models": AVAILABLE_MODELS, "default": "claude-haiku"})


async def api_chat_stream(request: Request):
    """POST: send a message and stream the response via SSE."""
    err = await _check_auth(request)
    if err: return err
    data = await request.json()
    cid = data.get("conversation_id")
    user_msg = data.get("message", "").strip()
    model_id = data.get("model", "claude-haiku")
    attachment_ids = data.get("attachment_ids", [])

    if not user_msg and not attachment_ids:
        return JSONResponse({"error": "message required"}, status_code=400)

    p = await get_pool()

    # Auto-create conversation
    if not cid:
        row = await p.fetchrow("INSERT INTO ops.chat_conversations DEFAULT VALUES RETURNING id")
        cid = str(row["id"])

    # Save user message
    await p.execute("""
        INSERT INTO ops.chat_messages (conversation_id, role, content)
        VALUES ($1::uuid, 'user', $2)
    """, cid, user_msg)

    # Auto-title
    count = await p.fetchval(
        "SELECT COUNT(*) FROM ops.chat_messages WHERE conversation_id=$1::uuid AND role='user'", cid)
    if count <= 1:
        await p.execute(
            "UPDATE ops.chat_conversations SET title=$2 WHERE id=$1::uuid",
            cid, (user_msg or "Chat")[:80])

    # Build messages from history
    history = await p.fetch("""
        SELECT role, content FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user','assistant')
        ORDER BY created_at
    """, cid)

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": h["role"], "content": h["content"] or ""})

    model_cfg = next((m for m in AVAILABLE_MODELS if m["id"] == model_id), AVAILABLE_MODELS[0])
    api_model = model_cfg["model"]
    provider = model_cfg.get("provider", "openai")

    async def generate():
        full_content = []
        try:
            yield f"data: {json.dumps({'conversation_id': cid, 'model': api_model, 'model_label': model_cfg['label']})}\n\n"

            if provider == "anthropic":
                # Try Claude Code CLI first for streaming
                import asyncio as _aio
                _cli_ok = False
                try:
                    model_map = {"claude-haiku-4-5-20251001": "haiku", "claude-sonnet-4-6": "sonnet", "claude-opus-4-6": "opus"}
                    model_alias = model_map.get(api_model, api_model)

                    # Build prompt from messages
                    _parts = []
                    _sys_text = ""
                    for m in messages:
                        if m.get("role") == "system":
                            _sys_text += m["content"] + "\n"
                        elif m.get("role") == "user":
                            _parts.append(m["content"])
                        elif m.get("role") == "assistant":
                            _parts.append(f"[Previous response: {m['content'][:200]}...]")
                    _prompt = "\n\n".join(_parts) if _parts else "Hello"

                    cmd = ["/usr/local/bin/claude", "-p", "--model", model_alias, "--output-format", "text"]
                    if _sys_text.strip():
                        cmd.extend(["--append-system-prompt", _sys_text.strip()])

                    proc = await _aio.create_subprocess_exec(
                        *cmd,
                        stdin=_aio.subprocess.PIPE,
                        stdout=_aio.subprocess.PIPE,
                        stderr=_aio.subprocess.PIPE,
                        env=_claude_cli_env(),
                    )
                    proc.stdin.write(_prompt.encode("utf-8"))
                    await proc.stdin.drain()
                    proc.stdin.close()

                    while True:
                        chunk = await _aio.wait_for(proc.stdout.read(100), timeout=120)
                        if not chunk:
                            break
                        text = chunk.decode("utf-8", errors="replace")
                        full_content.append(text)
                        yield f"data: {json.dumps({'content': text})}\n\n"

                    await proc.wait()
                    if proc.returncode != 0:
                        _stderr = (await proc.stderr.read()).decode()[:300]
                        raise RuntimeError(f"Claude Code CLI error: {_stderr}")
                    _cli_ok = True
                except Exception as _cli_err:
                    print(f"[STREAM] Claude Code CLI failed: {_cli_err}, falling back to API")
                    full_content.clear()

                if not _cli_ok:
                    # Anthropic API streaming fallback
                    system_text = ""
                    api_messages = []
                    for m in messages:
                        if m["role"] == "system":
                            system_text += m["content"] + "\n"
                        else:
                            api_messages.append({"role": m["role"], "content": m["content"]})

                    body = {"model": api_model, "max_tokens": 8192, "messages": api_messages, "stream": True}
                    if system_text.strip():
                        body["system"] = system_text.strip()

                    for _attempt in range(3):
                        async with httpx.AsyncClient() as client:
                            async with client.stream("POST", "https://api.anthropic.com/v1/messages",
                                headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                                json=body, timeout=120) as resp:

                                if resp.status_code == 429 and _attempt < 2:
                                    wait = int(resp.headers.get("retry-after", str(5 * (_attempt+1))))
                                    yield f"data: {json.dumps({'status': f'Rate limited, retrying in {wait}s...'})}\n\n"
                                    await _aio.sleep(wait)
                                    continue

                                resp.raise_for_status()
                                async for line in resp.aiter_lines():
                                    if not line.startswith("data: "):
                                        continue
                                    chunk_str = line[6:]
                                    if chunk_str.strip() == "[DONE]":
                                        break
                                    try:
                                        chunk = json.loads(chunk_str)
                                        if chunk.get("type") == "content_block_delta":
                                            delta = chunk.get("delta", {})
                                            text = delta.get("text", "")
                                            if text:
                                                full_content.append(text)
                                                yield f"data: {json.dumps({'content': text})}\n\n"
                                    except json.JSONDecodeError:
                                        pass
                                break  # Success, exit retry loop

            else:
                # OpenAI streaming
                reasoning_effort = model_cfg.get("reasoning_effort")
                payload = {"model": api_model, "messages": messages, "stream": True, "max_completion_tokens": 8192}
                if reasoning_effort:
                    payload["reasoning_effort"] = reasoning_effort

                async with httpx.AsyncClient() as client:
                    async with client.stream("POST", "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
                        json=payload, timeout=120) as resp:

                        resp.raise_for_status()
                        async for line in resp.aiter_lines():
                            if not line.startswith("data: "):
                                continue
                            chunk_str = line[6:]
                            if chunk_str == "[DONE]":
                                break
                            try:
                                chunk = json.loads(chunk_str)
                                delta = chunk.get("choices", [{}])[0].get("delta", {})
                                text = delta.get("content", "")
                                if text:
                                    full_content.append(text)
                                    yield f"data: {json.dumps({'content': text})}\n\n"
                            except json.JSONDecodeError:
                                pass

            # Save assistant response
            assistant_text = "".join(full_content)
            if assistant_text:
                await p.execute("""
                    INSERT INTO ops.chat_messages (conversation_id, role, content, model)
                    VALUES ($1::uuid, 'assistant', $2, $3)
                """, cid, assistant_text, api_model)

            yield f"data: {json.dumps({'done': True})}\n\n"

        except Exception as e:
            import traceback
            traceback.print_exc()
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# ── Health Probe ──────────────────────────────────────────────
async def health(request: Request):
  start = time.time()
  timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

  try:
    p = await get_pool()
    async with p.acquire() as conn:
      email_sync_check = await audit_sync_configuration_health(conn)

    overall = str(email_sync_check.get("status", "healthy"))
    payload = {
      "status": overall,
      "service": "athena-dashboard-v2",
      "timestamp": timestamp,
      "check_latency_ms": int((time.time() - start) * 1000),
      "checks": {
        "email_sync_config": email_sync_check,
      },
    }
    return JSONResponse(payload, status_code=_health_status_code(overall))
  except Exception as e:
    payload = {
      "status": "unhealthy",
      "service": "athena-dashboard-v2",
      "timestamp": timestamp,
      "check_latency_ms": int((time.time() - start) * 1000),
      "checks": {
        "email_sync_config": {
          "status": "unhealthy",
          "summary": "Email sync config health check failed.",
          "error": str(e),
        },
      },
    }
    return JSONResponse(payload, status_code=503)


# ── Serve SPA ─────────────────────────────────────────────────
async def page_dashboard(request: Request):
    return HTMLResponse(DASHBOARD_HTML)




# ── MCP Accounts & Query Analytics ───────────────────────────

async def api_mcp_clients(request):
    """List and manage OAuth clients."""
    err = await _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT c.client_id, c.client_name, c.is_active, c.created_at, c.last_used,
               c.redirect_uris, c.scopes, c.description, c.client_secret_hash,
               COALESCE(c.client_secret, '') as client_secret,
               COALESCE(c.total_tool_calls, 0) as total_tool_calls,
               (SELECT COUNT(*) FROM ops.oauth_access_tokens t WHERE t.client_id = c.client_id) as active_tokens,
               (SELECT COUNT(*) FROM ops.mcp_query_log q WHERE q.client_id = c.client_id) as logged_calls,
               (SELECT MAX(q.created_at) FROM ops.mcp_query_log q WHERE q.client_id = c.client_id) as last_query
        FROM ops.oauth_clients c
        ORDER BY c.created_at
    """)
    clients = []
    for r in rows:
        clients.append({
            "client_id": r["client_id"],
            "client_name": r["client_name"],
            "is_active": r["is_active"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "last_used": r["last_used"].isoformat() if r["last_used"] else None,
            "redirect_uris": list(r["redirect_uris"] or []),
            "scopes": list(r["scopes"] or []),
            "description": r["description"] or "",
            "total_tool_calls": r["total_tool_calls"],
            "active_tokens": r["active_tokens"],
            "logged_calls": r["logged_calls"],
            "last_query": r["last_query"].isoformat() if r["last_query"] else None,
            "client_secret_hash": r["client_secret_hash"][:12] + "…" if r.get("client_secret_hash") else "N/A",
            "client_secret": r["client_secret"] or "",
        })
    return JSONResponse({"clients": clients})


async def api_mcp_client_toggle(request):
    """Toggle a client active/inactive."""
    err = await _check_auth(request)
    if err:
        return err
    cid = request.path_params["cid"]
    p = await get_pool()
    row = await p.fetchrow("SELECT is_active FROM ops.oauth_clients WHERE client_id = $1", cid)
    if not row:
        return JSONResponse({"error": "Client not found"}, status_code=404)
    new_state = not row["is_active"]
    await p.execute("UPDATE ops.oauth_clients SET is_active = $1 WHERE client_id = $2", new_state, cid)
    if not new_state:
        # Revoke all tokens when disabling
        await p.execute("DELETE FROM ops.oauth_access_tokens WHERE client_id = $1", cid)
        await p.execute("DELETE FROM ops.oauth_refresh_tokens WHERE client_id = $1", cid)
    return JSONResponse({"client_id": cid, "is_active": new_state})


async def api_mcp_query_log(request):
    """Paginated query log with filters."""
    err = await _check_auth(request)
    if err:
        return err
    params = request.query_params
    client_id = params.get("client_id", "")
    tool_name = params.get("tool_name", "")
    limit = min(int(params.get("limit", "50")), 200)
    offset = int(params.get("offset", "0"))
    hours = int(params.get("hours", "168"))  # default 7 days

    conditions = ["created_at > now() - interval '1 hour' * $1"]
    args = [hours]
    idx = 2

    if client_id:
        conditions.append(f"client_id = ${idx}")
        args.append(client_id)
        idx += 1
    if tool_name:
        conditions.append(f"tool_name = ${idx}")
        args.append(tool_name)
        idx += 1

    where = " AND ".join(conditions)

    p = await get_pool()
    total = await p.fetchval(f"SELECT COUNT(*) FROM ops.mcp_query_log WHERE {where}", *args)

    rows = await p.fetch(f"""
        SELECT id, client_id, client_name, tool_name, arguments, result_summary,
               result_count, duration_ms, error, created_at
        FROM ops.mcp_query_log
        WHERE {where}
        ORDER BY created_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """, *args, limit, offset)

    entries = []
    for r in rows:
        entries.append({
            "id": r["id"],
            "client_id": r["client_id"],
            "client_name": r["client_name"],
            "tool_name": r["tool_name"],
            "arguments": r["arguments"] if r["arguments"] else {},
            "result_summary": r["result_summary"] if r["result_summary"] else {},
            "result_count": r["result_count"],
            "duration_ms": r["duration_ms"],
            "error": r["error"],
            "error_message": r["error"],  # alias for JS compatibility
            "called_at": r["created_at"].isoformat() if r["created_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        })

    return JSONResponse(entries)



async def api_mcp_query_log_detail(request):
    """Get full details of a single query log entry."""
    err = await _check_auth(request)
    if err:
        return err
    entry_id = request.path_params["id"]
    p = await get_pool()
    row = await p.fetchrow("""
        SELECT id, client_id, client_name, tool_name, arguments, result_summary,
               result_count, duration_ms, error, created_at,
               COALESCE(result_raw, '') as result_raw
        FROM ops.mcp_query_log WHERE id = $1
    """, int(entry_id))
    if not row:
        return JSONResponse({"error": "Entry not found"}, status_code=404)
    return JSONResponse({
        "id": row["id"],
        "client_id": row["client_id"],
        "client_name": row["client_name"],
        "tool_name": row["tool_name"],
        "arguments": row["arguments"] if row["arguments"] else {},
        "result_summary": row["result_summary"] if row["result_summary"] else {},
        "result_raw": row["result_raw"] or "",
        "result_count": row["result_count"],
        "duration_ms": row["duration_ms"],
        "error": row["error"],
        "called_at": row["created_at"].isoformat() if row["created_at"] else None,
    })


async def api_mcp_analytics(request):
    """Aggregated analytics for MCP tool usage."""
    err = await _check_auth(request)
    if err:
        return err
    hours = int(request.query_params.get("hours", "168"))
    p = await get_pool()

    # Tool usage breakdown
    tool_stats = await p.fetch("""
        SELECT tool_name,
               COUNT(*) as call_count,
               AVG(duration_ms)::int as avg_ms,
               MAX(duration_ms) as max_ms,
               SUM(result_count) as total_results,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as error_count
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
        GROUP BY tool_name
        ORDER BY call_count DESC
    """, hours)

    # Client usage breakdown
    client_stats = await p.fetch("""
        SELECT client_id, client_name,
               COUNT(*) as call_count,
               AVG(duration_ms)::int as avg_ms,
               COUNT(DISTINCT tool_name) as tools_used,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as error_count,
               MAX(created_at) as last_call
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
        GROUP BY client_id, client_name
        ORDER BY call_count DESC
    """, hours)

    # Hourly activity (last 24h)
    hourly = await p.fetch("""
        SELECT date_trunc('hour', created_at) as hour,
               COUNT(*) as calls,
               COUNT(DISTINCT client_id) as clients
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '24 hours'
        GROUP BY date_trunc('hour', created_at)
        ORDER BY hour
    """)

    # Recent errors
    errors = await p.fetch("""
        SELECT id, client_id, client_name, tool_name, arguments, error, created_at
        FROM ops.mcp_query_log
        WHERE error IS NOT NULL AND created_at > now() - interval '1 hour' * $1
        ORDER BY created_at DESC
        LIMIT 20
    """, hours)

    # Overall totals
    totals = await p.fetchrow("""
        SELECT COUNT(*) as total_calls,
               COUNT(DISTINCT client_id) as unique_clients,
               COUNT(DISTINCT tool_name) as unique_tools,
               AVG(duration_ms)::int as avg_latency,
               COUNT(CASE WHEN error IS NOT NULL THEN 1 END) as total_errors
        FROM ops.mcp_query_log
        WHERE created_at > now() - interval '1 hour' * $1
    """, hours)

    # Flatten totals to top level with JS-expected names
    t = dict(totals) if totals else {}
    return JSONResponse({
        "period_hours": hours,
        "total_queries": t.get("total_calls", 0),
        "unique_tools": t.get("unique_tools", 0),
        "unique_clients": t.get("unique_clients", 0),
        "avg_duration_ms": t.get("avg_latency"),
        "error_count": t.get("total_errors", 0),
        "by_tool": [{"tool_name": r["tool_name"], "count": r["call_count"],
                      "avg_ms": r["avg_ms"], "max_ms": r["max_ms"],
                      "total_results": r["total_results"], "errors": r["error_count"]}
                     for r in tool_stats],
        "by_client": [{"client_id": r["client_id"], "client_name": r["client_name"],
                        "count": r["call_count"], "avg_ms": r["avg_ms"],
                        "tools_used": r["tools_used"], "errors": r["error_count"],
                        "last_active": r["last_call"].isoformat() if r["last_call"] else None}
                       for r in client_stats],
        "hourly": [{"hour": r["hour"].isoformat(), "calls": r["calls"], "clients": r["clients"]}
                   for r in hourly],
        "recent_errors": [{
            "id": r["id"], "client_id": r["client_id"], "client_name": r["client_name"],
            "tool_name": r["tool_name"], "arguments": r["arguments"] if r["arguments"] else {},
            "error_message": r["error"], "called_at": r["created_at"].isoformat() if r["created_at"] else None,
        } for r in errors],
    })


async def api_quality_summary(request: Request):
  """Summary of eval results from the Quality Eval agent."""
  err = await _check_auth(request)
  if err:
    return err
  hours = int(request.query_params.get("hours", "168"))
  limit = min(int(request.query_params.get("limit", "20")), 100)
  p = await get_pool()

  rows = await p.fetch("""
    SELECT id, status, started_at, ended_at, summary, metrics
    FROM ops.agent_runs
    WHERE agent_id = 'quality-eval'
      AND started_at > now() - interval '1 hour' * $1
    ORDER BY started_at DESC
    LIMIT $2
  """, hours, limit)

  runs = []
  pass_rates = []
  for r in rows:
    metrics = r["metrics"] or {}
    if isinstance(metrics, str):
      try:
        metrics = json.loads(metrics)
      except Exception:
        metrics = {}
    pass_rate = metrics.get("pass_rate")
    if isinstance(pass_rate, str):
      try:
        pass_rate = float(pass_rate)
      except Exception:
        pass_rate = None
    if pass_rate is not None:
      pass_rates.append(pass_rate)

    runs.append({
      "id": r["id"],
      "status": r["status"],
      "started_at": _ser(r["started_at"]),
      "ended_at": _ser(r["ended_at"]),
      "summary": r["summary"],
      "metrics": metrics,
      "pass_rate": pass_rate,
    })

  latest = runs[0] if runs else None
  avg_pass_rate = round(sum(pass_rates) / len(pass_rates), 3) if pass_rates else None
  trend = None
  if len(runs) >= 2 and runs[0].get("pass_rate") is not None and runs[1].get("pass_rate") is not None:
    trend = round(runs[0]["pass_rate"] - runs[1]["pass_rate"], 3)

  return JSONResponse({
    "hours": hours,
    "latest": latest,
    "avg_pass_rate": avg_pass_rate,
    "trend": trend,
    "runs": runs,
  })


async def api_reliability_summary(request: Request):
  """Reliability rollups for agent runs, MCP tool calls, and health checks."""
  err = await _check_auth(request)
  if err:
    return err
  hours = int(request.query_params.get("hours", "24"))
  p = await get_pool()

  agent_stats = await p.fetchrow("""
    SELECT COUNT(*) AS total_runs,
         COUNT(*) FILTER (WHERE status = 'error') AS errors,
         AVG(duration_ms)::int AS avg_ms,
         PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms
    FROM ops.agent_runs
    WHERE started_at > now() - interval '1 hour' * $1
  """, hours)

  mcp_stats = await p.fetchrow("""
    SELECT COUNT(*) AS total_calls,
         COUNT(*) FILTER (WHERE error IS NOT NULL) AS errors,
         AVG(duration_ms)::int AS avg_ms,
         PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
         PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99_ms
    FROM ops.mcp_query_log
    WHERE created_at > now() - interval '1 hour' * $1
  """, hours)

  health_rows = await p.fetch("""
    SELECT status, COUNT(*) AS count
    FROM ops.health_checks
    WHERE checked_at > now() - interval '1 hour' * $1
    GROUP BY status
  """, hours)

  health_counts = {r["status"]: r["count"] for r in health_rows}

  total_agent = agent_stats["total_runs"] or 0
  agent_errors = agent_stats["errors"] or 0
  total_mcp = mcp_stats["total_calls"] or 0
  mcp_errors = mcp_stats["errors"] or 0

  return JSONResponse({
    "hours": hours,
    "agents": {
      "total_runs": total_agent,
      "errors": agent_errors,
      "error_rate": round(agent_errors / max(total_agent, 1), 3),
      "avg_ms": agent_stats["avg_ms"],
      "p95_ms": agent_stats["p95_ms"],
    },
    "mcp": {
      "total_calls": total_mcp,
      "errors": mcp_errors,
      "error_rate": round(mcp_errors / max(total_mcp, 1), 3),
      "avg_ms": mcp_stats["avg_ms"],
      "p95_ms": mcp_stats["p95_ms"],
      "p99_ms": mcp_stats["p99_ms"],
    },
    "health_checks": health_counts,
  })


# ══════════════════════════════════════════════════════════════
#  ROUTES + MIDDLEWARE
# ══════════════════════════════════════════════════════════════

def _osint_auth_wrap(endpoint):
    """Wrap OSINT endpoints with dashboard auth check."""
    async def wrapped(request):
        err = await _check_auth(request)
        if err:
            return err
        return await endpoint(request)
    wrapped.__name__ = getattr(endpoint, "__name__", "osint")
    return wrapped


routes = [
    Route("/health", health),
  Route("/api/health", health),
    Route("/", page_dashboard),
    Route("/dashboard", page_dashboard),
    Route("/dashboard/login", api_login, methods=["POST"]),
    Route("/dashboard/logout", api_logout, methods=["POST"]),
    Route("/api/me", api_me),
    # Telemetry
    Route("/api/stats", api_stats),
    Route("/api/domains", api_domain_stats),
    Route("/api/doctypes", api_doctype_stats),
    Route("/api/db-health", api_db_health),
    Route("/api/timeline", api_timeline),
    # Ingestion
    Route("/api/ingestion/jobs", api_ingestion_jobs),
    Route("/api/ingestion/live", api_ingestion_live),
    Route("/api/ingestion/medical-zip", api_ingestion_medical_zip, methods=["POST"]),
    # Documents
    Route("/api/documents", api_documents),
    Route("/api/documents/{doc_id}/download", api_document_download),
    Route("/api/documents/{doc_id}", api_document_detail),
    # Legal
    Route("/api/cases", api_cases),
    # Audit
    Route("/api/audit/access", api_audit_access),
    Route("/api/audit/searches", api_audit_searches),
    # Agents
    Route("/api/agents", api_agents),
    Route("/api/agents/sessions", api_agent_sessions),
    Route("/api/agents/runs", api_agent_runs),
    Route("/api/agents/findings", api_agent_findings),
    Route("/api/agents/findings/{id:int}/resolve", api_agent_finding_resolve, methods=["POST"]),
    Route("/api/agents/{agent_id}/trigger", api_agent_trigger, methods=["POST"]),
    Route("/api/agents/{agent_id}/toggle", api_agent_toggle, methods=["POST"]),
    Route("/api/agents/{agent_id}/detail", api_agent_detail),
    Route("/api/agents/comms", api_agent_comms),
    Route("/api/agents/chatroom/channels", api_chatroom_channels),
    Route("/api/agents/chatroom/messages", api_chatroom_messages),
    Route("/api/agents/chatroom/post", api_chatroom_post, methods=["POST"]),
    Route("/api/agents/chatroom/stats", api_chatroom_stats),
    # ACL Management
    Route("/api/acl/blacklist", api_acl_blacklist),
    Route("/api/acl/blacklist/add", api_acl_blacklist_add, methods=["POST"]),
    Route("/api/acl/blacklist/{id:int}/toggle", api_acl_blacklist_toggle, methods=["POST"]),
    Route("/api/acl/blacklist/{id:int}", api_acl_blacklist_delete, methods=["DELETE"]),
    Route("/api/acl/whitelist", api_acl_whitelist, methods=["GET", "POST"]),
    Route("/api/acl/whitelist/{id:int}", api_acl_whitelist_delete, methods=["DELETE"]),

    # Search
    Route("/api/search", api_search, methods=["POST"]),
    # ADA Tagging System
    Route("/api/ada/tags", api_ada_tags),
    Route("/api/ada/agencies", api_ada_agency_summary),
    Route("/api/ada/search", api_ada_search),
    Route("/api/ada/tag", api_ada_tag_document, methods=["POST"]),
    Route("/api/ada/auto-tag", api_ada_auto_tag, methods=["POST"]),
    Route("/api/ada/auto-tag/single", api_ada_auto_tag_single, methods=["POST"]),
    Route("/api/ada/documents/{doc_id}/tags", api_ada_document_tags),
    Route("/api/ada/documents/{doc_id}/tags/{tag_slug}", api_ada_remove_tag, methods=["DELETE"]),
    Route("/api/ada/rules", api_ada_tag_rules),
    Route("/api/ada/report", api_ada_systemic_report),
    # Email Sync Management
    Route("/api/email/mailboxes", api_email_mailboxes, methods=["GET", "POST"]),
    Route("/api/email/mailboxes/{id:int}", api_email_mailbox_action, methods=["PUT", "DELETE"]),
    Route("/api/email/rules", api_email_rules, methods=["GET", "POST"]),
    Route("/api/email/rules/{id:int}", api_email_rule_action, methods=["PUT", "DELETE"]),
    Route("/api/email/runs", api_email_runs),
    Route("/api/email/sync", api_email_trigger_sync, methods=["POST"]),
    Route("/api/email/sync/live", api_email_sync_live),
    Route("/api/email/classifications", api_email_classifications),
    Route("/api/email/classify", api_email_trigger_classify, methods=["POST"]),
    # Chat-DB Email System (proxied to Chat API)
    Route("/api/cemail/accounts", api_chat_email_accounts),
    Route("/api/cemail/sync", api_chat_email_sync_status),
    Route("/api/cemail/sync/trigger", api_chat_email_trigger_sync, methods=["POST"]),
    Route("/api/cemail/messages", api_chat_email_messages),
    Route("/api/cemail/action-required", api_chat_email_action_required),
    Route("/api/cemail/semantic-search", api_chat_email_semantic_search),
    Route("/api/cemail/search", api_chat_email_search),
    Route("/api/cemail/webhooks", api_chat_email_webhooks, methods=["GET", "POST"]),
    Route("/api/cemail/webhooks/renew", api_chat_email_webhooks_renew, methods=["POST"]),
    Route("/api/cemail/webhooks/{sub_id}", api_chat_email_webhooks_delete, methods=["DELETE"]),
    # OneDrive
    Route("/api/onedrive/stats", api_onedrive_stats),
    Route("/api/onedrive/targets", api_onedrive_targets),
    Route("/api/onedrive/targets/{target_id}/toggle", api_onedrive_target_toggle, methods=["POST"]),
    Route("/api/onedrive/files", api_onedrive_files),
    Route("/api/onedrive/search", api_onedrive_search),
    Route("/api/onedrive/sync", api_onedrive_sync, methods=["POST"]),
    Route("/api/onedrive/webhooks", api_onedrive_webhooks, methods=["GET", "POST"]),
    Route("/api/onedrive/webhooks/{sub_id}", api_onedrive_webhook_delete, methods=["DELETE"]),
    # MCP Accounts & Analytics
    Route("/api/mcp/clients", api_mcp_clients),
    Route("/api/mcp/clients/{cid}/toggle", api_mcp_client_toggle, methods=["POST"]),
    Route("/api/mcp/query-log", api_mcp_query_log),
    Route("/api/mcp/query-log/{id:int}", api_mcp_query_log_detail),
    Route("/api/mcp/analytics", api_mcp_analytics),
    Route("/api/quality/summary", api_quality_summary),
    Route("/api/reliability/summary", api_reliability_summary),
    # Chat
    Route("/api/chat/conversations", api_chat_conversations, methods=["GET", "POST"]),
    Route("/api/chat/conversations/{cid}", api_chat_conversation_detail, methods=["GET", "PUT", "DELETE"]),
    Route("/api/chat/send", api_chat_send, methods=["POST"]),
    Route("/api/chat/stream", api_chat_stream, methods=["POST"]),
    Route("/api/chat/upload", api_chat_upload, methods=["POST"]),
    Route("/api/chat/attachments/{aid}", api_chat_attachment_download, methods=["GET"]),
    Route("/api/chat/retry", api_chat_retry, methods=["POST"]),
    Route("/api/chat/models", api_chat_models),
    # ── OSINT Investigation Database ──
    *[Route(r.path, _osint_auth_wrap(r.endpoint), methods=r.methods) for r in get_osint_routes()],
    # Static files — dashboard assets
    Mount("/static", StaticFiles(directory="/opt/wdws/dashboard/static"), name="static"),
    # Static files — OSINT module assets
    Mount("/osint-static", StaticFiles(directory="/opt/wdws/dashboard/osint_static"), name="osint-static"),
]

middleware = [
    Middleware(
        CORSMiddleware, allow_origins=["*"], allow_credentials=True,
        allow_methods=["*"], allow_headers=["*"],
    ),
]

app = Starlette(routes=routes, middleware=middleware)


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
def main():
    host = os.getenv("DASHBOARD_HOST", "0.0.0.0")
    print(f"\n{'=' * 60}")
    print(f"  Athena Cognitive Platform Dashboard v2.0")
    print(f"  http://{host}:{DASHBOARD_PORT}/dashboard")
    db_host = DATABASE_URL.split("@")[1] if "@" in DATABASE_URL else DATABASE_URL
    print(f"  Database: {db_host}")
    print(f"{'=' * 60}\n")
    uvicorn.run(app, host=host, port=DASHBOARD_PORT)


# ══════════════════════════════════════════════════════════════
#  EMBEDDED SPA  (dark-theme single-page application)
# ══════════════════════════════════════════════════════════════

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Athena Cognitive Platform</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><text y='28' font-size='28'>🏛️</text></svg>"/>
<style>
:root{--bg:#0a0e1a;--surface:#111827;--surface2:#1a2332;--border:#1e3a5f;
--text:#e2e8f0;--muted:#64748b;--accent:#3b82f6;--cyan:#22d3ee;--purple:#a855f7;
--green:#22c55e;--red:#ef4444;--amber:#f59e0b;--radius:8px;
--glow:0 0 20px rgba(59,130,246,.15)}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
a{color:var(--cyan);text-decoration:none}
button{cursor:pointer;font-family:inherit}
input,select,textarea{font-family:inherit;background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:var(--radius);padding:8px 12px;font-size:.9rem}
input:focus,select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 2px rgba(59,130,246,.2)}

/* Layout */
#app{display:flex;flex-direction:column;min-height:100vh}
header{background:linear-gradient(135deg,#0f172a,#1a1f3a);border-bottom:1px solid var(--border);padding:14px 24px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;box-shadow:var(--glow)}
header h1{font-size:1.1rem;font-weight:700;background:linear-gradient(135deg,var(--accent),var(--cyan),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
header .right{display:flex;align-items:center;gap:12px;color:var(--muted);font-size:.8rem}

nav{background:var(--surface);border-bottom:1px solid var(--border);display:flex;gap:0;padding:0 24px;overflow:visible;position:relative;z-index:100}
nav button{background:none;border:none;color:var(--muted);padding:11px 18px;font-size:.85rem;font-weight:500;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap}
nav button:hover{color:var(--text)}
nav button.active{color:var(--cyan);border-bottom-color:var(--cyan)}

/* Dropdown Menu */
.nav-category{position:relative;display:inline-block}
.nav-category-btn{background:none;border:none;color:var(--muted);padding:11px 18px;font-size:.85rem;font-weight:500;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap;cursor:pointer;display:flex;align-items:center;gap:6px}
.nav-category-btn:hover{color:var(--text)}
.nav-category.active .nav-category-btn{color:var(--cyan);border-bottom-color:var(--cyan)}
.nav-category-btn .arrow{font-size:.6rem;transition:transform .2s;opacity:.5}
.nav-category.open .nav-category-btn .arrow{transform:rotate(180deg)}
.nav-dropdown{position:absolute;top:100%;left:0;background:var(--surface);border:1px solid var(--border);border-radius:0 0 var(--radius) var(--radius);min-width:220px;box-shadow:0 8px 24px rgba(0,0,0,.5);display:none;z-index:9999}
.nav-category.open .nav-dropdown{display:block}
.nav-dropdown-item{background:none;border:none;color:var(--text);padding:10px 16px;font-size:.85rem;width:100%;text-align:left;transition:all .15s;display:flex;align-items:center;gap:8px;cursor:pointer}
.nav-dropdown-item:hover{background:rgba(59,130,246,.08);color:var(--cyan)}
.nav-dropdown-item.active{background:rgba(59,130,246,.12);color:var(--cyan);font-weight:600}
main{flex:1;padding:24px;max-width:1500px;margin:0 auto;width:100%}
main:has(#tab-chat:not(.hidden)){padding:0;max-width:none}

/* OSINT Investigation Module */
.osint-inv-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(360px,1fr));gap:16px}
.osint-inv-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;cursor:pointer;transition:all .2s}
.osint-inv-card:hover{border-color:var(--accent);box-shadow:0 4px 16px rgba(59,130,246,.12)}
.osint-inv-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.osint-inv-case{font-family:monospace;font-size:.85rem;color:var(--cyan)}
.osint-inv-title{font-size:1.05rem;font-weight:600;margin-bottom:6px}
.osint-inv-desc{font-size:.85rem;color:var(--muted);margin-bottom:8px;line-height:1.4}
.osint-inv-stats{display:flex;gap:14px;font-size:.8rem;color:var(--muted);flex-wrap:wrap}
.osint-inv-date{font-size:.75rem;color:var(--muted);margin-top:8px}
.osint-breadcrumb{padding:10px 0;margin-bottom:16px;font-size:.9rem;color:var(--muted);border-bottom:1px solid var(--border)}
.osint-breadcrumb a{color:var(--cyan);text-decoration:none}
.osint-breadcrumb strong{color:var(--text)}
.osint-detail-header{padding:16px 0;border-bottom:1px solid var(--border);margin-bottom:16px}
.osint-detail-header h2{font-size:1.3rem;font-weight:700}
.osint-fields{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px;padding:16px 0}
.osint-person-header{display:flex;gap:20px;align-items:center;padding:20px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.osint-person-avatar{font-size:3rem;width:64px;height:64px;display:flex;align-items:center;justify-content:center;background:var(--surface2);border-radius:50%}
.osint-person-header h2{font-size:1.3rem;font-weight:700}
.osint-contact-grid{display:flex;flex-direction:column;gap:14px;padding:8px 0}
.osint-contact-section{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px}
.osint-contact-section h4{font-size:.85rem;margin-bottom:8px;color:var(--cyan)}
.osint-contact-item{padding:6px 0;font-size:.9rem;border-bottom:1px solid var(--border)}
.osint-contact-item:last-child{border-bottom:none}
.osint-filters{display:flex;gap:10px;align-items:center;margin-bottom:16px;flex-wrap:wrap;padding:12px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.osint-filters input{min-width:180px}
.osint-pag{display:flex;gap:12px;align-items:center;justify-content:center;padding:16px 0}
.osint-timeline{border-left:3px solid var(--border);padding-left:20px;margin-left:12px}
.osint-timeline-item{position:relative;padding-bottom:16px}
.osint-timeline-item::before{content:'';position:absolute;left:-27px;top:4px;width:12px;height:12px;border-radius:50%;background:var(--accent);border:2px solid var(--bg)}
.osint-timeline-date{font-size:.8rem;color:var(--muted);margin-bottom:4px}
.osint-timeline-content{font-size:.9rem}
.osint-timeline-content strong{color:var(--cyan);margin-right:8px}
.osint-note{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:14px;margin-bottom:10px}
.osint-note-header{font-size:.8rem;color:var(--muted);margin-bottom:6px}
.osint-note-body{font-size:.9rem;line-height:1.5;white-space:pre-wrap}
.osint-raw{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:16px;font-family:monospace;font-size:.8rem;overflow-x:auto;max-height:400px;overflow-y:auto;white-space:pre;line-height:1.6}
.osint-content-preview{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;font-size:.85rem;line-height:1.6;max-height:500px;overflow-y:auto;white-space:pre-wrap;font-family:monospace}
.osint-social-profiles{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:1rem}
.osint-social-chip{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:20px;background:rgba(255,255,255,0.06);border:1px solid rgba(255,255,255,0.1);text-decoration:none;color:var(--fg);font-size:.85rem;transition:all .15s}
.osint-social-chip:hover{background:rgba(0,200,255,0.12);border-color:var(--accent)}
.osint-social-icon{font-size:1.1rem}
.osint-social-platform{text-transform:capitalize;color:var(--muted);font-size:.75rem}
.osint-social-name{font-weight:500}
.cols-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}

/* Login */
.login-box{max-width:380px;margin:80px auto;background:var(--surface);padding:32px;border-radius:12px;border:1px solid var(--border);box-shadow:var(--glow)}
.login-box h2{margin-bottom:20px;font-size:1.3rem;background:linear-gradient(135deg,var(--accent),var(--cyan));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.login-box input{width:100%;margin-bottom:12px}
.login-box button{width:100%;padding:10px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;border:none;border-radius:var(--radius);font-weight:600}
.login-box .error{color:var(--red);font-size:.85rem;margin-top:8px}

/* Cards grid */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px;margin-bottom:24px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:18px;transition:border-color .2s}
.card:hover{border-color:var(--accent)}
.card .label{font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-bottom:4px}
.card .value{font-size:1.7rem;font-weight:700}
.card .sub{font-size:.75rem;color:var(--muted);margin-top:4px}
.blue{color:var(--accent)}.cyan{color:var(--cyan)}.green{color:var(--green)}.amber{color:var(--amber)}.purple{color:var(--purple)}.red{color:var(--red)}

/* Section headers */
.section-title{font-size:1rem;font-weight:700;margin:24px 0 12px;color:var(--text);display:flex;align-items:center;gap:8px}

/* Bar chart */
.bar-chart{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px;margin-bottom:16px}
.bar-chart h3{font-size:.85rem;margin-bottom:12px;color:var(--muted)}
.bar-row{display:flex;align-items:center;margin-bottom:6px;gap:8px}
.bar-label{width:140px;font-size:.78rem;color:var(--muted);text-align:right;flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.bar-track{flex:1;background:var(--bg);border-radius:4px;height:24px;overflow:hidden}
.bar-fill{height:100%;border-radius:4px;min-width:2px;display:flex;align-items:center;padding:0 8px;font-size:.72rem;font-weight:600;color:#fff;white-space:nowrap}
.bar-count{width:60px;font-size:.78rem;color:var(--text);text-align:right}

/* Table */
.tbl-wrap{overflow-x:auto;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);margin-bottom:16px}
table{width:100%;border-collapse:collapse;font-size:.83rem}
th{background:var(--bg);padding:10px 12px;text-align:left;font-weight:600;color:var(--muted);text-transform:uppercase;font-size:.72rem;letter-spacing:.05em;position:sticky;top:0}
td{padding:8px 12px;border-top:1px solid rgba(30,58,95,.5);max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tr:hover td{background:rgba(59,130,246,.04)}

/* Badges */
.badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:.72rem;font-weight:600}
.badge-blue{background:rgba(59,130,246,.15);color:var(--accent)}
.badge-green{background:rgba(34,197,94,.15);color:var(--green)}
.badge-amber{background:rgba(245,158,11,.15);color:var(--amber)}
.badge-red{background:rgba(239,68,68,.15);color:var(--red)}
.badge-purple{background:rgba(168,85,247,.15);color:var(--purple)}
.badge-cyan{background:rgba(34,211,238,.15);color:var(--cyan)}

/* Log viewer */
.log-viewer{background:#000;border:1px solid var(--border);border-radius:var(--radius);padding:16px;font-family:'JetBrains Mono','Fira Code',monospace;font-size:.78rem;line-height:1.6;color:#a8d8a8;max-height:500px;overflow:auto;white-space:pre-wrap;word-break:break-all}

/* Buttons */
.btn{padding:8px 16px;border:none;border-radius:var(--radius);font-weight:600;font-size:.85rem;transition:all .15s}
.btn-primary{background:var(--accent);color:#fff}.btn-primary:hover{background:#2563eb}
.btn-outline{background:transparent;border:1px solid var(--border);color:var(--text)}.btn-outline:hover{border-color:var(--accent)}
.btn-sm{padding:4px 10px;font-size:.78rem}

/* Search bar */
.search-bar{display:flex;gap:8px;margin-bottom:20px;flex-wrap:wrap}
.search-bar input{flex:1;min-width:200px}

/* Two columns */
.cols-2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@media(max-width:900px){.cols-2{grid-template-columns:1fr}}

/* Status dot */
.dot{width:8px;height:8px;border-radius:50%;display:inline-block;margin-right:6px}
.dot-green{background:var(--green);box-shadow:0 0 6px var(--green)}
.dot-red{background:var(--red);box-shadow:0 0 6px var(--red)}
.dot-amber{background:var(--amber);box-shadow:0 0 6px var(--amber)}

.hidden{display:none!important}
.spinner{display:inline-block;width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--cyan);border-radius:50%;animation:spin .6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.empty-state{text-align:center;padding:40px;color:var(--muted)}
.clickable{cursor:pointer}.clickable:hover{background:rgba(59,130,246,.06)}

/* Modal */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);display:flex;align-items:center;justify-content:center;z-index:200}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;max-width:800px;width:95%;max-height:85vh;overflow-y:auto}
.modal h2{margin-bottom:16px;font-size:1.05rem;color:var(--cyan)}
.modal .field{margin-bottom:10px}
.modal .field label{display:block;font-size:.72rem;color:var(--muted);margin-bottom:3px;text-transform:uppercase;letter-spacing:.04em}
.modal .field .val{font-size:.88rem;padding:6px 0}
.modal .chunk{background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;margin-bottom:8px;font-size:.82rem;line-height:1.5}
.modal .chunk-header{font-size:.72rem;color:var(--muted);margin-bottom:6px}
.qd-modal{position:fixed;inset:0;background:rgba(0,0,0,.7);display:flex;align-items:center;justify-content:center;z-index:200}
.qd-content{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:24px;max-width:1000px;width:96%;max-height:85vh;overflow-y:auto}
.qd-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px}
.qd-header h3{margin:0;font-size:1.05rem;color:var(--cyan)}
.qd-close{background:none;border:none;color:var(--muted);font-size:1.4rem;cursor:pointer;padding:0 4px;line-height:1}
.qd-close:hover{color:var(--fg)}
.qd-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
@media(max-width:768px){.qd-grid{grid-template-columns:1fr}}
.qd-card{background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:14px}
.qd-card h4{margin:0 0 8px 0;font-size:.8rem;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.qd-json{font-family:'JetBrains Mono','Fira Code',monospace;font-size:.78rem;line-height:1.5;white-space:pre-wrap;word-break:break-word;max-height:400px;overflow-y:auto;background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;color:var(--fg)}
.qd-kv{display:flex;gap:8px;margin-bottom:6px}
.qd-kv .k{color:var(--muted);font-size:.75rem;min-width:100px;text-transform:uppercase;letter-spacing:.03em}
.qd-kv .v{font-size:.85rem;color:var(--fg)}
.qd-status-ok{color:var(--green);font-weight:600}
.qd-status-err{color:var(--red);font-weight:600}
.qd-error-box{background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:6px;padding:12px;margin-bottom:12px;color:var(--red);font-size:.85rem}
.qd-tabs{display:flex;gap:4px;margin-bottom:12px;border-bottom:1px solid var(--border);padding-bottom:4px}
.qd-tab{padding:6px 14px;border-radius:6px 6px 0 0;cursor:pointer;font-size:.8rem;color:var(--muted);border:1px solid transparent;border-bottom:none;transition:all .15s}
.qd-tab.active{color:var(--cyan);border-color:var(--border);background:var(--bg)}
.qd-tab:hover:not(.active){color:var(--fg)}
.qd-result-panel{display:none}.qd-result-panel.active{display:block}

/* Agent detail modal tabs */
.agent-tab{padding:6px 14px;border-radius:6px 6px 0 0;cursor:pointer;font-size:.8rem;color:var(--muted);border:1px solid transparent;border-bottom:none;background:none;transition:all .15s}
.agent-tab.active{color:var(--cyan);border-color:var(--border);background:var(--bg)}
.agent-tab:hover:not(.active){color:var(--fg)}
tr.qd-row{cursor:pointer;transition:background .15s}
tr.qd-row:hover{background:rgba(56,189,248,.08)!important}

/* Auto-refresh indicator */
.pulse{animation:pulse-anim 2s infinite}
@keyframes pulse-anim{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head><body>
<div id="app">

<!-- ═══ LOGIN SCREEN ═══ -->
<div id="login-screen" class="login-box">
  <h2>🔒 Athena Cognitive Platform</h2>
  <input id="login-user" type="text" placeholder="Username" autocomplete="username"/>
  <input id="login-pass" type="password" placeholder="Password" autocomplete="current-password"
    onkeydown="if(event.key==='Enter')doLogin()"/>
  <button onclick="doLogin()">Sign In</button>
  <div id="login-error" class="error hidden"></div>
</div>

<!-- ═══ MAIN APP ═══ -->
<div id="main-app" class="hidden">
<header>
  <div style="display:flex;align-items:center;gap:16px">
    <img src="/static/athena-portrait.jpg" alt="Athena AI" style="height:52px;width:52px;border-radius:8px;object-fit:cover;box-shadow:0 2px 8px rgba(0,0,0,0.15)"/>
    <h1 style="margin:0">🧠 Athena Cognitive Platform</h1>
  </div>
  <div class="right">
    <span id="hdr-info"></span>
    <span id="hdr-refresh" class="pulse" style="font-size:10px;color:var(--green)">● LIVE</span>
    <button class="btn btn-outline btn-sm" onclick="doLogout()">Logout</button>
  </div>
</header>
<nav id="main-nav">
  <div class="nav-category" data-category="osint">
    <button class="nav-category-btn">🔍 Investigations <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="osint-inv" onclick="switchTab('osint-inv')">📂 Investigations</button>
      <button class="nav-dropdown-item" data-tab="osint-persons" onclick="switchTab('osint-persons')">👤 Persons</button>
      <button class="nav-dropdown-item" data-tab="osint-records" onclick="switchTab('osint-records')">⚖️ Court Records</button>
      <button class="nav-dropdown-item" data-tab="osint-docs" onclick="switchTab('osint-docs')">📄 OSINT Documents</button>
      <button class="nav-dropdown-item" data-tab="osint-props" onclick="switchTab('osint-props')">🏠 Properties</button>
      <button class="nav-dropdown-item" data-tab="osint-search" onclick="switchTab('osint-search')">🔎 OSINT Search</button>
    </div>
  </div>
  <div class="nav-category" data-category="content">
    <button class="nav-category-btn">📚 Data & Content <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="overview" onclick="switchTab('overview')">📊 Overview</button>
      <button class="nav-dropdown-item" data-tab="documents" onclick="switchTab('documents')">📄 Documents</button>
      <button class="nav-dropdown-item" data-tab="cases" onclick="switchTab('cases')">⚖️ Legal Cases</button>
      <button class="nav-dropdown-item" data-tab="search" onclick="switchTab('search')">🔍 Search</button>
    </div>
  </div>
  <div class="nav-category" data-category="operations">
    <button class="nav-category-btn">⚙️ Operations <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="ingestion" onclick="switchTab('ingestion')">📥 Ingestion</button>
      <button class="nav-dropdown-item" data-tab="email" onclick="switchTab('email')">📧 Email Sync</button>
      <button class="nav-dropdown-item" data-tab="onedrive" onclick="switchTab('onedrive')">☁️ OneDrive</button>
    </div>
  </div>
  <div class="nav-category" data-category="ai">
    <button class="nav-category-btn">🤖 AI & Agents <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="chat" onclick="switchTab('chat')">⚖️ Athena AI</button>
      <button class="nav-dropdown-item" data-tab="chatroom" onclick="switchTab('chatroom')">🗣️ Agent Chat Room</button>
      <button class="nav-dropdown-item" data-tab="agents" onclick="switchTab('agents')">🤖 Agents</button>
      <button class="nav-dropdown-item" data-tab="comms" onclick="switchTab('comms')">📡 Agent Comms</button>
    </div>
  </div>
  <div class="nav-category" data-category="system">
    <button class="nav-category-btn">🖥️ System <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="database" onclick="switchTab('database')">🗄️ Database</button>
      <button class="nav-dropdown-item" data-tab="mcp" onclick="switchTab('mcp')">🔑 MCP Accounts</button>
    </div>
  </div>
  <div class="nav-category" data-category="analytics">
    <button class="nav-category-btn">📊 Analytics & Security <span class="arrow">▼</span></button>
    <div class="nav-dropdown">
      <button class="nav-dropdown-item" data-tab="audit" onclick="switchTab('audit')">🔐 Audit Trail</button>
      <button class="nav-dropdown-item" data-tab="analytics" onclick="switchTab('analytics')">📈 Query Analytics</button>
      <button class="nav-dropdown-item" data-tab="security" onclick="switchTab('security')">🛡️ Security</button>
    </div>
  </div>
</nav>
<main>

<!-- ═══ OVERVIEW TAB ═══ -->
<section id="tab-overview">
  <div id="stats-cards" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>
  <div class="cols-2">
    <div id="domain-chart" class="bar-chart"><h3>Documents by Domain</h3><div class="empty-state"><span class="spinner"></span></div></div>
    <div id="doctype-chart" class="bar-chart"><h3>Documents by Type</h3><div class="empty-state"><span class="spinner"></span></div></div>
  </div>
  <div class="section-title">📡 Recent Activity</div>
  <div id="timeline-list"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ DOCUMENTS TAB ═══ -->
<section id="tab-documents" class="hidden">
  <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-bottom:16px">
    <div style="display:flex;gap:12px;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;margin-bottom:10px">
      <div>
        <div style="font-size:.98rem;font-weight:700;color:var(--cyan);margin-bottom:4px">🏥 Medical ZIP Import</div>
        <div style="font-size:.82rem;color:var(--muted);max-width:760px">Upload a ZIP bundle of medical records from the portal. Supported files are safely extracted into the medical dropbox and ingestion is started immediately.</div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <input id="medical-zip-input" type="file" accept=".zip,application/zip,application/x-zip-compressed" onchange="handleMedicalZipSelect(event)" style="max-width:280px"/>
        <button class="btn btn-primary btn-sm" id="medical-zip-btn" onclick="uploadMedicalZip()">Import ZIP</button>
      </div>
    </div>
    <div style="font-size:.74rem;color:var(--muted);margin-bottom:8px">Supported after extraction: PDF, DOC/DOCX, TXT, EML, XML, CSV, RTF, XLS/XLSX, PNG/JPG/TIFF. ZIPs with unsafe paths or encryption are rejected.</div>
    <div id="medical-zip-status" class="empty-state" style="padding:14px 16px;background:rgba(255,255,255,.02);border:1px dashed var(--border);border-radius:var(--radius);font-size:.82rem">Select a medical ZIP to import.</div>
  </div>
  <div class="search-bar">
    <select id="doc-domain" onchange="docOffset=0;loadDocs()"><option value="">All Domains</option><option value="legal">Legal</option><option value="medical">Medical</option><option value="paperless">Paperless</option></select>
    <select id="doc-type" onchange="docOffset=0;loadDocs()"><option value="">All Types</option></select>
    <select id="doc-case" onchange="docOffset=0;loadDocs()"><option value="">All Cases</option></select>
    <button class="btn btn-outline btn-sm" onclick="loadDocs()">Refresh</button>
    <span id="doc-count" style="color:var(--muted);font-size:.8rem;line-height:36px"></span>
  </div>
  <div id="doc-table"></div>
  <div id="doc-paging" style="display:flex;gap:8px;margin-top:8px"></div>
</section>

<!-- ═══ CASES TAB ═══ -->
<section id="tab-cases" class="hidden">
  <div id="cases-grid"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ SEARCH TAB ═══ -->
<section id="tab-search" class="hidden">
  <div class="search-bar">
    <input id="search-q" type="text" placeholder="Full-text search across all domains…"
      onkeydown="if(event.key==='Enter')doSearch()"/>
    <select id="search-domain"><option value="">All Domains</option><option value="legal">Legal</option><option value="medical">Medical</option></select>
    <button class="btn btn-primary" onclick="doSearch()">Search</button>
  </div>
  <div id="search-results"></div>
</section>

<!-- ═══ INGESTION TAB ═══ -->
<section id="tab-ingestion" class="hidden">
  <div id="ingest-status" style="margin-bottom:16px"></div>
  <div class="section-title">📋 Live Ingestion Log</div>
  <div style="margin-bottom:8px">
    <button class="btn btn-outline btn-sm" onclick="loadIngestLive()">Refresh Log</button>
    <label style="margin-left:12px;font-size:.8rem;color:var(--muted)">
      <input type="checkbox" id="ingest-auto" onchange="toggleIngestAuto()"/> Auto-refresh (5s)
    </label>
  </div>
  <div id="ingest-log" class="log-viewer">Loading…</div>
  <div class="section-title" style="margin-top:24px">📊 Ingestion Jobs History</div>
  <div id="ingest-jobs"></div>
</section>

<!-- ═══ AUDIT TAB ═══ -->
<section id="tab-audit" class="hidden">
  <div class="section-title">🤖 MCP Client Activity <span style="font-size:.75rem;color:var(--muted);font-weight:400">— Athena AI, Claude Desktop tool calls</span></div>
  <div id="audit-mcp"></div>
  <div class="section-title" style="margin-top:24px">🔐 Access Log</div>
  <div id="audit-access"></div>
  <div class="section-title" style="margin-top:24px">🔍 Search Analytics</div>
  <div id="audit-searches"></div>
</section>

<!-- ═══ AGENTS TAB ═══ -->
<section id="tab-agents" class="hidden">
  <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap;align-items:center">
    <button class="btn btn-outline btn-sm" onclick="loadAgents()">↻ Refresh</button>
    <select id="agent-finding-filter" onchange="loadAgentFindings()" style="font-size:.8rem;padding:4px 8px">
      <option value="open">Open Findings</option>
      <option value="all">All Findings</option>
      <option value="resolved">Resolved</option>
    </select>
    <select id="agent-severity-filter" onchange="loadAgentFindings()" style="font-size:.8rem;padding:4px 8px">
      <option value="">All Severities</option>
      <option value="critical">Critical</option>
      <option value="warning">Warning</option>
      <option value="info">Info</option>
    </select>
  </div>

  <div class="section-title">🤖 Agent Fleet</div>
  <div id="agents-fleet"></div>

  <div class="section-title" style="margin-top:24px">🚨 Findings <span id="findings-badge" style="font-size:.75rem;color:var(--muted);font-weight:400"></span></div>
  <div id="agents-findings"></div>

  <div class="section-title" style="margin-top:24px">📋 Recent Runs</div>
  <div id="agents-runs"></div>
</section>

<!-- ═══ DATABASE TAB ═══ -->
<section id="tab-database" class="hidden">
  <div id="db-info"><div class="empty-state"><span class="spinner"></span></div></div>
</section>

<!-- ═══ EMAIL SYNC TAB ═══ -->
<section id="tab-email" class="hidden">
  <div id="email-stats" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title">📫 Registered Mailboxes</div>
  <div id="email-mb-form" class="hidden" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-bottom:12px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">
    <input id="mb-email" placeholder="email@example.com" style="flex:1;min-width:200px"/>
    <input id="mb-name" placeholder="Display Name" style="width:180px"/>
    <button class="btn btn-primary btn-sm" onclick="addMailbox()">Add</button>
    <button class="btn btn-outline btn-sm" onclick="$('email-mb-form').classList.add('hidden')">Cancel</button>
  </div>
  <div style="margin-bottom:8px"><button class="btn btn-outline btn-sm" onclick="$('email-mb-form').classList.remove('hidden')">+ Add Mailbox</button></div>
  <div id="email-mailboxes"></div>

  <div class="section-title" style="margin-top:24px">📋 Sync Rules <span style="font-size:.75rem;color:var(--muted);font-weight:400">— Domains, addresses &amp; keywords to monitor</span></div>
  <div id="email-rule-form" class="hidden" style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:16px;margin-bottom:12px">
    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
      <input id="rule-name" placeholder="Rule Name" style="width:180px"/>
      <select id="rule-type" style="width:160px"><option value="domain">Domain</option><option value="email_address">Email Address</option><option value="keyword">Keyword</option><option value="subject_pattern">Subject Pattern</option></select>
      <input id="rule-pattern" placeholder="Pattern (e.g. snoco.org)" style="flex:1;min-width:200px"/>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">
      <input id="rule-cases" placeholder="Case numbers (comma-separated)" style="flex:1;min-width:200px"/>
      <input id="rule-desc" placeholder="Description" style="flex:1;min-width:200px"/>
      <button class="btn btn-primary btn-sm" onclick="addRule()">Add Rule</button>
      <button class="btn btn-outline btn-sm" onclick="$('email-rule-form').classList.add('hidden')">Cancel</button>
    </div>
  </div>
  <div style="margin-bottom:8px"><button class="btn btn-outline btn-sm" onclick="$('email-rule-form').classList.remove('hidden')">+ Add Rule</button></div>
  <div id="email-rules"></div>

  <div class="section-title" style="margin-top:24px">🔄 Sync Control</div>
  <div style="margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary" id="sync-btn" onclick="triggerSync()">▶ Run Email Sync</button>
    <button class="btn btn-outline btn-sm" onclick="loadSyncLive()">Refresh Log</button>
    <span id="sync-indicator" style="font-size:.85rem"></span>
  </div>
  <div id="email-sync-log" class="log-viewer" style="max-height:300px;margin-bottom:16px">Click "Run Email Sync" or "Refresh Log" to view sync output</div>

  <div class="section-title" style="margin-top:16px">📊 Sync History</div>
  <div id="email-runs"></div>

  <div class="section-title" style="margin-top:24px">🧠 AI Email Classifications</div>
  <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary btn-sm" onclick="triggerClassify()">🤖 Classify Unprocessed</button>
    <button class="btn btn-outline btn-sm" onclick="triggerClassify(true)">♻️ Re-classify All</button>
    <span id="classify-indicator" style="font-size:.8rem;color:var(--muted)"></span>
  </div>
  <div id="email-classifications"></div>

  <!-- ═══ NEW: Chat-DB Email Integration ═══ -->
  <div style="margin-top:32px;padding-top:24px;border-top:2px solid var(--border)">
    <h2 style="color:var(--cyan);margin:0 0 16px 0;font-size:1.1rem">📨 Real-Time Email Integration <span style="font-size:.7rem;color:var(--muted);font-weight:400">(Graph API + RAG)</span></h2>
  </div>

  <div id="cemail-stats" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title" style="margin-top:20px">📫 Synced Email Accounts</div>
  <div id="cemail-accounts"></div>

  <div class="section-title" style="margin-top:20px">🔔 Graph Webhook Subscriptions</div>
  <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary btn-sm" onclick="createWebhooks()">+ Register Webhooks</button>
    <button class="btn btn-outline btn-sm" onclick="renewWebhooks()">🔄 Renew All</button>
    <span id="webhook-indicator" style="font-size:.8rem;color:var(--muted)"></span>
  </div>
  <div id="cemail-webhooks"></div>

  <div class="section-title" style="margin-top:20px">🔄 Chat-DB Sync Runs</div>
  <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary btn-sm" onclick="triggerCemailSync()">▶ Trigger RAG Sync</button>
    <span id="cemail-sync-indicator" style="font-size:.8rem;color:var(--muted)"></span>
  </div>
  <div id="cemail-sync-runs"></div>

  <div class="section-title" style="margin-top:20px">🚨 Action Required <span style="font-size:.75rem;color:var(--muted);font-weight:400">— AI-flagged urgent emails</span></div>
  <div id="cemail-action"></div>

  <div class="section-title" style="margin-top:20px">🔍 Email RAG Search</div>
  <div style="margin-bottom:12px;display:flex;gap:8px;align-items:center">
    <input id="cemail-search-q" placeholder="Semantic search across all synced emails..." style="flex:1" onkeydown="if(event.key==='Enter')cemailSearch()"/>
    <button class="btn btn-primary btn-sm" onclick="cemailSearch()">Search</button>
  </div>
  <div id="cemail-search-results"></div>

  <div class="section-title" style="margin-top:20px">📋 Recent Synced Emails <span style="font-size:.75rem;color:var(--muted);font-weight:400">(domain-filtered)</span></div>
  <div id="cemail-messages"></div>
</section>

<!-- ═══ ONEDRIVE TAB ═══ -->
<section id="tab-onedrive" class="hidden">
  <div id="od-stats" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title">📂 Sync Targets</div>
  <div id="od-targets"></div>

  <div class="section-title" style="margin-top:24px">🔔 Webhook Subscriptions</div>
  <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary btn-sm" onclick="odRegisterWebhooks()">+ Register Webhooks</button>
    <span id="od-webhook-indicator" style="font-size:.8rem;color:var(--muted)"></span>
  </div>
  <div id="od-webhooks"></div>

  <div class="section-title" style="margin-top:24px">🔄 Sync Control</div>
  <div style="margin-bottom:12px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <button class="btn btn-primary" id="od-sync-btn" onclick="odTriggerSync()">▶ Run OneDrive Sync</button>
    <span id="od-sync-indicator" style="font-size:.85rem"></span>
  </div>

  <div class="section-title" style="margin-top:16px">📊 Sync History</div>
  <div id="od-runs"></div>

  <div class="section-title" style="margin-top:24px">📊 File Type Breakdown</div>
  <div id="od-ext-breakdown"></div>

  <div class="section-title" style="margin-top:24px">🔍 OneDrive RAG Search</div>
  <div style="margin-bottom:12px;display:flex;gap:8px;align-items:center">
    <input id="od-search-q" placeholder="Semantic search across all indexed OneDrive files..." style="flex:1" onkeydown="if(event.key==='Enter')odSearch()"/>
    <select id="od-search-type" style="width:130px"><option value="hybrid">Hybrid</option><option value="semantic">Semantic</option><option value="fulltext">Full Text</option></select>
    <button class="btn btn-primary btn-sm" onclick="odSearch()">Search</button>
  </div>
  <div id="od-search-results"></div>

  <div class="section-title" style="margin-top:24px">📁 Indexed Files <span id="od-files-count" style="font-size:.75rem;color:var(--muted);font-weight:400"></span></div>
  <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
    <input id="od-files-q" placeholder="Filter by filename..." style="width:250px" onkeydown="if(event.key==='Enter')loadOdFiles()"/>
    <select id="od-files-status" style="width:130px" onchange="loadOdFiles()"><option value="">All Statuses</option><option value="indexed">Indexed</option><option value="pending">Pending</option><option value="failed">Failed</option><option value="skipped">Skipped</option></select>
    <button class="btn btn-outline btn-sm" onclick="loadOdFiles()">Refresh</button>
  </div>
  <div id="od-files"></div>
  <div id="od-files-pager" style="margin-top:8px;display:flex;gap:8px;align-items:center"></div>
</section>

<!-- ═══ CHAT TAB ═══ -->
<section id="tab-chat" class="hidden">
<style>
main:has(#tab-chat:not(.hidden)){padding:0;max-width:none}
#tab-chat{display:flex;height:calc(100vh - 110px);padding:0!important;margin:0;width:100%;position:relative}
.chat-sidebar{width:260px;min-width:260px;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.chat-sidebar-hdr{padding:16px 16px 8px;display:flex;flex-direction:column;gap:8px}
.chat-sidebar-search{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);padding:8px 12px;border-radius:8px;font-size:.82rem;outline:none;transition:border-color .2s}
.chat-sidebar-search:focus{border-color:var(--accent)}
.chat-sidebar-search::placeholder{color:var(--muted)}
.chat-sidebar-hdr .new-chat-btn{width:100%;background:none;border:1px solid var(--border);color:var(--text);padding:9px 14px;border-radius:22px;font-size:.85rem;font-weight:500;transition:all .2s;display:flex;align-items:center;gap:6px;justify-content:center;cursor:pointer}
.chat-sidebar-hdr .new-chat-btn:hover{border-color:var(--accent);color:var(--cyan);background:rgba(59,130,246,.06)}
.chat-sidebar-label{padding:14px 16px 6px;font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);display:flex;align-items:center;justify-content:space-between}
.chat-sidebar-label .toggle-archived{font-size:.7rem;font-weight:500;text-transform:none;letter-spacing:normal;color:var(--accent);cursor:pointer;padding:2px 8px;border-radius:10px;border:1px solid transparent;transition:all .2s}
.chat-sidebar-label .toggle-archived:hover{background:rgba(59,130,246,.08);border-color:var(--accent)}
.chat-conv-list{flex:1;overflow-y:auto;padding:4px 8px}
.chat-conv-item{padding:10px 12px;border-radius:8px;cursor:pointer;font-size:.84rem;color:var(--muted);display:flex;align-items:center;gap:6px;border:1px solid transparent;transition:all .15s;position:relative}
.chat-conv-item:hover{background:var(--surface2);color:var(--text)}
.chat-conv-item.active{background:var(--surface2);color:var(--text);border-color:var(--border)}
.chat-conv-item.archived{opacity:.6}
.chat-conv-item .conv-title{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.chat-conv-item .conv-title-input{flex:1;background:var(--bg);border:1px solid var(--accent);border-radius:4px;padding:4px 8px;font-size:.84rem;color:var(--text);outline:none}
.chat-conv-item .conv-actions{display:none;gap:2px;align-items:center}
.chat-conv-item:hover .conv-actions{display:flex}
.chat-conv-item .conv-action{opacity:.5;background:none;border:none;font-size:.9rem;padding:2px 4px;cursor:pointer;line-height:1;transition:all .15s;color:var(--text)}
.chat-conv-item .conv-action:hover{opacity:1;transform:scale(1.1)}
.chat-conv-item .conv-action.delete{color:var(--red)}
.chat-conv-item .conv-action.archive{color:var(--yellow)}
.chat-conv-item .conv-action.rename{color:var(--cyan)}
.chat-main{flex:1;display:flex;flex-direction:column;overflow:hidden;background:var(--bg);position:relative}
.chat-messages{flex:1;overflow-y:auto;padding:24px 0;display:flex;flex-direction:column;gap:4px;scroll-behavior:smooth}
.chat-msg-wrap{display:flex;flex-direction:column;width:100%;max-width:820px;margin:0 auto;padding:0 24px;position:relative}
.chat-msg-wrap.user{align-items:flex-end}
.chat-msg-wrap.assistant{align-items:flex-start}
.chat-msg-meta{display:flex;align-items:center;gap:8px;margin-bottom:4px;padding:0 4px}
.chat-msg-name{font-size:.72rem;font-weight:600;letter-spacing:.02em}
.chat-msg-time{font-size:.65rem;color:var(--muted);font-weight:400}
.chat-msg-wrap.user .chat-msg-name{color:var(--accent)}
.chat-msg-wrap.assistant .chat-msg-name{color:var(--purple)}
.model-badge{font-size:.62rem;color:var(--muted);font-weight:500;background:var(--surface);border:1px solid var(--border);padding:1px 8px;border-radius:10px;margin-left:4px}
.chat-msg{font-size:.92rem;line-height:1.7;word-wrap:break-word;position:relative}
.chat-msg.user{display:inline-block;max-width:75%;padding:12px 18px;border-radius:20px 20px 4px 20px;background:var(--accent);color:#fff;white-space:pre-wrap}
.chat-msg.assistant{display:block;width:100%;padding:16px 20px;border-radius:12px;background:var(--surface2);border:1px solid var(--border);white-space:pre-wrap}
.chat-msg .msg-hover-actions{position:absolute;top:8px;right:8px;display:none;gap:4px;align-items:center}
.chat-msg-wrap:hover .msg-hover-actions{display:flex}
.chat-msg .msg-hover-btn{background:var(--surface);border:1px solid var(--border);color:var(--muted);width:28px;height:28px;border-radius:6px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:.78rem;transition:all .15s}
.chat-msg .msg-hover-btn:hover{color:var(--text);border-color:var(--accent);background:var(--surface2)}
.chat-msg.assistant .tool-badge{display:inline-block;background:rgba(34,211,238,.08);border:1px solid rgba(34,211,238,.2);border-radius:14px;padding:2px 10px;font-size:.72rem;color:var(--cyan);margin:4px 4px 4px 0}
.chat-msg.assistant code{background:var(--surface);padding:1px 5px;border-radius:3px;font-size:.85em}
.chat-msg.assistant pre{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px;overflow-x:auto;margin:8px 0;font-size:.82rem;position:relative}
.chat-msg.assistant pre code{background:none;padding:0}
.chat-msg.assistant pre .code-copy-btn{position:absolute;top:6px;right:6px;background:var(--surface);border:1px solid var(--border);color:var(--muted);padding:3px 8px;border-radius:4px;font-size:.68rem;cursor:pointer;opacity:0;transition:opacity .2s}
.chat-msg.assistant pre:hover .code-copy-btn{opacity:1}
.chat-msg.assistant pre .code-copy-btn:hover{color:var(--text);border-color:var(--accent)}
.chat-msg.assistant strong{color:var(--cyan)}
.chat-msg.assistant a{color:var(--cyan)}
.chat-msg.assistant ul,.chat-msg.assistant ol{margin:8px 0;padding-left:20px}
.chat-msg.assistant li{margin:4px 0}
.chat-msg.assistant blockquote{border-left:3px solid var(--border);padding-left:12px;color:var(--muted);margin:8px 0}
.chat-msg.assistant h1,.chat-msg.assistant h2,.chat-msg.assistant h3{margin:12px 0 8px;color:var(--text)}
.chat-msg.assistant table{border-collapse:collapse;margin:8px 0;font-size:.82rem;width:100%;overflow-x:auto;display:block}
.chat-msg.assistant th,.chat-msg.assistant td{border:1px solid var(--border);padding:6px 10px;text-align:left}
.chat-msg.assistant th{background:var(--surface);color:var(--cyan);font-weight:600}
.chat-msg.assistant tr:hover td{background:rgba(59,130,246,.03)}

.chat-msg-wrap.assistant .retry-bar{display:none;align-items:center;gap:6px;margin-top:8px;padding:2px 4px}
.chat-msg-wrap.assistant:last-of-type .retry-bar,.chat-msg-wrap.assistant:hover .retry-bar{display:flex}
.retry-bar .retry-btn{background:none;border:1px solid var(--border);color:var(--muted);font-size:.72rem;padding:4px 12px;border-radius:14px;cursor:pointer;display:flex;align-items:center;gap:4px;transition:all .2s}
.retry-bar .retry-btn:hover{border-color:var(--accent);color:var(--cyan);background:rgba(59,130,246,.06)}
.retry-bar .retry-btn:disabled{opacity:.5;cursor:not-allowed}
.retry-bar .retry-model{appearance:none;-webkit-appearance:none;background:var(--bg);border:1px solid var(--border);color:var(--muted);font-size:.7rem;padding:4px 22px 4px 8px;border-radius:12px;cursor:pointer;outline:none;transition:all .2s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='8' height='5'%3E%3Cpath d='M0 0l4 5 4-5z' fill='%236b7280'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 6px center}
.retry-bar .retry-model:hover,.retry-bar .retry-model:focus{border-color:var(--accent);color:var(--text)}
.retry-bar .retry-model option{background:var(--surface);color:var(--text)}

/* Welcome screen */
.chat-welcome{display:flex;flex-direction:column;align-items:center;justify-content:center;flex:1;padding:40px 20px;text-align:center}
.chat-welcome-greeting{font-size:2rem;font-weight:300;color:var(--text);margin-bottom:40px;line-height:1.3}
.chat-welcome-greeting span{background:linear-gradient(135deg,var(--accent),var(--cyan),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-weight:500}
.chat-welcome .suggestions{display:flex;flex-wrap:wrap;gap:10px;justify-content:center;max-width:640px}
.chat-welcome .suggestions button{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:10px 20px;border-radius:24px;font-size:.85rem;transition:all .2s;white-space:nowrap;cursor:pointer}
.chat-welcome .suggestions button:hover{border-color:var(--accent);color:var(--cyan);background:rgba(59,130,246,.06);transform:translateY(-1px)}

/* Scroll-to-bottom */
.chat-scroll-btn{position:absolute;bottom:140px;left:50%;transform:translateX(-50%);width:36px;height:36px;border-radius:50%;background:var(--surface);border:1px solid var(--border);color:var(--muted);font-size:1rem;display:none;align-items:center;justify-content:center;cursor:pointer;z-index:10;transition:all .2s;box-shadow:0 2px 8px rgba(0,0,0,.2)}
.chat-scroll-btn:hover{color:var(--text);border-color:var(--accent);background:var(--surface2)}
.chat-scroll-btn.visible{display:flex}

/* Stop generation */
.chat-stop-btn{display:none;background:var(--red,#ef4444);color:#fff;border:none;padding:6px 18px;border-radius:20px;font-size:.82rem;font-weight:500;cursor:pointer;transition:all .2s;align-items:center;gap:6px;margin:0 auto 8px}
.chat-stop-btn.visible{display:flex}
.chat-stop-btn:hover{filter:brightness(1.1);transform:scale(1.02)}

/* Input area */
.chat-input-area{padding:8px 24px 24px;background:var(--bg);position:relative}
.chat-input-wrap{max-width:820px;margin:0 auto;background:var(--surface);border:1px solid var(--border);border-radius:26px;display:flex;align-items:flex-end;padding:4px 4px 4px 20px;transition:border-color .2s,box-shadow .2s}
.chat-input-wrap:focus-within{border-color:var(--accent);box-shadow:0 0 0 2px rgba(59,130,246,.15)}
.chat-input-wrap textarea{flex:1;resize:none;min-height:40px;max-height:140px;border:none;background:transparent;color:var(--text);font-size:.9rem;line-height:1.5;padding:8px 0;outline:none;font-family:inherit}
.chat-input-wrap textarea::placeholder{color:var(--muted)}
.chat-input-wrap button#chat-send-btn{width:40px;height:40px;border-radius:50%;border:none;background:var(--accent);color:#fff;font-size:1.1rem;display:flex;align-items:center;justify-content:center;transition:all .2s;flex-shrink:0;margin-bottom:2px;cursor:pointer}
.chat-input-wrap button#chat-send-btn:hover{background:#2563eb;transform:scale(1.05)}
.chat-input-wrap button#chat-send-btn:disabled{background:transparent;color:var(--muted);cursor:not-allowed;transform:none}
.chat-input-wrap select{appearance:none;-webkit-appearance:none;background:var(--bg);border:1px solid var(--border);color:var(--muted);font-size:.73rem;padding:5px 24px 5px 8px;border-radius:14px;cursor:pointer;margin:0 6px 4px 0;outline:none;flex-shrink:0;transition:all .2s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%236b7280'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center}
.chat-input-wrap select:hover,.chat-input-wrap select:focus{border-color:var(--accent);color:var(--text)}
.chat-input-wrap select option{background:var(--surface);color:var(--text)}
/* Attachment button */
.chat-attach-btn{width:36px;height:36px;border-radius:50%;border:none;background:transparent;color:var(--muted);font-size:1.2rem;display:flex;align-items:center;justify-content:center;cursor:pointer;transition:all .2s;flex-shrink:0;margin-bottom:2px}
.chat-attach-btn:hover{color:var(--accent);background:rgba(59,130,246,.08)}
/* Preview strip */
.chat-preview-strip{display:flex;gap:8px;padding:0 0 8px 0;overflow-x:auto;max-width:820px;margin:0 auto}
.chat-preview-strip:empty{display:none}
.chat-preview-item{position:relative;display:flex;align-items:center;gap:6px;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:6px 28px 6px 8px;font-size:.78rem;color:var(--text);max-width:200px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex-shrink:0}
.chat-preview-item img{width:36px;height:36px;object-fit:cover;border-radius:6px}
.chat-preview-item .file-icon{font-size:1.2rem;flex-shrink:0}
.chat-preview-item .file-info{display:flex;flex-direction:column;overflow:hidden}
.chat-preview-item .file-name{overflow:hidden;text-overflow:ellipsis}
.chat-preview-item .file-size{font-size:.65rem;color:var(--muted)}
.chat-preview-remove{position:absolute;top:-4px;right:-4px;width:18px;height:18px;border-radius:50%;border:none;background:var(--red,#ef4444);color:#fff;font-size:.7rem;display:flex;align-items:center;justify-content:center;cursor:pointer;line-height:1}
/* Drag drop overlay */
.chat-drop-overlay{display:none;position:absolute;inset:0;background:rgba(59,130,246,.08);border:2px dashed var(--accent);border-radius:16px;z-index:50;align-items:center;justify-content:center;font-size:1rem;color:var(--accent);pointer-events:none}
.chat-input-area.dragging .chat-drop-overlay{display:flex}
/* Inline images in messages */
.chat-msg img.chat-inline-img{max-width:100%;max-height:400px;border-radius:8px;margin:8px 0;cursor:pointer;transition:transform .2s}
.chat-msg img.chat-inline-img:hover{transform:scale(1.02)}
.chat-attach-chips{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0}
.chat-attach-chip{display:inline-flex;align-items:center;gap:4px;padding:4px 10px;background:rgba(59,130,246,.08);border:1px solid var(--border);border-radius:8px;font-size:.75rem;color:var(--accent);text-decoration:none;cursor:pointer;transition:all .2s}
.chat-attach-chip:hover{background:rgba(59,130,246,.15);border-color:var(--accent)}
.chat-thinking{align-self:center;width:100%;max-width:820px;color:var(--muted);font-size:.85rem;display:flex;align-items:center;gap:8px;padding:8px 24px}
.chat-thinking .dots span{animation:dotPulse 1.4s infinite;animation-fill-mode:both;display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--cyan);margin:0 2px}
.chat-thinking .dots span:nth-child(2){animation-delay:.2s}
.chat-thinking .dots span:nth-child(3){animation-delay:.4s}
@keyframes dotPulse{0%,80%,100%{opacity:.3;transform:scale(.8)}40%{opacity:1;transform:scale(1)}}
/* Edit mode */
.chat-edit-area{width:100%;background:var(--bg);border:1px solid var(--accent);border-radius:12px;padding:10px;font-size:.9rem;color:var(--text);resize:none;outline:none;font-family:inherit;min-height:60px;max-height:200px}
.chat-edit-actions{display:flex;gap:6px;margin-top:6px;justify-content:flex-end}
.chat-edit-actions button{padding:5px 14px;border-radius:14px;font-size:.78rem;cursor:pointer;transition:all .15s;border:1px solid var(--border);background:var(--surface);color:var(--text)}
.chat-edit-actions button.save{background:var(--accent);color:#fff;border-color:var(--accent)}
.chat-edit-actions button:hover{filter:brightness(1.1)}
/* Error message with retry */
.chat-error-wrap{display:flex;align-items:center;gap:10px;padding:10px 16px;background:rgba(239,68,68,.06);border:1px solid var(--red,#ef4444);border-radius:10px;margin:4px 0}
.chat-error-wrap .error-text{flex:1;color:var(--red,#ef4444);font-size:.85rem}
.chat-error-wrap .error-retry-btn{background:none;border:1px solid var(--red,#ef4444);color:var(--red,#ef4444);padding:4px 14px;border-radius:14px;font-size:.78rem;cursor:pointer;transition:all .2s;white-space:nowrap}
.chat-error-wrap .error-retry-btn:hover{background:rgba(239,68,68,.1)}
</style>
<div class="chat-sidebar">
  <div class="chat-sidebar-hdr">
    <input type="text" class="chat-sidebar-search" id="chat-search" placeholder="Search conversations..." oninput="filterConversations(this.value)">
    <button class="new-chat-btn" onclick="newConversation()">+ New Chat</button>
  </div>
  <div class="chat-sidebar-label">
    <span id="chat-view-label">Conversations</span>
    <span class="toggle-archived" id="toggle-archived" onclick="toggleArchivedView()">Archived</span>
  </div>
  <div class="chat-conv-list" id="chat-conv-list"></div>
</div>
<div class="chat-main">
  <div class="chat-messages" id="chat-messages">
    <div class="chat-welcome">
      <div class="chat-welcome-greeting">Hey <span id="chat-greeting-name">there</span>, how can I help?</div>
      <div class="suggestions">
        <button onclick="chatSuggestion('What cases are currently active?')">Active cases</button>
        <button onclick="chatSuggestion('Search for documents mentioning Starbucks')">Search documents</button>
        <button onclick="chatSuggestion('Show me recent emails about court filings')">Recent emails</button>
        <button onclick="chatSuggestion('What are the database statistics?')">Database stats</button>
        <button onclick="chatSuggestion('Search the web for Washington state RCW 4.92')">Web search</button>
        <button onclick="chatSuggestion('Summarize case 24-2-01031-31')">Summarize a case</button>
      </div>
    </div>
  </div>
  <div class="chat-scroll-btn" id="chat-scroll-btn" onclick="scrollChatToBottom()">&#8595;</div>
  <button class="chat-stop-btn" id="chat-stop-btn" onclick="stopChatGeneration()">Stop generating</button>
  <div class="chat-input-area" id="chat-input-area">
    <div class="chat-preview-strip" id="chat-preview-strip"></div>
    <div class="chat-input-wrap">
      <input type="file" id="chat-file-input" multiple accept="image/*,.pdf,.doc,.docx,.txt,.csv,.md,.json,.py,.js,.sql,.log,.xml,.html,.htm,.xlsx,.xls" style="display:none" onchange="handleFileSelect(event)">
      <button class="chat-attach-btn" onclick="document.getElementById('chat-file-input').click()" title="Attach files" type="button">&#128206;</button>
      <textarea id="chat-input" placeholder="Message Athena AI..." rows="1" onkeydown="chatKeyDown(event)" oninput="autoGrow(this)"></textarea>
      <select id="chat-model-select" title="Select model">
        <option value="claude-haiku">Haiku 4.5</option>
        <option value="claude-sonnet">Sonnet 4.6</option>
        <option value="claude-opus">Opus 4.6</option>
        <option value="auto">GPT Auto</option>
        <option value="think-low">GPT Think Low</option>
        <option value="think-med">GPT Think Med</option>
        <option value="think-high">GPT Think High</option>
      </select>
      <button id="chat-send-btn" onclick="sendChat()" title="Send">&#10148;</button>
    </div>
    <div class="chat-drop-overlay">Drop files here to attach</div>
  </div>
</div>
</section>

<!-- ═══ MCP ACCOUNTS TAB ═══ -->
<section id="tab-mcp" class="hidden">
  <div class="section-title">🔑 OAuth Client Accounts</div>
  <p style="color:var(--muted);font-size:.85rem;margin-bottom:16px">
    Manage AI clients (Athena AI, Claude Desktop) connected via OAuth 2.0 to the MCP server at
    <code style="color:var(--cyan)">klunky.12432.net</code>
  </p>
  <div id="mcp-clients-grid"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title" style="margin-top:24px">🔒 Active Access Tokens</div>
  <div id="mcp-tokens-info" style="color:var(--muted);font-size:.85rem">Loading…</div>
</section>

<!-- ═══ MCP CLIENT DETAIL MODAL ═══ -->
<div id="mcp-detail-modal" class="modal-overlay hidden" onclick="closeMcpModal()">
  <div class="modal" onclick="event.stopPropagation()" style="max-width:620px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 id="mcp-modal-title" style="margin:0;color:var(--cyan)">Client Details</h2>
      <button onclick="closeMcpModal()" style="background:none;border:none;color:var(--muted);font-size:1.4rem;padding:4px 8px;cursor:pointer;line-height:1" title="Close">&times;</button>
    </div>
    <div id="mcp-modal-body"></div>
    <div id="mcp-modal-actions" style="margin-top:16px;display:flex;justify-content:space-between;align-items:center"></div>
  </div>
</div>

<!-- ═══ QUERY ANALYTICS TAB ═══ -->
<section id="tab-analytics" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap">
    <div class="section-title" style="margin:0">📈 MCP Query Analytics</div>
    <select id="analytics-period" onchange="loadAnalytics()" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="24">Last 24 hours</option>
      <option value="168" selected>Last 7 days</option>
      <option value="720">Last 30 days</option>
      <option value="2160">Last 90 days</option>
    </select>
    <button class="btn btn-outline btn-sm" onclick="loadAnalytics()">Refresh</button>
  </div>

  <div id="analytics-summary" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>

  <div class="section-title" style="margin-top:24px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
    ✅ Quality & Reliability
    <button class="btn btn-outline btn-sm" onclick="triggerQualityEval()">▶ Run Eval</button>
    <span id="quality-trigger-status" style="font-size:.75rem;color:var(--muted)"></span>
  </div>
  <div id="quality-summary" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>
  <div id="reliability-summary" class="cards"><div class="empty-state"><span class="spinner"></span></div></div>
  <div class="section-title" style="margin-top:16px">🧪 Recent Eval Runs</div>
  <div id="quality-runs"></div>

  <div class="cols-2" style="margin-top:16px">
    <div>
      <div class="section-title">🛠️ Tool Usage Breakdown</div>
      <div id="analytics-tools"></div>
    </div>
    <div>
      <div class="section-title">👤 Client Usage Breakdown</div>
      <div id="analytics-clients"></div>
    </div>
  </div>

  <div class="section-title" style="margin-top:24px">⚠️ Recent Errors</div>
  <div id="analytics-errors"></div>

  <div class="section-title" style="margin-top:24px">📋 Query Log
    <span style="font-size:.75rem;color:var(--muted);font-weight:400">— Every tool call by AI clients</span>
  </div>
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;flex-wrap:wrap">
    <select id="qlog-client" onchange="qlogOffset=0;loadQueryLog()" style="padding:6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="">All Clients</option>
    </select>
    <select id="qlog-tool" onchange="qlogOffset=0;loadQueryLog()" style="padding:6px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem">
      <option value="">All Tools</option>
    </select>
    <span id="qlog-count" style="color:var(--muted);font-size:.8rem"></span>
    <div style="margin-left:auto;display:flex;gap:4px">
      <button class="btn btn-outline btn-sm" id="qlog-prev" onclick="qlogPrev()" disabled>&laquo; Prev</button>
      <button class="btn btn-outline btn-sm" id="qlog-next" onclick="qlogNext()">Next &raquo;</button>
    </div>
  </div>
  <div id="qlog-table"></div>
</section>

<!-- ═══ SECURITY TAB (ACL BLACKLIST / WHITELIST) ═══ -->
<section id="tab-security" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap">
    <div class="section-title" style="margin:0">🛡️ Security — IP Access Control</div>
    <button class="btn btn-outline btn-sm" onclick="loadACL()">Refresh</button>
  </div>

  <!-- Blacklist -->
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <div class="section-title" style="margin:0">🚫 IP Blacklist</div>
    <div style="display:flex;gap:8px;align-items:center">
      <input id="bl-ip" placeholder="IP address" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem;width:160px">
      <input id="bl-reason" placeholder="Reason" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem;width:200px">
      <button class="btn btn-sm" onclick="addBlacklist()">+ Block IP</button>
    </div>
  </div>
  <div id="acl-blacklist"><div class="empty-state"><span class="spinner"></span></div></div>

  <!-- Whitelist -->
  <div style="display:flex;justify-content:space-between;align-items:center;margin:24px 0 12px">
    <div class="section-title" style="margin:0">✅ IP Whitelist <span style="font-size:.75rem;color:var(--muted);font-weight:400">— IPs here are never auto-blocked</span></div>
    <div style="display:flex;gap:8px;align-items:center">
      <input id="wl-ip" placeholder="IP address" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem;width:160px">
      <input id="wl-label" placeholder="Label (optional)" style="padding:6px 10px;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:.85rem;width:200px">
      <button class="btn btn-sm" style="background:var(--success)" onclick="addWhitelist()">+ Whitelist IP</button>
    </div>
  </div>
  <div id="acl-whitelist"><div class="empty-state">Loading…</div></div>
</section>


<section id="tab-comms" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px;flex-wrap:wrap">
    <div class="section-title" style="margin:0">📡 Agent Communications & Audit Trail</div>
    <button class="btn btn-outline btn-sm" onclick="loadComms()">↻ Refresh</button>
    <select id="comms-agent-filter" onchange="loadComms()" style="font-size:.8rem;padding:4px 8px">
      <option value="">All Agents</option>
    </select>
    <select id="comms-level-filter" onchange="loadComms()" style="font-size:.8rem;padding:4px 8px">
      <option value="">All Levels</option>
      <option value="reasoning">🧠 Reasoning</option>
      <option value="code-fix">🔧 Code Fixes</option>
      <option value="email">📧 Email</option>
      <option value="error">❌ Errors</option>
      <option value="warn">⚠️ Warnings</option>
      <option value="info">ℹ️ Info</option>
    </select>
    <select id="comms-view" onchange="loadComms()" style="font-size:.8rem;padding:4px 8px">
      <option value="timeline">📜 Timeline</option>
      <option value="messages">💬 Messages</option>
      <option value="fixes">🔧 Code Fixes</option>
      <option value="emails">📧 Email Log</option>
    </select>
  </div>

  <!-- Comms Stats Cards -->
  <div id="comms-stats" class="cards" style="margin-bottom:16px"></div>

  <!-- Timeline / Main Content -->
  <div id="comms-content"></div>
</section>

<!-- ═══ CHAT ROOM TAB ═══ -->
<section id="tab-chatroom" class="hidden">
<style>
  .chatroom-layout { display:flex; height:calc(100vh - 180px); gap:0; border:1px solid var(--border); border-radius:12px; overflow:hidden; background:var(--surface); }
  .chatroom-sidebar { width:220px; min-width:180px; border-right:1px solid var(--border); display:flex; flex-direction:column; background:rgba(0,0,0,.15); }
  .chatroom-sidebar h3 { padding:16px 16px 8px; margin:0; font-size:.85rem; color:var(--muted); text-transform:uppercase; letter-spacing:.05em; }
  .channel-list { flex:1; overflow-y:auto; padding:0 8px; }
  .channel-item { display:flex; align-items:center; gap:8px; padding:8px 12px; border-radius:8px; cursor:pointer; font-size:.85rem; color:var(--text); transition:background .15s; }
  .channel-item:hover { background:rgba(255,255,255,.06); }
  .channel-item.active { background:var(--accent); color:#fff; }
  .channel-item .ch-icon { font-size:1rem; }
  .channel-item .ch-name { flex:1; }
  .channel-item .ch-badge { background:var(--accent); color:#fff; font-size:.65rem; padding:2px 6px; border-radius:99px; min-width:18px; text-align:center; }
  .channel-item.active .ch-badge { background:rgba(255,255,255,.3); }
  .chatroom-main { flex:1; display:flex; flex-direction:column; min-width:0; }
  .chatroom-header { padding:12px 20px; border-bottom:1px solid var(--border); display:flex; align-items:center; gap:12px; }
  .chatroom-header .ch-title { font-size:1.1rem; font-weight:600; }
  .chatroom-header .ch-desc { font-size:.78rem; color:var(--muted); flex:1; }
  .chatroom-header .ch-stats { font-size:.72rem; color:var(--muted); display:flex; gap:12px; }
  .chatroom-messages { flex:1; overflow-y:auto; padding:12px 20px; display:flex; flex-direction:column; gap:2px; }
  .chatroom-messages .chat-msg { display:flex; gap:10px; padding:6px 8px; border-radius:8px; transition:background .15s; position:relative; }
  .chatroom-messages .chat-msg:hover { background:rgba(255,255,255,.03); }
  .chatroom-messages .chat-msg .msg-avatar { width:36px; height:36px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:.9rem; flex-shrink:0; font-weight:600; }
  .chatroom-messages .chat-msg .msg-body { flex:1; min-width:0; }
  .chatroom-messages .chat-msg .msg-header { display:flex; align-items:baseline; gap:8px; margin-bottom:2px; }
  .chatroom-messages .chat-msg .msg-from { font-weight:600; font-size:.85rem; }
  .chatroom-messages .chat-msg .msg-time { font-size:.7rem; color:var(--muted); }
  .chatroom-messages .chat-msg .msg-type-badge { font-size:.6rem; padding:1px 5px; border-radius:4px; text-transform:uppercase; }
  .chatroom-messages .chat-msg .msg-content { font-size:.88rem; line-height:1.5; word-break:break-word; }
  .chatroom-messages .chat-msg .msg-content .mention { color:var(--accent); font-weight:600; cursor:pointer; }
  .chatroom-messages .chat-msg .msg-reply-ref { font-size:.72rem; color:var(--muted); margin-bottom:3px; cursor:pointer; }
  .chatroom-messages .chat-msg .msg-reply-ref:hover { color:var(--accent); }
  .chatroom-messages .chat-msg .msg-actions { position:absolute; right:8px; top:4px; display:none; gap:4px; }
  .chatroom-messages .chat-msg:hover .msg-actions { display:flex; }
  .msg-actions button { background:var(--surface); border:1px solid var(--border); border-radius:4px; padding:2px 6px; font-size:.7rem; cursor:pointer; color:var(--muted); }
  .msg-actions button:hover { color:var(--accent); border-color:var(--accent); }
  /* Agent colors */
  .agent-orchestrator { background:linear-gradient(135deg,#6366f1,#818cf8); color:#fff; }
  .agent-watchdog { background:linear-gradient(135deg,#f59e0b,#fbbf24); color:#000; }
  .agent-code-doctor { background:linear-gradient(135deg,#10b981,#34d399); color:#fff; }
  .agent-security-sentinel { background:linear-gradient(135deg,#ef4444,#f87171); color:#fff; }
  .agent-self-healing { background:linear-gradient(135deg,#8b5cf6,#a78bfa); color:#fff; }
  .agent-data-quality { background:linear-gradient(135deg,#06b6d4,#22d3ee); color:#fff; }
  .agent-case-strategy { background:linear-gradient(135deg,#6366f1,#818cf8); color:#fff; }
  .agent-retention { background:linear-gradient(135deg,#14b8a6,#2dd4bf); color:#fff; }
  .agent-query-insight { background:linear-gradient(135deg,#a855f7,#c084fc); color:#fff; }
  .agent-dba { background:linear-gradient(135deg,#f97316,#fb923c); color:#fff; }
  .agent-athena { background:linear-gradient(135deg,#0ea5e9,#38bdf8); color:#fff; }
  .agent-quality-eval { background:linear-gradient(135deg,#10b981,#34d399); color:#fff; }
  .agent-daily-digest { background:linear-gradient(135deg,#ec4899,#f472b6); color:#fff; }
  .agent-human { background:linear-gradient(135deg,#3b82f6,#60a5fa); color:#fff; }
  .agent-system { background:linear-gradient(135deg,#64748b,#94a3b8); color:#fff; }
  .type-alert { background:#fecaca; color:#991b1b; }
  .type-question { background:#dbeafe; color:#1e40af; }
  .type-answer { background:#dcfce7; color:#166534; }
  .type-status { background:#e0e7ff; color:#3730a3; }
  .type-system { background:#f1f5f9; color:#475569; }
  .chatroom-input { border-top:1px solid var(--border); padding:12px 20px; display:flex; gap:8px; align-items:flex-end; }
  .chatroom-input textarea { flex:1; resize:none; border:1px solid var(--border); border-radius:8px; padding:10px 14px; font-size:.88rem; font-family:inherit; background:var(--bg); color:var(--text); min-height:42px; max-height:120px; outline:none; transition:border-color .2s; }
  .chatroom-input textarea:focus { border-color:var(--accent); }
  .chatroom-input .send-btn { background:var(--accent); color:#fff; border:none; border-radius:8px; padding:10px 20px; font-size:.85rem; cursor:pointer; font-weight:600; white-space:nowrap; }
  .chatroom-input .send-btn:hover { opacity:.9; }
  .chatroom-input .send-btn:disabled { opacity:.5; cursor:not-allowed; }
  .chat-date-sep { text-align:center; color:var(--muted); font-size:.72rem; padding:12px 0 4px; }
  .chat-date-sep span { background:var(--surface); padding:2px 12px; border-radius:99px; border:1px solid var(--border); }
  .reply-preview { background:rgba(99,102,241,.1); border-left:3px solid var(--accent); padding:6px 12px; margin-bottom:8px; border-radius:0 6px 6px 0; font-size:.8rem; display:flex; align-items:center; gap:8px; }
  .reply-preview .rp-close { cursor:pointer; color:var(--muted); padding:2px 6px; }
  .reply-preview .rp-close:hover { color:var(--accent); }
  .chatroom-online { padding:12px 16px; border-top:1px solid var(--border); }
  .chatroom-online .online-agents { display:flex; flex-wrap:wrap; gap:4px; }
  .chatroom-online .online-dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
  .chatroom-online .agent-tag { font-size:.68rem; padding:2px 6px; border-radius:4px; background:rgba(255,255,255,.06); display:flex; align-items:center; gap:4px; }
  .chatroom-empty { text-align:center; color:var(--muted); padding:60px 20px; }
  .chatroom-empty .big-icon { font-size:3rem; margin-bottom:12px; }
</style>

<div class="chatroom-layout">
  <!-- Sidebar: Channels -->
  <div class="chatroom-sidebar">
    <h3>Channels</h3>
    <div class="channel-list" id="cr-channels"></div>
    <div class="chatroom-online" id="cr-online">
      <div style="font-size:.7rem;color:var(--muted);margin-bottom:6px">RECENT AGENTS</div>
      <div class="online-agents" id="cr-agents-online"></div>
    </div>
  </div>

  <!-- Main -->
  <div class="chatroom-main">
    <div class="chatroom-header">
      <span id="cr-ch-icon" class="ch-title"></span>
      <span id="cr-ch-name" class="ch-title">#general</span>
      <span id="cr-ch-desc" class="ch-desc"></span>
      <div class="ch-stats">
        <span id="cr-stat-total">—</span>
        <span id="cr-stat-24h">—</span>
      </div>
      <button class="btn btn-outline btn-sm" onclick="crRefresh()" title="Refresh">↻</button>
    </div>

    <div class="chatroom-messages" id="cr-messages">
      <div class="chatroom-empty">
        <div class="big-icon">💬</div>
        <div>No messages yet. Agents will start chatting soon!</div>
      </div>
    </div>

    <!-- Reply preview -->
    <div id="cr-reply-preview" class="reply-preview" style="display:none">
      <span>↩ Replying to <strong id="cr-reply-from"></strong>: <span id="cr-reply-text"></span></span>
      <span class="rp-close" onclick="crCancelReply()">✕</span>
    </div>

    <!-- Input -->
    <div class="chatroom-input">
      <textarea id="cr-input" placeholder="Type a message... Use @agent to mention" rows="1"
                onkeydown="if(event.key==='Enter'&&!event.shiftKey){event.preventDefault();crSend()}"
                oninput="this.style.height='auto';this.style.height=Math.min(this.scrollHeight,120)+'px'"></textarea>
      <button class="send-btn" onclick="crSend()" id="cr-send-btn">Send</button>
    </div>
  </div>
</div>
</section>


<!-- ═══ OSINT MODULE TABS ═══ -->
<section id="tab-osint-inv" class="hidden">
  <div id="osint-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-persons" class="hidden">
  <div id="osint-persons-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-records" class="hidden">
  <div id="osint-records-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-docs" class="hidden">
  <div id="osint-docs-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-props" class="hidden">
  <div id="osint-props-content"><div class="empty-state"><span class="spinner"></span></div></div>
</section>
<section id="tab-osint-search" class="hidden">
  <div style="display:flex;gap:12px;align-items:center;margin-bottom:16px">
    <input id="osint-search-input" placeholder="Search across all OSINT data..." style="flex:1;padding:10px 14px;font-size:1rem"
      onkeydown="if(event.key==='Enter')doOsintSearch()"/>
    <button class="btn btn-primary" onclick="doOsintSearch()">🔍 Search</button>
  </div>
  <div id="osint-search-results"></div>
</section>

</main>
</div>

<!-- ═══ DOCUMENT MODAL ═══ -->
<div id="doc-modal" class="modal-overlay hidden" onclick="closeModal()">
  <div class="modal" onclick="event.stopPropagation()">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 id="modal-title" style="margin:0">Document</h2>
      <button onclick="closeModal()" style="background:none;border:none;color:var(--muted);font-size:1.4rem;padding:4px 8px;cursor:pointer;line-height:1" title="Close">&times;</button>
    </div>
    <div id="modal-body"></div>
    <div style="margin-top:16px;text-align:right">
      <button class="btn btn-outline btn-sm" onclick="closeModal()">Close</button>
    </div>
  </div>
</div>

<!-- ═══ QUERY DETAIL MODAL ═══ -->
<div id="qd-overlay" class="modal-overlay hidden" onclick="closeQdModal()">
  <div class="modal qd-modal" onclick="event.stopPropagation()">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px">
      <h2 id="qd-title" style="margin:0;color:var(--cyan)">Query Detail</h2>
      <button onclick="closeQdModal()" style="background:none;border:none;color:var(--muted);font-size:1.4rem;padding:4px 8px;cursor:pointer;line-height:1" title="Close">&times;</button>
    </div>
    <div id="qd-body"><span class="spinner"></span></div>
  </div>
</div>

</div><!-- /app -->

<script src="/osint-static/osint.js"></script>
<script>
// ═══════════════════════════════════════════════════════════
//  GLOBAL STATE
// ═══════════════════════════════════════════════════════════
let stats={}, docOffset=0, casesList=[], ingestTimer=null, currentUser='User';
let medicalZipPendingName='';

async function api(path, opts={}){
  const r = await fetch(path, {credentials:'same-origin', ...opts,
    headers: {'Content-Type':'application/json', ...(opts.headers||{})}});
  if (r.status===401) { showLogin(); throw new Error('unauthorized'); }
  return r.json();
}

// ═══ AUTH ═══
async function doLogin(){
  const u=$('login-user').value, p=$('login-pass').value;
  try{
    const r = await fetch('/dashboard/login', {method:'POST', credentials:'same-origin',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({username:u, password:p})});
    if(!r.ok) throw 0;
    const d = await r.json();
    currentUser = d.display_name || u;
    $('login-screen').classList.add('hidden');
    $('main-app').classList.remove('hidden');
    init();
  } catch {
    const e = $('login-error');
    e.textContent = 'Invalid credentials';
    e.classList.remove('hidden');
  }
}
function doLogout(){
  fetch('/dashboard/logout',{method:'POST',credentials:'same-origin'});
  showLogin();
}
function showLogin(){
  $('login-screen').classList.remove('hidden');
  $('main-app').classList.add('hidden');
}

// ═══ UTILITY ═══
function $(id){ return document.getElementById(id); }

// ═══ DROPDOWN MENUS ═══
(function initDropdowns() {
  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.nav-category-btn').forEach(btn => {
      btn.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        const parent = this.closest('.nav-category');
        const wasOpen = parent.classList.contains('open');
        document.querySelectorAll('.nav-category').forEach(c => c.classList.remove('open'));
        if (!wasOpen) parent.classList.add('open');
      });
    });
    document.addEventListener('click', function(e) {
      if (!e.target.closest('.nav-category')) {
        document.querySelectorAll('.nav-category').forEach(c => c.classList.remove('open'));
      }
    });
  });
})();
function switchTab(t){
  // Save to localStorage for persistence across refresh
  localStorage.setItem('acp-current-tab', t);
  
  document.querySelectorAll('main>section').forEach(s=>s.classList.add('hidden'));
  $('tab-'+t).classList.remove('hidden');
  
  // Update active states for dropdown items
  document.querySelectorAll('.nav-dropdown-item').forEach(
    b => b.classList.toggle('active', b.dataset.tab===t));
  // Update active state for parent category
  document.querySelectorAll('.nav-category').forEach(cat => {
    const hasActive = cat.querySelector('.nav-dropdown-item.active');
    cat.classList.toggle('active', !!hasActive);
  });
  // Close all dropdowns
  document.querySelectorAll('.nav-category').forEach(c => c.classList.remove('open'));
  
  const loaders = {
    overview: loadOverview,
    documents: loadDocs,
    cases: loadCases,
    ingestion: ()=>{ loadIngestLive(); loadIngestJobs(); },
    audit: loadAudit,
    agents: loadAgents,
    database: loadDbHealth,
    email: loadEmail,
    chat: loadChat,
    onedrive: loadOneDrive,
    mcp: loadMcpAccounts,
    analytics: loadAnalytics,
    security: loadACL,
    comms: loadComms,
    chatroom: loadChatRoom,
    'osint-inv': loadOsintInvestigations,
    'osint-persons': loadOsintPersons,
    'osint-records': loadOsintCourtRecords,
    'osint-docs': loadOsintDocuments,
    'osint-props': loadOsintProperties,
    'osint-search': ()=>{},
  };
  if (loaders[t]) loaders[t]();
}

const COLORS=['#3b82f6','#22d3ee','#a855f7','#22c55e','#f59e0b',
              '#ef4444','#ec4899','#6366f1','#14b8a6'];
function barColor(i){ return COLORS[i % COLORS.length]; }
function fmtTime(iso){
  if(!iso) return '—';
  try { return new Date(iso).toLocaleString('en-US',
    {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit',second:'2-digit'});
  } catch { return iso; }
}
function fmtNum(n){ return (n||0).toLocaleString(); }
function esc(s){ return s ? s.replace(/&/g,'&amp;').replace(/</g,'&lt;')
  .replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }

// ═══════════════════════════════════════════════════════════
//  TAB: OVERVIEW
// ═══════════════════════════════════════════════════════════
async function loadOverview(){
  try {
    const [s, doms, dts, tl] = await Promise.all([
      api('/api/stats'), api('/api/domains'),
      api('/api/doctypes'), api('/api/timeline?limit=20')
    ]);
    stats = s;
    $('hdr-info').textContent =
      `${fmtNum(s.total_documents)} docs · ${fmtNum(s.total_chunks)} chunks`;

    const embPct = s.total_chunks
      ? Math.round((s.chunks_with_embeddings||0) / s.total_chunks * 100) : 0;

    $('stats-cards').innerHTML = `
      <div class="card"><div class="label">Total Documents</div>
        <div class="value blue">${fmtNum(s.total_documents)}</div></div>
      <div class="card"><div class="label">Total Chunks</div>
        <div class="value cyan">${fmtNum(s.total_chunks)}</div></div>
      <div class="card"><div class="label">With Embeddings</div>
        <div class="value green">${fmtNum(s.chunks_with_embeddings)}</div>
        <div class="sub">${embPct}% coverage</div></div>
      <div class="card"><div class="label">Legal Cases</div>
        <div class="value purple">${fmtNum(s.legal_cases)}</div></div>
      <div class="card"><div class="label">Entities</div>
        <div class="value amber">${fmtNum(s.entities)}</div></div>
      <div class="card"><div class="label">Patients</div>
        <div class="value green">${fmtNum(s.patients)}</div></div>
      <div class="card"><div class="label">Searches (24h)</div>
        <div class="value cyan">${fmtNum(s.recent_searches)}</div></div>
      <div class="card"><div class="label">API Calls (24h)</div>
        <div class="value blue">${fmtNum(s.recent_access)}</div></div>`;

    // Domain bar chart
    if (doms.length) {
      const mx = Math.max(...doms.map(d=>d.documents), 1);
      $('domain-chart').innerHTML = '<h3>Documents by Domain</h3>' +
        doms.map((d,i)=>`<div class="bar-row">
          <div class="bar-label">${d.domain}</div>
          <div class="bar-track"><div class="bar-fill"
            style="width:${d.documents/mx*100}%;background:${barColor(i)}">${d.documents}</div></div>
          <div class="bar-count">${fmtNum(d.chunks)} ch</div></div>`).join('');
    }

    // DocType bar chart
    if (dts.length) {
      const mx = Math.max(...dts.map(d=>d.count), 1);
      $('doctype-chart').innerHTML = '<h3>Documents by Type</h3>' +
        dts.slice(0,10).map((d,i)=>`<div class="bar-row">
          <div class="bar-label">${d.domain}/${d.document_type}</div>
          <div class="bar-track"><div class="bar-fill"
            style="width:${d.count/mx*100}%;background:${barColor(i)}">${d.count}</div></div></div>`).join('');
    }

    // Timeline
    const icons = {document:'📄', ingestion:'⚙️', search:'🔍', access:'🔐'};
    $('timeline-list').innerHTML = tl.length
      ? `<div class="tbl-wrap"><table>
          <thead><tr><th>Time</th><th>Event</th><th>Domain</th><th>Detail</th><th>Description</th></tr></thead>
          <tbody>${tl.map(e=>`<tr>
            <td>${fmtTime(e.timestamp)}</td>
            <td>${icons[e.event_type]||''} ${e.event_type}</td>
            <td><span class="badge badge-blue">${e.domain||''}</span></td>
            <td>${e.detail||''}</td>
            <td title="${esc(e.description||'')}">${(e.description||'').substring(0,60)}</td>
          </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No recent activity</div>';
  } catch(e) {
    console.error(e);
    $('stats-cards').innerHTML = '<div class="empty-state">Failed to load stats — check DB connection</div>';
  }
}

// ═══════════════════════════════════════════════════════════
//  TAB: DOCUMENTS
// ═══════════════════════════════════════════════════════════
function setMedicalZipStatus(html, isError=false){
  const el=$('medical-zip-status');
  if(!el) return;
  el.className='';
  el.style.padding='14px 16px';
  el.style.background=isError?'rgba(239,68,68,.08)':'rgba(255,255,255,.02)';
  el.style.border='1px solid ' + (isError?'rgba(239,68,68,.35)':'var(--border)');
  el.style.borderRadius='var(--radius)';
  el.style.fontSize='.82rem';
  el.innerHTML=html;
}

function handleMedicalZipSelect(event){
  const file=(event.target.files||[])[0];
  medicalZipPendingName=file?file.name:'';
  if(!file){
    setMedicalZipStatus('Select a medical ZIP to import.');
    return;
  }
  const size=file.size<1048576?`${(file.size/1024).toFixed(1)} KB`:`${(file.size/1048576).toFixed(1)} MB`;
  setMedicalZipStatus(`Ready to import <strong>${esc(file.name)}</strong> (${size}). The portal will extract supported files into the medical dropbox and run ingestion.`);
}

async function uploadMedicalZip(){
  const input=$('medical-zip-input');
  const btn=$('medical-zip-btn');
  const file=(input.files||[])[0];
  if(!file){
    setMedicalZipStatus('Please choose a ZIP file first.', true);
    return;
  }
  if(!/\.zip$/i.test(file.name)){
    setMedicalZipStatus('Only .zip files are supported for portal medical import.', true);
    return;
  }

  const fd=new FormData();
  fd.append('file', file);
  fd.append('ingest_now', 'true');

  btn.disabled=true;
  setMedicalZipStatus('<span class="spinner"></span> Uploading ZIP, extracting medical records, and starting ingestion...');
  try {
    const resp=await fetch('/api/ingestion/medical-zip',{method:'POST',body:fd,credentials:'same-origin'});
    const data=await resp.json();
    if(resp.status===401){ showLogin(); throw new Error('unauthorized'); }
    if(!resp.ok) throw new Error(data.error||'Medical ZIP import failed');

    const extracted=(data.extracted_files||[]).slice(0,6).map(f=>`<li><code>${esc(f)}</code></li>`).join('');
    const skipped=(data.skipped_files||[]).slice(0,4).map(f=>`<li><code>${esc(f.path||'')}</code> — ${esc(f.reason||'skipped')}</li>`).join('');
    const ingest=data.ingestion||{};
    const ingestLine=ingest.completed
      ? `Ingestion finished${ingest.processed_count!=null?` and processed <strong>${ingest.processed_count}</strong> pending file(s).`:'.'}`
      : 'Ingestion is still running in the background — the live log below will show progress.';
    const logTail=ingest.log_tail?`<details style="margin-top:10px"><summary style="cursor:pointer;color:var(--cyan)">Watcher output</summary><pre style="margin-top:8px;background:var(--bg);border:1px solid var(--border);border-radius:8px;padding:10px;white-space:pre-wrap;max-height:220px;overflow:auto">${esc(ingest.log_tail)}</pre></details>`:'';
    setMedicalZipStatus(`
      <div style="display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap;align-items:center">
        <div><strong>Imported ${esc(data.uploaded_file||medicalZipPendingName||file.name)}</strong><br/><span style="color:var(--muted)">${ingestLine}</span></div>
        <button class="btn btn-outline btn-sm" onclick="$('doc-domain').value='medical';docOffset=0;switchTab('documents')">View medical documents</button>
      </div>
      <div style="margin-top:10px;display:flex;gap:16px;flex-wrap:wrap;color:var(--muted)">
        <span><strong style="color:var(--text)">${data.extracted_count||0}</strong> extracted</span>
        <span><strong style="color:var(--text)">${data.skipped_count||0}</strong> skipped</span>
        <span>staged under <code>${esc(data.staging_dir||'')}</code></span>
      </div>
      ${extracted?`<div style="margin-top:10px"><div style="font-size:.75rem;color:var(--muted);margin-bottom:4px">Extracted files</div><ul style="margin:0;padding-left:18px">${extracted}</ul></div>`:''}
      ${skipped?`<div style="margin-top:10px"><div style="font-size:.75rem;color:var(--muted);margin-bottom:4px">Skipped files</div><ul style="margin:0;padding-left:18px">${skipped}</ul></div>`:''}
      ${logTail}
    `);
    input.value='';
    medicalZipPendingName='';
    $('doc-domain').value='medical';
    docOffset=0;
    loadDocs();
    loadIngestLive();
    loadIngestJobs();
  } catch(e) {
    setMedicalZipStatus(esc(e.message||'Medical ZIP import failed'), true);
  } finally {
    btn.disabled=false;
  }
}

async function loadDocs(){
  const domain=$('doc-domain').value, dtype=$('doc-type').value, cn=$('doc-case').value;
  const limit=50;
  let url=`/api/documents?limit=${limit}&offset=${docOffset}`;
  if(domain) url+=`&domain=${domain}`;
  if(dtype) url+=`&type=${dtype}`;
  if(cn) url+=`&case_number=${cn}`;
  $('doc-table').innerHTML='<div class="empty-state"><span class="spinner"></span></div>';
  try {
    const data=await api(url);
    $('doc-count').textContent=`${fmtNum(data.total)} documents total`;
    if(!data.documents.length){
      $('doc-table').innerHTML='<div class="empty-state">No documents found</div>'; return;
    }
    $('doc-table').innerHTML=`<div class="tbl-wrap"><table>
      <thead><tr><th>Domain</th><th>Type</th><th>Title / Filename</th><th>Case</th><th>Chunks</th><th>Ingested</th></tr></thead>
      <tbody>${data.documents.map(d=>`<tr class="clickable" onclick="openDoc('${d.id}')">
        <td><span class="badge badge-${d.domain==='legal'?'blue':d.domain==='medical'?'green':'purple'}">${d.domain}</span></td>
        <td><span class="badge badge-cyan">${d.document_type||''}</span></td>
        <td title="${esc(d.title||d.filename||'')}">${d.title||d.filename||'—'}</td>
        <td>${d.case_number||''}</td>
        <td>${d.total_chunks||0}</td>
        <td>${fmtTime(d.created_at)}</td>
      </tr>`).join('')}</tbody></table></div>`;

    // Pagination
    const pages=Math.ceil(data.total/limit), cur=Math.floor(docOffset/limit);
    let pg='';
    if(cur>0) pg+=`<button class="btn btn-outline btn-sm" onclick="docOffset=${docOffset-limit};loadDocs()">← Prev</button>`;
    pg+=`<span style="color:var(--muted);font-size:.8rem;line-height:32px">Page ${cur+1} of ${pages}</span>`;
    if(cur<pages-1) pg+=`<button class="btn btn-outline btn-sm" onclick="docOffset=${docOffset+limit};loadDocs()">Next →</button>`;
    $('doc-paging').innerHTML=pg;
  } catch { $('doc-table').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function openDoc(id){
  showModal();
  $('modal-title').textContent='Loading…';
  $('modal-body').innerHTML='<span class="spinner"></span>';
  try {
    const data=await api(`/api/documents/${id}`);
    const d=data.document;
    const downloadUrl=d.download_url||(`/api/documents/${id}/download`);
    const inlineUrl=downloadUrl+(downloadUrl.includes('?')?'&':'?')+'disposition=inline';
    $('modal-title').textContent = d.title || d.filename || id;
    let h='';
    h+=`<div style="display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap;margin-bottom:12px">
      <a class="btn btn-primary btn-sm" href="${downloadUrl}" target="_blank" rel="noopener">⬇ Download Original</a>
      <a class="btn btn-outline btn-sm" href="${inlineUrl}" target="_blank" rel="noopener">👁 Open Inline</a>
    </div>`;
    h+=field('ID', `<span style="font-family:monospace;font-size:.8rem">${d.id}</span>`);
    h+=field('Domain / Type', `<span class="badge badge-blue">${d.domain}</span>
      <span class="badge badge-cyan">${d.document_type||''}</span>`);
    h+=field('Source Path', `<span style="font-size:.8rem;word-break:break-all">${d.source_path||''}</span>`);
    if(d.case_number) h+=field('Case', `${d.case_number} — ${d.case_title||''} (${d.court||''})`);
    if(d.sender) h+=field('Email', `From: ${d.sender}<br/>To: ${d.recipients||''}<br/>
      Direction: ${d.direction||''} · Mailbox: ${d.mailbox||''}`);
    if(d.date_sent) h+=field('Date Sent', d.date_sent);
    if(d.patient_name) h+=field('Patient', d.patient_name);
    if(d.provider_name) h+=field('Provider', `${d.provider_name} — ${d.facility||''}`);
    if(d.date_of_service) h+=field('Date of Service', d.date_of_service);
    h+=field('Content Hash', `<span style="font-family:monospace;font-size:.8rem">${d.content_hash||''}</span>`);
    h+=field('Ingested', d.created_at);
    if(d.full_content_preview) h+=`<div class="section-title" style="margin:16px 0 8px">📝 Content Preview</div>
      <div class="chunk">${esc(d.full_content_preview)}</div>`;
    h+=`<div class="section-title" style="margin:16px 0 8px">📦 Chunks (${data.chunks.length})</div>`;
    h+=data.chunks.map(c=>`<div class="chunk">
      <div class="chunk-header">Chunk ${c.chunk_index+1}/${c.total_chunks}
        ${c.has_embedding?'<span class="badge badge-green">embedded</span>':'<span class="badge badge-red">no embedding</span>'}</div>
      ${esc(c.content)}</div>`).join('');
    $('modal-body').innerHTML=h;
  } catch { $('modal-body').innerHTML='<div class="empty-state">Failed to load document</div>'; }
}
function field(label, val){ return `<div class="field"><label>${label}</label><div class="val">${val}</div></div>`; }
function closeModal(){
  $('doc-modal').classList.add('hidden');
}
function showModal(){
  $('doc-modal').classList.remove('hidden');
}
// ═══ Query Detail Modal ═══
function closeQdModal(){ $('qd-overlay').classList.add('hidden'); }
function showQdModal(){ $('qd-overlay').classList.remove('hidden'); }

function qdSwitchTab(tabName) {
  document.querySelectorAll('.qd-tab').forEach(t => t.classList.toggle('active', t.dataset.tab===tabName));
  document.querySelectorAll('.qd-result-panel').forEach(p => p.classList.toggle('active', p.dataset.panel===tabName));
}

function syntaxHighlight(json) {
  if (typeof json !== 'string') json = JSON.stringify(json, null, 2);
  json = json.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function(match) {
    let cls = 'number';
    if (/^"/.test(match)) {
      cls = /:$/.test(match) ? 'key' : 'string';
    } else if (/true|false/.test(match)) {
      cls = 'boolean';
    } else if (/null/.test(match)) {
      cls = 'null';
    }
    const colors = {key:'#38bdf8',string:'#86efac',number:'#fbbf24',boolean:'#c084fc',null:'#94a3b8'};
    return '<span style="color:'+colors[cls]+'">'+match+'</span>';
  });
}

function formatBytes(str) {
  const bytes = new TextEncoder().encode(str).length;
  if (bytes > 1024*1024) return (bytes/1024/1024).toFixed(1) + ' MB';
  if (bytes > 1024) return (bytes/1024).toFixed(1) + ' KB';
  return bytes + ' B';
}

async function openQueryDetail(id) {
  showQdModal();
  $('qd-title').textContent = 'Loading…';
  $('qd-body').innerHTML = '<span class="spinner"></span>';
  try {
    const d = await api('/api/mcp/query-log/' + id);
    $('qd-title').innerHTML = '<span class="badge badge-purple" style="font-size:.9rem;margin-right:8px">' + esc(d.tool_name) + '</span> Query #' + d.id;

    const ts = new Date(d.called_at);
    const timeStr = ts.toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric',year:'numeric'})
      + ' ' + ts.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit'});

    let argsObj = d.arguments;
    if (typeof argsObj === 'string') try { argsObj = JSON.parse(argsObj); } catch(e){}
    let argsJson = '{}';
    try { argsJson = JSON.stringify(argsObj, null, 2); } catch(e){ argsJson = String(argsObj); }

    let summaryObj = d.result_summary;
    if (typeof summaryObj === 'string') try { summaryObj = JSON.parse(summaryObj); } catch(e){}
    let summaryJson = '{}';
    try { summaryJson = JSON.stringify(summaryObj, null, 2); } catch(e){ summaryJson = String(summaryObj); }

    // Try to pretty-print result_raw
    let resultFormatted = d.result_raw || '';
    let resultIsJson = false;
    try {
      const parsed = JSON.parse(resultFormatted);
      resultFormatted = JSON.stringify(parsed, null, 2);
      resultIsJson = true;
    } catch(e) { /* not JSON, show as-is */ }

    let h = '';

    // Top summary bar
    h += '<div class="qd-grid">';
    h += '<div class="qd-card">';
    h += '<h4>⏱ Execution</h4>';
    h += '<div class="qd-kv"><span class="k">Time</span><span class="v">' + timeStr + '</span></div>';
    h += '<div class="qd-kv"><span class="k">Duration</span><span class="v">' + (d.duration_ms != null ? d.duration_ms + 'ms' : '—') + '</span></div>';
    h += '<div class="qd-kv"><span class="k">Status</span><span class="v ' + (d.error ? 'qd-status-err' : 'qd-status-ok') + '">' + (d.error ? '✗ Error' : '✓ Success') + '</span></div>';
    h += '<div class="qd-kv"><span class="k">Results</span><span class="v">' + (d.result_count != null ? d.result_count + ' items' : '—') + '</span></div>';
    h += '</div>';
    h += '<div class="qd-card">';
    h += '<h4>🔌 Client</h4>';
    h += '<div class="qd-kv"><span class="k">Name</span><span class="v">' + esc(d.client_name || d.client_id) + '</span></div>';
    h += '<div class="qd-kv"><span class="k">Client ID</span><span class="v" style="font-family:monospace;font-size:.8rem">' + esc(d.client_id) + '</span></div>';
    h += '<div class="qd-kv"><span class="k">Tool</span><span class="v"><span class="badge badge-purple">' + esc(d.tool_name) + '</span></span></div>';
    h += '<div class="qd-kv"><span class="k">Response Size</span><span class="v">' + (resultFormatted ? formatBytes(resultFormatted) : '—') + '</span></div>';
    h += '</div></div>';

    // Error box if any
    if (d.error) {
      h += '<div class="qd-error-box"><strong>⚠ Error:</strong> ' + esc(d.error) + '</div>';
    }

    // Request Arguments
    h += '<div class="qd-card" style="margin-bottom:16px">';
    h += '<h4>📤 Request Arguments</h4>';
    h += '<pre class="qd-json">' + syntaxHighlight(argsJson) + '</pre>';
    h += '</div>';

    // Response with tabs
    h += '<div class="qd-card">';
    h += '<h4>📥 Response</h4>';
    h += '<div class="qd-tabs">';
    h += '<div class="qd-tab active" data-tab="raw" onclick="qdSwitchTab(\'raw\')">Full Response</div>';
    h += '<div class="qd-tab" data-tab="summary" onclick="qdSwitchTab(\'summary\')">Summary</div>';
    h += '</div>';

    if (resultFormatted) {
      h += '<div class="qd-result-panel active" data-panel="raw">';
      h += '<pre class="qd-json">' + (resultIsJson ? syntaxHighlight(resultFormatted) : esc(resultFormatted)) + '</pre>';
      h += '</div>';
    } else {
      h += '<div class="qd-result-panel active" data-panel="raw">';
      h += '<div class="empty-state" style="padding:20px">No raw result stored for this query.<br><span style="font-size:.8rem;color:var(--muted)">Results are captured for queries made after this update.</span></div>';
      h += '</div>';
    }

    h += '<div class="qd-result-panel" data-panel="summary">';
    h += '<pre class="qd-json">' + syntaxHighlight(summaryJson) + '</pre>';
    h += '</div>';
    h += '</div>';

    $('qd-body').innerHTML = h;
  } catch(e) {
    $('qd-body').innerHTML = '<div class="empty-state">Failed to load query detail: ' + esc(String(e)) + '</div>';
  }
}

document.addEventListener('keydown',function(e){
  if(e.key==='Escape') { closeModal(); closeQdModal(); closeMcpModal(); }
});

// ═══════════════════════════════════════════════════════════
//  TAB: CASES
// ═══════════════════════════════════════════════════════════
async function loadCases(){
  $('cases-grid').innerHTML='<div class="empty-state"><span class="spinner"></span></div>';
  try {
    casesList = await api('/api/cases');
    // Populate filter
    const sel=$('doc-case');
    while(sel.options.length>1) sel.remove(1);
    casesList.forEach(c=>sel.add(new Option(
      `${c.case_number} — ${(c.case_title||'').substring(0,40)}`, c.case_number)));

    $('cases-grid').innerHTML=`<div class="tbl-wrap"><table>
      <thead><tr><th>Case #</th><th>Title</th><th>Court</th><th>Jurisdiction</th><th>Status</th><th>Docs</th><th>Filings</th><th>Emails</th></tr></thead>
      <tbody>${casesList.map(c=>`<tr class="clickable"
        onclick="$('doc-case').value='${c.case_number}';switchTab('documents')">
        <td><strong>${c.case_number}</strong></td>
        <td>${c.case_title||''}</td>
        <td>${c.court||''}</td>
        <td><span class="badge badge-${c.jurisdiction==='federal'?'purple':'blue'}">${c.jurisdiction||''}</span></td>
        <td><span class="badge badge-${c.status==='active'?'green':'amber'}">${c.status||''}</span></td>
        <td><strong>${c.doc_count}</strong></td>
        <td>${c.filing_count}</td>
        <td>${c.email_count}</td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch { $('cases-grid').innerHTML='<div class="empty-state">Failed to load cases</div>'; }
}

// ═══════════════════════════════════════════════════════════
//  TAB: SEARCH
// ═══════════════════════════════════════════════════════════
async function doSearch(){
  const q=$('search-q').value.trim(); if(!q) return;
  const domain=$('search-domain').value;
  $('search-results').innerHTML='<div class="empty-state"><span class="spinner"></span> Searching…</div>';
  try {
    const data=await api('/api/search',{method:'POST',
      body:JSON.stringify({query:q, domain:domain||undefined, limit:30})});
    if(!data.results.length){
      $('search-results').innerHTML='<div class="empty-state">No results found</div>'; return;
    }
    $('search-results').innerHTML=`
      <div style="color:var(--muted);font-size:.8rem;margin-bottom:12px">
        ${data.result_count} results in ${data.duration_ms}ms</div>
      <div class="tbl-wrap"><table>
      <thead><tr><th>#</th><th>Domain</th><th>Type</th><th>Title</th><th>Rank</th><th>Headline</th></tr></thead>
      <tbody>${data.results.map((r,i)=>`<tr class="clickable" onclick="openDoc('${r.document_id}')">
        <td>${i+1}</td>
        <td><span class="badge badge-blue">${r.domain}</span></td>
        <td><span class="badge badge-cyan">${r.document_type||''}</span></td>
        <td>${r.title||r.filename||''}</td>
        <td>${r.rank}</td>
        <td style="max-width:400px;white-space:normal;font-size:.8rem">${r.headline}</td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch { $('search-results').innerHTML='<div class="empty-state">Search failed</div>'; }
}

// ═══════════════════════════════════════════════════════════
//  TAB: INGESTION
// ═══════════════════════════════════════════════════════════
async function loadIngestLive(){
  try {
    const data=await api('/api/ingestion/live?lines=50');
    $('ingest-status').innerHTML = data.running
      ? '<span class="dot dot-green"></span><strong class="green">Ingestion Running</strong>'
      : '<span class="dot dot-amber"></span><strong class="amber">No Active Ingestion</strong>';
    $('ingest-log').textContent = data.log || '(no log output)';
    $('ingest-log').scrollTop = $('ingest-log').scrollHeight;
  } catch { $('ingest-log').textContent='Failed to load log'; }
}

async function loadIngestJobs(){
  try {
    const jobs=await api('/api/ingestion/jobs');
    if(!jobs.length){
      $('ingest-jobs').innerHTML='<div class="empty-state">No ingestion jobs recorded yet</div>'; return;
    }
    $('ingest-jobs').innerHTML=`<div class="tbl-wrap"><table>
      <thead><tr><th>ID</th><th>Domain</th><th>Source</th><th>Status</th><th>Docs</th><th>Chunks</th><th>Failed</th><th>Started</th><th>Completed</th></tr></thead>
      <tbody>${jobs.map(j=>`<tr>
        <td>${j.id}</td>
        <td><span class="badge badge-blue">${j.domain||''}</span></td>
        <td>${j.source||''}</td>
        <td><span class="badge badge-${j.status==='completed'?'green':j.status==='running'?'amber':'red'}">${j.status||''}</span></td>
        <td>${j.documents_processed||0}</td>
        <td>${j.chunks_processed||0}</td>
        <td>${j.documents_failed||0}</td>
        <td>${fmtTime(j.started_at)}</td>
        <td>${fmtTime(j.completed_at)}</td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch { $('ingest-jobs').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

function toggleIngestAuto(){
  if($('ingest-auto').checked){
    ingestTimer = setInterval(loadIngestLive, 5000);
  } else {
    clearInterval(ingestTimer); ingestTimer=null;
  }
}

// ═══════════════════════════════════════════════════════════
//  TAB: AUDIT
// ═══════════════════════════════════════════════════════════
async function loadAudit(){
  try {
    const [access, searches, mcpLog] = await Promise.all([
      api('/api/audit/access?limit=100'),
      api('/api/audit/searches?limit=50'),
      api('/api/mcp/query-log?limit=100')
    ]);

    // MCP Client Activity (Athena AI / Claude tool calls)
    $('audit-mcp').innerHTML = mcpLog.length
      ? `<div class="tbl-wrap"><table class="tbl">
        <thead><tr><th>Time</th><th>Client</th><th>Tool</th><th>Arguments</th><th>Results</th><th>Duration</th><th>Status</th></tr></thead>
        <tbody>${mcpLog.map(m=>{
          const args = m.arguments ? JSON.stringify(m.arguments) : '';
          const shortArgs = args.length > 60 ? args.substring(0,60)+'…' : args;
          const status = m.error_message
            ? '<span class="red" title="'+esc(m.error_message)+'">✗ Error</span>'
            : '<span class="green">✓</span>';
          return `<tr class="qd-row" onclick="openQueryDetail(${m.id})" title="Click for full details">
            <td style="white-space:nowrap">${fmtTime(m.called_at||m.created_at)}</td>
            <td>${esc(m.client_name||m.client_id)}</td>
            <td><span class="badge badge-purple">${esc(m.tool_name)}</span></td>
            <td title="${esc(args)}" style="max-width:250px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.8rem">${esc(shortArgs)}</td>
            <td>${m.result_count!=null ? m.result_count : '—'}</td>
            <td>${m.duration_ms!=null ? m.duration_ms+'ms' : '—'}</td>
            <td>${status}</td>
          </tr>`;
        }).join('')}</tbody></table></div>`
      : '<div class="empty-state">No MCP client queries logged yet — make a query from Athena AI or Claude Desktop</div>';

    $('audit-access').innerHTML = access.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Time</th><th>Agent</th><th>Tool</th><th>Action</th><th>Domain</th><th>Query</th><th>Results</th><th>Duration</th></tr></thead>
        <tbody>${access.map(a=>`<tr>
          <td>${fmtTime(a.timestamp)}</td>
          <td>${a.agent_id||''}</td>
          <td><span class="badge badge-purple">${a.tool_name||''}</span></td>
          <td>${a.action||''}</td>
          <td>${a.domain||''}</td>
          <td title="${esc(a.query||'')}">${(a.query||'').substring(0,40)}</td>
          <td>${a.result_count!=null?a.result_count:'—'}</td>
          <td>${a.duration_ms?Math.round(a.duration_ms)+'ms':'—'}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No access log entries yet</div>';

    $('audit-searches').innerHTML = searches.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Time</th><th>Query</th><th>Type</th><th>Domain</th><th>Results</th><th>Top Score</th><th>Duration</th><th>Agent</th></tr></thead>
        <tbody>${searches.map(s=>`<tr>
          <td>${fmtTime(s.timestamp)}</td>
          <td title="${esc(s.query_text||'')}">${(s.query_text||'').substring(0,50)}</td>
          <td><span class="badge badge-cyan">${s.search_type||''}</span></td>
          <td>${s.domain||'all'}</td>
          <td>${s.result_count}</td>
          <td>${s.top_score?Number(s.top_score).toFixed(3):''}</td>
          <td>${s.duration_ms?Math.round(s.duration_ms)+'ms':''}</td>
          <td>${s.agent_id||''}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No search analytics yet</div>';
  } catch { $('audit-access').innerHTML='<div class="empty-state">Failed to load audit data</div>'; }
}

// ═══════════════════════════════════════════════════════════
//  TAB: AGENTS
// ═══════════════════════════════════════════════════════════
async function loadAgents(){
  try {
    const [agents, runs, findings] = await Promise.all([
      api('/api/agents'),
      api('/api/agents/runs?limit=30'),
      api('/api/agents/findings?status=' + ($('agent-finding-filter')?.value || 'open') + 
          '&severity=' + ($('agent-severity-filter')?.value || '') + '&limit=50')
    ]);

    // ── Fleet table ──
    $('agents-fleet').innerHTML = agents.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Agent</th><th>P</th><th>Status</th><th>Schedule</th><th>Last Run</th><th>Runs</th><th>Errors</th><th>Actions</th></tr></thead>
        <tbody>${agents.sort((a,b)=>(a.priority||99)-(b.priority||99)).map(a=>{
          const errRate = a.run_count > 0 ? ((a.error_count||0)/a.run_count*100).toFixed(0) : 0;
          const statusDot = a.is_active 
            ? (a.last_status==='error'?'dot-yellow':'dot-green')
            : 'dot-red';
          const statusText = a.is_active 
            ? (a.last_status==='error'?'Error':'Active')
            : 'Disabled';
          return `<tr style="cursor:pointer" onclick="showAgentDetail('${a.id}')">
            <td><strong style="color:var(--cyan)">${a.name||a.id}</strong><br><span style="font-size:.7rem;color:var(--muted)">${a.id} v${a.version||'?'}</span></td>
            <td style="text-align:center">${a.priority??'-'}</td>
            <td><span class="dot ${statusDot}"></span>${statusText}</td>
            <td style="font-family:monospace;font-size:.75rem">${a.schedule||'-'}</td>
            <td>${fmtTime(a.last_run_at)}</td>
            <td>${a.run_count||0}</td>
            <td>${(a.error_count||0)}${errRate>20?' <span style="color:var(--danger);font-size:.7rem">('+errRate+'%)</span>':''}</td>
            <td>
              <button class="btn btn-outline btn-sm" style="font-size:.7rem;padding:2px 6px" onclick="event.stopPropagation();triggerAgent('${a.id}')">▶ Run</button>
              <button class="btn btn-outline btn-sm" style="font-size:.7rem;padding:2px 6px" onclick="event.stopPropagation();toggleAgent('${a.id}')">${a.is_active?'⏸':'▶'}</button>
            </td>
          </tr>`}).join('')}</tbody></table></div>`
      : '<div class="empty-state">No agents registered. Deploy the agent system to get started.</div>';

    // ── Findings ──
    const sevColors = {critical:'var(--danger)',warning:'#f59e0b',info:'var(--accent)'};
    const sevIcons = {critical:'🔴',warning:'🟡',info:'🔵'};
    const openCount = findings.filter(f=>f.status==='open').length;
    $('findings-badge').textContent = openCount ? `(${openCount} open)` : '';
    
    $('agents-findings').innerHTML = findings.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Sev</th><th>Agent</th><th>Category</th><th>Title</th><th>Time</th><th>Status</th><th></th></tr></thead>
        <tbody>${findings.map(f=>`<tr style="cursor:pointer" onclick="showFinding(${JSON.stringify(JSON.stringify(f))})">
          <td>${sevIcons[f.severity]||'⚪'}</td>
          <td style="font-size:.8rem">${f.agent_id}</td>
          <td><span class="badge badge-purple">${f.category||'-'}</span></td>
          <td>${f.title||'-'}</td>
          <td style="font-size:.8rem">${fmtTime(f.created_at)}</td>
          <td>${f.status==='open'?'<span class="badge badge-yellow">Open</span>':'<span class="badge badge-green">Resolved</span>'}</td>
          <td>${f.status==='open'?'<button class="btn btn-outline btn-sm" style="font-size:.65rem;padding:1px 5px" onclick="event.stopPropagation();resolveFinding('+f.id+')">✓</button>':''}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No findings yet</div>';

    // ── Runs ──
    $('agents-runs').innerHTML = runs.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Agent</th><th>Status</th><th>Started</th><th>Duration</th><th>Summary</th></tr></thead>
        <tbody>${runs.map(r=>{
          const dur = r.duration_ms ? (r.duration_ms/1000).toFixed(1)+'s' : '-';
          const sc = r.status==='completed'?'dot-green':r.status==='error'?'dot-red':'dot-yellow';
          return `<tr>
            <td style="font-size:.85rem">${r.agent_id}</td>
            <td><span class="dot ${sc}"></span>${r.status}</td>
            <td>${fmtTime(r.started_at)}</td>
            <td style="font-family:monospace;font-size:.8rem">${dur}</td>
            <td style="font-size:.8rem;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.summary||r.error_message||'-'}</td>
          </tr>`}).join('')}</tbody></table></div>`
      : '<div class="empty-state">No runs yet</div>';

  } catch(e) { $('agents-fleet').innerHTML='<div class="empty-state">Failed to load agents: '+e.message+'</div>'; }
}

function loadAgentFindings(){ loadAgents(); }

async function triggerAgent(id){
  if(!confirm('Run agent "'+id+'" now?')) return;
  try {
    await api('/api/agents/'+id+'/trigger','POST');
    showToast('Agent '+id+' triggered');
    setTimeout(loadAgents, 3000);
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

async function toggleAgent(id){
  try {
    const r = await api('/api/agents/'+id+'/toggle','POST');
    showToast('Agent '+id+': '+(r.is_active?'Enabled':'Disabled'));
    loadAgents();
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

async function resolveFinding(id){
  try {
    await api('/api/agents/findings/'+id+'/resolve','POST');
    showToast('Finding resolved');
    loadAgents();
  } catch(e){ showToast('Failed: '+e.message,'error'); }
}

function showFinding(jsonStr){
  const f = JSON.parse(jsonStr);
  const sevColors = {critical:'#ef4444',warning:'#f59e0b',info:'#3b82f6'};
  let detailsHtml = '';
  if(f.details){
    try {
      const d = typeof f.details === 'string' ? JSON.parse(f.details) : f.details;
      detailsHtml = '<pre style="background:#1a1a2e;padding:12px;border-radius:6px;overflow:auto;max-height:300px;font-size:.75rem;color:#e0e0e0">'+JSON.stringify(d,null,2)+'</pre>';
    } catch{ detailsHtml = '<pre style="background:#1a1a2e;padding:12px;border-radius:6px;font-size:.75rem">'+f.details+'</pre>'; }
  }
  const m = document.createElement('div');
  m.className = 'qd-modal';
  m.onclick = e=>{ if(e.target===m) m.remove(); };
  m.innerHTML = `<div class="qd-content" style="max-width:700px">
    <div class="qd-header">
      <h3 style="color:${sevColors[f.severity]||'#888'}">${f.severity?.toUpperCase()||'INFO'} — ${f.title||'Finding'}</h3>
      <button class="qd-close" onclick="this.closest('.qd-modal').remove()">&times;</button>
    </div>
    <div class="qd-grid">
      <div class="qd-card"><label>Agent</label><div>${f.agent_id}</div></div>
      <div class="qd-card"><label>Category</label><div>${f.category||'-'}</div></div>
      <div class="qd-card"><label>Status</label><div>${f.status}</div></div>
      <div class="qd-card"><label>Created</label><div>${fmtTime(f.created_at)}</div></div>
    </div>
    <div style="margin-top:12px"><label style="font-size:.75rem;color:var(--muted)">Description</label><p style="white-space:pre-wrap;font-size:.85rem">${f.description||'-'}</p></div>
    ${detailsHtml ? '<div style="margin-top:12px"><label style="font-size:.75rem;color:var(--muted)">Details</label>'+detailsHtml+'</div>' : ''}
    ${f.status==='open'?'<div style="margin-top:16px;text-align:right"><button class="btn btn-primary btn-sm" onclick="resolveFinding('+f.id+');this.closest(\'.qd-modal\').remove()">✓ Resolve</button></div>':''}
  </div>`;
  document.body.appendChild(m);
}

// ═══════════════════════════════════════════════════════════
//  AGENT DETAIL MODAL
// ═══════════════════════════════════════════════════════════

async function showAgentDetail(agentId){
  // Show loading modal immediately
  const m = document.createElement('div');
  m.className = 'qd-modal';
  m.onclick = e=>{ if(e.target===m) m.remove(); };
  m.innerHTML = `<div class="qd-content" style="max-width:900px;max-height:90vh;overflow:auto">
    <div class="qd-header"><h3>Loading agent…</h3>
      <button class="qd-close" onclick="this.closest('.qd-modal').remove()">&times;</button>
    </div>
    <div class="empty-state"><span class="spinner"></span></div>
  </div>`;
  document.body.appendChild(m);

  try {
    const d = await api('/api/agents/'+agentId+'/detail');
    const a = d.agent;
    const statusDot = a.is_active ? (a.last_status==='error'?'dot-yellow':'dot-green') : 'dot-red';
    const statusText = a.is_active ? (a.last_status==='error'?'Error':'Active') : 'Disabled';
    const errRate = a.run_count > 0 ? ((a.error_count||0)/a.run_count*100).toFixed(0) : 0;
    const sevIcons = {critical:'🔴',warning:'🟡',info:'🔵'};

    let html = `<div class="qd-content" style="max-width:900px;max-height:90vh;overflow:auto">
      <div class="qd-header">
        <div>
          <h3 style="margin:0"><span class="dot ${statusDot}"></span> ${a.name} <span style="font-size:.7rem;color:var(--muted);font-weight:400">v${a.version||'?'}</span></h3>
          <div style="font-size:.78rem;color:var(--muted);margin-top:2px">${a.id} · ${a.agent_type} · priority ${a.priority}</div>
        </div>
        <button class="qd-close" onclick="this.closest('.qd-modal').remove()">&times;</button>
      </div>

      <!-- Info Cards -->
      <div class="qd-grid" style="margin-top:12px">
        <div class="qd-card"><label>Status</label><div><span class="dot ${statusDot}"></span>${statusText}</div></div>
        <div class="qd-card"><label>Schedule</label><div style="font-family:monospace;font-size:.85rem">${a.schedule||'manual'}</div></div>
        <div class="qd-card"><label>Runs</label><div>${a.run_count||0}</div></div>
        <div class="qd-card"><label>Errors</label><div>${a.error_count||0}${errRate>10?' <span style="color:var(--danger);font-size:.75rem">('+errRate+'%)</span>':''}</div></div>
        <div class="qd-card"><label>Last Run</label><div style="font-size:.8rem">${fmtTime(a.last_run_at)}</div></div>
        <div class="qd-card"><label>Created</label><div style="font-size:.8rem">${fmtTime(a.created_at)}</div></div>
      </div>

      <!-- Description -->
      <div style="margin-top:12px;padding:10px 14px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)">
        <div style="font-size:.8rem;color:var(--muted);margin-bottom:4px">Description</div>
        <div style="font-size:.88rem">${esc(a.description||'No description')}</div>
      </div>

      <!-- Capabilities -->
      <div style="margin-top:10px;display:flex;gap:6px;flex-wrap:wrap">
        ${(a.capabilities||[]).map(c=>'<span class="badge badge-blue" style="font-size:.72rem">'+esc(c)+'</span>').join('')}
      </div>`;

    // ── Tab navigation within modal ──
    html += `<div style="margin-top:16px;display:flex;gap:4px;border-bottom:1px solid var(--border);padding-bottom:0">
      <button class="agent-tab active" onclick="switchAgentTab(this,'agent-runs-section')">📋 Runs (${d.runs.length})</button>
      <button class="agent-tab" onclick="switchAgentTab(this,'agent-findings-section')">🚨 Findings (${d.findings.length})</button>
      <button class="agent-tab" onclick="switchAgentTab(this,'agent-messages-section')">💬 Messages (${d.messages.length})</button>
      <button class="agent-tab" onclick="switchAgentTab(this,'agent-memory-section')">🧠 Memory (${d.memory.length})</button>
    </div>`;

    // ── Runs section ──
    html += `<div id="agent-runs-section" class="agent-tab-panel">`;
    if(d.runs.length){
      html += `<table class="tbl" style="font-size:.82rem"><thead><tr><th>Status</th><th>Trigger</th><th>Started</th><th>Duration</th><th>Findings</th><th>Actions</th><th>Summary / Error</th></tr></thead><tbody>`;
      for(const r of d.runs){
        const dur = r.duration_ms ? (r.duration_ms/1000).toFixed(1)+'s' : '-';
        const sc = r.status==='completed'?'dot-green':r.status==='error'?'dot-red':'dot-yellow';
        const txt = r.summary||r.error_message||'-';
        html += `<tr>
          <td><span class="dot ${sc}"></span>${r.status}</td>
          <td><span class="badge badge-outline" style="font-size:.7rem">${r.trigger||'schedule'}</span></td>
          <td>${fmtTime(r.started_at)}</td>
          <td style="font-family:monospace">${dur}</td>
          <td style="text-align:center">${r.findings||0}</td>
          <td style="text-align:center">${r.actions||0}</td>
          <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(txt)}">${esc(txt)}</td>
        </tr>`;
      }
      html += `</tbody></table>`;
    } else {
      html += '<div class="empty-state" style="padding:20px">No runs recorded yet</div>';
    }
    html += `</div>`;

    // ── Findings section ──
    html += `<div id="agent-findings-section" class="agent-tab-panel" style="display:none">`;
    if(d.findings.length){
      html += `<table class="tbl" style="font-size:.82rem"><thead><tr><th>Sev</th><th>Category</th><th>Title</th><th>Status</th><th>Time</th></tr></thead><tbody>`;
      for(const f of d.findings){
        html += `<tr style="cursor:pointer" onclick="showFinding(${JSON.stringify(JSON.stringify(f))})">
          <td>${sevIcons[f.severity]||'⚪'}</td>
          <td><span class="badge badge-purple">${esc(f.category||'-')}</span></td>
          <td>${esc(f.title||'-')}</td>
          <td>${f.status==='open'?'<span class="badge badge-yellow">Open</span>':'<span class="badge badge-green">Resolved</span>'}</td>
          <td>${fmtTime(f.created_at)}</td>
        </tr>`;
      }
      html += `</tbody></table>`;
    } else {
      html += '<div class="empty-state" style="padding:20px">No findings from this agent</div>';
    }
    html += `</div>`;

    // ── Messages section ──
    html += `<div id="agent-messages-section" class="agent-tab-panel" style="display:none">`;
    if(d.messages.length){
      html += `<div style="display:flex;flex-direction:column;gap:8px;padding:8px 0">`;
      for(const msg of d.messages){
        const isSent = msg.from_agent === agentId;
        const arrow = isSent ? '→' : '←';
        const other = isSent ? msg.to_agent : msg.from_agent;
        const dir = isSent ? 'Sent to' : 'Received from';
        const bgColor = isSent ? 'rgba(59,130,246,.08)' : 'rgba(168,85,247,.08)';
        const borderColor = isSent ? 'rgba(59,130,246,.2)' : 'rgba(168,85,247,.2)';
        let bodyTxt = '';
        if(msg.body){
          try{ bodyTxt = typeof msg.body==='string' ? msg.body : JSON.stringify(msg.body,null,2); }
          catch{ bodyTxt = String(msg.body); }
        }
        html += `<div style="background:${bgColor};border:1px solid ${borderColor};border-radius:var(--radius);padding:10px 14px">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
            <div><strong>${dir}</strong> <span style="color:var(--cyan)">${esc(other)}</span>
              <span class="badge badge-outline" style="font-size:.68rem;margin-left:6px">${esc(msg.msg_type)}</span>
              <span class="badge badge-${msg.status==='processed'?'green':'yellow'}" style="font-size:.68rem;margin-left:4px">${msg.status}</span>
            </div>
            <span style="font-size:.75rem;color:var(--muted)">${fmtTime(msg.created_at)}</span>
          </div>
          ${msg.subject?'<div style="font-weight:600;font-size:.85rem;margin-bottom:4px">'+esc(msg.subject)+'</div>':''}
          ${bodyTxt?'<pre style="background:var(--bg);padding:8px;border-radius:4px;font-size:.75rem;max-height:150px;overflow:auto;margin:0;white-space:pre-wrap">'+esc(bodyTxt)+'</pre>':''}
        </div>`;
      }
      html += `</div>`;
    } else {
      html += '<div class="empty-state" style="padding:20px">No inter-agent messages</div>';
    }
    html += `</div>`;

    // ── Memory section ──
    html += `<div id="agent-memory-section" class="agent-tab-panel" style="display:none">`;
    if(d.memory.length){
      html += `<table class="tbl" style="font-size:.82rem"><thead><tr><th>Key</th><th>Value</th><th>Updated</th></tr></thead><tbody>`;
      for(const mem of d.memory){
        let valStr;
        try{ valStr = typeof mem.value==='string' ? mem.value : JSON.stringify(mem.value,null,2); }
        catch{ valStr = String(mem.value); }
        const shortVal = valStr.length > 120 ? valStr.substring(0,120)+'…' : valStr;
        html += `<tr>
          <td style="font-family:monospace;font-weight:600;white-space:nowrap">${esc(mem.key)}</td>
          <td><pre style="margin:0;font-size:.75rem;max-height:80px;overflow:auto;background:var(--surface);padding:4px 8px;border-radius:4px;white-space:pre-wrap">${esc(valStr)}</pre></td>
          <td style="font-size:.78rem">${fmtTime(mem.updated_at)}</td>
        </tr>`;
      }
      html += `</tbody></table>`;
    } else {
      html += '<div class="empty-state" style="padding:20px">No persistent memory stored</div>';
    }
    html += `</div>`;

    // ── Action buttons ──
    html += `<div style="margin-top:16px;display:flex;gap:8px;justify-content:flex-end">
      <button class="btn btn-outline btn-sm" onclick="triggerAgent('${a.id}');this.closest('.qd-modal').remove()">▶ Run Now</button>
      <button class="btn btn-outline btn-sm" onclick="toggleAgent('${a.id}');this.closest('.qd-modal').remove()">${a.is_active?'⏸ Disable':'▶ Enable'}</button>
    </div>`;

    html += `</div>`;
    m.innerHTML = html;

  } catch(e) {
    m.querySelector('.qd-content').innerHTML = `
      <div class="qd-header"><h3>Error</h3>
        <button class="qd-close" onclick="this.closest('.qd-modal').remove()">&times;</button>
      </div>
      <div class="empty-state">Failed to load agent: ${esc(e.message)}</div>`;
  }
}

function switchAgentTab(btn, panelId){
  // Toggle active on buttons
  btn.parentElement.querySelectorAll('.agent-tab').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  // Toggle panels
  btn.closest('.qd-content').querySelectorAll('.agent-tab-panel').forEach(p=>p.style.display='none');
  document.getElementById(panelId).style.display='block';
}

//  TAB: DATABASE
// ═══════════════════════════════════════════════════════════
async function loadDbHealth(){
  $('db-info').innerHTML='<div class="empty-state"><span class="spinner"></span> Loading…</div>';
  try {
    const d = await api('/api/db-health');
    let h = `<div class="cards">
      <div class="card"><div class="label">Database Size</div>
        <div class="value cyan">${d.database_size}</div></div>
      <div class="card"><div class="label">Uptime</div>
        <div class="value green">${(d.uptime||'').split('.')[0]}</div></div>
    </div>
    <div style="font-size:.82rem;color:var(--muted);margin-bottom:16px">${d.version||''}</div>`;

    h += `<div class="section-title">📋 Table Statistics</div>
      <div class="tbl-wrap"><table>
      <thead><tr><th>Schema</th><th>Table</th><th>Rows</th><th>Size</th></tr></thead>
      <tbody>${d.tables.map(t=>`<tr>
        <td><span class="badge badge-blue">${t.schema}</span></td>
        <td>${t.table}</td>
        <td><strong>${fmtNum(t.rows)}</strong></td>
        <td>${t.size}</td>
      </tr>`).join('')}</tbody></table></div>`;
    $('db-info').innerHTML = h;
  } catch { $('db-info').innerHTML='<div class="empty-state">Failed to load database info</div>'; }
}

// ═══════════════════════════════════════════════════════════
//  TAB: EMAIL SYNC
// ═══════════════════════════════════════════════════════════
async function loadEmail(){
  await Promise.all([loadEmailStats(), loadMailboxes(), loadRules(), loadSyncRuns(), loadClassifications()]);
  // Also load new chat-DB email integration
  await Promise.all([loadCemailStats(), loadCemailAccounts(), loadCemailWebhooks(), loadCemailSyncRuns(), loadCemailAction(), loadCemailMessages()]);
}

async function loadEmailStats(){
  try {
    const [mbs, rules, runs, cls] = await Promise.all([
      api('/api/email/mailboxes'), api('/api/email/rules'),
      api('/api/email/runs?limit=5'), api('/api/email/classifications?limit=1'),
    ]);
    const activeMb = mbs.filter(m=>m.is_active).length;
    const activeRules = rules.filter(r=>r.is_active).length;
    const lastRun = runs.length ? runs[0] : null;
    $('email-stats').innerHTML = `
      <div class="card"><div class="label">Mailboxes</div>
        <div class="value blue">${mbs.length}</div>
        <div class="sub">${activeMb} active</div></div>
      <div class="card"><div class="label">Sync Rules</div>
        <div class="value cyan">${rules.length}</div>
        <div class="sub">${activeRules} active</div></div>
      <div class="card"><div class="label">Last Sync</div>
        <div class="value green">${lastRun ? lastRun.emails_ingested : '—'}</div>
        <div class="sub">${lastRun ? fmtTime(lastRun.started_at) : 'never'}</div></div>
      <div class="card"><div class="label">AI Classified</div>
        <div class="value purple">${fmtNum(cls.total||0)}</div></div>`;
  } catch(e){ console.error(e); $('email-stats').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function loadMailboxes(){
  try {
    const mbs = await api('/api/email/mailboxes');
    if(!mbs.length){ $('email-mailboxes').innerHTML='<div class="empty-state">No mailboxes configured</div>'; return; }
    $('email-mailboxes').innerHTML = `<div class="tbl-wrap"><table>
      <thead><tr><th>Email</th><th>Name</th><th>Active</th><th>Last Sync</th><th>Total Synced</th><th>Actions</th></tr></thead>
      <tbody>${mbs.map(m=>`<tr>
        <td><strong>${esc(m.email)}</strong></td>
        <td>${esc(m.display_name||'')}</td>
        <td>${m.is_active?'<span class="dot dot-green"></span>Active':'<span class="dot dot-red"></span>Disabled'}</td>
        <td>${fmtTime(m.last_sync_at)}</td>
        <td>${m.total_synced||0}</td>
        <td>
          <button class="btn btn-outline btn-sm" onclick="toggleMailbox(${m.id},${!m.is_active})">${m.is_active?'Disable':'Enable'}</button>
          <button class="btn btn-outline btn-sm" style="color:var(--red)" onclick="deleteMailbox(${m.id})">✕</button>
        </td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch{ $('email-mailboxes').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function addMailbox(){
  const email=$('mb-email').value.trim(), name=$('mb-name').value.trim();
  if(!email) return;
  await api('/api/email/mailboxes',{method:'POST',body:JSON.stringify({email,display_name:name})});
  $('mb-email').value=''; $('mb-name').value='';
  $('email-mb-form').classList.add('hidden');
  loadMailboxes(); loadEmailStats();
}

async function toggleMailbox(id,active){
  await api(`/api/email/mailboxes/${id}`,{method:'PUT',body:JSON.stringify({is_active:active})});
  loadMailboxes();
}

async function deleteMailbox(id){
  if(!confirm('Delete this mailbox?')) return;
  await api(`/api/email/mailboxes/${id}`,{method:'DELETE'});
  loadMailboxes(); loadEmailStats();
}

async function loadRules(){
  try {
    const rules = await api('/api/email/rules');
    if(!rules.length){ $('email-rules').innerHTML='<div class="empty-state">No sync rules configured — add domains or email addresses to monitor</div>'; return; }
    const tb=t=>({domain:'blue',email_address:'cyan',keyword:'purple',subject_pattern:'amber'}[t]||'blue');
    $('email-rules').innerHTML = `<div class="tbl-wrap"><table>
      <thead><tr><th>Name</th><th>Type</th><th>Pattern</th><th>Case Numbers</th><th>Active</th><th>Priority</th><th>Actions</th></tr></thead>
      <tbody>${rules.map(r=>`<tr>
        <td><strong>${esc(r.name)}</strong>${r.description?`<div style="font-size:.72rem;color:var(--muted)">${esc(r.description)}</div>`:''}</td>
        <td><span class="badge badge-${tb(r.rule_type)}">${r.rule_type}</span></td>
        <td><code style="background:var(--bg);padding:2px 6px;border-radius:4px;font-size:.82rem">${esc(r.pattern)}</code></td>
        <td>${(r.case_numbers||[]).map(c=>`<span class="badge badge-purple">${c}</span>`).join(' ')||'<span style="color:var(--muted)">any</span>'}</td>
        <td>${r.is_active?'<span class="dot dot-green"></span>On':'<span class="dot dot-red"></span>Off'}</td>
        <td>${r.priority}</td>
        <td>
          <button class="btn btn-outline btn-sm" onclick="toggleRule(${r.id},${!r.is_active})">${r.is_active?'Disable':'Enable'}</button>
          <button class="btn btn-outline btn-sm" style="color:var(--red)" onclick="deleteRule(${r.id})">✕</button>
        </td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch{ $('email-rules').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function addRule(){
  const name=$('rule-name').value.trim(), rt=$('rule-type').value,
    pattern=$('rule-pattern').value.trim(), cases=$('rule-cases').value.trim(),
    desc=$('rule-desc').value.trim();
  if(!name||!pattern){alert('Name and pattern are required');return;}
  await api('/api/email/rules',{method:'POST',body:JSON.stringify({
    name,rule_type:rt,pattern,
    case_numbers:cases?cases.split(',').map(c=>c.trim()):[],
    description:desc})});
  ['rule-name','rule-pattern','rule-cases','rule-desc'].forEach(id=>$(id).value='');
  $('email-rule-form').classList.add('hidden');
  loadRules(); loadEmailStats();
}

async function toggleRule(id,active){
  await api(`/api/email/rules/${id}`,{method:'PUT',body:JSON.stringify({is_active:active})});
  loadRules();
}

async function deleteRule(id){
  if(!confirm('Delete this sync rule?')) return;
  await api(`/api/email/rules/${id}`,{method:'DELETE'});
  loadRules(); loadEmailStats();
}

async function loadSyncRuns(){
  try {
    const runs = await api('/api/email/runs');
    if(!runs.length){ $('email-runs').innerHTML='<div class="empty-state">No sync runs yet — click "Run Email Sync" to start</div>'; return; }
    $('email-runs').innerHTML = `<div class="tbl-wrap"><table>
      <thead><tr><th>ID</th><th>Started</th><th>Status</th><th>Trigger</th><th>Found</th><th>Ingested</th><th>Skipped</th><th>Failed</th><th>Chunks</th><th>AI</th><th>Duration</th></tr></thead>
      <tbody>${runs.map(r=>`<tr>
        <td>${r.id}</td>
        <td>${fmtTime(r.started_at)}</td>
        <td><span class="badge badge-${r.status==='completed'?'green':r.status==='running'?'amber':'red'}">${r.status}</span></td>
        <td><span class="badge badge-blue">${r.trigger_type}</span></td>
        <td>${r.emails_found}</td>
        <td><strong>${r.emails_ingested}</strong></td>
        <td>${r.emails_skipped}</td>
        <td>${r.emails_failed}</td>
        <td>${r.chunks_created}</td>
        <td>${r.ai_classified||0}</td>
        <td>${r.duration_seconds?r.duration_seconds.toFixed(1)+'s':'—'}</td>
      </tr>`).join('')}</tbody></table></div>`;
  } catch{ $('email-runs').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function triggerSync(){
  try {
    $('sync-btn').disabled=true;
    $('sync-indicator').innerHTML='<span class="spinner"></span> Starting sync...';
    const r=await api('/api/email/sync',{method:'POST'});
    if(r.error){$('sync-indicator').innerHTML=`<span class="red">${r.error}</span>`;return;}
    $('sync-indicator').innerHTML='<span class="green">✓ Sync started — refreshing in 3s</span>';
    setTimeout(()=>{loadSyncLive();loadSyncRuns();loadEmailStats();},3000);
  } catch(e){$('sync-indicator').innerHTML=`<span class="red">Error: ${e.message}</span>`;}
  finally{$('sync-btn').disabled=false;}
}

async function loadSyncLive(){
  try {
    const data=await api('/api/email/sync/live?lines=40');
    $('email-sync-log').textContent=data.log||'(no log output)';
    $('email-sync-log').scrollTop=$('email-sync-log').scrollHeight;
    if(data.running) $('sync-indicator').innerHTML='<span class="dot dot-green"></span><span class="green pulse">Sync running…</span>';
  } catch{$('email-sync-log').textContent='(failed to load log)';}
}

async function loadClassifications(){
  try {
    const data=await api('/api/email/classifications?limit=30');
    if(!data.classifications||!data.classifications.length){
      $('email-classifications').innerHTML='<div class="empty-state">No AI classifications yet. Click "Classify Unprocessed" to start.</div>';
      return;
    }
    const ui=u=>({critical:'🔴',high:'🟠',medium:'🟡',low:'🟢'}[u]||'⚪');
    $('email-classifications').innerHTML = `
      <div style="color:var(--muted);font-size:.8rem;margin-bottom:8px">${data.total} total classifications</div>
      <div class="tbl-wrap"><table>
      <thead><tr><th>Urgency</th><th>Subject</th><th>From</th><th>Cases</th><th>Categories</th><th>Summary</th><th>Conf.</th><th>When</th></tr></thead>
      <tbody>${data.classifications.map(c=>{
        const cl=c.classification||{};
        const cases=(cl.case_associations||[]).map(a=>`<span class="badge badge-purple">${a.case_number}</span>`).join(' ')||'—';
        const cats=(cl.categories||[]).slice(0,3).map(t=>`<span class="badge badge-cyan">${t}</span>`).join(' ');
        return `<tr class="clickable" onclick="openDoc('${c.document_id}')">
          <td>${ui(cl.urgency)} ${cl.urgency||'?'}</td>
          <td title="${esc(c.title||'')}">${(c.title||'').substring(0,45)}</td>
          <td>${esc((c.sender||'').substring(0,25))}</td>
          <td>${cases}</td>
          <td>${cats}</td>
          <td title="${esc(cl.summary||'')}" style="max-width:220px;white-space:normal;font-size:.78rem;line-height:1.3">${(cl.summary||'').substring(0,100)}</td>
          <td>${c.confidence?c.confidence.toFixed(2):''}</td>
          <td>${fmtTime(c.processed_at)}</td>
        </tr>`;
      }).join('')}</tbody></table></div>`;
  } catch(e){ console.error(e); $('email-classifications').innerHTML='<div class="empty-state">Failed to load</div>'; }
}

async function triggerClassify(reclassify){
  try {
    $('classify-indicator').innerHTML='<span class="spinner"></span> Starting AI classification...';
    const r=await api('/api/email/classify',{method:'POST',
      body:JSON.stringify({limit:50,reclassify:!!reclassify})});
    if(r.error){$('classify-indicator').textContent='Error: '+r.error;return;}
    $('classify-indicator').innerHTML='<span class="green">✓ Classification started — refreshing in 8s</span>';
    setTimeout(loadClassifications,8000);
  } catch(e){$('classify-indicator').textContent='Error: '+e.message;}
}

// ── Chat-DB Email Integration JS ──
async function loadCemailStats(){
  try {
    const [accts,sync]=await Promise.all([api('/api/cemail/accounts'),api('/api/cemail/sync')]);
    const tot=Array.isArray(accts)?accts.reduce((s,a)=>s+(a.message_count||0),0):0;
    const emb=Array.isArray(accts)?accts.reduce((s,a)=>s+(a.embedding_count||0),0):0;
    const cls=Array.isArray(accts)?accts.reduce((s,a)=>s+(a.classified_count||0),0):0;
    const lastSync=sync?.last_sync||sync?.last_run||'—';
    $('cemail-stats').innerHTML=`
      <div class="card"><div class="card-value">${fmtNum(tot)}</div><div class="card-label">Synced Emails</div></div>
      <div class="card"><div class="card-value">${fmtNum(emb)}</div><div class="card-label">RAG Embeddings</div></div>
      <div class="card"><div class="card-value">${fmtNum(cls)}</div><div class="card-label">AI Classified</div></div>
      <div class="card"><div class="card-value">${Array.isArray(accts)?accts.length:0}</div><div class="card-label">Accounts</div></div>
      <div class="card"><div class="card-value" style="font-size:.85rem">${typeof lastSync==='string'&&lastSync!=='—'?fmtTime(lastSync):lastSync}</div><div class="card-label">Last Sync</div></div>`;
  } catch(e){$('cemail-stats').innerHTML=`<div class="empty-state red">Error: ${esc(e.message)}</div>`;}
}

async function loadCemailAccounts(){
  try {
    const accts=await api('/api/cemail/accounts');
    if(!Array.isArray(accts)||!accts.length){$('cemail-accounts').innerHTML='<div class="empty-state">No accounts configured</div>';return;}
    let h='<table><tr><th>Email</th><th>Display Name</th><th>Messages</th><th>Embeddings</th><th>Classified</th><th>Last Sync</th><th>Status</th></tr>';
    for(const a of accts){
      const st=a.is_active?'<span class="green">● Active</span>':'<span class="red">● Disabled</span>';
      h+=`<tr><td>${esc(a.email_address||'')}</td><td>${esc(a.display_name||'')}</td><td>${fmtNum(a.message_count||0)}</td><td>${fmtNum(a.embedding_count||0)}</td><td>${fmtNum(a.classified_count||0)}</td><td>${a.last_sync?fmtTime(a.last_sync):'Never'}</td><td>${st}</td></tr>`;
    }
    $('cemail-accounts').innerHTML=h+'</table>';
  } catch(e){$('cemail-accounts').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function loadCemailWebhooks(){
  try {
    const data=await api('/api/cemail/webhooks');
    const subs=data?.subscriptions||data||[];
    if(!Array.isArray(subs)||!subs.length){$('cemail-webhooks').innerHTML='<div class="empty-state">No webhook subscriptions</div>';return;}
    let h='<table><tr><th>Account</th><th>Resource</th><th>Expires</th><th>Status</th><th>Actions</th></tr>';
    for(const s of subs){
      const exp=s.expiration_datetime||s.expires||'';
      const isExp=exp&&new Date(exp)<new Date();
      const st=isExp?'<span class="red">Expired</span>':'<span class="green">Active</span>';
      h+=`<tr><td>${esc(s.email_address||s.account||'')}</td><td style="font-size:.78rem;max-width:200px;overflow:hidden;text-overflow:ellipsis">${esc(s.resource||'')}</td><td>${exp?fmtTime(exp):'—'}</td><td>${st}</td><td><button class="btn btn-outline btn-sm" onclick="deleteWebhook('${esc(s.subscription_id||s.id||'')}')">Delete</button></td></tr>`;
    }
    $('cemail-webhooks').innerHTML=h+'</table>';
  } catch(e){$('cemail-webhooks').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function createWebhooks(){
  try {
    $('webhook-indicator').innerHTML='<span class="spinner"></span> Registering...';
    const r=await api('/api/cemail/webhooks',{method:'POST',body:JSON.stringify({})});
    $('webhook-indicator').innerHTML='<span class="green">✓ Done</span>';
    setTimeout(()=>{$('webhook-indicator').textContent='';},3000);
    await loadCemailWebhooks();
  } catch(e){$('webhook-indicator').textContent='Error: '+e.message;}
}

async function renewWebhooks(){
  try {
    $('webhook-indicator').innerHTML='<span class="spinner"></span> Renewing...';
    const r=await api('/api/cemail/webhooks/renew',{method:'POST'});
    $('webhook-indicator').innerHTML='<span class="green">✓ Renewed</span>';
    setTimeout(()=>{$('webhook-indicator').textContent='';},3000);
    await loadCemailWebhooks();
  } catch(e){$('webhook-indicator').textContent='Error: '+e.message;}
}

async function deleteWebhook(id){
  if(!confirm('Delete this webhook subscription?'))return;
  try {
    await api('/api/cemail/webhooks/'+id,{method:'DELETE'});
    await loadCemailWebhooks();
  } catch(e){alert('Error: '+e.message);}
}

async function loadCemailSyncRuns(){
  try {
    const data=await api('/api/cemail/sync');
    const runs=data?.recent_runs||data?.runs||[];
    if(!Array.isArray(runs)||!runs.length){
      const info=data||{};
      $('cemail-sync-runs').innerHTML=`<div class="empty-state">Last sync: ${info.last_sync?fmtTime(info.last_sync):'Never'} · Total messages: ${fmtNum(info.total_messages||0)} · Status: ${esc(info.status||'Unknown')}</div>`;
      return;
    }
    let h='<table><tr><th>Time</th><th>Account</th><th>New</th><th>Embeddings</th><th>Classified</th><th>Duration</th><th>Status</th></tr>';
    for(const r of runs.slice(0,15)){
      h+=`<tr><td>${r.started_at?fmtTime(r.started_at):'—'}</td><td>${esc(r.account||r.email_address||'—')}</td><td>${r.new_messages||0}</td><td>${r.new_embeddings||0}</td><td>${r.classified||0}</td><td>${r.duration_seconds?r.duration_seconds.toFixed(1)+'s':'—'}</td><td>${r.status==='completed'?'<span class="green">✓</span>':'<span class="red">'+esc(r.status||'')+'</span>'}</td></tr>`;
    }
    $('cemail-sync-runs').innerHTML=h+'</table>';
  } catch(e){$('cemail-sync-runs').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function triggerCemailSync(){
  try {
    $('cemail-sync-indicator').innerHTML='<span class="spinner"></span> Triggering sync...';
    const r=await api('/api/cemail/sync/trigger',{method:'POST',body:JSON.stringify({})});
    $('cemail-sync-indicator').innerHTML='<span class="green">✓ Sync started</span>';
    setTimeout(async()=>{
      $('cemail-sync-indicator').textContent='';
      await Promise.all([loadCemailStats(),loadCemailSyncRuns(),loadCemailMessages()]);
    },10000);
  } catch(e){$('cemail-sync-indicator').textContent='Error: '+e.message;}
}

async function loadCemailAction(){
  try {
    const data=await api('/api/cemail/action-required');
    const items=data?.messages||data||[];
    if(!Array.isArray(items)||!items.length){$('cemail-action').innerHTML='<div class="empty-state" style="color:var(--green)">✓ No action-required emails</div>';return;}
    let h='<table><tr><th>From</th><th>Subject</th><th>Date</th><th>Category</th><th>Priority</th></tr>';
    for(const m of items.slice(0,20)){
      const pr=m.priority_score>=8?'🔴':m.priority_score>=5?'🟡':'🟢';
      h+=`<tr><td>${esc(m.from_address||m.sender||'')}</td><td style="max-width:300px;overflow:hidden;text-overflow:ellipsis">${esc(m.subject||'')}</td><td>${m.received_at?fmtTime(m.received_at):m.date?fmtTime(m.date):'—'}</td><td>${esc(m.category||'')}</td><td>${pr} ${m.priority_score||'—'}</td></tr>`;
    }
    $('cemail-action').innerHTML=h+'</table>';
  } catch(e){$('cemail-action').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function cemailSearch(){
  const q=$('cemail-search-q').value.trim();
  if(!q){$('cemail-search-results').innerHTML='<div class="empty-state">Enter a query</div>';return;}
  $('cemail-search-results').innerHTML='<div class="empty-state"><span class="spinner"></span> Searching...</div>';
  try {
    const data=await api('/api/cemail/semantic-search?q='+encodeURIComponent(q)+'&limit=10');
    const results=data?.results||data||[];
    if(!Array.isArray(results)||!results.length){$('cemail-search-results').innerHTML='<div class="empty-state">No results</div>';return;}
    let h='<table><tr><th>Score</th><th>From</th><th>Subject</th><th>Date</th><th>Snippet</th></tr>';
    for(const r of results){
      const score=r.similarity!==undefined?(r.similarity*100).toFixed(1)+'%':r.score!==undefined?(r.score*100).toFixed(1)+'%':'—';
      const snip=(r.chunk_text||r.snippet||r.body||'').substring(0,120);
      h+=`<tr><td>${score}</td><td>${esc(r.from_address||r.sender||'')}</td><td>${esc(r.subject||'')}</td><td>${r.received_at?fmtTime(r.received_at):'—'}</td><td style="font-size:.78rem;max-width:300px;overflow:hidden;text-overflow:ellipsis">${esc(snip)}</td></tr>`;
    }
    $('cemail-search-results').innerHTML=h+'</table>';
  } catch(e){$('cemail-search-results').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function loadCemailMessages(){
  try {
    const data=await api('/api/cemail/messages?limit=25');
    const msgs=data?.messages||data||[];
    if(!Array.isArray(msgs)||!msgs.length){$('cemail-messages').innerHTML='<div class="empty-state">No messages synced yet</div>';return;}
    let h='<table><tr><th>From</th><th>Subject</th><th>Date</th><th>Category</th><th>Priority</th><th>Has Embed</th></tr>';
    for(const m of msgs.slice(0,25)){
      const pr=m.priority_score>=8?'🔴':m.priority_score>=5?'🟡':'🟢';
      const cat=m.category||m.classification||'—';
      const emb=m.has_embeddings||m.embedding_count>0?'<span class="green">✓</span>':'<span class="red">✗</span>';
      h+=`<tr><td>${esc(m.from_address||m.sender||'')}</td><td style="max-width:300px;overflow:hidden;text-overflow:ellipsis">${esc(m.subject||'')}</td><td>${m.received_at?fmtTime(m.received_at):m.date?fmtTime(m.date):'—'}</td><td>${esc(cat)}</td><td>${pr} ${m.priority_score||'—'}</td><td>${emb}</td></tr>`;
    }
    $('cemail-messages').innerHTML=h+'</table>';
  } catch(e){$('cemail-messages').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

// ═══════════════════════════════════════════════════════════
//  TAB: ONEDRIVE
// ═══════════════════════════════════════════════════════════
let odFilesOffset=0;
async function loadOneDrive(){
  await Promise.all([loadOdStats(), loadOdTargets(), loadOdWebhooks(), loadOdRuns(), loadOdExtBreakdown(), loadOdFiles()]);
}

async function loadOdStats(){
  try {
    const d=await api('/api/onedrive/stats');
    const t=d.totals||{};
    const idx=t.indexed_files||0, pend=t.pending_files||0, fail=t.failed_files||0, skip=t.skipped_files||0;
    const chunks=d.total_chunks||0;
    const targets=Array.isArray(d.targets)?d.targets.length:0;
    const wh=d.webhook_count||0;
    const lastRun=d.recent_runs&&d.recent_runs.length?d.recent_runs[0].started_at:null;
    $('od-stats').innerHTML=`
      <div class="card"><div class="card-value">${fmtNum(idx)}</div><div class="card-label">Indexed Files</div></div>
      <div class="card"><div class="card-value">${fmtNum(chunks)}</div><div class="card-label">RAG Chunks</div></div>
      <div class="card"><div class="card-value">${esc(t.total_file_size||'0 bytes')}</div><div class="card-label">Total Size</div></div>
      <div class="card"><div class="card-value">${fmtNum(pend)}</div><div class="card-label">Pending</div></div>
      <div class="card"><div class="card-value">${fmtNum(fail)}</div><div class="card-label">Failed</div></div>
      <div class="card"><div class="card-value">${targets}</div><div class="card-label">Sync Targets</div></div>
      <div class="card"><div class="card-value">${wh}</div><div class="card-label">Webhooks</div></div>
      <div class="card"><div class="card-value" style="font-size:.85rem">${lastRun?fmtTime(lastRun):'Never'}</div><div class="card-label">Last Sync</div></div>`;
  } catch(e){$('od-stats').innerHTML=`<div class="empty-state red">Error: ${esc(e.message)}</div>`;}
}

async function loadOdTargets(){
  try {
    const targets=await api('/api/onedrive/targets');
    const list=Array.isArray(targets)?targets:[];
    if(!list.length){$('od-targets').innerHTML='<div class="empty-state">No sync targets configured</div>';return;}
    let h='<table><tr><th>ID</th><th>Type</th><th>Owner</th><th>Path</th><th>Label</th><th>Files</th><th>Chunks</th><th>Size</th><th>Last Sync</th><th>Status</th><th>Actions</th></tr>';
    for(const t of list){
      const st=t.is_active?'<span class="green">● Active</span>':'<span class="red">● Paused</span>';
      const err=t.last_sync_error?`<br><span style="font-size:.72rem;color:var(--red)">${esc((t.last_sync_error||'').substring(0,60))}</span>`:'';
      h+=`<tr>
        <td>${t.id}</td><td>${esc(t.drive_type||'')}</td><td>${esc(t.drive_owner||'')}</td>
        <td>${esc(t.folder_path||'/')}</td><td>${esc(t.label||'—')}</td>
        <td>${fmtNum(t.total_files||0)}</td><td>${fmtNum(t.total_chunks||0)}</td>
        <td>${esc(t.total_size||'—')}</td>
        <td>${t.last_sync_at?fmtTime(t.last_sync_at):'Never'}${err}</td>
        <td>${st}</td>
        <td><button class="btn btn-outline btn-sm" onclick="odToggleTarget(${t.id})">${t.is_active?'Pause':'Resume'}</button></td>
      </tr>`;
    }
    $('od-targets').innerHTML=h+'</table>';
  } catch(e){$('od-targets').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function odToggleTarget(id){
  try {
    await api('/api/onedrive/targets/'+id+'/toggle',{method:'POST'});
    await loadOdTargets();
  } catch(e){alert('Error: '+e.message);}
}

async function loadOdWebhooks(){
  try {
    const data=await api('/api/onedrive/webhooks');
    const wh=data?.webhooks||[];
    if(!wh.length){$('od-webhooks').innerHTML='<div class="empty-state">No webhook subscriptions</div>';return;}
    let h='<table><tr><th>Target</th><th>Owner</th><th>Resource</th><th>Expires</th><th>Created</th><th>Status</th><th>Actions</th></tr>';
    for(const w of wh){
      const exp=w.expiration_at||'';
      const isExp=exp&&new Date(exp)<new Date();
      const st=!w.is_active?'<span class="red">Inactive</span>':isExp?'<span class="red">Expired</span>':'<span class="green">Active</span>';
      h+=`<tr><td>${esc(w.target_label||'#'+w.sync_target_id)}</td><td>${esc(w.drive_owner||'')}</td><td style="font-size:.72rem;max-width:220px;overflow:hidden;text-overflow:ellipsis">${esc(w.resource||'')}</td><td>${exp?fmtTime(exp):'—'}</td><td>${w.created_at?fmtTime(w.created_at):'—'}</td><td>${st}</td><td><button class="btn btn-outline btn-sm" onclick="odDeleteWebhook('${esc(w.subscription_id||'')}')">Delete</button></td></tr>`;
    }
    $('od-webhooks').innerHTML=h+'</table>';
  } catch(e){$('od-webhooks').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function odRegisterWebhooks(){
  try {
    $('od-webhook-indicator').innerHTML='<span class="spinner"></span> Registering...';
    await api('/api/onedrive/webhooks',{method:'POST',body:JSON.stringify({})});
    $('od-webhook-indicator').innerHTML='<span class="green">✓ Done</span>';
    setTimeout(()=>{$('od-webhook-indicator').textContent='';},3000);
    await loadOdWebhooks();
  } catch(e){$('od-webhook-indicator').textContent='Error: '+e.message;}
}

async function odDeleteWebhook(id){
  if(!confirm('Delete this OneDrive webhook?'))return;
  try {
    await api('/api/onedrive/webhooks/'+id,{method:'DELETE'});
    await loadOdWebhooks();
  } catch(e){alert('Error: '+e.message);}
}

async function odTriggerSync(){
  try {
    $('od-sync-indicator').innerHTML='<span class="spinner"></span> Starting sync...';
    $('od-sync-btn').disabled=true;
    const r=await api('/api/onedrive/sync',{method:'POST',body:JSON.stringify({})});
    $('od-sync-indicator').innerHTML=`<span class="green">✓ Sync started (PID ${r.pid||'?'})</span>`;
    setTimeout(async()=>{
      $('od-sync-indicator').textContent='';
      $('od-sync-btn').disabled=false;
      await Promise.all([loadOdStats(),loadOdRuns(),loadOdFiles()]);
    },15000);
  } catch(e){
    $('od-sync-indicator').textContent='Error: '+e.message;
    $('od-sync-btn').disabled=false;
  }
}

async function loadOdRuns(){
  try {
    const d=await api('/api/onedrive/stats');
    const runs=d?.recent_runs||[];
    if(!runs.length){$('od-runs').innerHTML='<div class="empty-state">No sync runs yet</div>';return;}
    let h='<table><tr><th>Time</th><th>Target</th><th>Trigger</th><th>Discovered</th><th>New</th><th>Updated</th><th>Failed</th><th>Chunks</th><th>Duration</th><th>Status</th></tr>';
    for(const r of runs){
      const dur=r.duration_seconds?r.duration_seconds.toFixed(1)+'s':'—';
      const stClass=r.status==='completed'?'green':r.status==='running'?'cyan':'red';
      h+=`<tr><td>${r.started_at?fmtTime(r.started_at):'—'}</td><td>${esc(r.label||'#'+(r.sync_target_id||''))}</td><td>${esc(r.trigger||'—')}</td><td>${fmtNum(r.files_discovered||0)}</td><td>${fmtNum(r.files_new||0)}</td><td>${fmtNum(r.files_updated||0)}</td><td>${fmtNum(r.files_failed||0)}</td><td>${fmtNum(r.chunks_created||0)}</td><td>${dur}</td><td><span class="${stClass}">${esc(r.status||'?')}</span></td></tr>`;
    }
    $('od-runs').innerHTML=h+'</table>';
  } catch(e){$('od-runs').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function loadOdExtBreakdown(){
  try {
    const d=await api('/api/onedrive/stats');
    const exts=d?.extension_breakdown||[];
    if(!exts.length){$('od-ext-breakdown').innerHTML='<div class="empty-state">No files indexed yet</div>';return;}
    let h='<div style="display:flex;flex-wrap:wrap;gap:8px">';
    for(const e2 of exts){
      const ext=e2.file_extension||'(none)';
      const cnt=e2.cnt||0;
      const sz=e2.total_size||'—';
      h+=`<div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:8px 14px;font-size:.82rem"><strong>${esc(ext)}</strong> <span style="color:var(--muted)">${fmtNum(cnt)} files · ${esc(sz)}</span></div>`;
    }
    $('od-ext-breakdown').innerHTML=h+'</div>';
  } catch(e){$('od-ext-breakdown').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function loadOdFiles(){
  try {
    const q=$('od-files-q')?.value||'';
    const st=$('od-files-status')?.value||'';
    let path='/api/onedrive/files?limit=50&offset='+odFilesOffset;
    if(q) path+='&q='+encodeURIComponent(q);
    if(st) path+='&status='+st;
    const d=await api(path);
    const files=d?.files||[];
    const total=d?.total||0;
    $('od-files-count').textContent=`(${fmtNum(total)} total)`;
    if(!files.length){$('od-files').innerHTML='<div class="empty-state">No files found</div>';$('od-files-pager').innerHTML='';return;}
    let h='<table><tr><th>Name</th><th>Path</th><th>Type</th><th>Size</th><th>Status</th><th>Modified</th><th>Indexed</th></tr>';
    for(const f of files){
      const stClass=f.status==='indexed'?'green':f.status==='pending'?'cyan':f.status==='failed'?'red':'muted';
      const sz=f.size_bytes?(f.size_bytes<1024?f.size_bytes+' B':f.size_bytes<1048576?(f.size_bytes/1024).toFixed(1)+' KB':(f.size_bytes/1048576).toFixed(1)+' MB'):'—';
      const name=f.web_url?`<a href="${esc(f.web_url)}" target="_blank" style="color:var(--cyan)">${esc(f.file_name||'')}</a>`:esc(f.file_name||'');
      h+=`<tr><td>${name}</td><td style="font-size:.75rem;max-width:250px;overflow:hidden;text-overflow:ellipsis;color:var(--muted)">${esc(f.file_path||'')}</td><td>${esc(f.file_extension||'—')}</td><td>${sz}</td><td><span class="${stClass}">${esc(f.status||'')}</span></td><td>${f.graph_modified_at?fmtTime(f.graph_modified_at):'—'}</td><td>${f.indexed_at?fmtTime(f.indexed_at):'—'}</td></tr>`;
    }
    $('od-files').innerHTML=h+'</table>';
    // Pager
    let pg='';
    if(odFilesOffset>0) pg+=`<button class="btn btn-outline btn-sm" onclick="odFilesOffset-=50;loadOdFiles()">← Prev</button>`;
    pg+=`<span style="font-size:.82rem;color:var(--muted)">Showing ${odFilesOffset+1}–${Math.min(odFilesOffset+50,total)} of ${fmtNum(total)}</span>`;
    if(odFilesOffset+50<total) pg+=`<button class="btn btn-outline btn-sm" onclick="odFilesOffset+=50;loadOdFiles()">Next →</button>`;
    $('od-files-pager').innerHTML=pg;
  } catch(e){$('od-files').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

async function odSearch(){
  const q=$('od-search-q').value.trim();
  if(!q){$('od-search-results').innerHTML='<div class="empty-state">Enter a search query</div>';return;}
  try {
    $('od-search-results').innerHTML='<div class="empty-state"><span class="spinner"></span> Searching...</div>';
    const st=$('od-search-type').value;
    const data=await api('/api/onedrive/search?q='+encodeURIComponent(q)+'&search_type='+st+'&limit=15');
    const results=data?.results||[];
    if(!results.length){$('od-search-results').innerHTML='<div class="empty-state">No results found</div>';return;}
    let h=`<div style="font-size:.78rem;color:var(--muted);margin-bottom:8px">${results.length} results in ${data.execution_time_ms||0}ms (${esc(data.search_type||'')})</div>`;
    h+='<table><tr><th>File</th><th>Path</th><th>Chunk</th><th>Score</th><th>Excerpt</th></tr>';
    for(const r of results){
      const name=r.web_url?`<a href="${esc(r.web_url)}" target="_blank" style="color:var(--cyan)">${esc(r.file_name||'')}</a>`:esc(r.file_name||'');
      const score=(r.score||0).toFixed(3);
      const excerpt=esc((r.chunk_text||'').substring(0,200));
      h+=`<tr><td>${name}</td><td style="font-size:.72rem;max-width:180px;overflow:hidden;text-overflow:ellipsis;color:var(--muted)">${esc(r.file_path||'')}</td><td>#${r.chunk_index||0}${r.page_number?' p'+r.page_number:''}</td><td>${score}</td><td style="font-size:.78rem;max-width:350px">${excerpt}...</td></tr>`;
    }
    $('od-search-results').innerHTML=h+'</table>';
  } catch(e){$('od-search-results').innerHTML=`<div class="empty-state red">${esc(e.message)}</div>`;}
}

// ═══════════════════════════════════════════════════════════
//  TAB: AI CHAT
// ═══════════════════════════════════════════════════════════
let chatConvId=null, chatSending=false, chatConversations=[], chatShowArchived=false, chatModelOptions=null;
let chatPendingFiles=[];
let chatAbortController=null;
let chatSearchFilter='';

// ── Relative time helper ──
function chatRelTime(dateStr){
  if(!dateStr) return '';
  const now=Date.now(), then=new Date(dateStr).getTime();
  const diff=Math.max(0,now-then);
  const sec=Math.floor(diff/1000), min=Math.floor(sec/60), hr=Math.floor(min/60), day=Math.floor(hr/24);
  if(sec<30) return 'just now';
  if(min<1) return sec+'s ago';
  if(min<60) return min+'m ago';
  if(hr<24) return hr+'h ago';
  if(day===1) return 'yesterday';
  return day+'d ago';
}

// ── File attachment helpers ──
function handleFileSelect(event){
  const files=Array.from(event.target.files||[]);
  for(const f of files){
    if(f.size>50*1024*1024){alert(f.name+' exceeds 50 MB limit');continue;}
    chatPendingFiles.push(f);
  }
  event.target.value='';
  renderPreviewStrip();
}

function removePreviewFile(idx){
  chatPendingFiles.splice(idx,1);
  renderPreviewStrip();
}

function renderPreviewStrip(){
  const el=$('chat-preview-strip');
  if(!el) return;
  if(!chatPendingFiles.length){el.innerHTML='';return;}
  el.innerHTML=chatPendingFiles.map((f,i)=>{
    const isImg=f.type.startsWith('image/');
    const thumb=isImg?URL.createObjectURL(f):'';
    const icon=isImg?'':(f.name.match(/\\.pdf$/i)?'&#128196;':f.name.match(/\\.docx?$/i)?'&#128221;':'&#128206;');
    const size=f.size<1024?(f.size+' B'):f.size<1048576?((f.size/1024).toFixed(1)+' KB'):((f.size/1048576).toFixed(1)+' MB');
    return `<div class="chat-preview-item">
      ${isImg?'<img src="'+thumb+'" alt=""/>':'<span class="file-icon">'+icon+'</span>'}
      <div class="file-info"><span class="file-name" title="${esc(f.name)}">${esc(f.name)}</span><span class="file-size">${size}</span></div>
      <button class="chat-preview-remove" onclick="removePreviewFile(${i})" title="Remove">&times;</button>
    </div>`;
  }).join('');
}

async function uploadPendingFiles(conversationId){
  if(!chatPendingFiles.length) return {attachmentIds:[],conversationId:conversationId};
  const fd=new FormData();
  fd.append('conversation_id',conversationId||'');
  for(const f of chatPendingFiles) fd.append('files',f);
  const resp=await fetch('/api/chat/upload',{method:'POST',body:fd,credentials:'same-origin'});
  if(!resp.ok){const e=await resp.json();throw new Error(e.error||'Upload failed');}
  const data=await resp.json();
  chatPendingFiles=[];
  renderPreviewStrip();
  return {attachmentIds:(data.attachments||[]).map(a=>a.id), conversationId:data.conversation_id||conversationId};
}

function renderAttachmentChips(attachments){
  if(!attachments||!attachments.length) return '';
  return '<div class="chat-attach-chips">'+attachments.map(a=>{
    const isImg=(a.asset_type||a.content_type||'').startsWith('image');
    if(isImg){
      return '<a class="chat-attach-chip" href="/api/chat/attachments/'+a.id+'" target="_blank">&#128248; '+esc(a.file_name)+'</a>';
    }
    return '<a class="chat-attach-chip" href="/api/chat/attachments/'+a.id+'" target="_blank">&#128196; '+esc(a.file_name)+'</a>';
  }).join('')+'</div>';
}

function renderAttachmentImages(attachments){
  if(!attachments||!attachments.length) return '';
  let html='';
  for(const a of attachments){
    const isImg=(a.asset_type||'').startsWith('image')||(a.content_type||'').startsWith('image');
    if(isImg){
      html+='<img class="chat-inline-img" src="/api/chat/attachments/'+a.id+'" alt="'+esc(a.file_name)+'" onclick="window.open(this.src,\'_blank\')" />';
    }
  }
  return html;
}

// ── Drag-drop on chat input area ──
document.addEventListener('DOMContentLoaded',()=>{
  const area=$('chat-input-area');
  if(!area) return;
  let dragCounter=0;
  area.addEventListener('dragenter',e=>{e.preventDefault();dragCounter++;area.classList.add('dragging');});
  area.addEventListener('dragleave',e=>{e.preventDefault();dragCounter--;if(dragCounter<=0){dragCounter=0;area.classList.remove('dragging');}});
  area.addEventListener('dragover',e=>{e.preventDefault();});
  area.addEventListener('drop',e=>{
    e.preventDefault();dragCounter=0;area.classList.remove('dragging');
    const files=Array.from(e.dataTransfer.files||[]);
    for(const f of files){
      if(f.size>50*1024*1024){alert(f.name+' exceeds 50 MB limit');continue;}
      chatPendingFiles.push(f);
    }
    renderPreviewStrip();
  });
  // Scroll-to-bottom watcher
  const msgsEl=$('chat-messages');
  if(msgsEl){
    msgsEl.addEventListener('scroll',()=>{
      const btn=$('chat-scroll-btn');
      if(!btn) return;
      const atBottom=msgsEl.scrollHeight-msgsEl.scrollTop-msgsEl.clientHeight<120;
      btn.classList.toggle('visible',!atBottom);
    });
  }
});

function scrollChatToBottom(){
  const el=$('chat-messages');
  if(el) el.scrollTop=el.scrollHeight;
}

// ── Conversation search ──
function filterConversations(query){
  chatSearchFilter=(query||'').toLowerCase().trim();
  renderConvList();
}

// ── Copy helpers ──
function copyMsgText(btn){
  const wrap=btn.closest('.chat-msg');
  if(!wrap) return;
  const text=wrap.innerText||wrap.textContent||'';
  navigator.clipboard.writeText(text).then(()=>{
    const orig=btn.innerHTML;
    btn.innerHTML='&#10003;';
    setTimeout(()=>{btn.innerHTML=orig;},1200);
  });
}

function copyCodeBlock(btn){
  const pre=btn.closest('pre');
  if(!pre) return;
  const code=pre.querySelector('code');
  const text=code?code.textContent:pre.textContent;
  navigator.clipboard.writeText(text).then(()=>{
    const orig=btn.textContent;
    btn.textContent='Copied!';
    setTimeout(()=>{btn.textContent=orig;},1200);
  });
}

// ── Edit user message ──
function editUserMsg(btn){
  const wrap=btn.closest('.chat-msg-wrap.user');
  if(!wrap) return;
  const msgDiv=wrap.querySelector('.chat-msg.user');
  if(!msgDiv) return;
  const rawText=msgDiv.getAttribute('data-raw')||msgDiv.innerText||'';
  msgDiv.style.display='none';
  const editArea=document.createElement('textarea');
  editArea.className='chat-edit-area';
  editArea.value=rawText;
  const actions=document.createElement('div');
  actions.className='chat-edit-actions';
  actions.innerHTML='<button onclick="cancelEditMsg(this)">Cancel</button><button class="save" onclick="submitEditMsg(this)">Save & Resend</button>';
  wrap.appendChild(editArea);
  wrap.appendChild(actions);
  editArea.focus();
  editArea.style.height=Math.min(editArea.scrollHeight,200)+'px';
  editArea.addEventListener('keydown',e=>{
    if(e.key==='Escape'){e.preventDefault();cancelEditMsg(actions.querySelector('button'));}
    if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();submitEditMsg(actions.querySelector('.save'));}
  });
}

function cancelEditMsg(btn){
  const wrap=btn.closest('.chat-msg-wrap.user');
  if(!wrap) return;
  wrap.querySelector('.chat-edit-area')?.remove();
  wrap.querySelector('.chat-edit-actions')?.remove();
  const msgDiv=wrap.querySelector('.chat-msg.user');
  if(msgDiv) msgDiv.style.display='';
}

async function submitEditMsg(btn){
  const wrap=btn.closest('.chat-msg-wrap.user');
  if(!wrap) return;
  const editArea=wrap.querySelector('.chat-edit-area');
  if(!editArea) return;
  const newText=editArea.value.trim();
  if(!newText){cancelEditMsg(btn);return;}
  // Remove all messages after this one
  const msgsEl=$('chat-messages');
  let sibling=wrap.nextElementSibling;
  while(sibling){
    const next=sibling.nextElementSibling;
    sibling.remove();
    sibling=next;
  }
  // Update the message display
  wrap.querySelector('.chat-edit-area')?.remove();
  wrap.querySelector('.chat-edit-actions')?.remove();
  const msgDiv=wrap.querySelector('.chat-msg.user');
  if(msgDiv){
    msgDiv.style.display='';
    msgDiv.innerHTML=esc(newText);
    msgDiv.setAttribute('data-raw',newText);
  }
  // Resend
  $('chat-input').value=newText;
  await sendChat();
}

// ── Stop generation ──
function stopChatGeneration(){
  if(chatAbortController){
    chatAbortController.abort();
    chatAbortController=null;
  }
}

async function loadChat(){
  try {
    const url=chatShowArchived?'/api/chat/conversations?limit=50&archived=true':'/api/chat/conversations?limit=50';
    chatConversations = await api(url);
    renderConvList();
    $('chat-view-label').textContent=chatShowArchived?'Archived':'Conversations';
    $('toggle-archived').textContent=chatShowArchived?'Active':'Archived';
    const gn=$('chat-greeting-name');
    if(gn) gn.textContent=currentUser||'there';
  } catch(e){ console.error('loadChat',e); }
}

function renderConvList(){
  const el=$('chat-conv-list');
  let filtered=chatConversations;
  if(chatSearchFilter){
    filtered=chatConversations.filter(c=>(c.title||'').toLowerCase().includes(chatSearchFilter));
  }
  if(!filtered.length){
    el.innerHTML=`<div style="padding:20px;text-align:center;color:var(--muted);font-size:.82rem">${chatSearchFilter?'No matches':chatShowArchived?'No archived conversations':'No conversations yet'}</div>`;
    return;
  }
  el.innerHTML=filtered.map(c=>`
    <div class="chat-conv-item${c.id===chatConvId?' active':''}${c.archived?' archived':''}" onclick="openConversation('${c.id}')" data-id="${c.id}">
      <span class="conv-title" title="${esc(c.title)}">${esc(c.title)}</span>
      <div class="conv-actions">
        <button class="conv-action rename" onclick="event.stopPropagation();renameConversation('${c.id}')" title="Rename">&#9998;</button>
        <button class="conv-action archive" onclick="event.stopPropagation();${c.archived?'unarchive':'archive'}Conversation('${c.id}')" title="${c.archived?'Unarchive':'Archive'}">${c.archived?'&#128228;':'&#128229;'}</button>
        <button class="conv-action delete" onclick="event.stopPropagation();deleteConversation('${c.id}')" title="Delete">&#128465;</button>
      </div>
    </div>
  `).join('');
}

async function newConversation(){
  chatConvId=null;
  $('chat-messages').innerHTML=`
    <div class="chat-welcome">
      <div class="chat-welcome-greeting">Hey <span id="chat-greeting-name">${currentUser||'there'}</span>, how can I help?</div>
      <div class="suggestions">
        <button onclick="chatSuggestion('What cases are currently active?')">Active cases</button>
        <button onclick="chatSuggestion('Search for documents mentioning Starbucks')">Search documents</button>
        <button onclick="chatSuggestion('Show me recent emails about court filings')">Recent emails</button>
        <button onclick="chatSuggestion('What are the database statistics?')">Database stats</button>
        <button onclick="chatSuggestion('Search the web for Washington state RCW 4.92')">Web search</button>
        <button onclick="chatSuggestion('Summarize case 24-2-01031-31')">Summarize a case</button>
      </div>
    </div>`;
  renderConvList();
  $('chat-input').value='';
  $('chat-input').focus();
}

function buildMsgHTML(role, content, opts={}){
  const ts=opts.timestamp?chatRelTime(opts.timestamp):'';
  const modelBadge=opts.modelLabel&&role==='assistant'?`<span class="model-badge">${esc(opts.modelLabel)}</span>`:'';
  const nameLabel=role==='assistant'?'Athena AI':esc(currentUser||'You');
  const avatar=role==='assistant'?'<img src="/static/athena-portrait.jpg" style="width:28px;height:28px;border-radius:50%;margin-right:6px;vertical-align:middle;object-fit:cover"/>':'';
  const attachHtml=opts.attachments?(renderAttachmentImages(opts.attachments)+renderAttachmentChips(opts.attachments)):'';
  const contentHtml=role==='assistant'?renderMarkdown(content):esc(content);
  const copyBtn=`<button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button>`;
  const editBtn=role==='user'?`<button class="msg-hover-btn" onclick="editUserMsg(this)" title="Edit">&#9998;</button>`:'';

  return `<div class="chat-msg-wrap ${role}">
    <div class="chat-msg-meta">
      ${avatar}<span class="chat-msg-name">${nameLabel}</span>${modelBadge}
      ${ts?'<span class="chat-msg-time">'+ts+'</span>':''}
    </div>
    <div class="chat-msg ${role}" ${role==='user'?'data-raw="'+esc(content).replace(/"/g,'&quot;')+'"':''}>${contentHtml}${attachHtml}
      <div class="msg-hover-actions">${editBtn}${copyBtn}</div>
    </div>
    ${opts.showRetry?retryBarHTML():''}
  </div>`;
}

async function openConversation(id){
  chatConvId=id;
  chatPendingFiles=[];
  renderPreviewStrip();
  renderConvList();
  try {
    const msgs = await api('/api/chat/conversations/'+id);
    const el=$('chat-messages');
    if(!msgs.length){
      el.innerHTML='<div style="padding:40px;text-align:center;color:var(--muted)">Empty conversation. Start typing below.</div>';
      return;
    }
    el.innerHTML=msgs.map((m,i,arr)=>{
      const isLastAssistant=m.role==='assistant'&&!arr.slice(i+1).some(x=>x.role==='assistant');
      return buildMsgHTML(m.role, m.content, {
        timestamp:m.created_at||m.timestamp,
        modelLabel:m.model_label||m.model||'',
        attachments:m.attachments,
        showRetry:isLastAssistant
      });
    }).join('');
    el.scrollTop=el.scrollHeight;
  } catch(e){ console.error(e); }
}

async function deleteConversation(id){
  if(!confirm('Delete this conversation? This cannot be undone.')) return;
  try {
    await api('/api/chat/conversations/'+id,{method:'DELETE'});
    if(chatConvId===id) newConversation();
    chatConversations=chatConversations.filter(c=>c.id!==id);
    renderConvList();
  } catch(e){ alert('Error: '+e.message); }
}

async function archiveConversation(id){
  try {
    await api('/api/chat/conversations/'+id,{method:'PUT',body:JSON.stringify({archived:true})});
    chatConversations=chatConversations.filter(c=>c.id!==id);
    if(chatConvId===id) newConversation();
    renderConvList();
  } catch(e){ alert('Error: '+e.message); }
}

async function unarchiveConversation(id){
  try {
    await api('/api/chat/conversations/'+id,{method:'PUT',body:JSON.stringify({archived:false})});
    chatConversations=chatConversations.filter(c=>c.id!==id);
    if(chatConvId===id) newConversation();
    renderConvList();
  } catch(e){ alert('Error: '+e.message); }
}

function renameConversation(id){
  const item=document.querySelector(`.chat-conv-item[data-id="${id}"]`);
  if(!item) return;
  const titleSpan=item.querySelector('.conv-title');
  const oldTitle=titleSpan.textContent;
  const input=document.createElement('input');
  input.type='text';
  input.className='conv-title-input';
  input.value=oldTitle;
  input.onclick=e=>e.stopPropagation();
  input.onblur=async()=>{
    const newTitle=input.value.trim()||oldTitle;
    titleSpan.textContent=newTitle;
    titleSpan.style.display='';
    input.remove();
    if(newTitle!==oldTitle){
      try {
        await api('/api/chat/conversations/'+id,{method:'PUT',body:JSON.stringify({title:newTitle})});
        const conv=chatConversations.find(c=>c.id===id);
        if(conv) conv.title=newTitle;
      } catch(e){ alert('Error: '+e.message); titleSpan.textContent=oldTitle; }
    }
  };
  input.onkeydown=e=>{
    if(e.key==='Enter'){ e.preventDefault(); input.blur(); }
    if(e.key==='Escape'){ e.preventDefault(); input.value=oldTitle; input.blur(); }
  };
  titleSpan.style.display='none';
  titleSpan.parentNode.insertBefore(input,titleSpan);
  input.focus();
  input.select();
}

function toggleArchivedView(){
  chatShowArchived=!chatShowArchived;
  loadChat();
}

function chatSuggestion(text){
  $('chat-input').value=text;
  sendChat();
}

function chatKeyDown(e){
  if(e.key==='Enter'&&!e.shiftKey){
    e.preventDefault();
    sendChat();
  }
  if(e.key==='Escape'){
    const editArea=document.querySelector('.chat-edit-area');
    if(editArea){
      const cancelBtn=editArea.parentElement.querySelector('.chat-edit-actions button');
      if(cancelBtn) cancelEditMsg(cancelBtn);
    }
  }
}

function autoGrow(el){
  el.style.height='auto';
  el.style.height=Math.min(el.scrollHeight,160)+'px';
}

async function sendChat(){
  if(chatSending) return;
  const input=$('chat-input');
  const msg=input.value.trim();
  const hasFiles=chatPendingFiles.length>0;
  if(!msg&&!hasFiles) return;

  const modelId=$('chat-model-select').value;
  const modelLabel=$('chat-model-select').selectedOptions[0].textContent;
  chatSending=true;
  $('chat-send-btn').disabled=true;
  input.value='';
  input.style.height='auto';

  const pendingFileInfo=chatPendingFiles.map(f=>({
    name:f.name, type:f.type, size:f.size,
    thumbUrl:f.type.startsWith('image/')?URL.createObjectURL(f):null
  }));

  const msgsEl=$('chat-messages');
  const welcome=msgsEl.querySelector('.chat-welcome');
  if(welcome) welcome.remove();

  // Add user message
  const userWrap=document.createElement('div');
  userWrap.className='chat-msg-wrap user';
  const userTs=new Date().toISOString();
  userWrap.innerHTML=`<div class="chat-msg-meta"><span class="chat-msg-name">${esc(currentUser||'You')}</span><span class="chat-msg-time">${chatRelTime(userTs)}</span></div>`;
  const userDiv=document.createElement('div');
  userDiv.className='chat-msg user';
  userDiv.setAttribute('data-raw',msg);
  let userHtml=msg?esc(msg):'';
  if(pendingFileInfo.length){
    userHtml+='<div class="chat-attach-chips">';
    for(const pf of pendingFileInfo){
      const icon=pf.type.startsWith('image/')?'&#128248;':pf.name.match(/\\.pdf$/i)?'&#128196;':'&#128206;';
      userHtml+=`<span class="chat-attach-chip">${icon} ${esc(pf.name)}</span>`;
    }
    userHtml+='</div>';
    for(const pf of pendingFileInfo){
      if(pf.thumbUrl) userHtml+=`<img class="chat-inline-img" src="${pf.thumbUrl}" alt="${esc(pf.name)}" />`;
    }
  }
  userHtml+='<div class="msg-hover-actions"><button class="msg-hover-btn" onclick="editUserMsg(this)" title="Edit">&#9998;</button><button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button></div>';
  userDiv.innerHTML=userHtml;
  userWrap.appendChild(userDiv);
  msgsEl.appendChild(userWrap);

  // Add thinking indicator
  const thinkDiv=document.createElement('div');
  thinkDiv.className='chat-thinking';
  const thinkLabel=hasFiles?'Uploading & processing...':modelId.startsWith('think')?'Reasoning deeply...':'Searching & thinking...';
  thinkDiv.innerHTML='<div class="dots"><span></span><span></span><span></span></div> '+thinkLabel;
  msgsEl.appendChild(thinkDiv);
  msgsEl.scrollTop=msgsEl.scrollHeight;

  // Show stop button
  const stopBtn=$('chat-stop-btn');
  if(stopBtn) stopBtn.classList.add('visible');

  try {
    // Upload files first if any
    let attachmentIds=[];
    if(hasFiles){
      const uploadResult=await uploadPendingFiles(chatConvId||'');
      attachmentIds=uploadResult.attachmentIds;
      if(!chatConvId && uploadResult.conversationId){
        chatConvId=uploadResult.conversationId;
      }
      thinkDiv.innerHTML='<div class="dots"><span></span><span></span><span></span></div> '+(modelId.startsWith('think')?'Reasoning deeply...':'Searching & thinking...');
    }

    // Stream via SSE
    chatAbortController=new AbortController();
    const fetchResp=await fetch('/api/chat/stream',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({conversation_id:chatConvId,message:msg,model:modelId,attachment_ids:attachmentIds}),
      credentials:'same-origin',
      signal:chatAbortController.signal
    });

    if(!fetchResp.ok){
      const errData=await fetchResp.json().catch(()=>({}));
      throw new Error(errData.error||'Request failed ('+fetchResp.status+')');
    }

    // Remove thinking
    thinkDiv.remove();

    // Prepare assistant message container for streaming
    const aWrap=document.createElement('div');
    aWrap.className='chat-msg-wrap assistant';
    const aTs=new Date().toISOString();
    aWrap.innerHTML=`<div class="chat-msg-meta"><img src="/static/athena-portrait.jpg" style="width:28px;height:28px;border-radius:50%;margin-right:6px;vertical-align:middle;object-fit:cover"/><span class="chat-msg-name">Athena AI</span><span class="model-badge">${esc(modelLabel)}</span><span class="chat-msg-time">${chatRelTime(aTs)}</span></div>`;
    const aDiv=document.createElement('div');
    aDiv.className='chat-msg assistant';
    aDiv.innerHTML='<span class="streaming-cursor" style="display:inline-block;width:2px;height:1em;background:var(--cyan);animation:dotPulse 1s infinite"></span>';
    aWrap.appendChild(aDiv);
    msgsEl.appendChild(aWrap);
    msgsEl.scrollTop=msgsEl.scrollHeight;

    // Read SSE stream
    let fullText='';
    let toolCalls=[];
    let respConvId=null;
    let respModelLabel=null;
    const reader=fetchResp.body.getReader();
    const decoder=new TextDecoder();
    let buffer='';

    while(true){
      const {done,value}=await reader.read();
      if(done) break;
      buffer+=decoder.decode(value,{stream:true});
      const lines=buffer.split('\n');
      buffer=lines.pop()||'';
      for(const line of lines){
        if(!line.startsWith('data: ')) continue;
        const jsonStr=line.slice(6).trim();
        if(!jsonStr) continue;
        try {
          const evt=JSON.parse(jsonStr);
          if(evt.content){
            fullText+=evt.content;
            aDiv.innerHTML=renderMarkdown(fullText)+'<div class="msg-hover-actions"><button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button></div>';
            msgsEl.scrollTop=msgsEl.scrollHeight;
          }
          if(evt.tool_calls) toolCalls=evt.tool_calls;
          if(evt.conversation_id) respConvId=evt.conversation_id;
          if(evt.model_label) respModelLabel=evt.model_label;
          if(evt.done){
            // Final render
            let toolHtml='';
            if(toolCalls.length){
              toolHtml=toolCalls.map(t=>`<span class="tool-badge">&#128295; ${esc(t.tool||t.name||'tool')}</span>`).join('')+'<br>';
            }
            aDiv.innerHTML=toolHtml+renderMarkdown(fullText||'(no response)')+'<div class="msg-hover-actions"><button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button></div>';
            if(respModelLabel){
              const badge=aWrap.querySelector('.model-badge');
              if(badge) badge.textContent=respModelLabel;
            }
          }
        } catch(pe){ /* skip unparseable */ }
      }
    }

    // Finalize: add retry bar
    aWrap.appendChild(makeRetryBar());

    // Set conversation ID if new
    if(!chatConvId && respConvId){
      chatConvId=respConvId;
      const url=chatShowArchived?'/api/chat/conversations?limit=50&archived=true':'/api/chat/conversations?limit=50';
      chatConversations = await api(url);
      renderConvList();
    }

    // Update sidebar title
    const existing=chatConversations.find(c=>c.id===chatConvId);
    if(existing&&existing.title==='New Conversation'){
      existing.title=(msg||'File upload').substring(0,80);
      renderConvList();
    }

    msgsEl.scrollTop=msgsEl.scrollHeight;
  } catch(e){
    thinkDiv.remove();
    if(e.name==='AbortError'){
      // User stopped generation - just clean up
      const lastAssist=msgsEl.querySelector('.chat-msg-wrap.assistant:last-of-type .chat-msg.assistant');
      if(lastAssist){
        const cursor=lastAssist.querySelector('.streaming-cursor');
        if(cursor) cursor.remove();
        lastAssist.innerHTML+=' <em style="color:var(--muted)">[stopped]</em>';
      }
      const lastWrap=msgsEl.querySelector('.chat-msg-wrap.assistant:last-of-type');
      if(lastWrap&&!lastWrap.querySelector('.retry-bar')) lastWrap.appendChild(makeRetryBar());
    } else {
      // Show error with retry button
      const errWrap=document.createElement('div');
      errWrap.className='chat-msg-wrap assistant';
      errWrap.innerHTML=`<div class="chat-msg-meta"><img src="/static/athena-portrait.jpg" style="width:28px;height:28px;border-radius:50%;margin-right:6px;vertical-align:middle;object-fit:cover"/><span class="chat-msg-name">Athena AI</span></div>`;
      const errDiv=document.createElement('div');
      errDiv.className='chat-error-wrap';
      errDiv.innerHTML=`<span class="error-text">Error: ${esc(e.message)}</span><button class="error-retry-btn" onclick="retrySendChat(this,'${esc(msg).replace(/'/g,"\\\\'")}')">Retry</button>`;
      errWrap.appendChild(errDiv);
      msgsEl.appendChild(errWrap);
    }
  } finally {
    chatSending=false;
    chatAbortController=null;
    $('chat-send-btn').disabled=false;
    const stopBtn2=$('chat-stop-btn');
    if(stopBtn2) stopBtn2.classList.remove('visible');
    input.focus();
  }
}

function retrySendChat(btn,msg){
  const wrap=btn.closest('.chat-msg-wrap');
  if(wrap) wrap.remove();
  $('chat-input').value=msg;
  sendChat();
}

function retryBarHTML(){
  const opts=chatModelOptions||[
    {id:'auto',label:'Auto'},{id:'think-low',label:'Think Low'},
    {id:'think-med',label:'Think Med'},{id:'think-high',label:'Think High'}
  ];
  const curModel=$('chat-model-select')?$('chat-model-select').value:'auto';
  return `<div class="retry-bar">
    <button class="retry-btn" onclick="retryChat(this)" title="Regenerate with selected model">&#8634; Retry</button>
    <select class="retry-model" title="Pick model for retry">${opts.map(o=>`<option value="${o.id}"${o.id===curModel?' selected':''}>${o.label}</option>`).join('')}</select>
  </div>`;
}

function makeRetryBar(){
  const d=document.createElement('div');
  d.innerHTML=retryBarHTML();
  return d.firstElementChild;
}

async function retryChat(btnEl){
  if(chatSending||!chatConvId) return;
  const bar=btnEl.closest('.retry-bar');
  const modelSel=bar.querySelector('.retry-model');
  const modelId=modelSel?modelSel.value:'auto';
  const modelLabel=modelSel?modelSel.selectedOptions[0].textContent:'Auto';

  chatSending=true;
  btnEl.disabled=true;
  btnEl.textContent='Retrying...';

  const msgsEl=$('chat-messages');
  const stopBtn=$('chat-stop-btn');
  if(stopBtn) stopBtn.classList.add('visible');

  const lastAssistant=bar.closest('.chat-msg-wrap.assistant');
  if(lastAssistant){
    const thinkDiv=document.createElement('div');
    thinkDiv.className='chat-thinking';
    const thinkLabel=modelId.startsWith('think')?'Re-reasoning deeply...':'Re-searching & thinking...';
    thinkDiv.innerHTML='<div class="dots"><span></span><span></span><span></span></div> '+thinkLabel;
    lastAssistant.replaceWith(thinkDiv);

    try {
      chatAbortController=new AbortController();
      const fetchResp=await fetch('/api/chat/stream',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({conversation_id:chatConvId,retry:true,model:modelId}),
        credentials:'same-origin',
        signal:chatAbortController.signal
      });

      if(!fetchResp.ok){
        const errData=await fetchResp.json().catch(()=>({}));
        throw new Error(errData.error||'Retry failed ('+fetchResp.status+')');
      }

      thinkDiv.remove();

      const aWrap=document.createElement('div');
      aWrap.className='chat-msg-wrap assistant';
      const aTs=new Date().toISOString();
      aWrap.innerHTML=`<div class="chat-msg-meta"><img src="/static/athena-portrait.jpg" style="width:28px;height:28px;border-radius:50%;margin-right:6px;vertical-align:middle;object-fit:cover"/><span class="chat-msg-name">Athena AI</span><span class="model-badge">${esc(modelLabel)}</span><span class="chat-msg-time">${chatRelTime(aTs)}</span></div>`;
      const aDiv=document.createElement('div');
      aDiv.className='chat-msg assistant';
      aWrap.appendChild(aDiv);
      msgsEl.appendChild(aWrap);

      let fullText='';
      let toolCalls=[];
      let respModelLabel=null;
      const reader=fetchResp.body.getReader();
      const decoder=new TextDecoder();
      let buffer='';

      while(true){
        const {done,value}=await reader.read();
        if(done) break;
        buffer+=decoder.decode(value,{stream:true});
        const lines=buffer.split('\n');
        buffer=lines.pop()||'';
        for(const line of lines){
          if(!line.startsWith('data: ')) continue;
          const jsonStr=line.slice(6).trim();
          if(!jsonStr) continue;
          try {
            const evt=JSON.parse(jsonStr);
            if(evt.content){
              fullText+=evt.content;
              aDiv.innerHTML=renderMarkdown(fullText)+'<div class="msg-hover-actions"><button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button></div>';
              msgsEl.scrollTop=msgsEl.scrollHeight;
            }
            if(evt.tool_calls) toolCalls=evt.tool_calls;
            if(evt.model_label) respModelLabel=evt.model_label;
            if(evt.done){
              let toolHtml='';
              if(toolCalls.length){
                toolHtml=toolCalls.map(t=>`<span class="tool-badge">&#128295; ${esc(t.tool||t.name||'tool')}</span>`).join('')+'<br>';
              }
              aDiv.innerHTML=toolHtml+renderMarkdown(fullText||'(no response)')+'<div class="msg-hover-actions"><button class="msg-hover-btn" onclick="copyMsgText(this)" title="Copy">&#128203;</button></div>';
              if(respModelLabel){
                const badge=aWrap.querySelector('.model-badge');
                if(badge) badge.textContent=respModelLabel;
              }
            }
          } catch(pe){ /* skip */ }
        }
      }

      aWrap.appendChild(makeRetryBar());
      msgsEl.scrollTop=msgsEl.scrollHeight;
    } catch(e){
      thinkDiv.remove();
      if(e.name!=='AbortError'){
        const errWrap=document.createElement('div');
        errWrap.className='chat-msg-wrap assistant';
        errWrap.innerHTML=`<div class="chat-msg-meta"><img src="/static/athena-portrait.jpg" style="width:28px;height:28px;border-radius:50%;margin-right:6px;vertical-align:middle;object-fit:cover"/><span class="chat-msg-name">Athena AI</span></div>`;
        const errDiv=document.createElement('div');
        errDiv.className='chat-error-wrap';
        errDiv.innerHTML=`<span class="error-text">Retry error: ${esc(e.message)}</span>`;
        errWrap.appendChild(errDiv);
        errWrap.appendChild(makeRetryBar());
        msgsEl.appendChild(errWrap);
        msgsEl.scrollTop=msgsEl.scrollHeight;
      }
    }
  }

  chatSending=false;
  chatAbortController=null;
  $('chat-send-btn').disabled=false;
  const stopBtn2=$('chat-stop-btn');
  if(stopBtn2) stopBtn2.classList.remove('visible');
  $('chat-input').focus();
}

// Markdown renderer with code copy buttons, tables, full formatting
function renderMarkdown(text){
  if(!text) return '';
  let html=esc(text);
  // Code blocks with copy button
  html=html.replace(/```(\w*)\n([\s\S]*?)```/g,(m,lang,code)=>{
    const langLabel=lang?`<span style="position:absolute;top:6px;left:10px;font-size:.62rem;color:var(--muted);text-transform:uppercase">${lang}</span>`:'';
    return `<pre style="position:relative">${langLabel}<button class="code-copy-btn" onclick="copyCodeBlock(this)">Copy</button><code>${code}</code></pre>`;
  });
  // Inline code
  html=html.replace(/`([^`]+)`/g,'<code>$1</code>');
  // Bold
  html=html.replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>');
  // Italic
  html=html.replace(/\*([^*]+)\*/g,'<em>$1</em>');
  // Headers
  html=html.replace(/^### (.+)$/gm,'<h3>$1</h3>');
  html=html.replace(/^## (.+)$/gm,'<h2>$1</h2>');
  html=html.replace(/^# (.+)$/gm,'<h1>$1</h1>');
  // Links
  html=html.replace(/\[([^\]]+)\]\(([^)]+)\)/g,'<a href="$2" target="_blank" rel="noopener">$1</a>');
  // Tables
  html=html.replace(/^(\|.+\|)\n(\|[\s\-:|]+\|)\n((?:\|.+\|\n?)*)/gm,(m,headerRow,sepRow,bodyRows)=>{
    const headers=headerRow.split('|').filter(c=>c.trim());
    const rows=bodyRows.trim().split('\n').filter(r=>r.trim());
    let table='<table><thead><tr>'+headers.map(h=>'<th>'+h.trim()+'</th>').join('')+'</tr></thead><tbody>';
    for(const row of rows){
      const cells=row.split('|').filter(c=>c.trim());
      table+='<tr>'+cells.map(c=>'<td>'+c.trim()+'</td>').join('')+'</tr>';
    }
    table+='</tbody></table>';
    return table;
  });
  // Unordered lists
  html=html.replace(/^[\u2022\-\*] (.+)$/gm,'<li>$1</li>');
  html=html.replace(/(<li>[\s\S]*?<\/li>)/g,'<ul>$1</ul>');
  // Ordered lists
  html=html.replace(/^\d+\. (.+)$/gm,'<li>$1</li>');
  // Blockquotes
  html=html.replace(/^&gt; (.+)$/gm,'<blockquote>$1</blockquote>');
  // Line breaks
  html=html.replace(/\n\n/g,'<br><br>');
  return html;
}

// ═══════════════════════════════════════════════════════════
//  TAB: MCP ACCOUNTS
// ═══════════════════════════════════════════════════════════
let _mcpClients = [];
async function loadMcpAccounts(){
  try {
    const resp = await api('/api/mcp/clients');
    const clients = resp.clients || [];
    _mcpClients = clients;
    if(!clients.length){
      $('mcp-clients-grid').innerHTML='<div class="empty-state">No OAuth clients registered</div>';
    } else {
      let html=`<table style="width:100%;border-collapse:collapse;font-size:.88rem">
        <thead><tr style="border-bottom:2px solid var(--border);text-align:left">
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Client Name</th>
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Client ID</th>
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Status</th>
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Scopes</th>
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Queries</th>
          <th style="padding:10px 12px;color:var(--muted);font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.04em">Last Used</th>
        </tr></thead><tbody>`;
      clients.forEach((c,i)=>{
        const used = c.last_used ? fmtTime(c.last_used) : '<span style="color:var(--muted)">Never</span>';
        const status = c.is_active ? '<span class="green">● Active</span>' : '<span class="red">● Disabled</span>';
        html+=`<tr onclick="showMcpDetail(${i})" style="border-bottom:1px solid var(--border);cursor:pointer;transition:background .15s" onmouseover="this.style.background='rgba(59,130,246,.06)'" onmouseout="this.style.background='none'">
          <td style="padding:10px 12px;font-weight:600">${esc(c.client_name||'Unnamed')}</td>
          <td style="padding:10px 12px"><code style="color:var(--cyan);font-size:.8rem">${esc(c.client_id)}</code></td>
          <td style="padding:10px 12px">${status}</td>
          <td style="padding:10px 12px;font-size:.78rem;color:var(--muted)">${(c.scopes||[]).join(', ')}</td>
          <td style="padding:10px 12px">${fmtNum(c.logged_calls||0)}</td>
          <td style="padding:10px 12px">${used}</td>
        </tr>`;
      });
      html+='</tbody></table>';
      $('mcp-clients-grid').innerHTML=html;
    }

    // Token summary
    const analytics = await api('/api/mcp/analytics?hours=720');
    $('mcp-tokens-info').innerHTML = `
      <span>${fmtNum(analytics.total_queries||0)} total queries logged across all clients</span>`;
  } catch(e){
    $('mcp-clients-grid').innerHTML='<div class="empty-state red">Error loading clients: '+esc(e.message)+'</div>';
    console.error('loadMcpAccounts',e);
  }
}

function showMcpDetail(idx){
  const c = _mcpClients[idx];
  if(!c) return;
  $('mcp-modal-title').textContent = c.client_name || c.client_id;
  const created = c.created_at ? fmtTime(c.created_at) : 'Unknown';
  const lastUsed = c.last_used ? fmtTime(c.last_used) : 'Never';
  const lastQuery = c.last_query ? fmtTime(c.last_query) : 'Never';
  $('mcp-modal-body').innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px 24px;margin-bottom:16px">
      <div class="field"><label>Client ID</label>
        <div class="val" style="display:flex;align-items:center;gap:6px">
          <code style="color:var(--cyan);font-size:.82rem;word-break:break-all">${esc(c.client_id)}</code>
          <button onclick="navigator.clipboard.writeText('${esc(c.client_id)}');this.textContent='✓';setTimeout(()=>this.textContent='📋',1200)" style="background:none;border:none;cursor:pointer;font-size:.9rem;padding:2px" title="Copy">📋</button>
        </div>
      </div>
      <div class="field" style="grid-column:1/-1"><label>Client Secret</label>
        <div class="val" style="display:flex;align-items:center;gap:8px">
          <code id="mcp-secret-display" style="color:var(--cyan);font-size:.82rem;word-break:break-all;background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:6px 10px;flex:1;font-family:monospace;user-select:all">${c.client_secret ? esc(c.client_secret) : '<span style="color:var(--muted)">Not stored — only hash available: '+esc(c.client_secret_hash)+'</span>'}</code>
          ${c.client_secret ? '<button onclick="navigator.clipboard.writeText(\''+esc(c.client_secret)+'\');this.innerHTML=\'<span class=green>✓ Copied!</span>\';setTimeout(()=>this.textContent=\'Copy Secret\',1500)" class="btn btn-primary btn-sm" style="white-space:nowrap">📋 Copy Secret</button>' : ''}
        </div>
      </div>
      <div class="field"><label>Status</label>
        <div class="val">${c.is_active ? '<span class="green">● Active</span>' : '<span class="red">● Disabled</span>'}</div>
      </div>
      <div class="field"><label>Created</label><div class="val">${created}</div></div>
      <div class="field"><label>Last Used</label><div class="val">${lastUsed}</div></div>
      <div class="field"><label>Last Query</label><div class="val">${lastQuery}</div></div>
      <div class="field"><label>Total Tool Calls</label><div class="val">${fmtNum(c.total_tool_calls||0)}</div></div>
      <div class="field"><label>Logged Queries</label><div class="val">${fmtNum(c.logged_calls||0)}</div></div>
      <div class="field"><label>Active Tokens</label><div class="val">${fmtNum(c.active_tokens||0)}</div></div>
    </div>
    <div class="field"><label>Description</label><div class="val">${esc(c.description)||'<span style="color:var(--muted)">—</span>'}</div></div>
    <div class="field"><label>Scopes</label>
      <div class="val">${(c.scopes||[]).map(s=>'<span style="background:var(--bg);border:1px solid var(--border);border-radius:4px;padding:2px 8px;font-size:.78rem;margin-right:4px;display:inline-block;margin-bottom:4px">'+esc(s)+'</span>').join('')||'<span style="color:var(--muted)">None</span>'}</div>
    </div>
    <div class="field"><label>Redirect URIs</label>
      <div class="val" style="font-size:.82rem;word-break:break-all">${(c.redirect_uris||[]).map(u=>'<div style="padding:3px 0;border-bottom:1px solid var(--border)"><code>'+esc(u)+'</code></div>').join('')||'<span style="color:var(--muted)">None</span>'}</div>
    </div>
  `;
  $('mcp-modal-actions').innerHTML = `
    <button class="btn ${c.is_active?'btn-outline':'btn-primary'} btn-sm" onclick="toggleMcpClient('${esc(c.client_id)}',${c.is_active?'false':'true'})">
      ${c.is_active?'Disable Client':'Enable Client'}
    </button>
    <button class="btn btn-outline btn-sm" onclick="closeMcpModal()">Close</button>
  `;
  $('mcp-detail-modal').classList.remove('hidden');
}

function closeMcpModal(){ $('mcp-detail-modal').classList.add('hidden'); }

async function toggleMcpClient(clientId, enable){
  try {
    await api(`/api/mcp/clients/${clientId}/toggle`,{method:'POST',
      body:JSON.stringify({active:enable})});
    closeMcpModal();
    loadMcpAccounts();
  } catch(e){ alert('Error: '+e.message); }
}

// ═══════════════════════════════════════════════════════════
//  TAB: QUERY ANALYTICS
// ═══════════════════════════════════════════════════════════
let qlogOffset=0;
const QLOG_LIMIT=25;

async function loadAnalytics(){
  try {
    const hours = $('analytics-period')?.value || 168;
    const [data, quality, reliability] = await Promise.all([
      api(`/api/mcp/analytics?hours=${hours}`),
      api(`/api/quality/summary?hours=${hours}`),
      api(`/api/reliability/summary?hours=${Math.min(hours, 168)}`),
    ]);

    // Summary cards
    $('analytics-summary').innerHTML=`<div class="cards">
      <div class="card"><div class="stat"><div class="stat-value">${fmtNum(data.total_queries||0)}</div><div class="stat-label">Total Queries</div></div></div>
      <div class="card"><div class="stat"><div class="stat-value">${fmtNum(data.unique_tools||0)}</div><div class="stat-label">Unique Tools</div></div></div>
      <div class="card"><div class="stat"><div class="stat-value">${fmtNum(data.unique_clients||0)}</div><div class="stat-label">Active Clients</div></div></div>
      <div class="card"><div class="stat"><div class="stat-value">${data.avg_duration_ms!=null?data.avg_duration_ms+'ms':'—'}</div><div class="stat-label">Avg Duration</div></div></div>
      <div class="card"><div class="stat"><div class="stat-value">${fmtNum(data.error_count||0)}</div><div class="stat-label">Errors</div></div></div>
    </div>`;

    // Quality summary
    const qLatest = quality?.latest || {};
    const qMetrics = qLatest.metrics || {};
    const passRate = qLatest.pass_rate != null ? Math.round(qLatest.pass_rate * 100) + '%' : '—';
    const trend = quality?.trend != null ? (quality.trend >= 0 ? '+' : '') + Math.round(quality.trend * 100) + '%' : '—';
    $('quality-summary').innerHTML = `
      <div class="card"><div class="label">Eval Pass Rate</div>
        <div class="value green">${passRate}</div>
        <div class="sub">Trend ${trend}</div></div>
      <div class="card"><div class="label">Passed</div>
        <div class="value cyan">${fmtNum(qMetrics.passed||0)}</div></div>
      <div class="card"><div class="label">Failed</div>
        <div class="value red">${fmtNum(qMetrics.failed||0)}</div></div>
      <div class="card"><div class="label">Skipped</div>
        <div class="value amber">${fmtNum(qMetrics.skipped||0)}</div></div>
      <div class="card"><div class="label">Avg Results</div>
        <div class="value blue">${qMetrics.avg_results!=null?qMetrics.avg_results:'—'}</div></div>
      <div class="card"><div class="label">Last Eval</div>
        <div class="value purple">${fmtTime(qLatest.started_at)}</div></div>
    `;

    // Recent eval runs table
    const qRuns = quality?.runs || [];
    if(qRuns.length){
      $('quality-runs').innerHTML = `<div class="tbl-wrap"><table>
        <thead><tr><th>Time</th><th>Status</th><th>Pass Rate</th><th>Passed</th><th>Failed</th><th>Skipped</th><th>Summary</th></tr></thead>
        <tbody>${qRuns.slice(0,10).map(r=>`<tr>
          <td>${fmtTime(r.started_at)}</td>
          <td>${r.status==='success'||r.status==='completed' ? '<span class="badge badge-green">OK</span>' : '<span class="badge badge-red">ERR</span>'}</td>
          <td>${r.pass_rate!=null?Math.round(r.pass_rate*100)+'%':'—'}</td>
          <td>${r.metrics?.passed??0}</td>
          <td>${r.metrics?.failed??0}</td>
          <td>${r.metrics?.skipped??0}</td>
          <td title="${esc(r.summary||'')}">${esc((r.summary||'').slice(0,60))}</td>
        </tr>`).join('')}</tbody></table></div>`;
    } else {
      $('quality-runs').innerHTML = '<div class="empty-state">No eval runs recorded yet</div>';
    }

    // Reliability summary
    const rAgents = reliability?.agents || {};
    const rMcp = reliability?.mcp || {};
    const rHealth = reliability?.health_checks || {};
    $('reliability-summary').innerHTML = `
      <div class="card"><div class="label">Agent Error Rate</div>
        <div class="value amber">${Math.round((rAgents.error_rate||0)*100)}%</div>
        <div class="sub">${fmtNum(rAgents.errors||0)} / ${fmtNum(rAgents.total_runs||0)}</div></div>
      <div class="card"><div class="label">Agent p95</div>
        <div class="value cyan">${rAgents.p95_ms!=null?Math.round(rAgents.p95_ms)+'ms':'—'}</div></div>
      <div class="card"><div class="label">MCP Error Rate</div>
        <div class="value amber">${Math.round((rMcp.error_rate||0)*100)}%</div>
        <div class="sub">${fmtNum(rMcp.errors||0)} / ${fmtNum(rMcp.total_calls||0)}</div></div>
      <div class="card"><div class="label">MCP p95</div>
        <div class="value cyan">${rMcp.p95_ms!=null?Math.round(rMcp.p95_ms)+'ms':'—'}</div></div>
      <div class="card"><div class="label">MCP p99</div>
        <div class="value purple">${rMcp.p99_ms!=null?Math.round(rMcp.p99_ms)+'ms':'—'}</div></div>
      <div class="card"><div class="label">Health Checks</div>
        <div class="value green">${fmtNum(rHealth.healthy||0)}</div>
        <div class="sub">degraded: ${fmtNum(rHealth.degraded||0)} · critical: ${fmtNum(rHealth.critical||0)}</div></div>
    `;

    // Tool breakdown
    const tools = data.by_tool || [];
    if(tools.length){
      let thtml='<table class="tbl"><thead><tr><th>Tool</th><th>Calls</th><th>Avg ms</th></tr></thead><tbody>';
      tools.forEach(t=>{ thtml+=`<tr><td><code>${esc(t.tool_name)}</code></td><td>${fmtNum(t.count)}</td><td>${t.avg_ms||'—'}</td></tr>`; });
      thtml+='</tbody></table>';
      $('analytics-tools').innerHTML=thtml;
    } else { $('analytics-tools').innerHTML='<div class="empty-state">No tool data</div>'; }

    // Client breakdown
    const cls = data.by_client || [];
    if(cls.length){
      let chtml='<table class="tbl"><thead><tr><th>Client</th><th>Calls</th><th>Last Active</th></tr></thead><tbody>';
      cls.forEach(c=>{ chtml+=`<tr><td>${esc(c.client_name||c.client_id)}</td><td>${fmtNum(c.count)}</td><td>${fmtTime(c.last_active)}</td></tr>`; });
      chtml+='</tbody></table>';
      $('analytics-clients').innerHTML=chtml;
    } else { $('analytics-clients').innerHTML='<div class="empty-state">No client data</div>'; }

    // Errors
    const errs = data.recent_errors || [];
    if(errs.length){
      let ehtml='<table class="tbl"><thead><tr><th>Time</th><th>Client</th><th>Tool</th><th>Error</th></tr></thead><tbody>';
      errs.forEach(e=>{ ehtml+=`<tr><td>${fmtTime(e.called_at)}</td><td>${esc(e.client_name)}</td><td><code>${esc(e.tool_name)}</code></td><td class="red">${esc(e.error_message)}</td></tr>`; });
      ehtml+='</tbody></table>';
      $('analytics-errors').innerHTML=ehtml;
    } else { $('analytics-errors').innerHTML='<div class="empty-state green">No recent errors ✓</div>'; }

    // Load query log
    await loadQueryLog();

    // Populate filter dropdowns
    await populateQlogFilters(data);
  } catch(e){
    $('analytics-summary').innerHTML='<div class="empty-state red">Error: '+esc(e.message)+'</div>';
    console.error('loadAnalytics',e);
  }
}

async function triggerQualityEval(){
  const statusEl = $('quality-trigger-status');
  if (statusEl) statusEl.textContent = 'Triggering…';
  try {
    await api('/api/agents/quality-eval/trigger', {method:'POST'});
    if (statusEl) statusEl.textContent = 'Eval triggered';
    setTimeout(loadAnalytics, 4000);
  } catch(e){
    if (statusEl) statusEl.textContent = 'Failed to trigger';
  }
}

async function populateQlogFilters(analytics){
  const cs=$('qlog-client'), ts=$('qlog-tool');
  if(!cs||!ts) return;
  const curC=cs.value, curT=ts.value;
  cs.innerHTML='<option value="">All Clients</option>';
  ts.innerHTML='<option value="">All Tools</option>';
  (analytics.by_client||[]).forEach(c=>{
    cs.innerHTML+=`<option value="${esc(c.client_id)}">${esc(c.client_name||c.client_id)}</option>`;
  });
  (analytics.by_tool||[]).forEach(t=>{
    ts.innerHTML+=`<option value="${esc(t.tool_name)}">${esc(t.tool_name)}</option>`;
  });
  if(curC) cs.value=curC;
  if(curT) ts.value=curT;
}

async function loadQueryLog(){
  try {
    const client=$('qlog-client')?.value||'';
    const tool=$('qlog-tool')?.value||'';
    let url=`/api/mcp/query-log?limit=${QLOG_LIMIT}&offset=${qlogOffset}`;
    if(client) url+=`&client_id=${encodeURIComponent(client)}`;
    if(tool) url+=`&tool_name=${encodeURIComponent(tool)}`;
    const rows = await api(url);
    if(!rows.length && qlogOffset===0){
      $('qlog-table').innerHTML='<div class="empty-state">No queries logged yet</div>';
      $('qlog-count').textContent='';
      return;
    }
    let html='<table class="tbl"><thead><tr><th>Time</th><th>Client</th><th>Tool</th><th>Duration</th><th>Result</th></tr></thead><tbody>';
    rows.forEach(r=>{
      const dur = r.duration_ms!=null ? r.duration_ms+'ms' : '—';
      const err = r.error_message ? `<span class="red" title="${esc(r.error_message)}">✗ Error</span>` : '<span class="green">✓</span>';
      const args = r.arguments ? `<div style="font-size:.7rem;color:var(--muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(JSON.stringify(r.arguments))}">${esc(JSON.stringify(r.arguments).slice(0,80))}</div>` : '';
      html+=`<tr class="qd-row" onclick="openQueryDetail(${r.id})" title="Click for full details">
        <td style="white-space:nowrap">${fmtTime(r.called_at)}</td>
        <td>${esc(r.client_name||r.client_id)}</td>
        <td><code>${esc(r.tool_name)}</code>${args}</td>
        <td>${dur}</td>
        <td>${err}</td>
      </tr>`;
    });
    html+='</tbody></table>';
    $('qlog-table').innerHTML=html;
    $('qlog-count').textContent=`Showing ${qlogOffset+1}–${qlogOffset+rows.length}`;
    $('qlog-prev').disabled = qlogOffset===0;
    $('qlog-next').disabled = rows.length < QLOG_LIMIT;
  } catch(e){ $('qlog-table').innerHTML='<div class="empty-state red">Error: '+esc(e.message)+'</div>'; }
}

function qlogPrev(){ qlogOffset=Math.max(0,qlogOffset-QLOG_LIMIT); loadQueryLog(); }
function qlogNext(){ qlogOffset+=QLOG_LIMIT; loadQueryLog(); }

// ═══════════════════════════════════════════════════════════
//  INIT
// ═══════════════════════════════════════════════════════════
async function init(){
  // Restore last active tab from localStorage
  const savedTab = localStorage.getItem('acp-current-tab');
  if (savedTab && document.getElementById('tab-' + savedTab)) {
    switchTab(savedTab);
  } else {
    switchTab('overview');
  }
}

// ═══════════════════════════════════════════════════════════
// SECURITY — ACL Blacklist / Whitelist
// ═══════════════════════════════════════════════════════════

async function loadACL(){
  await Promise.all([loadBlacklist(), loadWhitelist()]);
}

async function loadBlacklist(){
  const rows = await api('/api/acl/blacklist');
  const el = $('acl-blacklist');
  if(!rows || !rows.length){ el.innerHTML='<div class="empty-state">No blacklisted IPs</div>'; return; }
  let h=`<table class="tbl"><thead><tr>
    <th>IP Address</th><th>Source</th><th>Reason</th><th>Attacks</th>
    <th>Last Seen</th><th>Active</th><th>Actions</th></tr></thead><tbody>`;
  for(const r of rows){
    const active = r.is_active;
    h+=`<tr style="${active?'':'opacity:.5'}">
      <td style="font-family:monospace;font-weight:600">${esc(r.ip_address)}</td>
      <td><span class="badge badge-${r.source==='watchdog'?'info':'outline'}">${esc(r.source)}</span></td>
      <td style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.reason)}">${esc(r.reason)}</td>
      <td style="text-align:center;font-weight:600">${r.attack_count||0}</td>
      <td>${fmtTime(r.last_seen||r.blocked_at)}</td>
      <td style="text-align:center"><span class="badge badge-${active?'danger':'outline'}">${active?'BLOCKED':'Disabled'}</span></td>
      <td>
        <button class="btn btn-outline btn-sm" onclick="toggleBlacklist(${r.id})">${active?'Disable':'Enable'}</button>
        <button class="btn btn-outline btn-sm" onclick="moveToWhitelist('${esc(r.ip_address)}',${r.id})" title="Move to whitelist">✅</button>
        <button class="btn btn-outline btn-sm" onclick="deleteBlacklist(${r.id})" style="color:var(--danger)" title="Delete">🗑️</button>
      </td></tr>`;
  }
  el.innerHTML = h + '</tbody></table>';
}

async function loadWhitelist(){
  const rows = await api('/api/acl/whitelist');
  const el = $('acl-whitelist');
  if(!rows || !rows.length){ el.innerHTML='<div class="empty-state">No whitelisted IPs</div>'; return; }
  let h=`<table class="tbl"><thead><tr>
    <th>IP Address</th><th>Label</th><th>Added By</th><th>Added</th><th>Actions</th></tr></thead><tbody>`;
  for(const r of rows){
    h+=`<tr>
      <td style="font-family:monospace;font-weight:600">${esc(r.ip_address)}</td>
      <td>${esc(r.label||'')}</td>
      <td>${esc(r.added_by||'')}</td>
      <td>${fmtTime(r.created_at)}</td>
      <td><button class="btn btn-outline btn-sm" onclick="deleteWhitelist(${r.id})" style="color:var(--danger)">Remove</button></td>
    </tr>`;
  }
  el.innerHTML = h + '</tbody></table>';
}

async function addBlacklist(){
  const ip = $('bl-ip').value.trim();
  const reason = $('bl-reason').value.trim() || 'Manual block';
  if(!ip){ alert('Enter an IP address'); return; }
  await api('/api/acl/blacklist/add',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({ip_address:ip, reason:reason})});
  $('bl-ip').value=''; $('bl-reason').value='';
  loadBlacklist();
}

async function toggleBlacklist(id){
  await api(`/api/acl/blacklist/${id}/toggle`,{method:'POST'});
  loadBlacklist();
}

async function deleteBlacklist(id){
  if(!confirm('Delete this blacklist entry?')) return;
  await api(`/api/acl/blacklist/${id}`,{method:'DELETE'});
  loadBlacklist();
}

async function moveToWhitelist(ip, blId){
  const label = prompt('Label for whitelist entry:', ip);
  if(label===null) return;
  await api('/api/acl/whitelist',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({ip_address:ip, label:label})});
  loadACL();
}

async function addWhitelist(){
  const ip = $('wl-ip').value.trim();
  const label = $('wl-label').value.trim();
  if(!ip){ alert('Enter an IP address'); return; }
  await api('/api/acl/whitelist',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({ip_address:ip, label:label})});
  $('wl-ip').value=''; $('wl-label').value='';
  loadACL();
}

async function deleteWhitelist(id){
  if(!confirm('Remove from whitelist?')) return;
  await api(`/api/acl/whitelist/${id}`,{method:'DELETE'});
  loadWhitelist();
}

// Auto-login check
(async()=>{
  try {
    const me = await api('/api/me');
    currentUser = me.display_name || 'User';
    $('login-screen').classList.add('hidden');
    $('main-app').classList.remove('hidden');
    init();
  } catch { showLogin(); }
})();


async function loadComms(){
  const view = document.getElementById('comms-view')?.value || 'timeline';
  const agent = document.getElementById('comms-agent-filter')?.value || '';
  const level = document.getElementById('comms-level-filter')?.value || '';
  const params = new URLSearchParams({view, agent, level, limit: '200'});

  try {
    const resp = await fetch('/api/agents/comms?' + params, {credentials:'include'});
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const d = await resp.json();

    // Stats cards
    const statsEl = document.getElementById('comms-stats');
    if (statsEl && d.stats) {
      const s = d.stats;
      statsEl.innerHTML = `
        <div class="card"><div class="card-value">${s.total_logs||0}</div><div class="card-label">Logs (24h)</div></div>
        <div class="card"><div class="card-value">${s.total_messages||0}</div><div class="card-label">Messages (24h)</div></div>
        <div class="card"><div class="card-value">${s.reasoning_entries||0}</div><div class="card-label">Reasoning Steps</div></div>
        <div class="card"><div class="card-value">${s.fixes_applied||0}/${s.total_fixes||0}</div><div class="card-label">Fixes (7d)</div></div>
        <div class="card"><div class="card-value">${s.total_emails||0}</div><div class="card-label">Emails (7d)</div></div>
      `;

      // Populate agent filter dropdown
      const sel = document.getElementById('comms-agent-filter');
      if (sel && s.agents && sel.options.length <= 1) {
        for (const a of s.agents) {
          const opt = document.createElement('option');
          opt.value = a.id;
          opt.textContent = a.name || a.id;
          sel.appendChild(opt);
        }
      }
    }

    // Content
    const el = document.getElementById('comms-content');
    if (!el) return;

    if (!d.data || d.data.length === 0) {
      el.innerHTML = '<div class="empty-state">No communications yet. Agent logs will appear here as agents run.</div>';
      return;
    }

    if (view === 'timeline') renderCommsTimeline(el, d.data);
    else if (view === 'messages') renderCommsMessages(el, d.data);
    else if (view === 'fixes') renderCommsFixes(el, d.data);
    else if (view === 'emails') renderCommsEmails(el, d.data);
  } catch(e) {
    const el = document.getElementById('comms-content');
    if (el) el.innerHTML = '<div class="empty-state">Failed to load: ' + esc(e.message) + '</div>';
  }
}

function renderCommsTimeline(el, data) {
  const levelIcons = {
    'reasoning': '🧠', 'code-fix': '🔧', 'email': '📧',
    'error': '❌', 'warn': '⚠️', 'info': 'ℹ️', 'debug': '🔍'
  };
  const levelColors = {
    'reasoning': '#a855f7', 'code-fix': '#22c55e', 'email': '#0ea5e9',
    'error': '#ef4444', 'warn': '#f59e0b', 'info': '#64748b', 'debug': '#475569'
  };

  let html = '<div style="display:flex;flex-direction:column;gap:4px">';

  for (const item of data) {
    const ts = item.timestamp ? new Date(item.timestamp).toLocaleString() : '';

    if (item.type === 'message') {
      // Inter-agent message
      html += `<div style="display:flex;gap:12px;padding:8px 12px;background:var(--surface);border:1px solid #1e40af40;border-radius:8px;align-items:flex-start">
        <div style="font-size:1.2rem;flex-shrink:0">💬</div>
        <div style="flex:1;min-width:0">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span style="font-weight:600;color:#60a5fa">${esc(item.from_name)}</span>
            <span style="color:var(--muted)">→</span>
            <span style="font-weight:600;color:#34d399">${esc(item.to_name)}</span>
            <span style="background:#1e40af40;color:#93c5fd;padding:1px 6px;border-radius:4px;font-size:.7rem">${esc(item.msg_type||'')}</span>
            <span style="color:var(--muted);font-size:.75rem;margin-left:auto">${ts}</span>
          </div>
          <div style="margin-top:4px;font-size:.85rem;color:var(--text)">${esc(item.subject||'')}</div>
          ${item.body && Object.keys(item.body).length ? '<div style="margin-top:4px;font-size:.75rem;color:var(--muted);font-family:monospace;white-space:pre-wrap;max-height:80px;overflow:auto">'+esc(JSON.stringify(item.body,null,2))+'</div>' : ''}
        </div>
      </div>`;
    } else {
      // Agent log entry
      const color = levelColors[item.level] || '#64748b';
      const icon = levelIcons[item.level] || '📝';
      const isCode = item.level === 'code-fix' || item.category === 'code-response';
      const isReasoning = item.level === 'reasoning';

      html += `<div style="display:flex;gap:12px;padding:8px 12px;background:var(--surface);border-left:3px solid ${color};border-radius:0 8px 8px 0;align-items:flex-start;${isReasoning?'background:rgba(168,85,247,0.05)':''}">
        <div style="font-size:1.2rem;flex-shrink:0">${icon}</div>
        <div style="flex:1;min-width:0">
          <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
            <span style="font-weight:600;color:${color}">${esc(item.agent_name||item.agent_id)}</span>
            <span style="background:${color}30;color:${color};padding:1px 6px;border-radius:4px;font-size:.7rem">${esc(item.level)}</span>
            <span style="color:var(--muted);font-size:.7rem">${esc(item.category||'')}</span>
            ${item.run_id ? '<span style="color:var(--muted);font-size:.7rem">Run #'+item.run_id+'</span>' : ''}
            <span style="color:var(--muted);font-size:.75rem;margin-left:auto">${ts}</span>
          </div>
          <div style="margin-top:4px;font-size:.85rem;color:var(--text);${isCode?'font-family:monospace;':''}">${esc(item.message)}</div>
          ${item.context && Object.keys(item.context).length > 0 ?
            '<details style="margin-top:4px"><summary style="font-size:.75rem;color:var(--muted);cursor:pointer">Context</summary><pre style="font-size:.7rem;color:var(--muted);margin-top:4px;max-height:200px;overflow:auto;white-space:pre-wrap">'+esc(JSON.stringify(item.context,null,2))+'</pre></details>'
            : ''}
        </div>
      </div>`;
    }
  }

  html += '</div>';
  el.innerHTML = html;
}

function renderCommsMessages(el, data) {
  if (!data.length) { el.innerHTML = '<div class="empty-state">No messages</div>'; return; }
  let html = '<table class="tbl"><thead><tr><th>Time</th><th>From</th><th>To</th><th>Type</th><th>Subject</th><th>Priority</th><th>Status</th></tr></thead><tbody>';
  for (const m of data) {
    const ts = m.timestamp ? new Date(m.timestamp).toLocaleTimeString() : '';
    const pColor = m.priority <= 3 ? '#ef4444' : m.priority <= 5 ? '#f59e0b' : '#64748b';
    html += `<tr>
      <td style="font-size:.75rem">${ts}</td>
      <td style="font-weight:600;color:#60a5fa">${esc(m.from_name)}</td>
      <td style="font-weight:600;color:#34d399">${esc(m.to_name)}</td>
      <td><span style="background:var(--surface);padding:2px 6px;border-radius:4px;font-size:.75rem">${esc(m.msg_type)}</span></td>
      <td>${esc(m.subject||'')}</td>
      <td style="color:${pColor}">${m.priority}</td>
      <td>${m.status==='read'?'✓':'⏳'} ${esc(m.status)}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}

function renderCommsFixes(el, data) {
  if (!data.length) { el.innerHTML = '<div class="empty-state">No code fixes recorded yet</div>'; return; }
  let html = '<div style="display:flex;flex-direction:column;gap:8px">';
  for (const f of data) {
    const ts = f.timestamp ? new Date(f.timestamp).toLocaleString() : '';
    const statusColor = f.applied ? (f.verified ? '#22c55e' : '#f59e0b') : '#ef4444';
    const statusLabel = f.applied ? (f.verified ? '✅ Applied & Verified' : '⚡ Applied') : '❌ Failed';
    const patch = typeof f.patch === 'object' ? f.patch : {};

    html += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
        <div>
          <span style="font-weight:600;color:var(--cyan)">${esc(f.target_file||'')}</span>
          <span style="background:${statusColor}30;color:${statusColor};padding:2px 8px;border-radius:4px;font-size:.75rem;margin-left:8px">${statusLabel}</span>
        </div>
        <div style="font-size:.75rem;color:var(--muted)">${ts} · ${esc(f.model_used||'unknown')} · ${esc(f.fix_type||'')}</div>
      </div>
      <div style="font-size:.85rem;color:var(--text);margin-bottom:8px"><strong>Diagnosis:</strong> ${esc(f.diagnosis||'')}</div>
      ${f.original_error ? '<details style="margin-bottom:8px"><summary style="font-size:.8rem;color:#ef4444;cursor:pointer">Original Error</summary><pre style="font-size:.7rem;color:#fca5a5;margin-top:4px;max-height:120px;overflow:auto;white-space:pre-wrap;background:#1c1917;padding:8px;border-radius:4px">'+esc(f.original_error)+'</pre></details>' : ''}
      ${patch.old ? '<div style="margin-bottom:4px"><span style="font-size:.75rem;color:#ef4444;font-weight:600">− Old Code:</span><pre style="font-size:.7rem;color:#fca5a5;background:#1c1917;padding:8px;border-radius:4px;max-height:100px;overflow:auto;white-space:pre-wrap">'+esc(patch.old)+'</pre></div>' : ''}
      ${patch.new ? '<div><span style="font-size:.75rem;color:#22c55e;font-weight:600">+ New Code:</span><pre style="font-size:.7rem;color:#86efac;background:#0a1f0a;padding:8px;border-radius:4px;max-height:100px;overflow:auto;white-space:pre-wrap">'+esc(patch.new)+'</pre></div>' : ''}
    </div>`;
  }
  html += '</div>';
  el.innerHTML = html;
}

function renderCommsEmails(el, data) {
  if (!data.length) { el.innerHTML = '<div class="empty-state">No email notifications sent yet</div>'; return; }
  let html = '<table class="tbl"><thead><tr><th>Time</th><th>Agent</th><th>Subject</th><th>Recipient</th><th>Status</th></tr></thead><tbody>';
  for (const e of data) {
    const ts = e.timestamp ? new Date(e.timestamp).toLocaleString() : '';
    const stColor = e.status==='sent'?'#22c55e':e.status==='failed'?'#ef4444':'#f59e0b';
    html += `<tr>
      <td style="font-size:.75rem">${ts}</td>
      <td>${esc(e.agent_id)}</td>
      <td>${esc(e.subject||'')}</td>
      <td>${esc(e.recipient)}</td>
      <td style="color:${stColor}">${e.status==='sent'?'✓':e.status==='failed'?'✗':'⏳'} ${esc(e.status)}</td>
    </tr>`;
    if (e.error) html += `<tr><td colspan="5" style="font-size:.7rem;color:#ef4444;padding-left:40px">${esc(e.error)}</td></tr>`;
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}



// ═══ AGENT CHAT ROOM ══════════════════════════════════════════
let crChannel = 'general';
let crMessages = [];
let crChannels = [];
let crLastId = 0;
let crReplyTo = null;
let crPollTimer = null;

const AGENT_INITIALS = {
  'orchestrator':'OR','watchdog':'WD','code-doctor':'CD','security-sentinel':'SS',
  'self-healing':'SH','data-quality':'DQ','dba':'DB','athena':'AT',
  'case-strategy':'CS','retention':'RT','query-insight':'QI','quality-eval':'QE',
  'daily-digest':'DD'
};

const AGENT_NAMES = {
  'orchestrator':'Orchestrator','watchdog':'Watchdog','code-doctor':'Code Doctor',
  'security-sentinel':'Security Sentinel','self-healing':'Self Healing',
  'data-quality':'Data Quality','dba':'DBA','athena':'Athena',
  'case-strategy':'Case Strategy','retention':'Retention',
  'query-insight':'Query Insight','quality-eval':'Quality Eval',
  'daily-digest':'Daily Digest'
};

const CH_ICONS = {
  'general':'💬','alerts':'🚨','security':'🛡️','legal':'⚖️',
  'medical':'🏥','ops':'⚙️','code-fixes':'🔧'
};


function crAgentClass(agent) {
  if (!agent) return 'agent-system';
  const base = agent.replace(/^human:?/, 'human').split(':')[0];
  return 'agent-' + (base || 'system');
}

function crAgentInitials(agent) {
  if (!agent) return '?';
  if (agent.startsWith('human')) {
    const u = agent.split(':')[1] || 'H';
    return u.substring(0,2).toUpperCase();
  }
  return AGENT_INITIALS[agent] || agent.substring(0,2).toUpperCase();
}

function crAgentName(agent) {
  if (!agent) return 'System';
  if (agent.startsWith('human')) {
    const u = agent.split(':')[1] || 'human';
    return '👤 ' + u;
  }
  return AGENT_NAMES[agent] || agent;
}

function crFormatContent(text) {
  if (!text) return '';
  let s = esc(text);
  // Highlight @mentions
  s = s.replace(/@([\w-]+)/g, '<span class="mention" onclick="crMention(&#39;$1&#39;)">@$1</span>');
  // Code blocks
  s = s.replace(/```([\s\S]*?)```/g, '<pre style="background:rgba(0,0,0,.2);padding:8px;border-radius:6px;margin:4px 0;overflow-x:auto;font-size:.8rem">$1</pre>');
  // Inline code
  s = s.replace(/`([^`]+)`/g, '<code style="background:rgba(0,0,0,.2);padding:1px 4px;border-radius:3px;font-size:.82rem">$1</code>');
  // URLs
  s = s.replace(/(https?:\/\/[^\s<]+)/g, '<a href="$1" target="_blank" style="color:var(--accent)">$1</a>');
  // Newlines
  s = s.replace(/\n/g, '<br>');
  return s;
}

function crTypeBadge(type) {
  if (!type || type === 'chat') return '';
  const cls = 'type-' + type;
  return `<span class="msg-type-badge ${cls}">${type}</span>`;
}

function crTimeStr(ts) {
  const d = new Date(ts);
  const now = new Date();
  const hm = d.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
  if (d.toDateString() === now.toDateString()) return hm;
  return d.toLocaleDateString([], {month:'short', day:'numeric'}) + ' ' + hm;
}

function crDateStr(ts) {
  const d = new Date(ts);
  const now = new Date();
  if (d.toDateString() === now.toDateString()) return 'Today';
  const y = new Date(now); y.setDate(y.getDate()-1);
  if (d.toDateString() === y.toDateString()) return 'Yesterday';
  return d.toLocaleDateString([], {weekday:'long', month:'short', day:'numeric'});
}

async function loadChatRoom() {
  // Load channels and stats in parallel
  try {
    const [chResp, stResp] = await Promise.all([
      fetch('/api/agents/chatroom/channels', {credentials:'include'}),
      fetch('/api/agents/chatroom/stats', {credentials:'include'})
    ]);
    if (chResp.ok) {
      crChannels = await chResp.json();
      crRenderChannels();
    }
    if (stResp.ok) {
      const st = await stResp.json();
      document.getElementById('cr-stat-total').textContent = st.total_messages + ' total';
      document.getElementById('cr-stat-24h').textContent = st.messages_24h + ' today';

      // Online agents
      const onEl = document.getElementById('cr-agents-online');
      if (st.recent_agents && st.recent_agents.length) {
        onEl.innerHTML = st.recent_agents.map(a =>
          `<div class="agent-tag"><span class="online-dot" style="background:#10b981"></span>${esc(a.agent)}</div>`
        ).join('');
      } else {
        onEl.innerHTML = '<span style="font-size:.7rem;color:var(--muted)">No recent activity</span>';
      }
    }
  } catch(e) { console.error('loadChatRoom', e); }

  // Load messages for current channel
  await crLoadMessages();

  // Start polling
  crStartPoll();
}

function crRenderChannels() {
  const el = document.getElementById('cr-channels');
  el.innerHTML = crChannels.map(c => {
    const icon = c.icon || CH_ICONS[c.name] || '#';
    const active = c.name === crChannel ? ' active' : '';
    const badge = c.message_count > 0 ? `<span class="ch-badge">${c.message_count}</span>` : '';
    return `<div class="channel-item${active}" onclick="crSwitchChannel('${c.name}')">
      <span class="ch-icon">${icon}</span>
      <span class="ch-name">#${esc(c.name)}</span>
      ${badge}
    </div>`;
  }).join('');
}

async function crLoadMessages(append) {
  try {
    let url = `/api/agents/chatroom/messages?channel=${crChannel}&limit=100`;
    if (append && crLastId) url += `&since_id=${crLastId}`;
    const resp = await fetch(url, {credentials:'include'});
    if (!resp.ok) return;
    const msgs = await resp.json();

    if (append) {
      // Only add new messages
      const existingIds = new Set(crMessages.map(m => m.id));
      const newMsgs = msgs.filter(m => !existingIds.has(m.id));
      if (newMsgs.length > 0) {
        crMessages.push(...newMsgs);
        crRenderMessages(true);
      }
    } else {
      crMessages = msgs;
      crRenderMessages(false);
    }

    if (crMessages.length) {
      crLastId = Math.max(...crMessages.map(m => m.id));
    }
  } catch(e) { console.error('crLoadMessages', e); }
}

function crRenderMessages(scrollToBottom) {
  const el = document.getElementById('cr-messages');
  if (!crMessages.length) {
    el.innerHTML = `<div class="chatroom-empty">
      <div class="big-icon">💬</div>
      <div>No messages in #${esc(crChannel)} yet</div>
    </div>`;
    return;
  }

  let html = '';
  let lastDate = '';
  for (const m of crMessages) {
    // Date separator
    const dateStr = crDateStr(m.ts);
    if (dateStr !== lastDate) {
      html += `<div class="chat-date-sep"><span>${dateStr}</span></div>`;
      lastDate = dateStr;
    }

    const avatarClass = crAgentClass(m.from);
    const initials = crAgentInitials(m.from);
    const name = crAgentName(m.from);
    const time = crTimeStr(m.ts);
    const typeBadge = crTypeBadge(m.type);
    const content = crFormatContent(m.content);

    // Reply reference
    let replyRef = '';
    if (m.reply_to) {
      const parent = crMessages.find(p => p.id === m.reply_to);
      if (parent) {
        replyRef = `<div class="msg-reply-ref" onclick="crScrollToMsg(${m.reply_to})">↩ ${esc(crAgentName(parent.from))}: ${esc((parent.content||'').substring(0,80))}…</div>`;
      } else {
        replyRef = `<div class="msg-reply-ref">↩ Reply to #${m.reply_to}</div>`;
      }
    }

    html += `<div class="chat-msg" id="cr-msg-${m.id}" data-id="${m.id}">
      <div class="msg-avatar ${avatarClass}">${initials}</div>
      <div class="msg-body">
        ${replyRef}
        <div class="msg-header">
          <span class="msg-from">${esc(name)}</span>
          ${typeBadge}
          <span class="msg-time">${time}</span>
        </div>
        <div class="msg-content">${content}</div>
      </div>
      <div class="msg-actions">
        <button onclick="crSetReply(${m.id})" title="Reply">↩</button>
      </div>
    </div>`;
  }

  el.innerHTML = html;

  // Auto-scroll to bottom
  if (scrollToBottom !== false) {
    el.scrollTop = el.scrollHeight;
  } else {
    // Initial load: scroll to bottom
    requestAnimationFrame(() => { el.scrollTop = el.scrollHeight; });
  }
}

function crSwitchChannel(name) {
  crChannel = name;
  crMessages = [];
  crLastId = 0;
  crCancelReply();
  crRenderChannels();

  // Update header
  const ch = crChannels.find(c => c.name === name);
  document.getElementById('cr-ch-icon').textContent = ch?.icon || CH_ICONS[name] || '#';
  document.getElementById('cr-ch-name').textContent = '#' + name;
  document.getElementById('cr-ch-desc').textContent = ch?.description || '';

  crLoadMessages();
}

async function crSend() {
  const input = document.getElementById('cr-input');
  const text = input.value.trim();
  if (!text) return;

  const btn = document.getElementById('cr-send-btn');
  btn.disabled = true;
  input.disabled = true;

  try {
    const body = {channel: crChannel, content: text};
    if (crReplyTo) body.reply_to = crReplyTo;

    const resp = await fetch('/api/agents/chatroom/post', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      credentials: 'include',
      body: JSON.stringify(body)
    });

    if (resp.ok) {
      input.value = '';
      input.style.height = 'auto';
      crCancelReply();
      await crLoadMessages(true);
    } else {
      const err = await resp.json();
      alert('Send failed: ' + (err.error || 'Unknown error'));
    }
  } catch(e) {
    alert('Send error: ' + e.message);
  } finally {
    btn.disabled = false;
    input.disabled = false;
    input.focus();
  }
}

function crSetReply(msgId) {
  const m = crMessages.find(m => m.id === msgId);
  if (!m) return;
  crReplyTo = msgId;
  document.getElementById('cr-reply-preview').style.display = 'flex';
  document.getElementById('cr-reply-from').textContent = crAgentName(m.from);
  document.getElementById('cr-reply-text').textContent = (m.content||'').substring(0, 100);
  document.getElementById('cr-input').focus();
}

function crCancelReply() {
  crReplyTo = null;
  document.getElementById('cr-reply-preview').style.display = 'none';
}

function crScrollToMsg(msgId) {
  const el = document.getElementById('cr-msg-' + msgId);
  if (el) {
    el.scrollIntoView({behavior:'smooth', block:'center'});
    el.style.background = 'rgba(99,102,241,.15)';
    setTimeout(() => { el.style.background = ''; }, 2000);
  }
}

function crMention(agent) {
  const input = document.getElementById('cr-input');
  input.value += (input.value ? ' ' : '') + '@' + agent + ' ';
  input.focus();
}

function crRefresh() {
  crMessages = [];
  crLastId = 0;
  loadChatRoom();
}

function crStartPoll() {
  if (crPollTimer) clearInterval(crPollTimer);
  crPollTimer = setInterval(() => {
    // Only poll if chatroom tab is visible
    const tab = document.getElementById('tab-chatroom');
    if (tab && !tab.classList.contains('hidden')) {
      crLoadMessages(true);
    }
  }, 5000);
}

// Hook into switchTab
// Chat Room: extend original switchTab to handle chatroom tab
// (chatroom tab loading is handled by original switchTab via loaders map patch below)



</script>
</body></html>"""


if __name__ == "__main__":
    main()