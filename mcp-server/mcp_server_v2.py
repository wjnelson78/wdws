#!/usr/bin/env python3
"""
Athena Cognitive Platform MCP Server v2 — PostgreSQL + OAuth 2.0

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
  • Athena AI       — own client_id / client_secret

Transport:
  stdio:  /opt/wdws/venv/bin/python3 mcp_server_v2.py
  http:   /opt/wdws/venv/bin/python3 mcp_server_v2.py --http --port 9200
"""

import os
import subprocess
import sys
import json
import asyncio
import base64
import email as email_lib
import html
import re
import hashlib
import logging
import mimetypes
import secrets
import shutil
import time
import uuid
import xml.etree.ElementTree as ET
import zipfile
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, timezone
from email import policy
from pathlib import Path
from typing import Any, AsyncIterator, Optional, Sequence
from urllib.parse import quote

import asyncpg
import httpx
from pydantic import AnyUrl
from cryptography.fernet import Fernet, InvalidToken
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse, Response

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
sys.path.insert(0, "/opt/wdws")
try:
    from athena_logging import setup_logging, get_logger
    setup_logging("mcp-server")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    log = logging.getLogger("athena-mcp")
from agents.email_util import (
    build_notification_html as build_alert_notification_html,
    send_email as send_alert_email,
)

# ── Config ───────────────────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")
GRAPH_CLIENT_ID = os.getenv("GRAPH_CLIENT_ID", "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
GRAPH_TENANT_ID = os.getenv("GRAPH_TENANT_ID", "")
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_TOKEN_URL = f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token" if GRAPH_TENANT_ID else ""
DEFAULT_ATHENA_MAILBOX = (os.getenv("GRAPH_SENDER_EMAIL", "athena@seattleseahawks.me") or "athena@seattleseahawks.me").strip()
ATHENA_ALERT_EMAIL = (os.getenv("ATHENA_ALERT_EMAIL", DEFAULT_ATHENA_MAILBOX) or DEFAULT_ATHENA_MAILBOX).strip()
ATHENA_EMAIL_DRAFT_MODEL = os.getenv("ATHENA_EMAIL_DRAFT_MODEL", os.getenv("AGENT_LLM_MODEL_HIGH", "gpt-5.4"))
GRAPH_ATTACHMENT_SIMPLE_LIMIT = 3 * 1024 * 1024
GRAPH_ATTACHMENT_UPLOAD_CHUNK_SIZE = 320 * 1024 * 10
from embedding_service import (
    embed_query_sync, embed_texts_sync, EMBEDDING_DIMENSIONS, EMBEDDING_MODEL,
    _vec_literal, rerank,
)

# Sprint A Task 3: retrieval-safety helper for privilege + PHI filtering.
# Primary-surface tools propagated in this pass: search, fetch, semantic_search,
# fulltext_search, search_emails, search_by_case, case_timeline, search_medical,
# search_by_date_range, fetch_document. Secondary reads (helpers at 775/834/994,
# write-path reads at 4900/5752, document-relations at 5786/5794, etc.) tracked
# in the T3_REVIEW manifest — see SPRINT_A_WORK_ORDER_v2.2.md and the
# ops.pending_approvals row for sprint_a_t3_review.
import sys as _sys
_sys.path.insert(0, "/opt/wdws")
from core_safety import (  # noqa: E402
    build_document_safety_filter,
    fetch_safe_document,
    PrivilegeDeniedException,
    PHIAccessDeniedException,
)


def _default_include_privileged() -> bool:
    """First-party clients (claude-desktop, chatgpt) get default access to
    attorney_client and work_product_fact-classified documents. Third-party
    or anonymous callers must opt-in explicitly via include_privileged=True.
    Anything in LEGAL_PRIVILEGED_EXTENDED (work_product_opinion, joint_defense,
    common_interest) still requires explicit privileged_categories opt-in.
    """
    return _current_client_id() in FULL_SCOPE_FIRST_PARTY_CLIENT_IDS


def _mcp_caller_context(tool_name: str, extra: Optional[dict] = None) -> dict:
    """Build caller_context for Sprint A retrieval-safety logging.

    The MCP auth layer gives us client_id via _current_client_id() but no
    session UUID (MCP's session model doesn't map cleanly to a uuid-typed
    column yet). We pass None for session_id; correlation across MCP calls
    can be added in a follow-up if the audit trail warrants it.
    """
    ctx = {
        'tool': f"mcp.{tool_name}",
        'agent_id': _current_client_id(),
        'session_id': None,
    }
    if extra:
        ctx.update(extra)
    return ctx
EMBEDDING_DIMS = EMBEDDING_DIMENSIONS   # 1024 (BGE-M3 local)
EMBEDDING_PROVIDER = (os.getenv("EMBEDDING_PROVIDER", "local") or "local").strip()
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/opt/wdws/data/uploads/claude-desktop"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
TRANSFER_DIR = Path(os.getenv("TRANSFER_DIR", "/opt/wdws/data/transfers"))
TRANSFER_UPLOAD_DIR = TRANSFER_DIR / "upload"
TRANSFER_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
TRANSFER_SESSION_FILE_LIMIT = int(os.getenv("TRANSFER_SESSION_FILE_LIMIT", str(100 * 1024 * 1024)))
TRANSFER_UPLOAD_SESSION_TTL_SECONDS = int(os.getenv("TRANSFER_UPLOAD_SESSION_TTL_SECONDS", "3600"))
TRANSFER_DOWNLOAD_SESSION_TTL_SECONDS = int(os.getenv("TRANSFER_DOWNLOAD_SESSION_TTL_SECONDS", "900"))
TRANSFER_UPLOAD_MAX_CHUNK_SIZE = int(os.getenv("TRANSFER_UPLOAD_MAX_CHUNK_SIZE", str(4 * 1024 * 1024)))
SOFFICE_BIN = os.getenv("SOFFICE_BIN", "/usr/bin/soffice")
DOCX_CONVERT_TIMEOUT_SECONDS = int(os.getenv("DOCX_CONVERT_TIMEOUT_SECONDS", "180"))
CONVERTED_DIR = Path(os.getenv("CONVERTED_DIR", str(Path(os.getenv("UPLOAD_DIR", "/opt/wdws/data/uploads/claude-desktop")) / "_converted")))
CONVERTED_DIR.mkdir(parents=True, exist_ok=True)
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
OCR_DPI = 150
BASE_URL = os.getenv("MCP_BASE_URL", "https://klunky.12432.net")
OAUTH_CLIENT_SECRET_KEY = os.getenv("OAUTH_CLIENT_SECRET_KEY", "").strip()
RAG_SEMANTIC_WEIGHT_DEFAULT = float(os.getenv("RAG_SEMANTIC_WEIGHT", "0.7"))
RAG_FULLTEXT_WEIGHT_DEFAULT = float(os.getenv("RAG_FULLTEXT_WEIGHT", "0.3"))
RAG_NEIGHBOR_DEFAULT = int(os.getenv("RAG_NEIGHBOR_DEFAULT", "1"))
RAG_NEIGHBOR_MAX = int(os.getenv("RAG_NEIGHBOR_MAX", "3"))
MCP_ENDPOINT_PATH = "/mcp/sse"

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
FULL_SCOPE_FIRST_PARTY_CLIENT_IDS = {"claude-desktop", "chatgpt"}

if OAUTH_CLIENT_SECRET_KEY:
    log.info("OAuth client secret encryption enabled")
else:
    log.warning("OAUTH_CLIENT_SECRET_KEY not set; client secrets will only be read from env")

# ── Connection Pool ──────────────────────────────────────────
pool: Optional[asyncpg.Pool] = None
_graph_access_token: Optional[str] = None
_graph_token_expires_at: float = 0.0
_transfer_schema_ready = False
_embedding_model_id_cache: dict[tuple[str, int], int] = {}

# ── Schema Alias Resolution ─────────────────────────────────
# Transparently rewrites column name variations (filed_date → date_filed, etc.)
try:
    from schema_aliases import normalize_sql as _normalize_sql
    log.info("Schema alias resolution loaded — column name variations handled transparently")
except ImportError:
    _normalize_sql = None
    log.warning("schema_aliases module not found — column name normalization disabled")


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


async def _fetch(sql: str, *args) -> list:
    """Execute a read query with transparent column alias resolution."""
    if _normalize_sql:
        sql = _normalize_sql(sql, log_rewrites=True)
    p = await get_pool()
    return await p.fetch(sql, *args)


async def _execute(sql: str, *args):
    """Execute a write query with transparent column alias resolution."""
    if _normalize_sql:
        sql = _normalize_sql(sql, log_rewrites=True)
    p = await get_pool()
    return await p.execute(sql, *args)


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


def _parse_date_param(s: str) -> datetime:
    # asyncpg infers param type from SQL casts ($1::timestamptz / $1::date) and
    # rejects str even though postgres would coerce. Pass a real datetime.
    d = date.fromisoformat(s[:10])
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def _row_dict(row, keys=None) -> dict:
    if keys:
        return {k: _ser(row[k]) for k in keys if k in row.keys()}
    return {k: _ser(v) for k, v in dict(row).items()}


def _guess_mime_type(filename: Optional[str], fallback: str = "application/octet-stream") -> str:
    if filename:
        guessed, _ = mimetypes.guess_type(filename)
        if guessed:
            return guessed
    return fallback


async def _get_graph_access_token() -> str:
    global _graph_access_token, _graph_token_expires_at

    if not all([GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_TENANT_ID, GRAPH_TOKEN_URL]):
        raise RuntimeError("Graph credentials are not configured for MCP downloads")

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


def _require_scope(required_scope: str) -> None:
    token_info = _get_auth_token()
    if token_info is None:
        raise PermissionError("Authenticated OAuth access is required")

    scopes = getattr(token_info, "scopes", None) or []
    if isinstance(scopes, str):
        scopes = scopes.split()
    normalized = {str(scope).lower() for scope in scopes}
    if "admin" in normalized:
        return
    if required_scope.lower() not in normalized:
        raise PermissionError(
            f"This tool requires OAuth scope '{required_scope}'. Re-authorize the MCP client with write access."
        )


def _current_client_id() -> str:
    try:
        token_info = _get_auth_token()
        return token_info.client_id if token_info else "anonymous"
    except Exception:
        return "anonymous"


def _normalize_scope_list(scopes: Optional[Sequence[str]]) -> list[str]:
    normalized: list[str] = []
    for scope in scopes or []:
        value = str(scope).strip().lower()
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def _registered_client_scopes(client: OAuthClientInformationFull) -> list[str]:
    return _normalize_scope_list((client.scope or "").split()) or ["read"]


def _granted_scopes_for_client(
    client: OAuthClientInformationFull,
    requested_scopes: Optional[Sequence[str]],
) -> list[str]:
    """Resolve the scopes that should be embedded into a token.

    Claude.ai / Claude Desktop and ChatGPT currently reconnect through the MCP
    OAuth flow asking for only the minimum `read` scope, even when the Athena
    client registration explicitly allows richer trusted scopes. That causes
    previously working write tools like `send_mail_message(...)` to fail after a
    reconnect. For these first-party clients, always issue their full registered
    scope set so reconnects preserve historical behavior.
    """
    registered_scopes = _registered_client_scopes(client)
    if client.client_id in FULL_SCOPE_FIRST_PARTY_CLIENT_IDS:
        return registered_scopes

    requested = _normalize_scope_list(requested_scopes)
    if not requested:
        return registered_scopes

    granted = [scope for scope in requested if scope in registered_scopes]
    return granted or registered_scopes


def _normalize_mailbox(mailbox: Optional[str]) -> str:
    value = (mailbox or DEFAULT_ATHENA_MAILBOX or "athena@seattleseahawks.me").strip().lower()
    if not value or "@" not in value:
        raise ValueError("A valid mailbox email address is required")
    return value


def _split_csv_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    else:
        items = str(value).split(",")
    return [str(item).strip() for item in items if str(item).strip()]


def _recipient_objects(value: Any) -> list[dict[str, dict[str, str]]]:
    return [{"emailAddress": {"address": addr}} for addr in _split_csv_list(value)]


def _normalize_importance(value: Optional[str], *, default: str = "normal") -> str:
    normalized = (value or default).strip().lower()
    if normalized not in {"low", "normal", "high"}:
        raise ValueError("importance must be one of: low, normal, high")
    return normalized.capitalize()


def _html_to_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _text_to_html(value: str) -> str:
    parts = [p.strip() for p in str(value).replace("\r\n", "\n").split("\n\n") if p.strip()]
    if not parts:
        return "<p></p>"
    rendered = []
    for part in parts:
        escaped = html.escape(part).replace("\n", "<br>")
        rendered.append(f"<p>{escaped}</p>")
    return "\n".join(rendered)


def _prepare_email_body(body: str, body_content_type: str = "html") -> tuple[str, str, str]:
    content_type = (body_content_type or "html").strip().lower()
    if content_type not in {"html", "text"}:
        raise ValueError("body_content_type must be 'html' or 'text'")

    raw = (body or "").strip()
    if content_type == "html":
        body_html = raw if re.search(r"<[^>]+>", raw) else _text_to_html(raw)
        body_text = _html_to_text(body_html)
        return body_html, body_text, "HTML"

    body_text = raw
    body_html = _text_to_html(raw)
    return body_html, body_text, "Text"


from graph_mail import (
    build_rfc5322_mime as _build_rfc5322_mime,
    graph_send_raw_mime as _graph_send_raw_mime_shared,
)


_mailbox_display_name_cache: dict[str, str] = {}


async def _get_mailbox_display_name(mailbox: str) -> str:
    cached = _mailbox_display_name_cache.get(mailbox)
    if cached is not None:
        return cached
    try:
        info = await _graph_request("GET", f"{GRAPH_BASE_URL}/users/{mailbox}?$select=displayName")
        name = (info.get("displayName") or "").strip()
    except Exception:
        name = ""
    _mailbox_display_name_cache[mailbox] = name
    return name


async def _graph_send_raw_mime(mailbox: str, mime_bytes: bytes) -> None:
    token = await _get_graph_access_token()
    await _graph_send_raw_mime_shared(token, mailbox, mime_bytes)


async def _graph_request(
    method: str,
    url: str,
    *,
    json_body: Optional[dict] = None,
    data: Any = None,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: float = 60.0,
) -> dict:
    token = await _get_graph_access_token()
    request_headers = {"Authorization": f"Bearer {token}"}
    if headers:
        request_headers.update(headers)

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.request(
            method,
            url,
            headers=request_headers,
            json=json_body,
            data=data,
            params=params,
        )
        resp.raise_for_status()
        if not resp.content:
            return {}
        content_type = resp.headers.get("content-type", "")
        if "application/json" in content_type.lower():
            return resp.json()
        return {"raw_text": resp.text}


async def _graph_create_draft(mailbox: str, message_payload: dict) -> dict:
    return await _graph_request(
        "POST",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages",
        json_body=message_payload,
        headers={"Content-Type": "application/json"},
    )


async def _graph_patch_message(mailbox: str, message_id: str, payload: dict) -> dict:
    return await _graph_request(
        "PATCH",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}",
        json_body=payload,
        headers={"Content-Type": "application/json"},
    )


async def _graph_send_draft(mailbox: str, message_id: str) -> None:
    await _graph_request(
        "POST",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/send",
        headers={"Content-Length": "0"},
    )


async def _graph_add_small_attachment(mailbox: str, message_id: str, attachment: dict) -> dict:
    payload = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": attachment["filename"],
        "contentType": attachment["mime_type"],
        "contentBytes": base64.b64encode(attachment["data"]).decode("ascii"),
    }
    return await _graph_request(
        "POST",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/attachments",
        json_body=payload,
        headers={"Content-Type": "application/json"},
        timeout=120.0,
    )


async def _graph_add_large_attachment(mailbox: str, message_id: str, attachment: dict) -> dict:
    session = await _graph_request(
        "POST",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{message_id}/attachments/createUploadSession",
        json_body={
            "AttachmentItem": {
                "attachmentType": "file",
                "name": attachment["filename"],
                "size": len(attachment["data"]),
            }
        },
        headers={"Content-Type": "application/json"},
        timeout=120.0,
    )

    upload_url = session.get("uploadUrl")
    if not upload_url:
        raise RuntimeError(f"Graph upload session did not return uploadUrl for {attachment['filename']}")

    data = attachment["data"]
    final_response = None
    async with httpx.AsyncClient(timeout=300.0) as client:
        start = 0
        total = len(data)
        while start < total:
            end = min(start + GRAPH_ATTACHMENT_UPLOAD_CHUNK_SIZE, total) - 1
            chunk = data[start:end + 1]
            final_response = await client.put(
                upload_url,
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{total}",
                },
                content=chunk,
            )
            if final_response.status_code not in (200, 201, 202):
                raise RuntimeError(
                    f"Graph large attachment upload failed for {attachment['filename']}: "
                    f"HTTP {final_response.status_code} {final_response.text[:300]}"
                )
            start = end + 1

    return {
        "name": attachment["filename"],
        "size": len(data),
        "upload": "session",
        "location": final_response.headers.get("Location") if final_response else None,
    }


async def _resolve_document_attachments(attachment_document_ids: Any) -> list[dict]:
    attachments = []
    for document_id in _split_csv_list(attachment_document_ids):
        info, payload = await _load_original_document_bytes(document_id)
        filename = info.get("filename") or f"document-{document_id}.bin"
        mime_type = info.get("mime_type") or _guess_mime_type(filename)
        attachments.append({
            "document_id": document_id,
            "filename": filename,
            "mime_type": mime_type,
            "data": payload,
            "source_kind": info.get("source_kind"),
            "original_available": info.get("original_available", False),
        })
    return attachments


async def _attach_documents_to_message(mailbox: str, message_id: str, attachment_document_ids: Any) -> list[dict]:
    attachments = await _resolve_document_attachments(attachment_document_ids)
    uploaded = []
    for attachment in attachments:
        if len(attachment["data"]) < GRAPH_ATTACHMENT_SIMPLE_LIMIT:
            result = await _graph_add_small_attachment(mailbox, message_id, attachment)
            uploaded.append({
                "document_id": attachment["document_id"],
                "filename": attachment["filename"],
                "size": len(attachment["data"]),
                "upload": "simple",
                "attachment_id": result.get("id"),
            })
        else:
            result = await _graph_add_large_attachment(mailbox, message_id, attachment)
            uploaded.append({
                "document_id": attachment["document_id"],
                "filename": attachment["filename"],
                "size": len(attachment["data"]),
                "upload": result.get("upload", "session"),
                "attachment_location": result.get("location"),
            })
    return uploaded


def _message_summary(message: dict) -> dict:
    return {
        "message_id": message.get("id"),
        "internet_message_id": message.get("internetMessageId"),
        "conversation_id": message.get("conversationId"),
        "subject": message.get("subject"),
        "importance": message.get("importance"),
        "is_read": message.get("isRead"),
        "is_draft": message.get("isDraft"),
        "has_attachments": message.get("hasAttachments"),
        "is_read_receipt_requested": message.get("isReadReceiptRequested"),
        "is_delivery_receipt_requested": message.get("isDeliveryReceiptRequested"),
        "received_at": message.get("receivedDateTime"),
        "sent_at": message.get("sentDateTime"),
        "body_preview": message.get("bodyPreview"),
        "web_link": message.get("webLink"),
        "from": message.get("from"),
        "to_recipients": message.get("toRecipients", []),
        "cc_recipients": message.get("ccRecipients", []),
        "bcc_recipients": message.get("bccRecipients", []),
        "reply_to": message.get("replyTo", []),
        "categories": message.get("categories", []),
    }


async def _create_reply_draft(mailbox: str, message_id: str, *, reply_all: bool) -> dict:
    graph_message_id = await _graph_resolve_message_graph_id(mailbox, message_id)
    action = "createReplyAll" if reply_all else "createReply"
    draft = await _graph_request(
        "POST",
        f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_message_id}/{action}",
        headers={"Content-Length": "0"},
    )
    if not draft.get("id"):
        raise RuntimeError(f"Graph did not return a draft ID for {action}")
    return draft


async def _draft_email_completion(prompt: str, tone: str, recipient_name: Optional[str]) -> dict:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is required to draft emails")

    system_prompt = (
        "You are Athena AI drafting polished, well-formatted professional email messages. "
        "Return JSON only with keys subject, body_html, body_text, tone_used, and checklist. "
        "Use clean HTML paragraphs and bullet lists where appropriate. "
        "Keep the message concise, professional, and action-oriented."
    )
    recipient_line = recipient_name or "the recipient"
    user_prompt = (
        f"Tone: {tone}\n"
        f"Recipient: {recipient_line}\n"
        f"Mailbox sender: {DEFAULT_ATHENA_MAILBOX}\n"
        f"Draft purpose and key details:\n{prompt}\n\n"
        "Respond with valid JSON only."
    )

    async with httpx.AsyncClient(timeout=90.0) as client:
        resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={
                "model": ATHENA_EMAIL_DRAFT_MODEL,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.3,
                "response_format": {"type": "json_object"},
                "max_tokens": 1200,
            },
        )
        resp.raise_for_status()
        return json.loads(resp.json()["choices"][0]["message"]["content"])


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


def _extract_all_mime_attachments(mime_data: bytes) -> list[dict]:
    msg = email_lib.message_from_bytes(mime_data, policy=policy.default)
    attachments: list[dict] = []

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


def _match_attachment_bytes(candidates: list[dict], filename: str, content_type: Optional[str], file_size: Optional[int]) -> Optional[dict]:
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


async def _load_original_document_bytes(document_id: str) -> tuple[dict, bytes]:
    p = await get_pool()
    doc = await p.fetchrow("""
        SELECT d.id, d.title, d.filename, d.domain, d.document_type, d.source_path,
               d.full_content, d.metadata,
               d.storage_backend, d.storage_uri,
               em.message_id, em.subject, em.mailbox
        FROM core.documents d
        LEFT JOIN legal.email_metadata em ON em.document_id = d.id
        WHERE d.id = $1::uuid
    """, document_id)

    if not doc:
        raise RuntimeError(f"Document {document_id} not found")

    source_path = doc["source_path"] or ""
    filename = doc["filename"] or doc["title"] or f"document-{document_id}"

    # MinIO / S3 blob — preferred when populated. These docs were ingested
    # with their raw bytes uploaded to object storage; we read from there
    # directly rather than re-fetching from Graph or the legacy FS path.
    storage_backend = doc["storage_backend"] or ""
    storage_uri = doc["storage_uri"] or ""
    if storage_backend in ("minio", "s3") and storage_uri:
        try:
            import blob_storage as _bs
            blob_bytes = _bs.get_bytes(storage_uri)
        except Exception as _bs_err:
            log.warning("Blob storage fetch raised for %s (%s): %s",
                        document_id, storage_uri, _bs_err)
            blob_bytes = None
        if blob_bytes is not None:
            mime_type = _guess_mime_type(filename)
            if doc["document_type"] in ("email", "eml"):
                mime_type = "message/rfc822"
            return ({
                "document_id": str(doc["id"]),
                "filename": filename,
                "mime_type": mime_type,
                "source_kind": f"blob_storage:{storage_backend}",
                "storage_uri": storage_uri,
                "original_available": True,
            }, blob_bytes)
        # If blob storage failed, fall through to legacy retrieval paths
        # below — gives us graceful degradation during MinIO outages.
        log.info("Blob fetch returned no bytes for %s; falling back to "
                 "legacy paths", document_id)

    if source_path.startswith("graph://"):
        if doc["document_type"] in ("email", "eml"):
            graph_ref = source_path.replace("graph://", "", 1)
            mailbox, message_ref = graph_ref.split("/", 1)
            graph_id = await _graph_resolve_message_graph_id(mailbox, message_ref)
            mime_bytes = await _graph_get_bytes(f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_id}/$value")
            return ({
                "document_id": str(doc["id"]),
                "filename": filename if filename.lower().endswith(".eml") else f"{filename}.eml",
                "mime_type": "message/rfc822",
                "source_kind": "graph_email",
                "original_available": True,
            }, mime_bytes)

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
            return ({
                "document_id": str(doc["id"]),
                "filename": att["filename"] or filename,
                "mime_type": att["content_type"] or _guess_mime_type(att["filename"] or filename),
                "source_kind": "graph_attachment",
                "original_available": True,
            }, matched["data"])

    local_path = source_path
    if local_path.startswith("file://"):
        local_path = local_path[7:]
    if local_path and os.path.exists(local_path):
        return ({
            "document_id": str(doc["id"]),
            "filename": os.path.basename(local_path) or filename,
            "mime_type": _guess_mime_type(os.path.basename(local_path) or filename),
            "source_kind": "filesystem",
            "original_available": True,
        }, Path(local_path).read_bytes())

    if doc["full_content"]:
        text_bytes = (doc["full_content"] or "").encode("utf-8")
        export_name = filename if "." in filename else f"{filename}.txt"
        return ({
            "document_id": str(doc["id"]),
            "filename": export_name,
            "mime_type": "text/plain; charset=utf-8",
            "source_kind": "database_export",
            "original_available": False,
        }, text_bytes)

    raise RuntimeError(f"No downloadable source found for document {document_id}")


_CONTENT_RANGE_RE = re.compile(r"^bytes (\d+)-(\d+)/(\d+)$")
_RANGE_HEADER_RE = re.compile(r"^bytes=(\d*)-(\d*)$")


def _content_disposition_value(filename: str, disposition: str = "attachment") -> str:
    safe_name = (filename or "download.bin").replace("\r", " ").replace("\n", " ").replace('"', "'")
    return f"{disposition}; filename=\"{safe_name}\"; filename*=UTF-8''{quote(safe_name)}"


def _normalize_download_disposition(disposition: Optional[str]) -> str:
    value = (disposition or "attachment").strip().lower()
    return value if value in {"attachment", "inline"} else "attachment"


def _safe_upload_filename(filename: str) -> str:
    candidate = (filename or "upload.bin").strip()
    cleaned = re.sub(r"[^\w\-.]", "_", candidate)
    return cleaned or "upload.bin"


def _build_upload_save_path(domain: str, filename: str) -> Path:
    safe_name = _safe_upload_filename(filename)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    domain_dir = UPLOAD_DIR / domain
    domain_dir.mkdir(parents=True, exist_ok=True)
    save_path = domain_dir / f"{ts}_{safe_name}"
    if save_path.exists():
        save_path = domain_dir / f"{ts}_{uuid.uuid4().hex[:8]}_{safe_name}"
    return save_path


def _default_document_type_for_extension(ext: str) -> str:
    if ext in (".xlsx", ".xls", ".csv"):
        return "spreadsheet"
    if ext in (".png", ".jpg", ".jpeg", ".tif", ".tiff"):
        return "image"
    return "document"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _append_bytes_to_file(path: Path, data: bytes) -> None:
    with path.open("ab") as handle:
        handle.write(data)


def _next_expected_ranges(next_offset: int, total_size: int) -> list[str]:
    return [] if next_offset >= total_size else [f"{next_offset}-"]


def _parse_content_range(value: str) -> tuple[int, int, int]:
    match = _CONTENT_RANGE_RE.match((value or "").strip())
    if not match:
        raise ValueError("Content-Range must be in the form 'bytes start-end/total'")
    start, end, total = (int(match.group(i)) for i in range(1, 4))
    if total <= 0:
        raise ValueError("Content-Range total size must be greater than zero")
    if start < 0 or end < start or end >= total:
        raise ValueError("Content-Range values are invalid")
    return start, end, total


def _parse_range_header(value: Optional[str], total_size: int) -> tuple[int, int] | None:
    if not value:
        return None
    match = _RANGE_HEADER_RE.match(value.strip())
    if not match:
        raise ValueError("Range must be in the form 'bytes=start-end'")

    start_raw, end_raw = match.groups()
    if start_raw == "" and end_raw == "":
        raise ValueError("Range header is empty")

    if start_raw == "":
        suffix = int(end_raw)
        if suffix <= 0:
            raise ValueError("Range suffix must be greater than zero")
        start = max(total_size - suffix, 0)
        end = total_size - 1
    else:
        start = int(start_raw)
        end = total_size - 1 if end_raw == "" else int(end_raw)

    if start >= total_size:
        raise IndexError("Range start exceeds file size")
    if end >= total_size:
        end = total_size - 1
    if start > end:
        raise ValueError("Range start must be less than or equal to the range end")
    return start, end


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


async def _resolve_original_document_download(document_id: str) -> dict:
    p = await get_pool()
    doc = await p.fetchrow(
        "SELECT id, filename, title, source_path, document_type, metadata, full_content "
        "FROM core.documents WHERE id = $1::uuid",
        document_id,
    )
    if not doc:
        raise RuntimeError(f"Document not found: {document_id}")

    filename = doc["filename"] or f"document-{document_id}.bin"
    source_path = (doc["source_path"] or "").strip()

    if source_path.startswith("graph://"):
        if doc["document_type"] in ("email", "eml"):
            mailbox, message_ref = source_path[len("graph://"):].split("/messages/", 1)
            graph_id = await _graph_resolve_message_graph_id(mailbox, message_ref)
            payload = await _graph_get_bytes(f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_id}/$value")
            out_name = filename if filename.lower().endswith(".eml") else f"{Path(filename).stem or 'message'}.eml"
            return {
                "document_id": str(doc["id"]),
                "filename": out_name,
                "mime_type": "message/rfc822",
                "source_kind": "graph_email",
                "original_available": True,
                "content_bytes": payload,
                "total_size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }

        att = await p.fetchrow(
            """
            SELECT filename, content_type, file_size, metadata
            FROM legal.email_attachments
            WHERE download_document_id = $1::uuid
            """,
            document_id,
        )
        if att:
            metadata = att["metadata"] if isinstance(att["metadata"], dict) else {}
            parent_ref = (metadata or {}).get("parent_graph_ref")
            if parent_ref:
                mailbox, message_ref = parent_ref.split("/", 1)
                graph_id = await _graph_resolve_message_graph_id(mailbox, message_ref)
                mime_bytes = await _graph_get_bytes(f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_id}/$value")
                attachments = _extract_all_mime_attachments(mime_bytes)
                matched = _match_attachment_bytes(attachments, att["filename"], att["content_type"], att["file_size"])
                if matched:
                    payload = matched["data"]
                    return {
                        "document_id": str(doc["id"]),
                        "filename": att["filename"] or filename,
                        "mime_type": att["content_type"] or _guess_mime_type(att["filename"] or filename),
                        "source_kind": "graph_attachment",
                        "original_available": True,
                        "content_bytes": payload,
                        "total_size": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
            raise RuntimeError(f"Attachment bytes not found in parent email for document {document_id}")

    local_path = source_path
    if local_path.startswith("file://"):
        local_path = local_path[7:]
    if local_path and os.path.exists(local_path):
        local_file = Path(local_path)
        total_size = local_file.stat().st_size
        return {
            "document_id": str(doc["id"]),
            "filename": local_file.name or filename,
            "mime_type": _guess_mime_type(local_file.name or filename),
            "source_kind": "filesystem",
            "original_available": True,
            "local_path": str(local_file),
            "total_size": total_size,
            "sha256": await asyncio.to_thread(_file_sha256, local_file),
        }

    if doc["full_content"]:
        payload = (doc["full_content"] or "").encode("utf-8")
        export_name = filename if "." in filename else f"{filename}.txt"
        return {
            "document_id": str(doc["id"]),
            "filename": export_name,
            "mime_type": "text/plain; charset=utf-8",
            "source_kind": "database_export",
            "original_available": False,
            "content_bytes": payload,
            "total_size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    raise RuntimeError(f"No downloadable source found for document {document_id}")




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
            if isinstance(parsed.get("results"), list):
                result_count = len(parsed["results"])
            elif isinstance(parsed.get("tools"), list):
                result_count = len(parsed["tools"])
            elif isinstance(parsed.get("documents"), list):
                result_count = len(parsed["documents"])
            elif isinstance(parsed.get("cases"), list):
                result_count = len(parsed["cases"])
            elif isinstance(parsed.get("filings"), list):
                result_count = len(parsed["filings"])
            elif isinstance(parsed.get("attachments"), list):
                result_count = len(parsed["attachments"])
            elif isinstance(parsed.get("messages"), list):
                result_count = len(parsed["messages"])
            else:
                result_count = parsed.get("count", parsed.get("total", 0))
            # Keep summary small
            summary = {k: v for k, v in parsed.items()
                       if k in ("count", "total", "query", "domain", "case_number",
                                "tool", "type", "message", "id", "title")}
            if not summary and isinstance(parsed.get("results"), list):
                summary = {"count": result_count}
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


async def _log_access(tool_name: str, query: str = "", result_count: int = 0):
    """Backward-compatible lightweight access logger.

    Older MCP tools in this server call `_log_access(...)` directly. A newer
    decorator-based logging path superseded it, but the helper itself was
    removed, leaving runtime NameErrors in tools that still reference it.
    Keep a small compatibility shim so those tools remain stable.
    """
    preview = (query or "")[:500]
    summary = {"query": preview, "count": int(result_count or 0)}
    await _log_tool_call(
        tool_name,
        {"query": preview} if preview else {},
        json.dumps(summary),
        0,
        None,
    )


async def _send_athena_error_alert(title: str, detail: str) -> None:
    if not all([GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_TENANT_ID]):
        return

    body_html = build_alert_notification_html(
        title,
        [{
            "heading": "Technical details",
            "type": "error",
            "plain_summary": "Athena captured an MCP server error that needs review.",
            "content": detail,
        }],
    )
    result = await send_alert_email(
        tenant_id=GRAPH_TENANT_ID,
        client_id=GRAPH_CLIENT_ID,
        client_secret=GRAPH_CLIENT_SECRET,
        sender=DEFAULT_ATHENA_MAILBOX,
        to_recipients=[ATHENA_ALERT_EMAIL],
        subject=title,
        body_html=body_html,
        importance="high",
    )
    if result.get("status") != "sent":
        log.warning("Failed to send Athena MCP alert email: %s", result.get("error", "unknown error"))


def _schedule_athena_error_alert(title: str, detail: str) -> None:
    try:
        asyncio.create_task(_send_athena_error_alert(title, detail))
    except Exception as e:
        log.warning("Unable to schedule Athena MCP alert email: %s", e)


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
            _schedule_athena_error_alert(
                f"Athena MCP tool error — {tool_name}",
                "\n".join([
                    f"tool: {tool_name}",
                    f"client: {_current_client_id()}",
                    f"error: {error}",
                    f"kwargs: {json.dumps({k: str(v)[:500] for k, v in kwargs.items() if v is not None}, default=str)}",
                ]),
            )
            raise
        finally:
            duration_ms = int((time.time() - t0) * 1000)
            # kwargs are the tool arguments
            tool_args = {k: v for k, v in kwargs.items() if v is not None}
            asyncio.create_task(
                _log_tool_call(tool_name, tool_args, result or "", duration_ms, error)
            )
    return wrapper

# ── Redis cache ─────────────────────────────────────────────
_redis_client = None

async def _get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        try:
            import redis.asyncio as aioredis
            _redis_client = aioredis.from_url(_redis_url, socket_timeout=2)
            await _redis_client.ping()
        except Exception as e:
            log.warning("Redis not available for caching: %s", e)
            _redis_client = None
    return _redis_client

# ── Embedding helper (local BGE-M3) with Redis cache ─────────
async def _embed_query(text: str) -> list[float]:
    # Check Redis cache first (5-minute TTL on embeddings)
    cache_key = f"emb:{hashlib.md5(text.encode()).hexdigest()}"
    try:
        r = await _get_redis()
        if r:
            cached = await r.get(cache_key)
            if cached:
                return json.loads(cached)
    except Exception:
        pass  # Cache miss or error, proceed to local model

    embedding = embed_query_sync(text)

    # Cache the result
    try:
        r = await _get_redis()
        if r:
            await r.setex(cache_key, 300, json.dumps(embedding))
    except Exception:
        pass

    return embedding


async def _get_current_embedding_model_id() -> int:
    """Resolve the current embedding model ID, creating/updating the registry row if needed."""
    cache_key = (EMBEDDING_MODEL, EMBEDDING_DIMS)
    cached = _embedding_model_id_cache.get(cache_key)
    if cached is not None:
        return cached

    p = await get_pool()
    row = await p.fetchrow(
        """
        INSERT INTO core.embedding_models (name, provider, dimensions)
        VALUES ($1, $2, $3)
        ON CONFLICT (name) DO UPDATE SET
            provider = EXCLUDED.provider,
            dimensions = EXCLUDED.dimensions
        RETURNING id
        """,
        EMBEDDING_MODEL,
        EMBEDDING_PROVIDER,
        EMBEDDING_DIMS,
    )
    if not row:
        raise RuntimeError("Unable to resolve the current embedding model ID")

    model_id = int(row["id"])
    _embedding_model_id_cache[cache_key] = model_id
    return model_id


_CASE_NUMBER_PATTERNS = [
    re.compile(r"\b\d{2,4}-\d{1,2}-\d{3,6}-\d{2,4}\b"),
    re.compile(r"\b\d{2,4}-\d{1,2}-\d{3,6}\b"),
    re.compile(r"\b\d{2}:\d{2}-[a-z]{2}-\d{5}\b", re.IGNORECASE),
]


def _extract_case_number(text: str) -> Optional[str]:
    if not text:
        return None
    for pattern in _CASE_NUMBER_PATTERNS:
        match = pattern.search(text)
        if match:
            return match.group(0)
    return None


def _normalize_query(text: str) -> str:
    if not text:
        return text
    return re.sub(r"\s+", " ", text).strip()


def _coerce_weights(semantic_weight: Optional[float], fulltext_weight: Optional[float]) -> tuple[float, float]:
    sem = RAG_SEMANTIC_WEIGHT_DEFAULT if semantic_weight is None else float(semantic_weight)
    ft = RAG_FULLTEXT_WEIGHT_DEFAULT if fulltext_weight is None else float(fulltext_weight)
    sem = max(0.0, sem)
    ft = max(0.0, ft)
    total = sem + ft
    if total <= 0:
        return RAG_SEMANTIC_WEIGHT_DEFAULT, RAG_FULLTEXT_WEIGHT_DEFAULT
    return sem / total, ft / total


def _coerce_neighbor_window(expand_neighbors: Optional[int]) -> int:
    if expand_neighbors is None:
        value = RAG_NEIGHBOR_DEFAULT
    else:
        value = int(expand_neighbors)
    return min(max(value, 0), RAG_NEIGHBOR_MAX)


async def _get_chunk_window(
    p: asyncpg.Pool,
    document_id: str,
    chunk_index: int,
    window: int,
) -> list[asyncpg.Record]:
    if window <= 0:
        return []
    start_idx = max(0, chunk_index - window)
    end_idx = chunk_index + window
    return await p.fetch(
        """
        SELECT chunk_index, content
        FROM core.document_chunks
        WHERE document_id = $1::uuid
          AND chunk_index BETWEEN $2 AND $3
        ORDER BY chunk_index
        """,
        document_id,
        start_idx,
        end_idx,
    )


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

TRANSFER_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ops.file_transfer_sessions (
    session_id   UUID PRIMARY KEY,
    direction    TEXT NOT NULL CHECK (direction IN ('upload', 'download')),
    token_hash   TEXT NOT NULL UNIQUE,
    status       TEXT NOT NULL DEFAULT 'pending',
    client_id    TEXT,
    filename     TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    domain       TEXT,
    title        TEXT,
    document_type TEXT,
    description  TEXT,
    disposition  TEXT,
    total_size   BIGINT NOT NULL DEFAULT 0,
    next_offset  BIGINT NOT NULL DEFAULT 0,
    temp_path    TEXT,
    document_id  UUID,
    sha256       TEXT,
    metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_file_transfer_sessions_expires_at
    ON ops.file_transfer_sessions (expires_at);

CREATE INDEX IF NOT EXISTS idx_file_transfer_sessions_direction_status
    ON ops.file_transfer_sessions (direction, status);
"""


async def _ensure_transfer_schema():
    global _transfer_schema_ready
    if _transfer_schema_ready:
        return
    p = await get_pool()
    await p.execute(TRANSFER_SCHEMA_SQL)
    _transfer_schema_ready = True
    log.info("Transfer session schema ensured")


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
        granted_scopes = _granted_scopes_for_client(client, params.scopes)

        # Generate authorization code (256 bits of entropy)
        code = secrets.token_urlsafe(32)
        expires_at = time.time() + 600  # 10 min

        await p.execute("""
            INSERT INTO ops.oauth_authorization_codes
                (code, client_id, scopes, code_challenge, redirect_uri,
                 redirect_uri_provided_explicitly, resource, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, code, client.client_id, granted_scopes,
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
        granted_scopes = _granted_scopes_for_client(client, authorization_code.scopes)

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
        """, access_token, client.client_id, granted_scopes,
             authorization_code.resource, now_ts + expires_in)

        await p.execute("""
            INSERT INTO ops.oauth_refresh_tokens (token, client_id, scopes, expires_at)
            VALUES ($1, $2, $3, $4)
        """, refresh_token, client.client_id, granted_scopes,
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
            scope=" ".join(granted_scopes) if granted_scopes else None,
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
        use_scopes = _granted_scopes_for_client(client, scopes if scopes else refresh_token.scopes)
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
    log.info("Athena MCP Server v2 starting up...")
    p = await get_pool()
    await p.execute(OAUTH_SCHEMA_SQL)
    await _ensure_transfer_schema()
    log.info("OAuth schema ready, pool connected")
    try:
        yield {}
    finally:
        # NOTE: In Streamable HTTP mode, lifespan runs per-session.
        # Do NOT close the shared pool here — it kills auth for all
        # other sessions. Pool cleanup happens on process exit via
        # atexit / signal handlers.
        log.info("Athena MCP Server v2 shut down")


# ══════════════════════════════════════════════════════════════
#  CREATE THE SERVER
# ══════════════════════════════════════════════════════════════

oauth_provider = PostgresOAuthProvider()

mcp = FastMCP(
    name="Athena Cognitive Engine",
    instructions="""You are Athena AI, an elite Legal Analyst and Investigator connected to the 
Athena Cognitive Platform. You serve as William Nelson's dedicated legal research 
assistant with expertise in civil litigation, employment law, and insurance disputes.

═══════════════════════════════════════════════════════════════════════════════════════════
DATABASE TECHNOLOGY
═══════════════════════════════════════════════════════════════════════════════════════════
**PostgreSQL 17 with pgvector Extension** - NOT SQLite!

This is a HYBRID system with TWO powerful search capabilities:

1️⃣ **VECTOR SEARCH (pgvector)** - Semantic similarity search
   • Finds documents by MEANING, not just keywords
   • Uses embeddings (1024-dimensional vectors via BGE-M3)
   • Best for: "Find documents about X", natural language queries
   • Tools: semantic_search(), rag_query()
   • Example: Find all docs related to "employment discrimination" even without exact phrase

2️⃣ **REGULAR RELATIONAL DATABASE (PostgreSQL)** - Structured data
   • Traditional SQL queries on tables, joins, aggregations
   • Full-text search with tsvector/tsquery
   • Best for: Exact filters, statistics, structured queries
   • Tools: search_emails(), search_filings(), list_cases(), execute_sql()
   • Example: Find all emails sent on a specific date to a specific recipient

📌 KEY POINTS:
• When writing SQL: Use PostgreSQL syntax (NOT SQLite)
• Schema-qualified names required: legal.cases, core.documents, medical.records
• Available features: JSONB, arrays, CTEs, window functions, regex, date functions
• Full-text search: Use tsvector/tsquery (NOT SQLite FTS5)
• For semantic/meaning-based search: Use vector tools (semantic_search, rag_query)
• For structured/exact queries: Use SQL tools or specialized search functions

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
1. **START EVERY SESSION**: Call get_william_context() first — it loads William's profile,
   Athena identity, top memories, pending tasks, and upcoming deadlines in one round trip.
   (get_agent_context('athena') is also available for the per-agent KV store if you need it.)
2. **RECALL BEFORE ANSWERING**: When the topic is non-trivial, call recall_personal_memories()
   with a short natural-language query. Recall is ranked by 0.7·similarity + 0.3·importance
   and skips superseded entries automatically.
3. **DOCUMENT EVERYTHING**: Use create_note() to record findings, questions, and tasks.
4. **PRESERVE DELIVERABLES**: Use create_document() or create_document_upload_session() whenever you create a substantive artifact William may want searchable later — memos, reports, drafts, code notes, research summaries, or supporting files.
5. **WORK ITERATIVELY**: Break complex analysis into steps, documenting progress.
6. **USE BULK TOOLS**: Prefer get_case_report() and get_all_cases_summary() over multiple queries.
7. **TRACK INVESTIGATIONS**: Use get_investigation_summary() to see status and open items.

═══════════════════════════════════════════════════════════════════════════════════════════
MEMORY & FEEDBACK (core.memories — semantic, cross-session)
═══════════════════════════════════════════════════════════════════════════════════════════
Three tools, distinct purposes:

• **save_personal_memory(content, memory_type, importance, supersedes_memory_id?)**
  Persist a fact, decision, preference, event, or insight. Saving the same content twice
  reinforces the existing row (raises importance, bumps access_count) — it does NOT create
  a duplicate, so re-asserting "William prefers X" is safe and idempotent. Use
  ``supersedes_memory_id`` only when the new content REPLACES an older memory (e.g. a
  changed deadline or corrected fact).

• **record_feedback(content, importance=0.7, supersedes_memory_id?)**
  Use whenever William corrects your approach ("don't do X", "stop Y", "from now on Z"),
  confirms a non-obvious choice that worked, or states a workflow preference. Lead with
  the rule itself; add a "Why:" line if William gave a reason. Stored as
  memory_type='feedback' and surfaces alongside facts in recall.

• **recall_personal_memories(query, limit=5)**
  Semantic search over all active memories. Returns ``score`` (importance-weighted) and
  ``relevance`` (raw cosine). Superseded and morning-briefing rows are filtered out.

Rule of thumb: facts and decisions → save_personal_memory; corrections, rules, and
preferences → record_feedback. Don't reach for save_memory/get_memory — those are the
per-agent JSONB KV store for structured agent state, not human-readable memory.

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
  1. get_william_context() → Profile + identity + top memories + tasks + deadlines
  2. recall_personal_memories(<topic of this session>) → Pull relevant prior facts
  3. get_investigation_summary('athena') → See open tasks and recent work

During Investigation:
  4. get_case_report(case_number) → Get full case details
  5. rag_query/semantic_search → Find relevant documents
  6. create_note(..., note_type='finding') → Document what you discover
  7. create_document(...) / create_document_upload_session(...) → Preserve substantial work product in Athena when it should remain searchable
  8. create_note(..., note_type='task') → Track follow-up items

When William corrects you or states a preference:
  → record_feedback("rule. Why: <reason>")
  → if it replaces a prior memory, pass supersedes_memory_id=<old id>

Session End:
  → save_personal_memory(<key insight>, memory_type='insight', importance=0.7)
    for anything that should be recallable cross-session.

═══════════════════════════════════════════════════════════════════════════════════════════
AVAILABLE DATA
═══════════════════════════════════════════════════════════════════════════════════════════
• Court filings, emails, attachments across multiple active cases
• Medical records and correspondence
• Email archives (inbox triage and categorization)
• Embedded document chunks for semantic search

═══════════════════════════════════════════════════════════════════════════════════════════
ORIGINAL DOCUMENT ACCESS
═══════════════════════════════════════════════════════════════════════════════════════════
• Ingested documents are expected to retain access to their originals whenever a source
    file or source system reference exists.
• Use `fetch_document(document_id)` to inspect document metadata and confirm the download hint.
• Use `fetch_attachments(email_document_id)` to enumerate email attachments and obtain each
    attachment's `download_document_id`.
• Use `create_document_download_session(document_id)` to obtain a raw HTTP `download_url`.
    All file transfers go over HTTP (PUT for upload, GET with optional `Range` for download).
    There is no base64 chunked path.
• For email messages, the original should be retrieved as `.eml` / `message/rfc822` bytes.
• For attachments and filesystem-backed documents, the original should be retrieved in its
    native file format whenever available.
• If `original_available` is false, the server is returning a text export fallback because the
    original source file is no longer directly reachable.

Recommended retrieval workflow:
    1. Search or fetch the target document.
    2. If it is an email with attachments, call `fetch_attachments`.
    3. Call `create_document_download_session` and `GET` the returned `download_url`
       (use `Range: bytes=...` for partial reads or resume).

═══════════════════════════════════════════════════════════════════════════════════════════
DOCX → PDF CONVERSION
═══════════════════════════════════════════════════════════════════════════════════════════
Athena can render any DOCX/DOC/ODT/RTF document into a PDF for viewing or downloading.
Two entry points — pick based on how the bytes arrive:

• `create_document_upload_session(..., auto_convert_to_pdf=True)` — for uploading new
    DOCX content. Stream the raw DOCX bytes to the returned `upload_url` with PUT +
    Content-Range; once the final chunk completes, the server automatically converts
    the file and the completion payload includes `pdf_document_id`, `pdf_download_url`,
    and `pdf_view_url`.
• `convert_docx_to_pdf(document_id)` — convert a DOCX that is already stored in Athena.

After conversion, the PDF is a first-class document in Athena: it is searchable,
tagged with a `converted_from` relationship back to the DOCX source, and can be
fetched later through the standard download path: a `download_url` returned by
`create_document_download_session` (or inline view via `disposition='inline'`).

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
    streamable_http_path=MCP_ENDPOINT_PATH,
    lifespan=lifespan,
)


def _get_public_base_url(request: Request) -> str:
    forwarded_proto = request.headers.get("x-forwarded-proto")
    forwarded_host = request.headers.get("x-forwarded-host")
    if forwarded_proto and forwarded_host:
        return f"{forwarded_proto}://{forwarded_host}".rstrip("/")
    return BASE_URL.rstrip("/")


def _mcp_discovery_payload(base_url: str) -> dict[str, Any]:
    endpoint = f"{base_url}{MCP_ENDPOINT_PATH}"
    return {
        "name": "Athena Cognitive Engine",
        "description": "Athena Cognitive Engine MCP server for legal, email, and medical data.",
        "version": "2.0",
        "mcp": {
            "transport": "streamable-http",
            "streamable_http_endpoint": endpoint,
            "http_endpoint": endpoint,
            "sse_endpoint": endpoint,
        },
        "oauth": {
            "issuer": f"{base_url}/",
            "authorization_endpoint": f"{base_url}/authorize",
            "token_endpoint": f"{base_url}/token",
            "registration_endpoint": f"{base_url}/register",
            "revocation_endpoint": f"{base_url}/revoke",
            "scopes_supported": ["read", "write", "admin", "search", "medical"],
        },
    }


def _build_transfer_url(direction: str, token: str) -> str:
    return f"{BASE_URL.rstrip('/')}/transfer/{direction}/{token}"


_BACKGROUND_UPLOAD_TASKS: set[asyncio.Task] = set()


def _spawn_upload_finalizer(coro) -> None:
    task = asyncio.create_task(coro)
    _BACKGROUND_UPLOAD_TASKS.add(task)
    task.add_done_callback(_BACKGROUND_UPLOAD_TASKS.discard)


async def _finalize_upload_session(
    session_id: uuid.UUID,
    final_path: Path,
    filename: str,
    domain: str,
    title: Optional[str],
    document_type: Optional[str],
    description: Optional[str],
    auto_convert_to_pdf: bool,
) -> None:
    """Background ingestion for a completed upload session.

    Runs text extraction, chunking, embedding, DB inserts, and optional DOCX→PDF
    conversion after the final chunk has been written to disk. Updates the session
    row to `completed` (with `document_id`) on success or `failed` on error so
    clients polling the status URL can observe the transition.
    """
    p = await get_pool()
    try:
        ingest_result = await _ingest_saved_upload(
            final_path,
            filename,
            domain,
            title,
            document_type,
            description,
            "mcp-upload-session",
        )
    except Exception as exc:
        log.error("Background ingest failed for session %s: %s", session_id, exc)
        try:
            await p.execute(
                """
                UPDATE ops.file_transfer_sessions
                SET status = 'failed',
                    updated_at = now(),
                    metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb
                WHERE session_id = $1
                """,
                session_id,
                json.dumps({"error": f"Upload finished but ingest failed: {exc}"}),
            )
        except Exception as db_err:
            log.error("Failed to mark session %s as failed: %s", session_id, db_err)
        return

    completion_meta: dict[str, Any] = {"ingest_result": ingest_result}

    if auto_convert_to_pdf:
        upload_ext = Path(filename).suffix.lower()
        if upload_ext in (".docx", ".doc", ".odt", ".rtf"):
            try:
                conversion = await _run_docx_to_pdf_pipeline(ingest_result["document_id"])
                completion_meta["pdf_conversion"] = {
                    "pdf_document_id": conversion["pdf_document_id"],
                    "pdf_filename": conversion["pdf_filename"],
                    "pdf_size_bytes": conversion["pdf_size_bytes"],
                    "download_url": conversion["download_url"],
                    "view_url": conversion["view_url"],
                }
            except Exception as conv_err:
                log.error("Auto DOCX→PDF conversion failed: %s", conv_err)
                completion_meta["pdf_conversion_error"] = str(conv_err)
        else:
            completion_meta["pdf_conversion_error"] = (
                f"auto_convert_to_pdf skipped — unsupported extension {upload_ext}"
            )

    try:
        await p.execute(
            """
            UPDATE ops.file_transfer_sessions
            SET status = 'completed',
                next_offset = total_size,
                document_id = $2::uuid,
                completed_at = now(),
                updated_at = now(),
                metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb
            WHERE session_id = $1
            """,
            session_id,
            uuid.UUID(ingest_result["document_id"]),
            json.dumps(completion_meta),
        )
    except Exception as db_err:
        log.error("Failed to mark session %s as completed: %s", session_id, db_err)


def _upload_session_status_payload(
    row: asyncpg.Record,
    token: str,
    *,
    next_offset: Optional[int] = None,
    status: Optional[str] = None,
) -> dict[str, Any]:
    offset = int(row["next_offset"] if next_offset is None else next_offset)
    expires_at = row["expires_at"]
    metadata = _json_object(row["metadata"])
    total_size = int(row["total_size"])
    payload = {
        "session_id": str(row["session_id"]),
        "status": status or row["status"],
        "filename": row["filename"],
        "content_type": row["content_type"],
        "domain": row["domain"],
        "title": row["title"],
        "document_type": row["document_type"],
        "total_size": total_size,
        "next_offset": offset,
        "next_expected_ranges": _next_expected_ranges(offset, total_size),
        "nextExpectedRanges": _next_expected_ranges(offset, total_size),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "expirationDateTime": expires_at.isoformat() if expires_at else None,
        "upload_url": _build_transfer_url("upload", token),
        "status_url": _build_transfer_url("upload", token),
        "cancel_url": _build_transfer_url("upload", token),
        "max_chunk_size": TRANSFER_UPLOAD_MAX_CHUNK_SIZE,
    }
    if row["document_id"]:
        payload["document_id"] = str(row["document_id"])
    if metadata.get("ingest_result"):
        payload["ingest_result"] = metadata["ingest_result"]
    if metadata.get("pdf_conversion"):
        pdf_info = metadata["pdf_conversion"]
        payload["pdf_conversion"] = pdf_info
        if pdf_info.get("pdf_document_id"):
            payload["pdf_document_id"] = pdf_info["pdf_document_id"]
        if pdf_info.get("download_url"):
            payload["pdf_download_url"] = pdf_info["download_url"]
        if pdf_info.get("view_url"):
            payload["pdf_view_url"] = pdf_info["view_url"]
    if metadata.get("pdf_conversion_error"):
        payload["pdf_conversion_error"] = metadata["pdf_conversion_error"]
    if metadata.get("auto_convert_to_pdf") is not None:
        payload["auto_convert_to_pdf"] = bool(metadata["auto_convert_to_pdf"])
    if metadata.get("error"):
        payload["error"] = metadata["error"]
    return payload


@mcp.custom_route("/.well-known/mcp.json", methods=["GET"], include_in_schema=False)
@mcp.custom_route("/mcp/.well-known/mcp.json", methods=["GET"], include_in_schema=False)
@mcp.custom_route(f"{MCP_ENDPOINT_PATH}/.well-known/mcp.json", methods=["GET"], include_in_schema=False)
async def mcp_well_known(request: Request) -> JSONResponse:
    """Publish MCP discovery metadata for clients that probe well-known routes."""
    return JSONResponse(_mcp_discovery_payload(_get_public_base_url(request)))


@mcp.custom_route("/transfer/upload/{token}", methods=["GET", "PUT", "DELETE"], include_in_schema=False)
async def transfer_upload_session(request: Request) -> Response:
    await _ensure_transfer_schema()
    token = str(request.path_params.get("token", "") or "").strip()
    if not token:
        return JSONResponse({"error": "Missing transfer token"}, status_code=400)

    token_hash = hashlib.sha256(token.encode()).hexdigest()
    p = await get_pool()

    if request.method == "DELETE":
        temp_path = None
        async with p.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1 FOR UPDATE",
                    token_hash,
                )
                if not row or row["direction"] != "upload":
                    return JSONResponse({"error": "Upload session not found"}, status_code=404)
                if row["status"] in {"completed", "ingesting"}:
                    return JSONResponse(_upload_session_status_payload(row, token), status_code=409)
                metadata = _json_object(row["metadata"])
                metadata["cancelled_at"] = datetime.now(timezone.utc).isoformat()
                await conn.execute(
                    """
                    UPDATE ops.file_transfer_sessions
                    SET status = 'cancelled', updated_at = now(), metadata = $2::jsonb
                    WHERE session_id = $1
                    """,
                    row["session_id"],
                    json.dumps(metadata),
                )
                temp_path = row["temp_path"]
        if temp_path:
            await asyncio.to_thread(Path(temp_path).unlink, missing_ok=True)
        return Response(status_code=204)

    if request.method == "GET":
        row = await p.fetchrow(
            "SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1",
            token_hash,
        )
        if not row or row["direction"] != "upload":
            return JSONResponse({"error": "Upload session not found"}, status_code=404)
        if (
            row["status"] not in {"completed", "ingesting"}
            and row["expires_at"]
            and row["expires_at"] <= datetime.now(timezone.utc)
        ):
            metadata = _json_object(row["metadata"])
            metadata["error"] = metadata.get("error") or "Upload session expired"
            await p.execute(
                """
                UPDATE ops.file_transfer_sessions
                SET status = 'expired', updated_at = now(), metadata = $2::jsonb
                WHERE session_id = $1
                """,
                row["session_id"],
                json.dumps(metadata),
            )
            if row["temp_path"]:
                await asyncio.to_thread(Path(row["temp_path"]).unlink, missing_ok=True)
            return JSONResponse(_upload_session_status_payload(row, token, status="expired"), status_code=410)

        current_size = row["next_offset"]
        if row["temp_path"] and Path(row["temp_path"]).exists():
            current_size = Path(row["temp_path"]).stat().st_size
            if current_size != row["next_offset"]:
                await p.execute(
                    "UPDATE ops.file_transfer_sessions SET next_offset = $2, updated_at = now() WHERE session_id = $1",
                    row["session_id"],
                    current_size,
                )
        return JSONResponse(_upload_session_status_payload(row, token, next_offset=int(current_size)))

    body = await request.body()
    if len(body) > TRANSFER_UPLOAD_MAX_CHUNK_SIZE:
        return JSONResponse(
            {"error": f"Chunk exceeds {TRANSFER_UPLOAD_MAX_CHUNK_SIZE} byte limit"},
            status_code=413,
        )

    try:
        start, end, total = _parse_content_range(request.headers.get("content-range", ""))
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    if len(body) != (end - start + 1):
        return JSONResponse({"error": "Chunk length does not match Content-Range"}, status_code=400)

    completed = False
    session_id = None
    temp_path_str = ""
    upload_domain = DEFAULT_DOCUMENT_DOMAIN
    upload_filename = ""
    upload_title = None
    upload_document_type = None
    upload_description = None
    expected_sha256 = None
    async with p.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1 FOR UPDATE",
                token_hash,
            )
            if not row or row["direction"] != "upload":
                return JSONResponse({"error": "Upload session not found"}, status_code=404)

            if row["status"] in {"completed", "ingesting", "cancelled", "failed", "expired"}:
                return JSONResponse(_upload_session_status_payload(row, token), status_code=409)

            if row["expires_at"] and row["expires_at"] <= datetime.now(timezone.utc):
                metadata = _json_object(row["metadata"])
                metadata["error"] = metadata.get("error") or "Upload session expired"
                await conn.execute(
                    """
                    UPDATE ops.file_transfer_sessions
                    SET status = 'expired', updated_at = now(), metadata = $2::jsonb
                    WHERE session_id = $1
                    """,
                    row["session_id"],
                    json.dumps(metadata),
                )
                if row["temp_path"]:
                    await asyncio.to_thread(Path(row["temp_path"]).unlink, missing_ok=True)
                return JSONResponse(_upload_session_status_payload(row, token, status="expired"), status_code=410)

            if total != int(row["total_size"]):
                return JSONResponse({"error": "Content-Range total does not match the upload session size"}, status_code=409)

            temp_path = Path(row["temp_path"])
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            current_size = temp_path.stat().st_size if temp_path.exists() else 0
            if current_size != row["next_offset"]:
                await conn.execute(
                    "UPDATE ops.file_transfer_sessions SET next_offset = $2, updated_at = now() WHERE session_id = $1",
                    row["session_id"],
                    current_size,
                )

            if start != current_size:
                mismatch_payload = _upload_session_status_payload(row, token, next_offset=int(current_size))
                mismatch_payload["error"] = "Chunk start does not match the next expected offset"
                return JSONResponse(
                    mismatch_payload,
                    status_code=416,
                    headers={"Content-Range": f"bytes */{row['total_size']}"},
                )

            await asyncio.to_thread(_append_bytes_to_file, temp_path, body)
            next_offset = end + 1
            metadata = _json_object(row["metadata"])
            metadata["last_chunk_size"] = len(body)
            metadata["last_uploaded_at"] = datetime.now(timezone.utc).isoformat()
            await conn.execute(
                """
                UPDATE ops.file_transfer_sessions
                SET next_offset = $2,
                    status = $3,
                    updated_at = now(),
                    expires_at = $4,
                    metadata = $5::jsonb
                WHERE session_id = $1
                """,
                row["session_id"],
                next_offset,
                "processing" if next_offset >= total else "uploading",
                datetime.now(timezone.utc) + timedelta(seconds=TRANSFER_UPLOAD_SESSION_TTL_SECONDS),
                json.dumps(metadata),
            )

            completed = next_offset >= total
            session_id = row["session_id"]
            temp_path_str = row["temp_path"]
            upload_domain = row["domain"] or DEFAULT_DOCUMENT_DOMAIN
            upload_filename = row["filename"]
            upload_title = row["title"]
            upload_document_type = row["document_type"]
            upload_description = row["description"]
            expected_sha256 = (row["sha256"] or "").strip().lower() or None

    if not completed:
        row = await p.fetchrow("SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1", token_hash)
        return JSONResponse(_upload_session_status_payload(row, token), status_code=202)

    temp_path = Path(temp_path_str)
    if expected_sha256:
        actual_sha256 = await asyncio.to_thread(_file_sha256, temp_path)
        if actual_sha256.lower() != expected_sha256:
            await p.execute(
                """
                UPDATE ops.file_transfer_sessions
                SET status = 'failed', updated_at = now(), metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb
                WHERE session_id = $1
                """,
                session_id,
                json.dumps({"error": "Uploaded file hash did not match expected sha256"}),
            )
            row = await p.fetchrow("SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1", token_hash)
            return JSONResponse(_upload_session_status_payload(row, token), status_code=409)

    final_path = _build_upload_save_path(upload_domain, upload_filename)
    await asyncio.to_thread(shutil.move, str(temp_path), str(final_path))

    # All bytes are on disk and verified. Flip the session to `ingesting` and
    # hand ingestion off to a background task so the PUT response returns well
    # within Cloudflare's 100s idle timeout. Clients poll GET the status URL
    # until status transitions to `completed` (with document_id) or `failed`.
    meta_row = await p.fetchrow(
        "SELECT metadata FROM ops.file_transfer_sessions WHERE session_id = $1",
        session_id,
    )
    session_metadata = _json_object(meta_row["metadata"]) if meta_row else {}
    auto_convert_flag = bool(session_metadata.get("auto_convert_to_pdf"))

    await p.execute(
        """
        UPDATE ops.file_transfer_sessions
        SET status = 'ingesting',
            next_offset = total_size,
            updated_at = now()
        WHERE session_id = $1
        """,
        session_id,
    )

    _spawn_upload_finalizer(
        _finalize_upload_session(
            session_id,
            final_path,
            upload_filename,
            upload_domain,
            upload_title,
            upload_document_type,
            upload_description,
            auto_convert_flag,
        )
    )

    row = await p.fetchrow(
        "SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1",
        token_hash,
    )
    return JSONResponse(
        _upload_session_status_payload(row, token),
        status_code=202,
    )


@mcp.custom_route("/transfer/download/{token}", methods=["GET", "HEAD"], include_in_schema=False)
async def transfer_download_session(request: Request) -> Response:
    await _ensure_transfer_schema()
    token = str(request.path_params.get("token", "") or "").strip()
    if not token:
        return JSONResponse({"error": "Missing transfer token"}, status_code=400)

    token_hash = hashlib.sha256(token.encode()).hexdigest()
    p = await get_pool()
    row = await p.fetchrow(
        "SELECT * FROM ops.file_transfer_sessions WHERE token_hash = $1",
        token_hash,
    )
    if not row or row["direction"] != "download" or not row["document_id"]:
        return JSONResponse({"error": "Download session not found"}, status_code=404)

    if row["expires_at"] and row["expires_at"] <= datetime.now(timezone.utc):
        await p.execute(
            "UPDATE ops.file_transfer_sessions SET status = 'expired', updated_at = now() WHERE session_id = $1",
            row["session_id"],
        )
        return JSONResponse({"error": "Download session expired"}, status_code=410)

    try:
        download = await _resolve_original_document_download(str(row["document_id"]))
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=404)

    disposition = _normalize_download_disposition(row["disposition"])
    filename = download["filename"]
    mime_type = download["mime_type"]
    total_size = int(download["total_size"])
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": _content_disposition_value(filename, disposition),
        "Content-Length": str(total_size),
        "ETag": f'"{download["sha256"]}"',
        "X-Document-Id": download["document_id"],
        "X-Original-Available": "true" if download.get("original_available") else "false",
        "X-Source-Kind": download.get("source_kind", "unknown"),
    }

    range_header = request.headers.get("range")
    if range_header:
        try:
            start, end = _parse_range_header(range_header, total_size)
        except IndexError:
            return Response(status_code=416, headers={"Content-Range": f"bytes */{total_size}", "Accept-Ranges": "bytes"})
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)

        headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"
        headers["Content-Length"] = str(end - start + 1)
        if request.method == "HEAD":
            return Response(status_code=206, headers=headers, media_type=mime_type)

        if download.get("local_path"):
            body = await asyncio.to_thread(Path(download["local_path"]).read_bytes)
            body = body[start:end + 1]
        else:
            body = download.get("content_bytes", b"")[start:end + 1]
        return Response(content=body, status_code=206, media_type=mime_type, headers=headers)

    if request.method == "HEAD":
        return Response(status_code=200, headers=headers, media_type=mime_type)

    if download.get("local_path"):
        file_headers = {k: v for k, v in headers.items() if k not in {"Content-Disposition", "Content-Length"}}
        return FileResponse(
            download["local_path"],
            media_type=mime_type,
            filename=filename,
            headers=file_headers,
            content_disposition_type=disposition,
        )

    return Response(content=download.get("content_bytes", b""), media_type=mime_type, headers=headers)


# ══════════════════════════════════════════════════════════════
#  SEARCH TOOLS
# ══════════════════════════════════════════════════════════════

@mcp.tool(name="search", annotations=READ_ONLY)
@logged_tool
async def search(query: str) -> dict:
    """Search for documents (MCP connector-compatible).

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

        async with p.acquire() as conn:
            # Cross-domain default: legal→privilege, medical→phi, non-scope strict
            clause, filter_params, log_cb = await build_document_safety_filter(
                conn=conn, domain=None,
                caller_context=_mcp_caller_context('search'),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=3,
            )
            rows = await conn.fetch(f"""
                SELECT c.document_id, d.id, d.title, d.filename, d.source_path,
                       d.domain, d.document_type, d.privilege, d.phi_status,
                       (c.embedding::halfvec(1024) <=> $1::halfvec(1024)) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.embedding IS NOT NULL
                  AND {clause}
                ORDER BY c.embedding::halfvec(1024) <=> $1::halfvec(1024)
                LIMIT $2
            """, vec_str, limit * 3, *filter_params)
            await log_cb(rows)

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
    """Fetch full content for a document (MCP connector-compatible).

    Args:
        id: Document ID from search results
    """
    p = await get_pool()
    try:
        async with p.acquire() as conn:
            try:
                doc = await fetch_safe_document(
                    id, conn=conn,
                    caller_context=_mcp_caller_context('fetch'),
                    include_privileged=_default_include_privileged(),
                )
            except PrivilegeDeniedException as e:
                return {"error": "privilege_denied",
                        "detail": "document is privileged; caller not authorized for privileged content"}
            except PHIAccessDeniedException as e:
                return {"error": "phi_access_denied",
                        "detail": "document contains PHI; caller did not supply purpose_of_use"}

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
        domain: Filter by domain (for example general, coding, legal, medical, paperless, web) or omit for all
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

        async with p.acquire() as conn:
            # Safety filter: pass the domain filter directly so we use the
            # domain-specific filter branch (legal→privilege, medical→phi) or
            # cross-domain strict. Caller's explicit domain takes precedence.
            safety_domain = domain if domain in ('legal', 'medical') else None
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain=safety_domain,
                caller_context=_mcp_caller_context(
                    'semantic_search',
                    {'query_domain': domain, 'case_number': case_number},
                ),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
            # Oversample 5x so the reranker has a useful candidate pool.
            fetch_n = max(limit * 5, 50)
            params.append(fetch_n)

            rows = await conn.fetch(f"""
                SELECT c.id AS chunk_id, d.id AS doc_id, d.id, d.title, d.filename,
                       d.domain, d.document_type, d.source_path, d.phi_status,
                       c.content, c.chunk_index,
                       (c.embedding::halfvec(1024) <=> $1::halfvec(1024)) AS distance,
                       em.sender, em.subject, em.date_sent
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                LEFT JOIN legal.email_metadata em ON d.id = em.document_id
                {where}
                ORDER BY c.embedding::halfvec(1024) <=> $1::halfvec(1024)
                LIMIT ${idx}
            """, *params)
            await log_cb(rows)

        # Rerank the dense-retrieval candidates with bge-reranker-v2-m3.
        # On CPU this is ~50ms per pair; with fetch_n=50 that's ~2.5s total.
        rows_list = list(rows)
        rerank_scores_by_chunk: dict[str, float] = {}
        if rows_list:
            try:
                passages = [r["content"] for r in rows_list]
                ranked = await rerank(query, passages)
                # ranked is [(orig_idx, score), ...] sorted desc. Capture scores
                # first (keyed by chunk_id from original row order), then reorder.
                for orig_idx, score in ranked:
                    rerank_scores_by_chunk[str(rows_list[orig_idx]["chunk_id"])] = float(score)
                rows_list = [rows_list[orig_idx] for orig_idx, _ in ranked]
            except Exception as e:
                log.warning("semantic_search rerank failed, falling back to cosine order: %s", e)

        # Deduplicate: keep best chunk per document
        seen_docs: dict[str, dict] = {}
        for r in rows_list:
            doc_id = str(r["doc_id"])
            if doc_id not in seen_docs:
                entry = {
                    "document_id": doc_id,
                    "title": r["title"] or r["filename"],
                    "domain": r["domain"],
                    "type": r["document_type"],
                    "similarity": round(1 - r["distance"], 4),
                    "rerank_score": round(
                        rerank_scores_by_chunk.get(str(r["chunk_id"]), 0.0), 6
                    ) if rerank_scores_by_chunk else None,
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
        async with p.acquire() as conn:
            safety_domain = domain if domain in ('legal', 'medical') else None
            if domain:
                # Params: $1=query, $2=limit, $3=domain, $4+=safety filter
                safety_clause, safety_params, log_cb = await build_document_safety_filter(
                    conn=conn, domain=safety_domain,
                    caller_context=_mcp_caller_context(
                        'fulltext_search', {'query_domain': domain},
                    ),
                    include_privileged=_default_include_privileged(),
                    table_alias='d', next_param_index=4,
                )
                rows = await conn.fetch(f"""
                    SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                           d.source_path, d.created_at, d.privilege, d.phi_status,
                           ts_rank(d.full_content_tsv, websearch_to_tsquery('english', $1)) AS rank,
                           ts_headline('english', d.full_content, websearch_to_tsquery('english', $1),
                                       'MaxWords=60, MinWords=20, StartSel=**, StopSel=**') AS headline
                    FROM core.documents d
                    WHERE d.full_content_tsv @@ websearch_to_tsquery('english', $1)
                      AND d.domain = $3
                      AND {safety_clause}
                    ORDER BY rank DESC LIMIT $2
                """, query, limit, domain, *safety_params)
                await log_cb(rows)
            else:
                safety_clause, safety_params, log_cb = await build_document_safety_filter(
                    conn=conn, domain=None,
                    caller_context=_mcp_caller_context('fulltext_search'),
                    include_privileged=_default_include_privileged(),
                    table_alias='d', next_param_index=3,
                )
                rows = await conn.fetch(f"""
                    SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                           d.source_path, d.created_at, d.privilege, d.phi_status,
                           ts_rank(d.full_content_tsv, websearch_to_tsquery('english', $1)) AS rank,
                           ts_headline('english', d.full_content, websearch_to_tsquery('english', $1),
                                       'MaxWords=60, MinWords=20, StartSel=**, StopSel=**') AS headline
                    FROM core.documents d
                    WHERE d.full_content_tsv @@ websearch_to_tsquery('english', $1)
                      AND {safety_clause}
                    ORDER BY rank DESC LIMIT $2
                """, query, limit, *safety_params)
                await log_cb(rows)

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
            conditions.append(f"(em.recipients ILIKE ${idx} OR em.cc ILIKE ${idx} OR d.full_content ILIKE ${idx})")
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
            params.append(_parse_date_param(start_date))
            idx += 1
        if end_date:
            conditions.append(f"em.date_sent <= ${idx}::timestamptz + interval '1 day'")
            params.append(_parse_date_param(end_date))
            idx += 1
        if has_attachments is not None:
            conditions.append(f"em.has_attachments = ${idx}")
            params.append(has_attachments)
            idx += 1

        async with p.acquire() as conn:
            # All email hits are domain='legal' by join; apply legal safety filter
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain='legal',
                caller_context=_mcp_caller_context(
                    'search_emails', {'case_number': case_number},
                ),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit)
            where = " AND ".join(conditions)

            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.source_path, d.created_at,
                       d.privilege,
                       em.sender, em.recipients, em.cc, em.subject,
                       em.date_sent, em.direction, em.has_attachments, em.thread_id,
                       LEFT(d.full_content, 500) AS preview
                FROM core.documents d
                JOIN legal.email_metadata em ON d.id = em.document_id
                WHERE {where}
                ORDER BY em.date_sent DESC NULLS LAST
                LIMIT ${idx}
            """, *params)
            await log_cb(rows)

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

        async with p.acquire() as conn:
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain='legal',
                caller_context=_mcp_caller_context(
                    'search_by_case', {'case_number': case_number},
                ),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit)
            where = " AND ".join(conditions)

            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.created_at, d.source_path, d.privilege,
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
            await log_cb(rows)

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
            params.append(_parse_date_param(start_date))
            idx += 1
        if end_date:
            conditions.append(f"COALESCE(em.date_sent, d.created_at) <= ${idx}::timestamptz + interval '1 day'")
            params.append(_parse_date_param(end_date))
            idx += 1

        async with p.acquire() as conn:
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain='legal',
                caller_context=_mcp_caller_context(
                    'case_timeline', {'case_number': case_number},
                ),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit)
            where = " AND ".join(conditions)

            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.document_type, d.privilege,
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
            await log_cb(rows)

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
    purpose_of_use: Optional[str] = None,
    authorization_id: Optional[str] = None,
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
        purpose_of_use: Required to surface PHI records (e.g. 'litigation_support',
            'clinical_care'). Without it, only de-identified records surface.
            Sprint A: when set, every PHI record surfaced is logged to
            medical.disclosures_log per 45 CFR 164.528.
        authorization_id: UUID of a medical.authorizations row. Required to
            surface psychotherapy_notes or 42 CFR Part 2 records; the row's
            scope must match the heightened category.
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
            params.append(_parse_date_param(start_date))
            idx += 1
        if end_date:
            conditions.append(f"rm.date_of_service <= ${idx}::date")
            params.append(_parse_date_param(end_date))
            idx += 1
        if provider:
            conditions.append(f"prov.name ILIKE ${idx}")
            params.append(f"%{provider}%")
            idx += 1
        if facility:
            conditions.append(f"rm.facility ILIKE ${idx}")
            params.append(f"%{facility}%")
            idx += 1

        async with p.acquire() as conn:
            # Medical domain filter. purpose_of_use=None → de-identified only.
            # authorization_id checked for revocation + expiration at call time.
            try:
                auth_uuid = (
                    __import__('uuid').UUID(authorization_id) if authorization_id else None
                )
            except (ValueError, AttributeError):
                return json.dumps({"error": "invalid authorization_id UUID"})
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain='medical',
                caller_context=_mcp_caller_context(
                    'search_medical',
                    {'record_type': record_type, 'provider': provider},
                ),
                purpose_of_use=purpose_of_use,
                authorization_id=auth_uuid,
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit)
            where = " AND ".join(conditions)

            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.document_type, d.created_at,
                       d.phi_status, d.phi_categories, d.minor_patient,
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
            await log_cb(rows)

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
        domain: Filter by domain (for example general, coding, legal, medical, paperless, web)
        document_type: Filter by type (email, court_filing, email_attachment, note, etc.)
        limit: Max results (default 50)
    """
    limit = min(max(limit, 1), 100)
    p = await get_pool()
    try:
        conditions = ["d.created_at >= $1::timestamptz", "d.created_at <= $2::timestamptz + interval '1 day'"]
        params: list[Any] = [_parse_date_param(start_date), _parse_date_param(end_date)]
        idx = 3

        if domain:
            conditions.append(f"d.domain = ${idx}")
            params.append(domain)
            idx += 1
        if document_type:
            conditions.append(f"d.document_type = ${idx}")
            params.append(document_type)
            idx += 1

        async with p.acquire() as conn:
            # Cross-domain unless caller narrows to legal/medical explicitly
            safety_domain = domain if domain in ('legal', 'medical') else None
            safety_clause, safety_params, log_cb = await build_document_safety_filter(
                conn=conn, domain=safety_domain,
                caller_context=_mcp_caller_context(
                    'search_by_date_range',
                    {'query_domain': domain, 'document_type': document_type},
                ),
                include_privileged=_default_include_privileged(),
                table_alias='d', next_param_index=idx,
            )
            idx += len(safety_params)
            params.extend(safety_params)
            conditions.append(safety_clause)

            params.append(limit)
            where = " AND ".join(conditions)

            rows = await conn.fetch(f"""
                SELECT d.id, d.title, d.filename, d.domain, d.document_type,
                       d.created_at, d.source_path, d.privilege, d.phi_status,
                       em.sender, em.date_sent, em.subject
                FROM core.documents d
                LEFT JOIN legal.email_metadata em ON d.id = em.document_id
                WHERE {where}
                ORDER BY d.created_at DESC LIMIT ${idx}
            """, *params)
            await log_cb(rows)

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
async def fetch_document(
    document_id: str,
    include_chunks: bool = False,
    include_privileged: Optional[bool] = None,
    purpose_of_use: Optional[str] = None,
    authorization_id: Optional[str] = None,
) -> str:
    """Fetch a document's full content and metadata by ID.

    Args:
        document_id: UUID of the document
        include_chunks: Whether to include all chunk texts (default False)
        include_privileged: If True, attorney_client and work_product_fact
            documents may be returned. If None (default), first-party clients
            (claude-desktop, chatgpt) get privileged access automatically;
            third-party callers must opt-in explicitly.
        purpose_of_use: Required to fetch PHI documents (medical domain).
        authorization_id: UUID of medical.authorizations — required for
            heightened-protection categories (psychotherapy_notes, 42 CFR Part 2).
    """
    if include_privileged is None:
        include_privileged = _default_include_privileged()
    p = await get_pool()
    try:
        async with p.acquire() as conn:
            try:
                auth_uuid = (
                    __import__('uuid').UUID(authorization_id) if authorization_id else None
                )
            except (ValueError, AttributeError):
                return json.dumps({"error": "invalid authorization_id UUID"})
            try:
                doc = await fetch_safe_document(
                    document_id, conn=conn,
                    caller_context=_mcp_caller_context('fetch_document'),
                    include_privileged=include_privileged,
                    purpose_of_use=purpose_of_use,
                    authorization_id=auth_uuid,
                )
            except PrivilegeDeniedException:
                return json.dumps({
                    "error": "privilege_denied",
                    "detail": "document is privileged; pass include_privileged=True if authorized",
                })
            except PHIAccessDeniedException as e:
                return json.dumps({
                    "error": "phi_access_denied",
                    "detail": str(e),
                })

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
            "download": {
                "tool": "create_document_download_session",
                "available": True,
                "note": "Call create_document_download_session(document_id) to obtain a raw HTTP download URL. Supports Range: bytes=... for partial reads.",
            },
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
            "download_document_id": str(r["child_doc_id"]) if r["child_doc_id"] else None,
            "download_tool": "create_document_download_session" if r["child_doc_id"] else None,
            "extracted_text_preview": r["extracted_text"][:500] if r["extracted_text"] else None,
        } for r in rows]

        return json.dumps({"email_document_id": email_document_id, "attachments": results, "count": len(results)})
    except Exception as e:
        log.error("fetch_attachments error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_mailbox_messages(
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    folder: str = "inbox",
    top: int = 25,
    unread_only: bool = False,
    search: Optional[str] = None,
    importance: Optional[str] = None,
) -> str:
    """Read live mailbox messages directly from Microsoft Graph.

    Args:
        mailbox: Mailbox to read (defaults to Athena's mailbox)
        folder: Mail folder such as inbox, sentitems, drafts, archive
        top: Maximum number of messages to return (1-100, default 25)
        unread_only: If true, only return unread messages
        search: Optional Graph mailbox search query
        importance: Optional filter — low, normal, or high
    """
    try:
        mailbox = _normalize_mailbox(mailbox)
        top = min(max(top, 1), 100)

        filter_parts = []
        if unread_only:
            filter_parts.append("isRead eq false")
        if importance:
            filter_parts.append(f"importance eq '{_normalize_importance(importance).lower()}'")

        params = {
            "$top": top,
            "$select": (
                "id,internetMessageId,conversationId,subject,from,toRecipients,ccRecipients,"
                "receivedDateTime,sentDateTime,bodyPreview,importance,isRead,isDraft,"
                "hasAttachments,isReadReceiptRequested,isDeliveryReceiptRequested,webLink,categories"
            ),
        }
        if filter_parts:
            params["$filter"] = " and ".join(filter_parts)
        if search:
            params["$search"] = f'"{search}"'
        else:
            params["$orderby"] = "receivedDateTime desc"

        if search:
            data = await _graph_request(
                "GET",
                f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders/{folder}/messages",
                params=params,
                headers={"ConsistencyLevel": "eventual"},
            )
        else:
            data = await _graph_get_json(
                f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders/{folder}/messages",
                params=params,
            )
        messages = [_message_summary(message) for message in data.get("value", [])]
        return json.dumps({
            "mailbox": mailbox,
            "folder": folder,
            "messages": messages,
            "count": len(messages),
        })
    except Exception as e:
        log.error("list_mailbox_messages error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def get_mailbox_message(
    message_id: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    include_body: bool = True,
    include_attachments: bool = True,
) -> str:
    """Fetch a live mailbox message directly from Microsoft Graph.

    Args:
        message_id: Graph message ID or RFC internet message ID
        mailbox: Mailbox to read from (defaults to Athena's mailbox)
        include_body: Include full message body content
        include_attachments: Include attachment metadata
    """
    try:
        mailbox = _normalize_mailbox(mailbox)
        graph_message_id = await _graph_resolve_message_graph_id(mailbox, message_id)
        params = {
            "$select": (
                "id,internetMessageId,conversationId,subject,from,sender,toRecipients,ccRecipients,"
                "bccRecipients,replyTo,receivedDateTime,sentDateTime,bodyPreview,body,importance,"
                "isRead,isDraft,hasAttachments,isReadReceiptRequested,isDeliveryReceiptRequested,"
                "webLink,categories"
            )
        }
        message = await _graph_get_json(
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_message_id}",
            params=params,
        )

        result = _message_summary(message)
        result["mailbox"] = mailbox
        if include_body:
            result["body"] = message.get("body")
        if include_attachments and message.get("hasAttachments"):
            attachment_data = await _graph_get_json(
                f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{graph_message_id}/attachments",
                params={"$select": "id,name,contentType,size,isInline"},
            )
            result["attachments"] = attachment_data.get("value", [])

        return json.dumps(result)
    except Exception as e:
        log.error("get_mailbox_message error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def draft_email_message(
    prompt: str,
    recipient_name: Optional[str] = None,
    tone: str = "professional",
) -> str:
    """Generate a polished email draft Athena can send or use in a reply.

    Args:
        prompt: Purpose, facts, and any must-include points for the message
        recipient_name: Optional recipient display name for salutation tuning
        tone: professional, warm, firm, concise, diplomatic, or follow-up
    """
    try:
        draft = await _draft_email_completion(prompt, tone, recipient_name)
        body_html = draft.get("body_html") or _text_to_html(draft.get("body_text", ""))
        body_text = draft.get("body_text") or _html_to_text(body_html)
        return json.dumps({
            "subject": (draft.get("subject") or "Draft email").strip(),
            "body_html": body_html,
            "body_text": body_text,
            "tone_used": draft.get("tone_used") or tone,
            "checklist": draft.get("checklist") or [],
            "model": ATHENA_EMAIL_DRAFT_MODEL,
        })
    except Exception as e:
        log.error("draft_email_message error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def send_mail_message(
    to_recipients: str,
    subject: str,
    body: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    cc_recipients: Optional[str] = None,
    bcc_recipients: Optional[str] = None,
    body_content_type: str = "html",
    importance: str = "normal",
    request_read_receipt: bool = False,
    request_delivery_receipt: bool = False,
    attachment_document_ids: Optional[str] = None,
) -> str:
    """Send a new email from the specified mailbox via Microsoft Graph.

    HOW TO USE THIS TOOL — send-format guidance for callers (including Athena):

    • Default to body_content_type="html" and supply a full HTML body when composing
      new outbound mail. Plain paragraphs wrapped in <p>…</p> render correctly
      everywhere; a plain-text version is generated automatically as a fallback.

    • When the body contains only plain text and you do NOT want Athena's auto
      HTML wrapping, pass body_content_type="text".

    • To attach files Athena created, pass attachment_document_ids — comma-separated
      document IDs returned by create_document / create_document_upload_session.

    • Use cc_recipients / bcc_recipients as comma-separated lists. BCC recipients
      are not visible to TO/CC recipients.

    TRANSPORT SELECTION (automatic, returned in the response as "transport"):

      raw_mime_utf8_base64 — Used for HTML bodies without attachments. This path
        sends a raw RFC-5322 multipart/alternative message (UTF-8 + base64 for
        both text/plain and text/html parts) directly via Graph /sendMail. It
        bypasses Exchange's default HTML-to-MIME conversion (which produces
        Windows-1252 + quoted-printable output that some strict recipient
        tenants — notably Defender-for-Office 365 and some government mail
        hygiene rules — strip, causing blank-body delivery). USE THIS for any
        external recipient whose tenant has previously delivered blank bodies.

      draft_attach_send — Used when attachments are present or body is plain
        text. Creates a draft, uploads attachments, then sends via Graph. Adds
        extra round-trips for attachment upload but is required for file
        attachments.

    REPLY FLOWS — reply_to_mail_message and reply_all_mail_message still use
    the draft+patch+send path in order to preserve References/In-Reply-To and
    correct reply threading. If a reply also arrives blank at a strict
    recipient, re-send as a new message (which uses the raw-MIME path) and
    manually reference the prior subject in the body.

    Args:
        to_recipients: Comma-separated recipient email addresses
        subject: Message subject
        body: Email body — HTML (with body_content_type="html") or plain text
        mailbox: Mailbox to send from (defaults to Athena's mailbox)
        cc_recipients: Optional comma-separated CC recipients
        bcc_recipients: Optional comma-separated BCC recipients
        body_content_type: "html" (default, recommended) or "text"
        importance: low, normal, or high
        request_read_receipt: Request a read receipt
        request_delivery_receipt: Request a delivery receipt
        attachment_document_ids: Optional comma-separated Athena document IDs to attach

    Returns:
        JSON string with keys: status, transport (see above), mailbox, subject,
        to_recipients, cc_recipients, bcc_recipients, importance,
        read_receipt_requested, delivery_receipt_requested, attachments.
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        to_list = _split_csv_list(to_recipients)
        cc_list = _split_csv_list(cc_recipients)
        bcc_list = _split_csv_list(bcc_recipients)
        if not to_list:
            raise ValueError("At least one recipient is required")

        body_html, body_text, graph_content_type = _prepare_email_body(body, body_content_type)
        normalized_importance = _normalize_importance(importance)
        attachment_ids_list = _split_csv_list(attachment_document_ids)
        subject_clean = subject.strip()

        # Raw-MIME path for HTML without attachments. Exchange's contentType=HTML path
        # produces Windows-1252 + quoted-printable multipart/alternative, which some
        # strict recipient tenants strip, delivering a blank body. Raw RFC-5322 UTF-8
        # base64 survives those rules.
        if graph_content_type == "HTML" and not attachment_ids_list:
            display_name = await _get_mailbox_display_name(mailbox)
            mime_bytes = _build_rfc5322_mime(
                sender=mailbox,
                sender_display_name=display_name,
                to_recipients=to_list,
                cc_recipients=cc_list,
                bcc_recipients=bcc_list,
                subject=subject_clean,
                body_html=body_html,
                body_text=body_text,
                importance=normalized_importance,
                request_read_receipt=bool(request_read_receipt),
                request_delivery_receipt=bool(request_delivery_receipt),
            )
            await _graph_send_raw_mime(mailbox, mime_bytes)
            return json.dumps({
                "status": "sent",
                "transport": "raw_mime_utf8_base64",
                "mailbox": mailbox,
                "subject": subject_clean,
                "to_recipients": to_list,
                "cc_recipients": cc_list,
                "bcc_recipients": bcc_list,
                "importance": normalized_importance.lower(),
                "read_receipt_requested": bool(request_read_receipt),
                "delivery_receipt_requested": bool(request_delivery_receipt),
                "attachments": [],
            })

        draft_payload = {
            "subject": subject_clean,
            "importance": normalized_importance,
            "body": {
                "contentType": graph_content_type,
                "content": body_html if graph_content_type == "HTML" else body_text,
            },
            "toRecipients": [{"emailAddress": {"address": a}} for a in to_list],
            "ccRecipients": [{"emailAddress": {"address": a}} for a in cc_list],
            "bccRecipients": [{"emailAddress": {"address": a}} for a in bcc_list],
            "isReadReceiptRequested": bool(request_read_receipt),
            "isDeliveryReceiptRequested": bool(request_delivery_receipt),
        }
        draft = await _graph_create_draft(mailbox, draft_payload)
        draft_id = draft.get("id")
        if not draft_id:
            raise RuntimeError("Graph did not return a draft message ID")

        uploaded_attachments = []
        if attachment_document_ids:
            uploaded_attachments = await _attach_documents_to_message(mailbox, draft_id, attachment_document_ids)

        await _graph_send_draft(mailbox, draft_id)
        return json.dumps({
            "status": "sent",
            "transport": "draft_attach_send",
            "mailbox": mailbox,
            "draft_id": draft_id,
            "subject": subject_clean,
            "to_recipients": to_list,
            "cc_recipients": cc_list,
            "bcc_recipients": bcc_list,
            "importance": normalized_importance.lower(),
            "read_receipt_requested": bool(request_read_receipt),
            "delivery_receipt_requested": bool(request_delivery_receipt),
            "attachments": uploaded_attachments,
        })
    except Exception as e:
        log.error("send_mail_message error: %s", e)
        return json.dumps({"error": str(e)})


async def _reply_to_mail_message_impl(
    *,
    message_id: str,
    body: str,
    mailbox: str,
    reply_all: bool,
    body_content_type: str,
    importance: Optional[str],
    request_read_receipt: Optional[bool],
    request_delivery_receipt: Optional[bool],
    attachment_document_ids: Optional[str],
) -> str:
    _require_scope("write")
    mailbox = _normalize_mailbox(mailbox)
    body_html, body_text, graph_content_type = _prepare_email_body(body, body_content_type)
    attachment_ids_list = _split_csv_list(attachment_document_ids)
    normalized_importance = (
        _normalize_importance(importance) if importance else "Normal"
    )
    rr_flag = bool(request_read_receipt) if request_read_receipt is not None else False
    dr_flag = bool(request_delivery_receipt) if request_delivery_receipt is not None else False

    # Create the reply draft. createReply/createReplyAll gives us:
    #   • Correct To/Cc recipient split (especially for reply-all)
    #   • Subject with "Re: " prefix
    #   • A pre-built quoted body with the nicely-formatted "From/Sent/To/Subject" divider
    draft = await _create_reply_draft(mailbox, message_id, reply_all=reply_all)
    draft_id = draft.get("id")
    if not draft_id:
        raise RuntimeError("Graph did not return a reply draft ID")

    # Raw-MIME reply path: no attachments and HTML body. We reuse the draft's
    # recipients/subject/quoted body, fetch the original for In-Reply-To /
    # References threading, build raw RFC-5322 MIME, delete the orphan draft,
    # and send via /sendMail. This bypasses Exchange's HTML→MIME re-encoding
    # that strict recipient tenants strip (blank-body symptom).
    if graph_content_type == "HTML" and not attachment_ids_list:
        try:
            draft_full = await _graph_request(
                "GET",
                f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{draft_id}",
            )
            reply_subject = (draft_full.get("subject") or "").strip()
            to_list = [
                r.get("emailAddress", {}).get("address", "")
                for r in draft_full.get("toRecipients", [])
                if r.get("emailAddress", {}).get("address")
            ]
            cc_list = [
                r.get("emailAddress", {}).get("address", "")
                for r in draft_full.get("ccRecipients", [])
                if r.get("emailAddress", {}).get("address")
            ]
            quoted_html = (draft_full.get("body") or {}).get("content") or ""

            original_graph_id = await _graph_resolve_message_graph_id(mailbox, message_id)
            original = await _graph_request(
                "GET",
                f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{original_graph_id}?$select=internetMessageId,internetMessageHeaders,subject",
            )
            original_message_id = (original.get("internetMessageId") or "").strip()
            original_references = ""
            for h in original.get("internetMessageHeaders", []) or []:
                if (h.get("name") or "").lower() == "references":
                    original_references = (h.get("value") or "").strip()
                    break

            in_reply_to_header = original_message_id if original_message_id else None
            references_header: Optional[str]
            if original_message_id and original_references:
                references_header = f"{original_references} {original_message_id}"
            elif original_message_id:
                references_header = original_message_id
            elif original_references:
                references_header = original_references
            else:
                references_header = None

            combined_html = body_html
            if quoted_html:
                combined_html = (
                    body_html
                    + '<br><div style="border-top:1px solid #ccc;margin:1em 0"></div>'
                    + quoted_html
                )
            combined_text = body_text
            if quoted_html:
                combined_text = body_text + "\n\n" + _html_to_text(quoted_html)

            display_name = await _get_mailbox_display_name(mailbox)
            mime_bytes = _build_rfc5322_mime(
                sender=mailbox,
                sender_display_name=display_name,
                to_recipients=to_list,
                cc_recipients=cc_list,
                bcc_recipients=[],
                subject=reply_subject,
                body_html=combined_html,
                body_text=combined_text,
                importance=normalized_importance,
                request_read_receipt=rr_flag,
                request_delivery_receipt=dr_flag,
                in_reply_to=in_reply_to_header,
                references=references_header,
            )
            await _graph_send_raw_mime(mailbox, mime_bytes)
            try:
                await _graph_request(
                    "DELETE",
                    f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{draft_id}",
                )
            except Exception as delete_err:
                log.warning("Failed to delete orphan reply draft %s: %s", draft_id, delete_err)

            return json.dumps({
                "status": "sent",
                "transport": "raw_mime_utf8_base64",
                "mailbox": mailbox,
                "reply_all": reply_all,
                "source_message_id": message_id,
                "draft_id": draft_id,
                "subject": reply_subject,
                "to_recipients": to_list,
                "cc_recipients": cc_list,
                "importance": normalized_importance.lower(),
                "read_receipt_requested": rr_flag,
                "delivery_receipt_requested": dr_flag,
                "attachments": [],
            })
        except Exception as raw_err:
            log.warning(
                "Raw-MIME reply path failed (%s); falling back to draft flow",
                raw_err,
            )

    patch_payload: dict[str, Any] = {
        "body": {
            "contentType": graph_content_type,
            "content": body_html if graph_content_type == "HTML" else body_text,
        },
    }
    if importance:
        patch_payload["importance"] = _normalize_importance(importance)
    if request_read_receipt is not None:
        patch_payload["isReadReceiptRequested"] = bool(request_read_receipt)
    if request_delivery_receipt is not None:
        patch_payload["isDeliveryReceiptRequested"] = bool(request_delivery_receipt)

    updated = await _graph_patch_message(mailbox, draft_id, patch_payload)
    uploaded_attachments = []
    if attachment_document_ids:
        uploaded_attachments = await _attach_documents_to_message(mailbox, draft_id, attachment_document_ids)

    await _graph_send_draft(mailbox, draft_id)
    return json.dumps({
        "status": "sent",
        "transport": "draft_attach_send",
        "mailbox": mailbox,
        "reply_all": reply_all,
        "source_message_id": message_id,
        "draft_id": draft_id,
        "subject": updated.get("subject") or draft.get("subject"),
        "importance": (updated.get("importance") or draft.get("importance") or "normal").lower(),
        "read_receipt_requested": updated.get("isReadReceiptRequested"),
        "delivery_receipt_requested": updated.get("isDeliveryReceiptRequested"),
        "attachments": uploaded_attachments,
    })


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def reply_to_mail_message(
    message_id: str,
    body: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    body_content_type: str = "html",
    importance: Optional[str] = None,
    request_read_receipt: Optional[bool] = None,
    request_delivery_receipt: Optional[bool] = None,
    attachment_document_ids: Optional[str] = None,
) -> str:
    """Reply to a message from Athena's mailbox.

    Transport (automatic, returned as "transport"):
      raw_mime_utf8_base64 — HTML reply without attachments. Calls createReply
        to reuse Graph's recipient split, Re: subject, and quoted-prior-body
        divider, fetches the original for In-Reply-To / References headers,
        then sends as raw RFC-5322 UTF-8 base64 MIME. Preserves threading AND
        bypasses Exchange's HTML→MIME re-encoding that strict recipient
        tenants strip (blank-body).
      draft_attach_send — Plain-text reply OR reply with attachments. Uses
        createReply → PATCH body → upload attachments → send via the draft.

    Supports attachment_document_ids, so Athena can attach documents she generated.

    Args:
        message_id: Graph message ID or RFC internet message ID to reply to
        body: Reply body — HTML (body_content_type="html") or plain text
        mailbox: Mailbox to send from (defaults to Athena's mailbox)
        body_content_type: html or text (default html)
        importance: Optional low, normal, or high importance override
        request_read_receipt: Optional read receipt flag for the outgoing reply
        request_delivery_receipt: Optional delivery receipt flag for the outgoing reply
        attachment_document_ids: Optional comma-separated Athena document IDs to attach
    """
    try:
        return await _reply_to_mail_message_impl(
            message_id=message_id,
            body=body,
            mailbox=mailbox,
            reply_all=False,
            body_content_type=body_content_type,
            importance=importance,
            request_read_receipt=request_read_receipt,
            request_delivery_receipt=request_delivery_receipt,
            attachment_document_ids=attachment_document_ids,
        )
    except Exception as e:
        log.error("reply_to_mail_message error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def reply_all_mail_message(
    message_id: str,
    body: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    body_content_type: str = "html",
    importance: Optional[str] = None,
    request_read_receipt: Optional[bool] = None,
    request_delivery_receipt: Optional[bool] = None,
    attachment_document_ids: Optional[str] = None,
) -> str:
    """Reply-all to a message from Athena's mailbox.

    Same transport selection as reply_to_mail_message:
      raw_mime_utf8_base64 — HTML without attachments; preserves threading and
        survives strict recipient mail hygiene.
      draft_attach_send — plain-text or when attachments are present.

    Supports attachment_document_ids, so Athena can attach generated documents.

    Args:
        message_id: Graph message ID or RFC internet message ID to reply-all to
        body: Reply-all body — HTML (body_content_type="html") or plain text
        mailbox: Mailbox to send from (defaults to Athena's mailbox)
        body_content_type: "html" (default) or "text"
        importance: Optional low, normal, or high importance override
        request_read_receipt: Optional read receipt flag for the outgoing reply-all
        request_delivery_receipt: Optional delivery receipt flag for the outgoing reply-all
        attachment_document_ids: Optional comma-separated Athena document IDs to attach
    """
    try:
        return await _reply_to_mail_message_impl(
            message_id=message_id,
            body=body,
            mailbox=mailbox,
            reply_all=True,
            body_content_type=body_content_type,
            importance=importance,
            request_read_receipt=request_read_receipt,
            request_delivery_receipt=request_delivery_receipt,
            attachment_document_ids=attachment_document_ids,
        )
    except Exception as e:
        log.error("reply_all_mail_message error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def update_mail_message(
    message_id: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    importance: Optional[str] = None,
    is_read: Optional[bool] = None,
    request_read_receipt: Optional[bool] = None,
    request_delivery_receipt: Optional[bool] = None,
    categories: Optional[str] = None,
) -> str:
    """Update a mailbox message via Graph.

    Use this to mark a message important, mark it read/unread, or adjust receipt flags.

    Args:
        message_id: Graph message ID or RFC internet message ID
        mailbox: Mailbox to update in (defaults to Athena's mailbox)
        importance: Optional low, normal, or high
        is_read: Optional true/false to mark read or unread
        request_read_receipt: Optional read receipt flag
        request_delivery_receipt: Optional delivery receipt flag
        categories: Optional comma-separated Outlook categories
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        graph_message_id = await _graph_resolve_message_graph_id(mailbox, message_id)
        payload: dict[str, Any] = {}
        if importance:
            payload["importance"] = _normalize_importance(importance)
        if is_read is not None:
            payload["isRead"] = bool(is_read)
        if request_read_receipt is not None:
            payload["isReadReceiptRequested"] = bool(request_read_receipt)
        if request_delivery_receipt is not None:
            payload["isDeliveryReceiptRequested"] = bool(request_delivery_receipt)
        if categories:
            payload["categories"] = _split_csv_list(categories)
        if not payload:
            raise ValueError("At least one updatable field must be provided")

        updated = await _graph_patch_message(mailbox, graph_message_id, payload)
        result = _message_summary(updated)
        result["mailbox"] = mailbox
        return json.dumps(result)
    except Exception as e:
        log.error("update_mail_message error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║           DOCUMENT CREATION (text) & UPLOAD (binary)         ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def create_document(
    title: str,
    content: str,
    domain: str = "general",
    document_type: str = "document",
    filename: Optional[str] = None,
    description: Optional[str] = None,
    file_format: str = "md",
) -> str:
    """Create and ingest a new document from text content into Athena.

    USE THIS when Claude creates a durable artifact that should be preserved in Athena —
    analyses, summaries, memos, reports, coding notes, research digests, or other work product.
    The content is saved as a file, chunked, embedded, and made searchable.
    For binary files (DOCX, PDF, images, XLSX), use create_document_upload_session instead.

    Args:
        title: Document title (e.g. "Nelson v Starbucks Case Summary")
        content: The full document text content (markdown, plain text, etc.)
        domain: Classification domain (default "general"). Call list_document_domains() if you're unsure which domain to use.
        document_type: Type — "document", "report", "memo", "analysis", "letter", "summary", "brief"
        filename: Optional filename (auto-generated from title if omitted)
        description: Optional notes about the document
        file_format: File extension — "md", "txt" (default "md")
    """
    try:
        domain = await _normalize_document_domain(domain)
        if not content or len(content.strip()) < 10:
            return json.dumps({"error": "Content is too short (minimum 10 characters)"})

        # Generate filename
        if not filename:
            safe_title = re.sub(r'[^\w\-]', '_', title)[:80]
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{ts}_{safe_title}.{file_format}"

        # Save to disk
        domain_dir = UPLOAD_DIR / domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        save_path = domain_dir / filename
        if save_path.exists():
            save_path = domain_dir / f"{uuid.uuid4().hex[:8]}_{filename}"
        save_path.write_text(content, encoding="utf-8")
        log.info("Created document: %s (%d chars)", save_path, len(content))

        # Chunk
        chunks = _chunk_text_upload(content)
        if not chunks:
            chunks = [content[:CHUNK_SIZE]]

        # Embed
        all_embeddings: list[list[float]] = []
        for i in range(0, len(chunks), 2000):
            batch = chunks[i:i + 2000]
            try:
                embs = await _embed_batch_upload(batch)
                all_embeddings.extend(embs)
            except Exception as e:
                log.error("Embedding failed for %s: %s", filename, e)
                all_embeddings.extend([[0.0] * EMBEDDING_DIMS] * len(batch))

        doc_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        meta = {
            "source": "claude-desktop-created",
            "created_by": "claude-desktop",
            "preserved_by": "athena-cognitive-engine",
            "preserve_requested": True,
            "file_type": file_format,
            "file_size": len(content.encode("utf-8")),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
            "local_attachment_path": str(save_path),
        }
        if description:
            meta["description"] = description

        p = await get_pool()
        async with p.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO core.documents
                        (id, domain, source_path, filename, document_type, title,
                         content_hash, total_chunks, full_content, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                    ON CONFLICT (id) DO NOTHING
                """,
                    uuid.UUID(doc_id), domain, str(save_path), filename,
                    document_type, title, content_hash, len(chunks),
                    content[:500000], json.dumps(meta),
                )
                for i, (chunk_text, emb) in enumerate(zip(chunks, all_embeddings)):
                    chunk_id = hashlib.md5(f"{save_path}:{i}".encode()).hexdigest()
                    vec_literal = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
                    await conn.execute("""
                        INSERT INTO core.document_chunks
                            (id, document_id, chunk_index, total_chunks,
                             content, embedding, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                        ON CONFLICT (id) DO NOTHING
                    """,
                        chunk_id, uuid.UUID(doc_id), i, len(chunks),
                        chunk_text, vec_literal, json.dumps({}),
                    )
                await conn.execute("""
                    INSERT INTO ops.ingestion_jobs
                        (source, domain, status, documents_processed, chunks_processed,
                         documents_failed, completed_at, metadata)
                    VALUES ($1, $2, 'completed', 1, $3, 0, now(), $4::jsonb)
                """,
                    "claude-desktop-created", domain, len(chunks),
                    json.dumps({"filename": filename, "document_id": doc_id}),
                )

        return json.dumps({
            "status": "ingested",
            "document_id": doc_id,
            "filename": filename,
            "title": title,
            "domain": domain,
            "document_type": document_type,
            "file_size": len(content.encode("utf-8")),
            "saved_to": str(save_path),
            "text_length": len(content),
            "chunks_created": len(chunks),
            "searchable": True,
        })
    except Exception as e:
        log.error("create_document error: %s", e)
        return json.dumps({"error": str(e)})

def _extract_pdf_text_upload(pdf_path: Path) -> str:
    try:
        from pdf2image import convert_from_path, pdfinfo_from_path
        import pytesseract
        try:
            info = pdfinfo_from_path(str(pdf_path))
            num_pages = info.get("Pages", 0)
        except Exception:
            num_pages = 0
        if num_pages > 0:
            parts = []
            for pn in range(1, num_pages + 1):
                try:
                    images = convert_from_path(str(pdf_path), dpi=OCR_DPI, first_page=pn, last_page=pn, thread_count=1)
                    if images:
                        txt = pytesseract.image_to_string(images[0])
                        if txt and txt.strip():
                            parts.append(f"[Page {pn}]\n{txt}")
                        del images
                except Exception:
                    pass
            return "\n\n".join(parts)
        else:
            images = convert_from_path(str(pdf_path), dpi=OCR_DPI, thread_count=1)
            return "\n\n".join(f"[Page {i}]\n{pytesseract.image_to_string(img)}" for i, img in enumerate(images, 1) if pytesseract.image_to_string(img).strip())
    except Exception:
        try:
            result = subprocess.run(["pdftotext", "-layout", str(pdf_path), "-"], capture_output=True, text=True, timeout=60)
            return result.stdout
        except Exception:
            return ""

def _extract_docx_text_upload(docx_path: Path) -> str:
    try:
        with zipfile.ZipFile(docx_path) as z:
            with z.open("word/document.xml") as f:
                tree = ET.parse(f)
                root = tree.getroot()
                ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                return "\n".join("".join(r.text or "" for r in p.findall(".//w:t", ns)) for p in root.findall(".//w:p", ns) if "".join(r.text or "" for r in p.findall(".//w:t", ns)))
    except Exception:
        return ""

def _convert_docx_to_pdf_sync(docx_path: Path, out_dir: Optional[Path] = None) -> Path:
    """Convert a DOCX/DOC file to PDF using headless LibreOffice.

    Raises RuntimeError on failure. Returns the path to the generated PDF.
    """
    ext = docx_path.suffix.lower()
    if ext not in (".docx", ".doc", ".odt", ".rtf"):
        raise RuntimeError(f"Unsupported source extension for PDF conversion: {ext}")
    if not docx_path.exists():
        raise RuntimeError(f"Source file not found: {docx_path}")

    output_dir = Path(out_dir) if out_dir else CONVERTED_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # Use an isolated user profile per invocation so concurrent conversions don't collide.
    profile_dir = output_dir / f"_profile_{uuid.uuid4().hex}"
    profile_dir.mkdir(parents=True, exist_ok=True)
    try:
        proc = subprocess.run(
            [
                SOFFICE_BIN,
                f"-env:UserInstallation=file://{profile_dir}",
                "--headless",
                "--nologo",
                "--nofirststartwizard",
                "--norestore",
                "--convert-to",
                "pdf",
                "--outdir",
                str(output_dir),
                str(docx_path),
            ],
            capture_output=True,
            text=True,
            timeout=DOCX_CONVERT_TIMEOUT_SECONDS,
        )
    finally:
        shutil.rmtree(profile_dir, ignore_errors=True)

    expected_pdf = output_dir / f"{docx_path.stem}.pdf"
    if proc.returncode != 0 or not expected_pdf.exists():
        stderr = (proc.stderr or "").strip()[:500]
        stdout = (proc.stdout or "").strip()[:500]
        raise RuntimeError(
            f"LibreOffice conversion failed (rc={proc.returncode}): {stderr or stdout or 'no output'}"
        )
    # Give the PDF a collision-safe stable name inside output_dir.
    final_pdf = output_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{docx_path.stem}.pdf"
    shutil.move(str(expected_pdf), str(final_pdf))
    return final_pdf


def _extract_text_for_upload(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        return _extract_pdf_text_upload(file_path)
    elif ext in (".docx", ".doc"):
        return _extract_docx_text_upload(file_path)
    elif ext in (".md", ".txt", ".csv", ".json"):
        try:
            return file_path.read_text(errors="replace")
        except Exception:
            return ""
    elif ext in (".png", ".jpg", ".jpeg", ".tiff", ".tif"):
        try:
            import pytesseract
            from PIL import Image
            return pytesseract.image_to_string(Image.open(file_path))
        except Exception:
            return ""
    return ""

def _chunk_text_upload(text: str) -> list[str]:
    if not text:
        return []
    chunks, current, total = [], [], 0
    for para in text.split("\n\n"):
        if not para.strip():
            continue
        if total + len(para) > CHUNK_SIZE and current:
            chunks.append("\n\n".join(current))
            # Keep overlap
            while total > CHUNK_OVERLAP and len(current) > 1:
                total -= len(current[0])
                current.pop(0)
        current.append(para)
        total += len(para)
    if current:
        chunks.append("\n\n".join(current))
    return chunks or [text[:CHUNK_SIZE]]

async def _embed_batch_upload(texts: list[str]) -> list[list[float]]:
    return embed_texts_sync(texts)


async def _ingest_saved_upload(
    save_path: Path,
    filename: str,
    domain: str,
    title: Optional[str],
    document_type: Optional[str],
    description: Optional[str],
    source_label: str,
) -> dict[str, Any]:
    ext = save_path.suffix.lower() or Path(filename).suffix.lower()
    file_size = save_path.stat().st_size
    text = await asyncio.to_thread(_extract_text_for_upload, save_path)

    doc_id = str(uuid.uuid4())
    doc_title = title or Path(filename).stem.replace("_", " ").replace("-", " ")
    meta = {
        "source": source_label,
        "created_by": _current_client_id(),
        "preserved_by": "athena-cognitive-engine",
        "preserve_requested": True,
        "file_type": ext.lstrip("."),
        "file_size": file_size,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "local_attachment_path": str(save_path),
    }
    if description:
        meta["description"] = description

    resolved_document_type = document_type or _default_document_type_for_extension(ext)
    p = await get_pool()

    if not text or len(text.strip()) < 10:
        async with p.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, 0, '', $8::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                uuid.UUID(doc_id),
                domain,
                str(save_path),
                filename,
                resolved_document_type,
                doc_title,
                await asyncio.to_thread(_file_sha256, save_path),
                json.dumps(meta),
            )
        return {
            "status": "saved",
            "document_id": doc_id,
            "filename": filename,
            "file_size": file_size,
            "saved_to": str(save_path),
            "ingested": False,
            "warning": "Text extraction returned no content — original saved but not searchable",
        }

    chunks = _chunk_text_upload(text)
    log.info("Uploading %s: %d chars, %d chunks", filename, len(text), len(chunks))

    all_embeddings: list[list[float]] = []
    for i in range(0, len(chunks), 2000):
        batch = chunks[i:i + 2000]
        try:
            embs = await _embed_batch_upload(batch)
            all_embeddings.extend(embs)
        except Exception as e:
            log.error("Embedding failed for %s: %s", filename, e)
            all_embeddings.extend([[0.0] * EMBEDDING_DIMS] * len(batch))

    content_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
    async with p.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO core.documents
                    (id, domain, source_path, filename, document_type, title,
                     content_hash, total_chunks, full_content, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (id) DO NOTHING
                """,
                uuid.UUID(doc_id),
                domain,
                str(save_path),
                filename,
                resolved_document_type,
                doc_title,
                content_hash,
                len(chunks),
                text[:500000],
                json.dumps(meta),
            )
            for i, (chunk_text, emb) in enumerate(zip(chunks, all_embeddings)):
                chunk_id = hashlib.md5(f"{save_path}:{i}".encode()).hexdigest()
                vec_literal = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
                await conn.execute(
                    """
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks,
                         content, embedding, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec, $7::jsonb)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    chunk_id,
                    uuid.UUID(doc_id),
                    i,
                    len(chunks),
                    chunk_text,
                    vec_literal,
                    json.dumps({}),
                )
            await conn.execute(
                """
                INSERT INTO ops.ingestion_jobs
                    (source, domain, status, documents_processed, chunks_processed,
                     documents_failed, completed_at, metadata)
                VALUES ($1, $2, 'completed', 1, $3, 0, now(), $4::jsonb)
                """,
                source_label,
                domain,
                len(chunks),
                json.dumps({"filename": filename, "document_id": doc_id}),
            )

    return {
        "status": "ingested",
        "document_id": doc_id,
        "filename": filename,
        "title": doc_title,
        "domain": domain,
        "document_type": resolved_document_type,
        "file_size": file_size,
        "saved_to": str(save_path),
        "text_length": len(text),
        "chunks_created": len(chunks),
        "searchable": True,
    }


DEFAULT_DOCUMENT_DOMAIN_SPECS = (
    {
        "name": "general",
        "label": "General",
        "description": "General notes, summaries, memos, and other catch-all documents.",
        "sort_order": 10,
    },
    {
        "name": "legal",
        "label": "Legal",
        "description": "Court filings, legal correspondence, case analysis, and litigation work product.",
        "sort_order": 20,
    },
    {
        "name": "medical",
        "label": "Medical",
        "description": "Medical records, care notes, treatment summaries, and health documentation.",
        "sort_order": 30,
    },
    {
        "name": "paperless",
        "label": "Paperless",
        "description": "Documents synchronized from Paperless-ngx or equivalent paper archive workflows.",
        "sort_order": 40,
    },
    {
        "name": "web",
        "label": "Web",
        "description": "Web-crawled pages, website captures, and online reference material.",
        "sort_order": 50,
    },
    {
        "name": "coding",
        "label": "Coding",
        "description": "Code notes, architecture docs, implementation plans, debugging writeups, and engineering artifacts.",
        "sort_order": 60,
    },
    {
        "name": "research",
        "label": "Research",
        "description": "Background research, source digests, investigation summaries, and exploratory notes.",
        "sort_order": 70,
    },
    {
        "name": "business",
        "label": "Business",
        "description": "Business plans, strategy documents, vendor notes, and operational paperwork.",
        "sort_order": 80,
    },
    {
        "name": "finance",
        "label": "Finance",
        "description": "Financial analyses, invoices, budgets, statements, and related supporting material.",
        "sort_order": 90,
    },
    {
        "name": "personal",
        "label": "Personal",
        "description": "Personal reference material, household notes, and non-business miscellaneous documents.",
        "sort_order": 100,
    },
    {
        "name": "operations",
        "label": "Operations",
        "description": "Runbooks, process docs, service notes, and administrative operations artifacts.",
        "sort_order": 110,
    },
)
DEFAULT_DOCUMENT_DOMAIN = "general"
DEFAULT_DOCUMENT_DOMAINS = tuple(spec["name"] for spec in DEFAULT_DOCUMENT_DOMAIN_SPECS)
_DOCUMENT_DOMAIN_CACHE: dict[str, Any] = {
    "domains": [dict(spec) for spec in DEFAULT_DOCUMENT_DOMAIN_SPECS],
    "expires_at": 0.0,
}


def _document_domain_names(catalog: Sequence[dict[str, Any]]) -> tuple[str, ...]:
    names = [str(item.get("name", "")).strip().lower() for item in catalog if item.get("name")]
    return tuple(dict.fromkeys(names)) or DEFAULT_DOCUMENT_DOMAINS


async def _get_document_domain_catalog(force_refresh: bool = False) -> list[dict[str, Any]]:
    now = time.time()
    if not force_refresh and now < float(_DOCUMENT_DOMAIN_CACHE.get("expires_at", 0.0)):
        return [dict(item) for item in _DOCUMENT_DOMAIN_CACHE.get("domains", [])]

    catalog = [dict(spec) for spec in DEFAULT_DOCUMENT_DOMAIN_SPECS]
    try:
        p = await get_pool()
        async with p.acquire() as conn:
            rows = await conn.fetch("""
                SELECT name, label, description, is_active, sort_order
                FROM core.document_domains
                WHERE is_active = true
                ORDER BY sort_order, name
            """)

        if rows:
            catalog = [
                {
                    "name": (row["name"] or "").strip().lower(),
                    "label": row["label"] or row["name"],
                    "description": row["description"] or "",
                    "is_active": bool(row["is_active"]),
                    "sort_order": row["sort_order"] or 100,
                }
                for row in rows
                if row["name"]
            ]
    except Exception as e:
        log.warning("Falling back to default document domains: %s", e)

    _DOCUMENT_DOMAIN_CACHE["domains"] = [dict(item) for item in catalog]
    _DOCUMENT_DOMAIN_CACHE["expires_at"] = now + 300.0
    return [dict(item) for item in catalog]


async def _normalize_document_domain(domain: str) -> str:
    normalized = (domain or "").strip().lower()
    valid_domains = _document_domain_names(await _get_document_domain_catalog())
    if normalized not in valid_domains:
        allowed = ", ".join(valid_domains)
        raise ValueError(f"domain must be one of: {allowed}")
    return normalized


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_document_domains() -> str:
    """List the currently available Athena document domains and how to use them.

    Use this when choosing a domain for create_document, create_document_upload_session, or create_note.
    The results come from Athena's domain registry when available, with safe defaults if the
    registry is unavailable.
    """
    try:
        catalog = await _get_document_domain_catalog(force_refresh=True)
        return json.dumps({
            "default_domain": DEFAULT_DOCUMENT_DOMAIN,
            "count": len(catalog),
            "domains": catalog,
            "guidance": "Use create_document or create_document_upload_session to preserve substantial Claude-created artifacts in Athena.",
        })
    except Exception as e:
        log.error("list_document_domains error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def create_document_upload_session(
    filename: str,
    file_size: int,
    domain: str = DEFAULT_DOCUMENT_DOMAIN,
    content_type: Optional[str] = None,
    title: Optional[str] = None,
    document_type: Optional[str] = None,
    description: Optional[str] = None,
    sha256: Optional[str] = None,
    expires_in_seconds: Optional[int] = None,
    auto_convert_to_pdf: bool = False,
) -> str:
    """Create a Graph-style raw HTTP upload session for a document.

    This is the only binary-upload path. Upload raw chunks to the returned `upload_url`
    with `PUT` and a `Content-Range` header, then `GET` the same URL for status or
    `DELETE` it to cancel.

    Args:
        filename: Original filename with extension.
        file_size: Total file size in bytes.
        domain: Athena document domain for the eventual stored document.
        content_type: Optional MIME type (guessed from filename if omitted).
        title: Optional human-readable title.
        document_type: Optional Athena document type.
        description: Optional notes stored with the document.
        sha256: Optional expected SHA-256 hex digest used to verify the completed upload.
        expires_in_seconds: Optional TTL for the upload session.
        auto_convert_to_pdf: If True and the uploaded file is DOCX/DOC/ODT/RTF, the
            server renders a PDF once the upload finishes and exposes `pdf_document_id`,
            `pdf_download_url`, and `pdf_view_url` in the final completion payload.
    """
    try:
        _require_scope("write")
        await _ensure_transfer_schema()

        normalized_domain = await _normalize_document_domain(domain)
        normalized_filename = _safe_upload_filename(filename)
        total_size = int(file_size)
        if total_size <= 0:
            return json.dumps({"error": "file_size must be greater than zero"})
        if total_size > TRANSFER_SESSION_FILE_LIMIT:
            return json.dumps({"error": f"File exceeds {TRANSFER_SESSION_FILE_LIMIT} byte limit"})

        normalized_sha256 = (sha256 or "").strip().lower() or None
        if normalized_sha256 and not re.fullmatch(r"[0-9a-f]{64}", normalized_sha256):
            return json.dumps({"error": "sha256 must be a 64-character hexadecimal digest"})

        ttl = max(60, min(int(expires_in_seconds or TRANSFER_UPLOAD_SESSION_TTL_SECONDS), 86400))
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        session_id = uuid.uuid4()
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        temp_path = TRANSFER_UPLOAD_DIR / f"{session_id}.part"
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path.touch(exist_ok=True)

        guessed_content_type = content_type or _guess_mime_type(normalized_filename)
        auto_convert_flag = bool(auto_convert_to_pdf)
        if auto_convert_flag:
            upload_ext = Path(normalized_filename).suffix.lower()
            if upload_ext not in (".docx", ".doc", ".odt", ".rtf"):
                return json.dumps({
                    "error": (
                        f"auto_convert_to_pdf requires a DOCX/DOC/ODT/RTF filename "
                        f"(got '{upload_ext}')"
                    ),
                })
        initial_metadata = {
            "transport": "raw-http",
            "created_via": "create_document_upload_session",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "auto_convert_to_pdf": auto_convert_flag,
        }

        p = await get_pool()
        await p.execute(
            """
            INSERT INTO ops.file_transfer_sessions
                (session_id, direction, token_hash, status, client_id, filename,
                 content_type, domain, title, document_type, description,
                 total_size, next_offset, temp_path, sha256, metadata, expires_at)
            VALUES ($1, 'upload', $2, 'pending', $3, $4,
                    $5, $6, $7, $8, $9,
                    $10, 0, $11, $12, $13::jsonb, $14)
            """,
            session_id,
            token_hash,
            _current_client_id(),
            normalized_filename,
            guessed_content_type,
            normalized_domain,
            title,
            document_type,
            description,
            total_size,
            str(temp_path),
            normalized_sha256,
            json.dumps(initial_metadata),
            expires_at,
        )

        return json.dumps({
            "session_id": str(session_id),
            "filename": normalized_filename,
            "content_type": guessed_content_type,
            "domain": normalized_domain,
            "total_size": total_size,
            "upload_url": _build_transfer_url("upload", token),
            "status_url": _build_transfer_url("upload", token),
            "cancel_url": _build_transfer_url("upload", token),
            "method": "PUT",
            "max_chunk_size": TRANSFER_UPLOAD_MAX_CHUNK_SIZE,
            "expires_at": expires_at.isoformat(),
            "expirationDateTime": expires_at.isoformat(),
            "next_expected_ranges": ["0-"],
            "nextExpectedRanges": ["0-"],
            "sha256": normalized_sha256,
            "auto_convert_to_pdf": auto_convert_flag,
            "note": "Upload raw bytes with PUT + Content-Range. GET the same URL for status; DELETE cancels the session.",
        })
    except Exception as e:
        log.error("create_document_upload_session error: %s", e)
        return json.dumps({"error": str(e)})


async def _create_download_session_row(
    document_id: str,
    disposition: str = "attachment",
    expires_in_seconds: Optional[int] = None,
) -> dict[str, Any]:
    """Allocate a raw HTTP download session for a document and return its URL payload."""
    await _ensure_transfer_schema()
    normalized_disposition = _normalize_download_disposition(disposition)
    ttl = max(60, min(int(expires_in_seconds or TRANSFER_DOWNLOAD_SESSION_TTL_SECONDS), 86400))
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
    descriptor = await _resolve_original_document_download(document_id)

    session_id = uuid.uuid4()
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    metadata = {
        "transport": "raw-http",
        "source_kind": descriptor.get("source_kind"),
        "original_available": descriptor.get("original_available", False),
        "sha256": descriptor.get("sha256"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    p = await get_pool()
    await p.execute(
        """
        INSERT INTO ops.file_transfer_sessions
            (session_id, direction, token_hash, status, client_id, filename,
             content_type, disposition, total_size, document_id, metadata, expires_at)
        VALUES ($1, 'download', $2, 'ready', $3, $4,
                $5, $6, $7, $8::uuid, $9::jsonb, $10)
        """,
        session_id,
        token_hash,
        _current_client_id(),
        descriptor["filename"],
        descriptor["mime_type"],
        normalized_disposition,
        int(descriptor["total_size"]),
        document_id,
        json.dumps(metadata),
        expires_at,
    )

    return {
        "session_id": str(session_id),
        "document_id": document_id,
        "filename": descriptor["filename"],
        "mime_type": descriptor["mime_type"],
        "total_size": int(descriptor["total_size"]),
        "sha256": descriptor["sha256"],
        "disposition": normalized_disposition,
        "download_url": _build_transfer_url("download", token),
        "method": "GET",
        "supports_range": True,
        "expires_at": expires_at.isoformat(),
        "expirationDateTime": expires_at.isoformat(),
        "original_available": descriptor.get("original_available", False),
        "source_kind": descriptor.get("source_kind", "unknown"),
    }


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def create_document_download_session(
    document_id: str,
    disposition: str = "attachment",
    expires_in_seconds: Optional[int] = None,
) -> str:
    """Create a raw HTTP download URL for a document's original bytes.

    This is the only file-download path. Bytes travel over HTTP (GET, with optional
    `Range: bytes=...` for partial reads or resume) — not in MCP JSON payloads.

    Args:
        document_id: UUID of the document to download.
        disposition: `attachment` or `inline`.
        expires_in_seconds: Optional TTL for the download session.
    """
    try:
        payload = await _create_download_session_row(document_id, disposition, expires_in_seconds)
        return json.dumps(payload)
    except Exception as e:
        log.error("create_document_download_session error: %s", e)
        return json.dumps({"error": str(e)})


async def _run_docx_to_pdf_pipeline(source_docx_doc_id: str) -> dict[str, Any]:
    """Convert an already-ingested DOCX document to PDF.

    Produces a new `core.documents` row for the PDF, links it to the source via a
    `converted_from` relationship, and returns download/view URL payloads.
    """
    p = await get_pool()
    doc = await p.fetchrow(
        "SELECT id, domain, source_path, filename, title, document_type, metadata "
        "FROM core.documents WHERE id = $1::uuid",
        source_docx_doc_id,
    )
    if not doc:
        raise RuntimeError(f"Document not found: {source_docx_doc_id}")

    filename = (doc["filename"] or "").strip()
    src_path_str = (doc["source_path"] or "").strip()
    if src_path_str.startswith("file://"):
        src_path_str = src_path_str[7:]
    source_file = Path(src_path_str) if src_path_str else None

    if not source_file or not source_file.exists():
        raise RuntimeError(
            f"Source DOCX file is not available on disk for document {source_docx_doc_id}"
        )

    ext = source_file.suffix.lower() or Path(filename).suffix.lower()
    if ext not in (".docx", ".doc", ".odt", ".rtf"):
        raise RuntimeError(
            f"Document {source_docx_doc_id} is not a DOCX/DOC/ODT/RTF source (ext={ext})"
        )

    pdf_path = await asyncio.to_thread(_convert_docx_to_pdf_sync, source_file, CONVERTED_DIR)

    pdf_filename = f"{Path(filename).stem or 'document'}.pdf"
    pdf_title = (doc["title"] or Path(filename).stem.replace("_", " ").replace("-", " ")) + " (PDF)"
    pdf_domain = doc["domain"] or DEFAULT_DOCUMENT_DOMAIN
    pdf_description = f"PDF rendered from DOCX source document {source_docx_doc_id}"

    ingest_result = await _ingest_saved_upload(
        pdf_path,
        pdf_filename,
        pdf_domain,
        pdf_title,
        "document",
        pdf_description,
        "docx-to-pdf-conversion",
    )
    pdf_document_id = ingest_result["document_id"]

    try:
        await p.execute(
            """
            INSERT INTO core.document_relationships
                (source_document_id, target_document_id, relationship_type, confidence)
            VALUES ($1::uuid, $2::uuid, 'converted_from', 1.0)
            ON CONFLICT (source_document_id, target_document_id, relationship_type) DO NOTHING
            """,
            pdf_document_id,
            source_docx_doc_id,
        )
    except Exception as rel_err:
        log.warning("Failed to record converted_from relationship: %s", rel_err)

    download_payload = await _create_download_session_row(pdf_document_id, "attachment")
    view_payload = await _create_download_session_row(pdf_document_id, "inline")

    return {
        "source_document_id": source_docx_doc_id,
        "pdf_document_id": pdf_document_id,
        "pdf_filename": pdf_filename,
        "pdf_title": pdf_title,
        "pdf_size_bytes": pdf_path.stat().st_size,
        "pdf_local_path": str(pdf_path),
        "ingest_result": ingest_result,
        "download_url": download_payload["download_url"],
        "view_url": view_payload["download_url"],
        "download_session": download_payload,
        "view_session": view_payload,
    }


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def convert_docx_to_pdf(document_id: str) -> str:
    """Convert an ingested DOCX/DOC document into a PDF and make it viewable/downloadable.

    The source document must already be stored in Athena (see `create_document_upload_session`)
    and must point to a DOCX/DOC/ODT/RTF file on disk.
    The rendered PDF is ingested as a new document and linked back to its source via a
    `converted_from` relationship. The response includes a `download_url`
    (Content-Disposition: attachment) and a `view_url` (inline) — both follow the
    standard raw HTTP download session flow.

    Args:
        document_id: UUID of the DOCX-family document to convert.
    """
    try:
        _require_scope("write")
        result = await _run_docx_to_pdf_pipeline(document_id)
        return json.dumps(result)
    except Exception as e:
        log.error("convert_docx_to_pdf error: %s", e)
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
async def rag_query(
    question: str,
    domain: Optional[str] = None,
    case_number: Optional[str] = None,
    top_k: int = 15,
    semantic_weight: Optional[float] = None,
    fulltext_weight: Optional[float] = None,
    expand_neighbors: Optional[int] = None,
) -> str:
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
        domain: Restrict to domain (for example general, coding, legal, medical, paperless, web) or omit for all
        case_number: Filter to a specific case (partial match, e.g. "26-2-00762")
        top_k: Number of context chunks to retrieve (default 15)
        semantic_weight: Optional weight for semantic results (default 0.7)
        fulltext_weight: Optional weight for full-text results (default 0.3)
        expand_neighbors: Include adjacent chunks for context (default 1, max 3)
    """
    top_k = min(max(top_k, 1), 30)
    p = await get_pool()
    try:
        question = _normalize_query(question)
        auto_case = _extract_case_number(question)
        if not case_number and auto_case:
            case_number = auto_case

        semantic_weight, fulltext_weight = _coerce_weights(semantic_weight, fulltext_weight)
        neighbor_window = _coerce_neighbor_window(expand_neighbors)

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
                   (c.embedding::halfvec(1024) <=> $1::halfvec(1024)) AS distance,
                   em.sender, em.date_sent, em.subject
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE c.embedding IS NOT NULL {sem_where}
            ORDER BY c.embedding::halfvec(1024) <=> $1::halfvec(1024)
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
            chunk_scores[cid] = chunk_scores.get(cid, 0) + semantic_weight / (rrf_k + rank + 1)
            if cid not in chunk_data:
                chunk_data[cid] = dict(r)

        for rank, r in enumerate(ft_rows):
            cid = r["chunk_id"]
            chunk_scores[cid] = chunk_scores.get(cid, 0) + fulltext_weight / (rrf_k + rank + 1)
            if cid not in chunk_data:
                chunk_data[cid] = dict(r)

        # Sort by RRF score, then rerank the top-N with bge-reranker-v2-m3.
        # Rerank improves top-k ordering dramatically for question-style
        # queries; we run it on a truncated candidate pool to keep latency
        # bounded (CPU path: ~50ms/pair, so 50 candidates ≈ 2.5s).
        sorted_chunks = sorted(chunk_scores.items(), key=lambda x: x[1], reverse=True)

        rerank_pool = min(len(sorted_chunks), max(top_k * 4, 50))
        rerank_candidates = sorted_chunks[:rerank_pool]

        rerank_scores: dict[str, float] = {}
        if rerank_candidates:
            try:
                passages = [chunk_data[cid]["content"] for cid, _ in rerank_candidates]
                ranked = await rerank(question, passages)
                for (cid, _), (orig_idx, score) in zip(
                    rerank_candidates,
                    sorted(ranked, key=lambda x: x[0]),  # score back to original order
                ):
                    rerank_scores[cid] = float(score)
            except Exception as e:
                log.warning("rerank failed, falling back to RRF order: %s", e)

        # Final ordering: rerank score if available, else RRF score. Candidates
        # outside the rerank pool fall through on RRF alone.
        def _final_score(item):
            cid, rrf = item
            return rerank_scores.get(cid, -1e9), rrf
        sorted_chunks = sorted(sorted_chunks, key=_final_score, reverse=True)

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

            contexts.append({
                "source": r["title"] or r["filename"],
                "type": r["document_type"],
                "domain": r["domain"],
                "rrf_score": round(score, 6),
                "rerank_score": round(rerank_scores[cid], 6) if cid in rerank_scores else None,
                "chunk_index": r["chunk_index"],
                "text": r["content"],
                "_header": (
                    f"[{r['domain']}/{r['document_type']}] {r['title'] or r['filename']}"
                    + (f"\nFrom: {r['sender']} | Date: {_ser(r['date_sent'])} | Subject: {r['subject']}"
                       if r.get("sender") else "")
                ),
                "_doc_id": doc_id,
                "_chunk_index": r["chunk_index"],
            })

        if neighbor_window > 0:
            for c in contexts:
                doc_id = c.get("_doc_id")
                chunk_index = c.get("_chunk_index")
                if not doc_id or chunk_index is None:
                    continue
                window_rows = await _get_chunk_window(p, doc_id, int(chunk_index), neighbor_window)
                if window_rows:
                    combined = "\n\n".join(
                        f"[chunk {r['chunk_index']}] {r['content']}" for r in window_rows
                    )
                    c["text"] = f"{c['_header']}\n\n{combined}"
                    c["chunk_range"] = {
                        "start": window_rows[0]["chunk_index"],
                        "end": window_rows[-1]["chunk_index"],
                    }
                else:
                    c["text"] = f"{c['_header']}\n\n{c['text']}"
        else:
            for c in contexts:
                c["text"] = f"{c['_header']}\n\n{c['text']}"

        # Remove internal fields
        for c in contexts:
            c.pop("_doc_id", None)
            c.pop("_chunk_index", None)
            c.pop("_header", None)

        return json.dumps({
            "question": question,
            "context_chunks": contexts,
            "count": len(contexts),
            "search_method": (
                f"hybrid_rrf (semantic={semantic_weight:.2f}, fulltext={fulltext_weight:.2f}, "
                f"neighbors={neighbor_window})"
            ),
            "case_number": case_number,
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
    document_type: str = "note",
) -> str:
    """Create a new document/note in Athena and make it fully searchable.

    Saves the original as a file on disk, chunks for large documents, embeds all
    chunks for semantic search, and optionally links to a legal case.

    Use this whenever Claude creates content that should be preserved in Athena —
    analyses, summaries, memos, research notes, case briefs, letters, etc.

    Args:
        title: Document title (e.g. "Nelson v Starbucks Case Analysis")
        content: Full document text (markdown or plain text, any length)
        domain: Domain for the note (default legal). Available domains are registry-backed and can include values like general, coding, research, legal, medical, paperless, or web.
        case_number: Optional case number to associate (e.g. "24-2-01031-31")
        tags: Comma-separated tags (e.g. "analysis,nelson,important")
        document_type: Type — "note", "analysis", "memo", "summary", "brief", "letter", "report"
    """
    p = await get_pool()
    try:
        domain = await _normalize_document_domain(domain)
        doc_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        # Save to disk as a file
        safe_title = re.sub(r'[^\w\-]', '_', title)[:80]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ts}_{safe_title}.md"
        domain_dir = UPLOAD_DIR / domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        save_path = domain_dir / filename
        save_path.write_text(f"# {title}\n\n{content}", encoding="utf-8")

        meta = {
            "source": "claude-desktop-created",
            "created_by": "ai_assistant",
            "preserved_by": "athena-cognitive-engine",
            "preserve_requested": True,
            "file_type": "md",
            "file_size": len(content.encode("utf-8")),
            "local_attachment_path": str(save_path),
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

        await p.execute("""
            INSERT INTO core.documents (id, domain, source_path, filename, document_type,
                                        title, full_content, content_hash, total_chunks, metadata)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, 0, $9::jsonb)
        """, doc_id, domain, str(save_path), filename, document_type,
             title, content[:500000], content_hash, json.dumps(meta))

        # Case linking
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

        # Tags
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

        # Chunk and embed
        chunks_created = 0
        try:
            chunks = _chunk_text_upload(content) if len(content) > CHUNK_SIZE else [content]
            all_embeddings = []
            for i in range(0, len(chunks), 2000):
                batch = chunks[i:i + 2000]
                try:
                    embs = await _embed_batch_upload(batch)
                    all_embeddings.extend(embs)
                except Exception:
                    all_embeddings.extend([[0.0] * EMBEDDING_DIMS] * len(batch))

            for i, (chunk_text, emb) in enumerate(zip(chunks, all_embeddings)):
                chunk_id = hashlib.md5(f"{save_path}:{i}".encode()).hexdigest()
                vec_str = "[" + ",".join(f"{v:.6f}" for v in emb) + "]"
                await p.execute("""
                    INSERT INTO core.document_chunks
                        (id, document_id, chunk_index, total_chunks, content, embedding, metadata)
                    VALUES ($1, $2::uuid, $3, $4, $5, $6::halfvec(1024), '{}'::jsonb)
                    ON CONFLICT (id) DO NOTHING
                """, chunk_id, doc_id, i, len(chunks), chunk_text, vec_str)

            await p.execute("UPDATE core.documents SET total_chunks = $1 WHERE id = $2::uuid",
                            len(chunks), doc_id)
            chunks_created = len(chunks)
        except Exception as embed_err:
            log.warning("Note created but embedding failed: %s", embed_err)

        return json.dumps({
            "success": True,
            "document_id": doc_id,
            "title": title,
            "domain": domain,
            "document_type": document_type,
            "saved_to": str(save_path),
            "chunks_created": chunks_created,
            "searchable": chunks_created > 0,
            "text_length": len(content),
        })
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

    ⚠️  DATABASE: PostgreSQL 17 with pgvector (NOT SQLite)
    This database has BOTH:
    - Regular PostgreSQL relational tables (for structured queries)
    - pgvector semantic search (for meaning-based document search)
    
    ⚠️  PREFER these tools instead:
    - rag_query: for answering questions about case content (uses pgvector)
    - semantic_search: for finding documents by meaning (uses pgvector)
    - search_emails, search_filings, list_cases: for structured queries

    Only use execute_sql for:
    - Database statistics (COUNT, aggregate queries)
    - Schema exploration (PostgreSQL information_schema queries)
    - Advanced PostgreSQL queries the other tools cannot handle
    - Queries requiring PostgreSQL-specific features (CTEs, window functions, JSONB, etc.)

    PostgreSQL Features Available:
    - Regular SQL: Tables, joins, indexes, constraints
    - Full-text search: tsvector, tsquery, to_tsquery (NOT SQLite FTS)
    - JSONB: Native JSON storage with indexing and operators
    - pgvector: Semantic similarity search via vector embeddings
    - Window functions, CTEs, recursive queries
    - Arrays, ranges, and custom types
    - Advanced date/time, regex, and string functions

    Only SELECT queries allowed. Tables: core.documents, legal.cases, etc.
    Use schema-qualified names (schema.table) for all queries.

    Args:
        query: PostgreSQL SELECT query (schema.table format required, PostgreSQL syntax only)
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
        
        # Get recent findings — open status, skip suggestion/info noise,
        # dedupe repeated identical titles (e.g. periodic "reasoning cycle
        # failed" warnings) so the context surfaces signal not noise.
        finding_rows = await p.fetch("""
            SELECT DISTINCT ON (title) category, severity, title, created_at
            FROM ops.agent_findings
            WHERE agent_id = $1
              AND status = 'open'
              AND severity IN ('critical', 'warning')
              AND created_at > now() - interval '30 days'
            ORDER BY title, created_at DESC
        """, agent_id)

        # Re-sort by recency since DISTINCT ON forces a different order.
        recent_findings = sorted(
            [_row_dict(r) for r in finding_rows],
            key=lambda r: r["created_at"],
            reverse=True,
        )[:10]
        
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

async def _embed_agent_note(title: str, content: str) -> tuple[Optional[str], Optional[int]]:
    """Embed (title + content) for an agent note. Returns (vec_literal, model_id) or (None, None) on failure."""
    body = f"{title}\n\n{content}".strip()
    if not body:
        return None, None
    try:
        vec = await _embed_query(body)
        return _vec_literal(vec), await _get_current_embedding_model_id()
    except Exception as exc:
        log.warning("agent_note embedding failed: %s", exc)
        return None, None


@mcp.tool(annotations=WRITE_OP)
async def create_agent_note(
    agent_id: str,
    title: str,
    content: str,
    note_type: str = "general",
    case_number: str = None,
    tags: str = None,
    priority: int = 5
) -> str:
    """
    Create an internal agent note for investigation tracking (stored in ops.agent_notes).
    For creating documents that William should see, use create_note instead.

    The note's title + content are embedded at write time so they can be
    retrieved later via ``search_agent_notes(query)`` even if you don't
    remember exact words.

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
        vec_str, model_id = await _embed_agent_note(title, content)

        row = await p.fetchrow(f"""
            INSERT INTO ops.agent_notes
            (agent_id, title, content, note_type, case_number, tags, priority,
             embedding, embedding_model_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7,
                    $8::halfvec({EMBEDDING_DIMS}), $9)
            RETURNING id, created_at
        """, agent_id, title, content, note_type, case_number, tag_array, priority,
             vec_str, model_id)

        return json.dumps({
            "success": True,
            "note_id": row["id"],
            "created_at": row["created_at"].isoformat(),
            "embedded": vec_str is not None,
            "message": f"Note '{title}' created successfully"
        })
    except Exception as e:
        log.error("create_agent_note error: %s", e)
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

        # If content was appended, re-embed against the new (concatenated) body.
        re_embedded = False
        if updated and content:
            row = await p.fetchrow(
                "SELECT title, content FROM ops.agent_notes WHERE id = $1", note_id
            )
            if row:
                vec_str, model_id = await _embed_agent_note(row["title"], row["content"])
                if vec_str is not None:
                    await p.execute(f"""
                        UPDATE ops.agent_notes
                           SET embedding = $1::halfvec({EMBEDDING_DIMS}),
                               embedding_model_id = $2
                         WHERE id = $3
                    """, vec_str, model_id, note_id)
                    re_embedded = True

        return json.dumps({
            "success": updated,
            "note_id": note_id,
            "re_embedded": re_embedded,
            "message": "Note updated" if updated else "Note not found"
        })
    except Exception as e:
        log.error("update_note error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def search_agent_notes(
    query: str,
    agent_id: Optional[str] = None,
    note_type: Optional[str] = None,
    case_number: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search agent notes by meaning (semantic + recency).

    Use this to recall prior findings, tasks, or analyses without remembering
    exact wording — e.g. "what did I conclude about the Starbucks deposition?"
    Ranks by ``0.8 * cosine_similarity + 0.2 * recency_boost``. Falls back to
    full-text search if the embedding service is unreachable.

    Args:
        query: Natural-language search query.
        agent_id: Optional filter ('athena', 'case-strategy', etc.).
        note_type: Optional filter ('finding', 'task', 'evidence', ...).
        case_number: Optional filter to a specific case.
        limit: Max notes to return (default 10).
    """
    try:
        query = (query or "").strip()
        if not query:
            return json.dumps({"error": "query is required"})
        limit = min(max(int(limit), 1), 50)
        p = await get_pool()

        try:
            vec = await _embed_query(query)
            vec_str = _vec_literal(vec)
        except Exception as exc:
            log.warning("search_agent_notes embedding fallback: %s", exc)
            vec_str = None

        conditions = ["embedding IS NOT NULL"] if vec_str else ["true"]
        params: list = []
        idx = 1
        if vec_str:
            params.append(vec_str)
            idx = 2

        if agent_id:
            conditions.append(f"agent_id = ${idx}")
            params.append(agent_id)
            idx += 1
        if note_type:
            conditions.append(f"note_type = ${idx}")
            params.append(note_type)
            idx += 1
        if case_number:
            conditions.append(f"case_number = ${idx}")
            params.append(case_number)
            idx += 1

        where = " AND ".join(conditions)

        if vec_str:
            sql = f"""
                SELECT id, agent_id, note_type, title, content, case_number,
                       status, priority, created_at,
                       (embedding <=> $1::halfvec({EMBEDDING_DIMS})) AS distance
                  FROM ops.agent_notes
                 WHERE {where}
                 ORDER BY embedding <=> $1::halfvec({EMBEDDING_DIMS})
                 LIMIT {max(limit * 3, 20)}
            """
            rows = await p.fetch(sql, *params)

            scored = []
            now_ts = datetime.now(timezone.utc)
            for r in rows:
                similarity = max(0.0, 1.0 - float(r["distance"]))
                age_days = max(0.0, (now_ts - r["created_at"]).total_seconds() / 86400.0)
                recency = 1.0 / (1.0 + age_days / 30.0)
                score = 0.8 * similarity + 0.2 * recency
                scored.append({
                    "note_id": r["id"],
                    "agent_id": r["agent_id"],
                    "note_type": r["note_type"],
                    "title": r["title"],
                    "content_preview": (r["content"] or "")[:400],
                    "case_number": r["case_number"],
                    "status": r["status"],
                    "priority": r["priority"],
                    "created_at": r["created_at"].isoformat(),
                    "similarity": round(similarity, 4),
                    "score": round(score, 4),
                })
            scored.sort(key=lambda n: n["score"], reverse=True)
            results = scored[:limit]
            return json.dumps({"results": results, "count": len(results),
                               "query": query, "mode": "semantic"})

        # Fallback path: tsvector full-text search.
        params = [query]
        conditions = ["search_vector @@ plainto_tsquery('english', $1)"]
        idx = 2
        if agent_id:
            conditions.append(f"agent_id = ${idx}")
            params.append(agent_id)
            idx += 1
        if note_type:
            conditions.append(f"note_type = ${idx}")
            params.append(note_type)
            idx += 1
        if case_number:
            conditions.append(f"case_number = ${idx}")
            params.append(case_number)
            idx += 1

        sql = f"""
            SELECT id, agent_id, note_type, title, content, case_number,
                   status, priority, created_at,
                   ts_rank(search_vector, plainto_tsquery('english', $1)) AS rank
              FROM ops.agent_notes
             WHERE {' AND '.join(conditions)}
             ORDER BY rank DESC, created_at DESC
             LIMIT {limit}
        """
        rows = await p.fetch(sql, *params)
        results = [{
            "note_id": r["id"],
            "agent_id": r["agent_id"],
            "note_type": r["note_type"],
            "title": r["title"],
            "content_preview": (r["content"] or "")[:400],
            "case_number": r["case_number"],
            "status": r["status"],
            "priority": r["priority"],
            "created_at": r["created_at"].isoformat(),
            "rank": float(r["rank"]),
        } for r in rows]
        return json.dumps({"results": results, "count": len(results),
                           "query": query, "mode": "fulltext"})
    except Exception as e:
        log.error("search_agent_notes error: %s", e)
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


# ╔══════════════════════════════════════════════════════════════╗
# ║                      SMS / TELNYX TOOLS                      ║
# ╚══════════════════════════════════════════════════════════════╝

_E164_RE = re.compile(r"^\+[1-9][0-9]{1,14}$")


async def _sms_find_or_create_thread(
    p, peer: str, ours: str, subject: Optional[str]
) -> int:
    """Find the open thread matching (peer, ours, subject) or create one."""
    row = await p.fetchrow(
        """
        SELECT id FROM core.sms_threads
         WHERE peer_number = $1 AND our_number = $2
           AND COALESCE(subject, '') = COALESCE($3, '')
           AND closed_at IS NULL
         LIMIT 1
        """,
        peer,
        ours,
        subject,
    )
    if row:
        return row["id"]
    row = await p.fetchrow(
        """
        INSERT INTO core.sms_threads (peer_number, our_number, subject)
        VALUES ($1, $2, $3)
        RETURNING id
        """,
        peer,
        ours,
        subject,
    )
    return row["id"]


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def send_sms_message(
    to: str,
    text: str,
    subject: Optional[str] = None,
) -> str:
    """Send an SMS via Telnyx and log it to the threaded conversation store.

    Args:
        to: Recipient phone number in E.164 format (e.g. +14255551212)
        text: Message body. Telnyx handles concatenation for long messages.
        subject: Optional thread subject to scope this conversation.
                 If omitted, attaches to the existing open unsubjected
                 thread with this recipient (or opens one).

    Returns JSON with thread_id, telnyx message_id, parts, cost_usd, and ok.
    """
    if not _E164_RE.match(to):
        return json.dumps({"error": f"'to' must be E.164 (e.g. +14255551212); got {to!r}"})
    if not text:
        return json.dumps({"error": "text must not be empty"})

    from telnyx_sms import send_sms
    ours = os.environ.get("TELNYX_SMS_FROM", "")
    if not ours:
        return json.dumps({"error": "TELNYX_SMS_FROM not configured"})

    p = await get_pool()
    try:
        thread_id = await _sms_find_or_create_thread(p, to, ours, subject)
        result = send_sms(to, text)

        cost = None
        if result.cost_usd is not None:
            try:
                cost = float(result.cost_usd)
            except (TypeError, ValueError):
                cost = None

        await p.execute(
            """
            INSERT INTO core.sms_messages
                (thread_id, direction, body, telnyx_message_id, parts,
                 cost_usd, error_title, error_detail)
            VALUES ($1, 'outbound', $2, $3, $4, $5, $6, $7)
            """,
            thread_id,
            text,
            result.message_id,
            result.parts,
            cost,
            None if result.ok else result.error_title,
            None if result.ok else result.error_detail,
        )
        await p.execute(
            "UPDATE core.sms_threads SET last_message_at = now() WHERE id = $1",
            thread_id,
        )

        return json.dumps({
            "ok": result.ok,
            "thread_id": thread_id,
            "to": to,
            "from": ours,
            "message_id": result.message_id,
            "parts": result.parts,
            "cost_usd": result.cost_usd,
            "error_title": result.error_title,
            "error_detail": result.error_detail,
        })
    except Exception as e:
        log.error("send_sms_message error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def check_sms_replies(number: str, since_minutes: int = 60) -> str:
    """Return inbound SMS replies received from a number within the lookback window.

    Args:
        number: Peer number in E.164 format (e.g. +14255551212)
        since_minutes: Lookback window in minutes (default 60)
    """
    if not _E164_RE.match(number):
        return json.dumps({"error": f"number must be E.164; got {number!r}"})
    if since_minutes <= 0:
        return json.dumps({"error": "since_minutes must be positive"})

    p = await get_pool()
    try:
        rows = await p.fetch(
            """
            SELECT m.id, m.thread_id, m.body, m.telnyx_message_id, m.created_at,
                   t.subject
              FROM core.sms_messages m
              JOIN core.sms_threads t ON t.id = m.thread_id
             WHERE m.direction = 'inbound'
               AND t.peer_number = $1
               AND m.created_at >= now() - ($2::int || ' minutes')::interval
             ORDER BY m.created_at DESC
            """,
            number,
            since_minutes,
        )
        return json.dumps({
            "number": number,
            "since_minutes": since_minutes,
            "count": len(rows),
            "replies": [{
                "id": r["id"],
                "thread_id": r["thread_id"],
                "subject": r["subject"],
                "body": r["body"],
                "message_id": r["telnyx_message_id"],
                "received_at": _ser(r["created_at"]),
            } for r in rows],
        })
    except Exception as e:
        log.error("check_sms_replies error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_sms_conversation(number: str, limit: int = 50) -> str:
    """Return the threaded SMS conversation (both directions) with a number, newest first.

    Args:
        number: Peer number in E.164 format
        limit: Max messages to return (default 50)
    """
    if not _E164_RE.match(number):
        return json.dumps({"error": f"number must be E.164; got {number!r}"})
    if limit <= 0 or limit > 1000:
        return json.dumps({"error": "limit must be between 1 and 1000"})

    p = await get_pool()
    try:
        rows = await p.fetch(
            """
            SELECT m.id, m.thread_id, m.direction, m.body, m.parts, m.cost_usd,
                   m.error_title, m.telnyx_message_id, m.created_at,
                   t.subject, t.opened_at, t.closed_at, t.our_number
              FROM core.sms_messages m
              JOIN core.sms_threads t ON t.id = m.thread_id
             WHERE t.peer_number = $1
             ORDER BY m.created_at DESC
             LIMIT $2
            """,
            number,
            limit,
        )
        return json.dumps({
            "peer_number": number,
            "count": len(rows),
            "messages": [{
                "id": r["id"],
                "thread_id": r["thread_id"],
                "subject": r["subject"],
                "direction": r["direction"],
                "body": r["body"],
                "parts": r["parts"],
                "cost_usd": str(r["cost_usd"]) if r["cost_usd"] is not None else None,
                "error_title": r["error_title"],
                "message_id": r["telnyx_message_id"],
                "created_at": _ser(r["created_at"]),
                "thread_opened_at": _ser(r["opened_at"]),
                "thread_closed_at": _ser(r["closed_at"]),
                "our_number": r["our_number"],
            } for r in rows],
        })
    except Exception as e:
        log.error("list_sms_conversation error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def close_sms_thread(thread_id: int) -> str:
    """Close an SMS thread. Future inbound replies from the peer start a new thread.

    Args:
        thread_id: ID of the thread to close
    """
    p = await get_pool()
    try:
        row = await p.fetchrow(
            """
            UPDATE core.sms_threads
               SET closed_at = now()
             WHERE id = $1 AND closed_at IS NULL
            RETURNING id, peer_number, our_number, subject
            """,
            thread_id,
        )
        if not row:
            return json.dumps({"error": f"thread {thread_id} not found or already closed"})
        return json.dumps({
            "ok": True,
            "thread_id": row["id"],
            "peer_number": row["peer_number"],
            "our_number": row["our_number"],
            "subject": row["subject"],
        })
    except Exception as e:
        log.error("close_sms_thread error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                 LEGAL DEADLINE TOOLS                         ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool()
async def add_case_deadline(
    case_number: str,
    title: str,
    deadline_date: str,
    deadline_type: str = "filing",
    priority: str = "normal",
    description: str = "",
) -> str:
    """Add a deadline for a legal case.

    Args:
        case_number: Case number (e.g. "24-2-01031-31")
        title: Short description of the deadline
        deadline_date: Date in YYYY-MM-DD format
        deadline_type: Type — "filing", "hearing", "discovery", "response", "trial", "other"
        priority: Priority — "critical", "high", "normal", "low"
        description: Optional detailed description
    """
    try:
        from datetime import date as date_type
        dl_date = date_type.fromisoformat(deadline_date)
        p = await get_pool()
        case = await p.fetchrow(
            "SELECT id FROM legal.cases WHERE case_number ILIKE $1 LIMIT 1",
            f"%{case_number}%",
        )
        if not case:
            return json.dumps({"error": f"Case not found: {case_number}"})
        row = await p.fetchrow("""
            INSERT INTO legal.case_deadlines
                (case_id, title, deadline_date, deadline_type, priority, description, reminder_days)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        """, case["id"], title, dl_date, deadline_type, priority, description, [7, 3, 1])
        await _log_access("add_case_deadline", f"{case_number}: {title}", 1)
        return json.dumps({
            "status": "created", "deadline_id": row["id"],
            "case_number": case_number, "title": title,
            "deadline_date": deadline_date, "priority": priority,
        })
    except Exception as e:
        log.error("add_case_deadline error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_upcoming_deadlines(days: int = 30, case_number: Optional[str] = None) -> str:
    """List upcoming legal deadlines across all cases.

    Args:
        days: Look ahead window in days (default 30)
        case_number: Optional — filter to a specific case
    """
    try:
        p = await get_pool()
        cutoff = date.today() + timedelta(days=days)
        if case_number:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.deadline_date, d.deadline_type,
                       d.priority, d.status, d.description,
                       c.case_number, c.case_title
                FROM legal.case_deadlines d
                JOIN legal.cases c ON d.case_id = c.id
                WHERE d.deadline_date <= $1 AND d.status = 'pending'
                  AND c.case_number ILIKE $2
                ORDER BY d.deadline_date
            """, cutoff, f"%{case_number}%")
        else:
            rows = await p.fetch("""
                SELECT d.id, d.title, d.deadline_date, d.deadline_type,
                       d.priority, d.status, d.description,
                       c.case_number, c.case_title
                FROM legal.case_deadlines d
                JOIN legal.cases c ON d.case_id = c.id
                WHERE d.deadline_date <= $1 AND d.status = 'pending'
                ORDER BY d.deadline_date
            """, cutoff)

        today = date.today()
        deadlines = [{
            "deadline_id": r["id"], "case_number": r["case_number"],
            "case_name": r["case_name"], "title": r["title"],
            "deadline_date": str(r["deadline_date"]),
            "days_remaining": (r["deadline_date"] - today).days,
            "deadline_type": r["deadline_type"], "priority": r["priority"],
            "status": "OVERDUE" if (r["deadline_date"] - today).days < 0 else r["status"],
            "description": r["description"] or "",
        } for r in rows]

        await _log_access("list_upcoming_deadlines", f"{days} days", len(deadlines))
        return json.dumps({"deadlines": deadlines, "count": len(deadlines), "window_days": days})
    except Exception as e:
        log.error("list_upcoming_deadlines error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def complete_deadline(deadline_id: int) -> str:
    """Mark a legal deadline as completed.

    Args:
        deadline_id: ID of the deadline to mark complete
    """
    try:
        p = await get_pool()
        result = await p.execute("""
            UPDATE legal.case_deadlines
            SET status = 'completed', completed_at = now(), updated_at = now()
            WHERE id = $1 AND status = 'pending'
        """, deadline_id)
        if result == "UPDATE 1":
            return json.dumps({"status": "completed", "deadline_id": deadline_id})
        return json.dumps({"error": f"Deadline {deadline_id} not found or already completed"})
    except Exception as e:
        log.error("complete_deadline error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def list_legal_templates(category: Optional[str] = None) -> str:
    """List available legal document templates.

    Args:
        category: Optional filter — "motion", "letter", "declaration", "discovery", "pleading"
    """
    try:
        p = await get_pool()
        if category:
            rows = await p.fetch("""
                SELECT id, name, category, description, jurisdiction, court_type, variables
                FROM legal.document_templates WHERE is_active = true AND category = $1
                ORDER BY name
            """, category)
        else:
            rows = await p.fetch("""
                SELECT id, name, category, description, jurisdiction, court_type, variables
                FROM legal.document_templates WHERE is_active = true
                ORDER BY category, name
            """)
        templates = [{"id": r["id"], "name": r["name"], "category": r["category"],
                       "description": r["description"], "jurisdiction": r["jurisdiction"],
                       "court_type": r["court_type"], "variables": r["variables"]} for r in rows]
        return json.dumps({"templates": templates, "count": len(templates)})
    except Exception as e:
        log.error("list_legal_templates error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
async def fill_legal_template(template_id: int, variables: str) -> str:
    """Fill a legal document template with provided variables.

    Args:
        template_id: ID of the template (from list_legal_templates)
        variables: JSON string of variable name/value pairs
    """
    try:
        import re as re_mod
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT id, name, template_body, variables as var_defs
            FROM legal.document_templates WHERE id = $1 AND is_active = true
        """, template_id)
        if not row:
            return json.dumps({"error": f"Template {template_id} not found"})
        vars_dict = json.loads(variables) if isinstance(variables, str) else variables
        body = row["template_body"]
        var_defs = row["var_defs"] if isinstance(row["var_defs"], list) else json.loads(row["var_defs"] or "[]")
        for v in var_defs:
            if v.get("default_value") and v["name"] not in vars_dict:
                vars_dict[v["name"]] = v["default_value"]
        placeholders = set(re_mod.findall(r'\{\{(\w+)\}\}', body))
        rendered = body
        filled, missing = {}, []
        for ph in placeholders:
            if ph in vars_dict:
                rendered = rendered.replace(f"{{{{{ph}}}}}", str(vars_dict[ph]))
                filled[ph] = str(vars_dict[ph])
            else:
                missing.append(ph)
        await _log_access("fill_legal_template", row["name"], 1)
        return json.dumps({"template_name": row["name"], "rendered_document": rendered,
                           "variables_filled": filled, "variables_missing": missing})
    except Exception as e:
        log.error("fill_legal_template error: %s", e)
        return json.dumps({"error": str(e)})


# ╔══════════════════════════════════════════════════════════════╗
# ║                 PERSONAL ASSISTANT TOOLS                     ║
# ╚══════════════════════════════════════════════════════════════╝

@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def create_task(
    title: str,
    due_date: Optional[str] = None,
    due_time: Optional[str] = None,
    priority: str = "normal",
    category: str = "personal",
    description: Optional[str] = None,
) -> str:
    """Create a task or reminder for William.

    Use this whenever William asks to be reminded of something, needs to track a to-do,
    or when you identify an action item he should follow up on.

    Args:
        title: What needs to be done (e.g. "Call James Reed about deposition")
        due_date: When it's due in YYYY-MM-DD format (optional)
        due_time: Time in HH:MM format, 24-hour (optional)
        priority: "critical", "high", "normal", or "low"
        category: "personal", "legal", "medical", "financial", "errands"
        description: Additional details or context
    """
    try:
        from datetime import date as date_type, time as time_type
        p = await get_pool()
        dl_date = date_type.fromisoformat(due_date) if due_date else None
        dl_time = time_type.fromisoformat(due_time) if due_time else None

        row = await p.fetchrow("""
            INSERT INTO core.tasks
                (title, description, due_date, due_time, priority, category, source)
            VALUES ($1, $2, $3, $4, $5, $6, 'claude-desktop')
            RETURNING id
        """, title, description, dl_date, dl_time, priority, category)

        await _log_access("create_task", title, 1)
        return json.dumps({
            "status": "created", "task_id": row["id"], "title": title,
            "due_date": due_date, "due_time": due_time, "priority": priority,
            "category": category,
        })
    except Exception as e:
        log.error("create_task error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def list_tasks(
    status: str = "pending",
    category: Optional[str] = None,
    days_ahead: int = 7,
) -> str:
    """List William's tasks and reminders.

    Args:
        status: Filter — "pending", "completed", or "all"
        category: Filter by category (optional)
        days_ahead: How far ahead to look for due dates (default 7)
    """
    try:
        p = await get_pool()
        cutoff = date.today() + timedelta(days=days_ahead)
        conditions = []
        params = []
        idx = 1

        if status != "all":
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if category:
            conditions.append(f"category = ${idx}")
            params.append(category)
            idx += 1

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = await p.fetch(f"""
            SELECT id, title, description, due_date, due_time, priority,
                   category, status, created_at
            FROM core.tasks
            {where}
            ORDER BY COALESCE(due_date, '2099-12-31'), priority
            LIMIT 50
        """, *params)

        today = date.today()
        tasks = []
        for r in rows:
            t = {
                "task_id": r["id"], "title": r["title"],
                "due_date": str(r["due_date"]) if r["due_date"] else None,
                "due_time": str(r["due_time"]) if r["due_time"] else None,
                "priority": r["priority"], "category": r["category"],
                "status": r["status"],
            }
            if r["description"]:
                t["description"] = r["description"]
            if r["due_date"]:
                days_left = (r["due_date"] - today).days
                t["days_remaining"] = days_left
                if days_left < 0:
                    t["overdue"] = True
            tasks.append(t)

        return json.dumps({"tasks": tasks, "count": len(tasks)})
    except Exception as e:
        log.error("list_tasks error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def complete_task(task_id: int) -> str:
    """Mark a task as completed.

    Args:
        task_id: ID of the task to complete
    """
    try:
        p = await get_pool()
        result = await p.execute("""
            UPDATE core.tasks
            SET status = 'completed', completed_at = now(), updated_at = now()
            WHERE id = $1 AND status = 'pending'
        """, task_id)
        if result == "UPDATE 1":
            return json.dumps({"status": "completed", "task_id": task_id})
        return json.dumps({"error": f"Task {task_id} not found or already completed"})
    except Exception as e:
        log.error("complete_task error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def get_calendar(days_ahead: int = 7) -> str:
    """Get William's upcoming calendar events.

    Args:
        days_ahead: How many days ahead to look (default 7)
    """
    try:
        p = await get_pool()
        now = datetime.now(timezone.utc)
        end = now + timedelta(days=days_ahead)

        rows = await p.fetch("""
            SELECT id, subject, start_time, end_time, location,
                   organizer, is_all_day, importance
            FROM core.calendar_events
            WHERE start_time BETWEEN $1 AND $2
              AND NOT is_cancelled
            ORDER BY start_time
        """, now, end)

        events = [{
            "subject": r["subject"],
            "start": r["start_time"].isoformat(),
            "end": r["end_time"].isoformat(),
            "location": r["location"] or "",
            "organizer": r["organizer"] or "",
            "all_day": r["is_all_day"],
            "importance": r["importance"],
        } for r in rows]

        return json.dumps({"events": events, "count": len(events), "days_ahead": days_ahead})
    except Exception as e:
        log.error("get_calendar error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  Athena Outlook Calendar (athena@seattleseahawks.me)
#
#  These tools let Athena manage her OWN Outlook calendar via Microsoft
#  Graph (app-only, Calendars.ReadWrite). Distinct from get_calendar()
#  above which reads William's synced calendar from core.calendar_events.
#
#  Reminders ride on calendar events (isReminderOn + reminderMinutesBeforeStart)
#  because Microsoft To-Do (/todo) does NOT support app-only auth.
# ══════════════════════════════════════════════════════════════


def _parse_iso_datetime(value: str, *, default_tz: timezone = timezone.utc) -> datetime:
    """Parse an ISO-8601 datetime; tolerate 'Z' suffix; assume UTC if naive."""
    if not value:
        raise ValueError("datetime is required")
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return dt


def _graph_event_summary(ev: dict) -> dict:
    return {
        "id": ev.get("id"),
        "subject": ev.get("subject") or "",
        "start": (ev.get("start") or {}).get("dateTime"),
        "start_tz": (ev.get("start") or {}).get("timeZone"),
        "end": (ev.get("end") or {}).get("dateTime"),
        "end_tz": (ev.get("end") or {}).get("timeZone"),
        "location": ((ev.get("location") or {}).get("displayName") or ""),
        "is_all_day": bool(ev.get("isAllDay")),
        "is_cancelled": bool(ev.get("isCancelled")),
        "is_reminder_on": bool(ev.get("isReminderOn")),
        "reminder_minutes_before_start": ev.get("reminderMinutesBeforeStart"),
        "importance": ev.get("importance"),
        "organizer": ((ev.get("organizer") or {}).get("emailAddress") or {}).get("address"),
        "attendees": [
            ((a.get("emailAddress") or {}).get("address"))
            for a in (ev.get("attendees") or [])
            if (a.get("emailAddress") or {}).get("address")
        ],
        "body_preview": (ev.get("bodyPreview") or "")[:500],
        "web_link": ev.get("webLink"),
    }


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def calendar_list_events(
    days_ahead: int = 7,
    days_behind: int = 0,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
    top: int = 50,
) -> str:
    """List calendar events on Athena's Outlook calendar (athena@seattleseahawks.me by default).

    Use this whenever Athena needs to know what is on her own calendar — e.g.
    before scheduling a follow-up, before promising a deadline, when answering
    "what do you have coming up?", or to spot conflicts before creating a new
    event with calendar_create_event().

    Args:
        days_ahead: How many days forward to look (default 7).
        days_behind: How many days back to include (default 0 = future only).
        mailbox: Mailbox whose calendar to query (default Athena's mailbox).
        top: Max number of events to return (Graph caps at 999).
    """
    try:
        mailbox = _normalize_mailbox(mailbox)
        now = datetime.now(timezone.utc)
        start = now - timedelta(days=max(0, int(days_behind)))
        end = now + timedelta(days=max(1, int(days_ahead)))
        url = f"{GRAPH_BASE_URL}/users/{mailbox}/calendar/calendarView"
        params = {
            "startDateTime": start.isoformat(),
            "endDateTime": end.isoformat(),
            "$top": str(min(int(top), 999)),
            "$orderby": "start/dateTime",
            "$select": (
                "id,subject,start,end,location,isAllDay,isCancelled,isReminderOn,"
                "reminderMinutesBeforeStart,importance,organizer,attendees,"
                "bodyPreview,webLink"
            ),
        }
        data = await _graph_request("GET", url, params=params,
                                     headers={"Prefer": 'outlook.timezone="UTC"'})
        events = [_graph_event_summary(e) for e in (data.get("value") or [])]
        return json.dumps({
            "mailbox": mailbox,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "count": len(events),
            "events": events,
        })
    except Exception as e:
        log.error("calendar_list_events error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def calendar_create_event(
    subject: str,
    start: str,
    end: str,
    body: Optional[str] = None,
    location: Optional[str] = None,
    attendees: Optional[str] = None,
    is_all_day: bool = False,
    is_reminder_on: bool = True,
    reminder_minutes_before_start: int = 15,
    importance: str = "normal",
    time_zone: str = "UTC",
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Create an event on Athena's Outlook calendar with optional reminder.

    Use this for ANY time-bound thing Athena needs to track for herself —
    appointments, scheduled call-backs, deposition windows, court hearings she
    must monitor, follow-up windows for opposing-counsel responses. The
    reminder fields surface a pop-up in Outlook at the chosen lead time, which
    is Athena's primary "ping me later" channel since Microsoft To-Do does not
    support app-only Graph auth.

    For a pure reminder with no real meeting, set start = the moment the
    reminder should fire, end = start + a few minutes, is_reminder_on=true,
    and reminder_minutes_before_start=0.

    Args:
        subject: Event title.
        start: ISO-8601 start datetime ("2026-04-29T15:00:00Z" or "2026-04-29T15:00:00").
        end: ISO-8601 end datetime.
        body: Optional HTML or plain-text body.
        location: Optional location string (free text).
        attendees: Optional comma-separated email addresses.
        is_all_day: Whether this is an all-day event.
        is_reminder_on: Show a reminder pop-up (default True).
        reminder_minutes_before_start: Minutes before start to ping (default 15).
        importance: "low", "normal", or "high".
        time_zone: IANA/Windows TZ used for start/end (default "UTC"). Graph
            stores UTC; this controls how naive datetimes are interpreted.
        mailbox: Mailbox to create the event on (default Athena's mailbox).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not subject or not subject.strip():
            raise ValueError("subject is required")

        attendee_list = _split_csv_list(attendees)
        importance_norm = _normalize_importance(importance, default="normal").lower()

        payload: dict[str, Any] = {
            "subject": subject.strip(),
            "start": {"dateTime": start, "timeZone": time_zone},
            "end": {"dateTime": end, "timeZone": time_zone},
            "isAllDay": bool(is_all_day),
            "isReminderOn": bool(is_reminder_on),
            "reminderMinutesBeforeStart": max(0, int(reminder_minutes_before_start)),
            "importance": importance_norm,
        }
        if body:
            is_html = bool(re.search(r"<[a-zA-Z][^>]*>", body))
            payload["body"] = {
                "contentType": "html" if is_html else "text",
                "content": body,
            }
        if location:
            payload["location"] = {"displayName": location}
        if attendee_list:
            payload["attendees"] = [
                {"emailAddress": {"address": a}, "type": "required"}
                for a in attendee_list
            ]

        ev = await _graph_request(
            "POST",
            f"{GRAPH_BASE_URL}/users/{mailbox}/calendar/events",
            json_body=payload,
            headers={"Content-Type": "application/json"},
        )
        return json.dumps({"status": "created", "mailbox": mailbox,
                          "event": _graph_event_summary(ev)})
    except Exception as e:
        log.error("calendar_create_event error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def calendar_update_event(
    event_id: str,
    subject: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    body: Optional[str] = None,
    location: Optional[str] = None,
    is_reminder_on: Optional[bool] = None,
    reminder_minutes_before_start: Optional[int] = None,
    importance: Optional[str] = None,
    time_zone: str = "UTC",
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Patch an existing event on Athena's calendar. Only fields you pass are changed.

    Use this to reschedule, change reminder lead-time, mark importance, or edit
    body/location. To cancel/remove the event entirely, use calendar_delete_event.

    Args:
        event_id: Graph event id (returned from calendar_list_events / calendar_create_event).
        subject: New title (optional).
        start: New ISO-8601 start (optional).
        end: New ISO-8601 end (optional).
        body: New body text/HTML (optional).
        location: New location (optional).
        is_reminder_on: Toggle reminder on/off (optional).
        reminder_minutes_before_start: New reminder lead-time minutes (optional).
        importance: "low" | "normal" | "high" (optional).
        time_zone: TZ for start/end if provided (default UTC).
        mailbox: Mailbox owning the event (default Athena's).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not event_id or not event_id.strip():
            raise ValueError("event_id is required")

        payload: dict[str, Any] = {}
        if subject is not None:
            payload["subject"] = subject.strip()
        if start is not None:
            payload["start"] = {"dateTime": start, "timeZone": time_zone}
        if end is not None:
            payload["end"] = {"dateTime": end, "timeZone": time_zone}
        if body is not None:
            is_html = bool(re.search(r"<[a-zA-Z][^>]*>", body))
            payload["body"] = {
                "contentType": "html" if is_html else "text",
                "content": body,
            }
        if location is not None:
            payload["location"] = {"displayName": location}
        if is_reminder_on is not None:
            payload["isReminderOn"] = bool(is_reminder_on)
        if reminder_minutes_before_start is not None:
            payload["reminderMinutesBeforeStart"] = max(0, int(reminder_minutes_before_start))
        if importance is not None:
            payload["importance"] = _normalize_importance(importance, default="normal").lower()

        if not payload:
            return json.dumps({"error": "no fields supplied to update"})

        ev = await _graph_request(
            "PATCH",
            f"{GRAPH_BASE_URL}/users/{mailbox}/calendar/events/{event_id}",
            json_body=payload,
            headers={"Content-Type": "application/json"},
        )
        return json.dumps({"status": "updated", "mailbox": mailbox,
                          "event": _graph_event_summary(ev)})
    except Exception as e:
        log.error("calendar_update_event error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=DESTRUCTIVE)
@logged_tool
async def calendar_delete_event(
    event_id: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Delete an event from Athena's calendar.

    Args:
        event_id: Graph event id.
        mailbox: Mailbox owning the event (default Athena's).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not event_id or not event_id.strip():
            raise ValueError("event_id is required")
        await _graph_request(
            "DELETE",
            f"{GRAPH_BASE_URL}/users/{mailbox}/calendar/events/{event_id}",
        )
        return json.dumps({"status": "deleted", "mailbox": mailbox, "event_id": event_id})
    except Exception as e:
        log.error("calendar_delete_event error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  Athena Notes (Outlook mail folder, athena@seattleseahawks.me)
#
#  Microsoft retired app-only OneNote auth on 2025-03-31 — same pattern
#  as Tasks (To-Do is also delegated-only). To give Athena a writeable
#  notes surface that William can also see in his Outlook, each "note"
#  is a draft message stored in a dedicated mail folder named
#  "Athena Notes". Uses Mail.ReadWrite (already consented).
#
#  Subject = note title; body = note content. isDraft=true so notes
#  never accidentally send. The folder is auto-created on first write.
# ══════════════════════════════════════════════════════════════

ATHENA_NOTES_FOLDER_NAME = "Athena Notes"
_athena_notes_folder_id_cache: dict[str, str] = {}


def _note_message_summary(msg: dict) -> dict:
    return {
        "id": msg.get("id"),
        "title": msg.get("subject") or "",
        "preview": (msg.get("bodyPreview") or "")[:500],
        "created_at": msg.get("createdDateTime"),
        "modified_at": msg.get("lastModifiedDateTime"),
        "is_draft": bool(msg.get("isDraft")),
        "categories": msg.get("categories") or [],
        "web_link": msg.get("webLink"),
    }


async def _resolve_athena_notes_folder_id(mailbox: str) -> str:
    """Return the id of the 'Athena Notes' mail folder, creating it if missing."""
    cached = _athena_notes_folder_id_cache.get(mailbox)
    if cached:
        return cached

    safe_name = ATHENA_NOTES_FOLDER_NAME.replace("'", "''")
    data = await _graph_request(
        "GET",
        f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders",
        params={
            "$filter": f"displayName eq '{safe_name}'",
            "$select": "id,displayName",
            "$top": "1",
        },
    )
    folders = data.get("value") or []
    if folders:
        folder_id = folders[0]["id"]
    else:
        created = await _graph_request(
            "POST",
            f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders",
            json_body={"displayName": ATHENA_NOTES_FOLDER_NAME},
            headers={"Content-Type": "application/json"},
        )
        folder_id = created["id"]

    _athena_notes_folder_id_cache[mailbox] = folder_id
    return folder_id


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def notes_list(
    search: Optional[str] = None,
    top: int = 25,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """List notes Athena has saved in her "Athena Notes" Outlook folder.

    Each note is a draft message; subject is the note title and the body
    holds the content. William can also see this folder by browsing
    Athena's mailbox in Outlook.

    Args:
        search: Optional substring filter on the note title (subject).
        top: Max number of notes to return (default 25, max 100).
        mailbox: Mailbox (default Athena's).
    """
    try:
        mailbox = _normalize_mailbox(mailbox)
        folder_id = await _resolve_athena_notes_folder_id(mailbox)
        params: dict[str, str] = {
            "$top": str(min(int(top), 100)),
            "$orderby": "lastModifiedDateTime desc",
            "$select": (
                "id,subject,bodyPreview,createdDateTime,lastModifiedDateTime,"
                "isDraft,categories,webLink"
            ),
        }
        if search:
            safe = search.replace("'", "''")
            params["$filter"] = f"contains(tolower(subject),'{safe.lower()}')"
        data = await _graph_request(
            "GET",
            f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders/{folder_id}/messages",
            params=params,
        )
        notes = [_note_message_summary(m) for m in (data.get("value") or [])]
        return json.dumps({
            "mailbox": mailbox,
            "folder": ATHENA_NOTES_FOLDER_NAME,
            "count": len(notes),
            "notes": notes,
        })
    except Exception as e:
        log.error("notes_list error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def notes_get(
    note_id: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Fetch a single note by id (returns full body).

    Args:
        note_id: Note message id.
        mailbox: Mailbox (default Athena's).
    """
    try:
        mailbox = _normalize_mailbox(mailbox)
        if not note_id or not note_id.strip():
            raise ValueError("note_id is required")
        msg = await _graph_request(
            "GET",
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{note_id}",
            params={"$select": (
                "id,subject,body,bodyPreview,createdDateTime,lastModifiedDateTime,"
                "isDraft,categories,webLink"
            )},
        )
        body_obj = msg.get("body") or {}
        result = _note_message_summary(msg)
        result["content_type"] = body_obj.get("contentType", "html")
        result["content"] = body_obj.get("content", "")
        return json.dumps({"mailbox": mailbox, "note": result})
    except Exception as e:
        log.error("notes_get error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def notes_create(
    title: str,
    content: str,
    content_is_html: bool = False,
    categories: Optional[str] = None,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Create a new note in Athena's "Athena Notes" Outlook folder.

    Use this to durably write down structured notes that William can also
    see in Outlook — research summaries, case-strategy memos, follow-up
    checklists, "things I noticed" longer than fit in core.memories. For
    short ephemeral facts, prefer save_personal_memory; for case-attached
    investigation notes, prefer create_agent_note.

    Each note is stored as a draft message (so it can never accidentally
    send) inside a dedicated folder. The folder is auto-created on first
    use.

    Args:
        title: Note title (becomes the message subject).
        content: Note body. Plain text is auto-wrapped in <p>; pass HTML
            directly and set content_is_html=True for richer formatting.
        content_is_html: True if `content` is already HTML.
        categories: Optional comma-separated Outlook category names to tag
            this note with (visible as colored bands in Outlook).
        mailbox: Mailbox (default Athena's).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not title or not title.strip():
            raise ValueError("title is required")
        if content is None:
            content = ""

        folder_id = await _resolve_athena_notes_folder_id(mailbox)

        if content_is_html:
            body_content = content
            body_type = "HTML"
        else:
            paragraphs = [
                f"<p>{html.escape(line)}</p>"
                for line in content.splitlines()
                if line.strip()
            ] or ["<p></p>"]
            body_content = "\n".join(paragraphs)
            body_type = "HTML"

        cat_list = _split_csv_list(categories)
        payload: dict[str, Any] = {
            "subject": title.strip(),
            "body": {"contentType": body_type, "content": body_content},
        }
        if cat_list:
            payload["categories"] = cat_list

        msg = await _graph_request(
            "POST",
            f"{GRAPH_BASE_URL}/users/{mailbox}/mailFolders/{folder_id}/messages",
            json_body=payload,
            headers={"Content-Type": "application/json"},
        )
        return json.dumps({"status": "created", "mailbox": mailbox,
                          "folder": ATHENA_NOTES_FOLDER_NAME,
                          "note": _note_message_summary(msg)})
    except Exception as e:
        log.error("notes_create error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def notes_update(
    note_id: str,
    title: Optional[str] = None,
    content: Optional[str] = None,
    content_is_html: bool = False,
    append: bool = False,
    categories: Optional[str] = None,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Update an existing note. Only the fields you pass are changed.

    By default, passing `content` REPLACES the note body. To add to an
    existing note instead of replacing, set append=True — the new content
    is appended after the existing HTML body.

    Args:
        note_id: Note message id.
        title: New note title (subject), if changing.
        content: New body content, or content to append if append=True.
        content_is_html: True if `content` is already HTML.
        append: If True, append to existing body instead of replacing.
        categories: Optional comma-separated category names (replaces existing list).
        mailbox: Mailbox (default Athena's).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not note_id or not note_id.strip():
            raise ValueError("note_id is required")

        payload: dict[str, Any] = {}
        if title is not None:
            payload["subject"] = title.strip()
        if categories is not None:
            payload["categories"] = _split_csv_list(categories)
        if content is not None:
            if content_is_html:
                new_html = content
            else:
                paragraphs = [
                    f"<p>{html.escape(line)}</p>"
                    for line in content.splitlines()
                    if line.strip()
                ] or ["<p></p>"]
                new_html = "\n".join(paragraphs)

            if append:
                existing = await _graph_request(
                    "GET",
                    f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{note_id}",
                    params={"$select": "body"},
                )
                existing_body = ((existing.get("body") or {}).get("content")) or ""
                separator = (
                    f"<p><em>— appended {datetime.now(timezone.utc).isoformat()} —</em></p>"
                )
                merged = f"{existing_body}\n{separator}\n{new_html}"
                payload["body"] = {"contentType": "HTML", "content": merged}
            else:
                payload["body"] = {"contentType": "HTML", "content": new_html}

        if not payload:
            return json.dumps({"error": "no fields supplied to update"})

        msg = await _graph_request(
            "PATCH",
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{note_id}",
            json_body=payload,
            headers={"Content-Type": "application/json"},
        )
        return json.dumps({"status": "updated", "mailbox": mailbox,
                          "note": _note_message_summary(msg)})
    except Exception as e:
        log.error("notes_update error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=DESTRUCTIVE)
@logged_tool
async def notes_delete(
    note_id: str,
    mailbox: str = DEFAULT_ATHENA_MAILBOX,
) -> str:
    """Delete a note from Athena's "Athena Notes" folder.

    Args:
        note_id: Note message id.
        mailbox: Mailbox (default Athena's).
    """
    try:
        _require_scope("write")
        mailbox = _normalize_mailbox(mailbox)
        if not note_id or not note_id.strip():
            raise ValueError("note_id is required")
        await _graph_request(
            "DELETE",
            f"{GRAPH_BASE_URL}/users/{mailbox}/messages/{note_id}",
        )
        return json.dumps({"status": "deleted", "mailbox": mailbox, "note_id": note_id})
    except Exception as e:
        log.error("notes_delete error: %s", e)
        return json.dumps({"error": str(e)})


async def _save_personal_memory_impl(
    content: str,
    memory_type: str,
    importance: float,
    supersedes_memory_id: Optional[int],
) -> str:
    try:
        content = (content or "").strip()
        if not content:
            return json.dumps({"error": "content is required"})

        memory_type = (memory_type or "fact").strip().lower() or "fact"
        importance = max(0.0, min(float(importance), 1.0))
        p = await get_pool()
        source = _current_client_id() or "claude-desktop"

        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

        # Dedup: reinforce an active, non-superseded row with identical content.
        existing = await p.fetchrow("""
            SELECT id, importance
            FROM core.memories
            WHERE content_hash = $1
              AND is_active = true
              AND superseded_by IS NULL
            ORDER BY id ASC
            LIMIT 1
        """, content_hash)

        if existing is not None and supersedes_memory_id is None:
            new_importance = max(float(existing["importance"]), importance)
            await p.execute("""
                UPDATE core.memories
                   SET importance = $1,
                       access_count = access_count + 1,
                       last_accessed = now(),
                       updated_at = now()
                 WHERE id = $2
            """, new_importance, existing["id"])
            await _log_access("save_personal_memory", content[:80], 1)
            return json.dumps({
                "status": "reinforced",
                "memory_id": existing["id"],
                "type": memory_type,
                "importance": new_importance,
            })

        embedding_model_id = await _get_current_embedding_model_id()
        # Embed the memory for semantic retrieval
        vec = await _embed_query(content)
        vec_str = _vec_literal(vec)

        async with p.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(f"""
                    INSERT INTO core.memories
                        (memory_type, content, content_hash, source, importance,
                         embedding, embedding_model_id, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6::halfvec({EMBEDDING_DIMS}), $7, $8::jsonb)
                    RETURNING id
                """,
                    memory_type,
                    content,
                    content_hash,
                    source,
                    importance,
                    vec_str,
                    embedding_model_id,
                    json.dumps({
                        "saved_at": datetime.now(timezone.utc).isoformat(),
                        "semantic": True,
                        "embedding_model": EMBEDDING_MODEL,
                        "embedding_provider": EMBEDDING_PROVIDER,
                        "embedding_dimensions": EMBEDDING_DIMS,
                        "supersedes": supersedes_memory_id,
                    }),
                )

                superseded = False
                if supersedes_memory_id is not None:
                    res = await conn.execute("""
                        UPDATE core.memories
                           SET superseded_by = $1,
                               superseded_at = now(),
                               is_active = false,
                               updated_at = now()
                         WHERE id = $2
                           AND id <> $1
                           AND superseded_by IS NULL
                    """, row["id"], int(supersedes_memory_id))
                    superseded = res.endswith(" 1")

        await _log_access("save_personal_memory", content[:80], 1)
        return json.dumps({
            "status": "saved",
            "memory_id": row["id"],
            "type": memory_type,
            "importance": importance,
            "superseded": (int(supersedes_memory_id) if superseded else None),
        })
    except Exception as e:
        log.error("save_personal_memory error: %s", e)
        _schedule_athena_error_alert(
            "Athena MCP tool error — save_personal_memory",
            "\n".join([
                "tool: save_personal_memory",
                f"client: {_current_client_id()}",
                f"memory_type: {memory_type}",
                f"importance: {importance}",
                f"content_preview: {content[:500]}",
                f"error: {e}",
            ]),
        )
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def save_personal_memory(
    content: str,
    memory_type: str = "fact",
    importance: float = 0.5,
    supersedes_memory_id: Optional[int] = None,
) -> str:
    """Save an important fact, decision, or insight to Athena's long-term memory.

    Use this to remember things William tells you, decisions made, preferences learned,
    or important context that should persist across conversations.

    Dedup: if an active memory with the same exact content already exists, this call
    reinforces it (raises importance to the higher of the two, bumps access_count) and
    returns ``status="reinforced"`` without creating a duplicate row or re-embedding.

    Supersession: pass ``supersedes_memory_id`` when this memory replaces an older one
    (corrected fact, changed preference, updated decision). The old row is marked
    ``superseded_by=<new id>`` and ``is_active=false`` so it stops appearing in recalls.

    Args:
        content: What to remember (e.g. "William prefers declarations over affidavits in state court")
        memory_type: "fact", "decision", "preference", "event", "insight", "feedback"
        importance: 0.0 to 1.0 — how important this is (default 0.5)
        supersedes_memory_id: Optional id of an older memory this one replaces.
    """
    return await _save_personal_memory_impl(
        content=content,
        memory_type=memory_type,
        importance=importance,
        supersedes_memory_id=supersedes_memory_id,
    )


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def recall_personal_memories(query: str, limit: int = 5) -> str:
    """Search Athena's long-term memory for relevant facts, decisions, and context.

    Use this at the start of conversations to recall what Athena knows about a topic,
    or when William asks "do you remember...?" or "what did we decide about...?"

    Ranking: combines semantic similarity (cosine) with stored ``importance``
    (``score = 0.7 * similarity + 0.3 * importance``). Superseded memories and
    morning-briefing chatter are filtered out automatically.

    Args:
        query: What to search for in memory (natural language)
        limit: Max memories to return (default 5)
    """
    try:
        query = (query or "").strip()
        if not query:
            return json.dumps({"error": "query is required"})

        limit = min(max(int(limit), 1), 20)
        p = await get_pool()
        vec = await _embed_query(query)
        vec_str = _vec_literal(vec)

        # Pull a candidate pool by raw cosine, then re-rank with importance.
        candidate_pool = max(limit * 4, 20)
        rows = await p.fetch(f"""
            SELECT id, memory_type, content, importance, source, created_at,
                   (embedding <=> $1::halfvec({EMBEDDING_DIMS})) AS distance
            FROM core.memories
            WHERE is_active = true
              AND superseded_by IS NULL
              AND embedding IS NOT NULL
              AND COALESCE(source, '') <> 'morning_briefing'
              AND COALESCE(metadata->>'type', '') <> 'daily_briefing'
            ORDER BY embedding <=> $1::halfvec({EMBEDDING_DIMS})
            LIMIT $2
        """, vec_str, candidate_pool)

        scored = []
        for r in rows:
            similarity = max(0.0, 1.0 - float(r["distance"]))
            importance_val = float(r["importance"]) if r["importance"] is not None else 0.5
            combined = 0.7 * similarity + 0.3 * importance_val
            scored.append({
                "memory_id": r["id"],
                "type": r["memory_type"],
                "content": r["content"],
                "importance": importance_val,
                "relevance": round(similarity, 4),
                "score": round(combined, 4),
                "source": r["source"],
                "saved_on": str(r["created_at"].date()),
            })
        scored.sort(key=lambda m: m["score"], reverse=True)
        memories = scored[:limit]

        await _log_access("recall_personal_memories", query, len(memories))
        return json.dumps({"memories": memories, "count": len(memories), "query": query})
    except Exception as e:
        log.error("recall_personal_memories error: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool(annotations=WRITE_OP)
@logged_tool
async def record_feedback(
    content: str,
    importance: float = 0.7,
    supersedes_memory_id: Optional[int] = None,
) -> str:
    """Record a correction, preference, or behavioral guidance from William.

    Use this whenever William corrects Athena's approach ("don't do X", "stop
    doing Y", "from now on do Z"), confirms a non-obvious choice that worked
    ("yes, exactly that"), or states a workflow preference. Stored as
    ``memory_type='feedback'`` in ``core.memories`` so future
    ``recall_personal_memories`` calls surface it alongside facts.

    If this feedback corrects something Athena already saved, pass
    ``supersedes_memory_id`` so the old guidance is retired and won't be
    recalled again.

    Args:
        content: The rule/correction to remember. Lead with the rule itself,
            then a short "Why:" line if William gave a reason.
        importance: 0.0–1.0; defaults to 0.7 because feedback usually matters.
        supersedes_memory_id: Optional id of an older memory this one corrects.
    """
    return await _save_personal_memory_impl(
        content=content,
        memory_type="feedback",
        importance=importance,
        supersedes_memory_id=supersedes_memory_id,
    )


@mcp.tool(annotations=READ_ONLY)
@logged_tool
async def get_william_context() -> str:
    """Get William's profile and Athena's system context.

    Call this at the start of important conversations to load William's
    profile, accessibility needs, active cases, and current priorities.
    This helps you provide better, more personalized assistance.
    """
    try:
        p = await get_pool()
        profile = await p.fetchrow("SELECT * FROM core.user_profile LIMIT 1")
        identity = await p.fetch("SELECT key, value FROM core.system_identity ORDER BY key")

        # Recent memories (last 10)
        memories = await p.fetch("""
            SELECT content, memory_type, importance
            FROM core.memories
            WHERE is_active = true
            ORDER BY importance DESC, created_at DESC
            LIMIT 10
        """)

        # Pending tasks
        tasks = await p.fetch("""
            SELECT title, due_date, priority, category
            FROM core.tasks WHERE status = 'pending'
            ORDER BY COALESCE(due_date, '2099-12-31') LIMIT 5
        """)

        # Active deadlines
        today = date.today()
        deadlines = await p.fetch("""
            SELECT d.title, d.deadline_date, c.case_number
            FROM legal.case_deadlines d
            JOIN legal.cases c ON d.case_id = c.id
            WHERE d.status = 'pending' AND d.deadline_date >= $1
            ORDER BY d.deadline_date LIMIT 5
        """, today)

        result = {
            "profile": {
                "name": profile["display_name"] if profile else "William Nelson",
                "role": profile["role"] if profile else "",
                "bio": profile["bio"] if profile else "",
                "preferences": profile["preferences"] if profile else {},
                "accessibility": profile["accessibility"] if profile else {},
            },
            "athena_identity": {r["key"]: r["value"] for r in identity},
            "recent_memories": [{"content": m["content"], "type": m["memory_type"]} for m in memories],
            "pending_tasks": [{"title": t["title"], "due": str(t["due_date"]) if t["due_date"] else None, "priority": t["priority"]} for t in tasks],
            "upcoming_deadlines": [{"title": d["title"], "date": str(d["deadline_date"]), "case": d["case_number"]} for d in deadlines],
        }

        return json.dumps(result)
    except Exception as e:
        log.error("get_william_context error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

def main():
    import argparse
    import atexit
    import signal

    parser = argparse.ArgumentParser(description="Athena Cognitive Platform MCP Server v2")
    parser.add_argument("--http", action="store_true", help="Run as HTTP/SSE server (default: stdio)")
    parser.add_argument("--port", type=int, default=9200, help="HTTP port (default 9200)")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    args = parser.parse_args()

    # Clean up DB pool on actual process exit
    def _cleanup_pool():
        if pool is not None:
            asyncio.get_event_loop().run_until_complete(close_pool())
            log.info("Database pool closed on process exit")
    atexit.register(_cleanup_pool)

    if args.http:
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        log.info("Starting Athena MCP Server v2 (Streamable HTTP + OAuth) on %s:%d", args.host, args.port)
        log.info("OAuth issuer: %s", BASE_URL)
        mcp.run(transport="streamable-http")
    else:
        log.info("Starting Athena MCP Server v2 (stdio)")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
