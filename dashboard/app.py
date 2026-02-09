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

import os, json, time, secrets, subprocess, asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any

import asyncpg
import httpx
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, StreamingResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
import uvicorn

# ── Config ────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY environment variable is required")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-5.2")
EMBEDDING_MODEL = "text-embedding-3-large"
AVAILABLE_MODELS = [
  {"id": "auto",       "model": "gpt-5.2",      "label": "⚡ Auto",       "reasoning_effort": None,      "max_tool_rounds": 12, "description": "Automatically decides when to think deeper"},
  {"id": "think-low",  "model": "gpt-5.2",      "label": "💭 Think Low",  "reasoning_effort": "low",     "max_tool_rounds": 8,  "description": "Light reasoning — faster responses"},
  {"id": "think-med",  "model": "gpt-5.2",      "label": "💭 Think Med",  "reasoning_effort": "medium",  "max_tool_rounds": 10, "description": "Balanced reasoning and speed"},
  {"id": "think-high", "model": "gpt-5.2",      "label": "🧠 Think High", "reasoning_effort": "high",    "max_tool_rounds": 14, "description": "Deep reasoning — most thorough"},
  {"id": "think-xhigh","model": "gpt-5.2",      "label": "🧠 Think XHigh","reasoning_effort": "xhigh",   "max_tool_rounds": 18, "description": "Extreme reasoning — longest thinking time"},
]

pool: Optional[asyncpg.Pool] = None


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
    limit = min(int(params.get("limit", "50")), 200)
    offset = int(params.get("offset", "0"))

    where, args, idx = [], [], 1
    if domain:
        where.append(f"d.domain = ${idx}"); args.append(domain); idx += 1
    if doc_type:
        where.append(f"d.document_type = ${idx}"); args.append(doc_type); idx += 1
    if case_number:
        where.append(f"lc.case_number = ${idx}"); args.append(case_number); idx += 1
    wc = " AND ".join(where) if where else "TRUE"

    total = await p.fetchval(f"""
        SELECT COUNT(DISTINCT d.id)
        FROM core.documents d
        LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
        LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
        WHERE {wc}
    """, *args)

    rows = await p.fetch(f"""
        SELECT d.id, d.domain, d.document_type, d.title, d.filename,
               d.source_path, d.total_chunks, d.content_hash,
               d.created_at, d.metadata,
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
    p = await get_pool()
    doc = await p.fetchrow("""
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
    limit = min(int(data.get("limit", 20)), 100)
    if not query:
        return JSONResponse({"error": "query required"}, status_code=400)

    p = await get_pool()
    t0 = time.monotonic()

    if domain:
        rows = await p.fetch("""
            SELECT c.id AS chunk_id, c.document_id, c.content,
                   ts_rank_cd(c.content_tsv,
                       websearch_to_tsquery('english', $1))::FLOAT AS rank,
                   d.domain, d.document_type, d.title, d.filename,
                   ts_headline('english', c.content,
                       websearch_to_tsquery('english', $1),
                       'MaxWords=60, MinWords=20, StartSel=<mark>, StopSel=</mark>'
                   ) AS headline
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', $1)
              AND d.domain = $2
            ORDER BY rank DESC LIMIT $3
        """, query, domain, limit)
    else:
        rows = await p.fetch("""
            SELECT c.id AS chunk_id, c.document_id, c.content,
                   ts_rank_cd(c.content_tsv,
                       websearch_to_tsquery('english', $1))::FLOAT AS rank,
                   d.domain, d.document_type, d.title, d.filename,
                   ts_headline('english', c.content,
                       websearch_to_tsquery('english', $1),
                       'MaxWords=60, MinWords=20, StartSel=<mark>, StopSel=</mark>'
                   ) AS headline
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            WHERE c.content_tsv @@ websearch_to_tsquery('english', $1)
            ORDER BY rank DESC LIMIT $2
        """, query, limit)

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
    """Get embedding vector for a search query."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json={"model": EMBEDDING_MODEL, "input": text, "dimensions": 3072},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]


async def _tool_semantic_search(args: dict) -> str:
    """Semantic vector search against document chunks."""
    query = args["query"]
    domain = args.get("domain")
    limit = min(args.get("limit", 8), 20)
    try:
        embedding = await _embed_query(query)
        vec_literal = "[" + ",".join(str(x) for x in embedding) + "]"
        p = await get_pool()
        if domain:
            rows = await p.fetch(f"""
                SELECT c.id, c.content, c.chunk_index,
                       d.id AS doc_id, d.title, d.filename, d.domain, d.document_type,
                       c.embedding <=> $1::halfvec(3072) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.embedding IS NOT NULL AND d.domain = $2
                ORDER BY c.embedding <=> $1::halfvec(3072)
                LIMIT $3
            """, vec_literal, domain, limit)
        else:
            rows = await p.fetch(f"""
                SELECT c.id, c.content, c.chunk_index,
                       d.id AS doc_id, d.title, d.filename, d.domain, d.document_type,
                       c.embedding <=> $1::halfvec(3072) AS distance
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.embedding IS NOT NULL
                ORDER BY c.embedding <=> $1::halfvec(3072)
                LIMIT $2
            """, vec_literal, limit)
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
    limit = min(args.get("limit", 10), 30)
    p = await get_pool()
    try:
        if domain:
            rows = await p.fetch("""
                SELECT c.id, c.content, d.id AS doc_id, d.title, d.filename,
                       d.domain, d.document_type,
                       ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', $1))::FLOAT AS rank
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.content_tsv @@ websearch_to_tsquery('english', $1) AND d.domain = $2
                ORDER BY rank DESC LIMIT $3
            """, query, domain, limit)
        else:
            rows = await p.fetch("""
                SELECT c.id, c.content, d.id AS doc_id, d.title, d.filename,
                       d.domain, d.document_type,
                       ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', $1))::FLOAT AS rank
                FROM core.document_chunks c
                JOIN core.documents d ON c.document_id = d.id
                WHERE c.content_tsv @@ websearch_to_tsquery('english', $1)
                ORDER BY rank DESC LIMIT $2
            """, query, limit)
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
        docs = await p.fetch("""
            SELECT d.id, d.title, d.filename, d.document_type, d.created_at
            FROM legal.case_documents cd
            JOIN core.documents d ON cd.document_id = d.id
            WHERE cd.case_id = $1
            ORDER BY d.created_at DESC LIMIT 20
        """, case["id"])
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
    """Get full document content."""
    doc_id = args["document_id"]
    p = await get_pool()
    try:
        doc = await p.fetchrow("""
            SELECT d.*, em.sender, em.recipients, em.date_sent, em.direction,
                   em.mailbox
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE d.id = $1::uuid
        """, doc_id)
        if not doc:
            return json.dumps({"error": "Document not found"})
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
        params.append(limit); limit_idx = idx
        where = " AND ".join(conditions)
        rows = await p.fetch(f"""
            SELECT d.id, d.title, d.filename, em.sender, em.recipients,
                   em.date_sent, em.direction, em.mailbox,
                   LEFT(d.full_content, 400) AS excerpt
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            WHERE {where}
            ORDER BY em.date_sent DESC NULLS LAST
            LIMIT ${limit_idx}
        """, *params)
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

    total = await p.fetchval(
      f"""
      SELECT COUNT(*)
      FROM legal.case_documents cd
      JOIN core.documents d ON cd.document_id = d.id
      WHERE {where_sql}
      """,
      *params,
    )

    limit_idx = len(params) + 1
    offset_idx = len(params) + 2
    rows = await p.fetch(
      f"""
      SELECT d.id, d.title, d.filename, d.document_type, d.domain, d.created_at
      FROM legal.case_documents cd
      JOIN core.documents d ON cd.document_id = d.id
      WHERE {where_sql}
      ORDER BY d.created_at ASC
      LIMIT ${limit_idx} OFFSET ${offset_idx}
      """,
      *params,
      limit,
      offset,
    )

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
    rows = await p.fetch(
      """
      SELECT d.id, d.title, d.filename, d.domain, d.document_type, d.created_at,
           d.full_content, em.sender, em.recipients, em.date_sent,
           em.direction, em.mailbox
      FROM core.documents d
      LEFT JOIN legal.email_metadata em ON d.id = em.document_id
      WHERE d.id = ANY($1::uuid[])
      """,
      doc_ids,
    )
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
            'self-healing', 'data-quality', 'email-triage', 'db-tuner',
            'case-strategy', 'retention', 'timeline', 'query-insight'
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


async def _run_chat_turn(
    messages: List[Dict], conversation_id: str, model_id: str = "auto"
) -> Dict[str, Any]:
    """Run one full chat turn with tool calling loop."""
    p = await get_pool()
    all_tool_results = []
    tool_cache: dict[str, str] = {}

    # Resolve model configuration
    model_cfg = next((m for m in AVAILABLE_MODELS if m["id"] == model_id), AVAILABLE_MODELS[0])
    api_model = model_cfg["model"]
    reasoning_effort = model_cfg["reasoning_effort"]

    max_tool_rounds = int(model_cfg.get("max_tool_rounds", 8))
    max_tool_rounds = max(1, min(max_tool_rounds, 20))

    async with httpx.AsyncClient() as client:
        for _iteration in range(max_tool_rounds):
          payload = {
            "model": api_model,
            "messages": messages,
            "tools": CHAT_TOOLS,
            "tool_choice": "auto",
          }
          if reasoning_effort:
            payload["reasoning_effort"] = reasoning_effort
          payload["max_completion_tokens"] = 16384
          resp = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            json=payload,
            timeout=120,
          )
          resp.raise_for_status()
          data = resp.json()
          choice = data["choices"][0]
          msg = choice["message"]
          usage = data.get("usage", {})

          # If no tool calls, we're done
          if not msg.get("tool_calls"):
            assistant_content = msg.get("content", "")
            # Save assistant message
            await p.execute("""
              INSERT INTO ops.chat_messages
                (conversation_id, role, content, tokens_used, model)
              VALUES ($1, 'assistant', $2, $3, $4)
            """, conversation_id, assistant_content,
              usage.get("total_tokens"), api_model)
            await p.execute("""
              UPDATE ops.chat_conversations SET updated_at = now(),
              title = CASE WHEN title = 'New Conversation'
                THEN LEFT($2, 80) ELSE title END
              WHERE id = $1
            """, conversation_id, assistant_content[:80] if assistant_content else "Chat")
            return {
              "content": assistant_content,
              "tool_calls_made": all_tool_results,
              "tokens": usage,
              "model": api_model,
              "model_label": model_cfg["label"],
            }

          # Process tool calls
          messages.append(msg)  # Add the assistant message with tool_calls
          for tc in msg["tool_calls"]:
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
            # Save tool message to DB
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

        # Tool budget exhausted — return the best possible response without further tool calls.
        finalize_messages = messages + [{
          "role": "system",
          "content": (
            "Tool-call budget reached. Provide the best possible answer using the tool results "
            "already gathered. Be explicit about citations, note gaps, and propose the most "
            "targeted next steps to continue if needed."
          )
        }]
        payload = {
          "model": api_model,
          "messages": finalize_messages,
          "tool_choice": "none",
          "max_completion_tokens": 8192,
        }
        if reasoning_effort:
          payload["reasoning_effort"] = reasoning_effort
        resp = await client.post(
          "https://api.openai.com/v1/chat/completions",
          headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
          json=payload,
          timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        choice = data["choices"][0]
        msg = choice["message"]
        usage = data.get("usage", {})
        assistant_content = msg.get("content", "") or ""
        await p.execute("""
          INSERT INTO ops.chat_messages
            (conversation_id, role, content, tokens_used, model)
          VALUES ($1, 'assistant', $2, $3, $4)
        """, conversation_id, assistant_content,
          usage.get("total_tokens"), api_model)
        await p.execute("""
          UPDATE ops.chat_conversations SET updated_at = now(),
          title = CASE WHEN title = 'New Conversation'
            THEN LEFT($2, 80) ELSE title END
          WHERE id = $1
        """, conversation_id, assistant_content[:80] if assistant_content else "Chat")
        return {
          "content": assistant_content,
          "tool_calls_made": all_tool_results,
          "tokens": usage,
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
        SELECT id, role, content, tool_calls, tool_name, tokens_used, model, created_at
        FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user', 'assistant')
        ORDER BY created_at
    """, cid)
    return JSONResponse([{
        "id": r["id"], "role": r["role"], "content": r["content"],
        "tool_name": r["tool_name"], "tokens_used": r["tokens_used"],
        "model": r["model"], "created_at": r["created_at"].isoformat(),
    } for r in rows])


async def api_chat_send(request: Request):
    """POST: send a message and get AI response."""
    err = await _check_auth(request)
    if err: return err
    data = await request.json()
    cid = data.get("conversation_id")
    user_msg = data.get("message", "").strip()
    model_id = data.get("model", "auto")
    if not user_msg:
        return JSONResponse({"error": "message required"}, status_code=400)

    p = await get_pool()

    # Auto-create conversation if needed
    if not cid:
        row = await p.fetchrow(
            "INSERT INTO ops.chat_conversations DEFAULT VALUES RETURNING id")
        cid = str(row["id"])

    # Save user message
    await p.execute("""
        INSERT INTO ops.chat_messages (conversation_id, role, content)
        VALUES ($1::uuid, 'user', $2)
    """, cid, user_msg)

    # Build messages array from conversation history
    history = await p.fetch("""
        SELECT role, content FROM ops.chat_messages
        WHERE conversation_id = $1::uuid AND role IN ('user', 'assistant')
        ORDER BY created_at
    """, cid)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": h["role"], "content": h["content"]})

    # Auto-title from first user message
    if len([h for h in history if h["role"] == "user"]) <= 1:
        title = user_msg[:80]
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
    return JSONResponse({"models": AVAILABLE_MODELS, "default": "auto"})


# ── Health Probe ──────────────────────────────────────────────
async def health(request: Request):
    return JSONResponse({"status": "healthy", "service": "athena-dashboard-v2"})


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
routes = [
    Route("/health", health),
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
    # Documents
    Route("/api/documents", api_documents),
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
    Route("/api/chat/retry", api_chat_retry, methods=["POST"]),
    Route("/api/chat/models", api_chat_models),
    # Static files
    Mount("/static", StaticFiles(directory="/opt/wdws/dashboard/static"), name="static"),
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
  <div class="section-title">🤖 MCP Client Activity <span style="font-size:.75rem;color:var(--muted);font-weight:400">— ChatGPT, Claude Desktop tool calls</span></div>
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
</section>

<!-- ═══ CHAT TAB ═══ -->
<section id="tab-chat" class="hidden">
<style>
#tab-chat{display:flex;gap:0;height:calc(100vh - 160px);padding:0!important;margin:-24px;margin-top:0}
.chat-sidebar{width:260px;min-width:260px;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.chat-sidebar-hdr{padding:16px 16px 8px;display:flex;gap:8px;align-items:center}
.chat-sidebar-hdr .new-chat-btn{flex:1;background:none;border:1px solid var(--border);color:var(--text);padding:9px 14px;border-radius:22px;font-size:.85rem;font-weight:500;transition:all .2s;display:flex;align-items:center;gap:6px;justify-content:center}
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
.chat-main{flex:1;display:flex;flex-direction:column;overflow:hidden;background:var(--bg)}
.chat-messages{flex:1;overflow-y:auto;padding:20px 40px;display:flex;flex-direction:column;gap:16px}
.chat-msg{max-width:82%;padding:14px 18px;border-radius:16px;font-size:.9rem;line-height:1.65;word-wrap:break-word;white-space:pre-wrap}
.chat-msg-wrap{display:flex;flex-direction:column;max-width:82%}
.chat-msg-wrap.user{align-self:flex-end;align-items:flex-end}
.chat-msg-wrap.assistant{align-self:flex-start;align-items:flex-start}
.chat-msg-name{font-size:.72rem;font-weight:600;margin-bottom:4px;padding:0 6px;letter-spacing:.02em}
.chat-msg-wrap.user .chat-msg-name{color:var(--accent)}
.chat-msg-wrap.assistant .chat-msg-name{color:var(--purple)}
.chat-msg.user{display:block;padding:14px 18px;border-radius:16px 16px 4px 16px;background:var(--accent);color:#fff}
.chat-msg.assistant{display:block;padding:14px 18px;border-radius:16px 16px 16px 4px;background:var(--surface2);border:1px solid var(--border)}
.chat-msg.assistant .tool-badge{display:inline-block;background:rgba(34,211,238,.08);border:1px solid rgba(34,211,238,.2);border-radius:14px;padding:2px 10px;font-size:.72rem;color:var(--cyan);margin:4px 4px 4px 0}
.chat-msg.assistant code{background:var(--surface);padding:1px 5px;border-radius:3px;font-size:.85em}
.chat-msg.assistant pre{background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px;overflow-x:auto;margin:8px 0;font-size:.82rem}
.chat-msg.assistant pre code{background:none;padding:0}
.chat-msg.assistant strong{color:var(--cyan)}
.chat-msg.assistant a{color:var(--cyan)}
.chat-msg.assistant ul,.chat-msg.assistant ol{margin:8px 0;padding-left:20px}
.chat-msg.assistant li{margin:4px 0}
.chat-msg.assistant blockquote{border-left:3px solid var(--border);padding-left:12px;color:var(--muted);margin:8px 0}
.chat-msg.assistant h1,.chat-msg.assistant h2,.chat-msg.assistant h3{margin:12px 0 8px;color:var(--text)}
.chat-msg.assistant table{border-collapse:collapse;margin:8px 0;font-size:.82rem}
.chat-msg.assistant th,.chat-msg.assistant td{border:1px solid var(--border);padding:4px 8px}
.chat-msg.assistant th{background:var(--surface);color:var(--cyan)}

.chat-msg-wrap.assistant .retry-bar{display:none;align-items:center;gap:6px;margin-top:6px;padding:2px 0}
.chat-msg-wrap.assistant:last-of-type .retry-bar,.chat-msg-wrap.assistant:hover .retry-bar{display:flex}
.retry-bar .retry-btn{background:none;border:1px solid var(--border);color:var(--muted);font-size:.72rem;padding:4px 12px;border-radius:14px;cursor:pointer;display:flex;align-items:center;gap:4px;transition:all .2s}
.retry-bar .retry-btn:hover{border-color:var(--accent);color:var(--cyan);background:rgba(59,130,246,.06)}
.retry-bar .retry-btn:disabled{opacity:.5;cursor:not-allowed}
.retry-bar .retry-model{appearance:none;-webkit-appearance:none;background:var(--bg);border:1px solid var(--border);color:var(--muted);font-size:.7rem;padding:4px 22px 4px 8px;border-radius:12px;cursor:pointer;outline:none;transition:all .2s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='8' height='5'%3E%3Cpath d='M0 0l4 5 4-5z' fill='%236b7280'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 6px center}
.retry-bar .retry-model:hover,.retry-bar .retry-model:focus{border-color:var(--accent);color:var(--text)}
.retry-bar .retry-model option{background:var(--surface);color:var(--text)}

/* Welcome screen - Copilot style */
.chat-welcome{display:flex;flex-direction:column;align-items:center;justify-content:center;flex:1;padding:40px 20px;text-align:center}
.chat-welcome-greeting{font-size:2rem;font-weight:300;color:var(--text);margin-bottom:40px;line-height:1.3}
.chat-welcome-greeting span{background:linear-gradient(135deg,var(--accent),var(--cyan),var(--purple));-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-weight:500}
.chat-welcome .suggestions{display:flex;flex-wrap:wrap;gap:10px;justify-content:center;max-width:640px}
.chat-welcome .suggestions button{background:var(--surface);border:1px solid var(--border);color:var(--text);padding:10px 20px;border-radius:24px;font-size:.85rem;transition:all .2s;white-space:nowrap}
.chat-welcome .suggestions button:hover{border-color:var(--accent);color:var(--cyan);background:rgba(59,130,246,.06);transform:translateY(-1px)}

/* Input area - Copilot style */
.chat-input-area{padding:16px 40px 24px;background:var(--bg)}
.chat-input-wrap{max-width:720px;margin:0 auto;background:var(--surface);border:1px solid var(--border);border-radius:26px;display:flex;align-items:flex-end;padding:4px 4px 4px 20px;transition:border-color .2s,box-shadow .2s}
.chat-input-wrap:focus-within{border-color:var(--accent);box-shadow:0 0 0 2px rgba(59,130,246,.15)}
.chat-input-wrap textarea{flex:1;resize:none;min-height:40px;max-height:140px;border:none;background:transparent;color:var(--text);font-size:.9rem;line-height:1.5;padding:8px 0;outline:none}
.chat-input-wrap textarea::placeholder{color:var(--muted)}
.chat-input-wrap button{width:40px;height:40px;border-radius:50%;border:none;background:var(--accent);color:#fff;font-size:1.1rem;display:flex;align-items:center;justify-content:center;transition:all .2s;flex-shrink:0;margin-bottom:2px}
.chat-input-wrap button:hover{background:#2563eb;transform:scale(1.05)}
.chat-input-wrap select{appearance:none;-webkit-appearance:none;background:var(--bg);border:1px solid var(--border);color:var(--muted);font-size:.73rem;padding:5px 24px 5px 8px;border-radius:14px;cursor:pointer;margin:0 6px 4px 0;outline:none;flex-shrink:0;transition:all .2s;background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath d='M0 0l5 6 5-6z' fill='%236b7280'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 8px center}
.chat-input-wrap select:hover,.chat-input-wrap select:focus{border-color:var(--accent);color:var(--text)}
.chat-input-wrap select option{background:var(--surface);color:var(--text)}
.model-badge{font-size:.65rem;color:var(--muted);font-weight:400;margin-left:6px;opacity:.7}
.chat-input-wrap button:disabled{background:transparent;color:var(--muted);cursor:not-allowed;transform:none}
.chat-thinking{align-self:flex-start;color:var(--muted);font-size:.85rem;display:flex;align-items:center;gap:8px;padding:8px 16px}
.chat-thinking .dots span{animation:dotPulse 1.4s infinite;animation-fill-mode:both;display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--cyan);margin:0 2px}
.chat-thinking .dots span:nth-child(2){animation-delay:.2s}
.chat-thinking .dots span:nth-child(3){animation-delay:.4s}
@keyframes dotPulse{0%,80%,100%{opacity:.3;transform:scale(.8)}40%{opacity:1;transform:scale(1)}}
</style>
<div class="chat-sidebar">
  <div class="chat-sidebar-hdr">
    <button class="new-chat-btn" onclick="newConversation()">✦ New Chat</button>
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
        <button onclick="chatSuggestion('What cases are currently active?')">📋 Active cases</button>
        <button onclick="chatSuggestion('Search for documents mentioning Starbucks')">🔍 Search documents</button>
        <button onclick="chatSuggestion('Show me recent emails about court filings')">📧 Recent emails</button>
        <button onclick="chatSuggestion('What are the database statistics?')">📊 Database stats</button>
        <button onclick="chatSuggestion('Search the web for Washington state RCW 4.92')">🌐 Web search</button>
        <button onclick="chatSuggestion('Summarize case 24-2-01031-31')">⚖️ Summarize a case</button>
      </div>
    </div>
  </div>
  <div class="chat-input-area">
    <div class="chat-input-wrap">
      <textarea id="chat-input" placeholder="Message Athena AI..." rows="1" onkeydown="chatKeyDown(event)" oninput="autoGrow(this)"></textarea>
      <select id="chat-model-select" title="Select model">
        <option value="auto">⚡ Auto</option>
        <option value="think-low">💭 Low</option>
        <option value="think-med">💭 Med</option>
        <option value="think-high">🧠 High</option>
        <option value="think-xhigh">🧠 XHigh</option>
      </select>
      <button id="chat-send-btn" onclick="sendChat()" title="Send">➤</button>
    </div>
  </div>
</div>
</section>

<!-- ═══ MCP ACCOUNTS TAB ═══ -->
<section id="tab-mcp" class="hidden">
  <div class="section-title">🔑 OAuth Client Accounts</div>
  <p style="color:var(--muted);font-size:.85rem;margin-bottom:16px">
    Manage AI clients (ChatGPT, Claude Desktop) connected via OAuth 2.0 to the MCP server at
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
  .agent-email-triage { background:linear-gradient(135deg,#ec4899,#f472b6); color:#fff; }
  .agent-db-tuner { background:linear-gradient(135deg,#f97316,#fb923c); color:#fff; }
  .agent-case-strategy { background:linear-gradient(135deg,#6366f1,#818cf8); color:#fff; }
  .agent-retention { background:linear-gradient(135deg,#14b8a6,#2dd4bf); color:#fff; }
  .agent-timeline { background:linear-gradient(135deg,#0ea5e9,#38bdf8); color:#fff; }
  .agent-query-insight { background:linear-gradient(135deg,#a855f7,#c084fc); color:#fff; }
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

<script>
// ═══════════════════════════════════════════════════════════
//  GLOBAL STATE
// ═══════════════════════════════════════════════════════════
let stats={}, docOffset=0, casesList=[], ingestTimer=null, currentUser='User';

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
  localStorage.setItem('wdws-current-tab', t);
  
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
    mcp: loadMcpAccounts,
    analytics: loadAnalytics,
    security: loadACL,
    comms: loadComms,
    chatroom: loadChatRoom,
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
    $('modal-title').textContent = d.title || d.filename || id;
    let h='';
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

    // MCP Client Activity (ChatGPT / Claude tool calls)
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
      : '<div class="empty-state">No MCP client queries logged yet — make a query from ChatGPT or Claude Desktop</div>';

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

// ═══════════════════════════════════════════════════════════
//  TAB: AI CHAT
// ═══════════════════════════════════════════════════════════
let chatConvId=null, chatSending=false, chatConversations=[], chatShowArchived=false, chatModelOptions=null;

async function loadChat(){
  try {
    const url=chatShowArchived?'/api/chat/conversations?limit=50&archived=true':'/api/chat/conversations?limit=50';
    chatConversations = await api(url);
    renderConvList();
    $('chat-view-label').textContent=chatShowArchived?'Archived':'Conversations';
    $('toggle-archived').textContent=chatShowArchived?'Active':'Archived';
    // Set personalized greeting
    const gn=$('chat-greeting-name');
    if(gn) gn.textContent=currentUser||'there';
  } catch(e){ console.error('loadChat',e); }
}

function renderConvList(){
  const el=$('chat-conv-list');
  if(!chatConversations.length){
    el.innerHTML=`<div style="padding:20px;text-align:center;color:var(--muted);font-size:.82rem">${chatShowArchived?'No archived conversations':'No conversations yet'}</div>`;
    return;
  }
  el.innerHTML=chatConversations.map(c=>`
    <div class="chat-conv-item${c.id===chatConvId?' active':''}${c.archived?' archived':''}" onclick="openConversation('${c.id}')" data-id="${c.id}">
      <span class="conv-title" title="${esc(c.title)}">${esc(c.title)}</span>
      <div class="conv-actions">
        <button class="conv-action rename" onclick="event.stopPropagation();renameConversation('${c.id}')" title="Rename">✏️</button>
        <button class="conv-action archive" onclick="event.stopPropagation();${c.archived?'unarchive':'archive'}Conversation('${c.id}')" title="${c.archived?'Unarchive':'Archive'}">${c.archived?'📤':'📥'}</button>
        <button class="conv-action delete" onclick="event.stopPropagation();deleteConversation('${c.id}')" title="Delete">🗑️</button>
      </div>
    </div>
  `).join('');
}

async function newConversation(){
  chatConvId=null;
  $('chat-messages').innerHTML=`
    <div class="chat-welcome">
      <div style="display:flex;align-items:center;gap:24px;margin-bottom:32px">
        <img src="/static/athena-portrait.jpg" alt="Athena AI" style="height:96px;width:96px;border-radius:50%;box-shadow:0 4px 16px rgba(0,0,0,0.2);object-fit:cover"/>
        <div class="chat-welcome-greeting" style="margin:0">⚖️ Athena AI at your service, <span id="chat-greeting-name">${currentUser||'Counselor'}</span>. What shall we investigate?</div>
      </div>
      <div class="suggestions">
        <button onclick="chatSuggestion('What cases are currently active?')">📋 Active cases</button>
        <button onclick="chatSuggestion('Search for documents mentioning Starbucks')">🔍 Search documents</button>
        <button onclick="chatSuggestion('Show me recent emails about court filings')">📧 Recent emails</button>
        <button onclick="chatSuggestion('What are the database statistics?')">📊 Database stats</button>
        <button onclick="chatSuggestion('Search the web for Washington state RCW 4.92')">🌐 Web search</button>
        <button onclick="chatSuggestion('Summarize case 24-2-01031-31')">⚖️ Summarize a case</button>
      </div>
    </div>`;
  renderConvList();
  $('chat-input').value='';
  $('chat-input').focus();
}

async function openConversation(id){
  chatConvId=id;
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
      const avatar = m.role==='assistant' 
        ? '<img src="/static/athena-portrait.jpg" style="width:32px;height:32px;border-radius:50%;margin-right:8px;vertical-align:middle;object-fit:cover"/>'
        : '';
      return `<div class="chat-msg-wrap ${m.role}">
        <div class="chat-msg-name">${avatar}${m.role==='assistant'?'Athena AI':currentUser}</div>
        <div class="chat-msg ${m.role}">${m.role==='assistant'?renderMarkdown(m.content):esc(m.content)}</div>
        ${isLastAssistant?retryBarHTML():''}
      </div>`;
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
}

function autoGrow(el){
  el.style.height='auto';
  el.style.height=Math.min(el.scrollHeight,160)+'px';
}

async function sendChat(){
  if(chatSending) return;
  const input=$('chat-input');
  const msg=input.value.trim();
  if(!msg) return;

  const modelId=$('chat-model-select').value;
  const modelLabel=$('chat-model-select').selectedOptions[0].textContent;
  chatSending=true;
  $('chat-send-btn').disabled=true;
  input.value='';
  input.style.height='auto';

  const msgsEl=$('chat-messages');
  // Remove welcome if present
  const welcome=msgsEl.querySelector('.chat-welcome');
  if(welcome) welcome.remove();

  // Add user message
  const userWrap=document.createElement('div');
  userWrap.className='chat-msg-wrap user';
  userWrap.innerHTML=`<div class="chat-msg-name">${esc(currentUser)}</div>`;
  const userDiv=document.createElement('div');
  userDiv.className='chat-msg user';
  userDiv.textContent=msg;
  userWrap.appendChild(userDiv);
  msgsEl.appendChild(userWrap);

  // Add thinking indicator (model-aware)
  const thinkDiv=document.createElement('div');
  thinkDiv.className='chat-thinking';
  const thinkLabel=modelId.startsWith('think')?'Reasoning deeply...':'Searching & thinking...';
  thinkDiv.innerHTML='<div class="dots"><span></span><span></span><span></span></div> '+thinkLabel;
  msgsEl.appendChild(thinkDiv);
  msgsEl.scrollTop=msgsEl.scrollHeight;

  try {
    const resp=await api('/api/chat/send',{
      method:'POST',
      body:JSON.stringify({conversation_id:chatConvId,message:msg,model:modelId})
    });

    // Remove thinking
    thinkDiv.remove();

    // Set conversation ID if new
    if(!chatConvId && resp.conversation_id){
      chatConvId=resp.conversation_id;
      // Reload sidebar
      const url=chatShowArchived?'/api/chat/conversations?limit=50&archived=true':'/api/chat/conversations?limit=50';
      chatConversations = await api(url);
      renderConvList();
    }

    // Show tool badges if any
    let toolHtml='';
    if(resp.tool_calls&&resp.tool_calls.length){
      toolHtml=resp.tool_calls.map(t=>
        `<span class="tool-badge">\ud83d\udd27 ${t.tool}</span>`
      ).join('')+'<br>';
    }

    // Add assistant message
    const rLabel=resp.model_label||modelLabel;
    const aWrap=document.createElement('div');
    aWrap.className='chat-msg-wrap assistant';
    aWrap.innerHTML=`<div class="chat-msg-name"><img src="/static/athena-portrait.jpg" style="width:32px;height:32px;border-radius:50%;margin-right:8px;vertical-align:middle;object-fit:cover"/>Athena AI <span class="model-badge">${esc(rLabel)}</span></div>`;
    const aDiv=document.createElement('div');
    aDiv.className='chat-msg assistant';
    aDiv.innerHTML=toolHtml+renderMarkdown(resp.response||'(no response)');
    aWrap.appendChild(aDiv);
    aWrap.appendChild(makeRetryBar());
    msgsEl.appendChild(aWrap);
    msgsEl.scrollTop=msgsEl.scrollHeight;

    // Update sidebar title
    const existing=chatConversations.find(c=>c.id===chatConvId);
    if(existing&&existing.title==='New Conversation'){
      existing.title=msg.substring(0,80);
      renderConvList();
    }
  } catch(e){
    thinkDiv.remove();
    const errWrap=document.createElement('div');
    errWrap.className='chat-msg-wrap assistant';
    errWrap.innerHTML=`<div class="chat-msg-name"><img src="/static/athena-portrait.jpg" style="width:32px;height:32px;border-radius:50%;margin-right:8px;vertical-align:middle;object-fit:cover"/>Athena AI</div>`;
    const errDiv=document.createElement('div');
    errDiv.className='chat-msg assistant';
    errDiv.style.borderColor='var(--red)';
    errDiv.textContent='Error: '+e.message;
    errWrap.appendChild(errDiv);
    msgsEl.appendChild(errWrap);
  } finally {
    chatSending=false;
    $('chat-send-btn').disabled=false;
    input.focus();
  }
}


function retryBarHTML(){
  const opts=chatModelOptions||[
    {id:'auto',label:'⚡ Auto'},{id:'think-low',label:'💭 Low'},
    {id:'think-med',label:'💭 Med'},{id:'think-high',label:'🧠 High'},
    {id:'think-xhigh',label:'🧠 XHigh'}
  ];
  const curModel=$('chat-model-select')?$('chat-model-select').value:'auto';
  return `<div class="retry-bar">
    <button class="retry-btn" onclick="retryChat(this)" title="Regenerate with selected model">🔄 Retry</button>
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
  btnEl.textContent='⏳ Retrying...';

  const msgsEl=$('chat-messages');

  // Remove the last assistant message wrapper (which contains this retry bar)
  const lastAssistant=bar.closest('.chat-msg-wrap.assistant');
  if(lastAssistant){
    // Add thinking indicator in its place
    const thinkDiv=document.createElement('div');
    thinkDiv.className='chat-thinking';
    const thinkLabel=modelId.startsWith('think')?'Re-reasoning deeply...':'Re-searching & thinking...';
    thinkDiv.innerHTML='<div class="dots"><span></span><span></span><span></span></div> '+thinkLabel;
    lastAssistant.replaceWith(thinkDiv);

    try {
      const resp=await api('/api/chat/retry',{
        method:'POST',
        body:JSON.stringify({conversation_id:chatConvId,model:modelId})
      });

      thinkDiv.remove();

      let toolHtml='';
      if(resp.tool_calls&&resp.tool_calls.length){
        toolHtml=resp.tool_calls.map(t=>`<span class="tool-badge">🔧 ${t.tool}</span>`).join('')+'<br>';
      }

      const rLabel=resp.model_label||modelLabel;
      const aWrap=document.createElement('div');
      aWrap.className='chat-msg-wrap assistant';
      aWrap.innerHTML=`<div class="chat-msg-name"><img src="/static/athena-portrait.jpg" style="width:32px;height:32px;border-radius:50%;margin-right:8px;vertical-align:middle;object-fit:cover"/>Athena AI <span class="model-badge">${esc(rLabel)}</span></div>`;
      const aDiv=document.createElement('div');
      aDiv.className='chat-msg assistant';
      aDiv.innerHTML=toolHtml+renderMarkdown(resp.response||'(no response)');
      aWrap.appendChild(aDiv);
      aWrap.appendChild(makeRetryBar());
      msgsEl.appendChild(aWrap);
      msgsEl.scrollTop=msgsEl.scrollHeight;
    } catch(e){
      thinkDiv.remove();
      const errWrap=document.createElement('div');
      errWrap.className='chat-msg-wrap assistant';
      errWrap.innerHTML=`<div class="chat-msg-name"><img src="/static/athena-portrait.jpg" style="width:32px;height:32px;border-radius:50%;margin-right:8px;vertical-align:middle;object-fit:cover"/>Athena AI</div>`;
      const errDiv=document.createElement('div');
      errDiv.className='chat-msg assistant';
      errDiv.style.borderColor='var(--red)';
      errDiv.textContent='Retry error: '+e.message;
      errWrap.appendChild(errDiv);
      errWrap.appendChild(makeRetryBar());
      msgsEl.appendChild(errWrap);
      msgsEl.scrollTop=msgsEl.scrollHeight;
    }
  }

  chatSending=false;
  $('chat-send-btn').disabled=false;
  $('chat-input').focus();
}

// Simple markdown renderer
function renderMarkdown(text){
  if(!text) return '';
  let html=esc(text);
  // Code blocks
  html=html.replace(/```(\w*)\n([\s\S]*?)```/g,(m,lang,code)=>
    `<pre><code>${code}</code></pre>`);
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
  html=html.replace(/\[([^\]]+)\]\(([^)]+)\)/g,'<a href="$2" target="_blank">$1</a>');
  // Unordered lists
  html=html.replace(/^[•\-\*] (.+)$/gm,'<li>$1</li>');
  html=html.replace(/(<li>.*<\/li>)/gs,'<ul>$1</ul>');
  // Ordered lists
  html=html.replace(/^\d+\. (.+)$/gm,'<li>$1</li>');
  // Blockquotes
  html=html.replace(/^&gt; (.+)$/gm,'<blockquote>$1</blockquote>');
  // Line breaks (double newline = paragraph break)
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
  const savedTab = localStorage.getItem('wdws-current-tab');
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
      const isCode = item.level === 'code-fix' || item.category === 'codex-response';
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
  'self-healing':'SH','data-quality':'DQ','email-triage':'ET','db-tuner':'DT',
  'case-strategy':'CS','retention':'RT','timeline':'TL','query-insight':'QI'
};

const AGENT_NAMES = {
  'orchestrator':'Orchestrator','watchdog':'Watchdog','code-doctor':'Code Doctor',
  'security-sentinel':'Security Sentinel','self-healing':'Self Healing',
  'data-quality':'Data Quality','email-triage':'Email Triage','db-tuner':'DB Tuner',
  'case-strategy':'Case Strategy','retention':'Retention','timeline':'Timeline',
  'query-insight':'Query Insight'
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