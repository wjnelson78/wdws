#!/usr/bin/env python3
"""
WDWS Enterprise Dashboard v2.0 — PostgreSQL-native.

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

import os, json, time, secrets, subprocess
from pathlib import Path
from typing import Optional

import asyncpg
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn

# ── Config ────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "nelson2026")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "9100"))
INGEST_LOG     = os.getenv("INGEST_LOG", "/opt/wdws/ingest_full.log")
SYNC_LOG       = os.getenv("SYNC_LOG", "/opt/wdws/email_sync.log")
AGENT_LOG      = os.getenv("AGENT_LOG", "/opt/wdws/email_agent.log")

pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=8, command_timeout=30
        )
    return pool


# ── Auth ──────────────────────────────────────────────────────
_session_tok = secrets.token_hex(24)


def _check_auth(request: Request):
    if request.cookies.get("dash_session") == _session_tok:
        return None
    import base64
    auth = request.headers.get("authorization", "")
    if auth.startswith("Basic "):
        try:
            u, p = base64.b64decode(auth[6:]).decode().split(":", 1)
            if u == DASHBOARD_USER and p == DASHBOARD_PASS:
                return None
        except Exception:
            pass
    return JSONResponse({"error": "unauthorized"}, status_code=401)


async def api_login(request: Request):
    data = await request.json()
    if data.get("username") == DASHBOARD_USER and data.get("password") == DASHBOARD_PASS:
        resp = JSONResponse({"ok": True})
        resp.set_cookie(
            "dash_session", _session_tok, httponly=True,
            max_age=86400 * 7, samesite="lax"
        )
        return resp
    return JSONResponse({"error": "bad credentials"}, status_code=401)


async def api_logout(request: Request):
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("dash_session")
    return resp


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
    err = _check_auth(request)
    if err:
        return err
    p = await get_pool()
    row = await p.fetchval("SELECT ops.get_system_stats()")
    return JSONResponse(json.loads(row))


# ── Domain Breakdown ──────────────────────────────────────────
async def api_domain_stats(request: Request):
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
    if err:
        return err
    p = await get_pool()
    rows = await p.fetch("""
        SELECT ar.id, ar.name, ar.description, ar.capabilities,
               ar.is_active, ar.created_at,
               COUNT(s.id)      AS session_count,
               MAX(s.started_at) AS last_session
        FROM ops.agent_registry ar
        LEFT JOIN ops.agent_sessions s ON ar.id = s.agent_id
        GROUP BY ar.id ORDER BY ar.created_at DESC
    """)
    return JSONResponse([_row_dict(r, ["capabilities"]) for r in rows])


# ── Agent Sessions ────────────────────────────────────────────
async def api_agent_sessions(request: Request):
    err = _check_auth(request)
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


# ── Full-Text Search ──────────────────────────────────────────
async def api_search(request: Request):
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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
    err = _check_auth(request)
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


# ── Health Probe ──────────────────────────────────────────────
async def health(request: Request):
    return JSONResponse({"status": "healthy", "service": "wdws-dashboard-v2"})


# ── Serve SPA ─────────────────────────────────────────────────
async def page_dashboard(request: Request):
    return HTMLResponse(DASHBOARD_HTML)


# ══════════════════════════════════════════════════════════════
#  ROUTES + MIDDLEWARE
# ══════════════════════════════════════════════════════════════
routes = [
    Route("/health", health),
    Route("/", page_dashboard),
    Route("/dashboard", page_dashboard),
    Route("/dashboard/login", api_login, methods=["POST"]),
    Route("/dashboard/logout", api_logout, methods=["POST"]),
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
    print(f"  WDWS Enterprise Dashboard v2.0")
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
<title>WDWS Enterprise Dashboard</title>
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

nav{background:var(--surface);border-bottom:1px solid var(--border);display:flex;gap:0;padding:0 24px;overflow-x:auto}
nav button{background:none;border:none;color:var(--muted);padding:11px 18px;font-size:.85rem;font-weight:500;border-bottom:2px solid transparent;transition:all .2s;white-space:nowrap}
nav button:hover{color:var(--text)}
nav button.active{color:var(--cyan);border-bottom-color:var(--cyan)}
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

/* Auto-refresh indicator */
.pulse{animation:pulse-anim 2s infinite}
@keyframes pulse-anim{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head><body>
<div id="app">

<!-- ═══ LOGIN SCREEN ═══ -->
<div id="login-screen" class="login-box">
  <h2>🔒 WDWS Enterprise Dashboard</h2>
  <input id="login-user" type="text" placeholder="Username" autocomplete="username"/>
  <input id="login-pass" type="password" placeholder="Password" autocomplete="current-password"
    onkeydown="if(event.key==='Enter')doLogin()"/>
  <button onclick="doLogin()">Sign In</button>
  <div id="login-error" class="error hidden"></div>
</div>

<!-- ═══ MAIN APP ═══ -->
<div id="main-app" class="hidden">
<header>
  <h1>🏛️ WDWS Enterprise Platform</h1>
  <div class="right">
    <span id="hdr-info"></span>
    <span id="hdr-refresh" class="pulse" style="font-size:10px;color:var(--green)">● LIVE</span>
    <button class="btn btn-outline btn-sm" onclick="doLogout()">Logout</button>
  </div>
</header>
<nav id="main-nav">
  <button class="active" data-tab="overview" onclick="switchTab('overview')">📊 Overview</button>
  <button data-tab="documents" onclick="switchTab('documents')">📄 Documents</button>
  <button data-tab="cases" onclick="switchTab('cases')">⚖️ Legal Cases</button>
  <button data-tab="search" onclick="switchTab('search')">🔍 Search</button>
  <button data-tab="ingestion" onclick="switchTab('ingestion')">⚙️ Ingestion</button>
  <button data-tab="audit" onclick="switchTab('audit')">🔐 Audit Trail</button>
  <button data-tab="agents" onclick="switchTab('agents')">🤖 Agents</button>
  <button data-tab="database" onclick="switchTab('database')">🗄️ Database</button>
  <button data-tab="email" onclick="switchTab('email')">📧 Email Sync</button>
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
  <div class="section-title">🔐 Access Log</div>
  <div id="audit-access"></div>
  <div class="section-title" style="margin-top:24px">🔍 Search Analytics</div>
  <div id="audit-searches"></div>
</section>

<!-- ═══ AGENTS TAB ═══ -->
<section id="tab-agents" class="hidden">
  <div id="agents-list"></div>
  <div class="section-title" style="margin-top:24px">📝 Recent Sessions</div>
  <div id="agent-sessions"></div>
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

</div><!-- /app -->

<script>
// ═══════════════════════════════════════════════════════════
//  GLOBAL STATE
// ═══════════════════════════════════════════════════════════
let stats={}, docOffset=0, casesList=[], ingestTimer=null;

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
function switchTab(t){
  document.querySelectorAll('main>section').forEach(s=>s.classList.add('hidden'));
  $('tab-'+t).classList.remove('hidden');
  document.querySelectorAll('#main-nav button').forEach(
    b => b.classList.toggle('active', b.dataset.tab===t));
  const loaders = {
    overview: loadOverview,
    documents: loadDocs,
    cases: loadCases,
    ingestion: ()=>{ loadIngestLive(); loadIngestJobs(); },
    audit: loadAudit,
    agents: loadAgents,
    database: loadDbHealth,
    email: loadEmail,
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
document.addEventListener('keydown',function(e){
  if(e.key==='Escape') closeModal();
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
    const [access, searches] = await Promise.all([
      api('/api/audit/access?limit=100'),
      api('/api/audit/searches?limit=50')
    ]);

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
    const [agents, sessions] = await Promise.all([
      api('/api/agents'), api('/api/agents/sessions?limit=20')
    ]);

    $('agents-list').innerHTML = agents.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>ID</th><th>Name</th><th>Active</th><th>Capabilities</th><th>Sessions</th><th>Last Active</th></tr></thead>
        <tbody>${agents.map(a=>`<tr>
          <td style="font-family:monospace;font-size:.8rem">${a.id||''}</td>
          <td><strong>${a.name||''}</strong></td>
          <td>${a.is_active?'<span class="dot dot-green"></span>Active':'<span class="dot dot-red"></span>Inactive'}</td>
          <td>${(a.capabilities||[]).map(c=>`<span class="badge badge-purple">${c}</span>`).join(' ')}</td>
          <td>${a.session_count||0}</td>
          <td>${fmtTime(a.last_session)}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No agents registered yet. Agents self-register when they first connect.</div>';

    $('agent-sessions').innerHTML = sessions.length
      ? `<div class="tbl-wrap"><table>
        <thead><tr><th>Session</th><th>Agent</th><th>Started</th><th>Ended</th><th>Summary</th></tr></thead>
        <tbody>${sessions.map(s=>`<tr>
          <td style="font-family:monospace;font-size:.78rem">${(s.id||'').substring(0,8)}…</td>
          <td>${s.agent_id||''}</td>
          <td>${fmtTime(s.started_at)}</td>
          <td>${fmtTime(s.ended_at)}</td>
          <td>${s.summary||''}</td>
        </tr>`).join('')}</tbody></table></div>`
      : '<div class="empty-state">No sessions yet</div>';
  } catch { $('agents-list').innerHTML='<div class="empty-state">Failed to load agents</div>'; }
}

// ═══════════════════════════════════════════════════════════
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
//  INIT
// ═══════════════════════════════════════════════════════════
async function init(){ await loadOverview(); }

// Auto-login check
(async()=>{
  try {
    await api('/api/stats');
    $('login-screen').classList.add('hidden');
    $('main-app').classList.remove('hidden');
    init();
  } catch { showLogin(); }
})();
</script>
</body></html>"""


if __name__ == "__main__":
    main()
