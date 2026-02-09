#!/usr/bin/env python3
"""
WDWS Unified MCP Server — PostgreSQL-native.

Single MCP server consolidating all domains:
  • Legal cases (court filings, emails, attachments)
  • Medical records (clinical notes, lab results, imaging)
  • Paperless-ngx documents

Connects directly to PostgreSQL + pgvector on localhost.
Supports both stdio (Claude Desktop) and SSE (HTTP) transport.

Run:
  stdio:  /opt/wdws/venv/bin/python3 /opt/wdws/mcp-server/mcp_server.py
  http:   /opt/wdws/venv/bin/python3 /opt/wdws/mcp-server/mcp_server.py --http --port 9200
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime, date
from typing import Optional, Any

import asyncpg
import httpx
from mcp.server.fastmcp import FastMCP

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
async def _log_access(tool_name: str, query: str, result_count: int):
    """Record tool usage for audit trail."""
    try:
        p = await get_pool()
        await p.execute(
            """INSERT INTO ops.search_analytics (query, tool_name, result_count, searched_at)
               VALUES ($1, $2, $3, now())""",
            query, tool_name, result_count,
        )
    except Exception:
        pass  # Don't let logging failures break tools


# ══════════════════════════════════════════════════════════════
#  MCP SERVER
# ══════════════════════════════════════════════════════════════
mcp = FastMCP(
    "WDWS",
    instructions=(
        "William Nelson's unified document intelligence server. "
        "Searches legal cases, court filings, emails, medical records, "
        "and paperless documents via PostgreSQL + pgvector."
    ),
    host="0.0.0.0",
    port=9200,
)


# ══════════════════════════════════════════════════════════════
#  TOOL: Semantic Search (vector similarity)
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Full-Text Search (tsvector)
# ══════════════════════════════════════════════════════════════
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
        document_type: Filter by type — "email", "court_filing", "email_attachment", "medical_export"
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Search Emails
# ══════════════════════════════════════════════════════════════
@mcp.tool()
async def search_emails(
    query: str = "",
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    mailbox: Optional[str] = None,
    case_number: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Get Document (full content)
# ══════════════════════════════════════════════════════════════
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
        if len(content) > 8000:
            content = content[:8000] + "\n\n... [truncated — document is longer]"

        result = {
            "id": str(doc["id"]),
            "title": doc["title"],
            "filename": doc["filename"],
            "domain": doc["domain"],
            "document_type": doc["document_type"],
            "content": content,
            "created_at": _ser(doc["created_at"]),
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

        # Check for medical metadata
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

        await _log_access("get_document", document_id, 1)
        return json.dumps(result)
    except Exception as e:
        log.error("get_document error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  TOOL: Lookup Legal Case
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  TOOL: List All Cases
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Search Court Filings
# ══════════════════════════════════════════════════════════════
@mcp.tool()
async def search_court_filings(
    query: str = "",
    case_number: Optional[str] = None,
    filing_type: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search court filings (motions, orders, declarations, etc.).

    Args:
        query: Full-text search query
        case_number: Filter to specific case
        filing_type: Filter by filing type (e.g. "motion", "order", "declaration")
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Search Medical Records
# ══════════════════════════════════════════════════════════════
@mcp.tool()
async def search_medical_records(
    query: str,
    record_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 10,
) -> str:
    """Search medical records including clinical notes, lab results, and imaging.

    Args:
        query: Search terms (symptoms, diagnoses, medications, etc.)
        record_type: Filter by type (e.g. "lab_result", "clinical_note", "imaging")
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Search by Case Number
# ══════════════════════════════════════════════════════════════
@mcp.tool()
async def search_by_case(case_number: str, limit: int = 20) -> str:
    """Find all documents related to a specific case number.

    Returns emails, filings, and attachments associated with the case.

    Args:
        case_number: Full or partial case number
        limit: Max results (default 20)
    """
    limit = min(max(limit, 1), 50)
    p = await get_pool()
    try:
        # Search via case_documents join AND full-text match on case number
        rows = await p.fetch("""
            SELECT DISTINCT d.id, d.title, d.filename, d.domain,
                   d.document_type, d.created_at,
                   em.sender, em.date_sent, em.direction
            FROM core.documents d
            LEFT JOIN legal.email_metadata em ON d.id = em.document_id
            LEFT JOIN legal.case_documents cd ON d.id = cd.document_id
            LEFT JOIN legal.cases c ON cd.case_id = c.id
            WHERE c.case_number ILIKE $1
               OR d.title ILIKE $1
               OR d.source_path ILIKE $1
            ORDER BY d.created_at DESC NULLS LAST
            LIMIT $2
        """, f"%{case_number.strip()}%", limit)

        results = [{
            "document_id": str(r["id"]),
            "title": r["title"] or r["filename"],
            "domain": r["domain"],
            "type": r["document_type"],
            "date": _ser(r["created_at"]),
            "sender": r["sender"],
            "email_date": _ser(r["date_sent"]),
            "direction": r["direction"],
        } for r in rows]

        await _log_access("search_by_case", case_number, len(results))
        return json.dumps({"results": results, "count": len(results), "case_number": case_number})
    except Exception as e:
        log.error("search_by_case error: %s", e)
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════
#  TOOL: RAG Query (search + LLM synthesis)
# ══════════════════════════════════════════════════════════════
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
        # Step 1: Semantic search for relevant chunks
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

        # Step 2: Build context and call LLM
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Database Stats
# ══════════════════════════════════════════════════════════════
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

        return json.dumps({
            "total_documents": total["cnt"],
            "total_chunks": chunks["cnt"],
            "chunks_with_embeddings": chunks["embedded"],
            "total_cases": cases["cnt"],
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


# ══════════════════════════════════════════════════════════════
#  TOOL: Search Email Threads
# ══════════════════════════════════════════════════════════════
@mcp.tool()
async def get_email_thread(message_id: Optional[str] = None, thread_id: Optional[str] = None) -> str:
    """Retrieve a full email thread/conversation.

    Provide either the message_id of any email in the thread, or the thread_id directly.

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


# ══════════════════════════════════════════════════════════════
#  TOOL: Timeline / Recent Activity
# ══════════════════════════════════════════════════════════════
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


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description="WDWS Unified MCP Server")
    parser.add_argument("--http", action="store_true", help="Run as HTTP/SSE server")
    parser.add_argument("--port", type=int, default=9200, help="HTTP port (default 9200)")
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    args = parser.parse_args()

    if args.http:
        mcp.settings.host = args.host
        mcp.settings.port = args.port
        log.info("Starting WDWS MCP Server (HTTP/SSE) on %s:%d", args.host, args.port)
        mcp.run(transport="sse")
    else:
        log.info("Starting WDWS MCP Server (stdio)")
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
