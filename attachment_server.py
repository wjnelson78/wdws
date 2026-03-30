#!/usr/bin/env python3
"""
WDWS Attachment Download Server — lightweight file server for email attachments.

Serves binary attachment files (PDFs, Excel, etc.) from the Athena Cognitive Engine
by attachment_id or document_id. Used by remote AI agents and tools that need
to fetch attachment files over HTTP.

Runs on uvicorn, default port 9101.

Usage:
    /opt/wdws/venv/bin/python3 /opt/wdws/attachment_server.py
    
    # Or with custom port:
    ATTACHMENT_PORT=9101 /opt/wdws/venv/bin/python3 /opt/wdws/attachment_server.py

Endpoints:
    GET /attachments/{attachment_id}   — Download by email attachment UUID
    GET /documents/{document_id}/file  — Download by document UUID
    GET /attachments?q=filename        — List/search attachments
    GET /health                        — Health check
"""

import os
import logging
from pathlib import Path
from typing import Optional

import asyncpg
import uvicorn
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("attachment-server")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
ATTACHMENT_PORT = int(os.getenv("ATTACHMENT_PORT", "9101"))
ALLOWED_BASE = Path("/opt/wdws/data")  # Security: only serve files under this path

pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4, command_timeout=15)
    return pool


async def health(request: Request):
    return JSONResponse({"status": "ok", "service": "wdws-attachment-server"})


async def download_by_attachment_id(request: Request):
    """Download an attachment file by its UUID from legal.email_attachments."""
    attachment_id = request.path_params["attachment_id"]
    try:
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT ea.filename, ea.content_type,
                   d.metadata->>'local_attachment_path' AS file_path
            FROM legal.email_attachments ea
            LEFT JOIN core.documents d ON d.id = ea.child_doc_id
            WHERE ea.id = $1::uuid
        """, attachment_id)
    except Exception as e:
        log.error(f"DB error for attachment {attachment_id}: {e}")
        return JSONResponse({"error": "Database error", "detail": str(e)}, status_code=500)

    if not row or not row["file_path"]:
        return JSONResponse({"error": "Attachment not found"}, status_code=404)

    file_path = Path(row["file_path"])

    # Security: ensure the file is under the allowed base directory
    try:
        file_path.resolve().relative_to(ALLOWED_BASE.resolve())
    except ValueError:
        log.warning(f"Path traversal attempt blocked: {file_path}")
        return JSONResponse({"error": "Access denied"}, status_code=403)

    if not file_path.exists():
        return JSONResponse({"error": "File not found on disk", "path": str(file_path)}, status_code=404)

    content_type = row["content_type"] or "application/octet-stream"
    filename = row["filename"] or file_path.name

    log.info(f"Serving attachment {attachment_id}: {filename} ({file_path})")
    return FileResponse(str(file_path), media_type=content_type, filename=filename)


async def download_by_document_id(request: Request):
    """Download a file by its document UUID from core.documents metadata."""
    document_id = request.path_params["document_id"]
    try:
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT filename,
                   metadata->>'local_attachment_path' AS file_path,
                   COALESCE(
                       (SELECT ea.content_type FROM legal.email_attachments ea WHERE ea.child_doc_id = d.id LIMIT 1),
                       'application/octet-stream'
                   ) AS content_type
            FROM core.documents d
            WHERE d.id = $1::uuid
        """, document_id)
    except Exception as e:
        log.error(f"DB error for document {document_id}: {e}")
        return JSONResponse({"error": "Database error", "detail": str(e)}, status_code=500)

    if not row or not row["file_path"]:
        return JSONResponse({"error": "Document not found or has no file path"}, status_code=404)

    file_path = Path(row["file_path"])

    try:
        file_path.resolve().relative_to(ALLOWED_BASE.resolve())
    except ValueError:
        log.warning(f"Path traversal attempt blocked: {file_path}")
        return JSONResponse({"error": "Access denied"}, status_code=403)

    if not file_path.exists():
        return JSONResponse({"error": "File not found on disk", "path": str(file_path)}, status_code=404)

    content_type = row["content_type"] or "application/octet-stream"
    filename = row["filename"] or file_path.name

    log.info(f"Serving document {document_id}: {filename} ({file_path})")
    return FileResponse(str(file_path), media_type=content_type, filename=filename)


async def list_attachments(request: Request):
    """List recent attachments with their download URLs. Useful for discovery."""
    limit = min(int(request.query_params.get("limit", "20")), 100)
    query = request.query_params.get("q", "")

    try:
        p = await get_pool()
        if query:
            rows = await p.fetch("""
                SELECT ea.id AS attachment_id, ea.filename, ea.content_type, ea.file_size,
                       ea.email_doc_id, ea.created_at
                FROM legal.email_attachments ea
                WHERE ea.filename ILIKE $1
                ORDER BY ea.created_at DESC
                LIMIT $2
            """, f"%{query}%", limit)
        else:
            rows = await p.fetch("""
                SELECT ea.id AS attachment_id, ea.filename, ea.content_type, ea.file_size,
                       ea.email_doc_id, ea.created_at
                FROM legal.email_attachments ea
                ORDER BY ea.created_at DESC
                LIMIT $1
            """, limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

    base_url = str(request.base_url).rstrip("/")
    results = []
    for row in rows:
        results.append({
            "attachment_id": str(row["attachment_id"]),
            "filename": row["filename"],
            "content_type": row["content_type"],
            "file_size": row["file_size"],
            "download_url": f"{base_url}/attachments/{row['attachment_id']}",
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
        })

    return JSONResponse({"attachments": results, "count": len(results)})


routes = [
    Route("/health", health),
    Route("/attachments", list_attachments),
    Route("/attachments/{attachment_id}", download_by_attachment_id),
    Route("/documents/{document_id}/file", download_by_document_id),
]

middleware = [
    Middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]),
]

app = Starlette(routes=routes, middleware=middleware)


def main():
    host = os.getenv("ATTACHMENT_HOST", "0.0.0.0")
    print(f"\n{'=' * 50}")
    print(f"  WDWS Attachment Server")
    print(f"  http://{host}:{ATTACHMENT_PORT}/attachments")
    print(f"  Health: http://{host}:{ATTACHMENT_PORT}/health")
    print(f"{'=' * 50}\n")
    uvicorn.run(app, host=host, port=ATTACHMENT_PORT)


if __name__ == "__main__":
    main()
