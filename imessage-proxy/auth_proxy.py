#!/usr/bin/env python3
"""
iMessage MCP Auth Proxy — runs on 172.16.32.207

Full OAuth 2.0 reverse-proxy for the iMessage MCP server.
Handles the complete OAuth flow by proxying auth requests to the
WDWS MCP server (port 9200), validates Bearer tokens against the
shared PostgreSQL database, and proxies authenticated MCP requests
to the iMessage MCP backend (Mac, port 9400).

Implements:
    - RFC 8414  /.well-known/oauth-authorization-server
    - RFC 9728  /.well-known/oauth-protected-resource
    - OAuth endpoints: /authorize, /token, /register, /revoke
    - MCP data endpoints: /sse, /mcp, /messages (auth-required)

Usage:
    /opt/wdws/venv/bin/python3 /opt/wdws/imessage-proxy/auth_proxy.py

Systemd:  wdws-imessage-proxy.service (port 9400)
Tunnel:   imessage.12432.net → http://127.0.0.1:9400
Auth:     Proxied to WDWS MCP OAuth → http://127.0.0.1:9200
Backend:  http://172.16.88.223:9400 (Mac running mcp-proxy with iMessage MCP)
"""

import asyncio
import logging
import os
import time

import asyncpg
import httpx
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse, Response
from starlette.routing import Route, Mount

# ── Config ───────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
BACKEND_URL = os.getenv("IMESSAGE_BACKEND_URL", "http://172.16.88.223:9400")
AUTH_SERVER_URL = os.getenv("AUTH_SERVER_URL", "http://127.0.0.1:9200")
LISTEN_PORT = int(os.getenv("LISTEN_PORT", "9400"))
LISTEN_HOST = os.getenv("LISTEN_HOST", "0.0.0.0")
IMESSAGE_BASE = "https://imessage.12432.net"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("imessage-auth-proxy")

# ── Database Pool ────────────────────────────────────────────
pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global pool
    if pool is None:
        pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=5, command_timeout=10
        )
        log.info("PostgreSQL pool created")
    return pool


# ── Token Validation ─────────────────────────────────────────
async def validate_token(token: str) -> dict | None:
    """Validate a Bearer token against ops.oauth_access_tokens.
    Returns token row dict if valid, None otherwise."""
    p = await get_pool()
    row = await p.fetchrow(
        "SELECT token, client_id, scopes, expires_at FROM ops.oauth_access_tokens WHERE token = $1",
        token,
    )
    if not row:
        return None
    if row["expires_at"] and row["expires_at"] < int(time.time()):
        # Expired — clean up
        await p.execute("DELETE FROM ops.oauth_access_tokens WHERE token = $1", token)
        return None
    return dict(row)


def extract_bearer_token(request: Request) -> str | None:
    """Extract Bearer token from Authorization header."""
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


# ── OAuth Metadata (RFC 8414) ────────────────────────────────
# All OAuth endpoints point to THIS server (imessage.12432.net), which
# proxies them to the real OAuth server at klunky.12432.net / port 9200.
OAUTH_METADATA = {
    "issuer": IMESSAGE_BASE,
    "authorization_endpoint": f"{IMESSAGE_BASE}/authorize",
    "token_endpoint": f"{IMESSAGE_BASE}/token",
    "registration_endpoint": f"{IMESSAGE_BASE}/register",
    "revocation_endpoint": f"{IMESSAGE_BASE}/revoke",
    "response_types_supported": ["code"],
    "grant_types_supported": ["authorization_code", "refresh_token"],
    "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
    "scopes_supported": ["read", "write", "admin", "search", "medical"],
    "code_challenge_methods_supported": ["S256"],
}

# ── OAuth Protected Resource (RFC 9728) ──────────────────────
OAUTH_PROTECTED_RESOURCE = {
    "resource": IMESSAGE_BASE,
    "authorization_servers": [IMESSAGE_BASE],
    "bearer_methods_supported": ["header"],
    "scopes_supported": ["read", "write", "admin", "search", "medical"],
}


# ── Request Handlers ─────────────────────────────────────────

async def health(request: Request) -> JSONResponse:
    """Health check endpoint."""
    try:
        p = await get_pool()
        await p.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False

    # Check backend connectivity (SSE streams indefinitely, so just check TCP connect)
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            async with client.stream("GET", f"{BACKEND_URL}/sse") as resp:
                backend_ok = resp.status_code == 200
                # Don't consume the stream — just check we got a response
    except Exception:
        backend_ok = False

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{AUTH_SERVER_URL}/.well-known/oauth-authorization-server")
            auth_ok = resp.status_code == 200
    except Exception:
        auth_ok = False

    all_ok = db_ok and backend_ok and auth_ok
    return JSONResponse({
        "status": "ok" if all_ok else "degraded",
        "database": "connected" if db_ok else "error",
        "backend": "connected" if backend_ok else "unreachable",
        "auth_server": "connected" if auth_ok else "unreachable",
        "service": "imessage-auth-proxy",
    })


async def oauth_metadata_handler(request: Request) -> JSONResponse:
    """Serve OAuth 2.0 Authorization Server Metadata (RFC 8414)."""
    return JSONResponse(OAUTH_METADATA)


async def oauth_protected_resource(request: Request) -> JSONResponse:
    """Serve OAuth Protected Resource Metadata (RFC 9728)."""
    return JSONResponse(OAUTH_PROTECTED_RESOURCE)


# ── OAuth Proxy Handlers ─────────────────────────────────────
# These proxy /authorize, /token, /register, /revoke to the real
# WDWS MCP OAuth server running on port 9200. This keeps the entire
# OAuth flow on imessage.12432.net from ChatGPT's perspective.

async def proxy_authorize(request: Request) -> Response:
    """Proxy GET /authorize to the WDWS OAuth server."""
    query_string = str(request.url.query) if request.url.query else ""
    auth_url = f"{AUTH_SERVER_URL}/authorize"
    if query_string:
        auth_url += f"?{query_string}"

    log.info("Proxying authorize: %s", auth_url)

    async with httpx.AsyncClient(timeout=30, follow_redirects=False) as client:
        resp = await client.get(
            auth_url,
            headers={
                k: v for k, v in request.headers.items()
                if k.lower() not in ("host",)
            },
        )

    response_headers = dict(resp.headers)
    for h in ("transfer-encoding", "connection", "keep-alive"):
        response_headers.pop(h, None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
    )


async def proxy_token(request: Request) -> Response:
    """Proxy POST /token to the WDWS OAuth server."""
    body = await request.body()
    log.info("Proxying token request (%d bytes)", len(body))

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{AUTH_SERVER_URL}/token",
            content=body,
            headers={
                "Content-Type": request.headers.get("content-type", "application/x-www-form-urlencoded"),
                "Accept": request.headers.get("accept", "application/json"),
            },
        )

    log.info("Token response: %d", resp.status_code)

    response_headers = dict(resp.headers)
    for h in ("transfer-encoding", "connection", "keep-alive"):
        response_headers.pop(h, None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
    )


async def proxy_register(request: Request) -> Response:
    """Proxy POST /register (dynamic client registration) to OAuth server."""
    body = await request.body()
    log.info("Proxying client registration (%d bytes)", len(body))

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{AUTH_SERVER_URL}/register",
            content=body,
            headers={
                "Content-Type": request.headers.get("content-type", "application/json"),
                "Accept": request.headers.get("accept", "application/json"),
            },
        )

    log.info("Register response: %d", resp.status_code)

    response_headers = dict(resp.headers)
    for h in ("transfer-encoding", "connection", "keep-alive"):
        response_headers.pop(h, None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
    )


async def proxy_revoke(request: Request) -> Response:
    """Proxy POST /revoke to OAuth server."""
    body = await request.body()
    log.info("Proxying token revocation")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{AUTH_SERVER_URL}/revoke",
            content=body,
            headers={
                "Content-Type": request.headers.get("content-type", "application/x-www-form-urlencoded"),
            },
        )

    response_headers = dict(resp.headers)
    for h in ("transfer-encoding", "connection", "keep-alive"):
        response_headers.pop(h, None)

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=response_headers,
    )


async def require_auth(request: Request):
    """Returns (token_info, None) or (None, JSONResponse)."""
    token = extract_bearer_token(request)
    if not token:
        return None, JSONResponse(
            {"error": "unauthorized", "message": "Bearer token required"},
            status_code=401,
            headers={"WWW-Authenticate": 'Bearer realm="imessage.12432.net"'},
        )

    token_info = await validate_token(token)
    if not token_info:
        return None, JSONResponse(
            {"error": "unauthorized", "message": "Invalid or expired token"},
            status_code=401,
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        )

    return token_info, None


async def proxy_sse(request: Request) -> StreamingResponse:
    """Proxy SSE connections to the backend Mac after auth validation.
    Handles both /sse and /mcp/sse — both map to backend /sse."""
    token_info, err = await require_auth(request)
    if err:
        return err

    log.info("SSE connection from client: %s (path: %s)",
             token_info["client_id"], request.url.path)

    # Log the connection
    try:
        p = await get_pool()
        await p.execute(
            "UPDATE ops.oauth_clients SET last_used = now() WHERE client_id = $1",
            token_info["client_id"],
        )
    except Exception as e:
        log.warning("Failed to update last_used: %s", e)

    # Stream SSE from backend
    async def stream_sse():
        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", f"{BACKEND_URL}/sse") as resp:
                async for chunk in resp.aiter_bytes():
                    yield chunk

    return StreamingResponse(
        stream_sse(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def proxy_messages(request: Request) -> Response:
    """Proxy POST /messages requests to backend after auth validation."""
    token_info, err = await require_auth(request)
    if err:
        return err

    log.info("Message from client %s: %s %s",
             token_info["client_id"], request.method, request.url.path)

    # Forward the request to backend
    body = await request.body()
    query_string = str(request.url.query) if request.url.query else ""
    backend_path = request.url.path
    backend_url = f"{BACKEND_URL}{backend_path}"
    if query_string:
        backend_url += f"?{query_string}"

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.request(
            method=request.method,
            url=backend_url,
            content=body,
            headers={
                "Content-Type": request.headers.get("content-type", "application/json"),
                "Accept": request.headers.get("accept", "*/*"),
            },
        )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )


async def proxy_mcp(request: Request) -> Response:
    """Proxy /mcp endpoint (streamable HTTP) after auth validation."""
    token_info, err = await require_auth(request)
    if err:
        return err

    log.info("MCP request from client %s: %s %s",
             token_info["client_id"], request.method, request.url.path)

    body = await request.body()
    query_string = str(request.url.query) if request.url.query else ""
    backend_url = f"{BACKEND_URL}{request.url.path}"
    if query_string:
        backend_url += f"?{query_string}"

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.request(
            method=request.method,
            url=backend_url,
            content=body,
            headers={
                "Content-Type": request.headers.get("content-type", "application/json"),
                "Accept": request.headers.get("accept", "*/*"),
            },
        )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )


async def catch_all(request: Request) -> Response:
    """Catch-all handler — proxy any other path with auth."""
    token_info, err = await require_auth(request)
    if err:
        return err

    body = await request.body()
    query_string = str(request.url.query) if request.url.query else ""
    backend_url = f"{BACKEND_URL}{request.url.path}"
    if query_string:
        backend_url += f"?{query_string}"

    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.request(
            method=request.method,
            url=backend_url,
            content=body,
            headers={
                k: v for k, v in request.headers.items()
                if k.lower() not in ("host", "authorization")
            },
        )

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )


# ── Startup / Shutdown ───────────────────────────────────────

async def startup():
    log.info("Starting iMessage Auth Proxy")
    log.info("Backend (iMessage MCP): %s", BACKEND_URL)
    log.info("Auth Server (OAuth): %s", AUTH_SERVER_URL)
    log.info("Listening on %s:%d", LISTEN_HOST, LISTEN_PORT)
    await get_pool()
    log.info("Database connected")


async def shutdown():
    global pool
    if pool:
        await pool.close()
        pool = None
    log.info("Shutdown complete")


# ── App ──────────────────────────────────────────────────────

app = Starlette(
    routes=[
        # Health
        Route("/health", health, methods=["GET"]),
        # RFC 9728 — OAuth Protected Resource Metadata
        Route("/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
        Route("/.well-known/oauth-protected-resource/{path:path}", oauth_protected_resource, methods=["GET"]),
        # RFC 8414 — OAuth Authorization Server Metadata
        Route("/.well-known/oauth-authorization-server", oauth_metadata_handler, methods=["GET"]),
        Route("/.well-known/oauth-authorization-server/{path:path}", oauth_metadata_handler, methods=["GET"]),
        Route("/.well-known/openid-configuration", oauth_metadata_handler, methods=["GET"]),
        Route("/.well-known/openid-configuration/{path:path}", oauth_metadata_handler, methods=["GET"]),
        # Also handle /{base}/.well-known/... patterns ChatGPT tries
        Route("/mcp/sse/.well-known/oauth-authorization-server", oauth_metadata_handler, methods=["GET"]),
        Route("/mcp/sse/.well-known/openid-configuration", oauth_metadata_handler, methods=["GET"]),
        Route("/mcp/sse/.well-known/oauth-protected-resource", oauth_protected_resource, methods=["GET"]),
        # OAuth endpoints — proxied to WDWS MCP server (port 9200)
        Route("/authorize", proxy_authorize, methods=["GET"]),
        Route("/token", proxy_token, methods=["POST"]),
        Route("/register", proxy_register, methods=["POST"]),
        Route("/revoke", proxy_revoke, methods=["POST"]),
        # MCP data endpoints — auth required
        Route("/mcp/sse", proxy_sse, methods=["GET"]),
        Route("/sse", proxy_sse, methods=["GET"]),
        Route("/messages", proxy_messages, methods=["POST"]),
        Route("/messages/{path:path}", proxy_messages, methods=["GET", "POST"]),
        Route("/mcp", proxy_mcp, methods=["POST"]),
        Route("/mcp/{path:path}", proxy_mcp, methods=["GET", "POST", "DELETE"]),
        Route("/{path:path}", catch_all, methods=["GET", "POST", "PUT", "DELETE", "PATCH"]),
    ],
    on_startup=[startup],
    on_shutdown=[shutdown],
)


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=LISTEN_HOST,
        port=LISTEN_PORT,
        log_level="info",
        access_log=True,
    )
