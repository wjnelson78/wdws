"""
Athena Cognitive Engine — Health Check Endpoint

FastAPI router that checks all subsystems and returns structured health status.
Mount into any existing Starlette/FastAPI app:

    from health_check import health_router
    app.mount("/health", health_router)

Or run standalone:
    uvicorn health_check:app --port 9099

Checks:
  - PostgreSQL (wdws database)
  - PostgreSQL (athena_chat database)
    - Email sync config drift / target coverage
  - Redis (if configured)
  - OpenAI API reachability
  - Anthropic API reachability (if configured)
  - Ollama (if configured)
  - Disk space
  - Memory usage
  - Service port liveness (all Athena services)
"""

import asyncio
import os
import shutil
import time
from typing import Any

import asyncpg
import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from email_sync_config import audit_sync_configuration_health

# ── Config ───────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]
CHAT_DATABASE_URL = os.environ["CHAT_DATABASE_URL"]
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
REDIS_URL = os.getenv("REDIS_URL", "")

# Service ports to check
SERVICE_PORTS = {
    "mcp-server": 9200,
    "chat-api": 9350,
    "dashboard": 9100,
    "word-mcp": 9300,
    "docx-proxy": 9301,
    "imessage-proxy": 9400,
    "investigator-mcp": 9500,
    "paperless-mcp": 9001,
}

# Thresholds
DISK_WARN_PERCENT = 80
DISK_CRIT_PERCENT = 90
MEM_WARN_PERCENT = 85
MEM_CRIT_PERCENT = 95


# ── Check Functions ──────────────────────────────────────────

async def check_postgres(dsn: str, name: str) -> dict:
    """Check PostgreSQL connectivity and basic stats."""
    start = time.time()
    try:
        conn = await asyncpg.connect(dsn, timeout=5)
        try:
            row = await conn.fetchrow("SELECT now() AS ts, pg_database_size(current_database()) AS size")
            latency = int((time.time() - start) * 1000)
            return {
                "status": "healthy",
                "latency_ms": latency,
                "database": name,
                "size_bytes": row["size"],
            }
        finally:
            await conn.close()
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "database": name,
            "latency_ms": int((time.time() - start) * 1000),
        }


async def check_redis() -> dict:
    """Check Redis connectivity."""
    if not REDIS_URL:
        return {"status": "not_configured"}

    start = time.time()
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(REDIS_URL, socket_timeout=3)
        pong = await r.ping()
        info = await r.info("server")
        await r.aclose()
        return {
            "status": "healthy" if pong else "unhealthy",
            "latency_ms": int((time.time() - start) * 1000),
            "redis_version": info.get("redis_version", "unknown"),
        }
    except ImportError:
        return {"status": "not_installed", "error": "redis package not available"}
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "latency_ms": int((time.time() - start) * 1000),
        }


async def check_openai() -> dict:
    """Check OpenAI API reachability."""
    if not OPENAI_API_KEY:
        return {"status": "not_configured"}

    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.openai.com/v1/models",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            )
            latency = int((time.time() - start) * 1000)
            if resp.status_code == 200:
                return {"status": "healthy", "latency_ms": latency}
            else:
                return {
                    "status": "degraded",
                    "latency_ms": latency,
                    "http_status": resp.status_code,
                }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "latency_ms": int((time.time() - start) * 1000),
        }


async def check_anthropic() -> dict:
    """Check Anthropic API reachability."""
    if not ANTHROPIC_API_KEY:
        return {"status": "not_configured"}

    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            # Use a lightweight endpoint to check reachability
            resp = await client.get(
                "https://api.anthropic.com/v1/models",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                },
            )
            latency = int((time.time() - start) * 1000)
            if resp.status_code in (200, 401):  # 401 = key invalid but API reachable
                return {
                    "status": "healthy" if resp.status_code == 200 else "auth_error",
                    "latency_ms": latency,
                }
            return {
                "status": "degraded",
                "latency_ms": latency,
                "http_status": resp.status_code,
            }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "latency_ms": int((time.time() - start) * 1000),
        }


async def check_ollama() -> dict:
    """Check Ollama local LLM availability."""
    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{OLLAMA_BASE_URL}/api/tags")
            latency = int((time.time() - start) * 1000)
            if resp.status_code == 200:
                models = [m["name"] for m in resp.json().get("models", [])]
                return {
                    "status": "healthy",
                    "latency_ms": latency,
                    "models": models,
                }
            return {"status": "degraded", "latency_ms": latency}
    except Exception:
        return {"status": "not_running"}


async def check_email_sync_config() -> dict:
    """Check active email sync config for drift and missing targets."""
    start = time.time()
    try:
        conn = await asyncpg.connect(DATABASE_URL, timeout=5)
        try:
            report = await audit_sync_configuration_health(conn)
        finally:
            await conn.close()

        return {
            **report,
            "latency_ms": int((time.time() - start) * 1000),
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "summary": "Email sync config health check failed.",
            "error": str(e),
            "latency_ms": int((time.time() - start) * 1000),
        }


def check_disk() -> dict:
    """Check disk space on root partition."""
    usage = shutil.disk_usage("/")
    pct_used = (usage.used / usage.total) * 100

    if pct_used >= DISK_CRIT_PERCENT:
        status = "critical"
    elif pct_used >= DISK_WARN_PERCENT:
        status = "warning"
    else:
        status = "healthy"

    return {
        "status": status,
        "total_gb": round(usage.total / (1024**3), 1),
        "used_gb": round(usage.used / (1024**3), 1),
        "free_gb": round(usage.free / (1024**3), 1),
        "percent_used": round(pct_used, 1),
    }


def check_memory() -> dict:
    """Check system memory usage."""
    try:
        with open("/proc/meminfo") as f:
            lines = f.readlines()

        info = {}
        for line in lines:
            parts = line.split(":")
            if len(parts) == 2:
                key = parts[0].strip()
                val = parts[1].strip().split()[0]  # kB value
                info[key] = int(val)

        total_kb = info.get("MemTotal", 0)
        avail_kb = info.get("MemAvailable", 0)
        used_kb = total_kb - avail_kb
        pct_used = (used_kb / total_kb * 100) if total_kb else 0

        if pct_used >= MEM_CRIT_PERCENT:
            status = "critical"
        elif pct_used >= MEM_WARN_PERCENT:
            status = "warning"
        else:
            status = "healthy"

        return {
            "status": status,
            "total_gb": round(total_kb / (1024**2), 1),
            "available_gb": round(avail_kb / (1024**2), 1),
            "percent_used": round(pct_used, 1),
        }
    except Exception as e:
        return {"status": "unknown", "error": str(e)}


async def check_service_port(name: str, port: int) -> dict:
    """Check if a service port is responsive."""
    start = time.time()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port),
            timeout=3,
        )
        writer.close()
        await writer.wait_closed()
        return {
            "status": "healthy",
            "port": port,
            "latency_ms": int((time.time() - start) * 1000),
        }
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return {"status": "down", "port": port}


# ── Health Endpoint ──────────────────────────────────────────

async def health_endpoint(request: Request) -> JSONResponse:
    """Comprehensive health check across all subsystems."""
    start = time.time()

    # Run all async checks concurrently
    (
        pg_wdws,
        pg_chat,
        email_sync_check,
        redis_check,
        openai_check,
        anthropic_check,
        ollama_check,
    ) = (
        await asyncio.gather(
            check_postgres(DATABASE_URL, "wdws"),
            check_postgres(CHAT_DATABASE_URL, "athena_chat"),
            check_email_sync_config(),
            check_redis(),
            check_openai(),
            check_anthropic(),
            check_ollama(),
        )
    )

    # Service port checks (concurrent)
    service_checks = await asyncio.gather(
        *[check_service_port(name, port) for name, port in SERVICE_PORTS.items()]
    )
    services = dict(zip(SERVICE_PORTS.keys(), service_checks))

    # Sync checks
    disk = check_disk()
    memory = check_memory()

    # Determine overall status
    checks = {
        "postgresql_wdws": pg_wdws,
        "postgresql_chat": pg_chat,
        "email_sync_config": email_sync_check,
        "redis": redis_check,
        "openai_api": openai_check,
        "anthropic_api": anthropic_check,
        "ollama": ollama_check,
        "disk": disk,
        "memory": memory,
        "services": services,
    }

    # Overall: unhealthy if any critical service is down
    critical_services = [pg_wdws, pg_chat, email_sync_check]
    if any(c.get("status") == "unhealthy" for c in critical_services):
        overall = "unhealthy"
    elif any(service.get("status") == "down" for service in services.values()):
        overall = "degraded"
    elif any(c.get("status") in ("warning", "degraded") for c in checks.values() if isinstance(c, dict)):
        overall = "degraded"
    elif disk.get("status") in ("critical", "warning"):
        overall = "degraded"
    elif memory.get("status") in ("critical", "warning"):
        overall = "degraded"
    else:
        overall = "healthy"

    total_latency = int((time.time() - start) * 1000)

    response = {
        "status": overall,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "check_latency_ms": total_latency,
        "server": "Athena Cognitive Engine",
        "checks": checks,
    }

    status_code = 200 if overall == "healthy" else (207 if overall == "degraded" else 503)
    return JSONResponse(response, status_code=status_code)


async def health_simple(request: Request) -> JSONResponse:
    """Lightweight liveness check (fast, no external calls)."""
    return JSONResponse({"status": "alive", "service": "athena"})


# ── App ──────────────────────────────────────────────────────

routes = [
    Route("/", health_endpoint),
    Route("/live", health_simple),
    Route("/ready", health_endpoint),
]

app = Starlette(routes=routes)
health_router = app

# For standalone: uvicorn health_check:app --port 9099
