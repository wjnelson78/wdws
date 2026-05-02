"""FastAPI app factory."""
from __future__ import annotations

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from orchestrator.api import chat, conversations, health, memory, principals, users
from orchestrator.api.deps import get_prompt_assembler
from orchestrator.config import get_settings
from orchestrator.db.session import SessionLocal
from orchestrator.jobs.scheduler import (
    register_jobs,
    shutdown_scheduler,
    start_scheduler,
)
from orchestrator.logging_config import configure_logging, get_logger

_log = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    assembler = get_prompt_assembler()
    async with SessionLocal() as session:
        await assembler.load(session, settings.active_system_prompt_version)

    register_jobs()
    start_scheduler()

    _log.info(
        "orchestrator_started",
        version=settings.active_system_prompt_version,
        host=settings.orchestrator_host,
        port=settings.orchestrator_port,
    )
    try:
        yield
    finally:
        _log.info("orchestrator_stopping")
        shutdown_scheduler()


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = rid
        response: Response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(
        title="ACE Orchestrator",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.add_middleware(RequestIdMiddleware)
    app.include_router(health.router)
    app.include_router(users.router)
    app.include_router(principals.router)
    app.include_router(conversations.router)
    app.include_router(memory.router)
    app.include_router(chat.router)
    return app


app = create_app()
