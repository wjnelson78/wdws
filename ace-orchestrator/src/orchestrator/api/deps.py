"""Shared FastAPI dependencies."""
from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.config import Settings, get_settings
from orchestrator.db.session import SessionLocal
from orchestrator.services.prompt_assembler import PromptAssembler
from orchestrator.services.user_context_loader import UserContextLoader

_assembler_singleton: PromptAssembler | None = None
_user_loader_singleton: UserContextLoader | None = None


async def get_db() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


def get_prompt_assembler() -> PromptAssembler:
    global _assembler_singleton
    if _assembler_singleton is None:
        _assembler_singleton = PromptAssembler()
    return _assembler_singleton


def get_user_context_loader() -> UserContextLoader:
    global _user_loader_singleton
    if _user_loader_singleton is None:
        _user_loader_singleton = UserContextLoader()
    return _user_loader_singleton


def require_api_key(
    authorization: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or malformed Authorization header",
        )
    token = authorization.split(None, 1)[1].strip()
    if token != settings.orchestrator_api_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="invalid API key"
        )
