"""Conversation + message CRUD. No business logic."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.db.models import (
    CacheTelemetry,
    Conversation,
    Message,
    User,
)


async def get_or_create_conversation(
    session: AsyncSession,
    *,
    conversation_id: UUID | None,
    user: User | None,
    active_case_id: str | None,
    domain_module: str,
    mode: str,
    model: str,
    system_prompt_version: str,
    title: str | None = None,
) -> Conversation:
    if conversation_id is not None:
        existing = await session.get(Conversation, conversation_id)
        if existing is None:
            raise LookupError(f"conversation {conversation_id} not found")
        return existing

    convo = Conversation(
        user_id=user.id if user is not None else None,
        active_case_id=active_case_id,
        domain_module=domain_module,
        mode=mode,
        model=model,
        system_prompt_version=system_prompt_version,
        title=title,
    )
    session.add(convo)
    await session.flush()
    return convo


async def list_conversations(
    session: AsyncSession,
    *,
    user_id: UUID | None = None,
    limit: int = 50,
    offset: int = 0,
    include_archived: bool = False,
) -> list[Conversation]:
    stmt = select(Conversation)
    if user_id is not None:
        stmt = stmt.where(Conversation.user_id == user_id)
    if not include_archived:
        stmt = stmt.where(Conversation.archived.is_(False))
    stmt = stmt.order_by(Conversation.updated_at.desc()).limit(limit).offset(offset)
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def get_conversation(
    session: AsyncSession, conversation_id: UUID
) -> Conversation | None:
    return await session.get(Conversation, conversation_id)


async def list_messages(
    session: AsyncSession, conversation_id: UUID
) -> list[Message]:
    result = await session.execute(
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
    )
    return list(result.scalars().all())


async def append_message(
    session: AsyncSession,
    *,
    conversation_id: UUID,
    role: str,
    content: Any,
    input_tokens: int | None = None,
    output_tokens: int | None = None,
    cache_creation_tokens: int | None = None,
    cache_read_tokens: int | None = None,
    model: str | None = None,
) -> Message:
    msg = Message(
        conversation_id=conversation_id,
        role=role,
        content=content,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_tokens=cache_creation_tokens,
        cache_read_tokens=cache_read_tokens,
        model=model,
    )
    session.add(msg)
    await session.execute(
        update(Conversation)
        .where(Conversation.id == conversation_id)
        .values(last_message_at=text("now()"), updated_at=text("now()"))
    )
    await session.flush()
    return msg


async def patch_conversation(
    session: AsyncSession,
    conversation_id: UUID,
    *,
    title: str | None = None,
    archived: bool | None = None,
) -> Conversation:
    convo = await session.get(Conversation, conversation_id)
    if convo is None:
        raise LookupError(f"conversation {conversation_id} not found")
    if title is not None:
        convo.title = title
    if archived is not None:
        convo.archived = archived
    await session.flush()
    return convo


async def delete_conversation(
    session: AsyncSession, conversation_id: UUID
) -> None:
    convo = await session.get(Conversation, conversation_id)
    if convo is not None:
        await session.delete(convo)
        await session.flush()


async def record_cache_telemetry(
    session: AsyncSession,
    *,
    conversation_id: UUID | None,
    model: str,
    input_tokens: int,
    output_tokens: int,
    cache_creation_tokens: int = 0,
    cache_read_tokens: int = 0,
) -> None:
    row = CacheTelemetry(
        conversation_id=conversation_id,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_creation_tokens=cache_creation_tokens,
        cache_read_tokens=cache_read_tokens,
    )
    session.add(row)
    await session.flush()
