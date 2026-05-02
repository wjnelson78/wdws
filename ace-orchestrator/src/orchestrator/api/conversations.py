"""/v1/conversations CRUD."""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from orchestrator.api.deps import get_db, require_api_key
from orchestrator.schemas.conversation import (
    ConversationOut,
    ConversationPatch,
    MessageOut,
)
from orchestrator.services import session_manager

router = APIRouter(
    prefix="/v1/conversations",
    tags=["conversations"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=list[ConversationOut])
async def list_conversations(
    limit: int = 50,
    offset: int = 0,
    include_archived: bool = False,
    db: AsyncSession = Depends(get_db),
):
    return await session_manager.list_conversations(
        db, limit=limit, offset=offset, include_archived=include_archived
    )


@router.get("/{conversation_id}", response_model=ConversationOut)
async def get_conversation(conversation_id: UUID, db: AsyncSession = Depends(get_db)):
    convo = await session_manager.get_conversation(db, conversation_id)
    if convo is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    return convo


@router.get("/{conversation_id}/messages", response_model=list[MessageOut])
async def list_messages(conversation_id: UUID, db: AsyncSession = Depends(get_db)):
    convo = await session_manager.get_conversation(db, conversation_id)
    if convo is None:
        raise HTTPException(status_code=404, detail="conversation not found")
    return await session_manager.list_messages(db, conversation_id)


@router.patch("/{conversation_id}", response_model=ConversationOut)
async def patch_conversation(
    conversation_id: UUID,
    patch: ConversationPatch,
    db: AsyncSession = Depends(get_db),
):
    try:
        updated = await session_manager.patch_conversation(
            db, conversation_id, title=patch.title, archived=patch.archived
        )
        await db.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return updated


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: UUID, db: AsyncSession = Depends(get_db)
):
    await session_manager.delete_conversation(db, conversation_id)
    await db.commit()
