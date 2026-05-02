from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ConversationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID | None = None
    title: str | None = None
    active_case_id: str | None = None
    domain_module: str
    mode: str
    model: str
    system_prompt_version: str
    created_at: datetime
    updated_at: datetime
    last_message_at: datetime | None = None
    archived: bool


class ConversationPatch(BaseModel):
    title: str | None = None
    archived: bool | None = None


class MessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    conversation_id: UUID
    role: str
    content: Any
    input_tokens: int | None = None
    output_tokens: int | None = None
    cache_creation_tokens: int | None = None
    cache_read_tokens: int | None = None
    model: str | None = None
    created_at: datetime


class ChatMessageIn(BaseModel):
    role: str
    content: Any


class ChatRequest(BaseModel):
    conversation_id: UUID | None = None
    user_identifier: str | None = None
    active_case_id: str | None = None
    domain_module: str | None = None
    mode: str = "default"
    model: str | None = None
    messages: list[ChatMessageIn] = Field(default_factory=list)
    extended_thinking: bool = False
    extended_thinking_budget_tokens: int = 10000
    preload_case: bool = False
    compaction: bool = False
    title: str | None = None
