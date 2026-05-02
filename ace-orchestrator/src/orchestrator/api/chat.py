"""POST /v1/chat/completions — the orchestrator's main entry point.

Pulls everything together:
  user_context_loader → prompt_assembler → cache_strategist → mcp_router → claude_client
  → session_manager (persist messages + telemetry).

Streams events back to the caller as Server-Sent Events.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sse_starlette.sse import EventSourceResponse

from orchestrator.api.deps import (
    get_db,
    get_prompt_assembler,
    get_user_context_loader,
    require_api_key,
)
from orchestrator.config import get_settings
from orchestrator.logging_config import get_logger
from orchestrator.schemas.conversation import ChatRequest
from orchestrator.services import session_manager
from orchestrator.services.cache_strategist import (
    cache_breakpoint_summary,
    to_anthropic_system,
)
from orchestrator.services.claude_client import (
    ChatUsage,
    ClaudeUnavailable,
    get_claude_client,
)
from orchestrator.services.mcp_router import get_mcp_router
from orchestrator.services.memory_system.encoder import EncoderInput, maybe_encode_turn
from orchestrator.services.memory_system.morning_brief import maybe_pop_brief
from orchestrator.services.memory_system.retriever import get_memory_retriever
from orchestrator.services.prompt_assembler import (
    DOMAIN_TO_BLOCK,
    PromptAssembler,
    default_runtime_context,
    detect_domain,
)
from orchestrator.services.time_signals import evaluate_time_signals
from orchestrator.services.user_context_loader import UserContextLoader

_log = get_logger(__name__)

router = APIRouter(
    prefix="/v1/chat",
    tags=["chat"],
    dependencies=[Depends(require_api_key)],
)


def _resolve_model(req: ChatRequest) -> str:
    settings = get_settings()
    if req.model:
        return req.model
    if req.mode == "drafting":
        return settings.anthropic_drafting_model
    return settings.anthropic_default_model


def _resolve_domain(req: ChatRequest) -> Literal["legal", "comms", "medical", "osint", "dochandling"]:
    if req.domain_module and req.domain_module in DOMAIN_TO_BLOCK:
        return req.domain_module  # type: ignore[return-value]
    last_user = next(
        (m.content for m in reversed(req.messages) if m.role == "user"), ""
    )
    text = last_user if isinstance(last_user, str) else json.dumps(last_user)
    return detect_domain(active_case_id=req.active_case_id, message_text=text)


def _resolve_user_identifier(req: ChatRequest) -> str:
    return req.user_identifier or get_settings().default_user_identifier


def _messages_to_anthropic(messages: list[Any]) -> list[dict]:
    out = []
    for m in messages:
        content = m.content if isinstance(m, dict) else m.model_dump()["content"]
        role = m.role if isinstance(m, dict) else m.role  # type: ignore[union-attr]
        if isinstance(content, str):
            out.append({"role": role, "content": content})
        else:
            out.append({"role": role, "content": content})
    return out


@router.post("/completions")
async def chat_completions(
    req: ChatRequest,
    db: AsyncSession = Depends(get_db),
    assembler: PromptAssembler = Depends(get_prompt_assembler),
    loader: UserContextLoader = Depends(get_user_context_loader),
):
    settings = get_settings()
    user_identifier = _resolve_user_identifier(req)
    try:
        rendered = await loader.load(db, user_identifier)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    domain = _resolve_domain(req)
    model = _resolve_model(req)

    # Make sure the assembler's in-memory blocks are loaded.
    if assembler._version is None:  # pragma: no cover  (defensive)
        await assembler.load(db, settings.active_system_prompt_version)

    convo = await session_manager.get_or_create_conversation(
        db,
        conversation_id=req.conversation_id,
        user=rendered.raw_user,
        active_case_id=req.active_case_id,
        domain_module=domain,
        mode=req.mode,
        model=model,
        system_prompt_version=assembler.version,
        title=req.title,
    )
    await db.commit()  # commit conversation row before streaming

    if not req.messages:
        raise HTTPException(status_code=400, detail="messages must be non-empty")

    # Persist user message(s) BEFORE streaming so they survive disconnect.
    for m in req.messages:
        if m.role == "user":
            await session_manager.append_message(
                db,
                conversation_id=convo.id,
                role="user",
                content=m.content,
            )
    await db.commit()

    history = await session_manager.list_messages(db, convo.id)
    history_messages = [
        {"role": h.role, "content": h.content}
        for h in history
        if h.role in ("user", "assistant")
    ]

    runtime_ctx = default_runtime_context(datetime.now())
    if req.active_case_id:
        runtime_ctx["ACTIVE_CASE_NUMBER"] = req.active_case_id

    # Patch 02 §E: time signals are dormant unless the principal has opted in.
    if not rendered.is_onboarding:
        ts = evaluate_time_signals(rendered.raw_user)
        for k, v in ts.as_runtime_lines():
            runtime_ctx[k] = v

    # Phase 2 §8.7: morning brief — surface unread significant dream hypotheses.
    morning = await maybe_pop_brief(
        user_id=rendered.user_id,
        user_timezone=rendered.timezone,
        is_onboarding=rendered.is_onboarding,
    )
    if morning is not None:
        runtime_ctx["MORNING_BRIEF"] = morning.text

    # Build MEMORY_CONTEXT from the most-recent user message.
    last_user_message_text = next(
        (
            (m.content if isinstance(m.content, str) else json.dumps(m.content))
            for m in reversed(req.messages)
            if m.role == "user"
        ),
        "",
    )
    retriever = get_memory_retriever()
    memory_result, voice_block = await asyncio.gather(
        retriever.build_memory_context(
            user_id=rendered.user_id,
            user_message=last_user_message_text,
            active_case_id=req.active_case_id,
        ),
        retriever.build_voice_context(user_id=rendered.user_id),
    )

    blocks = assembler.assemble(
        domain=domain,
        role=rendered.role,
        user_profile_block=rendered.user_profile_block,
        active_case_context_block=rendered.active_case_context_block,
        case_file_block=None,
        runtime_context=runtime_ctx,
        substitutions={
            "USER_NAME": rendered.display_name,
            "USER_PREFERRED_NAME": rendered.preferred_name or rendered.display_name,
            "CURRENT_DATE": runtime_ctx["CURRENT_DATE"],
            "CURRENT_DATETIME": runtime_ctx["CURRENT_DATETIME"],
            "ACTIVE_CASE_NUMBER": req.active_case_id or "",
        },
        user_cache_key=f"{rendered.user_id}:{rendered.content_hash}",
        memory_context_block=memory_result.block or None,
        voice_context_block=voice_block or None,
        is_onboarding=rendered.is_onboarding,
    )
    system = to_anthropic_system(blocks, mode=req.mode)
    cache_summary = cache_breakpoint_summary(blocks)

    mcp_servers = get_mcp_router().for_request()

    thinking = None
    if req.extended_thinking:
        thinking = {
            "type": "enabled",
            "budget_tokens": req.extended_thinking_budget_tokens,
        }

    _log.info(
        "chat_request",
        conversation_id=str(convo.id),
        user=user_identifier,
        domain=domain,
        mode=req.mode,
        model=model,
        history_messages=len(history_messages),
        cache_summary=cache_summary,
        mcp_servers=[s["name"] for s in mcp_servers],
        thinking=bool(thinking),
    )

    async def event_stream() -> AsyncIterator[dict]:
        client = get_claude_client()
        usage = ChatUsage()
        assembled_text_blocks: list[str] = []
        full_message: Any = None

        yield {"event": "orchestrator_start", "data": json.dumps({
            "conversation_id": str(convo.id),
            "user": user_identifier,
            "model": model,
            "domain": domain,
            "mode": req.mode,
            "cache_summary": cache_summary,
        })}

        try:
            async with client.stream_chat(
                model=model,
                system=system,
                messages=history_messages,
                max_tokens=8192 if req.mode != "drafting" else 16384,
                mcp_servers=mcp_servers,
                thinking=thinking,
                compaction=req.compaction,
            ) as stream:
                async for event in stream:
                    payload = _event_to_payload(event)
                    if payload is not None:
                        yield payload
                full_message = await stream.get_final_message()
                usage = client.usage_from_message(full_message)
                if full_message and full_message.content:
                    for block in full_message.content:
                        if getattr(block, "type", None) == "text":
                            assembled_text_blocks.append(block.text)
        except ClaudeUnavailable as exc:
            yield {
                "event": "error",
                "data": json.dumps({"error": "claude_unavailable", "detail": str(exc)}),
            }
            return
        except Exception as exc:
            _log.exception("chat_stream_error", error=str(exc))
            yield {
                "event": "error",
                "data": json.dumps({"error": "stream_error", "detail": str(exc)}),
            }
            return

        # Persist assistant response and telemetry.
        assistant_message_id = None
        try:
            assistant_content = (
                [b.model_dump() for b in full_message.content]
                if full_message and full_message.content
                else "".join(assembled_text_blocks)
            )
            assistant_msg = await session_manager.append_message(
                db,
                conversation_id=convo.id,
                role="assistant",
                content=assistant_content,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_creation_tokens=usage.cache_creation_input_tokens,
                cache_read_tokens=usage.cache_read_input_tokens,
                model=model,
            )
            assistant_message_id = assistant_msg.id
            await session_manager.record_cache_telemetry(
                db,
                conversation_id=convo.id,
                model=model,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_creation_tokens=usage.cache_creation_input_tokens,
                cache_read_tokens=usage.cache_read_input_tokens,
            )
            await db.commit()
        except Exception as exc:  # pragma: no cover
            _log.exception("chat_persist_error", error=str(exc))
            await db.rollback()

        # Fire encoder as a detached background task — must NOT block the response,
        # must NOT raise back to this handler.
        try:
            assistant_text = "".join(assembled_text_blocks) or (
                full_message.content[0].text
                if full_message and full_message.content and getattr(full_message.content[0], "type", None) == "text"
                else ""
            )
            encoder_payload = EncoderInput(
                user_id=rendered.user_id,
                user_identifier=rendered.identifier,
                conversation_id=convo.id,
                message_id=assistant_message_id,
                user_message=last_user_message_text,
                assistant_response=assistant_text,
                domain=domain,
                active_case_id=req.active_case_id,
                categories=[domain],
                tool_invocations=None,
            )
            task = asyncio.create_task(maybe_encode_turn(encoder_payload))
            task.add_done_callback(_swallow_encoder_exceptions)
            retriever.invalidate_user(rendered.user_id)
        except Exception as exc:  # pragma: no cover
            _log.exception("encoder_dispatch_failed", error=str(exc))

        yield {
            "event": "orchestrator_done",
            "data": json.dumps(
                {
                    "conversation_id": str(convo.id),
                    "usage": {
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                        "cache_read_input_tokens": usage.cache_read_input_tokens,
                    },
                }
            ),
        }

    return EventSourceResponse(event_stream())


def _swallow_encoder_exceptions(task: asyncio.Task) -> None:
    """Detached encoder tasks must never bubble their failure back to the loop."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _log.warning("encoder_task_exception", error=type(exc).__name__, detail=str(exc)[:200])


def _event_to_payload(event: Any) -> dict | None:
    """Serialize SDK events into SSE payloads."""
    et = getattr(event, "type", None)
    if et is None:
        return None
    try:
        body = event.model_dump(mode="json", exclude_none=True)
    except AttributeError:
        body = {"type": et}
    return {"event": et, "data": json.dumps(body, default=str)}
