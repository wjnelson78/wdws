"""Thin wrapper around the Anthropic SDK.

Pure pass-through. No cache logic, no MCP config beyond what callers supply,
no system-prompt assembly. Streams events from the beta endpoint so we get
typed `mcp_servers` and `context_management` parameters.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from anthropic import (
    APIConnectionError,
    APIStatusError,
    AsyncAnthropic,
    InternalServerError,
    NOT_GIVEN,
    Omit,
    RateLimitError,
)

from orchestrator.config import get_settings
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)

DEFAULT_BETAS: list[str] = [
    "mcp-client-2025-04-04",
]


class ClaudeUnavailable(RuntimeError):
    """Raised when retries on 429/529/connection errors are exhausted."""


@dataclass
class ChatUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_creation_input_tokens: int = 0
    cache_read_input_tokens: int = 0
    duration_ms: int = 0


class ClaudeClient:
    """Single-process holder for an `AsyncAnthropic` instance."""

    def __init__(self, *, api_key: str | None = None) -> None:
        settings = get_settings()
        self._client = AsyncAnthropic(api_key=api_key or settings.anthropic_api_key)

    @asynccontextmanager
    async def stream_chat(
        self,
        *,
        model: str,
        system: list[dict] | str,
        messages: list[dict],
        max_tokens: int = 8192,
        mcp_servers: list[dict] | None = None,
        thinking: dict | None = None,
        betas: list[str] | None = None,
        compaction: bool = False,
        max_attempts: int = 3,
    ) -> AsyncIterator[Any]:
        """Async context manager yielding a stream object.

        The returned stream supports `async for event in stream` plus
        `await stream.get_final_message()` after iteration completes.
        """
        beta_list = list(betas) if betas is not None else list(DEFAULT_BETAS)
        if compaction and "compact-2026-01-12" not in beta_list:
            beta_list.append("compact-2026-01-12")

        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
            "betas": beta_list,
        }
        if mcp_servers:
            kwargs["mcp_servers"] = mcp_servers
        if thinking:
            kwargs["thinking"] = thinking

        attempt = 0
        last_exc: Exception | None = None
        while attempt < max_attempts:
            attempt += 1
            try:
                started = time.monotonic()
                async with self._client.beta.messages.stream(**kwargs) as stream:
                    _log.info(
                        "claude_stream_started",
                        model=model,
                        mcp_count=len(mcp_servers or []),
                        thinking_enabled=bool(thinking),
                        betas=beta_list,
                        attempt=attempt,
                    )
                    yield stream
                    duration = int((time.monotonic() - started) * 1000)
                    _log.info("claude_stream_finished", duration_ms=duration)
                return
            except (RateLimitError, InternalServerError, APIConnectionError) as exc:
                last_exc = exc
                if attempt >= max_attempts:
                    break
                backoff = 0.5 * (2 ** (attempt - 1))
                _log.warning(
                    "claude_retry",
                    attempt=attempt,
                    error=type(exc).__name__,
                    backoff_seconds=backoff,
                )
                await asyncio.sleep(backoff)
            except APIStatusError as exc:
                _log.error("claude_api_error", status=exc.status_code, error=str(exc))
                raise

        raise ClaudeUnavailable(
            f"Claude API failed after {max_attempts} attempts: {last_exc!r}"
        ) from last_exc

    @staticmethod
    def usage_from_message(message: Any) -> ChatUsage:
        """Extract usage stats from a final Message object."""
        usage = getattr(message, "usage", None)
        if usage is None:
            return ChatUsage()
        return ChatUsage(
            input_tokens=getattr(usage, "input_tokens", 0) or 0,
            output_tokens=getattr(usage, "output_tokens", 0) or 0,
            cache_creation_input_tokens=getattr(
                usage, "cache_creation_input_tokens", 0
            )
            or 0,
            cache_read_input_tokens=getattr(usage, "cache_read_input_tokens", 0) or 0,
        )


_client_singleton: ClaudeClient | None = None


def get_claude_client() -> ClaudeClient:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = ClaudeClient()
    return _client_singleton


__all__ = [
    "ChatUsage",
    "ClaudeClient",
    "ClaudeUnavailable",
    "DEFAULT_BETAS",
    "NOT_GIVEN",
    "Omit",
    "get_claude_client",
]
