"""Build the ``mcp_servers`` array passed to Claude per request."""
from __future__ import annotations

from dataclasses import dataclass

import httpx

from orchestrator.config import get_settings
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)


@dataclass(frozen=True)
class McpServerSpec:
    name: str
    url: str


class McpRouter:
    """Holds configured MCP servers and returns per-request slices."""

    def __init__(self) -> None:
        settings = get_settings()
        servers: list[McpServerSpec] = []
        if settings.ace_mcp_url:
            servers.append(McpServerSpec(name="ace", url=settings.ace_mcp_url))
        if settings.ms365_mcp_url:
            servers.append(McpServerSpec(name="ms365", url=settings.ms365_mcp_url))
        if settings.investigator_mcp_url:
            servers.append(
                McpServerSpec(name="investigator", url=settings.investigator_mcp_url)
            )
        self._servers = tuple(servers)

    @property
    def servers(self) -> tuple[McpServerSpec, ...]:
        return self._servers

    def for_request(
        self, *, include: set[str] | None = None, exclude: set[str] | None = None
    ) -> list[dict]:
        """Return the ``mcp_servers`` list for a Claude request.

        ``include`` trumps ``exclude`` when both are supplied.
        """
        picked = self._servers
        if include is not None:
            picked = tuple(s for s in picked if s.name in include)
        elif exclude:
            picked = tuple(s for s in picked if s.name not in exclude)
        return [{"type": "url", "url": s.url, "name": s.name} for s in picked]

    async def health_check(self, *, timeout: float = 3.0) -> dict[str, bool]:
        """Probe each server with a HEAD. Warnings only; never raises."""
        results: dict[str, bool] = {}
        async with httpx.AsyncClient(timeout=timeout) as client:
            for s in self._servers:
                try:
                    resp = await client.head(s.url)
                    ok = resp.status_code < 500
                except Exception as exc:
                    ok = False
                    _log.warning("mcp_health_check_failed", server=s.name, error=str(exc))
                results[s.name] = ok
        return results


_router_singleton: McpRouter | None = None


def get_mcp_router() -> McpRouter:
    global _router_singleton
    if _router_singleton is None:
        _router_singleton = McpRouter()
    return _router_singleton
