"""Tiny client for the local BGE-M3 HTTP service (embedding_http.py on the wdws side)."""
from __future__ import annotations

from collections.abc import Sequence

import httpx

from orchestrator.config import get_settings
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)

EMBEDDING_DIMS = 1024


class EmbeddingUnavailable(RuntimeError):
    pass


class EmbeddingClient:
    def __init__(self, base_url: str | None = None, timeout: float = 30.0) -> None:
        settings = get_settings()
        self._base_url = (base_url or settings.embedding_service_url).rstrip("/")
        self._timeout = timeout

    async def embed(self, text: str) -> list[float]:
        if not text:
            raise ValueError("embed: empty text")
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.post(
                    f"{self._base_url}/embed", json={"text": text}
                )
            except httpx.HTTPError as exc:
                raise EmbeddingUnavailable(f"embedding service unreachable: {exc}") from exc
        if resp.status_code >= 400:
            raise EmbeddingUnavailable(
                f"embedding service returned {resp.status_code}: {resp.text[:200]}"
            )
        body = resp.json()
        return body["embedding"]

    async def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                resp = await client.post(
                    f"{self._base_url}/embed_batch", json={"texts": list(texts)}
                )
            except httpx.HTTPError as exc:
                raise EmbeddingUnavailable(f"embedding service unreachable: {exc}") from exc
        if resp.status_code >= 400:
            raise EmbeddingUnavailable(
                f"embedding service returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json()["embeddings"]


_client_singleton: EmbeddingClient | None = None


def get_embedding_client() -> EmbeddingClient:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = EmbeddingClient()
    return _client_singleton
