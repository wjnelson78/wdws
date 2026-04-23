"""Tiny HTTP wrapper around embedding_service.py.

Exposes BGE-M3 embeddings and bge-reranker-v2-m3 cross-encoder scoring to
other services on the box (notably the ACE Orchestrator and the MCP server)
without forcing each consumer to load its own model copy.

Routes:
    GET  /healthz                                       → liveness
    POST /embed        {"text": str}                    → {"embedding": [...], "dims": 1024}
    POST /embed_batch  {"texts": [str]}                 → {"embeddings": [[...]], "dims": 1024}
    POST /rerank       {"query": str, "passages": [str], "top_k": int?}
                                                        → {"results": [{"index", "score"}]}

Run:
    /opt/wdws/venv/bin/python /opt/wdws/embedding_http.py
or via systemd unit wdws-embedding-http.service.
"""
from __future__ import annotations

import os
import sys

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

sys.path.insert(0, "/opt/wdws")
from embedding_service import (  # type: ignore[import-untyped]
    EMBEDDING_DIMENSIONS,
    EMBEDDING_MODEL_NAME,
    HF_RERANKER_ENABLED,
    RERANKER_MODEL_NAME,
    embed_query,
    embed_texts,
    rerank,
)

app = FastAPI(title="Athena Embedding Service", version="1.1.0")


class EmbedIn(BaseModel):
    text: str = Field(..., min_length=1)


class EmbedBatchIn(BaseModel):
    texts: list[str] = Field(..., min_length=1, max_length=128)


class RerankIn(BaseModel):
    query: str = Field(..., min_length=1)
    passages: list[str] = Field(..., min_length=1, max_length=200)
    top_k: int | None = Field(default=None, ge=1, le=200)


@app.get("/healthz")
async def healthz() -> dict:
    return {
        "status": "ok",
        "embed_model": EMBEDDING_MODEL_NAME,
        "embed_dims": EMBEDDING_DIMENSIONS,
        "rerank_model": RERANKER_MODEL_NAME,
        "rerank_hf_endpoint_enabled": HF_RERANKER_ENABLED,
    }


@app.post("/embed")
async def embed(req: EmbedIn) -> dict:
    try:
        vec = await embed_query(req.text)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"embed failed: {exc}") from exc
    return {"embedding": vec, "dims": EMBEDDING_DIMENSIONS}


@app.post("/embed_batch")
async def embed_batch(req: EmbedBatchIn) -> dict:
    if any(not t for t in req.texts):
        raise HTTPException(status_code=400, detail="empty string in texts")
    try:
        vecs = await embed_texts(req.texts)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"embed_batch failed: {exc}") from exc
    return {"embeddings": vecs, "dims": EMBEDDING_DIMENSIONS}


@app.post("/rerank")
async def rerank_route(req: RerankIn) -> dict:
    if any(not p for p in req.passages):
        raise HTTPException(status_code=400, detail="empty string in passages")
    try:
        ranked = await rerank(req.query, req.passages, top_k=req.top_k)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"rerank failed: {exc}") from exc
    return {
        "results": [{"index": i, "score": s} for i, s in ranked],
        "model": RERANKER_MODEL_NAME,
    }


if __name__ == "__main__":
    port = int(os.getenv("EMBEDDING_HTTP_PORT", "9098"))
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
