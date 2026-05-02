"""
Athena GPU Embedding + Rerank Server (TEI-compatible)
=====================================================

A small FastAPI server that mirrors HuggingFace TEI's `/embed` and `/rerank`
routes so it is a drop-in for `HF_ENDPOINT_URL` / `HF_RERANKER_ENDPOINT_URL`
in `embedding_service.py`. Originally written to replace the saturn box at
172.16.81.113:9098 with a sturdier deployment on a 2× T4 host.

Model placement:
    cuda:0  BAAI/bge-m3                  (embedder, ~2.3 GB fp16)
    cuda:1  BAAI/bge-reranker-v2-m3      (cross-encoder reranker, ~2.2 GB fp16)

If only one GPU is visible (CUDA_VISIBLE_DEVICES=0), both models load on
cuda:0 — same behavior as the original saturn box.

Configuration (env vars, all optional except BGE_BEARER_TOKEN):
    BGE_HOST                  bind address (default 0.0.0.0)
    BGE_PORT                  port (default 9098)
    BGE_BEARER_TOKEN          required Bearer auth — must match HF_API_TOKEN
                              on the client side
    BGE_EMBED_DEVICE          cuda:0 / cuda:1 / cpu (default cuda:0 if avail)
    BGE_RERANK_DEVICE         cuda:0 / cuda:1 / cpu (default cuda:1 if avail,
                              else cuda:0, else cpu)
    BGE_MAX_SEQ_LENGTH        embedder max seq tokens (default 8192)
    BGE_EMBED_BATCH_MAX       max texts per /embed call (default 64)
    BGE_RERANK_BATCH_MAX      max texts per /rerank call (default 128)
    BGE_CONCURRENCY           in-flight request cap (default 4)
    BGE_DTYPE                 fp16 / fp32 (default fp16)

Routes (TEI-compatible):
    POST /embed     {"inputs": [str, ...]}                     -> [[float, ...], ...]
    POST /rerank    {"query": str, "texts": [str, ...],
                     "raw_scores": bool=false}                 -> [{"index": int, "score": float}, ...]
    GET  /info                                                 -> service metadata
    GET  /health                                               -> "ok"

Notes:
- Embedder normalizes vectors (cosine-ready), matching sentence-transformers
  default for BGE-M3 + the local fallback path in embedding_service.py.
- Reranker scores are sigmoid-normalized when raw_scores=false, matching TEI.
- torch.cuda.empty_cache() runs in every finally to keep VRAM stable under
  long uptime — same belt-and-braces pattern saturn used.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import torch
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("bge.server")

# ── Config ───────────────────────────────────────────────────────────
HOST = os.getenv("BGE_HOST", "0.0.0.0")
PORT = int(os.getenv("BGE_PORT", "9098"))
BEARER_TOKEN = os.getenv("BGE_BEARER_TOKEN", "").strip()

EMBED_MODEL_NAME = os.getenv("BGE_EMBED_MODEL", "BAAI/bge-m3")
RERANK_MODEL_NAME = os.getenv("BGE_RERANK_MODEL", "BAAI/bge-reranker-v2-m3")
MAX_SEQ_LENGTH = int(os.getenv("BGE_MAX_SEQ_LENGTH", "8192"))

EMBED_BATCH_MAX = int(os.getenv("BGE_EMBED_BATCH_MAX", "64"))
RERANK_BATCH_MAX = int(os.getenv("BGE_RERANK_BATCH_MAX", "128"))
CONCURRENCY = int(os.getenv("BGE_CONCURRENCY", "4"))

DTYPE_NAME = os.getenv("BGE_DTYPE", "fp16").lower()
TORCH_DTYPE = torch.float16 if DTYPE_NAME == "fp16" else torch.float32


def _pick_default_device(prefer_index: int) -> str:
    """Pick a sensible default device. prefer_index = 0 for embedder, 1 for reranker."""
    if not torch.cuda.is_available():
        return "cpu"
    n = torch.cuda.device_count()
    if n == 0:
        return "cpu"
    return f"cuda:{prefer_index}" if prefer_index < n else "cuda:0"


EMBED_DEVICE = os.getenv("BGE_EMBED_DEVICE") or _pick_default_device(0)
RERANK_DEVICE = os.getenv("BGE_RERANK_DEVICE") or _pick_default_device(1)


# ── Auth ─────────────────────────────────────────────────────────────
def require_bearer(request: Request) -> None:
    if not BEARER_TOKEN:
        return
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    if auth.split(" ", 1)[1].strip() != BEARER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid bearer token")


# ── Models ───────────────────────────────────────────────────────────
class _State:
    embedder = None
    reranker = None
    sem: Optional[asyncio.Semaphore] = None
    loaded_at: Optional[float] = None


STATE = _State()


def _load_embedder():
    from sentence_transformers import SentenceTransformer

    log.info("Loading embedder %s on %s (dtype=%s)...",
             EMBED_MODEL_NAME, EMBED_DEVICE, DTYPE_NAME)
    t0 = time.time()
    model = SentenceTransformer(
        EMBED_MODEL_NAME,
        device=EMBED_DEVICE,
        trust_remote_code=True,
    )
    model.max_seq_length = MAX_SEQ_LENGTH
    if EMBED_DEVICE.startswith("cuda") and TORCH_DTYPE == torch.float16:
        model = model.half()
    log.info("Embedder ready in %.1fs", time.time() - t0)
    return model


def _load_reranker():
    from sentence_transformers import CrossEncoder

    log.info("Loading reranker %s on %s (dtype=%s)...",
             RERANK_MODEL_NAME, RERANK_DEVICE, DTYPE_NAME)
    t0 = time.time()
    model = CrossEncoder(
        RERANK_MODEL_NAME,
        device=RERANK_DEVICE,
        trust_remote_code=True,
    )
    if RERANK_DEVICE.startswith("cuda") and TORCH_DTYPE == torch.float16:
        model.model = model.model.half()
    log.info("Reranker ready in %.1fs", time.time() - t0)
    return model


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Boot: torch=%s cuda=%s gpus=%d",
             torch.__version__,
             torch.cuda.is_available(),
             torch.cuda.device_count() if torch.cuda.is_available() else 0)
    if torch.cuda.is_available():
        for i in range(torch.cuda.device_count()):
            log.info("  GPU %d: %s", i, torch.cuda.get_device_name(i))

    STATE.embedder = _load_embedder()
    STATE.reranker = _load_reranker()
    STATE.sem = asyncio.Semaphore(CONCURRENCY)
    STATE.loaded_at = time.time()
    log.info("Server ready: embed=%s rerank=%s concurrency=%d",
             EMBED_DEVICE, RERANK_DEVICE, CONCURRENCY)
    try:
        yield
    finally:
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
        log.info("Shutdown.")


app = FastAPI(title="Athena BGE Server", lifespan=lifespan)


# ── Schemas ──────────────────────────────────────────────────────────
class EmbedRequest(BaseModel):
    inputs: list[str] = Field(..., description="Texts to embed")


class RerankRequest(BaseModel):
    query: str
    texts: list[str]
    raw_scores: bool = False


class RerankItem(BaseModel):
    index: int
    score: float


# ── Helpers ──────────────────────────────────────────────────────────
def _truncate_texts(texts: list[str]) -> list[str]:
    """Char-approx truncation to keep us safely below the model's token limit
    even for adversarial inputs. Matches the client-side behavior in
    /opt/wdws/embedding_service.py:_truncate."""
    limit = MAX_SEQ_LENGTH * 4
    return [(t or "")[:limit] for t in texts]


def _embed_blocking(texts: list[str]) -> list[list[float]]:
    try:
        with torch.inference_mode():
            vectors = STATE.embedder.encode(
                _truncate_texts(texts),
                normalize_embeddings=True,
                show_progress_bar=False,
                convert_to_numpy=True,
            )
        return vectors.tolist()
    finally:
        if EMBED_DEVICE.startswith("cuda"):
            torch.cuda.empty_cache()


def _rerank_blocking(query: str, texts: list[str], raw_scores: bool) -> list[dict]:
    try:
        pairs = [(query, t) for t in _truncate_texts(texts)]
        with torch.inference_mode():
            scores = STATE.reranker.predict(
                pairs,
                show_progress_bar=False,
                convert_to_numpy=True,
            )
        if not raw_scores:
            # Match TEI's normalization (sigmoid). bge-reranker-v2-m3 emits
            # logits; sigmoid maps them into a comparable [0,1] range.
            import numpy as np
            scores = 1.0 / (1.0 + np.exp(-scores))
        ranked = sorted(
            ((i, float(s)) for i, s in enumerate(scores)),
            key=lambda x: -x[1],
        )
        return [{"index": i, "score": s} for i, s in ranked]
    finally:
        if RERANK_DEVICE.startswith("cuda"):
            torch.cuda.empty_cache()


# ── Routes ───────────────────────────────────────────────────────────
@app.get("/health", response_class=PlainTextResponse)
async def health() -> str:
    return "ok"


@app.get("/info")
async def info(_=Depends(require_bearer)) -> dict:
    return {
        "model_id": EMBED_MODEL_NAME,
        "rerank_model_id": RERANK_MODEL_NAME,
        "embed_device": EMBED_DEVICE,
        "rerank_device": RERANK_DEVICE,
        "max_seq_length": MAX_SEQ_LENGTH,
        "max_batch_size": EMBED_BATCH_MAX,
        "max_rerank_batch": RERANK_BATCH_MAX,
        "concurrency": CONCURRENCY,
        "dtype": DTYPE_NAME,
        "loaded_at": STATE.loaded_at,
        "uptime_s": (time.time() - STATE.loaded_at) if STATE.loaded_at else 0,
        "torch_version": torch.__version__,
        "cuda": torch.cuda.is_available(),
        "gpu_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
    }


@app.post("/embed")
async def embed(req: EmbedRequest, _=Depends(require_bearer)) -> list[list[float]]:
    if not req.inputs:
        return []
    if len(req.inputs) > EMBED_BATCH_MAX:
        raise HTTPException(
            status_code=413,
            detail=f"batch too large: {len(req.inputs)} > {EMBED_BATCH_MAX}",
        )
    async with STATE.sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _embed_blocking, req.inputs)


@app.post("/rerank", response_model=list[RerankItem])
async def rerank(req: RerankRequest, _=Depends(require_bearer)) -> list[dict]:
    if not req.texts:
        return []
    if len(req.texts) > RERANK_BATCH_MAX:
        raise HTTPException(
            status_code=413,
            detail=f"batch too large: {len(req.texts)} > {RERANK_BATCH_MAX}",
        )
    async with STATE.sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, _rerank_blocking, req.query, req.texts, req.raw_scores,
        )


# ── Entry point ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "gpu_embedding_server:app",
        host=HOST,
        port=PORT,
        log_level="info",
        workers=1,
    )
