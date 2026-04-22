"""
Athena Cognitive Engine — Embedding Service (BGE-M3)
====================================================
Produces 1024-dim dense vectors via BGE-M3. Two inference paths:

1. HuggingFace Inference Endpoint (GPU, TEI) — primary when HF_ENDPOINT_URL
   + HF_API_TOKEN are set. ~10-20x faster than CPU for bulk work.
2. Local sentence-transformers on CPU — always available as fallback for
   cold starts, network blips, or HF outages.

Both paths return the same 1024-dim unit-normalized vectors, so vectors
from either path are interchangeable in pgvector search.

Usage:
    from embedding_service import embed_texts, embed_query

    vector = await embed_query("employment discrimination")
    vectors = await embed_texts(["chunk 1 text", "chunk 2 text", ...])
"""
import asyncio
import logging
import os
import time
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/wdws/.env")
except ImportError:
    pass

log = logging.getLogger("athena.embeddings")

# ── Configuration ────────────────────────────────────────────
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")
EMBEDDING_DIMENSIONS = 1024   # BGE-M3 output dimensions
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "64"))
EMBEDDING_MAX_LENGTH = int(os.getenv("EMBEDDING_MAX_SEQ_LENGTH", "8192"))

# HuggingFace Inference Endpoint (optional — falls back to local CPU if unset)
HF_ENDPOINT_URL = os.getenv("HF_ENDPOINT_URL", "").rstrip("/")
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")
HF_TIMEOUT = float(os.getenv("HF_EMBEDDING_TIMEOUT", "90"))  # long enough for cold-start wake
HF_BATCH_SIZE = int(os.getenv("HF_EMBEDDING_BATCH_SIZE", "32"))  # TEI default max_client_batch_size
HF_ENABLED = bool(HF_ENDPOINT_URL and HF_API_TOKEN)

if HF_ENABLED:
    log.info("HF Inference Endpoint enabled (GPU path primary, CPU fallback): %s",
             HF_ENDPOINT_URL)
else:
    log.info("HF Inference Endpoint disabled (CPU-only) — set HF_ENDPOINT_URL + HF_API_TOKEN to enable")

# Legacy compat — these are used by search endpoints
EMBEDDING_MODEL = EMBEDDING_MODEL_NAME
EMBEDDING_DIMS = EMBEDDING_DIMENSIONS

# ── Singleton Model ──────────────────────────────────────────
_model = None
_model_lock = asyncio.Lock() if hasattr(asyncio, 'Lock') else None


def _load_model():
    """Load BGE-M3 model (CPU, ~2.3 GB RAM).

    Uses local cache only when HF_HUB_OFFLINE / TRANSFORMERS_OFFLINE are set
    (the normal production path) — skips all HuggingFace network round-trips.
    """
    global _model
    if _model is not None:
        return _model

    from sentence_transformers import SentenceTransformer
    # Respect offline env vars set by the systemd service unit so the model
    # loads directly from the on-disk cache with zero network latency.
    offline = os.getenv("HF_HUB_OFFLINE", "0") == "1" or \
              os.getenv("TRANSFORMERS_OFFLINE", "0") == "1"
    log.info("Loading BGE-M3 embedding model (local_files_only=%s)...", offline)
    t0 = time.time()
    _model = SentenceTransformer(
        EMBEDDING_MODEL_NAME,
        device="cpu",
        trust_remote_code=True,
        local_files_only=offline,
    )
    _model.max_seq_length = EMBEDDING_MAX_LENGTH
    log.info("BGE-M3 loaded in %.1fs (dims=%d, max_seq=%d)",
             time.time() - t0, EMBEDDING_DIMENSIONS, EMBEDDING_MAX_LENGTH)
    return _model


def get_embedder():
    """Get the singleton embedding model instance."""
    return _load_model()


def _vec_literal(vec: list[float]) -> str:
    """Convert embedding list to pgvector halfvec literal string."""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def _truncate(texts: list[str]) -> list[str]:
    """Truncate texts to max sequence length (char-approx, not tokens)."""
    limit = EMBEDDING_MAX_LENGTH * 4
    return [t[:limit] if t else "" for t in texts]


# ── HuggingFace Inference Endpoint (GPU path) ────────────────

_hf_sync_client = None
_hf_async_client = None


def _get_hf_sync_client():
    global _hf_sync_client
    if _hf_sync_client is None:
        import httpx
        _hf_sync_client = httpx.Client(
            timeout=HF_TIMEOUT,
            headers={
                "Authorization": f"Bearer {HF_API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
    return _hf_sync_client


def _get_hf_async_client():
    global _hf_async_client
    if _hf_async_client is None:
        import httpx
        _hf_async_client = httpx.AsyncClient(
            timeout=HF_TIMEOUT,
            headers={
                "Authorization": f"Bearer {HF_API_TOKEN}",
                "Content-Type": "application/json",
            },
        )
    return _hf_async_client


def _embed_hf_sync(texts: list[str]) -> list[list[float]]:
    """Call HF Inference Endpoint /embed. Caller handles batching."""
    client = _get_hf_sync_client()
    r = client.post(
        f"{HF_ENDPOINT_URL}/embed",
        json={"inputs": _truncate(texts)},
    )
    r.raise_for_status()
    return r.json()


async def _embed_hf_async(texts: list[str]) -> list[list[float]]:
    """Async variant of _embed_hf_sync."""
    client = _get_hf_async_client()
    r = await client.post(
        f"{HF_ENDPOINT_URL}/embed",
        json={"inputs": _truncate(texts)},
    )
    r.raise_for_status()
    return r.json()


# ── Local CPU path (fallback) ────────────────────────────────

def _embed_cpu_sync(texts: list[str]) -> list[list[float]]:
    """Embed via local sentence-transformers on CPU. Caller handles batching."""
    model = _load_model()
    embeddings = model.encode(
        _truncate(texts),
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return embeddings.tolist()


# ── Sync Public API ──────────────────────────────────────────

def embed_texts_sync(texts: list[str], batch_size: int = None) -> list[list[float]]:
    """Embed a list of texts synchronously. Returns 1024-dim vectors.

    Tries HF Inference Endpoint first (if configured), falls back to local CPU
    per-batch on any HF failure. Batch size differs between paths because GPU
    handles larger batches efficiently while CPU prefers smaller.
    """
    if not texts:
        return []

    bs = batch_size or (HF_BATCH_SIZE if HF_ENABLED else EMBEDDING_BATCH_SIZE)
    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), bs):
        batch = texts[i:i + bs]
        vecs = None

        if HF_ENABLED:
            try:
                vecs = _embed_hf_sync(batch)
            except Exception as e:
                log.warning(
                    "HF endpoint failed for batch %d (%s: %s) — falling back to local CPU",
                    i // bs, type(e).__name__, e,
                )

        if vecs is None:
            vecs = _embed_cpu_sync(batch)

        all_embeddings.extend(vecs)

    return all_embeddings


def embed_query_sync(text: str) -> list[float]:
    """Embed a single query text synchronously. Returns 1024-dim vector."""
    if HF_ENABLED:
        try:
            return _embed_hf_sync([text])[0]
        except Exception as e:
            log.warning(
                "HF endpoint failed for query (%s: %s) — falling back to local CPU",
                type(e).__name__, e,
            )
    return _embed_cpu_sync([text])[0]


# ── Async Public API ─────────────────────────────────────────

async def embed_texts(texts: list[str], batch_size: int = None) -> list[list[float]]:
    """Embed a list of texts asynchronously. Returns 1024-dim vectors.

    Uses true async HTTP for HF path (doesn't block event loop). CPU fallback
    runs in the default thread pool executor.
    """
    if not texts:
        return []

    bs = batch_size or (HF_BATCH_SIZE if HF_ENABLED else EMBEDDING_BATCH_SIZE)
    all_embeddings: list[list[float]] = []
    loop = asyncio.get_event_loop()

    for i in range(0, len(texts), bs):
        batch = texts[i:i + bs]
        vecs = None

        if HF_ENABLED:
            try:
                vecs = await _embed_hf_async(batch)
            except Exception as e:
                log.warning(
                    "HF endpoint failed for async batch %d (%s: %s) — falling back to local CPU",
                    i // bs, type(e).__name__, e,
                )

        if vecs is None:
            vecs = await loop.run_in_executor(None, _embed_cpu_sync, batch)

        all_embeddings.extend(vecs)

    return all_embeddings


async def embed_query(text: str) -> list[float]:
    """Embed a single query text asynchronously. Returns 1024-dim vector."""
    if HF_ENABLED:
        try:
            vecs = await _embed_hf_async([text])
            return vecs[0]
        except Exception as e:
            log.warning(
                "HF endpoint failed for async query (%s: %s) — falling back to local CPU",
                type(e).__name__, e,
            )
    loop = asyncio.get_event_loop()
    vecs = await loop.run_in_executor(None, _embed_cpu_sync, [text])
    return vecs[0]


# ── Batch Embedding with Progress ────────────────────────────

async def embed_texts_with_progress(
    texts: list[str],
    batch_size: int = None,
    callback=None,
) -> list[list[float]]:
    """Embed texts in batches with optional progress callback.

    Args:
        texts: List of text strings to embed
        batch_size: Override default batch size
        callback: async fn(batch_num, total_batches, elapsed_secs)
    """
    if not texts:
        return []

    bs = batch_size or EMBEDDING_BATCH_SIZE
    total_batches = (len(texts) + bs - 1) // bs
    all_embeddings = []
    t0 = time.time()

    for batch_num in range(total_batches):
        start = batch_num * bs
        batch = texts[start:start + bs]
        batch_vecs = await embed_texts(batch, batch_size=len(batch))
        all_embeddings.extend(batch_vecs)

        if callback:
            await callback(batch_num + 1, total_batches, time.time() - t0)

    return all_embeddings


# ── CLI Test ─────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    print("BGE-M3 Embedding Service — Path Comparison")
    print("=" * 60)
    print(f"HF_ENABLED: {HF_ENABLED}")
    if HF_ENABLED:
        print(f"HF endpoint: {HF_ENDPOINT_URL}")

    test_texts = [
        "Employment discrimination claim under WLAD RCW 49.60",
        "Motion to dismiss for failure to state a claim",
        "Medical records showing traumatic brain injury diagnosis",
        "Deposition transcript of plaintiff William Nelson",
        "Insurance bad faith denial of UIM coverage",
    ]

    # Path 1: public API (HF if enabled, else CPU)
    print(f"\n── Public API (HF if enabled) — {len(test_texts)} texts ──")
    t0 = time.time()
    vecs_hf = embed_texts_sync(test_texts)
    t_hf = time.time() - t0
    print(f"  Time: {t_hf:.2f}s ({t_hf/len(test_texts)*1000:.0f}ms per text)")
    print(f"  Dims: {len(vecs_hf[0])}  norm: {sum(x*x for x in vecs_hf[0]):.4f}")

    # Path 2: CPU-only for comparison
    print(f"\n── Local CPU (fallback path) — {len(test_texts)} texts ──")
    t0 = time.time()
    vecs_cpu = _embed_cpu_sync(test_texts)
    t_cpu = time.time() - t0
    print(f"  Time: {t_cpu:.2f}s ({t_cpu/len(test_texts)*1000:.0f}ms per text)")
    print(f"  Dims: {len(vecs_cpu[0])}  norm: {sum(x*x for x in vecs_cpu[0]):.4f}")

    # Agreement check — vectors should be nearly identical (same model, same inputs)
    import numpy as np
    a = np.array(vecs_hf)
    b = np.array(vecs_cpu)
    per_text_cos = (a * b).sum(axis=1) / (np.linalg.norm(a, axis=1) * np.linalg.norm(b, axis=1))
    print(f"\n── Agreement (cosine sim between HF and CPU vectors per text) ──")
    for i, s in enumerate(per_text_cos):
        print(f"  [{i}] {s:.6f}  {test_texts[i][:50]}")

    if HF_ENABLED and t_cpu > 0:
        print(f"\nSpeedup (CPU / public API): {t_cpu / t_hf:.1f}x")
