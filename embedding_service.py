"""
Athena Cognitive Engine — Local Embedding Service (BGE-M3)
==========================================================
Replaces OpenAI text-embedding-3-large with local BGE-M3 model.

BGE-M3 produces 1024-dimensional dense vectors on CPU with no API dependency.
Used by all ingest pipelines, search endpoints, and agents.

Usage:
    from embedding_service import get_embedder, embed_texts, embed_query

    # Single query (for search)
    vector = await embed_query("employment discrimination")

    # Batch (for ingestion)
    vectors = await embed_texts(["chunk 1 text", "chunk 2 text", ...])
"""
import asyncio
import logging
import os
import time
from typing import Optional

log = logging.getLogger("athena.embeddings")

# ── Configuration ────────────────────────────────────────────
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-m3")
EMBEDDING_DIMENSIONS = 1024   # BGE-M3 output dimensions
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", "64"))
EMBEDDING_MAX_LENGTH = int(os.getenv("EMBEDDING_MAX_SEQ_LENGTH", "8192"))

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


# ── Sync Embedding Functions ─────────────────────────────────

def embed_texts_sync(texts: list[str], batch_size: int = None) -> list[list[float]]:
    """Embed a list of texts synchronously. Returns list of 1024-dim vectors.

    Handles batching internally for memory efficiency on CPU.
    """
    if not texts:
        return []

    model = _load_model()
    bs = batch_size or EMBEDDING_BATCH_SIZE

    all_embeddings = []
    for i in range(0, len(texts), bs):
        batch = texts[i:i + bs]
        # Truncate long texts to max sequence length (chars, not tokens)
        batch = [t[:EMBEDDING_MAX_LENGTH * 4] if t else "" for t in batch]
        embeddings = model.encode(
            batch,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        all_embeddings.extend(embeddings.tolist())

    return all_embeddings


def embed_query_sync(text: str) -> list[float]:
    """Embed a single query text synchronously. Returns 1024-dim vector."""
    model = _load_model()
    truncated = text[:EMBEDDING_MAX_LENGTH * 4] if text else ""
    embedding = model.encode(
        [truncated],
        normalize_embeddings=True,
        show_progress_bar=False,
    )
    return embedding[0].tolist()


# ── Async Embedding Functions ────────────────────────────────

async def embed_texts(texts: list[str], batch_size: int = None) -> list[list[float]]:
    """Embed a list of texts asynchronously (runs model in thread pool).

    Returns list of 1024-dimensional normalized vectors.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, embed_texts_sync, texts, batch_size
    )


async def embed_query(text: str) -> list[float]:
    """Embed a single query text asynchronously. Returns 1024-dim vector."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, embed_query_sync, text)


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
    import sys

    print("BGE-M3 Embedding Service — Quick Test")
    print("=" * 50)

    model = _load_model()

    test_texts = [
        "Employment discrimination claim under WLAD RCW 49.60",
        "Motion to dismiss for failure to state a claim",
        "Medical records showing traumatic brain injury diagnosis",
        "Deposition transcript of plaintiff William Nelson",
        "Insurance bad faith denial of UIM coverage",
    ]

    print(f"\nEmbedding {len(test_texts)} texts...")
    t0 = time.time()
    vecs = embed_texts_sync(test_texts)
    elapsed = time.time() - t0

    print(f"Time: {elapsed:.2f}s ({elapsed/len(test_texts)*1000:.0f}ms per text)")
    print(f"Dimensions: {len(vecs[0])}")

    # Similarity test
    import numpy as np
    vecs_np = np.array(vecs)
    sims = vecs_np @ vecs_np.T

    print(f"\nSimilarity matrix (cosine):")
    for i, t in enumerate(test_texts):
        print(f"  [{i}] {t[:50]}...")
    print()
    for i in range(len(test_texts)):
        row = " ".join(f"{sims[i][j]:.2f}" for j in range(len(test_texts)))
        print(f"  [{i}] {row}")
