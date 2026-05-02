"""
Athena Cognitive Engine — Embedding + Reranking Service
========================================================
Produces 1024-dim dense vectors via BGE-M3 and cross-encoder rerank scores
via bge-reranker-v2-m3. Two inference paths for each, with matching
fallback semantics:

Embeddings (BGE-M3, 1024 dims):
  1. HuggingFace Inference Endpoint (HF_ENDPOINT_URL + HF_API_TOKEN) —
     primary when configured; ~10-20x faster than CPU for bulk work.
  2. Local sentence-transformers on CPU — always-available fallback.

Reranker (bge-reranker-v2-m3, cross-encoder):
  1. HF TEI rerank endpoint (HF_RERANKER_ENDPOINT_URL + HF_API_TOKEN) —
     primary when configured. TEI's `/rerank` route takes
     {"query": str, "texts": [str]} and returns [{index, score}, ...].
  2. Local CrossEncoder on CPU — fallback. ~50ms per query/passage pair
     on CPU, so 50 candidates ≈ 2.5s; fine for interactive search.

Both reranker paths return the same (index, score) pairs sorted by score
descending, so callers can swap transparently.

Usage:
    from embedding_service import embed_query, embed_texts, rerank

    vec = await embed_query("employment discrimination")
    vecs = await embed_texts(["chunk 1", "chunk 2", ...])

    # After dense retrieval returns top-50 candidates:
    ranked = await rerank(query, [c["text"] for c in candidates], top_k=10)
    top = [candidates[i] for i, score in ranked]
"""
import asyncio
import itertools
import logging
import os
import threading
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

# ── Embed endpoint pool ──────────────────────────────────────
# Primary + up to two extras loaded from env. All healthy endpoints receive
# batches round-robin; any endpoint that errors is skipped for that batch
# (tried in round-robin order until one succeeds, then CPU fallback).
# To add a second GPU worker (e.g. cuda:1 on 172.16.81.187:9099), set
# HF_ENDPOINT_URL_2 in .env — no code change needed.
HF_API_TOKEN = os.getenv("HF_API_TOKEN", "")
HF_TIMEOUT = float(os.getenv("HF_EMBEDDING_TIMEOUT", "90"))
HF_BATCH_SIZE = int(os.getenv("HF_EMBEDDING_BATCH_SIZE", "32"))

def _build_endpoint_pool(prefix: str) -> list[str]:
    """Collect HF_<prefix>_URL, HF_<prefix>_URL_FALLBACK, HF_<prefix>_URL_2, ..."""
    urls = []
    for suffix in ("URL", "URL_FALLBACK", "URL_2", "URL_3"):
        v = os.getenv(f"HF_{prefix}_{suffix}", "").rstrip("/")
        if v:
            urls.append(v)
    return urls

_EMBED_POOL: list[str] = _build_endpoint_pool("ENDPOINT")
HF_ENDPOINT_URL = _EMBED_POOL[0] if _EMBED_POOL else ""
HF_ENDPOINT_URL_FALLBACK = _EMBED_POOL[1] if len(_EMBED_POOL) > 1 else ""
HF_ENABLED = bool(_EMBED_POOL and HF_API_TOKEN)
HF_FALLBACK_ENABLED = len(_EMBED_POOL) > 1 and bool(HF_API_TOKEN)

# Thread-safe round-robin counter for the embed pool
_embed_pool_lock = threading.Lock()
_embed_pool_cycle = itertools.cycle(range(max(len(_EMBED_POOL), 1)))
_embed_pool_idx: int = 0

def _next_embed_url() -> str:
    """Return the next endpoint URL in round-robin order."""
    global _embed_pool_idx
    with _embed_pool_lock:
        _embed_pool_idx = next(_embed_pool_cycle)
        return _EMBED_POOL[_embed_pool_idx % len(_EMBED_POOL)]

if os.getenv("EMBEDDING_FORCE_LOCAL", "0") == "1":
    HF_ENABLED = False
    HF_FALLBACK_ENABLED = False
    log.warning("EMBEDDING_FORCE_LOCAL=1 — using local CPU for all embeddings")

# Reranker pool — same structure, separate env prefix
RERANKER_MODEL_NAME = os.getenv("RERANKER_MODEL", "BAAI/bge-reranker-v2-m3")
HF_RERANKER_TIMEOUT = float(os.getenv("HF_RERANKER_TIMEOUT", "60"))
_RERANKER_POOL: list[str] = _build_endpoint_pool("RERANKER_ENDPOINT")
HF_RERANKER_ENDPOINT_URL = _RERANKER_POOL[0] if _RERANKER_POOL else ""
HF_RERANKER_ENDPOINT_URL_FALLBACK = _RERANKER_POOL[1] if len(_RERANKER_POOL) > 1 else ""
HF_RERANKER_ENABLED = bool(_RERANKER_POOL and HF_API_TOKEN)
HF_RERANKER_FALLBACK_ENABLED = len(_RERANKER_POOL) > 1 and bool(HF_API_TOKEN)

if HF_ENABLED:
    log.info("HF embed pool (%d endpoint%s): %s",
             len(_EMBED_POOL), "s" if len(_EMBED_POOL) != 1 else "", _EMBED_POOL)
else:
    log.info("HF embed endpoint disabled — set HF_ENDPOINT_URL + HF_API_TOKEN to enable")

if HF_RERANKER_ENABLED:
    log.info("HF Reranker Endpoint enabled: %s", HF_RERANKER_ENDPOINT_URL)
else:
    log.info("HF Reranker Endpoint disabled (local CPU CrossEncoder fallback) — "
             "set HF_RERANKER_ENDPOINT_URL + HF_API_TOKEN to enable")

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


def _embed_hf_sync_at(url: str, texts: list[str]) -> list[list[float]]:
    """POST a batch to the given TEI /embed URL and return vectors."""
    client = _get_hf_sync_client()
    r = client.post(f"{url}/embed", json={"inputs": _truncate(texts)})
    r.raise_for_status()
    return r.json()


async def _embed_hf_async_at(url: str, texts: list[str]) -> list[list[float]]:
    client = _get_hf_async_client()
    r = await client.post(f"{url}/embed", json={"inputs": _truncate(texts)})
    r.raise_for_status()
    return r.json()


def _with_fallback_sync(primary_url, fallback_url, fallback_enabled, label, fn, *args):
    """Try fn(primary_url, *args); on any exception try fn(fallback_url, *args)."""
    try:
        return fn(primary_url, *args)
    except Exception as err:
        if not fallback_enabled:
            raise
        log.warning("HF %s primary %s failed (%s: %s) — trying fallback %s",
                    label, primary_url, type(err).__name__, err, fallback_url)
        return fn(fallback_url, *args)


async def _with_fallback_async(primary_url, fallback_url, fallback_enabled, label, fn, *args):
    """Async variant of _with_fallback_sync."""
    try:
        return await fn(primary_url, *args)
    except Exception as err:
        if not fallback_enabled:
            raise
        log.warning("HF %s primary %s failed (%s: %s) — trying fallback %s",
                    label, primary_url, type(err).__name__, err, fallback_url)
        return await fn(fallback_url, *args)


def _embed_hf_sync(texts: list[str]) -> list[list[float]]:
    """Round-robin across embed pool; fall back to next endpoint on error."""
    if len(_EMBED_POOL) == 1:
        return _embed_hf_sync_at(_EMBED_POOL[0], texts)
    primary = _next_embed_url()
    rest = [u for u in _EMBED_POOL if u != primary]
    try:
        return _embed_hf_sync_at(primary, texts)
    except Exception as err:
        log.warning("HF embed %s failed (%s) — trying remaining pool", primary, err)
        for url in rest:
            try:
                return _embed_hf_sync_at(url, texts)
            except Exception:
                continue
        raise


async def _embed_hf_async(texts: list[str]) -> list[list[float]]:
    """Async round-robin across embed pool."""
    if len(_EMBED_POOL) == 1:
        return await _embed_hf_async_at(_EMBED_POOL[0], texts)
    primary = _next_embed_url()
    rest = [u for u in _EMBED_POOL if u != primary]
    try:
        return await _embed_hf_async_at(primary, texts)
    except Exception as err:
        log.warning("HF embed %s failed (%s) — trying remaining pool", primary, err)
        for url in rest:
            try:
                return await _embed_hf_async_at(url, texts)
            except Exception:
                continue
        raise


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


# ============================================================
# RERANKER (bge-reranker-v2-m3, cross-encoder)
# ============================================================
#
# The reranker is a SEPARATE model from the embedder — cross-encoders
# read (query, passage) pairs jointly, producing higher-quality relevance
# scores than cosine-on-dense-vectors but at O(n) cost per query.
#
# Pipeline: dense retrieval returns top-50 → rerank to top-10 → return.
# Net effect on search quality is large; compute cost per query is small
# (one short forward pass per candidate, ~50ms on CPU).

_reranker = None


def _load_reranker():
    """Load BAAI/bge-reranker-v2-m3 as a CrossEncoder (local fallback)."""
    global _reranker
    if _reranker is not None:
        return _reranker

    from sentence_transformers import CrossEncoder
    offline = os.getenv("HF_HUB_OFFLINE", "0") == "1" or \
              os.getenv("TRANSFORMERS_OFFLINE", "0") == "1"
    log.info("Loading reranker model %s (local_files_only=%s)...",
             RERANKER_MODEL_NAME, offline)
    t0 = time.time()
    _reranker = CrossEncoder(
        RERANKER_MODEL_NAME,
        device="cpu",
        trust_remote_code=True,
        local_files_only=offline,
    )
    log.info("Reranker loaded in %.1fs", time.time() - t0)
    return _reranker


def _rerank_hf_sync_at(url: str, query: str, passages: list[str]) -> list[tuple[int, float]]:
    """Call TEI /rerank at the given URL. Returns [(index, score), ...] sorted desc."""
    client = _get_hf_sync_client()
    r = client.post(
        f"{url}/rerank",
        json={"query": query, "texts": _truncate(passages), "raw_scores": False},
        timeout=HF_RERANKER_TIMEOUT,
    )
    r.raise_for_status()
    return [(item["index"], float(item["score"])) for item in r.json()]


async def _rerank_hf_async_at(url: str, query: str, passages: list[str]) -> list[tuple[int, float]]:
    client = _get_hf_async_client()
    r = await client.post(
        f"{url}/rerank",
        json={"query": query, "texts": _truncate(passages), "raw_scores": False},
        timeout=HF_RERANKER_TIMEOUT,
    )
    r.raise_for_status()
    return [(item["index"], float(item["score"])) for item in r.json()]


def _rerank_hf_sync(query: str, passages: list[str]) -> list[tuple[int, float]]:
    return _with_fallback_sync(
        HF_RERANKER_ENDPOINT_URL, HF_RERANKER_ENDPOINT_URL_FALLBACK, HF_RERANKER_FALLBACK_ENABLED,
        "reranker", _rerank_hf_sync_at, query, passages,
    )


async def _rerank_hf_async(query: str, passages: list[str]) -> list[tuple[int, float]]:
    return await _with_fallback_async(
        HF_RERANKER_ENDPOINT_URL, HF_RERANKER_ENDPOINT_URL_FALLBACK, HF_RERANKER_FALLBACK_ENABLED,
        "reranker", _rerank_hf_async_at, query, passages,
    )


def _rerank_cpu_sync(query: str, passages: list[str]) -> list[tuple[int, float]]:
    """Local CrossEncoder rerank — returns (index, score) desc."""
    model = _load_reranker()
    pairs = [(query, p) for p in _truncate(passages)]
    scores = model.predict(pairs, show_progress_bar=False)
    ranked = sorted(enumerate(scores.tolist()), key=lambda x: -x[1])
    return ranked


# ── Public reranker API ──────────────────────────────────────

def rerank_sync(
    query: str,
    passages: list[str],
    top_k: Optional[int] = None,
) -> list[tuple[int, float]]:
    """Rerank `passages` against `query`. Returns [(original_index, score), ...]
    sorted by score desc, optionally truncated to top_k.

    Tries HF reranker endpoint first (if configured), falls back to local CPU
    on any failure — matching the embedder's path-selection semantics.
    """
    if not passages:
        return []

    ranked: Optional[list[tuple[int, float]]] = None

    if HF_RERANKER_ENABLED:
        try:
            ranked = _rerank_hf_sync(query, passages)
        except Exception as e:
            log.warning(
                "HF reranker endpoint failed (%s: %s) — falling back to local CPU",
                type(e).__name__, e,
            )

    if ranked is None:
        ranked = _rerank_cpu_sync(query, passages)

    if top_k is not None and top_k > 0:
        ranked = ranked[:top_k]
    return ranked


async def rerank(
    query: str,
    passages: list[str],
    top_k: Optional[int] = None,
) -> list[tuple[int, float]]:
    """Async rerank — same semantics as rerank_sync."""
    if not passages:
        return []

    ranked: Optional[list[tuple[int, float]]] = None

    if HF_RERANKER_ENABLED:
        try:
            ranked = await _rerank_hf_async(query, passages)
        except Exception as e:
            log.warning(
                "HF reranker endpoint failed (%s: %s) — falling back to local CPU",
                type(e).__name__, e,
            )

    if ranked is None:
        loop = asyncio.get_event_loop()
        ranked = await loop.run_in_executor(None, _rerank_cpu_sync, query, passages)

    if top_k is not None and top_k > 0:
        ranked = ranked[:top_k]
    return ranked


# ============================================================
# Model registry helper
# ============================================================
#
# Returns the core.embedding_models.id for the currently-active embedder,
# creating/upserting the registry row if needed. Called by the ingestion
# pipeline per-document so core.document_chunks.embedding_model_id is
# always populated — lets future re-embedding migrations run side-by-side
# via the ops.embedding_migration tracker.

_embedding_model_id_cache: dict[tuple, int] = {}


async def get_or_create_embedding_model_id(conn_or_pool) -> int:
    """Resolve (and cache) the current embedding model ID.

    Accepts either an asyncpg Connection or Pool. Safe to call inside
    another transaction — the upsert is idempotent.
    """
    cache_key = (EMBEDDING_MODEL_NAME, EMBEDDING_DIMENSIONS)
    cached = _embedding_model_id_cache.get(cache_key)
    if cached is not None:
        return cached

    provider = "local" if not HF_ENABLED else "local+hf_tei"
    row = await conn_or_pool.fetchrow(
        """
        INSERT INTO core.embedding_models (name, provider, dimensions, is_local, is_active)
        VALUES ($1, $2, $3, true, true)
        ON CONFLICT (name) DO UPDATE SET
            provider = EXCLUDED.provider,
            dimensions = EXCLUDED.dimensions
        RETURNING id
        """,
        EMBEDDING_MODEL_NAME,
        provider,
        EMBEDDING_DIMENSIONS,
    )
    if not row:
        raise RuntimeError("Unable to resolve the current embedding model ID")
    model_id = int(row["id"])
    _embedding_model_id_cache[cache_key] = model_id
    return model_id


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
