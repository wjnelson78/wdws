# Athena Cognitive Engine — Embedding & Retrieval Architecture

**Purpose**: This document describes the complete embedding infrastructure so that any AI agent (or human engineer) can understand, evaluate, extend, or replicate it.

**Last Updated**: April 5, 2026

---

## 1. Design Goals

1. **Zero API dependency** — All embeddings generated locally, no external API keys required
2. **Contextual Retrieval** — Every chunk carries document-level awareness before embedding (per Anthropic's research showing 49% reduction in retrieval failures)
3. **Auditability** — Track exactly what text was embedded, which model was used, and when
4. **Graceful migration** — Switch embedding models without downtime; old and new vectors can coexist during transition
5. **Cost elimination** — Replace $0.13/1M token OpenAI embedding costs with local CPU inference at zero marginal cost

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION PIPELINE                          │
│                                                                 │
│  Document (PDF, email, filing, medical record, web page)        │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────┐     ┌──────────────────────────┐              │
│  │  Extract &   │     │  Context Generator       │              │
│  │  Clean Text  │────▶│  (Claude Sonnet via CLI)  │              │
│  │              │     │  2-3 sentence summary     │              │
│  └──────┬───────┘     └────────────┬─────────────┘              │
│         │                          │                            │
│         ▼                          ▼                            │
│  ┌─────────────┐     ┌──────────────────────────┐              │
│  │   Chunker    │     │  Context Prepender        │              │
│  │  ~800 chars  │────▶│  <context>...</context>   │              │
│  │  200 overlap │     │  + original chunk text    │              │
│  └──────┬───────┘     └────────────┬─────────────┘              │
│         │                          │                            │
│         │              ┌───────────▼──────────────┐             │
│         │              │  BGE-M3 Embedding        │             │
│         │              │  1024-dim dense vector    │             │
│         │              │  Local CPU inference      │             │
│         │              └───────────┬──────────────┘             │
│         │                          │                            │
│         ▼                          ▼                            │
│  ┌──────────────────────────────────────────────────────┐      │
│  │              PostgreSQL 17 + pgvector                 │      │
│  │                                                       │      │
│  │  core.documents                                       │      │
│  │    ├── full_content   (cleaned text)                  │      │
│  │    └── raw_content    (pre-cleaning original)         │      │
│  │                                                       │      │
│  │  core.document_chunks                                 │      │
│  │    ├── content          (original chunk text)         │      │
│  │    ├── embedded_content (contextual text that was     │      │
│  │    │                     actually embedded)           │      │
│  │    ├── embedding        (halfvec(1024) — BGE-M3)     │      │
│  │    ├── embedding_model_id (FK → embedding_models)     │      │
│  │    └── embedded_at      (timestamp of last embedding) │      │
│  │                                                       │      │
│  │  core.embedding_models                                │      │
│  │    ├── id=1: text-embedding-3-large (openai, 3072)   │      │
│  │    └── id=2: BAAI/bge-m3 (local, 1024) ← ACTIVE     │      │
│  └──────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      SEARCH / RETRIEVAL                         │
│                                                                 │
│  User Query                                                     │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────┐                                           │
│  │  BGE-M3 Embed   │  (same model used for ingestion)          │
│  │  query → 1024d  │                                           │
│  └────────┬────────┘                                           │
│           │                                                     │
│           ▼                                                     │
│  ┌─────────────────────────────────────────┐                   │
│  │  pgvector HNSW Index                     │                   │
│  │  cosine similarity: embedding <=> query  │                   │
│  │  + optional tsvector full-text filter     │                   │
│  └─────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Contextual Retrieval — The Key Innovation

### Problem
Standard RAG chunks lose document-level context. A chunk saying "The court denied the motion on procedural grounds" is ambiguous — which court? which case? which motion? Embedding this raw text produces a generic vector that matches many irrelevant documents.

### Solution (Anthropic Research)
Before embedding, prepend a 2-3 sentence document-level context summary to every chunk. This summary is generated once per document by a capable LLM (Claude Sonnet), then mechanically prepended to each chunk.

### Implementation

**Step 1: Context Generation** (`contextual_retrieval.py`)
```python
# For each document, call Claude Sonnet via CLI:
context = await generate_context(
    title="Order Denying Motion to Dismiss",
    domain="legal",
    document_type="court_order",
    content_preview="<first 3000 chars of document>",
    case_number="24-2-01031-31",
)
# Returns: "This is a court order in Nelson v. BIIA, Case No. 24-2-01031-31,
#           in Snohomish County Superior Court. The order denies the defendant's
#           motion to dismiss on procedural grounds..."
```

**Step 2: Context Prepending** (`contextual_retrieval.py`)
```python
enriched = prepend_context(context, chunk_text)
# Returns:
# <context>
# This is a court order in Nelson v. BIIA, Case No. 24-2-01031-31...
# </context>
# The court denied the motion on procedural grounds, noting that
# the filing deadline had passed on March 15.
```

**Step 3: Embed the Enriched Text** (`embedding_service.py`)
```python
vector = embed_query_sync(enriched)  # 1024-dim BGE-M3 vector
```

### Why This Works
The embedding model sees "court order + Nelson v. BIIA + Case 24-2-01031-31 + motion to dismiss" in every chunk. Searches for "Nelson case dismissal" now match with high similarity because the chunk's vector encodes the case identity, not just the procedural language.

### Context Generator Design
- **Model**: Claude Sonnet via Claude Code CLI (uses Max subscription OAuth, zero API cost)
- **Prompt**: Strict 2-3 sentence output — document type, parties, subject, case number
- **Fallback**: If CLI fails, generates a basic template from metadata fields
- **Concurrency**: 3-5 parallel CLI processes (configurable)
- **Speed**: ~35 documents/minute at 5 concurrent
- **One-time cost**: Context is generated once per document and stored in `ops.embedding_migration.context_summary`

---

## 4. Embedding Model: BGE-M3

### Why BGE-M3
| Property | Value | Why It Matters |
|----------|-------|---------------|
| Dimensions | 1024 | 3x smaller than OpenAI 3072 — less storage, faster search |
| Context window | 8,192 tokens | Handles long chunks + contextual prefix |
| Multi-lingual | 100+ languages | Future-proof for multilingual documents |
| Retrieval modes | Dense + Sparse + ColBERT | Single model replaces semantic + full-text |
| MTEB ranking | Top 5 open-source | Competitive with commercial APIs |
| Inference | CPU-only capable | No GPU required |
| Cost | Free | No API key, no per-token charges |
| License | MIT | No commercial restrictions |

### Local Deployment
```python
# embedding_service.py — singleton pattern
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("BAAI/bge-m3", device="cpu")
model.max_seq_length = 8192

# Encode with normalization for cosine similarity
vectors = model.encode(texts, normalize_embeddings=True)
```

- **RAM**: ~2.3 GB for model weights
- **CPU speed**: ~80-100ms per text on 32-core Xeon
- **Batch speed**: ~64 texts in ~2 seconds
- **First load**: ~6 seconds (cached after)

### API Surface (`embedding_service.py`)
```python
# Async (for web servers, agents)
vector = await embed_query(text)           # Single text → 1024-dim list
vectors = await embed_texts(texts)         # Batch → list of 1024-dim lists

# Sync (for ingestion scripts, migration)
vector = embed_query_sync(text)            # Single text → 1024-dim list
vectors = embed_texts_sync(texts)          # Batch → list of 1024-dim lists

# Utility
vec_str = _vec_literal(vector)             # → "[0.123456,0.234567,...]" for pgvector

# Constants
EMBEDDING_DIMENSIONS = 1024
EMBEDDING_MODEL = "BAAI/bge-m3"
```

---

## 5. Database Schema

### Core Tables

**`core.documents`**
| Column | Type | Purpose |
|--------|------|---------|
| id | UUID PK | Document identifier |
| full_content | TEXT | Cleaned/normalized text (what gets chunked) |
| raw_content | TEXT | Original pre-cleaning text (audit trail) |
| domain | TEXT | legal, medical, web, etc. |
| document_type | TEXT | email, filing, pdf, etc. |
| title | TEXT | Document title |
| content_hash | TEXT | Deduplication hash |
| metadata | JSONB | Flexible metadata |

**`core.document_chunks`**
| Column | Type | Purpose |
|--------|------|---------|
| id | TEXT PK | Chunk identifier |
| document_id | UUID FK | Parent document |
| chunk_index | INT | Position in document |
| content | TEXT | Original chunk text (without context prefix) |
| embedded_content | TEXT | Actual text that was embedded (with context prefix) |
| embedding | halfvec(1024) | BGE-M3 vector |
| embedding_model_id | INT FK | References core.embedding_models |
| embedded_at | TIMESTAMPTZ | When last embedded |
| content_tsv | tsvector | Full-text search index |
| metadata | JSONB | Includes embedding_model name, contextual flag |

**`core.embedding_models`**
| Column | Type | Purpose |
|--------|------|---------|
| id | SERIAL PK | Model identifier |
| name | TEXT UNIQUE | e.g., "BAAI/bge-m3" |
| provider | TEXT | "local", "openai", "anthropic" |
| dimensions | INT | Vector dimensions |
| is_local | BOOLEAN | Whether model runs locally |
| is_active | BOOLEAN | Currently in use |

### pgvector Index
```sql
CREATE INDEX idx_chunks_embedding_hnsw
ON core.document_chunks
USING hnsw ((embedding::halfvec(1024)) halfvec_cosine_ops)
WITH (m = 16, ef_construction = 200);
```

### Search Query Pattern
```sql
-- Semantic search with cosine similarity
SELECT c.content, c.embedded_content, d.title, d.domain,
       (c.embedding <=> $1::halfvec(1024)) as distance
FROM core.document_chunks c
JOIN core.documents d ON c.document_id = d.id
WHERE c.embedding IS NOT NULL
ORDER BY c.embedding <=> $1::halfvec(1024)
LIMIT 10;
```

### Hybrid Search Pattern
```sql
-- Combined semantic + full-text with RRF fusion
SELECT * FROM core.hybrid_search(
    query_text,              -- for tsvector matching
    query_embedding,         -- for vector similarity
    top_k,
    domain_filter,
    semantic_weight,         -- default 0.7
    fulltext_weight,         -- default 0.3
    rrf_k                    -- default 60
);
```

---

## 6. Migration Strategy

### Overview
The migration from OpenAI text-embedding-3-large (3072-dim) to BGE-M3 (1024-dim) with Contextual Retrieval is handled by `migrate_embeddings.py` in three phases:

### Phase 1: Context Generation
- Iterate all documents in `core.documents`
- For each, call Claude Sonnet to generate a 2-3 sentence context summary
- Store in `ops.embedding_migration` table with checkpointing every 50 docs
- **Resumable**: skips documents that already have contexts on restart

### Phase 2: Re-embedding
- For each document with a context summary:
  - Fetch all chunks from `core.document_chunks`
  - Prepend context to each chunk
  - Embed batch with BGE-M3 locally
  - Update: `embedding` (1024-dim vector), `embedded_content` (contextual text), `embedding_model_id` (2), `embedded_at` (now)
- **Resumable**: skips documents marked `status = 'complete'`

### Phase 3: Index Rebuild
- Drop old HNSW index (was 3072-dim)
- Create new HNSW index for 1024-dim halfvec
- Parameters: m=16, ef_construction=200

### Schema Migration Required Before Phase 2
```sql
-- Column type must change from halfvec(3072) to halfvec(1024)
DROP VIEW IF EXISTS core.searchable_chunks;
DROP INDEX IF EXISTS core.idx_chunks_embedding;
ALTER TABLE core.document_chunks
    ALTER COLUMN embedding TYPE halfvec(1024) USING NULL;
-- Recreate view and index after migration
```

### Running the Migration
```bash
# Preview
python migrate_embeddings.py --dry-run

# Full run (contexts + embedding + index)
python migrate_embeddings.py

# Resume after interruption
python migrate_embeddings.py --resume

# Run phases separately
python migrate_embeddings.py --contexts-only
python migrate_embeddings.py --embed-only

# Tune concurrency
python migrate_embeddings.py --concurrency 5 --batch-size 128
```

### Migration Tracking Table
```sql
-- ops.embedding_migration
document_id UUID PRIMARY KEY,
context_summary TEXT,              -- Sonnet-generated context
context_generated_at TIMESTAMPTZ,
embedded_at TIMESTAMPTZ,
old_model TEXT,                    -- 'text-embedding-3-large'
new_model TEXT,                    -- 'BAAI/bge-m3'
old_dims INT,                      -- 3072
new_dims INT,                      -- 1024
chunk_count INT,
status TEXT                        -- pending → context_ready → complete
```

---

## 7. LLM Provider Stack

The platform uses a multi-provider LLM architecture for non-embedding tasks (chat, reasoning, code generation). This is separate from embeddings but relevant context:

```
Primary:   OpenAI (GPT-5.4, GPT-5.2-Codex)     — API key
Fallback1: Anthropic API (Haiku, Sonnet, Opus)   — API key
Fallback2: Claude Code CLI (haiku, sonnet, opus)  — Max subscription OAuth
Fallback3: Ollama (llama3.2:3b)                   — Local, free
```

- Task-aware routing: legal/medical → stronger models, triage → lightweight
- Circuit breaker per provider with 60s timeout
- Automatic failover on 401/5xx/timeout
- See `model_router.py` and `agents/framework.py`

---

## 8. File Reference

| File | Purpose |
|------|---------|
| `embedding_service.py` | BGE-M3 singleton, sync/async embed functions, vec_literal helper |
| `contextual_retrieval.py` | Claude Sonnet context generator, prepend_context, batch processing |
| `migrate_embeddings.py` | 3-phase migration script with resume support |
| `model_router.py` | Multi-provider LLM router (separate from embeddings) |
| `agents/framework.py` | Agent base classes, LLM helpers with fallback chain |
| `agents/config.py` | Environment configuration, model defaults |
| `ingest.py` | Primary document ingestion pipeline |
| `email_sync.py` | Microsoft Graph email ingestion |
| `mcp-server/mcp_server_v2.py` | MCP server with semantic search endpoints |

---

## 9. Performance Characteristics

### Embedding (BGE-M3 on 32-core Xeon E5-2697v4, no GPU)
| Operation | Latency | Throughput |
|-----------|---------|------------|
| Single query embed | ~80ms | 12/sec |
| Batch 64 texts | ~5s | ~13/sec |
| Full corpus (50K chunks) | ~70 min | ~12/sec |

### Search (pgvector HNSW on PostgreSQL 17)
| Operation | Latency |
|-----------|---------|
| Vector similarity (top 10) | ~20-50ms |
| Hybrid search (semantic + FTS) | ~100-150ms |
| Full-text only | ~20-50ms |

### Storage
| Model | Dims | Per-chunk | 50K chunks |
|-------|------|-----------|------------|
| text-embedding-3-large | 3072 | ~6 KB (halfvec) | ~276 MB |
| BGE-M3 | 1024 | ~2 KB (halfvec) | ~92 MB |

---

## 10. Extending This System

### Adding a New Embedding Model
1. Add entry to `core.embedding_models` table
2. Update `embedding_service.py` to load the new model
3. Run migration with `--embed-only` to re-embed existing chunks
4. Update HNSW index dimensions if changed

### Adding Sparse Vectors (BM25-like from BGE-M3)
BGE-M3 can produce sparse vectors alongside dense. To enable:
1. Add `sparse_embedding` column (sparsevec type from pgvector)
2. Modify `embed_texts_sync()` to return both dense and sparse
3. Combine in search: `alpha * dense_score + (1-alpha) * sparse_score`

### Adding ColBERT Multi-Vector Retrieval
BGE-M3 also supports ColBERT-style token-level vectors for fine-grained matching:
1. Store per-token vectors in a separate table
2. Late interaction scoring at query time
3. Higher accuracy but more storage and computation

### Switching to GPU
If a GPU becomes available:
```python
model = SentenceTransformer("BAAI/bge-m3", device="cuda")
# Speed increase: ~10-20x over CPU
# Batch of 64: ~200ms instead of ~5s
```

---

## 11. Known Limitations

1. **CPU-only inference** — No GPU available on current hardware. Embedding is ~80ms/text. Acceptable for search queries, slower for bulk ingestion.

2. **Context generation requires Claude CLI** — The Sonnet context summaries depend on Claude Code CLI (Max subscription). If unavailable, falls back to template-based contexts from metadata only.

3. **Single-node** — No distributed embedding service. All inference runs on the same machine as the database and agents.

4. **No sparse vector index yet** — BGE-M3's sparse vectors are not currently stored or indexed. The hybrid search uses pgvector dense + PostgreSQL tsvector instead.

5. **Migration is destructive** — The column type change from halfvec(3072) to halfvec(1024) clears existing embeddings. During migration, semantic search returns no results for un-migrated documents.
