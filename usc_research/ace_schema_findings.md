# ACE Schema Findings — Phase 0 Research

**Author:** USC ingestion agent
**Date:** 2026-04-28
**Method:** Live introspection via MCP `execute_sql` against the running ACE MCP at `klunky.12432.net` (PID 2732102, `wdws-mcp.service`, Python venv at `/opt/wdws/venv`).

---

## 0. Pre-Phase-0 Blocker — RESOLVED

**Question:** Does the MCP server read from a different DB than the wdws ingestion pipeline writes to?

**Answer:** **NO. Both sides target `wdws`.** Verified:

| Service | Source | DSN |
|---|---|---|
| wdws-mcp.service (port 9200, `klunky.12432.net/mcp/sse`) | `/opt/wdws/mcp-server/.env` line 5 | `postgresql://wdws:***@127.0.0.1:5432/wdws` |
| Athena API gateway (`/opt/acp/services/api_gateway.py`) | `/opt/acp/.env` | `postgresql://wdws:***@127.0.0.1:5432/wdws` |
| Athena Chat API (separate, conversation-state only) | `/opt/acp/.env` | `postgresql://athena_chat:***@127.0.0.1:5432/athena_chat` |
| `sprint_a_llm_backfill.py` ingestion pipeline | `--dsn` default | `postgresql:///wdws?host=/var/run/postgresql` |
| `morning_briefing.py` | `os.environ["DATABASE_URL"]` | wdws |

`athena_chat` is a Chat API conversation-state DB — not the corpus retrieval DB. **USC corpus → `wdws` DB, MCP retrieval will see it.** No schema-divergence blocker.

---

## 1. Database & Server

- **Database:** `wdws` on `127.0.0.1:5432` (PostgreSQL 17 + pgvector)
- **Schemas present:** `core`, `legal`, `medical`, `ops`, `paperless`, `web`, `public` — no `usc` schema yet (must be created)
- **Total corpus:** 10,947 documents / 116,644 chunks / 12 active cases / 2,052 MB DB size
- **Date range of existing docs:** 2026-02-08 → 2026-04-28
- **MCP runtime:** `wdws-mcp.service` active since 2026-04-28 08:14 PDT, ~112 MB RSS, asyncpg pool 2-10 connections, 30s command timeout.

## 2. Embedding Model — CONFIRMED BGE-M3

`core.embedding_models`:

| id | name | provider | dimensions | active |
|---|---|---|---|---|
| 1 | `text-embedding-3-large` | openai | 3072 | **inactive** (legacy, 67,154 chunks still embedded) |
| 2 | **`BAAI/bge-m3`** | `local+hf_tei` | **1024** | **active** (44,078 chunks) |

USC chunks must be embedded with **model_id=2 (BGE-M3, 1024-dim)**. The legacy 3072-dim chunks coexist but are not the target.

## 3. Vector Storage — `halfvec`, NOT `vector`

`core.document_chunks.embedding` column type: **`halfvec(1024)`** (pgvector half-precision variant — 50% storage of `vector`, native operator support).

**Index:**
```sql
CREATE INDEX idx_chunks_embedding_hnsw
  ON core.document_chunks
  USING hnsw (embedding halfvec_cosine_ops)
  WITH (m='16', ef_construction='200');
```

This means our ingestion code must **cast embeddings to `halfvec`** before insert (asyncpg will accept the `vector` -> `halfvec` cast if we pass them as the right type, or we explicitly `::halfvec(1024)` in the INSERT).

## 4. `core.documents` — Full Column Map

37 columns. Critical ones for USC ingestion:

| Column | Type | Notes |
|---|---|---|
| `id` | uuid (gen_random_uuid()) | PK |
| `domain` | text NOT NULL | **Set to `'legal'`** |
| `document_type` | text NOT NULL default `'unknown'` | **Set to `'statute'`** (new value, no enum constraint) |
| `source_path` | text (idx) | **Per memory: must be unique per logical doc.** Use `usc:{release_point}:t{N}:s{M}` |
| `filename` | text | Suggest `t{N}-s{M}.xml` |
| `title` | text | **Canonical citation + heading**: `42 U.S.C. § 12101 — Findings and purpose` |
| `content_hash` | text (idx) | SHA-256 of normalized full_text — drives idempotency |
| `total_chunks` | int (default 0) | Maintain post-chunking |
| `full_content` | text | Normalized markdown (the chunkable text) |
| `full_content_tsv` | tsvector (idx GIN) | **Trigger-maintained or write at insert** |
| `metadata` | jsonb (idx GIN, jsonb_path_ops) | Carries the Phase-1 metadata schema verbatim |
| `created_at`/`updated_at` | timestamptz | default now() |
| `classification` | text | **Set to `'public'`** for USC |
| `privilege` | text | **Leave NULL** (USC is public domain) |
| `confidentiality` | text | **Set to `'public'`** |
| `phi_status` | text | **Leave NULL** |
| `phi_categories` | text[] | `'{}'` |
| `legal_hold` | bool NOT NULL default false | leave false |
| `storage_*` columns | various | leave NULL — XML source preserved in `usc.sections.source_xml` instead |

**Per memory** (MCP privilege filter, NULL-tolerant + first-party default): USC docs with `privilege=NULL` will retrieve normally under the post-2026-04-24 retrieval gating semantics — confirmed compatible.

**Indexes already in place that we get for free:**
- `idx_documents_domain` (btree)
- `idx_documents_type` (btree)
- `idx_documents_domain_type` (btree composite)
- `idx_documents_fulltext` (GIN on `full_content_tsv`)
- `idx_documents_fts` (GIN on `to_tsvector('english', title || filename || full_content)` — **language config = `english`**)
- `idx_documents_metadata` (GIN on `metadata jsonb_path_ops`)
- `idx_documents_source` (btree on `source_path` — supports idempotent ingestion)

**Tsvector language confirmed: `english`.**

## 5. `core.document_chunks` — Full Column Map

13 columns:

| Column | Type | Notes |
|---|---|---|
| `id` | text | PK — convention is `{document_id}:{chunk_index}` based on existing rows |
| `document_id` | uuid NOT NULL | FK to documents |
| `chunk_index` | int NOT NULL | 0-based |
| `total_chunks` | int NOT NULL | denormalized total for the parent doc |
| `content` | text NOT NULL | the chunk body (markdown) |
| `embedding` | halfvec(1024) | nullable — embed-after-insert pattern |
| `embedding_model_id` | int | FK to embedding_models — set 2 |
| `token_count` | int | populate via tokenizer |
| `content_tsv` | tsvector | trigger or explicit |
| `metadata` | jsonb | Per Phase-1 schema (incl. `uslm_identifier`, `subsection`, breadcrumb) |
| `embedded_content` | text | the exact text fed to BGE-M3 (may differ from content if breadcrumbs prepended) |
| `embedded_at` | timestamptz | timestamp of embed op |

**Unique constraint:** `(document_id, chunk_index)` — supports re-embedding without duplicate insert.

## 6. Tags — NORMALIZED, Not a JSONB Array

**This is a delta from the Phase 1 doc design.** The doc assumed `tags` is a text[] column on documents. It is NOT.

Schema:
- **`core.tags`** — `id (int4 pk)`, `name`, `slug`, `category`, `parent_id`, `description`, `created_at`. 212 tags currently exist; 14 categories.
- **`core.document_tags`** — `(document_id uuid, tag_id int4)` join table.
- **`core.tag_rules`** — auto-tagging rule registry (out of scope for Phase 1, but worth knowing).
- **`core.v_tag_hierarchy`** — view for navigating parent_id trees.

**Existing tags relevant to USC ingestion** (we should reuse, not duplicate):
- `ada` (legal-framework)
- `ada-title-ii`, `federal-supremacy`, `ada-violations`, `ada-anti-surcharge` (uncategorized — operator-created topical tags)
- `WA ADA — *` family (jurisdiction/wa-agency) — NOT applicable to USC corpus
- 212 tags total, 14 categories: ada-doc-type, wa-agency, document_class, disability_type, judge, outcome, wa-healthcare, jurisdiction, prr_source, legal-framework, scope, wa-employer, plus uncategorized

**Phase 1 design implication:**
- Need to seed a new tag category — recommend `usc-meta` for `usc`, `federal-statute`, `appendix`, `repealed`, `transferred`, `omitted`
- Plus `usc-title` for `title-1` … `title-54`
- Plus `usc-chapter` for `title-42-chapter-126` (composite slug to avoid collision)
- Topical tags (`section-1983`, `qualified-immunity`, `judicial-review`, etc.) go in a new `usc-topic` category — must NOT collide with existing operator topical tags. Recommend slug prefix `usc:topic:section-1983`.

The `usc.sections` schema in Phase 1 should drop the `tags array` field; tag attachment happens via `core.document_tags` rows during ingestion (one INSERT per tag per section).

## 7. Domains

`list_document_domains` returned 11 active domains. **`legal`** is sort_order 20, label "Legal", description "Court filings, legal correspondence, case analysis, and litigation work product." Suitable for USC corpus — no new domain required.

## 8. Existing Document-Type Distribution (Sanity Check)

Top types: `legal/email` (4,840), `legal/email_attachment` (4,385), `medical/progress_note` (482), `legal/court_filing` (443), `medical/other` (270). No existing `legal/statute` document_type — we are introducing a new value. The column has no enum constraint so this is safe.

## 9. RAG-Query Behavior (Baseline)

`rag_query` with `domain="legal"` returned correctly-ranked chunks with `search_method: "hybrid_rrf (semantic=0.70, fulltext=0.30, neighbors=1)"` and reranker scores. The retrieval pipeline is healthy and will surface USC sections once ingested. **No retrieval-side changes needed.**

The first ADA Title II probe returned a transient `MCP server connection lost` error — non-blocking, retry on reconnect. The FRCP 17(c)(2) probe succeeded and surfaced exactly the existing case-law / motion content one would expect, validating the pipeline.

## 10. Athena Agent State

`get_agent_context(athena)`: 32 memories, run_count=5217, last cycle 2026-04-28T16:00:53Z = success. Recent reasoning-cycle warnings cleared by 16:00. Memories not enumerated here (94k chars output truncated); structure confirms `save_memory("usc_corpus", …)` is the right Phase-6 hook.

## 11. Active Litigation Context (Litigation-Priority Confirmation)

12 active cases — all Nelson litigation:
- **Federal (WDWA):** `325CV05551DGE` Nelson v BIIA et al (259 docs); `226CV00337KKE` Nelson v Silver Lake (256 docs)
- **Snohomish County Superior Court:** Prisma Wellness, IAFF v SSCFR (653 docs — biggest), Baseggio, Haward, Gibson, FTCU (both directions), Starbucks, Unum, Kuker

**Litigation-priority titles confirmed:**
- **Title 42** — ADA, § 1983, civil rights — invoked across nearly every case
- **Title 28** — FRCP appendix (Rule 17(c)(2) is central to current motion practice), § 1738 full faith and credit, § 1915(e)(1) IFP
- **Title 29** — Rehab Act § 794 (parallel to ADA Title II), labor/ERISA
- **Title 5** — APA judicial review (BIIA case)
- **Title 18** — criminal procedure (criminal-adjacent counts)
- **Title 11** — bankruptcy (FTCU litigation context)
- Title 38 (veterans) — relevance unclear, lower priority
- Title 26 (IRC) — only if FTCU triggers tax issues

**Confirms the Phase 1 priority order: 42, 28, 29, 5, 18, 11, then everything else.**

## 12. Schema Deltas vs. Phase 1 Doc — Summary for Phase 1 Revision

| Phase 1 Doc Said | Reality | Action |
|---|---|---|
| `embedding vector(1024)` | `halfvec(1024)` | Cast to halfvec on insert |
| `tags array` on documents | normalized `core.tags` + `core.document_tags` | Drop tags column from `usc.sections`, attach via join during ingest |
| Release point `PL 119-73not60` | **PL 119-84 (2026-04-18)** | Use `119-84` URL pattern |
| (implicit) `vector` cosine ops | `halfvec_cosine_ops` HNSW already in place | No new index needed — chunks just need `embedding_model_id=2` |
| `core.documents` minimal | 37-column table with full governance/PHI/privilege apparatus | Set `domain='legal'`, `classification='public'`, `confidentiality='public'`, leave privilege/phi_* NULL |

## 13. Ingestion Path Recommendation

For 50k–80k sections, **direct asyncpg INSERT** into the running wdws DB (option per Phase-1 §5.1) is the right path. The MCP `create_document` route is single-doc and unsuitable. Pseudocode for Phase 4:

```python
async with pool.acquire() as conn, conn.transaction():
    doc_id = await conn.fetchval(
        "INSERT INTO core.documents (domain, document_type, title, source_path, filename, full_content, metadata, classification, confidentiality) "
        "VALUES ('legal','statute',$1,$2,$3,$4,$5,'public','public') "
        "ON CONFLICT (source_path) DO UPDATE SET updated_at=now() RETURNING id",
        title, source_path, filename, normalized_md, metadata_jsonb)
    # then usc.sections, usc.section_history, usc.section_notes
    # then chunks (without embedding)
# embeddings batched separately via BGE-M3 batches of 64
```

We need an explicit unique constraint on `core.documents.source_path` for the ON CONFLICT path to work — currently `source_path` has only a non-unique btree index. Phase 1 schema migration should add a partial unique index:

```sql
CREATE UNIQUE INDEX uq_documents_source_path
  ON core.documents (source_path)
  WHERE source_path IS NOT NULL;
```

(Will not collide with the existing btree.)

## 14. Open Items for Phase 1 Approval

1. Add `core.documents.source_path` partial unique index (mod to existing schema).
2. Add `usc-meta`, `usc-title`, `usc-chapter`, `usc-topic` categories to `core.tags` and seed initial values.
3. Confirm with operator: do we add `'statute'` to the existing `document_type` value set (no enum constraint, just convention) or use `'legal/statute'` to match the type-prefix pattern visible in `get_database_stats` output (`legal/email`, `legal/court_filing`)? **Recommend `'statute'`** — the prefix appears to be cosmetic in the stats roll-up; raw `document_type` values like `email`, `court_filing` are the actual stored strings.
