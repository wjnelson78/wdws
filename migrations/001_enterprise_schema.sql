-- ============================================================
-- WDWS Enterprise Database Schema v1.0
-- PostgreSQL 17 + pgvector 0.8.0
--
-- Design: Normalized multi-domain document platform with
--   - Hierarchical document→chunk RAG architecture
--   - Cross-domain entity tracking & relationship graphs
--   - Legal case management (state & federal)
--   - Medical record enrichment
--   - AI Agent self-discovery & observability
--   - RRF hybrid search (semantic + full-text) in SQL
--
-- Embedding: halfvec(3072) — required for HNSW on pgvector 0.8
--            (vector type caps at 2000 dims for HNSW)
-- ============================================================

-- Safety: drop old migration schemas (if any)
DROP SCHEMA IF EXISTS legal_cases CASCADE;
DROP SCHEMA IF EXISTS medical_records CASCADE;
DROP SCHEMA IF EXISTS paperless_docs CASCADE;
DROP SCHEMA IF EXISTS system CASCADE;

-- Idempotent: drop current design for clean re-deploy
DROP SCHEMA IF EXISTS ops CASCADE;
DROP SCHEMA IF EXISTS paperless CASCADE;
DROP SCHEMA IF EXISTS medical CASCADE;
DROP SCHEMA IF EXISTS legal CASCADE;
DROP SCHEMA IF EXISTS core CASCADE;

-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;      -- pgvector (halfvec, hnsw)
CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- trigram fuzzy matching

-- Schemas
CREATE SCHEMA core;       -- universal document & chunk management
CREATE SCHEMA legal;      -- court-case enrichment
CREATE SCHEMA medical;    -- healthcare record enrichment
CREATE SCHEMA paperless;  -- paperless-ngx integration
CREATE SCHEMA ops;        -- agents, access logs, analytics

-- ============================================================
-- UTILITY
-- ============================================================

CREATE OR REPLACE FUNCTION core.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- CORE SCHEMA
-- ============================================================

-- Reference table for embedding model provenance
CREATE TABLE core.embedding_models (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    provider    TEXT NOT NULL,
    dimensions  INT  NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);
INSERT INTO core.embedding_models (name, provider, dimensions)
VALUES ('text-embedding-3-large', 'openai', 3072);

COMMENT ON TABLE core.embedding_models IS
  'Registry of embedding models used to generate vectors. Every chunk records which model produced its embedding for reproducibility.';

-- Master document registry — one row per source file
CREATE TABLE core.documents (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    domain           TEXT NOT NULL CHECK (domain IN ('legal', 'medical', 'paperless')),
    source_path      TEXT,
    filename         TEXT,
    document_type    TEXT NOT NULL DEFAULT 'unknown',
    title            TEXT,
    content_hash     TEXT,
    total_chunks     INT  DEFAULT 0,
    full_content     TEXT,
    full_content_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(title,'') || ' ' || coalesce(full_content,''))
    ) STORED,
    metadata         JSONB DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE core.documents IS
  'Master registry of every ingested document across all domains. One row per unique source file. Contains concatenated full_content for document-level full-text search.';

CREATE TRIGGER trg_documents_updated
    BEFORE UPDATE ON core.documents
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

-- Chunk table — the core RAG retrieval unit
CREATE TABLE core.document_chunks (
    id                 TEXT PRIMARY KEY,
    document_id        UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    chunk_index        INT  NOT NULL,
    total_chunks       INT  NOT NULL,
    content            TEXT NOT NULL,
    embedding          halfvec(3072),
    embedding_model_id INT  REFERENCES core.embedding_models(id) DEFAULT 1,
    token_count        INT,
    content_tsv        TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', content)
    ) STORED,
    metadata           JSONB DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

COMMENT ON TABLE core.document_chunks IS
  'Chunked content with halfvec(3072) embeddings — the primary RAG retrieval unit. Each chunk references its parent document and carries positional info (chunk_index/total_chunks) for context window expansion.';

-- Document-to-document relationships (threads, attachments, references)
CREATE TABLE core.document_relationships (
    id                   SERIAL PRIMARY KEY,
    source_document_id   UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    target_document_id   UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    relationship_type    TEXT NOT NULL,  -- reply_to, attachment_of, references, supersedes
    confidence           FLOAT DEFAULT 1.0,
    metadata             JSONB DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ DEFAULT now(),
    UNIQUE (source_document_id, target_document_id, relationship_type)
);

COMMENT ON TABLE core.document_relationships IS
  'Graph edges between documents — email reply chains, attachment links, case cross-references, superseding orders.';

-- Named entities extracted from documents
CREATE TABLE core.entities (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    entity_type     TEXT NOT NULL,  -- person, organization, court, judge, attorney, case_number
    normalized_name TEXT,
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (normalized_name, entity_type)
);

COMMENT ON TABLE core.entities IS
  'Cross-domain entity registry. The same person or organization appearing in legal and medical records shares a single entity row, enabling cross-domain discovery.';

-- Entity ↔ Document junction
CREATE TABLE core.entity_mentions (
    id              SERIAL PRIMARY KEY,
    entity_id       INT  NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    document_id     UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    chunk_id        TEXT REFERENCES core.document_chunks(id) ON DELETE SET NULL,
    role            TEXT,  -- sender, recipient, judge, attorney, patient, provider
    mention_context TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE core.entity_mentions IS
  'Records where each entity is mentioned and in what role, enabling cross-domain queries like "show me all documents mentioning Dr. Smith".';

-- Flexible tagging
CREATE TABLE core.tags (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    category   TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE core.document_tags (
    document_id UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    tag_id      INT  NOT NULL REFERENCES core.tags(id)      ON DELETE CASCADE,
    PRIMARY KEY (document_id, tag_id)
);

-- ============================================================
-- LEGAL SCHEMA
-- ============================================================

CREATE TABLE legal.cases (
    id            SERIAL PRIMARY KEY,
    case_number   TEXT NOT NULL UNIQUE,
    court         TEXT,
    case_title    TEXT,
    case_type     TEXT,           -- civil, criminal, administrative
    status        TEXT DEFAULT 'active',
    date_filed    DATE,
    date_closed   DATE,
    jurisdiction  TEXT,           -- state, federal
    metadata      JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE legal.cases IS
  'Registry of all court cases. Supports both state (e.g. 24-2-01031-31) and federal (e.g. 325cv05551dge) case numbers.';

CREATE TRIGGER trg_cases_updated
    BEFORE UPDATE ON legal.cases
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TABLE legal.parties (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    party_type  TEXT,  -- individual, organization, government
    email       TEXT,
    metadata    JSONB DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX idx_parties_name_type ON legal.parties (name, COALESCE(party_type, ''));

COMMENT ON TABLE legal.parties IS
  'People and organizations involved in legal cases — plaintiffs, defendants, attorneys, judges.';

CREATE TABLE legal.case_parties (
    id                    SERIAL PRIMARY KEY,
    case_id               INT NOT NULL REFERENCES legal.cases(id)   ON DELETE CASCADE,
    party_id              INT NOT NULL REFERENCES legal.parties(id) ON DELETE CASCADE,
    role                  TEXT NOT NULL,  -- plaintiff, defendant, attorney, judge
    representing_party_id INT REFERENCES legal.parties(id),
    metadata              JSONB DEFAULT '{}'::jsonb,
    UNIQUE (case_id, party_id, role)
);

-- Many-to-many: documents can belong to multiple cases
CREATE TABLE legal.case_documents (
    case_id     INT  NOT NULL REFERENCES legal.cases(id)     ON DELETE CASCADE,
    document_id UUID NOT NULL REFERENCES core.documents(id)  ON DELETE CASCADE,
    PRIMARY KEY (case_id, document_id)
);

COMMENT ON TABLE legal.case_documents IS
  'Links documents to legal cases. An email may reference multiple cases; a filing belongs to exactly one.';

-- Email-specific enrichment
CREATE TABLE legal.email_metadata (
    document_id    UUID PRIMARY KEY REFERENCES core.documents(id) ON DELETE CASCADE,
    message_id     TEXT,
    in_reply_to    TEXT,
    thread_id      TEXT,
    sender         TEXT,
    recipients     TEXT,
    cc             TEXT,
    date_sent      TIMESTAMPTZ,
    direction      TEXT CHECK (direction IN ('inbound', 'outbound')),
    mailbox        TEXT,
    has_attachments BOOLEAN DEFAULT false
);

COMMENT ON TABLE legal.email_metadata IS
  'Enrichment layer for email documents — threading, sender/recipient, direction. Enables thread reconstruction and communication pattern analysis.';

-- Court filing enrichment
CREATE TABLE legal.filing_metadata (
    document_id          UUID PRIMARY KEY REFERENCES core.documents(id) ON DELETE CASCADE,
    docket_number        TEXT,
    filing_type          TEXT,   -- motion, order, complaint, response, declaration
    filed_by             TEXT,
    date_filed           DATE,
    court_document_number TEXT
);

COMMENT ON TABLE legal.filing_metadata IS
  'Enrichment layer for court filings — docket numbers, filing types, court document references.';

-- ============================================================
-- MEDICAL SCHEMA
-- ============================================================

CREATE TABLE medical.patients (
    id             SERIAL PRIMARY KEY,
    name           TEXT NOT NULL UNIQUE,
    date_of_birth  DATE,
    metadata       JSONB DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE medical.patients IS
  'Patient registry for medical records. Name-based deduplication.';

CREATE TABLE medical.providers (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    facility   TEXT,
    specialty  TEXT,
    metadata   JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX idx_providers_name_facility ON medical.providers (name, COALESCE(facility, ''));

COMMENT ON TABLE medical.providers IS
  'Healthcare providers and facilities linked to medical records.';

CREATE TABLE medical.record_metadata (
    document_id     UUID PRIMARY KEY REFERENCES core.documents(id) ON DELETE CASCADE,
    patient_id      INT REFERENCES medical.patients(id),
    provider_id     INT REFERENCES medical.providers(id),
    record_type     TEXT,     -- lab_result, imaging, clinical_note, prescription, discharge_summary
    date_of_service DATE,
    facility        TEXT,
    section_title   TEXT,
    metadata        JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE medical.record_metadata IS
  'Enrichment layer for medical documents — patient, provider, service date, record classification.';

-- ============================================================
-- PAPERLESS SCHEMA
-- ============================================================

CREATE TABLE paperless.document_metadata (
    document_id   UUID PRIMARY KEY REFERENCES core.documents(id) ON DELETE CASCADE,
    paperless_id  INT,
    correspondent TEXT,
    document_type TEXT,
    storage_path  TEXT,
    added_date    TIMESTAMPTZ,
    metadata      JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE paperless.document_metadata IS
  'Integration layer for Paperless-ngx document management system.';

-- ============================================================
-- OPS SCHEMA — AI Agent infrastructure & observability
-- ============================================================

CREATE TABLE ops.agent_registry (
    id           TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT,
    capabilities JSONB DEFAULT '[]'::jsonb,
    api_key_hash TEXT,
    is_active    BOOLEAN DEFAULT true,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE ops.agent_registry IS
  'Registry of AI agents authorized to access the database. Each agent has declared capabilities and an API key for authentication.';

CREATE TABLE ops.agent_sessions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id   TEXT REFERENCES ops.agent_registry(id),
    started_at TIMESTAMPTZ DEFAULT now(),
    ended_at   TIMESTAMPTZ,
    context    JSONB DEFAULT '{}'::jsonb,
    summary    TEXT
);

COMMENT ON TABLE ops.agent_sessions IS
  'Tracks AI agent conversation sessions for auditability and context continuity.';

CREATE TABLE ops.access_log (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    TIMESTAMPTZ DEFAULT now(),
    agent_id     TEXT,
    session_id   UUID,
    tool_name    TEXT NOT NULL,
    action       TEXT NOT NULL,  -- search, read, create, update, delete
    domain       TEXT,
    query        TEXT,
    result_count INT,
    duration_ms  FLOAT,
    metadata     JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE ops.access_log IS
  'Comprehensive audit trail of every tool invocation — who searched for what, when, and how many results were returned.';

CREATE TABLE ops.oauth_clients (
    client_id          TEXT PRIMARY KEY,
    client_name        TEXT NOT NULL,
    client_secret_hash TEXT NOT NULL,
    redirect_uris      TEXT[] DEFAULT '{}',
    scopes             TEXT[] DEFAULT '{read}',
    is_active          BOOLEAN DEFAULT true,
    created_at         TIMESTAMPTZ DEFAULT now(),
    last_used          TIMESTAMPTZ
);

CREATE TABLE ops.ingestion_jobs (
    id                  SERIAL PRIMARY KEY,
    domain              TEXT NOT NULL,
    source              TEXT NOT NULL,  -- chromadb_migration, email_sync, file_upload
    status              TEXT DEFAULT 'running',
    documents_processed INT DEFAULT 0,
    chunks_processed    INT DEFAULT 0,
    documents_failed    INT DEFAULT 0,
    started_at          TIMESTAMPTZ DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    error_log           JSONB DEFAULT '[]'::jsonb,
    metadata            JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE ops.ingestion_jobs IS
  'Tracks every data ingestion run — source, counts, errors. Enables lineage and debugging.';

CREATE TABLE ops.search_analytics (
    id           BIGSERIAL PRIMARY KEY,
    timestamp    TIMESTAMPTZ DEFAULT now(),
    query_text   TEXT,
    search_type  TEXT,   -- semantic, fulltext, hybrid
    domain       TEXT,
    result_count INT,
    top_score    FLOAT,
    duration_ms  FLOAT,
    agent_id     TEXT
);

-- ============================================================
-- INDEXES
-- ============================================================

-- Core: documents
CREATE INDEX idx_documents_domain     ON core.documents (domain);
CREATE INDEX idx_documents_type       ON core.documents (document_type);
CREATE INDEX idx_documents_source     ON core.documents (source_path);
CREATE INDEX idx_documents_hash       ON core.documents (content_hash);
CREATE INDEX idx_documents_created    ON core.documents (created_at);
CREATE INDEX idx_documents_fulltext   ON core.documents USING GIN (full_content_tsv);
CREATE INDEX idx_documents_metadata   ON core.documents USING GIN (metadata jsonb_path_ops);

-- Core: chunks (the critical retrieval indexes)
CREATE INDEX idx_chunks_document      ON core.document_chunks (document_id);
CREATE INDEX idx_chunks_fulltext      ON core.document_chunks USING GIN (content_tsv);
CREATE INDEX idx_chunks_metadata      ON core.document_chunks USING GIN (metadata jsonb_path_ops);
CREATE INDEX idx_chunks_embedding     ON core.document_chunks
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200);

-- Core: relationships
CREATE INDEX idx_docrel_source ON core.document_relationships (source_document_id);
CREATE INDEX idx_docrel_target ON core.document_relationships (target_document_id);
CREATE INDEX idx_docrel_type   ON core.document_relationships (relationship_type);

-- Core: entities
CREATE INDEX idx_entities_type       ON core.entities (entity_type);
CREATE INDEX idx_entities_normalized ON core.entities (normalized_name);
CREATE INDEX idx_entities_trgm       ON core.entities USING GIN (name gin_trgm_ops);
CREATE INDEX idx_entmention_entity   ON core.entity_mentions (entity_id);
CREATE INDEX idx_entmention_document ON core.entity_mentions (document_id);

-- Legal
CREATE INDEX idx_cases_number   ON legal.cases (case_number);
CREATE INDEX idx_cases_court    ON legal.cases (court);
CREATE INDEX idx_cases_status   ON legal.cases (status);
CREATE INDEX idx_casedoc_doc    ON legal.case_documents (document_id);
CREATE INDEX idx_email_thread   ON legal.email_metadata (thread_id);
CREATE INDEX idx_email_sender   ON legal.email_metadata (sender);
CREATE INDEX idx_email_date     ON legal.email_metadata (date_sent);
CREATE INDEX idx_email_msgid    ON legal.email_metadata (message_id);
CREATE INDEX idx_email_mailbox  ON legal.email_metadata (mailbox);

-- Medical
CREATE INDEX idx_medrec_patient  ON medical.record_metadata (patient_id);
CREATE INDEX idx_medrec_provider ON medical.record_metadata (provider_id);
CREATE INDEX idx_medrec_date     ON medical.record_metadata (date_of_service);

-- Ops
CREATE INDEX idx_access_ts     ON ops.access_log (timestamp);
CREATE INDEX idx_access_agent  ON ops.access_log (agent_id);
CREATE INDEX idx_access_tool   ON ops.access_log (tool_name);
CREATE INDEX idx_search_ts     ON ops.search_analytics (timestamp);
CREATE INDEX idx_ingest_status ON ops.ingestion_jobs (status);

-- ============================================================
-- VIEWS
-- ============================================================

-- Unified document view — cross-domain with enrichment
CREATE OR REPLACE VIEW core.unified_documents AS
SELECT
    d.id,
    d.domain,
    d.document_type,
    d.title,
    d.filename,
    d.source_path,
    d.total_chunks,
    d.metadata,
    d.created_at,
    -- Legal enrichment
    lc.case_number,
    lc.court,
    lc.case_title,
    em.sender,
    em.recipients,
    em.date_sent,
    em.direction,
    em.mailbox,
    -- Medical enrichment
    rm.date_of_service,
    rm.facility   AS medical_facility,
    rm.record_type,
    p.name        AS patient_name,
    pr.name       AS provider_name
FROM core.documents d
LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
LEFT JOIN legal.cases          lc  ON lcd.case_id = lc.id
LEFT JOIN legal.email_metadata em  ON d.id = em.document_id
LEFT JOIN medical.record_metadata rm ON d.id = rm.document_id
LEFT JOIN medical.patients     p   ON rm.patient_id = p.id
LEFT JOIN medical.providers    pr  ON rm.provider_id = pr.id;

-- Chunk search view — chunk + parent document context
CREATE OR REPLACE VIEW core.searchable_chunks AS
SELECT
    c.id            AS chunk_id,
    c.document_id,
    c.chunk_index,
    c.total_chunks,
    c.content,
    c.embedding,
    c.content_tsv,
    d.domain,
    d.document_type,
    d.title,
    d.filename,
    d.source_path,
    d.metadata      AS document_metadata,
    c.metadata      AS chunk_metadata
FROM core.document_chunks c
JOIN core.documents d ON c.document_id = d.id;

-- ============================================================
-- FUNCTIONS — Enterprise search & RAG
-- ============================================================

-- 1. Semantic search with domain / type / case filtering
CREATE OR REPLACE FUNCTION core.semantic_search(
    query_embedding     halfvec(3072),
    match_count         INT   DEFAULT 10,
    domain_filter       TEXT  DEFAULT NULL,
    doc_type_filter     TEXT  DEFAULT NULL,
    case_number_filter  TEXT  DEFAULT NULL,
    similarity_threshold FLOAT DEFAULT 0.0
) RETURNS TABLE (
    chunk_id      TEXT,
    document_id   UUID,
    content       TEXT,
    similarity    FLOAT,
    domain        TEXT,
    document_type TEXT,
    title         TEXT,
    filename      TEXT,
    chunk_index   INT,
    total_chunks  INT,
    metadata      JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id,
        c.document_id,
        c.content,
        (1 - (c.embedding <=> query_embedding))::FLOAT,
        d.domain,
        d.document_type,
        d.title,
        d.filename,
        c.chunk_index,
        c.total_chunks,
        c.metadata
    FROM core.document_chunks c
    JOIN core.documents d ON c.document_id = d.id
    LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
    LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
    WHERE c.embedding IS NOT NULL
      AND (domain_filter       IS NULL OR d.domain        = domain_filter)
      AND (doc_type_filter     IS NULL OR d.document_type = doc_type_filter)
      AND (case_number_filter  IS NULL OR lc.case_number  = case_number_filter)
      AND (1 - (c.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY c.embedding <=> query_embedding
    LIMIT match_count;
END;
$$ LANGUAGE plpgsql;

-- 2. Full-text search with headline generation
CREATE OR REPLACE FUNCTION core.fulltext_search(
    query_text    TEXT,
    match_count   INT  DEFAULT 10,
    domain_filter TEXT DEFAULT NULL
) RETURNS TABLE (
    chunk_id      TEXT,
    document_id   UUID,
    content       TEXT,
    rank          FLOAT,
    domain        TEXT,
    document_type TEXT,
    title         TEXT,
    filename      TEXT,
    headline      TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id,
        c.document_id,
        c.content,
        ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', query_text))::FLOAT,
        d.domain,
        d.document_type,
        d.title,
        d.filename,
        ts_headline('english', c.content,
            websearch_to_tsquery('english', query_text),
            'MaxWords=60, MinWords=20, StartSel=**, StopSel=**')
    FROM core.document_chunks c
    JOIN core.documents d ON c.document_id = d.id
    WHERE c.content_tsv @@ websearch_to_tsquery('english', query_text)
      AND (domain_filter IS NULL OR d.domain = domain_filter)
    ORDER BY ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', query_text)) DESC
    LIMIT match_count;
END;
$$ LANGUAGE plpgsql;

-- 3. Hybrid search — Reciprocal Rank Fusion (semantic + full-text)
CREATE OR REPLACE FUNCTION core.hybrid_search(
    query_text      TEXT,
    query_embedding halfvec(3072),
    match_count     INT   DEFAULT 10,
    domain_filter   TEXT  DEFAULT NULL,
    semantic_weight FLOAT DEFAULT 0.7,
    fulltext_weight FLOAT DEFAULT 0.3,
    rrf_k           INT   DEFAULT 60
) RETURNS TABLE (
    chunk_id       TEXT,
    document_id    UUID,
    content        TEXT,
    hybrid_score   FLOAT,
    semantic_score FLOAT,
    fulltext_score FLOAT,
    domain         TEXT,
    document_type  TEXT,
    title          TEXT,
    filename       TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH semantic AS (
        SELECT
            c.id AS cid, c.document_id AS did, c.content AS cnt,
            (1 - (c.embedding <=> query_embedding))::FLOAT AS score,
            ROW_NUMBER() OVER (ORDER BY c.embedding <=> query_embedding) AS rnk,
            d.domain AS dom, d.document_type AS dtype, d.title AS ttl, d.filename AS fname
        FROM core.document_chunks c
        JOIN core.documents d ON c.document_id = d.id
        WHERE c.embedding IS NOT NULL
          AND (domain_filter IS NULL OR d.domain = domain_filter)
        ORDER BY c.embedding <=> query_embedding
        LIMIT match_count * 4
    ),
    fulltext AS (
        SELECT
            c.id AS cid, c.document_id AS did, c.content AS cnt,
            ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', query_text))::FLOAT AS score,
            ROW_NUMBER() OVER (
                ORDER BY ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', query_text)) DESC
            ) AS rnk,
            d.domain AS dom, d.document_type AS dtype, d.title AS ttl, d.filename AS fname
        FROM core.document_chunks c
        JOIN core.documents d ON c.document_id = d.id
        WHERE c.content_tsv @@ websearch_to_tsquery('english', query_text)
          AND (domain_filter IS NULL OR d.domain = domain_filter)
        ORDER BY ts_rank_cd(c.content_tsv, websearch_to_tsquery('english', query_text)) DESC
        LIMIT match_count * 4
    ),
    combined AS (
        SELECT
            COALESCE(s.cid, f.cid)     AS cid,
            COALESCE(s.did, f.did)     AS did,
            COALESCE(s.cnt, f.cnt)     AS cnt,
            COALESCE(s.score, 0)::FLOAT AS sem,
            COALESCE(f.score, 0)::FLOAT AS ft,
            (  semantic_weight * COALESCE(1.0 / (rrf_k + s.rnk), 0)
             + fulltext_weight * COALESCE(1.0 / (rrf_k + f.rnk), 0)
            )::FLOAT                    AS rrf,
            COALESCE(s.dom,   f.dom)   AS dom,
            COALESCE(s.dtype, f.dtype) AS dtype,
            COALESCE(s.ttl,   f.ttl)   AS ttl,
            COALESCE(s.fname, f.fname) AS fname
        FROM semantic s
        FULL OUTER JOIN fulltext f ON s.cid = f.cid
    )
    SELECT combined.cid, combined.did, combined.cnt,
           combined.rrf, combined.sem, combined.ft,
           combined.dom, combined.dtype, combined.ttl, combined.fname
    FROM combined
    ORDER BY combined.rrf DESC
    LIMIT match_count;
END;
$$ LANGUAGE plpgsql;

-- 4. Context expansion — parent document with all chunks
CREATE OR REPLACE FUNCTION core.get_document_with_chunks(doc_id UUID)
RETURNS TABLE (
    document_id   UUID,
    domain        TEXT,
    document_type TEXT,
    title         TEXT,
    filename      TEXT,
    chunk_id      TEXT,
    chunk_index   INT,
    total_chunks  INT,
    content       TEXT,
    metadata      JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT d.id, d.domain, d.document_type, d.title, d.filename,
           c.id, c.chunk_index, c.total_chunks, c.content, c.metadata
    FROM core.documents d
    JOIN core.document_chunks c ON d.id = c.document_id
    WHERE d.id = doc_id
    ORDER BY c.chunk_index;
END;
$$ LANGUAGE plpgsql;

-- 5. Context window — surrounding chunks for a matched chunk
CREATE OR REPLACE FUNCTION core.get_surrounding_chunks(
    target_chunk_id TEXT,
    window_size     INT DEFAULT 1
) RETURNS TABLE (
    chunk_id    TEXT,
    document_id UUID,
    chunk_index INT,
    content     TEXT,
    is_target   BOOLEAN
) AS $$
DECLARE
    v_doc_id UUID;
    v_index  INT;
BEGIN
    SELECT c.document_id, c.chunk_index INTO v_doc_id, v_index
    FROM core.document_chunks c WHERE c.id = target_chunk_id;

    RETURN QUERY
    SELECT c.id, c.document_id, c.chunk_index, c.content,
           (c.id = target_chunk_id)
    FROM core.document_chunks c
    WHERE c.document_id = v_doc_id
      AND c.chunk_index BETWEEN (v_index - window_size) AND (v_index + window_size)
    ORDER BY c.chunk_index;
END;
$$ LANGUAGE plpgsql;

-- 6. Cross-domain entity search
CREATE OR REPLACE FUNCTION core.find_entity_across_domains(
    search_name       TEXT,
    entity_type_filter TEXT DEFAULT NULL
) RETURNS TABLE (
    entity_id     INT,
    entity_name   TEXT,
    entity_type   TEXT,
    domain        TEXT,
    document_count BIGINT,
    mention_count  BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT e.id, e.name, e.entity_type, d.domain,
           COUNT(DISTINCT em.document_id), COUNT(em.id)
    FROM core.entities e
    JOIN core.entity_mentions em ON e.id = em.entity_id
    JOIN core.documents d ON em.document_id = d.id
    WHERE (e.name ILIKE '%' || search_name || '%'
           OR e.normalized_name ILIKE '%' || lower(search_name) || '%')
      AND (entity_type_filter IS NULL OR e.entity_type = entity_type_filter)
    GROUP BY e.id, e.name, e.entity_type, d.domain
    ORDER BY COUNT(em.id) DESC;
END;
$$ LANGUAGE plpgsql;

-- 7. System statistics for dashboards
CREATE OR REPLACE FUNCTION ops.get_system_stats()
RETURNS JSONB AS $$
DECLARE
    result JSONB;
BEGIN
    SELECT jsonb_build_object(
        'total_documents',        (SELECT COUNT(*) FROM core.documents),
        'total_chunks',           (SELECT COUNT(*) FROM core.document_chunks),
        'chunks_with_embeddings', (SELECT COUNT(*) FROM core.document_chunks WHERE embedding IS NOT NULL),
        'domains', (
            SELECT COALESCE(jsonb_object_agg(domain, cnt), '{}'::jsonb)
            FROM (SELECT domain, COUNT(*) AS cnt FROM core.documents GROUP BY domain) s
        ),
        'document_types', (
            SELECT COALESCE(jsonb_object_agg(document_type, cnt), '{}'::jsonb)
            FROM (SELECT document_type, COUNT(*) AS cnt FROM core.documents GROUP BY document_type) s
        ),
        'legal_cases',     (SELECT COUNT(*) FROM legal.cases),
        'legal_parties',   (SELECT COUNT(*) FROM legal.parties),
        'entities',        (SELECT COUNT(*) FROM core.entities),
        'patients',        (SELECT COUNT(*) FROM medical.patients),
        'providers',       (SELECT COUNT(*) FROM medical.providers),
        'ingestion_jobs',  (SELECT COUNT(*) FROM ops.ingestion_jobs),
        'recent_searches', (SELECT COUNT(*) FROM ops.search_analytics
                            WHERE timestamp > now() - interval '24 hours'),
        'recent_access',   (SELECT COUNT(*) FROM ops.access_log
                            WHERE timestamp > now() - interval '24 hours')
    ) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- 8. AI Agent self-discovery — returns complete schema map
CREATE OR REPLACE FUNCTION ops.discover_schema()
RETURNS JSONB AS $$
SELECT jsonb_build_object(
    'name',        'WDWS Enterprise Database',
    'version',     '1.0',
    'description', 'Multi-domain legal, medical, and document management platform with AI-powered semantic, full-text, and hybrid search.',
    'schemas', jsonb_build_object(
        'core',      'Universal document → chunk hierarchy with halfvec(3072) embeddings, entity graph, and relationship edges',
        'legal',     'Court case registry, party management, email metadata, filing enrichment',
        'medical',   'Patient/provider registry, medical record enrichment',
        'paperless', 'Paperless-ngx document management integration',
        'ops',       'AI agent registry, session tracking, access audit log, OAuth, ingestion jobs, search analytics'
    ),
    'search_functions', jsonb_build_array(
        jsonb_build_object('name', 'core.semantic_search',   'args', 'query_embedding halfvec, match_count int, domain_filter text, doc_type_filter text, case_number_filter text, similarity_threshold float', 'description', 'Vector cosine similarity search across all chunks'),
        jsonb_build_object('name', 'core.fulltext_search',   'args', 'query_text text, match_count int, domain_filter text', 'description', 'PostgreSQL full-text search with ts_headline'),
        jsonb_build_object('name', 'core.hybrid_search',     'args', 'query_text text, query_embedding halfvec, match_count int, domain_filter text, semantic_weight float, fulltext_weight float', 'description', 'Reciprocal Rank Fusion combining semantic + full-text'),
        jsonb_build_object('name', 'core.get_document_with_chunks', 'args', 'doc_id uuid', 'description', 'Returns full document with all chunks ordered'),
        jsonb_build_object('name', 'core.get_surrounding_chunks',   'args', 'chunk_id text, window int', 'description', 'Context window expansion around a matched chunk'),
        jsonb_build_object('name', 'core.find_entity_across_domains', 'args', 'name text, type text', 'description', 'Cross-domain entity search with mention counts'),
        jsonb_build_object('name', 'ops.get_system_stats',   'args', 'none', 'description', 'Dashboard statistics as JSONB'),
        jsonb_build_object('name', 'ops.discover_schema',    'args', 'none', 'description', 'This function — returns complete schema map for AI agent self-discovery')
    ),
    'embedding_model', jsonb_build_object(
        'name', 'text-embedding-3-large',
        'provider', 'openai',
        'dimensions', 3072,
        'storage_type', 'halfvec(3072)',
        'index_type', 'HNSW with halfvec_cosine_ops'
    ),
    'domains', jsonb_build_object(
        'legal',     jsonb_build_object('documents', (SELECT COUNT(*) FROM core.documents WHERE domain = 'legal'),
                                        'chunks',    (SELECT COUNT(*) FROM core.document_chunks c JOIN core.documents d ON c.document_id = d.id WHERE d.domain = 'legal'),
                                        'cases',     (SELECT COUNT(*) FROM legal.cases)),
        'medical',   jsonb_build_object('documents', (SELECT COUNT(*) FROM core.documents WHERE domain = 'medical'),
                                        'chunks',    (SELECT COUNT(*) FROM core.document_chunks c JOIN core.documents d ON c.document_id = d.id WHERE d.domain = 'medical')),
        'paperless', jsonb_build_object('documents', (SELECT COUNT(*) FROM core.documents WHERE domain = 'paperless'),
                                        'chunks',    (SELECT COUNT(*) FROM core.document_chunks c JOIN core.documents d ON c.document_id = d.id WHERE d.domain = 'paperless'))
    )
);
$$ LANGUAGE sql;
