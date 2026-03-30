-- ============================================================
-- Space Knowledge Base: RAG with Isolated Semantic Search
-- Migration: 008_space_knowledge_base.sql
-- ============================================================

-- ============================================================
-- SPACE DOCUMENTS (Core content storage)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_documents (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    space_id          UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    source_type       TEXT NOT NULL CHECK (source_type IN ('email', 'file', 'note', 'url', 'attachment', 'case', 'medical_record', 'other')),
    title             TEXT NOT NULL,
    content_text      TEXT,
    content_html      TEXT,
    summary           TEXT,
    
    -- External source tracking
    source_link_id    BIGINT REFERENCES chat.space_links(id) ON DELETE SET NULL,
    source_system     TEXT,
    source_id         TEXT,
    source_uri        TEXT,
    
    -- File metadata
    file_name         TEXT,
    file_path         TEXT,
    content_type      TEXT,
    size_bytes        BIGINT,
    sha256            TEXT,
    
    -- Author/sender info
    author_name       TEXT,
    author_email      TEXT,
    sender_name       TEXT,
    sender_email      TEXT,
    
    -- Timestamps
    document_date     TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now(),
    last_indexed_at   TIMESTAMPTZ,
    
    -- Status and processing
    status            TEXT NOT NULL DEFAULT 'pending' 
                      CHECK (status IN ('pending', 'processing', 'indexed', 'failed')),
    processing_error  TEXT,
    
    -- Metadata and tags
    metadata          JSONB DEFAULT '{}'::jsonb,
    tags              TEXT[],
    
    created_by        BIGINT REFERENCES chat.users(id) ON DELETE SET NULL
);

-- Full-text search index (instead of generated column)
CREATE INDEX IF NOT EXISTS idx_space_documents_content_tsv 
    ON chat.space_documents USING GIN(
        to_tsvector('english',
            coalesce(title, '') || ' ' ||
            coalesce(content_text, '') || ' ' ||
            coalesce(author_name, '') || ' ' ||
            coalesce(array_to_string(tags, ' '), '')
        )
    );

CREATE INDEX IF NOT EXISTS idx_space_documents_space 
    ON chat.space_documents(space_id);
CREATE INDEX IF NOT EXISTS idx_space_documents_source_type 
    ON chat.space_documents(source_type);
CREATE INDEX IF NOT EXISTS idx_space_documents_status 
    ON chat.space_documents(status);
CREATE INDEX IF NOT EXISTS idx_space_documents_document_date 
    ON chat.space_documents(document_date DESC);
CREATE INDEX IF NOT EXISTS idx_space_documents_content_tsv 
    ON chat.space_documents USING GIN(content_tsv);
CREATE INDEX IF NOT EXISTS idx_space_documents_tags 
    ON chat.space_documents USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_space_documents_metadata 
    ON chat.space_documents USING GIN(metadata);

CREATE TRIGGER trg_space_documents_updated
    BEFORE UPDATE ON chat.space_documents
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

-- ============================================================
-- DOCUMENT CHUNKS (Chunked content for better embeddings)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_document_chunks (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id         UUID NOT NULL REFERENCES chat.space_documents(id) ON DELETE CASCADE,
    space_id            UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    chunk_index         INT NOT NULL,
    chunk_text          TEXT NOT NULL,
    chunk_summary       TEXT,
    
    -- Chunk metadata
    start_offset        INT,
    end_offset          INT,
    token_count         INT,
    
    -- Hierarchy (for structured documents)
    parent_chunk_id     UUID REFERENCES chat.space_document_chunks(id) ON DELETE SET NULL,
    section_title       TEXT,
    section_path        TEXT[],
    
    metadata            JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_space_document_chunks_document 
    ON chat.space_document_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_space_document_chunks_space 
    ON chat.space_document_chunks(space_id);

-- ============================================================
-- DOCUMENT EMBEDDINGS (pgvector semantic search)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_document_embeddings (
    chunk_id            UUID PRIMARY KEY REFERENCES chat.space_document_chunks(id) ON DELETE CASCADE,
    space_id            UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    embedding           halfvec(3072) NOT NULL,
    embedding_model_id  INT REFERENCES chat.embedding_models(id) DEFAULT 1,
    created_at          TIMESTAMPTZ DEFAULT now()
);

-- HNSW index for fast approximate nearest neighbor search
CREATE INDEX IF NOT EXISTS idx_space_document_embeddings_hnsw
    ON chat.space_document_embeddings 
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Space isolation index
CREATE INDEX IF NOT EXISTS idx_space_document_embeddings_space
    ON chat.space_document_embeddings(space_id);

-- ============================================================
-- SPACE NOTES (Quick notes within a space)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_notes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    space_id        UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    title           TEXT,
    content         TEXT NOT NULL,
    format          TEXT NOT NULL DEFAULT 'markdown' CHECK (format IN ('markdown', 'html', 'plain')),
    
    -- Organization
    tags            TEXT[],
    color           TEXT,
    is_pinned       BOOLEAN DEFAULT false,
    
    -- Collaboration
    created_by      BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    updated_by      BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    
    -- Timestamps
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Full-text search index (instead of generated column)
CREATE INDEX IF NOT EXISTS idx_space_notes_content_tsv 
    ON chat.space_notes USING GIN(
        to_tsvector('english',
            coalesce(title, '') || ' ' ||
            coalesce(content, '') || ' ' ||
            coalesce(array_to_string(tags, ' '), '')
        )
    );

CREATE INDEX IF NOT EXISTS idx_space_notes_space 
    ON chat.space_notes(space_id);
CREATE INDEX IF NOT EXISTS idx_space_notes_created_by 
    ON chat.space_notes(created_by);
CREATE INDEX IF NOT EXISTS idx_space_notes_content_tsv 
    ON chat.space_notes USING GIN(content_tsv);
CREATE INDEX IF NOT EXISTS idx_space_notes_tags 
    ON chat.space_notes USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_space_notes_pinned 
    ON chat.space_notes(space_id, is_pinned) WHERE is_pinned = true;

CREATE TRIGGER trg_space_notes_updated
    BEFORE UPDATE ON chat.space_notes
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

-- ============================================================
-- SEARCH HISTORY (Track searches for optimization)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_searches (
    id                  BIGSERIAL PRIMARY KEY,
    space_id            UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    user_id             BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    query               TEXT NOT NULL,
    search_type         TEXT NOT NULL CHECK (search_type IN ('semantic', 'fulltext', 'hybrid')),
    
    -- Results
    result_count        INT,
    top_result_id       UUID,
    
    -- Performance
    execution_time_ms   INT,
    
    -- Feedback
    was_helpful         BOOLEAN,
    feedback_text       TEXT,
    
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_space_searches_space 
    ON chat.space_searches(space_id);
CREATE INDEX IF NOT EXISTS idx_space_searches_user 
    ON chat.space_searches(user_id);
CREATE INDEX IF NOT EXISTS idx_space_searches_created 
    ON chat.space_searches(created_at DESC);

-- ============================================================
-- CONVENIENCE VIEWS
-- ============================================================

-- View: All content in a space (documents + notes)
CREATE OR REPLACE VIEW chat.v_space_content AS
SELECT
    d.id,
    d.space_id,
    'document' AS content_type,
    d.source_type,
    d.title,
    d.content_text AS content,
    d.author_name,
    d.document_date AS content_date,
    d.created_at,
    d.tags,
    d.metadata
FROM chat.space_documents d
WHERE d.status = 'indexed'
UNION ALL
SELECT
    n.id,
    n.space_id,
    'note' AS content_type,
    'note' AS source_type,
    n.title,
    n.content,
    u.display_name AS author_name,
    n.created_at AS content_date,
    n.created_at,
    n.tags,
    '{}'::jsonb AS metadata
FROM chat.space_notes n
LEFT JOIN chat.users u ON n.created_by = u.id;

-- View: Document processing stats per space
CREATE OR REPLACE VIEW chat.v_space_document_stats AS
SELECT
    space_id,
    COUNT(*) AS total_documents,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
    COUNT(*) FILTER (WHERE status = 'processing') AS processing,
    COUNT(*) FILTER (WHERE status = 'indexed') AS indexed,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
    COUNT(DISTINCT source_type) AS source_types,
    SUM(size_bytes) AS total_size_bytes,
    MAX(created_at) AS last_added,
    MAX(last_indexed_at) AS last_indexed
FROM chat.space_documents
GROUP BY space_id;
