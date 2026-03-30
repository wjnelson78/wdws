-- ============================================================
-- Athena Chat / Memory Database Schema v1.0
-- Database: athena_chat (separate from WDWS MCP database)
-- PostgreSQL 17 + pgvector 0.8.0
--
-- Concepts:
--   • Spaces (NOT projects) for grouping conversations
--   • Conversations + messages (multi-modal content)
--   • Assets (images/files) stored on disk with DB metadata
--   • Web search tracking (Tavily)
--   • Memory store (user/space/conversation/global) with embeddings
--   • Cross-system links (emails, documents, etc.)
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS vector;      -- pgvector (halfvec, hnsw)
CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- trigram fuzzy matching
CREATE EXTENSION IF NOT EXISTS pgcrypto;    -- gen_random_uuid()

-- Schema
CREATE SCHEMA IF NOT EXISTS chat;

-- ============================================================
-- UTILITY
-- ============================================================
CREATE OR REPLACE FUNCTION chat.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- EMBEDDING MODEL REGISTRY
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.embedding_models (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    provider    TEXT NOT NULL,
    dimensions  INT  NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

INSERT INTO chat.embedding_models (name, provider, dimensions)
VALUES ('text-embedding-3-large', 'openai', 3072)
ON CONFLICT (name) DO NOTHING;

-- ============================================================
-- USERS (Azure AD identities)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.users (
    id            BIGSERIAL PRIMARY KEY,
    aad_oid       TEXT NOT NULL UNIQUE,
    tenant_id     TEXT,
    email         TEXT,
    display_name  TEXT,
    created_at    TIMESTAMPTZ DEFAULT now(),
    last_seen_at  TIMESTAMPTZ
);

-- ============================================================
-- SPACES (aka Projects)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.spaces (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    description TEXT,
    created_by  BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    is_private  BOOLEAN DEFAULT true,
    metadata    JSONB DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TRIGGER trg_spaces_updated
    BEFORE UPDATE ON chat.spaces
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

CREATE TABLE IF NOT EXISTS chat.space_members (
    space_id     UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    user_id      BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    role         TEXT NOT NULL DEFAULT 'member'
                 CHECK (role IN ('owner', 'admin', 'member', 'viewer')),
    is_favorite  BOOLEAN DEFAULT false,
    joined_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (space_id, user_id)
);

-- Links to external systems (emails, documents, attachments, etc.)
CREATE TABLE IF NOT EXISTS chat.space_links (
    id            BIGSERIAL PRIMARY KEY,
    space_id      UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    link_type     TEXT NOT NULL,          -- email, document, attachment, case, url, other
    source_system TEXT,                   -- wdws, m365, external
    source_db     TEXT,
    source_table  TEXT,
    source_id     TEXT,
    source_uri    TEXT,
    title         TEXT,
    metadata      JSONB DEFAULT '{}'::jsonb,
    created_by    BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- CONVERSATIONS + MESSAGES
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.conversations (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    space_id      UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    title         TEXT,
    system_prompt TEXT,
    model         TEXT,
    status        TEXT NOT NULL DEFAULT 'active'
                  CHECK (status IN ('active', 'archived', 'deleted')),
    created_by    BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    metadata      JSONB DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TRIGGER trg_conversations_updated
    BEFORE UPDATE ON chat.conversations
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

CREATE TABLE IF NOT EXISTS chat.messages (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES chat.conversations(id) ON DELETE CASCADE,
    role           TEXT NOT NULL CHECK (role IN ('system', 'user', 'assistant', 'tool')),
    content_text   TEXT,
    content_json   JSONB DEFAULT '{}'::jsonb,  -- multi-modal payload (images, tool calls, etc.)
    tool_name      TEXT,
    tool_call_id   TEXT,
    token_count    INT,
    created_by     BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    parent_id      UUID REFERENCES chat.messages(id) ON DELETE SET NULL,
    ordinal        BIGSERIAL,
    created_at     TIMESTAMPTZ DEFAULT now(),
    content_tsv    TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(content_text, ''))
    ) STORED
);

CREATE TABLE IF NOT EXISTS chat.message_embeddings (
    message_id        UUID PRIMARY KEY REFERENCES chat.messages(id) ON DELETE CASCADE,
    embedding         halfvec(3072),
    embedding_model_id INT REFERENCES chat.embedding_models(id) DEFAULT 1,
    created_at        TIMESTAMPTZ DEFAULT now()
);

-- ============================================================
-- ASSETS (Images / Files)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.assets (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_type   TEXT NOT NULL CHECK (asset_type IN ('image', 'file', 'audio', 'video', 'other')),
    storage_path TEXT NOT NULL,
    content_type TEXT,
    file_name    TEXT,
    sha256       TEXT,
    size_bytes   BIGINT,
    width        INT,
    height       INT,
    duration_ms  INT,
    metadata     JSONB DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chat.message_assets (
    message_id UUID NOT NULL REFERENCES chat.messages(id) ON DELETE CASCADE,
    asset_id   UUID NOT NULL REFERENCES chat.assets(id) ON DELETE CASCADE,
    PRIMARY KEY (message_id, asset_id)
);

-- ============================================================
-- WEB SEARCH (Tavily)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.web_searches (
    id               BIGSERIAL PRIMARY KEY,
    conversation_id  UUID NOT NULL REFERENCES chat.conversations(id) ON DELETE CASCADE,
    message_id       UUID REFERENCES chat.messages(id) ON DELETE SET NULL,
    provider         TEXT NOT NULL DEFAULT 'tavily',
    query            TEXT NOT NULL,
    search_depth     TEXT,
    result_count     INT,
    response_time_ms INT,
    raw_response     JSONB DEFAULT '{}'::jsonb,
    created_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chat.web_search_results (
    search_id  BIGINT NOT NULL REFERENCES chat.web_searches(id) ON DELETE CASCADE,
    rank       INT NOT NULL,
    title      TEXT,
    url        TEXT,
    snippet    TEXT,
    content    TEXT,
    metadata   JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (search_id, rank)
);

-- ============================================================
-- MEMORY STORE
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.memories (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope            TEXT NOT NULL CHECK (scope IN ('global', 'user', 'space', 'conversation')),
    user_id          BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    space_id         UUID REFERENCES chat.spaces(id) ON DELETE CASCADE,
    conversation_id  UUID REFERENCES chat.conversations(id) ON DELETE CASCADE,
    key              TEXT NOT NULL,
    value            JSONB NOT NULL,
    tags             TEXT[] DEFAULT '{}',
    source_message_id UUID REFERENCES chat.messages(id) ON DELETE SET NULL,
    embedding        halfvec(3072),
    embedding_model_id INT REFERENCES chat.embedding_models(id) DEFAULT 1,
    is_active        BOOLEAN DEFAULT true,
    created_at       TIMESTAMPTZ DEFAULT now(),
    updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TRIGGER trg_memories_updated
    BEFORE UPDATE ON chat.memories
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

-- ============================================================
-- INDEXES
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_spaces_created_by ON chat.spaces (created_by);
CREATE INDEX IF NOT EXISTS idx_space_members_user ON chat.space_members (user_id);
CREATE INDEX IF NOT EXISTS idx_space_links_space ON chat.space_links (space_id);

CREATE INDEX IF NOT EXISTS idx_conversations_space ON chat.conversations (space_id);
CREATE INDEX IF NOT EXISTS idx_messages_convo ON chat.messages (conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_role ON chat.messages (role);
CREATE INDEX IF NOT EXISTS idx_messages_text ON chat.messages USING GIN (content_tsv);

CREATE INDEX IF NOT EXISTS idx_assets_sha ON chat.assets (sha256);
CREATE INDEX IF NOT EXISTS idx_message_assets_asset ON chat.message_assets (asset_id);

CREATE INDEX IF NOT EXISTS idx_msg_embeddings_hnsw ON chat.message_embeddings
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200);

CREATE INDEX IF NOT EXISTS idx_searches_convo ON chat.web_searches (conversation_id);
CREATE INDEX IF NOT EXISTS idx_searches_message ON chat.web_searches (message_id);

CREATE INDEX IF NOT EXISTS idx_memories_scope ON chat.memories (scope);
CREATE INDEX IF NOT EXISTS idx_memories_space ON chat.memories (space_id);
CREATE INDEX IF NOT EXISTS idx_memories_convo ON chat.memories (conversation_id);
CREATE INDEX IF NOT EXISTS idx_memories_tags ON chat.memories USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_memories_embedding ON chat.memories
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200);
