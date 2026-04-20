-- Migration 018: Core personal memories + BGE-M3 alignment
-- Athena Cognitive Engine
-- Date: 2026-04-17
--
-- Aligns the legacy core.memories table in wdws with the current BGE-M3
-- embedding stack while preserving the live integer primary key and existing
-- compatibility columns used by older Athena flows.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

INSERT INTO core.embedding_models (name, provider, dimensions)
VALUES ('BAAI/bge-m3', 'local', 1024)
ON CONFLICT (name) DO UPDATE SET
    provider = EXCLUDED.provider,
    dimensions = EXCLUDED.dimensions;

CREATE TABLE IF NOT EXISTS core.memories (
    id                 SERIAL PRIMARY KEY,
    memory_type        TEXT NOT NULL DEFAULT 'fact',
    content            TEXT NOT NULL DEFAULT '',
    source             TEXT NOT NULL DEFAULT 'unknown',
    source_id          TEXT,
    importance         DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    embedding          halfvec(1024),
    embedding_model_id INT REFERENCES core.embedding_models(id) ON DELETE SET NULL,
    metadata           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_accessed      TIMESTAMPTZ,
    access_count       INT NOT NULL DEFAULT 0,
    is_active          BOOLEAN NOT NULL DEFAULT true,
    user_id            INT
);

ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS memory_type TEXT;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS content TEXT;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS source_id TEXT;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS importance DOUBLE PRECISION;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS embedding halfvec(1024);
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS embedding_model_id INT REFERENCES core.embedding_models(id) ON DELETE SET NULL;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS last_accessed TIMESTAMPTZ;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS access_count INT;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS is_active BOOLEAN;
ALTER TABLE core.memories ADD COLUMN IF NOT EXISTS user_id INT;

ALTER TABLE core.memories
    ALTER COLUMN importance TYPE DOUBLE PRECISION USING importance::double precision;

UPDATE core.memories
SET memory_type = 'fact'
WHERE memory_type IS NULL OR btrim(memory_type) = '';

UPDATE core.memories
SET content = ''
WHERE content IS NULL;

UPDATE core.memories
SET source = 'unknown'
WHERE source IS NULL OR btrim(source) = '';

UPDATE core.memories
SET importance = 0.5
WHERE importance IS NULL;

UPDATE core.memories
SET metadata = '{}'::jsonb
WHERE metadata IS NULL;

UPDATE core.memories
SET created_at = now()
WHERE created_at IS NULL;

UPDATE core.memories
SET updated_at = COALESCE(updated_at, created_at, now())
WHERE updated_at IS NULL;

UPDATE core.memories
SET access_count = 0
WHERE access_count IS NULL;

UPDATE core.memories
SET is_active = true
WHERE is_active IS NULL;

ALTER TABLE core.memories ALTER COLUMN memory_type SET DEFAULT 'fact';
ALTER TABLE core.memories ALTER COLUMN content SET DEFAULT '';
ALTER TABLE core.memories ALTER COLUMN source SET DEFAULT 'unknown';
ALTER TABLE core.memories ALTER COLUMN importance SET DEFAULT 0.5;
ALTER TABLE core.memories ALTER COLUMN metadata SET DEFAULT '{}'::jsonb;
ALTER TABLE core.memories ALTER COLUMN created_at SET DEFAULT now();
ALTER TABLE core.memories ALTER COLUMN updated_at SET DEFAULT now();
ALTER TABLE core.memories ALTER COLUMN access_count SET DEFAULT 0;
ALTER TABLE core.memories ALTER COLUMN is_active SET DEFAULT true;
ALTER TABLE core.memories ALTER COLUMN embedding_model_id DROP DEFAULT;

ALTER TABLE core.memories ALTER COLUMN memory_type SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN content SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN source SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN importance SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN metadata SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN created_at SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN access_count SET NOT NULL;
ALTER TABLE core.memories ALTER COLUMN is_active SET NOT NULL;

COMMENT ON TABLE core.memories IS
    'Athena long-term personal memory store for semantically searchable facts, decisions, preferences, and system context rows.';
COMMENT ON COLUMN core.memories.embedding IS
    'BGE-M3 dense embedding (halfvec(1024)) for semantic memory recall. Null for non-semantic rows such as daily briefings.';
COMMENT ON COLUMN core.memories.embedding_model_id IS
    'Embedding provenance. Null when no embedding is stored for a row.';
COMMENT ON COLUMN core.memories.metadata IS
    'Structured attributes for memory rows, including embedding provenance and system-generation hints.';

DROP INDEX IF EXISTS core.idx_memories_embedding;
DROP INDEX IF EXISTS core.idx_memories_embedding_hnsw;

DO $$
DECLARE
    embedding_type TEXT;
BEGIN
    SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
      INTO embedding_type
      FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'core'
       AND c.relname = 'memories'
       AND a.attname = 'embedding'
       AND NOT a.attisdropped;

    IF embedding_type IS NULL THEN
        EXECUTE 'ALTER TABLE core.memories ADD COLUMN embedding halfvec(1024)';
    ELSIF embedding_type <> 'halfvec(1024)' THEN
        EXECUTE 'ALTER TABLE core.memories ALTER COLUMN embedding TYPE halfvec(1024) USING NULL';
    END IF;
END $$;

UPDATE core.memories
SET embedding_model_id = NULL
WHERE embedding IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'core'
          AND rel.relname = 'memories'
          AND con.contype = 'p'
    ) THEN
        EXECUTE 'ALTER TABLE core.memories ADD CONSTRAINT memories_pkey PRIMARY KEY (id)';
    END IF;
END $$;

ALTER TABLE core.memories DROP CONSTRAINT IF EXISTS memories_embedding_model_id_fkey;
ALTER TABLE core.memories
    ADD CONSTRAINT memories_embedding_model_id_fkey
    FOREIGN KEY (embedding_model_id) REFERENCES core.embedding_models(id) ON DELETE SET NULL;

ALTER TABLE core.memories DROP CONSTRAINT IF EXISTS memories_importance_range;
ALTER TABLE core.memories
    ADD CONSTRAINT memories_importance_range
    CHECK (importance >= 0.0 AND importance <= 1.0);

ALTER TABLE core.memories DROP CONSTRAINT IF EXISTS memories_embedding_model_pair;
ALTER TABLE core.memories
    ADD CONSTRAINT memories_embedding_model_pair
    CHECK (
        (embedding IS NULL AND embedding_model_id IS NULL)
        OR (embedding IS NOT NULL AND embedding_model_id IS NOT NULL)
    );

DROP TRIGGER IF EXISTS trg_memories_updated ON core.memories;
CREATE TRIGGER trg_memories_updated
    BEFORE UPDATE ON core.memories
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_memories_active_created_at
    ON core.memories (is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_memories_source_created_at
    ON core.memories (source, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_memories_type_created_at
    ON core.memories (memory_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_memories_embedding_hnsw
    ON core.memories
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200);

COMMIT;
