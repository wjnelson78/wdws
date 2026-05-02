-- 033_agent_notes_embeddings.sql
-- Adds vector embeddings to ops.agent_notes so claude.ai can search agent
-- notes by meaning, not just by exact key/title. Matches the dimensionality
-- and indexing strategy used by core.memories (halfvec(1024), HNSW cosine).

BEGIN;

ALTER TABLE ops.agent_notes
    ADD COLUMN IF NOT EXISTS embedding         halfvec(1024),
    ADD COLUMN IF NOT EXISTS embedding_model_id integer
        REFERENCES core.embedding_models(id) ON DELETE SET NULL;

ALTER TABLE ops.agent_notes
    DROP CONSTRAINT IF EXISTS agent_notes_embedding_model_pair;

ALTER TABLE ops.agent_notes
    ADD CONSTRAINT agent_notes_embedding_model_pair
    CHECK (
        (embedding IS NULL AND embedding_model_id IS NULL)
        OR (embedding IS NOT NULL AND embedding_model_id IS NOT NULL)
    );

CREATE INDEX IF NOT EXISTS idx_agent_notes_embedding_hnsw
    ON ops.agent_notes
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 200);

COMMIT;
