-- 033_agent_notes_embeddings_down.sql

BEGIN;

DROP INDEX IF EXISTS ops.idx_agent_notes_embedding_hnsw;

ALTER TABLE ops.agent_notes
    DROP CONSTRAINT IF EXISTS agent_notes_embedding_model_pair;

ALTER TABLE ops.agent_notes
    DROP COLUMN IF EXISTS embedding_model_id,
    DROP COLUMN IF EXISTS embedding;

COMMIT;
