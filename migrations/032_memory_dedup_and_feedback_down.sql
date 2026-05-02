-- 032_memory_dedup_and_feedback_down.sql
-- Reverse 032. Drops indexes first, then columns. Supersession chains are lost.

BEGIN;

DROP INDEX IF EXISTS core.idx_memories_active_unsuperseded;
DROP INDEX IF EXISTS core.idx_memories_content_hash_active;

ALTER TABLE core.memories
    DROP COLUMN IF EXISTS superseded_at,
    DROP COLUMN IF EXISTS superseded_by,
    DROP COLUMN IF EXISTS content_hash;

COMMIT;
