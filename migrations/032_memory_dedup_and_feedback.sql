-- 032_memory_dedup_and_feedback.sql
-- Add content-hash dedup + supersession chain to core.memories so save_personal_memory
-- can reinforce existing facts instead of inserting duplicates, and corrections
-- (memory_type='feedback') can explicitly retire the row they replace.

BEGIN;

ALTER TABLE core.memories
    ADD COLUMN IF NOT EXISTS content_hash text,
    ADD COLUMN IF NOT EXISTS superseded_by integer
        REFERENCES core.memories(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS superseded_at timestamptz;

UPDATE core.memories
   SET content_hash = encode(digest(content, 'sha256'), 'hex')
 WHERE content_hash IS NULL;

CREATE INDEX IF NOT EXISTS idx_memories_content_hash_active
    ON core.memories (content_hash)
    WHERE is_active = true AND superseded_by IS NULL;

CREATE INDEX IF NOT EXISTS idx_memories_active_unsuperseded
    ON core.memories (memory_type, importance DESC, created_at DESC)
    WHERE is_active = true AND superseded_by IS NULL;

COMMIT;
