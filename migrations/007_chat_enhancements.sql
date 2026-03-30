-- ============================================================
-- Chat API Enhancements: Ratings, Retry, Edit, Cancel
-- Migration: 007_chat_enhancements.sql
-- ============================================================

-- Add columns to messages table for edit/retry tracking
ALTER TABLE chat.messages 
    ADD COLUMN IF NOT EXISTS edited_from UUID REFERENCES chat.messages(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS retry_of UUID REFERENCES chat.messages(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS quality_tier TEXT CHECK (quality_tier IN ('auto', 'low', 'med', 'high', 'xhigh'));

-- ============================================================
-- COMPLETION REQUESTS (for cancellation tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.completion_requests (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id   UUID NOT NULL REFERENCES chat.conversations(id) ON DELETE CASCADE,
    user_id           BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    status            TEXT NOT NULL DEFAULT 'running' 
                      CHECK (status IN ('running', 'completed', 'cancelled', 'failed')),
    quality_tier      TEXT,
    model             TEXT,
    message_count     INT,
    started_at        TIMESTAMPTZ DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_completion_requests_status 
    ON chat.completion_requests(conversation_id, status);

-- ============================================================
-- MESSAGE RATINGS (for training/feedback)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.message_ratings (
    id             BIGSERIAL PRIMARY KEY,
    message_id     UUID NOT NULL REFERENCES chat.messages(id) ON DELETE CASCADE,
    user_id        BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    rating         INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
    rating_type    TEXT CHECK (rating_type IN ('accuracy', 'helpfulness', 'quality', 'overall')),
    feedback_text  TEXT,
    metadata       JSONB DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now(),
    UNIQUE (message_id, user_id, rating_type)
);

CREATE TRIGGER trg_message_ratings_updated
    BEFORE UPDATE ON chat.message_ratings
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

CREATE INDEX IF NOT EXISTS idx_message_ratings_message 
    ON chat.message_ratings(message_id);
CREATE INDEX IF NOT EXISTS idx_message_ratings_user 
    ON chat.message_ratings(user_id);
CREATE INDEX IF NOT EXISTS idx_message_ratings_rating 
    ON chat.message_ratings(rating);

-- ============================================================
-- MESSAGE EDITS (audit trail)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.message_edits (
    id                BIGSERIAL PRIMARY KEY,
    message_id        UUID NOT NULL REFERENCES chat.messages(id) ON DELETE CASCADE,
    previous_content  TEXT,
    new_content       TEXT,
    edited_by         BIGINT NOT NULL REFERENCES chat.users(id) ON DELETE CASCADE,
    edit_reason       TEXT,
    created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_message_edits_message 
    ON chat.message_edits(message_id);
