-- ============================================================
-- Migration 003: Email Attachment Support
-- ============================================================
-- Adds attachment tracking, parent-child document relationships,
-- and extended classification fields for GPT-4o deep analysis.
-- ============================================================

BEGIN;

-- 1. Attachment metadata table
--    Tracks each attachment on an email with extracted text
CREATE TABLE IF NOT EXISTS legal.email_attachments (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email_doc_id    uuid NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    child_doc_id    uuid REFERENCES core.documents(id) ON DELETE SET NULL,
    filename        text NOT NULL,
    content_type    text,          -- application/pdf, image/png, etc.
    file_size       integer,       -- bytes
    extracted_text  text,          -- OCR / text extraction output
    extraction_method text,        -- 'ocr', 'text_decode', 'html_strip', 'none'
    is_processed    boolean DEFAULT false,
    created_at      timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_email_attachments_email
    ON legal.email_attachments(email_doc_id);
CREATE INDEX IF NOT EXISTS idx_email_attachments_child
    ON legal.email_attachments(child_doc_id);

-- 2. Populate document_relationships for existing has_attachments data
--    (We'll use the relationship table for formal parent-child links)
COMMENT ON TABLE core.document_relationships IS
    'Tracks parent-child relationships: email→attachment, filing→exhibit, etc.';

-- 3. Add attachment_text column to email_metadata for quick access
--    This holds the CONCATENATED text of all attachments for search/classification
ALTER TABLE legal.email_metadata
    ADD COLUMN IF NOT EXISTS attachment_text text;

-- 4. Upgrade ai_classifications for GPT-4o deeper analysis
ALTER TABLE ops.ai_classifications
    ADD COLUMN IF NOT EXISTS legal_analysis jsonb,
    ADD COLUMN IF NOT EXISTS attachment_summary text,
    ADD COLUMN IF NOT EXISTS token_usage jsonb;

-- legal_analysis holds structured legal reasoning:
-- {
--   "procedural_posture": "Motion to Compel pending",
--   "deadlines": [{"date": "2025-02-15", "type": "response_due", "rule": "CR 26(c)"}],
--   "legal_issues": ["discovery dispute", "protective order scope"],
--   "opposing_counsel_tone": "aggressive",
--   "recommended_actions": ["File response by 2/15", "Review attached exhibit B"],
--   "risk_assessment": "high - sanctions possible if deadline missed"
-- }

-- 5. Index for finding unprocessed attachments
CREATE INDEX IF NOT EXISTS idx_email_attachments_unprocessed
    ON legal.email_attachments(is_processed) WHERE is_processed = false;

-- 6. Full-text search on attachment_text
CREATE INDEX IF NOT EXISTS idx_email_metadata_attachment_text
    ON legal.email_metadata USING gin(to_tsvector('english', COALESCE(attachment_text, '')));

COMMIT;
