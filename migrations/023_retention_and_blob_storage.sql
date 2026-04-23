-- ============================================================
-- 023_retention_and_blob_storage.sql
-- ============================================================
--
-- Adds generic retention / tombstone / blob-storage columns to core.documents
-- and email-specific source-lifecycle columns to legal.email_metadata.
--
-- Context:
-- Following the unified ingestion pipeline (ingestion_pipeline.py writing to
-- core.documents), retention becomes a universal document concern rather than
-- an email-specific one. This migration extends core.documents with:
--
--   1. Classification (content | spam | privileged | transactional | unknown)
--   2. Retention policy (retain_until + legal_hold override)
--   3. Purge tombstone (row persists as audit gravestone after bytes deleted)
--   4. Blob storage pointers (ready for off-FS storage without another migration)
--
-- And legal.email_metadata gets:
--   5. source_deleted_at / source_check_at — for the Graph delta-poll worker
--      that will tombstone messages Graph reports as deleted upstream
--
-- Scope deliberately excluded:
--   * Conversation-scope bridge (chat.conversations lives in athena_chat DB;
--     cross-DB FK isn't possible. Defer to a separate migration that can
--     touch both DBs or use postgres_fdw.)
--   * The delta-poll worker itself (code, not DDL — a follow-up piece).
--   * Blob move from filesystem → object storage (operational, not schema).
--   * Classifier (ML pipeline code — separate.) Default classification is NULL
--     until a classifier populates it.
--
-- Safety: all ADD COLUMN and CREATE INDEX statements use IF NOT EXISTS, so
-- the migration is idempotent and safe to re-run after partial failure.

BEGIN;

-- ================================================================
-- 1. core.documents — generic retention + tombstone + blob pointers
-- ================================================================

ALTER TABLE core.documents
    -- Classification tier — set by a classifier job (not part of this migration)
    ADD COLUMN IF NOT EXISTS classification TEXT,
    ADD COLUMN IF NOT EXISTS classified_at  TIMESTAMPTZ,

    -- Retention policy — when the doc becomes eligible for purge.
    -- NULL means retain indefinitely (default for existing rows).
    ADD COLUMN IF NOT EXISTS retain_until       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS legal_hold         BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS legal_hold_reason  TEXT,

    -- Purge tombstone — bytes gone, metadata row remains as audit record.
    -- A nightly sweep queries: retain_until < now() AND NOT legal_hold
    -- AND purged_at IS NULL, then deletes storage bytes and stamps purged_at.
    ADD COLUMN IF NOT EXISTS purged_at      TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS purge_reason   TEXT,

    -- Blob storage pointer — optional. NULL means "no external blob"
    -- (content is in full_content column OR source_path points at local FS).
    -- Populated when the ingest writes .eml / attachments / large PDFs to
    -- object storage instead of inlining in full_content.
    ADD COLUMN IF NOT EXISTS storage_backend    TEXT,   -- 'fs' | 's3' | 'azure' | 'minio'
    ADD COLUMN IF NOT EXISTS storage_uri        TEXT,   -- 's3://bucket/key' OR '/opt/wdws/data/...'
    ADD COLUMN IF NOT EXISTS storage_sha256     TEXT,
    ADD COLUMN IF NOT EXISTS storage_size_bytes BIGINT;

-- Indexes to support the retention sweep query pattern efficiently:
--   WHERE retain_until < now() AND NOT legal_hold AND purged_at IS NULL
CREATE INDEX IF NOT EXISTS idx_documents_retention
    ON core.documents (retain_until)
    WHERE purged_at IS NULL AND legal_hold = false;

-- Legal-hold is a hard filter that overrides everything; partial index keeps
-- it tiny (only a handful of docs should ever be on hold).
CREATE INDEX IF NOT EXISTS idx_documents_legal_hold
    ON core.documents (legal_hold)
    WHERE legal_hold = true;

CREATE INDEX IF NOT EXISTS idx_documents_classification
    ON core.documents (classification)
    WHERE classification IS NOT NULL;

COMMENT ON COLUMN core.documents.classification IS
    'Content tier — content | spam | privileged | transactional | unknown. '
    'Populated by a classifier job. Feeds retention policy.';
COMMENT ON COLUMN core.documents.retain_until IS
    'When the document becomes eligible for purge. NULL = retain indefinitely.';
COMMENT ON COLUMN core.documents.legal_hold IS
    'When true, overrides retain_until — document cannot be purged regardless '
    'of retention schedule. Required for active litigation custodians.';
COMMENT ON COLUMN core.documents.purged_at IS
    'Timestamp when storage bytes were deleted under retention policy. The '
    'row remains as an audit gravestone so we can prove what was destroyed, '
    'when, and why (purge_reason).';
COMMENT ON COLUMN core.documents.storage_backend IS
    'Where the binary payload lives — fs | s3 | azure | minio. NULL for '
    'text-only docs whose full_content is the source of truth.';

-- ================================================================
-- 2. legal.email_metadata — source lifecycle from Graph
-- ================================================================

ALTER TABLE legal.email_metadata
    -- When Microsoft Graph first reported this message as deleted upstream.
    -- Starts the retention clock for policies like "5y from source deletion".
    -- Populated by a future delta-poll worker; NULL means still present at source.
    ADD COLUMN IF NOT EXISTS source_deleted_at TIMESTAMPTZ,

    -- Last time we successfully fetched or confirmed the source message exists.
    -- Useful for detecting long-dark mailboxes (auth expired, tenant changes).
    ADD COLUMN IF NOT EXISTS source_check_at   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_email_metadata_source_deleted
    ON legal.email_metadata (source_deleted_at)
    WHERE source_deleted_at IS NOT NULL;

COMMENT ON COLUMN legal.email_metadata.source_deleted_at IS
    'Timestamp when Graph delta query first reported this message as @removed. '
    'Feeds core.documents.retain_until via retention policy: when a source is '
    'deleted and the classification is not spam/privileged, retain for 5y.';
COMMENT ON COLUMN legal.email_metadata.source_check_at IS
    'Last delta-poll timestamp that saw this message still present at source. '
    'Used to detect stale entries (mailbox access revoked, tenant migrated).';

COMMIT;
