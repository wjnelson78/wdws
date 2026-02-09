-- ============================================================
-- WDWS Schema Migration 002: Email Sync Management
-- PostgreSQL 17 + pgvector 0.8.0
--
-- Adds tables for:
--   • Mailbox registration (which M365 accounts to scan)
--   • Sync rules (domains, addresses, keywords + case associations)
--   • Sync run history (execution logs with metrics)
--   • AI classifications (GPT-powered email analysis)
--
-- Incremental migration — safe to re-run.
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- Sync Mailboxes — email accounts registered for Graph API sync
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ops.sync_mailboxes (
    id           SERIAL PRIMARY KEY,
    email        TEXT NOT NULL UNIQUE,
    display_name TEXT,
    is_active    BOOLEAN DEFAULT true,
    last_sync_at TIMESTAMPTZ,
    total_synced INT DEFAULT 0,
    metadata     JSONB DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_sync_mailboxes_updated ON ops.sync_mailboxes;
CREATE TRIGGER trg_sync_mailboxes_updated
    BEFORE UPDATE ON ops.sync_mailboxes
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

COMMENT ON TABLE ops.sync_mailboxes IS
  'Email mailboxes registered for Graph API sync. Each row is an M365 account that can be scanned for matching emails.';

-- ────────────────────────────────────────────────────────────
-- Sync Rules — filtering patterns with case associations
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ops.sync_rules (
    id           SERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    rule_type    TEXT NOT NULL CHECK (rule_type IN (
        'domain', 'email_address', 'keyword', 'subject_pattern'
    )),
    pattern      TEXT NOT NULL,
    case_numbers TEXT[] DEFAULT '{}',
    mailbox_ids  INT[] DEFAULT '{}',   -- empty = applies to all mailboxes
    is_active    BOOLEAN DEFAULT true,
    priority     INT DEFAULT 0,
    description  TEXT,
    metadata     JSONB DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_rules_type_pattern
    ON ops.sync_rules (rule_type, pattern);

DROP TRIGGER IF EXISTS trg_sync_rules_updated ON ops.sync_rules;
CREATE TRIGGER trg_sync_rules_updated
    BEFORE UPDATE ON ops.sync_rules
    FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

COMMENT ON TABLE ops.sync_rules IS
  'Rules controlling which emails are fetched during sync. Domain rules match sender/recipient domains. Email address rules match specific addresses. Keywords search body. Subject patterns match subjects. Each rule can be linked to specific case numbers for auto-classification.';

-- ────────────────────────────────────────────────────────────
-- Sync Runs — execution history with metrics
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ops.sync_runs (
    id                SERIAL PRIMARY KEY,
    started_at        TIMESTAMPTZ DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    status            TEXT DEFAULT 'running' CHECK (status IN (
        'running', 'completed', 'failed', 'cancelled'
    )),
    trigger_type      TEXT DEFAULT 'manual' CHECK (trigger_type IN (
        'manual', 'scheduled', 'api'
    )),
    mailboxes_scanned TEXT[],
    rules_applied     INT[],
    emails_found      INT DEFAULT 0,
    emails_ingested   INT DEFAULT 0,
    emails_skipped    INT DEFAULT 0,
    emails_failed     INT DEFAULT 0,
    chunks_created    INT DEFAULT 0,
    embedding_tokens  INT DEFAULT 0,
    ai_classified     INT DEFAULT 0,
    duration_seconds  FLOAT,
    error_log         JSONB DEFAULT '[]'::jsonb,
    metadata          JSONB DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_status
    ON ops.sync_runs (status);
CREATE INDEX IF NOT EXISTS idx_sync_runs_started
    ON ops.sync_runs (started_at DESC);

COMMENT ON TABLE ops.sync_runs IS
  'Tracks every email sync execution — trigger source, rule scope, results, errors, timing. Dashboard shows these as sync history.';

-- ────────────────────────────────────────────────────────────
-- AI Classifications — intelligent email analysis
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ops.ai_classifications (
    id             SERIAL PRIMARY KEY,
    document_id    UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
    model_used     TEXT NOT NULL,
    classification JSONB NOT NULL,
    confidence     FLOAT,
    processed_at   TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ai_class_doc
    ON ops.ai_classifications (document_id);
CREATE INDEX IF NOT EXISTS idx_ai_class_confidence
    ON ops.ai_classifications (confidence);
CREATE INDEX IF NOT EXISTS idx_ai_class_processed
    ON ops.ai_classifications (processed_at DESC);
-- GIN index on classification JSONB for urgency/category queries
CREATE INDEX IF NOT EXISTS idx_ai_class_data
    ON ops.ai_classifications USING GIN (classification jsonb_path_ops);

COMMENT ON TABLE ops.ai_classifications IS
  'AI agent classifications of emails — case associations, entity extraction, urgency scoring, summaries, categories. Produced by GPT-4o-mini analysis.';

-- ────────────────────────────────────────────────────────────
-- Seed Data — initial mailboxes and rules from current config
-- ────────────────────────────────────────────────────────────

INSERT INTO ops.sync_mailboxes (email, display_name) VALUES
    ('william@seattleseahawks.me', 'William Nelson'),
    ('athena@seattleseahawks.me', 'Athena AI')
ON CONFLICT (email) DO NOTHING;

INSERT INTO ops.sync_rules (name, rule_type, pattern, description) VALUES
    ('Snohomish County', 'domain', 'snoco.org',
     'Snohomish County government domain'),
    ('Snohomish County WA', 'domain', 'co.snohomish.wa.us',
     'Snohomish County Washington state domain')
ON CONFLICT (rule_type, pattern) DO NOTHING;

-- ────────────────────────────────────────────────────────────
-- Done
-- ────────────────────────────────────────────────────────────
