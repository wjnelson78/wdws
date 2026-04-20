-- ============================================================
-- ACP Schema Migration 002: Email Sync Management
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
  'AI agent classifications of emails — case associations, entity extraction, urgency scoring, summaries, categories. Produced by Athena AI analysis.';

-- ────────────────────────────────────────────────────────────
-- Seed Data — initial mailboxes and rules from current config
-- ────────────────────────────────────────────────────────────

INSERT INTO ops.sync_mailboxes (email, display_name) VALUES
    ('william@seattleseahawks.me', 'William Nelson'),
    ('athena@seattleseahawks.me', 'Athena AI')
ON CONFLICT (email) DO NOTHING;

INSERT INTO ops.sync_rules (name, rule_type, pattern, description, priority) VALUES
    ('Snohomish County', 'domain', 'snoco.org',
     'Snohomish County government domain', 0),
    ('Snohomish County WA', 'domain', 'co.snohomish.wa.us',
     'Snohomish County Washington state domain', 0),
    ('Chelan County WA', 'domain', 'co.chelan.wa.us',
     'Chelan County Washington state domain', 0),
    ('Washington State Courts', 'domain', 'courts.wa.gov',
     'Washington State court system domain', 0),
    ('Ninth Circuit', 'domain', 'ca9.uscourts.gov',
     'Ninth Circuit court email domain', 60),
    ('Federal courts wildcard', 'domain', '*.uscourts.gov',
     'Covers federal judiciary subdomains such as wawd.uscourts.gov', 55),
    ('South Snohomish Fire domain', 'domain', 'southsnofire.org',
     'South Snohomish Fire / South County Fire domain', 40),
    ('workerlaw.com', 'domain', 'workerlaw.com',
     'Opposing counsel — Iglitzin, Cole, Fernando (WorkerLaw)', 0),
    ('csdlaw.com', 'domain', 'csdlaw.com',
     'Defense counsel — Becker, Davis, Dobbs, Paxton (CSD Law)', 0),
    ('cabornelaw.com', 'domain', 'cabornelaw.com',
     'Defense counsel — CSD Law alternate domain', 0),
    ('clearpathpllc.com', 'domain', 'clearpathpllc.com',
     'Daniel Fox — ClearPath PLLC', 0),
    ('fwwlaw.com', 'domain', 'fwwlaw.com',
     'Defense counsel — Lyman, Bertolino, Himes, Ross (FWW Law)', 0),
    ('kantorlaw.net', 'domain', 'kantorlaw.net',
     'Counsel — B. Davis (Kantor Law)', 0),
    ('bn-lawyers.com', 'domain', 'bn-lawyers.com',
     'Counsel — Suttell (BN Lawyers)', 0),
    ('ogletree.com', 'domain', 'ogletree.com',
     'Defense counsel — Shapero, Shely (Ogletree Deakins)', 0),
    ('ogletreedeakins.com', 'domain', 'ogletreedeakins.com',
     'Defense counsel — Shapero, Shely (Ogletree Deakins alt)', 0),
    ('lewisbrisbois.com', 'domain', 'lewisbrisbois.com',
     'Defense counsel — Hunter (Lewis Brisbois)', 0),
    ('grsm.com', 'domain', 'grsm.com',
     'Counsel — Jardine, Lockwood (GRSM)', 0),
    ('insleebest.com', 'domain', 'insleebest.com',
     'Counsel — Chambers, Lee (InsleeBest)', 0),
    ('jmblawyers.com', 'domain', 'jmblawyers.com',
     'Counsel — Baker (JMB Lawyers)', 0),
    ('uscanadalaw.com', 'domain', 'uscanadalaw.com',
     'Counsel — Paul (USCanada Law)', 0),
    ('favros.com', 'domain', 'favros.com',
     'James B. Meade (Favros Law)', 0),
    ('cozen.com', 'domain', 'cozen.com',
     'Counsel — Lee (Cozen O''Connor)', 0),
    ('slwsd.com', 'domain', 'slwsd.com',
     'Counsel — Brees (SLWSD)', 0),
    ('lldkb.com', 'domain', 'lldkb.com',
     'Counsel — tam@ (LLDKB)', 0),
    ('mhb.com', 'domain', 'mhb.com',
     'MHB Law — opposing counsel', 0),
    ('wahbexchange.org', 'domain', 'wahbexchange.org',
     'Washington Health Benefit Exchange (WAHBE) — ADA accommodation requests', 0),
    ('evergreenhealthcare.org', 'domain', 'evergreenhealthcare.org',
     'EvergreenHealth — primary domain (MyChart, staff, billing, donotreply)', 0),
    ('evergreenhealth.com', 'domain', 'evergreenhealth.com',
     'EvergreenHealth — staff email domain (physicians, administrators)', 0),
    ('starbucks.com', 'domain', 'starbucks.com',
     'Starbucks legal / corporate counsel', 0),
    ('playfasa.com', 'domain', 'playfasa.com',
     'Bobby Hair — PlayFASA website development (Fastpitch America Softball Association)', 0)
ON CONFLICT (rule_type, pattern) DO NOTHING;

-- ────────────────────────────────────────────────────────────
-- Done
-- ────────────────────────────────────────────────────────────
