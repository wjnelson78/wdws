-- ============================================================
-- Athena Autonomy Infrastructure
-- Migration: 011_athena_autonomy.sql
-- Database: wdws
--
-- Adds the foundation for Athena's autonomous operation:
--   1. ops.migrations_applied  — audit trail for Athena-driven migrations
--   2. ops.athena_decisions    — log of every major decision with reasoning
--   3. ops.pending_approvals   — items Athena wants to do but needs sign-off
-- ============================================================

BEGIN;

-- ── 1. Migration audit trail ────────────────────────────────
CREATE TABLE IF NOT EXISTS ops.migrations_applied (
    id              BIGSERIAL PRIMARY KEY,
    migration_name  TEXT NOT NULL,
    sql_forward     TEXT NOT NULL,          -- the DDL that was applied
    sql_rollback    TEXT NOT NULL,          -- auto-generated inverse
    applied_by      TEXT NOT NULL DEFAULT 'athena',
    applied_at      TIMESTAMPTZ DEFAULT now(),
    duration_ms     INT,
    analysis        TEXT,                   -- Athena's reasoning for the change
    confidence      NUMERIC(4,3),           -- 0.000–1.000
    status          TEXT NOT NULL DEFAULT 'applied'
                    CHECK (status IN ('applied', 'rolled_back', 'failed')),
    rolled_back_at  TIMESTAMPTZ,
    rolled_back_by  TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_migrations_name
    ON ops.migrations_applied (migration_name);

-- ── 2. Athena decision log ──────────────────────────────────
-- Every reasoning cycle that results in an action gets recorded here.
-- This becomes Athena's long-term memory of what it tried and what happened.
CREATE TABLE IF NOT EXISTS ops.athena_decisions (
    id              BIGSERIAL PRIMARY KEY,
    run_id          BIGINT REFERENCES ops.agent_runs(id),
    decision_type   TEXT NOT NULL,          -- 'config_change','schema_migration','delegation','patch','no_action'
    target          TEXT,                   -- agent_id, table name, file path, etc.
    description     TEXT NOT NULL,
    reasoning       TEXT,                   -- full LLM analysis
    confidence      NUMERIC(4,3),
    action_taken    JSONB DEFAULT '{}'::jsonb,
    outcome         TEXT,                   -- 'success','failed','pending','rolled_back'
    outcome_detail  TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_athena_decisions_type
    ON ops.athena_decisions (decision_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_athena_decisions_outcome
    ON ops.athena_decisions (outcome, created_at DESC);

-- ── 3. Pending approvals (human gate) ──────────────────────
-- Athena writes here when confidence < threshold or action is destructive.
-- Human reviews and sets approved = true/false.
-- Athena polls this table each cycle.
CREATE TABLE IF NOT EXISTS ops.pending_approvals (
    id              BIGSERIAL PRIMARY KEY,
    decision_type   TEXT NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT NOT NULL,
    proposed_action JSONB NOT NULL,         -- full structured action for Athena to execute
    sql_preview     TEXT,                   -- for migrations: the exact SQL
    rollback_sql    TEXT,                   -- for migrations: rollback script
    reasoning       TEXT,
    confidence      NUMERIC(4,3),
    risk_level      TEXT DEFAULT 'medium'
                    CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    expires_at      TIMESTAMPTZ,            -- Athena re-escalates when this passes
    approved        BOOLEAN,               -- NULL = pending, true = go, false = reject
    approved_by     TEXT,
    approved_at     TIMESTAMPTZ,
    rejection_note  TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    notified_at     TIMESTAMPTZ             -- when email was sent
);

CREATE INDEX IF NOT EXISTS idx_pending_approvals_status
    ON ops.pending_approvals (approved, created_at DESC);

COMMIT;
