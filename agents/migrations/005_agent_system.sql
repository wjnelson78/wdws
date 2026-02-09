-- ═══════════════════════════════════════════════════════════════════
-- WDWS Agent System — Database Migration
-- Extends ops.agent_registry + ops.agent_sessions with:
--   ops.agent_runs     — individual execution records
--   ops.agent_findings — issues/recommendations discovered
--   ops.agent_messages — inter-agent communication queue
--   ops.health_checks  — watchdog metrics
-- ═══════════════════════════════════════════════════════════════════

-- Add scheduling + config columns to existing agent_registry
ALTER TABLE ops.agent_registry
  ADD COLUMN IF NOT EXISTS agent_type   TEXT DEFAULT 'autonomous',
  ADD COLUMN IF NOT EXISTS schedule     TEXT DEFAULT '*/5 * * * *',
  ADD COLUMN IF NOT EXISTS config       JSONB DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS last_run_at  TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS last_status  TEXT DEFAULT 'idle',
  ADD COLUMN IF NOT EXISTS run_count    INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS error_count  INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS priority     INT DEFAULT 5,
  ADD COLUMN IF NOT EXISTS version      TEXT DEFAULT '1.0.0';

-- Agent execution history
CREATE TABLE IF NOT EXISTS ops.agent_runs (
    id          BIGSERIAL PRIMARY KEY,
    agent_id    TEXT NOT NULL REFERENCES ops.agent_registry(id),
    started_at  TIMESTAMPTZ DEFAULT now(),
    ended_at    TIMESTAMPTZ,
    status      TEXT DEFAULT 'running',  -- running, success, error, timeout, skipped
    trigger     TEXT DEFAULT 'schedule', -- schedule, manual, orchestrator, event
    summary     TEXT,
    findings    INT DEFAULT 0,
    actions     INT DEFAULT 0,
    metrics     JSONB DEFAULT '{}'::jsonb,
    error       TEXT,
    duration_ms INT
);
CREATE INDEX IF NOT EXISTS idx_agent_runs_agent   ON ops.agent_runs(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_runs_status  ON ops.agent_runs(status);

-- Agent findings (issues, recommendations, alerts)
CREATE TABLE IF NOT EXISTS ops.agent_findings (
    id          BIGSERIAL PRIMARY KEY,
    agent_id    TEXT NOT NULL REFERENCES ops.agent_registry(id),
    run_id      BIGINT REFERENCES ops.agent_runs(id),
    severity    TEXT NOT NULL DEFAULT 'info',  -- critical, warning, info, suggestion
    category    TEXT NOT NULL,                 -- security, performance, data-quality, health, etc.
    title       TEXT NOT NULL,
    detail      TEXT,
    evidence    JSONB DEFAULT '{}'::jsonb,
    status      TEXT DEFAULT 'open',           -- open, acknowledged, resolved, dismissed
    resolved_at TIMESTAMPTZ,
    resolved_by TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agent_findings_sev    ON ops.agent_findings(severity, status);
CREATE INDEX IF NOT EXISTS idx_agent_findings_cat    ON ops.agent_findings(category);
CREATE INDEX IF NOT EXISTS idx_agent_findings_agent  ON ops.agent_findings(agent_id);

-- Inter-agent message queue
CREATE TABLE IF NOT EXISTS ops.agent_messages (
    id          BIGSERIAL PRIMARY KEY,
    from_agent  TEXT NOT NULL,
    to_agent    TEXT NOT NULL,  -- '*' = broadcast
    msg_type    TEXT NOT NULL,  -- command, query, alert, report, ack
    subject     TEXT,
    body        JSONB DEFAULT '{}'::jsonb,
    priority    INT DEFAULT 5,
    status      TEXT DEFAULT 'pending',  -- pending, read, processed, expired
    created_at  TIMESTAMPTZ DEFAULT now(),
    processed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_agent_msgs_to     ON ops.agent_messages(to_agent, status);
CREATE INDEX IF NOT EXISTS idx_agent_msgs_from   ON ops.agent_messages(from_agent);

-- Health check metrics (watchdog)
CREATE TABLE IF NOT EXISTS ops.health_checks (
    id          BIGSERIAL PRIMARY KEY,
    check_name  TEXT NOT NULL,   -- service:wdws-mcp, db:connections, disk:root, etc.
    status      TEXT NOT NULL,   -- healthy, degraded, critical, unknown
    value       NUMERIC,
    unit        TEXT,
    detail      JSONB DEFAULT '{}'::jsonb,
    checked_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_health_checks_name ON ops.health_checks(check_name, checked_at DESC);

-- Grant permissions
GRANT ALL ON ALL TABLES    IN SCHEMA ops TO wdws;
GRANT ALL ON ALL SEQUENCES IN SCHEMA ops TO wdws;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON TABLES    TO wdws;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT ALL ON SEQUENCES TO wdws;
