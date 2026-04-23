-- ============================================================
-- 024_ops_mailbox_delta_state.sql
-- ============================================================
--
-- Adds ops.mailbox_delta_state — stores the Graph delta cursor per
-- (mailbox, folder) pair so graph_delta_worker.py can incrementally
-- poll for changes (including @removed tombstones) without repeating
-- prior work.
--
-- Why a dedicated table instead of a JSONB column on ops.sync_mailboxes?
-- Delta cursors accumulate per-folder (inbox, sentitems, etc.), and each
-- one is a long opaque token (~500-2000 chars) that Graph returns verbatim.
-- A narrow keyed table keeps indexes clean and lets us retire stale folders
-- without a JSONB rewrite dance.
--
-- Safety: idempotent — CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS.

BEGIN;

CREATE TABLE IF NOT EXISTS ops.mailbox_delta_state (
    id                  BIGSERIAL PRIMARY KEY,
    mailbox             TEXT NOT NULL,              -- 'athena@seattleseahawks.me'
    folder              TEXT NOT NULL,              -- 'inbox' / 'sentitems' / folder id
    delta_link          TEXT,                       -- full URL from Graph @odata.deltaLink
    last_polled_at      TIMESTAMPTZ,
    last_success_at     TIMESTAMPTZ,
    last_error          TEXT,
    last_error_at       TIMESTAMPTZ,
    messages_seen_total BIGINT NOT NULL DEFAULT 0,
    removals_total      BIGINT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (mailbox, folder)
);

CREATE INDEX IF NOT EXISTS idx_mailbox_delta_state_mailbox
    ON ops.mailbox_delta_state (mailbox);

COMMENT ON TABLE ops.mailbox_delta_state IS
    'Per-(mailbox,folder) Graph delta cursor. graph_delta_worker.py reads '
    'delta_link on startup, issues GET /messages/delta?deltatoken=..., '
    'processes @removed entries (stamps source_deleted_at on the email doc), '
    'then stores the new deltaLink. Bootstraps with delta_link=NULL (full scan).';
COMMENT ON COLUMN ops.mailbox_delta_state.delta_link IS
    'Opaque Graph deltaLink URL — treat as a black box.';
COMMENT ON COLUMN ops.mailbox_delta_state.last_error IS
    'Last error message (truncated). NULL on success; cleared when a run succeeds.';

-- Trigger to keep updated_at current without relying on app-side stamping.
CREATE OR REPLACE FUNCTION ops.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_mailbox_delta_state_updated
    ON ops.mailbox_delta_state;
CREATE TRIGGER trg_mailbox_delta_state_updated
    BEFORE UPDATE ON ops.mailbox_delta_state
    FOR EACH ROW EXECUTE FUNCTION ops.set_updated_at();

COMMIT;
