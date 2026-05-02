-- 031_sms_threads.sql
-- Telnyx SMS integration: threaded conversations + per-message log.
--
-- Threading rule:
--   • Outbound send resolves-or-creates a thread by (peer, our_number, subject).
--   • Inbound webhook attaches to the most recent OPEN thread with matching
--     (peer_number, our_number); if none, opens a new thread with subject NULL.
--   • close_sms_thread() sets closed_at; subsequent inbounds start a new thread.

BEGIN;

CREATE TABLE IF NOT EXISTS core.sms_threads (
    id                BIGSERIAL PRIMARY KEY,
    peer_number       TEXT NOT NULL,
    our_number        TEXT NOT NULL,
    subject           TEXT,
    opened_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_message_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at         TIMESTAMPTZ,
    CONSTRAINT sms_threads_e164_peer CHECK (peer_number ~ '^\+[1-9][0-9]{1,14}$'),
    CONSTRAINT sms_threads_e164_ours CHECK (our_number  ~ '^\+[1-9][0-9]{1,14}$')
);

-- One open thread per (peer, our, subject) at a time. Closed threads retained
-- for history and do not block a new thread from being opened with the same key.
CREATE UNIQUE INDEX IF NOT EXISTS sms_threads_open_uniq
    ON core.sms_threads (peer_number, our_number, COALESCE(subject, ''))
    WHERE closed_at IS NULL;

CREATE INDEX IF NOT EXISTS sms_threads_peer_recent
    ON core.sms_threads (peer_number, last_message_at DESC);

CREATE TABLE IF NOT EXISTS core.sms_messages (
    id                 BIGSERIAL PRIMARY KEY,
    thread_id          BIGINT NOT NULL REFERENCES core.sms_threads(id),
    direction          TEXT NOT NULL CHECK (direction IN ('outbound','inbound')),
    body               TEXT NOT NULL,
    telnyx_message_id  TEXT UNIQUE,
    parts              INT,
    cost_usd           NUMERIC(10,6),
    error_title        TEXT,
    error_detail       TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sms_messages_thread_recent
    ON core.sms_messages (thread_id, created_at DESC);

COMMIT;
