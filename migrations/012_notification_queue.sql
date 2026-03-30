-- 012 — Notification queue for daily digest
-- All agent notifications are written here instead of sent immediately.
-- The DailyDigestAgent reads this table at 7 AM and sends one email.
-- Critical-severity events bypass the queue and are sent immediately.

CREATE TABLE IF NOT EXISTS ops.notification_queue (
    id              BIGSERIAL     PRIMARY KEY,
    agent_id        TEXT          NOT NULL,
    subject         TEXT          NOT NULL,
    sections        JSONB         NOT NULL DEFAULT '[]',
    severity        TEXT          NOT NULL DEFAULT 'info',  -- info | warning | critical
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    digest_sent_at  TIMESTAMPTZ,          -- NULL = pending; set = included in a digest
    immediate_sent  BOOLEAN       NOT NULL DEFAULT FALSE    -- TRUE = bypassed queue
);

CREATE INDEX IF NOT EXISTS notification_queue_pending
    ON ops.notification_queue (created_at)
    WHERE digest_sent_at IS NULL AND immediate_sent = FALSE;
