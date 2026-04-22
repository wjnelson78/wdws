-- 020 — Email sync hardening
--
-- Two tables supporting the email_sync pipeline:
--
-- 1. ops.email_mime_failures
--    Records Graph /$value MIME fetch failures per (mailbox, graph_id).
--    Some item classes (IPM.Schedule.Meeting.Resp.*, cancellations,
--    voicemail, Teams items) have no RFC-822 representation and return
--    HTTP 500 every time. We skip them on future runs to stop wasting
--    Graph calls and flooding operator alerts with identical warnings.
--
-- 2. ops.sync_alert_dedup
--    Fingerprints the content of each sync-alert email so we don't fire
--    identical "0 errors / N warnings" messages every 6 minutes when
--    the underlying condition is unchanged.

CREATE TABLE IF NOT EXISTS ops.email_mime_failures (
    mailbox           TEXT        NOT NULL,
    graph_id          TEXT        NOT NULL,
    item_class        TEXT,
    subject           TEXT,
    first_failed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt_count     INT         NOT NULL DEFAULT 1,
    last_http_status  INT,
    last_error        TEXT,
    permanent         BOOLEAN     NOT NULL DEFAULT FALSE,
    PRIMARY KEY (mailbox, graph_id)
);

CREATE INDEX IF NOT EXISTS email_mime_failures_last_failed_idx
    ON ops.email_mime_failures (last_failed_at DESC);

CREATE INDEX IF NOT EXISTS email_mime_failures_skip_idx
    ON ops.email_mime_failures (mailbox)
    WHERE permanent = TRUE OR attempt_count >= 3;

CREATE TABLE IF NOT EXISTS ops.sync_alert_dedup (
    run_label      TEXT        NOT NULL,
    content_hash   TEXT        NOT NULL,
    first_sent_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_sent_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    send_count     INT         NOT NULL DEFAULT 1,
    suppress_count INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (run_label, content_hash)
);

CREATE INDEX IF NOT EXISTS sync_alert_dedup_last_sent_idx
    ON ops.sync_alert_dedup (last_sent_at DESC);
