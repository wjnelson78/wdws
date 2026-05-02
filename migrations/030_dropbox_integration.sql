-- ═══════════════════════════════════════════════════════════════════
-- Migration 030 — Athena Dropbox Integration Service (Phase 2a)
--
-- Adds three ops.* tables for the Dropbox connector's connection
-- lifecycle, sync-pipeline audit, and human-review quarantine queue.
--
-- Single-tenant model: athena_user_id on dropbox_connections always
-- references the platform owner (core.users). External-user
-- attribution (e.g. "Sara Murray") is carried on each ingested
-- document's metadata JSONB column, not via tenant isolation.
--
-- References:
--   dropbox_integration_design.md
--     §7  — data model
--     §9.3 — ingestion metadata seeding (source, source_connection, client, dropbox_*)
--     §18.3 — provisioning log
--     §20 — tenancy and attribution model (single-tenant rationale)
-- ═══════════════════════════════════════════════════════════════════


-- 1. dropbox_connections — one row per OAuth-authorized Dropbox account

CREATE TABLE IF NOT EXISTS ops.dropbox_connections (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Owner of the connection. Single-tenant: always platform owner
    -- (core.users.id). Kept explicit to avoid hardcoding user_id=1 in app code.
    athena_user_id          INTEGER NOT NULL REFERENCES core.users(id),

    -- Display label propagated as metadata.client on every ingested document.
    -- E.g. "Sara Murray", "Bobby Hair (expert)". See §9.3 and §20.
    external_user_label     TEXT,

    -- Dropbox app + identity
    app_registration        TEXT NOT NULL
                            CHECK (app_registration IN ('app_folder', 'full_dropbox')),
    dropbox_account_id      TEXT NOT NULL,
    dropbox_team_id         TEXT,
    dropbox_email           TEXT NOT NULL,
    dropbox_display_name    TEXT,

    -- OAuth tokens encrypted at rest (AES-256-GCM per §12.1).
    -- Encryption key managed alongside other Athena secrets; not captured here.
    access_token_enc        BYTEA NOT NULL,
    refresh_token_enc       BYTEA NOT NULL,
    token_expires_at        TIMESTAMPTZ NOT NULL,
    scopes                  TEXT[] NOT NULL,

    -- Delta-sync cursor
    watched_path            TEXT NOT NULL DEFAULT '',
    delta_cursor            TEXT,
    cursor_initialized_at   TIMESTAMPTZ,

    -- Routing: where ingested content lands on the Athena side
    target_case_id          INTEGER REFERENCES legal.cases(id),
    target_domain           TEXT,
    trust_tier              TEXT NOT NULL DEFAULT 'review'
                            CHECK (trust_tier IN ('auto_ingest', 'review', 'quarantine')),
    auto_tag                TEXT[] NOT NULL DEFAULT '{}',

    -- Lifecycle status
    status                  TEXT NOT NULL DEFAULT 'active'
                            CHECK (status IN ('active', 'paused',
                                              'revoked_local', 'revoked_remote',
                                              'error')),
    status_reason           TEXT,

    -- Audit timestamps
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by              INTEGER REFERENCES core.users(id),
    last_sync_at            TIMESTAMPTZ,
    last_webhook_at         TIMESTAMPTZ,
    last_error_at           TIMESTAMPTZ,
    last_error_message      TEXT,

    UNIQUE (dropbox_account_id, watched_path, app_registration)
);

CREATE INDEX IF NOT EXISTS idx_dbx_conn_account
    ON ops.dropbox_connections (dropbox_account_id);

-- Hot path: sync-worker scan of active connections
CREATE INDEX IF NOT EXISTS idx_dbx_conn_status_active
    ON ops.dropbox_connections (status)
    WHERE status = 'active';

-- Case-to-connections lookup
CREATE INDEX IF NOT EXISTS idx_dbx_conn_case
    ON ops.dropbox_connections (target_case_id)
    WHERE target_case_id IS NOT NULL;

COMMENT ON TABLE  ops.dropbox_connections           IS 'One row per OAuth-authorized Dropbox account linked to Athena. See design doc §7.1, §20.';
COMMENT ON COLUMN ops.dropbox_connections.external_user_label IS 'Human-readable attribution ("Sara Murray", etc.) propagated to metadata.client on every ingested document. §9.3, §20.';
COMMENT ON COLUMN ops.dropbox_connections.athena_user_id      IS 'Platform owner in single-tenant model. §20.';


-- 2. dropbox_sync_events — append-only audit of sync-pipeline activity

CREATE TABLE IF NOT EXISTS ops.dropbox_sync_events (
    id                  BIGSERIAL PRIMARY KEY,
    connection_id       UUID NOT NULL
                        REFERENCES ops.dropbox_connections(id) ON DELETE CASCADE,
    event_type          TEXT NOT NULL
                        CHECK (event_type IN (
                            'connection_created', 'connection_paused',
                            'connection_resumed', 'connection_revoked',
                            'token_refreshed',
                            'webhook_received',
                            'cursor_advanced',
                            'file_detected', 'file_downloaded',
                            'file_ingested', 'file_skipped_dedup',
                            'file_quarantined',
                            'remote_delete',
                            'error'
                        )),
    dropbox_path        TEXT,
    dropbox_rev         TEXT,
    content_hash        TEXT,
    file_size           BIGINT,
    athena_doc_id       UUID REFERENCES core.documents(id) ON DELETE SET NULL,
    duration_ms         INTEGER,
    details             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dbx_events_conn_time
    ON ops.dropbox_sync_events (connection_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dbx_events_type
    ON ops.dropbox_sync_events (event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dbx_events_doc
    ON ops.dropbox_sync_events (athena_doc_id)
    WHERE athena_doc_id IS NOT NULL;

COMMENT ON TABLE ops.dropbox_sync_events IS 'Append-only audit of Dropbox sync-pipeline events, for chain-of-custody. See design doc §12.5, §7.2.';


-- 3. dropbox_quarantine — human-review queue for trust_tier = review | quarantine

CREATE TABLE IF NOT EXISTS ops.dropbox_quarantine (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_id       UUID NOT NULL
                        REFERENCES ops.dropbox_connections(id) ON DELETE CASCADE,
    dropbox_path        TEXT NOT NULL,
    dropbox_rev         TEXT NOT NULL,
    content_hash        TEXT NOT NULL,
    file_size           BIGINT NOT NULL,

    -- Staging location on disk (purged on approve/reject/expire)
    local_path          TEXT NOT NULL,
    detected_mime       TEXT,

    review_status       TEXT NOT NULL DEFAULT 'pending'
                        CHECK (review_status IN ('pending', 'approved',
                                                 'rejected', 'expired')),
    reviewer_id         INTEGER REFERENCES core.users(id),
    reviewed_at         TIMESTAMPTZ,
    reviewer_notes      TEXT,
    athena_doc_id       UUID REFERENCES core.documents(id) ON DELETE SET NULL,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dbx_quar_pending
    ON ops.dropbox_quarantine (connection_id, created_at DESC)
    WHERE review_status = 'pending';
CREATE INDEX IF NOT EXISTS idx_dbx_quar_status_time
    ON ops.dropbox_quarantine (review_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dbx_quar_expires_pending
    ON ops.dropbox_quarantine (expires_at)
    WHERE review_status = 'pending';

COMMENT ON TABLE ops.dropbox_quarantine IS 'Human-review queue for Dropbox ingests at trust_tier review or quarantine. Auto-expires unreviewed items. See design doc §7.3, §10.1.';
