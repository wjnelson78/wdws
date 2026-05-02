-- ═══════════════════════════════════════════════════════════════════
-- Migration 030 — Rollback
-- Drops the three Dropbox integration tables added by 030_dropbox_integration.sql.
-- Order: dependents first (quarantine and sync_events FK → connections).
-- CASCADE on each DROP is defensive; after ON DELETE CASCADE on child FKs
-- a plain DROP works too, but CASCADE covers any referring views/triggers
-- added outside this migration.
-- ═══════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS ops.dropbox_quarantine    CASCADE;
DROP TABLE IF EXISTS ops.dropbox_sync_events   CASCADE;
DROP TABLE IF EXISTS ops.dropbox_connections   CASCADE;
