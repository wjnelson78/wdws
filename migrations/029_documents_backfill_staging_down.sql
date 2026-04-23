-- Rollback Migration 029: drop the backfill staging table.

BEGIN;

DROP TABLE IF EXISTS core.documents_backfill_staging CASCADE;

COMMIT;
