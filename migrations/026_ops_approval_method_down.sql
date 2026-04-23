-- Rollback Migration 026: Drop approval_method column from ops.pending_approvals
-- Sprint A preflight substrate rollback

BEGIN;

ALTER TABLE ops.pending_approvals DROP COLUMN IF EXISTS approval_method;

COMMIT;
