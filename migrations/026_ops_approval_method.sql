-- Migration 026: Add approval_method column to ops.pending_approvals
-- Sprint A preflight substrate (per SPRINT_A_WORK_ORDER_v2.2.md §2.1)
-- Additive only.
--
-- Rationale: Sprint A's conversational-approval mechanics (§10 rule 9) need a
-- way to distinguish how a given approval was recorded — directed-chat vs.
-- dashboard-click vs. direct-row-update (rollback scripts) vs. bootstrap
-- (this migration itself, which cannot pass through rule 9 because the column
-- it adds is what rule 9 depends on).
--
-- Chicken-and-egg: this migration is applied during preflight by the migration
-- runner with applied_by='sprint_a_bootstrap'. No ops.pending_approvals row
-- is created for it; Will authorizes this one-time exception in conversation
-- directly (kickoff message, 2026-04-23).

BEGIN;

ALTER TABLE ops.pending_approvals
    ADD COLUMN approval_method text;

COMMENT ON COLUMN ops.pending_approvals.approval_method IS
    'How approval was recorded. Values: conversation (directed chat), dashboard (UI click), api (direct row update, e.g. rollback scripts), bootstrap (pre-rule-9 migrations). NULL on rows created before this column existed.';

COMMIT;
