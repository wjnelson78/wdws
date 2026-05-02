-- =============================================================================
-- WA Court Rules Phase 1 SCHEMA EXTENSION #2 — STAGED FOR OPERATOR REVIEW
-- =============================================================================
-- Status:        STAGED — DO NOT APPLY without explicit operator authorization.
-- Author:        WA Court Rules ingest agent
-- Date:          2026-05-01 (post Phase 4 boundary halt)
-- Operator:      William J. Nelson — apply via psql, not autonomously
--
-- Purpose:
--   Phase 4 ingest halted at first repealed-row INSERT (GR 8) because
--   chk_rule_text_status_invariant requires is_repealed rows to have empty
--   full_text. 47 of 51 repealed rows in the parsed corpus carry meaningful
--   placeholder markers like "[RESCINDED]", "[Deleted]", "[Rescinded.
--   Provisions transferred to rule 13.]" — these are operationally useful
--   for retrieval and should be preserved.
--
-- This extension applies operator-approved Option A:
--
--   §1 Loosens chk_rule_text_status_invariant — extends Path 1 treatment
--      from is_reserved to is_repealed (doctrinal symmetry; ghost-flag tags
--      bear retrieval-filter weight, not the CHECK).
--
--   §2 Adds a new defensive CHECK chk_repealed_text_meaningful — regex
--      audit on body text (structurally-different audit channel from the
--      ghost-flag column itself). Catches mis-flagging where a row claims
--      is_repealed but the body has no rescission marker; suggests parser
--      bug rather than actual rescission. Belt-and-suspenders parallel to
--      chk_at_most_one_parent_pointer from extension #1.
--
-- Both changes preserve the Phase 6 §K audit-predicate-diversification
-- doctrine: schema CHECKs catch what parser predicates miss because they
-- test data shape independent from production code paths.
--
-- This script is atomic. ON_ERROR_STOP triggers ROLLBACK on any failure.
--
-- HOW TO APPLY (after operator authorizes):
--
--   psql "$DATABASE_URL" -f /opt/wdws/wa_court_rules_research/phase1_schema_extension_2.sql \
--     2>&1 | tee /opt/wdws/wa_court_rules_research/phase1_schema_extension_2_output.log
--
-- Estimated wall time: under 2 sec (DDL only on empty table).
-- =============================================================================

\set ON_ERROR_STOP on
\timing on

BEGIN;


-- =============================================================================
-- §0  PRE-EXECUTION GUARDS
-- =============================================================================

DO $$
DECLARE
  schema_present              INTEGER;
  rules_table_present         INTEGER;
  rules_row_count             INTEGER;
  current_inv_def             TEXT;
  meaningful_check_exists     INTEGER;
BEGIN
  -- §0.1 schema + table present
  SELECT COUNT(*) INTO schema_present
    FROM information_schema.schemata WHERE schema_name = 'wa_court_rules';
  IF schema_present <> 1 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.1]: wa_court_rules schema missing.';
  END IF;

  SELECT COUNT(*) INTO rules_table_present
    FROM information_schema.tables
    WHERE table_schema = 'wa_court_rules' AND table_name = 'rules';
  IF rules_table_present <> 1 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2]: wa_court_rules.rules table missing.';
  END IF;

  -- §0.2 wa_court_rules.rules empty (no partial Phase 4 ingest in progress)
  SELECT COUNT(*) INTO rules_row_count FROM wa_court_rules.rules;
  IF rules_row_count <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.3]: wa_court_rules.rules has % rows; expected 0. Halt — investigate before applying CHECK swap.', rules_row_count;
  END IF;

  -- §0.3 Current chk_rule_text_status_invariant has the STRICT form
  --      (is_repealed AND empty) — i.e., the swap has meaningful effect
  SELECT pg_get_constraintdef(oid) INTO current_inv_def
    FROM pg_constraint
    WHERE conname = 'chk_rule_text_status_invariant'
      AND conrelid = 'wa_court_rules.rules'::regclass;
  IF current_inv_def IS NULL THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4a]: chk_rule_text_status_invariant absent. Phase 1 incomplete or modified.';
  END IF;
  -- The strict form contains both 'is_repealed AND' and 'full_text IS NULL OR' clauses;
  -- the loosened form drops the conjunction with full_text emptiness.
  IF current_inv_def NOT LIKE '%is_repealed AND%' OR current_inv_def NOT LIKE '%full_text IS NULL%' THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4b]: chk_rule_text_status_invariant does not match expected strict form. Found: %', current_inv_def;
  END IF;

  -- §0.4 chk_repealed_text_meaningful does NOT yet exist
  SELECT COUNT(*) INTO meaningful_check_exists
    FROM pg_constraint
    WHERE conname = 'chk_repealed_text_meaningful'
      AND conrelid = 'wa_court_rules.rules'::regclass;
  IF meaningful_check_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5]: chk_repealed_text_meaningful already exists.';
  END IF;

  RAISE NOTICE 'Pre-flight: PASS — schema + rules table present; wa_court_rules.rules empty (0 rows); chk_rule_text_status_invariant present with strict form; chk_repealed_text_meaningful absent.';
END $$;


-- =============================================================================
-- §1  LOOSEN chk_rule_text_status_invariant (mirror is_reserved Path 1)
-- =============================================================================
-- Operator-approved Option A 2026-05-01: extends Path 1 loosening from
-- is_reserved to is_repealed for doctrinal symmetry. 47 of 51 repealed
-- rows in the parsed corpus carry meaningful placeholder markers
-- ("[RESCINDED]", "[Deleted]", "[Rescinded. Provisions transferred to
-- rule 13.]") — the original CHECK was over-strict relative to ghost-flag
-- tag boundaries. Ghost-flag tags (court-rule-rescinded, court-rule-reserved)
-- bear retrieval-filter weight; the CHECK no longer enforces empty-body
-- for repealed.

ALTER TABLE wa_court_rules.rules DROP CONSTRAINT chk_rule_text_status_invariant;

ALTER TABLE wa_court_rules.rules ADD CONSTRAINT chk_rule_text_status_invariant CHECK (
  (full_text IS NOT NULL AND full_text <> ''
   AND NOT (is_repealed OR is_reserved))
  OR
  (is_repealed)        -- LOOSENED 2026-05-01: was (is_repealed AND empty);
                       -- mirrors is_reserved Path 1 treatment
  OR
  (is_reserved)        -- Phase 1 Path 1: reserved rows may have empty OR placeholder text
);

COMMENT ON CONSTRAINT chk_rule_text_status_invariant ON wa_court_rules.rules IS
  'Path 1 loosened 2026-05-01 (Phase 4 boundary): is_repealed rows may have '
  'empty OR placeholder text (mirroring is_reserved branch). 47 of 51 '
  'repealed rows in the WA Court Rules corpus carry meaningful placeholder '
  'markers like [RESCINDED], [Deleted], [Rescinded. Provisions transferred '
  'to rule 13.] — the original CHECK was over-strict relative to corpus '
  'reality. Ghost-flag tags (court-rule-rescinded, court-rule-reserved) bear '
  'retrieval-time filter weight; this CHECK now encodes corpus shape, not '
  'idealized ghost-flag-implies-empty-body assumption.';


-- =============================================================================
-- §2  ADD chk_repealed_text_meaningful (defensive regex audit)
-- =============================================================================
-- Structurally-different audit channel from the ghost-flag column itself.
-- Operationally: if a row claims is_repealed but the body has no recognized
-- rescission marker, it suggests parser bug or mis-classification rather
-- than actual rescission. The CHECK fires before INSERT lands, surfacing
-- the anomaly cleanly.
--
-- Recognized markers (case-insensitive): rescind, deleted, repealed,
-- removed, transferred. Empty/NULL body always passes (intentional —
-- some rules are genuinely repealed without redirect text).
--
-- This is the kind of audit-predicate-diversification pattern §K
-- corroborations #14-16 endorse: schema CHECK on body-text content
-- structurally-different from parser's ghost-flag classification.

ALTER TABLE wa_court_rules.rules
  ADD CONSTRAINT chk_repealed_text_meaningful CHECK (
    NOT is_repealed
    OR full_text IS NULL
    OR full_text = ''
    OR full_text ~* '(rescind|deleted|repealed|removed|transferred)'
  );

COMMENT ON CONSTRAINT chk_repealed_text_meaningful ON wa_court_rules.rules IS
  'Defensive audit on repealed-rule body text: if is_repealed=TRUE and '
  'body is non-empty, body must contain a recognized rescission/deletion/'
  'redirect marker (rescind/deleted/repealed/removed/transferred, '
  'case-insensitive). Empty/NULL body always passes. Catches parser '
  'mis-classification where a row is flagged repealed but body has no '
  'rescission marker. Structurally-different audit channel from is_repealed '
  'column itself — Phase 6 §K audit-predicate-diversification doctrine.';


-- =============================================================================
-- §POST  POST-APPLY GUARDS
-- =============================================================================

DO $$
DECLARE
  loosened_inv_def       TEXT;
  meaningful_def         TEXT;
  meaningful_count       INTEGER;
  total_check_count      INTEGER;
BEGIN
  -- §POST.1 chk_rule_text_status_invariant has the LOOSENED form
  --   (does NOT contain 'is_repealed AND' clause; the new form has '(is_repealed)' standalone)
  SELECT pg_get_constraintdef(oid) INTO loosened_inv_def
    FROM pg_constraint
    WHERE conname = 'chk_rule_text_status_invariant'
      AND conrelid = 'wa_court_rules.rules'::regclass;
  IF loosened_inv_def IS NULL THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1a]: chk_rule_text_status_invariant missing after swap.';
  END IF;
  -- The loosened form should NOT contain the strict 'is_repealed AND ...empty' conjunction
  IF loosened_inv_def LIKE '%is_repealed AND%' AND loosened_inv_def LIKE '%full_text IS NULL%' THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1b]: chk_rule_text_status_invariant still has strict form. Swap did not apply: %', loosened_inv_def;
  END IF;

  -- §POST.2 chk_repealed_text_meaningful present + correct
  SELECT COUNT(*), MAX(pg_get_constraintdef(oid))
    INTO meaningful_count, meaningful_def
    FROM pg_constraint
    WHERE conname = 'chk_repealed_text_meaningful'
      AND conrelid = 'wa_court_rules.rules'::regclass;
  IF meaningful_count <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2a]: chk_repealed_text_meaningful missing.';
  END IF;
  IF meaningful_def NOT LIKE '%is_repealed%' OR meaningful_def NOT LIKE '%rescind%' THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2b]: chk_repealed_text_meaningful definition unexpected: %', meaningful_def;
  END IF;

  -- §POST.3 Total CHECK constraints on rules: 7 (5 baseline) + 2 (extension #1) + 1 (extension #2 new) = 8
  SELECT COUNT(*) INTO total_check_count
    FROM pg_constraint
    WHERE conrelid = 'wa_court_rules.rules'::regclass
      AND contype = 'c';
  IF total_check_count <> 8 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.3]: wa_court_rules.rules has % CHECK constraints; expected 8 (5 baseline + 2 extension #1 + 1 extension #2 new).', total_check_count;
  END IF;

  RAISE NOTICE 'Post-apply: PASS — chk_rule_text_status_invariant loosened (is_repealed branch standalone); chk_repealed_text_meaningful added; total CHECK constraint count on rules = 8.';
END $$;


-- =============================================================================
-- §V  VERIFICATION SELECTs
-- =============================================================================

\echo ''
\echo '=== §V.1 All CHECK constraints on wa_court_rules.rules (8 expected) ==='
SELECT conname AS constraint_name, pg_get_constraintdef(oid) AS definition
  FROM pg_constraint
  WHERE conrelid = 'wa_court_rules.rules'::regclass
    AND contype = 'c'
  ORDER BY conname;

\echo ''
\echo '=== §V.2 chk_rule_text_status_invariant before/after comparison (post-state) ==='
SELECT conname, pg_get_constraintdef(oid) AS loosened_definition
  FROM pg_constraint
  WHERE conname = 'chk_rule_text_status_invariant'
    AND conrelid = 'wa_court_rules.rules'::regclass;


COMMIT;

\echo ''
\echo '=== Phase 1 schema extension #2: COMPLETE ==='
\echo 'Phase 4 ingest can now resume — re-run phase4_ingest.py from clean state.'
