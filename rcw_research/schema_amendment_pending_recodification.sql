-- ============================================================================
-- RCW Phase 4 Schema Amendment — pending_recodification fields
-- ============================================================================
-- Status:        APPLY-READY — atomic single-transaction migration
-- Generated:     2026-04-29 by wdws agent
-- Operator:      William J. Nelson — apply via psql, not autonomously
--
-- Adds two columns + matching indexes + a paired-NULL CHECK to rcw.sections,
-- representing the "pre-effective recodification" state observed in 100 RCW
-- sections (mostly Title 42.17A.* → 29B.*) where a future recodification is
-- announced in the live heading text but not yet effective.
--
-- The original chk_section_text_status_invariant CHECK constraint is preserved
-- intact; this amendment ADDS new columns rather than relaxing existing ones.
-- Two new partial indexes support the future Phase 7 maintenance job that
-- promotes "pending" → "applied" recodifications when the effective_date
-- arrives.
--
-- Pre-execution requirements: rcw.sections exists (i.e., Phase 1 schema_apply.sql
-- already applied). Pre-flight guards verify the new columns don't already exist
-- so this script is safe to re-apply (DO blocks raise EXCEPTION on second run).
--
-- HOW TO APPLY (operator instructions):
--   psql -h 127.0.0.1 -U wdws -d wdws -f \
--     /opt/wdws/rcw_research/schema_amendment_pending_recodification.sql
--
-- Capture the entire output and paste back to the agent for the parser work
-- block continuation.
-- ============================================================================

\set ON_ERROR_STOP on
\timing on

BEGIN;


-- ============================================================================
-- §0 PRE-EXECUTION GUARDS
-- ============================================================================

DO $$
DECLARE
  has_pending_to       INTEGER;
  has_pending_eff      INTEGER;
  has_chk              INTEGER;
  rcw_sections_exists  INTEGER;
  rcw_section_count    INTEGER;
BEGIN
  -- §0.1 rcw.sections must already exist (Phase 1 must have landed)
  SELECT COUNT(*) INTO rcw_sections_exists FROM information_schema.tables
    WHERE table_schema='rcw' AND table_name='sections';
  IF rcw_sections_exists <> 1 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.1]: rcw.sections does not exist. Apply Phase 1 schema first.';
  END IF;

  -- §0.2 New column pending_recodification_to must NOT yet exist
  SELECT COUNT(*) INTO has_pending_to FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='sections'
      AND column_name='pending_recodification_to';
  IF has_pending_to <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2]: pending_recodification_to column already exists.';
  END IF;

  -- §0.3 New column pending_recodification_effective must NOT yet exist
  SELECT COUNT(*) INTO has_pending_eff FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='sections'
      AND column_name='pending_recodification_effective';
  IF has_pending_eff <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.3]: pending_recodification_effective column already exists.';
  END IF;

  -- §0.4 The new CHECK constraint must NOT yet exist
  SELECT COUNT(*) INTO has_chk FROM pg_constraint
    WHERE conname='chk_pending_recodification_pair';
  IF has_chk <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4]: chk_pending_recodification_pair constraint already exists.';
  END IF;

  -- §0.5 Snapshot current rcw.sections count for post-apply verification
  SELECT COUNT(*) INTO rcw_section_count FROM rcw.sections;
  RAISE NOTICE 'Pre-flight guards: PASS (rcw.sections has % rows; will be unchanged after this migration)', rcw_section_count;
END $$;


-- ============================================================================
-- §1 ADD COLUMNS
-- ============================================================================

ALTER TABLE rcw.sections
  ADD COLUMN pending_recodification_to        TEXT,
  ADD COLUMN pending_recodification_effective DATE;

COMMENT ON COLUMN rcw.sections.pending_recodification_to IS
  'Future canonical_citation when a recodification is announced but not yet '
  'effective (e.g., RCW 42.17A.001 heading reads "Effective until January 1, '
  '2026. Recodified as RCW 29B.05.010.")  Set to ''RCW 29B.05.010''. '
  'NULL when no pending recodification. Distinct from recodified_to (which '
  'applies once is_recodified=TRUE / the recodification has taken effect).';

COMMENT ON COLUMN rcw.sections.pending_recodification_effective IS
  'Date the announced recodification takes effect. NULL when no pending '
  'recodification. Phase 7 maintenance flips pending → applied (sets '
  'is_recodified=TRUE, copies pending_recodification_to to recodified_to, '
  'NULLs the body, NULLs both pending fields) when this date arrives.';


-- ============================================================================
-- §2 ADD PAIRED-NULL CHECK
-- ============================================================================
-- Either both fields are NULL or both populated. Prevents half-set state.

ALTER TABLE rcw.sections
  ADD CONSTRAINT chk_pending_recodification_pair CHECK (
    (pending_recodification_to IS NULL AND pending_recodification_effective IS NULL)
    OR
    (pending_recodification_to IS NOT NULL AND pending_recodification_effective IS NOT NULL)
  );


-- ============================================================================
-- §3 ADD PARTIAL INDEXES
-- ============================================================================

CREATE INDEX idx_rcw_sections_pending_recodif_to
  ON rcw.sections(pending_recodification_to)
  WHERE pending_recodification_to IS NOT NULL;

CREATE INDEX idx_rcw_sections_pending_recodif_effective
  ON rcw.sections(pending_recodification_effective)
  WHERE pending_recodification_effective IS NOT NULL;


-- ============================================================================
-- §POST-APPLY GUARDS
-- ============================================================================

DO $$
DECLARE
  has_pending_to       INTEGER;
  has_pending_eff      INTEGER;
  has_chk              INTEGER;
  has_idx_to           INTEGER;
  has_idx_eff          INTEGER;
  rcw_section_count    INTEGER;
  populated_count      INTEGER;
BEGIN
  SELECT COUNT(*) INTO has_pending_to FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='sections'
      AND column_name='pending_recodification_to';
  IF has_pending_to <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL: pending_recodification_to column not added';
  END IF;

  SELECT COUNT(*) INTO has_pending_eff FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='sections'
      AND column_name='pending_recodification_effective';
  IF has_pending_eff <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL: pending_recodification_effective column not added';
  END IF;

  SELECT COUNT(*) INTO has_chk FROM pg_constraint
    WHERE conname='chk_pending_recodification_pair';
  IF has_chk <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL: chk_pending_recodification_pair not added';
  END IF;

  SELECT COUNT(*) INTO has_idx_to FROM pg_indexes
    WHERE schemaname='rcw' AND indexname='idx_rcw_sections_pending_recodif_to';
  IF has_idx_to <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL: idx_rcw_sections_pending_recodif_to not created';
  END IF;

  SELECT COUNT(*) INTO has_idx_eff FROM pg_indexes
    WHERE schemaname='rcw' AND indexname='idx_rcw_sections_pending_recodif_effective';
  IF has_idx_eff <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL: idx_rcw_sections_pending_recodif_effective not created';
  END IF;

  -- Verify existing rcw.sections rows still exist (no data loss)
  SELECT COUNT(*) INTO rcw_section_count FROM rcw.sections;

  -- All existing rows should have NULL pending fields (no parser run yet)
  SELECT COUNT(*) INTO populated_count FROM rcw.sections
    WHERE pending_recodification_to IS NOT NULL
       OR pending_recodification_effective IS NOT NULL;
  IF populated_count <> 0 THEN
    RAISE EXCEPTION 'Post-apply FAIL: % existing rows already have populated pending fields (expected 0)', populated_count;
  END IF;

  RAISE NOTICE 'Post-apply guards: PASS (% rcw.sections rows preserved, both new columns/indexes/constraint added cleanly)', rcw_section_count;
END $$;


-- ============================================================================
-- COMMIT
-- ============================================================================

COMMIT;


-- ============================================================================
-- §4 POST-COMMIT VERIFICATION SELECTS
-- ============================================================================

\echo '--- Verification: schema state ---'
SELECT
  EXISTS (SELECT 1 FROM information_schema.columns
          WHERE table_schema='rcw' AND table_name='sections'
            AND column_name='pending_recodification_to')        AS col_pending_to,
  EXISTS (SELECT 1 FROM information_schema.columns
          WHERE table_schema='rcw' AND table_name='sections'
            AND column_name='pending_recodification_effective') AS col_pending_eff,
  EXISTS (SELECT 1 FROM pg_constraint
          WHERE conname='chk_pending_recodification_pair')      AS chk_pair,
  EXISTS (SELECT 1 FROM pg_indexes
          WHERE schemaname='rcw'
            AND indexname='idx_rcw_sections_pending_recodif_to')        AS idx_to,
  EXISTS (SELECT 1 FROM pg_indexes
          WHERE schemaname='rcw'
            AND indexname='idx_rcw_sections_pending_recodif_effective') AS idx_eff;

\echo ''
\echo '--- Verification: existing rcw.sections data preserved ---'
SELECT
  COUNT(*)                                              AS total_sections,
  COUNT(*) FILTER (WHERE full_text IS NOT NULL)         AS sections_with_text,
  COUNT(*) FILTER (WHERE pending_recodification_to IS NOT NULL) AS pending_recodif_count;

\echo ''
\echo '=== END schema_amendment_pending_recodification.sql ==='
