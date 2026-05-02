-- =====================================================================
-- Refinement Sweep — Tasks 1-3 (idempotent re-run, post-RCW-Phase-8)
-- =====================================================================
-- Purpose: close FK gaps in wa_court_rules.cross_references and
--          wac.cross_references that are now resolvable because RCW
--          Phase 8 added recodified-now-ghost RCW rows.
--
-- Schema-static. NO ALTER TABLE — target_rcw_section_id and
-- target_usc_section_id columns already exist in both cross_references
-- tables (added by phase4_4_wa_court_rules_fk_resolution.sql and the
-- WAC Phase 4.4 Option (b) precedent).
--
-- Pre-flight (verified 2026-05-01, all match operator-projected exactly):
--   Task 1 — WCR→RCW NULL candidates resolvable: 15
--   Task 2 — WAC→RCW NULL candidates resolvable: 842
--   Task 3 — WAC→USC subsection-strip NULL candidates resolvable: 12
--
-- Halt bands (per operator):
--   Task 1: [10, 20]
--   Task 2: [800, 900]
--   Task 3: [10, 14]
-- =====================================================================

BEGIN;

-- Defensive: confirm FK columns exist (would mean prior phases applied)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='wa_court_rules' AND table_name='cross_references'
                   AND column_name='target_rcw_section_id') THEN
    RAISE EXCEPTION 'Pre-flight FAIL: wa_court_rules.cross_references.target_rcw_section_id missing.';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='wac' AND table_name='cross_references'
                   AND column_name='target_rcw_section_id') THEN
    RAISE EXCEPTION 'Pre-flight FAIL: wac.cross_references.target_rcw_section_id missing.';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_schema='wac' AND table_name='cross_references'
                   AND column_name='target_usc_section_id') THEN
    RAISE EXCEPTION 'Pre-flight FAIL: wac.cross_references.target_usc_section_id missing.';
  END IF;
  RAISE NOTICE 'Pre-flight OK: all FK columns present';
END $$;

-- ---------------------------------------------------------------------
-- Task 1 — WCR→RCW direct exact match against post-Phase-8 RCW corpus
-- ---------------------------------------------------------------------
UPDATE wa_court_rules.cross_references c
SET target_rcw_section_id = s.id
FROM rcw.sections s
WHERE c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_rcw_section_id IS NULL
  AND s.canonical_citation = c.target_citation;

-- ---------------------------------------------------------------------
-- Task 2 — WAC→RCW direct exact match against post-Phase-8 RCW corpus
-- ---------------------------------------------------------------------
UPDATE wac.cross_references c
SET target_rcw_section_id = s.id
FROM rcw.sections s
WHERE c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_rcw_section_id IS NULL
  AND s.canonical_citation = c.target_citation;

-- ---------------------------------------------------------------------
-- Task 3 — WAC→USC subsection-strip JOIN
--   Strip pinpoint at match time; USC canonical stays section-level.
-- ---------------------------------------------------------------------
UPDATE wac.cross_references c
SET target_usc_section_id = s.id
FROM usc.sections s
WHERE c.target_jurisdiction = 'federal-usc'
  AND c.target_usc_section_id IS NULL
  AND c.target_citation ~ '\([a-z0-9]+\)'
  AND s.canonical_citation = regexp_replace(c.target_citation, '\([a-z0-9]+\)', '');

-- ---------------------------------------------------------------------
-- Post-apply halt bands (per operator)
-- ---------------------------------------------------------------------
DO $$
DECLARE
  wcr_rcw_now    INTEGER;
  wac_rcw_now    INTEGER;
  wac_usc_now    INTEGER;
  wcr_rcw_delta  INTEGER;
  wac_rcw_delta  INTEGER;
  wac_usc_delta  INTEGER;
BEGIN
  SELECT COUNT(*) INTO wcr_rcw_now FROM wa_court_rules.cross_references
    WHERE target_jurisdiction='state-wa-rcw' AND target_rcw_section_id IS NOT NULL;
  SELECT COUNT(*) INTO wac_rcw_now FROM wac.cross_references
    WHERE target_jurisdiction='state-wa-rcw' AND target_rcw_section_id IS NOT NULL;
  SELECT COUNT(*) INTO wac_usc_now FROM wac.cross_references
    WHERE target_jurisdiction='federal-usc' AND target_usc_section_id IS NOT NULL;

  -- Pre-write baselines (captured 2026-05-01, this transaction):
  --   WCR→RCW resolved: 981
  --   WAC→RCW resolved: 24120
  --   WAC→USC resolved: 82
  wcr_rcw_delta := wcr_rcw_now - 981;
  wac_rcw_delta := wac_rcw_now - 24120;
  wac_usc_delta := wac_usc_now - 82;

  RAISE NOTICE 'Task 1 (WCR→RCW) delta: % (resolved now=%)',  wcr_rcw_delta, wcr_rcw_now;
  RAISE NOTICE 'Task 2 (WAC→RCW) delta: % (resolved now=%)',  wac_rcw_delta, wac_rcw_now;
  RAISE NOTICE 'Task 3 (WAC→USC) delta: % (resolved now=%)',  wac_usc_delta, wac_usc_now;

  IF wcr_rcw_delta NOT BETWEEN 10 AND 20 THEN
    RAISE EXCEPTION 'HALT Task 1: delta=% outside [10,20]', wcr_rcw_delta;
  END IF;
  IF wac_rcw_delta NOT BETWEEN 800 AND 900 THEN
    RAISE EXCEPTION 'HALT Task 2: delta=% outside [800,900]', wac_rcw_delta;
  END IF;
  IF wac_usc_delta NOT BETWEEN 10 AND 14 THEN
    RAISE EXCEPTION 'HALT Task 3: delta=% outside [10,14]', wac_usc_delta;
  END IF;

  RAISE NOTICE 'All three tasks within halt bands. Committing.';
END $$;

COMMIT;
