-- =====================================================================
-- RCW Phase 4.4 — Idempotent FK Resolution Re-run (post-Phase-8)
-- =====================================================================
-- Companion to phase4_4_fk_resolution.sql, designed to be re-runnable
-- after Phase 8 ghost-row materialization. The original script's pre-flight
-- (assert 0 pre-resolved) blocks re-execution; this script captures the
-- pre-state, runs the same UPDATE, and verifies an UPWARD delta.
--
-- Filters: state-wa-rcw jurisdiction, target_section_id IS NULL, direct
-- equality match between target_citation and canonical_citation. Identical
-- semantics to the original. No risk of double-resolution: the
-- target_section_id IS NULL filter prevents touching already-populated rows.
-- =====================================================================

\echo ''
\echo '--- Pre-state (existing FK resolution before Phase 8 effects) ---'
SELECT target_jurisdiction,
       COUNT(*)                                              AS total,
       COUNT(*) FILTER (WHERE target_section_id IS NOT NULL) AS resolved,
       ROUND(100.0 * COUNT(*) FILTER (WHERE target_section_id IS NOT NULL)
             / NULLIF(COUNT(*), 0), 2)                       AS pct
FROM rcw.cross_references
GROUP BY target_jurisdiction
ORDER BY total DESC;

BEGIN;

-- Capture pre-state count for the upward-delta guard
DO $$
DECLARE
  pre_resolved INTEGER;
BEGIN
  SELECT COUNT(*) INTO pre_resolved
  FROM rcw.cross_references WHERE target_section_id IS NOT NULL;
  RAISE NOTICE 'Pre-state: % rows resolved', pre_resolved;
  PERFORM set_config('app.fk_rerun_pre_resolved', pre_resolved::text, true);
END $$;

-- Same UPDATE as the original script — idempotent because of the IS NULL filter
UPDATE rcw.cross_references c
SET target_section_id = s.id
FROM rcw.sections s
WHERE c.target_section_id IS NULL
  AND c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_citation = s.canonical_citation;

-- Post-apply: confirm UPWARD delta. Halt if delta < 5,000 (suggests Phase 8
-- ghost-row materialization didn't take effect) or > 8,000 (suggests
-- over-match — would imply ghost rows duplicated or JOIN cardinality issue).
DO $$
DECLARE
  pre_resolved INTEGER := current_setting('app.fk_rerun_pre_resolved')::int;
  post_resolved INTEGER;
  delta INTEGER;
BEGIN
  SELECT COUNT(*) INTO post_resolved
  FROM rcw.cross_references WHERE target_section_id IS NOT NULL;
  delta := post_resolved - pre_resolved;
  RAISE NOTICE 'Post-state: % rows resolved (delta +%)', post_resolved, delta;
  IF delta < 5000 THEN
    RAISE EXCEPTION 'Re-run FAIL: delta +% below floor 5,000 (Phase 8 ghosts insufficient)', delta;
  END IF;
  IF delta > 8000 THEN
    RAISE EXCEPTION 'Re-run FAIL: delta +% above ceiling 8,000 (over-match risk)', delta;
  END IF;
  RAISE NOTICE 'Post-apply OK: delta +% within band [5,000-8,000]', delta;
END $$;

COMMIT;

\echo ''
\echo '--- Final RCW cross_references resolution by jurisdiction ---'
SELECT target_jurisdiction,
       COUNT(*)                                              AS total,
       COUNT(*) FILTER (WHERE target_section_id IS NOT NULL) AS resolved,
       ROUND(100.0 * COUNT(*) FILTER (WHERE target_section_id IS NOT NULL)
             / NULLIF(COUNT(*), 0), 2)                       AS pct
FROM rcw.cross_references
GROUP BY target_jurisdiction
ORDER BY total DESC;

\echo ''
\echo '--- Residual NULL breakdown by structural class (state-wa-rcw) ---'
SELECT
  CASE
    WHEN target_citation ~ '^chapter [0-9]+[A-Z]?\.[0-9]+[A-Z]? RCW$' THEN 'D_chapter_natural (inherently unresolvable to section FK)'
    WHEN target_citation ~ '^Title [0-9]+[A-Z]? RCW$' THEN 'E_title (inherently unresolvable to section FK)'
    WHEN target_citation ~ '^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?[\.\-][0-9]+[A-Za-z]*$' THEN 'A+B section-form (still unresolved; no ghost row created)'
    ELSE 'Z_other (residual)'
  END AS reason,
  COUNT(*)
FROM rcw.cross_references
WHERE target_jurisdiction = 'state-wa-rcw' AND target_section_id IS NULL
GROUP BY reason ORDER BY COUNT(*) DESC;

\echo ''
\echo '=== END phase4_4_fk_resolution_rerun.sql ==='
