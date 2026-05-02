-- =====================================================================
-- RCW Phase 4.4 — Cross-Reference FK Resolution
-- =====================================================================
-- Resolves rcw.cross_references.target_section_id for internal RCW refs
-- via direct equality match between target_citation and canonical_citation.
--
-- Background: rcw.cross_references.target_section_id was left NULL during
-- the original Phase 3/4 ingest. 2026-05-01 audit: 0 of 75,152 rows have
-- the FK populated. Of the 75,117 state-wa-rcw refs:
--   - 60,510 are section-level form (A+B regex classes) → FK-resolvable
--   - 14,607 are chapter/title-level form → inherently unresolvable to
--     a section FK (no section to point to)
--   -  6,703 (subset of A+B) are section-level form pointing to genuinely-
--     missing sections (repealed-and-removed; no ghost row in rcw.sections)
--
-- Approach: single direct-equality UPDATE. No subsection paren stripping
-- needed (Phase 3 parser already strips). No recodified_to redirection
-- (mapping yields 0 additional matches).
--
-- Out of scope (per kickoff): wa-constitution (34) and cfr (1) jurisdiction
-- references; both have no target table to FK to. They remain NULL.
--
-- Pattern source: /opt/wdws/usc_research/phase4_4_fk_resolution.sql
-- =====================================================================

BEGIN;

-- Pre-flight: nothing should already be resolved
DO $$
DECLARE
  pre_resolved INTEGER;
BEGIN
  SELECT COUNT(*) INTO pre_resolved
  FROM rcw.cross_references WHERE target_section_id IS NOT NULL;
  IF pre_resolved <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL: % rows already have target_section_id populated. Investigate before remediation.', pre_resolved;
  END IF;
  RAISE NOTICE 'Pre-flight OK: 0 rows pre-resolved';
END $$;

-- Resolve via direct text-match (target_citation = canonical_citation)
UPDATE rcw.cross_references c
SET target_section_id = s.id
FROM rcw.sections s
WHERE c.target_section_id IS NULL
  AND c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_citation = s.canonical_citation;

-- Post-apply guard: expected ~52,588 resolved (87% of A+B section-level form,
-- 70% of state-wa-rcw raw). Halt below 50,000 (clear under-resolution) or
-- above 65,000 (clear over-resolution; would imply JOIN cardinality issue).
DO $$
DECLARE
  post_resolved INTEGER;
BEGIN
  SELECT COUNT(*) INTO post_resolved
  FROM rcw.cross_references WHERE target_section_id IS NOT NULL;
  IF post_resolved < 50000 THEN
    RAISE EXCEPTION 'Post-apply FAIL: only % rows resolved (expected ~52,588). Diagnostic mismatch.', post_resolved;
  END IF;
  IF post_resolved > 65000 THEN
    RAISE EXCEPTION 'Post-apply FAIL: % rows resolved exceeds upper band (expected ~52,588). Possible JOIN cardinality issue.', post_resolved;
  END IF;
  RAISE NOTICE 'Post-apply OK: % rows resolved', post_resolved;
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
    WHEN target_citation ~ '^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?[\.\-][0-9]+[A-Za-z]*$' THEN 'A+B section-form (target repealed-and-removed; no ghost row)'
    ELSE 'Z_other (residual)'
  END AS reason,
  COUNT(*)
FROM rcw.cross_references
WHERE target_jurisdiction = 'state-wa-rcw' AND target_section_id IS NULL
GROUP BY reason ORDER BY COUNT(*) DESC;

\echo ''
\echo '=== END phase4_4_fk_resolution.sql ==='
