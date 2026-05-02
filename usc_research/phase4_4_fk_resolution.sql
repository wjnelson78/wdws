-- =====================================================================
-- Phase 4.4 — Cross-Reference FK Resolution (deferred from original ingest)
-- =====================================================================
-- Resolves usc.cross_references.target_section_id for internal refs by
-- text-matching target_citation against the canonical
-- "{title_number} U.S.C. § {section_number}" form of usc.sections.
--
-- Background: schema_design.sql:209 and usc_ingest.py:586 marked this
-- step "resolved in Phase 4.4" but the second pass was never written.
-- Investigation 2026-05-01 confirmed 0 of 271,590 rows had FK populated;
-- 52,759 internal refs are immediately resolvable via text-match against
-- already-ingested priority-core titles (5/28/29/42).
--
-- Approach: text-match (indexable via idx_xref_target on target_citation
-- + the implicit btree on usc.sections.section_number). uslm_href-based
-- resolution would catch additional sub-section pinpoints but the text-
-- match path is sufficient for the documented Phase 4.4 scope.
-- =====================================================================

BEGIN;

-- Pre-flight: nothing should already be resolved
DO $$
DECLARE
  pre_resolved INTEGER;
BEGIN
  SELECT COUNT(*) INTO pre_resolved
  FROM usc.cross_references WHERE target_section_id IS NOT NULL;
  IF pre_resolved <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL: % rows already have target_section_id populated. Investigate before remediation.', pre_resolved;
  END IF;
  RAISE NOTICE 'Pre-flight OK: 0 rows pre-resolved';
END $$;

-- Resolve via text-match
UPDATE usc.cross_references c
SET target_section_id = s.id
FROM usc.sections s
WHERE c.target_section_id IS NULL
  AND c.target_citation = s.title_number || ' U.S.C. § ' || s.section_number;

-- Post-apply guard
DO $$
DECLARE
  post_resolved INTEGER;
BEGIN
  SELECT COUNT(*) INTO post_resolved
  FROM usc.cross_references WHERE target_section_id IS NOT NULL;
  IF post_resolved < 50000 THEN
    RAISE EXCEPTION 'Post-apply FAIL: only % rows resolved (expected ~52,759). Diagnostic mismatch.', post_resolved;
  END IF;
  RAISE NOTICE 'Post-apply OK: % rows resolved', post_resolved;
END $$;

COMMIT;
