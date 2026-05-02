-- =====================================================================
-- WA Court Rules Phase 4.4 — Cross-corpus FK Resolution (Option (b))
-- =====================================================================
-- Mirrors WAC Phase 4.4 Option (b) precedent. Original schema has
-- target_rule_id FK to wa_court_rules.rules(id) ONLY (intra-corpus).
-- Cross-corpus refs (state-wa-rcw, federal-usc) carry target_citation
-- text but no FK column — this script adds the columns + resolves.
--
-- Pre-flight (verified 2026-05-01):
--   intra-corpus  state-wa-court-rule: 791/812 resolved (97.41%, structural ceiling)
--   cross-corpus  state-wa-rcw:        0/1,041 (FK column missing)
--   cross-corpus  federal-usc:         0/9 (FK column missing)
--   cross-corpus  wa-constitution:     0/4 (no target table — out of scope)
--
-- Anti-join projection (post-reconciliation, see §K #33):
--   state-wa-rcw: 981/1,041 resolvable (94.24%) — 885 direct + 96 chapter-only
--   federal-usc: 6/9 resolvable (66.67%) — 4 direct + 2 pinpoint-stripped;
--                3 are section-range form (12101-12213/12231), structurally
--                non-resolvable to single section_id.
--
-- §K #33 reconciliation note: Pass 2 chapter-only candidates use
--   regex `^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?$` (operator-reconciled)
--   not `^RCW \d+\.\d+$` (agent's original, missed 17 letter-suffix rows
--   like RCW 9.94A, 26.26A, 9A.28). Both CTE filter AND regexp_replace
--   anchor in Pass 2 must use the letter-suffix-tolerant pattern.
-- =====================================================================

BEGIN;

-- Pre-flight: confirm columns don't already exist (idempotency-friendly halt)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='wa_court_rules' AND table_name='cross_references'
               AND column_name='target_rcw_section_id') THEN
    RAISE EXCEPTION 'Pre-flight FAIL: target_rcw_section_id already exists. Investigate.';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='wa_court_rules' AND table_name='cross_references'
               AND column_name='target_usc_section_id') THEN
    RAISE EXCEPTION 'Pre-flight FAIL: target_usc_section_id already exists. Investigate.';
  END IF;
  RAISE NOTICE 'Pre-flight OK: schema clean for additive change';
END $$;

-- Additive schema change: two new FK columns, NULL by default
ALTER TABLE wa_court_rules.cross_references
  ADD COLUMN target_rcw_section_id UUID REFERENCES rcw.sections(id),
  ADD COLUMN target_usc_section_id UUID REFERENCES usc.sections(id);

-- Pass 1 — state-wa-rcw direct exact match
UPDATE wa_court_rules.cross_references c
SET target_rcw_section_id = s.id
FROM rcw.sections s
WHERE c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_rcw_section_id IS NULL
  AND s.canonical_citation = c.target_citation;

-- Pass 2 — state-wa-rcw chapter-only refs (letter-suffix tolerant, §K #33)
-- Pattern: 'RCW [0-9]+[A-Z]?.[0-9]+[A-Z]?' → first section in chapter
-- Pass 2 candidates: 96 rows (operator-reconciled; agent original was 79
--   due to missing letter-suffix tolerance — see §K #33).
WITH chapter_first AS (
  SELECT DISTINCT ON (chap)
         regexp_replace(canonical_citation,
                        '^(RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?)\..*$', '\1') AS chap,
         id
  FROM rcw.sections
  WHERE canonical_citation ~ '^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?\.'
  ORDER BY chap, canonical_citation
)
UPDATE wa_court_rules.cross_references c
SET target_rcw_section_id = cf.id
FROM chapter_first cf
WHERE c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_rcw_section_id IS NULL
  AND c.target_citation = cf.chap;

-- Pass 3 — federal-usc direct exact match
UPDATE wa_court_rules.cross_references c
SET target_usc_section_id = s.id
FROM usc.sections s
WHERE c.target_jurisdiction = 'federal-usc'
  AND c.target_usc_section_id IS NULL
  AND s.canonical_citation = c.target_citation;

-- Pass 4 — federal-usc with subsection pinpoint stripped ('20 U.S.C. § 1091(r)' → '20 U.S.C. § 1091')
UPDATE wa_court_rules.cross_references c
SET target_usc_section_id = s.id
FROM usc.sections s
WHERE c.target_jurisdiction = 'federal-usc'
  AND c.target_usc_section_id IS NULL
  AND s.canonical_citation = regexp_replace(c.target_citation, '\(.*$', '');

-- Post-apply guards (halt-band per §K #28/#32 calibration)
DO $$
DECLARE
  rcw_resolved INTEGER;
  usc_resolved INTEGER;
  rcw_total INTEGER;
  usc_total INTEGER;
BEGIN
  SELECT COUNT(*) INTO rcw_total FROM wa_court_rules.cross_references
    WHERE target_jurisdiction='state-wa-rcw';
  SELECT COUNT(*) INTO rcw_resolved FROM wa_court_rules.cross_references
    WHERE target_jurisdiction='state-wa-rcw' AND target_rcw_section_id IS NOT NULL;
  SELECT COUNT(*) INTO usc_total FROM wa_court_rules.cross_references
    WHERE target_jurisdiction='federal-usc';
  SELECT COUNT(*) INTO usc_resolved FROM wa_court_rules.cross_references
    WHERE target_jurisdiction='federal-usc' AND target_usc_section_id IS NOT NULL;
  -- Post-reconciliation halt-bands (§K #33):
  --   Pass 1 + Pass 2 combined floor: 952 (= 70% × 96 chapter-only + 885 direct)
  --   Combined ceiling: 1,012 (5% buffer over reconciled 981 projection)
  --   USC: hard target 6 (small enumerated population)
  IF rcw_resolved < 952 THEN
    RAISE EXCEPTION 'Post-apply FAIL (RCW): only % resolved of % (combined floor 952; expected ~981 = 885 direct + 96 chapter-only). Investigate.',
                    rcw_resolved, rcw_total;
  END IF;
  IF rcw_resolved > 1012 THEN
    RAISE EXCEPTION 'Post-apply FAIL (RCW): % resolved exceeds ceiling (1,012). Likely false-positive joins.',
                    rcw_resolved;
  END IF;
  IF usc_resolved <> 6 THEN
    RAISE EXCEPTION 'Post-apply FAIL (USC): % resolved (expected exactly 6). Surface mismatch.',
                    usc_resolved;
  END IF;
  RAISE NOTICE 'Post-apply OK: RCW resolved=%/%  USC resolved=%/%',
               rcw_resolved, rcw_total, usc_resolved, usc_total;
END $$;

COMMIT;
