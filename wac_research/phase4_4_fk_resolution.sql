-- =====================================================================
-- WAC Phase 4.4 — Cross-Reference FK Resolution (Option (b) — additive schema)
-- =====================================================================
-- Operator-authorized scope (kickoff #2, 2026-05-01):
--   - ALTER TABLE adding two new FK columns to wac.cross_references:
--       target_rcw_section_id UUID REFERENCES rcw.sections(id)
--       target_usc_section_id UUID REFERENCES usc.sections(id)
--   - UPDATE wac.cross_references for three resolution scopes:
--       §1  state-wa-wac self-refs → target_section_id (existing column)
--       §2  state-wa-rcw refs      → target_rcw_section_id (new column)
--       §3  federal-usc refs       → target_usc_section_id (new column)
--   - Existing target_section_id values preserved (none currently populated;
--     pre-flight verifies). Schema change is additive — non-destructive.
--
-- Anti-join projections (verified 2026-05-01):
--   §1 self-refs:     44,554 / 52,152 = 85.43% expected
--   §2 WAC→RCW:       24,120 / 36,610 = 65.88% expected (looser band — RCW recodifications)
--   §3 WAC→USC:           82 /    105 = 78.10% expected (subsection notation residue)
--   TOTAL:           68,756 / 89,082 = 77.2% across resolvable jurisdictions
--
-- Out-of-scope NULL preserved (no target table by design):
--   cfr:               199 rows
--   wa-constitution:    16 rows
--   TOTAL:             215 rows
--
-- Single transaction. All DDL + DML in one BEGIN/COMMIT. Rollback-safe
-- if any post-apply guard fails.
-- =====================================================================

BEGIN;

-- =====================================================================
-- §0 Pre-flight guards
-- =====================================================================
DO $$
DECLARE
  pre_resolved_self INTEGER;
  pre_columns INTEGER;
  pre_authority_section_resolved INTEGER;
  pre_oos_null INTEGER;
BEGIN
  -- §0.1: target_section_id must not yet be populated
  SELECT COUNT(*) INTO pre_resolved_self
  FROM wac.cross_references WHERE target_section_id IS NOT NULL;
  IF pre_resolved_self <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.1]: % rows already have target_section_id populated. Investigate before remediation.', pre_resolved_self;
  END IF;

  -- §0.2: new columns must not yet exist
  SELECT COUNT(*) INTO pre_columns FROM information_schema.columns
   WHERE table_schema='wac' AND table_name='cross_references'
     AND column_name IN ('target_rcw_section_id','target_usc_section_id');
  IF pre_columns <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2]: target_rcw_section_id or target_usc_section_id columns already exist (count=%). Investigate before remediation.', pre_columns;
  END IF;

  -- §0.3: snapshot authority_chain section-level resolved (must remain ≥83,752)
  SELECT COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law AND rcw_section_id IS NOT NULL)
    INTO pre_authority_section_resolved
  FROM wac.authority_chain;
  IF pre_authority_section_resolved < 83752 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.3]: authority_chain section-level resolved is % (expected ≥83,752 baseline).', pre_authority_section_resolved;
  END IF;

  -- §0.4: snapshot out-of-scope NULL count (must be 215 = 199 cfr + 16 wa-constitution)
  SELECT COUNT(*) INTO pre_oos_null
  FROM wac.cross_references
  WHERE target_jurisdiction IN ('cfr','wa-constitution');
  IF pre_oos_null <> 215 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4]: out-of-scope (cfr+wa-constitution) row count is % (expected 215).', pre_oos_null;
  END IF;

  RAISE NOTICE 'Pre-flight OK: 0 cross_refs pre-resolved; 0 collision on new columns; authority_chain section-resolved=%; out-of-scope NULL=%',
               pre_authority_section_resolved, pre_oos_null;
END $$;


-- =====================================================================
-- §SCHEMA: Additive ALTER TABLE — new FK columns
-- =====================================================================
-- target_rcw_section_id: cross-corpus FK to rcw.sections.id
-- target_usc_section_id: cross-corpus FK to usc.sections.id
-- Both nullable (most cross_references rows will not have a cross-corpus
-- target — only those with target_jurisdiction='state-wa-rcw' or
-- 'federal-usc' get populated by §2/§3 below).
ALTER TABLE wac.cross_references
  ADD COLUMN target_rcw_section_id UUID REFERENCES rcw.sections(id),
  ADD COLUMN target_usc_section_id UUID REFERENCES usc.sections(id);

COMMENT ON COLUMN wac.cross_references.target_rcw_section_id IS
  'FK to rcw.sections(id) for cross-references with target_jurisdiction=''state-wa-rcw''. '
  'Populated by Phase 4.4 (2026-05-01) via text-match on canonical_citation. '
  'NULL when target_jurisdiction is not state-wa-rcw OR target citation does not '
  'resolve (recodified RCW, chapter-form, etc).';
COMMENT ON COLUMN wac.cross_references.target_usc_section_id IS
  'FK to usc.sections(id) for cross-references with target_jurisdiction=''federal-usc''. '
  'Populated by Phase 4.4 (2026-05-01) via text-match on canonical_citation. '
  'NULL when target_jurisdiction is not federal-usc OR target citation has subsection '
  'notation that USC canonical_citation strips (e.g., ''12 U.S.C. § 3201(3)'').';


-- =====================================================================
-- §1 Self-ref resolution (state-wa-wac)
-- =====================================================================
-- Match wac.cross_references.target_citation = wac.sections.canonical_citation
-- exactly. Three-segment section refs (e.g., 'WAC 296-15-001') match;
-- chapter-form refs (e.g., 'chapter 173-476 WAC') do NOT match section-
-- level rows by design and remain NULL.
UPDATE wac.cross_references c
SET target_section_id = s.id
FROM wac.sections s
WHERE c.target_section_id IS NULL
  AND c.target_jurisdiction = 'state-wa-wac'
  AND c.target_citation = s.canonical_citation;


-- =====================================================================
-- §2 WAC→RCW cross-corpus resolution
-- =====================================================================
-- Match wac.cross_references.target_citation = rcw.sections.canonical_citation
-- exactly. Section-form RCW refs (e.g., 'RCW 49.60.180') match;
-- chapter-form refs (e.g., 'chapter 43.04 RCW') and recodified-out
-- citations (e.g., 'RCW 34.04.090' from old RCW Title 34) do NOT match
-- and remain NULL. The 12,490 unresolved residue is documented expected.
UPDATE wac.cross_references c
SET target_rcw_section_id = s.id
FROM rcw.sections s
WHERE c.target_rcw_section_id IS NULL
  AND c.target_jurisdiction = 'state-wa-rcw'
  AND c.target_citation = s.canonical_citation;


-- =====================================================================
-- §3 WAC→USC cross-corpus resolution
-- =====================================================================
-- Match wac.cross_references.target_citation = usc.sections.canonical_citation
-- exactly. Standard USC refs (e.g., '42 U.S.C. § 12132') match;
-- subsection-notation refs (e.g., '12 U.S.C. § 3201(3)') do NOT match
-- because USC canonical_citation stores section-level only without
-- subsection. The 23 unresolved residue is documented expected.
UPDATE wac.cross_references c
SET target_usc_section_id = s.id
FROM usc.sections s
WHERE c.target_usc_section_id IS NULL
  AND c.target_jurisdiction = 'federal-usc'
  AND c.target_citation = s.canonical_citation;


-- =====================================================================
-- §POST: Verification guards
-- =====================================================================
DO $$
DECLARE
  -- §1 self-ref
  post_self_resolved INTEGER;
  post_self_total INTEGER;
  pct_self NUMERIC;
  -- §2 WAC→RCW
  post_rcw_resolved INTEGER;
  post_rcw_total INTEGER;
  pct_rcw NUMERIC;
  -- §3 WAC→USC
  post_usc_resolved INTEGER;
  post_usc_total INTEGER;
  pct_usc NUMERIC;
  -- preserved invariants
  post_authority_section_resolved INTEGER;
  post_oos_null INTEGER;
  -- new column FK integrity (orphan check; FK enforces this but explicit verification)
  orphan_rcw INTEGER;
  orphan_usc INTEGER;
BEGIN
  -- §1 self-ref rate
  SELECT COUNT(*) FILTER (WHERE target_section_id IS NOT NULL),
         COUNT(*)
    INTO post_self_resolved, post_self_total
  FROM wac.cross_references WHERE target_jurisdiction = 'state-wa-wac';
  pct_self := 100.0 * post_self_resolved / NULLIF(post_self_total, 0);
  IF pct_self < 85 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1]: self-ref FK rate % (%/%) < 85%% halt threshold.',
                    ROUND(pct_self, 2), post_self_resolved, post_self_total;
  END IF;
  IF pct_self > 99.5 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1]: self-ref FK rate % (%/%) > 99.5%% suggests JOIN over-matching.',
                    ROUND(pct_self, 2), post_self_resolved, post_self_total;
  END IF;

  -- §2 WAC→RCW rate
  SELECT COUNT(*) FILTER (WHERE target_rcw_section_id IS NOT NULL),
         COUNT(*)
    INTO post_rcw_resolved, post_rcw_total
  FROM wac.cross_references WHERE target_jurisdiction = 'state-wa-rcw';
  pct_rcw := 100.0 * post_rcw_resolved / NULLIF(post_rcw_total, 0);
  IF pct_rcw < 60 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2]: WAC->RCW rate % (%/%) < 60%% halt threshold.',
                    ROUND(pct_rcw, 2), post_rcw_resolved, post_rcw_total;
  END IF;
  IF pct_rcw > 90 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2]: WAC->RCW rate % (%/%) > 90%% upper band, suggests JOIN over-matching.',
                    ROUND(pct_rcw, 2), post_rcw_resolved, post_rcw_total;
  END IF;

  -- §3 WAC→USC rate
  SELECT COUNT(*) FILTER (WHERE target_usc_section_id IS NOT NULL),
         COUNT(*)
    INTO post_usc_resolved, post_usc_total
  FROM wac.cross_references WHERE target_jurisdiction = 'federal-usc';
  pct_usc := 100.0 * post_usc_resolved / NULLIF(post_usc_total, 0);
  IF pct_usc < 70 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.3]: WAC->USC rate % (%/%) < 70%% halt threshold.',
                    ROUND(pct_usc, 2), post_usc_resolved, post_usc_total;
  END IF;
  IF pct_usc > 95 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.3]: WAC->USC rate % (%/%) > 95%% upper band.',
                    ROUND(pct_usc, 2), post_usc_resolved, post_usc_total;
  END IF;

  -- §POST.4 authority chain integrity preserved
  SELECT COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law AND rcw_section_id IS NOT NULL)
    INTO post_authority_section_resolved
  FROM wac.authority_chain;
  IF post_authority_section_resolved < 83752 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.4]: authority_chain section-level resolved dropped to % (was 83,752 baseline).',
                    post_authority_section_resolved;
  END IF;

  -- §POST.5 out-of-scope NULL preserved (cfr+wa-constitution = 215 exact)
  SELECT COUNT(*) INTO post_oos_null
  FROM wac.cross_references
  WHERE target_jurisdiction IN ('cfr','wa-constitution')
    AND target_section_id IS NULL
    AND target_rcw_section_id IS NULL
    AND target_usc_section_id IS NULL;
  IF post_oos_null <> 215 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.5]: out-of-scope (cfr+wa-constitution) all-NULL row count = % (expected 215).', post_oos_null;
  END IF;

  -- §POST.6 FK integrity (orphan check; FK declarations enforce this but
  -- the explicit count surfaces any discrepancy clearly)
  SELECT COUNT(*) INTO orphan_rcw
  FROM wac.cross_references c
  WHERE c.target_rcw_section_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM rcw.sections s WHERE s.id = c.target_rcw_section_id);
  IF orphan_rcw <> 0 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.6a]: % orphan target_rcw_section_id values (FK integrity violated).', orphan_rcw;
  END IF;

  SELECT COUNT(*) INTO orphan_usc
  FROM wac.cross_references c
  WHERE c.target_usc_section_id IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM usc.sections s WHERE s.id = c.target_usc_section_id);
  IF orphan_usc <> 0 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.6b]: % orphan target_usc_section_id values (FK integrity violated).', orphan_usc;
  END IF;

  RAISE NOTICE 'Post-apply OK: self=%/% (%%%); rcw=%/% (%%%); usc=%/% (%%%); authority_chain section-resolved=%; oos_null=215; FK orphans=0/0',
               post_self_resolved, post_self_total, ROUND(pct_self, 2),
               post_rcw_resolved, post_rcw_total, ROUND(pct_rcw, 2),
               post_usc_resolved, post_usc_total, ROUND(pct_usc, 2),
               post_authority_section_resolved;
END $$;

COMMIT;


-- =====================================================================
-- §V Verification SELECTs (post-commit, for operator review)
-- =====================================================================
\echo ''
\echo '--- Final wac.cross_references resolution rates by jurisdiction ---'
SELECT target_jurisdiction,
       COUNT(*)                                                       AS total,
       COUNT(*) FILTER (WHERE target_section_id     IS NOT NULL)      AS self_resolved,
       COUNT(*) FILTER (WHERE target_rcw_section_id IS NOT NULL)      AS rcw_resolved,
       COUNT(*) FILTER (WHERE target_usc_section_id IS NOT NULL)      AS usc_resolved,
       ROUND(100.0 *
         COUNT(*) FILTER (WHERE target_section_id IS NOT NULL
                            OR target_rcw_section_id IS NOT NULL
                            OR target_usc_section_id IS NOT NULL)
         / NULLIF(COUNT(*), 0), 2)                                     AS any_resolved_pct
FROM wac.cross_references
GROUP BY target_jurisdiction
ORDER BY total DESC;

\echo ''
\echo '--- Self-ref residual NULL breakdown ---'
SELECT
  CASE
    WHEN target_citation ~ '^chapter [0-9]+[A-Za-z]?-[0-9]+[A-Za-z]? WAC$' THEN 'chapter_form (not resolvable)'
    WHEN target_citation ~ '^WAC [0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?$' THEN 'three_segment (citation not in wac.sections)'
    ELSE 'other'
  END AS reason,
  COUNT(*)
FROM wac.cross_references
WHERE target_jurisdiction = 'state-wa-wac' AND target_section_id IS NULL
GROUP BY reason ORDER BY COUNT(*) DESC;

\echo ''
\echo '--- WAC->RCW residual NULL breakdown ---'
SELECT
  CASE
    WHEN target_citation ~ '^chapter [0-9][0-9A-Z]*\.[0-9A-Z]+ RCW$' THEN 'chapter_form (not resolvable)'
    WHEN target_citation ~ '^Title [0-9]+[A-Z]? RCW$'                 THEN 'title_form (not resolvable)'
    WHEN target_citation ~ '^RCW '                                    THEN 'section_form (citation not in rcw.sections — likely recodified)'
    ELSE 'other'
  END AS reason,
  COUNT(*)
FROM wac.cross_references
WHERE target_jurisdiction = 'state-wa-rcw' AND target_rcw_section_id IS NULL
GROUP BY reason ORDER BY COUNT(*) DESC;

\echo ''
\echo '--- WAC->USC residual NULL breakdown ---'
SELECT target_citation, COUNT(*)
FROM wac.cross_references
WHERE target_jurisdiction = 'federal-usc' AND target_usc_section_id IS NULL
GROUP BY target_citation ORDER BY COUNT(*) DESC LIMIT 20;

\echo ''
\echo '--- New schema state: verify columns + FK constraints ---'
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema='wac' AND table_name='cross_references'
  AND column_name IN ('target_section_id','target_rcw_section_id','target_usc_section_id')
ORDER BY column_name;

SELECT con.conname, pg_get_constraintdef(con.oid) AS definition
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace ns ON ns.oid = rel.relnamespace
WHERE ns.nspname='wac' AND rel.relname='cross_references' AND con.contype='f'
ORDER BY con.conname;

\echo ''
\echo '=== END phase4_4_fk_resolution.sql ==='
