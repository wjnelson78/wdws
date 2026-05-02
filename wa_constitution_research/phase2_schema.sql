-- =============================================================================
-- WA Constitution Phase 2 — Schema Migration
-- =============================================================================
-- Date: 2026-05-01
-- Project: WA Constitution Ingest Agent
-- Authorization: operator confirmation 2026-05-01 of all 8 Phase 1 dispositions
--
-- Single transaction:
--   §0  Pre-flight guards (live state matches Phase 1 verification)
--   §1  CREATE SCHEMA wa_constitution
--   §2  CREATE TABLE wa_constitution.articles
--   §3  CREATE TABLE wa_constitution.sections
--   §4  CREATE TABLE wa_constitution.cross_references
--   §5  CREATE TABLE wa_constitution.amendments
--   §6  ALTER existing cross_references tables (rcw + wac + wa_court_rules)
--       ADD COLUMN target_wa_const_section_id + target_wa_const_article_id
--   §POST  Post-flight verification (4 tables created, 6 columns added)
--
-- §K canon adherence:
--   - §K #1-#9 (audit-predicate diversification): defensive CHECK constraints
--     enforce structural invariants at INSERT, structurally distinct from
--     parser predicates that emit the rows
--   - §K #25 reinforcement: pre-flight verifies actual state, not inherited
--     premise; halt before any writes if state drifts
--   - §K #29 reinforcement: halt-band recalibrated to actual measured count
--     [270, 285] per operator approval (was [180, 220] per kickoff)
--   - §K #36 (agent-scope boundary): operates ONLY on wa_constitution.* schema
--     creation + 6 ALTER ADD COLUMN on existing cross_references tables.
--     ZERO destructive changes to existing tables.
--   - §K #40 (heterogeneous structural granularity): chk_canonical_citation
--     regex accepts both '§ 1A' (capital-suffix) and '§ 2(a)' (paren) forms
--     per Pattern α (standalone) disposition; subsection-fold (Pattern β)
--     handled at parser layer, not schema CHECK.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

-- =============================================================================
-- §0  PRE-FLIGHT GUARDS — Phase 1 verified state must hold
-- =============================================================================

DO $$
DECLARE
  schema_already_exists       INTEGER;
  rcw_section_count           INTEGER;
  usc_section_count           INTEGER;
  wac_section_count           INTEGER;
  wcr_rule_count              INTEGER;
  wac_authority_chain_count   INTEGER;
  cross_corpus_xref_count     INTEGER;
  rcw_xref_to_const           INTEGER;
  wac_xref_to_const           INTEGER;
  wcr_xref_to_const           INTEGER;
  rcw_const_col_exists        INTEGER;
  wac_const_col_exists        INTEGER;
  wcr_const_col_exists        INTEGER;
BEGIN
  -- §0.1 wa_constitution schema does not yet exist
  SELECT COUNT(*) INTO schema_already_exists
    FROM information_schema.schemata WHERE schema_name = 'wa_constitution';
  IF schema_already_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.1]: wa_constitution schema already exists. Drop or rename before re-applying.';
  END IF;

  -- §0.2 Existing corpora intact (frozen baselines from Phase 1 verification)
  SELECT COUNT(*) INTO rcw_section_count FROM rcw.sections;
  IF rcw_section_count <> 58529 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2a]: rcw.sections has % rows, expected 58,529. Halt — corpus drift.', rcw_section_count;
  END IF;

  SELECT COUNT(*) INTO usc_section_count FROM usc.sections;
  IF usc_section_count <> 61133 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2b]: usc.sections has % rows, expected 61,133. Halt — corpus drift.', usc_section_count;
  END IF;

  SELECT COUNT(*) INTO wac_section_count FROM wac.sections;
  IF wac_section_count <> 73180 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2c]: wac.sections has % rows, expected 73,180. Halt — corpus drift.', wac_section_count;
  END IF;

  SELECT COUNT(*) INTO wcr_rule_count FROM wa_court_rules.rules;
  IF wcr_rule_count <> 1286 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2d]: wa_court_rules.rules has % rows, expected 1,286. Halt — corpus drift.', wcr_rule_count;
  END IF;

  -- §0.3 wac.authority_chain orthogonality witness
  SELECT COUNT(*) INTO wac_authority_chain_count
    FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL;
  IF wac_authority_chain_count <> 83752 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.3]: wac.authority_chain @ RCW resolved = %, expected 83,752. Halt — orthogonality witness drift.', wac_authority_chain_count;
  END IF;

  -- §0.4 Cross-corpus xref demand (54 total: rcw=34, wac=16, wcr=4)
  SELECT COUNT(*) INTO rcw_xref_to_const
    FROM rcw.cross_references WHERE target_jurisdiction = 'wa-constitution';
  IF rcw_xref_to_const <> 34 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4a]: rcw.cross_references → wa-constitution = %, expected 34.', rcw_xref_to_const;
  END IF;

  SELECT COUNT(*) INTO wac_xref_to_const
    FROM wac.cross_references WHERE target_jurisdiction = 'wa-constitution';
  IF wac_xref_to_const <> 16 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4b]: wac.cross_references → wa-constitution = %, expected 16.', wac_xref_to_const;
  END IF;

  SELECT COUNT(*) INTO wcr_xref_to_const
    FROM wa_court_rules.cross_references WHERE target_jurisdiction = 'wa-constitution';
  IF wcr_xref_to_const <> 4 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4c]: wa_court_rules.cross_references → wa-constitution = %, expected 4.', wcr_xref_to_const;
  END IF;

  cross_corpus_xref_count := rcw_xref_to_const + wac_xref_to_const + wcr_xref_to_const;
  IF cross_corpus_xref_count <> 54 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4d]: total cross-corpus xrefs to wa-constitution = %, expected 54.', cross_corpus_xref_count;
  END IF;

  -- §0.5 target_wa_const_* columns do NOT yet exist on existing cross_references tables
  SELECT COUNT(*) INTO rcw_const_col_exists
    FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF rcw_const_col_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5a]: rcw.cross_references already has wa_const FK columns. Migration partially applied?';
  END IF;

  SELECT COUNT(*) INTO wac_const_col_exists
    FROM information_schema.columns
    WHERE table_schema='wac' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF wac_const_col_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5b]: wac.cross_references already has wa_const FK columns. Migration partially applied?';
  END IF;

  SELECT COUNT(*) INTO wcr_const_col_exists
    FROM information_schema.columns
    WHERE table_schema='wa_court_rules' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF wcr_const_col_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5c]: wa_court_rules.cross_references already has wa_const FK columns. Migration partially applied?';
  END IF;

  RAISE NOTICE 'Pre-flight: PASS — wa_constitution absent; corpora intact (rcw=%, usc=%, wac=%, wcr=%); orthogonality witness=%; 54 cross-corpus refs ready for FK uplift; no FK columns pre-existing on target tables',
    rcw_section_count, usc_section_count, wac_section_count, wcr_rule_count, wac_authority_chain_count;
END $$;


-- =============================================================================
-- §1  CREATE SCHEMA wa_constitution
-- =============================================================================

CREATE SCHEMA wa_constitution;
COMMENT ON SCHEMA wa_constitution IS
  'Washington State Constitution corpus. Fifth primary-source corpus parallel to USC, RCW, WAC, WA Court Rules. Created 2026-05-01.';


-- =============================================================================
-- §2  wa_constitution.articles
-- =============================================================================

CREATE TABLE wa_constitution.articles (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  article_number           TEXT NOT NULL UNIQUE,           -- Roman: 'I', 'IV', 'XXXII'
  article_number_arabic    INTEGER NOT NULL UNIQUE,        -- Sort order: 1, 4, 32
  article_title            TEXT NOT NULL,                  -- 'Declaration of Rights'
  is_repealed              BOOLEAN NOT NULL DEFAULT FALSE,
  is_amended               BOOLEAN NOT NULL DEFAULT FALSE,
  repealed_by              TEXT,                           -- Amendment cite if repealed
  source_url               TEXT,                           -- leg.wa.gov per-article URL
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT chk_article_number_format
    CHECK (article_number ~ '^[IVXLC]+$'),

  CONSTRAINT chk_article_number_arabic_range
    CHECK (article_number_arabic BETWEEN 1 AND 99),

  CONSTRAINT chk_repealed_consistency
    CHECK ((NOT is_repealed) OR (is_repealed AND repealed_by IS NOT NULL))
);

CREATE INDEX idx_articles_arabic ON wa_constitution.articles(article_number_arabic);

COMMENT ON TABLE wa_constitution.articles IS
  'Article-level metadata. ~32 rows expected (Articles I-XXXII). NO entire article currently repealed (per Phase 1 verification 2026-05-01). is_repealed kept for schema generality.';


-- =============================================================================
-- §3  wa_constitution.sections
-- =============================================================================

CREATE TABLE wa_constitution.sections (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id              UUID NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
  article_id               UUID NOT NULL REFERENCES wa_constitution.articles(id) ON DELETE CASCADE,
  section_number           TEXT NOT NULL,                  -- '1', '12', '35', '2(a)', '1A'
  canonical_citation       TEXT NOT NULL UNIQUE,           -- 'Wash. Const. art. IV, § 31'
  section_heading          TEXT,                           -- 'Trial by Jury'
  full_text                TEXT,                           -- Operative section text
  is_repealed              BOOLEAN NOT NULL DEFAULT FALSE,
  is_amended               BOOLEAN NOT NULL DEFAULT FALSE,
  amended_by               TEXT,                           -- 'Amendment 88, 1993'
  original_text            TEXT,                           -- Pre-amendment text if amended
  effective_date           DATE,                           -- Most-recent amendment ratification
  source_url               TEXT,                           -- leg.wa.gov section URL
  letter_suffix_pattern    TEXT,                           -- §K #40: 'standalone', 'subsection-folded', or NULL
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- §K #40 disposition: regex accepts '§ 1A' (capital) and '§ 2(a)' (parens)
  CONSTRAINT chk_canonical_citation_format
    CHECK (canonical_citation ~ '^Wash\. Const\. art\. [IVXLC]+(, § [0-9]+([A-Z]|\([a-z]\))?)?$'),

  -- §K #1-#11 audit-predicate diversification: full_text required if not repealed
  CONSTRAINT chk_section_text_status_invariant
    CHECK (
      (NOT is_repealed AND full_text IS NOT NULL AND length(full_text) > 0)
      OR
      (is_repealed)
    ),

  -- §K #6 paired-NULL defensive: amendment metadata consistency
  CONSTRAINT chk_amendment_pair
    CHECK (
      (NOT is_amended)
      OR
      (is_amended AND amended_by IS NOT NULL)
    ),

  -- §K #1 defensive: section_number must follow standalone-form regex
  -- (paren'd letters, capital letters, or arabic only)
  CONSTRAINT chk_section_number_format
    CHECK (section_number ~ '^[0-9]+([A-Z]|\([a-z]\))?$'),

  -- letter_suffix_pattern enum: discriminates §K #40 Pattern α vs β
  CONSTRAINT chk_letter_suffix_pattern
    CHECK (letter_suffix_pattern IS NULL OR letter_suffix_pattern IN ('standalone', 'parent-no-suffix')),

  -- One row per (article, section_number) pair
  UNIQUE (article_id, section_number)
);

CREATE INDEX idx_sections_article ON wa_constitution.sections(article_id);
CREATE INDEX idx_sections_canonical ON wa_constitution.sections(canonical_citation);
CREATE INDEX idx_sections_document ON wa_constitution.sections(document_id);

COMMENT ON TABLE wa_constitution.sections IS
  '~276-278 rows expected per Phase 1 (273 parent-numbered + 3-5 standalone letter-suffix). letter_suffix_pattern discriminates §K #40 Pattern α (standalone, e.g. art IV § 2(a) with distinct heading) from parent-no-suffix (default).';


-- =============================================================================
-- §4  wa_constitution.cross_references
-- =============================================================================

CREATE TABLE wa_constitution.cross_references (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_section_id        UUID NOT NULL REFERENCES wa_constitution.sections(id) ON DELETE CASCADE,
  target_citation          TEXT NOT NULL,
  target_section_id        UUID REFERENCES wa_constitution.sections(id),  -- internal section
  target_article_id        UUID REFERENCES wa_constitution.articles(id),  -- internal article-only
  target_rcw_section_id    UUID REFERENCES rcw.sections(id),
  target_usc_section_id    UUID REFERENCES usc.sections(id),
  target_jurisdiction      TEXT NOT NULL,
  context_snippet          TEXT,
  ref_type                 TEXT NOT NULL DEFAULT 'reference',
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT chk_target_jurisdiction
    CHECK (target_jurisdiction IN (
      'wa-constitution', 'state-wa-rcw', 'state-wa-wac',
      'state-wa-court-rule', 'federal-usc', 'wa-amendment',
      'cfr', 'session-law', 'other'
    )),

  CONSTRAINT chk_ref_type
    CHECK (ref_type IN ('reference', 'cross-statute', 'see-also', 'authority', 'amendment'))
);

CREATE INDEX idx_const_xref_source ON wa_constitution.cross_references(source_section_id);
CREATE INDEX idx_const_xref_target_section ON wa_constitution.cross_references(target_section_id);
CREATE INDEX idx_const_xref_target_article ON wa_constitution.cross_references(target_article_id);
CREATE INDEX idx_const_xref_target_rcw ON wa_constitution.cross_references(target_rcw_section_id);
CREATE INDEX idx_const_xref_target_usc ON wa_constitution.cross_references(target_usc_section_id);
CREATE INDEX idx_const_xref_jurisdiction ON wa_constitution.cross_references(target_jurisdiction);

COMMENT ON TABLE wa_constitution.cross_references IS
  'Outbound cross-references from constitution sections. Phase 4 internal resolution + Phase 4.4 cross-corpus FK resolution. Multi-FK pattern (target_*_id) follows wa_court_rules.cross_references precedent.';


-- =============================================================================
-- §5  wa_constitution.amendments
-- =============================================================================

CREATE TABLE wa_constitution.amendments (
  id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  amendment_number         INTEGER NOT NULL UNIQUE,
  amendment_title          TEXT,
  ratification_date        DATE,
  legislative_session      TEXT,                           -- e.g., '1993 House Joint Resolution No. 4200'
  affects_articles         TEXT[],                         -- ['I', 'IV']
  affects_sections         TEXT[],                         -- ['I § 11', 'IV § 31']
  summary                  TEXT,
  source_url               TEXT,
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT chk_amendment_number_range
    CHECK (amendment_number BETWEEN 1 AND 999)
);

CREATE INDEX idx_amendments_number ON wa_constitution.amendments(amendment_number);
CREATE INDEX idx_amendments_articles_gin ON wa_constitution.amendments USING GIN(affects_articles);

COMMENT ON TABLE wa_constitution.amendments IS
  '~110 rows expected (per Phase 1 verification — Amendments 1-110 ratified through 2025). ratification_date may be NULL where source HTML did not surface date.';


-- =============================================================================
-- §6  Cross-corpus FK uplift — ADD COLUMN on existing tables
-- =============================================================================
-- Per kickoff Phase 4.4 design (Option (b) additive schema). 6 new NULLABLE
-- FK columns. ZERO destructive changes — all rows on existing tables remain
-- unchanged. Phase 4.4 UPDATE passes will populate these columns.

-- §6.1 rcw.cross_references
ALTER TABLE rcw.cross_references
  ADD COLUMN target_wa_const_section_id UUID REFERENCES wa_constitution.sections(id),
  ADD COLUMN target_wa_const_article_id UUID REFERENCES wa_constitution.articles(id);

CREATE INDEX idx_rcw_xref_wa_const_section ON rcw.cross_references(target_wa_const_section_id);
CREATE INDEX idx_rcw_xref_wa_const_article ON rcw.cross_references(target_wa_const_article_id);

-- §6.2 wac.cross_references
ALTER TABLE wac.cross_references
  ADD COLUMN target_wa_const_section_id UUID REFERENCES wa_constitution.sections(id),
  ADD COLUMN target_wa_const_article_id UUID REFERENCES wa_constitution.articles(id);

CREATE INDEX idx_wac_xref_wa_const_section ON wac.cross_references(target_wa_const_section_id);
CREATE INDEX idx_wac_xref_wa_const_article ON wac.cross_references(target_wa_const_article_id);

-- §6.3 wa_court_rules.cross_references
ALTER TABLE wa_court_rules.cross_references
  ADD COLUMN target_wa_const_section_id UUID REFERENCES wa_constitution.sections(id),
  ADD COLUMN target_wa_const_article_id UUID REFERENCES wa_constitution.articles(id);

CREATE INDEX idx_wcr_xref_wa_const_section ON wa_court_rules.cross_references(target_wa_const_section_id);
CREATE INDEX idx_wcr_xref_wa_const_article ON wa_court_rules.cross_references(target_wa_const_article_id);


-- =============================================================================
-- §POST  Post-flight verification
-- =============================================================================

DO $$
DECLARE
  schema_present                   INTEGER;
  articles_table_present           INTEGER;
  sections_table_present           INTEGER;
  xref_table_present               INTEGER;
  amendments_table_present         INTEGER;
  rcw_const_col_count              INTEGER;
  wac_const_col_count              INTEGER;
  wcr_const_col_count              INTEGER;

  -- Corpora intact post-DDL (orthogonality verification)
  rcw_section_count                INTEGER;
  usc_section_count                INTEGER;
  wac_section_count                INTEGER;
  wcr_rule_count                   INTEGER;
  wac_authority_chain_count        INTEGER;
  rcw_xref_to_const                INTEGER;
  wac_xref_to_const                INTEGER;
  wcr_xref_to_const                INTEGER;
BEGIN
  SELECT COUNT(*) INTO schema_present
    FROM information_schema.schemata WHERE schema_name = 'wa_constitution';
  IF schema_present <> 1 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wa_constitution schema not present after CREATE.';
  END IF;

  SELECT COUNT(*) INTO articles_table_present FROM information_schema.tables
    WHERE table_schema='wa_constitution' AND table_name='articles';
  SELECT COUNT(*) INTO sections_table_present FROM information_schema.tables
    WHERE table_schema='wa_constitution' AND table_name='sections';
  SELECT COUNT(*) INTO xref_table_present FROM information_schema.tables
    WHERE table_schema='wa_constitution' AND table_name='cross_references';
  SELECT COUNT(*) INTO amendments_table_present FROM information_schema.tables
    WHERE table_schema='wa_constitution' AND table_name='amendments';
  IF (articles_table_present + sections_table_present + xref_table_present + amendments_table_present) <> 4 THEN
    RAISE EXCEPTION 'Post-flight FAIL: expected 4 wa_constitution.* tables, got %.',
      (articles_table_present + sections_table_present + xref_table_present + amendments_table_present);
  END IF;

  -- 6 new FK columns added (2 per table × 3 tables)
  SELECT COUNT(*) INTO rcw_const_col_count
    FROM information_schema.columns
    WHERE table_schema='rcw' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF rcw_const_col_count <> 2 THEN
    RAISE EXCEPTION 'Post-flight FAIL: rcw.cross_references should have 2 wa_const FK columns, has %.', rcw_const_col_count;
  END IF;

  SELECT COUNT(*) INTO wac_const_col_count
    FROM information_schema.columns
    WHERE table_schema='wac' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF wac_const_col_count <> 2 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wac.cross_references should have 2 wa_const FK columns, has %.', wac_const_col_count;
  END IF;

  SELECT COUNT(*) INTO wcr_const_col_count
    FROM information_schema.columns
    WHERE table_schema='wa_court_rules' AND table_name='cross_references'
      AND column_name IN ('target_wa_const_section_id', 'target_wa_const_article_id');
  IF wcr_const_col_count <> 2 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wa_court_rules.cross_references should have 2 wa_const FK columns, has %.', wcr_const_col_count;
  END IF;

  -- Corpora intact (post-DDL verification — every existing table count must be unchanged)
  SELECT COUNT(*) INTO rcw_section_count FROM rcw.sections;
  IF rcw_section_count <> 58529 THEN
    RAISE EXCEPTION 'Post-flight FAIL: rcw.sections=%, expected 58,529. Schema migration broke RCW corpus integrity.', rcw_section_count;
  END IF;

  SELECT COUNT(*) INTO usc_section_count FROM usc.sections;
  IF usc_section_count <> 61133 THEN
    RAISE EXCEPTION 'Post-flight FAIL: usc.sections=%, expected 61,133.', usc_section_count;
  END IF;

  SELECT COUNT(*) INTO wac_section_count FROM wac.sections;
  IF wac_section_count <> 73180 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wac.sections=%, expected 73,180.', wac_section_count;
  END IF;

  SELECT COUNT(*) INTO wcr_rule_count FROM wa_court_rules.rules;
  IF wcr_rule_count <> 1286 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wa_court_rules.rules=%, expected 1,286.', wcr_rule_count;
  END IF;

  SELECT COUNT(*) INTO wac_authority_chain_count
    FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL;
  IF wac_authority_chain_count <> 83752 THEN
    RAISE EXCEPTION 'Post-flight FAIL: wac.authority_chain @ RCW=%, expected 83,752 (orthogonality witness).', wac_authority_chain_count;
  END IF;

  -- 54 cross-corpus xrefs unchanged (no rows touched, only columns added)
  SELECT COUNT(*) INTO rcw_xref_to_const FROM rcw.cross_references WHERE target_jurisdiction='wa-constitution';
  SELECT COUNT(*) INTO wac_xref_to_const FROM wac.cross_references WHERE target_jurisdiction='wa-constitution';
  SELECT COUNT(*) INTO wcr_xref_to_const FROM wa_court_rules.cross_references WHERE target_jurisdiction='wa-constitution';
  IF (rcw_xref_to_const + wac_xref_to_const + wcr_xref_to_const) <> 54 THEN
    RAISE EXCEPTION 'Post-flight FAIL: cross-corpus xrefs to wa-constitution=%, expected 54.',
      rcw_xref_to_const + wac_xref_to_const + wcr_xref_to_const;
  END IF;

  RAISE NOTICE 'Post-flight: PASS — 4 wa_constitution.* tables created; 6 FK columns added (2×rcw + 2×wac + 2×wcr); all corpora intact (rcw=%, usc=%, wac=%, wcr=%); orthogonality witness=%; 54 cross-corpus xrefs ready for Phase 4.4 uplift',
    rcw_section_count, usc_section_count, wac_section_count, wcr_rule_count, wac_authority_chain_count;
END $$;

COMMIT;

-- End of phase2_schema.sql
