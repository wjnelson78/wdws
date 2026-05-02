-- =============================================================================
-- WA Court Rules Phase 1 Schema Design — STAGED FOR OPERATOR REVIEW
-- =============================================================================
-- Status:        STAGED — DO NOT APPLY without explicit operator authorization.
-- Author:        WA Court Rules ingest agent
-- Date:          2026-04-30
-- Companion to:  /opt/wdws/wa_court_rules_research/phase0_cumulative_report.md
--                /opt/wdws/wa_court_rules_research/litigation_priority_addendum.md
--                /opt/wdws/wa_court_rules_research/rule_set_inventory.md
--                /opt/wdws/wa_court_rules_research/wa_court_rules_source_findings.md
-- Apply file:    /opt/wdws/wa_court_rules_research/phase1_schema_apply.sql
--                (transaction-wrapped, RAISE EXCEPTION guards — review THIS file
--                 first; apply that file second after explicit authorization)
-- T17 test:      /opt/wdws/wa_court_rules_research/phase1_regex_test.py
-- Target DB:     ACE on klunky.12432.net — PostgreSQL 17 + pgvector
--
-- Sections:
--   §0  Pre-apply assertions (review reference; phase1_schema_apply.sql RAISEs on these)
--   §HARMONIZE  rcw.cross_references + wac.cross_references CHECK enum harmonization
--               ('wa-court-rule' → 'state-wa-court-rule', BOTH tables, same transaction)
--   §1  wa_court_rules schema + 5 table definitions
--        §1.1  rule_sets       (one row per rule set; tier 1/2/NULL discriminator)
--        §1.2  rules           (one row per rule; ghost flags + range + parent_canon + comment)
--        §1.3  cross_references (Phase-8-deferred citations resolve to target_rule_id=NULL)
--        §1.4  rule_history    (amendment-line entries per rule)
--        §1.5  ingestion_runs  (audit trail for Phase 4)
--   §2  Indexes (9 indexes: incl. partial idx on range_doc_id, parent_canon)
--   §3  Tag taxonomy additions (29 new rows)
--        §3a  code-jurisdiction: state-wa-court-rule (joins 3 existing slugs)
--        §3b  court-rule-meta (3 ghost-flag tags)
--        §3c  court-rule-set (15 in-scope set tags — Tier 1 + Tier 2)
--        §3d  court-rule-topical (10 topical tags)
--   §POST  Post-apply verification (all three CHECK constraints harmonized,
--          tags seeded, no 'wa-court-rule' short form anywhere)
--   §V  Verification SELECTs (print to stdout post-commit for operator review)
--
-- Idempotency: every INSERT uses ON CONFLICT (slug) DO NOTHING. Pre-flight
-- guards in apply file ensure clean state before any DDL — re-running after
-- a partial apply is safe (guards will halt cleanly).
--
-- VERIFIED PRE-FLIGHT FINDINGS (2026-04-30, re-verified by §0 in apply file):
--   1. wa_court_rules schema absent
--   2. state-wa-court-rule slug absent in core.tags
--   3. 0 rows use 'wa-court-rule' short form on rcw.cross_references
--   4. 0 rows use 'wa-court-rule' short form on wac.cross_references
--   5. rcw.cross_references chk_target_jurisdiction has 'wa-court-rule' short form (ready for harmonization)
--   6. wac.cross_references chk_target_jurisdiction has 'wa-court-rule' short form (ready for harmonization)
--   7. Corpus counts: rcw.sections=51,764, wac.sections=73,180, usc tag=11,678, rcw tag=51,764, wac tag=73,180
--   8. 3 baseline code-jurisdiction tags present (federal-usc, state-wa-rcw, state-wa-wac)
--
-- LIKE pattern discrimination (used in §POST harmonization verification):
--   '%''wa-court-rule''%' matches the literal apostrophe-wa-court-rule-apostrophe.
--   '%''state-wa-court-rule''%' matches apostrophe-state-wa-court-rule-apostrophe.
--   In post-harmonization fragment `,'state-wa-court-rule',`, the apostrophe
--   before 'wa-court-rule' is preceded by 'state-' (NOT an apostrophe), so
--   LIKE '%''wa-court-rule''%' will NOT match. Discriminator is sound — same
--   pattern proven in WAC Phase 1 harmonization 2026-04-30.
--
-- Estimated wall time on apply: under 15 seconds. DDL is metadata-only on
-- rcw + wac (DROP/RECREATE CHECK with 0 affected rows = free) + new schema
-- creation + 29 tag inserts. No large UPDATEs.
-- =============================================================================
--
-- Phase-8-deferred rule sets (kept in regex SET_CODES alternation but NOT
-- ingested in this run; cross-refs resolve to target_jurisdiction='state-wa-
-- court-rule' + target_rule_id=NULL until Phase 8 ingest resolves FK):
--
--   CCR (34) — Civil Commitment Rules
--   IRLJ (26) — Infraction Rules (traffic court)
--   AR (6) — Superior Court Administrative Rules
--   ARLJ (16) — Admin Rules for Limited Jurisdiction
--   BJAR (5) — Board for Judicial Administration Rules
--   DRJ (14) — Discipline Rules for Judges
--   JISCR (18) — JIS Committee Rules
--   SPR (9) — Special Proceedings
--   SPRC (7) — Special Proceedings Criminal
--   LLLT RPC, ELLLTC, ELPOC, LPORPC, BBP, ATJ, BJA, SCAR (defunct/mono-PDF)
--
-- =============================================================================

-- (No \set ON_ERROR_STOP / BEGIN here — this is a REVIEW-ONLY file. Transaction
--  wrapping and ON_ERROR_STOP semantics live in phase1_schema_apply.sql.)


-- =============================================================================
-- §0  PRE-EXECUTION GUARDS (review reference; phase1_schema_apply.sql RAISEs)
-- =============================================================================
-- Confirms live schema state matches what Phase 0 introspection found
-- (2026-04-30). Any drift halts before any writes.

DO $$
DECLARE
  schema_already_exists       INTEGER;
  slug_already_exists         INTEGER;
  short_form_rows_rcw         INTEGER;
  short_form_rows_wac         INTEGER;
  rcw_section_count           INTEGER;
  wac_section_count           INTEGER;
  usc_tag_count               INTEGER;
  rcw_tag_count               INTEGER;
  wac_tag_count               INTEGER;
  cj_baseline_count           INTEGER;
  rcw_check_pre_def           TEXT;
  wac_check_pre_def           TEXT;
BEGIN
  -- §0.1 wa_court_rules schema does not yet exist
  SELECT COUNT(*) INTO schema_already_exists
    FROM information_schema.schemata WHERE schema_name = 'wa_court_rules';
  IF schema_already_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.1]: wa_court_rules schema already exists. Drop or rename before re-applying.';
  END IF;

  -- §0.2 state-wa-court-rule slug absent in core.tags
  SELECT COUNT(*) INTO slug_already_exists
    FROM core.tags WHERE slug = 'state-wa-court-rule';
  IF slug_already_exists <> 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.2]: state-wa-court-rule slug already exists (% rows). Investigate before re-applying.', slug_already_exists;
  END IF;

  -- §0.3 Free harmonization preserved: 0 rows use 'wa-court-rule' short form on rcw
  SELECT COUNT(*) INTO short_form_rows_rcw
    FROM rcw.cross_references WHERE target_jurisdiction = 'wa-court-rule';
  IF short_form_rows_rcw > 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.3a]: % rcw.cross_references rows use ''wa-court-rule'' short form. Migrate values before harmonization.', short_form_rows_rcw;
  END IF;

  -- §0.4 Free harmonization preserved: 0 rows use 'wa-court-rule' short form on wac
  SELECT COUNT(*) INTO short_form_rows_wac
    FROM wac.cross_references WHERE target_jurisdiction = 'wa-court-rule';
  IF short_form_rows_wac > 0 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.4]: % wac.cross_references rows use ''wa-court-rule'' short form. Migrate values before harmonization.', short_form_rows_wac;
  END IF;

  -- §0.5 Existing CHECK constraints currently have 'wa-court-rule' short form
  --      on BOTH rcw + wac (i.e., the migration is meaningful, not a no-op)
  SELECT pg_get_constraintdef(oid) INTO rcw_check_pre_def
    FROM pg_constraint
    WHERE conname = 'chk_target_jurisdiction'
      AND connamespace = 'rcw'::regnamespace;
  IF rcw_check_pre_def NOT LIKE '%''wa-court-rule''%' THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5a]: rcw.cross_references chk_target_jurisdiction does not contain expected ''wa-court-rule'' short form. Constraint state is unexpected — investigate.';
  END IF;
  IF rcw_check_pre_def LIKE '%''state-wa-court-rule''%' THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5b]: rcw.cross_references chk_target_jurisdiction already contains ''state-wa-court-rule''. Harmonization may have partial-applied — investigate.';
  END IF;

  SELECT pg_get_constraintdef(oid) INTO wac_check_pre_def
    FROM pg_constraint
    WHERE conname = 'chk_target_jurisdiction'
      AND connamespace = 'wac'::regnamespace;
  IF wac_check_pre_def NOT LIKE '%''wa-court-rule''%' THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5c]: wac.cross_references chk_target_jurisdiction does not contain expected ''wa-court-rule'' short form. Constraint state is unexpected — investigate.';
  END IF;
  IF wac_check_pre_def LIKE '%''state-wa-court-rule''%' THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.5d]: wac.cross_references chk_target_jurisdiction already contains ''state-wa-court-rule''. Harmonization may have partial-applied — investigate.';
  END IF;

  -- §0.6 RCW + WAC + USC corpora intact (count snapshots from 2026-04-30)
  SELECT COUNT(*) INTO rcw_section_count FROM rcw.sections;
  IF rcw_section_count <> 51764 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.6a]: rcw.sections has % rows, expected 51,764. Halt.', rcw_section_count;
  END IF;

  SELECT COUNT(*) INTO wac_section_count FROM wac.sections;
  IF wac_section_count <> 73180 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.6b]: wac.sections has % rows, expected 73,180. Halt.', wac_section_count;
  END IF;

  -- §0.7 code-jurisdiction tag baseline: federal-usc + state-wa-rcw + state-wa-wac
  SELECT COUNT(*) INTO cj_baseline_count FROM core.tags
    WHERE category = 'code-jurisdiction'
      AND slug IN ('federal-usc','state-wa-rcw','state-wa-wac');
  IF cj_baseline_count <> 3 THEN
    RAISE EXCEPTION 'Pre-flight FAIL [§0.7]: code-jurisdiction baseline has % of expected 3 baseline rows. Halt.', cj_baseline_count;
  END IF;

  RAISE NOTICE 'Pre-flight: PASS — wa_court_rules absent; state-wa-court-rule slug absent; 0 short-form rows on rcw + wac; CHECKs ready for harmonization; corpora intact (rcw=%, wac=%); 3 baseline code-jurisdiction tags present',
               rcw_section_count, wac_section_count;
END $$;


-- =============================================================================
-- §HARMONIZE  Cross-references CHECK enum harmonization (rcw + wac, both)
-- =============================================================================
-- Per Phase 0 close-out 2026-04-30: harmonize the target_jurisdiction CHECK
-- enums on BOTH rcw.cross_references AND wac.cross_references in a single
-- transaction, replacing the placeholder 'wa-court-rule' short form with
-- slug-aligned 'state-wa-court-rule'. ZERO existing rows use 'wa-court-rule'
-- on either table (verified §0.3 + §0.4 above), so this is metadata-only DDL.
--
-- Same atomic transaction as wa_court_rules.* schema creation — rolls back
-- if any subsequent step fails.

ALTER TABLE rcw.cross_references DROP CONSTRAINT chk_target_jurisdiction;
ALTER TABLE rcw.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
  target_jurisdiction IN (
    'state-wa-rcw', 'federal-usc', 'state-wa-wac', 'state-wa-court-rule',
    'cfr', 'session-law', 'wa-constitution'
  )
);
COMMENT ON CONSTRAINT chk_target_jurisdiction ON rcw.cross_references IS
  'Harmonized 2026-04-30 (WA Court Rules Phase 1): replaced placeholder '
  '''wa-court-rule'' with slug-aligned ''state-wa-court-rule''. All four '
  'code-jurisdiction values (federal-usc, state-wa-rcw, state-wa-wac, '
  'state-wa-court-rule) now match their core.tags slugs. Zero existing '
  'rows used the pre-existing short form.';

ALTER TABLE wac.cross_references DROP CONSTRAINT chk_target_jurisdiction;
ALTER TABLE wac.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
  target_jurisdiction IN (
    'state-wa-rcw', 'federal-usc', 'state-wa-wac', 'state-wa-court-rule',
    'cfr', 'session-law', 'wa-constitution'
  )
);
COMMENT ON CONSTRAINT chk_target_jurisdiction ON wac.cross_references IS
  'Harmonized 2026-04-30 (WA Court Rules Phase 1): replaced placeholder '
  '''wa-court-rule'' with slug-aligned ''state-wa-court-rule''. Mirror of '
  'rcw.cross_references harmonization, applied in same transaction.';


-- =============================================================================
-- §1  wa_court_rules SCHEMA + 5 TABLES
-- =============================================================================

CREATE SCHEMA wa_court_rules;
COMMENT ON SCHEMA wa_court_rules IS
  'Washington State Court Rules — promulgated by WA Supreme Court under RCW '
  '2.04.190. Fourth code-jurisdiction corpus alongside usc.*, rcw.*, wac.*. '
  'Phase 4 ingest writes here; Phase 6 retrieval reads via core.documents/'
  'document_chunks (FK from wa_court_rules.rules.document_id). Designed '
  '2026-04-30; see /opt/wdws/wa_court_rules_research/phase0_cumulative_report.md.';


-- §1.1 wa_court_rules.rule_sets
-- One row per rule set (GR, RPC, CR, ER, RAP, CJC, GALR, etc.). Lazy-populated
-- at Phase 4 ingest time. All rule sets share statutory authority RCW 2.04.190
-- (WA Supreme Court rule-making power) — no per-rule authority chain table.
CREATE TABLE wa_court_rules.rule_sets (
    rule_set_code        TEXT PRIMARY KEY,
    rule_set_name        TEXT NOT NULL,
    promulgating_court   TEXT NOT NULL DEFAULT 'WA Supreme Court',
    statutory_authority  TEXT NOT NULL DEFAULT 'RCW 2.04.190',
    publication_date     DATE NOT NULL,
    last_ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rule_count           INTEGER,
    tier                 INTEGER,                -- 1 (Pause A) or 2 (Pause B); NULL for Phase-8-deferred
    is_in_scope          BOOLEAN NOT NULL DEFAULT FALSE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_rule_sets_tier CHECK (tier IS NULL OR tier IN (1, 2))
);
COMMENT ON TABLE wa_court_rules.rule_sets IS
  'One row per WA court-rule set. tier=1 = Tier 1 (Pause-Checkpoint A); '
  'tier=2 = Tier 2 (Pause-Checkpoint B); tier IS NULL = Phase-8-deferred '
  '(recognized in cross-refs but not ingested in this run). Lazy-populated '
  'at Phase 4 ingest time.';


-- §1.2 wa_court_rules.rules
-- One row per rule. Schema accommodates:
--   - Decimal numbering (RPC 1.1, ELC 15.1)
--   - Three-segment numbering (CrR 3.2.1)
--   - Alpha-suffix-on-decimal (RAP 18.13A)
--   - Alpha-prefix-then-dot (JuCR 5A.1)
--   - CJC inner rules unpacked (parent_canon='Canon 2', canonical_citation='CJC 2.11')
--   - Range-placeholder children (range_doc_id pointing to canonical range row)
--   - Range-placeholder canonical rows (range_start/range_end populated)
--   - Reserved/repealed singletons
--
-- chk_rule_text_status_invariant uses Path 1 loosening: is_reserved rows MAY
-- have placeholder text (parallel to WAC's Class A/B1/D treatment) so
-- range-expansion children can carry "This rule is reserved as part of CRLJ
-- 27-37. See range citation." while still being structurally `is_reserved`.
CREATE TABLE wa_court_rules.rules (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id          UUID NOT NULL UNIQUE REFERENCES core.documents(id) ON DELETE CASCADE,
    rule_set_code        TEXT NOT NULL REFERENCES wa_court_rules.rule_sets(rule_set_code),
    rule_number          TEXT NOT NULL,                 -- '31', '12', '1.1', '5A.1', '3.2.1'
    rule_title           TEXT NOT NULL,
    canonical_citation   TEXT NOT NULL,                 -- 'GR 31', 'RPC 1.1', 'CJC 2.11'
    is_repealed          BOOLEAN NOT NULL DEFAULT FALSE,
    is_reserved          BOOLEAN NOT NULL DEFAULT FALSE,
    range_doc_id         UUID REFERENCES wa_court_rules.rules(id),  -- self-FK; NULL on canonical range row, set on expanded children
    range_start          TEXT,                          -- on canonical range row only (e.g., '27' for CRLJ 27-37)
    range_end            TEXT,                          -- on canonical range row only (e.g., '37')
    parent_canon         TEXT,                          -- 'Canon 2' for CJC inner rules; NULL otherwise
    full_text            TEXT,                          -- NULL or '' for non-range repealed; populated for active and reserved-via-range
    source_pdf_url       TEXT,
    source_pdf_path      TEXT,
    has_comment          BOOLEAN NOT NULL DEFAULT FALSE,
    comment_text         TEXT,                          -- RPC-style [1]-[N] comments; CJC nested
    effective_date       DATE,
    last_amended_date    DATE,
    publication_date     DATE NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (canonical_citation, publication_date),

    -- Path 1 CHECK loosening (operator-approved 2026-04-30):
    --   - active rules: full_text required, no ghost flag
    --   - repealed: full_text empty/NULL
    --   - reserved: full_text MAY be empty OR placeholder (e.g.,
    --     'This rule is reserved as part of CRLJ 27-37. See range citation.')
    -- Parallel to WAC's Class A/B1/D placeholder treatment — see WAC schema for prior pattern.
    CONSTRAINT chk_rule_text_status_invariant CHECK (
      (full_text IS NOT NULL AND full_text <> ''
       AND NOT (is_repealed OR is_reserved))
      OR
      (is_repealed AND (full_text IS NULL OR full_text = ''))
      OR
      (is_reserved)
    ),

    -- Range-pair invariant: range_start and range_end are both set (canonical
    -- range row) or both NULL (everything else). Expanded child rows have
    -- range_doc_id set but NOT range_start/range_end.
    CONSTRAINT chk_range_pair CHECK (
      (range_start IS NULL AND range_end IS NULL)
      OR
      (range_start IS NOT NULL AND range_end IS NOT NULL)
    ),

    -- Canonical range row invariant: if range_start/range_end populated,
    -- this row is itself the canonical range row, so range_doc_id should be NULL.
    -- Expanded children have range_doc_id set but range_start/range_end NULL.
    CONSTRAINT chk_range_self_consistency CHECK (
      NOT (range_start IS NOT NULL AND range_doc_id IS NOT NULL)
    ),

    -- parent_canon should only be populated for CJC rules
    CONSTRAINT chk_parent_canon_only_cjc CHECK (
      parent_canon IS NULL OR rule_set_code = 'CJC'
    ),

    -- has_comment + comment_text consistency
    CONSTRAINT chk_comment_consistency CHECK (
      (has_comment = TRUE AND comment_text IS NOT NULL AND comment_text <> '')
      OR
      (has_comment = FALSE AND (comment_text IS NULL OR comment_text = ''))
    )
);
COMMENT ON TABLE wa_court_rules.rules IS
  'One row per WA court rule. Three structural classes via ghost flags + '
  'range_doc_id: (1) active — text + no flag; (2) repealed/rescinded — empty '
  'text + is_repealed=TRUE; (3) reserved — empty OR placeholder text + '
  'is_reserved=TRUE. CJC inner rules have parent_canon=''Canon N''. Range-'
  'placeholder canonical rows have range_start/range_end; expanded children '
  'have range_doc_id pointing to canonical. CHECK constraints are load-'
  'bearing audit channels per Phase 6 §K methodology.';


-- §1.3 wa_court_rules.cross_references
-- One row per cross-reference parsed from rule body / comments / history.
-- Phase-8-deferred sets resolve to target_jurisdiction='state-wa-court-rule'
-- + target_rule_id=NULL (FK resolved retroactively when Phase 8 ingests CCR/IRLJ/etc.).
CREATE TABLE wa_court_rules.cross_references (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_rule_id       UUID NOT NULL REFERENCES wa_court_rules.rules(id) ON DELETE CASCADE,
    target_citation      TEXT NOT NULL,                  -- e.g., 'GR 31', 'CR 12(b)(6)'
    target_rule_id       UUID REFERENCES wa_court_rules.rules(id),  -- nullable: unresolved if Phase-8-deferred or out-of-corpus
    target_jurisdiction  TEXT NOT NULL,
    target_set_code      TEXT,                           -- e.g., 'GR', 'CR', 'CCR' (recognized but unresolved)
    target_rule_number   TEXT,                           -- e.g., '31', '12', '1.1'
    target_subsection    TEXT,                           -- e.g., '(b)(6)', '(a)(2)(A)'
    target_cmt_index     INTEGER,                        -- e.g., 3 for "RPC 1.1 Comment [3]"
    context_snippet      TEXT,
    ref_type             TEXT NOT NULL,                  -- 'reference', 'authority', 'cross-rule', 'cross-statute', 'comment', 'subsection-cite'
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_cross_ref_jurisdiction CHECK (
      target_jurisdiction IN (
        'state-wa-court-rule', 'state-wa-rcw', 'state-wa-wac', 'federal-usc',
        'cfr', 'session-law', 'wa-constitution'
      )
    ),

    CONSTRAINT chk_cross_ref_type CHECK (
      ref_type IN ('reference', 'authority', 'cross-rule', 'cross-statute',
                   'comment', 'subsection-cite')
    )
);
COMMENT ON TABLE wa_court_rules.cross_references IS
  'One row per cross-reference parsed from rule body, comments, or history. '
  'Phase-8-deferred set citations are recognized (target_set_code populated) '
  'but unresolved (target_rule_id=NULL). Phase 8 ingest of deferred sets '
  'resolves the FK retroactively. See phase0_cumulative_report.md §10 for '
  'authority-resolution rate accounting doctrine.';


-- §1.4 wa_court_rules.rule_history
CREATE TABLE wa_court_rules.rule_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id         UUID NOT NULL REFERENCES wa_court_rules.rules(id) ON DELETE CASCADE,
    amendment_date  DATE,
    amendment_text  TEXT NOT NULL,                      -- e.g., 'Adopted effective 9/1/2002', 'Amended effective 4/1/2020'
    sort_order      INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (rule_id, sort_order)
);
COMMENT ON TABLE wa_court_rules.rule_history IS
  'One row per amendment-history entry parsed from a rule''s [Adopted ... '
  'Amended ... Repealed ...] block. sort_order preserves source ordering.';


-- §1.5 wa_court_rules.ingestion_runs
CREATE TABLE wa_court_rules.ingestion_runs (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_label                TEXT,                       -- e.g., 'tier-1-priority-block-2026-04-30'
    started_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at             TIMESTAMPTZ,
    rule_sets_attempted      TEXT[],                     -- e.g., ARRAY['GR','RPC','CR']
    rules_inserted           INTEGER NOT NULL DEFAULT 0,
    rules_updated            INTEGER NOT NULL DEFAULT 0,
    rules_skipped            INTEGER NOT NULL DEFAULT 0,
    error_count              INTEGER NOT NULL DEFAULT 0,
    notes                    TEXT,
    UNIQUE (run_label, started_at)
);
COMMENT ON TABLE wa_court_rules.ingestion_runs IS
  'Audit trail for Phase 4 ingestion runs. One row per logical run (tier '
  'block, single-set re-ingest, etc.).';


-- =============================================================================
-- §2  INDEXES
-- =============================================================================

-- §2.1 rules: by (rule_set_code, rule_number) — primary lookup pattern
CREATE INDEX idx_rules_set_num ON wa_court_rules.rules (rule_set_code, rule_number);

-- §2.2 rules: by canonical_citation — case-insensitive lookup
CREATE INDEX idx_rules_citation_lower ON wa_court_rules.rules (LOWER(canonical_citation));

-- §2.3 rules: by document_id — already UNIQUE via constraint, but explicit btree
--      for foreign-key joins from core.documents
-- (already covered by UNIQUE constraint)

-- §2.4 rules: partial idx on range_doc_id for reverse-range queries
--      ("show me all expanded children of this canonical range row")
CREATE INDEX idx_rules_range_doc_id_partial
  ON wa_court_rules.rules (range_doc_id)
  WHERE range_doc_id IS NOT NULL;

-- §2.5 rules: partial idx on parent_canon for CJC retrieval grouping
CREATE INDEX idx_rules_parent_canon_partial
  ON wa_court_rules.rules (parent_canon)
  WHERE parent_canon IS NOT NULL;

-- §2.6 cross_references: by source_rule_id — outgoing-cite lookup
CREATE INDEX idx_xrefs_source_rule_id ON wa_court_rules.cross_references (source_rule_id);

-- §2.7 cross_references: by target_rule_id partial — incoming-cite lookup,
--      excludes unresolved Phase-8-deferred citations
CREATE INDEX idx_xrefs_target_rule_id_partial
  ON wa_court_rules.cross_references (target_rule_id)
  WHERE target_rule_id IS NOT NULL;

-- §2.8 cross_references: by (target_jurisdiction, target_set_code, target_rule_number)
--      — for FK resolution during Phase 8 ingest of deferred sets
CREATE INDEX idx_xrefs_target_lookup
  ON wa_court_rules.cross_references (target_jurisdiction, target_set_code, target_rule_number);

-- §2.9 rule_history: by rule_id (for ordered history retrieval)
CREATE INDEX idx_rule_history_rule_id ON wa_court_rules.rule_history (rule_id, sort_order);


-- =============================================================================
-- §3  TAG INSERTS (29 new rows)
-- =============================================================================

-- §3a code-jurisdiction: state-wa-court-rule (1 new slug joining federal-usc + state-wa-rcw + state-wa-wac)
INSERT INTO core.tags (name, category, slug, description) VALUES
  ('Washington State Court Rules', 'code-jurisdiction', 'state-wa-court-rule',
   'Court rules promulgated by WA Supreme Court under RCW 2.04.190. Fourth code-jurisdiction corpus joining federal-usc, state-wa-rcw, state-wa-wac.')
ON CONFLICT (slug) DO NOTHING;

-- §3b court-rule-meta (3 ghost-flag tags for retrieval-time filtering)
INSERT INTO core.tags (name, category, slug, description) VALUES
  ('Rescinded Rule', 'court-rule-meta', 'court-rule-rescinded',
   'Rule has [RESCINDED] marker; full_text is empty. Cite-resolves but body is unavailable.'),
  ('Reserved Rule', 'court-rule-meta', 'court-rule-reserved',
   'Rule has [RESERVED] marker; full_text may be empty OR placeholder for range-expanded children. Cite-resolves to a placeholder.'),
  ('Range Placeholder Rule', 'court-rule-meta', 'court-rule-range-placeholder',
   'Rule was expanded from a reserved-range index entry (e.g., CRLJ 27-37 → CRLJ 27, CRLJ 28, ...). Has range_doc_id pointing to canonical range row.')
ON CONFLICT (slug) DO NOTHING;

-- §3c court-rule-set (15 in-scope rule-set tags for retrieval-time set-filtering)
INSERT INTO core.tags (name, category, slug, description) VALUES
  ('General Rules (GR)',                          'court-rule-set', 'gr',     'WA General Rules — court administration, records access, ADA, fee waivers'),
  ('Rules of Professional Conduct (RPC)',         'court-rule-set', 'rpc',    'WA RPC — lawyer ethics; load-bearing for Carlisle WSBA / Athena AI compliance'),
  ('Superior Court Civil Rules (CR)',             'court-rule-set', 'cr',     'WA superior-court civil procedure'),
  ('Rules of Evidence (ER)',                      'court-rule-set', 'er',     'WA evidence rules'),
  ('Rules of Appellate Procedure (RAP)',          'court-rule-set', 'rap',    'WA appellate procedure'),
  ('Code of Judicial Conduct (CJC)',              'court-rule-set', 'cjc',    'WA judicial conduct — 4 Canons unpacked into RULE N.M inner rules'),
  ('Guardian ad Litem Rules (GALR)',              'court-rule-set', 'galr',   'WA GAL rules — load-bearing for active Snohomish 26-2-00762-31 / 26-2-02783-31'),
  ('Superior Court Criminal Rules (CrR)',         'court-rule-set', 'crr',    'WA superior-court criminal procedure'),
  ('Criminal Rules for Limited Jurisdiction',     'court-rule-set', 'crrlj',  'WA criminal procedure for courts of limited jurisdiction'),
  ('Civil Rules for Limited Jurisdiction',        'court-rule-set', 'crlj',   'WA civil procedure for courts of limited jurisdiction'),
  ('Juvenile Court Rules (JuCR)',                 'court-rule-set', 'jucr',   'WA juvenile-court rules'),
  ('Civil Arbitration Rules (SCCAR)',             'court-rule-set', 'sccar',  'WA superior-court civil arbitration — modern replacement for retired MAR'),
  ('Limited Jurisdiction Appeal Rules (RALJ)',    'court-rule-set', 'ralj',   'WA appeal rules from courts of limited jurisdiction'),
  ('Admission and Practice Rules (APR)',          'court-rule-set', 'apr',    'WA admission and practice rules'),
  ('Lawyer Conduct Enforcement (ELC)',            'court-rule-set', 'elc',    'WA enforcement procedure for lawyer conduct — 133 rules; framework for Carlisle WSBA consultation about Athena AI compliance')
ON CONFLICT (slug) DO NOTHING;

-- §3d court-rule-topical (10 topical tags for cross-jurisdiction retrieval)
INSERT INTO core.tags (name, category, slug, description) VALUES
  ('Records Access (GR 31, 31.1)',         'court-rule-topical', 'records-access-gr31',
   'Court records access; load-bearing for Snohomish records work.'),
  ('ADA Accommodation (GR 33)',            'court-rule-topical', 'ada-accommodation-gr33',
   'ADA accommodation requests in WA courts; cross-references federal 42 USC 12132 + state RCW 49.60 + state WAC 162.'),
  ('Fee Waiver — Indigency (GR 34)',       'court-rule-topical', 'fee-waiver-gr34',
   'WA fee waiver on indigency basis; Jafar v. Webb authority anchor.'),
  ('Lawyer Discipline Procedure',          'court-rule-topical', 'lawyer-conduct-elc',
   'Topical tag for ELC procedural framework rules — Carlisle WSBA consultation context. Distinct from court-rule-set ''Lawyer Conduct Enforcement (ELC)'' which tags the corpus itself.'),
  ('Judicial Conduct (CJC)',               'court-rule-topical', 'judicial-conduct-cjc',
   'Topical tag for judicial-conduct rules; CJC Canon + inner-rule scope.'),
  ('Guardian ad Litem Strategy',           'court-rule-topical', 'gal-strategy-galr',
   'Topical tag for GALR work; active Snohomish matter context.'),
  ('Civil Procedure',                      'court-rule-topical', 'civil-procedure-cr',
   'Topical tag for CR superior-court civil procedure.'),
  ('Evidence (Court Rules)',               'court-rule-topical', 'evidence-er',
   'Topical tag for ER rules. Distinct from legal-framework ''Evidence'' which tags substantive evidence-law topics across statute + case law.'),
  ('Appellate Procedure',                  'court-rule-topical', 'appellate-rap',
   'Topical tag for RAP rules.'),
  ('Professional Conduct',                 'court-rule-topical', 'professional-conduct-rpc',
   'Topical tag for RPC rules.')
ON CONFLICT (slug) DO NOTHING;


-- =============================================================================
-- §POST  POST-APPLY GUARDS (verify all expected state)
-- =============================================================================

DO $$
DECLARE
  rcw_check_def              TEXT;
  wac_check_def              TEXT;
  wcr_check_def              TEXT;
  rcw_check_has_short        BOOLEAN;
  rcw_check_has_full         BOOLEAN;
  wac_check_has_short        BOOLEAN;
  wac_check_has_full         BOOLEAN;
  wcr_check_has_full         BOOLEAN;
  schema_present             INTEGER;
  table_count                INTEGER;
  cj_slug_present            INTEGER;
  meta_count                 INTEGER;
  set_count                  INTEGER;
  topical_count              INTEGER;
  index_count                INTEGER;
BEGIN
  -- §POST.1 rcw.cross_references CHECK is harmonized
  SELECT pg_get_constraintdef(oid) INTO rcw_check_def
    FROM pg_constraint
    WHERE conname = 'chk_target_jurisdiction'
      AND connamespace = 'rcw'::regnamespace;
  -- Pattern discrimination: 'wa-court-rule' is a SUBSTRING of 'state-wa-court-rule',
  -- but the LIKE pattern uses surrounding apostrophes which makes them non-overlapping:
  --   - %''wa-court-rule''% matches the literal apostrophe-wa-court-rule-apostrophe
  --   - %''state-wa-court-rule''% matches apostrophe-state-wa-court-rule-apostrophe
  -- The post-harmonization fragment is `,'state-wa-court-rule',` so the apostrophe
  -- before 'wa-court-rule' is preceded by 'state-' (NOT an apostrophe), meaning
  -- LIKE '%''wa-court-rule''%' will NOT match. Same discriminator pattern as WAC harmonization.
  rcw_check_has_short := (rcw_check_def LIKE '%''wa-court-rule''%' AND rcw_check_def NOT LIKE '%''state-wa-court-rule''%');
  rcw_check_has_full  := (rcw_check_def LIKE '%''state-wa-court-rule''%');
  IF rcw_check_has_short THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1a]: rcw.cross_references CHECK still contains short form ''wa-court-rule''. Harmonization did not apply.';
  END IF;
  IF NOT rcw_check_has_full THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.1b]: rcw.cross_references CHECK does not contain ''state-wa-court-rule''. Harmonization missing.';
  END IF;

  -- §POST.2 wac.cross_references CHECK is harmonized (mirror of §POST.1)
  SELECT pg_get_constraintdef(oid) INTO wac_check_def
    FROM pg_constraint
    WHERE conname = 'chk_target_jurisdiction'
      AND connamespace = 'wac'::regnamespace;
  wac_check_has_short := (wac_check_def LIKE '%''wa-court-rule''%' AND wac_check_def NOT LIKE '%''state-wa-court-rule''%');
  wac_check_has_full  := (wac_check_def LIKE '%''state-wa-court-rule''%');
  IF wac_check_has_short THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2a]: wac.cross_references CHECK still contains short form ''wa-court-rule''. Harmonization did not apply.';
  END IF;
  IF NOT wac_check_has_full THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.2b]: wac.cross_references CHECK does not contain ''state-wa-court-rule''. Harmonization missing.';
  END IF;

  -- §POST.3 wa_court_rules.cross_references CHECK uses 'state-wa-court-rule' from creation
  SELECT pg_get_constraintdef(oid) INTO wcr_check_def
    FROM pg_constraint
    WHERE conname = 'chk_cross_ref_jurisdiction'
      AND connamespace = 'wa_court_rules'::regnamespace;
  wcr_check_has_full := (wcr_check_def LIKE '%''state-wa-court-rule''%');
  IF NOT wcr_check_has_full THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.3]: wa_court_rules.cross_references CHECK does not contain ''state-wa-court-rule''. Definition error.';
  END IF;

  -- §POST.4 schema + 5 tables present
  SELECT COUNT(*) INTO schema_present
    FROM information_schema.schemata WHERE schema_name = 'wa_court_rules';
  IF schema_present <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.4a]: wa_court_rules schema missing.';
  END IF;

  SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'wa_court_rules';
  IF table_count <> 5 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.4b]: expected 5 tables in wa_court_rules, found %.', table_count;
  END IF;

  -- §POST.5 state-wa-court-rule slug present in core.tags
  SELECT COUNT(*) INTO cj_slug_present
    FROM core.tags WHERE slug = 'state-wa-court-rule';
  IF cj_slug_present <> 1 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.5]: state-wa-court-rule slug missing or duplicated (%).', cj_slug_present;
  END IF;

  -- §POST.6 court-rule-meta tags (3 expected)
  SELECT COUNT(*) INTO meta_count FROM core.tags WHERE category = 'court-rule-meta';
  IF meta_count <> 3 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.6]: court-rule-meta has % rows, expected 3.', meta_count;
  END IF;

  -- §POST.7 court-rule-set tags (15 expected — Tier 1 + Tier 2)
  SELECT COUNT(*) INTO set_count FROM core.tags WHERE category = 'court-rule-set';
  IF set_count <> 15 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.7]: court-rule-set has % rows, expected 15.', set_count;
  END IF;

  -- §POST.8 court-rule-topical tags (10 expected)
  SELECT COUNT(*) INTO topical_count FROM core.tags WHERE category = 'court-rule-topical';
  IF topical_count <> 10 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.8]: court-rule-topical has % rows, expected 10.', topical_count;
  END IF;

  -- §POST.9 indexes present (count is informational; we don't enforce exact match
  --         to avoid coupling to system-generated UNIQUE-constraint indexes)
  SELECT COUNT(*) INTO index_count
    FROM pg_indexes WHERE schemaname = 'wa_court_rules';
  IF index_count < 5 THEN
    RAISE EXCEPTION 'Post-apply FAIL [§POST.9]: only % indexes in wa_court_rules, expected at least 5.', index_count;
  END IF;

  RAISE NOTICE 'Post-apply: PASS — all three CHECK constraints harmonized; schema + 5 tables + % indexes present; tags seeded (1 jurisdiction, 3 meta, 15 set, 10 topical = 29 new rows)',
               index_count;
END $$;


-- =============================================================================
-- §V  VERIFICATION SELECTs (print to stdout for operator review)
-- =============================================================================

-- §V.1 Confirm wa_court_rules schema + tables
\echo ''
\echo '=== §V.1 wa_court_rules tables ==='
SELECT table_name, table_type
  FROM information_schema.tables
  WHERE table_schema = 'wa_court_rules'
  ORDER BY table_name;

-- §V.2 Confirm harmonized CHECK definitions on rcw + wac
\echo ''
\echo '=== §V.2 Harmonized CHECK constraints (rcw + wac + wa_court_rules) ==='
SELECT
  n.nspname || '.' || c.relname AS table_name,
  pg_get_constraintdef(con.oid) AS check_definition
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE con.conname IN ('chk_target_jurisdiction', 'chk_cross_ref_jurisdiction')
  AND n.nspname IN ('rcw', 'wac', 'wa_court_rules')
ORDER BY n.nspname;

-- §V.3 New tag rows (29 expected)
\echo ''
\echo '=== §V.3 New tag rows ==='
SELECT category, COUNT(*) AS count
  FROM core.tags
  WHERE category IN ('code-jurisdiction', 'court-rule-meta', 'court-rule-set', 'court-rule-topical')
  GROUP BY category
  ORDER BY category;

-- §V.4 Indexes on wa_court_rules
\echo ''
\echo '=== §V.4 wa_court_rules indexes ==='
SELECT indexname, tablename
  FROM pg_indexes
  WHERE schemaname = 'wa_court_rules'
  ORDER BY tablename, indexname;

-- §V.5 CHECK constraints on wa_court_rules.rules
\echo ''
\echo '=== §V.5 wa_court_rules.rules CHECK constraints ==='
SELECT conname AS constraint_name, pg_get_constraintdef(oid) AS definition
  FROM pg_constraint
  WHERE conrelid = 'wa_court_rules.rules'::regclass
    AND contype = 'c'
  ORDER BY conname;


-- (No COMMIT here — this is a REVIEW-ONLY file. The COMMIT lives in
--  phase1_schema_apply.sql at the same position.)

-- =============================================================================
-- END OF DESIGN — review and approve before applying via phase1_schema_apply.sql
-- =============================================================================
