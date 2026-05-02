-- =============================================================================
-- WAC Phase 1 Schema Design — STAGED FOR OPERATOR REVIEW
-- =============================================================================
-- Status:        STAGED — DO NOT APPLY without explicit operator authorization.
-- Author:        Athena WAC-Ingest agent
-- Date:          2026-04-30
-- Companion to:  /opt/wdws/wac_research/wac_source_findings.md
--                /opt/wdws/wac_research/authority_chain_design.md
--                /opt/wdws/wac_research/litigation_priority_addendum.md
--                /opt/wdws/wac_research/phase0_cumulative_report.md
-- Apply file:    /opt/wdws/wac_research/schema_apply.sql
--                (transaction-wrapped, RAISE EXCEPTION guards — review THIS file
--                 first; apply that file second)
-- Target DB:     ACE on klunky.12432.net — PostgreSQL 17 + pgvector
--
-- Sections:
--   §0  Pre-apply assertions (review reference; schema_apply.sql RAISEs on these)
--   §1  wac schema + table definitions (8 tables)
--   §2  wac.* indexes
--   §3  Tag taxonomy additions
--        §3a  code-jurisdiction: state-wa-wac (one new slug — joins 2 existing)
--        §3b  wac-meta category (8 values)
--        §3c  legal-framework topical tags (11 new values for WAC)
--        §3d  wac-title category — INTENTIONALLY EMPTY (lazy-populate during ingest per kickoff)
--   §4  No retroactive tagging needed (USC + RCW already carry code-jurisdiction)
--   §HARMONIZE  rcw.cross_references CHECK enum harmonization ('wac' → 'state-wa-wac')
--   §5  Post-apply verification queries
--   §6  Cross-references target_jurisdiction CHECK enum convention (post-harmonization)
--
-- Idempotency: every INSERT uses ON CONFLICT (slug) DO NOTHING. Re-running
-- after a partial apply is safe.
-- =============================================================================


-- =============================================================================
-- VERIFIED PRE-FLIGHT FINDINGS (recorded for review traceability)
-- =============================================================================
-- The following were observed via SELECT-only introspection on 2026-04-30
-- BEFORE this file was drafted. The schema_apply.sql §0 RAISE EXCEPTION
-- guards re-verify these at apply time; any drift halts the transaction
-- before any writes.
--
-- 1. core.tags is a normalized taxonomy:
--      (id INT PK, name TEXT NOT NULL UNIQUE, category TEXT NULL,
--       slug TEXT NULL UNIQUE, description TEXT NULL, parent_id INT NULL,
--       created_at TIMESTAMPTZ DEFAULT now())
--    Both `name` and `slug` are globally unique. All 20 proposed WAC slugs
--    were checked for collisions — zero hits.
--
-- 2. core.documents has NO `tags` array column. Tag association is exclusively
--    through the join table core.document_tags. Cross-jurisdiction filtering
--    uses an EXISTS subquery against this join (pattern documented in §6 of
--    /opt/wdws/rcw_research/schema_design.sql).
--
-- 3. The existing partial unique index `uniq_documents_usc_source` on
--    core.documents(source_path) WHERE document_type='statute' covers WAC
--    automatically (predicate is on document_type, not corpus). No new index
--    needed. Verified present.
--
-- 4. core.tags categories `code-jurisdiction` (2 rows: federal-usc,
--    state-wa-rcw), `wac-meta` (0 rows), `wac-title` (0 rows). RCW Phase 1
--    pre-created code-jurisdiction; this migration adds the third value
--    state-wa-wac.
--
-- 5. RCW corpus state: 51,764 sections, all tagged code-jurisdiction:
--    state-wa-rcw. USC corpus: 11,678 documents, all tagged code-jurisdiction:
--    federal-usc. Total: 63,442. WAC will add ~30,000 more.
--
-- 6. Existing CHECK constraint chk_target_jurisdiction on rcw.cross_references
--    accepts: 'state-wa-rcw','federal-usc','wac','cfr','wa-court-rule',
--             'session-law','wa-constitution'.
--    The SHORT form 'wac' is a forward-looking placeholder; ZERO existing rows
--    use it (verified 2026-04-30: 75,117 'state-wa-rcw' + 34 'wa-constitution'
--    + 1 'cfr' + 0 'wac'). Per operator architectural correction, this
--    migration HARMONIZES the enum to 'state-wa-wac' (matching the slug)
--    via §HARMONIZE below — DROP/RECREATE the CHECK on rcw.cross_references
--    in the same transaction. Free migration (0 existing rows). After harmonize,
--    code-jurisdiction values match between tag slugs and CHECK enums for the
--    three corpora: 'federal-usc', 'state-wa-rcw', 'state-wa-wac'.
--
-- 7. wac schema does not exist. wac-meta and wac-title tag categories empty.
--    Apply is a clean creation; no migration of pre-existing WAC data needed.
--
-- 8. rcw.* schema has 8 tables (7 + later-added pending_recodification columns
--    via amendment file). WAC will mirror with 8 tables: 7 parallel +
--    wac.authority_chain (the WAC-specific Option B table from kickoff §4.3).
-- =============================================================================


-- =============================================================================
-- §0  PRE-APPLY ASSERTIONS — operator runs these first
-- =============================================================================
-- These confirm the live schema is in the state this file was designed
-- against. Each assertion produces a 1-row result with `ok = true`. A `false`
-- result means schema drift since 2026-04-30 — halt and reconcile before
-- applying schema_apply.sql.

-- §0.1 WAC schema does not yet exist
SELECT NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='wac')
       AS ok_wac_schema_absent;

-- §0.2 RCW + USC corpora intact
SELECT 51764 = (SELECT COUNT(*) FROM rcw.sections) AS ok_rcw_section_count;
SELECT 11678 = (SELECT COUNT(*) FROM core.document_tags dt
                JOIN core.tags t ON t.id=dt.tag_id
                WHERE t.category='code-jurisdiction' AND t.slug='federal-usc')
       AS ok_usc_tag_count;

-- §0.3 None of the proposed WAC slugs collide
SELECT 0 = (SELECT COUNT(*) FROM core.tags WHERE slug IN (
              'state-wa-wac',
              'wac','state-regulation',
              'wac-repealed','wac-recodified','wac-decodified',
              'wac-expired','wac-reserved','wac-suspended',
              'wlad-enforcement-wac','bii-regulations-wac',
              'dshs-wac','dcyf-wac','hca-wac','doh-wac',
              'civil-service-wac','employment-security-wac',
              'licensing-wac','revenue-wac','pra-regulations-wac'
            )) AS ok_no_slug_collisions;

-- §0.4 wac-meta and wac-title categories empty (lazy-populated on ingest)
SELECT 0 = (SELECT COUNT(*) FROM core.tags WHERE category='wac-meta')   AS ok_no_wac_meta_yet;
SELECT 0 = (SELECT COUNT(*) FROM core.tags WHERE category='wac-title')  AS ok_no_wac_title_yet;

-- §0.5 Existing partial unique index on core.documents(source_path) for statutes
SELECT EXISTS (SELECT 1 FROM pg_indexes
               WHERE schemaname='core' AND indexname='uniq_documents_usc_source')
       AS ok_source_path_partial_unique_index_exists;

-- §0.6 RCW + USC code-jurisdiction tags present (will not be touched)
SELECT 2 = (SELECT COUNT(*) FROM core.tags WHERE category='code-jurisdiction'
            AND slug IN ('federal-usc','state-wa-rcw')) AS ok_existing_code_jurisdiction;


-- =============================================================================
-- §1  WAC SCHEMA + TABLES
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS wac;
COMMENT ON SCHEMA wac IS
  'Washington Administrative Code — state administrative regulations parallel '
  'to rcw.* and usc.*. Phase 4 ingest writes here; Phase 6 retrieval reads via '
  'core.documents/document_chunks (FK from wac.sections.document_id). Authority '
  'chain to rcw.* via wac.authority_chain join table. Designed 2026-04-30; see '
  '/opt/wdws/wac_research/schema_design.sql.';


-- ---------------------------------------------------------------------------
-- §1.1 wac.titles  (~116 rows; populated LAZY on Phase 4 ingest per kickoff)
-- ---------------------------------------------------------------------------
-- title_number is TEXT to accommodate lettered variants (e.g. 132A, 181, 132T,
-- 28A — confirmed in Phase 0 sampling).
CREATE TABLE wac.titles (
    title_number      TEXT PRIMARY KEY,
    title_name        TEXT NOT NULL,
    agency_name       TEXT,
    publication_date  DATE NOT NULL,                     -- '2026-04-01' for current corpus
    last_ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    chapter_count     INTEGER,
    section_count     INTEGER,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE wac.titles IS
  'One row per populated WAC title. Differs from rcw.titles in that titles '
  'are LAZY-POPULATED at Phase 4 ingest time, not pre-seeded in §3 below '
  '(operator decision per kickoff §4.5). publication_date is the canonical '
  'version anchor — currently 2026-04-01 (WAC landing page Last update: marker '
  'as of 2026-04-30). agency_name is WAC-specific (e.g. "Department of Labor '
  'and Industries"); RCW titles do not have an agency dimension.';


-- ---------------------------------------------------------------------------
-- §1.2 wac.chapters  (~2,300 rows expected)
-- ---------------------------------------------------------------------------
-- chapter_number is TEXT — 'T-CC' form with possible lettered variant
-- (e.g. '296-15', '296-14A', '162-16').
CREATE TABLE wac.chapters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_number     TEXT NOT NULL REFERENCES wac.titles(title_number),
    chapter_number   TEXT NOT NULL,
    chapter_heading  TEXT NOT NULL,
    sort_order       INTEGER NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (title_number, chapter_number)
);
COMMENT ON TABLE wac.chapters IS
  'One row per WAC chapter (e.g. 296-15, 162-16, 296-14A). Populated '
  'incrementally during Phase 4 priority-sequence ingest. Chapter format uses '
  'dashes, not dots like RCW (citation parser regex: \\d+-\\d+[A-Z]?).';


-- ---------------------------------------------------------------------------
-- §1.3 wac.sections  (~30,000 active rows + ghost rows for dispositions)
-- ---------------------------------------------------------------------------
CREATE TABLE wac.sections (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id              UUID NOT NULL UNIQUE REFERENCES core.documents(id) ON DELETE CASCADE,
    title_number             TEXT NOT NULL REFERENCES wac.titles(title_number),
    chapter_id               UUID REFERENCES wac.chapters(id),
    section_number           TEXT NOT NULL,                 -- '001', '030', '030(1)' (rare)
    canonical_citation       TEXT NOT NULL,                 -- 'WAC 296-15-001'
    section_heading          TEXT NOT NULL,
    -- Status flags. Mutually-non-exclusive booleans matching rcw.sections pattern.
    is_repealed              BOOLEAN NOT NULL DEFAULT FALSE,
    is_recodified            BOOLEAN NOT NULL DEFAULT FALSE,
    is_decodified            BOOLEAN NOT NULL DEFAULT FALSE,
    is_expired               BOOLEAN NOT NULL DEFAULT FALSE,
    is_reserved              BOOLEAN NOT NULL DEFAULT FALSE,
    -- Recodification target (set when is_recodified=TRUE)
    recodified_to            TEXT,                          -- e.g. 'WAC 296-15-XXX'
    -- Pending recodification fields (carried forward from RCW pattern; included from inception)
    pending_recodification_to        TEXT,
    pending_recodification_effective DATE,
    effective_date           DATE,
    -- WAC-specific: most-recent WSR register citation. Full revision history
    -- in wac.section_history; this column holds the latest filing's WSR.
    wsr_register_number      TEXT,                          -- e.g. 'WSR 21-11-083'
    -- full_text NULLable; CHECK constraint enforces "active ⇔ has text" invariant
    full_text                TEXT,
    source_html              TEXT,                          -- raw chapter HTML slice
    publication_date         DATE NOT NULL,                 -- '2026-04-01' for current corpus
    -- Unit type: 'section' for normal WAC sections, 'reserved' for placeholder slots
    unit_type                TEXT NOT NULL DEFAULT 'section',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (canonical_citation, publication_date),

    -- Active section ⇔ has text (Phase 6 §K methodology: load-bearing audit channel).
    -- Identical to rcw.sections.chk_section_text_status_invariant.
    CONSTRAINT chk_section_text_status_invariant CHECK (
      (full_text IS NOT NULL AND full_text <> ''
       AND NOT (is_repealed OR is_recodified OR is_decodified
                OR is_expired OR is_reserved))
      OR
      ((is_repealed OR is_recodified OR is_decodified
        OR is_expired OR is_reserved)
       AND (full_text IS NULL OR full_text = ''))
    ),

    CONSTRAINT chk_unit_type CHECK (unit_type IN ('section','reserved')),

    -- If recodified, recodified_to must be populated.
    CONSTRAINT chk_recodified_target CHECK (
      NOT is_recodified OR (recodified_to IS NOT NULL AND recodified_to <> '')
    ),

    -- Pending recodification: both NULL or both populated (defensive — same
    -- pattern as rcw.sections.chk_pending_recodification_pair from
    -- schema_amendment_pending_recodification.sql)
    CONSTRAINT chk_pending_recodification_pair CHECK (
      (pending_recodification_to IS NULL AND pending_recodification_effective IS NULL)
      OR
      (pending_recodification_to IS NOT NULL AND pending_recodification_effective IS NOT NULL)
    )
);
COMMENT ON TABLE wac.sections IS
  'One row per WAC section. Three flavors mirroring rcw.sections: '
  '(a) Live current — full_text populated, no status flags, chunked + embedded. ~30,000 rows expected. '
  '(b) Recodified ghost — is_recodified=TRUE, recodified_to set, full_text NULL. '
  '(c) Repealed/decodified/expired/reserved ghost — corresponding flag TRUE, full_text NULL. '
  'wsr_register_number stores the latest revision''s WSR citation for quick lookup; '
  'full revision history in wac.section_history. CHECK constraints are load-bearing '
  'audit channels per Phase 6 §K methodology.';


-- ---------------------------------------------------------------------------
-- §1.4 wac.cross_references  (parsed from section bodies)
-- ---------------------------------------------------------------------------
-- target_jurisdiction CHECK uses 'state-wa-wac' (slug-aligned long form),
-- matching the harmonized rcw.cross_references CHECK enum (§HARMONIZE below).
-- For code-jurisdiction values, slug == CHECK enum: federal-usc, state-wa-rcw,
-- state-wa-wac. The other enum values ('cfr', 'wa-court-rule', 'session-law',
-- 'wa-constitution') don't have established tag slugs yet — harmonization
-- decisions for those defer to when their respective corpora are scoped.
CREATE TABLE wac.cross_references (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_section_id   UUID NOT NULL REFERENCES wac.sections(id) ON DELETE CASCADE,
    target_citation     TEXT NOT NULL,                   -- normalized form: 'RCW 49.60.180', 'WAC 296-15-001', '42 U.S.C. § 12112'
    target_section_id   UUID REFERENCES wac.sections(id),  -- resolved post-Phase-4
    target_jurisdiction TEXT NOT NULL,                   -- enum below (slug-aligned for code-jurisdiction values)
    context_snippet     TEXT,                            -- sentence around the cite
    ref_type            TEXT NOT NULL,                   -- enum below
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_target_jurisdiction CHECK (
      target_jurisdiction IN (
        'state-wa-rcw','federal-usc','state-wa-wac','cfr',
        'wa-court-rule','session-law','wa-constitution'
      )
    ),
    CONSTRAINT chk_ref_type CHECK (
      ref_type IN ('reference','recodification','cross-statute','see-also','authority')
    )
);
COMMENT ON TABLE wac.cross_references IS
  'Cross-citations parsed from WAC section bodies. RCW citations in BODY text '
  'land here with target_jurisdiction=state-wa-rcw, ref_type=reference. RCW '
  'citations in [Statutory Authority: ...] BRACKETS land in wac.authority_chain '
  'instead — Phase 3 parser must use scope discriminator. Inbound recodification '
  'pointers (target_jurisdiction=state-wa-wac, ref_type=recodification) '
  'populated when a section''s disposition entry includes "Recodified as WAC ...". '
  'target_jurisdiction enum is slug-aligned for code-jurisdiction values per '
  'operator harmonization decision 2026-04-30.';


-- ---------------------------------------------------------------------------
-- §1.5 wac.section_history  (revision events from authority brackets + dispositions)
-- ---------------------------------------------------------------------------
CREATE TABLE wac.section_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id      UUID NOT NULL REFERENCES wac.sections(id) ON DELETE CASCADE,
    wsr_register    TEXT NOT NULL,                       -- 'WSR 21-11-083'
    register_year   INTEGER,                             -- 2021 (parsed from WSR YY-NN-NNN)
    filed_date      DATE,
    effective_date  DATE,
    action_type     TEXT,                                -- 'adopted' | 'amended' | 'repealed' | 'recodified' | 'decodified'
    action_summary  TEXT,                                -- free-form context (Initiative ref, etc.)
    sort_order      INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_action_type CHECK (
      action_type IS NULL OR action_type IN ('adopted','amended','repealed','recodified','decodified','reorganized')
    )
);
COMMENT ON TABLE wac.section_history IS
  'Parsed from the [Statutory Authority: ... WSR YY-NN-NNN, § T-CC-SSS, filed '
  'M/D/YY, effective M/D/YY; ...] revision events. One row per WSR cite. '
  'sort_order=1 is the most recent filing (matches wac.sections.wsr_register_number). '
  'Dispositions in the chapter''s inline disposition table also produce rows '
  'here with action_type=repealed/recodified/decodified.';


-- ---------------------------------------------------------------------------
-- §1.6 wac.section_notes  (any "NOTES:" block following a section)
-- ---------------------------------------------------------------------------
CREATE TABLE wac.section_notes (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id    UUID NOT NULL REFERENCES wac.sections(id) ON DELETE CASCADE,
    -- Permissive TEXT (no CHECK) — Phase 0 sample size 4 chapters did not
    -- surface notes blocks; allow unexpected types to accumulate as data and
    -- audit at Pause-Checkpoint A.
    note_type     TEXT NOT NULL,
    note_topic    TEXT,
    note_heading  TEXT,
    note_text     TEXT NOT NULL,
    sort_order    INTEGER NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE wac.section_notes IS
  'NOTES: blocks following WAC sections (if any). Phase 0 sampling did not '
  'surface any in 4 chapter samples — table kept for parity with rcw.section_notes. '
  'Permissive note_type (no CHECK) lets unexpected types accumulate as data; '
  'add a CHECK constraint in Phase 5 once corpus is full.';


-- ---------------------------------------------------------------------------
-- §1.7 wac.ingestion_runs  (mirrors rcw.ingestion_runs)
-- ---------------------------------------------------------------------------
CREATE TABLE wac.ingestion_runs (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    publication_date      DATE NOT NULL,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ,
    titles_planned        SMALLINT,
    titles_completed      SMALLINT,
    chapters_ingested     INTEGER,
    sections_ingested     INTEGER,
    chunks_ingested       INTEGER,
    embeddings_completed  INTEGER,
    authority_rows        INTEGER,                       -- WAC-specific: count of wac.authority_chain rows produced
    status                TEXT NOT NULL DEFAULT 'running',
    notes                 TEXT,

    CONSTRAINT chk_ingestion_status CHECK (
      status IN ('running','completed','failed','partial','paused')
    )
);
COMMENT ON TABLE wac.ingestion_runs IS
  'One row per Phase 4 ingest invocation. authority_rows counts wac.authority_chain '
  'rows produced (WAC-specific addition vs rcw.ingestion_runs). status enum '
  'matches rcw.* with paused for Pause-Checkpoint A and B.';


-- ---------------------------------------------------------------------------
-- §1.8 wac.authority_chain  (THE WAC-SPECIFIC TABLE — Option B per kickoff §4.3)
-- ---------------------------------------------------------------------------
-- One row per RCW citation parsed from a WAC section's [Statutory Authority: ...]
-- bracket. Resolves the WAC → RCW regulatory-authority chain. Foreign key to
-- rcw.sections populated by post-ingest resolution pass (3 passes — direct,
-- subsection-stripped, recodification-fallback).
--
-- The three discriminator flags (is_chapter_level, is_session_law, +
-- chk_authority_xor) refine the operator-locked sketch from kickoff §4.3.
-- Approved in operator's Phase 1 authorization message.
CREATE TABLE wac.authority_chain (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wac_section_id    UUID NOT NULL REFERENCES wac.sections(id) ON DELETE CASCADE,
    authorizing_rcw   TEXT NOT NULL,                    -- 'RCW 49.60.120(3)' | 'RCW 49.04' (chapter-level) | '2020 c 277' (session-law)
    rcw_section_id    UUID REFERENCES rcw.sections(id),  -- NULL until resolved post-ingest
    authority_text    TEXT,                              -- raw bracket text up through first period (audit trail)
    is_chapter_level  BOOLEAN NOT NULL DEFAULT FALSE,    -- true for 'chapter X.YY RCW' citations
    is_session_law    BOOLEAN NOT NULL DEFAULT FALSE,    -- true for 'YYYY c NN' (no RCW reference)
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (wac_section_id, authorizing_rcw),

    -- Mutual exclusion: a citation cannot be BOTH chapter-level AND session-law.
    -- Defensive pattern — same shape as chk_pending_recodification_pair.
    CONSTRAINT chk_authority_xor CHECK (
      NOT (is_chapter_level AND is_session_law)
    )
);
COMMENT ON TABLE wac.authority_chain IS
  'WAC → RCW regulatory-authority chain. One row per RCW citation in each WAC '
  'section''s [Statutory Authority: ...] bracket. Resolves the doctrinal reverse '
  'query "show all WAC sections promulgated under RCW 49.60". '
  'is_chapter_level discriminates chapter-scope authority (chapter 49.60 RCW); '
  'is_session_law captures non-RCW session-law citations (2020 c 277). '
  'rcw_section_id populated by post-ingest resolution pass (3-pass: direct, '
  'subsection-stripped, recodification-fallback). See authority_chain_design.md '
  'for parser routing and resolution strategy.';


-- =============================================================================
-- §2  WAC INDEXES
-- =============================================================================

-- Title / chapter / section lookups
CREATE INDEX idx_wac_chapters_title              ON wac.chapters(title_number);
CREATE INDEX idx_wac_sections_title              ON wac.sections(title_number);
CREATE INDEX idx_wac_sections_chapter            ON wac.sections(chapter_id);

-- Citation-lookup hot path
CREATE INDEX idx_wac_sections_citation           ON wac.sections(canonical_citation);

-- Publication-date filter
CREATE INDEX idx_wac_sections_publication        ON wac.sections(publication_date);

-- Status-flag partial indexes
CREATE INDEX idx_wac_sections_repealed           ON wac.sections(canonical_citation) WHERE is_repealed   = TRUE;
CREATE INDEX idx_wac_sections_recodified         ON wac.sections(canonical_citation) WHERE is_recodified = TRUE;
CREATE INDEX idx_wac_sections_decodified         ON wac.sections(canonical_citation) WHERE is_decodified = TRUE;
CREATE INDEX idx_wac_sections_expired            ON wac.sections(canonical_citation) WHERE is_expired    = TRUE;
CREATE INDEX idx_wac_sections_reserved           ON wac.sections(canonical_citation) WHERE is_reserved   = TRUE;

-- Effective-date filter
CREATE INDEX idx_wac_sections_effective_date     ON wac.sections(effective_date) WHERE effective_date IS NOT NULL;

-- Unit-type partial
CREATE INDEX idx_wac_sections_unit_type          ON wac.sections(unit_type) WHERE unit_type <> 'section';

-- Recodification target — supports forward-redirect
CREATE INDEX idx_wac_sections_recodified_to      ON wac.sections(recodified_to) WHERE recodified_to IS NOT NULL;

-- Pending-recodification partials (mirrors rcw)
CREATE INDEX idx_wac_sections_pending_recodif_to
  ON wac.sections(pending_recodification_to)
  WHERE pending_recodification_to IS NOT NULL;
CREATE INDEX idx_wac_sections_pending_recodif_effective
  ON wac.sections(pending_recodification_effective)
  WHERE pending_recodification_effective IS NOT NULL;

-- WSR register lookup
CREATE INDEX idx_wac_sections_wsr                ON wac.sections(wsr_register_number) WHERE wsr_register_number IS NOT NULL;

-- Full-text search
CREATE INDEX idx_wac_sections_text_fts           ON wac.sections USING gin(to_tsvector('english', COALESCE(full_text,'')));

-- Cross-references — both directions
CREATE INDEX idx_wac_xref_source                 ON wac.cross_references(source_section_id);
CREATE INDEX idx_wac_xref_target                 ON wac.cross_references(target_citation);
CREATE INDEX idx_wac_xref_target_jurisdiction    ON wac.cross_references(target_jurisdiction);
CREATE INDEX idx_wac_xref_ref_type               ON wac.cross_references(ref_type);

-- Section history
CREATE INDEX idx_wac_history_section             ON wac.section_history(section_id);
CREATE INDEX idx_wac_history_wsr                 ON wac.section_history(wsr_register);
CREATE INDEX idx_wac_history_year                ON wac.section_history(register_year) WHERE register_year IS NOT NULL;

-- Section notes
CREATE INDEX idx_wac_notes_section               ON wac.section_notes(section_id);
CREATE INDEX idx_wac_notes_type                  ON wac.section_notes(note_type);

-- Authority chain (the WAC-specific load-bearing indexes)
-- The reverse-authority query "every WAC under RCW 49.60.x" is a B-tree prefix
-- scan on authorizing_rcw. Forward query "all authorities for this WAC" uses
-- wac_section_id. Resolved-FK partial supports the post-resolution audit.
CREATE INDEX idx_wac_authority_rcw               ON wac.authority_chain(authorizing_rcw);
CREATE INDEX idx_wac_authority_section           ON wac.authority_chain(wac_section_id);
CREATE INDEX idx_wac_authority_resolved          ON wac.authority_chain(rcw_section_id) WHERE rcw_section_id IS NOT NULL;


-- =============================================================================
-- §3  TAG TAXONOMY ADDITIONS
-- =============================================================================
-- All inserts use ON CONFLICT (slug) DO NOTHING. Slug is globally unique;
-- collision check passed §0.3.


-- ---------------------------------------------------------------------------
-- §3a  code-jurisdiction category — add WAC value
-- ---------------------------------------------------------------------------
-- Joins existing federal-usc and state-wa-rcw (created in RCW Phase 1).
-- Phase 4 WAC ingest applies code-jurisdiction:state-wa-wac to every WAC
-- chunk. No retroactive tagging needed — RCW + USC already carry their
-- code-jurisdiction tags.
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('state-wa-wac',
   'State WA WAC',
   'code-jurisdiction',
   'Document is a section of the Washington Administrative Code (Washington State administrative regulations).')
ON CONFLICT (slug) DO NOTHING;


-- ---------------------------------------------------------------------------
-- §3b  wac-meta category — corpus / status structural tags (8 values)
-- ---------------------------------------------------------------------------
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('wac',
   'WAC',
   'wac-meta',
   'Document is a section of the Washington Administrative Code.'),
  ('state-regulation',
   'State Regulation',
   'wac-meta',
   'State-level administrative regulation (as opposed to federal CFR or court rule).'),
  ('wac-repealed',
   'WAC Repealed',
   'wac-meta',
   'WAC section was repealed; preserved as a ghost row for retrospective citation lookup.'),
  ('wac-recodified',
   'WAC Recodified',
   'wac-meta',
   'WAC section was recodified to a different citation; ghost row carries the recodified_to pointer.'),
  ('wac-decodified',
   'WAC Decodified',
   'wac-meta',
   'WAC section was decodified (omitted from publication); ghost row preserved.'),
  ('wac-expired',
   'WAC Expired',
   'wac-meta',
   'WAC section''s sunset clause fired; ghost row preserved.'),
  ('wac-reserved',
   'WAC Reserved',
   'wac-meta',
   'WAC section number is reserved; never had text.'),
  ('wac-suspended',
   'WAC Suspended',
   'wac-meta',
   'WAC section''s effective application is suspended pending some condition; rare.')
ON CONFLICT (slug) DO NOTHING;


-- ---------------------------------------------------------------------------
-- §3c  legal-framework topical tags — WAC additions (11 new values)
-- ---------------------------------------------------------------------------
-- Joins the existing 28 federal + 17 RCW = 45 legal-framework tags. Naming
-- convention: <concept>-wac to discriminate from RCW topical tags.
-- Operator-supplied list per Phase 1 authorization message.
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('wlad-enforcement-wac',
   'WLAD Enforcement (WAC)',
   'legal-framework',
   'WAC Title 162 — Human Rights Commission regulations enforcing RCW 49.60 (Washington Law Against Discrimination).'),
  ('bii-regulations-wac',
   'BII Regulations (WAC)',
   'legal-framework',
   'WAC Title 296 — Department of Labor & Industries regulations implementing RCW Title 51 (industrial insurance / workers'' compensation).'),
  ('dshs-wac',
   'DSHS Regulations (WAC)',
   'legal-framework',
   'WAC Title 388 — Department of Social and Health Services regulations implementing RCW Title 74 (public assistance, child welfare, long-term care).'),
  ('dcyf-wac',
   'DCYF Regulations (WAC)',
   'legal-framework',
   'WAC Title 110 — Department of Children, Youth and Families regulations implementing RCW chapter 26.44 (child abuse) and RCW Title 74 (foster care, adoption).'),
  ('hca-wac',
   'HCA Regulations (WAC)',
   'legal-framework',
   'WAC Title 182 — Health Care Authority regulations implementing RCW chapter 41.05 (Apple Health, employee benefits) and RCW Title 71 (behavioral health).'),
  ('doh-wac',
   'DOH Regulations (WAC)',
   'legal-framework',
   'WAC Title 246 — Department of Health regulations implementing RCW Title 18 (medical practice acts) and RCW Title 70 (public health).'),
  ('civil-service-wac',
   'Civil Service Regulations (WAC)',
   'legal-framework',
   'WAC Title 357 — State Human Resources / civil service regulations implementing RCW chapter 41.06.'),
  ('employment-security-wac',
   'Employment Security Regulations (WAC)',
   'legal-framework',
   'WAC Title 192 — Employment Security Department regulations implementing RCW Title 50 (unemployment insurance) and Title 50A (paid family medical leave).'),
  ('licensing-wac',
   'Licensing Regulations (WAC)',
   'legal-framework',
   'WAC Title 308 — Department of Licensing regulations across professional licensing, vehicle licensing, and related domains.'),
  ('revenue-wac',
   'Revenue Regulations (WAC)',
   'legal-framework',
   'WAC Title 458 — Department of Revenue regulations implementing RCW Title 82 (excise taxes), Title 84 (property taxes), and Title 83 (estate taxation).'),
  ('pra-regulations-wac',
   'PRA Regulations (WAC)',
   'legal-framework',
   'Cross-cutting WAC chapters implementing RCW chapter 42.56 (Public Records Act) — each agency publishes its own PRA-implementation WAC under its respective title.')
ON CONFLICT (slug) DO NOTHING;


-- ---------------------------------------------------------------------------
-- §3d  wac-title category — INTENTIONALLY EMPTY at Phase 1 apply
-- ---------------------------------------------------------------------------
-- Per kickoff §4.5 + Phase 1 authorization message: WAC titles are populated
-- LAZY during Phase 4 ingest (one tag row per title, on first encounter).
-- This differs from RCW Phase 1, which pre-seeded all 100 RCW titles in §3d
-- of its schema_design.sql. Reason: WAC has lettered title variants
-- (132A, 132B, etc.) and the exact populated set is not certain until Phase 2
-- enumeration. Lazy population avoids pre-seeding non-existent slugs.

-- Phase 4 ingest will execute (per title encountered):
--   INSERT INTO core.tags (slug, name, category, description) VALUES
--     ('wac-title-296', 'WAC Title 296 — Labor and Industries', 'wac-title',
--      'WAC Title 296: LABOR AND INDUSTRIES, DEPARTMENT OF')
--   ON CONFLICT (slug) DO NOTHING;


-- =============================================================================
-- §4  RETROACTIVE TAGGING — NOT NEEDED
-- =============================================================================
-- USC corpus is fully tagged code-jurisdiction:federal-usc (11,678 rows,
-- verified §0.2 above). RCW corpus is fully tagged code-jurisdiction:state-wa-rcw
-- (51,764 rows, verified §0.2 above). The only new code-jurisdiction value
-- introduced here is state-wa-wac, which gets applied to WAC chunks at Phase 4
-- ingest time. No retroactive UPDATE needed.


-- =============================================================================
-- §HARMONIZE  Cross-references CHECK enum harmonization
-- =============================================================================
-- Per operator architectural correction 2026-04-30: harmonize the
-- target_jurisdiction CHECK enum across BOTH cross-reference tables to use
-- slug-aligned long forms for code-jurisdiction values. The pre-existing
-- 'wac' short form on rcw.cross_references is a forward-looking placeholder;
-- ZERO existing rows use it (independently verified — operator + agent
-- introspection both confirmed 0 rows of 'wac' against 75,117 'state-wa-rcw',
-- 34 'wa-constitution', 1 'cfr').
--
-- Free migration. Compounding cost if deferred (post-WAC ingest, ~7,000+ rows
-- across rcw.cross_references and wac.cross_references would use 'wac'; Phase 8
-- migration becomes substantially more expensive).
--
-- Other CHECK enum values stay as-is. The 'cfr', 'wa-court-rule',
-- 'session-law', 'wa-constitution' values don't have established tag slugs
-- yet — those harmonization decisions defer naturally to when those corpora
-- are scoped. Don't touch them.

-- Pre-flight guard: confirm 0 rows still use 'wac' short form (any drift halts).
-- (Live in schema_apply.sql §0.7 as a RAISE EXCEPTION DO block.)

ALTER TABLE rcw.cross_references DROP CONSTRAINT chk_target_jurisdiction;
ALTER TABLE rcw.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
  target_jurisdiction IN (
    'state-wa-rcw', 'federal-usc', 'state-wa-wac',
    'cfr', 'wa-court-rule', 'session-law', 'wa-constitution'
  )
);
COMMENT ON CONSTRAINT chk_target_jurisdiction ON rcw.cross_references IS
  'Slug-aligned for code-jurisdiction values: federal-usc, state-wa-rcw, state-wa-wac '
  'all match their core.tags slugs. Other enum values await harmonization when their '
  'respective corpora are scoped (CFR, WA court rules, session laws, WA Constitution). '
  'Harmonized 2026-04-30 as part of WAC Phase 1 schema apply (zero existing rows used '
  'pre-existing ''wac'' short form).';


-- =============================================================================
-- §5  POST-APPLY VERIFICATION QUERIES
-- =============================================================================
-- Operator runs these after applying schema_apply.sql to confirm correct
-- state. Each should produce the expected count exactly.

-- §5.1 wac schema and all 8 tables exist
SELECT
  EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='wac')                                                 AS wac_schema,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='titles')           AS titles_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='chapters')         AS chapters_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='sections')         AS sections_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='cross_references') AS xref_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='section_history')  AS history_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='section_notes')    AS notes_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='ingestion_runs')   AS runs_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='wac' AND table_name='authority_chain')  AS authority_tbl;

-- §5.2 Tag pre-seeds inserted (lazy categories empty)
SELECT
  (SELECT COUNT(*) FROM core.tags WHERE category='code-jurisdiction')   AS code_jurisdiction_n,    -- expect 3 (federal-usc, state-wa-rcw, state-wa-wac)
  (SELECT COUNT(*) FROM core.tags WHERE category='wac-meta')            AS wac_meta_n,             -- expect 8
  (SELECT COUNT(*) FROM core.tags WHERE category='wac-title')           AS wac_title_n,            -- expect 0 (lazy-populated)
  (SELECT COUNT(*) FROM core.tags WHERE category='legal-framework')     AS legal_framework_total;  -- expect 45 (existing) + 11 (new) = 56

-- §5.3 No code-jurisdiction:state-wa-wac doc-tags yet (no WAC ingest)
SELECT COUNT(*) AS wac_docs_with_code_jurisdiction
FROM core.document_tags dt
JOIN core.tags t ON t.id = dt.tag_id
WHERE t.category='code-jurisdiction' AND t.slug='state-wa-wac';
-- Expect: 0 (Phase 4 will populate)

-- §5.4 RCW + USC corpora untouched
SELECT
  (SELECT COUNT(*) FROM core.document_tags dt JOIN core.tags t ON t.id=dt.tag_id WHERE t.slug='federal-usc')  AS usc_count,        -- expect 11,678
  (SELECT COUNT(*) FROM core.document_tags dt JOIN core.tags t ON t.id=dt.tag_id WHERE t.slug='state-wa-rcw') AS rcw_count,        -- expect 51,764
  (SELECT COUNT(*) FROM rcw.sections)                                                                          AS rcw_sections;     -- expect 51,764

-- §5.5 CHECK constraints registered correctly
SELECT
  EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_section_text_status_invariant' AND conrelid='wac.sections'::regclass)         AS chk_text_status,
  EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_recodified_target'             AND conrelid='wac.sections'::regclass)         AS chk_recod_target,
  EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_pending_recodification_pair'   AND conrelid='wac.sections'::regclass)         AS chk_pending_pair,
  EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_unit_type'                     AND conrelid='wac.sections'::regclass)         AS chk_unit,
  EXISTS (SELECT 1 FROM pg_constraint WHERE conname='chk_authority_xor'                 AND conrelid='wac.authority_chain'::regclass)  AS chk_auth_xor;


-- =============================================================================
-- §6  Cross-references target_jurisdiction CHECK enum convention
--     (post-harmonization)
-- =============================================================================
-- After §HARMONIZE above runs, BOTH rcw.cross_references and wac.cross_references
-- use the same target_jurisdiction CHECK enum:
--
--   'state-wa-rcw'    — slug-aligned, matches core.tags.slug='state-wa-rcw'
--   'federal-usc'     — slug-aligned, matches core.tags.slug='federal-usc'
--   'state-wa-wac'    — slug-aligned, matches core.tags.slug='state-wa-wac' (NEW)
--   'cfr'             — placeholder; harmonize when CFR corpus scoped
--   'wa-court-rule'   — placeholder; harmonize when WA court rules scoped
--   'session-law'     — placeholder; harmonize if session-law corpus is later modeled
--   'wa-constitution' — placeholder; harmonize if WA Constitution is later modeled
--
-- For code-jurisdiction values (the three-corpus discriminators), the
-- convention is now: tag slug == CHECK enum value. Document this in Phase 6
-- retrieval guide §M (cross-jurisdiction) per operator instruction.
--
-- Cross-citation directionality:
--   WAC → RCW: target_jurisdiction='state-wa-rcw'
--   WAC → USC: target_jurisdiction='federal-usc'
--   WAC → WAC: target_jurisdiction='state-wa-wac'
--   RCW → WAC: target_jurisdiction='state-wa-wac'
--   (RCW → RCW already used 'state-wa-rcw' pre-harmonization; unchanged)
--
-- For the canary_lib code_jurisdiction_filter kwarg, WAC adds the third value.
-- SQL composition unchanged from RCW Phase 1 design:
--   AND EXISTS (
--     SELECT 1 FROM core.document_tags dt
--     JOIN core.tags t ON t.id = dt.tag_id
--     WHERE dt.document_id = sc.document_id
--       AND t.category = 'code-jurisdiction'
--       AND t.slug = $code_jurisdiction_filter      -- 'state-wa-wac'
--   )
-- Pattern composes cleanly with existing tag_filter via two AND'd EXISTS.


-- =============================================================================
-- END OF schema_design.sql
-- =============================================================================
-- For apply, see /opt/wdws/wac_research/schema_apply.sql
-- Submit this file to operator for review. Do NOT apply until explicit go.
