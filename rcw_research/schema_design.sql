-- =============================================================================
-- RCW Phase 1 Schema Design — STAGED FOR OPERATOR REVIEW
-- =============================================================================
-- Status:        STAGED — DO NOT APPLY without explicit operator authorization.
-- Author:        Athena RCW-Ingest agent
-- Date:          2026-04-28
-- Companion to:  rcw_source_findings.md, cross_jurisdiction_design.md,
--                litigation_priority_addendum.md (all in /opt/wdws/rcw_research/)
-- Target DB:     ACE on klunky.12432.net — PostgreSQL 17 + pgvector
-- Pre-flight introspection completed:  yes — all assumptions verified against
--                live schema as of 2026-04-28. See "Verified Pre-flight Findings"
--                below.
-- Apply pattern: identical to USC Phase 1 — operator runs the §0 checks, reads
--                the §1–§5 DDL/DML, and authorizes apply via a separate session.
--
-- Sections of this file:
--   §0  Pre-apply assertions (operator runs first)
--   §1  rcw schema + table definitions
--   §2  rcw.* indexes
--   §3  Tag taxonomy additions
--        §3a  code-jurisdiction category (the new corpus-discriminator)
--        §3b  rcw-meta category
--        §3c  legal-framework topical tags (RCW additions)
--        §3d  rcw-title category — 100 title rows
--   §4  Retroactive tagging of all 11,678 existing USC documents
--   §5  Post-apply verification queries
--   §6  code_jurisdiction_filter SQL composition pattern (canary_lib reference)
--
-- Idempotency: every INSERT uses ON CONFLICT DO NOTHING anchored to the
-- canonical unique key (slug for tags, (document_id, tag_id) for joins, etc.).
-- Re-running this file after a partial apply is safe.
--
-- DDL is wrapped in NO transaction — let the operator decide BEGIN/COMMIT
-- pacing. The §4 retroactive tag UPDATE is the only operation with non-trivial
-- volume (~11,678 rows); everything else is small.
-- =============================================================================


-- =============================================================================
-- VERIFIED PRE-FLIGHT FINDINGS (recorded for review traceability)
-- =============================================================================
-- The following were observed via SELECT-only introspection on 2026-04-28
-- BEFORE this file was drafted. If any drift between then and apply, the §0
-- assertions will catch it.
--
-- 1. core.tags is a normalized taxonomy:
--      (id INT PK, name TEXT NOT NULL UNIQUE, category TEXT NULL,
--       slug TEXT NULL UNIQUE, description TEXT NULL, parent_id INT NULL,
--       created_at TIMESTAMPTZ DEFAULT now())
--    Both `name` and `slug` are globally unique (UNIQUE constraints
--    tags_name_key and tags_slug_key respectively). My proposed slugs and
--    names were checked against existing tags — zero collisions.
--
-- 2. core.documents has NO `tags` array column. Tag association is exclusively
--    through the join table:
--      core.document_tags (document_id UUID, tag_id INT,
--                          PRIMARY KEY (document_id, tag_id),
--                          both columns ON DELETE CASCADE).
--    Cross-jurisdiction filtering (the `code_jurisdiction_filter` kwarg from
--    the design doc) uses an EXISTS subquery against this join, NOT a
--    `tags && ARRAY[…]` predicate. See §6 for the canonical SQL form.
--
-- 3. core.searchable_chunks is a (regular) view joining core.document_chunks
--    to core.documents — no tag join inside the view. Tag filtering is added
--    by the caller as an EXISTS clause.
--
-- 4. The existing partial unique index `uniq_documents_usc_source` on
--    core.documents(source_path) WHERE document_type='statute' covers RCW
--    automatically (predicate is on document_type, not on the corpus). No
--    new index needed for source_path idempotency.
--
-- 5. Existing tag category `jurisdiction` has one row {slug:'wa:ada', name:
--    'WA ADA'} — case/matter scope of litigation, NOT code-jurisdiction.
--    Confirmed: my new tags use the distinct category name `code-jurisdiction`
--    to avoid the semantic collision the operator flagged.
--
-- 6. USC document count: 11,678 (matches operator's stated count). All 11,678
--    carry both `usc-meta:usc` (id=505) and `usc-meta:federal-statute` (id=506)
--    in core.document_tags. The §4 retroactive UPDATE uses `usc-meta:usc` as
--    the predicate anchor — most semantic fit.
--
-- 7. usc.* schema has 8 tables:
--      titles, chapters, subchapters, sections, cross_references,
--      section_history, section_notes, ingestion_runs.
--    RCW will mirror with 7 tables — drop `subchapters` (RCW chapters do not
--    use subchapters; deepest standard structure is title→chapter→section).
--
-- 8. usc.sections.full_text is NOT NULL. Per operator (Phase 1 decision #1),
--    rcw.sections.full_text is NULLable + a CHECK constraint enforcing the
--    "active section ⇔ has text; ghost row ⇔ no text" invariant.
--
-- 9. usc.sections has constraints:
--      UNIQUE (canonical_citation, release_point)   ← version-pair uniqueness
--      UNIQUE (document_id)                          ← 1:1 with core.documents
--      CHECK unit_type IN ('section','court_rule','reorganization_plan')
--      CHECK rule_set IN ('FRCP','FRAP','FRE','FRCrP','FRBP') OR NULL
--    rcw.sections mirrors the first two and replaces the unit_type check
--    with `IN ('section','reserved')` and drops the rule_set column entirely
--    (court rules are Phase 8 backlog).
-- =============================================================================


-- =============================================================================
-- §0  PRE-APPLY ASSERTIONS — operator runs these first
-- =============================================================================
-- These confirm the live schema is in the state this file was designed against.
-- Each assertion should produce a 1-row result with `ok = true`. A `false`
-- result means schema drift since 2026-04-28 — halt and reconcile before
-- applying §1-§5.

-- §0.1 USC document count is exactly 11,678 (the count we will retro-tag in §4)
SELECT 11678 = (SELECT COUNT(*) FROM core.documents
                WHERE document_type='statute' AND domain='legal') AS ok_usc_count;

-- §0.2 The `code-jurisdiction` tag category does NOT yet exist (we are creating it)
SELECT NOT EXISTS (SELECT 1 FROM core.tags WHERE category='code-jurisdiction')
       AS ok_no_code_jurisdiction_yet;

-- §0.3 The existing `jurisdiction` category (with slug 'wa:ada') is intact and
--      will not be touched by this migration
SELECT EXISTS (SELECT 1 FROM core.tags WHERE category='jurisdiction' AND slug='wa:ada')
       AS ok_existing_jurisdiction_intact;

-- §0.4 The `rcw` schema does NOT yet exist
SELECT NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='rcw')
       AS ok_rcw_schema_absent;

-- §0.5 The `usc.titles` template exists (we depend on its column structure for
--      §1 mirror-design; if it disappeared, this migration's structure assumptions
--      may be stale)
SELECT EXISTS (SELECT 1 FROM information_schema.tables
               WHERE table_schema='usc' AND table_name='titles')
       AS ok_usc_titles_exists;

-- §0.6 The partial unique index on core.documents(source_path) for statutes is
--      in place — RCW will rely on it for source_path idempotency
SELECT EXISTS (SELECT 1 FROM pg_indexes
               WHERE schemaname='core' AND indexname='uniq_documents_usc_source')
       AS ok_source_path_partial_unique_index_exists;

-- §0.7 None of the proposed RCW slugs already exist (zero-collision check)
SELECT 0 = (SELECT COUNT(*) FROM core.tags
            WHERE slug IN (
              'wlad','pra','oma','bii',
              'wa-civil-procedure','wa-criminal-procedure',
              'wa-mental-health','wa-public-assistance',
              'wa-domestic-relations','wa-probate',
              'wa-public-employment','wa-courts','wa-constitution',
              'cpa-wa','writs-wa','child-abuse-wa','gal-wa',
              'rcw','state-statute',
              'rcw-repealed','rcw-recodified','rcw-decodified',
              'rcw-expired','rcw-reserved','rcw-suspended',
              'federal-usc','state-wa-rcw'
            )) AS ok_no_slug_collisions;

-- §0.8 Title-row count: we expect to insert 100 rcw-title rows (§3d).
--      Pre-apply, count must be 0.
SELECT 0 = (SELECT COUNT(*) FROM core.tags WHERE category='rcw-title')
       AS ok_no_rcw_title_rows_yet;


-- =============================================================================
-- §1  RCW SCHEMA + TABLES
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS rcw;
COMMENT ON SCHEMA rcw IS
  'Revised Code of Washington — state statutory corpus parallel to usc.*. '
  'Phase 4 ingest writes here; Phase 6 retrieval reads via core.documents/'
  'document_chunks (FK from rcw.sections.document_id). Designed 2026-04-28; '
  'see /opt/wdws/rcw_research/schema_design.sql.';


-- ---------------------------------------------------------------------------
-- §1.1 rcw.titles  (100 rows; pre-seeded in §3d-equivalent application step)
-- ---------------------------------------------------------------------------
-- Note: title_number is TEXT (not SMALLINT like usc.titles) because RCW has
-- 16 lettered titles (9A, 23B, 28A, 28B, 28C, 29A, 29B, 30A, 30B, 35A, 50A,
-- 50B, 62A, 70A, 71A, 79A) that aren't expressible as integer.
CREATE TABLE rcw.titles (
    title_number      TEXT PRIMARY KEY,
    title_name        TEXT NOT NULL,
    publication_date  DATE NOT NULL,                     -- '2025-08-15' for current corpus
    last_ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    chapter_count     INTEGER,
    section_count     INTEGER,
    notes             TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE rcw.titles IS
  'One row per RCW title (1, 2, 9A, 23B, …, 91). Populated at start of '
  'Phase 4 from app.leg.wa.gov landing-page title list. publication_date '
  'is the canonical version anchor — currently 2025-08-15 (RCW landing '
  'page Last update: marker as of 2026-04-28).';


-- ---------------------------------------------------------------------------
-- §1.2 rcw.chapters  (~2,763 rows expected)
-- ---------------------------------------------------------------------------
-- Note: chapter_number is TEXT to accommodate forms like '4.04', '9A.04',
-- '4.24A', '70A.560'.
CREATE TABLE rcw.chapters (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title_number     TEXT NOT NULL REFERENCES rcw.titles(title_number),
    chapter_number   TEXT NOT NULL,
    chapter_heading  TEXT NOT NULL,
    sort_order       INTEGER NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (title_number, chapter_number)
);
COMMENT ON TABLE rcw.chapters IS
  'One row per RCW chapter (e.g. 4.04, 42.56, 9A.04, 11.96A). '
  'Populated incrementally during Phase 4 priority-sequence ingest.';


-- ---------------------------------------------------------------------------
-- §1.3 rcw.sections  (~45,000 active rows + ghost rows for repealed/recodified)
-- ---------------------------------------------------------------------------
CREATE TABLE rcw.sections (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id              UUID NOT NULL UNIQUE REFERENCES core.documents(id) ON DELETE CASCADE,
    title_number             TEXT NOT NULL REFERENCES rcw.titles(title_number),
    chapter_id               UUID REFERENCES rcw.chapters(id),
    section_number           TEXT NOT NULL,                 -- '030', '030(1)', '030A', '94A.030'
    canonical_citation       TEXT NOT NULL,                 -- 'RCW 42.56.030'
    section_heading          TEXT NOT NULL,
    -- Status flags. Mutually-non-exclusive booleans rather than an enum because
    -- WA dispositions occasionally combine flags (e.g., 'expired AND recodified').
    is_repealed              BOOLEAN NOT NULL DEFAULT FALSE,
    is_recodified            BOOLEAN NOT NULL DEFAULT FALSE,
    is_decodified            BOOLEAN NOT NULL DEFAULT FALSE,
    is_expired               BOOLEAN NOT NULL DEFAULT FALSE,
    is_reserved              BOOLEAN NOT NULL DEFAULT FALSE,
    -- Recodification target (set when is_recodified=TRUE)
    recodified_to            TEXT,                          -- e.g. 'RCW 42.56.520'
    effective_date           DATE,
    enacted_by_session_law   TEXT,                          -- e.g. '2005 c 274 s 285'
    -- full_text NULLable per Phase 0 finding §3 + Phase 1 operator decision #1.
    -- Active sections must have text; ghost rows must NOT (initial state) until
    -- a Phase 7+ archive sweep backfills prior-version text from the certified
    -- RCW Archive PDFs. The CHECK constraint below enforces this invariant.
    full_text                TEXT,
    source_html              TEXT,                          -- raw chapter HTML slice (NULLable for ghost rows)
    publication_date         DATE NOT NULL,                 -- '2025-08-15' for current corpus
    -- Unit type: 'section' for normal RCW sections, 'reserved' for reserved/
    -- placeholder slots. WA does NOT have USC's 'court_rule' or 'reorganization_plan'
    -- variants in-corpus (court rules are Phase 8 backlog, separate ingest).
    unit_type                TEXT NOT NULL DEFAULT 'section',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (canonical_citation, publication_date),

    -- Operator-approved CHECK constraint (Phase 1 decision #1, extended to
    -- include is_expired and is_reserved as additional ghost-row flags).
    -- Semantic: a row is EITHER an active section (with text) XOR a ghost row
    -- (no text, marked by ≥1 status flag). Phase 7+ archive backfill of prior-
    -- version text into ghost rows would require relaxing this constraint —
    -- defer that schema change until Phase 7 lands.
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

    -- If recodified, recodified_to should be populated with the target citation.
    CONSTRAINT chk_recodified_target CHECK (
      NOT is_recodified OR (recodified_to IS NOT NULL AND recodified_to <> '')
    )
);
COMMENT ON TABLE rcw.sections IS
  'One row per RCW section. Three flavors: '
  '(a) Live current section — full_text populated, no status flags set, '
  '    chunked + embedded into core.document_chunks. ~45,000 rows expected. '
  '(b) Recodified ghost — is_recodified=TRUE, recodified_to set, full_text NULL. '
  '    Allows historical-citation lookups (e.g., RCW 42.17.320 → 42.56.520). '
  '(c) Repealed/decodified/expired/reserved ghost — corresponding status flag '
  '    TRUE, full_text NULL. '
  'Ghost rows have FK to core.documents but the document row is a stub '
  '(short pointer text in full_content; no embeddings). '
  'See cross_jurisdiction_design.md §6 for retrieval semantics.';


-- ---------------------------------------------------------------------------
-- §1.4 rcw.cross_references  (parsed from section bodies; ~hundreds of thousands of rows expected)
-- ---------------------------------------------------------------------------
CREATE TABLE rcw.cross_references (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_section_id   UUID NOT NULL REFERENCES rcw.sections(id) ON DELETE CASCADE,
    target_citation     TEXT NOT NULL,                                 -- normalized form, e.g. 'RCW 42.56.030', '42 U.S.C. § 1983'
    target_section_id   UUID REFERENCES rcw.sections(id),              -- resolved later (post-Phase-4)
    target_jurisdiction TEXT NOT NULL,                                 -- 'state-wa-rcw' | 'federal-usc' | 'wac' | 'cfr' | 'wa-court-rule' | 'session-law' | 'wa-constitution'
    context_snippet     TEXT,                                          -- sentence around the cite
    ref_type            TEXT NOT NULL,                                 -- 'reference' | 'recodification' | 'cross-statute' | 'see-also' | 'authority'
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_target_jurisdiction CHECK (
      target_jurisdiction IN (
        'state-wa-rcw','federal-usc','wac','cfr',
        'wa-court-rule','session-law','wa-constitution'
      )
    ),
    CONSTRAINT chk_ref_type CHECK (
      ref_type IN ('reference','recodification','cross-statute','see-also','authority')
    )
);
COMMENT ON TABLE rcw.cross_references IS
  'Cross-citations parsed from RCW section bodies. Includes inbound recodification '
  'pointers (target_jurisdiction=state-wa-rcw, ref_type=recodification — populated '
  'when a section''s source-credit bracket includes "Formerly RCW X.YY.ZZZ"). '
  'target_section_id is a post-Phase-4 resolution step; nullable until then.';


-- ---------------------------------------------------------------------------
-- §1.5 rcw.section_history  (session-law citations from the [...] brackets)
-- ---------------------------------------------------------------------------
CREATE TABLE rcw.section_history (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id      UUID NOT NULL REFERENCES rcw.sections(id) ON DELETE CASCADE,
    session_law     TEXT NOT NULL,                          -- '2017 c 303 s 3', '1973 c 1 s 32', '1862 p 83 s 1', 'Code 1881 s 1', 'RRS s 143'
    session_year    INTEGER,                                -- 1862, 1881, 2017 — NULL for RRS / Code references
    session_law_pdf TEXT,                                   -- URL of the linked SL PDF (lawfilesext or CodeReviser)
    action_type     TEXT,                                   -- 'enacted' | 'amended' | 'recodified' | 'repealed'
    action_summary  TEXT,                                   -- '(Initiative Measure No. 276, approved November 7, 1972)' etc.
    sort_order      INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_action_type CHECK (
      action_type IS NULL OR action_type IN ('enacted','amended','recodified','repealed','reorganized')
    )
);
COMMENT ON TABLE rcw.section_history IS
  'Parsed from the [ ... ] source-credit brackets at the end of each section body. '
  'Each linked session-law citation gets one row. session_year is NULL for pre-'
  'codification references like "Code 1881 s 1" or "RRS s 143". sort_order '
  'preserves the in-bracket order — most recent first per RCW publication '
  'convention, so sort_order=1 is the latest amendment.';


-- ---------------------------------------------------------------------------
-- §1.6 rcw.section_notes  (the NOTES: block following each section)
-- ---------------------------------------------------------------------------
CREATE TABLE rcw.section_notes (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id    UUID NOT NULL REFERENCES rcw.sections(id) ON DELETE CASCADE,
    -- note_type is INTENTIONALLY a permissive TEXT (no CHECK constraint).
    -- Phase 0 §2.7 catalog identified 12+ types from 7 samples
    -- (Reviser's note, Findings, Effective date, Construction, Conflict with
    --  federal requirements, Expiration date, Severability, Short title,
    --  Application, Finding, Legislative findings, Purpose), and the list is
    -- explicitly non-exhaustive. Permissive TEXT lets unexpected types
    -- accumulate as data; audit at end of priority-core to refine taxonomy.
    note_type     TEXT NOT NULL,
    note_topic    TEXT,                       -- e.g. session-law cite the note attaches to: '2010 c 69'
    note_heading  TEXT,                       -- e.g. 'Finding—2010 c 69'
    note_text     TEXT NOT NULL,
    sort_order    INTEGER NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE rcw.section_notes IS
  'Parsed from the <h3>NOTES:</h3> block (or <h3>Notes:</h3> in single-section view) '
  'after each section''s source-credit bracket. note_type is permissive TEXT '
  'rather than a strict enum — see Phase 0 findings §2.7 for the observed '
  'catalog. Add a CHECK constraint in Phase 5 once the corpus is full and the '
  'distribution is known.';


-- ---------------------------------------------------------------------------
-- §1.7 rcw.ingestion_runs  (mirrors usc.ingestion_runs)
-- ---------------------------------------------------------------------------
CREATE TABLE rcw.ingestion_runs (
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
    status                TEXT NOT NULL DEFAULT 'running',
    notes                 TEXT,

    CONSTRAINT chk_ingestion_status CHECK (
      status IN ('running','completed','failed','partial','paused')
    )
);
COMMENT ON TABLE rcw.ingestion_runs IS
  'One row per Phase 4 ingest invocation (priority-core, long-tail, Phase 7 '
  'differential update). Populated incrementally as titles/chapters complete. '
  'Adds a "paused" status to USC''s set, for the explicit pause-checkpoints '
  '(A and B) in the Phase 4 priority sequence.';


-- =============================================================================
-- §2  RCW INDEXES
-- =============================================================================

-- Title lookups
CREATE INDEX idx_rcw_chapters_title             ON rcw.chapters(title_number);
CREATE INDEX idx_rcw_sections_title             ON rcw.sections(title_number);
CREATE INDEX idx_rcw_sections_chapter           ON rcw.sections(chapter_id);

-- Citation-lookup (Pattern (b) hot path — must be O(log n))
CREATE INDEX idx_rcw_sections_citation          ON rcw.sections(canonical_citation);

-- Publication-date filtering (for Phase 7+ when we have multiple publication versions)
CREATE INDEX idx_rcw_sections_publication       ON rcw.sections(publication_date);

-- Status-flag partial indexes — small, used for "show me the ghost rows" queries
CREATE INDEX idx_rcw_sections_repealed          ON rcw.sections(canonical_citation) WHERE is_repealed = TRUE;
CREATE INDEX idx_rcw_sections_recodified        ON rcw.sections(canonical_citation) WHERE is_recodified = TRUE;
CREATE INDEX idx_rcw_sections_decodified        ON rcw.sections(canonical_citation) WHERE is_decodified = TRUE;
CREATE INDEX idx_rcw_sections_expired           ON rcw.sections(canonical_citation) WHERE is_expired   = TRUE;
CREATE INDEX idx_rcw_sections_reserved          ON rcw.sections(canonical_citation) WHERE is_reserved  = TRUE;

-- Effective-date filtering (sparse — most sections lack one)
CREATE INDEX idx_rcw_sections_effective_date    ON rcw.sections(effective_date) WHERE effective_date IS NOT NULL;

-- Unit-type partial index (matches USC pattern; all live rows share unit_type='section', so the partial filter excludes them)
CREATE INDEX idx_rcw_sections_unit_type         ON rcw.sections(unit_type) WHERE unit_type <> 'section';

-- Recodification target — supports forward-redirect lookups
CREATE INDEX idx_rcw_sections_recodified_to     ON rcw.sections(recodified_to) WHERE recodified_to IS NOT NULL;

-- Full-text search on the raw section text (parallel to usc.idx_sections_text_fts)
CREATE INDEX idx_rcw_sections_text_fts          ON rcw.sections USING gin(to_tsvector('english', COALESCE(full_text,'')));

-- Cross-references — both directions
CREATE INDEX idx_rcw_xref_source                ON rcw.cross_references(source_section_id);
CREATE INDEX idx_rcw_xref_target                ON rcw.cross_references(target_citation);
CREATE INDEX idx_rcw_xref_target_jurisdiction   ON rcw.cross_references(target_jurisdiction);
CREATE INDEX idx_rcw_xref_ref_type              ON rcw.cross_references(ref_type);

-- Section history
CREATE INDEX idx_rcw_history_section            ON rcw.section_history(section_id);
CREATE INDEX idx_rcw_history_session_law        ON rcw.section_history(session_law);
CREATE INDEX idx_rcw_history_year               ON rcw.section_history(session_year) WHERE session_year IS NOT NULL;

-- Section notes
CREATE INDEX idx_rcw_notes_section              ON rcw.section_notes(section_id);
CREATE INDEX idx_rcw_notes_type                 ON rcw.section_notes(note_type);
CREATE INDEX idx_rcw_notes_topic                ON rcw.section_notes(note_topic) WHERE note_topic IS NOT NULL;


-- =============================================================================
-- §3  TAG TAXONOMY ADDITIONS
-- =============================================================================
-- All inserts use ON CONFLICT (slug) DO NOTHING for idempotency. Slug is
-- globally unique (tags_slug_key); collision check passed §0.7.
--
-- Tag-id values are auto-assigned by the sequence on tags.id. We never
-- hardcode tag_id values in this file — downstream INSERTs into
-- core.document_tags resolve tag_id by slug lookup at apply time.


-- ---------------------------------------------------------------------------
-- §3a  code-jurisdiction category — the new corpus-discriminator
-- ---------------------------------------------------------------------------
-- This category disambiguates BODY-OF-LAW jurisdiction. Distinct from the
-- existing `jurisdiction` category, which holds case/matter scope (the lone
-- existing row is `wa:ada` — "WA ADA matters").
--
-- Phase 4 RCW sections get `code-jurisdiction:state-wa-rcw`.
-- Phase 1 §4 retroactive operation tags all 11,678 USC docs with
--   `code-jurisdiction:federal-usc`.
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('federal-usc',
   'Federal USC',
   'code-jurisdiction',
   'Document is a section of the United States Code (federal statutory law).'),
  ('state-wa-rcw',
   'State WA RCW',
   'code-jurisdiction',
   'Document is a section of the Revised Code of Washington (Washington State statutory law).')
ON CONFLICT (slug) DO NOTHING;
-- Phase 8 future additions (NOT inserted now):
--   ('state-wa-wac',     'State WA WAC',        'code-jurisdiction', '… WAC …'),
--   ('wa-court-rule',    'WA Court Rule',       'code-jurisdiction', 'GR/CR/CrR/ER/RAP/etc.'),
--   ('cfr',              'Federal CFR',         'code-jurisdiction', '… CFR …').


-- ---------------------------------------------------------------------------
-- §3b  rcw-meta category — corpus / status structural tags
-- ---------------------------------------------------------------------------
-- Mirrors usc-meta but specific to RCW. Tags applied at Phase 4 ingest.
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('rcw',
   'RCW',
   'rcw-meta',
   'Document is a section of the Revised Code of Washington.'),
  ('state-statute',
   'State Statute',
   'rcw-meta',
   'State statutory law (as opposed to court rule, regulation, constitutional provision).'),
  ('rcw-repealed',
   'RCW Repealed',
   'rcw-meta',
   'RCW section was repealed; preserved as a ghost row for retrospective citation lookup.'),
  ('rcw-recodified',
   'RCW Recodified',
   'rcw-meta',
   'RCW section was recodified to a different citation; ghost row carries the recodified_to pointer.'),
  ('rcw-decodified',
   'RCW Decodified',
   'rcw-meta',
   'RCW section was decodified (removed from the code); rare.'),
  ('rcw-expired',
   'RCW Expired',
   'rcw-meta',
   'RCW section''s sunset clause fired; ghost row preserved.'),
  ('rcw-reserved',
   'RCW Reserved',
   'rcw-meta',
   'RCW section number is reserved; never had text.'),
  ('rcw-suspended',
   'RCW Suspended',
   'rcw-meta',
   'RCW section''s effective application is suspended pending some condition; rare.')
ON CONFLICT (slug) DO NOTHING;


-- ---------------------------------------------------------------------------
-- §3c  legal-framework topical tags — RCW additions
-- ---------------------------------------------------------------------------
-- Joins the existing 28 federal legal-framework tags. Slugs prefixed with
-- 'wa-' or named after the WA-specific framework (wlad, pra, oma, bii, cpa-wa)
-- to disambiguate from federal counterparts (e.g. existing `criminal-procedure`
-- is federal; new `wa-criminal-procedure` is WA).
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('wlad',
   'WLAD',
   'legal-framework',
   'Washington Law Against Discrimination — RCW chapter 49.60. Civil-rights statute analogue/pair to federal Title VII / ADA.'),
  ('pra',
   'PRA',
   'legal-framework',
   'Washington Public Records Act — RCW chapter 42.56. Records-access statute, parallels FOIA (5 U.S.C. § 552).'),
  ('oma',
   'OMA',
   'legal-framework',
   'Washington Open Public Meetings Act — RCW chapter 42.30.'),
  ('bii',
   'BII',
   'legal-framework',
   'Washington Industrial Insurance / workers'' compensation — RCW Title 51. Includes BIIA appeals (chapter 51.52).'),
  ('wa-civil-procedure',
   'WA Civil Procedure',
   'legal-framework',
   'Washington civil-procedure statutes — RCW Title 4. Includes statutes of limitations (4.16), claims against governmental entities (4.96), frivolous-action fees (4.84).'),
  ('wa-criminal-procedure',
   'WA Criminal Procedure',
   'legal-framework',
   'Washington criminal procedure — RCW Title 10 plus procedure-flavored sections in Titles 9 and 9A. Distinct from federal `criminal-procedure` tag.'),
  ('wa-mental-health',
   'WA Mental Health',
   'legal-framework',
   'Washington behavioral-health / civil-commitment statutes — RCW Title 71.'),
  ('wa-public-assistance',
   'WA Public Assistance',
   'legal-framework',
   'Washington public-assistance statutes — RCW Title 74. Includes vulnerable-adult civil cause of action (74.34).'),
  ('wa-domestic-relations',
   'WA Domestic Relations',
   'legal-framework',
   'Washington domestic-relations statutes — RCW Title 26. Includes parenting-plan restrictions (26.09.191) and child-abuse reporting (26.44).'),
  ('wa-probate',
   'WA Probate',
   'legal-framework',
   'Washington probate / trust law — RCW Title 11. Includes incapacitated-person doctrine (11.88.010(1)) and TEDRA (11.96A).'),
  ('wa-public-employment',
   'WA Public Employment',
   'legal-framework',
   'Washington public-employment / civil-service / pensions — RCW Title 41. Includes State Civil Service Law (41.06) and PERS (41.40).'),
  ('wa-courts',
   'WA Courts',
   'legal-framework',
   'Washington courts of record — RCW Title 2. Includes Supreme Court rule-making power (2.04) — the statutory authority under which GR 31 / GR 31.1 are promulgated.'),
  ('wa-constitution',
   'WA Constitution',
   'legal-framework',
   'Marker for sections that cross-reference the Washington State Constitution. (The Constitution itself is Phase 8 backlog.)'),
  ('cpa-wa',
   'WA Consumer Protection Act',
   'legal-framework',
   'Washington Consumer Protection Act — RCW chapter 19.86. Pleads in tandem with WLAD and consumer-facing PRA refusals.'),
  ('writs-wa',
   'WA Writs',
   'legal-framework',
   'Washington writs (mandamus, prohibition, certiorari) — RCW chapter 7.16. Relevant to records-access enforcement and judicial-review compulsion.'),
  ('child-abuse-wa',
   'WA Child Abuse Reporting',
   'legal-framework',
   'Washington child-abuse mandatory-reporting framework — RCW chapter 26.44. Including 26.44.030 reporting duty and 26.44.186/.188 records access.'),
  ('gal-wa',
   'WA GAL Framework',
   'legal-framework',
   'Washington Guardian ad Litem framework — cuts across RCW Titles 11 (probate), 26 (domestic relations), 26.44 (child abuse).')
ON CONFLICT (slug) DO NOTHING;


-- ---------------------------------------------------------------------------
-- §3d  rcw-title category — 100 title rows
-- ---------------------------------------------------------------------------
-- Generated from the master title list at app.leg.wa.gov/RCW/default.aspx
-- (fetched 2026-04-28). Title-name punctuation preserved as published; em-dashes
-- (—) verbatim from source.
INSERT INTO core.tags (slug, name, category, description) VALUES
  ('rcw-title-1',     'RCW Title 1 — General Provisions',                                            'rcw-title', 'RCW Title 1: GENERAL PROVISIONS'),
  ('rcw-title-2',     'RCW Title 2 — Courts of Record',                                              'rcw-title', 'RCW Title 2: COURTS OF RECORD'),
  ('rcw-title-3',     'RCW Title 3 — District Courts—Courts of Limited Jurisdiction',                'rcw-title', 'RCW Title 3: DISTRICT COURTS—COURTS OF LIMITED JURISDICTION'),
  ('rcw-title-4',     'RCW Title 4 — Civil Procedure',                                               'rcw-title', 'RCW Title 4: CIVIL PROCEDURE'),
  ('rcw-title-5',     'RCW Title 5 — Evidence',                                                       'rcw-title', 'RCW Title 5: EVIDENCE'),
  ('rcw-title-6',     'RCW Title 6 — Enforcement of Judgments',                                       'rcw-title', 'RCW Title 6: ENFORCEMENT OF JUDGMENTS'),
  ('rcw-title-7',     'RCW Title 7 — Special Proceedings and Actions',                                'rcw-title', 'RCW Title 7: SPECIAL PROCEEDINGS AND ACTIONS'),
  ('rcw-title-8',     'RCW Title 8 — Eminent Domain',                                                 'rcw-title', 'RCW Title 8: EMINENT DOMAIN'),
  ('rcw-title-9',     'RCW Title 9 — Crimes and Punishments',                                         'rcw-title', 'RCW Title 9: CRIMES AND PUNISHMENTS'),
  ('rcw-title-9a',    'RCW Title 9A — Washington Criminal Code',                                      'rcw-title', 'RCW Title 9A: WASHINGTON CRIMINAL CODE'),
  ('rcw-title-10',    'RCW Title 10 — Criminal Procedure',                                            'rcw-title', 'RCW Title 10: CRIMINAL PROCEDURE'),
  ('rcw-title-11',    'RCW Title 11 — Probate and Trust Law',                                         'rcw-title', 'RCW Title 11: PROBATE AND TRUST LAW'),
  ('rcw-title-12',    'RCW Title 12 — District Courts—Civil Procedure',                               'rcw-title', 'RCW Title 12: DISTRICT COURTS—CIVIL PROCEDURE'),
  ('rcw-title-13',    'RCW Title 13 — Juvenile Courts and Juvenile Offenders',                        'rcw-title', 'RCW Title 13: JUVENILE COURTS AND JUVENILE OFFENDERS'),
  ('rcw-title-14',    'RCW Title 14 — Aeronautics',                                                   'rcw-title', 'RCW Title 14: AERONAUTICS'),
  ('rcw-title-15',    'RCW Title 15 — Agriculture and Marketing',                                     'rcw-title', 'RCW Title 15: AGRICULTURE AND MARKETING'),
  ('rcw-title-16',    'RCW Title 16 — Animals and Livestock',                                         'rcw-title', 'RCW Title 16: ANIMALS AND LIVESTOCK'),
  ('rcw-title-17',    'RCW Title 17 — Weeds, Rodents, and Pests',                                     'rcw-title', 'RCW Title 17: WEEDS, RODENTS, AND PESTS'),
  ('rcw-title-18',    'RCW Title 18 — Businesses and Professions',                                    'rcw-title', 'RCW Title 18: BUSINESSES AND PROFESSIONS'),
  ('rcw-title-19',    'RCW Title 19 — Business Regulations—Miscellaneous',                            'rcw-title', 'RCW Title 19: BUSINESS REGULATIONS—MISCELLANEOUS'),
  ('rcw-title-20',    'RCW Title 20 — Commission Merchants—Agricultural Products',                    'rcw-title', 'RCW Title 20: COMMISSION MERCHANTS—AGRICULTURAL PRODUCTS'),
  ('rcw-title-21',    'RCW Title 21 — Securities and Investments',                                    'rcw-title', 'RCW Title 21: SECURITIES AND INVESTMENTS'),
  ('rcw-title-22',    'RCW Title 22 — Warehousing and Deposits',                                      'rcw-title', 'RCW Title 22: WAREHOUSING AND DEPOSITS'),
  ('rcw-title-23',    'RCW Title 23 — Corporations and Associations (Profit)',                        'rcw-title', 'RCW Title 23: CORPORATIONS AND ASSOCIATIONS (PROFIT) (Business Corporation Act: See Title 23B RCW)'),
  ('rcw-title-23b',   'RCW Title 23B — Washington Business Corporation Act',                          'rcw-title', 'RCW Title 23B: WASHINGTON BUSINESS CORPORATION ACT'),
  ('rcw-title-24',    'RCW Title 24 — Corporations and Associations (Nonprofit)',                     'rcw-title', 'RCW Title 24: CORPORATIONS AND ASSOCIATIONS (NONPROFIT)'),
  ('rcw-title-25',    'RCW Title 25 — Partnerships',                                                  'rcw-title', 'RCW Title 25: PARTNERSHIPS'),
  ('rcw-title-26',    'RCW Title 26 — Domestic Relations',                                            'rcw-title', 'RCW Title 26: DOMESTIC RELATIONS'),
  ('rcw-title-27',    'RCW Title 27 — Libraries, Museums, and Historical Activities',                 'rcw-title', 'RCW Title 27: LIBRARIES, MUSEUMS, AND HISTORICAL ACTIVITIES'),
  ('rcw-title-28a',   'RCW Title 28A — Common School Provisions',                                     'rcw-title', 'RCW Title 28A: COMMON SCHOOL PROVISIONS'),
  ('rcw-title-28b',   'RCW Title 28B — Higher Education',                                             'rcw-title', 'RCW Title 28B: HIGHER EDUCATION'),
  ('rcw-title-28c',   'RCW Title 28C — Vocational Education',                                         'rcw-title', 'RCW Title 28C: VOCATIONAL EDUCATION'),
  ('rcw-title-29a',   'RCW Title 29A — Elections',                                                    'rcw-title', 'RCW Title 29A: ELECTIONS'),
  ('rcw-title-29b',   'RCW Title 29B — Campaign Disclosure and Contribution',                         'rcw-title', 'RCW Title 29B: CAMPAIGN DISCLOSURE AND CONTRIBUTION'),
  ('rcw-title-30a',   'RCW Title 30A — Washington Commercial Bank Act',                               'rcw-title', 'RCW Title 30A: WASHINGTON COMMERCIAL BANK ACT'),
  ('rcw-title-30b',   'RCW Title 30B — Washington Trust Institutions Act',                            'rcw-title', 'RCW Title 30B: WASHINGTON TRUST INSTITUTIONS ACT'),
  ('rcw-title-31',    'RCW Title 31 — Miscellaneous Loan Agencies',                                   'rcw-title', 'RCW Title 31: MISCELLANEOUS LOAN AGENCIES'),
  ('rcw-title-32',    'RCW Title 32 — Washington Savings Bank Act',                                   'rcw-title', 'RCW Title 32: WASHINGTON SAVINGS BANK ACT'),
  ('rcw-title-33',    'RCW Title 33 — Washington Savings Association Act',                            'rcw-title', 'RCW Title 33: WASHINGTON SAVINGS ASSOCIATION ACT'),
  ('rcw-title-34',    'RCW Title 34 — Administrative Law',                                            'rcw-title', 'RCW Title 34: ADMINISTRATIVE LAW'),
  ('rcw-title-35',    'RCW Title 35 — Cities and Towns',                                              'rcw-title', 'RCW Title 35: CITIES AND TOWNS'),
  ('rcw-title-35a',   'RCW Title 35A — Optional Municipal Code',                                      'rcw-title', 'RCW Title 35A: OPTIONAL MUNICIPAL CODE'),
  ('rcw-title-36',    'RCW Title 36 — Counties',                                                      'rcw-title', 'RCW Title 36: COUNTIES'),
  ('rcw-title-37',    'RCW Title 37 — Federal Areas—Indians',                                         'rcw-title', 'RCW Title 37: FEDERAL AREAS—INDIANS'),
  ('rcw-title-38',    'RCW Title 38 — Militia and Military Affairs',                                  'rcw-title', 'RCW Title 38: MILITIA AND MILITARY AFFAIRS'),
  ('rcw-title-39',    'RCW Title 39 — Public Contracts and Indebtedness',                             'rcw-title', 'RCW Title 39: PUBLIC CONTRACTS AND INDEBTEDNESS'),
  ('rcw-title-40',    'RCW Title 40 — Public Documents, Records, and Publications',                   'rcw-title', 'RCW Title 40: PUBLIC DOCUMENTS, RECORDS, AND PUBLICATIONS'),
  ('rcw-title-41',    'RCW Title 41 — Public Employment, Civil Service, and Pensions',                'rcw-title', 'RCW Title 41: PUBLIC EMPLOYMENT, CIVIL SERVICE, AND PENSIONS'),
  ('rcw-title-42',    'RCW Title 42 — Public Officers and Agencies',                                  'rcw-title', 'RCW Title 42: PUBLIC OFFICERS AND AGENCIES'),
  ('rcw-title-43',    'RCW Title 43 — State Government—Executive',                                    'rcw-title', 'RCW Title 43: STATE GOVERNMENT—EXECUTIVE'),
  ('rcw-title-44',    'RCW Title 44 — State Government—Legislative',                                  'rcw-title', 'RCW Title 44: STATE GOVERNMENT—LEGISLATIVE'),
  ('rcw-title-46',    'RCW Title 46 — Motor Vehicles',                                                'rcw-title', 'RCW Title 46: MOTOR VEHICLES'),
  ('rcw-title-47',    'RCW Title 47 — Public Highways and Transportation',                            'rcw-title', 'RCW Title 47: PUBLIC HIGHWAYS AND TRANSPORTATION'),
  ('rcw-title-48',    'RCW Title 48 — Insurance',                                                     'rcw-title', 'RCW Title 48: INSURANCE'),
  ('rcw-title-49',    'RCW Title 49 — Labor Regulations',                                             'rcw-title', 'RCW Title 49: LABOR REGULATIONS'),
  ('rcw-title-50',    'RCW Title 50 — Unemployment Compensation',                                     'rcw-title', 'RCW Title 50: UNEMPLOYMENT COMPENSATION'),
  ('rcw-title-50a',   'RCW Title 50A — Family and Medical Leave',                                     'rcw-title', 'RCW Title 50A: FAMILY AND MEDICAL LEAVE'),
  ('rcw-title-50b',   'RCW Title 50B — Long-Term Care',                                               'rcw-title', 'RCW Title 50B: LONG-TERM CARE'),
  ('rcw-title-51',    'RCW Title 51 — Industrial Insurance',                                          'rcw-title', 'RCW Title 51: INDUSTRIAL INSURANCE'),
  ('rcw-title-52',    'RCW Title 52 — Fire Protection Districts',                                     'rcw-title', 'RCW Title 52: FIRE PROTECTION DISTRICTS'),
  ('rcw-title-53',    'RCW Title 53 — Port Districts',                                                'rcw-title', 'RCW Title 53: PORT DISTRICTS'),
  ('rcw-title-54',    'RCW Title 54 — Public Utility Districts',                                      'rcw-title', 'RCW Title 54: PUBLIC UTILITY DISTRICTS'),
  ('rcw-title-55',    'RCW Title 55 — Sanitary Districts',                                            'rcw-title', 'RCW Title 55: SANITARY DISTRICTS'),
  ('rcw-title-57',    'RCW Title 57 — Water-Sewer Districts',                                         'rcw-title', 'RCW Title 57: WATER-SEWER DISTRICTS'),
  ('rcw-title-58',    'RCW Title 58 — Boundaries and Plats',                                          'rcw-title', 'RCW Title 58: BOUNDARIES AND PLATS'),
  ('rcw-title-59',    'RCW Title 59 — Landlord and Tenant',                                           'rcw-title', 'RCW Title 59: LANDLORD AND TENANT'),
  ('rcw-title-60',    'RCW Title 60 — Liens',                                                         'rcw-title', 'RCW Title 60: LIENS'),
  ('rcw-title-61',    'RCW Title 61 — Mortgages, Deeds of Trust, and Real Estate Contracts',          'rcw-title', 'RCW Title 61: MORTGAGES, DEEDS OF TRUST, AND REAL ESTATE CONTRACTS'),
  ('rcw-title-62a',   'RCW Title 62A — Uniform Commercial Code',                                      'rcw-title', 'RCW Title 62A: UNIFORM COMMERCIAL CODE'),
  ('rcw-title-63',    'RCW Title 63 — Personal Property',                                             'rcw-title', 'RCW Title 63: PERSONAL PROPERTY'),
  ('rcw-title-64',    'RCW Title 64 — Real Property and Conveyances',                                 'rcw-title', 'RCW Title 64: REAL PROPERTY AND CONVEYANCES'),
  ('rcw-title-65',    'RCW Title 65 — Recording, Registration, and Legal Publication',                'rcw-title', 'RCW Title 65: RECORDING, REGISTRATION, AND LEGAL PUBLICATION'),
  ('rcw-title-66',    'RCW Title 66 — Alcoholic Beverage Control',                                    'rcw-title', 'RCW Title 66: ALCOHOLIC BEVERAGE CONTROL'),
  ('rcw-title-67',    'RCW Title 67 — Sports and Recreation—Convention Facilities',                   'rcw-title', 'RCW Title 67: SPORTS AND RECREATION—CONVENTION FACILITIES'),
  ('rcw-title-68',    'RCW Title 68 — Cemeteries, Morgues, and Human Remains',                        'rcw-title', 'RCW Title 68: CEMETERIES, MORGUES, AND HUMAN REMAINS'),
  ('rcw-title-69',    'RCW Title 69 — Food, Drugs, Cosmetics, and Poisons',                           'rcw-title', 'RCW Title 69: FOOD, DRUGS, COSMETICS, AND POISONS'),
  ('rcw-title-70',    'RCW Title 70 — Public Health and Safety',                                      'rcw-title', 'RCW Title 70: PUBLIC HEALTH AND SAFETY'),
  ('rcw-title-70a',   'RCW Title 70A — Environmental Health and Safety',                              'rcw-title', 'RCW Title 70A: ENVIRONMENTAL HEALTH AND SAFETY'),
  ('rcw-title-71',    'RCW Title 71 — Behavioral Health',                                             'rcw-title', 'RCW Title 71: BEHAVIORAL HEALTH'),
  ('rcw-title-71a',   'RCW Title 71A — Developmental Disabilities',                                   'rcw-title', 'RCW Title 71A: DEVELOPMENTAL DISABILITIES'),
  ('rcw-title-72',    'RCW Title 72 — State Institutions',                                            'rcw-title', 'RCW Title 72: STATE INSTITUTIONS'),
  ('rcw-title-73',    'RCW Title 73 — Veterans and Veterans'' Affairs',                               'rcw-title', 'RCW Title 73: VETERANS AND VETERANS'' AFFAIRS'),
  ('rcw-title-74',    'RCW Title 74 — Public Assistance',                                             'rcw-title', 'RCW Title 74: PUBLIC ASSISTANCE'),
  ('rcw-title-76',    'RCW Title 76 — Forests and Forest Products',                                   'rcw-title', 'RCW Title 76: FORESTS AND FOREST PRODUCTS'),
  ('rcw-title-77',    'RCW Title 77 — Fish and Wildlife',                                             'rcw-title', 'RCW Title 77: FISH AND WILDLIFE'),
  ('rcw-title-78',    'RCW Title 78 — Mines, Minerals, and Petroleum',                                'rcw-title', 'RCW Title 78: MINES, MINERALS, AND PETROLEUM'),
  ('rcw-title-79',    'RCW Title 79 — Public Lands',                                                  'rcw-title', 'RCW Title 79: PUBLIC LANDS'),
  ('rcw-title-79a',   'RCW Title 79A — Public Recreational Lands',                                    'rcw-title', 'RCW Title 79A: PUBLIC RECREATIONAL LANDS'),
  ('rcw-title-80',    'RCW Title 80 — Public Utilities',                                              'rcw-title', 'RCW Title 80: PUBLIC UTILITIES'),
  ('rcw-title-81',    'RCW Title 81 — Transportation',                                                'rcw-title', 'RCW Title 81: TRANSPORTATION'),
  ('rcw-title-82',    'RCW Title 82 — Excise Taxes',                                                  'rcw-title', 'RCW Title 82: EXCISE TAXES'),
  ('rcw-title-83',    'RCW Title 83 — Estate Taxation',                                               'rcw-title', 'RCW Title 83: ESTATE TAXATION'),
  ('rcw-title-84',    'RCW Title 84 — Property Taxes',                                                'rcw-title', 'RCW Title 84: PROPERTY TAXES'),
  ('rcw-title-85',    'RCW Title 85 — Diking and Drainage',                                           'rcw-title', 'RCW Title 85: DIKING AND DRAINAGE'),
  ('rcw-title-86',    'RCW Title 86 — Flood Control',                                                 'rcw-title', 'RCW Title 86: FLOOD CONTROL'),
  ('rcw-title-87',    'RCW Title 87 — Irrigation',                                                    'rcw-title', 'RCW Title 87: IRRIGATION'),
  ('rcw-title-88',    'RCW Title 88 — Navigation and Harbor Improvements',                            'rcw-title', 'RCW Title 88: NAVIGATION AND HARBOR IMPROVEMENTS'),
  ('rcw-title-89',    'RCW Title 89 — Reclamation, Soil Conservation, and Land Settlement',           'rcw-title', 'RCW Title 89: RECLAMATION, SOIL CONSERVATION, AND LAND SETTLEMENT'),
  ('rcw-title-90',    'RCW Title 90 — Water Rights—Environment',                                      'rcw-title', 'RCW Title 90: WATER RIGHTS—ENVIRONMENT'),
  ('rcw-title-91',    'RCW Title 91 — Waterways',                                                     'rcw-title', 'RCW Title 91: WATERWAYS')
ON CONFLICT (slug) DO NOTHING;
-- Expected: 100 rows inserted on first apply, 0 on subsequent re-runs.


-- =============================================================================
-- §4  RETROACTIVE TAGGING — apply `code-jurisdiction:federal-usc` to all 11,678
--                            existing USC documents
-- =============================================================================
-- Anchor: every USC doc in core.documents currently carries the existing
-- `usc-meta:usc` tag (id=505, count=11,678 verified §0/§Pre-flight). We use
-- that as the predicate selector. ON CONFLICT against the (document_id, tag_id)
-- primary key makes this idempotent.
INSERT INTO core.document_tags (document_id, tag_id)
SELECT
  dt.document_id,
  (SELECT id FROM core.tags
    WHERE category='code-jurisdiction' AND slug='federal-usc') AS tag_id
FROM core.document_tags dt
JOIN core.tags t ON t.id = dt.tag_id
WHERE t.category='usc-meta' AND t.slug='usc'
ON CONFLICT (document_id, tag_id) DO NOTHING;
-- Expected on first apply: ~11,678 rows inserted.
-- Expected on re-run: 0 rows inserted (all already there).


-- =============================================================================
-- §5  POST-APPLY VERIFICATION QUERIES
-- =============================================================================
-- Operator runs these after applying §1-§4 to confirm correct state.

-- §5.1 rcw schema and all 7 tables exist
SELECT
  EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='rcw') AS rcw_schema,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='titles')           AS titles_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='chapters')         AS chapters_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='sections')         AS sections_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='cross_references') AS xref_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='section_history')  AS history_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='section_notes')    AS notes_tbl,
  EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='rcw' AND table_name='ingestion_runs')   AS runs_tbl;

-- §5.2 Tag pre-seeds inserted
SELECT
  (SELECT COUNT(*) FROM core.tags WHERE category='code-jurisdiction')   AS code_jurisdiction_n,    -- expect 2
  (SELECT COUNT(*) FROM core.tags WHERE category='rcw-meta')            AS rcw_meta_n,             -- expect 8
  (SELECT COUNT(*) FROM core.tags WHERE category='rcw-title')           AS rcw_title_n,            -- expect 100
  (SELECT COUNT(*) FROM core.tags WHERE category='legal-framework')     AS legal_framework_total;  -- expect 28 (existing) + 17 (new) = 45

-- §5.3 USC retroactive tagging worked
SELECT COUNT(*) AS usc_docs_with_code_jurisdiction
FROM core.document_tags dt
JOIN core.tags t ON t.id = dt.tag_id
WHERE t.category='code-jurisdiction' AND t.slug='federal-usc';
-- Expect: 11,678

-- §5.4 The CHECK constraint actually rejects bad rows.
-- DO NOT RUN THIS in apply session — it's documented for manual verification:
--   INSERT INTO rcw.sections (document_id, title_number, section_number,
--      canonical_citation, section_heading, full_text, publication_date)
--   VALUES (gen_random_uuid()::uuid, '4', '04.010', 'RCW 4.04.010',
--           'Test', NULL, '2025-08-15');
-- ↑ should fail with: "new row for relation 'sections' violates check constraint
--                      'chk_section_text_status_invariant'"


-- =============================================================================
-- §6  code_jurisdiction_filter SQL COMPOSITION PATTERN (canary_lib reference)
-- =============================================================================
-- Per cross_jurisdiction_design.md §3.1, canary_lib.hybrid_rrf gains a
-- `code_jurisdiction_filter` kwarg (renamed from the original draft per
-- operator architectural correction). Default None = both jurisdictions.
--
-- Implementation note: because tags live in the join table
-- core.document_tags (NOT in a `tags` array column on core.documents — see
-- Pre-flight Finding §2 above), the SQL composition uses an EXISTS clause,
-- not the `tags && ARRAY[…]` shape used in many other systems.
--
-- Canonical SQL injection point inside hybrid_rrf:
--
--     -- single-jurisdiction
--     AND EXISTS (
--       SELECT 1 FROM core.document_tags dt
--       JOIN core.tags t ON t.id = dt.tag_id
--       WHERE dt.document_id = sc.document_id
--         AND t.category = 'code-jurisdiction'
--         AND t.slug = $jurisdiction_filter
--     )
--
--     -- multi-jurisdiction (or default-None expanded explicitly)
--     AND EXISTS (
--       SELECT 1 FROM core.document_tags dt
--       JOIN core.tags t ON t.id = dt.tag_id
--       WHERE dt.document_id = sc.document_id
--         AND t.category = 'code-jurisdiction'
--         AND t.slug = ANY($jurisdiction_filter::text[])
--     )
--
-- Where `sc` is the alias for core.searchable_chunks in the outer SELECT.
--
-- Default-None (no filter): omit the EXISTS clause entirely. (Cheaper than
-- expanding to ALL jurisdictions because RRF then doesn't have to evaluate
-- the join for every candidate.)
--
-- Pattern composes with existing tag_filter (which uses an analogous EXISTS
-- against `t.slug = ANY(tag_filter)`) by AND-ing two EXISTS clauses, OR by
-- combining into one EXISTS with two tag-category subselects. The simpler
-- form (two EXISTS) is preferable for the planner — categories partition
-- the tag namespace cleanly, so the planner can treat each EXISTS as a
-- semi-join with a small tag-id set.
--
-- See cross_jurisdiction_design.md §3 for full kwarg semantics. See §4.2
-- of that document for the three-test cross-jurisdiction canary spec
-- (Test 1 disability, Test 2 records-access FOIA/PRA parallel, Test 3
-- recodification chain — including the new third test added by operator).


-- =============================================================================
-- END OF schema_design.sql
-- =============================================================================
-- Submit to operator for review. Do NOT apply until explicit go-ahead.
