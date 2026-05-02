-- ============================================================================
--  USC Full Ingestion — Schema Design (Phase 1 Deliverable)
-- ============================================================================
--  Author      : USC ingestion agent
--  Date        : 2026-04-28
--  Target DB   : wdws (PostgreSQL 17 + pgvector + halfvec)
--  Status      : DRAFT — DDL is NOT to be applied until operator sign-off.
--
--  Phase 0 → Phase 1 deltas this design incorporates (vs the original
--  instruction document in §2):
--    • Embedding column is halfvec(1024), not vector(1024).
--    • Tags are normalized into core.tags + core.document_tags. The Phase-1
--      doc's `tags array` field is REMOVED from usc.sections; tag attachment
--      goes through core.document_tags inserts during ingestion.
--    • Existing core.tags 'legal-framework' category is the home for USC
--      *topical* tags (joins existing 'ADA' tag). New structural categories
--      'usc-meta', 'usc-title', 'usc-chapter' are added because no equivalent
--      exists. NO parallel 'usc-topic' category created.
--    • core.documents.source_path needs a partial unique index scoped to
--      USC documents — added below.
--    • halfvec cast happens in the INSERT statement ($1::halfvec(1024)) NOT
--      in Python — avoids float32 → fp16 double-rounding by going directly
--      from the BGE-M3 float32 numpy array into Postgres.
--    • Governance defaults for USC documents are spelled out (§6 below) and
--      verified against the live core_safety.build_document_safety_filter
--      logic in /opt/wdws/core_safety.py:485-538.
--    • Release point identifier corrected to PL 119-84 (2026-04-18).
-- ============================================================================


-- ============================================================================
-- §1.  Pre-flight: required extensions
-- ============================================================================
-- Verify (do not blindly CREATE) — both should already be present on a wdws
-- instance that hosts core.document_chunks.embedding halfvec(1024).
-- The pgcrypto requirement of legacy gen_random_uuid() is satisfied since
-- PG13 by the built-in `pg_catalog.gen_random_uuid()`; no extension needed.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    RAISE EXCEPTION 'pgvector extension is required on database wdws';
  END IF;
END$$;


-- ============================================================================
-- §2.  Schema
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS usc AUTHORIZATION wdws;
COMMENT ON SCHEMA usc IS
  'United States Code structural metadata — sections, hierarchy, cross-refs, '
  'amendment history, and notes. Section bodies are stored in core.documents '
  'and chunked into core.document_chunks like every other corpus item; this '
  'schema captures the legal structure that does not fit cleanly in JSONB.';


-- ============================================================================
-- §3.  Reference tables
-- ============================================================================

-- §3.1 Titles — 54 rows once seeded; positive-law flag must be hand-curated
--      because USLM does not encode it. is_positive_law list verified against
--      OLRC "Positive Law Codification" page as of 2026-04.
CREATE TABLE usc.titles (
    title_number      SMALLINT      PRIMARY KEY,
    title_name        TEXT          NOT NULL,
    is_positive_law   BOOLEAN       NOT NULL,
    has_appendix      BOOLEAN       NOT NULL DEFAULT FALSE,
    release_point     TEXT          NOT NULL,    -- e.g. 'PL 119-84'
    current_through   DATE          NOT NULL,    -- per OLRC release-notes date
    last_ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    section_count     INTEGER,                   -- post-ingestion populated
    notes             TEXT
);
COMMENT ON COLUMN usc.titles.is_positive_law IS
  'True for titles enacted into positive law (1, 3, 4, 5, 9, 10, 11, 13, 14, '
  '17, 18, 23, 28, 31, 32, 35, 36, 37, 38, 39, 40, 41, 44, 46, 49, 51, 54). '
  'False for titles that are prima facie evidence only — citations in '
  'pleadings may need to fall back to the underlying Public Law text when '
  'the Code text and the enacted text diverge.';


-- §3.2 Chapters — title 42 has chapters numbered "126", "126A" so number is text.
CREATE TABLE usc.chapters (
    id                UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    title_number      SMALLINT      NOT NULL REFERENCES usc.titles(title_number),
    chapter_number    TEXT          NOT NULL,
    chapter_heading   TEXT          NOT NULL,
    sort_order        INTEGER       NOT NULL,
    UNIQUE (title_number, chapter_number)
);


-- §3.3 Subchapters
CREATE TABLE usc.subchapters (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    chapter_id          UUID        NOT NULL REFERENCES usc.chapters(id) ON DELETE CASCADE,
    subchapter_id_text  TEXT        NOT NULL,    -- 'I', 'II', 'III', 'A', etc.
    subchapter_heading  TEXT        NOT NULL,
    sort_order          INTEGER     NOT NULL,
    UNIQUE (chapter_id, subchapter_id_text)
);


-- ============================================================================
-- §4.  Sections — the citation unit
-- ============================================================================
-- Idempotency is keyed on (canonical_citation, release_point). The same
-- canonical citation (e.g. '42 U.S.C. § 12101') will get a NEW row when a
-- new release point ships and the text changed — preserving prior versions
-- for retrospective litigation per Phase 7.2.
--
-- Fields rationale:
--   document_id       — 1:1 with core.documents row holding the chunkable
--                       text + metadata + governance fields.
--   source_xml        — original USLM <section> XML preserved for re-parse.
--                       Stored here, not in core.documents.storage_uri,
--                       because it's structured data subordinate to the
--                       parsed representation.
--   transferred_to    — populated when status='transferred'; canonical
--                       citation of the destination, parsed from the
--                       editorial note. Cross-reference resolution in
--                       Phase 4.4 may also link target_section_id from
--                       cross_references onto the destination.
--   created_at /
--   updated_at        — Audit timestamps. core.documents has its own
--                       created_at/updated_at, but joining cross-DB during
--                       Phase 7 differential-update / versioning queries
--                       defeats the purpose of having these here too.
--                       Both default NOW() at INSERT; updated_at must be
--                       maintained by ingestion code (no trigger in Phase 1
--                       — keep behavior explicit).
--
-- NO `tags` column — tags live in core.document_tags joined on document_id.
--
-- ─────────────────────────────────────────────────────────────────────────
-- BOOLEAN ↔ tag sync contract (status flags ↔ usc-* tags)
-- ─────────────────────────────────────────────────────────────────────────
-- Two parallel representations of the same status MUST stay in sync:
--
--   Column                              Tag (slug, in usc-meta category)
--   ─────────────────────────────       ──────────────────────────────
--   usc.sections.is_repealed=TRUE   ↔   usc-repealed  (attached to document_id)
--   usc.sections.is_transferred=TRUE ↔  usc-transferred
--   usc.sections.is_omitted=TRUE    ↔   usc-omitted
--
-- Drift between the BOOLEAN and the tag attachment creates retrieval bugs:
-- an SQL filter on `WHERE is_repealed=TRUE` may include a section whose
-- `usc-repealed` tag was never attached, and a tag-based retrieval will
-- silently miss it (or vice versa).
--
-- CONTRACT: Phase 4 ingestion code is the SOLE writer of these flags. Any
-- transition that sets a status BOOLEAN to TRUE (or back to FALSE) MUST,
-- in the SAME transaction, attach (or detach) the corresponding `usc-{status}`
-- tag on the parent document_id via core.document_tags.
--
-- A future DB-level trigger MAY enforce this contract automatically. For
-- Phase 1 the contract lives in ingestion code only — the DDL does not
-- enforce it. If you discover drift in Phase 5 validation, the recovery
-- query is documented in the validation suite (§6.1 of the instruction doc).
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE usc.sections (
    id                    UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id           UUID         NOT NULL UNIQUE
                                       REFERENCES core.documents(id) ON DELETE CASCADE,
    title_number          SMALLINT     NOT NULL REFERENCES usc.titles(title_number),
    chapter_id            UUID                  REFERENCES usc.chapters(id),
    subchapter_id         UUID                  REFERENCES usc.subchapters(id),
    section_number        TEXT         NOT NULL,    -- '12101', '12101a', '12101-1'
    section_heading       TEXT         NOT NULL,
    canonical_citation    TEXT         NOT NULL,    -- '42 U.S.C. § 12101'
    is_repealed           BOOLEAN      NOT NULL DEFAULT FALSE,
    is_transferred        BOOLEAN      NOT NULL DEFAULT FALSE,
    is_omitted            BOOLEAN      NOT NULL DEFAULT FALSE,
    transferred_to        TEXT,
    effective_date        DATE,
    enacted_by_pl         TEXT,                       -- 'Pub. L. 101-336'
    statutes_at_large_ref TEXT,                       -- '104 Stat. 327'
    full_text             TEXT         NOT NULL,      -- normalized markdown (also lives in core.documents.full_content)
    source_xml            TEXT         NOT NULL,      -- raw USLM <section> XML
    release_point         TEXT         NOT NULL,
    created_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (canonical_citation, release_point)
);

CREATE INDEX idx_sections_title       ON usc.sections(title_number);
CREATE INDEX idx_sections_chapter     ON usc.sections(chapter_id);
CREATE INDEX idx_sections_citation    ON usc.sections(canonical_citation);
CREATE INDEX idx_sections_release     ON usc.sections(release_point);
CREATE INDEX idx_sections_text_fts    ON usc.sections USING gin(to_tsvector('english', full_text));
-- Partial indexes for status flags (cheap targeted queries)
CREATE INDEX idx_sections_repealed    ON usc.sections(canonical_citation) WHERE is_repealed = TRUE;
CREATE INDEX idx_sections_transferred ON usc.sections(canonical_citation) WHERE is_transferred = TRUE;
CREATE INDEX idx_sections_omitted     ON usc.sections(canonical_citation) WHERE is_omitted = TRUE;
-- Phase 7 versioning queries: WHERE effective_date <= '<conduct-date>' ORDER BY effective_date DESC
CREATE INDEX idx_sections_effective_date ON usc.sections(effective_date) WHERE effective_date IS NOT NULL;


-- ============================================================================
-- §5.  Supporting tables
-- ============================================================================

-- §5.1 Cross-references (intra-USC + outbound)
CREATE TABLE usc.cross_references (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    source_section_id   UUID         NOT NULL REFERENCES usc.sections(id) ON DELETE CASCADE,
    target_citation     TEXT         NOT NULL,                -- '42 U.S.C. § 12102'
    target_section_id   UUID                  REFERENCES usc.sections(id),  -- resolved in Phase 4.4
    context_snippet     TEXT,                                  -- ~200 chars surrounding the citation
    ref_type            TEXT         NOT NULL,                -- 'internal','internal-unresolved','cfr','public-law','statutes-at-large','external'
    uslm_href           TEXT                                  -- raw @href from <ref> for traceability
);
CREATE INDEX idx_xref_source ON usc.cross_references(source_section_id);
CREATE INDEX idx_xref_target ON usc.cross_references(target_citation);


-- §5.2 Source-credit / amendment history (one row per Pub. L. segment in <sourceCredit>)
CREATE TABLE usc.section_history (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id      UUID         NOT NULL REFERENCES usc.sections(id) ON DELETE CASCADE,
    pub_law         TEXT         NOT NULL,                  -- 'Pub. L. 110-325'
    pub_law_date    DATE,
    statutes_ref    TEXT,                                    -- '122 Stat. 3553'
    action_type     TEXT,                                    -- 'enacted','amended','renumbered','repealed','transferred','redesignated','reenacted'
    action_summary  TEXT,
    sort_order      INTEGER      NOT NULL
);
CREATE INDEX idx_history_section ON usc.section_history(section_id);
CREATE INDEX idx_history_pub_law ON usc.section_history(pub_law);


-- §5.3 Notes (statutory, editorial, effective-date, short-title, etc.)
-- One row per <statutoryNote>, <editorialNote>, <changeNote>, etc.
CREATE TABLE usc.section_notes (
    id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    section_id   UUID         NOT NULL REFERENCES usc.sections(id) ON DELETE CASCADE,
    note_type    TEXT         NOT NULL,        -- 'statutory','editorial','change','source_credit','generic'
    note_topic   TEXT,                          -- USLM @topic: 'Effective Date','Short Title','References in Text','Codification','Repeal','Construction','Transfer of Functions'
    note_heading TEXT,
    note_text    TEXT         NOT NULL,
    sort_order   INTEGER      NOT NULL
);
CREATE INDEX idx_notes_section ON usc.section_notes(section_id);
CREATE INDEX idx_notes_topic   ON usc.section_notes(note_topic);


-- §5.4 Ingestion run manifest
CREATE TABLE usc.ingestion_runs (
    id                  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    release_point       TEXT         NOT NULL,
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    titles_planned      SMALLINT,
    titles_completed    SMALLINT,
    sections_ingested   INTEGER,
    chunks_ingested     INTEGER,
    embeddings_completed INTEGER,
    status              TEXT         NOT NULL DEFAULT 'running',
                                       -- CHECK: 'running','completed','failed','partial'
    notes               TEXT,
    CONSTRAINT chk_ingestion_status CHECK (status IN ('running','completed','failed','partial'))
);


-- ============================================================================
-- §6.  Modification to core.documents — partial unique index
-- ============================================================================
-- Rationale: the ingestion path uses
--   INSERT INTO core.documents (...) VALUES (...) ON CONFLICT (source_path) DO UPDATE ...
-- to be re-runnable after partial failures. core.documents.source_path
-- currently has only a non-unique btree index (idx_documents_source), shared
-- across all document types. Per operator direction: scope the new unique
-- constraint to USC documents so it can't collide with email/court_filing/etc.
-- rows that may legitimately share an overlapping source_path namespace.
CREATE UNIQUE INDEX uniq_documents_usc_source
  ON core.documents(source_path)
  WHERE document_type = 'statute';

COMMENT ON INDEX core.uniq_documents_usc_source IS
  'Phase 1 USC ingestion idempotency. Scope: USC sections only — does not '
  'apply to other document_type values. Source_path convention: '
  'usc:{release_point}:t{N}:s{M} (e.g., usc:PL_119-84:t42:s12101).';


-- ============================================================================
-- §7.  Tag taxonomy seeding
-- ============================================================================
-- Per operator direction:
--   * USC topical tags (section-1983, frcp, rehab-act, …) join the existing
--     'legal-framework' category alongside 'ADA'.
--   * 'usc-meta' / 'usc-title' / 'usc-chapter' are NEW structural categories
--     because no equivalent exists in core.tags today.
--   * NO parallel 'usc-topic' category.
--
-- Tag attachment per ingested USC document happens via core.document_tags
-- (document_id, tag_id) — INSERTED at section ingest time once tag IDs are
-- known. The topical tagger (Phase 3.4) is a regex/keyword map operating on
-- section heading + chapter heading + first 500 chars of full_text.
-- An LLM-based second-pass enrichment is deferred per the instruction doc.

-- §7.1 Structural meta tags — apply to every USC section
INSERT INTO core.tags (name, slug, category, description) VALUES
  ('USC',                            'usc',               'usc-meta',  'Document is a section of the United States Code'),
  ('Federal Statute',                'federal-statute',   'usc-meta',  'Federal statutory law'),
  ('USC Appendix',                   'usc-appendix',      'usc-meta',  'Section is part of a USC title appendix (FRCP, FRCrP, FRE, APA, War Powers)'),
  ('Repealed',                       'usc-repealed',      'usc-meta',  'USC section has been repealed; preserved for retrospective litigation'),
  ('Transferred',                    'usc-transferred',   'usc-meta',  'USC section has been transferred to a different citation'),
  ('Omitted',                        'usc-omitted',       'usc-meta',  'USC section number reserved/omitted, no current text'),
  ('Positive Law',                   'usc-positive-law',  'usc-meta',  'Section is in a positive-law title (Code text is binding)'),
  ('Non-Positive Law',               'usc-non-positive',  'usc-meta',  'Section is in a non-positive-law title (Code text is prima facie evidence)')
ON CONFLICT (slug) DO NOTHING;

-- §7.2 Per-title tags (54 rows). Slug 'title-N' for human-readability.
INSERT INTO core.tags (name, slug, category, description)
SELECT format('Title %s', n), format('title-%s', n), 'usc-title',
       format('USC Title %s', n)
FROM generate_series(1, 54) AS n
ON CONFLICT (slug) DO NOTHING;

-- §7.3 Per-chapter tags are inserted lazily during ingestion (one row per
--      (title, chapter) pair encountered). Slug pattern: 'usc-t{N}-c{C}'.
--      Inserted by the parser, not seeded here, because chapter numbers are
--      title-specific text (e.g., '126', '126A', 'I-A') and seeding all
--      possible combinations would be guesswork.

-- §7.4 Topical tags — added to existing 'legal-framework' category.
--      Existing 'ADA' tag is already in this category and stays.
INSERT INTO core.tags (name, slug, category, description) VALUES
  ('Section 1983',         'section-1983',        'legal-framework', '42 U.S.C. § 1983 — civil rights claims under color of state law'),
  ('FRCP',                 'frcp',                'legal-framework', 'Federal Rules of Civil Procedure (Title 28 appendix)'),
  ('FRCrP',                'frcrp',               'legal-framework', 'Federal Rules of Criminal Procedure (Title 18 appendix)'),
  ('FRE',                  'fre',                 'legal-framework', 'Federal Rules of Evidence (Title 28 appendix)'),
  ('FRAP',                 'frap',                'legal-framework', 'Federal Rules of Appellate Procedure (Title 28 appendix)'),
  ('Rehab Act',            'rehab-act',           'legal-framework', 'Rehabilitation Act of 1973 — § 504 / 29 U.S.C. § 794'),
  ('ERISA',                'erisa',               'legal-framework', 'Employee Retirement Income Security Act'),
  ('APA',                  'apa',                 'legal-framework', 'Administrative Procedure Act (Title 5)'),
  ('Due Process',          'due-process',         'legal-framework', 'Constitutional due-process doctrine'),
  ('Equal Protection',     'equal-protection',    'legal-framework', '14th Amendment Equal Protection Clause'),
  ('Qualified Immunity',   'qualified-immunity',  'legal-framework', 'Qualified-immunity defense to § 1983 claims'),
  ('Judicial Review',      'judicial-review',     'legal-framework', 'Judicial review of agency action'),
  ('Bankruptcy',           'bankruptcy',          'legal-framework', 'Federal bankruptcy law (Title 11)'),
  ('IRC',                  'irc',                 'legal-framework', 'Internal Revenue Code (Title 26)'),
  ('OSHA',                 'osha',                'legal-framework', 'Occupational Safety and Health Act'),
  ('FLSA',                 'flsa',                'legal-framework', 'Fair Labor Standards Act'),
  ('Medicare',             'medicare',            'legal-framework', 'Medicare (Title 42 chapter 7 subchapter XVIII)'),
  ('Medicaid',             'medicaid',            'legal-framework', 'Medicaid (Title 42 chapter 7 subchapter XIX)'),
  ('Social Security',      'social-security',     'legal-framework', 'Social Security Act'),
  ('Civil Rights',         'civil-rights',        'legal-framework', 'Federal civil rights statutes'),
  ('Criminal Procedure',   'criminal-procedure',  'legal-framework', 'Federal criminal procedure'),
  ('Evidence',             'evidence',            'legal-framework', 'Evidence law'),
  ('Veterans',             'veterans',            'legal-framework', 'Veterans benefits law (Title 38)'),
  ('Removal',              'removal',             'legal-framework', 'Removal jurisdiction (28 U.S.C. § 1441 et seq.)'),
  ('Jurisdiction',         'jurisdiction-fed',    'legal-framework', 'Federal jurisdictional statutes (Title 28)'),
  ('Full Faith and Credit','full-faith-credit',   'legal-framework', '28 U.S.C. § 1738')
ON CONFLICT (slug) DO NOTHING;

-- 'ada' (existing legal-framework tag, lowercase slug) — referenced as-is.
-- Existing operator-created uncategorized tags (slug=NULL) like 'ada-title-ii',
-- 'federal-supremacy', 'ada-violations', 'ada-anti-surcharge' remain untouched.


-- ============================================================================
-- §8.  Governance defaults for USC documents
-- ============================================================================
-- Every USC section row inserted into core.documents MUST set:
--
--   domain          = 'legal'
--   document_type   = 'statute'
--   classification  = 'public'
--   confidentiality = 'public'
--   privilege       = NULL          -- public-domain federal statute; no privilege applies
--   phi_status      = NULL
--   phi_categories  = '{}'
--   legal_hold      = FALSE
--   minor_patient   = FALSE
--
-- Verified against the live legal-domain safety filter at
-- /opt/wdws/core_safety.py:531-538:
--
--     if domain == 'legal':
--         allowed = sorted(_resolve_legal_allowed(include_privileged, ...))
--         clause = "(d.privilege IS NULL OR d.privilege = ANY($N::text[]))"
--
-- LEGAL_DEFAULT_ALLOWED = frozenset(['none']) (line 82). Anonymous callers
-- (third-party MCP clients) get the bare default; first-party clients
-- (claude-desktop, chatgpt) get LEGAL_DEFAULT_ALLOWED ∪ LEGAL_PRIVILEGED_BASE
-- = {'none','attorney_client','work_product_fact'}. USC rows with
-- privilege=NULL pass the first clause unconditionally regardless of caller —
-- this is the NULL-tolerant property the operator's memory describes.
--
-- We do NOT set privilege='none' (even though it would also pass): NULL
-- semantically means "no privilege classification applicable" whereas 'none'
-- means "classified, no privilege found". For public-domain federal statutes
-- the former is the more accurate signal.


-- ============================================================================
-- §9.  Test query — proves rag_query returns a 'statute' row
-- ============================================================================
-- Two-part verification, run as Phase 1 acceptance immediately after the DDL
-- in this file is applied AND a single sample USC section is ingested. The
-- sample section is 42 U.S.C. § 12101 ("Findings and purpose") because:
--   * It's the keystone ADA section, lexically rich enough to retrieve well.
--   * The operator's active docket prominently features ADA Title II claims.
--   * If the smoke test fails for § 12101, retrieval is broken; we have a
--     loud, fast signal before bulk ingestion.
--
-- ----------------------------------------------------------------------------
-- §9.1  Static SQL simulation of the legal-domain safety filter
-- ----------------------------------------------------------------------------
-- This query mirrors core_safety.build_document_safety_filter exactly for an
-- anonymous third-party caller (the most restrictive case). If this returns
-- the sample row, every caller class will retrieve it.

SELECT d.id,
       d.title,
       d.document_type,
       d.privilege,
       d.classification,
       d.confidentiality
FROM core.documents d
WHERE d.domain = 'legal'
  AND d.document_type = 'statute'
  AND (d.privilege IS NULL OR d.privilege = ANY(ARRAY['none']::text[]))   -- LEGAL_DEFAULT_ALLOWED for anon caller
  AND d.title = '42 U.S.C. § 12101 — Findings and purpose'
LIMIT 1;
-- EXPECTED: one row, with privilege=NULL, classification='public',
-- confidentiality='public'.

-- ----------------------------------------------------------------------------
-- §9.2  Live rag_query end-to-end smoke test
-- ----------------------------------------------------------------------------
-- Run via the MCP after sample ingestion + chunk embedding completes:
--
--   rag_query(
--     question = 'What does the ADA say about findings and purpose?',
--     domain   = 'legal',
--     top_k    = 5
--   )
--
-- ACCEPTANCE: the response's context_chunks array contains at least one
-- chunk whose `source` string starts with '42 U.S.C. § 12101' AND whose
-- chunk metadata `usc_section_id` matches the sample row's usc.sections.id.
-- search_method should report "hybrid_rrf (semantic=0.70, fulltext=0.30, ...)"
-- — confirming both the BGE-M3 vector path AND the english-config full-text
-- path retrieved the row.
--
-- Failure modes this test catches:
--   * Chunk inserted but embedding NULL → semantic miss, hybrid still wins
--     via fulltext = symptom: lower rrf_score, fulltext-only hit.
--   * privilege accidentally set to a non-allowed value → row filtered out
--     entirely → ZERO matches, indistinguishable from "not ingested".
--   * domain set to anything other than 'legal' → fails the domain filter
--     in rag_query.
--
-- ----------------------------------------------------------------------------
-- §9.3  Sample ingest used by §9.1 / §9.2 (illustrative — actual statement
-- runs at ingestion time with parameter binding from the parser):
-- ----------------------------------------------------------------------------
--
-- BEGIN;
--   WITH new_doc AS (
--     INSERT INTO core.documents
--       (domain, document_type, title, source_path, filename,
--        full_content, metadata, classification, confidentiality)
--     VALUES
--       ('legal', 'statute',
--        '42 U.S.C. § 12101 — Findings and purpose',
--        'usc:PL_119-84:t42:s12101',
--        't42-s12101.xml',
--        $$# § 12101 — Findings and purpose
--          ## (a) Findings
--          The Congress finds that — ...$$,
--        '{"type":"usc_section","title_number":42,"chapter_number":"126","section_number":"12101","canonical_citation":"42 U.S.C. § 12101","release_point":"PL 119-84","is_repealed":false}'::jsonb,
--        'public', 'public')
--     ON CONFLICT (source_path)
--       WHERE document_type='statute'  -- NB: partial-unique-index target predicate
--       DO UPDATE SET updated_at = now()
--     RETURNING id
--   )
--   INSERT INTO usc.sections
--     (document_id, title_number, section_number, section_heading,
--      canonical_citation, full_text, source_xml, release_point)
--   SELECT id, 42, '12101', 'Findings and purpose',
--          '42 U.S.C. § 12101',
--          $$normalized markdown body$$,
--          $$<section identifier="/us/usc/t42/s12101">…</section>$$,
--          'PL 119-84'
--   FROM new_doc;
-- COMMIT;
--
-- -- Then, one chunk with embedding inserted via the canonical halfvec cast:
-- --
-- --   INSERT INTO core.document_chunks
-- --     (id, document_id, chunk_index, total_chunks, content,
-- --      embedding, embedding_model_id, token_count, embedded_content)
-- --   VALUES
-- --     ($1, $2, 0, 1, $3, $4::halfvec(1024), 2, $5, $3);
-- --
-- -- where $4 is the BGE-M3 float32 numpy array passed straight to asyncpg
-- -- (the ::halfvec(1024) cast in SQL is the canonical conversion path).


-- ============================================================================
-- §10.  Open items the operator may want to weigh in on before DDL runs
-- ============================================================================
-- (a) document_type value: this design uses literal 'statute'. The existing
--     get_database_stats roll-up displays types as 'legal/email',
--     'legal/court_filing', etc., but the underlying core.documents rows
--     store the bare value ('email', 'court_filing'); the 'legal/' prefix is
--     a cosmetic concatenation of domain + document_type. 'statute' matches
--     this convention. Alternative ('legal/statute') would be inconsistent
--     with stored values elsewhere — recommend declining.
--
-- (b) Repealed-section text: when OLRC ships only a stub note for a repealed
--     section (most older repeals), full_text in core.documents is the note
--     itself. For litigation involving conduct *before* the repeal, the
--     pre-repeal text must come from the historical release-point archive
--     (Phase 7 versioning). The current schema supports this — the same
--     canonical_citation gets a new usc.sections row keyed to the older
--     release_point — but Phase 2 acquisition only pulls 119-84.
--     Backfilling historical release points is a follow-up project.
--
-- (c) Chapter tag attachment: §7.3 inserts chapter tags lazily during
--     ingestion. Confirm this is acceptable. Alternative: pre-seed all
--     known chapters from the OLRC chapter list at parser-bootstrap time.
--     Lazy is cheaper (~3,000 chapter rows total); pre-seed costs an extra
--     fetch but produces a stable tag taxonomy snapshot.
--
-- (d) JSONB query pattern reminder for Phase 6 retrieval guide:
--     idx_documents_metadata is GIN(metadata jsonb_path_ops). All USC
--     metadata queries must use containment to hit the index:
--         WHERE metadata @> '{"title_number": 42}'::jsonb        -- ✓
--         WHERE metadata->>'title_number' = '42'                  -- ✗ seq scan
--     The Phase 6 retrieval-patterns note will document this for downstream
--     agents; capturing here so the constraint isn't lost between phases.

-- ============================================================================
-- End schema_design.sql — DO NOT APPLY UNTIL OPERATOR APPROVES.
-- ============================================================================
