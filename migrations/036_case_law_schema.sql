-- =============================================================================
-- Migration 036: case_law schema (WA appellate case law from CourtListener bulk data)
-- =============================================================================
-- Author:        Athena WA-Case-Law-Ingest agent
-- Date:          2026-05-01
-- Companion:     /opt/wdws/wa_caselaw_research/phase6_retrieval_guide_notes.md
-- Source:        CourtListener bulk-data S3 (CC0 / Public Domain Mark)
-- Dump version:  2026-03-31 quarterly regeneration
-- Scope:         WA Supreme Court (court_id='wash') + WA Court of Appeals
--                (court_id='washctapp'). 1889-present. Published opinions only.
-- Projected:     ~94K opinion-clusters / ~120K opinions / WA-filtered citation
--                graph from search_opinionscited.
-- Cross-corpus:  Companion migration 037 adds target_case_law_opinion_id
--                column to existing 5 cross_references tables. This migration
--                creates the case_law schema only; uplift is in 037.
--
-- Architecture decision provenance:
--  - §K #41 — source-authorization-vs-operational-citation-pattern audit
--  - §K #42 — coordinator under-estimated bulk-data acquisition path richness
--  - §K #43 — bulk-data scale (>3× disk headroom) requires streaming pipeline
--    NOT staged-then-filter. Phase 3 uses external Python CSV filter →
--    psql COPY FROM stdin, no full-corpus on-disk staging.
--
-- ID type decision: case_law tables retain CL native INTEGER ids (no UUID
-- mapping) for these reasons:
--  1. CL ids are stable and globally unique in CL's namespace; useful for
--     re-fetching individual records via CL REST API for spot-check verification
--     during Phase 8.
--  2. The OpinionsCited citation graph is keyed by CL integer ids; preserving
--     them eliminates Phase 5 (citation graph FK resolution) entirely.
--  3. Cross-corpus references (in cross_references.target_case_law_opinion_id)
--     can use the same CL integer id, providing a stable join key.
--  4. core.documents.id (UUID) is the chunking/embedding anchor; case_law.opinions
--     joins to core.documents via case_law.opinions.document_id (UUID FK).
--
-- Idempotency: schema creation is wrapped in DO blocks; rerun-safe.
-- =============================================================================

-- =============================================================================
-- §0  PRE-APPLY ASSERTIONS
-- =============================================================================
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='case_law') THEN
        RAISE EXCEPTION 'case_law schema already exists; expected absent. Halt.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname='vector') THEN
        RAISE EXCEPTION 'pgvector extension required but not installed. Halt.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema='core' AND table_name='documents') THEN
        RAISE EXCEPTION 'core.documents not present; required FK target. Halt.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema='core' AND table_name='document_chunks') THEN
        RAISE EXCEPTION 'core.document_chunks not present; required for embeddings. Halt.';
    END IF;
END $$;

-- =============================================================================
-- §1  case_law schema + tables
-- =============================================================================

CREATE SCHEMA case_law;
COMMENT ON SCHEMA case_law IS
  'WA appellate case law (Supreme Court + Court of Appeals) from CourtListener
   bulk-data 2026-03-31 dump. Published opinions only. court_id IN (''wash'',
   ''washctapp''). See /opt/wdws/wa_caselaw_research/phase6_retrieval_guide_notes.md.';

-- §1.1  case_law.courts — CL court reference (small; only 2 rows after WA filter
-- but full CL court table loaded for completeness so parent_court_id FKs resolve)
CREATE TABLE case_law.courts (
    id              VARCHAR(15)  PRIMARY KEY,        -- CL court_id, e.g. 'wash'
    short_name      VARCHAR(100) NOT NULL,
    full_name       VARCHAR(200) NOT NULL,
    citation_string VARCHAR(100) NOT NULL,
    jurisdiction    VARCHAR(3)   NOT NULL,           -- 'SS','SA','ST', etc.
    in_use          BOOLEAN      NOT NULL,
    position        DOUBLE PRECISION,
    url             VARCHAR(500),
    start_date      DATE,
    end_date        DATE,
    notes           TEXT,
    parent_court_id VARCHAR(15),
    date_modified   TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
COMMENT ON TABLE case_law.courts IS
  'CL search_court reference table. Loaded fully (all CL courts) so parent_court_id
   chains resolve. Filter for WA: id IN (''wash'',''washctapp'',''washterr'').';

-- §1.2  case_law.dockets — WA dockets only
CREATE TABLE case_law.dockets (
    id                 INTEGER     PRIMARY KEY,       -- CL search_docket.id
    court_id           VARCHAR(15) NOT NULL REFERENCES case_law.courts(id),
    case_name          TEXT        NOT NULL,
    case_name_short    TEXT,
    case_name_full     TEXT,
    docket_number      TEXT,
    slug               VARCHAR(75),
    date_filed         DATE,
    date_terminated    DATE,
    date_argued        DATE,
    nature_of_suit     VARCHAR(1000),
    cause              VARCHAR(2000),
    appeal_from_id     VARCHAR(15),
    appeal_from_str    TEXT,
    source             SMALLINT,
    blocked            BOOLEAN     NOT NULL DEFAULT false,
    date_blocked       DATE,
    date_created       TIMESTAMPTZ NOT NULL,
    date_modified      TIMESTAMPTZ NOT NULL,
    ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_dockets_wa_court CHECK (court_id IN ('wash','washctapp','washterr'))
);
CREATE INDEX idx_dockets_court ON case_law.dockets(court_id);
CREATE INDEX idx_dockets_date_filed ON case_law.dockets(date_filed) WHERE date_filed IS NOT NULL;

-- §1.3  case_law.opinion_clusters — one row per case (may have multiple opinion docs)
CREATE TABLE case_law.opinion_clusters (
    id                       INTEGER     PRIMARY KEY,    -- CL search_opinioncluster.id
    docket_id                INTEGER     NOT NULL REFERENCES case_law.dockets(id),
    case_name                TEXT        NOT NULL,
    case_name_short          TEXT,
    case_name_full           TEXT,
    date_filed               DATE        NOT NULL,
    date_filed_is_approximate BOOLEAN    NOT NULL DEFAULT false,
    slug                     VARCHAR(75),
    judges                   TEXT,
    nature_of_suit           TEXT,
    posture                  TEXT,
    procedural_history       TEXT,
    syllabus                 TEXT,
    headnotes                TEXT,
    summary                  TEXT,
    disposition              TEXT,
    history                  TEXT,
    citation_count           INTEGER     NOT NULL DEFAULT 0,
    precedential_status      VARCHAR(50) NOT NULL,
    source                   VARCHAR(10),
    blocked                  BOOLEAN     NOT NULL DEFAULT false,
    date_blocked             DATE,
    cross_reference          TEXT,
    correction               TEXT,
    other_dates              TEXT,
    attorneys                TEXT,
    arguments                TEXT,
    headmatter               TEXT,
    canonical_citation       TEXT,                       -- normalized e.g. "130 Wn.2d 97"
    parallel_citations       TEXT[],                     -- array of normalized citations
    division                 TEXT,                       -- 'I','II','III' for COA, NULL for SS
    date_created             TIMESTAMPTZ NOT NULL,
    date_modified            TIMESTAMPTZ NOT NULL,
    ingested_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_clusters_docket ON case_law.opinion_clusters(docket_id);
CREATE INDEX idx_clusters_date_filed ON case_law.opinion_clusters(date_filed);
CREATE INDEX idx_clusters_canonical_citation ON case_law.opinion_clusters(canonical_citation)
    WHERE canonical_citation IS NOT NULL;
CREATE INDEX idx_clusters_precedential ON case_law.opinion_clusters(precedential_status);

-- §1.4  case_law.opinions — one row per opinion document (lead/dissent/concurrence/etc.)
-- Joins to core.documents for chunking + embeddings.
CREATE TABLE case_law.opinions (
    id                  INTEGER     PRIMARY KEY,        -- CL search_opinion.id
    cluster_id          INTEGER     NOT NULL REFERENCES case_law.opinion_clusters(id),
    document_id         UUID        UNIQUE REFERENCES core.documents(id) ON DELETE SET NULL,
    type                VARCHAR(20) NOT NULL,           -- '020lead','040dissent','060concurrence', etc.
    author_str          TEXT,                           -- 'Guy, J.' (free-form)
    author_id           INTEGER,                        -- CL person id (sparsely populated)
    joined_by_str       TEXT,
    per_curiam          BOOLEAN     NOT NULL DEFAULT false,
    page_count          INTEGER,
    sha1                VARCHAR(40) NOT NULL,
    extracted_by_ocr    BOOLEAN     NOT NULL DEFAULT false,
    download_url        VARCHAR(500),
    local_path          VARCHAR(100),
    plain_text          TEXT,                           -- preferred extracted text
    html_with_citations TEXT,                           -- Harvard CAP TEI XML for historical opinions
    xml_harvard         TEXT,
    extracted_text      TEXT,                           -- canonical text used for chunking (post-extraction)
    text_source         TEXT,                           -- 'plain_text' | 'html_with_citations' | 'xml_harvard'
    char_count          INTEGER,
    date_created        TIMESTAMPTZ NOT NULL,
    date_modified       TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_opinions_cluster ON case_law.opinions(cluster_id);
CREATE INDEX idx_opinions_document ON case_law.opinions(document_id) WHERE document_id IS NOT NULL;
CREATE INDEX idx_opinions_type ON case_law.opinions(type);

-- §1.5  case_law.opinions_cited — pre-computed citation graph (replaces Phase 5)
-- Filtered to citing OR cited opinion in case_law.opinions.
CREATE TABLE case_law.opinions_cited (
    id                  INTEGER     PRIMARY KEY,        -- CL search_opinionscited.id
    citing_opinion_id   INTEGER     NOT NULL,           -- CL opinion id; may reference outside corpus
    cited_opinion_id    INTEGER     NOT NULL,           -- CL opinion id; may reference outside corpus
    depth               INTEGER     NOT NULL DEFAULT 1,
    citing_in_corpus    BOOLEAN     NOT NULL,           -- citing_opinion_id IS in case_law.opinions
    cited_in_corpus     BOOLEAN     NOT NULL,           -- cited_opinion_id IS in case_law.opinions
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_cited_citing ON case_law.opinions_cited(citing_opinion_id);
CREATE INDEX idx_cited_cited ON case_law.opinions_cited(cited_opinion_id);
CREATE INDEX idx_cited_in_corpus ON case_law.opinions_cited(citing_in_corpus, cited_in_corpus);
COMMENT ON TABLE case_law.opinions_cited IS
  'Pre-computed citation graph from CL search_opinionscited (citation-map CSV).
   citing_in_corpus / cited_in_corpus precomputed at ingest for fast filter.
   Replaces Phase 5 (internal citation graph resolution).';

-- §1.6  case_law.citations — parallel reporter citations on cluster
CREATE TABLE case_law.citations (
    id            INTEGER     PRIMARY KEY,              -- CL search_citation.id
    cluster_id    INTEGER     NOT NULL REFERENCES case_law.opinion_clusters(id),
    volume        TEXT,
    reporter      TEXT        NOT NULL,
    page          TEXT        NOT NULL,
    type          SMALLINT    NOT NULL,                 -- 1=federal, 2=state, etc. (CL convention)
    canonical     TEXT,                                 -- normalized e.g. "130 Wn.2d 97"
    date_created  TIMESTAMPTZ NOT NULL,
    date_modified TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_citations_cluster ON case_law.citations(cluster_id);
CREATE INDEX idx_citations_canonical ON case_law.citations(canonical) WHERE canonical IS NOT NULL;
CREATE INDEX idx_citations_reporter_volume_page ON case_law.citations(reporter, volume, page);

-- §1.7  case_law.parentheticals — descriptive parentheticals attached to opinions
CREATE TABLE case_law.parentheticals (
    id                    INTEGER          PRIMARY KEY,  -- CL search_parenthetical.id
    described_opinion_id  INTEGER          NOT NULL REFERENCES case_law.opinions(id) ON DELETE CASCADE,
    describing_opinion_id INTEGER          NOT NULL,     -- may reference outside corpus
    text                  TEXT             NOT NULL,
    score                 DOUBLE PRECISION NOT NULL,
    group_id              INTEGER,
    ingested_at           TIMESTAMPTZ      NOT NULL DEFAULT now()
);
CREATE INDEX idx_paren_described ON case_law.parentheticals(described_opinion_id);
CREATE INDEX idx_paren_describing ON case_law.parentheticals(describing_opinion_id);

-- =============================================================================
-- §2  Tag taxonomy additions
-- =============================================================================
INSERT INTO core.tags (name, category, slug, description) VALUES
    ('WA Case Law',           'code-jurisdiction', 'state-wa-case-law',     'Washington appellate case law (Supreme Court + Court of Appeals) from CourtListener.'),
    ('WA Supreme Court',      'court-tier',        'wa-supreme-court',      'Washington Supreme Court opinions (court_id=wash).'),
    ('WA Court of Appeals',   'court-tier',        'wa-court-of-appeals',   'Washington Court of Appeals (Div I/II/III) (court_id=washctapp).'),
    ('Lead opinion',          'opinion-type',      'opinion-lead',          'Majority/lead opinion (CL type=020lead).'),
    ('Dissent opinion',       'opinion-type',      'opinion-dissent',       'Dissenting opinion (CL type=040dissent).'),
    ('Concurrence opinion',   'opinion-type',      'opinion-concurrence',   'Concurring opinion (CL type=060concurrence).'),
    ('Per curiam opinion',    'opinion-type',      'opinion-per-curiam',    'Per curiam (CL type=010combined or per_curiam=true).')
ON CONFLICT (slug) DO NOTHING;

-- =============================================================================
-- §3  Migration bookkeeping
-- =============================================================================
COMMENT ON SCHEMA case_law IS
  'WA appellate case law from CL bulk-data 2026-03-31. Migration 036 — see
   companion 037 for cross-corpus FK uplift to existing cross_references tables.';

-- §3.1  Verification (post-apply, agent inspects):
--   SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='case_law';
--     -- expected: 7
--   SELECT COUNT(*) FROM core.tags WHERE slug='state-wa-case-law';
--     -- expected: 1
