-- =============================================================================
-- Migration 037: cross-corpus FK uplift for case_law citations
-- =============================================================================
-- Author:        Athena WA-Case-Law-Ingest agent
-- Date:          2026-05-01
-- Companion:     /opt/wdws/migrations/036_case_law_schema.sql (must be applied first)
--
-- Adds target_case_law_opinion_id INTEGER NULL to each existing per-corpus
-- cross_references table (5 tables: usc, rcw, wac, wa_court_rules, wa_constitution).
-- Where chk_target_jurisdiction CHECK constraints exist, harmonize to add
-- 'state-wa-case-law' as a permitted target.
--
-- This is FK-additive: zero existing rows change, no data migration. The new
-- column is populated by Phase 6 from case_law.opinions_cited + cross-corpus
-- citation extraction over each corpus's full_text.
-- =============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name='case_law') THEN
        RAISE EXCEPTION 'case_law schema not present; apply 036 first. Halt.';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables
                   WHERE table_schema='case_law' AND table_name='opinions') THEN
        RAISE EXCEPTION 'case_law.opinions not present; apply 036 first. Halt.';
    END IF;
END $$;

-- §1  ADD COLUMN target_case_law_opinion_id to all 5 cross_references tables
-- (FK ensures referential integrity; NULL allowed since most xrefs are not
-- case-law citations).

ALTER TABLE usc.cross_references
    ADD COLUMN IF NOT EXISTS target_case_law_opinion_id INTEGER NULL
    REFERENCES case_law.opinions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_usc_xref_case_law
    ON usc.cross_references(target_case_law_opinion_id)
    WHERE target_case_law_opinion_id IS NOT NULL;

ALTER TABLE rcw.cross_references
    ADD COLUMN IF NOT EXISTS target_case_law_opinion_id INTEGER NULL
    REFERENCES case_law.opinions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_rcw_xref_case_law
    ON rcw.cross_references(target_case_law_opinion_id)
    WHERE target_case_law_opinion_id IS NOT NULL;

ALTER TABLE wac.cross_references
    ADD COLUMN IF NOT EXISTS target_case_law_opinion_id INTEGER NULL
    REFERENCES case_law.opinions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_wac_xref_case_law
    ON wac.cross_references(target_case_law_opinion_id)
    WHERE target_case_law_opinion_id IS NOT NULL;

ALTER TABLE wa_court_rules.cross_references
    ADD COLUMN IF NOT EXISTS target_case_law_opinion_id INTEGER NULL
    REFERENCES case_law.opinions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_wcr_xref_case_law
    ON wa_court_rules.cross_references(target_case_law_opinion_id)
    WHERE target_case_law_opinion_id IS NOT NULL;

ALTER TABLE wa_constitution.cross_references
    ADD COLUMN IF NOT EXISTS target_case_law_opinion_id INTEGER NULL
    REFERENCES case_law.opinions(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_const_xref_case_law
    ON wa_constitution.cross_references(target_case_law_opinion_id)
    WHERE target_case_law_opinion_id IS NOT NULL;

-- §2  Harmonize chk_target_jurisdiction CHECK constraints to accept
-- 'state-wa-case-law' (matching tag slug from migration 036). Three tables
-- have this CHECK; usc + wa_court_rules do not.

ALTER TABLE rcw.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE rcw.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'state-wa-rcw','federal-usc','state-wa-wac','state-wa-court-rule',
        'cfr','session-law','wa-constitution','state-wa-case-law'
    ])
);

ALTER TABLE wac.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE wac.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'state-wa-rcw','federal-usc','state-wa-wac','state-wa-court-rule',
        'cfr','session-law','wa-constitution','state-wa-case-law'
    ])
);

ALTER TABLE wa_constitution.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE wa_constitution.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'wa-constitution','state-wa-rcw','state-wa-wac','state-wa-court-rule',
        'federal-usc','wa-amendment','cfr','session-law','other','state-wa-case-law'
    ])
);

-- §3  ref_type extension: existing 'authority' value already covers case-law
-- citations (operator's existing semantic — see rcw schema canon). No CHECK
-- modification required for ref_type.

-- §4  Verification queries (post-apply):
--   SELECT table_schema, COUNT(*)
--     FROM information_schema.columns
--    WHERE column_name='target_case_law_opinion_id'
--    GROUP BY 1 ORDER BY 1;
--     -- expected: 5 schemas, 1 column each
