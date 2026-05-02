-- =============================================================================
-- Migration 037 DOWN — Remove cross-corpus FK uplift for case_law
-- =============================================================================
-- Drops target_case_law_opinion_id columns + reverts CHECK constraints.
-- Note: any rows where target_case_law_opinion_id IS NOT NULL OR
-- target_jurisdiction='state-wa-case-law' will lose data; verify before
-- running.
-- =============================================================================

-- §1  Verify no data loss
DO $$
DECLARE
    n_rows INTEGER;
BEGIN
    SELECT COALESCE(SUM(c), 0) INTO n_rows FROM (
        SELECT COUNT(*) c FROM usc.cross_references            WHERE target_case_law_opinion_id IS NOT NULL OR target_jurisdiction='state-wa-case-law'
        UNION ALL
        SELECT COUNT(*) c FROM rcw.cross_references            WHERE target_case_law_opinion_id IS NOT NULL OR target_jurisdiction='state-wa-case-law'
        UNION ALL
        SELECT COUNT(*) c FROM wac.cross_references            WHERE target_case_law_opinion_id IS NOT NULL OR target_jurisdiction='state-wa-case-law'
        UNION ALL
        SELECT COUNT(*) c FROM wa_court_rules.cross_references WHERE target_case_law_opinion_id IS NOT NULL OR target_jurisdiction='state-wa-case-law'
        UNION ALL
        SELECT COUNT(*) c FROM wa_constitution.cross_references WHERE target_case_law_opinion_id IS NOT NULL OR target_jurisdiction='state-wa-case-law'
    ) s;
    IF n_rows > 0 THEN
        RAISE NOTICE 'WARNING: % cross-references rows reference case_law; rolling back will null/drop them.', n_rows;
    END IF;
END $$;

-- §2  Drop columns + indexes
DROP INDEX IF EXISTS usc.idx_usc_xref_case_law;
ALTER TABLE usc.cross_references DROP COLUMN IF EXISTS target_case_law_opinion_id;

DROP INDEX IF EXISTS rcw.idx_rcw_xref_case_law;
ALTER TABLE rcw.cross_references DROP COLUMN IF EXISTS target_case_law_opinion_id;

DROP INDEX IF EXISTS wac.idx_wac_xref_case_law;
ALTER TABLE wac.cross_references DROP COLUMN IF EXISTS target_case_law_opinion_id;

DROP INDEX IF EXISTS wa_court_rules.idx_wcr_xref_case_law;
ALTER TABLE wa_court_rules.cross_references DROP COLUMN IF EXISTS target_case_law_opinion_id;

DROP INDEX IF EXISTS wa_constitution.idx_const_xref_case_law;
ALTER TABLE wa_constitution.cross_references DROP COLUMN IF EXISTS target_case_law_opinion_id;

-- §3  Revert CHECK constraints (remove 'state-wa-case-law' from accepted set)
ALTER TABLE rcw.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE rcw.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'state-wa-rcw','federal-usc','state-wa-wac','state-wa-court-rule',
        'cfr','session-law','wa-constitution'
    ])
);

ALTER TABLE wac.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE wac.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'state-wa-rcw','federal-usc','state-wa-wac','state-wa-court-rule',
        'cfr','session-law','wa-constitution'
    ])
);

ALTER TABLE wa_constitution.cross_references DROP CONSTRAINT IF EXISTS chk_target_jurisdiction;
ALTER TABLE wa_constitution.cross_references ADD CONSTRAINT chk_target_jurisdiction CHECK (
    target_jurisdiction = ANY(ARRAY[
        'wa-constitution','state-wa-rcw','state-wa-wac','state-wa-court-rule',
        'federal-usc','wa-amendment','cfr','session-law','other'
    ])
);
