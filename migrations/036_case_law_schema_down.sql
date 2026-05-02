-- =============================================================================
-- Migration 036 DOWN — Drop case_law schema
-- =============================================================================
-- Removes the case_law schema + all tables created by 036_case_law_schema.sql.
-- Tag rows in core.tags are NOT auto-removed (they may be referenced by
-- core.document_tags rows from a prior partial ingest — manual cleanup if
-- needed via DELETE FROM core.tags WHERE slug IN (...)).
-- =============================================================================

DROP SCHEMA IF EXISTS case_law CASCADE;

-- Tag rows: removable manually if no document_tags references remain.
-- Verify before deleting:
--   SELECT COUNT(*) FROM core.document_tags dt
--    JOIN core.tags t ON t.id=dt.tag_id
--    WHERE t.slug IN ('state-wa-case-law','wa-supreme-court','wa-court-of-appeals',
--                     'opinion-lead','opinion-dissent','opinion-concurrence',
--                     'opinion-per-curiam');
-- If 0, then:
--   DELETE FROM core.tags WHERE slug IN ('state-wa-case-law','wa-supreme-court',
--                                        'wa-court-of-appeals','opinion-lead',
--                                        'opinion-dissent','opinion-concurrence',
--                                        'opinion-per-curiam');
