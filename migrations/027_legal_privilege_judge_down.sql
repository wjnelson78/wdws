-- Rollback Migration 027: Legal privilege, confidentiality, protective orders, judge fields
-- Reverses everything migration 027 added. Additive-only forward means no data is lost
-- by this rollback (the dropped columns were all added NULL-default by 027).

BEGIN;

DROP VIEW IF EXISTS core.v_documents_safe_retrieval;

DROP INDEX IF EXISTS core.idx_documents_confidentiality;
DROP INDEX IF EXISTS core.idx_documents_privilege;
DROP INDEX IF EXISTS legal.idx_cases_assigned_judge;

ALTER TABLE core.documents DROP COLUMN IF EXISTS protective_order_id;
DROP TABLE IF EXISTS legal.protective_orders;

ALTER TABLE legal.cases DROP COLUMN IF EXISTS opposing_firm;
ALTER TABLE legal.cases DROP COLUMN IF EXISTS assigned_judge;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_confidentiality_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS confidentiality;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_privilege_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege_classified_by;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege_classified_at;
ALTER TABLE core.documents DROP COLUMN IF EXISTS privilege;

COMMIT;
