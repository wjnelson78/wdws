-- Rollback Migration 028: Medical PHI classification and HIPAA tracking
-- Reverses every addition from 028. Additive-only forward means no data loss
-- via this rollback.

BEGIN;

DROP VIEW IF EXISTS core.v_medical_safe_retrieval;

DROP INDEX IF EXISTS core.idx_documents_minor_patient;
DROP INDEX IF EXISTS core.idx_documents_phi_categories;
DROP INDEX IF EXISTS core.idx_documents_phi_status;

ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_authorization_id;
DROP TABLE IF EXISTS medical.disclosures_log;
DROP TABLE IF EXISTS medical.authorizations;

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_phi_status_check;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_classified_by;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_classified_at;
ALTER TABLE core.documents DROP COLUMN IF EXISTS minor_patient;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_categories;
ALTER TABLE core.documents DROP COLUMN IF EXISTS phi_status;

COMMIT;
