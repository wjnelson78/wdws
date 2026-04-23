-- Migration 028: Medical PHI classification and HIPAA tracking
-- Sprint A Task 2 (per SPRINT_A_WORK_ORDER_v2.2.md §4)
-- Additive only. No drops.

BEGIN;

-- ---------- Part A: PHI classification on core.documents ----------

ALTER TABLE core.documents
    ADD COLUMN phi_status text,
    ADD COLUMN phi_categories text[] DEFAULT '{}',
    ADD COLUMN minor_patient boolean DEFAULT false,
    ADD COLUMN phi_classified_at timestamp with time zone,
    ADD COLUMN phi_classified_by text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_phi_status_check
    CHECK (phi_status IS NULL OR phi_status IN (
        'phi', 'limited_data_set', 'safe_harbor_deidentified',
        'expert_determination_deidentified', 'not_phi'
    ));

COMMENT ON COLUMN core.documents.phi_status IS
    'HIPAA classification. NULL = not yet classified. Retrieval treats NULL in medical domain as PHI until classified.';

COMMENT ON COLUMN core.documents.phi_categories IS
    'Heightened-protection categories. Valid values: sud_42_cfr_part_2, psychotherapy_notes, genetic_gina, mental_health, hiv_aids.';

-- ---------- Part B: Authorization tracking ----------

CREATE TABLE medical.authorizations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    patient_id integer NOT NULL REFERENCES medical.patients(id),
    purpose_of_use text NOT NULL,
    scope text NOT NULL,
    effective_date date NOT NULL,
    expiration_date date,
    revoked_at timestamp with time zone,
    authorization_document_id uuid REFERENCES core.documents(id),
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_auth_patient ON medical.authorizations(patient_id);
-- idx_auth_active narrows to non-revoked authorizations. The original spec
-- in SPRINT_A_WORK_ORDER_v2.2.md §4.2 additionally filtered by
-- expiration_date >= CURRENT_DATE, but CURRENT_DATE is STABLE (not
-- IMMUTABLE) and Postgres rejects non-IMMUTABLE functions in partial-index
-- predicates. Expired-filtering happens at query time against the
-- already-narrowed index. (Caught by shadow verification; closeout note
-- covers the draft-time predicate-review lesson.)
CREATE INDEX idx_auth_active ON medical.authorizations(patient_id)
    WHERE revoked_at IS NULL;

ALTER TABLE core.documents
    ADD COLUMN phi_authorization_id uuid REFERENCES medical.authorizations(id);

-- ---------- Part C: HIPAA accounting-of-disclosures log ----------

CREATE TABLE medical.disclosures_log (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id uuid NOT NULL REFERENCES core.documents(id),
    patient_id integer REFERENCES medical.patients(id),
    disclosed_to text NOT NULL,
    purpose_of_use text NOT NULL,
    disclosure_date timestamp with time zone NOT NULL DEFAULT now(),
    authorization_id uuid REFERENCES medical.authorizations(id),
    disclosed_by text,
    notes text,
    created_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_disclosures_document ON medical.disclosures_log(document_id);
CREATE INDEX idx_disclosures_patient ON medical.disclosures_log(patient_id);
CREATE INDEX idx_disclosures_date ON medical.disclosures_log(disclosure_date);

COMMENT ON TABLE medical.disclosures_log IS
    'HIPAA 45 CFR 164.528 accounting of disclosures. Every disclosure of PHI outside TPO is logged here.';

-- ---------- Part D: Indexes for PHI-aware retrieval ----------

CREATE INDEX idx_documents_phi_status
    ON core.documents(phi_status) WHERE phi_status IS NOT NULL;
CREATE INDEX idx_documents_phi_categories
    ON core.documents USING GIN(phi_categories) WHERE phi_categories <> '{}';
CREATE INDEX idx_documents_minor_patient
    ON core.documents(minor_patient) WHERE minor_patient = true;

-- ---------- Part E: Safe-retrieval view for medical ----------

CREATE OR REPLACE VIEW core.v_medical_safe_retrieval AS
    SELECT * FROM core.documents
    WHERE domain = 'medical'
      AND phi_status IN ('safe_harbor_deidentified', 'expert_determination_deidentified', 'not_phi')
      AND purged_at IS NULL;

COMMENT ON VIEW core.v_medical_safe_retrieval IS
    'Medical documents safe for non-clinical retrieval contexts. Excludes raw PHI and unclassified documents.';

COMMIT;
