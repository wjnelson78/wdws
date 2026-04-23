-- Migration 027: Legal privilege, confidentiality, protective orders, judge fields
-- Sprint A Task 1 (per SPRINT_A_WORK_ORDER_v2.2.md §3)
-- Additive only. No drops. No data loss.

BEGIN;

-- ---------- Part A: Privilege classification on core.documents ----------

ALTER TABLE core.documents
    ADD COLUMN privilege text,
    ADD COLUMN privilege_classified_at timestamp with time zone,
    ADD COLUMN privilege_classified_by text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_privilege_check
    CHECK (privilege IS NULL OR privilege IN (
        'attorney_client', 'work_product_opinion', 'work_product_fact',
        'joint_defense', 'common_interest', 'none', 'unknown'
    ));

COMMENT ON COLUMN core.documents.privilege IS
    'Privilege status. NULL = not yet classified. Retrieval layer treats NULL as potentially privileged until classified.';

-- ---------- Part B: Confidentiality tier ----------

ALTER TABLE core.documents
    ADD COLUMN confidentiality text;

ALTER TABLE core.documents
    ADD CONSTRAINT documents_confidentiality_check
    CHECK (confidentiality IS NULL OR confidentiality IN (
        'public', 'confidential', 'aeo', 'under_seal', 'highly_confidential'
    ));

-- ---------- Part C: Protective orders table ----------

CREATE TABLE legal.protective_orders (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    case_id integer NOT NULL REFERENCES legal.cases(id) ON DELETE CASCADE,
    order_document_id uuid REFERENCES core.documents(id),
    effective_date date NOT NULL,
    tiers text[] NOT NULL DEFAULT '{}',
    notes text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
CREATE INDEX idx_protective_orders_case ON legal.protective_orders(case_id);

ALTER TABLE core.documents
    ADD COLUMN protective_order_id uuid REFERENCES legal.protective_orders(id);

-- ---------- Part D: Judge and opposing firm on cases ----------

ALTER TABLE legal.cases
    ADD COLUMN assigned_judge text,
    ADD COLUMN opposing_firm text;

CREATE INDEX idx_cases_assigned_judge
    ON legal.cases(assigned_judge) WHERE assigned_judge IS NOT NULL;

-- ---------- Part E: Indexes for privilege-aware retrieval ----------

CREATE INDEX idx_documents_privilege
    ON core.documents(privilege) WHERE privilege IS NOT NULL;
CREATE INDEX idx_documents_confidentiality
    ON core.documents(confidentiality) WHERE confidentiality IS NOT NULL;

-- ---------- Part F: Safe-retrieval view ----------

CREATE OR REPLACE VIEW core.v_documents_safe_retrieval AS
    SELECT * FROM core.documents
    WHERE privilege = 'none' AND purged_at IS NULL;

COMMENT ON VIEW core.v_documents_safe_retrieval IS
    'Documents explicitly classified non-privileged and not purged. Default view for non-privileged retrieval contexts. NULL-privilege documents are excluded until classified.';

COMMIT;
