-- Migration 029: Sprint A Task 4 LLM-assisted classification staging table
-- Per SPRINT_A_WORK_ORDER_v2.2.md §6.4 + 2026-04-23 design refinement
-- Additive only.

BEGIN;

CREATE TABLE core.documents_backfill_staging (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id uuid NOT NULL REFERENCES core.documents(id),
    batch_id uuid NOT NULL,
    domain text NOT NULL,
    -- Legal-domain proposed classifications
    proposed_privilege text,
    proposed_confidentiality text,
    -- Medical-domain proposed classifications
    proposed_phi_status text,
    proposed_phi_categories text[] DEFAULT '{}',
    proposed_minor_patient boolean,
    -- Classifier metadata
    confidence numeric NOT NULL,
    rationale text NOT NULL,
    classifier_version text NOT NULL,
    classifier_model text NOT NULL,
    -- Lifecycle
    status text NOT NULL DEFAULT 'pending',
    promoted_at timestamp with time zone,
    promoted_by text,
    superseded_at timestamp with time zone,
    error_detail text,  -- populated when status='rejected' for operator diagnostic
    created_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE core.documents_backfill_staging
    ADD CONSTRAINT backfill_staging_status_check
    CHECK (status IN ('pending', 'promoted', 'superseded', 'rejected'));

ALTER TABLE core.documents_backfill_staging
    ADD CONSTRAINT backfill_staging_domain_check
    CHECK (domain IN ('legal', 'medical'));

ALTER TABLE core.documents_backfill_staging
    ADD CONSTRAINT backfill_staging_confidence_check
    CHECK (confidence >= 0.0 AND confidence <= 1.0);

CREATE INDEX idx_bfs_document ON core.documents_backfill_staging(document_id);
CREATE INDEX idx_bfs_batch ON core.documents_backfill_staging(batch_id);
CREATE INDEX idx_bfs_pending ON core.documents_backfill_staging(document_id)
    WHERE status = 'pending';
CREATE INDEX idx_bfs_domain_status ON core.documents_backfill_staging(domain, status);
CREATE INDEX idx_bfs_created ON core.documents_backfill_staging(created_at DESC);

COMMENT ON TABLE core.documents_backfill_staging IS
    'Sprint A Task 4 staging table per v2.2 §6.4. LLM-assisted classifier outputs land here first; one-transaction batches of 100 promote to core.documents. Status lifecycle: pending (initial) → promoted (UPDATE landed on core.documents) | superseded (replaced by later staging row for same document) | rejected (manual review rejected the proposal or transient worker failure).';
COMMENT ON COLUMN core.documents_backfill_staging.status IS
    'Lifecycle state. pending = awaiting promotion (possibly awaiting human review). promoted = the classification has been written to core.documents. superseded = a later staging row for the same document exists and is authoritative. rejected = not promoted (either human review rejected or worker error; error_detail explains).';
COMMENT ON COLUMN core.documents_backfill_staging.confidence IS
    'Classifier confidence [0.0, 1.0]. Routing per v2.2 §6.3: >0.9 auto-promote; 0.7–0.9 queue for review; <0.7 flag as unknown. Heightened-protection categories route to review regardless of confidence.';

COMMIT;
