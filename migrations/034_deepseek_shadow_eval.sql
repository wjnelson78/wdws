-- Migration 034: DeepSeek shadow classifier evaluation table
--
-- Stores DeepSeek V4 Pro classifications run in parallel with the production
-- Sonnet 4.6 backfill, for ongoing apples-to-apples comparison without
-- interfering with the production worker's queue or staging rows.

BEGIN;

CREATE TABLE IF NOT EXISTS ops.deepseek_shadow_classifications (
  id                       uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  document_id              uuid        NOT NULL REFERENCES core.documents(id) ON DELETE CASCADE,
  sonnet_staging_id        uuid        REFERENCES core.documents_backfill_staging(id) ON DELETE SET NULL,

  -- Sonnet's verdict (snapshotted at shadow time so we have stable comparison)
  sonnet_privilege         text,
  sonnet_confidentiality   text,
  sonnet_confidence        numeric,

  -- DeepSeek's verdict
  deepseek_model           text        NOT NULL,
  deepseek_privilege       text,
  deepseek_confidentiality text,
  deepseek_confidence      numeric,
  deepseek_rationale       text,

  -- Diagnostics
  latency_ms               integer,
  error_detail             text,
  created_at               timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT deepseek_shadow_unique_per_sonnet_run UNIQUE (sonnet_staging_id)
);

CREATE INDEX IF NOT EXISTS idx_deepseek_shadow_doc
  ON ops.deepseek_shadow_classifications(document_id);

CREATE INDEX IF NOT EXISTS idx_deepseek_shadow_created
  ON ops.deepseek_shadow_classifications(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_deepseek_shadow_match
  ON ops.deepseek_shadow_classifications((sonnet_privilege = deepseek_privilege))
  WHERE error_detail IS NULL;

COMMENT ON TABLE ops.deepseek_shadow_classifications IS
  'Parallel DeepSeek V4 Pro classifications for comparison vs production Sonnet 4.6 backfill. '
  'Populated by sprint_a_deepseek_shadow.py polling core.documents_backfill_staging. '
  'sonnet_staging_id UNIQUE prevents double-shadowing the same Sonnet run.';

COMMIT;
