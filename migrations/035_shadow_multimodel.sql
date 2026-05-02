-- Migration 035: generalize shadow eval table to support multiple models
--
-- Originally migration 034 hardcoded a UNIQUE constraint on sonnet_staging_id
-- alone, which prevents shadowing the same Sonnet run with different models
-- (DeepSeek + Haiku side-by-side). Replace with a composite unique constraint
-- and rename the columns to be model-agnostic.

BEGIN;

ALTER TABLE ops.deepseek_shadow_classifications
  RENAME TO shadow_classifications;

ALTER TABLE ops.shadow_classifications
  DROP CONSTRAINT IF EXISTS deepseek_shadow_unique_per_sonnet_run;

ALTER TABLE ops.shadow_classifications
  RENAME COLUMN deepseek_model           TO shadow_model;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN deepseek_privilege       TO shadow_privilege;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN deepseek_confidentiality TO shadow_confidentiality;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN deepseek_confidence      TO shadow_confidence;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN deepseek_rationale       TO shadow_rationale;

ALTER TABLE ops.shadow_classifications
  ADD CONSTRAINT shadow_unique_per_sonnet_per_model
  UNIQUE (sonnet_staging_id, shadow_model);

DROP INDEX IF EXISTS ops.idx_deepseek_shadow_doc;
DROP INDEX IF EXISTS ops.idx_deepseek_shadow_created;
DROP INDEX IF EXISTS ops.idx_deepseek_shadow_match;

CREATE INDEX IF NOT EXISTS idx_shadow_doc
  ON ops.shadow_classifications(document_id);
CREATE INDEX IF NOT EXISTS idx_shadow_created
  ON ops.shadow_classifications(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_shadow_model
  ON ops.shadow_classifications(shadow_model);
CREATE INDEX IF NOT EXISTS idx_shadow_match
  ON ops.shadow_classifications((sonnet_privilege = shadow_privilege))
  WHERE error_detail IS NULL;

COMMENT ON TABLE ops.shadow_classifications IS
  'Parallel shadow classifications (DeepSeek, Haiku, etc.) for comparison vs '
  'production Sonnet 4.6 backfill. shadow_model column discriminates which '
  'alternative model produced the row.';

COMMIT;
