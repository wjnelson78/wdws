-- Rollback for migration 035: revert table to deepseek-only naming
BEGIN;

ALTER TABLE ops.shadow_classifications
  DROP CONSTRAINT IF EXISTS shadow_unique_per_sonnet_per_model;

ALTER TABLE ops.shadow_classifications
  RENAME COLUMN shadow_model           TO deepseek_model;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN shadow_privilege       TO deepseek_privilege;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN shadow_confidentiality TO deepseek_confidentiality;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN shadow_confidence      TO deepseek_confidence;
ALTER TABLE ops.shadow_classifications
  RENAME COLUMN shadow_rationale       TO deepseek_rationale;

ALTER TABLE ops.shadow_classifications
  RENAME TO deepseek_shadow_classifications;

ALTER TABLE ops.deepseek_shadow_classifications
  ADD CONSTRAINT deepseek_shadow_unique_per_sonnet_run UNIQUE (sonnet_staging_id);

DROP INDEX IF EXISTS ops.idx_shadow_doc;
DROP INDEX IF EXISTS ops.idx_shadow_created;
DROP INDEX IF EXISTS ops.idx_shadow_model;
DROP INDEX IF EXISTS ops.idx_shadow_match;

CREATE INDEX idx_deepseek_shadow_doc
  ON ops.deepseek_shadow_classifications(document_id);
CREATE INDEX idx_deepseek_shadow_created
  ON ops.deepseek_shadow_classifications(created_at DESC);
CREATE INDEX idx_deepseek_shadow_match
  ON ops.deepseek_shadow_classifications((sonnet_privilege = deepseek_privilege))
  WHERE error_detail IS NULL;

COMMIT;
