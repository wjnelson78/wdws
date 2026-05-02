-- Rollback for migration 034: drop DeepSeek shadow classifier table
BEGIN;
DROP TABLE IF EXISTS ops.deepseek_shadow_classifications;
COMMIT;
