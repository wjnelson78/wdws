-- =====================================================================
-- Phase 4.4 idempotent re-run — post-Task-4 deferred-set ingest
-- =====================================================================
-- Resolves the 6 cross-rule references that were stranded NULL because
-- their target sets (DRJ, IRLJ, SPRC) didn't exist at original Phase 4
-- ingest time. Now that Task 4 has ingested those sets, the FK should
-- resolve via target_set_code + target_rule_number tuple match.
--
-- Pre-flight expected (verified in Task 4 pre-flight):
--   DRJ 14:        1 ref (DRJ 14(e))
--   IRLJ 1.2:      1 ref (IRLJ 1.2(j))
--   IRLJ 2.1:      1 ref
--   IRLJ 2.6:      2 refs (IRLJ 2.6(b) + IRLJ 2.6(c)(2))
--   SPRC 3:        1 ref
--   TOTAL:         6 refs
--
-- Halt band (per operator): delta MUST be in [+5, +10]; outside → halt.
-- =====================================================================

BEGIN;

-- Snapshot pre-state (intra-corpus only; cross-corpus FKs already done in Tasks 1-3)
DO $$
DECLARE
  pre_resolved INTEGER;
  post_resolved INTEGER;
  delta INTEGER;
BEGIN
  SELECT COUNT(*) INTO pre_resolved
    FROM wa_court_rules.cross_references
   WHERE target_jurisdiction='state-wa-court-rule'
     AND target_rule_id IS NOT NULL;

  -- Resolve target_rule_id by canonical-citation match against the now-richer
  -- wa_court_rules.rules table. Anchored on existing target_set_code +
  -- target_rule_number tuple (this is how Phase 4 originally did it).
  UPDATE wa_court_rules.cross_references c
  SET target_rule_id = r.id
  FROM wa_court_rules.rules r
  WHERE c.target_jurisdiction = 'state-wa-court-rule'
    AND c.target_rule_id IS NULL
    AND c.target_set_code IS NOT NULL
    AND c.target_rule_number IS NOT NULL
    AND r.rule_set_code = c.target_set_code
    AND r.rule_number = c.target_rule_number;

  SELECT COUNT(*) INTO post_resolved
    FROM wa_court_rules.cross_references
   WHERE target_jurisdiction='state-wa-court-rule'
     AND target_rule_id IS NOT NULL;

  delta := post_resolved - pre_resolved;
  RAISE NOTICE 'Pre-resolved=%   Post-resolved=%   Delta=%', pre_resolved, post_resolved, delta;

  IF delta NOT BETWEEN 5 AND 10 THEN
    RAISE EXCEPTION 'HALT: delta=% outside [+5,+10] band', delta;
  END IF;
  RAISE NOTICE 'OK: delta within halt band; committing.';
END $$;

COMMIT;
