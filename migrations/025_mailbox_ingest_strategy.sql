-- ============================================================
-- 025_mailbox_ingest_strategy.sql
-- ============================================================
--
-- Adds per-mailbox ingest strategy + filter config to ops.sync_mailboxes.
-- Lets graph_delta_worker.py decide per (mailbox, folder) whether to ingest
-- everything (full) or only messages matching an allowlist (filtered).
--
-- Strategies:
--   full        — ingest every change; no filter applied.
--   filtered    — fetch delta metadata, then only ingest messages whose
--                 sender matches filter_config.domains OR filter_config.emails.
--                 Delta tombstones (@removed) always apply regardless of filter.
--   disabled    — worker skips the mailbox entirely (useful for maintenance).
--
-- Seed:
--   athena@seattleseahawks.me → 'full' — the work mailbox; ingest everything.
--   william@seattleseahawks.me → 'filtered' — personal mailbox; ingest only
--     legal/professional senders matching the 49 target domains + 7 specific
--     email addresses from email_sync_config.py.
--
-- Safety: ADD COLUMN IF NOT EXISTS + conditional seed. Re-runnable.

BEGIN;

ALTER TABLE ops.sync_mailboxes
    ADD COLUMN IF NOT EXISTS ingest_strategy TEXT NOT NULL DEFAULT 'filtered'
        CHECK (ingest_strategy IN ('full', 'filtered', 'disabled')),
    ADD COLUMN IF NOT EXISTS filter_config   JSONB;

COMMENT ON COLUMN ops.sync_mailboxes.ingest_strategy IS
    'full | filtered | disabled. Drives graph_delta_worker.py: full pulls '
    'every message in the mailbox (subject to delta cursor); filtered only '
    'ingests messages whose sender matches filter_config; disabled skips.';
COMMENT ON COLUMN ops.sync_mailboxes.filter_config IS
    'JSONB allowlist for filtered strategy: '
    '{"domains": ["workerlaw.com", ...], "emails": ["sara@example.com", ...]}. '
    'NULL or empty for strategy=full.';

-- Seed: william stays on filtered, athena switches to full.
UPDATE ops.sync_mailboxes
   SET ingest_strategy = 'full',
       filter_config   = NULL
 WHERE email = 'athena@seattleseahawks.me';

-- William's allowlist mirrors email_sync_config.py TARGET_DOMAINS +
-- TARGET_SPECIFIC_EMAILS. Source of truth moves from Python constants into
-- DB, so operators can tweak the list without code deploys.
UPDATE ops.sync_mailboxes
   SET ingest_strategy = 'filtered',
       filter_config   = jsonb_build_object(
         'domains', jsonb_build_array(
           -- Government / court
           'snoco.org', 'co.snohomish.wa.us', 'co.chelan.wa.us',
           'courts.wa.gov', 'ca9.uscourts.gov', 'southsnofire.org',
           -- Opposing / defense counsel
           'workerlaw.com', 'csdlaw.com', 'cabornelaw.com',
           'clearpathpllc.com', 'fwwlaw.com', 'kantorlaw.net',
           'bn-lawyers.com', 'ogletree.com', 'ogletreedeakins.com',
           'lewisbrisbois.com', 'grsm.com', 'insleebest.com',
           'jmblawyers.com', 'uscanadalaw.com', 'favros.com',
           'cozen.com', 'slwsd.com', 'lldkb.com', 'mhb.com',
           -- Organizations / other
           'wahbexchange.org', 'evergreenhealthcare.org',
           'evergreenhealth.com', 'starbucks.com', 'sedgwick.com',
           'unum.com', 'playfasa.com'
         ),
         'domain_patterns', jsonb_build_array(
           '*.uscourts.gov'   -- federal court catchall
         ),
         'emails', jsonb_build_array(
           'fayejwonglawfirm@gmail.com',
           'sara.c.murray@gmail.com',
           'wjnelson78@gmail.com',
           'rcwcodebuster@gmail.com',
           'rcwcodebuster@aol.com',
           'rcwcodebuster@yahoo.com'
         )
       )
 WHERE email = 'william@seattleseahawks.me';

COMMIT;
