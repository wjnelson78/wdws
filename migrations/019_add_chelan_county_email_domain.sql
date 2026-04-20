-- ============================================================
-- Migration: 019_add_chelan_county_email_domain.sql
-- Database: wdws
--
-- Ensure Chelan County email traffic is included in Athena's enterprise
-- Graph sync rules for William and Athena mailboxes.
-- ============================================================

INSERT INTO ops.sync_rules (name, rule_type, pattern, description, priority)
VALUES (
    'Chelan County WA',
    'domain',
    'co.chelan.wa.us',
    'Chelan County Washington state domain',
    0
)
ON CONFLICT (rule_type, pattern) DO NOTHING;