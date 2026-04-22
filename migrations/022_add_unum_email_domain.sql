-- ============================================================
-- Migration: 022_add_unum_email_domain.sql
-- Database: wdws
--
-- Add Unum (disability insurance / LTD claims) to Athena's
-- enterprise Graph sync rules for William and Athena mailboxes.
-- ============================================================

INSERT INTO ops.sync_rules (name, rule_type, pattern, description, priority)
VALUES
    (
        'Unum',
        'domain',
        'unum.com',
        'Unum — disability insurance / LTD claims correspondence',
        0
    )
ON CONFLICT (rule_type, pattern) DO NOTHING;
