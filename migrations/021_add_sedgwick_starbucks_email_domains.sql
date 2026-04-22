-- ============================================================
-- Migration: 021_add_sedgwick_starbucks_email_domains.sql
-- Database: wdws
--
-- Ensure Sedgwick (claims management) and Starbucks (employer /
-- corp counsel) email traffic is included in Athena's enterprise
-- Graph sync rules for William and Athena mailboxes.
-- ============================================================

INSERT INTO ops.sync_rules (name, rule_type, pattern, description, priority)
VALUES
    (
        'Sedgwick Claims Management',
        'domain',
        'sedgwick.com',
        'Sedgwick claims management — workers'' comp / disability correspondence',
        0
    ),
    (
        'Starbucks',
        'domain',
        'starbucks.com',
        'Starbucks employer / corporate counsel correspondence',
        0
    )
ON CONFLICT (rule_type, pattern) DO NOTHING;
