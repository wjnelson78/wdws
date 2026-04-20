-- Migration 017: Document Domain Registry
-- Athena Cognitive Engine
-- Date: 2026-04-16
--
-- Replaces the hard-coded documents_domain_check constraint with a registry-backed
-- domain table so new document domains (coding, research, etc.) can be added
-- without touching application code or altering CHECK constraints each time.

BEGIN;

CREATE TABLE IF NOT EXISTS core.document_domains (
    name        TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    description TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    sort_order  INT NOT NULL DEFAULT 100,
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

COMMENT ON TABLE core.document_domains IS
  'Registry of valid document domains for core.documents. Domains are now data-driven instead of hard-coded in a CHECK constraint.';

CREATE INDEX IF NOT EXISTS idx_document_domains_active_order
    ON core.document_domains (is_active, sort_order, name);

INSERT INTO core.document_domains (name, label, description, sort_order)
VALUES
    ('general', 'General', 'General notes, summaries, memos, and other catch-all documents.', 10),
    ('legal', 'Legal', 'Court filings, legal correspondence, case analysis, and litigation work product.', 20),
    ('medical', 'Medical', 'Medical records, care notes, treatment summaries, and health documentation.', 30),
    ('paperless', 'Paperless', 'Documents synchronized from Paperless-ngx or equivalent archive workflows.', 40),
    ('web', 'Web', 'Web-crawled pages, website captures, and online reference material.', 50),
    ('coding', 'Coding', 'Code notes, architecture docs, implementation plans, debugging writeups, and engineering artifacts.', 60),
    ('research', 'Research', 'Background research, source digests, investigation summaries, and exploratory notes.', 70),
    ('business', 'Business', 'Business plans, strategy documents, vendor notes, and operational paperwork.', 80),
    ('finance', 'Finance', 'Financial analyses, invoices, budgets, statements, and related supporting material.', 90),
    ('personal', 'Personal', 'Personal reference material, household notes, and non-business miscellaneous documents.', 100),
    ('operations', 'Operations', 'Runbooks, process docs, service notes, and administrative operations artifacts.', 110)
ON CONFLICT (name) DO UPDATE SET
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    sort_order = EXCLUDED.sort_order,
    is_active = true,
    updated_at = now();

ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_domain_check;
ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_domain_fk;
ALTER TABLE core.documents
    ADD CONSTRAINT documents_domain_fk
    FOREIGN KEY (domain) REFERENCES core.document_domains(name);

COMMIT;
