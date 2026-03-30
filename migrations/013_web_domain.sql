-- ============================================================
-- Migration 013: Web Domain Support
--
-- Adds 'web' domain to core.documents and creates web schema
-- for storing crawled website content (playfasa.com, etc.)
-- ============================================================

-- 1. Add 'web' to the domain CHECK constraint
ALTER TABLE core.documents DROP CONSTRAINT IF EXISTS documents_domain_check;
ALTER TABLE core.documents ADD CONSTRAINT documents_domain_check
    CHECK (domain = ANY (ARRAY['legal', 'medical', 'paperless', 'web']));

-- 2. Web schema
CREATE SCHEMA IF NOT EXISTS web;

-- 3. Sites table — each website we crawl
CREATE TABLE IF NOT EXISTS web.sites (
    id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain      text NOT NULL UNIQUE,          -- e.g. 'playfasa.com'
    base_url    text NOT NULL,                 -- e.g. 'https://playfasa.com'
    label       text,                          -- friendly name
    is_active   boolean NOT NULL DEFAULT true,
    crawl_config jsonb NOT NULL DEFAULT '{}'::jsonb,  -- depth, rate limit, etc.
    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now()
);

-- 4. Page metadata — web-specific enrichment for each crawled page
CREATE TABLE IF NOT EXISTS web.page_metadata (
    document_id  uuid PRIMARY KEY REFERENCES core.documents(id) ON DELETE CASCADE,
    site_id      uuid REFERENCES web.sites(id) ON DELETE CASCADE,
    url          text NOT NULL,
    url_path     text,                         -- '/about.asp'
    http_status  integer,
    content_type text,                         -- 'text/html', 'application/pdf'
    crawled_at   timestamptz NOT NULL DEFAULT now(),
    last_modified timestamptz,                 -- from HTTP header
    content_length integer,
    link_count   integer DEFAULT 0,
    metadata     jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_page_metadata_site ON web.page_metadata(site_id);
CREATE INDEX IF NOT EXISTS idx_page_metadata_url ON web.page_metadata(url);

-- 5. Seed playfasa.com
INSERT INTO web.sites (domain, base_url, label, crawl_config)
VALUES ('playfasa.com', 'https://playfasa.com', 'Fastpitch America Softball Association',
        '{"max_depth": 3, "rate_limit_seconds": 1, "include_pdfs": true}'::jsonb)
ON CONFLICT (domain) DO NOTHING;
