-- Migration 016: ADA Tagging System — Hierarchical tags for ADA-related
-- communications, requests, and documentation across WA state agencies
-- Athena Cognitive Engine
-- Date: 2026-04-16

-- ============================================================
-- 1. EXTEND core.tags WITH HIERARCHY
-- ============================================================

-- Add parent_id for hierarchical tag trees (state > agency > category)
ALTER TABLE core.tags ADD COLUMN IF NOT EXISTS parent_id INT REFERENCES core.tags(id);
-- Add description for documentation
ALTER TABLE core.tags ADD COLUMN IF NOT EXISTS description TEXT;
-- Add slug for programmatic lookups (e.g. "wa:ada:dshs")
ALTER TABLE core.tags ADD COLUMN IF NOT EXISTS slug TEXT UNIQUE;

CREATE INDEX IF NOT EXISTS idx_tags_parent   ON core.tags(parent_id);
CREATE INDEX IF NOT EXISTS idx_tags_category ON core.tags(category);
CREATE INDEX IF NOT EXISTS idx_tags_slug     ON core.tags(slug);

-- ============================================================
-- 2. SEED ADA TAG TAXONOMY
-- ============================================================

-- Level 0: Top-level ADA tag
INSERT INTO core.tags (name, category, slug, description)
VALUES ('ADA', 'legal-framework', 'ada',
        'Americans with Disabilities Act — federal disability rights law')
ON CONFLICT (name) DO UPDATE SET slug = EXCLUDED.slug, description = EXCLUDED.description, category = EXCLUDED.category;

-- Level 1: Washington State ADA umbrella
INSERT INTO core.tags (name, category, slug, description, parent_id)
VALUES ('WA ADA', 'jurisdiction', 'wa:ada',
        'All ADA-related matters in Washington State — systemic analysis of government agency violations',
        (SELECT id FROM core.tags WHERE slug = 'ada'))
ON CONFLICT (name) DO UPDATE SET slug = EXCLUDED.slug, description = EXCLUDED.description, parent_id = EXCLUDED.parent_id;

-- Level 2: Individual WA agencies / entities
-- Each tied to the WA ADA parent for state-level rollup

INSERT INTO core.tags (name, category, slug, description, parent_id)
VALUES
  ('WA ADA — DSHS',
   'wa-agency', 'wa:ada:dshs',
   'Department of Social and Health Services — ADA accommodation requests, denials, violations',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — HBE / WA HealthPlanFinder',
   'wa-agency', 'wa:ada:hbe',
   'Health Benefit Exchange / WA HealthPlanFinder — ADA accommodation requests, denials, violations',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — Attorney General',
   'wa-agency', 'wa:ada:ag',
   'Washington Attorney General — ADA complaints, investigations, correspondence',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — BIIA',
   'wa-agency', 'wa:ada:biia',
   'Board of Industrial Insurance Appeals — ADA accommodation requests and proceedings',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — Snohomish County Superior Court',
   'wa-agency', 'wa:ada:snoco-court',
   'Snohomish County Superior Court — ADA accommodation requests, denials, violations in court proceedings',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — Snohomish County Prosecutors',
   'wa-agency', 'wa:ada:snoco-prosecutor',
   'Snohomish County Prosecuting Attorney — ADA violations in criminal proceedings',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — South County Fire',
   'wa-agency', 'wa:ada:south-county-fire',
   'South County Fire (South Snohomish Fire) — ADA violations in emergency response',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — L&I / Workers Comp',
   'wa-agency', 'wa:ada:lni',
   'Department of Labor & Industries — ADA accommodation in workers compensation proceedings',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — ESD / Employment Security',
   'wa-agency', 'wa:ada:esd',
   'Employment Security Department — ADA accommodation requests and violations',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — Starbucks',
   'wa-employer', 'wa:ada:starbucks',
   'Starbucks Corporation — ADA accommodation requests, denials, violations as employer',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — EvergreenHealth',
   'wa-healthcare', 'wa:ada:evergreenhealth',
   'EvergreenHealth Medical Center — ADA accommodation in healthcare delivery',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada')),

  ('WA ADA — UW Medicine',
   'wa-healthcare', 'wa:ada:uw-medicine',
   'UW Medicine — ADA accommodation in healthcare, inaccurate psychiatric diagnoses',
   (SELECT id FROM core.tags WHERE slug = 'wa:ada'))
ON CONFLICT (name) DO UPDATE
  SET slug = EXCLUDED.slug,
      description = EXCLUDED.description,
      parent_id = EXCLUDED.parent_id,
      category = EXCLUDED.category;

-- Level 3: Document/communication type sub-tags (applied alongside agency tags)
INSERT INTO core.tags (name, category, slug, description, parent_id)
VALUES
  ('ADA — Accommodation Request',
   'ada-doc-type', 'ada:type:request',
   'Formal or informal request for ADA accommodation',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Accommodation Denial',
   'ada-doc-type', 'ada:type:denial',
   'Denial or failure to provide requested ADA accommodation',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Complaint / Grievance',
   'ada-doc-type', 'ada:type:complaint',
   'Formal complaint or grievance regarding ADA violation',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Correspondence',
   'ada-doc-type', 'ada:type:correspondence',
   'General correspondence related to ADA matters',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Legal Filing',
   'ada-doc-type', 'ada:type:filing',
   'Court filing, motion, or legal document related to ADA claims',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Medical Documentation',
   'ada-doc-type', 'ada:type:medical',
   'Medical records supporting ADA accommodation need',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Policy / Procedure',
   'ada-doc-type', 'ada:type:policy',
   'Agency or entity ADA policies and procedures',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Retaliation',
   'ada-doc-type', 'ada:type:retaliation',
   'Evidence of retaliation for exercising ADA rights',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Interactive Process',
   'ada-doc-type', 'ada:type:interactive-process',
   'Interactive process communications (or failure to engage)',
   (SELECT id FROM core.tags WHERE slug = 'ada')),

  ('ADA — Systemic Violation',
   'ada-doc-type', 'ada:type:systemic',
   'Evidence of systemic or pattern-and-practice ADA violations',
   (SELECT id FROM core.tags WHERE slug = 'ada'))
ON CONFLICT (name) DO UPDATE
  SET slug = EXCLUDED.slug,
      description = EXCLUDED.description,
      parent_id = EXCLUDED.parent_id,
      category = EXCLUDED.category;


-- ============================================================
-- 3. AUTO-TAGGING RULES TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS core.tag_rules (
    id          SERIAL PRIMARY KEY,
    tag_id      INT NOT NULL REFERENCES core.tags(id) ON DELETE CASCADE,
    rule_type   TEXT NOT NULL,  -- domain, email_address, keyword, case_number, entity, sender_pattern
    pattern     TEXT NOT NULL,  -- regex or exact match depending on rule_type
    priority    INT DEFAULT 100,
    is_active   BOOLEAN DEFAULT TRUE,
    description TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tag_rules_active ON core.tag_rules(is_active, rule_type);

COMMENT ON TABLE core.tag_rules IS
  'Auto-tagging rules — when documents match a rule, the associated tag is automatically applied. '
  'Enables automatic classification of ADA-related documents by agency.';

-- ============================================================
-- 4. SEED AUTO-TAGGING RULES
-- ============================================================

-- DSHS
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'dshs.wa.gov', 'DSHS email domain'
FROM core.tags WHERE slug = 'wa:ada:dshs';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(dshs|department of social|social.and.health)\b', 'DSHS keyword match'
FROM core.tags WHERE slug = 'wa:ada:dshs';

-- HBE / WA HealthPlanFinder
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'wahbexchange.org', 'WAHBE email domain'
FROM core.tags WHERE slug = 'wa:ada:hbe';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(wahbe|healthplanfinder|health.benefit.exchange|hbe)\b', 'HBE keyword match'
FROM core.tags WHERE slug = 'wa:ada:hbe';

-- Attorney General
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'atg.wa.gov', 'WA AG email domain'
FROM core.tags WHERE slug = 'wa:ada:ag';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(attorney.general|ag.office|atg\.wa)\b', 'AG keyword match'
FROM core.tags WHERE slug = 'wa:ada:ag';

-- BIIA
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'biia.wa.gov', 'BIIA email domain'
FROM core.tags WHERE slug = 'wa:ada:biia';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(biia|board.of.industrial|industrial.insurance.appeals)\b', 'BIIA keyword match'
FROM core.tags WHERE slug = 'wa:ada:biia';

-- Snohomish County Superior Court
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'snoco.org', 'Snohomish County email domain'
FROM core.tags WHERE slug = 'wa:ada:snoco-court';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'co.snohomish.wa.us', 'Snohomish County alt domain'
FROM core.tags WHERE slug = 'wa:ada:snoco-court';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(snohomish.county.superior|snoco.superior|case.no.?\s*2[0-9]-2-\d{5}-31)\b', 'Snohomish Superior Court keyword'
FROM core.tags WHERE slug = 'wa:ada:snoco-court';

-- Snohomish County Prosecutors
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(snohomish.county.prosecut|snoco.prosecut|prosecuting.attorney.*snohomish)\b', 'Snohomish Prosecutor keyword'
FROM core.tags WHERE slug = 'wa:ada:snoco-prosecutor';

-- South County Fire
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'southsnofire.org', 'South County Fire email domain'
FROM core.tags WHERE slug = 'wa:ada:south-county-fire';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(south.county.fire|south.snohomish.fire|southsnofire)\b', 'South County Fire keyword'
FROM core.tags WHERE slug = 'wa:ada:south-county-fire';

-- L&I
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'lni.wa.gov', 'L&I email domain'
FROM core.tags WHERE slug = 'wa:ada:lni';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(labor.and.industries|l&i|lni|workers?.comp)\b', 'L&I keyword match'
FROM core.tags WHERE slug = 'wa:ada:lni';

-- ESD
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'esd.wa.gov', 'ESD email domain'
FROM core.tags WHERE slug = 'wa:ada:esd';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(employment.security|esd\.wa|unemployment)\b', 'ESD keyword match'
FROM core.tags WHERE slug = 'wa:ada:esd';

-- Starbucks
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'starbucks.com', 'Starbucks email domain'
FROM core.tags WHERE slug = 'wa:ada:starbucks';

-- EvergreenHealth
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'evergreenhealth.com', 'EvergreenHealth domain'
FROM core.tags WHERE slug = 'wa:ada:evergreenhealth';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'evergreenhealthcare.org', 'EvergreenHealth alt domain'
FROM core.tags WHERE slug = 'wa:ada:evergreenhealth';

-- UW Medicine
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'domain', 'uw.edu', 'UW email domain'
FROM core.tags WHERE slug = 'wa:ada:uw-medicine';

INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(uw.medicine|university.of.washington.medic|harborview|uwmc)\b', 'UW Medicine keyword'
FROM core.tags WHERE slug = 'wa:ada:uw-medicine';

-- ADA keyword → parent ADA tag (catches any ADA reference)
INSERT INTO core.tag_rules (tag_id, rule_type, pattern, description)
SELECT id, 'keyword', '(?i)\b(ada.accommodat|americans?.with.disabilit|disability.accommodat|reasonable.accommodat|ada.violation|ada.complaint|ada.request)\b',
       'ADA keyword — broad match for accommodation/disability language'
FROM core.tags WHERE slug = 'wa:ada';


-- ============================================================
-- 5. VIEWS FOR EASY QUERYING
-- ============================================================

-- All tags with their full hierarchy path
CREATE OR REPLACE VIEW core.v_tag_hierarchy AS
WITH RECURSIVE tag_tree AS (
    SELECT id, name, slug, category, description, parent_id,
           name AS path, slug AS slug_path, 0 AS depth
    FROM core.tags WHERE parent_id IS NULL
    UNION ALL
    SELECT t.id, t.name, t.slug, t.category, t.description, t.parent_id,
           tt.path || ' > ' || t.name, tt.slug_path || '/' || t.slug, tt.depth + 1
    FROM core.tags t
    JOIN tag_tree tt ON t.parent_id = tt.id
)
SELECT * FROM tag_tree ORDER BY path;

-- Documents with their ADA tags (for easy filtering)
CREATE OR REPLACE VIEW core.v_ada_documents AS
SELECT DISTINCT
    d.id AS document_id,
    d.title,
    d.domain,
    d.document_type,
    d.filename,
    d.created_at,
    d.metadata,
    t.name AS tag_name,
    t.slug AS tag_slug,
    t.category AS tag_category,
    parent.slug AS parent_tag_slug,
    parent.name AS parent_tag_name,
    lc.case_number,
    lc.case_title,
    em.sender,
    em.direction,
    em.date_sent
FROM core.document_tags dt
JOIN core.tags t ON dt.tag_id = t.id
JOIN core.documents d ON dt.document_id = d.id
LEFT JOIN core.tags parent ON t.parent_id = parent.id
LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
LEFT JOIN legal.email_metadata em ON d.id = em.document_id
WHERE t.slug LIKE 'wa:ada%'
   OR t.slug LIKE 'ada:%'
   OR t.slug = 'ada';

-- Per-agency document counts for systemic analysis
CREATE OR REPLACE VIEW core.v_ada_agency_summary AS
SELECT
    t.name AS agency,
    t.slug,
    t.description,
    COUNT(DISTINCT dt.document_id) AS document_count,
    MIN(d.created_at) AS earliest_document,
    MAX(d.created_at) AS latest_document
FROM core.tags t
LEFT JOIN core.document_tags dt ON t.id = dt.tag_id
LEFT JOIN core.documents d ON dt.document_id = d.id
WHERE t.category IN ('wa-agency', 'wa-employer', 'wa-healthcare')
GROUP BY t.id, t.name, t.slug, t.description
ORDER BY document_count DESC;

-- Systemic overview: all WA ADA documents across all agencies
CREATE OR REPLACE VIEW core.v_ada_systemic_overview AS
SELECT
    t.name AS agency_or_entity,
    t.slug AS tag_slug,
    t.category,
    dtype.name AS doc_type_tag,
    dtype.slug AS doc_type_slug,
    d.id AS document_id,
    d.title,
    d.domain,
    d.document_type,
    d.created_at,
    em.sender,
    em.direction,
    em.date_sent,
    lc.case_number
FROM core.document_tags dt
JOIN core.tags t ON dt.tag_id = t.id
JOIN core.documents d ON dt.document_id = d.id
LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
LEFT JOIN legal.email_metadata em ON d.id = em.document_id
-- Also join any doc-type tags on the same document
LEFT JOIN core.document_tags dt2 ON d.id = dt2.document_id AND dt2.tag_id != dt.tag_id
LEFT JOIN core.tags dtype ON dt2.tag_id = dtype.id AND dtype.category = 'ada-doc-type'
WHERE t.parent_id = (SELECT id FROM core.tags WHERE slug = 'wa:ada')
ORDER BY d.created_at DESC;


-- ============================================================
-- 6. HELPER FUNCTIONS
-- ============================================================

-- Get all descendant tag IDs for a given slug (for recursive queries)
CREATE OR REPLACE FUNCTION core.get_tag_descendants(root_slug TEXT)
RETURNS TABLE(tag_id INT, tag_slug TEXT, depth INT) AS $$
WITH RECURSIVE descendants AS (
    SELECT id AS tag_id, slug AS tag_slug, 0 AS depth
    FROM core.tags WHERE slug = root_slug
    UNION ALL
    SELECT t.id, t.slug, d.depth + 1
    FROM core.tags t
    JOIN descendants d ON t.parent_id = d.tag_id
)
SELECT * FROM descendants;
$$ LANGUAGE sql STABLE;

-- Tag a document (idempotent — skips if already tagged)
CREATE OR REPLACE FUNCTION core.tag_document(
    p_document_id UUID,
    p_tag_slug TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_tag_id INT;
BEGIN
    SELECT id INTO v_tag_id FROM core.tags WHERE slug = p_tag_slug;
    IF v_tag_id IS NULL THEN
        RAISE WARNING 'Tag slug % not found', p_tag_slug;
        RETURN FALSE;
    END IF;
    INSERT INTO core.document_tags (document_id, tag_id)
    VALUES (p_document_id, v_tag_id)
    ON CONFLICT DO NOTHING;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- Tag a document with multiple tags at once
CREATE OR REPLACE FUNCTION core.tag_document_multi(
    p_document_id UUID,
    p_tag_slugs TEXT[]
) RETURNS INT AS $$
DECLARE
    v_count INT := 0;
    v_slug TEXT;
BEGIN
    FOREACH v_slug IN ARRAY p_tag_slugs LOOP
        IF core.tag_document(p_document_id, v_slug) THEN
            v_count := v_count + 1;
        END IF;
    END LOOP;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Search documents by tag slug (includes all descendants)
CREATE OR REPLACE FUNCTION core.search_by_tag(
    p_tag_slug TEXT,
    p_limit INT DEFAULT 100,
    p_offset INT DEFAULT 0
) RETURNS TABLE(
    document_id UUID,
    title TEXT,
    domain TEXT,
    document_type TEXT,
    filename TEXT,
    created_at TIMESTAMPTZ,
    tag_names TEXT[],
    case_number TEXT,
    sender TEXT,
    direction TEXT,
    date_sent TIMESTAMPTZ
) AS $$
SELECT
    d.id AS document_id,
    d.title,
    d.domain,
    d.document_type,
    d.filename,
    d.created_at,
    array_agg(DISTINCT t.name) AS tag_names,
    lc.case_number,
    em.sender,
    em.direction,
    em.date_sent
FROM core.documents d
JOIN core.document_tags dt ON d.id = dt.document_id
JOIN core.tags t ON dt.tag_id = t.id
LEFT JOIN legal.case_documents lcd ON d.id = lcd.document_id
LEFT JOIN legal.cases lc ON lcd.case_id = lc.id
LEFT JOIN legal.email_metadata em ON d.id = em.document_id
WHERE dt.tag_id IN (SELECT tag_id FROM core.get_tag_descendants(p_tag_slug))
GROUP BY d.id, d.title, d.domain, d.document_type, d.filename, d.created_at,
         lc.case_number, em.sender, em.direction, em.date_sent
ORDER BY d.created_at DESC
LIMIT p_limit OFFSET p_offset;
$$ LANGUAGE sql STABLE;

-- Auto-tag a document based on core.tag_rules
CREATE OR REPLACE FUNCTION core.auto_tag_document(p_document_id UUID)
RETURNS TEXT[] AS $$
DECLARE
    v_doc RECORD;
    v_rule RECORD;
    v_text TEXT;
    v_applied TEXT[] := '{}';
BEGIN
    -- Gather document text for matching
    SELECT
        d.title,
        d.filename,
        d.full_content,
        d.metadata,
        em.sender,
        em.recipients,
        em.mailbox
    INTO v_doc
    FROM core.documents d
    LEFT JOIN legal.email_metadata em ON d.id = em.document_id
    WHERE d.id = p_document_id;

    IF NOT FOUND THEN RETURN v_applied; END IF;

    -- Concatenate all searchable text
    v_text := coalesce(v_doc.title, '') || ' ' ||
              coalesce(v_doc.filename, '') || ' ' ||
              coalesce(v_doc.full_content, '') || ' ' ||
              coalesce(v_doc.sender, '') || ' ' ||
              coalesce(v_doc.recipients::text, '') || ' ' ||
              coalesce(v_doc.mailbox, '');

    FOR v_rule IN
        SELECT tr.tag_id, tr.rule_type, tr.pattern, t.slug
        FROM core.tag_rules tr
        JOIN core.tags t ON tr.tag_id = t.id
        WHERE tr.is_active = TRUE
        ORDER BY tr.priority
    LOOP
        IF v_rule.rule_type = 'domain' THEN
            -- Match email sender/recipient domains
            IF v_text ~* ('@' || v_rule.pattern) OR v_text ~* (v_rule.pattern) THEN
                INSERT INTO core.document_tags (document_id, tag_id)
                VALUES (p_document_id, v_rule.tag_id)
                ON CONFLICT DO NOTHING;
                v_applied := v_applied || v_rule.slug;
            END IF;
        ELSIF v_rule.rule_type = 'keyword' THEN
            -- Regex keyword match
            IF v_text ~ v_rule.pattern THEN
                INSERT INTO core.document_tags (document_id, tag_id)
                VALUES (p_document_id, v_rule.tag_id)
                ON CONFLICT DO NOTHING;
                v_applied := v_applied || v_rule.slug;
            END IF;
        ELSIF v_rule.rule_type = 'case_number' THEN
            IF v_text ~* v_rule.pattern THEN
                INSERT INTO core.document_tags (document_id, tag_id)
                VALUES (p_document_id, v_rule.tag_id)
                ON CONFLICT DO NOTHING;
                v_applied := v_applied || v_rule.slug;
            END IF;
        END IF;
    END LOOP;

    -- If any agency-level tag was applied, also apply the parent WA ADA tag
    IF array_length(v_applied, 1) > 0 THEN
        INSERT INTO core.document_tags (document_id, tag_id)
        SELECT p_document_id, id FROM core.tags WHERE slug = 'wa:ada'
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN v_applied;
END;
$$ LANGUAGE plpgsql;

-- Batch auto-tag: run auto_tag_document on all untagged documents
CREATE OR REPLACE FUNCTION core.auto_tag_all_untagged(p_limit INT DEFAULT 1000)
RETURNS TABLE(document_id UUID, tags_applied TEXT[]) AS $$
    SELECT d.id, core.auto_tag_document(d.id)
    FROM core.documents d
    WHERE NOT EXISTS (
        SELECT 1 FROM core.document_tags dt
        JOIN core.tags t ON dt.tag_id = t.id
        WHERE dt.document_id = d.id AND t.slug LIKE 'wa:ada%'
    )
    LIMIT p_limit;
$$ LANGUAGE sql;
