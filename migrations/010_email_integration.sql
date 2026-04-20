-- ============================================================
-- O365 Email Integration: Global Index + Space Linking + RAG
-- Migration: 010_email_integration.sql
-- Database: athena_chat
--
-- Architecture (Option C — Hybrid):
--   Layer 1: Global email index (email_accounts, email_messages,
--            email_attachments, email_message_chunks)
--            Synced by email_sync_chat.py via Graph API (app-only)
--   Layer 2: Space linking (space_email_links)
--            Link mailboxes or individual emails to spaces
--   Layer 3: AI enrichment (email_ai_classifications)
--            Urgency, case linking, deadline extraction
--
-- The Athena Teams app connects DIRECTLY to Graph API for
-- real-time UX (delegated auth), while this server-side layer
-- handles background sync, embedding, RAG, and AI enrichment.
-- ============================================================

-- ============================================================
-- EMAIL ACCOUNTS (registered mailboxes for Graph API sync)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_accounts (
    id              BIGSERIAL PRIMARY KEY,
    
    -- Mailbox identity
    email_address   TEXT NOT NULL UNIQUE,
    display_name    TEXT,
    user_principal  TEXT,                    -- UPN if different from email
    
    -- Graph API resolution
    graph_user_id   TEXT,                    -- resolved Graph user ID
    
    -- Sync configuration
    is_active       BOOLEAN DEFAULT true,
    sync_folders    TEXT[] DEFAULT ARRAY['inbox', 'sentitems'],
    sync_domains    TEXT[],                  -- NULL = all, or ['snoco.org','co.snohomish.wa.us']
    sync_since      TIMESTAMPTZ,            -- NULL = last 90 days
    max_messages    INT DEFAULT 10000,       -- per sync run
    
    -- Delta sync state
    delta_links     JSONB DEFAULT '{}'::jsonb,  -- folder_name → delta link
    last_sync_at    TIMESTAMPTZ,
    last_sync_count INT DEFAULT 0,
    last_sync_error TEXT,
    
    -- Stats
    total_messages  INT DEFAULT 0,
    total_indexed   INT DEFAULT 0,
    total_bytes     BIGINT DEFAULT 0,
    
    -- Metadata
    label           TEXT,                    -- friendly name: "William's O365"
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TRIGGER trg_email_accounts_updated
    BEFORE UPDATE ON chat.email_accounts
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

-- ============================================================
-- EMAIL MESSAGES (every synced email)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_messages (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id          BIGINT NOT NULL REFERENCES chat.email_accounts(id) ON DELETE CASCADE,
    
    -- Graph API identifiers
    graph_message_id    TEXT NOT NULL,        -- Graph message ID (immutable)
    graph_conversation_id TEXT,               -- Graph conversationId (thread grouping)
    internet_message_id TEXT,                 -- RFC 2822 Message-ID header
    
    -- Thread tracking
    thread_id           TEXT,                 -- our normalized thread ID
    in_reply_to         TEXT,                 -- Message-ID of parent
    
    -- Envelope
    subject             TEXT,
    sender_email        TEXT,
    sender_name         TEXT,
    to_recipients       JSONB DEFAULT '[]'::jsonb,   -- [{email, name}]
    cc_recipients       JSONB DEFAULT '[]'::jsonb,
    bcc_recipients      JSONB DEFAULT '[]'::jsonb,
    reply_to            JSONB DEFAULT '[]'::jsonb,
    
    -- Direction
    direction           TEXT NOT NULL DEFAULT 'inbound'
                        CHECK (direction IN ('inbound', 'outbound')),
    folder_name         TEXT,                -- inbox, sentitems, drafts, etc.
    
    -- Content
    body_preview        TEXT,                -- Graph bodyPreview (max 255 chars)
    body_text           TEXT,                -- full plain-text body
    body_html           TEXT,                -- full HTML body (optional, for rendering)
    content_hash        TEXT,                -- SHA-256 of body for dedup
    
    -- Flags
    is_read             BOOLEAN DEFAULT false,
    is_flagged          BOOLEAN DEFAULT false,
    is_draft            BOOLEAN DEFAULT false,
    importance          TEXT DEFAULT 'normal'
                        CHECK (importance IN ('low', 'normal', 'high')),
    has_attachments     BOOLEAN DEFAULT false,
    
    -- Categories/labels from O365
    categories          TEXT[],
    
    -- Graph timestamps
    received_at         TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    graph_created_at    TIMESTAMPTZ,
    graph_modified_at   TIMESTAMPTZ,
    
    -- Web link
    web_link            TEXT,                -- Outlook web URL
    
    -- Processing state
    status              TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'chunking', 'embedding',
                                          'indexed', 'failed', 'skipped')),
    processing_error    TEXT,
    
    -- Our timestamps
    first_seen_at       TIMESTAMPTZ DEFAULT now(),
    last_synced_at      TIMESTAMPTZ DEFAULT now(),
    indexed_at          TIMESTAMPTZ,
    
    metadata            JSONB DEFAULT '{}'::jsonb,
    
    UNIQUE (account_id, graph_message_id)
);

-- Core lookup indexes
CREATE INDEX IF NOT EXISTS idx_email_messages_account
    ON chat.email_messages(account_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_graph_id
    ON chat.email_messages(graph_message_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_inet_id
    ON chat.email_messages(internet_message_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_thread
    ON chat.email_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_conversation
    ON chat.email_messages(graph_conversation_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_sender
    ON chat.email_messages(sender_email);
CREATE INDEX IF NOT EXISTS idx_email_messages_received
    ON chat.email_messages(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_email_messages_status
    ON chat.email_messages(status);
CREATE INDEX IF NOT EXISTS idx_email_messages_direction
    ON chat.email_messages(direction);
CREATE INDEX IF NOT EXISTS idx_email_messages_folder
    ON chat.email_messages(folder_name);
CREATE INDEX IF NOT EXISTS idx_email_messages_importance
    ON chat.email_messages(importance) WHERE importance != 'normal';
CREATE INDEX IF NOT EXISTS idx_email_messages_flagged
    ON chat.email_messages(is_flagged) WHERE is_flagged = true;
CREATE INDEX IF NOT EXISTS idx_email_messages_hash
    ON chat.email_messages(content_hash);

-- Full-text search
CREATE INDEX IF NOT EXISTS idx_email_messages_fts
    ON chat.email_messages USING GIN(
        to_tsvector('english',
            coalesce(subject, '') || ' ' ||
            coalesce(sender_name, '') || ' ' ||
            coalesce(sender_email, '') || ' ' ||
            coalesce(body_text, '')
        )
    );

-- Trigram for fuzzy subject search
CREATE INDEX IF NOT EXISTS idx_email_messages_subject_trgm
    ON chat.email_messages USING GIN(subject gin_trgm_ops);

-- ============================================================
-- EMAIL ATTACHMENTS
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_attachments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      UUID NOT NULL REFERENCES chat.email_messages(id) ON DELETE CASCADE,
    
    -- Graph identifiers
    graph_attachment_id TEXT,
    
    -- File metadata
    file_name       TEXT NOT NULL,
    content_type    TEXT,
    size_bytes      BIGINT,
    is_inline       BOOLEAN DEFAULT false,
    content_id      TEXT,                    -- for inline images
    
    -- Extracted content
    content_text    TEXT,                    -- extracted text (PDF/DOCX/etc.)
    extraction_method TEXT,                  -- 'docx', 'pdf', 'ocr', 'plain', 'xlsx'
    
    -- Storage
    storage_path    TEXT,                    -- local file path if saved
    sha256          TEXT,
    
    -- Processing
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'downloaded', 'extracted', 
                                       'indexed', 'failed', 'skipped')),
    processing_error TEXT,
    
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_email_attachments_message
    ON chat.email_attachments(message_id);
CREATE INDEX IF NOT EXISTS idx_email_attachments_name_trgm
    ON chat.email_attachments USING GIN(file_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_email_attachments_type
    ON chat.email_attachments(content_type);

-- ============================================================
-- EMAIL MESSAGE CHUNKS (chunked for embeddings)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_message_chunks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      UUID NOT NULL REFERENCES chat.email_messages(id) ON DELETE CASCADE,
    chunk_index     INT NOT NULL,
    chunk_text      TEXT NOT NULL,
    
    -- What this chunk contains
    chunk_source    TEXT NOT NULL DEFAULT 'body'
                    CHECK (chunk_source IN ('body', 'attachment', 'header')),
    attachment_id   UUID REFERENCES chat.email_attachments(id) ON DELETE CASCADE,
    
    -- Chunk boundaries
    start_offset    INT,
    end_offset      INT,
    token_count     INT,
    
    -- Embedding
    embedding       halfvec(3072),
    
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE (message_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_email_chunks_message
    ON chat.email_message_chunks(message_id);

-- HNSW vector index for semantic search
CREATE INDEX IF NOT EXISTS idx_email_chunks_embedding_hnsw
    ON chat.email_message_chunks
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Full-text search on chunk content
CREATE INDEX IF NOT EXISTS idx_email_chunks_fts
    ON chat.email_message_chunks USING GIN(
        to_tsvector('english', chunk_text)
    );

-- ============================================================
-- SPACE ↔ EMAIL LINKING
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_email_links (
    id              BIGSERIAL PRIMARY KEY,
    space_id        UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    
    -- Link an entire account or individual message
    account_id      BIGINT REFERENCES chat.email_accounts(id) ON DELETE CASCADE,
    message_id      UUID REFERENCES chat.email_messages(id) ON DELETE CASCADE,
    
    -- Optional: link by domain filter (e.g., link all snoco.org emails)
    domain_filter   TEXT,
    -- Optional: link by sender filter
    sender_filter   TEXT,
    -- Optional: link by subject/keyword filter
    keyword_filter  TEXT,
    
    -- Who linked it
    linked_by       BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    linked_at       TIMESTAMPTZ DEFAULT now(),
    
    -- At least account or message must be set
    CHECK (account_id IS NOT NULL OR message_id IS NOT NULL),
    
    -- Prevent duplicate message links
    UNIQUE (space_id, message_id)
);

-- Prevent duplicate account+filter links (uses COALESCE for nullable columns)
CREATE UNIQUE INDEX IF NOT EXISTS idx_space_email_links_account_unique
    ON chat.space_email_links (
        space_id, account_id, 
        COALESCE(domain_filter, ''), 
        COALESCE(sender_filter, ''), 
        COALESCE(keyword_filter, '')
    )
    WHERE account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_space_email_links_space
    ON chat.space_email_links(space_id);
CREATE INDEX IF NOT EXISTS idx_space_email_links_account
    ON chat.space_email_links(account_id);
CREATE INDEX IF NOT EXISTS idx_space_email_links_message
    ON chat.space_email_links(message_id);

-- ============================================================
-- AI CLASSIFICATIONS (urgency, case linking, deadlines)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_ai_classifications (
    id              BIGSERIAL PRIMARY KEY,
    message_id      UUID NOT NULL REFERENCES chat.email_messages(id) ON DELETE CASCADE,
    
    -- Classification results
    urgency         TEXT CHECK (urgency IN ('urgent', 'high', 'normal', 'low')),
    category        TEXT,                    -- court-order, filing, discovery, etc.
    
    -- Case linking
    case_number     TEXT,
    case_confidence FLOAT,
    
    -- Deadlines
    deadlines       JSONB DEFAULT '[]'::jsonb,  -- [{date, description}]
    
    -- Action
    requires_action BOOLEAN DEFAULT false,
    action_summary  TEXT,
    
    -- AI reasoning
    reasoning       TEXT,
    model_used      TEXT,
    
    -- Feedback loop
    user_override   TEXT,                    -- if user corrected the classification
    overridden_by   BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    overridden_at   TIMESTAMPTZ,
    
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE (message_id)
);

CREATE INDEX IF NOT EXISTS idx_email_ai_class_message
    ON chat.email_ai_classifications(message_id);
CREATE INDEX IF NOT EXISTS idx_email_ai_class_urgency
    ON chat.email_ai_classifications(urgency);
CREATE INDEX IF NOT EXISTS idx_email_ai_class_case
    ON chat.email_ai_classifications(case_number);
CREATE INDEX IF NOT EXISTS idx_email_ai_class_action
    ON chat.email_ai_classifications(requires_action) WHERE requires_action = true;
CREATE INDEX IF NOT EXISTS idx_email_ai_class_deadlines
    ON chat.email_ai_classifications USING GIN(deadlines);

CREATE TRIGGER trg_email_ai_class_updated
    BEFORE UPDATE ON chat.email_ai_classifications
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();

-- ============================================================
-- SYNC HISTORY (audit trail)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT REFERENCES chat.email_accounts(id) ON DELETE CASCADE,
    
    trigger_type    TEXT NOT NULL DEFAULT 'scheduled'
                    CHECK (trigger_type IN ('scheduled', 'manual', 'webhook', 'initial')),
    folder_name     TEXT,                    -- which folder was synced
    
    started_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    
    -- Stats
    messages_fetched    INT DEFAULT 0,
    messages_new        INT DEFAULT 0,
    messages_updated    INT DEFAULT 0,
    messages_skipped    INT DEFAULT 0,
    messages_failed     INT DEFAULT 0,
    chunks_created      INT DEFAULT 0,
    embeddings_created  INT DEFAULT 0,
    embedding_tokens    INT DEFAULT 0,
    
    duration_seconds    FLOAT,
    error_message       TEXT,
    
    -- Delta tracking
    delta_link_before   TEXT,
    delta_link_after    TEXT
);

CREATE INDEX IF NOT EXISTS idx_email_sync_runs_account
    ON chat.email_sync_runs(account_id);
CREATE INDEX IF NOT EXISTS idx_email_sync_runs_started
    ON chat.email_sync_runs(started_at DESC);

-- ============================================================
-- GRAPH WEBHOOK SUBSCRIPTIONS (for near-real-time push)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.email_webhooks (
    id              BIGSERIAL PRIMARY KEY,
    account_id      BIGINT REFERENCES chat.email_accounts(id) ON DELETE CASCADE,
    
    subscription_id TEXT NOT NULL UNIQUE,     -- Graph subscription ID
    resource        TEXT NOT NULL,            -- e.g., /users/{id}/messages
    change_type     TEXT NOT NULL DEFAULT 'created,updated',
    
    -- Lifecycle
    expiration_at   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    renewed_at      TIMESTAMPTZ,
    
    is_active       BOOLEAN DEFAULT true
);

-- ============================================================
-- CONVENIENCE VIEWS
-- ============================================================

-- View: Email messages linked to a space (via account link or direct)
CREATE OR REPLACE VIEW chat.v_space_emails AS
SELECT
    sel.space_id,
    em.id AS message_id,
    em.subject,
    em.sender_email,
    em.sender_name,
    em.to_recipients,
    em.cc_recipients,
    em.direction,
    em.folder_name,
    em.body_preview,
    em.has_attachments,
    em.importance,
    em.is_flagged,
    em.is_read,
    em.received_at,
    em.sent_at,
    em.web_link,
    em.status AS sync_status,
    aic.urgency,
    aic.category,
    aic.case_number,
    aic.requires_action,
    aic.action_summary,
    aic.deadlines,
    sel.linked_by,
    sel.linked_at,
    'message' AS link_type
FROM chat.space_email_links sel
JOIN chat.email_messages em ON em.id = sel.message_id
LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
WHERE sel.message_id IS NOT NULL
UNION ALL
SELECT
    sel.space_id,
    em.id AS message_id,
    em.subject,
    em.sender_email,
    em.sender_name,
    em.to_recipients,
    em.cc_recipients,
    em.direction,
    em.folder_name,
    em.body_preview,
    em.has_attachments,
    em.importance,
    em.is_flagged,
    em.is_read,
    em.received_at,
    em.sent_at,
    em.web_link,
    em.status AS sync_status,
    aic.urgency,
    aic.category,
    aic.case_number,
    aic.requires_action,
    aic.action_summary,
    aic.deadlines,
    sel.linked_by,
    sel.linked_at,
    'account' AS link_type
FROM chat.space_email_links sel
JOIN chat.email_messages em ON em.account_id = sel.account_id
LEFT JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
WHERE sel.account_id IS NOT NULL
  AND sel.message_id IS NULL
  AND (sel.domain_filter IS NULL OR em.sender_email ILIKE '%' || sel.domain_filter)
  AND (sel.sender_filter IS NULL OR em.sender_email ILIKE '%' || sel.sender_filter || '%')
  AND (sel.keyword_filter IS NULL OR em.subject ILIKE '%' || sel.keyword_filter || '%');

-- View: Email sync overview
CREATE OR REPLACE VIEW chat.v_email_sync_status AS
SELECT
    a.id AS account_id,
    a.label,
    a.email_address,
    a.is_active,
    a.sync_folders,
    a.total_messages,
    a.total_indexed,
    pg_size_pretty(a.total_bytes) AS total_size,
    a.last_sync_at,
    a.last_sync_count,
    a.last_sync_error,
    (SELECT COUNT(*) FROM chat.email_messages m 
     WHERE m.account_id = a.id AND m.status = 'indexed') AS current_indexed,
    (SELECT COUNT(*) FROM chat.email_messages m 
     WHERE m.account_id = a.id AND m.status = 'failed') AS current_failed,
    (SELECT COUNT(*) FROM chat.email_messages m 
     WHERE m.account_id = a.id AND m.status = 'pending') AS current_pending,
    (SELECT COUNT(*) FROM chat.space_email_links sel 
     WHERE sel.account_id = a.id) AS linked_to_spaces,
    (SELECT COUNT(*) FROM chat.email_ai_classifications aic
     JOIN chat.email_messages m ON m.id = aic.message_id
     WHERE m.account_id = a.id AND aic.urgency = 'urgent') AS urgent_count
FROM chat.email_accounts a;

-- View: Emails needing attention (urgent + action required)
CREATE OR REPLACE VIEW chat.v_email_action_required AS
SELECT
    em.id AS message_id,
    em.subject,
    em.sender_email,
    em.sender_name,
    em.received_at,
    em.web_link,
    aic.urgency,
    aic.category,
    aic.case_number,
    aic.action_summary,
    aic.deadlines,
    a.email_address AS mailbox
FROM chat.email_messages em
JOIN chat.email_accounts a ON a.id = em.account_id
JOIN chat.email_ai_classifications aic ON aic.message_id = em.id
WHERE aic.requires_action = true
ORDER BY 
    CASE aic.urgency 
        WHEN 'urgent' THEN 1 
        WHEN 'high' THEN 2 
        WHEN 'normal' THEN 3 
        ELSE 4 
    END,
    em.received_at DESC;

-- ============================================================
-- SEED DATA: Register existing mailboxes
-- ============================================================
INSERT INTO chat.email_accounts (email_address, display_name, label, sync_folders, sync_domains)
VALUES 
    ('william@seattleseahawks.me', 'William Nelson', 'William O365',
     ARRAY['inbox', 'sentitems'],
    ARRAY['snoco.org', 'co.snohomish.wa.us', 'co.chelan.wa.us']),
    ('athena@seattleseahawks.me', 'Athena AI', 'Athena O365',
     ARRAY['inbox', 'sentitems'],
    ARRAY['snoco.org', 'co.snohomish.wa.us', 'co.chelan.wa.us'])
ON CONFLICT (email_address) DO NOTHING;

-- ============================================================
-- COMMENTS
-- ============================================================
COMMENT ON TABLE chat.email_accounts IS 
    'O365 mailboxes registered for Graph API sync. Server uses app-only auth; app uses delegated auth.';
COMMENT ON TABLE chat.email_messages IS 
    'Global email index synced from O365 via Graph API. Body text + embeddings for RAG.';
COMMENT ON TABLE chat.email_attachments IS 
    'Email attachments with extracted text content for search.';
COMMENT ON TABLE chat.email_message_chunks IS 
    'Chunked email content with pgvector embeddings for semantic search.';
COMMENT ON TABLE chat.space_email_links IS 
    'Links emails to spaces — by account (all mail), by message, or by filter.';
COMMENT ON TABLE chat.email_ai_classifications IS 
    'AI-generated urgency/category/case/deadline classifications per email.';
COMMENT ON VIEW chat.v_space_emails IS 
    'Emails visible within a space based on linking rules, with AI enrichment.';
COMMENT ON VIEW chat.v_email_action_required IS 
    'Emails flagged by AI as requiring action, ordered by urgency.';
