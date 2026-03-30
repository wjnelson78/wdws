-- ============================================================
-- OneDrive Integration: Global Index + Space Linking
-- Migration: 009_onedrive_integration.sql
-- Database: athena_chat
--
-- Architecture:
--   Layer 1: Global OneDrive file index (onedrive_files + onedrive_file_chunks)
--            Synced by onedrive_sync.py via Graph API client credentials
--   Layer 2: Space linking (space_onedrive_links)
--            Instant link — no duplicate embeddings
-- ============================================================

-- ============================================================
-- SYNC CONFIGURATION (which drives/folders to sync)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.onedrive_sync_targets (
    id              BIGSERIAL PRIMARY KEY,
    
    -- What to sync
    drive_type      TEXT NOT NULL CHECK (drive_type IN ('user', 'site', 'group')),
    drive_owner     TEXT NOT NULL,           -- user UPN, site URL, or group ID
    drive_id        TEXT,                    -- resolved Graph driveId (populated on first sync)
    folder_path     TEXT DEFAULT '/',        -- '/' = entire drive, or '/Documents/Legal' etc.
    folder_id       TEXT,                    -- resolved Graph folder itemId
    
    -- Sync behavior
    is_active       BOOLEAN DEFAULT true,
    sync_mode       TEXT NOT NULL DEFAULT 'full' 
                    CHECK (sync_mode IN ('full', 'selective')),
    file_extensions TEXT[],                  -- NULL = all supported, or ['docx','pdf','xlsx']
    max_file_size   BIGINT DEFAULT 104857600, -- 100MB default max
    recursive       BOOLEAN DEFAULT true,
    
    -- Delta sync state
    delta_link      TEXT,                    -- Graph delta link for incremental sync
    last_sync_at    TIMESTAMPTZ,
    last_sync_files INT DEFAULT 0,
    last_sync_error TEXT,
    
    -- Stats
    total_files     INT DEFAULT 0,
    total_chunks    INT DEFAULT 0,
    total_bytes     BIGINT DEFAULT 0,
    
    -- Metadata
    label           TEXT,                    -- friendly name: "William's OneDrive"
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE (drive_type, drive_owner, folder_path)
);

-- ============================================================
-- GLOBAL FILE INDEX (every synced OneDrive file)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.onedrive_files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_target_id  BIGINT REFERENCES chat.onedrive_sync_targets(id) ON DELETE CASCADE,
    
    -- Graph API identifiers
    drive_id        TEXT NOT NULL,
    item_id         TEXT NOT NULL,           -- Graph item ID
    parent_item_id  TEXT,                    -- parent folder item ID
    
    -- File metadata
    file_name       TEXT NOT NULL,
    file_path       TEXT NOT NULL,           -- full path within drive: /Documents/Legal/brief.docx
    file_extension  TEXT,
    mime_type       TEXT,
    size_bytes      BIGINT,
    sha256          TEXT,                    -- eTag or quickXorHash for change detection
    
    -- Extracted content
    content_text    TEXT,                    -- full extracted text
    content_summary TEXT,                    -- AI-generated summary (optional)
    
    -- Author/metadata from Graph
    created_by      TEXT,                    -- Graph user displayName
    created_by_email TEXT,
    last_modified_by TEXT,
    last_modified_by_email TEXT,
    web_url         TEXT,                    -- OneDrive web URL for direct access
    
    -- Processing state
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'downloading', 'extracting', 
                                       'chunking', 'embedding', 'indexed', 
                                       'failed', 'skipped', 'deleted')),
    processing_error TEXT,
    extraction_method TEXT,                  -- 'docx', 'pdf', 'xlsx', 'plain', 'ocr', etc.
    
    -- Graph timestamps
    graph_created_at    TIMESTAMPTZ,
    graph_modified_at   TIMESTAMPTZ,
    
    -- Our timestamps
    first_seen_at   TIMESTAMPTZ DEFAULT now(),
    last_synced_at  TIMESTAMPTZ DEFAULT now(),
    indexed_at      TIMESTAMPTZ,
    
    -- Change tracking
    etag            TEXT,                    -- Graph eTag for change detection
    ctag            TEXT,                    -- Graph cTag for content change detection
    
    metadata        JSONB DEFAULT '{}'::jsonb,
    
    UNIQUE (drive_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_onedrive_files_sync_target
    ON chat.onedrive_files(sync_target_id);
CREATE INDEX IF NOT EXISTS idx_onedrive_files_status
    ON chat.onedrive_files(status);
CREATE INDEX IF NOT EXISTS idx_onedrive_files_extension
    ON chat.onedrive_files(file_extension);
CREATE INDEX IF NOT EXISTS idx_onedrive_files_path
    ON chat.onedrive_files(file_path);
CREATE INDEX IF NOT EXISTS idx_onedrive_files_modified
    ON chat.onedrive_files(graph_modified_at DESC);
CREATE INDEX IF NOT EXISTS idx_onedrive_files_name_trgm
    ON chat.onedrive_files USING GIN(file_name gin_trgm_ops);

-- Full-text search on file metadata
CREATE INDEX IF NOT EXISTS idx_onedrive_files_fts
    ON chat.onedrive_files USING GIN(
        to_tsvector('english',
            coalesce(file_name, '') || ' ' ||
            coalesce(content_summary, '') || ' ' ||
            coalesce(created_by, '') || ' ' ||
            coalesce(last_modified_by, '')
        )
    );

-- ============================================================
-- FILE CHUNKS (chunked text for embeddings)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.onedrive_file_chunks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_id         UUID NOT NULL REFERENCES chat.onedrive_files(id) ON DELETE CASCADE,
    chunk_index     INT NOT NULL,
    chunk_text      TEXT NOT NULL,
    
    -- Chunk boundaries
    start_offset    INT,
    end_offset      INT,
    token_count     INT,
    
    -- Section context (for structured docs)
    section_title   TEXT,
    section_path    TEXT[],
    page_number     INT,                    -- for PDFs
    sheet_name      TEXT,                   -- for Excel
    
    -- Embedding
    embedding       halfvec(3072),
    
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    
    UNIQUE (file_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_onedrive_chunks_file
    ON chat.onedrive_file_chunks(file_id);

-- HNSW vector index for semantic search
CREATE INDEX IF NOT EXISTS idx_onedrive_chunks_embedding_hnsw
    ON chat.onedrive_file_chunks
    USING hnsw (embedding halfvec_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- Full-text search on chunk content
CREATE INDEX IF NOT EXISTS idx_onedrive_chunks_fts
    ON chat.onedrive_file_chunks USING GIN(
        to_tsvector('english', chunk_text)
    );

-- ============================================================
-- SPACE ↔ ONEDRIVE LINKING (the magic join table)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.space_onedrive_links (
    id              BIGSERIAL PRIMARY KEY,
    space_id        UUID NOT NULL REFERENCES chat.spaces(id) ON DELETE CASCADE,
    
    -- Can link individual files or entire sync targets (folders/drives)
    file_id         UUID REFERENCES chat.onedrive_files(id) ON DELETE CASCADE,
    sync_target_id  BIGINT REFERENCES chat.onedrive_sync_targets(id) ON DELETE CASCADE,
    
    -- Who linked it
    linked_by       BIGINT REFERENCES chat.users(id) ON DELETE SET NULL,
    linked_at       TIMESTAMPTZ DEFAULT now(),
    
    -- One of file_id or sync_target_id must be set
    CHECK (file_id IS NOT NULL OR sync_target_id IS NOT NULL),
    
    -- Prevent duplicate links
    UNIQUE (space_id, file_id),
    UNIQUE (space_id, sync_target_id)
);

CREATE INDEX IF NOT EXISTS idx_space_onedrive_links_space
    ON chat.space_onedrive_links(space_id);
CREATE INDEX IF NOT EXISTS idx_space_onedrive_links_file
    ON chat.space_onedrive_links(file_id);
CREATE INDEX IF NOT EXISTS idx_space_onedrive_links_target
    ON chat.space_onedrive_links(sync_target_id);

-- ============================================================
-- SYNC HISTORY (audit trail)
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.onedrive_sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    sync_target_id  BIGINT REFERENCES chat.onedrive_sync_targets(id) ON DELETE CASCADE,
    
    trigger_type    TEXT NOT NULL DEFAULT 'scheduled'
                    CHECK (trigger_type IN ('scheduled', 'manual', 'webhook', 'initial')),
    
    started_at      TIMESTAMPTZ DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    
    -- Stats
    files_discovered    INT DEFAULT 0,
    files_new           INT DEFAULT 0,
    files_updated       INT DEFAULT 0,
    files_deleted       INT DEFAULT 0,
    files_skipped       INT DEFAULT 0,
    files_failed        INT DEFAULT 0,
    chunks_created      INT DEFAULT 0,
    embedding_tokens    INT DEFAULT 0,
    
    duration_seconds    FLOAT,
    error_message       TEXT,
    
    -- Delta tracking
    delta_link_before   TEXT,
    delta_link_after    TEXT
);

CREATE INDEX IF NOT EXISTS idx_onedrive_sync_runs_target
    ON chat.onedrive_sync_runs(sync_target_id);
CREATE INDEX IF NOT EXISTS idx_onedrive_sync_runs_started
    ON chat.onedrive_sync_runs(started_at DESC);

-- ============================================================
-- GRAPH WEBHOOK SUBSCRIPTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS chat.onedrive_webhooks (
    id              BIGSERIAL PRIMARY KEY,
    sync_target_id  BIGINT REFERENCES chat.onedrive_sync_targets(id) ON DELETE CASCADE,
    
    subscription_id TEXT NOT NULL UNIQUE,     -- Graph subscription ID
    resource        TEXT NOT NULL,            -- Graph resource path
    change_type     TEXT NOT NULL DEFAULT 'updated',
    
    -- Lifecycle
    expiration_at   TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now(),
    renewed_at      TIMESTAMPTZ,
    
    is_active       BOOLEAN DEFAULT true
);

-- ============================================================
-- CONVENIENCE VIEWS
-- ============================================================

-- View: OneDrive files linked to a space (via direct link or sync target link)
CREATE OR REPLACE VIEW chat.v_space_onedrive_files AS
SELECT
    sol.space_id,
    f.id AS file_id,
    f.file_name,
    f.file_path,
    f.file_extension,
    f.mime_type,
    f.size_bytes,
    f.content_summary,
    f.created_by,
    f.last_modified_by,
    f.web_url,
    f.status,
    f.graph_modified_at,
    f.indexed_at,
    sol.linked_by,
    sol.linked_at,
    'file' AS link_type
FROM chat.space_onedrive_links sol
JOIN chat.onedrive_files f ON f.id = sol.file_id
WHERE sol.file_id IS NOT NULL
UNION ALL
SELECT
    sol.space_id,
    f.id AS file_id,
    f.file_name,
    f.file_path,
    f.file_extension,
    f.mime_type,
    f.size_bytes,
    f.content_summary,
    f.created_by,
    f.last_modified_by,
    f.web_url,
    f.status,
    f.graph_modified_at,
    f.indexed_at,
    sol.linked_by,
    sol.linked_at,
    'target' AS link_type
FROM chat.space_onedrive_links sol
JOIN chat.onedrive_files f ON f.sync_target_id = sol.sync_target_id
WHERE sol.sync_target_id IS NOT NULL;

-- View: OneDrive sync overview  
CREATE OR REPLACE VIEW chat.v_onedrive_sync_status AS
SELECT
    t.id AS target_id,
    t.label,
    t.drive_type,
    t.drive_owner,
    t.folder_path,
    t.is_active,
    t.sync_mode,
    t.total_files,
    t.total_chunks,
    pg_size_pretty(t.total_bytes) AS total_size,
    t.last_sync_at,
    t.last_sync_error,
    (SELECT COUNT(*) FROM chat.onedrive_files f 
     WHERE f.sync_target_id = t.id AND f.status = 'indexed') AS indexed_files,
    (SELECT COUNT(*) FROM chat.onedrive_files f 
     WHERE f.sync_target_id = t.id AND f.status = 'failed') AS failed_files,
    (SELECT COUNT(*) FROM chat.space_onedrive_links sol 
     WHERE sol.sync_target_id = t.id) AS linked_to_spaces
FROM chat.onedrive_sync_targets t;

-- ============================================================
-- HELPER FUNCTION: Updated-at trigger
-- ============================================================
CREATE OR REPLACE FUNCTION chat.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_onedrive_sync_targets_updated
    BEFORE UPDATE ON chat.onedrive_sync_targets
    FOR EACH ROW EXECUTE FUNCTION chat.set_updated_at();
