-- Migration 014: Platform Features — Activity, Chat, Teams, Calendar, Calls
-- Athena Cognitive Engine
-- Date: 2026-04-04

BEGIN;

-- ══════════════════════════════════════════════════════════════
--  1. ACTIVITY FEED
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS core.activity_stream (
    id          BIGSERIAL PRIMARY KEY,
    user_id     INT REFERENCES core.users(id),
    actor_type  TEXT NOT NULL DEFAULT 'user',       -- user, agent, system
    actor_id    TEXT NOT NULL,                       -- user id, agent id, or 'system'
    event_type  TEXT NOT NULL,                       -- document.created, email.received, call.completed, etc.
    category    TEXT NOT NULL,                       -- activity, chat, teams, calendar, calls
    title       TEXT NOT NULL,
    detail      TEXT,
    object_type TEXT,                                -- document, case, email, conversation, call, event, task
    object_id   TEXT,                                -- UUID or ID of the related object
    metadata    JSONB DEFAULT '{}',
    visibility  TEXT NOT NULL DEFAULT 'private',     -- public, private, team
    team_id     INT,                                 -- if team-scoped
    is_read     BOOLEAN DEFAULT false,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_activity_user ON core.activity_stream (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_category ON core.activity_stream (category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_unread ON core.activity_stream (user_id, is_read) WHERE NOT is_read;

-- ══════════════════════════════════════════════════════════════
--  2. USER-TO-USER CHAT (Direct Messages)
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS core.user_presence (
    user_id     INT PRIMARY KEY REFERENCES core.users(id),
    status      TEXT NOT NULL DEFAULT 'offline',     -- online, away, dnd, offline
    status_text TEXT,                                -- custom status message
    last_seen   TIMESTAMPTZ DEFAULT now(),
    device      TEXT,                                -- macos, ios, web
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dm_threads (
    id          SERIAL PRIMARY KEY,
    thread_type TEXT NOT NULL DEFAULT 'direct',      -- direct (1:1), group
    name        TEXT,                                -- NULL for 1:1, name for group
    created_by  INT REFERENCES core.users(id),
    avatar_url  TEXT,
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dm_thread_members (
    thread_id   INT NOT NULL REFERENCES core.dm_threads(id) ON DELETE CASCADE,
    user_id     INT NOT NULL REFERENCES core.users(id),
    role        TEXT DEFAULT 'member',               -- owner, admin, member
    joined_at   TIMESTAMPTZ DEFAULT now(),
    muted       BOOLEAN DEFAULT false,
    PRIMARY KEY (thread_id, user_id)
);

CREATE TABLE IF NOT EXISTS core.dm_messages (
    id          BIGSERIAL PRIMARY KEY,
    thread_id   INT NOT NULL REFERENCES core.dm_threads(id) ON DELETE CASCADE,
    sender_id   INT NOT NULL REFERENCES core.users(id),
    content     TEXT NOT NULL,
    content_type TEXT DEFAULT 'text',                -- text, image, file, system
    attachments JSONB DEFAULT '[]',                  -- [{filename, url, mime_type, size}]
    reply_to    BIGINT REFERENCES core.dm_messages(id),
    reactions   JSONB DEFAULT '{}',                  -- {"👍": [user_id, ...], "❤️": [...]}
    edited_at   TIMESTAMPTZ,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dm_messages_thread ON core.dm_messages (thread_id, created_at DESC);

CREATE TABLE IF NOT EXISTS core.dm_read_receipts (
    thread_id       INT NOT NULL REFERENCES core.dm_threads(id) ON DELETE CASCADE,
    user_id         INT NOT NULL REFERENCES core.users(id),
    last_read_id    BIGINT REFERENCES core.dm_messages(id),
    last_read_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (thread_id, user_id)
);

-- ══════════════════════════════════════════════════════════════
--  3. TEAMS
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS core.teams (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT,
    icon        TEXT,                                -- emoji or URL
    created_by  INT REFERENCES core.users(id),
    is_archived BOOLEAN DEFAULT false,
    metadata    JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT now(),
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.team_members (
    team_id     INT NOT NULL REFERENCES core.teams(id) ON DELETE CASCADE,
    user_id     INT NOT NULL REFERENCES core.users(id),
    role        TEXT NOT NULL DEFAULT 'member',      -- owner, admin, member
    joined_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (team_id, user_id)
);

CREATE TABLE IF NOT EXISTS core.team_channels (
    id          SERIAL PRIMARY KEY,
    team_id     INT NOT NULL REFERENCES core.teams(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    description TEXT,
    is_default  BOOLEAN DEFAULT false,
    is_archived BOOLEAN DEFAULT false,
    created_by  INT REFERENCES core.users(id),
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (team_id, name)
);

CREATE TABLE IF NOT EXISTS core.team_channel_messages (
    id          BIGSERIAL PRIMARY KEY,
    channel_id  INT NOT NULL REFERENCES core.team_channels(id) ON DELETE CASCADE,
    sender_id   INT NOT NULL REFERENCES core.users(id),
    content     TEXT NOT NULL,
    content_type TEXT DEFAULT 'text',
    attachments JSONB DEFAULT '[]',
    reply_to    BIGINT REFERENCES core.team_channel_messages(id),
    reactions   JSONB DEFAULT '{}',
    edited_at   TIMESTAMPTZ,
    deleted_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_team_msgs_channel ON core.team_channel_messages (channel_id, created_at DESC);

-- Link teams to AI spaces for shared AI collaboration
CREATE TABLE IF NOT EXISTS core.team_space_links (
    team_id     INT NOT NULL REFERENCES core.teams(id) ON DELETE CASCADE,
    space_id    UUID NOT NULL,                       -- references chat.spaces in athena_chat DB
    linked_by   INT REFERENCES core.users(id),
    linked_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (team_id, space_id)
);

-- ══════════════════════════════════════════════════════════════
--  4. CALENDAR (extend existing + multi-source)
-- ══════════════════════════════════════════════════════════════

-- Calendar sources (providers per user)
CREATE TABLE IF NOT EXISTS core.calendar_sources (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES core.users(id),
    provider        TEXT NOT NULL,                   -- local, outlook, google, apple
    name            TEXT NOT NULL,                   -- "Work Calendar", "Personal", etc.
    is_active       BOOLEAN DEFAULT true,
    is_primary      BOOLEAN DEFAULT false,
    color           TEXT,                            -- hex color for UI
    credentials     JSONB DEFAULT '{}',              -- encrypted tokens/config
    sync_state      JSONB DEFAULT '{}',              -- delta tokens, sync cursors
    last_synced_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cal_sources_user ON core.calendar_sources (user_id);

-- Extend calendar_events with source tracking
ALTER TABLE core.calendar_events
    ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'outlook',
    ADD COLUMN IF NOT EXISTS source_id INT REFERENCES core.calendar_sources(id),
    ADD COLUMN IF NOT EXISTS recurrence JSONB,
    ADD COLUMN IF NOT EXISTS reminders JSONB DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS created_by INT REFERENCES core.users(id),
    ADD COLUMN IF NOT EXISTS description TEXT,
    ADD COLUMN IF NOT EXISTS color TEXT,
    ADD COLUMN IF NOT EXISTS is_private BOOLEAN DEFAULT false;

-- RSVP tracking
CREATE TABLE IF NOT EXISTS core.calendar_rsvps (
    id          SERIAL PRIMARY KEY,
    event_id    TEXT NOT NULL,                       -- references core.calendar_events.id
    user_id     INT REFERENCES core.users(id),
    email       TEXT,                                -- for external attendees
    response    TEXT NOT NULL DEFAULT 'pending',     -- accepted, declined, tentative, pending
    comment     TEXT,
    responded_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ DEFAULT now(),
    UNIQUE (event_id, user_id)
);

-- ══════════════════════════════════════════════════════════════
--  5. CALLS (VoIP/SIP)
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS core.call_sessions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    direction       TEXT NOT NULL,                   -- inbound, outbound
    status          TEXT NOT NULL DEFAULT 'ringing',  -- ringing, in_progress, completed, missed, failed, voicemail
    caller_id       INT REFERENCES core.users(id),   -- internal caller (NULL for inbound external)
    caller_number   TEXT,                            -- E.164 phone number
    caller_name     TEXT,
    callee_id       INT REFERENCES core.users(id),   -- internal callee (NULL for outbound external)
    callee_number   TEXT,
    callee_name     TEXT,
    answered_by     TEXT DEFAULT 'user',             -- user, ai, voicemail
    sip_call_id     TEXT,                            -- SIP Call-ID header
    provider_sid    TEXT,                            -- Twilio/Telnyx call SID
    started_at      TIMESTAMPTZ DEFAULT now(),
    answered_at     TIMESTAMPTZ,
    ended_at        TIMESTAMPTZ,
    duration_secs   INT,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_calls_user ON core.call_sessions (caller_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_callee ON core.call_sessions (callee_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_status ON core.call_sessions (status) WHERE status IN ('ringing', 'in_progress');

CREATE TABLE IF NOT EXISTS core.call_recordings (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    call_id         UUID NOT NULL REFERENCES core.call_sessions(id) ON DELETE CASCADE,
    file_path       TEXT,                            -- local path to recording
    file_url        TEXT,                            -- remote URL (Twilio recording)
    duration_secs   INT,
    file_size       BIGINT,
    mime_type       TEXT DEFAULT 'audio/wav',
    transcript      TEXT,                            -- full transcription
    transcript_segments JSONB DEFAULT '[]',          -- [{start, end, speaker, text}]
    ai_summary      TEXT,                            -- AI-generated call summary
    transcribed_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.call_participants (
    id          SERIAL PRIMARY KEY,
    call_id     UUID NOT NULL REFERENCES core.call_sessions(id) ON DELETE CASCADE,
    user_id     INT REFERENCES core.users(id),
    phone_number TEXT,
    display_name TEXT,
    role        TEXT DEFAULT 'participant',          -- initiator, participant, ai
    joined_at   TIMESTAMPTZ DEFAULT now(),
    left_at     TIMESTAMPTZ
);

-- SIP configuration
CREATE TABLE IF NOT EXISTS core.sip_config (
    id              SERIAL PRIMARY KEY,
    provider        TEXT NOT NULL DEFAULT 'twilio',  -- twilio, telnyx, custom
    sip_domain      TEXT,
    sip_username    TEXT,
    credentials     JSONB DEFAULT '{}',              -- encrypted
    phone_numbers   TEXT[],                          -- assigned DIDs
    ai_enabled      BOOLEAN DEFAULT true,            -- AI can answer calls
    ai_greeting     TEXT DEFAULT 'Hello, this is Athena. How can I help you?',
    voicemail_enabled BOOLEAN DEFAULT true,
    voicemail_greeting TEXT,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT now()
);

COMMIT;
