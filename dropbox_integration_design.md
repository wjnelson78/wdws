# Athena Dropbox Integration Service — Design Doc

**Author:** William Nelson (with Athena)
**Status:** Draft v0.5
**Last updated:** April 24, 2026
**Component owner:** Athena Platform
**Related:** Microsoft Graph ingestion pipeline, upload-session standard, Athena Document Agent Domain

---

## 1. Summary

The Athena Dropbox Integration Service ("DIS") is a first-class user-authorized connector that lets any external party (clients, witnesses, experts, opposing counsel, collaborators) link their own Dropbox account to Athena and have selected content automatically ingested into the Athena Cognitive Engine.

Each user connects via OAuth, authorizes Athena to read (and optionally write) a scoped folder, and Athena mirrors changes into its document pipeline using the same raw HTTP upload-session path already standardized for Microsoft Graph email and attachment ingestion.

The initial driver is one user (Sara) needing to share case materials, but the service is designed from day one as a general-purpose multi-tenant integration — not a one-off.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- **User-owned storage.** Each external user connects their own Dropbox; Athena never owns the data at rest in Dropbox.
- **Scoped access.** Default to Dropbox "App folder" mode so Athena only sees a dedicated subfolder, never the user's entire Dropbox.
- **Trust-tiered ingestion.** A connection's `trust_tier` governs whether content auto-ingests into the live RAG index, lands in a review queue, or is held in quarantine.
- **Route to the right place.** Each connection maps to an Athena case, domain, or investigation so ingested files land in the correct tenant/context, not a generic pile.
- **Resumable, idempotent sync.** Use Dropbox delta cursors and content hashes so restarts, retries, and replays don't duplicate documents.
- **Auditable.** Every sync event (connect, webhook, download, ingest, skip, error) is recorded for forensic review — relevant for litigation chain-of-custody.
- **Revocable on both sides.** Users can revoke at Dropbox; Athena admins can pause/revoke per connection.
- **Pattern reuse.** Reuse Athena's upload-session conventions, tagging, agent-note model, and email-pipeline observability.

### 2.2 Non-Goals (v1)

- Real-time bidirectional sync with conflict resolution. v1 is primarily ingest (Dropbox → Athena); writeback (Athena → Dropbox) is a phase-2 add.
- Replacing Athena's email pipeline. Dropbox is complementary, not a substitute for Microsoft Graph ingestion.
- Supporting Dropbox Paper, Dropbox Sign workflows, or Dropbox Replay beyond basic file ingestion.
- Automatic OCR or document-type classification at the sync layer — that's handled downstream by Athena's existing ingestion agents.
- End-user file picker UI inside Athena for selecting individual files in someone else's Dropbox. v1 scopes entire folders.

### 2.3 Explicitly out of scope

- Hosting Dropbox content outside Athena (e.g., re-sharing to third parties).
- Modifying user files in place on Dropbox without explicit user action.
- Any flow that would require a full-Dropbox scope for a user who doesn't need one.

---

## 3. Background and Motivation

Athena currently ingests documents through three primary paths:

1. **Microsoft Graph** — inbound email and attachments via `list_mailbox_messages` / `fetch_attachments`.
2. **Direct upload** — `create_document_upload_session` + PUT with `Content-Range` (standardized; base64 `upload_document` as fallback).
3. **Manual/scripted ingestion** — drag-and-drop, CLI tools, batch imports for PRR productions.

Several recurring scenarios don't fit these paths cleanly:

- **Clients and experts** who work primarily in Dropbox and resist switching to email attachments or SharePoint.
- **Third-party productions** (e.g., BIIA PRR responses, Sedgwick records) delivered via Dropbox links that currently require manual download and ingestion.
- **Collaborators** (e.g., Bobby Hair, Donald Newman) who have shared Dropbox folders for project work.
- **Caregiver workflows** where Jessie or a medical provider uploads documents that need to reach Athena without them navigating an upload form.

A user-authorized Dropbox connector solves all of these with one architecture. It also establishes the pattern for future connectors (Google Drive, Box, OneDrive personal) — each is a variant of the same OAuth + webhook + delta-sync shape.

---

## 4. High-Level Architecture

```
┌─────────────┐       ┌───────────────────────────────────────────────────┐
│  End User   │       │                   Athena Platform                 │
│  (e.g.,     │       │                                                   │
│   Sara)     │       │  ┌──────────────┐        ┌────────────────────┐   │
│             │       │  │  Connect UI  │        │  Webhook Endpoint  │   │
│  Dropbox ──┼───────┼─▶│  /connect/   │        │  /webhooks/dropbox │   │
│  Account    │       │  │   dropbox    │        └─────────┬──────────┘   │
└─────────────┘       │  └──────┬───────┘                  │              │
       ▲              │         │                          ▼              │
       │              │         │                 ┌────────────────┐      │
       │              │  ┌──────▼───────┐         │  Sync Worker   │      │
       │  OAuth       │  │  OAuth       │         │  (per-conn     │      │
       │  + API       │  │  Callback    │         │   cursor loop) │      │
       └──────────────┼─▶│  + Token     │         └───────┬────────┘      │
                      │  │  Store       │                 │               │
                      │  └──────┬───────┘                 ▼               │
                      │         │                 ┌────────────────┐      │
                      │         ▼                 │  Upload        │      │
                      │  ┌──────────────┐         │  Session       │      │
                      │  │ Connections  │         │  Pipeline      │      │
                      │  │ Table (enc.) │         │  (existing)    │      │
                      │  └──────────────┘         └───────┬────────┘      │
                      │                                   │               │
                      │                                   ▼               │
                      │                           ┌────────────────┐      │
                      │                           │  Athena Docs   │      │
                      │                           │  + RAG Index   │      │
                      │                           │  + Case Tags   │      │
                      │                           └────────────────┘      │
                      └───────────────────────────────────────────────────┘
```

### 4.1 Component summary

| Component | Responsibility |
|---|---|
| Connect UI | Initiates OAuth, displays consent status, shows active connections and their routing. |
| OAuth Callback | Exchanges auth code for tokens, stores encrypted, initializes cursor, emits `connection.created`. |
| Token Store | Encrypted at-rest storage of access + refresh tokens; handles refresh on expiry. |
| Webhook Endpoint | Verifies Dropbox signature, enqueues sync job per changed `dropbox_account_id`. |
| Sync Worker | Pulls deltas via `list_folder/continue`, downloads new/modified files, routes to upload-session pipeline. |
| Upload Session Pipeline | Existing Athena ingestion path (`create_document_upload_session` + chunked PUT). |
| Audit Log | Per-event record of every sync action for chain-of-custody. |

---

## 5. Dropbox App Registration

### 5.1 App configuration

- **App name:** `Athena Cognitive Engine` *(registered; see §18 Provisioning Log)*
- **Publisher:** Divergent Technology Group
- **App type:** Scoped access (required for modern Dropbox APIs).
- **Access mode:** Default to **App folder**. A separate app registration supports Full Dropbox for advanced/privileged connections.
- **Permissions (scopes):**
  - `account_info.read` — identify the connecting user, display name/email.
  - `files.metadata.read` — list folder contents, detect changes.
  - `files.content.read` — download file content.
  - `files.content.write` — only for bidirectional connections (phase 2 or explicitly configured).
  - `sharing.read` — for connections that need to resolve shared-link content (optional).
- **Redirect URIs:** `https://connect-athena.12432.net/dropbox/callback` (prod); `https://connect-athena-staging.12432.net/dropbox/callback` (staging).
- **Webhook URI:** `https://hooks-athena.12432.net/dropbox`.
- **Token access type:** `offline` — ensures refresh tokens are issued.

### 5.2 Two registrations, not one

We register two separate Dropbox apps:

1. **Athena Cognitive Engine (App folder mode)** — used for 95% of connections. User sees "Athena Cognitive Engine will create a folder in your Dropbox at `/Apps/Athena Cognitive Engine`." Minimum-privilege. **Registered April 23, 2026; app key and secret recorded in §18.**
2. **Athena Case Vault (Full Dropbox mode)** — used only when a user has an existing case folder they want to expose and cannot/will not move it into an app folder. Requires admin approval on the Athena side before a user can initiate this flow. **Deferred — not registered. Will be created when first full-Dropbox use case materializes (no current user requires it).**

Separation prevents scope creep: no user ever grants full-Dropbox access by default, and the UI for the privileged app can explicitly warn and require a reason.

---

## 6. OAuth Flow

### 6.1 Connect

1. User navigates to `https://connect-athena.12432.net/dropbox` (or follows an emailed link with an invite token pre-associating the connection with a case/tier).
2. Athena generates a `state` value (signed, includes invite token if present, CSRF nonce, expiry) and a PKCE `code_verifier`.
3. Athena redirects to:
   ```
   https://www.dropbox.com/oauth2/authorize
     ?client_id={APP_KEY}
     &response_type=code
     &redirect_uri={CALLBACK}
     &token_access_type=offline
     &state={SIGNED_STATE}
     &code_challenge={SHA256(code_verifier)}
     &code_challenge_method=S256
     &scope=account_info.read files.metadata.read files.content.read
   ```
4. User authenticates and approves at Dropbox.
5. Dropbox redirects to the callback with `code` and `state`.

### 6.2 Callback

1. Verify `state` signature and expiry, recover the PKCE `code_verifier` and invite token.
2. Exchange the code:
   ```
   POST https://api.dropboxapi.com/oauth2/token
     code={code}
     grant_type=authorization_code
     client_id={APP_KEY}
     client_secret={APP_SECRET}       # server-side; PKCE still used defensively
     redirect_uri={CALLBACK}
     code_verifier={code_verifier}
   ```
3. Receive `access_token`, `refresh_token`, `expires_in`, `account_id`, `uid`, `team_id` (if applicable).
4. Call `/2/users/get_current_account` to capture display name and primary email.
5. Insert into `dropbox_connections`:
   - Resolve `athena_user_id` — existing Athena principal if invite token matched one, otherwise create a lightweight external-user record.
   - Resolve routing (`target_case_id`, `target_domain`, `trust_tier`) from invite token or admin default.
   - Initialize `delta_cursor` via `/2/files/list_folder` with `path=""` (app folder root) or the selected watched path.
6. Emit `connection.created` event and audit log entry.
7. Render confirmation page to user with next steps ("Drop files in `/Apps/Athena` — they'll appear in Athena within a minute or two.")

### 6.3 Token refresh

- Access tokens live ~4 hours. Sync worker checks `token_expires_at` before each API call; refreshes if within a 5-minute buffer.
- Refresh tokens are long-lived but can be revoked by the user at any time.
- On 401 during any API call, attempt one refresh and retry; on repeated 401 after refresh, mark connection `status=revoked_remote` and notify the Athena admin.

### 6.4 Revocation

- User revokes at Dropbox → next API call 401s → connection flipped to `revoked_remote`.
- Athena admin revokes → connection flipped to `paused` or `revoked_local`; sync worker skips; optionally call `/2/auth/token/revoke` to invalidate server-side.
- Revocation does **not** delete already-ingested Athena documents; it only stops future sync. Deletion is a separate explicit operation.

---

## 7. Data Model

### 7.1 `dropbox_connections`

```sql
CREATE TABLE dropbox_connections (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  athena_user_id       UUID NOT NULL REFERENCES users(id),
  external_user_label  TEXT,                        -- display label, e.g., "Sara Smith (expert)"
  app_registration     TEXT NOT NULL,               -- 'app_folder' | 'full_dropbox'
  dropbox_account_id   TEXT NOT NULL,               -- from users/get_current_account
  dropbox_team_id      TEXT,                        -- null for personal accounts
  dropbox_email        TEXT NOT NULL,
  dropbox_display_name TEXT,

  access_token_enc     BYTEA NOT NULL,              -- encrypted (AES-GCM, KMS key)
  refresh_token_enc    BYTEA NOT NULL,
  token_expires_at     TIMESTAMPTZ NOT NULL,
  scopes               TEXT[] NOT NULL,

  watched_path         TEXT NOT NULL DEFAULT '',    -- '' = app folder root; absolute path for full-dropbox
  delta_cursor         TEXT,                        -- opaque Dropbox cursor
  cursor_initialized_at TIMESTAMPTZ,

  target_case_id       UUID REFERENCES cases(id),
  target_domain        TEXT,                        -- 'nelson' | 'investigator' | etc.
  trust_tier           TEXT NOT NULL DEFAULT 'review',
                       -- 'auto_ingest' | 'review' | 'quarantine'
  auto_tag             TEXT[] DEFAULT '{}',         -- tags applied to every ingested doc

  status               TEXT NOT NULL DEFAULT 'active',
                       -- 'active' | 'paused' | 'revoked_local' | 'revoked_remote' | 'error'
  status_reason        TEXT,

  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by           UUID REFERENCES users(id),
  last_sync_at         TIMESTAMPTZ,
  last_webhook_at      TIMESTAMPTZ,
  last_error_at        TIMESTAMPTZ,
  last_error_message   TEXT,

  UNIQUE (dropbox_account_id, watched_path, app_registration)
);

CREATE INDEX idx_dbx_conn_account ON dropbox_connections(dropbox_account_id);
CREATE INDEX idx_dbx_conn_status  ON dropbox_connections(status) WHERE status = 'active';
CREATE INDEX idx_dbx_conn_case    ON dropbox_connections(target_case_id);
```

### 7.2 `dropbox_sync_events` (audit)

```sql
CREATE TABLE dropbox_sync_events (
  id               BIGSERIAL PRIMARY KEY,
  connection_id    UUID NOT NULL REFERENCES dropbox_connections(id),
  event_type       TEXT NOT NULL,
                   -- 'webhook_received' | 'cursor_advanced' | 'file_downloaded'
                   -- | 'file_ingested' | 'file_skipped_dedup' | 'file_quarantined'
                   -- | 'error' | 'token_refreshed' | 'connection_revoked'
  dropbox_path     TEXT,
  dropbox_rev      TEXT,
  content_hash     TEXT,        -- sha256 of content, hex
  file_size        BIGINT,
  athena_doc_id    UUID,        -- populated when ingestion creates a doc
  duration_ms      INTEGER,
  details          JSONB,       -- per-event-type structured data
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dbx_events_conn_time ON dropbox_sync_events(connection_id, created_at DESC);
CREATE INDEX idx_dbx_events_type      ON dropbox_sync_events(event_type, created_at DESC);
```

### 7.3 `dropbox_quarantine` (review queue)

Files from `trust_tier = review` or `quarantine` land here instead of (or in addition to) the live index.

```sql
CREATE TABLE dropbox_quarantine (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  connection_id    UUID NOT NULL REFERENCES dropbox_connections(id),
  dropbox_path     TEXT NOT NULL,
  dropbox_rev      TEXT NOT NULL,
  content_hash     TEXT NOT NULL,
  file_size        BIGINT NOT NULL,
  local_path       TEXT NOT NULL,         -- staging location on disk
  detected_mime    TEXT,
  review_status    TEXT NOT NULL DEFAULT 'pending',
                   -- 'pending' | 'approved' | 'rejected' | 'expired'
  reviewer_id      UUID REFERENCES users(id),
  reviewed_at      TIMESTAMPTZ,
  reviewer_notes   TEXT,
  athena_doc_id    UUID,                   -- set on approval + ingestion
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at       TIMESTAMPTZ NOT NULL    -- auto-purge after N days if unreviewed
);
```

---

## 8. Webhook Handling

### 8.1 Endpoint contract

Dropbox calls `https://hooks-athena.12432.net/dropbox` with two methods:

- **GET** — verification handshake. Query parameter `challenge` must be echoed back as the response body. Must respond within 10 seconds with status 200 and content-type `text/plain`.
- **POST** — change notification. Body:
  ```json
  {
    "list_folder": { "accounts": ["dbid:AAH4f99..."] },
    "delta":       { "users": [12345] }
  }
  ```

### 8.2 Verification

- Dropbox signs POSTs with HMAC-SHA256 of the raw request body using the app secret, delivered in the `X-Dropbox-Signature` header.
- Handler must verify the signature against the exact raw body bytes — do not reserialize.
- Reject with 401 on missing or invalid signature. Log to audit with `event_type='error'`, `details.reason='invalid_signature'`.

### 8.3 Processing model

1. Verify signature.
2. Parse accounts list.
3. For each `dropbox_account_id`: look up all active `dropbox_connections`. (A single Dropbox account could, in theory, have multiple connections to different watched paths in full-dropbox mode.)
4. Enqueue a `sync_connection(connection_id)` job onto the background queue. Do **not** do the sync inline — Dropbox expects a fast 200.
5. Update `last_webhook_at` on each affected connection.
6. Return 200 within the 10-second window.

### 8.4 Idempotency

- The webhook says only "this account has changes" — it does not specify what. Multiple webhooks arriving close together can be coalesced: if a `sync_connection` job is already queued or running for the same connection, new webhooks are no-ops for queue purposes.
- Cursor-based sync is idempotent on Dropbox's side; replaying the same cursor returns no new entries.

---

## 9. Sync Worker

### 9.1 Per-connection sync loop

```
function sync_connection(connection_id):
    conn = load_connection(connection_id)
    if conn.status != 'active':
        return

    ensure_valid_token(conn)
    cursor = conn.delta_cursor
    if cursor is None:
        # First sync ever — should have been initialized at callback time,
        # but handle defensively.
        result = dropbox.files.list_folder(path=conn.watched_path, recursive=True)
    else:
        result = dropbox.files.list_folder_continue(cursor=cursor)

    for entry in result.entries:
        match entry:
            FileMetadata:
                handle_file(conn, entry)
            DeletedMetadata:
                handle_delete(conn, entry)
            FolderMetadata:
                # No-op at ingestion layer; folders are structure, not content.
                pass

    conn.delta_cursor = result.cursor
    conn.last_sync_at = now()
    save(conn)

    if result.has_more:
        enqueue(sync_connection(connection_id))   # tail recurse via queue
```

### 9.2 `handle_file`

```
function handle_file(conn, entry):
    audit(conn, 'file_detected', path=entry.path_display, rev=entry.rev)

    # Dedup: if we've already ingested this exact rev or content hash, skip.
    existing = find_athena_doc(
        connection_id=conn.id,
        dropbox_path=entry.path_display,
        dropbox_rev=entry.rev
    )
    if existing:
        audit(conn, 'file_skipped_dedup', athena_doc_id=existing.id)
        return

    # Download to staging.
    local_path, content_hash = download_to_staging(conn, entry)
    audit(conn, 'file_downloaded', path=entry.path_display,
          rev=entry.rev, content_hash=content_hash, file_size=entry.size)

    # Secondary dedup: same content hash already in case?
    if athena_doc_exists_with_hash(conn.target_case_id, content_hash):
        audit(conn, 'file_skipped_dedup', details={'reason': 'content_hash_match'})
        cleanup(local_path)
        return

    # Route by trust tier.
    if conn.trust_tier == 'quarantine':
        enqueue_quarantine(conn, entry, local_path, content_hash)
        return

    if conn.trust_tier == 'review':
        enqueue_review(conn, entry, local_path, content_hash)
        return

    # auto_ingest
    ingest_via_upload_session(conn, entry, local_path, content_hash)
```

### 9.3 Ingestion handoff

Uses the existing Athena path:

```
function ingest_via_upload_session(conn, entry, local_path, content_hash):
    session = create_document_upload_session(
        filename=basename(entry.path_display),
        case_id=conn.target_case_id,
        domain=conn.target_domain,
        metadata={
            'source': 'dropbox',
            'dropbox_account_id': conn.dropbox_account_id,
            'dropbox_email': conn.dropbox_email,
            'dropbox_path': entry.path_display,
            'dropbox_rev': entry.rev,
            'dropbox_modified': entry.server_modified,
            'content_hash': content_hash,
            'connection_id': str(conn.id),
            'sharer_label': conn.external_user_label,
        },
        tags=['source:dropbox', f'sharer:{conn.dropbox_email}',
              *conn.auto_tag],
    )

    put_chunks(session.upload_url, local_path,
               max_chunk_size=session.max_chunk_size,
               resume_from=session.next_expected_ranges)

    doc = finalize_upload(session.id)
    audit(conn, 'file_ingested', path=entry.path_display,
          rev=entry.rev, content_hash=content_hash,
          athena_doc_id=doc.id, file_size=entry.size)
    cleanup(local_path)
```

### 9.4 Deletes

Dropbox deletes show up in the delta stream as `DeletedMetadata` entries. Default policy:

- Do **not** cascade-delete from Athena. Athena's documents are authoritative for legal retention.
- Record the deletion as `event_type='remote_delete'` with the path and timestamp for chain-of-custody.
- Optionally flag the corresponding Athena doc's metadata with `dropbox_deleted_at` so UI can surface "source file removed upstream."
- A separate admin operation handles deliberate deletion requests — never webhook-driven.

### 9.5 Large files

- Files > 150 MB in Dropbox require chunked download. Use `/2/files/download_zip` for directories or streamed download for single files.
- Athena's upload-session path already supports chunked PUT with `Content-Range` at ≤4 MiB chunks. Stream Dropbox → disk → upload session; do not hold the whole file in memory.
- Very large files (> 2 GB) should be flagged and routed to quarantine by default, regardless of trust tier, for explicit admin approval.

---

## 10. Trust Tiers

| Tier | Behavior | Typical use |
|---|---|---|
| `auto_ingest` | File → Athena document → RAG index, immediately visible to agents/tools. | Client-provided materials, trusted experts, self-connections. |
| `review` | File → staging + quarantine queue. Human reviewer (Will or designee) approves before ingestion into the live index. | Witness materials, third-party productions, first-time expert connections. |
| `quarantine` | File → staging only. Not visible to agents until manually promoted. | Opposing counsel, unknown parties, high-sensitivity productions. |

### 10.1 Quarantine UI

A small review surface inside Athena:

- List of pending items per case, with preview, source path, uploader, detected MIME, size.
- Approve → ingest via upload-session pipeline, set `dropbox_quarantine.review_status='approved'`, record reviewer + timestamp.
- Reject → delete staging file, mark `rejected`, reason captured. Source file on Dropbox is untouched.
- Auto-expire unreviewed items after N days (default 30), purging staging content and marking `expired`. Source files on Dropbox are untouched.

### 10.2 Default tier policy

- Invite-token-driven connects: tier set by invite.
- Self-initiated connects by existing Athena users: `auto_ingest` by default (it's their own data).
- Self-initiated connects by external users with no invite: `review` by default.
- Full-Dropbox mode connections: minimum tier is `review`, never `auto_ingest` by default.

---

## 11. API Endpoints (Athena-side)

### 11.1 User-facing

All user-facing routes hosted at `connect-athena.12432.net`.

- `GET /dropbox` — start OAuth, optional query params: `invite=<token>`, `case=<id>`, `mode=app_folder|full_dropbox`.
- `GET /dropbox/callback` — OAuth callback handler.
- `GET /dropbox/status` — current user's active connections (display only).
- `POST /dropbox/{id}/pause` — user-initiated pause.
- `POST /dropbox/{id}/revoke` — user-initiated revoke (also calls Dropbox token revoke).

### 11.2 Admin

Hosted at `connect-athena.12432.net` under `/admin/*`, gated behind Cloudflare Access so these endpoints are not publicly reachable.

- `GET /admin/dropbox/connections` — list all, filter by status/case/tier.
- `GET /admin/dropbox/connections/{id}` — detail + recent sync events.
- `POST /admin/dropbox/connections/{id}/pause`
- `POST /admin/dropbox/connections/{id}/resume`
- `POST /admin/dropbox/connections/{id}/revoke`
- `POST /admin/dropbox/connections/{id}/trust_tier` — promote/demote.
- `POST /admin/dropbox/connections/{id}/reroute` — change target case/domain.
- `POST /admin/dropbox/connections/{id}/force_resync` — discard cursor, re-list from `watched_path` (dedup still protects against reingest).
- `GET /admin/dropbox/quarantine?case={id}` — review queue.
- `POST /admin/dropbox/quarantine/{id}/approve`
- `POST /admin/dropbox/quarantine/{id}/reject`

### 11.3 Webhook

Hosted at `hooks-athena.12432.net`. POST-only in practice; GET permitted only for the verification handshake.

- `GET /dropbox` — verification.
- `POST /dropbox` — change notifications.

### 11.4 MCP tools (Athena Cognitive Engine)

New tools exposed via the existing MCP server for Will/Athena-agent use:

- `dropbox_list_connections(filters...)`
- `dropbox_get_connection(connection_id)`
- `dropbox_sync_events(connection_id, limit=50)`
- `dropbox_quarantine_list(case_id=None)`
- `dropbox_quarantine_approve(quarantine_id)`
- `dropbox_force_resync(connection_id)`

These let Athena itself report on connection health and let Will drive admin flows conversationally.

---

## 12. Security

### 12.1 Token storage

- Access and refresh tokens stored in `BYTEA` columns, encrypted at rest with AES-256-GCM using a key managed by the same KMS mechanism Athena uses for Microsoft Graph credentials.
- Plaintext tokens never written to logs, audit records, or error messages. Redact at the logging layer defensively.
- Token rotation on refresh: when Dropbox issues a new refresh token, rotate in place and write to audit with `event_type='token_refreshed'` (no token values).

### 12.2 Webhook security

- Signature verification on every POST — reject unsigned or mismatched requests.
- Rate limit the endpoint to prevent abuse.
- Log request IPs for forensic review.

### 12.3 Transport

- All outbound calls to Dropbox over TLS 1.2+.
- Certificate pinning optional; not required for v1.

### 12.4 Privilege and data-minimization

- Default to app-folder mode. Full-Dropbox mode requires explicit admin flag to even be offered.
- Scopes requested are the minimum for the connection's intended direction (read vs read+write).
- Connection records never store file *content* long-term outside Athena's document store — staging files purged after successful ingest or quarantine resolution.

### 12.5 Legal chain of custody

- Every file ingested records: `dropbox_account_id`, `dropbox_email`, `dropbox_path`, `dropbox_rev`, `server_modified` timestamp from Dropbox, `content_hash`, ingestion timestamp, connection ID.
- Audit log is append-only; no UPDATE or DELETE on `dropbox_sync_events` from application code.
- Dropbox's own metadata (who uploaded, when modified, revision IDs) is preserved in Athena's document metadata, supporting authentication of the source if ever needed for evidentiary purposes.

### 12.6 Threat model quick notes

| Threat | Mitigation |
|---|---|
| Malicious user connects and uploads harmful content | Trust tiers; quarantine default for unknowns; MIME checks; size caps. |
| Token leak from database | KMS-encrypted at rest, redacted in logs, refresh rotation. |
| Webhook spoofing | HMAC signature verification. |
| Replay of old webhook | Cursor-based sync is idempotent; no state change from replay. |
| Compromised refresh token | User revocation at Dropbox immediately stops sync on next call; admin can revoke server-side. |
| Data exfiltration via full-Dropbox scope | Separate app registration, admin gate, minimum review tier. |

---

## 13. Observability

### 13.1 Metrics (Prometheus-style)

- `dropbox_connections_total{status}` — gauge.
- `dropbox_webhooks_received_total` — counter.
- `dropbox_webhook_signature_failures_total` — counter.
- `dropbox_sync_jobs_total{result}` — counter (result = success|error|skipped).
- `dropbox_sync_job_duration_seconds` — histogram.
- `dropbox_files_ingested_total{case,tier}` — counter.
- `dropbox_files_quarantined_total{case}` — counter.
- `dropbox_token_refreshes_total{result}` — counter.
- `dropbox_api_errors_total{endpoint,status}` — counter.

### 13.2 Alerts

- Sync worker error rate > 5% over 10 minutes.
- Webhook signature failures > 10 in any minute.
- Connection stuck in `error` status > 1 hour.
- Quarantine queue depth > 100 pending (operational warning).
- Token refresh failure for any active connection.

### 13.3 Dashboards

- Per-connection last-sync time, files-in-last-24h, current cursor, error count.
- Per-case ingestion rate from Dropbox vs other sources.
- Quarantine queue age distribution.

---

## 14. Phased Rollout

### Phase 0 — Dropbox app provisioning (½ day)
- Register `Athena` app (app-folder mode) at dropbox.com/developers.
- Register `Athena Case Vault` app (full-dropbox mode).
- Configure redirect URIs, webhook URI, scopes.
- Store app keys in Athena's secrets store.

### Phase 1 — Sara's connection, manual-admin-driven (1 sprint)
- Schema migrations (`dropbox_connections`, `dropbox_sync_events`, `dropbox_quarantine`).
- OAuth connect + callback endpoints.
- Webhook endpoint (verification + enqueue).
- Sync worker with `handle_file` + upload-session handoff.
- Admin-only connection creation (no self-serve user UI yet).
- Admin list/pause/revoke endpoints.
- **Ship:** Sara connects via an admin-emailed link, her folder syncs into her case.

### Phase 2 — Self-serve and trust tiers (1 sprint)
- Invite-token system with pre-bound case/tier/domain.
- Quarantine queue + review UI.
- User-facing status page (`/connect/dropbox/status`).
- MCP tools for Athena-agent visibility.
- Metrics + dashboards + alerts.

### Phase 3 — Scale and polish (1 sprint)
- Large-file streaming path validated on 2 GB+ files.
- Full-Dropbox mode UI with explicit warnings and admin gate.
- Bulk force-resync and cursor reset tools.
- Per-connection audit export for legal chain-of-custody.

### Phase 4 — Writeback (phase 2 goal, separate project)
- `files.content.write` scope, Athena → Dropbox pushes (e.g., signed drafts to client's folder).
- Conflict detection on rev changes.

---

## 15. Integration with Existing Athena Components

### 15.1 Upload-session pipeline
Dropbox sync reuses `create_document_upload_session` unchanged. The only new inputs are the Dropbox-specific metadata fields added to the `metadata` JSONB. Falls back to base64 `upload_document` only if the upload session path is unavailable, matching the existing standard.

### 15.2 Document metadata / tagging
All ingested docs get a canonical `source='dropbox'` tag and a `dropbox_connection:{connection_id}` tag, in addition to any `auto_tag` values on the connection. Existing Athena search and RAG queries pick these up without changes.

### 15.3 Agent notes
On `connection.created`, an agent note is auto-filed on the relevant case with summary: "Dropbox connection established: {email} → {folder}. Trust tier: {tier}." Keeps Will's investigation trail current.

### 15.4 Email pipeline parity
Dropbox sync mirrors the email pipeline's architectural shape:

| Concern | Email pipeline | Dropbox pipeline |
|---|---|---|
| Source of truth | MS Graph (user's mailbox) | Dropbox (user's folder) |
| Change notification | Graph webhooks / poll | Dropbox webhooks |
| Delta primitive | Delta token | Delta cursor |
| Ingestion handoff | Upload session | Upload session |
| Metadata preservation | Message-ID, headers, attachments | path, rev, hash, account |
| Trust/routing | Per-mailbox config | Per-connection config |

This parity is deliberate — future connectors (Google Drive, Box) slot into the same shape.

---

## 16. URL Architecture and Cloudflare Configuration

### 16.1 Hostnames

The integration uses dedicated, purpose-named hostnames under `12432.net`, routed through the existing Cloudflare Tunnel to the Athena host. The existing `klunky.12432.net` hostname is reserved for internal/direct platform access and is not used for any external-facing OAuth or webhook traffic.

| Hostname | Purpose | Traffic type | Access |
|---|---|---|---|
| `connect-athena.12432.net` | User-facing OAuth flows, status pages, admin UI | Browser | Public (admin routes gated by Cloudflare Access) |
| `hooks-athena.12432.net` | Service-to-service webhooks from Dropbox, Google, Microsoft, etc. | Server-to-server | Public but signature-verified; optional IP allowlist per provider |
| `connect-athena-staging.12432.net` | Staging/test environment for new connectors | Browser | Restricted via Cloudflare Access |

**Why this split:**

- **Trust signal on OAuth consent screens.** When Dropbox shows Sara the authorization page, the redirect URL it displays is part of the trust decision. `connect-athena.12432.net` reads as a purpose-built integration endpoint; `klunky.12432.net` reads as personal infrastructure and may trigger user hesitation or phishing concerns.
- **Differential security posture.** `hooks.*` has no legitimate browser traffic. WAF and rate-limit rules can be stricter than on `connect.*` without impacting end users.
- **Future-proof for additional connectors.** The same scheme covers Google Drive, Box, OneDrive, Slack, and any future integration with zero URL restructuring.
- **Clean audit story.** For any legal or security review, purpose-named hostnames document intent at the DNS layer.

### 16.2 Path conventions

Each provider gets a top-level path segment. No `/connect/` or `/webhooks/` prefix is needed because the subdomain already encodes that.

```
https://connect-athena.12432.net/dropbox              — OAuth start
https://connect-athena.12432.net/dropbox/callback     — OAuth callback
https://connect-athena.12432.net/dropbox/status       — user status
https://connect-athena.12432.net/admin/dropbox/*      — admin (Cloudflare Access)

https://hooks-athena.12432.net/dropbox                — Dropbox webhook

Future connectors follow the same pattern:
https://connect-athena.12432.net/google-drive/...
https://connect-athena.12432.net/microsoft/...
https://hooks-athena.12432.net/google-drive
https://hooks-athena.12432.net/microsoft/graph
https://hooks-athena.12432.net/slack/events
```

### 16.3 Cloudflare Tunnel configuration

The Athena host (`172.16.32.207`) runs `cloudflared` inside the existing `tukwila-colo-tunnel` (ID `8d161118-84e3-44ed-9fb0-7f6d80cb4548`). The tunnel is **dashboard-managed**: its ingress configuration lives in Cloudflare's control plane and is edited via the Cloudflare API (`PUT /accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations`), **not** a local `config.yml` on the host. The endpoint replaces the full configuration in one atomic call, so every edit is fetch-splice-PUT-verify with a drift-check against the current canonical version before the PUT and a byte-identical preservation check on unchanged rules after. `cloudflared` picks up the new config from Cloudflare automatically within a few seconds — no host-side reload. **The full procedure (drift-check, atomic PUT, byte-identical preservation, in-memory rollback) is documented in §16.8 and is the canonical edit pattern for any future change to this tunnel.**

The actual host IP is never exposed publicly; all inbound traffic arrives through Cloudflare's edge.

**Phase 0 ingress rules for this integration** (positions 19–21 of the 22-rule config, immediately before the catch-all):

```
- hostname: connect-athena.12432.net
  service: http://127.0.0.1:8080       # Athena connect app (OAuth + /admin)
- hostname: hooks-athena.12432.net
  service: http://127.0.0.1:8081       # Webhook receiver (separate process)
- hostname: connect-athena-staging.12432.net
  service: http://127.0.0.1:8090       # Staging
```

Listeners bind `127.0.0.1` because `cloudflared` and the Phase 0 services run on the same host; binding to loopback rather than `0.0.0.0` avoids LAN exposure.

Running the webhook receiver as a separate internal port/process lets us scale, deploy, and restart webhooks independently of the user-facing app — important because webhook availability is what preserves sync completeness.

> **Correction from earlier drafts (v0.1–v0.4):** Prior revisions stated the Athena host was `172.16.32.236`. This was incorrect. The actual host — where the existing `athena-*.12432.net` fleet already terminates, and where Phase 0 placeholders now run — is `172.16.32.207`. Prior drafts also implied the tunnel was locally-managed (`config.yml` on the host). It has always been dashboard-managed. Both facts were confirmed during Phase 0-A inventory and are reflected above.

### 16.4 Cloudflare rules

**`connect-athena.12432.net`:**
- Full (strict) TLS, minimum TLS 1.2.
- Standard bot management, optional country rules if abuse appears.
- Browser Integrity Check enabled.
- `/admin` and `/admin/*` paths behind Cloudflare Access (deployed Phase 0-F, 2026-04-24):
  - Access app: `Athena Connect — Admin`, id `c568d7a4-69f5-4688-bde3-65682c741b72`; policy id `54cc8ee5-e0bc-4f53-ad50-9cbab014dd03`.
  - Identity provider: `Azure AD Divergent` (Microsoft 365 OIDC), IdP id `483f638f-1317-4b1a-ab53-8bf65eedf86e`, explicitly bound via `allowed_idps`.
  - `session_duration=24h`, `auto_redirect_to_identity=true`, `app_launcher_visible=false`.
  - Sole principal: `william@seattleseahawks.me`. No groups, no wildcards, no service tokens.
  - User-facing OAuth paths (`/dropbox`, `/dropbox/callback`, `/dropbox/status`) and `/health` remain public — the `/admin` + `/admin/*` path scope does not extend to them. Verified end-to-end on deploy.

**`hooks-athena.12432.net`:**
- Full (strict) TLS, minimum TLS 1.2.
- Firewall rule: require `Content-Type: application/json` on POST, reject GET except to known verification paths.
- Rate limit: default 100 requests/minute per source IP, with burst allowance for legitimate provider traffic.
- Where providers publish webhook IP ranges (Dropbox currently does not publish a stable list; Slack and Microsoft Graph do), add an allowlist rule as a defense-in-depth layer.
- WAF: challenge or block any request missing the expected provider signature header (`X-Dropbox-Signature`, `X-Hub-Signature-256`, etc.).
- Log all requests to Cloudflare Logpush for forensic review.

**`connect-athena-staging.12432.net`:**
- Entire hostname behind Cloudflare Access (deployed Phase 0-F, 2026-04-24):
  - Access app: `Athena Connect — Staging`, id `c365c47b-814e-485f-9ba5-7ca14aad0389`; policy id `719f1be9-bca4-4e73-9d59-b7dbe45c093b`.
  - Same IdP, session duration, `auto_redirect_to_identity`, and sole principal as the admin app above.
- Not registered as a redirect URI with production Dropbox/Google/Microsoft apps — staging uses separate app registrations.

### 16.5 DNS records

```
connect-athena          CNAME   <tunnel-id>.cfargotunnel.com   (proxied)
hooks-athena            CNAME   <tunnel-id>.cfargotunnel.com   (proxied)
connect-athena-staging  CNAME   <tunnel-id>.cfargotunnel.com   (proxied)
```

All three records are proxied through Cloudflare (orange cloud), so the origin is never exposed.

### 16.6 Certificate and HSTS

- Cloudflare-managed Universal SSL certificate covers `*.12432.net` — no separate cert management per hostname.
- HSTS enabled at the zone level with a moderate max-age (6 months) initially, extending to 1 year after verification. No preload until all 12432.net subdomains are confirmed HTTPS-only.

### 16.7 Operational notes

- **Deprecating `klunky.12432.net` for external-facing use.** Any existing external references to klunky (emails, documents, legacy integrations) should be migrated to `connect-athena.*` or `hooks-athena.*` as appropriate. Klunky remains valid for direct admin/infrastructure access.
- **Tunnel health monitoring.** `cloudflared` metrics scraped into the existing Prometheus; alert on tunnel disconnect exceeding 60 seconds — a down tunnel means missed webhooks.
- **Backup path for webhooks during outages.** Not implemented in v1. If `hooks-athena.12432.net` is unreachable, providers retry per their own schedules (Dropbox retries for up to ~1 hour). For longer outages, a scheduled reconciliation job (see §14 Phase 3) can force-resync all active connections to catch anything missed.

### 16.8 Canonical procedures for editing Cloudflare zone, tunnel, and Access (Phase 0 learnings)

Phase 0 established these procedural patterns through four consecutive edits (DNS creation, two tunnel-config PUTs, two Access app creations). They are the standing operating procedure for any subsequent change touching the `12432.net` zone, the `tukwila-colo-tunnel` ingress, or the Cloudflare Access app set. **Phase 1 and beyond should follow them by default; deviation requires explicit justification.**

**Atomic-PUT-replace for tunnel ingress.** The tunnel is dashboard-managed (§16.3), and its configurations endpoint (`PUT /accounts/{acct}/cfd_tunnel/{id}/configurations`) replaces the entire config in one call. Every edit therefore:

1. **Fetches** the full current config and holds the raw result in memory for the duration of the edit.
2. **Drift-checks** the fetched config against the expected canonical state (hostname + path + service per rule, plus the rule count). Any divergence means someone or something else modified the tunnel since the last known-good state — abort, do not overwrite.
3. **Splices** the minimum necessary change into a deep copy of the fetched config. Everything not being explicitly changed is preserved byte-for-byte.
4. **PUTs** the spliced config. The API returns the new version number.
5. **Verifies** post-PUT by re-fetching and confirming: version advanced, rule count matches expectation, SHA256 of every preserved rule identical pre→post, new/modified rules present as intended, catch-all at the tail.
6. **Rolls back** by PUTting the cached pre-edit config if any verification step fails. Rollback itself is verified the same way (re-fetch, re-hash).

**Drift-check baselines roll forward.** Each successful atomic PUT establishes a new canonical state. Subsequent edits drift-check against the *latest known-good baseline*, not the original. Phase 0 trail: v134 (pre-0-D) → v135 (post-0-D, 0-E drift-checks this) → v136 (post-0-E, future work drift-checks this). When documenting a change, record both the pre-PUT version and the post-PUT version so later edits can reconstruct the baseline.

**Canary hostnames on every CF zone or tunnel edit.** Before any change, capture `curl -I` responses for at least `klunky.12432.net` plus any other Athena-adjacent hostnames that could be collaterally affected (Phase 0 used `athena-ai`, `investigator`, and `klunky`). After the edit, re-capture and diff excluding known-varying headers (`cf-ray`, `date`, `age`, `cf-cache-status`, `expires`, `last-modified`, `x-request-id`, `report-to`, `nel`, `server-timing`, `etag`). A zero non-varying-header diff is the acceptance bar. A non-zero diff on an unrelated hostname is an abort signal, even if the edit itself appears to have succeeded.

**Structural apps-list canary for Access edits.** Before creating or modifying any Access app, snapshot the current app list with per-app and per-policy SHA256 fingerprints over canonical JSON of `{name, domain, self_hosted_domains, type, allowed_idps, session_duration, app_launcher_visible, auto_redirect_to_identity, http_only_cookie_attribute, same_site_cookie_attribute}` for apps and `{name, decision, precedence, include, require, exclude}` for policies. After the edit, re-snapshot and compare. The required delta is exactly the intended additions/removals — every unchanged app and its policies must hash identically.

**Agent-verifiable rollback rehearsal before any Access edit that could lock out admin.** Before creating a real Access app on a production-adjacent hostname, create a throwaway app against a guaranteed-unused domain (e.g., `rehearsal-<hex>.invalid.12432.net`) and immediately delete it. Confirm create+delete authority end-to-end on the credential being used, and confirm the baseline apps list is byte-identical after the rehearsal. If delete fails after create succeeds, halt — that is the worst-case state (can build but can't tear down). Phase 0-F used this pattern with rehearsal hostname `rehearsal-9f3a7b.invalid.12432.net` under the `ZZZ-` name prefix (sorts to the bottom of any list; unmistakable if something unexpected happens).

**Audit-log verification runs *after* propagation, not before.** Cloudflare's audit log has ~30–60 s ingestion lag before entries become queryable. Queries run immediately after a mutation will return empty even though the state change completed successfully. Procedure: record `start_ts` at the top of the edit sequence, do the edits, wait for the propagation window (≥60 s), *then* query audit logs for mutations in `[start_ts, now]` and verify both count and actor match expectations. Treat a Global-API-Key actor as `account-owner`; any mutation in the window by any other actor is a signal to pause and investigate before declaring the change complete.

**Pre-authorized rollback is conditional, not blanket.** A rollback is always the right move when verification fails in a way that suggests the PUT or POST went wrong (structure corrupted, existing resources altered, canary delta). A rollback is not the right move for cosmetic or transient conditions (local DNS cache staleness, HEAD-vs-GET discrepancy on a scaffold, sub-resource propagation catching up). Document which failure modes trigger automatic rollback and which require human review before proceeding.

**Preserve rollback authority in memory for the entire edit window.** The cached pre-edit config must remain available from the start of the fetch until verification completes. Do not discard it after the PUT; discard only after a full successful verification. If the script fails mid-way (network blip, uncaught exception), the next action taken must be to restore the cached config, not to retry the PUT.

---

## 17. Open Questions

1. **Sara's connection specifically:** Is she client, witness, or expert? Determines default trust tier and target case.
2. **External user records:** Do we create full Athena `users` rows for external connectors like Sara, or a lighter-weight `external_parties` table? Current design assumes lightweight record.
3. **Email notifications:** On quarantine-requires-review, do we email Will immediately, batch daily, or rely on dashboard? Proposal: immediate for first item in any 1-hour window, then batch.
4. **Retention of staging files:** On successful ingest, staging file is deleted immediately. On quarantine expiry, staging file is deleted. Any case where we need to keep staging around longer?
5. **Cross-connection dedup:** If Sara and Dan both have the same exhibit PDF in their respective Dropboxes, do we ingest once or twice? Current design ingests both but flags via `content_hash`. Might want a UI that shows "this exhibit also appeared in {other_connection}."
6. **Writeback direction for phase 2:** Signed drafts back to client folder is the obvious use; are there others (e.g., filed-pleading mirror for client records)?
7. **Team/business Dropbox accounts:** Does Sara have a personal Dropbox or is she on a team plan? Team accounts have additional admin-approval requirements for third-party apps that we may need to handle.
8. **Rate limits:** Dropbox has per-app and per-user rate limits. v1 should be well under them for a handful of connections, but we should budget for 50+ connections before revisiting.

---

---

## 18. Provisioning Log

Living record of actual deployment state for this integration. Updated as provisioning steps complete.

### 18.1 Dropbox App #1 — Athena Cognitive Engine (App folder mode)

| Field | Value |
|---|---|
| **Status** | Registered, Development mode |
| **Registered** | April 23, 2026 |
| **App name** | `Athena Cognitive Engine` |
| **Publisher** | Divergent Technology Group |
| **Permission type** | Scoped App (App folder) |
| **App folder name** | `Athena Cognitive Engine` *(users see `/Apps/Athena Cognitive Engine/` in their Dropbox)* |
| **App key** | `newe7nvul21i2hm` *(not secret)* |
| **App secret** | Stored outside doc. **⚠️ Rotation required before first external user** — current secret was transmitted in chat during setup and must be regenerated via the Dropbox app console before Sara or any external user connects. |
| **Redirect URIs** | `https://connect-athena.12432.net/dropbox/callback`<br>`https://connect-athena-staging.12432.net/dropbox/callback` |
| **Webhook URI** | Deferred — not yet configured. Will be added after `hooks-athena.12432.net` endpoint is deployed (Phase 1). Target: `https://hooks-athena.12432.net/dropbox` |
| **Allow public clients (PKCE)** | Allow |
| **Scopes granted** | `account_info.read`, `files.metadata.read`, `files.metadata.write`, `files.content.read`, `files.content.write` |
| **Development user limit** | 50 |
| **Branding** | App name + publisher + description + placeholder website/privacy URLs set. Icons not uploaded (puzzle-piece placeholders). |
| **Privacy policy URL** | `https://connect-athena.12432.net/privacy` *(placeholder — page must be authored and deployed before first external user)* |

### 18.2 Dropbox App #2 — Athena Case Vault (Full Dropbox mode)

| Field | Value |
|---|---|
| **Status** | Deferred — not registered |
| **Reason** | No current use case requires full-Dropbox scope. Sara fits app-folder mode cleanly. |
| **Trigger to register** | First user who cannot move their case folder into an app folder and whose trust tier justifies full-Dropbox access. |

### 18.3 Cloudflare / DNS

**Athena host:** `172.16.32.207` (corrected from `172.16.32.236` in v0.1–v0.4; see §16.3 correction note).
**Tunnel:** `tukwila-colo-tunnel` (ID `8d161118-84e3-44ed-9fb0-7f6d80cb4548`), dashboard-managed, currently at config version 136.

| Item | Status |
|---|---|
| `connect-athena.12432.net` CNAME | **Configured** 2026-04-24 — proxied CNAME to `<tunnel-id>.cfargotunnel.com`, record id `72e09eba4780c4ba004d4b40368d63b1` |
| `hooks-athena.12432.net` CNAME | **Configured** 2026-04-24 — proxied CNAME, record id `1a0d08f6ffc1d149889ad33e94c92459` |
| `connect-athena-staging.12432.net` CNAME | **Configured** 2026-04-24 — proxied CNAME, record id `1c1e817ac71c427f80c03cba229ebe45` |
| Cloudflare Tunnel ingress rules | **Configured** 2026-04-24 — 3 rules at positions 19–21, targeting `127.0.0.1:{8080,8081,8090}`; tunnel config version 134 → 135 → 136 across 0-D and 0-E |
| Cloudflare Access app — `connect-athena.12432.net/admin` + `/admin/*` | **Configured** 2026-04-24 — app id `c568d7a4-69f5-4688-bde3-65682c741b72`, policy id `54cc8ee5-e0bc-4f53-ad50-9cbab014dd03`, IdP `Azure AD Divergent` (`483f638f-1317-4b1a-ab53-8bf65eedf86e`), principal `william@seattleseahawks.me`, session 24h |
| Cloudflare Access app — `connect-athena-staging.12432.net` (entire hostname) | **Configured** 2026-04-24 — app id `c365c47b-814e-485f-9ba5-7ca14aad0389`, policy id `719f1be9-bca4-4e73-9d59-b7dbe45c093b`, same IdP + principal |
| WAF rules for `hooks-athena.*` | Not yet configured — Phase 1 dependency; rate limits, provider-specific signature-header rules, and Logpush config are tuned once the real webhook handler is deployed and the provider IP patterns are known. |

### 18.3a Phase 0 placeholder listeners

Three HTTP placeholders deployed on `172.16.32.207`, bound to `127.0.0.1` only, serving `GET /health` with a JSON body (`{"service":"<name>","status":"placeholder","phase":"0"}`) and `404` on every other path. These are scaffolding to prove the Cloudflare → tunnel → origin plumbing works end-to-end; Phase 1 handlers replace them entirely.

| Unit | Port | Script |
|---|---|---|
| `phase0-connect-athena.service` | 127.0.0.1:8080 | `/opt/wdws/phase0_placeholder.py --port 8080 --name connect-athena --bind 127.0.0.1` |
| `phase0-hooks-athena.service` | 127.0.0.1:8081 | `/opt/wdws/phase0_placeholder.py --port 8081 --name hooks-athena --bind 127.0.0.1` |
| `phase0-connect-athena-staging.service` | 127.0.0.1:8090 | `/opt/wdws/phase0_placeholder.py --port 8090 --name connect-athena-staging --bind 127.0.0.1` |

**Durability:** all three units are `enable`d (start on boot); `Restart=always, RestartSec=2, StartLimitBurst=10/60s`. Auto-restart verified via `systemctl kill -s KILL` on each — new MainPID within 3 s, `/health` resumed without intervention.

**Known limitation (cosmetic):** the placeholder implements only `do_GET`; `HEAD` requests return `501 Not Implemented` from `BaseHTTPRequestHandler`'s default. The briefing's acceptance contract is `GET`, so this is not a functional gap; Phase 1 replaces the placeholder.

**Teardown when Phase 1 ships:** `systemctl disable --now phase0-<name>.service && rm /etc/systemd/system/phase0-<name>.service` for each, then `systemctl daemon-reload`. The script at `/opt/wdws/phase0_placeholder.py` can be removed or retained as a testing tool.

### 18.4 Athena application

| Item | Status |
|---|---|
| Database schema migration (§7) | Not started |
| OAuth endpoints (§6, §11.1) | Not started |
| Webhook handler (§8, §11.3) | Not started |
| Sync worker (§9) | Not started |
| Upload-session handoff (§9.3) | Reuses existing Athena pipeline — no new work |
| Admin UI (§11.2) | Not started |
| MCP tools (§11.4) | Not started |
| Privacy policy page | Not drafted |

### 18.5 Outstanding before first external user (Sara)

- [ ] Rotate `DROPBOX_ATHENA_APP_SECRET` via Dropbox app console
- [ ] Answer §17 open questions (especially Sara's trust tier and Dropbox plan type)
- [x] Register DNS records and configure Cloudflare Tunnel ingress *(Phase 0, 2026-04-24)*
- [x] Configure Cloudflare Access on the admin surface and staging hostname *(Phase 0, 2026-04-24)*
- [ ] Deploy webhook handler; add webhook URI to Dropbox app config *(Phase 1)*
- [ ] Deploy OAuth endpoints and database schema *(Phase 1)*
- [ ] Draft and deploy privacy policy page
- [ ] Upload Athena branding icons (64×64 and 256×256) for consent screen polish
- [ ] End-to-end test of self-connection by Will before inviting Sara

---

## 19. Appendix: Dropbox API reference (quick)

- OAuth authorize: `https://www.dropbox.com/oauth2/authorize`
- OAuth token: `https://api.dropboxapi.com/oauth2/token`
- OAuth token revoke: `https://api.dropboxapi.com/2/auth/token/revoke`
- Current account: `https://api.dropboxapi.com/2/users/get_current_account`
- List folder: `https://api.dropboxapi.com/2/files/list_folder`
- List folder continue: `https://api.dropboxapi.com/2/files/list_folder/continue`
- Download: `https://content.dropboxapi.com/2/files/download`
- Upload (small): `https://content.dropboxapi.com/2/files/upload`
- Upload session (large): `/2/files/upload_session/{start,append_v2,finish}`
- Webhook signature header: `X-Dropbox-Signature` (HMAC-SHA256, hex, of raw body with app secret)

---

## 20. Tenancy and attribution model

Athena is a single-tenant system. External users (e.g. a client whose Dropbox is connected, an expert delivering documents, a collaborator sharing a folder) are represented as **attribution context on ingested content**, not as distinct Athena tenants. The Dropbox integration inherits this shape.

Consequently, the integration does **not** introduce per-tenant isolation at the schema or query layer. `ops.dropbox_connections`, `ops.dropbox_sync_events`, and `ops.dropbox_quarantine` (§7) are owned by the platform owner — `athena_user_id` references `core.users(id)` of the owner — and all ingested content lands in the existing single-tenant document store alongside everything else.

**Attribution is carried via the `metadata` JSONB column on `core.documents`**, seeded at ingest by the sync worker (§9.3). Minimum fields written on every Dropbox-sourced document:

- `metadata.source = "dropbox"` (existing convention)
- `metadata.source_connection = "<connection_id>"` — always present; links back to `ops.dropbox_connections.id`
- `metadata.client = "<external_user_label>"` — human-readable origin (e.g. `"Sara Murray"`); does **not** depend on the downstream enrichment pass firing
- `metadata.dropbox_path`, `metadata.dropbox_rev`, `metadata.dropbox_account_id`, `metadata.dropbox_email`, `metadata.content_hash` — full lineage per §9.3

This replaces an earlier draft (v0.2–v0.4) assumption that each external user would be their own tenant. Single-tenant with metadata attribution is sufficient for the concrete use cases on the table (Sara as a client matter routed to `legal` domain, `auto_ingest` trust tier), and avoids touching the ~66 MCP tools, the agent fleet, the dashboard, and the ingestion pipeline — all of which are currently single-tenant-scoped. Phase 1 (tenancy survey and minimum completion) in the full-project briefing was retired on 2026-04-24 based on this decision.

Should hard user-isolation ever be needed, that work is scoped as a separate future project and would touch the whole stack; it is out of scope for the Dropbox integration.

---

*End of draft v0.5.*

**Changes from v0.4 (Phase 0 completion, 2026-04-24):**

- **Phase 0 executed and verified end-to-end.** Tasks 0-A (inventory) through 0-G (this doc update) completed. Three DNS CNAMEs proxied, tunnel config advanced v134 → v135 → v136, three placeholder listeners on `127.0.0.1:{8080,8081,8090}`, two Cloudflare Access apps gating `/admin` + `/admin/*` and the full staging hostname. Browser-flow verification (probe D) confirmed.
- **§16.3 rewritten.** Tunnel is dashboard-managed (edited via Cloudflare API, not a local `config.yml`). The local-YAML example from prior drafts has been replaced with a description of the atomic-PUT-replace pattern; full procedural detail moved to the new §16.8.
- **Host correction: `.236` → `.207`.** Prior drafts (v0.1–v0.4) stated the Athena host was `172.16.32.236`. This was wrong — every existing `athena-*.12432.net` tunnel rule terminates at `172.16.32.207`, and Phase 0 placeholders now run there too. Corrected in §16.3 and recorded in §18.3 with an inline correction note.
- **§16.4 populated with the actually-deployed Access configuration.** IdP IDs, app IDs, policy IDs, session durations, redirect flags, and the exact path scope recorded. Replaces the aspirational "bound to Will's identity (Microsoft 365 or GitHub OIDC)" text from earlier drafts.
- **§16.8 added (new) — "Canonical procedures for editing Cloudflare zone, tunnel, and Access."** Codifies the patterns that Phase 0 established through four consecutive edits: atomic-PUT-replace, drift-check-before-PUT with rolling-forward baseline, klunky canary and related unrelated-hostname canary, structural apps-list canary for Access, rollback rehearsal for lockout-risk tasks, audit-log-verification-after-propagation (30–60 s CF ingestion lag), conditional rollback authority, and in-memory rollback-state preservation. These are the standing operating procedure for Phase 1 and beyond.
- **§18.3 populated with real state.** Six rows now read "Configured 2026-04-24" with record IDs, app IDs, policy IDs, and version trail; WAF rules for `hooks-athena.*` still deferred to Phase 1 (needs the real handler first).
- **§18.3a added (new) — Phase 0 placeholder listeners.** Documents the three systemd units, bind model (`127.0.0.1`), durability properties, one cosmetic limitation (HEAD-501), and Phase-1 teardown steps.
- **§18.5 ticks.** DNS/tunnel/Access items marked done; remaining items (Dropbox app secret rotation, privacy policy page, branding icons, end-to-end test, §17 question answers) carried forward to Phase 1.

**Earlier history (unchanged):** v0.3→v0.4 added §18 Provisioning Log and registered Athena Cognitive Engine as Dropbox App #1. v0.2→v0.3 renamed hostnames to `connect-athena` / `hooks-athena`. v0.1→v0.2 added §16 URL Architecture and Cloudflare Configuration.

**Next (Phase 1 — separate briefing to be produced):** database schema migration (§7), OAuth endpoints and callback (§6, §11.1), webhook handler with HMAC signature verification (§8, §11.3), sync worker with delta-cursor loop and upload-session handoff (§9, §9.3), admin endpoints behind the Access gate we just deployed (§11.2), privacy policy page, `DROPBOX_ATHENA_APP_SECRET` rotation, answers to §17 open questions.
