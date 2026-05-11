# Athena Cognitive Engine — MCP Server

**Server file:** [mcp_server_v2.py](mcp_server_v2.py)
**Public name:** `Athena Cognitive Engine`
**Transport:** Streamable HTTP, port `9200` (path defined by `MCP_ENDPOINT_PATH`)
**Auth:** OAuth 2.1 via `PostgresOAuthProvider` — required scope `read`; valid scopes: `read`, `write`, `admin`, `search`, `medical`.

This README is the field manual for AI agents (Athena, sub-agents, external clients) that use this MCP. The server's runtime `instructions` block — included automatically with every connection — is the canonical operating doctrine; this file is for orientation, the tool taxonomy, and the bits that don't fit cleanly inside a system prompt.

> **PostgreSQL 17 + pgvector.** Not SQLite. When writing SQL via `execute_sql`, use schema-qualified names (`legal.cases`, `core.documents`, `medical.records`) and PostgreSQL syntax (`tsvector`/`tsquery`, JSONB, CTEs).

---

## Tool taxonomy

Annotations: `READ_ONLY` is safe; `WRITE_OP` requires `write` scope and mutates state; `DESTRUCTIVE` deletes data.

### Search & retrieval
| Tool | Purpose |
|---|---|
| `search`, `fetch` | Generic search/fetch entrypoints. |
| `semantic_search`, `rag_query` | pgvector similarity over BGE-M3 (1024-d) embeddings. |
| `fulltext_search`, `search_by_case`, `search_by_date_range`, `search_emails`, `search_filings`, `search_medical`, `search_entities`, `search_document_relationships`, `search_agent_notes` | Structured PostgreSQL queries. |
| `fetch_document`, `fetch_attachments`, `fetch_email_thread`, `fetch_case_details` | Hydrate a known id. |
| `execute_sql` | Raw read-only SQL when nothing else fits. |

### Mailbox (Microsoft Graph) — read
| Tool | Purpose |
|---|---|
| `list_mailbox_messages` | Live read of a folder's messages. |
| `get_mailbox_message` | Fetch one message (with body, headers, attachments metadata). |
| `list_mail_folders` | Enumerate folders / subfolders. Use to discover ids before moving. |

### Mailbox (Microsoft Graph) — write
| Tool | Purpose | Notes |
|---|---|---|
| `draft_email_message` | Create a draft. | LLM-assisted body composition. |
| `send_mail_message` | Send a message. | |
| `reply_to_mail_message`, `reply_all_mail_message` | Threaded reply. | |
| `update_mail_message` | PATCH `isRead`, `importance`, `categories`, receipt flags. | Does **not** move messages. |
| `create_mail_folder` | Create a folder or subfolder. | Move does not auto-create. |
| `move_mail_message` | Move a message between folders. | **Graph reissues the message id on move.** Persist the `internet_message_id` (stable) rather than `message_id` (regenerated). |

#### Mail-triage workflow

```
list_mailbox_messages(folder='inbox', unread_only=True)
        ↓
list_mail_folders(parent='Cases')          # discover or confirm
        ↓
create_mail_folder(name='Smith v. Acme',
                   parent='Cases')          # only if missing
        ↓
move_mail_message(message_id=...,
                  destination_folder='Cases/Smith v. Acme')
        ↓
update_mail_message(message_id=<NEW id>,
                    is_read=True,
                    categories='triaged')
```

`destination_folder` accepts three forms:

1. **Well-known names** — `inbox`, `archive`, `sentitems`, `drafts`, `deleteditems`, `junkemail`, `outbox`, `conversationhistory`, etc. (Microsoft's reserved set; case-insensitive.)
2. **Display-name path** — `'Cases/Smith v. Acme/Discovery'`. Segments are matched case-sensitively against folder `displayName` via `$filter`. Each segment must already exist; create with `create_mail_folder` first.
3. **Raw Graph folder id** — the `id` returned by `list_mail_folders`. Detected by length ≥ 80 chars + base64url-only chars.

⚠️ **Id reissue on move.** Microsoft Graph regenerates `message.id` whenever a message is relocated. After a successful `move_mail_message`:
- `message.message_id` in the response = the **new** id (use this for follow-up actions in the same flow).
- `previous_message_id` in the response = the dead prior id (do **not** retry against this).
- `internet_message_id` (RFC 5322) is **stable** across moves — prefer it as a persistent reference when storing pointers in the DB.

`_graph_resolve_message_graph_id` accepts either form transparently, so persisting `internet_message_id` is the low-friction default.

### SharePoint / Files (Microsoft Graph)

Athena is **not limited to specific shares** — these tools work against every site and
library the Graph app has rights to (Sites.ReadWrite.All + Files.ReadWrite.All on the
tenant). Use `prr_list_inbound_shares` to enumerate the agency drop-folders configured
for the active public-records request, but treat them as one of many destinations rather
than the only one Athena can reach.

| Tool | Annotation | Purpose |
|---|---|---|
| `sharepoint_list_sites(query=None, top=25)` | READ_ONLY | Discover sites (search or list). |
| `sharepoint_list_drives(site)` | READ_ONLY | Document libraries on a site. `site` accepts a Graph site id, `'hostname:/sites/path'`, or the literal `'root'`. |
| `sharepoint_list_items(drive_id=..., item_id=..., path=..., share_url=..., top=100)` | READ_ONLY | List children of a folder. Provide drive_id+item_id, drive_id+path, drive_id alone (= root), or share_url. |
| `sharepoint_get_item(...)` | READ_ONLY | Metadata for one item. File responses include a short-lived `download_url` (no auth needed for a follow-up GET). |
| `sharepoint_search(query, drive_id=None, site=None, top=25)` | READ_ONLY | Search; scope precedence drive → site → tenant-wide (Microsoft Search API). |
| `sharepoint_resolve_share(share_url)` | READ_ONLY | Resolve any sharing link to its driveItem (drive_id + item_id). The app token bypasses anonymous-share password gates when it has tenant-wide Sites/Files perms. |
| `sharepoint_create_folder(drive_id, parent_item_id, name, conflict_behavior='rename')` | WRITE_OP | Create a folder. `parent_item_id='root'` for drive root. |
| `sharepoint_upload_file(drive_id, parent_item_id, name, content_text \| content_b64 \| document_id, conflict_behavior='rename')` | WRITE_OP | Upload a file. Provide exactly one source. ≤3 MB uses a single PUT; larger uses a chunked upload session. |
| `sharepoint_delete_item(drive_id, item_id)` | **DESTRUCTIVE** | Delete (goes to recycle bin). |
| `sharepoint_move_item(drive_id, item_id, new_parent_item_id=..., new_parent_drive_id=..., new_name=...)` | WRITE_OP | Move and/or rename. Cross-drive supported via `new_parent_drive_id`. |
| `sharepoint_copy_item(drive_id, item_id, destination_parent_item_id, destination_drive_id=..., new_name=...)` | WRITE_OP | Async copy. Response includes `monitor_url` to poll Graph for completion. |
| `sharepoint_update_item(drive_id, item_id, new_name=..., list_fields_json=...)` | WRITE_OP | Rename and/or PATCH listItem custom column fields. `list_fields_json` is a JSON object string. |
| `sharepoint_create_sharing_link(drive_id, item_id, link_type='view', scope='anonymous', password=..., expiration_date_time=..., retain_inherited_permissions=True)` | WRITE_OP | Create or reuse a sharing link. `link_type` ∈ {view, edit, embed}. `scope` ∈ {anonymous, organization, users}. |
| `sharepoint_invite(drive_id, item_id, recipients, roles='read', message=..., require_sign_in=True, send_invitation=True, expiration_date_time=..., password=...)` | WRITE_OP | Invite specific users (CSV emails). `roles` is CSV: read \| write \| owner. |
| `sharepoint_list_permissions(drive_id, item_id)` | READ_ONLY | All permissions on an item — links + invites + inherited. |
| `sharepoint_revoke_permission(drive_id, item_id, permission_id)` | **DESTRUCTIVE** | Delete a permission (kills a sharing link or invite). |
| `sharepoint_update_permission(drive_id, item_id, permission_id, expiration_date_time=..., password=..., roles=...)` | WRITE_OP | Extend expiration, rotate password, change roles. |
| `sharepoint_list_versions(drive_id, item_id, top=50)` | READ_ONLY | Version history for a file. |
| `sharepoint_restore_version(drive_id, item_id, version_id)` | WRITE_OP | Restore a prior version (current state preserved as a new version on top). |
| `prr_list_inbound_shares()` | READ_ONLY | Read `/root/.athena/*_share.env` registry. Returns share URL, permission id, expiration, folder item id, and `has_password` (the password itself is never returned — it's an external-recipient secret). Override the directory with the `PRR_SHARE_DIR` env var. |
| `prr_register_inbound_share(name, share_url, permission_id=..., expires_at=..., folder_item_id=..., password=..., replace=False)` | WRITE_OP | Persist a new share to the on-disk registry. File written 0600 because it carries a password. |

#### SharePoint workflow patterns

**Discovery → walk → read:**
```
sharepoint_list_sites(query='Legal')
        ↓ pick a site
sharepoint_list_drives(site=<site_id>)
        ↓ pick a library
sharepoint_list_items(drive_id=<drive_id>, path='Cases/Smith v. Acme')
        ↓ pick a file
sharepoint_get_item(drive_id=..., item_id=...)
        ↓ GET item.download_url (no auth)
```

**Working with a sharing link directly:**
```
sharepoint_list_items(share_url='https://...')           # walks the share contents
sharepoint_get_item(share_url='https://...')             # metadata + download_url
sharepoint_resolve_share(share_url='https://...')        # explicit (drive_id, item_id) resolve
```

**PRR triage (agency drop-folder check-in):**
```
prr_list_inbound_shares()                                 # enumerate configured shares
sharepoint_list_items(share_url=<snoco_clerk URL>)        # see what they uploaded
sharepoint_get_item(drive_id=..., item_id=<file>)         # grab download_url
# fetch download_url → create_document_upload_session     # ingest into Athena for search
```

**Upload an Athena document to SharePoint:**
```
sharepoint_upload_file(
    drive_id=...,
    parent_item_id=<destination folder>,
    name='draft-response.pdf',
    document_id=<Athena document id>,         # bytes loaded from Athena store
    conflict_behavior='rename',
)
```

**Build a NEW PRR inbound share end-to-end:**
```
sharepoint_list_drives(site=<Inbound Records site>)            # pick library
sharepoint_create_folder(drive_id, parent_item_id='root',
                         name='Snoco Assessor')                # → folder item id
sharepoint_create_sharing_link(drive_id, item_id=<folder id>,
                               link_type='edit', scope='anonymous',
                               password='strong-pw',
                               expiration_date_time='2026-08-01T00:00:00Z')
                                                                # → share_link_url + perm id
prr_register_inbound_share(name='snoco_assessor',
                           share_url=<share_link_url>,
                           permission_id=<perm id>,
                           expires_at='2026-08-01T00:00:00Z',
                           folder_item_id=<folder id>,
                           password='strong-pw')                # persist registry
# (out-of-band) email the agency the URL + password
```

**Revoke / extend / rotate password on an existing share:**
```
sharepoint_list_permissions(drive_id, item_id=<folder id>)     # find permission_id
sharepoint_update_permission(drive_id, item_id, permission_id,
                             expiration_date_time='2026-12-01T00:00:00Z')
sharepoint_update_permission(drive_id, item_id, permission_id,
                             password='rotated-pw')
sharepoint_revoke_permission(drive_id, item_id, permission_id)  # kill the link
```

**File-level versioning:**
```
sharepoint_list_versions(drive_id, item_id)                     # see history
sharepoint_restore_version(drive_id, item_id, version_id='3.0') # roll back
```

#### Auth model

Both mail and SharePoint operations use a **single Graph app token** acquired from
`GRAPH_CLIENT_ID` / `GRAPH_CLIENT_SECRET` / `GRAPH_TENANT_ID` via client-credentials.
The app's tenant-level admin consents bound the surface — no per-user delegation, no
per-share sub-tokens. If a SP call returns `403 Forbidden`, the cause is almost always
a missing app permission grant, not a per-resource ACL.

#### PRR share registry on disk

Each `*_share.env` under `/root/.athena/` declares one outbound share for an external
agency (anonymous link + password the agency uses to authenticate; Athena, holding the
app token, does not need the password). Naming convention:

```
<NAME>_SHARE_URL=https://...
<NAME>_SHARE_PASSWORD=<external-only — never returned by MCP>
<NAME>_SHARE_PERM_ID=<Graph permission id, for revocation/expiry updates>
<NAME>_SHARE_EXPIRES=<ISO-8601>
<NAME>_FOLDER_ITEM_ID=<driveItem id of the dropoff folder, for direct app-token access>
```

Currently configured (2026-05-05): aoc, coa_div1, dshs, hca, snoco_clerk,
snoco_court_admin, snoco_sheriff, wbe. Adding/removing files updates `prr_list_inbound_shares`
output without a server restart.

### Documents (storage / lifecycle)
| Tool | Purpose |
|---|---|
| `create_document` | Text/markdown documents. |
| `create_document_upload_session`, `create_document_download_session` | Binary HTTP transfer (PUT/GET, supports `Range`). No base64 path. |
| `convert_docx_to_pdf` | Render existing DOCX → PDF (also available inline via `auto_convert_to_pdf=True` on upload session). |
| `update_document_metadata`, `delete_document` | Lifecycle. |
| `create_document_relationship` | Link documents (e.g. `converted_from`). |
| `list_document_domains`, `add_tag`, `remove_tag`, `list_tags` | Tagging surface. |

### Cases / investigations / tasks
`list_cases`, `fetch_case_details`, `get_case_report`, `get_all_cases_summary`, `update_case_status`, `case_timeline`, `get_investigation_summary`, `create_task`, `complete_task`, `list_tasks`, `add_case_deadline`, `complete_deadline`, `list_upcoming_deadlines`.

### Notes (two distinct surfaces — don't mix)
| Surface | Tools | Storage |
|---|---|---|
| **Database notes** (case-tagged, structured) | `create_note`, `get_notes`, `update_note`, `create_agent_note`, `search_agent_notes` | Postgres `core.notes` |
| **Athena's Outlook notes** (drafts in mailbox folder) | `notes_list`, `notes_get`, `notes_create`, `notes_update`, `notes_delete` | Drafts in the `Athena Notes` mail folder, auto-created on first write |

The Outlook notes surface exists because the OneNote app-only API was retired 2025-03-31; the implementation stores each note as an unsent draft so William can browse them in Outlook.

### Memory (semantic, cross-session)
| Tool | Purpose |
|---|---|
| `save_personal_memory` | Persist a fact/decision/insight. Re-saving the same content reinforces (raises importance, bumps `access_count`) — does NOT duplicate. Pass `supersedes_memory_id` only on replacement. |
| `record_feedback` | Persist a correction or workflow rule. Lead with the rule; add a `Why:` line. |
| `recall_personal_memories(query, limit=5)` | Semantic recall ranked by `0.7·similarity + 0.3·importance`; superseded rows filtered. |
| `delete_memory` | Remove a memory. |
| `list_memories` | Enumerate (use sparingly; prefer recall). |

`save_memory`/`get_memory` are the **per-agent JSONB KV store** — agent runtime state, not human-readable memory. Don't confuse them with the personal-memory tools above.

### Calendar (Microsoft Graph)
`get_calendar`, `calendar_list_events`, `calendar_create_event`, `calendar_update_event`, `calendar_delete_event`.

### SMS (Telnyx)
`send_sms_message`, `check_sms_replies`, `list_sms_conversation`, `close_sms_thread`. Profile: `Athena-Internal`, sender `+14258008358`. See the `reference_telnyx_sms` memory for schema details.

### Templates / drafting
`list_legal_templates`, `fill_legal_template`.

### Context bootstraps (call early in each session)
`get_william_context` (one-shot: profile + identity + memories + tasks + deadlines), `get_agent_context`, `get_database_stats`, `recent_activity`, `ingestion_status`.

---

## Conventions for AI agents

1. **Bootstrap each session with `get_william_context()`** before answering non-trivial questions; follow with `recall_personal_memories(<topic>)`.
2. **Document substantive work product** with `create_note(...)` (case-tagged) or `create_document(...)` (searchable artifact). Don't lose findings to the chat transcript alone.
3. **Use schema-qualified PostgreSQL** in `execute_sql` — `legal.*`, `core.*`, `medical.*`. tsvector/tsquery for full-text; JSONB operators for structured filters.
4. **Prefer the bulk tools** — `get_case_report(case_number)` over five separate fetches; `get_all_cases_summary()` for inventory.
5. **For mail moves, store `internet_message_id`** (not `message_id`) when you need a durable reference — see id-reissue note above.
6. **For attachments / binary documents, use the HTTP transfer path** (`create_document_download_session` → GET; `create_document_upload_session` → PUT with `Content-Range`). The legacy base64 path was retired.
7. **`record_feedback`** when the user corrects you or states a rule; lead with the rule, add `Why:`.
8. **Never fabricate** — if unsure, search. Flag deadlines as high-priority notes the moment you spot risk.

---

## Operational notes

- **Logging.** Every `@logged_tool` call writes a row to `core.tool_invocations` with truncated args, status, and duration. `mcp_server_v2.log` carries the human-readable stream.
- **Scopes.** Read-only tools require `read`; mutating tools call `_require_scope("write")` internally. Document upload/download sessions are gated by `write`.
- **Mailbox default.** All mail tools accept a `mailbox` argument that defaults to `DEFAULT_ATHENA_MAILBOX` (Athena's mailbox). Pass an explicit address to operate on William's mailbox or a shared box.
- **Graph well-known folders.** The constant `WELL_KNOWN_MAIL_FOLDERS` enumerates the 17 reserved names Graph accepts as literal `destinationId`s; anything else is resolved by `_resolve_mail_folder_id` (display-name path or raw id).
