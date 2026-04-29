# Legacy `mcp_server.py` retirement — closeout

**Branch:** `sprint-a-privilege-phi`
**Executed:** 2026-04-29 PDT, single Claude session.
**Outcome:** Legacy MCP server source removed from active codebase. The running surface (`mcp_server_v2.py` on klunky.12432.net) is unaffected.

---

## 1. Why retire

Earlier in this session, [mcp-server/mcp_server_v2.py](../mcp-server/mcp_server_v2.py) (the file actually served at `klunky.12432.net:9200` by `wdws-mcp.service`) had its base64 chunked download tool (`download_original_document`) removed, on the principle that **all document upload and download must go over raw HTTP** — `PUT` to a session `upload_url` with `Content-Range`, `GET` from a session `download_url` with optional `Range: bytes=...`. The HTTP routes (`/transfer/upload/{token}` and `/transfer/download/{token}`) and their MCP tool wrappers (`create_document_upload_session`, `create_document_download_session`) already existed and remain the only file-transfer surface.

The legacy file `mcp-server/mcp_server.py` still defined:
- A base64 `download_original_document` tool (the very surface we just removed from v2).
- Pre-rename tool names (`lookup_case`, `search_medical_records`) that no longer exist in v2.
- An obsolete OAuth/tool surface that has diverged from the canonical v2 implementation.

Risk if left in place: a future operator could run the wrong file (`mcp_server.py` instead of `mcp_server_v2.py`) and silently re-expose the base64 transfer path that the user has explicitly disallowed. The systemd unit currently points at v2, but nothing about the directory layout enforces that — both files lived side by side, both importable, both runnable with the same `--http --port 9200` invocation.

## 2. Operational status (pre-retirement)

| Property | Value |
|---|---|
| File path | `/opt/wdws/mcp-server/mcp_server.py` |
| Size | 134,621 bytes |
| Last modified | 2026-04-24 09:59 |
| First commit | `319015b` (initial commit) |
| Most recent commit touching it | `f512216` (2026-04-19) |
| Loaded by any running process | **No** |
| Loaded by any systemd unit | **No** (`wdws-mcp.service` runs `mcp_server_v2.py`) |
| Imported by anything in repo | Only `test_mcp.py` (smoke test, also retired in this work) |
| Doc references | `PORTS.md:29` (incorrect path — fixed in this work); `mcp_server_v1_backup.py:14-15` (already a backup file, not active doc); `SPRINT_A_WORK_ORDER_v2.{1,2}.md` (generic conceptual references, untouched) |

The `acp-mcp.service` systemd unit *also* runs a file named `mcp_server.py`, but at a different path: `/opt/acp/api/mcp_server.py` on port 9201. That is a separate file (Athena Cognitive Platform v3 surface) and is **not** part of this retirement.

## 3. Retirement actions taken

| Step | Action | Result |
|---|---|---|
| Prepend retirement notice | Added `# RETIRED — 2026-04-29` comment block at top of `mcp_server.py`, replacing the misleading "v2.0" docstring header | File self-documents its retired state |
| Prepend retirement notice | Added matching notice to `test_mcp.py` (smoke test; imports legacy module's pre-rename tool names) | Test file marked inert |
| Rename source files | `git mv mcp-server/mcp_server.py mcp-server/mcp_server.py.retired` and same for `test_mcp.py` | Files preserved, no longer importable as `mcp_server` / `test_mcp` |
| Fix doc reference | `PORTS.md:29` updated from `mcp_server.py` to `mcp_server_v2.py` (the file the wdws-mcp.service ExecStart actually invokes) | Doc now matches running configuration |
| Service restart | None needed | `wdws-mcp.service` was already running `mcp_server_v2.py`; retiring the unused legacy file does not affect the live surface |

## 4. Preserved evidence

- **`mcp_server.py.retired`** — full source preserved (modulo the retirement-notice prepend and removal of the misleading shebang/docstring header).
- **`test_mcp.py.retired`** — full source preserved.
- **`mcp_server_v1_backup.py`** — earlier snapshot in same directory, untouched.
- **`mcp_server_v2.py.pre_logging_backup`** — untouched.
- **`__pycache__/mcp_server.cpython-313.pyc`** — left in place. Will be orphaned on next bytecode-cache update; inert (Python won't load a `.pyc` whose source has been renamed away under the standard import machinery).
- **Git history** — no rewrites. The rename uses `git mv` so blame/history follow through.

## 5. Verification

- `mcp_server_v2.py` syntax-checks clean (`ast.parse` OK).
- `wdws-mcp.service` is `active`; edge probe `https://klunky.12432.net/mcp/sse` returns the expected 401 OAuth challenge.
- `grep -rn 'download_original_document' mcp-server/mcp_server_v2.py` → no matches (the v2 base64 tool is gone).
- `grep -rn 'mcp_server\.py' --include='*.py' /opt/wdws/` → only references are within the retired file itself and the v1 backup. No live code path imports it.
- `ls mcp-server/` confirms both files now have `.retired` suffix.

## 6. Open items

### 6.1 `acp-mcp.service` (`/opt/acp/api/mcp_server.py`) base64 audit

This retirement is scoped to `/opt/wdws/mcp-server/mcp_server.py` (the legacy WDWS MCP surface). The other file named `mcp_server.py` — at `/opt/acp/api/mcp_server.py`, served on port 9201 by `acp-mcp.service` — was **not** examined in this work. If the "HTTP-only file transfer" policy applies to the ACP surface as well, that file should be audited separately for any `data_base64` or `b64encode` document-transfer paths.

### 6.2 `PORTS.md` line 30 (`9201 reserved`)

`PORTS.md:30` lists port 9201 as "(reserved)", but `acp-mcp.service` is actively listening on 9201. Out of scope for this work order; flag for a future doc-accuracy pass.

### 6.3 Stale `.bak` files in mcp-server/

`mcp-server/.env.bak`, `.env.bak2`, `.env.bak.1777279319` — these are environment-file backups (potential credential history). Not touched by this retirement. Should be reviewed and pruned in a future cleanup pass.

---

*Written by Claude Opus 4.7, 2026-04-29 PDT.*
