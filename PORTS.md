# Port reservations

Canonical list of TCP ports used or reserved by services on the Athena host
(`172.16.32.207`). **Check here before picking a port for a new service.**

Ports are loopback-only (`127.0.0.1`) unless explicitly noted. External
exposure is done via the `tukwila-colo-tunnel` Cloudflare tunnel (see
`dropbox_integration_design.md` §16.3 and §16.8 for edit discipline).

| Port  | Service                          | Binding     | Notes |
|-------|----------------------------------|-------------|-------|
| 1143  | (reserved, code ref)             | —           | Referenced in code; purpose unconfirmed — treat as reserved |
| 3389  | RDP                              | host        | OS-level |
| 5432  | PostgreSQL                       | 127.0.0.1   | Primary DB (`wdws` + `athena_chat`) |
| 6379  | Redis                            | 127.0.0.1   | Cache / task queue |
| 8080  | Phase 0 placeholder — `connect-athena`         | 127.0.0.1 | Dropbox Phase 1 landing (placeholder until replaced) |
| 8081  | Phase 0 placeholder — `hooks-athena`           | 127.0.0.1 | Dropbox Phase 1 webhook target (placeholder until replaced) |
| 8090  | Phase 0 placeholder — `connect-athena-staging` | 127.0.0.1 | Dropbox staging (placeholder until replaced) |
| 9000  | (reserved)                       | —           | Reserved — historical use |
| 9001  | (reserved)                       | —           | Reserved — historical use |
| 9002  | (reserved)                       | —           | Reserved — historical use |
| 9097  | (reserved, code ref)             | —           | Referenced in code |
| 9098  | (reserved)                       | —           | Reserved |
| 9099  | (reserved)                       | —           | Reserved |
| 9100  | Athena dashboard (`app.py`)      | 127.0.0.1   | UI |
| 9101  | Attachment server                | 127.0.0.1   | `attachment_server.py` |
| 9110  | **Telnyx SMS webhook**           | 127.0.0.1   | `telnyx_webhook.py` — inbound SMS from Telnyx, routed via `klunky.12432.net/telnyx/*` |
| 9199  | (reserved)                       | —           | Reserved |
| 9200  | Athena MCP server (HTTP)         | 127.0.0.1   | `mcp-server/mcp_server_v2.py --http --port 9200`; routed via `klunky.12432.net/*` |
| 9201  | (reserved)                       | —           | Reserved |
| 9300  | Embedding HTTP (`embedding_http.py`) | 127.0.0.1 | BGE-M3 / rerank |
| 9301  | (reserved)                       | —           | Reserved |
| 9350  | (reserved)                       | —           | Reserved |
| 9400  | iMessage MCP auth proxy          | 0.0.0.0     | `wdws-imessage-proxy.service` (`imessage-proxy/auth_proxy.py`) — running since 2026-04-15; bound on all interfaces |
| 9401  | ACE Orchestrator                 | 127.0.0.1   | `ace-orchestrator.service` — FastAPI prompt-assembly + Opus routing (Phase 1, internal only) |
| 9500  | (reserved)                       | —           | Reserved |
| 9797  | (reserved)                       | —           | Reserved |
| 9999  | (reserved, code ref)             | —           | Referenced in code |
| 11434 | Ollama                           | host        | Local model runtime |

## Free ranges (pick from here for new services)

- `9102` – `9109`
- `9111` – `9198`
- `9202` – `9299`
- `9302` – `9349`
- `9351` – `9399`
- `9402` – `9499`
- `9501` – `9796`
- `9798` – `9998`

## When adding a new port

1. Pick an unused port from a free range above.
2. Add a row to the table with service name, binding, and notes.
3. If externally exposed, add the Cloudflare tunnel ingress rule per §16.8
   (atomic PUT + canary + drift check).
4. Commit the updated `PORTS.md` alongside the service code in the same PR.
