# Athena Cognitive Engine

A comprehensive legal workflow automation system featuring email management, case tracking, autonomous AI agents, and Model Context Protocol (MCP) servers for intelligent document processing and legal analysis.

## Features

### Email Management
- Automated email ingestion and classification
- AI-powered email classification with entity extraction
- Attachment extraction and processing
- Email-to-case linking and tracking
- Direct Microsoft Graph mailbox actions from the Athena Cognitive Engine MCP
   - Live mailbox read access
   - Send, reply, and reply-all
   - Mark messages important / read-unread
   - Request read and delivery receipts
   - Attach documents Athena generates via MCP document IDs

### Case Management
- Legal case tracking and organization
- Strategic case analysis with daily briefs
- Document relationship mapping

### MCP Servers
- Legal cases MCP server (OAuth 2.0 secured)
- Medical records MCP server
- Paperless-NGX integration
- Word/DOCX proxy, iMessage proxy, Investigator MCP

### Multi-Provider LLM Routing
The platform uses an intelligent model router with automatic failover:

| Priority | Provider | Models | Auth | Best For |
|----------|----------|--------|------|----------|
| 1 | Anthropic API | Haiku 4.5, Sonnet 4.6, Opus 4.6 | API key | Fast, cost-effective |
| 2 | Claude Code CLI | haiku, sonnet, opus | Max subscription (OAuth) | Zero-cost fallback |
| 3 | OpenAI | GPT-5.4, GPT-5.2-Codex | API key | Complex tasks |
| 4 | Ollama | llama3.2:3b | Local | Private/offline |

- Task-aware routing (triage, legal, medical, code, reasoning)
- Circuit breaker per provider with automatic recovery
- Privacy-aware: local-only mode for sensitive PII queries
- See [model_router.py](model_router.py) for configuration

### Intelligent Agents (13-agent fleet)

All agents run via `wdws-agents` systemd service, scheduled by cron expressions, with concurrency limiting and priority-based dispatch.

| Agent | Priority | Schedule | Role |
|-------|----------|----------|------|
| **Orchestrator** | P0 | `*/5 min` | Fleet manager — validates findings, resolves conflicts, escalates |
| **Athena** | P1 | `*/10 min` | Autonomous reasoning — LLM-driven decisions, schema migrations, code patches |
| **Code Doctor** | P1 | `*/10 min` | Auto-remediation — diagnoses failures, generates fixes via Codex |
| **Watchdog** | P1 | `*/2 min` | System monitor — CPU/memory/disk, log scanning, service health, Cloudflare tunnel |
| **Daily Digest** | P2 | `7 AM` | Notification aggregator — single daily email (weekly scorecard on Mondays) |
| **Security Sentinel** | P2 | `*/10 min` | OAuth/PII scanner — access patterns, threat detection, unknown client alerts |
| **Self-Healing** | P2 | `4 AM` + on-demand | Auto-repair — kills stuck connections, restarts services, cleans orphans |
| **DBA** | P2 | `3 AM` | Database admin — bloat/VACUUM, index analysis, schema drift, query termination |
| **Data Quality** | P3 | `*/6 hr` | Data integrity — missing embeddings, bad OCR, duplicates, orphaned records |
| **Quality Eval** | P3 | `*/12 hr` | RAG evaluation — retrieval scoring, PII safety checks, regression detection |
| **Case Strategy** | P4 | `6 AM` | Legal intelligence — cross-case analysis, party mapping, morning briefs |
| **Retention** | P4 | `Sun 3 AM` | Data governance — purges expired ops data, PII detection, litigation holds |
| **Query Insight** | P5 | `*/8 hr` | MCP analytics — tool usage patterns, search quality scoring |

**Agent Infrastructure:**
- [framework.py](agents/framework.py) — Base classes, LLM integration with multi-provider fallback, DB pooling, structured logging
- [run.py](agents/run.py) — Scheduler with cron + croniter, concurrency control, notification watcher for @mention wake-ups
- [reliability.py](agents/reliability.py) — Circuit breakers, retry with exponential backoff, rate limiting
- [model_router.py](model_router.py) — Multi-provider LLM routing with task classification and fallback chains
- [config.py](agents/config.py) — All configuration from `/opt/wdws/.env`

### Dashboard
- Real-time case and email monitoring
- Interactive chat interface with AI agents
- Document search and retrieval
- System status and health monitoring

## Architecture

```
/opt/wdws/
├── agents/              # AI agent framework and 13 agent implementations
│   ├── framework.py     # BaseAgent, RunContext, LLM helpers with fallback
│   ├── run.py           # Scheduler daemon (systemd: wdws-agents)
│   ├── config.py        # Environment configuration
│   ├── reliability.py   # Circuit breakers, retry, rate limiting
│   └── agent_*.py       # Individual agent implementations
├── model_router.py      # Multi-provider LLM router (Anthropic, Claude CLI, OpenAI, Ollama)
├── dashboard/           # Web dashboard (systemd: wdws-dashboard, port 9100)
├── mcp-server/          # Main MCP server (systemd: wdws-mcp, port 9200)
├── mcp-servers/         # Specialized MCP servers
│   └── paperless-ngx/   # Paperless-NGX integration
├── docx-proxy/          # Document proxy service
├── imessage-proxy/      # iMessage integration proxy
├── migrations/          # Database schema migrations (PostgreSQL 17 + pgvector)
├── data/                # Data storage (cases, emails, medical records)
└── patches/             # System patches and utilities
```

## Prerequisites

- Python 3.11+
- PostgreSQL 17 with pgvector extension
- At least one LLM provider configured (see Environment Variables)
- Claude Code CLI (for Max subscription fallback — `pip install claude-code`)
- nginx (for proxies)

## Environment Variables

Create a `.env` file in the project root:

```bash
# Database (required)
DATABASE_URL=postgresql://user:password@localhost:5432/wdws

# LLM Providers (at least one required)
OPENAI_API_KEY=sk-...                    # OpenAI — primary provider
ANTHROPIC_API_KEY=sk-ant-...             # Anthropic — fallback provider
# Claude Code CLI uses Max subscription OAuth — no key needed

# LLM Model Configuration
AGENT_LLM_MODEL=gpt-5.4                 # Default model
AGENT_LLM_MODEL_HIGH=gpt-5.4            # High-risk tasks (legal, medical)
AGENT_LLM_MODEL_LOW=gpt-5.4             # Low-risk tasks (triage, routing)
AGENT_LLM_MODEL_CODEX=gpt-5.2-codex     # Code generation
AGENT_LLM_ROUTING_ENABLED=true          # Enable task-aware model routing

# Agent Configuration
AGENT_MAX_CONCURRENT=3                   # Max agents running simultaneously
AGENT_TICK_SECONDS=30                    # Scheduler tick interval
AGENT_RUN_TIMEOUT_SECONDS=900            # Max agent run time (15 min)

# Microsoft Graph API (email notifications)
GRAPH_TENANT_ID=your_tenant_id
GRAPH_CLIENT_ID=your_client_id
GRAPH_CLIENT_SECRET=your_client_secret
GRAPH_SENDER_EMAIL=athena@yourdomain.com
# Optional override for AI-drafted email composition in the MCP server
ATHENA_EMAIL_DRAFT_MODEL=gpt-5.4

# Alerts
ALERT_EMAIL=you@yourdomain.com

# Server
MCP_PORT=9200
DASHBOARD_PORT=9100
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/wjnelson78/wdws.git
   cd wdws
   ```

2. Install Python dependencies:
   ```bash
   pip install -r agents/requirements.txt
   pip install -r requirements-chat.txt
   ```

3. Set up the database:
   ```bash
   psql -U postgres -f migrations/001_enterprise_schema.sql
   # Apply subsequent migrations in order (002, 003, ...)
   ```

4. Configure environment variables (see above)

## Usage

### Start the Agent System
```bash
# Systemd (recommended — runs as daemon)
sudo systemctl start wdws-agents
sudo systemctl status wdws-agents

# Or manually
cd agents && python run.py

# Run a single agent once (testing)
cd agents && python run.py --once watchdog

# Show fleet status
cd agents && python run.py --status
```

### Start the Dashboard
```bash
sudo systemctl start wdws-dashboard
# Or: cd dashboard && python app.py
```

### Start the MCP Server
```bash
sudo systemctl start wdws-mcp
# Or: cd mcp-server && python mcp_server_v2.py --http --port 9200
```

### Raw document transfer (preferred for binary files)

For larger documents and attachments, prefer the Graph-style raw HTTP transfer flow instead of base64 payloads.

Control plane via Athena MCP tools:
- `create_document_upload_session(...)` returns an `upload_url`
- `create_document_download_session(document_id=...)` returns a `download_url`
- `upload_document(...)` and `download_original_document(...)` remain available as base64 fallbacks

Data plane via raw HTTP:
- Upload with `PUT` to `upload_url` and a `Content-Range` header
- Poll upload status with `GET upload_url`
- Cancel an upload session with `DELETE upload_url`
- Download raw bytes with `GET download_url`
- Resume/partial reads with `Range: bytes=...`

Default server limits:
- Max file size: `100 MiB`
- Max upload chunk size: `4 MiB`
- Upload session TTL: `3600s`
- Download session TTL: `900s`

Quick examples:

```bash
# Upload raw bytes to an existing session URL
curl -X PUT "$UPLOAD_URL" \
   -H "Content-Range: bytes 0-1048575/7340032" \
   --data-binary @chunk.bin

# Check upload status
curl "$UPLOAD_URL"

# Download the full file
curl -L "$DOWNLOAD_URL" -o output.bin

# Download just the first KiB
curl -L "$DOWNLOAD_URL" -H "Range: bytes=0-1023" -o first-kib.bin
```

A small helper is included for the raw HTTP side-channel:

```bash
python mcp-server/raw_transfer_client.py upload --upload-url "$UPLOAD_URL" --file ./report.pdf
python mcp-server/raw_transfer_client.py status --upload-url "$UPLOAD_URL"
python mcp-server/raw_transfer_client.py download --download-url "$DOWNLOAD_URL" --output ./report.pdf
python mcp-server/raw_transfer_client.py head --download-url "$DOWNLOAD_URL"
```

There is also a full end-to-end example that uses the real MCP control plane to
create the session first, then performs the raw HTTP transfer automatically.
It expects a bearer token with the right scopes (`read` for downloads, `write`
for uploads):

```bash
export MCP_ACCESS_TOKEN="..."

# Upload via MCP + raw HTTP
python mcp-server/mcp_transfer_client.py upload \
   --endpoint http://127.0.0.1:9200/mcp/sse \
   --transfer-base-url http://127.0.0.1:9200 \
   --file ./report.pdf \
   --domain operations \
   --compute-sha256

# Download via MCP + raw HTTP
python mcp-server/mcp_transfer_client.py download \
   --endpoint http://127.0.0.1:9200/mcp/sse \
   --transfer-base-url http://127.0.0.1:9200 \
   --document-id "$DOCUMENT_ID" \
   --output ./report-copy.pdf \
   --overwrite

# Upload, download, and verify the bytes match
python mcp-server/mcp_transfer_client.py roundtrip \
   --endpoint http://127.0.0.1:9200/mcp/sse \
   --transfer-base-url http://127.0.0.1:9200 \
   --file ./sample.txt \
   --output ./sample.out.txt \
   --domain operations \
   --compute-sha256 \
   --overwrite
```

Why `--transfer-base-url`? Athena may advertise public transfer URLs based on
`MCP_BASE_URL`. When you are testing against a local server directly on `9200`,
that flag rewrites the returned session URL to the local base while preserving
the `/transfer/...` path and token.

### OAuth scope notes for transfer + mail tools

- `create_document_upload_session(...)` and mail-send/update tools require the `write` scope.
- `create_document_download_session(...)` requires the `read` scope.
- Trusted first-party MCP clients such as Claude Desktop and ChatGPT are issued their
   full registered scope set during authorize/refresh so reconnects do not silently
   downgrade them to read-only access.
- If a connector is still using an older token minted before this fix, reconnect or
   re-authorize it once to pick up the corrected scope grant.

### Systemd Services
| Service | Port | Description |
|---------|------|-------------|
| `wdws-agents` | — | Agent fleet scheduler daemon |
| `wdws-mcp` | 9200 | Main MCP server (OAuth secured) |
| `wdws-dashboard` | 9100 | Web dashboard |
| `wdws-docx-proxy` | — | Document proxy |
| `wdws-imessage-proxy` | — | iMessage proxy |
| `wdws-word-mcp` | — | Word/DOCX MCP server |
| `wdws-investigator-mcp` | — | Investigator MCP server |
| `wdws-paperless-mcp` | — | Paperless-NGX MCP server |

## Security

- Never commit `.env` files or API keys to version control
- OAuth 2.0 with client_secret_post validation on MCP server
- Microsoft Graph application permissions required for Athena email MCP actions:
   - `Mail.Send` — send new mail and send reply drafts
   - `Mail.ReadWrite` — read live mailbox content, create reply drafts, add attachments, and update message metadata
- The system handles sensitive legal and medical (HIPAA) information
- Security Sentinel agent monitors access patterns and PII exposure
- Retention agent enforces litigation holds and data governance
- Use HTTPS in production (Cloudflare Tunnel configured)

## License

Proprietary - All rights reserved

## Support

For issues and questions, please contact the development team.
