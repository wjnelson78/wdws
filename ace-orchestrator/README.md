# ACE Orchestrator

FastAPI service that sits between Claude-facing clients and the Anthropic Claude API. Owns conversation state, system-prompt assembly, prompt-cache management, MCP server routing, and case-file preloading for the Athena AI ecosystem.

Phase 1 — headless API service only. No UI.

## Architecture

- **State DB:** dedicated `athena` PostgreSQL database (orchestrator schema). Portable, backed up independently.
- **ACE access:** read-only via `wdws` database (existing ACE schemas: `core`, `legal`, `medical`, `ops`, `paperless`).
- **MCP servers:** ACE, MS365 (Investigator wired but optional in Phase 1).
- **Models:** Opus 4.6 default; Opus 4.7 for drafting; Sonnet 4.6 for cost-sensitive paths.

## Run

```bash
uv sync
cp .env.example .env  # then fill in real values
alembic upgrade head
uv run fastapi dev src/orchestrator/main.py
```

Smoke test:

```bash
ORCHESTRATOR_API_KEY=... uv run python scripts/smoke_test.py
```

## Env vars

See [.env.example](.env.example).

## Layout

See spec §4. Briefly:

- `src/orchestrator/api/` — FastAPI routers
- `src/orchestrator/services/` — claude client, prompt assembler, cache strategist, mcp router, session manager, case preloader
- `src/orchestrator/db/` — SQLAlchemy models + async session
- `alembic/` — migrations
- `scripts/` — bootstrap and smoke scripts
- `tests/` — pytest suite
