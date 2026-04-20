# Athena Cognitive Engine — Reliability & Performance Optimization
**Last Updated**: April 5, 2026
**Status**: Ongoing — production system

---

## Architecture Overview

The platform uses a layered reliability architecture:

1. **LLM Layer** — Multi-provider routing with automatic failover (OpenAI → Anthropic API → Claude Code CLI → Ollama)
2. **Agent Layer** — 13 autonomous agents with cron scheduling, priority dispatch, and concurrency control
3. **Database Layer** — PostgreSQL 17 + pgvector, monitored by DBA agent
4. **Recovery Layer** — Self-Healing agent, Code Doctor auto-remediation, circuit breakers

---

## Reliability Features

### Multi-Provider LLM Failover (April 2026)
When the primary LLM provider (OpenAI) is unavailable, the framework automatically falls through to alternative providers:

```
OpenAI (primary) → Anthropic API → Claude Code CLI → Ollama (local)
```

- **framework.py**: `llm_chat()` and `llm_json()` try OpenAI first, catch failures, then delegate to model_router
- **model_router.py**: Task-aware routing with per-provider circuit breakers
- **Claude Code CLI**: Uses Max subscription OAuth — no API key required, zero incremental cost
- **Task classification**: Routes legal/medical to stronger models (Opus), triage to lightweight (Haiku)

### Retry with Exponential Backoff
All LLM calls use `@retry_with_backoff` decorator (3 attempts, 1s → 2s → 4s delays).

### Circuit Breaker Pattern
Per-provider circuit breakers prevent cascade failures:
- **Threshold**: 5 consecutive failures → circuit opens
- **Timeout**: 60 seconds → half-open (allows one test request)
- **Recovery**: 2 consecutive successes → circuit closes

### Agent Scheduling Fix (April 2026)
Fixed a critical scheduling bug where low-priority, infrequent agents were permanently starved:
- **Root cause**: `_should_run()` had a 60-second first-tick window + concurrency starvation
- **Fix**: Bootstrap `_last_runs` from database on startup; seed from previous cron fire time
- **Ghost cleanup**: Agents registered in DB but removed from code are auto-deactivated on startup

---

## Monitoring & Observability

### Database Tables
- `ops.agent_runs` — Every agent execution with duration, status, metrics
- `ops.agent_findings` — Issues discovered by agents (severity: info/warning/critical)
- `ops.agent_registry` — Agent fleet registry with run counts, error rates
- `ops.mcp_query_log` — MCP tool call analytics
- `ops.health_checks` — System health metrics
- `ops.notification_queue` — Buffered notifications for Daily Digest
- `ops.agent_chat` — Inter-agent communication and @mentions

### Key Metrics (queried from ops tables)
```sql
-- Agent performance
SELECT agent_id, COUNT(*), AVG(duration_ms),
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)
FROM ops.agent_runs WHERE started_at > now() - interval '7 days'
GROUP BY agent_id;

-- Open findings by severity
SELECT severity, COUNT(*) FROM ops.agent_findings
WHERE status = 'open' GROUP BY severity;
```

### Agent-Level Monitoring
| Agent | What It Monitors |
|-------|-----------------|
| Watchdog | CPU, memory, disk, services, logs, latency regressions |
| DBA | Connections, bloat, slow queries, schema drift, indexes |
| Security Sentinel | OAuth access, PII exposure, unknown clients |
| Data Quality | Embeddings, OCR, duplicates, orphaned records |
| Self-Healing | Stuck connections, dead services, integration tests |
| Orchestrator | Fleet health, consecutive failures, finding conflicts |

---

## Systemd Services

| Service | Status | Description |
|---------|--------|-------------|
| `wdws-agents` | Active | Agent fleet scheduler |
| `wdws-mcp` | Active | MCP server (port 9200) |
| `wdws-dashboard` | Active | Dashboard (port 9100) |
| `wdws-docx-proxy` | Active | Document proxy |
| `wdws-imessage-proxy` | Active | iMessage proxy |
| `wdws-word-mcp` | Active | Word MCP |
| `wdws-investigator-mcp` | Active | Investigator MCP |
| `wdws-paperless-mcp` | Active | Paperless-NGX MCP |

---

## Historical Issues Resolved

### Service Name Conflict (Feb 2026)
- Old `nelson-dashboard` conflicted with `athena-dashboard` on port 9100
- Resolved: removed legacy service, enabled `athena-dashboard` as primary

### Agent Scheduling Starvation (Feb–Apr 2026)
- 4 agents (Case Strategy, Retention, Query Insight, Quality Eval) stopped running Feb 9
- Root cause: narrow 60-second first-tick window + concurrency starvation
- Fix: DB-bootstrapped `_last_runs` + cron-seeded fallback

### Ghost Agent Cleanup (Apr 2026)
- 5 agents deleted from code but still registered in DB (db-tuner, email-triage, software-engineer, scorecard, timeline)
- Fix: Auto-deactivation on startup for agents not in current `ALL_AGENTS`

### OpenAI API Outage Resilience (Apr 2026)
- Previously: OpenAI 401/outage → all LLM-dependent agents returned placeholder text
- Fix: Multi-provider fallback via model_router (Anthropic → Claude CLI → Ollama)
