# Athena Cognitive Engine — Quality & Reliability Scorecard

This document defines production-level quality targets and how they are measured.

---

## Product Goals

### Core Outcomes
1. **Accurate retrieval** of legal/medical evidence with minimal hallucination risk.
2. **Consistent tool selection** (semantic vs. structured SQL) without manual intervention.
3. **Stable operations** under normal and burst traffic.
4. **Actionable answers** with citations that reduce manual review time.
5. **LLM resilience** — platform functions even when primary LLM provider is unavailable.

### Non-Goals
- Training custom foundation models.
- Replacing legal counsel judgment.
- Automatic destructive actions without human approval.

---

## Quality Metrics

### Retrieval Quality (RAG)
- **Eval pass rate** >= 0.85 (weekly median)
- **Eval failure rate** <= 0.15
- **Avg results per eval** >= 5

### Safety Signals
- **PII exposure rate** tracked (allowed in legal/medical context, monitored)
- **No uncited claims** for legal summaries

### Agent Reliability
- **Agent error rate** <= 3% over last 24h
- **Agent scheduling**: All registered agents must run within their expected interval
- **Orchestrator critical finding agreement** >= 90%

### Latency & Performance
- **MCP tool call p95** <= 2500ms
- **MCP tool call p99** <= 25000ms
- **Agent run p95** <= 15000ms

### LLM Availability
- **Primary provider failure** must trigger automatic fallback within 1 request
- **Fallback chain**: OpenAI -> Anthropic API -> Claude Code CLI -> Ollama
- **Target**: >= 99% LLM availability across all providers combined

### Availability
- **MCP service uptime** >= 99.5%
- **Dashboard uptime** >= 99.5%
- **PostgreSQL uptime** >= 99.9%

---

## Instrumentation

| System | What It Tracks | Table |
|--------|---------------|-------|
| Quality Eval agent | RAG pass/fail + PII safety | `ops.agent_runs` (metrics JSONB) |
| Watchdog agent | System metrics, latency regressions | `ops.health_checks` |
| MCP query log | Tool call duration, errors, result counts | `ops.mcp_query_log` |
| Agent runs | Duration, status, error traces | `ops.agent_runs` |
| Model router | Provider used, fallback events, latency | Agent logs |
| Daily Digest | Weekly scorecard on Mondays | Email + `ops.notification_queue` |

---

## Ownership

| Metric Area | Owner Agent |
|------------|-------------|
| Eval failures & regressions | Quality Eval, Orchestrator |
| Tool improvement suggestions | Query Insight |
| Latency regressions | Watchdog, DBA |
| PII exposure spikes | Security Sentinel, Retention |
| LLM provider health | Model Router (framework.py) |
| Weekly scorecard delivery | Daily Digest (Monday edition) |
| Fleet scheduling health | Orchestrator |

---

## Iteration Cadence

- **Daily**: Review eval pass rate + open findings (via Daily Digest email)
- **Weekly**: Adjust eval cases and thresholds (Monday scorecard)
- **Monthly**: Expand eval suite (new query types, edge cases)

---

## Notes
This scorecard is a living document and should evolve as the platform grows.
Last updated: April 5, 2026.
