# Athena Cognitive Platform — Quality & Reliability Scorecard

This document defines **what production-level quality means for this platform**, and how we measure it.

---

## 🎯 Product Goals (Scope & Quality Targets)

### Core Outcomes
1. **Accurate retrieval** of legal/medical evidence with minimal hallucination risk.
2. **Consistent tool selection** (semantic vs. structured SQL) without manual intervention.
3. **Stable operations** under normal and burst traffic.
4. **Actionable answers** with citations that reduce manual review time.

### Non‑Goals (for now)
- Training custom foundation models.
- Replacing legal counsel judgment.
- Automatic destructive actions without human approval.

---

## 📏 Quality Metrics (Measured)

### Retrieval Quality (RAG)
- **Eval pass rate** ≥ 0.85 (weekly median)
- **Eval failure rate** ≤ 0.15
- **Avg results per eval** ≥ 5

### Safety Signals
- **PII exposure rate** tracked (allowed in legal/medical context, but monitored)
- **No uncited claims** for legal summaries

### Agent Accuracy
- **Agent error rate** ≤ 3% over last 24h
- **Orchestrator critical finding agreement** ≥ 90%

### Latency & Performance
- **MCP tool call p95** ≤ 2500ms
- **MCP tool call p99** ≤ 25000ms
- **Agent run p95** ≤ 15000ms

### Availability
- **MCP service uptime** ≥ 99.5%
- **Dashboard uptime** ≥ 99.5%
- **PostgreSQL uptime** ≥ 99.9%

---

## ✅ Current Instrumentation (in place)

- **Eval Agent** (`quality-eval`) with pass/fail + safety tracking
- **Watchdog metrics** stored in `ops.health_checks`
- **MCP analytics** in `ops.mcp_query_log`
- **Agent runs + errors** in `ops.agent_runs`
- **Dashboard summaries** in Analytics → Quality & Reliability

---

## 📌 Baseline (last 7d as of 2026‑02‑09)

- **Eval pass rate**: 1.00 (n=2) — 14/14 passed, 1 skipped
- **Avg results per eval**: 8.0
- **Agent error rate**: 1.53% (20 / 1308 runs)
- **Agent p95**: 9656.6ms
- **MCP tool call p95**: 2073.3ms
- **MCP tool call p99**: 22599.4ms
- **MCP error rate**: 0% (0 / 347 calls)

---

## 🔁 Iteration Cadence

- **Daily**: Review eval pass rate + open findings
- **Weekly**: Adjust eval cases and thresholds
- **Monthly**: Expand eval suite (new query types, edge cases)

---

## ✅ Ownership & Actions

- **Orchestrator**: triages eval failures + flags regressions
- **Query Insight Agent**: suggests tool improvements
- **DB Tuner**: optimizes latency regressions
- **Security Sentinel**: flags PII exposure spikes

---

## Notes
This scorecard is a living document and should evolve as the platform grows.
