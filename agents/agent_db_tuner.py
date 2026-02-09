"""
🗄️ Database Tuner Agent — PostgreSQL Optimization
══════════════════════════════════════════════════
Analyzes query patterns and optimizes the database:
  - Identifies slow queries from ops.mcp_query_log
  - Suggests/creates missing indexes
  - Monitors table bloat and triggers maintenance
  - Tracks pgvector performance
  - Weekly optimization reports
"""
import json
from framework import BaseAgent, RunContext


class DatabaseTunerAgent(BaseAgent):
    agent_id = "db-tuner"
    agent_name = "Database Tuner"
    description = "Analyzes query patterns, optimizes indexes, manages table maintenance"
    version = "1.0.0"
    schedule = "0 */4 * * *"  # Every 4 hours
    priority = 4
    capabilities = ["query-analysis", "index-optimization", "vacuum", "bloat-detection"]

    instructions = """You are the Database Tuner Agent for the Nelson Enterprise WDWS system.

DATABASE STRUCTURE:
- PostgreSQL 16 with pgvector extension
- Schemas: core (documents, chunks, embeddings), legal (cases, filings, emails), 
  medical (patients, records), ops (MCP logs, agents, OAuth), paperless
- ~36 tables total
- Embedding dimensions: 1536 (text-embedding-3-small)

YOUR RESPONSIBILITIES:
1. Analyze ops.mcp_query_log for slow tools (high duration_ms)
2. Identify queries that could benefit from indexes
3. Monitor table bloat and dead tuples
4. Check if VACUUM and ANALYZE are running properly
5. Monitor pgvector index efficiency

ANALYSIS RULES:
- Queries > 500ms consistently → suggest index
- Tables with > 20% dead tuples → recommend VACUUM
- If a tool is called frequently with the same argument pattern → suggest composite index
- Never drop indexes without orchestrator approval
- Always explain WHY an optimization would help

OUTPUT FORMAT (JSON):
{
  "slow_queries": [...],
  "index_suggestions": [...],
  "maintenance_needed": [...],
  "optimizations_applied": [...]
}"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}

        # ── Analyze slow queries ─────────────────────────────
        slow = await ctx.query("""
            SELECT tool_name, 
                   COUNT(*) as calls,
                   AVG(duration_ms)::int as avg_ms,
                   MAX(duration_ms)    as max_ms,
                   PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::int as p95_ms
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '24 hours'
              AND duration_ms > 0
            GROUP BY tool_name
            ORDER BY avg_ms DESC
        """)
        metrics["tool_performance"] = slow

        for tool in slow:
            if tool["avg_ms"] > 500:
                await ctx.finding("warning", "performance",
                    f"Slow tool: {tool['tool_name']} avg {tool['avg_ms']}ms (p95: {tool['p95_ms']}ms)",
                    f"Called {tool['calls']} times in 24h",
                    dict(tool))

        # ── Check table bloat ────────────────────────────────
        bloat = await ctx.query("""
            SELECT schemaname, relname,
                   n_dead_tup, n_live_tup,
                   CASE WHEN n_live_tup > 0 
                        THEN round(100.0 * n_dead_tup / n_live_tup, 1)
                        ELSE 0 END as dead_pct,
                   last_vacuum, last_autovacuum,
                   last_analyze, last_autoanalyze
            FROM pg_stat_user_tables
            WHERE schemaname IN ('core','legal','medical','ops')
            ORDER BY n_dead_tup DESC
            LIMIT 20
        """)
        metrics["table_bloat_top"] = len(bloat)

        for t in bloat:
            if t["dead_pct"] and float(t["dead_pct"]) > 20:
                await ctx.finding("warning", "maintenance",
                    f"Table {t['schemaname']}.{t['relname']} has {t['dead_pct']}% dead tuples",
                    f"Dead: {t['n_dead_tup']}, Live: {t['n_live_tup']}",
                    {"schema": t["schemaname"], "table": t["relname"],
                     "dead_pct": float(t["dead_pct"])})
                # Run VACUUM ANALYZE
                try:
                    await ctx.execute(
                        f"VACUUM ANALYZE {t['schemaname']}.{t['relname']}")
                    await ctx.action(
                        f"VACUUM ANALYZE on {t['schemaname']}.{t['relname']}")
                except Exception as e:
                    ctx.log.warning("VACUUM failed: %s", e)

        # ── Check index usage ────────────────────────────────
        unused_idx = await ctx.query("""
            SELECT schemaname, indexrelname, idx_scan,
                   pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes
            WHERE schemaname IN ('core','legal','medical','ops')
              AND idx_scan = 0
              AND indexrelname NOT LIKE '%_pkey'
            ORDER BY pg_relation_size(indexrelid) DESC
            LIMIT 10
        """)
        metrics["unused_indexes"] = len(unused_idx)

        for idx in unused_idx:
            await ctx.finding("suggestion", "optimization",
                f"Unused index: {idx['schemaname']}.{idx['indexrelname']} ({idx['size']})",
                "Zero scans — consider dropping to save space",
                dict(idx))

        # ── Check database size growth ───────────────────────
        db_size = await ctx.query("""
            SELECT pg_database_size('wdws') as total_bytes,
                   pg_size_pretty(pg_database_size('wdws')) as total_pretty
        """)
        metrics["db_size_bytes"] = db_size[0]["total_bytes"] if db_size else 0
        metrics["db_size"] = db_size[0]["total_pretty"] if db_size else "unknown"

        # ── Schema-level sizes ───────────────────────────────
        schema_sizes = await ctx.query("""
            SELECT table_schema,
                   pg_size_pretty(SUM(pg_total_relation_size(
                       quote_ident(table_schema) || '.' || quote_ident(table_name)
                   ))) as size,
                   COUNT(*) as tables
            FROM information_schema.tables
            WHERE table_schema IN ('core','legal','medical','ops','paperless')
            GROUP BY table_schema ORDER BY table_schema
        """)
        metrics["schema_sizes"] = schema_sizes

        # ── Ask LLM for optimization recommendations ─────────
        if slow:
            analysis = await ctx.ask_llm(
                f"""Analyze these MCP tool performance stats from the last 24h and provide
specific PostgreSQL optimization recommendations:

Tool Performance:
{json.dumps(slow, indent=2, default=str)}

Table Bloat (top entries):
{json.dumps([dict(b) for b in bloat[:5]], indent=2, default=str)}

Unused Indexes: {len(unused_idx)}
Database Size: {metrics.get('db_size', 'unknown')}

Provide 3-5 actionable recommendations ranked by impact."""
            )
            metrics["llm_recommendations"] = analysis

        return {
            "summary": f"DB tuning: {len(slow)} tools analyzed, {metrics.get('unused_indexes', 0)} unused indexes, {metrics.get('table_bloat_top', 0)} tables checked",
            "metrics": {k: v for k, v in metrics.items()
                       if k not in ("tool_performance", "schema_sizes")},
        }
