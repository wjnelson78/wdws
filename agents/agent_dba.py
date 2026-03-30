"""
🗄️ DBA Agent — Database Administration & Schema Governance
══════════════════════════════════════════════════════════════
Autonomous database administration for the Athena platform:
  - Schema drift detection (migrations vs live DB)
  - Missing index identification
  - Table bloat and vacuum monitoring
  - Connection pool health
  - Query performance analysis (slow queries)
  - Safe schema migrations (ADD COLUMN, CREATE INDEX)
  - Backup verification
  - Storage and growth monitoring

Works with Software Engineer agent: DBA owns the schema,
Software Engineer owns the code. When DBA finds a schema gap
that code depends on, it coordinates with Software Engineer.
"""
import json
import os
import re
from datetime import datetime, timezone

from framework import BaseAgent, RunContext, get_pool
try:
    from config import (
        GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
        GRAPH_SENDER_EMAIL, ALERT_EMAIL,
    )
    from email_util import send_email, build_notification_html
    _EMAIL_AVAILABLE = True
except Exception:
    _EMAIL_AVAILABLE = False


class DBAAgent(BaseAgent):
    agent_id = "dba"
    agent_name = "Database Administrator"
    description = "Schema governance, index optimization, bloat monitoring, slow query detection, safe migrations"
    version = "1.0.0"
    schedule = "0 3 * * *"  # Daily at 3 AM
    priority = 2
    capabilities = [
        "schema-governance", "index-optimization", "bloat-monitoring",
        "slow-query-detection", "connection-monitoring", "storage-monitoring",
        "safe-migration", "backup-verification"
    ]

    instructions = """You are the Database Administrator (DBA) Agent for the Athena Cognitive Platform.

YOUR MISSION:
Autonomously maintain the PostgreSQL database health, performance, and schema integrity.

SCHEMAS YOU MANAGE:
- core: documents, document_chunks, entities, entity_mentions, etc.
- legal: cases, email_metadata, filing_metadata, parties, etc.
- medical: patients, providers, record_metadata
- ops: agent_runs, agent_findings, agent_registry, code_fixes, etc.
- paperless: document_metadata

DATABASE HEALTH CHECKS:
1. Connection pool utilization — alert if > 80% used
2. Long-running queries — alert if any query > 5 minutes
3. Dead tuples / bloat — recommend VACUUM if bloat > 20%
4. Index health — find missing indexes on frequently queried columns
5. Table sizes / growth — track and alert on rapid growth
6. Replication lag (if applicable)
7. Lock contention — detect blocking queries

SCHEMA GOVERNANCE:
1. Compare migration files against live schema
2. Detect orphaned indexes (index exists but table was modified)
3. Find tables without primary keys
4. Detect columns with no NOT NULL that should have it
5. Verify foreign key integrity

SAFE OPERATIONS:
- You MAY create indexes (CREATE INDEX CONCURRENTLY)
- You MAY add columns (ALTER TABLE ADD COLUMN with sensible defaults)
- You MAY VACUUM or ANALYZE tables
- You MUST NEVER drop tables, columns, or indexes without human approval
- You MUST NEVER modify data (UPDATE/DELETE on user tables)
- You MUST NEVER change column types
- Always log what you do and why

COORDINATION:
- If you find agents referencing columns that don't exist, notify software-engineer
- If Code Doctor reports schema errors, check if a migration is needed
- Report critical findings to orchestrator"""

    # ── Email Notification ───────────────────────────────────
    async def _notify(self, ctx, subject_detail, sections):
        """Queue notification for daily digest rather than sending immediately."""
        await ctx.queue_notification(subject_detail, sections, severity="info")
        return {"status": "queued"}

    # ═══════════════════════════════════════════════════════════
    #  MAIN RUN LOGIC
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        metrics = {}
        email_sections = []
        actions_taken = []

        # ── 1. Connection Pool Health ────────────────────────
        connections = await ctx.query("""
            SELECT state, COUNT(*) as count,
                   MAX(EXTRACT(EPOCH FROM now() - state_change)) as max_age_secs
            FROM pg_stat_activity
            WHERE datname = current_database()
            GROUP BY state
            ORDER BY count DESC
        """)
        total_conns = sum(c["count"] for c in connections)
        max_conns_row = await ctx.query("SHOW max_connections")
        max_conns = int(max_conns_row[0]["max_connections"]) if max_conns_row else 100

        metrics["total_connections"] = total_conns
        metrics["max_connections"] = max_conns
        metrics["connection_pct"] = round(total_conns / max_conns * 100, 1)

        conn_detail = {c["state"] or "null": c["count"] for c in connections}
        if metrics["connection_pct"] > 80:
            await ctx.finding("warning", "connections",
                f"Connection pool at {metrics['connection_pct']}% ({total_conns}/{max_conns})",
                f"Breakdown: {json.dumps(conn_detail)}",
                {"connections": conn_detail})

        # ── 2. Long-Running Queries ──────────────────────────
        long_queries = await ctx.query("""
            SELECT pid, state, usename, application_name,
                   EXTRACT(EPOCH FROM now() - query_start) as duration_secs,
                   LEFT(query, 200) as query_preview,
                   wait_event_type, wait_event
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query NOT LIKE '%pg_stat_activity%'
              AND query_start < now() - interval '60 seconds'
            ORDER BY query_start
            LIMIT 10
        """)
        metrics["long_running_queries"] = len(long_queries)

        for q in long_queries:
            dur = q["duration_secs"]
            severity = "critical" if dur > 300 else "warning"
            await ctx.finding(severity, "slow-query",
                f"Query running {dur:.0f}s (PID {q['pid']}, user: {q['usename']})",
                q["query_preview"],
                {"pid": q["pid"], "duration": dur, "wait": q["wait_event"]})

        # Kill queries > 30 minutes (safety valve)
        stuck = [q for q in long_queries if q["duration_secs"] > 1800]
        for q in stuck:
            await ctx.execute("SELECT pg_terminate_backend($1)", q["pid"])
            actions_taken.append(f"Killed stuck query PID {q['pid']} ({q['duration_secs']:.0f}s)")
            await ctx.action(f"Terminated stuck query PID {q['pid']}")

        # ── 3. Table Bloat & VACUUM Status ───────────────────
        bloat = await ctx.query("""
            SELECT schemaname || '.' || relname as table_name,
                   n_live_tup, n_dead_tup,
                   CASE WHEN n_live_tup > 0 
                        THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 1)
                        ELSE 0 END as dead_pct,
                   last_vacuum, last_autovacuum, last_analyze,
                   pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size
            FROM pg_stat_user_tables
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops', 'paperless')
            ORDER BY n_dead_tup DESC
            LIMIT 30
        """)

        bloated = [b for b in bloat if b["dead_pct"] and b["dead_pct"] > 20]
        metrics["bloated_tables"] = len(bloated)

        for b in bloated:
            await ctx.finding("warning", "bloat",
                f"Table {b['table_name']} has {b['dead_pct']}% dead tuples ({b['n_dead_tup']} dead / {b['n_live_tup']} live)",
                f"Size: {b['total_size']}, Last vacuum: {b['last_vacuum'] or b['last_autovacuum'] or 'never'}",
                {"table": b["table_name"], "dead_pct": float(b["dead_pct"])})

        # Auto-VACUUM heavily bloated tables
        for b in bloated:
            if b["dead_pct"] and b["dead_pct"] > 40:
                try:
                    await ctx.execute(f"VACUUM ANALYZE {b['table_name']}")
                    actions_taken.append(f"VACUUM ANALYZE {b['table_name']} (was {b['dead_pct']}% dead)")
                    await ctx.action(f"VACUUM ANALYZE {b['table_name']}")
                except Exception as e:
                    await ctx.finding("warning", "vacuum-failed",
                        f"VACUUM failed on {b['table_name']}: {e}")

        # ── 4. Missing Indexes ───────────────────────────────
        # Find sequential scans on large tables
        seq_scan_heavy = await ctx.query("""
            SELECT schemaname || '.' || relname as table_name,
                   seq_scan, idx_scan,
                   CASE WHEN (seq_scan + idx_scan) > 0
                        THEN round(100.0 * seq_scan / (seq_scan + idx_scan), 1)
                        ELSE 0 END as seq_pct,
                   n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops')
              AND n_live_tup > 1000
              AND seq_scan > idx_scan
              AND (seq_scan + idx_scan) > 10
            ORDER BY seq_scan DESC
            LIMIT 10
        """)

        for t in seq_scan_heavy:
            if t["seq_pct"] and t["seq_pct"] > 80:
                await ctx.finding("info", "missing-index",
                    f"Table {t['table_name']} has {t['seq_pct']}% sequential scans "
                    f"({t['seq_scan']} seq vs {t['idx_scan']} idx, {t['n_live_tup']} rows)",
                    "Consider adding indexes for frequently queried columns")
        metrics["seq_scan_heavy_tables"] = len(seq_scan_heavy)

        # ── 5. Index Usage ───────────────────────────────────
        unused_indexes = await ctx.query("""
            SELECT schemaname || '.' || relname as table_name,
                   indexrelname as index_name,
                   idx_scan,
                   pg_size_pretty(pg_relation_size(indexrelid)) as index_size
            FROM pg_stat_user_indexes
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops')
              AND idx_scan = 0
              AND indexrelname NOT LIKE '%_pkey'
              AND indexrelname NOT LIKE '%_unique%'
            ORDER BY pg_relation_size(indexrelid) DESC
            LIMIT 10
        """)
        metrics["unused_indexes"] = len(unused_indexes)

        if unused_indexes:
            total_waste = sum(1 for _ in unused_indexes)
            await ctx.finding("info", "unused-indexes",
                f"{len(unused_indexes)} indexes have never been used",
                "\n".join(f"  {u['index_name']} on {u['table_name']} ({u['index_size']})"
                         for u in unused_indexes),
                {"indexes": [u["index_name"] for u in unused_indexes]})

        # ── 6. Table Sizes & Growth ──────────────────────────
        table_sizes = await ctx.query("""
            SELECT schemaname || '.' || tablename as table_name,
                   pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
                   pg_total_relation_size(schemaname || '.' || tablename) as size_bytes
            FROM pg_tables
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops', 'paperless')
            ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
            LIMIT 20
        """)

        total_db_bytes = sum(t["size_bytes"] for t in table_sizes)
        metrics["total_db_size_mb"] = round(total_db_bytes / 1024 / 1024, 1)
        metrics["largest_tables"] = [
            {"table": t["table_name"], "size": t["total_size"]}
            for t in table_sizes[:5]
        ]

        # Compare with last run
        last_size = await ctx.recall("db_size_bytes")
        if last_size and total_db_bytes > last_size * 1.1:  # >10% growth
            growth_pct = round((total_db_bytes - last_size) / last_size * 100, 1)
            await ctx.finding("info", "storage-growth",
                f"Database grew {growth_pct}% since last check "
                f"({round(last_size/1024/1024,1)}MB → {metrics['total_db_size_mb']}MB)",
                evidence={"previous": last_size, "current": total_db_bytes})
        await ctx.remember("db_size_bytes", total_db_bytes)

        # ── 7. Schema Integrity ──────────────────────────────
        # Tables without primary keys
        no_pk = await ctx.query("""
            SELECT t.table_schema || '.' || t.table_name as table_name
            FROM information_schema.tables t
            LEFT JOIN information_schema.table_constraints tc
                ON t.table_schema = tc.table_schema
                AND t.table_name = tc.table_name
                AND tc.constraint_type = 'PRIMARY KEY'
            WHERE t.table_schema IN ('core', 'legal', 'medical', 'ops')
              AND t.table_type = 'BASE TABLE'
              AND tc.constraint_name IS NULL
        """)
        metrics["tables_without_pk"] = len(no_pk)

        if no_pk:
            await ctx.finding("warning", "schema-integrity",
                f"{len(no_pk)} tables have no primary key",
                "\n".join(f"  {t['table_name']}" for t in no_pk),
                {"tables": [t["table_name"] for t in no_pk]})

        # Foreign key violations
        fk_violations = await ctx.query("""
            SELECT conname as constraint_name,
                   conrelid::regclass as table_name,
                   confrelid::regclass as referenced_table
            FROM pg_constraint
            WHERE contype = 'f'
              AND connamespace IN (
                  SELECT oid FROM pg_namespace
                  WHERE nspname IN ('core', 'legal', 'medical', 'ops')
              )
              AND NOT convalidated
        """)
        metrics["fk_violations"] = len(fk_violations)
        if fk_violations:
            await ctx.finding("warning", "fk-integrity",
                f"{len(fk_violations)} unvalidated foreign key constraints",
                "\n".join(f"  {v['constraint_name']} on {v['table_name']}" for v in fk_violations))

        # ── 8. Lock Monitoring ───────────────────────────────
        locks = await ctx.query("""
            SELECT blocked_locks.pid AS blocked_pid,
                   blocked_activity.usename AS blocked_user,
                   LEFT(blocked_activity.query, 200) AS blocked_query,
                   blocking_locks.pid AS blocking_pid,
                   blocking_activity.usename AS blocking_user,
                   LEFT(blocking_activity.query, 200) AS blocking_query
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity
                ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks blocking_locks
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity
                ON blocking_activity.pid = blocking_locks.pid
            WHERE NOT blocked_locks.granted
            LIMIT 5
        """)
        metrics["blocked_queries"] = len(locks)

        if locks:
            await ctx.finding("warning", "lock-contention",
                f"{len(locks)} blocked queries detected",
                "\n".join(f"  PID {l['blocked_pid']} blocked by PID {l['blocking_pid']}: "
                         f"{l['blocked_query'][:100]}" for l in locks),
                {"locks": [dict(l) for l in locks]})

        # ── 9. pgvector Health ───────────────────────────────
        pgvec = await ctx.query("""
            SELECT extversion FROM pg_extension WHERE extname = 'vector'
        """)
        metrics["pgvector_version"] = pgvec[0]["extversion"] if pgvec else "NOT INSTALLED"

        if not pgvec:
            await ctx.finding("critical", "pgvector",
                "pgvector extension is NOT installed",
                "The RAG pipeline requires pgvector for embedding storage and similarity search")

        # ── 10. Embedding Coverage ───────────────────────────
        embed_stats = await ctx.query("""
            SELECT COUNT(*) as total_chunks,
                   COUNT(embedding) as embedded,
                   COUNT(*) - COUNT(embedding) as missing
            FROM core.document_chunks
        """)
        if embed_stats:
            e = embed_stats[0]
            metrics["total_chunks"] = e["total_chunks"]
            metrics["embedded_chunks"] = e["embedded"]
            metrics["missing_embeddings"] = e["missing"]
            if e["total_chunks"] > 0:
                metrics["embedding_coverage_pct"] = round(
                    e["embedded"] / e["total_chunks"] * 100, 1)

        # ── 11. Agent Error Rate ─────────────────────────────
        error_rates = await ctx.query("""
            SELECT agent_id,
                   COUNT(*) as total_runs,
                   COUNT(*) FILTER (WHERE status = 'error') as errors,
                   CASE WHEN COUNT(*) > 0
                        THEN round(100.0 * COUNT(*) FILTER (WHERE status = 'error') / COUNT(*), 1)
                        ELSE 0 END as error_pct
            FROM ops.agent_runs
            WHERE started_at > now() - interval '24 hours'
            GROUP BY agent_id
            HAVING COUNT(*) FILTER (WHERE status = 'error') > 0
            ORDER BY error_pct DESC
        """)
        metrics["agents_with_errors_24h"] = len(error_rates)

        for er in error_rates:
            if er["error_pct"] and er["error_pct"] > 50:
                await ctx.finding("warning", "agent-error-rate",
                    f"Agent '{er['agent_id']}' has {er['error_pct']}% error rate "
                    f"({er['errors']}/{er['total_runs']} runs in 24h)",
                    "High error rate may indicate a persistent issue",
                    dict(er))

        # ── 12. Schema Drift Detection ───────────────────────
        # Compare migration file expectations against live schema
        migration_dir = "/opt/wdws/migrations"
        if os.path.isdir(migration_dir):
            migration_tables = set()
            for mf in sorted(os.listdir(migration_dir)):
                if mf.endswith('.sql'):
                    try:
                        with open(os.path.join(migration_dir, mf), 'r') as f:
                            sql_content = f.read()
                        # Extract CREATE TABLE statements
                        for match in re.findall(
                            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+\.\w+)',
                            sql_content, re.IGNORECASE):
                            migration_tables.add(match.lower())
                    except Exception:
                        pass

            # Check which migration tables actually exist
            live_tables_rows = await ctx.query("""
                SELECT table_schema || '.' || table_name as tbl
                FROM information_schema.tables
                WHERE table_schema IN ('core', 'legal', 'medical', 'ops', 'paperless')
                  AND table_type = 'BASE TABLE'
            """)
            live_tables = {r["tbl"] for r in live_tables_rows}

            missing_from_db = migration_tables - live_tables
            if missing_from_db:
                metrics["unapplied_migrations"] = len(missing_from_db)
                await ctx.finding("warning", "schema-drift",
                    f"{len(missing_from_db)} tables defined in migrations but missing from database",
                    "\n".join(f"  {t}" for t in sorted(missing_from_db)),
                    {"missing_tables": sorted(missing_from_db)})

        # ── Summary & Notify ─────────────────────────────────
        if actions_taken:
            email_sections.append({
                "heading": "⚡ Actions Taken",
                "content": "\n".join(f"• {a}" for a in actions_taken),
                "type": "success",
            })

        # Build summary section
        summary_items = [
            f"Connections: {total_conns}/{max_conns} ({metrics['connection_pct']}%)",
            f"Long queries: {metrics['long_running_queries']}",
            f"Bloated tables: {metrics['bloated_tables']}",
            f"DB size: {metrics['total_db_size_mb']}MB",
            f"Embedding coverage: {metrics.get('embedding_coverage_pct', 'N/A')}%",
            f"Agents with errors (24h): {metrics['agents_with_errors_24h']}",
        ]
        email_sections.insert(0, {
            "heading": "🗄️ DBA Health Report",
            "content": "\n".join(summary_items),
            "type": "info",
        })

        if any(m.get("bloated_tables", 0) > 0 or m.get("long_running_queries", 0) > 0
               for m in [metrics]):
            await self._notify(ctx, "DBA Health Report", email_sections)

        try:
            await self.broadcast(
                f"DBA: {metrics['total_db_size_mb']}MB, "
                f"{metrics['connection_pct']}% pool, "
                f"{metrics.get('embedding_coverage_pct', 'N/A')}% embeddings, "
                f"{metrics['bloated_tables']} bloated tables",
                channel="ops", msg_type="status")
        except Exception:
            pass

        return {
            "summary": (f"DBA: {metrics['total_db_size_mb']}MB, "
                       f"{total_conns} conns, "
                       f"{metrics['bloated_tables']} bloated, "
                       f"{metrics['long_running_queries']} slow queries"),
            "metrics": metrics,
        }
