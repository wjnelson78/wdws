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
from decimal import Decimal

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

    instructions = """You are the Database Administrator (DBA) Agent for the Athena Cognitive Engine.

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
- If you find agents referencing columns that don't exist, notify code-doctor
- If Code Doctor reports schema errors, check if a migration is needed
- Report critical findings to orchestrator"""

    # ── Safe Finding Logging ────────────────────────────────
    async def _safe_finding(self, ctx, severity: str, category: str, title: str,
                           detail: str = "", evidence: dict = None):
        """Safely report a finding with cascading exception protection."""
        try:
            try:
                await ctx.finding(severity, category, title, detail, evidence)
            except Exception as finding_err:
                # If finding fails, log and continue
                try:
                    self.log.warning("Could not log finding %s.%s: %s", severity, category, str(finding_err)[:100])
                except Exception:
                    # If even logging fails, silently continue
                    pass
        except Exception:
            # Triple-guard: if ANYTHING fails, just continue
            pass

    # ── Email Notification ───────────────────────────────────
    async def _notify(self, ctx, subject_detail, sections):
        """Queue notification for daily digest rather than sending immediately."""
        try:
            await ctx.queue_notification(subject_detail, sections, severity="info")
        except Exception as e:
            # If notification queue is unavailable, log and continue
            self.log.warning("Could not queue notification: %s", str(e)[:200])
        return {"status": "queued"}

    # ── Safe Broadcast ───────────────────────────────────────
    async def _safe_broadcast(self, message: str, channel: str = "ops", msg_type: str = "status"):
        """Safely broadcast status message, catching any errors from ops.agent_chat."""
        try:
            await self.broadcast(message, channel=channel, msg_type=msg_type)
        except Exception as e:
            # If broadcast/agent_chat is unavailable, just log locally
            self.log.debug("Could not broadcast to ops channel: %s", str(e)[:100])

    # ═══════════════════════════════════════════════════════════
    #  MAIN RUN LOGIC
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        metrics = {}
        email_sections = []
        actions_taken = []

        # Initialize metrics with safe defaults BEFORE try block
        metrics['total_connections'] = 0
        metrics['max_connections'] = 100
        metrics['connection_pct'] = 0

        # Local vars track values for later use
        total_conns = 0
        max_conns = 100

        # Defensive: Log that run started with comprehensive logging enabled
        try:
            self.log.info("DBA agent run started - version 1.0.0 with comprehensive diagnostics")
        except Exception:
            pass

        # Track which section we're in for diagnostic purposes
        current_section = "initialization"

        # Pre-check: Verify database connectivity before main logic
        try:
            test_pool = await ctx.db()
            if test_pool and not test_pool._closed:
                self.log.info("Pool health check PASS: connections available")
            else:
                self.log.warning("Pool health check WARN: pool is closed or invalid")
        except Exception as pool_err:
            self.log.error("Pool health check FAIL: %s", str(pool_err)[:100])
            # Continue anyway — the main logic will handle it

        # ── 1. Connection Pool Health ────────────────────────
        current_section = "connection-pool"
        connections = []
        try:
            # Use simpler query without current_database() to avoid pool initialization issues
            connections = await ctx.query("""
            SELECT state, COUNT(*) as count,
                   MAX(EXTRACT(EPOCH FROM now() - state_change)) as max_age_secs
            FROM pg_stat_activity
            GROUP BY state
            ORDER BY count DESC
            """.strip())
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "connection-query-failed",
                    f"Could not query connection pool: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log connection-query-failed finding: %s", fe)
            # Keep connection values as numeric defaults, not string 'N/A'
            metrics["total_connections"] = 0
            metrics["max_connections"] = 100
            metrics["connection_pct"] = 0
        if connections:
            total_conns = sum(c["count"] for c in connections)
            try:
                max_conns_row = await ctx.query("SHOW max_connections")
                max_conns = int(max_conns_row[0]["max_connections"]) if max_conns_row else 100
            except Exception as e:
                try:
                    await self._safe_finding(ctx,"warning", "max-connections-query-failed",
                        f"Could not query max_connections: {str(e)[:100]}")
                except Exception as fe:
                    self.log.warning("Could not log max-connections-query-failed finding: %s", fe)
                max_conns = 100

            metrics["total_connections"] = total_conns
            metrics["max_connections"] = max_conns
            metrics["connection_pct"] = round(total_conns / max_conns * 100, 1)

            conn_detail = {c["state"] or "null": c["count"] for c in connections}
            if metrics["connection_pct"] > 80:
                await self._safe_finding(ctx,"warning", "connections",
                    f"Connection pool at {metrics['connection_pct']}% ({total_conns}/{max_conns})",
                    f"Breakdown: {json.dumps(conn_detail)}",
                    {"connections": conn_detail})
        else:
            total_conns = 0
            max_conns = 100
            # No connections queried; use numeric defaults
            metrics["total_connections"] = 0
            metrics["max_connections"] = 100
            metrics["connection_pct"] = 0

        # ── 2. Long-Running Queries ──────────────────────────
        current_section = "long-running-queries"
        long_queries = []
        try:
            long_queries = await ctx.query("""
                SELECT pid, state, usename, application_name,
                       EXTRACT(EPOCH FROM now() - query_start) as duration_secs,
                       SUBSTRING(query, 1, 200) as query_preview,
                       wait_event_type, wait_event
                FROM pg_stat_activity
                WHERE state = 'active'
                  AND query NOT LIKE '%pg_stat_activity%'
                  AND query_start < now() - interval '60 seconds'
                ORDER BY query_start
                LIMIT 10
            """)
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "long-queries-query-failed",
                    f"Could not query long-running queries: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log long-queries-query-failed finding: %s", fe)
        metrics["long_running_queries"] = len(long_queries)

        for q in long_queries:
            dur = q["duration_secs"]
            severity = "critical" if dur > 300 else "warning"
            await self._safe_finding(ctx,severity, "slow-query",
                f"Query running {dur:.0f}s (PID {q['pid']}, user: {q['usename']})",
                q["query_preview"],
                {"pid": q["pid"], "duration": dur, "wait": q["wait_event"]})

        # Warn about queries > 30 minutes, only kill after > 60 minutes
        stuck_warn = [q for q in long_queries if 1800 < q["duration_secs"] <= 3600]
        for q in stuck_warn:
            await self._safe_finding(ctx,"warning", "stuck-query",
                f"Query PID {q['pid']} running {q['duration_secs']:.0f}s — will be terminated if still running next cycle",
                q["query_preview"],
                {"pid": q["pid"], "duration": q["duration_secs"], "user": q["usename"]})

        # Kill queries > 60 minutes (safety valve — warned at 30 min)
        stuck_kill = [q for q in long_queries if q["duration_secs"] > 3600]
        for q in stuck_kill:
            try:
                await ctx.execute("SELECT pg_terminate_backend($1)", q["pid"])
                actions_taken.append(f"Killed stuck query PID {q['pid']} ({q['duration_secs']:.0f}s)")
                await ctx.action(f"Terminated stuck query PID {q['pid']} (running > 60 min)")
            except Exception as e:
                self.log.warning("Failed to terminate query PID %s: %s", q.get('pid'), str(e)[:100])

        # ── 3. Table Bloat & VACUUM Status ───────────────────
        current_section = "table-bloat"
        bloat = []
        try:
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "bloat-query-failed",
                    f"Could not query table bloat: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log bloat-query-failed finding: %s", fe)

        bloated = [b for b in bloat if b["dead_pct"] and b["dead_pct"] > 20]
        metrics["bloated_tables"] = len(bloated)

        for b in bloated:
            await self._safe_finding(ctx,"warning", "bloat",
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
                    await self._safe_finding(ctx,"warning", "vacuum-failed",
                        f"VACUUM failed on {b['table_name']}: {e}")

        # ── 4. Missing Indexes ───────────────────────────────
        # Find sequential scans on large tables
        seq_scan_heavy = []
        try:
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "seq-scan-query-failed",
                    f"Could not query sequential scans: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log seq-scan-query-failed finding: %s", fe)

        for t in seq_scan_heavy:
            if t["seq_pct"] and t["seq_pct"] > 80:
                await self._safe_finding(ctx,"info", "missing-index",
                    f"Table {t['table_name']} has {t['seq_pct']}% sequential scans "
                    f"({t['seq_scan']} seq vs {t['idx_scan']} idx, {t['n_live_tup']} rows)",
                    "Consider adding indexes for frequently queried columns")
        metrics["seq_scan_heavy_tables"] = len(seq_scan_heavy)

        # ── 5. Index Usage ───────────────────────────────────
        unused_indexes = []
        try:
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "unused-indexes-query-failed",
                    f"Could not query unused indexes: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log unused-indexes-query-failed finding: %s", fe)
        metrics["unused_indexes"] = len(unused_indexes)

        if unused_indexes:
            total_waste = sum(1 for _ in unused_indexes)
            await self._safe_finding(ctx,"info", "unused-indexes",
                f"{len(unused_indexes)} indexes have never been used",
                "\n".join(f"  {u['index_name']} on {u['table_name']} ({u['index_size']})"
                         for u in unused_indexes),
                {"indexes": [u["index_name"] for u in unused_indexes]})

        # ── 6. Table Sizes & Growth ──────────────────────────
        table_sizes = []
        try:
            table_sizes = await ctx.query("""
                SELECT schemaname || '.' || tablename as table_name,
                       pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
                       pg_total_relation_size(schemaname || '.' || tablename) as size_bytes
                FROM pg_tables
                WHERE schemaname IN ('core', 'legal', 'medical', 'ops', 'paperless')
                ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
                LIMIT 20
            """)
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "table-sizes-query-failed",
                    f"Could not query table sizes: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log table-sizes-query-failed finding: %s", fe)

        if table_sizes:
            total_db_bytes = sum(t["size_bytes"] for t in table_sizes)
            metrics["total_db_size_mb"] = round(total_db_bytes / 1024 / 1024, 1)
            metrics["largest_tables"] = [
                {"table": t["table_name"], "size": t["total_size"]}
                for t in table_sizes[:5]
            ]

            # Compare with last run
            try:
                last_size = await ctx.recall("db_size_bytes")
                if last_size and isinstance(last_size, (int, float)) and total_db_bytes > last_size * 1.1:  # >10% growth
                    growth_pct = round((total_db_bytes - last_size) / last_size * 100, 1)
                    await self._safe_finding(ctx,"info", "storage-growth",
                        f"Database grew {growth_pct}% since last check "
                        f"({round(last_size/1024/1024,1)}MB → {metrics['total_db_size_mb']}MB)",
                        evidence={"previous": last_size, "current": total_db_bytes})
                await ctx.remember("db_size_bytes", total_db_bytes)
            except Exception as e:
                self.log.warning("Database size memory operation failed: %s", e)
                # Don't fail the entire run if memory operations fail
        else:
            metrics["total_db_size_mb"] = 0
            metrics["largest_tables"] = []

        # ── 7. Schema Integrity ──────────────────────────────
        # Tables without primary keys
        no_pk = []
        try:
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "pk-query-failed",
                    f"Could not query primary keys: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log pk-query-failed finding: %s", fe)
        metrics["tables_without_pk"] = len(no_pk)

        if no_pk:
            await self._safe_finding(ctx,"warning", "schema-integrity",
                f"{len(no_pk)} tables have no primary key",
                "\n".join(f"  {t['table_name']}" for t in no_pk),
                {"tables": [t["table_name"] for t in no_pk]})

        # Foreign key violations
        fk_violations = []
        try:
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "fk-query-failed",
                    f"Could not query foreign key constraints: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log fk-query-failed finding: %s", fe)
        metrics["fk_violations"] = len(fk_violations)
        if fk_violations:
            await self._safe_finding(ctx,"warning", "fk-integrity",
                f"{len(fk_violations)} unvalidated foreign key constraints",
                "\n".join(f"  {v['constraint_name']} on {v['table_name']}" for v in fk_violations))

        # ── 8. Lock Monitoring ───────────────────────────────
        locks = []
        try:
            locks = await ctx.query("""
                SELECT blocked_locks.pid AS blocked_pid,
                       blocked_activity.usename AS blocked_user,
                       SUBSTRING(blocked_activity.query, 1, 200) AS blocked_query,
                       blocking_locks.pid AS blocking_pid,
                       blocking_activity.usename AS blocking_user,
                       SUBSTRING(blocking_activity.query, 1, 200) AS blocking_query
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
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "lock-query-failed",
                    f"Could not query lock contention: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log lock-query-failed finding: %s", fe)
        metrics["blocked_queries"] = len(locks)

        if locks:
            await self._safe_finding(ctx,"warning", "lock-contention",
                f"{len(locks)} blocked queries detected",
                "\n".join(f"  PID {l['blocked_pid']} blocked by PID {l['blocking_pid']}: "
                         f"{l['blocked_query'][:100]}" for l in locks),
                {"locks": [dict(l) for l in locks]})

        # ── 9. pgvector Health ───────────────────────────────
        pgvec = []
        try:
            pgvec = await ctx.query("""
                SELECT extversion FROM pg_extension WHERE extname = 'vector'
            """)
        except Exception as e:
            try:
                await self._safe_finding(ctx,"warning", "pgvector-query-failed",
                    f"Could not query pgvector status: {str(e)[:100]}")
            except Exception as fe:
                self.log.warning("Could not log pgvector-query-failed finding: %s", fe)

        metrics["pgvector_version"] = pgvec[0]["extversion"] if pgvec else "NOT INSTALLED"

        if not pgvec:
            await self._safe_finding(ctx,"critical", "pgvector",
                "pgvector extension is NOT installed",
                "The RAG pipeline requires pgvector for embedding storage and similarity search")

        # ── 10. Embedding Coverage ───────────────────────────
        metrics["total_chunks"] = 0
        metrics["embedding_coverage_pct"] = "N/A"
        try:
            # First, check if core.document_chunks table exists
            chunk_table_exists = await ctx.query("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'core' AND table_name = 'document_chunks'
                ) as chunk_table_exists_check
            """)

            chunk_table_exists_bool = False
            if chunk_table_exists:
                first_row = chunk_table_exists[0]
                chunk_table_exists_bool = next(iter(first_row.values())) if first_row else False

            if chunk_table_exists_bool:
                embed_stats = await ctx.query("""
                    SELECT COUNT(*) as total_chunks
                    FROM core.document_chunks
                """)
                if embed_stats:
                    e = embed_stats[0]
                    total_chunks = e.get("total_chunks", 0)
                    metrics["total_chunks"] = total_chunks
        except Exception as e:
            self.log.debug("Embedding coverage check failed: %s", str(e)[:100])

        # ── 11. Agent Error Rate ─────────────────────────────
        metrics["agents_with_errors_24h"] = 0
        try:
            # First, check if ops.agent_runs table exists (it may not be installed yet)
            table_exists = await ctx.query("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ops' AND table_name = 'agent_runs'
                ) as table_exists_check
            """)

            # More robust: check first row's first value (EXISTS returns bool in first column)
            table_exists_bool = False
            if table_exists:
                first_row = table_exists[0]
                # EXISTS result is in the first (and only) column
                table_exists_bool = next(iter(first_row.values())) if first_row else False

            if table_exists_bool:
                error_rates = await ctx.query("""
                    SELECT agent_id,
                           COUNT(*) as total_runs,
                           SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
                           CASE WHEN COUNT(*) > 0
                                THEN round(100.0 * SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                ELSE 0 END as error_pct
                    FROM ops.agent_runs
                    WHERE created_at > now() - interval '24 hours'
                    GROUP BY agent_id
                    HAVING SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) > 0
                    ORDER BY error_pct DESC
                """)
                metrics["agents_with_errors_24h"] = len(error_rates)

                for er in error_rates:
                    try:
                        error_pct = er["error_pct"] if "error_pct" in er else None
                        if error_pct is not None and float(error_pct) > 50:
                            # Convert Decimal values to float for JSON serialization
                            er_data = {}
                            for k in ["agent_id", "total_runs", "errors", "error_pct"]:
                                if k in er:
                                    v = er[k]
                                    er_data[k] = float(v) if isinstance(v, Decimal) else v
                            await self._safe_finding(ctx,"warning", "agent-error-rate",
                                f"Agent '{er['agent_id']}' has {error_pct}% error rate "
                                f"({er['errors']}/{er['total_runs']} runs in 24h)",
                                "High error rate may indicate a persistent issue",
                                er_data)
                    except Exception as er_err:
                        self.log.warning("Failed to process agent error rate: %s", str(er_err)[:100])
            else:
                # ops.agent_runs table is not yet installed — log finding for admin
                try:
                    await self._safe_finding(ctx,"warning", "missing-agent-runs-table",
                        "ops.agent_runs table is not installed",
                        "Agent error rate monitoring disabled. Apply migrations to enable this feature.")
                except Exception as fe:
                    self.log.warning("Could not log missing-agent-runs-table finding: %s", fe)
        except Exception as e:
            # If schema check itself fails, log it but continue
            self.log.debug("Agent error rate check failed: %s", str(e)[:100])
            metrics["agents_with_errors_24h"] = 0

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

            # Check which migration tables actually exist (after for loop completes)
            live_tables_rows = []
            try:
                live_tables_rows = await ctx.query("""
                    SELECT table_schema || '.' || table_name as tbl
                    FROM information_schema.tables
                    WHERE table_schema IN ('core', 'legal', 'medical', 'ops', 'paperless')
                      AND table_type = 'BASE TABLE'
                """)
            except Exception as e:
                try:
                    await self._safe_finding(ctx,"warning", "live-tables-query-failed",
                        f"Could not query live tables: {str(e)[:100]}")
                except Exception as fe:
                    self.log.warning("Could not log live-tables-query-failed finding: %s", fe)

            if live_tables_rows:
                live_tables = {r["tbl"] for r in live_tables_rows}

                missing_from_db = migration_tables - live_tables
                if missing_from_db:
                    metrics["unapplied_migrations"] = len(missing_from_db)
                    await self._safe_finding(ctx,"warning", "schema-drift",
                        f"{len(missing_from_db)} tables defined in migrations but missing from database",
                        "\n".join(f"  {t}" for t in sorted(missing_from_db)),
                        {"missing_tables": sorted(missing_from_db)})

        except Exception as main_error:
            # Catch any unhandled exception during main run logic with cascading protection
            try:
                import traceback
                error_msg = f"DBA agent main logic error: {str(main_error)[:200]}"
                error_type = type(main_error).__name__
                tb_msg = traceback.format_exc()
                try:
                    self.log.error("EXCEPTION IN MAIN LOGIC [%s]: %s\nTraceback:\n%s",
                                   error_type, error_msg, tb_msg[:500])
                    self.log.error("Last section before exception: %s", current_section)

                    # If connection-related error, attempt to close and reset pool
                    if any(term in str(main_error).lower() for term in ['connection', 'pool', 'closed', 'eof']):
                        self.log.warning("Detected connection-related error - attempting pool reset")
                        try:
                            from framework import close_pool
                            await close_pool()
                            self.log.info("Pool reset attempted (will reconnect on next run)")
                        except Exception as reset_err:
                            self.log.warning("Could not reset pool: %s", str(reset_err)[:100])
                except Exception:
                    pass  # If logging fails, continue anyway

                # Try to log finding, but don't let it fail the handler
                try:
                    await self._safe_finding(ctx, "critical", "dba-main-error",
                        f"DBA agent encountered {error_type} in section {current_section}",
                        error_msg)
                except Exception as finding_err:
                    try:
                        self.log.warning("Could not log finding for main error: %s", str(finding_err)[:100])
                    except Exception:
                        pass  # If even this fails, give up and return

                # Build return dict with maximum safety
                try:
                    return {
                        "summary": f"DBA run failed in section {current_section}: {error_msg[:100]}",
                        "metrics": metrics if isinstance(metrics, dict) else {},
                        "error": str(main_error)[:200]
                    }
                except Exception as ret_err:
                    # Last resort return if dict building fails
                    return {
                        "summary": "DBA run encountered cascading error",
                        "metrics": {}
                    }
            except Exception as cascade_err:
                # Absolute last resort: if ENTIRE exception handler fails
                return {"summary": "DBA run failed with cascading error in handler", "metrics": {}}

        # ── Summary & Notify ─────────────────────────────────
        # Wrapped in try/except to prevent unhandled exceptions
        try:
            if actions_taken:
                email_sections.append({
                    "heading": "⚡ Actions Taken",
                    "content": "\n".join(f"• {a}" for a in actions_taken),
                    "type": "success",
                })

            # Build summary section — ensure all metrics have proper typed defaults
            metrics.setdefault('bloated_tables', 0)
            metrics.setdefault('total_db_size_mb', 0)
            metrics.setdefault('agents_with_errors_24h', 0)
            metrics.setdefault('long_running_queries', 0)

            # Format metrics for display, converting numeric types to strings for presentation
            bloated_display = metrics.get('bloated_tables', 0)
            total_db_mb = metrics.get('total_db_size_mb', 0)
            db_size_display = f"{total_db_mb}" if isinstance(total_db_mb, (int, float)) else 'N/A'
            chunks_display = metrics.get('total_chunks', 'N/A')
            errors_display = metrics.get('agents_with_errors_24h', 0)

            # Ensure all values are properly typed before formatting
            conns_display = metrics.get('total_connections', 0)
            max_conns_display = metrics.get('max_connections', 100)
            pct_display = metrics.get('connection_pct', 'N/A')

            summary_items = [
                f"Connections: {conns_display}/{max_conns_display} ({pct_display}%)",
                f"Long queries: {metrics.get('long_running_queries', 0)}",
                f"Bloated tables: {bloated_display}",
                f"DB size: {db_size_display}MB",
                f"Total chunks: {chunks_display}",
                f"Agents with errors (24h): {errors_display}",
            ]
            email_sections.insert(0, {
                "heading": "🗄️ DBA Health Report",
                "content": "\n".join(summary_items),
                "type": "info",
            })

            # Check if we should notify — both values are now guaranteed integers
            bloated_tables = metrics.get('bloated_tables', 0)
            long_running_queries = metrics.get('long_running_queries', 0)
            if (bloated_tables > 0) or (long_running_queries > 0):
                await self._notify(ctx, "DBA Health Report", email_sections)
        except Exception as notify_err:
            self.log.warning("Error in summary/notify section: %s", str(notify_err)[:100])

        try:
            db_size_val = metrics.get('total_db_size_mb', 0)
            db_size_str = f"{db_size_val}MB" if isinstance(db_size_val, (int, float)) else "N/AMB"
            conn_pct_val = metrics.get('connection_pct', 'N/A')
            conn_pct_str = f"{conn_pct_val}%" if conn_pct_val != 'N/A' else 'N/A'
            embed_pct_val = metrics.get('embedding_coverage_pct', 'N/A')
            embed_pct_str = f"{embed_pct_val}%" if embed_pct_val != 'N/A' else 'N/A'
            bloated_val = metrics.get('bloated_tables', 0)
            bloated_str = str(bloated_val)
            try:
                await self._safe_broadcast(
                    f"DBA: {db_size_str}, {conn_pct_str} pool, {embed_pct_str} embeddings, {bloated_str} bloated",
                    channel="ops", msg_type="status")
            except Exception as be:
                self.log.debug("Broadcast failed: %s", str(be)[:100])
        except Exception as setup_err:
            # Outer broadcast setup failure is non-critical
            self.log.debug("Broadcast setup failed: %s", str(setup_err)[:100])

        # Sanitize metrics dict to remove any Decimal types that cause JSON serialization issues
        try:
            sanitized_metrics = {}
            for k, v in metrics.items():
                if isinstance(v, Decimal):
                    sanitized_metrics[k] = float(v)
                elif isinstance(v, list):
                    sanitized_metrics[k] = [float(item) if isinstance(item, Decimal) else item for item in v]
                else:
                    sanitized_metrics[k] = v
            metrics = sanitized_metrics
        except Exception as e:
            self.log.warning("Could not sanitize metrics dict: %s", str(e)[:100])

        # Build final result with absolute fallback
        try:
            # Ensure metrics dict is valid before processing
            if not isinstance(metrics, dict):
                metrics = {}

            # Use local variables for connection data (most current source of truth)
            # Use metrics dict for other values
            bloated_final = int(metrics.get('bloated_tables', 0)) if isinstance(metrics.get('bloated_tables'), (int, float)) else 0
            slow_final = int(metrics.get('long_running_queries', 0)) if isinstance(metrics.get('long_running_queries'), (int, float)) else 0
            db_size_final = int(metrics.get('total_db_size_mb', 0)) if isinstance(metrics.get('total_db_size_mb'), (int, float)) else 0
            # Always use metrics dict for total_connections to ensure availability after exception handlers
            total_conns_final = metrics.get('total_connections', 0)

            summary_result = {
                "summary": (f"DBA: {db_size_final}MB, "
                           f"{total_conns_final} conns, "
                           f"{bloated_final} bloated, "
                           f"{slow_final} slow queries"),
                "metrics": metrics if isinstance(metrics, dict) else {},
            }
            self.log.info("DBA run complete - returning valid response: %s", summary_result.get("summary", "N/A")[:100])
            return summary_result
        except Exception as re:
            # Next-to-last resort: return minimal response
            self.log.error("Failed to build return result: %s", str(re)[:100])
            try:
                return {
                    "summary": "DBA run completed with errors",
                    "metrics": metrics if isinstance(metrics, dict) else {},
                }
            except Exception as ultimate_fallback:
                # Absolute last resort
                self.log.critical("ULTIMATE FALLBACK: Even return statement failed: %s", str(ultimate_fallback)[:100])
                return {"summary": "DBA run completed", "metrics": {}}
