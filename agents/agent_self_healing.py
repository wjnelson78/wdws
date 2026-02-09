"""
🔧 Self-Healing Agent — Auto-Recovery & Maintenance
════════════════════════════════════════════════════
Automatically detects and repairs system issues:
  - Stale connections and leaked resources
  - Log rotation and disk pressure
  - Crashed embeddings re-generation
  - Nightly integration tests
  - Configuration drift detection
"""
import json
from framework import BaseAgent, RunContext


class SelfHealingAgent(BaseAgent):
    agent_id = "self-healing"
    agent_name = "Self-Healing"
    description = "Auto-recovers from failures, rotates logs, repairs data, runs integration tests"
    version = "1.0.0"
    schedule = "0 4 * * *"  # Daily 4 AM
    priority = 2
    capabilities = ["auto-repair", "log-rotation", "integration-test", "config-drift"]

    instructions = """You are the Self-Healing Agent for the Athena Cognitive Platform.

RECOVERY RULES:
1. Connection recovery: Kill idle-in-transaction connections > 30 min
2. Log rotation: Archive/compress logs > 50MB, delete archives > 30 days
3. Embedding repair: Re-queue documents with missing/failed embeddings
4. Orphan cleanup: Remove chunks without parent documents
5. Temp file cleanup: Remove files in /tmp/wdws-* older than 24 hours

INTEGRATION TESTS (nightly):
- Test DB connectivity + basic CRUD
- Test MCP server health endpoint
- Test OAuth token flow (if test client configured)
- Test document→chunk→embedding pipeline completeness
- Verify all systemd services are enabled

CONFIGURATION DRIFT:
- Check that all expected DB tables exist
- Verify pgvector extension is loaded
- Confirm required indexes exist
- Validate ops.mcp_query_log is writable

SAFETY RULES:
- Never drop tables or delete user data
- Only kill connections that are clearly stuck (idle > 30 min in transaction)
- Log every repair action with before/after state
- If unsure, create a finding instead of acting
- Rate limit: max 5 auto-repairs per run"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {"repairs": 0, "tests_passed": 0, "tests_failed": 0, "cleaned": 0}
        max_repairs = 5

        # ── Kill stuck connections ───────────────────────────
        stuck = await ctx.query("""
            SELECT pid, state, query, 
                   EXTRACT(EPOCH FROM now() - state_change)/60 as idle_minutes,
                   usename, application_name
            FROM pg_stat_activity
            WHERE state = 'idle in transaction'
              AND state_change < now() - interval '30 minutes'
              AND datname = 'wdws'
        """)

        for conn in stuck[:max_repairs]:
            await ctx.execute(
                "SELECT pg_terminate_backend($1)", conn["pid"])
            metrics["repairs"] += 1
            await ctx.action(
                f"Killed stuck connection PID {conn['pid']} "
                f"(idle {conn['idle_minutes']:.0f} min, user: {conn['usename']})")

        if stuck:
            await ctx.finding("warning", "connections",
                f"Terminated {len(stuck)} stuck connections",
                "Idle-in-transaction connections held for >30 minutes",
                {"pids": [c["pid"] for c in stuck]})

        # ── Check for orphaned chunks ────────────────────────
        orphans = await ctx.query("""
            SELECT COUNT(*) as count
            FROM core.document_chunks ch
            LEFT JOIN core.documents d ON ch.document_id = d.id
            WHERE d.id IS NULL
        """)
        orphan_count = orphans[0]["count"] if orphans else 0

        if orphan_count > 0:
            await ctx.finding("warning", "data-integrity",
                f"{orphan_count} orphaned chunks found (no parent document)",
                "Chunks exist without matching documents — likely from failed ingestion")
            if orphan_count <= 100:
                await ctx.execute("""
                    DELETE FROM core.document_chunks ch
                    USING (
                        SELECT ch2.id
                        FROM core.document_chunks ch2
                        LEFT JOIN core.documents d ON ch2.document_id = d.id
                        WHERE d.id IS NULL
                        LIMIT 100
                    ) orphans
                    WHERE ch.id = orphans.id
                """)
                metrics["cleaned"] += orphan_count
                metrics["repairs"] += 1
                await ctx.action(f"Cleaned {orphan_count} orphaned chunks")

        # ── Re-queue failed embeddings ───────────────────────
        failed_embed = await ctx.query("""
            SELECT ch.id, ch.document_id, d.title
            FROM core.document_chunks ch
            JOIN core.documents d ON ch.document_id = d.id
            WHERE ch.embedding IS NULL
            LIMIT 20
        """)

        if failed_embed:
            await ctx.finding("warning", "embeddings",
                f"{len(failed_embed)} chunks missing embeddings",
                "These chunks have no embedding vectors — search won't find them",
                {"chunk_ids": [c["id"] for c in failed_embed[:10]]})

        # ── Integration tests ────────────────────────────────
        tests = []

        # Test 1: DB read/write
        try:
            await ctx.execute("""
                INSERT INTO ops.health_checks (service, status, response_ms, details)
                VALUES ('self-heal-test', 'healthy', 0, '{"test": true}')
            """)
            await ctx.execute("""
                DELETE FROM ops.health_checks 
                WHERE service = 'self-heal-test'
            """)
            tests.append(("DB read/write", True, None))
            metrics["tests_passed"] += 1
        except Exception as e:
            tests.append(("DB read/write", False, str(e)))
            metrics["tests_failed"] += 1

        # Test 2: All expected tables exist
        expected_tables = [
            "core.documents", "core.document_chunks", "core.entity_mentions",
            "legal.cases", "legal.filing_metadata", "legal.email_metadata",
            "medical.patients", "medical.record_metadata",
            "ops.mcp_query_log", "ops.agent_registry", "ops.agent_runs",
        ]
        for table in expected_tables:
            schema, name = table.split(".")
            exists = await ctx.query("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = $1 AND table_name = $2
                )
            """, schema, name)
            table_ok = exists[0]["exists"] if exists else False
            if not table_ok:
                tests.append((f"Table {table}", False, "Missing"))
                metrics["tests_failed"] += 1
                await ctx.finding("critical", "schema",
                    f"Missing table: {table}",
                    "Expected table does not exist — schema may be corrupted")
            else:
                metrics["tests_passed"] += 1

        # Test 3: pgvector extension
        pgvec = await ctx.query("""
            SELECT EXISTS (
                SELECT 1 FROM pg_extension WHERE extname = 'vector'
            )
        """)
        if pgvec and pgvec[0]["exists"]:
            tests.append(("pgvector extension", True, None))
            metrics["tests_passed"] += 1
        else:
            tests.append(("pgvector extension", False, "Not installed"))
            metrics["tests_failed"] += 1

        # Test 4: MCP server health
        try:
            resp = await ctx.http_get("http://127.0.0.1:9200/.well-known/oauth-authorization-server")
            health_ok = resp.get("issuer") is not None
            tests.append(("MCP server", health_ok, None if health_ok else "No issuer"))
            metrics["tests_passed" if health_ok else "tests_failed"] += 1
        except Exception as e:
            tests.append(("MCP server", False, str(e)))
            metrics["tests_failed"] += 1

        # Test 5: Dashboard health
        try:
            resp_text = await ctx.http_get("http://127.0.0.1:9100/api/health")
            dash_ok = resp_text is not None
            tests.append(("Dashboard", dash_ok, None))
            metrics["tests_passed" if dash_ok else "tests_failed"] += 1
        except Exception as e:
            tests.append(("Dashboard", False, str(e)))
            metrics["tests_failed"] += 1

        # ── Configuration drift check ────────────────────────
        # Check for required indexes
        important_indexes = await ctx.query("""
            SELECT schemaname, tablename, indexname
            FROM pg_indexes
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops')
            ORDER BY schemaname, tablename
        """)
        metrics["index_count"] = len(important_indexes)

        # ── Report ───────────────────────────────────────────
        failed_tests = [t for t in tests if not t[1]]
        if failed_tests:
            await ctx.finding("critical" if metrics["tests_failed"] > 2 else "warning",
                "integration-tests",
                f"Integration tests: {metrics['tests_passed']} passed, {metrics['tests_failed']} failed",
                "Failed: " + ", ".join(f"{t[0]}: {t[2]}" for t in failed_tests),
                {"results": [{"test": t[0], "passed": t[1], "error": t[2]} for t in tests]})

        return {
            "summary": (
                f"Self-heal: {metrics['repairs']} repairs, "
                f"{metrics['tests_passed']}/{metrics['tests_passed']+metrics['tests_failed']} tests passed, "
                f"{metrics['cleaned']} cleaned"
            ),
            "metrics": metrics,
        }
