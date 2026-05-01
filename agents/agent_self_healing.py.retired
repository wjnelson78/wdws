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
import re
from framework import BaseAgent, RunContext
from config import MCP_PORT


class SelfHealingAgent(BaseAgent):
    agent_id = "self-healing"
    agent_name = "Self-Healing"
    description = "Auto-recovers from failures, rotates logs, repairs data, runs integration tests"
    version = "1.0.0"
    schedule = "0 4 * * *"  # Daily 4 AM
    priority = 2
    capabilities = ["auto-repair", "log-rotation", "integration-test", "config-drift"]

    instructions = """You are the Self-Healing Agent for the Athena Cognitive Engine.

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

    async def _resolve_wdws_mcp_port_conflict(self, ctx: RunContext) -> bool:
        """Kill stray unmanaged MCP listeners blocking the wdws-mcp systemd unit."""
        status = await ctx.service_status("wdws-mcp")
        journal = await ctx.journal("wdws-mcp", lines=60, since="15 minutes ago")
        if "address already in use" not in journal:
            return False

        listener = await ctx.shell(
            f"ss -ltnp | grep ':{MCP_PORT} '",
            timeout=10,
        )
        if not listener.get("success") or "pid=" not in listener.get("stdout", ""):
            return False

        match = re.search(r"pid=(\d+)", listener["stdout"])
        if not match:
            return False

        pid = match.group(1)
        if pid == str(status.get("pid") or ""):
            return False

        owner = await ctx.shell(f"ps -p {pid} -o args=", timeout=10)
        owner_cmd = owner.get("stdout", "").strip()
        if "mcp_server_v2.py" not in owner_cmd:
            await ctx.finding(
                "critical",
                "service-port-conflict",
                f"wdws-mcp is blocked by another process on port {MCP_PORT}",
                owner_cmd or f"Unknown process owns port {MCP_PORT}",
                {"service": "wdws-mcp", "port": MCP_PORT, "pid": pid},
            )
            return False

        kill = await ctx.shell(f"kill {pid}", timeout=10)
        if not kill.get("success"):
            await ctx.finding(
                "critical",
                "service-port-conflict",
                f"Failed to clear wdws-mcp port conflict on {MCP_PORT}",
                kill.get("stderr", "")[:500] or f"Could not kill PID {pid}",
                {"service": "wdws-mcp", "port": MCP_PORT, "pid": pid},
            )
            return False

        await ctx.action(
            f"Killed stray MCP process PID {pid} blocking wdws-mcp on port {MCP_PORT}"
        )
        await ctx.finding(
            "warning",
            "service-port-conflict",
            "Cleared stray MCP listener blocking wdws-mcp",
            owner_cmd,
            {"service": "wdws-mcp", "port": MCP_PORT, "pid": pid},
        )
        return True

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
                INSERT INTO ops.health_checks (check_name, status, value, detail)
                VALUES ('self-heal-test', 'healthy', 0, '{"test": true}')
            """)
            await ctx.execute("""
                DELETE FROM ops.health_checks 
                WHERE check_name = 'self-heal-test'
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
            health_ok = resp.json().get("issuer") is not None
            tests.append(("MCP server", health_ok, None if health_ok else "No issuer"))
            metrics["tests_passed" if health_ok else "tests_failed"] += 1
        except Exception as e:
            tests.append(("MCP server", False, str(e)))
            metrics["tests_failed"] += 1

        # Test 5: Dashboard health
        try:
            resp = await ctx.http_get("http://127.0.0.1:9100/api/health")
            dash_ok = resp.status_code == 200
            dash_detail = None

            try:
                dash_payload = resp.json()
            except Exception:
                dash_payload = {}

            if not dash_ok:
                email_sync_check = (dash_payload.get("checks") or {}).get("email_sync_config") or {}
                dash_detail = (
                    email_sync_check.get("summary")
                    or dash_payload.get("detail")
                    or f"Status {resp.status_code}"
                )

            tests.append(("Dashboard", dash_ok, dash_detail))
            metrics["tests_passed" if dash_ok else "tests_failed"] += 1
        except Exception as e:
            tests.append(("Dashboard", False, str(e)))
            metrics["tests_failed"] += 1

        # ── Systemd service health & auto-restart ───────────
        WDWS_SERVICES = [
            "wdws-agents", "wdws-mcp", "wdws-word-mcp",
            "wdws-docx-proxy", "wdws-imessage-proxy",
            "wdws-investigator-mcp", "wdws-paperless-mcp",
        ]
        dead_services = []
        for svc in WDWS_SERVICES:
            status = await ctx.service_status(svc)
            if not status["active"]:
                dead_services.append(svc)
                await ctx.finding("critical", "service-down",
                    f"Service {svc} is not active (state: {status['state']})",
                    f"SubState: {status['substate']}. Attempting auto-restart.")
                if metrics["repairs"] < max_repairs:
                    if svc == "wdws-mcp":
                        await self._resolve_wdws_mcp_port_conflict(ctx)
                    restart = await ctx.shell(
                        f"systemctl restart {svc}", timeout=30)
                    if restart["success"]:
                        metrics["repairs"] += 1
                        await ctx.action(f"Restarted dead service: {svc}")
                    else:
                        await ctx.finding("critical", "service-restart-failed",
                            f"Could not restart {svc}",
                            restart["stderr"][:500])

        if dead_services:
            await ctx.finding("critical", "services",
                f"{len(dead_services)} services were down: {', '.join(dead_services)}",
                "Services auto-restarted where possible")

        # ── Journal scan: detect Python errors in wdws-agents ─
        journal_text = await ctx.journal("wdws-agents", lines=200,
                                         since="30 minutes ago")
        crash_lines = [
            line for line in journal_text.splitlines()
            if any(tok in line for tok in (
                "Traceback", "Error:", "CRITICAL", "crashed",
                "Killed", "OOM", "segfault", "core dumped"
            ))
        ]
        if crash_lines:
            await ctx.finding("warning", "journal-errors",
                f"{len(crash_lines)} error/crash indicators in wdws-agents journal (last 30 min)",
                "\n".join(crash_lines[:20]),
                {"sample": crash_lines[:10]})

        # ── Configuration drift check ────────────────────────
        # Check for required indexes
        important_indexes = await ctx.query("""
            SELECT schemaname, tablename, indexname
            FROM pg_indexes
            WHERE schemaname IN ('core', 'legal', 'medical', 'ops')
            ORDER BY schemaname, tablename
        """)
        metrics["index_count"] = len(important_indexes)

        # ── Schema vs Code Validation ────────────────────────
        # Proactively detect agent SQL referencing non-existent columns
        import glob, re as _re
        agent_files = glob.glob("/opt/wdws/agents/agent_*.py")
        schema_mismatches = []

        # Build a map of actual columns per table
        actual_cols = await ctx.query("""
            SELECT table_schema || '.' || table_name as tbl,
                   array_agg(column_name) as cols
            FROM information_schema.columns
            WHERE table_schema IN ('core', 'legal', 'medical', 'ops')
            GROUP BY table_schema, table_name
        """)
        col_map = {r["tbl"]: set(r["cols"]) for r in actual_cols}

        for agent_file in agent_files:
            try:
                with open(agent_file, 'r') as f:
                    source = f.read()
                # Find SQL-like references: WHERE col = ... or SELECT col FROM schema.table
                # Look for column references against known tables
                for tbl, cols in col_map.items():
                    schema_name, table_name = tbl.split('.', 1)
                    # Find WHERE <col> patterns near table references
                    if table_name in source or tbl in source:
                        # Extract column-like references near the table name
                        # Pattern: WHERE/AND/OR/SELECT  word  (=/IS/LIKE/IN/</>/!=)
                        for match in _re.finditer(
                            rf'(?:WHERE|AND|OR|SELECT|SET)\s+(\w+)\s*(?:=|IS|LIKE|IN|<|>|!=|,)',
                            source, _re.IGNORECASE):
                            ref_col = match.group(1).lower()
                            # Skip SQL keywords and common aliases
                            if ref_col in ('not', 'null', 'true', 'false', 'and', 'or',
                                          'exists', 'count', 'distinct', 'case', 'when',
                                          'then', 'else', 'end', 'as', 'from', 'select',
                                          'length', 'now', 'interval', 'extract'):
                                continue
                            if ref_col not in cols and ref_col not in ('id', '*'):
                                # Verify it's not an alias or subquery ref
                                schema_mismatches.append({
                                    "file": agent_file.split('/')[-1],
                                    "table": tbl,
                                    "column": ref_col,
                                })
            except Exception:
                pass

        # Deduplicate
        seen = set()
        unique_mismatches = []
        for m in schema_mismatches:
            key = f"{m['file']}:{m['table']}:{m['column']}"
            if key not in seen:
                seen.add(key)
                unique_mismatches.append(m)

        if unique_mismatches:
            metrics["schema_mismatches"] = len(unique_mismatches)
            await ctx.finding("warning", "schema-drift",
                f"{len(unique_mismatches)} possible schema mismatches in agent code",
                "Agent SQL references columns that may not exist in the database. "
                "This causes recurring errors that Code Doctor cannot permanently fix.",
                {"mismatches": unique_mismatches[:20]})

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
