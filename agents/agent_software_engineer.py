"""
🛠️ Software Engineer Agent — Autonomous Code Maintenance & Repair
═════════════════════════════════════════════════════════════════
Proactively scans agent source code for schema mismatches, broken SQL,
stale references, and code quality issues. Applies verified fixes.

Unlike Code Doctor (which reacts to runtime errors after they happen),
Software Engineer proactively validates code against the live database
schema BEFORE errors occur, and applies surgical fixes.

Capabilities:
  - Schema-vs-code validation (SQL column/table references)
  - Instruction docstring accuracy (agent instructions match reality)
  - Dead code / unreachable branch detection
  - Python syntax & import validation
  - Cross-agent API contract verification
  - Safe auto-patching with backup + syntax check
"""
import ast
import glob
import hashlib
import json
import os
import re
import traceback
from datetime import datetime, timezone

from framework import BaseAgent, RunContext, get_pool, llm_chat, llm_json, codex_json
from config import OPENAI_API_KEY, ALERT_EMAIL, CODEX_MODEL
try:
    from config import (
        GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_SENDER_EMAIL,
    )
    from email_util import send_email, build_notification_html
    _EMAIL_AVAILABLE = True
except Exception:
    _EMAIL_AVAILABLE = False


class SoftwareEngineerAgent(BaseAgent):
    agent_id = "software-engineer"
    agent_name = "Software Engineer"
    description = "Proactive code maintenance: validates SQL against live schema, fixes mismatches, ensures code quality"
    version = "1.0.0"
    schedule = "30 */4 * * *"  # Every 4 hours at :30
    priority = 2
    capabilities = [
        "schema-validation", "code-fix", "syntax-check",
        "instruction-audit", "cross-agent-validation", "auto-patch"
    ]

    instructions = """You are the Software Engineer Agent for the Athena Cognitive Platform.

YOUR MISSION:
Proactively maintain code quality across the agent fleet by:
1. Validating all SQL in agent code against the LIVE database schema
2. Ensuring agent instruction docstrings match actual schema
3. Detecting and fixing broken column/table references BEFORE they cause runtime errors
4. Verifying Python syntax and import correctness

SCHEMA VALIDATION RULES:
- Query information_schema.columns and information_schema.tables to get the TRUTH
- Parse each agent's Python source for SQL strings
- Extract column references (SELECT x, WHERE x, GROUP BY x, ORDER BY x, INSERT INTO t(x))
- Cross-reference against actual schema
- For each mismatch: determine the correct column name and generate a fix
- The framework now includes schema_aliases.py which auto-resolves common
  column name synonyms at query time. Known aliases include:
    filed_date/filing_date → date_filed (legal.cases)
    email_date/sent_date → date_sent (legal.email_metadata)
    filing_date/date_filed → actual_filing_date (legal.filing_metadata)
    full_content_preview → full_content (core.documents)
    ocr_text → extracted_text (legal.email_attachments)
  Code should still use canonical names, but alias resolution provides a safety net.

FIX GENERATION:
- Generate exact old_code → new_code patches
- Fix SQL to match the ACTUAL schema (never suggest adding columns)
- Also fix any Python dict key references that depend on the SQL aliases
- Fix instruction docstrings to list correct column names
- Validate Python syntax after patching (ast.parse)
- Always create .bak backup before writing

SAFETY:
- Never modify framework.py or run.py
- Never drop tables, delete data, or alter schema
- Max 10 fixes per run
- Always log every fix with before/after
- Send email summary if fixes were applied

OUTPUT FORMAT (for diagnosis):
{
  "file": "/opt/wdws/agents/agent_xxx.py",
  "mismatches": [
    {"table": "legal.cases", "bad_column": "filed_date", "correct_column": "date_filed",
     "line": 66, "context": "SQL snippet"}
  ],
  "fixes": [
    {"description": "...", "old_code": "exact old", "new_code": "exact new"}
  ]
}"""

    # ── Schema Cache ─────────────────────────────────────────
    _schema_cache: dict = None

    async def _load_schema(self, ctx: RunContext) -> dict:
        """Load the full database schema into a lookup dict."""
        if self._schema_cache:
            return self._schema_cache

        rows = await ctx.query("""
            SELECT table_schema || '.' || table_name as tbl,
                   array_agg(column_name ORDER BY ordinal_position) as cols
            FROM information_schema.columns
            WHERE table_schema IN ('core', 'legal', 'medical', 'ops', 'paperless')
            GROUP BY table_schema, table_name
        """)
        self._schema_cache = {r["tbl"]: set(r["cols"]) for r in rows}
        return self._schema_cache

    async def _get_all_tables(self, ctx: RunContext) -> set:
        schema = await self._load_schema(ctx)
        return set(schema.keys())

    # ── SQL Parser ───────────────────────────────────────────
    def _extract_sql_blocks(self, source: str) -> list[tuple[int, str]]:
        """Extract SQL strings from Python source with approximate line numbers."""
        sql_blocks = []
        # Match triple-quoted strings that contain SQL keywords
        pattern = re.compile(
            r'("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')',
            re.MULTILINE
        )
        for match in pattern.finditer(source):
            content = match.group(0)[3:-3]
            if re.search(r'\b(SELECT|INSERT|UPDATE|DELETE|FROM|JOIN|WHERE)\b', content, re.IGNORECASE):
                # Approximate line number
                line_no = source[:match.start()].count('\n') + 1
                sql_blocks.append((line_no, content))
        return sql_blocks

    def _extract_column_refs(self, sql: str, schema: dict) -> list[dict]:
        """Extract column references from SQL and validate against schema."""
        mismatches = []
        sql_upper = sql.upper()

        # Find all table references: schema.table or alias assignments
        table_refs = re.findall(
            r'(?:FROM|JOIN|INTO|UPDATE)\s+(\w+\.\w+)(?:\s+(\w+))?',
            sql, re.IGNORECASE
        )

        # Build alias → table mapping
        aliases = {}
        for tbl, alias in table_refs:
            tbl_lower = tbl.lower()
            if tbl_lower in schema:
                if alias:
                    aliases[alias.lower()] = tbl_lower
                else:
                    # Use the table name part as implicit alias
                    parts = tbl_lower.split('.')
                    aliases[parts[-1]] = tbl_lower

        # Find aliased column references: alias.column
        col_refs = re.findall(r'\b(\w+)\.(\w+)\b', sql)
        for alias, col in col_refs:
            alias_lower = alias.lower()
            col_lower = col.lower()

            # Skip schema.table references themselves
            full_ref = f"{alias_lower}.{col_lower}"
            if full_ref in schema:
                continue

            # Skip common SQL functions/keywords
            if alias_lower in ('now', 'count', 'sum', 'avg', 'min', 'max',
                               'left', 'right', 'array_agg', 'string_agg',
                               'extract', 'coalesce', 'pg_stat', 'information_schema',
                               'pg_catalog', 'interval', 'json_build_object'):
                continue

            if alias_lower in aliases:
                table = aliases[alias_lower]
                actual_cols = schema.get(table, set())
                if actual_cols and col_lower not in actual_cols:
                    # Find closest match
                    close = self._find_closest_column(col_lower, actual_cols)
                    mismatches.append({
                        "table": table,
                        "alias": alias_lower,
                        "bad_column": col_lower,
                        "correct_column": close,
                        "sql_fragment": full_ref,
                    })

        # Find bare column references in WHERE/SELECT/GROUP BY/ORDER BY
        # Pattern: WHERE col = ... or SELECT col, ...
        bare_refs = re.findall(
            r'(?:WHERE|AND|OR|SET|SELECT)\s+(\w+)\s*(?:=|IS\s|LIKE\s|IN\s|<|>|!=|,|\s+(?:AS|DESC|ASC|BETWEEN))',
            sql, re.IGNORECASE
        )
        sql_keywords = {
            'not', 'null', 'true', 'false', 'and', 'or', 'exists', 'count',
            'distinct', 'case', 'when', 'then', 'else', 'end', 'as', 'from',
            'select', 'length', 'now', 'interval', 'extract', 'all', 'any',
            'in', 'between', 'like', 'having', 'group', 'order', 'by', 'asc',
            'desc', 'limit', 'offset', 'union', 'join', 'left', 'right',
            'inner', 'outer', 'cross', 'on', 'where', 'set', 'values',
            'insert', 'update', 'delete', 'into', 'table', 'index',
            'coalesce', 'cast', 'array_agg', 'string_agg', 'sum', 'avg',
        }
        for col in bare_refs:
            if col.lower() not in sql_keywords:
                # Check against all referenced tables
                for tbl in [t for t, _ in table_refs]:
                    tbl_lower = tbl.lower()
                    actual_cols = schema.get(tbl_lower, set())
                    if actual_cols and col.lower() not in actual_cols:
                        close = self._find_closest_column(col.lower(), actual_cols)
                        if close:
                            mismatches.append({
                                "table": tbl_lower,
                                "alias": None,
                                "bad_column": col.lower(),
                                "correct_column": close,
                                "sql_fragment": col,
                            })

        return mismatches

    def _find_closest_column(self, bad_col: str, actual_cols: set) -> str | None:
        """Find the most likely correct column name."""
        # Direct word-reorder match: filed_date → date_filed
        bad_parts = set(bad_col.split('_'))
        for col in actual_cols:
            col_parts = set(col.split('_'))
            if bad_parts == col_parts and bad_col != col:
                return col

        # Substring match
        for col in actual_cols:
            if bad_col in col or col in bad_col:
                return col

        # Common renames
        renames = {
            'email_date': 'date_sent',
            'filed_date': 'date_filed',
            'filing_date': 'actual_filing_date',
            'full_content_preview': 'full_content',
            'ocr_text': 'extracted_text',
            'email_id': 'email_doc_id',
            'service': 'check_name',
            'response_ms': 'value',
            'details': 'detail',
            'has_embedding': None,  # column doesn't exist at all
        }
        return renames.get(bad_col)

    # ── File Operations ──────────────────────────────────────
    def _read_file(self, filepath: str) -> str | None:
        try:
            with open(filepath, 'r') as f:
                return f.read()
        except Exception:
            return None

    def _apply_fix(self, filepath: str, old_code: str, new_code: str) -> tuple[bool, str]:
        """Apply a code fix with backup and syntax validation."""
        content = self._read_file(filepath)
        if content is None:
            return False, f"Cannot read {filepath}"

        if old_code not in content:
            return False, f"old_code not found in {filepath}"

        new_content = content.replace(old_code, new_code, 1)

        # Validate syntax
        if filepath.endswith('.py'):
            try:
                ast.parse(new_content)
            except SyntaxError as e:
                return False, f"Syntax error after patch: {e}"

        # Backup
        try:
            with open(filepath + '.bak', 'w') as f:
                f.write(content)
        except Exception as e:
            return False, f"Backup failed: {e}"

        # Write
        try:
            with open(filepath, 'w') as f:
                f.write(new_content)
        except Exception as e:
            # Rollback
            with open(filepath, 'w') as f:
                f.write(content)
            return False, f"Write failed: {e}"

        return True, "Applied"

    # ── Email Notification ───────────────────────────────────
    async def _notify(self, ctx, subject_detail, sections):
        """Queue notification for daily digest rather than sending immediately."""
        await ctx.queue_notification(subject_detail, sections, severity="info")
        return {"status": "queued"}

    # ═══════════════════════════════════════════════════════════
    #  MAIN RUN LOGIC
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        metrics = {
            "files_scanned": 0,
            "sql_blocks_found": 0,
            "mismatches_found": 0,
            "fixes_applied": 0,
            "fixes_failed": 0,
            "syntax_errors": 0,
        }
        max_fixes = 10
        email_sections = []

        # ── 1. Load live schema ──────────────────────────────
        schema = await self._load_schema(ctx)
        await ctx.action(f"Loaded schema: {len(schema)} tables across "
                         f"{len(set(t.split('.')[0] for t in schema))} schemas")

        # ── 2. Scan all agent files ──────────────────────────
        agent_files = sorted(glob.glob("/opt/wdws/agents/agent_*.py"))
        all_mismatches = []

        for filepath in agent_files:
            filename = os.path.basename(filepath)
            # Skip self and framework
            if filename in ('agent_software_engineer.py', 'agent_dba.py'):
                continue

            source = self._read_file(filepath)
            if not source:
                continue

            metrics["files_scanned"] += 1

            # Syntax check
            try:
                ast.parse(source)
            except SyntaxError as e:
                metrics["syntax_errors"] += 1
                await ctx.finding("critical", "syntax",
                    f"Syntax error in {filename}: {e}",
                    f"File {filepath} has a Python syntax error and will fail to import")
                continue

            # Extract and validate SQL
            sql_blocks = self._extract_sql_blocks(source)
            metrics["sql_blocks_found"] += len(sql_blocks)

            for line_no, sql in sql_blocks:
                mismatches = self._extract_column_refs(sql, schema)
                for m in mismatches:
                    m["file"] = filepath
                    m["filename"] = filename
                    m["line"] = line_no
                    all_mismatches.append(m)

        # Deduplicate
        seen = set()
        unique_mismatches = []
        for m in all_mismatches:
            key = f"{m['filename']}:{m['table']}:{m['bad_column']}"
            if key not in seen:
                seen.add(key)
                unique_mismatches.append(m)

        metrics["mismatches_found"] = len(unique_mismatches)

        if not unique_mismatches:
            await ctx.finding("info", "code-quality",
                f"All {metrics['files_scanned']} agent files pass schema validation",
                f"{metrics['sql_blocks_found']} SQL blocks checked against {len(schema)} tables")
            return {
                "summary": f"Software Engineer: {metrics['files_scanned']} files scanned, 0 mismatches — all clean",
                "metrics": metrics,
            }

        # ── 3. Report findings ───────────────────────────────
        await ctx.finding("warning", "schema-mismatch",
            f"{len(unique_mismatches)} SQL-schema mismatches found across agent code",
            "\n".join(f"  {m['filename']}: {m['bad_column']} → {m.get('correct_column', '???')} "
                      f"(table {m['table']})" for m in unique_mismatches),
            {"mismatches": unique_mismatches[:20]})

        # ── 4. Generate and apply fixes ──────────────────────
        # Group mismatches by file
        by_file: dict[str, list] = {}
        for m in unique_mismatches:
            by_file.setdefault(m["file"], []).append(m)

        for filepath, file_mismatches in by_file.items():
            if metrics["fixes_applied"] >= max_fixes:
                break

            filename = os.path.basename(filepath)
            source = self._read_file(filepath)
            if not source:
                continue

            # Build a prompt for the LLM with actual schema data
            schema_context = ""
            tables_referenced = set(m["table"] for m in file_mismatches)
            for tbl in tables_referenced:
                cols = schema.get(tbl, set())
                if cols:
                    schema_context += f"\n{tbl}: {', '.join(sorted(cols))}"

            mismatch_desc = "\n".join(
                f"- {m['bad_column']} should be {m.get('correct_column', 'REMOVED')} "
                f"(table {m['table']})"
                for m in file_mismatches
            )

            try:
                fix_plan = await codex_json(
                    self.instructions,
                    f"""Fix these SQL-schema mismatches in {filename}.

MISMATCHES:
{mismatch_desc}

ACTUAL DATABASE SCHEMA:
{schema_context}

SOURCE FILE:
{source[:8000]}

Generate a JSON with "fixes" array. Each fix must have:
- "description": what the fix does
- "old_code": exact string from the source file to find
- "new_code": exact replacement string

RULES:
- The old_code must be an EXACT substring of the source file
- Fix the SQL to use the correct column names from the ACTUAL schema
- Also fix any Python dict key references that depend on changed SQL aliases
- Also fix instruction docstrings if they list wrong column names
- Do NOT add columns to the database — fix the CODE to match the schema
""",
                    model=CODEX_MODEL, effort="medium"
                )
            except Exception as e:
                await ctx.finding("warning", "fix-generation-failed",
                    f"Could not generate fixes for {filename}: {e}")
                continue

            fixes = fix_plan.get("fixes", [])
            for fix in fixes[:5]:  # Max 5 fixes per file
                if metrics["fixes_applied"] >= max_fixes:
                    break

                old_code = fix.get("old_code", "")
                new_code = fix.get("new_code", "")
                desc = fix.get("description", "Schema fix")

                if not old_code or not new_code or old_code == new_code:
                    continue

                success, message = self._apply_fix(filepath, old_code, new_code)

                if success:
                    metrics["fixes_applied"] += 1
                    await ctx.action(f"Applied fix to {filename}: {desc}")

                    # Record in code_fixes table
                    p = await ctx.db()
                    error_hash = hashlib.sha256(
                        f"{filename}:{fix_plan.get('mismatches', mismatch_desc)[:200]}".encode()
                    ).hexdigest()[:16]
                    await p.execute("""
                        INSERT INTO ops.code_fixes
                            (agent_id, run_id, target_file, fix_type, error_hash,
                             original_error, diagnosis, patch, applied, model_used, rollback_patch)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """, self.agent_id, self._run_id, filepath, "python",
                        error_hash, mismatch_desc[:2000],
                        f"Schema mismatch: {desc}",
                        json.dumps({"old": old_code[:500], "new": new_code[:500]}),
                        True, CODEX_MODEL, old_code[:2000])

                    email_sections.append({
                        "heading": f"✅ Fix: {filename}",
                        "content": f"{desc}\n\nOld: {old_code[:200]}\nNew: {new_code[:200]}",
                        "type": "success",
                    })
                    # Re-read the file after successful fix for next iteration
                    source = self._read_file(filepath)
                else:
                    metrics["fixes_failed"] += 1
                    email_sections.append({
                        "heading": f"❌ Fix Failed: {filename}",
                        "content": f"{desc}\nReason: {message}",
                        "type": "error",
                    })

        # ── 5. Notify ────────────────────────────────────────
        if email_sections:
            summary_section = {
                "heading": "📊 Software Engineer Scan Summary",
                "content": (
                    f"Files Scanned: {metrics['files_scanned']}\n"
                    f"SQL Blocks Checked: {metrics['sql_blocks_found']}\n"
                    f"Mismatches Found: {metrics['mismatches_found']}\n"
                    f"Fixes Applied: {metrics['fixes_applied']}\n"
                    f"Fixes Failed: {metrics['fixes_failed']}"
                ),
                "type": "info",
            }
            email_sections.insert(0, summary_section)

            await self._notify(ctx,
                f"Software Engineer — {metrics['fixes_applied']} Fixes Applied",
                email_sections)

        # ── 6. Notify orchestrator ───────────────────────────
        if metrics["fixes_applied"] > 0:
            await self.send_message("orchestrator", "code-fix",
                f"Software Engineer applied {metrics['fixes_applied']} proactive fixes",
                {"fixes_applied": metrics['fixes_applied'],
                 "recommendation": "restart-agents"})

        # Post to chat
        try:
            await self.broadcast(
                f"Proactive scan: {metrics['files_scanned']} files, "
                f"{metrics['mismatches_found']} mismatches, "
                f"{metrics['fixes_applied']} fixed",
                channel="code-fixes", msg_type="status")
        except Exception:
            pass

        return {
            "summary": (f"Software Engineer: {metrics['files_scanned']} files, "
                       f"{metrics['mismatches_found']} mismatches, "
                       f"{metrics['fixes_applied']} fixed"),
            "metrics": metrics,
        }
