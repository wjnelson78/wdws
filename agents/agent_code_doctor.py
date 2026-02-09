"""
🩺 Code Doctor Agent — AI-Powered Auto-Remediation
═══════════════════════════════════════════════════
Athena's Code Doctor: detects agent failures, diagnoses root causes,
generates fixes using OpenAI Codex, applies them safely, and notifies
via Microsoft Graph API email.

Capabilities:
  - Monitor agent errors from ops.agent_runs
  - Parse tracebacks to identify error patterns
  - Query information_schema for actual DB structure
  - Use OpenAI Codex to generate Python/SQL patches
  - Apply fixes with syntax validation
  - Send email notifications via Graph API
  - Full audit trail in ops.agent_logs
"""
import hashlib
import json
import re
import ast
import traceback
from datetime import datetime, timezone

from framework import BaseAgent, RunContext, get_pool, llm_chat, llm_json, codex_json
from config import (
    OPENAI_API_KEY, ALERT_EMAIL,
    GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET,
    GRAPH_SENDER_EMAIL, CODEX_MODEL,
)
from email_util import send_email, build_notification_html


class CodeDoctorAgent(BaseAgent):
    agent_id = "code-doctor"
    agent_name = "Code Doctor"
    description = "AI-powered auto-remediation: diagnoses agent failures, generates Codex fixes, applies patches, sends email alerts"
    version = "1.0.0"
    schedule = "*/10 * * * *"  # Every 10 minutes
    priority = 1  # High priority — needs to run before other agents
    capabilities = [
        "error-diagnosis", "codex-fix", "schema-introspection",
        "auto-patch", "email-notification", "audit-trail"
    ]

    instructions = """You are Code Doctor (Athena AI), the auto-remediation agent for the Nelson Enterprise WDWS platform.

YOUR MISSION:
Detect agent failures, diagnose root causes by analyzing tracebacks and database schema,
generate precise code fixes, apply them safely, and notify the platform administrator.

DIAGNOSIS RULES:
1. Parse the full traceback to identify the exact error type and location
2. For SQL errors (column not found, table not found, relation does not exist):
   - Query information_schema to find the actual column/table names
   - Compare expected vs actual schema
   - Generate a Python code patch that fixes the SQL query
3. For Python errors (KeyError, AttributeError, TypeError):
   - Identify the variable/key that's wrong
   - Suggest the correct accessor pattern
4. For connection errors:
   - Check if the service is running
   - Verify connection parameters

FIX GENERATION:
- Generate minimal, surgical patches — change only what's broken
- Include the exact old code and exact new code for each fix
- Validate Python syntax before applying
- Never modify the framework.py or run.py files
- Never drop tables, delete data, or modify schema destructively

SAFETY:
- Max 3 auto-fixes per run
- If confidence < 0.8, create a finding instead of auto-fixing
- Always log your reasoning chain to ops.agent_logs
- Always send email notification after applying fixes
- Keep a rollback patch for every fix applied
- If the same error occurs 3+ times, escalate instead of re-fixing

OUTPUT FORMAT (when asked for diagnosis as JSON):
{
  "error_type": "column_not_found | table_not_found | key_error | attribute_error | connection | other",
  "confidence": 0.0-1.0,
  "root_cause": "Description of what went wrong",
  "target_file": "/opt/wdws/agents/agent_xxx.py",
  "fixes": [
    {
      "description": "What this fix does",
      "old_code": "exact string to find in file",
      "new_code": "exact replacement string",
      "line_hint": "approximate line number or context"
    }
  ],
  "sql_context": "relevant schema info used for diagnosis"
}"""

    # ── Internal logging helper ──────────────────────────────
    async def _log(self, ctx: RunContext, level: str, category: str, message: str, context: dict = None):
        """Write to ops.agent_logs for full audit trail."""
        p = await ctx.db()
        await p.execute("""
            INSERT INTO ops.agent_logs (agent_id, run_id, level, category, message, context)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, self.agent_id, self._run_id, level, category, message,
            json.dumps(context or {}))

    # ── Email notification ───────────────────────────────────
    async def _notify(self, ctx: RunContext, subject_detail: str, sections: list[dict]):
        """Send email notification via Graph API and log it."""
        subject = f"NELSON ENTERPRISE PLATFORM: {subject_detail}"
        body_html = build_notification_html(subject_detail, sections)

        result = await send_email(
            tenant_id=GRAPH_TENANT_ID,
            client_id=GRAPH_CLIENT_ID,
            client_secret=GRAPH_CLIENT_SECRET,
            sender=GRAPH_SENDER_EMAIL,
            to_recipients=[ALERT_EMAIL],
            subject=subject,
            body_html=body_html,
            importance="high",
        )

        # Track in DB
        p = await ctx.db()
        await p.execute("""
            INSERT INTO ops.email_notifications
                (agent_id, run_id, recipient, subject, body_html, status, error, graph_msg_id, sent_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, self.agent_id, self._run_id, ALERT_EMAIL, subject, body_html,
            result["status"],
            result.get("error"),
            result.get("message_id"),
            datetime.now(timezone.utc) if result["status"] == "sent" else None)

        await self._log(ctx, "email", "notification",
            f"Email {'sent' if result['status']=='sent' else 'FAILED'}: {subject}",
            {"result": result, "recipient": ALERT_EMAIL})

        return result

    # ── Schema introspection ─────────────────────────────────
    async def _get_schema_info(self, ctx: RunContext, schema: str, table: str) -> dict:
        """Get actual table schema from information_schema."""
        columns = await ctx.query("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """, schema, table)
        return {
            "schema": schema,
            "table": table,
            "columns": [dict(c) for c in columns],
            "column_names": [c["column_name"] for c in columns],
        }

    async def _get_all_tables(self, ctx: RunContext) -> list:
        """Get all tables across relevant schemas."""
        rows = await ctx.query("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('core', 'legal', 'medical', 'ops', 'paperless')
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name
        """)
        return [f"{r['table_schema']}.{r['table_name']}" for r in rows]

    # ── Error pattern matching ───────────────────────────────
    def _parse_traceback(self, error_text: str) -> dict:
        """Extract structured info from a traceback string."""
        info = {
            "error_class": None,
            "error_message": None,
            "file": None,
            "line": None,
            "sql_fragments": [],
            "missing_columns": [],
            "missing_tables": [],
            "key_errors": [],
        }

        # Extract error class and message
        lines = error_text.strip().split("\n")
        for line in reversed(lines):
            m = re.match(r'^(\w+(?:\.\w+)*Error|Exception|asyncpg\.\w+):\s*(.+)', line.strip())
            if m:
                info["error_class"] = m.group(1)
                info["error_message"] = m.group(2)
                break

        # Extract file/line from traceback
        file_matches = re.findall(r'File "([^"]+)", line (\d+)', error_text)
        for fpath, lineno in file_matches:
            if "/opt/wdws/agents/" in fpath:
                info["file"] = fpath
                info["line"] = int(lineno)

        # SQL-related patterns
        col_missing = re.findall(r'column ["\']?(\w+\.\w+|\w+)["\']? (?:does not exist|of relation)', error_text)
        info["missing_columns"] = col_missing

        tbl_missing = re.findall(r'relation ["\']?(\w+\.\w+|\w+)["\']? does not exist', error_text)
        info["missing_tables"] = tbl_missing

        # KeyError
        key_errors = re.findall(r"KeyError:\s*['\"](\w+)['\"]", error_text)
        info["key_errors"] = key_errors

        # SQL fragments in error
        sql_frags = re.findall(r'(?:SELECT|INSERT|UPDATE|DELETE|FROM|JOIN|WHERE).*', error_text, re.IGNORECASE)
        info["sql_fragments"] = sql_frags[:5]

        return info

    def _error_hash(self, agent_id: str, error_text: str) -> str:
        """Generate a stable hash for deduplication."""
        # Normalize: strip timestamps, line numbers, PIDs
        normalized = re.sub(r'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}', '', error_text)
        normalized = re.sub(r'Run #\d+', 'Run #X', normalized)
        return hashlib.sha256(f"{agent_id}:{normalized[:500]}".encode()).hexdigest()[:16]

    # ── Codex-powered fix generation ─────────────────────────
    async def _generate_fix(self, ctx: RunContext, agent_id: str,
                            error_text: str, parsed: dict,
                            schema_info: list[dict], file_content: str) -> dict:
        """Use OpenAI Codex to diagnose and generate a fix."""
        # Build context about the actual schema
        schema_context = ""
        for si in schema_info:
            schema_context += f"\nTable {si['schema']}.{si['table']} columns: {', '.join(si['column_names'])}"

        all_tables = await self._get_all_tables(ctx)

        prompt = f"""Analyze this agent error and generate a precise code fix.

FAILED AGENT: {agent_id}
ERROR:
{error_text[:3000]}

PARSED ERROR INFO:
- Error class: {parsed['error_class']}
- Error message: {parsed['error_message']}
- File: {parsed['file']}
- Line: {parsed['line']}
- Missing columns: {parsed['missing_columns']}
- Missing tables: {parsed['missing_tables']}
- Key errors: {parsed['key_errors']}

ACTUAL DATABASE SCHEMA:
{schema_context}

ALL TABLES IN DATABASE:
{', '.join(all_tables)}

SOURCE FILE CONTENT (relevant section):
{file_content[:6000]}

Generate a JSON diagnosis with exact code patches. The old_code must be an EXACT substring
of the source file. The new_code must be a valid replacement.
"""

        await self._log(ctx, "reasoning", "codex-prompt",
            f"Sending diagnosis request to Codex for {agent_id}",
            {"model": CODEX_MODEL, "api": "responses", "reasoning_effort": "medium", "prompt_length": len(prompt)})

        diagnosis = await codex_json(self.instructions, prompt, model=CODEX_MODEL, effort="medium")

        await self._log(ctx, "reasoning", "codex-response",
            f"Codex diagnosis: {diagnosis.get('root_cause', 'unknown')[:200]}",
            {"diagnosis": diagnosis})

        return diagnosis

    # ── Apply a fix to a file on disk ────────────────────────
    async def _apply_fix(self, ctx: RunContext, target_file: str,
                         old_code: str, new_code: str) -> tuple[bool, str]:
        """Apply a code fix by replacing old_code with new_code in target_file."""
        try:
            # Read the file
            p = await ctx.db()
            file_result = await p.fetchval(
                "SELECT pg_read_file($1)",
                target_file
            )
        except Exception:
            # pg_read_file might not work for agent files, use a different approach
            file_result = None

        # We'll apply fixes via direct file operations through the framework
        # Since we're running on the same server, we can read/write files directly
        try:
            with open(target_file, 'r') as f:
                content = f.read()
        except Exception as e:
            return False, f"Cannot read {target_file}: {e}"

        if old_code not in content:
            # Try with normalized whitespace
            normalized_content = re.sub(r'[ \t]+', ' ', content)
            normalized_old = re.sub(r'[ \t]+', ' ', old_code)
            if normalized_old not in normalized_content:
                return False, f"old_code not found in {target_file}"
            # Find the actual text to replace
            idx = normalized_content.find(normalized_old)
            # Map back to original positions — too risky, bail
            return False, "old_code match requires whitespace normalization — manual review needed"

        # Validate new code syntax (for Python files)
        if target_file.endswith('.py'):
            # Try to parse just the new code in context
            test_content = content.replace(old_code, new_code, 1)
            try:
                ast.parse(test_content)
            except SyntaxError as e:
                return False, f"Syntax error in patched file: {e}"

        # Create backup
        backup_path = target_file + ".bak"
        try:
            with open(backup_path, 'w') as f:
                f.write(content)
        except Exception as e:
            return False, f"Cannot create backup: {e}"

        # Apply the fix
        new_content = content.replace(old_code, new_code, 1)
        try:
            with open(target_file, 'w') as f:
                f.write(new_content)
        except Exception as e:
            # Rollback
            with open(target_file, 'w') as f:
                f.write(content)
            return False, f"Write failed, rolled back: {e}"

        return True, "Applied successfully"

    # ── Read agent source file ───────────────────────────────
    def _read_agent_file(self, filepath: str) -> str:
        """Read an agent source file."""
        try:
            with open(filepath, 'r') as f:
                return f.read()
        except Exception as e:
            return f"# ERROR reading file: {e}"

    # ═══════════════════════════════════════════════════════════
    #  MAIN RUN LOGIC
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        metrics = {
            "errors_found": 0,
            "diagnosed": 0,
            "fixes_applied": 0,
            "fixes_failed": 0,
            "emails_sent": 0,
            "escalated": 0,
            "skipped_duplicate": 0,
        }
        max_fixes_per_run = 3

        await self._log(ctx, "info", "startup",
            "Code Doctor starting diagnostic scan")

        # ── 1. Find recent agent errors ──────────────────────
        errors = await ctx.query("""
            SELECT r.id as run_id, r.agent_id, r.error, r.started_at,
                   a.name as agent_name, a.version,
                   r.ended_at, r.duration_ms
            FROM ops.agent_runs r
            JOIN ops.agent_registry a ON r.agent_id = a.id
            WHERE r.status = 'error'
              AND r.started_at > now() - interval '2 hours'
              AND r.agent_id != 'code-doctor'
            ORDER BY r.started_at DESC
            LIMIT 20
        """)

        metrics["errors_found"] = len(errors)

        if not errors:
            await self._log(ctx, "info", "scan",
                "No agent errors in the last 2 hours — all clear")
            return {
                "summary": "Code Doctor: No errors found — system healthy",
                "metrics": metrics,
            }

        await self._log(ctx, "info", "scan",
            f"Found {len(errors)} agent errors in the last 2 hours",
            {"agents_with_errors": list(set(e["agent_id"] for e in errors))})

        # ── 2. Deduplicate errors ────────────────────────────
        seen_hashes = set()
        unique_errors = []
        for err in errors:
            h = self._error_hash(err["agent_id"], err["error"] or "")
            if h not in seen_hashes:
                seen_hashes.add(h)
                unique_errors.append((err, h))
            else:
                metrics["skipped_duplicate"] += 1

        await self._log(ctx, "info", "dedup",
            f"{len(unique_errors)} unique errors after deduplication "
            f"({metrics['skipped_duplicate']} duplicates)")

        # ── 3. Check for repeat offenders ────────────────────
        fix_counts = {}
        for err, h in unique_errors:
            existing = await ctx.query("""
                SELECT COUNT(*) as count FROM ops.code_fixes
                WHERE error_hash = $1
            """, h)
            fix_counts[h] = existing[0]["count"] if existing else 0

        # ── 4. Diagnose and fix each unique error ────────────
        applied_fixes = []
        email_sections = []

        for err, error_hash in unique_errors:
            if metrics["fixes_applied"] >= max_fixes_per_run:
                await self._log(ctx, "warn", "rate-limit",
                    f"Max fixes per run ({max_fixes_per_run}) reached — deferring remaining errors")
                break

            agent_id = err["agent_id"]
            error_text = err["error"] or "Unknown error"

            await self._log(ctx, "reasoning", "diagnosis-start",
                f"Diagnosing error from {agent_id}: {error_text[:200]}",
                {"run_id": err["run_id"], "error_hash": error_hash})

            # Check repeat count
            if fix_counts.get(error_hash, 0) >= 3:
                metrics["escalated"] += 1
                await ctx.finding("critical", "repeat-failure",
                    f"Agent {agent_id} has failed 3+ times with same error",
                    error_text[:500],
                    {"error_hash": error_hash, "fix_attempts": fix_counts[error_hash]})

                email_sections.append({
                    "heading": f"⚠️ ESCALATION: {agent_id} — Recurring Failure",
                    "content": f"This error has been auto-fixed {fix_counts[error_hash]} times but keeps recurring.\n\n"
                               f"Error: {error_text[:300]}\n\nManual intervention required.",
                    "type": "error",
                })
                continue

            # Parse the traceback
            parsed = self._parse_traceback(error_text)

            await self._log(ctx, "reasoning", "parsed-error",
                f"Parsed: class={parsed['error_class']}, "
                f"file={parsed['file']}, line={parsed['line']}, "
                f"missing_cols={parsed['missing_columns']}, "
                f"missing_tables={parsed['missing_tables']}",
                {"parsed": parsed})

            # Gather schema info for relevant tables
            schema_info = []
            # Extract table references from error
            table_refs = set()
            for col in parsed["missing_columns"]:
                if "." in col:
                    parts = col.split(".")
                    table_refs.add(parts[0])

            for tbl in parsed["missing_tables"]:
                if "." in tbl:
                    schema, name = tbl.split(".", 1)
                    schema_info.append(await self._get_schema_info(ctx, schema, name))
                table_refs.add(tbl)

            # Also get schema for tables mentioned in SQL fragments
            for frag in parsed["sql_fragments"]:
                for match in re.findall(r'(?:FROM|JOIN|INTO|UPDATE)\s+(\w+\.\w+)', frag, re.IGNORECASE):
                    schema, name = match.split(".", 1)
                    schema_info.append(await self._get_schema_info(ctx, schema, name))

            # Get commonly referenced tables
            for common_table in [
                ("ops", "agent_runs"), ("ops", "agent_findings"), ("ops", "agent_registry"),
                ("ops", "agent_messages"), ("ops", "agent_memory"),
                ("core", "documents"), ("core", "document_chunks"),
                ("legal", "email_metadata"), ("ops", "oauth_clients"),
            ]:
                if any(common_table[1] in str(f) for f in parsed["sql_fragments"]):
                    schema_info.append(await self._get_schema_info(ctx, *common_table))

            # Deduplicate schema_info
            seen_tables = set()
            deduped_schema = []
            for si in schema_info:
                key = f"{si['schema']}.{si['table']}"
                if key not in seen_tables and si["columns"]:
                    seen_tables.add(key)
                    deduped_schema.append(si)

            # Read the agent source file
            target_file = parsed["file"]
            if not target_file:
                # Infer from agent_id
                target_file = f"/opt/wdws/agents/agent_{agent_id.replace('-', '_')}.py"

            file_content = self._read_agent_file(target_file)

            # ── Generate fix using Codex ─────────────────────
            try:
                diagnosis = await self._generate_fix(
                    ctx, agent_id, error_text, parsed,
                    deduped_schema, file_content
                )
                metrics["diagnosed"] += 1
            except Exception as e:
                await self._log(ctx, "error", "codex-failure",
                    f"Codex failed to generate diagnosis: {e}",
                    {"traceback": traceback.format_exc()})
                await ctx.finding("warning", "diagnosis-failed",
                    f"Code Doctor could not diagnose {agent_id} error",
                    str(e))
                continue

            confidence = diagnosis.get("confidence", 0)
            root_cause = diagnosis.get("root_cause", "Unknown")
            fixes = diagnosis.get("fixes", [])

            await self._log(ctx, "reasoning", "diagnosis-complete",
                f"Diagnosis for {agent_id}: confidence={confidence}, "
                f"root_cause={root_cause[:200]}, fixes={len(fixes)}",
                {"diagnosis": diagnosis})

            # ── Confidence check ─────────────────────────────
            if confidence < 0.8 or not fixes:
                await ctx.finding("warning", "low-confidence",
                    f"Code Doctor diagnosis for {agent_id} (confidence: {confidence:.0%})",
                    f"Root cause: {root_cause}\n\nFixes suggested but not auto-applied due to low confidence.",
                    {"diagnosis": diagnosis})

                email_sections.append({
                    "heading": f"🔍 Low-Confidence Diagnosis: {agent_id}",
                    "content": f"Confidence: {confidence:.0%}\nRoot Cause: {root_cause}\n\n"
                               f"Fixes proposed but NOT applied — manual review needed.",
                    "type": "warning",
                })
                continue

            # ── Apply fixes ──────────────────────────────────
            for fix in fixes[:3]:  # Max 3 patches per error
                old_code = fix.get("old_code", "")
                new_code = fix.get("new_code", "")
                fix_desc = fix.get("description", "Code fix")

                if not old_code or not new_code:
                    continue

                await self._log(ctx, "reasoning", "applying-fix",
                    f"Applying fix to {target_file}: {fix_desc}",
                    {"old_code": old_code[:300], "new_code": new_code[:300]})

                success, message = await self._apply_fix(ctx, target_file, old_code, new_code)

                # Record the fix
                p = await ctx.db()
                await p.execute("""
                    INSERT INTO ops.code_fixes
                        (agent_id, run_id, target_file, fix_type, error_hash,
                         original_error, diagnosis, patch, applied, model_used, rollback_patch)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """, self.agent_id, self._run_id, target_file, "python",
                    error_hash, error_text[:2000], root_cause,
                    json.dumps({"old": old_code, "new": new_code}),
                    success, CODEX_MODEL, old_code)

                if success:
                    metrics["fixes_applied"] += 1
                    await ctx.action(f"Applied Codex fix to {target_file}: {fix_desc}")

                    email_sections.append({
                        "heading": f"✅ Auto-Fix Applied: {agent_id}",
                        "content": f"File: {target_file}\n"
                                   f"Root Cause: {root_cause}\n"
                                   f"Fix: {fix_desc}\n\n"
                                   f"Old Code:\n{old_code[:300]}\n\n"
                                   f"New Code:\n{new_code[:300]}",
                        "type": "success",
                    })
                else:
                    metrics["fixes_failed"] += 1
                    await self._log(ctx, "error", "fix-failed",
                        f"Fix failed for {target_file}: {message}",
                        {"fix": fix})

                    email_sections.append({
                        "heading": f"❌ Fix Failed: {agent_id}",
                        "content": f"File: {target_file}\n"
                                   f"Reason: {message}\n"
                                   f"Manual intervention needed.",
                        "type": "error",
                    })

            applied_fixes.append(agent_id)

        # ── 5. Notify agent system to reload ─────────────────
        if metrics["fixes_applied"] > 0:
            await self.send_message("orchestrator", "code-fix",
                f"Code Doctor applied {metrics['fixes_applied']} fixes",
                {
                    "agents_fixed": applied_fixes,
                    "recommendation": "restart-agents",
                    "fixes_applied": metrics["fixes_applied"],
                })

            await self._log(ctx, "info", "agent-message",
                f"Notified orchestrator: {metrics['fixes_applied']} fixes applied, "
                f"agents to restart: {applied_fixes}")

        # ── 6. Send email notification ───────────────────────
        if email_sections:
            summary_section = {
                "heading": "📊 Scan Summary",
                "content": (
                    f"Errors Found: {metrics['errors_found']}\n"
                    f"Unique Errors: {len(unique_errors)}\n"
                    f"Fixes Applied: {metrics['fixes_applied']}\n"
                    f"Fixes Failed: {metrics['fixes_failed']}\n"
                    f"Escalated: {metrics['escalated']}\n"
                    f"Duplicates Skipped: {metrics['skipped_duplicate']}"
                ),
                "type": "info",
            }
            email_sections.insert(0, summary_section)

            subject_detail = (
                f"Code Doctor — {metrics['fixes_applied']} Fixes Applied"
                if metrics['fixes_applied'] > 0
                else f"Code Doctor — {metrics['errors_found']} Errors Detected"
            )

            result = await self._notify(ctx, subject_detail, email_sections)
            if result["status"] == "sent":
                metrics["emails_sent"] += 1

        # ── 7. Store run memory ──────────────────────────────
        await ctx.remember("last_scan", {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "errors_found": metrics["errors_found"],
            "fixes_applied": metrics["fixes_applied"],
            "agents_fixed": applied_fixes,
        })

        summary = (
            f"Code Doctor: {metrics['errors_found']} errors, "
            f"{metrics['diagnosed']} diagnosed, "
            f"{metrics['fixes_applied']} fixed, "
            f"{metrics['emails_sent']} emails sent"
        )

        await self._log(ctx, "info", "complete", summary, {"metrics": metrics})

        # ── Post to Chat Room ──
        try:
            await self.broadcast(
                f"Code scan complete: {metrics.get('issues_found',0)} issues, "
                f"{metrics.get('fixes_proposed',0)} fixes proposed, "
                f"{metrics.get('fixes_applied',0)} applied",
                channel="code-fixes", msg_type="status")
        except Exception as e:
            self.log.debug(f"Chat post failed: {e}")

        return {"summary": summary, "metrics": metrics}
