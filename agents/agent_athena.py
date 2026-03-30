"""
🧠 Athena — Autonomous Platform Intelligence
══════════════════════════════════════════════
The central reasoning agent for the Athena Cognitive Platform.

Unlike other agents, Athena does NOT have hardcoded detection logic.
Each run it:
  1. Loads full platform state (findings, runs, metrics, source files, memory)
  2. Asks the LLM: "What needs attention? What should I do about it?"
  3. Executes the plan using its full capability set
  4. Records outcomes in memory so future runs build on them

Capabilities:
  - Update any agent's runtime config (via remember_for)
  - Write and apply additive schema migrations
  - Queue destructive changes for human approval
  - Patch agent source files with syntax validation
  - Delegate work to specialist agents via @mention
  - Send the morning brief
  - Learn from outcomes across cycles

Confidence gates:
  >= 0.85  → Execute immediately
  0.70–0.84 → Execute low-risk actions; queue high-risk for approval
  < 0.70   → Queue for human review
"""
import os
import json
import re
import time
import hashlib
import asyncio
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Optional

from framework import BaseAgent, RunContext, codex_json
from email_util import send_email, build_notification_html
from config import OPENAI_MODEL

ALERT_EMAIL = os.getenv("ALERT_EMAIL", "william@seattleseahawks.me")
ALERT_FROM   = os.getenv("ALERT_FROM_EMAIL", "athena@seattleseahawks.me")

# Additive DDL keywords — safe to auto-apply after dry-run
_ADDITIVE_KEYWORDS = frozenset([
    "create table", "create index", "create type", "create sequence",
    "create view", "add column", "create function", "create trigger",
    "comment on",
])

# These DDL keywords touch existing data — require human approval
_DESTRUCTIVE_KEYWORDS = frozenset([
    "drop table", "drop column", "drop index", "truncate",
    "rename table", "rename column", "alter column",
    "not null",  # adding NOT NULL to existing column
])

# Agent source files Athena is allowed to patch
_AGENT_FILES_ROOT = "/opt/wdws/agents"


def _is_additive(sql: str) -> bool:
    low = sql.lower()
    has_additive = any(k in low for k in _ADDITIVE_KEYWORDS)
    has_destructive = any(k in low for k in _DESTRUCTIVE_KEYWORDS)
    return has_additive and not has_destructive


def _generate_rollback(sql: str) -> str:
    """Best-effort rollback SQL generation for common additive DDL."""
    lines = []
    for stmt in re.split(r';\s*', sql.strip()):
        s = stmt.strip().lower()
        if not s:
            continue
        if s.startswith("create table if not exists"):
            m = re.search(r'create table if not exists\s+([\w.]+)', s)
            if m:
                lines.append(f"DROP TABLE IF EXISTS {m.group(1)};")
        elif s.startswith("create table"):
            m = re.search(r'create table\s+([\w.]+)', s)
            if m:
                lines.append(f"DROP TABLE IF EXISTS {m.group(1)};")
        elif s.startswith("create index") or s.startswith("create unique index"):
            m = re.search(r'create (?:unique )?index (?:concurrently )?(?:if not exists )?(\w+)', s)
            if m:
                lines.append(f"DROP INDEX IF EXISTS {m.group(1)};")
        elif s.startswith("alter table") and "add column" in s:
            tbl_m = re.search(r'alter table\s+([\w.]+)', s)
            col_m = re.search(r'add column(?:\s+if not exists)?\s+(\w+)', s)
            if tbl_m and col_m:
                lines.append(f"ALTER TABLE {tbl_m.group(1)} DROP COLUMN IF EXISTS {col_m.group(1)};")
        elif s.startswith("create type"):
            m = re.search(r'create type\s+([\w.]+)', s)
            if m:
                lines.append(f"DROP TYPE IF EXISTS {m.group(1)};")
        elif s.startswith("create view"):
            m = re.search(r'create (?:or replace )?view\s+([\w.]+)', s)
            if m:
                lines.append(f"DROP VIEW IF EXISTS {m.group(1)};")
    return "\n".join(lines) if lines else "-- Manual rollback required"


class AthenaAgent(BaseAgent):
    agent_id   = "athena"
    agent_name = "Athena"
    description = (
        "Autonomous platform intelligence — reasons over full system state, "
        "updates agent configs, applies schema migrations, delegates work, "
        "and learns from outcomes across cycles."
    )
    version  = "1.0.0"
    schedule = "*/10 * * * *"   # Every 10 minutes
    priority = 1                 # High — runs right after orchestrator
    capabilities = [
        "autonomous-reasoning", "schema-migration", "agent-config",
        "source-patching", "delegation", "morning-brief", "compounding-memory",
    ]

    instructions = """You are Athena, the autonomous intelligence core of the Athena Cognitive Platform.

You are NOT a reactive rule-engine. You reason over the full state of the platform each cycle
and decide what to do about it. You have genuine agency: you can update configurations,
apply database migrations, patch agent source code, delegate tasks, and learn from outcomes.

THE PLATFORM:
- PostgreSQL database (schema: ops, core, legal, medical, paperless, chat)
- 16+ specialized agents running on schedule (watchdog, dba, code-doctor, etc.)
- Python 3 / asyncio codebase in /opt/wdws/agents/
- systemd service: wdws-agents

YOUR TOOLS (used via RunContext):
- ctx.query(sql) / ctx.execute(sql)  — read/write the DB
- ctx.shell(cmd)                      — run shell commands
- ctx.read_file(path)                 — read any file
- ctx.patch_file(path, old, new)      — patch a file (validates Python syntax)
- ctx.remember(key, value)            — store in your own memory
- ctx.recall(key)                     — retrieve from your memory
- ctx.remember_for(agent_id, k, v)    — update another agent's runtime config
- ctx.recall_for(agent_id, key)       — read another agent's config
- ctx.finding(sev, cat, title, ...)   — record a finding
- ctx.ask_agent(agent_id, question)   — delegate via @mention

DECISION FRAMEWORK:
Confidence >= 0.85: Execute immediately. Record outcome.
Confidence 0.70-0.84: Execute low-risk. Queue high-risk for human approval.
Confidence < 0.70: Queue for human review. Explain reasoning.

SCHEMA MIGRATION RULES:
- Additive only (CREATE TABLE, ADD COLUMN, CREATE INDEX): auto-apply after dry-run
- Destructive (DROP, TRUNCATE, RENAME, ALTER type): always queue for approval
- Every migration gets a name, forward SQL, and auto-generated rollback SQL
- Always dry-run first: BEGIN; [sql]; ROLLBACK; — if it errors, do not apply

LEARNING:
After every action, record the outcome in your memory. Next cycle, load that history
and build on it. If something you tried didn't work, don't repeat it — escalate instead.

MORNING BRIEF (runs at 6 AM):
Produce a concise email to the human owner covering:
- What you did in the last 24h and whether it worked
- Open issues requiring human decisions
- System health summary
- What you plan to do next

NEVER:
- Drop or truncate data without explicit human approval in ops.pending_approvals
- Override litigation holds in the retention system
- Modify authentication or security credentials
- Take actions you haven't recorded with reasoning
"""

    def _finding_signature(self, title: str, category: str = "", agent_id: str = "") -> str:
        """Normalize dynamic finding titles into stable signatures.

        Watchdog performance alerts often embed live measurements in the title,
        which makes exact-title grouping useless for learning. Collapse those
        into stable signatures so Athena can recognize repeat patterns.
        """
        title = (title or "").strip()
        if not title:
            return ""

        if category == "performance" and agent_id == "watchdog":
            if title.startswith("Agent p95 latency regression:"):
                return "Agent p95 latency regression"
            if title.startswith("MCP p95 latency regression:"):
                return "MCP p95 latency regression"

        return title

    def _summarize_repeated_findings(self, rows: list[dict]) -> list[dict]:
        """Aggregate repeated findings using normalized signatures."""
        grouped = {}
        for row in rows:
            title = row.get("title", "")
            category = row.get("category", "")
            agent_id = row.get("agent_id", "")
            signature = self._finding_signature(title, category, agent_id)
            key = (signature, category, agent_id)
            bucket = grouped.setdefault(key, {
                "signature": signature,
                "title": title,
                "category": category,
                "agent_id": agent_id,
                "occurrences": 0,
                "first_seen": row.get("created_at"),
                "last_seen": row.get("created_at"),
                "sample_titles": [],
            })
            bucket["occurrences"] += 1
            created_at = row.get("created_at")
            if created_at is not None:
                if bucket["first_seen"] is None or created_at < bucket["first_seen"]:
                    bucket["first_seen"] = created_at
                if bucket["last_seen"] is None or created_at > bucket["last_seen"]:
                    bucket["last_seen"] = created_at
                    bucket["title"] = title
            if title and title not in bucket["sample_titles"]:
                bucket["sample_titles"].append(title)

        repeated = [
            {
                **bucket,
                "sample_titles": bucket["sample_titles"][:5],
            }
            for bucket in grouped.values()
            if bucket["occurrences"] >= 3
        ]
        repeated.sort(key=lambda item: (item["occurrences"], item["last_seen"]), reverse=True)
        return repeated[:20]

    def _merge_tasks(self, *task_lists: list[dict]) -> list[dict]:
        """Merge task lists while removing obvious duplicates."""
        merged = []
        seen = set()
        for tasks in task_lists:
            for task in tasks or []:
                key = (
                    task.get("type", "unknown"),
                    task.get("title", ""),
                    task.get("agent_id") or task.get("target_agent") or task.get("file_path") or "",
                    task.get("config_key", ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                merged.append(task)
        return merged

    def _task_guardrail_key(self, task: dict) -> str:
        """Build a stable key for suppressing retries of unsafe actions."""
        t_type = task.get("type", "unknown")
        if t_type == "config_change":
            target = "|".join([
                task.get("agent_id", ""),
                task.get("config_key", ""),
                task.get("finding_signature", ""),
                ",".join(sorted(task.get("exclude_agents") or [])),
            ])
            return f"config_change|{target}"
        if t_type == "source_patch":
            patch_hash = hashlib.sha256(
                f"{task.get('file_path','')}|{task.get('old_code','')}|{task.get('new_code','')}".encode()
            ).hexdigest()[:12]
            return f"source_patch|{task.get('file_path','')}|{patch_hash}"
        return f"{t_type}|{task.get('title','')}|{task.get('agent_id') or task.get('target_agent') or task.get('file_path','')}"

    def _strategy_key(self, task: dict) -> str:
        """Build a stable identifier for ranking mitigation strategies over time."""
        t_type = task.get("type", "unknown")
        if t_type == "config_change":
            value = task.get("config_value")
            if isinstance(value, list):
                value = ",".join(sorted(str(v) for v in value))
            return "|".join([
                t_type,
                task.get("agent_id", ""),
                task.get("config_key", ""),
                str(value),
                task.get("finding_signature", ""),
            ])
        if t_type == "source_patch":
            return "|".join([
                t_type,
                task.get("file_path", ""),
                hashlib.sha256(task.get("new_code", "").encode()).hexdigest()[:12],
            ])
        return "|".join([
            t_type,
            task.get("title", ""),
            task.get("agent_id") or task.get("target_agent") or task.get("file_path", ""),
        ])

    def _strategy_sort_key(self, task: dict, memory: dict) -> tuple:
        """Score tasks so previously successful strategies rise to the top."""
        key = self._strategy_key(task)
        scores = memory.get("strategy_scores") or {}
        entry = scores.get(key, {}) if isinstance(scores, dict) else {}
        notes = memory.get("strategy_notes") or []
        verified = int(entry.get("verified", 0) or 0)
        rolled_back = int(entry.get("rolled_back", 0) or 0)
        failed = int(entry.get("failed", 0) or 0)
        attempts = int(entry.get("attempts", 0) or 0)
        success_ratio = verified / attempts if attempts else 0.0
        context_bonus = 0
        task_ctx = task.get("strategy_context") or {}
        for note in notes:
            if note.get("strategy_key") != key:
                continue
            note_ctx = note.get("conditions") or {}
            if not task_ctx or not note_ctx:
                continue
            matches = 0
            total = 0
            for k, v in note_ctx.items():
                if k not in task_ctx:
                    continue
                total += 1
                if task_ctx.get(k) == v:
                    matches += 1
            if total:
                context_bonus = max(context_bonus, matches / total)
        return (
            -(success_ratio + context_bonus),
            rolled_back + failed,
            -verified,
            int(task.get("priority", 999) or 999),
            task.get("title", ""),
        )

    async def _record_strategy_outcome(self, ctx: RunContext, verification: dict, rollback: dict):
        """Persist success/failure counts for a mitigation strategy."""
        t_type = verification.get("task_type")
        if t_type not in {"config_change", "source_patch", "schema_migration"}:
            return

        task = {
            "type": t_type,
            "title": verification.get("task_title", ""),
            "agent_id": verification.get("agent_id"),
            "config_key": verification.get("config_key"),
            "config_value": verification.get("expected_value"),
            "finding_signature": verification.get("finding_signature"),
            "file_path": verification.get("file_path"),
            "new_code": verification.get("new_code", ""),
        }
        key = self._strategy_key(task)
        scores = await ctx.recall("strategy_scores") or {}
        if not isinstance(scores, dict):
            scores = {}

        entry = scores.get(key, {
            "strategy": task.get("title", ""),
            "task_type": t_type,
            "attempts": 0,
            "verified": 0,
            "failed": 0,
            "rolled_back": 0,
            "last_result": None,
            "last_time": None,
        })
        entry["attempts"] = int(entry.get("attempts", 0) or 0) + 1
        if verification.get("status") == "verified":
            entry["verified"] = int(entry.get("verified", 0) or 0) + 1
            entry["last_result"] = "verified"
        elif rollback.get("success"):
            entry["rolled_back"] = int(entry.get("rolled_back", 0) or 0) + 1
            entry["last_result"] = "rolled_back"
        else:
            entry["failed"] = int(entry.get("failed", 0) or 0) + 1
            entry["last_result"] = "failed"
        entry["last_time"] = verification.get("checked_at") or datetime.now(timezone.utc).isoformat()
        scores[key] = entry
        await ctx.remember("strategy_scores", scores)

        notes = await ctx.recall("strategy_notes") or []
        notes.append({
            "time": verification.get("checked_at") or datetime.now(timezone.utc).isoformat(),
            "strategy_key": key,
            "strategy": task.get("title", ""),
            "result": entry.get("last_result"),
            "conditions": verification.get("strategy_context") or {},
            "detail": verification.get("detail", "")[:240],
        })
        await ctx.remember("strategy_notes", notes[-60:])

    def _task_has_fresh_evidence(self, task: dict, guardrail: dict, state: dict) -> bool:
        """Decide if evidence changed enough to allow retrying a blocked action."""
        blocked_at = guardrail.get("blocked_at")
        if task.get("type") == "config_change" and task.get("agent_id") == "watchdog":
            signature = task.get("finding_signature") or guardrail.get("finding_signature")
            repeated = next(
                (
                    item for item in (state.get("repeated_findings") or [])
                    if item.get("agent_id") == "watchdog"
                    and item.get("signature") == signature
                ),
                None,
            )
            if not repeated:
                return False

            current_occurrences = int(repeated.get("occurrences") or 0)
            blocked_occurrences = int((guardrail.get("evidence") or {}).get("occurrences") or 0)
            current_agents = set(task.get("exclude_agents") or [])
            blocked_agents = set(guardrail.get("exclude_agents") or [])
            if current_agents - blocked_agents:
                return True
            if current_occurrences >= blocked_occurrences + 3:
                return True

            current_last_seen = repeated.get("last_seen")
            blocked_last_seen = (guardrail.get("evidence") or {}).get("last_seen")
            if current_last_seen and blocked_last_seen:
                try:
                    current_dt = datetime.fromisoformat(str(current_last_seen))
                    blocked_dt = datetime.fromisoformat(str(blocked_last_seen))
                    if current_dt > blocked_dt + timedelta(hours=4):
                        return True
                except Exception:
                    pass

        if blocked_at:
            try:
                blocked_dt = datetime.fromisoformat(str(blocked_at))
                if datetime.now(timezone.utc) - blocked_dt >= timedelta(hours=6):
                    return True
            except Exception:
                pass

        return False

    def _get_retry_guardrail(self, task: dict, memory: dict, state: dict) -> Optional[dict]:
        """Return a matching retry guardrail if the task should still be suppressed."""
        key = self._task_guardrail_key(task)
        for guardrail in (memory.get("retry_guardrails") or []):
            if guardrail.get("key") != key:
                continue
            if self._task_has_fresh_evidence(task, guardrail, state):
                return None
            return guardrail
        return None

    async def _remember_retry_guardrail(self, ctx: RunContext, verification: dict, rollback: dict):
        """Persist a guardrail so Athena does not blindly retry a reverted action."""
        t_type = verification.get("task_type")
        if t_type not in {"config_change", "source_patch"}:
            return

        blocked_at = verification.get("checked_at") or datetime.now(timezone.utc).isoformat()
        guardrails = await ctx.recall("retry_guardrails") or []
        task = {
            "type": t_type,
            "title": verification.get("task_title", ""),
            "agent_id": verification.get("agent_id"),
            "config_key": verification.get("config_key"),
            "finding_signature": verification.get("finding_signature"),
            "exclude_agents": verification.get("exclude_agents") or [],
            "file_path": verification.get("file_path"),
            "old_code": verification.get("old_code", ""),
            "new_code": verification.get("new_code", ""),
        }
        key = self._task_guardrail_key(task)
        evidence = {
            "occurrences": int(verification.get("matching_count") or len(verification.get("evidence") or [])),
            "last_seen": blocked_at,
        }

        updated = False
        for item in guardrails:
            if item.get("key") != key:
                continue
            item.update({
                "blocked_at": blocked_at,
                "reason": verification.get("detail", "")[:300],
                "rollback": rollback,
                "evidence": evidence,
                "exclude_agents": verification.get("exclude_agents") or item.get("exclude_agents") or [],
                "finding_signature": verification.get("finding_signature") or item.get("finding_signature"),
            })
            updated = True
            break

        if not updated:
            guardrails.append({
                "key": key,
                "task_type": t_type,
                "task_title": verification.get("task_title", ""),
                "agent_id": verification.get("agent_id"),
                "config_key": verification.get("config_key"),
                "finding_signature": verification.get("finding_signature"),
                "exclude_agents": verification.get("exclude_agents") or [],
                "file_path": verification.get("file_path"),
                "blocked_at": blocked_at,
                "reason": verification.get("detail", "")[:300],
                "rollback": rollback,
                "evidence": evidence,
            })

        await ctx.remember("retry_guardrails", guardrails[-30:])

    def _mitigation_plan_key(self, plan_type: str, signature: str) -> str:
        return f"{plan_type}|{signature}"

    def _get_mitigation_plan(self, memory: dict, plan_key: str) -> Optional[dict]:
        for plan in (memory.get("mitigation_plans") or []):
            if plan.get("plan_key") == plan_key:
                return plan
        return None

    async def _sync_mitigation_plans(self, ctx: RunContext, state: dict, memory: dict) -> list[dict]:
        """Create or refresh active mitigation plans from current repeated findings."""
        plans = await ctx.recall("mitigation_plans") or []
        now = datetime.now(timezone.utc).isoformat()
        changed = False

        latency_repeat = next(
            (
                item for item in (state.get("repeated_findings") or [])
                if item.get("agent_id") == "watchdog"
                and item.get("signature") == "Agent p95 latency regression"
            ),
            None,
        )
        if latency_repeat:
            plan_key = self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression")
            existing = next((p for p in plans if p.get("plan_key") == plan_key), None)
            if not existing or existing.get("status") in {"resolved", "escalated"}:
                plan = {
                    "plan_key": plan_key,
                    "plan_type": "watchdog_latency",
                    "title": "Mitigate watchdog latency false positives",
                    "finding_signature": "Agent p95 latency regression",
                    "status": "active",
                    "current_step": 0,
                    "steps": ["exclude_heavy_agents", "broaden_window", "raise_min_runs"],
                    "max_failures": 3,
                    "failures": 0,
                    "created_at": now,
                    "updated_at": now,
                    "last_seen": str(latency_repeat.get("last_seen")),
                    "occurrences": int(latency_repeat.get("occurrences") or 0),
                }
                if existing:
                    plans = [plan if p.get("plan_key") == plan_key else p for p in plans]
                else:
                    plans.append(plan)
                changed = True
            else:
                if (
                    existing.get("last_seen") != str(latency_repeat.get("last_seen"))
                    or int(existing.get("occurrences") or 0) != int(latency_repeat.get("occurrences") or 0)
                ):
                    existing["last_seen"] = str(latency_repeat.get("last_seen"))
                    existing["occurrences"] = int(latency_repeat.get("occurrences") or 0)
                    existing["updated_at"] = now
                    changed = True

        if changed:
            await ctx.remember("mitigation_plans", plans[-20:])
        return plans

    async def _advance_mitigation_plan(self, ctx: RunContext, task: dict, result: str, detail: str) -> Optional[dict]:
        """Advance, resolve, or escalate a mitigation plan based on an outcome."""
        plan_key = task.get("plan_key")
        if not plan_key:
            return None

        plans = await ctx.recall("mitigation_plans") or []
        plan = next((p for p in plans if p.get("plan_key") == plan_key), None)
        if not plan or plan.get("status") != "active":
            return None

        now = datetime.now(timezone.utc).isoformat()
        step_index = int(task.get("plan_step_index", plan.get("current_step", 0)) or 0)
        if result == "verified":
            plan["status"] = "resolved"
            plan["resolved_at"] = now
            plan["resolution_detail"] = detail[:300]
            plan["updated_at"] = now
        else:
            plan["failures"] = int(plan.get("failures", 0) or 0) + 1
            plan["current_step"] = max(int(plan.get("current_step", 0) or 0), step_index + 1)
            plan["updated_at"] = now
            plan["last_failure_detail"] = detail[:300]
            steps = plan.get("steps") or []
            if int(plan.get("failures", 0)) >= int(plan.get("max_failures", 3)) or int(plan.get("current_step", 0)) >= len(steps):
                plan["status"] = "escalated"
                plan["escalated_at"] = now
                plan["escalation_reason"] = detail[:300]

        await ctx.remember("mitigation_plans", plans[-20:])
        return plan

    def _new_verification_id(self, task: dict, outcome: dict) -> str:
        base = "|".join([
            task.get("type", "unknown"),
            task.get("title", ""),
            task.get("agent_id") or task.get("target_agent") or task.get("file_path", ""),
            outcome.get("result", "unknown"),
            datetime.now(timezone.utc).isoformat(),
        ])
        return hashlib.sha256(base.encode()).hexdigest()[:16]

    def _build_verification_record(self, task: dict, outcome: dict) -> Optional[dict]:
        """Create a deferred verification record for successful autonomous changes."""
        if outcome.get("result") != "success":
            return None

        now = datetime.now(timezone.utc)
        t_type = task.get("type")

        if t_type == "config_change":
            due_after = 20
            kind = "config_persistence"
            record = {
                "id": self._new_verification_id(task, outcome),
                "kind": kind,
                "status": "pending",
                "created_at": now.isoformat(),
                "due_at": (now + timedelta(minutes=due_after)).isoformat(),
                "task_type": t_type,
                "task_title": task.get("title", ""),
                "agent_id": task.get("agent_id"),
                "config_key": task.get("config_key"),
                "expected_value": outcome.get("new_value", task.get("config_value")),
                "old_value": outcome.get("old_value"),
                "risk_level": task.get("risk_level", "medium"),
                "strategy_context": task.get("strategy_context") or {},
                "reasoning": task.get("reasoning", "")[:400],
                "attempts": 0,
                "rollback_attempted": False,
            }
            if task.get("agent_id") == "watchdog" and task.get("config_key") == "latency_exclude_list":
                record["kind"] = "watchdog_latency_exclusion"
                record["finding_signature"] = task.get("finding_signature", "Agent p95 latency regression")
                record["exclude_agents"] = task.get("exclude_agents") or []
            return record

        if t_type == "source_patch":
            path = task.get("file_path", "")
            agent_match = re.search(r'agent_([\w_]+)\.py$', path)
            target_agent = task.get("agent_id")
            if not target_agent and agent_match:
                target_agent = agent_match.group(1).replace("_", "-")
            return {
                "id": self._new_verification_id(task, outcome),
                "kind": "agent_patch_health",
                "status": "pending",
                "created_at": now.isoformat(),
                "due_at": (now + timedelta(minutes=15)).isoformat(),
                "task_type": t_type,
                "task_title": task.get("title", ""),
                "target_agent": target_agent,
                "file_path": path,
                "old_code": task.get("old_code", ""),
                "new_code": task.get("new_code", ""),
                "risk_level": task.get("risk_level", "medium"),
                "strategy_context": task.get("strategy_context") or {},
                "reasoning": task.get("reasoning", "")[:400],
                "attempts": 0,
                "rollback_attempted": False,
            }

        if t_type == "schema_migration":
            return {
                "id": self._new_verification_id(task, outcome),
                "kind": "migration_applied",
                "status": "pending",
                "created_at": now.isoformat(),
                "due_at": (now + timedelta(minutes=10)).isoformat(),
                "task_type": t_type,
                "task_title": task.get("title", ""),
                "migration_name": task.get("migration_name"),
                "risk_level": task.get("risk_level", "medium"),
                "strategy_context": task.get("strategy_context") or {},
                "reasoning": task.get("reasoning", "")[:400],
                "attempts": 0,
                "rollback_attempted": False,
            }

        return None

    async def _evaluate_verification(self, ctx: RunContext, verification: dict) -> dict:
        """Evaluate whether a previously applied change actually held up."""
        kind = verification.get("kind")
        created_at = datetime.fromisoformat(verification["created_at"])
        now = datetime.now(timezone.utc)
        result = {
            **verification,
            "checked_at": now.isoformat(),
            "status": "verified",
            "result": "success",
            "detail": "Verification passed.",
        }

        if kind == "watchdog_latency_exclusion":
            current = await ctx.recall_for(verification.get("agent_id"), verification.get("config_key"), [])
            current = current or []
            expected_agents = set(verification.get("exclude_agents") or [])
            missing = sorted(expected_agents - set(current))

            rows = await ctx.query("""
                SELECT title, category, agent_id, created_at
                FROM ops.agent_findings
                WHERE agent_id = 'watchdog'
                  AND category = 'performance'
                  AND status = 'open'
                  AND created_at > $1
                ORDER BY created_at DESC
                LIMIT 50
            """, created_at)
            matching = [
                row for row in rows
                if self._finding_signature(row.get("title", ""), row.get("category", ""), row.get("agent_id", ""))
                == verification.get("finding_signature")
            ]

            if missing:
                result["status"] = "failed"
                result["result"] = "failed"
                result["detail"] = (
                    f"Watchdog config drifted: {verification.get('config_key')} is missing {missing}."
                )
            elif matching:
                result["status"] = "failed"
                result["result"] = "failed"
                result["matching_count"] = len(matching)
                result["detail"] = (
                    f"The normalized '{verification.get('finding_signature')}' alert still fired "
                    f"{len(matching)} time(s) after the mitigation was applied."
                )
                result["evidence"] = [row.get("title") for row in matching[:5]]
            else:
                result["detail"] = (
                    f"Watchdog retained the exclusion and no new '{verification.get('finding_signature')}' alerts "
                    f"appeared after {created_at.isoformat()}."
                )

        elif kind == "config_persistence":
            current = await ctx.recall_for(verification.get("agent_id"), verification.get("config_key"), None)
            if current != verification.get("expected_value"):
                result["status"] = "failed"
                result["result"] = "failed"
                result["detail"] = (
                    f"Config drift detected for {verification.get('agent_id')}.{verification.get('config_key')}."
                )
            else:
                result["detail"] = (
                    f"Config {verification.get('agent_id')}.{verification.get('config_key')} persisted as expected."
                )

        elif kind == "agent_patch_health":
            target_agent = verification.get("target_agent")
            if not target_agent:
                result["status"] = "failed"
                result["result"] = "failed"
                result["detail"] = "Could not determine target agent for patch verification."
            else:
                rows = await ctx.query("""
                    SELECT status, COUNT(*) as count
                    FROM ops.agent_runs
                    WHERE agent_id = $1
                      AND started_at > $2
                    GROUP BY status
                """, target_agent, created_at)
                counts = {row["status"]: int(row["count"]) for row in rows}
                successes = counts.get("success", 0)
                errors = counts.get("error", 0)
                if errors > 0:
                    result["status"] = "failed"
                    result["result"] = "failed"
                    result["detail"] = (
                        f"Agent {target_agent} still logged {errors} error run(s) after the patch."
                    )
                elif successes == 0:
                    result["status"] = "deferred"
                    result["result"] = "pending"
                    result["detail"] = (
                        f"Agent {target_agent} has not completed a post-patch success run yet."
                    )
                    result["due_at"] = (now + timedelta(minutes=10)).isoformat()
                else:
                    result["detail"] = (
                        f"Agent {target_agent} recorded {successes} successful run(s) and no errors after the patch."
                    )

        elif kind == "migration_applied":
            rows = await ctx.query(
                "SELECT migration_name, applied_by, applied_at FROM ops.migrations_applied WHERE migration_name = $1",
                verification.get("migration_name"),
            )
            if not rows:
                result["status"] = "failed"
                result["result"] = "failed"
                result["detail"] = (
                    f"Migration {verification.get('migration_name')} is missing from ops.migrations_applied."
                )
            else:
                result["detail"] = (
                    f"Migration {verification.get('migration_name')} remains recorded in ops.migrations_applied."
                )

        else:
            result["status"] = "failed"
            result["result"] = "failed"
            result["detail"] = f"Unknown verification kind: {kind}"

        return result

    async def _attempt_verification_rollback(self, ctx: RunContext, verification: dict) -> dict:
        """Attempt automatic rollback for low-risk verified regressions."""
        kind = verification.get("kind")
        risk = verification.get("risk_level", "medium")
        if risk != "low":
            return {"attempted": False, "success": False, "detail": f"Auto-rollback skipped for risk level: {risk}"}

        if verification.get("rollback_attempted"):
            return {"attempted": False, "success": False, "detail": "Auto-rollback already attempted."}

        if kind in {"config_persistence", "watchdog_latency_exclusion"}:
            agent_id = verification.get("agent_id")
            key = verification.get("config_key")
            old_value = verification.get("old_value")
            if not agent_id or not key:
                return {"attempted": False, "success": False, "detail": "Missing rollback target for config change."}

            await ctx.remember_for(agent_id, key, old_value)
            await ctx.action(f"Rolled back config: {agent_id}.{key}")
            return {
                "attempted": True,
                "success": True,
                "detail": f"Rolled back {agent_id}.{key} to the previous value.",
            }

        if kind == "agent_patch_health":
            path = verification.get("file_path")
            old_code = verification.get("old_code", "")
            new_code = verification.get("new_code", "")
            if not path or not old_code or not new_code:
                return {"attempted": False, "success": False, "detail": "Missing rollback data for source patch."}

            result = await ctx.patch_file(path, new_code, old_code)
            if not result.get("success"):
                return {
                    "attempted": True,
                    "success": False,
                    "detail": result.get("error", "Patch rollback failed."),
                }

            restart = await ctx.shell("systemctl restart wdws-agents", timeout=15)
            if not restart.get("success"):
                return {
                    "attempted": True,
                    "success": False,
                    "detail": f"Patch reverted but service restart failed: {restart.get('stderr', '')[:200]}",
                }

            await ctx.action(f"Rolled back source patch: {path}")
            return {
                "attempted": True,
                "success": True,
                "detail": f"Reverted patch in {path} and restarted wdws-agents.",
            }

        return {"attempted": False, "success": False, "detail": f"No auto-rollback rule for verification kind: {kind}"}

    async def _process_pending_verifications(self, ctx: RunContext, metrics: dict) -> list[dict]:
        """Run any deferred post-change verification checks that are due."""
        pending = await ctx.recall("pending_verifications") or []
        if not pending:
            return []

        now = datetime.now(timezone.utc)
        remaining = []
        outcomes = []
        history = await ctx.recall("verification_history") or []

        for item in pending:
            due_at = datetime.fromisoformat(item["due_at"])
            if due_at > now:
                remaining.append(item)
                continue

            checked = await self._evaluate_verification(ctx, item)
            checked["attempts"] = int(item.get("attempts", 0)) + 1
            metrics["verifications_run"] += 1

            if checked.get("status") == "verified":
                metrics["verifications_passed"] += 1
                await ctx.action(f"Verified: {checked.get('task_title', checked.get('kind', 'change'))}")
                await self._advance_mitigation_plan(ctx, checked, "verified", checked.get("detail", ""))
            elif checked.get("status") == "deferred" and checked["attempts"] < 3:
                remaining.append(checked)
                continue
            else:
                metrics["verifications_failed"] += 1
                rollback = await self._attempt_verification_rollback(ctx, checked)
                checked["rollback"] = rollback
                checked["rollback_attempted"] = rollback.get("attempted", False)
                if rollback.get("attempted"):
                    metrics["rollbacks_attempted"] += 1
                    if rollback.get("success"):
                        metrics["rollbacks_succeeded"] += 1
                    else:
                        metrics["rollbacks_failed"] += 1
                await self._record_strategy_outcome(ctx, checked, rollback)
                await self._remember_retry_guardrail(ctx, checked, rollback)
                plan = await self._advance_mitigation_plan(
                    ctx,
                    checked,
                    "failed",
                    (
                        checked.get("detail", "")
                        + (f" | Rollback: {rollback.get('detail')}" if rollback.get("attempted") else "")
                    ),
                )
                await ctx.finding(
                    "warning",
                    "athena-verification",
                    f"Post-change verification failed: {checked.get('task_title', checked.get('kind', 'change'))}",
                    (
                        checked.get("detail", "Verification failed.")
                        + (
                            f"\n\nRollback: {rollback.get('detail')}"
                            if rollback.get("attempted") else ""
                        )
                    ),
                    {
                        "verification": {
                            "kind": checked.get("kind"),
                            "id": checked.get("id"),
                            "status": checked.get("status"),
                            "attempts": checked.get("attempts"),
                        },
                        "rollback": rollback,
                        "plan": plan,
                        "evidence": checked.get("evidence"),
                    },
                )

                if plan and plan.get("status") == "escalated":
                    await ctx.finding(
                        "critical",
                        "athena-plan",
                        f"Mitigation plan escalated: {plan.get('title', plan.get('plan_key'))}",
                        plan.get("escalation_reason", "Mitigation plan exhausted its allowed tactics."),
                        {
                            "plan_key": plan.get("plan_key"),
                            "steps": plan.get("steps"),
                            "failures": plan.get("failures"),
                        },
                    )

            history.append({
                "time": checked.get("checked_at", now.isoformat()),
                "task": checked.get("task_title", checked.get("kind", "verification")),
                "kind": checked.get("kind"),
                "status": checked.get("status"),
                "detail": (
                    checked.get("detail", "")
                    + (f" | Rollback: {checked['rollback'].get('detail')}" if checked.get("rollback", {}).get("attempted") else "")
                )[:300],
            })
            outcomes.append({
                "task": f"verify: {checked.get('task_title', checked.get('kind', 'change'))}",
                "type": "verification",
                "result": (
                    "rolled_back"
                    if checked.get("rollback", {}).get("success")
                    else checked.get("result", checked.get("status", "unknown"))
                ),
                "detail": (
                    checked.get("detail", "")
                    + (f" | Rollback: {checked['rollback'].get('detail')}" if checked.get("rollback", {}).get("attempted") else "")
                ),
            })

        await ctx.remember("pending_verifications", remaining)
        await ctx.remember("verification_history", history[-40:])
        return outcomes

    def _derive_reflex_tasks(self, state: dict, memory: dict) -> list[dict]:
        """Produce deterministic low-risk tasks from learned patterns.

        Athena should not need an LLM round-trip to restore a previously learned
        watchdog mitigation when the same false positive pattern returns.
        """
        tasks = []
        repeated = state.get("repeated_findings") or []
        agent_health = state.get("agent_health") or []
        watchdog_cfg = state.get("watchdog_config") or {}
        current_exclude = set(watchdog_cfg.get("latency_exclude_list") or [])

        learned_fp = []
        for item in (memory.get("known_false_positives") or []):
            if item.get("agent_id") == "watchdog" and item.get("signature") == "Agent p95 latency regression":
                learned_fp.append(item)

        latency_repeat = next(
            (
                item for item in repeated
                if item.get("agent_id") == "watchdog"
                and item.get("signature") == "Agent p95 latency regression"
            ),
            None,
        )
        plan = self._get_mitigation_plan(memory, self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression"))
        current_step = (plan or {}).get("steps", ["exclude_heavy_agents"])
        active_step = current_step[int((plan or {}).get("current_step", 0) or 0)] if plan and plan.get("status") == "active" and int((plan or {}).get("current_step", 0) or 0) < len(current_step) else "exclude_heavy_agents"

        candidate_agents = set()
        for item in learned_fp:
            candidate_agents.update(item.get("exclude_agents") or [])

        if latency_repeat:
            for agent in agent_health:
                agent_id = agent.get("agent_id")
                if not agent_id or agent_id in current_exclude:
                    continue
                if agent_id in {"watchdog", "code-doctor", "orchestrator", "email-triage", "security-sentinel"}:
                    continue
                if int(agent.get("errors") or 0) > 0:
                    continue

                p95_ms = float(agent.get("p95_ms") or 0)
                avg_ms = float(agent.get("avg_ms") or 0)
                successes = int(agent.get("successes") or 0)
                if p95_ms >= 30000 or (avg_ms >= 20000 and successes <= 18):
                    candidate_agents.add(agent_id)

        missing_agents = sorted(agent for agent in candidate_agents if agent not in current_exclude)
        if missing_agents and active_step == "exclude_heavy_agents":
            new_exclude = sorted(current_exclude | set(missing_agents))
            occurrences = latency_repeat.get("occurrences", 0) if latency_repeat else 0
            candidate_task = {
                "type": "config_change",
                "priority": 1,
                "title": "Restore watchdog latency exclusions for heavy scheduled agents",
                "confidence": 0.98,
                "risk_level": "low",
                "reasoning": (
                    "Watchdog is repeatedly firing the normalized 'Agent p95 latency regression' alert, "
                    f"and heavy agents {missing_agents} are not currently excluded. "
                    f"This pattern has appeared {occurrences} times recently and matches a known false positive: "
                    "slow scheduled agents skew fleet-wide p95 baselines without indicating a real platform regression."
                ),
                "agent_id": "watchdog",
                "config_key": "latency_exclude_list",
                "config_value": new_exclude,
                "finding_signature": "Agent p95 latency regression",
                "finding_category": "performance",
                "exclude_agents": missing_agents,
                "strategy_context": {
                    "finding_signature": "Agent p95 latency regression",
                    "occurrences_bucket": "high" if occurrences >= 6 else "medium",
                    "window_min": int(state.get("watchdog_config", {}).get("latency_regression_window_min") or 15),
                    "min_agent_runs": int(state.get("watchdog_config", {}).get("latency_regression_min_agent_runs") or 5),
                    "candidate_count": len(missing_agents),
                },
                "plan_key": self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression"),
                "plan_step": "exclude_heavy_agents",
                "plan_step_index": 0,
            }
            guardrail = self._get_retry_guardrail(candidate_task, memory, state)
            if not guardrail:
                tasks.append(candidate_task)

        return tasks

    def _derive_alternative_tasks(self, state: dict, memory: dict) -> list[dict]:
        """Choose a safer next-best mitigation when a prior strategy is guardrailed."""
        tasks = []
        repeated = state.get("repeated_findings") or []
        watchdog_cfg = state.get("watchdog_config") or {}

        latency_repeat = next(
            (
                item for item in repeated
                if item.get("agent_id") == "watchdog"
                and item.get("signature") == "Agent p95 latency regression"
            ),
            None,
        )
        if not latency_repeat:
            return tasks

        guarded_exclusion = self._get_retry_guardrail({
            "type": "config_change",
            "title": "Restore watchdog latency exclusions for heavy scheduled agents",
            "agent_id": "watchdog",
            "config_key": "latency_exclude_list",
            "finding_signature": "Agent p95 latency regression",
            "exclude_agents": ["athena"],
        }, memory, state)
        plan = self._get_mitigation_plan(memory, self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression"))
        steps = (plan or {}).get("steps", ["exclude_heavy_agents", "broaden_window", "raise_min_runs"])
        active_index = int((plan or {}).get("current_step", 0) or 0)
        active_step = steps[active_index] if plan and plan.get("status") == "active" and active_index < len(steps) else None
        if not guarded_exclusion and active_step not in {"broaden_window", "raise_min_runs"}:
            return tasks

        current_window = int(watchdog_cfg.get("latency_regression_window_min") or 15)
        current_min_runs = int(watchdog_cfg.get("latency_regression_min_agent_runs") or 5)
        occurrences = int(latency_repeat.get("occurrences") or 0)

        if current_window < 30 and active_step in {None, "broaden_window"}:
            window_task = {
                "type": "config_change",
                "priority": 2,
                "title": "Broaden watchdog latency regression comparison window",
                "confidence": 0.94,
                "risk_level": "low",
                "reasoning": (
                    "The direct exclusion strategy is currently blocked by a retry guardrail, but the normalized "
                    f"'Agent p95 latency regression' alert is still repeating ({occurrences} occurrences). "
                    "A longer comparison window is a safer fallback because it reduces short-window volatility "
                    "without disabling detection."
                ),
                "agent_id": "watchdog",
                "config_key": "latency_regression_window_min",
                "config_value": 30,
                "finding_signature": "Agent p95 latency regression",
                "finding_category": "performance",
                "strategy_context": {
                    "finding_signature": "Agent p95 latency regression",
                    "occurrences_bucket": "high" if occurrences >= 6 else "medium",
                    "window_min": current_window,
                    "min_agent_runs": current_min_runs,
                    "alternative_reason": "exclude_guardrailed",
                },
                "plan_key": self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression"),
                "plan_step": "broaden_window",
                "plan_step_index": 1,
            }
            if not self._get_retry_guardrail(window_task, memory, state):
                tasks.append(window_task)

        if current_min_runs < 12 and active_step in {None, "raise_min_runs"}:
            sample_task = {
                "type": "config_change",
                "priority": 3,
                "title": "Raise watchdog minimum agent runs for latency regression checks",
                "confidence": 0.93,
                "risk_level": "low",
                "reasoning": (
                    "The exclusion-based mitigation is guardrailed, and the latency regression alert remains noisy. "
                    "Requiring more successful agent runs before comparing p95 is a safer fallback that reduces false "
                    "positives from sparse scheduled workloads while preserving true regression detection."
                ),
                "agent_id": "watchdog",
                "config_key": "latency_regression_min_agent_runs",
                "config_value": 12,
                "finding_signature": "Agent p95 latency regression",
                "finding_category": "performance",
                "strategy_context": {
                    "finding_signature": "Agent p95 latency regression",
                    "occurrences_bucket": "high" if occurrences >= 6 else "medium",
                    "window_min": current_window,
                    "min_agent_runs": current_min_runs,
                    "alternative_reason": "exclude_guardrailed",
                },
                "plan_key": self._mitigation_plan_key("watchdog_latency", "Agent p95 latency regression"),
                "plan_step": "raise_min_runs",
                "plan_step_index": 2,
            }
            if not self._get_retry_guardrail(sample_task, memory, state):
                tasks.append(sample_task)

        tasks.sort(key=lambda task: self._strategy_sort_key(task, memory))
        return tasks

    # ═══════════════════════════════════════════════════════════
    #  Main run loop
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        t_start = time.time()
        metrics = {
            "decisions_made": 0,
            "actions_taken": 0,
            "configs_updated": 0,
            "migrations_applied": 0,
            "patches_applied": 0,
            "approvals_queued": 0,
            "delegations_sent": 0,
            "verifications_run": 0,
            "verifications_passed": 0,
            "verifications_failed": 0,
            "rollbacks_attempted": 0,
            "rollbacks_succeeded": 0,
            "rollbacks_failed": 0,
            "tasks_suppressed": 0,
            "cycle": datetime.now(timezone.utc).isoformat(),
        }

        # ── 1. Load full platform state ──────────────────────
        state = await self._load_platform_state(ctx)

        # ── 2. Load Athena's accumulated memory ─────────────
        memory = await self._load_memory(ctx)

        # ── 2a. Sync mitigation plans from live repeated findings ──
        await self._sync_mitigation_plans(ctx, state, memory)
        memory = await self._load_memory(ctx)

        # ── 2b. Verify prior autonomous changes ─────────────
        verification_outcomes = await self._process_pending_verifications(ctx, metrics)
        if verification_outcomes:
            memory = await self._load_memory(ctx)

        # ── 2c. Deterministic reflexes from prior learning ──
        reflex_tasks = self._derive_reflex_tasks(state, memory)
        alternative_tasks = self._derive_alternative_tasks(state, memory)

        # ── 3. Ask LLM: what needs attention? ───────────────
        plan = await self._reason(ctx, state, memory)
        planned_tasks = self._merge_tasks(reflex_tasks, alternative_tasks, (plan or {}).get("tasks", []))
        if not planned_tasks and not verification_outcomes:
            await ctx.remember("last_cycle_summary", {
                "time": metrics["cycle"],
                "tasks": 0,
                "state": "nothing to do",
            })
            return {"summary": "Athena: nothing actionable this cycle", "metrics": metrics}

        # ── 4. Execute task plan ─────────────────────────────
        outcomes = list(verification_outcomes)
        for task in planned_tasks:
            outcome = await self._execute_task(ctx, task, metrics)
            outcomes.append(outcome)

        # ── 5. Record cycle in memory ────────────────────────
        await ctx.remember("last_cycle_summary", {
            "time": metrics["cycle"],
            "tasks": len(outcomes),
            "actions_taken": metrics["actions_taken"],
            "approvals_queued": metrics["approvals_queued"],
            "reflex_tasks": len(reflex_tasks),
            "alternative_tasks": len(alternative_tasks),
            "llm_tasks": len((plan or {}).get("tasks", [])),
            "verifications_run": metrics["verifications_run"],
            "rollbacks_attempted": metrics["rollbacks_attempted"],
            "tasks_suppressed": metrics["tasks_suppressed"],
            "outcomes": [{"task": o["task"], "result": o["result"]} for o in outcomes[-10:]],
        })

        # Update long-running outcome history (last 50 outcomes)
        history = await ctx.recall("outcome_history") or []
        history = (history + outcomes)[-50:]
        await ctx.remember("outcome_history", [
            {"task": o["task"], "result": o["result"],
             "time": metrics["cycle"], "type": o.get("type")}
            for o in history
        ])

        # ── 6. Morning brief (6 AM only) ─────────────────────
        hour = datetime.now(timezone.utc).hour
        last_brief = await ctx.recall("last_brief_date") or ""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if hour == 6 and last_brief != today:
            await self._send_morning_brief(ctx, state, outcomes, metrics)
            await ctx.remember("last_brief_date", today)

        # ── 7. Check pending approvals for responses ─────────
        await self._process_approvals(ctx, metrics)

        duration_ms = int((time.time() - t_start) * 1000)
        summary = (
            f"Athena: {metrics['decisions_made']} decisions, "
            f"{metrics['actions_taken']} actions, "
            f"{metrics['approvals_queued']} queued for approval "
            f"({duration_ms}ms)"
        )
        return {"summary": summary, "metrics": metrics}

    # ═══════════════════════════════════════════════════════════
    #  State loading
    # ═══════════════════════════════════════════════════════════
    async def _load_platform_state(self, ctx: RunContext) -> dict:
        """Gather raw platform data — everything Athena needs to reason."""
        state = {}

        # Open findings (last 24h)
        state["open_findings"] = await ctx.query("""
            SELECT agent_id, severity, category, title, detail,
                   evidence::text, created_at
            FROM ops.agent_findings
            WHERE status = 'open'
              AND created_at > now() - interval '24 hours'
            ORDER BY
                CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                created_at DESC
            LIMIT 40
        """)

        # Repeated findings — potential false positives or systemic issues
        repeated_rows = await ctx.query("""
            SELECT agent_id, severity, category, title, detail,
               evidence::text, created_at
            FROM ops.agent_findings
            WHERE status = 'open'
              AND created_at > now() - interval '48 hours'
            ORDER BY created_at DESC
            LIMIT 250
        """)
        state["repeated_findings"] = self._summarize_repeated_findings(repeated_rows)

        # Recent agent run health
        state["agent_health"] = await ctx.query("""
            SELECT agent_id,
                   COUNT(*) FILTER (WHERE status='success') as successes,
                   COUNT(*) FILTER (WHERE status='error')   as errors,
                   MAX(started_at) as last_run,
                   ROUND(AVG(duration_ms)) as avg_ms,
                   ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)) as p95_ms
            FROM ops.agent_runs
            WHERE started_at > now() - interval '2 hours'
            GROUP BY agent_id
            ORDER BY errors DESC, agent_id
        """)

        # Recent agent errors (with tracebacks)
        state["recent_errors"] = await ctx.query("""
            SELECT agent_id, error, started_at, duration_ms
            FROM ops.agent_runs
            WHERE status = 'error'
              AND started_at > now() - interval '4 hours'
            ORDER BY started_at DESC
            LIMIT 15
        """)

        # Pending human approvals (so Athena doesn't re-queue what's already waiting)
        state["pending_approvals"] = await ctx.query("""
            SELECT id, decision_type, title, risk_level, confidence,
                   created_at, expires_at
            FROM ops.pending_approvals
            WHERE approved IS NULL
            ORDER BY created_at DESC
            LIMIT 20
        """)

        # Recent Athena decisions and their outcomes
        state["recent_decisions"] = await ctx.query("""
            SELECT decision_type, target, description,
                   confidence, outcome, outcome_detail, created_at
            FROM ops.athena_decisions
            WHERE created_at > now() - interval '24 hours'
            ORDER BY created_at DESC
            LIMIT 20
        """)

        # System metrics snapshot (from latest watchdog run)
        sysmetrics = await ctx.query("""
            SELECT metrics
            FROM ops.agent_runs
            WHERE agent_id = 'watchdog' AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
        """)
        state["system_metrics"] = (
            sysmetrics[0]["metrics"] if sysmetrics else {}
        )

        # DB size trend
        state["db_sizes"] = await ctx.query("""
            SELECT schemaname, tablename,
                   pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                   pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
            FROM pg_tables
            WHERE schemaname IN ('ops','core','legal','medical')
            ORDER BY size_bytes DESC
            LIMIT 20
        """)

        # Watchdog's current runtime config (what Athena may have changed)
        state["watchdog_config"] = {
            "latency_exclude_list": await ctx.recall_for("watchdog", "latency_exclude_list"),
            "latency_regression_window_min": await ctx.recall_for("watchdog", "latency_regression_window_min"),
            "latency_regression_min_agent_runs": await ctx.recall_for("watchdog", "latency_regression_min_agent_runs"),
            "latency_regression_ratio": await ctx.recall_for("watchdog", "latency_regression_ratio"),
            "latency_regression_delta_ms": await ctx.recall_for("watchdog", "latency_regression_delta_ms"),
            "latency_regression_human_p95_ms": await ctx.recall_for("watchdog", "latency_regression_human_p95_ms"),
        }

        return state

    # ═══════════════════════════════════════════════════════════
    #  Memory loading
    # ═══════════════════════════════════════════════════════════
    async def _load_memory(self, ctx: RunContext) -> dict:
        """Load Athena's accumulated knowledge."""
        return {
            "last_cycle": await ctx.recall("last_cycle_summary"),
            "outcome_history": (await ctx.recall("outcome_history") or [])[-20:],
            "known_false_positives": await ctx.recall("known_false_positives") or [],
            "failed_attempts": await ctx.recall("failed_attempts") or [],
            "config_changes": await ctx.recall("config_changes") or [],
            "reasoning_notes": (await ctx.recall("reasoning_notes") or [])[-20:],
            "pending_verifications": await ctx.recall("pending_verifications") or [],
            "verification_history": (await ctx.recall("verification_history") or [])[-20:],
            "retry_guardrails": (await ctx.recall("retry_guardrails") or [])[-20:],
            "strategy_scores": await ctx.recall("strategy_scores") or {},
            "strategy_notes": (await ctx.recall("strategy_notes") or [])[-20:],
            "mitigation_plans": (await ctx.recall("mitigation_plans") or [])[-10:],
        }

    # ═══════════════════════════════════════════════════════════
    #  Core reasoning
    # ═══════════════════════════════════════════════════════════
    async def _reason(self, ctx: RunContext, state: dict, memory: dict) -> Optional[dict]:
        """Ask the LLM to analyze platform state and produce an action plan."""

        # Serialize state compactly
        def _fmt(data, limit=30):
            if isinstance(data, list):
                return json.dumps(data[:limit], default=str, indent=None)
            return json.dumps(data, default=str, indent=None)

        prompt = f"""You are Athena, analyzing the Athena Cognitive Platform. Current time: {datetime.now(timezone.utc).isoformat()}

## OPEN FINDINGS (last 24h)
{_fmt(state['open_findings'])}

## REPEATED FINDINGS (≥3 times in 48h — potential systemic issues)
{_fmt(state['repeated_findings'])}

## AGENT HEALTH (last 2h)
{_fmt(state['agent_health'])}

## RECENT ERRORS
{_fmt(state['recent_errors'])}

## SYSTEM METRICS (latest watchdog snapshot)
{_fmt(state['system_metrics'])}

## CURRENT WATCHDOG CONFIG (what you've set)
{_fmt(state['watchdog_config'])}

## PENDING APPROVALS (already queued — don't re-queue)
{_fmt(state['pending_approvals'])}

## YOUR RECENT DECISIONS + OUTCOMES
{_fmt(state['recent_decisions'])}

## YOUR ACCUMULATED MEMORY
Last cycle: {json.dumps(memory.get('last_cycle'), default=str)}
Known false positives: {json.dumps(memory.get('known_false_positives'))}
Failed attempts (don't repeat): {json.dumps(memory.get('failed_attempts'))}
Recent config changes: {json.dumps(memory.get('config_changes'))}
Recent reasoning notes: {json.dumps(memory.get('reasoning_notes'), default=str)}
Pending verifications: {json.dumps(memory.get('pending_verifications'), default=str)}
Recent verification history: {json.dumps(memory.get('verification_history'), default=str)}
Retry guardrails (do not repeat unless evidence changes): {json.dumps(memory.get('retry_guardrails'), default=str)}
Strategy scores (prefer what has verified successfully): {json.dumps(memory.get('strategy_scores'), default=str)}
Strategy notes (what worked under which conditions): {json.dumps(memory.get('strategy_notes'), default=str)}
Active mitigation plans: {json.dumps(memory.get('mitigation_plans'), default=str)}

---
Analyze this state. Produce a prioritized list of tasks to execute this cycle.

For each task be specific and actionable. Include ALL context needed to execute it.

Return JSON:
{{
  "analysis": "2-3 sentence summary of platform state",
  "tasks": [
    {{
      "type": "config_change|schema_migration|source_patch|delegation|no_action",
      "priority": 1-5,
      "title": "short description",
      "confidence": 0.0-1.0,
      "risk_level": "low|medium|high|critical",
      "reasoning": "why this is needed",

      // For config_change:
      "agent_id": "watchdog",
      "config_key": "latency_exclude_list",
      "config_value": [...],

      // For schema_migration:
      "migration_name": "012_add_case_outcome_column",
      "sql_forward": "ALTER TABLE legal.cases ADD COLUMN ...",
      "sql_rollback": "ALTER TABLE legal.cases DROP COLUMN ...",

      // For source_patch:
      "file_path": "/opt/wdws/agents/agent_watchdog.py",
      "old_code": "exact text to replace",
      "new_code": "replacement text",

      // For delegation:
      "target_agent": "dba",
      "message": "message to send via @mention"
    }}
  ]
}}

Only include tasks that are genuinely needed. If nothing is wrong, return {{"analysis": "...", "tasks": []}}.
Avoid repeating tasks from recent_decisions that already have outcome='success'.
Never queue something already in pending_approvals.
If a normalized repeated finding matches a known false positive and the mitigating config is missing,
prefer a low-risk config_change with high confidence over delegation or no_action.
Do not repeat mitigations that appear in retry_guardrails unless the evidence has materially changed."""

        try:
            result = await codex_json(
                self.instructions,
                prompt,
                effort="high",
            )
            return result
        except Exception as e:
            self.log.error("Reasoning failed: %s", e)
            await ctx.finding("warning", "athena-self",
                "Athena reasoning cycle failed",
                str(e))
            return None

    # ═══════════════════════════════════════════════════════════
    #  Task execution
    # ═══════════════════════════════════════════════════════════
    async def _execute_task(self, ctx: RunContext, task: dict, metrics: dict) -> dict:
        t_type    = task.get("type", "no_action")
        title     = task.get("title", "unnamed task")
        confidence = float(task.get("confidence", 0.5))
        risk      = task.get("risk_level", "medium")
        reasoning = task.get("reasoning", "")

        metrics["decisions_made"] += 1
        outcome = {"task": title, "type": t_type, "result": "skipped", "detail": ""}

        memory = await self._load_memory(ctx)
        state = await self._load_platform_state(ctx)
        guardrail = self._get_retry_guardrail(task, memory, state)
        if guardrail and t_type != "no_action":
            metrics["tasks_suppressed"] += 1
            outcome["result"] = "suppressed"
            outcome["detail"] = (
                "Suppressed by retry guardrail — this action previously failed verification or was rolled back. "
                "Fresh evidence is required before retrying."
            )
            await self._record_decision(ctx, task, outcome)
            await self._learn_from_outcome(ctx, task, outcome)
            return outcome

        # Low-confidence or high-risk: queue for approval
        needs_approval = (
            confidence < 0.70
            or (confidence < 0.85 and risk in ("high", "critical"))
            or risk == "critical"
        )

        if t_type == "config_change":
            if needs_approval:
                await self._queue_approval(ctx, task, metrics)
                outcome["result"] = "queued_for_approval"
            else:
                outcome = await self._apply_config_change(ctx, task, metrics)

        elif t_type == "schema_migration":
            outcome = await self._apply_migration(ctx, task, metrics, needs_approval)

        elif t_type == "source_patch":
            if needs_approval or risk in ("high", "critical"):
                await self._queue_approval(ctx, task, metrics)
                outcome["result"] = "queued_for_approval"
            else:
                outcome = await self._apply_source_patch(ctx, task, metrics)

        elif t_type == "delegation":
            outcome = await self._delegate(ctx, task, metrics)

        elif t_type == "no_action":
            outcome["result"] = "no_action"
            outcome["detail"] = task.get("reasoning", "")

        # Record every decision
        await self._record_decision(ctx, task, outcome)
        await self._learn_from_outcome(ctx, task, outcome)
        return outcome

    # ═══════════════════════════════════════════════════════════
    #  Config change
    # ═══════════════════════════════════════════════════════════
    async def _apply_config_change(self, ctx: RunContext, task: dict, metrics: dict) -> dict:
        agent_id   = task.get("agent_id")
        key        = task.get("config_key")
        value      = task.get("config_value")
        title      = task.get("title", f"{agent_id}.{key}")

        if not agent_id or not key:
            return {"task": title, "type": "config_change",
                    "result": "failed", "detail": "Missing agent_id or config_key"}

        try:
            # Save old value for rollback
            old_val = await ctx.recall_for(agent_id, key)

            await ctx.remember_for(agent_id, key, value)
            metrics["actions_taken"] += 1
            metrics["configs_updated"] += 1

            # Track change in Athena's own memory
            changes = await ctx.recall("config_changes") or []
            changes = (changes + [{
                "time": datetime.now(timezone.utc).isoformat(),
                "agent": agent_id, "key": key,
                "old": old_val, "new": value,
                "reason": task.get("reasoning", ""),
            }])[-30:]
            await ctx.remember("config_changes", changes)

            await ctx.action(f"Config update: {agent_id}.{key} → {json.dumps(value)[:100]}")
            return {"task": title, "type": "config_change",
                    "result": "success", "detail": f"{agent_id}.{key} updated",
                    "old_value": old_val, "new_value": value}
        except Exception as e:
            return {"task": title, "type": "config_change",
                    "result": "failed", "detail": str(e)}

    # ═══════════════════════════════════════════════════════════
    #  Schema migration
    # ═══════════════════════════════════════════════════════════
    async def _apply_migration(self, ctx: RunContext, task: dict,
                                metrics: dict, needs_approval: bool) -> dict:
        name      = task.get("migration_name", "")
        sql_fwd   = task.get("sql_forward", "")
        sql_rb    = task.get("sql_rollback") or _generate_rollback(sql_fwd)
        title     = task.get("title", name)

        if not name or not sql_fwd:
            return {"task": title, "type": "schema_migration",
                    "result": "failed", "detail": "Missing migration_name or sql_forward"}

        # Check if already applied
        already = await ctx.query(
            "SELECT id FROM ops.migrations_applied WHERE migration_name = $1", name)
        if already:
            return {"task": title, "type": "schema_migration",
                    "result": "skipped", "detail": "Already applied"}

        # Destructive check — always needs approval
        if not _is_additive(sql_fwd) or needs_approval:
            await self._queue_approval(ctx, task, metrics, sql_preview=sql_fwd, rollback_sql=sql_rb)
            return {"task": title, "type": "schema_migration",
                    "result": "queued_for_approval", "detail": "Destructive or low-confidence — sent for approval"}

        # Dry-run first
        dry_run = await ctx.shell(
            f"""PGPASSWORD=NEL2233obs psql -U wdws -h 127.0.0.1 -d wdws -c """
            f"""'BEGIN; {sql_fwd.replace("'", "''")}; ROLLBACK;' 2>&1""",
            timeout=30,
        )
        if not dry_run["success"] or "ERROR" in dry_run.get("stdout", ""):
            detail = dry_run.get("stdout", "") + dry_run.get("stderr", "")
            await ctx.finding("warning", "athena-migration",
                f"Migration dry-run failed: {name}", detail[:500])
            # Track failed attempt so we don't repeat
            failed = await ctx.recall("failed_attempts") or []
            failed = (failed + [{"type": "migration", "name": name,
                                  "error": detail[:200],
                                  "time": datetime.now(timezone.utc).isoformat()}])[-20:]
            await ctx.remember("failed_attempts", failed)
            return {"task": title, "type": "schema_migration",
                    "result": "failed", "detail": f"Dry-run error: {detail[:300]}"}

        # Write migration file
        mig_path = f"/opt/wdws/migrations/{name}.sql"
        header = textwrap.dedent(f"""\
            -- ============================================================
            -- Migration: {name}
            -- Applied by: Athena (autonomous)
            -- Applied at: {datetime.now(timezone.utc).isoformat()}
            -- Reasoning: {task.get('reasoning', 'N/A')[:200]}
            -- Rollback: see ops.migrations_applied
            -- ============================================================
        """)
        await ctx.shell(f"cat > {mig_path} << 'MIGEOF'\n{header}\n{sql_fwd}\nMIGEOF", timeout=10)

        # Apply
        t0 = time.time()
        result = await ctx.shell(
            f"PGPASSWORD=NEL2233obs psql -U wdws -h 127.0.0.1 -d wdws "
            f"-f {mig_path} 2>&1",
            timeout=60,
        )
        duration_ms = int((time.time() - t0) * 1000)

        if not result["success"] or "ERROR" in result.get("stdout", ""):
            err = result.get("stdout", "") + result.get("stderr", "")
            await ctx.finding("warning", "athena-migration",
                f"Migration failed to apply: {name}", err[:500])
            return {"task": title, "type": "schema_migration",
                    "result": "failed", "detail": err[:300]}

        # Record in audit table
        await ctx.execute("""
            INSERT INTO ops.migrations_applied
                (migration_name, sql_forward, sql_rollback, applied_by,
                 duration_ms, analysis, confidence)
            VALUES ($1, $2, $3, 'athena', $4, $5, $6)
            ON CONFLICT (migration_name) DO NOTHING
        """, name, sql_fwd, sql_rb, duration_ms,
            task.get("reasoning", ""), float(task.get("confidence", 0.85)))

        metrics["actions_taken"] += 1
        metrics["migrations_applied"] += 1
        await ctx.action(f"Migration applied: {name} ({duration_ms}ms)")
        return {"task": title, "type": "schema_migration",
                "result": "success", "detail": f"Applied in {duration_ms}ms"}

    # ═══════════════════════════════════════════════════════════
    #  Source patch
    # ═══════════════════════════════════════════════════════════
    async def _apply_source_patch(self, ctx: RunContext, task: dict, metrics: dict) -> dict:
        path     = task.get("file_path", "")
        old_code = task.get("old_code", "")
        new_code = task.get("new_code", "")
        title    = task.get("title", f"patch {path}")

        if not path or not old_code or not new_code:
            return {"task": title, "type": "source_patch",
                    "result": "failed", "detail": "Missing file_path, old_code, or new_code"}

        # Only allow patching within the agents directory
        if not path.startswith(_AGENT_FILES_ROOT):
            return {"task": title, "type": "source_patch",
                    "result": "failed", "detail": f"Path outside allowed root: {path}"}

        result = await ctx.patch_file(path, old_code, new_code)

        if not result.get("success"):
            return {"task": title, "type": "source_patch",
                    "result": "failed", "detail": result.get("error", "unknown")}

        # Restart the service if we patched a running agent
        restart = await ctx.shell("systemctl restart wdws-agents", timeout=15)
        if not restart["success"]:
            await ctx.finding("warning", "athena-patch",
                f"Patch applied to {path} but service restart failed",
                restart.get("stderr", ""))

        metrics["actions_taken"] += 1
        metrics["patches_applied"] += 1
        await ctx.action(f"Source patch applied: {path}")
        return {"task": title, "type": "source_patch",
                "result": "success", "detail": f"Patched {path}, service restarted"}

    # ═══════════════════════════════════════════════════════════
    #  Delegation
    # ═══════════════════════════════════════════════════════════
    async def _delegate(self, ctx: RunContext, task: dict, metrics: dict) -> dict:
        target  = task.get("target_agent", "")
        message = task.get("message", "")
        title   = task.get("title", f"delegate to {target}")

        if not target or not message:
            return {"task": title, "type": "delegation",
                    "result": "failed", "detail": "Missing target_agent or message"}

        agent_obj = getattr(ctx, "agent", None)
        ask_agent = getattr(agent_obj, "ask_agent", None) if agent_obj else None

        if callable(ask_agent):
            try:
                await ask_agent(target, message, channel="ops")
                metrics["actions_taken"] += 1
                metrics["delegations_sent"] += 1
                return {"task": title, "type": "delegation",
                        "result": "success", "detail": f"Delegated to @{target}"}
            except AttributeError:
                pass

        message_agent = getattr(ctx, "message_agent", None)
        notify_agent = getattr(ctx, "notify_agent", None)

        if callable(message_agent):
            await message_agent(target, message, channel="ops")
            metrics["actions_taken"] += 1
            metrics["delegations_sent"] += 1
            return {"task": title, "type": "delegation",
                    "result": "success", "detail": f"Delegated to @{target}"}
        if callable(notify_agent):
            await notify_agent(target, message, channel="ops")
            metrics["actions_taken"] += 1
            metrics["delegations_sent"] += 1
            return {"task": title, "type": "delegation",
                    "result": "success", "detail": f"Delegated to @{target}"}

        await ctx.finding(
            "warning",
            "athena-delegation",
            "Delegation failed: no delegation method available",
            f"Unable to delegate to @{target}: RunContext lacks delegation helpers",
        )
        return {"task": title, "type": "delegation",
                "result": "failed", "detail": "No delegation method available"}

    # ═══════════════════════════════════════════════════════════
    #  Human approval queue
    # ═══════════════════════════════════════════════════════════
    async def _queue_approval(self, ctx: RunContext, task: dict, metrics: dict,
                               sql_preview: str = None, rollback_sql: str = None):
        title    = task.get("title", "Athena proposed action")
        desc     = task.get("reasoning", "")
        risk     = task.get("risk_level", "medium")
        conf     = float(task.get("confidence", 0.5))
        expires  = datetime.now(timezone.utc) + timedelta(hours=48)

        # Don't re-queue if identical title is already pending
        existing = await ctx.query("""
            SELECT id FROM ops.pending_approvals
            WHERE title = $1 AND approved IS NULL
        """, title)
        if existing:
            return

        await ctx.execute("""
            INSERT INTO ops.pending_approvals
                (decision_type, title, description, proposed_action,
                 sql_preview, rollback_sql, reasoning, confidence,
                 risk_level, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
            task.get("type", "unknown"),
            title,
            desc[:500],
            json.dumps(task),
            sql_preview,
            rollback_sql,
            desc[:2000],
            conf,
            risk,
            expires,
        )
        metrics["approvals_queued"] += 1
        await ctx.action(f"Queued for approval: {title}")

    # ═══════════════════════════════════════════════════════════
    #  Process approved/rejected items
    # ═══════════════════════════════════════════════════════════
    async def _process_approvals(self, ctx: RunContext, metrics: dict):
        """Execute any items the human has approved since last cycle."""
        approved = await ctx.query("""
            SELECT id, decision_type, title, proposed_action,
                   sql_preview, rollback_sql, approved_at
            FROM ops.pending_approvals
            WHERE approved = true
              AND approved_at > now() - interval '10 minutes'
        """)

        for item in approved:
            task = item["proposed_action"]
            if isinstance(task, str):
                try:
                    task = json.loads(task)
                except Exception:
                    task = {}

            task_type = item["decision_type"]
            outcome_detail = ""
            try:
                if task_type == "schema_migration" and item.get("sql_preview"):
                    task["sql_forward"] = item["sql_preview"]
                    task["sql_rollback"] = item.get("rollback_sql", "")
                    task["migration_name"] = task.get("migration_name", f"approved_{item['id']}")
                    result = await self._apply_migration(ctx, task, metrics, needs_approval=False)
                    outcome_detail = result.get("detail", "")
                elif task_type == "config_change":
                    result = await self._apply_config_change(ctx, task, metrics)
                    outcome_detail = result.get("detail", "")
                elif task_type == "source_patch":
                    result = await self._apply_source_patch(ctx, task, metrics)
                    outcome_detail = result.get("detail", "")
                else:
                    outcome_detail = f"Unhandled approved type: {task_type}"

                await ctx.execute("""
                    UPDATE ops.pending_approvals
                    SET approved = true  -- mark fully processed
                    WHERE id = $1
                """, item["id"])

            except Exception as e:
                outcome_detail = str(e)
                await ctx.finding("warning", "athena-approval",
                    f"Failed to execute approved item: {item['title']}", str(e))

            self.log.info("Processed approved item #%d: %s → %s",
                         item["id"], item["title"], outcome_detail)

    # ═══════════════════════════════════════════════════════════
    #  Decision recording
    # ═══════════════════════════════════════════════════════════
    async def _record_decision(self, ctx: RunContext, task: dict, outcome: dict):
        try:
            await ctx.execute("""
                INSERT INTO ops.athena_decisions
                    (run_id, decision_type, target, description,
                     reasoning, confidence, action_taken, outcome, outcome_detail)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                ctx.agent._run_id,
                task.get("type", "unknown"),
                task.get("agent_id") or task.get("target_agent") or task.get("file_path", ""),
                task.get("title", "")[:500],
                task.get("reasoning", "")[:2000],
                float(task.get("confidence", 0.5)),
                json.dumps(task),
                outcome.get("result", "unknown"),
                outcome.get("detail", "")[:500],
            )
        except Exception as e:
            self.log.warning("Failed to record decision: %s", e)

    async def _learn_from_outcome(self, ctx: RunContext, task: dict, outcome: dict):
        """Persist useful lessons so Athena can react without rediscovering them."""
        now = datetime.now(timezone.utc).isoformat()
        notes = await ctx.recall("reasoning_notes") or []
        note = {
            "time": now,
            "task": task.get("title", ""),
            "type": task.get("type", "unknown"),
            "outcome": outcome.get("result", "unknown"),
            "detail": (outcome.get("detail") or "")[:240],
        }

        if outcome.get("result") == "failed":
            failed = await ctx.recall("failed_attempts") or []
            failed = (failed + [{
                "type": task.get("type", "unknown"),
                "name": task.get("title", ""),
                "error": (outcome.get("detail") or "")[:200],
                "time": now,
            }])[-20:]
            await ctx.remember("failed_attempts", failed)
            note["lesson"] = "This action failed recently; avoid blindly retrying without new evidence."
        elif outcome.get("result") == "suppressed":
            note["lesson"] = "Retry suppressed by guardrail because the same mitigation recently failed verification or was rolled back."

        if (
            task.get("type") == "config_change"
            and outcome.get("result") == "success"
            and task.get("agent_id") == "watchdog"
            and task.get("config_key") == "latency_exclude_list"
        ):
            signature = task.get("finding_signature") or "Agent p95 latency regression"
            learned = await ctx.recall("known_false_positives") or []
            exclude_agents = task.get("exclude_agents") or []
            if not exclude_agents:
                old_value = set(outcome.get("old_value") or [])
                new_value = set(outcome.get("new_value") or task.get("config_value") or [])
                exclude_agents = sorted(new_value - old_value)

            merged = False
            for item in learned:
                if (
                    item.get("signature") == signature
                    and item.get("agent_id") == "watchdog"
                    and item.get("config_key") == "latency_exclude_list"
                ):
                    item["exclude_agents"] = sorted(set(item.get("exclude_agents") or []) | set(exclude_agents))
                    item["learned_at"] = now
                    item["reason"] = task.get("reasoning", item.get("reason", ""))[:300]
                    merged = True
                    break

            if not merged:
                learned.append({
                    "signature": signature,
                    "agent_id": "watchdog",
                    "category": task.get("finding_category", "performance"),
                    "config_key": "latency_exclude_list",
                    "exclude_agents": sorted(exclude_agents),
                    "reason": task.get("reasoning", "")[:300],
                    "learned_at": now,
                })

            await ctx.remember("known_false_positives", learned[-20:])
            note["lesson"] = (
                f"Learned that {signature} is a false positive when heavy agents "
                f"{sorted(exclude_agents)} are included in the watchdog latency baseline."
            )

        elif outcome.get("result") == "success":
            note["lesson"] = "Successful action recorded for future reasoning."

        verification = self._build_verification_record(task, outcome)
        if verification:
            pending = await ctx.recall("pending_verifications") or []
            pending = (pending + [verification])[-40:]
            await ctx.remember("pending_verifications", pending)
            note["verification"] = {
                "kind": verification.get("kind"),
                "due_at": verification.get("due_at"),
            }

        notes = (notes + [note])[-40:]
        await ctx.remember("reasoning_notes", notes)

    # ═══════════════════════════════════════════════════════════
    #  Morning brief
    # ═══════════════════════════════════════════════════════════
    async def _send_morning_brief(self, ctx: RunContext, state: dict,
                                   outcomes: list, metrics: dict):
        """Queue the daily morning brief for inclusion in the daily digest."""
        try:
            brief_prompt = f"""Produce a concise morning brief for the human owner of the Athena platform.

PLATFORM STATE:
Open findings: {json.dumps(state['open_findings'][:10], default=str)}
Agent health: {json.dumps(state['agent_health'], default=str)}
System metrics: {json.dumps(state['system_metrics'], default=str)}
Pending human approvals: {json.dumps(state['pending_approvals'], default=str)}

THIS CYCLE'S OUTCOMES:
{json.dumps([{"task": o["task"], "result": o["result"]} for o in outcomes], default=str)}

Today's metrics: {json.dumps(metrics, default=str)}

Return JSON:
{{
  "subject": "Athena Morning Brief — [DATE]",
  "sections": [
    {{
      "heading": "What I Did",
      "content": "...",
      "type": "info"
    }},
    {{
      "heading": "Issues Requiring Your Decision",
      "content": "...",
      "type": "warning"
    }},
    {{
      "heading": "System Health",
      "content": "...",
      "type": "info"
    }},
    {{
      "heading": "What I'll Do Next",
      "content": "...",
      "type": "info"
    }}
  ]
}}"""

            brief = await codex_json(self.instructions, brief_prompt, effort="medium")
            if not brief or not brief.get("sections"):
                return

            subject = brief.get("subject", "Athena Morning Brief")
            await ctx.queue_notification(subject, brief["sections"], severity="info")
            await ctx.action("Queued morning brief for daily digest")
            self.log.info("Morning brief queued for daily digest")

        except Exception as e:
            self.log.warning("Morning brief failed: %s", e)
            await ctx.finding("warning", "athena-self",
                "Morning brief failed to queue", str(e))
