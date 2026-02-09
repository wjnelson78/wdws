"""
WDWS Agent Framework — Base classes and infrastructure
═══════════════════════════════════════════════════════
Every agent inherits from BaseAgent which provides:
  - Database connection pooling
  - LLM integration (OpenAI)
  - Structured logging to ops.agent_runs / ops.agent_findings
  - Inter-agent messaging
  - Self-registration
"""
import asyncio
import json
import re
import logging
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import httpx
from openai import AsyncOpenAI

from config import (
    DATABASE_URL, OPENAI_API_KEY, OPENAI_MODEL, LOG_LEVEL
)
from reliability import retry_with_backoff, get_circuit_breaker, CircuitBreakerOpenError

log = logging.getLogger("agents")

# ═══════════════════════════════════════════════════════════════
#  Shared DB pool
# ═══════════════════════════════════════════════════════════════
_pool: Optional[asyncpg.Pool] = None

async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None or _pool._closed:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool

async def close_pool():
    global _pool
    if _pool and not _pool._closed:
        await _pool.close()

# ═══════════════════════════════════════════════════════════════
#  LLM Helper
# ═══════════════════════════════════════════════════════════════
_llm: Optional[AsyncOpenAI] = None

def get_llm() -> AsyncOpenAI:
    global _llm
    if _llm is None:
        _llm = AsyncOpenAI(api_key=OPENAI_API_KEY)
    return _llm

def _is_reasoning_model(model: str) -> bool:
    """Check if model is an o-series reasoning model (o1, o3, etc.)."""
    return bool(model and re.match(r"^o[0-9]", model))

def _is_codex_model(model: str) -> bool:
    """Check if model uses the Responses API (gpt-5.x-codex variants)."""
    return bool(model and "codex" in model.lower())

@retry_with_backoff(max_attempts=3, initial_delay=1.0, exceptions=(Exception,))
async def llm_chat(system: str, user: str, model: str = None,
                   temperature: float = 0.3, max_tokens: int = 2000) -> str:
    """Quick LLM call with system + user prompt. Returns assistant text.
    Includes retry logic and circuit breaker for reliability."""
    breaker = get_circuit_breaker("openai-api", failure_threshold=5, timeout_seconds=60)
    
    try:
        async with breaker:
            client = get_llm()
            mdl = model or OPENAI_MODEL
            kwargs = dict(
                model=mdl,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            if _is_reasoning_model(mdl):
                kwargs["max_completion_tokens"] = max_tokens
            else:
                kwargs["temperature"] = temperature
                kwargs["max_tokens"] = max_tokens
            resp = await client.chat.completions.create(**kwargs)
            return resp.choices[0].message.content.strip()
    except CircuitBreakerOpenError as e:
        log.warning(f"OpenAI circuit breaker open: {e}")
        return f"[AI temporarily unavailable - circuit breaker open. Using fallback response.]"

@retry_with_backoff(max_attempts=3, initial_delay=1.0, exceptions=(Exception,))
async def llm_json(system: str, user: str, model: str = None) -> dict:
    """LLM call that returns parsed JSON. Includes retry logic and circuit breaker."""
    breaker = get_circuit_breaker("openai-api", failure_threshold=5, timeout_seconds=60)
    
    try:
        async with breaker:
            client = get_llm()
            mdl = model or OPENAI_MODEL
            # Reasoning models (o1, o3, etc.) don't support response_format or max_tokens
            if _is_reasoning_model(mdl):
                json_prompt = user + "\n\nIMPORTANT: Respond with valid JSON only. No markdown, no code fences."
                resp = await client.chat.completions.create(
                    model=mdl,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": json_prompt},
                    ],
                    max_completion_tokens=4000,
                )
            else:
                resp = await client.chat.completions.create(
                    model=mdl,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=0.1,
                    max_tokens=4000,
                    response_format={"type": "json_object"},
                )
            text = resp.choices[0].message.content.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = re.sub(r"^```(?:json)?\n?", "", text)
                text = re.sub(r"\n?```$", "", text)
            return json.loads(text)
    except CircuitBreakerOpenError as e:
        log.warning(f"OpenAI circuit breaker open: {e}")
        return {"error": "AI temporarily unavailable", "fallback": True}



# ── Responses API helpers (GPT-5.2-Codex) ─────────────────────
def _get_sync_client():
    """Get a sync OpenAI client for Responses API."""
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)

async def codex_generate(system: str, user: str, model: str = None,
                         effort: str = "high",
                         max_tokens: int = 4000) -> str:
    """Call GPT-5.2-Codex via the Responses API with reasoning effort.
    Returns the model's output text.
    """
    import asyncio
    mdl = model or "gpt-5.2-codex"
    client = _get_sync_client()

    def _call():
        return client.responses.create(
            model=mdl,
            instructions=system,
            input=user,
            reasoning={"effort": effort},
            max_output_tokens=max_tokens,
        )

    resp = await asyncio.get_event_loop().run_in_executor(None, _call)
    return resp.output_text.strip()

async def codex_json(system: str, user: str, model: str = None,
                     effort: str = "high") -> dict:
    """Call GPT-5.2-Codex via Responses API and parse the result as JSON."""
    json_instruction = system + "\nYou MUST respond with valid JSON only. No markdown fences, no commentary."
    text = await codex_generate(json_instruction, user, model=model, effort=effort,
                                 max_tokens=8000)
    # Strip markdown code fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return json.loads(text)


# ═══════════════════════════════════════════════════════════════
#  BaseAgent
# ═══════════════════════════════════════════════════════════════
class BaseAgent(ABC):
    """
    Abstract base for all WDWS agents.

    Subclasses must implement:
      - agent_id: str           — unique identifier
      - agent_name: str         — human-readable name
      - description: str        — what this agent does
      - schedule: str           — cron expression
      - instructions: str       — detailed LLM system prompt
      - async run(ctx) -> dict  — the agent's main logic
    """
    agent_id: str = "base"
    agent_name: str = "Base Agent"
    description: str = ""
    version: str = "1.0.0"
    schedule: str = "*/5 * * * *"
    priority: int = 5
    capabilities: list = []

    # The detailed instructions for this agent — used as LLM system prompt
    instructions: str = "You are a helpful AI agent."

    def __init__(self):
        self.log = logging.getLogger(f"agent.{self.agent_id}")
        self._run_id: Optional[int] = None
        self._findings_count = 0
        self._actions_count = 0

    # ── Registration ─────────────────────────────────────────
    async def register(self):
        """Register or update this agent in ops.agent_registry."""
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.agent_registry (id, name, description, capabilities, agent_type,
                                            schedule, priority, version, is_active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                capabilities = EXCLUDED.capabilities,
                schedule = EXCLUDED.schedule,
                priority = EXCLUDED.priority,
                version = EXCLUDED.version,
                updated_at = now()
        """, self.agent_id, self.agent_name, self.description,
            json.dumps(self.capabilities), "autonomous",
            self.schedule, self.priority, self.version)
        self.log.info("Registered: %s v%s", self.agent_name, self.version)

    # ── Run lifecycle ────────────────────────────────────────
    async def execute(self, trigger: str = "schedule") -> dict:
        """Full execution lifecycle: start → run → finish → log."""
        p = await get_pool()
        t0 = time.time()
        self._findings_count = 0
        self._actions_count = 0

        # Create run record
        self._run_id = await p.fetchval("""
            INSERT INTO ops.agent_runs (agent_id, trigger)
            VALUES ($1, $2) RETURNING id
        """, self.agent_id, trigger)

        self.log.info("▶ Run #%d started (trigger=%s)", self._run_id, trigger)

        try:
            # Process incoming mentions and messages before main run
            try:
                inbox_count = await self._process_inbox()
                if inbox_count > 0:
                    self.log.info("Processed %d inbox items", inbox_count)
            except Exception as e:
                self.log.warning("Inbox processing error (non-fatal): %s", e)

            result = await self.run(RunContext(self))
            duration_ms = int((time.time() - t0) * 1000)
            summary = result.get("summary", "Completed")

            await p.execute("""
                UPDATE ops.agent_runs SET
                    ended_at = now(), status = 'success', summary = $2,
                    findings = $3, actions = $4, metrics = $5, duration_ms = $6
                WHERE id = $1
            """, self._run_id, summary, self._findings_count,
                self._actions_count, json.dumps(result.get("metrics", {})),
                duration_ms)

            await p.execute("""
                UPDATE ops.agent_registry SET
                    last_run_at = now(), last_status = 'success',
                    run_count = COALESCE(run_count, 0) + 1
                WHERE id = $1
            """, self.agent_id)

            self.log.info("✓ Run #%d complete (%dms) — %d findings, %d actions",
                         self._run_id, duration_ms, self._findings_count, self._actions_count)
            return result

        except Exception as e:
            duration_ms = int((time.time() - t0) * 1000)
            tb = traceback.format_exc()
            self.log.error("✗ Run #%d failed: %s", self._run_id, e)

            await p.execute("""
                UPDATE ops.agent_runs SET
                    ended_at = now(), status = 'error', error = $2, duration_ms = $3
                WHERE id = $1
            """, self._run_id, f"{e}\n{tb}", duration_ms)

            await p.execute("""
                UPDATE ops.agent_registry SET
                    last_run_at = now(), last_status = 'error',
                    run_count = COALESCE(run_count, 0) + 1,
                    error_count = COALESCE(error_count, 0) + 1
                WHERE id = $1
            """, self.agent_id)

            return {"summary": f"Error: {e}", "error": str(e)}

    @abstractmethod
    async def run(self, ctx: "RunContext") -> dict:
        """
        Main agent logic. Must return a dict with at least:
          {"summary": "What happened", "metrics": {...}}
        Use ctx.finding() to report issues, ctx.action() to log actions,
        ctx.ask_llm() for AI reasoning, ctx.db() for database access.
        """
        ...

    # ── Fast Inbox Execution (mention wake-up) ───────────────
    async def execute_inbox(self, trigger: str = "mention") -> dict:
        """
        Lightweight execution that ONLY processes inbox (mentions + messages).
        Used by the notification watcher to wake agents instantly when they
        are @mentioned in chat — no full run() cycle needed.
        """
        p = await get_pool()
        t0 = __import__('time').time()
        self._findings_count = 0
        self._actions_count = 0

        # Create run record
        self._run_id = await p.fetchval("""
            INSERT INTO ops.agent_runs (agent_id, trigger)
            VALUES ($1, $2) RETURNING id
        """, self.agent_id, trigger)

        self.log.info("\u26a1 Run #%d started (trigger=%s) — inbox only",
                      self._run_id, trigger)

        try:
            inbox_count = await self._process_inbox()
            duration_ms = int((__import__('time').time() - t0) * 1000)

            await p.execute("""
                UPDATE ops.agent_runs SET
                    ended_at = now(), status = 'success',
                    summary = $2, findings = $3, actions = $4,
                    metrics = $5, duration_ms = $6
                WHERE id = $1
            """, self._run_id,
                f"Inbox wake-up: processed {inbox_count} items",
                self._findings_count, self._actions_count,
                __import__('json').dumps({"inbox_items": inbox_count}),
                duration_ms)

            await p.execute("""
                UPDATE ops.agent_registry SET
                    last_run_at = now(), last_status = 'success',
                    run_count = COALESCE(run_count, 0) + 1
                WHERE id = $1
            """, self.agent_id)

            self.log.info("\u26a1 Run #%d inbox done (%dms) — %d items",
                         self._run_id, duration_ms, inbox_count)
            return {"summary": f"Inbox: {inbox_count} items", "inbox": inbox_count}

        except Exception as e:
            duration_ms = int((__import__('time').time() - t0) * 1000)
            self.log.error("\u2717 Inbox run #%d failed: %s", self._run_id, e)
            await p.execute("""
                UPDATE ops.agent_runs SET
                    ended_at = now(), status = 'error', error = $2, duration_ms = $3
                WHERE id = $1
            """, self._run_id, str(e), duration_ms)
            return {"summary": f"Inbox error: {e}", "error": str(e)}


    # ── Inbox Processing ─────────────────────────────────────
    async def _process_inbox(self):
        """Check for @mentions and direct messages, respond using LLM."""
        p = await get_pool()

        # 1. Check chat mentions we haven't replied to yet
        mentions = await p.fetch("""
            SELECT c.id, c.channel_id, c.from_agent, c.content,
                   c.msg_type, c.created_at
            FROM ops.agent_chat c
            WHERE $1 = ANY(c.mentions)
              AND c.msg_type IN ('question', 'chat')
              AND NOT EXISTS (
                  SELECT 1 FROM ops.agent_chat r
                  WHERE r.reply_to = c.id AND r.from_agent = $1
              )
            ORDER BY c.created_at DESC
            LIMIT 5
        """, self.agent_id)

        for m in mentions:
            try:
                question = m["content"].replace(f"@{self.agent_id}", "").strip()
                if not question:
                    continue

                # Use LLM to generate a response
                response = await llm_chat(
                    self.instructions,
                    f"You received this message from agent '{m['from_agent']}' "
                    f"in channel #{m['channel_id']}:\n\n{question}\n\n"
                    f"Respond concisely as the {self.agent_name} agent. "
                    f"If you cannot help, say so and suggest which agent might.",
                    max_tokens=500
                )

                # Reply in chat
                await self.reply_chat(
                    reply_to=m["id"],
                    content=response,
                    channel=m["channel_id"],
                    msg_type="answer"
                )

                # Log to agent_logs for Agent Comms visibility
                await p.execute("""
                    INSERT INTO ops.agent_logs
                        (agent_id, run_id, level, category, message, context)
                    VALUES ($1, $2, 'info', 'chat-reply', $3, $4)
                """, self.agent_id, self._run_id,
                    f"Replied to @mention from {m['from_agent']}: {response[:200]}",
                    json.dumps({"question": question[:500],
                               "from": m["from_agent"],
                               "channel": m["channel_id"],
                               "reply_to": m["id"]}))

                self.log.info("Replied to @%s in #%s",
                             m["from_agent"], m["channel_id"])

            except Exception as e:
                self.log.warning("Failed to reply to mention %d: %s", m["id"], e)

        # 2. Check direct messages (agent_messages)
        messages = await self.get_messages(limit=10)
        for msg in messages:
            try:
                body_data = msg.get("body", {})
                if isinstance(body_data, str):
                    try:
                        body_data = json.loads(body_data)
                    except Exception:
                        body_data = {"raw": body_data}

                # Generate response
                response = await llm_chat(
                    self.instructions,
                    f"You received a direct message from agent '{msg['from_agent']}':\n"
                    f"Type: {msg['msg_type']}\n"
                    f"Subject: {msg.get('subject', 'N/A')}\n"
                    f"Body: {json.dumps(body_data)}\n\n"
                    f"Respond concisely with what action you will take.",
                    max_tokens=500
                )

                # Post response to chat so it's visible
                await self.chat(
                    f"Received {msg['msg_type']} from @{msg['from_agent']}: "
                    f"{msg.get('subject', '')}\n\n{response}",
                    channel="general",
                    mentions=[msg["from_agent"]],
                    msg_type="answer"
                )

                # Log for Agent Comms
                await p.execute("""
                    INSERT INTO ops.agent_logs
                        (agent_id, run_id, level, category, message, context)
                    VALUES ($1, $2, 'info', 'message-response', $3, $4)
                """, self.agent_id, self._run_id,
                    f"Processed message from {msg['from_agent']}: {msg.get('subject', '')}",
                    json.dumps({"from": msg["from_agent"],
                               "msg_type": msg["msg_type"],
                               "subject": msg.get("subject", ""),
                               "response": response[:500]}))

            except Exception as e:
                self.log.warning("Failed to process message %s: %s",
                                msg.get("id"), e)

        # 3. Check for unanswered human questions — ALL agents respond to general queries
        human_questions = []

        # Find general human questions (no @mentions) that THIS agent hasn't replied to
        human_questions = await p.fetch("""
            SELECT c.id, c.channel_id, c.from_agent, c.content,
                   c.msg_type, c.created_at
            FROM ops.agent_chat c
            WHERE c.from_agent LIKE 'human%'
              AND c.mentions = '{}'
              AND c.created_at > now() - interval '2 hours'
              AND NOT EXISTS (
                  SELECT 1 FROM ops.agent_chat r
                  WHERE r.reply_to = c.id AND r.from_agent = $1
              )
            ORDER BY c.created_at ASC
            LIMIT 3
        """, self.agent_id)

        for hq in human_questions:
            try:
                question = hq["content"]

                # Each agent responds from its own domain perspective
                response = await llm_chat(
                    self.instructions,
                    f"A human team member ({hq['from_agent']}) asked a general question "
                    f"to ALL agents in #{hq['channel_id']}:\n\n{question}\n\n"
                    f"Respond concisely from YOUR domain as the {self.agent_name} agent. "
                    f"Provide a brief status or answer relevant to your area of expertise. "
                    f"If the question is completely outside your domain, respond with a "
                    f"one-line status summary for your area instead. "
                    f"Keep your response to 2-4 sentences maximum.",
                    max_tokens=300
                )

                await self.reply_chat(
                    reply_to=hq["id"],
                    content=response,
                    channel=hq["channel_id"],
                    msg_type="answer"
                )

                self.log.info("Responded to general human question %d", hq["id"])

            except Exception as e:
                self.log.warning("Failed to respond to human question %d: %s", hq["id"], e)

        return len(mentions) + len(messages) + len(human_questions)

    # ── Messaging ────────────────────────────────────────────
    async def send_message(self, to_agent: str, msg_type: str,
                           subject: str, body: dict = None, priority: int = 5):
        """Send a message to another agent (or '*' for broadcast)."""
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.agent_messages
                (from_agent, to_agent, msg_type, subject, body, priority)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, self.agent_id, to_agent, msg_type, subject,
            json.dumps(body or {}), priority)

    async def get_messages(self, limit: int = 20) -> list:
        """Get pending messages for this agent."""
        p = await get_pool()
        rows = await p.fetch("""
            SELECT id, from_agent, msg_type, subject, body, priority, created_at
            FROM ops.agent_messages
            WHERE (to_agent = $1 OR to_agent = '*')
              AND status = 'pending'
            ORDER BY priority ASC, created_at ASC
            LIMIT $2
        """, self.agent_id, limit)

        # Mark as read
        if rows:
            ids = [r["id"] for r in rows]
            await p.execute("""
                UPDATE ops.agent_messages SET status = 'read', processed_at = now()
                WHERE id = ANY($1::bigint[])
            """, ids)

        return [dict(r) for r in rows]

    # ── Chat Room ────────────────────────────────────────────
    async def chat(self, content: str, channel: str = "general",
                   mentions: list = None, reply_to: int = None,
                   msg_type: str = "chat", metadata: dict = None) -> int:
        """Post a message to the agent chat room."""
        if mentions is None:
            mentions = re.findall(r'@([\w-]+)', content)
        p = await get_pool()
        row = await p.fetchrow("""
            INSERT INTO ops.agent_chat
                (channel_id, from_agent, content, mentions, reply_to, msg_type, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
        """, channel, self.agent_id, content,
            mentions, reply_to, msg_type,
            json.dumps(metadata or {}))
        return row["id"]

    async def broadcast(self, content: str, channel: str = "general",
                        msg_type: str = "status", metadata: dict = None) -> int:
        """Broadcast a message to all agents in a channel."""
        return await self.chat(content, channel=channel, msg_type=msg_type, metadata=metadata)

    async def ask_agent(self, target_agent: str, question: str,
                        channel: str = "general", metadata: dict = None) -> int:
        """Ask a specific agent a question via @mention."""
        content = f"@{target_agent} {question}"
        return await self.chat(content, channel=channel,
                               mentions=[target_agent], msg_type="question",
                               metadata=metadata)

    async def reply_chat(self, reply_to: int, content: str,
                         channel: str = "general", msg_type: str = "answer",
                         metadata: dict = None, mentions: list = None) -> int:
        """Reply to a specific chat message (threading)."""
        return await self.chat(content, channel=channel, reply_to=reply_to,
                               msg_type=msg_type, metadata=metadata, mentions=mentions)

    async def get_chat_mentions(self, limit: int = 20) -> list:
        """Get recent chat messages that @mention this agent."""
        p = await get_pool()
        rows = await p.fetch("""
            SELECT id, channel_id, from_agent, content, mentions,
                   reply_to, msg_type, metadata, created_at
            FROM ops.agent_chat
            WHERE $1 = ANY(mentions)
            ORDER BY created_at DESC
            LIMIT $2
        """, self.agent_id, limit)
        return [dict(r) for r in rows]

    async def get_channel_history(self, channel: str = "general",
                                   limit: int = 50, since_id: int = 0) -> list:
        """Get recent messages from a channel."""
        p = await get_pool()
        rows = await p.fetch("""
            SELECT id, channel_id, from_agent, content, mentions,
                   reply_to, msg_type, metadata, created_at
            FROM ops.agent_chat
            WHERE channel_id = $1 AND id > $2
            ORDER BY created_at DESC
            LIMIT $3
        """, channel, since_id, limit)
        return [dict(r) for r in rows]




# ═══════════════════════════════════════════════════════════════
#  RunContext — passed to agent.run()
# ═══════════════════════════════════════════════════════════════
class RunContext:
    """
    Context object for a single agent run.
    Provides helpers for DB access, LLM calls, findings, and actions.
    """
    def __init__(self, agent: BaseAgent):
        self.agent = agent
        self.log = agent.log

    async def db(self) -> asyncpg.Pool:
        """Get the shared DB pool."""
        return await get_pool()

    async def ask_llm(self, user_prompt: str, system_prompt: str = None,
                      temperature: float = 0.3, max_tokens: int = 2000) -> str:
        """Ask the LLM using the agent's instructions as system prompt."""
        sys = system_prompt or self.agent.instructions
        return await llm_chat(sys, user_prompt, temperature=temperature,
                             max_tokens=max_tokens)

    async def ask_llm_json(self, user_prompt: str, system_prompt: str = None) -> dict:
        """Ask the LLM and get parsed JSON back."""
        sys = system_prompt or self.agent.instructions
        return await llm_json(sys, user_prompt)

    async def finding(self, severity: str, category: str, title: str,
                      detail: str = "", evidence: dict = None):
        """Report a finding (issue, recommendation, alert)."""
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.agent_findings
                (agent_id, run_id, severity, category, title, detail, evidence)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, self.agent.agent_id, self.agent._run_id,
            severity, category, title, detail, json.dumps(evidence or {}))
        self.agent._findings_count += 1
        self.log.info("📋 Finding [%s/%s]: %s", severity, category, title)

    async def action(self, description: str):
        """Log an action taken by the agent."""
        self.agent._actions_count += 1
        self.log.info("⚡ Action: %s", description)


    async def log_comms(self, level: str, category: str, message: str,
                        context: dict = None):
        """Write to ops.agent_logs for the Agent Comms audit trail.
        Levels: info, reasoning, error, warn, email, code-fix
        """
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.agent_logs (agent_id, run_id, level, category, message, context)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, self.agent.agent_id, self.agent._run_id, level, category,
            message[:2000], json.dumps(context or {}))

    async def query(self, sql: str, *args) -> list:
        """Execute a read query and return list of dicts."""
        p = await get_pool()
        rows = await p.fetch(sql, *args)
        return [dict(r) for r in rows]

    async def execute(self, sql: str, *args):
        """Execute a write query."""
        p = await get_pool()
        await p.execute(sql, *args)

    async def http_get(self, url: str, **kwargs) -> httpx.Response:
        """Make an HTTP GET request."""
        async with httpx.AsyncClient(timeout=30) as c:
            return await c.get(url, **kwargs)

    # ── Persistent Memory ────────────────────────────────────
    async def remember(self, key: str, value) -> None:
        """Store a value in persistent agent memory (survives restarts)."""
        p = await get_pool()
        await p.execute("""
            INSERT INTO ops.agent_memory (agent_id, key, value, updated_at)
            VALUES ($1, $2, $3, now())
            ON CONFLICT (agent_id, key) DO UPDATE SET
                value = $3, updated_at = now()
        """, self.agent.agent_id, key,
            json.dumps(value))

    async def recall(self, key: str, default=None):
        """Retrieve a value from persistent agent memory."""
        p = await get_pool()
        row = await p.fetchrow("""
            SELECT value FROM ops.agent_memory
            WHERE agent_id = $1 AND key = $2
        """, self.agent.agent_id, key)
        if row is None:
            return default
        val = row["value"]
        if val is None:
            return default
        # asyncpg may return JSONB as string — parse if needed
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return val
        return val

    async def recall_all(self) -> dict:
        """Retrieve all memory for this agent."""
        p = await get_pool()
        rows = await p.fetch("""
            SELECT key, value FROM ops.agent_memory
            WHERE agent_id = $1
        """, self.agent.agent_id)
        return {r["key"]: r["value"] for r in rows}

    async def forget(self, key: str) -> None:
        """Delete a key from persistent memory."""
        p = await get_pool()
        await p.execute("""
            DELETE FROM ops.agent_memory WHERE agent_id = $1 AND key = $2
        """, self.agent.agent_id, key)

    # ── HTTP helpers ─────────────────────────────────────────
    async def http_post(self, url: str, **kwargs):
        """Make an HTTP POST request."""
        async with httpx.AsyncClient(timeout=30) as c:
            return await c.post(url, **kwargs)

    async def http_request(self, method: str, url: str, **kwargs):
        """Make an arbitrary HTTP request."""
        async with httpx.AsyncClient(timeout=30) as c:
            return await c.request(method, url, **kwargs)
