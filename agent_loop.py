"""
Athena Cognitive Engine — Async Agent Loop

Minimal async agent loop with tool registration, execution, and result
injection back into LLM context. Designed to integrate with the existing
agent framework at /opt/wdws/agents/framework.py.

Features:
  - Tool registry with type-safe definitions
  - Async tool execution with configurable timeout
  - Automatic result injection into conversation context
  - Conversation memory management with token budgeting
  - Multi-step reasoning (observe → think → act → observe loop)
  - Integrates with ModelRouter for provider selection

Usage:
    loop = AgentLoop(name="legal-assistant")
    loop.register_tool(semantic_search_tool)
    loop.register_tool(get_document_tool)

    result = await loop.run(
        "Find all filings related to the Nelson case",
        system_prompt="You are a legal research assistant.",
    )
"""

import asyncio
import inspect
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

log = logging.getLogger("athena.agent_loop")

# Maximum number of tool-call iterations before forcing a final answer
MAX_ITERATIONS = 10
# Default timeout for individual tool execution (seconds)
TOOL_TIMEOUT = 30
# Approximate token budget for conversation context
MAX_CONTEXT_TOKENS = 100_000
# Rough chars-per-token estimate for budgeting
CHARS_PER_TOKEN = 4


@dataclass
class ToolDefinition:
    """Describes a tool available to the agent."""
    name: str
    description: str
    parameters: dict[str, Any]  # JSON Schema for parameters
    handler: Callable           # async callable((**kwargs) -> str|dict)
    timeout: int = TOOL_TIMEOUT
    requires_confirmation: bool = False


@dataclass
class ToolCall:
    """A tool invocation requested by the LLM."""
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class ToolResult:
    """Result of executing a tool."""
    tool_call_id: str
    name: str
    content: str
    success: bool = True
    latency_ms: int = 0


@dataclass
class AgentStep:
    """One step in the agent loop."""
    iteration: int
    reasoning: str
    tool_calls: list[ToolCall]
    tool_results: list[ToolResult]
    timestamp: float = field(default_factory=time.time)


@dataclass
class AgentResult:
    """Final result of an agent loop run."""
    answer: str
    steps: list[AgentStep]
    total_iterations: int
    total_latency_ms: int
    tokens_used: int = 0
    model_used: str = ""


class ToolRegistry:
    """Registry of tools available to agents."""

    def __init__(self):
        self._tools: dict[str, ToolDefinition] = {}

    def register(
        self,
        name: str,
        description: str,
        parameters: dict[str, Any],
        handler: Callable,
        timeout: int = TOOL_TIMEOUT,
        requires_confirmation: bool = False,
    ) -> None:
        """Register a tool."""
        if not asyncio.iscoroutinefunction(handler):
            # Wrap sync handler in async
            sync_fn = handler
            async def async_handler(**kwargs):
                return sync_fn(**kwargs)
            handler = async_handler

        self._tools[name] = ToolDefinition(
            name=name,
            description=description,
            parameters=parameters,
            handler=handler,
            timeout=timeout,
            requires_confirmation=requires_confirmation,
        )
        log.info("Registered tool: %s", name)

    def get(self, name: str) -> Optional[ToolDefinition]:
        return self._tools.get(name)

    def list_tools(self) -> list[ToolDefinition]:
        return list(self._tools.values())

    def to_openai_tools(self) -> list[dict]:
        """Convert registered tools to OpenAI function-calling format."""
        return [
            {
                "type": "function",
                "function": {
                    "name": t.name,
                    "description": t.description,
                    "parameters": t.parameters,
                },
            }
            for t in self._tools.values()
        ]

    def to_anthropic_tools(self) -> list[dict]:
        """Convert registered tools to Anthropic tool-use format."""
        return [
            {
                "name": t.name,
                "description": t.description,
                "input_schema": t.parameters,
            }
            for t in self._tools.values()
        ]


class ConversationMemory:
    """Manages conversation context within token budget."""

    def __init__(self, max_tokens: int = MAX_CONTEXT_TOKENS):
        self.max_tokens = max_tokens
        self.system_prompt: str = ""
        self.messages: list[dict[str, Any]] = []

    def set_system(self, prompt: str):
        self.system_prompt = prompt

    def add_user(self, content: str):
        self.messages.append({"role": "user", "content": content})

    def add_assistant(self, content: str, tool_calls: list[dict] | None = None):
        msg: dict[str, Any] = {"role": "assistant", "content": content}
        if tool_calls:
            msg["tool_calls"] = tool_calls
        self.messages.append(msg)

    def add_tool_result(self, tool_call_id: str, name: str, content: str):
        self.messages.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "name": name,
            "content": content,
        })

    def get_messages(self) -> list[dict]:
        """Get messages within token budget, trimming oldest if needed."""
        all_msgs = []
        if self.system_prompt:
            all_msgs.append({"role": "system", "content": self.system_prompt})

        # Estimate total tokens
        total_chars = len(self.system_prompt)
        for m in self.messages:
            total_chars += len(str(m.get("content", "")))

        if total_chars / CHARS_PER_TOKEN <= self.max_tokens:
            return all_msgs + self.messages

        # Trim: keep system + first user message + last N messages
        budget_chars = self.max_tokens * CHARS_PER_TOKEN
        budget_chars -= len(self.system_prompt)

        # Always keep the first user message (the original query)
        first_user = None
        for m in self.messages:
            if m["role"] == "user":
                first_user = m
                break

        kept: list[dict] = []
        if first_user:
            kept.append(first_user)
            budget_chars -= len(str(first_user.get("content", "")))

        # Fill from the end
        tail: list[dict] = []
        for m in reversed(self.messages):
            if m is first_user:
                continue
            msg_chars = len(str(m.get("content", "")))
            if budget_chars - msg_chars < 0:
                break
            tail.insert(0, m)
            budget_chars -= msg_chars

        # Insert truncation marker if we dropped messages
        if len(tail) + (1 if first_user else 0) < len(self.messages):
            kept.append({
                "role": "system",
                "content": f"[{len(self.messages) - len(tail) - 1} earlier messages omitted for context budget]",
            })

        kept.extend(tail)
        return all_msgs + kept


class AgentLoop:
    """Async agent loop with tool execution and multi-step reasoning."""

    def __init__(
        self,
        name: str = "athena-agent",
        max_iterations: int = MAX_ITERATIONS,
        model_router=None,
    ):
        self.name = name
        self.max_iterations = max_iterations
        self.tools = ToolRegistry()
        self._router = model_router  # Optional ModelRouter instance
        self._llm_call: Optional[Callable] = None

    def register_tool(
        self,
        name: str,
        description: str,
        parameters: dict[str, Any],
        handler: Callable,
        **kwargs,
    ):
        """Register a tool for the agent to use."""
        self.tools.register(name, description, parameters, handler, **kwargs)

    def set_llm_caller(self, fn: Callable):
        """Set a custom LLM call function.

        Signature: async fn(messages, tools) -> dict with keys:
            content: str, tool_calls: list[dict] | None
        """
        self._llm_call = fn

    async def _default_llm_call(self, messages: list[dict], tools: list[dict]) -> dict:
        """Default LLM call using ModelRouter or direct OpenAI."""
        if self._router:
            from model_router import route_query
            result = await route_query(
                messages=messages,
                context={"domain": "general"},
                tools=tools if tools else None,
            )
            return {"content": result.content, "tool_calls": None}

        # Fallback: direct OpenAI call
        import httpx
        import os
        api_key = os.getenv("OPENAI_API_KEY", "")
        body: dict[str, Any] = {
            "model": "gpt-5.4",
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 4096,
        }
        if tools:
            body["tools"] = tools

        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json=body,
            )
            resp.raise_for_status()
            data = resp.json()

        choice = data["choices"][0]
        message = choice.get("message", {})
        tool_calls_raw = message.get("tool_calls")

        tool_calls = None
        if tool_calls_raw:
            tool_calls = []
            for tc in tool_calls_raw:
                tool_calls.append({
                    "id": tc["id"],
                    "type": "function",
                    "function": {
                        "name": tc["function"]["name"],
                        "arguments": tc["function"]["arguments"],
                    },
                })

        return {
            "content": message.get("content", ""),
            "tool_calls": tool_calls,
        }

    async def _execute_tool(self, tool_call: ToolCall) -> ToolResult:
        """Execute a single tool call with timeout."""
        tool_def = self.tools.get(tool_call.name)
        if not tool_def:
            return ToolResult(
                tool_call_id=tool_call.id,
                name=tool_call.name,
                content=json.dumps({"error": f"Unknown tool: {tool_call.name}"}),
                success=False,
            )

        start = time.time()
        try:
            result = await asyncio.wait_for(
                tool_def.handler(**tool_call.arguments),
                timeout=tool_def.timeout,
            )
            latency = int((time.time() - start) * 1000)

            if isinstance(result, dict):
                content = json.dumps(result)
            elif isinstance(result, str):
                content = result
            else:
                content = str(result)

            # Truncate very large results
            if len(content) > 50_000:
                content = content[:50_000] + "\n... [truncated, total length: " + str(len(content)) + "]"

            return ToolResult(
                tool_call_id=tool_call.id,
                name=tool_call.name,
                content=content,
                success=True,
                latency_ms=latency,
            )

        except asyncio.TimeoutError:
            return ToolResult(
                tool_call_id=tool_call.id,
                name=tool_call.name,
                content=json.dumps({"error": f"Tool {tool_call.name} timed out after {tool_def.timeout}s"}),
                success=False,
                latency_ms=tool_def.timeout * 1000,
            )
        except Exception as e:
            latency = int((time.time() - start) * 1000)
            log.error("Tool %s failed: %s", tool_call.name, e)
            return ToolResult(
                tool_call_id=tool_call.id,
                name=tool_call.name,
                content=json.dumps({"error": str(e)}),
                success=False,
                latency_ms=latency,
            )

    async def run(
        self,
        query: str,
        system_prompt: str = "You are Athena, an AI assistant with access to tools. Use tools when needed to answer the user's question accurately.",
        context: dict | None = None,
    ) -> AgentResult:
        """Run the agent loop until completion or max iterations."""
        run_id = uuid.uuid4().hex[:8]
        log.info("[%s] Starting agent loop: %s", run_id, query[:100])
        start = time.time()

        memory = ConversationMemory()
        memory.set_system(system_prompt)
        memory.add_user(query)

        llm_call = self._llm_call or self._default_llm_call
        openai_tools = self.tools.to_openai_tools()

        steps: list[AgentStep] = []
        final_answer = ""

        for iteration in range(self.max_iterations):
            log.info("[%s] Iteration %d", run_id, iteration + 1)

            # Call LLM
            messages = memory.get_messages()
            response = await llm_call(messages, openai_tools)

            content = response.get("content", "") or ""
            tool_calls_raw = response.get("tool_calls")

            # No tool calls → agent is done
            if not tool_calls_raw:
                final_answer = content
                steps.append(AgentStep(
                    iteration=iteration + 1,
                    reasoning=content,
                    tool_calls=[],
                    tool_results=[],
                ))
                break

            # Parse tool calls
            tool_calls: list[ToolCall] = []
            for tc in tool_calls_raw:
                fn = tc.get("function", {})
                args_str = fn.get("arguments", "{}")
                try:
                    args = json.loads(args_str) if isinstance(args_str, str) else args_str
                except json.JSONDecodeError:
                    args = {}

                tool_calls.append(ToolCall(
                    id=tc.get("id", uuid.uuid4().hex[:8]),
                    name=fn.get("name", ""),
                    arguments=args,
                ))

            # Add assistant message with tool calls to memory
            memory.add_assistant(content, tool_calls_raw)

            # Execute tools concurrently
            tool_results = await asyncio.gather(
                *[self._execute_tool(tc) for tc in tool_calls]
            )

            # Add tool results to memory
            for tr in tool_results:
                memory.add_tool_result(tr.tool_call_id, tr.name, tr.content)

            steps.append(AgentStep(
                iteration=iteration + 1,
                reasoning=content,
                tool_calls=tool_calls,
                tool_results=list(tool_results),
            ))

            log.info(
                "[%s] Executed %d tools: %s",
                run_id, len(tool_calls),
                ", ".join(tc.name for tc in tool_calls),
            )
        else:
            # Max iterations hit — force a final answer
            memory.add_user(
                "You have reached the maximum number of tool-call iterations. "
                "Please provide your best answer now based on what you've gathered."
            )
            messages = memory.get_messages()
            response = await llm_call(messages, [])  # No tools → force text
            final_answer = response.get("content", "")

        total_ms = int((time.time() - start) * 1000)
        log.info("[%s] Complete in %dms, %d steps", run_id, total_ms, len(steps))

        return AgentResult(
            answer=final_answer,
            steps=steps,
            total_iterations=len(steps),
            total_latency_ms=total_ms,
        )


# ── Example tool definitions ─────────────────────────────────

def create_semantic_search_tool(search_fn: Callable) -> dict:
    """Create a semantic search tool definition for the agent."""
    return {
        "name": "semantic_search",
        "description": "Search all documents using AI semantic similarity. Finds conceptually related content.",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Natural language search query"},
                "domain": {"type": "string", "enum": ["legal", "medical", "general"], "description": "Filter by domain"},
                "limit": {"type": "integer", "default": 5, "description": "Max results"},
            },
            "required": ["query"],
        },
        "handler": search_fn,
    }


def create_get_document_tool(get_fn: Callable) -> dict:
    """Create a document retrieval tool definition."""
    return {
        "name": "get_document",
        "description": "Retrieve a specific document by ID, including its full content.",
        "parameters": {
            "type": "object",
            "properties": {
                "document_id": {"type": "string", "description": "UUID of the document"},
            },
            "required": ["document_id"],
        },
        "handler": get_fn,
    }
