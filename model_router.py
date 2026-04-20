"""
Athena Cognitive Engine — Model Router

Routes queries to the optimal LLM provider based on:
  - Task classification (triage, summary, reasoning, code, legal, medical)
  - Privacy requirements (local-only for sensitive data)
  - Cost/latency targets
  - Provider availability (circuit breaker)

Provider tiers:
  1. Ollama (local)     — llama3.2:3b for triage, classification, private queries
  2. Claude Haiku       — fast cloud tier for summaries, simple Q&A
  3. Claude Sonnet      — standard reasoning, drafting
  4. GPT-5.4            — existing default, complex tasks
  5. Claude Opus / o3   — deep legal/medical reasoning, high-stakes analysis

Usage:
    router = ModelRouter()
    await router.initialize()

    result = await router.route_and_call(
        messages=[{"role": "user", "content": "Summarize this email"}],
        context={"domain": "legal", "privacy": "normal"}
    )
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Optional

import httpx

log = logging.getLogger("athena.model_router")

# ── Load .env ────────────────────────────────────────────────
_env_file = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_file):
    with open(_env_file) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

# ── Configuration ────────────────────────────────────────────

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")


class TaskType(str, Enum):
    TRIAGE = "triage"           # Classification, routing, yes/no
    SUMMARY = "summary"         # Summarization, extraction
    REASONING = "reasoning"     # Multi-step logic, analysis
    CODE = "code"               # Code generation, review
    LEGAL = "legal"             # Legal analysis, case reasoning
    MEDICAL = "medical"         # Medical record analysis
    CREATIVE = "creative"       # Drafting, writing
    CONVERSATION = "conversation"  # General chat


class PrivacyLevel(str, Enum):
    LOCAL_ONLY = "local_only"   # Must stay on-device (PII, credentials)
    NORMAL = "normal"           # Can use cloud providers
    ANY = "any"                 # No restrictions


@dataclass
class ProviderConfig:
    name: str
    model: str
    provider: str               # "ollama", "anthropic", "openai"
    max_tokens: int = 4096
    temperature: float = 0.7
    cost_per_1k_input: float = 0.0
    cost_per_1k_output: float = 0.0
    avg_latency_ms: int = 500
    supports_streaming: bool = True
    supports_tools: bool = True
    is_local: bool = False
    context_window: int = 128000


@dataclass
class CircuitState:
    failures: int = 0
    last_failure: float = 0.0
    is_open: bool = False
    open_until: float = 0.0

    def record_failure(self):
        self.failures += 1
        self.last_failure = time.time()
        if self.failures >= 3:
            self.is_open = True
            self.open_until = time.time() + 60  # 60s cooldown

    def record_success(self):
        self.failures = 0
        self.is_open = False

    def is_available(self) -> bool:
        if not self.is_open:
            return True
        if time.time() > self.open_until:
            self.is_open = False  # Half-open: allow one attempt
            return True
        return False


@dataclass
class RoutingResult:
    provider: str
    model: str
    content: str
    tokens_in: int = 0
    tokens_out: int = 0
    latency_ms: int = 0
    cost_usd: float = 0.0
    fallback_used: bool = False
    task_type: str = ""


# ── Provider Definitions ─────────────────────────────────────

PROVIDERS: dict[str, ProviderConfig] = {
    "claude-code:haiku": ProviderConfig(
        name="Claude Haiku (via CLI)",
        model="haiku",
        provider="claude-code",
        max_tokens=4096,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        avg_latency_ms=3000,
        supports_tools=False,
        context_window=200000,
    ),
    "claude-code:sonnet": ProviderConfig(
        name="Claude Sonnet (via CLI)",
        model="sonnet",
        provider="claude-code",
        max_tokens=8192,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        avg_latency_ms=5000,
        supports_tools=False,
        context_window=200000,
    ),
    "claude-code:opus": ProviderConfig(
        name="Claude Opus (via CLI)",
        model="opus",
        provider="claude-code",
        max_tokens=16384,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        avg_latency_ms=8000,
        supports_tools=False,
        context_window=1000000,
    ),
    "ollama:llama3.2:3b": ProviderConfig(
        name="Ollama Llama 3.2 3B",
        model="llama3.2:3b",
        provider="ollama",
        max_tokens=2048,
        cost_per_1k_input=0.0,
        cost_per_1k_output=0.0,
        avg_latency_ms=200,
        supports_tools=False,
        is_local=True,
        context_window=128000,
    ),
    "anthropic:claude-haiku-4-5": ProviderConfig(
        name="Claude Haiku 4.5",
        model="claude-haiku-4-5-20251001",
        provider="anthropic",
        max_tokens=4096,
        cost_per_1k_input=0.001,
        cost_per_1k_output=0.005,
        avg_latency_ms=400,
        context_window=200000,
    ),
    "anthropic:claude-sonnet-4-6": ProviderConfig(
        name="Claude Sonnet 4.6",
        model="claude-sonnet-4-6",
        provider="anthropic",
        max_tokens=8192,
        cost_per_1k_input=0.003,
        cost_per_1k_output=0.015,
        avg_latency_ms=800,
        context_window=200000,
    ),
    "openai:gpt-5.4": ProviderConfig(
        name="GPT-5.4",
        model="gpt-5.4",
        provider="openai",
        max_tokens=8192,
        cost_per_1k_input=0.005,
        cost_per_1k_output=0.015,
        avg_latency_ms=1000,
        context_window=128000,
    ),
    "anthropic:claude-opus-4-6": ProviderConfig(
        name="Claude Opus 4.6",
        model="claude-opus-4-6",
        provider="anthropic",
        max_tokens=16384,
        cost_per_1k_input=0.015,
        cost_per_1k_output=0.075,
        avg_latency_ms=2000,
        context_window=1000000,
    ),
}

# ── Routing Tables ───────────────────────────────────────────
# Each task type maps to an ordered list of provider keys (preference order).
# Router tries each in order, skipping unavailable providers.

ROUTING_TABLE: dict[TaskType, list[str]] = {
    TaskType.TRIAGE: [
        "anthropic:claude-haiku-4-5",
        "claude-code:haiku",
        "ollama:llama3.2:3b",
        "openai:gpt-5.4",
    ],
    TaskType.SUMMARY: [
        "anthropic:claude-haiku-4-5",
        "claude-code:haiku",
        "openai:gpt-5.4",
        "ollama:llama3.2:3b",
    ],
    TaskType.REASONING: [
        "anthropic:claude-sonnet-4-6",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "claude-code:opus",
        "ollama:llama3.2:3b",
    ],
    TaskType.CODE: [
        "anthropic:claude-sonnet-4-6",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "ollama:llama3.2:3b",
    ],
    TaskType.LEGAL: [
        "anthropic:claude-opus-4-6",
        "anthropic:claude-sonnet-4-6",
        "claude-code:opus",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "ollama:llama3.2:3b",
    ],
    TaskType.MEDICAL: [
        "anthropic:claude-opus-4-6",
        "anthropic:claude-sonnet-4-6",
        "claude-code:opus",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "ollama:llama3.2:3b",
    ],
    TaskType.CREATIVE: [
        "anthropic:claude-sonnet-4-6",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "claude-code:haiku",
        "ollama:llama3.2:3b",
    ],
    TaskType.CONVERSATION: [
        "anthropic:claude-haiku-4-5",
        "claude-code:haiku",
        "claude-code:sonnet",
        "openai:gpt-5.4",
        "ollama:llama3.2:3b",
    ],
}


class ModelRouter:
    """Routes LLM queries to optimal provider with fallback chain."""

    def __init__(self):
        self._circuits: dict[str, CircuitState] = {}
        self._http: Optional[httpx.AsyncClient] = None

    async def initialize(self):
        """Initialize HTTP client and verify provider availability."""
        self._http = httpx.AsyncClient(timeout=60.0)
        for key in PROVIDERS:
            self._circuits[key] = CircuitState()

        # Check which providers are actually reachable
        available = []
        if OLLAMA_BASE_URL:
            try:
                resp = await self._http.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
                if resp.status_code == 200:
                    models = [m["name"] for m in resp.json().get("models", [])]
                    available.append(f"ollama ({', '.join(models)})")
                else:
                    self._circuits["ollama:llama3.2:3b"].is_open = True
            except Exception:
                self._circuits["ollama:llama3.2:3b"].is_open = True

        if ANTHROPIC_API_KEY:
            available.append("anthropic")
        else:
            for key in PROVIDERS:
                if PROVIDERS[key].provider == "anthropic":
                    self._circuits[key].is_open = True

        if OPENAI_API_KEY:
            available.append("openai")
        else:
            for key in PROVIDERS:
                if PROVIDERS[key].provider == "openai":
                    self._circuits[key].is_open = True

        log.info("ModelRouter initialized. Available providers: %s", available)

    async def close(self):
        if self._http:
            await self._http.aclose()

    def classify_task(self, messages: list[dict], context: dict | None = None) -> TaskType:
        """Classify the task type from the conversation messages.

        Uses keyword heuristics for speed. For production, replace with
        a lightweight classifier (local LLM or fine-tuned model).
        """
        context = context or {}

        # Domain override from context
        domain = context.get("domain", "").lower()
        if domain == "legal":
            return TaskType.LEGAL
        if domain == "medical":
            return TaskType.MEDICAL

        # Analyze the last user message
        last_msg = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                last_msg = m.get("content", "").lower()
                break

        if not last_msg:
            return TaskType.CONVERSATION

        # Keyword classification
        legal_kw = {"case", "court", "filing", "motion", "deposition", "plaintiff",
                     "defendant", "counsel", "statute", "litigation", "verdict"}
        medical_kw = {"patient", "diagnosis", "treatment", "medication", "lab result",
                       "imaging", "clinical", "prescription", "symptoms", "prognosis"}
        code_kw = {"code", "function", "class", "debug", "refactor", "python",
                    "javascript", "sql", "api", "endpoint", "bug", "error"}
        summary_kw = {"summarize", "summary", "tldr", "key points", "overview",
                       "brief", "recap", "digest"}
        triage_kw = {"classify", "categorize", "which", "yes or no", "true or false",
                      "is this", "does this", "should i"}

        words = set(last_msg.split())

        if len(words & legal_kw) >= 2:
            return TaskType.LEGAL
        if len(words & medical_kw) >= 2:
            return TaskType.MEDICAL
        if len(words & code_kw) >= 2:
            return TaskType.CODE
        if words & summary_kw:
            return TaskType.SUMMARY
        if words & triage_kw:
            return TaskType.TRIAGE

        # Default based on message length
        if len(last_msg) < 50:
            return TaskType.CONVERSATION
        return TaskType.REASONING

    def select_provider(
        self,
        task_type: TaskType,
        privacy: PrivacyLevel = PrivacyLevel.NORMAL,
    ) -> tuple[str, ProviderConfig]:
        """Select the best available provider for the task."""
        candidates = ROUTING_TABLE.get(task_type, ROUTING_TABLE[TaskType.CONVERSATION])

        for key in candidates:
            config = PROVIDERS[key]
            circuit = self._circuits.get(key)

            # Privacy filter: local_only requires is_local
            if privacy == PrivacyLevel.LOCAL_ONLY and not config.is_local:
                continue

            # Circuit breaker check
            if circuit and not circuit.is_available():
                continue

            return key, config

        # Absolute fallback: try any available provider
        for key, config in PROVIDERS.items():
            circuit = self._circuits.get(key)
            if circuit and circuit.is_available():
                if privacy != PrivacyLevel.LOCAL_ONLY or config.is_local:
                    return key, config

        raise RuntimeError("All LLM providers are unavailable")

    async def call_provider(
        self,
        key: str,
        config: ProviderConfig,
        messages: list[dict],
        temperature: float | None = None,
        max_tokens: int | None = None,
        tools: list[dict] | None = None,
    ) -> RoutingResult:
        """Call a specific LLM provider."""
        assert self._http is not None, "Router not initialized. Call initialize() first."

        temp = temperature if temperature is not None else config.temperature
        max_tok = max_tokens or config.max_tokens
        start = time.time()

        try:
            if config.provider == "claude-code":
                result = await self._call_claude_code(config, messages, max_tok)
            elif config.provider == "ollama":
                result = await self._call_ollama(config, messages, temp, max_tok)
            elif config.provider == "anthropic":
                result = await self._call_anthropic(config, messages, temp, max_tok, tools)
            elif config.provider == "openai":
                result = await self._call_openai(config, messages, temp, max_tok, tools)
            else:
                raise ValueError(f"Unknown provider: {config.provider}")

            latency = int((time.time() - start) * 1000)
            result.latency_ms = latency
            result.cost_usd = (
                (result.tokens_in / 1000) * config.cost_per_1k_input
                + (result.tokens_out / 1000) * config.cost_per_1k_output
            )

            self._circuits[key].record_success()
            return result

        except Exception as e:
            self._circuits[key].record_failure()
            log.warning("Provider %s failed: %s", key, e)
            raise

    async def route_and_call(
        self,
        messages: list[dict],
        context: dict | None = None,
        temperature: float | None = None,
        max_tokens: int | None = None,
        tools: list[dict] | None = None,
        privacy: PrivacyLevel = PrivacyLevel.NORMAL,
    ) -> RoutingResult:
        """Classify, route, call with automatic fallback."""
        task_type = self.classify_task(messages, context)
        candidates = ROUTING_TABLE.get(task_type, ROUTING_TABLE[TaskType.CONVERSATION])

        last_error = None
        fallback_used = False

        for i, key in enumerate(candidates):
            config = PROVIDERS.get(key)
            if not config:
                continue

            circuit = self._circuits.get(key)
            if circuit and not circuit.is_available():
                continue
            if privacy == PrivacyLevel.LOCAL_ONLY and not config.is_local:
                continue

            try:
                result = await self.call_provider(
                    key, config, messages, temperature, max_tokens, tools
                )
                result.fallback_used = fallback_used
                result.task_type = task_type.value
                return result
            except Exception as e:
                last_error = e
                fallback_used = True
                log.warning("Fallback from %s: %s", key, e)
                continue

        raise RuntimeError(f"All providers failed for {task_type}. Last error: {last_error}")

    # ── Provider-specific call implementations ───────────────

    async def _call_claude_code(
        self, config: ProviderConfig, messages: list[dict],
        max_tokens: int,
    ) -> RoutingResult:
        """Call Claude via Claude Code CLI (uses Max subscription)."""
        import asyncio as _aio
        import os as _os

        # Clean env so CLI uses its own OAuth, not expired API keys
        env = _os.environ.copy()
        env.pop("ANTHROPIC_API_KEY", None)
        env.pop("OPENAI_API_KEY", None)
        env.setdefault("HOME", "/root")

        # Build prompt from messages
        parts = []
        system_parts = []
        for m in messages:
            if m.get("role") == "system":
                system_parts.append(m["content"])
            elif m.get("role") == "user":
                parts.append(m["content"])
            elif m.get("role") == "assistant":
                parts.append(f"[Previous response: {m['content'][:200]}...]")
        prompt = "\n\n".join(parts) if parts else "Hello"

        # Find claude CLI — check common install locations
        import shutil
        claude_bin = shutil.which("claude") or "/root/.local/bin/claude"
        cmd = [claude_bin, "-p", "--model", config.model, "--output-format", "text"]
        if system_parts:
            cmd.extend(["--append-system-prompt", "\n".join(system_parts)])

        proc = await _aio.create_subprocess_exec(
            *cmd,
            stdin=_aio.subprocess.PIPE,
            stdout=_aio.subprocess.PIPE,
            stderr=_aio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await _aio.wait_for(
            proc.communicate(input=prompt.encode("utf-8")),
            timeout=120,
        )

        if proc.returncode != 0:
            raise RuntimeError(f"Claude Code CLI error: {stderr.decode()[:300]}")

        content = stdout.decode("utf-8", errors="replace").strip()

        return RoutingResult(
            provider=config.name,
            model=config.model,
            content=content,
        )

    async def _call_ollama(
        self, config: ProviderConfig, messages: list[dict],
        temperature: float, max_tokens: int,
    ) -> RoutingResult:
        resp = await self._http.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json={
                "model": config.model,
                "messages": messages,
                "options": {"temperature": temperature, "num_predict": max_tokens},
                "stream": False,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        return RoutingResult(
            provider=config.name,
            model=config.model,
            content=data.get("message", {}).get("content", ""),
            tokens_in=data.get("prompt_eval_count", 0),
            tokens_out=data.get("eval_count", 0),
        )

    async def _call_anthropic(
        self, config: ProviderConfig, messages: list[dict],
        temperature: float, max_tokens: int,
        tools: list[dict] | None = None,
    ) -> RoutingResult:
        # Convert OpenAI-style messages to Anthropic format
        system_msg = ""
        anthropic_messages = []
        for m in messages:
            if m["role"] == "system":
                system_msg = m["content"]
            else:
                anthropic_messages.append({
                    "role": m["role"],
                    "content": m["content"],
                })

        body: dict[str, Any] = {
            "model": config.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": anthropic_messages,
        }
        if system_msg:
            body["system"] = system_msg
        if tools:
            body["tools"] = tools

        resp = await self._http.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        content_parts = data.get("content", [])
        text = " ".join(
            p.get("text", "") for p in content_parts if p.get("type") == "text"
        )

        usage = data.get("usage", {})
        return RoutingResult(
            provider=config.name,
            model=config.model,
            content=text,
            tokens_in=usage.get("input_tokens", 0),
            tokens_out=usage.get("output_tokens", 0),
        )

    async def _call_openai(
        self, config: ProviderConfig, messages: list[dict],
        temperature: float, max_tokens: int,
        tools: list[dict] | None = None,
    ) -> RoutingResult:
        body: dict[str, Any] = {
            "model": config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            body["tools"] = tools

        resp = await self._http.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        choice = data["choices"][0]
        content = choice.get("message", {}).get("content", "")
        usage = data.get("usage", {})

        return RoutingResult(
            provider=config.name,
            model=config.model,
            content=content,
            tokens_in=usage.get("prompt_tokens", 0),
            tokens_out=usage.get("completion_tokens", 0),
        )


# ── Convenience ──────────────────────────────────────────────

_router: Optional[ModelRouter] = None


async def get_router() -> ModelRouter:
    """Get or create the singleton ModelRouter."""
    global _router
    if _router is None:
        _router = ModelRouter()
        await _router.initialize()
    return _router


async def route_query(
    messages: list[dict],
    context: dict | None = None,
    **kwargs,
) -> RoutingResult:
    """Convenience function: classify, route, and call."""
    router = await get_router()
    return await router.route_and_call(messages, context, **kwargs)
