"""
WDWS Agent System — Configuration
===================================
All config loaded from /opt/wdws/.env via environment variables.
No secrets are hardcoded. Missing required vars will raise on import.
"""
import os
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
ENV_FILE = Path("/opt/wdws/.env")

# ── Load .env ────────────────────────────────────────────────
def _load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())
_load_env()

def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise RuntimeError(f"Missing required env var: {key} — check /opt/wdws/.env")
    return val

# ── Database ─────────────────────────────────────────────────
DATABASE_URL = _require("DATABASE_URL")

# ── OpenAI ───────────────────────────────────────────────────
OPENAI_API_KEY = _require("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("AGENT_LLM_MODEL", "gpt-4o")
OPENAI_EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")
AGENT_LLM_MODEL_HIGH = os.getenv("AGENT_LLM_MODEL_HIGH", OPENAI_MODEL)
AGENT_LLM_MODEL_LOW = os.getenv("AGENT_LLM_MODEL_LOW", OPENAI_MODEL)
AGENT_LLM_ROUTING_ENABLED = os.getenv("AGENT_LLM_ROUTING_ENABLED", "true").lower() in (
    "1", "true", "yes", "on"
)

# ── Agent defaults ───────────────────────────────────────────
LOG_LEVEL = os.getenv("AGENT_LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s %(message)s"
AGENT_MAX_CONCURRENT = int(os.getenv("AGENT_MAX_CONCURRENT", "3"))
AGENT_TICK_SECONDS = int(os.getenv("AGENT_TICK_SECONDS", "30"))
AGENT_RUN_TIMEOUT_SECONDS = int(os.getenv("AGENT_RUN_TIMEOUT_SECONDS", "900"))
AGENT_WAKE_TIMEOUT_SECONDS = int(os.getenv("AGENT_WAKE_TIMEOUT_SECONDS", "180"))

# ── Server ───────────────────────────────────────────────────
MCP_SERVER_HOST = os.getenv("MCP_SERVER_HOST", "172.16.32.207")
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "9100"))
MCP_PORT = int(os.getenv("MCP_PORT", "9200"))

# ── Cloudflare ───────────────────────────────────────────────
CLOUDFLARE_EMAIL = os.getenv("CLOUDFLARE_EMAIL", "")
CLOUDFLARE_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CLOUDFLARE_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID_12432NET", "")
CLOUDFLARE_TUNNEL_ID = os.getenv("CLOUDFLARE_TUNNEL_ID", "")
CLOUDFLARE_TUNNEL_NAME = os.getenv("CLOUDFLARE_TUNNEL_NAME", "tukwila-colo-tunnel")

# ── Alerts ───────────────────────────────────────────────────
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "william@seattleseahawks.me")
OPENAI_BALANCE_THRESHOLD = float(os.getenv("OPENAI_BALANCE_THRESHOLD", "5.0"))

# ── Microsoft Graph API (Email) ──────────────────────────────
GRAPH_TENANT_ID = _require("GRAPH_TENANT_ID")
GRAPH_CLIENT_ID = _require("GRAPH_CLIENT_ID")
GRAPH_CLIENT_SECRET = _require("GRAPH_CLIENT_SECRET")
GRAPH_SENDER_EMAIL = os.getenv("GRAPH_SENDER_EMAIL", "athena@seattleseahawks.me")

# ── OpenAI Codex (Code Doctor) ───────────────────────────────
CODEX_MODEL = os.getenv("CODEX_MODEL", "gpt-5.2-codex")
AGENT_LLM_MODEL_CODEX = os.getenv("AGENT_LLM_MODEL_CODEX", CODEX_MODEL)

