import os
import pathlib
import sys

import pytest
from dotenv import load_dotenv

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

sys.path.insert(0, str(REPO_ROOT / "src"))

# Load real .env if present so live integration tests can hit the athena DB.
# Falls back to safe placeholders so unit tests that don't touch the DB still
# import cleanly.
load_dotenv(REPO_ROOT / ".env", override=False)
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test"
)
os.environ.setdefault(
    "ACE_DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test"
)
os.environ.setdefault("ORCHESTRATOR_API_KEY", "test-orchestrator-key")


@pytest.fixture(scope="session")
def v2_markdown() -> str:
    return (REPO_ROOT / "data" / "system_prompt_v2_0.md").read_text(encoding="utf-8")
