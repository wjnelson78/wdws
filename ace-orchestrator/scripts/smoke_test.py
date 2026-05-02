"""End-to-end smoke test against a running ACE Orchestrator service.

Usage (from project root):
    .venv/bin/python scripts/smoke_test.py

Reads ORCHESTRATOR_HOST, ORCHESTRATOR_PORT, ORCHESTRATOR_API_KEY from .env.
Hits /healthz, /readyz, /v1/users, /v1/users/will.nelson/context, and
/v1/chat/completions with a simple "hi" message.
"""
from __future__ import annotations

import asyncio
import os
import sys

import httpx
from dotenv import load_dotenv


async def main() -> int:
    load_dotenv()
    host = os.getenv("ORCHESTRATOR_HOST", "127.0.0.1")
    port = int(os.getenv("ORCHESTRATOR_PORT", "8080"))
    api_key = os.getenv("ORCHESTRATOR_API_KEY")
    if host == "0.0.0.0":
        host = "127.0.0.1"
    if not api_key:
        print("ORCHESTRATOR_API_KEY not set", file=sys.stderr)
        return 1

    base = f"http://{host}:{port}"
    headers = {"Authorization": f"Bearer {api_key}"}

    async with httpx.AsyncClient(timeout=120.0) as client:
        print(f"→ GET {base}/healthz")
        r = await client.get(f"{base}/healthz")
        print(f"  {r.status_code} {r.json()}")

        print(f"→ GET {base}/readyz")
        r = await client.get(f"{base}/readyz")
        print(f"  {r.status_code} {r.json()}")

        print(f"→ GET {base}/v1/users")
        r = await client.get(f"{base}/v1/users", headers=headers)
        print(f"  {r.status_code} {[u['identifier'] for u in r.json()]}")

        print(f"→ GET {base}/v1/users/will.nelson/context")
        r = await client.get(
            f"{base}/v1/users/will.nelson/context", headers=headers
        )
        meta = r.json()
        print(
            f"  {r.status_code} hash={meta['content_hash']} "
            f"contexts={meta['contexts_count']} cases={meta['cases_count']}"
        )

        print(f"→ POST {base}/v1/chat/completions  (streaming)")
        body = {
            "user_identifier": "will.nelson",
            "mode": "default",
            "messages": [{"role": "user", "content": "Reply with the single word 'pong'."}],
        }
        async with client.stream(
            "POST",
            f"{base}/v1/chat/completions",
            headers={**headers, "Accept": "text/event-stream"},
            json=body,
        ) as resp:
            print(f"  status: {resp.status_code}")
            event_count = 0
            async for line in resp.aiter_lines():
                if line.startswith("event:") or line.startswith("data:"):
                    event_count += 1
                    if event_count <= 6 or "orchestrator_done" in line:
                        print(f"  {line}")
                if event_count > 200:
                    print("  (truncated)")
                    break
            print(f"  total event/data lines: {event_count}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
