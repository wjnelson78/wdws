#!/usr/bin/env python3
"""
Athena Email Webhook Lifecycle Manager.

Handles:
  1. Creating webhook subscriptions for all active accounts
  2. Renewing expiring subscriptions (Graph max ~3 days for mail)
  3. Cleaning up stale/inactive subscriptions

Designed to run via systemd timer every 12 hours, or on-demand.
"""
import os
import sys
import json
import asyncio
import logging
import httpx
from pathlib import Path

# Load .env
_env_file = Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("webhook_lifecycle")

CHAT_API_URL = os.getenv("CHAT_API_URL", "http://127.0.0.1:9350")


async def main():
    async with httpx.AsyncClient(timeout=60) as client:
        # Step 1: Create subscriptions for any accounts that don't have one
        log.info("Creating webhook subscriptions for active accounts...")
        resp = await client.post(f"{CHAT_API_URL}/api/email/webhooks", json={})
        if resp.status_code == 200:
            data = resp.json()
            for sub in data.get("subscriptions", []):
                log.info("  %s: %s (%s)", sub["email"], sub["status"], sub.get("subscription_id", "")[:12])
        else:
            log.error("  Create failed: %s %s", resp.status_code, resp.text[:200])

        # Step 2: Renew any expiring subscriptions
        log.info("Renewing expiring subscriptions...")
        resp = await client.post(f"{CHAT_API_URL}/api/email/webhooks/renew", json={})
        if resp.status_code == 200:
            data = resp.json()
            log.info("  Renewed: %s", data.get("renewed", 0))
            for r in data.get("results", []):
                log.info("  %s: %s", r["email"], r["status"])
        else:
            log.error("  Renew failed: %s %s", resp.status_code, resp.text[:200])

        # Step 3: Show current status
        resp = await client.get(f"{CHAT_API_URL}/api/email/webhooks")
        if resp.status_code == 200:
            data = resp.json()
            subs = data.get("subscriptions", [])
            log.info("Active subscriptions: %d", len(subs))
            for s in subs:
                log.info("  %s → %s (expires %s)", s["email_address"], s["subscription_id"][:12], s["expiration_at"])
        else:
            log.error("  List failed: %s", resp.status_code)


if __name__ == "__main__":
    asyncio.run(main())
