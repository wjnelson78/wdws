#!/usr/bin/env python3
"""
Telnyx SMS adapter.

Simple wrapper around Telnyx's `/v2/messages` REST endpoint. We call this
directly via `requests` rather than the pip SDK — the SDK churns its public
surface across major versions, the REST API is stable, and this is ~30 lines.

Config lives in /opt/wdws/.env:
    TELNYX_API_KEY    — provisioned from portal (starts with KEY...)
    TELNYX_SMS_FROM   — our sending number in E.164 (e.g. +14256459222)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

import httpx

log = logging.getLogger("telnyx-sms")

TELNYX_API_URL = "https://api.telnyx.com/v2/messages"
DEFAULT_TIMEOUT = 15.0


@dataclass
class SmsResult:
    ok: bool
    message_id: Optional[str] = None
    cost_usd: Optional[str] = None
    parts: Optional[int] = None
    error_code: Optional[str] = None
    error_title: Optional[str] = None
    error_detail: Optional[str] = None


def _load_env() -> None:
    env_path = "/opt/wdws/.env"
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.startswith("TELNYX_") and k not in os.environ:
                os.environ[k] = v


def send_sms(
    to: str,
    text: str,
    *,
    from_: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> SmsResult:
    """Send a single SMS via Telnyx. Returns structured result."""
    _load_env()

    api_key = os.environ.get("TELNYX_API_KEY", "")
    from_num = from_ or os.environ.get("TELNYX_SMS_FROM", "")
    if not api_key:
        return SmsResult(ok=False, error_title="config_missing", error_detail="TELNYX_API_KEY not set")
    if not from_num:
        return SmsResult(ok=False, error_title="config_missing", error_detail="TELNYX_SMS_FROM not set")

    payload = {"from": from_num, "to": to, "text": text}
    try:
        resp = httpx.post(
            TELNYX_API_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            content=json.dumps(payload),
            timeout=timeout,
        )
    except httpx.HTTPError as e:
        return SmsResult(ok=False, error_title="http_error", error_detail=str(e))

    try:
        body = resp.json()
    except Exception:
        return SmsResult(
            ok=False,
            error_title=f"http_{resp.status_code}",
            error_detail=resp.text[:200],
        )

    if "data" in body:
        m = body["data"]
        cost = m.get("cost") or {}
        return SmsResult(
            ok=True,
            message_id=m.get("id"),
            cost_usd=str(cost.get("amount")) if cost else None,
            parts=m.get("parts"),
        )

    errors = body.get("errors") or []
    first = errors[0] if errors else {}
    return SmsResult(
        ok=False,
        error_code=str(first.get("code")) if first.get("code") is not None else None,
        error_title=first.get("title") or f"http_{resp.status_code}",
        error_detail=first.get("detail") or resp.text[:200],
    )


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("usage: telnyx_sms.py <to-e164> <text>", file=sys.stderr)
        sys.exit(2)
    r = send_sms(sys.argv[1], sys.argv[2])
    print(json.dumps(r.__dict__, indent=2))
    sys.exit(0 if r.ok else 1)
