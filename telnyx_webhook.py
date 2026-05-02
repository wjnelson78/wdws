#!/usr/bin/env python3
"""
Telnyx inbound-SMS webhook receiver.

Single-purpose FastAPI service bound to 127.0.0.1:9110. Receives
`message.received` events from Telnyx's messaging profile, verifies the
Ed25519 signature, and writes an inbound row into core.sms_messages,
attaching it to the most-recent-open thread for (peer, our_number) or
opening a new one.

External URL routed via the tukwila-colo-tunnel:
    POST https://klunky.12432.net/telnyx/webhook

Config (/opt/wdws/.env):
    DATABASE_URL               — Postgres (wdws DB)
    TELNYX_PUBLIC_KEY          — Ed25519 public key from Telnyx portal,
                                 base64-encoded (32 bytes)
    TELNYX_WEBHOOK_PORT        — optional; defaults to 9110

Signature verification follows Telnyx's spec:
    message = timestamp + "|" + raw_body
    signature = base64(ed25519_sign(message, private_key))
    headers:
        Telnyx-Signature-Ed25519
        Telnyx-Timestamp
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

import asyncpg
import uvicorn
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fastapi import FastAPI, Header, HTTPException, Request

# Load /opt/wdws/.env
_env_file = Path("/opt/wdws/.env")
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

sys.path.insert(0, "/opt/wdws")
try:
    from athena_logging import setup_logging, get_logger
    setup_logging("telnyx-webhook")
    log = get_logger()
except ImportError:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    log = logging.getLogger("telnyx-webhook")

DATABASE_URL = os.environ["DATABASE_URL"]
TELNYX_PUBLIC_KEY_B64 = os.environ.get("TELNYX_PUBLIC_KEY", "")
WEBHOOK_PORT = int(os.environ.get("TELNYX_WEBHOOK_PORT", "9110"))

# Reject events whose timestamp is older than this (replay window).
MAX_TIMESTAMP_SKEW_SECS = 300

app = FastAPI(title="Telnyx SMS Webhook", version="1.0.0")
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    return _pool


def _verify_signature(raw_body: bytes, signature_b64: str, timestamp: str) -> None:
    if not TELNYX_PUBLIC_KEY_B64:
        raise HTTPException(status_code=500, detail="TELNYX_PUBLIC_KEY not configured")
    try:
        pub_key_bytes = base64.b64decode(TELNYX_PUBLIC_KEY_B64)
        pub_key = Ed25519PublicKey.from_public_bytes(pub_key_bytes)
        sig_bytes = base64.b64decode(signature_b64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"malformed signature or key: {e}")

    try:
        ts = int(timestamp)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid timestamp")
    if abs(time.time() - ts) > MAX_TIMESTAMP_SKEW_SECS:
        raise HTTPException(status_code=400, detail="timestamp outside replay window")

    signed_payload = f"{timestamp}|".encode() + raw_body
    try:
        pub_key.verify(sig_bytes, signed_payload)
    except InvalidSignature:
        raise HTTPException(status_code=401, detail="invalid signature")


async def _attach_or_open_thread(
    pool: asyncpg.Pool, peer: str, ours: str
) -> int:
    """Return thread_id: most-recent-open with matching (peer, ours), else new."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT id FROM core.sms_threads
                 WHERE peer_number = $1 AND our_number = $2 AND closed_at IS NULL
                 ORDER BY last_message_at DESC
                 LIMIT 1
                """,
                peer,
                ours,
            )
            if row:
                return row["id"]
            row = await conn.fetchrow(
                """
                INSERT INTO core.sms_threads (peer_number, our_number, subject)
                VALUES ($1, $2, NULL)
                RETURNING id
                """,
                peer,
                ours,
            )
            return row["id"]


@app.get("/healthz")
async def healthz() -> dict[str, Any]:
    return {"ok": True, "service": "telnyx-webhook", "port": WEBHOOK_PORT}


@app.post("/telnyx/webhook")
async def telnyx_webhook(
    request: Request,
    telnyx_signature_ed25519: str = Header(..., alias="telnyx-signature-ed25519"),
    telnyx_timestamp: str = Header(..., alias="telnyx-timestamp"),
) -> dict[str, Any]:
    raw = await request.body()
    _verify_signature(raw, telnyx_signature_ed25519, telnyx_timestamp)

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid JSON")

    data = payload.get("data") or {}
    event_type = data.get("event_type")
    if event_type != "message.received":
        # Acknowledge other event types (e.g., message.sent, message.finalized)
        # so Telnyx doesn't retry. We only persist inbound messages here;
        # outbound is logged by the MCP tool at send-time.
        log.info("ignored event_type=%s", event_type)
        return {"ok": True, "ignored": event_type}

    payload_obj = data.get("payload") or {}
    msg_id = payload_obj.get("id")
    text = payload_obj.get("text") or ""

    def _num(v: Any) -> Optional[str]:
        # Telnyx payload fields for `from` / `to` entries are dicts with a
        # phone_number key, but historical/edge responses have sometimes been
        # bare strings. Accept either shape.
        if isinstance(v, dict):
            return v.get("phone_number")
        if isinstance(v, str):
            return v
        return None

    peer = _num(payload_obj.get("from"))
    to_raw = payload_obj.get("to")
    if isinstance(to_raw, list) and to_raw:
        ours = _num(to_raw[0])
    else:
        ours = _num(to_raw)

    if not peer or not ours:
        log.warning(
            "inbound missing from/to: msg_id=%s raw_from=%r raw_to=%r",
            msg_id, payload_obj.get("from"), payload_obj.get("to"),
        )
        raise HTTPException(status_code=400, detail="missing from/to")

    pool = await get_pool()
    thread_id = await _attach_or_open_thread(pool, peer, ours)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO core.sms_messages
                    (thread_id, direction, body, telnyx_message_id, parts, cost_usd)
                VALUES ($1, 'inbound', $2, $3, $4, NULL)
                ON CONFLICT (telnyx_message_id) DO NOTHING
                """,
                thread_id,
                text,
                msg_id,
                payload_obj.get("parts"),
            )
            await conn.execute(
                "UPDATE core.sms_threads SET last_message_at = now() WHERE id = $1",
                thread_id,
            )

    log.info("inbound SMS: thread=%d peer=%s msg_id=%s len=%d", thread_id, peer, msg_id, len(text))
    return {"ok": True, "thread_id": thread_id, "message_id": msg_id}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=WEBHOOK_PORT, log_level="info")
