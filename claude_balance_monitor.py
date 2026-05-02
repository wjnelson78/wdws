#!/usr/bin/env python3
"""
Monitor Claude API credit balance and SMS when it drops below a threshold.

Anthropic does not expose a direct "credits remaining" endpoint, so this works
by anchoring to a known top-up and subtracting cumulative spend reported by the
Admin API cost_report endpoint since that anchor.

State lives at /opt/wdws/.claude_balance_state.json and is editable by hand.
Whenever you top up the Anthropic account, record the new anchor:

    python3 /opt/wdws/claude_balance_monitor.py --topup 50

Requires ANTHROPIC_ADMIN_API_KEY in /opt/wdws/.env (an Admin API key, distinct
from the regular ANTHROPIC_API_KEY — create one at console → Settings → Admin
Keys; it starts with sk-ant-admin01-).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx

sys.path.insert(0, "/opt/wdws")
from telnyx_sms import send_sms  # noqa: E402

STATE_PATH = Path("/opt/wdws/.claude_balance_state.json")
ENV_PATH = Path("/opt/wdws/.env")
ADMIN_KEY_ENV = "ANTHROPIC_ADMIN_API_KEY"
COST_REPORT_URL = "https://api.anthropic.com/v1/organizations/cost_report"
ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_DEDUP_HOURS = 24

log = logging.getLogger("claude-balance-monitor")


def _load_env() -> None:
    if not ENV_PATH.exists():
        return
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k and k not in os.environ:
            os.environ[k] = v


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text())


def _save_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.chmod(tmp, 0o600)
    tmp.replace(STATE_PATH)


def fetch_spent_cents(api_key: str, starting_at: str) -> int:
    total = 0
    page: str | None = None
    while True:
        params: dict[str, str] = {"starting_at": starting_at}
        if page:
            params["page"] = page
        r = httpx.get(
            COST_REPORT_URL,
            headers={
                "X-Api-Key": api_key,
                "anthropic-version": ANTHROPIC_VERSION,
            },
            params=params,
            timeout=30.0,
        )
        r.raise_for_status()
        body = r.json()
        for bucket in body.get("data", []):
            for item in bucket.get("results", []):
                amt = item.get("amount")
                if amt is None:
                    continue
                total += int(round(float(amt)))
        if not body.get("has_more"):
            break
        page = body.get("next_page")
        if not page:
            break
    return total


def _fmt_money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents)/100:.2f}"


def cmd_show(state: dict) -> int:
    print(json.dumps(state, indent=2))
    return 0


def cmd_topup(state: dict, amount: float) -> int:
    _load_env()
    api_key = os.environ.get(ADMIN_KEY_ENV, "")
    now = _now()
    now_iso = _iso(now)

    anchor_day_prior_cents = 0
    if api_key:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            anchor_day_prior_cents = fetch_spent_cents(api_key, _iso(day_start))
            log.info(
                "captured anchor-day prior spend at top-up: %s",
                _fmt_money(anchor_day_prior_cents),
            )
        except Exception as e:
            log.warning(
                "could not capture anchor-day prior spend (%s); checks will over-count until next UTC day",
                e,
            )
    else:
        log.warning(
            "%s not set — anchor-day prior spend NOT captured; same-day pre-topup spend will wrongly count against balance until next UTC day",
            ADMIN_KEY_ENV,
        )

    state["top_up_amount_usd"] = amount
    state["top_up_at"] = now_iso
    state["anchor_day_prior_spend_cents"] = anchor_day_prior_cents
    state["last_alert_at"] = None
    state["last_alerted_balance_usd"] = None
    _save_state(state)
    print(
        f"Recorded top-up of ${amount:.2f} at {now_iso} "
        f"(anchor-day prior spend: {_fmt_money(anchor_day_prior_cents)})"
    )
    return 0


def cmd_check(state: dict) -> int:
    _load_env()
    api_key = os.environ.get(ADMIN_KEY_ENV, "")
    if not api_key:
        log.error("%s not set in /opt/wdws/.env", ADMIN_KEY_ENV)
        return 2
    if not state.get("top_up_at") or "top_up_amount_usd" not in state:
        log.error("no top-up anchor in state; run with --topup <amount> first")
        return 2

    top_up_cents = int(round(state["top_up_amount_usd"] * 100))
    threshold_cents = int(round(state.get("threshold_usd", 11.0) * 100))
    anchor_day_prior_cents = int(state.get("anchor_day_prior_spend_cents", 0))

    top_up_dt = datetime.fromisoformat(state["top_up_at"].replace("Z", "+00:00"))
    day_start = top_up_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    gross_spent_cents = fetch_spent_cents(api_key, _iso(day_start))
    net_spent_cents = max(0, gross_spent_cents - anchor_day_prior_cents)
    remaining_cents = top_up_cents - net_spent_cents

    log.info(
        "top_up=%s anchor_day_prior=%s gross_since_anchor_day=%s net_post_anchor=%s remaining=%s threshold=%s",
        _fmt_money(top_up_cents),
        _fmt_money(anchor_day_prior_cents),
        _fmt_money(gross_spent_cents),
        _fmt_money(net_spent_cents),
        _fmt_money(remaining_cents),
        _fmt_money(threshold_cents),
    )

    if remaining_cents >= threshold_cents:
        return 0

    dedup_hours = state.get("dedup_hours", DEFAULT_DEDUP_HOURS)
    last_alert = state.get("last_alert_at")
    if last_alert:
        last_dt = datetime.fromisoformat(last_alert.replace("Z", "+00:00"))
        age_hours = (_now() - last_dt).total_seconds() / 3600
        if age_hours < dedup_hours:
            log.info(
                "threshold breached but alert suppressed: last alert %.1fh ago, dedup=%sh",
                age_hours,
                dedup_hours,
            )
            return 0

    phone = state.get("alert_phone")
    if not phone:
        log.error("alert_phone not set in state")
        return 2

    msg = (
        f"Claude API credit low: {_fmt_money(remaining_cents)} remaining "
        f"(threshold {_fmt_money(threshold_cents)}, anchor {_fmt_money(top_up_cents)} "
        f"on {state['top_up_at']}). Top up, then run claude_balance_monitor.py --topup <amount>."
    )
    result = send_sms(phone, msg)
    if not result.ok:
        log.error("SMS send failed: %s %s", result.error_title, result.error_detail)
        return 3

    state["last_alert_at"] = _iso(_now())
    state["last_alerted_balance_usd"] = round(remaining_cents / 100, 2)
    _save_state(state)
    log.info("alert SMS sent to %s: id=%s", phone, result.message_id)
    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group()
    g.add_argument("--topup", type=float, help="record a new top-up (USD) and reset alert state")
    g.add_argument("--show", action="store_true", help="print state and exit")
    args = p.parse_args()

    state = _load_state()
    if args.show:
        return cmd_show(state)
    if args.topup is not None:
        return cmd_topup(state, args.topup)
    return cmd_check(state)


if __name__ == "__main__":
    sys.exit(main())
