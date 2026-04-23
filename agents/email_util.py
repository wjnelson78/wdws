"""
📧 Graph API Email Utility — Send Email via Microsoft Graph
═══════════════════════════════════════════════════════════
Uses client_credentials flow to send mail as athena@seattleseahawks.me
via Microsoft Graph API.

HTML sends go out as raw RFC-5322 UTF-8 + base64 multipart/alternative
(via the shared graph_mail module) so strict recipient mail hygiene
doesn't strip the body.

Requires Azure AD App Permission: Mail.Send (Application)
"""
import html as _html
import json
import re
import sys
import time
import logging
from typing import Optional

import httpx

sys.path.insert(0, "/opt/wdws")
from graph_mail import (
    build_rfc5322_mime,
    graph_send_raw_mime,
    graph_fetch_display_name,
)

log = logging.getLogger("agents.email")

# Azure AD / Graph API constants
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

# Cached token
_token_cache = {"access_token": None, "expires_at": 0}


async def _get_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Acquire or refresh an app-only access token."""
    global _token_cache

    # Return cached token if still valid (with 5 min buffer)
    if _token_cache["access_token"] and time.time() < _token_cache["expires_at"] - 300:
        return _token_cache["access_token"]

    url = TOKEN_URL.format(tenant=tenant_id)
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, data=data)
        resp.raise_for_status()
        token_data = resp.json()

    _token_cache["access_token"] = token_data["access_token"]
    _token_cache["expires_at"] = time.time() + token_data.get("expires_in", 3600)

    log.info("Acquired Graph API token (expires in %ds)", token_data.get("expires_in", 3600))
    return _token_cache["access_token"]


def _html_to_text(value: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", value or "", flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = _html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


async def send_email(
    *,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    sender: str,
    to_recipients: list[str],
    subject: str,
    body_html: str,
    cc_recipients: list[str] = None,
    importance: str = "normal",
    save_to_sent: bool = True,
    body_text: Optional[str] = None,
    sender_display_name: Optional[str] = None,
) -> dict:
    """Send an HTML email via Graph API using raw RFC-5322 MIME (UTF-8, base64).

    Raw MIME bypasses Exchange's Windows-1252 + quoted-printable re-encoding
    that strict recipient tenants strip (blank-body delivery).

    Args:
        tenant_id: Azure AD tenant ID
        client_id: Azure AD application ID
        client_secret: Application client secret
        sender: Email address to send from (e.g., athena@seattleseahawks.me)
        to_recipients: List of recipient email addresses
        subject: Email subject line
        body_html: HTML body content
        cc_recipients: Optional CC recipients
        importance: "low", "normal", or "high"
        save_to_sent: Unused; Graph always saves raw-MIME sends to Sent Items.
        body_text: Optional plain-text fallback. If omitted, derived from body_html.
        sender_display_name: Optional display name for From header. If omitted,
            fetched from Graph /users/{sender}?$select=displayName.

    Returns:
        {"status": "sent", "transport": "raw_mime_utf8_base64", ...}
        or {"status": "failed", "error": "..."}
    """
    try:
        token = await _get_token(tenant_id, client_id, client_secret)

        display_name = sender_display_name
        if display_name is None:
            display_name = await graph_fetch_display_name(token, sender)

        imp = (importance or "normal").strip().capitalize()
        if imp not in {"Low", "Normal", "High"}:
            imp = "Normal"

        text_fallback = body_text or _html_to_text(body_html)

        mime_bytes = build_rfc5322_mime(
            sender=sender,
            sender_display_name=display_name,
            to_recipients=list(to_recipients),
            cc_recipients=list(cc_recipients or []),
            subject=subject,
            body_html=body_html,
            body_text=text_fallback,
            importance=imp,
        )
        await graph_send_raw_mime(token, sender, mime_bytes)

        log.info("Email sent successfully: %s → %s", subject, to_recipients)
        return {
            "status": "sent",
            "transport": "raw_mime_utf8_base64",
            "to": list(to_recipients),
            "cc": list(cc_recipients or []),
            "subject": subject,
        }

    except Exception as e:
        log.error("Email send exception: %s", e)
        return {"status": "failed", "error": str(e)}


def build_notification_html(
    title: str,
    sections: list[dict],
    footer: str = "Athena Cognitive Engine - Developed by William Nelson 2016",
) -> str:
    """
    Build a styled HTML email for agent notifications.

    Args:
        title: Main heading
        sections: List of {"heading": "...", "content": "...", "type": "info|warning|error|success"}
        footer: Footer text

    Returns:
        Complete HTML email string
    """
    colors = {
        "info": ("#0ea5e9", "#0c4a6e", "#e0f2fe"),
        "warning": ("#f59e0b", "#78350f", "#fef3c7"),
        "error": ("#ef4444", "#7f1d1d", "#fee2e2"),
        "success": ("#22c55e", "#14532d", "#dcfce7"),
        "code": ("#a855f7", "#3b0764", "#f3e8ff"),
    }

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#0f172a;color:#e2e8f0;font-family:'Segoe UI',system-ui,sans-serif">
<div style="max-width:640px;margin:0 auto;padding:24px">

<!-- Header -->
<div style="background:linear-gradient(135deg,#1e293b,#0f172a);border:1px solid #334155;border-radius:12px;padding:24px;margin-bottom:16px">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
    <span style="font-size:28px">🏛️</span>
    <div>
      <div style="font-size:18px;font-weight:700;color:#f8fafc">ATHENA COGNITIVE PLATFORM</div>
      <div style="font-size:12px;color:#64748b;letter-spacing:1px">ATHENA AI — AUTOMATED NOTIFICATION</div>
    </div>
  </div>
  <div style="font-size:20px;font-weight:600;color:#22d3ee;margin-top:12px">
    {title}
  </div>
</div>
"""

    status_badges = {
        "success": ("✅", "Resolved automatically — no action required"),
        "error":   ("🔴", "Needs manual attention"),
        "warning": ("🔍", "Awaiting review — no changes applied"),
        "info":    ("ℹ️",  "For your information"),
        "code":    ("🔧", "Code change recorded"),
    }

    for section in sections:
        stype = section.get("type", "info")
        accent, _, bg = colors.get(stype, colors["info"])
        heading = section.get("heading", "")
        content = section.get("content", "")
        plain_summary = section.get("plain_summary", "")

        if plain_summary:
            badge_icon, badge_label = status_badges.get(stype, status_badges["info"])
            html += f"""
<div style="background:{bg}10;border:1px solid {accent}40;border-radius:10px;padding:20px;margin-bottom:16px">
  <div style="font-size:12px;font-weight:700;color:{accent};text-transform:uppercase;letter-spacing:0.8px;margin-bottom:14px">{heading}</div>
  <div style="font-size:15px;color:#f1f5f9;line-height:1.8;margin-bottom:16px">{plain_summary}</div>
  <div style="display:inline-block;font-size:12px;font-weight:600;color:{accent};padding:6px 14px;background:{accent}18;border-radius:20px;margin-bottom:14px">{badge_icon}&nbsp; {badge_label}</div>
  <div style="border-top:1px solid {accent}20;padding-top:12px">
    <details>
      <summary style="cursor:pointer;font-size:12px;color:#64748b;user-select:none;list-style:none;outline:none">▶ View technical details</summary>
      <div style="font-size:12px;color:#94a3b8;line-height:1.6;white-space:pre-wrap;font-family:'Consolas','Courier New',monospace;margin-top:12px;padding:14px;background:#0a1120;border-radius:6px;border:1px solid #1e293b;overflow-x:auto">{content}</div>
    </details>
  </div>
</div>
"""
        else:
            html += f"""
<div style="background:{bg}10;border:1px solid {accent}40;border-radius:8px;padding:16px;margin-bottom:12px">
  <div style="font-size:14px;font-weight:600;color:{accent};margin-bottom:8px">{heading}</div>
  <div style="font-size:13px;color:#cbd5e1;line-height:1.6;white-space:pre-wrap">{content}</div>
</div>
"""

    html += f"""
<!-- Footer -->
<div style="text-align:center;padding:16px;color:#475569;font-size:11px;border-top:1px solid #1e293b;margin-top:16px">
  {footer}
</div>

</div>
</body>
</html>"""

    return html
