"""
📧 Graph API Email Utility — Send Email via Microsoft Graph
═══════════════════════════════════════════════════════════
Uses client_credentials flow to send mail as athena@seattleseahawks.me
via Microsoft Graph API.

Requires Azure AD App Permission: Mail.Send (Application)
"""
import json
import time
import logging
from typing import Optional

import httpx

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
) -> dict:
    """
    Send an email via Microsoft Graph API.

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
        save_to_sent: Whether to save in sender's Sent folder

    Returns:
        {"status": "sent", "message_id": "..."} or {"status": "failed", "error": "..."}
    """
    try:
        token = await _get_token(tenant_id, client_id, client_secret)

        # Build message payload
        message = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body_html,
                },
                "toRecipients": [
                    {"emailAddress": {"address": addr}} for addr in to_recipients
                ],
                "importance": importance,
            },
            "saveToSentItems": save_to_sent,
        }

        if cc_recipients:
            message["message"]["ccRecipients"] = [
                {"emailAddress": {"address": addr}} for addr in cc_recipients
            ]

        url = f"{GRAPH_BASE_URL}/users/{sender}/sendMail"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=message)

        if resp.status_code == 202:
            log.info("Email sent successfully: %s → %s", subject, to_recipients)
            return {"status": "sent", "message_id": resp.headers.get("request-id", "")}
        else:
            error_body = resp.text
            log.error("Graph API sendMail failed (%d): %s", resp.status_code, error_body[:500])
            return {"status": "failed", "error": f"HTTP {resp.status_code}: {error_body[:500]}"}

    except Exception as e:
        log.error("Email send exception: %s", e)
        return {"status": "failed", "error": str(e)}


def build_notification_html(
    title: str,
    sections: list[dict],
    footer: str = "Nelson Enterprise Platform — Athena AI Agent",
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
      <div style="font-size:18px;font-weight:700;color:#f8fafc">NELSON ENTERPRISE PLATFORM</div>
      <div style="font-size:12px;color:#64748b;letter-spacing:1px">ATHENA AI — AUTOMATED NOTIFICATION</div>
    </div>
  </div>
  <div style="font-size:20px;font-weight:600;color:#22d3ee;margin-top:12px">
    {title}
  </div>
</div>
"""

    for section in sections:
        stype = section.get("type", "info")
        accent, _, bg = colors.get(stype, colors["info"])
        heading = section.get("heading", "")
        content = section.get("content", "")

        html += f"""
<div style="background:{bg}10;border:1px solid {accent}40;border-radius:8px;padding:16px;margin-bottom:12px">
  <div style="font-size:14px;font-weight:600;color:{accent};margin-bottom:8px">{heading}</div>
  <div style="font-size:13px;color:#cbd5e1;line-height:1.6;white-space:pre-wrap">{content}</div>
</div>
"""

    html += f"""
<!-- Footer -->
<div style="text-align:center;padding:16px;color:#475569;font-size:11px;border-top:1px solid #1e293b;margin-top:16px">
  {footer}<br>
  <span style="color:#334155">Powered by GPT Codex · Microsoft Graph API · PostgreSQL</span>
</div>

</div>
</body>
</html>"""

    return html
