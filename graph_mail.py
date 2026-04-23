"""Shared helpers for sending RFC-5322 MIME via Microsoft Graph /sendMail.

Graph's JSON sendMail with body.contentType="HTML" lets Exchange convert the
HTML to MIME using Windows-1252 + quoted-printable. A small number of strict
recipient tenants (notably Defender-for-Office 365 with aggressive content
inspection) strip those bodies, delivering a blank message.

Building raw RFC-5322 MIME in UTF-8 + base64 ourselves and POSTing it to Graph
/sendMail bypasses Exchange's re-encoding — Graph passes our MIME through
unchanged and the message lands cleanly at strict recipients.

Usage:
    from graph_mail import build_rfc5322_mime, graph_send_raw_mime

    mime_bytes = build_rfc5322_mime(
        sender="athena@example.com",
        sender_display_name="Athena AI",
        to_recipients=["user@example.com"],
        cc_recipients=[],
        bcc_recipients=[],
        subject="Hello",
        body_html="<p>Hi</p>",
        body_text="Hi",
    )
    await graph_send_raw_mime(token, "athena@example.com", mime_bytes)
"""

from __future__ import annotations

import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from typing import Optional, Sequence

import httpx

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

_PRIORITY_HEADER_MAP = {
    "Low": "5 (Lowest)",
    "Normal": "3 (Normal)",
    "High": "1 (Highest)",
}


def build_rfc5322_mime(
    *,
    sender: str,
    sender_display_name: str = "",
    to_recipients: Sequence[str],
    cc_recipients: Sequence[str] = (),
    bcc_recipients: Sequence[str] = (),
    subject: str,
    body_html: str,
    body_text: str,
    importance: str = "Normal",
    request_read_receipt: bool = False,
    request_delivery_receipt: bool = False,
    in_reply_to: Optional[str] = None,
    references: Optional[str] = None,
    extra_headers: Optional[dict[str, str]] = None,
) -> bytes:
    """Build an RFC-5322 multipart/alternative MIME message.

    Both parts (text/plain and text/html) are encoded as UTF-8 base64, which
    preserves arbitrary Unicode and survives mail-hygiene strip rules that
    target Windows-1252 + quoted-printable.

    Args:
        sender: The authenticated mailbox address (goes in From:).
        sender_display_name: Display name for the From header; omit if unknown.
        to_recipients: Addresses for the To: header.
        cc_recipients: Addresses for the Cc: header (may be empty).
        bcc_recipients: Addresses for the Bcc: header (may be empty).
        subject: Message subject (UTF-8 safe).
        body_html: HTML body.
        body_text: Plain-text fallback body.
        importance: "Low", "Normal", or "High" (title-cased).
        request_read_receipt: Adds Disposition-Notification-To.
        request_delivery_receipt: Adds Return-Receipt-To.
        in_reply_to: Optional In-Reply-To header value (the original
            message's Message-ID, including angle brackets).
        references: Optional References header value (space-separated chain
            of Message-IDs for threading).
        extra_headers: Any additional custom headers to include.

    Returns:
        The MIME message serialized as bytes, ready to be base64-encoded for
        POST to Graph /sendMail.
    """
    msg = MIMEMultipart("alternative")
    msg["From"] = formataddr((sender_display_name or "", sender))
    msg["To"] = ", ".join(to_recipients)
    if cc_recipients:
        msg["Cc"] = ", ".join(cc_recipients)
    if bcc_recipients:
        msg["Bcc"] = ", ".join(bcc_recipients)
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=False)
    sender_domain = sender.split("@", 1)[-1] if "@" in sender else "localhost"
    msg["Message-ID"] = make_msgid(domain=sender_domain)
    msg["MIME-Version"] = "1.0"

    msg["Importance"] = importance
    msg["X-Priority"] = _PRIORITY_HEADER_MAP.get(importance, "3 (Normal)")

    if request_read_receipt:
        msg["Disposition-Notification-To"] = sender
    if request_delivery_receipt:
        msg["Return-Receipt-To"] = sender

    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
    if references:
        msg["References"] = references

    if extra_headers:
        for k, v in extra_headers.items():
            if k in msg:
                del msg[k]
            msg[k] = v

    msg.attach(MIMEText(body_text or "", "plain", "utf-8"))
    msg.attach(MIMEText(body_html or "", "html", "utf-8"))

    return msg.as_bytes()


async def graph_send_raw_mime(
    token: str,
    mailbox: str,
    mime_bytes: bytes,
    *,
    timeout: float = 120.0,
) -> None:
    """POST a raw RFC-5322 MIME message to Graph /sendMail.

    Graph accepts raw MIME when the request Content-Type is text/plain and the
    body is the base64-encoded MIME bytes.

    Raises RuntimeError on non-2xx responses.
    """
    url = f"{GRAPH_BASE_URL}/users/{mailbox}/sendMail"
    body_b64 = base64.b64encode(mime_bytes).decode("ascii")
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "text/plain",
            },
            content=body_b64,
        )
    if resp.status_code not in (200, 202):
        raise RuntimeError(
            f"Graph raw-MIME sendMail failed: HTTP {resp.status_code} "
            f"{resp.text[:500]}"
        )


async def graph_fetch_display_name(
    token: str,
    mailbox: str,
    *,
    timeout: float = 30.0,
) -> str:
    """Fetch a mailbox's displayName from Graph. Returns '' on failure."""
    url = f"{GRAPH_BASE_URL}/users/{mailbox}?$select=displayName"
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers={"Authorization": f"Bearer {token}"})
        if resp.status_code == 200:
            return (resp.json().get("displayName") or "").strip()
    except Exception:
        pass
    return ""
