#!/usr/bin/env python3
"""Test email sending via Graph API."""
import asyncio
from email_util import send_email, build_notification_html
from config import GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, GRAPH_SENDER_EMAIL

async def test():
    html = build_notification_html(
        'Code Doctor — Test Notification',
        [{'heading': 'System Status', 'body': (
            'This is a test email from the Athena Cognitive Platform agent system.<br><br>'
            '<b>Model:</b> gpt-5.2-codex<br>'
            '<b>Reasoning Effort:</b> medium<br>'
            '<b>Agents:</b> 12 registered<br>'
            '<b>Code Doctor:</b> Operational'
        )}]
    )
    result = await send_email(
        tenant_id=GRAPH_TENANT_ID,
        client_id=GRAPH_CLIENT_ID,
        client_secret=GRAPH_CLIENT_SECRET,
        sender=GRAPH_SENDER_EMAIL,
        to_recipients=['william@seattleseahawks.me'],
        subject='ATHENA COGNITIVE PLATFORM: Test Email',
        body_html=html
    )
    print(f'Result: {result}')

asyncio.run(test())
