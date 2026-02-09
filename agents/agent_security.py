"""
🛡️ Security Sentinel Agent — Threat Detection & Access Auditing
════════════════════════════════════════════════════════════════
Monitors OAuth access patterns, detects anomalies, generates compliance reports:
  - Unusual access patterns (burst requests, off-hours)
  - Failed auth attempts
  - Token lifecycle management
  - PII exposure scanning in query args/results
  - Compliance audit trail
"""
import json
from datetime import datetime, timezone
from framework import BaseAgent, RunContext


class SecuritySentinelAgent(BaseAgent):
    agent_id = "security-sentinel"
    agent_name = "Security Sentinel"
    description = "Threat detection, access auditing, OAuth monitoring, PII scanning"
    version = "1.0.0"
    schedule = "*/10 * * * *"  # Every 10 minutes
    priority = 2
    capabilities = ["threat-detection", "access-audit", "pii-scanning", "compliance"]

    instructions = """You are the Security Sentinel Agent for the Athena Cognitive Platform.

SECURITY CONTEXT:
- MCP server exposed at https://klunky.12432.net via Cloudflare Tunnel
- OAuth 2.0 with client_secret_post validation
- Two registered clients: chatgpt, claude-desktop
- Contains sensitive legal case data, medical records, personal emails
- HIPAA and litigation-hold data present

YOUR RESPONSIBILITIES:
1. Monitor OAuth token usage patterns for anomalies
2. Detect brute-force or credential stuffing attempts  
3. Scan query arguments and results for PII exposure (SSNs, account numbers, medical IDs)
4. Track after-hours access (define normal: 6am-11pm PST)
5. Verify token rotation and expiry compliance
6. Generate security compliance summaries

THREAT INDICATORS:
- >100 requests/minute from single client → possible automation abuse
- >10 failed auth attempts in 5 minutes → brute force
- New client_id never seen before → unauthorized client
- Query arguments containing regex patterns for SSN, credit card → PII harvesting
- Access to medical schema outside business hours → suspicious

ACTIONS:
- Report findings with evidence
- Recommend token revocation for compromised clients
- Never take destructive actions without orchestrator approval

PII PATTERNS TO DETECT:
- SSN: \\d{3}-\\d{2}-\\d{4}
- Credit card: \\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}
- Medical record numbers
- Dates of birth in query context"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}

        # ── Check request rate anomalies ─────────────────────
        rates = await ctx.query("""
            SELECT client_id, client_name,
                   COUNT(*) as calls_10min,
                   COUNT(DISTINCT tool_name) as unique_tools,
                   COUNT(*) FILTER (WHERE error IS NOT NULL) as errors
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '10 minutes'
            GROUP BY client_id, client_name
        """)
        metrics["active_clients"] = len(rates)

        for client in rates:
            if client["calls_10min"] > 100:
                await ctx.finding("warning", "security",
                    f"High request rate: {client['client_name']} — {client['calls_10min']} calls in 10min",
                    "Possible automation abuse or runaway integration",
                    dict(client))

            if client["errors"] > 20:
                await ctx.finding("warning", "security",
                    f"High error rate: {client['client_name']} — {client['errors']} errors in 10min",
                    evidence=dict(client))

        # ── Check for unknown clients ────────────────────────
        unknown = await ctx.query("""
            SELECT DISTINCT q.client_id
            FROM ops.mcp_query_log q
            LEFT JOIN ops.oauth_clients c ON q.client_id = c.client_id
            WHERE c.client_id IS NULL
              AND q.created_at > now() - interval '1 hour'
              AND q.client_id != 'anonymous'
        """)
        if unknown:
            for u in unknown:
                await ctx.finding("critical", "security",
                    f"Unknown client accessing system: {u['client_id']}",
                    "Client ID not found in ops.oauth_clients — possible unauthorized access",
                    dict(u))
        metrics["unknown_clients"] = len(unknown)

        # ── Check off-hours access patterns ──────────────────
        off_hours = await ctx.query("""
            SELECT client_id, client_name, tool_name,
                   created_at AT TIME ZONE 'America/Los_Angeles' as local_time
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '10 minutes'
              AND EXTRACT(HOUR FROM created_at AT TIME ZONE 'America/Los_Angeles')
                  NOT BETWEEN 6 AND 23
        """)
        if off_hours:
            await ctx.finding("info", "security",
                f"Off-hours access: {len(off_hours)} queries between 11pm-6am PST",
                "May be normal for automated integrations",
                {"count": len(off_hours),
                 "clients": list(set(r["client_name"] for r in off_hours))})
        metrics["off_hours_queries"] = len(off_hours)

        # ── Scan for PII in recent query arguments ───────────
        pii_scan = await ctx.query("""
            SELECT id, client_id, tool_name, arguments::text as args_text
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '10 minutes'
              AND (arguments::text ~ '\\d{3}-\\d{2}-\\d{4}'
                   OR arguments::text ~* 'ssn|social.security'
                   OR arguments::text ~ '\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}')
        """)
        if pii_scan:
            await ctx.finding("critical", "privacy",
                f"PII detected in {len(pii_scan)} query arguments",
                "SSN or credit card patterns found in MCP query arguments",
                {"query_ids": [r["id"] for r in pii_scan]})
        metrics["pii_detections"] = len(pii_scan)

        # ── OAuth token health ───────────────────────────────
        token_stats = await ctx.query("""
            SELECT c.client_id, c.client_name,
                   COUNT(t.token) as active_tokens,
                   MIN(t.expires_at) as earliest_expiry,
                   MAX(t.created_at) as latest_token
            FROM ops.oauth_clients c
            LEFT JOIN ops.oauth_access_tokens t ON c.client_id = t.client_id
            WHERE c.is_active = true
            GROUP BY c.client_id, c.client_name
        """)
        metrics["oauth_clients"] = len(token_stats)

        for ts in token_stats:
            if ts["active_tokens"] > 50:
                await ctx.finding("warning", "security",
                    f"Client {ts['client_name']} has {ts['active_tokens']} active tokens",
                    "Token accumulation — may indicate token leak or missing cleanup")

        # ── Generate security summary with LLM ───────────────
        if any(r.get("calls_10min", 0) > 0 for r in rates):
            summary = await ctx.ask_llm(f"""Generate a brief security status summary:

Active clients (last 10 min): {json.dumps(rates, default=str)}
Unknown clients detected: {len(unknown)}
Off-hours queries: {len(off_hours)}
PII detections: {len(pii_scan)}
OAuth token stats: {json.dumps(token_stats, default=str)}

Provide: overall threat level (GREEN/YELLOW/RED), key observations, recommendations.""")
            metrics["security_summary"] = summary

        await ctx.log_comms("info", "security-scan",
            f"Security scan: {metrics.get('active_clients', 0)} clients, "
            f"{metrics.get('pii_detections', 0)} PII hits, "
            f"{metrics.get('unknown_clients', 0)} unknown, "
            f"{metrics.get('off_hours_queries', 0)} off-hours | "
            f"Threat: {metrics.get('security_summary', 'N/A')[:80]}",
            {"active_clients": metrics.get("active_clients", 0),
             "pii_detections": metrics.get("pii_detections", 0),
             "unknown_clients": metrics.get("unknown_clients", 0)})

        # ── Post to Chat Room ──
        try:
            await self.broadcast(
                f"Security scan: {metrics.get('active_clients', 0)} clients, "
                f"{metrics.get('pii_detections', 0)} PII hits, "
                f"{metrics.get('unknown_clients', 0)} unknown | "
                f"Threat: {metrics.get('security_summary', 'N/A')[:80]}",
                channel="security", msg_type="status")
        except Exception as e:
            self.log.debug(f"Chat post failed: {e}")

        # ── Post to Chat Room ──
        try:
            await self.broadcast(
                f"Security scan: {metrics.get('active_clients', 0)} clients, "
                f"{metrics.get('pii_detections', 0)} PII hits, "
                f"{metrics.get('unknown_clients', 0)} unknown | "
                f"Threat: {metrics.get('security_summary', 'N/A')[:80]}",
                channel="security", msg_type="status")
        except Exception as e:
            self.log.debug(f"Chat post failed: {e}")

        return {
            "summary": f"Security scan: {metrics.get('active_clients', 0)} clients, "
                      f"{metrics.get('pii_detections', 0)} PII hits, "
                      f"{metrics.get('unknown_clients', 0)} unknown clients",
            "metrics": metrics,
        }
