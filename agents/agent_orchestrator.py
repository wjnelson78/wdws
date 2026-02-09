"""
🎯 Orchestrator Agent — Fleet Manager & Governor
═════════════════════════════════════════════════
The supreme coordinator of all agents:
  - Schedules and dispatches agents based on cron + priority
  - Reviews and challenges agent findings before action
  - Validates agent recommendations via LLM cross-check
  - Manages concurrency and rate limits
  - Generates fleet status reports
  - Escalates critical findings
  - Resolves conflicting agent recommendations
"""
import json
import asyncio
from datetime import datetime, timezone
from framework import BaseAgent, RunContext


class OrchestratorAgent(BaseAgent):
    agent_id = "orchestrator"
    agent_name = "Orchestrator"
    description = "Fleet manager: schedules agents, validates findings, resolves conflicts, escalates"
    version = "1.0.0"
    schedule = "*/5 * * * *"  # Every 5 minutes
    priority = 0  # Highest priority — runs first
    capabilities = [
        "fleet-management", "finding-validation", "conflict-resolution",
        "escalation", "fleet-report", "agent-dispatch", "challenge-review"
    ]

    instructions = """You are the Orchestrator — the Manager of all agents in the Nelson Enterprise WDWS system.

YOUR ROLE:
You oversee a fleet of specialized agents. You do NOT perform their work directly.
Instead, you coordinate, validate, challenge, and escalate.

FLEET (by priority):
  P0: Orchestrator (you) — fleet management
  P1: Watchdog — health monitoring, service restarts
  P2: Security Sentinel — threat detection, anomaly alerts
  P2: Self-Healing — auto-recovery, integration tests
  P3: Data Quality — embedding/chunk/doc integrity
  P3: Email Triage — email classification and urgency
  P4: Database Tuner — query optimization, vacuum
  P4: Case Strategy — legal morning brief
  P4: Retention & Redaction — data governance, PII
  P5: Timeline — case chronology
  P5: Query Insight — usage analytics

GOVERNANCE RULES:
1. CHALLENGE: When an agent proposes a destructive action (delete, drop, purge), 
   you MUST review it. If the action affects >100 records or any user data, 
   mark the finding as "needs_review" and do NOT auto-approve.
   
2. VALIDATE: Cross-check agent findings against each other. 
   Example: If Data Quality says "delete duplicate docs" but Retention says 
   those docs are under litigation hold — BLOCK the deletion.

3. CONFLICT RESOLUTION: When two agents disagree:
   - Safety wins over efficiency (keep data vs. purge)
   - Security wins over convenience (block access vs. allow)
   - Legal compliance wins over everything
   
4. ESCALATION: Create a "critical" finding when:
   - Any agent has 3+ consecutive failures
   - Security threat level is RED
   - Multiple agents report related issues
   - Any finding affects litigation-held data

5. FLEET HEALTH:
   - Agents that haven't run in 2x their schedule interval are "stale"
   - Agents with error_count > run_count * 0.3 are "unhealthy"
   - Disabled agents should be flagged for review

6. MORNING BRIEF (6 AM runs):
   Generate a consolidated fleet report with:
   - Critical findings from last 24h
   - Agent health summary
   - Recommended actions requiring human approval
   - System health metrics

NEVER:
- Override a litigation hold
- Approve bulk deletions of case documents
- Modify agent code (only configuration)
- Bypass security findings
- Run more than 3 agents concurrently"""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {
            "agents_reviewed": 0,
            "findings_validated": 0,
            "findings_challenged": 0,
            "conflicts_resolved": 0,
            "escalations": 0,
        }

        # ── 1. Fleet Health Check ────────────────────────────
        agents = await ctx.query("""
            SELECT ar.id, ar.name, ar.schedule, ar.is_active,
                   ar.last_run_at, ar.last_status, ar.run_count, ar.error_count,
                   ar.priority
            FROM ops.agent_registry ar
            WHERE ar.id != 'orchestrator'
            ORDER BY ar.priority, ar.name
        """)
        metrics["agents_reviewed"] = len(agents)

        stale_agents = []
        unhealthy_agents = []
        disabled_agents = []

        for agent in agents:
            if not agent["is_active"]:
                disabled_agents.append(agent["name"])
                continue

            # Check staleness (hasn't run in 2x expected interval)
            if agent["last_run_at"]:
                since_last = (datetime.now(timezone.utc) - agent["last_run_at"]).total_seconds()
                # Simple heuristic: if daily agent hasn't run in 48h, it's stale
                if since_last > 172800:  # 48 hours
                    stale_agents.append(agent["name"])

            # Check error rate
            if agent["run_count"] and agent["run_count"] > 5:
                error_rate = (agent["error_count"] or 0) / agent["run_count"]
                if error_rate > 0.3:
                    unhealthy_agents.append(
                        f"{agent['name']} ({agent['error_count']}/{agent['run_count']} errors)")

        if stale_agents:
            await ctx.finding("warning", "fleet-health",
                f"Stale agents: {', '.join(stale_agents)}",
                "These agents haven't run in over 2x their expected interval")

        if unhealthy_agents:
            await ctx.finding("warning", "fleet-health",
                f"Unhealthy agents (>30% error rate): {', '.join(unhealthy_agents)}",
                "High error rate may indicate misconfiguration or external failures")

        if disabled_agents:
            await ctx.finding("info", "fleet-health",
                f"Disabled agents: {', '.join(disabled_agents)}",
                "Review whether these should be re-enabled")

        # ── 2. Check for consecutive failures ────────────────
        await ctx.log_comms("info", "fleet-health",
            f"Fleet check: {len(agents)} agents, "
            f"{len(stale_agents)} stale, {len(unhealthy_agents)} unhealthy",
            {"agents": len(agents), "stale": stale_agents,
             "unhealthy": unhealthy_agents})

        failure_streaks = await ctx.query("""
            WITH ranked AS (
                SELECT agent_id,
                       status,
                       ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY started_at DESC) as rn
                FROM ops.agent_runs
                WHERE started_at > now() - interval '24 hours'
            )
            SELECT agent_id, COUNT(*) as consecutive_failures
            FROM ranked
            WHERE rn <= 3 AND status = 'error'
            GROUP BY agent_id
            HAVING COUNT(*) >= 3
        """)

        for streak in failure_streaks:
            metrics["escalations"] += 1
            await ctx.finding("critical", "agent-failure",
                f"Agent '{streak['agent_id']}' has {streak['consecutive_failures']} consecutive failures",
                "This agent needs immediate attention — may have a configuration or dependency issue")

        # ── 3. Validate pending findings ─────────────────────
        pending = await ctx.query("""
            SELECT f.id, f.agent_id, f.severity, f.category, 
                   f.title, f.detail, f.evidence,
                   f.created_at
            FROM ops.agent_findings f
            WHERE f.status = 'open'
              AND f.severity IN ('warning', 'critical')
              AND f.created_at > now() - interval '24 hours'
            ORDER BY 
                CASE f.severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                f.created_at DESC
            LIMIT 30
        """)
        metrics["findings_validated"] = len(pending)

        # ── 4. Cross-check for conflicts ─────────────────────
        # Check: Data Quality deletion vs Retention holds
        quality_deletes = [f for f in pending
                          if f["agent_id"] == "data-quality"
                          and "delete" in (f["detail"] or "").lower()]
        retention_holds = [f for f in pending
                         if f["agent_id"] == "retention"
                         and "hold" in (f["category"] or "").lower()]

        if quality_deletes and retention_holds:
            metrics["conflicts_resolved"] += 1
            for qd in quality_deletes:
                await ctx.execute("""
                    UPDATE ops.agent_findings SET status = 'needs_review',
                    detail = detail || ' [ORCHESTRATOR: Blocked — conflicts with active litigation hold]'
                    WHERE id = $1
                """, qd["id"])
                await ctx.action(
                    f"Blocked data-quality finding #{qd['id']}: conflicts with retention hold")

        # ── 5. Challenge high-impact findings ────────────────
        high_impact = [f for f in pending if f["severity"] == "critical"]

        if high_impact and len(high_impact) <= 5:
            finding_summaries = "\n".join(
                f"- [{f['agent_id']}] {f['title']}: {f.get('detail', '')[:200]}"
                for f in high_impact
            )

            challenge = await ctx.ask_llm_json(f"""You are the Orchestrator reviewing critical findings.

CRITICAL FINDINGS:
{finding_summaries}

For each finding, assess:
1. Is the severity appropriate? (over-classified or under-classified?)
2. Should this be auto-resolved or require human review?
3. Are there any conflicting findings?
4. What's the actual risk level on 1-10 scale?

Return JSON:
{{
  "assessments": [
    {{
      "finding_title": "...",
      "severity_correct": true/false,
      "recommended_severity": "critical|warning|info",
      "auto_resolve": true/false,
      "risk_score": 1-10,
      "notes": "..."
    }}
  ],
  "overall_threat_level": "GREEN|YELLOW|RED",
  "recommended_actions": ["..."]
}}""")

            if challenge and "assessments" in challenge:
                for assessment in challenge["assessments"]:
                    if not assessment.get("severity_correct"):
                        metrics["findings_challenged"] += 1
                        await ctx.finding("info", "governance",
                            f"Challenged: {assessment.get('finding_title', 'unknown')}",
                            f"Recommended severity: {assessment.get('recommended_severity')}. "
                            f"Risk: {assessment.get('risk_score', '?')}/10. "
                            f"Notes: {assessment.get('notes', '')}",
                            assessment)

                if challenge.get("overall_threat_level") == "RED":
                    metrics["escalations"] += 1
                    await ctx.finding("critical", "escalation",
                        "THREAT LEVEL RED — Multiple critical findings require attention",
                        json.dumps(challenge.get("recommended_actions", []), indent=2),
                        challenge)

        # ── 6. Process inter-agent messages ──────────────────
        messages = await ctx.query("""
            SELECT id, from_agent, to_agent, msg_type, body, created_at
            FROM ops.agent_messages
            WHERE to_agent = 'orchestrator' AND status = 'pending'
            ORDER BY created_at
            LIMIT 20
        """)

        for msg in messages:
            await ctx.execute(
                "UPDATE ops.agent_messages SET processed_at = now(), status = 'processed' WHERE id = $1",
                msg["id"])

            if msg["msg_type"] == "escalation":
                metrics["escalations"] += 1
                await ctx.finding("critical", "agent-escalation",
                    f"Escalation from {msg['from_agent']}",
                    json.dumps(msg["body"]) if isinstance(msg["body"], dict) else str(msg["body"]))

        # ── 7. Fleet Status Report ───────────────────────────
        recent_runs = await ctx.query("""
            SELECT agent_id, status, 
                   COUNT(*) as count,
                   AVG(duration_ms) as avg_ms
            FROM ops.agent_runs
            WHERE started_at > now() - interval '24 hours'
            GROUP BY agent_id, status
            ORDER BY agent_id
        """)

        open_findings_count = await ctx.query("""
            SELECT severity, COUNT(*) as count
            FROM ops.agent_findings
            WHERE status = 'open'
            GROUP BY severity
        """)

        fleet_status = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "agents_active": sum(1 for a in agents if a["is_active"]),
            "agents_total": len(agents),
            "stale": stale_agents,
            "unhealthy": unhealthy_agents,
            "runs_24h": {r["agent_id"]: {"status": r["status"], "count": r["count"],
                                          "avg_ms": float(r["avg_ms"]) if r["avg_ms"] else 0}
                         for r in recent_runs},
            "open_findings": {r["severity"]: r["count"] for r in open_findings_count},
            "findings_validated": metrics["findings_validated"],
            "findings_challenged": metrics["findings_challenged"],
            "escalations": metrics["escalations"],
        }

        await ctx.finding("info", "fleet-report",
            "Fleet Status Report",
            json.dumps(fleet_status, indent=2, default=str),
            fleet_status)

        await ctx.log_comms("info", "fleet-report",
            f"Fleet: {fleet_status['agents_active']}/{fleet_status['agents_total']} active | "
            f"{metrics['findings_validated']} findings | "
            f"{metrics['findings_challenged']} challenged | "
            f"{metrics['escalations']} escalations",
            {"active": fleet_status.get("agents_active"),
             "total": fleet_status.get("agents_total"),
             "findings_validated": metrics["findings_validated"],
             "escalations": metrics["escalations"]})

        # ── Post to Chat Room ──
        try:
            summary = (
                f"Fleet: {fleet_status['agents_active']}/{fleet_status['agents_total']} active | "
                f"{metrics['findings_validated']} reviewed | "
                f"{metrics['escalations']} escalations"
            )
            await self.broadcast(summary, channel="general", msg_type="status")
            if metrics["escalations"] > 0:
                await self.broadcast(
                    f"⚠️ {metrics['escalations']} escalation(s) require attention!",
                    channel="alerts", msg_type="alert")
        except Exception as e:
            self.log.debug(f"Chat post failed: {e}")

        # ── 8. Issue Chat Room Directives ────────────────────
        # Post @mention directives so agents wake up (via notification watcher)
        # and take immediate action on critical items.
        try:
            # 8a. Direct agents with critical findings to investigate
            critical_by_agent = {}
            for f in pending:
                if f["severity"] == "critical":
                    aid = f["agent_id"]
                    critical_by_agent.setdefault(aid, []).append(f)

            for aid, findings in critical_by_agent.items():
                titles = "; ".join(f["title"][:80] for f in findings[:3])
                await self.ask_agent(
                    aid,
                    f"You have {len(findings)} critical finding(s) that need attention: "
                    f"{titles}. Please investigate and report back.",
                    channel="ops"
                )
                self.log.info("Directed @%s to address %d critical findings", aid, len(findings))

            # 8b. Notify team about stale/down agents
            if stale_agents:
                stale_names = ", ".join(f"@{a['id']}" for a in stale_agents[:5])
                stale_mentions = [a["id"] for a in stale_agents[:5]]
                await self.chat(
                    f"Team awareness: {stale_names} — you appear stale (no run in 15+ min). "
                    f"Please check in. @self-healing — can you verify these agents are healthy?",
                    channel="ops",
                    mentions=stale_mentions + ["self-healing"],
                    msg_type="directive"
                )

            # 8c. Route escalations to relevant agents via chat
            for msg in messages:
                if msg["msg_type"] == "escalation":
                    body = msg["body"] if isinstance(msg["body"], dict) else {}
                    from_agent = msg["from_agent"]
                    # Post to ops channel so the whole team is aware
                    await self.chat(
                        f"Escalation from @{from_agent}: {body.get('reason', str(msg['body'])[:200])}. "
                        f"Team — please review and assist.",
                        channel="ops",
                        mentions=[from_agent, "self-healing", "watchdog"],
                        msg_type="escalation"
                    )

            # 8d. If unhealthy agents found, direct self-healing
            if unhealthy_agents:
                names = ", ".join(f"@{a['id']}" for a in unhealthy_agents[:5])
                await self.ask_agent(
                    "self-healing",
                    f"Unhealthy agents detected: {names}. "
                    f"Please run diagnostics and attempt auto-recovery.",
                    channel="ops"
                )

        except Exception as e:
            self.log.debug(f"Chat directives failed: {e}")

        return {
            "summary": (
                f"Fleet: {fleet_status['agents_active']}/{fleet_status['agents_total']} active, "
                f"{metrics['findings_validated']} reviewed, "
                f"{metrics['findings_challenged']} challenged, "
                f"{metrics['escalations']} escalations"
            ),
            "metrics": metrics,
        }
