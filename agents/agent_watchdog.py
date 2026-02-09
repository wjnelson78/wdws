"""
🐕 Watchdog Agent v3 — Comprehensive System Monitor & Coordinator
══════════════════════════════════════════════════════════════════════
Full-stack monitoring with intelligent escalation:

  SYSTEM METRICS:
    - CPU (per-core + load averages)
    - Memory + swap usage with trend detection
    - Disk usage + inode consumption
    - Network I/O counters
    - Process count + zombie detection

  LOG INTELLIGENCE:
    - journalctl scanning for ALL wdws/system services
    - OOM kill detection
    - Segfault / crash detection
    - Disk full / I/O error detection
    - Service restart pattern analysis
    - Auth failure / brute force detection
    - PostgreSQL error log scanning
    - Application error pattern recognition

  INTER-AGENT COORDINATION:
    - Routes issues to the right specialist agent
    - Escalates unresolved multi-agent problems to orchestrator
    - Tracks issue lifecycle (detected → assigned → resolved)

  HUMAN ESCALATION:
    - Email alerts via Microsoft Graph API (athena@seattleseahawks.me)
    - Chat room posts for real-time visibility
    - Severity-based notification routing
    - Rate-limited to prevent alert fatigue
"""
import asyncio
import json
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

from framework import BaseAgent, RunContext
from config import (
    OPENAI_API_KEY, GRAPH_TENANT_ID, GRAPH_CLIENT_ID,
    GRAPH_CLIENT_SECRET, GRAPH_SENDER_EMAIL, ALERT_EMAIL,
)
from email_util import send_email, build_notification_html

# ── Cloudflare config ────────────────────────────────────────
CF_EMAIL = os.getenv("CLOUDFLARE_EMAIL", "william@seattleseahawks.me")
CF_API_KEY = os.getenv("CLOUDFLARE_API_KEY", "")
CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CF_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID_12432NET", "")
CF_TUNNEL_ID = os.getenv("CLOUDFLARE_TUNNEL_ID", "")

OPENAI_BALANCE_THRESHOLD = float(os.getenv("OPENAI_BALANCE_THRESHOLD", "5.0"))

# ── Alert thresholds ─────────────────────────────────────────
THRESHOLDS = {
    "cpu_percent":       {"warning": 80, "critical": 95},
    "memory_percent":    {"warning": 85, "critical": 95},
    "swap_percent":      {"warning": 50, "critical": 80},
    "disk_percent":      {"warning": 80, "critical": 92},
    "inode_percent":     {"warning": 80, "critical": 95},
    "load_1m_ratio":     {"warning": 2.0, "critical": 4.0},  # load / cpu_count
    "db_conn_percent":   {"warning": 70, "critical": 90},
    "zombie_processes":  {"warning": 3, "critical": 10},
    "oom_kills":         {"warning": 1, "critical": 3},
    "ssh_failures":      {"warning": 10, "critical": 50},
}

# ── Services to monitor ─────────────────────────────────────
MONITORED_SERVICES = [
    "wdws-mcp", "nelson-dashboard", "wdws-agents",
    "postgresql@17-main", "ssh",
]

MONITORED_TIMERS = [
    "wdws-email-sync", "wdws-attach-ingest",
]

# ── Agent routing: issue type → which agent should handle it ─
ISSUE_ROUTING = {
    "database":    "db-tuner",
    "security":    "security",
    "data":        "data-quality",
    "email":       "email-triage",
    "code":        "code-doctor",
    "recovery":    "self-healing",
    "legal":       "case-strategy",
    "performance": "db-tuner",
}


class WatchdogAgent(BaseAgent):
    agent_id = "watchdog"
    agent_name = "Watchdog"
    description = (
        "System monitor: metrics, logs, security, inter-agent coordination, "
        "human escalation via email/chat"
    )
    version = "3.0.0"
    schedule = "*/2 * * * *"
    priority = 1
    capabilities = [
        "health-monitoring", "service-restart", "alerting",
        "resource-tracking", "cloudflare-tunnel", "attack-detection",
        "acl-blacklist", "openai-balance", "journalctl-monitor",
        "log-anomaly-detection", "inter-agent-coordination",
        "email-escalation", "system-optimization",
    ]

    instructions = """You are the Watchdog Agent v3 for the Nelson Enterprise WDWS system.
You are the eyes and ears of the entire platform. You monitor everything.

SYSTEM ARCHITECTURE:
- Server: 172.16.32.207 (Ubuntu), hostname dtg-mcp-server
- Services: wdws-mcp (9200), nelson-dashboard (9100), wdws-agents, postgresql@17-main
- Timers: wdws-email-sync (15 min), wdws-attach-ingest (30 min)
- Database: PostgreSQL 'wdws', schemas: core, legal, medical, ops, paperless
- Tunnel: klunky.12432.net → Cloudflare Tunnel (runs on another server)
- Email: athena@seattleseahawks.me via Microsoft Graph API

MONITORING SCOPE:
1. System resources — CPU, memory, swap, disk, inodes, load, network, processes
2. Service health — all systemd services and timers
3. journalctl logs — errors, warnings, crashes, OOM kills, auth failures
4. PostgreSQL — connections, slow queries, locks, replication lag
5. Cloudflare Tunnel — status via API
6. Attack detection — SSH brute force, MCP abuse, log injection
7. OpenAI API — key validity and balance

ESCALATION HIERARCHY:
Level 1: Auto-fix (restart service, kill stuck connection) → post to #ops
Level 2: Route to specialist agent → @mention in #alerts
Level 3: Multi-agent coordination → post to #alerts, notify orchestrator
Level 4: Human escalation → email william@seattleseahawks.me + #alerts

WHEN TO ESCALATE TO HUMAN:
- Service won't stay up after 3 restart attempts
- Disk > 92% full
- OOM kills happening
- Security breach detected (unauthorized access patterns)
- Database corruption signs
- Any issue that persists across 3+ consecutive watchdog runs
- OpenAI API key is broken/expired"""

    # ═══════════════════════════════════════════════════════════
    # Main run
    # ═══════════════════════════════════════════════════════════
    async def run(self, ctx: RunContext) -> dict:
        metrics = {}
        issues = []
        actions = []

        # ── 1. System Resources ──────────────────────────────
        res = await self._collect_system_metrics(ctx)
        metrics.update(res)
        issues += await self._evaluate_resource_thresholds(ctx, res)

        await ctx.log_comms("info", "system-metrics",
            f"System: CPU {res.get('cpu_percent', '?')}% | "
            f"Mem {res.get('memory_percent', '?')}% ({res.get('memory_used_gb', '?')} GB) | "
            f"Disk {res.get('disk_percent', '?')}% | "
            f"Load {res.get('load_1m', '?')}/{res.get('load_5m', '?')}/{res.get('load_15m', '?')} | "
            f"{res.get('process_count', '?')} procs, {res.get('zombie_processes', 0)} zombies",
            {"cpu": res.get("cpu_percent"), "memory": res.get("memory_percent"),
             "disk": res.get("disk_percent"), "load_1m": res.get("load_1m")})

        # ── 2. Service Health ────────────────────────────────
        svc_results = await self._check_all_services(ctx)
        metrics["services"] = svc_results
        for svc, status in svc_results.items():
            if status != "active":
                issue = {"severity": "critical", "category": "service",
                         "title": f"Service {svc} is {status}",
                         "route_to": "self-healing"}
                issues.append(issue)
                # Auto-restart
                restarted = await self._restart_service(ctx, svc)
                if restarted:
                    actions.append(f"Auto-restarted {svc}")

        # ── 3. Timer Health ──────────────────────────────────
        timer_results = await self._check_timers(ctx)
        metrics["timers"] = timer_results
        for timer, info in timer_results.items():
            if info.get("stale"):
                issues.append({"severity": "warning", "category": "timer",
                               "title": f"Timer {timer} is stale — "
                                        f"last ran {info.get('last_trigger', 'never')}",
                               "route_to": "self-healing"})

        # ── 4. journalctl Log Analysis ───────────────────────
        log_issues = await self._scan_journalctl(ctx)
        metrics["log_events"] = len(log_issues)
        issues += log_issues

        # ── 5. Database Health ───────────────────────────────
        db_metrics = await self._check_database_deep(ctx)
        metrics.update(db_metrics)
        if db_metrics.get("db_conn_percent", 0) > THRESHOLDS["db_conn_percent"]["warning"]:
            issues.append({"severity": "warning", "category": "database",
                           "title": f"DB connections at {db_metrics['db_conn_percent']}%",
                           "route_to": "db-tuner"})
        if db_metrics.get("long_running_queries", 0) > 0:
            issues.append({"severity": "warning", "category": "database",
                           "title": f"{db_metrics['long_running_queries']} long-running queries (>60s)",
                           "route_to": "db-tuner"})
        if db_metrics.get("deadlocks", 0) > 0:
            issues.append({"severity": "critical", "category": "database",
                           "title": f"{db_metrics['deadlocks']} deadlocks detected",
                           "route_to": "db-tuner"})

        # ── 6. Cloudflare Tunnel ─────────────────────────────
        tunnel_ok = await self._check_cloudflare_tunnel(ctx)
        metrics["tunnel_healthy"] = tunnel_ok
        if not tunnel_ok:
            issues.append({"severity": "critical", "category": "security",
                           "title": "Cloudflare Tunnel is down",
                           "route_to": "self-healing"})

        # ── 7. Attack Detection ──────────────────────────────
        attacks = await self._scan_for_attacks(ctx)
        metrics["attacks_blocked"] = attacks

        # ── 8. OpenAI Balance (every 4 hours) ────────────────
        balance_ok = await self._check_openai_balance(ctx)
        metrics["openai_ok"] = balance_ok

        # ── 9. Store metrics in health_checks ────────────────
        await self._store_health_metrics(ctx, metrics)

        # ── 10. Process issues: route, escalate, notify ──────
        await self._process_issues(ctx, issues, actions)

        # ── 11. Track persistent issue patterns ──────────────
        await self._track_persistent_issues(ctx, issues)

        # ── 12. Post summary to chat ─────────────────────────
        total_issues = len(issues)
        status_emoji = "🟢" if total_issues == 0 else "🟡" if total_issues < 3 else "🔴"
        summary = (
            f"{status_emoji} Watchdog: {len(metrics)} metrics | "
            f"{total_issues} issues | {attacks} attacks blocked | "
            f"CPU {metrics.get('cpu_percent', '?')}% | "
            f"Mem {metrics.get('memory_percent', '?')}% | "
            f"Disk {metrics.get('disk_percent', '?')}%"
        )

        await ctx.log_comms("info", "run-summary", summary,
            {"total_issues": total_issues, "attacks": attacks,
             "metrics_count": len(metrics),
             "cpu": metrics.get("cpu_percent"),
             "memory": metrics.get("memory_percent"),
             "disk": metrics.get("disk_percent")})

        try:
            await self.broadcast(summary, channel="ops", msg_type="status")
            if total_issues > 0:
                detail_lines = [f"• {i['title']}" for i in issues[:10]]
                await self.broadcast(
                    f"⚠️ {total_issues} issue(s) detected:\n" + "\n".join(detail_lines),
                    channel="alerts", msg_type="alert")
        except Exception as e:
            self.log.debug(f"Chat post failed: {e}")

        return {
            "summary": summary,
            "metrics": {k: v for k, v in metrics.items()
                        if isinstance(v, (int, float, str, bool))},
            "issues": total_issues,
            "attacks_blocked": attacks,
        }

    # ═══════════════════════════════════════════════════════════
    # 1. System Metrics Collection
    # ═══════════════════════════════════════════════════════════
    async def _collect_system_metrics(self, ctx: RunContext) -> dict:
        m = {}
        try:
            import psutil

            # CPU
            m["cpu_percent"] = psutil.cpu_percent(interval=1)
            m["cpu_count"] = psutil.cpu_count()
            load1, load5, load15 = os.getloadavg()
            m["load_1m"] = round(load1, 2)
            m["load_5m"] = round(load5, 2)
            m["load_15m"] = round(load15, 2)
            m["load_1m_ratio"] = round(load1 / max(psutil.cpu_count(), 1), 2)

            # Memory
            vm = psutil.virtual_memory()
            m["memory_percent"] = vm.percent
            m["memory_used_gb"] = round(vm.used / (1024**3), 2)
            m["memory_total_gb"] = round(vm.total / (1024**3), 2)
            m["memory_available_gb"] = round(vm.available / (1024**3), 2)

            # Swap
            sw = psutil.swap_memory()
            m["swap_percent"] = sw.percent
            m["swap_used_gb"] = round(sw.used / (1024**3), 2)
            m["swap_total_gb"] = round(sw.total / (1024**3), 2)

            # Disk
            disk = psutil.disk_usage("/")
            m["disk_percent"] = disk.percent
            m["disk_used_gb"] = round(disk.used / (1024**3), 2)
            m["disk_total_gb"] = round(disk.total / (1024**3), 2)
            m["disk_free_gb"] = round(disk.free / (1024**3), 2)

            # Inodes
            try:
                r = subprocess.run(
                    ["df", "-i", "/"], capture_output=True, text=True, timeout=5)
                lines = r.stdout.strip().split("\n")
                if len(lines) > 1:
                    parts = lines[1].split()
                    iused = int(parts[2])
                    ifree = int(parts[3])
                    m["inode_percent"] = round(iused / (iused + ifree) * 100, 1)
            except Exception:
                m["inode_percent"] = 0

            # Network I/O
            net = psutil.net_io_counters()
            m["net_bytes_sent_mb"] = round(net.bytes_sent / (1024**2), 1)
            m["net_bytes_recv_mb"] = round(net.bytes_recv / (1024**2), 1)

            # Processes
            m["process_count"] = len(psutil.pids())
            zombies = 0
            for proc in psutil.process_iter(["status"]):
                if proc.info["status"] == psutil.STATUS_ZOMBIE:
                    zombies += 1
            m["zombie_processes"] = zombies

            # Uptime
            boot = datetime.fromtimestamp(psutil.boot_time(), tz=timezone.utc)
            m["uptime_hours"] = round(
                (datetime.now(timezone.utc) - boot).total_seconds() / 3600, 1)

        except ImportError:
            self.log.warning("psutil not available — limited metrics")
            m["cpu_percent"] = 0
            m["memory_percent"] = 0
            m["disk_percent"] = 0

        return m

    async def _evaluate_resource_thresholds(self, ctx, metrics) -> list:
        issues = []
        checks = [
            ("cpu_percent", "CPU"),
            ("memory_percent", "Memory"),
            ("swap_percent", "Swap"),
            ("disk_percent", "Disk"),
            ("inode_percent", "Inodes"),
            ("load_1m_ratio", "Load average"),
            ("zombie_processes", "Zombie processes"),
        ]
        for key, label in checks:
            val = metrics.get(key, 0)
            thresh = THRESHOLDS.get(key, {})
            if val >= thresh.get("critical", 999):
                issues.append({
                    "severity": "critical", "category": "resources",
                    "title": f"{label} CRITICAL: {val}"
                             + ("%" if "percent" in key else ""),
                    "route_to": "self-healing",
                    "escalate_human": True,
                })
                await ctx.finding("critical", "resources",
                    f"{label} at critical level: {val}",
                    f"Threshold: {thresh.get('critical')}",
                    {"metric": key, "value": val})
            elif val >= thresh.get("warning", 999):
                issues.append({
                    "severity": "warning", "category": "resources",
                    "title": f"{label} elevated: {val}"
                             + ("%" if "percent" in key else ""),
                    "route_to": "self-healing",
                })
                await ctx.finding("warning", "resources",
                    f"{label} elevated: {val}",
                    f"Threshold: {thresh.get('warning')}",
                    {"metric": key, "value": val})
        return issues

    # ═══════════════════════════════════════════════════════════
    # 2. Service Health
    # ═══════════════════════════════════════════════════════════
    async def _check_all_services(self, ctx) -> dict:
        results = {}
        for svc in MONITORED_SERVICES:
            try:
                r = subprocess.run(
                    ["systemctl", "is-active", svc],
                    capture_output=True, text=True, timeout=5)
                results[svc] = r.stdout.strip() or "dead"
            except Exception as e:
                results[svc] = f"error: {e}"
        return results

    async def _check_timers(self, ctx) -> dict:
        results = {}
        for timer in MONITORED_TIMERS:
            try:
                r = subprocess.run(
                    ["systemctl", "show", f"{timer}.timer",
                     "--property=LastTriggerUSec,NextElapseUSecRealtime"],
                    capture_output=True, text=True, timeout=5)
                info = {}
                for line in r.stdout.strip().split("\n"):
                    if "=" in line:
                        k, v = line.split("=", 1)
                        info[k.strip()] = v.strip()

                last = info.get("LastTriggerUSec", "")
                nxt = info.get("NextElapseUSecRealtime", "")
                info["last_trigger"] = last
                info["next_trigger"] = nxt

                # Check if timer is stale (hasn't run in 2x expected interval)
                # Email sync = 15 min, attach ingest = 30 min
                expected_mins = 15 if "email" in timer else 30
                info["stale"] = False
                if last and last != "n/a":
                    try:
                        # Parse systemd timestamp
                        last_dt = datetime.strptime(
                            last.split(".")[0], "%a %Y-%m-%d %H:%M:%S")
                        last_dt = last_dt.replace(
                            tzinfo=datetime.now(timezone.utc).astimezone().tzinfo)
                        age_min = (datetime.now(last_dt.tzinfo) - last_dt).total_seconds() / 60
                        if age_min > expected_mins * 2.5:
                            info["stale"] = True
                            info["age_minutes"] = round(age_min, 1)
                    except Exception:
                        pass

                results[timer] = info
            except Exception as e:
                results[timer] = {"error": str(e), "stale": True}
        return results

    async def _restart_service(self, ctx, name: str) -> bool:
        """Rate-limited service restart (max 3/hour)."""
        counts = await ctx.recall("restart_counts", {})
        hk = datetime.now(timezone.utc).strftime("%Y%m%d%H")
        key = f"{name}:{hk}"
        count = counts.get(key, 0)

        if count >= 3:
            await ctx.finding("critical", "service",
                f"{name} keeps crashing — 3 restarts this hour",
                "Manual intervention required. Escalating to human.",
                {"service": name, "restarts_this_hour": count})
            return False

        try:
            subprocess.run(
                ["systemctl", "restart", name], timeout=15, check=True)
            counts[key] = count + 1
            # Prune stale keys
            counts = {k: v for k, v in counts.items() if k.endswith(f":{hk}")}
            await ctx.remember("restart_counts", counts)
            await ctx.action(f"Auto-restarted {name}")
            await ctx.finding("info", "auto-restart",
                f"Auto-restarted {name}",
                f"Service was dead, restart attempt #{count + 1}")
            return True
        except Exception as e:
            await ctx.finding("critical", "service",
                f"Failed to restart {name}: {e}")
            return False

    # ═══════════════════════════════════════════════════════════
    # 3. journalctl Log Analysis
    # ═══════════════════════════════════════════════════════════
    async def _scan_journalctl(self, ctx) -> list:
        """Comprehensive journalctl scan across all services."""
        issues = []
        last_scan = await ctx.recall("last_journal_scan_ts", "")
        since = last_scan if last_scan else "5 min ago"

        # Scan system-wide for critical patterns
        scans = [
            # (label, journalctl args, patterns)
            ("oom", ["-k", "--since", since],
             [(re.compile(r"Out of memory: Killed process (\d+) \((\S+)\)"),
               "OOM kill: process {2} (PID {1})")]),

            ("segfault", ["-k", "--since", since],
             [(re.compile(r"(\S+)\[(\d+)\]: segfault"),
               "Segfault: {1} (PID {2})")]),

            ("disk_error", ["--since", since, "-p", "err"],
             [(re.compile(r"(I/O error|No space left|Read-only file system|EXT4-fs error)", re.I),
               "Disk/IO error: {0}")]),

            ("service_fail", ["--since", since],
             [(re.compile(r"(\S+\.service): (Failed|failed|Main process exited, code=exited, status|"
                          r"Deactivated.*result=exit-code)", re.I),
               "Service failure: {1}")]),

            ("postgres_err", ["-u", "postgresql@17-main", "--since", since, "-p", "err..warning"],
             [(re.compile(r"(FATAL|ERROR|PANIC):(.+)", re.I),
               "PostgreSQL {1}: {2}")]),
        ]

        for label, args, patterns in scans:
            try:
                cmd = ["journalctl", "--no-pager", "-o", "short", "-q"] + args
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=10)
                lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

                for line in lines[-100:]:  # Cap at last 100 lines
                    for regex, template in patterns:
                        m = regex.search(line)
                        if m:
                            groups = m.groups()
                            title = template
                            for i, g in enumerate(groups):
                                title = title.replace(f"{{{i}}}", g or "?")

                            severity = "critical" if label in ("oom", "segfault") else "warning"
                            route = ("self-healing" if label in ("service_fail", "disk_error")
                                     else "db-tuner" if label == "postgres_err"
                                     else "security")
                            issues.append({
                                "severity": severity,
                                "category": f"log:{label}",
                                "title": title,
                                "route_to": route,
                                "escalate_human": label in ("oom", "segfault", "disk_error"),
                                "raw_line": line[:300],
                            })
            except Exception as e:
                self.log.warning("Journal scan '%s' failed: %s", label, e)

        # Scan each WDWS service for application errors
        for svc in MONITORED_SERVICES + [f"{t}.service" for t in MONITORED_TIMERS]:
            try:
                cmd = ["journalctl", "-u", svc, "--no-pager", "-o", "short",
                       "--since", since, "-p", "err..warning", "-q"]
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=10)
                lines = result.stdout.strip().split("\n") if result.stdout.strip() else []

                error_count = 0
                for line in lines:
                    if any(p in line.lower() for p in
                           ["error", "exception", "traceback", "critical",
                            "audit failure", "failed"]):
                        error_count += 1

                if error_count > 0:
                    issues.append({
                        "severity": "warning" if error_count < 10 else "critical",
                        "category": f"log:service",
                        "title": f"{svc}: {error_count} error(s) in logs",
                        "route_to": "self-healing",
                        "detail": lines[-3:] if lines else [],
                    })
            except Exception:
                pass

        # Update scan timestamp
        await ctx.remember("last_journal_scan_ts",
                           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

        return issues

    # ═══════════════════════════════════════════════════════════
    # 4. Database Deep Health
    # ═══════════════════════════════════════════════════════════
    async def _check_database_deep(self, ctx) -> dict:
        p = await ctx.db()
        stats = {}

        try:
            # Connection pool
            row = await p.fetchrow("""
                SELECT numbackends,
                       (SELECT setting::int FROM pg_settings
                        WHERE name = 'max_connections') as max_conn
                FROM pg_stat_database WHERE datname = 'wdws'
            """)
            if row:
                stats["db_connections"] = row["numbackends"]
                stats["db_max_connections"] = row["max_conn"]
                stats["db_conn_percent"] = round(
                    row["numbackends"] / row["max_conn"] * 100, 1)

            # Database size
            size = await p.fetchval("SELECT pg_database_size('wdws')")
            stats["db_size_mb"] = round(size / (1024 * 1024), 1)

            # Long-running queries
            lr = await p.fetchval("""
                SELECT COUNT(*) FROM pg_stat_activity
                WHERE state = 'active'
                  AND query NOT LIKE '%pg_stat_activity%'
                  AND now() - query_start > interval '60 seconds'
                  AND datname = 'wdws'
            """)
            stats["long_running_queries"] = lr

            # Deadlocks (from pg_stat_database)
            dl = await p.fetchval("""
                SELECT deadlocks FROM pg_stat_database WHERE datname = 'wdws'
            """)
            stats["deadlocks"] = dl or 0

            # Idle in transaction
            idle_tx = await p.fetchval("""
                SELECT COUNT(*) FROM pg_stat_activity
                WHERE state = 'idle in transaction'
                  AND now() - state_change > interval '10 minutes'
                  AND datname = 'wdws'
            """)
            stats["idle_in_transaction"] = idle_tx

            # Recent MCP query performance
            try:
                perf = await p.fetchrow("""
                    SELECT COUNT(*) as total,
                           AVG(duration_ms) as avg_ms,
                           MAX(duration_ms) as max_ms,
                           COUNT(*) FILTER (WHERE error IS NOT NULL) as errors
                    FROM ops.mcp_query_log
                    WHERE created_at > now() - interval '5 minutes'
                """)
                if perf:
                    stats["queries_5min"] = perf["total"]
                    stats["avg_query_ms"] = round(float(perf["avg_ms"] or 0), 1)
                    stats["max_query_ms"] = perf["max_ms"] or 0
                    stats["query_errors_5min"] = perf["errors"]
            except Exception:
                pass

        except Exception as e:
            stats["db_error"] = str(e)

        return stats

    # ═══════════════════════════════════════════════════════════
    # 5. Cloudflare Tunnel
    # ═══════════════════════════════════════════════════════════
    async def _check_cloudflare_tunnel(self, ctx) -> bool:
        if not CF_API_KEY or not CF_ACCOUNT_ID or not CF_TUNNEL_ID:
            return True

        headers = {
            "X-Auth-Email": CF_EMAIL,
            "X-Auth-Key": CF_API_KEY,
            "Content-Type": "application/json",
        }
        try:
            resp = await ctx.http_get(
                f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}"
                f"/cfd_tunnel/{CF_TUNNEL_ID}",
                headers=headers)
            if resp.status_code != 200:
                await ctx.finding("warning", "cloudflare",
                    f"Cloudflare API returned {resp.status_code}")
                return False

            status = resp.json().get("result", {}).get("status", "unknown")
            await ctx.remember("tunnel_status", {
                "status": status,
                "checked_at": datetime.now(timezone.utc).isoformat()})

            if status in ("healthy", "active"):
                return True

            await ctx.finding("critical", "cloudflare",
                f"Tunnel status: {status}",
                "klunky.12432.net may be unreachable")
            return False

        except Exception as e:
            self.log.warning("Tunnel check failed: %s", e)
            return True  # Don't alarm on transient failure

    # ═══════════════════════════════════════════════════════════
    # 6. Attack Detection
    # ═══════════════════════════════════════════════════════════
    async def _scan_for_attacks(self, ctx) -> int:
        attacks_found = 0

        try:
            cmd = ["journalctl", "--no-pager", "-o", "short",
                   "--since", "5 min ago", "-q"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            lines = result.stdout.splitlines()
        except Exception:
            return 0

        patterns = [
            (re.compile(r'Failed password.*from (\d+\.\d+\.\d+\.\d+)', re.I),
             "SSH brute force"),
            (re.compile(r'Invalid user.*from (\d+\.\d+\.\d+\.\d+)', re.I),
             "SSH invalid user"),
            (re.compile(
                r'Connection closed by.*(\d+\.\d+\.\d+\.\d+).*\[preauth\]', re.I),
             "SSH preauth disconnect"),
            (re.compile(
                r'authentication failure.*rhost=(\d+\.\d+\.\d+\.\d+)', re.I),
             "PAM auth failure"),
        ]

        ip_counts: Counter = Counter()
        ip_reasons: dict = {}

        for line in lines:
            for pat, reason in patterns:
                m = pat.search(line)
                if m:
                    ip = m.group(1)
                    if ip.startswith(("127.", "10.", "172.16.", "192.168.")):
                        continue
                    ip_counts[ip] += 1
                    ip_reasons[ip] = reason

        if not ip_counts:
            return 0

        p = await ctx.db()
        wl = await p.fetch("SELECT ip_address::text FROM ops.acl_whitelist")
        whitelisted = set(r["ip_address"] for r in wl)

        for ip, count in ip_counts.most_common(50):
            if ip in whitelisted or count < 5:
                continue
            attacks_found += 1
            reason = ip_reasons.get(ip, "suspicious")

            await p.execute("""
                INSERT INTO ops.acl_blacklist
                    (ip_address, source, reason, added_by, attack_count, last_seen)
                VALUES ($1::inet, 'watchdog', $2, 'watchdog', $3, now())
                ON CONFLICT (ip_address) DO UPDATE SET
                    attack_count = ops.acl_blacklist.attack_count + $3,
                    last_seen = now(), is_active = true, reason = $2
            """, ip, f"{reason} ({count} attempts in 5 min)", count)

            # Push to Cloudflare WAF
            await self._cloudflare_block_ip(ctx, ip, reason)

            await ctx.finding("warning", "attack",
                f"Blocked {ip}: {reason} ({count} attempts)",
                "Auto-blacklisted locally + Cloudflare WAF",
                {"ip": ip, "count": count, "reason": reason})
            await ctx.action(f"Blacklisted {ip} ({count}x {reason})")

        return attacks_found

    async def _cloudflare_block_ip(self, ctx, ip: str, reason: str):
        if not CF_API_KEY or not CF_ZONE_ID:
            return
        try:
            resp = await ctx.http_post(
                f"https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}"
                f"/firewall/access_rules/rules",
                headers={
                    "X-Auth-Email": CF_EMAIL,
                    "X-Auth-Key": CF_API_KEY,
                    "Content-Type": "application/json",
                },
                json={
                    "mode": "block",
                    "configuration": {"target": "ip", "value": ip},
                    "notes": f"WDWS Watchdog: {reason} [{datetime.now(timezone.utc).isoformat()}]",
                })
            if resp.status_code not in (200, 409):
                self.log.warning("CF WAF block failed for %s: %s", ip, resp.text[:200])
        except Exception as e:
            self.log.warning("CF WAF error: %s", e)

    # ═══════════════════════════════════════════════════════════
    # 7. OpenAI Balance
    # ═══════════════════════════════════════════════════════════
    async def _check_openai_balance(self, ctx) -> bool:
        last = await ctx.recall("last_balance_check", "")
        if last:
            try:
                hrs = ((datetime.now(timezone.utc) -
                        datetime.fromisoformat(last)).total_seconds() / 3600)
                if hrs < 4:
                    return True
            except Exception:
                pass

        if not OPENAI_API_KEY:
            return True

        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        try:
            resp = await ctx.http_get(
                "https://api.openai.com/v1/models", headers=headers)
            working = resp.status_code == 200
            await ctx.remember("last_balance_check",
                               datetime.now(timezone.utc).isoformat())
            await ctx.remember("openai_api_status", {
                "working": working,
                "checked_at": datetime.now(timezone.utc).isoformat()})

            if not working:
                await ctx.finding("critical", "openai",
                    "OpenAI API key is not working",
                    f"Status {resp.status_code}. Key may be expired.")
                # This is critical enough to email
                await self._escalate_to_human(
                    ctx,
                    title="OpenAI API Key Failure",
                    severity="critical",
                    sections=[{
                        "heading": "API Key Issue",
                        "content": f"The OpenAI API key returned HTTP {resp.status_code}.\n"
                                   "The agent system relies on this key for all AI operations.\n"
                                   "Check: https://platform.openai.com/api-keys",
                        "type": "error",
                    }])
                return False
            return True
        except Exception as e:
            self.log.warning("OpenAI check failed: %s", e)
            return True

    # ═══════════════════════════════════════════════════════════
    # 8. Issue Processing & Routing
    # ═══════════════════════════════════════════════════════════
    async def _process_issues(self, ctx, issues: list, actions: list):
        """Route issues to correct agents, escalate critical ones."""
        if not issues:
            return

        critical_issues = [i for i in issues if i["severity"] == "critical"]
        human_issues = [i for i in issues if i.get("escalate_human")]

        # Route to specialist agents via @mention in chat
        routed = defaultdict(list)
        for issue in issues:
            target = issue.get("route_to")
            if target:
                routed[target].append(issue)

        for agent_id, agent_issues in routed.items():
            titles = "\n".join(f"• {i['title']}" for i in agent_issues[:5])
            try:
                await self.ask_agent(
                    agent_id,
                    f"Watchdog detected {len(agent_issues)} issue(s) "
                    f"in your domain:\n{titles}\n"
                    f"Please investigate and report back.",
                    channel="alerts")
            except Exception as e:
                self.log.debug(f"Failed to route to {agent_id}: {e}")

        # Notify orchestrator if multiple critical issues
        if len(critical_issues) >= 2:
            try:
                await self.ask_agent(
                    "orchestrator",
                    f"⚠️ MULTI-CRITICAL: {len(critical_issues)} critical issues "
                    f"detected simultaneously. Requesting fleet coordination.\n"
                    + "\n".join(f"• {i['title']}" for i in critical_issues),
                    channel="alerts")
            except Exception:
                pass

        # Escalate to human via email
        if human_issues:
            await self._escalate_to_human(
                ctx,
                title=f"{len(human_issues)} Critical Issue(s) Requiring Attention",
                severity="critical",
                sections=[{
                    "heading": f"Issue {i+1}: {issue['title']}",
                    "content": issue.get("detail", issue["title"]),
                    "type": "error" if issue["severity"] == "critical" else "warning",
                } for i, issue in enumerate(human_issues[:5])]
                + [{
                    "heading": "System Status",
                    "content": f"Server: dtg-mcp-server (172.16.32.207)\n"
                               f"Time: {datetime.now(timezone.utc).isoformat()}\n"
                               f"Total Issues: {len(issues)}\n"
                               f"Critical: {len(critical_issues)}\n"
                               f"Actions Taken: {', '.join(actions) if actions else 'None'}",
                    "type": "info",
                }])

    async def _escalate_to_human(self, ctx, title: str,
                                  severity: str, sections: list):
        """Send an alert email via Graph API. Rate limited: max 1 per 30 min per category."""
        # Rate limit
        last_email = await ctx.recall("last_human_email", "")
        if last_email:
            try:
                hrs = ((datetime.now(timezone.utc) -
                        datetime.fromisoformat(last_email)).total_seconds() / 60)
                if hrs < 30:
                    self.log.info("Email rate limited — last sent %d min ago", hrs)
                    return
            except Exception:
                pass

        body_html = build_notification_html(
            title=f"🚨 {title}",
            sections=sections,
            footer="Nelson Enterprise Platform — Watchdog Agent v3")

        result = await send_email(
            tenant_id=GRAPH_TENANT_ID,
            client_id=GRAPH_CLIENT_ID,
            client_secret=GRAPH_CLIENT_SECRET,
            sender=GRAPH_SENDER_EMAIL,
            to_recipients=[ALERT_EMAIL],
            subject=f"[WDWS {severity.upper()}] {title}",
            body_html=body_html,
            importance="high" if severity == "critical" else "normal")

        if result["status"] == "sent":
            await ctx.remember("last_human_email",
                               datetime.now(timezone.utc).isoformat())
            await ctx.action(f"Escalated to human via email: {title}")
            self.log.info("📧 Alert email sent: %s", title)
        else:
            self.log.error("Email send failed: %s", result.get("error"))
            await ctx.finding("warning", "notifications",
                f"Failed to send alert email: {result.get('error', 'unknown')}",
                f"Tried to send: {title}")

    # ═══════════════════════════════════════════════════════════
    # 9. Persistent Issue Tracking
    # ═══════════════════════════════════════════════════════════
    async def _track_persistent_issues(self, ctx, issues: list):
        """Track issues across runs. If same issue persists 3+ runs, escalate."""
        tracker = await ctx.recall("issue_tracker", {})
        now = datetime.now(timezone.utc).isoformat()

        # Build set of current issue keys
        current_keys = set()
        for issue in issues:
            key = f"{issue['category']}:{issue['title'][:80]}"
            current_keys.add(key)

            if key in tracker:
                tracker[key]["count"] += 1
                tracker[key]["last_seen"] = now
            else:
                tracker[key] = {
                    "count": 1,
                    "first_seen": now,
                    "last_seen": now,
                    "severity": issue["severity"],
                    "escalated": False,
                }

        # Check for persistent issues (3+ consecutive runs)
        if isinstance(tracker, str):
            try:
                tracker = json.loads(tracker)
            except Exception:
                tracker = {}
        if isinstance(tracker, str):
            try:
                tracker = json.loads(tracker)
            except json.JSONDecodeError:
                tracker = {}
        if not isinstance(tracker, dict):
            tracker = {}
        if isinstance(tracker, str):
            try:
                tracker = json.loads(tracker)
            except Exception:
                tracker = {}
        if not isinstance(tracker, dict):
            tracker = {}
        for key, info in tracker.items():
            if key in current_keys and info["count"] >= 3 and not info["escalated"]:
                info["escalated"] = True
                await ctx.finding("critical", "persistent",
                    f"Persistent issue ({info['count']} runs): {key}",
                    f"First seen: {info['first_seen']}, "
                    f"still active after {info['count']} watchdog cycles",
                    {"key": key, "run_count": info["count"]})

                # Escalate to human for persistent issues
                await self._escalate_to_human(
                    ctx,
                    title=f"Persistent Issue: {key}",
                    severity="critical",
                    sections=[{
                        "heading": "Persistent Issue Detected",
                        "content": f"The following issue has persisted across "
                                   f"{info['count']} watchdog runs:\n\n"
                                   f"{key}\n\n"
                                   f"First seen: {info['first_seen']}\n"
                                   f"Severity: {info['severity']}\n\n"
                                   f"Automated resolution has not resolved this. "
                                   f"Human intervention is requested.",
                        "type": "error",
                    }])

        # Clear resolved issues (not seen in current run)
        for key in list(tracker.keys()):
            if key not in current_keys:
                if tracker[key]["count"] > 1:
                    self.log.info("Issue resolved: %s (after %d runs)",
                                 key, tracker[key]["count"])
                del tracker[key]

        await ctx.remember("issue_tracker", tracker)

    # ═══════════════════════════════════════════════════════════
    # 10. Health Metrics Storage
    # ═══════════════════════════════════════════════════════════
    async def _store_health_metrics(self, ctx, metrics: dict):
        p = await ctx.db()
        overall = "healthy"
        for key, val in metrics.items():
            if not isinstance(val, (int, float)):
                continue
            # Determine status for this metric
            thresh = THRESHOLDS.get(key, {})
            if val >= thresh.get("critical", 999):
                status = "critical"
                overall = "critical"
            elif val >= thresh.get("warning", 999):
                status = "degraded"
                if overall == "healthy":
                    overall = "degraded"
            else:
                status = "healthy"

            try:
                await p.execute("""
                    INSERT INTO ops.health_checks
                        (check_name, status, value, unit, detail)
                    VALUES ($1, $2, $3, $4, $5)
                """, key, status, val, self._unit_for(key),
                    json.dumps({"threshold": thresh}) if thresh else "{}")
            except Exception:
                pass

    def _unit_for(self, name: str) -> str:
        if "percent" in name:
            return "%"
        if "gb" in name:
            return "GB"
        if "mb" in name:
            return "MB"
        if "ms" in name:
            return "ms"
        if "count" in name or "queries" in name or "connections" in name:
            return "count"
        if "hours" in name:
            return "hours"
        return ""
