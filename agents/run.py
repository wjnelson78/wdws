"""
🚀 WDWS Agent Runner — Main Entry Point
════════════════════════════════════════
Scheduler that runs all registered agents based on their cron schedules.
Uses asyncio + croniter to tick every AGENT_TICK_SECONDS and launch
agents whose cron expression matches.

Usage:
    python run.py                   # Run scheduler (daemon mode)
    python run.py --once <agent>    # Run a single agent once and exit
    python run.py --list            # List all registered agents
    python run.py --status          # Show agent fleet status
"""
import sys
import os
import signal
import asyncio
import argparse
import logging
from datetime import datetime, timezone

# Add agents dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    DATABASE_URL, AGENT_TICK_SECONDS, AGENT_MAX_CONCURRENT,
    LOG_LEVEL, LOG_FORMAT
)
from framework import get_pool, close_pool

# ── Import all agents ────────────────────────────────────
from agent_orchestrator import OrchestratorAgent
from agent_watchdog import WatchdogAgent
from agent_security import SecuritySentinelAgent
from agent_self_healing import SelfHealingAgent
from agent_data_quality import DataQualityAgent
from agent_email_triage import EmailTriageAgent
from agent_db_tuner import DatabaseTunerAgent
from agent_case_strategy import CaseStrategyAgent
from agent_retention import RetentionAgent
from agent_timeline import TimelineAgent
from agent_query_insight import QueryInsightAgent
from agent_code_doctor import CodeDoctorAgent

# ── Logging ──────────────────────────────────────────────
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
log = logging.getLogger("agent-runner")

# ── Agent Fleet ──────────────────────────────────────────
ALL_AGENTS = [
    OrchestratorAgent(),     # P0 — fleet manager
    CodeDoctorAgent(),       # P1 — auto-remediation with Codex
    WatchdogAgent(),         # P1 — health
    SecuritySentinelAgent(), # P2 — security
    SelfHealingAgent(),      # P2 — auto-repair
    DataQualityAgent(),      # P3 — data integrity
    EmailTriageAgent(),      # P3 — email classification
    DatabaseTunerAgent(),    # P4 — performance
    CaseStrategyAgent(),     # P4 — legal brief
    RetentionAgent(),        # P4 — data governance
    TimelineAgent(),         # P5 — chronology
    QueryInsightAgent(),     # P5 — analytics
]

AGENT_MAP = {a.agent_id: a for a in ALL_AGENTS}

# Track last run times to determine when to fire next
_last_runs: dict[str, datetime] = {}
_running: set[str] = set()
_shutdown = False
_wake_queue: set[str] = set()      # Agents to wake immediately
_last_mention_id: int = 0          # High-water mark for notification polling


def _should_run(agent, now: datetime) -> bool:
    """Check if agent's cron schedule says it's time to run."""
    try:
        from croniter import croniter
    except ImportError:
        log.error("croniter not installed — pip install croniter")
        return False

    last = _last_runs.get(agent.agent_id)
    if not last:
        # First tick — check if current minute matches cron
        cron = croniter(agent.schedule, now)
        prev = cron.get_prev(datetime)
        # Run if the previous cron time is within the last tick window
        return (now - prev).total_seconds() < AGENT_TICK_SECONDS * 2

    cron = croniter(agent.schedule, last)
    next_run = cron.get_next(datetime)
    return now >= next_run


async def _run_agent(agent):
    """Execute a single agent with concurrency tracking."""
    if agent.agent_id in _running:
        log.warning(f"[{agent.agent_id}] Already running — skipping")
        return

    _running.add(agent.agent_id)
    try:
        log.info(f"[{agent.agent_id}] ▶ Starting")
        result = await agent.execute()
        _last_runs[agent.agent_id] = datetime.now(timezone.utc)
        log.info(f"[{agent.agent_id}] ✓ {result.get('summary', 'done')}")
    except Exception as e:
        log.error(f"[{agent.agent_id}] ✗ {e}", exc_info=True)
        _last_runs[agent.agent_id] = datetime.now(timezone.utc)
    finally:
        _running.discard(agent.agent_id)


async def _wake_agent(agent):
    """Execute inbox-only for a mention-woken agent."""
    if agent.agent_id in _running:
        log.debug(f"[{agent.agent_id}] Already running — defer wake")
        return

    _running.add(agent.agent_id)
    try:
        log.info(f"[{agent.agent_id}] \u26a1 WAKE — mention detected")
        result = await agent.execute_inbox(trigger="mention")
        log.info(f"[{agent.agent_id}] \u26a1 Wake done: {result.get('summary', 'ok')}")
    except Exception as e:
        log.error(f"[{agent.agent_id}] \u2717 Wake failed: {e}", exc_info=True)
    finally:
        _running.discard(agent.agent_id)


async def notification_watcher():
    """
    Lightweight poller that checks for new @mentions every 5 seconds.
    When a mention is found for an agent that isn't running, it triggers
    an immediate inbox-only execution — no waiting for the cron schedule.
    """
    global _last_mention_id, _shutdown
    from framework import get_pool

    log.info("\U0001f514 Notification watcher started (5s poll)")

    # Initialize high-water mark from DB
    try:
        pool = await get_pool()
        row = await pool.fetchval("SELECT COALESCE(MAX(id), 0) FROM ops.agent_chat")
        _last_mention_id = row
        log.info(f"\U0001f514 Notification baseline: message #{_last_mention_id}")
    except Exception as e:
        log.warning(f"Notification init error: {e}")
        _last_mention_id = 0

    while not _shutdown:
        try:
            await asyncio.sleep(5)
            if _shutdown:
                break

            pool = await get_pool()

            # Find new messages with @mentions since our last check
            new_mentions = await pool.fetch("""
                SELECT DISTINCT m.agent_id, c.id
                FROM ops.agent_chat c,
                     LATERAL unnest(c.mentions) AS m(agent_id)
                WHERE c.id > $1
                  AND c.from_agent != m.agent_id
                  AND NOT EXISTS (
                      SELECT 1 FROM ops.agent_chat r
                      WHERE r.reply_to = c.id AND r.from_agent = m.agent_id
                  )
                ORDER BY c.id
            """, _last_mention_id)

            # Check for general human questions (no mentions) — both new AND
            # unanswered ones from before restart.  For each agent, check if
            # there's a pending human question it hasn't replied to yet.
            agents_needing_wake = set()
            pending_human = await pool.fetch("""
                SELECT c.id, array_agg(DISTINCT r.from_agent) FILTER (WHERE r.from_agent IS NOT NULL) AS responders
                FROM ops.agent_chat c
                LEFT JOIN ops.agent_chat r ON r.reply_to = c.id
                WHERE c.from_agent LIKE 'human%%'
                  AND c.mentions = '{}'
                  AND c.created_at > now() - interval '2 hours'
                GROUP BY c.id
            """)

            all_agent_ids = set(AGENT_MAP.keys())
            for row in pending_human:
                responders = set(row["responders"] or [])
                missing = all_agent_ids - responders
                agents_needing_wake.update(missing)

            # Update high-water mark
            max_id = await pool.fetchval("SELECT COALESCE(MAX(id), $1) FROM ops.agent_chat", _last_mention_id)
            _last_mention_id = max_id

            # Wake mentioned agents
            woken = set()
            for row in new_mentions:
                aid = row["agent_id"]
                if aid in AGENT_MAP and aid not in _running and aid not in woken:
                    woken.add(aid)
                    asyncio.create_task(_wake_agent(AGENT_MAP[aid]))

            # Wake agents that haven't responded to general human questions
            for aid in agents_needing_wake:
                if aid in AGENT_MAP and aid not in _running and aid not in woken:
                    woken.add(aid)
                    asyncio.create_task(_wake_agent(AGENT_MAP[aid]))

            if woken:
                log.info(f"\U0001f514 Waking agents: {', '.join(sorted(woken))}")

        except Exception as e:
            log.warning(f"Notification watcher error: {e}")
            await asyncio.sleep(10)

    log.info("\U0001f514 Notification watcher stopped")


async def scheduler():
    """Main scheduling loop."""
    global _shutdown
    log.info("=" * 60)
    log.info("  WDWS Agent Runner — Starting")
    log.info(f"  Agents: {len(ALL_AGENTS)}")
    log.info(f"  Tick: {AGENT_TICK_SECONDS}s | Max concurrent: {AGENT_MAX_CONCURRENT}")
    log.info("=" * 60)

    # Register all agents in DB
    pool = await get_pool()
    for agent in ALL_AGENTS:
        await agent.register()
        log.info(f"  Registered: {agent.agent_id} ({agent.agent_name}) "
                 f"schedule={agent.schedule} priority={agent.priority}")

    log.info("-" * 60)

    # Launch the notification watcher as a background task
    notify_task = asyncio.create_task(notification_watcher())

    while not _shutdown:
        now = datetime.now(timezone.utc)
        
        # Find agents due to run, sorted by priority
        due = [a for a in ALL_AGENTS if _should_run(a, now) and a.agent_id not in _running]
        due.sort(key=lambda a: a.priority)

        # Respect concurrency limit
        slots = AGENT_MAX_CONCURRENT - len(_running)
        to_launch = due[:max(0, slots)]

        if to_launch:
            log.info(f"Launching {len(to_launch)} agents: "
                     f"{', '.join(a.agent_id for a in to_launch)}")

        # Launch as concurrent tasks
        tasks = [asyncio.create_task(_run_agent(a)) for a in to_launch]

        # Wait for tick
        await asyncio.sleep(AGENT_TICK_SECONDS)

    # Cleanup
    log.info("Shutting down...")
    notify_task.cancel()
    try:
        await notify_task
    except asyncio.CancelledError:
        pass
    await close_pool()


async def run_once(agent_id: str):
    """Run a single agent once and exit."""
    if agent_id not in AGENT_MAP:
        print(f"Unknown agent: {agent_id}")
        print(f"Available: {', '.join(AGENT_MAP.keys())}")
        sys.exit(1)

    agent = AGENT_MAP[agent_id]
    await agent.register()
    log.info(f"Running {agent.agent_id} ({agent.agent_name})...")
    result = await agent.execute()
    log.info(f"Result: {json.dumps(result, indent=2, default=str)}")
    await close_pool()


def list_agents():
    """Print all registered agents."""
    print(f"\n{'ID':<20} {'Name':<25} {'Schedule':<15} {'P':>2}  Description")
    print("─" * 90)
    for a in sorted(ALL_AGENTS, key=lambda x: x.priority):
        print(f"{a.agent_id:<20} {a.agent_name:<25} {a.schedule:<15} {a.priority:>2}  {a.description[:40]}")
    print(f"\n{len(ALL_AGENTS)} agents total\n")


async def show_status():
    """Show current fleet status from the database."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        agents = await conn.fetch("""
            SELECT id, name, is_active, last_run_at, last_status, 
                   run_count, error_count, priority
            FROM ops.agent_registry
            ORDER BY priority, name
        """)

        print(f"\n{'ID':<20} {'Status':<10} {'Last Run':<22} {'Runs':>5} {'Errs':>5} {'P':>2}")
        print("─" * 75)
        for a in agents:
            status = "ACTIVE" if a["is_active"] else "OFF"
            last = a["last_run_at"].strftime("%Y-%m-%d %H:%M") if a["last_run_at"] else "never"
            lr_status = a["last_status"] or "-"
            print(f"{a['id']:<20} {status:<10} {last:<22} "
                  f"{a['run_count'] or 0:>5} {a['error_count'] or 0:>5} {a['priority'] or 0:>2}")

        # Recent findings
        findings = await conn.fetch("""
            SELECT severity, COUNT(*) as count
            FROM ops.agent_findings
            WHERE status = 'open'
            GROUP BY severity
        """)
        if findings:
            print(f"\nOpen findings: ", end="")
            print(", ".join(f"{f['severity']}: {f['count']}" for f in findings))

    await close_pool()
    print()


def handle_signal(sig, frame):
    global _shutdown
    log.info(f"Received signal {sig} — shutting down")
    _shutdown = True


if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description="WDWS Agent Runner")
    parser.add_argument("--once", type=str, help="Run a single agent once")
    parser.add_argument("--list", action="store_true", help="List all agents")
    parser.add_argument("--status", action="store_true", help="Show fleet status")
    args = parser.parse_args()

    if args.list:
        list_agents()
        sys.exit(0)

    if args.once:
        asyncio.run(run_once(args.once))
        sys.exit(0)

    if args.status:
        asyncio.run(show_status())
        sys.exit(0)

    # Daemon mode
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    asyncio.run(scheduler())
