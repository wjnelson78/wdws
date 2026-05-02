"""APScheduler setup for the orchestrator.

One AsyncIOScheduler per process. Jobs persist in the orchestrator DB so they
survive restarts. The scheduler iterates over active principals; per-user runs
are serial (within a user) and parallel across users.

Spec references:
- Phase 2 §9 — scheduled jobs registry
- Patch 02 §B.4 — per-principal timezones for cron firing
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from orchestrator.config import get_settings
from orchestrator.db.models import User
from orchestrator.db.session import SessionLocal
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)

_scheduler: AsyncIOScheduler | None = None
_per_user_locks: dict[UUID, asyncio.Lock] = {}


def get_scheduler() -> AsyncIOScheduler:
    global _scheduler
    if _scheduler is None:
        settings = get_settings()
        # Reuse the orchestrator (athena) DB for the jobstore. Use sync URL
        # because APScheduler's SQLAlchemy jobstore expects a sync engine.
        sync_url = settings.database_url.replace("+asyncpg", "")
        jobstore = SQLAlchemyJobStore(url=sync_url, tablename="apscheduler_jobs")
        _scheduler = AsyncIOScheduler(
            jobstores={"default": jobstore},
            timezone="UTC",
        )
    return _scheduler


async def _iter_active_principals() -> list[User]:
    async with SessionLocal() as session:
        result = await session.execute(
            select(User).where(
                User.account_type == "principal", User.active.is_(True)
            )
        )
        return list(result.scalars().all())


async def _run_per_user(
    job_name: str, fn: Callable[[User], Awaitable[Any]]
) -> None:
    """Run ``fn(user)`` per active principal. Serial within user, parallel across."""
    principals = await _iter_active_principals()
    _log.info("scheduled_job_started", job=job_name, principals=len(principals))

    async def _run_for(user: User) -> None:
        lock = _per_user_locks.setdefault(user.id, asyncio.Lock())
        async with lock:
            try:
                await fn(user)
                _log.info("scheduled_job_user_done", job=job_name, user=user.identifier)
            except Exception as exc:
                _log.exception(
                    "scheduled_job_user_failed",
                    job=job_name,
                    user=user.identifier,
                    error=str(exc),
                )

    await asyncio.gather(*[_run_for(u) for u in principals], return_exceptions=False)


async def _job_nightly_consolidation() -> None:
    from orchestrator.services.memory_system.consolidator import get_consolidator

    consolidator = get_consolidator()

    async def _for_user(user: User) -> None:
        await consolidator.run(user_id=user.id, kind="nightly")

    await _run_per_user("nightly_consolidation", _for_user)


async def _job_weekly_deep_consolidation() -> None:
    from orchestrator.services.memory_system.consolidator import get_consolidator

    consolidator = get_consolidator()

    async def _for_user(user: User) -> None:
        await consolidator.run(user_id=user.id, kind="weekly_deep")

    await _run_per_user("weekly_deep_consolidation", _for_user)


async def _job_retention_sweep() -> None:
    """Light daily pass: apply retention without an Opus call."""
    from orchestrator.services.memory_system.consolidator import get_consolidator

    consolidator = get_consolidator()

    async def _for_user(user: User) -> None:
        await consolidator.run(user_id=user.id, kind="nightly", force_metadata_only=True)

    await _run_per_user("retention_sweep", _for_user)


def register_jobs() -> None:
    """Register all recurring jobs. Called once during app startup."""
    settings = get_settings()
    sched = get_scheduler()

    sched.add_job(
        _job_nightly_consolidation,
        trigger=CronTrigger.from_crontab(settings.dreaming_nightly_cron, timezone="UTC"),
        id="nightly_consolidation",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    sched.add_job(
        _job_weekly_deep_consolidation,
        trigger=CronTrigger.from_crontab(settings.dreaming_weekly_cron, timezone="UTC"),
        id="weekly_deep_consolidation",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    sched.add_job(
        _job_retention_sweep,
        trigger=CronTrigger.from_crontab("30 2 * * *", timezone="UTC"),
        id="retention_sweep",
        replace_existing=True,
        misfire_grace_time=3600,
    )
    _log.info(
        "scheduler_jobs_registered",
        nightly=settings.dreaming_nightly_cron,
        weekly=settings.dreaming_weekly_cron,
    )


def start_scheduler() -> None:
    sched = get_scheduler()
    if not sched.running:
        sched.start(paused=False)
        _log.info("scheduler_started")


def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler is not None and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _log.info("scheduler_shutdown")
    _scheduler = None
