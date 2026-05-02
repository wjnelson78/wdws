"""Patch 02 §B: per-principal runtime configuration resolver.

Resolution order:
    1. memory.system_config row for (user_id, parameter_path)
    2. env-var fallback (registered per parameter)
    3. hard-coded default

In-memory cache, per principal, 60s TTL. Best-effort invalidation via
``invalidate(user_id)``. Cache key collisions impossible since prefix is the
UUID.
"""
from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from orchestrator.db.memory_models import SystemConfig
from orchestrator.db.session import MemorySessionLocal
from orchestrator.logging_config import get_logger

_log = get_logger(__name__)


@dataclass(frozen=True)
class ParameterDef:
    """Describes a tunable parameter and how to resolve its env-var fallback."""

    path: str
    env_var: str | None
    default: Any
    parser: Callable[[str], Any] = str  # type: ignore[assignment]


def _parse_bool(s: str) -> bool:
    return s.strip().lower() in ("1", "true", "yes", "on")


def _parse_json(s: str) -> Any:
    import json as _json

    return _json.loads(s)


# Patch 02 §B.1 — registry of parameters that flow through the resolver.
PARAMETERS: dict[str, ParameterDef] = {
    "council_gate.category_floors": ParameterDef(
        path="council_gate.category_floors",
        env_var="COUNCIL_CATEGORY_FLOORS",
        default={"legal_filing": "mini", "medical_decision": "full"},
        parser=_parse_json,
    ),
    "council_gate.mini_threshold": ParameterDef(
        path="council_gate.mini_threshold",
        env_var="COUNCIL_MINI_THRESHOLD",
        default=0.40,
        parser=float,
    ),
    "council_gate.full_threshold": ParameterDef(
        path="council_gate.full_threshold",
        env_var="COUNCIL_FULL_THRESHOLD",
        default=0.75,
        parser=float,
    ),
    "council.daily_budget_usd": ParameterDef(
        path="council.daily_budget_usd",
        env_var="COUNCIL_DAILY_BUDGET_USD",
        default=3.00,
        parser=float,
    ),
    "council.soft_cap_percent": ParameterDef(
        path="council.soft_cap_percent",
        env_var="COUNCIL_SOFT_CAP_PERCENT",
        default=75,
        parser=int,
    ),
    "dreaming.daily_budget_usd": ParameterDef(
        path="dreaming.daily_budget_usd",
        env_var="DREAMING_DAILY_BUDGET_USD",
        default=3.00,
        parser=float,
    ),
    "dreaming.significance_threshold": ParameterDef(
        path="dreaming.significance_threshold",
        env_var="DREAMING_SIGNIFICANCE_THRESHOLD",
        default=0.70,
        parser=float,
    ),
    "retention.legal.policy": ParameterDef(
        path="retention.legal.policy",
        env_var="RETENTION_LEGAL_POLICY",
        default="preserve",
    ),
    "retention.medical.policy": ParameterDef(
        path="retention.medical.policy",
        env_var="RETENTION_MEDICAL_POLICY",
        default="preserve",
    ),
    "retention.casual.policy": ParameterDef(
        path="retention.casual.policy",
        env_var="RETENTION_CASUAL_POLICY",
        default="compress_30_delete_90",
    ),
    "retention.dev.policy": ParameterDef(
        path="retention.dev.policy",
        env_var="RETENTION_DEV_POLICY",
        default="compress_30",
    ),
    "retention.default.policy": ParameterDef(
        path="retention.default.policy",
        env_var="RETENTION_DEFAULT_POLICY",
        default="compress_30",
    ),
    "adaptation.max_auto_changes_per_week": ParameterDef(
        path="adaptation.max_auto_changes_per_week",
        env_var="ADAPTATION_MAX_AUTO_CHANGES_PER_WEEK",
        default=5,
        parser=int,
    ),
}


class ConfigResolver:
    def __init__(self, ttl_seconds: float = 60.0) -> None:
        self._cache: dict[str, tuple[float, Any]] = {}
        self._ttl = ttl_seconds

    async def get(
        self, user_id: UUID, parameter_path: str, default: Any | None = None
    ) -> Any:
        cache_key = f"{user_id}|{parameter_path}"
        now = time.monotonic()
        cached = self._cache.get(cache_key)
        if cached is not None and (now - cached[0]) < self._ttl:
            return cached[1]

        async with MemorySessionLocal() as session:
            result = await session.execute(
                select(SystemConfig.value).where(
                    SystemConfig.user_id == user_id,
                    SystemConfig.parameter_path == parameter_path,
                )
            )
            db_value = result.scalar_one_or_none()

        if db_value is not None:
            self._cache[cache_key] = (now, db_value)
            return db_value

        # Env-var fallback.
        param = PARAMETERS.get(parameter_path)
        if param is not None and param.env_var:
            env_raw = os.getenv(param.env_var)
            if env_raw is not None and env_raw != "":
                try:
                    parsed = param.parser(env_raw)
                except Exception as exc:
                    _log.warning(
                        "config_env_parse_failed",
                        env_var=param.env_var,
                        error=str(exc),
                    )
                    parsed = param.default
                self._cache[cache_key] = (now, parsed)
                return parsed

        # Hard-coded default.
        if param is not None:
            self._cache[cache_key] = (now, param.default)
            return param.default
        return default

    async def set(
        self,
        user_id: UUID,
        parameter_path: str,
        value: Any,
        source: str = "principal",
    ) -> None:
        if source not in ("default", "principal", "adaptation"):
            raise ValueError(f"invalid source {source!r}")
        async with MemorySessionLocal() as session:
            stmt = pg_insert(SystemConfig.__table__).values(
                user_id=user_id,
                parameter_path=parameter_path,
                value=value,
                source=source,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=[SystemConfig.user_id, SystemConfig.parameter_path],
                set_={
                    "value": stmt.excluded.value,
                    "source": stmt.excluded.source,
                    "updated_at": text("now()"),
                },
            )
            await session.execute(stmt)
            await session.commit()
        self._cache.pop(f"{user_id}|{parameter_path}", None)

    async def get_all(self, user_id: UUID, prefix: str = "") -> dict[str, Any]:
        """Return resolved values for every registered parameter (and optionally only those under prefix)."""
        out: dict[str, Any] = {}
        for path in PARAMETERS:
            if prefix and not path.startswith(prefix):
                continue
            out[path] = await self.get(user_id, path)
        return out

    def invalidate(self, user_id: UUID) -> None:
        prefix = f"{user_id}|"
        keys = [k for k in self._cache if k.startswith(prefix)]
        for k in keys:
            self._cache.pop(k, None)
        _log.info("config_cache_invalidated", user_id=str(user_id), entries=len(keys))


_resolver_singleton: ConfigResolver | None = None


def get_config_resolver() -> ConfigResolver:
    global _resolver_singleton
    if _resolver_singleton is None:
        _resolver_singleton = ConfigResolver()
    return _resolver_singleton
