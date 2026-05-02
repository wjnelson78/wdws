"""Live tests for ConfigResolver against the seeded athena DB."""
from __future__ import annotations

import pytest
import pytest_asyncio
from sqlalchemy import select

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def will_id():
    from orchestrator.db.models import User
    from orchestrator.db.session import SessionLocal

    try:
        async with SessionLocal() as s:
            r = await s.execute(select(User).where(User.identifier == "will.nelson"))
            user = r.scalar_one_or_none()
    except Exception as exc:
        pytest.skip(f"orchestrator DB unreachable: {exc}")
    if user is None:
        pytest.skip("will.nelson not seeded")
    return user.id


@pytest_asyncio.fixture(loop_scope="session")
async def jessie_id():
    from orchestrator.db.models import User
    from orchestrator.db.session import SessionLocal

    async with SessionLocal() as s:
        r = await s.execute(select(User).where(User.identifier == "jessie.nelson"))
        user = r.scalar_one_or_none()
    if user is None:
        pytest.skip("jessie.nelson not seeded")
    return user.id


async def test_resolves_seeded_default(will_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    value = await resolver.get(will_id, "council_gate.mini_threshold")
    assert value == 0.4


async def test_unknown_param_returns_default_argument(will_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    value = await resolver.get(will_id, "no.such.param", default="fallback")
    assert value == "fallback"


async def test_set_then_get_round_trips(will_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    await resolver.set(will_id, "council_gate.mini_threshold", 0.55, source="principal")
    value = await resolver.get(will_id, "council_gate.mini_threshold")
    assert value == 0.55
    # restore
    await resolver.set(will_id, "council_gate.mini_threshold", 0.4, source="default")


async def test_per_principal_divergence(will_id, jessie_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    await resolver.set(will_id, "council_gate.mini_threshold", 0.50, source="principal")
    await resolver.set(jessie_id, "council_gate.mini_threshold", 0.30, source="principal")
    will_v = await resolver.get(will_id, "council_gate.mini_threshold")
    jessie_v = await resolver.get(jessie_id, "council_gate.mini_threshold")
    assert will_v == 0.50
    assert jessie_v == 0.30
    # restore
    await resolver.set(will_id, "council_gate.mini_threshold", 0.4, source="default")
    await resolver.set(jessie_id, "council_gate.mini_threshold", 0.4, source="default")


async def test_invalidate_clears_cache(will_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    a = await resolver.get(will_id, "council_gate.mini_threshold")
    cache_keys_before = set(resolver._cache.keys())
    assert any(k.startswith(f"{will_id}|") for k in cache_keys_before)
    resolver.invalidate(will_id)
    assert not any(k.startswith(f"{will_id}|") for k in resolver._cache.keys())


async def test_get_all_with_prefix(will_id):
    from orchestrator.services.config_resolver import ConfigResolver

    resolver = ConfigResolver()
    council_only = await resolver.get_all(will_id, prefix="council_gate.")
    assert all(k.startswith("council_gate.") for k in council_only)
    assert "council_gate.mini_threshold" in council_only
    assert "dreaming.daily_budget_usd" not in council_only
