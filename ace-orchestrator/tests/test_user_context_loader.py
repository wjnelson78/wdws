"""Live tests against the seeded athena DB. Skip if DB unreachable."""
from __future__ import annotations

import pytest
import pytest_asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def session():
    from orchestrator.db.session import SessionLocal

    try:
        async with SessionLocal() as s:
            yield s
    except Exception as exc:
        pytest.skip(f"athena DB unreachable: {exc}")


async def test_loads_will_with_full_context(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    rendered = await loader.load(session, "will.nelson")
    assert rendered.identifier == "will.nelson"
    assert rendered.role == "principal"
    assert rendered.preferred_name == "Will"
    assert len(rendered.raw_contexts) == 38
    assert len(rendered.raw_cases) == 8  # one is historical_precedent → excluded


async def test_user_profile_block_includes_signature_and_contexts(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    rendered = await loader.load(session, "will.nelson")
    block = rendered.user_profile_block
    assert "## User Profile" in block
    assert "Pro se federal litigant" in block  # authorization context
    assert "Chris Carlisle" in block  # counsel context content
    assert "#### counsel" in block
    assert "#### provider" in block


async def test_active_case_block_has_top_priority_first(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    rendered = await loader.load(session, "will.nelson")
    block = rendered.active_case_context_block
    snohomish_idx = block.index("2:26-cv-00776-JCC")
    iaff_idx = block.index("26-2-00762-31")
    assert snohomish_idx < iaff_idx  # priority 95 before 85


async def test_active_case_block_excludes_historical(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    rendered = await loader.load(session, "will.nelson")
    assert "19-2-03310-31" not in rendered.active_case_context_block


async def test_cache_hit_returns_same_object(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    a = await loader.load(session, "will.nelson")
    b = await loader.load(session, "will.nelson")
    assert a is b


async def test_invalidate_clears_cache(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    a = await loader.load(session, "will.nelson")
    loader.invalidate(a.user_id)
    b = await loader.load(session, "will.nelson")
    assert a is not b
    assert a.content_hash == b.content_hash  # state hasn't changed


async def test_unknown_user_raises(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    with pytest.raises(LookupError, match="not found"):
        await loader.load(session, "no.such.user")


async def test_jessie_caregiver_renders(session):
    from orchestrator.services.user_context_loader import UserContextLoader

    loader = UserContextLoader()
    rendered = await loader.load(session, "jessie.nelson")
    assert rendered.role == "caregiver"
    assert rendered.preferred_name == "Jessie"
    assert "Warm regards" in rendered.user_profile_block
    assert rendered.raw_cases == []  # jessie has no case registry entries
