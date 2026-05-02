"""Live test: retriever surfaces relevant claims into MEMORY_CONTEXT (M2.2)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def fixture_user_id():
    """Synthetic principal with a small entity+claim graph."""
    from orchestrator.db.memory_models import Episode
    from orchestrator.db.models import User
    from orchestrator.db.session import MemorySessionLocal, SessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateClaim,
        CandidateEntity,
        resolve_turn,
    )

    suffix = uuid.uuid4().hex[:8]
    identifier = f"test.retr.{suffix}"
    try:
        async with SessionLocal() as s:
            user = User(
                identifier=identifier,
                display_name=f"Test Retr {suffix}",
                default_session_role="principal",
                preferences={"onboarding_incomplete": False},
                active=True,
            )
            s.add(user)
            await s.flush()
            uid = user.id
            await s.commit()
    except Exception as exc:
        pytest.skip(f"orchestrator DB unreachable: {exc}")

    async with MemorySessionLocal() as session:
        ep = Episode(
            user_id=uid,
            occurred_at=datetime.now(timezone.utc),
            event_type="conversation_turn",
            participants=["test"],
            summary="Carlisle assigned to BIIA case",
            full_text="Carlisle was appointed counsel by the BIIA on 2026-04-01.",
            salience=0.9,
            retention_tier="full",
        )
        session.add(ep)
        await session.flush()
        ep_id = ep.id
        await session.commit()

    async with MemorySessionLocal() as session:
        ents = [
            CandidateEntity("person", "Chris Carlisle", ["Carlisle"], {"firm": "CBCL"}),
            CandidateEntity("organization", "BIIA", [], {}),
        ]
        claims = [
            CandidateClaim("Chris Carlisle", "person", "represents", None, None, "Will Nelson", 0.95),
            CandidateClaim("Chris Carlisle", "person", "appointed_on", None, None, "2026-04-01", 0.95),
            CandidateClaim("Chris Carlisle", "person", "works_at", "CBCL", "organization", None, 0.9),
        ]
        # CBCL won't resolve as object since not in entities — set object_value path instead.
        claims[2] = CandidateClaim("Chris Carlisle", "person", "works_at", None, None, "CBCL", 0.9)
        await resolve_turn(session, uid, ents, claims, source_episode_id=ep_id)
        await session.commit()

    yield uid

    async with SessionLocal() as s:
        u = await s.get(User, uid)
        if u is not None:
            await s.delete(u)
            await s.commit()


async def test_retriever_surfaces_claims_about_named_entity(fixture_user_id):
    from orchestrator.services.memory_system.retriever import MemoryRetriever

    retr = MemoryRetriever()
    result = await retr.build_memory_context(
        user_id=fixture_user_id,
        user_message="Tell me what we know about Carlisle and the BIIA matter.",
        active_case_id=None,
    )
    assert result.claims_returned >= 1
    assert "Relevant facts" in result.block
    assert "Chris Carlisle" in result.block
    assert "represents" in result.block or "appointed_on" in result.block or "works_at" in result.block


async def test_retriever_returns_no_claims_when_message_unrelated(fixture_user_id):
    from orchestrator.services.memory_system.retriever import MemoryRetriever

    retr = MemoryRetriever()
    result = await retr.build_memory_context(
        user_id=fixture_user_id,
        user_message="What time should I leave for the airport?",
        active_case_id=None,
    )
    assert result.claims_returned == 0
