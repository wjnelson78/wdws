"""Live tests against the memory engine + embedding service. Skip if either is unreachable."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def memory_session():
    from orchestrator.db.session import MemorySessionLocal

    try:
        async with MemorySessionLocal() as s:
            yield s
    except Exception as exc:
        pytest.skip(f"memory DB unreachable: {exc}")


@pytest_asyncio.fixture(loop_scope="session")
async def will_user_id():
    from sqlalchemy import select

    from orchestrator.db.models import User
    from orchestrator.db.session import SessionLocal

    try:
        async with SessionLocal() as s:
            result = await s.execute(select(User).where(User.identifier == "will.nelson"))
            user = result.scalar_one_or_none()
    except Exception as exc:
        pytest.skip(f"orchestrator DB unreachable: {exc}")
    if user is None:
        pytest.skip("will.nelson not seeded — run scripts/bootstrap_user_context.py first")
    return user.id


@pytest_asyncio.fixture(loop_scope="session")
async def seeded_episode(memory_session, will_user_id):
    """Insert one episode with a known embedding-via-service. Cleans up after."""
    from orchestrator.db.memory_models import Episode
    from orchestrator.services.memory_system.embedding_client import (
        EmbeddingUnavailable,
        get_embedding_client,
    )

    text = "Will asked about the Snohomish PRR deadline; Athena confirmed April 24, 2026."
    try:
        embedding = await get_embedding_client().embed(text)
    except EmbeddingUnavailable:
        pytest.skip("embedding service unreachable on 9098")

    # Put it >48h back so it does NOT appear in the recent window — exercises
    # the similarity path cleanly without dedup interference.
    ep = Episode(
        user_id=will_user_id,
        occurred_at=datetime.now(timezone.utc) - timedelta(days=5),
        event_type="conversation_turn",
        participants=["user:will.nelson", "athena"],
        domain="legal",
        summary="Asked about Snohomish PRR deadline; confirmed April 24.",
        full_text=text,
        linked_case_id="2:26-cv-00776-JCC",
        salience=0.85,
        emotional_valence=0.0,
        embedding=embedding,
        retention_tier="full",
    )
    memory_session.add(ep)
    await memory_session.flush()
    ep_id = ep.id
    await memory_session.commit()

    yield ep_id

    # Cleanup
    from orchestrator.db.memory_models import Episode as Ep

    obj = await memory_session.get(Ep, ep_id)
    if obj is not None:
        await memory_session.delete(obj)
        await memory_session.commit()


async def test_retriever_surfaces_seeded_episode(seeded_episode, will_user_id):
    from orchestrator.services.memory_system.retriever import MemoryRetriever

    retriever = MemoryRetriever()
    result = await retriever.build_memory_context(
        user_id=will_user_id,
        user_message="when is the PRR response due?",
        active_case_id=None,
    )
    assert result.episodes_returned + result.similar_episodes_returned >= 1
    assert "Snohomish" in result.block


async def test_retriever_similarity_finds_seeded_episode(seeded_episode, will_user_id):
    from orchestrator.services.memory_system.retriever import MemoryRetriever

    retriever = MemoryRetriever()
    result = await retriever.build_memory_context(
        user_id=will_user_id,
        user_message="public records request response window",
        active_case_id=None,
    )
    # Even without exact-keyword overlap, BGE-M3 should pull the seeded episode in.
    assert result.similar_episodes_returned >= 1


async def test_retriever_disabled_returns_empty(monkeypatch, will_user_id):
    from orchestrator.config import get_settings
    from orchestrator.services.memory_system.retriever import MemoryRetriever

    settings = get_settings()
    monkeypatch.setattr(settings, "memory_retrieval_enabled", False)
    retriever = MemoryRetriever()
    result = await retriever.build_memory_context(
        user_id=will_user_id,
        user_message="anything",
        active_case_id=None,
    )
    assert result.block == ""
    assert result.episodes_returned == 0
