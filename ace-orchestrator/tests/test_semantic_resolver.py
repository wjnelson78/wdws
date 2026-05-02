"""Live tests for entity + claim resolution against the seeded athena DB.

Uses a synthetic test user so it doesn't pollute Will's or Jessie's data.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import select

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def test_user_id():
    """Create a temporary principal solely for resolver tests; clean up after."""
    from orchestrator.db.models import User
    from orchestrator.db.session import SessionLocal

    suffix = uuid.uuid4().hex[:8]
    identifier = f"test.semantic.{suffix}"

    try:
        async with SessionLocal() as s:
            user = User(
                identifier=identifier,
                display_name=f"Test Semantic {suffix}",
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

    yield uid

    async with SessionLocal() as s:
        u = await s.get(User, uid)
        if u is not None:
            await s.delete(u)
            await s.commit()


async def test_inserts_new_entities(test_user_id):
    from orchestrator.db.session import MemorySessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateEntity,
        resolve_entities,
    )

    candidates = [
        CandidateEntity(
            entity_type="person",
            canonical_name="Chris Carlisle",
            aliases=["Carlisle"],
            attributes={"firm": "CBCL"},
        ),
        CandidateEntity(
            entity_type="organization",
            canonical_name="Carlisle Barnett Carlisle",
            aliases=["CBCL"],
            attributes={},
        ),
    ]
    async with MemorySessionLocal() as session:
        index, inserts, reinforces = await resolve_entities(session, test_user_id, candidates)
        await session.commit()
    assert inserts == 2
    assert reinforces == 0
    assert ("person", "Chris Carlisle") in index


async def test_reinforces_existing_entity_and_merges_aliases(test_user_id):
    from orchestrator.db.memory_models import SemanticEntity
    from orchestrator.db.session import MemorySessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateEntity,
        resolve_entities,
    )

    candidates_first = [
        CandidateEntity("person", "Beth Davis", ["Beth"], {"firm": "Kantor & Kantor"}),
    ]
    async with MemorySessionLocal() as session:
        await resolve_entities(session, test_user_id, candidates_first)
        await session.commit()

    candidates_second = [
        CandidateEntity("person", "Beth Davis", ["BD"], {"role": "co-counsel"}),
    ]
    async with MemorySessionLocal() as session:
        index, inserts, reinforces = await resolve_entities(
            session, test_user_id, candidates_second
        )
        await session.commit()
        ent = await session.execute(
            select(SemanticEntity).where(
                SemanticEntity.user_id == test_user_id,
                SemanticEntity.entity_type == "person",
                SemanticEntity.canonical_name == "Beth Davis",
            )
        )
        row = ent.scalar_one()
    assert inserts == 0
    assert reinforces == 1
    assert row.reference_count == 2
    assert "Beth" in row.aliases
    assert "BD" in row.aliases
    # First-write attribute preserved; new attribute added.
    assert row.attributes.get("firm") == "Kantor & Kantor"
    assert row.attributes.get("role") == "co-counsel"


async def test_claim_dedup_appends_source_episode(test_user_id):
    from orchestrator.db.memory_models import Episode, SemanticClaim
    from orchestrator.db.session import MemorySessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateClaim,
        CandidateEntity,
        resolve_turn,
    )

    async with MemorySessionLocal() as session:
        # Two episodes
        ep1 = Episode(
            user_id=test_user_id,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            event_type="conversation_turn",
            participants=["test"],
            summary="seed1",
            full_text="seed1",
            salience=0.5,
            retention_tier="full",
        )
        ep2 = Episode(
            user_id=test_user_id,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            event_type="conversation_turn",
            participants=["test"],
            summary="seed2",
            full_text="seed2",
            salience=0.5,
            retention_tier="full",
        )
        session.add_all([ep1, ep2])
        await session.flush()
        ep1_id = ep1.id
        ep2_id = ep2.id

        ents = [CandidateEntity("person", "Diaz Provider", [], {})]
        claims = [CandidateClaim("Diaz Provider", "person", "has_email", None, None, "RDResearch@providence.org", 0.9)]

        await resolve_turn(session, test_user_id, ents, claims, source_episode_id=ep1_id)
        await session.commit()

    async with MemorySessionLocal() as session:
        # Same claim again from a second episode should reinforce, not insert.
        ents = []
        claims = [CandidateClaim("Diaz Provider", "person", "has_email", None, None, "RDResearch@providence.org", 0.95)]
        result = await resolve_turn(session, test_user_id, ents, claims, source_episode_id=ep2_id)
        await session.commit()
    assert result.claim_inserts == 0
    assert result.claim_reinforcements == 1

    async with MemorySessionLocal() as session:
        rows = (
            await session.execute(
                select(SemanticClaim).where(SemanticClaim.user_id == test_user_id)
            )
        ).scalars().all()
        rows = [r for r in rows if r.predicate == "has_email"]
    assert len(rows) == 1
    assert ep1_id in rows[0].source_episode_ids
    assert ep2_id in rows[0].source_episode_ids
    # Confidence should have been bumped to the higher value.
    assert float(rows[0].confidence) == 0.95


async def test_claim_supersession_when_object_changes(test_user_id):
    from orchestrator.db.memory_models import Episode, SemanticClaim
    from orchestrator.db.session import MemorySessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateClaim,
        CandidateEntity,
        resolve_turn,
    )

    async def _new_episode():
        async with MemorySessionLocal() as session:
            ep = Episode(
                user_id=test_user_id,
                occurred_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                event_type="conversation_turn",
                participants=["test"],
                summary="x",
                full_text="x",
                salience=0.5,
                retention_tier="full",
            )
            session.add(ep)
            await session.flush()
            ep_id = ep.id
            await session.commit()
        return ep_id

    ep1 = await _new_episode()
    async with MemorySessionLocal() as session:
        ents = [CandidateEntity("person", "Jones Counsel", [], {})]
        claims = [CandidateClaim("Jones Counsel", "person", "works_at", None, None, "Old Firm LLP", 0.85)]
        await resolve_turn(session, test_user_id, ents, claims, source_episode_id=ep1)
        await session.commit()

    ep2 = await _new_episode()
    async with MemorySessionLocal() as session:
        ents = []
        claims = [CandidateClaim("Jones Counsel", "person", "works_at", None, None, "New Firm LLP", 0.9)]
        result = await resolve_turn(session, test_user_id, ents, claims, source_episode_id=ep2)
        await session.commit()
    assert result.claim_inserts == 1
    assert result.claim_supersessions == 1

    async with MemorySessionLocal() as session:
        rows = (
            await session.execute(
                select(SemanticClaim).where(SemanticClaim.user_id == test_user_id)
            )
        ).scalars().all()
        works_at = [r for r in rows if r.predicate == "works_at"]
    assert len(works_at) == 2
    active = [r for r in works_at if r.valid_to is None]
    superseded = [r for r in works_at if r.valid_to is not None]
    assert len(active) == 1 and active[0].object_value == "New Firm LLP"
    assert len(superseded) == 1 and superseded[0].object_value == "Old Firm LLP"
    assert superseded[0].superseded_by == active[0].id


async def test_claim_with_unknown_subject_is_skipped(test_user_id):
    from orchestrator.db.memory_models import Episode
    from orchestrator.db.session import MemorySessionLocal
    from orchestrator.services.memory_system.semantic_resolver import (
        CandidateClaim,
        resolve_turn,
    )

    async with MemorySessionLocal() as session:
        ep = Episode(
            user_id=test_user_id,
            occurred_at=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            event_type="conversation_turn",
            participants=["test"],
            summary="x",
            full_text="x",
            salience=0.5,
            retention_tier="full",
        )
        session.add(ep)
        await session.flush()
        ep_id = ep.id
        await session.commit()

    async with MemorySessionLocal() as session:
        # Subject doesn't exist anywhere — should be silently skipped.
        claims = [CandidateClaim("Nobody Atall", "person", "is_a", None, None, "ghost", 0.9)]
        result = await resolve_turn(session, test_user_id, [], claims, source_episode_id=ep_id)
        await session.commit()
    assert result.claim_inserts == 0
    assert result.claim_reinforcements == 0
