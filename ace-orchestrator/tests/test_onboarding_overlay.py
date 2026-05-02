"""Patch 02 §C: onboarding overlay block injection."""
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from sqlalchemy import select

from orchestrator.services.prompt_assembler import PromptAssembler


def _stub_assembler() -> PromptAssembler:
    asm = PromptAssembler()
    asm._version = "v2.0"

    class Stub:
        def __init__(self, name, content):
            self.block_name = name
            self.content = content
            self.cache_ttl = "1h"
            self.sort_order = 0

    asm._blocks = {
        "SYSTEM_CORE": Stub("SYSTEM_CORE", "core"),
        "ROLE_OVERLAY_PRINCIPAL": Stub("ROLE_OVERLAY_PRINCIPAL", "p"),
        "ROLE_OVERLAY_CAREGIVER": Stub("ROLE_OVERLAY_CAREGIVER", "c"),
        "ROLE_OVERLAY_COLLABORATOR": Stub("ROLE_OVERLAY_COLLABORATOR", "x"),
        "DOMAIN_LEGAL": Stub("DOMAIN_LEGAL", "legal"),
        "DOMAIN_COMMS": Stub("DOMAIN_COMMS", "comms"),
        "DOMAIN_MEDICAL": Stub("DOMAIN_MEDICAL", "med"),
        "DOMAIN_OSINT": Stub("DOMAIN_OSINT", "osint"),
        "DOMAIN_DOCHANDLING": Stub("DOMAIN_DOCHANDLING", "doc"),
        "ONBOARDING_OVERLAY": Stub("ONBOARDING_OVERLAY", "onboarding instructions"),
    }
    return asm


def test_overlay_injected_between_role_overlay_and_domain():
    asm = _stub_assembler()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
        is_onboarding=True,
    )
    names = [b.name for b in out]
    role_idx = names.index("ROLE_OVERLAY_PRINCIPAL")
    overlay_idx = names.index("ONBOARDING_OVERLAY")
    legal_idx = names.index("DOMAIN_LEGAL")
    assert role_idx < overlay_idx < legal_idx


def test_overlay_omitted_when_not_onboarding():
    asm = _stub_assembler()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
        is_onboarding=False,
    )
    names = [b.name for b in out]
    assert "ONBOARDING_OVERLAY" not in names


# Live detection tests against the seeded athena DB.
pytestmark_live = pytest.mark.asyncio


@pytest_asyncio.fixture(loop_scope="session")
async def will_user():
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
    return user


@pytest.mark.asyncio
async def test_will_is_not_onboarding(will_user):
    """Patch 02 §C.6: Will and Jessie were bootstrapped manually with full context."""
    from orchestrator.db.session import SessionLocal
    from orchestrator.services.onboarding import detect_onboarding

    async with SessionLocal() as s:
        state = await detect_onboarding(s, will_user)
    assert state.is_onboarding is False, f"Will incorrectly detected as onboarding: {state.reasons}"


@pytest.mark.asyncio
async def test_explicit_flag_triggers_onboarding(will_user):
    """If preferences.onboarding_incomplete=true, detection flips on regardless of other state."""
    from orchestrator.db.session import SessionLocal
    from orchestrator.services.onboarding import detect_onboarding

    fake = type(will_user)(
        identifier="x",
        display_name="Test",
        default_session_role="principal",
        preferences={"onboarding_incomplete": True},
        signature_block=will_user.signature_block,
        primary_sending_email=will_user.primary_sending_email,
        default_cc=will_user.default_cc,
        primary_phone=will_user.primary_phone,
        active=True,
        created_at=datetime.now(timezone.utc) - timedelta(days=365),
    )
    async with SessionLocal() as s:
        state = await detect_onboarding(s, fake)
    # The fake row isn't in the DB so case/context counts will be 0;
    # both the explicit flag AND context_underpopulated should fire.
    assert state.is_onboarding
    assert "preferences.onboarding_incomplete" in state.reasons
