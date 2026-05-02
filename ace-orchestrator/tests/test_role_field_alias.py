"""Patch 02 §A.4 backward compatibility for the `role` field rename."""
from __future__ import annotations

import uuid
from datetime import datetime

from orchestrator.schemas.user import UserOut


def test_pydantic_role_property_falls_back_to_principal():
    obj = UserOut(
        id=uuid.uuid4(),
        identifier="x",
        display_name="X",
        active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    # default_session_role unset → role property returns 'principal'
    assert obj.role == "principal"
    assert obj.default_session_role is None


def test_pydantic_role_property_reflects_default_session_role():
    obj = UserOut(
        id=uuid.uuid4(),
        identifier="x",
        display_name="X",
        default_session_role="caregiver",
        active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    assert obj.role == "caregiver"


def test_pydantic_role_appears_in_model_dump_for_back_compat():
    obj = UserOut(
        id=uuid.uuid4(),
        identifier="x",
        display_name="X",
        default_session_role="principal",
        active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    dump = obj.model_dump()
    assert "role" in dump
    assert dump["role"] == "principal"
    assert dump["default_session_role"] == "principal"


def test_account_type_default_is_principal():
    obj = UserOut(
        id=uuid.uuid4(),
        identifier="x",
        display_name="X",
        active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    assert obj.account_type == "principal"


def test_timezone_default_is_la():
    obj = UserOut(
        id=uuid.uuid4(),
        identifier="x",
        display_name="X",
        active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )
    assert obj.timezone == "America/Los_Angeles"


def test_sqlalchemy_user_role_property_falls_back():
    from orchestrator.db.models import User

    u = User(identifier="x", display_name="X")
    u.default_session_role = None
    assert u.role == "principal"
    u.default_session_role = "collaborator"
    assert u.role == "collaborator"
