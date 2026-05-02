from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field


class UserOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    identifier: str
    account_type: str = "principal"
    display_name: str
    preferred_name: str | None = None
    primary_email: str | None = None
    primary_phone: str | None = None
    default_session_role: str | None = None
    timezone: str = "America/Los_Angeles"
    authorization_context: str | None = None
    primary_sending_email: str | None = None
    default_cc: list[str] = Field(default_factory=list)
    privileged_contacts: list[str] = Field(default_factory=list)
    signature_image_ace_doc_id: str | None = None
    signing_script_ace_doc_id: str | None = None
    preferences: dict[str, Any] = Field(default_factory=dict)
    active: bool
    created_at: datetime
    updated_at: datetime

    @computed_field  # type: ignore[prop-decorator]
    @property
    def role(self) -> str:
        """Patch 02 §A.4 deprecated alias retained for back-compat. Removed in Phase 3."""
        return self.default_session_role or "principal"


class UserContextOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    context_type: str
    key: str
    content: dict[str, Any]
    priority: int
    active: bool
    updated_at: datetime


class UserCaseOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    user_id: UUID
    case_id: str
    caption: str | None = None
    court: str | None = None
    judge: str | None = None
    tribunal: str | None = None
    role: str | None = None
    status: str
    opposing_counsel: Any | None = None
    named_defendants_include: Any | None = None
    subject: str | None = None
    active_workstream: str | None = None
    next_deadline: date | None = None
    notes: str | None = None
    priority: int
    context_refs: list[str] | None = None
    created_at: datetime
    updated_at: datetime


class RenderedUserContextOut(BaseModel):
    user_id: UUID
    identifier: str
    display_name: str
    preferred_name: str | None
    role: str
    user_profile_block: str
    active_case_context_block: str
    content_hash: str
    rendered_at: datetime
    contexts_count: int
    cases_count: int
