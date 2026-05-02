"""Pick the role-overlay block name for a given user role."""
from __future__ import annotations

from orchestrator.logging_config import get_logger

_log = get_logger(__name__)

VALID_ROLES = {"principal", "caregiver", "collaborator"}
ROLE_TO_BLOCK = {
    "principal": "ROLE_OVERLAY_PRINCIPAL",
    "caregiver": "ROLE_OVERLAY_CAREGIVER",
    "collaborator": "ROLE_OVERLAY_COLLABORATOR",
}


def select_role_overlay_block(role: str | None) -> str:
    """Return the system_prompt_blocks.block_name for the role.

    Unknown / missing roles fall back to principal and log a warning.
    The v2.0 core (§3.4) instructs Athena to ask for role confirmation in this case.
    """
    if role is None or role not in VALID_ROLES:
        _log.warning("role_overlay_fallback", requested=role, fallback="principal")
        return ROLE_TO_BLOCK["principal"]
    return ROLE_TO_BLOCK[role]
