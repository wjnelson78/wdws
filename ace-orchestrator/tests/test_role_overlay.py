from orchestrator.services.role_overlay import select_role_overlay_block


def test_principal_maps_to_principal_block():
    assert select_role_overlay_block("principal") == "ROLE_OVERLAY_PRINCIPAL"


def test_caregiver_maps_to_caregiver_block():
    assert select_role_overlay_block("caregiver") == "ROLE_OVERLAY_CAREGIVER"


def test_collaborator_maps_to_collaborator_block():
    assert select_role_overlay_block("collaborator") == "ROLE_OVERLAY_COLLABORATOR"


def test_unknown_role_falls_back_to_principal():
    assert select_role_overlay_block("admin") == "ROLE_OVERLAY_PRINCIPAL"


def test_none_role_falls_back_to_principal():
    assert select_role_overlay_block(None) == "ROLE_OVERLAY_PRINCIPAL"
