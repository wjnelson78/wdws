from orchestrator.services.prompt_assembler import (
    DOMAIN_TO_BLOCK,
    PromptAssembler,
    default_runtime_context,
    detect_domain,
)


def _stub_blocks():
    """Hand-craft a PromptAssembler with in-memory blocks (no DB)."""
    asm = PromptAssembler()
    asm._version = "v2.0"

    class Stub:
        def __init__(self, name, content, cache_ttl="1h"):
            self.block_name = name
            self.content = content
            self.cache_ttl = cache_ttl
            self.sort_order = 0

    asm._blocks = {
        "SYSTEM_CORE": Stub(
            "SYSTEM_CORE", "Core for {{USER_NAME}} on {{CURRENT_DATE}}"
        ),
        "ROLE_OVERLAY_PRINCIPAL": Stub("ROLE_OVERLAY_PRINCIPAL", "principal overlay"),
        "ROLE_OVERLAY_CAREGIVER": Stub("ROLE_OVERLAY_CAREGIVER", "caregiver overlay"),
        "ROLE_OVERLAY_COLLABORATOR": Stub(
            "ROLE_OVERLAY_COLLABORATOR", "collaborator overlay"
        ),
        "DOMAIN_LEGAL": Stub("DOMAIN_LEGAL", "legal block"),
        "DOMAIN_COMMS": Stub("DOMAIN_COMMS", "comms block"),
        "DOMAIN_MEDICAL": Stub("DOMAIN_MEDICAL", "medical block"),
        "DOMAIN_OSINT": Stub("DOMAIN_OSINT", "osint block"),
        "DOMAIN_DOCHANDLING": Stub("DOMAIN_DOCHANDLING", "doc block"),
    }
    return asm


def test_default_assembly_order_legal_principal():
    asm = _stub_blocks()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="profile body",
        active_case_context_block="cases body",
        case_file_block=None,
        runtime_context={"CURRENT_DATE": "2026-04-21"},
        substitutions={"USER_NAME": "Will", "CURRENT_DATE": "2026-04-21"},
    )
    assert [b.name for b in out] == [
        "SYSTEM_CORE",
        "ROLE_OVERLAY_PRINCIPAL",
        "DOMAIN_LEGAL",
        "DOMAIN_COMMS",
        "USER_PROFILE",
        "ACTIVE_CASE_CONTEXT",
        "RUNTIME_CONTEXT",
    ]


def test_substitution_only_uppercase_tokens():
    asm = _stub_blocks()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="see {{user_profile.preferred_name}} and {{USER_NAME}}",
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
        substitutions={"USER_NAME": "Will"},
    )
    profile = next(b for b in out if b.name == "USER_PROFILE")
    assert "Will" in profile.text
    assert "{{user_profile.preferred_name}}" in profile.text


def test_role_drives_overlay_block():
    asm = _stub_blocks()
    out = asm.assemble(
        domain="medical",
        role="caregiver",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    names = [b.name for b in out]
    assert "ROLE_OVERLAY_CAREGIVER" in names
    assert "ROLE_OVERLAY_PRINCIPAL" not in names


def test_comms_domain_does_not_double_include_comms():
    asm = _stub_blocks()
    out = asm.assemble(
        domain="comms",
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    names = [b.name for b in out]
    assert names.count("DOMAIN_COMMS") == 1


def test_only_runtime_context_is_uncacheable():
    asm = _stub_blocks()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="profile",
        active_case_context_block="cases",
        case_file_block="case file",
        runtime_context={},
    )
    cacheable_names = {b.name for b in out if b.cacheable}
    uncacheable_names = {b.name for b in out if not b.cacheable}
    assert "RUNTIME_CONTEXT" in uncacheable_names
    assert "CASE_FILE" in cacheable_names
    assert "USER_PROFILE" in cacheable_names


def test_all_domain_blocks_mapped():
    for domain, block in DOMAIN_TO_BLOCK.items():
        assert block.startswith("DOMAIN_"), domain


def test_detect_domain_active_case_wins():
    assert detect_domain(active_case_id="2:26-cv-00776", message_text="hi") == "legal"


def test_detect_domain_medical_keyword():
    assert detect_domain(active_case_id=None, message_text="my doctor said") == "medical"


def test_detect_domain_default_legal():
    assert detect_domain(active_case_id=None, message_text="random text") == "legal"


def test_detect_domain_comms_keyword():
    assert detect_domain(active_case_id=None, message_text="reply to that email") == "comms"


def test_default_runtime_context_has_date_and_datetime():
    rc = default_runtime_context()
    assert "CURRENT_DATE" in rc
    assert "CURRENT_DATETIME" in rc
