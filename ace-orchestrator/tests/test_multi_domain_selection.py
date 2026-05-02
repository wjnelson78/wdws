"""Patch 02 §D: multi-domain block composition."""
from orchestrator.services.cache_strategist import to_anthropic_system
from orchestrator.services.prompt_assembler import (
    DOMAIN_TO_BLOCK,
    PromptAssembler,
    select_domain_blocks,
)


def _stub() -> PromptAssembler:
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
        "ONBOARDING_OVERLAY": Stub("ONBOARDING_OVERLAY", "onboarding"),
    }
    return asm


def test_select_picks_top_two():
    out = select_domain_blocks([("legal", 0.9), ("medical", 0.7), ("osint", 0.5)])
    assert out == ["legal", "medical"]


def test_select_drops_low_confidence_secondary():
    out = select_domain_blocks([("legal", 0.9), ("medical", 0.3)])
    assert out == ["legal"]


def test_select_dedupes():
    out = select_domain_blocks([("legal", 0.9), ("legal", 0.5), ("medical", 0.6)])
    assert out == ["legal", "medical"]


def test_select_skips_unknown_domains():
    out = select_domain_blocks([("legal", 0.9), ("alien", 0.7)])
    assert out == ["legal"]


def test_assemble_with_two_domains_emits_both_in_sorted_order():
    asm = _stub()
    out = asm.assemble(
        domains=["legal", "medical"],
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    names = [b.name for b in out]
    legal_idx = names.index("DOMAIN_LEGAL")
    medical_idx = names.index("DOMAIN_MEDICAL")
    assert legal_idx < medical_idx  # alphabetical: DOMAIN_LEGAL < DOMAIN_MEDICAL
    assert "DOMAIN_COMMS" in names


def test_assemble_input_order_independence_for_cache_stability():
    asm = _stub()
    out_a = asm.assemble(
        domains=["medical", "legal"],
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    out_b = asm.assemble(
        domains=["legal", "medical"],
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    assert [b.name for b in out_a] == [b.name for b in out_b]


def test_comms_only_once_when_chosen_as_primary():
    asm = _stub()
    out = asm.assemble(
        domains=["comms"],
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    names = [b.name for b in out]
    assert names.count("DOMAIN_COMMS") == 1


def test_legacy_single_domain_param_still_works():
    asm = _stub()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block=None,
        active_case_context_block=None,
        case_file_block=None,
        runtime_context={},
    )
    names = [b.name for b in out]
    assert names.count("DOMAIN_LEGAL") == 1
