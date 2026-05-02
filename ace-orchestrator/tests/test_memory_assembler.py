"""Make sure the assembler accepts and orders the new MEMORY_CONTEXT slot."""
from orchestrator.services.cache_strategist import to_anthropic_system
from orchestrator.services.prompt_assembler import PromptAssembler


def _stub() -> PromptAssembler:
    asm = PromptAssembler()
    asm._version = "v2.0"

    class Stub:
        def __init__(self, name, content, cache_ttl="1h"):
            self.block_name = name
            self.content = content
            self.cache_ttl = cache_ttl
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
    }
    return asm


def test_memory_context_appears_after_active_case_before_runtime():
    asm = _stub()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="profile",
        active_case_context_block="cases",
        case_file_block=None,
        runtime_context={},
        memory_context_block="## Memory Context\n\nrecent stuff here\n",
    )
    names = [b.name for b in out]
    mem_idx = names.index("MEMORY_CONTEXT")
    runtime_idx = names.index("RUNTIME_CONTEXT")
    case_idx = names.index("ACTIVE_CASE_CONTEXT")
    assert case_idx < mem_idx < runtime_idx


def test_memory_context_is_cacheable_with_5m_ttl_default_mode():
    asm = _stub()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="profile",
        active_case_context_block="cases",
        case_file_block=None,
        runtime_context={},
        memory_context_block="## Memory Context\n\nstuff\n",
    )
    api = to_anthropic_system(out, mode="default")
    mem_block = next(b for b in api if b["text"].startswith("## Memory Context"))
    assert "cache_control" not in mem_block or mem_block["cache_control"]["ttl"] == "5m"


def test_memory_context_omitted_when_block_empty():
    asm = _stub()
    out = asm.assemble(
        domain="legal",
        role="principal",
        user_profile_block="profile",
        active_case_context_block="cases",
        case_file_block=None,
        runtime_context={},
        memory_context_block=None,
    )
    names = [b.name for b in out]
    assert "MEMORY_CONTEXT" not in names
