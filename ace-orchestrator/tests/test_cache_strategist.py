from orchestrator.services.cache_strategist import (
    cache_breakpoint_summary,
    to_anthropic_system,
)
from orchestrator.services.prompt_assembler import AssembledBlock


def _blocks() -> list[AssembledBlock]:
    """Mirror what PromptAssembler produces: empty cache_ttl so mode picks TTL."""
    return [
        AssembledBlock("SYSTEM_CORE", "core", cacheable=True, cache_ttl=""),
        AssembledBlock("ROLE_OVERLAY_PRINCIPAL", "role", cacheable=True, cache_ttl=""),
        AssembledBlock("DOMAIN_LEGAL", "legal", cacheable=True, cache_ttl=""),
        AssembledBlock("DOMAIN_COMMS", "comms", cacheable=True, cache_ttl=""),
        AssembledBlock("USER_PROFILE", "profile", cacheable=True, cache_ttl=""),
        AssembledBlock(
            "ACTIVE_CASE_CONTEXT", "matters", cacheable=True, cache_ttl=""
        ),
        AssembledBlock("RUNTIME_CONTEXT", "today", cacheable=False, cache_ttl=""),
    ]


def test_per_block_override_wins_over_mode():
    blocks = [
        AssembledBlock("SYSTEM_CORE", "core", cacheable=True, cache_ttl=""),
        AssembledBlock("CASE_FILE", "case", cacheable=True, cache_ttl="1h"),  # pinned
    ]
    out = to_anthropic_system(blocks, mode="default")
    assert out[1]["cache_control"]["ttl"] == "1h"  # override beats default mode (5m)


def test_marks_only_last_cacheable_block_with_cache_control_default_mode():
    out = to_anthropic_system(_blocks(), mode="default")
    assert len(out) == 7
    cached_indices = [i for i, b in enumerate(out) if "cache_control" in b]
    assert cached_indices == [5]
    assert out[5]["cache_control"] == {"type": "ephemeral", "ttl": "5m"}


def test_drafting_mode_uses_1h_ttl():
    out = to_anthropic_system(_blocks(), mode="drafting")
    cached_indices = [i for i, b in enumerate(out) if "cache_control" in b]
    assert cached_indices == [5]
    assert out[5]["cache_control"]["ttl"] == "1h"


def test_runtime_context_block_never_marked():
    out = to_anthropic_system(_blocks(), mode="drafting")
    assert "cache_control" not in out[6]
    assert out[6]["text"] == "today"


def test_summary_counts_blocks():
    s = cache_breakpoint_summary(_blocks())
    assert s["total_blocks"] == 7
    assert s["cacheable_blocks"] == 6
    assert s["block_names_uncached"] == ["RUNTIME_CONTEXT"]


def test_no_cacheable_blocks_yields_no_cache_markers():
    blocks = [
        AssembledBlock("RUNTIME_CONTEXT", "today", cacheable=False, cache_ttl=""),
    ]
    out = to_anthropic_system(blocks, mode="default")
    assert "cache_control" not in out[0]
