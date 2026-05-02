import pytest

from orchestrator.services.prompt_parser import (
    BLOCK_LAYOUT,
    ROLE_OVERLAY_LAYOUT,
    parse_v2_blocks,
)


def test_parses_all_expected_blocks(v2_markdown):
    blocks = parse_v2_blocks(v2_markdown)
    names = {b.name for b in blocks}
    expected = {name for name, _, _ in BLOCK_LAYOUT} | {
        name for name, _, _ in ROLE_OVERLAY_LAYOUT
    }
    assert names == expected


def test_sort_order_is_dense_from_zero(v2_markdown):
    blocks = parse_v2_blocks(v2_markdown)
    assert [b.sort_order for b in blocks] == list(range(len(blocks)))


def test_role_overlays_only_contain_their_subsection(v2_markdown):
    blocks = {b.name: b for b in parse_v2_blocks(v2_markdown)}
    assert "### 3.1" in blocks["ROLE_OVERLAY_PRINCIPAL"].content
    assert "### 3.2" not in blocks["ROLE_OVERLAY_PRINCIPAL"].content
    assert "### 3.2" in blocks["ROLE_OVERLAY_CAREGIVER"].content
    assert "### 3.3" in blocks["ROLE_OVERLAY_COLLABORATOR"].content


def test_system_core_includes_tool_reference(v2_markdown):
    """Per user choice: §22 (tool reference) lives inside SYSTEM_CORE."""
    blocks = {b.name: b for b in parse_v2_blocks(v2_markdown)}
    assert "## 22. Complete Tool Reference" in blocks["SYSTEM_CORE"].content
    assert "## 1. Agent Identity" in blocks["SYSTEM_CORE"].content


def test_domain_blocks_contain_correct_sections(v2_markdown):
    blocks = {b.name: b for b in parse_v2_blocks(v2_markdown)}
    assert "## 9. Legal Drafting" in blocks["DOMAIN_LEGAL"].content
    assert "## 10. Legal Research" in blocks["DOMAIN_LEGAL"].content
    assert "## 11. Communication" in blocks["DOMAIN_COMMS"].content
    assert "## 12. Medical" in blocks["DOMAIN_MEDICAL"].content
    assert "## 13. OSINT" in blocks["DOMAIN_OSINT"].content
    assert "## 14. Document Handling" in blocks["DOMAIN_DOCHANDLING"].content
    assert "## 15. Task & Deadline" in blocks["DOMAIN_DOCHANDLING"].content
    assert "## 16. Memory" in blocks["DOMAIN_DOCHANDLING"].content


def test_user_profile_section_is_NOT_in_any_static_block(v2_markdown):
    """§2 and §17 are runtime-injected by user_context_loader."""
    blocks = parse_v2_blocks(v2_markdown)
    for b in blocks:
        assert "## 2. Principal Context" not in b.content, b.name
        assert "## 17. Active Matters" not in b.content, b.name


def test_missing_section_raises(v2_markdown):
    """Drop §10 only — DOMAIN_LEGAL must complain."""
    head, _, rest = v2_markdown.partition("## 10. Legal Research")
    _, _, tail = rest.partition("\n## 11.")
    truncated = head + "## 11." + tail
    with pytest.raises(ValueError, match="DOMAIN_LEGAL"):
        parse_v2_blocks(truncated)
