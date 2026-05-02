"""Parse the Athena v2.0 system-prompt markdown into named cache blocks.

The v2.0 document uses `## N. Title` for top-level sections and `### N.M Title`
for sub-sections. Block layout matches Patch 01 §7.

Per the spec: do not paraphrase or improve. The parser only slices and concatenates.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# (block_name, list of section numbers to include, cache_ttl)
BLOCK_LAYOUT: list[tuple[str, list[int], str]] = [
    ("SYSTEM_CORE", [1, 4, 5, 6, 7, 8, 18, 19, 20, 21, 22], "1h"),
    ("DOMAIN_LEGAL", [9, 10], "1h"),
    ("DOMAIN_COMMS", [11], "1h"),
    ("DOMAIN_MEDICAL", [12], "1h"),
    ("DOMAIN_OSINT", [13], "1h"),
    ("DOMAIN_DOCHANDLING", [14, 15, 16], "1h"),
]

# (block_name, sub-section identifier (e.g., "3.1"), cache_ttl)
ROLE_OVERLAY_LAYOUT: list[tuple[str, str, str]] = [
    ("ROLE_OVERLAY_PRINCIPAL", "3.1", "1h"),
    ("ROLE_OVERLAY_CAREGIVER", "3.2", "1h"),
    ("ROLE_OVERLAY_COLLABORATOR", "3.3", "1h"),
]

_SECTION_HEADER_RE = re.compile(r"^##\s+(\d+)\.\s+(.+?)\s*$", re.MULTILINE)
_SUBSECTION_HEADER_RE = re.compile(
    r"^###\s+(\d+)\.(\d+)\s+(.+?)\s*$", re.MULTILINE
)


@dataclass(frozen=True)
class ParsedBlock:
    name: str
    content: str
    cache_ttl: str
    sort_order: int


def _split_top_sections(markdown: str) -> dict[int, str]:
    """Return {section_number: full text including its `## N. Title` header}."""
    matches = list(_SECTION_HEADER_RE.finditer(markdown))
    if not matches:
        raise ValueError("No `## N. Title` section headers found in v2.0 markdown")
    sections: dict[int, str] = {}
    for i, m in enumerate(matches):
        num = int(m.group(1))
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(markdown)
        sections[num] = markdown[start:end].rstrip() + "\n"
    return sections


def _split_subsections(section_text: str) -> dict[str, str]:
    """Return {'N.M': full text including its `### N.M Title` header}."""
    matches = list(_SUBSECTION_HEADER_RE.finditer(section_text))
    if not matches:
        return {}
    subs: dict[str, str] = {}
    for i, m in enumerate(matches):
        sub_id = f"{m.group(1)}.{m.group(2)}"
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(section_text)
        subs[sub_id] = section_text[start:end].rstrip() + "\n"
    return subs


def parse_v2_blocks(markdown: str) -> list[ParsedBlock]:
    """Parse v2.0 markdown into the Patch 01 block layout.

    Returns blocks in the order they should be persisted (sort_order matches order).
    Raises ValueError if any expected section is missing — the spec forbids silent
    guessing.
    """
    sections = _split_top_sections(markdown)
    blocks: list[ParsedBlock] = []
    sort = 0

    for name, section_numbers, ttl in BLOCK_LAYOUT:
        missing = [n for n in section_numbers if n not in sections]
        if missing:
            raise ValueError(
                f"Block {name} requires sections {missing} which are missing from v2.0"
            )
        body = "\n".join(sections[n] for n in section_numbers).strip() + "\n"
        blocks.append(ParsedBlock(name=name, content=body, cache_ttl=ttl, sort_order=sort))
        sort += 1

    section_3 = sections.get(3)
    if section_3 is None:
        raise ValueError("Section 3 (role overlays) missing from v2.0")
    sub_3 = _split_subsections(section_3)
    for name, sub_id, ttl in ROLE_OVERLAY_LAYOUT:
        if sub_id not in sub_3:
            raise ValueError(
                f"Role overlay sub-section {sub_id} missing for block {name}"
            )
        blocks.append(
            ParsedBlock(name=name, content=sub_3[sub_id], cache_ttl=ttl, sort_order=sort)
        )
        sort += 1

    return blocks
