"""Translate AssembledBlock list into Anthropic system content with cache_control.

Pure transform. No DB access, no Claude calls.

Cache rules:
- ``mode="drafting"`` → 1h TTL on every cacheable block.
- ``mode="default"`` → 5m TTL on every cacheable block.
- Override allowed per-block via AssembledBlock.cache_ttl when the assembler set
  it explicitly (e.g., a CASE_FILE that the operator wants pinned for 1h regardless).
- ``RUNTIME_CONTEXT`` is never cached.

Anthropic semantics: a ``cache_control`` marker on a block caches the prefix
ending at and including that block. To get a fresh write per group, mark the
LAST block of each group. We mark the last cacheable block in the assembled list
(and only that one) to maximize prefix cache reuse — every block before it is
implicitly cached too.
"""
from __future__ import annotations

from typing import Literal

from orchestrator.services.prompt_assembler import AssembledBlock

Mode = Literal["default", "drafting"]


def _ttl_for_mode(mode: Mode, override: str | None) -> str:
    if override:
        return override
    return "1h" if mode == "drafting" else "5m"


def to_anthropic_system(
    blocks: list[AssembledBlock], *, mode: Mode = "default"
) -> list[dict]:
    """Return the ``system`` parameter as Anthropic's API expects it.

    Each entry is ``{"type": "text", "text": "...", "cache_control": {...}?}``.
    """
    system: list[dict] = []
    last_cacheable_idx = -1
    for i, b in enumerate(blocks):
        if b.cacheable:
            last_cacheable_idx = i

    for i, b in enumerate(blocks):
        entry: dict = {"type": "text", "text": b.text}
        if b.cacheable and i == last_cacheable_idx:
            ttl = _ttl_for_mode(mode, b.cache_ttl)
            entry["cache_control"] = {"type": "ephemeral", "ttl": ttl}
        system.append(entry)
    return system


def cache_breakpoint_summary(blocks: list[AssembledBlock]) -> dict:
    """Telemetry helper: summarize what got cached vs. not."""
    return {
        "total_blocks": len(blocks),
        "cacheable_blocks": sum(1 for b in blocks if b.cacheable),
        "block_names_cached": [b.name for b in blocks if b.cacheable],
        "block_names_uncached": [b.name for b in blocks if not b.cacheable],
    }
