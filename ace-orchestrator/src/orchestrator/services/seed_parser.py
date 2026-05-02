"""Parse a `<user>-user-context-seed.md` file into typed records.

Format:
- Each fenced ```yaml block (under any `## ...` heading) is one row.
- The YAML body is self-describing:
    - `identifier` + `display_name` → user row.
    - `context_type` + `key` + `content` → user_context row.
    - `case_id` → user_case_registry row.
- Context and case rows belong to the most recent `# Contexts for user: <id>`
  or `# Case registry for user: <id>` H1 marker (or the user heading immediately
  preceding them).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import yaml

_FENCE_RE = re.compile(r"^```yaml\s*\n(.*?)^```\s*$", re.MULTILINE | re.DOTALL)
_H1_USER_RE = re.compile(r"^#\s+(?:Contexts|Case registry)\s+for user:\s*([\w.\-]+)", re.MULTILINE)


@dataclass
class UserRow:
    identifier: str
    body: dict[str, Any]


@dataclass
class ContextRow:
    user_identifier: str
    context_type: str
    key: str
    content: dict[str, Any]
    priority: int = 50
    active: bool = True


@dataclass
class CaseRow:
    user_identifier: str
    case_id: str
    body: dict[str, Any]


@dataclass
class ParsedSeed:
    users: list[UserRow] = field(default_factory=list)
    contexts: list[ContextRow] = field(default_factory=list)
    cases: list[CaseRow] = field(default_factory=list)


def parse_seed(markdown: str) -> ParsedSeed:
    seed = ParsedSeed()

    h1_markers: list[tuple[int, str]] = [
        (m.start(), m.group(1)) for m in _H1_USER_RE.finditer(markdown)
    ]

    last_user_seen: str | None = None

    for fence in _FENCE_RE.finditer(markdown):
        body = yaml.safe_load(fence.group(1))
        if not isinstance(body, dict):
            raise ValueError(
                f"Expected a YAML mapping in fence at offset {fence.start()}, "
                f"got {type(body).__name__}"
            )

        active_user = _user_for_offset(fence.start(), h1_markers, fallback=last_user_seen)

        if "identifier" in body and "display_name" in body:
            user = UserRow(identifier=body["identifier"], body=body)
            seed.users.append(user)
            last_user_seen = user.identifier
            continue

        if "context_type" in body and "key" in body:
            if active_user is None:
                raise ValueError(
                    f"Context fence at offset {fence.start()} has no active user; "
                    "expected a `# Contexts for user: <id>` heading first"
                )
            content = body.get("content")
            if not isinstance(content, dict):
                raise ValueError(
                    f"Context {body['key']!r} is missing or has non-mapping `content`"
                )
            seed.contexts.append(
                ContextRow(
                    user_identifier=active_user,
                    context_type=body["context_type"],
                    key=body["key"],
                    content=content,
                    priority=int(body.get("priority", 50)),
                    active=bool(body.get("active", True)),
                )
            )
            continue

        if "case_id" in body:
            if active_user is None:
                raise ValueError(
                    f"Case fence at offset {fence.start()} has no active user; "
                    "expected a `# Case registry for user: <id>` heading first"
                )
            seed.cases.append(
                CaseRow(
                    user_identifier=active_user,
                    case_id=str(body["case_id"]),
                    body=body,
                )
            )
            continue

        raise ValueError(
            f"YAML fence at offset {fence.start()} did not match user / context / case shape"
        )

    return seed


def _user_for_offset(
    offset: int, markers: list[tuple[int, str]], *, fallback: str | None
) -> str | None:
    """Find the most recent H1 marker preceding `offset`."""
    current = fallback
    for marker_offset, identifier in markers:
        if marker_offset < offset:
            current = identifier
        else:
            break
    return current
