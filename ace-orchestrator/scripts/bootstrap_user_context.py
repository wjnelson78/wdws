"""Bootstrap users + user_contexts + user_case_registry from a seed markdown file.

Idempotent: re-running upserts each row by its natural key.

Usage:
    .venv/bin/python scripts/bootstrap_user_context.py [path-to-seed.md]
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
from datetime import date
from typing import Any
from uuid import UUID

import datetime
import yaml
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert


def _jsonify(value):
    """Recursively convert YAML-parsed dates/datetimes to ISO strings for JSONB."""
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))

from orchestrator.db.models import (  # noqa: E402
    User,
    UserCaseRegistry,
    UserContext,
)
from orchestrator.db.session import SessionLocal  # noqa: E402
from orchestrator.services.seed_parser import parse_seed  # noqa: E402

DEFAULT_SEED_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "data"
    / "william_nelson_user_context_seed.md"
)

USER_COLUMNS = {
    "identifier",
    "account_type",
    "display_name",
    "preferred_name",
    "primary_email",
    "primary_phone",
    "default_session_role",
    "timezone",
    "authorization_context",
    "signature_block",
    "primary_sending_email",
    "default_cc",
    "signature_image_ace_doc_id",
    "signing_script_ace_doc_id",
    "preferences",
    "privileged_contacts",
    "active",
}

# Patch 02 §A.2: accept the legacy `role` field name; emit a one-line warning.
USER_FIELD_ALIASES = {"role": "default_session_role"}

CASE_COLUMNS = {
    "case_id",
    "caption",
    "court",
    "judge",
    "tribunal",
    "role",
    "status",
    "opposing_counsel",
    "named_defendants_include",
    "subject",
    "active_workstream",
    "next_deadline",
    "notes",
    "priority",
    "context_refs",
}

CASE_ALIASES = {"key_personnel_refs": "context_refs"}


def _split_user_fields(body: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Pull recognized user columns out of YAML body; leftover goes into preferences."""
    cols: dict[str, Any] = {}
    extras: dict[str, Any] = {}
    for k, v in body.items():
        target = USER_FIELD_ALIASES.get(k, k)
        if target != k:
            print(f"  [warn] seed field '{k}' renamed to '{target}' (Patch 02 §A.2)")
        if target in USER_COLUMNS:
            cols[target] = v
        else:
            extras[k] = v

    cols.setdefault("default_cc", [])
    cols.setdefault("privileged_contacts", [])
    prefs = dict(cols.get("preferences") or {})
    if extras:
        prefs.setdefault("_extras_from_seed", {}).update(extras)
    cols["preferences"] = prefs
    cols.setdefault("active", True)
    return cols, extras


def _split_case_fields(body: dict[str, Any]) -> dict[str, Any]:
    """Map YAML body to user_case_registry columns; extras flow into `notes`."""
    cols: dict[str, Any] = {}
    extras: dict[str, Any] = {}
    for k, v in body.items():
        target = CASE_ALIASES.get(k, k)
        if target in CASE_COLUMNS:
            cols[target] = v
        else:
            extras[k] = v

    if extras:
        extras_yaml = yaml.safe_dump(extras, sort_keys=False, allow_unicode=True).strip()
        existing = cols.get("notes")
        appended = (
            f"{existing.rstrip()}\n\n--- extras from seed ---\n{extras_yaml}"
            if existing
            else extras_yaml
        )
        cols["notes"] = appended

    nd = cols.get("next_deadline")
    if isinstance(nd, str):
        cols["next_deadline"] = date.fromisoformat(nd)
    cols.setdefault("priority", 50)
    return cols


async def _upsert_user(session, body: dict[str, Any]) -> UUID:
    cols, _ = _split_user_fields(body)
    if cols.get("preferences") is not None:
        cols["preferences"] = _jsonify(cols["preferences"])
    stmt = pg_insert(User.__table__).values(**cols)
    update_set = {k: stmt.excluded[k] for k in cols if k != "identifier"}
    update_set["updated_at"] = text("now()")
    stmt = stmt.on_conflict_do_update(
        index_elements=["identifier"],
        set_=update_set,
    ).returning(User.id)
    result = await session.execute(stmt)
    return result.scalar_one()


async def _upsert_context(session, user_id: UUID, row) -> None:
    values = {
        "user_id": user_id,
        "context_type": row.context_type,
        "key": row.key,
        "content": _jsonify(row.content),
        "priority": row.priority,
        "active": row.active,
    }
    stmt = pg_insert(UserContext.__table__).values(**values)
    stmt = stmt.on_conflict_do_update(
        constraint="uniq_user_contexts_key",
        set_={
            "content": stmt.excluded.content,
            "priority": stmt.excluded.priority,
            "active": stmt.excluded.active,
            "updated_at": text("now()"),
        },
    )
    await session.execute(stmt)


async def _upsert_case(session, user_id: UUID, body: dict[str, Any]) -> None:
    cols = _split_case_fields(body)
    for jsonb_field in ("opposing_counsel", "named_defendants_include"):
        if cols.get(jsonb_field) is not None:
            cols[jsonb_field] = _jsonify(cols[jsonb_field])
    cols["user_id"] = user_id
    stmt = pg_insert(UserCaseRegistry.__table__).values(**cols)
    update_set = {k: stmt.excluded[k] for k in cols if k not in ("user_id", "case_id")}
    update_set["updated_at"] = text("now()")
    stmt = stmt.on_conflict_do_update(
        constraint="pk_user_case_registry",
        set_=update_set,
    )
    await session.execute(stmt)


async def main(path: pathlib.Path) -> None:
    seed = parse_seed(path.read_text(encoding="utf-8"))
    print(
        f"Parsed seed: {len(seed.users)} users, "
        f"{len(seed.contexts)} contexts, {len(seed.cases)} cases"
    )

    async with SessionLocal() as session:
        identifier_to_id: dict[str, UUID] = {}
        for u in seed.users:
            uid = await _upsert_user(session, u.body)
            identifier_to_id[u.identifier] = uid
            print(f"  user {u.identifier:<16} → {uid}")

        for ctx in seed.contexts:
            uid = identifier_to_id.get(ctx.user_identifier)
            if uid is None:
                result = await session.execute(
                    select(User.id).where(User.identifier == ctx.user_identifier)
                )
                uid = result.scalar_one()
                identifier_to_id[ctx.user_identifier] = uid
            await _upsert_context(session, uid, ctx)

        for case in seed.cases:
            uid = identifier_to_id[case.user_identifier]
            await _upsert_case(session, uid, case.body)

        await session.commit()

    print("Seed applied.")


if __name__ == "__main__":
    seed_path = (
        pathlib.Path(sys.argv[1]).resolve()
        if len(sys.argv) > 1
        else DEFAULT_SEED_PATH
    )
    asyncio.run(main(seed_path))
