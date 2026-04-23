#!/usr/bin/env python3
"""Minimal migration runner for Sprint A.

Applies a migration SQL file in a single transaction and records it in
ops.migrations_applied with the forward and rollback SQL bodies populated
from disk (per SPRINT_A_WORK_ORDER_v2.2.md §3.2 runner pseudo-code).

Usage:
    python3 apply_migration.py \\
        --forward migrations/026_ops_approval_method.sql \\
        --rollback migrations/026_ops_approval_method_down.sql \\
        --approved-by sprint_a_bootstrap \\
        --analysis "Sprint A preflight substrate: approval_method column."

For Sprint A Tasks 1/2 (migrations 027/028), --approved-by is the
approved_by string from the matching ops.pending_approvals row after the
T1/T2 stop-gate is satisfied via rule 9 conversational approval.

Scope: intentionally minimal. A full migration framework is out of Sprint A
scope. This script handles forward application + bookkeeping only; rollback
is invoked separately with --rollback-of to update the existing row.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import asyncpg


VALID_APPROVAL_METHODS = ("conversation", "dashboard", "api", "bootstrap")


async def verify_approval(conn: asyncpg.Connection, approval_id: int, expected_migration_name: str) -> dict:
    """Verify a pending_approvals row is in an acceptable state to execute against.

    Per SPRINT_A_WORK_ORDER_v2.2.md §10 rule 9 polling semantics, the runner
    reads fresh state from the table and does not trust cached approval state
    from conversation memory or caller arguments.
    """
    row = await conn.fetchrow(
        """
        SELECT id, decision_type, approved, approved_by, approval_method,
               proposed_action, rejection_note
          FROM ops.pending_approvals
         WHERE id = $1
        """,
        approval_id,
    )
    if row is None:
        raise RuntimeError(f"approval id {approval_id} not found")
    if row["approved"] is not True:
        raise RuntimeError(
            f"approval id {approval_id} not approved "
            f"(approved={row['approved']!r}, rejection_note={row['rejection_note']!r})"
        )
    if row["approval_method"] not in VALID_APPROVAL_METHODS:
        raise RuntimeError(
            f"approval id {approval_id} has approval_method="
            f"{row['approval_method']!r}, not in {VALID_APPROVAL_METHODS}"
        )
    raw_action = row["proposed_action"]
    if isinstance(raw_action, str):
        raw_action = json.loads(raw_action) if raw_action else {}
    proposed_migration = (raw_action or {}).get("migration")
    if proposed_migration != expected_migration_name:
        raise RuntimeError(
            f"approval id {approval_id} proposed_action.migration="
            f"{proposed_migration!r}, expected {expected_migration_name!r}"
        )
    return dict(row)


async def apply_forward(
    dsn: str,
    forward_path: Path,
    rollback_path: Path,
    approved_by: str,
    analysis: str,
    approval_id: int | None = None,
) -> None:
    forward_sql = forward_path.read_text()
    rollback_sql = rollback_path.read_text()
    migration_name = forward_path.stem

    conn = await asyncpg.connect(dsn)
    try:
        executing_role = await conn.fetchval("SELECT current_user")
        if approval_id is not None:
            approval = await verify_approval(conn, approval_id, migration_name)
            print(
                f"approval id {approval_id} verified: approved=true, "
                f"approval_method={approval['approval_method']!r}, "
                f"decision_type={approval['decision_type']!r}, "
                f"proposed_action.migration={migration_name!r}"
            )

        # Augment analysis with identity-execution facts the caller cannot know in advance.
        identity_note = (
            f"applied_as_role={executing_role!r}; "
            f"approved_by={approved_by!r}; "
            f"approval_id={approval_id}"
        )
        full_analysis = f"{analysis}\n\n[runner-appended] {identity_note}"

        t0 = time.monotonic()
        async with conn.transaction():
            await conn.execute(forward_sql)
            duration_ms = int((time.monotonic() - t0) * 1000)
            await conn.execute(
                """
                INSERT INTO ops.migrations_applied
                    (migration_name, sql_forward, sql_rollback, applied_by,
                     status, duration_ms, analysis)
                VALUES ($1, $2, $3, $4, 'applied', $5, $6)
                """,
                migration_name, forward_sql, rollback_sql,
                approved_by, duration_ms, full_analysis,
            )
        print(f"Applied {migration_name} in {duration_ms}ms by {approved_by} (executing role: {executing_role})")
    finally:
        await conn.close()


async def apply_rollback(dsn: str, migration_name: str, rolled_back_by: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            "SELECT sql_rollback, status FROM ops.migrations_applied WHERE migration_name=$1",
            migration_name,
        )
        if row is None:
            sys.exit(f"error: {migration_name} not in ops.migrations_applied")
        if row["status"] != "applied":
            sys.exit(f"error: {migration_name} status is {row['status']!r}, cannot roll back")
        async with conn.transaction():
            await conn.execute(row["sql_rollback"])
            await conn.execute(
                """
                UPDATE ops.migrations_applied
                   SET status='rolled_back',
                       rolled_back_at=now(),
                       rolled_back_by=$2
                 WHERE migration_name=$1
                """,
                migration_name, rolled_back_by,
            )
        print(f"Rolled back {migration_name} by {rolled_back_by}")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    fwd = sub.add_parser("forward", help="apply a forward migration")
    fwd.add_argument("--forward", required=True, type=Path)
    fwd.add_argument("--rollback", required=True, type=Path)
    fwd.add_argument("--approved-by", required=True, help="who authorized (e.g. 'will', 'sprint_a_bootstrap')")
    fwd.add_argument("--analysis", required=True)
    fwd.add_argument("--dsn", default=None,
                     help="override connection string (default: DATABASE_URL env). Use for superuser DSN when "
                          "target tables are owned by a role other than the default DATABASE_URL user.")
    fwd.add_argument("--approval-id", type=int, default=None,
                     help="ops.pending_approvals row id to verify before applying. When set, runner halts if "
                          "approved!=true, approval_method is invalid, or proposed_action.migration mismatches.")

    back = sub.add_parser("rollback", help="roll back a previously applied migration")
    back.add_argument("--name", required=True, help="migration_name (filename stem)")
    back.add_argument("--rolled-back-by", required=True)
    back.add_argument("--dsn", default=None, help="override connection string (default: DATABASE_URL env)")

    args = parser.parse_args()
    dsn = args.dsn or os.environ["DATABASE_URL"]

    if args.cmd == "forward":
        asyncio.run(apply_forward(dsn, args.forward, args.rollback, args.approved_by, args.analysis, args.approval_id))
    elif args.cmd == "rollback":
        asyncio.run(apply_rollback(dsn, args.name, args.rolled_back_by))


if __name__ == "__main__":
    main()
