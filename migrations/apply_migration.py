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
import os
import sys
import time
from pathlib import Path

import asyncpg


async def apply_forward(
    dsn: str,
    forward_path: Path,
    rollback_path: Path,
    approved_by: str,
    analysis: str,
) -> None:
    forward_sql = forward_path.read_text()
    rollback_sql = rollback_path.read_text()
    migration_name = forward_path.stem

    conn = await asyncpg.connect(dsn)
    try:
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
                approved_by, duration_ms, analysis,
            )
        print(f"Applied {migration_name} in {duration_ms}ms by {approved_by}")
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
    fwd.add_argument("--approved-by", required=True)
    fwd.add_argument("--analysis", required=True)

    back = sub.add_parser("rollback", help="roll back a previously applied migration")
    back.add_argument("--name", required=True, help="migration_name (filename stem)")
    back.add_argument("--rolled-back-by", required=True)

    args = parser.parse_args()
    dsn = os.environ["DATABASE_URL"]

    if args.cmd == "forward":
        asyncio.run(apply_forward(dsn, args.forward, args.rollback, args.approved_by, args.analysis))
    elif args.cmd == "rollback":
        asyncio.run(apply_rollback(dsn, args.name, args.rolled_back_by))


if __name__ == "__main__":
    main()
