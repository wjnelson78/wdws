#!/usr/bin/env python3
"""Shadow-schema verification for migration 029 (backfill staging table).

Per SPRINT_A_WORK_ORDER_v2.2.md §3.4 shadow pattern.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import date
from pathlib import Path

import asyncpg


SHADOW_REWRITES = [
    ("core.documents_backfill_staging", "core_shadow.documents_backfill_staging"),
    ("core.documents", "core_shadow.documents"),
    ("idx_bfs_document", "idx_bfs_document_shadow"),
    ("idx_bfs_batch", "idx_bfs_batch_shadow"),
    ("idx_bfs_pending", "idx_bfs_pending_shadow"),
    ("idx_bfs_domain_status", "idx_bfs_domain_status_shadow"),
    ("idx_bfs_created", "idx_bfs_created_shadow"),
    ("backfill_staging_status_check", "backfill_staging_status_check_shadow"),
    ("backfill_staging_domain_check", "backfill_staging_domain_check_shadow"),
    ("backfill_staging_confidence_check", "backfill_staging_confidence_check_shadow"),
]


def rewrite_for_shadow(sql: str) -> str:
    out = sql
    for old, new in SHADOW_REWRITES:
        out = out.replace(old, new)
    out = out.replace("BEGIN;", "").replace("COMMIT;", "")
    return out


async def run() -> list[str]:
    dsn = os.environ.get("DATABASE_URL_SUPERUSER") or os.environ["DATABASE_URL"]
    forward_sql = (Path(__file__).parent / "029_documents_backfill_staging.sql").read_text()
    shadow_sql = rewrite_for_shadow(forward_sql)
    outcomes: list[str] = []

    conn = await asyncpg.connect(dsn)
    try:
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute("CREATE SCHEMA core_shadow")
            outcomes.append("created shadow schema")

            # Need core.documents clone for FK target
            await conn.execute(
                "CREATE TABLE core_shadow.documents (LIKE core.documents INCLUDING ALL)"
            )
            outcomes.append("cloned core.documents into shadow (for FK target)")

            await conn.execute(shadow_sql)
            outcomes.append("applied shadow-rewritten migration 029")

            # Structure checks
            cols = await conn.fetch("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema='core_shadow' AND table_name='documents_backfill_staging'
                ORDER BY ordinal_position
            """)
            expected_cols = {
                'id', 'document_id', 'batch_id', 'domain',
                'proposed_privilege', 'proposed_confidentiality',
                'proposed_phi_status', 'proposed_phi_categories', 'proposed_minor_patient',
                'confidence', 'rationale', 'classifier_version', 'classifier_model',
                'status', 'promoted_at', 'promoted_by', 'superseded_at',
                'error_detail', 'created_at',
            }
            actual_cols = {c['column_name'] for c in cols}
            missing = expected_cols - actual_cols
            extra = actual_cols - expected_cols
            if missing or extra:
                outcomes.append(f"FAIL: column set mismatch — missing={missing}, extra={extra}")
            else:
                outcomes.append(f"all {len(expected_cols)} columns present")

            # Seed a document to enable FK insertion
            doc_id = uuid.uuid4()
            await conn.execute("""
                INSERT INTO core_shadow.documents
                    (id, domain, document_type, legal_hold, privilege)
                VALUES ($1, 'legal', 'email', false, NULL)
            """, doc_id)

            # Valid insert
            batch_id = uuid.uuid4()
            staging_id = await conn.fetchval("""
                INSERT INTO core_shadow.documents_backfill_staging
                    (document_id, batch_id, domain, proposed_privilege,
                     proposed_confidentiality, confidence, rationale,
                     classifier_version, classifier_model)
                VALUES ($1, $2, 'legal', 'attorney_client', 'confidential',
                        0.92, 'Document contains header assertion of attorney-client privilege.',
                        'agent_athena_v1', 'claude-sonnet-4-6')
                RETURNING id
            """, doc_id, batch_id)
            outcomes.append(f"insert ok (staging id={str(staging_id)[:8]}...)")

            # Default status
            row = await conn.fetchrow(
                "SELECT status, promoted_at, created_at, proposed_phi_categories "
                "FROM core_shadow.documents_backfill_staging WHERE id=$1",
                staging_id,
            )
            if row['status'] != 'pending':
                outcomes.append(f"FAIL: default status = {row['status']!r}, expected 'pending'")
            elif row['promoted_at'] is not None:
                outcomes.append(f"FAIL: promoted_at default not NULL: {row['promoted_at']}")
            elif row['proposed_phi_categories'] != []:
                outcomes.append(f"FAIL: phi_categories default = {row['proposed_phi_categories']!r}, expected []")
            else:
                outcomes.append("defaults ok: status='pending', promoted_at=NULL, phi_categories=[]")

            # CHECK constraints
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents_backfill_staging
                            (document_id, batch_id, domain, confidence, rationale,
                             classifier_version, classifier_model, status)
                        VALUES ($1, $2, 'legal', 0.5, 'x', 'v1', 'x', 'not_a_status')
                    """, doc_id, batch_id)
                outcomes.append("FAIL: invalid status accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("status CHECK rejects invalid value (ok)")

            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents_backfill_staging
                            (document_id, batch_id, domain, confidence, rationale,
                             classifier_version, classifier_model)
                        VALUES ($1, $2, 'unknown', 0.5, 'x', 'v1', 'x')
                    """, doc_id, batch_id)
                outcomes.append("FAIL: invalid domain accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("domain CHECK rejects non-legal/medical (ok)")

            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents_backfill_staging
                            (document_id, batch_id, domain, confidence, rationale,
                             classifier_version, classifier_model)
                        VALUES ($1, $2, 'legal', 1.5, 'x', 'v1', 'x')
                    """, doc_id, batch_id)
                outcomes.append("FAIL: confidence > 1.0 accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("confidence CHECK rejects > 1.0 (ok)")

            # FK enforcement
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents_backfill_staging
                            (document_id, batch_id, domain, confidence, rationale,
                             classifier_version, classifier_model)
                        VALUES ($1, $2, 'legal', 0.5, 'x', 'v1', 'x')
                    """, uuid.uuid4(), batch_id)  # random doc_id not in core_shadow.documents
                outcomes.append("FAIL: orphan document_id accepted")
            except asyncpg.ForeignKeyViolationError:
                outcomes.append("document_id FK rejects orphan (ok)")

            # Indexes exist
            idx_count = await conn.fetchval("""
                SELECT COUNT(*) FROM pg_indexes
                WHERE schemaname='core_shadow' AND tablename='documents_backfill_staging'
            """)
            # Expect 6 indexes: PK + 5 explicit
            if idx_count >= 6:
                outcomes.append(f"indexes present: {idx_count}")
            else:
                outcomes.append(f"FAIL: expected ≥6 indexes, found {idx_count}")

        finally:
            await tx.rollback()
            outcomes.append("rolled back transaction; shadow schema gone")
    finally:
        await conn.close()
    return outcomes


def main() -> None:
    try:
        outcomes = asyncio.run(run())
    except Exception as e:
        print(f"VERIFICATION FAILED: {e!r}", file=sys.stderr)
        sys.exit(1)

    print("Shadow-schema verification for migration 029:")
    for o in outcomes:
        prefix = "FAIL" if o.startswith("FAIL") else "ok  "
        print(f"  [{prefix}] {o}")

    if any(o.startswith("FAIL") for o in outcomes):
        sys.exit(1)


if __name__ == "__main__":
    main()
