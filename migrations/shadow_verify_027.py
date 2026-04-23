#!/usr/bin/env python3
"""Shadow-schema verification for migration 027.

Per SPRINT_A_WORK_ORDER_v2.2.md §3.4:
- Create legal_shadow and core_shadow schemas.
- Replicate core.documents and legal.cases structure (CREATE TABLE LIKE ... INCLUDING ALL).
- Apply schema-rewritten migration 027 to shadow tables.
- Verify:
    * New columns appear on shadow documents/cases tables.
    * CHECK constraint rejects invalid privilege/confidentiality values.
    * Valid values accepted.
    * legal_shadow.protective_orders accepts inserts with valid case_id.
    * Safe-retrieval view compiles.
- Roll the entire transaction back — no shadow schemas persist beyond the test.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path

import asyncpg


SHADOW_REWRITES = [
    # Order matters: longer/specific patterns first, but none of these overlap so order is moot here.
    ("core.documents", "core_shadow.documents"),
    ("legal.cases", "legal_shadow.cases"),
    ("legal.protective_orders", "legal_shadow.protective_orders"),
    ("core.v_documents_safe_retrieval", "core_shadow.v_documents_safe_retrieval"),
    ("legal.idx_cases_assigned_judge", "legal_shadow.idx_cases_assigned_judge"),
    ("core.idx_documents_privilege", "core_shadow.idx_documents_privilege"),
    ("core.idx_documents_confidentiality", "core_shadow.idx_documents_confidentiality"),
]


def rewrite_for_shadow(sql: str) -> str:
    out = sql
    for old, new in SHADOW_REWRITES:
        out = out.replace(old, new)
    # Strip outer BEGIN/COMMIT — we run inside our own transaction.
    out = out.replace("BEGIN;", "").replace("COMMIT;", "")
    return out


async def run() -> list[str]:
    dsn = os.environ["DATABASE_URL"]
    forward_sql = (Path(__file__).parent / "027_legal_privilege_judge.sql").read_text()
    shadow_sql = rewrite_for_shadow(forward_sql)
    outcomes: list[str] = []

    conn = await asyncpg.connect(dsn)
    try:
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute("CREATE SCHEMA core_shadow")
            await conn.execute("CREATE SCHEMA legal_shadow")
            outcomes.append("created shadow schemas")

            await conn.execute("CREATE TABLE core_shadow.documents (LIKE core.documents INCLUDING ALL)")
            await conn.execute("CREATE TABLE legal_shadow.cases (LIKE legal.cases INCLUDING ALL)")
            outcomes.append("cloned structures into shadow")

            await conn.execute(shadow_sql)
            outcomes.append("applied shadow-rewritten migration 027")

            # 1. New columns on core_shadow.documents
            cols = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='core_shadow' AND table_name='documents'
                  AND column_name IN (
                      'privilege', 'privilege_classified_at', 'privilege_classified_by',
                      'confidentiality', 'protective_order_id'
                  )
                ORDER BY column_name
            """)
            added = [r["column_name"] for r in cols]
            expected = sorted([
                "confidentiality", "privilege", "privilege_classified_at",
                "privilege_classified_by", "protective_order_id",
            ])
            if sorted(added) == expected:
                outcomes.append(f"core_shadow.documents has all 5 new columns")
            else:
                outcomes.append(f"FAIL: expected {expected}, got {sorted(added)}")

            # 2. New columns on legal_shadow.cases
            case_cols = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='legal_shadow' AND table_name='cases'
                  AND column_name IN ('assigned_judge', 'opposing_firm')
                ORDER BY column_name
            """)
            case_added = sorted([r["column_name"] for r in case_cols])
            if case_added == ["assigned_judge", "opposing_firm"]:
                outcomes.append("legal_shadow.cases has assigned_judge + opposing_firm")
            else:
                outcomes.append(f"FAIL: legal.cases additions {case_added}")

            # 3. Protective orders table
            po_exists = await conn.fetchval("""
                SELECT to_regclass('legal_shadow.protective_orders')::text IS NOT NULL
            """)
            outcomes.append(f"legal_shadow.protective_orders exists: {po_exists}")

            # 4. Safe-retrieval view compiles and is queryable
            view_ok = await conn.fetchval("""
                SELECT to_regclass('core_shadow.v_documents_safe_retrieval')::text IS NOT NULL
            """)
            outcomes.append(f"core_shadow.v_documents_safe_retrieval exists: {view_ok}")
            # Query the view (empty result is fine — shadow has no data)
            _ = await conn.fetch("SELECT * FROM core_shadow.v_documents_safe_retrieval LIMIT 1")
            outcomes.append("safe-retrieval view queryable")

            # 5. CHECK constraint rejects invalid privilege
            bad_id = uuid.uuid4()
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold, privilege)
                        VALUES ($1, 'legal', 'email', false, 'not_a_real_privilege')
                    """, bad_id)
                outcomes.append("FAIL: invalid privilege value accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("CHECK rejects invalid privilege value (ok)")

            # 6. CHECK allows each valid privilege
            for priv in ["attorney_client", "work_product_opinion", "work_product_fact",
                         "joint_defense", "common_interest", "none", "unknown"]:
                doc_id = uuid.uuid4()
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold, privilege)
                        VALUES ($1, 'legal', 'email', false, $2)
                    """, doc_id, priv)
            outcomes.append("CHECK accepts all 7 valid privilege values")

            # 7. CHECK constraint rejects invalid confidentiality
            bad2 = uuid.uuid4()
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold, confidentiality)
                        VALUES ($1, 'legal', 'email', false, 'top_secret')
                    """, bad2)
                outcomes.append("FAIL: invalid confidentiality accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("CHECK rejects invalid confidentiality value (ok)")

            # 8. Protective orders insert requires a valid case_id
            #    Seed a case in legal_shadow, then insert a protective_order.
            case_id = await conn.fetchval("""
                INSERT INTO legal_shadow.cases (case_number) VALUES ('SHADOW-TEST-001') RETURNING id
            """)
            po_id = await conn.fetchval("""
                INSERT INTO legal_shadow.protective_orders (case_id, effective_date, tiers)
                VALUES ($1, CURRENT_DATE, ARRAY['confidential'])
                RETURNING id
            """, case_id)
            outcomes.append(f"legal_shadow.protective_orders insert ok (po_id={str(po_id)[:8]}...)")

            # 9. core_shadow.documents.protective_order_id FK works
            linked = uuid.uuid4()
            await conn.execute("""
                INSERT INTO core_shadow.documents
                    (id, domain, document_type, legal_hold, privilege, confidentiality, protective_order_id)
                VALUES ($1, 'legal', 'email', false, 'attorney_client', 'confidential', $2)
            """, linked, po_id)
            outcomes.append("core_shadow.documents.protective_order_id FK works")

            # 10. 'no defaults applied' sanity: count rows WHERE privilege IS NULL after migration
            #     on a fresh table this equals total row count; we only have rows we inserted with explicit values,
            #     so check the count of NULL-privilege rows is 0 on our synthetic data.
            null_count = await conn.fetchval("SELECT COUNT(*) FROM core_shadow.documents WHERE privilege IS NULL")
            outcomes.append(f"synthetic rows with NULL privilege: {null_count} (all inserts used explicit value)")

        finally:
            await tx.rollback()
            outcomes.append("rolled back transaction; shadow schemas gone")
    finally:
        await conn.close()
    return outcomes


def main() -> None:
    try:
        outcomes = asyncio.run(run())
    except Exception as e:
        print(f"VERIFICATION FAILED: {e!r}", file=sys.stderr)
        sys.exit(1)

    print("Shadow-schema verification for migration 027:")
    for o in outcomes:
        prefix = "FAIL" if o.startswith("FAIL") else "ok  "
        print(f"  [{prefix}] {o}")

    if any(o.startswith("FAIL") for o in outcomes):
        sys.exit(1)


if __name__ == "__main__":
    main()
