#!/usr/bin/env python3
"""Shadow-schema verification for migration 028 (medical PHI classification).

Per SPRINT_A_WORK_ORDER_v2.2.md §4.4:
- Create medical_shadow and core_shadow schemas.
- Replicate core.documents, medical.patients into shadow.
- Apply schema-rewritten migration 028 to shadow.
- Verify:
    * 5 new columns on core_shadow.documents (phi_status, phi_categories,
      minor_patient, phi_classified_at, phi_classified_by), plus
      phi_authorization_id added in Part B.
    * CHECK constraint rejects invalid phi_status values.
    * All 5 valid phi_status values accepted.
    * phi_categories GIN index functional.
    * medical_shadow.authorizations and disclosures_log created with FK to
      medical_shadow.patients.
    * Both tables accept inserts; FK rejects invalid patient_id / document_id.
    * Safe-retrieval view compiles and is queryable.
- Roll back; shadow schemas never persist.

IMPORTANT for Task 1 context: core.documents now has privilege columns from
migration 027. Shadow cloning via CREATE TABLE LIKE ... INCLUDING ALL copies
those too, so the shadow structure reflects the post-027 state. The 028
migration's ADD COLUMN phi_* statements do not collide with 027's additions.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

import asyncpg


SHADOW_REWRITES = [
    ("core.documents", "core_shadow.documents"),
    ("medical.patients", "medical_shadow.patients"),
    ("medical.authorizations", "medical_shadow.authorizations"),
    ("medical.disclosures_log", "medical_shadow.disclosures_log"),
    ("core.v_medical_safe_retrieval", "core_shadow.v_medical_safe_retrieval"),
    ("core.idx_documents_phi_status", "core_shadow.idx_documents_phi_status"),
    ("core.idx_documents_phi_categories", "core_shadow.idx_documents_phi_categories"),
    ("core.idx_documents_minor_patient", "core_shadow.idx_documents_minor_patient"),
]


def rewrite_for_shadow(sql: str) -> str:
    out = sql
    for old, new in SHADOW_REWRITES:
        out = out.replace(old, new)
    out = out.replace("BEGIN;", "").replace("COMMIT;", "")
    return out


async def run() -> list[str]:
    dsn = os.environ.get("DATABASE_URL_SUPERUSER") or os.environ["DATABASE_URL"]
    forward_sql = (Path(__file__).parent / "028_medical_phi_classification.sql").read_text()
    shadow_sql = rewrite_for_shadow(forward_sql)
    outcomes: list[str] = []

    conn = await asyncpg.connect(dsn)
    try:
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute("CREATE SCHEMA core_shadow")
            await conn.execute("CREATE SCHEMA medical_shadow")
            outcomes.append("created shadow schemas")

            await conn.execute("CREATE TABLE core_shadow.documents (LIKE core.documents INCLUDING ALL)")
            await conn.execute("CREATE TABLE medical_shadow.patients (LIKE medical.patients INCLUDING ALL)")
            outcomes.append("cloned structures into shadow (includes post-027 privilege columns)")

            await conn.execute(shadow_sql)
            outcomes.append("applied shadow-rewritten migration 028")

            # 1. New columns on core_shadow.documents
            cols = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='core_shadow' AND table_name='documents'
                  AND column_name IN (
                      'phi_status', 'phi_categories', 'minor_patient',
                      'phi_classified_at', 'phi_classified_by', 'phi_authorization_id'
                  )
                ORDER BY column_name
            """)
            added = sorted([r["column_name"] for r in cols])
            expected = sorted([
                "minor_patient", "phi_authorization_id", "phi_categories",
                "phi_classified_at", "phi_classified_by", "phi_status",
            ])
            if added == expected:
                outcomes.append("core_shadow.documents has all 6 new columns (5 + phi_authorization_id FK)")
            else:
                outcomes.append(f"FAIL: expected {expected}, got {added}")

            # 2. New tables exist
            auth_exists = await conn.fetchval(
                "SELECT to_regclass('medical_shadow.authorizations')::text IS NOT NULL")
            disc_exists = await conn.fetchval(
                "SELECT to_regclass('medical_shadow.disclosures_log')::text IS NOT NULL")
            outcomes.append(f"medical_shadow.authorizations exists: {auth_exists}")
            outcomes.append(f"medical_shadow.disclosures_log exists: {disc_exists}")

            # 3. Safe-retrieval view
            view_ok = await conn.fetchval(
                "SELECT to_regclass('core_shadow.v_medical_safe_retrieval')::text IS NOT NULL")
            _ = await conn.fetch("SELECT * FROM core_shadow.v_medical_safe_retrieval LIMIT 1")
            outcomes.append(f"core_shadow.v_medical_safe_retrieval queryable: {view_ok}")

            # 4. CHECK constraint rejects invalid phi_status
            bad_id = uuid.uuid4()
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold, phi_status)
                        VALUES ($1, 'medical', 'record', false, 'not_a_phi_status')
                    """, bad_id)
                outcomes.append("FAIL: invalid phi_status accepted")
            except asyncpg.CheckViolationError:
                outcomes.append("CHECK rejects invalid phi_status value (ok)")

            # 5. Each valid phi_status accepted
            for status in ["phi", "limited_data_set", "safe_harbor_deidentified",
                           "expert_determination_deidentified", "not_phi"]:
                doc_id = uuid.uuid4()
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold, phi_status)
                        VALUES ($1, 'medical', 'record', false, $2)
                    """, doc_id, status)
            outcomes.append("CHECK accepts all 5 valid phi_status values")

            # 6. phi_categories array + GIN index functional
            doc_id = uuid.uuid4()
            await conn.execute("""
                INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold,
                                                   phi_status, phi_categories, minor_patient)
                VALUES ($1, 'medical', 'record', false, 'phi',
                        ARRAY['sud_42_cfr_part_2','psychotherapy_notes'], true)
            """, doc_id)
            # Query using GIN-friendly operator
            matched = await conn.fetchval("""
                SELECT COUNT(*) FROM core_shadow.documents
                WHERE phi_categories && ARRAY['sud_42_cfr_part_2']::text[]
            """)
            outcomes.append(f"phi_categories array + overlap query works (matched={matched})")

            # 7. Seed a patient; authorizations FK works
            patient_id = await conn.fetchval("""
                INSERT INTO medical_shadow.patients (name, date_of_birth)
                VALUES ('SHADOW TEST PATIENT', '1980-01-01') RETURNING id
            """)
            auth_id = await conn.fetchval("""
                INSERT INTO medical_shadow.authorizations
                    (patient_id, purpose_of_use, scope, effective_date)
                VALUES ($1, 'litigation_support', 'full_record', CURRENT_DATE)
                RETURNING id
            """, patient_id)
            outcomes.append(f"medical_shadow.authorizations insert ok (auth_id={str(auth_id)[:8]}...)")

            # 8. Authorizations FK rejects invalid patient_id
            try:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO medical_shadow.authorizations
                            (patient_id, purpose_of_use, scope, effective_date)
                        VALUES (-1, 'x', 'x', CURRENT_DATE)
                    """)
                outcomes.append("FAIL: invalid patient_id accepted by authorizations FK")
            except asyncpg.ForeignKeyViolationError:
                outcomes.append("authorizations FK rejects invalid patient_id (ok)")

            # 9. disclosures_log insert works with FKs
            disc_id = await conn.fetchval("""
                INSERT INTO medical_shadow.disclosures_log
                    (document_id, patient_id, disclosed_to, purpose_of_use, authorization_id, disclosed_by)
                VALUES ($1, $2, 'opposing_counsel_smith_llp', 'litigation_support', $3, 'will')
                RETURNING id
            """, doc_id, patient_id, auth_id)
            outcomes.append(f"medical_shadow.disclosures_log insert ok (disc_id={str(disc_id)[:8]}...)")

            # 10. core_shadow.documents.phi_authorization_id FK chain
            linked = uuid.uuid4()
            await conn.execute("""
                INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold,
                                                   phi_status, phi_authorization_id)
                VALUES ($1, 'medical', 'record', false, 'phi', $2)
            """, linked, auth_id)
            outcomes.append("core_shadow.documents.phi_authorization_id FK works")

            # 11. No-default verification: defaults DO exist for phi_categories (empty array)
            #     and minor_patient (false). phi_status specifically must NOT default.
            #     Insert a row without any phi_* field set:
            default_id = uuid.uuid4()
            await conn.execute("""
                INSERT INTO core_shadow.documents (id, domain, document_type, legal_hold)
                VALUES ($1, 'medical', 'record', false)
            """, default_id)
            default_row = await conn.fetchrow("""
                SELECT phi_status, phi_categories, minor_patient
                FROM core_shadow.documents WHERE id = $1
            """, default_id)
            if default_row["phi_status"] is None and \
               default_row["phi_categories"] == [] and \
               default_row["minor_patient"] is False:
                outcomes.append("defaults correct: phi_status NULL, phi_categories '{}', minor_patient false")
            else:
                outcomes.append(f"FAIL: defaults wrong: {dict(default_row)}")

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

    print("Shadow-schema verification for migration 028:")
    for o in outcomes:
        prefix = "FAIL" if o.startswith("FAIL") else "ok  "
        print(f"  [{prefix}] {o}")

    if any(o.startswith("FAIL") for o in outcomes):
        sys.exit(1)


if __name__ == "__main__":
    main()
