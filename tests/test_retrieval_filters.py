#!/usr/bin/env python3
"""tests/test_retrieval_filters.py — Sprint A Task 3 filter tests.

Per SPRINT_A_WORK_ORDER_v2.2.md §5.8. 21 tests against a 30-document
synthetic fixture seeded into a disposable test_safety.* schema.

Standalone async runner (no pytest dependency). Exits 0 on all pass, 1 on
any failure. Each test prints an outcome line; the final summary counts
pass/fail.

Fixture structure:
  test_safety.documents      (LIKE core.documents INCLUDING ALL)
  test_safety.patients       (LIKE medical.patients INCLUDING ALL)
  test_safety.authorizations (LIKE medical.authorizations INCLUDING ALL)
  test_safety.access_log     (LIKE ops.access_log INCLUDING ALL)
  test_safety.disclosures_log (LIKE medical.disclosures_log INCLUDING ALL)

The 30 synthetic documents span every relevant classification:
  legal: 'none' x2, 'attorney_client' x1, 'work_product_fact' x1,
         'work_product_opinion' x1, 'joint_defense' x1,
         'common_interest' x1, NULL x2, 'unknown' x1         (10 docs)
  medical: safe-harbor x2, expert-deid x1, not_phi x1,
           limited_data_set x1, phi (plain) x2,
           phi+psychotherapy_notes x1, phi+sud_42_cfr_part_2 x1,
           phi+mental_health x1, phi+minor_patient x1,
           NULL x2                                           (13 docs)
  web:      1, general: 1, coding: 1 (out-of-scope domains)   (3 docs)
  minor+psych combo: 1, minor+safe_harbor: 1                  (2 docs)
  minor+phi_authorization + psych_auth:                       (1 doc)
  ---
  Total:                                                      30 docs

Tests 13-21 are "integration sketches" that exercise the filter through
the patterns each surface will use, not the actual surface files (which
are not modified in this mid-Task-3 checkpoint).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from core_safety import (  # noqa: E402
    build_document_safety_filter,
    build_morning_brief_safety_filter,
    fetch_safe_document,
    AccessLogger,
    PrivilegeDeniedException,
    PHIAccessDeniedException,
)

DSN = (os.environ.get("DATABASE_URL_SUPERUSER")
       or "postgresql:///wdws?host=/var/run/postgresql")


# ============================================================
# Fixture
# ============================================================

@dataclass
class Fixture:
    patient_id: int
    minor_patient_id: int
    psych_auth: uuid.UUID
    part2_auth: uuid.UUID
    full_auth: uuid.UUID
    revoked_psych_auth: uuid.UUID
    docs: dict[str, uuid.UUID] = field(default_factory=dict)
    total: int = 0


async def setup_fixture(conn: asyncpg.Connection) -> Fixture:
    await conn.execute("DROP SCHEMA IF EXISTS test_safety CASCADE")
    await conn.execute("CREATE SCHEMA test_safety")
    await conn.execute("CREATE TABLE test_safety.documents (LIKE core.documents INCLUDING ALL)")
    await conn.execute("CREATE TABLE test_safety.patients (LIKE medical.patients INCLUDING ALL)")
    await conn.execute("CREATE TABLE test_safety.authorizations (LIKE medical.authorizations INCLUDING ALL)")
    await conn.execute("CREATE TABLE test_safety.access_log (LIKE ops.access_log INCLUDING ALL)")
    await conn.execute("CREATE TABLE test_safety.disclosures_log (LIKE medical.disclosures_log INCLUDING ALL)")

    patient_id = await conn.fetchval(
        "INSERT INTO test_safety.patients (name, date_of_birth) VALUES ($1, $2) RETURNING id",
        "TEST PATIENT A", date(1980, 1, 1),
    )
    minor_id = await conn.fetchval(
        "INSERT INTO test_safety.patients (name, date_of_birth) VALUES ($1, $2) RETURNING id",
        "TEST MINOR B", date(2015, 1, 1),
    )

    psych_auth = await conn.fetchval(
        """INSERT INTO test_safety.authorizations
               (patient_id, purpose_of_use, scope, effective_date)
           VALUES ($1, 'clinical_care', 'psychotherapy_notes', CURRENT_DATE) RETURNING id""",
        patient_id,
    )
    part2_auth = await conn.fetchval(
        """INSERT INTO test_safety.authorizations
               (patient_id, purpose_of_use, scope, effective_date)
           VALUES ($1, 'clinical_care', 'sud_42_cfr_part_2', CURRENT_DATE) RETURNING id""",
        patient_id,
    )
    full_auth = await conn.fetchval(
        """INSERT INTO test_safety.authorizations
               (patient_id, purpose_of_use, scope, effective_date)
           VALUES ($1, 'litigation_support', 'full_record', CURRENT_DATE) RETURNING id""",
        patient_id,
    )
    revoked_psych = await conn.fetchval(
        """INSERT INTO test_safety.authorizations
               (patient_id, purpose_of_use, scope, effective_date, revoked_at)
           VALUES ($1, 'clinical_care', 'psychotherapy_notes',
                   CURRENT_DATE - interval '30 days', now()) RETURNING id""",
        patient_id,
    )

    fx = Fixture(
        patient_id=patient_id, minor_patient_id=minor_id,
        psych_auth=psych_auth, part2_auth=part2_auth,
        full_auth=full_auth, revoked_psych_auth=revoked_psych,
    )

    # (name, domain, dtype, privilege, phi_status, phi_categories, minor_patient)
    specs: list[tuple] = [
        # Legal (10)
        ('legal_none_1', 'legal', 'email', 'none', None, [], False),
        ('legal_none_2', 'legal', 'email', 'none', None, [], False),
        ('legal_ac_1', 'legal', 'motion', 'attorney_client', None, [], False),
        ('legal_wpf_1', 'legal', 'memo', 'work_product_fact', None, [], False),
        ('legal_wpo_1', 'legal', 'memo', 'work_product_opinion', None, [], False),
        ('legal_jd_1', 'legal', 'email', 'joint_defense', None, [], False),
        ('legal_ci_1', 'legal', 'email', 'common_interest', None, [], False),
        ('legal_null_1', 'legal', 'email', None, None, [], False),
        ('legal_null_2', 'legal', 'email', None, None, [], False),
        ('legal_unknown_1', 'legal', 'email', 'unknown', None, [], False),
        # Medical (15)
        ('med_shd_1', 'medical', 'record', None, 'safe_harbor_deidentified', [], False),
        ('med_shd_2', 'medical', 'record', None, 'safe_harbor_deidentified', [], False),
        ('med_edd_1', 'medical', 'record', None, 'expert_determination_deidentified', [], False),
        ('med_nphi_1', 'medical', 'record', None, 'not_phi', [], False),
        ('med_lds_1', 'medical', 'record', None, 'limited_data_set', [], False),
        ('med_phi_1', 'medical', 'record', None, 'phi', [], False),
        ('med_phi_2', 'medical', 'record', None, 'phi', [], False),
        ('med_psych_1', 'medical', 'record', None, 'phi', ['psychotherapy_notes'], False),
        ('med_part2_1', 'medical', 'record', None, 'phi', ['sud_42_cfr_part_2'], False),
        ('med_mh_1', 'medical', 'record', None, 'phi', ['mental_health'], False),
        ('med_minor_1', 'medical', 'record', None, 'phi', [], True),
        ('med_null_1', 'medical', 'record', None, None, [], False),
        ('med_null_2', 'medical', 'record', None, None, [], False),
        ('med_minor_shd_1', 'medical', 'record', None, 'safe_harbor_deidentified', [], True),
        ('med_minor_psych_1', 'medical', 'record', None, 'phi', ['psychotherapy_notes'], True),
        # Out-of-scope domains (3)
        ('web_1', 'web', 'article', None, None, [], False),
        ('general_1', 'general', 'note', None, None, [], False),
        ('coding_1', 'coding', 'snippet', None, None, [], False),
        # Additional legal non-privileged for volume
        ('legal_none_3', 'legal', 'email', 'none', None, [], False),
        ('legal_none_4', 'legal', 'email', 'none', None, [], False),
        # Cross-domain safety cases (t22, t23) — NULL-classified personal
        # doc surfaces under cross-domain; mis-domained privileged business
        # doc does NOT.
        ('personal_unclass_1', 'personal', 'note', None, None, [], False),
        ('business_misdomained_ac_1', 'business', 'email', 'attorney_client', None, [], False),
    ]

    for (name, domain, dtype, priv, phi_status, phi_cats, minor) in specs:
        fid = uuid.uuid4()
        await conn.execute(
            """INSERT INTO test_safety.documents
                   (id, domain, document_type, legal_hold, title,
                    privilege, phi_status, phi_categories, minor_patient)
               VALUES ($1, $2, $3, false, $4, $5, $6, $7, $8)""",
            fid, domain, dtype, name, priv, phi_status, phi_cats, minor,
        )
        fx.docs[name] = fid

    fx.total = len(specs)
    return fx


async def teardown(conn: asyncpg.Connection) -> None:
    await conn.execute("DROP SCHEMA IF EXISTS test_safety CASCADE")


# ============================================================
# Helpers
# ============================================================

def make_logger(conn) -> AccessLogger:
    return AccessLogger(
        conn,
        access_log_table='test_safety.access_log',
        disclosures_table='test_safety.disclosures_log',
    )


async def run_filter_and_fetch(
    conn, fx, *, extra_select_cols: str = '', **filter_kwargs,
) -> tuple[list[asyncpg.Record], int, int]:
    """Build filter, run SELECT against test_safety.documents, return rows + log counts."""
    logger = filter_kwargs.pop('logger', None) or make_logger(conn)
    before_access = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    before_disc = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")

    clause, params, log = await build_document_safety_filter(
        conn=conn, logger=logger,
        authorizations_table='test_safety.authorizations',
        **filter_kwargs,
    )

    base_cols = "d.id, d.title, d.domain, d.privilege, d.phi_status, d.phi_categories, d.minor_patient"
    sel = f"{base_cols}{', ' + extra_select_cols if extra_select_cols else ''}"
    sql = f"SELECT {sel} FROM test_safety.documents d WHERE {clause}"
    rows = await conn.fetch(sql, *params)
    await log(rows)

    after_access = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    after_disc = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")
    return rows, after_access - before_access, after_disc - before_disc


def titles(rows: list[asyncpg.Record]) -> set[str]:
    return {r['title'] for r in rows}


def assert_eq(actual, expected, name: str) -> None:
    if actual != expected:
        raise AssertionError(
            f"{name}: expected {expected!r}, got {actual!r} "
            f"(diff: only-in-actual={actual - expected if isinstance(actual, set) else 'N/A'}, "
            f"only-in-expected={expected - actual if isinstance(expected, set) else 'N/A'})"
        )


# ============================================================
# Test cases
# ============================================================

TESTS = []


def test(name: str):
    def deco(fn):
        TESTS.append((name, fn))
        return fn
    return deco


# --- Filter-logic unit tests (1-12) ---

@test("t01: default legal retrieval excludes NULL-privilege documents")
async def t01(conn, fx):
    rows, access, _ = await run_filter_and_fetch(
        conn, fx, domain='legal', caller_context={'tool': 't01'},
    )
    # NULL-privilege and 'unknown' legal docs must be absent
    for n in ('legal_null_1', 'legal_null_2', 'legal_unknown_1'):
        assert n not in titles(rows), f"{n} should be excluded"
    assert access == 1, f"expected 1 access_log row, got {access}"


@test("t02: default legal retrieval excludes attorney_client documents")
async def t02(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal', caller_context={'tool': 't02'},
    )
    for n in ('legal_ac_1', 'legal_wpf_1', 'legal_wpo_1',
              'legal_jd_1', 'legal_ci_1'):
        assert n not in titles(rows), f"{n} should be excluded by default"
    # none-privilege must surface
    assert 'legal_none_1' in titles(rows)


@test("t03: include_privileged=True surfaces attorney_client and work_product_fact, not opinion/jd/ci without allow-list")
async def t03(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal', caller_context={'tool': 't03'},
        include_privileged=True,
    )
    t = titles(rows)
    assert 'legal_ac_1' in t and 'legal_wpf_1' in t
    assert 'legal_wpo_1' not in t and 'legal_jd_1' not in t and 'legal_ci_1' not in t


@test("t03b: include_privileged=True + privileged_categories surfaces extended")
async def t03b(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal', caller_context={'tool': 't03b'},
        include_privileged=True,
        privileged_categories=['work_product_opinion', 'joint_defense'],
    )
    t = titles(rows)
    assert 'legal_wpo_1' in t and 'legal_jd_1' in t
    assert 'legal_ci_1' not in t  # not in allow-list


@test("t03c: privileged_categories without include_privileged raises ValueError")
async def t03c(conn, fx):
    try:
        await build_document_safety_filter(
            conn=conn, domain='legal',
            caller_context={'tool': 't03c'},
            include_privileged=False,
            privileged_categories=['work_product_opinion'],
            logger=make_logger(conn),
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised ValueError")
    except ValueError as e:
        assert 'include_privileged=True' in str(e)


@test("t04: default medical retrieval excludes phi and limited_data_set")
async def t04(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't04'},
    )
    t = titles(rows)
    # PHI and limited_data_set excluded
    for n in ('med_phi_1', 'med_phi_2', 'med_lds_1',
              'med_psych_1', 'med_part2_1', 'med_mh_1',
              'med_minor_1', 'med_null_1', 'med_null_2'):
        assert n not in t, f"{n} should be excluded"
    # Safe + expert-deid + not_phi (non-minor) should surface
    assert 'med_shd_1' in t and 'med_edd_1' in t and 'med_nphi_1' in t


@test("t05: medical + purpose_of_use surfaces PHI and logs disclosure")
async def t05(conn, fx):
    rows, access, disc = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't05', 'user': 'will'},
        purpose_of_use='litigation_support',
    )
    t = titles(rows)
    # med_phi_1/2 now surfaced (heightened cats still excluded without auth;
    # minor also still excluded? No — purpose_of_use releases minor_patient filter.
    # See §5.4 "minor_patient=true requires purpose_of_use even in deidentified
    # contexts" — purpose_of_use is what enables it. So minor_1 surfaces too.)
    assert 'med_phi_1' in t and 'med_phi_2' in t and 'med_lds_1' in t
    # Heightened categories still blocked (no auth)
    assert 'med_psych_1' not in t and 'med_part2_1' not in t
    # Disclosure log records one row per phi/limited_data_set document
    phi_count = sum(1 for r in rows if r['phi_status'] in ('phi', 'limited_data_set'))
    assert disc == phi_count, f"expected {phi_count} disclosure rows, got {disc}"
    assert access == 1


@test("t06: psychotherapy_notes never surfaced without matching non-revoked auth")
async def t06(conn, fx):
    # 6a: purpose_of_use without auth — psych excluded
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't06a'},
        purpose_of_use='litigation_support',
    )
    assert 'med_psych_1' not in titles(rows)

    # 6b: purpose_of_use + psych_auth — psych surfaces
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't06b'},
        purpose_of_use='clinical_care', authorization_id=fx.psych_auth,
    )
    assert 'med_psych_1' in titles(rows)

    # 6c: revoked psych_auth — psych excluded
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't06c'},
        purpose_of_use='clinical_care', authorization_id=fx.revoked_psych_auth,
    )
    assert 'med_psych_1' not in titles(rows)


@test("t07: sud_42_cfr_part_2 never surfaced without matching Part 2 auth")
async def t07(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't07a'},
        purpose_of_use='litigation_support', authorization_id=fx.psych_auth,
    )
    # psych auth doesn't unlock part2
    assert 'med_part2_1' not in titles(rows)

    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't07b'},
        purpose_of_use='litigation_support', authorization_id=fx.part2_auth,
    )
    assert 'med_part2_1' in titles(rows)


@test("t08: minor_patient=true requires explicit purpose_of_use")
async def t08(conn, fx):
    # 8a: default, no purpose_of_use — minor excluded even though safe-harbor
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't08a'},
    )
    assert 'med_minor_shd_1' not in titles(rows), \
        "safe-harbor minor should be excluded without purpose_of_use"

    # 8b: with purpose_of_use — minor surfaces (still bound by other rules)
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='medical', caller_context={'tool': 't08b'},
        purpose_of_use='litigation_support',
    )
    assert 'med_minor_shd_1' in titles(rows)
    # minor+phi surfaces; minor+psych still blocked (no auth)
    assert 'med_minor_1' in titles(rows)
    assert 'med_minor_psych_1' not in titles(rows)


@test("t09: fetch_safe_document raises PrivilegeDeniedException on attorney_client")
async def t09(conn, fx):
    try:
        await fetch_safe_document(
            fx.docs['legal_ac_1'], conn=conn,
            caller_context={'tool': 't09', 'user': 'will'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised")
    except PrivilegeDeniedException as e:
        assert 'attorney_client' in str(e)


@test("t10: fetch_safe_document raises PHIAccessDeniedException on phi doc without purpose_of_use")
async def t10(conn, fx):
    try:
        await fetch_safe_document(
            fx.docs['med_phi_1'], conn=conn,
            caller_context={'tool': 't10', 'user': 'will'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised")
    except PHIAccessDeniedException as e:
        assert 'phi_status' in str(e)


@test("t11: denied fetch still writes to access_log")
async def t11(conn, fx):
    before = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    try:
        await fetch_safe_document(
            fx.docs['legal_ac_1'], conn=conn,
            caller_context={'tool': 't11'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
    except PrivilegeDeniedException:
        pass
    after = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    assert after == before + 1, \
        f"access_log should gain 1 row on denied fetch; got {after - before}"

    # Check the outcome field is recorded as denial
    latest = await conn.fetchrow(
        "SELECT metadata FROM test_safety.access_log ORDER BY id DESC LIMIT 1")
    meta = json.loads(latest['metadata'])
    assert meta['outcome'] == 'denied_privilege', \
        f"expected outcome=denied_privilege, got {meta['outcome']!r}"


@test("t12: count_only mode returns aggregate counts, never document IDs in log")
async def t12(conn, fx):
    logger = make_logger(conn)
    clause, params, log = await build_document_safety_filter(
        conn=conn, logger=logger,
        domain='legal', caller_context={'tool': 't12'},
        mode='count_only',
        authorizations_table='test_safety.authorizations',
    )
    # count_only returns TRUE, so the caller's SELECT sees all rows
    rows = await conn.fetch(
        f"SELECT d.domain, COUNT(*) as count FROM test_safety.documents d "
        f"WHERE d.domain='legal' AND {clause} GROUP BY d.domain",
        *params,
    )
    assert len(rows) == 1 and rows[0]['count'] == 12, \
        f"expected 12 legal docs counted, got {rows[0]['count'] if rows else 'nothing'}"
    await log(rows)

    latest = await conn.fetchrow(
        "SELECT action, metadata FROM test_safety.access_log "
        "WHERE action='document_filter_count_only' ORDER BY id DESC LIMIT 1")
    assert latest is not None, "count_only access_log entry not found"
    meta = json.loads(latest['metadata'])
    assert meta['mode'] == 'count_only'
    assert 'document_ids' not in meta, \
        f"count_only log should not include document_ids; got keys {sorted(meta.keys())}"


# --- Per-surface integration sketches (13-21) ---

@test("t13: MCP semantic_search pattern filters with default params")
async def t13(conn, fx):
    # Simulates the MCP pattern: vector JOIN + filter composition
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal',
        caller_context={'tool': 'mcp.semantic_search', 'agent_id': 'claude-desktop'},
    )
    # No privileged results surfaced
    for n in ('legal_ac_1', 'legal_wpf_1', 'legal_null_1'):
        assert n not in titles(rows)


@test("t14: MCP fetch pattern raises on privileged doc without include_privileged")
async def t14(conn, fx):
    try:
        await fetch_safe_document(
            fx.docs['legal_ac_1'], conn=conn,
            caller_context={'tool': 'mcp.fetch', 'agent_id': 'claude-desktop'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised")
    except PrivilegeDeniedException:
        pass


@test("t15: Dashboard _tool_get_document returns denial on privileged doc (exception surfaces)")
async def t15(conn, fx):
    # Simulates dashboard's _tool_get_document wrapping fetch_safe_document and
    # converting the exception to a 'denied' response shape. Here we verify the
    # exception is raised; the surface layer would catch and format.
    try:
        await fetch_safe_document(
            fx.docs['legal_wpo_1'], conn=conn,
            caller_context={'tool': 'dashboard._tool_get_document', 'user': 'will'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised")
    except PrivilegeDeniedException:
        pass  # dashboard layer will format a denial response


@test("t16: Dashboard api_documents endpoint filter yields no privileged content")
async def t16(conn, fx):
    # Simulates the dashboard HTTP endpoint using the filter.
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal',
        caller_context={'tool': 'dashboard.api_documents', 'user': 'will'},
    )
    privileged = {r for r in rows if r['privilege'] and r['privilege'] != 'none'}
    assert len(privileged) == 0, \
        f"api_documents default should surface zero privileged; got {[r['title'] for r in privileged]}"


@test("t17: morning_briefing.get_case_summary — metadata-only, no privileged content")
async def t17(conn, fx):
    # Simulates brief pulling case-linked docs; helper enforces metadata-only semantics
    clause, params, log = await build_morning_brief_safety_filter(
        conn=conn, logger=make_logger(conn),
        domain='legal',
        caller_context={'tool': 'morning_briefing.get_case_summary', 'user': 'will'},
        authorizations_table='test_safety.authorizations',
    )
    # Brief SELECT is restricted to metadata fields (no content/full_content)
    rows = await conn.fetch(
        f"SELECT d.id, d.title, d.document_type, d.privilege FROM test_safety.documents d "
        f"WHERE d.domain='legal' AND {clause}",
        *params,
    )
    await log(rows)
    # Privileged docs surface in the BRIEF (metadata only) because
    # include_privileged=True is locked in. But opinion work product, joint
    # defense, common interest remain excluded.
    t = titles(rows)
    assert 'legal_ac_1' in t, "attorney_client metadata should surface in brief"
    assert 'legal_wpo_1' not in t, "opinion work product must not surface"
    assert 'legal_jd_1' not in t and 'legal_ci_1' not in t
    # NULL-privilege still excluded
    assert 'legal_null_1' not in t


@test("t18: morning_briefing.get_recent_important_emails — PHI email metadata-only")
async def t18(conn, fx):
    # Simulates brief pulling legal-domain emails; brief-wrapper still applies
    # minor_patient hard exclusion and heightened-protection hard exclusion.
    # This test's relevance to PHI: brief's medical view excludes heightened
    # even with purpose_of_use, per §5.5. Simulated with a medical-domain query.
    clause, params, log = await build_morning_brief_safety_filter(
        conn=conn, logger=make_logger(conn),
        domain='medical',
        caller_context={'tool': 'morning_briefing.get_recent_important_emails', 'user': 'will'},
        authorizations_table='test_safety.authorizations',
    )
    rows = await conn.fetch(
        f"SELECT d.id, d.title, d.phi_status, d.phi_categories, d.minor_patient "
        f"FROM test_safety.documents d WHERE d.domain='medical' AND {clause}",
        *params,
    )
    await log(rows)
    t = titles(rows)
    # Heightened categories never in brief (Part 2, psychotherapy_notes)
    assert 'med_psych_1' not in t and 'med_part2_1' not in t
    # Minor patient never in brief (hard exclusion)
    assert 'med_minor_1' not in t and 'med_minor_shd_1' not in t and 'med_minor_psych_1' not in t
    # Safe-harbor / plain PHI metadata CAN surface in brief (trusted caller restricts fields)
    assert 'med_shd_1' in t and 'med_phi_1' in t


@test("t19: legal_module case-document list surfaces only privilege='none' by default")
async def t19(conn, fx):
    # Simulates legal_module's case-document retrieval using the helper
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal',
        caller_context={'tool': 'legal_module.case_documents_list', 'user': 'will'},
    )
    for r in rows:
        assert r['privilege'] == 'none', \
            f"default legal_module retrieval yielded privilege={r['privilege']!r}"


@test("t20: one access_log row per retrieval call with correct metadata")
async def t20(conn, fx):
    # Fresh call — just check tool, domain, result_count
    logger = make_logger(conn)
    before = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain='legal',
        caller_context={'tool': 't20_tool', 'agent_id': 't20_agent'},
        logger=logger,
    )
    after = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    assert after == before + 1, f"expected exactly 1 new access_log row, got {after - before}"
    latest = await conn.fetchrow(
        "SELECT tool_name, domain, result_count "
        "FROM test_safety.access_log ORDER BY id DESC LIMIT 1")
    assert latest['tool_name'] == 't20_tool'
    assert latest['domain'] == 'legal'
    assert latest['result_count'] == len(rows)


@test("t21: one disclosures_log row per PHI document surfaced")
async def t21(conn, fx):
    before = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")
    rows, _, disc = await run_filter_and_fetch(
        conn, fx, domain='medical',
        caller_context={'tool': 't21_tool', 'user': 'will'},
        purpose_of_use='litigation_support',
    )
    phi_count = sum(1 for r in rows if r['phi_status'] in ('phi', 'limited_data_set'))
    after = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")
    gained = after - before
    assert gained == phi_count, \
        f"disclosures_log should gain {phi_count} rows (one per PHI doc), got {gained}"
    assert disc == gained  # delta returned by run_filter_and_fetch matches


# --- Cross-domain safety + denial-logging correctness (22-24) ---

@test("t22: domain=None surfaces unclassified non-scope-domain doc (personal, NULL/NULL)")
async def t22(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain=None,
        caller_context={'tool': 't22'},
    )
    assert 'personal_unclass_1' in titles(rows), \
        "NULL-classified personal-domain doc should surface under cross-domain default"
    # Sanity: also surfaces web/general/coding NULL-classified docs
    assert 'web_1' in titles(rows)
    assert 'general_1' in titles(rows)
    assert 'coding_1' in titles(rows)


@test("t23: domain=None excludes mis-domained business doc tagged attorney_client")
async def t23(conn, fx):
    rows, _, _ = await run_filter_and_fetch(
        conn, fx, domain=None,
        caller_context={'tool': 't23'},
    )
    assert 'business_misdomained_ac_1' not in titles(rows), \
        "business-domain doc with privilege='attorney_client' must NOT surface under cross-domain default"
    # Confirm the doc actually exists in fixture (defensive)
    exists = await conn.fetchval(
        "SELECT COUNT(*) FROM test_safety.documents WHERE title = 'business_misdomained_ac_1'")
    assert exists == 1, "fixture sanity: business_misdomained_ac_1 missing from fixture"

    # With include_privileged=True, the mis-domained AC doc DOES surface
    # (treated as legal-privilege under the filter, since cross-domain applies
    # the legal filter to non-scope domains now? No — non-scope domains have
    # their own strict rule. Re-check: non-scope surfaces only if
    # privilege IS NULL or 'none'. include_privileged widens the legal branch,
    # not the non-scope branch. So this still stays blocked.)
    rows2, _, _ = await run_filter_and_fetch(
        conn, fx, domain=None,
        caller_context={'tool': 't23b'},
        include_privileged=True,
    )
    assert 'business_misdomained_ac_1' not in titles(rows2), \
        "Even with include_privileged=True, non-scope domains stay strict (surface only NULL/'none')"


@test("t24: denied PHI fetch writes access_log outcome=denied_phi but NOT disclosures_log")
async def t24(conn, fx):
    access_before = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    disc_before = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")

    try:
        await fetch_safe_document(
            fx.docs['med_phi_1'], conn=conn,
            caller_context={'tool': 't24', 'user': 'will'},
            logger=make_logger(conn),
            documents_table='test_safety.documents',
            authorizations_table='test_safety.authorizations',
        )
        raise AssertionError("should have raised PHIAccessDeniedException")
    except PHIAccessDeniedException:
        pass

    access_after = await conn.fetchval("SELECT COUNT(*) FROM test_safety.access_log")
    disc_after = await conn.fetchval("SELECT COUNT(*) FROM test_safety.disclosures_log")

    assert access_after == access_before + 1, \
        f"denied fetch should add exactly 1 access_log row; got {access_after - access_before}"
    assert disc_after == disc_before, \
        f"denied fetch MUST NOT add to disclosures_log; got {disc_after - disc_before} new rows"

    latest = await conn.fetchrow(
        "SELECT metadata FROM test_safety.access_log ORDER BY id DESC LIMIT 1")
    meta = json.loads(latest['metadata'])
    assert meta['outcome'] == 'denied_phi', \
        f"expected outcome=denied_phi, got {meta['outcome']!r}"


# ============================================================
# Runner
# ============================================================

async def main() -> int:
    conn = await asyncpg.connect(DSN)
    try:
        print("setting up fixture...")
        fx = await setup_fixture(conn)
        print(f"  fixture: {fx.total} documents, 2 patients, 4 authorizations\n")

        passed = 0
        failed = 0
        for name, fn in TESTS:
            try:
                await fn(conn, fx)
                print(f"  [ok  ] {name}")
                passed += 1
            except Exception as e:
                print(f"  [FAIL] {name}")
                print(f"         {type(e).__name__}: {e}")
                failed += 1

        print(f"\n{passed}/{passed + failed} tests passed")
        return 0 if failed == 0 else 1
    finally:
        await teardown(conn)
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
