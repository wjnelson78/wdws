#!/usr/bin/env python3
"""Sprint A Task 4 v2-reclassification of mental_health-flagged medical rows.

Per Will 2026-04-23, Path (a) resolution of the §5.4/§6.3 scope bug that
backfilled 103 heightened-review entries (92 mental_health + 9 hiv_aids +
2 genetic_gina) on top of the worker's original 22 (psych + part2).

Scope: all medical staging rows where v1 flagged 'mental_health'. Re-run
the classifier (now reading the swapped v2 prompt) against the same
documents. For each, compare v2 output to v1 output and reconcile:

  1. Insert NEW staging row with classifier_version='agent_athena_v2'
  2. Mark OLD staging row status='superseded', superseded_at=now()
  3. For each heightened category, apply the per-category action:
       v1 had AND v2 has      → create new review linked to v2 staging_id;
                                 mark v1-linked review 'superseded' (not 'retracted')
       v1 had AND v2 does not → mark v1-linked review 'superseded' with
                                 rejection_note explaining v2 under primary-subject-
                                 test did not reproduce the flag (burden reduction)
       v1 did not AND v2 has  → create new review for v2 staging_id (v2 caught
                                 something v1 missed)
  4. Classification review (medium-confidence 0.7-0.89) handling:
       v1 had review AND v2 is high-conf (≥0.9) → supersede v1 review
       v1 had review AND v2 is also medium       → create v2-linked review;
                                                    supersede v1 review
       v1 did not have AND v2 is medium           → create v2-linked review
     One active classification_review per document at a time.
  5. Write ops.athena_decisions row per reclassified doc with decision_type
     'sprint_a_t4_v2_reclassification'; evidence JSON carries both v1 and v2
     proposals and the category-diff actions taken.

Rate-limited via shared TokenBucket (100/hr rolling). Legal bulk may be
running in parallel — they share the global bucket.

On completion: writes a marker ops.athena_decisions row with decision_type
'sprint_a_t4_v2_reclassification_complete' so the monitor can emit the
event and trigger the downstream mini-validation gate.

Usage:
    sudo -u postgres /opt/wdws/venv/bin/python3 \\
        /opt/wdws/sprint_a_v2_reclassify.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Optional

import anthropic
import asyncpg

sys.path.insert(0, "/opt/wdws")
from core_safety import REVIEW_REQUIRED_CATEGORIES  # noqa: E402
# Reuse the worker's infrastructure — classifier, rate limiter, promotion utilities.
from sprint_a_llm_backfill import (  # noqa: E402
    CLASSIFIER_MODEL, CLASSIFIER_VERSION as V1_VERSION,
    CONFIDENCE_HIGH, CONFIDENCE_MEDIUM,
    DomainClassifier, TokenBucket,
    check_protective_order,
)

V2_VERSION = 'agent_athena_v2'


# ============================================================
# Reclassification
# ============================================================

async def select_candidates(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    """Select all v1 medical staging rows that flagged mental_health."""
    return await conn.fetch("""
        SELECT s.id AS staging_id, s.document_id, s.confidence AS v1_confidence,
               s.proposed_privilege, s.proposed_confidentiality,
               s.proposed_phi_status AS v1_phi_status,
               s.proposed_phi_categories AS v1_phi_categories,
               s.proposed_minor_patient AS v1_minor_patient,
               s.rationale AS v1_rationale,
               d.id AS doc_id, d.title, d.filename, d.document_type,
               d.created_at, d.full_content
          FROM core.documents_backfill_staging s
          JOIN core.documents d ON d.id = s.document_id
         WHERE s.status = 'pending'
           AND s.domain = 'medical'
           AND s.classifier_version = $1
           AND 'mental_health' = ANY(s.proposed_phi_categories)
         ORDER BY s.created_at ASC
    """, V1_VERSION)


async def reconcile_one(
    conn: asyncpg.Connection,
    v1: asyncpg.Record,
    v2_proposal: dict,
    batch_id: uuid.UUID,
) -> dict:
    """Insert v2 staging row, supersede v1, reconcile reviews, record decision.

    Returns a summary dict used both for runtime logging and for the final
    ops.athena_decisions evidence on each reconciled document.
    """
    v1_cats = set(v1['v1_phi_categories'] or [])
    v2_cats = set(v2_proposal.get('phi_categories') or [])
    v1_heightened = v1_cats & REVIEW_REQUIRED_CATEGORIES
    v2_heightened = v2_cats & REVIEW_REQUIRED_CATEGORIES

    cats_retained = sorted(v1_heightened & v2_heightened)
    cats_dropped = sorted(v1_heightened - v2_heightened)
    cats_added = sorted(v2_heightened - v1_heightened)

    # Protective-order upgrade pass-through (same as worker stage path)
    po_active = await check_protective_order(conn, v1['document_id'])

    # phi_status low-confidence safe default
    phi_status = v2_proposal['phi_status']
    confidence = v2_proposal['confidence']
    if confidence < CONFIDENCE_MEDIUM:
        phi_status = 'phi'

    v2_staging_id: uuid.UUID
    async with conn.transaction():
        # 1. Insert new v2 staging row
        v2_staging_id = await conn.fetchval("""
            INSERT INTO core.documents_backfill_staging
                (document_id, batch_id, domain,
                 proposed_phi_status, proposed_phi_categories, proposed_minor_patient,
                 confidence, rationale, classifier_version, classifier_model)
            VALUES ($1, $2, 'medical', $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """, v1['document_id'], batch_id, phi_status, sorted(v2_cats),
             bool(v2_proposal.get('minor_patient', False)),
             confidence, v2_proposal['rationale'], V2_VERSION, CLASSIFIER_MODEL)

        # 2. Mark v1 row as superseded
        await conn.execute("""
            UPDATE core.documents_backfill_staging
               SET status='superseded', superseded_at=now()
             WHERE id=$1
        """, v1['staging_id'])

        # 3. Reconcile heightened_review entries — per category
        v1_sid_text = str(v1['staging_id'])
        v2_sid_text = str(v2_staging_id)

        # 3a. Retained categories: supersede v1-linked review, create new v2-linked review
        for cat in cats_retained:
            await conn.execute("""
                UPDATE ops.pending_approvals
                   SET approved=false,
                       approved_at=now(),
                       approved_by='sprint_a_v2_reclassify',
                       approval_method='api',
                       rejection_note=$2
                 WHERE decision_type='sprint_a_t4_heightened_review'
                   AND proposed_action->>'staging_id' = $1
                   AND proposed_action->>'heightened_category' = $3
                   AND approved IS NULL
            """,
                v1_sid_text,
                f"Superseded by v2 reclassification under primary-subject-test prompt; "
                f"v2 also flags this category — new review linked to v2 staging_id.",
                cat,
            )
            await conn.execute("""
                INSERT INTO ops.pending_approvals
                    (decision_type, title, description, proposed_action,
                     reasoning, risk_level)
                VALUES ('sprint_a_t4_heightened_review', $1, $2, $3::jsonb, $4, 'critical')
            """,
                f"Heightened-category review ({cat}) [v2]: "
                f"{(v1['title'] or v1['filename'] or '(none)')[:60]}",
                f"v2 reclassification under primary-subject-test prompt retains this "
                f"heightened category ({cat}). v1 also flagged; v1 review superseded; "
                f"this is the new active review linked to v2 staging_id.",
                json.dumps({
                    'staging_id': v2_sid_text,
                    'document_id': str(v1['document_id']),
                    'heightened_category': cat,
                    'all_proposed_categories': sorted(v2_cats),
                    'phi_status': phi_status,
                    'confidence': float(confidence),
                    'classifier_version': V2_VERSION,
                    'superseded_v1_staging_id': v1_sid_text,
                }),
                v2_proposal['rationale'],
            )

        # 3b. Dropped categories: burden-reduction path
        for cat in cats_dropped:
            await conn.execute("""
                UPDATE ops.pending_approvals
                   SET approved=false,
                       approved_at=now(),
                       approved_by='sprint_a_v2_reclassify',
                       approval_method='api',
                       rejection_note=$2
                 WHERE decision_type='sprint_a_t4_heightened_review'
                   AND proposed_action->>'staging_id' = $1
                   AND proposed_action->>'heightened_category' = $3
                   AND approved IS NULL
            """,
                v1_sid_text,
                f"Superseded by v2 reclassification under primary-subject-test prompt; "
                f"v2 does not reproduce this category flag under the revised "
                f"classificatory standard. v1 classification preserved in audit "
                f"trail (ops.athena_decisions reference).",
                cat,
            )

        # 3c. Added categories: v2 caught something v1 missed
        for cat in cats_added:
            await conn.execute("""
                INSERT INTO ops.pending_approvals
                    (decision_type, title, description, proposed_action,
                     reasoning, risk_level)
                VALUES ('sprint_a_t4_heightened_review', $1, $2, $3::jsonb, $4, 'critical')
            """,
                f"Heightened-category review ({cat}) [v2-new]: "
                f"{(v1['title'] or v1['filename'] or '(none)')[:60]}",
                f"v2 reclassification flags heightened category {cat!r} that v1 "
                f"did not flag. Per v2's primary-subject-test prompt, this "
                f"category applies to the document. Human review required.",
                json.dumps({
                    'staging_id': v2_sid_text,
                    'document_id': str(v1['document_id']),
                    'heightened_category': cat,
                    'all_proposed_categories': sorted(v2_cats),
                    'phi_status': phi_status,
                    'confidence': float(confidence),
                    'classifier_version': V2_VERSION,
                    'v1_did_not_flag': True,
                }),
                v2_proposal['rationale'],
            )

        # 4. Classification-review handling
        # Supersede any v1-linked classification_review row for this staging_id.
        # Create a new v2-linked review only if v2 is medium-confidence.
        await conn.execute("""
            UPDATE ops.pending_approvals
               SET approved=false,
                   approved_at=now(),
                   approved_by='sprint_a_v2_reclassify',
                   approval_method='api',
                   rejection_note=$2
             WHERE decision_type='sprint_a_t4_classification_review'
               AND proposed_action->>'staging_id' = $1
               AND approved IS NULL
        """,
            v1_sid_text,
            f"Superseded by v2 reclassification under primary-subject-test prompt; "
            f"v2 confidence={confidence:.2f}. See ops.athena_decisions for full v1/v2 diff.",
        )
        if CONFIDENCE_MEDIUM <= confidence < CONFIDENCE_HIGH:
            await conn.execute("""
                INSERT INTO ops.pending_approvals
                    (decision_type, title, description, proposed_action,
                     reasoning, risk_level)
                VALUES ('sprint_a_t4_classification_review', $1, $2, $3::jsonb, $4, 'medium')
            """,
                f"Medical classification review [v2]: "
                f"{(v1['title'] or v1['filename'] or '(none)')[:80]}",
                f"v2 medium-confidence ({confidence:.2f}) proposal: "
                f"phi_status={phi_status!r}, categories={sorted(v2_cats)}, "
                f"minor={bool(v2_proposal.get('minor_patient', False))}. "
                f"v2 rationale: {v2_proposal['rationale']}",
                json.dumps({
                    'staging_id': v2_sid_text,
                    'document_id': str(v1['document_id']),
                    'domain': 'medical',
                    'proposed': {'phi_status': phi_status,
                                 'phi_categories': sorted(v2_cats),
                                 'minor_patient': bool(v2_proposal.get('minor_patient', False))},
                    'confidence': float(confidence),
                    'classifier_version': V2_VERSION,
                    'superseded_v1_staging_id': v1_sid_text,
                }),
                v2_proposal['rationale'],
            )

        # 5. ops.athena_decisions record — both v1 and v2 proposals preserved
        await conn.execute("""
            INSERT INTO ops.athena_decisions
                (decision_type, target, description, reasoning, confidence,
                 action_taken, outcome)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        """,
            'sprint_a_t4_v2_reclassification',
            f'core.documents_backfill_staging ({str(v1["document_id"])})',
            f"Reclassified medical document under v2 prompt. "
            f"v1: {sorted(v1_cats)} @ {float(v1['v1_confidence']):.2f}. "
            f"v2: {sorted(v2_cats)} @ {float(confidence):.2f}. "
            f"Actions: retained={cats_retained}, dropped={cats_dropped}, added={cats_added}.",
            f"v2 prompt applies primary-subject test; v1 prompt did not. "
            f"Category-level diff documents how v2's revised reasoning framework "
            f"changed the classification for this document.",
            float(confidence),
            json.dumps({
                'document_id': str(v1['document_id']),
                'v1_staging_id': v1_sid_text,
                'v2_staging_id': v2_sid_text,
                'v1_proposal': {
                    'phi_status': v1['v1_phi_status'],
                    'phi_categories': list(v1_cats),
                    'minor_patient': v1['v1_minor_patient'],
                    'confidence': float(v1['v1_confidence']),
                    'rationale': v1['v1_rationale'],
                },
                'v2_proposal': {
                    'phi_status': phi_status,
                    'phi_categories': sorted(v2_cats),
                    'minor_patient': bool(v2_proposal.get('minor_patient', False)),
                    'confidence': float(confidence),
                    'rationale': v2_proposal['rationale'],
                },
                'categories_retained': cats_retained,
                'categories_dropped': cats_dropped,
                'categories_added': cats_added,
            }),
            'applied',
        )

    return {
        'v1_staging_id': v1_sid_text,
        'v2_staging_id': str(v2_staging_id),
        'retained': cats_retained,
        'dropped': cats_dropped,
        'added': cats_added,
        'v2_confidence': float(confidence),
    }


async def run(dsn: str) -> None:
    anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
    if not anthropic_key:
        sys.exit("ANTHROPIC_API_KEY env var required")
    client = anthropic.AsyncAnthropic(api_key=anthropic_key)

    conn = await asyncpg.connect(dsn)
    try:
        print(f"v2 reclassify as {await conn.fetchval('SELECT current_user')}")
        bucket = await TokenBucket.from_db(conn)
        print(f"rate bucket initialized: {len(bucket.timestamps)} recent consumptions")

        batch_id = uuid.uuid4()
        print(f"batch_id = {batch_id} (v2 reclassification)")

        classifier = DomainClassifier('medical', client)
        candidates = await select_candidates(conn)
        print(f"candidates: {len(candidates)} medical staging rows with v1 mental_health flag")

        summary = {'retained': 0, 'dropped': 0, 'added': 0,
                   'failed': 0, 'reclassified': 0}
        per_cat_retained: dict[str, int] = {}
        per_cat_dropped: dict[str, int] = {}
        per_cat_added: dict[str, int] = {}

        for i, row in enumerate(candidates, start=1):
            await bucket.acquire()
            print(f"  [{i}/{len(candidates)}] v2 classify {str(row['document_id'])[:8]}... "
                  f"title={(row['title'] or row['filename'] or '')[:50]}", flush=True)
            result = await classifier.classify(row)
            if not result['ok']:
                summary['failed'] += 1
                print(f"    [fail] {result['error_detail'][:120]}", file=sys.stderr, flush=True)
                continue

            diff = await reconcile_one(conn, row, result['proposal'], batch_id)
            summary['reclassified'] += 1
            for c in diff['retained']: per_cat_retained[c] = per_cat_retained.get(c, 0) + 1
            for c in diff['dropped']: per_cat_dropped[c] = per_cat_dropped.get(c, 0) + 1
            for c in diff['added']: per_cat_added[c] = per_cat_added.get(c, 0) + 1
            summary['retained'] += len(diff['retained'])
            summary['dropped'] += len(diff['dropped'])
            summary['added'] += len(diff['added'])
            print(f"    v2conf={diff['v2_confidence']:.2f} "
                  f"retained={diff['retained']} dropped={diff['dropped']} added={diff['added']}",
                  flush=True)

        # Completion marker row for the monitor to detect
        await conn.execute("""
            INSERT INTO ops.athena_decisions
                (decision_type, target, description, reasoning, confidence,
                 action_taken, outcome)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
        """,
            'sprint_a_t4_v2_reclassification_complete',
            'ops.pending_approvals + core.documents_backfill_staging',
            f"v2 reclassification complete. Reclassified {summary['reclassified']} of "
            f"{len(candidates)} medical v1 rows flagged with mental_health. "
            f"Categories retained={summary['retained']}, "
            f"dropped={summary['dropped']}, added={summary['added']}.",
            f"Executed per Will 2026-04-23 Path (a) resolution of the §5.4/§6.3 scope "
            f"bug. Burden reduction: 'dropped' heightened_review entries no longer "
            f"require manual review because v2 does not reproduce the flag under "
            f"the primary-subject-test prompt.",
            1.0,
            json.dumps({
                'summary': summary,
                'per_category_retained': per_cat_retained,
                'per_category_dropped': per_cat_dropped,
                'per_category_added': per_cat_added,
                'batch_id': str(batch_id),
                'v1_version': V1_VERSION,
                'v2_version': V2_VERSION,
            }),
            'applied',
        )

        print(f"\nv2 reclassification complete:")
        print(f"  reclassified: {summary['reclassified']}/{len(candidates)} "
              f"(failed: {summary['failed']})")
        print(f"  category-level retained: {per_cat_retained}")
        print(f"  category-level dropped:  {per_cat_dropped}")
        print(f"  category-level added:    {per_cat_added}")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None)
    args = parser.parse_args()
    dsn = args.dsn or os.environ["DATABASE_URL"]
    asyncio.run(run(dsn))


if __name__ == "__main__":
    main()
