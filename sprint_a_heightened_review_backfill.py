#!/usr/bin/env python3
"""Sprint A Task 4 heightened-review backfill (post-sample_batch remediation).

After sample_batch completes, this script sweeps all pending staging rows
and creates ops.pending_approvals heightened-review entries for any of the
five REVIEW_REQUIRED_CATEGORIES that didn't already have one. Idempotent:
skips (staging_id, category) pairs that already have a review row.

Run AFTER sample_batch completes, BEFORE the T4_SAMPLE_REVIEW
ops.pending_approvals row is created, so the review queue Will sees at
T4_SAMPLE_REVIEW approval time is complete per v2.2 §6.3.

Per Will 2026-04-23 Option 2 fix: the first 100 classifications from the
running worker used the narrow HEIGHTENED_CATEGORIES set (2 cats). The
worker code has since been updated to use REVIEW_REQUIRED_CATEGORIES (5
cats), but the already-running Python process doesn't reload. This
backfill produces the correct final state regardless of which version of
the worker code produced any given staging row.

Usage:
    sudo -u postgres /opt/wdws/venv/bin/python3 \\
        /opt/wdws/sprint_a_heightened_review_backfill.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import asyncpg

sys.path.insert(0, "/opt/wdws")
from core_safety import REVIEW_REQUIRED_CATEGORIES  # noqa: E402


async def run(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        executing_role = await conn.fetchval("SELECT current_user")
        print(f"backfill as {executing_role}")

        # Pull every pending staging row with any review-required category
        rows = await conn.fetch("""
            SELECT s.id AS staging_id, s.document_id, s.confidence,
                   s.proposed_phi_categories, s.proposed_phi_status,
                   s.rationale,
                   d.title, d.filename, d.document_type
              FROM core.documents_backfill_staging s
              JOIN core.documents d ON d.id = s.document_id
             WHERE s.status = 'pending'
               AND s.proposed_phi_categories && $1::text[]
        """, sorted(REVIEW_REQUIRED_CATEGORIES))

        print(f"scanned {len(rows)} pending staging rows with any "
              f"review-required category")

        # For each staging row, for each review-required category it has,
        # check whether a matching review row already exists — if not, create.
        created_count = 0
        skipped_count = 0
        per_category: dict[str, int] = {}

        async with conn.transaction():
            for r in rows:
                cats_in_doc = [c for c in r['proposed_phi_categories']
                               if c in REVIEW_REQUIRED_CATEGORIES]
                for cat in cats_in_doc:
                    existing = await conn.fetchval("""
                        SELECT COUNT(*) FROM ops.pending_approvals
                         WHERE decision_type = 'sprint_a_t4_heightened_review'
                           AND proposed_action->>'staging_id' = $1
                           AND proposed_action->>'heightened_category' = $2
                    """, str(r['staging_id']), cat)
                    if existing:
                        skipped_count += 1
                        continue

                    await conn.execute("""
                        INSERT INTO ops.pending_approvals
                            (decision_type, title, description,
                             proposed_action, reasoning, risk_level)
                        VALUES ($1, $2, $3, $4, $5, 'critical')
                    """,
                        'sprint_a_t4_heightened_review',
                        f"Heightened-category review ({cat}) [backfill]: "
                        f"{(r['title'] or r['filename'] or '(none)')[:60]}",
                        (f"[BACKFILL by sprint_a_heightened_review_backfill.py] "
                         f"Classifier flagged heightened category {cat!r} on "
                         f"this medical document. Per v2.2 §6.3, the five "
                         f"heightened categories (42 CFR Part 2, psychotherapy "
                         f"notes, mental health, HIV, genetic) all require "
                         f"classification-time human review. The worker "
                         f"originally used the narrower §5.4 two-category set; "
                         f"this backfill pass reconciles the gap. Human review "
                         f"required regardless of classifier confidence "
                         f"({float(r['confidence']):.2f})."),
                        json.dumps({
                            'staging_id': str(r['staging_id']),
                            'document_id': str(r['document_id']),
                            'heightened_category': cat,
                            'all_proposed_categories': list(r['proposed_phi_categories']),
                            'phi_status': r['proposed_phi_status'],
                            'confidence': float(r['confidence']),
                            'backfill': True,
                        }),
                        r['rationale'],
                    )
                    created_count += 1
                    per_category[cat] = per_category.get(cat, 0) + 1

            # Record the backfill as an ops.athena_decisions row
            await conn.execute("""
                INSERT INTO ops.athena_decisions
                    (decision_type, target, description, reasoning, confidence,
                     action_taken, outcome)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """,
                'sprint_a_t4_heightened_review_backfill',
                'ops.pending_approvals',
                f"Backfilled {created_count} heightened_review rows "
                f"(skipped {skipped_count} already-existing).",
                ("Remediation for the §5.4 vs §6.3 scope-divergence bug: the "
                 "worker used the narrow HEIGHTENED_CATEGORIES set (2 cats) "
                 "for review routing when §6.3 requires all 5. Backfill "
                 "sweeps all pending staging rows and creates heightened_"
                 "review entries for any of the 5 categories that weren't "
                 "already gated. Idempotent: (staging_id, category) pairs "
                 "already having a review row are skipped."),
                1.0,
                json.dumps({
                    'created_count': created_count,
                    'skipped_count': skipped_count,
                    'per_category': per_category,
                    'executing_role': executing_role,
                }),
                'applied',
            )

        print(f"\nbackfill complete: created={created_count} "
              f"skipped={skipped_count} per_category={per_category}")
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
