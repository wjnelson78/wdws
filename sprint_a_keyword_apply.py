#!/usr/bin/env python3
"""Sprint A Task 4 §6.2 mechanical keyword auto-classification pass.

Applies the refined core_safety.KEYWORD_PATTERNS set — with the
PATTERN_TO_PRIVILEGE mapping ('work_product_header' → 'work_product_opinion'
per Will 2026-04-23) — to every legal-domain document with privilege IS NULL.

Must run as postgres (core.documents is postgres-owned). Invoke via:

    sudo -u postgres /opt/wdws/venv/bin/python3 \\
        /opt/wdws/sprint_a_keyword_apply.py \\
        --dsn postgresql:///wdws?host=/var/run/postgresql

Writes:
  - UPDATE core.documents SET privilege=<mapped>,
         privilege_classified_by='keyword_autoclassify_v1',
         privilege_classified_at=now()
    for each matching document, WHERE privilege IS NULL (no overwrite).
  - Single progress row to ops.athena_decisions with decision_type=
    'sprint_a_keyword_autoclassify' per v2.2 §6.2.

No stop-gate. Mechanical + deterministic + bounded to NULL-privilege rows.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, "/opt/wdws")
from core_safety import (  # noqa: E402
    KEYWORD_PATTERNS,
    PATTERN_TO_PRIVILEGE,
    classify_by_keywords,
)


async def run(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        executing_role = await conn.fetchval("SELECT current_user")
        print(f"connected as: {executing_role}")

        rows = await conn.fetch("""
            SELECT id, filename, title, full_content
              FROM core.documents
             WHERE domain = 'legal'
               AND privilege IS NULL
               AND full_content IS NOT NULL
        """)
        print(f"corpus size (legal + NULL privilege): {len(rows)}")

        classified: list[dict] = []
        for r in rows:
            classification = classify_by_keywords(r["full_content"] or "")
            if classification is None:
                continue
            classified.append({
                'id': r['id'],
                'filename': r['filename'],
                'title': r['title'],
                'pattern': classification['pattern_matched'],
                'privilege': classification['privilege'],
            })

        dist: dict[str, int] = {}
        for c in classified:
            dist[c['privilege']] = dist.get(c['privilege'], 0) + 1

        print(f"classified: {len(classified)}")
        for priv, cnt in sorted(dist.items()):
            print(f"  {priv}: {cnt}")

        if not classified:
            print("no matches; nothing to apply")
            return

        # Apply in a single transaction: UPDATEs + one decision row.
        async with conn.transaction():
            # Defensive WHERE: only UPDATE if privilege IS NULL (no overwrite).
            applied = 0
            for c in classified:
                status = await conn.execute("""
                    UPDATE core.documents
                       SET privilege = $2,
                           privilege_classified_by = 'keyword_autoclassify_v1',
                           privilege_classified_at = now()
                     WHERE id = $1::uuid AND privilege IS NULL
                """, c['id'], c['privilege'])
                # asyncpg returns "UPDATE <n>"
                if status.endswith(" 1"):
                    applied += 1

            decision_id = await conn.fetchval("""
                INSERT INTO ops.athena_decisions
                    (decision_type, target, description, reasoning,
                     confidence, action_taken, outcome)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                RETURNING id
            """,
                'sprint_a_keyword_autoclassify',
                'core.documents.privilege',
                (f"Task 4 §6.2 mechanical keyword pass: applied {applied} "
                 f"classifications (of {len(classified)} matches, "
                 f"{len(rows)} rows scanned)."),
                ("Applied the refined KEYWORD_PATTERNS (v2) with the "
                 "work_product_header → work_product_opinion mapping per "
                 "Will's conversational approval 2026-04-23 (post-sanity-"
                 "check finding id 49023). Two patterns active: "
                 "attorney_client_literal and work_product_header. "
                 "Disqualifier check excludes disclaimer footers via ±50-"
                 "char 'may contain/may be/may include' filter. "
                 "attorney_client_literal requires match in first 500 chars. "
                 "Execution role: " + executing_role + ". Bounded by "
                 "WHERE privilege IS NULL — existing classifications not "
                 "overwritten."),
                1.0,  # deterministic pass → confidence 1.0
                json.dumps({
                    'pattern_set_version': 'sprint_a_keyword_v2_refined',
                    'classified_by': 'keyword_autoclassify_v1',
                    'executing_role': executing_role,
                    'corpus_scope': {
                        'domain': 'legal',
                        'privilege_filter_at_scan': 'IS NULL',
                        'rows_scanned': len(rows),
                    },
                    'classification_counts': dist,
                    'total_matched': len(classified),
                    'total_applied': applied,
                    'skipped_due_to_concurrent_classification': len(classified) - applied,
                    'pattern_to_privilege_mapping': PATTERN_TO_PRIVILEGE,
                    'classified_document_ids': [str(c['id']) for c in classified],
                    'sanity_check_finding_id': 49023,
                    'approval_method': 'conversation',
                    'approved_by': 'will',
                }),
                'applied',
            )

        print(f"\nops.athena_decisions row: {decision_id}")
        print(f"applied: {applied} (matched: {len(classified)})")
        if applied != len(classified):
            print(f"  NOTE: {len(classified) - applied} matches did not UPDATE "
                  f"(row's privilege was not NULL at UPDATE time — concurrent "
                  f"classification).")
    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=None,
                        help="Override DATABASE_URL. Use a socket peer-auth "
                             "DSN when invoked via sudo -u postgres.")
    args = parser.parse_args()
    dsn = args.dsn or os.environ["DATABASE_URL"]
    asyncio.run(run(dsn))


if __name__ == "__main__":
    main()
