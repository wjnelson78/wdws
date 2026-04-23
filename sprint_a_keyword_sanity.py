#!/usr/bin/env python3
"""Sprint A Task 4 keyword-pattern sanity check (per v2.2 §6.2, refined).

Runs the refined core_safety.KEYWORD_PATTERNS + classify_by_keywords()
against all legal-domain documents with privilege IS NULL, in READ-ONLY
mode. Writes a finding row to ops.agent_findings with
category='sprint_a_keyword_pattern_review' and evidence JSON containing:
  - new total match count + per-pattern breakdown
  - 10 random samples (seed=42) of current matches
  - delta: documents matched under v1 (permissive) but NOT under v2
    (refined), plus 3-5 random samples of those for disclaimer-footer
    confirmation

Will reviews in conversation; on confirmation, sprint_a_keyword_apply.py
runs the mechanical classification.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sys
from pathlib import Path

import asyncpg

sys.path.insert(0, "/opt/wdws")
from core_safety import (  # noqa: E402
    KEYWORD_PATTERNS,
    classify_by_keywords,
    scan_corpus_for_match_delta,
)


async def run() -> None:
    dsn = os.environ.get("DATABASE_URL_SUPERUSER") or os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch("""
            SELECT id, filename, title, full_content
              FROM core.documents
             WHERE domain = 'legal'
               AND privilege IS NULL
               AND full_content IS NOT NULL
        """)
        print(f"scanned {len(rows)} legal documents with NULL privilege")

        current_matches = []
        pattern_hit_counts = {name: 0 for name in KEYWORD_PATTERNS}
        removed_by_refinement = []  # docs that matched v1 but not v2

        for r in rows:
            content = r["full_content"] or ""
            classification = classify_by_keywords(content)
            old_hit, new_hit = scan_corpus_for_match_delta(content)
            if classification is not None:
                pattern_hit_counts[classification['pattern_matched']] += 1
                current_matches.append({
                    'document_id': str(r['id']),
                    'filename': r['filename'],
                    'title': r['title'],
                    'pattern_matched': classification['pattern_matched'],
                    'matched_text': classification['matched_text'],
                    'context_preview': classification['context_preview'],
                    'match_span': list(classification['match_span']),
                })
            if old_hit is not None and new_hit is None:
                removed_by_refinement.append({
                    'document_id': str(r['id']),
                    'filename': r['filename'],
                    'title': r['title'],
                    'old_pattern': old_hit,
                    'removal_reason': 'unanchored or disclaimer-qualified in v1; strict regex in v2 excludes',
                })

        total = len(current_matches)
        removed_total = len(removed_by_refinement)
        print(f"v2 matches (refined): {total}")
        print(f"documents removed by refinement: {removed_total}")
        print("per-pattern hit counts (v2):")
        for name, cnt in pattern_hit_counts.items():
            print(f"  {name}: {cnt}")

        random.seed(42)
        sample_current = random.sample(current_matches, min(10, total))
        sample_removed = random.sample(removed_by_refinement, min(5, removed_total))

        # Pull the disclaimer context for each removed sample — show why the
        # refinement kicked it out.
        for sr in sample_removed:
            doc = await conn.fetchrow(
                "SELECT full_content FROM core.documents WHERE id = $1::uuid",
                sr['document_id'],
            )
            content = (doc['full_content'] or '') if doc else ''
            # Find the original (unanchored) match
            import re as _re
            old_pat = _re.compile(r'ATTORNEY[-\s]CLIENT\s+PRIVILEGED', _re.IGNORECASE)
            m = old_pat.search(content)
            if m:
                win_start = max(0, m.start() - 200)
                win_end = min(len(content), m.end() + 200)
                sr['match_position'] = m.start()
                sr['context_preview'] = content[win_start:win_end]
                sr['disqualifier_found'] = bool(_re.search(
                    r'may\s+(?:contain|be|include)',
                    content[max(0, m.start()-50):min(len(content), m.end()+50)],
                    _re.IGNORECASE,
                ))
            else:
                # Must have been work_product_header or similar
                sr['context_preview'] = '(match position not recoverable — likely work_product_header)'

        evidence = {
            'pattern_set_version': 'sprint_a_keyword_v2_refined',
            'refinements_applied': [
                'attorney_client_literal now anchored to first 500 chars (\\A.{0,500}?)',
                'attorney_client_literal now disqualified by "may contain"/"may be"/"may include" within ±50 chars of match (disclaimer filter)',
                'privileged_confidential_with_context dropped (0 hits in v1 run, cognitive overhead without benefit)',
            ],
            'patterns_active': list(KEYWORD_PATTERNS.keys()),
            'corpus_scope': {
                'domain': 'legal',
                'privilege_filter': 'IS NULL',
                'rows_scanned': len(rows),
            },
            'match_totals': {
                'total_documents_matching_v2': total,
                'per_pattern_hit_counts': pattern_hit_counts,
                'documents_removed_by_refinement': removed_total,
            },
            'samples_current': sample_current,
            'samples_removed_by_refinement': sample_removed,
            'sample_seed': 42,
            'all_keyword_matches_imply': 'privilege=attorney_client per §6.2',
        }

        finding_id = await conn.fetchval("""
            INSERT INTO ops.agent_findings
                (agent_id, run_id, severity, category, title, detail, evidence)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            RETURNING id
        """,
            'athena', None, 'info',
            'sprint_a_keyword_pattern_review',
            f"Task 4 keyword pattern sanity check (refined v2) — {total} matches, {removed_total} removed by refinement",
            f"Second pass with refined patterns. attorney_client_literal now requires "
            f"match within first 500 chars AND no 'may contain/may be/may include' "
            f"within ±50 chars of match. privileged_confidential_with_context dropped. "
            f"work_product_header unchanged. {total} documents currently match; "
            f"{removed_total} documents matched in v1 but are excluded by v2 — "
            f"spot-check samples in evidence.samples_removed_by_refinement.",
            json.dumps(evidence),
        )
        print(f"\nfinding_id: {finding_id}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())
