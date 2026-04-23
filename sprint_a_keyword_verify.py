#!/usr/bin/env python3
"""Post-refinement verification: scan ALL 9539 legal documents with the
refined-v3 disqualifier set, count matches, and report per-pattern
distribution. Used to confirm Will's expected-outcome of 18 matches
after the FASA reversal + conditional-assertion disqualifier extension.

Does NOT apply or modify anything — pure scan + count.
"""

from __future__ import annotations

import asyncio
import os
import sys

import asyncpg

sys.path.insert(0, "/opt/wdws")
from core_safety import classify_by_keywords  # noqa: E402


async def run() -> None:
    dsn = os.environ.get("DATABASE_URL_SUPERUSER") or os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    try:
        # Scan ALL legal documents — we want to verify the pattern catches
        # the same 18 docs it caught before (minus FASA), regardless of
        # whether their privilege is currently NULL or classified.
        rows = await conn.fetch("""
            SELECT id, filename, title, privilege, full_content
              FROM core.documents
             WHERE domain = 'legal'
               AND full_content IS NOT NULL
        """)
        print(f"scanned {len(rows)} legal documents (all, regardless of current privilege)")

        matches_by_pattern: dict[str, int] = {}
        already_classified_match = 0
        null_match_count = 0
        match_details: list[dict] = []

        for r in rows:
            classification = classify_by_keywords(r["full_content"] or "")
            if classification is None:
                continue
            pat = classification['pattern_matched']
            matches_by_pattern[pat] = matches_by_pattern.get(pat, 0) + 1
            if r['privilege'] is None:
                null_match_count += 1
            else:
                already_classified_match += 1
            match_details.append({
                'id': str(r['id']),
                'title': (r['title'] or '')[:60],
                'pattern': pat,
                'new_privilege': classification['privilege'],
                'current_privilege': r['privilege'],
            })

        total = len(match_details)
        print(f"\ntotal matches under refined v3 (disqualifiers+conditional): {total}")
        print("per-pattern:")
        for pat, cnt in sorted(matches_by_pattern.items()):
            print(f"  {pat}: {cnt}")
        print(f"\nsplit by current-state:")
        print(f"  already classified (privilege NOT NULL): {already_classified_match}")
        print(f"  NULL-privilege (would be applied): {null_match_count}")

        # Show any NULL-privilege matches — these are docs that would be
        # classified on a mechanical re-apply. Expected: 0 (FASA was the
        # only NULL-privilege match before the refinement; refinement
        # excludes it now).
        null_matches = [m for m in match_details if m['current_privilege'] is None]
        if null_matches:
            print(f"\nNULL-privilege docs that STILL match refined pattern:")
            for m in null_matches:
                print(f"  - {m['id'][:8]}... | {m['pattern']} | → {m['new_privilege']} | {m['title']}")

        # Show any classified docs that should no longer match (regression check)
        should_still_match_count = already_classified_match
        print(f"\n18 previously-classified docs expected to still match: {should_still_match_count}")
        if should_still_match_count != 18:
            print(f"  WARNING: expected 18, got {should_still_match_count} — "
                  f"the refinement disqualified a previously-correct classification")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run())
