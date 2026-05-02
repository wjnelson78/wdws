#!/usr/bin/env python3
"""Phase 4 — Internal cross-reference FK resolution.

Resolves wa_constitution.cross_references rows by populating:
  - target_section_id (UUID FK → wa_constitution.sections) for section-form xrefs
  - target_article_id (UUID FK → wa_constitution.articles) for article-only xrefs
  - target_rcw_section_id (UUID FK → rcw.sections) for state-wa-rcw xrefs
  - target_usc_section_id (UUID FK → usc.sections) for federal-usc xrefs

Idempotent: re-runs only update NULL targets to non-NULL; non-NULL targets
preserved.

Halt: report resolution rate by target_jurisdiction; halt if internal
wa-constitution rate < 50%.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import psycopg2

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/wdws/.env")
except ImportError:
    pass

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase4_internal_xref.log")


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def main() -> int:
    log("=== Phase 4 internal xref resolution begin ===")
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Pre-flight stats
            cur.execute("""
                SELECT target_jurisdiction, COUNT(*) FROM wa_constitution.cross_references
                GROUP BY target_jurisdiction ORDER BY target_jurisdiction
            """)
            log("Pre-flight xref distribution:")
            for jur, ct in cur.fetchall():
                log(f"  {jur}: {ct}")

            # Pass 1: section-form internal wa-constitution → target_section_id
            cur.execute("""
                UPDATE wa_constitution.cross_references xr
                SET target_section_id = s.id
                FROM wa_constitution.sections s
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_section_id IS NULL
                  AND s.canonical_citation = xr.target_citation
                RETURNING xr.id
            """)
            p1 = cur.rowcount
            log(f"Pass 1 (section-form internal): {p1} resolved")

            # Pass 2: article-only internal wa-constitution → target_article_id
            # target_citation like 'Wash. Const. art. IV' (no section part)
            cur.execute("""
                UPDATE wa_constitution.cross_references xr
                SET target_article_id = a.id
                FROM wa_constitution.articles a
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_section_id IS NULL
                  AND xr.target_article_id IS NULL
                  AND xr.target_citation = 'Wash. Const. art. ' || a.article_number
                RETURNING xr.id
            """)
            p2 = cur.rowcount
            log(f"Pass 2 (article-only internal): {p2} resolved")

            # Pass 3: state-wa-rcw → target_rcw_section_id (3-segment exact match)
            cur.execute("""
                UPDATE wa_constitution.cross_references xr
                SET target_rcw_section_id = r.id
                FROM rcw.sections r
                WHERE xr.target_jurisdiction = 'state-wa-rcw'
                  AND xr.target_rcw_section_id IS NULL
                  AND r.canonical_citation = xr.target_citation
                RETURNING xr.id
            """)
            p3 = cur.rowcount
            log(f"Pass 3 (RCW section-form): {p3} resolved")

            # Pass 4: federal-usc → target_usc_section_id (exact match)
            cur.execute("""
                UPDATE wa_constitution.cross_references xr
                SET target_usc_section_id = u.id
                FROM usc.sections u
                WHERE xr.target_jurisdiction = 'federal-usc'
                  AND xr.target_usc_section_id IS NULL
                  AND u.canonical_citation = xr.target_citation
                RETURNING xr.id
            """)
            p4 = cur.rowcount
            log(f"Pass 4 (USC section-form): {p4} resolved")

            # Post-flight: per-jurisdiction resolution rates
            log("Post-flight resolution rates:")
            cur.execute("""
                SELECT target_jurisdiction,
                       COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE
                          target_section_id IS NOT NULL
                          OR target_article_id IS NOT NULL
                          OR target_rcw_section_id IS NOT NULL
                          OR target_usc_section_id IS NOT NULL
                       ) AS resolved
                FROM wa_constitution.cross_references
                GROUP BY target_jurisdiction
                ORDER BY target_jurisdiction
            """)
            stats = {}
            for jur, total, resolved in cur.fetchall():
                rate = resolved / total if total else 0
                stats[jur] = (total, resolved, rate)
                log(f"  {jur}: {resolved}/{total} = {rate:.1%}")

            # Halt check: internal wa-constitution rate must be reasonable
            internal = stats.get('wa-constitution', (0, 0, 0))
            if internal[0] > 0 and internal[2] < 0.5:
                raise HaltError(f"Internal wa-constitution resolution rate {internal[2]:.1%} < 50% — investigate")

            # Per-target_section_id spot-check: Art IV § 31 should resolve from any
            # WCR cross-ref that targets it (this is Phase 4.4 territory but verify
            # the section exists and has correct UUID for downstream uplift)
            cur.execute("""
                SELECT id, canonical_citation, section_heading
                FROM wa_constitution.sections
                WHERE canonical_citation = 'Wash. Const. art. IV, § 31'
            """)
            row = cur.fetchone()
            if not row:
                raise HaltError("Art IV § 31 missing — load-bearing for Phase 4.4")
            log(f"  ✓ Art IV § 31 row: {row[0]} '{row[2]}'")

            conn.commit()
            log("=== Phase 4 internal xref commit ===")
            return 0
    except HaltError as e:
        conn.rollback()
        log(f"HALT: {e} (rolled back)")
        return 2
    except Exception as e:
        conn.rollback()
        log(f"ERROR: {type(e).__name__}: {e} (rolled back)")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
