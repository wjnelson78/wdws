#!/usr/bin/env python3
"""Phase 4.4 — Cross-corpus FK uplift.

UPDATE existing cross_references rows on rcw + wac + wa_court_rules to populate
target_wa_const_section_id (for section-form) and target_wa_const_article_id
(for article-only) using the wa_constitution.sections + wa_constitution.articles
rows now present.

Expected uplift (per Phase 1 verified state):
  rcw.cross_references → wa_constitution.articles: 34 article-only refs
  wac.cross_references → wa_constitution.articles: 16 article-only refs
  wa_court_rules.cross_references → wa_constitution.sections: 3 section-form refs (all → art. IV § 31)
  wa_court_rules.cross_references → wa_constitution.articles: 1 article-only ref
  TOTAL: 54 refs (matches kickoff projection)

Halt-band: [+50, +60] for total uplift count.

Idempotent: re-runs only update NULL targets to non-NULL.
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
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase4_4_cross_corpus_fk.log")

UPLIFT_BAND = (50, 60)
EXPECTED_RCW_ARTICLE_UPLIFT = 34
EXPECTED_WAC_ARTICLE_UPLIFT = 16
EXPECTED_WCR_SECTION_UPLIFT = 3
EXPECTED_WCR_ARTICLE_UPLIFT = 1
EXPECTED_TOTAL = 54

EXPECTED_RCW = 58529
EXPECTED_USC = 61133
EXPECTED_WAC = 73180
EXPECTED_WCR = 1286
EXPECTED_AUTH_CHAIN = 83752


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def preflight(conn) -> None:
    """Verify state matches Phase 1 + Phase 2-4 disposition."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wa_constitution.articles")
        a = cur.fetchone()[0]
        if a != 32:
            raise HaltError(f"Pre-flight: wa_constitution.articles={a}, expected 32")
        cur.execute("SELECT COUNT(*) FROM wa_constitution.sections")
        s = cur.fetchone()[0]
        if not (270 <= s <= 285):
            raise HaltError(f"Pre-flight: wa_constitution.sections={s}, outside [270, 285]")

        # Existing corpora intact
        cur.execute("SELECT COUNT(*) FROM rcw.sections")
        if cur.fetchone()[0] != EXPECTED_RCW:
            raise HaltError("Pre-flight: rcw.sections drift")
        cur.execute("SELECT COUNT(*) FROM usc.sections")
        if cur.fetchone()[0] != EXPECTED_USC:
            raise HaltError("Pre-flight: usc.sections drift")
        cur.execute("SELECT COUNT(*) FROM wac.sections")
        if cur.fetchone()[0] != EXPECTED_WAC:
            raise HaltError("Pre-flight: wac.sections drift")
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
        if cur.fetchone()[0] != EXPECTED_WCR:
            raise HaltError("Pre-flight: wa_court_rules.rules drift")
        cur.execute("SELECT COUNT(*) FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL")
        if cur.fetchone()[0] != EXPECTED_AUTH_CHAIN:
            raise HaltError("Pre-flight: wac.authority_chain orthogonality witness drift")

        # Verify 54 cross-corpus xrefs to wa-constitution unchanged
        cur.execute("""
            SELECT 'rcw' AS src, COUNT(*) FROM rcw.cross_references
            WHERE target_jurisdiction='wa-constitution'
            UNION ALL
            SELECT 'wac', COUNT(*) FROM wac.cross_references
            WHERE target_jurisdiction='wa-constitution'
            UNION ALL
            SELECT 'wcr', COUNT(*) FROM wa_court_rules.cross_references
            WHERE target_jurisdiction='wa-constitution'
        """)
        counts = {src: c for src, c in cur.fetchall()}
        if counts != {'rcw': 34, 'wac': 16, 'wcr': 4}:
            raise HaltError(f"Pre-flight: cross-corpus xref distribution mismatch: {counts}")

        # Verify Art IV § 31 row exists (load-bearing for WCR uplift)
        cur.execute("SELECT id FROM wa_constitution.sections WHERE canonical_citation='Wash. Const. art. IV, § 31'")
        if not cur.fetchone():
            raise HaltError("Pre-flight: Wash. Const. art. IV, § 31 absent from wa_constitution.sections")

    log("  [ok] pre-flight: schema present, baselines intact, 54 xrefs ready, Art IV § 31 present")


def main() -> int:
    log("=== Phase 4.4 cross-corpus FK uplift begin ===")
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        preflight(conn)

        with conn.cursor() as cur:
            # Pass A: RCW article-only → target_wa_const_article_id
            cur.execute("""
                UPDATE rcw.cross_references xr
                SET target_wa_const_article_id = a.id
                FROM wa_constitution.articles a
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_article_id IS NULL
                  AND xr.target_citation = 'Wash. Const. art. ' || a.article_number
            """)
            rcw_article = cur.rowcount
            log(f"Pass A — RCW article-only: {rcw_article} resolved (expected {EXPECTED_RCW_ARTICLE_UPLIFT})")

            # Pass B: RCW section-form → target_wa_const_section_id
            cur.execute("""
                UPDATE rcw.cross_references xr
                SET target_wa_const_section_id = s.id
                FROM wa_constitution.sections s
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_section_id IS NULL
                  AND s.canonical_citation = xr.target_citation
            """)
            rcw_section = cur.rowcount
            log(f"Pass B — RCW section-form: {rcw_section} resolved (expected 0)")

            # Pass C: WAC article-only → target_wa_const_article_id
            cur.execute("""
                UPDATE wac.cross_references xr
                SET target_wa_const_article_id = a.id
                FROM wa_constitution.articles a
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_article_id IS NULL
                  AND xr.target_citation = 'Wash. Const. art. ' || a.article_number
            """)
            wac_article = cur.rowcount
            log(f"Pass C — WAC article-only: {wac_article} resolved (expected {EXPECTED_WAC_ARTICLE_UPLIFT})")

            # Pass D: WAC section-form → target_wa_const_section_id
            cur.execute("""
                UPDATE wac.cross_references xr
                SET target_wa_const_section_id = s.id
                FROM wa_constitution.sections s
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_section_id IS NULL
                  AND s.canonical_citation = xr.target_citation
            """)
            wac_section = cur.rowcount
            log(f"Pass D — WAC section-form: {wac_section} resolved (expected 0)")

            # Pass E: WCR section-form → target_wa_const_section_id
            cur.execute("""
                UPDATE wa_court_rules.cross_references xr
                SET target_wa_const_section_id = s.id
                FROM wa_constitution.sections s
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_section_id IS NULL
                  AND s.canonical_citation = xr.target_citation
            """)
            wcr_section = cur.rowcount
            log(f"Pass E — WCR section-form: {wcr_section} resolved (expected {EXPECTED_WCR_SECTION_UPLIFT})")

            # Pass F: WCR article-only → target_wa_const_article_id
            cur.execute("""
                UPDATE wa_court_rules.cross_references xr
                SET target_wa_const_article_id = a.id
                FROM wa_constitution.articles a
                WHERE xr.target_jurisdiction = 'wa-constitution'
                  AND xr.target_wa_const_article_id IS NULL
                  AND xr.target_citation = 'Wash. Const. art. ' || a.article_number
            """)
            wcr_article = cur.rowcount
            log(f"Pass F — WCR article-only: {wcr_article} resolved (expected {EXPECTED_WCR_ARTICLE_UPLIFT})")

            total_uplift = rcw_article + rcw_section + wac_article + wac_section + wcr_section + wcr_article
            log(f"\nTOTAL uplift: {total_uplift} (expected {EXPECTED_TOTAL}; halt-band {UPLIFT_BAND})")

            if not (UPLIFT_BAND[0] <= total_uplift <= UPLIFT_BAND[1]):
                raise HaltError(f"Total uplift {total_uplift} outside band {UPLIFT_BAND}")

            # Post-flight per-corpus resolution rate (for wa-constitution xrefs)
            log("\nPost-flight per-corpus wa-constitution FK resolution rate:")
            for src_table, src_label in [
                ('rcw.cross_references', 'rcw'),
                ('wac.cross_references', 'wac'),
                ('wa_court_rules.cross_references', 'wcr'),
            ]:
                cur.execute(f"""
                    SELECT COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE
                             target_wa_const_section_id IS NOT NULL
                             OR target_wa_const_article_id IS NOT NULL
                           ) AS resolved
                    FROM {src_table}
                    WHERE target_jurisdiction='wa-constitution'
                """)
                total, resolved = cur.fetchone()
                rate = resolved / total if total else 0
                log(f"  {src_label}: {resolved}/{total} = {rate:.1%}")

            # Existing corpora must remain unchanged in row counts (only added FK columns
            # were touched; original rows still present)
            cur.execute("SELECT COUNT(*) FROM rcw.sections")
            if cur.fetchone()[0] != EXPECTED_RCW:
                raise HaltError("Post-flight: rcw.sections drift")
            cur.execute("SELECT COUNT(*) FROM usc.sections")
            if cur.fetchone()[0] != EXPECTED_USC:
                raise HaltError("Post-flight: usc.sections drift")
            cur.execute("SELECT COUNT(*) FROM wac.sections")
            if cur.fetchone()[0] != EXPECTED_WAC:
                raise HaltError("Post-flight: wac.sections drift")
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
            if cur.fetchone()[0] != EXPECTED_WCR:
                raise HaltError("Post-flight: wa_court_rules.rules drift")
            cur.execute("SELECT COUNT(*) FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL")
            if cur.fetchone()[0] != EXPECTED_AUTH_CHAIN:
                raise HaltError("Post-flight: wac.authority_chain orthogonality witness drift")

            # Operator-active matter authority verification: 3 WCR section-form
            # uplifts must all resolve to Art IV § 31's UUID
            cur.execute("""
                SELECT s.id FROM wa_constitution.sections s
                WHERE s.canonical_citation = 'Wash. Const. art. IV, § 31'
            """)
            art_iv_31_uuid = str(cur.fetchone()[0])
            cur.execute("""
                SELECT COUNT(*) FROM wa_court_rules.cross_references
                WHERE target_jurisdiction='wa-constitution'
                  AND target_wa_const_section_id::text = %s
            """, (art_iv_31_uuid,))
            wcr_iv31_count = cur.fetchone()[0]
            if wcr_iv31_count != 3:
                raise HaltError(f"Post-flight: WCR refs resolving to Art IV § 31 = {wcr_iv31_count}, expected 3")
            log(f"  ✓ WCR section-form uplift: 3 refs all resolve to Art IV § 31 ({art_iv_31_uuid})")

            log("  ✓ post-flight: all 5 corpora intact; orthogonality witness intact")

            conn.commit()
            log("=== Phase 4.4 cross-corpus FK uplift commit ===")
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
