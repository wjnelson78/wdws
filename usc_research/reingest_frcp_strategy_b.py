"""
Strategy B targeted re-ingest of the 8 FRCP rules (Rules 1-8) that were
corrupted by the duplicate-URI bug in usc28a.xml's Civil wrapper.

Per Strategy B: the parser now drops Social Security Supplemental Rules
1-8 as duplicates and keeps the FRCP main rules. This script DELETEs the
8 corrupted DB rows and re-INSERTs from the patched parser's output.
"""
from __future__ import annotations
import sys
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2

from usc_parser import parse_title_xml
from usc_ingest import (
    ingest_unit, embed_pending_for_title, load_tag_id_cache,
    POSITIVE_LAW_TITLES, TITLE_NAMES, db_conn,
)

TARGETS = {f"Fed. R. Civ. P. {n}" for n in (1, 2, 3, 4, 5, 6, 7, 8)}
RELEASE = "PL 119-84"


def main():
    print(f"Targets: {sorted(TARGETS)}")

    # Pre-DELETE staging refs
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM core.documents_backfill_staging b
                WHERE b.document_id IN (
                    SELECT s.document_id FROM usc.sections s
                    WHERE s.canonical_citation = ANY(%s) AND s.release_point = %s
                )
            """, [list(TARGETS), RELEASE])
            staging_pre = cur.fetchone()[0]
        print(f"Pre-DELETE staging refs for these 8 docs: {staging_pre}")

        # Delete staging
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM core.documents_backfill_staging
                WHERE document_id IN (
                    SELECT s.document_id FROM usc.sections s
                    WHERE s.canonical_citation = ANY(%s) AND s.release_point = %s
                )
            """, [list(TARGETS), RELEASE])
            staging_deleted = cur.rowcount

        # Delete the 8 documents (CASCADE handles the rest)
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM core.documents
                WHERE id IN (
                    SELECT s.document_id FROM usc.sections s
                    WHERE s.canonical_citation = ANY(%s) AND s.release_point = %s
                )
            """, [list(TARGETS), RELEASE])
            docs_deleted = cur.rowcount
        conn.commit()
        print(f"Deleted: {staging_deleted} staging refs, {docs_deleted} documents")

    # Re-parse with patched parser, ingest_unit for the 8 targets only
    n_reingested = 0
    n_chunks_new = 0
    title_name = TITLE_NAMES[28]
    is_positive = False  # T28a is appendix; per Strategy B same convention as before
    with db_conn() as conn:
        tag_cache = load_tag_id_cache(conn)
        chapter_cache: dict = {}
        subchapter_cache: dict = {}
        for unit in parse_title_xml("/opt/wdws/data/usc/extracted/usc28a.xml",
                                     title_number=28, release_point=RELEASE,
                                     is_appendix=True):
            if unit.canonical_citation not in TARGETS:
                continue
            doc_id, sec_id, n_chunks = ingest_unit(
                conn, unit=unit, release_point=RELEASE,
                title_name=title_name, is_positive_law=is_positive,
                chapter_id_cache=chapter_cache,
                subchapter_id_cache=subchapter_cache,
                tag_id_cache=tag_cache,
                source_filename="usc28a.xml",
            )
            n_reingested += 1
            n_chunks_new += n_chunks
            print(f"  RE-INGESTED  {unit.canonical_citation:<22} "
                  f"heading={unit.section_heading[:50]!r} chunks={n_chunks}")
        conn.commit()
    print(f"\nRe-ingested {n_reingested} FRCP rules, {n_chunks_new} new chunks")
    if n_reingested != 8:
        print(f"ERROR: expected 8 re-ingestions, got {n_reingested}")
        sys.exit(1)

    # Embed the new chunks
    print("\nEmbedding new chunks...")
    with db_conn() as conn:
        n = embed_pending_for_title(conn, title_number=28, release_point=RELEASE)
        print(f"Embedded {n} chunks")

    # Verify
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.canonical_citation, s.section_heading
                FROM usc.sections s
                WHERE s.canonical_citation = ANY(%s) AND s.release_point = %s
                ORDER BY s.section_number
            """, [list(TARGETS), RELEASE])
            for c, h in cur.fetchall():
                print(f"  {c:<22} {h[:60]}")


if __name__ == "__main__":
    main()
