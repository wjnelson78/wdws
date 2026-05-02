"""
Targeted re-ingest of sections corrupted by the pre-fix parser.

Strategy:
  1. Pull the corrupted canonical_citations from the audit query.
  2. Group by title (and appendix flag) so we re-parse each XML once.
  3. For each parsed unit whose canonical_citation is in the corrupted set:
     - DELETE existing core.documents row (CASCADE wipes usc.sections,
       section_history, section_notes, cross_references, document_chunks,
       document_tags).
     - Call ingest_unit() to re-insert with the corrected parser output.
  4. Embed the newly-inserted chunks.
  5. Re-run the audit query — must return 0 rows.
"""
from __future__ import annotations
import sys, time
from pathlib import Path
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2

from usc_parser import parse_title_xml
from usc_ingest import (
    ingest_unit, embed_pending_for_title, load_tag_id_cache,
    POSITIVE_LAW_TITLES, TITLE_NAMES, db_conn, DSN,
)

# (title_number, source_xml_filename, is_appendix)
SOURCE_FILES = [
    (5,  "usc05.xml",  False),
    (28, "usc28.xml",  False),
    (28, "usc28a.xml", True),
    (29, "usc29.xml",  False),
    (42, "usc42.xml",  False),
]
RELEASE = "PL 119-84"
XML_DIR = Path("/opt/wdws/data/usc/extracted")


def get_corrupted_set(conn) -> set[str]:
    """Audit query — sections where title or full_text doesn't contain section_heading."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT s.canonical_citation FROM usc.sections s
            JOIN core.documents d ON d.id = s.document_id
            WHERE s.title_number IN (5, 28, 29, 42)
              AND s.section_heading IS NOT NULL AND s.section_heading <> ''
              AND (d.title NOT LIKE '%' || s.section_heading || '%'
                   OR s.full_text NOT ILIKE '%' || s.section_heading || '%')
        """)
        return {r[0] for r in cur.fetchall()}


def reingest_one(conn, *, unit, title_name, is_positive_law,
                 chapter_cache, subchapter_cache, tag_id_cache,
                 source_filename) -> tuple:
    # Per operator authorization (Option 1): delete Sprint A staging rows for
    # this document first. The FK from documents_backfill_staging.document_id
    # is NO ACTION (deliberate, per Sprint A design — surfaces destructive
    # ops as a halt). For USC docs the staging proposals are no-ops
    # (privilege='none', confidentiality='public', status='pending'),
    # so deletion loses audit-trail only. Idempotent: zero rows when no
    # staging ref exists.
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM core.documents_backfill_staging
            WHERE document_id IN (
                SELECT document_id FROM usc.sections
                WHERE canonical_citation = %s AND release_point = %s
            )
        """, (unit.canonical_citation, RELEASE))
    # Now delete the document row (CASCADE handles the rest of the schema)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM core.documents
            WHERE id IN (
                SELECT document_id FROM usc.sections
                WHERE canonical_citation = %s AND release_point = %s
            )
        """, (unit.canonical_citation, RELEASE))
    # Now insert fresh
    return ingest_unit(
        conn, unit=unit, release_point=RELEASE,
        title_name=title_name, is_positive_law=is_positive_law,
        chapter_id_cache=chapter_cache,
        subchapter_id_cache=subchapter_cache,
        tag_id_cache=tag_id_cache,
        source_filename=source_filename,
    )


def main():
    with db_conn() as conn:
        corrupted = get_corrupted_set(conn)
    print(f"[reingest] corrupted set size: {len(corrupted)}")
    if not corrupted:
        print("[reingest] nothing to do")
        return

    t_start = time.time()
    total_reingested = 0
    total_chunks_new = 0

    for title_num, fname, is_appendix in SOURCE_FILES:
        xml_path = XML_DIR / fname
        if not xml_path.exists():
            print(f"[reingest] SKIP {fname} (not found)")
            continue
        title_name = TITLE_NAMES.get(title_num, f"Title {title_num}")
        is_positive = (title_num in POSITIVE_LAW_TITLES) and not is_appendix

        with db_conn() as conn:
            tag_cache = load_tag_id_cache(conn)
            chapter_cache: dict = {}
            subchapter_cache: dict = {}
            seen_in_file = 0
            reingested_in_file = 0
            for unit in parse_title_xml(xml_path, title_number=title_num,
                                        release_point=RELEASE,
                                        is_appendix=is_appendix):
                seen_in_file += 1
                if unit.canonical_citation not in corrupted:
                    continue
                _, _, n_chunks = reingest_one(
                    conn, unit=unit, title_name=title_name,
                    is_positive_law=is_positive,
                    chapter_cache=chapter_cache,
                    subchapter_cache=subchapter_cache,
                    tag_id_cache=tag_cache,
                    source_filename=fname,
                )
                reingested_in_file += 1
                total_reingested += 1
                total_chunks_new += n_chunks
                if reingested_in_file % 25 == 0:
                    conn.commit()
                    print(f"  [{fname}] {reingested_in_file} re-ingested so far")
            conn.commit()
        print(f"[reingest] {fname}: scanned {seen_in_file} units, "
              f"re-ingested {reingested_in_file} corrupted")

    elapsed = time.time() - t_start
    print(f"\n[reingest] STAGE-1 COMPLETE: {total_reingested} units re-ingested, "
          f"{total_chunks_new} new chunks, {elapsed:.0f}s")

    # Stage 2: embed the new chunks (across all 4 affected titles)
    print(f"\n[reingest] STAGE 2 — embedding new chunks per title")
    for title_num in (5, 28, 29, 42):
        with db_conn() as conn:
            n = embed_pending_for_title(conn, title_number=title_num,
                                        release_point=RELEASE)
            print(f"  title {title_num}: embedded {n} chunks")

    # Final audit verify
    with db_conn() as conn:
        remaining = get_corrupted_set(conn)
    print(f"\n[reingest] post-fix audit: {len(remaining)} corrupted units remain "
          f"(expected: 0)")
    if remaining:
        print(f"  remaining: {sorted(remaining)[:10]}{'...' if len(remaining) > 10 else ''}")
        sys.exit(1)
    print("[reingest] AUDIT CLEAN")


if __name__ == "__main__":
    main()
