#!/usr/bin/env python3
"""Rebuild usc.cross_references WITHOUT touching chunks/embeddings.

Parses each USC title XML, looks up each section_id from usc.sections
via canonical_citation, then DELETE+INSERT cross_references rows for that
section. Skips chunk/embed logic entirely — idempotent safe.

Recovery script for TRUNCATE-CASCADE incident 2026-05-01.
"""
from __future__ import annotations
import argparse, os, sys, time
from pathlib import Path

sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2, psycopg2.extras
from usc_parser import parse_title_xml

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
XML_DIR = Path("/opt/wdws/data/usc/extracted")
RELEASE_POINT = "PL 119-84"

# All 53 ingested title numbers
ALL_TITLES = [
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,
    26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,
    48,49,50,51,52,54,
]

# Titles already restored from May 1 backup — skip unless --all
ALREADY_RESTORED = {5, 28, 29, 42}


def xml_path_for(title: int, appendix: bool = False) -> Path | None:
    suffix = "a" if appendix else ""
    p = XML_DIR / f"usc{title:02d}{suffix}.xml"
    return p if p.exists() else None


def rebuild_title_xrefs(conn, title: int, skip_if_nonzero: bool = True) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM usc.cross_references xr
              JOIN usc.sections s ON s.id = xr.source_section_id
             WHERE s.title_number = %s
        """, (str(title),))
        current = cur.fetchone()[0]

    if skip_if_nonzero and current > 0:
        print(f"[T{title}] already has {current:,} xrefs — skipping", flush=True)
        return current

    appendix_mode = False
    xml = xml_path_for(title)
    if xml is None:
        xml = xml_path_for(title, appendix=True)
        appendix_mode = True
    if xml is None:
        print(f"[T{title}] WARNING: no XML file found", flush=True)
        return 0

    print(f"[T{title}] parsing {xml.name} ...", flush=True)
    t0 = time.time()
    n_sections = 0
    n_xrefs = 0

    with conn.cursor() as cur:
        for unit in parse_title_xml(str(xml), title_number=title, release_point=RELEASE_POINT,
                                    is_appendix=appendix_mode):
            if unit.unit_type != "section" or not unit.cross_refs:
                continue
            # Look up section_id by canonical_citation
            cur.execute(
                "SELECT id FROM usc.sections WHERE canonical_citation = %s LIMIT 1",
                (unit.canonical_citation,)
            )
            row = cur.fetchone()
            if row is None:
                continue
            section_id = row[0]
            # Delete + re-insert cross_references for this section
            cur.execute("DELETE FROM usc.cross_references WHERE source_section_id = %s",
                        (section_id,))
            if unit.cross_refs:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO usc.cross_references
                        (source_section_id, target_citation, target_section_id,
                         context_snippet, ref_type, uslm_href)
                    VALUES %s
                """, [(section_id, x.target_citation, None,
                       x.context_snippet, x.ref_type, x.uslm_href)
                      for x in unit.cross_refs])
                n_xrefs += len(unit.cross_refs)
            n_sections += 1

    conn.commit()
    elapsed = time.time() - t0
    print(f"[T{title}] {n_sections:,} sections, {n_xrefs:,} xrefs in {elapsed:.0f}s", flush=True)
    return n_xrefs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--titles", nargs="*", type=int,
                    help="specific title numbers; default=all missing")
    ap.add_argument("--all", action="store_true",
                    help="force re-run even for already-restored titles")
    args = ap.parse_args()

    titles = args.titles or ALL_TITLES
    skip = not args.all

    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    total_xrefs = 0
    for title in titles:
        if skip and title in ALREADY_RESTORED:
            print(f"[T{title}] in ALREADY_RESTORED set — skipping (use --all to override)", flush=True)
            continue
        total_xrefs += rebuild_title_xrefs(conn, title, skip_if_nonzero=skip)

    conn.close()
    print(f"\nDONE: {total_xrefs:,} total xrefs inserted", flush=True)


if __name__ == "__main__":
    main()
