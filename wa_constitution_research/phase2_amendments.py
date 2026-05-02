#!/usr/bin/env python3
"""Phase 2.4b — Extract + ingest amendment metadata from PDF Part B index.

Parses the Part B amendment index (110 entries expected) and joins with
ratification-year data extracted from section-body AMENDMENT markers.

Output: rows in wa_constitution.amendments
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

try:
    from dotenv import load_dotenv
    load_dotenv("/opt/wdws/.env")
except ImportError:
    pass

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
PDF_TXT = Path("/tmp/wa_const_cache/constitution.txt")
PARSED_JSONL = Path("/opt/wdws/wa_constitution_research/phase2_parsed.jsonl")
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase2_amendments.log")

PART_B_HEADER_RE = re.compile(r'^\s*\(B\)\s+Constitutional\s+Amendments', re.IGNORECASE)
PART_C_HEADER_RE = re.compile(r'^\s*\(C\)\s+INDEX\s+TO', re.IGNORECASE)
# Amendment index entry: '   25 Art.   4 § 3(a)   Retirement of supreme court'
# Or:                    '   28 Art.   4 § 6     Jurisdiction of superior courts.'
# Or wrapping:           '      Art.   4 § 10    Justices of the peace.'
AMEND_INDEX_RE = re.compile(
    r'^\s+(\d+)\s+Art\.\s+(\d+[A-Z]?)\s+§§?\s+([0-9, ()a-zA-Z]+?)\s{2,}(.+?)\s*$'
)
AMEND_NUM_RE = re.compile(r'^\s+(\d+)\s+Art\.')
# Continuation (no amendment number; secondary article ref): '      Art.   4 § 10  ...'
AMEND_CONT_ART_RE = re.compile(
    r'^\s{20,}Art\.\s+(\d+[A-Z]?)\s+§§?\s+([0-9, ()a-zA-Z]+?)\s{2,}(.+?)\s*$'
)
# Plain continuation (title wrap): '            Uniformity and equality—'
PLAIN_CONT_RE = re.compile(r'^\s{20,}(.+?)\s*$')
# Special: 'Amendment 42 — repeals only': '   42                        (Repeals Art. 2 § 33 ...)'
AMEND_REPEAL_RE = re.compile(r'^\s+(\d+)\s{20,}\((.*)\)\s*$')

EXPECTED_AMENDMENTS = (105, 115)  # band; expected ~110


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def find_part_b(lines: list[str]) -> tuple[int, int]:
    """Return (start_idx, end_idx) for Part B (last (B) header → (C) header)."""
    start = None
    for i, line in enumerate(lines):
        if PART_B_HEADER_RE.match(line):
            start = i
    if start is None:
        raise HaltError("Part B header not found")
    end = len(lines)
    for i, line in enumerate(lines[start:], start):
        if PART_C_HEADER_RE.match(line):
            end = i
            break
    return start, end


def parse_amendment_index(part_b_lines: list[str]) -> dict[int, dict]:
    """Parse the Part B index entries. Returns dict[amendment_number] → metadata."""
    amendments: dict[int, dict] = {}
    current_num: int | None = None

    for raw_line in part_b_lines:
        line = raw_line.rstrip()
        if not line.strip():
            continue
        # Skip page footers
        if re.match(r'^\s*\d+/\d+/\d{4}\s+\d+:\d+\s+(?:AM|PM)\s+\[', line):
            continue
        # Skip Part B title block lines
        if re.match(r'^\s*\(B\)\s+Constitutional', line, re.IGNORECASE):
            continue
        if re.match(r'^\s*\(In Order of Adoption\)', line, re.IGNORECASE):
            continue

        # Try amendment-number index entry
        m = AMEND_INDEX_RE.match(line)
        if m:
            num = int(m.group(1))
            article = m.group(2)
            section_part = m.group(3).strip()
            title = m.group(4).strip().rstrip('.').strip()
            current_num = num
            if num not in amendments:
                amendments[num] = {
                    'amendment_number': num,
                    'affects_articles': [article],
                    'affects_sections': [f"{article} § {section_part}"],
                    'title_parts': [title],
                }
            continue

        # Try repeal-only entry (amendment N has no Art. ref, only "(Repeals ...)")
        m_rep = AMEND_REPEAL_RE.match(line)
        if m_rep:
            num = int(m_rep.group(1))
            current_num = num
            if num not in amendments:
                amendments[num] = {
                    'amendment_number': num,
                    'affects_articles': [],
                    'affects_sections': [],
                    'title_parts': [m_rep.group(2).strip()],
                }
            continue

        # Try secondary article continuation under same amendment
        m_cont = AMEND_CONT_ART_RE.match(line)
        if m_cont and current_num is not None:
            article = m_cont.group(1)
            section_part = m_cont.group(2).strip()
            title_continuation = m_cont.group(3).strip()
            if article not in amendments[current_num]['affects_articles']:
                amendments[current_num]['affects_articles'].append(article)
            amendments[current_num]['affects_sections'].append(f"{article} § {section_part}")
            amendments[current_num]['title_parts'].append(title_continuation)
            continue

        # Plain title continuation
        m_plain = PLAIN_CONT_RE.match(line)
        if m_plain and current_num is not None:
            text = m_plain.group(1).strip()
            # Skip lines that look like new section markers in body
            if re.match(r'^\d+\.\s', text) or text.startswith('SECTION '):
                continue
            amendments[current_num]['title_parts'].append(text)
            continue

        # Unknown line — likely Part B body content; stop index parsing on first non-index line
        # if we've already accumulated some
        if current_num is not None and len(amendments) > 100 and 'Section' in line:
            break

    # Compose final titles
    for num, a in amendments.items():
        a['amendment_title'] = ' '.join(a.pop('title_parts')).strip()
    return amendments


def extract_year_map_from_sections() -> dict[int, int]:
    """Build amendment_number → year map from section-body AMENDMENT markers in parsed.jsonl."""
    year_map: dict[int, int] = {}
    with PARSED_JSONL.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            art = json.loads(line)
            for sec in art['sections']:
                amended_by = sec.get('amended_by') or ''
                # Match 'AMENDMENT 88, 1993'
                m = re.match(r'AMENDMENT\s+(\d+),\s+(\d+)', amended_by)
                if m:
                    num = int(m.group(1))
                    year = int(m.group(2))
                    year_map[num] = year
                # Also pick up repeal-amendment year
                rep = sec.get('repealed_by') or ''
                m2 = re.match(r'AMENDMENT\s+(\d+),\s+(\d+)', rep)
                if m2:
                    num = int(m2.group(1))
                    year = int(m2.group(2))
                    year_map.setdefault(num, year)
    return year_map


def insert_amendments(conn, amendments: dict[int, dict], year_map: dict[int, int]) -> int:
    inserted = 0
    with conn.cursor() as cur:
        for num in sorted(amendments.keys()):
            a = amendments[num]
            year = year_map.get(num)
            ratification_date = f"{year}-01-01" if year else None
            title = a['amendment_title'][:500] if a['amendment_title'] else None
            cur.execute("""
                INSERT INTO wa_constitution.amendments
                    (amendment_number, amendment_title, ratification_date,
                     affects_articles, affects_sections, source_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (amendment_number) DO UPDATE
                  SET amendment_title = EXCLUDED.amendment_title,
                      ratification_date = EXCLUDED.ratification_date,
                      affects_articles = EXCLUDED.affects_articles,
                      affects_sections = EXCLUDED.affects_sections
            """, (
                num, title, ratification_date,
                a.get('affects_articles', []),
                a.get('affects_sections', []),
                "https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/",
            ))
            inserted += 1
    return inserted


def main() -> int:
    log("=== Phase 2.4b amendment ingest begin ===")
    text = PDF_TXT.read_text(encoding='utf-8').replace('\x0c', '')
    lines = text.split('\n')
    log(f"Total lines: {len(lines)}")

    start, end = find_part_b(lines)
    log(f"Part B: lines [{start}, {end}) = {end-start} lines")

    amendments = parse_amendment_index(lines[start:end])
    log(f"Parsed {len(amendments)} amendment index entries")
    if not (EXPECTED_AMENDMENTS[0] <= len(amendments) <= EXPECTED_AMENDMENTS[1]):
        log(f"!! HALT: amendment count {len(amendments)} outside band {EXPECTED_AMENDMENTS}")
        return 2

    # Coverage check: amendments must form contiguous range 1..N
    nums = sorted(amendments.keys())
    if nums[0] != 1:
        log(f"WARN: lowest amendment is {nums[0]}, expected 1")
    if max(nums) - min(nums) + 1 != len(nums):
        missing = set(range(min(nums), max(nums)+1)) - set(nums)
        log(f"WARN: missing amendment numbers: {sorted(missing)}")

    year_map = extract_year_map_from_sections()
    log(f"Year-map from section AMENDMENT markers: {len(year_map)} amendments mapped")

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        n = insert_amendments(conn, amendments, year_map)
        # Post-flight
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wa_constitution.amendments")
            db_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_constitution.amendments WHERE ratification_date IS NOT NULL")
            with_date = cur.fetchone()[0]
        log(f"Inserted/upserted: {n}; DB count: {db_count}; with ratification_date: {with_date}")
        conn.commit()
        log("=== Phase 2.4b amendment ingest commit ===")
        return 0
    except Exception as e:
        conn.rollback()
        log(f"ERROR: {type(e).__name__}: {e} (rolled back)")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
