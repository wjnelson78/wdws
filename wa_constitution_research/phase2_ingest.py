#!/usr/bin/env python3
"""Phase 2.4a — Ingest: articles + sections + cross-references into PostgreSQL.

Source: /opt/wdws/wa_constitution_research/phase2_parsed.jsonl
Target: wa_constitution.articles, wa_constitution.sections, core.documents,
        wa_constitution.cross_references

Single transaction. Pre-flight + post-flight halt-on-fail.

Idempotency:
  - Articles:    UNIQUE on article_number → ON CONFLICT DO NOTHING; resolve UUID via SELECT
  - Sections:    UNIQUE on canonical_citation → ON CONFLICT DO NOTHING
  - core.documents: source_path 'wa_const:2026-05-01:art={ROMAN}:sec={NUM}' →
                    no UNIQUE constraint; we DELETE+INSERT per section to ensure
                    fresh state
  - cross_references: DELETE-by-source_section + INSERT (whole-section refresh)

Halt triggers:
  - article count != 32
  - section count outside [270, 285]
  - 6 operationally-active sections must be present + active post-INSERT
  - Existing corpora (rcw/usc/wac/wcr) row counts must be unchanged
"""
from __future__ import annotations

import json
import os
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
PARSED_JSONL = Path("/opt/wdws/wa_constitution_research/phase2_parsed.jsonl")
LOG_PATH = Path("/opt/wdws/wa_constitution_research/phase2_ingest.log")

EXPECTED_ARTICLES = 32
SECTION_BAND = (270, 285)
EXPECTED_RCW = 58529
EXPECTED_USC = 61133
EXPECTED_WAC = 73180
EXPECTED_WCR = 1286
EXPECTED_AUTH_CHAIN = 83752
ACTIVE_TARGETS = [
    ('I', '3'), ('I', '12'), ('I', '14'), ('I', '21'), ('I', '32'),
    ('IV', '31'),
]
INGEST_DATE = '2026-05-01'

ROMAN_TO_INT = {
    'I':1,'II':2,'III':3,'IV':4,'V':5,'VI':6,'VII':7,'VIII':8,'IX':9,'X':10,
    'XI':11,'XII':12,'XIII':13,'XIV':14,'XV':15,'XVI':16,'XVII':17,'XVIII':18,
    'XIX':19,'XX':20,'XXI':21,'XXII':22,'XXIII':23,'XXIV':24,'XXV':25,'XXVI':26,
    'XXVII':27,'XXVIII':28,'XXIX':29,'XXX':30,'XXXI':31,'XXXII':32,
}


class HaltError(Exception):
    pass


def log(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def preflight(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name='wa_constitution'")
        if cur.fetchone()[0] != 1:
            raise HaltError("Pre-flight: wa_constitution schema absent (Phase 2.1 not applied?)")
        cur.execute("SELECT COUNT(*) FROM rcw.sections")
        if cur.fetchone()[0] != EXPECTED_RCW:
            raise HaltError(f"Pre-flight: rcw.sections drift, expected {EXPECTED_RCW}")
        cur.execute("SELECT COUNT(*) FROM usc.sections")
        if cur.fetchone()[0] != EXPECTED_USC:
            raise HaltError(f"Pre-flight: usc.sections drift, expected {EXPECTED_USC}")
        cur.execute("SELECT COUNT(*) FROM wac.sections")
        if cur.fetchone()[0] != EXPECTED_WAC:
            raise HaltError(f"Pre-flight: wac.sections drift, expected {EXPECTED_WAC}")
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
        if cur.fetchone()[0] != EXPECTED_WCR:
            raise HaltError(f"Pre-flight: wa_court_rules.rules drift, expected {EXPECTED_WCR}")
        cur.execute("SELECT COUNT(*) FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL")
        if cur.fetchone()[0] != EXPECTED_AUTH_CHAIN:
            raise HaltError(f"Pre-flight: wac.authority_chain orthogonality witness drift")
    log("  [ok] pre-flight: schema present + 5 baselines intact")


def insert_article(conn, art: dict) -> str:
    """INSERT article row, return UUID."""
    arabic = ROMAN_TO_INT[art['article_number']]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wa_constitution.articles
                (article_number, article_number_arabic, article_title, source_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (article_number) DO UPDATE
              SET article_title = EXCLUDED.article_title,
                  source_url = EXCLUDED.source_url
            RETURNING id
        """, (
            art['article_number'],
            arabic,
            art['article_title'] or f"Article {art['article_number']}",
            f"https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/",
        ))
        return str(cur.fetchone()[0])


def insert_section(conn, art: dict, art_uuid: str, sec: dict) -> str | None:
    """INSERT core.documents + wa_constitution.sections rows. Returns section UUID."""
    canonical = sec['canonical_citation']
    section_num = sec['section_number']
    full_text = sec.get('full_text') or ''
    is_repealed = bool(sec.get('is_repealed'))
    is_amended = bool(sec.get('is_amended'))
    repealed_by = sec.get('repealed_by')
    amended_by = sec.get('amended_by')
    section_heading = sec.get('section_heading') or ''
    original_text = sec.get('original_text')
    effective_year = sec.get('effective_date_year')
    letter_pattern = sec.get('letter_suffix_pattern')

    # core.documents row
    source_path = f"wa_const:{INGEST_DATE}:art={art['article_number']}:sec={section_num}"
    title = f"{canonical} — {section_heading}".strip(' —')
    metadata = {
        'corpus': 'wa-constitution',
        'article_number': art['article_number'],
        'article_title': art['article_title'],
        'section_number': section_num,
        'canonical_citation': canonical,
        'is_repealed': is_repealed,
        'is_amended': is_amended,
        'amended_by': amended_by,
        'repealed_by': repealed_by,
        'effective_date_year': effective_year,
        'letter_suffix_pattern': letter_pattern,
    }

    with conn.cursor() as cur:
        # Resolve existing document_id for idempotent re-run
        cur.execute("""
            SELECT id FROM core.documents
            WHERE source_path = %s AND domain = 'legal' AND document_type = 'constitution-section'
        """, (source_path,))
        row = cur.fetchone()
        if row:
            doc_id = str(row[0])
            cur.execute("""
                UPDATE core.documents
                   SET title = %s, full_content = %s, metadata = %s, updated_at = NOW()
                 WHERE id = %s
            """, (title, full_text, json.dumps(metadata), doc_id))
        else:
            cur.execute("""
                INSERT INTO core.documents
                    (domain, source_path, document_type, title, full_content, metadata, legal_hold)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                'legal', source_path, 'constitution-section',
                title, full_text, json.dumps(metadata), False,
            ))
            doc_id = str(cur.fetchone()[0])

        # wa_constitution.sections row
        # Schema CHECK: full_text required if not repealed (chk_section_text_status_invariant)
        # If parser left full_text NULL on a non-repealed section, fall back to section_heading marker
        section_full_text = full_text if full_text else None
        if not is_repealed and not section_full_text:
            section_full_text = f"[{section_heading}] (text unavailable from source PDF)"

        # ON CONFLICT canonical_citation DO UPDATE
        try:
            effective_date_param = f"{effective_year}-01-01" if effective_year else None
            cur.execute("""
                INSERT INTO wa_constitution.sections
                    (document_id, article_id, section_number, canonical_citation,
                     section_heading, full_text, is_repealed, is_amended, amended_by,
                     original_text, effective_date, source_url, letter_suffix_pattern)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (canonical_citation) DO UPDATE
                  SET document_id = EXCLUDED.document_id,
                      section_heading = EXCLUDED.section_heading,
                      full_text = EXCLUDED.full_text,
                      is_repealed = EXCLUDED.is_repealed,
                      is_amended = EXCLUDED.is_amended,
                      amended_by = EXCLUDED.amended_by,
                      original_text = EXCLUDED.original_text,
                      effective_date = EXCLUDED.effective_date,
                      letter_suffix_pattern = EXCLUDED.letter_suffix_pattern
                RETURNING id
            """, (
                doc_id, art_uuid, section_num, canonical,
                section_heading, section_full_text,
                is_repealed, is_amended, amended_by,
                original_text, effective_date_param,
                f"https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/",
                letter_pattern,
            ))
            return str(cur.fetchone()[0])
        except psycopg2.errors.CheckViolation as e:
            log(f"  ✗ CHECK violation on {canonical}: {e}")
            raise


def insert_cross_refs(conn, section_uuid: str, sec: dict) -> int:
    """DELETE existing + INSERT current cross-refs for this section."""
    xrefs = sec.get('cross_references', [])
    with conn.cursor() as cur:
        cur.execute("DELETE FROM wa_constitution.cross_references WHERE source_section_id = %s", (section_uuid,))
        if not xrefs:
            return 0
        rows = []
        for xr in xrefs:
            rows.append((
                section_uuid,
                xr['target_citation'],
                xr['target_jurisdiction'],
                xr.get('context_snippet'),
                xr.get('ref_type', 'reference'),
            ))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO wa_constitution.cross_references
                  (source_section_id, target_citation, target_jurisdiction, context_snippet, ref_type)
               VALUES %s""",
            rows,
        )
        return len(rows)


def postflight(conn, articles_inserted: int, sections_inserted: int, xrefs_inserted: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wa_constitution.articles")
        ac = cur.fetchone()[0]
        if ac != EXPECTED_ARTICLES:
            raise HaltError(f"Post-flight: articles count={ac}, expected {EXPECTED_ARTICLES}")
        cur.execute("SELECT COUNT(*) FROM wa_constitution.sections")
        sc = cur.fetchone()[0]
        if not (SECTION_BAND[0] <= sc <= SECTION_BAND[1]):
            raise HaltError(f"Post-flight: sections count={sc}, outside band {SECTION_BAND}")
        cur.execute("SELECT COUNT(*) FROM wa_constitution.cross_references")
        xc = cur.fetchone()[0]
        if xc < 10:
            raise HaltError(f"Post-flight: cross_references count={xc}, suspiciously low")

        # Operationally-active sections present + not repealed
        for art_num, sec_num in ACTIVE_TARGETS:
            cur.execute("""
                SELECT s.is_repealed, s.section_heading
                FROM wa_constitution.sections s
                JOIN wa_constitution.articles a ON s.article_id = a.id
                WHERE a.article_number = %s AND s.section_number = %s
            """, (art_num, sec_num))
            row = cur.fetchone()
            if not row:
                raise HaltError(f"Post-flight: missing operationally-active Art {art_num} § {sec_num}")
            is_rep, heading = row
            if is_rep:
                raise HaltError(f"Post-flight: Art {art_num} § {sec_num} flagged repealed")
            log(f"  ✓ Art {art_num} § {sec_num}: {heading!r}")

        # Existing corpora unchanged
        cur.execute("SELECT COUNT(*) FROM rcw.sections")
        if cur.fetchone()[0] != EXPECTED_RCW:
            raise HaltError("Post-flight: rcw.sections drift after ingest")
        cur.execute("SELECT COUNT(*) FROM usc.sections")
        if cur.fetchone()[0] != EXPECTED_USC:
            raise HaltError("Post-flight: usc.sections drift after ingest")
        cur.execute("SELECT COUNT(*) FROM wac.sections")
        if cur.fetchone()[0] != EXPECTED_WAC:
            raise HaltError("Post-flight: wac.sections drift after ingest")
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
        if cur.fetchone()[0] != EXPECTED_WCR:
            raise HaltError("Post-flight: wa_court_rules.rules drift after ingest")
        cur.execute("SELECT COUNT(*) FROM wac.authority_chain WHERE rcw_section_id IS NOT NULL")
        if cur.fetchone()[0] != EXPECTED_AUTH_CHAIN:
            raise HaltError("Post-flight: wac.authority_chain orthogonality drift")

    log(f"  ✓ post-flight: articles={ac}, sections={sc}, xrefs={xc}; corpora intact")


def main() -> int:
    log("=== Phase 2.4a ingest begin ===")
    log(f"DSN: {DSN.split('@')[-1] if '@' in DSN else DSN}")
    log(f"Source JSONL: {PARSED_JSONL}")

    articles_data: list[dict] = []
    with PARSED_JSONL.open() as f:
        for line in f:
            line = line.strip()
            if line:
                articles_data.append(json.loads(line))
    log(f"Loaded {len(articles_data)} articles from JSONL")

    if len(articles_data) != EXPECTED_ARTICLES:
        log(f"!! HALT: parsed article count {len(articles_data)} != {EXPECTED_ARTICLES}")
        return 2

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        preflight(conn)
        log("--- Inserting articles + sections + cross-refs ---")
        articles_inserted = 0
        sections_inserted = 0
        xrefs_inserted = 0
        for art in articles_data:
            art_uuid = insert_article(conn, art)
            articles_inserted += 1
            for sec in art['sections']:
                sec_uuid = insert_section(conn, art, art_uuid, sec)
                sections_inserted += 1
                xc = insert_cross_refs(conn, sec_uuid, sec)
                xrefs_inserted += xc
        log(f"  inserted: articles={articles_inserted}, sections={sections_inserted}, xrefs={xrefs_inserted}")

        postflight(conn, articles_inserted, sections_inserted, xrefs_inserted)
        conn.commit()
        log("=== Phase 2.4a ingest commit ===")
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
