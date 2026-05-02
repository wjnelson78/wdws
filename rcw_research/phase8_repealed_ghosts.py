"""
RCW Phase 8 — Repealed-Section Ghost-Row Materialization (Tier C, all 7,284)

Pipeline:
  Stage 1 (Acquisition): Fetch dispo.aspx for ~846 distinct chapters with
    repealed-target citations. Disk-cache to /tmp/rcw_dispo_cache_phase8/.
    Halt if per-chapter fetch failure rate >5%.
  Stage 2 (Parse + Plan): Use existing parse_disposition_html (rcw_parser).
    Filter entries to those whose canonical_citation matches our 7,284
    unresolved set. Build ghost-row tuples.
    Halt if parse extraction <95% of expected.
  Stage 3 (Insert): Single transaction.
    a) INSERT rcw.titles for 7 missing titles (28, 29, 30, 36A, 56, 62, 75).
       Additive only (ON CONFLICT (title_number) DO NOTHING).
    b) For each ghost: create core.documents row + rcw.sections ghost row.
       Idempotent (ON CONFLICT (canonical_citation) DO NOTHING).
    Halt on any CHECK constraint violation; halt if final count drift >10%.
  Stage 4 (FK re-run): Invoke psql -f phase4_4_fk_resolution_rerun.sql.

No schema changes. No external dependencies beyond leg.wa.gov dispo.aspx.
Wall time projection: ~35 min total (14 min HTTP + 14 min parse + 6 min
insert + 7 sec FK re-run).

Halt-band thresholds:
  - Acquisition success rate ≥95%
  - Parse extraction rate ≥95% of target 7,284
  - INSERT success rate 100%
  - Volume of ghost rows ±10% of 7,284 (≥6,556 and ≤8,012)
  - Phase 4.4 re-run delta in [5,000, 8,000]
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import http.client
import re
import urllib.request
import urllib.error
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras as extras

sys.path.insert(0, "/opt/wdws/rcw_research")
from rcw_parser import parse_disposition_html  # uses lxml

ROOT = Path("/opt/wdws/rcw_research")
CACHE_DIR = Path("/tmp/rcw_dispo_cache_phase8")
CACHE_DIR.mkdir(exist_ok=True)
LOG = ROOT / "phase8_repealed_ghosts.log"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
USER_AGENT = "Athena-RCW-Phase8/1.0 (research; will@nelson; pro se litigant assistive tech)"
RATE_LIMIT_SEC = 1.0  # 1 request/sec; same throttle as RCW Phase 2
PUBLICATION_DATE = date(2025, 8, 15)  # matches existing ghost rows

# Acquisition thresholds (Path B recalibration 2026-05-01 per §K #37)
ACQ_FAILURE_HALT_PCT = 5.0
PARSE_EXTRACT_FLOOR_PCT = 85.0  # was 95.0; lowered for Class 2 absence-of-record tail
GHOST_VOLUME_FLOOR = 6650  # Path B band [6,650, 7,150]
GHOST_VOLUME_CEIL = 7150
PATTERN1_FLOOR = 186   # ±10% of actual 207 (recalibrated 2026-05-01 per Stage-2 finding;
PATTERN1_CEIL = 228    # missing-title chapters mostly have residual dispo, only 207 are empty)
PATTERN2_FLOOR = 467   # ±10% of actual 519 (recalibrated; absence-of-record bucket
PATTERN2_CEIL = 571    # is larger than initial estimate)
EXPECTED_DISTINCT_GHOSTS = 7284
PATTERN2_RESIDUAL_PATH = ROOT / "phase8_pattern2_residuals.json"

MISSING_TITLES = {
    "28": "[Repealed Title 28] Recodified to Title 28A",
    "29": "[Repealed Title 29] Recodified to Title 29A",
    "30": "[Repealed Title 30] Recodified to Title 30A",
    "36A": "[Repealed Title 36A] Repealed/recodified",
    "56": "[Repealed Title 56] Repealed/recodified",
    "62": "[Repealed Title 62] Recodified to Title 62A",
    "75": "[Repealed Title 75] Recodified to Title 77",
}


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

def log(msg: str):
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


# -----------------------------------------------------------------------------
# Stage 0: Discover scope
# -----------------------------------------------------------------------------

UNRESOLVED_SECTION_REGEX = (
    r"^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?\.[0-9]+[A-Z]?$"
    r"|^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?-[0-9]+[A-Z]?$"
)

DISCOVER_SQL = """
SELECT DISTINCT
  regexp_replace(x.target_citation, '^RCW ([0-9]+[A-Z]?\\.[0-9]+[A-Z]?)[\\.\\-].*$', '\\1') AS chapter_number,
  x.target_citation
FROM rcw.cross_references x
LEFT JOIN rcw.sections s ON s.canonical_citation = x.target_citation
WHERE x.target_jurisdiction = 'state-wa-rcw'
  AND x.target_section_id IS NULL
  AND s.id IS NULL
  AND (x.target_citation ~ %s OR x.target_citation ~ %s)
"""

CHAPTER_VALIDATE_RE = r"^[0-9]+[A-Z]?\.[0-9]+[A-Z]?$"


def discover_scope(conn) -> tuple[set[str], set[str]]:
    """Return (chapters_set, repealed_citations_set)."""
    section_dot = r"^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?\.[0-9]+[A-Z]?$"
    section_dash = r"^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?-[0-9]+[A-Z]?$"
    with conn.cursor() as cur:
        cur.execute(DISCOVER_SQL, (section_dot, section_dash))
        rows = cur.fetchall()
    chapters = {r[0] for r in rows}
    citations = {r[1] for r in rows}
    log(f"Stage 0: discovered {len(chapters)} distinct chapters, {len(citations)} distinct repealed citations")
    return chapters, citations


# -----------------------------------------------------------------------------
# Stage 1: Acquisition
# -----------------------------------------------------------------------------

def cache_path(chapter: str) -> Path:
    safe = chapter.replace("/", "_")
    return CACHE_DIR / f"dispo_{safe}.html"


def fetch_chapter_dispo(chapter: str) -> tuple[bool, int]:
    """Fetch dispo.aspx for a chapter; cache on disk. Return (success, bytes)."""
    cache = cache_path(chapter)
    if cache.exists() and cache.stat().st_size > 1000:
        return True, cache.stat().st_size

    if not re.match(CHAPTER_VALIDATE_RE, chapter):
        log(f"  FETCH-SKIP {chapter}: not a valid chapter form (skipping)")
        return False, 0

    url = f"https://app.leg.wa.gov/RCW/dispo.aspx?cite={chapter}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            data = r.read()
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError,
            http.client.HTTPException, ConnectionError, OSError) as e:
        log(f"  FETCH-FAIL {chapter}: {type(e).__name__}: {e}")
        return False, 0

    if len(data) < 1000:
        log(f"  FETCH-FAIL {chapter}: response too small ({len(data)} bytes)")
        return False, len(data)

    cache.write_bytes(data)
    time.sleep(RATE_LIMIT_SEC)
    return True, len(data)


def acquire_all(chapters: set[str]) -> tuple[int, int]:
    log(f"Stage 1: acquiring dispo.aspx for {len(chapters)} chapters")
    ok = 0
    fail = 0
    for i, chapter in enumerate(sorted(chapters), 1):
        success, _ = fetch_chapter_dispo(chapter)
        if success:
            ok += 1
        else:
            fail += 1
        if i % 100 == 0:
            log(f"  acquired {i}/{len(chapters)} (ok={ok}, fail={fail})")
    fail_pct = 100.0 * fail / len(chapters) if chapters else 0
    log(f"Stage 1 done: ok={ok}, fail={fail} ({fail_pct:.1f}%)")
    if fail_pct > ACQ_FAILURE_HALT_PCT:
        raise RuntimeError(f"HALT: acquisition failure rate {fail_pct:.1f}% > {ACQ_FAILURE_HALT_PCT}%")
    return ok, fail


# -----------------------------------------------------------------------------
# Stage 2: Parse + plan
# -----------------------------------------------------------------------------

def parse_all_dispos(chapters: set[str], target_citations: set[str]) -> list[dict]:
    """For each cached dispo HTML, extract ghost-row tuples for citations
    in target_citations set. Path B (per §K #37):
      - Pattern 1 (citation in 7 missing titles): synthesize stub ghost
        with placeholder heading + is_repealed=TRUE.
      - Pattern 2 (citation in present chapter, but section number absent
        from dispo entries): preserve as residual NULL; record for report.
    Return list of ghost-row dicts; pattern2_residuals saved to JSON."""
    log(f"Stage 2: parsing {len(chapters)} dispo HTMLs; targeting {len(target_citations)} citations")
    plan: list[dict] = []
    pattern2_residuals: list[dict] = []
    pattern1_stubs = 0
    parse_failures = 0
    not_found = 0
    missing_title_keys = set(MISSING_TITLES.keys())  # {"28","29","30","36A","56","62","75"}

    for chapter in sorted(chapters):
        cache = cache_path(chapter)
        if not cache.exists():
            continue
        try:
            rec = parse_disposition_html(cache)
        except Exception as e:
            log(f"  PARSE-FAIL {chapter}: {type(e).__name__}: {e}")
            parse_failures += 1
            continue

        by_cite = {e.canonical_citation: e for e in rec.entries}
        chapter_targets = {c for c in target_citations
                           if c.startswith(f"RCW {chapter}.") or c.startswith(f"RCW {chapter}-")}
        title_number = chapter.split(".")[0]

        for cite in chapter_targets:
            entry = by_cite.get(cite)
            if entry:
                section_number = cite.replace(f"RCW {chapter}.", "").replace(f"RCW {chapter}-", "")
                plan.append({
                    "canonical_citation": cite,
                    "title_number": title_number,
                    "chapter_number": chapter,
                    "section_number": section_number,
                    "section_heading": entry.section_caption[:300] if entry.section_caption else f"[Repealed] (former {cite})",
                    "is_repealed": entry.is_repealed,
                    "is_recodified": entry.is_recodified,
                    "is_decodified": entry.is_decodified,
                    "is_reserved": entry.is_reserved,
                    "recodified_to": entry.recodified_to,
                    "raw_disposition_text": entry.raw_disposition_text[:1000],
                    "_pattern": "0_dispo_entry_found",
                })
            elif title_number in missing_title_keys:
                # Pattern 1: missing-title chapter — synthesize stub ghost
                section_number = cite.replace(f"RCW {chapter}.", "").replace(f"RCW {chapter}-", "")
                plan.append({
                    "canonical_citation": cite,
                    "title_number": title_number,
                    "chapter_number": chapter,
                    "section_number": section_number,
                    "section_heading": f"[Repealed] (former {cite})",
                    "is_repealed": True,
                    "is_recodified": False,
                    "is_decodified": False,
                    "is_reserved": False,
                    "recodified_to": None,
                    "raw_disposition_text": f"[Pattern 1 stub: title {title_number} fully repealed; dispo data unavailable]",
                    "_pattern": "1_missing_title_stub",
                })
                pattern1_stubs += 1
            else:
                # Pattern 2: residual — section number absent from a present chapter's dispo
                pattern2_residuals.append({
                    "canonical_citation": cite,
                    "chapter": chapter,
                    "title": title_number,
                    "parser_entries_in_chapter": len(by_cite),
                    "reason": "non-existent section in present chapter (preserved as NULL per §K #37)",
                })
                not_found += 1

    # Persist Pattern 2 residual list for the cumulative report
    PATTERN2_RESIDUAL_PATH.write_text(json.dumps(pattern2_residuals, indent=2))
    log(f"Stage 2 done: planned={len(plan)} (pattern1_stubs={pattern1_stubs}), "
        f"parse_failures={parse_failures}, pattern2_residuals={len(pattern2_residuals)}")

    # Halt-band checks (Path B)
    extract_pct = 100.0 * len(plan) / EXPECTED_DISTINCT_GHOSTS
    log(f"  extraction rate: {extract_pct:.1f}% of expected {EXPECTED_DISTINCT_GHOSTS}")
    if extract_pct < PARSE_EXTRACT_FLOOR_PCT:
        raise RuntimeError(
            f"HALT: extraction rate {extract_pct:.1f}% below floor {PARSE_EXTRACT_FLOOR_PCT}%"
        )
    if not (PATTERN1_FLOOR <= pattern1_stubs <= PATTERN1_CEIL):
        raise RuntimeError(
            f"HALT: Pattern 1 stub count {pattern1_stubs} outside band [{PATTERN1_FLOOR}, {PATTERN1_CEIL}]"
        )
    if not (PATTERN2_FLOOR <= len(pattern2_residuals) <= PATTERN2_CEIL):
        raise RuntimeError(
            f"HALT: Pattern 2 residual count {len(pattern2_residuals)} outside band "
            f"[{PATTERN2_FLOOR}, {PATTERN2_CEIL}]"
        )
    return plan


# -----------------------------------------------------------------------------
# Stage 3: Insert (single transaction)
# -----------------------------------------------------------------------------

def insert_all(plan: list[dict]) -> tuple[int, int, int]:
    """Insert missing titles, per-ghost documents, and ghost section rows.
    Returns (titles_added, docs_added, sections_added)."""
    log(f"Stage 3: inserting {len(plan)} ghost rows in single transaction")
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    titles_added = 0
    docs_added = 0
    sections_added = 0
    try:
        with conn.cursor() as cur:
            # Stage 3a: missing titles
            for tnum, tname in MISSING_TITLES.items():
                cur.execute("""
                    INSERT INTO rcw.titles (title_number, title_name, publication_date, notes)
                    VALUES (%s, %s, %s, 'Phase 8 (2026-05-01): synthesized for repealed-target FK navigation')
                    ON CONFLICT (title_number) DO NOTHING
                """, (tnum, tname, PUBLICATION_DATE))
                titles_added += cur.rowcount
            log(f"  3a: titles added: {titles_added}")

            # Stage 3b: per-ghost documents + sections
            for g in plan:
                source_path = f"rcw:{PUBLICATION_DATE.isoformat()}:t{g['title_number']}:c{g['chapter_number']}:s{g['section_number']}:ghost"
                doc_title = f"{g['canonical_citation']} — {g['section_heading']}"[:500]
                # core.documents has no UNIQUE other than PK; SELECT-then-INSERT for idempotency
                cur.execute("SELECT id FROM core.documents WHERE source_path = %s LIMIT 1",
                            (source_path,))
                r2 = cur.fetchone()
                if r2:
                    doc_id = r2[0]
                else:
                    cur.execute("""
                        INSERT INTO core.documents (domain, document_type, title, source_path, metadata)
                        VALUES ('legal', 'statute', %s, %s,
                                jsonb_build_object('phase', 8, 'ghost', true,
                                                   'is_repealed', %s, 'is_recodified', %s,
                                                   'recodified_to', %s,
                                                   'raw_disposition', %s))
                        RETURNING id
                    """, (
                        doc_title, source_path,
                        g["is_repealed"], g["is_recodified"],
                        g.get("recodified_to"),
                        g.get("raw_disposition_text"),
                    ))
                    row = cur.fetchone()
                    if not row:
                        log(f"  WARN: doc insert returned no row for {source_path}; skipping section")
                        continue
                    doc_id = row[0]
                    docs_added += 1

                cur.execute("""
                    SELECT id FROM rcw.chapters WHERE chapter_number = %s LIMIT 1
                """, (g["chapter_number"],))
                cr = cur.fetchone()
                chapter_id = cr[0] if cr else None

                # Determine flag set: at least one must be TRUE per chk_section_text_status_invariant
                flags_any = g["is_repealed"] or g["is_recodified"] or g["is_decodified"] or g["is_reserved"]
                if not flags_any:
                    # default to is_repealed=TRUE since these came from the
                    # repealed-and-removed pool
                    g["is_repealed"] = True

                cur.execute("""
                    INSERT INTO rcw.sections (
                        document_id, title_number, chapter_id,
                        section_number, canonical_citation, section_heading,
                        is_repealed, is_recodified, is_decodified, is_reserved,
                        recodified_to, full_text, publication_date, unit_type
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, NULL, %s, 'section'
                    )
                    ON CONFLICT (canonical_citation, publication_date) DO NOTHING
                """, (
                    doc_id, g["title_number"], chapter_id,
                    g["section_number"], g["canonical_citation"], g["section_heading"],
                    g["is_repealed"], g["is_recodified"], g["is_decodified"], g["is_reserved"],
                    g.get("recodified_to"),
                    PUBLICATION_DATE,
                ))
                if cur.rowcount > 0:
                    sections_added += 1

        conn.commit()
        log(f"Stage 3 done: titles_added={titles_added}, docs_added={docs_added}, sections_added={sections_added}")

        if sections_added < GHOST_VOLUME_FLOOR or sections_added > GHOST_VOLUME_CEIL:
            raise RuntimeError(
                f"HALT: ghost rows {sections_added} outside band [{GHOST_VOLUME_FLOOR}, {GHOST_VOLUME_CEIL}]"
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return titles_added, docs_added, sections_added


# -----------------------------------------------------------------------------
# Stage 4: FK re-run via psql
# -----------------------------------------------------------------------------

def run_fk_rerun() -> int:
    log("Stage 4: invoking phase4_4_fk_resolution_rerun.sql")
    rerun_sql = ROOT / "phase4_4_fk_resolution_rerun.sql"
    env = os.environ.copy()
    env["PGPASSWORD"] = "NEL2233obs"
    cmd = ["psql", "-h", "127.0.0.1", "-U", "wdws", "-d", "wdws",
           "-v", "ON_ERROR_STOP=1", "-f", str(rerun_sql)]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    log(f"  exit={p.returncode}")
    if p.stdout:
        for line in p.stdout.splitlines():
            log(f"  [psql] {line}")
    if p.stderr:
        for line in p.stderr.splitlines():
            log(f"  [psql-stderr] {line}")
    return p.returncode


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    log("=== Phase 8 — Repealed-Section Ghost-Row Materialization (Tier C) ===")
    t0 = time.time()
    conn = psycopg2.connect(DSN); conn.autocommit = True
    chapters, citations = discover_scope(conn)
    conn.close()
    # Pre-flight discovery drift check: ±10% of EXPECTED_DISTINCT_GHOSTS
    discovery_floor = int(EXPECTED_DISTINCT_GHOSTS * 0.9)  # 6556
    discovery_ceil = int(EXPECTED_DISTINCT_GHOSTS * 1.1)   # 8012
    if not (discovery_floor <= len(citations) <= discovery_ceil):
        raise RuntimeError(
            f"HALT pre-flight: discovered {len(citations)} citations outside band "
            f"[{discovery_floor}, {discovery_ceil}] (expected {EXPECTED_DISTINCT_GHOSTS})"
        )

    ok, fail = acquire_all(chapters)
    plan = parse_all_dispos(chapters, citations)
    titles_added, docs_added, sections_added = insert_all(plan)
    rc = run_fk_rerun()
    if rc != 0:
        raise RuntimeError(f"Phase 4.4 re-run exited {rc}")

    elapsed_min = (time.time() - t0) / 60
    log(f"=== Phase 8 COMPLETE === elapsed={elapsed_min:.1f}min "
        f"acquire={ok}/{ok+fail} ghosts={sections_added} docs={docs_added} titles={titles_added}")


if __name__ == "__main__":
    main()
