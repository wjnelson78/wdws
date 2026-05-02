"""
USC Ingestion Driver — Phase 4.

Two-stage ingest of one USC title:

  Stage 1 (fast, ~1 min for T42):
    parse → INSERT usc.titles/chapters/subchapters/sections + core.documents +
    section_history/notes + core.document_tags + chunks (without embedding).
    Fulltext retrieval becomes live as soon as Stage 1 finishes.

  Stage 2 (slow, ~hours for T42):
    Embed all chunks in BGE-M3 batches of 64; UPDATE core.document_chunks.
    Semantic retrieval comes online incrementally as batches complete.

Usage:
    python3 usc_ingest.py --title 42 --xml /opt/wdws/data/usc/extracted/usc42.xml
    python3 usc_ingest.py --title 28 --xml ...usc28a.xml --appendix
"""
from __future__ import annotations

import argparse, json, hashlib, os, sys, time, uuid
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
sys.path.insert(0, "/opt/wdws/usc_research")

import psycopg2, psycopg2.extras
from embedding_service import embed_texts_sync, EMBEDDING_DIMENSIONS

from usc_parser import (
    parse_title_xml, title_meta_from_xml, ParsedUnit, POSITIVE_LAW_TITLES,
)

assert EMBEDDING_DIMENSIONS == 1024

DSN = os.environ.get("DATABASE_URL",
                    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
EMBEDDING_MODEL_ID = 2   # BAAI/bge-m3 active model

# Chunking thresholds. 6000 chars ≈ 1500 tokens, leaving headroom under
# BGE-M3's 8192-token cap even after breadcrumb prefixes.
CHUNK_TARGET_CHARS = 6000
CHUNK_MIN_CHARS    = 200    # don't emit silly tiny chunks
EMBED_BATCH_SIZE   = 64

# Title metadata for the litigation-priority titles. Phase 4 will pull
# title_name from the XML <meta>, but we override with hand-curated official
# names where helpful.
TITLE_NAMES = {
    1:  "General Provisions",
    2:  "The Congress",
    3:  "The President",
    4:  "Flag and Seal, Seat of Government, and the States",
    5:  "Government Organization and Employees",
    6:  "Domestic Security",
    7:  "Agriculture",
    8:  "Aliens and Nationality",
    9:  "Arbitration",
    10: "Armed Forces",
    11: "Bankruptcy",
    12: "Banks and Banking",
    13: "Census",
    14: "Coast Guard",
    15: "Commerce and Trade",
    16: "Conservation",
    17: "Copyrights",
    18: "Crimes and Criminal Procedure",
    19: "Customs Duties",
    20: "Education",
    21: "Food and Drugs",
    22: "Foreign Relations and Intercourse",
    23: "Highways",
    24: "Hospitals and Asylums",
    25: "Indians",
    26: "Internal Revenue Code",
    27: "Intoxicating Liquors",
    28: "Judiciary and Judicial Procedure",
    29: "Labor",
    30: "Mineral Lands and Mining",
    31: "Money and Finance",
    32: "National Guard",
    33: "Navigation and Navigable Waters",
    34: "Crime Control and Law Enforcement",
    35: "Patents",
    36: "Patriotic and National Observances, Ceremonies, and Organizations",
    37: "Pay and Allowances of the Uniformed Services",
    38: "Veterans' Benefits",
    39: "Postal Service",
    40: "Public Buildings, Property, and Works",
    41: "Public Contracts",
    42: "The Public Health and Welfare",
    43: "Public Lands",
    44: "Public Printing and Documents",
    45: "Railroads",
    46: "Shipping",
    47: "Telecommunications",
    48: "Territories and Insular Possessions",
    49: "Transportation",
    50: "War and National Defense",
    51: "National and Commercial Space Programs",
    52: "Voting and Elections",
    54: "National Park Service and Related Programs",
}

# Chapter-level definitive attribution. Statutes don't carry their own popular
# names in the codified text — § 794's heading is "Nondiscrimination under
# Federal grants and programs," not "Rehabilitation Act § 504". Tag attribution
# by (title, chapter) is the only deterministic way to attach the popular-name
# tag to every section in the act, regardless of whether the section's text
# happens to mention the act by name.
#
# Format: (title_number:int, chapter_number:str) → list[tag_slug]
# Slug "*" matches any chapter in that title (rare; reserved for whole-title acts).
CHAPTER_TAG_MAP: dict[tuple[int, str], list[str]] = {
    (5,  "5"):   ["apa"],                              # Administrative Procedure (FOIA, Privacy Act, APA core)
    (5,  "7"):   ["apa", "judicial-review"],           # Judicial Review of Agency Action (5 USC §§ 701-706)
    (29, "16"):  ["rehab-act"],                        # Vocational Rehabilitation
    (29, "18"):  ["erisa"],                            # ERISA
    (29, "8"):   ["flsa"],                             # Fair Labor Standards
    (29, "15"):  ["osha"],                             # Occupational Safety and Health
    (42, "126"): ["ada"],                              # ADA
    (42, "21"):  ["civil-rights"],                     # Civil Rights (§ 1983 lives here)
    (42, "7"):   ["social-security", "medicare", "medicaid"],  # Social Security Act
    (18, "13"):  ["civil-rights", "criminal-procedure"],   # Criminal Civil Rights (§§ 241-249) — §242 is criminal counterpart of §1983
    (28, "85"):  ["jurisdiction-fed"],                 # District Courts; Jurisdiction
    (28, "89"):  ["removal", "jurisdiction-fed"],      # District Courts; Removal of Cases
    (11, "*"):   ["bankruptcy"],                       # entire Title 11
    (26, "*"):   ["irc"],                              # entire Title 26
    (38, "*"):   ["veterans"],                         # entire Title 38
}

# Topical-tag keyword map — applies to (chapter_heading + section_heading +
# full_text). Lower-cased substring match. Multi-tag. Operates as ADDITIVE
# augmentation to CHAPTER_TAG_MAP (a section gets BOTH the chapter-map tags
# AND any text-search hits — they compose).
TOPICAL_TAG_RULES = [
    # ADA / disability
    ("disability",                              ["ada", "civil-rights"]),
    ("discrimination",                          ["civil-rights"]),
    ("americans with disabilities",             ["ada", "ada-title-ii"]),
    ("equal opportunity for individuals",       ["ada"]),
    ("rehabilitation act",                      ["rehab-act"]),
    # Civil rights / § 1983 / due process
    ("under color of",                          ["section-1983", "civil-rights"]),
    ("section 1983",                            ["section-1983"]),
    ("equal protection",                        ["equal-protection"]),
    ("due process",                             ["due-process"]),
    ("voting rights",                           ["civil-rights"]),
    ("fair housing",                            ["civil-rights"]),
    # FRCP/FRE/FRAP/FRCrP/FRBP — by file mapping
    # Bankruptcy
    ("bankruptcy",                              ["bankruptcy"]),
    ("debtor",                                  ["bankruptcy"]),
    # Criminal
    ("criminal",                                ["criminal-procedure"]),
    ("sentencing",                              ["criminal-procedure"]),
    # Tax / IRC
    ("internal revenue",                        ["irc"]),
    ("taxable income",                          ["irc"]),
    # Labor
    ("employee retirement income security",     ["erisa"]),
    ("fair labor standards",                    ["flsa"]),
    ("occupational safety",                     ["osha"]),
    # APA / judicial review
    ("administrative procedure",                ["apa", "judicial-review"]),
    ("judicial review",                         ["judicial-review"]),
    ("agency action",                           ["apa", "judicial-review"]),
    # Healthcare
    ("medicare",                                ["medicare"]),
    ("medicaid",                                ["medicaid"]),
    ("social security",                         ["social-security"]),
    # Veterans
    ("veterans",                                ["veterans"]),
    # Jurisdiction / removal
    ("removal of cases",                        ["removal", "jurisdiction-fed"]),
    ("federal question",                        ["jurisdiction-fed"]),
    ("diversity jurisdiction",                  ["jurisdiction-fed"]),
    ("full faith and credit",                   ["full-faith-credit"]),
    # Qualified immunity
    ("qualified immunity",                      ["qualified-immunity"]),
]


# ────────────────────────────────────────────────────────────────────────────
# Chunker
# ────────────────────────────────────────────────────────────────────────────

def chunk_unit_text(unit: ParsedUnit, *, breadcrumb: str) -> list[str]:
    """Split a unit's markdown into BGE-M3-sized chunks, each prefixed with
    the breadcrumb + section heading so retrieval surfaces citation context.

    Algorithm:
      1. If full_text fits, single chunk = breadcrumb + full_text.
      2. Else split on '## ' (subsection headers); attach breadcrumb to each.
      3. If a subsection chunk is still too big, split further on '### '.
      4. Greedy-merge adjacent small splits so we don't burn embedding budget
         on tiny fragments.
    """
    full = unit.full_text
    head_line = full.splitlines()[0] if full else ""
    full_with_breadcrumb = f"{breadcrumb}\n\n{full}"
    if len(full_with_breadcrumb) <= CHUNK_TARGET_CHARS:
        return [full_with_breadcrumb]

    # Split by subsection (## ) — keep the header attached to its body
    parts = _split_by_marker(full, marker="\n## ")
    if len(parts) <= 1:
        # No subsection structure — fall through to paragraph-level
        parts = _split_by_marker(full, marker="\n### ")
    if len(parts) <= 1:
        # Hard fallback — slice every CHUNK_TARGET_CHARS - len(breadcrumb)
        return _slice_long_text(full_with_breadcrumb,
                                target=CHUNK_TARGET_CHARS,
                                head=head_line, breadcrumb=breadcrumb)

    # Greedy-merge tiny adjacent parts back together
    merged: list[str] = []
    buf = ""
    for p in parts:
        if len(buf) + len(p) + 2 <= CHUNK_TARGET_CHARS:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                merged.append(buf)
            if len(p) > CHUNK_TARGET_CHARS:
                # Recursively split this overlong part
                sub = _slice_long_text(p, target=CHUNK_TARGET_CHARS,
                                        head=head_line, breadcrumb="")
                merged.extend(sub)
                buf = ""
            else:
                buf = p
    if buf:
        merged.append(buf)

    # Prepend breadcrumb + heading to each chunk
    out = []
    for chunk in merged:
        if chunk.startswith("# "):
            # First chunk already begins with section header; just prepend breadcrumb
            out.append(f"{breadcrumb}\n\n{chunk}")
        else:
            out.append(f"{breadcrumb}\n\n{head_line}\n\n{chunk}")
    return out


def _split_by_marker(text: str, *, marker: str) -> list[str]:
    """Split keeping marker on the right-hand side."""
    if marker not in text:
        return [text]
    pieces = text.split(marker)
    out = [pieces[0]] if pieces[0].strip() else []
    for p in pieces[1:]:
        out.append(marker.lstrip("\n") + p)
    return [p for p in out if p.strip()]


def _slice_long_text(text: str, *, target: int, head: str, breadcrumb: str) -> list[str]:
    """Hard slice with sentence-boundary preference. breadcrumb may be empty."""
    if breadcrumb and not text.startswith(breadcrumb):
        text = f"{breadcrumb}\n\n{head}\n\n{text}"
    out = []
    i = 0
    while i < len(text):
        end = min(i + target, len(text))
        # Prefer to break at \n\n
        cut = text.rfind("\n\n", i + max(target // 2, 200), end)
        if cut <= i:
            cut = end
        out.append(text[i:cut].strip())
        i = cut
    return [c for c in out if c.strip()]


# ────────────────────────────────────────────────────────────────────────────
# Tagging
# ────────────────────────────────────────────────────────────────────────────

def topical_tags_for(unit: ParsedUnit) -> list[str]:
    """Compute topical tags for one unit.

    Two attribution paths, both ADDITIVE:
      (1) CHAPTER_TAG_MAP — definitive (title, chapter) → tag mapping.
          Catches sections like § 794 where the popular name doesn't appear
          in the section text at all.
      (2) Text search over heading + chapter heading + full_text. Catches
          sections that mention an act in passing (cross-references, notes).

    Text-search scope is full_text, not first-500-chars — popular names can
    appear anywhere in the body (e.g., § 701 has 'Rehabilitation Act' deeper
    in the text). Performance cost is trivial; tagging runs once per section
    at ingest.
    """
    matched: set[str] = set()

    # (1) Chapter-map definitive attribution
    if unit.chapter_number:
        cn = unit.chapter_number
        for tags in (CHAPTER_TAG_MAP.get((unit.title_number, cn), []) +
                     CHAPTER_TAG_MAP.get((unit.title_number, "*"), [])):
            matched.add(tags) if isinstance(tags, str) else matched.update(tags)
        # ^ defensive: list-of-str above. Below the simpler form:
    matched.clear()
    if unit.chapter_number:
        for key in ((unit.title_number, unit.chapter_number),
                    (unit.title_number, "*")):
            for tag in CHAPTER_TAG_MAP.get(key, []):
                matched.add(tag)

    # (2) Text-search — full_text scope, not truncated
    haystack = " ".join(filter(None, [
        (unit.section_heading or "").lower(),
        (unit.chapter_heading or "").lower(),
        (unit.full_text or "").lower(),
    ]))
    for needle, tags in TOPICAL_TAG_RULES:
        if needle in haystack:
            matched.update(tags)

    return sorted(matched)


def base_tags_for(unit: ParsedUnit, *, is_positive_law: bool) -> list[str]:
    base = ["usc", "federal-statute", f"title-{unit.title_number}"]
    base.append("usc-positive-law" if is_positive_law else "usc-non-positive")
    if unit.unit_type == "section" and unit.full_text and "App. §" in unit.canonical_citation:
        base.append("usc-appendix")
    if unit.unit_type == "court_rule":
        base.extend(["usc-court-rule", "usc-appendix"])
        rule_to_tag = {"FRCP":"frcp","FRAP":"frap","FRE":"fre","FRCrP":"frcrp","FRBP":"frbp"}
        if unit.rule_set in rule_to_tag:
            base.append(rule_to_tag[unit.rule_set])
    if unit.unit_type == "reorganization_plan":
        base.extend(["usc-reorganization-plan", "usc-appendix"])
    if unit.is_repealed:    base.append("usc-repealed")
    if unit.is_transferred: base.append("usc-transferred")
    if unit.is_omitted:     base.append("usc-omitted")
    return base


# ────────────────────────────────────────────────────────────────────────────
# DB helpers
# ────────────────────────────────────────────────────────────────────────────

@contextmanager
def db_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()


def load_tag_id_cache(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT slug, id FROM core.tags WHERE slug IS NOT NULL")
        return dict(cur.fetchall())


def ensure_title_row(conn, *, title_number: int, release_point: str,
                    is_appendix: bool, has_appendix: bool, current_through: str,
                    title_name: str) -> None:
    is_positive = (title_number in POSITIVE_LAW_TITLES) and not is_appendix
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO usc.titles
                (title_number, title_name, is_positive_law, has_appendix,
                 release_point, current_through, last_ingested_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NULL)
            ON CONFLICT (title_number) DO UPDATE SET
                title_name       = EXCLUDED.title_name,
                is_positive_law  = EXCLUDED.is_positive_law,
                has_appendix     = EXCLUDED.has_appendix,
                release_point    = EXCLUDED.release_point,
                current_through  = EXCLUDED.current_through,
                last_ingested_at = NOW()
        """, (title_number, title_name, is_positive, has_appendix,
              release_point, current_through))


def ensure_chapter(conn, cache: dict, *, title_number: int,
                  chapter_number: str, chapter_heading: str) -> str:
    """Return chapter_id (UUID), creating row if needed. Cache by (title, num)."""
    if not chapter_number:
        return None
    key = (title_number, chapter_number)
    if key in cache:
        return cache[key]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO usc.chapters (title_number, chapter_number, chapter_heading, sort_order)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (title_number, chapter_number) DO UPDATE SET
                chapter_heading = EXCLUDED.chapter_heading
            RETURNING id
        """, (title_number, chapter_number, chapter_heading or "",
              _sort_order_for(chapter_number)))
        chapter_id = cur.fetchone()[0]
    cache[key] = chapter_id
    return chapter_id


def _sort_order_for(text_number: str) -> int:
    """Best-effort numeric ordering for chapter/subchapter sort_order."""
    import re as _re
    m = _re.match(r"(\d+)", text_number or "")
    return int(m.group(1)) if m else 0


def ensure_subchapter(conn, cache: dict, *, chapter_id: str,
                     subchapter_id_text: str, subchapter_heading: str) -> str:
    if not chapter_id or not subchapter_id_text:
        return None
    key = (chapter_id, subchapter_id_text)
    if key in cache:
        return cache[key]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO usc.subchapters (chapter_id, subchapter_id_text, subchapter_heading, sort_order)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chapter_id, subchapter_id_text) DO UPDATE SET
                subchapter_heading = EXCLUDED.subchapter_heading
            RETURNING id
        """, (chapter_id, subchapter_id_text, subchapter_heading or "",
              _sort_order_for(subchapter_id_text)))
        subchapter_id = cur.fetchone()[0]
    cache[key] = subchapter_id
    return subchapter_id


# ────────────────────────────────────────────────────────────────────────────
# Per-unit ingest
# ────────────────────────────────────────────────────────────────────────────

def ingest_unit(conn, *, unit: ParsedUnit, release_point: str,
                title_name: str, is_positive_law: bool,
                chapter_id_cache: dict, subchapter_id_cache: dict,
                tag_id_cache: dict[str, int],
                source_filename: str) -> tuple[str, str, int]:
    """Ingest one ParsedUnit. Returns (document_id, section_id_or_None, chunk_count)."""
    chapter_id = None
    subchapter_id = None
    if unit.unit_type == "section":
        chapter_id = ensure_chapter(conn, chapter_id_cache,
                                    title_number=unit.title_number,
                                    chapter_number=unit.chapter_number,
                                    chapter_heading=unit.chapter_heading)
        subchapter_id = ensure_subchapter(conn, subchapter_id_cache,
                                          chapter_id=chapter_id,
                                          subchapter_id_text=unit.subchapter_id_text,
                                          subchapter_heading=unit.subchapter_heading)

    rp_token = release_point.replace(" ", "_")
    if unit.unit_type == "section":
        source_path = f"usc:{rp_token}:t{unit.title_number}{'a' if 'App.' in unit.canonical_citation else ''}:s{unit.section_number}"
    elif unit.unit_type == "court_rule":
        source_path = f"usc:{rp_token}:rule:{unit.rule_set}:{unit.section_number}"
    else:  # reorganization_plan
        source_path = f"usc:{rp_token}:reorgplan:{unit.section_number.replace(' ', '_')}"

    title_str = f"{unit.canonical_citation} — {unit.section_heading or '(no heading)'}"
    metadata = {
        "type": f"usc_{unit.unit_type}",
        "title_number": unit.title_number,
        "title_name": title_name,
        "is_positive_law": is_positive_law,
        "chapter_number": unit.chapter_number,
        "chapter_heading": unit.chapter_heading,
        "subchapter": unit.subchapter_id_text,
        "section_number": unit.section_number,
        "section_heading": unit.section_heading,
        "canonical_citation": unit.canonical_citation,
        "release_point": release_point,
        "is_repealed": unit.is_repealed,
        "is_transferred": unit.is_transferred,
        "is_omitted": unit.is_omitted,
        "transferred_to": unit.transferred_to,
        "effective_date": unit.effective_date,
        "enacted_by_pl": unit.enacted_by_pl,
        "statutes_at_large_ref": unit.statutes_at_large_ref,
        "unit_type": unit.unit_type,
        "rule_set": unit.rule_set,
        "uslm_identifier": unit.uslm_identifier,
        "olrc_url": _olrc_url(unit),
        "source_file": source_filename,
    }
    content_hash = hashlib.sha256(unit.full_text.encode("utf-8")).hexdigest()
    filename = source_path.replace("usc:", "").replace(":", "-") + ".xml"

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO core.documents
                (domain, document_type, title, source_path, filename,
                 full_content, content_hash, metadata,
                 classification, confidentiality, legal_hold,
                 privilege, privilege_classified_at, privilege_classified_by)
            VALUES
                ('legal', 'statute', %s, %s, %s,
                 %s, %s, %s::jsonb,
                 'public', 'public', FALSE,
                 'none', NOW(), 'rule:statute_is_public')
            ON CONFLICT (source_path)
                WHERE document_type = 'statute'
                DO UPDATE SET
                    title        = EXCLUDED.title,
                    full_content = EXCLUDED.full_content,
                    metadata     = EXCLUDED.metadata,
                    content_hash = EXCLUDED.content_hash,
                    updated_at   = NOW()
            RETURNING id
        """, (title_str, source_path, filename, unit.full_text,
              content_hash, json.dumps(metadata)))
        document_id = cur.fetchone()[0]

        # Tags: base + topical
        slugs = base_tags_for(unit, is_positive_law=is_positive_law) + topical_tags_for(unit)
        tag_ids = [tag_id_cache[s] for s in slugs if s in tag_id_cache]
        if tag_ids:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_tags (document_id, tag_id) VALUES %s
                ON CONFLICT DO NOTHING
            """, [(document_id, tid) for tid in tag_ids])

        # usc.sections row (all unit types, including court_rule and reorg_plan,
        # land here — discriminated by unit_type column)
        cur.execute("""
            INSERT INTO usc.sections
                (document_id, title_number, chapter_id, subchapter_id,
                 section_number, section_heading, canonical_citation,
                 unit_type, rule_set,
                 is_repealed, is_transferred, is_omitted, transferred_to,
                 effective_date, enacted_by_pl, statutes_at_large_ref,
                 full_text, source_xml, release_point)
            VALUES (%s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s)
            ON CONFLICT (canonical_citation, release_point) DO UPDATE SET
                document_id = EXCLUDED.document_id,
                full_text   = EXCLUDED.full_text,
                source_xml  = EXCLUDED.source_xml,
                updated_at  = NOW()
            RETURNING id
        """, (document_id, unit.title_number, chapter_id, subchapter_id,
              unit.section_number, unit.section_heading, unit.canonical_citation,
              unit.unit_type, unit.rule_set,
              unit.is_repealed, unit.is_transferred, unit.is_omitted, unit.transferred_to,
              unit.effective_date, unit.enacted_by_pl, unit.statutes_at_large_ref,
              unit.full_text, unit.source_xml, release_point))
        section_id = cur.fetchone()[0]

        # History
        cur.execute("DELETE FROM usc.section_history WHERE section_id = %s", (section_id,))
        if unit.history:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO usc.section_history
                    (section_id, pub_law, pub_law_date, statutes_ref,
                     action_type, action_summary, sort_order)
                VALUES %s
            """, [(section_id, h.pub_law, h.pub_law_date, h.statutes_ref,
                   h.action_type, h.action_summary, h.sort_order) for h in unit.history])

        # Notes
        cur.execute("DELETE FROM usc.section_notes WHERE section_id = %s", (section_id,))
        if unit.notes:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO usc.section_notes
                    (section_id, note_type, note_topic, note_heading,
                     note_text, sort_order)
                VALUES %s
            """, [(section_id, n.note_type, n.note_topic, n.note_heading,
                   n.note_text, n.sort_order) for n in unit.notes])

        # Cross references
        cur.execute("DELETE FROM usc.cross_references WHERE source_section_id = %s", (section_id,))
        if unit.cross_refs:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO usc.cross_references
                    (source_section_id, target_citation, target_section_id,
                     context_snippet, ref_type, uslm_href)
                VALUES %s
            """, [(section_id, x.target_citation, None,  # target_section_id resolved in Phase 4.4
                   x.context_snippet, x.ref_type, x.uslm_href) for x in unit.cross_refs])

        # Chunks (without embeddings)
        breadcrumb = _breadcrumb_for(unit)
        chunks = chunk_unit_text(unit, breadcrumb=breadcrumb)
        # First, clear old chunks for this document if any (idempotent re-run)
        cur.execute("DELETE FROM core.document_chunks WHERE document_id = %s", (document_id,))
        chunk_rows = []
        for i, ctext in enumerate(chunks):
            chunk_id = f"{document_id}:{i}"
            chunk_rows.append((
                chunk_id, document_id, i, len(chunks), ctext,
                EMBEDDING_MODEL_ID, len(ctext.split()),
                ctext,  # embedded_content
            ))
        if chunk_rows:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks, content,
                     embedding_model_id, token_count, embedded_content)
                VALUES %s
            """, chunk_rows)

        cur.execute("UPDATE core.documents SET total_chunks = %s WHERE id = %s",
                    (len(chunks), document_id))

    return document_id, section_id, len(chunks)


def _olrc_url(unit: ParsedUnit) -> str:
    if unit.unit_type == "section":
        return (f"https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-"
                f"title{unit.title_number}-section{unit.section_number}")
    if unit.unit_type == "court_rule":
        return f"https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title{unit.title_number}-courtRules"
    return f"https://uscode.house.gov/view.xhtml?req=granuleid:USC-prelim-title{unit.title_number}-appendix"


def _breadcrumb_for(unit: ParsedUnit) -> str:
    parts = [f"Title {unit.title_number}"]
    if unit.chapter_number:
        parts.append(f"Chapter {unit.chapter_number}")
    if unit.subchapter_id_text:
        parts.append(f"Subchapter {unit.subchapter_id_text}")
    if unit.unit_type == "court_rule" and unit.rule_set:
        parts.append(unit.rule_set)
    parts.append(unit.canonical_citation)
    return " → ".join(parts)


# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — embed
# ────────────────────────────────────────────────────────────────────────────

def embed_pending_for_title(conn, *, title_number: int, release_point: str,
                            batch_size: int = EMBED_BATCH_SIZE,
                            log_every: int = 5) -> int:
    """Embed any chunks that belong to documents in the given title and lack
    a stored embedding. Returns count embedded.

    Uses a client-side cursor (no server-side named cursor) because we
    need to commit after each embed batch — a named cursor would be
    invalidated by the commit. For ~17k chunks per title the memory cost
    of materializing the (id, content) list is trivial (~50 MB).
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.content
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            JOIN usc.sections s   ON s.document_id = d.id
            WHERE s.title_number = %s
              AND s.release_point = %s
              AND c.embedding IS NULL
            ORDER BY d.id, c.chunk_index
        """, (title_number, release_point))
        rows = cur.fetchall()
    conn.commit()  # release the read transaction before we start writing

    total_pending = len(rows)
    embedded = 0
    batch_no = 0
    t_start = time.time()
    print(f"  [embed] {total_pending} chunks pending; batch_size={batch_size}", flush=True)
    for i in range(0, total_pending, batch_size):
        batch = rows[i:i + batch_size]
        batch_ids = [r[0] for r in batch]
        batch_texts = [r[1] for r in batch]
        _flush_embed_batch(conn, batch_ids, batch_texts)
        embedded += len(batch)
        batch_no += 1
        if batch_no % log_every == 0 or embedded == total_pending:
            elapsed = time.time() - t_start
            rate = embedded / elapsed if elapsed > 0 else 0
            eta_sec = (total_pending - embedded) / rate if rate > 0 else 0
            print(f"  [embed] batch {batch_no}: {embedded}/{total_pending} "
                  f"rate={rate:.1f} chunks/sec elapsed={elapsed:.0f}s eta={eta_sec:.0f}s",
                  flush=True)
    return embedded


def _flush_embed_batch(conn, ids: list[str], texts: list[str]) -> None:
    """Embed batch then UPDATE chunks. Conn must be in autocommit or caller commits."""
    vecs = embed_texts_sync(texts)
    rows = []
    for cid, vec in zip(ids, vecs):
        if len(vec) != EMBEDDING_DIMENSIONS:
            raise RuntimeError(f"unexpected dim {len(vec)} for chunk {cid}")
        lit = "[" + ",".join(f"{x:.7f}" for x in vec) + "]"
        rows.append((lit, cid))
    with conn.cursor() as upd:
        # One UPDATE per chunk — execute_batch is fine here
        psycopg2.extras.execute_batch(upd, """
            UPDATE core.document_chunks
            SET embedding = %s::halfvec(1024),
                embedded_at = NOW()
            WHERE id = %s
        """, rows, page_size=64)
    conn.commit()


# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", type=int, required=True)
    ap.add_argument("--xml", required=True, help="path to title XML file")
    ap.add_argument("--release", default="PL 119-84")
    ap.add_argument("--current-through", default="2026-04-18")
    ap.add_argument("--appendix", action="store_true")
    ap.add_argument("--has-appendix", action="store_true",
                    help="set on the main title row when an appendix file also exists")
    ap.add_argument("--skip-stage1", action="store_true",
                    help="skip parsing/inserts; only embed pending chunks")
    ap.add_argument("--skip-stage2", action="store_true",
                    help="parse + insert + chunks; skip embedding")
    ap.add_argument("--limit", type=int, default=None,
                    help="ingest at most N units (smoke testing)")
    ap.add_argument("--run-id", default=None,
                    help="UUID for this ingestion_runs row; auto-generated if omitted")
    args = ap.parse_args()

    title_meta = title_meta_from_xml(args.xml)
    title_name = TITLE_NAMES.get(args.title, title_meta.get("title", f"Title {args.title}"))
    is_positive = (args.title in POSITIVE_LAW_TITLES) and not args.appendix
    if title_meta.get("is_positive_law_meta") is not None:
        is_positive = title_meta["is_positive_law_meta"] and not args.appendix

    run_id = args.run_id or str(uuid.uuid4())
    print(f"[ingest] title={args.title} xml={args.xml} appendix={args.appendix} "
          f"release={args.release!r} run_id={run_id}", flush=True)

    if not args.skip_stage1:
        with db_conn() as conn:
            # Appendix ingests are NOT authoritative for title-level metadata.
            # Title row + section_count belong to the main (non-appendix) ingest.
            # Skipping prevents an appendix run from clobbering is_positive_law,
            # has_appendix, or section_count set by the main run.
            if not args.appendix:
                ensure_title_row(conn,
                                 title_number=args.title,
                                 release_point=args.release,
                                 is_appendix=args.appendix,
                                 has_appendix=args.has_appendix,
                                 current_through=args.current_through,
                                 title_name=title_name)
            else:
                print(f"[ingest] appendix run for title {args.title} — "
                      f"skipping ensure_title_row (main title row authoritative)",
                      flush=True)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO usc.ingestion_runs
                        (id, release_point, started_at, titles_planned, status, notes)
                    VALUES (%s, %s, NOW(), 1, 'running', %s)
                    ON CONFLICT (id) DO NOTHING
                """, (run_id, args.release, f"title {args.title} ({Path(args.xml).name})"))
            conn.commit()

            tag_id_cache = load_tag_id_cache(conn)
            chapter_cache: dict = {}
            subchapter_cache: dict = {}

            t0 = time.time()
            n_units = 0
            n_chunks = 0
            source_filename = Path(args.xml).name
            for unit in parse_title_xml(args.xml, title_number=args.title,
                                        release_point=args.release,
                                        is_appendix=args.appendix):
                doc_id, sec_id, chunk_count = ingest_unit(
                    conn, unit=unit, release_point=args.release,
                    title_name=title_name, is_positive_law=is_positive,
                    chapter_id_cache=chapter_cache,
                    subchapter_id_cache=subchapter_cache,
                    tag_id_cache=tag_id_cache,
                    source_filename=source_filename,
                )
                n_units += 1
                n_chunks += chunk_count
                if args.limit and n_units >= args.limit:
                    break
                # Commit every 100 units so a long run isn't one big transaction
                if n_units % 100 == 0:
                    conn.commit()
                    elapsed = time.time() - t0
                    print(f"  [stage1] {n_units} units, {n_chunks} chunks, "
                          f"{elapsed:.0f}s elapsed, {n_units/elapsed:.1f} u/s",
                          flush=True)
            conn.commit()
            elapsed = time.time() - t0
            print(f"[stage1] DONE: {n_units} units, {n_chunks} chunks in {elapsed:.0f}s "
                  f"({n_chunks/n_units:.2f} chunks/unit)", flush=True)

            # Same rule: appendix ingests don't own section_count.
            if not args.appendix:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE usc.titles SET section_count = %s
                        WHERE title_number = %s
                    """, (n_units, args.title))
                conn.commit()
            else:
                print(f"[ingest] appendix run — skipping section_count UPDATE", flush=True)

    if not args.skip_stage2:
        print(f"[stage2] embedding pending chunks for title {args.title} "
              f"({args.release})", flush=True)
        with db_conn() as conn:
            embedded = embed_pending_for_title(
                conn, title_number=args.title, release_point=args.release)
            print(f"[stage2] embedded {embedded} chunks", flush=True)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE usc.ingestion_runs
                    SET completed_at = NOW(),
                        sections_ingested = COALESCE(sections_ingested, 0),
                        chunks_ingested = COALESCE(chunks_ingested, 0) + %s,
                        embeddings_completed = COALESCE(embeddings_completed, 0) + %s,
                        status = 'completed',
                        titles_completed = COALESCE(titles_completed, 0) + 1
                    WHERE id = %s
                """, (embedded, embedded, run_id))
            conn.commit()

    print(f"[ingest] DONE title={args.title} run_id={run_id}", flush=True)


if __name__ == "__main__":
    main()
