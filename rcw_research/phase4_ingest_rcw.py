"""
RCW Phase 4 Ingest Driver — per-section transactional ingest with deferred BGE-M3 embedding.

Mirrors /opt/wdws/usc_research/usc_ingest.py architecture:

  Stage 1 (fast):
    parse cached chapter HTML → INSERT rcw.titles/chapters/sections + core.documents +
    section_history/notes/cross_references + core.document_tags + chunks (no embedding).

  Stage 2 (slower):
    Embed all chunks for the title via BGE-M3 in batches; UPDATE core.document_chunks.

Each section is a single transaction: rcw.sections row + core.documents row +
all child tables for that section + chunks. Failure of any step rolls back the
whole section (parser_design_notes.md §11 contract).

Usage:
    python3 phase4_ingest_rcw.py --title 51
    python3 phase4_ingest_rcw.py --title 42 --skip-stage2
    python3 phase4_ingest_rcw.py --title 4 --only-stage2
"""
from __future__ import annotations

import argparse, hashlib, json, os, sys, time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

sys.path.insert(0, "/opt/wdws")
sys.path.insert(0, "/opt/wdws/rcw_research")

import psycopg2, psycopg2.extras
from embedding_service import embed_texts_sync, EMBEDDING_DIMENSIONS

from rcw_parser import (
    parse_chapter_html, parse_disposition_html,
    ChapterRecord, SectionRecord, HistoryEntry, NoteEntry, CrossRef,
    DispositionRecord,
)

assert EMBEDDING_DIMENSIONS == 1024, f"unexpected embedding dim {EMBEDDING_DIMENSIONS}"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
PUBLICATION_DATE = "2025-08-15"  # RCW landing-page Last update marker as of 2026-04-28

# Chunking — same parameters as USC (BGE-M3 has 8192-token cap)
CHUNK_TARGET_CHARS = 6000
CHUNK_MIN_CHARS    = 200
EMBED_BATCH_SIZE   = 64

ROOT = Path("/opt/wdws/data/rcw/html")
COVERAGE = ROOT / "coverage_map.tsv"


# ─────────────────────────────────────────────────────────────────────────────
# Topical-tag rules (corpus-wide). Applied by text search over section text +
# heading. Additive to (title, chapter) rule map below.
# ─────────────────────────────────────────────────────────────────────────────

CHAPTER_TAG_MAP: dict[tuple[str, str], list[str]] = {
    # WLAD — Title 49 chapter 49.60
    ("49", "49.60"): ["wlad"],
    # PRA — Title 42 chapter 42.56
    ("42", "42.56"): ["pra"],
    # OMA — Title 42 chapter 42.30
    ("42", "42.30"): ["oma"],
    # BIIA — Title 51 (whole title applies)
    ("51", "*"):     ["bii"],
    # Civil Procedure umbrella — Title 4
    ("4", "*"):      ["wa-civil-procedure"],
    # Criminal procedure umbrellas
    ("9",  "*"):     ["wa-criminal-procedure"],
    ("9A", "*"):     ["wa-criminal-procedure"],
    ("10", "*"):     ["wa-criminal-procedure"],
    # Public-employment umbrella — Title 41
    ("41", "*"):     ["wa-public-employment"],
    # Courts umbrella — Title 2 (also includes the rule-making power chapter 2.04)
    ("2",  "*"):     ["wa-courts"],
    # Probate — Title 11
    ("11", "*"):     ["wa-probate"],
    # Domestic relations — Title 26 (chapter 26.44 also gets child-abuse + GAL tags)
    ("26", "*"):     ["wa-domestic-relations"],
    ("26", "26.44"): ["child-abuse-wa", "gal-wa"],
    # Mental health / public assistance
    ("71",  "*"):    ["wa-mental-health"],
    ("71A", "*"):    ["wa-mental-health"],
    ("74",  "*"):    ["wa-public-assistance"],
    # Special proceedings & writs — Title 7 (especially chapter 7.16)
    ("7", "*"):      [],
    ("7", "7.16"):   ["writs-wa"],
    # CPA — chapter 19.86
    ("19", "19.86"): ["cpa-wa"],
}

TOPICAL_TAG_RULES: list[tuple[str, list[str]]] = [
    # Civil-rights overlap
    ("discrimination",          ["civil-rights"]),
    ("equal protection",        ["equal-protection"]),
    ("due process",             ["due-process"]),
    ("section 1983",            ["section-1983"]),
    # Records access
    ("public records request",  ["pra"]),
    ("open public meetings",    ["oma"]),
    # Industrial insurance
    ("worker's compensation",   ["bii"]),
    ("workers' compensation",   ["bii"]),
    ("industrial insurance",    ["bii"]),
    # Mandamus / writs
    ("writ of mandamus",        ["writs-wa"]),
    ("certiorari",              ["writs-wa"]),
    # Child welfare
    ("guardian ad litem",       ["gal-wa"]),
    ("mandatory reporter",      ["child-abuse-wa"]),
]


# ─────────────────────────────────────────────────────────────────────────────
# Chunker — same algorithm as USC (parser-output is markdown-flavored already)
# ─────────────────────────────────────────────────────────────────────────────

def chunk_section_text(sec: SectionRecord, *, breadcrumb: str) -> list[str]:
    full = sec.full_text or ""
    if not full.strip():
        return []  # ghost / empty body — skip chunking
    full_with_breadcrumb = f"{breadcrumb}\n\n{sec.section_heading}\n\n{full}"
    if len(full_with_breadcrumb) <= CHUNK_TARGET_CHARS:
        return [full_with_breadcrumb]

    # RCW body paragraphs are separated by '\n\n' from the parser
    paras = full.split("\n\n")
    out: list[str] = []
    buf = ""
    for p in paras:
        if len(buf) + len(p) + 2 <= CHUNK_TARGET_CHARS - len(breadcrumb) - len(sec.section_heading) - 4:
            buf = (buf + "\n\n" + p) if buf else p
        else:
            if buf:
                out.append(f"{breadcrumb}\n\n{sec.section_heading}\n\n{buf}")
            if len(p) > CHUNK_TARGET_CHARS:
                # Hard slice
                for chunk in _hard_slice(p, CHUNK_TARGET_CHARS):
                    out.append(f"{breadcrumb}\n\n{sec.section_heading}\n\n{chunk}")
                buf = ""
            else:
                buf = p
    if buf:
        out.append(f"{breadcrumb}\n\n{sec.section_heading}\n\n{buf}")
    return out


def _hard_slice(text: str, target: int) -> list[str]:
    out = []
    i = 0
    while i < len(text):
        end = min(i + target, len(text))
        cut = text.rfind(" ", i + max(target // 2, 200), end)
        if cut <= i:
            cut = end
        out.append(text[i:cut].strip())
        i = cut
    return [c for c in out if c.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Tagging
# ─────────────────────────────────────────────────────────────────────────────

def base_tags_for(chap: ChapterRecord, sec: SectionRecord) -> list[str]:
    base = [
        "rcw",
        "state-statute",
        "state-wa-rcw",                       # code-jurisdiction
        f"rcw-title-{chap.title_number.lower()}",
        # rcw-chapter-{N.NN} tags are intentionally NOT pre-seeded; they're
        # lazily inserted via ensure_chapter_tag() at first use.
    ]
    if sec.is_repealed:    base.append("rcw-repealed")
    if sec.is_recodified:  base.append("rcw-recodified")
    if sec.is_decodified:  base.append("rcw-decodified")
    if sec.is_expired:     base.append("rcw-expired")
    if sec.is_reserved:    base.append("rcw-reserved")
    return base


def topical_tags_for(chap: ChapterRecord, sec: SectionRecord) -> list[str]:
    matched: set[str] = set()
    for key in ((chap.title_number, chap.chapter_number),
                (chap.title_number, "*")):
        for tag in CHAPTER_TAG_MAP.get(key, []):
            matched.add(tag)
    haystack = " ".join(filter(None, [
        (sec.section_heading or "").lower(),
        (chap.chapter_heading or "").lower(),
        (sec.full_text or "").lower(),
    ]))
    for needle, tags in TOPICAL_TAG_RULES:
        if needle in haystack:
            matched.update(tags)
    return sorted(matched)


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

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


def ensure_title_row(conn, *, title_number: str, title_name: str,
                     chapter_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO rcw.titles
                (title_number, title_name, publication_date,
                 last_ingested_at, chapter_count)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT (title_number) DO UPDATE SET
                title_name       = EXCLUDED.title_name,
                publication_date = EXCLUDED.publication_date,
                last_ingested_at = NOW(),
                chapter_count    = EXCLUDED.chapter_count
        """, (title_number, title_name, PUBLICATION_DATE, chapter_count))


def ensure_chapter_row(conn, cache: dict, *, chap: ChapterRecord) -> Optional[str]:
    key = (chap.title_number, chap.chapter_number)
    if key in cache:
        return cache[key]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO rcw.chapters (title_number, chapter_number, chapter_heading, sort_order)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (title_number, chapter_number) DO UPDATE SET
                chapter_heading = EXCLUDED.chapter_heading
            RETURNING id
        """, (chap.title_number, chap.chapter_number, chap.chapter_heading,
              _sort_order_for(chap.chapter_number)))
        cid = cur.fetchone()[0]
    cache[key] = cid
    return cid


def ensure_chapter_tag(conn, cache: dict[str, int], chapter_number: str) -> int:
    """Lazily insert a rcw-chapter-{N.NN} tag and cache its id."""
    slug = f"rcw-chapter-{chapter_number.lower()}"
    if slug in cache:
        return cache[slug]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO core.tags (slug, name, category, description)
            VALUES (%s, %s, 'rcw-chapter', %s)
            ON CONFLICT (slug) DO NOTHING
            RETURNING id
        """, (slug, f"RCW chapter {chapter_number}",
              f"RCW chapter {chapter_number}"))
        row = cur.fetchone()
        if row:
            tag_id = row[0]
        else:
            cur.execute("SELECT id FROM core.tags WHERE slug = %s", (slug,))
            tag_id = cur.fetchone()[0]
    cache[slug] = tag_id
    return tag_id


def _sort_order_for(text_number: str) -> int:
    import re as _re
    m = _re.match(r"(\d+)", text_number or "")
    return int(m.group(1)) if m else 0


# ─────────────────────────────────────────────────────────────────────────────
# Per-section ingest
# ─────────────────────────────────────────────────────────────────────────────

def ingest_section(conn, *, chap: ChapterRecord, sec: SectionRecord,
                   chapter_id: str, tag_id_cache: dict[str, int],
                   source_filename: str) -> tuple[str, str, int]:
    """Ingest one SectionRecord. Returns (document_id, section_id, chunk_count)."""
    source_path = (
        f"rcw:{PUBLICATION_DATE}:t{chap.title_number}"
        f":c{chap.chapter_number}:s{sec.section_number}"
    )
    title_str = f"{sec.canonical_citation} — {sec.section_heading or '(no heading)'}"
    metadata = {
        "type": "rcw_section",
        "title_number": chap.title_number,
        "chapter_number": chap.chapter_number,
        "chapter_heading": chap.chapter_heading,
        "section_number": sec.section_number,
        "section_heading": sec.section_heading,
        "canonical_citation": sec.canonical_citation,
        "publication_date": PUBLICATION_DATE,
        "is_repealed": sec.is_repealed,
        "is_recodified": sec.is_recodified,
        "is_decodified": sec.is_decodified,
        "is_expired": sec.is_expired,
        "is_reserved": sec.is_reserved,
        "recodified_to": sec.recodified_to,
        "recodified_from": sec.recodified_from,
        "effective_date": sec.effective_date,
        "enacted_by_session_law": sec.enacted_by_session_law,
        "unit_type": sec.unit_type,
        "leg_wa_url": (
            f"https://app.leg.wa.gov/RCW/default.aspx?cite={chap.chapter_number}.{sec.section_number}"
            if "-" not in sec.section_number
            else f"https://app.leg.wa.gov/RCW/default.aspx?cite={chap.chapter_number}-{sec.section_number.split('-', 1)[-1]}"
            # UCC URL form: app.leg.wa.gov/RCW/default.aspx?cite=62A.1-101 (use full citation)
        ),
        "source_file": source_filename,
    }
    # Re-compute leg_wa_url cleanly using canonical citation
    cite_no_prefix = sec.canonical_citation.replace("RCW ", "").strip()
    metadata["leg_wa_url"] = f"https://app.leg.wa.gov/RCW/default.aspx?cite={cite_no_prefix}"

    content_hash = hashlib.sha256((sec.full_text or "").encode("utf-8")).hexdigest()
    filename = source_path.replace("rcw:", "").replace(":", "-") + ".html"

    with conn.cursor() as cur:
        # core.documents (upsert on partial unique index for document_type='statute')
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
        """, (title_str, source_path, filename,
              sec.full_text or "",
              content_hash, json.dumps(metadata)))
        document_id = cur.fetchone()[0]

        # Tags: base + chapter-specific + topical + the code-jurisdiction tag
        # We attach the code-jurisdiction tag explicitly via slug 'state-wa-rcw'
        slugs = base_tags_for(chap, sec) + topical_tags_for(chap, sec)
        # Add the chapter-specific tag (lazily insert on first use)
        chap_tag_id = ensure_chapter_tag(conn, tag_id_cache, chap.chapter_number)
        # Resolve named slugs to IDs (silently drop unknown slugs — defensive)
        tag_ids = [tag_id_cache[s] for s in slugs if s in tag_id_cache]
        tag_ids.append(chap_tag_id)
        # De-duplicate
        tag_ids = list(set(tag_ids))
        if tag_ids:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_tags (document_id, tag_id) VALUES %s
                ON CONFLICT DO NOTHING
            """, [(document_id, tid) for tid in tag_ids])

        # rcw.sections row
        cur.execute("""
            INSERT INTO rcw.sections
                (document_id, title_number, chapter_id,
                 section_number, canonical_citation, section_heading,
                 is_repealed, is_recodified, is_decodified, is_expired, is_reserved,
                 recodified_to, effective_date, enacted_by_session_law,
                 full_text, source_html, publication_date, unit_type,
                 pending_recodification_to, pending_recodification_effective)
            VALUES (%s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s)
            ON CONFLICT (canonical_citation, publication_date) DO UPDATE SET
                document_id                       = EXCLUDED.document_id,
                full_text                         = EXCLUDED.full_text,
                source_html                       = EXCLUDED.source_html,
                section_heading                   = EXCLUDED.section_heading,
                is_repealed                       = EXCLUDED.is_repealed,
                is_recodified                     = EXCLUDED.is_recodified,
                is_decodified                     = EXCLUDED.is_decodified,
                is_expired                        = EXCLUDED.is_expired,
                is_reserved                       = EXCLUDED.is_reserved,
                recodified_to                     = EXCLUDED.recodified_to,
                effective_date                    = EXCLUDED.effective_date,
                enacted_by_session_law            = EXCLUDED.enacted_by_session_law,
                pending_recodification_to         = EXCLUDED.pending_recodification_to,
                pending_recodification_effective  = EXCLUDED.pending_recodification_effective,
                updated_at                        = NOW()
            RETURNING id
        """, (document_id, chap.title_number, chapter_id,
              sec.section_number, sec.canonical_citation, sec.section_heading,
              sec.is_repealed, sec.is_recodified, sec.is_decodified,
              sec.is_expired, sec.is_reserved,
              sec.recodified_to, sec.effective_date, sec.enacted_by_session_law,
              sec.full_text or None,        # NULL-able for ghost rows
              sec.source_html or None,
              PUBLICATION_DATE, sec.unit_type,
              sec.pending_recodification_to,
              sec.pending_recodification_effective))
        section_id = cur.fetchone()[0]

        # History (delete-then-insert for idempotency)
        cur.execute("DELETE FROM rcw.section_history WHERE section_id = %s", (section_id,))
        if sec.history:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO rcw.section_history
                    (section_id, session_law, session_year, session_law_pdf,
                     action_type, action_summary, sort_order)
                VALUES %s
            """, [(section_id, h.session_law, h.session_year, h.session_law_pdf,
                   h.action_type, h.action_summary, h.sort_order)
                  for h in sec.history])

        # Notes
        cur.execute("DELETE FROM rcw.section_notes WHERE section_id = %s", (section_id,))
        if sec.notes:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO rcw.section_notes
                    (section_id, note_type, note_topic, note_heading,
                     note_text, sort_order)
                VALUES %s
            """, [(section_id, n.note_type, n.note_topic, n.note_heading,
                   n.note_text, n.sort_order)
                  for n in sec.notes])

        # Cross references
        cur.execute("DELETE FROM rcw.cross_references WHERE source_section_id = %s", (section_id,))
        if sec.cross_refs:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO rcw.cross_references
                    (source_section_id, target_citation, target_section_id,
                     target_jurisdiction, context_snippet, ref_type)
                VALUES %s
            """, [(section_id, x.target_citation, None,
                   x.target_jurisdiction, x.context_snippet, x.ref_type)
                  for x in sec.cross_refs])

        # Chunks (no embeddings yet)
        breadcrumb = (
            f"Title {chap.title_number} — Chapter {chap.chapter_number}: "
            f"{chap.chapter_heading} → {sec.canonical_citation}"
        )
        chunks = chunk_section_text(sec, breadcrumb=breadcrumb)
        cur.execute("DELETE FROM core.document_chunks WHERE document_id = %s", (document_id,))
        if chunks:
            chunk_rows = []
            for i, ctext in enumerate(chunks):
                chunk_id = f"{document_id}:{i}"
                chunk_rows.append((
                    chunk_id, document_id, i, len(chunks), ctext,
                    EMBEDDING_MODEL_ID, len(ctext.split()),
                    ctext,
                ))
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_chunks
                    (id, document_id, chunk_index, total_chunks, content,
                     embedding_model_id, token_count, embedded_content)
                VALUES %s
            """, chunk_rows)

        cur.execute(
            "UPDATE core.documents SET total_chunks = %s WHERE id = %s",
            (len(chunks), document_id),
        )

    return document_id, section_id, len(chunks)


# ─────────────────────────────────────────────────────────────────────────────
# Title-level driver
# ─────────────────────────────────────────────────────────────────────────────

def stage1_ingest_title(conn, *, title_number: str, title_name: str,
                        chapter_files: list[Path]) -> dict:
    tag_id_cache = load_tag_id_cache(conn)
    chapter_id_cache: dict = {}

    ensure_title_row(conn, title_number=title_number, title_name=title_name,
                     chapter_count=len(chapter_files))
    conn.commit()

    stats = {
        "title": title_number,
        "chapters_processed": 0,
        "sections_ingested": 0,
        "chunks_created": 0,
        "history_rows": 0,
        "notes_rows": 0,
        "xref_rows": 0,
        "reserved_chapters": 0,
        "errors": [],
    }
    t_start = time.time()

    for fp in chapter_files:
        try:
            chap = parse_chapter_html(fp, publication_date=PUBLICATION_DATE)
        except Exception as e:
            stats["errors"].append({"file": str(fp), "error": str(e)})
            print(f"  [stage1] PARSE FAIL {fp.name}: {e}", flush=True)
            continue

        if chap.is_reserved_chapter:
            stats["reserved_chapters"] += 1

        chapter_id = ensure_chapter_row(conn, chapter_id_cache, chap=chap)
        if chap.sections:
            for sec in chap.sections:
                try:
                    _, _, n_chunks = ingest_section(
                        conn,
                        chap=chap, sec=sec,
                        chapter_id=chapter_id,
                        tag_id_cache=tag_id_cache,
                        source_filename=fp.name,
                    )
                    stats["sections_ingested"] += 1
                    stats["chunks_created"] += n_chunks
                    stats["history_rows"] += len(sec.history)
                    stats["notes_rows"] += len(sec.notes)
                    stats["xref_rows"] += len(sec.cross_refs)
                except Exception as e:
                    conn.rollback()
                    stats["errors"].append({
                        "section": sec.canonical_citation,
                        "error": str(e),
                    })
                    print(f"  [stage1] SECTION FAIL {sec.canonical_citation}: {e}",
                          flush=True)
                    continue
                conn.commit()
        stats["chapters_processed"] += 1
        if stats["chapters_processed"] % 5 == 0 or stats["chapters_processed"] == len(chapter_files):
            elapsed = time.time() - t_start
            print(
                f"  [stage1 t{title_number}] chap {stats['chapters_processed']}/"
                f"{len(chapter_files)}  sections={stats['sections_ingested']}  "
                f"chunks={stats['chunks_created']}  elapsed={elapsed:.1f}s",
                flush=True,
            )

    stats["stage1_seconds"] = time.time() - t_start
    return stats


def stage2_embed_title(conn, *, title_number: str,
                       batch_size: int = EMBED_BATCH_SIZE,
                       log_every: int = 5) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.content
            FROM core.document_chunks c
            JOIN core.documents d ON c.document_id = d.id
            JOIN rcw.sections   s ON s.document_id = d.id
            WHERE s.title_number = %s
              AND s.publication_date = %s
              AND c.embedding IS NULL
            ORDER BY d.id, c.chunk_index
        """, (title_number, PUBLICATION_DATE))
        rows = cur.fetchall()
    conn.commit()

    total = len(rows)
    if total == 0:
        print(f"  [stage2 t{title_number}] no pending chunks", flush=True)
        return 0

    print(f"  [stage2 t{title_number}] {total} chunks pending; batch_size={batch_size}",
          flush=True)
    embedded = 0
    t_start = time.time()
    batch_no = 0
    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        ids   = [r[0] for r in batch]
        texts = [r[1] for r in batch]
        vecs = embed_texts_sync(texts)
        upd_rows = []
        for cid, vec in zip(ids, vecs):
            if len(vec) != EMBEDDING_DIMENSIONS:
                raise RuntimeError(f"unexpected dim {len(vec)} for chunk {cid}")
            lit = "[" + ",".join(f"{x:.7f}" for x in vec) + "]"
            upd_rows.append((lit, cid))
        with conn.cursor() as upd:
            psycopg2.extras.execute_batch(upd, """
                UPDATE core.document_chunks
                SET embedding = %s::halfvec(1024),
                    embedded_at = NOW()
                WHERE id = %s
            """, upd_rows, page_size=64)
        conn.commit()
        embedded += len(batch)
        batch_no += 1
        if batch_no % log_every == 0 or embedded == total:
            elapsed = time.time() - t_start
            rate = embedded / elapsed if elapsed > 0 else 0
            eta = (total - embedded) / rate if rate > 0 else 0
            print(
                f"  [stage2 t{title_number}] batch {batch_no}: {embedded}/{total} "
                f"rate={rate:.1f} chunks/s elapsed={elapsed:.0f}s eta={eta:.0f}s",
                flush=True,
            )
    return embedded


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

# Operator-curated litigation-priority title names. Phase 1 §3d already pre-
# seeded the rcw-title-N tags with these; we look up rcw.titles.title_name
# from the same table at INSERT.
TITLE_NAMES = {
    "51":  "Industrial Insurance",
    "42":  "Public Officers and Agencies",
    "4":   "Civil Procedure",
    "49":  "Labor Regulations",
    "26":  "Domestic Relations",
    "41":  "Public Employment, Civil Service, and Pensions",
    "2":   "Courts of Record",
    "9":   "Crimes and Punishments",
    "9A":  "Washington Criminal Code",
    "10":  "Criminal Procedure",
    "19":  "Business Regulations — Miscellaneous",
    "7":   "Special Proceedings and Actions",
    "70":  "Public Health and Safety",
    "71":  "Behavioral Health",
    "74":  "Public Assistance",
    "11":  "Probate and Trust Law",
}


def find_chapter_files(title_number: str) -> list[Path]:
    """Return sorted list of chapter HTML files for the given title."""
    title_dir = ROOT / "chapters" / f"title_{title_number}"
    if not title_dir.is_dir():
        raise SystemExit(f"No chapter dir found at {title_dir}")
    files = sorted(title_dir.glob("chap_*_FULL.html"))
    return files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", required=True,
                    help="RCW title number (e.g., '51', '9A', '42')")
    ap.add_argument("--skip-stage1", action="store_true")
    ap.add_argument("--skip-stage2", action="store_true",
                    help="Stage 1 only (parse + insert, no embeddings)")
    ap.add_argument("--only-stage2", action="store_true",
                    help="Stage 2 only (embed pending chunks for the title)")
    args = ap.parse_args()

    title_number = args.title
    title_name = TITLE_NAMES.get(title_number, f"Title {title_number}")
    chapter_files = find_chapter_files(title_number)
    print(f"=== RCW Phase 4 ingest: Title {title_number} — {title_name}")
    print(f"    chapters cached: {len(chapter_files)}")
    print(f"    publication_date: {PUBLICATION_DATE}")
    print(f"    embedding_model_id: {EMBEDDING_MODEL_ID}")

    with db_conn() as conn:
        # Open ingestion_runs row
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rcw.ingestion_runs
                    (publication_date, started_at, titles_planned,
                     titles_completed, status, notes)
                VALUES (%s, NOW(), 1, 0, 'running', %s)
                RETURNING id
            """, (PUBLICATION_DATE, f"Title {title_number} ({title_name})"))
            run_id = cur.fetchone()[0]
        conn.commit()

        stage1_stats = None
        if not args.only_stage2:
            print(f"--- Stage 1: parse + INSERT chapters/sections ---", flush=True)
            stage1_stats = stage1_ingest_title(
                conn, title_number=title_number, title_name=title_name,
                chapter_files=chapter_files,
            )
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE rcw.ingestion_runs SET
                        chapters_ingested = %s,
                        sections_ingested = %s,
                        chunks_ingested   = %s
                    WHERE id = %s
                """, (stage1_stats["chapters_processed"],
                      stage1_stats["sections_ingested"],
                      stage1_stats["chunks_created"],
                      run_id))
            conn.commit()
            print(f"--- Stage 1 done: {stage1_stats}", flush=True)

        embedded = 0
        if not args.skip_stage2:
            print(f"--- Stage 2: embed pending chunks ---", flush=True)
            embedded = stage2_embed_title(conn, title_number=title_number)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE rcw.ingestion_runs SET
                        embeddings_completed = %s
                    WHERE id = %s
                """, (embedded, run_id))
            conn.commit()
            print(f"--- Stage 2 done: {embedded} chunks embedded", flush=True)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE rcw.ingestion_runs SET
                    completed_at     = NOW(),
                    titles_completed = 1,
                    status           = 'completed'
                WHERE id = %s
            """, (run_id,))
        conn.commit()

    print(f"=== RCW Title {title_number} ingest COMPLETE ===")


if __name__ == "__main__":
    main()
