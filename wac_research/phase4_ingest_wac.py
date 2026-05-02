"""
WAC Phase 4 Ingest Driver — mirrors RCW phase4_ingest_rcw.py with WAC extensions.

  Stage 1 (fast):
    parse cached chapter HTML → INSERT wac.titles/chapters/sections + core.documents +
    section_history/notes/cross_references + authority_chain (WAC-specific) +
    core.document_tags + chunks (no embedding).

  Stage 2 (slower):
    Embed all chunks for the title via BGE-M3 in batches.

WAC-specific extensions:
  - wac.authority_chain INSERT per parsed authority entry
  - wsr_register_number column on wac.sections (latest WSR)
  - Disposition-only chapter handling: chapter row + ghost section rows
    derived from rec.dispositions (no embeddings on ghosts)
  - Skip list: 11 non-standard chapters (8 title-redirect + 2 meta-notice)
  - Dash citation format throughout

Usage:
    python3 phase4_ingest_wac.py --title 296
    python3 phase4_ingest_wac.py --title 162 --skip-stage2
    python3 phase4_ingest_wac.py --title 388 --only-stage2
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

sys.path.insert(0, "/opt/wdws")
sys.path.insert(0, "/opt/wdws/wac_research")

import psycopg2
import psycopg2.extras
from embedding_service import embed_texts_sync, EMBEDDING_DIMENSIONS

from wac_parser import (
    parse_chapter_html, TitleRedirectError,
    ChapterRecord, SectionRecord, HistoryEntry, NoteEntry, CrossRef,
    AuthorityChainEntry, DispositionEntry,
)

assert EMBEDDING_DIMENSIONS == 1024, f"unexpected embedding dim {EMBEDDING_DIMENSIONS}"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
EMBEDDING_MODEL_ID = 2  # BAAI/bge-m3
PUBLICATION_DATE = "2026-04-01"  # WAC landing-page Last update marker

CHUNK_TARGET_CHARS = 6000
CHUNK_MIN_CHARS    = 200
EMBED_BATCH_SIZE   = 64

ROOT = Path("/opt/wdws/data/wac/html")

# Skip list per Phase 3 anomaly inventory + operator decision (332-26 inspection)
SKIP_CITES: set[str] = {
    # 8 title-redirects (server fall-through to title TOC)
    "365-06", "365-12", "365-14", "365-22", "365-24", "365-31", "365-40", "365-60",
    # 2 meta-notice / appendix-only chapters (no operative content)
    "237-990", "332-26",
}


# ─────────────────────────────────────────────────────────────────────────────
# Topical-tag rules (WAC-specific). Title → operator-supplied topical tag.
# ─────────────────────────────────────────────────────────────────────────────

CHAPTER_TAG_MAP: dict[tuple[str, str], list[str]] = {
    # Priority core
    ("296", "*"):  ["bii-regulations-wac"],
    ("162", "*"):  ["wlad-enforcement-wac"],
    ("388", "*"):  ["dshs-wac"],
    ("110", "*"):  ["dcyf-wac"],
    # Priority block (slots 5-10)
    ("246", "*"):  ["doh-wac"],
    ("182", "*"):  ["hca-wac"],
    ("357", "*"):  ["civil-service-wac"],
    ("192", "*"):  ["employment-security-wac"],
    ("308", "*"):  ["licensing-wac"],
    ("458", "*"):  ["revenue-wac"],
}

# Cross-cutting topical tags from body text
TOPICAL_TAG_RULES: list[tuple[str, list[str]]] = [
    ("public records request",  ["pra-regulations-wac"]),
    ("public records act",      ["pra-regulations-wac"]),
    ("workers' compensation",   ["bii-regulations-wac"]),
    ("worker's compensation",   ["bii-regulations-wac"]),
    ("industrial insurance",    ["bii-regulations-wac"]),
    ("law against discrimination", ["wlad-enforcement-wac"]),
]


def base_tags_for(chap: ChapterRecord, sec: SectionRecord) -> list[str]:
    base = [
        "wac",
        "state-regulation",
        "state-wa-wac",
        f"wac-title-{chap.title_number.lower()}",
    ]
    if sec.is_repealed:    base.append("wac-repealed")
    if sec.is_recodified:  base.append("wac-recodified")
    if sec.is_decodified:  base.append("wac-decodified")
    if sec.is_expired:     base.append("wac-expired")
    if sec.is_reserved:    base.append("wac-reserved")
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
# Chunker
# ─────────────────────────────────────────────────────────────────────────────

def chunk_section_text(sec: SectionRecord, *, breadcrumb: str) -> list[str]:
    full = sec.full_text or ""
    if not full.strip():
        return []
    full_with_breadcrumb = f"{breadcrumb}\n\n{sec.section_heading}\n\n{full}"
    if len(full_with_breadcrumb) <= CHUNK_TARGET_CHARS:
        return [full_with_breadcrumb]

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
                     agency_name: str | None, chapter_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wac.titles
                (title_number, title_name, agency_name, publication_date,
                 last_ingested_at, chapter_count)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            ON CONFLICT (title_number) DO UPDATE SET
                title_name       = EXCLUDED.title_name,
                agency_name      = EXCLUDED.agency_name,
                publication_date = EXCLUDED.publication_date,
                last_ingested_at = NOW(),
                chapter_count    = EXCLUDED.chapter_count
        """, (title_number, title_name, agency_name, PUBLICATION_DATE, chapter_count))


def ensure_chapter_row(conn, cache: dict, *, chap: ChapterRecord) -> str:
    key = (chap.title_number, chap.chapter_number)
    if key in cache:
        return cache[key]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO wac.chapters (title_number, chapter_number, chapter_heading, sort_order)
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
    slug = f"wac-chapter-{chapter_number.lower()}"
    if slug in cache:
        return cache[slug]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO core.tags (slug, name, category, description)
            VALUES (%s, %s, 'wac-chapter', %s)
            ON CONFLICT (slug) DO NOTHING
            RETURNING id
        """, (slug, f"WAC chapter {chapter_number}",
              f"WAC chapter {chapter_number}"))
        row = cur.fetchone()
        if row:
            tag_id = row[0]
        else:
            cur.execute("SELECT id FROM core.tags WHERE slug = %s", (slug,))
            tag_id = cur.fetchone()[0]
    cache[slug] = tag_id
    return tag_id


def ensure_title_tag(conn, cache: dict[str, int], title_number: str, title_name: str) -> int:
    slug = f"wac-title-{title_number.lower()}"
    if slug in cache:
        return cache[slug]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO core.tags (slug, name, category, description)
            VALUES (%s, %s, 'wac-title', %s)
            ON CONFLICT (slug) DO NOTHING
            RETURNING id
        """, (slug, f"WAC Title {title_number}",
              f"WAC Title {title_number}: {title_name}"))
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
        f"wac:{PUBLICATION_DATE}:t{chap.title_number}"
        f":c{chap.chapter_number}:s{sec.section_number}"
    )
    title_str = f"{sec.canonical_citation} — {sec.section_heading or '(no heading)'}"
    metadata = {
        "type": "wac_section",
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
        "wsr_register_number": sec.wsr_register_number,
        "effective_date": sec.effective_date,
        "unit_type": sec.unit_type,
        "leg_wa_url": f"https://app.leg.wa.gov/WAC/default.aspx?cite={chap.chapter_number}-{sec.section_number}",
        "source_file": source_filename,
    }

    content_hash = hashlib.sha256((sec.full_text or "").encode("utf-8")).hexdigest()
    filename = source_path.replace("wac:", "").replace(":", "-") + ".html"

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
        """, (title_str, source_path, filename,
              sec.full_text or "",
              content_hash, json.dumps(metadata)))
        document_id = cur.fetchone()[0]

        # Tags
        slugs = base_tags_for(chap, sec) + topical_tags_for(chap, sec)
        chap_tag_id = ensure_chapter_tag(conn, tag_id_cache, chap.chapter_number)
        tag_ids = [tag_id_cache[s] for s in slugs if s in tag_id_cache]
        tag_ids.append(chap_tag_id)
        tag_ids = list(set(tag_ids))
        if tag_ids:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO core.document_tags (document_id, tag_id) VALUES %s
                ON CONFLICT DO NOTHING
            """, [(document_id, tid) for tid in tag_ids])

        # wac.sections row
        cur.execute("""
            INSERT INTO wac.sections
                (document_id, title_number, chapter_id,
                 section_number, canonical_citation, section_heading,
                 is_repealed, is_recodified, is_decodified, is_expired, is_reserved,
                 recodified_to, effective_date, wsr_register_number,
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
                wsr_register_number               = EXCLUDED.wsr_register_number,
                pending_recodification_to         = EXCLUDED.pending_recodification_to,
                pending_recodification_effective  = EXCLUDED.pending_recodification_effective,
                updated_at                        = NOW()
            RETURNING id
        """, (document_id, chap.title_number, chapter_id,
              sec.section_number, sec.canonical_citation, sec.section_heading,
              sec.is_repealed, sec.is_recodified, sec.is_decodified,
              sec.is_expired, sec.is_reserved,
              sec.recodified_to, sec.effective_date, sec.wsr_register_number,
              sec.full_text or None,
              sec.source_html or None,
              PUBLICATION_DATE, sec.unit_type,
              sec.pending_recodification_to,
              sec.pending_recodification_effective))
        section_id = cur.fetchone()[0]

        # History (WSR-based)
        cur.execute("DELETE FROM wac.section_history WHERE section_id = %s", (section_id,))
        if sec.history:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wac.section_history
                    (section_id, wsr_register, register_year, filed_date, effective_date,
                     action_type, action_summary, sort_order)
                VALUES %s
            """, [(section_id, h.wsr_register, h.register_year,
                   h.filed_date, h.effective_date,
                   h.action_type, h.action_summary, h.sort_order)
                  for h in sec.history])

        # Authority chain (WAC-specific)
        cur.execute("DELETE FROM wac.authority_chain WHERE wac_section_id = %s", (section_id,))
        if sec.authority_chain:
            # Deduplicate by (wac_section_id, authorizing_rcw) to satisfy UNIQUE constraint
            seen: set[str] = set()
            unique_auths = []
            for a in sec.authority_chain:
                if a.authorizing_rcw not in seen:
                    seen.add(a.authorizing_rcw)
                    unique_auths.append(a)
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wac.authority_chain
                    (wac_section_id, authorizing_rcw, authority_text,
                     is_chapter_level, is_session_law, sort_order)
                VALUES %s
            """, [(section_id, a.authorizing_rcw, a.authority_text,
                   a.is_chapter_level, a.is_session_law, a.sort_order)
                  for a in unique_auths])

        # Notes
        cur.execute("DELETE FROM wac.section_notes WHERE section_id = %s", (section_id,))
        if sec.notes:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wac.section_notes
                    (section_id, note_type, note_topic, note_heading,
                     note_text, sort_order)
                VALUES %s
            """, [(section_id, n.note_type, n.note_topic, n.note_heading,
                   n.note_text, n.sort_order)
                  for n in sec.notes])

        # Cross references
        cur.execute("DELETE FROM wac.cross_references WHERE source_section_id = %s", (section_id,))
        if sec.cross_refs:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO wac.cross_references
                    (source_section_id, target_citation, target_section_id,
                     target_jurisdiction, context_snippet, ref_type)
                VALUES %s
            """, [(section_id, x.target_citation, None,
                   x.target_jurisdiction, x.context_snippet, x.ref_type)
                  for x in sec.cross_refs])

        # Chunks (no embeddings yet) - skip for ghost rows
        if sec.full_text and sec.full_text.strip():
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
        else:
            chunks = []
            cur.execute("DELETE FROM core.document_chunks WHERE document_id = %s", (document_id,))
            cur.execute(
                "UPDATE core.documents SET total_chunks = 0 WHERE id = %s",
                (document_id,),
            )

    return document_id, section_id, len(chunks)


def ingest_disposition_as_ghost(conn, *, chap: ChapterRecord, dispo: DispositionEntry,
                                 chapter_id: str, tag_id_cache: dict[str, int],
                                 source_filename: str) -> str:
    """Ingest a disposition entry as a ghost wac.sections row.

    Ghost rows: no full_text, status flag set, no chunks/embeddings.
    """
    # Build a synthetic SectionRecord with status flags from disposition
    ghost_sec = SectionRecord(
        section_number=dispo.section_number,
        canonical_citation=dispo.canonical_citation,
        section_heading=dispo.section_caption,
        full_text="",  # ghost — no body
        source_html="",
        is_repealed=dispo.is_repealed,
        is_recodified=dispo.is_recodified,
        is_decodified=dispo.is_decodified,
        is_expired=dispo.is_expired,
        is_reserved=dispo.is_reserved,
        recodified_to=dispo.recodified_to,
        effective_date=dispo.effective_date,
        history=dispo.history,  # carry history from disposition row
    )
    # Reuse ingest_section path; chunks will be 0 since full_text is empty
    document_id, section_id, _ = ingest_section(
        conn, chap=chap, sec=ghost_sec,
        chapter_id=chapter_id, tag_id_cache=tag_id_cache,
        source_filename=source_filename,
    )
    return section_id


# ─────────────────────────────────────────────────────────────────────────────
# Title-level driver
# ─────────────────────────────────────────────────────────────────────────────

def stage1_ingest_title(conn, *, title_number: str, title_name: str,
                        agency_name: str | None,
                        chapter_files: list[Path]) -> dict:
    tag_id_cache = load_tag_id_cache(conn)
    chapter_id_cache: dict = {}

    ensure_title_row(conn, title_number=title_number, title_name=title_name,
                     agency_name=agency_name, chapter_count=len(chapter_files))
    # Lazy-create the wac-title-N tag (operator decision: lazy populate)
    ensure_title_tag(conn, tag_id_cache, title_number, title_name)
    conn.commit()

    stats = {
        "title": title_number,
        "chapters_processed": 0,
        "chapters_skipped": 0,
        "sections_ingested": 0,
        "ghost_sections": 0,
        "chunks_created": 0,
        "history_rows": 0,
        "authority_rows": 0,
        "notes_rows": 0,
        "xref_rows": 0,
        "errors": [],
    }
    t_start = time.time()

    for fp in chapter_files:
        chapter_cite = f"{fp.parent.name}-{fp.stem}"
        if chapter_cite in SKIP_CITES:
            stats["chapters_skipped"] += 1
            continue

        try:
            chap = parse_chapter_html(fp, publication_date=PUBLICATION_DATE)
        except TitleRedirectError as e:
            stats["chapters_skipped"] += 1
            print(f"  [stage1] SKIP (title-redirect) {fp.name}: {e}", flush=True)
            continue
        except Exception as e:
            stats["errors"].append({"file": str(fp), "error": str(e)})
            print(f"  [stage1] PARSE FAIL {fp.name}: {e}", flush=True)
            continue

        chapter_id = ensure_chapter_row(conn, chapter_id_cache, chap=chap)

        # Ingest active sections
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
                stats["authority_rows"] += len(sec.authority_chain)
                stats["notes_rows"] += len(sec.notes)
                stats["xref_rows"] += len(sec.cross_refs)
                conn.commit()
            except Exception as e:
                conn.rollback()
                stats["errors"].append({
                    "section": sec.canonical_citation,
                    "error": str(e),
                })
                print(f"  [stage1] SECTION FAIL {sec.canonical_citation}: {e}",
                      flush=True)
                continue

        # Ingest disposition entries as ghost rows
        for dispo in chap.dispositions:
            try:
                ingest_disposition_as_ghost(
                    conn, chap=chap, dispo=dispo,
                    chapter_id=chapter_id,
                    tag_id_cache=tag_id_cache,
                    source_filename=fp.name,
                )
                stats["ghost_sections"] += 1
                conn.commit()
            except Exception as e:
                conn.rollback()
                stats["errors"].append({
                    "ghost": dispo.canonical_citation,
                    "error": str(e),
                })
                print(f"  [stage1] GHOST FAIL {dispo.canonical_citation}: {e}",
                      flush=True)
                continue

        stats["chapters_processed"] += 1
        if stats["chapters_processed"] % 5 == 0 or stats["chapters_processed"] == len(chapter_files):
            elapsed = time.time() - t_start
            print(
                f"  [stage1 t{title_number}] chap {stats['chapters_processed']}/"
                f"{len(chapter_files)}  sec={stats['sections_ingested']}  "
                f"ghost={stats['ghost_sections']}  chunks={stats['chunks_created']}  "
                f"auth={stats['authority_rows']}  elapsed={elapsed:.1f}s",
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
            JOIN wac.sections   s ON s.document_id = d.id
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

TITLE_NAMES_AGENCIES = {
    "296": ("Labor and Industries (BIIA)", "Department of Labor and Industries"),
    "162": ("Human Rights Commission",     "Human Rights Commission"),
    "388": ("DSHS",                        "Department of Social and Health Services"),
    "110": ("DCYF",                        "Department of Children, Youth and Families"),
    "246": ("Department of Health",        "Department of Health"),
    "182": ("Health Care Authority",       "Health Care Authority"),
    "357": ("Civil Service",               "Office of Financial Management — State HR"),
    "192": ("Employment Security",         "Employment Security Department"),
    "308": ("Licensing",                   "Department of Licensing"),
    "458": ("Revenue",                     "Department of Revenue"),
}


def find_chapter_files(title_number: str) -> list[Path]:
    title_dir = ROOT / "chapters" / title_number
    if not title_dir.is_dir():
        raise SystemExit(f"No chapter dir found at {title_dir}")
    files = sorted(title_dir.glob("*.html"))
    return files


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", required=True,
                    help="WAC title number (e.g., '296', '162')")
    ap.add_argument("--skip-stage2", action="store_true",
                    help="Stage 1 only (parse + insert, no embeddings)")
    ap.add_argument("--only-stage2", action="store_true",
                    help="Stage 2 only (embed pending chunks for the title)")
    args = ap.parse_args()

    title_number = args.title
    title_name, agency_name = TITLE_NAMES_AGENCIES.get(
        title_number, (f"Title {title_number}", None),
    )
    chapter_files = find_chapter_files(title_number)
    print(f"=== WAC Phase 4 ingest: Title {title_number} — {title_name}")
    print(f"    chapters cached: {len(chapter_files)}")
    print(f"    publication_date: {PUBLICATION_DATE}")
    print(f"    embedding_model_id: {EMBEDDING_MODEL_ID}")

    with db_conn() as conn:
        # Open ingestion_runs row
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO wac.ingestion_runs
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
                agency_name=agency_name, chapter_files=chapter_files,
            )
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE wac.ingestion_runs SET
                        chapters_ingested = %s,
                        sections_ingested = %s,
                        chunks_ingested   = %s,
                        authority_rows    = %s
                    WHERE id = %s
                """, (stage1_stats["chapters_processed"],
                      stage1_stats["sections_ingested"] + stage1_stats["ghost_sections"],
                      stage1_stats["chunks_created"],
                      stage1_stats["authority_rows"],
                      run_id))
            conn.commit()
            print(f"--- Stage 1 done: {stage1_stats}", flush=True)

        embedded = 0
        if not args.skip_stage2:
            print(f"--- Stage 2: embed pending chunks ---", flush=True)
            embedded = stage2_embed_title(conn, title_number=title_number)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE wac.ingestion_runs SET
                        embeddings_completed = %s
                    WHERE id = %s
                """, (embedded, run_id))
            conn.commit()
            print(f"--- Stage 2 done: {embedded} chunks embedded", flush=True)

        with conn.cursor() as cur:
            cur.execute("""
                UPDATE wac.ingestion_runs SET
                    completed_at     = NOW(),
                    titles_completed = 1,
                    status           = 'completed'
                WHERE id = %s
            """, (run_id,))
        conn.commit()

    print(f"=== WAC Title {title_number} ingest COMPLETE ===")


if __name__ == "__main__":
    main()
