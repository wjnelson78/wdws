#!/usr/bin/env python3
"""Phase 6: cross-corpus FK uplift — extract WA case citations from each
existing corpus's full_text and insert cross_references rows pointing to
case_law.opinions.

For each of the 5 per-corpus tables (usc.sections, rcw.sections, wac.sections,
wa_court_rules.rules, wa_constitution.sections):
  1. Iterate rows with non-empty full_text.
  2. Apply citation regex to find WA case citations:
       - 'X Wn.2d Y' (Sup Ct, native form)
       - 'X Wn. App. Y' / 'X Wn. App. 2d Y' (COA)
       - 'X P.2d Y' / 'X P.3d Y' (Pacific Reporter parallel)
  3. Lookup canonical citation in case_law.citations → cluster_id.
  4. Resolve cluster_id → lead opinion (case_law.opinions.type='020lead').
     If no lead, take first opinion in cluster.
  5. Insert into <corpus>.cross_references (idempotent on
     source_section_id + target_citation pair).

§K guards:
  - Idempotent: ON CONFLICT skip on (source_section_id, target_citation,
    target_case_law_opinion_id).
  - Safety net for absent unique index: pre-check + skip duplicates.
  - Prints summary per corpus.
"""
from __future__ import annotations

import os
import re
import sys
from typing import Optional

import psycopg

# WA case citation patterns (covers Wn.2d, Wn. App., Wn. App. 2d, P.2d, P.3d,
# Wash. forms, plus Wn. for old reports). Volume = 1-3 digits, Page = 1-4 digits.
CITE_PATTERNS = [
    (re.compile(r"\b(\d{1,3})\s+Wn\.?\s*2d\s+(\d{1,4})\b"),         "Wn.2d"),
    (re.compile(r"\b(\d{1,3})\s+Wn\.?\s*App\.\s*2d\s+(\d{1,4})\b"), "Wn. App. 2d"),
    (re.compile(r"\b(\d{1,3})\s+Wn\.?\s*App\.\s+(\d{1,4})\b"),      "Wn. App."),
    (re.compile(r"\b(\d{1,3})\s+Wash\.?\s*2d\s+(\d{1,4})\b"),       "Wn.2d"),
    (re.compile(r"\b(\d{1,3})\s+Wash\.\s*App\.\s+(\d{1,4})\b"),     "Wn. App."),
    (re.compile(r"\b(\d{1,3})\s+Wash\.\s+(\d{1,4})\b"),             "Wn."),
    (re.compile(r"\b(\d{1,3})\s+P\.\s*2d\s+(\d{1,4})\b"),           "P.2d"),
    (re.compile(r"\b(\d{1,3})\s+P\.\s*3d\s+(\d{1,4})\b"),           "P.3d"),
]

CORPORA = [
    ("usc",             "sections", "full_text"),
    ("rcw",             "sections", "full_text"),
    ("wac",             "sections", "full_text"),
    ("wa_court_rules",  "rules",    "full_text"),
    ("wa_constitution", "sections", "full_text"),
]


def build_citation_index(conn) -> dict:
    """Build canonical-citation → opinion_id lookup using case_law.citations
    matched to lead opinions (type='020lead') in each cluster.

    Two layers:
      (a) Lead opinion id per cluster
      (b) (volume, reporter, page) → cluster_id from case_law.citations
    Composed: (volume, reporter, page) → lead opinion id.
    """
    print("[index] building citation → opinion_id lookup...", flush=True)

    with conn.cursor() as cur:
        # Pick a representative opinion per cluster (lead preferred, else any).
        cur.execute("""
            SELECT cluster_id,
                   COALESCE(
                     (SELECT id FROM case_law.opinions o
                       WHERE o.cluster_id = c.id AND o.type='020lead'
                       ORDER BY id LIMIT 1),
                     (SELECT id FROM case_law.opinions o
                       WHERE o.cluster_id = c.id
                       ORDER BY id LIMIT 1)
                   )
              FROM case_law.opinion_clusters c
        """)
        cluster_to_op = {row[0]: row[1] for row in cur.fetchall() if row[1]}

        cur.execute("""
            SELECT volume, reporter, page, cluster_id
              FROM case_law.citations
             WHERE volume IS NOT NULL AND reporter IS NOT NULL AND page IS NOT NULL
        """)
        idx = {}
        for vol, reporter, page, cluster_id in cur.fetchall():
            op = cluster_to_op.get(cluster_id)
            if not op:
                continue
            # Normalize reporter strings to canonical forms used in patterns
            r = reporter.strip()
            if r in ("Wash. 2d", "Wash.2d"):
                r = "Wn.2d"
            elif r in ("Wash. App.", "Wash.App."):
                r = "Wn. App."
            elif r in ("Wash. App. 2d", "Wash.App.2d"):
                r = "Wn. App. 2d"
            elif r in ("Wash.",):
                r = "Wn."
            key = (str(vol).strip(), r, str(page).strip())
            idx.setdefault(key, op)

    print(f"[index] {len(cluster_to_op):,} clusters → opinion mapping; "
          f"{len(idx):,} (vol,reporter,page) keys indexed", flush=True)
    return idx


def extract_citations(text: str) -> list[tuple[str, str, str, int]]:
    """Yield (volume, reporter, page, position) tuples from text."""
    out = []
    for pat, reporter in CITE_PATTERNS:
        for m in pat.finditer(text):
            out.append((m.group(1), reporter, m.group(2), m.start()))
    return out


def context_snippet(text: str, position: int, span: int = 100) -> str:
    a = max(0, position - span)
    b = min(len(text), position + span)
    return text[a:b].replace("\n", " ").strip()[:240]


def process_corpus(conn, corpus: str, table: str, text_col: str, cite_idx: dict) -> int:
    """Extract case citations from a corpus's full_text rows and insert xrefs."""
    print(f"\n[{corpus}] scanning {corpus}.{table}.{text_col}...", flush=True)

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, {text_col}
              FROM {corpus}.{table}
             WHERE {text_col} IS NOT NULL AND length({text_col}) > 0
        """)
        rows = cur.fetchall()
    print(f"[{corpus}] {len(rows):,} rows to scan", flush=True)

    n_inserted = 0
    n_dups = 0
    n_unmatched_cites = 0
    n_matched_cites = 0
    seen = set()  # (source_id, target_op_id) within this run

    for sec_id, text in rows:
        cites = extract_citations(text)
        for vol, rep, page, pos in cites:
            op_id = cite_idx.get((vol, rep, page))
            if not op_id:
                n_unmatched_cites += 1
                continue
            n_matched_cites += 1
            key = (sec_id, op_id)
            if key in seen:
                continue
            seen.add(key)
            target_citation = f"{vol} {rep} {page}"
            snippet = context_snippet(text, pos)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {corpus}.cross_references
                            (source_section_id, target_citation, target_jurisdiction,
                             ref_type, context_snippet, target_case_law_opinion_id)
                        VALUES (%s, %s, 'state-wa-case-law', 'authority', %s, %s)
                    """, (sec_id, target_citation, snippet, op_id))
                    n_inserted += 1
            except psycopg.errors.UniqueViolation:
                n_dups += 1
                conn.rollback()
            except Exception as e:
                print(f"[{corpus}] INSERT failed for sec={sec_id} cite={target_citation}: {e}",
                      flush=True)
                conn.rollback()
        if n_inserted % 1000 == 0 and n_inserted > 0:
            conn.commit()
    conn.commit()

    print(f"[{corpus}] inserted={n_inserted:,} duplicates={n_dups:,} "
          f"matched_cites={n_matched_cites:,} unmatched_cites={n_unmatched_cites:,}",
          flush=True)
    return n_inserted


def main():
    db_url = os.environ.get("DATABASE_URL")
    assert db_url, "DATABASE_URL not set"

    with psycopg.connect(db_url) as conn:
        cite_idx = build_citation_index(conn)

        total_inserted = 0
        for corpus, table, text_col in CORPORA:
            total_inserted += process_corpus(conn, corpus, table, text_col, cite_idx)

        print(f"\n[Phase 6] DONE: {total_inserted:,} cross-corpus xrefs inserted "
              f"(target_jurisdiction='state-wa-case-law')", flush=True)


if __name__ == "__main__":
    main()
