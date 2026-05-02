#!/usr/bin/env python3
"""Rebuild wa_court_rules.cross_references from all rules' full_text.

Recovery script for TRUNCATE-CASCADE incident 2026-05-01.
Iterates all wa_court_rules.rules, extracts citations using the project's
established regex patterns, and inserts cross_references rows + resolves
cross-corpus FKs. Replaces both phase4_cross_refs_reingest.py (1145 rules)
and task4_xrefs_resume.py (141 deferred rules) in a single idempotent pass.
"""
from __future__ import annotations
import os, sys, time
from pathlib import Path
import psycopg2, psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from phase1_regex_test import (
    WACR_CITE_RE, RCW_CITE_RE, RCW_CHAPTER_CITE_RE,
    USC_CITE_RE, WAC_CITE_RE, WACONST_CITE_RE,
    parse_cite, normalize_usc_section,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")


def extract_cross_refs(body: str, rule_set_code: str, rule_number: str) -> list[dict]:
    if not body:
        return []
    refs: list[dict] = []
    seen: set = set()

    for m in WACR_CITE_RE.finditer(body):
        info = parse_cite(m.group(0))
        if not info:
            continue
        t_set = info.get("set"); t_num = info.get("num")
        if not t_set or not t_num:
            continue
        if t_set == rule_set_code and t_num == rule_number:
            continue  # self-cite
        key = ("rule", t_set, t_num)
        if key in seen:
            continue
        seen.add(key)
        refs.append({
            "target_citation": f"{t_set} {t_num}",
            "target_jurisdiction": "state-wa-court-rule",
            "target_set_code": t_set, "target_rule_number": t_num,
            "target_subsection": info.get("subs", [None])[0] if info.get("subs") else None,
            "target_cmt_index": info.get("cmt"),
            "context_snippet": body[max(0,m.start()-100):m.end()+100][:240],
            "ref_type": "cross-rule",
        })

    for m in RCW_CITE_RE.finditer(body):
        cit = m.group(0).strip()
        key = ("rcw", cit)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cit, "target_jurisdiction": "state-wa-rcw",
            "target_set_code": None, "target_rule_number": None,
            "target_subsection": None, "target_cmt_index": None,
            "context_snippet": body[max(0,m.start()-100):m.end()+100][:240],
            "ref_type": "cross-statute",
        })

    for m in USC_CITE_RE.finditer(body):
        cit = m.group(0).strip()
        key = ("usc", cit)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cit, "target_jurisdiction": "federal-usc",
            "target_set_code": None, "target_rule_number": None,
            "target_subsection": None, "target_cmt_index": None,
            "context_snippet": body[max(0,m.start()-100):m.end()+100][:240],
            "ref_type": "cross-statute",
        })

    for m in WACONST_CITE_RE.finditer(body):
        cit = m.group(0).strip()
        key = ("const", cit)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cit, "target_jurisdiction": "wa-constitution",
            "target_set_code": None, "target_rule_number": None,
            "target_subsection": None, "target_cmt_index": None,
            "context_snippet": body[max(0,m.start()-100):m.end()+100][:240],
            "ref_type": "cross-statute",
        })

    for m in WAC_CITE_RE.finditer(body):
        cit = m.group(0).strip()
        key = ("wac", cit)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cit, "target_jurisdiction": "state-wa-wac",
            "target_set_code": None, "target_rule_number": None,
            "target_subsection": None, "target_cmt_index": None,
            "context_snippet": body[max(0,m.start()-100):m.end()+100][:240],
            "ref_type": "cross-statute",
        })

    return refs


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("SELECT id, rule_set_code, rule_number, full_text FROM wa_court_rules.rules")
        rules = cur.fetchall()
    print(f"Loaded {len(rules)} rules", flush=True)

    # Build citation_to_uuid for in-corpus rule FK resolution
    with conn.cursor() as cur:
        cur.execute("SELECT rule_set_code, rule_number, id FROM wa_court_rules.rules")
        citation_to_uuid = {f"{rs} {rn}": rid for rs, rn, rid in cur.fetchall()}

    # Clear existing rows (they should be 0 from truncate, but idempotent)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM wa_court_rules.cross_references")
    print(f"Cleared existing cross_references", flush=True)

    total_rows = 0
    t0 = time.time()
    all_rows = []

    for rule_id, rs_code, rn, body in rules:
        xrefs = extract_cross_refs(body or "", rs_code or "", rn or "")
        for x in xrefs:
            target_rule_uuid = None
            if x["target_jurisdiction"] == "state-wa-court-rule" and x["target_set_code"] and x["target_rule_number"]:
                target_rule_uuid = citation_to_uuid.get(f"{x['target_set_code']} {x['target_rule_number']}")
            all_rows.append((
                str(rule_id), x["target_citation"], target_rule_uuid,
                x["target_jurisdiction"], x["target_set_code"], x["target_rule_number"],
                x["target_subsection"], x["target_cmt_index"], x["context_snippet"],
                x["ref_type"],
            ))

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO wa_court_rules.cross_references
                (source_rule_id, target_citation, target_rule_id,
                 target_jurisdiction, target_set_code, target_rule_number,
                 target_subsection, target_cmt_index, context_snippet, ref_type)
            VALUES %s
        """, all_rows, page_size=1000)
        total_rows = len(all_rows)

    # Resolve cross-corpus FKs
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE wa_court_rules.cross_references c
               SET target_rcw_section_id = s.id
              FROM rcw.sections s
             WHERE c.target_jurisdiction = 'state-wa-rcw'
               AND c.target_rcw_section_id IS NULL
               AND s.canonical_citation = c.target_citation
        """)
        rcw_res = cur.rowcount

        cur.execute("""
            UPDATE wa_court_rules.cross_references c
               SET target_usc_section_id = s.id
              FROM usc.sections s
             WHERE c.target_jurisdiction = 'federal-usc'
               AND c.target_usc_section_id IS NULL
               AND s.canonical_citation = c.target_citation
        """)
        usc_res = cur.rowcount

    conn.commit()
    elapsed = time.time() - t0
    print(f"Inserted {total_rows:,} cross_references in {elapsed:.0f}s", flush=True)
    print(f"  RCW FK resolved: {rcw_res:,}  USC FK resolved: {usc_res:,}", flush=True)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wa_court_rules.cross_references")
        final = cur.fetchone()[0]
    print(f"Final wa_court_rules.cross_references count: {final:,}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
