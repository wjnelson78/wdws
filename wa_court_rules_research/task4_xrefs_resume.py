"""
Task 4 cross-references resume — runs after task4_deferred_ingest.py committed
the 141 rule rows but halted on chk_cross_ref_type.

Re-extracts cross-refs from each newly-ingested rule's full_text using only
the allowed ref_type values: 'reference', 'authority', 'cross-rule',
'cross-statute', 'comment', 'subsection-cite'.

Mapping (post-halt fix):
  - court-rule citations (WACR_CITE_RE)        → 'cross-rule'
  - WAC citations                              → 'cross-rule'
  - RCW section/chapter citations              → 'cross-statute'
  - USC citations                              → 'cross-statute'
  - WA Constitution citations                  → 'cross-statute'

After insert, also resolves target_rcw_section_id / target_usc_section_id /
target_rule_id FKs by direct join — same logic as Phase 4.4.
"""
from __future__ import annotations
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from phase1_regex_test import (  # type: ignore
    WACR_CITE_RE, RCW_CITE_RE, RCW_CHAPTER_CITE_RE,
    USC_CITE_RE, WAC_CITE_RE, WACONST_CITE_RE,
    parse_cite, normalize_usc_section,
)

DSN = os.environ.get("DATABASE_URL", "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
DEFERRED_SETS = ['CCR','IRLJ','AR','ARLJ','BJAR','DRJ','JISCR','SPR','SPRC',
                 'ELLLTC','ELPOC','LPORPC','ATJ','BJA','SCAR']


def extract_cross_refs(body: str) -> list[dict]:
    if not body:
        return []
    refs: list[dict] = []
    seen = set()

    for m in WACR_CITE_RE.finditer(body):
        info = parse_cite(m.group(0))
        if not info:
            continue
        set_c = info.get("set"); num = info.get("num")
        if not set_c or not num:
            continue
        subs_list = info.get("subs") or []
        cmt = info.get("cmt")
        key = ("court", set_c, num)
        if key in seen:
            continue
        seen.add(key)
        refs.append({
            "target_citation": f"{set_c} {num}",
            "target_set_code": set_c,
            "target_rule_number": num,
            "target_subsection": subs_list[0] if subs_list else None,
            "target_cmt_index": cmt,
            "target_jurisdiction": "state-wa-court-rule",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-rule",
        })

    for m in RCW_CITE_RE.finditer(body):
        section = m.group(1) if m.lastindex else None
        if not section:
            continue
        cite = f"RCW {section}"
        key = ("rcw", cite)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cite,
            "target_set_code": "RCW",
            "target_rule_number": section,
            "target_subsection": None, "target_cmt_index": None,
            "target_jurisdiction": "state-wa-rcw",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-statute",
        })

    for m in RCW_CHAPTER_CITE_RE.finditer(body):
        chap = m.group(1) if m.lastindex else None
        if not chap:
            continue
        cite = f"RCW {chap}"
        key = ("rcw", cite)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cite,
            "target_set_code": "RCW",
            "target_rule_number": chap,
            "target_subsection": None, "target_cmt_index": None,
            "target_jurisdiction": "state-wa-rcw",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-statute",
        })

    for m in USC_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        section = normalize_usc_section(m.group(0)) or None
        key = ("usc", cite)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cite,
            "target_set_code": "USC",
            "target_rule_number": section,
            "target_subsection": None, "target_cmt_index": None,
            "target_jurisdiction": "federal-usc",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-statute",
        })

    for m in WAC_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        key = ("wac", cite)
        if key in seen: continue
        seen.add(key)
        wac_num = cite.replace("WAC ", "").strip() if cite.startswith("WAC ") else None
        refs.append({
            "target_citation": cite,
            "target_set_code": "WAC",
            "target_rule_number": wac_num,
            "target_subsection": None, "target_cmt_index": None,
            "target_jurisdiction": "state-wa-wac",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-rule",
        })

    for m in WACONST_CITE_RE.finditer(body):
        cite = m.group(0).strip()
        key = ("waconst", cite)
        if key in seen: continue
        seen.add(key)
        refs.append({
            "target_citation": cite,
            "target_set_code": "WA-CONST",
            "target_rule_number": None,
            "target_subsection": None, "target_cmt_index": None,
            "target_jurisdiction": "wa-constitution",
            "context_snippet": body[max(0,m.start()-60):m.end()+60].replace("\n", " ")[:500],
            "ref_type": "cross-statute",
        })

    return refs


def main() -> int:
    print(f"=== Task 4 xref resume ===")
    t0 = time.time()
    conn = psycopg2.connect(DSN)
    conn.autocommit = False

    try:
        # Pre-flight: confirm rules are in place but xrefs NOT yet for these source rules
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM wa_court_rules.rules
                 WHERE rule_set_code = ANY(%s)
            """, (DEFERRED_SETS,))
            rule_count = cur.fetchone()[0]
            cur.execute("""
                SELECT COUNT(*) FROM wa_court_rules.cross_references c
                JOIN wa_court_rules.rules r ON r.id = c.source_rule_id
                WHERE r.rule_set_code = ANY(%s)
            """, (DEFERRED_SETS,))
            existing_xrefs = cur.fetchone()[0]
        print(f"  Pre-flight: {rule_count} deferred-set rules; {existing_xrefs} pre-existing xrefs from these rules")
        if existing_xrefs > 0:
            print(f"  WARNING: {existing_xrefs} xrefs already exist; this resume run would duplicate.")
            print(f"  Aborting safely. To proceed, manually clear: DELETE FROM wa_court_rules.cross_references "
                  f"WHERE source_rule_id IN (SELECT id FROM wa_court_rules.rules WHERE rule_set_code = ANY(...));")
            return 1

        # Fetch all rules + their full_text
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, rule_set_code, rule_number, canonical_citation, full_text
                FROM wa_court_rules.rules
                WHERE rule_set_code = ANY(%s)
                ORDER BY rule_set_code, rule_number
            """, (DEFERRED_SETS,))
            rules = cur.fetchall()

        # Build citation -> uuid map for in-corpus self-resolution (deferred sets only)
        citation_to_uuid_deferred = {f"{rs} {rn}": str(rid) for rid, rs, rn, cc, _ in rules}

        # Build citation -> uuid map for ALL existing rules (for cross-set FK resolution)
        with conn.cursor() as cur:
            cur.execute("SELECT canonical_citation, id FROM wa_court_rules.rules")
            citation_to_uuid_all = {cc: str(rid) for cc, rid in cur.fetchall()}

        # Extract + insert xrefs per rule
        all_rows = []
        skipped_self = 0
        per_set_counts: dict[str, int] = {}
        for rid, rs_code, rn, cc, body in rules:
            xrefs = extract_cross_refs(body or "")
            for x in xrefs:
                t_set = x.get("target_set_code")
                t_num = x.get("target_rule_number")
                # Self-cite filter
                if t_set == rs_code and t_num == rn:
                    skipped_self += 1
                    continue
                # Resolve target_rule_id for in-corpus court-rule citations
                target_uuid = None
                if x["target_jurisdiction"] == "state-wa-court-rule" and t_set and t_num:
                    target_uuid = citation_to_uuid_all.get(f"{t_set} {t_num}")
                all_rows.append((
                    str(rid),
                    x["target_citation"],
                    target_uuid,
                    x["target_jurisdiction"],
                    t_set,
                    t_num,
                    x.get("target_subsection"),
                    x.get("target_cmt_index"),
                    x.get("context_snippet"),
                    x["ref_type"],
                ))
            per_set_counts[rs_code] = per_set_counts.get(rs_code, 0) + len(xrefs)

        print(f"  Extracted {len(all_rows)} cross-refs ({skipped_self} self-cites filtered)")
        for rs, n in sorted(per_set_counts.items()):
            print(f"    {rs}: {n} xrefs")

        if all_rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, """
                    INSERT INTO wa_court_rules.cross_references
                        (source_rule_id, target_citation, target_rule_id,
                         target_jurisdiction, target_set_code, target_rule_number,
                         target_subsection, target_cmt_index, context_snippet, ref_type)
                    VALUES %s
                """, all_rows)

                # Resolve cross-corpus FKs by direct join — same as Phase 4.4
                cur.execute("""
                    UPDATE wa_court_rules.cross_references c
                    SET target_rcw_section_id = s.id
                    FROM rcw.sections s, wa_court_rules.rules r
                    WHERE c.source_rule_id = r.id
                      AND r.rule_set_code = ANY(%s)
                      AND c.target_jurisdiction = 'state-wa-rcw'
                      AND c.target_rcw_section_id IS NULL
                      AND s.canonical_citation = c.target_citation
                """, (DEFERRED_SETS,))
                rcw_resolved = cur.rowcount
                cur.execute("""
                    UPDATE wa_court_rules.cross_references c
                    SET target_usc_section_id = s.id
                    FROM usc.sections s, wa_court_rules.rules r
                    WHERE c.source_rule_id = r.id
                      AND r.rule_set_code = ANY(%s)
                      AND c.target_jurisdiction = 'federal-usc'
                      AND c.target_usc_section_id IS NULL
                      AND s.canonical_citation = c.target_citation
                """, (DEFERRED_SETS,))
                usc_resolved = cur.rowcount
            conn.commit()
            print(f"  Inserted {len(all_rows)} xrefs; "
                  f"FK resolution → RCW={rcw_resolved}, USC={usc_resolved}")
        else:
            print("  No rows to insert")
        print(f"Wall: {time.time()-t0:.1f}s")
        return 0

    except Exception as e:
        conn.rollback()
        import traceback
        print(f"FAIL: {e}\n{traceback.format_exc()}")
        return 2
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
