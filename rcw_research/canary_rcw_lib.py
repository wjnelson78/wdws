"""
RCW canary helpers — extends /opt/wdws/usc_research/canary_lib.py with an
RCW-aware SQL citation lookup.

The hybrid_rrf and run_set_check helpers from canary_lib are corpus-agnostic
(they query core.documents directly), so we re-export them as-is. Only
sql_citation_lookup needs an RCW-specific variant since it queries the
schema-specific sections table.
"""
from __future__ import annotations
import sys
sys.path.insert(0, "/opt/wdws"); sys.path.insert(0, "/opt/wdws/mcp-server")
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2

from canary_lib import (
    DSN, hybrid_rrf, cite_match, run_set_check,
    _ensure_iterative_scan, _resolve_tag_to_slug,
    sql_citation_lookup as usc_sql_citation_lookup,
)


def sql_citation_lookup_rcw(conn, *, citation: str) -> list[dict]:
    """Deterministic RCW citation lookup — pattern (b)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.title
            FROM rcw.sections s
            JOIN core.documents d ON s.document_id = d.id
            WHERE s.canonical_citation = %s
            LIMIT 1
        """, [citation])
        row = cur.fetchone()
    if not row:
        return []
    return [{"doc_id": str(row[0]), "title": row[1], "rrf_score": 1.0}]


def sql_citation_lookup_either(conn, *, citation: str) -> list[dict]:
    """Try RCW first, then USC. Useful when the canary check doesn't pre-know
    which corpus the citation belongs to."""
    rs = sql_citation_lookup_rcw(conn, citation=citation)
    if rs:
        return rs
    return usc_sql_citation_lookup(conn, citation=citation)


def _print_check_header(chk: dict) -> None:
    name = chk["name"]
    method = chk.get("method", "hybrid_rrf")
    print(f"\n=== {name}  [method={method}] ===")
    if chk.get("question"):
        print(f"Q: {chk['question']!r}")
    if chk.get("tag_filter"):
        print(f"Tag filter: {chk['tag_filter']!r}")
    if chk.get("doc_type_filter"):
        print(f"Doc-type filter: {chk['doc_type_filter']!r}")
    print(f"Expected: {chk['expected']!r} in top-{chk['max_rank']}")


def run_checks(checks: list[dict], *, top_k: int = 10) -> bool:
    """RCW-aware run_checks — recognizes method='sql_citation_rcw'."""
    conn = psycopg2.connect(DSN); conn.autocommit = True
    overall = True
    for chk in checks:
        _print_check_header(chk)
        method = chk.get("method", "hybrid_rrf")
        eff_top_k = max(top_k, chk["max_rank"])
        try:
            if method == "sql_citation_rcw":
                results = sql_citation_lookup_rcw(conn, citation=chk["expected"])
            elif method == "sql_citation_usc":
                results = usc_sql_citation_lookup(conn, citation=chk["expected"])
            elif method == "sql_citation":
                results = sql_citation_lookup_either(conn, citation=chk["expected"])
            elif method == "hybrid_rrf":
                results = hybrid_rrf(
                    conn, question=chk["question"], top_k=eff_top_k,
                    domain=chk.get("domain", "legal"),
                    tag_filter=chk.get("tag_filter"),
                    doc_type_filter=chk.get("doc_type_filter"),
                )
            else:
                print(f"  RESULT: FAIL — unknown method {method!r}")
                overall = False
                continue
        except Exception as e:
            print(f"  RESULT: FAIL — exception {type(e).__name__}: {e}")
            overall = False
            continue

        found = None
        for i, r in enumerate(results):
            ok = cite_match(chk["expected"], r["title"])
            if ok and found is None:
                found = i
            mark = "  ★ MATCH" if ok else ""
            score_label = "rrf" if method == "hybrid_rrf" else "score"
            print(f"  rank={i+1} {score_label}={r['rrf_score']:.6f}  {r['title'][:90]}{mark}")
        if not results:
            print(f"  (no results)")
        if found is None:
            print(f"  RESULT: FAIL — {chk['expected']!r} not in top {eff_top_k}")
            overall = False
        elif found + 1 > chk["max_rank"]:
            print(f"  RESULT: FAIL — at rank {found+1}, max {chk['max_rank']}")
            overall = False
        else:
            print(f"  RESULT: PASS — rank {found+1}")
    conn.close()
    return overall
