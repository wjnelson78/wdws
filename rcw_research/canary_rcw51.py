"""Per-title canary for RCW Title 51 (Industrial Insurance) — methodology v3.

Three retrieval patterns × 3 mandatory litigation anchors:
  (a) statutory phrasing — hybrid_rrf with doc_type_filter='statute'
  (b) citation lookup    — SQL exact-match on rcw.sections.canonical_citation
  (c) popular-name+tag   — hybrid_rrf with tag_filter='bii'

Mandatory anchors (per Phase 4 authorization):
  RCW 51.32.040  — assignment of compensation, attachment
  RCW 51.52.060  — BIIA appeals procedure
  RCW 51.52.115  — BIIA judicial review (superior court hook)
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # ── (a) Statutory phrasing — semantic question that SHOULD surface anchor ──
    {
        "name": "T51 (a) statutory phrasing — assignment / attachment of comp",
        "method": "hybrid_rrf",
        "question": "industrial insurance compensation assignment attachment workers",
        "doc_type_filter": "statute",
        "expected": "RCW 51.32.040",
        "max_rank": 10,
    },
    {
        "name": "T51 (a) statutory phrasing — BIIA appeals procedure",
        "method": "hybrid_rrf",
        "question": "Board of Industrial Insurance Appeals procedure notice service",
        "doc_type_filter": "statute",
        "expected": "RCW 51.52.060",
        "max_rank": 10,
    },
    {
        "name": "T51 (a) statutory phrasing — judicial review superior court",
        "method": "hybrid_rrf",
        "question": "industrial insurance judicial review superior court appeal",
        "doc_type_filter": "statute",
        "expected": "RCW 51.52.115",
        "max_rank": 10,
    },

    # ── (b) Citation lookup — deterministic SQL exact-match ──
    {
        "name": "T51 (b) citation lookup — RCW 51.32.040",
        "method": "sql_citation_rcw",
        "expected": "RCW 51.32.040",
        "max_rank": 1,
    },
    {
        "name": "T51 (b) citation lookup — RCW 51.52.060",
        "method": "sql_citation_rcw",
        "expected": "RCW 51.52.060",
        "max_rank": 1,
    },
    {
        "name": "T51 (b) citation lookup — RCW 51.52.115",
        "method": "sql_citation_rcw",
        "expected": "RCW 51.52.115",
        "max_rank": 1,
    },

    # ── (c) Popular name + tag filter (bii) — strict_order iterative_scan ──
    {
        "name": "T51 (c) tag=bii — assignment of comp",
        "method": "hybrid_rrf",
        "question": "assignment of compensation workers comp",
        "tag_filter": "bii",
        "expected": "RCW 51.32.040",
        "max_rank": 10,
    },
    {
        "name": "T51 (c) tag=bii — BIIA appeals procedure",
        "method": "hybrid_rrf",
        "question": "BIIA notice of appeal procedure",
        "tag_filter": "bii",
        "expected": "RCW 51.52.060",
        "max_rank": 10,
    },
    {
        "name": "T51 (c) tag=bii — judicial review",
        "method": "hybrid_rrf",
        "question": "judicial review appeal superior court",
        "tag_filter": "bii",
        "expected": "RCW 51.52.115",
        "max_rank": 10,
    },
]


if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print("=" * 70)
    print(f"RCW Title 51 canary: {'PASS' if ok else 'FAIL'}")
    print("=" * 70)
    sys.exit(0 if ok else 1)
