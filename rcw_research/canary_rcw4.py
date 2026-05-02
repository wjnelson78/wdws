"""Per-title canary for RCW Title 4 (Civil Procedure).
Mandatory anchors: 4.16.080 / 4.96.010 / 4.84.185.
Tag pattern (c) filter: wa-civil-procedure.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # (a) Statutory phrasing
    {"name": "T4 (a) statute of limitations 3 years",
     "method": "hybrid_rrf",
     "question": "statute of limitations three years personal injury fraud",
     "doc_type_filter": "statute",
     "expected": "RCW 4.16.080", "max_rank": 10},
    {"name": "T4 (a) claims governmental entities",
     "method": "hybrid_rrf",
     "question": "claims against state local government tort presentment",
     "doc_type_filter": "statute",
     "expected": "RCW 4.96.010", "max_rank": 10},
    {"name": "T4 (a) frivolous action attorney fees",
     "method": "hybrid_rrf",
     "question": "frivolous action attorney fees award",
     "doc_type_filter": "statute",
     "expected": "RCW 4.84.185", "max_rank": 10},

    # (b) Citation lookup
    {"name": "T4 (b) RCW 4.16.080", "method": "sql_citation_rcw",
     "expected": "RCW 4.16.080", "max_rank": 1},
    {"name": "T4 (b) RCW 4.96.010", "method": "sql_citation_rcw",
     "expected": "RCW 4.96.010", "max_rank": 1},
    {"name": "T4 (b) RCW 4.84.185", "method": "sql_citation_rcw",
     "expected": "RCW 4.84.185", "max_rank": 1},

    # (c) Tag-filter popular name
    {"name": "T4 (c) tag=wa-civil-procedure — SOL",
     "method": "hybrid_rrf",
     "question": "statute of limitations actions",
     "tag_filter": "wa-civil-procedure",
     "expected": "RCW 4.16.080", "max_rank": 10},
    {"name": "T4 (c) tag=wa-civil-procedure — government claims",
     "method": "hybrid_rrf",
     "question": "claims local government tort",
     "tag_filter": "wa-civil-procedure",
     "expected": "RCW 4.96.010", "max_rank": 10},
    {"name": "T4 (c) tag=wa-civil-procedure — frivolous fees",
     "method": "hybrid_rrf",
     "question": "frivolous lawsuit attorney fees",
     "tag_filter": "wa-civil-procedure",
     "expected": "RCW 4.84.185", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 4 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
