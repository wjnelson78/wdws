"""Per-title canary for RCW Title 41 (Public Employment).
Mandatory anchors: 41.06.150 (state civil service rules), 41.40.010 (PERS).
Tag: wa-public-employment.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    {"name": "T41 (a) civil service rules",
     "method": "hybrid_rrf",
     "question": "Washington state civil service personnel rules classified employees",
     "doc_type_filter": "statute",
     "expected": "RCW 41.06.150", "max_rank": 10},
    {"name": "T41 (a) PERS retirement system",
     "method": "hybrid_rrf",
     "question": "Washington public employees retirement system PERS definitions",
     "doc_type_filter": "statute",
     "expected": "RCW 41.40.010", "max_rank": 10},

    {"name": "T41 (b) RCW 41.06.150", "method": "sql_citation_rcw",
     "expected": "RCW 41.06.150", "max_rank": 1},
    {"name": "T41 (b) RCW 41.40.010", "method": "sql_citation_rcw",
     "expected": "RCW 41.40.010", "max_rank": 1},

    {"name": "T41 (c) tag=wa-public-employment — civil service",
     "method": "hybrid_rrf",
     "question": "civil service rules state employees",
     "tag_filter": "wa-public-employment",
     "expected": "RCW 41.06.150", "max_rank": 10},
    {"name": "T41 (c) tag=wa-public-employment — PERS",
     "method": "hybrid_rrf",
     "question": "public employees retirement",
     "tag_filter": "wa-public-employment",
     "expected": "RCW 41.40.010", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 41 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
