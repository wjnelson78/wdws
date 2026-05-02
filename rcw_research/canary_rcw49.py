"""Per-title canary for RCW Title 49 (Labor — WLAD).
Mandatory anchors: 49.60.030 / .180 / .215.
Tag pattern (c) filter: wlad.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # (a) Statutory phrasing
    {"name": "T49 (a) WLAD right of equality",
     "method": "hybrid_rrf",
     "question": "Washington civil rights freedom from discrimination protected classes",
     "doc_type_filter": "statute",
     "expected": "RCW 49.60.030", "max_rank": 10},
    {"name": "T49 (a) WLAD employment discrimination",
     "method": "hybrid_rrf",
     "question": "unfair practice employer discrimination employment refusing to hire",
     "doc_type_filter": "statute",
     "expected": "RCW 49.60.180", "max_rank": 10},
    {"name": "T49 (a) WLAD public accommodation",
     "method": "hybrid_rrf",
     "question": "place of public accommodation discrimination service refusal",
     "doc_type_filter": "statute",
     "expected": "RCW 49.60.215", "max_rank": 10},

    # (b) Citation lookup
    {"name": "T49 (b) RCW 49.60.030", "method": "sql_citation_rcw",
     "expected": "RCW 49.60.030", "max_rank": 1},
    {"name": "T49 (b) RCW 49.60.180", "method": "sql_citation_rcw",
     "expected": "RCW 49.60.180", "max_rank": 1},
    {"name": "T49 (b) RCW 49.60.215", "method": "sql_citation_rcw",
     "expected": "RCW 49.60.215", "max_rank": 1},

    # (c) Tag-filter popular name
    {"name": "T49 (c) tag=wlad — civil rights",
     "method": "hybrid_rrf",
     "question": "civil rights freedom from discrimination",
     "tag_filter": "wlad",
     "expected": "RCW 49.60.030", "max_rank": 10},
    {"name": "T49 (c) tag=wlad — employment discrimination",
     "method": "hybrid_rrf",
     "question": "unfair employment practice discrimination",
     "tag_filter": "wlad",
     "expected": "RCW 49.60.180", "max_rank": 10},
    {"name": "T49 (c) tag=wlad — public accommodation",
     "method": "hybrid_rrf",
     "question": "public accommodation discrimination",
     "tag_filter": "wlad",
     "expected": "RCW 49.60.215", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 49 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
