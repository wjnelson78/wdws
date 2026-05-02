"""Per-title canary for RCW Title 7 (Special Proceedings — Writs).

Anchors corrected per Pause-Checkpoint B operator decision (kickoff anchor
table conflated which sections were mandamus- vs certiorari-specific):

  RCW 7.16.040  — Grounds for granting writ  (general — covers all 3 writs)
  RCW 7.16.150  — Mandamus defined           (mandamus-specific)
  RCW 7.16.290  — Prohibition defined        (added per operator)

Tag: writs-wa (46 sections corpus-wide, 36 in Title 7).
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # Pattern (a) — statutory phrasing (cross-jurisdiction-competition documented limitation)
    {"name": "T7 (a) general grounds for writ",
     "method": "hybrid_rrf",
     "question": "Washington statutory grounds for granting writ certiorari mandamus prohibition",
     "doc_type_filter": "statute",
     "expected": "RCW 7.16.040", "max_rank": 10},
    {"name": "T7 (a) mandamus defined",
     "method": "hybrid_rrf",
     "question": "Washington statutory definition mandamus compel public officer act",
     "doc_type_filter": "statute",
     "expected": "RCW 7.16.150", "max_rank": 10},
    {"name": "T7 (a) prohibition defined",
     "method": "hybrid_rrf",
     "question": "Washington statutory definition prohibition arrest tribunal proceedings",
     "doc_type_filter": "statute",
     "expected": "RCW 7.16.290", "max_rank": 10},

    # Pattern (b) — citation lookup (deterministic)
    {"name": "T7 (b) RCW 7.16.040", "method": "sql_citation_rcw",
     "expected": "RCW 7.16.040", "max_rank": 1},
    {"name": "T7 (b) RCW 7.16.150", "method": "sql_citation_rcw",
     "expected": "RCW 7.16.150", "max_rank": 1},
    {"name": "T7 (b) RCW 7.16.290", "method": "sql_citation_rcw",
     "expected": "RCW 7.16.290", "max_rank": 1},

    # Pattern (c) — tag filter (scoped pool of 46 sections)
    {"name": "T7 (c) tag=writs-wa — general grounds",
     "method": "hybrid_rrf",
     "question": "grounds for granting writ",
     "tag_filter": "writs-wa",
     "expected": "RCW 7.16.040", "max_rank": 10},
    {"name": "T7 (c) tag=writs-wa — mandamus",
     "method": "hybrid_rrf",
     "question": "writ of mandamus",
     "tag_filter": "writs-wa",
     "expected": "RCW 7.16.150", "max_rank": 10},
    {"name": "T7 (c) tag=writs-wa — prohibition",
     "method": "hybrid_rrf",
     "question": "writ of prohibition",
     "tag_filter": "writs-wa",
     "expected": "RCW 7.16.290", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 7 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
