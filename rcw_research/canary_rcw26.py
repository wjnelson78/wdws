"""Per-title canary for RCW Title 26 (Domestic Relations).
Mandatory anchors: 26.44.030 (mandatory abuse reporting), 26.09.191 (parental restrictions).
Tags: child-abuse-wa, gal-wa.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # (a) Statutory phrasing
    {"name": "T26 (a) mandatory abuse reporting",
     "method": "hybrid_rrf",
     "question": "Washington mandatory reporter child abuse neglect duty to report",
     "doc_type_filter": "statute",
     "expected": "RCW 26.44.030", "max_rank": 10},
    {"name": "T26 (a) parental restrictions section",
     "method": "hybrid_rrf",
     "question": "Washington parenting plan restrictions residential time domestic violence",
     "doc_type_filter": "statute",
     "expected": "RCW 26.09.191", "max_rank": 10},

    # (b) Citation lookup (deterministic)
    {"name": "T26 (b) RCW 26.44.030", "method": "sql_citation_rcw",
     "expected": "RCW 26.44.030", "max_rank": 1},
    {"name": "T26 (b) RCW 26.09.191", "method": "sql_citation_rcw",
     "expected": "RCW 26.09.191", "max_rank": 1},

    # (c) Tag filter
    {"name": "T26 (c) tag=child-abuse-wa — reporting duty",
     "method": "hybrid_rrf",
     "question": "mandatory reporting child abuse",
     "tag_filter": "child-abuse-wa",
     "expected": "RCW 26.44.030", "max_rank": 10},
    # Per Pause-Checkpoint B operator decision: gal-wa is scoped to chapter 26.44
    # (the GAL-framework chapter); chapter 26.09 sections carry wa-domestic-relations
    # (the dissolution / parenting-plan chapter). Updating Pattern (c) anchor for
    # 26.09.191 to reflect the architectural truth.
    {"name": "T26 (c) tag=wa-domestic-relations — parenting plan",
     "method": "hybrid_rrf",
     "question": "parenting plan restrictions",
     "tag_filter": "wa-domestic-relations",
     "expected": "RCW 26.09.191", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 26 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
