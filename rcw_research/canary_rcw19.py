"""Per-title canary for RCW Title 19 (CPA — Consumer Protection Act).
Mandatory anchors: 19.86.020 (prohibited acts), 19.86.090 (private right of action).
Tag: cpa-wa.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    {"name": "T19 (a) CPA prohibited acts",
     "method": "hybrid_rrf",
     "question": "Washington Consumer Protection Act unfair deceptive acts trade commerce",
     "doc_type_filter": "statute",
     "expected": "RCW 19.86.020", "max_rank": 10},
    {"name": "T19 (a) CPA private right of action",
     "method": "hybrid_rrf",
     "question": "Washington Consumer Protection Act private right action treble damages",
     "doc_type_filter": "statute",
     "expected": "RCW 19.86.090", "max_rank": 10},

    {"name": "T19 (b) RCW 19.86.020", "method": "sql_citation_rcw",
     "expected": "RCW 19.86.020", "max_rank": 1},
    {"name": "T19 (b) RCW 19.86.090", "method": "sql_citation_rcw",
     "expected": "RCW 19.86.090", "max_rank": 1},

    {"name": "T19 (c) tag=cpa-wa — prohibited acts",
     "method": "hybrid_rrf",
     "question": "unfair deceptive acts",
     "tag_filter": "cpa-wa",
     "expected": "RCW 19.86.020", "max_rank": 10},
    {"name": "T19 (c) tag=cpa-wa — private action",
     "method": "hybrid_rrf",
     "question": "private right of action damages",
     "tag_filter": "cpa-wa",
     "expected": "RCW 19.86.090", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 19 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
