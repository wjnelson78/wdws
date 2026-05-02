"""Combined per-title canary for the criminal trio (RCW 9 + 9A + 10).
Operator-specified chapter-level anchors with representative sections:
  9A.36 (assault)            → 9A.36.011  (Assault in the first degree)
  9A.46 (harassment)         → 9A.46.020  (Harassment — definition / penalty)
  10.99 (domestic violence)  → 10.99.020  (DV — definitions)
Tag: wa-criminal-procedure.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # 9A.36 — Assault
    {"name": "T9A (a) Assault first degree",
     "method": "hybrid_rrf",
     "question": "Washington assault first degree great bodily harm intent",
     "doc_type_filter": "statute",
     "expected": "RCW 9A.36.011", "max_rank": 10},
    {"name": "T9A (b) RCW 9A.36.011", "method": "sql_citation_rcw",
     "expected": "RCW 9A.36.011", "max_rank": 1},
    {"name": "T9A (c) tag=wa-criminal-procedure — assault",
     "method": "hybrid_rrf",
     "question": "assault first degree",
     "tag_filter": "wa-criminal-procedure",
     "expected": "RCW 9A.36.011", "max_rank": 10},

    # 9A.46 — Harassment
    {"name": "T9A (a) Harassment definition",
     "method": "hybrid_rrf",
     "question": "Washington criminal harassment threats fear penalty",
     "doc_type_filter": "statute",
     "expected": "RCW 9A.46.020", "max_rank": 10},
    {"name": "T9A (b) RCW 9A.46.020", "method": "sql_citation_rcw",
     "expected": "RCW 9A.46.020", "max_rank": 1},
    {"name": "T9A (c) tag=wa-criminal-procedure — harassment",
     "method": "hybrid_rrf",
     "question": "harassment definition penalty",
     "tag_filter": "wa-criminal-procedure",
     "expected": "RCW 9A.46.020", "max_rank": 10},

    # 10.99 — Domestic Violence
    {"name": "T10 (a) DV definitions",
     "method": "hybrid_rrf",
     "question": "Washington domestic violence definitions intimate partner",
     "doc_type_filter": "statute",
     "expected": "RCW 10.99.020", "max_rank": 10},
    {"name": "T10 (b) RCW 10.99.020", "method": "sql_citation_rcw",
     "expected": "RCW 10.99.020", "max_rank": 1},
    {"name": "T10 (c) tag=wa-criminal-procedure — DV",
     "method": "hybrid_rrf",
     "question": "domestic violence",
     "tag_filter": "wa-criminal-procedure",
     "expected": "RCW 10.99.020", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW 9/9A/10 (criminal trio) canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
