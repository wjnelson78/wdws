"""Per-title canary for RCW Title 2 (Courts of Record).
Mandatory anchors: 2.04.190 (Supreme Court rule-making power), 2.36.080 (juries).
Tag: wa-courts.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    {"name": "T2 (a) Supreme Court rule-making power",
     "method": "hybrid_rrf",
     "question": "Washington Supreme Court rule-making power practice procedure",
     "doc_type_filter": "statute",
     "expected": "RCW 2.04.190", "max_rank": 10},
    {"name": "T2 (a) juries",
     "method": "hybrid_rrf",
     "question": "Washington juries selection summons jurors",
     "doc_type_filter": "statute",
     "expected": "RCW 2.36.080", "max_rank": 10},

    {"name": "T2 (b) RCW 2.04.190", "method": "sql_citation_rcw",
     "expected": "RCW 2.04.190", "max_rank": 1},
    {"name": "T2 (b) RCW 2.36.080", "method": "sql_citation_rcw",
     "expected": "RCW 2.36.080", "max_rank": 1},

    {"name": "T2 (c) tag=wa-courts — rule-making power",
     "method": "hybrid_rrf",
     "question": "Supreme Court rule-making",
     "tag_filter": "wa-courts",
     "expected": "RCW 2.04.190", "max_rank": 10},
    {"name": "T2 (c) tag=wa-courts — juries",
     "method": "hybrid_rrf",
     "question": "juries selection",
     "tag_filter": "wa-courts",
     "expected": "RCW 2.36.080", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 2 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
