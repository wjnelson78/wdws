"""Per-title canary for RCW Title 42 (Public Officers / PRA / OMA).
Mandatory anchors: 42.56.030 / .520 / .550 (PRA core) + 42.30.030 (OMA).
Tag pattern (c) filters: pra + oma.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_checks

CHECKS = [
    # ── (a) Statutory phrasing — WA-discriminating queries with PRA/OMA terms ──
    {"name": "T42 (a) PRA construction broad disclosure",
     "method": "hybrid_rrf",
     "question": "Public Records Act liberal construction broad disclosure agencies",
     "doc_type_filter": "statute",
     "expected": "RCW 42.56.030", "max_rank": 10},
    {"name": "T42 (a) PRA five business days response",
     "method": "hybrid_rrf",
     "question": "public records request five business days agency response",
     "doc_type_filter": "statute",
     "expected": "RCW 42.56.520", "max_rank": 10},
    {"name": "T42 (a) PRA judicial review denial",
     "method": "hybrid_rrf",
     "question": "judicial review public records denial superior court attorney fees",
     "doc_type_filter": "statute",
     "expected": "RCW 42.56.550", "max_rank": 10},
    {"name": "T42 (a) OMA definitions",
     "method": "hybrid_rrf",
     "question": "Open Public Meetings Act governing body action definitions",
     "doc_type_filter": "statute",
     "expected": "RCW 42.30.030", "max_rank": 10},

    # ── (b) Citation lookup — deterministic SQL exact-match ──
    {"name": "T42 (b) RCW 42.56.030", "method": "sql_citation_rcw",
     "expected": "RCW 42.56.030", "max_rank": 1},
    {"name": "T42 (b) RCW 42.56.520", "method": "sql_citation_rcw",
     "expected": "RCW 42.56.520", "max_rank": 1},
    {"name": "T42 (b) RCW 42.56.550", "method": "sql_citation_rcw",
     "expected": "RCW 42.56.550", "max_rank": 1},
    {"name": "T42 (b) RCW 42.30.030", "method": "sql_citation_rcw",
     "expected": "RCW 42.30.030", "max_rank": 1},

    # ── (c) Popular-name + tag filter (pra / oma) ──
    {"name": "T42 (c) tag=pra — construction",
     "method": "hybrid_rrf",
     "question": "Public Records Act construction",
     "tag_filter": "pra",
     "expected": "RCW 42.56.030", "max_rank": 10},
    {"name": "T42 (c) tag=pra — response time",
     "method": "hybrid_rrf",
     "question": "response time public records request",
     "tag_filter": "pra",
     "expected": "RCW 42.56.520", "max_rank": 10},
    {"name": "T42 (c) tag=pra — judicial review",
     "method": "hybrid_rrf",
     "question": "judicial review denial",
     "tag_filter": "pra",
     "expected": "RCW 42.56.550", "max_rank": 10},
    {"name": "T42 (c) tag=oma — definitions",
     "method": "hybrid_rrf",
     "question": "open public meetings act",
     "tag_filter": "oma",
     "expected": "RCW 42.30.030", "max_rank": 10},
]

if __name__ == "__main__":
    ok = run_checks(CHECKS, top_k=10)
    print()
    print(f"RCW Title 42 canary: {'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)
