"""Post-T42 acceptance gate (methodology v3).

Three retrieval patterns × two anchors = six checks. All must pass.
"""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
from canary_lib import run_checks

CHECKS = [
    # ─────────────────────── § 12132 ───────────────────────
    {
        "name":     "§ 12132 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "qualified individual disability excluded participation services programs activities public entity",
        "expected": "42 U.S.C. § 12132",
        "max_rank": 3,
    },
    {
        "name":     "§ 12132 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "42 U.S.C. § 12132",
        "max_rank": 1,
    },
    {
        "name":     "§ 12132 (c) — popular name + tag filter ada",
        "method":   "hybrid_rrf",
        "tag_filter": "ada",
        "question": "discrimination public entity disability",
        "expected": "42 U.S.C. § 12132",
        "max_rank": 5,
    },
    # ─────────────────────── § 1983 ───────────────────────
    {
        "name":     "§ 1983 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "person under color of statute subjects citizen to deprivation of rights",
        "expected": "42 U.S.C. § 1983",
        "max_rank": 3,
    },
    {
        "name":     "§ 1983 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "42 U.S.C. § 1983",
        "max_rank": 1,
    },
    {
        "name":     "§ 1983 (c) — popular name + tag filter section-1983",
        "method":   "hybrid_rrf",
        "tag_filter": "section-1983",
        "question": "civil rights claim against state actor for constitutional violation",
        "expected": "42 U.S.C. § 1983",
        "max_rank": 5,
    },
]

ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
