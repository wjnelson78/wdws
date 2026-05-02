"""Post-T11 canary. Three patterns × 3 anchors. Title row baseline check."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# Title 11 row baseline (pre-T11a check; should be is_positive_law=TRUE, has_appendix=TRUE)
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, is_positive_law, has_appendix, section_count
        FROM usc.titles WHERE title_number = 11
    """)
    row = cur.fetchone()
conn.close()
print(f"=== T11 title row pre-T11a baseline ===")
print(f"  title=11 is_positive_law={row[1]} has_appendix={row[2]} section_count={row[3]}")

CHECKS = [
    # § 362 — Automatic stay
    {
        "name":     "§ 362 (a) — automatic stay statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "filing petition operates stay applicable to all entities action collection enforcement",
        "expected": "11 U.S.C. § 362",
        "max_rank": 3,
    },
    {
        "name":     "§ 362 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "11 U.S.C. § 362",
        "max_rank": 1,
    },
    {
        "name":     "§ 362 (c) — popular name + tag bankruptcy",
        "method":   "hybrid_rrf",
        "tag_filter": "bankruptcy",
        "question": "automatic stay bankruptcy filing collection",
        "expected": "11 U.S.C. § 362",
        "max_rank": 5,
    },
    # § 523 — Exceptions to discharge
    {
        "name":     "§ 523 (a) — exceptions to discharge statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "discharge does not discharge individual debtor debt for taxes fraud willful malicious",
        "expected": "11 U.S.C. § 523",
        "max_rank": 3,
    },
    {
        "name":     "§ 523 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "11 U.S.C. § 523",
        "max_rank": 1,
    },
    {
        "name":     "§ 523 (c) — popular name + tag bankruptcy",
        "method":   "hybrid_rrf",
        "tag_filter": "bankruptcy",
        "question": "exceptions to discharge nondischargeable debts",
        "expected": "11 U.S.C. § 523",
        "max_rank": 5,
    },
    # § 727 — Discharge
    {
        "name":     "§ 727 (a) — discharge statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "court shall grant debtor discharge unless conceal transferred destroyed records",
        "expected": "11 U.S.C. § 727",
        "max_rank": 3,
    },
    {
        "name":     "§ 727 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "11 U.S.C. § 727",
        "max_rank": 1,
    },
    {
        "name":     "§ 727 (c) — popular name + tag bankruptcy",
        "method":   "hybrid_rrf",
        "tag_filter": "bankruptcy",
        "question": "Chapter 7 discharge debtor",
        "expected": "11 U.S.C. § 727",
        "max_rank": 5,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
