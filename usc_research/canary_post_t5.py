"""Post-T5 canary. Three patterns × 3 anchors + appendix-row baseline check."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# Baseline check on the title row before T5A runs (pre-fix sanity).
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, is_positive_law, has_appendix, section_count
        FROM usc.titles WHERE title_number = 5
    """)
    row = cur.fetchone()
conn.close()
print(f"=== T5 title row pre-T5A baseline ===")
print(f"  title=5 is_positive_law={row[1]} has_appendix={row[2]} section_count={row[3]}")
print(f"  Expected: is_positive_law=True, has_appendix=True (T5 IS positive law and has T5A appendix)")
if not row[1]:
    print(f"  WARN: T5 is_positive_law=FALSE — should be TRUE per POSITIVE_LAW_TITLES list")
if not row[2]:
    print(f"  WARN: T5 has_appendix=FALSE — should be TRUE (T5A appendix exists)")

CHECKS = [
    # § 552 (FOIA)
    {
        "name":     "§ 552 (a) — FOIA statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "agency public records request availability records reasonably described",
        "expected": "5 U.S.C. § 552",
        "max_rank": 3,
    },
    {
        "name":     "§ 552 (b) — FOIA citation lookup",
        "method":   "sql_citation",
        "expected": "5 U.S.C. § 552",
        "max_rank": 1,
    },
    {
        "name":     "§ 552 (c) — FOIA popular name + tag filter apa",
        "method":   "hybrid_rrf",
        "tag_filter": "apa",
        "question": "Freedom of Information Act agency records public",
        "expected": "5 U.S.C. § 552",
        "max_rank": 5,
    },
    # § 552a (Privacy Act)
    {
        "name":     "§ 552a (a) — Privacy Act statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "system of records personal information disclosure individual consent",
        "expected": "5 U.S.C. § 552a",
        "max_rank": 3,
    },
    {
        "name":     "§ 552a (b) — Privacy Act citation lookup",
        "method":   "sql_citation",
        "expected": "5 U.S.C. § 552a",
        "max_rank": 1,
    },
    {
        "name":     "§ 552a (c) — Privacy Act popular name + tag filter apa",
        "method":   "hybrid_rrf",
        "tag_filter": "apa",
        "question": "Privacy Act records system personal information federal agency",
        "expected": "5 U.S.C. § 552a",
        "max_rank": 5,
    },
    # § 2302 (Prohibited personnel practices)
    {
        "name":     "§ 2302 (a) — prohibited personnel practices statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "personnel action discrimination retaliation whistleblower employee",
        "expected": "5 U.S.C. § 2302",
        "max_rank": 3,
    },
    {
        "name":     "§ 2302 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "5 U.S.C. § 2302",
        "max_rank": 1,
    },
    # No tag-filtered (c) for §2302 — it's in T5 Chapter 23 (Merit System), no current legal-framework tag
    # covers it cleanly. Citation lookup + statutory phrasing are the right paths for this section.
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
