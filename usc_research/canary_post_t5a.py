"""Post-T5A canary. Reorg plan + ancillary section + T5 appendix-row regression."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# T5 appendix-row regression test — verify T5A ingest didn't corrupt the T5 row
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, is_positive_law, has_appendix, section_count
        FROM usc.titles WHERE title_number = 5
    """)
    row = cur.fetchone()
    cur.execute("""
        SELECT unit_type, COUNT(*),
               MIN(canonical_citation), MAX(canonical_citation)
        FROM usc.sections
        WHERE title_number = 5
        GROUP BY unit_type ORDER BY unit_type
    """)
    unit_breakdown = cur.fetchall()
conn.close()

print("=== T5 appendix-row regression test (post-T5A) ===")
print(f"  title=5 is_positive_law={row[1]} has_appendix={row[2]} section_count={row[3]}")
expected_positive = True
expected_has_appendix = True
problems = []
if row[1] != expected_positive:
    problems.append(f"is_positive_law={row[1]} (expected {expected_positive})")
if row[2] != expected_has_appendix:
    problems.append(f"has_appendix={row[2]} (expected {expected_has_appendix})")
if problems:
    print(f"  REGRESSION: {problems}")
    sys.exit(1)
print(f"  RESULT: PASS — title row preserved through T5A appendix ingest")

print("\n=== T5 unit-type breakdown ===")
for ut, cnt, lo, hi in unit_breakdown:
    print(f"  {ut:<22} count={cnt:>5}  range: {lo} ... {hi}")

CHECKS = [
    # Reorganization Plan canary
    {
        "name":     "Reorg Plan III/1970 (a) — statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "Reorganization Plan executive branch transfer of functions",
        "expected": "Reorg. Plan No. III of 1970",
        "max_rank": 5,   # widened — multiple reorg plans compete on this query
    },
    {
        "name":     "Reorg Plan III/1970 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Reorg. Plan No. III of 1970",
        "max_rank": 1,
    },
    # Ancillary statutory section — try APA-related from T5A (5 USC App. § 1)
    {
        "name":     "5 U.S.C. App. § 1 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "5 U.S.C. App. § 1",
        "max_rank": 1,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
