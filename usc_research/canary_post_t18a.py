"""Post-T18a canary. FRCrP rule discriminator + 3 anchors. T18 appendix-row regression."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# T18 appendix-row regression test
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, is_positive_law, has_appendix, section_count
        FROM usc.titles WHERE title_number = 18
    """)
    row = cur.fetchone()
conn.close()
print("=== T18 appendix-row regression test (post-T18a) ===")
print(f"  title=18 is_positive_law={row[1]} has_appendix={row[2]} section_count={row[3]}")
problems = []
if row[1] != True:  problems.append(f"is_positive_law={row[1]} (expected TRUE)")
if row[2] != True:  problems.append(f"has_appendix={row[2]} (expected TRUE)")
if problems:
    print(f"  REGRESSION: {problems}")
    sys.exit(1)
print(f"  RESULT: PASS")

# rule_set discriminator SQL spot check
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT rule_set, COUNT(*),
               MIN(canonical_citation), MAX(canonical_citation)
        FROM usc.sections
        WHERE title_number = 18 AND unit_type = 'court_rule'
        GROUP BY rule_set ORDER BY rule_set NULLS FIRST
    """)
    rs_rows = cur.fetchall()
    cur.execute("""
        SELECT COUNT(*) FROM usc.sections
        WHERE title_number = 18 AND unit_type = 'court_rule' AND rule_set IS NULL
    """)
    null_rs = cur.fetchone()[0]
conn.close()
print("\n=== T18a rule_set discriminator ===")
for rs, cnt, lo, hi in rs_rows:
    print(f"  {str(rs):<8} count={cnt:>4}  range: {lo} ... {hi}")
if null_rs > 0:
    print(f"  REGRESSION: {null_rs} court_rules with rule_set IS NULL")
    sys.exit(1)
expected = "FRCrP"
got = {rs for rs, *_ in rs_rows}
if expected not in got:
    print(f"  REGRESSION: expected rule_set {expected!r} not present (got {got})")
    sys.exit(1)
print(f"  RESULT: PASS — FRCrP correctly classified")

CHECKS = [
    # FRCrP 11 — Plea
    {
        "name":     "FRCrP 11 (a) — plea statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "guilty plea defendant knowingly voluntarily waiver constitutional rights",
        "expected": "Fed. R. Crim. P. 11",
        "max_rank": 3,
    },
    {
        "name":     "FRCrP 11 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Crim. P. 11",
        "max_rank": 1,
    },
    {
        "name":     "FRCrP 11 (c) — popular name + tag frcrp",
        "method":   "hybrid_rrf",
        "tag_filter": "frcrp",
        "question": "Rule 11 plea agreement guilty",
        "expected": "Fed. R. Crim. P. 11",
        "max_rank": 5,
    },
    # FRCrP 16 — Discovery
    {
        "name":     "FRCrP 16 (a) — discovery statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "discovery and inspection government disclosure defendant statements",
        "expected": "Fed. R. Crim. P. 16",
        "max_rank": 3,
    },
    {
        "name":     "FRCrP 16 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Crim. P. 16",
        "max_rank": 1,
    },
    {
        "name":     "FRCrP 16 (c) — popular name + tag frcrp",
        "method":   "hybrid_rrf",
        "tag_filter": "frcrp",
        "question": "Rule 16 criminal discovery government documents",
        "expected": "Fed. R. Crim. P. 16",
        "max_rank": 5,
    },
    # FRCrP 41 — Search and Seizure
    {
        "name":     "FRCrP 41 (a) — search and seizure statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "search warrant probable cause magistrate judge property seizure",
        "expected": "Fed. R. Crim. P. 41",
        "max_rank": 3,
    },
    {
        "name":     "FRCrP 41 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Crim. P. 41",
        "max_rank": 1,
    },
    {
        "name":     "FRCrP 41 (c) — popular name + tag frcrp",
        "method":   "hybrid_rrf",
        "tag_filter": "frcrp",
        "question": "Rule 41 search warrant seizure",
        "expected": "Fed. R. Crim. P. 41",
        "max_rank": 5,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
