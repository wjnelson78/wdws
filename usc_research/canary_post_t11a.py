"""Post-T11a canary. FRBP rule discriminator + 2 anchors. T11 appendix-row regression.
Plus combined FRCrP+FRBP rule_set discriminator check (operator's spec §6 query)."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# T11 appendix-row regression test
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, is_positive_law, has_appendix, section_count
        FROM usc.titles WHERE title_number = 11
    """)
    row = cur.fetchone()
conn.close()
print("=== T11 appendix-row regression test (post-T11a) ===")
print(f"  title=11 is_positive_law={row[1]} has_appendix={row[2]} section_count={row[3]}")
problems = []
if row[1] != True:  problems.append(f"is_positive_law={row[1]} (expected TRUE)")
if row[2] != True:  problems.append(f"has_appendix={row[2]} (expected TRUE)")
if problems:
    print(f"  REGRESSION: {problems}")
    sys.exit(1)
print(f"  RESULT: PASS")

# rule_set discriminator — combined T18a + T11a per operator spec
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT title_number, rule_set, COUNT(*),
               MIN(canonical_citation), MAX(canonical_citation)
        FROM usc.sections
        WHERE title_number IN (18, 11) AND unit_type = 'court_rule'
        GROUP BY title_number, rule_set
        ORDER BY title_number, rule_set NULLS FIRST
    """)
    rows = cur.fetchall()
    cur.execute("""
        SELECT COUNT(*) FROM usc.sections
        WHERE title_number IN (18, 11) AND unit_type = 'court_rule' AND rule_set IS NULL
    """)
    null_rs = cur.fetchone()[0]
conn.close()
print("\n=== Combined rule_set discriminator (T18a + T11a) ===")
for tn, rs, cnt, lo, hi in rows:
    print(f"  title={tn}  {str(rs):<8} count={cnt:>4}  range: {lo} ... {hi}")
if null_rs > 0:
    print(f"  REGRESSION: {null_rs} court_rules with rule_set IS NULL")
    sys.exit(1)
got_rule_sets = {rs for _, rs, *_ in rows}
expected = {"FRCrP", "FRBP"}
missing = expected - got_rule_sets
if missing:
    print(f"  REGRESSION: missing rule_sets {missing}")
    sys.exit(1)
print(f"  RESULT: PASS — both FRCrP and FRBP correctly classified")

CHECKS = [
    # FRBP 7001 — Adversary proceedings
    {
        "name":     "FRBP 7001 (a) — adversary proceedings statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "adversary proceeding bankruptcy court complaint recover money property",
        "expected": "Fed. R. Bankr. P. 7001",
        "max_rank": 3,
    },
    {
        "name":     "FRBP 7001 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Bankr. P. 7001",
        "max_rank": 1,
    },
    {
        "name":     "FRBP 7001 (c) — popular name + tag frbp",
        "method":   "hybrid_rrf",
        "tag_filter": "frbp",
        "question": "Rule 7001 adversary proceeding bankruptcy",
        "expected": "Fed. R. Bankr. P. 7001",
        "max_rank": 5,
    },
    # FRBP 9011 — Sanctions (FRCP 11 analog)
    {
        "name":     "FRBP 9011 (a) — sanctions statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "signing pleading certifies frivolous improper purpose sanctions",
        "expected": "Fed. R. Bankr. P. 9011",
        "max_rank": 3,
    },
    {
        "name":     "FRBP 9011 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Bankr. P. 9011",
        "max_rank": 1,
    },
    {
        "name":     "FRBP 9011 (c) — popular name + tag frbp",
        "method":   "hybrid_rrf",
        "tag_filter": "frbp",
        "question": "Rule 9011 sanctions signing pleading bankruptcy",
        "expected": "Fed. R. Bankr. P. 9011",
        "max_rank": 5,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
