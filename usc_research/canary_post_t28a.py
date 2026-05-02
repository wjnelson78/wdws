"""Post-T28a acceptance gate. First test of unit_type='court_rule' and the
rule_set discriminator. Exits 0 on PASS, 1 on FAIL."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT rule_set, COUNT(*),
               MIN(canonical_citation) AS sample_low,
               MAX(canonical_citation) AS sample_high
        FROM usc.sections
        WHERE title_number = 28 AND unit_type = 'court_rule'
        GROUP BY rule_set ORDER BY rule_set NULLS FIRST
    """)
    rows = cur.fetchall()
    cur.execute("""
        SELECT COUNT(*) FROM usc.sections
        WHERE title_number = 28 AND unit_type = 'court_rule' AND rule_set IS NULL
    """)
    null_rs = cur.fetchone()[0]
conn.close()

print("=== T28a SQL spot check (rule_set discriminator) ===")
print(f"  rule_set | count | sample_low | sample_high")
got = {}
for rule_set, cnt, lo, hi in rows:
    print(f"  {str(rule_set):<8} | {cnt:>5} | {lo:<25} | {hi}")
    got[rule_set] = cnt

problems = []
if null_rs > 0:
    problems.append(f"{null_rs} court_rules with rule_set IS NULL — discriminator failure")

# Expected: FRAP, FRCP, FRE all present with non-zero counts
for required in ("FRAP", "FRCP", "FRE"):
    if got.get(required, 0) == 0:
        problems.append(f"{required} count is zero — wrapper grouping bug")

# Sample-citation prefix sanity
expected_prefixes = {"FRCP": "Fed. R. Civ. P.",
                     "FRAP": "Fed. R. App. P.",
                     "FRE":  "Fed. R. Evid."}
for rule_set, cnt, lo, hi in rows:
    if rule_set in expected_prefixes:
        prefix = expected_prefixes[rule_set]
        if not (lo or "").startswith(prefix):
            problems.append(f"{rule_set}: sample_low {lo!r} does not start with {prefix!r}")

if problems:
    for p in problems:
        print(f"  ✗ {p}")
    print("  RESULT: FAIL — fix discriminator before T18a/T11a")
    sys.exit(1)

print("  RESULT: PASS — three rule_sets correctly classified")

CHECKS = [
    {
        "name":     "FRCP 17 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "court must appoint guardian ad litem to protect minor or incompetent person unrepresented",
        "expected": "Fed. R. Civ. P. 17",
        "max_rank": 3,
    },
    {
        "name":     "FRCP 17 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "Fed. R. Civ. P. 17",
        "max_rank": 1,
    },
    {
        "name":     "FRCP 17 (c) — popular name + tag filter frcp",
        "method":   "hybrid_rrf",
        "tag_filter": "frcp",
        "question": "Rule 17 incompetent person guardian ad litem",
        "expected": "Fed. R. Civ. P. 17",
        "max_rank": 5,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
