"""Post-T28 acceptance gate. Exits 0 on PASS, 1 on FAIL."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

# 1. SQL spot check — section count
conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT COUNT(*) FROM usc.sections
        WHERE title_number = 28 AND unit_type = 'section'
    """)
    n_sections = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM core.document_chunks c
        JOIN core.documents d ON c.document_id = d.id
        JOIN usc.sections s ON s.document_id = d.id
        WHERE s.title_number = 28 AND c.embedding IS NOT NULL
    """)
    n_embedded = cur.fetchone()[0]
conn.close()

print(f"=== T28 SQL spot check ===")
print(f"  usc.sections (title=28, unit_type='section'): {n_sections}")
print(f"  embedded chunks for T28: {n_embedded}")
if n_sections < 800 or n_sections > 950:
    print(f"  RESULT: FAIL — section count out of expected band [800, 950]")
    sys.exit(1)
if n_embedded < n_sections:
    print(f"  RESULT: FAIL — embeddings incomplete ({n_embedded} < {n_sections})")
    sys.exit(1)
print(f"  RESULT: PASS")

CHECKS = [
    {
        "name":     "§ 1331 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "district courts original jurisdiction civil actions arising under Constitution laws treaties",
        "expected": "28 U.S.C. § 1331",
        "max_rank": 3,
    },
    {
        "name":     "§ 1331 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "28 U.S.C. § 1331",
        "max_rank": 1,
    },
    {
        "name":     "§ 1331 (c) — popular name + tag filter jurisdiction-fed",
        "method":   "hybrid_rrf",
        "tag_filter": "jurisdiction-fed",
        "question": "federal question jurisdiction",
        "expected": "28 U.S.C. § 1331",
        "max_rank": 5,
    },
]
ok = run_checks(CHECKS)
sys.exit(0 if ok else 1)
