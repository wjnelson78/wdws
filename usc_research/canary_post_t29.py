"""Post-T29 PAUSE-CHECKPOINT canary suite. Full litigation-priority validation
across T42+T28+T28a+T29. Exits 0 on PASS, 1 on FAIL.

Per operator pause-checkpoint spec:
  1. Cross-domain RAG: Rehab Act + ADA Title II in same result set
  2. FRCP appendix retrieval (cross-title still works)
  3. ERISA spot check: 29 U.S.C. § 1104 in top-3
  4. 10-sample OLRC round-trip (run separately — content-fidelity)
  5. Cumulative numbers across T42+T28+T28a+T29
"""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, run_set_check, DSN

conn = psycopg2.connect(DSN); conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("""
        SELECT s.title_number,
               COUNT(DISTINCT s.id) AS sections,
               COUNT(DISTINCT s.id) FILTER (WHERE s.unit_type='court_rule')   AS court_rules,
               COUNT(DISTINCT c.id) AS chunks,
               COUNT(DISTINCT c.id) FILTER (WHERE c.embedding IS NOT NULL)    AS embedded,
               COUNT(DISTINCT xr.id)                                          AS xrefs
        FROM usc.sections s
        JOIN core.documents d ON s.document_id = d.id
        LEFT JOIN core.document_chunks c ON c.document_id = d.id
        LEFT JOIN usc.cross_references xr ON xr.source_section_id = s.id
        WHERE s.title_number IN (28, 29, 42)
        GROUP BY s.title_number ORDER BY s.title_number
    """)
    rows = cur.fetchall()
print("=== Cumulative T42 + T28 + T28a + T29 ===")
print(f"  title | sections | court_rules | chunks  | embedded | xrefs")
totals = [0, 0, 0, 0, 0]
for tn, sec, cr, ch, em, xr in rows:
    print(f"  {tn:>5} | {sec:>8} | {cr:>11} | {ch:>7} | {em:>8} | {xr}")
    totals[0] += sec; totals[1] += cr; totals[2] += ch; totals[3] += em; totals[4] += xr
print(f"  TOTAL | {totals[0]:>8} | {totals[1]:>11} | {totals[2]:>7} | {totals[3]:>8} | {totals[4]}")
conn.close()

# Cross-domain co-retrieval — restructured per operator direction. The
# original "same standard" comparative meta-query was correctly surfacing
# §12201 (ADA construction) etc., not the foundational sections. The actual
# acceptance goal is "researcher can find each foundational section through
# its own natural-language query." Two independent checks scoped to statutes:
SET_CHECK = None  # legacy single-set form removed
CROSS_DOMAIN_CHECKS = [
    {
        "name":     "Cross-domain A — ADA Title II foundational §12132",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "ADA Title II discrimination public entity disability",
        "expected": "42 U.S.C. § 12132",
        "max_rank": 3,
    },
    {
        "name":     "Cross-domain B — Rehab Act §504 foundational §794",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "Section 504 disability discrimination federal financial assistance",
        "expected": "29 U.S.C. § 794",
        "max_rank": 3,
    },
]
set_ok = run_checks(CROSS_DOMAIN_CHECKS)

CHECKS = [
    # Rehab Act § 794 — three patterns
    {
        "name":     "§ 794 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "qualified individual disability solely by reason of disability excluded participation program receiving federal financial assistance",
        "expected": "29 U.S.C. § 794",
        "max_rank": 3,
    },
    {
        "name":     "§ 794 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "29 U.S.C. § 794",
        "max_rank": 1,
    },
    {
        "name":     "§ 794 (c) — popular name + tag filter rehab-act",
        "method":   "hybrid_rrf",
        "tag_filter": "rehab-act",
        # Refined per operator: citation-alias "504" + key statutory concepts —
        # what a researcher who knows the act by popular name would type.
        "question": "Section 504 disability discrimination federal financial assistance",
        "expected": "29 U.S.C. § 794",
        "max_rank": 5,
    },
    # ERISA § 1104 — three patterns
    {
        "name":     "§ 1104 (a) — statutory phrasing, scoped to statutes",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "fiduciary shall discharge duties solely in interest of participants and beneficiaries",
        "expected": "29 U.S.C. § 1104",
        "max_rank": 3,
    },
    {
        "name":     "§ 1104 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "29 U.S.C. § 1104",
        "max_rank": 1,
    },
    {
        "name":     "§ 1104 (c) — popular name + tag filter erisa",
        "method":   "hybrid_rrf",
        "tag_filter": "erisa",
        "question": "ERISA fiduciary duty",
        "expected": "29 U.S.C. § 1104",
        "max_rank": 5,
    },
    # FRCP 17 cross-title — three patterns
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
checks_ok = run_checks(CHECKS)

if not (set_ok and checks_ok):
    print("\n[pause-checkpoint] FAIL — investigate before launching T5")
    sys.exit(1)
print("\n[pause-checkpoint] ALL PASS — ready for explicit go-ahead on T5")
sys.exit(0)
