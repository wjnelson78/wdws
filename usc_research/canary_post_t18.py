"""Post-T18 canary. Three patterns × 3 anchors + cross-domain §242 ↔ §1983."""
import sys
sys.path.insert(0, "/opt/wdws/usc_research")
import psycopg2
from canary_lib import run_checks, DSN

CHECKS = [
    # § 241 — Conspiracy against rights
    {
        "name":     "§ 241 (a) — conspiracy against rights statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "two or more persons conspire injure oppress threaten intimidate person free exercise enjoyment right",
        "expected": "18 U.S.C. § 241",
        "max_rank": 3,
    },
    {
        "name":     "§ 241 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "18 U.S.C. § 241",
        "max_rank": 1,
    },
    {
        "name":     "§ 241 (c) — conspiracy against rights popular name + tag civil-rights",
        "method":   "hybrid_rrf",
        "tag_filter": "civil-rights",
        "question": "criminal conspiracy violate civil rights",
        "expected": "18 U.S.C. § 241",
        "max_rank": 5,
    },
    # § 242 — Deprivation of rights under color of law (criminal counterpart of §1983)
    {
        "name":     "§ 242 (a) — deprivation under color of law statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "willfully under color of law subjects person deprivation rights privileges immunities",
        "expected": "18 U.S.C. § 242",
        "max_rank": 3,
    },
    {
        "name":     "§ 242 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "18 U.S.C. § 242",
        "max_rank": 1,
    },
    {
        "name":     "§ 242 (c) — popular name + tag civil-rights",
        "method":   "hybrid_rrf",
        "tag_filter": "civil-rights",
        "question": "criminal deprivation rights under color of state law",
        "expected": "18 U.S.C. § 242",
        "max_rank": 5,
    },
    # § 1001 — False statements
    {
        "name":     "§ 1001 (a) — false statements statutory phrasing",
        "method":   "hybrid_rrf",
        "doc_type_filter": "statute",
        "question": "knowingly willfully falsifies conceals material fact false fictitious fraudulent statement",
        "expected": "18 U.S.C. § 1001",
        "max_rank": 3,
    },
    {
        "name":     "§ 1001 (b) — citation lookup",
        "method":   "sql_citation",
        "expected": "18 U.S.C. § 1001",
        "max_rank": 1,
    },
    # § 242 ↔ § 1983 cross-domain — one query, both must surface
    {
        "name":     "Cross-domain — §242 (criminal counterpart of §1983), via civil-rights tag",
        "method":   "hybrid_rrf",
        "tag_filter": "civil-rights",
        "question": "deprivation rights under color of state law color of state law",
        "expected": "18 U.S.C. § 242",
        "max_rank": 10,  # widened; cross-domain check
    },
]

ok = run_checks(CHECKS)

# Independent set check: §242 + §1983 both surface in civil-rights tag
print("\n=== §242 ↔ §1983 co-retrieval set check ===")
from canary_lib import hybrid_rrf, cite_match
conn = psycopg2.connect(DSN); conn.autocommit = True
results = hybrid_rrf(conn, question="deprivation rights under color of state law civil rights",
                     tag_filter="civil-rights", top_k=10)
required = ["18 U.S.C. § 242", "42 U.S.C. § 1983"]
found = set()
for i, r in enumerate(results):
    marks = [c for c in required if cite_match(c, r["title"])]
    if marks:
        found.update(marks)
    print(f"  rank={i+1} rrf={r['rrf_score']:.6f}  {r['title'][:80]}{'  ★ ' + ', '.join(marks) if marks else ''}")
missing = [c for c in required if c not in found]
if missing:
    print(f"  RESULT: FAIL — missing {missing}")
    ok = False
else:
    print(f"  RESULT: PASS — both §242 and §1983 surface")
conn.close()

sys.exit(0 if ok else 1)
