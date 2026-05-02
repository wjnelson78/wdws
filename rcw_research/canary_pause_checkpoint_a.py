"""Pause-Checkpoint A — Cross-Jurisdiction Canary Suite.

Three tests required by Phase 4 authorization. All run with default
(no jurisdiction filter), domain='legal', top_k=10. Each test is a SET
check (multiple required citations must all surface in top-10).
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from canary_rcw_lib import run_set_check

def banner(msg):
    print("\n" + "═" * 75)
    print(f"  {msg}")
    print("═" * 75)

if __name__ == "__main__":
    overall = True

    banner("TEST 1 — Disability discrimination, state + federal employment")
    t1 = {
        "name": "Cross-jurisdiction Test 1: ADA Title I + WLAD employment",
        "question": "disability discrimination state and federal employment",
        "required": ["42 U.S.C. § 12112", "RCW 49.60.180"],
    }
    overall &= run_set_check(t1, top_k=10)

    banner("TEST 2 — Public records response time + judicial review (FOIA/PRA parallel)")
    t2 = {
        "name": "Cross-jurisdiction Test 2: PRA response/review + ideally FOIA",
        "question": "public records request response time judicial review",
        "required": ["RCW 42.56.520", "RCW 42.56.550"],
    }
    t2_pass = run_set_check(t2, top_k=10)
    # Also report whether 5 U.S.C. § 552 (FOIA) surfaces — soft-pass per spec
    print("  (soft-pass) Looking for 5 U.S.C. § 552 in top-10 of same query...")
    from canary_rcw_lib import hybrid_rrf
    import psycopg2
    from canary_lib import DSN
    conn = psycopg2.connect(DSN); conn.autocommit = True
    results = hybrid_rrf(conn, question=t2["question"], top_k=10)
    foia_found = any("5 U.S.C. § 552" in r["title"] and "552a" not in r["title"]
                     and "552b" not in r["title"] for r in results)
    conn.close()
    print(f"  FOIA (5 U.S.C. § 552) in top-10: {foia_found}  (soft-pass — informational)")
    overall &= t2_pass

    banner("TEST 3 — Recodification chain (PDC / 29B / 42.17A)")
    t3 = {
        "name": "Cross-jurisdiction Test 3: recodification chain queryability",
        "question": "public disclosure commission campaign finance reporting",
        "required": ["RCW 42.17A"],   # any 42.17A.* citation in top-10
    }
    t3_pass = run_set_check(t3, top_k=10)
    overall &= t3_pass

    print("\n" + "═" * 75)
    print(f"  PAUSE-CHECKPOINT A CROSS-JURISDICTION CANARY: {'PASS' if overall else 'FAIL'}")
    print("═" * 75)
    sys.exit(0 if overall else 1)
