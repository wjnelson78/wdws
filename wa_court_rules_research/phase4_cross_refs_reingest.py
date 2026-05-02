#!/usr/bin/env python3
"""Phase 4 cross_references re-ingest driver.

Operator-authorized 2026-05-01 (Phase 5 Investigation 2 → Option A immediate fix).

Sequence:
  1. Pre-flight guards: rules=1145, rule_history=4056, range_doc_id=58
     (any drift halts before TRUNCATE)
  2. Hydrate citation_to_uuid map from existing rules
  3. TRUNCATE wa_court_rules.cross_references (812 rows expected)
  4. Re-run insert_cross_references against re-parsed phase3_parsed.jsonl
     with cross-jurisdiction extraction (RCW + USC + WAC + WA-Const)
  5. POST guards: count >1200; ≥4 jurisdictions populated; in-scope
     resolution ≥95%; state-wa-rcw resolution against rcw.sections ≥50%;
     federal-usc rate logged but not gated; state-wa-wac legitimately 0
     per Option 1 known limitation (CrRLJ 6.13 chapter-form gap)
  6. COMMIT or ROLLBACK on any guard failure
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.insert(0, '/opt/wdws/wa_court_rules_research')
from phase4_ingest import insert_cross_references, DSN, PARSED_JSONL, HaltError

EXPECTED_RULES = 1145
EXPECTED_HISTORY = 4056
EXPECTED_RANGE_RESOLVED = 58
TOTAL_FLOOR = 1200
INSCOPE_RATE_FLOOR = 0.95
RCW_RATE_FLOOR = 0.50
EXPECTED_JURISDICTIONS = {
    'state-wa-court-rule', 'state-wa-rcw', 'federal-usc',
    'wa-constitution', 'state-wa-wac',
}
# state-wa-wac legitimately 0 per Option 1 (CrRLJ 6.13 chapter-form gap;
# Phase 7 backlog item) — POST guard requires ≥4 of expected 5
EXPECTED_NONZERO_FLOOR = 4


def main() -> int:
    print("=== Phase 4 cross_references re-ingest ===", flush=True)

    # Load re-parsed records
    records: list[dict] = []
    with PARSED_JSONL.open() as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    print(f"Loaded {len(records)} parsed records", flush=True)

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    try:
        # Pre-flight
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules")
            n_rules = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rule_history")
            n_hist = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.rules WHERE range_doc_id IS NOT NULL")
            n_range = cur.fetchone()[0]
        if n_rules < EXPECTED_RULES:
            raise HaltError(f"Pre-flight FAIL: rules={n_rules} < expected {EXPECTED_RULES}")
        if n_hist != EXPECTED_HISTORY:
            raise HaltError(f"Pre-flight FAIL: rule_history={n_hist} (expected {EXPECTED_HISTORY})")
        if n_range != EXPECTED_RANGE_RESOLVED:
            raise HaltError(f"Pre-flight FAIL: range_doc_id non-NULL={n_range} (expected {EXPECTED_RANGE_RESOLVED})")
        print(f"  [ok] pre-flight: rules={n_rules} history={n_hist} range_resolved={n_range}", flush=True)

        # Hydrate citation_to_uuid
        citation_to_uuid: dict[str, str] = {}
        with conn.cursor() as cur:
            cur.execute("SELECT canonical_citation, id FROM wa_court_rules.rules")
            for cite, rid in cur.fetchall():
                citation_to_uuid[cite] = str(rid)
        print(f"  [ok] hydrated {len(citation_to_uuid)} citation->uuid mappings", flush=True)

        # TRUNCATE
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.cross_references")
            n_before = cur.fetchone()[0]
            cur.execute("TRUNCATE wa_court_rules.cross_references")
        print(f"  [ok] TRUNCATE wa_court_rules.cross_references (was {n_before} rows)", flush=True)

        # Re-ingest
        t0 = time.time()
        result = insert_cross_references(conn, citation_to_uuid, records)
        elapsed = time.time() - t0
        print(f"\n=== Re-ingest complete ({elapsed:.1f}s) ===", flush=True)
        print(f"  inserted: {result['inserted']}")
        print(f"  skipped self-cites: {result['skipped_self']}")
        print(f"  in-scope resolved: {result['resolved_inscope']}")
        print(f"  in-scope unresolved: {result['unresolved_inscope']}")
        print(f"  in-scope rate: {result['in_scope_resolution_rate']*100:.1f}%")

        # POST guards
        print(f"\n=== POST guards ===", flush=True)

        # 1. Total count
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM wa_court_rules.cross_references")
            n_total = cur.fetchone()[0]
        print(f"  total cross_refs: {n_total}")
        if n_total < TOTAL_FLOOR:
            raise HaltError(f"POST FAIL: cross_references count {n_total} < {TOTAL_FLOOR}; extension didn't take effect")
        print(f"  [ok] count >= {TOTAL_FLOOR}")

        # 2. Jurisdiction breakdown
        with conn.cursor() as cur:
            cur.execute("""
                SELECT target_jurisdiction, COUNT(*)
                FROM wa_court_rules.cross_references
                GROUP BY target_jurisdiction
                ORDER BY 2 DESC
            """)
            by_juris = dict(cur.fetchall())
        print(f"  jurisdiction breakdown:")
        for j, c in sorted(by_juris.items(), key=lambda x: -x[1]):
            print(f"    {j}: {c}")
        non_zero_in_expected = {j for j, c in by_juris.items() if c > 0 and j in EXPECTED_JURISDICTIONS}
        if len(non_zero_in_expected) < EXPECTED_NONZERO_FLOOR:
            raise HaltError(
                f"POST FAIL: only {len(non_zero_in_expected)} of expected {EXPECTED_NONZERO_FLOOR}+ "
                f"jurisdictions populated. Got: {non_zero_in_expected}"
            )
        wac_status = "legitimately 0 per Option 1 known limitation" if by_juris.get('state-wa-wac', 0) == 0 else f"populated ({by_juris.get('state-wa-wac', 0)})"
        print(f"  [ok] {len(non_zero_in_expected)} expected jurisdictions populated; state-wa-wac {wac_status}")

        # 3. In-scope rate
        if result['in_scope_resolution_rate'] < INSCOPE_RATE_FLOOR:
            raise HaltError(
                f"POST FAIL: in-scope rate {result['in_scope_resolution_rate']*100:.1f}% "
                f"< {INSCOPE_RATE_FLOOR*100:.0f}%; intra-corpus regression detected"
            )
        print(f"  [ok] in-scope rate {result['in_scope_resolution_rate']*100:.1f}% >= {INSCOPE_RATE_FLOOR*100:.0f}%")

        # 4. state-wa-rcw resolution against rcw.sections (canonical_citation match)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE EXISTS (
                        SELECT 1 FROM rcw.sections s
                        WHERE s.canonical_citation = x.target_citation
                    )) as resolved
                FROM wa_court_rules.cross_references x
                WHERE x.target_jurisdiction = 'state-wa-rcw'
            """)
            rcw_total, rcw_resolved = cur.fetchone()
        rcw_rate = rcw_resolved / rcw_total if rcw_total else 1.0
        print(f"  state-wa-rcw: {rcw_resolved}/{rcw_total} = {rcw_rate*100:.1f}% resolve against rcw.sections")
        if rcw_rate < RCW_RATE_FLOOR:
            raise HaltError(
                f"POST FAIL: state-wa-rcw resolution rate {rcw_rate*100:.1f}% "
                f"< {RCW_RATE_FLOOR*100:.0f}% (parsing format mismatch suspected)"
            )
        print(f"  [ok] state-wa-rcw rate {rcw_rate*100:.1f}% >= {RCW_RATE_FLOOR*100:.0f}%")

        # 5. federal-usc resolution (logged, not gated)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE EXISTS (
                        SELECT 1 FROM usc.sections s
                        WHERE s.canonical_citation = x.target_citation
                    )) as resolved
                FROM wa_court_rules.cross_references x
                WHERE x.target_jurisdiction = 'federal-usc'
            """)
            usc_total, usc_resolved = cur.fetchone()
        usc_rate = usc_resolved / usc_total if usc_total else 1.0
        print(f"  federal-usc: {usc_resolved}/{usc_total} = {usc_rate*100:.1f}% resolve against usc.sections (logged, not gated)")

        # 6. state-wa-wac (legitimately 0 per Option 1)
        wac_total = by_juris.get('state-wa-wac', 0)
        print(f"  state-wa-wac: {wac_total} citations (Phase 7 backlog: WAC chapter-form pattern)")

        # 7. wa-constitution (no FK target)
        wc_total = by_juris.get('wa-constitution', 0)
        print(f"  wa-constitution: {wc_total} citations (text-only, no FK target)")

        # COMMIT
        conn.commit()
        print(f"\n=== COMMIT successful ===", flush=True)
        print(f"Total wall time: {time.time() - t0:.1f}s")
        return 0

    except Exception as e:
        conn.rollback()
        print(f"\n!!! ROLLBACK: {e}", file=sys.stderr, flush=True)
        return 2
    finally:
        conn.close()


if __name__ == '__main__':
    sys.exit(main())
