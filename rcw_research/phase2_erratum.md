# Phase 2 Cumulative Report — ERRATUM

**Issued during Phase 3 execution.** Amends the Phase 2 anomaly inventory and
records a methodology lesson for Phase 6.

---

## What changed

The Phase 2 cumulative report ("Phase 2 — Cumulative Report") classified 24
chapters as Class 1 ("Empty chapters — 0 section anchors, parser must handle
gracefully"). That classification was **wrong for 14 of the 24 chapters**.
All 14 erroneously-classified chapters were in Title 62A (Uniform Commercial
Code), which uses dash-separated section numbers (`62A.1-101`, `62A.9A-203`)
per the national UCC convention. Phase 2's anchor-counter regex required dot
separators between chapter and section parts, so it returned 0 for all UCC
chapters even though they contain substantial section content.

Verified during Phase 3:

```
Title 62A actual section counts (per dash-aware re-count):
  62A.1   General Provisions:                   26 sections
  62A.2   Sales:                                103
  62A.2A  Leases:                                78
  62A.3   Negotiable Instruments:                76
  62A.4   Bank Deposits and Collections:         41
  62A.4A  Funds Transfers:                       38
  62A.5   Letters of Credit:                     20
  62A.7   Warehouse Receipts, Bills of Lading:   41
  62A.8   Investment Securities:                 52
  62A.9A  Secured Transactions:                 149
  62A.10  Effective Date and Repealer:            3
  62A.11  Effective Date and Transition:         13
  62A.12  Controllable Electronic Records:        7
  62A.12A Transitional Provisions for UCC 2022:   9
                                          TOTAL: 656 sections
```

## Corrected anomaly inventory

The 24 chapters in Phase 2 §"Class 1 — Empty chapters" should be split:

### Class 1 — Truly empty / reserved chapters (10 chapters, unchanged)

These are valid live chapters with no current section content. Parser emits a
chapter row, no section rows, no error. Detector pattern: `<div>(RESERVED)</div>`
in caption (canonical) or status_hint mapping (fallback).

| Cite | Caption | Status hint |
|---|---|---|
| 14.30 | Western Regional Short-Haul Air Transportation Compact | reserved |
| 18.09 | Attorneys-at-Law | reserved |
| 29A.76C | Congressional Districts and Apportionment | reserved |
| 32.28 | Satellite Facilities | reserved |
| 32.30 | Conversion of Mutual Savings Bank to B&L | reserved |
| 33.54 | Satellite Facilities | reserved |
| 36.42 | Retail Sales and Use Taxes | reserved |
| 44.07F | Legislative Districts and Apportionment | reserved |
| 48.26 | Marine and Transportation Insurance | explicit `(RESERVED)` marker |
| 76.10 | Surface Mining | reserved |

### Class 1B — UCC dash-format chapters (14 chapters, NEW classification)

These chapters have substantial section content using the UCC's national
Article-Section numbering convention with dashes. They are **not empty** —
the Phase 2 anchor-counter just couldn't see their sections. Parser handles
them via scoped alternation in `ANCHOR_RE` and via `<title>`-element fallback
in `_extract_h1_chapter` (UCC chapters use `<h1>Article N</h1>` instead of
`<h1>Chapter T.CC RCW</h1>`).

All 14 chapters listed above; all parse cleanly post-Phase-3 fix. Total: 656
sections across these 14 chapters.

## Updated section-count totals

```
Phase 2 estimate:  35,000 – 50,000 sections (operator range)
Phase 0 sample-projected: ~45,000 sections
Phase 3 (with fix): 51,790 sections
  Of which Title 62A contributes:  656
  Total minus Title 62A:        51,134
```

The Phase 3 actual count (51,790) is 3.6% above the operator's stated upper
bound (50,000) but well within the >2× halt threshold. The variance comes
primarily from richer-than-expected per-chapter section counts in Titles 43,
18, 48, 36, 35, 46 (state government, professions, insurance, counties, cities,
motor vehicles) — none individually surprising; the cumulative count was
underestimated because the Phase 0 random-sample projection averaged over the
long-tail small chapters and didn't fully weight the head of the distribution.

## Methodology lesson (Phase 6 retrieval-guide entry)

When the audit tool and the production code share the same predicate, both
fail in the same way and the audit silently confirms the bug instead of
catching it. In this case:

- **Production code**: `_section_anchor_iterator` in `rcw_parser.py`, regex
  `r'^[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+$'` (dot-only)
- **Audit tool**: `count_section_anchors` and the Phase 2 verification
  bash command, both using the same dot-only regex

When the audit and the code under test apply identical predicates, a
malformed input that the predicate misses is not detected by either. Phase 2's
"24 chapters with 0 section anchors" finding looked authoritative because
audit and code agreed — but they agreed because they shared the bug, not
because they confirmed the fact.

**Discipline for Phase 6 §K (audit hygiene):**

1. Audit tools and production code should share interfaces (input/output
   shapes), but not predicates (the actual logic that decides whether
   something matches).
2. Where possible, audits should use a *more permissive* predicate than the
   code under test, then sanity-check the production code by counting how
   many records the production code rejected vs. the audit accepted.
3. If audit and code must share a predicate (e.g., both depend on a regex
   constant), an additional, structurally-different audit should
   cross-check (in this case: parsing each chapter end-to-end as Phase 3
   does is structurally different from regex-counting anchors and would
   have caught the bug immediately by raising `ValueError` on the UCC
   chapters' missing `<h1>`).
4. "All 14 chapters with N=0 anchors" is a class-of-one observation when
   N=0 happens to be the audit's predicate-failure value. Always check
   whether the failure mode collapses N to a sentinel that the audit
   conflates with a legitimate state.

This entry should be added to the Phase 6 retrieval guide as §K
"Audit-predicate diversification" — applies to both static analysis of
ingested data and to retrieval-test methodology where canary queries vs.
production retrieval might share a predicate that hides a bug.

End of erratum.
