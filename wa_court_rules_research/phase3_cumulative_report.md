# Phase 3 — Parser Cumulative Report

**Run time:** 2026-05-01T08:43:24.219857

## 1. Summary

- Total PDFs attempted: 1040
- Total parsed records emitted: 1131
- Total cross-references extracted: 2935
- Parse errors (PDF unresolvable / extraction failed): 0
- Empty/garbled PDFs (still emitted, flagged): 0
- Anomaly entries logged: 1

## 2. Records by Rule Set

| Set | Records emitted |
|---|---|
| GR | 58 |
| RPC | 64 |
| CR | 101 |
| ER | 67 |
| RAP | 183 |
| CJC | 35 |
| GALR | 7 |
| CrR | 65 |
| CrRLJ | 76 |
| CRLJ | 101 |
| JuCR | 99 |
| SCCAR | 27 |
| RALJ | 45 |
| APR | 70 |
| ELC | 133 |
| **TOTAL** | **1131** |

## 3. Status & special-class breakdown

- is_reserved: 89
- is_repealed: 51
- has_comment: 187
- with parent_canon (CJC inner rules): 35
- with parent_regulation_handle (APR irregular IDs): 19
- range canonical rows: 8
- range expanded children: 44

## 4. Cross-reference observations

- Self-citations are present: each PDF starts with its own citation as a heading line (e.g., "RPC 1.1" at top of RPC 1.1 PDF), so the cross-reference extractor captures `target_citation = canonical_citation` for nearly every rule. Phase 4 ingest can filter these by comparing `target_set_code + target_rule_number` to the source rule. They are preserved in JSONL for now to keep the parser deterministic.
- Resolution order matches phase1_regex_test.py: CJC → APR_HANDLE → main. All three patterns are run independently to capture overlapping or multiple matches in a single body (parse_cite() only returns the first).

## 5. Disposition

**Ready for Phase 4 ingest** — zero parse errors, zero halt triggers.

## 6. Spot-check samples

### RAP 18.13A — Accelerated Review of Juvenile Dependency Disposition Orders, Orders Terminating Parental Rights, Dependency Guardianship Orders, and Orders Entered in Dependency and Dependency Guardianship Proceedings

- is_reserved=False, is_repealed=False, has_comment=False
- effective_date=2008-10-02, last_amended_date=2012-04-03
- history_count=7, cross_ref_count=6, body_chars=9895

### CRLJ 30 — Reserved

- is_reserved=True, is_repealed=False, has_comment=False
- effective_date=None, last_amended_date=None
- range_canonical_citation=CRLJ 27-37
- history_count=0, cross_ref_count=0, body_chars=64

### APR 15P — Client Protection Fund (APR 15) Procedural Regulations

- is_reserved=False, is_repealed=False, has_comment=False
- effective_date=1995-07-18, last_amended_date=2017-09-01
- history_count=56, cross_ref_count=4, body_chars=24544

### CJC 2.11 — Disqualification

- is_reserved=False, is_repealed=False, has_comment=True
- effective_date=None, last_amended_date=None
- parent_canon=Canon 2
- history_count=3, cross_ref_count=0, body_chars=6638

### ELC 1.1 — Scope of Rules

- is_reserved=False, is_repealed=False, has_comment=False
- effective_date=2002-10-01, last_amended_date=None
- history_count=1, cross_ref_count=1, body_chars=348

### ER 401 — Definition of "Relevant Evidence"

- is_reserved=False, is_repealed=False, has_comment=True
- effective_date=1979-04-02, last_amended_date=2006-09-01
- history_count=2, cross_ref_count=1, body_chars=458

### GR 31 — Access to Court Records

- is_reserved=False, is_repealed=False, has_comment=True
- effective_date=2004-10-26, last_amended_date=2006-01-03
- history_count=5, cross_ref_count=5, body_chars=11678

### RPC 1.1 — Competence

- is_reserved=False, is_repealed=False, has_comment=True
- effective_date=1985-09-01, last_amended_date=2016-09-01
- history_count=7, cross_ref_count=4, body_chars=6605

### CR 12 — Defenses and Objections

- is_reserved=False, is_repealed=False, has_comment=False
- effective_date=1967-07-01, last_amended_date=1972-01-01
- history_count=6, cross_ref_count=4, body_chars=7667

### GALR 1 — Scope and Definitions

- is_reserved=False, is_repealed=False, has_comment=False
- effective_date=2001-11-27, last_amended_date=2024-12-10
- history_count=2, cross_ref_count=6, body_chars=2465

## 7. Anomaly inventory (first 50)

- **CRLJ 86.04--99.04** — Range expansion ambiguous — multi-decimal notation; operator review required (`CLJ_CRLJ_86.04-99.04.pdf`)
