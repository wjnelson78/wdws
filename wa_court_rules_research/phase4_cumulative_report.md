# WA Court Rules Phase 4 Cumulative Report

**Generated:** 2026-05-01T07:39:03.435503

**Publication date:** 2026-04-30

**Source:** `/opt/wdws/wa_court_rules_research/phase3_parsed.jsonl` (1,145 records)


**Total wall time:** 51.7s

## Embedding stats (HF .187)

- Calls: 653
- Total seconds: 36.0
- Avg seconds/call: 0.055
- Max seconds/call: 1.777

## Tier 1 — Pause-Checkpoint A

**Total inserted:** 7 / 515 expected


| Set | Inserted | Expected | Elapsed (s) | Rate/s | Pat (a) | Pat (b) | Pat (c) |
|-----|----------|----------|-------------|--------|---------|---------|---------|
| GR | 0 | 58 | 0.0 | 0.0 | PASS | PASS | PASS |
| RPC | 0 | 64 | 0.0 | 0.0 | PASS | PASS | PASS |
| CR | 0 | 101 | 0.0 | 0.0 | PASS | PASS | PASS |
| ER | 0 | 67 | 0.0 | 0.0 | PASS | PASS | PASS |
| RAP | 0 | 183 | 0.0 | 0.0 | log-only | PASS | PASS |
| CJC | 0 | 35 | 0.0 | 0.0 | PASS | PASS | PASS |
| GALR | 7 | 7 | 0.7 | 10.3 | PASS | PASS | PASS |

### GR canaries

- Pattern (a) semantic: top10 for 'access to court records' in GR: ['GR 31', 'GR 22', 'GR 31.1', 'GR 12.4', 'GR 15']... anchor=GR 31 HIT
- Pattern (b) `GR 31`: PASS — row found, full_text len=11678
- Pattern (b) `GR 33`: PASS — row found, full_text len=6676
- Pattern (b) `GR 34`: PASS — row found, full_text len=5412
- Pattern (c) FTS: FTS 'records' on GR: 23 hits

### RPC canaries

- Pattern (a) semantic: top10 for 'attorney competence' in RPC: ['RPC 1.1', 'RPC 1.14', 'RPC 1.3', 'RPC 8.5', 'RPC 1.2']... anchor=RPC 1.1 HIT
- Pattern (b) `RPC 1.1`: PASS — row found, full_text len=6605
- Pattern (b) `RPC 1.5`: PASS — row found, full_text len=20382
- Pattern (b) `RPC 5.5`: PASS — row found, full_text len=19064
- Pattern (c) FTS: FTS 'competence' on RPC: 22 hits

### CR canaries

- Pattern (a) semantic: top10 for 'summary judgment' in CR: ['CR 56', 'CR 49', 'CR 54', 'CR 65', 'CR 57']... anchor=CR 56 HIT
- Pattern (b) `CR 56`: PASS — row found, full_text len=5328
- Pattern (b) `CR 12`: PASS — row found, full_text len=7667
- Pattern (b) `CR 26`: PASS — row found, full_text len=18239
- Pattern (c) FTS: FTS 'subpoena' on CR: 7 hits

### ER canaries

- Pattern (a) semantic: top10 for 'relevance of evidence' in ER: ['ER 401', 'ER 403', 'ER 402', 'ER 104', 'ER 102']... anchor=ER 401 HIT
- Pattern (b) `ER 401`: PASS — row found, full_text len=458
- Pattern (b) `ER 403`: PASS — row found, full_text len=565
- Pattern (b) `ER 802`: PASS — row found, full_text len=342
- Pattern (c) FTS: FTS 'hearsay' on ER: 6 hits

### RAP canaries

- Pattern (a) semantic: top10 for 'appeal procedure' in RAP: ['RAP 18.9', 'RAP 2.1', 'RAP 16.8.1', 'RAP 13.1', 'RAP 16.13']... anchor=RAP 18.13A MISS
- Pattern (b) `RAP 18.13A`: PASS — row found, full_text len=9895
- Pattern (c) FTS: FTS 'appellate' on RAP: 123 hits

### CJC canaries

- Pattern (a) semantic: top10 for 'judicial impartiality' in CJC: ['CJC 2.2', 'CJC 1.2', 'CJC 2.11', 'CJC 2.16', 'CJC 2.12']... anchor=CJC 2.11 HIT
- Pattern (b) `CJC 2.11`: PASS — row found, full_text len=6638
- Pattern (b) `CJC 1.1`: PASS — row found, full_text len=189
- Pattern (c) FTS: FTS 'judge' on CJC: 32 hits

### GALR canaries

- Pattern (a) semantic: top10 for 'guardian ad litem scope' in GALR: ['GALR 4', 'GALR 2', 'GALR 1', 'GALR 3', 'GALR 6']... anchor=GALR 1 HIT
- Pattern (b) `GALR 1`: PASS — row found, full_text len=2465
- Pattern (b) `GALR 5`: PASS — row found, full_text len=699
- Pattern (c) FTS: FTS 'guardian' on GALR: 7 hits

## Tier 2 — Pause-Checkpoint B

**Total inserted:** 630 / 630 expected


| Set | Inserted | Expected | Elapsed (s) | Rate/s | Pat (a) | Pat (b) | Pat (c) |
|-----|----------|----------|-------------|--------|---------|---------|---------|
| CrR | 65 | 65 | 7.1 | 9.1 | PASS | PASS | PASS |
| CrRLJ | 76 | 76 | 8.2 | 9.3 | PASS | PASS | PASS |
| CRLJ | 115 | 115 | 7.3 | 15.7 | log-only | PASS | PASS |
| JuCR | 99 | 99 | 6.7 | 14.7 | PASS | PASS | PASS |
| SCCAR | 27 | 27 | 1.5 | 17.6 | PASS | PASS | PASS |
| RALJ | 45 | 45 | 2.8 | 16.3 | PASS | PASS | PASS |
| APR | 70 | 70 | 7.4 | 9.4 | log-only | PASS | PASS |
| ELC | 133 | 133 | 8.9 | 15.0 | PASS | PASS | PASS |

### CrR canaries

- Pattern (a) semantic: top10 for 'criminal procedure scope' in CrR: ['CrR 1.1', 'CrR 1.2', 'CrR 4.2', 'CrR 5.1', 'CrR 3.2.1']... anchor=CrR 1.1 HIT
- Pattern (b) `CrR 1.1`: PASS — row found, full_text len=556
- Pattern (b) `CrR 3.5`: PASS — row found, full_text len=2428
- Pattern (c) FTS: FTS 'criminal' on CrR: 22 hits

### CrRLJ canaries

- Pattern (a) semantic: top10 for 'limited jurisdiction criminal' in CrRLJ: ['CrRLJ 1.1', 'CrRLJ 1.8', 'CrRLJ 9.1', 'CrRLJ 3.2', 'CrRLJ 9.2']... anchor=CrRLJ 1.1 HIT
- Pattern (b) `CrRLJ 1.1`: PASS — row found, full_text len=562
- Pattern (c) FTS: FTS 'criminal' on CrRLJ: 22 hits

### CRLJ canaries

- Pattern (a) semantic: top10 for 'limited jurisdiction civil' in CRLJ: ['CRLJ 1', 'CRLJ 48', 'CRLJ 57', 'CRLJ 82', 'CRLJ 73']... anchor=CRLJ 30 MISS
- Pattern (b) `CRLJ 30`: PASS — row found, full_text len=64
- Pattern (b) `CRLJ 92.04`: PASS — row found, full_text len=71
- Pattern (c) FTS: FTS 'civil' on CRLJ: 18 hits

### JuCR canaries

- Pattern (a) semantic: top10 for 'juvenile court' in JuCR: ['JuCR 1.2', 'JuCR 5.1', 'JuCR 7.1', 'JuCR 10.2', 'JuCR 4.1']... anchor=JuCR 1.1 HIT
- Pattern (b) `JuCR 1.1`: PASS — row found, full_text len=200
- Pattern (b) `JuCR 5A.1`: PASS — row found, full_text len=232
- Pattern (c) FTS: FTS 'juvenile' on JuCR: 52 hits

### SCCAR canaries

- Pattern (a) semantic: top10 for 'civil arbitration' in SCCAR: ['SCCAR 1.2', 'SCCAR 8.1', 'SCCAR 1.1', 'SCCAR 3.2', 'SCCAR 2.3']... anchor=SCCAR 1.1 HIT
- Pattern (b) `SCCAR 1.1`: PASS — row found, full_text len=399
- Pattern (c) FTS: FTS 'arbitration' on SCCAR: 24 hits

### RALJ canaries

- Pattern (a) semantic: top10 for 'appeal limited jurisdiction' in RALJ: ['RALJ 2.2', 'RALJ 11.1', 'RALJ 4.1', 'RALJ 1.1', 'RALJ 2.3']... anchor=RALJ 1.1 HIT
- Pattern (b) `RALJ 1.1`: PASS — row found, full_text len=2028
- Pattern (c) FTS: FTS 'appeal' on RALJ: 27 hits

### APR canaries

- Pattern (a) semantic: top10 for 'admission and practice' in APR: ['APR 24.3', 'APR 20.1', 'APR 5', 'APR 3', 'APR 24.1']... anchor=APR 15P MISS
- Pattern (b) `APR 15P`: PASS — row found, full_text len=24544
- Pattern (b) `APR Regs 101`: PASS — row found, full_text len=195
- Pattern (c) FTS: FTS 'admission' on APR: 25 hits

### ELC canaries

- Pattern (a) semantic: top10 for 'lawyer discipline procedure' in ELC: ['ELC 9.4', 'ELC 9.3', 'ELC 1.1', 'ELC 13.1', 'ELC 1.5']... anchor=ELC 1.1 HIT
- Pattern (b) `ELC 1.1`: PASS — row found, full_text len=348
- Pattern (b) `ELC 5.1`: PASS — row found, full_text len=6164
- Pattern (b) `ELC 13.1`: PASS — row found, full_text len=708
- Pattern (c) FTS: FTS 'discipline' on ELC: 37 hits

## FK Pass 1 — `range_doc_id` resolution

- Resolved: 58
- Unresolved: 0

## FK Pass 2 — cross-references

- Inserted: 812
- Skipped self-cites: 1069
- In-scope target_rule_id resolved: 791
- In-scope target_rule_id unresolved: 15
- In-scope resolution rate: 98.1%

## Final corpus state

- wa_court_rules.rules: 1145
- wa_court_rules.rule_sets: 15
- wa_court_rules.cross_references: 812
- wa_court_rules.rule_history: 4056
- core.document_chunks (court_rule docs): 1412
