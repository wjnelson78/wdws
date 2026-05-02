# WA Court Rules Phase 5 Validation Report

Generated: 2026-05-01T15:51:06.135197+00:00

Mode: **PRODUCTION**

## Summary

11/11 checks completed (PASS=11, halted=0)

## Phase 5 Disposition: **PASS**

### §H2 documented limitations

- federal-usc resolution 11.1% (1/9) — Phase 7 USC long-tail backlog
- state-wa-wac cross-ref count=0 (Phase 7 chapter-form pattern backlog)
- Pattern (a) MISS on rule sets: ['RAP', 'CRLJ', 'APR'] (documented as Phase 6 §H2 query-vocabulary limitation)

## Per-check results

### 1_embedding_count: PASS

```json
{
  "check": "1_embedding_count",
  "pass": true,
  "total_rules": 1145,
  "rules_with_embedding": 1145,
  "coverage": 1.0,
  "docs_court_rule": 1145,
  "chunks_total": 1412
}
```

### 2_resolution_rates: PASS

```json
{
  "check": "2_resolution_rates",
  "pass": true,
  "court_rule": {
    "total": 812,
    "resolved": 791,
    "rate": 0.9741,
    "min": 0.95,
    "ok": true
  },
  "rcw_section": {
    "total": 818,
    "resolved": 767,
    "rate": 0.9377,
    "min": 0.5,
    "ok": true
  },
  "rcw_chapter_form_count": 223,
  "federal_usc": {
    "total": 9,
    "resolved": 1,
    "rate": 0.1111
  },
  "wa_constitution_count": 4,
  "state_wa_wac_count": 0
}
```

### 3_canary_suite: PASS

```json
{
  "check": "3_canary_suite",
  "pass": true,
  "by_rule_set": {
    "GR": {
      "pattern_b": [
        [
          "GR 31",
          true,
          "row found, full_text len=11678"
        ],
        [
          "GR 33",
          true,
          "row found, full_text len=6676"
        ],
        [
          "GR 34",
          true,
          "row found, full_text len=5412"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'records' on GR: 23 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['GR 31', 'GR 22', 'GR 31.1', 'GR 12.4', 'GR 15'] anchor=GR 31 HIT"
      ]
    },
    "RPC": {
      "pattern_b": [
        [
          "RPC 1.1",
          true,
          "row found, full_text len=6605"
        ],
        [
          "RPC 1.5",
          true,
          "row found, full_text len=20382"
        ],
        [
          "RPC 5.5",
          true,
          "row found, full_text len=19064"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'competence' on RPC: 22 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['RPC 1.1', 'RPC 1.14', 'RPC 1.3', 'RPC 8.5', 'RPC 1.2'] anchor=RPC 1.1 HIT"
      ]
    },
    "CR": {
      "pattern_b": [
        [
          "CR 56",
          true,
          "row found, full_text len=5328"
        ],
        [
          "CR 12",
          true,
          "row found, full_text len=7667"
        ],
        [
          "CR 26",
          true,
          "row found, full_text len=18239"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'subpoena' on CR: 7 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['CR 56', 'CR 49', 'CR 54', 'CR 65', 'CR 57'] anchor=CR 56 HIT"
      ]
    },
    "ER": {
      "pattern_b": [
        [
          "ER 401",
          true,
          "row found, full_text len=458"
        ],
        [
          "ER 403",
          true,
          "row found, full_text len=565"
        ],
        [
          "ER 802",
          true,
          "row found, full_text len=342"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'hearsay' on ER: 6 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['ER 401', 'ER 403', 'ER 402', 'ER 104', 'ER 102'] anchor=ER 401 HIT"
      ]
    },
    "RAP": {
      "pattern_b": [
        [
          "RAP 18.13A",
          true,
          "row found, full_text len=9895"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'appellate' on RAP: 123 hits"
      ],
      "pattern_a": [
        false,
        "top10[:5]=['RAP 18.9', 'RAP 2.1', 'RAP 16.8.1', 'RAP 13.1', 'RAP 16.13'] anchor=RAP 18.13A MISS"
      ]
    },
    "CJC": {
      "pattern_b": [
        [
          "CJC 2.11",
          true,
          "row found, full_text len=6638"
        ],
        [
          "CJC 1.1",
          true,
          "row found, full_text len=189"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'judge' on CJC: 32 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['CJC 2.2', 'CJC 1.2', 'CJC 2.11', 'CJC 2.16', 'CJC 2.12'] anchor=CJC 2.11 HIT"
      ]
    },
    "GALR": {
      "pattern_b": [
        [
          "GALR 1",
          true,
          "row found, full_text len=2465"
        ],
        [
          "GALR 5",
          true,
          "row found, full_text len=699"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'guardian' on GALR: 7 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['GALR 4', 'GALR 2', 'GALR 1', 'GALR 3', 'GALR 6'] anchor=GALR 1 HIT"
      ]
    },
    "CrR": {
      "pattern_b": [
        [
          "CrR 1.1",
          true,
          "row found, full_text len=556"
        ],
        [
          "CrR 3.5",
          true,
          "row found, full_text len=2428"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'criminal' on CrR: 22 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['CrR 1.1', 'CrR 1.2', 'CrR 4.2', 'CrR 5.1', 'CrR 3.2.1'] anchor=CrR 1.1 HIT"
      ]
    },
    "CrRLJ": {
      "pattern_b": [
        [
          "CrRLJ 1.1",
          true,
          "row found, full_text len=562"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'criminal' on CrRLJ: 22 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['CrRLJ 1.1', 'CrRLJ 1.8', 'CrRLJ 9.1', 'CrRLJ 3.2', 'CrRLJ 9.2'] anchor=CrRLJ 1.1 HIT"
      ]
    },
    "CRLJ": {
      "pattern_b": [
        [
          "CRLJ 30",
          true,
          "row found, full_text len=64"
        ],
        [
          "CRLJ 92.04",
          true,
          "row found, full_text len=71"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'civil' on CRLJ: 18 hits"
      ],
      "pattern_a": [
        false,
        "top10[:5]=['CRLJ 1', 'CRLJ 48', 'CRLJ 57', 'CRLJ 82', 'CRLJ 73'] anchor=CRLJ 30 MISS"
      ]
    },
    "JuCR": {
      "pattern_b": [
        [
          "JuCR 1.1",
          true,
          "row found, full_text len=200"
        ],
        [
          "JuCR 5A.1",
          true,
          "row found, full_text len=232"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'juvenile' on JuCR: 52 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['JuCR 1.2', 'JuCR 5.1', 'JuCR 7.1', 'JuCR 10.2', 'JuCR 4.1'] anchor=JuCR 1.1 HIT"
      ]
    },
    "SCCAR": {
      "pattern_b": [
        [
          "SCCAR 1.1",
          true,
          "row found, full_text len=399"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'arbitration' on SCCAR: 24 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['SCCAR 1.2', 'SCCAR 8.1', 'SCCAR 1.1', 'SCCAR 3.2', 'SCCAR 2.3'] anchor=SCCAR 1.1 HIT"
      ]
    },
    "RALJ": {
      "pattern_b": [
        [
          "RALJ 1.1",
          true,
          "row found, full_text len=2028"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'appeal' on RALJ: 27 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['RALJ 2.2', 'RALJ 11.1', 'RALJ 4.1', 'RALJ 1.1', 'RALJ 2.3'] anchor=RALJ 1.1 HIT"
      ]
    },
    "APR": {
      "pattern_b": [
        [
          "APR 15P",
          true,
          "row found, full_text len=24544"
        ],
        [
          "APR Regs 101",
          true,
          "row found, full_text len=195"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'admission' on APR: 25 hits"
      ],
      "pattern_a": [
        false,
        "top10[:5]=['APR 24.3', 'APR 20.1', 'APR 5', 'APR 3', 'APR 24.1'] anchor=APR 15P MISS"
      ]
    },
    "ELC": {
      "pattern_b": [
        [
          "ELC 1.1",
          true,
          "row found, full_text len=348"
        ],
        [
          "ELC 5.1",
          true,
          "row found, full_text len=6164"
        ],
        [
          "ELC 13.1",
          true,
          "row found, full_text len=708"
        ]
      ],
      "pattern_b_pass": true,
      "pattern_c": [
        true,
        "FTS 'discipline' on ELC: 37 hits"
      ],
      "pattern_a": [
        true,
        "top10[:5]=['ELC 9.4', 'ELC 9.3', 'ELC 1.1', 'ELC 13.1', 'ELC 1.5'] anchor=ELC 1.1 HIT"
      ]
    }
  },
  "pattern_a_hit": 12,
  "pattern_a_misses": [
    {
      "code": "RAP",
      "anchor": "RAP 18.13A",
      "semantic": "appeal procedure",
      "top10": [
        "RAP 18.9",
        "RAP 2.1",
        "RAP 16.8.1",
        "RAP 13.1",
        "RAP 16.13",
        "RAP 6.3",
        "RAP 18.1",
        "RAP 9.13",
        "RAP 13.5",
        "RAP 16.3"
      ]
    },
    {
      "code": "CRLJ",
      "anchor": "CRLJ 30",
      "semantic": "limited jurisdiction civil",
      "top10": [
        "CRLJ 1",
        "CRLJ 48",
        "CRLJ 57",
        "CRLJ 82",
        "CRLJ 73",
        "CRLJ 85",
        "CRLJ 27-37",
        "CRLJ 84",
        "CRLJ 39",
        "CRLJ 77"
      ]
    },
    {
      "code": "APR",
      "anchor": "APR 15P",
      "semantic": "admission and practice",
      "top10": [
        "APR 24.3",
        "APR 20.1",
        "APR 5",
        "APR 3",
        "APR 24.1",
        "APR 4",
        "APR 9",
        "APR 1",
        "APR 2",
        "APR 22"
      ]
    }
  ]
}
```

### 4_content_fidelity: PASS

```json
{
  "check": "4_content_fidelity",
  "pass": true,
  "samples": 20,
  "match": 20,
  "miss": 0,
  "details": [
    {
      "cite": "APR 25.4",
      "status": "match"
    },
    {
      "cite": "CJC 2.7",
      "status": "match"
    },
    {
      "cite": "CR 4.1",
      "status": "match"
    },
    {
      "cite": "CRLJ 47",
      "status": "match"
    },
    {
      "cite": "CrR 6.8",
      "status": "match"
    },
    {
      "cite": "CrRLJ 2.5",
      "status": "match"
    },
    {
      "cite": "ELC 8.10",
      "status": "match"
    },
    {
      "cite": "ER 413",
      "status": "match"
    },
    {
      "cite": "GALR 5",
      "status": "match"
    },
    {
      "cite": "GR 12.2",
      "status": "match"
    },
    {
      "cite": "JuCR 6.4",
      "status": "match"
    },
    {
      "cite": "RALJ 9.2",
      "status": "match"
    },
    {
      "cite": "RAP 15D",
      "status": "match"
    },
    {
      "cite": "RPC 6.2",
      "status": "match"
    },
    {
      "cite": "SCCAR 7.3",
      "status": "match"
    },
    {
      "cite": "RAP 15.4",
      "status": "match"
    },
    {
      "cite": "ELC 9.1",
      "status": "match"
    },
    {
      "cite": "CRLJ 63",
      "status": "match"
    },
    {
      "cite": "JuCR 8.1",
      "status": "match"
    },
    {
      "cite": "CrRLJ 8.1",
      "status": "match"
    }
  ]
}
```

### 5_citation_graph_density: PASS

```json
{
  "check": "5_citation_graph_density",
  "pass": true,
  "rules": 1145,
  "xrefs": 1866,
  "ratio": 1.6297,
  "by_ref_type": {
    "cross-statute": 954,
    "cross-rule": 362,
    "subsection-cite": 359,
    "authority": 175,
    "comment": 16
  },
  "by_jurisdiction": {
    "state-wa-rcw": 1041,
    "state-wa-court-rule": 812,
    "federal-usc": 9,
    "wa-constitution": 4
  }
}
```

### 6_ghost_flags: PASS

```json
{
  "check": "6_ghost_flags",
  "pass": true,
  "bad_repealed_count": 0,
  "bad_reserved_count": 0,
  "both_flag_set": [
    "CR 5",
    "CR 78",
    "CRLJ 5",
    "ER 301",
    "ER 302",
    "GR 8",
    "RAP 11.5",
    "RAP 11.6"
  ],
  "expected_both_flag_set": [
    "CR 5",
    "CR 78",
    "CRLJ 5",
    "ER 301",
    "ER 302",
    "GR 8",
    "RAP 11.5",
    "RAP 11.6"
  ]
}
```

### 7_range_expansion: PASS

```json
{
  "check": "7_range_expansion",
  "pass": true,
  "expansion_children": 58,
  "unresolved_doc_id": 0,
  "canonicals": 8,
  "self_consistency_violations": 0
}
```

### 8_parent_canon: PASS

```json
{
  "check": "8_parent_canon",
  "pass": true,
  "non_cjc_violations": 0,
  "cjc_distribution": {
    "Canon 1": 3,
    "Canon 2": 14,
    "Canon 3": 13,
    "Canon 4": 5
  },
  "cjc_missing": 0
}
```

### 9_parent_regulation_handle: PASS

```json
{
  "check": "9_parent_regulation_handle",
  "pass": true,
  "non_apr_violations": 0,
  "apr_distribution": {
    "LP_DIS_REGS": 1,
    "LP_REGS": 1,
    "Regs": 17
  },
  "both_pointer_violations": 0
}
```

### 10_four_jurisdiction_canary: PASS

```json
{
  "check": "10_four_jurisdiction_canary",
  "pass": true,
  "verdict": "PASS",
  "jurisdictions_present": 4,
  "buckets": {
    "court_rule": 1,
    "rcw_section": 2,
    "usc_section": 2,
    "wac_section": 25
  },
  "samples": {
    "court_rule": [
      {
        "title": "GR 33 \u2014 Requests for Accommodation by Persons with Disabilities",
        "dist": 0.4043
      }
    ],
    "rcw_section": [
      {
        "title": "RCW 2.56.210 \u2014 Court access and accommodations coordinator\u2014Duties",
        "dist": 0.3181
      },
      {
        "title": "RCW 28B.20.445 \u2014 Child and adolescent psychiatry residency program\u2014Requirement to offer additional residency position",
        "dist": 0.4128
      }
    ],
    "usc_section": [
      {
        "title": "28 U.S.C. \u00a7 462 \u2014 Court accommodations",
        "dist": 0.4083
      },
      {
        "title": "28 U.S.C. \u00a7 128 \u2014 Washington",
        "dist": 0.4135
      }
    ],
    "wac_section": [
      {
        "title": "WAC 132A-350-040 \u2014 Reasonable accommodations/academic adjustment for persons with disabilities",
        "dist": 0.344
      },
      {
        "title": "WAC 10-24-010 \u2014 Representation as an accommodation for a party with disabilities",
        "dist": 0.3518
      },
      {
        "title": "WAC 388-78A-2661 \u2014 Resident rights\u2014Transfer and discharge notice\u2014Residents with medicaid as a payor",
        "dist": 0.3868
      },
      {
        "title": "WAC 110-301-0130 \u2014 Indoor school-age program space",
        "dist": 0.3878
      },
      {
        "title": "WAC 381-10-150 \u2014 Reasonable accommodation/return to work policy",
        "dist": 0.3906
      }
    ]
  }
}
```

### 11_citation_roundtrip: PASS

```json
{
  "check": "11_citation_roundtrip",
  "pass": true,
  "samples": 20,
  "matches": 20,
  "misses": 0,
  "match_evidence": [
    {
      "xref_id": "165aa0f4-6412-400a-9321-2554f7d97fff",
      "src": "RAP 6.3",
      "target_citation": "RCW 34.05.518",
      "target_jurisdiction": "state-wa-rcw",
      "evidence": "rcw target_rule_number '34.05.518' substring"
    },
    {
      "xref_id": "4d34e3c9-6f19-4aa7-9361-2e19e2f84f3f",
      "src": "APR 28",
      "target_citation": "APR 3",
      "target_jurisdiction": "state-wa-court-rule",
      "evidence": "target_citation substring"
    },
    {
      "xref_id": "08723ce4-c136-4d6a-9e6a-4dd8e8f35617",
      "src": "GR 22",
      "target_citation": "CR 4.1",
      "target_jurisdiction": "state-wa-court-rule",
      "evidence": "target_citation substring"
    },
    {
      "xref_id": "3536b21b-b191-4953-b1b2-229a3a84874f",
      "src": "RALJ 11.7",
      "target_citation": "CR 1",
      "target_jurisdiction": "state-wa-court-rule",
      "evidence": "target_citation substring"
    },
    {
      "xref_id": "88e4faf4-ab30-4f1b-89c1-13a3b8dd3af3",
      "src": "JuCR 7.4",
      "target_citation": "RCW 13.40.050",
      "target_jurisdiction": "state-wa-rcw",
      "evidence": "rcw target_rule_number '13.40.050' substring"
    }
  ],
  "miss_records": []
}
```
