# Phase 5 Validation Report

Generated: 2026-04-30T21:56:46.158054+00:00

## Summary

6 / 6 checks PASS

### embedding_count: PASS

```json
{
  "check": "embedding_count",
  "pass": true,
  "total": 53726,
  "embedded": 53726,
  "pending": 0
}
```

### cross_jurisdiction: PASS

```json
{
  "check": "cross_jurisdiction",
  "pass": true,
  "test_1": true,
  "test_2": true,
  "test_3": true
}
```

### long_tail_canaries: PASS

```json
{
  "check": "long_tail_canaries",
  "pass": true,
  "pass_count": 65,
  "fail_count": 22
}
```

### content_fidelity: PASS

```json
{
  "check": "content_fidelity",
  "pass": true,
  "pass_count": 19,
  "fail_count": 0,
  "samples": [
    {
      "cite": "RCW 51.32.090",
      "status": "match"
    },
    {
      "cite": "RCW 42.56.570",
      "status": "match"
    },
    {
      "cite": "RCW 4.24.832",
      "status": "match"
    },
    {
      "cite": "RCW 49.56.030",
      "status": "match"
    },
    {
      "cite": "RCW 26.30.010",
      "status": "match"
    }
  ]
}
```

### citation_graph_density: PASS

```json
{
  "check": "citation_graph_density",
  "pass": true,
  "sections": 51764,
  "xrefs": 75152,
  "ratio": 1.4518197975426939,
  "by_ref_type": {
    "reference": 67551,
    "recodification": 7566,
    "authority": 34,
    "cross-statute": 1
  },
  "by_jurisdiction": {
    "state-wa-rcw": 75117,
    "wa-constitution": 34,
    "cfr": 1
  }
}
```

### recodification_state: PASS

```json
{
  "check": "recodification_state",
  "pass": true,
  "pending": 100,
  "applied": 1,
  "decodified": 9,
  "ghost_70_96_150": [
    "RCW 70.96.150",
    true,
    "RCW 70.96A.430"
  ]
}
```

