# WA Court Rules — Phase 6 Retrieval Guide Notes

**Status:** Phase 6 closeout — final docs prior to HARD STOP.
**Date:** 2026-05-01 (refined post-Task-4-deferred-set-ingest 2026-05-01)
**Corpus:** 1,286 rules across 30 rule sets (15 originally in-scope + 15 Task-4
deferred sets), 2,072 cross-references across 6 populated jurisdictions, ≈1,547
embedding chunks. Two skipped sets (BBP, LLLT RPC) carry documented
non-ingestable dispositions.

This document captures retrieval patterns, four-jurisdiction unified-query
architecture, methodology corroborations (#14-22 in-file; #23-30 in
`/opt/wdws/usc_research/phase6_retrieval_guide_notes.md`; #31-32 awaiting
cross-project canonization; #33 in-file; #34-38 in prior project canons; #39
introduced by the refinement-sweep agent at Task-4 close), documented
limitations (§H2), and Phase 7 backlog.

---

## §K refinement-sweep additions (introduced 2026-05-01 at Task-4 close)

### Corroboration #29 — REINFORCEMENT: session-memory inflation pattern

**Reinforcement note:** #29 (per-source pre-flight inspection before locking
projection) is now reinforced with a recurring observation: when projections
come from coordinator/session-memory aggregates rather than per-source
ground-truth verification, they consistently inflate by 2-5×. Three instances
this session:

1. **USC tier estimates** — coordinator-projected long-tail tiers ran
   substantially higher than per-title XML pre-flight measurement.
2. **RCW Phase 8 ghost-row Pattern 1/2** — coordinator-projected ghost-row
   counts ran higher than chapter-anchor ground-truth verification (operator
   reconciliation gave ground-truth 96 vs agent's 79; pattern of
   coordinator-source over-projection).
3. **WCR Task 4 deferred-set ingest (this run)** — session memory projected
   "~218 new rules" from deferred sets; agent's per-set inventory inspection
   found 141 ingestable rules (overstated by 55%).

**Generalization:** projections from coordinator/session-memory should be
treated as upper-bound estimates only. Always verify via per-source pre-flight
(read the actual list page / inventory / source artifact) before locking
halt-band invariants. The inflation is structural, not random — coordinator
estimates derive from prior project memory and aggregate residuals across
heterogeneous resolution paths, while per-source counts measure the specific
addressable artifact.

**Mitigation pattern:** in any kickoff that cites a session-memory aggregate
as the projection denominator, the agent's first pre-flight task should be to
independently verify that aggregate against per-source measurement and
surface any divergence > 10% before consuming it as the basis for halt bands.

### Corroboration #39 — Residual NULL classification dual-axis

**Failure mode:** when a backlog item's stated benefit is "X residuals become
resolvable," the projection assumes all X residuals share a single resolution
path. In practice, an aggregated residual count often masks a multi-cause
root structure where only a subset shares the proposed work item's
resolution path; the rest require orthogonal work (parser refinement, schema
extension, separate ingest).

**Generalizes:** #38 (classification refinement under halt-band feedback) —
applied to backlog items with multi-cause root structure rather than to
halt-band tuning.

**Audit channel:** before authorizing a backlog item that promises "X
residuals resolved," classify each of the X residuals across both axes:

1. **Jurisdiction axis** — what corpus is the target citation in?
2. **Resolution-path axis** — does the target row exist (FK resolves on
   re-run)? Does the target row exist but the citation has subsection /
   chapter / pin-cite form the parser failed to canonicalize? Is the target
   corpus not yet ingested?

Lock the projection to the subset that actually shares the proposed work
item's resolution path; surface the remaining residuals as separate backlog
items requiring distinct work.

**Concrete instance (WA Court Rules Task 4 pre-flight, 2026-05-01):**
- Session memory projected: "21 residuals resolved by Task 4 deferred-set
  ingest" → projection of WCR intra-corpus FK climb 97.41% → ~99.50%
- Agent per-residual classification:
  - 6 of 21 target deferred sets (DRJ:14(e), IRLJ:1.2(j)/2.1/2.6(b)/2.6(c)(2),
    SPRC:3) — share Task-4 ingest resolution path
  - 15 of 21 target already-ingested sets (APR×9, CRLJ×1, CrR×2, RALJ×2,
    RPC×1) — subsection pin-cites and chapter-only refs requiring
    parser-refinement work, NOT deferred-set ingest
- Reconciled projection: 97.41% → ~98.15% (+6 from Task 4; 15 deferred to
  Phase 7 parser-refinement backlog)
- Phase 4.4 post-Task-4 idempotent re-run: actual delta +6 (exactly within
  reconciled projection halt band [+5, +10])

**Mitigation pattern:** for any "X residuals" projection, draft a
per-residual resolution-path table during pre-flight; the table's row count
sharing the proposed work item's path is the locked halt-band projection,
not the raw aggregate count.

---

## §S — WA Court Rules retrieval patterns

### §S.1 Citation form table

Across the four legal corpora in ACE, citation formats differ structurally.
Retrieval queries must match the source corpus's convention.

| Corpus | Citation form | Example | Storage in `canonical_citation` |
|---|---|---|---|
| Federal USC | `<title> U.S.C. § <section>` (dotted form most common) | `42 U.S.C. § 12132` | `42 U.S.C. § 12132` |
| Federal USC range | `§§ <start>-<end>` | `42 U.S.C. §§ 12101-12213` | `42 U.S.C. § 12101-12213` (single-§ in storage) |
| State RCW (standard) | `RCW <title>.<chapter>.<section>` dotted | `RCW 4.28.080` | `RCW 4.28.080` |
| State RCW (UCC) | `RCW 62A.<chapter>-<section>` dash form, Title 62A only | `RCW 62A.9-310` | `RCW 62A.9-310` |
| State RCW chapter-form | `chapter <T>.<C> RCW` or `ch. <T>.<C> RCW` | `chapter 49.60 RCW` | `RCW 49.60` (canonicalized) |
| State WAC section | `WAC <title>-<chapter>-<section>` dash form | `WAC 162-22-010` | `WAC 162-22-010` |
| State WAC chapter | `chapter <T>-<C> WAC` (Phase 7 backlog — see §H2) | `chapter 448-12 WAC` | n/a in this run |
| Court rule (numbered) | `<SET> <num>` space-separated | `GR 31`, `RPC 1.1`, `CR 12` | `GR 31` |
| Court rule (CJC inner) | `CJC <canon>.<rule>` | `CJC 2.11` | `CJC 2.11` (parent_canon='Canon 2') |
| Court rule (CJC index) | `CJC Canon <N>` (NOT a row — index/heading form only) | `CJC Canon 2` | resolves via `parent_canon='Canon 2'` query |
| Court rule (range) | `<SET> <start>-<end>` | `CRLJ 27-37` | canonical row + N expanded children |
| Court rule (APR irregular) | `APR Regs/<N>`, `APR LP_REGS`, `APR LP_DIS_REGS`, `APR 15P` | `APR Regs/101` | `APR Regs/101` (parent_regulation_handle='Regs') |
| WA Constitution | `Wash. Const. art. <I>, § <N>` (explicit) or `Article <I> § <N> of the Washington Constitution` (narrative) | `Wash. Const. art. I, § 3` | `Wash. Const. art. I, § 3` |

**Rule for parser authors:** match the literal cited form in body text; canonicalize at storage time. Cross-reference parser must accept both citation
variants (e.g., RCW chapter-form `ch. 49.60` and `chapter 49.60`) and
canonicalize to a single `target_citation` form.

### §S.2 Ghost-flag filtering patterns

Retrieval queries against `wa_court_rules.rules` should respect three
ghost-flag classes:

```sql
-- Active rules only (operationally most common)
SELECT * FROM wa_court_rules.rules
WHERE NOT is_repealed AND NOT is_reserved;

-- Include rescinded for historical research
SELECT * FROM wa_court_rules.rules WHERE is_repealed;

-- Include reserved-via-range for citation resolution
-- (returns the placeholder row + lets caller JOIN to canonical)
SELECT child.canonical_citation, parent.range_start, parent.range_end
FROM wa_court_rules.rules child
LEFT JOIN wa_court_rules.rules parent ON parent.id = child.range_doc_id
WHERE child.is_reserved AND child.range_doc_id IS NOT NULL;

-- Both-flag rows (rescinded AND reserved — there are 8: GR 8, CR 5, CR 78,
-- CRLJ 5, ER 301, ER 302, RAP 11.5, RAP 11.6)
SELECT * FROM wa_court_rules.rules WHERE is_repealed AND is_reserved;
```

Tag-based filtering (preferred for retrieval-time gates):

```sql
-- All rules tagged court-rule-rescinded
SELECT r.* FROM wa_court_rules.rules r
JOIN core.documents d ON d.id = r.document_id
JOIN core.document_tags dt ON dt.document_id = d.id
JOIN core.tags t ON t.id = dt.tag_id
WHERE t.slug = 'court-rule-rescinded';
```

### §S.3 Range-canonical join example

The 8 reserved-range entries each have one canonical row + N expanded children
pointing to it via `range_doc_id`. To navigate from a child cite to its
range context:

```sql
-- "What range does CRLJ 92.04 belong to?"
SELECT child.canonical_citation AS child_cite,
       parent.canonical_citation AS range_canonical,
       parent.range_start, parent.range_end
FROM wa_court_rules.rules child
JOIN wa_court_rules.rules parent ON parent.id = child.range_doc_id
WHERE child.canonical_citation = 'CRLJ 92.04';
-- → CRLJ 92.04 | CRLJ 86.04--99.04 | 86.04 | 99.04
```

The 8 canonicals: `CR 72-76`, `CRLJ 27-37`, `CRLJ 65-67`, `CRLJ 69-70`,
`CRLJ 78-80`, `CRLJ 86.04--99.04`, `JuCR 11.4-11.20`, `RAP 18.18 - 18.20`.

Total range-related rows: 8 canonicals + 58 expansion children = 66.

### §S.4 parent_canon navigation pattern (CJC)

CJC's 4 Canons unpack into 35 inner rules. Canon-level rows are NOT ingested
(Phase 0 Decision 5); each inner rule has `parent_canon='Canon N'`. Retrieval:

```sql
-- "All rules under Canon 2"
SELECT canonical_citation, rule_title FROM wa_court_rules.rules
WHERE rule_set_code = 'CJC' AND parent_canon = 'Canon 2'
ORDER BY rule_number;
-- 14 rows: CJC 2.1, 2.2, ..., 2.9 (note: not 1-14 sequential — see Phase 3 unpacking)

-- "Where is CJC 2.11 (Disqualification)?"
SELECT canonical_citation, rule_title, parent_canon FROM wa_court_rules.rules
WHERE canonical_citation = 'CJC 2.11';
-- → CJC 2.11 | Disqualification | Canon 2
```

**Citation form `CJC Canon 1` is NOT a row** — it's the index/heading form. For
deterministic lookup matching `Canon N` form, query `parent_canon` directly:

```sql
SELECT * FROM wa_court_rules.rules WHERE parent_canon = 'Canon 1';
```

### §S.5 parent_regulation_handle navigation pattern (APR)

19 APR rows have irregular handles (Phase 0 Gate 2, Option B):
- `Regs` → 17 rows (`Regs/101` through `Regs/117`, all `is_repealed=TRUE`,
  CLE Board regulations deleted 2016)
- `LP_REGS` → 1 row (`is_reserved=TRUE`, Limited Practice Board appendix)
- `LP_DIS_REGS` → 1 row (`is_repealed=TRUE`, last_amended 2017-12-05)

Plus `APR 15P` is handled via the main numbered-rule path (`15P` matches the
main regex). Retrieval:

```sql
-- "All Continuing Legal Education Board regulations (deleted)"
SELECT canonical_citation, rule_title FROM wa_court_rules.rules
WHERE rule_set_code = 'APR' AND parent_regulation_handle = 'Regs'
ORDER BY rule_number;

-- "Cross-reference to APR LP_REGS specifically"
SELECT * FROM wa_court_rules.rules
WHERE canonical_citation = 'APR LP_REGS';
```

### §S.6 Routing duality — the architectural surprise from Check 10

**Phase 5 Check 10** (four-jurisdiction unified canary on "ADA accommodation in
Washington courts") returned **4/4 jurisdictions in top-30** including 25 WAC
sections — yet `wa_court_rules.cross_references` has **zero** state-wa-wac
rows. The WAC content surfaced via embedding similarity, not deterministic
cross-reference traversal.

This is the **dual-routing architecture**:

1. **Deterministic routing** (cross_references FK chain): explicit citation in
   body text → parsed cross_reference row → FK-resolved target.
   Strengths: provenance, exact match, audit trail. Weaknesses: only works
   when citation format matches parser regex AND target corpus is ingested.

2. **Semantic routing** (embedding similarity): query → top-N nearest chunks
   across all corpora regardless of citation graph.
   Strengths: surfaces topically-related content even without explicit
   citations; survives parser scope gaps; cross-corpus by default.
   Weaknesses: no provenance trail; relevance varies by query phrasing.

**Both are operational paths; neither subsumes the other.** Researchers
working an issue should use both: deterministic routing for "trace this
citation" tasks, semantic routing for "find related rules across
jurisdictions" tasks.

For §T four-jurisdiction queries: combining the two routes is most
powerful — start with deterministic path from a known anchor (e.g., GR 33
→ RCW 49.60 via FK), then expand to semantic neighbors of each hop's body.

### §S.7 Self-cite filter behavior at FK Pass 2

Phase 4 `insert_cross_references()` filters out self-citations: when a rule
cites its own canonical citation (every PDF self-cites its own heading line
at body parse time), the cross-ref is dropped.

Filter logic ([phase4_ingest.py](phase4_ingest.py)):
```python
if t_set == src_code and t_num == src_num:
    skipped_self += 1
    continue
```

In Phase 4 Tier 1+2: **1,069 self-cites filtered out** of 2,935 raw cross-refs
captured by Phase 3 parser → 1,866 net stored. Filter does NOT trigger on
cross-jurisdiction targets (target_set_code='RCW' will never match a court
rule's rule_set_code), so cross-jurisdiction cross_references are preserved.

### §S.8 Comment-section retrieval (RPC + CJC + ER)

187 rules have `has_comment=TRUE` with substantive `comment_text`. Three
extraction patterns:

- **RPC `[1]` ... `[N]` numbered comments** + "Additional Washington Comments"
  section + `[Comment N Adopted DATE]` history brackets + `[Washington
  revision]` inline tags
- **CJC nested comments** inside Canon PDFs alongside RULE N.M chunks
- **ER 'Comment N' rule-numbered headings** (variant: `Comment 401` instead
  of generic `Comment`)

Comment retrieval query:

```sql
-- "Show me rules with substantive interpretive commentary"
SELECT canonical_citation, rule_title, length(comment_text) AS comment_len
FROM wa_court_rules.rules
WHERE has_comment AND length(comment_text) > 500
ORDER BY canonical_citation;

-- "Find comments mentioning a specific concept"
SELECT canonical_citation, comment_text
FROM wa_court_rules.rules
WHERE has_comment AND comment_text ILIKE '%competence%';
```

Comment text is also embedded in `core.document_chunks` (parser merges
`comment_text` into the rule's full content for embedding purposes), so
semantic search returns commentary alongside rule body.

### §S.9 Phase-8-deferred cross-reference accounting

17 Phase-8-deferred rule sets (CCR, IRLJ, AR, ARLJ, BJAR, DRJ, JISCR, SPR,
SPRC, LLLT RPC, ELLLTC, ELPOC, LPORPC, BBP, ATJ, BJA, SCAR) are recognized
in citation parsing but not ingested. Their cross-references resolve to
`target_jurisdiction='state-wa-court-rule'` + `target_rule_id=NULL`.

Authority-resolution rate accounting must distinguish:
- "unresolved because target rule does not exist" (bad citation in source)
- "unresolved because target FK target not yet ingested" (Phase 8 candidate)

A `target_rule_id IS NULL` alone doesn't disambiguate. Use `target_set_code`
membership in the deferred-set list:

```sql
WITH deferred AS (
  SELECT unnest(ARRAY[
    'CCR','IRLJ','AR','ARLJ','BJAR','DRJ','JISCR','SPR','SPRC',
    'LLLT RPC','ELLLTC','ELPOC','LPORPC','BBP','ATJ','BJA','SCAR'
  ]) AS code
)
SELECT
  COUNT(*) FILTER (WHERE target_rule_id IS NOT NULL)                AS resolved,
  COUNT(*) FILTER (WHERE target_rule_id IS NULL
                   AND target_set_code IN (SELECT code FROM deferred)) AS deferred,
  COUNT(*) FILTER (WHERE target_rule_id IS NULL
                   AND target_set_code NOT IN (SELECT code FROM deferred)) AS genuinely_missing
FROM wa_court_rules.cross_references
WHERE target_jurisdiction = 'state-wa-court-rule';
```

When Phase 8 ingest of a deferred set runs, the FK resolves retroactively
without schema change.

---

## §T — Four-jurisdiction unified queries

### §T.1 Canonical example: "ADA accommodation in WA courts"

**Phase 5 Check 10 result (verified 2026-05-01):**

Top-30 results across all 4 in-scope corpora (USC, RCW, WAC, court rules):
- 1 court_rule (GR 33 itself)
- 2 rcw_section (WLAD provisions, RCW 49.60.x)
- 2 usc_section (federal ADA, 42 U.S.C. 12101+)
- 25 wac_section (HRC enforcement regulations, WAC 162-x)

This is the §T premise operational. A researcher querying "ADA accommodation
in Washington courts" sees:
- The applicable WA court rule (GR 33)
- The state statute (WLAD)
- The federal statute (ADA Title II)
- The state administrative code (HRC enforcement)

All 4 jurisdictions surface in a single query.

### §T.2 Cross-jurisdiction multi-hop SQL pattern

```sql
-- Start from a court rule, traverse cross_references to other jurisdictions
WITH start_rule AS (
  SELECT id FROM wa_court_rules.rules WHERE canonical_citation = 'GR 33'
)
SELECT DISTINCT
  c.target_jurisdiction,
  c.target_citation,
  c.ref_type
FROM wa_court_rules.cross_references c
WHERE c.source_rule_id = (SELECT id FROM start_rule);
-- For GR 33: returns 3× RCW 49.60 (chapter, ref_type='authority') +
--           3× 42 U.S.C. § 12101-12213/12231 (range, ref_type='cross-statute')
```

**Two-hop pattern** (court rule → RCW → WAC):

```sql
-- "Trace authority chain from GR 33 to WAC enforcement regulations"
WITH gr33 AS (
  SELECT id FROM wa_court_rules.rules WHERE canonical_citation = 'GR 33'
),
rcw_targets AS (
  SELECT DISTINCT target_citation FROM wa_court_rules.cross_references
  WHERE source_rule_id = (SELECT id FROM gr33)
    AND target_jurisdiction = 'state-wa-rcw'
),
rcw_secs AS (
  SELECT id FROM rcw.sections
  WHERE canonical_citation IN (SELECT target_citation FROM rcw_targets)
)
SELECT wac_xref.target_citation, wac_xref.ref_type
FROM rcw.cross_references wac_xref
WHERE wac_xref.source_section_id IN (SELECT id FROM rcw_secs)
  AND wac_xref.target_jurisdiction = 'state-wa-wac';
```

Two-hop chain works because each corpus's cross_references table is
symmetric: RCW parser extracted court-rule cross-refs in RCW bodies; WAC
parser extracted RCW cross-refs in WAC bodies; etc.

### §T.3 Embedding-based fallback

When deterministic routing returns nothing (e.g., a court rule citation
isn't extractable due to parser-scope gap), semantic routing surfaces
related content via embedding similarity:

```python
# Pseudocode using pgvector
query_embedding = bge_m3_encode("ADA accommodation in Washington courts")
sql = """
SELECT d.document_type, c.content,
       d.title, c.embedding <=> %s AS distance
FROM core.document_chunks c
JOIN core.documents d ON d.id = c.document_id
WHERE d.domain = 'legal'
  AND d.document_type IN ('court_rule', 'rcw_section', 'usc_section', 'wac_section')
ORDER BY c.embedding <=> %s
LIMIT 30;
"""
```

This is the path Phase 5 Check 10 used. It returned 25 WAC sections topically
related to ADA enforcement even though no court-rule body cites WAC at all.

### §T.4 Cross-reference resolution accounting (Phase 5 Check 2 breakdown)

Verified 2026-05-01:

| Jurisdiction | Total | Resolved | Rate | Notes |
|---|---:|---:|---:|---|
| state-wa-court-rule | 812 | 791 | **97.4%** | In-corpus FK resolution; 21 unresolved are mostly Phase-8-deferred |
| state-wa-rcw section-form | 818 | 767 | **93.8%** | FK against `rcw.sections.canonical_citation` (3-segment) |
| state-wa-rcw chapter-form | 223 | n/a | n/a | Chapter-level cites have no specific section target — `target_rule_id` legitimately NULL |
| federal-usc | 9 | 1 | 11.1% | USC long-tail not in priority core (§H2, Phase 7 backlog) |
| wa-constitution | 4 | n/a | n/a | Text-only; no FK target table |
| state-wa-wac | 0 | n/a | n/a | Phase 7 backlog (WAC chapter-form pattern) |

**Total cross-references: 1,866** across 4 populated jurisdictions.

---

## §K — Methodology corroborations (#14-22)

Carrying forward 13 §K corroborations from prior projects (USC, RCW, WAC).
These 9 new corroborations were observed during the WA Court Rules ingest run.

The audit-predicate-diversification doctrine spans three category families:
- **Schema-shape audits** (CHECK constraints) — corroborations #1-9, #17
- **Data-extraction audits** (programmatic vs LLM) — corroborations #8, #10, #12, #15, #20, #22
- **Process-state audits** (system introspection vs narrative) — corroboration #14
- **Doctrine-vs-symptom audits** (canary semantics) — corroborations #11, #18, #21

### Corroboration #14 — Subagent vs process-state audit divergence

**Failure mode:** dispatched LLM subagent reports completion via narrative ("Monitor is armed; fetcher running") while underlying spawned process continues running. Operator cannot trust agent narrative as authoritative.

**Audit channel that caught it:** process-state introspection — `ps -ef | grep` from parent shell + `wc -l` on output file at intervals. Both are independent of agent narrative.

**Phase 2 incident:** Phase 2 acquisition subagent reported back at 135 sec; underlying Python fetcher kept running 12 more min, completed 1035/1039 fetches with 4 errors. Operator perception at 135 sec showed only 23 PDFs cached and no fetcher process visible to a `ps` introspection from the parent shell — would have suggested termination if narrative was trusted.

**Generalization:** for any dispatched fetch/ingest/long-running work, prefer process-state audit channels (PID alive, output file mtime stable for N seconds, child-process tree empty) over agent self-report. Audit-predicate diversification: agent narrative + process introspection are structurally different channels — use both.

**Phase 7 lesson:** when scheduling cron jobs that spawn subprocesses, add a process-state probe (e.g., a separate cron entry that checks "is the spawning agent's PID still alive AND its output file mtime older than 5 min?" → if yes, agent crashed; the lock file alone won't catch this).

### Corroboration #15 — Sub-structure-vs-citation-pattern audit divergence

**Failure mode:** internal document structure (sub-regulations, sub-sections, sub-rules) does not always correspond to citation patterns in the actual corpus. Expanding internal sub-structure into separate rows when citations don't reference sub-structure creates unreachable rows.

**Audit channel that caught it:** corpus-grep for "APR 15P Regulation N" / "APR 15P Reg. N" / "APR 15P §N" patterns across 1,040 PDFs — 0 hits.

**Phase 3 incident:** APR 15P (Client Protection Fund Procedural Regulations) has 13 internal Regulations (Regulation 1, 2, ..., 13). Default architectural assumption: expand to 13 rows per Phase 0 Decision 5 doctrine (CJC parallel). Corpus grep verified 0 citations to "APR 15P Regulation N" form — operators don't cite sub-regulations of APR 15P. Correct treatment: 1 row containing all 13 Regulations in source order.

**Generalization:** before expanding internal sub-structure into separate rows, audit citation patterns in the corpus to verify the expansion matches how content is actually cited. The audit channel for citation-pattern (corpus-grep) is structurally different from the audit channel for sub-structure (parser internal extraction); these can diverge.

**Phase 7 lesson:** when an amendment introduces a rule with sub-numbering, run the corpus-grep audit against the new rule's citation form before deciding row architecture.

### Corroboration #16 — Parser-level over-matching caught by sample-axis methodology

**Failure mode:** production parsers may over-match patterns that look correct in isolation but fail on actual corpus samples.

**Audit channel that caught it:** sample-axis methodology — testing parser predicates against actual corpus samples covering quality + coverage axes (corroboration #12 generalizes here), not against hypothetical edge cases.

**Phase 3 incidents:**
- CJC inner-rule heading regex over-matched mixed-case "Rule 2.11" inline references (caught 7 false-positive duplicates). Resolution: tightened to require uppercase `RULE` + same-line title.
- `is_reserved`/`is_repealed` whole-rule detection mis-flagged on inline `(4) [Reserved.]` sub-paragraphs and `Comment 401 [Deleted...]` child blocks. Resolution: required short body OR marker on first substantive line.

**Generalization:** the audit channel for parser correctness is corpus-sample evidence, not regex unit tests alone. Test parser predicates against a sample of ≥5 actual rules per rule set covering the patterns the parser recognizes, before declaring patterns complete.

**Phase 7 lesson:** when extending parser regex with new variants, add a corpus-sample run against ≥5 rules per affected set BEFORE declaring T17 GREEN sufficient.

### Corroboration #17 — Ghost-flag CHECK constraints based on idealized assumption

**Failure mode:** original schema CHECK constraints based on ghost-flag-implies-empty-body assumption may not match actual corpus shape.

**Audit channel that caught it:** Phase 4 boundary halt at first `chk_rule_text_status_invariant` violation (GR 8 INSERT) → programmatic scan of `phase3_parsed.jsonl` revealed 47 rows in the same failure class.

**Phase 4 incident:** schema design assumed `is_repealed=TRUE` rows would have empty `full_text`. Actual corpus: 47 of 51 repealed-only rows carry meaningful placeholder markers like `[RESCINDED]`, `[Deleted]`, `[Rescinded. Provisions transferred to rule 13.]`. Resolution: Path 1 loosen — extend the `is_reserved` CHECK Path 1 treatment to `is_repealed`. Schema CHECKs should encode actual corpus shape, not idealized assumptions about ghost-flag semantics.

**Generalization:** the audit channel for ghost-flag status is the ghost-flag column itself + topical tag (e.g., `court-rule-rescinded`), NOT the body-text emptiness. CHECK constraints should validate structural invariants the corpus actually has, not the schema designer's idealized assumptions about what those invariants should be.

**Phase 7 lesson:** when a CHECK fires on a class of >5 rows, halt and audit the assumption — the schema may be over-strict relative to corpus reality.

### Corroboration #18 — Canary anchor doctrinally invalid

**Failure mode:** Pattern (b) deterministic citation lookup expected an anchor that doctrinally doesn't exist as a row.

**Audit channel that caught it:** Phase 4 Tier 1 canary halt on "Pattern (b) FAIL on CJC: ['CJC Canon 1' expected 1 row, got 0]" → architectural-decision documentation review (Phase 0 Decision 5 — CJC inner-rule unpacking, no Canon-level rows).

**Phase 4 incident:** canary spec listed `CJC Canon 1` as a Pattern (b) extra anchor. Per Phase 0 Decision 5, Canon-level rows are NOT ingested as `wa_court_rules.rules` rows — they're a `parent_canon` attribute on inner rules. The canary's anchor was doctrinally invalid, not a corpus failure. Resolution: replace `CJC Canon 1` extras with `CJC 1.1` (first inner rule under Canon 1).

**Generalization (inverse of feedback_canary_criterion_doctrine):** when Pattern (b) FAILS, investigate whether the anchor specification matches the architectural decisions before treating as corpus failure. Canary specs derived from doctrinal terminology ("Canon 1") may not match data model decisions (`CJC 1.1` with `parent_canon='Canon 1'`). The audit channel for canary-spec validity is the architectural-decision documentation (Phase 0 records); halt-and-investigate before assuming corpus error.

**Phase 7 lesson:** when porting canary specs across projects or revising them, cross-validate against current schema definitions and Phase 0 architectural decisions. Update canary anchor lists when ingest architecture changes.

### Corroboration #19 — Resume-mode pre-flight defensive halt

**Failure mode:** resume-mode ingest scripts that don't distinguish between "fully ingested" and "partially ingested" state per logical unit risk double-INSERT or silent partial-skip.

**Audit channel that caught it:** Phase 4 boundary halt at first repealed row → operator-approved Path B (resume mode) instead of destructive wipe-and-rerun → defensive halt added to pre-flight covering "all already-ingested units must be at expected row count" assertion.

**Phase 4 incident:** when repealed-row CHECK loosening required re-running Phase 4, choosing between Path A (wipe + restart, destructive) and Path B (resume from where we stopped). Path B selected with defensive guard: pre-flight halts if any already-ingested set has count != expected. Prevents resuming on top of partial state.

**Generalization:** resume-mode ingest scripts must distinguish between "fully ingested" and "partially ingested" state per logical unit (rule set, title, chapter, etc.). Allowing resume only when all already-ingested units are at expected row counts prevents double-INSERT or silent partial-skip. The "rules empty" assertion was over-strict for the post-halt state; the "all already-ingested at expected count" assertion is the correct generalization.

**Phase 7 lesson:** all ingest scripts should be resume-aware with this pattern (capture per-unit row counts at pre-flight; halt on partial-count mismatch). Defaulting to wipe-and-restart wastes embedding-call budget unnecessarily.

### Corroboration #20 — Parser scope vs full citation graph

**Failure mode:** corpus parser scoped to intra-corpus citations only silently drops cross-jurisdiction citations.

**Audit channel that caught it:** Phase 5 Investigation 2 — `cross_references.target_jurisdiction GROUP BY` showed 0 rows for state-wa-rcw, federal-usc, state-wa-wac, wa-constitution.

**Phase 4 incident:** WA Court Rules parser was scoped to court-rule citations only (CJC + APR_HANDLE + main court-rule patterns); silently dropped 945 RCW + 9 USC + 4 WA-Const + 0 WAC citations corpus-wide (954 cross-jurisdiction citations, ~65% of legitimate cross-ref graph). Resolution: extend parser regex set to symmetric pattern with sibling corpora's parsers (RCW + WAC). Time to fix: ~30 min (regex constants + 6 unit tests + re-parse + cross_refs re-ingest).

**Generalization:** the audit channel for cross-jurisdiction citation completeness is `cross_references.target_jurisdiction GROUP BY` — if it shows only the source corpus's own jurisdiction, parser scope is deficient. RCW + WAC parsers correctly extract court-rule citations as cross-references; court-rule parser must reciprocally extract RCW/USC/WAC/wa-constitution citations. Architectural inheritance is symmetric; asymmetric extraction breaks four-jurisdiction unified retrieval.

**Phase 7 lesson:** before declaring four-jurisdiction unified retrieval operational on a new corpus, run the GROUP BY audit. If the result shows only one jurisdiction, the parser is scoped wrong.

### Corroboration #21 — Halt-trigger semantic distinction (symptom-based vs cause-based gating)

**Failure mode:** halt triggers worded by symptom may fire on multiple distinct failure modes with different remediation paths.

**Audit channel that caught it:** Phase 5 WAC chapter-form gap analysis — single corpus citation `WAC 448-12` (CrRLJ 6.13) is chapter-form (2-segment); WAC_CITE_RE requires section-form (3-segment). Strict reading of "state-wa-wac resolution rate <50% → halt" would fire (0% capture rate is technically <50%). Cause-analysis distinguished: this is missing pattern variant (case b), not wrong pattern matching format-correct content (case a) or corpus-genuine 0-content (case c).

**Phase 5 incident:** halt-trigger language is conservative-by-design but requires cause-based judgment to apply correctly. Agent surfaced the distinction; operator approved Phase 7 backlog treatment (Option 1: accept + document) rather than immediate fix.

**Generalization:** when a halt symptom triggers, distinguish between:
- (a) **wrong pattern matching format-correct content** (schema/parser drift — fix immediately)
- (b) **missing pattern variant for legitimate but uncaptured content** (Phase 7 backlog acceptable)
- (c) **corpus-genuine 0-content case** (no remediation needed)

Symptom-based halt triggers are conservative-by-design but require cause-based judgment to apply correctly.

**Phase 7 lesson:** when authoring halt triggers, include a cause-classification framework alongside the symptom threshold. Halt triggers should specify: "halt and surface for cause classification before remediation."

### Corroboration #22 — Citation regex incomplete-capture vs no-capture failure modes

**Failure mode:** citation regex defects come in two flavors:
- (a) **no capture** (missing pattern variant — caught by jurisdiction-breakdown audit channel as in #20)
- (b) **incomplete capture** (regex matches but truncates at boundary — caught only by spot-check or downstream FK resolution failure)

Failure mode (b) is more dangerous in legal work because it produces malformed records that look authoritative ('42 U.S.C. § 12101-' with trailing dash; '25 U.S.C. § 1901et' with et-bleed).

**Audit channel that caught it:** GR 33 cross-ref spot-check during Phase 5 verification (operator anticipated GR 33 → RCW 49.60 cross-ref; investigation found USC trailing-dash truncation in 3 GR 33 rows + et-bleed in 1 APR 8 row).

**Phase 5 incident:** original USC regex section group `[0-9A-Z]+(?:\([0-9a-zA-Z]+\))*[a-zA-Z\-]*` allowed trailing letters/dash but didn't continue past dash to capture range end. Body "42 U.S.C. §§ 12101-12213" → captured `12101-` (truncated). Body "25 U.S.C. § 1901et seq." (no space) → captured `1901et` (et-bleed). Resolution: range-tail extension `(?:-[0-9]+[a-zA-Z]?)?` + `normalize_usc_section()` post-capture trim helper.

**Audit channel addition (T17.35):** roundtrip test — for each cross_reference, verify `target_citation` value re-matches against source rule body text. Mismatch indicates truncation. Phase 5 Check 11 production-DB version samples 20 random cross_references rows.

**Generalization:** existing CHECK constraints validate jurisdiction enum membership, not citation format. The audit channel for incomplete-capture is regex-roundtrip test (re-extract from `cross_references.target_citation` against original body text; mismatch indicates truncation). Add T17 roundtrip test when extending citation regex with range or alternation patterns.

**Phase 7 lesson:** when extending citation regex with range, alternation, or word-boundary edge cases, add a roundtrip test to the regex test suite + a periodic production sample audit to catch corruption that escapes unit tests.

### Corroboration #33 — Anti-join classification regex consistency between coordinator and agent

**Cross-project canon note:** Corroborations #23-30 are documented in
`/opt/wdws/usc_research/phase6_retrieval_guide_notes.md` (USC long-tail
completion canon). #31-32 referenced in WA Court Rules Phase 4.4
kickoff are awaiting cross-project canonization. #33 documented here
because the failure surfaced in WA Court Rules Phase 4.4 pre-flight.

**Failure mode:** when operator and agent independently project per-pass
resolution counts via different regex patterns, the projections diverge
by small but invariant-affecting amounts. Both regexes may be
"correct" per their author's mental model, but only one matches the
actual corpus structural reality. Locking halt-band invariants on the
wrong projection causes either false halts (under-projection) or
silent under-resolution (over-projection).

**Audit channel:** cross-validate per-pass classification regexes
BEFORE locking halt-band invariants. Run both regexes against the same
column, compute the symmetric difference (rows in A but not B, rows in
B but not A), and inspect the delta rows manually to determine which
regex correctly captures the structural reality.

**Concrete instance (WA Court Rules Phase 4.4 pre-flight,
2026-05-01):**
- Agent regex (Pass 2 chapter-only RCW classification):
  `^RCW \d+\.\d+$` → 79 rows
- Operator regex: `^RCW [0-9]+[A-Z]?\.[0-9]+[A-Z]?$` → 96 rows
- Delta = 17 rows across 10 distinct citations, all letter-suffix
  chapter forms (`9.94A`, `26.26A`, `26.26B`, `9A.28`, `9A.40`,
  `9A.42`, `11.96A`, `26.21A`, `36.70C`, `9.47A`)
- Sanity check: agent's set was a strict subset of operator's
- Pass 2 resolvability check: all 10 distinct delta citations have
  backing chapters in `rcw.sections` → all 17 rows resolvable
- Agent regex was missing the optional `[A-Z]?` letter-suffix
  tolerance — same bug also present in agent's draft script CTE
  (`^RCW \d+\.\d+\.` and `^(RCW \d+\.\d+)\..*$`), would have silently
  under-resolved by 17 rows if operator hadn't reconciled

**Generalized rule:** Extends #29 (per-title XML pre-flight inspection)
from per-title cardinality to per-pass classification logic. The
agent's regex is a measurement instrument; the coordinator's regex is
an independent measurement instrument; agreement validates the
measurement, divergence triggers reconciliation. When the two diverge,
do NOT lock invariants on either projection — surface the symmetric
difference and inspect the delta rows to determine the correct
classification before proceeding. Same audit-predicate-diversification
principle as #14 (subagent narrative vs process-state) — applied to
regex-based row classification.

**Mitigation pattern:** when drafting multi-pass FK resolution scripts,
include a brief note in each pass comment of the form
`-- Pass N candidates: <regex>; agent count: <N1>; operator count: <N2>; reconciled: <N>`
so future maintenance scripts inherit the reconciled regex rather than
re-deriving (and potentially re-introducing) the original divergence.

### Pre-#14 corroboration candidate (Phase 1 schema design — confirmed at Phase 6)

Per operator architectural recognition 2026-04-30 (Phase 1 close-out):

> "Phase 1 schema design SHOULD add defensive CHECKs beyond the
> operator-specified minimum where structurally-related invariants exist
> (e.g., `parent_canon ↔ rule_set_code='CJC'`, `has_comment ↔ comment_text`
> non-empty). These act as load-bearing audit channels during Phase 4
> ingest."

This is methodology guidance applicable to all schema design, not a specific failure-mode corroboration. Treated as **defensive-CHECK doctrine** — companion to the audit-predicate-diversification principle. Future ingest projects should default-add these defensive CHECKs even when not requested in operator spec, with brief rationale.

---

## §H2 — Documented limitations (logged, not blocking)

### §H2.1 — federal-usc cross-ref FK resolution at 11.1% (1/9)

USC long-tail not in priority core (~11,678 USC sections ingested vs ~150,000 full corpus). Court-rule citations to USC sections outside priority core can't FK-resolve. The 9 court-rule USC cross-refs target Title 25 (Indian Child Welfare Act, APR 8), Title 28 (federal jurisdiction), Title 42 (ADA, civil rights). Of these, only 1 resolves against the current `usc.sections` corpus.

**Mitigation:** semantic similarity routing per §S.6 surfaces USC sections topically related to the cited concepts even where FK doesn't resolve. Phase 5 Check 10 demonstrated this — federal ADA content surfaced for "ADA accommodation" query despite partial FK resolution.

**Phase 7 backlog:** USC long-tail completion (T5A → T18+T18a → T11+T11a → T26 → T38 → ~40 small titles) — assigned to USC agent on separate kickoff.

### §H2.2 — state-wa-wac cross-ref count = 0

Single corpus citation `WAC 448-12` (CrRLJ 6.13, breath-test toxicology chapter) is chapter-form (2-segment); WAC_CITE_RE requires 3-segment section-form. Pattern not yet in regex. 0 capture rate.

**Mitigation:** semantic similarity routing surfaces WAC content for cross-jurisdiction queries even where deterministic cross-refs are absent. Phase 5 Check 10 returned 25 WAC sections in top-30 for "ADA accommodation in Washington courts" — all via embedding similarity, not cross_references traversal.

**Phase 7 backlog:** add `WAC_CHAPTER_RE` pattern parallel to section-form (~5 LOC + 1 unit test).

### §H2.3 — Pattern (a) semantic MISS on RAP / CRLJ / APR

Phase 5 Check 3 per-rule-set canary showed 12/15 sets HIT on Pattern (a) anchor; 3 sets MISS:
- **RAP 18.13A** missed for "appeal procedure" — specialized juvenile dependency rule doesn't surface on generic appellate query
- **CRLJ 30** missed for "limited jurisdiction civil" — reserved-via-range placeholder body has no semantic substance
- **APR 15P** missed for "admission and practice" — specialty topic (Client Protection Fund) doesn't surface on generic admission query

This is a query-vocabulary limitation, not a corpus issue. All three explainable per "Canary criteria vs doctrine" feedback memory: Pattern (a) failures don't always indicate corpus failure; they may reflect query-vocabulary mismatch. Pattern (b) deterministic and Pattern (c) FTS both PASS for all three sets.

**Phase 7 lesson:** for new ingest projects, choose Pattern (a) anchors with sufficient semantic substance and query-vocabulary alignment to the body text. Avoid placeholder rows (reserved-via-range), specialty rules, or generic queries against narrow rule sets as anchors.

### §H2.4 — Phase-8-deferred WA court rule sets

17 sets recognized in cross-references but bodies not ingested in this run:
- **CCR** (34 rules) — Civil Commitment Rules
- **IRLJ** (26 rules) — Infraction Rules (traffic court)
- **AR** (6) — Superior Court Administrative Rules
- **ARLJ** (16) — Admin Rules for Limited Jurisdiction
- **BJAR** (5) — Board for Judicial Administration Rules
- **DRJ** (14) — Discipline Rules for Judges
- **JISCR** (18) — JIS Committee Rules
- **SPR** (9) — Special Proceedings
- **SPRC** (7) — Special Proceedings Criminal
- **LLLT RPC**, **ELLLTC**, **ELPOC**, **LPORPC**, **BBP**, **ATJ**, **BJA**, **SCAR** (defunct or mono-PDF, ~7 sets)

Their citations from in-scope rule bodies resolve to `target_jurisdiction='state-wa-court-rule'` + `target_rule_id=NULL`. Phase 8 ingest later resolves FKs retroactively without schema change.

**Phase 7 / Phase 8 backlog:** ingest these sets when scoped (estimate ~218 rules total, ~3-4 hr Phase 8 wall time using same Phase 0-5 architecture).

---

## Phase 7 backlog (consolidated)

Items deferred to Phase 7 differential cron + maintenance work.

### P7.1 — USC long-tail completion
USC priority core (~11,678 sections) covers high-value titles. Long-tail backlog: T5A → T18+T18a → T11+T11a → T26 → T38 → ~40 small titles. Assigned to USC agent on separate kickoff. Resolves §H2.1.

### P7.2 — WAC_CHAPTER_RE pattern for court-rule parser
~5 LOC + 1 unit test. Mirror `wac_parser.py:171-174` chapter pattern in court-rule parser; re-parse + cross_refs re-ingest. Affects 1 corpus citation (CrRLJ 6.13 → WAC 448-12). Resolves §H2.2.

### P7.3 — Phase-8-deferred court rule sets ingest
17 sets, ~218 rules. Use same Phase 0-5 architecture; CCR + IRLJ are most operationally interesting (civil commitment + infractions). Estimated ~3-4 hr wall time. Resolves §H2.4.

### P7.4 — Differential cron design
Periodic re-ingest as WA Supreme Court amends rules (typically annual major amendments + mid-year specific-rule amendments). Three feedback-memory-derived risks to address:

- **URL encoding edge case** ([feedback_url_encoding_phase7](/root/.claude/projects/-opt-wdws/memory/feedback_url_encoding_phase7.md)): strict-RFC URL libraries reject unencoded spaces in `courts.wa.gov` PDF filenames. Defensive `urllib.parse.quote(path, safe='/')` required.
- **Inventory parser cross-validation** ([feedback_inventory_parser_phase7](/root/.claude/projects/-opt-wdws/memory/feedback_inventory_parser_phase7.md)): multi-line title formatting can hide PDF links. 5% sample cross-validation of inventory output against direct page-fetch required.
- **Symptom-vs-cause halt-trigger discipline** (corroboration #21): halt triggers worded by symptom must include cause-classification framework. When a Phase 7 halt fires, distinguish (a) wrong pattern, (b) missing pattern, (c) corpus-genuine 0-content before remediation.

### P7.5 — Subagent vs standalone fetcher policy alignment (corroboration #14 lesson)
The dispatched subagent's per-URL soft-error tolerance vs standalone fetcher's halt-on-first-error policy diverged silently in Phase 2. Align in Phase 7 cron to prevent silent partial gaps. Either standardize on halt-on-first-error (preferred for correctness) or document soft-error policy explicitly (preferred for resilience).

### P7.6 — Three-segment alpha suffix regex extension (Phase 0 Caveat B)
Current regex `\d+[A-Z]?(?:\.\d+[A-Z]?)?(?:\.\d+)?` does not allow alpha suffix on the third segment (hypothetical `CrR 3.2.1A` would fail). No such citation in 2026-04-30 inventory. Phase 7 differential cron must re-validate regex against any new citation forms introduced by amendments.

### P7.7 — APR irregular-handle FK resolution
APR Regs/N, LP_REGS, LP_DIS_REGS rows have `target_rule_id` populated for in-corpus citations but no FK resolution against `wac_court_rules.rules` from external corpora. Cross-ref from RCW/WAC bodies to "APR Regs 101" form is parser scope. RCW+WAC parsers don't currently extract APR irregular handles. Phase 7 enhancement: extend RCW + WAC parsers with APR_HANDLE_RE.

---

## Project disposition

**WA Court Rules ingest: COMPLETE.** Hard stop reached after Phase 5 validation.

**Final state:**
- 1,145 rules across 15 in-scope sets (Tier 1: GR, RPC, CR, ER, RAP, CJC, GALR; Tier 2: CrR, CrRLJ, CRLJ, JuCR, SCCAR, RALJ, APR, ELC)
- 1,866 cross-references across 4 jurisdictions (state-wa-rcw, state-wa-court-rule, federal-usc, wa-constitution)
- 4,056 amendment-history entries
- 1,412 embedding chunks
- 11/11 Phase 5 validation checks PASS
- 9 new §K corroborations (#14-22) documented
- 4 §H2 limitations logged for Phase 7 backlog
- 7 Phase 7 backlog items (1 USC long-tail, 1 WAC pattern, 1 deferred sets ingest, 4 cron-design lessons)

**Cumulative project wall time: ~4 hr** across Phases 0-6 (research → schema → acquisition → parser → ingest → validation → docs).

**Operationally load-bearing for active matters:**
- GR 31, 31.1 — Snohomish records access work
- GR 33 — ADA accommodation theory (now four-jurisdiction-routed: court rule + RCW WLAD + federal ADA + WAC HRC)
- GR 34 — Jafar v. Webb fee waiver authority
- RPC 1.1, 1.5, 5.5 — Carlisle WSBA consultation (Athena AI compliance)
- CR 4, 12, 26, 56 — state-court civil practice
- ER 401, 403, 802 — evidence rules
- RAP 18.13A — appellate procedure
- CJC 2.11 — judicial disqualification
- GALR 1, 5 — active GAL strategy (Snohomish 26-2-00762-31 / 26-2-02783-31)
- ELC procedural framework — Carlisle's WSBA consultation context (133 rules)

The §T four-jurisdiction unified retrieval premise is operational: researchers
working an issue can query across federal USC + state RCW + state WAC + state
court rules in a single retrieval, with both deterministic FK routing (where
explicit citations exist) and semantic similarity routing (filling gaps).
