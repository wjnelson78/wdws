# WA Court Rules — Phase 0 Cumulative Report

**Date:** 2026-04-30
**Phase 0 status:** CLOSED. Ready for Phase 1 schema staging authorization.
**Halt protocols triggered + reconciled:** #2 (rule sets beyond briefing), #4 (citation regex divergence), #6 (third ghost-flag class).
**Halt protocols NOT triggered:** #1 (volume — 1,178 = 1.18× upper bound, within 2× tolerance), #3 (rate-limit / 5xx), #5 (comment pattern divergence — RPC `[1]`-`[N]` confirmed).

This report supersedes Phase 0 deliverables individually; the four artifact files (`HALT.md`, `wa_court_rules_source_findings.md`, `rule_set_inventory.md`, `litigation_priority_addendum.md`) were updated in place to reflect the operator's 2026-04-30 reconciliation. Cached HTML and per-rule-set sample PDFs in `cached_html/` are unchanged from initial agent run.

## 1. Source format confirmation

`https://www.courts.wa.gov/court_rules/` is a 3-tier ColdFusion site:

```
master index (?fa=court_rules.list)
  └── group page (?fa=court_rules.list&group=<grp>)        — 6 group pages
       └── list page (?fa=court_rules.list&group=<grp>&set=<set>) — 32 list pages
            └── per-rule PDF (../court_rules/pdf/<set>/<filename>.pdf)
```

**Group keys:** `sup` (Superior Court), `ga` (General Application), `clj` (Limited Jurisdiction), `app` (Appeal), `aca` (Appellate-court admin), `am` (Administrative materials).

**Body format:** PDF only. No canonical HTML view. The `?fa=court_rules.display&...` URL renders a "click to open the PDF" page, not the rule body. All Phase 2 body extraction will use `pdftotext -layout`. Sample PDFs verified as text-PDFs (not scanned), extract cleanly without OCR.

**HTML structure for list pages:** `<table>` rows with `<td nowrap>RULE_NUM</td><td><a href="...pdf">RULE_TITLE</a></td>`. Programmatically parseable.

**Caching status:** all 32 list pages + 1 master index + 6 group pages + 11 priority sample PDFs cached at `cached_html/`. `cached_html/inventory.json` is the machine-readable per-rule manifest (1,178 entries) used for all programmatic checks in this report.

## 2. Volume sanity check

| Metric | Value |
|---|---:|
| Total rules counted | **1,178** |
| Rule sets | 32 |
| Expected band (briefing) | 500-1,000 |
| Actual vs upper | **1.18×** |
| Halt threshold | 2× |
| **Halt #1 status** | **NOT TRIGGERED** |

Of 1,178 rules:
- ~960 are in scope for this run (15 rule sets — see §4)
- ~218 are deferred to Phase 8 (17 rule sets)

## 3. Citation regex (final, with LLLT lookbehind)

Tested against 18 edge cases + 30 actual rule citations sampled across all 15 in-scope sets. **All 18 edge cases match correctly. All 30 actual citations match. Zero misses on in-scope citations.**

```python
import re

SET_CODES = (
    'GR|CR|CrR|CrRLJ|CRLJ|ER|RAP|RPC|JuCR|RALJ|APR|SCCAR|'
    'GALR|ELC|'                                  # in-scope, folded in 2026-04-30
    'AR|ARLJ|IRLJ|CCR|SPR|SPRC|BJAR|DRJ|JISCR'   # Phase 8 deferred (kept for cross-ref)
)

# Lookbehind prevents 'LLLT RPC 1.1' from matching as RPC citation.
# LLLT program defunct (Phase 8 deferral); historical references in
# in-scope rule bodies must not be parsed as live RPC citations.
WACR_CITE_RE = re.compile(
    rf'(?<!LLLT\s)\b({SET_CODES})\s+'
    rf'(\d+[A-Z]?(?:\.\d+[A-Z]?)?(?:\.\d+)?)\b'
)

# CJC supports both 'CJC Canon N' (index/heading form) and 'CJC N.M' (inner-rule form)
CJC_CITE_RE = re.compile(
    r'\bCJC\s+(?:Canon\s+(\d+)|(\d+\.\d+))\b'
)

# Subsection notation is captured separately from the base citation, then
# attached to the parent rule via cross_references rather than splitting
# subsections into separate rule rows.
SUBSEC_RE = re.compile(r'\(([a-z]|\d+|[A-Z]|[ivxlcdm]+)\)')

# Comment references attach to a parent rule with cmt_index set
CMT_RE = re.compile(r'\b(?:Comment|cmt\.?)\s*\[(\d+)\]')
```

### Test results (Phase 1 must include T17 unit test covering all 18 cases)

```
✓ GR 31                  base — bare integer            → set=GR num=31
✓ RPC 1.1                base — decimal                 → set=RPC num=1.1
✓ CR 12                  base — bare integer            → set=CR num=12
✓ CR 12(b)(6)            subsection 1-level             → set=CR num=12 subs=['b','6']
✓ CR 12(b)(2)(A)         subsection 2-level             → set=CR num=12 subs=['b','2','A']
✓ RPC 1.5                decimal sub                    → set=RPC num=1.5
✓ CJC 2.11               CJC inner-rule decimal         → CJC pattern, captures 2.11
✓ RAP 18.13A             alpha-suffix-on-decimal        → set=RAP num=18.13A
✓ JuCR 5A.1              alpha-prefix-then-dot          → set=JuCR num=5A.1
✓ CrR 3.2.1              three-segment numbering        → set=CrR num=3.2.1
✓ CJC Canon 1            CJC Canon form                 → CJC pattern, captures 1
✓ RPC 1.1 Comment [3]    comment full word              → set=RPC num=1.1 cmt=[3]
✓ RPC 1.1 cmt. [3]       comment abbrev                 → set=RPC num=1.1 cmt=[3]
✓ ELC 1.1                ELC base                       → set=ELC num=1.1
✓ ELC 15.6               ELC reserved row               → set=ELC num=15.6
✓ GALR 7                 GALR base                      → set=GALR num=7
✓ "see LLLT RPC 1.1"     LLLT prefix lookbehind         → no match (correct)
✓ "see RPC 1.1"          plain RPC ref                  → set=RPC num=1.1
✓ ATJ 1                  mono-PDF deferred              → no match (correct, out of scope)

30/30 actual citations from inventory.json passed (samples covering all 15 in-scope rule sets).
```

### Documented future-gotchas

- **Caveat B — three-segment alpha suffix.** Pattern matches `\d+[A-Z]?(?:\.\d+[A-Z]?)?(?:\.\d+)?`. The third segment doesn't allow alpha suffix (e.g., a hypothetical `CrR 3.2.1A` would fail). No such citation in actual 2026-04-30 inventory. Document for Phase 7 differential cron — re-validate against any new citations introduced by amendments.

## 4. Reconciled in-scope rule sets (operator-approved)

**Tier 1 (priority block, Pause-Checkpoint A):** GR, RPC, CR, ER, RAP, CJC, GALR
**Tier 2 (long tail, Pause-Checkpoint B):** CrR, CrRLJ, CRLJ, JuCR, SCCAR, RALJ, APR, ELC

Briefing corrections:
- **MAR → SCCAR** (MAR doesn't exist on courts.wa.gov; SCCAR is modern equivalent)
- **DR dropped entirely** (does not exist standalone; lawyer-discipline procedural framework is `ELC`)
- **GALR added** (load-bearing for active Snohomish GAL strategy work)
- **ELC added** (load-bearing for Carlisle WSBA RPC consultation about Athena AI compliance)

**Phase 8 deferred (17 sets, ~218 rules):** CCR, IRLJ, AR, ARLJ, BJAR, DRJ, JISCR, SPR, SPRC, LLLT RPC, ELLLTC, ELPOC, LPORPC, BBP, ATJ, BJA, SCAR.

Phase-8-deferred sets are kept in `SET_CODES` regex alternation for cross-reference recognition. Cross-refs to deferred sets resolve to `target_jurisdiction='state-wa-court-rule'`, `target_rule_id=NULL`. Phase 8 ingest later resolves FKs retroactively. **Phase 6 §S retrieval guide** must document the unresolved-because-target-absent vs unresolved-because-FK-not-yet-ingested distinction in authority-resolution rate accounting.

## 5. Comment-section pattern (RPC + CJC + ER)

**RPC `[N]` numbered convention confirmed.** Halt criterion #5 NOT triggered.

Additional patterns:
- Per-comment history: `[Comment 6 Adopted September 1, 2016.]` between numbered comments
- WA-specific section header: `Additional Washington Comments (9-10)` separating ABA-aligned from WA-specific commentary
- Inline tag: `[Washington revision]` on revised comments
- ER pattern uses rule-numbered headings: `Comment 401` (rule-prefixed) instead of generic `Comment`

Schema: `wa_court_rules.rules.comment_text` column captures full comment block; cross-references with `ref_type='comment'` and `cmt_index` field for citation form `RPC 1.1 cmt. [3]`.

## 6. Subsection notation patterns

Hierarchical: `(a)` → `(1)` → `(A)` → `(i)`. Confirmed across CR, GR, RPC, ER samples. CJC adds an outer level: Canon → `RULE N.M` → `(A)/(B)`.

Subsections are captured but **not split as separate rule rows**. They attach to the parent rule via cross_references (with `ref_type='subsection-cite'`) when the cite carries them.

## 7. Rule-history / amendment-date pattern

Universal pattern: `[Adopted effective <DATE>; Amended effective <DATE>; <DATE>; ...; <DATE>.]`

- Repealed adds `Repealed effective <DATE>`
- Date format: U.S. long form `Month D, YYYY`
- `[RESCINDED]` is the repeal marker (not `[REPEALED]`)
- `[RESERVED]` and `[Reserved]` (case-variant) are reservation markers

History-line regex (in `wa_court_rules_source_findings.md`):
```python
HIST_RE = re.compile(
    r'\[(Adopted\s+effective[^]]+|Amended\s+effective[^]]+|Repealed\s+effective[^]]+|RESCINDED|RESERVED|Reserved)\]'
)
DATE_RE = re.compile(
    r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}'
)
```

## 8. Schema refinements locked for Phase 1

Per operator approval 2026-04-30:

### Path 1 — loosen CHECK constraint to permit placeholder text on `is_reserved` rows

```sql
CONSTRAINT chk_rule_text_status_invariant CHECK (
  (full_text IS NOT NULL AND full_text <> ''
   AND NOT (is_repealed OR is_reserved))
  OR
  (is_repealed AND (full_text IS NULL OR full_text = ''))
  OR
  (is_reserved)  -- reserved rows may have empty OR placeholder text
                 -- (parallel to WAC's Class A/B1/D placeholder treatment)
)
```

### Range-placeholder columns on `wa_court_rules.rules`

```sql
range_doc_id     UUID REFERENCES wa_court_rules.rules(id),  -- canonical range row pointer
range_start      TEXT,                                      -- on canonical range row only
range_end        TEXT,                                      -- on canonical range row only
parent_canon     TEXT,                                      -- e.g., 'Canon 2', for CJC retrieval grouping
```

Range expansion strategy (Option A, operator-approved):
- 8 observed reserved ranges expand into ~50-70 individual reserved rule rows
- Each expanded row: `is_reserved=TRUE`, `range_doc_id=<canonical_range_uuid>`, placeholder `full_text` like "This rule is reserved as part of CRLJ 27-37. See range citation."
- Citation `CRLJ 30` resolves to the reserved row directly; retrieval can JOIN to canonical via `range_doc_id` for full range context

### CJC unpacking strategy (Option A, operator-approved)

- Phase 2 parser splits each Canon PDF into per-`RULE N.M` chunks
- Each inner rule becomes a row with `canonical_citation='CJC 2.11'`, `parent_canon='Canon 2'`
- ~30 inner rules total once 4 Canons unpacked

## 9. New ghost-flag class (Halt #6 reconciled)

Beyond `is_repealed` + `is_reserved`, a third class is needed: **range-placeholder** (handled via Path 1 CHECK loosening + `range_doc_id` column described above; no new boolean column required since it's a special case of `is_reserved=TRUE`).

## 10. Doctrinal additions for Phase 6 retrieval guide

To document in `phase6_retrieval_guide_notes.md` during Phase 6:

**§S — Phase-8-deferred cross-reference recognition.** Phase-8-deferred rule sets are recognized in citation parsing but not resolved to FKs until their corpus is ingested. Authority-resolution rate accounting must distinguish:
- "unresolved because target rule does not exist" (e.g., bad citation in source text)
- "unresolved because target FK target not yet ingested" (Phase 8 candidates)

**§K — Citation regex anchored to actual corpus.** Regex matches what Phase 0 sampled. When Phase 7 differential cron runs, re-validate regex against any new citation forms introduced by amendments. Caveat B (three-segment alpha suffix `CrR 3.2.1A`) is the documented future-gotcha.

## 11. Anchor citations for Phase 4 acceptance gates (20 anchors)

See `litigation_priority_addendum.md` §"Spot-check anchor citations". 20 anchors covering all 15 in-scope rule sets. If all 20 retrieve correctly post-Phase-4, corpus is ingest-good for operator's primary use cases.

## 12. Phase 1 readiness disposition

✅ All halt-class findings reconciled.
✅ Citation regex finalized + tested + LLLT lookbehind locked in.
✅ Schema refinements specified with SQL syntax.
✅ Reconciled scope (15 in-scope sets, 17 Phase-8 deferred) operator-approved.
✅ Phase-8 cross-ref discipline doctrine articulated.
✅ Caveat B (3-segment alpha) documented as future-gotcha.

**Ready for Phase 1 authorization to proceed with `wa_court_rules` schema staging + `chk_target_jurisdiction` harmonization on `rcw.cross_references` + `wac.cross_references` (`'wa-court-rule'` → `'state-wa-court-rule'`, 0 rows affected per pre-Phase-0 verification).**

Phase 1 deliverables expected: `phase1_schema_design.sql` (operator review BEFORE DDL apply per kickoff §6), pre-flight RAISE EXCEPTION guards, T17 regex unit test, harmonization migration script.

---

**Phase 0 close-out cross-reference:**
- HALT.md — original halt findings (preserved as audit trail)
- rule_set_inventory.md — reconciled scope + Phase-8 deferral list
- litigation_priority_addendum.md — Tier 1/2 sequence + 20 anchors
- wa_court_rules_source_findings.md — URL/HTML/PDF/comment/history patterns
- cached_html/inventory.json — 1,178-rule machine-readable manifest
- cached_html/pdf_samples/ — 11 priority anchor PDFs

**Status:** Awaiting operator authorization for Phase 1.
