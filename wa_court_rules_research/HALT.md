# Phase 0 HALT — WA Court Rules ingestion research

**Date:** 2026-04-30
**Status:** HALTED at Phase 0 review; awaiting operator decision before Phase 1 schema staging.
**Halt criteria triggered:** #2 (rule sets beyond briefing list), #4 (citation format meaningfully diverges from regex sketch), #6 (structural anomaly suggesting new ghost-flag class). Volume halt (#1) NOT triggered.

The four Phase 0 deliverables are written and the cached HTML / sample PDFs are intact. This file enumerates the load-bearing findings the operator should review before authorizing Phase 1.

## Quick numbers

- **Total rules counted (programmatic, from list-page row enumeration):** 1,178
- **Rule sets discovered:** 32 (vs. 14 in briefing primary list)
- **Volume vs. expected (500-1,000):** 1,178 = 1.18× upper bound. Within 2× tolerance — **no volume halt**.
- **Cached HTML files:** 1 master index + 6 group pages + 32 list pages + 11 sample PDFs in `cached_html/pdf_samples/`.

## Halt trigger #2 — rule sets beyond briefing list

The briefing primary list named 14 rule sets: GR, CR, CrR, CrRLJ, CRLJ, ER, RAP, RPC, CJC, JuCR, MAR, RALJ, APR, DR. The "check for" list added 4 more (Special Proceedings = SPR; Tax Court — not present; Supreme Court Supp Rules ≈ SCAR/SCCAR; RFE = ELC).

**16 rule sets present on courts.wa.gov are not in either list:**

| code | count | name | suggested disposition |
|---|---:|---|---|
| `AR` | 6 | Superior Court Administrative Rules | defer (administrative) |
| `ARLJ` | 16 | Admin Rules for Courts of Limited Jurisdiction | defer (administrative) |
| `ATJ` | 0/2 | Access to Justice Tech Principles | defer (mono-PDF, governance) |
| `BBP` | 0 | Bench-Bar-Press Principles | skip (narrative, no rule structure) |
| `BJA` | 0/1 | Advisory Case Processing Time Standards | defer (mono-PDF) |
| `BJAR` | 5 | Board for Judicial Administration Rules | defer (governance) |
| `CCR` | 34 | Superior Court Civil Commitment Rules | **decide:** include if commitment-law in scope; otherwise defer |
| `DRJ` | 14 | Discipline Rules for Judges | defer (governance) |
| `ELLLTC` | 1 | Enforcement of LLLT Conduct | skip (LLLT program retired) |
| `ELPOC` | 1 | Enforcement of Limited Practice Officer Conduct | defer (mono-PDF) |
| `GALR` | 7 | Superior Court Guardian ad Litem Rules | **decide:** include if family/dependency in scope |
| `IRLJ` | 26 | Infraction Rules for Courts of Limited Jurisdiction | **decide:** include if traffic-court / infractions in scope |
| `JISCR` | 18 | Judicial Information System Committee Rules | defer (administrative) |
| `LLLT RPC` | 0 | LLLT RPC | skip (no body, retired program) |
| `LPORPC` | 1 | Limited Practice Officer RPC | defer (mono-PDF) |
| `SCCAR` | 27 | Superior Court Civil Arbitration Rules | **include — likely modern MAR replacement** |

**Modern-vs-briefing reconciliation:**
- **MAR (in briefing) does not exist on courts.wa.gov.** WA's Mandatory Arbitration Rules were superseded; the modern arbitration rule set is `SCCAR` (Superior Court Civil Arbitration Rules, 27 rules). Doctrinally — the WA legislature replaced MAR with the broader civil-arbitration scheme codified in RCW 7.06 with court rules now under SCCAR. Recommend substituting `SCCAR` for `MAR` in the priority sequence.
- **DR (in briefing) does not exist as a standalone rule set.** "DR" historically meant Dissolution Rules in some pro-se materials; modern WA dissolution practice is governed by `CR` chapters and statutory RCW 26.09 / 26.50. If the operator intended Civil Commitment, the closest match is `CCR`. Recommend dropping `DR` or substituting with `CCR` based on case scope.

**Decision needed:** for each "decide" row above and for the `MAR→SCCAR` / `DR→drop-or-CCR` substitutions, fold in or defer to Phase 8?

## Halt trigger #4 — citation regex divergence

Briefing-sketched regex:

```
\b(GR|CR|CrR|CrRLJ|CRLJ|ER|RAP|RPC|CJC|JuCR|MAR|RALJ|APR|DR)\s+(\d+(?:\.\d+[A-Z]?)?)\b
```

Tested against 1,178 actual rule labels. Divergences observed:

1. **MAR and DR don't exist** — would never match. Replace with `SCCAR` (and optionally `CCR`).
2. **18 additional rule-set codes** present and missing from the alternation: `AR`, `ARLJ`, `ATJ`, `BBP`, `BJA`, `BJAR`, `CCR`, `DRJ`, `ELC`, `ELLLTC`, `ELPOC`, `GALR`, `IRLJ`, `JISCR`, `LPORPC`, `SCAR`, `SCCAR`, `SPR`, `SPRC`, plus the pseudo-code `LLLT RPC` and `LLLT-RPC` (variant spelling).
3. **CJC uses `Canon N`** — current regex won't match `CJC Canon 1`. Need separate alternation `CJC\s+(?:Canon\s+\d+|\d+\.\d+)`.
4. **Three-segment numbering** — `CrR 3.2.1`, `CrRLJ 6.1.1`, `RALJ 6.3.1`. Current `(\d+(?:\.\d+[A-Z]?)?)` matches at most 2 segments.
5. **Alpha-prefix-then-dot** — `JuCR 5A.1` through `5A.6`. Form is `\d+[A-Z](?:\.\d+)?`.
6. **`Stds` / `Standards` / `Appx` companions** — e.g. `CrR 3.1 Stds`, `GR 26_Stds`, `GR 14_Appx`, `JuCR 9.2 Stds`. These are companion documents, not citations operators will use; safe to exclude from regex match scope.
7. **Range placeholders** — `CRLJ 27-37`, `RAP 18.18 - 18.20`, `JuCR 11.4-11.20`. Index-only; not citation tokens.
8. **APR irregular IDs** — `APR Regs`, `APR LP_REGS`, `APR LP_DIS_REGS`. Document handles, not numbered rules.

**Refined regex candidate** (suggested for Phase 1 review):

```python
SET_CODES = (
    'GR|CR|CrR|CrRLJ|CRLJ|ER|RAP|RPC|JuCR|RALJ|APR|SCCAR|'
    'AR|ARLJ|IRLJ|GALR|CCR|SPR|SPRC|BJAR|DRJ|ELC|JISCR'
)
WACR_CITE_RE = re.compile(
    rf'\b({SET_CODES})\s+'
    rf'(\d+[A-Z]?(?:\.\d+[A-Z]?)?(?:\.\d+)?)\b'
)
# CJC needs its own clause (Canon vs rule):
CJC_CITE_RE = re.compile(
    r'\bCJC\s+(?:Canon\s+(\d+)|(\d+\.\d+))\b'
)
```

This supports `RAP 18.13A`, `JuCR 5A.1`, `CrR 3.2.1`, `CJC Canon 1`, `CJC 2.11` (hypothetical inner-rule cite). Excluded by design: mono-PDF set codes (ATJ, BBP, BJA, SCAR, ELLLTC, ELPOC, LPORPC, LLLT RPC), document-handle APR variants, Stds/Appx companions, range placeholders.

## Halt trigger #6 — structural anomaly: third ghost-flag class

The briefing anticipated `is_repealed` + `is_reserved` ghost flags. WA court rules need a **third class**:

- **`is_range_placeholder`** — a single index entry covering a contiguous range of reserved rule numbers, e.g.:
  - `CRLJ 27-37 — Reserved` (covers 11 rule numbers in one row)
  - `CRLJ 65-67 — Reserved` (3)
  - `CRLJ 69-70 — Reserved` (2)
  - `CRLJ 78-80 — Reserved` (3)
  - `CRLJ 86.04--99.04 — Reserved` (multi-decimal, ambiguous count)
  - `RAP 18.18 - 18.20 — Reserved` (3)
  - `JuCR 11.4-11.20 — Reserved` (~17 if dot-numbered)
  - `CR 72-76 — Reserved` (5)

These are structurally different from individually-reserved rules (`RPC 7.2 — [RESERVED.]`) because the index does not enumerate the contained rule numbers. If a downstream user cites e.g. `CRLJ 30`, the system needs to know that rule was reserved-via-range, not "missing".

**Decision needed:** during Phase 4 ingest, expand range placeholders into individual rule slots (each with `is_reserved=true` and a reference back to the canonical range PDF), or store the range as one document with a parsed `range_start` / `range_end` metadata pair?

## Other findings (not halt-class but operator should know)

### CJC nested-rule structure

CJC's index lists 4 Canons; each Canon PDF contains nested `RULE N.M` sub-rules (e.g., Canon 1 → RULE 1.1, 1.2, 1.3). The Phase 2 PDF parser must recognize this and either (a) split each Canon PDF into per-rule chunks, or (b) ingest Canon PDFs whole and rely on retrieval to surface the relevant sub-rule by text matching. Recommendation: option (a) — per-rule chunks — to align with how operators cite (e.g., "CJC 2.11 disqualification").

### Bodies are PDF-only

There is **no canonical HTML view** of WA court rule bodies. The `?fa=court_rules.display&...` URL exists but it just renders a "click to open the PDF" page. All body extraction in Phase 2 will require `pdftotext` (or equivalent). Sample PDFs extract cleanly with `pdftotext -layout` — these are text-PDFs (not scanned), so OCR is not needed.

### Mono-PDF rule sets

7 rule sets are not enumerated rule-by-rule — they are single consolidated PDFs (or in BBP's case, narrative HTML only). These don't fit the "set → list of rules" model. Phase 4 ingest needs to either skip them or chunk each PDF internally.

### `LLLT RPC` is a 0-link landing page

The Limited License Legal Technician Rules of Professional Conduct page has no PDF link and no rule body — almost certainly because the WA Supreme Court sunset the LLLT program in 2020. Recommend skipping `LLLT RPC` and `ELLLTC` entirely.

### Comment-section pattern

Confirmed RPC convention `[1]` `[2]` ... `[N]` for numbered comments. Halt criterion #5 NOT triggered. Additional patterns observed:
- Per-comment history: `[Comment 6 Adopted September 1, 2016.]` between numbered comments
- WA-specific section header: `Additional Washington Comments (9-10)` separating ABA-aligned from WA-specific commentary
- Inline tag: `[Washington revision]` on revised comments
- Some rule sets use rule-numbered headings: `Comment 401` instead of generic `Comment` (ER pattern)

### History-line pattern

Universal: `[Adopted effective <DATE>; Amended effective <DATE>; <DATE>; ...; <DATE>.]`
Repealed adds `Repealed effective <DATE>`. Date format is U.S. long form `Month D, YYYY`. Robust regex provided in `wa_court_rules_source_findings.md`.

### Subsection notation

Hierarchical: `(a)` → `(1)` → `(A)` → `(i)`. Confirmed across CR, GR, RPC, ER samples. CJC adds an outer level — Canon → `RULE N.M` → `(A)/(B)`.

## What's ready for Phase 1

- `wa_court_rules_source_findings.md` — URL pattern, HTML/PDF structure, comment/history/subsection patterns, ghost-flag class doc
- `rule_set_inventory.md` — programmatic 1,178-rule, 32-set table, with caveats
- `litigation_priority_addendum.md` — 15 anchor citations verified for Phase 4 spot-check; priority sequence corrected (MAR→SCCAR; DR→drop-or-CCR)
- `cached_html/inventory.json` — machine-readable per-rule inventory (rule_set, num, title, pdf_url) for downstream parsing
- 11 sample PDFs covering all priority anchors + 1 reserved + 1 rescinded + 1 CJC Canon

## What's blocked

- `phase0_cumulative_report.md` is intentionally not written — the disposition section depends on operator decisions on the items above (which rule sets to fold in, which to defer; how to handle mono-PDF sets; how to expand range placeholders). When the operator confirms scope, the cumulative report can be generated quickly from existing materials and the regex finalized.

## Recommended operator decisions

1. **Scope confirmation:** confirm the 14 briefing rule sets minus MAR/DR plus SCCAR for Phase 4 (Tier 1 + Tier 2). Punt the 16 "beyond list" sets to Phase 8 except CCR/IRLJ/GALR which are case-scope-dependent.
2. **MAR substitution:** replace MAR with SCCAR throughout pipeline.
3. **DR handling:** drop DR entirely from sequence.
4. **CJC unpacking:** Phase 2 parser unpacks Canon PDFs into per-`RULE N.M` chunks.
5. **Range-placeholder handling:** expand into individual reserved-slot rule rows in Phase 4 (preferred), OR store as single document with range metadata (acceptable but reduces citation-resolution coverage).
6. **Citation regex:** adopt the refined regex above (or a variant) for Phase 1 schema staging.
7. **Mono-PDF / LLLT RPC handling:** skip `LLLT RPC`, `ELLLTC`, `LPORPC`, `BBP`, `ATJ`, `BJA`, `SCAR` for Phase 4 — defer if needed in Phase 8.

Once these are settled, Phase 1 schema staging can proceed.
