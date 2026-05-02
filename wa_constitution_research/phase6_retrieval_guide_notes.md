# WA Constitution — Phase 6 Retrieval Guide Notes (Phase 1 stub)

**Status:** Phase 1 investigation complete; Phase 6 retrieval-guide content TBD post-ingest.
**Date:** 2026-05-01.
**Project:** WA Constitution Ingest Agent.
**Canon inheritance:** This project inherits §K methodology canon at corroborations
#1-#39 from four prior project notes (refinement_sweep canon path cited in
kickoff was missing — §K #39 inherited via WA Court Rules canon):

- /opt/wdws/usc_research/phase6_retrieval_guide_notes.md (#1, #3, #14-30 USC tier)
- /opt/wdws/rcw_research/phase6_retrieval_guide_notes.md (#7, #32, #34-37 RCW tier)
- /opt/wdws/wac_research/phase6_retrieval_guide_notes.md (#1-13, #31 WAC tier)
- /opt/wdws/wa_court_rules_research/phase6_retrieval_guide_notes.md (#14-22, #29-#39 reinforcement, #33 WCR)

(refinement_sweep_research canon at /opt/wdws/refinement_sweep_research/ does not
exist as of 2026-05-01; kickoff inheritance list contained a stale path. The
§K #39 corroboration the kickoff attributes to that canon is documented in the
WCR canon at lines 52-95. Methodology inheritance intact via 4 canons.)

---

## §K corroborations encountered during Phase 1 (canon at #40 if surfaced)

### Reinforcement of #25 (coordinator structural-knowledge currency) — TWO instances this Phase 1

**Instance 1 — kickoff source URL stale.** Kickoff cited
`https://leg.wa.gov/lawsandagencyrules/Pages/constitution.aspx` as primary
source. Returns HTTP 404. WSL has restructured to a new path prefix:
`/state-laws-and-rules/washington-state-constitution/`. Article-level access uses
opaque GUID query parameters (`?section={uuid}`) rather than human-readable
paths. Per §K #25 audit channel ("treat the location claim as a hypothesis to
verify, not a fact to act on"), the agent verified the kickoff URL before
launching Phase 2 acquisition. If Phase 2 had been launched without
verification, the entire 32-article fetch loop would have hit 404 in
production.

**Instance 2 — Article XX kickoff anomaly note wrong.** Kickoff Phase 1.3
prompt said: "Note any anomalies (Article XX repealed; ...)." Direct fetch of
Article XX at the verified URL shows: Article XX is "PUBLIC HEALTH AND VITAL
STATISTICS" with 2 active sections (§§ 1, 2). Neither section is repealed.
The kickoff's coordinator-side mental model conflated this with some other
historical state — possibly Article XXVII (Schedule, which has § 13
repealed) or a prior amendment-superseding event. Surfaced for operator
disposition: parser should NOT pre-mark Article XX as repealed.

**Generalization (already canonized as #25):** the failure mode is identical
to the USC Title 5A long-tail kickoff investigation — coordinator-described
structural premise treated as hypothesis to verify before consuming as
projection input. Two same-class instances within one Phase 1 reinforces #25.

### Reinforcement of #29 (per-source pre-flight vs coordinator estimates) — section-count under-estimate

**Concrete instance:** Kickoff projected ~200 total sections with halt band
[180, 220]. Phase 1 per-article inspection across all 32 articles measured
**273 parent-numbered sections + 3-5 letter-suffix standalone sections =
276-278 row-level total** — 36% above kickoff projection, well outside the
halt band on the high side.

If Phase 2 had launched against the [180, 220] band, the halt would fire
correctly per the spec at the upper boundary, but the underlying concern
(capacity, parse correctness) would have been mis-routed to "investigate why
we're emitting too many sections" instead of "the coordinator estimate was
stale, recalibrate to actual structure."

Per the §K #29 mitigation pattern documented in the WCR canon, "the agent's
first pre-flight task should be to independently verify [the coordinator
aggregate] against per-source measurement and surface any divergence > 10%
before consuming it as the basis for halt bands." Divergence here is 36%;
halt band recalibrated to [260, 290] for row-level count.

### §K corroboration #40 — Heterogeneous structural granularity within a single corpus (canonized 2026-05-01)

**Operator-approved canon entry (Phase 1 confirmation, 2026-05-01):**
"Heterogeneous structural granularity within a single corpus. When a corpus
has multiple structural patterns for ostensibly-similar citation forms
(letter-suffix-as-standalone vs letter-suffix-as-subsection), schema design
must accommodate both via per-pattern parser dispatch rather than uniform
regex. Generalizes #28 (halt-band cardinality axis) to schema-design-time
pattern variance rather than execution-time count variance. Audit channel:
agent surfaces structural heterogeneity during pre-flight stress-test rather
than discovering at INSERT-failure."

**Concrete instance (WA Const Phase 1, 2026-05-01):** Per-article structural
inspection across 32 articles surfaced two distinct letter-suffix patterns:

- **Pattern α (standalone)** — Articles III, IV, IX. Entries like Art IV §
  2(a) "TEMPORARY PERFORMANCE OF JUDICIAL DUTIES" carry distinct headings
  separate from the parent (§ 2 "SUPREME COURT"). These are doctrinally
  separate provisions added by amendment, sharing only a numeric prefix
  with the parent. Best modeled as separate rows.
- **Pattern β (subsection)** — Articles II, VII, VIII. Entries like Art VIII
  § 1(a-l) are enumerated parts within the parent section's body, lacking
  distinct headings. Best modeled as part of parent's `full_text`.

**Audit channel:** the disambiguator is **whether the entry has its own
section heading distinct from the parent**. The audit channel is per-source
structural inspection (pre-flight stress-test) — NOT regex over citation
strings, NOT inferred from sub-numbering depth. Inspecting only headings
makes the audit predicate structurally different from any parser predicate
that operates on citation form alone.

**Mitigation pattern:** parser must dispatch per-pattern at parse time:
- Stage 1: detect entries matching `[0-9]+[A-Za-z]?(\([a-z]\))?` form
- Stage 2: classify by heading-presence — standalone if distinct heading
  AND distinct opening prose; subsection-fold if no distinct heading or if
  opening prose continues parent's argument
- Stage 3: emit row-level for standalone (§ 2(a) row); append-to-parent for
  subsection (§ 1(a) text appended to § 1 body)

Schema CHECK regex must accommodate both standalone forms:
`^Wash\. Const\. art\. [IVXLC]+(, § [0-9]+[A-Za-z]?(\([a-z]\))?)?$`
This accepts both `§ 1A` (capital) and `§ 2(a)` (parens) forms.

**Generalizes:** #28 (halt-band cardinality axis — gate on the right
metric) extended from execution-time count variance (chunk count vs section
count) to schema-design-time pattern variance (subsection-fold count vs
standalone-row count). Same audit-predicate-diversification principle:
inspect at the right structural granularity per pattern, don't apply
uniform regex to heterogeneous patterns.

**Phase 1 disposition:** Option A authorized (standalone rows for
distinct-heading entries; subsection-fold for enumerated-parts entries).
Total expected row count: ~276-278 (273 parent-numbered + 3-5 standalone
letter-suffix). Halt-band recalibrated to [270, 285] for total row-level
count.

**Canon-extension note:** §K #40 is now load-bearing operational discipline
for any future corpus with mixed structural-granularity citation forms
(state constitutions, federal acts with mixed standalone-section vs
subsection conventions, court-rule comment-vs-rule mixtures, etc.).
Pre-flight stress-test must classify per-pattern before locking schema.

---

## §K reinforcement entries this Phase 1 (already-canonized corroborations)

### Reinforcement of #25 (operator-provided URL/structural-assumption drift) — TWO instances

1. **Kickoff URL stale.** `https://leg.wa.gov/lawsandagencyrules/Pages/constitution.aspx`
   returns 404. Verified working URL:
   `https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/?section={GUID}`.
   Per the §K #25 audit channel, agent verified before launching Phase 2 acquisition.
2. **Article XX kickoff anomaly note wrong.** Kickoff said "Article XX repealed";
   verified Art XX has 2 active sections. Actual repealed sections: Art VI § 2,
   Art XII § 14, Art XXVII § 13.

### Reinforcement of #29 (5th instance this session) — coordinator estimate inflation

Kickoff projected ~200 sections; verified 273 parent-numbered (36% inflation).
Halt band recalibrated [180, 220] → [270, 285]. Pattern of coordinator-estimate
inflation when projecting from session-memory aggregates is now load-bearing
operational discipline — always verify per-source before locking thresholds.

---

## §A — WA Constitution retrieval patterns (TBD, Phase 6)

[Drafted at end of Phase 4.4 once schema + ingest + cross-corpus FK uplift
are operational. Will mirror §S (WA Court Rules) and §P (WAC) sections of
prior project canons.]

---

## §B — Five-jurisdiction queries (TBD, Phase 6)

[Drafted at end of Phase 4.4 — extend §T (four-jurisdiction) to five-
jurisdiction including wa-constitution.]

---

## §C — Phase 7 backlog (TBD, Phase 6)

[Captured during Phases 2-4.4 as encountered. Likely items: amendment
ratification-date HTML parse refinement; SOS cross-check (currently 402
inaccessible from this environment); subsection-as-section re-classification
if operator chooses post-ingest restructure.]
