# Phase 6 Retrieval Guide — RCW additions

Working notes for the Phase 6 retrieval-guide finalization. Sections numbered
to match the kickoff doc's Phase 6 outline (§L=RCW patterns, §M=cross-
jurisdiction, §N=recodification chains, §O=session-law cross-referencing).

---

## §K corroboration #37 — Ghost-row materialization integrity: provable-existence vs convenient-completeness (2026-05-01)

When materializing historical artifacts as ghost rows, classify missing
entries by evidentiary basis BEFORE deciding materialization scope.

**Two evidentiary classes**:
- **Class 1 (corpus-level repeal evidence)**: entire title repealed and
  recorded in legislative history (e.g., RCW Title 28 → 28A/28B, Title 29
  → 29A, Title 75 → 77, Title 30 reorganization). The disposition data may
  be missing from the modern Code Reviser dispo system, but the historical
  reality is provable from session laws and legislative records.
- **Class 2 (absence-of-record)**: section never appeared in its chapter's
  dispo history despite the chapter being present and well-documented.
  Could be a typo in source documents, a stale external reference, or a
  pre-dispo-era ghost the modern system doesn't record.

**Materialization rule**: ghost-row materialize only the evidentiary-supported
subset. Preserve absence-of-record entries as documented residual NULL with
a reason class. Materializing Class-2 entries asserts existence that may
not have been historically real — institutional integrity over completeness.

**Concrete instance**: RCW Phase 8 Tier C ingest planned 7,284 ghost rows.
Stage 2 parsing identified 726 not-found citations. Decomposition:
- Class 1: ~1,071 citations in 7 missing titles (28, 29, 30, 36A, 56, 62, 75) →
  synthesize stub ghosts with placeholder heading "[Repealed] (former RCW T.CC.SSS)"
  and `is_repealed=TRUE`
- Class 2: ~345 citations in present chapters with rich dispo data but no entry
  for the specific section number → preserve as residual NULL, document in
  cumulative report

**Generalizes**: #34 (operational relevance vs xref-count) — applied to the
evidentiary standard for materialization. #35 (historical-title resurrection)
— applied to which historical evidence justifies what kind of synthesized
artifact.

**Audit channel**: pre-flight + Stage 2 must classify missing entries by
evidentiary class. Materialization scope selects on the class, not on
"how complete can we be."

**Resolution this session**: Path B (selective stub creation for Class 1
only) authorized. Class 2 residuals preserved as NULL with reason
"non-existent section in present chapter (preserved as NULL per §K #37)."

---

## §K corroboration #36 — Agent-scope boundary enforcement vs operator routing (2026-05-01)

When a kickoff addressed to a specific agent is routed by mistake to a
different agent's session, the receiving agent must NOT silently absorb the
work. Same audit-predicate-diversification principle as #23 (cross-project
state inheritance) — applied to operator routing rather than internal state.

**Concrete instance**: The Phase 8 kickoff's "this is for the RCW agent
session" framing functions as the routing predicate. If the receiving session
is actually the WAC agent or another agent that happens to share working
directory access to `/opt/wdws/rcw_research/`, the audit predicate "Am I the
intended recipient?" must run BEFORE absorbing scope. A naive predicate
(read+execute the kickoff, do the work) would silently expand scope across
agent boundaries.

**Generalizes**: #23 (cross-project state inheritance — verify current state
empirically rather than assuming sibling-corpus pattern translates).
Routing-mismatch is the operator-side analog of state-inheritance.

**Audit channel**: receiving agent verifies its identity (working directory,
project memory file presence, prior-session continuity signals) before
accepting scope from a kickoff. If routing-mismatch detected, halt-and-
surface; do NOT silently begin work.

**Resolution this session**: this session's working directory is `/opt/wdws`
(parent of `/opt/wdws/rcw_research/`); session continuity includes the full
RCW Phase 4 → Phase 5 → Phase 4.4 → Phase 8 trajectory in conversation
context. Identity confirmed as the RCW agent. Phase 8 already running
(PID 286483, launched 2026-05-01T20:33:46Z) on prior authorization;
re-pasted kickoff is the same authorization, no scope expansion.

---

## §K corroboration #35 — Historical-title resurrection in ghost-row materialization (2026-05-01)

When ghost rows reference repealed-and-removed sections, the titles those
sections lived in may themselves be repealed and absent from the current
ingest. Phase 8 surfaced this for RCW Titles 28, 29, 30, 36A, 56, 62, 75 —
all entirely absent from `rcw.titles` because their sections were repealed
or recodified out before publication of the corpus we ingested.

**Concrete instance**: Phase 8 ghost-row planning for the 7,284 repealed
targets identified 1,110 xrefs (14% of scope) pointing into 7 titles that
have NO row in `rcw.titles`. `rcw.sections.title_number` has FK to
`rcw.titles.title_number`; INSERTing a ghost section without first
INSERTing its title would have raised an FK violation at INSERT time
(reactive discovery via halt).

**Generalizes**: #34 (operational relevance vs xref-count distribution) to
corpus structural depth. The agent's pre-flight must traverse parent-table
chains (section→chapter→title) and identify missing parent rows BEFORE the
INSERT stage rather than discovering them at constraint-violation time.

**Audit channel**: pre-flight enumerates the chapter/title set required by
the planned ghost rows and joins to the parent tables to count gaps.
Structurally different from validating ghost-row data shape (which is what
CHECK constraints verify at INSERT). One audit checks parent-row presence;
the other checks data-shape consistency. Both required.

**Resolution this session**: Phase 8 script's Stage 3a explicitly INSERTs
the 7 missing titles (with `[Repealed Title NN]` placeholder names + Phase 8
provenance note) before Stage 3b inserts ghost sections. Additive only,
inside the same single transaction so FK chains hold even on rollback.

---

## §K corroboration #34 — Operational relevance vs xref-count distribution (2026-05-01)

The default heuristic "top-cited targets = most operationally important" doesn't
hold for legal corpora where doctrinal areas distribute differently than usage
frequency.

**Concrete instance**: RCW Phase 8 kickoff defaulted to "Tier A (top-cited 100)"
as the high-operational-value scope. Investigation showed:

- Top-cited targets: tax/gambling/admin-law (RCW 82.08.030, 82.12.030, 9.46.020) — NOT operator-active matters
- Operator's active-matter clusters (BIIA Title 51, ADA Title 49, PRA 42.17/.17A/.56) hold 126 distinct repealed targets total
- Of those 126, virtually all are singleton-cited (1 xref each) — invisible in xref-count tiers
- Tier-by-xref-count would systematically miss the operationally vital long tail

**Generalizes**: #28 (wrong-axis halt-band — gate on the right metric), #29
(per-title XML inspection — verify at the right granularity). Extends both to
operational-priority axis selection.

**Audit channel**: BEFORE accepting tier-by-cardinality scope, agent verifies
operational relevance via direct target intersection with active-matter title
patterns. The operational-relevance audit is structurally different from the
xref-count audit.

**Resolution this session**: Tier C (full coverage, all 7,284) authorized
because Tier B/A poor cost/value ratio + miss the long-tail operator-relevant
clusters. Per-chapter fetch model means Tier C is only ~25% more cost than
Tier B for ~10× more coverage.

---

## §K corroboration #32 — Corpus-shape-imposed FK ceilings vs cross-corpus halt-band assumptions (2026-05-01)

Halt thresholds derived from one corpus's pattern can be wrong-shape for
another corpus with different cross-reference structure.

**Concrete instance**: USC Phase 4.4 FK resolution achieved 94.58% (operator
benchmark). RCW Phase 4.4 kickoff inherited a "90-99%" halt-band derived
from that USC pattern. RCW corpus structure imposed a **70.0% / 86.9%**
structural floor:

- 19.4% of `state-wa-rcw` xrefs target chapters/titles (`chapter X.YY RCW`,
  `Title T RCW`) — inherently unresolvable to a section-level FK.
- ~13% of A+B section-form xrefs point to sections that were repealed and
  removed from the corpus entirely (no ghost row exists in `rcw.sections`).

Audit predicate (USC's 90% rate) and production predicate (RCW's actual
form distribution) shared the assumption "xrefs are predominantly
section-level and target sections are persistent." That assumption holds for
USC, fails for RCW.

**Generalizes**: #28 (wrong-axis halt-band — gate on the right metric) and
#25 (operator structural-knowledge currency — verify, don't infer from
sibling corpora).

**Audit channel**: BEFORE accepting a halt-band derived from sibling-corpus
precedent, agent surfaces the target corpus's structural shape (form
distribution of cross-references; ratio of resolvable vs structurally-
unresolvable forms). The shape audit is structurally different from the
rate audit.

**Resolution this session**: Halt fired at 70%/87% (below operator's 90%
band). Operator accepted finding, recalibrated halt band to 50,000-65,000
absolute count for RCW's structural ceiling, authorized execution.

---

## §H2 — Statute retrieval in mixed-corpus environments (RCW addition)

When the corpus contains both statutes (USC + RCW) AND case correspondence
about those statutes, generic-concept queries (e.g., "public records request
response time", "appeals procedure", "judicial review") return both classes
ranked together. **This is correct behavior for legal research, not a defect.**

For statute-only retrieval, use `doc_type_filter='statute'`. For cross-
jurisdiction-but-statute-only, combine `doc_type_filter='statute'` with the
`code_jurisdiction_filter` kwarg (`'state-wa-rcw'` or `'federal-usc'`).

For mixed retrieval (statutes + case-doc enforcement examples), accept and
embrace the dual-class result — that's how a researcher actually using this
corpus wants ranking.

When a research question spans multiple doctrinal frameworks (e.g.,
"parenting plans AND GAL roles"), use compound tag filters or chapter-scoped
retrieval rather than expecting any single tag to cover the conjunction.
Tag scope tracks doctrinal authority, not co-occurrence pattern: `gal-wa`
covers chapter 26.44 (where GAL involvement is explicitly authorized), not
chapter 26.09 (parenting plans where GALs commonly serve but the chapter
isn't itself a GAL-framework chapter).

---

## §K — Audit-predicate diversification (load-bearing)

When audit predicates and production predicates share the same logic, both
fail in the same way and the audit silently confirms the bug instead of
catching it. Diversify predicates between audit tools and the production
code being audited where possible.

**Schema-level CHECK constraints serve as audit channels structurally
independent from parser/audit predicates. Use them as belt-and-suspenders
against false-confirmation cascades. The §K rule is fully validated when
the parser, the audit, AND the constraint all agree.**

### Corroboration log (Phase 2 → Phase 4)

This run validated §K six times. Each entry: PARSER PREDICATE / AUDIT
PREDICATE / CHECK CONSTRAINT / OUTCOME.

| # | Failure class | Parser predicate | Audit predicate | Caught by |
|---|---|---|---|---|
| 1 | UCC dash-form section anchors (Title 62A) | `text-indent:0.5in` paras + dot-only anchor regex | Phase 2 anchor counter (same regex) | Phase 3 parser end-to-end run (raised ValueError on missing `<h1>`) |
| 2 | UCC sub-class characterization (601 vs 26 anchors) | dot-only anchor regex | regex-only Method A (anchor + centered-div) | Method B (parser anchor count − sections emitted), structurally different |
| 3 | Standalone synthetic anchors (e.g., 42.17A.89999) | dot-form anchor regex matched these | Phase 3 verify shared regex — both confirmed false count | `chk_section_text_status_invariant` at INSERT (constraint on resulting data shape) |
| 4 | Pending recodification CHECK conflict | parser tried to set `is_recodified=TRUE` with body present | (no audit; was design conflict) | Original `chk_section_text_status_invariant` rejected at INSERT during smoke test |
| 5 | Table-only sections (RCW 9.94A.510 etc.) | body extractor required `text-indent:0.5in` | (no separate audit) | `chk_section_text_status_invariant` rejected 7 sections at INSERT |
| 6 | Hardcoded `text-indent:0.5in` xpath missed `0pt`/`1in`/`-25px` variants | `text-indent:0.5in` exact match | parser-shared anchor count (same predicate) | `chk_section_text_status_invariant` rejected 31 sections at INSERT (Title 24) |
| 7 | HF_SLOW_BATCH halt threshold calibrated to probe baseline (5s for 144ms test sentences) | 5s/batch threshold | (no separate audit; threshold WAS the audit) | First production batches (5-7s on real RCW text) tripped halt 2026-04-30; production-workload baseline differs from probe baseline |
| 8 | Phase 5 validation script SQL bugs escaped to production launch (DISTINCT/random; multi-column scalar subquery) | (script's own queries) | (no pre-flight audit; the script was its own check) | Hit at runtime against production DB during RCW Phase 5 validation 2026-04-30; bypassed all earlier syntactic review |

**Lesson on health-check thresholds (corroboration #7)**: Health-check thresholds must be calibrated against the actual workload they'll encounter in production, not a synthetic probe baseline. Synthetic probe characteristics (short inputs, single-call patterns) systematically underestimate real-workload per-call latency. Calibrate by sampling 10-20 representative production inputs through the same code path before locking thresholds.

**Lesson elevated to load-bearing**: Hardcoded style-attribute predicates
(e.g., `text-indent:0.5in`) are brittle against editorial-template variation.
The CHECK constraint at INSERT time is the structurally-different audit
channel that catches these — exactly as §K predicts. Future style-driven
extraction patterns should use **permissive matching with structural
gating** (e.g., `text-indent:` + `in_notes` flag) rather than value-specific
allowlists, OR maintain explicit allowlists with periodic corpus-wide
audits to detect new variants.

The CHECK constraint cost is essentially zero (validated at INSERT, no
runtime overhead at SELECT) and the audit benefit is high (catches an
entire class of parser-vs-audit predicate-collision bugs).

---

## §L — Stage 2 embedding backfill (deferred work)

Phase 4 long-tail completed in Stage-1-only mode (parse + insert only) due to
HuggingFace inference endpoint being unreachable from the Proxmox host
during the run (ConnectError: No route to host, observed 2026-04-29). Local
CPU fallback works but is ~5× slower (~0.7 chunks/sec vs ~3.0 chunks/sec via
HF endpoint), which would have extended long-tail wall time from ~3.5 hr to
~16 hr.

**Pre-deferral verification (2026-04-29)**: 5-sample numerical-equivalence
check confirmed HF-served and local-CPU-served BAAI/bge-m3 vectors are
mathematically equivalent (cosine similarity range 0.99999739–0.99999901,
well above the operator-set 0.9999 threshold). Mixing embeddings from both
sources in the same corpus is safe.

### Resumption query — counts and identifies chunks awaiting Stage 2

```sql
SELECT
  s.title_number,
  COUNT(*) AS chunks_awaiting_embedding
FROM core.document_chunks c
JOIN rcw.sections s ON s.document_id = c.document_id
WHERE c.embedding IS NULL
GROUP BY s.title_number
ORDER BY s.title_number;
```

Total expected: ~27,000 chunks across the long-tail titles ingested in
Stage-1-only mode.

### Backfill execution

Use the same BGE-M3 embedder (`embedding_service.embed_texts_sync`), batch
in groups of 64, target chunks `WHERE embedding IS NULL` only. Idempotent
under interruption — partial completion leaves untouched chunks for the
next pass.

```python
# Pseudocode skeleton (similar to phase4_ingest_rcw.py stage2_embed_title()
# but parameterless on title — sweeps all NULL embeddings corpus-wide)
batch_size = 64
while True:
    rows = SELECT id, content FROM core.document_chunks
           WHERE embedding IS NULL LIMIT batch_size
    if not rows: break
    vecs = embed_texts_sync([content for _id, content in rows])
    UPDATE core.document_chunks SET embedding = ?, embedded_at = NOW() WHERE id = ?
    COMMIT
```

### Triggering policy

- **HF endpoint healthy**: probe with a single embedding call; if <100ms
  response, run via HF (~3 chunks/sec, ~2.5 hr for ~27,000 chunks).
- **HF endpoint unhealthy after 24-hour ceiling** (operator-set): fall back
  to local CPU (~0.7 chunks/sec, ~10.7 hr) on overnight schedule.

### Post-backfill verification

Once Stage 2 backfill completes for a long-tail title, run the deferred
Pattern (a)/(c) canaries as a separate verification batch. Log per-title:
- Anchor sections (if any specified for that title — most long-tail
  titles have no operator-curated anchor list)
- Tag distribution sanity check
- Cross-jurisdiction canary suite re-run if any priority-anchor titles
  affected (none expected — long-tail titles aren't priority anchors)


## §L-bis — RCW retrieval patterns (parallel to USC §H1)

[TBD — to be drafted at end-of-Phase-4 long-tail completion]

---

## §M — Cross-jurisdiction queries (federal + state combined)

[TBD — drafted in /opt/wdws/rcw_research/cross_jurisdiction_design.md §3-4;
to be folded in here at finalization]

---

## §N — Recodification chains

[TBD — covered partially in cross_jurisdiction_design.md §4.4]

---

## §O — Session-law cross-referencing

[TBD — RCW source credits link to lawfilesext.leg.wa.gov session-law PDFs;
USC has parallel statutes-at-large refs; cross-reference pattern doc TBD]

---

## §P — Deferred follow-ups (post-RCW disposition, pre/during WAC)

Captured 2026-04-30 at RCW project closeout. Do during WAC quiet windows;
do not block WAC Phase 0 kickoff.

### §P.1 — Phase 5 validation script `--dry-run` mode

**Motivation**: The RCW Phase 5 run hit two SQL bugs at runtime — `SELECT
DISTINCT … ORDER BY random()` (column not in select list) and a scalar
subquery returning two columns. Both bugs escaped pre-launch testing because
the script was launched directly against the production DB.

**Spec**:
- Add `--dry-run` flag to `phase5_validation.py`.
- Each check function executes its SQL queries against the DB but skips
  substantive work (no `rag_query`/`hybrid_rrf` calls, no `urllib` fetches,
  no fidelity comparisons).
- Total runtime budget < 30 sec.
- Exit code 0 on clean SQL parse for all 6 checks; nonzero on any
  `psycopg2.errors.SyntaxError`/`InvalidColumnReference`/`UndefinedColumn`.

**Reuse**: Apply the same template to the WAC Phase 5 validation script when
written. The pattern generalizes to any future corpus validation.

**Audit-predicate connection**: §K corroboration #8 — the validation script
was its own audit; no structurally-different channel caught the SQL bugs
before production. A `--dry-run` mode IS that structurally-different channel
(validates SQL parse without trusting the runtime path).

### §P.2 — Encode §H2 mixed-corpus interpretation in Phase 5 Test 2 (DONE for RCW; carry-forward to WAC)

**Status (RCW)**: Implemented 2026-04-30. Test 2 PASS condition is now:
- (a) Statute is retrievable via deterministic SQL citation lookup
  (`sql_citation_lookup_rcw`) — corpus-completeness check.
- (b) ≥1 document substantively discussing the statute appears in top-10 of
  the generic-concept query (proxy: any top-10 doc whose chunks contain the
  citation literal). Retrieval-quality check.

The implementation is in `phase5_validation.py` `check_2_cross_jurisdiction()`.

**Carry-forward**: Apply the identical pattern to any future generic-concept
canary tests in WAC Phase 5 (and any subsequent corpus). The substantive
operational test is "is the statute reachable + does the case-doc layer
correctly surface enforcement context", NOT "is the bare statute in top-10".

**Documented in**: §H2 above.

### §P.3 — 1/20 content-fidelity miss investigation

**Status**: First Phase 5 run reported 19/20 match (1 miss). Subsequent run
with fidelity-miss surfacing reported 19/19 match (0 miss) — sample size
varied because the random query in `check_4_content_fidelity` doesn't always
return 20 candidates (long-tail sampling depends on chance).

**Open question**: Was the original 1/20 miss a systematic fidelity issue
(whitespace/normalization variance, encoding, byte-truncation) or transient
sampling variance? The fidelity-miss surfacing logging is now in place; the
next time `fail_count > 0` we'll have the specific RCW citation and the
`first_para_preview` to investigate.

**Action when triggered**: If a future Phase 5 run shows any fidelity miss,
investigate by (a) fetching the live HTML, (b) comparing byte-for-byte to
DB `full_text`, (c) determining whether the miss is whitespace normalization
(acceptable; just expand the substring tolerance) or actual content drift
(real fidelity issue requiring re-ingestion).

**Boundary**: 5% miss rate (1/20) is the boundary between "acceptable noise"
and "investigate". Above 10% on any run = block on investigation.
