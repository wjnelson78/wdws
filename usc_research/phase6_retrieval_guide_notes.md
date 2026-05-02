# Phase 6 Retrieval Guide — Captured Notes (assembled during Phase 4)

These items must land in the `USC Corpus — Retrieval Patterns for AI Agents`
note that ships in Phase 6. Captured during Phase 4 ingest so they don't
slip between phases.

## ✅ Phase 4.4 — Cross-Reference FK Resolution: COMPLETE (2026-05-01)

The deferred Phase 4.4 second pass (resolves `usc.cross_references.target_section_id`
for internal references) was investigated and remediated. Investigation found
the pass had been documented in `schema_design.sql:209` and `usc_ingest.py:586`
but never written, leaving 0 of 271,590 rows resolved. The remediation
(`/opt/wdws/usc_research/phase4_4_fk_resolution.sql`) text-matches
`target_citation` against the canonical `{title} U.S.C. § {section}` form and
populates `target_section_id`. Result: **52,759 rows resolved** (19.43% of
total xrefs, 76.94% of internal refs). The remaining 23% of internal refs
target titles not yet ingested (T11/T18/T26/T38 + small titles); they will
resolve when their target sections land — re-run `phase4_4_fk_resolution.sql`
after each subsequent title ingest, idempotent on `target_section_id IS NULL`.

Spot-check: § 12132 internal refs now correctly resolve to
`usc.sections.id = 37703107-9830-4e31-93ab-e501c360e77d` (heading "Discrimination").

Sibling-corpus note: rcw.cross_references and wac.cross_references show the
same 0% resolution rate. Per Phase 7 backlog, separate remediation kickoffs
needed for those projects (different agents, different schemas — e.g.,
wa_court_rules has no `target_section_id` column at all and uses a different
authoritative-text model).

Housekeeping companion: two zombie `usc.ingestion_runs` rows (T42 with
`status='running'` since 2026-04-28 from a cursor-bug crash early in the
session) were transitioned to `status='failed'` with descriptive cleanup
notes appended. The actual T42 ingest completed via a successor run.

## §K corroborations #23 and #24 — agent-coordination methodology

These extend the §K corroboration canon (originated in the wdws agent's WA
Court Rules project) into the agent-coordination layer:

### #23 — Cross-project state inheritance vs verified state

**Failure mode:** A kickoff document inherits a claim about sibling-project
state ("RCW + WAC have working FK resolution") and uses it as the comparison
point for assessing the current project. If that inherited claim is wrong,
the agent's disposition recommendation is anchored to false ground.

**Audit channel:** When a kickoff cites sibling-project state, query the
sibling tables directly before accepting the comparison. `SELECT COUNT(*)
FILTER (WHERE target_section_id IS NOT NULL)` on the same column being
investigated is cheap and authoritative; trust verified state over
inherited narrative.

**Concrete instance:** USC investigation 2026-05-01. Kickoff said RCW + WAC
were "complete with documented FK resolution." Direct query showed both at
0% — same defect as USC. Halt-and-surface (per kickoff's explicit halt
trigger for premise contradiction) caught a cross-project pattern: three
corpora share the same deferred Phase 4.4 step. Operator confirmed: kickoff
had conflated `wac.authority_chain.rcw_section_id` (95.51% resolved) and
`wa_court_rules.cross_references.target_rule_id` (97.41% resolved) — different
columns, different metrics — with the `target_section_id` column in
question. Same column name across corpora masked semantically distinct
defects. The agent's discipline in querying before accepting the premise
is the §K methodology working at the agent-coordination layer.

### #24 — Constraint-aware operator instructions

**Failure mode:** An operator drafts SQL using an enum value or column
reference based on the project's design intent, but the actual schema
constraint rejects it. Executing the SQL as-written fails the transaction;
the operator's intent is correct but the literal text is wrong.

**Audit channel:** Before executing operator-drafted SQL that uses literal
enum values or schema-bound identifiers, verify the value against the
actual schema (`pg_get_constraintdef`, `information_schema.columns`,
`\d+ table`). Surface the mismatch with proposed alternatives that satisfy
both the constraint and the operator's intent.

**Concrete instance:** USC zombie cleanup 2026-05-01. Operator's UPDATE
specified `status = 'zombie-superseded'`; `chk_ingestion_status` allows
only `('running','completed','failed','partial')`. Halt-and-propose:
`status = 'failed'` with the descriptive cleanup-provenance text in the
`notes` column captures both the constraint and the operator's intent
(audit trail) without schema modification. Operator confirmed.

**Generalized rule:** Operator-instruction-text and schema-actual-state are
independent audit predicates. Verify both before executing. Same
audit-predicate-diversification principle as #20 (citation regex
incomplete-capture) and #22 (regex-roundtrip vs. enum-membership):
diversify the audit channel against the failure mode you're trying to
catch.

### #25 — Coordinator structural-knowledge currency

**Failure mode:** A kickoff document references statutory or regulatory
structure ("Title 5A contains the Federal Advisory Committee Act") based
on the coordinator's mental model. If the underlying source has been
restructured by a recodification, the structural claim is stale and the
agent's downstream plan is anchored to a location that no longer holds
the cited content.

**Audit channel:** Before accepting a kickoff's structural premise about
where a statute lives, verify the current location via direct corpus
query OR fresh USLM XML inspection. The verification cost is one short
SQL query (`SELECT canonical_citation FROM usc.sections WHERE ...`) or
one head-of-XML inspection (`<heading>` of top-level structural element).

**Concrete instance:** USC Title 5A long-tail kickoff 2026-05-01.
Kickoff said FACA / IGA / EIGA were in Title 5A and operationally
relevant to operator's D.C. District Court action. Verification showed:
Pub. L. 117-286 (Dec. 27, 2022) recodified all three INTO main Title 5
(FACA at §§ 1001-1014, IGA at §§ 401-424, EIGA at §§ 13101+); the
existing priority-core T5 ingest already captures them. The 5A appendix
file now contains only post-recodification stubs (`Repealed.`,
`Transferred]`) plus 107 historical reorganization plans. Surfacing the
recodification fact before bulk ingest let the operator pivot from
"ingest all of 5A for FACA" to "investigate 107 reorg plans for any
judicial-administration overlap" — a meaningfully smaller and more
targeted action.

**Generalized rule:** Generalizes #23 (cross-project state inheritance vs
verified state) to "coordinator structural-knowledge inheritance vs
verified current statutory state." When the kickoff references a specific
statutory location, treat the location claim as a hypothesis to verify,
not a fact to act on. Recodifications happen — Pub. L. 117-286 was the
biggest Title 5 restructure in decades and any kickoff written from
pre-2022 mental models may misroute the agent.

### #26 — Structural-constitutional reasoning as audit channel for ingest scope

**Failure mode:** A kickoff authorizes "investigate whether subset X
contains content addressing concern Y" and the agent runs an empirical
keyword sweep across the subset. If the subset's content domain is
constitutionally bounded such that it CANNOT reach Y (separation of
powers, federalism, jurisdictional limits), the empirical sweep is
redundant work — and worse, may produce false positives that misroute
follow-up effort.

**Audit channel:** Before (or alongside) running the empirical sweep,
ask whether the subset has authority over the target domain at all.
Constitutional / structural analysis of scope-of-authority is a cheap,
authoritative filter that can shortcut empirical work entirely.

**Concrete instance:** USC Title 5A reorganization-plan investigation
2026-05-01. Operator asked whether any of the 107 reorganization plans
in 5A touch AOUSC / Judicial Conference / federal court administration.
Empirical keyword sweep surfaced 5 substantive matches (DOJ, BOP, DC
local courts) — all non-relevant. The structural finding made this
result inevitable: Reorganization Act of 1949 authority is bounded to
**executive-branch** reorganization (5 U.S.C. ch. 9); Article III courts
and judicial-branch administrative entities (AOUSC, FJC, Judicial
Conference) are **constitutionally outside that authority** under the
separation-of-powers boundary between Article II and Article III.
Reorg plans literally cannot reach AOUSC. The constitutional analysis
elevates the empirical "no relevant plans found" finding from "true in
this corpus snapshot" to "true by structural necessity, will remain
true across future release points and recodifications."

**Generalized rule:** Extends #25 (statutory-location currency) to
**scope-of-authority limits on the corpus subset itself**. Two questions
to ask before bulk-ingesting a corpus subset for a specific operational
need:
1. Is the cited content currently located where the kickoff says it is? (#25)
2. Does the corpus subset have constitutional/structural authority over
   the target domain at all? (#26)

When the answer to (2) is no, an empirical sweep is at most confirmation;
the disposition can be reached by structural reasoning alone. Cite the
constitutional/statutory bound (here: Reorganization Act of 1949 §
901(a) executive-branch limits + Article III separation) as the
load-bearing reason, not just empirical absence.

### #27 — Query construction artifacts in operator pre-flight diagnostics

**Failure mode:** A coordinator-drafted diagnostic query extracts citation-
string variants (e.g., via `regexp_match` on `target_citation`) that don't
correspond to actual schema column semantics. The diagnostic surfaces
"pending" entries for citation-text patterns like `'11A'`, `'18A'`, `'28A'`
that look like distinct corpus subsets but are structurally impossible
to store as primary keys (e.g., `title_number SMALLINT` cannot hold
`'28A'`). Acting on the diagnostic without reconciling against the actual
schema column types leads to scope misclassification — agents either
"re-ingest" content that's already present (under a different
column-type representation) or fruitlessly search for content that can
never have existed at the cited location.

**Audit channel:** Before treating a coordinator's pre-flight pending list
as ground truth, reconcile each entry against the actual schema column
type. If `title_number` is `SMALLINT`, the only valid pending values are
integers; string variants like `'28A'` reflect citation-text extraction
patterns, not separate ingestable units. Ingestion happened under
the integer title with a `unit_type` discriminator (e.g., T28a court
rules live as `title_number=28, unit_type='court_rule'`).

**Concrete instance:** USC long-tail Phase B kickoff 2026-05-01.
Operator's pending list included `T28A`. Reconciliation showed:
`title_number=28` has 846 sections + 230 court rules = 1,076 rows; T28a
ingest completed in priority core (`usc.ingestion_runs` id
`91cae97d-...`, status `completed`, 760 embeddings). The `'28A'` entry
in the pending list was a regex artifact from the coordinator's
diagnostic query — the integer-typed `title_number` cannot hold the
text `'28A'`. Same pattern predicts `T11A` (FRBP) and `T18A` (FRCrP)
will subsume into their main-title ingests via `unit_type='court_rule'`,
not appear as separate corpus subsets.

**Generalized rule:** Extends #18 (canary anchor doctrinally invalid)
to coordinator query construction. Diagnostic queries are themselves
authored artifacts subject to construction errors. Reconcile against
the actual schema (column types, constraints, discriminator columns)
before accepting reported state as ground truth.

### #28 — Halt-band calibration on the wrong cardinality axis

**Failure mode:** A coordinator-specified halt threshold is calibrated
against a cardinality axis (e.g., section count) that doesn't match the
operational capacity metric for the corpus subset in question (e.g.,
chunk count, total embedding wall-time, sub-paragraph depth). The agent
correctly fires the halt per the specified threshold, but the underlying
concern was capacity (chunks × wall-time-per-chunk) — not the metric the
threshold gated on. Without surfacing the actual axis breakdown, the
operator can't recalibrate against the right metric.

**Audit channel:** When a halt threshold fires, surface the actual
distribution across multiple cardinality axes — sections, chunks,
chunks/section ratio, sub-element nesting depth, file-size — so the
operator can reason about which axis was the right gating dimension.
The halt is the right discipline; the additional surfacing turns the
halt into a calibration moment rather than a blocker.

**Concrete instance:** USC long-tail Phase B Tier 1, T26 (IRC) launch
2026-05-01. Kickoff specified halt floor of 4,500 sections (based on
"~9,000+ sections estimated" working assumption). Source-file
inspection showed 2,276 sections — below floor by 2x. Halt fired
correctly. But the underlying concern (T26's structural-stress-test
size and runtime) was about content density, not section count: T26 is
the largest USC title by content density (deep `(i)(I)(aa)(AA)`
sub-paragraph nesting), even though it's not the largest by section
count. Surfacing chunks/section projection (5-15 expected vs T42's 1.90
baseline) gave the operator the right axis to recalibrate the halt band
against.

**Generalized rule:** Extends #21 (symptom-based vs cause-based gating)
from failure-detection thresholds to capacity-planning thresholds. When
specifying halt thresholds, ask: what's the operational concern, and
what cardinality axis directly measures it? Section count is a proxy;
chunk count + wall-time-per-chunk is the actual capacity metric. When
halt-bands fire, surface the proxy-vs-metric distinction so the
operator can recalibrate.

### #29 — Agent source-file pre-flight vs coordinator rough estimates

**Failure mode:** A coordinator-supplied estimated cardinality (section
count, chunk count, demand-rank, "this should be roughly N") is used as
a gating value (halt-band threshold, queue ordering, capacity plan)
without first cross-checking against direct source-file inspection. The
coordinator estimate may be 2-3× off in either direction; using it as a
hard threshold either fires a false halt or lets a real anomaly slip
through.

**Audit channel:** Before launching a multi-title or multi-document
ingest tier, the agent runs a per-source structural inspection (XML
section counts after identifier-guard, chunk projections, nesting
depth, xref density, special-handling flags) and surfaces the table for
operator review BEFORE halt-bands are calibrated. The coordinator's
rough estimates serve as a sanity check on the agent's measurements,
not as the source of truth for thresholds.

**Concrete instances (USC Tier B+C kickoff, 2026-05-01):**
1. T26 estimated "~9,000+ sections" → actual 2,276 sections (~4× off).
   Halt-band floor of 4,500 fired falsely. (See #28.)
2. T26 chunks/section estimated "5-15 expected" → actual avg 2.94
   (bimodal: 60.7% single-chunk admin + 2.3% mega-sections). Required
   max+bucket-distribution recalibration mid-run.
3. Tier C demand estimated "~4,127 currently-NULL internal refs" →
   actual 9,537 (~2.3× under-estimate by coordinator). Per-title XML
   inspection produced accurate chunk/section/xref-density numbers
   that the coordinator independently verified before launch
   authorization.

**Generalized rule:** Extends #25 (coordinator structural-knowledge
currency) from statutory-structure currency to estimated-cardinality
currency. The agent's job is to convert source-file structure to
measured cardinality before halt-bands are set; the coordinator's job
is to specify the operational concerns that the cardinality axes
should gate on. Trust source files over working memory.

### #30 — Counter artifacts vs DB ground truth in concurrent-ingest pipelines

**Failure mode:** When parallel ingest processes accumulate per-process
counters (chunks generated, chunks embedded, units processed) in-flight,
the counters may diverge from final DB state due to (a) DELETE-then-
INSERT idempotency consolidating duplicate-URI parses to a single row,
or (b) shared-pool competition where two processes both query
"WHERE embedding IS NULL" against overlapping rows and both increment
their own counter for the same UPDATE. The counters are advisory; they
do not establish data integrity or loss.

**Audit channel:** DB state is ground truth. After any concurrent or
appendix-pair ingest, verify `COUNT(*) FROM core.document_chunks WHERE
document_id IN (...)` and `COUNT(*) FILTER (WHERE embedding IS NOT
NULL)` rather than trusting per-process accumulator messages. If the
counters disagree with DB state, investigate the divergence pattern
(URI-overlap, DELETE-then-INSERT, embed-pool competition) — but do not
treat counter mismatch as a halt-trigger. Halt only on DB-level
inconsistency (chunks unembedded, sections missing chunks, FK
violations).

**Concrete instances (USC Tier C, 2026-05-01):**
1. T18 + T18a Stage 2 concurrency: both processes ran `--title 18`
   simultaneously, both called `embed_pending_for_title(title=18)`,
   both incremented their counters as competing UPDATEs touched the
   same WHERE-NULL pool. T18a accumulator reported 1,154 chunks
   embedded; actual unique T18a chunks = 374. DB final state: 2,029
   (T18) + 341 (T18a) = 2,370 unique chunks, all embedded, no loss.
2. T25 (2,737 → 2,736), T38 (2,290 → 2,289), T40 (587 → 586): 1-chunk
   Stage 1 → Stage 2 deltas. Pattern: chunker accumulator counts a
   section's chunks twice when parser yields a duplicate URI; DELETE-
   then-INSERT idempotency consolidates to one set in DB.
3. T18 substantive section count drift: Stage 1 reported 1,399 units,
   DB has 1,416 unit_type='section' rows for title 18. T18a contributes
   ~17 net-new substantive sections that overlap T18 URIs and got
   DELETE-then-INSERTed. All chunks embedded.

**Generalized rule:** Extends #14 (subagent narrative vs process state)
from agent narrative-vs-actual to in-flight-counter-vs-DB-actual. Same
audit-predicate-diversification principle. When designing concurrent
ingest pipelines, either (a) serialize potentially-overlapping launches
(substantive title before its appendix), or (b) add a discriminator
(e.g., `WHERE unit_type IN (...)`) to scope the embed-pending query to
non-overlapping pools. Mitigation applied at Tier D for T11+T11a
appendix-pair.

**Maintenance note — `status='running'` rows are NOT always zombies.**
Two distinct failure modes can leave a `usc.ingestion_runs` row at
`status='running'`:
1. **Cursor-bug crash (zombie).** Process died mid-Stage-2; row never
   got the closing UPDATE. Cleanup pattern documented in the T42 zombie
   cleanup (transition to `status='failed'` with cleanup-provenance
   notes appended).
2. **Two-stage-halt mid-state (legitimate, expected).** Stage 1 ran
   with `--skip-stage2`; Stage 2 deferred for operator review. Row sits
   at `'running'` legitimately until Stage 2 launches and the same row
   UPDATEs to `'completed'` via `--run-id` continuation.

Differentiator: process status (`pgrep -f usc_ingest`) — if no live
process, examine notes/timestamps to classify; if live, it's mid-state.
Future cleanup scripts must NOT mass-cleanup all `status='running'`
rows on the assumption they're zombies — confirm process state and
elapsed time first.

**Maintenance note — `COUNT(DISTINCT title_number)` ≠ logical-title count.**
Title 28 Appendix (FRCP/FRAP/FRE/FBkrP rules), Title 18 Appendix (FRCrP),
Title 11 Appendix (Bankruptcy Rules), Title 50 Appendix, Title 5 Appendix
all live as `unit_type='court_rule'` (or analog) rows under their parent
`title_number` — not as separate physical title rows. So:
- **Physical-schema state:** `SELECT COUNT(DISTINCT title_number) FROM usc.sections` returns the parent count (e.g., 12 after Tier B close)
- **Logical project-tracking count:** priority-core (5/28/29/42) + T28a + T26 + 7 Tier B = 13 logical titles
Both are correct at their abstraction layer. When writing maintenance
or progress-tracking scripts, choose the count form that matches the
question being asked, and include the appendix-rule sets explicitly when
"how many corpora are loaded" is the operative question.

## Title 5A — skip disposition (2026-05-01)

**Status:** Investigated and skipped with documented justification.
Phase A close-out via skip rather than ingest.

**Why:**
- Operator's FACA / IGA / EIGA need (for D.C. District Court action vs
  AOUSC / Judicial Conference) is already met by the priority-core T5
  ingest. Pub. L. 117-286 (2022) recodified all three acts INTO main
  Title 5: FACA at §§ 1001-1014, IGA at §§ 401-424, EIGA at §§ 13101+.
  All present in `usc.sections` and queryable.
- The 19 `<section>` elements in `usc05A.xml` are all post-recodification
  stubs (`Repealed.`, `Transferred]`) with empty `@identifier`; the
  parser identifier-guard correctly skips them.
- The 107 `<reorganizationPlan>` elements would ingest cleanly but carry
  no operational value for the AOUSC / Judicial Conference matter.
  Reorganization Act of 1949 authority cannot reach Article III courts
  (#26 analysis above). Empirical confirmation: 5 substantive matches
  across the 107 plans, all reaching executive-branch DOJ / BOP / DC
  LOCAL courts — none touch federal court administration.

**Targeted future-ingest path preserved:** if a specific reorg-plan
citation later surfaces in opposing-counsel briefing or operator's
research, a single plan can be ingested in <5 minutes via the existing
parser (already supports `<reorganizationPlan>`) with a one-shot
filtered-emission script. No architectural commitment foreclosed.

**Cross-references that target 5A reorg plans (currently 142 unresolved
"5 U.S.C. § N" refs that don't match priority-core T5):** these don't
correspond to reorg-plan citation form (reorg plans are cited as
"Reorg. Plan No. X of YYYY", not "5 U.S.C. § N"). They look like
pre-recodification administrative-procedure citations whose targets are
elsewhere or no longer extant. Not blocked by the 5A skip.

## Title 53 — skip disposition (2026-05-01)

**Status:** No source XML exists; nothing to ingest. Tier D close-out
documented this absence parallel to T5A.

**Why:**
- USC Title 53 is **historically Reserved** (vacant). It has never been
  given substantive subject matter and contains no statutory provisions.
- The OLRC USLM corpus at PL 119-84 release point reflects this by
  omitting `usc53.xml` entirely. The local extracted set runs T1-T54
  with T53 missing (verified: `find /opt/wdws/data/usc/ -name "*53*"`
  returns zero matches).
- No future-ingest path needed unless Congress later assigns Title 53
  substantive subject matter and OLRC publishes an XML body for it.

**Cross-references that look like "53 U.S.C. § N":** any such citation
in the corpus is either (a) a typographical error in source text, or
(b) a forward-looking reference to a potential future codification.
None are resolvable; FK resolution will leave them NULL. Not blocked
by the T53 skip — the skip simply explains why these will never
resolve.

## A. Status-flag default: active only

21.2% of Title 42 sections are non-current (698 repealed + 909 transferred +
202 omitted out of 8,537). Default retrieval MUST filter out non-current
sections unless the agent explicitly asks for historical/superseded text.

```sql
-- Default: active sections only
WHERE NOT s.is_repealed AND NOT s.is_transferred AND NOT s.is_omitted
```

Helper pattern downstream agents should use:

```python
def fetch_section(citation: str, include_inactive: bool = False):
    """
    Fetch a USC section by canonical citation.

    Default: active sections only. Pass include_inactive=True for
    retrospective litigation involving conduct that pre-dates a repeal,
    transfer, or omission. When include_inactive=True, the response MUST
    surface the status flag plus the historical Pub. L. that took the
    section out of force.
    """
    ...
```

When `include_inactive=True`, the agent must read the matching
`usc.section_notes` rows for `note_topic IN ('Repeal', 'Transfer', 'Effective Date')`
to surface what's actually applicable.

## B. `section_notes` is litigation-critical

T42 alone has 38,479 notes (4.5/section). Three note_topic values control
what version of a statute applies to dated conduct:

| `note_topic` | Why it matters |
|---|---|
| `Effective Date` | Determines when an amendment took effect — for litigation involving conduct on date X, the agent must pull the version in force on X, NOT the current text. |
| `Short Title` | Popular name (e.g., "Americans with Disabilities Act of 1990"). Without this, agents producing pleadings will cite raw section numbers without the canonical popular-name reference. |
| `Transfer of Functions` | When a function moves between agencies, the operative provisions may live elsewhere. |
| `Codification` | OLRC editorial explanation of how the section was codified. Often contains "References in Text" hints. |
| `Construction` | Statutory-construction directives (rules of interpretation). |

JOIN pattern:

```sql
SELECT s.canonical_citation, n.note_topic, n.note_heading, n.note_text
FROM usc.sections s
JOIN usc.section_notes n ON n.section_id = s.id
WHERE s.canonical_citation = '42 U.S.C. § 12132'
  AND n.note_topic IN ('Effective Date', 'Short Title', 'Transfer of Functions',
                       'Codification', 'Construction')
ORDER BY n.sort_order;
```

The Phase 6 guide MUST tell agents: "if you are drafting a citation for an
event that happened before today, fetch effective-date notes too."

## C. `section_history` is the Pub. L. amendment trail

T42 has 17,993 history entries (2.1 per section). For point-in-time
retrieval ("what did § 12132 say in 2015?"), walk `section_history` in
chronological order and reconstruct the version chain.

```sql
SELECT pub_law, pub_law_date, statutes_ref, action_type, action_summary
FROM usc.section_history sh
JOIN usc.sections s ON sh.section_id = s.id
WHERE s.canonical_citation = '42 U.S.C. § 12132'
ORDER BY sh.sort_order;
```

For Phase 7 differential updates, history rows are the diff anchor — when a
new release point ships, comparing source-credit chains tells us which
sections were amended.

## D. Cross-reference traversal patterns

T42 alone has 188,444 cross-refs (22/section). The citation graph is the
connective tissue. Three concrete patterns the Phase 6 guide must show:

### D.1 Find all sections citing a given section (inbound xrefs)

```sql
SELECT src.canonical_citation, src.section_heading, x.context_snippet
FROM usc.cross_references x
JOIN usc.sections src ON x.source_section_id = src.id
WHERE x.target_citation = '42 U.S.C. § 12132'
ORDER BY src.title_number, src.section_number;
```

### D.2 Find all sections cited from a given section (outbound xrefs)

```sql
SELECT x.target_citation, x.ref_type, x.context_snippet, tgt.canonical_citation, tgt.section_heading
FROM usc.cross_references x
JOIN usc.sections src ON x.source_section_id = src.id
LEFT JOIN usc.sections tgt ON x.target_section_id = tgt.id   -- resolved internal refs only
WHERE src.canonical_citation = '42 U.S.C. § 12132'
ORDER BY x.id;
```

### D.3 Build the statutory-framework citation graph (recursive CTE)

```sql
WITH RECURSIVE cite_graph (citation, depth, path) AS (
  SELECT '42 U.S.C. § 12132', 0, ARRAY['42 U.S.C. § 12132']
  UNION ALL
  SELECT x.target_citation, cg.depth + 1, cg.path || x.target_citation
  FROM cite_graph cg
  JOIN usc.sections src ON src.canonical_citation = cg.citation
  JOIN usc.cross_references x ON x.source_section_id = src.id
  WHERE x.ref_type = 'internal'
    AND cg.depth < 3                                  -- depth bound
    AND NOT (x.target_citation = ANY(cg.path))        -- cycle guard
)
SELECT DISTINCT citation, MIN(depth) AS shortest_depth
FROM cite_graph
GROUP BY citation
ORDER BY shortest_depth, citation;
```

The depth bound (3 here) is critical — without it the graph fans out
catastrophically across the whole USC. Agents should default depth=2 for
strategic queries, depth=1 for direct-neighbor queries.

## E. JSONB metadata access — use `@>` containment, not `->>` extraction

`idx_documents_metadata` is `GIN(metadata jsonb_path_ops)` — supports
containment, NOT extraction. USC retrieval queries against metadata MUST
use `@>` to hit the index:

```sql
WHERE metadata @> '{"title_number": 42}'::jsonb              -- ✓ index hit
WHERE metadata @> '{"is_repealed": false}'::jsonb            -- ✓ index hit
WHERE metadata @> '{"chapter_number": "126"}'::jsonb         -- ✓ index hit

WHERE metadata->>'title_number' = '42'                        -- ✗ seq scan
WHERE (metadata->>'is_repealed')::bool = false                -- ✗ seq scan
```

For multi-key filters, build a single JSON object — containment treats it as AND:

```sql
WHERE metadata @> '{"title_number": 42, "is_repealed": false}'::jsonb
```

## F. `unit_type` discriminator — never assume `section`

The `usc.sections` table holds three unit kinds (per the Phase 3 schema
amendment). Queries that intend to retrieve only USC sections must filter:

```sql
WHERE unit_type = 'section'
```

Otherwise FRCP/FRAP/FRE/FRCrP/FRBP rules and 5-A reorganization plans will
appear in results. Conversely, court-rule-specific queries:

```sql
WHERE unit_type = 'court_rule' AND rule_set = 'FRCP'
```

## G. Tag-based retrieval composes with `core.document_tags`

Topical filtering at the document level uses the join-table pattern
(NOT a metadata containment). For "find all USC sections tagged ADA":

```sql
SELECT d.id, d.title
FROM core.documents d
JOIN core.document_tags dt ON dt.document_id = d.id
JOIN core.tags t ON dt.tag_id = t.id
WHERE d.document_type = 'statute'
  AND t.slug = 'ada';
```

Multi-tag intersection ("ADA AND civil-rights"):

```sql
SELECT d.id, d.title
FROM core.documents d
WHERE d.document_type = 'statute'
  AND d.id IN (SELECT document_id FROM core.document_tags dt JOIN core.tags t ON dt.tag_id=t.id WHERE t.slug='ada')
  AND d.id IN (SELECT document_id FROM core.document_tags dt JOIN core.tags t ON dt.tag_id=t.id WHERE t.slug='civil-rights');
```

## H1. Three retrieval patterns for legal research (METHODOLOGY v3)

Real legal-research queries fall into three patterns. Each pattern needs a
distinct retrieval mechanism. **Don't try to do all three with one tool.**

### Pattern (a) — statutory phrasing, scoped to authority

The researcher writes prose drawn from the statute itself. The query:

> `qualified individual disability excluded participation services programs activities public entity`

needs to surface 42 U.S.C. § 12132. Without scoping, the case-law / motion
corpus dominates because it uses identical vocabulary. Solution: filter to
`document_type='statute'` so only statutory text competes:

```python
hybrid_rrf(question="...", domain="legal", doc_type_filter="statute")
```

### Pattern (b) — citation lookup (verbatim)

The researcher types `42 U.S.C. § 12132`. Hybrid RRF is the WRONG tool here:
the english tsquery strips `§`, BGE-M3 maps the citation string to "USC
section breadcrumb" space, and peer sections (§ 12681, § 1301) rank higher.
Use SQL exact-match against `usc.sections.canonical_citation`:

```sql
SELECT d.id, d.title FROM usc.sections s
JOIN core.documents d ON s.document_id = d.id
WHERE s.canonical_citation = '42 U.S.C. § 12132';
```

Deterministic, single-row, citation-perfect.

### Pattern (c) — popular name with tag pre-filter

The researcher writes "ADA Title II". The statute itself contains neither
"ADA" nor "Title II". The connective tissue is the tag taxonomy
(`legal-framework` category — `ada`, `erisa`, `frcp`, `section-1983`, etc.).
Pre-filter the candidate pool by tag, then run hybrid RRF:

```python
hybrid_rrf(question="discrimination public entity disability",
           domain="legal", tag_filter="ada")
```

**CRITICAL — pgvector HNSW + EXISTS interaction:** the pgvector HNSW index
returns top-N nearest neighbors *before* applying any post-filter (the tag
EXISTS clause). At high tag selectivity (small candidate set), zero of the
HNSW's neighbors may pass the filter → query silently returns 0 rows.

Fix: `SET hnsw.iterative_scan = strict_order` once per connection. With
this set, pgvector iteratively broadens the HNSW probe until enough
filter-passing rows surface. **This must be done before any tag-filtered
vector ORDER BY query.**

The wdws cluster has `session_preload_libraries = 'vector'` configured
(/etc/postgresql/17/main/conf.d/10-pgvector-session-preload.conf), so the
GUCs are registered at session start and the `SET` works on the first SQL
of any new connection — no warm-up vector op needed.

**Production note (mcp_server_v2.py:5440-5630):** the current `rag_query`
implementation does not use `tag_filter`. If/when it adopts tag-filter for
USC corpus retrieval, the same `SET hnsw.iterative_scan = strict_order`
must precede the vector ORDER BY query, or popular-name retrieval will
silently fail.

### When to use which pattern

| Researcher intent | Pattern | Tool |
|---|---|---|
| "I'm looking for the statute on this topic" | (a) | hybrid_rrf + doc_type_filter='statute' |
| "I want § 12132 verbatim" | (b) | SQL exact match |
| "Find ADA Title II provisions" | (c) | hybrid_rrf + tag_filter='ada' + iterative_scan |
| "Cross-domain (Rehab Act ↔ ADA)" | (a) variant | hybrid_rrf + doc_type_filter='statute', no tag filter (multi-statute query) |
| "What other USC sections cite this?" | — | SQL on usc.cross_references (see §D.1) |

## G2. Positive/non-positive tags apply only to USC sections

`usc-positive-law` and `usc-non-positive` are doctrinal flags — they apply
ONLY to `unit_type='section'` rows. Court rules (FRCP/FRAP/FRE/FRCrP/FRBP)
and reorganization plans are intentionally untagged on this dimension:

- **Court rules** are promulgated under the Rules Enabling Act
  (28 U.S.C. § 2072) via SCOTUS transmission to Congress and take effect
  via congressional acquiescence rather than affirmative passage. They
  have no Statutes-at-Large equivalent — the Code text IS the rule.
  Neither "positive law" nor "prima facie evidence" describes them.
- **Reorganization plans** (T5A, 107 of them) operate similarly:
  presidential authority + congressional acquiescence.

The discriminator for these is `usc-court-rule` / `usc-reorganization-plan`
(both `usc-meta` category) plus the rule-set tag (`frcp`, `frap`, `fre`,
`frcrp`, `frbp`). When filtering by tag, agents should:

- For "all USC sections in positive-law titles": `tag=usc-positive-law` (already implies unit_type='section' since it's only attached there)
- For "all FRCP rules": `tag=usc-court-rule + tag=frcp` (both via document_tags joins)
- Do NOT filter court rules / reorg plans by `usc-positive-law` or `usc-non-positive` — they aren't tagged either way and will return zero.

## H2. Popular-name retrieval limitations (citation-alias gap)

Popular names that are themselves citation aliases — "Section 504" → 29 U.S.C.
§ 794, "Section 502" → 29 U.S.C. § 1132, "Section 1983" → 42 U.S.C. § 1983 —
sit in a particular blind spot for semantic retrieval:

- The popular name is not in the section's statutory text. § 794 says
  "Nondiscrimination under Federal grants and programs"; nowhere in the
  Code does it say "Section 504." The name lives in legal history (Pub. L.
  93-112 § 504), not in the codified body.
- The tag taxonomy maps the section to the act (`rehab-act`), so pattern
  (c) tag-filtered retrieval CAN surface § 794 — but only because the tag
  pre-filter narrows the candidate pool. Without that pre-filter, the query
  "Section 504 disability federal financial assistance" semantically
  matches every "Nondiscrimination" statute roughly equally, and § 794
  doesn't stand out.

**Recommended retrieval path for citation-alias popular names:**

| Researcher input | Tool |
|---|---|
| `"42 U.S.C. § 1983"`, `"29 U.S.C. § 794"` (canonical citation) | SQL exact match (pattern b) — deterministic, single-row |
| `"Section 1983 against state actor"` | hybrid_rrf with tag_filter=`section-1983` (pattern c) |
| `"Section 504 federal grants disability"` | hybrid_rrf with tag_filter=`rehab-act` (pattern c) |
| `"ADA Title II"`, `"ERISA fiduciary"` | hybrid_rrf with tag_filter=`ada` / `erisa` — these are thematic popular names and pattern (c) works cleanly |

Pattern (c) without a tag filter (the no-filter cross-domain case) is NOT
the right shape for citation-alias retrieval. Document and prefer (b) or
tag-filtered (c) for sections in this category.

**Phase 7 backlog (architectural improvement, not Phase 4 work):** add
an `alt_citations TEXT[]` column to `usc.sections` for primary-act
sections — e.g., `["Section 504", "Rehab Act § 504"]` for § 794. Indexed
trigram search against alt_citations would close this gap without changing
the corpus content fidelity. Not blocking; statutes work for actual
research workflows via patterns (a), (b), and tag-filtered (c).

## I. Phase 7 backlog — proper OLRC content-fidelity round-trip

The Phase 4 attempt (canary_olrc_roundtrip.py) measured the wrong thing:
it Jaccarded `usc.sections.full_text` against the entirety of OLRC's
`<main>` element, which includes statutory notes, editorial notes,
source-credit, advisory-committee comments, navigation breadcrumbs, and
"References in Text" annotations. Our schema correctly stores the
substantive statutory text in `usc.sections.full_text` and splits the
notes/credit/history into `usc.section_notes` + `usc.section_history`.
The high Jaccard divergence (0.30+ on active sections) was measuring
the schema split, not corpus infidelity.

Independent byte-comparison on § 12132 against OLRC + GovInfo + Cornell
LII confirmed substantive content fidelity — the corpus body text is
faithful; the round-trip script's normalization was just too crude.

A proper content-fidelity check would:

1. **Reconstruct the OLRC-equivalent view from the schema.** Concatenate
   `usc.sections.full_text` + ordered `usc.section_history` rows
   (rendered as a parenthetical source credit) + ordered
   `usc.section_notes` rows grouped by `note_type` (statutory notes,
   then editorial notes, then change notes, in OLRC's display order).
2. **Extract just the OLRC `<section>` body element.** OLRC's HTML wraps
   each section in a `<section>` element keyed by `id="...statute-..."`.
   The `<main>` element contains everything around it — strip down to
   the section element only.
3. **Normalize both sides identically.** Collapse whitespace, normalize
   dash variants (— ↔ - ↔ –), normalize quote marks, strip footnote
   superscript markers, remove `<ref>`-rendered citations that may
   render differently in HTML vs our markdown.
4. **Compare via token-level Jaccard with a tighter threshold.** With
   proper noise stripped, divergence on substantively-identical text
   should be <0.05. Anything higher is real drift worth investigating.
5. **Stratified sampling per the original Phase 4 spec** (4 active +
   2 court rules + 2 repealed + 2 transferred), but with corrected URL
   patterns for court rules — OLRC's court-rule URL pattern is NOT
   `granuleid:USC-prelim-titleNa-courtRules-XYZ-ruleN`; the actual
   pattern needs to be probed empirically before the round-trip runs.

This is a Phase 5 validation enhancement, not Phase 4 work. The current
Phase 4 corpus is fidelity-verified by the §12132 spot check; bulk
content audit can wait for the corrected methodology.

## J. Avoid opaque function calls on indexed predicates (planner gotcha)

Wrapping an indexed column in a function disables PG's ability to fold a
parameter into a constant predicate at plan time — and with pgvector HNSW
in the picture, this can silently return INCORRECT (not just slow) results.

| Predicate shape | Plan picked | Behavior |
|---|---|---|
| `LOWER(t.slug) = LOWER(%s)` (canary v3 first-cut) | HNSW Index Scan + Nested Loop Semi Join | HNSW returns top-N nearest chunks; tag filter applied AFTER. At high tag selectivity, target chunks are silently outside the HNSW probe and DROPPED. `hnsw.iterative_scan = strict_order` does not rescue this query shape. |
| `t.slug = %s` (param matches column type/case) | HashAggregate over tags + Sort by distance | Filter-first plan; correct results. PG folds the param into a constant predicate, sees the small candidate set, picks the right join order. |

**Generalizes beyond slug.** Any indexed-column-wrapped-in-function in
production retrieval code (rag_query, future Athena query rewriters,
metadata filters) will hit the same plan-shape issue. The canary fix:
pre-resolve the input in app code (e.g., lowercase the tag input once,
look up the canonical slug, then use single-equality `t.slug = %s` in the
retrieval SQL).

**Quick rule:** if a retrieval query combines a vector ORDER BY with a
filter, and the filter is selective (pool ≪ index size), CHECK THE PLAN
BEFORE TRUSTING THE RESULTS. Empty results from a filter that should
match rows is a planner-opacity smell, not a tag-data-missing smell.
(Verifiable: SELECT COUNT against the same filter without the vector
ORDER BY. If count > 0 and the vector query returns 0, planner opacity
is your culprit.)

## K. Court rule disambiguation in OLRC USLM appendix files

**STATUS: FINAL** — Strategy B selected (first-occurrence-wins on duplicate URIs within a wrapper). 8 Social Security Supplemental Rules dropped from corpus and logged at `/opt/wdws/data/usc/logs/strategy_b_dropped.log`. FRCP main Rules 1-8 re-ingested with correct content; dictionary check verifies all 230 court rules clean.

The OLRC USLM appendix XML files bundle multiple distinct rule sets under a
single `<courtRules>` wrapper, with `<courtRule>` elements that share the
same `@identifier` URI prefix. Naïve canonical-citation generators (taking
`@identifier` + `<num value=>` as the key) collide and silently overwrite
real rules with content from supplementary rule sets via `ON CONFLICT`.

### Confirmed cases as of PL 119-84

| File | Wrapper | Rule sets bundled | Duplicate count |
|---|---|---|---|
| `usc28a.xml` | `/us/usc/t28a/courtRules/Civil` | Federal Rules of Civil Procedure (Rules 1-86) + Supplemental Rules for Social Security Actions Under 42 U.S.C. § 405(g) (Rules 1-8) | **8 duplicates** |
| `usc28a.xml` | `/us/usc/t28a/courtRules/App` | Federal Rules of Appellate Procedure only | 0 |
| `usc28a.xml` | `/us/usc/t28a/courtRules/Evid` | Federal Rules of Evidence only | 0 |
| `usc18a.xml` | `/us/usc/t18a/courtRules/Crim` | Federal Rules of Criminal Procedure only | 0 |
| `usc11a.xml` | (no identifier — heading "FEDERAL RULES OF BANKRUPTCY PROCEDURE") | Federal Rules of Bankruptcy Procedure only | 0 |

**Validation rule for the ingest pipeline:** for any new appendix file, count
`(wrapper_identifier, num.value)` pairs and emit zero if duplicates exist.
HALT and surface for design review. Don't assume future appendix files are
single-rule-set; the duplicate-bundling pattern is real and undetectable
from URI structure alone.

### Sub-shell discriminator (canonical citation generator)

Within a wrapper that bundles multiple rule sets, the parent element of
each `<courtRule>` is the disambiguating signal:

| Sub-shell element | Indicates |
|---|---|
| `<title>` with `@identifier="/us/usc/t28a/courtRules/Civil/tI"` (or similar) | Main rule set (FRCP) |
| `<level>` with no `@identifier` and heading "SUPPLEMENTAL RULES FOR ..." | Supplementary rule set |

The `<heading>` text on the immediate parent (e.g., "SUPPLEMENTAL RULES FOR
SOCIAL SECURITY ACTIONS UNDER 42 [U.S.C. § 405(g)]") names the supplementary
rule set explicitly when present.

### Detection methodology — known-anchor dictionary

The Phase 4 LIKE-mismatch audit (`title NOT LIKE '%heading%' OR full_text
NOT ILIKE '%heading%'`) catches mixed-content corruption (heading from one
section + body of another) but MISSES fully-substituted corruption where
heading, title, and full_text all carry coherent but wrong content. For
court rules specifically, a known-anchor dictionary cross-check is the
required complement:

```python
KNOWN_FRCP_HEADINGS = {
    "1": "Scope and Purpose",
    "2": "One Form of Action",
    "3": "Commencing an Action",
    "4": "Summons",
    # ... full FRCP set ...
}
# DB heading must substring-match the dictionary heading (case-insensitive,
# either direction). Mismatch = full substitution corruption.
```

The dictionary itself can be built deterministically by parsing the
appendix XML with the corrected sub-shell-aware parser — first-occurrence
per (wrapper_identifier, num) is the authoritative entry IF the wrapper
contains only one rule set; otherwise sub-shell discrimination determines
which occurrence is the main one.

### Litigation-priority impact

FRCP rules 1-8 were the only court rules confirmed corrupted by this bug
class in Phase 4. FRCP 17(c)(2) — load-bearing for active federal cases —
is in the clean range (Rule 9+) and was independently verified intact.
FRAP 1-48, FRE 101-1103 are clean (no duplicate sub-shells). FRCrP 1-60
and FRBP 1001-9037 are not yet ingested but are clean of the duplicate
pattern at the XML level.

### Strategy B resolution (Phase 4)

Implemented in `usc_parser.py:parse_title_xml`: within each `<courtRules>`
wrapper, track `num.value` of every emitted `<courtRule>`. On a duplicate
emission, drop and log to `/opt/wdws/data/usc/logs/strategy_b_dropped.log`
with the format:

```
file=usc28a.xml wrapper=/us/usc/t28a/courtRules/Civil num=1
  heading="Review of Social Security Decisions Under 42 U.S.C. § 405(g)"
  reason=duplicate_uri_first_occurrence_wins
```

Dropped units in PL 119-84:
- Social Security Supplemental Rules 1-5 (under FRCP wrapper, second occurrence)
- Tax Court / Court of Federal Claims Rules 6-8 (Brief / Reply Brief sequence)

Total: **8 court rules deliberately excluded from the Phase 4 corpus.**

If litigation scope ever requires SS supplemental procedure or Tax Court
practice, run a separate focused ingest project against authoritative
sources: SSA appeals procedure pages (ssa.gov), ustaxcourt.gov, and
uscfc.uscourts.gov. The OLRC bundling is not the right shape for those
rule sets — they belong as their own corpus subdomains.

### Phase 7 hardening backlog

- Ingest pipeline should validate `(wrapper, num)` uniqueness per appendix
  file as a pre-flight check, not silently rely on it.
- The known-anchor dictionary for FRCP/FRAP/FRE/FRCrP/FRBP (and any future
  rule set in the corpus) belongs in a versioned constants file checked
  into the parser repo, alongside the OLRC release-point identifier. Diff
  the dictionary across release points to surface real OLRC rule changes
  vs. parser regressions.
- **Future ingest project for Social Security Supplemental Rules and Tax
  Court / COFC Rules** from authoritative sources (SSA appeals procedure
  pages, ustaxcourt.gov, uscfc.uscourts.gov) if litigation scope expands
  to require them. They are intentionally absent from the current corpus
  per Strategy B's "first-occurrence-wins" disambiguation.

## H. Rule-set filter for court rules

When the question is rule-specific (FRCP 26, FRE 401, etc.), filter on
`unit_type='court_rule' AND rule_set=...` rather than scanning all chunks.
The `idx_sections_rule_set` partial index makes this cheap.

```sql
SELECT canonical_citation, section_heading
FROM usc.sections
WHERE unit_type = 'court_rule' AND rule_set = 'FRCP'
  AND section_number = '17';
```
