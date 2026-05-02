---
title: WA Appellate Case Law — Phase 6 Retrieval Guide Notes
date: 2026-05-01
status: Phase 2 launching (CL bulk-data PostgreSQL COPY pivot authorized 2026-05-01 post-Phase-1-halt + post-Phase-2-launch-divergence)
---

# WA Appellate Case Law Ingest — Phase 6 Retrieval Guide Notes

**Project:** /opt/wdws/wa_caselaw_research/
**Source:** CourtListener / Free Law Project (CC0 / Public Domain Mark — bulk-data S3, PostgreSQL COPY format)
**Source change provenance:** Path B authorized 2026-05-01 after Phase 1 halt
established that the kickoff's authorized source (courts.wa.gov) covered only
2013-present and missed 7 of 11 operationally-relevant authorities. Halt-findings
record at `phase1_investigation/phase1_halt_findings.md`.
**Acquisition pivot provenance:** Bulk-data PostgreSQL-native pivot authorized
2026-05-01 (mid-Phase-2 launch). Phase 1 scope plan recommended API pagination
+ citation-map CSV; Phase 2 launcher diverged to full bulk CSV download.
Operator authorization-after-the-fact resolved the divergence; runtime choice
held because §K halt-and-investigate by the agent surfaced both the
runtime-vs-plan divergence and a coordinator factual error in the pivot
directive itself before any destructive corrective action. See §K #25 7th
instance + §K #43 candidate below.

---

## §K canon inheritance — corroborations #1-#40 + new #41 candidate

This project inherits the §K methodology canon at corroborations #1-#40 from
five prior project phase6 canons + one close-report:

- /opt/wdws/usc_research/phase6_retrieval_guide_notes.md (#1, #3, #14-30 USC tier)
- /opt/wdws/rcw_research/phase6_retrieval_guide_notes.md (#7, #32, #34-37 RCW tier)
- /opt/wdws/wac_research/phase6_retrieval_guide_notes.md (#1-13, #31 WAC tier)
- /opt/wdws/wa_court_rules_research/phase6_retrieval_guide_notes.md (#14-22, #29-39 WCR + refinement-sweep tier)
- /opt/wdws/wa_constitution_research/phase6_retrieval_guide_notes.md (#25/29 reinforcements + #40 WA-Const tier)
- /opt/wdws/wa_court_rules_research/refinement_sweep_close_report.md (#39 originating-record context)

(The kickoff's `refinement_sweep_research/` canon path does not exist as a
separate directory; refinement-sweep work is integrated into the WCR project's
phase6 notes + close report. The WA Constitution Phase 1 stub recorded the same
disposition. Methodology inheritance intact via 5 phase6 canons + 1 close report.)

---

## §K corroborations canonized this project

### §K #45 NEW (operator-canonized 2026-05-01) — TRUNCATE CASCADE ignores ON DELETE policy

**Failure mode:** A foreign-key constraint with `ON DELETE SET NULL` (or `ON
DELETE NO ACTION`, `RESTRICT`, etc.) does **not** prevent CASCADE truncation
of the referencing table. `ON DELETE` only governs row-level DELETE
operations. `TRUNCATE ... CASCADE` always propagates to every referencing
table regardless of the ON DELETE policy.

**Audit channel — pre-flight (mandatory before any TRUNCATE CASCADE):**
```sql
SELECT confrelid::regclass AS referenced, conrelid::regclass AS referencing,
       conname, confdeltype
  FROM pg_constraint
 WHERE confrelid::regclass::text = '<target_table>'
   AND contype = 'f';
```
If the result contains any referencing table whose rows must be preserved,
choose an alternative clearing mechanism — DO NOT proceed with TRUNCATE
CASCADE.

**Safer alternatives:**
- `DELETE FROM <table> WHERE 1=1` — slower but row-level, respects ON DELETE
  policy; SET NULL fires correctly.
- `DROP CONSTRAINT <fk> ; TRUNCATE ; ADD CONSTRAINT <fk>` — unambiguous,
  audit-trailed.
- Drop+recreate the schema entirely if the referencing tables are
  reconstructible.

**Concrete instance (WA Case Law Phase 2/3, 2026-05-01):**
- Agent issued `TRUNCATE case_law.opinions CASCADE` to clear partial-load
  staging in case_law derivative tables, before re-running with fixed
  CSV parser.
- Migration 037 had added `target_case_law_opinion_id INTEGER REFERENCES
  case_law.opinions(id) ON DELETE SET NULL` to all 5 per-corpus
  cross_references tables.
- TRUNCATE CASCADE propagated through the FK relationships, **truncating
  all 5 cross_references tables**, even though `ON DELETE SET NULL` on
  the FK suggested SET NULL semantics.
- Total rows lost: 1,365,994 (rcw 75,117 + usc 1,199,699 + wac 89,082 +
  wcr 2,072 + wa_const 24).
- Recovery: rebuild from each corpus's phase4_4_fk_resolution.sql against
  unchanged corpus full_text (deterministic regex extraction) + manual
  WA Constitution xref reconstruction. ~1-2 hour wall time.

**Generalizes:** #43 (resource-architecture discipline; pre-flight verifies
disk-headroom budget) → #45 (referential-integrity-side-effect discipline;
pre-flight enumerates referencing tables before destructive operations).
Both belong to the same audit-channel class: pre-flight verification of
*the second-order consequences of a primary action*, not just the action
itself.

**Mitigation pattern (for future projects):** Add `target_*_id` columns to
existing cross-corpus tables as **plain INTEGER/UUID without `REFERENCES`
clause** when the referenced corpus is itself a frequently-rebuilt /
freshly-reload-able corpus. The application-layer JOIN still works
(`LEFT JOIN case_law.opinions o ON o.id = xref.target_case_law_opinion_id`)
but no DB-level FK enforcement exists, so TRUNCATE on the referenced
corpus does not cascade. Trade-off: orphan-id detection becomes the
application's responsibility (or a periodic verification query), not the
DB's. For corpus tables that are reloaded only via tightly-controlled
migrations, the operational discipline is acceptable.

**Renumbering note:** This corroboration was originally proposed as #44
during incident triage, but #44 was already canonized earlier in the
session by the storage-provisioning agent (online-resize vs physical-add
on virtualized hosts). Operator-canonized as #45 to preserve numbering
continuity.

---

### §K #43 NEW CANDIDATE — Bulk-data scale exceeding storage headroom requires streaming-architecture (canonized 2026-05-01)

**Operator-confirmed canon entry (Phase 2 architectural pivot, 2026-05-01):**

When acquiring an external bulk corpus whose decompressed size exceeds local
storage headroom by ≥3×, the agent MUST design Phase 3 ingest as a
streaming-decompress + filter pipeline with no full-corpus on-disk
materialization, rather than a staged-then-filter pattern.

Generalizes #11 (schema discipline during ingest) → #43 (disk-resource
discipline during ingest). Same audit channel: pre-flight verifies the
resource budget against the projected materialization, and routes
architecture choice on the budget rather than on the convenience of the
default-import pattern.

**Audit channel:** Agent computes `decompressed_size / disk_headroom` at
Phase 1 close. If ≥3, Phase 3 design must use streaming pipeline (`bzcat
| filter | psql COPY FROM stdin` or `COPY FROM PROGRAM` with external
filter), NOT the default `bzcat | psql COPY` which materializes a full
staging table inside the pg datadir. If the ratio is unverified at
pre-flight and surfaces only at COPY-FROM-fails-with-disk-full
mid-execution, that is a §K #43 violation — same severity class as
schema-corner-cut-discovered-mid-ingest under #11.

**Concrete instance (WA Case Law Phase 2, 2026-05-01):**
- Decompressed projection: ~200 GB (opinions ~140 GB + dockets ~50 GB +
  clusters + citations + parentheticals).
- Local disk headroom (`/dev/sda2` post-download): ~4 GB free after
  opinions + dockets bz2 land on disk.
- Ratio: ~50× decompressed-vs-headroom. Trivially over the 3× threshold.
- Architecture decision: external-streaming filter via Python (csv module
  for embedded-comma/quoted-newline correctness) → psql COPY FROM stdin.
  Final case_law.* tables hold ~94K WA-scoped rows (~14 GB), well within
  budget.
- bz2 source files retained on disk during Phase 3 (read-only, streamed)
  and cleaned post-Phase-6 once cross-corpus FK uplift is complete.

**Sub-pattern: filter-cardinality dependency ordering.** Streaming pipelines
must be ordered so each filter step can run with only the prior step's output
in memory (or a small id-set on disk). For CL bulk: dockets first (yields WA
docket-id set, ~100K UUIDs), then clusters filtered by docket-id, then
opinions filtered by cluster-id, then citation-map / citations / parentheticals
filtered by opinion-id or cluster-id. Audit channel: filter manifest is
acyclic and each step's working-set fits in memory budget.

**Mitigation pattern (for future projects):** Phase 1 close MUST include
a `decompressed_size_estimate_GB` field and a `disk_headroom_GB_at_pre_acquire`
measurement. If the ratio ≥3, Phase 3 design defaults to streaming pipeline.
Add `filter_dependency_order` field to Phase 1 close artifact when the
streaming path is selected.

**Generalizes:** #11 (schema discipline) was about NOT cutting corners in
the schema during a hot ingest path. #43 is the resource-budget analog:
do not cut corners in the *resource architecture* by deferring it to
COPY-FROM time. Both are pre-flight gates that surface the actual cost
before commitment.

---

### §K #42 NEW CANDIDATE — Coordinator scope-cardinality inflation runs in BOTH directions (canonized 2026-05-01)

**Operator-confirmed canon entry (CL bulk-data pivot discovery, 2026-05-01):**

§K #29 has been canonized as "coordinator over-estimates scope cardinality;
agent verifies per-source actuals at pre-flight." This project surfaces the
inverse: **coordinator can also under-estimate the available infrastructure**.
The kickoff assumed CL access via API pagination; it did not anticipate that
CL publishes its full PostgreSQL database as bulk COPY-format files with
pre-computed citation graphs (search_opinionscited table) and quarterly
regeneration cycles.

**Audit channel during pre-flight:** When investigating an external source,
agent verifies BOTH:
- (a) the declared scope's actual cardinality vs. coordinator estimate
  (#29 over-estimation direction — 7 instances this session)
- (b) the available infrastructure / acquisition path richness vs.
  coordinator estimate (#42 under-estimation direction — 1st instance:
  CL bulk-data path richer than the API-pagination path the coordinator
  assumed)

Same generalization frame as #41: coordinator's pre-flight model is a
candidate hypothesis to be verified, not a constraint on what the agent
investigates. The canonized #29 mitigation ("verify per-source actuals
before locking halt-band") extends to "verify per-source acquisition-path
richness before locking acquisition strategy."

**Concrete instance (CL bulk-data pivot, 2026-05-01):**
- Coordinator's Phase 1 plan: API pagination (~2,340 calls, ~1-2 hours) +
  citation-map CSV. Reasonable framing under the coordinator's hypothesis
  that bulk acquisition would be "54 GB compressed, ~200 GB decompressed —
  impractical."
- Agent verification (post-Phase-2-launch divergence + operator pivot):
  Bulk path provides PostgreSQL COPY format (no JSON parser needed),
  pre-computed citation graph (eliminates Phase 5 entirely), quarterly
  regeneration (matches authoritative-source pattern), public domain
  license (no compatibility risk for active-litigation use).
- Net wall-time: ~2-2.5 weeks (from 2-3 weeks) due to Phase 5 elimination
  + Phase 3 simplification (no JSON parser).

**Generalizes:** #29 mitigation extends from "verify cardinality" to
"verify acquisition surface area." Both axes need the same per-source
pre-flight discipline. Coordinator estimates are heuristic priors;
authoritative source documentation is the verified posterior.

---

### §K #41 candidate — Source-authorization-vs-operational-citation-pattern mismatch (canonized 2026-05-01)

**Operator-confirmed canon entry (Path B authorization, 2026-05-01):**

When evaluating a source for case-law (or any external) corpus ingest, verify
that source coverage aligns with the **operator's existing citation patterns**
rather than coverage-as-such (volume, vintage, or institutional reputation).
Source selection that misses operator's most-cited authorities is structurally
wrong even if the source is "authoritative" in the abstract. Same
audit-predicate-diversification principle as #34 (operational relevance vs
xref-count distribution) — applied to source selection rather than per-row
materialization.

**Audit channel:** During pre-flight, agent verifies operator-citation-prevalence
overlap with source coverage, NOT coverage-volume metrics alone. Concretely:
sample N operator-cited authorities (e.g., 11 from active-matter doctrinal
clusters) and verify each is reachable via the candidate source. If miss-rate
exceeds halt threshold, halt-and-surface BEFORE locking source authorization.
Coverage-volume audit (e.g., "courts.wa.gov has ~25K opinions") and
operational-citation-pattern audit ("courts.wa.gov reaches 4 of operator's 11
foundational authorities") are structurally different audit channels — both
required.

**Concrete instance (WA Appellate Case Law Phase 1, 2026-05-01):**
- Kickoff authorized `courts.wa.gov exclusively`. Coverage volume ~15K-25K
  opinions (Feb-2013 to present). Coverage-volume audit alone would frame this
  as "smaller than projected, restrict scope" (Path A in halt-findings).
- Operator-citation-pattern audit: 11 foundational authorities sampled. 7 of 11
  pre-date 2013-02-22, including:
  - Marquis v. City of Spokane, 130 Wn.2d 97 (1996) — WLAD foundation, 8 operator-cited docs
  - State v. Gunwall, 106 Wn.2d 54 (1986) — independent state doctrine anchor, 2 operator-cited docs
  - City of Federal Way v. Koenig, 167 Wn.2d 341 (2009) — PRA framework, 1 operator-cited doc
  - Jafar v. Webb, 177 Wn.2d 520 (2013) — borderline; 38 operator-cited docs
  - Plus Fell, Negron, Frisino, Loeffelholz, Nast (operator's WLAD progeny + court records access lineage)
- Source-authorization verdict: courts.wa.gov fails the operational-citation
  audit even though it passes the coverage-volume audit (in the constrained
  sense). Path B (CourtListener authorization) selected because it satisfies
  both audit channels.

**Generalizes:** #34 (operational relevance vs xref-count distribution at
ingest-tier-selection) → #41 (operational citation prevalence at
source-selection). Same principle: tier/source selection should gate on
operationally-load-bearing axis, not on convenient-cardinality axis. Operator
citation-prevalence is the structurally-different audit channel from
source-coverage-volume.

**Mitigation pattern (for future projects):** any kickoff that authorizes a
single external source for ingest MUST include a Phase 1.0 task: sample N
operator-cited authorities (target N ≥ 10) and verify each is reachable via
the source. Halt and surface on miss-rate > kickoff-specified threshold (this
project: > 2 of 11 = halt). Source coverage in the abstract is necessary but
not sufficient — operationally-relevant subset must also be in scope.

**Canon-extension note:** §K #41 generalizes to any external-source
authorization decision (not just case-law ingest). Future corpus-source
selections — federal case law, agency-rulings, treatise corpora, reporter-series
expansions — should run the operational-citation-pattern audit alongside the
volume audit at the pre-flight gate, not as a post-acquisition discovery.

---

## §K corroborations reinforced this project

### §K #25 reinforcement — 7th instance this session: coordinator-directive factual error caught by agent halt-and-investigate (canonized 2026-05-01)

**Failure mode (refined to coordinator-directive layer):** §K #25 instances
1-6 covered stale URLs / endpoint drift / parameter-scheme drift in
operator-authored kickoffs and prior session memory. The 7th instance is
*coordinator factual error in a real-time directive*: the architectural-pivot
directive instructed the agent to "cancel ongoing API-based bulk download
in progress (~50.5 GB opinions ~4% complete)." Verified state at the
moment of directive: the running download (PID 327383) was wget against
the S3 PostgreSQL COPY-format bulk URL — exactly the path the pivot was
authorizing. There was no API-based download to cancel.

**Generalizes:** #25 originally framed as "kickoff stale" but the
underlying audit channel is broader: any coordinator claim about
present runtime state must be verified against actual runtime state
before the agent acts on the claim — kickoff documents, session-memory
recall, and real-time directives are all coordinator artifacts that can
factually drift from reality. Halt-and-investigate is the correct
disposition; the cost of one round-trip clarification is trivially
less than the cost of killing 34 GB of in-progress download.

**Concrete instance (CL bulk pivot directive, 2026-05-01):** Agent caught
two separate coordinator-side errors in the same directive:
- (a) Premise error: directive claimed an API-based download was running
  when the running process was the bulk-CSV download.
- (b) Plan-divergence omission: directive presented the bulk-CSV approach
  as a "pivot" but the runtime had already diverged from the Phase 1
  scope plan to the bulk-CSV path. The "pivot" was retroactive
  authorization of a runtime divergence, not a clean re-direction.

**Disposition:** Coordinator confirmed both findings + canonized this
instance as #25's 7th occurrence. Agent did not kill PID 327383; download
finished cleanly and is the artifact Phase 3 consumes.

---

### §K #25 reinforcement — 6th instance this session: kickoff URL doubly stale (endpoint AND parameter scheme)

**Failure mode (refined):** kickoff URLs may be stale at *both* the endpoint
level (path moved) AND the parameter-scheme level (query parameter renamed or
semantically repurposed). HTTP 200 is not enough — body fingerprint required.

**Concrete instance:** kickoff URL
`https://www.courts.wa.gov/opinions/?fa=opinions.allopinions&courtId=A08`
returns HTTP 200 (not 404 or 301) but the body is the courts.wa.gov error page
("You have requested a page that does not exist"). All four courtId values
return identical 7,126-byte error page (MD5 ea5c66501c625312f61d382e14d82499 for
the v2 displayAll variant). Without body inspection the agent could have
proceeded to "URLs work, scaling fine" misjudgment. Endpoint moved to
`/opinions/index.cfm?fa=opinions.byYear&fileYear=YYYY&crtLevel=S/C&pubStatus=PUB/PAR/UNP`.

**Generalizes:** prior 5 instances of #25 (USC 5A recodification, USC long-tail
title currency, RCW Phase 8 ghost-row title-existence, WA Constitution kickoff
URL drift, WA Constitution Article XX status note) were content-level. This 6th
instance is endpoint + parameter scheme level. Same audit channel: verify the
premise via body-fingerprint check, don't infer from prior project memory or
HTTP status alone.

### §K #29 reinforcement — 6th instance this session: coordinator scope-cardinality inflation

**Concrete instance:** kickoff projected ~160K-250K opinions back to 1950 from
courts.wa.gov. Verified scope reachable from courts.wa.gov: ~15K-25K opinions
(2013-present aggregate). Inflation factor ~6-17× — the largest observed this
session (prior largest was WCR Task 4 at 55%). The inflation here is not from
session-memory aggregate but from a source-currency assumption that didn't
survive Phase 1 verification.

**Generalizes:** #29 inflation isn't only about session-memory aggregates vs
per-source counts; it can also surface as the boundary case where the source
itself does not contain the full set the coordinator believed it did. Same
mitigation: per-source pre-flight verification before any halt-band lock.

### §K #37 sharpening — provable-existence applies to source-authorization scope

**Failure mode:** §K #37 was canonized at the per-row materialization level
(RCW Phase 8 ghost-row materialization for repealed sections). This Phase 1
extends the principle: an authorized source must provably contain the declared
scope's content, not be assumed via the source's name or institutional
reputation.

**Concrete instance:** "courts.wa.gov" has the institutional reputation of "WA
appellate court source" but in fact serves only 2013-present slip opinions.
Provable-existence-failed at Phase 1 source verification.

**Generalizes:** `source name × declared scope × actual source coverage` is a
trinary verification that must occur at Phase 1 pre-flight, not at Phase 2
acquisition wall-time discovery.

---

## §A — WA Appellate Case Law retrieval patterns (TBD, Phase 6)

[Drafted at end of Phase 6 once schema + ingest + cross-corpus FK uplift are
operational. Will mirror §S (WA Court Rules) and §P (WAC) sections of prior
project canons.]

---

## §B — Six-jurisdiction queries (TBD, Phase 6)

[Drafted at end of Phase 6 — extend §T (four-jurisdiction) / §B (five-jurisdiction
including wa-constitution) to six-jurisdiction including case_law.]

---

## §C — Phase 7 backlog (TBD, Phase 6)

[Captured during Phases 2-6 as encountered. Likely items: CL citation-graph
parallel-citation reconciliation; opinion-cluster vs opinion-document
disambiguation; treatment-signal extraction (out-of-scope per kickoff but a
documented Phase 7 candidate); per-Division (Div I/II/III) metadata extraction
where CL exposes it.]
