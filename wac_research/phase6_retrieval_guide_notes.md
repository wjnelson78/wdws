# WAC Retrieval Guide — Phase 6 §P/Q/R

**Phase 6 deliverable.** Author: Athena WAC-Ingest agent. Date: 2026-05-01.
Companion to `/opt/wdws/usc_research/phase6_retrieval_guide_notes.md` and `/opt/wdws/rcw_research/phase6_retrieval_guide_notes.md`.

This document documents WAC-specific retrieval patterns, the WAC→RCW authority-chain queryability, and three-jurisdiction queries combining federal-usc + state-wa-rcw + state-wa-wac.

---

## §P. WAC retrieval patterns

### §P.1 Citation format conventions

| Form | Example | Where it appears |
|---|---|---|
| Section citation | `WAC 296-15-001` | `wac.sections.canonical_citation` (canonical) |
| Chapter citation | `chapter 296-15 WAC` | Body text references; not a separate row |
| Title citation | `Title 296 WAC` | Body text; tag `wac-title-296` |

WAC uses **dashes** throughout (`T-CC-SSS`), unlike RCW which uses dots (`T.CC.SSS`). When parsing user queries that mix RCW/WAC citations, the dash-vs-dot distinction is the cheapest discriminator.

### §P.2 Three-flavor row taxonomy (mirrors RCW)

`wac.sections` rows fall into three flavors per the `chk_section_text_status_invariant` CHECK constraint:

1. **Live current section** — `full_text` populated, all status flags FALSE, chunked + embedded into `core.document_chunks`. Typical retrieval target.
2. **Recodified-out ghost** — `is_recodified=TRUE`, `recodified_to` set, `full_text=NULL`, no embeddings. Allows old citations to resolve to current via `recodified_to`.
3. **Repealed/decodified/expired/reserved ghost** — corresponding flag TRUE, `full_text=NULL`, no embeddings.

### §P.3 Empty-body classes (WAC-specific)

WAC has 5 sub-classes of "active section but empty operative body" that the parser handles via placeholder text in `full_text`:

| Class | Trigger | Placeholder text begins with |
|---|---|---|
| **A: image-only** | Body contains `<img>`/`<picture>` (regulatory diagram/flowchart) | `[Section content is a regulatory diagram or flowchart...` |
| **B1: part-heading shell** | `<div text-align:center>` Part divider before citation H3 | `[Section header: ... organizes requirements in WAC ...` |
| **D-CFR: federal incorporation** | Authority bracket cites `\d+ C.F.R. ...` | `[Section ... adopts federal regulatory standards by reference. ... at the citation(s) listed: ...` |
| **D-external: external standard** | No CFR cite; substantive caption + empty body (incorporates ANSI/ASME/ICC) | `[Section ... exists as a citation slot in the certified WAC publication. Operative content (where present) is incorporated by reference from external industry or model-code standards ...` |
| **Reserved-empty** | Caption matches `\bReserved\b` | (no placeholder; `is_reserved=TRUE`, ghost row) |

When retrieving these, the agent should:
- For A: fetch the image URL embedded in the placeholder if visual content is needed
- For B1: navigate to the subsection cluster (the heading's child sections)
- For D-CFR: redirect to the cited federal CFR provision (NOT in this corpus; agent fetches from an external CFR source if available)
- For D-external: surface the external-standards citation; the operative text is not in WA's certified publication
- For Reserved: return as-is; no operative text to provide

Class corpus-wide counts post-ingest:

```
class_a_image:           30
class_b1_partheading:    41
class_d_cfr:             26
class_d_external:       124
reserved_ghosts:        284
```

### §P.4 Citation lookup (Pattern (b) — deterministic, MUST PASS)

```sql
SELECT canonical_citation, section_heading, full_text,
       is_repealed, is_recodified, is_decodified, is_expired, is_reserved,
       recodified_to,
       wsr_register_number, effective_date
FROM wac.sections
WHERE canonical_citation = $1;
```

Fully qualified citation is the lookup key. Pattern (b) failures halt — citation lookup must always work for an extant WAC citation.

### §P.5 Tag-filtered popular-name lookup (Pattern (c))

```sql
SELECT s.canonical_citation, s.section_heading
FROM wac.sections s
JOIN core.document_tags dt ON dt.document_id = s.document_id
JOIN core.tags t ON t.id = dt.tag_id
WHERE t.slug = $1   -- e.g., 'wlad-enforcement-wac', 'bii-regulations-wac'
  AND NOT s.is_repealed
ORDER BY s.canonical_citation;
```

Topical tags applied during Phase 4 ingest:
- `wlad-enforcement-wac` (Title 162)
- `bii-regulations-wac` (Title 296)
- `dshs-wac` (Title 388)
- `dcyf-wac` (Title 110)
- `hca-wac` (Title 182)
- `doh-wac` (Title 246)
- `civil-service-wac` (Title 357)
- `employment-security-wac` (Title 192)
- `licensing-wac` (Title 308)
- `revenue-wac` (Title 458)
- `pra-regulations-wac` (cross-cutting; applied via body-text rule)

### §P.6 Semantic search (Pattern (a))

Standard `rag_query()` pattern via `core.document_chunks.embedding`. Default `code_jurisdiction_filter=None` returns mixed federal-usc + state-wa-rcw + state-wa-wac results — federal/state/regulatory triple where applicable. Pass `code_jurisdiction_filter='state-wa-wac'` to scope to WAC only.

### §P.7 WSR (Washington State Register) integration

`wac.sections.wsr_register_number` holds the most-recent revision's WSR cite (e.g., `WSR 21-11-083`). Full revision history (one row per WSR event) is in `wac.section_history`:

```sql
SELECT wsr_register, register_year, filed_date, effective_date, action_type
FROM wac.section_history sh
JOIN wac.sections s ON s.id = sh.section_id
WHERE s.canonical_citation = $1
ORDER BY sh.sort_order;   -- sort_order=1 is most recent
```

WSR documents themselves are NOT in this corpus (Phase 8+ if needed). The string is a navigation hint to lawfilesext.leg.wa.gov.

---

## §Q. Authority-chain queries (WAC → RCW)

The doctrinally-critical retrieval direction is "show me all WAC sections promulgated under RCW X.YY". Phase 1 schema includes `wac.authority_chain` (Option B from Phase 0 design):

```sql
CREATE TABLE wac.authority_chain (
    id                UUID PRIMARY KEY,
    wac_section_id    UUID REFERENCES wac.sections(id),
    authorizing_rcw   TEXT,                         -- 'RCW 49.60.120(3)' | 'RCW 49.04' | '2020 c 277'
    rcw_section_id    UUID REFERENCES rcw.sections(id), -- NULL until resolved
    authority_text    TEXT,
    is_chapter_level  BOOLEAN,
    is_session_law    BOOLEAN,
    sort_order        INTEGER
);
```

Post-ingest resolution rate corpus-wide: **95.51%** (83,752 / 87,689 section-level).

### §Q.1 Reverse-authority query (the doctrinal use case)

```sql
-- All WAC sections promulgated under any RCW 49.60 provision (WLAD authority chain)
SELECT s.canonical_citation, s.section_heading, ac.authorizing_rcw
FROM wac.sections s
JOIN wac.authority_chain ac ON ac.wac_section_id = s.id
WHERE ac.authorizing_rcw LIKE 'RCW 49.60%'
  AND NOT s.is_repealed AND NOT s.is_recodified
ORDER BY s.canonical_citation;
```

Corpus stats:
- `LIKE 'RCW 49.60%'` (WLAD): 200 hits
- `LIKE 'RCW 51%'` (BIIA / industrial insurance): 2,317 hits
- `LIKE 'RCW 49.17%'` (workplace safety): 11,343 hits in T296 alone
- `LIKE 'RCW 74.13%'` (child welfare): 138 hits
- `LIKE 'RCW 26.44%'` (child abuse mandatory reporting): 4 hits — doctrinally accurate (statute is mostly self-executing)

### §Q.2 Forward-authority query (drafting use case)

```sql
-- For a given WAC section, list authorizing RCW citations + resolved RCW text
SELECT ac.authorizing_rcw,
       ac.is_chapter_level,
       ac.is_session_law,
       rs.canonical_citation AS resolved_rcw,
       rs.section_heading,
       rs.full_text
FROM wac.authority_chain ac
LEFT JOIN rcw.sections rs ON rs.id = ac.rcw_section_id
WHERE ac.wac_section_id = (SELECT id FROM wac.sections WHERE canonical_citation = $1)
ORDER BY ac.sort_order;
```

When `ac.rcw_section_id IS NULL`:
- `is_chapter_level=TRUE`: query at chapter level via `LIKE 'RCW {chapter}%'` to get the actual section list
- `is_session_law=TRUE`: navigate to the session-law cite outside this corpus
- Both FALSE: cite is unresolved (~5% of section-level rows; targets repealed RCW sections, typos, or pre-codification cites)

### §Q.3 Ultra-vires audit query (doctrinal — illustrative)

```sql
-- WAC sections whose authorizing RCW was amended after the WAC's last revision
WITH wac_last_revision AS (
  SELECT s.id AS section_id, MAX(sh.effective_date) AS last_wac_revision
  FROM wac.sections s
  LEFT JOIN wac.section_history sh ON sh.section_id = s.id
  GROUP BY s.id
)
SELECT s.canonical_citation,
       wlr.last_wac_revision,
       rs.canonical_citation   AS authorizing_rcw,
       rsh.session_year         AS rcw_amended_year
FROM wac.sections s
JOIN wac_last_revision wlr ON wlr.section_id = s.id
JOIN wac.authority_chain ac ON ac.wac_section_id = s.id
JOIN rcw.sections rs ON rs.id = ac.rcw_section_id
JOIN rcw.section_history rsh ON rsh.section_id = rs.id
WHERE rsh.session_year::int * 365.25 + 0
       > EXTRACT(EPOCH FROM wlr.last_wac_revision) / 86400.0 / 365.25
  AND NOT s.is_repealed
ORDER BY s.canonical_citation;
```

(Refine date arithmetic per `wac.section_history.effective_date` semantics. This identifies WAC sections potentially out-of-step with their authorizing RCW.)

### §Q.4 Authority-chain via cross-references (FYI)

`wac.cross_references` does NOT hold authority bracket cites — those go to `wac.authority_chain`. RCW citations in operative section body text DO appear in `wac.cross_references` with `target_jurisdiction='state-wa-rcw'` and `ref_type='reference'`. This dual storage is the §K #9 scope-discriminator pattern.

---

## §R. Three-jurisdiction queries

After Phase 4, the ACE corpus carries three code-jurisdiction values:
- `federal-usc` (USC priority core: Titles 5, 28, 28A, 29, 42)
- `state-wa-rcw` (RCW full corpus: 100 titles, 51,764 sections)
- `state-wa-wac` (WAC full corpus: 176 titles, 51,204 active sections)

### §R.1 Three-jurisdiction unified semantic query

Default `rag_query()` returns a mix when called without `code_jurisdiction_filter`. Verified by Phase 5 Check 3c probing the `RCW 49.60.180` (WLAD employment) embedding against the full corpus — top-15:

```
8 RCW results (49.60 cluster — WLAD provisions)
5 WAC results (162-22-025, 162-36-005, 162-22-035, 392-190-0591, 162-16-250 — WLAD enforcement regs)
2 USC results (29 USC 623 ADEA, 42 USC 2000e-2 Title VII)
```

This is the canonical three-jurisdiction shape: agent gets the federal anchor + state statute + state regulation in a single retrieval.

### §R.2 Doctrinal triple — when to surface all three

| Federal anchor | RCW anchor | WAC anchor | Doctrinal pattern |
|---|---|---|---|
| 42 U.S.C. § 12112 (ADA Title I employment) | RCW 49.60.180 (WLAD employment) | WAC 162-22-* (HRC handicapped employment regs) | Disability employment discrimination — federal floor + state expansion + state enforcement |
| 5 U.S.C. § 552 (FOIA) | RCW 42.56 (PRA) | (per-agency PRA WACs) | Records access — federal + state + agency-specific procedures |
| Workers' comp federal LHWCA/FECA | RCW 51 (industrial insurance) | WAC 296-* (L&I implementation) | Workers' comp — federal collateral + state primary + state regs |
| 18 U.S.C. § 1961 (federal RICO) | RCW 9A.82 (state criminal profiteering) | (none — criminal predicates) | Predicate-act analysis |
| 45 C.F.R. § 1340 (federal CAPTA) | RCW 26.44 (mandatory reporting) | WAC 110-30 (DCYF CPS procedures) | Child-abuse reporting + investigation |

When researching any of these patterns, the agent should retrieve from all three jurisdictions and synthesize. The corpus default of `code_jurisdiction_filter=None` makes this happen automatically.

### §R.3 Single-jurisdiction filter (when scope is intentional)

```python
rag_query("disability discrimination state and federal employment",
          code_jurisdiction_filter='state-wa-wac', top_k=10)
```

Use when:
- Drafting WAC-only research (administrative challenge to a regulation)
- Scoping a query intentionally to one corpus

Use `code_jurisdiction_filter=None` (default) when:
- Research questions ("what authority do I have for X?")
- Frameworks ("how does WA implement federal anti-discrimination law?")
- Most user queries — federal/state/regulatory pair is usually the right answer

### §R.4 Cross-reference traversal across jurisdictions

`wac.cross_references` distribution (post-ingest):
- 52,152 to `state-wa-wac` (WAC self-references)
- 36,610 to `state-wa-rcw` (WAC body cites RCW)
- 199 to `cfr` (WAC body cites federal CFR, distinct from authority-bracket CFR cites)
- 105 to `federal-usc` (WAC body cites USC)
- 16 to `wa-constitution`

Inbound query (which WAC sections cite this RCW?):

```sql
SELECT s.canonical_citation, x.context_snippet
FROM wac.cross_references x
JOIN wac.sections s ON s.id = x.source_section_id
WHERE x.target_citation = $1
  AND x.target_jurisdiction = 'state-wa-rcw';
```

This complements `wac.authority_chain` for the body-text route: authority chain for "promulgated under" relationship; cross_references for "operative text references" relationship.

---

## §K corroborations — full list (1–13)

Phase 6 §K (audit-predicate diversification): when audit predicates and production predicates share the same logic, both fail in the same way and the audit silently confirms the bug. Resolution: schema-level CHECK constraints serve as primary audit channels structurally independent from parser/audit predicates.

### Corroborations #1–7 (USC + RCW projects, summarized for completeness)

- **§K #1** (USC project): regex-based parser audit predicate matched the same shape as the parser's emit predicate; both missed the same bug. Schema CHECK constraint at INSERT was the structurally-different audit channel.
- **§K #2** (USC): tag-equality audit ran on materialized tag-id values that the parser also wrote; tag-id matched but slug semantics drifted. SLUG-based audit (independent table) caught the drift.
- **§K #3** (USC): chunk-count audit checked existence in `core.document_chunks` but parser's emit was conditional on `full_text != ''`. Empty-body sections produced 0-chunk records that satisfied count audit but broke retrieval. CHECK on (full_text NOT NULL XOR ghost flag) caught it.
- **§K #4** (RCW): UCC dash-format anchors (`62A.1-101`) were caught by anchor regex but mis-parsed by the dot-form section-number extractor. Schema-level CHECK on canonical_citation format caught the divergence.
- **§K #5** (RCW): synthetic part-heading anchors (e.g., `42.17A.89999`) matched section-anchor regex but had no caption/body/credit. Multi-criterion structural test (citation H3 AND caption H3 AND body OR credit) was structurally-different from anchor regex; caught the false positives.
- **§K #6** (RCW): `chk_pending_recodification_pair` defensive paired-NULL CHECK caught a parser bug where one half of the pending pair was set without the other.
- **§K #7** (RCW): the original draft of `wac` short-form in `rcw.cross_references.chk_target_jurisdiction` survived into Phase 1 because the schema-design test only verified the constraint COMPILED, not that it matched slug values. Operator caught at code-review during WAC Phase 1 (harmonization).

### Corroborations #8–13 (added during WAC ingestion project, 2026-04-30)

- **§K #8** (LLM summarization vs programmatic count): for inventories/counts, parse the cached source programmatically; LLM summaries silently truncate. Phase 0 reported 116 titles via WebFetch summarization; programmatic regex over the cached HTML found 228. Halt protocol triggered correctly.
- **§K #9** (cross-reference scope discriminator): RCW citations in WAC body text → `cross_references`; same RCW citations in `[Statutory Authority: ...]` brackets → `authority_chain`. DOM-position-based routing (structurally different from regex match alone) catches parser misroutes.
- **§K #10** (acquisition-time ≠ parse-time classification): Phase 2 acquisition WARN was over-broad (matched any "Repealed by" text); Phase 3 parse-time multi-criterion classification (H1 + table + section-anchor count) was definitive. Don't treat acquisition-time WARN as final classification.
- **§K #11** (CHECK constraint catches silent parser failure): `chk_section_text_status_invariant` rejected 454 active sections with empty `full_text`. Phase 3 verification counted but didn't INSERT, so the bug survived verification. Schema-level CHECK at INSERT is the structurally-different audit channel.
- **§K #12** (sample-first gate axes): when proposing a sample-first gate for an extraction strategy, the gate criterion should cover both extraction QUALITY (garbled vs clean) AND source COVERAGE (content present vs absent). My Class D PDF gate covered only quality; the actual failure mode was empty source PDFs.
- **§K #13** (silent fallbacks defeat halt-on-error): `embedding_service.py` has HF→CPU fallback. With a URL bug producing 404 from HF, all batches fell back to CPU silently — Title 162 ran on CPU for 5 min before the URL bug surfaced. Configure fallback paths to log loudly (WARN-level) and increment a counter that downstream halt logic can monitor.

### §K #31 — Coordinator scope assumption vs schema-enforced constraint reality (added during Phase 4.4 FK resolution, 2026-05-01)

When operator-drafted kickoffs describe operations across schema objects, the kickoff implicitly asserts that the schema permits those operations. Agents must verify the schema's actual constraints (FKs, CHECKs, column types) BEFORE drafting investigation tasks or execution scripts. Generalizes #24 (constraint-aware operator instructions), applied to architectural FK constraints rather than CHECK constraints.

**Why:** WAC Phase 4.4 kickoff scoped resolution across 4 jurisdictions (state-wa-wac self + state-wa-rcw + federal-usc + cfr/wa-constitution out-of-scope). The kickoff implicitly assumed `wac.cross_references.target_section_id` could hold any section_id from any corpus — but the actual FK declaration was `target_section_id REFERENCES wac.sections(id)` only, blocking cross-corpus FK resolution at the schema level. RCW and USC have the same constraint pattern in their cross_references tables — meaning USC's prior 94.58% Phase 4.4 was internal-only by architectural necessity, not just by scope choice. This is a structural property of all three corpora's Phase 1 schemas, not a per-project choice.

**How to apply (Task 0 for Phase 4.4 mirror kickoffs):**
- Before drafting investigation tasks for a Phase 4.4-style FK resolution, run a Task 0 schema-introspection check: `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='target_table'::regclass AND contype='f'`. Surface the FK targets explicitly in the investigation report.
- If the schema's FK targets don't match the kickoff's described scope, halt-and-surface BEFORE drafting execution. Three resolution paths: (a) restrict scope to schema-permitted operations, (b) additive schema change to enable cross-corpus FK targets, (c) destructive schema change (drop FK or polymorphic UUID).
- The operator's kickoff describes desired *operations*; the schema enforces *what's permitted*. Agents bridge that gap by verifying alignment and surfacing mismatches before execution.

This is §K corroboration #31 (was 30 at session start). Aligned with prior #24 (constraint-aware operator instructions); generalizes from CHECK constraints to FK constraints.

### §H2 elaboration — criterion-vs-corpus failure modes (NEW)

When a Pattern (b) deterministic test passes but a Pattern (a)/(c) criterion fails by a wide margin, **investigate the doctrinal basis of the criterion BEFORE treating it as corpus failure**. The criterion may have been wrong; the corpus may be correctly modeling reality. Halt-and-investigate doctrine first.

Example from this project: RCW 26.44 authority-chain count = 4. Initial criterion was "> 10". Investigation showed the corpus correctly captures WAC's regulatory structure under RCW 26.44 — the statute is mostly self-executing (mandatory-reporting duties on teachers/doctors), and DCYF's regulatory framework lives under RCW 74.13 (child welfare programs) instead. Doctrinal-PASS / criterion-FAIL.

---

## End of WAC retrieval guide notes
