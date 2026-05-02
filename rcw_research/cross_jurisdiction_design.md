# Cross-Jurisdiction Retrieval Architecture

**Phase 0 deliverable.** Author: Athena RCW-Ingest agent. Date: 2026-04-28.
Companion to `rcw_source_findings.md`. Specifies how the new RCW corpus interoperates
with the existing USC corpus inside a single ACE retrieval surface.

---

## 1. The decision in one paragraph

ACE will host both corpora in the existing `core.documents` / `core.document_chunks` tables. Each chunk carries a **jurisdiction tag** that lets retrieval scope to one corpus, the other, or both. The default for every retrieval call is **all jurisdictions** — most user queries don't pre-specify federal vs state, and the right answer is often a federal/state pair (ADA Title I + WLAD; FOIA + PRA; FRCP analogues + WA Civil Procedure). When the caller knows the scope, they pass an explicit `jurisdiction_filter` kwarg and retrieval narrows. The mechanism is a thin extension to `canary_lib.hybrid_rrf` that piggybacks on the existing `tag_filter` infrastructure, so no new index work is required and the existing v3 strict-order optimization is preserved.

---

## 2. Tagging model

### 2.1 New tag category: `jurisdiction`

```
jurisdiction:federal-usc    # USC corpus (existing, retroactively tagged)
jurisdiction:state-wa-rcw   # WA RCW (new in Phase 4)
jurisdiction:wac            # Phase 8 backlog
jurisdiction:wa-court-rule  # Phase 8 backlog (GR/CR/CrR/ER/RAP/etc.)
jurisdiction:cfr            # placeholder (Phase 8+ if regulations come in)
```

Inserted into `core.tags` once during Phase 1 schema apply, alongside the existing `usc-meta`, `usc-title`, `legal-framework` categories.

### 2.2 Retroactive USC tagging

Every existing USC chunk gets `jurisdiction:federal-usc` added to its `tags` array via a one-time UPDATE in the same Phase 1 migration that adds the new tag rows. Verified in Phase 1 by:

```sql
SELECT COUNT(*) FROM core.documents
WHERE document_type = 'statute'
  AND domain = 'legal'
  AND 'jurisdiction:federal-usc' = ANY(tags)
  AND title ILIKE '%U.S.C.%';
```

Count must match the existing USC section count from prior canary work. If not, halt.

### 2.3 RCW tagging at ingest

Each RCW chunk inserted in Phase 4 carries (at minimum):

```
jurisdiction:state-wa-rcw
rcw
state-statute
rcw-title-{N}                 # e.g. rcw-title-42, rcw-title-9A
rcw-chapter-{N.NN}            # e.g. rcw-chapter-42.56
{topical tags from §3.3 of kickoff}   # e.g. pra, wlad, bii, oma
```

Pre-seeded in Phase 1 alongside the schema apply.

---

## 3. Retrieval surface

### 3.1 New `jurisdiction_filter` kwarg

`canary_lib.hybrid_rrf` gains a single new optional parameter, parallel to the existing `tag_filter` and `doc_type_filter`:

```python
def hybrid_rrf(
    query_text: str,
    *,
    domain: str | None = None,
    doc_type_filter: str | None = None,
    tag_filter: list[str] | None = None,
    jurisdiction_filter: str | list[str] | None = None,   # NEW
    top_k: int = 10,
    iterative_scan: str | None = None,
    ef_search: int | None = None,
) -> list[Result]:
    ...
```

Semantics:

- `jurisdiction_filter=None` (default) — no filter; both jurisdictions returned, ranked together by RRF score
- `jurisdiction_filter='state-wa-rcw'` — RCW only
- `jurisdiction_filter='federal-usc'` — USC only
- `jurisdiction_filter=['state-wa-rcw', 'federal-usc']` — explicit both (same as None, but documents intent at call site)

Implementation: convert `jurisdiction_filter` into one or more `jurisdiction:{value}` tags and inject them into the same array-overlap predicate used by `tag_filter`. Single SQL clause:

```sql
WHERE (... existing predicates ...)
  AND tags && ARRAY['jurisdiction:state-wa-rcw', 'jurisdiction:federal-usc']::text[]
  -- or, when filter is a single value:
  AND 'jurisdiction:state-wa-rcw' = ANY(tags)
```

GIN index on `tags` already exists (USC uses it). No new index needed.

### 3.2 `rag_query` (the agent-visible MCP tool) defaults

`rag_query` is the tool Athena calls to do retrieval-augmented research. It currently accepts `domain`, `doc_type_filter`, `tag_filter`, `top_k`. Add `jurisdiction_filter` plumbed straight through to `hybrid_rrf`. **Default: None (all jurisdictions).** This matters: most user queries are framed without explicit jurisdiction, and the right answer is usually a federal+state pair. Forcing the caller to opt-in to cross-jurisdiction retrieval would silently bias outputs toward whichever corpus the caller defaults to (likely federal in habit). Default-all forces parity.

### 3.3 Composition with existing filters

`jurisdiction_filter`, `tag_filter`, `doc_type_filter`, and `domain` compose with `AND`. Example:

```python
# All employment-discrimination statutes (federal + state) of doc_type='statute' in legal domain
hybrid_rrf("employment discrimination",
           domain='legal',
           doc_type_filter='statute',
           tag_filter=['employment-discrimination', 'wlad'],   # topical OR within tag_filter
           jurisdiction_filter=None)                            # both jurisdictions
```

The `tag_filter` continues to mean "any of these tags" (OR); `jurisdiction_filter` is a separate axis that ANDs with tag_filter.

### 3.4 Backwards compatibility

Existing call sites that don't pass `jurisdiction_filter` get default-None, which is a no-op compared to current behavior pre-RCW. After Phase 4 lands, the *output* of those call sites changes (RCW results may rank into top-k), which is the intended behavior. No call-site changes required, but the Phase 6 retrieval-guide update should call out the new behavior so agents understand why state results started appearing in their queries.

---

## 4. Retrieval-pattern guidance (Phase 6 §M draft)

### 4.1 When to pass `jurisdiction_filter`

Pass it only when the question is *intrinsically* one-jurisdiction:

- "What's the federal statute of limitations for §1983 claims?" → `jurisdiction_filter='federal-usc'`
- "What is RCW 4.16.080?" → `jurisdiction_filter='state-wa-rcw'` (citation lookup, the citation form already names the jurisdiction)
- Drafting a federal-court complaint and need only the federal claim's statutory anchor → `'federal-usc'`
- Drafting a WA-state-court motion and need only the state procedural rule → `'state-wa-rcw'`

In all other cases — research questions, frameworks, "what authority do I have for X" — leave it None. The federal/state pair is usually informative.

### 4.2 The two-test cross-jurisdiction canary

Phase 5 must verify cross-jurisdiction retrieval works before declaring the priority-core ingest valid. Two mandatory tests, both run with `jurisdiction_filter=None`, `domain='legal'`, `top_k=10`:

**Test A — discrimination:**

```python
rag_query("disability discrimination state and federal employment",
          domain='legal', top_k=10)
```

PASS criterion: top-10 results MUST include both
- 42 U.S.C. § 12112 (federal ADA Title I employment) — `jurisdiction:federal-usc`
- RCW 49.60.180 (WLAD employment discrimination) — `jurisdiction:state-wa-rcw`

If only federal or only state results appear in top-10, retrieval is broken at the cross-jurisdiction layer. Diagnose before further ingest: most likely cause is an unbalanced relevance score where one corpus dominates due to chunk-volume asymmetry (USC has ~65k sections, RCW will have ~45k). Mitigations to try in order: (1) verify both corpora got the query as `jurisdiction:` tags applied; (2) raise top_k to 30 and confirm both appear within 30 — if so, the issue is RRF ranking; (3) consider a per-jurisdiction reservation in the result blending step (e.g., guarantee at least 3 of each in top-10 for `jurisdiction_filter=None` calls — costs some RRF purity but ensures cross-jurisdiction coverage).

**Test B — records access (federal/state parallel):**

```python
rag_query("public records request response time judicial review",
          domain='legal', top_k=10)
```

PASS criterion: top-10 MUST include
- RCW 42.56.520 (PRA: prompt response required)
- RCW 42.56.550 (PRA: judicial review of agency action)

PASS+: ideally 5 U.S.C. § 552 (FOIA) also appears. If FOIA is missing but the two RCW anchors are present, that's a soft pass — the federal/state parallel here is doctrinally important to records-access work but FOIA's response-time provisions rank-compete with state versions on similar query terms, so its absence is a known cross-jurisdiction symmetry risk rather than a hard failure.

If Test B fails entirely (neither RCW anchor in top-10 with `jurisdiction_filter=None`), retrieval is broken — same diagnosis path as Test A.

### 4.3 When to pre-resolve slugs

Per USC canary v3, popular-name retrieval (Pattern (c)) requires `tag_filter` + `iterative_scan='strict_order'` and **never** uses `LOWER()` wrappers in SQL (defeats the planner). When the popular-name slug is jurisdiction-specific (e.g., `wlad` is a WA-specific tag, not a federal one), `jurisdiction_filter` is redundant — passing the WLAD slug already constrains to RCW. But for cross-jurisdiction popular names (rare, e.g., "civil rights" tag applies to both), pass both `jurisdiction_filter='federal-usc'` (or 'state-wa-rcw') and the slug to disambiguate.

### 4.4 The "Formerly RCW X.YY.ZZZ" lookup

Recodification chains (RCW 42.17.320 → RCW 42.56.520 in 2005) are common in WA. When an agent encounters a citation in older case law or pleadings that no longer exists in the current RCW, the lookup pattern is:

1. SQL exact-match against `rcw.sections.full_citation` for the old citation. If `is_recodified=TRUE`, follow `recodified_to`.
2. If exact match fails (very old recodifications), search `rcw.cross_references` for `target_citation = '{old citation}'` with `ref_type='recodification'` to find the new citation.
3. Then re-query with the new citation for full text.

Phase 6 should document this as the canonical recodification-resolution pattern.

---

## 5. Implementation checklist (for Phase 1 schema apply + Phase 4 ingest)

- [ ] Phase 1 schema migration adds `jurisdiction:federal-usc`, `jurisdiction:state-wa-rcw`, `jurisdiction:wac`, `jurisdiction:wa-court-rule`, `jurisdiction:cfr` rows to `core.tags`.
- [ ] Phase 1 same migration runs `UPDATE core.documents SET tags = array_append(tags, 'jurisdiction:federal-usc') WHERE …` for every existing USC chunk. Idempotent (skip if tag already present).
- [ ] Phase 1 verification SQL: count chunks tagged `jurisdiction:federal-usc` matches USC chunk-count from prior canary baseline.
- [ ] Phase 4 RCW ingest writes `jurisdiction:state-wa-rcw` plus `rcw`, `state-statute`, `rcw-title-{N}`, `rcw-chapter-{N.NN}`, and applicable topical tags into every chunk.
- [ ] `canary_lib.hybrid_rrf` extended with `jurisdiction_filter` kwarg. Default None. Composes AND with existing filters via the same `tags && ARRAY[...]` predicate path used by `tag_filter`.
- [ ] `rag_query` MCP tool surface adds `jurisdiction_filter` passthrough. Default None.
- [ ] Phase 5 canary suite runs Test A (disability employment) and Test B (records access) on the priority-core after Title 49 lands. Both tests block further ingest until they pass.
- [ ] Phase 6 retrieval-guide update adds new §M (cross-jurisdiction) and updates §H/§L examples to show jurisdiction usage.

---

## 6. Edge cases and known risks

1. **Result-volume asymmetry**: USC and RCW differ in chunk volume (~65k vs ~45k). RRF blending with default `top_k=10` may favor one corpus due to per-corpus density alone, regardless of relevance. Mitigation deferred to Test A diagnosis if it surfaces.
2. **Citation-form ambiguity**: "section 12112" alone is ambiguous (federal or state? RCW 42.56.030 has internal subsections numbered (1), (2), etc.). Citation-lookup pattern (b) requires exact-match on `full_citation` which is `RCW T.CC.SSS` or `42 U.S.C. § N` — fully qualified. If callers pass partial citations, return empty rather than guessing.
3. **Cross-jurisdiction false matches**: a query like "civil procedure rule 12" might match FRCP 12, RCW 4.32 (civil procedure pleadings), and CR 12 (WA court rule, OUT OF SCOPE Phase 8). Until Phase 8, court-rule queries are deliberately not satisfiable from this corpus — leave for the operator to detect from result absence.
4. **Recodified-out ghost rows in RRF**: if `rcw.sections` ghost rows have `full_text=NULL` and no embeddings, they cannot rank into RRF. Good — that's the design. They surface only via citation lookup (pattern (b)) which goes through `rcw.sections.full_citation` directly, not through `core.document_chunks`.
5. **Federal-state preemption queries**: questions like "is RCW 49.60.180 preempted by Title VII?" need both citations, but the *answer* depends on case law (which is not in this corpus). Phase 6 §M should explicitly flag that this corpus is statutory-only — preemption analysis still needs case-law sources. The corpus surfaces the parallel statutes; reasoning about which controls is the agent's job.

---

## 7. Default for `rag_query`: confirmed

After tracing the surface end-to-end, the default-all-jurisdictions choice is the right one because:

- it preserves existing USC-query behavior (no breakage; USC still ranks)
- it surfaces the cross-jurisdiction federal/state pair when it exists
- explicit single-jurisdiction queries cost just one kwarg (no opt-out friction)
- the alternative (default to "federal-usc" or detect-from-context) silently introduces bias and surprises users when state law is the right answer

End of cross-jurisdiction design.
