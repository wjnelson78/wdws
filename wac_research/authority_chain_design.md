# WAC Authority-Chain Architecture

**Phase 0 deliverable.** Author: Athena WAC-Ingest agent. Date: 2026-04-30 (revised from preliminary).
Companion to `wac_source_findings.md`. Specifies how the regulatory-authority
chain (WAC → RCW) is captured in the schema. Authority chain modeling is **operator-locked Option B** (kickoff §4.3).

---

## 1. The decision in one paragraph

ACE uses a **dedicated `wac.authority_chain` join table** to capture the regulatory-authority chain. Each WAC section gets one row per authorizing RCW citation parsed from its `[Statutory Authority: ...]` bracket. The table stores the raw authority text, the parsed RCW citation, and a foreign key to `rcw.sections` resolved post-WAC-ingest. This supports the doctrinally-critical reverse query "show me every WAC section promulgated under RCW 49.60" with a single indexed predicate, without forcing the cross-reference table to mix two semantics. Cross-references inside section bodies (RCW citations in operative text, not authority brackets) continue to use `wac.cross_references`.

This design is operator-locked per kickoff §4.3. This document specifies the parser routing, resolution strategy, and the methodology Phase 6 §K diversification rationale.

---

## 2. Why this matters

Every WAC section is promulgated under specific RCW authority. This is the doctrinal predicate for the regulation's validity. Challenging a WAC requires demonstrating either:

1. **Ultra vires:** the regulation exceeds the authorizing statute's scope (the *Hillis* / *Dep't of Ecology* line; standard administrative-law challenge).
2. **Conflict:** the regulation conflicts with a different statute or with the same statute as later amended.
3. **Procedural defect:** the rulemaking process violated APA requirements (separately tracked via WSR history).

For (1) and (2), the agent needs both directions: which RCW authorizes which WACs, and which WACs implement a given RCW. Both directions must be efficient — agent research on a recently-amended RCW chapter (e.g., RCW 49.60 after a 2024 WLAD amendment) needs to surface every WAC implementation that may now be ultra vires.

Operator-relevant examples from the active docket:

- **Nelson v BIIA** — challenging BIIA appeal procedure regulations requires querying which WACs (Title 296 chapters 14, 15, 17 etc.) are authorized under RCW 51.04, 51.32, 51.52.
- **WLAD enforcement** — Human Rights Commission Title 162 regulations are authorized under RCW 49.60.120 (and various sub-grants). `WHERE authorizing_rcw LIKE 'RCW 49.60%'` returns the complete WLAD enforcement framework as administered.
- **Sara Murray work (RCW 26.44 + DCYF Title 110)** — child abuse mandatory reporting and CPS investigation procedures cross-cite. Authority chain captures the implementation web.

---

## 3. Schema (operator-locked)

```sql
CREATE TABLE wac.authority_chain (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    wac_section_id    UUID NOT NULL REFERENCES wac.sections(id) ON DELETE CASCADE,
    authorizing_rcw   TEXT NOT NULL,                    -- 'RCW 49.60.120(3)' or 'RCW 49.04' (chapter-level) or '2020 c 277' (session-law)
    rcw_section_id    UUID REFERENCES rcw.sections(id), -- NULL until resolved post-ingest
    authority_text    TEXT,                              -- raw bracket text from source, for audit
    is_chapter_level  BOOLEAN NOT NULL DEFAULT FALSE,    -- true for 'chapter X.YY RCW' citations
    is_session_law    BOOLEAN NOT NULL DEFAULT FALSE,    -- true for 'YYYY c NN' citations (no RCW reference)
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (wac_section_id, authorizing_rcw)
);

CREATE INDEX idx_wac_authority_rcw       ON wac.authority_chain(authorizing_rcw);
CREATE INDEX idx_wac_authority_resolved  ON wac.authority_chain(rcw_section_id) WHERE rcw_section_id IS NOT NULL;
CREATE INDEX idx_wac_authority_section   ON wac.authority_chain(wac_section_id);
```

Two flags (`is_chapter_level`, `is_session_law`) discriminate the three real-world variants beyond simple section-level RCW citations. These are added to the operator-locked schema sketch from kickoff §4.3 — they refine the original sketch without contradicting it; surface this refinement in the Phase 0 report.

**Why `wac.authority_chain` over `wac.cross_references`:** the cross-references table holds heterogeneous reference types (in-body citations, recodifications, see-also pointers). Mixing authority chain into that table forces every reverse-query consumer to filter by `ref_type='statutory-authority'`. Splitting into a dedicated table makes the doctrinal reverse query a single index range scan against `idx_wac_authority_rcw`.

**Why a dedicated table over a JSONB column on `wac.sections`:** PostgreSQL GIN-on-JSONB does not index well for prefix-LIKE queries on array elements. The reverse query "all WACs under RCW 49.60.x" is a prefix scan; B-tree on a TEXT column is the right index for that workload.

---

## 4. Parser specification — what goes where

The `[Statutory Authority: <authority-list>. <revision-event-1>; ... .]` bracket contains heterogeneous content. Phase 3 parser routing:

| Authority bracket content | Storage |
|---|---|
| Section-level RCW: `RCW 49.60.120(3)` | `wac.authority_chain`, `is_chapter_level=FALSE`, `is_session_law=FALSE`. `authorizing_rcw='RCW 49.60.120(3)'` (preserved verbatim including subsection). |
| Chapter-level RCW: `chapter 49.04 RCW`, `chapters 51.04, 51.08, 51.12 RCW` | One `wac.authority_chain` row per chapter, `is_chapter_level=TRUE`. `authorizing_rcw='RCW 49.04'`. `rcw_section_id` always NULL. |
| Title-level RCW (rare): `Title 51 RCW` | One `wac.authority_chain` row, `authorizing_rcw='RCW Title 51'`, `is_chapter_level=TRUE` |
| Session-law: `2020 c 277`, `2011 1st sp.s. c 37 § 305` | One `wac.authority_chain` row, `is_session_law=TRUE`, `authorizing_rcw='2020 c 277'`. `rcw_section_id` always NULL. |
| Federal authority (rare): `45 C.F.R. § 84.4` | `wac.cross_references` with `ref_type='authority'`, `target_jurisdiction='cfr'` (NOT `wac.authority_chain` — that table is RCW-scoped) |
| Each `WSR YY-NN-NNN, § ..., filed M/D/YY, effective M/D/YY` revision event | `wac.section_history` row (separate from authority_chain) |
| `wac.sections.wsr_register_number` | The most recent WSR (sort_order=1 in section_history) |

The `authority_text` column stores the raw bracket text up through the first period (the `<authority-list>` portion). Audit trail preservation — if the parser is ever found to have misclassified, the source-of-truth text remains.

**Parser scope discriminator (load-bearing):** A given RCW citation may appear BOTH in the authority bracket AND in operative body text. The two appearances have different semantics and different storage. Phase 3 parser must:

1. Identify the authority bracket FIRST (matched by regex on `\[Statutory Authority: ... \]`).
2. Extract its contents → route to `wac.authority_chain` (RCW citations) or `wac.cross_references` (federal cites).
3. Then process body text → all RCW citations there go to `wac.cross_references` with `ref_type='reference'`.

This avoids dual-routing the same citation. **Methodology lesson (Phase 6 §K corroboration #N+1):** a single regex that catches both bracket-RCW and body-RCW will misroute one to the wrong table. The parser must use a structurally-different scope discriminator (DOM position / text section) for each routing decision.

---

## 5. Resolution pass — populating `rcw_section_id`

Phase 4 Stage 1 inserts `wac.authority_chain` rows with `rcw_section_id=NULL`. After all WAC ingestion completes, a one-time **authority-chain resolution pass** runs:

```sql
-- Pass 1: direct match on canonical_citation
UPDATE wac.authority_chain ac
SET rcw_section_id = rs.id
FROM rcw.sections rs
WHERE ac.is_chapter_level = FALSE
  AND ac.is_session_law = FALSE
  AND rs.canonical_citation = ac.authorizing_rcw;

-- Pass 2: subsection-stripped match (for citations like 'RCW 49.60.120(3)')
UPDATE wac.authority_chain ac
SET rcw_section_id = rs.id
FROM rcw.sections rs
WHERE ac.is_chapter_level = FALSE
  AND ac.is_session_law = FALSE
  AND ac.rcw_section_id IS NULL
  AND rs.canonical_citation = regexp_replace(ac.authorizing_rcw, '\(\d+\)$', '');

-- Pass 3: recodified-out ghost match (citation points to a recodified RCW)
-- This DELIBERATELY resolves to the recodified-OUT ghost row, not the live target.
-- Application logic following recodified_to reaches the live section.
UPDATE wac.authority_chain ac
SET rcw_section_id = rs.id
FROM rcw.sections rs
WHERE ac.is_chapter_level = FALSE
  AND ac.is_session_law = FALSE
  AND ac.rcw_section_id IS NULL
  AND rs.is_recodified = TRUE
  AND rs.canonical_citation = ac.authorizing_rcw;
```

Expected resolution rate: **>80% of section-level authority rows resolve**. Unresolved class includes:

- Repealed RCWs that were authority for now-amended WACs (still cited as historical authority)
- Pre-codification or extra-RCW authority that doesn't have a `rcw.sections` counterpart
- Typos in authority brackets (rare)

**Halt threshold:** if section-level resolution rate falls below 70%, halt and audit before declaring Phase 4 complete. Possible causes: parser dropping authority brackets, regex misrouting body-RCW into authority_chain, recent RCW recodification not yet absorbed.

Resolution wall-time: ~30 minutes for ~30,000 active sections × 1–4 authority rows each ≈ ~60,000 authority rows.

Per RCW Phase 5 lesson: the resolution-pass UPDATE must be wrapped in a single explicit transaction with progress reporting. Use the worker-pool wrapper pattern from `phase4_stage2_backfill_parallel.py` for deterministic shutdown via the shared HALT_FLAG.

Final reporting (post-resolution audit):

```sql
SELECT
  COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law)                  AS section_level_total,
  COUNT(*) FILTER (WHERE NOT is_chapter_level AND NOT is_session_law AND rcw_section_id IS NOT NULL) AS section_level_resolved,
  COUNT(*) FILTER (WHERE is_chapter_level)                                              AS chapter_level_total,
  COUNT(*) FILTER (WHERE is_session_law)                                                AS session_law_total
FROM wac.authority_chain;
```

Halt if section_level_resolved / section_level_total < 0.70.

---

## 6. Query patterns enabled

### 6.1 Reverse query (the doctrinal use case)

```sql
-- Every WAC section promulgated under any RCW 49.60 provision (WLAD authority chain)
SELECT s.canonical_citation, s.section_heading
FROM wac.sections s
JOIN wac.authority_chain ac ON ac.wac_section_id = s.id
WHERE ac.authorizing_rcw LIKE 'RCW 49.60%'
  AND NOT s.is_repealed
  AND NOT s.is_recodified
ORDER BY s.canonical_citation;
```

### 6.2 Forward query (drafting use case)

```sql
-- For a given WAC section, list authorizing RCW citations + resolved RCW text
SELECT ac.authorizing_rcw,
       ac.is_chapter_level,
       ac.is_session_law,
       rs.canonical_citation AS resolved_rcw,
       rs.section_heading    AS resolved_heading,
       rs.full_text          AS resolved_text
FROM wac.authority_chain ac
LEFT JOIN rcw.sections rs ON rs.id = ac.rcw_section_id
WHERE ac.wac_section_id = '<wac-section-uuid>'
ORDER BY ac.sort_order;
```

### 6.3 Ultra-vires audit query (illustrative)

WAC sections promulgated under an RCW that has been amended after the WAC's last revision. Schema details depend on `wac.section_history` columns; finalize column names in Phase 1.

---

## 7. Edge cases and known risks

1. **Chapter-level authority resolution.** When the bracket cites `chapter 49.04 RCW`, `is_chapter_level=TRUE` and `rcw_section_id=NULL` by design. Reverse-query `WHERE authorizing_rcw LIKE 'RCW 49.04%' OR authorizing_rcw = 'RCW 49.04'` correctly captures both section-level (`RCW 49.04.020`) and chapter-level (`RCW 49.04`) entries via the LIKE-prefix.

2. **Session-law-only authority.** Sections with only session-law authority (`2020 c 277`) won't appear in WLAD/BII reverse queries. Correct — session-law authority is a one-time legislative grant, not ongoing RCW chapter authority. `is_session_law=TRUE` flag makes this visible.

3. **Subsection notation in authority.** `RCW 49.60.120(3)` parser stores the full form for fidelity; resolution Pass 2 strips `(3)` to match `rcw.sections.canonical_citation`. LIKE-prefix reverse query (`'RCW 49.60.120%'`) catches both forms.

4. **Multiple RCW citations grouped with "and".** `RCW 51.04.020, 51.32.190 and 2020 c 277` — parser splits on commas AND on " and " to extract three authorities. Conservative split logic; unit-test on representative bracket samples in Phase 3.

5. **Ghost-section authority.** Repealed/recodified WAC ghost rows MAY still have authority bracket text in the disposition entry. Decision: parse and store authority-chain rows for ghost sections too. Reverse queries filter `WHERE NOT s.is_repealed` to scope to active regs.

6. **Authority RCW that has been recodified.** Pass 3 resolves the FK to the recodified-OUT ghost row. Application logic follows `rcw.sections.recodified_to` to reach the live target. Document this in Phase 6 §Q.

7. **WAC-on-WAC authority chain.** Some WACs cite other WACs as authorizing — secondary rulemaking. Out of scope for `wac.authority_chain` (RCW-scoped). Such cross-citations land in `wac.cross_references` with `ref_type='reference'`.

8. **Preserve `authority_text` raw.** If the parser ever misroutes (e.g., a future "RCW 49.60.120 and 49.60.180" bracket has the second citation dropped), the raw text in `authority_text` allows a re-parse without re-fetching source HTML. Phase 7 maintenance also benefits — a parser update can re-run on stored authority_text without going back to the chapter URL.

---

## 8. Methodology Phase 6 §K diversification (corroboration)

This design embodies §K predicate diversification:

- **Authority chain stored separately from cross-references.** A common audit query like "did the parser correctly identify all in-bracket vs in-body RCW references?" can compare `wac.authority_chain` row count against `wac.cross_references` row count for the same source section. Two different storage paths catching the same citation = misroute. Two paths catching different citations = correct routing.
- **CHECK constraint** `chk_authority_xor` (proposed for Phase 1 inclusion):
  ```sql
  CONSTRAINT chk_authority_xor CHECK (
    NOT (is_chapter_level AND is_session_law)  -- mutually exclusive
  )
  ```
- **Authority-text preservation** as primary audit channel. Re-parsing `authority_text` via a fresh regex catches parser drift without going back to source HTML.
- **Cross-validation with RCW corpus.** Resolution pass FK target is `rcw.sections.id` — if the RCW corpus is missing an authorizing RCW that should resolve, the resolution rate drops below threshold and surfaces the gap. This is a structurally-different audit channel than the parser's regex match.

---

## 9. Implementation checklist

- [ ] Phase 1 schema-apply migration creates `wac.authority_chain` table per §3 SQL.
- [ ] Phase 1 creates indexes `idx_wac_authority_rcw`, `idx_wac_authority_resolved`, `idx_wac_authority_section`.
- [ ] Phase 1 includes `chk_authority_xor` CHECK constraint (proposed §8).
- [ ] Phase 3 parser produces three structured outputs from each authority bracket: authority-chain rows, history rows, effective-date.
- [ ] Phase 3 parser unit-tests verify routing rules in §4 (section-level RCW vs chapter-level vs session-law vs federal).
- [ ] Phase 4 Stage 1 inserts `wac.authority_chain` rows alongside `wac.sections` rows. `rcw_section_id` left NULL initially.
- [ ] Authority-chain resolution pass runs after all WAC titles are ingested. Three passes (§5): direct, subsection-stripped, recodification-fallback.
- [ ] Phase 5 validation includes authority-chain canary: count + resolution rate. **Halt threshold: <70% section-level resolution.**
- [ ] Phase 5 includes doctrinal reverse-query canary: WLAD chain (`RCW 49.60%`) returns full Title 162 enforcement set; BII chain (`RCW 51.52%`) returns BIIA appeals procedures.
- [ ] Phase 6 retrieval guide §Q documents SQL patterns from §6.1–§6.3.
- [ ] Refinements to operator-locked schema sketch (§3 — added `is_chapter_level`, `is_session_law`, `created_at`, `idx_wac_authority_section`) surfaced in Phase 0 report for confirmation; no DDL applied without authorization.

End of authority-chain design.
