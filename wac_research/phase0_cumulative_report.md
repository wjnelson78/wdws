# WAC Phase 0 — Cumulative Report

> **⚠ Phase 2 Correction (2026-04-30 / Phase 2 coverage-map step):**
>
> Phase 0 reported "116 populated WAC titles" via WebFetch summarization. Programmatic regex audit during Phase 2 coverage-map step found **228 distinct cite values** (197 bare numeric + 31 lettered: 132A–N+P–Z, 249A, 495A–E). The 116 was an LLM-summarization artifact; **228 is the authoritative count**.
>
> Phase 4 Stage 2 wall-time estimate corrected from ~20 hours (Phase 0) and ~33 hours (intermediate) to **~1.4 hours** based on empirical sampling 2026-04-30: WAC mean chunks/section = 1.43 (N=76 sections sampled across 3 chapters; vs RCW empirical 1.04). With HF .187 stable at ~13 chunks/sec aggregate, ~67,000 chunks = ~85 min wall-time.
>
> Phase 2 fetch wall-time corrected to ~66 min (228 titles + ~3,550 chapters at 1 req/sec).
>
> Operative numbers in §3.1, §3.2, §3.3, §6.1, §7 below are SUPERSEDED by these corrections; original Phase 0 numbers preserved in this document for audit-trail purposes per Phase 6 §K methodology corroboration #8 (LLM summarization vs programmatic extraction; counts/inventories require programmatic verification, not LLM paraphrase).

---

**Status:** PHASE 0 COMPLETE; awaiting operator authorization for Phase 1 schema design.
**Author:** Athena WAC-Ingest agent.
**Date:** 2026-04-30.
**Operator:** William J. Nelson.

This report is the Phase 0 closeout per kickoff §7. Three deliverables produced (re-validated from preliminary draft); two halt-trigger checks performed; one off-axis anomaly surfaced for operator resolution.

---

## 1. Deliverables

| Document | Status | Path |
|---|---|---|
| Source-format findings | revised | [wac_research/wac_source_findings.md](wac_source_findings.md) |
| Authority-chain design | revised | [wac_research/authority_chain_design.md](authority_chain_design.md) |
| Litigation-priority addendum | revised | [wac_research/litigation_priority_addendum.md](litigation_priority_addendum.md) |

The three preliminary versions on disk before formal authorization (written 2026-04-30 prematurely) were re-validated against the locked-in kickoff parameters and the RCW Phase 1 reference assets (`schema_design.sql`, `schema_amendment_pending_recodification.sql`, `rcw_parser.py`, `phase6_retrieval_guide_notes.md`). All three are now revised.

---

## 2. Source format — confirmed findings

### 2.1 Endpoints

- `app.leg.wa.gov/wac/` landing page lists 116 populated WAC titles.
- Per-chapter `&full=true` URL pattern `default.aspx?cite=T-CC&full=true` returns full chapter text + statutory authority brackets + revision history + **inline disposition table**.
- Last update marker: `April 1, 2026` → publication_date for all Phase 4 sections.
- Archive at `lawfilesext.leg.wa.gov/law/WACArchive/2025/` with per-title HTM + PDF (Phase 5 fidelity dictionary).
- Justia is Cloudflare-403 (matches RCW Phase 0).

### 2.2 Section anchor format — DASHES not DOTS

`<a name='T-CC-SSS'></a>` with examples `296-15-001`, `162-16-200`, `296-14A-010`. Parser regex must use scoped alternation `[\d]+-[\d]+[A-Z]?-[\d]+`, NOT permissive substitution (carry-forward from RCW UCC dash lesson).

### 2.3 Statutory authority bracket — verbatim samples

```
[Statutory Authority: RCW 51.04.020, 51.32.190 and 2020 c 277.
 WSR 21-11-083, § 296-15-001, filed 5/18/21, effective 7/1/21.]

[Statutory Authority: RCW 49.60.120(3).
 WSR 15-24-071, § 162-16-200, filed 11/25/15, effective 12/26/15;
 WSR 99-15-025, § 162-16-200, filed 7/12/99, effective 8/12/99.]
```

Format: `[Statutory Authority: <RCW citations + session-laws>. <revision-event-1>; <revision-event-2>; ... .]`. Each revision event references a WSR (Washington State Register) issue.

### 2.4 Status-marker taxonomy

Inline disposition table at end of each chapter `&full=true` view holds:
- Repealed (with WSR cite + statutory authority — chapter-level or section-level)
- Recodified (`Recodified as WAC T-CC-SSS`)
- Decodified (`This section has been decodified as it was omitted from "..." filed M/D/YY`)
- Reserved (rare; section number reserved)
- Expired (anticipated; not observed in samples)

All status flags map to `wac.sections` boolean columns inherited from RCW pattern: `is_repealed`, `is_recodified`, `is_decodified`, `is_expired`, `is_reserved`. CHECK constraint `chk_section_text_status_invariant` enforces "active ⇔ has text" invariant at INSERT time (Phase 6 §K methodology — load-bearing audit channel).

### 2.5 Authority-chain pattern

Every active WAC section has 1–4 RCW authorities in its bracket. Three flavor variants:
- Section-level RCW (e.g., `RCW 49.60.120(3)`) — most common
- Chapter-level RCW (e.g., `chapters 51.04, 51.08, 51.12 RCW`)
- Session-law (e.g., `2020 c 277`, `2011 1st sp.s. c 37 § 305`)

Modeled in `wac.authority_chain` join table (operator-locked Option B) with `is_chapter_level` and `is_session_law` discriminator flags. Resolution pass populates FK to `rcw.sections.id` post-WAC-ingest.

---

## 3. Volume sanity check

### 3.1 Title census

**116 populated WAC titles** confirmed (operator-corrected from preliminary "~270" estimate). Some title-number slots hold only lettered variants (e.g., `cite=132` returns "not found" but `cite=132A` works for Peninsula College).

### 3.2 Chapter census (sampled, 17 titles)

| Title | Chapters | Title | Chapters |
|---|---|---|---|
| 4 | 1 | 192 | 44 |
| 51 | 11 | 222 | 15 |
| 110 | 28 | 230 | 16 |
| 132A | 14 | 246 | 86 |
| 162 | 14 | 296 | 71 |
| 173 | 96 | 308 | 71 |
| 182 | 41 | 314 | 30 |
| 357 | 12 | 388 | 88 |
| 458 | 21 | | |

Long-tail subset (titles 4, 51, 132A, 222, 230, 314, 357, 458) — 8 long-tail samples, 122 chapters, mean **15.3 chapters/title**.

Priority/large titles subset (10 titles, 580 chapters, mean ~58).

**Refined full-corpus chapter projection:**

- Priority core (slots 1–4): ~201 chapters (sampled directly)
- Priority-priority (slots 5–10, with corrected slot 6 = Title 182): ~275 chapters
- Long tail (~106 titles at mean 15): ~1,600 chapters
- **Total: ~2,000–2,500 chapters**

### 3.3 Section census

Sample data: 9–18 active sections per chapter; ~0.5–1.0× that count in disposition ghosts.

- Active sections: ~2,300 chapters × 13 mean = **~30,000 active sections** (in operator's locked 30,000–50,000 range)
- Ghost rows: ~15,000–30,000
- Total `wac.sections` rows: **~45,000–60,000**

### 3.4 Halt-trigger check

Volume sanity check thresholds (per kickoff §5.3 — ">2x deviation triggers halt"):

| Quantity | Estimate | Lower halt | Upper halt | Status |
|---|---|---|---|---|
| Populated titles | 116 (confirmed by direct enumeration) | n/a | n/a | **Verified** |
| Total chapters | ~2,300 | 1,150 | 4,600 | **In range; no halt** |
| Total active sections | ~30,000 | 15,000 | 60,000 | **In range; no halt** |
| Disposition ghost rows | ~22,500 (midpoint) | 11,250 | 45,000 | **In range; no halt** |

Operator's locked-in section count estimate (30,000–50,000) sits within halt thresholds. Operator's locked-in chapter estimate (~1,000–1,500) is below my refined estimate (~2,300). Both estimates are within the halt threshold of each other (< 2× deviation), so no halt fires — but the chapter count delta is worth surfacing for operator review (see §6 below).

**Phase 0 disposition: VOLUME SANITY CHECK PASS — no halt trigger fires.**

---

## 4. Architectural inheritance from USC + RCW (locked, no re-litigation)

Per kickoff §4, these are operator-decided. Phase 0 confirms the design integrates cleanly with existing infrastructure:

| Locked parameter | Status | Phase 0 finding |
|---|---|---|
| `state-wa-wac` slug in `code-jurisdiction` category | confirmed | Bare slug, no colon. Aligns with RCW Phase 1 `core.tags` model. |
| `wac.*` schema parallel to `rcw.*` (7 + authority_chain = 8 tables) | confirmed | All 5 ghost flags + pending_recodification columns inherited from RCW. WAC-specific `wsr_register_number TEXT` added to `wac.sections`. |
| Authority chain Option B | confirmed | Schema sketch from kickoff §4.3 absorbed; refined with `is_chapter_level`/`is_session_law` flags + `created_at` + `idx_wac_authority_section`. Surfaced in §6.2 below. |
| Dash citation format | confirmed | `<a name='T-CC-SSS'></a>` verified across 4 chapter samples. |
| Priority sequence (slots 1–10) | confirmed (with §6.1 anomaly) | Slot 6 reads "Title 222 (HCA)" but Title 222 = Forest Practices Board; HCA = Title 182. Halt-and-investigate item. |
| Update cadence weekly + monthly hard refresh | confirmed | WAC publishes ~twice monthly; Phase 7 cron design accommodates. |
| HF .187 stable, .113 down, FORCE_LOCAL standing fallback | confirmed | `.env` has `HF_ENDPOINT_URL` set; no `HF_ENDPOINT_URL_FALLBACK` set per operator instruction. Reranker fallback is set (different env var, different endpoint). |
| Stage 1 + Stage 2 integrated for WAC (no deferred backfill) | confirmed | Worker-pool wrapper from `phase4_stage2_backfill_parallel.py` directly applicable. |
| canary_lib `code_jurisdiction_filter` kwarg (RCW Phase 1 rename) | confirmed | EXISTS clause against `core.document_tags`; default None. |

---

## 5. Methodology principles — how they apply to WAC

Per kickoff §5, carry-forward from USC + RCW. Phase 0 confirms each principle's WAC application:

**§5.1 Audit-predicate diversification (Phase 6 §K):**
- `wac.sections` CHECK constraints catch what regex audits miss.
- `wac.authority_chain` separate from `wac.cross_references` enables a structurally-different audit (compare row counts for the same source section to detect parser misroute).
- Resolution-pass FK target is `rcw.sections.id` — gaps surface RCW-corpus-vs-parser disagreements. Documented in `authority_chain_design.md` §8.

**§5.2 EXPLAIN before narrating:**
- Phase 5 validation script (copied from `rcw_research/phase5_validation.py`, with the two known SQL bugs fixed and `--dry-run` added) must EXPLAIN every query before executing under load.

**§5.3 Halt-and-investigate:**
- Phase 0 surfaced one off-axis anomaly (§6.1 Title 222→182). Halt-and-ask honored.
- New structural classes during Phase 3 parser work → halt.
- Authority resolution rate < 70% → halt before declaring Phase 5 complete.
- HF .187 degradation during Phase 4 → halt cleanly; do NOT auto-fall-back to FORCE_LOCAL.

**§5.4 Health-check threshold calibration:**
- Phase 4 health checks must use representative WAC chapter-fetch latency baselines, not synthetic probes. Sample 10–20 chapter `&full=true` fetches before locking thresholds.

**§5.5 Pre-flight verification on tooling:**
- Phase 5 validation script `--dry-run` mode exercises every SQL query without doing substantive work. Apply before launch.
- Phase 1 schema apply uses `BEGIN/COMMIT` transaction guards + pre-flight assertion block (template at `rcw_research/schema_apply.sql`).

---

## 6. Items requiring operator decision before Phase 1

### 6.1 ⚠ HALT-AND-ASK — Title 222 in priority slot 6

Kickoff §4.5 reads:

> 6. Title 222 (Health Care Authority — parallel to RCW 70 health care)

**WAC Title 222 is the Forest Practices Board** (15 chapters; verified). The Health Care Authority is **WAC Title 182** (41 chapters; verified). The parenthetical "Health Care Authority — parallel to RCW 70 health care" describes Title 182's actual scope.

**Recommended resolution:** swap Title 182 into slot 6, drop Title 222 from priority sequence (relegate to long tail — Forest Practices is irrelevant to active docket).

**Halt:** before Phase 4 Pause-Checkpoint A (priority core ingestion) until operator confirms or corrects.

### 6.2 Authority-chain schema refinement

The kickoff §4.3 schema sketch is operator-locked. Phase 0 design (`authority_chain_design.md` §3) refines the sketch with:

- `is_chapter_level BOOLEAN NOT NULL DEFAULT FALSE` — discriminator for `chapter X.YY RCW` citations
- `is_session_law BOOLEAN NOT NULL DEFAULT FALSE` — discriminator for `YYYY c NN` citations
- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()` — audit trail
- `idx_wac_authority_section ON wac.authority_chain(wac_section_id)` — forward-query index
- `chk_authority_xor` CHECK: `NOT (is_chapter_level AND is_session_law)` — mutual exclusion

These additions don't contradict the locked sketch but elaborate it. Surface for confirmation; do not apply DDL until authorized.

### 6.3 Topical tag pre-creation in Phase 1

RCW Phase 1 pre-created 17 new `legal-framework` topical tags as part of schema apply. WAC additions proposed:
- `wlad-enforcement-wac`
- `bii-regulations-wac`
- `pra-regulations-wac`
- `dshs-wac`
- `dcyf-wac`
- `hca-wac`
- `health-wac`
- `wa-public-employment-wac`
- `wa-apa-implementation`
- `wac-procedure`

(~10 new tags). Confirm scope or trim/extend list.

### 6.4 Title 44 priority slot

Title 44 (Code Reviser / agency rulemaking) has not been chapter-sampled yet. If small (<20 chapters), include in priority block; if large (>50), defer to long tail. Sample option:

- **(a)** Sample now in Phase 0 final pass (one extra fetch — minor delay).
- **(b)** Sample in Phase 2 acquisition once priority sequence is locked.

Defaulting to (b) unless operator prefers (a).

### 6.5 Open questions from `litigation_priority_addendum.md` §9

1. Title 222 → 182 swap (above §6.1 — primary halt-and-ask)
2. Title 388 ingest approach — full-with-selective-tagging vs full-with-universal-tagging
3. Title 44 sampling timing
4. Title 208 / 284 long-tail confirmations
5. Phase 1 topical-tag pre-creation scope
6. Phase 4 priority-core wall-time estimate confirmation (~3–4 hours)

---

## 7. Estimated remaining timeline (with HF stable + Title 222→182 resolved)

Per kickoff §6 and refined volume estimates:

| Phase | Estimated wall-time | Notes |
|---|---|---|
| Phase 1 (schema design + apply) | 30–45 min | Mostly review/wait; apply is fast |
| Phase 2 (acquisition) | ~42 min | ~2,300 chapters at 1 req/sec |
| Phase 3 (parser build + tests) | 3–5 hours | Inherits from RCW parser scaffolding |
| Phase 4 priority core (slots 1–4) | ~3–4 hours | 201 chapters → ~2,500 active sections |
| Phase 4 priority block (slots 5–10) | ~3–4 hours | 275 chapters → ~3,500 active sections |
| Phase 4 long tail (~106 titles) | 6–10 hours | 1,800 chapters → ~24,000 active sections |
| Phase 4 authority resolution pass | ~30 min | ~60,000 authority rows |
| Phase 5 validation (6-check + Test E/F/G) | ~30 min | After authority resolution |
| Phase 6 docs (§P, Q, R) | 1 hour | Inherits structure from USC §M |
| **Total** | **~17–24 hours** | Across 2–3 sessions |

Within operator's kickoff §11 estimate (17–25 hours).

---

## 8. Closing posture

**Phase 0 disposition: COMPLETE.**

- Three deliverables revised on disk; volume sanity check passes; one off-axis anomaly surfaced (§6.1 Title 222→182).
- No DB writes. No schema applied. No code generated.
- All operator-locked architectural parameters confirmed compatible with empirical findings.
- Standing instruction §7 honored: HARD STOP after Phase 5; for Phase 0, HARD STOP after this report.

**Awaiting operator authorization for Phase 1 schema design**, conditioned on:

1. Resolution of §6.1 (Title 222 → 182).
2. Confirmation of §6.2 schema refinements (is_chapter_level / is_session_law / chk_authority_xor).
3. Decisions on §6.3–§6.5.

Will not stage `schema_design.sql` or apply any DDL until receipt.

End of Phase 0 cumulative report.
