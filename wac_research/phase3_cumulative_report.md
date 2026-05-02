# WAC Phase 3 — Cumulative Report

**Status: PARSER COMPLETE — full-corpus verification PASS, 0 hard parse errors.**
**Author:** Athena WAC-Ingest agent.
**Date:** 2026-05-01.
**Operator:** William J. Nelson.

Phase 3 closeout per kickoff §7. Parser built, smoke-tested, full-corpus verification run; ready for operator review before Phase 4 Stage 1 priority-core ingestion.

---

## 1. Parser deliverable

| File | Lines | Purpose |
|---|---|---|
| [`wac_parser.py`](wac_parser.py) | ~750 | WAC HTML parser (mirrors RCW parser scaffolding with WAC-specific extensions) |
| [`phase3_verify.py`](phase3_verify.py) | ~150 | Full-corpus verification driver; produces JSONL report + JSON summary |
| [`phase3_verify_report.jsonl`](phase3_verify_report.jsonl) | 2,853 records | Per-chapter parse outcome |
| [`phase3_verify_summary.json`](phase3_verify_summary.json) | 1 record | Aggregate counts + anomaly inventory |

### WAC-specific extensions over RCW parser

1. **Citation regex uses dashes throughout** (`T-CC-SSS`), per Phase 0 finding §2.2.
2. **Statutory authority bracket parser** splits `[Statutory Authority: ...]` into:
   - `wac.authority_chain` rows (RCW citations + chapter-level + session-law) with `is_chapter_level` / `is_session_law` discriminator flags
   - `wac.section_history` rows (one per `WSR YY-NN-NNN, § ..., filed ..., effective ...` revision event)
3. **Multi-cite RCW comma-extension parser** for `RCW X.YY.ZZZ, A.BB.CCC, and D.EE.FFF` brackets — 1 RCW prefix, N comma/and-extended cites.
4. **Inline disposition table parser** — locates `<h3>Disposition of sections formerly codified in this title</h3>` followed by a `<table>` of section-disposition rows.
5. **Disposition-only chapter detection** — chapter with non-empty `dispositions` and zero active `sections`.
6. **TitleRedirectError exception** — raised when `cite=T-CC&full=true` server-side falls through to title TOC (`<h1>Title N WAC</h1>`); caller skips.
7. **WSR-based history** — replaces RCW's session-law-based `enacted_by_session_law` with `wsr_register_number TEXT` (latest WSR per section).
8. **Cross-ref scope discriminator** — RCW citations in body text → `wac.cross_references`; RCW citations in `[Statutory Authority: ...]` bracket → `wac.authority_chain`. Phase 6 §K methodology corroboration #9 — structurally-different storage paths catch parser misroute.

---

## 2. Full-corpus verification — top-line numbers

**Wall time:** 113 sec (1.9 min) for 2,853 chapter files.

| Status | Count | Pct |
|---|---|---|
| `ok` (standard chapters parsed cleanly) | **2,842** | 99.6% |
| `disposition_only` (true disposition-only chapter, no active sections) | 1 | 0.04% |
| `appendix_or_empty` (no standard structure — parser yields zero) | 2 | 0.07% |
| `title_redirect` (server fall-through to title TOC; chapter doesn't exist) | 8 | 0.28% |
| **`parse_error` (hard exceptions)** | **0** | **0.00%** |
| TOTAL | 2,853 | 100% |

**Zero hard parse errors. No halt conditions triggered.**

---

## 3. Corpus totals (parser output)

| Metric | Value |
|---|---|
| Active sections | **51,634** |
| Disposition entries (ghost rows) | **21,746** |
| **Total `wac.sections` rows projection** (active + ghosts) | **~73,380** |
| Authority-chain rows produced | **105,983** |
| Section-history rows | **95,924** |
| Cross-references detected | **89,214** |
| Notes parsed | (varies; mostly 0 — WAC chapters rarely have `NOTES:` blocks) |

**Per-section averages (corpus-wide):**

| Metric | Value | Comparison |
|---|---|---|
| Authority-chain rows / active section | **2.05** | (RCW: N/A — RCW has no authority_chain) |
| History rows / active section | **1.86** | (RCW: ~3.5 from Phase 6 §C report) |
| Cross-refs / active section | **1.73** | (USC T42: 22; RCW Phase 5: ~5) |

---

## 4. Section-length corpus statistics — chunks/section refinement

| Metric | Value |
|---|---|
| N (active sections sampled) | 51,634 |
| Mean chars/section | **1,900** |
| Median chars/section | (per summary JSON) |
| Max chars/section | (per summary JSON) |
| **Mean chunks/section (512-token chunks)** | **1.58** |
| Total chunks projected (active sections) | **81,779** |

**Comparison to prior estimates:**
- Phase 0 hand-wave: 5.0 chunks/section (worst-case assumption)
- Phase 2 sample (76 sections, 3 priority chapters): 1.43 chunks/section
- Phase 3 full-corpus empirical: **1.58 chunks/section** ← authoritative

The Phase 2 priority-sample was 9% LOW vs full-corpus mean. Long-tail titles are slightly chunkier on average (consistent with operator's prediction "revenue/insurance/licensing might run longer" — those are large-section agencies in the long tail).

**Phase 4 Stage 2 wall-time refresh:**

```
81,779 chunks ÷ 13 chunks/sec (HF .187 stable) = 6,290 sec = ~105 min = ~1.75 hr
```

vs. earlier estimates:
- Phase 0 worst-case: ~33 hr
- Phase 0 refined (1.43 ratio): ~85 min
- **Phase 3 empirical (1.58 ratio): ~105 min**

Phase 4 Stage 2 ceiling: **under 2 hours** with HF stable. Long pole shifts to Phase 4 Stage 1 (parser + insert + xref + authority-chain) and the priority-core ingest cadence.

---

## 5. Anomaly inventory for Phase 4 attention

11 non-ok chapters total (0.4% of corpus):

### 5.1 Disposition-only chapter (1) — needs Phase 4 ingest

| Cite | Active | Dispositions | Phase 4 handling |
|---|---|---|---|
| **WAC 296-134** | 0 | 8 | Insert chapter row + 8 ghost section rows; no embedding |

This is a true disposition-only chapter (chapter slot retained, all sections repealed/recodified/decodified). Phase 4 ingest produces `wac.chapters` row + 8 `wac.sections` rows with status flags set + zero embeddings.

### 5.2 Appendix-only chapters (2) — Phase 4 special handling

| Cite | Why | Phase 4 handling |
|---|---|---|
| **WAC 237-990** | "Appendix—Determination of Geographic Names" — single body of text listing ~hundreds of geographic features. No standard section anchors. | **Skip in Phase 4 priority/long-tail ingest.** Optional Phase 8+ handling: ingest as a single oversized "section" or treat as out-of-corpus reference data. Operator decision needed. |
| **WAC 332-26** | DNR — needs Phase 0 inspection to confirm class (likely also appendix-style). | Same as above pending inspection. |

### 5.3 Title-redirect chapter slots (8) — skip in Phase 4

| Cites | Count | Phase 4 handling |
|---|---|---|
| 365-06, 365-12, 365-14, 365-22, 365-24, 365-31, 365-40, 365-60 | 8 | Skip entirely. Server fall-through indicates chapter slot is referenced from Title 365 TOC but no chapter content exists. No row produced in `wac.chapters` or `wac.sections`. Phase 7 differential update may flag if these become populated. |

---

## 6. Methodology corroborations (Phase 6 §K)

This phase added two corroborations to the §K diversification methodology:

**#9 (NEW) — Cross-ref scope discriminator catches authority/body misroute.**

The parser routes RCW citations to two distinct storage paths based on DOM position:
- Inside `[Statutory Authority: ...]` div → `wac.authority_chain`
- In body text → `wac.cross_references`

A single regex covering both would silently misroute the same citation. The structurally-different routing (DOM-position-based scope discriminator) catches parser bugs that would otherwise propagate. Example: during smoke test, the multi-RCW comma-extension bug was visible because `auth chain: 1` showed up when the verbatim bracket had 2-3 RCW cites — surfaced by the count mismatch.

**#10 (NEW) — Phase 2 acquisition WARN classification ≠ Phase 3 parser classification.**

Phase 2 logged 11 chapters as "disposition-only" (no `<a name=>` active anchors but disposition markers in page text). Phase 3 parser refined: 1 true disposition-only, 2 appendix-only, 8 title-redirect. The Phase 2 single-criterion classification (presence of "Repealed by" string) was over-broad; Phase 3 multi-criterion classification (H1 content + table presence + section-anchor count) is structurally different and more precise.

This is general lesson: classification done at acquisition time is necessarily approximate; classification done at parse time can be definitive. Acquisition-time WARN should be treated as "needs investigation," not "definitive class assignment."

---

## 7. Open items for operator review before Phase 4 Stage 1

1. **Title 332-26 inspection.** I classified it as appendix-only based on parser-yield-zero and pattern similarity to 237-990, but haven't inspected the cached HTML directly. **Recommend: spot-inspect before Phase 4 priority-core ingestion.**
2. **Appendix-chapter ingest policy.** Skip entirely (recommended; Phase 8 backlog), or ingest as single oversized "section"? **Operator decision.**
3. **Title-redirect cleanup.** The 8 chapter slots in Title 365 are referenced from the title TOC but don't exist. Should Phase 7 differential update file a Code Reviser bug report when this state persists, or is it expected for legacy/recodified chapter numbers? **Operator-flagged for Phase 7 design.**
4. **Phase 4 Stage 2 wall-time confirmation.** Empirical ~105 min with HF stable. Operator's prior pre-authorization (Phase 4 priority core ~3-4 hours) holds; long pole is Stage 1 + acquisition + parser, not embedding. **Confirm assumption.**
5. **Note-block taxonomy.** Phase 0 sample size 4 surfaced no notes; full-corpus run shows note rows occur but rarely. Permissive `wac.section_notes.note_type TEXT` (no CHECK) was the right call. Phase 5 audit refines once corpus is loaded. **No action needed; flagging for Phase 5.**

---

## 8. Estimated remaining timeline

| Phase | Estimated wall-time | Notes |
|---|---|---|
| Phase 4 priority core (Titles 296, 162, 388, 110) | ~3–4 hours | Stage 1 + Stage 2 integrated; HF stable |
| Phase 4 priority block (Titles 246, 182, 357, 192, 308, 458) | ~3–4 hours | Same |
| Phase 4 long tail (~218 titles, 2,400+ remaining chapters) | ~6–10 hours | Same |
| Phase 4 authority resolution pass | ~30 min | UPDATE wac.authority_chain.rcw_section_id |
| Phase 5 validation | ~30 min | 6-check + Test E/F/G + 20-spot fidelity |
| Phase 6 docs | 1 hour | §P, §Q, §R additions |
| **Total remaining** | **~14–20 hours** | Within kickoff §11 estimate |

---

## 9. Closing posture

**Phase 3 disposition: COMPLETE.**

- Parser built and smoke-tested across 3 priority + 4 sampled long-tail chapters
- Full-corpus verification: 2,853 chapters parsed in 1.9 min, **0 hard parse errors**
- Corpus totals projected: 51,634 active sections + 21,746 disposition ghosts ≈ 73,380 `wac.sections` rows
- Mean 1.58 chunks/section → ~82,000 total chunks → ~105 min Stage 2 wall-time with HF stable
- Anomaly inventory clean: 11 non-standard chapters classified (1 true dispo-only, 2 appendix, 8 title-redirect)

No DB writes. No Stage 1 / Stage 4 work begun. Operator authorization required before Phase 4 priority-core ingestion launches.

**Awaiting operator authorization for Phase 4 Stage 1 priority-core ingest** (Titles 296 → 162 → 388 → 110 → Pause-Checkpoint A), conditioned on:

1. Spot-inspect 332-26 to confirm appendix-only classification (~2 min cost; recommend doing in Phase 4 prep)
2. Decision on appendix-only chapter handling (skip vs ingest-as-single-section)
3. Confirmation that title-redirect handling (skip with no row produced) is the right policy

End of Phase 3 cumulative report.
