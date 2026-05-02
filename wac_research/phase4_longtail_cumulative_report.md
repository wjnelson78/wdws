# WAC Phase 4 — End-of-Long-Tail Cumulative Report

**Status: COMPLETE.** All 224 long-tail titles processed; full WAC corpus ingested + embedded.
**Author:** Athena WAC-Ingest agent. Date: 2026-05-01.
**Operator:** William J. Nelson.

---

## Top-line — full corpus

| Metric | Value |
|---|---|
| Total title cites in coverage map (Phase 2) | 228 |
| Empty title slots (0 chapters in TOC; expected) | 52 |
| All-chapters-skipped title slots (Title 237 appendix-only) | 1 |
| **Titles with ingested content (`wac.sections` rows present)** | **175** |
| `wac.titles` rows created (includes Title 237 with 0 sections) | 176 |
| Chapters | 2,843 |
| **Total `wac.sections` rows** | **73,180** |
| Active sections | 51,204 |
| Ghost sections (repealed/recodified/decodified/expired/reserved) | 21,976 |
| Authority-chain rows | **105,775** |
| Cross-references | 89,082 |
| Section-history (WSR events) | 126,329 |
| **Chunks total** | **56,998** |
| **Chunks embedded** | **56,998 (100%)** |
| Chunks pending | 0 |
| **Authority-chain resolution rate** | **95.51%** (section-level: 83,752 / 87,689) |
| Empty-body active sections (should be 0) | **0** ✓ |

**Total WAC ingest wall-time:** 1h 52m 21s (start of priority core to last embedding).

## Phase 4 long-tail breakdown

| Stage | Wall-time | Throughput |
|---|---|---|
| Stage 1 driver (224 titles) | **12.8 min** | 224 / 12.8 = ~17 titles/min; ~30 chunks/sec produced |
| Stage 2 backfill (concurrent) | ~22 min total | 20-26 chunks/sec aggregate (dual T4) |
| Authority resolution UPDATE (post) | ~5.3 sec | 54,233 rows resolved Pass 1 + 4,199 Pass 2 |

## Stage 1 errors

52 title-slots returned exit 1 with `No chapter dir found at /opt/wdws/data/wac/html/chapters/<N>` — these are the empty title slots from Phase 2 coverage map (titles whose TOC parsed to 0 chapters). Expected; no anomaly. The driver counted them as "errors" but they're benign skips.

## Audit A — title-count reconciliation (post-closure)

`wac.titles` count = 176; `SELECT COUNT(DISTINCT title_number) FROM wac.sections` = 175. The 1-row delta is **Title 237**: title row created by `ensure_title_row()` at the start of Stage 1 ingest, but Title 237's only chapter (`237-990` "Determination of Geographic Names" — appendix-only) is in `SKIP_CITES` and was correctly skipped, leaving 0 sections under that title.

Reconciled: **175 titles have ingested content**; 1 title (237) was created in `wac.titles` but holds an appendix-only chapter that was deliberately skipped per Phase 3 anomaly inventory. Benign asymmetry; preserves audit data ("WAC corpus knows Title 237 exists with no in-corpus operative content").

## Audit B — missing-title classification (228 → 175)

228 populated title slots in coverage map − 175 ingested = **53 missing**. All 53 classified:

| Class | Count | Description |
|---|---|---|
| Empty-in-coverage (0 chapters in TOC per Phase 2) | 52 | Title slot listed on landing page but TOC parsed to 0 chapter links — pre-existing legitimate empties |
| All-chapters-in-skip-list (Title 237, chapter 237-990 appendix-only) | 1 | Only chapter is in `SKIP_CITES` → no sections ingested |
| **Other / unexpected** | **0** | (halt criterion: `> 0` would have triggered halt) |
| TOTAL | 53 | ✓ matches 228 − 175 |

**Verdict: WAC corpus is operationally complete.** All 53 missing are legitimately ineligible per Phase 3 protocol. No erroneous skips. Classification details in [`audit_b_missing_titles.json`](audit_b_missing_titles.json).

## Class distribution corpus-wide

| Class | Count | Treatment |
|---|---|---|
| Class A (image/diagram body) | 30 | Placeholder + URL |
| Class B1 (part-heading shell) | 41 | Placeholder + heading context |
| Class D-CFR (federal incorporation by reference) | 26 | Placeholder + extracted CFR cite list |
| Class D-external (ANSI/ASME/ICC) | 124 | Placeholder noting external standard |
| Reserved (empty body + Reserved caption) | 284 | Ghost row, `is_reserved=TRUE` |
| **Standard active sections** | **50,699** | Full body text |

## Authority-chain resolution

| Variant | Count | Resolution % |
|---|---|---|
| Section-level RCW citations | 87,689 | **95.51%** (83,752 resolved) |
| Chapter-level RCW citations (`chapter X.YY RCW`) | 13,515 | n/a (no specific section to FK to) |
| Session-law citations (`YYYY c NN`) | 4,571 | n/a (no FK by design) |
| **Total** | **105,775** | |

Section-level resolution rate of 95.51% far exceeds the 70% halt threshold. Unresolved 4,470 section-level cites correspond to RCW citations targeting sections that:
- Were repealed before being recodified (Pass 3 found 0; expected)
- Have subsection notation that doesn't strip to a real section (e.g., `RCW X.YY.ZZZ(99)` where (99) isn't a real subsection)
- Are typos or pre-codification citations

Acceptable per kickoff criteria.

## Tag distribution

| Category | Count |
|---|---|
| `code-jurisdiction` | 3 (federal-usc, state-wa-rcw, state-wa-wac) |
| `wac-meta` | 8 |
| `wac-title` (lazy-populated) | 176 (1 per ingested title) ✓ |
| WAC documents tagged `code-jurisdiction:state-wa-wac` | 73,180 |

## Methodology corroborations from this run

Updated Phase 6 §K corroboration count to **#13** (from initial #7 at start of WAC project; gained 6 corroborations during Phase 4):

- **§K #8**: LLM summarization vs programmatic count (Phase 0 title count — 116 reported, 228 actual)
- **§K #9**: Cross-ref scope discriminator catches authority/body misroute (DOM position-based routing)
- **§K #10**: Acquisition-time classification ≠ parse-time classification (Phase 2 WARN refinement)
- **§K #11**: Schema CHECK at INSERT catches silent parser failures that count-based verification misses (454 empty-body sections)
- **§K #12**: Sample-first gates need to cover both extraction QUALITY and source COVERAGE axes (Class D PDF empty discovery)
- **§K #13**: Silent fallbacks defeat halt-on-error semantics (HF→CPU 404 silent fallback, Title 162 URL bug)

Plus the doctrinal-criterion-vs-corpus elaboration noted in Phase 6 §H2 (Pattern (a)/(c) refinement).

## Operator decisions / audit trail honored

- Title 162 CPU-fallback chunks: accepted as-is per operator (vectors GPU-equivalent at cosine ≥0.9999); audit row written to `wac.ingestion_runs`
- RCW 26.44 doctrinal-PASS verdict: accepted (4 = correct per WA legal architecture; criterion was based on incorrect assumption)
- Class D treatment: Option (a) placeholder + URL with CFR-discriminator (Option (b) PDF fetch ruled out after sample showed PDFs themselves empty)
- URL bug fix: applied; corpus-wide grep verified clean
- cuda:1 service on .187: systemd-managed via `bge-embedding-cuda1.service`

## Next steps

Per operator pre-authorization:

1. **Phase 5 validation suite** (~30 min) — adapt RCW phase5_validation.py for WAC, run 6-check + Test E/F/G + 20-spot fidelity sample
2. **Phase 6 retrieval guide §P, §Q, §R** updates documenting WAC-specific patterns + authority chain + three-jurisdiction queries
3. **HARD STOP** for operator review

Authority-chain resolution UPDATE is complete (95.51% rate); no further updates required unless RCW corpus changes.

## State at end of long tail

- DB: 73,180 sections, 56,998 chunks 100% embedded, 105,775 authority rows 95.51% resolved
- HF services: .187:9098 (cuda:0) + .187:9099 (cuda:1) both running healthy
- 0 anomalies / halts during long tail
- 0 hard parse errors (52 empty-title skips classified separately)
- Original wac.ingestion_runs preserved for audit; Title 162 row carries CPU-fallback note

End of long-tail cumulative report.
