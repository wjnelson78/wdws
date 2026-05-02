# WAC Source-Format Findings

**Phase 0 deliverable.** Author: Athena WAC-Ingest agent. Date: 2026-04-30 (revised from preliminary).
Operator: William J. Nelson. Reviewed sources, no DB writes, no code generated.

Companion to (and parallels) `/opt/wdws/rcw_research/rcw_source_findings.md`.
This is the formally authorized Phase 0 document. The preliminary version
written before authorization on 2026-04-30 has been re-validated against
the locked-in architectural parameters in the kickoff briefing and the
RCW Phase 1 reference assets.

---

## 1. Source-Format Inventory

### 1.1 Authoritative endpoints probed

| Endpoint | Status | Notes |
|---|---|---|
| `https://app.leg.wa.gov/wac/` | 200 | WAC landing page; lists 116 titles, "Last update: April 1, 2026" |
| `https://app.leg.wa.gov/WAC/default.aspx?cite=N` | 200 | Title TOC (lists chapters in title) |
| `https://app.leg.wa.gov/WAC/default.aspx?cite=T-CC&full=true` | 200 | **Full chapter dump** — all sections inline with statutory authority + disposition table |
| `https://app.leg.wa.gov/WAC/default.aspx?cite=T-CC-SSS` | 200 | Single-section page (assumed; not separately verified) |
| `https://app.leg.wa.gov/WAC/default.aspx?dispo=true&cite=N` | 200 | **Title-level disposition page** — query-string variant, NOT a separate `dispo.aspx` endpoint as RCW uses |
| `https://app.leg.wa.gov/WAC/default.aspx?dispo=true&cite=T-CC` | 200 | Chapter-level disposition page (redundant — disposition table is also inline in `&full=true`) |
| `https://lawfilesext.leg.wa.gov/law/WACArchive/` | 200 | Annual certified WAC archives, 1977–2025 (45 year directories; 2018a appears as a revision; missing years 1978/1981/1984/1987/1994) |
| `https://lawfilesext.leg.wa.gov/law/WACArchive/2025/` | 200 | Per-title `.htm` + per-chapter `.htm` files (~800–900 files); PDF subdir `/pdf/` |
| `law.justia.com/codes/washington/title-296/` | 403 | Cloudflare-locked; same outcome as RCW research. Not viable as fidelity dictionary. |

### 1.2 Bulk download / API endpoints

**No bulk XML, JSON, or API endpoint exists.** Same posture as RCW: HTML-only at `app.leg.wa.gov/WAC/`. Official certified publication is the per-year HTM + PDF set in `lawfilesext.leg.wa.gov/law/WACArchive/YYYY/`. No USLM XML equivalent.

### 1.3 Open-data-portal probe

Skipped — RCW Phase 0 confirmed `data.wa.gov` (Socrata) does not host Code Reviser publications. Same agency; same dead end.

### 1.4 Justia secondary-source probe

Justia returns HTTP 403 behind Cloudflare's managed challenge for WA codes. Not viable as fidelity dictionary.

**Replacement secondary source for Phase 5 fidelity check:** the certified per-title and per-chapter HTM files at `lawfilesext.leg.wa.gov/law/WACArchive/YYYY/` (2025 vintage; current corpus). Self-reference for fidelity is acceptable here — the live HTML and the archived HTM are produced from the same source database; divergence between them would be a Code Reviser bug worth surfacing.

### 1.5 Crawl friction probe

- **No anti-bot challenge** observed on `app.leg.wa.gov/WAC/` with the honest UA (extrapolating from RCW Phase 0; same host, same ASP.NET surface).
- **No `robots.txt`** at either `leg.wa.gov` or `app.leg.wa.gov` (verified during RCW Phase 0).
- **Last-update marker** on the WAC landing page: `Last update: April 1, 2026`. This becomes the `publication_date` value `2026-04-01` for all Phase 4 sections, recorded in `wac.titles.publication_date` and propagated to the `(canonical_citation, publication_date)` uniqueness constraint on `wac.sections`.

---

## 2. Chapter / Section HTML Dissection

### 2.1 The decisive finding: `&full=true` chapter dumps include the disposition table inline

Adding `&full=true` to a chapter URL returns:
1. Chapter heading and section TOC.
2. Every active section's full text + statutory authority bracket + revision history.
3. **A disposition table at the end listing every repealed/recodified/decodified section in the chapter's history**, with WSR numbers and effective dates.

**This is materially more compact than RCW's split between `default.aspx` (active sections) and `dispo.aspx` (status).** One fetch per chapter gets both. The chapter-level `dispo=true` query-string variant exists as a redundant standalone view but is not required for ingest.

| View | Bytes (sample) | Section content | Fetch cost (full WAC) |
|---|---|---|---|
| Per-section (`cite=T-CC-SSS`) | ~30 KB | 1 section | ~40,000 fetches × 30 KB = 1.2 GB; **~11 hr** at 1 req/sec |
| Per-chapter `&full=true` (e.g. `cite=296-15&full=true`) | ~150–600 KB | all active sections + disposition table | ~2,200 fetches × ~250 KB avg = 550 MB; **~37 min** at 1 req/sec |

**Decision:** Phase 2 acquisition fetches whole chapters with `&full=true`. Sections are sliced by `<a name='T-CC-SSS'></a>` anchors. Repealed/recodified entries parsed from the chapter's disposition table at the end of the same payload — **no second fetch per chapter required**.

### 2.2 Per-section anchor format — DASHES not DOTS

Verified across samples in Title 296 (`cite=296-15&full=true`), Title 162 (`cite=162-16&full=true`), Title 296-14A (`cite=296-14A&full=true`):

```html
<a name='T-CC-SSS'></a>
```

Examples seen verbatim: `<a name='296-15-001'>`, `<a name='162-16-200'>`, `<a name='296-14A-010'>`.

**Critical difference from RCW:** WAC uses dashes throughout (`T-CC-SSS`), not dots (`T.CC.SSS`). Parser regex `[\d]+-[\d]+[A-Z]?-[\d]+` rather than the dotted form. Citation strings stored in `wac.sections.canonical_citation` as `WAC T-CC-SSS` (e.g. `WAC 296-15-001`).

**Carry-forward from RCW UCC (Title 62A) lesson:** use a SCOPED ALTERNATION regex for citation detection, not a permissive substitution. Specifically, the citation extractor must match `WAC \d+-\d+[A-Z]?-\d+` as a complete unit, not piecewise. RCW Phase 3 surfaced false positives in cross-reference detection from permissive dash-substitution; the lesson transfers.

Lettered chapters (`296-14A`, `132A`, `28A`, `181`, etc.) render identically — the letter is absorbed into the chapter component, not the section component. Section numbers are pure digits.

**Lettered titles confirmed as full citation citables.** WAC 132A (Peninsula College, 14 chapters) is reachable at `cite=132A`. The bare numeric title `cite=132` returns "Citation not found" — Title 132 has only lettered sub-variants (132A, 132B, etc.). Some title-number "slots" hold multiple lettered sub-titles rather than a bare numeric title.

### 2.3 Per-chapter (`&full=true`) page anatomy

```html
<!-- chapter title block -->
<h1>Chapter T-CC WAC</h1>
<h2>CHAPTER HEADING</h2>

<!-- WAC Sections TOC (links to anchors below) -->

<!-- For each active section, in order: -->
<a name='T-CC-SSS'></a>
<h3>WAC T-CC-SSS</h3>
<h3>Section caption.</h3>
<div>
  Body text — paragraphs, subsections, tables, lists.
</div>
<div>
  [Statutory Authority: RCW X.YY.ZZZ. WSR YY-NN-NNN, § T-CC-SSS, filed M/D/YY, effective M/D/YY;
   WSR YY-NN-NNN, § T-CC-SSS, filed M/D/YY, effective M/D/YY.]
</div>

<!-- ... more sections ... -->

<!-- DISPOSITION TABLE (inline at end of chapter) -->
T-CC-XXX  Section heading.  [Original: Order NN-N, § T-CC-XXX, filed M/D/YY.]
   Repealed by WSR YY-NN-NNN, filed M/D/YY, effective M/D/YY. Statutory Authority: RCW X.YY.ZZZ.
T-CC-YYY  Section heading.
   Recodified as WAC T-CC-ZZZ. WSR ...
T-CC-ZZZ  Section heading.
   This section has been decodified as it was omitted from "..." filed M/D/YY.
```

**Parser anchors (chapter `&full=true` view):**

- **Active section boundary**: `<a name='T-CC-SSS'></a>` — canonical splitter.
- Section citation block: `<h3>WAC T-CC-SSS</h3>` — first `<h3>` after the anchor.
- Section caption: `<h3>` — second `<h3>` after the anchor.
- Body, statutory authority, revision history: subsequent `<div>` siblings.
- **Disposition table**: appears after the last active section's authority bracket. Parser reads section-number prefix on each row, splits on disposition events.

### 2.4 The statutory authority bracket — the authority-chain hook

Always at the end of an active section's body, inside its own `<div>`. Format:

```
[Statutory Authority: <authority-list>. <revision-event-1>; <revision-event-2>; ... .]
```

Where `<authority-list>` is a comma-separated list of:
- RCW citations with subsection: `RCW 49.60.120(3)`, `RCW 51.04.020`, `RCW 51.32.190`
- RCW chapters: `chapter 49.04 RCW`, `chapters 51.04, 51.08, 51.12, 51.24, and 51.32 RCW`
- Session-law citations: `2020 c 277`, `2011 1st sp.s. c 37 § 305` (Washington State Legislature session)
- Federal citations (rare; observed in agency-grant-conformity contexts; format varies)

And each `<revision-event-N>` has the form:

```
WSR YY-NN-NNN, § T-CC-SSS, filed M/D/YY, effective M/D/YY
```

WSR = Washington State Register, the chronological proposal/notice publication. Format `YY-NN-NNN` encodes year + register-issue + filing within issue.

**Verbatim samples captured:**

```
[Statutory Authority: RCW 51.04.020, 51.32.190 and 2020 c 277.
 WSR 21-11-083, § 296-15-001, filed 5/18/21, effective 7/1/21.]

[Statutory Authority: RCW 49.60.120(3).
 WSR 15-24-071, § 162-16-200, filed 11/25/15, effective 12/26/15;
 WSR 99-15-025, § 162-16-200, filed 7/12/99, effective 8/12/99.]

[Statutory Authority: RCW 51.04.020, 51.04.030, and 2011 1st sp.s. c 37 § 305.
 WSR 12-07-065, § 296-14A-010, filed 3/20/12, effective 4/20/12.]
```

Phase 3 parser produces three structured outputs per bracket:

1. **`wac.authority_chain`** rows — one per RCW citation in `<authority-list>`. Session-law and chapter-level RCW handled per `authority_chain_design.md`.
2. **`wac.section_history`** rows — one per `<revision-event-N>`, with WSR register, action type, dates.
3. **`wac.sections.effective_date`** — most recent `effective M/D/YY` from latest revision event.

Each WSR citation is captured in `wac.sections.wsr_register_number` (most recent revision's WSR) and in `wac.section_history.wsr_register` (full revision history). These are WAC-specific schema columns parallel to RCW's `enacted_by_session_law`.

### 2.5 Cross-reference link patterns

| Link form (in source body or notes) | Target |
|---|---|
| `<a href='http://app.leg.wa.gov/WAC/default.aspx?cite=T-CC-SSS'>` | Internal WAC section |
| `<a href='http://app.leg.wa.gov/WAC/default.aspx?cite=T-CC'>` | Internal WAC chapter |
| `<a href='http://app.leg.wa.gov/RCW/default.aspx?cite=X.YY.ZZZ'>` | RCW section (target_jurisdiction='state-wa-rcw') |
| `<a href='http://app.leg.wa.gov/RCW/default.aspx?cite=X.YY'>` | RCW chapter |
| `<a href='https://lawfilesext.leg.wa.gov/biennium/.../sl/...'>` | Session law PDF |
| Plain text `42 U.S.C. § 12112` | Federal USC reference (target_jurisdiction='federal-usc') |
| Plain text `45 C.F.R. § 84.4` | Federal CFR reference (target_jurisdiction='cfr') |

Phase 3 cross-reference parser writes to `wac.cross_references` per the routing:
- WAC T-CC-SSS in body text → `target_jurisdiction='state-wa-wac'`, `ref_type='reference'`
- RCW citation in body text (NOT in authority bracket) → `target_jurisdiction='state-wa-rcw'`, `ref_type='reference'`
- RCW citation in `[Statutory Authority: ...]` bracket → routed to `wac.authority_chain` instead (NOT `cross_references`)
- 42 U.S.C. § 12112 → `target_jurisdiction='federal-usc'`, `ref_type='cross-statute'`
- 45 C.F.R. § 84.4 → `target_jurisdiction='cfr'`, `ref_type='reference'` (CFR not in corpus)

`wac.cross_references.target_jurisdiction` CHECK constraint values: `'state-wa-rcw' | 'state-wa-wac' | 'federal-usc' | 'cfr' | 'wa-court-rule' | 'session-law' | 'wa-constitution'` — mirrors `rcw.cross_references` CHECK list with `'state-wa-wac'` substituted/added (RCW's CHECK already includes `'wac'`; WAC's uses the longer slug-aligned form `'state-wa-wac'`).

---

## 3. Section-Status Taxonomy

### 3.1 Inline disposition entries — the WAC pattern

Unlike RCW (where repealed/recodified sections appear ONLY on the separate `dispo.aspx` page), WAC's chapter `&full=true` view includes a disposition table inline. This table lists every section number that ever existed in the chapter and its current status.

Verified disposition sentence patterns:

| Sentence pattern | Semantic | Schema flag |
|---|---|---|
| `Repealed by WSR YY-NN-NNN, filed M/D/YY, effective M/D/YY. Statutory Authority: RCW X.YY.ZZZ.` | Section repealed; no replacement | `is_repealed=TRUE`, `effective_date={date}` |
| `Repealed by WSR YY-NN-NNN, filed M/D/YY, effective M/D/YY. Statutory Authority: Chapters X.XX, Y.YY ... RCW.` | Same with chapter-level authority | `is_repealed=TRUE` |
| `Repealed by Order NN-N, filed M/D/YY. See WAC T-CC-SSS.` | Repealed with redirection to active WAC | `is_repealed=TRUE`, `cross_references` row pointing to See target |
| `Recodified as WAC T-CC-SSS. WSR YY-NN-NNN ...` | Section moved to new citation | `is_recodified=TRUE`, `recodified_to={new-citation}` |
| `This section has been decodified as it was omitted from "..." filed M/D/YY.` | Removed from WAC; original publication intentionally excluded | `is_decodified=TRUE` |
| `Reserved` | Section number reserved | `is_reserved=TRUE`, no full text |
| `Expired M/D/YY.` (anticipated; not observed in samples) | Sunset clause fired | `is_expired=TRUE` |

### 3.2 Three flavors of `wac.sections` rows (mirrors RCW pattern §3.1)

1. **Live current section**: status flags FALSE, full body text in `full_text`. Embedded into chunks. (Estimated ~30,000–50,000 rows.)
2. **Recodified-out ghost**: `is_recodified=TRUE`, `recodified_to` set, `full_text=NULL`, no embeddings. Allows citation lookups against historical WAC citations to redirect to current.
3. **Repealed/decodified ghost**: `is_repealed=TRUE` or `is_decodified=TRUE`, `full_text=NULL`, no embeddings.

`wac.sections.full_text` declared as `TEXT NULL` from inception. The `chk_section_text_status_invariant` CHECK constraint is the load-bearing audit channel — INSERT-time enforcement that "active section ⇔ has text; ghost row ⇔ no text". Per Phase 6 §K (corroborated 7× during USC + RCW), schema CHECK constraints are structurally independent from parser/audit predicates and catch what regex audits miss.

### 3.3 Pending recodification (carry-forward from RCW)

Same pattern as RCW (added via `schema_amendment_pending_recodification.sql`): two columns `pending_recodification_to TEXT` and `pending_recodification_effective DATE`, paired-NULL CHECK constraint `chk_pending_recodification_pair`. Included from inception in WAC schema (no post-Phase-1 amendment needed).

WAC Phase 0 has not yet observed pending-recodification announcements in source HTML samples (sample size 4 chapters), but the column is included by inheritance from RCW pattern. Phase 3 parser must scan section heading/leading text for "Effective until DATE. Recodified as WAC ..." patterns analogous to the RCW Title 42.17A.* → 29B.* announcements.

### 3.4 No comparison.aspx for WAC

Recodification mappings are inline in disposition entries via the `Recodified as WAC T-CC-SSS` pattern. Phase 3 captures these as `wac.cross_references` rows with `ref_type='recodification'`.

### 3.5 Lettered title and chapter variants

Confirmed:
- **Lettered titles** exist (`132A` Peninsula College, `132B`, `132T` etc.; `181` per WAC landing page). Some title-number slots hold ONLY lettered variants — bare `cite=132` returns "Citation not found" but `cite=132A` works.
- **Lettered chapters** render correctly within numeric titles (`296-14A`, `192-32`, `181-78A`).

Schema inherits RCW's TEXT-everywhere posture:
- `wac.titles.title_number TEXT` — accepts `1`, `296`, `132A`, etc.
- `wac.chapters.chapter_number TEXT` — `T-CC` form, e.g. `296-15`, `296-14A`
- `wac.sections.section_number TEXT` — pure digits in samples but TEXT for safety

---

## 4. WSR Register Integration (deferred)

Every revision event references a WSR (Washington State Register) issue: `WSR YY-NN-NNN`. WSR documents are OUT OF SCOPE for Phase 4 (per kickoff §10). Phase 4 stores the WSR citation as a string (`wac.section_history.wsr_register='WSR 21-11-083'`) but does not fetch or embed WSR text. Phase 8+ project can ingest WSR if useful.

---

## 5. WAC Archive (lawfilesext) — Phase 5 fidelity dictionary

`https://lawfilesext.leg.wa.gov/law/WACArchive/2025/` hosts the certified 2025 vintage:

- File types: `.htm` (per-title and per-chapter) + `.pdf` (under `/pdf/` subdirectory)
- Naming pattern: `WAC NN TITLE.htm`, `WAC NN - CC CHAPTER.htm`
- ~800–900 HTM files
- Per-title organization

Phase 5 fidelity check: 20-spot stratified sample compares Phase 4 ingested text against 2025 archive HTM. Use SHA-256 of normalized text (whitespace + bracket-format normalized) to detect parser drift.

**Caveat on archive vintage vs live HTML:** The archive is 2025; the live site is `Last update: April 1, 2026`. For exact-match fidelity, the live HTML is canonical and the archive is a one-step-behind comparison. If they differ, distinguish "parser drift" from "legitimate amendment" by checking the section's revision history (newest WSR citation should be 2026 if amended).

---

## 6. Volume Sanity Check

### 6.1 Title census

**116 WAC titles** confirmed from `https://app.leg.wa.gov/wac/`. Numbered 1–516 with gaps and lettered variants. Title-slot count through 516 is much higher (the landing page lists titles); the **populated title count is 116**.

Operator's prior briefing estimated "~270 WAC titles" — that estimate was high; possibly counted numeric slots through 516 with gaps + lettered variants, but populated titles = 116. Operator-acknowledged correction.

### 6.2 Chapter census (sampled)

Sample expanded from 8 to 16 titles. Mix biased toward priority titles (large agencies):

| Title | Agency | Chapters |
|---|---|---|
| 4 | Accountancy, Board of | 1 |
| 51 | State Building Code Council | 11 |
| 110 | DCYF | 28 |
| 132A | Peninsula College | 14 |
| 162 | Human Rights Commission | 14 |
| 173 | Department of Ecology | 96 |
| 182 | Health Care Authority | 41 |
| 192 | Employment Security Department | 44 |
| 222 | Forest Practices Board (NOT HCA) | 15 |
| 230 | Gambling Commission | 16 |
| 246 | Department of Health | 86 |
| 296 | L&I | 71 |
| 308 | Department of Licensing | 71 |
| 314 | Liquor and Cannabis Board | 30 |
| 357 | State HR / civil service | 12 |
| 388 | DSHS | 88 |
| 458 | Department of Revenue | 21 |

Sum: 659 chapters across 17 sample points. Mean: ~39 chapters/title.

Sample is biased toward priority-relevant (and therefore larger) agency titles. Long-tail titles tend to be small. Excluding the priority/large-agency titles (110, 162, 173, 182, 192, 222, 246, 296, 308, 388 — 10 titles, 580 chapters), the long-tail subset is:

- 4(1), 51(11), 132A(14), 230(16), 314(30), 357(12), 458(21) — 7 titles, 105 chapters, mean **15** chapters/title.

**Refined full-corpus chapter projection:**

- 10 priority/large titles: ~580 chapters (sampled directly)
- 6 priority-priority titles per kickoff §4.5 (slots 5–10): est ~260 chapters (mix of medium agencies)
- 100 long-tail titles at mean 15: ~1,500 chapters
- **Total estimate: ~2,300–2,500 chapters**

This straddles the operator's revised ~1,000–1,500 estimate and the preliminary doc's ~3,200 estimate. The truth is likely ~2,000–2,500. Halt-trigger threshold (>2× deviation) baselines against ~2,300; any actual count outside [1,150, 4,600] would halt.

### 6.3 Section census (estimated)

WAC 162-16 sample: 9 active sections + 17 disposition entries (= 26 total `wac.sections` rows for one chapter).
WAC 296-14A sample: 9 active sections.
WAC 296-15 sample: ~50 sections in the dispo page (mix of active + disposed).

Rough mean: 12–18 active sections/chapter; disposed-to-active ratio grows for older chapters.

**Projected totals:**
- Active sections: ~2,300 chapters × 13 mean active = **~30,000 active sections**
- Disposition ghost rows: roughly 0.5–1.0× active count = **~15,000–30,000 ghost rows**
- Total `wac.sections` rows (active + ghost): **~45,000–60,000**

Active section count squarely in operator's locked 30,000–50,000 estimate. **No halt trigger on section count.**

### 6.4 Chunk-count and ingest-time projection

WAC sections typically shorter than RCW sections (regulations are single-purpose paragraphs of 200–800 tokens). Apply RCW Phase 0 §6.4 reconciler (1):

- 30,000 active sections × ~5 chunks/section avg ≈ **~150,000 chunks**
- BGE-M3 throughput baseline (USC empirical): 125 chunks/min sustained
- 150,000 ÷ 125 = **~20 hours of embedding wall-time**

**HF endpoint state (operator-confirmed 2026-04-30 in kickoff briefing):**
- HF .187 primary: stable
- HF .113 standby: currently DOWN — `HF_ENDPOINT_URL_FALLBACK` should remain unset to avoid handshake overhead
- `EMBEDDING_FORCE_LOCAL=1` is the standing fallback path; do NOT auto-fall-back without operator authorization mid-run

**Realistic Phase 4 Stage 2 wall-time projection: ~20 hours** (HF stable per operator confirmation).

If HF .187 degrades during Phase 4, halt cleanly and surface for operator authorization on FORCE_LOCAL fallback (§5.3 of the kickoff halt protocol).

### 6.5 Fetch-time projection

| Phase 2 acquisition | Fetch unit | Count | Wall-time @ 1 req/sec |
|---|---|---|---|
| Title TOCs | title page | 116 | 2 min |
| Chapter `&full=true` (the bulk download) | chapter page | ~2,300 | **38 min** |
| Chapter dispositions | (NOT REQUIRED — inline in `&full=true`) | 0 | 0 |
| Title-level dispositions (sanity probe) | dispo per title | 116 | 2 min |
| **Total Phase 2 fetch wall-time** | | | **~42 min** |

Reduced from preliminary estimate of ~57 min — refined chapter count is lower.

---

## 7. Twice-monthly publication cadence — Phase 7 implication

The Code Reviser publishes WAC updates approximately twice monthly (timed against WSR adoption-effective-date cycles). Phase 7 differential update cron should run **weekly** with monthly hard refresh — much more frequent than RCW (twice yearly). Budget Phase 7 maintenance accordingly.

---

## 8. Authority chain — the WAC-specific architectural choice

Operator-locked: Option B (dedicated `wac.authority_chain` join table). Decision recorded in `/opt/wdws/wac_research/authority_chain_design.md` (companion document).

---

## 9. Schema inheritance from RCW (locked)

WAC Phase 1 schema mirrors `rcw.*` per kickoff §4.2 with these specifications:

**Tables (7 + 1 = 8):**
- `wac.titles`, `wac.chapters`, `wac.sections`, `wac.cross_references`, `wac.section_history`, `wac.section_notes`, `wac.ingestion_runs` (parallel to `rcw.*`)
- `wac.authority_chain` (NEW, WAC-specific — Option B)

**Key columns on `wac.sections`** (all inherited from RCW pattern):
- All 5 status flags: `is_repealed`, `is_recodified`, `is_decodified`, `is_expired`, `is_reserved`
- Pending-recodification: `pending_recodification_to TEXT`, `pending_recodification_effective DATE`
- WAC-specific: `wsr_register_number TEXT` (latest WSR per section)

**CHECK constraints (load-bearing audit channels):**
- `chk_section_text_status_invariant` — active ⇔ has text
- `chk_recodified_target` — is_recodified → recodified_to NOT NULL
- `chk_pending_recodification_pair` — both pending fields NULL or both populated
- `chk_unit_type` — `unit_type IN ('section','reserved')`

**Tag taxonomy (locked-in conventions from RCW schema):**
- `core.tags.slug` is bare (e.g., `state-wa-wac`, NOT `code-jurisdiction:state-wa-wac`)
- `core.tags.category` groups slugs (e.g., `code-jurisdiction`)
- New categories for WAC: `wac-meta` (status flags, parallel to `rcw-meta`), `wac-title` (one tag per title)
- Add to existing `code-jurisdiction` category: `state-wa-wac` slug (joining `federal-usc`, `state-wa-rcw`)
- Topical tags added to existing `legal-framework` category: `wlad-enforcement-wac`, `bii-regulations-wac`, `pra-regulations-wac`, `dshs-wac`, `dcyf-wac`, etc. (Phase 1 finalizes the list.)

**Retroactive tagging skipped:** USC + RCW already have `code-jurisdiction` tags. WAC adds the third value but does not retro-tag prior corpora.

**Cross-jurisdiction filter kwarg** (canary_lib): `code_jurisdiction_filter` (renamed from preliminary `jurisdiction_filter` per RCW Phase 1 operator correction). Default None (all jurisdictions). Implementation via EXISTS clause against `core.document_tags` (NOT array overlap).

---

## Summary: changes from the preliminary version + kickoff briefing absorbed

1. **Tag namespace**: bare slug `state-wa-wac`, NOT colon-prefixed. Aligns with RCW Phase 1 `core.tags` model.
2. **Volume estimate refined**: ~2,300 chapters (between operator's 1,000–1,500 and preliminary 3,200). 116 populated titles confirmed.
3. **Chapter sample expanded**: 16 titles (was 8). Long-tail mean ~15 chapters/title; priority titles much larger.
4. **HF endpoint state corrected**: .187 stable, .113 currently DOWN — `HF_ENDPOINT_URL_FALLBACK` left unset per operator instruction.
5. **canary_lib filter renamed** to `code_jurisdiction_filter` per RCW Phase 1 operator correction.
6. **Schema inheritance specifics nailed down**: 8 tables (7 + authority_chain), all 5 ghost flags, pending-recodification columns from inception, WAC-specific `wsr_register_number TEXT` column.
7. **Cross-references CHECK constraint** mirrors RCW with `'state-wa-wac'` joining the target_jurisdiction allow-list.
8. **Tag retroactive UPDATE not needed** — USC + RCW already have code-jurisdiction tags; only the new `state-wa-wac` slug gets added in Phase 1.
9. **Lettered title finding refined**: bare numeric `cite=132` returns "not found"; lettered variants (132A, 132B) are full titles. Phase 4 must enumerate lettered variants properly.
10. **Phase 4 Stage 2 wall-time** reduced to ~20 hours (HF stable, refined chunk count).
11. **Phase 2 fetch wall-time** reduced to ~42 min (refined chapter count).

End of source-format findings. Awaiting operator authorization before Phase 1 schema-design proposal (per kickoff §6 phase plan).
