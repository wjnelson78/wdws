# RCW Source-Format Findings

**Phase 0 deliverable.** Author: Athena RCW-Ingest agent. Date: 2026-04-28.
Operator: William J. Nelson. Reviewed sources, no DB writes, no code generated.

---

## 1. Source-Format Inventory

### 1.1 Authoritative endpoints probed

| Endpoint | Status | Notes |
|---|---|---|
| `https://app.leg.wa.gov/RCW/` | 200 | RCW landing page; URL pattern `default.aspx?Cite=N` |
| `https://app.leg.wa.gov/RCW/default.aspx?cite=N.NN` | 200 | Chapter TOC (lists sections in chapter, with HTML/PDF buttons) |
| `https://app.leg.wa.gov/RCW/default.aspx?cite=N.NN&full=true` | 200 | **Full chapter dump** — all sections inline in one HTML page. Decisive architectural finding. |
| `https://app.leg.wa.gov/RCW/default.aspx?cite=N.NN.SSS` | 200 | Single-section page |
| `https://app.leg.wa.gov/RCW/default.aspx?cite=N.NN.SSS&pdf=true` | 200 | Per-section PDF (for content-fidelity spot-check) |
| `https://app.leg.wa.gov/RCW/dispo.aspx?Cite=N` | 200 | Title-level disposition index (chapter list + status pages) |
| `https://app.leg.wa.gov/RCW/dispo.aspx?Cite=N.NN` | 200 | **Chapter-level disposition page** — repealed/recodified/reserved entries with target citations |
| `https://app.leg.wa.gov/RCW/comparison.aspx?cite=N.NN` | seen as link | Recodification comparative table (e.g., 42.17→42.56 cross-walk) |
| `https://app.leg.wa.gov/RCW/default.aspx?cite=N` | 200 | Title TOC (lists all chapters in title) |
| `https://lawfilesext.leg.wa.gov/biennium/YYYY-YY/Pdf/Bills/Session Laws/...` | linked | Modern session-law PDFs (1995+) |
| `https://leg.wa.gov/CodeReviser/documents/sessionlaw/YYYYcN.pdf` | linked | Older session-law PDFs (pre-1995, certified by Code Reviser) |
| `https://lawfilesext.leg.wa.gov/law/RCWArchive/` | parent dir | Annual certified RCW archives (PDF; declared as the official publication) |
| `leg.wa.gov/robots.txt` | 404 | No robots.txt (verified) |
| `app.leg.wa.gov/robots.txt` | 404 | No robots.txt (verified) |

### 1.2 Bulk download / API endpoints

**No bulk XML, JSON, or API endpoint exists.** The Office of the Code Reviser's online RCW is HTML-only at `app.leg.wa.gov/RCW/`. The official certified publication is the per-year PDF set in the RCW Archive (`lawfilesext.leg.wa.gov/law/RCWArchive/YYYY/`). No USLM XML equivalent.

### 1.3 Open-data-portal probe

`data.wa.gov` (Washington State Open Data Portal, Socrata): no RCW dataset published. Catalog search returned no hits for "RCW", "Code Reviser", "statutes", or "legislation". Confirmed dead end — Code Reviser does not route through the open-data portal.

### 1.4 Justia secondary-source probe

Operator-confirmed candidate: `law.justia.com/codes/washington/`. **Result: not viable.** Justia is behind Cloudflare's "managed challenge" page that returns HTTP 403 to non-browser user-agents (verified with both WebFetch and curl using the honest project UA). The page demands JavaScript execution + Cloudflare turnstile to proceed, which scraping tooling cannot solve. Justia cannot serve as the §1.2 known-section content-fidelity dictionary.

**Replacement secondary source for Phase 5 fidelity check:** the certified per-section PDFs at `app.leg.wa.gov/RCW/default.aspx?cite=N.NN.SSS&pdf=true` and the annual certified archives at `lawfilesext.leg.wa.gov/law/RCWArchive/YYYY/`. Both are produced by the Code Reviser itself and constitute the official publication per the explicit statement on the RCW landing page ("the certified PDF publication documents in the RCW Archive area on the Office of the Code Reviser's website constitute the official publication"). Self-reference for fidelity is acceptable here because the HTML and the PDF are produced from the same source database — divergence between them would itself be a Code Reviser bug worth surfacing.

### 1.5 Crawl friction probe

- **No robots.txt** at either `leg.wa.gov` or `app.leg.wa.gov`. Crawl posture is governed only by terms of service and operator courtesy.
- **No anti-bot challenge** observed on `app.leg.wa.gov` with the honest UA `Athena-RCW-Ingest/1.0 (research; will@nelson; pro se litigant assistive tech)`. All 100 title TOCs + 30 random chapter TOCs + 7 section pages + dispositions were fetched at 1 req/sec without 429, captcha, or cookie-wall.
- **Cookies set:** `ASP.NET_SessionId` (HttpOnly, SameSite=Lax) and a non-secret `JSESSSIONID` (sic — typo in the source). Stateless GET works without cookie persistence; verified.
- **No ASP.NET ViewState** in section/chapter content pages. The aspx surface is read-only GET — no `__VIEWSTATE` or `__EVENTVALIDATION` hidden fields needed for navigation. Some auxiliary pages may still use ViewState; not encountered in the read paths.
- **Last-update marker** on the RCW landing page: `<b>Last update: August 15, 2025</b>` — the current corpus is the post-2025-session publication. This becomes the `publication_date` value `2025-08-15` for all Phase 4 sections, recorded in `rcw.titles.publication_date` and propagated to `(full_citation, publication_date)` uniqueness constraint on `rcw.sections`.

---

## 2. Chapter / Section HTML Dissection

### 2.1 The decisive finding: `&full=true` chapter dumps

Adding `&full=true` to a chapter URL returns the entire chapter — every section's full text, source credits, and notes — in a single HTML response. This collapses the ingest fetch volume by ~50× compared to per-section fetching.

| View | Bytes | Section count | Fetch cost (full RCW) |
|---|---|---|---|
| Per-section (`cite=42.56.520`) | ~110 KB | 1 section | ~45,000 fetches × 110 KB = 4.95 GB; **~12.5 hr** at 1 req/sec |
| Per-chapter, `&full=true` (`cite=42.56&full=true`) | ~600 KB (PRA: 92 sections in 606 KB) | all sections in chapter | ~2,763 fetches × ~150 KB avg = 414 MB; **~46 min** at 1 req/sec |

**Decision:** Phase 2 acquisition fetches whole chapters with `&full=true`. Sections are sliced from the chapter DOM by `<a name='X.YY.SSS'></a>` boundary anchors. This is the canonical fetch unit.

Phase 7 differential update can use the per-section URL when re-fetching individual changed sections (cheaper round-trip when only one section in a chapter changed), but the bulk Phase 4 ingest is per-chapter.

### 2.2 Per-section page anatomy

Anchors confirmed in three samples (RCW 4.04.010, RCW 42.56.520, RCW 26.44.030):

```html
<div class="container-xxl" id="mainContent">
  <div class="breadcrumb-line hidden-print">
    <div class="breadcrumb">
      Home / State laws and rules / RCWs / Title T / Chapter T.CC / Section T.CC.SSS
    </div>
  </div>
  <div class="main-page-wrapper">
    <div class="main-page-content">
      <div class="title-and-sidebar-wrapper">
        <div id="ContentPlaceHolder1_pnlTitleBlock" class="title-block">
          <div class="title-wrapper">
            <a id="ContentPlaceHolder1_lnkTitlePdf" href="default.aspx?cite=T.CC.SSS&pdf=true">PDF</a>
            <h1><!-- field: Citations -->RCW  T.CC.SSS<!-- field: --></h1>
          </div>
          <h2><!-- field: CaptionsTitles -->Section caption.<!-- field: --></h2>
        </div>
      </div>
      <div id='contentWrapper' class='section-page'>
        <div></div><div></div>           <!-- two empty placeholder divs; parser must skip -->
        <div>
          <div style="text-indent:0.5in;">(1) First-level subsection text.</div>
          <div style="text-indent:0.5in;">(a) Sub-paragraph.</div>
          <div style="text-indent:0.5in;">(2) Second top-level subsection.</div>
        </div>
      </div>
      <div style="margin-top:15pt;margin-bottom:0pt;">
        [ <a href="...session law PDF...">2017 c 303 s 3</a>;
          <a href="...">2010 c 69 s 2</a>;
          ... ;
          Formerly RCW <a href="...cite=42.17.320">42.17.320</a>.]
      </div>
      <div style="margin-top:0.25in;margin-bottom:0.25in;"><h3>Notes:</h3></div>
      <div style="margin-bottom:0.2in;">
        <div style="text-indent:0.75in;">
          <span style="font-weight:bold;">Finding<span style="font-family:times new roman;">—</span>2010 c 69:</span>
          "Note text in quotes." [ <a>session-law link</a>.]
        </div>
      </div>
      <div id="ContentPlaceHolder1_pnlExpanded">...possibly empty...</div>
    </div>
  </div>
</div>
```

**Parser anchors (per-section view):**
- Citation: `<h1><!-- field: Citations -->RCW  T.CC.SSS<!-- field: --></h1>` — note double space and the HTML-comment field markers (likely an XSL artifact). Both serve as parser anchors.
- Heading: `<h2><!-- field: CaptionsTitles -->...<!-- field: --></h2>` — same field-comment markers.
- Body container: `<div id='contentWrapper' class='section-page'>`.
- Body paragraphs: `<div style="text-indent:0.5in;">` — flat list; no nesting. Subsection level (`(1)`, `(a)`, `(i)`) must be parsed from the leading text token, NOT from indent depth (indent is uniform 0.5in regardless of subsection level).
- Source credits: sibling div with inline-style `margin-top:15pt;margin-bottom:0pt;` containing `[ ... ]` brackets with linked session-law cites and (when applicable) a `Formerly RCW X.YY.ZZZ` recodification-source.
- Notes block opener: `<h3>Notes:</h3>` inside `<div style="margin-top:0.25in;margin-bottom:0.25in;">`.
- Note entries: `<div style="margin-bottom:0.2in;"><div style="text-indent:0.75in;"><span style="font-weight:bold;">[Note-type heading]:</span> "[note body]" [ <a>session law</a>.]</div></div>`.
- `<div id="ContentPlaceHolder1_pnlExpanded">` is a container that may hold further expanded annotations; can be empty.

### 2.3 Per-chapter (`&full=true`) page anatomy

```html
<!-- chapter title block -->
<h1><!-- field: Citations -->Chapter  T.CC RCW<!-- field: --></h1>
<h2><!-- field: CaptionsTitles --><div>CHAPTER HEADING</div><!-- field: --></h2>

<!-- Sections list (table of contents), then... -->

<!-- For each section, in order: -->
<a name='T.CC.SSS' ></a>
<div>
  <h3>
    <a class="btn btn-outline hidden-print" href="...&pdf=true">PDF</a>
    RCW  <a href='...cite=T.CC.SSS'>T.CC.SSS</a>
  </h3>
</div>
<div><h3>Section caption.</h3></div>
<div>
  <div style="text-indent:0.5in;">(1) Body text...</div>
  ...
</div>
<div style="margin-top:15pt;margin-bottom:0pt;">[ session-law citations ]</div>
<div style="margin-top:0.25in;margin-bottom:0.25in;"><h3>NOTES:</h3></div>   <!-- note: ALL CAPS in chapter view -->
<div style="margin-bottom:0.2in;">
  <div style="text-indent:0.75in;">
    <span style="font-weight:bold;">Note-type heading:</span> "..."
  </div>
</div>
<!-- next section... -->
```

**Parser anchors (chapter `&full=true` view):**
- **Section boundary**: `<a name='T.CC.SSS'></a>` — the canonical splitter. Each section's content is everything between this anchor and the next `<a name='...'></a>` (or end of chapter content area).
- Section citation: `<h3>...RCW <a href='...cite=T.CC.SSS'>T.CC.SSS</a></h3>` — first `<h3>` after the anchor.
- Section caption: `<h3>Caption.</h3>` — second `<h3>` after the anchor.
- Body, source credits, notes: same DOM patterns as per-section view, but `Notes:` heading is rendered `NOTES:` (all caps) in the chapter view. Parser must accept both spellings.

### 2.4 Subsection-numbering note

Subsections use flat indent (`text-indent:0.5in`) regardless of nesting depth. The parser must recognize subsection markers from leading text tokens:

- Top-level: `(1)`, `(2)`, `(3)`, ...
- Second-level: `(a)`, `(b)`, `(c)`, ...
- Third-level: `(i)`, `(ii)`, `(iii)`, ... (Roman lowercase)
- Combined notation: `(3)(a)` for direct cross-reference within a single paragraph

The chunking strategy from the kickoff doc (whole-section default, subsection-split if >4000 tokens) remains correct. Just don't try to infer hierarchy from indentation.

### 2.5 Cross-reference link patterns

| Link form (in source body or notes) | Target |
|---|---|
| `<a href='http://app.leg.wa.gov/RCW/default.aspx?cite=T.CC.SSS'>` | Internal RCW section |
| `<a href='http://app.leg.wa.gov/RCW/default.aspx?cite=T.CC'>` | Internal RCW chapter |
| `<a href='http://app.leg.wa.gov/RCW/default.aspx?cite=T'>` | Internal RCW title |
| `<a href='https://lawfilesext.leg.wa.gov/biennium/YYYY-YY/.../Bills/Session Laws/...SL.pdf?cite=YYYY c NN s S'>` | Modern session-law PDF (1995+) |
| `<a href='https://leg.wa.gov/CodeReviser/documents/sessionlaw/YYYYcN.pdf?cite=YYYY c NN s S'>` | Older session-law PDF (pre-1995) |
| `<a href='https://leg.wa.gov/CodeReviser/Pages/session_laws.aspx?cite=YYYY p P s S'>` | Pre-statehood session-law landing |

Phase 3 cross-reference parser (writing to `rcw.cross_references`):
- RCW T.CC.SSS → `target_jurisdiction='state-wa-rcw'`, target_citation=`RCW T.CC.SSS`
- 42 U.S.C. § 12112 / 28 U.S.C. § 1331 → `target_jurisdiction='federal-usc'` (catch the `T U.S.C. § S` pattern in plain text)
- Chapter X.XX RCW (without dot-section) → `target_jurisdiction='state-wa-rcw'`, target_citation=`chapter X.XX RCW`
- Title X RCW → similarly
- Session-law links are not cross-references between sections; they are stored separately in `rcw.section_history` (see §2.7 below).

### 2.6 Source-credit format (the `[ ... ]` brackets)

Always at the end of section body, inside its own div with inline-style `margin-top:15pt;margin-bottom:0pt;`. Format:

```
[ {SessionLawLink1}; {SessionLawLink2}; ... ; Formerly RCW {prior-citation}. ]
```

Where each session-law citation has the form:
- Recent: `2017 c 303 s 3` (linked to the SL.pdf in lawfilesext.leg.wa.gov)
- Mid: `1995 c 397 s 15`
- Older: `1973 c 1 s 32 (Initiative Measure No. 276, approved November 7, 1972)` (parenthetical for ballot measures)
- Old: `Code 1881 s 1`, `RRS s 143` (Remington's Revised Statutes — pre-1881 codification)
- Territorial: `1862 p 83 s 1` (page-based, not chapter-based)

Phase 3 parser (writing to `rcw.section_history`):
- `session_law` field stores raw text, e.g. `'2017 c 303 s 3'`
- `session_year` field extracted as integer (1862, 1881, 2017, etc.)
- `action_type` field: `'enacted'` for the oldest cite, `'amended'` for subsequent cites (heuristic — use the `Code 1881` / RRS / pre-1900 cites as enacted markers)
- `action_summary` field: free-form for `(Initiative Measure No. ...)` and similar parenthetical context
- `Formerly RCW X.YY.ZZZ` is a recodification source, stored separately (sets `rcw.sections.is_recodified=FALSE` on the *current* citation but with a `recodified_from` semantic in metadata; the *prior* citation gets marked `is_recodified=TRUE` with `recodified_to=current` if/when we Phase 7+ ingest the prior-version archive)

### 2.7 Notes block taxonomy

Note-type vocabulary observed across the seven section samples:

| Note heading | Semantic | Frequency |
|---|---|---|
| `Reviser's note:` | Code Reviser's editorial annotation (typos, recodification context, reference fixes) | very common |
| `Findings—{year c chap}:` | Legislative findings preamble | common (heavily-amended sections) |
| `Effective date—{year c chap}:` | Effective-date provision separate from session-law cite | common |
| `Construction—{year c chap}:` | Statutory construction directive | common |
| `Conflict with federal requirements—{year c chap}:` | Federal preemption / conformity clause | common in Title 70/74/49 (federal grant conditions) |
| `Expiration date—{year c chap}:` | Sunset clause | occasional |
| `Severability—{year c chap}:` | Severability clause | common |
| `Short title—{year c chap}:` | Short-title declaration | common (act-naming chapters) |
| `Application—{year c chap}:` | Application/applicability scope | occasional |
| `Finding—{year c chap}:` | Singular finding | less common variant of `Findings—` |
| `Legislative findings—{year c chap}:` | Same as Findings, alternate spelling | observed |
| `Purpose—{year c chap}:` | Stated purpose of an amendment | common |

These become the `note_type` values in `rcw.section_notes`. The list is non-exhaustive (this catalog is from 7 samples spanning Titles 4, 9A, 26, 42, 49, 51); Phase 3 parser should use a permissive enum (i.e., capture whatever bold heading appears before the `:` colon) and let unexpected types accumulate as data, then audit at the end of priority-core ingest.

The em-dash separator inside the bold span uses an explicit Times-New-Roman span: `<span style="font-family:times new roman;">—</span>`. Parser must normalize to a plain `—` (U+2014).

---

## 3. Section-Status Taxonomy

Repealed/recodified/reserved sections are NOT present in the live `app.leg.wa.gov/RCW/` HTML — only currently-effective sections are rendered. Status information lives on the dispositions pages (`dispo.aspx`).

### 3.1 Disposition-page taxonomy

Verified on `dispo.aspx?Cite=42.17` (RCW 42.17 was largely recodified to 42.56 in 2005 via 2005 c 274). Status sentence patterns:

| Sentence pattern | Semantic | Schema flag |
|---|---|---|
| `Recodified as RCW {target} pursuant to {session-law} § N, effective {date}.` | Section moved to new citation; text preserved at target | `is_recodified=TRUE`, `recodified_to={target}` |
| `Repealed by {session-law} § N.` | Section repealed, no replacement | `is_repealed=TRUE` |
| `Repealed by {session-law} § N, effective {date}.` | Same with explicit effective-date | `is_repealed=TRUE`, `effective_date={date}` |
| `Repealed by {sl-A}; and repealed by {sl-B}.` | Compound repeal (multiple session laws repealed the same section) | `is_repealed=TRUE`, history rows for each |
| `Repealed by {session-law} § N, effective {date}. Later enactment, see RCW {newcite}.` | Repealed; new section enacted with similar subject matter | `is_repealed=TRUE`, history note with later-enactment pointer |
| `Reserved.` | Section number reserved, no current text | `is_recodified=FALSE`, `is_repealed=FALSE`, but `full_text=NULL`-equivalent — handle as "stub" record or skip |
| (not observed but anticipated) `Decodified by {session-law}.` | Removed from RCW per legislative direction (rare) | `is_decodified=TRUE` |
| (not observed but anticipated) `Expired {date}.` | Sunset clause fired | `is_expired=TRUE` |

**Phase 4 disposition-metadata ingest:** for each chapter ingested, also fetch the chapter's `dispo.aspx?Cite=N.NN` page. Parse out section-status rows. Populate `rcw.sections` rows with the status flag set and the cross-reference target. Do NOT populate `full_text` for repealed/recodified sections in Phase 4 — defer prior-version text capture to a Phase 7+ archive sweep that fetches certified PDFs from `lawfilesext.leg.wa.gov/law/RCWArchive/YYYY/`.

This means `rcw.sections` will have rows of three flavors:
1. **Live current section**: `is_repealed=FALSE`, `is_recodified=FALSE`, full body text in `full_text`. Embedded into chunks. (~45,000 rows.)
2. **Recodified-out ghost**: `is_recodified=TRUE`, `recodified_to` set, `full_text=NULL` (or short pointer like `"Recodified as RCW {target}"`), no embeddings. Allows citation lookups against historical RCW citations to redirect to the current citation. (Estimated thousands of rows.)
3. **Repealed ghost**: `is_repealed=TRUE`, `full_text=NULL`, no embeddings. Allows litigation involving conduct under prior law to confirm the prior citation existed and report the repealing session law. (Estimated thousands of rows.)

Mark the schema design accordingly: the `rcw.sections.full_text` column is `NOT NULL` per the Phase 1 draft — change to `TEXT NULL` (with constraint that live sections have non-null text) so ghost rows can exist.

### 3.2 Recodification cross-walk

The `comparison.aspx?cite=N.NN` page (linked from each chapter's dispo page when the chapter has been substantially recodified) provides an old↔new citation cross-walk table. For RCW 42.17 → 42.56, this is the canonical mapping. Phase 4 should fetch and parse these comparison tables for any chapter with recodification entries — the parsed rows go into `rcw.cross_references` with `ref_type='recodification'` and into a per-section `recodified_to` set on the ghost rows.

### 3.3 Lettered-suffix chapter / section variants

WA RCW uses lettered suffixes for both titles and chapters when new material is inserted between existing numerals:

- Lettered titles: `9A`, `23B`, `28A`, `28B`, `28C`, `29A`, `29B`, `30A`, `30B`, `35A`, `50A`, `50B`, `62A`, `70A`, `71A`, `79A` — 16 lettered titles.
- Lettered chapters within a title: e.g., `RCW 4.24A`, `RCW 70A.560` (subdivision in Title 70A), `RCW 11.96A`, `RCW 41.04A`. Common pattern: chapter number + uppercase letter.
- Lettered sections: `RCW 9A.04.020` (chapter has a letter, sections do not — but cf. `RCW 35A.02.020`).

**Schema implication:** `rcw.titles.title_number TEXT` (not INTEGER), `rcw.chapters.chapter_number TEXT`, `rcw.sections.section_number TEXT`. The Phase 1 draft already specifies TEXT — confirmed correct.

---

## 6. Volume Sanity Check

### 6.1 Title census

100 titles enumerated from the master list at `app.leg.wa.gov/RCW/default.aspx`:

```
1, 2, 3, 4, 5, 6, 7, 8, 9, 9A, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
21, 22, 23, 23B, 24, 25, 26, 27, 28A, 28B, 28C, 29A, 29B, 30A, 30B, 31,
32, 33, 34, 35, 35A, 36, 37, 38, 39, 40, 41, 42, 43, 44, 46, 47, 48, 49,
50, 50A, 50B, 51, 52, 53, 54, 55, 57, 58, 59, 60, 61, 62A, 63, 64, 65,
66, 67, 68, 69, 70, 70A, 71, 71A, 72, 73, 74, 76, 77, 78, 79, 79A, 80,
81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91
```

Title-number gaps (45, 56, 62, 75) are intentional — those numbers were either never assigned or have been completely repealed/recodified out.

### 6.2 Chapter census

All 100 title TOCs fetched (1 req/sec, 100 sec wall-time, all 200 OK). Chapter-link count by title:

```
Top 10 largest titles by chapter count:
  Title 43 (State Government—Executive)         178 chapters
  Title 18 (Businesses and Professions)          113
  Title 19 (Business Regulations Misc.)          111
  Title 48 (Insurance)                           101
  Title 70 (Public Health and Safety)             98
  Title 36 (Counties)                             97
  Title 35 (Cities and Towns)                     91
  Title 70A (Environmental Health and Safety)     87
  Title 28A (Common School Provisions)            76
  Title 82 (Excise Taxes)                         62

Litigation-priority titles:
  Title 51 (Industrial Insurance)                  14 chapters
  Title 42 (Public Officers and Agencies)          20
  Title 4  (Civil Procedure)                       29
  Title 49 (Labor Regulations)                     40
  Title 41 (Public Employment, Civil Service)      37
  Title 2  (Courts of Record)                      27
  Title 9  (Crimes and Punishments)                48
  Title 9A (Washington Criminal Code)              32
  Title 10 (Criminal Procedure)                    52
  Title 70 (Public Health and Safety)              98
  Title 71 (Mental Illness)                        13
  Title 71A (Developmental Disabilities)           10
  Title 74 (Public Assistance)                     41
  Title 7  (Special Proceedings and Actions)       43
  Title 11 (Probate and Trust Law)                 52
  Title 26 (Domestic Relations)                    26

TOTAL: 2,763 chapters across 100 titles
```

**Operator note:** 2,763 chapters is ~3.5× the kickoff doc's 600–800 estimate. This is **not** a parser-scope error — the count is dominated by mega-titles (Titles 43, 18, 19, 48, 70, 36, 35, 70A, 28A account for 1,066 chapters / 39% of total). Litigation-priority titles are individually small (Title 51 has 14 chapters, Title 42 has 20). The kickoff estimate appears to have anchored on litigation-relevant titles rather than the long tail.

### 6.3 Section census (estimated)

Random sample of 30 chapters drawn uniformly from the 2,763 (seeded RNG, deterministic) and fetched at 1 req/sec:

```
Random-sample chapter section counts (n=30):
  min   = 1   sections
  max   = 89  sections (RCW 13.34, Juvenile Court Act)
  mean  = 16.3 sections per chapter
  median ≈ 11 sections per chapter (skewed-right distribution)
  stddev ≈ 19 (heavy right tail)
```

Combined with the seven litigation-priority chapter samples (mean = 37.4, n=7, biased toward big chapters), the random sample gives the trustworthy estimate.

**Projected total sections: 2,763 × 16.3 ≈ 45,000 sections (range 35,000 – 55,000 with sample variance).**

Squarely within the kickoff doc's 35,000–50,000 range. **No halt trigger.**

### 6.4 Chunk-count and ingest-time projection

- Mean section length: not measured directly in Phase 0; using USC empirical baseline of ~2,000 tokens/section avg → ~12 chunks/section avg if we keep the kickoff chunking strategy (whole-section default, subsection-split >4000 tokens).
- 45,000 sections × 12 chunks = ~540,000 chunks
- BGE-M3 throughput from USC empirical: 125 chunks/min sustained
- 540,000 / 125 = **4,320 minutes ≈ 72 hours of embedding wall-time**

Caveat: that is materially longer than the kickoff doc's 7–10 hour estimate. Two reconcilers:
1. RCW sections are typically *shorter* than USC sections (state statutes are often single-purpose paragraphs; USC often packs whole regulatory regimes into one section). If RCW averages 800 tokens/section, chunks/section drops to ~5 and total chunks → ~225,000 → ~30 hours. Need to measure once Phase 4 starts.
2. The USC throughput baseline (125 chunks/min) was achieved with `embedding_model_id=2` (BGE-M3) batched at 64 — same configuration applies. If GPU resources are shared, real throughput may be lower.

**Operator note:** the 7–10 hour wall-time estimate in the kickoff doc looks optimistic. Realistic ceiling for Phase 4 priority-core (Titles 51 + 42 + 4 + 49 = 103 chapters × 16.3 ≈ 1,680 sections × ~10 chunks = ~17,000 chunks) is ~2.5 hours of embedding time. The full long-tail ingest is the long pole and could be 24–48+ hours wall-time. Plan Phase 4 pause-checkpoints assuming this longer tail.

### 6.5 Fetch-time projection

| Phase 2 acquisition | Fetch unit | Count | Wall-time @ 1 req/sec |
|---|---|---|---|
| Title TOCs (already done in Phase 0) | title page | 100 | done (1.7 min) |
| Chapter `&full=true` (the bulk download) | chapter page | 2,763 | **46 min** |
| Chapter dispositions (`dispo.aspx?Cite=N.NN`) | chapter dispo | 2,763 | 46 min |
| Chapter recodification comparisons (subset) | comparison.aspx | ~50 (chapters with recodifications) | 1 min |
| **Total Phase 2 fetch wall-time** | | | **~95 min ≈ 1.5 hr** |

This is well under the 4-hour ceiling implied by the rate-limit discipline.

---

## Summary: changes from the original kickoff plan

1. **Acquisition unit** changes from per-section to per-chapter using `&full=true`. ~50× fewer fetches; full corpus fetch in ~1.5 hr instead of ~14 hr.
2. **Justia secondary source** replaced by the Code Reviser's certified per-section PDFs and annual archive PDFs. Justia is Cloudflare-locked and not viable.
3. **`rcw.sections.full_text`** changes from `NOT NULL` to `NULL`-able to accommodate ghost rows for repealed/recodified sections (status metadata captured but prior-version text deferred to Phase 7+ archive sweep).
4. **Disposition pages** (`dispo.aspx?Cite=N.NN`) are a required Phase 2 fetch alongside the chapter `&full=true` pages — they are the only place repealed/recodified-out sections appear in the live system.
5. **Note-type taxonomy** for `rcw.section_notes` should be a permissive `TEXT` rather than a strict enum — observed types (Reviser's note, Findings, Effective date, Construction, Conflict with federal requirements, Expiration date, Severability, Short title, Application, Finding, Legislative findings, Purpose) are non-exhaustive and adopting a permissive parser lets unexpected types accumulate as data for later audit.
6. **Total chapter count** is 2,763 (not 600–800). Section count estimate 45,000 is in range.
7. **Phase 4 ingest wall-time** is more likely 24–48+ hours than 7–10. Build Phase 4 pause-checkpoints around section milestones (priority-core = ~2.5 hr at the BGE-M3 throughput baseline), not raw clock time.
8. **Publication date** for the current corpus is `2025-08-15` (per the RCW landing page's `Last update:` marker). Use this as the binding `publication_date` for all Phase 4 sections.

End of source-format findings. Awaiting operator review before Phase 1 schema-design proposal.
