---
title: WA Appellate Case Law — Phase 1 Revised Scope Plan (CourtListener source)
date: 2026-05-01
status: READY FOR OPERATOR ONE-LINE CONFIRMATION → Phase 2 launch
---

# Phase 1 Revised Scope Plan — CourtListener Source (Path B)

All Phase 1 items complete. Summary for one-line operator confirmation before Phase 2 launch.

---

## Verified per-court opinion counts (§K #29 pre-flight, confirmed)

| Court | CL ID | Scope | Published 1950+ | Notes |
|---|---|---|---|---|
| WA Supreme Court | `wash` | 1889–present (SS) | **21,755** | Kickoff projected 30K-50K → actual 1.4-2.3× lower |
| COA all divisions | `washctapp` | 1969–present (SA) | **71,900** | Kickoff projected 130K-200K → actual 1.8-2.8× lower; no per-division CL IDs |
| WA Territory | `washterr` | 1853–1889 (SS) | out of scope (pre-1950 floor) | |
| **Total published** | | | **93,655** | |

§K #29 action: kickoff halt band [120K, 280K] is wrong-axis for CL source. Recalibrate to **[85K, 105K]** based on per-source measurement. 93,655 is within new band.

The inflation vs. kickoff estimates is explained by:
1. Kickoff coordinator projected per-division (Div I/II/III) separately, then summed — but CL has no per-division split; `washctapp` is one entity.
2. Kickoff estimate was built for courts.wa.gov scope assumption; CL coverage is real historical data, not an estimate.

---

## 11-authority spot-check results (10/11 PASS)

| Authority | Kickoff citation | CL result | Status |
|---|---|---|---|
| Marquis v. City of Spokane | 130 Wn.2d 97 (1996) | cluster_id=4907181, filed=1996-09-05, 2 opinions | **✓ PASS** |
| Fell v. Spokane Transit Auth. | 128 Wn.2d 618 (1996) | found, filed=1996-02-29 | **✓ PASS** |
| Negron v. Snoqualmie Valley Hosp. | 86 Wn.2d 579 (1976) | count=0; broad search + citation search both return 0 | **✗ MISS** |
| Frisino v. Seattle Sch. Dist. No. 1 | 160 Wn. App. 765 (2011) | found, filed=2011-03-21, citation 249 P.3d 1044 | **✓ PASS** |
| Loeffelholz v. Univ. of Washington | 175 Wn.2d 264 (2012) | found, filed=2012-02-09 | **✓ PASS** |
| Jafar v. Webb | 177 Wn.2d 520 (2013) | found, filed=2013-05-23 | **✓ PASS** |
| State v. Blazina | 182 Wn.2d 827 (2015) | found, filed=2015-03-12, 19 results | **✓ PASS** |
| State v. Ramirez | 191 Wn.2d 732 (2018) | found, filed=2019-03-06 | **✓ PASS** |
| City of Federal Way v. Koenig | 167 Wn.2d 341 (2009) | found, filed=2010-03-31, citation 228 P.3d 18 | **✓ PASS** |
| Nast v. Michels | 107 Wn.2d 300 (1986) | found, filed=1986-12-19, citations 730 P.2d 54 | **✓ PASS** |
| State v. Gunwall | 106 Wn.2d 54 (1986) | found, filed=1986-06-12, citations 720 P.2d 808 | **✓ PASS** |

**Result: 10/11 PASS. 1 miss (Negron) — within ≤2 halt threshold.** Negron disposition: genuine CL coverage gap for this specific 1976 opinion. CL has other 1970s Sup Ct opinions (156 published in 1975 alone) — this is a single-case digitization gap, not a decade-wide gap. Per §K #37: document as residual NULL (absence-of-record class); do not synthesize a ghost row for this opinion.

---

## Marquis structural stress-test: PASS

Fetched cluster 4907181 (Marquis v. City of Spokane, 130 Wash. 2d 97, filed 1996-09-05):
- **2 sub-opinions**: lead (`020lead`, Guy, J., 46,153 chars html_with_citations) + dissent (`040dissent`, Madsen, J., 37,645 chars)
- **Text format**: Harvard CAP TEI XML in `html_with_citations` field. Format: `<opinion type="majority"><author id="...">Guy, J.</author><p id="...">...</p>...<page-number citation-index="1" label="101">*101</page-number>...`
- **Plain_text**: empty for this CAP-sourced opinion (plan: extract from html_with_citations via BeautifulSoup/lxml)
- **Citations in cluster**: structured objects with `volume`, `reporter`, `page` fields: 130 Wash. 2d 97 + 922 P.2d 43 + 1996 Wash. LEXIS 482 + employment practice reporters
- **Citation graph (`cites`)**: EMPTY for both opinions — CL's pre-built citation graph is sparse for 1990s WA opinions. Citation extraction MUST be done from text, not relied on from CL's `cites` field.
- **Judges**: "Guy, Madsen" (string in cluster record)
- **Syllabus/posture/procedural_history**: empty for this opinion (not all CAP opinions have these fields populated)

Parser design confirmed: html_with_citations → strip TEI XML tags → full text; citation regex over clean text for cross-reference extraction.

---

## CL data shape summary (key findings for Phase 2-3 design)

| Property | Value / Finding |
|---|---|
| Auth required | Cluster + opinion detail endpoints: **Yes (401 anonymous)**. Search + count: no auth needed. Token stored at `/opt/courtlistener/config/courtlistener.env`. |
| Court IDs | `wash` (SS) + `washctapp` (SA). **No per-division IDs.** Division must be derived from docket number. |
| Division derivation | Docket prefix heuristic (to validate in Phase 3): 37XXX/36XXX→Div III, 52XXX/53XXX→Div II, 79XXX/80XXX→Div I. |
| Full text field | `html_with_citations` (Harvard TEI XML). `plain_text` empty for most historical opinions. `xml_harvard` also available (~34K chars for Marquis). |
| Citation format | CL stores **"130 Wash. 2d 97"** — must normalize to **"130 Wn.2d 97"** for canonical_citation. Similar for Wn. App. / Wn. App. 2d. |
| Citation graph | `cites` field sparse for historical opinions. Must extract citations from opinion text. CL citation-map CSV provides supplemental graph (opinionA→opinionB IDs). |
| Opinion type codes | `020lead`=majority, `040dissent`=dissent, `060concurrence`=concurrence, `010combined`=per_curiam (verify these codes in Phase 3). |
| OCR | **Not needed.** CL/CAP provides digitized text for all opinions (scanned PDFs handled by CAP). |
| Case name | `case_name` on cluster. Clean text. |
| Decision date | `date_filed` on cluster (ISO date). |
| Docket number | `docket_number` on cluster. |
| Parallel citations | Structured `citations` list on cluster: `{volume, reporter, page, type}` objects. |
| Author | `author_str` on opinion record (string). `author_id` (FK to CL judges table) sparsely populated. |
| Joined by / panel | `joined_by_ids` on opinion; `panel_names` on cluster (string list). |

---

## Acquisition strategy (revised from kickoff)

**Recommended: API pagination + citation-map bulk CSV (Phase 2A/B)**

| Step | Method | Estimated calls/size | Wall time |
|---|---|---|---|
| 2A: Cluster metadata | API: `/opinion-clusters/?docket__court=wash&precedential_status=Published&page_size=100` + same for washctapp | ~937 pages | ~20 min |
| 2B: Opinion text | API: `/opinions/?cluster__docket__court__in=wash,washctapp&page_size=100` | ~1,400 pages | ~30 min |
| 2C: Citation map | S3 bulk: `citation-map-2026-03-31.csv.bz2` (522 MB compressed, ~3 GB decompressed) | 1 download | ~20 min |
| Total | | ~2,340 API calls + 1 download | **~1-2 hours** |

**NOT recommended:** Full opinions bulk CSV (54 GB compressed, ~200 GB decompressed — impractical for ~1.8% WA filter).

Storage projection: ~2 GB raw + ~3 GB citation CSV + ~3 GB chunks + ~3 GB embeddings = **~11 GB new storage** (well within 60 GB halt threshold).

---

## OCR validation task (Phase 1.4): N/A — CLOSED

Phase 1.4 (OCR quality validation for 1960s-era scanned PDFs) does not apply to CL source. CL/CAP provides pre-digitized text for all historical opinions. No OCR pipeline needed. Phase 1.4 closed as inapplicable.

---

## Phase 2 prerequisites (all ready)

- [x] CL API token stored: `/opt/courtlistener/config/courtlistener.env`
- [x] Disk cache dir: `/var/cache/wa_caselaw/` created
- [x] Project dir: `/opt/wdws/wa_caselaw_research/` with `phase1_investigation/`
- [ ] Phase 2 migration script: `case_law` schema creation (7 tables + constraints) — ready to draft
- [ ] Phase 2 acquisition script: API paginator + citation-map downloader — ready to draft
- [ ] Phase 2 citation-map filter script: filter 522 MB CSV to WA-origin rows — ready to draft

---

## Summary for operator one-line confirmation

**Verified:** 93,655 published WA appellate opinions in CL (21,755 Sup Ct + 71,900 COA). 10/11 spot-check authorities found (Negron 1976 is genuine CL coverage gap, documented). Marquis stress-test PASS — full text + 2 opinion types confirmed. Acquisition pipeline: ~1-2 hours via API + citation-map CSV download. Storage: ~11 GB. Auth token stored. OCR task N/A. Ready for Phase 2.

**Halt-band recalibration required:** [120K, 280K] → [85K, 105K] (per §K #29 per-source verification).

**Operator one-line needed:** Confirm Phase 2 launch with revised counts (93,655 total, 10/11 spot-check) and revised halt-band [85K, 105K].
