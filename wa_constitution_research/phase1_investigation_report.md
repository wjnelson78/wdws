# WA Constitution Phase 1 Investigation Report

**Date:** 2026-05-01
**Project:** WA Constitution Ingest Agent
**Status:** Phase 1 complete; Phase 2 launch awaits operator scope confirmation.
**Methodology canon inherited:** §K #1-#39 via 4 prior project canons (refinement_sweep canon path cited in kickoff missing; intact via WCR canon).

---

## Section 1 — Pre-flight verification (independently confirmed)

| Item | Kickoff value | Verified value | Status |
|---|---|---|---|
| `wa_constitution` schema present | absent | absent (0 rows in pg_namespace) | ✓ match |
| rcw.sections frozen baseline | 58,529 | 58,529 | ✓ match |
| usc.sections frozen baseline | 61,133 | 61,133 | ✓ match |
| wac.sections frozen baseline | 73,180 | 73,180 | ✓ match |
| wa_court_rules.rules frozen baseline | 1,286 | 1,286 | ✓ match |
| wac.authority_chain @ RCW resolved | 83,752 | 83,752 | ✓ match |
| Cross-corpus refs to wa-constitution | 54 (rcw=34, wac=16, wcr=4) | 54 (rcw=34, wac=16, wcr=4) | ✓ match |
| Citation form split | 51 article-only + 3 section-form | 51 article-only + 3 section-form (3 wcr refs all to art. IV § 31; 1 wcr ref article-only to art. IV) | ✓ match |
| Articles referenced | I, II, III, IV, VI, VII, VIII, IX, XI, XV, XVI | confirmed (RCW: 10 articles; WAC: 6 articles; WCR: 1 article — combined 11 distinct) | ✓ match |

**Pre-flight conclusion:** all coordinator-supplied pre-flight values are
empirically accurate. Schema absence + frozen baselines + cross-ref demand
all match exactly. Strong inheritance signal.

---

## Section 2 — Source acquisition findings

### 2.1 Primary source URL — stale in kickoff

| Resource | Kickoff URL | Verified URL | Status |
|---|---|---|---|
| Constitution landing | `https://leg.wa.gov/lawsandagencyrules/Pages/constitution.aspx` | (404) | ❌ stale |
| Constitution landing — actual | (none cited) | `https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/` | ✓ verified |
| Per-article pages | (none cited) | `https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/?section={GUID}` | ✓ verified — opaque GUID query string |
| Amendment landing | (cited as cross-check via SOS) | `https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/?section=dbfbe753-d62b-45a4-b6a5-92322f340d19` | ✓ verified — 110 amendments listed |
| SOS cross-check | `https://www.sos.wa.gov/elections/initiatives/constitution.aspx` | HTTP 402 from this environment | ❌ inaccessible |

**§K #25 reinforcement** (documented in `phase6_retrieval_guide_notes.md`):
two kickoff structural-currency misses surfaced — URL location and
Article XX repeal status. Per the audit channel, treat coordinator
structural premises as hypotheses to verify; both verifications saved
production runs from launching against stale ground.

### 2.2 Article enumeration — 32 articles confirmed

| # | Roman | Title | Section count | Letter-suffix entries | Repealed |
|---|---|---|---:|---|---|
| 1 | I | Declaration of Rights | 35 | — | — |
| 2 | II | Legislative Department | 43 | § 1(a-e), § 3(a) [subsections] | — |
| 3 | III | The Executive | 25 | § 2(a) [unconfirmed subsection vs standalone] | — |
| 4 | IV | The Judiciary | 31 | § 2(a), § 3(a) [confirmed STANDALONE — distinct headings] | — |
| 5 | V | Impeachment | 3 | — | — |
| 6 | VI | Elections and Elective Rights | 8 | § 1A [STANDALONE, but repealed] | § 1A, § 2 |
| 7 | VII | Revenue and Taxation | 12 | § 2(a)(b) [subsections] | — |
| 8 | VIII | State, County, Municipal Indebtedness | 11 | § 1(a-l) [subsections] | — |
| 9 | IX | Education | 5 | § 3(a) [unconfirmed] | — |
| 10 | X | Militia | 6 | — | — |
| 11 | XI | County, City, Township Organization | 16 | — | — |
| 12 | XII | Corporations Other Than Municipal | 22 | — | § 14 |
| 13 | XIII | State Institutions | 1 | — | — |
| 14 | XIV | Seat of Government | 3 | — | — |
| 15 | XV | Harbors and Tide Waters | 3 | — | — |
| 16 | XVI | School and Granted Lands | 6 | — | — |
| 17 | XVII | Tide Lands | 2 | — | — |
| 18 | XVIII | State Seal | 1 | — | — |
| 19 | XIX | Exemptions | 1 | — | — |
| 20 | XX | Public Health and Vital Statistics | 2 | — | — (NOT REPEALED — kickoff anomaly note wrong) |
| 21 | XXI | Water and Water Rights | 1 | — | — |
| 22 | XXII | Legislative Apportionment | 2 | — | — |
| 23 | XXIII | Amendments | 3 | — | — |
| 24 | XXIV | Boundaries | 1 | — | — |
| 25 | XXV | Jurisdiction | 1 | — | — |
| 26 | XXVI | Compact with the United States | 4 | — | — |
| 27 | XXVII | Schedule | 19 | — | § 13 |
| 28 | XXVIII | Compensation of State Officers | 1 | — | — |
| 29 | XXIX | Investments of Public Pension and Retirement Funds | 1 | — | — |
| 30 | XXX | Compensation of Public Officers | 1 | — | — |
| 31 | XXXI | Sex Equality – Rights and Responsibilities | 2 | — | — |
| 32 | XXXII | Special Revenue Financing | 1 | — | — |

**Totals:**
- Parent-numbered sections: **273**
- Confirmed letter-suffix standalone sections (distinct headings): **3** (IV § 2(a), IV § 3(a), VI § 1A — last is repealed)
- Letter-suffix entries pending standalone-vs-subsection disambiguation: **2** (III § 2(a), IX § 3(a))
- Likely-subsection letter-suffix entries (no distinct heading): Art II § 1(a-e), § 3(a); Art VII § 2(a)(b); Art VIII § 1(a-l)
- Repealed parent-numbered sections: **3** (VI § 2, XII § 14, XXVII § 13)
- Total expected row count: **276-278** (273 parent + 3-5 standalone letter-suffix)

### 2.3 Amendment enumeration

| Item | Verified |
|---|---|
| Total amendment count | 110 (kickoff projected ~110) ✓ |
| Per-amendment metadata | Each numbered, cross-referenced to article + section affected |
| Ratification dates on landing page | NOT visible via WebFetch summarization (LLM-truncated per §K #8) |
| Acquisition implication | Phase 2 amendment metadata extraction requires direct HTML fetch + per-amendment parse |
| Most-recent amendment observed | Amendment 110 (2025) — modified Art XXIX investments |
| Schema fit | wa_constitution.amendments per kickoff design — adequate; affects_articles + affects_sections fields confirmed appropriate |

---

## Section 3 — Operationally-active sections (per §K #34, kickoff Section "Operational relevance")

| Active matter | Section | Verified present | Heading | Notes |
|---|---|---|---|---|
| WLAD due-process anchor | Art I § 3 | ✓ | "Personal Rights" | "No person shall be deprived of life, liberty, or property..." |
| WLAD privileges/immunities | Art I § 12 | ✓ | "Special Privileges and Immunities Prohibited" | "No law shall be passed granting to any citizen..." |
| Post-Grants-Pass cruel-punishment (Gunwall) | Art I § 14 | ✓ | "Excessive Bail, Fines and Punishments" | — |
| Active filings jury-trial preservation | Art I § 21 | ✓ | "Trial by Jury" | "The right of trial by jury shall remain inviolate" |
| Fundamental rights catch-all | Art I § 32 | ✓ | "Fundamental Principles" | "A frequent recurrence to fundamental principles..." |
| D.C. AOUSC complaint judicial-admin | Art IV § 31 | ✓ | "Commission on Judicial Conduct" | Last section of Article IV |

All six load-bearing sections present and citation-resolvable. The 3 existing
section-form cross-corpus refs (all from WCR pointing to "Wash. Const. art. IV,
§ 31") will resolve to a real row in `wa_constitution.sections`. The 51
existing article-only cross-refs will resolve to `wa_constitution.articles`
rows.

---

## Section 4 — Schema design implications (deltas from kickoff)

### 4.1 Canonical citation CHECK regex revision needed

| Issue | Kickoff regex | Required revision | Rationale |
|---|---|---|---|
| Letter-suffix sections (§ 2(a), § 3(a)) | `^Wash\. Const\. art\. [IVXLC]+(, § [0-9]+[a-z]?)?$` | `^Wash\. Const\. art\. [IVXLC]+(, § [0-9]+[A-Za-z]?(\([a-z]\))?)?$` | Allows `§ 1A` (capital) for VI § 1A AND `§ 2(a)` (parens) for IV § 2(a) |

The kickoff regex allowed `§ 2a` (no parens) but actual citation form uses
parentheses. Two distinct letter-suffix conventions co-exist:
- **Capital letter no parens** — `§ 1A` (Article VI; rare; appears in
  amendment-numbered sections)
- **Parenthesized lowercase** — `§ 2(a)`, `§ 3(a)` (Articles III, IV, IX;
  more common; modern amendment style)

### 4.2 `wa_constitution.sections.section_number` value examples

Kickoff: TEXT NOT NULL (arabic, e.g. '1', '12', '35'). Actual values must
include: '1', '2', '2(a)', '3(a)' (Art IV); '1A' (Art VI). TEXT type
already handles all forms; CHECK constraint on `section_number ~
'^[0-9]+([A-Z]|\([a-z]\))?$'` recommended for format gating.

### 4.3 `wa_constitution.articles.is_repealed` should never be TRUE for current corpus

No entire article is repealed in the current ratified constitution (kickoff
flagged Article XX as anomaly; verified NOT repealed). The
`is_repealed` column on `articles` should remain present for schema
generality but no rows will have `is_repealed=TRUE` in initial Phase 2 ingest.

### 4.4 New schema-level disposition required

**Letter-suffix-section row architecture decision** — operator must
confirm one of:
- (Option A) **Standalone sections rows** — Art IV § 2(a), § 3(a) get their
  own rows in `wa_constitution.sections` with `section_number = '2(a)'`,
  `'3(a)'`. Total ~276-278 rows.
- (Option B) **Subsections folded into parent body** — § 2(a), § 3(a) text
  appended to § 2, § 3 body respectively. Total ~273 rows.

Recommended: **Option A** (standalone), because:
1. The entries have distinct headings (e.g., "TEMPORARY PERFORMANCE OF
   JUDICIAL DUTIES") that are doctrinally separate provisions, not
   subdivisions of the parent section.
2. WA case law cites these as separate provisions ("Wash. Const. art. IV,
   § 2(a)").
3. Preserves canonical-citation FK target granularity.

(Definitely subsection-form entries — Art II § 1(a-e), Art VIII § 1(a-l), etc. —
remain folded into parent body under either option.)

---

## Section 5 — Halt-band recalibration

Kickoff specified halt triggers:
- "Article I parses to fewer than 30 or more than 40 sections → halt (35 expected)" — Article I = 35 verified ✓; halt-band [30, 40] valid as-is
- "Aggregate section count outside [180, 220] → halt"

**Aggregate section count band is wrong** (per §K #29 — coordinator estimate
inflation). Verified actual count is 273-278 row-level total.

**Recommended recalibration** for aggregate halt-band:
- Parent-numbered sections: [265, 280] (verified 273)
- Total row-level count (Option A standalone): [270, 285] (verified 276-278)
- Total row-level count (Option B subsections): [265, 280] (verified 273)

Operator-confirmed band locks pre-Phase-2.

---

## Section 6 — Wall time projection

| Phase | Activity | Estimated wall time |
|---|---|---|
| Phase 2 | Schema definition + 4 tables + CHECK constraints + FK constraints | 10 min |
| Phase 2 | Source acquisition (32 article HTTP fetches @ 1 req/sec + 1 amendment landing + ~110 amendment fetches @ 1 req/sec) | 2.5 min for articles, 2 min for amendments = ~5 min |
| Phase 2 | Parser implementation (article + section + cross-ref + amendment) | 30-45 min |
| Phase 2 | Ingest pipeline + idempotent INSERT execution | 5 min |
| Phase 3 | BGE-M3 embedding generation (~278 sections × ~5 chunks/section = ~1,400 chunks; HF endpoint healthy: 3 chunks/sec → ~8 min; CPU fallback: 0.7 chunks/sec → ~33 min) | 8-33 min |
| Phase 4 | Internal cross-reference resolution (~30-50 internal refs estimated) | 10-20 min |
| Phase 4.4 | Cross-corpus FK uplift (6 ALTER TABLE + 4 UPDATE passes for 54 existing refs) | 5-10 min |
| Phase 5 | (Skip per kickoff — no parallel authority chain to WAC's RCW→WAC) | — |
| Phase 6 | Retrieval guide notes finalization | 30 min |
| **Total** | | **~2.0-3.0 hr** (HF healthy) **/ ~2.5-3.5 hr** (CPU fallback) |

---

## Section 7 — Parser anomaly flags

| Flag | Sections affected | Disposition |
|---|---|---|
| Inline amendment markers like "AMENDMENT 88, 1993..." in body text | Art I § 11, § 16, § 20; Art VI § 1; Art XXIII § 1; many others | Parser must extract amendment number + year + JR/HJR/SHJR identifier; populate `is_amended=TRUE` flag |
| Letter-suffix sections (§ 2(a), § 3(a), § 1A) | Art III, IV, IX (parens-form); Art VI (capital-letter form) | Per §4.4 disposition |
| Repealed sections | VI § 1A, VI § 2, XII § 14, XXVII § 13 | Parser sets `is_repealed=TRUE`; `full_text` may be NULL or contain marker like "[Repealed by Amendment NN, YYYY]" |
| Section heading "Same" referring back to prior section | Art I § 34 (refers to § 33 on recall) | Parser preserves heading verbatim; no special handling |
| Roman numeral sub-section per article? (e.g., § 2(a) treated as part of § 2 hierarchy) | None observed | Schema does not need a sub_section table |
| Original text vs amendment-superseded text | Multiple sections (e.g., II § 1, VI § 1, IX § 3, XV § 1, etc.) | Parser populates `original_text` field per kickoff schema; `effective_date` from amendment ratification |
| Cross-references to RCW chapters in body | Art XXII anomaly note: "current apportionment governed by RCW Chapter 44.07D" | Parser cross-ref regex must extract RCW citations from constitution body text |

---

## Section 8 — Per-article structural notes (worth surfacing)

- **Article I** — 35 sections, no anomalies; parser stress-test target ✓
- **Article II** — 43 parent sections (largest); contains initiative/referendum subsection cluster (§ 1(a-e))
- **Article III** — § 2(a) needs Phase 2 verification (subsection vs standalone)
- **Article IV** — 31 parent + 2 standalone (§ 2(a), § 3(a)) = 33 row-level
- **Article VI** — § 1A standalone-but-repealed (capital-letter form, unique in corpus); § 2 also repealed
- **Article XII** — § 14 repealed by Amendment 67 (1977)
- **Article XX** — **NOT REPEALED**; 2 active sections (kickoff anomaly note was wrong)
- **Article XXVII** (Schedule) — 19 sections; § 13 repealed by Amendment 74 (1983)
- **Article XXVIII-XXXII** — added by amendments (78, 49+75+93+110, 54, 61, 73 respectively); all 1-2 sections each

---

## Section 9 — Operator scope confirmation items

The following decisions need operator one-line confirmation before Phase 2
launch (per kickoff "Halt-and-surface for operator scope confirmation
before Phase 2"):

1. **Source URL pivot:** confirm Phase 2 acquisition uses
   `https://leg.wa.gov/state-laws-and-rules/washington-state-constitution/?section={GUID}`
   (32 article URLs already enumerated, cached locally).

2. **SOS secondary cross-check:** acceptable to skip (sos.wa.gov returns 402
   from this environment; primary leg.wa.gov suffices for ratified text +
   amendment metadata)?

3. **Letter-suffix architecture:** Option A (standalone rows for entries
   with distinct headings — recommended) or Option B (all subsections
   folded into parent)?

4. **Halt-band recalibration:** lock to [270, 285] for total row-level
   count if Option A, [265, 280] if Option B.

5. **Schema CHECK regex revision:** approve revised
   `chk_canonical_citation_format` to include `[A-Za-z]?(\([a-z]\))?`
   tolerance.

6. **Article XX disposition:** confirm parser does NOT pre-mark Article XX
   as repealed (kickoff anomaly note was wrong; XX is active).

7. **Amendment ratification dates:** acceptable that Phase 2 amendment
   ingest may need a second per-amendment HTTP fetch to extract dates that
   landing-page WebFetch couldn't surface (adds ~2 min wall time)?

8. **Phase 5 skip:** confirm skip per kickoff (no parallel to WAC
   authority_chain — WA Const doesn't have RCW-enabling-clause structure
   suitable for a chain table).

Operator one-line confirmation (any subset; defaults assumed otherwise):
"Approve: 1, 2 (skip SOS), 3=A, 4=[270,285], 5, 6, 7, 8 (skip Phase 5).
Begin Phase 2."
