# USLM Findings — Phase 0 Research

**Author:** USC ingestion agent
**Date:** 2026-04-28
**Sources:** `https://uscode.house.gov/xml/uslm.xsd` (404 — file moved); the working source is `https://github.com/usgpo/uslm/blob/main/USLM.xsd`, downloaded locally to `/opt/wdws/usc_research/USLM.xsd` (4,090 lines, 151 type/element defs). Companion: `USLM-User-Guide.pdf` (49 pp), `USLM-2_1-ReviewGuide.pdf` (current 2.1 review guide).

---

## 1. Schema Identity

- **XML namespace:** `http://xml.house.gov/schemas/uslm/1.0` (the namespace URI is pinned at 1.0 by design even though the schema version evolves)
- **Current schema version:** **USLM 2.1.0** (also still publishing 2.0.14 → 2.0.17 alongside; 2.1.0 is the head version on `main`)
- **Reference repository:** `github.com/usgpo/uslm` — schemas, CSS stylesheets, review guides; **no parser libraries or XSLTs published by GPO**. Parser must be built locally (Phase 3).
- The xsd file `uslm.xsd` advertised on `download.shtml` is stale; use the GPO github raw URL.

## 2. Element Hierarchy

### 2.1 Top-level container

```
<uscDoc xmlns="http://xml.house.gov/schemas/uslm/1.0">
  <meta>...metadata, identifiers, dates...</meta>
  <main>
    [<title> | <appendix> | …]
  </main>
</uscDoc>
```

### 2.2 The "level" substitution group (the structural backbone)

Every hierarchical level — from `<title>` down to `<subsubitem>` — is declared as `type="LevelType" substitutionGroup="level"`. `LevelType` is a single complexType extending `BaseBlockType`; the difference between a chapter and a clause is *element name*, not type. Chain (top-down):

```
title → subtitle → chapter → subchapter → part → subpart →
section → subsection → paragraph → subparagraph →
clause → subclause → item → subitem → subsubitem
```

**Naming prefixes** (case-sensitive, used inside `@identifier` URLs):
- title → `t` (e.g., `t42`)
- section → `s` (e.g., `s12101`)
- subsection → `ss` (e.g., `/us/usc/t42/s12101/ss/a`)

### 2.3 LevelType internal structure (in document order)

```
LevelType :=
  NumStructure?       — <num> (the level designator, e.g., "§ 12101.")
  HeadingStructure?   — <heading>
  TocStructure*       — <toc> (rare, manually authored)
  LevelStructure?     — the body
```

`LevelStructure` is missing on repealed/omitted/spent levels — only a `<note>` is left behind.

### 2.4 Text-only siblings inside a level body

These are `ContentType substitutionGroup="text"`:
- `<chapeau>` — introductory text *before* numbered children of a level
- `<continuation>` — closing text *after* numbered children
- `<def>` — defined-term wrapper
- `<proviso>` — proviso clause

### 2.5 Notes inside a level

`NoteType substitutionGroup="note"`:
- `<sourceCredit>` — the parenthetical Public-Law/Statutes-at-Large citation chain (parens are part of the text, not auto-rendered)
- `<statutoryNote>` — note that is itself part of the law
- `<editorialNote>` — OLRC editorial commentary
- `<changeNote>` — diff/change annotation
- `<note>` — generic catchall

`@type` on `<note>` uses `NoteTypeEnum`: **`inline | footnote | endnote | uscNote`** (default `footnote`). `@topic` is a free-text categorization (≤128 chars) — frequently `"Effective Date"`, `"Short Title"`, `"References in Text"`, `"Codification"`, `"Repeal"`, `"Transfer of Functions"`, `"Construction"`.

### 2.6 Cross-reference

```
<ref type="RefType" substitutionGroup="property"
     href="/us/usc/t5/s101"
     idref="..."           (same-doc anchor)
></ref>
```

`@href` format: relative URL of the form `/us/usc/t{N}/s{M}` for a USC section, `/us/usc/t{N}/s{M}/{level}/{designator}` for a sub-level. External targets:
- `/us/cfr/t29/s1630.2` — CFR
- `/us/pl/{Congress}/{LawNum}/s{Section}` — Public Law
- `/us/stat/{Vol}/{Page}` — Statutes at Large

The href grammar is consistent enough for a regex-based resolver.

### 2.7 Appendix elements (Title 5, 18, 28, 50)

`<appendix type="AppendixType">` is a generic container; derived element names (substitutionGroup="appendix") include `<schedule>`. The FRCP/FRCrP/FRE/APA appendices are children of `<title>` and structurally use the same Level chain (FRCP "Rule 17" → `<section num="Rule 17">`).

## 3. Status Markers — `StatusEnum`

`StatusEnum` (attribute `@status`, attaches to any LevelType element via `VersioningGroup`):

```
proposed   withdrawn    cancelled    pending      operational
suspended  renumbered   repealed     expired      terminated
hadItsEffect  omitted   notAdopted   transferred  redesignated
reserved   vacant       crossReference  unknown
```

For our purposes the actionable statuses are:
- **`repealed`** — section has been repealed; XML usually retains a stub `<note>` with the repeal credit. Ingest with `is_repealed=true`, preserve original text where present (often it's been removed and only the credit remains — that's still data we want).
- **`transferred`** — text has moved; ingest stub with `is_transferred=true` and a `transferred_to` field parsed from the editorial note.
- **`omitted`** — section number reserved but no current law assigned. `is_omitted=true`.
- **`reserved`/`vacant`** — number reserved for future use. Treat as `is_omitted=true` with a note.
- **`renumbered`/`redesignated`** — historical-only; the current section will be at the new number. Surface via `section_history`.
- **`crossReference`** — entry is purely a pointer to another section. Ingest as a stub document with `transferred_to`.

## 4. Source-credit Parsing

`<sourceCredit>` is a free-text NoteType with parenthetical content like:

```
(Pub. L. 101–336, title I, § 101, July 26, 1990, 104 Stat. 330;
 Pub. L. 110–325, § 4(a), Sept. 25, 2008, 122 Stat. 3555.)
```

Parser strategy: split on `;`, then regex each segment for `Pub. L. <Cong>-<Law>`, `§ <sec>`, `<MMM. DD, YYYY>`, `<Vol> Stat. <Page>`. Emit one `usc.section_history` row per matched segment with `action_type` inferred from the surrounding `<editorialNote topic="…">` ("Codification", "Amendments", "References in Text").

## 5. Positive Law vs. Non-Positive Law

The schema does not carry a positive-law boolean. We must seed `usc.titles.is_positive_law` from a known-good list. As of 2026-04, **positive-law titles**: 1, 3, 4, 5, 9, 10, 11, 13, 14, 17, 18, 23, 28, 31, 32, 35, 36, 37, 38, 39, 40, 41, 44, 46, 49, 51, 54. **Non-positive-law titles** (text is prima facie evidence): 2, 6, 7, 8, 12, 15, 16, 19, 20, 21, 22, 24, 25, 26, 27, 29, 30, 33, 34 (vacant/repealed in entirety), 42, 43, 45, 47, 48, 50, 52, 53. Citation in pleadings differs: positive-law titles cite the Code as binding text; non-positive-law titles technically must cite the underlying Public Law if the Code text differs from the enacted text. The corpus must surface this for downstream agents.

## 6. Edge Cases the Parser Must Handle

| Case | Detection | Handling |
|---|---|---|
| Section with only `<note>` body (repealed) | no `<content>` or sub-levels under `<section>`; `@status="repealed"` | Persist note text as full_text, set is_repealed=true |
| Section with `@status="transferred"` | `@status="transferred"` + `<note topic="Transfer">` | Parse target citation from note, set transferred_to |
| Sections like `§ 12101a`, `§ 12101-1` | `<num>` value is non-integer | Store section_number as text, never int |
| Reserved blocks (e.g., `[Reserved]`) | empty `<content>` + `@status="reserved"` | Persist with is_omitted=true, full_text="[Reserved]" |
| Subsection `(a)` containing only `<chapeau>` then list of paragraphs | LevelStructure has chapeau + paragraph children | Render chapeau as block-level intro before nested list |
| Continuation text after sub-list | `<continuation>` after the last paragraph | Render as block-level outro |
| Definitions section (e.g., 42 USC § 12102) | many `<def>` elements | Preserve `<def>` tags as italic in markdown |
| Cross-reference href to nonexistent target | resolver returns null | Log + mark `usc.cross_references.target_section_id=NULL`, ref_type='internal-unresolved' |

## 7. Recommended Parser Stack (Phase 3)

- **Library:** `lxml` with `iterparse` for streaming. Title 42 (~108 MB) and Title 26 (~54 MB) will OOM a tree-load parser on the Proxmox host. Stream by `<section>` end-events.
- **Namespace handling:** Use a `nsmap = {"u": "http://xml.house.gov/schemas/uslm/1.0"}` and address elements as `u:section`, `u:num`, etc. Don't strip namespaces — sub-namespaces (xhtml, MathML, Dublin Core) appear in `<content>`.
- **Markdown rendering:** map `<chapeau>` → block paragraph, `<num>` + `<heading>` of subsection → `## (a) Heading`, paragraphs → numbered list, subparagraphs → lettered sublist. Preserve the section heading verbatim in every chunk per the Phase 1 chunking rule.
- **Identifiers:** every element carries `@identifier` (e.g., `/us/usc/t42/s12101/a/1`). Use this as the pinpoint key when generating sub-section chunks; encode it in `core.document_chunks.metadata.uslm_identifier`.

## 8. Open Questions Surfaced

1. The xsd at `uscode.house.gov/xml/uslm.xsd` is gone (404). Phase 1 schema doc should cite the GPO github URL as the authoritative source.
2. The all-titles bulk zip groups every title into a single archive — the parser will work over per-title XML files inside; confirm in Phase 2 that the zip layout matches `uscNN.xml` (zero-padded) per title.
3. Title 53 is reserved; Title 34 was repealed in entirety (now reserved). Both will produce empty/near-empty XML — handle as legitimate, not as parser failure.

## 9. Phase-2 ground-truth discoveries (added 2026-04-28 post-extract)

After downloading and extracting the PL 119-84 bulk zip (104 MB compressed → 663.9 MB extracted, 58 XML files), three structural facts surfaced that the parser MUST handle and that were not visible from the xsd alone:

### 9.1 Title 53 truly absent from the bulk zip

`usc53.xml` is not in the all-titles archive (vs. an empty file). Parser must treat "title number missing from manifest" as legitimate, not as a download error. The zip ships 53 numbered titles + 5 appendix files = 58 files total. Note revision: Title 34 IS present in the current bulk (`usc34.xml`, 8.8 MB, 827 sections — recodified as "Crime Control and Law Enforcement"); only Title 53 is reserved/absent.

### 9.2 Appendix files use `<courtRule>` (not `<section>`)

The four rule-bearing appendix files express their citation units as `<courtRule>` inside a `<courtRules>` container, not as `<section>`. Counts:

| File | `<section>` | `<courtRule>` | Rule sets present |
|---|---:|---:|---|
| `usc28a.xml` | 0 | 237 | FRAP, FRCP, FRE (three `<courtRules>` wrappers) |
| `usc11a.xml` | 0 | 229 | FRBP (one wrapper, no identifier) |
| `usc18a.xml` | 28 | 76 | FRCrP (one wrapper) — note: also 28 sections (ancillary statutes) |
| `usc50A.xml` | 0 | 0 | (stub — War Powers lives in main `usc50.xml`) |

The 28 `<section>` rows in `usc18a.xml` are statutory provisions ancillary to FRCrP (e.g., bail laws, witness security) — those go in `usc.sections` with `unit_type='section'`, while the 76 FRCrP rules go with `unit_type='court_rule'` and `rule_set='FRCrP'`.

### 9.3 Rule-set discriminator: parse `<courtRules>/@identifier`, fall back to file-name mapping

Within `usc28a.xml`, three independent `<courtRules>` wrappers exist, distinguished by `@identifier` path tail and (redundantly) by `<heading>` text:

| `@identifier` | `<heading>` | rule_set value |
|---|---|---|
| `/us/usc/t28a/courtRules/App` | "FEDERAL RULES OF APPELLATE PROCEDURE" | `FRAP` |
| `/us/usc/t28a/courtRules/Civil` | "FEDERAL RULES OF CIVIL PROCEDURE" | `FRCP` |
| `/us/usc/t28a/courtRules/Evid` | "FEDERAL RULES OF EVIDENCE" | `FRE` |
| `/us/usc/t18a/courtRules/Crim` (only wrapper in usc18a) | "FEDERAL RULES OF CRIMINAL PROCEDURE" | `FRCrP` |
| (no `@identifier`, sole wrapper in usc11a) | "FEDERAL RULES OF BANKRUPTCY PROCEDURE" | `FRBP` |

**Parser algorithm for rule_set:** primary = parse `@identifier` tail (`App→FRAP`, `Civil→FRCP`, `Evid→FRE`, `Crim→FRCrP`); fallback 1 = file-name mapping (`usc11a.xml→FRBP`); fallback 2 = `<heading>` text inference (`'CIVIL'→FRCP`, etc.). Raise on any unmapped wrapper.

### 9.4 `usc05A.xml` uses `<reorganizationPlan>` (not section/courtRule)

Title 5 Appendix has 107 `<reorganizationPlan>` elements + 19 `<section>` elements. The reorganization plans use:

- `@identifier` path like `/us/usc/t5a/reorgPlan/1939/I`, `/reorgPlan/1939/II`, `/reorgPlan/1940/III`
- `<heading>` text like `REORGANIZATION PLAN NO. I OF 1939`, `REORGANIZATION PLAN NO. II OF 1939`

Canonical citation format: `Reorg. Plan No. I of 1939` (Roman numeral preserved, year suffixed).

The 19 `<section>` rows in `usc05A.xml` are the Administrative Procedure Act and adjacent legacy provisions; they ingest as `unit_type='section'` with their own canonical citations like `5 U.S.C. App. § 1`.

### 9.5 Canonical citation format catalog (parser must emit exactly this)

| `unit_type` | `rule_set` | Citation format |
|---|---|---|
| `section` | NULL | `42 U.S.C. § 12101` |
| `section` (in title appendix) | NULL | `5 U.S.C. App. § 1` |
| `court_rule` | `FRCP` | `Fed. R. Civ. P. 26` |
| `court_rule` | `FRAP` | `Fed. R. App. P. 4` |
| `court_rule` | `FRE` | `Fed. R. Evid. 401` |
| `court_rule` | `FRCrP` | `Fed. R. Crim. P. 11` |
| `court_rule` | `FRBP` | `Fed. R. Bankr. P. 7001` |
| `reorganization_plan` | NULL | `Reorg. Plan No. I of 1939` |
