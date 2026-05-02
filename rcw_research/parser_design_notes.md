# RCW Parser Design Notes

**Phase 3 deliverable.** Companion to `/opt/wdws/rcw_research/rcw_parser.py`
and `/opt/wdws/rcw_research/test_rcw_parser.py`.

This document captures the section-extraction logic, source-credit parsing
pattern, status-marker detection, and cross-reference resolution approach.
Read this before modifying the parser; structural changes require Phase 7
test-suite re-run + operator review.

---

## 1. Overall structure

The parser is organized around three top-level entry points:

| Function | Input | Output |
|---|---|---|
| `parse_chapter_html(filepath, publication_date)` | a `chap_{N.NN}_FULL.html` from `&full=true` | `ChapterRecord` with nested `SectionRecord`s, `HistoryEntry`s, `NoteEntry`s, `CrossRef`s |
| `parse_disposition_html(filepath)` | a `dispo_{N.NN}.html` from `dispo.aspx` | `DispositionRecord` with `DispositionEntry`s |
| `count_section_anchors(filepath)` | a `chap_{N.NN}_FULL.html` | int (used as audit ground-truth — see §6) |

All output dataclasses mirror the `rcw.*` schema applied in Phase 1. Phase 4
ingest serializes them directly to PostgreSQL via asyncpg.

The parser is read-only: no DB connections, no network, no global state.
Two parser invocations on the same file produce byte-identical outputs.

---

## 2. Section-extraction logic

### 2.1 The `&full=true` chapter view

Every chapter is fetched as a single HTML page using
`https://app.leg.wa.gov/RCW/default.aspx?cite=N.NN&full=true`. This view
inlines all the chapter's sections sequentially. Phase 0 §2.3 documented the
layout; the parser inverts that layout.

### 2.2 Anchor-based slicing

Each section begins with an HTML anchor `<a name='X.YY.SSS' ></a>` (or
`<a name='62A.1-101' ></a>` for UCC). The parser extracts all section anchors
in document order, then for each anchor walks forward through the DOM
collecting siblings until the next anchor (or end of chapter content).

Two anchor forms are recognized:

```
Standard:  T.CC.SSS   (T={1..91 + lettered like 9A,23B}, CC=numeric+optional letter, SSS=numeric+optional letter)
UCC dash:  62A.X-NNN  (Title 62A only; X=Article number, NNN=Section number)
```

The `ANCHOR_RE` regex accepts either separator (`[.\-]`) between the chapter
and section parts. Cross-reference detection in body text uses scoped
alternation (§5.2) to avoid the false positives this permissive form would
otherwise create.

### 2.3 Within-section structural elements

For each section's slice of the DOM (between successive anchors), the parser
recognizes these elements:

| Element | DOM signature | Output field |
|---|---|---|
| Citation H3 | first `<h3>` after anchor; contains `RCW <a href='...cite=X'>X</a>` and a sibling PDF button | `SectionRecord.canonical_citation` |
| Caption H3 | second `<h3>` after anchor; plain text | `SectionRecord.section_heading` |
| Body container | first `<div>` containing nested `<div style="text-indent:0.5in;">` paragraphs | iterates paragraphs into `SectionRecord.full_text` (joined with double-newlines) |
| Source-credit bracket | sibling `<div style="margin-top:15pt;margin-bottom:0pt;">` whose text starts with `[` and ends with `]` | parsed by `_parse_credit()` into `HistoryEntry`s |
| NOTES header | `<h3>NOTES:</h3>` (or `Notes:` in single-section view) inside `margin-top:0.25in` div | switches parser into "in_notes" mode |
| Note entry | `<div style="margin-bottom:0.2in;">` containing `<div style="text-indent:0.75in;">` with bold-span heading + body text + bracketed session-law links | `NoteEntry` |

The walker uses `getnext()` to traverse direct siblings of the anchor's parent.
When it reaches the end of one parent's siblings, it ascends to the
grandparent and continues. This handles cases where the chapter's sections
are wrapped in different container divs (rare but observed).

### 2.4 Subsection numbering

WA RCW subsections are flat-indented (always `text-indent:0.5in`) regardless
of nesting level. `(1)`, `(a)`, `(i)` all appear at the same indent. The
parser preserves the subsection markers verbatim in `full_text` paragraph
splits — downstream chunking (Phase 4) parses them from leading text tokens
rather than from indent depth.

### 2.5 Multi-paragraph body cohesion

Subsection (1) might contain (a)-(e) sub-points across multiple `<div>`s.
The parser concatenates all `text-indent:0.5in;` divs found in the section's
body container into `full_text` joined by `\n\n`. This loses inline
sub-paragraph hierarchy but preserves semantic ordering and is sufficient
for embedding-based retrieval (Phase 4 chunker handles further subdivision).

---

## 3. Source-credit parsing (`_parse_credit`)

### 3.1 Bracket structure

Every section ends with a source-credit bracket of the form:

```
[ {SL_1}; {SL_2}; ... {SL_N}; Formerly RCW {prior_citation} ]
```

Where each `SL_i` is a session-law citation, optionally followed by a
parenthetical action_summary, e.g.:

```
2017 c 303 s 3
1973 c 1 s 32 (Initiative Measure No. 276, approved November 7, 1972)
1862 p 83 s 1
Code 1881 s 1
RRS s 143
```

### 3.2 Splitting

The parser splits on `;` characters that appear at depth 0 (i.e., not
inside parentheses). This correctly handles parenthetical commentary that
contains its own commas and semicolons.

### 3.3 Per-entry parsing

For each session-law entry:

1. Strip trailing whitespace and period (`.`).
2. Search for parenthetical at end (`\s*\(([^)]+)\)\s*$`); if found, extract
   to `action_summary` and remove from `session_law` text.
3. The remaining text is the canonical session-law citation.
4. Look up the URL in `link_map` (built from `<a href>` elements in the
   credit div); attach as `session_law_pdf`.
5. Parse year from the citation: try modern `YYYY c NN s S` form, then
   page-form `YYYY p NN s S`, then Code/RRS forms (year sometimes absent).
6. `action_type` heuristic: oldest entry (sorted by position — RCW
   convention is most-recent-first) gets `'enacted'`, others get `'amended'`.
   Phase 4 may revise this using disposition-page data.
7. `sort_order = 1 + position` so sort_order=1 is the most recent amendment.

### 3.4 The "Formerly RCW X" pointer

If the credit text ends with `Formerly RCW T.CC.SSS`, the current section
was previously known as that citation. The parser:

1. Strips the `Formerly RCW ...` suffix from the credit text before splitting.
2. Records the prior citation in `SectionRecord.recodified_from`.
3. Emits a `CrossRef(target_citation=prior, target_jurisdiction='state-wa-rcw',
   ref_type='recodification', context='Formerly {prior}')`.

Phase 4 uses these together with disposition-page data to create the ghost
row for the prior citation with `is_recodified=TRUE`, `recodified_to=current`.

---

## 4. Notes-block parsing (`_parse_notes`)

### 4.1 Heading format

Each note begins with a bold span of the form:

```
{NoteType}{em-dash}{Topic}:
```

Where `{NoteType}` is one of the categories observed in the corpus (see
§4.3 below) and `{Topic}` is typically a session-law citation like
`2010 c 69`. Some notes have no topic (e.g., `Reviser's note:` standalone).

The em-dash is rendered in the source HTML as a Times-New-Roman span:
`<span style="font-family:times new roman;">—</span>` (the actual character
is U+2014 EM DASH).

### 4.2 Splitting heading into type + topic

The parser uses `re.split(r'\s*[—–\-—]\s*', heading, maxsplit=1)` accepting
em-dash, en-dash, hyphen-minus, or the U+2014 character (depending on how
lxml decoded the source). The first capture is the type, second is the topic.
Trailing colons are stripped from both.

### 4.3 Observed note types (full corpus)

Top types by frequency (from Phase 3 verification across 2,763 chapters):

```
Effective date              17,466
Severability                 6,812
Findings                     6,120
Intent                       5,168
Finding                      3,026
Effective dates              2,784
*Reviser's note              2,040
Application                  1,743
Short title                  1,510
Purpose                      1,340
Reviser's note               1,141
Part headings not law        1,026
Conflict with federal req.     948
Expiration date                880
Construction                   848
```

The Phase 0 §2.7 catalog identified ~12 types from 7 sample chapters; the
full-corpus catalog has many more in the long tail (e.g., `Intent`,
`Construction`, `Purpose`). The parser's `note_type` is a permissive `TEXT`
column rather than an enum, so unexpected types accumulate as data. Phase 5
should review the full distribution and decide whether a CHECK constraint
is warranted post-Phase-4.

The asterisk-prefix variant (`*Reviser's note`) is a Code Reviser convention
indicating an asterisk reference in the body text (`*chapter X.YY RCW`
indicates the cited chapter has been recodified or modified since the note
was written). Parser treats both forms as distinct `note_type` values.

### 4.4 Note body extraction

After the heading, the rest of the note's div is the body text. It often
ends with a bracketed session-law citation that supplies the note (e.g.,
`[ 2010 c 69 s 1.]`). The parser includes this bracket in `note_text` for
faithfulness; Phase 4 may strip it for embedding purposes.

---

## 5. Cross-reference detection (`_detect_cross_refs`)

Cross-references are detected from the body text (concatenated paragraphs)
using regex patterns. The parser does NOT walk the body's `<a href>`
attributes — those are separately captured for source-credit URLs and for
the Formerly-RCW pointer. Body cross-refs are extracted from text only,
which is more robust because the live HTML sometimes lacks an `<a>` wrapper
(parenthetical citations) or wraps the text in non-citation contexts.

### 5.1 Patterns

| Citation form | Regex | Jurisdiction tag | ref_type |
|---|---|---|---|
| `RCW 4.04.010` (dot) | `RCW_SECTION_TEXT_RE` standard branch | `state-wa-rcw` | `reference` |
| `RCW 62A.1-101` (UCC dash) | `RCW_SECTION_TEXT_RE` UCC branch | `state-wa-rcw` | `reference` |
| `chapter 4.04 RCW` | `RCW_CHAPTER_TEXT_RE` | `state-wa-rcw` | `reference` |
| `Title 4 RCW` | `RCW_TITLE_TEXT_RE` | `state-wa-rcw` | `reference` |
| `42 U.S.C. § 1983` | `USC_TEXT_RE` | `federal-usc` | `cross-statute` |
| `28 CFR § 35.107` | `CFR_TEXT_RE` | `cfr` | `cross-statute` |
| `Article I § 7 of the Washington Constitution` | `WA_CONST_TEXT_RE` | `wa-constitution` | `authority` |

### 5.2 Scoped alternation in `RCW_SECTION_TEXT_RE`

The crucial design decision (Phase 3 operator clarification): the section-
citation regex uses **scoped alternation**, not permissive separator
substitution.

```python
RCW_SECTION_TEXT_RE = re.compile(
    r'\bRCW\s+'
    r'('
        r'62A\.[0-9]+[A-Z]*-[0-9]+[A-Z]*'              # UCC dash-form, Title 62A ONLY
        r'|'
        r'[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+'        # Standard T.CC.SSS dot-form
    r')'
    r'(?:\([0-9a-zA-Z]+\))*'                            # subsection (1)(a)(i)
)
```

Why this matters: a permissive `[.\-]` separator would falsely match
`RCW 4.04-4.06` (a chapter range bridge) as a single section citation. The
scoped form structurally cannot do that — the standard branch requires
three dot-separated groups, no dashes admitted.

False-positive walkthrough (verified by `test_cross_ref_string_patterns` in
the test harness):

| Input | Outcome | Why |
|---|---|---|
| `between 2024-2025` | no match | no `RCW\s+` prefix |
| `RCW 4.04 to 4.06` | no match | only 2 dot-parts; standard branch needs 3 |
| `RCW 4.04.010 to 4.04.020` | matches first only | second cite needs its own `RCW` prefix |
| `Laws of 1991, ch. 4, §§ 1-15` | no match | no `RCW\s+` prefix |
| `RCW 62A.1-101 and RCW 4.04.010` | both match (different branches) | scoped alternation |
| `2017 c 303 s 3` | no match | no `RCW\s+` prefix |

The known-but-acceptable parsing limitation: `RCW 4.04.010 to 4.04.020`
catches only the first cite. Capturing the second would require a stateful
parser that tracks "we're inside a section range" — out of scope for
regex-based extraction.

### 5.3 Recodification cross-refs

When `recodified_from` is set on a section (from `Formerly RCW X` in the
credit), the parser emits an additional CrossRef of `ref_type='recodification'`
targeting the prior citation. Phase 4 ingest uses these to:

1. Create a ghost `rcw.sections` row for the prior citation with
   `is_recodified=TRUE`, `recodified_to={current}`, `full_text=NULL`.
2. Insert a row in `rcw.cross_references` recording the bidirectional pointer.

The disposition page for the prior chapter (e.g., `dispo.aspx?Cite=42.17`)
contains the same information from the OTHER side (`Recodified as RCW
42.56.520 pursuant to ...`). Phase 4 should reconcile the two sources: if
they disagree, prefer the disposition page (it includes the session-law
authority for the recodification).

---

## 6. Status-marker detection

### 6.1 In live chapter HTML

The `&full=true` view includes only currently-effective sections. Repealed,
recodified-out, decodified, and (mostly) reserved sections are absent from
this view. The parser's status flags on `SectionRecord` will therefore
typically all be `FALSE` for sections found in `&full=true` HTML.

The one exception: chapter-level `(RESERVED)` markers in the H2 caption.
The parser sets `ChapterRecord.is_reserved_chapter=TRUE` when this marker is
present. `RCW 48.26 (Marine and Transportation Insurance)` is the canonical
example.

### 6.2 In disposition pages

The dispositions page (`dispo.aspx?Cite=N.NN`) lists every section number
that ever existed in the chapter, with its status. Section-status sentence
patterns the parser recognizes:

| Pattern | Sets |
|---|---|
| `Recodified as RCW {target} pursuant to {SL} § N, effective {date}.` | `is_recodified=TRUE`, `recodified_to=target`, `effective_date=date` |
| `Repealed by {SL} § N.` | `is_repealed=TRUE`, `repealing_session_law=SL` |
| `Repealed by {SL_A}; and repealed by {SL_B}.` | `is_repealed=TRUE` (second SL goes to history if Phase 4 captures it) |
| `Repealed by {SL}, effective {date}. Later enactment, see RCW {newcite}.` | `is_repealed=TRUE` + raw text retained for the later-enactment hint |
| `Reserved.` | `is_reserved=TRUE` |
| `Decodified by {SL}.` | `is_decodified=TRUE` |
| `Expired {date}.` | `is_expired=TRUE` |

The parser regex catches these as substring patterns inside the
`<blockquote>` text following each `<b>SSS Caption.</b>` entry. The full
raw text is preserved in `DispositionEntry.raw_disposition_text` for audit.

### 6.3 Combined-flag entries

Some entries combine flags, e.g., a section that was repealed and later
re-enacted at a different citation. The parser sets all applicable flags;
Phase 4 ingest creates a single row with multiple flags TRUE and reconciles
priority by disposition order.

---

## 7. Encoding notes

The leg.wa.gov pages declare `Content-Type: text/html; charset=utf-8` and
the byte content is UTF-8. The parser explicitly decodes via
`raw.decode('utf-8', errors='replace')` before passing to `lxml.fromstring`.
Without this explicit decode, lxml occasionally falls back to latin-1 for
these specific files, producing visible artifacts like `DECISIONâ` instead
of `DECISION—`. The errors='replace' fallback is paranoia — the corpus is
clean UTF-8 in practice.

The em-dash separator inside note headings is a Times-New-Roman-styled
span, which `text_content()` normalizes to a literal U+2014 character. The
heading-split regex accepts em-dash, en-dash, and hyphen-minus to be robust
against future format changes.

---

## 8. Error handling

The parser raises `ValueError` only on truly broken input — currently the
only such case is `_extract_h1_chapter` when neither the `<h1>` nor the
`<title>` element contains a recognizable chapter identifier. Phase 3
`phase3_verify.py` caught zero such failures across 2,763 chapters after
the UCC fix landed.

Empty or near-empty chapters do not raise: the parser returns a
`ChapterRecord` with an empty sections list. Phase 4 ingest treats this as
a normal "no live sections" outcome and emits a `rcw.chapters` row with
`is_reserved_chapter` propagated, no `rcw.sections` rows.

---

## 9. Performance characteristics

Empirical (Phase 3 verification, single-threaded Python on the Proxmox host):

```
Chapters:        2,763 files (~766 MB total HTML)
Wall time:       ~73 seconds  (37 chapters/sec sustained)
Sections out:    51,790
Per-chapter avg: 26 ms (dominated by lxml HTML parse)
```

Largest chapter (RCW 82.04, 2.1 MB) parses in ~250 ms. Disposition pass is
~30 chapters/sec (slightly slower due to per-`<span>` iteration through
hundreds of disposition entries on chapters like 43.131).

Phase 4 ingest will pipeline parsing with the embedding-batch dispatch, so
this pass time becomes part of the embedding wall-clock budget rather than
a separate stage.

---

## 10. Test coverage

`test_rcw_parser.py` exercises 12 test groups:

| # | Test | What it covers |
|---|---|---|
| T1 | Anchor parity on RCW 4.04 | Single-section baseline |
| T2 | 7 Phase-0 dissection samples | Original sample anchor counts unchanged |
| T3 | 10 stress-test chapters | Largest chapters in the corpus, anchor parity |
| T4 | 10 Class-1 empty chapters | Reserved/empty chapters return zero sections without error |
| T5 | 14 Class-1B UCC chapters | Title 62A dash-form per-chapter section counts |
| T6 | UCC `section_number` preservation | Dash form retained as atomic identifier |
| T7 | Cross-ref scoped alternation | 5 non-62A chapters: 0 dash-form false positives across 819 cross-refs |
| T8 | Direct regex tests | RCW_SECTION_TEXT_RE behavior on 11 synthetic strings |
| T9 | Disposition parser on RCW 43.131 | Largest disposition file, 264 entries with status flags |
| T10 | Disposition parser on small chapter | RCW 4.04 dispositions extract chapter metadata |
| T11 | ANCHOR_RE direct tests | Both dot and dash anchor forms accepted |
| T12 | recodified_from pointer | RCW 42.56.520 → RCW 42.17.320 recodification chain |

Result: **94 individual assertions, all passing**, in <2 seconds wall time.

---

## 11. Phase 4 contract

Phase 4 ingest reads from these output dataclass fields:

```
ChapterRecord:
    title_number, chapter_number, chapter_heading, publication_date
    is_reserved_chapter
    sections: list[SectionRecord]

SectionRecord:
    section_number, canonical_citation, section_heading
    full_text, source_html
    is_repealed, is_recodified, is_decodified, is_expired, is_reserved
    recodified_to, recodified_from
    enacted_by_session_law, effective_date, unit_type
    history: list[HistoryEntry]
    notes: list[NoteEntry]
    cross_refs: list[CrossRef]

DispositionRecord:
    title_number, chapter_number, chapter_heading
    is_empty
    entries: list[DispositionEntry]

DispositionEntry:
    section_number, section_caption, canonical_citation
    is_repealed, is_recodified, is_decodified, is_expired, is_reserved
    recodified_to, repealing_session_law, effective_date
    raw_disposition_text
```

Phase 4 reconciliation logic (combining live + disposition data):

1. For each chapter, parse the `&full=true` HTML → emit `rcw.chapters` row,
   then `rcw.sections` rows for every `SectionRecord` (live current sections).
2. Parse the disposition HTML for the same chapter. For each
   `DispositionEntry` whose canonical_citation does NOT match a live
   `SectionRecord`, emit a ghost `rcw.sections` row with the appropriate
   status flags (no body text, no embeddings).
3. For sections appearing in both (i.e., repealed-and-replaced), prefer the
   live `SectionRecord` for the row; the disposition data goes to
   `rcw.section_history`.
4. For each `CrossRef`, emit a `rcw.cross_references` row. The
   `target_section_id` may be NULL until a post-pass resolution step links
   citations across chapters.

End of design notes.
