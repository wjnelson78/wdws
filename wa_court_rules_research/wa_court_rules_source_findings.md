# WA Court Rules — Source Findings

**Source:** `https://www.courts.wa.gov/court_rules/`
**Date:** 2026-04-30
**Cached HTML:** `cached_html/index.html`, `cached_html/group_*.html`, `cached_html/list_*.html`
**Cached PDF samples:** `cached_html/pdf_samples/`

## URL pattern (three tiers)

1. **Master index:** `https://www.courts.wa.gov/court_rules/`
   Contains links to six "group" pages: `sup`, `ga`, `clj`, `app`, `aca`, `am`.

2. **Group page (per court level / category):**
   `https://www.courts.wa.gov/court_rules/?fa=court_rules.state&group=<group>`
   Lists the rule sets within that group.

3. **Rule-set list page (per rule set):**
   `https://www.courts.wa.gov/court_rules/?fa=court_rules.list&group=<group>&set=<set>`
   Tabular HTML listing of rule numbers + titles + per-rule PDF links.

4. **Rule body (PDF):**
   `https://www.courts.wa.gov/court_rules/pdf/<set>/<filename>.pdf`
   Filenames follow patterns `<GROUP_PREFIX>_<SET>_<NN>_<NN>_<NN>.pdf`, e.g.:
   - `GA_GR_31_00_00.pdf` — General Rule 31, no sub-rule, no part
   - `GA_GR_31_01_00.pdf` — General Rule 31.1
   - `GA_RPC_01_05_00.pdf` — RPC 1.5
   - `GA_ER_04_01_00.pdf` — ER 401 (Article 4, Rule 1)
   - `SUP_CR_56_00_00.pdf` — CR 56
   - `APP_RAP_18_13A_00.pdf` — RAP 18.13A (alpha-suffix appended into segment 3)
   - `GA_CJC_canon1.pdf` — CJC Canon 1 (irregular naming)
   - `GA_GR_14_Appendix.pdf`, `GA_GR_26_Standards.pdf` — appendix/standards companions
   - `GA_ELLLTC.pdf`, `GA_ELPOC.pdf`, `GA_LPORPC.pdf` — mono-document rule sets

5. **Display URL (alternate, redirects to PDF):**
   `https://www.courts.wa.gov/court_rules/?fa=court_rules.display&group=<group>&set=<set>&ruleid=<ruleid>` — verified for `gagr31` to redirect to a "click to open PDF" page; **bodies are PDF-only, there is no canonical HTML body view.**

## HTML structure of list pages

Each rule appears as a `<tr>` of two `<td>` cells inside a centered `<table width="100%" align="center" cellpadding="3">`:

```html
<tr>
  <td nowrap valign="top">31</td>
  <td valign="top"><a href="../court_rules/pdf/GR/GA_GR_31_00_00.pdf"> Access to Court Records</a></td>
</tr>
```

Selector pattern (regex, sufficient given hand-coded ColdFusion HTML):

```python
re.findall(
    r'<td[^>]*nowrap[^>]*>([^<]+)</td>\s*<td[^>]*>(.*?)</td>',
    html, re.DOTALL
)
# rule_number = group(1).strip()
# inside group(2):  <a href="(.*?\.pdf)">(.*?)</a>  → pdf_url, title
```

Rule-set name is extracted from `<h2>Washington State Court Rules: <NAME></h2>`.

## Rule body (PDF) structure

All bodies are PDFs (typically text-PDF, not scanned — confirmed by `pdftotext` extraction working clean across samples). Standard layout for a numbered rule:

```
                                  GR 31
                       ACCESS TO COURT RECORDS

      (a) Policy and Purpose. <body text>...

      (b) Scope. <body text>...

         (1) ...
         (2) ...

         (A) ...
         (B) ...

[Adopted effective October 26, 2004; Amended effective January 3, 2006; May 15, 2023; June 27, 2023.]
```

Components, top-down:
- **Header line:** centered rule number (e.g., `GR 31`)
- **Title line:** centered ALL-CAPS title (e.g., `ACCESS TO COURT RECORDS`)
- **Body:** subsection-prefixed paragraphs `(a)`, `(b)` ... with nested `(1)`, `(A)`, etc.
- **History (always last):** `[Adopted effective <date>; Amended effective <date>; <date>; ...]` — bracketed, semicolon-delimited dates
- **Comment block (optional, always after history):** standalone `Comment` heading then either a single comment paragraph (e.g., ER 401: `[Deleted effective September 1, 2006.]`) or RPC-style `[1]`, `[2]` ... `[N]` numbered comment paragraphs.

## Reserved / Repealed / Rescinded markers

Three distinct conventions exist:

1. **List-page title:** `Reserved`, `[Reserved]`, `(Reserved)`, `Repealed`, e.g.:
   - `RPC 7.2 — Reserved`
   - `CrR 3.2A — Repealed`
   - `ER 807 — Child Victims or Witnesses (Reserved)`
   - `JuCR 10.5 — Access to Official Juvenile Court Files (Reserved)`

2. **In-PDF body:** `[RESERVED.]` (with brackets, ALL-CAPS, occasional period), or `[RESCINDED]`. Sample (RPC 7.2):
   ```
                                RPC 7.2
                              [RESERVED.]

   [Adopted effective September 1, 1985; Amended effective September 1, 1988;
    September 1, 2006; April 14, 2015; January 26, 2021.]
   ```
   Sample (CrR 3.2A):
   ```
                                CrR 3.2A
                              [RESCINDED]

   [Adopted effective July 1, 1992; Amended effective September 1, 1995;
    Repealed effective April 3, 2001.]
   ```

3. **List-page range entries:** `RAP 18.18 - 18.20 — Reserved`, `CRLJ 27-37 — Reserved`, `CRLJ 86.04--99.04 — Reserved`, etc. — multi-rule reserved blocks compressed into a single index row. The PDF behind these typically covers the whole range with one `[RESERVED.]` body.

**Implication for `is_repealed` / `is_reserved` ghost flags:** Three ghost classes, not two:
- `is_reserved` (placeholder for future use; rule number exists but body is `[RESERVED.]`)
- `is_repealed` (formerly active rule, now `[RESCINDED]` / "Repealed effective" in history)
- `is_range_placeholder` (single index entry covering a range of reserved numbers, e.g., `CRLJ 27-37`)

The third class is structurally distinct because it must be expanded into individual rule numbers if downstream citation parsing is to match e.g. `CRLJ 30`. **Halt criterion #6 — flagged in HALT.md.**

## Comment-section pattern (RPC and CJC)

Confirmed RPC convention from `pdf_samples/RPC_1_1.pdf`:

```
                                    Comment

Legal Knowledge and Skill          ← thematic heading (un-numbered)

[1] In determining whether a lawyer employs the requisite knowledge...

[2] A lawyer need not necessarily have special training...

...

[Comment 6 Adopted September 1, 2016.]   ← per-comment history line

[7] [Washington revision] When lawyers or LLLTs from more than one law firm...

[Comment 7 Adopted September 1, 2016].

Maintaining Competence              ← thematic heading

[8] To maintain the requisite knowledge and skill...

                  Additional Washington Comments (9-10)    ← Washington-specific section

[9] This rule applies to lawyers only when they are providing legal services...

[10] In some circumstances, a lawyer can also provide adequate representation...
```

Patterns confirmed:
- Numbered comments use `[N]` (single digit or multi-digit, square brackets, paragraph-leading).
- Per-comment history (e.g., `[Comment 6 Adopted September 1, 2016.]`) appears between numbered comments.
- "Additional Washington Comments (9-10)" is a Washington-specific header convention separating ABA-aligned comments from WA-specific ones.
- Some comments are tagged inline with `[Washington revision]`.

CJC uses the same `[N]` pattern (per Canon 1 sample), but with different thematic-heading conventions ("Comments" rather than "Comment").

ER 401 has `Comment 401` heading instead of generic `Comment` — suggests rule-numbered comment sections in some rule sets. Worth verifying breadth.

**No deviation from RPC-style numbered comments observed in samples.** Halt criterion #5 not triggered.

## Amendment-history markers

Universal pattern for active rules:

```
[Adopted effective <DATE>; Amended effective <DATE>; <DATE>; ...; <DATE>.]
```

Repealed rules add a "Repealed effective" clause:

```
[Adopted effective July 1, 1992; Amended effective September 1, 1995;
 Repealed effective April 3, 2001.]
```

Date format: `Month D, YYYY` (e.g., `September 1, 1985`, `April 28, 2015`, `June 27, 2023`). Always U.S. long-form, no zero-padding.

Robust regex sketch:
```python
HISTORY_RE = re.compile(
    r'\[\s*Adopted\s+effective\s+(?P<adopted>[A-Z][a-z]+\s+\d{1,2},\s+\d{4})'
    r'(?:\s*;\s*Amended\s+effective\s+(?P<amendments>[^.\]]+))?'
    r'(?:\s*;\s*Repealed\s+effective\s+(?P<repealed>[A-Z][a-z]+\s+\d{1,2},\s+\d{4}))?'
    r'\.?\s*\]', re.DOTALL)
```

Per-comment history (smaller scope):
```
[Comment 6 Adopted September 1, 2016.]
[Comment 7 Adopted September 1, 2016].   ← note period-bracket order swap
```

## Subsection notation patterns

Top-down hierarchy in body text, observed across CR 12, CR 56, CR 26, GR 31, GR 34, RPC 1.1, RPC 1.5, RPC 5.5, ER 401:

| Level | Marker | Example |
|---|---|---|
| 1 | `(a)`, `(b)`, `(c)` ... lowercase Latin in parens | `(a) Policy and Purpose.` |
| 2 | `(1)`, `(2)`, `(3)` ... arabic numeral in parens | `(1) "Access" means...` |
| 3 | `(A)`, `(B)`, `(C)` ... uppercase Latin in parens | `(A) Social Security Numbers.` |
| 4 | `(i)`, `(ii)`, `(iii)` ... lowercase Roman in parens | (rare; appears in CR 26) |

The header marker is followed by a bolded short title in many rules (e.g., `(a) Policy and Purpose.`) but bolding is not consistent in the PDF text extraction layer. Title-after-marker is recognizable as `Capital Word(s) followed by period before sentence body`.

CJC uses a slightly different convention (per Canon 1): each Canon contains `RULE 1.1`, `RULE 1.2` ... headings that function like sub-rules, then `(A)/(B)` underneath the rule.

## Rule-number patterns (1,178 rules surveyed)

| pattern | count | example |
|---|---:|---|
| `INT.INT` | 722 | `RAP 1.1`, `RPC 1.5`, `CR 56` (in clj/app sub-rules) |
| `INT` (top-level integer) | 383 | `GR 31`, `CR 12` |
| `INT.INT[A]` (alpha suffix) | 22 | `RAP 13.5A`, `RAP 18.13A`, `RPC 1.0A`, `RPC 1.0B`, `RPC 1.15A` |
| `INT[A]` | 9 | `RAP 12A`, `RAP 15A`-`15D` |
| Range entries (placeholder) | 8 | `RAP 18.18 - 18.20`, `CRLJ 27-37`, `CRLJ 86.04--99.04` |
| `Canon N` | 4 | `CJC Canon 1` ... `CJC Canon 4` |
| 3-segment numbering (`INT.INT.INT`) | 8 | `CrR 3.2.1`, `CrRLJ 6.1.1`, `RALJ 6.3.1` |
| `Stds` / `Appx` / `Standards` suffix | 8 | `CrR 3.1 Stds`, `GR 14_Appx`, `GR 26_Stds`, `GR 26_Standards`, `JuCR 9.2 Stds` |
| `INT[A].INT` (juvenile rules) | 6 | `JuCR 5A.1` ... `JuCR 5A.6` |
| Irregular text IDs | 5 | `APR Regs`, `APR LP_REGS`, `APR LP_DIS_REGS`, `ELLLTC`, `ELPOC`, `LPORPC` |

The `INT[A].INT` `JuCR 5A.x` form is genuinely 4-character: integer + alpha + dot + integer. Citation regex must allow `5A` as a "chapter" segment, then `.6` as a sub-rule.

## Implications for Phase 2 parser

1. Acquisition is **PDF-only**. No HTML body view exists. Plan for `pdftotext -layout` extraction.
2. List pages are reliable rule-set indices and should drive the crawl (1 list page → N PDFs).
3. Rule-set "code" can contain a space (`LLLT RPC`) — URL-encode as `%20`.
4. CJC requires special handling: list page reports 4 Canons but body PDFs contain nested `RULE 1.1` ... etc. The parser must descend.
5. Range entries (e.g., `CRLJ 27-37`) need normalization decision — collapse to one `[RESERVED.]` document or expand to individual rule slots.
6. `Stds` / `Appx` / `Standards` companions are real documents, not numbered rules; treat as appendix-class artifacts under the parent rule.
7. Mono-PDF rule sets (`ELLLTC`, `ELPOC`, `LPORPC`, `BJA`, etc.) cannot be enumerated rule-by-rule from the index; the rules are inside the single document and would require PDF-level chunking.
