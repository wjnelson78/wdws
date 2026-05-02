"""
RCW HTML Parser — Phase 3 deliverable.

Parses the chapter HTML pages cached at /opt/wdws/data/rcw/html/chapters/ (the
&full=true view from app.leg.wa.gov/RCW/) into structured records ready for
Phase 4 ingest.

The parser is read-only: it produces Python dataclasses. Persistence to
PostgreSQL is Phase 4's job.

Source structure (from Phase 0 §2.3):
    <h1><!-- field: Citations -->Chapter T.CC RCW<!-- field: --></h1>
    <h2><!-- field: CaptionsTitles --><div>HEADING</div><!-- field: --></h2>
    ... TOC table ...
    For each section:
        <a name='T.CC.SSS' ></a>
        <div><h3>...RCW <a href='...cite=T.CC.SSS'>T.CC.SSS</a></h3></div>
        <div><h3>Section caption.</h3></div>
        <div>
            <div style="text-indent:0.5in;">(1) Body text...</div>
            ...
        </div>
        <div style="margin-top:15pt;margin-bottom:0pt;">[ session-law cites ]</div>
        <div style="margin-top:0.25in;margin-bottom:0.25in;"><h3>NOTES:</h3></div>
        <div style="margin-bottom:0.2in;">
            <div style="text-indent:0.75in;">
                <span style="font-weight:bold;">[Note-type]<dash>[topic]:</span> "[text]"
                [ <a>session law</a>.]
            </div>
        </div>

Output dataclasses mirror the rcw.* schema applied in Phase 1.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from lxml import html as LH
from lxml.etree import _Element


# ---------------------------------------------------------------------------
# Output dataclasses (mirror the rcw.* schema)
# ---------------------------------------------------------------------------

@dataclass
class CrossRef:
    target_citation: str
    target_jurisdiction: str   # 'state-wa-rcw' | 'federal-usc' | 'wa-constitution' | 'session-law' | 'wac' | 'cfr'
    ref_type: str              # 'reference' | 'recodification' | 'cross-statute' | 'see-also' | 'authority'
    context_snippet: str = ''


@dataclass
class HistoryEntry:
    session_law: str           # raw, e.g. '2017 c 303 s 3' or 'Code 1881 s 1' or 'RRS s 143'
    session_year: Optional[int]
    session_law_pdf: str = ''  # URL of the linked SL PDF (if any)
    action_type: Optional[str] = None       # 'enacted' | 'amended' | 'recodified' | 'repealed' | 'reorganized'
    action_summary: str = ''                # parenthetical context, e.g. '(Initiative Measure No. 276, …)'
    sort_order: int = 0


@dataclass
class NoteEntry:
    note_type: str             # 'Reviser\'s note', 'Findings', 'Effective date', etc.
    note_topic: str = ''       # session-law cite the note attaches to, e.g. '2010 c 69'
    note_heading: str = ''     # full heading as rendered, e.g. 'Finding—2010 c 69'
    note_text: str = ''        # body of the note (may include trailing session-law bracket)
    sort_order: int = 0


@dataclass
class SectionRecord:
    section_number: str        # '030', '094A.030', '040A'
    canonical_citation: str    # 'RCW 42.56.030'
    section_heading: str
    full_text: str             # plain-text rendering of subsection paragraphs
    source_html: str           # raw HTML slice for the section (for debug / re-parse)
    is_repealed: bool = False
    is_recodified: bool = False
    is_decodified: bool = False
    is_expired: bool = False
    is_reserved: bool = False
    recodified_to: Optional[str] = None
    # Pending recodification — pre-effective announcement in the section heading
    # (e.g., "Declaration of policy. (Effective until January 1, 2026. Recodified
    # as RCW 29B.05.010.)"). Both fields are NULL together, or both populated
    # (paired-NULL CHECK in schema). Phase 7 maintenance promotes pending →
    # applied when pending_recodification_effective <= CURRENT_DATE.
    pending_recodification_to:        Optional[str] = None
    pending_recodification_effective: Optional[str] = None  # ISO 'YYYY-MM-DD'
    # When the source-credit bracket ends with "Formerly RCW X.YY.ZZZ", this
    # CURRENT section was previously known as X. Phase 4 uses this to emit a
    # recodification CrossRef from this section to the prior citation, AND a
    # ghost rcw.sections row for X with is_recodified=TRUE, recodified_to=current.
    recodified_from: Optional[str] = None
    enacted_by_session_law: Optional[str] = None  # earliest cite from history (acts as "enacted by" anchor)
    effective_date: Optional[str] = None          # 'YYYY-MM-DD' if explicit Effective date note
    unit_type: str = 'section'
    history: list[HistoryEntry] = field(default_factory=list)
    notes: list[NoteEntry] = field(default_factory=list)
    cross_refs: list[CrossRef] = field(default_factory=list)


@dataclass
class ChapterRecord:
    title_number: str          # '4', '9A', '42'
    chapter_number: str        # '4.04', '9A.04', '42.56'
    chapter_heading: str
    publication_date: str      # 'YYYY-MM-DD' — propagated from caller
    is_reserved_chapter: bool = False  # entire chapter has no live sections
    sections: list[SectionRecord] = field(default_factory=list)


@dataclass
class DispositionEntry:
    section_number: str        # '010', '040'
    section_caption: str       # heading text from disposition page
    canonical_citation: str    # 'RCW 42.17.010'
    is_repealed: bool = False
    is_recodified: bool = False
    is_decodified: bool = False
    is_expired: bool = False
    is_reserved: bool = False
    recodified_to: Optional[str] = None     # target citation (if recodified)
    repealing_session_law: Optional[str] = None
    effective_date: Optional[str] = None    # ISO date when known
    raw_disposition_text: str = ''          # full sentence, for audit


@dataclass
class DispositionRecord:
    title_number: str
    chapter_number: str
    chapter_heading: str
    is_empty: bool = False     # no disposition entries (most chapters)
    entries: list[DispositionEntry] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Citation regex catalog — used both by the section parser and the cross-ref
# detector inside section body text.
# ---------------------------------------------------------------------------

# Anchor name attribute. Two forms:
#   Standard:   <a name='4.04.010' >, <a name='9A.04.020' >
#   UCC dash:   <a name='62A.1-101' >, <a name='62A.9A-203' >  (Title 62A only)
# The character class [.\-] accepts either separator between the chapter and
# section parts. Anchor extraction is permissive; cross-reference TEXT detection
# (below) uses scoped alternation to avoid false positives on dash-bridged
# range references like 'RCW 4.04 to 4.06' in body text.
ANCHOR_RE = re.compile(r"<a name='([0-9][0-9A-Z]*\.[0-9A-Z]+(?:[.\-][0-9A-Z]+)?)'\s*>", re.IGNORECASE)

# RCW section citations within text bodies. Scoped alternation:
#   - UCC branch: hardcoded `62A\.` literal + dash form (Title 62A only)
#   - Standard branch: three dot-separated groups (all other titles)
# The mutual exclusion is structural, not order-dependent. The `\bRCW\s+` prefix
# eliminates false positives from raw date ranges, page ranges, and session-law
# citations. See parser_design_notes.md §"Cross-reference detection" for the
# full false-positive walkthrough.
RCW_SECTION_TEXT_RE = re.compile(
    r'\bRCW\s+'
    r'('
        r'62A\.[0-9]+[A-Z]*-[0-9]+[A-Z]*'              # UCC dash-form, Title 62A ONLY
        r'|'
        r'[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+'        # Standard T.CC.SSS dot-form
    r')'
    r'(?:\([0-9a-zA-Z]+\))*'                            # optional subsection suffix (1)(a)(i)
)

# Chapter form: chapter 4.04 RCW
RCW_CHAPTER_TEXT_RE = re.compile(
    r'\bchapter\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW\b', re.IGNORECASE,
)

# Title form: Title 4 RCW, Title 9A RCW
RCW_TITLE_TEXT_RE = re.compile(
    r'\bTitle\s+([0-9]+[A-Z]?)\s+RCW\b',
)

# Federal USC: 42 U.S.C. § 1983, 28 U.S.C. § 1331
USC_TEXT_RE = re.compile(
    r'\b([0-9]+[A-Z]?)\s+U\.S\.C\.\s*§+\s*([0-9A-Z]+(?:\([0-9a-zA-Z]+\))*[a-zA-Z\-]*)',
)

# CFR: 28 CFR § 35.107, 28 C.F.R. § 35.107
CFR_TEXT_RE = re.compile(
    r'\b([0-9]+)\s+C\.?F\.?R\.?\s*§+\s*([0-9]+(?:\.[0-9]+)*)',
)

# WA Constitution: Article I § 7, art. I, § 7
WA_CONST_TEXT_RE = re.compile(
    r"\bArticle\s+([IVX]+)(?:,?\s+§+\s*([0-9]+))?\s+of\s+(?:the\s+)?(?:Washington\s+state\s+|state\s+)?Constitution",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Session-law citation parsing (inside [ ... ] source-credit brackets and
# standalone in note brackets).
# ---------------------------------------------------------------------------

# Modern: "1995 c 397 s 15", "2017 c 303 s 3", "1975 1st ex.s. c 294 s 18"
SL_MODERN_RE = re.compile(
    r'^\s*([0-9]{4})\s+(\d+(?:st|nd|rd|th)?\s+(?:ex\.s\.|sp\.s\.|extra\s+session)\s+)?c\s+(\d+[A-Z]?)\s+s\s+(\d+)\s*$',
    re.IGNORECASE,
)
# Older: "Code 1881 s 1", "RRS s 143"  (page-based: "1862 p 83 s 1", "1877 p 3 s 1")
SL_PAGE_RE = re.compile(r'^\s*([0-9]{4})\s+p\s+\d+\s+s\s+\d+\s*$')
SL_CODE_RE = re.compile(r'^\s*Code\s+([0-9]{4})\b', re.IGNORECASE)
SL_RRS_RE  = re.compile(r'^\s*(?:RRS|Pierce.s\s+Code|Hill.s\s+Code)\b', re.IGNORECASE)


def parse_session_year(raw: str) -> Optional[int]:
    raw = raw.strip()
    for rx in (SL_MODERN_RE, SL_PAGE_RE, SL_CODE_RE):
        m = rx.match(raw)
        if m:
            try: return int(m.group(1))
            except (IndexError, ValueError): pass
    if SL_RRS_RE.match(raw):
        return None  # RRS/Pierce/Hill citations have no useful year
    # Fallback: extract first 4-digit year
    m = re.search(r'\b(1[78][0-9]{2}|19[0-9]{2}|20[0-9]{2})\b', raw)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Chapter file parsing
# ---------------------------------------------------------------------------

def _extract_h1_chapter(doc: _Element) -> tuple[str, str]:
    """Returns (title_number, chapter_number) parsed from the chapter HTML.

    Primary source: <h1> element containing 'Chapter T.CC RCW'.

    UCC fallback: Title 62A chapters use <h1>Article N</h1> instead (national
    UCC convention). For these, fall back to the <title> element which reliably
    contains 'Chapter T.CC RCW: ...' regardless of UCC/non-UCC chapter type.
    """
    # Primary: <h1>
    h1 = doc.xpath("//h1")
    for el in h1:
        text = ' '.join(el.text_content().split())
        m = re.search(r'Chapter\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW', text)
        if m:
            chapter = m.group(1)
            title = chapter.split('.')[0]
            return title, chapter

    # Fallback: <title> element (covers UCC chapters with <h1>Article N</h1>)
    title_el = doc.xpath("//title")
    for el in title_el:
        text = ' '.join(el.text_content().split())
        m = re.search(r'Chapter\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW', text)
        if m:
            chapter = m.group(1)
            title = chapter.split('.')[0]
            return title, chapter

    raise ValueError("Could not locate chapter identifier in <h1> or <title>")


def _extract_h2_caption(doc: _Element) -> tuple[str, bool]:
    """Returns (caption_text, is_reserved)."""
    h2_elements = doc.xpath("//h2")
    for el in h2_elements:
        # Skip nav/footer h2s — only the one inside #ContentPlaceHolder1_pnlTitleBlock
        ancestors = el.xpath("ancestor::*[@id='ContentPlaceHolder1_pnlTitleBlock']")
        if not ancestors:
            continue
        text = ' '.join(el.text_content().split())
        is_reserved = '(RESERVED)' in text or '(Reserved)' in text
        # Strip the (RESERVED) marker for canonical caption
        clean = re.sub(r'\s*\(RESERVED\)\s*', '', text, flags=re.IGNORECASE).strip()
        return clean, is_reserved
    return '', False


def _render_table_as_text(table_el: _Element) -> str:
    """Render an HTML <table> as pipe-separated rows, newline-separated.

    Used for sections whose body is a table (RCW 9.94A.510 sentencing grid,
    RCW 26.19 child support tables, etc.). The rendering preserves cell
    content for full-text search and embedding while losing visual styling.
    """
    rows = []
    for tr in table_el.xpath('.//tr'):
        cells = []
        for cell in tr.xpath('./td | ./th'):
            txt = cell.text_content()
            txt = re.sub(r'\s+', ' ', txt).strip()
            cells.append(txt)
        if any(c for c in cells):
            rows.append(' | '.join(cells))
    return '\n'.join(rows)


def _section_anchor_iterator(doc: _Element):
    """Yields (anchor_element, citation_text) for each section anchor.

    Recognizes two forms:
      Standard:  <a name='4.04.010' >    (T.CC.SSS dot form, all titles except 62A)
      UCC dash:  <a name='62A.1-101' >   (Title 62A only — UCC convention)
    """
    sec_re = re.compile(r'^[0-9][0-9A-Z]*\.[0-9A-Z]+[.\-][0-9A-Z]+$')
    for a in doc.xpath("//a[@name]"):
        name = a.get('name', '')
        if name and sec_re.match(name):
            yield a, name


def _extract_section_body_and_credit(anchor_el: _Element, next_anchor_el: Optional[_Element]):
    """
    Walk siblings after the anchor until reaching next_anchor_el (or end of parent).
    Categorize each sibling div into: citation_h3, caption_h3, body_div, credit_div,
    notes_block.

    Returns dict with:
        'citation_text'  (str: 'RCW 4.04.010')
        'caption_text'   (str: 'Extent to which common law prevails.')
        'body_paragraphs' (list[str]: each paragraph's text)
        'body_html'      (str: HTML slice of the body div)
        'credit_text'    (str: '[ ... ]' bracket contents, no outer brackets)
        'credit_links'   (list[(text, url)] from <a> in the credit div)
        'notes_blocks'   (list[(heading, body_text, note_link)] from each note div)
        'recodified_to'  (str | None: if the credit contains 'Formerly RCW X.YY.ZZZ')
    """
    cite_text   = ''
    caption_text = ''
    body_paragraphs: list[str] = []
    body_html   = ''
    credit_text = ''
    credit_links: list[tuple[str, str]] = []
    notes: list[tuple[str, str, list[tuple[str, str]]]] = []  # (heading, text, links_in_note)
    recodified_to: Optional[str] = None

    # We walk through subsequent siblings of the anchor's parent context until
    # we hit the next anchor. Anchors live inside <div> wrappers, so we need to
    # follow the document order of subsequent elements.
    cur = anchor_el
    in_notes = False
    seen_h3 = 0
    while True:
        cur = cur.getnext()
        if cur is None:
            # Move up to parent and continue with parent's next sibling
            parent = anchor_el.getparent()
            cur = parent.getnext() if parent is not None else None
            if cur is None:
                break
            anchor_el = parent  # update so we keep walking outward correctly
        # Stop when we cross the next section anchor
        if next_anchor_el is not None:
            anchors_inside = cur.xpath(".//a[@name]") if hasattr(cur, 'xpath') else []
            for a in anchors_inside:
                nm = a.get('name', '')
                if re.match(r'^[0-9][0-9A-Z]*\.[0-9A-Z]+[.\-][0-9A-Z]+$', nm) and a is next_anchor_el:
                    return {
                        'citation_text': cite_text,
                        'caption_text': caption_text,
                        'body_paragraphs': body_paragraphs,
                        'body_html': body_html,
                        'credit_text': credit_text,
                        'credit_links': credit_links,
                        'notes_blocks': notes,
                        'recodified_to': recodified_to,
                    }
            # Also stop if `cur` itself contains the next anchor at any depth
            if any(a is next_anchor_el for a in anchors_inside):
                return {
                    'citation_text': cite_text,
                    'caption_text': caption_text,
                    'body_paragraphs': body_paragraphs,
                    'body_html': body_html,
                    'credit_text': credit_text,
                    'credit_links': credit_links,
                    'notes_blocks': notes,
                    'recodified_to': recodified_to,
                }

        # Identify what `cur` is and accumulate
        tag = cur.tag.lower() if hasattr(cur, 'tag') else None
        if tag != 'div':
            continue

        # Citation/caption h3s
        h3s = cur.xpath(".//h3")
        if h3s and seen_h3 < 2 and not in_notes:
            for h3 in h3s:
                txt = ' '.join(h3.text_content().split())
                if txt.upper() == 'NOTES:':
                    in_notes = True
                    continue
                if seen_h3 == 0:
                    # Citation H3 — text format is "PDFRCW T.CC.SSS" (PDF button + cite,
                    # no space) or "PDFRCW 62A.1-101" for UCC. Use scoped alternation
                    # matching the section-citation forms (parallel to RCW_SECTION_TEXT_RE).
                    m = re.search(
                        r'RCW\s+(62A\.[0-9]+[A-Z]*-[0-9]+[A-Z]*'
                        r'|[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)',
                        txt,
                    )
                    cite_text = f'RCW {m.group(1)}' if m else txt
                    seen_h3 = 1
                elif seen_h3 == 1:
                    caption_text = txt
                    seen_h3 = 2
            if seen_h3 >= 2 and not in_notes:
                continue

        # Body, credit, notes — distinguish by inline-style and content
        style = (cur.get('style') or '').lower()
        inner_html = LH.tostring(cur, encoding='unicode')

        # Source credit: contains [ ... ] and starts with margin-top:15pt
        if 'margin-top:15pt' in style and 'margin-bottom:0pt' in style:
            text = cur.text_content().strip()
            if text.startswith('[') and text.endswith(']'):
                credit_text = text[1:-1].strip()
                # Capture links
                for a in cur.xpath('.//a'):
                    href = a.get('href', '')
                    txt  = ' '.join(a.text_content().split())
                    credit_links.append((txt, href))
                # Detect "Formerly RCW X.YY.ZZZ" — the current section was previously
                # known as X. Caller stores this as recodified_FROM on the current section;
                # Phase 4 creates the ghost row for X with is_recodified=TRUE, recodified_to=current.
                m = re.search(r'Formerly\s+RCW\s+([0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)', text)
                if m:
                    recodified_to = f'RCW {m.group(1)}'  # variable retained for back-compat; semantically this is "recodified_from" — the caller renames it
                continue

        # Notes header block: <h3>NOTES:</h3> wrapped in margin div
        if 'margin-top:0.25in' in style and cur.xpath(".//h3"):
            in_notes = True
            continue

        # Notes body: margin-bottom:0.2in OR inside the notes section
        if in_notes and 'margin-bottom:0.2in' in style:
            inner_div = cur.xpath(".//div[contains(@style,'text-indent:0.75in')]")
            if inner_div:
                d = inner_div[0]
                # Heading is the first <span style="font-weight:bold;">
                heading_span = d.xpath(".//span[contains(@style,'font-weight:bold')]")
                heading_text = ' '.join(heading_span[0].text_content().split()).rstrip(':') if heading_span else ''
                # Body text is everything after the heading span
                body_text = d.text_content()
                # Strip the heading text from the body
                if heading_text:
                    body_text = body_text.replace(heading_text + ':', '', 1).strip()
                else:
                    body_text = body_text.strip()
                # Capture session-law links inside the note body
                links: list[tuple[str, str]] = []
                for a in d.xpath('.//a'):
                    href = a.get('href', '')
                    txt  = ' '.join(a.text_content().split())
                    links.append((txt, href))
                notes.append((heading_text, body_text, links))
            continue

        # Otherwise: body. Capture body_html and walk its content.
        # Body div is the FIRST sibling div after the captions that contains
        # either nested <div style="text-indent:..."> paragraphs (any indent
        # value) OR a <table> (the rare form, used in sections like RCW
        # 9.94A.510 sentencing grid, RCW 9.94A.515 crime seriousness table,
        # RCW 26.19.* child support tables).
        #
        # The xpath matches ANY text-indent value (not just 0.5in) to capture:
        #   - 0.5in: canonical body paragraphs (174,671 occurrences corpus-wide)
        #   - 0pt:   RCW 24.03A nonprofit corp template (591 occurrences, 31 sections)
        #   - 1in:   sub-points in form-affidavits (e.g., RCW 11.125.430 POA)
        #   - 1.5in: TEDRA form templates (RCW 11.96A.130 etc.)
        #   - -25px: hanging-indent compact provisions (RCW 17.34, 11.140)
        # Notes-style 0.75in is excluded by the in_notes guard at line ~473
        # (notes-header div flips in_notes BEFORE body extraction sees them);
        # the gating is structural, not indent-value-based, so this xpath
        # broadening is safe. See parser_design_notes.md and Phase 6 §K.
        if not in_notes and not body_html:
            paras = cur.xpath(".//div[contains(@style,'text-indent:')]")
            tables = cur.xpath(".//table")
            if paras:
                body_html = inner_html
                for p in paras:
                    txt = p.text_content()
                    txt = re.sub(r'\s+', ' ', txt).strip()
                    if txt:
                        body_paragraphs.append(txt)
                # Tables MAY appear alongside text-indent paragraphs in some
                # sections (e.g., a paragraph followed by a table). Append
                # any tables in document order using pipe-separated rows.
                for tbl in tables:
                    rendered = _render_table_as_text(tbl)
                    if rendered:
                        body_paragraphs.append(rendered)
                continue
            elif tables:
                # Table-only body (no text-indent paragraphs at all).
                # Render each table as pipe-separated rows; section heading
                # plus column-and-cell content forms substantive law
                # (e.g., sentencing grid in RCW 9.94A.510).
                body_html = inner_html
                for tbl in tables:
                    rendered = _render_table_as_text(tbl)
                    if rendered:
                        body_paragraphs.append(rendered)
                # If still nothing extracted (extremely rare), fall back
                # to the div's full text_content().
                if not body_paragraphs:
                    txt = cur.text_content()
                    txt = re.sub(r'\s+', ' ', txt).strip()
                    if txt:
                        body_paragraphs.append(txt)
                continue

    return {
        'citation_text': cite_text,
        'caption_text': caption_text,
        'body_paragraphs': body_paragraphs,
        'body_html': body_html,
        'credit_text': credit_text,
        'credit_links': credit_links,
        'notes_blocks': notes,
        'recodified_to': recodified_to,
    }


def _parse_credit(credit_text: str, credit_links: list[tuple[str, str]]) -> list[HistoryEntry]:
    """Parse the contents of a [ ... ] source-credit bracket into HistoryEntry list.

    Format: 'YYYY c NN s S; YYYY c NN s S; ... ; Formerly RCW X.YY.ZZZ'
    Some entries include parenthetical commentary, e.g. '(Initiative Measure No. 276, ...)'.
    """
    if not credit_text:
        return []

    # Strip the "Formerly RCW X.YY.ZZZ" suffix if present
    formerly_re = re.compile(r'\s*Formerly\s+RCW\s+[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+\.?\s*$')
    credit_text = formerly_re.sub('', credit_text)

    # Split on semicolons that aren't inside parentheses
    entries: list[str] = []
    depth = 0
    cur = []
    for ch in credit_text:
        if ch == '(':
            depth += 1; cur.append(ch)
        elif ch == ')':
            depth -= 1; cur.append(ch)
        elif ch == ';' and depth == 0:
            entries.append(''.join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        entries.append(''.join(cur).strip())

    # Build URL lookup from credit_links
    link_map = {txt: url for txt, url in credit_links}

    history: list[HistoryEntry] = []
    for i, entry in enumerate(entries):
        if not entry: continue
        # Strip trailing period BEFORE looking for parenthetical (parens are sometimes
        # followed by a period, e.g. '1973 c 1 s 32 (Initiative Measure No. 276...).')
        entry = entry.rstrip('. \t')
        # Extract parenthetical action_summary
        action_summary = ''
        m = re.search(r'\s*\(([^)]+)\)\s*$', entry)
        if m:
            action_summary = m.group(1)
            entry = entry[:m.start()].strip()
        session_law = entry.rstrip('.').strip()
        url = link_map.get(session_law, '')
        # Heuristic action_type: oldest (last in list per RCW convention) is "enacted",
        # others are "amended". Phase 4 may revise based on disposition data.
        action_type = 'enacted' if i == len(entries) - 1 else 'amended'
        history.append(HistoryEntry(
            session_law=session_law,
            session_year=parse_session_year(session_law),
            session_law_pdf=url,
            action_type=action_type,
            action_summary=action_summary,
            sort_order=i + 1,  # 1 = most recent
        ))
    return history


def _parse_notes(notes_blocks) -> list[NoteEntry]:
    """Convert raw note blocks into NoteEntry records.

    Heading patterns observed in Phase 0 §2.7:
        Reviser's note:
        Findings—{year c chap}:
        Effective date—{year c chap}:
        Construction—{year c chap}:
        Severability—{year c chap}:
        Application—{year c chap}:
        Short title—{year c chap}:
        Conflict with federal requirements—{year c chap}:
        Expiration date—{year c chap}:
        Finding—{year c chap}:
        Legislative findings—{year c chap}:
        Purpose—{year c chap}:

    Em-dash separator is U+2014 (—) in the source; the heading also uses HTML
    entities for the dash. Be permissive on dash variants.
    """
    out: list[NoteEntry] = []
    for i, (heading, body, links) in enumerate(notes_blocks):
        # Split heading on em-dash to extract type vs topic
        # Try multiple dash variants
        parts = re.split(r'\s*[—–\-—]\s*', heading, maxsplit=1)
        if len(parts) == 2:
            note_type = parts[0].strip()
            note_topic = parts[1].strip()
        else:
            note_type = heading.strip()
            note_topic = ''
        # Strip trailing colon
        note_type = note_type.rstrip(':')
        note_topic = note_topic.rstrip(':')
        out.append(NoteEntry(
            note_type=note_type,
            note_topic=note_topic,
            note_heading=heading,
            note_text=body,
            sort_order=i + 1,
        ))
    return out


def _detect_cross_refs(text: str, body_html: str) -> list[CrossRef]:
    """Detect cross-citations within body text + linked anchors in body_html."""
    refs: list[CrossRef] = []
    # State RCW section refs
    for m in RCW_SECTION_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'RCW {m.group(1)}',
            target_jurisdiction='state-wa-rcw',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # State RCW chapter refs
    for m in RCW_CHAPTER_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'chapter {m.group(1)} RCW',
            target_jurisdiction='state-wa-rcw',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # State RCW title refs
    for m in RCW_TITLE_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'Title {m.group(1)} RCW',
            target_jurisdiction='state-wa-rcw',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # Federal USC
    for m in USC_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'{m.group(1)} U.S.C. § {m.group(2)}',
            target_jurisdiction='federal-usc',
            ref_type='cross-statute',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # Federal CFR
    for m in CFR_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'{m.group(1)} CFR § {m.group(2)}',
            target_jurisdiction='cfr',
            ref_type='cross-statute',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # WA Constitution
    for m in WA_CONST_TEXT_RE.finditer(text):
        section_part = f' § {m.group(2)}' if m.group(2) else ''
        refs.append(CrossRef(
            target_citation=f'Wash. Const. art. {m.group(1)}{section_part}',
            target_jurisdiction='wa-constitution',
            ref_type='authority',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    return refs


def parse_chapter_html(filepath: str | Path, publication_date: str = '2025-08-15') -> ChapterRecord:
    """Parse a single chapter HTML file into a ChapterRecord.

    Args:
        filepath: path to chap_{N.NN}_FULL.html
        publication_date: ISO date of the RCW publication this chapter is from

    Returns: ChapterRecord with sections list (possibly empty for reserved chapters).
    """
    filepath = Path(filepath)
    raw = filepath.read_bytes()
    # Decode as UTF-8 explicitly — leg.wa.gov declares Content-Type: text/html; charset=utf-8
    # but lxml.fromstring(bytes) without an explicit charset can misdetect for these files.
    text = raw.decode('utf-8', errors='replace')
    doc = LH.fromstring(text)

    title_number, chapter_number = _extract_h1_chapter(doc)
    chapter_heading, is_reserved = _extract_h2_caption(doc)

    rec = ChapterRecord(
        title_number=title_number,
        chapter_number=chapter_number,
        chapter_heading=chapter_heading,
        publication_date=publication_date,
        is_reserved_chapter=is_reserved,
    )

    # Section iteration
    anchors = list(_section_anchor_iterator(doc))
    if not anchors:
        # Empty chapter — could be reserved, or could be entirely-recodified-out
        return rec

    for i, (a, name) in enumerate(anchors):
        next_a = anchors[i + 1][0] if i + 1 < len(anchors) else None
        info = _extract_section_body_and_credit(a, next_a)

        # Skip synthetic part-heading anchors. The Code Reviser uses anchors
        # like <a name='42.17A.89999'></a><div style="text-align:center">CONSTRUCTION</div>
        # as in-page navigation dividers between part-heading sections of a
        # chapter. They match the section-anchor regex but have no citation H3,
        # no caption H3, no body, no source credit, no notes — emitting a
        # SectionRecord for one would violate chk_section_text_status_invariant
        # at INSERT time. Detection is structural: real sections always emit at
        # least the citation H3 and a body or credit. See parser_design_notes.md
        # §"Synthetic part-heading anchors" and Phase 6 §K (audit-predicate
        # diversification — schema CHECK as load-bearing audit channel).
        if (not info['citation_text']
                and not info['caption_text']
                and not info['body_paragraphs']
                and not info['credit_text']
                and not info['notes_blocks']):
            continue

        # Section number derivation:
        #   Standard form '4.04.010'  → '010'    (drop title.chapter prefix)
        #   UCC form '62A.1-101'      → '1-101'  (preserve dash-form atomic identifier;
        #                                          drop only title prefix per operator
        #                                          decision: dash is part of the section,
        #                                          not a chapter/section separator)
        if '-' in name:
            # UCC: split on first dot only, take suffix
            section_number = name.split('.', 1)[1]
        else:
            section_number = name.split('.', 2)[2]

        history = _parse_credit(info['credit_text'], info['credit_links'])
        notes_records = _parse_notes(info['notes_blocks'])

        full_text = '\n\n'.join(info['body_paragraphs'])
        cross_refs = _detect_cross_refs(full_text, info['body_html'])

        sec = SectionRecord(
            section_number=section_number,
            canonical_citation=info['citation_text'] or f'RCW {name}',
            section_heading=info['caption_text'].rstrip('.'),
            full_text=full_text,
            source_html=info['body_html'],
            history=history,
            notes=notes_records,
            cross_refs=cross_refs,
        )
        # The _extract_section_body_and_credit() returns the "Formerly RCW X" cite in
        # info['recodified_to'] — semantically that's recodified_FROM the prior citation.
        if info.get('recodified_to'):
            sec.recodified_from = info['recodified_to']
            # Also emit a CrossRef of type 'recodification' so Phase 4 can populate
            # rcw.cross_references and resolve the ghost row.
            sec.cross_refs.append(CrossRef(
                target_citation=sec.recodified_from,
                target_jurisdiction='state-wa-rcw',
                ref_type='recodification',
                context_snippet=f'Formerly {sec.recodified_from}',
            ))

        # Decodified-shell detection. Some sections exist on paper but have
        # been decodified by the Code Reviser per RCW 1.12.025 (when a section
        # is amended in one bill while simultaneously repealed by another,
        # without cross-reference). The Reviser preserves the citation+heading
        # shell with a Reviser's note explaining the conflict; the body is
        # intentionally absent.
        #
        # Detection signature (corpus-wide: 9 sections across 5 titles):
        #   - Citation H3 ✓ (already extracted)
        #   - Caption H3 ✓ (already extracted)
        #   - NO body content (full_text empty/whitespace)
        #   - NO source-credit bracket (info['credit_text'] is empty)
        #   - Reviser's note containing "decodified for publication purposes"
        #     (variants: "pursuant to RCW" or "under RCW")
        #
        # When all conditions hold, mark is_decodified=TRUE so the row
        # satisfies chk_section_text_status_invariant as a ghost row.
        # Counter-examples verified: real sections with "decodified" in notes
        # have body text AND source-credit brackets, so they never enter this
        # branch.
        if (info['citation_text']
                and info['caption_text']
                and not full_text.strip()
                and not info['credit_text']):
            for heading, body_text, _links in info['notes_blocks']:
                blob = (heading or '') + ' ' + (body_text or '')
                if re.search(r'decodified for publication purposes (?:pursuant to|under) RCW',
                             blob, re.IGNORECASE):
                    sec.is_decodified = True
                    break

        # Loose-text recodification detection. Singleton class-of-one observed
        # in RCW 70.96.150 (verified via corpus-wide scope check 2026-04-30):
        # Code Reviser preserves a pre-recodification shell with the loose
        # text "Repealed by ...; and subsequently recodified as RCW X.YY.ZZZ
        # pursuant to ..." between the credit </div> close and the notes
        # header — outside any structured div, so the div-walker misses it.
        #
        # Detection signature (6 conditions, all required):
        #   - citation_text set ✓
        #   - caption_text set ✓
        #   - body empty (no text-indent paragraphs found)
        #   - credit_text non-empty (real source-credit bracket present)
        #   - no other status flag already set (decodified, recodified, pending)
        #   - section's raw HTML byte range contains "recodified as RCW X"
        #
        # When all 6 hold, set is_recodified=TRUE, recodified_to=X. The row
        # becomes a recodified ghost (full_text=NULL, satisfies CHECK).
        #
        # Counter-example check: 7,466 sections with recodified_from (Formerly
        # RCW pointers) all have body text, so condition #3 (body empty) fails
        # for them — no collision. See parser_design_notes.md and Phase 6 §K
        # corroboration #8.
        if (info['citation_text']
                and info['caption_text']
                and not full_text.strip()
                and info['credit_text']
                and not sec.is_decodified
                and not sec.is_recodified
                and not sec.pending_recodification_to):
            # Find the section's raw HTML byte range
            anchor_marker_with_space = f"<a name='{name}' ></a>"
            anchor_marker_no_space   = f"<a name='{name}'></a>"
            anchor_start = text.find(anchor_marker_with_space)
            if anchor_start < 0:
                anchor_start = text.find(anchor_marker_no_space)
            if anchor_start >= 0:
                marker_len = (len(anchor_marker_with_space)
                              if text[anchor_start:anchor_start + len(anchor_marker_with_space)]
                              == anchor_marker_with_space
                              else len(anchor_marker_no_space))
                next_anchor_match = re.search(
                    r"<a name='[0-9]", text[anchor_start + marker_len:]
                )
                section_end = (anchor_start + marker_len
                               + (next_anchor_match.start() if next_anchor_match else 8000))
                section_raw = text[anchor_start:section_end]
                m = re.search(
                    r'recodified as RCW\s+(?:<a[^>]*>)?([0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)',
                    section_raw,
                )
                if m:
                    sec.is_recodified = True
                    sec.recodified_to = f'RCW {m.group(1)}'

        # Pending-recodification detection. Source pattern in section heading:
        #   "Declaration of policy. (Effective until January 1, 2026. Recodified
        #    as RCW 29B.05.010.)"
        # Per Phase 4 operator authorization (Option B): set the new
        # pending_recodification_{to,effective} fields, NOT is_recodified or
        # recodified_to. The current body text is preserved (section is still
        # effective). Phase 7 maintenance flips pending → applied when the
        # effective_date arrives.
        #
        # Note: ref_type='recodification' (not 'pending-recodification'); the
        # pending vs applied distinction lives in rcw.sections.pending_*
        # columns, not in cross_references.ref_type. Cross-ref consumers
        # interpret ref_type=recodification + source.pending_recodification_to
        # IS NOT NULL as "pending" (chain queryable, no schema CHECK changes).
        recod_match = re.search(
            r'Recodified as RCW\s+([0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)',
            info['caption_text'] or '',
        )
        if recod_match:
            sec.pending_recodification_to = f'RCW {recod_match.group(1)}'
            # Try to capture the effective_until date as ISO
            eff_match = re.search(
                r'Effective until ([A-Z][a-z]+\s+\d{1,2},\s+\d{4})',
                info['caption_text'] or '',
            )
            if eff_match:
                try:
                    from datetime import datetime
                    d = datetime.strptime(eff_match.group(1), '%B %d, %Y')
                    sec.pending_recodification_effective = d.strftime('%Y-%m-%d')
                except ValueError:
                    pass
            # If we got a target but no effective date, the paired-NULL CHECK
            # would reject the row at INSERT. Emit a synthetic effective_date
            # so the row is acceptable; Phase 7 maintenance handles edge cases.
            if sec.pending_recodification_effective is None:
                # Fallback: use a sentinel far-future date so the maintenance job
                # never auto-promotes without operator review.
                sec.pending_recodification_effective = '2099-01-01'
            # Emit a CrossRef so chain queries via rcw.cross_references find it
            sec.cross_refs.append(CrossRef(
                target_citation=sec.pending_recodification_to,
                target_jurisdiction='state-wa-rcw',
                ref_type='recodification',
                context_snippet=(
                    f'Pending recodification effective '
                    f'{sec.pending_recodification_effective}: '
                    f'this section becomes ghost on that date and the live '
                    f'citation moves to {sec.pending_recodification_to}'
                ),
            ))

        # Set "enacted_by_session_law" from the oldest history entry (last in list)
        if history:
            sec.enacted_by_session_law = history[-1].session_law
        # Look for "Effective date" note → effective_date (best-effort)
        for n in notes_records:
            if n.note_type.lower().startswith('effective date'):
                m = re.search(r'effective\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})', n.note_text, re.IGNORECASE)
                if m:
                    try:
                        from datetime import datetime
                        d = datetime.strptime(m.group(1), '%B %d, %Y')
                        sec.effective_date = d.strftime('%Y-%m-%d')
                    except ValueError:
                        pass
                break

        rec.sections.append(sec)

    return rec


# ---------------------------------------------------------------------------
# Disposition file parsing
# ---------------------------------------------------------------------------

def parse_disposition_html(filepath: str | Path) -> DispositionRecord:
    """Parse a chapter dispositions page (dispo.aspx?Cite=N.NN) into a record.

    Each disposition entry follows this pattern in the live HTML:
        <span><b>SSS Section caption.</b><br/>
            <blockquote>
                [session-law cites]
                {Repealed by ... | Recodified as RCW X.YY.ZZZ pursuant to ... | Reserved.}
            </blockquote>
        </span>
    """
    filepath = Path(filepath)
    raw = filepath.read_bytes()
    text = raw.decode('utf-8', errors='replace')
    doc = LH.fromstring(text)

    rec = DispositionRecord(title_number='', chapter_number='', chapter_heading='')

    # Pull title/chapter from H1
    h1 = doc.xpath("//h1")
    for el in h1:
        txt = ' '.join(el.text_content().split())
        m = re.search(r'Chapter\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW\s+dispositions', txt, re.IGNORECASE)
        if m:
            rec.chapter_number = m.group(1)
            rec.title_number = m.group(1).split('.')[0]
            break

    # H2 = chapter heading
    h2 = doc.xpath("//h2")
    for el in h2:
        ancestors = el.xpath("ancestor::*[@id='ContentPlaceHolder1_pnlContent']")
        if ancestors:
            rec.chapter_heading = ' '.join(el.text_content().split())
            break
    if not rec.chapter_heading:
        # Fallback: any non-nav h2
        for el in h2:
            txt = ' '.join(el.text_content().split())
            if txt and 'menu' not in txt.lower() and 'subject' not in txt.lower():
                rec.chapter_heading = txt
                break

    # Find the "<h3>Sections</h3>" or pnlContent block
    pnl = doc.xpath("//*[@id='ContentPlaceHolder1_pnlContent']")
    if not pnl:
        rec.is_empty = True
        return rec
    pnl_el = pnl[0]

    # Each entry is a <span> with <b>SSS Caption.</b><br/><blockquote>...
    entries: list[DispositionEntry] = []
    for span in pnl_el.xpath(".//span"):
        b = span.xpath("./b")
        if not b: continue
        b_text = ' '.join(b[0].text_content().split())
        # Format: 'SSS Caption.'
        m = re.match(r'^(\S+)\s+(.*?)\.?$', b_text)
        if not m: continue
        secnum, caption = m.group(1), m.group(2)
        # Filter: section number must look like a section (contains dots or digits)
        if not re.match(r'^[0-9][0-9A-Z.]*$', secnum):
            continue

        bq = span.xpath("./blockquote")
        bq_text = ' '.join(bq[0].text_content().split()) if bq else ''

        e = DispositionEntry(
            section_number=secnum,
            section_caption=caption,
            canonical_citation=f'RCW {rec.chapter_number}.{secnum}' if '.' not in secnum else f'RCW {secnum}',
            raw_disposition_text=bq_text,
        )

        bq_lower = bq_text.lower()
        # Recodified
        m2 = re.search(r'Recodified\s+as\s+RCW\s+([0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)', bq_text, re.IGNORECASE)
        if m2:
            e.is_recodified = True
            e.recodified_to = f'RCW {m2.group(1)}'
        # Repealed
        m3 = re.search(r'Repealed\s+by\s+([^.;]+?)(?:[.;]|$)', bq_text, re.IGNORECASE)
        if m3 and not e.is_recodified:
            e.is_repealed = True
            e.repealing_session_law = m3.group(1).strip()
        # Reserved
        if re.search(r'\bReserved\.?\s*$', bq_text, re.IGNORECASE) and not (e.is_recodified or e.is_repealed):
            e.is_reserved = True
        # Decodified
        if 'decodified' in bq_lower:
            e.is_decodified = True
        # Expired
        m4 = re.search(r'Expired\s+([A-Z][a-z]+\s+\d+,\s+\d{4})', bq_text)
        if m4:
            e.is_expired = True
        # Effective date for Recodified
        m5 = re.search(r'effective\s+([A-Z][a-z]+\s+\d+,\s+\d{4})', bq_text)
        if m5:
            try:
                from datetime import datetime
                d = datetime.strptime(m5.group(1), '%B %d, %Y')
                e.effective_date = d.strftime('%Y-%m-%d')
            except ValueError:
                pass

        entries.append(e)

    rec.entries = entries
    rec.is_empty = (len(entries) == 0)
    return rec


# ---------------------------------------------------------------------------
# Convenience / verification helpers
# ---------------------------------------------------------------------------

def count_section_anchors(filepath: str | Path) -> int:
    """Count <a name='X.YY.SSS'></a> anchors in a chapter file. Used for parser parity check."""
    filepath = Path(filepath)
    html = filepath.read_bytes()
    return len([m for m in ANCHOR_RE.finditer(html.decode('utf-8','replace'))])


if __name__ == '__main__':
    # Smoke test against RCW 4.04
    import sys
    p = '/opt/wdws/data/rcw/html/chapters/title_4/chap_4.04_FULL.html'
    rec = parse_chapter_html(p)
    print(f'Chapter: {rec.chapter_number} — {rec.chapter_heading}')
    print(f'Sections: {len(rec.sections)}')
    for s in rec.sections:
        print(f'  {s.canonical_citation}: {s.section_heading}')
        print(f'    body_paragraphs: {len(s.full_text.split(chr(10)+chr(10)))}')
        print(f'    history entries: {len(s.history)}')
        print(f'    notes:           {len(s.notes)}')
        print(f'    cross_refs:      {len(s.cross_refs)}')
