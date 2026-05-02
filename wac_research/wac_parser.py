"""
WAC HTML Parser — Phase 3 deliverable.

Parses the chapter HTML pages cached at /opt/wdws/data/wac/html/chapters/ (the
&full=true view from app.leg.wa.gov/WAC/) into structured records ready for
Phase 4 ingest.

Mirrors /opt/wdws/rcw_research/rcw_parser.py with WAC-specific extensions:

  1. Citation format uses DASHES (T-CC-SSS), not dots (T.CC.SSS).
  2. Statutory authority bracket [Statutory Authority: <RCW citations>.
     <WSR revision events>; ...] → splits into:
       - wac.authority_chain rows (RCW citations + flags)
       - wac.section_history rows (WSR events)
  3. Disposition table is INLINE in chapter &full=true (new structural class):
     <h3>Disposition of sections formerly codified in this title</h3>
     <table>...</table>
     Rows: <tr><td>SECTION_NUMBER</td><td>CAPTION + dispositions</td></tr>
  4. Disposition-only chapters: chapter exists, has dispositions, no active
     sections (e.g., 237-990, 296-134, 332-26, 365-06 cluster). New WAC
     structural class.
  5. WSR register format `WSR YY-NN-NNN` instead of session-law `YYYY c NN s S`.

Output dataclasses mirror the wac.* schema applied in Phase 1.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from lxml import html as LH
from lxml.etree import _Element


# ---------------------------------------------------------------------------
# Output dataclasses (mirror the wac.* schema)
# ---------------------------------------------------------------------------

@dataclass
class CrossRef:
    target_citation: str
    target_jurisdiction: str   # 'state-wa-rcw' | 'state-wa-wac' | 'federal-usc' | 'cfr' | 'wa-court-rule' | 'session-law' | 'wa-constitution'
    ref_type: str              # 'reference' | 'recodification' | 'cross-statute' | 'see-also' | 'authority'
    context_snippet: str = ''


@dataclass
class AuthorityChainEntry:
    """One row per RCW (or chapter-level RCW or session-law) citation in
    a WAC section's [Statutory Authority: ...] bracket.

    Maps to wac.authority_chain. is_chapter_level / is_session_law mutually
    exclusive (chk_authority_xor in schema).
    """
    authorizing_rcw: str       # 'RCW 49.60.120(3)' or 'RCW 49.04' (chapter) or '2020 c 277' (session-law)
    authority_text: str        # raw bracket-text excerpt up through first period
    is_chapter_level: bool = False
    is_session_law: bool = False
    sort_order: int = 0


@dataclass
class HistoryEntry:
    """One row per `WSR YY-NN-NNN, § T-CC-SSS, filed M/D/YY, effective M/D/YY`
    revision event from the authority bracket.

    Maps to wac.section_history.
    """
    wsr_register: str          # 'WSR 21-11-083'
    register_year: Optional[int]
    filed_date: Optional[str]      # ISO 'YYYY-MM-DD' or None
    effective_date: Optional[str]  # ISO 'YYYY-MM-DD' or None
    action_type: Optional[str] = None  # 'adopted' | 'amended' | 'repealed' | 'recodified' | 'decodified' | 'reorganized'
    action_summary: str = ''
    sort_order: int = 0


@dataclass
class NoteEntry:
    note_type: str
    note_topic: str = ''
    note_heading: str = ''
    note_text: str = ''
    sort_order: int = 0


@dataclass
class SectionRecord:
    section_number: str        # '001', '030', '4302' (rare 4-digit)
    canonical_citation: str    # 'WAC 296-15-001'
    section_heading: str
    full_text: str
    source_html: str
    is_repealed: bool = False
    is_recodified: bool = False
    is_decodified: bool = False
    is_expired: bool = False
    is_reserved: bool = False
    recodified_to: Optional[str] = None
    pending_recodification_to:        Optional[str] = None
    pending_recodification_effective: Optional[str] = None
    effective_date:        Optional[str] = None  # most recent revision's effective date
    wsr_register_number:   Optional[str] = None  # most recent WSR (sort_order=1)
    unit_type: str = 'section'
    history:         list[HistoryEntry]         = field(default_factory=list)
    authority_chain: list[AuthorityChainEntry]  = field(default_factory=list)
    notes:           list[NoteEntry]            = field(default_factory=list)
    cross_refs:      list[CrossRef]             = field(default_factory=list)


@dataclass
class DispositionEntry:
    """One row from the inline disposition table at the end of each chapter's
    &full=true view. Section was repealed/recodified/decodified — produces a
    GHOST row in wac.sections (full_text=NULL, status flag set).
    """
    section_number: str
    section_caption: str
    canonical_citation: str
    is_repealed: bool = False
    is_recodified: bool = False
    is_decodified: bool = False
    is_expired: bool = False
    is_reserved: bool = False
    recodified_to: Optional[str] = None
    repealing_wsr: Optional[str] = None
    effective_date: Optional[str] = None
    raw_disposition_text: str = ''
    history: list[HistoryEntry] = field(default_factory=list)


@dataclass
class ChapterRecord:
    title_number: str
    chapter_number: str
    chapter_heading: str
    publication_date: str
    is_disposition_only_chapter: bool = False    # all sections gone, only dispositions remain
    sections:     list[SectionRecord]     = field(default_factory=list)
    dispositions: list[DispositionEntry]  = field(default_factory=list)


# ---------------------------------------------------------------------------
# Citation regex catalog
# ---------------------------------------------------------------------------

# Section anchor: <a name='T-CC-SSS' [optional space]>
# T = digits + optional letter (e.g. 132A, 296)
# CC = digits + optional letter (e.g. 14A, 15)
# SSS = digits (3+ generally; some 4-digit forms like 296-15-4302)
ANCHOR_RE = re.compile(
    r"<a\s+name=['\"]"
    r"([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)"
    r"['\"]\s*>",
    re.IGNORECASE,
)

# WAC section citations within text bodies. Scoped alternation to avoid
# false positives from raw range strings like "296-15 to 296-17":
#   - require WAC prefix
#   - three dash-separated groups
WAC_SECTION_TEXT_RE = re.compile(
    r'\bWAC\s+'
    r'([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)'
    r'(?:\([0-9a-zA-Z]+\))*',
)

WAC_CHAPTER_TEXT_RE = re.compile(
    r'\bchapter\s+([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)\s+WAC\b',
    re.IGNORECASE,
)

# RCW citations (sections, chapters, titles, session-laws)
RCW_SECTION_TEXT_RE = re.compile(
    r'\bRCW\s+'
    r'(62A\.[0-9]+[A-Z]*-[0-9]+[A-Z]*'
    r'|[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)'
    r'(?:\([0-9a-zA-Z]+\))*'
)
RCW_CHAPTER_TEXT_RE = re.compile(
    r'\bchapter\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW\b', re.IGNORECASE,
)
# Plural form: 'chapters X.YY, Y.ZZ, ... and Z.AA RCW' — used in WAC authority
# brackets and notes; one match per chapter cite, scoped by trailing ' RCW'.
RCW_CHAPTERS_PLURAL_RE = re.compile(
    r'\bchapters?\s+((?:[0-9][0-9A-Z]*\.[0-9A-Z]+(?:\s*,\s*|\s+and\s+))*'
    r'[0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW\b',
    re.IGNORECASE,
)
RCW_TITLE_TEXT_RE = re.compile(
    r'\bTitle\s+([0-9]+[A-Z]?)\s+RCW\b',
)

# Federal USC, CFR, WA Constitution
USC_TEXT_RE = re.compile(
    r'\b([0-9]+[A-Z]?)\s+U\.S\.C\.\s*§+\s*([0-9A-Z]+(?:\([0-9a-zA-Z]+\))*[a-zA-Z\-]*)',
)
CFR_TEXT_RE = re.compile(
    r'\b([0-9]+)\s+C\.?F\.?R\.?\s*§+\s*([0-9]+(?:\.[0-9]+)*)',
)
WA_CONST_TEXT_RE = re.compile(
    r"\bArticle\s+([IVX]+)(?:,?\s+§+\s*([0-9]+))?\s+of\s+(?:the\s+)?(?:Washington\s+state\s+|state\s+)?Constitution",
    re.IGNORECASE,
)

# Session-law cite (used inside WAC authority brackets, occasionally):
# 'YYYY c NN', 'YYYY 1st sp.s. c NN', 'YYYY 1st sp.s. c NN § S'
SESSION_LAW_RE = re.compile(
    r'\b([0-9]{4})(?:\s+(\d+(?:st|nd|rd|th)?\s+(?:ex\.s\.|sp\.s\.|extra\s+session)))?\s+c\s+(\d+[A-Z]?)(?:\s+§\s*\d+)?\b',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Date parsing helpers
# ---------------------------------------------------------------------------

def _parse_short_date(s: str) -> Optional[str]:
    """Parse 'M/D/YY' or 'M/D/YYYY' to ISO 'YYYY-MM-DD'. Returns None on failure.

    WAC dates use 2-digit years; assume 1900s for years > 50, 2000s otherwise
    (inclusive heuristic: 1951-2050).
    """
    s = s.strip()
    for fmt in ('%m/%d/%y', '%m/%d/%Y'):
        try:
            d = datetime.strptime(s, fmt)
            # 2-digit year heuristic correction: %y interprets 00-68 as 2000-2068
            # and 69-99 as 1969-1999. Keep that default.
            return d.strftime('%Y-%m-%d')
        except ValueError:
            continue
    return None


def _wsr_year(wsr: str) -> Optional[int]:
    """Extract the YEAR from a WSR citation 'WSR 21-11-083' → 2021.

    YY-NN-NNN: 2-digit year + register-issue + filing-within-issue.
    Heuristic: 00-50 → 2000s, 51-99 → 1900s.
    """
    m = re.search(r'WSR\s+(\d{2})-\d+-\d+', wsr, re.IGNORECASE)
    if not m:
        return None
    yy = int(m.group(1))
    return 2000 + yy if yy <= 50 else 1900 + yy


# ---------------------------------------------------------------------------
# Statutory authority bracket parser
# ---------------------------------------------------------------------------

# Inside the bracket text (after stripping outer [ ]):
#   <authority-list>. <revision-event>; <revision-event>; ... .
# The first period separates authority list from revision events.
# Revision events are semicolon-separated; the last one ends with a period.

REVISION_EVENT_RE = re.compile(
    r'\bWSR\s+(\d{2}-\d+-\d+(?:\s+\([^)]+\))?)'      # WSR 21-11-083 or WSR 82-19-072 (Order 42)
    r'\s*,\s*§\s*([\d\-A-Za-z]+)'                     # § 296-15-001
    r'\s*,\s*filed\s+(\d{1,2}/\d{1,2}/\d{2,4})'       # filed 5/18/21
    r'(?:\s*,\s*effective\s+(\d{1,2}/\d{1,2}/\d{2,4}))?',  # effective 7/1/21 (optional)
    re.IGNORECASE,
)


def _parse_authority_list(authority_part: str) -> list[AuthorityChainEntry]:
    """Parse the authority-list portion of a [Statutory Authority: ...] bracket.

    authority_part is the plain text between 'Statutory Authority:' and the
    first WSR revision event.

    Format observed in samples:
      'RCW 49.60.120(3)'                          (single section)
      'RCW 51.04.020, 51.32.190 and 2020 c 277'   (multiple sections + session-law)
      'RCW 51.04.020, 51.04.030, and 2011 1st sp.s. c 37 § 305'  (Oxford-comma form)
      'chapter 49.04 RCW'                         (chapter-level)
      'chapters 51.04, 51.08, 51.12 RCW'          (multi-chapter)
      'RCW 49.60.120(3) and chapter 49.60 RCW'    (mixed; rare)

    Strategy:
      1. Extract chapter-level RCW (matches 'chapter[s] X.YY RCW')
      2. Mask out consumed spans
      3. Walk RCW-keyword positions; consume comma/and-extended section cites
      4. Extract session-law cites from remaining masked text
    """
    out: list[AuthorityChainEntry] = []
    sort_order = 1

    text = authority_part
    consumed_spans: list[tuple[int, int]] = []

    # 1. Chapter-level RCW citations (single 'chapter X.YY RCW' or plural form)
    for m in RCW_CHAPTERS_PLURAL_RE.finditer(text):
        chapters_part = m.group(1)
        chapter_tokens = re.split(r'\s*,\s*|\s+and\s+', chapters_part)
        for tok in chapter_tokens:
            tok = tok.strip()
            if re.match(r'^[0-9][0-9A-Z]*\.[0-9A-Z]+$', tok):
                out.append(AuthorityChainEntry(
                    authorizing_rcw=f'RCW {tok}',
                    authority_text=authority_part,
                    is_chapter_level=True,
                    sort_order=sort_order,
                ))
                sort_order += 1
        consumed_spans.append((m.start(), m.end()))

    # 2. Mask out chapter matches
    masked = list(text)
    for start, end in consumed_spans:
        for i in range(start, end):
            masked[i] = ' '
    masked_text = ''.join(masked)

    # 3. Section-level RCW citations with comma/and-extension. The pattern
    #    'RCW X.YY.ZZZ, A.BB.CCC, and D.EE.FFF' is one RCW keyword followed
    #    by 1+ comma/and-separated cites. After the FIRST cite, subsequent
    #    cites do NOT have an RCW prefix.
    section_cite_re = re.compile(
        r'([0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+(?:\([0-9a-zA-Z]+\))*)'
    )
    continuation_re = re.compile(r'\s*(?:,\s*and\s+|\s+and\s+|,\s*)')

    for m_kw in re.finditer(r'\bRCW\s+', masked_text):
        pos = m_kw.end()
        while True:
            m_cite = section_cite_re.match(masked_text, pos)
            if not m_cite:
                break
            full_cite = m_cite.group(1)
            out.append(AuthorityChainEntry(
                authorizing_rcw=f'RCW {full_cite}',
                authority_text=authority_part,
                is_chapter_level=False,
                is_session_law=False,
                sort_order=sort_order,
            ))
            sort_order += 1
            pos = m_cite.end()
            # Check for continuation (',' or ' and ' or ', and ') followed by
            # another section-form cite. If continuation matches but next
            # token is NOT a section cite (e.g., a session-law cite or the
            # word 'chapter'), break here — let session-law / chapter parsers
            # handle the rest.
            cont = continuation_re.match(masked_text, pos)
            if not cont:
                break
            next_pos = cont.end()
            if section_cite_re.match(masked_text, next_pos):
                pos = next_pos
                continue
            else:
                break

    # 4. Session-law citations (operates on text AFTER masking out chapter cites
    #    AND section-level RCW cites — but section-level matches were not masked,
    #    so we re-mask all cite positions to avoid double-counting).
    section_consumed: list[tuple[int, int]] = []
    for m_kw in re.finditer(r'\bRCW\s+', masked_text):
        pos = m_kw.end()
        while True:
            m_cite = section_cite_re.match(masked_text, pos)
            if not m_cite:
                break
            section_consumed.append((m_cite.start(), m_cite.end()))
            pos = m_cite.end()
            cont = continuation_re.match(masked_text, pos)
            if not cont:
                break
            next_pos = cont.end()
            if section_cite_re.match(masked_text, next_pos):
                pos = next_pos
            else:
                break

    masked2 = list(masked_text)
    for start, end in section_consumed:
        for i in range(start, end):
            masked2[i] = ' '
    masked2_text = ''.join(masked2)

    for m in SESSION_LAW_RE.finditer(masked2_text):
        year = m.group(1)
        sp = m.group(2) or ''
        chap = m.group(3)
        if sp:
            cite = f'{year} {sp} c {chap}'
        else:
            cite = f'{year} c {chap}'
        out.append(AuthorityChainEntry(
            authorizing_rcw=cite,
            authority_text=authority_part,
            is_chapter_level=False,
            is_session_law=True,
            sort_order=sort_order,
        ))
        sort_order += 1

    return out


def _parse_revision_events(events_part: str, action_type_default: str = 'amended') -> list[HistoryEntry]:
    """Parse the revision-events portion of the bracket into HistoryEntry list.

    Format: 'WSR YY-NN-NNN, § T-CC-SSS, filed M/D/YY[, effective M/D/YY]; WSR ...; ...'

    sort_order=1 is the most recent (first event in left-to-right order, per
    WAC convention).
    """
    out: list[HistoryEntry] = []
    sort_order = 1
    for m in REVISION_EVENT_RE.finditer(events_part):
        wsr_raw = m.group(1).strip()
        # Strip Order parenthetical if present, keep just 'WSR YY-NN-NNN'
        wsr_clean_match = re.match(r'(\d{2}-\d+-\d+)', wsr_raw)
        wsr_id = f'WSR {wsr_clean_match.group(1)}' if wsr_clean_match else f'WSR {wsr_raw}'
        action_summary = ''
        order_match = re.search(r'\(([^)]+)\)', wsr_raw)
        if order_match:
            action_summary = order_match.group(1)
        filed_iso = _parse_short_date(m.group(3))
        effective_iso = _parse_short_date(m.group(4)) if m.group(4) else None
        out.append(HistoryEntry(
            wsr_register=wsr_id,
            register_year=_wsr_year(wsr_id),
            filed_date=filed_iso,
            effective_date=effective_iso,
            action_type=action_type_default,
            action_summary=action_summary,
            sort_order=sort_order,
        ))
        sort_order += 1
    return out


def _parse_authority_bracket(bracket_text: str) -> tuple[list[AuthorityChainEntry], list[HistoryEntry]]:
    """Top-level bracket parser. Splits authority part vs revision events.

    bracket_text is the contents BETWEEN [ and ] — outer brackets stripped.
    Format: 'Statutory Authority: <authority-list>. <revision-event-1>; <revision-event-2>; ...'

    Some brackets omit the 'Statutory Authority:' prefix (rare; legacy form).
    Some have multiple periods inside the authority list (e.g. RCW citations
    with subsection notation like 'RCW 49.60.120(3)' — these are NOT period
    separators).

    Strategy: find the FIRST occurrence of 'WSR ' — everything before it is
    the authority list, everything after is revision events.
    """
    text = bracket_text.strip()
    # Strip 'Statutory Authority:' prefix if present
    text = re.sub(r'^\s*Statutory\s+Authority\s*:\s*', '', text, flags=re.IGNORECASE)

    # Find first WSR token; that's where revision events start
    wsr_match = re.search(r'\bWSR\s+\d{2}-\d+-\d+', text)
    if wsr_match:
        authority_part = text[:wsr_match.start()].rstrip(' .')
        events_part = text[wsr_match.start():]
    else:
        # No WSR — bracket is all authority list (rare; possibly malformed)
        authority_part = text
        events_part = ''

    auth_entries = _parse_authority_list(authority_part) if authority_part else []
    history_entries = _parse_revision_events(events_part) if events_part else []
    return auth_entries, history_entries


# ---------------------------------------------------------------------------
# Chapter HTML parsing
# ---------------------------------------------------------------------------

class TitleRedirectError(Exception):
    """Raised when a chapter URL returned a title-level page instead of a chapter.

    Observed pattern: cite=365-06&full=true returns the Title 365 TOC page
    (<h1>Title 365 WAC</h1>) when the chapter slot is referenced from the
    title TOC but is not actually populated — the server falls through to the
    title view. Caller handles by skipping with a warning.
    """


def _extract_chapter_id(doc: _Element) -> tuple[str, str]:
    """Returns (title_number, chapter_number) parsed from <h1> or <title>.

    Primary: <h1>Chapter T-CC WAC</h1>
    Raises TitleRedirectError if the H1 reads 'Title N WAC' (server-side
    fall-through from a non-existent chapter slot).
    Raises ValueError if neither pattern is found.
    """
    for el in doc.xpath("//h1"):
        text = ' '.join(el.text_content().split())
        m = re.search(r'Chapter\s+([0-9]+[A-Za-z]?)-([0-9]+[A-Za-z]?)\s+WAC', text)
        if m:
            return m.group(1), f'{m.group(1)}-{m.group(2)}'
        # Title-page redirect: <h1>Title NNN WAC</h1> means we got the title TOC
        # back, not a chapter. Caller should skip this URL.
        m_title = re.search(r'Title\s+([0-9]+[A-Za-z]?)\s+WAC', text)
        if m_title and 'Chapter' not in text:
            raise TitleRedirectError(
                f"H1 reads 'Title {m_title.group(1)} WAC' — chapter slot returned "
                f"title-level page (server fall-through). Skip this URL."
            )
    # Fallback: <title> element
    for el in doc.xpath("//title"):
        text = ' '.join(el.text_content().split())
        m = re.search(r'Chapter\s+([0-9]+[A-Za-z]?)-([0-9]+[A-Za-z]?)\s+WAC', text)
        if m:
            return m.group(1), f'{m.group(1)}-{m.group(2)}'
    raise ValueError("Could not locate chapter identifier in <h1> or <title>")


def _extract_chapter_heading(doc: _Element) -> str:
    """Returns the chapter heading text from <h2>."""
    for el in doc.xpath("//h2"):
        ancestors = el.xpath("ancestor::*[@id='ContentPlaceHolder1_pnlTitleBlock']")
        if not ancestors:
            continue
        return ' '.join(el.text_content().split())
    # Fallback: any non-nav h2
    for el in doc.xpath("//h2"):
        text = ' '.join(el.text_content().split())
        if text and 'menu' not in text.lower():
            return text
    return ''


def _section_anchor_iterator(doc: _Element):
    """Yields (anchor_element, citation_text) for each WAC section anchor."""
    sec_re = re.compile(r'^[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?$')
    for a in doc.xpath("//a[@name]"):
        name = a.get('name', '')
        if name and sec_re.match(name):
            yield a, name


def _render_table_as_text(table_el: _Element) -> str:
    """Render an HTML <table> as pipe-separated rows, newline-separated."""
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


def _extract_section_body_and_credit(anchor_el: _Element, next_anchor_el: Optional[_Element]):
    """Walk siblings after anchor_el until next_anchor_el; categorize each div.

    Returns dict with:
        'citation_text', 'caption_text', 'body_paragraphs', 'body_html',
        'credit_text', 'credit_links', 'notes_blocks'
    """
    cite_text   = ''
    caption_text = ''
    body_paragraphs: list[str] = []
    body_html   = ''
    credit_text = ''
    credit_links: list[tuple[str, str]] = []
    notes: list[tuple[str, str, list[tuple[str, str]]]] = []

    cur = anchor_el
    in_notes = False
    seen_h3 = 0
    while True:
        cur = cur.getnext()
        if cur is None:
            parent = anchor_el.getparent()
            cur = parent.getnext() if parent is not None else None
            if cur is None:
                break
            anchor_el = parent

        # Stop at next section anchor
        if next_anchor_el is not None:
            anchors_inside = cur.xpath(".//a[@name]") if hasattr(cur, 'xpath') else []
            for a in anchors_inside:
                nm = a.get('name', '')
                if (re.match(r'^[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?$', nm)
                        and a is next_anchor_el):
                    return _build_extract(cite_text, caption_text, body_paragraphs,
                                          body_html, credit_text, credit_links, notes)
            if any(a is next_anchor_el for a in anchors_inside):
                return _build_extract(cite_text, caption_text, body_paragraphs,
                                      body_html, credit_text, credit_links, notes)

        # Also stop if we hit the disposition table heading
        if hasattr(cur, 'xpath'):
            for h3 in cur.xpath(".//h3"):
                heading = ' '.join(h3.text_content().split())
                if 'Disposition of sections' in heading:
                    return _build_extract(cite_text, caption_text, body_paragraphs,
                                          body_html, credit_text, credit_links, notes)

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
                if 'Disposition of sections' in txt:
                    # caller-side guard caught this; bail
                    return _build_extract(cite_text, caption_text, body_paragraphs,
                                          body_html, credit_text, credit_links, notes)
                if seen_h3 == 0:
                    # Citation H3 — text format is "PDFT-CC-SSS" (PDF button + cite)
                    m = re.search(
                        r'([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)',
                        txt,
                    )
                    cite_text = f'WAC {m.group(1)}' if m else txt
                    seen_h3 = 1
                elif seen_h3 == 1:
                    caption_text = txt
                    seen_h3 = 2
            if seen_h3 >= 2 and not in_notes:
                continue

        style = (cur.get('style') or '').lower()
        inner_html = LH.tostring(cur, encoding='unicode')

        # Statutory authority bracket: contains '[Statutory Authority:' AND
        # in a margin-top:15pt div (canonical) — but be permissive on style
        if not credit_text and 'Statutory Authority' in inner_html and '[' in inner_html:
            full_text_inner = cur.text_content().strip()
            # Find the bracket pattern in the div text
            bracket_match = re.search(r'\[([^\[\]]*?)\]', full_text_inner)
            if bracket_match:
                credit_text = bracket_match.group(1).strip()
                for a in cur.xpath('.//a'):
                    href = a.get('href', '')
                    txt  = ' '.join(a.text_content().split())
                    credit_links.append((txt, href))
                continue

        # Notes header block
        if 'margin-top:0.25in' in style and cur.xpath(".//h3"):
            for h3 in cur.xpath(".//h3"):
                if 'NOTES' in h3.text_content().upper():
                    in_notes = True
            continue

        # Notes body
        if in_notes and 'margin-bottom:0.2in' in style:
            inner_div = cur.xpath(".//div[contains(@style,'text-indent:0.75in')]")
            if inner_div:
                d = inner_div[0]
                heading_span = d.xpath(".//span[contains(@style,'font-weight:bold')]")
                heading_text = ' '.join(heading_span[0].text_content().split()).rstrip(':') if heading_span else ''
                body_text = d.text_content()
                if heading_text:
                    body_text = body_text.replace(heading_text + ':', '', 1).strip()
                else:
                    body_text = body_text.strip()
                links: list[tuple[str, str]] = []
                for a in d.xpath('.//a'):
                    href = a.get('href', '')
                    txt  = ' '.join(a.text_content().split())
                    links.append((txt, href))
                notes.append((heading_text, body_text, links))
            continue

        # Body text
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
                for tbl in tables:
                    rendered = _render_table_as_text(tbl)
                    if rendered:
                        body_paragraphs.append(rendered)
                continue
            elif tables:
                body_html = inner_html
                for tbl in tables:
                    rendered = _render_table_as_text(tbl)
                    if rendered:
                        body_paragraphs.append(rendered)
                if not body_paragraphs:
                    txt = cur.text_content()
                    txt = re.sub(r'\s+', ' ', txt).strip()
                    if txt:
                        body_paragraphs.append(txt)
                continue

    return _build_extract(cite_text, caption_text, body_paragraphs,
                          body_html, credit_text, credit_links, notes)


def _build_extract(cite, caption, paras, body_html, credit, links, notes):
    return {
        'citation_text': cite,
        'caption_text': caption,
        'body_paragraphs': paras,
        'body_html': body_html,
        'credit_text': credit,
        'credit_links': links,
        'notes_blocks': notes,
    }


def _parse_notes(notes_blocks) -> list[NoteEntry]:
    """Convert raw note blocks into NoteEntry records."""
    out: list[NoteEntry] = []
    for i, (heading, body, _links) in enumerate(notes_blocks):
        parts = re.split(r'\s*[—–\-]\s*', heading, maxsplit=1)
        if len(parts) == 2:
            note_type = parts[0].strip()
            note_topic = parts[1].strip()
        else:
            note_type = heading.strip()
            note_topic = ''
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
    """Detect cross-citations within body text. NOTE: does NOT include
    citations within the [Statutory Authority: ...] bracket — those are
    routed to authority_chain instead by the caller's scope discriminator.
    """
    refs: list[CrossRef] = []
    # Internal WAC sections
    for m in WAC_SECTION_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'WAC {m.group(1)}',
            target_jurisdiction='state-wa-wac',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    for m in WAC_CHAPTER_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'chapter {m.group(1)} WAC',
            target_jurisdiction='state-wa-wac',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    # State RCW
    for m in RCW_SECTION_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'RCW {m.group(1)}',
            target_jurisdiction='state-wa-rcw',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
    for m in RCW_CHAPTER_TEXT_RE.finditer(text):
        refs.append(CrossRef(
            target_citation=f'chapter {m.group(1)} RCW',
            target_jurisdiction='state-wa-rcw',
            ref_type='reference',
            context_snippet=text[max(0, m.start()-40):m.end()+40].strip(),
        ))
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
    # CFR
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


# ---------------------------------------------------------------------------
# Disposition table parser
# ---------------------------------------------------------------------------

def _parse_disposition_table(doc: _Element, chapter_number: str) -> list[DispositionEntry]:
    """Parse the inline disposition table at the end of the chapter HTML.

    Source structure:
      <h3>Disposition of sections formerly codified in this title</h3>
      <table>
        <tr><td/><td/></tr>  <!-- spacer header row -->
        <tr>
          <td>SECTION_NUMBER</td>
          <td>caption [original-filing brackets] DISPOSITION-ACTION</td>
        </tr>
        ... more rows ...
      </table>
    """
    entries: list[DispositionEntry] = []

    # Locate the disposition heading
    h3_disp = None
    for h3 in doc.xpath("//h3"):
        if 'Disposition of sections' in h3.text_content():
            h3_disp = h3
            break
    if h3_disp is None:
        return entries

    # Find next <table> sibling/descendant in document order
    # The table follows the h3 in DOM order
    next_table = None
    for sibling in h3_disp.itersiblings():
        if sibling.tag == 'table':
            next_table = sibling
            break
    if next_table is None:
        # Try next-sibling-of-parent fallback
        parent = h3_disp.getparent()
        if parent is not None:
            for el in parent.xpath('.//table'):
                if el.sourceline and h3_disp.sourceline and el.sourceline > h3_disp.sourceline:
                    next_table = el
                    break

    if next_table is None:
        return entries

    for tr in next_table.xpath('.//tr'):
        tds = tr.xpath('./td')
        if len(tds) < 2:
            continue
        secnum_text = (tds[0].text_content() or '').strip()
        # Skip header/spacer rows (no section number).
        # Disposition rows show the full T-CC-SSS citation in the first cell
        # (e.g. '162-16-010'). Match either:
        #   - full citation T-CC-SSS (preferred form)
        #   - bare section number (rare alt form)
        full_cite_match = re.match(rf'^{re.escape(chapter_number)}-(\d+[A-Za-z]?)$', secnum_text)
        bare_match = re.match(r'^(\d+[A-Za-z]?)$', secnum_text)
        if full_cite_match:
            secnum = full_cite_match.group(1)
        elif bare_match:
            secnum = bare_match.group(1)
        else:
            continue  # skip header/spacer rows and any other non-section row

        dispo_html = LH.tostring(tds[1], encoding='unicode')
        dispo_text = ' '.join(tds[1].text_content().split())

        # Caption is the text up to the first '[' (original-filing bracket)
        caption_match = re.match(r'^([^\[]+?)\.\s*\[', dispo_text)
        caption = caption_match.group(1).strip() if caption_match else ''

        canonical = f'WAC {chapter_number}-{secnum}'
        e = DispositionEntry(
            section_number=secnum,
            section_caption=caption,
            canonical_citation=canonical,
            raw_disposition_text=dispo_text,
        )

        lower = dispo_text.lower()

        # Classify disposition:
        if 'recodified as wac' in lower:
            e.is_recodified = True
            m = re.search(r'Recodified as WAC\s+([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)', dispo_text)
            if m:
                e.recodified_to = f'WAC {m.group(1)}'
        elif 'decodified' in lower:
            e.is_decodified = True
        elif 'reserved' in lower and 'reserved.' in lower:
            e.is_reserved = True
        elif 'expired' in lower:
            m = re.search(r'Expired\s+(\d{1,2}/\d{1,2}/\d{2,4})', dispo_text)
            if m:
                e.is_expired = True
                e.effective_date = _parse_short_date(m.group(1))
            else:
                e.is_expired = True
        elif 'repealed' in lower:
            e.is_repealed = True
            # Capture repealing WSR if present
            m = re.search(r'Repealed by WSR\s+(\d{2}-\d+-\d+)', dispo_text)
            if m:
                e.repealing_wsr = f'WSR {m.group(1)}'
            # Effective date if explicit
            m_eff = re.search(r'effective\s+(\d{1,2}/\d{1,2}/\d{2,4})', dispo_text)
            if m_eff:
                e.effective_date = _parse_short_date(m_eff.group(1))
        else:
            # Unknown disposition class — record as repealed (most common default)
            # but log via raw_disposition_text for audit
            e.is_repealed = True

        # Parse original filing history from any bracket(s) in the disposition text.
        # Format inside brackets: 'Order N, § T-CC-SSS, filed M/D/YY; Order M, ...'
        # OR: 'Statutory Authority: ... WSR YY-NN-NNN, § T-CC-SSS, filed ..., effective ...'
        for bm in re.finditer(r'\[([^\[\]]+)\]', dispo_text):
            bracket = bm.group(1)
            # Treat as authority bracket if 'Statutory Authority' present
            if re.search(r'Statutory Authority', bracket, re.IGNORECASE):
                _auth, hist = _parse_authority_bracket(bracket)
                # Mark these history events as 'amended' (filing under prior authority)
                e.history.extend(hist)
            else:
                # Plain original-filing bracket: 'Order N, § ..., filed ...'
                # Approximate to HistoryEntry (no WSR — Order-based legacy)
                for fm in re.finditer(
                    r'Order\s+(\S+)\s*,\s*§\s*[\d\-A-Za-z]+\s*,\s*filed\s+(\d{1,2}/\d{1,2}/\d{2,4})',
                    bracket,
                ):
                    e.history.append(HistoryEntry(
                        wsr_register=f'Order {fm.group(1)}',
                        register_year=None,
                        filed_date=_parse_short_date(fm.group(2)),
                        effective_date=None,
                        action_type='adopted',
                        action_summary='legacy Order pre-WSR system',
                        sort_order=len(e.history) + 1,
                    ))

        entries.append(e)

    return entries


# ---------------------------------------------------------------------------
# Top-level chapter parser
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Empty-body classification helpers — Phase 4 corroboration #11
# ---------------------------------------------------------------------------
# Classification of sections with empty full_text:
#   A:        image/diagram body (operative content is visual)
#   RESERVED: caption matches \bReserved\b in structural form
#   B1:       part-heading shell (Part divider before citation H3)
#   D:        substantive caption + empty HTML body (Code Reviser HTML
#             rendering issue; operative text is in PDF). Phase 4 fetches
#             PDF text separately. Parser leaves these for the ingest driver
#             to fill via PDF fetch + pdftotext.

_RESERVED_STRUCTURAL_RE = re.compile(r'\bReserved\b', re.IGNORECASE)


def _is_structural_reserved_caption(cap: str) -> bool:
    """Return True if caption is structurally a Reserved marker.

    Patterns matched:
      - 'Reserved' / 'Reserved.'
      - '(Reserved)' / '(reserved.)' / '[Reserved]' parenthetical
      - 'Section <X>—Reserved', 'Appendix <X>—Reserved' suffix forms
      - Any caption ending with '—Reserved' or '- Reserved'

    Counter-examples that should return False (have substantive content
    despite containing word 'Reserved'):
      - 'Course titles reserved for prescribed curriculum courses' (308-124H-810)
      - 'Reserved rights' (332-12-290)
    Detection logic also requires body empty (caller-side gate); this
    function alone determines whether the caption shape is Reserved-like.
    """
    if not cap:
        return False
    c = cap.strip().rstrip('.').strip()
    if c.lower() == 'reserved':
        return True
    if re.search(r'\(\s*reserved\s*\.?\s*\)', cap, re.IGNORECASE):
        return True
    if re.search(r'\[\s*reserved\s*\.?\s*\]', cap, re.IGNORECASE):
        return True
    if re.search(r'(?:Section|Appendix|Chapter|Part)\s+\S+\s*[—\-]\s*Reserved\.?$',
                 cap, re.IGNORECASE):
        return True
    if re.search(r'[—\-]\s*Reserved\.?$', cap, re.IGNORECASE):
        return True
    return False


def _has_part_divider_before_h3(section_html: str) -> bool:
    """True if a <div text-align:center> appears before the first <h3> tag.

    Indicates the section is a part-heading shell (e.g., 'Part D Ladders').
    """
    h3_match = re.search(r'<h3[\s>]', section_html)
    if not h3_match:
        return False
    pre_h3 = section_html[:h3_match.start()]
    return bool(re.search(r"<div[^>]*text-align:center[^>]*>", pre_h3))


def _has_image_in_body(section_html: str) -> bool:
    return '<img' in section_html or '<picture' in section_html


def _build_placeholder_class_a(canonical: str, caption: str, leg_url: str,
                                authority_text: str) -> str:
    """Class A: image-only section."""
    return (
        f"[Section content is a regulatory diagram or flowchart. "
        f"The operative visual content is published at: {leg_url}\n\n"
        f"Statutory authority and source-credit history preserved below.\n\n"
        f"Cited as: {canonical}\n\n"
        f"{authority_text}"
    )


def _build_placeholder_class_b1(canonical: str, caption: str, chapter: str,
                                 leg_url: str, authority_text: str) -> str:
    """Class B1: part-heading shell (parent of subordinate subsections)."""
    return (
        f"[Section header: {caption}. This section organizes requirements "
        f"in WAC {chapter} and serves as the heading for subordinate "
        f"subsections. See subsections below for operative content.\n\n"
        f"Statutory authority and source-credit preserved below.\n\n"
        f"Cited as: {canonical}\n\n"
        f"{authority_text}"
    )


_CFR_CITE_RE = re.compile(
    r'\b(\d+)\s*C\.?\s*F\.?\s*R\.?\s*(?:§\s*|Part\s+|Subpart\s+)?(\d+(?:\.\d+)?[A-Za-z]*)?(?:\s*,\s*Subpart\s+[A-Z]+)?',
    re.IGNORECASE,
)


def _classify_class_d(authority_bracket_text: str) -> str:
    """Return 'class_d_cfr' if the authority bracket cites federal CFR,
    else 'class_d_external'. Per operator discrimination logic."""
    if re.search(r'\b\d+\s*C\.?\s*F\.?\s*R\.?', authority_bracket_text or ''):
        return 'class_d_cfr'
    return 'class_d_external'


def _extract_cfr_cites(authority_bracket_text: str) -> list[str]:
    """Extract human-readable CFR cite strings from authority text.

    Returns a list like ['29 C.F.R. 1926, Subpart CC', '29 C.F.R. 1910.1050'].
    Used in the Class D-CFR placeholder body.
    """
    out: list[str] = []
    if not authority_bracket_text:
        return out
    # Match common forms: '29 C.F.R. 1926, Subpart CC', '29 CFR 1910.1050',
    # '45 C.F.R. § 84.4', '29 C.F.R. Part 1926'
    for m in re.finditer(
        r'\b(\d+)\s*(C\.?F\.?R\.?)'
        r'(?:\s*(?:§|Part|Subpart)\s*)?'
        r'(?:\s*(\d+(?:\.\d+)?[A-Za-z]*))?'
        r'(?:\s*,\s*Subpart\s+([A-Z]+))?',
        authority_bracket_text, re.IGNORECASE,
    ):
        title_num = m.group(1)
        section = m.group(3) or ''
        subpart = m.group(4) or ''
        cite = f"{title_num} C.F.R."
        if section:
            cite += f" {section}"
        if subpart:
            cite += f", Subpart {subpart}"
        if cite not in out:
            out.append(cite)
    return out


def _build_placeholder_class_d_cfr(canonical: str, caption: str,
                                     authority_text: str) -> str:
    """Class D with CFR cite — federal-incorporation-by-reference."""
    cfr_cites = _extract_cfr_cites(authority_text)
    cfr_list = ', '.join(cfr_cites) if cfr_cites else '(see authority bracket)'
    return (
        f"[Section {canonical} \"{caption}\" adopts federal regulatory "
        f"standards by reference. Operative content is published in the "
        f"federal Code of Federal Regulations at the citation(s) listed in "
        f"the statutory authority bracket below: {cfr_list}\n\n"
        f"Washington adopts these federal standards under the RCW authority "
        f"shown. Any Washington-specific deviations or additions appear in "
        f"the surrounding sections of this WAC chapter.\n\n"
        f"Cited as: {canonical}\n\n"
        f"{authority_text}"
    )


def _build_placeholder_class_d_external(canonical: str, caption: str,
                                          authority_text: str) -> str:
    """Class D without CFR cite — external standard incorporation
    (ANSI/ASME/ICC) or empty citation slot."""
    return (
        f"[Section {canonical} \"{caption}\" exists as a citation slot in "
        f"the certified WAC publication. Operative content (where present) "
        f"is incorporated by reference from external industry or model-code "
        f"standards (commonly ANSI/ASME, ICC building codes, or similar). "
        f"Refer to the surrounding chapter and statutory authority for "
        f"context.\n\n"
        f"Cited as: {canonical}\n\n"
        f"{authority_text}"
    )


def parse_chapter_html(filepath: str | Path, publication_date: str = '2026-04-01') -> ChapterRecord:
    """Parse a single chapter HTML file into a ChapterRecord."""
    filepath = Path(filepath)
    raw = filepath.read_bytes()
    text = raw.decode('utf-8', errors='replace')
    doc = LH.fromstring(text)

    title_number, chapter_number = _extract_chapter_id(doc)
    chapter_heading = _extract_chapter_heading(doc)

    rec = ChapterRecord(
        title_number=title_number,
        chapter_number=chapter_number,
        chapter_heading=chapter_heading,
        publication_date=publication_date,
    )

    # Parse active sections
    anchors = list(_section_anchor_iterator(doc))
    for i, (a, name) in enumerate(anchors):
        next_a = anchors[i + 1][0] if i + 1 < len(anchors) else None
        info = _extract_section_body_and_credit(a, next_a)

        # Skip empty extractions (no citation H3, no caption, no body, no credit, no notes)
        if (not info['citation_text']
                and not info['caption_text']
                and not info['body_paragraphs']
                and not info['credit_text']
                and not info['notes_blocks']):
            continue

        # Section number = third dash-component of the anchor name
        parts = name.split('-')
        section_number = parts[-1] if parts else name

        # Parse statutory authority bracket → authority_chain + history
        auth_entries: list[AuthorityChainEntry] = []
        history: list[HistoryEntry] = []
        if info['credit_text']:
            auth_entries, history = _parse_authority_bracket(info['credit_text'])

        notes_records = _parse_notes(info['notes_blocks'])

        full_text = '\n\n'.join(info['body_paragraphs'])
        cross_refs = _detect_cross_refs(full_text, info['body_html'])

        # Most-recent WSR + effective_date
        most_recent_wsr = history[0].wsr_register if history else None
        most_recent_effective = (
            next((h.effective_date for h in history if h.effective_date), None)
        )

        sec = SectionRecord(
            section_number=section_number,
            canonical_citation=info['citation_text'] or f'WAC {name}',
            section_heading=info['caption_text'].rstrip('.'),
            full_text=full_text,
            source_html=info['body_html'],
            wsr_register_number=most_recent_wsr,
            effective_date=most_recent_effective,
            history=history,
            authority_chain=auth_entries,
            notes=notes_records,
            cross_refs=cross_refs,
        )

        # ─────────────────────────────────────────────────────────────────
        # Empty-body classification (Phase 4 corroboration #11)
        # When full_text is empty and no status flag is set, classify by
        # structural signature and apply the appropriate fix:
        #   - Reserved (incl. parenthetical) → set is_reserved=TRUE (ghost)
        #   - Class A (image-only) → placeholder + URL
        #   - Class B1 (part-heading shell) → placeholder + heading
        #   - Class D (substantive, empty HTML) → leave for PDF-fetch driver;
        #     parser sets sec.full_text='' AND sec.unit_type='pdf_pending' to
        #     signal Phase 4 to fetch PDF text via pdftotext.
        # ─────────────────────────────────────────────────────────────────
        if (not (sec.is_repealed or sec.is_recodified or sec.is_decodified
                 or sec.is_expired or sec.is_reserved)
                and not (sec.full_text or '').strip()):
            # Re-extract section span for signature checks
            anchor_marker_with_space = f"<a name='{name}' ></a>"
            anchor_marker_no_space   = f"<a name='{name}'></a>"
            anchor_start = text.find(anchor_marker_with_space)
            if anchor_start < 0:
                anchor_start = text.find(anchor_marker_no_space)
            section_span = ''
            if anchor_start >= 0:
                next_anchor = re.search(
                    r"<a name='[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?'",
                    text[anchor_start + 50:]
                )
                end = (anchor_start + 50 + next_anchor.start()
                       if next_anchor else anchor_start + 5000)
                section_span = text[anchor_start:end]

            cap_for_check = sec.section_heading or info['caption_text'] or ''
            leg_url = (
                f"https://app.leg.wa.gov/WAC/default.aspx?cite="
                f"{rec.chapter_number}-{section_number}"
            )
            pdf_url = (
                f"https://app.leg.wa.gov/WAC/default.aspx?cite="
                f"{rec.chapter_number}-{section_number}&pdf=true"
            )
            authority_text = (
                f"[{info['credit_text']}]" if info['credit_text'] else ""
            )

            if _is_structural_reserved_caption(cap_for_check):
                # RESERVED (includes parenthetical variants)
                sec.is_reserved = True
                # Ghost row: full_text stays empty
            elif _has_image_in_body(section_span):
                # Class A — image-only section
                sec.full_text = _build_placeholder_class_a(
                    canonical=sec.canonical_citation,
                    caption=cap_for_check,
                    leg_url=leg_url,
                    authority_text=authority_text,
                )
            elif _has_part_divider_before_h3(section_span):
                # Class B1 — part-heading shell
                sec.full_text = _build_placeholder_class_b1(
                    canonical=sec.canonical_citation,
                    caption=cap_for_check,
                    chapter=rec.chapter_number,
                    leg_url=leg_url,
                    authority_text=authority_text,
                )
            else:
                # Class D — substantive caption + empty body. Per operator
                # 2026-04-30 finding: this is incorporate-by-reference,
                # NOT a Code Reviser HTML rendering bug. Both HTML and PDF
                # have empty body for these sections — operative content
                # lives in external standards (federal CFR, ANSI/ASME/ICC).
                # Discriminate CFR-cited from non-CFR for downstream
                # routing.
                d_class = _classify_class_d(authority_text)
                if d_class == 'class_d_cfr':
                    sec.full_text = _build_placeholder_class_d_cfr(
                        canonical=sec.canonical_citation,
                        caption=cap_for_check,
                        authority_text=authority_text,
                    )
                else:
                    sec.full_text = _build_placeholder_class_d_external(
                        canonical=sec.canonical_citation,
                        caption=cap_for_check,
                        authority_text=authority_text,
                    )

        # Pending recodification detection (rare; mirror RCW pattern):
        # Caption text like "(Effective until M/D/YY. Recodified as WAC T-CC-SSS.)"
        recod_match = re.search(
            r'Recodified as WAC\s+([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)',
            info['caption_text'] or '',
        )
        if recod_match:
            sec.pending_recodification_to = f'WAC {recod_match.group(1)}'
            eff_match = re.search(
                r'Effective until ([A-Z][a-z]+\s+\d{1,2},\s+\d{4})',
                info['caption_text'] or '',
            )
            if eff_match:
                try:
                    d = datetime.strptime(eff_match.group(1), '%B %d, %Y')
                    sec.pending_recodification_effective = d.strftime('%Y-%m-%d')
                except ValueError:
                    pass
            if sec.pending_recodification_effective is None:
                sec.pending_recodification_effective = '2099-01-01'
            sec.cross_refs.append(CrossRef(
                target_citation=sec.pending_recodification_to,
                target_jurisdiction='state-wa-wac',
                ref_type='recodification',
                context_snippet=(
                    f'Pending recodification effective {sec.pending_recodification_effective}: '
                    f'this section becomes ghost on that date and the live citation moves to '
                    f'{sec.pending_recodification_to}'
                ),
            ))

        rec.sections.append(sec)

    # Parse inline disposition table → DispositionEntry list (ghost rows)
    rec.dispositions = _parse_disposition_table(doc, chapter_number)

    # Disposition-only chapter detection: zero active sections + non-empty dispositions
    if not rec.sections and rec.dispositions:
        rec.is_disposition_only_chapter = True

    return rec


# ---------------------------------------------------------------------------
# Convenience / verification helpers
# ---------------------------------------------------------------------------

def count_section_anchors(filepath: str | Path) -> int:
    """Count <a name='T-CC-SSS'></a> anchors in a chapter file."""
    filepath = Path(filepath)
    html = filepath.read_bytes()
    return len([m for m in ANCHOR_RE.finditer(html.decode('utf-8', 'replace'))])


if __name__ == '__main__':
    # Smoke test against priority sample chapters
    import sys
    samples = [
        ('/opt/wdws/data/wac/html/chapters/162/16.html', 'WAC 162-16 (WLAD employment)'),
        ('/opt/wdws/data/wac/html/chapters/296/14A.html', 'WAC 296-14A (structured settlements)'),
        ('/opt/wdws/data/wac/html/chapters/296/15.html', 'WAC 296-15 (self-insurer claims)'),
    ]
    for path, label in samples:
        if not Path(path).exists():
            print(f'SKIP: {label} (not cached)')
            continue
        print(f'\n=== {label} ===')
        try:
            rec = parse_chapter_html(path)
        except Exception as e:
            print(f'  ERROR: {e}')
            continue
        print(f'  title={rec.title_number} chapter={rec.chapter_number}')
        print(f'  heading: {rec.chapter_heading}')
        print(f'  active sections: {len(rec.sections)}')
        print(f'  dispositions:    {len(rec.dispositions)}')
        print(f'  disposition_only_chapter: {rec.is_disposition_only_chapter}')
        if rec.sections:
            s = rec.sections[0]
            print(f'  first section: {s.canonical_citation} — {s.section_heading}')
            print(f'    body chars:    {len(s.full_text)}')
            print(f'    history:       {len(s.history)} ({"/".join(h.wsr_register for h in s.history[:3])}{"..." if len(s.history) > 3 else ""})')
            print(f'    auth chain:    {len(s.authority_chain)} ({"/".join(a.authorizing_rcw for a in s.authority_chain[:3])}{"..." if len(s.authority_chain) > 3 else ""})')
            print(f'    cross refs:    {len(s.cross_refs)}')
            print(f'    notes:         {len(s.notes)}')
        if rec.dispositions:
            d = rec.dispositions[0]
            print(f'  first disposition: {d.canonical_citation} ({d.is_repealed=}, {d.is_recodified=}, {d.recodified_to=})')
