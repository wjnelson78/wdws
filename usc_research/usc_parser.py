"""
USC Parser — Phase 3.

Streams a USLM XML title file with lxml.iterparse and emits ParsedUnit
records (one per <section> | <courtRule> | <reorganizationPlan>) ready for
Phase 4 ingestion.

Design constraints:
  * Memory: Title 42 is ~108 MB and Title 26 ~54 MB. Tree-load OOMs the
    Proxmox host. We iterparse end-events and clear processed elements,
    keeping working set bounded regardless of title size.
  * Faithfulness: every <section>'s raw XML is preserved in
    ParsedUnit.source_xml so Phase 7 differential updates can re-parse.
  * Three unit types discriminated per uslm_findings.md §9:
      section / court_rule / reorganization_plan
  * Citation format catalog matches §9.5 exactly.

This module is pure parsing — no DB writes. The Phase 4 ingestion driver
consumes ParsedUnit records.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional

from lxml import etree

USLM_NS = "http://xml.house.gov/schemas/uslm/1.0"
NS = {"u": USLM_NS}


# ──────────────────────────────────────────────────────────────────────────
# Data classes
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class HistoryEntry:
    pub_law: str            # "Pub. L. 101-336"
    pub_law_date: Optional[str] = None     # "1990-07-26" or None
    statutes_ref: Optional[str] = None     # "104 Stat. 327"
    action_type: Optional[str] = None      # "enacted","amended","renumbered","repealed",...
    action_summary: Optional[str] = None
    sort_order: int = 0


@dataclass
class NoteEntry:
    note_type: str          # "statutory","editorial","change","source_credit","generic"
    note_topic: Optional[str] = None       # USLM @topic value
    note_heading: Optional[str] = None
    note_text: str = ""
    sort_order: int = 0


@dataclass
class CrossRef:
    target_citation: str    # "42 U.S.C. § 12102"
    context_snippet: Optional[str] = None
    ref_type: str = "internal"            # "internal","cfr","public-law","statutes-at-large","external"
    uslm_href: Optional[str] = None


@dataclass
class ParsedUnit:
    """One emitted record per ingestible USC element."""
    unit_type: str                         # "section" | "court_rule" | "reorganization_plan"
    title_number: int
    section_number: str                    # "12101", "12101a", "26" (for Rule 26), "I" (for Plan I)
    section_heading: str
    canonical_citation: str
    full_text: str                         # normalized markdown
    source_xml: str                        # raw <section> / <courtRule> / <reorganizationPlan> XML
    release_point: str

    # optional / discriminator
    rule_set: Optional[str] = None         # FRCP/FRAP/FRE/FRCrP/FRBP, NULL for non-rules
    chapter_number: Optional[str] = None   # "126" — only for unit_type='section'
    chapter_heading: Optional[str] = None
    subchapter_id_text: Optional[str] = None
    subchapter_heading: Optional[str] = None
    is_repealed: bool = False
    is_transferred: bool = False
    is_omitted: bool = False
    transferred_to: Optional[str] = None
    effective_date: Optional[str] = None
    enacted_by_pl: Optional[str] = None
    statutes_at_large_ref: Optional[str] = None

    history: list[HistoryEntry] = field(default_factory=list)
    notes: list[NoteEntry] = field(default_factory=list)
    cross_refs: list[CrossRef] = field(default_factory=list)
    uslm_identifier: Optional[str] = None  # raw @identifier of the unit element


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _qn(tag: str) -> str:
    return f"{{{USLM_NS}}}{tag}"


def _text(elem) -> str:
    """All inline text, with a space between siblings."""
    if elem is None:
        return ""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _local(elem) -> str:
    return etree.QName(elem).localname


def _to_xml_str(elem) -> str:
    return etree.tostring(elem, encoding="unicode", pretty_print=False)


# ──────────────────────────────────────────────────────────────────────────
# Rule set determination (uslm_findings.md §9.3)
# ──────────────────────────────────────────────────────────────────────────

_RULE_IDENT_TAIL = {
    "App":   "FRAP",
    "Civil": "FRCP",
    "Evid":  "FRE",
    "Crim":  "FRCrP",
}
_RULE_FILE_FALLBACK = {
    "usc11a.xml": "FRBP",
    "usc18a.xml": "FRCrP",
}
_RULE_HEADING_KEYWORDS = [
    ("CIVIL",     "FRCP"),
    ("APPELLATE", "FRAP"),
    ("EVIDENCE",  "FRE"),
    ("CRIMINAL",  "FRCrP"),
    ("BANKRUPTCY","FRBP"),
]


def determine_rule_set(courtRules_elem, source_filename: str) -> str:
    """Determine which rule set a <courtRules> wrapper holds. Raises on miss."""
    ident = courtRules_elem.get("identifier", "") or ""
    tail = ident.rsplit("/", 1)[-1] if "/" in ident else ""
    if tail in _RULE_IDENT_TAIL:
        return _RULE_IDENT_TAIL[tail]
    if source_filename in _RULE_FILE_FALLBACK:
        return _RULE_FILE_FALLBACK[source_filename]
    heading = courtRules_elem.find(f"{_qn('heading')}")
    if heading is not None:
        text = (_text(heading) or "").upper()
        for keyword, rule in _RULE_HEADING_KEYWORDS:
            if keyword in text:
                return rule
    raise ValueError(f"unmapped <courtRules> wrapper in {source_filename}: {ident!r}")


# ──────────────────────────────────────────────────────────────────────────
# Citation generation
# ──────────────────────────────────────────────────────────────────────────

_RULE_CITE_FORMAT = {
    "FRCP":  "Fed. R. Civ. P.",
    "FRAP":  "Fed. R. App. P.",
    "FRE":   "Fed. R. Evid.",
    "FRCrP": "Fed. R. Crim. P.",
    "FRBP":  "Fed. R. Bankr. P.",
}


def make_canonical_citation(*, unit_type: str, title_number: int,
                            section_number: str, rule_set: Optional[str],
                            is_appendix_section: bool = False) -> str:
    if unit_type == "section":
        if is_appendix_section:
            return f"{title_number} U.S.C. App. § {section_number}"
        return f"{title_number} U.S.C. § {section_number}"
    if unit_type == "court_rule":
        if not rule_set:
            raise ValueError("court_rule requires rule_set")
        return f"{_RULE_CITE_FORMAT[rule_set]} {section_number}"
    if unit_type == "reorganization_plan":
        # section_number for a plan is e.g. "I of 1939" or "III of 1970" — preserve as-is
        return f"Reorg. Plan No. {section_number}"
    raise ValueError(f"unknown unit_type {unit_type!r}")


# ──────────────────────────────────────────────────────────────────────────
# Source-credit parsing  →  HistoryEntry rows
# ──────────────────────────────────────────────────────────────────────────

# Match e.g. "Pub. L. 101–336", with both ASCII hyphen and en-dash
_RE_PL = re.compile(r"Pub\.\s*L\.\s*(\d+[–-]\d+)")
_RE_STAT = re.compile(r"(\d+)\s*Stat\.\s*(\d+)")
_RE_DATE = re.compile(
    r"(?:Jan|Feb|Mar|Apr|May|June?|July?|Aug|Sept?|Oct|Nov|Dec)\.?\s+\d{1,2},\s*\d{4}",
    re.IGNORECASE,
)
_MONTH_TO_NUM = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

def _parse_date_to_iso(s: str) -> Optional[str]:
    """'July 26, 1990' → '1990-07-26'.  Best-effort; returns None on miss."""
    m = re.match(r"([A-Za-z]+)\.?\s+(\d{1,2}),\s*(\d{4})", s.strip())
    if not m:
        return None
    mon = m.group(1)[:3].lower()
    if mon not in _MONTH_TO_NUM:
        return None
    return f"{m.group(3)}-{_MONTH_TO_NUM[mon]}-{int(m.group(2)):02d}"


def parse_source_credit(text: str) -> list[HistoryEntry]:
    """Parse a sourceCredit/text into HistoryEntry records.
    The parenthetical typically reads:
        (Pub. L. 101-336, title I, § 101, July 26, 1990, 104 Stat. 330;
         Pub. L. 110-325, § 4(a), Sept. 25, 2008, 122 Stat. 3555.)
    We split on ; and emit one HistoryEntry per Pub.L. segment.
    """
    text = text.strip().strip("()").strip()
    out: list[HistoryEntry] = []
    for sort_order, seg in enumerate(text.split(";")):
        seg = seg.strip().rstrip(".")
        m_pl = _RE_PL.search(seg)
        if not m_pl:
            continue
        pub_law = "Pub. L. " + m_pl.group(1).replace("–", "-")
        m_stat = _RE_STAT.search(seg)
        statutes_ref = f"{m_stat.group(1)} Stat. {m_stat.group(2)}" if m_stat else None
        m_date = _RE_DATE.search(seg)
        pub_law_date = _parse_date_to_iso(m_date.group(0)) if m_date else None
        out.append(HistoryEntry(
            pub_law=pub_law,
            pub_law_date=pub_law_date,
            statutes_ref=statutes_ref,
            action_type="enacted" if sort_order == 0 else "amended",
            action_summary=seg,
            sort_order=sort_order,
        ))
    return out


# ──────────────────────────────────────────────────────────────────────────
# Cross-reference walker  →  CrossRef rows
# ──────────────────────────────────────────────────────────────────────────

def classify_ref_href(href: str) -> str:
    if href.startswith("/us/usc/"):
        return "internal"
    if href.startswith("/us/cfr/"):
        return "cfr"
    if href.startswith("/us/pl/"):
        return "public-law"
    if href.startswith("/us/stat/"):
        return "statutes-at-large"
    return "external"


def href_to_canonical_citation(href: str) -> Optional[str]:
    """Convert /us/usc/t42/s12101 → '42 U.S.C. § 12101'.
    Returns None for non-USC refs.
    """
    m = re.match(r"^/us/usc/t(\d+[a-zA-Z]?)/s([\w\-./]+)", href)
    if not m:
        return None
    title = m.group(1)
    section = m.group(2).split("/")[0]   # drop subsection portion
    title_clean = re.sub(r"[a-zA-Z]+$", "", title)  # strip 'a' from t28a etc.
    return f"{title_clean} U.S.C. § {section}"


def collect_cross_refs(unit_elem) -> list[CrossRef]:
    """Walk every <ref> descendant of the unit; emit one CrossRef per."""
    out: list[CrossRef] = []
    for ref in unit_elem.iter(_qn("ref")):
        href = ref.get("href", "")
        if not href:
            continue
        ref_type = classify_ref_href(href)
        target = href_to_canonical_citation(href) if ref_type == "internal" else None
        if ref_type != "internal":
            target = href   # external refs use the raw href as target
        snippet = (_text(ref) or "").strip()[:200]
        out.append(CrossRef(
            target_citation=target or href,
            context_snippet=snippet,
            ref_type=ref_type,
            uslm_href=href,
        ))
    return out


# ──────────────────────────────────────────────────────────────────────────
# Markdown rendering
# ──────────────────────────────────────────────────────────────────────────

# Levels below section, in order of expected depth
_SUBLEVEL_ORDER = [
    "subsection", "paragraph", "subparagraph", "clause", "subclause",
    "item", "subitem", "subsubitem",
]
_SUBLEVEL_MD_HEADER = {
    "subsection":   "##",
    "paragraph":    "###",
    "subparagraph": "####",
    "clause":       "#####",
    "subclause":    "######",
    "item":         "######",
    "subitem":      "######",
    "subsubitem":   "######",
}


def render_markdown(unit_elem, *, citation_prefix: str, heading: str) -> str:
    """Render the unit body as nested markdown.
    Always begins with the breadcrumb + heading line so each chunk carries
    citation context per the Phase 1 chunking rule.
    """
    lines: list[str] = []
    lines.append(f"# {citation_prefix} — {heading}")
    lines.append("")
    _render_level(unit_elem, lines, depth=1)
    return "\n".join(lines).rstrip() + "\n"


def _render_level(elem, lines: list[str], depth: int) -> None:
    """Emit content for a level (or root unit). Walks children in document order."""
    for child in elem:
        local = _local(child)
        if local in ("num", "heading"):
            # already handled at the parent level
            continue
        if local == "chapeau":
            txt = _text(child).strip()
            if txt:
                lines.append(txt); lines.append("")
        elif local == "continuation":
            txt = _text(child).strip()
            if txt:
                lines.append(""); lines.append(txt); lines.append("")
        elif local == "content":
            txt = _text(child).strip()
            if txt:
                lines.append(txt); lines.append("")
        elif local in _SUBLEVEL_ORDER:
            num_elem = child.find(_qn("num"))
            heading_elem = child.find(_qn("heading"))
            num = _text(num_elem).strip() if num_elem is not None else ""
            head = _text(heading_elem).strip() if heading_elem is not None else ""
            label_parts = [p for p in (num, head) if p]
            label = " ".join(label_parts)
            md_h = _SUBLEVEL_MD_HEADER.get(local, "######")
            if label:
                lines.append(f"{md_h} {label}")
            _render_level(child, lines, depth + 1)
        elif local == "note":
            # inline notes — render as italic block prefix, indented
            topic = child.get("topic", "")
            txt = _text(child).strip()
            if txt:
                lines.append(f"_(Note{' — ' + topic if topic else ''}: {txt})_")
                lines.append("")
        elif local == "sourceCredit":
            # Source credit gets rendered last — caller handles separately
            continue
        else:
            # def, proviso, table, signatures, etc. — fall through and dump text
            txt = _text(child).strip()
            if txt:
                lines.append(txt); lines.append("")


# ──────────────────────────────────────────────────────────────────────────
# Status-flag detection
# ──────────────────────────────────────────────────────────────────────────

def detect_status(unit_elem) -> tuple[bool, bool, bool, Optional[str]]:
    """Returns (is_repealed, is_transferred, is_omitted, transferred_to)."""
    status = unit_elem.get("status", "") or ""
    is_repealed = status == "repealed"
    is_transferred = status == "transferred"
    is_omitted = status in ("omitted", "reserved", "vacant")
    transferred_to = None

    # If status not on unit, peek at note topics
    if not (is_repealed or is_transferred or is_omitted):
        for note in unit_elem.iter(_qn("note")):
            topic = (note.get("topic", "") or "").lower()
            if topic == "transfer":
                is_transferred = True
                # find an internal href in the note as the destination
                ref = note.find(f".//{_qn('ref')}")
                if ref is not None:
                    href = ref.get("href", "")
                    transferred_to = href_to_canonical_citation(href) if href.startswith("/us/usc/") else href
                break

    if is_transferred and transferred_to is None:
        # try the unit's own redirect
        ref = unit_elem.find(f".//{_qn('ref')}")
        if ref is not None:
            href = ref.get("href", "")
            if href:
                transferred_to = href_to_canonical_citation(href) if href.startswith("/us/usc/") else href
    return is_repealed, is_transferred, is_omitted, transferred_to


# ──────────────────────────────────────────────────────────────────────────
# Notes harvesting
# ──────────────────────────────────────────────────────────────────────────

def collect_notes(unit_elem) -> list[NoteEntry]:
    out: list[NoteEntry] = []
    sort_order = 0
    # Children that are directly under the unit and are note-typed
    for child in unit_elem:
        local = _local(child)
        if local == "sourceCredit":
            txt = _text(child).strip()
            if txt:
                out.append(NoteEntry(note_type="source_credit", note_topic=None,
                                     note_heading=None, note_text=txt, sort_order=sort_order))
                sort_order += 1
        elif local in ("statutoryNote", "editorialNote", "changeNote"):
            note_type = {
                "statutoryNote": "statutory",
                "editorialNote": "editorial",
                "changeNote":    "change",
            }[local]
            heading = child.find(_qn("heading"))
            head_txt = _text(heading).strip() if heading is not None else None
            topic = child.get("topic")
            txt = _text(child).strip()
            out.append(NoteEntry(note_type=note_type, note_topic=topic,
                                 note_heading=head_txt, note_text=txt, sort_order=sort_order))
            sort_order += 1
        elif local == "notes":
            # <notes> wrapper — walk children
            for note in child:
                if _local(note) != "note":
                    continue
                topic = note.get("topic")
                heading = note.find(_qn("heading"))
                head_txt = _text(heading).strip() if heading is not None else None
                txt = _text(note).strip()
                out.append(NoteEntry(note_type="generic", note_topic=topic,
                                     note_heading=head_txt, note_text=txt, sort_order=sort_order))
                sort_order += 1
    return out


# ──────────────────────────────────────────────────────────────────────────
# Section / courtRule / reorganizationPlan parsers
# ──────────────────────────────────────────────────────────────────────────

def _section_number_from_num(num_elem) -> str:
    if num_elem is None:
        return ""
    val = num_elem.get("value")
    if val:
        return val
    txt = (_text(num_elem) or "").strip()
    return re.sub(r"[§\.]", "", txt).strip()


def _build_section_unit(section_elem, *, title_number: int, release_point: str,
                       chapter_number: Optional[str], chapter_heading: Optional[str],
                       subchapter_id_text: Optional[str], subchapter_heading: Optional[str],
                       is_appendix: bool = False) -> ParsedUnit:
    num_elem = section_elem.find(_qn("num"))
    head_elem = section_elem.find(_qn("heading"))
    section_number = _section_number_from_num(num_elem)
    section_heading = (_text(head_elem) or "").strip() if head_elem is not None else ""
    citation = make_canonical_citation(
        unit_type="section", title_number=title_number,
        section_number=section_number, rule_set=None,
        is_appendix_section=is_appendix,
    )
    is_repealed, is_transferred, is_omitted, transferred_to = detect_status(section_elem)
    history: list[HistoryEntry] = []
    for sc in section_elem.iter(_qn("sourceCredit")):
        history.extend(parse_source_credit(_text(sc)))
    enacted_by_pl = history[0].pub_law if history else None
    statutes_at_large = history[0].statutes_ref if history else None
    effective_date = history[0].pub_law_date if history else None
    md = render_markdown(section_elem, citation_prefix=f"§ {section_number}", heading=section_heading)
    return ParsedUnit(
        unit_type="section",
        title_number=title_number,
        section_number=section_number,
        section_heading=section_heading,
        canonical_citation=citation,
        full_text=md,
        source_xml=_to_xml_str(section_elem),
        release_point=release_point,
        chapter_number=chapter_number,
        chapter_heading=chapter_heading,
        subchapter_id_text=subchapter_id_text,
        subchapter_heading=subchapter_heading,
        is_repealed=is_repealed, is_transferred=is_transferred, is_omitted=is_omitted,
        transferred_to=transferred_to,
        effective_date=effective_date,
        enacted_by_pl=enacted_by_pl,
        statutes_at_large_ref=statutes_at_large,
        history=history,
        notes=collect_notes(section_elem),
        cross_refs=collect_cross_refs(section_elem),
        uslm_identifier=section_elem.get("identifier"),
    )


def _build_court_rule_unit(rule_elem, *, title_number: int, release_point: str,
                          rule_set: str) -> ParsedUnit:
    num_elem = rule_elem.find(_qn("num"))
    head_elem = rule_elem.find(_qn("heading"))
    rule_number = (num_elem.get("value") if num_elem is not None else "") or _section_number_from_num(num_elem)
    rule_heading = (_text(head_elem) or "").strip() if head_elem is not None else ""
    citation = make_canonical_citation(
        unit_type="court_rule", title_number=title_number,
        section_number=rule_number, rule_set=rule_set,
    )
    is_repealed, is_transferred, is_omitted, transferred_to = detect_status(rule_elem)
    md = render_markdown(rule_elem, citation_prefix=f"Rule {rule_number}", heading=rule_heading)
    return ParsedUnit(
        unit_type="court_rule",
        title_number=title_number,
        section_number=rule_number,
        section_heading=rule_heading,
        canonical_citation=citation,
        full_text=md,
        source_xml=_to_xml_str(rule_elem),
        release_point=release_point,
        rule_set=rule_set,
        is_repealed=is_repealed, is_transferred=is_transferred, is_omitted=is_omitted,
        transferred_to=transferred_to,
        notes=collect_notes(rule_elem),
        cross_refs=collect_cross_refs(rule_elem),
        uslm_identifier=rule_elem.get("identifier"),
    )


_RE_REORG_NUM = re.compile(
    r"REORGANIZATION\s+PLAN\s+(?:NO\.\s*)?([IVXLCDM\d]+)\s+OF\s+(\d{4})",
    re.IGNORECASE,
)


def _build_reorg_plan_unit(plan_elem, *, title_number: int, release_point: str) -> ParsedUnit:
    head_elem = plan_elem.find(_qn("heading"))
    heading_text = (_text(head_elem) or "").strip() if head_elem is not None else ""
    m = _RE_REORG_NUM.search(heading_text)
    if m:
        roman = m.group(1)
        year = m.group(2)
        section_number = f"{roman} of {year}"
    else:
        # Fallback to identifier last two segments
        ident = plan_elem.get("identifier", "")
        parts = ident.rstrip("/").split("/")
        section_number = "/".join(parts[-2:]) if len(parts) >= 2 else heading_text
    citation = make_canonical_citation(
        unit_type="reorganization_plan", title_number=title_number,
        section_number=section_number, rule_set=None,
    )
    is_repealed, is_transferred, is_omitted, transferred_to = detect_status(plan_elem)
    md = render_markdown(plan_elem, citation_prefix="Reorg. Plan", heading=heading_text)
    return ParsedUnit(
        unit_type="reorganization_plan",
        title_number=title_number,
        section_number=section_number,
        section_heading=heading_text,
        canonical_citation=citation,
        full_text=md,
        source_xml=_to_xml_str(plan_elem),
        release_point=release_point,
        is_repealed=is_repealed, is_transferred=is_transferred, is_omitted=is_omitted,
        transferred_to=transferred_to,
        notes=collect_notes(plan_elem),
        cross_refs=collect_cross_refs(plan_elem),
        uslm_identifier=plan_elem.get("identifier"),
    )


# ──────────────────────────────────────────────────────────────────────────
# Streaming title parser — main entry point
# ──────────────────────────────────────────────────────────────────────────

# Tags whose end-events we want, in iterparse format (with namespace)
_TARGET_TAGS = (
    _qn("section"),
    _qn("courtRule"),
    _qn("reorganizationPlan"),
    _qn("chapter"),
    _qn("subchapter"),
    _qn("courtRules"),
)


def parse_title_xml(xml_path: str | Path, *, title_number: int, release_point: str,
                    is_appendix: bool = False) -> Iterator[ParsedUnit]:
    """Stream-parse one USC title XML file. Yields ParsedUnit per ingestible
    element. Memory-bounded via element clearing.

    Maintains shallow context state for chapter/subchapter/courtRules wrappers
    so that emitted ParsedUnit records carry enclosing structural metadata.
    """
    xml_path = Path(xml_path)
    source_filename = xml_path.name
    cur_chapter_number: Optional[str] = None
    cur_chapter_heading: Optional[str] = None
    cur_subchapter_id: Optional[str] = None
    cur_subchapter_heading: Optional[str] = None
    cur_rule_set: Optional[str] = None
    # Strategy B: first-occurrence wins on duplicate (wrapper, num) within a
    # single <courtRules> wrapper. Tracks num values already emitted under
    # the current cur_rule_set; later duplicates are dropped + logged.
    cur_seen_rule_nums: set[str] = set()
    cur_wrapper_ident: Optional[str] = None
    drop_log_path = Path("/opt/wdws/data/usc/logs/strategy_b_dropped.log")
    drop_log_path.parent.mkdir(parents=True, exist_ok=True)
    drop_log = open(drop_log_path, "a", encoding="utf-8")

    context = etree.iterparse(str(xml_path), events=("start", "end"), tag=_TARGET_TAGS)
    for event, elem in context:
        local = _local(elem)
        if event == "start":
            if local == "chapter":
                num = elem.find(_qn("num"))
                head = elem.find(_qn("heading"))
                cur_chapter_number = (num.get("value") if num is not None else None) or _section_number_from_num(num)
                cur_chapter_heading = (_text(head) or "").strip() if head is not None else None
                cur_subchapter_id = None
                cur_subchapter_heading = None
            elif local == "subchapter":
                num = elem.find(_qn("num"))
                head = elem.find(_qn("heading"))
                cur_subchapter_id = (num.get("value") if num is not None else None) or _section_number_from_num(num)
                cur_subchapter_heading = (_text(head) or "").strip() if head is not None else None
            elif local == "courtRules":
                cur_rule_set = determine_rule_set(elem, source_filename)
                cur_wrapper_ident = elem.get("identifier") or "(no-ident)"
                cur_seen_rule_nums = set()
            continue

        # event == 'end'
        if local == "section":
            # Identifier guard: only emit authoritative USC sections. OLRC USLM
            # wraps quoted Pub.L. text and other embedded passages inside
            # editorial notes in <section> elements (likely for typesetting
            # consistency). These quoted sections lack @identifier="/us/usc/..."
            # because they aren't part of the codified USC. Without this guard,
            # they get parsed as USC sections and (when their <num value=> matches
            # an existing USC section number) silently overwrite the correct
            # section's body via ON CONFLICT.
            ident = elem.get("identifier") or ""
            if ident.startswith("/us/usc/"):
                yield _build_section_unit(
                    elem,
                    title_number=title_number,
                    release_point=release_point,
                    chapter_number=cur_chapter_number,
                    chapter_heading=cur_chapter_heading,
                    subchapter_id_text=cur_subchapter_id,
                    subchapter_heading=cur_subchapter_heading,
                    is_appendix=is_appendix,
                )
        elif local == "courtRule":
            # Same identifier guard — quoted court rule snippets in advisory
            # committee notes lack the /us/usc/.../courtRules/... identifier.
            ident = elem.get("identifier") or ""
            if ident.startswith("/us/usc/"):
                if not cur_rule_set:
                    raise RuntimeError(
                        f"<courtRule> found outside any <courtRules> wrapper in {source_filename}"
                    )
                # Strategy B: first-occurrence wins. Detect duplicates within
                # the current <courtRules> wrapper by num.value.
                num_elem = elem.find(_qn("num"))
                num_val = (num_elem.get("value") if num_elem is not None else "") or ""
                if num_val and num_val in cur_seen_rule_nums:
                    head_elem = elem.find(_qn("heading"))
                    head_txt = (_text(head_elem) or "").strip()
                    drop_log.write(
                        f"file={source_filename} wrapper={cur_wrapper_ident} "
                        f"num={num_val} heading={head_txt[:120]!r} "
                        f"reason=duplicate_uri_first_occurrence_wins\n"
                    )
                    drop_log.flush()
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
                    continue
                cur_seen_rule_nums.add(num_val)
                yield _build_court_rule_unit(
                    elem,
                    title_number=title_number,
                    release_point=release_point,
                    rule_set=cur_rule_set,
                )
        elif local == "reorganizationPlan":
            ident = elem.get("identifier") or ""
            if ident.startswith("/us/usc/"):
                yield _build_reorg_plan_unit(
                    elem,
                    title_number=title_number,
                    release_point=release_point,
                )
        elif local == "courtRules":
            cur_rule_set = None
            cur_wrapper_ident = None
            cur_seen_rule_nums = set()
        elif local == "subchapter":
            cur_subchapter_id = None
            cur_subchapter_heading = None
        elif local == "chapter":
            cur_chapter_number = None
            cur_chapter_heading = None
            cur_subchapter_id = None
            cur_subchapter_heading = None

        # Free memory — element body has been processed
        elem.clear()
        # Also remove preceding siblings to keep the parent's children list short
        while elem.getprevious() is not None:
            del elem.getparent()[0]


# ──────────────────────────────────────────────────────────────────────────
# Title metadata extraction (for usc.titles seed)
# ──────────────────────────────────────────────────────────────────────────

# Hand-curated positive-law list per uslm_findings.md §5
POSITIVE_LAW_TITLES = frozenset({
    1, 3, 4, 5, 9, 10, 11, 13, 14, 17, 18, 23, 28, 31, 32, 35, 36, 37, 38,
    39, 40, 41, 44, 46, 49, 51, 54,
})


def title_meta_from_xml(xml_path: str | Path) -> dict:
    """Parse just the <meta> section of a title file for usc.titles row population."""
    tree = etree.parse(str(xml_path))
    root = tree.getroot()
    meta = root.find(_qn("meta"))
    out = {}
    if meta is not None:
        for child in meta:
            tag = etree.QName(child).localname
            if tag in ("title", "type", "publisher", "creator"):
                out[tag] = (child.text or "").strip()
            elif tag == "docNumber":
                out["docNumber"] = (child.text or "").strip()
            elif tag == "docPublicationName":
                out["docPublicationName"] = (child.text or "").strip()
            elif tag == "property":
                role = child.get("role")
                if role == "is-positive-law":
                    out["is_positive_law_meta"] = (child.text or "").strip().lower() == "yes"
    return out


if __name__ == "__main__":
    import argparse, json, sys
    ap = argparse.ArgumentParser()
    ap.add_argument("xml_path")
    ap.add_argument("--title", type=int, required=True)
    ap.add_argument("--release", default="PL 119-84")
    ap.add_argument("--appendix", action="store_true")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N units (smoke testing)")
    args = ap.parse_args()
    n = 0
    for u in parse_title_xml(args.xml_path, title_number=args.title,
                             release_point=args.release,
                             is_appendix=args.appendix):
        n += 1
        if args.limit and n > args.limit:
            break
        print(f"[{n:5d}] {u.unit_type:<20} {u.canonical_citation:<35} "
              f"len(text)={len(u.full_text):>6}  "
              f"refs={len(u.cross_refs)}  notes={len(u.notes)}  "
              f"flags={'R' if u.is_repealed else '-'}{'T' if u.is_transferred else '-'}{'O' if u.is_omitted else '-'}")
    print(f"-- emitted {n} units")
