"""WA Court Rules — Phase 3 parser.

Iterates over the 1,040 cached PDFs at /opt/wdws/data/wa_court_rules/pdf/<set>/
and emits one JSON-line per parsed rule to phase3_parsed.jsonl. NO DATABASE
INSERTS — Phase 4 ingest is a separate authorization gate.

Inputs:
  - /opt/wdws/wa_court_rules_research/cached_html/inventory.json (rule list)
  - /opt/wdws/data/wa_court_rules/pdf/<set>/*.pdf (1,040 cached PDFs)
  - phase1_regex_test.py (citation regex — imported)

Outputs:
  - /opt/wdws/wa_court_rules_research/phase3_parsed.jsonl
  - /opt/wdws/wa_court_rules_research/phase3_cumulative_report.md
  - /opt/wdws/wa_court_rules_research/phase3_anomalies.json
  - /opt/wdws/wa_court_rules_research/HALT_PHASE3.md (only on halt)

Special-case handling (locked-in per architectural decisions):
  - CR 45: standard rule, no special handling
  - CJC: descend into 4 Canon PDFs, emit one row per RULE N.M chunk
         (no Canon-level row); body is just the inner-rule chunk
  - Range placeholders: emit canonical (range citation) row + expanded
    child rows (one per integer in range, with placeholder body)
    Multi-decimal ambiguous ranges: canonical only, flag for review
  - APR Regs (GA_APR_11_REGS%20101-112.pdf): split internal "Regulation N."
    entries into N rows, rule_number='Regs/<N>', parent_regulation_handle='Regs',
    is_repealed=TRUE
  - APR LP_REGS: single row, rule_number='LP_REGS', is_reserved=TRUE
  - APR LP_DIS_REGS: single row, rule_number='LP_DIS_REGS', is_repealed=TRUE,
    last_amended_date='2017-12-05'
  - APR 15P: single row, rule_number='15P', body=entire PDF including all
    13 internal Regulations (per operator-confirmed sub-numbering audit)
  - Comments: RPC pattern [N] numbered + "Additional Washington Comments"
    section + history brackets. ER variant uses "Comment NNN" headings.
    CJC: per-RULE comments inside Canon PDFs.
  - Multi-line titles: collapse whitespace, strip.

Halt protocol:
  - Citation regex match outside T17-tested patterns → HALT
  - New structural class beyond Phase 0/Phase 2 inventory → HALT
  - Comment-section parsing diverges from RPC/CJC `[N]` numbered pattern → HALT
  - PDF extraction quality issue (>5% empty/garbled) → HALT
  - 2% parse error rate corpus-wide (~21 of 1,040) → HALT
  - Schema-affecting decision needed → HALT

Run:
    python /opt/wdws/wa_court_rules_research/phase3_parser.py
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.parse
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Optional

# Import citation regex constants + parse_cite from Phase 1.
sys.path.insert(0, str(Path(__file__).parent))
from phase1_regex_test import (  # type: ignore
    WACR_CITE_RE,
    CJC_CITE_RE,
    APR_HANDLE_RE,
    SUBSEC_RE,
    CMT_RE,
    parse_cite,
    # Cross-jurisdiction citation regex — added 2026-05-01 per Phase 5 Investigation 2
    RCW_CITE_RE,
    RCW_CHAPTER_CITE_RE,  # added 2026-05-01 per Phase 5 GR 33 verification (chapter-form)
    USC_CITE_RE,
    WAC_CITE_RE,
    WACONST_CITE_RE,
    normalize_usc_section,  # 2026-05-01: USC et-seq word-boundary post-capture trim
)

# =============================================================================
# Paths & constants
# =============================================================================

RESEARCH_DIR = Path('/opt/wdws/wa_court_rules_research')
PDF_BASE = Path('/opt/wdws/data/wa_court_rules/pdf')
INVENTORY_PATH = RESEARCH_DIR / 'cached_html' / 'inventory.json'
OUTPUT_JSONL = RESEARCH_DIR / 'phase3_parsed.jsonl'
ANOMALIES_PATH = RESEARCH_DIR / 'phase3_anomalies.json'
REPORT_PATH = RESEARCH_DIR / 'phase3_cumulative_report.md'
HALT_PATH = RESEARCH_DIR / 'HALT_PHASE3.md'

IN_SCOPE_SETS = ['GR', 'RPC', 'CR', 'ER', 'RAP', 'CJC', 'GALR',
                 'CrR', 'CrRLJ', 'CRLJ', 'JuCR', 'SCCAR', 'RALJ', 'APR', 'ELC']

# Halt thresholds
PARSE_ERROR_HALT_THRESHOLD = 0.02   # 2% corpus-wide
EMPTY_PDF_HALT_THRESHOLD = 0.05     # 5% empty/garbled


# =============================================================================
# Date parsing helpers
# =============================================================================

MONTHS = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
          'june': 6, 'july': 7, 'august': 8, 'september': 9, 'october': 10,
          'november': 11, 'december': 12,
          'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6, 'jul': 7,
          'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12}

# "September 1, 2002" / "Sep. 1, 2002" / "Sept. 1, 2002"
DATE_RE = re.compile(
    r'\b(' + '|'.join(MONTHS.keys()) + r')\.?\s+(\d{1,2}),\s+(\d{4})\b',
    re.IGNORECASE,
)


def parse_history_dates(text: str) -> list[tuple[Optional[date], str]]:
    """Extract amendment-history brackets like:

        [Adopted effective September 1, 2002.]
        [Adopted effective September 1, 2002; Amended effective April 1, 2020.]

    Returns list of (date_or_None, sentence_text). Only spans inside [...]
    that contain an "effective" / "adopted" / "amended" / "deleted" /
    "rescinded" keyword count as history brackets.
    """
    out: list[tuple[Optional[date], str]] = []
    history_kw = re.compile(
        r'(?:adopted|amended|effective|deleted|rescinded|repealed|reserved|added|renumbered|formerly)',
        re.IGNORECASE,
    )
    for m in re.finditer(r'\[([^\[\]]+)\]', text):
        body = m.group(1).strip()
        if not history_kw.search(body):
            continue
        # split on ';' but preserve grouping if there are multiple amendments
        # in a single bracket. Each clause becomes one history row.
        clauses = [c.strip() for c in re.split(r';\s*', body) if c.strip()]
        for clause in clauses:
            d_match = DATE_RE.search(clause)
            d_val: Optional[date] = None
            if d_match:
                try:
                    mo = MONTHS[d_match.group(1).lower().rstrip('.')]
                    day = int(d_match.group(2))
                    yr = int(d_match.group(3))
                    d_val = date(yr, mo, day)
                except (KeyError, ValueError):
                    d_val = None
            out.append((d_val, clause))
    return out


def first_and_last_dates(history: list[tuple[Optional[date], str]]) -> tuple[Optional[date], Optional[date]]:
    """Return (effective_date, last_amended_date).

    effective_date = earliest date with 'adopted'/'effective'/'added' keyword
    last_amended_date = latest date with 'amended'/'deleted'/'rescinded'/'reserved' keyword
    """
    eff: Optional[date] = None
    lam: Optional[date] = None
    for d, clause in history:
        if d is None:
            continue
        lc = clause.lower()
        if any(k in lc for k in ('adopted', 'originally effective', 'effective', 'added')):
            if eff is None or d < eff:
                eff = d
        if any(k in lc for k in ('amended', 'deleted', 'rescinded', 'reserved', 'renumbered')):
            if lam is None or d > lam:
                lam = d
    return eff, lam


# =============================================================================
# PDF text extraction
# =============================================================================

def extract_pdf_text(pdf_path: Path) -> tuple[str, Optional[str]]:
    """Run `pdftotext -layout` on a PDF; return (text, error_or_None).

    Empty/garbled output is signaled by error string when text is suspicious.
    """
    try:
        proc = subprocess.run(
            ['pdftotext', '-layout', str(pdf_path), '-'],
            capture_output=True, text=True, timeout=60,
        )
        if proc.returncode != 0:
            return ('', f'pdftotext rc={proc.returncode}: {proc.stderr.strip()[:200]}')
        text = proc.stdout
        if not text.strip():
            return ('', 'empty extraction')
        # Garbled-detection: if non-printable chars >5% of total, flag it
        nonprintable = sum(1 for c in text if not c.isprintable() and c not in '\n\r\t ')
        if len(text) > 100 and nonprintable / len(text) > 0.05:
            return (text, f'garbled: {nonprintable}/{len(text)} non-printable')
        return (text, None)
    except subprocess.TimeoutExpired:
        return ('', 'pdftotext timeout')
    except Exception as e:
        return ('', f'extraction exception: {type(e).__name__}: {e}')


# =============================================================================
# Title / body cleaning
# =============================================================================

WS_RUN_RE = re.compile(r'\s+')


def clean_title(s: str) -> str:
    """Collapse whitespace runs (incl. \n + indented continuations) to one space."""
    if not s:
        return ''
    return WS_RUN_RE.sub(' ', s).strip()


# =============================================================================
# Comment-section extraction (RPC + CJC + ER patterns)
# =============================================================================

# RPC: "Comment" header followed by [1]...[N] numbered paragraphs, possibly
# preceded by sub-section headers (e.g., "Legal Knowledge and Skill"). May
# also have "[Comment N Adopted ...]" brackets between paragraphs and a
# trailing "Additional Washington Comments" / "[Washington revision]" zone.
COMMENT_HEADER_RE = re.compile(
    r'^\s*Comment(?:s|\s+\d+)?\s*$',
    re.MULTILINE | re.IGNORECASE,
)


def extract_comment_block(body_text: str) -> Optional[str]:
    """Return the full comment block (header onward) if present, else None.

    Heuristic: locate "Comment" / "Comments" / "Comment NNN" header line
    near the bottom of the body. Everything from that line to end of body
    (or to the next top-level rule heading) constitutes the comment block.

    For ER variant ("Comment 401" instead of "Comment"), the same regex
    matches via the optional rule-number group.
    """
    matches = list(COMMENT_HEADER_RE.finditer(body_text))
    if not matches:
        return None
    # Use the first comment-header match (most rules have one such header).
    # We could pick the lowest match for safety, but tests show first is right.
    start = matches[0].start()
    block = body_text[start:].strip()
    # Sanity: a comment block should contain at least one bracketed item
    # ([1] / [Deleted ...] / [Comment N Adopted ...]) OR be a deletion stub
    # (e.g., "Comment 401\n\n[Deleted effective ...]").
    if re.search(r'\[\d+\]', block) or re.search(r'\[(?:Deleted|Adopted|Comment|Washington)', block, re.IGNORECASE):
        return block
    return None


# =============================================================================
# Parser data class
# =============================================================================

@dataclass
class ParsedRule:
    rule_set_code: str
    rule_number: str
    canonical_citation: str
    rule_title: str
    is_repealed: bool = False
    is_reserved: bool = False
    parent_canon: Optional[str] = None
    parent_regulation_handle: Optional[str] = None
    range_doc_id_ref: Optional[str] = None       # placeholder for Phase 4 link
    range_canonical_citation: Optional[str] = None
    range_start: Optional[str] = None
    range_end: Optional[str] = None
    full_text: str = ''
    has_comment: bool = False
    comment_text: Optional[str] = None
    effective_date: Optional[str] = None
    last_amended_date: Optional[str] = None
    source_pdf_path: Optional[str] = None
    history: list[dict] = field(default_factory=list)
    cross_references: list[dict] = field(default_factory=list)
    notes: Optional[str] = None


# =============================================================================
# Cross-reference extraction
# =============================================================================

def classify_ref_type(set_code: str, body_pos: int, body: str) -> str:
    """Heuristic ref_type classifier given a citation match position."""
    # Look at ~80 chars before the match for context cues.
    ctx_before = body[max(0, body_pos - 80):body_pos].lower()
    if 'authority' in ctx_before or 'pursuant to' in ctx_before:
        return 'authority'
    if 'comment' in ctx_before or 'cmt' in ctx_before:
        return 'comment'
    if set_code in ('RCW', 'WAC', 'USC'):
        return 'cross-statute'
    if '(' in body[body_pos:body_pos + 30]:
        # Subsection cite like "CR 12(b)(6)"
        return 'subsection-cite'
    return 'cross-rule'


def extract_cross_references(body: str) -> list[dict]:
    """Run citation regex over body and return cross-ref records.

    Resolution order: CJC → APR_HANDLE → main court-rule → RCW → USC → WAC → WA-Const.
    Court-rule passes resolve to target_jurisdiction='state-wa-court-rule';
    cross-jurisdiction passes resolve to their respective slugs (state-wa-rcw,
    federal-usc, state-wa-wac, wa-constitution).

    Each regex is iterated independently so all matches in a single body are
    captured. Span dedup avoids double-counting overlapping matches across
    patterns (matters mostly for the court-rule passes; cross-jurisdiction
    patterns rarely overlap with court-rule patterns).

    Cross-jurisdiction passes added 2026-05-01 per Phase 5 Investigation 2
    defect resolution (Option A — immediate fix). Authority symmetric with
    RCW + WAC corpus parsers.
    """
    refs: list[dict] = []
    seen_spans: list[tuple[int, int]] = []  # avoid double-counting overlapping matches

    def overlaps(start: int, end: int) -> bool:
        for s, e in seen_spans:
            if not (end <= s or start >= e):
                return True
        return False

    # Pass 1: CJC (most specific)
    for m in CJC_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        canon, inner = m.group(1), m.group(2)
        if canon:
            target_num = canon
            target_cite = f'CJC Canon {canon}'
            ref_form = 'canon'
        else:
            target_num = inner
            target_cite = f'CJC {inner}'
            ref_form = 'inner'
        refs.append({
            'target_citation': target_cite,
            'target_set_code': 'CJC',
            'target_rule_number': target_num,
            'target_jurisdiction': 'state-wa-court-rule',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'cross-rule',
            '_form': ref_form,
        })

    # Pass 2: APR_HANDLE
    for m in APR_HANDLE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        handle = m.group(1)
        sub = m.group(2)
        target_num = f'{handle}/{sub}' if sub else handle
        target_cite = f'APR {handle} {sub}' if sub else f'APR {handle}'
        refs.append({
            'target_citation': target_cite,
            'target_set_code': 'APR',
            'target_rule_number': target_num,
            'target_jurisdiction': 'state-wa-court-rule',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'cross-rule',
            '_apr_handle': handle,
            '_apr_sub': sub,
        })

    # Pass 3: main WACR_CITE_RE
    for m in WACR_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        set_code = m.group(1)
        num = m.group(2)
        # Look ahead for subsection notation right after the match
        rest = body[e:e + 40]
        subs = SUBSEC_RE.findall(rest)
        target_subsection = ''.join(f'({x})' for x in subs) if subs else None
        # Look for trailing comment ref
        cmt_match = CMT_RE.search(body[e:e + 40])
        target_cmt_index = int(cmt_match.group(1)) if cmt_match else None
        target_cite = f'{set_code} {num}'
        if target_subsection:
            target_cite += target_subsection
        if target_cmt_index is not None:
            target_cite += f' Comment [{target_cmt_index}]'
        ref_type = classify_ref_type(set_code, s, body)
        if target_cmt_index is not None:
            ref_type = 'comment'
        refs.append({
            'target_citation': target_cite,
            'target_set_code': set_code,
            'target_rule_number': num,
            'target_subsection': target_subsection,
            'target_cmt_index': target_cmt_index,
            'target_jurisdiction': 'state-wa-court-rule',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': ref_type,
        })

    # =========================================================================
    # Cross-jurisdiction passes (added 2026-05-01)
    # =========================================================================

    # Pass 4: RCW section citations → target_jurisdiction='state-wa-rcw'
    for m in RCW_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        section = m.group(1)  # e.g., '4.28.080' or '62A.9-310'
        refs.append({
            'target_citation': f'RCW {section}',
            'target_set_code': 'RCW',
            'target_rule_number': section,
            'target_jurisdiction': 'state-wa-rcw',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'cross-statute',
        })

    # Pass 4b: RCW chapter citations ('chapter X.YY RCW' or 'ch. X.YY RCW')
    # → target_jurisdiction='state-wa-rcw' with target_rule_number=chapter
    # (chapter-level cite has no section; FK resolution to rcw.sections will
    # be NULL for these — they target chapter-level statutes like
    # WLAD ch. 49.60 RCW). Added 2026-05-01 per Phase 5 GR 33 verification.
    for m in RCW_CHAPTER_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        chapter = m.group(1)  # e.g., '49.60' or '4.28'
        refs.append({
            'target_citation': f'RCW {chapter}',                # canonical chapter cite form
            'target_set_code': 'RCW',
            'target_rule_number': chapter,
            'target_jurisdiction': 'state-wa-rcw',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'authority',                             # chapter-level cites are usually authority/foundational
        })

    # Pass 5: Federal USC citations → target_jurisdiction='federal-usc'
    for m in USC_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        title = m.group(1)
        # Post-capture trim for the 'et seq.' word-boundary defect (e.g.,
        # APR 8: "25 U.S.C. § 1901et seq." raw-captures '1901et', helper
        # strips trailing 'et' when followed by 'seq' in body). T17.34.
        section = normalize_usc_section(m.group(2), body, e)
        seen_spans[-1] = (s, e)  # span unchanged; helper only trims captured text
        refs.append({
            'target_citation': f'{title} U.S.C. § {section}',
            'target_set_code': 'USC',
            'target_rule_number': f'{title}.{section}',
            'target_jurisdiction': 'federal-usc',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'cross-statute',
        })

    # Pass 6: WAC section citations → target_jurisdiction='state-wa-wac'
    for m in WAC_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        section = m.group(1)  # e.g., '162-22-010'
        refs.append({
            'target_citation': f'WAC {section}',
            'target_set_code': 'WAC',
            'target_rule_number': section,
            'target_jurisdiction': 'state-wa-wac',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'cross-statute',
        })

    # Pass 7: WA Constitution citations → target_jurisdiction='wa-constitution'
    for m in WACONST_CITE_RE.finditer(body):
        s, e = m.start(), m.end()
        if overlaps(s, e):
            continue
        seen_spans.append((s, e))
        # Two alternation branches: explicit form (groups 1, 2) or narrative form (groups 3, 4)
        article = m.group(1) or m.group(3)
        section = m.group(2) or m.group(4)
        section_part = f', § {section}' if section else ''
        target_cite = f'Wash. Const. art. {article}{section_part}'
        refs.append({
            'target_citation': target_cite,
            'target_set_code': 'WACONST',
            'target_rule_number': f'art.{article}' + (f'.{section}' if section else ''),
            'target_jurisdiction': 'wa-constitution',
            'context_snippet': body[max(0, s - 60):min(len(body), e + 60)].replace('\n', ' '),
            'ref_type': 'authority',
        })

    return refs


# =============================================================================
# Standard rule parser (default for non-special PDFs)
# =============================================================================

# Detection regexes for is_repealed / is_reserved.
#
# Reserved/repealed status applies to the WHOLE RULE, not just an inner
# sub-section or a child comment. So we only set the flag when the body is
# predominantly a reservation/rescission marker — operationalized as either
# (a) the body is short (<300 chars body content — a placeholder rule), OR
# (b) the [RESERVED]/[RESCINDED]/[REPEALED]/[DELETED] marker appears in the
# first ~200 chars after the heading area (i.e., where the rule body should be).
# This avoids false-positives like:
#   - CJC 2.11 contains "(4) [Reserved.]" mid-text → not a rule-level reservation
#   - ER 401 has Comment 401 [Deleted ...] but rule body is active
RESERVED_BODY_RE = re.compile(
    r'\[\s*RESERVED\.?\s*\]', re.IGNORECASE,
)
REPEALED_BODY_RE = re.compile(
    r'\[\s*(?:RESCINDED|REPEALED|DELETED)\b', re.IGNORECASE,
)


def detect_status(body: str) -> tuple[bool, bool]:
    """Return (is_repealed, is_reserved).

    Only flags a rule when the marker indicates whole-rule status, not a
    sub-section or comment-level note. Heuristics:
      - body length < 300 chars AND contains the marker → flag
      - first 400 chars (post-heading) contains the marker on its own line
        AND no substantive body content precedes it → flag
    """
    is_reserved = False
    is_repealed = False

    body_stripped = body.strip()
    body_len = len(body_stripped)

    # Heuristic 1: very short body (<300 chars) — definitely a placeholder
    if body_len < 300:
        if RESERVED_BODY_RE.search(body):
            is_reserved = True
        if REPEALED_BODY_RE.search(body):
            is_repealed = True
        return is_repealed, is_reserved

    # Heuristic 2: marker on its own line within the first ~400 chars after
    # the heading. We split into lines and scan for a standalone marker line
    # (whitespace + bracket-marker + whitespace) — substantive body would
    # have been ahead of it.
    lines = body.splitlines()
    # Skip blank lines + heading lines (typically first 1-3 lines are the
    # rule citation + title in ALL CAPS). Find first non-trivial line.
    first_substantive = None
    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue
        # Skip heading-style lines: short, all-caps, or matches rule citation
        if len(s) < 80 and (s.isupper() or re.match(r'^[A-Z]{2,8}\s+\d', s)):
            continue
        first_substantive = s
        break

    if first_substantive is not None:
        # If the first substantive line is the marker itself, flag it.
        if RESERVED_BODY_RE.match(first_substantive) or first_substantive.upper().startswith('[RESERVED'):
            is_reserved = True
        if REPEALED_BODY_RE.match(first_substantive) or any(
            first_substantive.upper().startswith(f'[{kw}')
            for kw in ('RESCINDED', 'REPEALED', 'DELETED')
        ):
            is_repealed = True

    return is_repealed, is_reserved


def parse_standard_rule(set_code: str, rule_num: str, title: str,
                        pdf_path: Path) -> tuple[Optional[ParsedRule], Optional[str]]:
    """Parse a single PDF as one rule. Return (record, error_or_None)."""
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        return (None, err)

    is_repealed, is_reserved = detect_status(body)
    history = parse_history_dates(body)
    eff, lam = first_and_last_dates(history)

    # Comment block extraction
    comment_text = extract_comment_block(body)
    has_comment = comment_text is not None

    # Cross-references
    xrefs = extract_cross_references(body)

    record = ParsedRule(
        rule_set_code=set_code,
        rule_number=rule_num,
        canonical_citation=f'{set_code} {rule_num}',
        rule_title=clean_title(title),
        is_repealed=is_repealed,
        is_reserved=is_reserved,
        full_text=body,
        has_comment=has_comment,
        comment_text=comment_text,
        effective_date=eff.isoformat() if eff else None,
        last_amended_date=lam.isoformat() if lam else None,
        source_pdf_path=str(pdf_path),
        history=[
            {'amendment_date': d.isoformat() if d else None,
             'amendment_text': txt,
             'sort_order': i + 1}
            for i, (d, txt) in enumerate(history)
        ],
        cross_references=xrefs,
        notes=err if err else None,
    )
    return (record, None)


# =============================================================================
# CJC Canon unpacking
# =============================================================================

# CJC Canon PDFs use ALL-CAPS "RULE N.M. Title" as section heading lines.
# We require uppercase "RULE" and the title to be on the same line as the
# heading (no newline within the match). Matching mixed-case "Rule" caused
# false-positive splits at inline references like "See Rule 2.11." mid-text.
CJC_INNER_RULE_RE = re.compile(
    r'^[ \t]*RULE\s+(\d+\.\d+)\.?[ \t]+([^\n]+?)[ \t]*$',
    re.MULTILINE,
)


def parse_cjc_canon(canon_num: str, canon_pdf: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Descend into a Canon PDF, emit one ParsedRule per RULE N.M chunk.

    Per architectural decision: NO Canon-level row. Each inner rule gets:
        rule_set_code='CJC', rule_number='2.11', canonical_citation='CJC 2.11',
        parent_canon='Canon 2', body = inner-rule chunk text only.
    """
    body, err = extract_pdf_text(canon_pdf)
    if err and not body:
        return ([], err)

    # Find all RULE N.M headings; their positions delimit chunks.
    matches = list(CJC_INNER_RULE_RE.finditer(body))
    if not matches:
        return ([], f'no inner RULE N.M chunks found in Canon {canon_num}')

    records: list[ParsedRule] = []
    for i, m in enumerate(matches):
        rule_num = m.group(1)
        rule_title = clean_title(m.group(2))
        chunk_start = m.start()
        chunk_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        chunk = body[chunk_start:chunk_end].strip()

        is_repealed, is_reserved = detect_status(chunk)
        history = parse_history_dates(chunk)
        eff, lam = first_and_last_dates(history)
        comment_text = extract_comment_block(chunk)
        has_comment = comment_text is not None
        xrefs = extract_cross_references(chunk)

        rec = ParsedRule(
            rule_set_code='CJC',
            rule_number=rule_num,
            canonical_citation=f'CJC {rule_num}',
            rule_title=rule_title,
            is_repealed=is_repealed,
            is_reserved=is_reserved,
            parent_canon=f'Canon {canon_num}',
            full_text=chunk,
            has_comment=has_comment,
            comment_text=comment_text,
            effective_date=eff.isoformat() if eff else None,
            last_amended_date=lam.isoformat() if lam else None,
            source_pdf_path=str(canon_pdf),
            history=[
                {'amendment_date': d.isoformat() if d else None,
                 'amendment_text': txt,
                 'sort_order': j + 1}
                for j, (d, txt) in enumerate(history)
            ],
            cross_references=xrefs,
        )
        records.append(rec)
    return (records, None)


# =============================================================================
# Range expansion
# =============================================================================

# Range patterns we'll handle:
#   '27-37'       integer range
#   '65-67'       integer range
#   '69-70'       integer range
#   '78-80'       integer range
#   '11.4-11.20'  dot-decimal range (same major) — ENUMERABLE
#   '18.18 - 18.20' dot-decimal range with spaces — ENUMERABLE
#   '72-76'       integer range
#   '86.04--99.04' multi-decimal ambiguous → canonical only

INT_RANGE_RE = re.compile(r'^\s*(\d+)\s*[-–]\s*(\d+)\s*$')
DECIMAL_RANGE_RE = re.compile(r'^\s*(\d+)\.(\d+)\s*[-–]\s*(\d+)\.(\d+)\s*$')


def enumerate_range(num_field: str) -> tuple[str, str, list[str], bool]:
    """Parse a range field like '27-37' or '11.4-11.20'.

    Returns (range_start, range_end, expanded_children, is_ambiguous).
    For ambiguous (multi-major-decimal) ranges, expanded_children=[] and
    is_ambiguous=True; the canonical row gets surfaced for operator review.
    """
    cleaned = num_field.strip()

    int_m = INT_RANGE_RE.match(cleaned)
    if int_m:
        a, b = int(int_m.group(1)), int(int_m.group(2))
        return (str(a), str(b), [str(i) for i in range(a, b + 1)], False)

    dec_m = DECIMAL_RANGE_RE.match(cleaned)
    if dec_m:
        major_a, minor_a = int(dec_m.group(1)), int(dec_m.group(2))
        major_b, minor_b = int(dec_m.group(3)), int(dec_m.group(4))
        if major_a == major_b:
            # Same major; enumerate minor numbers
            children = [f'{major_a}.{i}' for i in range(minor_a, minor_b + 1)]
            return (f'{major_a}.{minor_a}', f'{major_b}.{minor_b}', children, False)
        else:
            # Different majors with decimals — ambiguous
            return (f'{major_a}.{minor_a}', f'{major_b}.{minor_b}', [], True)

    # Unrecognized range pattern — canonical only, ambiguous flag
    return (cleaned, cleaned, [], True)


def parse_range_entry(set_code: str, num_field: str, title: str,
                      pdf_path: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Emit canonical row + expanded children for a range placeholder."""
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        body = ''  # range PDFs may have minimal/empty body

    range_start, range_end, children, ambiguous = enumerate_range(num_field)
    canonical_citation = f'{set_code} {num_field}'.strip()

    # Canonical row
    canonical_rec = ParsedRule(
        rule_set_code=set_code,
        rule_number=num_field,
        canonical_citation=canonical_citation,
        rule_title=clean_title(title) or 'Reserved',
        is_reserved=True,
        is_repealed=False,
        range_start=range_start,
        range_end=range_end,
        full_text=body,
        source_pdf_path=str(pdf_path),
        notes=('Range expansion ambiguous — multi-decimal notation; '
               'operator review required') if ambiguous else None,
        cross_references=extract_cross_references(body) if body else [],
    )

    records = [canonical_rec]

    if not ambiguous:
        for child_num in children:
            child_cite = f'{set_code} {child_num}'
            child = ParsedRule(
                rule_set_code=set_code,
                rule_number=child_num,
                canonical_citation=child_cite,
                rule_title='Reserved',
                is_reserved=True,
                is_repealed=False,
                range_canonical_citation=canonical_citation,
                range_start=None,
                range_end=None,
                full_text=(f'This rule is reserved as part of '
                           f'{canonical_citation}. See range citation.'),
                source_pdf_path=str(pdf_path),
            )
            records.append(child)

    return (records, None)


# =============================================================================
# APR irregular handles
# =============================================================================

APR_REG_HEADING_RE = re.compile(
    r'^\s*Regulation\s+(\d+)\.\s+(.+?)\s*$',
    re.MULTILINE,
)


def parse_apr_regs(pdf_path: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Parse APR Regs (Continuing Legal Education) — split per Regulation N."""
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        return ([], err)

    matches = list(APR_REG_HEADING_RE.finditer(body))
    if not matches:
        return ([], 'no Regulation N. headings found in APR Regs PDF')

    records: list[ParsedRule] = []
    for i, m in enumerate(matches):
        reg_num = m.group(1)
        reg_title = clean_title(m.group(2))
        chunk_start = m.start()
        chunk_end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        chunk = body[chunk_start:chunk_end].strip()

        history = parse_history_dates(chunk)
        eff, lam = first_and_last_dates(history)

        # All these were deleted, so is_repealed=TRUE per spec
        rec = ParsedRule(
            rule_set_code='APR',
            rule_number=f'Regs/{reg_num}',
            canonical_citation=f'APR Regs {reg_num}',
            rule_title=reg_title,
            is_repealed=True,
            is_reserved=False,
            parent_regulation_handle='Regs',
            full_text=chunk,
            has_comment=False,
            comment_text=None,
            effective_date=eff.isoformat() if eff else None,
            last_amended_date=lam.isoformat() if lam else None,
            source_pdf_path=str(pdf_path),
            history=[
                {'amendment_date': d.isoformat() if d else None,
                 'amendment_text': txt,
                 'sort_order': j + 1}
                for j, (d, txt) in enumerate(history)
            ],
            cross_references=extract_cross_references(chunk),
        )
        records.append(rec)
    return (records, None)


def parse_apr_lp_regs(pdf_path: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Single-row APR LP_REGS: [RESERVED]."""
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        body = ''
    history = parse_history_dates(body)
    eff, lam = first_and_last_dates(history)

    rec = ParsedRule(
        rule_set_code='APR',
        rule_number='LP_REGS',
        canonical_citation='APR LP_REGS',
        rule_title='Regulations of the APR 12 Limited Practice Board',
        is_reserved=True,
        is_repealed=False,
        parent_regulation_handle='LP_REGS',
        full_text=body or '[RESERVED]',
        effective_date=eff.isoformat() if eff else None,
        last_amended_date=lam.isoformat() if lam else None,
        source_pdf_path=str(pdf_path),
        history=[
            {'amendment_date': d.isoformat() if d else None,
             'amendment_text': txt,
             'sort_order': j + 1}
            for j, (d, txt) in enumerate(history)
        ],
        cross_references=extract_cross_references(body) if body else [],
    )
    return ([rec], None)


def parse_apr_lp_dis_regs(pdf_path: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Single-row APR LP_DIS_REGS: [RESCINDED effective 2017-12-05]."""
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        body = ''
    history = parse_history_dates(body)
    eff, lam = first_and_last_dates(history)
    # Override last_amended_date with the locked-in 2017-12-05 if not already set
    if lam is None:
        lam = date(2017, 12, 5)

    rec = ParsedRule(
        rule_set_code='APR',
        rule_number='LP_DIS_REGS',
        canonical_citation='APR LP_DIS_REGS',
        rule_title='Disciplinary Regulations Applicable to ELPOC Title (LPO)',
        is_repealed=True,
        is_reserved=False,
        parent_regulation_handle='LP_DIS_REGS',
        full_text=body or '[RESCINDED effective 2017-12-05]',
        effective_date=eff.isoformat() if eff else None,
        last_amended_date=lam.isoformat() if lam else None,
        source_pdf_path=str(pdf_path),
        history=[
            {'amendment_date': d.isoformat() if d else None,
             'amendment_text': txt,
             'sort_order': j + 1}
            for j, (d, txt) in enumerate(history)
        ],
        cross_references=extract_cross_references(body) if body else [],
    )
    return ([rec], None)


def parse_apr_15p(pdf_path: Path) -> tuple[list[ParsedRule], Optional[str]]:
    """Single-row APR 15P: entire PDF body, all 13 internal Regulations.

    Operator-confirmed via APR 15P sub-numbering audit (0 corpus hits) that
    we treat as one row, not 13.
    """
    body, err = extract_pdf_text(pdf_path)
    if err and not body:
        return ([], err)

    history = parse_history_dates(body)
    eff, lam = first_and_last_dates(history)

    rec = ParsedRule(
        rule_set_code='APR',
        rule_number='15P',
        canonical_citation='APR 15P',
        rule_title='Client Protection Fund (APR 15) Procedural Regulations',
        is_repealed=False,
        is_reserved=False,
        parent_regulation_handle=None,  # 15P uses main regex pattern, no handle
        full_text=body,
        effective_date=eff.isoformat() if eff else None,
        last_amended_date=lam.isoformat() if lam else None,
        source_pdf_path=str(pdf_path),
        history=[
            {'amendment_date': d.isoformat() if d else None,
             'amendment_text': txt,
             'sort_order': j + 1}
            for j, (d, txt) in enumerate(history)
        ],
        cross_references=extract_cross_references(body),
    )
    return ([rec], None)


# =============================================================================
# Inventory PDF-path resolution
# =============================================================================

def resolve_pdf_path(rel: str) -> Optional[Path]:
    """Resolve an inventory pdf field (e.g. '../court_rules/pdf/CJC/...')
    to an absolute path under PDF_BASE.

    Tries (1) as-is, (2) URL-encoded variant (spaces -> %20).
    """
    rel_clean = rel.replace('../court_rules/pdf/', '')
    cand1 = PDF_BASE / rel_clean
    if cand1.exists():
        return cand1
    cand2 = PDF_BASE / urllib.parse.quote(rel_clean, safe='/')
    if cand2.exists():
        return cand2
    cand3 = PDF_BASE / rel_clean.replace(' ', '%20')
    if cand3.exists():
        return cand3
    return None


# =============================================================================
# Main parse loop
# =============================================================================

def is_apr_regs(rule_num: str) -> bool:
    return rule_num == 'Regs'


def is_apr_lp_regs(rule_num: str) -> bool:
    return rule_num == 'LP_REGS'


def is_apr_lp_dis_regs(rule_num: str) -> bool:
    return rule_num == 'LP_DIS_REGS'


def is_apr_15p(rule_num: str) -> bool:
    return rule_num == '15P'


def is_range_field(num_field: str) -> bool:
    """True if num_field looks like a range (contains a dash separator).

    Excludes single-rule numbers like '15P' or '5A.1' which contain alpha
    suffixes but no dashes.
    """
    return '-' in num_field or '–' in num_field


def write_halt(reason: str, samples: list[str]):
    """Write HALT_PHASE3.md and exit with code 2."""
    body = [f'# HALT_PHASE3.md', '',
            f'**Reason:** {reason}',
            '',
            f'**Time:** {datetime.now().isoformat()}',
            '',
            '## Samples',
            '']
    for s in samples[:20]:
        body.append(f'- {s}')
    HALT_PATH.write_text('\n'.join(body))
    print(f'HALT WRITTEN to {HALT_PATH}', file=sys.stderr)
    sys.exit(2)


def main():
    if not INVENTORY_PATH.exists():
        print(f'inventory.json missing at {INVENTORY_PATH}', file=sys.stderr)
        sys.exit(1)

    inventory = json.loads(INVENTORY_PATH.read_text())
    in_scope_inventory = [s for s in inventory if s['set_code'] in IN_SCOPE_SETS]

    # Output streams
    OUTPUT_JSONL.write_text('')  # truncate
    out_jsonl = OUTPUT_JSONL.open('w')

    # Tracking
    total_pdfs_attempted = 0
    parse_errors: list[tuple[str, str, str]] = []  # (set, num, error)
    empty_garbled: list[tuple[str, str, str]] = []
    anomalies: list[dict] = []
    record_count = 0
    xref_count = 0
    by_set: dict[str, int] = {}

    def emit(rec: ParsedRule, anomaly_note: Optional[str] = None):
        nonlocal record_count, xref_count
        d = asdict(rec)
        out_jsonl.write(json.dumps(d, ensure_ascii=False) + '\n')
        record_count += 1
        xref_count += len(rec.cross_references)
        by_set[rec.rule_set_code] = by_set.get(rec.rule_set_code, 0) + 1
        if anomaly_note:
            anomalies.append({
                'set': rec.rule_set_code,
                'num': rec.rule_number,
                'note': anomaly_note,
                'pdf': rec.source_pdf_path,
            })

    cjc_canons_processed: set[str] = set()

    for s in in_scope_inventory:
        set_code = s['set_code']
        for r in s['rules']:
            num = r['num']
            title = r['title']
            rel_pdf = r['pdf']
            pdf_path = resolve_pdf_path(rel_pdf)

            total_pdfs_attempted += 1

            if pdf_path is None or not pdf_path.exists():
                parse_errors.append((set_code, num, f'PDF not found: {rel_pdf}'))
                anomalies.append({'set': set_code, 'num': num,
                                  'note': f'PDF unresolved: {rel_pdf}',
                                  'pdf': None})
                continue

            # ===== Special-case routing =====
            try:
                # CJC: Canon-level entry → unpack to inner rules
                if set_code == 'CJC' and num.startswith('Canon'):
                    canon_num = num.replace('Canon', '').strip()
                    if canon_num in cjc_canons_processed:
                        continue
                    cjc_canons_processed.add(canon_num)
                    inner_records, err = parse_cjc_canon(canon_num, pdf_path)
                    if err and not inner_records:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in inner_records:
                        emit(rec)
                    if err:
                        anomalies.append({'set': set_code, 'num': num,
                                          'note': err, 'pdf': str(pdf_path)})
                    continue

                # APR irregular handles
                if set_code == 'APR' and is_apr_regs(num):
                    recs, err = parse_apr_regs(pdf_path)
                    if err and not recs:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in recs:
                        emit(rec)
                    continue
                if set_code == 'APR' and is_apr_lp_regs(num):
                    recs, err = parse_apr_lp_regs(pdf_path)
                    if err and not recs:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in recs:
                        emit(rec)
                    continue
                if set_code == 'APR' and is_apr_lp_dis_regs(num):
                    recs, err = parse_apr_lp_dis_regs(pdf_path)
                    if err and not recs:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in recs:
                        emit(rec)
                    continue
                if set_code == 'APR' and is_apr_15p(num):
                    recs, err = parse_apr_15p(pdf_path)
                    if err and not recs:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in recs:
                        emit(rec)
                    continue

                # Range expansion (across all sets)
                if is_range_field(num):
                    recs, err = parse_range_entry(set_code, num, title, pdf_path)
                    if err and not recs:
                        parse_errors.append((set_code, num, err))
                        continue
                    for rec in recs:
                        emit(rec, anomaly_note=rec.notes if rec.notes else None)
                    continue

                # Standard rule
                rec, err = parse_standard_rule(set_code, num, title, pdf_path)
                if err and rec is None:
                    parse_errors.append((set_code, num, err))
                    continue
                if rec is None:
                    parse_errors.append((set_code, num, 'parse returned None'))
                    continue
                if err:
                    empty_garbled.append((set_code, num, err))
                    anomalies.append({'set': set_code, 'num': num,
                                      'note': f'extraction quality: {err}',
                                      'pdf': str(pdf_path)})
                emit(rec)
            except Exception as e:
                parse_errors.append((set_code, num, f'EXCEPTION: {type(e).__name__}: {e}'))

    out_jsonl.close()

    # Halt-protocol checks
    if total_pdfs_attempted > 0:
        err_rate = len(parse_errors) / total_pdfs_attempted
        empty_rate = len(empty_garbled) / total_pdfs_attempted
        if err_rate > PARSE_ERROR_HALT_THRESHOLD:
            write_halt(
                f'Parse error rate {err_rate:.2%} exceeds threshold '
                f'{PARSE_ERROR_HALT_THRESHOLD:.2%}',
                [f'{s} {n}: {e[:120]}' for s, n, e in parse_errors[:20]],
            )
        if empty_rate > EMPTY_PDF_HALT_THRESHOLD:
            write_halt(
                f'Empty/garbled PDF rate {empty_rate:.2%} exceeds threshold '
                f'{EMPTY_PDF_HALT_THRESHOLD:.2%}',
                [f'{s} {n}: {e[:120]}' for s, n, e in empty_garbled[:20]],
            )

    # Write anomaly inventory
    ANOMALIES_PATH.write_text(json.dumps({
        'parse_errors': [{'set': s, 'num': n, 'error': e} for s, n, e in parse_errors],
        'empty_or_garbled': [{'set': s, 'num': n, 'error': e} for s, n, e in empty_garbled],
        'anomalies': anomalies,
    }, indent=2))

    # Write cumulative report
    write_report(record_count, xref_count, total_pdfs_attempted,
                 parse_errors, empty_garbled, anomalies, by_set)


def write_report(record_count: int, xref_count: int, total_pdfs: int,
                 parse_errors: list, empty_garbled: list, anomalies: list,
                 by_set: dict[str, int]):
    """Write phase3_cumulative_report.md with summary + spot-check samples."""
    # Pull spot-check rules for sampling
    spot_check_targets = [
        ('GR', '31'),
        ('RPC', '1.1'),
        ('CR', '12'),
        ('ER', '401'),
        ('RAP', '18.13A'),
        ('CJC', '2.11'),
        ('ELC', '1.1'),
        ('GALR', '1'),
        ('APR', '15P'),
        ('CRLJ', '30'),  # range-via CRLJ 27-37
    ]
    spot_check_samples = []
    if OUTPUT_JSONL.exists():
        for line in OUTPUT_JSONL.read_text().splitlines():
            if not line.strip():
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            for sc, sn in spot_check_targets:
                if d.get('rule_set_code') == sc and d.get('rule_number') == sn:
                    spot_check_samples.append({
                        'citation': d.get('canonical_citation'),
                        'title': d.get('rule_title'),
                        'is_reserved': d.get('is_reserved'),
                        'is_repealed': d.get('is_repealed'),
                        'has_comment': d.get('has_comment'),
                        'effective_date': d.get('effective_date'),
                        'last_amended_date': d.get('last_amended_date'),
                        'parent_canon': d.get('parent_canon'),
                        'parent_regulation_handle': d.get('parent_regulation_handle'),
                        'range_canonical_citation': d.get('range_canonical_citation'),
                        'history_count': len(d.get('history') or []),
                        'cross_ref_count': len(d.get('cross_references') or []),
                        'body_chars': len(d.get('full_text') or ''),
                    })
                    break

    lines = []
    lines.append('# Phase 3 — Parser Cumulative Report')
    lines.append('')
    lines.append(f'**Run time:** {datetime.now().isoformat()}')
    lines.append('')
    lines.append('## 1. Summary')
    lines.append('')
    lines.append(f'- Total PDFs attempted: {total_pdfs}')
    lines.append(f'- Total parsed records emitted: {record_count}')
    lines.append(f'- Total cross-references extracted: {xref_count}')
    lines.append(f'- Parse errors (PDF unresolvable / extraction failed): {len(parse_errors)}')
    lines.append(f'- Empty/garbled PDFs (still emitted, flagged): {len(empty_garbled)}')
    lines.append(f'- Anomaly entries logged: {len(anomalies)}')
    lines.append('')
    lines.append('## 2. Records by Rule Set')
    lines.append('')
    lines.append('| Set | Records emitted |')
    lines.append('|---|---|')
    for set_code in IN_SCOPE_SETS:
        lines.append(f'| {set_code} | {by_set.get(set_code, 0)} |')
    lines.append(f'| **TOTAL** | **{record_count}** |')
    lines.append('')
    # Status breakdown
    if OUTPUT_JSONL.exists():
        all_recs = [json.loads(l) for l in OUTPUT_JSONL.read_text().splitlines() if l.strip()]
        n_reserved = sum(1 for r in all_recs if r['is_reserved'])
        n_repealed = sum(1 for r in all_recs if r['is_repealed'])
        n_comment = sum(1 for r in all_recs if r['has_comment'])
        n_canon = sum(1 for r in all_recs if r.get('parent_canon'))
        n_handle = sum(1 for r in all_recs if r.get('parent_regulation_handle'))
        n_range_canon = sum(1 for r in all_recs if r.get('range_start'))
        n_range_child = sum(1 for r in all_recs if r.get('range_canonical_citation'))
        lines.append('## 3. Status & special-class breakdown')
        lines.append('')
        lines.append(f'- is_reserved: {n_reserved}')
        lines.append(f'- is_repealed: {n_repealed}')
        lines.append(f'- has_comment: {n_comment}')
        lines.append(f'- with parent_canon (CJC inner rules): {n_canon}')
        lines.append(f'- with parent_regulation_handle (APR irregular IDs): {n_handle}')
        lines.append(f'- range canonical rows: {n_range_canon}')
        lines.append(f'- range expanded children: {n_range_child}')
        lines.append('')
        lines.append('## 4. Cross-reference observations')
        lines.append('')
        lines.append('- Self-citations are present: each PDF starts with its own citation '
                     'as a heading line (e.g., "RPC 1.1" at top of RPC 1.1 PDF), so the '
                     'cross-reference extractor captures `target_citation = canonical_citation` '
                     'for nearly every rule. Phase 4 ingest can filter these by comparing '
                     '`target_set_code + target_rule_number` to the source rule. They are '
                     'preserved in JSONL for now to keep the parser deterministic.')
        lines.append('- Resolution order matches phase1_regex_test.py: CJC → APR_HANDLE → main. '
                     'All three patterns are run independently to capture overlapping or '
                     'multiple matches in a single body (parse_cite() only returns the first).')
        lines.append('')
    lines.append('## 5. Disposition')
    lines.append('')
    err_rate = len(parse_errors) / max(1, total_pdfs)
    if err_rate > PARSE_ERROR_HALT_THRESHOLD:
        lines.append(f'**HALT** — parse error rate {err_rate:.2%} exceeds 2% threshold.')
    elif len(parse_errors) > 0:
        lines.append(f'**Ready for Phase 4 ingest** with caveat: {len(parse_errors)} '
                     f'parse errors logged in phase3_anomalies.json '
                     f'(error rate {err_rate:.2%}, below 2% halt threshold).')
    else:
        lines.append('**Ready for Phase 4 ingest** — zero parse errors, zero halt triggers.')
    lines.append('')
    lines.append('## 6. Spot-check samples')
    lines.append('')
    for sample in spot_check_samples:
        lines.append(f'### {sample["citation"]} — {sample["title"]}')
        lines.append('')
        lines.append(f'- is_reserved={sample["is_reserved"]}, is_repealed={sample["is_repealed"]}, has_comment={sample["has_comment"]}')
        lines.append(f'- effective_date={sample["effective_date"]}, last_amended_date={sample["last_amended_date"]}')
        if sample['parent_canon']:
            lines.append(f'- parent_canon={sample["parent_canon"]}')
        if sample['parent_regulation_handle']:
            lines.append(f'- parent_regulation_handle={sample["parent_regulation_handle"]}')
        if sample['range_canonical_citation']:
            lines.append(f'- range_canonical_citation={sample["range_canonical_citation"]}')
        lines.append(f'- history_count={sample["history_count"]}, cross_ref_count={sample["cross_ref_count"]}, body_chars={sample["body_chars"]}')
        lines.append('')

    lines.append('## 7. Anomaly inventory (first 50)')
    lines.append('')
    if not anomalies:
        lines.append('_(none logged)_')
    else:
        for a in anomalies[:50]:
            pdf = a.get('pdf') or ''
            lines.append(f'- **{a["set"]} {a["num"]}** — {a["note"]}'
                         + (f' (`{Path(pdf).name}`)' if pdf else ''))
        if len(anomalies) > 50:
            lines.append(f'\n_(...and {len(anomalies) - 50} more — see phase3_anomalies.json)_')
    lines.append('')

    if parse_errors:
        lines.append('## 8. Parse errors (first 30)')
        lines.append('')
        for s, n, e in parse_errors[:30]:
            lines.append(f'- **{s} {n}** — {e}')
        if len(parse_errors) > 30:
            lines.append(f'\n_(...and {len(parse_errors) - 30} more — see phase3_anomalies.json)_')
        lines.append('')

    REPORT_PATH.write_text('\n'.join(lines))
    print(f'wrote {REPORT_PATH}')
    print(f'wrote {OUTPUT_JSONL} ({record_count} records)')
    print(f'wrote {ANOMALIES_PATH}')
    print(f'cross-references: {xref_count}')


if __name__ == '__main__':
    main()
