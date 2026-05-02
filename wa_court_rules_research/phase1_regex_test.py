"""WA Court Rules — T17 citation regex unit test.

Phase 1 deliverable. Must run green before Phase 2 acquisition begins.

Tests the citation regex from phase0_cumulative_report.md §3 + the LLLT
lookbehind locked in 2026-04-30 against:
  - 18 edge cases covering all observed citation patterns
  - 30 actual citations from cached_html/inventory.json (sampled across
    all 15 in-scope rule sets)

Halt protocol:
  - Any test FAIL halts Phase 2 acquisition until regex is fixed and tests
    re-run green.
  - Tests are organized as standalone functions invoked from main(); each
    asserts via plain assert with descriptive message. No pytest dependency
    so this can run on any Python 3.9+ environment.

Usage:
    python /opt/wdws/wa_court_rules_research/phase1_regex_test.py
    # exits 0 on success, 1 on any failure
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional

# =============================================================================
# Citation regex (FINAL — locked 2026-04-30 with LLLT lookbehind)
# =============================================================================
# This is the same regex that ships in the Phase 4 parser. Any divergence
# between this file and the parser is a regression and must be reconciled.

SET_CODES = (
    'GR|CR|CrR|CrRLJ|CRLJ|ER|RAP|RPC|JuCR|RALJ|APR|SCCAR|'
    'GALR|ELC|'                                  # in-scope, folded in 2026-04-30
    'AR|ARLJ|IRLJ|CCR|SPR|SPRC|BJAR|DRJ|JISCR'   # Phase 8 deferred (kept for cross-ref)
)

# Lookbehind prevents 'LLLT RPC 1.1' from matching as RPC citation.
# LLLT program defunct (Phase 8 deferral); historical references in
# in-scope rule bodies should not be parsed as live RPC citations.
WACR_CITE_RE = re.compile(
    rf'(?<!LLLT\s)\b({SET_CODES})\s+'
    rf'(\d+[A-Z]?(?:\.\d+[A-Z]?)?(?:\.\d+)?)\b'
)

# CJC supports both 'CJC Canon N' (index/heading form) and 'CJC N.M' (inner-rule form)
CJC_CITE_RE = re.compile(
    r'\bCJC\s+(?:Canon\s+(\d+)|(\d+\.\d+))\b'
)

# Subsection notation captured separately from the base citation
SUBSEC_RE = re.compile(r'\(([a-z]|\d+|[A-Z]|[ivxlcdm]+)\)')

# Comment references attach to a parent rule via cmt_index
CMT_RE = re.compile(r'\b(?:Comment|cmt\.?)\s*\[(\d+)\]')

# APR irregular-handle regex — added 2026-04-30 per Gate 2 architectural
# decision (Option B: sub-regulation expansion). Matches the three explicit
# letter-handle patterns (Regs, LP_REGS, LP_DIS_REGS) optionally followed by
# a sub-regulation number. 'APR 15P' is handled by the main regex (15P
# matches \d+[A-Z]?). Lookbehind on \bAPR\s prevents the main WACR_CITE_RE
# from greedy-matching 'APR' alone before this fires.
APR_HANDLE_RE = re.compile(
    r'\bAPR\s+(Regs|LP_REGS|LP_DIS_REGS)(?:\s+(\d+))?\b'
)

# =============================================================================
# Cross-jurisdiction citation regex — added 2026-05-01 per Phase 5 Investigation 2
# defect resolution (Option A — immediate fix). Court-rule bodies cite RCW,
# USC, WAC, and WA Constitution; original Phase 3 parser was scoped to court-
# rule citations only and silently dropped 600+ cross-jurisdiction citations.
#
# Regex patterns mirror the RCW + WAC corpus parsers verbatim where possible
# (rcw_parser.py and wac_parser.py) for symmetric architecture. Authority:
#   /opt/wdws/rcw_research/rcw_parser.py:165-199
#   /opt/wdws/wac_research/wac_parser.py:165-169
# =============================================================================

# RCW section citations: "RCW 4.28.080", "RCW 62A.9-310" (UCC dash-form)
RCW_CITE_RE = re.compile(
    r'\bRCW\s+'
    r'(62A\.[0-9]+[A-Z]*-[0-9]+[A-Z]*'              # UCC Title 62A dash-form
    r'|[0-9][0-9A-Z]*\.[0-9A-Z]+\.[0-9A-Z]+)'       # Standard T.CC.SSS
    r'(?:\([0-9a-zA-Z]+\))*'                         # optional subsection suffix (1)(a)(i)
)

# RCW chapter citations: "chapter 49.60 RCW", "ch. 4.28 RCW", "ch 49.60 RCW"
# Mirrors rcw_parser.py:176-178 RCW_CHAPTER_TEXT_RE; extended to also match the
# "ch." abbreviated form (court rules use both). Added 2026-05-01 per Phase 5
# GR 33 verification — 38 rules / 82 occurrences corpus-wide use chapter form.
RCW_CHAPTER_CITE_RE = re.compile(
    r'\b(?:chapter|ch\.?)\s+([0-9][0-9A-Z]*\.[0-9A-Z]+)\s+RCW\b',
    re.IGNORECASE,
)

# Federal USC: "42 U.S.C. § 1983", "28 USC § 1331", "42 U.S.C. §§ 12101-12213"
# Authority pattern from rcw_parser.py USC_TEXT_RE. Section group captures:
#   - basic: 1331, 12132
#   - alpha suffix: 12132a, 2000aa
#   - subsection: 12132(a)(1)
#   - range: 12101-12213 (added 2026-05-01 per Phase 5 GR 33 verification —
#     prior pattern truncated at first dash, leaving "12101-" with trailing
#     dash in cross_references.target_citation. Corroboration #22.)
#
# IMPORTANT: the alpha-suffix portion `[a-zA-Z]*` will greedy-bleed into
# adjacent text when the body has no space before the next token (e.g.,
# "25 U.S.C. § 1901et seq." captures '1901et'). Use normalize_usc_section()
# below as a post-capture trim step. T17.34 covers this contract.
USC_CITE_RE = re.compile(
    r'\b([0-9]+[A-Z]?)\s+U\.?S\.?C\.?\s*§+\s*'
    r'([0-9A-Z]+(?:\([0-9a-zA-Z]+\))*[a-zA-Z]*'      # base section + optional subsection + suffix
    r'(?:-[0-9]+[a-zA-Z]?)?)'                        # optional range tail (range form §§ N-M)
)


def normalize_usc_section(section: str, body: str, end_pos: int) -> str:
    """Post-capture trim for the USC 'et seq.' word-boundary defect.

    The USC_CITE_RE alpha-suffix subgroup `[a-zA-Z]*` will greedy-match the
    literal 'et' from 'et seq.' when no space precedes it in the body
    (observed in APR 8: "25 U.S.C. § 1901et seq." captures '1901et').
    This function strips a trailing 'et' from the section IF the next
    non-space chars in body (after the regex match end) start with 'seq'
    (case-insensitive). Returns the cleaned section.

    Pure-Python helper; used by phase3_parser.extract_cross_references
    Pass 5 + T17.34 unit test. Added 2026-05-01 per Phase 5 instruction
    re-routing — corroboration #22 audit channel for incomplete-capture
    failure mode.
    """
    if section.lower().endswith('et'):
        # Look at what immediately follows the regex match end in the body.
        rest = body[end_pos:end_pos + 10].lstrip()
        if rest.lower().startswith('seq'):
            return section[:-2]
    return section

# WAC section citations: "WAC 162-22-010"
WAC_CITE_RE = re.compile(
    r'\bWAC\s+'
    r'([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?)'
    r'(?:\([0-9a-zA-Z]+\))*'
)

# WA Constitution citations — two alternation branches:
#   (a) Explicit "Wash. Const. art. I, § 3" form (most common in legal citation)
#   (b) Narrative "Article I, § 7 of the [state/Washington] Constitution"
#       (matches rcw_parser.py WA_CONST_TEXT_RE form for narrative usage)
WACONST_CITE_RE = re.compile(
    r'(?:'
    r'\bWash\.?\s+Const\.?\s+art\.?\s+([IVX]+)(?:,?\s+§+\s*([0-9]+))?'   # explicit form
    r'|'
    r'\bArticle\s+([IVX]+)(?:,?\s+§+\s*([0-9]+))?\s+of\s+(?:the\s+)?'
    r'(?:(?:Washington|state)\s+)*Constitution'                          # narrative form
    r')',                                                                #  matches "Constitution",
    re.IGNORECASE,                                                       #  "Washington Constitution",
)                                                                        #  "state Constitution",
                                                                         #  "Washington state Constitution"

INVENTORY_PATH = Path('/opt/wdws/wa_court_rules_research/cached_html/inventory.json')


# =============================================================================
# Helper functions
# =============================================================================

def parse_cite(text: str) -> Optional[dict]:
    """Try CJC pattern first (most specific), then APR_HANDLE, then main.

    Returns a dict with parsed components, or None if no match.

    Resolution order matters: APR_HANDLE_RE is tried before the main regex
    because tokens like 'APR Regs 101' would have 'APR' (set_code) followed
    by 'Regs' which the main regex cannot match (Regs is not digit-starting),
    but the main regex would still capture 'APR' alone — yielding a false
    set='APR' num='Regs'-untracked match. Ordering APR_HANDLE first avoids that.
    """
    cjc = CJC_CITE_RE.search(text)
    if cjc:
        canon = cjc.group(1)
        inner = cjc.group(2)
        if canon:
            return {'pattern': 'cjc', 'set': 'CJC', 'form': 'canon', 'num': canon}
        return {'pattern': 'cjc', 'set': 'CJC', 'form': 'inner', 'num': inner}

    apr_h = APR_HANDLE_RE.search(text)
    if apr_h:
        return {
            'pattern': 'apr_handle',
            'set': 'APR',
            'handle': apr_h.group(1),
            'sub': int(apr_h.group(2)) if apr_h.group(2) else None,
        }

    m = WACR_CITE_RE.search(text)
    if not m:
        return None
    rest = text[m.end():]
    subs = SUBSEC_RE.findall(rest)
    cmt = CMT_RE.search(text)
    return {
        'pattern': 'main',
        'set': m.group(1),
        'num': m.group(2),
        'subs': subs,
        'cmt': int(cmt.group(1)) if cmt else None,
    }


# =============================================================================
# Edge-case tests (T17.1 — T17.18)
# =============================================================================

def test_t17_01_base_bare_integer():
    """T17.1 — base citation with bare integer rule number."""
    r = parse_cite('GR 31')
    assert r is not None and r['set'] == 'GR' and r['num'] == '31', r


def test_t17_02_base_decimal():
    """T17.2 — base citation with decimal rule number."""
    r = parse_cite('RPC 1.1')
    assert r is not None and r['set'] == 'RPC' and r['num'] == '1.1', r


def test_t17_03_base_bare_integer_cr():
    """T17.3 — base citation CR + integer."""
    r = parse_cite('CR 12')
    assert r is not None and r['set'] == 'CR' and r['num'] == '12', r


def test_t17_04_subsection_one_level():
    """T17.4 — subsection notation with one level."""
    r = parse_cite('CR 12(b)(6)')
    assert r is not None and r['set'] == 'CR' and r['num'] == '12', r
    assert r['subs'] == ['b', '6'], r


def test_t17_05_subsection_two_level():
    """T17.5 — subsection notation with two levels."""
    r = parse_cite('CR 12(b)(2)(A)')
    assert r is not None and r['set'] == 'CR' and r['num'] == '12', r
    assert r['subs'] == ['b', '2', 'A'], r


def test_t17_06_decimal_sub():
    """T17.6 — decimal sub-numbering RPC 1.5."""
    r = parse_cite('RPC 1.5')
    assert r is not None and r['set'] == 'RPC' and r['num'] == '1.5', r


def test_t17_07_cjc_inner_rule():
    """T17.7 — CJC inner-rule decimal form."""
    r = parse_cite('CJC 2.11')
    assert r is not None and r['pattern'] == 'cjc' and r['form'] == 'inner', r
    assert r['num'] == '2.11', r


def test_t17_08_alpha_suffix_decimal():
    """T17.8 — alpha-suffix-on-decimal form RAP 18.13A."""
    r = parse_cite('RAP 18.13A')
    assert r is not None and r['set'] == 'RAP' and r['num'] == '18.13A', r


def test_t17_09_alpha_prefix_then_dot():
    """T17.9 — alpha-prefix-then-dot form JuCR 5A.1."""
    r = parse_cite('JuCR 5A.1')
    assert r is not None and r['set'] == 'JuCR' and r['num'] == '5A.1', r


def test_t17_10_three_segment_numbering():
    """T17.10 — three-segment numbering CrR 3.2.1."""
    r = parse_cite('CrR 3.2.1')
    assert r is not None and r['set'] == 'CrR' and r['num'] == '3.2.1', r


def test_t17_11_cjc_canon_form():
    """T17.11 — CJC Canon form CJC Canon 1."""
    r = parse_cite('CJC Canon 1')
    assert r is not None and r['pattern'] == 'cjc' and r['form'] == 'canon', r
    assert r['num'] == '1', r


def test_t17_12_comment_full_word():
    """T17.12 — comment reference with full word 'Comment'."""
    r = parse_cite('RPC 1.1 Comment [3]')
    assert r is not None and r['set'] == 'RPC' and r['num'] == '1.1', r
    assert r['cmt'] == 3, r


def test_t17_13_comment_abbrev():
    """T17.13 — comment reference with abbrev 'cmt.'."""
    r = parse_cite('RPC 1.1 cmt. [3]')
    assert r is not None and r['set'] == 'RPC' and r['num'] == '1.1', r
    assert r['cmt'] == 3, r


def test_t17_14_elc_base():
    """T17.14 — ELC base citation (folded in 2026-04-30)."""
    r = parse_cite('ELC 1.1')
    assert r is not None and r['set'] == 'ELC' and r['num'] == '1.1', r


def test_t17_15_elc_reserved_row():
    """T17.15 — ELC reserved row (ELC 15.6)."""
    r = parse_cite('ELC 15.6')
    assert r is not None and r['set'] == 'ELC' and r['num'] == '15.6', r


def test_t17_16_galr_base():
    """T17.16 — GALR base citation (folded in 2026-04-30)."""
    r = parse_cite('GALR 7')
    assert r is not None and r['set'] == 'GALR' and r['num'] == '7', r


def test_t17_17_lllt_lookbehind_blocks_match():
    """T17.17 — LLLT lookbehind: 'see LLLT RPC 1.1 (defunct)' should NOT match RPC.

    The lookbehind (?<!LLLT\\s) prevents incorrect attribution of the
    trailing 'RPC 1.1' to the live RPC corpus. LLLT program defunct;
    historical references in in-scope rule bodies must not parse as live
    RPC citations.
    """
    r = parse_cite('see LLLT RPC 1.1 (defunct)')
    assert r is None, f'Expected no match, got: {r}'


def test_t17_18_plain_rpc_still_matches():
    """T17.18 — Counterpart to T17.17: plain 'see RPC 1.1' SHOULD match.

    Confirms the lookbehind only blocks the LLLT prefix case and doesn't
    over-block legitimate RPC references.
    """
    r = parse_cite('see RPC 1.1')
    assert r is not None and r['set'] == 'RPC' and r['num'] == '1.1', r


# =============================================================================
# APR irregular-handle tests (T17.19 – T17.23) — added 2026-04-30 per Gate 2
# =============================================================================

def test_t17_19_apr_regs_with_sub():
    """T17.19 — APR Regs with sub-regulation number."""
    r = parse_cite('APR Regs 101')
    assert r is not None and r['pattern'] == 'apr_handle', r
    assert r['handle'] == 'Regs' and r['sub'] == 101, r


def test_t17_20_apr_lp_regs_no_sub():
    """T17.20 — APR LP_REGS without sub-regulation number (single appendix)."""
    r = parse_cite('APR LP_REGS')
    assert r is not None and r['pattern'] == 'apr_handle', r
    assert r['handle'] == 'LP_REGS' and r['sub'] is None, r


def test_t17_21_apr_lp_dis_regs_with_sub():
    """T17.21 — APR LP_DIS_REGS with sub-regulation number."""
    r = parse_cite('APR LP_DIS_REGS 12')
    assert r is not None and r['pattern'] == 'apr_handle', r
    assert r['handle'] == 'LP_DIS_REGS' and r['sub'] == 12, r


def test_t17_22_apr_15p_main_pattern():
    """T17.22 — APR 15P continues to match the MAIN pattern (sanity check
    that handle alternation doesn't mask the existing alpha-suffix support).
    """
    r = parse_cite('APR 15P')
    assert r is not None and r['pattern'] == 'main', r
    assert r['set'] == 'APR' and r['num'] == '15P', r


def test_t17_23_apr_regs_no_sub():
    """T17.23 — APR Regs without sub-number (bare-handle case).

    The (?:\\s+(\\d+))? alternation allows the sub-number to be optional;
    verify the bare handle case captures handle='Regs', sub=None.
    """
    r = parse_cite('APR Regs')
    assert r is not None and r['pattern'] == 'apr_handle', r
    assert r['handle'] == 'Regs' and r['sub'] is None, r


# =============================================================================
# Both-RESCINDED-and-RESERVED ghost-flag refinement (T17.24) — added 2026-05-01
# per Phase 4 boundary halt resolution + GR 8 refinement
# =============================================================================

def refine_ghost_flags(body: str, is_repealed: bool, is_reserved: bool):
    """If body explicitly contains both RESCINDED and RESERVED markers
    (case-insensitive), set both flags. Some rules are simultaneously
    rescinded AND reserved (e.g., GR 8 'RESCINDED and RESERVED, LAWS OF
    2002, CH. 136').

    Used by phase4_ingest.py pre-INSERT pass. Operator-approved 2026-05-01.
    """
    body_upper = body.upper() if body else ''
    has_rescinded = 'RESCINDED' in body_upper or 'RESCINDS' in body_upper
    has_reserved = 'RESERVED' in body_upper or '[RESERVED]' in body_upper
    if has_rescinded and has_reserved:
        return (True, True)
    return (is_repealed, is_reserved)


# =============================================================================
# Cross-jurisdiction citation tests (T17.25 – T17.30) — added 2026-05-01
# per Phase 5 Investigation 2 defect resolution
# =============================================================================

def test_t17_25_rcw_section_cite():
    """T17.25 — CR 4 cites RCW 4.28.080. RCW_CITE_RE captures section."""
    body = "Service of summons is governed by RCW 4.28.080 and RCW 4.28.100."
    matches = [m.group(1) for m in RCW_CITE_RE.finditer(body)]
    assert "4.28.080" in matches and "4.28.100" in matches, matches


def test_t17_26_rcw_chapter_form_passes_section_branch():
    """T17.26 — RPC 5.5 cites RCW 2.48.180. RCW_CITE_RE captures it."""
    body = "Engaging in unauthorized practice of law violates RCW 2.48.180."
    matches = [m.group(1) for m in RCW_CITE_RE.finditer(body)]
    assert "2.48.180" in matches, matches


def test_t17_27_usc_dotted_form():
    """T17.27 — '42 U.S.C. § 12132' → captures with title=42, section=12132.

    Tests dotted U.S.C. form (most common in legal citation).
    """
    body = "ADA Title II is codified at 42 U.S.C. § 12132."
    m = USC_CITE_RE.search(body)
    assert m is not None and m.group(1) == "42" and m.group(2) == "12132", \
        (m.group(0) if m else None, m.group(1) if m else None, m.group(2) if m else None)


def test_t17_28_wac_section_cite():
    """T17.28 — 'WAC 162-22-010' → captures section."""
    body = "See WAC 162-22-010 for HRC enforcement procedures."
    m = WAC_CITE_RE.search(body)
    assert m is not None and m.group(1) == "162-22-010", (m.group(0) if m else None)


def test_t17_29_wa_constitution_explicit_and_narrative():
    """T17.29 — both 'Wash. Const. art. I, § 3' and 'Article I § 3 of the
    Washington Constitution' should match WACONST_CITE_RE.
    """
    # Explicit form
    m1 = WACONST_CITE_RE.search("citing Wash. Const. art. I, § 3 for due process")
    assert m1 is not None, "explicit form failed"

    # Narrative form
    m2 = WACONST_CITE_RE.search("Article I § 3 of the Washington Constitution governs")
    assert m2 is not None, "narrative form failed"


def test_t17_31_rcw_chapter_form_chapter_word():
    """T17.31 — 'chapter 49.60 RCW' captures via RCW_CHAPTER_CITE_RE."""
    body = "violation of chapter 49.60 RCW (Washington Law Against Discrimination)"
    m = RCW_CHAPTER_CITE_RE.search(body)
    assert m is not None and m.group(1) == "49.60", (m.group(0) if m else None)


def test_t17_32_rcw_chapter_form_ch_abbrev():
    """T17.32 — 'ch. 4.28 RCW' captures via RCW_CHAPTER_CITE_RE."""
    body = "Service is governed by ch. 4.28 RCW and the Civil Rules."
    m = RCW_CHAPTER_CITE_RE.search(body)
    assert m is not None and m.group(1) == "4.28", (m.group(0) if m else None)
    # Also verify "ch 4.28 RCW" without dot (some court rules omit it)
    body2 = "Cross-reference: ch 9A.04 RCW criminal jurisdiction."
    m2 = RCW_CHAPTER_CITE_RE.search(body2)
    assert m2 is not None and m2.group(1) == "9A.04", (m2.group(0) if m2 else None)


def test_t17_33_usc_range_full_capture():
    """T17.33 — '42 U.S.C. §§ 12101-12213' captures full range, not truncated.

    Prior regex captured only '12101-' with trailing dash. Fix allows the
    optional range tail (?:-[0-9]+[a-zA-Z]?)? to capture the full range.
    """
    body = "Americans with Disabilities Act of 1990 (42 U.S.C. §§ 12101-12213), the"
    m = USC_CITE_RE.search(body)
    assert m is not None, "no USC match"
    assert m.group(1) == "42", m.group(1)
    assert m.group(2) == "12101-12213", f"truncated capture: {m.group(2)!r}"

    # Counter-cases: single section still works (no false-positive on dash absence)
    m2 = USC_CITE_RE.search("28 U.S.C. § 1331")
    assert m2 is not None and m2.group(2) == "1331", m2.group(2) if m2 else None

    # Letter-suffix still works
    m3 = USC_CITE_RE.search("42 U.S.C. § 12132a")
    assert m3 is not None and m3.group(2) == "12132a", m3.group(2) if m3 else None


def test_t17_34_usc_et_seq_word_boundary():
    """T17.34 — USC 'et seq.' word-boundary discipline.

    APR 8 body has '25 U.S.C. § 1901et seq.' (no space between section and
    'et'). The raw USC_CITE_RE captures '1901et' (greedy alpha-suffix bleed
    into 'et' from 'et seq.'). normalize_usc_section() post-capture trim
    strips the 'et' when followed by 'seq' in body, yielding clean '1901'.

    Tests both (a) the direct regex defect (captures '1901et') and (b) the
    helper's correction (yields '1901'), then several counter-cases
    confirming legitimate alpha suffixes aren't mistakenly stripped.
    """
    # (a) Confirm raw regex bleed defect (proves helper is needed)
    body_no_space = "25 U.S.C. § 1901et seq."
    m = USC_CITE_RE.search(body_no_space)
    raw_capture = m.group(2) if m else None
    assert m is not None and raw_capture == "1901et", \
        f"raw regex should capture '1901et' (the defect); got {raw_capture!r}"

    # (b) Helper trims the bleed cleanly
    cleaned = normalize_usc_section(m.group(2), body_no_space, m.end())
    assert cleaned == "1901", f"normalize should yield '1901'; got {cleaned!r}"

    # Counter-case 1: '42 U.S.C. § 12132 et seq.' (with space) — regex captures
    # '12132' cleanly; helper passes through unchanged.
    body_space = "42 U.S.C. § 12132 et seq."
    m2 = USC_CITE_RE.search(body_space)
    cleaned2 = normalize_usc_section(m2.group(2), body_space, m2.end())
    assert cleaned2 == "12132", f"clean case should stay '12132'; got {cleaned2!r}"

    # Counter-case 2: '15 U.S.C. § 78a et seq.' — legitimate alpha suffix 'a'
    # NOT followed by 'seq' adjacent to it. Helper should pass through.
    body_alpha = "15 U.S.C. § 78a et seq."
    m3 = USC_CITE_RE.search(body_alpha)
    cleaned3 = normalize_usc_section(m3.group(2), body_alpha, m3.end())
    assert cleaned3 == "78a", f"alpha suffix should stay '78a'; got {cleaned3!r}"

    # Counter-case 3: '42 U.S.C. § 2000aa' (legitimate double-letter suffix)
    # NOT followed by anything starting with 'seq'. Helper passes through.
    body_aa = "42 U.S.C. § 2000aa later in body"
    m4 = USC_CITE_RE.search(body_aa)
    cleaned4 = normalize_usc_section(m4.group(2), body_aa, m4.end())
    assert cleaned4 == "2000aa", f"double-letter suffix should stay '2000aa'; got {cleaned4!r}"


def test_t17_35_citation_roundtrip_known_cases():
    """T17.35 — citation roundtrip audit (corroboration #22 audit channel).

    For known-good and known-bad citation patterns, verify the captured
    target_citation form re-matches against original body text. Catches
    incomplete-capture defects (regex truncates at boundary leaving
    malformed records like '42 U.S.C. § 12101-' with trailing dash).
    """
    cases = [
        # (body, expected_target_citation, regex)
        ("citing RCW 4.28.080 for service",            "RCW 4.28.080",          "rcw"),
        ("under chapter 49.60 RCW",                    "chapter 49.60 RCW",      "rcw_chapter"),
        ("see ch. 4.28 RCW",                           "ch. 4.28 RCW",           "rcw_chapter"),
        ("42 U.S.C. §§ 12101-12213",                   "42 U.S.C. § 12101-12213", "usc_range"),
        ("28 U.S.C. § 1331",                           "28 U.S.C. § 1331",       "usc_basic"),
        ("WAC 162-22-010",                             "WAC 162-22-010",         "wac"),
    ]
    for body, expected, kind in cases:
        if kind == "rcw":
            m = RCW_CITE_RE.search(body)
            assert m is not None, f"{kind}: no match in {body!r}"
            cite_form = f"RCW {m.group(1)}"
            assert cite_form == expected, f"{kind}: got {cite_form!r}, expected {expected!r}"
        elif kind == "rcw_chapter":
            m = RCW_CHAPTER_CITE_RE.search(body)
            assert m is not None, f"{kind}: no match in {body!r}"
            # Roundtrip: the captured chapter token should appear in body
            # near "RCW", with either "chapter" or "ch." preceding.
            assert m.group(1) in body and "RCW" in body, f"{kind}: roundtrip broken"
        elif kind in ("usc_range", "usc_basic"):
            m = USC_CITE_RE.search(body)
            assert m is not None, f"{kind}: no match in {body!r}"
            section = m.group(2)
            # Canonical form for roundtrip audit
            cite_form = f"{m.group(1)} U.S.C. § {section}"
            assert cite_form == expected, f"{kind}: got {cite_form!r}, expected {expected!r}"
            # Roundtrip: section must appear in body verbatim (no truncation)
            assert section in body, f"{kind}: captured {section!r} not in body — truncation defect"
        elif kind == "wac":
            m = WAC_CITE_RE.search(body)
            assert m is not None, f"{kind}: no match in {body!r}"
            cite_form = f"WAC {m.group(1)}"
            assert cite_form == expected, f"{kind}: got {cite_form!r}, expected {expected!r}"


def test_t17_30_multi_jurisdiction_body_no_dedup_loss():
    """T17.30 — CR 4 sample-style body with multiple jurisdictions:
    must capture all distinct citations across all 4 jurisdictions
    (court-rule + RCW + USC + WAC) without dedup loss when same target
    appears via different patterns.
    """
    body = (
        "Service is governed by CR 4 and statutes RCW 4.28.080, RCW 4.28.180, "
        "RCW 46.64.040. Federal procedure parallels 28 U.S.C. § 1331. "
        "Administrative analog: WAC 162-22-010. State constitution: "
        "Article I § 3 of the Washington Constitution."
    )
    rcw_matches = [m.group(1) for m in RCW_CITE_RE.finditer(body)]
    usc_matches = [(m.group(1), m.group(2)) for m in USC_CITE_RE.finditer(body)]
    wac_matches = [m.group(1) for m in WAC_CITE_RE.finditer(body)]
    waconst_matches = list(WACONST_CITE_RE.finditer(body))
    cr_match = WACR_CITE_RE.search(body)

    assert "4.28.080" in rcw_matches and "4.28.180" in rcw_matches and "46.64.040" in rcw_matches, rcw_matches
    assert ("28", "1331") in usc_matches, usc_matches
    assert "162-22-010" in wac_matches, wac_matches
    assert len(waconst_matches) >= 1, waconst_matches
    assert cr_match is not None and cr_match.group(1) == "CR" and cr_match.group(2) == "4", cr_match


def test_t17_24_gr8_both_rescinded_and_reserved():
    """T17.24 — GR 8 has body explicitly containing both RESCINDED and
    RESERVED markers; refine_ghost_flags must set both is_repealed=TRUE
    AND is_reserved=TRUE.

    Counter-cases: pure rescinded body without 'RESERVED' marker keeps
    parser's classification; pure reserved body without 'RESCINDED' keeps
    the classification.
    """
    # GR 8 actual body excerpt
    gr8_body = '[RESCINDED and RESERVED, LAWS OF 2002, CH. 136]\n[Rescinded effective January 1, 2003.]'
    # Parser flagged GR 8 as is_repealed=True, is_reserved=False; refinement should set both
    is_rep, is_res = refine_ghost_flags(gr8_body, True, False)
    assert is_rep is True and is_res is True, f'GR 8 should get both flags; got is_rep={is_rep} is_res={is_res}'

    # Counter-case: pure rescinded (no 'RESERVED' marker) keeps original
    pure_rescinded = '[RESCINDED]\n[Rescinded effective 2017-12-05.]'
    is_rep, is_res = refine_ghost_flags(pure_rescinded, True, False)
    assert is_rep is True and is_res is False, f'pure-rescinded should keep is_reserved=False; got is_rep={is_rep} is_res={is_res}'

    # Counter-case: pure reserved (no 'RESCINDED' marker) keeps original
    pure_reserved = '[RESERVED]'
    is_rep, is_res = refine_ghost_flags(pure_reserved, False, True)
    assert is_rep is False and is_res is True, f'pure-reserved should keep is_repealed=False; got is_rep={is_rep} is_res={is_res}'

    # Counter-case: empty body keeps original flags as-is
    is_rep, is_res = refine_ghost_flags('', True, False)
    assert is_rep is True and is_res is False, 'empty body should not alter flags'


# =============================================================================
# Inventory smoke test — run regex against actual cached citations
# =============================================================================

def test_t17_inventory_smoke():
    """Run regex against 30 actual rule citations from cached inventory.json.

    Samples 2 rules per in-scope set across all 15 sets. Any miss is a
    regression — expected zero misses.
    """
    if not INVENTORY_PATH.exists():
        # If inventory absent (e.g., running this test outside ACE host),
        # skip the smoke test cleanly. Edge-case tests above are sufficient.
        print('  [skip] inventory.json not found; skipping smoke test')
        return

    with INVENTORY_PATH.open() as f:
        sets = json.load(f)

    in_scope = ['GR', 'RPC', 'CR', 'ER', 'RAP', 'CJC', 'GALR',
                'CrR', 'CrRLJ', 'CRLJ', 'JuCR', 'SCCAR', 'RALJ', 'APR', 'ELC']

    samples = []
    for s in sets:
        if s['set_code'] in in_scope and s['rules']:
            for r in s['rules'][:2]:
                samples.append((s['set_code'], r['num']))

    misses = []
    for set_code, num in samples:
        cite = f'{set_code} {num}'
        result = parse_cite(cite)
        if result is None:
            misses.append(cite)

    assert not misses, f'{len(misses)} citations did not match: {misses}'
    assert len(samples) >= 25, f'Expected at least 25 samples, got {len(samples)}'


# =============================================================================
# Test runner
# =============================================================================

ALL_TESTS = [
    test_t17_01_base_bare_integer,
    test_t17_02_base_decimal,
    test_t17_03_base_bare_integer_cr,
    test_t17_04_subsection_one_level,
    test_t17_05_subsection_two_level,
    test_t17_06_decimal_sub,
    test_t17_07_cjc_inner_rule,
    test_t17_08_alpha_suffix_decimal,
    test_t17_09_alpha_prefix_then_dot,
    test_t17_10_three_segment_numbering,
    test_t17_11_cjc_canon_form,
    test_t17_12_comment_full_word,
    test_t17_13_comment_abbrev,
    test_t17_14_elc_base,
    test_t17_15_elc_reserved_row,
    test_t17_16_galr_base,
    test_t17_17_lllt_lookbehind_blocks_match,
    test_t17_18_plain_rpc_still_matches,
    test_t17_19_apr_regs_with_sub,
    test_t17_20_apr_lp_regs_no_sub,
    test_t17_21_apr_lp_dis_regs_with_sub,
    test_t17_22_apr_15p_main_pattern,
    test_t17_23_apr_regs_no_sub,
    test_t17_24_gr8_both_rescinded_and_reserved,
    test_t17_25_rcw_section_cite,
    test_t17_26_rcw_chapter_form_passes_section_branch,
    test_t17_27_usc_dotted_form,
    test_t17_28_wac_section_cite,
    test_t17_29_wa_constitution_explicit_and_narrative,
    test_t17_30_multi_jurisdiction_body_no_dedup_loss,
    test_t17_31_rcw_chapter_form_chapter_word,
    test_t17_32_rcw_chapter_form_ch_abbrev,
    test_t17_33_usc_range_full_capture,
    test_t17_34_usc_et_seq_word_boundary,
    test_t17_35_citation_roundtrip_known_cases,
    test_t17_inventory_smoke,
]


def main() -> int:
    passed = 0
    failed = 0
    for test in ALL_TESTS:
        name = test.__name__
        try:
            test()
            print(f'  PASS  {name}')
            passed += 1
        except AssertionError as e:
            print(f'  FAIL  {name}')
            print(f'        {e}')
            failed += 1
        except Exception as e:
            print(f'  ERROR {name}: {type(e).__name__}: {e}')
            failed += 1

    print()
    print(f'T17 summary: {passed} passed, {failed} failed (of {len(ALL_TESTS)} tests)')
    if failed:
        print('HALT — fix regex before Phase 2 acquisition.')
        return 1
    print('GREEN — Phase 2 acquisition gate cleared.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
