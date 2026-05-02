"""
Phase 3 unit-test harness for rcw_parser.

Exercises the four anomaly classes from the Phase 2 / Phase 3 inventory plus
the 10 stress-test chapters identified in Phase 2 §"Class 3 — Parser stress
targets". Run with `python3 test_rcw_parser.py` (no test framework dep).

Tests are PASS-noisy and FAIL-loud: each test prints the assertion result and
the harness exits non-zero if any test fails.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from rcw_parser import (
    parse_chapter_html,
    parse_disposition_html,
    count_section_anchors,
    ANCHOR_RE,
    RCW_SECTION_TEXT_RE,
)

DATA_ROOT = Path('/opt/wdws/data/rcw/html')


# ---------------------------------------------------------------------------
# Tiny test framework
# ---------------------------------------------------------------------------

_failures: list[str] = []
_passes  = 0

def assertEqual(label: str, expected, actual):
    global _passes
    if expected == actual:
        print(f'  ✓ {label}: {actual}')
        _passes += 1
    else:
        msg = f'  ✗ {label}: expected {expected!r}, got {actual!r}'
        print(msg)
        _failures.append(msg)

def assertTrue(label: str, condition: bool, detail: str = ''):
    global _passes
    if condition:
        print(f'  ✓ {label}{("  " + detail) if detail else ""}')
        _passes += 1
    else:
        msg = f'  ✗ {label}{("  " + detail) if detail else ""}'
        print(msg)
        _failures.append(msg)

def assertZero(label: str, n: int, examples: list = None):
    assertTrue(label, n == 0, f'(count = {n})' + (f', examples={examples[:3]}' if examples else ''))


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

def test_anchor_parity_simple():
    """RCW 4.04 — single-section chapter, the canonical baseline."""
    print('\n[T1] Anchor-parity on RCW 4.04 (single-section baseline)')
    fp = DATA_ROOT / 'chapters/title_4/chap_4.04_FULL.html'
    rec = parse_chapter_html(fp)
    assertEqual('chapter_number', '4.04', rec.chapter_number)
    assertEqual('title_number', '4', rec.title_number)
    assertEqual('section count', 1, len(rec.sections))
    s = rec.sections[0]
    assertEqual('section.canonical_citation', 'RCW 4.04.010', s.canonical_citation)
    assertEqual('section.section_number', '010', s.section_number)
    assertEqual('section.section_heading', 'Extent to which common law prevails', s.section_heading)
    assertTrue('body has content', len(s.full_text) > 50, f'(len={len(s.full_text)})')
    assertEqual('history entries (5 cites)', 5, len(s.history))


def test_phase0_seven_samples():
    """Phase 0 §2 dissection samples — confirm baseline."""
    print('\n[T2] Phase 0 dissection samples (anchor parity)')
    samples = [
        ('title_4',  '4.04',   1),
        ('title_9A', '9A.04',  11),
        ('title_42', '42.30',  26),
        ('title_42', '42.56',  92),
        ('title_49', '49.60',  66),
        ('title_51', '51.52',  29),
        ('title_11', '11.96A', 37),
    ]
    for tdir, cite, expected in samples:
        fp = DATA_ROOT / 'chapters' / tdir / f'chap_{cite}_FULL.html'
        a = count_section_anchors(fp)
        p = len(parse_chapter_html(fp).sections)
        assertEqual(f'RCW {cite} (anchors={a})', expected, p)


def test_stress_chapters():
    """Phase 2 §"Class 3" — 10 largest chapters by section count.
    Required by Phase 3 authorization: section count must equal anchor count exactly."""
    print('\n[T3] Stress-test chapters (10 largest by section count)')
    samples = [
        ('title_89', '89.30',  270),  # Conservation Districts
        ('title_46', '46.04',  246),  # Motor Vehicles definitions
        ('title_82', '82.04',  227),  # B&O Tax — biggest file (2.1MB)
        ('title_24', '24.03A', 213),  # Nonprofit Corp Act
        ('title_35', '35.21',  205),  # Cities and Towns Misc
        ('title_47', '47.17',  197),  # State Highways
        ('title_87', '87.03',  192),  # Irrigation Districts
        ('title_46', '46.61',  191),  # Rules of the Road
        ('title_82', '82.08',  177),  # Retail Sales Tax
        ('title_43', '43.43',  172),  # State Patrol
    ]
    for tdir, cite, expected in samples:
        fp = DATA_ROOT / 'chapters' / tdir / f'chap_{cite}_FULL.html'
        a = count_section_anchors(fp)
        p = len(parse_chapter_html(fp).sections)
        assertEqual(f'RCW {cite} (anchors={a})', expected, p)


def test_empty_chapters_class1():
    """Phase 2 §"Class 1 — Truly empty chapters" (10 chapters, post-erratum).
    Parser must emit a chapter record with zero sections and no exception."""
    print('\n[T4] Class 1 empty/reserved chapters (10 chapters)')
    samples = [
        ('14',     '14.30'),
        ('18',     '18.09'),
        ('29A',    '29A.76C'),
        ('32',     '32.28'),
        ('32',     '32.30'),
        ('33',     '33.54'),
        ('36',     '36.42'),
        ('44',     '44.07F'),
        ('48',     '48.26'),
        ('76',     '76.10'),
    ]
    for tdir, cite in samples:
        fp = DATA_ROOT / 'chapters' / f'title_{tdir}' / f'chap_{cite}_FULL.html'
        rec = parse_chapter_html(fp)
        assertEqual(f'RCW {cite} sections', 0, len(rec.sections))
        # 48.26 is the only one with the explicit (RESERVED) marker in caption
        if cite == '48.26':
            assertTrue(f'RCW 48.26 is_reserved_chapter', rec.is_reserved_chapter)


def test_ucc_dash_format_class1b():
    """Phase 2 §"Class 1B" — Title 62A UCC chapters with dash-form sections.
    Per-chapter section counts known from Phase 3 verification."""
    print('\n[T5] Class 1B UCC dash-format chapters (14 chapters, Title 62A)')
    expected_62A = {
        '62A.1':  26,  '62A.2':  103, '62A.2A':  78, '62A.3':  76,
        '62A.4':  41,  '62A.4A':  38, '62A.5':   20, '62A.7':  41,
        '62A.8':  52,  '62A.9A': 149, '62A.10':   3, '62A.11': 13,
        '62A.12':  7,  '62A.12A': 9,
    }
    total = 0
    for cite, exp in expected_62A.items():
        fp = DATA_ROOT / 'chapters/title_62A' / f'chap_{cite}_FULL.html'
        rec = parse_chapter_html(fp)
        assertEqual(f'RCW {cite}', exp, len(rec.sections))
        total += len(rec.sections)
    assertEqual('Title 62A total', 656, total)


def test_ucc_dash_section_numbers():
    """UCC sections must preserve the dash form in section_number ('1-101'),
    not split it into chapter/section parts."""
    print('\n[T6] UCC section_number preservation (dash form is atomic)')
    fp = DATA_ROOT / 'chapters/title_62A/chap_62A.1_FULL.html'
    rec = parse_chapter_html(fp)
    expected_first_three = [('1-101', 'RCW 62A.1-101'),
                            ('1-102', 'RCW 62A.1-102'),
                            ('1-103', 'RCW 62A.1-103')]
    for i, (exp_num, exp_cite) in enumerate(expected_first_three):
        s = rec.sections[i]
        assertEqual(f'62A.1 section[{i}].section_number', exp_num, s.section_number)
        assertEqual(f'62A.1 section[{i}].canonical_citation', exp_cite, s.canonical_citation)


def test_cross_ref_no_dash_false_positives():
    """Confirm RCW_SECTION_TEXT_RE does not produce false positives on
    dash-bridged ranges in non-62A body text."""
    print('\n[T7] Cross-ref scoped-alternation (5 non-62A chapters with dates/ranges)')
    samples = [('title_49', '49.60'),
               ('title_42', '42.56'),
               ('title_4',  '4.16'),
               ('title_13', '13.34'),
               ('title_26', '26.44')]
    total_xrefs = 0
    suspect_xrefs = []
    for tdir, cite in samples:
        fp = DATA_ROOT / 'chapters' / tdir / f'chap_{cite}_FULL.html'
        rec = parse_chapter_html(fp)
        for s in rec.sections:
            for x in s.cross_refs:
                total_xrefs += 1
                # A non-62A target with a dash in citation = false positive
                if x.target_jurisdiction == 'state-wa-rcw' and '-' in x.target_citation:
                    if not x.target_citation.startswith('RCW 62A.'):
                        suspect_xrefs.append((cite, x.target_citation, x.context_snippet))
    assertTrue(f'aggregate xrefs across 5 non-62A chapters', total_xrefs > 500,
               f'(total={total_xrefs})')
    assertZero('false-positive dash-form xrefs in non-62A chapters',
               len(suspect_xrefs), suspect_xrefs)


def test_cross_ref_string_patterns():
    """Direct regex tests against synthetic strings — confirm the cross-ref
    detector's false-positive resistance."""
    print('\n[T8] RCW_SECTION_TEXT_RE direct string tests')
    cases = [
        # (input_string, expected_match_count, expected_first_match_or_None)
        ('See RCW 4.04.010 for details.',                   1, '4.04.010'),
        ('Reference to RCW 9A.04.020 here.',                1, '9A.04.020'),
        ('See RCW 62A.1-101 (UCC).',                        1, '62A.1-101'),
        ('Cf. RCW 62A.9A-203 and RCW 4.04.010.',            2, '62A.9A-203'),
        ('the period 1990-1991 was significant',            0, None),
        ('between 2024-2025 calendar years',                0, None),
        ('Laws of 1991, ch. 4, §§ 1-15 apply.',             0, None),
        ('RCW 4.04 to 4.06 (chapter range)',                0, None),  # only 2 dots, no match
        ('RCW 4.04.010-4.04.020 (section range)',           1, '4.04.010'),  # first cite only
        ('RCW 49.60.180(1)(a)(i) for chained subsections.', 1, '49.60.180'),
        ('Cite to RCW 4.04.010(1) and RCW 4.04.010(2).',    2, '4.04.010'),
    ]
    for s, expected_count, expected_first in cases:
        matches = RCW_SECTION_TEXT_RE.findall(s)
        assertEqual(f'count for {s!r}', expected_count, len(matches))
        if expected_first and matches:
            assertEqual(f'first match for {s!r}', expected_first, matches[0])


def test_disposition_parser_recodified():
    """Disposition parser on RCW 43.131 (Sunset Act, 264 entries, largest dispo file)."""
    print('\n[T9] Disposition parser — 43.131 Sunset Act (264 entries)')
    fp = DATA_ROOT / 'dispositions/title_43/dispo_43.131.html'
    rec = parse_disposition_html(fp)
    assertEqual('chapter_number', '43.131', rec.chapter_number)
    assertEqual('title_number', '43', rec.title_number)
    assertTrue('not is_empty', not rec.is_empty)
    assertTrue('many entries', len(rec.entries) > 200, f'(count={len(rec.entries)})')
    recodified = sum(1 for e in rec.entries if e.is_recodified)
    repealed   = sum(1 for e in rec.entries if e.is_repealed)
    assertTrue('recodified entries present', recodified > 0, f'(count={recodified})')
    assertTrue('repealed entries present',   repealed > 0,   f'(count={repealed})')


def test_disposition_parser_empty():
    """Disposition page with no recodified/repealed entries — most chapters fall here."""
    print('\n[T10] Disposition parser — empty/sparse disposition pages')
    # RCW 4.04 dispositions (small chapter, sparse history) — should have a few entries
    fp = DATA_ROOT / 'dispositions/title_4/dispo_4.04.html'
    rec = parse_disposition_html(fp)
    assertEqual('chapter_number', '4.04', rec.chapter_number)
    # Either is_empty or has few entries — both are valid


def test_anchor_re_string_patterns():
    """ANCHOR_RE direct tests on synthetic anchor strings."""
    print('\n[T11] ANCHOR_RE direct string tests')
    cases = [
        ("<a name='4.04.010' >",         '4.04.010'),
        ("<a name='9A.04.020' >",        '9A.04.020'),
        ("<a name='62A.1-101' >",        '62A.1-101'),
        ("<a name='62A.9A-203' >",       '62A.9A-203'),
        ("<a name='42.56.520' >",        '42.56.520'),
        # Should match these (they are valid section anchors per the regex)
        ("<a name='4.04.010A' >",        '4.04.010A'),
    ]
    for s, expected in cases:
        m = ANCHOR_RE.search(s)
        assertEqual(f'anchor match for {s!r}', expected, m.group(1) if m else None)


def test_synthetic_part_heading_anchors_skipped():
    """Synthetic part-heading anchors (e.g., RCW 42.17A.89999) are layout
    dividers, not real sections. Parser must skip them, not emit empty
    SectionRecords that would violate the schema CHECK constraint at INSERT.

    This test is a structurally-different audit channel from the anchor regex
    — it's a content-shape check on the resulting SectionRecord list. Per
    Phase 6 §K, this kind of audit-predicate diversification is load-bearing.
    """
    print('\n[T13] Synthetic part-heading anchor skip (RCW 42.17A.89999)')
    fp = DATA_ROOT / 'chapters/title_42/chap_42.17A_FULL.html'
    rec = parse_chapter_html(fp)
    # Confirm 42.17A.89999 is NOT in the parsed sections
    cites = [s.canonical_citation for s in rec.sections]
    assertTrue('synthetic anchor 42.17A.89999 not in parsed sections',
               'RCW 42.17A.89999' not in cites,
               f'(found in: {[c for c in cites if "89999" in c]})')
    # Real synthetic anchors have empty body. Confirm none of the parsed sections
    # have empty body (proves the skip caught all synthetic anchors in this chapter).
    empty_body = [s.canonical_citation for s in rec.sections if not s.full_text.strip()]
    assertEqual('parsed sections with empty body', 0, len(empty_body))
    # Confirm the chapter still has its real sections (RCW 42.17A.900, .904, etc.)
    assertTrue('chapter still has real sections', len(rec.sections) > 50,
               f'(parsed={len(rec.sections)})')


def test_loose_text_recodified_shell():
    """RCW 70.96.150 — singleton class-of-one shell with loose-text
    recodification clause between credit </div> and notes header. Parser
    must extract the recodification target and mark is_recodified=TRUE.

    Corpus scope: 1 section corpus-wide (verified 2026-04-30 phrase-variant
    enumeration). Counter-example check: 7,466 sections with recodified_from
    (Formerly RCW pointers) have body text, so they don't collide with the
    shell signature.
    """
    print('\n[T18] Loose-text recodification (RCW 70.96.150 → 70.96A.430)')
    fp = DATA_ROOT / 'chapters/title_70/chap_70.96_FULL.html'
    rec = parse_chapter_html(fp)
    target = next((s for s in rec.sections
                   if s.canonical_citation == 'RCW 70.96.150'), None)
    assertTrue('found 70.96.150', target is not None)
    if target:
        assertEqual('is_recodified', True, target.is_recodified)
        assertEqual('recodified_to', 'RCW 70.96A.430', target.recodified_to)
        # CHECK invariant: ghost row → no body
        assertEqual('full_text empty (ghost row)', '',
                    target.full_text.strip() if target.full_text else '')
        # Other status flags should remain FALSE (only is_recodified)
        assertEqual('is_decodified must stay FALSE', False, target.is_decodified)
        assertEqual('is_repealed must stay FALSE', False, target.is_repealed)
        assertEqual('pending_recodification_to (unrelated, must stay None)',
                    None, target.pending_recodification_to)


def test_decodified_shell_section():
    """Decodified-shell sections (e.g., RCW 36.24.110): citation + caption +
    no body + no source-credit + Reviser's note containing 'decodified for
    publication purposes under/pursuant to RCW'. Parser must mark
    is_decodified=TRUE so the row satisfies chk_section_text_status_invariant
    as a ghost row.

    Corroboration #7 of Phase 6 §K — caught by the schema CHECK constraint at
    INSERT, not by any parser-side regex audit.
    """
    print('\n[T17] Decodified-shell section (RCW 36.24.110, Form of warrant)')
    fp = DATA_ROOT / 'chapters/title_36/chap_36.24_FULL.html'
    rec = parse_chapter_html(fp)
    target = next((s for s in rec.sections
                   if s.canonical_citation == 'RCW 36.24.110'), None)
    assertTrue('found 36.24.110', target is not None)
    if target:
        assertEqual('is_decodified', True, target.is_decodified)
        assertEqual('full_text empty/None', '',
                    target.full_text.strip() if target.full_text else '')
        # Counter-positive flags should remain FALSE
        assertEqual('is_repealed (must stay FALSE)', False, target.is_repealed)
        assertEqual('is_recodified (must stay FALSE)', False, target.is_recodified)
        # Notes should contain the Reviser's note with the canonical phrase
        decodif_notes = [n for n in target.notes
                         if 'decodified for publication purposes' in n.note_text.lower()]
        assertTrue('Reviser note with canonical phrase present',
                   len(decodif_notes) >= 1)


def test_text_indent_0pt_body_extraction():
    """RCW 24.03A.* uses text-indent:0pt for body paragraphs (not the canonical
    0.5in). Parser must extract these as body content via the broadened
    text-indent: xpath. Targets the 31 sections rejected by CHECK during the
    initial Title 24 long-tail attempt.

    Audit-predicate diversification corroboration #6: the CHECK constraint
    at INSERT was the structurally-different audit channel that caught the
    hardcoded 0.5in xpath; broadening to permissive `text-indent:` with
    structural in_notes gating resolves the failure class.
    """
    print('\n[T16] text-indent:0pt body extraction (RCW 24.03A.035)')
    fp = DATA_ROOT / 'chapters/title_24/chap_24.03A_FULL.html'
    rec = parse_chapter_html(fp)
    target = next((s for s in rec.sections
                   if s.canonical_citation == 'RCW 24.03A.035'), None)
    assertTrue('found 24.03A.035', target is not None)
    if target:
        assertTrue('body non-empty', len(target.full_text) > 50,
                   f'(len={len(target.full_text)})')
        # Spot-check body content
        assertTrue('body contains substantive text',
                   'foreign nonprofit corporation registered as of December 31, 2021'
                   in target.full_text)
        assertTrue('history populated (2021 c 176 s 1107)',
                   any('2021 c 176 s 1107' in h.session_law for h in target.history))
    # Verify chapter-wide section count: RCW 24.03A chapter has 213 sections
    # per Phase 3 anchor count (not 442 — that was the Title 24 total across
    # all its chapters). The 31 previously-rejected sections were a subset of
    # this chapter; with the fix they should all parse cleanly.
    assertEqual('RCW 24.03A chapter sections', 213, len(rec.sections))
    # Confirm zero parsed sections have empty body — the previous failure mode
    # was 31 sections with empty body (CHECK rejection at INSERT)
    empty = [s.canonical_citation for s in rec.sections if not s.full_text.strip()]
    assertEqual('RCW 24.03A sections with empty body', 0, len(empty))


def test_table_only_sections():
    """Sections whose body is rendered as <table> rather than text-indent
    paragraphs. RCW 9.94A.510 (Sentencing grid) is the canonical example.
    Parser must extract table content as pipe-separated rows so the section
    has non-empty full_text and can survive the chk_section_text_status_invariant
    CHECK at INSERT time.
    """
    print('\n[T15] Table-only sections (RCW 9.94A.510 sentencing grid)')
    fp = DATA_ROOT / 'chapters/title_9/chap_9.94A_FULL.html'
    rec = parse_chapter_html(fp)
    targets = ['RCW 9.94A.510', 'RCW 9.94A.515', 'RCW 9.94A.518']
    for cite in targets:
        s = next((x for x in rec.sections if x.canonical_citation == cite), None)
        assertTrue(f'found {cite}', s is not None)
        if s:
            assertTrue(f'{cite} body non-empty', len(s.full_text) > 100,
                       f'(len={len(s.full_text)})')
            # Spot-check: table body should contain pipe separators (rendered cells)
            assertTrue(f'{cite} body contains pipe-separated cells',
                       '|' in s.full_text,
                       f'(first 200: {s.full_text[:200]!r})')


def test_pending_recodification_extraction():
    """Pre-effective recodification announced in section heading.
    RCW 42.17A.001 heading reads:
      'Declaration of policy. (Effective until January 1, 2026.
       Recodified as RCW 29B.05.010.)'
    Parser must:
      - set pending_recodification_to = 'RCW 29B.05.010'
      - set pending_recodification_effective = '2026-01-01'
      - leave is_recodified = FALSE and full_text NON-empty (current section
        is still effective; CHECK constraint satisfied)
      - emit a CrossRef with target_citation='RCW 29B.05.010', ref_type='recodification'
    """
    print('\n[T14] Pending recodification extraction (RCW 42.17A.001)')
    fp = DATA_ROOT / 'chapters/title_42/chap_42.17A_FULL.html'
    rec = parse_chapter_html(fp)
    target = next((s for s in rec.sections
                   if s.canonical_citation == 'RCW 42.17A.001'), None)
    assertTrue('found 42.17A.001', target is not None)
    if target:
        assertEqual('pending_recodification_to',
                    'RCW 29B.05.010', target.pending_recodification_to)
        assertEqual('pending_recodification_effective',
                    '2026-01-01', target.pending_recodification_effective)
        assertEqual('is_recodified (must stay FALSE — section still effective)',
                    False, target.is_recodified)
        assertEqual('recodified_to (must stay None — pre-effective state)',
                    None, target.recodified_to)
        assertTrue('body preserved', len(target.full_text) > 100,
                   f'(len={len(target.full_text)})')
        # Confirm a CrossRef of type=recodification points at the future citation
        future_xrefs = [x for x in target.cross_refs
                        if x.target_citation == 'RCW 29B.05.010'
                        and x.ref_type == 'recodification']
        assertEqual('count of pending-recodification CrossRefs', 1, len(future_xrefs))


def test_recodified_from_pointer():
    """RCW 42.56.520 has 'Formerly RCW 42.17.320' — confirm recodified_from is set
    and a recodification CrossRef is emitted."""
    print('\n[T12] recodified_from pointer (Formerly RCW)')
    fp = DATA_ROOT / 'chapters/title_42/chap_42.56_FULL.html'
    rec = parse_chapter_html(fp)
    target = next((s for s in rec.sections if s.canonical_citation == 'RCW 42.56.520'), None)
    assertTrue('found 42.56.520', target is not None)
    if target:
        assertEqual('recodified_from', 'RCW 42.17.320', target.recodified_from)
        recod_xrefs = [x for x in target.cross_refs if x.ref_type == 'recodification']
        assertEqual('count of recodification xrefs', 1, len(recod_xrefs))
        if recod_xrefs:
            assertEqual('recodification target_citation', 'RCW 42.17.320',
                        recod_xrefs[0].target_citation)


# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------

def main():
    tests = [
        test_anchor_re_string_patterns,
        test_anchor_parity_simple,
        test_phase0_seven_samples,
        test_stress_chapters,
        test_empty_chapters_class1,
        test_ucc_dash_format_class1b,
        test_ucc_dash_section_numbers,
        test_cross_ref_no_dash_false_positives,
        test_cross_ref_string_patterns,
        test_disposition_parser_recodified,
        test_disposition_parser_empty,
        test_synthetic_part_heading_anchors_skipped,
        test_table_only_sections,
        test_text_indent_0pt_body_extraction,
        test_decodified_shell_section,
        test_loose_text_recodified_shell,
        test_pending_recodification_extraction,
        test_recodified_from_pointer,
    ]
    print('=== rcw_parser test harness ===')
    for t in tests:
        try:
            t()
        except Exception as e:
            msg = f'  ✗ {t.__name__}: EXCEPTION {type(e).__name__}: {e}'
            print(msg)
            _failures.append(msg)

    print(f'\n=== Summary ===')
    print(f'  Passed: {_passes}')
    print(f'  Failed: {len(_failures)}')
    if _failures:
        print(f'\n  Failure details:')
        for f in _failures:
            print(f'    {f}')
        sys.exit(1)
    else:
        print(f'\n  ALL TESTS PASS')
        sys.exit(0)


if __name__ == '__main__':
    main()
