#!/usr/bin/env python3
"""Phase 2.3 — Parser: extract articles + sections + amendments + cross-refs from PDF text.

Source: /tmp/wa_const_cache/constitution.txt (extracted by pdftotext -layout)
Output: /opt/wdws/wa_constitution_research/phase2_parsed.jsonl

Structural understanding (Phase 2.2 inspection):
  - Part A: text from start to '(B) Constitutional Amendments' header
  - Part B: amendments in order of adoption (duplicates content; SKIP)
  - Part C: index (SKIP)

Parser design notes:
  - PDF has 278 form-feed characters (page breaks). Strip before splitting on '\\n'.
  - Page footer pattern: 'MM/DD/YYYY HH:MM AM/PM    [ N ]   AnyText' — strip
  - ARTICLE marker: '^\\s*ARTICLE [IVXLC]+\\s*$' on its own centered line
  - SECTION marker: '^\\s+SECTION (N) HEADING. body...' or '^\\s+SECTION N. body...'
  - Section number forms: '1', '1A', '1(a)' (operator-confirmed §K #40 disposition)
  - Amendment marker (current text terminator): '[AMENDMENT N, YYYY ... .]'
  - Repeal marker: '[Repealed by AMENDMENT N, YYYY ...]'
  - Article XXVI uses word-numbered ordinances (First/Second/Third/Fourth)
    — emit as single section "1" with prose containing all ordinances
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

PDF_TXT = Path("/tmp/wa_const_cache/constitution.txt")
PARSED_OUT = Path("/opt/wdws/wa_constitution_research/phase2_parsed.jsonl")

ARTICLE_LINE_RE = re.compile(r'^\s*ARTICLE\s+([IVXLC]+)\s*$')
# Section marker: SECTION followed by number (with optional letter or parens), followed by space or period
SECTION_LINE_RE = re.compile(r'^\s+SECTION\s+(\d+(?:[A-Z]|\([a-z]\))?)[ .]')
PAGE_FOOTER_RE = re.compile(r'^\s*\d+/\d+/\d{4}\s+\d+:\d+\s+(?:AM|PM)\s+\[\s*\d+\s*\]\s+\S.*$')
PART_B_HEADER_RE = re.compile(r'^\s*\(B\)\s+Constitutional\s+Amendments', re.IGNORECASE)
AMENDMENT_BLOCK_RE = re.compile(r'\[AMENDMENT\s+(\d+),\s+(\d+)([^\]]*?)\]', re.IGNORECASE | re.DOTALL)
REPEAL_BLOCK_RE = re.compile(r'\[Repealed\s+by\s+AMENDMENT\s+(\d+),\s+(\d+)([^\]]*?)\]', re.IGNORECASE | re.DOTALL)
ARTICLE_TOC_RE = re.compile(r'^\s*Article\s+([IVXLC]+)\s+[—\-]\s+(.+?)\s*$')
ARTICLE_TOC_CONTINUATION_RE = re.compile(r'^\s+([A-Z][A-Z ,\-—\(\)/&\']+)\s*$')

# Cross-reference patterns: extract outbound xrefs from section body text.
# Order matters — try specific patterns first.
XREF_PATTERNS = [
    # Federal USC: '42 U.S.C. § 12101' or '42 U.S.C. §§ 12101-12213'
    (re.compile(r'(\d+)\s+U\.\s*S\.\s*C\.\s+§§?\s+([0-9]+[a-z]?(?:[-–][0-9]+[a-z]?)?)'), 'federal-usc',
     lambda m: f"{m.group(1)} U.S.C. § {m.group(2)}"),
    # State RCW section: 'RCW 4.28.080'
    (re.compile(r'\bRCW\s+(\d+[A-Z]?\.\d+[A-Z]?\.\d+(?:\([0-9a-z]+\))?)\b'), 'state-wa-rcw',
     lambda m: f"RCW {m.group(1)}"),
    # State RCW chapter: 'chapter 49.60 RCW' or 'ch. 49.60 RCW'
    (re.compile(r'\b(?:chapter|ch\.)\s+(\d+[A-Z]?\.\d+[A-Z]?)\s+RCW\b'), 'state-wa-rcw',
     lambda m: f"RCW {m.group(1)}"),
    # State RCW title: 'Title 51 RCW'
    (re.compile(r'\bTitle\s+(\d+[A-Z]?)\s+RCW\b'), 'state-wa-rcw',
     lambda m: f"Title {m.group(1)} RCW"),
    # State WAC: 'WAC 296-15-001'
    (re.compile(r'\bWAC\s+(\d+-\d+-\d+)\b'), 'state-wa-wac',
     lambda m: f"WAC {m.group(1)}"),
    # WA court rule: 'GR 31', 'CR 12', 'RPC 1.1'
    (re.compile(r'\b(GR|RPC|CR|ER|RAP|CJC|GALR|CrR|CrRLJ|CRLJ|JuCR|SCCAR|RALJ|APR|ELC)\s+(\d+(?:\.\d+)?[A-Z]?)\b'),
     'state-wa-court-rule', lambda m: f"{m.group(1)} {m.group(2)}"),
    # CFR: '45 C.F.R. § 1340'
    (re.compile(r'(\d+)\s+C\.\s*F\.\s*R\.\s+§§?\s+([0-9]+[a-z]?(?:\.[0-9]+[a-z]?)?)'), 'cfr',
     lambda m: f"{m.group(1)} C.F.R. § {m.group(2)}"),
    # Internal WA Const section-form: 'Wash. Const. art. IV § 31', 'art. IV, § 31', 'Article IV, section 31'
    (re.compile(r'(?:Wash\.\s+Const\.\s+)?(?:[Aa]rt(?:icle|\.)\s+)([IVXLC]+),?\s+§?\s*(?:[Ss]ection\s+)?(\d+(?:[A-Z]|\([a-z]\))?)\b'), 'wa-constitution',
     lambda m: f"Wash. Const. art. {m.group(1)}, § {m.group(2)}"),
    # Internal WA Const article-only: 'Article IV', 'Wash. Const. art. IV', 'art. IV'
    (re.compile(r'(?:Wash\.\s+Const\.\s+)?(?:[Aa]rt(?:icle|\.)\s+)([IVXLC]+)\b(?!\s*,?\s+§)(?!\s*,?\s+[Ss]ection)'), 'wa-constitution',
     lambda m: f"Wash. Const. art. {m.group(1)}"),
]


def is_page_footer(line: str) -> bool:
    return bool(PAGE_FOOTER_RE.match(line))


def normalize_text(text: str) -> str:
    """Strip form feeds + normalize whitespace."""
    text = text.replace('\x0c', '')
    return text


def find_part_a(lines: list[str]) -> list[str]:
    """Return only Part A lines. Part B header appears twice in PDF: once in TOC
    near top, once as actual section divider. Take the LAST occurrence."""
    last_match = None
    for i, line in enumerate(lines):
        if PART_B_HEADER_RE.match(line):
            last_match = i
    if last_match is None:
        return lines
    return lines[:last_match]


def strip_page_footers(lines: list[str]) -> list[str]:
    return [l for l in lines if not is_page_footer(l)]


def extract_article_title_from_toc(all_lines: list[str], roman: str) -> str:
    """Look for 'Article IV — THE JUDICIARY' in TOC. Handles wrap to next line."""
    for i, line in enumerate(all_lines[:520]):
        m = ARTICLE_TOC_RE.match(line)
        if m and m.group(1) == roman:
            title = m.group(2).strip()
            # Check next line for continuation
            for j in range(i+1, min(i+4, len(all_lines))):
                next_line = all_lines[j].rstrip()
                if not next_line.strip():
                    break
                # If next line is another article TOC entry, stop
                if ARTICLE_TOC_RE.match(next_line):
                    break
                cont = ARTICLE_TOC_CONTINUATION_RE.match(next_line)
                if cont:
                    title = title + ' ' + cont.group(1).strip()
                else:
                    break
            return title
    return ""


def parse_section_marker(line: str) -> tuple[str, str, str] | None:
    """Parse a SECTION marker line. Returns (section_number, heading, body_start) or None."""
    m = SECTION_LINE_RE.match(line)
    if not m:
        return None
    section_num = m.group(1)
    # Get text after the section number marker
    after_match = line[m.end():].strip()

    if not after_match:
        return (section_num, "", "")

    # Heading is uppercase letters/punctuation up to first '. ' (period+space) or end of line
    # Some headings span multiple lines; collect up to next sentence start (lowercase after period)
    heading_match = re.match(r'^([A-Z0-9 ,\-—\(\)/&\'\.]+?)\.\s+([A-Z][a-z].*|\[)', after_match)
    if heading_match:
        heading = heading_match.group(1).strip().rstrip('.').strip()
        body_start = after_match[heading_match.end(1)+1:].strip()
        return (section_num, heading, body_start)
    # Couldn't separate heading from body; try simpler split on first period
    if '.' in after_match:
        parts = after_match.split('.', 1)
        candidate_heading = parts[0].strip()
        # Heuristic: heading is mostly uppercase
        if candidate_heading and sum(c.isupper() for c in candidate_heading if c.isalpha()) > len(candidate_heading) * 0.5:
            return (section_num, candidate_heading, parts[1].strip() if len(parts) > 1 else "")
    return (section_num, "", after_match)


def reconnect_hyphens(text: str) -> str:
    """Reconnect hyphenated line breaks ('limita- tions' → 'limitations')."""
    return re.sub(r'(\w+)-\s+([a-z])', r'\1\2', text)


def extract_xrefs(text: str, source_article: str, source_section: str | None) -> list[dict]:
    """Extract outbound cross-references from section body text."""
    out = []
    seen = set()
    for pat, jur, fmt in XREF_PATTERNS:
        for m in pat.finditer(text):
            try:
                target_cit = fmt(m)
            except (IndexError, AttributeError):
                continue
            target_cit = target_cit.strip()
            if not target_cit:
                continue
            # Skip self-reference
            if jur == 'wa-constitution':
                self_article = f"Wash. Const. art. {source_article}"
                if target_cit == self_article:
                    continue
                if source_section and target_cit == f"{self_article}, § {source_section}":
                    continue
            key = (target_cit, jur)
            if key in seen:
                continue
            seen.add(key)
            start = max(0, m.start() - 60)
            end = min(len(text), m.end() + 60)
            snippet = text[start:end].replace('\n', ' ').strip()
            out.append({
                'target_citation': target_cit,
                'target_jurisdiction': jur,
                'context_snippet': snippet[:300],
                'ref_type': 'authority' if jur == 'wa-constitution' else 'reference',
            })
    return out


def parse_section_body(raw_body_lines: list[str]) -> dict:
    """Parse section body lines into structured fields."""
    body_text = ' '.join(line.strip() for line in raw_body_lines if line.strip())
    body_text = re.sub(r'\s+', ' ', body_text)
    body_text = reconnect_hyphens(body_text)

    is_repealed = False
    repealed_by = None
    is_amended = False
    amended_by = None
    effective_date_year = None
    original_text = None

    repeal_m = REPEAL_BLOCK_RE.search(body_text)
    if repeal_m:
        is_repealed = True
        repealed_by = f"AMENDMENT {repeal_m.group(1)}, {repeal_m.group(2)}"

    amend_matches = list(AMENDMENT_BLOCK_RE.finditer(body_text))
    if amend_matches:
        first_amend = amend_matches[0]
        is_amended = True
        amended_by = f"AMENDMENT {first_amend.group(1)}, {first_amend.group(2)}"
        try:
            effective_date_year = int(first_amend.group(2))
        except (ValueError, AttributeError):
            pass
        current_text = body_text[:first_amend.end()].strip()
        if first_amend.end() < len(body_text):
            tail = body_text[first_amend.end():].strip()
            original_text = tail if tail else None
        full_text = current_text
    else:
        full_text = body_text
        original_text = None

    return {
        'full_text': full_text if full_text else None,
        'is_repealed': is_repealed,
        'repealed_by': repealed_by,
        'is_amended': is_amended,
        'amended_by': amended_by,
        'effective_date_year': effective_date_year,
        'original_text': original_text,
    }


def determine_letter_suffix_pattern(section_number: str) -> str | None:
    """§K #40: 'standalone' for letter-suffix sections, NULL otherwise."""
    if re.match(r'^\d+([A-Z]|\([a-z]\))$', section_number):
        return 'standalone'
    return None


def synthesize_article_xxvi(article: dict, raw_lines: list[str]) -> dict:
    """Article XXVI uses word-numbered ordinances (First./Second./Third./Fourth.)
    with no SECTION markers. Synthesize a single section '1' containing all prose."""
    body = ' '.join(line.strip() for line in raw_lines if line.strip())
    body = re.sub(r'\s+', ' ', body)
    body = reconnect_hyphens(body)
    section = {
        'section_number': '1',
        'section_heading': 'COMPACT WITH THE UNITED STATES',
        'canonical_citation': f"Wash. Const. art. {article['article_number']}, § 1",
        'letter_suffix_pattern': None,
        'full_text': body,
        'is_repealed': False,
        'repealed_by': None,
        'is_amended': False,
        'amended_by': None,
        'effective_date_year': None,
        'original_text': None,
        'cross_references': [],
    }
    # Extract xrefs from body
    section['cross_references'] = extract_xrefs(body, article['article_number'], '1')
    return section


def main() -> int:
    print("=== Phase 2.3 parser begin ===", flush=True)
    print(f"PDF text source: {PDF_TXT}", flush=True)

    raw = normalize_text(PDF_TXT.read_text(encoding='utf-8'))
    all_lines = raw.split('\n')
    print(f"Total lines (after FF strip): {len(all_lines)}", flush=True)

    part_a = find_part_a(all_lines)
    print(f"Part A lines: {len(part_a)}", flush=True)

    part_a = strip_page_footers(part_a)
    print(f"Part A lines after footer strip: {len(part_a)}", flush=True)

    articles: list[dict] = []
    current_article = None
    current_section = None
    section_body_buffer: list[str] = []
    article_raw_lines: list[str] = []  # For Article XXVI special handling

    def close_section() -> None:
        nonlocal current_section, section_body_buffer
        if current_section is not None and current_article is not None:
            parsed = parse_section_body(section_body_buffer)
            current_section.update(parsed)
            xref_text = (current_section.get('full_text') or '') + ' ' + (current_section.get('original_text') or '')
            current_section['cross_references'] = extract_xrefs(
                xref_text, current_article['article_number'], current_section.get('section_number')
            )
            current_article['sections'].append(current_section)
        current_section = None
        section_body_buffer = []

    def close_article() -> None:
        nonlocal current_article, article_raw_lines
        if current_article is not None:
            close_section()
            # Article XXVI special-case: no SECTION markers → synthesize
            if current_article['article_number'] == 'XXVI' and not current_article['sections']:
                # Filter raw lines to skip article-header lines
                body_lines = []
                seen_blank_after_header = False
                for ln in article_raw_lines:
                    if not ln.strip():
                        seen_blank_after_header = True
                        continue
                    # Skip article-header lines (the title that follows ARTICLE marker)
                    if not seen_blank_after_header and ln.strip().isupper():
                        continue
                    body_lines.append(ln)
                current_article['sections'].append(synthesize_article_xxvi(current_article, body_lines))
            articles.append(current_article)
        current_article = None
        article_raw_lines = []

    for line in part_a:
        am = ARTICLE_LINE_RE.match(line)
        if am:
            close_article()
            roman = am.group(1)
            title = extract_article_title_from_toc(all_lines, roman)
            current_article = {
                'article_number': roman,
                'article_title': title,
                'sections': [],
            }
            article_raw_lines = []
            continue

        if current_article is not None:
            article_raw_lines.append(line)
            sm = parse_section_marker(line)
            if sm:
                close_section()
                section_num, heading, body_start = sm
                current_section = {
                    'section_number': section_num,
                    'section_heading': heading,
                    'canonical_citation': f"Wash. Const. art. {current_article['article_number']}, § {section_num}",
                    'letter_suffix_pattern': determine_letter_suffix_pattern(section_num),
                }
                section_body_buffer = [body_start] if body_start else []
                continue

            if current_section is not None:
                section_body_buffer.append(line)

    close_article()

    # Summarize
    print(f"\n--- Parser results ---", flush=True)
    print(f"Articles parsed: {len(articles)}", flush=True)
    total_sections = sum(len(a['sections']) for a in articles)
    repealed = sum(1 for a in articles for s in a['sections'] if s.get('is_repealed'))
    amended = sum(1 for a in articles for s in a['sections'] if s.get('is_amended'))
    standalone_suffix = sum(1 for a in articles for s in a['sections']
                            if s.get('letter_suffix_pattern') == 'standalone')
    total_xrefs = sum(len(s.get('cross_references', [])) for a in articles for s in a['sections'])
    print(f"Sections total: {total_sections}", flush=True)
    print(f"  repealed:                 {repealed}", flush=True)
    print(f"  amended:                  {amended}", flush=True)
    print(f"  standalone letter-suffix: {standalone_suffix}", flush=True)
    print(f"Cross-references extracted: {total_xrefs}", flush=True)

    for a in articles:
        title = (a['article_title'] or '(no title)')[:55]
        print(f"  Art {a['article_number']:>5} ({title:<55}) — {len(a['sections']):>3} sections", flush=True)

    # Halt-band check
    halts = []
    if len(articles) != 32:
        halts.append(f"article count {len(articles)} != 32")
    if total_sections < 270 or total_sections > 285:
        halts.append(f"total_sections={total_sections} outside [270, 285]")
    art_i = next((a for a in articles if a['article_number'] == 'I'), None)
    if not art_i or len(art_i['sections']) not in range(30, 41):
        halts.append(f"Article I sections={len(art_i['sections']) if art_i else 'MISSING'} outside [30, 40]")

    # Operationally-active sections
    active_targets = {
        ('I', '3'), ('I', '12'), ('I', '14'), ('I', '21'), ('I', '32'),
        ('IV', '31'),
    }
    found_active = set()
    for a in articles:
        for s in a['sections']:
            key = (a['article_number'], s['section_number'])
            if key in active_targets and not s.get('is_repealed'):
                found_active.add(key)
    missing = active_targets - found_active
    if missing:
        halts.append(f"missing operationally-active sections: {missing}")

    # Cross-reference parser sanity: must extract some RCW/USC/court-rule patterns
    if total_xrefs == 0:
        halts.append("zero cross-references extracted — parser broken")

    if halts:
        print(f"\n!! HALTS: {halts}", flush=True)
        return 2
    else:
        print(f"  ✓ all 6 operationally-active sections present + active", flush=True)
        print(f"  ✓ halt-band [270, 285] check passed (got {total_sections})", flush=True)
        print(f"  ✓ 32 articles parsed", flush=True)

    # Section UNIQUE constraint pre-check (article_id, section_number)
    for a in articles:
        nums = [s['section_number'] for s in a['sections']]
        dups = [n for n in nums if nums.count(n) > 1]
        if dups:
            print(f"!! Article {a['article_number']}: duplicate section_numbers: {set(dups)}", flush=True)
            return 2

    # Write JSONL
    with PARSED_OUT.open('w') as f:
        for a in articles:
            f.write(json.dumps(a, ensure_ascii=False) + '\n')
    print(f"\nWrote: {PARSED_OUT}", flush=True)
    print("=== Phase 2.3 parser close ===", flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
