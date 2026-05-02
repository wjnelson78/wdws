"""
Phase 3 verification: parse ALL 2,763 cached chapter HTMLs + 2,763 dispositions,
emit JSONL audit report and aggregate stats.

Read-only: no DB writes.
"""
from __future__ import annotations
import json, os, sys, time, glob
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent))
from rcw_parser import parse_chapter_html, parse_disposition_html, count_section_anchors

ROOT = Path('/opt/wdws/data/rcw/html')
REPORT_PATH = Path('/opt/wdws/rcw_research/phase3_verify_report.jsonl')
SUMMARY_PATH = Path('/opt/wdws/rcw_research/phase3_verify_summary.json')

start = time.time()
chapter_files = sorted(glob.glob(str(ROOT / 'chapters' / 'title_*' / 'chap_*_FULL.html')))
print(f'[verify] {len(chapter_files)} chapter files to process')

agg = {
    'chapters_parsed': 0,
    'chapters_failed': 0,
    'parity_pass': 0,
    'parity_fail': 0,
    'parity_fail_details': [],
    'reserved_chapters': 0,
    'empty_chapters_no_anchors': 0,
    'sections_total': 0,
    'sections_with_recodified_from': 0,
    'history_entries_total': 0,
    'notes_entries_total': 0,
    'cross_refs_total': 0,
    'note_type_distribution': Counter(),
    'cross_ref_jurisdiction_distribution': Counter(),
    'cross_ref_type_distribution': Counter(),
    'session_year_distribution': Counter(),
    'sections_per_title': Counter(),
    'sections_with_no_history': 0,
    'sections_with_zero_body_paragraphs': 0,
}

with open(REPORT_PATH, 'w') as report:
    for i, fp in enumerate(chapter_files):
        if (i + 1) % 200 == 0:
            elapsed = time.time() - start
            rate = (i + 1) / elapsed
            eta = (len(chapter_files) - (i + 1)) / rate if rate > 0 else 0
            print(f'[verify] {i+1}/{len(chapter_files)} ({rate:.1f}/s, eta={eta:.0f}s)')
        try:
            anchors = count_section_anchors(fp)
            rec = parse_chapter_html(fp)
            agg['chapters_parsed'] += 1
            parsed_count = len(rec.sections)
            if parsed_count == anchors:
                agg['parity_pass'] += 1
            else:
                agg['parity_fail'] += 1
                agg['parity_fail_details'].append({
                    'file': fp, 'chapter': rec.chapter_number,
                    'anchors': anchors, 'parsed': parsed_count,
                })
            if rec.is_reserved_chapter:
                agg['reserved_chapters'] += 1
            if anchors == 0:
                agg['empty_chapters_no_anchors'] += 1
            agg['sections_total'] += parsed_count
            agg['sections_per_title'][rec.title_number] += parsed_count

            for s in rec.sections:
                if s.recodified_from is not None:
                    agg['sections_with_recodified_from'] += 1
                if not s.history:
                    agg['sections_with_no_history'] += 1
                if not s.full_text.strip():
                    agg['sections_with_zero_body_paragraphs'] += 1
                agg['history_entries_total'] += len(s.history)
                agg['notes_entries_total'] += len(s.notes)
                agg['cross_refs_total'] += len(s.cross_refs)
                for n in s.notes:
                    agg['note_type_distribution'][n.note_type] += 1
                for x in s.cross_refs:
                    agg['cross_ref_jurisdiction_distribution'][x.target_jurisdiction] += 1
                    agg['cross_ref_type_distribution'][x.ref_type] += 1
                for h in s.history:
                    if h.session_year:
                        decade = (h.session_year // 10) * 10
                        agg['session_year_distribution'][decade] += 1

            report.write(json.dumps({
                'file': fp,
                'chapter': rec.chapter_number,
                'title': rec.title_number,
                'is_reserved_chapter': rec.is_reserved_chapter,
                'anchors_in_html': anchors,
                'sections_parsed': parsed_count,
                'parity': anchors == parsed_count,
                'sections_with_recodified_from': sum(1 for s in rec.sections if s.recodified_from),
                'history_entries': sum(len(s.history) for s in rec.sections),
                'notes_entries': sum(len(s.notes) for s in rec.sections),
                'cross_refs': sum(len(s.cross_refs) for s in rec.sections),
            }) + '\n')

        except Exception as e:
            agg['chapters_failed'] += 1
            agg['parity_fail_details'].append({
                'file': fp, 'error': str(e),
            })
            print(f'[verify] FAIL {fp}: {e}', file=sys.stderr)
            report.write(json.dumps({'file': fp, 'error': str(e)}) + '\n')

# Disposition pass
print(f'\n[verify] Parsing 2,763 disposition pages...')
dispo_files = sorted(glob.glob(str(ROOT / 'dispositions' / 'title_*' / 'dispo_*.html')))
disp_agg = {
    'dispositions_parsed': 0,
    'dispositions_empty': 0,
    'disposition_entries_total': 0,
    'recodified_entries': 0,
    'repealed_entries': 0,
    'decodified_entries': 0,
    'expired_entries': 0,
    'reserved_entries': 0,
    'recodified_to_distribution_titles': Counter(),
}
for i, fp in enumerate(dispo_files):
    if (i + 1) % 500 == 0:
        print(f'[verify-dispo] {i+1}/{len(dispo_files)}')
    try:
        rec = parse_disposition_html(fp)
        disp_agg['dispositions_parsed'] += 1
        if rec.is_empty:
            disp_agg['dispositions_empty'] += 1
        for e in rec.entries:
            disp_agg['disposition_entries_total'] += 1
            if e.is_recodified:
                disp_agg['recodified_entries'] += 1
                if e.recodified_to:
                    target_title = e.recodified_to.replace('RCW ', '').split('.')[0]
                    disp_agg['recodified_to_distribution_titles'][target_title] += 1
            if e.is_repealed: disp_agg['repealed_entries'] += 1
            if e.is_decodified: disp_agg['decodified_entries'] += 1
            if e.is_expired: disp_agg['expired_entries'] += 1
            if e.is_reserved: disp_agg['reserved_entries'] += 1
    except Exception as e:
        print(f'[verify-dispo] FAIL {fp}: {e}', file=sys.stderr)

# Convert Counters to dicts and write summary
def _ser(v):
    if isinstance(v, Counter):
        return dict(sorted(v.items(), key=lambda x: (-x[1], x[0]) if isinstance(x[1], int) else x[0])[:50])
    return v

summary = {k: _ser(v) for k, v in agg.items()}
summary.update({k: _ser(v) for k, v in disp_agg.items()})
summary['elapsed_seconds'] = time.time() - start

with open(SUMMARY_PATH, 'w') as fh:
    json.dump(summary, fh, indent=2, default=str)

print(f'\n=== SUMMARY ===')
for k in [
    'chapters_parsed', 'chapters_failed', 'parity_pass', 'parity_fail',
    'reserved_chapters', 'empty_chapters_no_anchors',
    'sections_total', 'sections_with_recodified_from',
    'sections_with_no_history', 'sections_with_zero_body_paragraphs',
    'history_entries_total', 'notes_entries_total', 'cross_refs_total',
    'dispositions_parsed', 'dispositions_empty', 'disposition_entries_total',
    'recodified_entries', 'repealed_entries', 'decodified_entries',
    'expired_entries', 'reserved_entries',
]:
    print(f'  {k:42}: {summary.get(k)}')
print(f'  elapsed                                   : {summary["elapsed_seconds"]:.1f} sec')
print(f'\n  Top note types:')
for nt, n in list(summary.get('note_type_distribution', {}).items())[:15]:
    print(f'    {nt!r:35}: {n}')
print(f'\n  Cross-ref jurisdiction distribution:')
for j, n in summary.get('cross_ref_jurisdiction_distribution', {}).items():
    print(f'    {j:25}: {n}')
print(f'\n  Cross-ref type distribution:')
for t, n in summary.get('cross_ref_type_distribution', {}).items():
    print(f'    {t:25}: {n}')
print(f'\n  Session-year decade distribution:')
for d in sorted(summary.get('session_year_distribution', {}).keys()):
    print(f'    {d}s : {summary["session_year_distribution"][d]}')
if summary.get('parity_fail_details'):
    print(f'\n  PARITY FAILURES ({len(summary["parity_fail_details"])}):')
    for d in summary['parity_fail_details'][:20]:
        print(f'    {d}')
