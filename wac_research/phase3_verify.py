#!/usr/bin/env python3
"""
WAC Phase 3 — Full-corpus parser verification.

Walks every cached chapter HTML in /opt/wdws/data/wac/html/chapters/, parses
each, collects per-chapter result records, and writes:
  - phase3_verify_report.jsonl     (per-chapter records)
  - phase3_verify_summary.json     (aggregate counts + anomaly inventory)

Per-chapter record shape:
  {
    "title": "162", "chapter": "162-16",
    "status": "ok" | "title_redirect" | "appendix_only" | "parse_error",
    "active_sections": int,
    "dispositions": int,
    "is_disposition_only_chapter": bool,
    "authority_chain_count": int,
    "history_count": int,
    "cross_refs_count": int,
    "notes_count": int,
    "first_section_chars": int | null,
    "error": str | null,
  }

Halt protocol: continues on parse_error per chapter (collects in summary);
fatal exception only on full corpus walk failure.
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from wac_parser import parse_chapter_html, TitleRedirectError, ChapterRecord


CHAPTERS_DIR = Path('/opt/wdws/data/wac/html/chapters')
REPORT_PATH  = Path('/opt/wdws/wac_research/phase3_verify_report.jsonl')
SUMMARY_PATH = Path('/opt/wdws/wac_research/phase3_verify_summary.json')


def classify_chapter(rec: ChapterRecord) -> str:
    """Classify based on parse output."""
    if rec.is_disposition_only_chapter:
        return 'disposition_only'
    if rec.sections:
        return 'ok'
    # No active sections AND no dispositions → likely appendix-only or empty
    return 'appendix_or_empty'


def main() -> None:
    files = sorted(CHAPTERS_DIR.glob('*/*.html'))
    print(f'Found {len(files)} chapter files; parsing...')
    started = time.monotonic()

    records: list[dict] = []
    status_counter: Counter[str] = Counter()
    title_section_counts: dict[str, int] = {}     # title_number → cumulative active sections
    title_chapter_counts: dict[str, int] = {}     # title_number → cumulative chapters
    section_char_lengths: list[int] = []          # for chunks/section corpus-wide validation
    history_total = 0
    auth_total = 0
    xref_total = 0
    dispositions_total = 0
    errors: list[dict] = []

    for i, fp in enumerate(files, start=1):
        title_dir = fp.parent.name
        chapter_stem = fp.stem
        cite = f'{title_dir}-{chapter_stem}'
        rec_dict = {
            'title': title_dir,
            'chapter': cite,
            'file_path': str(fp),
            'status': '',
            'active_sections': 0,
            'dispositions': 0,
            'is_disposition_only_chapter': False,
            'authority_chain_count': 0,
            'history_count': 0,
            'cross_refs_count': 0,
            'notes_count': 0,
            'first_section_chars': None,
            'error': None,
        }
        try:
            rec = parse_chapter_html(fp)
            rec_dict['status'] = classify_chapter(rec)
            rec_dict['active_sections'] = len(rec.sections)
            rec_dict['dispositions'] = len(rec.dispositions)
            rec_dict['is_disposition_only_chapter'] = rec.is_disposition_only_chapter
            rec_dict['authority_chain_count'] = sum(len(s.authority_chain) for s in rec.sections)
            rec_dict['history_count'] = sum(len(s.history) for s in rec.sections)
            rec_dict['cross_refs_count'] = sum(len(s.cross_refs) for s in rec.sections)
            rec_dict['notes_count'] = sum(len(s.notes) for s in rec.sections)
            if rec.sections:
                rec_dict['first_section_chars'] = len(rec.sections[0].full_text)

            title_section_counts[title_dir] = title_section_counts.get(title_dir, 0) + len(rec.sections)
            title_chapter_counts[title_dir] = title_chapter_counts.get(title_dir, 0) + 1
            for s in rec.sections:
                section_char_lengths.append(len(s.full_text))
            history_total += rec_dict['history_count']
            auth_total += rec_dict['authority_chain_count']
            xref_total += rec_dict['cross_refs_count']
            dispositions_total += rec_dict['dispositions']
        except TitleRedirectError as e:
            rec_dict['status'] = 'title_redirect'
            rec_dict['error'] = str(e)
            errors.append({'cite': cite, 'class': 'title_redirect', 'msg': str(e)})
        except Exception as e:
            rec_dict['status'] = 'parse_error'
            rec_dict['error'] = f'{type(e).__name__}: {e}'
            errors.append({
                'cite': cite,
                'class': 'parse_error',
                'msg': str(e),
                'traceback': traceback.format_exc(),
            })

        status_counter[rec_dict['status']] += 1
        records.append(rec_dict)

        if i % 200 == 0 or i == len(files):
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed else 0
            print(f'  [{i}/{len(files)}] {rate:.1f}/s; status counts: {dict(status_counter)}')

    elapsed = time.monotonic() - started

    # Compute corpus-wide chunks/section estimate (carries forward Phase 2 §3.3)
    # Use 512-token chunk target; estimate tokens as words × 1.3.
    if section_char_lengths:
        # Approximate words/section as chars/5 (rough English word avg)
        words_per_sec = [c / 5 for c in section_char_lengths]
        tokens_per_sec = [w * 1.3 for w in words_per_sec]
        chunks_per_sec = [max(1, int((t + 511) // 512)) for t in tokens_per_sec]
        mean_chars = sum(section_char_lengths) / len(section_char_lengths)
        mean_chunks = sum(chunks_per_sec) / len(chunks_per_sec)
        median_chars = sorted(section_char_lengths)[len(section_char_lengths) // 2]
        max_chars = max(section_char_lengths)
        total_chunks = sum(chunks_per_sec)
    else:
        mean_chars = 0; median_chars = 0; max_chars = 0; mean_chunks = 0; total_chunks = 0

    summary = {
        'wall_seconds': elapsed,
        'files_total': len(files),
        'status_counts': dict(status_counter),
        'totals': {
            'active_sections': sum(r['active_sections'] for r in records),
            'dispositions':    dispositions_total,
            'authority_chain': auth_total,
            'history':         history_total,
            'cross_refs':      xref_total,
        },
        'titles_with_sections':   len([t for t, n in title_section_counts.items() if n > 0]),
        'titles_no_sections':     len([t for t, n in title_section_counts.items() if n == 0]),
        'section_length_stats': {
            'n':            len(section_char_lengths),
            'mean_chars':   round(mean_chars, 1),
            'median_chars': median_chars,
            'max_chars':    max_chars,
            'mean_chunks':  round(mean_chunks, 3),
            'total_chunks': total_chunks,
        },
        'errors_count': len(errors),
        'errors_sample': errors[:20],
        'top_titles_by_sections': sorted(
            ((t, n) for t, n in title_section_counts.items()),
            key=lambda x: -x[1],
        )[:15],
    }

    # Write outputs
    REPORT_PATH.write_text('\n'.join(json.dumps(r) for r in records) + '\n')
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2))

    print()
    print(f'=== PHASE 3 VERIFICATION COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f}m) ===')
    print(f'Files parsed: {len(files)}')
    print(f'Status: {dict(status_counter)}')
    print(f'Active sections total: {summary["totals"]["active_sections"]}')
    print(f'Dispositions total:    {summary["totals"]["dispositions"]}')
    print(f'Authority chain rows:  {summary["totals"]["authority_chain"]}')
    print(f'History rows:          {summary["totals"]["history"]}')
    print(f'Cross-refs:            {summary["totals"]["cross_refs"]}')
    print(f'Mean chars/section:    {mean_chars:.0f}')
    print(f'Mean chunks/section:   {mean_chunks:.2f}')
    print(f'Total chunks:          {total_chunks}')
    print(f'Errors:                {len(errors)}')
    print(f'Report:  {REPORT_PATH}')
    print(f'Summary: {SUMMARY_PATH}')


if __name__ == '__main__':
    main()
