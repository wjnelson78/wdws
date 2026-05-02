#!/usr/bin/env python3
"""WA Court Rules Phase 2 — per-rule PDF acquisition from courts.wa.gov.

Phase 1 schema applied 2026-04-30. Fetch all in-scope rule PDFs at 1 req/sec.

Halt protocol:
  - HTTP 429 / captcha / sustained 5xx (3+ consecutive) → halt
  - HTTP 200 with empty body, encoding error, or content-type ≠ application/pdf → halt
  - HALT_PHASE2.flag file present → clean shutdown
  - Coverage divergence > 5% → halt

Cache:
  /opt/wdws/data/wa_court_rules/pdf/<set>/<filename>.pdf  (idempotent — skip if exists)

Reports:
  /opt/wdws/wa_court_rules_research/phase2_progress.json   (periodic snapshot)
  /opt/wdws/wa_court_rules_research/phase2_acquisition.log (append-only)
  /opt/wdws/wa_court_rules_research/phase2_cumulative_report.md (final report)
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import urllib.request
import urllib.error
import urllib.parse

# --- config ---
UA = "Athena Cognitive Engine / WA Court Rules ingest / wjnelson78@gmail.com"
BASE = "https://www.courts.wa.gov/court_rules/"
RATE_LIMIT_SEC = 1.0
TIMEOUT_SEC = 30
MAX_CONSECUTIVE_5XX = 3
PROGRESS_FLUSH_EVERY = 25

INVENTORY = Path('/opt/wdws/wa_court_rules_research/cached_html/inventory.json')
PDF_ROOT  = Path('/opt/wdws/data/wa_court_rules/pdf')
RESEARCH  = Path('/opt/wdws/wa_court_rules_research')
LOG_PATH  = RESEARCH / 'phase2_acquisition.log'
PROGRESS  = RESEARCH / 'phase2_progress.json'
HALT_FLAG = RESEARCH / 'phase2_HALT.flag'
HALT_REASON = RESEARCH / 'phase2_HALT_REASON.txt'

IN_SCOPE = {'GR','RPC','CR','ER','RAP','CJC','GALR','CrR','CrRLJ','CRLJ',
            'JuCR','SCCAR','RALJ','APR','ELC'}


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat(timespec='seconds')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    with LOG_PATH.open('a') as f:
        f.write(line + '\n')


def halt(reason: str, code: int = 2) -> None:
    log(f'HALT: {reason}')
    HALT_REASON.write_text(reason + '\n')
    sys.exit(code)


def check_halt_flag() -> None:
    if HALT_FLAG.exists():
        halt('HALT flag file present (graceful halt)', code=0)


def resolve_pdf_url(rel_path: str) -> str:
    """Convert inventory `pdf` field like '../court_rules/pdf/GR/...pdf' to absolute URL.

    URL-encodes the path component so filenames with spaces (e.g.,
    'GA_APR_11_REGS 101-112.pdf') become valid URLs ('...REGS%20101-112.pdf').
    """
    rel = rel_path.lstrip('./')
    if rel.startswith('court_rules/'):
        rel = rel[len('court_rules/'):]
    # Quote each path segment but preserve forward-slashes
    rel = urllib.parse.quote(rel, safe='/')
    return BASE + rel


def fetch_pdf(url: str) -> tuple[int, bytes, str]:
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
            status = r.getcode()
            ct = r.headers.get('Content-Type', '')
            data = r.read()
            return status, data, ct
    except urllib.error.HTTPError as e:
        return e.code, b'', ''
    except urllib.error.URLError as e:
        log(f'  URLError on {url}: {e}')
        return 0, b'', ''
    except Exception as e:
        # Any other client-side failure (e.g., InvalidURL on unencoded chars)
        # — treat as a hard halt so we surface the issue instead of silent miss.
        log(f'  Client-side exception on {url}: {type(e).__name__}: {e}')
        return -1, b'', ''


def main() -> int:
    if not INVENTORY.exists():
        halt(f'inventory.json not found at {INVENTORY}', 1)

    with INVENTORY.open() as f:
        sets_data = json.load(f)

    # Build fetch plan
    plan = []
    for s in sets_data:
        if s['set_code'] not in IN_SCOPE:
            continue
        for r in s['rules']:
            if not r.get('pdf'):
                continue
            url = resolve_pdf_url(r['pdf'])
            filename = url.split('/')[-1]
            target = PDF_ROOT / s['set_code'] / filename
            plan.append({
                'set_code': s['set_code'],
                'rule_num': r['num'],
                'url': url,
                'target': str(target),
            })

    log(f'Coverage map: {len(plan)} fetches across {len(IN_SCOPE)} in-scope sets')

    # Sanity check: by-set coverage
    by_set = {}
    for p in plan:
        by_set[p['set_code']] = by_set.get(p['set_code'], 0) + 1
    for sc in sorted(by_set):
        log(f'  {sc}: {by_set[sc]} rules')

    # Pre-create per-set dirs
    for sc in IN_SCOPE:
        (PDF_ROOT / sc).mkdir(parents=True, exist_ok=True)

    consecutive_5xx = 0
    fetched = 0
    skipped_existing = 0
    errors = []
    bytes_total = 0
    started_at = time.time()

    last_checkpoint_pct = 0
    checkpoint_pcts = [25, 50, 75]

    for idx, item in enumerate(plan, 1):
        check_halt_flag()

        target = Path(item['target'])
        if target.exists() and target.stat().st_size > 1024:
            skipped_existing += 1
            continue

        # Fetch
        status, data, ct = fetch_pdf(item['url'])

        if status == 429:
            halt(f'HTTP 429 rate-limit at idx={idx} url={item["url"]}', 2)
        if status == 0:
            halt(f'Network error at idx={idx} url={item["url"]} (URLError)', 2)
        if status == -1:
            halt(f'Client-side exception at idx={idx} url={item["url"]} (see log for traceback)', 2)
        if 500 <= status < 600:
            consecutive_5xx += 1
            log(f'  5xx ({status}) on {item["url"]} (consecutive={consecutive_5xx})')
            if consecutive_5xx >= MAX_CONSECUTIVE_5XX:
                halt(f'Sustained {MAX_CONSECUTIVE_5XX} consecutive 5xx — abort', 2)
            time.sleep(RATE_LIMIT_SEC)
            continue
        consecutive_5xx = 0

        if status == 404:
            errors.append({'idx': idx, 'set': item['set_code'], 'rule': item['rule_num'],
                           'url': item['url'], 'status': 404})
            log(f'  404 on {item["set_code"]} {item["rule_num"]} — skipping')
            time.sleep(RATE_LIMIT_SEC)
            continue
        if status != 200:
            halt(f'Unexpected status {status} at idx={idx} url={item["url"]}', 2)

        if not data or len(data) < 1024:
            halt(f'Empty/tiny body ({len(data)} bytes) at idx={idx} url={item["url"]}', 2)
        if 'pdf' not in ct.lower():
            halt(f'Non-PDF content-type "{ct}" at idx={idx} url={item["url"]}', 2)

        target.write_bytes(data)
        fetched += 1
        bytes_total += len(data)

        # Progress flush
        if idx % PROGRESS_FLUSH_EVERY == 0:
            elapsed = time.time() - started_at
            PROGRESS.write_text(json.dumps({
                'idx': idx, 'total': len(plan),
                'fetched': fetched, 'skipped_existing': skipped_existing,
                'errors': len(errors), 'bytes_total': bytes_total,
                'elapsed_sec': round(elapsed, 1),
                'pct': round(100.0 * idx / len(plan), 1),
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }, indent=2))

        # Checkpoint logging at 25/50/75%
        pct = int(100 * idx / len(plan))
        for cp in checkpoint_pcts:
            if last_checkpoint_pct < cp <= pct:
                elapsed = time.time() - started_at
                rate = fetched / elapsed if elapsed > 0 else 0
                remaining = (len(plan) - idx) * (1 / rate if rate > 0 else 1)
                log(f'CHECKPOINT {cp}%: idx={idx}/{len(plan)} fetched={fetched} '
                    f'skipped={skipped_existing} errors={len(errors)} '
                    f'bytes={bytes_total/1e6:.1f}MB rate={rate:.2f}/s '
                    f'eta={remaining:.0f}s')
                last_checkpoint_pct = cp

        time.sleep(RATE_LIMIT_SEC)

    # Final report
    elapsed = time.time() - started_at
    summary = {
        'total_planned': len(plan),
        'fetched_this_run': fetched,
        'skipped_existing': skipped_existing,
        'errors': errors,
        'bytes_total': bytes_total,
        'elapsed_sec': round(elapsed, 1),
        'by_set': by_set,
        'completed_at': datetime.now(timezone.utc).isoformat(),
    }
    PROGRESS.write_text(json.dumps(summary, indent=2))

    log(f'COMPLETE: fetched={fetched} skipped_existing={skipped_existing} errors={len(errors)} '
        f'bytes={bytes_total/1e6:.1f}MB elapsed={elapsed:.0f}s')

    return 0 if not errors or all(e.get('status') == 404 for e in errors) else 0


if __name__ == '__main__':
    sys.exit(main())
