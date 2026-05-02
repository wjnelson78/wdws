#!/usr/bin/env python3
"""
WAC Phase 4 long-tail driver — iterates all populated titles minus already-ingested,
runs Stage 1 (parse + INSERT) per title, logs progress + per-25-title checkpoints.

Stage 2 embedding is run as a separate concurrent process (phase4_stage2_wac_parallel.py
launched separately) — drains the unembedded chunk queue corpus-wide, scoped by JOIN
to wac.sections.

Per operator long-tail authorization 2026-04-30: skip individual per-title canaries
(would dominate wall time); rely on Phase 5 validation suite to catch corpus-wide issues.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/opt/wdws/wac_research")
LOG = ROOT / "phase4_long_tail.log"
PROGRESS = ROOT / "phase4_long_tail.progress.json"

# Already-completed priority-core titles to skip
ALREADY_INGESTED = {"296", "162", "388", "110"}

# Coverage map produced by Phase 2 — list of all 228 populated titles + their chapters
COVERAGE_MAP = ROOT / "phase2_coverage_map.json"


def log(msg: str):
    line = f"[{datetime.now(timezone.utc).isoformat(timespec='seconds')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


def write_progress(payload: dict):
    payload["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    PROGRESS.write_text(json.dumps(payload, indent=2))


def main():
    map_data = json.loads(COVERAGE_MAP.read_text())
    titles_all = list(map_data["titles"].keys())  # all 228
    long_tail = [t for t in titles_all if t not in ALREADY_INGESTED]
    long_tail.sort(key=lambda t: (
        int(__import__('re').match(r'(\d+)', t).group(1)),
        t,
    ))

    log(f"=== WAC Phase 4 LONG-TAIL DRIVER START ===")
    log(f"  total populated titles in corpus: {len(titles_all)}")
    log(f"  already ingested (priority core): {sorted(ALREADY_INGESTED)}")
    log(f"  long-tail to ingest: {len(long_tail)}")
    log(f"  first 10: {long_tail[:10]}")
    log(f"  last 10:  {long_tail[-10:]}")

    started = time.monotonic()
    stats = {
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_titles": len(long_tail),
        "completed_titles": 0,
        "skipped_titles": [],
        "errors": [],
        "elapsed_seconds": 0,
    }
    write_progress(stats)

    for i, title in enumerate(long_tail, start=1):
        t0 = time.monotonic()
        cmd = [sys.executable, "-u", str(ROOT / "phase4_ingest_wac.py"),
               "--title", title, "--skip-stage2"]
        log(f"[{i}/{len(long_tail)}] starting Title {title}")
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=600,
            )
        except subprocess.TimeoutExpired:
            log(f"  TIMEOUT on Title {title}; skipping")
            stats["errors"].append({"title": title, "error": "timeout 600s"})
            continue
        except Exception as e:
            log(f"  EXCEPTION on Title {title}: {e}")
            stats["errors"].append({"title": title, "error": str(e)})
            continue

        if result.returncode != 0:
            log(f"  Title {title} returned exit {result.returncode}; stderr tail: {result.stderr[-400:]}")
            stats["errors"].append({"title": title, "exit": result.returncode,
                                     "stderr_tail": result.stderr[-400:]})
            continue

        elapsed = time.monotonic() - t0
        # Extract Stage 1 stats from last line of stdout (the "Stage 1 done: {dict}" line)
        try:
            for line in reversed(result.stdout.split("\n")):
                if "Stage 1 done:" in line:
                    log(f"  Title {title} done in {elapsed:.1f}s — {line.split('Stage 1 done: ', 1)[1]}")
                    break
            else:
                log(f"  Title {title} done in {elapsed:.1f}s (no Stage 1 done: line found)")
        except Exception:
            log(f"  Title {title} done in {elapsed:.1f}s")

        stats["completed_titles"] = i
        stats["elapsed_seconds"] = time.monotonic() - started
        if i % 10 == 0:
            write_progress(stats)
        if i % 25 == 0:
            log(f"=== CHECKPOINT-{i} === {i}/{len(long_tail)} titles complete; "
                f"elapsed={(time.monotonic()-started)/60:.1f}min; "
                f"errors={len(stats['errors'])}")

    elapsed = time.monotonic() - started
    stats["elapsed_seconds"] = elapsed
    stats["completed_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_progress(stats)
    log(f"=== LONG-TAIL DRIVER COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f}min) ===")
    log(f"  completed: {stats['completed_titles']}/{len(long_tail)}")
    log(f"  errors: {len(stats['errors'])}")
    if stats["errors"]:
        log(f"  first 5 errors: {stats['errors'][:5]}")


if __name__ == "__main__":
    main()
