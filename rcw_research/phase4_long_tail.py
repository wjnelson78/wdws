"""
Phase 4 Long-Tail Orchestrator.

Iterates through the 88 RCW titles NOT in the priority-priority-core sequence
(those 12 already-ingested titles: 51, 42, 4, 49, 26, 41, 2, 9, 9A, 10, 19, 7).

For each long-tail title:
  - Invokes phase4_ingest_rcw.py --title T (Stage 1 + Stage 2 in one process)
  - Parses stats from stdout (sections, chunks, history/notes/xrefs, errors)
  - Compares actual section count vs Phase 3 projection (>5% halt trigger)
  - Compares Stage 2 throughput vs floor (<0.5 chunks/s for 5+ batches halt)
  - Writes per-title checkpoint to JSONL log
  - Halts on: errors > 0, throughput floor, deviation > 5%, manual HALT flag

Cumulative checkpoint every 20 titles or every 4 hours.

Per-title canaries are NOT run during long tail (operator: "no per-title canary
unless operator specifies — these aren't priority-anchor titles").
"""
from __future__ import annotations

import argparse, json, os, re, signal, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/opt/wdws/rcw_research")
DATA_ROOT = Path("/opt/wdws/data/rcw/html")
LOG = ROOT / "phase4_long_tail.log"
CHECKPOINTS = ROOT / "phase4_long_tail_checkpoints.jsonl"
PROGRESS = ROOT / "phase4_long_tail.progress"
HALT_FLAG = ROOT / "phase4_long_tail.HALT"
PHASE3_REPORT = ROOT / "phase3_verify_report.jsonl"
INGEST_SCRIPT = ROOT / "phase4_ingest_rcw.py"

PRIORITY_INGESTED = {"51", "42", "4", "49", "26", "41", "2", "9", "9A", "10", "19", "7"}
ALL_TITLES = [
    "1","2","3","4","5","6","7","8","9","9A","10","11","12","13","14",
    "15","16","17","18","19","20","21","22","23","23B","24","25","26",
    "27","28A","28B","28C","29A","29B","30A","30B","31","32","33","34",
    "35","35A","36","37","38","39","40","41","42","43","44","46","47",
    "48","49","50","50A","50B","51","52","53","54","55","57","58","59",
    "60","61","62A","63","64","65","66","67","68","69","70","70A","71",
    "71A","72","73","74","76","77","78","79","79A","80","81","82","83",
    "84","85","86","87","88","89","90","91",
]

LONG_TAIL_TITLES = [t for t in ALL_TITLES if t not in PRIORITY_INGESTED]
assert len(LONG_TAIL_TITLES) == 88, f"expected 88 long-tail titles, got {len(LONG_TAIL_TITLES)}"

# Halt thresholds
SECTION_DEVIATION_PCT = 5.0           # halt if actual differs from Phase 3 projection by >5%
THROUGHPUT_FLOOR = 0.5                # halt if Stage 2 throughput drops below 0.5 chunks/s
THROUGHPUT_FLOOR_RUN = 5              # for 5+ consecutive batches
CHECKPOINT_EVERY_TITLES = 20
CHECKPOINT_EVERY_SECONDS = 4 * 3600   # 4 hours


def log(msg: str):
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


def write_progress(text: str):
    with open(PROGRESS, "w") as fh:
        fh.write(f"{datetime.now(timezone.utc).isoformat()} {text}\n")


def load_phase3_projections() -> dict[str, int]:
    """Per-title section count expected from Phase 3 cached-HTML parse."""
    counts: dict[str, int] = {}
    with open(PHASE3_REPORT) as fh:
        for line in fh:
            try:
                r = json.loads(line)
                if "error" in r:
                    continue
                t = r.get("title")
                if t:
                    counts[t] = counts.get(t, 0) + r.get("sections_parsed", 0)
            except Exception:
                pass
    return counts


def parse_stage1_stats(stdout: str) -> dict | None:
    """Extract the Stage 1 done line into a dict. Returns None if not found."""
    m = re.search(r"--- Stage 1 done: (\{.+?\})\s*$", stdout, re.MULTILINE | re.DOTALL)
    if not m:
        return None
    raw = m.group(1)
    # Python dict repr — eval safely
    try:
        return eval(raw, {"__builtins__": {}}, {})
    except Exception:
        return None


def parse_stage2_throughput(stdout: str) -> list[float]:
    """Extract per-batch chunks/sec values from Stage 2 progress lines."""
    rates = []
    for m in re.finditer(r"\[stage2 t\S+\] batch \d+: \d+/\d+ rate=([0-9.]+) chunks/s", stdout):
        rates.append(float(m.group(1)))
    return rates


def parse_stage2_total(stdout: str) -> int | None:
    m = re.search(r"--- Stage 2 done: (\d+) chunks embedded", stdout)
    return int(m.group(1)) if m else None


def parse_stage1_seconds(stage1_stats: dict) -> float:
    return float(stage1_stats.get("stage1_seconds", 0))


def ingest_title(title: str, stage1_only: bool = False) -> dict:
    """Run phase4_ingest_rcw.py for one title and return checkpoint dict."""
    t_start = time.time()
    cmd = [sys.executable, "-u", str(INGEST_SCRIPT), "--title", title]
    if stage1_only:
        cmd.append("--skip-stage2")
    log(f"START title={title}  cmd={' '.join(cmd[1:])}")
    try:
        p = subprocess.run(
            cmd, capture_output=True, text=True, timeout=7200,  # 2-hr per-title cap
            env={**os.environ},
        )
        ok = p.returncode == 0
    except subprocess.TimeoutExpired:
        return {
            "title": title,
            "ok": False,
            "halt_reason": "timeout (>2hr)",
            "wall_seconds": time.time() - t_start,
        }
    except Exception as e:
        return {
            "title": title,
            "ok": False,
            "halt_reason": f"subprocess exception: {e}",
            "wall_seconds": time.time() - t_start,
        }

    out = p.stdout
    err = p.stderr
    stage1 = parse_stage1_stats(out) or {}
    rates = parse_stage2_throughput(out)
    stage2_total = parse_stage2_total(out)
    wall = time.time() - t_start

    checkpoint = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "ok": ok,
        "exit_code": p.returncode,
        "wall_seconds": wall,
        "stage1": {
            "chapters_processed": stage1.get("chapters_processed"),
            "sections_ingested":  stage1.get("sections_ingested"),
            "chunks_created":     stage1.get("chunks_created"),
            "history_rows":       stage1.get("history_rows"),
            "notes_rows":         stage1.get("notes_rows"),
            "xref_rows":          stage1.get("xref_rows"),
            "reserved_chapters":  stage1.get("reserved_chapters"),
            "errors":             stage1.get("errors", []),
            "stage1_seconds":     stage1.get("stage1_seconds"),
        },
        "stage2": {
            "chunks_embedded": stage2_total,
            "batch_rates":     rates,
            "min_rate":        min(rates) if rates else None,
            "avg_rate":        sum(rates) / len(rates) if rates else None,
        },
    }

    # Halt-trigger evaluation
    halt_reason = None

    # 1. Subprocess fail
    if not ok:
        halt_reason = f"subprocess exit code {p.returncode}; stderr tail: {err[-500:]!r}"
    # 2. Errors in stage1 (CHECK rejections etc.)
    elif stage1.get("errors"):
        halt_reason = f"stage1 errors: {len(stage1['errors'])} sections rejected; first error: {stage1['errors'][0]}"
    # 3. Throughput floor — 5+ consecutive batches below 0.5 chunks/s
    elif rates:
        consec = 0; max_consec = 0
        for r in rates:
            if r < THROUGHPUT_FLOOR:
                consec += 1; max_consec = max(max_consec, consec)
            else:
                consec = 0
        if max_consec >= THROUGHPUT_FLOOR_RUN:
            halt_reason = f"throughput floor: {max_consec} consecutive batches below {THROUGHPUT_FLOOR} chunks/s"

    if halt_reason:
        checkpoint["halt_reason"] = halt_reason

    return checkpoint


def pattern_b_canary(title: str) -> dict:
    """Stage-1-only canary: verify Pattern (b) SQL citation lookup works on
    a randomly-sampled section from the just-ingested title. Pattern (a)
    and (c) are skipped during Stage-1-only mode (they require embeddings).
    """
    import psycopg2
    DSN = os.environ.get("DATABASE_URL",
                         "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws")
    out = {"method": "pattern_b_sql_citation_lookup", "title": title,
           "cite": None, "verified": False, "error": None}
    try:
        conn = psycopg2.connect(DSN); conn.autocommit = True
        with conn.cursor() as cur:
            # Pick a random section from the title (any with non-empty body)
            cur.execute("""
                SELECT canonical_citation FROM rcw.sections
                WHERE title_number = %s
                  AND full_text IS NOT NULL AND full_text <> ''
                ORDER BY random() LIMIT 1
            """, (title,))
            row = cur.fetchone()
            if not row:
                out["error"] = "no live sections found for sampling"
                conn.close()
                return out
            cite = row[0]
            out["cite"] = cite
            # Pattern (b) deterministic SQL citation lookup
            cur.execute("""
                SELECT d.id FROM rcw.sections s
                JOIN core.documents d ON d.id = s.document_id
                WHERE s.canonical_citation = %s LIMIT 1
            """, (cite,))
            out["verified"] = cur.fetchone() is not None
        conn.close()
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-from", default=None,
                    help="Resume from this title (skip everything before in the long-tail list)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Stop after this many titles (debugging)")
    ap.add_argument("--stage1-only", action="store_true",
                    help="Skip Stage 2 (embedding); ingest structure only. "
                         "Pattern (b) canary still runs per title; Pattern (a)/(c) deferred.")
    args = ap.parse_args()

    log(f"=== Phase 4 Long-Tail Orchestrator START ===")
    log(f"Mode: {'STAGE-1-ONLY (deferred embedding)' if args.stage1_only else 'FULL (Stage 1 + 2)'}")
    log(f"Long-tail titles to process: {len(LONG_TAIL_TITLES)}")

    projections = load_phase3_projections()
    log(f"Loaded {len(projections)} per-title section projections from Phase 3")

    todo = LONG_TAIL_TITLES
    if args.start_from:
        try:
            i = todo.index(args.start_from)
            todo = todo[i:]
            log(f"Resuming from title {args.start_from} (index {i})")
        except ValueError:
            log(f"FATAL: --start-from {args.start_from} not in long-tail list")
            sys.exit(2)
    if args.limit:
        todo = todo[: args.limit]
        log(f"Limited to first {args.limit} titles")

    cumulative = {
        "titles_done": 0,
        "titles_halted": 0,
        "sections_total": 0,
        "chunks_total": 0,
        "wall_seconds_total": 0.0,
    }
    last_checkpoint_ts = time.time()
    last_checkpoint_count = 0

    for idx, title in enumerate(todo):
        if HALT_FLAG.exists():
            log(f"HALT-FLAG detected at title {title} (idx {idx}/{len(todo)})")
            write_progress(f"HALTED-FLAG at title={title} done={cumulative['titles_done']}")
            break

        write_progress(
            f"In-flight title={title} ({idx+1}/{len(todo)}) "
            f"done={cumulative['titles_done']} sections={cumulative['sections_total']} "
            f"chunks={cumulative['chunks_total']} elapsed_min={(time.time()-t0_overall)/60:.1f}"
        )
        cp = ingest_title(title, stage1_only=args.stage1_only)
        cp["stage1_only_mode"] = args.stage1_only

        # Section count vs Phase 3 projection
        proj = projections.get(title)
        actual = cp["stage1"]["sections_ingested"] or 0
        if proj is not None and proj > 0:
            deviation = abs(actual - proj) / proj * 100
            cp["section_count_check"] = {
                "projected": proj, "actual": actual, "deviation_pct": deviation,
            }
            if deviation > SECTION_DEVIATION_PCT and not cp.get("halt_reason"):
                cp["halt_reason"] = (
                    f"section count deviation {deviation:.1f}% > {SECTION_DEVIATION_PCT}% "
                    f"(projected {proj}, actual {actual})"
                )

        # Pattern (b) canary — runs in BOTH full and stage1-only modes
        # (deterministic SQL exact-match, no embeddings required).
        # Pattern (a)/(c) deferred when stage1_only (embeddings absent).
        if not cp.get("halt_reason") and (cp["stage1"].get("sections_ingested") or 0) > 0:
            canary = pattern_b_canary(title)
            cp["canary_pattern_b"] = canary
            if args.stage1_only:
                cp["canary_pattern_a_c"] = "DEFERRED — Stage 2 embedding pending"
            if canary.get("error"):
                cp["halt_reason"] = f"Pattern (b) canary FAIL: {canary['error']}"
            elif not canary.get("verified"):
                cp["halt_reason"] = (
                    f"Pattern (b) canary FAIL: SQL citation lookup for "
                    f"{canary.get('cite')!r} did not return a row"
                )

        # Append to checkpoint JSONL
        with open(CHECKPOINTS, "a") as fh:
            fh.write(json.dumps(cp) + "\n")

        if cp.get("halt_reason"):
            cumulative["titles_halted"] += 1
            log(f"HALT title={title}: {cp['halt_reason']}")
            log(f"Stopping orchestrator. Operator review required.")
            write_progress(f"HALTED title={title} reason={cp['halt_reason'][:200]}")
            break

        cumulative["titles_done"] += 1
        cumulative["sections_total"] += actual
        cumulative["chunks_total"]   += cp["stage1"]["chunks_created"] or 0
        cumulative["wall_seconds_total"] += cp["wall_seconds"]
        log(
            f"DONE title={title} sections={actual} chunks={cp['stage1']['chunks_created']} "
            f"wall={cp['wall_seconds']:.1f}s "
            f"({idx+1}/{len(todo)})"
        )

        # Cumulative checkpoint
        now = time.time()
        elapsed_since_last = now - last_checkpoint_ts
        titles_since_last  = cumulative["titles_done"] - last_checkpoint_count
        if titles_since_last >= CHECKPOINT_EVERY_TITLES or elapsed_since_last >= CHECKPOINT_EVERY_SECONDS:
            log(
                f"=== CUMULATIVE CHECKPOINT ===\n"
                f"  titles done: {cumulative['titles_done']}\n"
                f"  sections:    {cumulative['sections_total']}\n"
                f"  chunks:      {cumulative['chunks_total']}\n"
                f"  wall (sum):  {cumulative['wall_seconds_total']/60:.1f} min\n"
                f"  pace:        {cumulative['titles_done']/((now-t0_overall)/60):.2f} titles/min"
            )
            last_checkpoint_ts = now
            last_checkpoint_count = cumulative["titles_done"]

    # Final summary
    log(f"=== Phase 4 Long-Tail FINAL ===")
    log(f"  titles done:    {cumulative['titles_done']} / {len(todo)}")
    log(f"  titles halted:  {cumulative['titles_halted']}")
    log(f"  sections added: {cumulative['sections_total']}")
    log(f"  chunks added:   {cumulative['chunks_total']}")
    log(f"  total wall:     {(time.time()-t0_overall)/60:.1f} min")

    write_progress(
        f"COMPLETE titles={cumulative['titles_done']} halted={cumulative['titles_halted']} "
        f"sections={cumulative['sections_total']} chunks={cumulative['chunks_total']}"
    )


if __name__ == "__main__":
    t0_overall = time.time()
    main()
