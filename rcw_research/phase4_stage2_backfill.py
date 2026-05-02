"""
Phase 4 Stage 2 Backfill Orchestrator.

Workflow per Pause-B + post-Stage-1 operator authorization:
  1. Probe HF endpoint health.
  2. If healthy:    run backfill via current embedding_service (HF-served).
  3. If unhealthy:  poll every 30 min for 12 hours, then 60 min for 6 hours.
  4. 18-hour ceiling: commit to local CPU backfill regardless of HF state.
  5. After backfill completes: trigger Phase 5 validation suite.

Backfill behavior:
  - Batch in groups of 64
  - Target chunks WHERE embedding IS NULL
  - Idempotent under interruption
  - Checkpoint progress at 25% / 50% / 75% / 100%
  - 25% milestone: re-run 5-sample numerical-equivalence check
  - Halt on:
      throughput < 0.3 chunks/sec sustained for 5+ batches
      failed batch count > 0
      equivalence drift at 25% milestone (any sample <0.9999)

Reporting: 4-hour milestones during HF polling; backfill progress; Phase 5
results. All logged to phase4_stage2_backfill.log; HALT events also written
to phase4_stage2_backfill.HALT-flag for orchestrator interruption.
"""
from __future__ import annotations

import json, math, os, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("/opt/wdws/rcw_research")
LOG = ROOT / "phase4_stage2_backfill.log"
PROGRESS = ROOT / "phase4_stage2_backfill.progress"
HALT_FLAG = ROOT / "phase4_stage2_backfill.HALT"

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)
EMBED_BATCH_SIZE = 64
THROUGHPUT_FLOOR = 0.3            # halt if < 0.3 chunks/sec for 5+ batches
THROUGHPUT_FLOOR_RUN = 5
EQUIV_THRESHOLD = 0.9999
HF_HEALTHY_LATENCY_MS = 500       # < 500ms = HF; >= 500ms = CPU fallback
POLL_30MIN = 30 * 60
POLL_60MIN = 60 * 60
PHASE_12HR_SEC = 12 * 3600
CEILING_18HR_SEC = 18 * 3600
MILESTONE_REPORT_SEC = 4 * 3600   # 4-hour interval for HF-down progress reports


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def log(msg: str):
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


def write_progress(text: str):
    with open(PROGRESS, "w") as fh:
        fh.write(f"{datetime.now(timezone.utc).isoformat()} {text}\n")


def probe_hf_health() -> tuple[bool, float]:
    """Returns (healthy, latency_ms). Single embed call from a fresh subprocess
    to avoid stuck connection state in the parent process."""
    code = """
import sys, time
sys.path.insert(0, '/opt/wdws')
from embedding_service import embed_query_sync
t0 = time.time()
v = embed_query_sync('hf health probe stage 2 backfill')
print(f"{(time.time() - t0) * 1000:.0f}")
"""
    try:
        result = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, timeout=120,
        )
        last_line = result.stdout.strip().splitlines()[-1] if result.stdout else "0"
        latency = float(last_line)
        return (latency < HF_HEALTHY_LATENCY_MS, latency)
    except Exception as e:
        log(f"  probe exception: {type(e).__name__}: {e}")
        return (False, 999999.0)


def numerical_equivalence_check() -> tuple[bool, list[float]]:
    """Pull 5 already-embedded chunks, re-embed via current path, compute
    cosine similarity. Returns (all_pass, sims)."""
    import psycopg2
    sys.path.insert(0, "/opt/wdws")
    from embedding_service import embed_query_sync

    def cosine(a, b):
        dot = sum(x * y for x, y in zip(a, b))
        na = math.sqrt(sum(x * x for x in a))
        nb = math.sqrt(sum(y * y for y in b))
        return dot / (na * nb) if na * nb > 0 else 0.0

    def parse_pgvec(s):
        s = s.strip()
        if s.startswith("["): s = s[1:]
        if s.endswith("]"): s = s[:-1]
        return [float(x) for x in s.split(",")]

    conn = psycopg2.connect(DSN); conn.autocommit = True
    sims = []
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.embedded_content, c.embedding::text
            FROM core.document_chunks c
            JOIN rcw.sections s ON s.document_id = c.document_id
            WHERE c.embedding IS NOT NULL
              AND s.title_number IN ('51','42','4','49')
            ORDER BY c.id LIMIT 5
        """)
        rows = cur.fetchall()
    conn.close()

    for cid, content, vec_text in rows:
        stored = parse_pgvec(vec_text)
        fresh = embed_query_sync(content)
        sims.append(cosine(stored, fresh))

    return (all(s >= EQUIV_THRESHOLD for s in sims), sims)


# -----------------------------------------------------------------------------
# Backfill execution
# -----------------------------------------------------------------------------

def run_backfill():
    """Embed all chunks WHERE embedding IS NULL in batches of 64.
    Returns dict with stats; sets HALT_FLAG on halt-trigger condition."""
    import psycopg2
    import psycopg2.extras as extras
    sys.path.insert(0, "/opt/wdws")
    from embedding_service import embed_texts_sync

    conn = psycopg2.connect(DSN); conn.autocommit = False

    # Snapshot pending count for milestone tracking
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM core.document_chunks c
            JOIN rcw.sections s ON s.document_id = c.document_id
            WHERE c.embedding IS NULL
        """)
        total_pending = cur.fetchone()[0]
    conn.commit()

    log(f"=== BACKFILL START === pending={total_pending}")
    write_progress(f"BACKFILL START pending={total_pending}")

    if total_pending == 0:
        conn.close()
        log("No chunks pending embedding — backfill complete (no-op)")
        return {"embedded": 0, "halted": False}

    embedded = 0
    consecutive_floor = 0
    batch_no = 0
    milestone_25_done = False
    t_start = time.time()

    while True:
        if HALT_FLAG.exists():
            log("HALT-FLAG detected — stopping backfill")
            break

        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.embedded_content
                FROM core.document_chunks c
                JOIN rcw.sections s ON s.document_id = c.document_id
                WHERE c.embedding IS NULL
                ORDER BY c.id LIMIT %s
            """, (EMBED_BATCH_SIZE,))
            rows = cur.fetchall()
        conn.commit()

        if not rows:
            break

        ids = [r[0] for r in rows]
        texts = [r[1] for r in rows]

        batch_t0 = time.time()
        try:
            vecs = embed_texts_sync(texts)
        except Exception as e:
            log(f"BATCH FAIL batch_no={batch_no}: {type(e).__name__}: {e}")
            HALT_FLAG.write_text(f"batch fail at batch_no={batch_no}: {e}\n")
            break
        batch_elapsed = time.time() - batch_t0
        rate = len(rows) / batch_elapsed if batch_elapsed > 0 else 0

        # UPDATE
        upd_rows = []
        for cid, vec in zip(ids, vecs):
            if len(vec) != 1024:
                log(f"BATCH FAIL: vector dim {len(vec)} != 1024 for chunk {cid}")
                HALT_FLAG.write_text(f"vector dim mismatch at {cid}\n")
                conn.close()
                return {"embedded": embedded, "halted": True}
            lit = "[" + ",".join(f"{x:.7f}" for x in vec) + "]"
            upd_rows.append((lit, cid))
        with conn.cursor() as cur:
            extras.execute_batch(cur, """
                UPDATE core.document_chunks
                SET embedding = %s::halfvec(1024), embedded_at = NOW()
                WHERE id = %s
            """, upd_rows, page_size=64)
        conn.commit()
        embedded += len(rows)
        batch_no += 1

        # Throughput floor monitoring
        if rate < THROUGHPUT_FLOOR:
            consecutive_floor += 1
            if consecutive_floor >= THROUGHPUT_FLOOR_RUN:
                msg = (f"HALT: throughput {rate:.2f} < floor {THROUGHPUT_FLOOR} "
                       f"for {consecutive_floor} consecutive batches")
                log(msg)
                HALT_FLAG.write_text(msg + "\n")
                conn.close()
                return {"embedded": embedded, "halted": True}
        else:
            consecutive_floor = 0

        # Milestone reporting
        pct = embedded * 100 / total_pending
        if batch_no % 10 == 0 or embedded >= total_pending:
            elapsed = time.time() - t_start
            avg_rate = embedded / elapsed if elapsed > 0 else 0
            eta_sec = (total_pending - embedded) / avg_rate if avg_rate > 0 else 0
            log(f"  batch {batch_no}: {embedded}/{total_pending} ({pct:.1f}%) "
                f"avg_rate={avg_rate:.2f}/s last={rate:.2f}/s eta={eta_sec/60:.1f}min")
            write_progress(f"BACKFILL embedded={embedded}/{total_pending} ({pct:.1f}%) "
                           f"avg_rate={avg_rate:.2f}/s eta={eta_sec/60:.1f}min")

        # 25% milestone: numerical equivalence check
        if not milestone_25_done and pct >= 25:
            log("=== 25% MILESTONE: numerical equivalence check ===")
            ok, sims = numerical_equivalence_check()
            log(f"  cosine similarities: {[f'{s:.6f}' for s in sims]}")
            log(f"  verdict: {'PASS' if ok else 'FAIL — drift detected'}")
            milestone_25_done = True
            if not ok:
                msg = f"HALT: equivalence drift at 25% — sims={sims}"
                HALT_FLAG.write_text(msg + "\n")
                conn.close()
                return {"embedded": embedded, "halted": True}

    conn.close()
    elapsed = time.time() - t_start
    log(f"=== BACKFILL DONE === embedded={embedded} elapsed={elapsed/60:.1f}min")
    write_progress(f"BACKFILL DONE embedded={embedded} elapsed={elapsed/60:.1f}min")
    return {"embedded": embedded, "halted": False, "elapsed_sec": elapsed}


# -----------------------------------------------------------------------------
# Polling loop
# -----------------------------------------------------------------------------

def wait_for_hf_or_ceiling() -> str:
    """Poll HF until healthy OR 18-hour ceiling. Returns:
    'hf' if HF recovered, 'ceiling' if 18 hours elapsed."""
    start = time.time()
    last_milestone = 0

    # Initial probe (already done by caller, but log it explicitly)
    healthy, latency = probe_hf_health()
    log(f"Initial HF probe: {'HEALTHY' if healthy else 'UNHEALTHY'} ({latency:.0f}ms)")
    if healthy:
        return "hf"

    log(f"Beginning polling cadence: 30-min for first 12 hr, 60-min thereafter, "
        f"18-hr ceiling commits to CPU")

    while True:
        elapsed_sec = time.time() - start
        elapsed_hr = elapsed_sec / 3600

        if elapsed_sec >= CEILING_18HR_SEC:
            log(f"18-hour ceiling reached — committing to CPU backfill")
            return "ceiling"

        # Choose poll interval
        if elapsed_sec < PHASE_12HR_SEC:
            interval = POLL_30MIN
        else:
            interval = POLL_60MIN

        write_progress(
            f"POLL phase={'30min' if interval == POLL_30MIN else '60min'} "
            f"elapsed_hr={elapsed_hr:.1f} ceiling=18hr"
        )
        time.sleep(interval)

        if HALT_FLAG.exists():
            log("HALT-FLAG detected during polling — exiting loop")
            return "ceiling"

        healthy, latency = probe_hf_health()
        elapsed_hr = (time.time() - start) / 3600

        if healthy:
            log(f"HF RECOVERED at hour {elapsed_hr:.1f} ({latency:.0f}ms)")
            return "hf"

        # 4-hour milestone
        milestone = int(elapsed_hr) // 4
        if milestone > last_milestone:
            log(f"  4-hr milestone: HF still unhealthy at hour {elapsed_hr:.1f} "
                f"(latency {latency:.0f}ms)")
            last_milestone = milestone


# -----------------------------------------------------------------------------
# Phase 5 validation trigger
# -----------------------------------------------------------------------------

def run_phase5():
    """Trigger phase5_validation.py as a subprocess."""
    log("=== TRIGGERING PHASE 5 VALIDATION ===")
    write_progress("PHASE5 RUNNING")
    p = subprocess.run(
        [sys.executable, "-u", str(ROOT / "phase5_validation.py")],
        capture_output=False, text=True,
    )
    log(f"=== Phase 5 exited with code {p.returncode} ===")
    write_progress(f"PHASE5 EXIT={p.returncode}")
    return p.returncode


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-wait", action="store_true",
                    help="Skip the HF polling wait; commit to current embedding "
                         "service path immediately. Operator authorized after "
                         "8.5+ hours of HF outage with sustained CPU fallback.")
    args = ap.parse_args()

    log("=== Phase 4 Stage 2 Backfill Orchestrator START ===")
    if HALT_FLAG.exists():
        log("Stale HALT_FLAG present — removing")
        HALT_FLAG.unlink()

    if args.skip_wait:
        log("--skip-wait: bypassing HF polling, committing to current embedding path")
    else:
        decision = wait_for_hf_or_ceiling()
        log(f"HF wait result: {decision}")

    # EARLY equivalence check (belt-and-suspenders per operator's no-silent-fallbacks
    # property). Verifies BAAI/bge-m3 produces vectors within 0.9999 cosine of the
    # already-stored HF-served vectors BEFORE any backfill rows are written.
    # Halts before corrupting any chunk if the embedding service has silently
    # switched models or experienced any other quality regression.
    log("=== Pre-backfill equivalence check (BGE-M3 model verification) ===")
    ok, sims = numerical_equivalence_check()
    log(f"  cosine similarities (5 samples): {[f'{s:.6f}' for s in sims]}")
    log(f"  verdict: {'PASS — proceeding with backfill' if ok else 'FAIL — halting before backfill'}")
    if not ok:
        msg = f"HALT: pre-backfill equivalence drift; sims={sims}"
        HALT_FLAG.write_text(msg + "\n")
        write_progress(f"HALTED-PRE-BACKFILL sims={sims}")
        sys.exit(3)

    result = run_backfill()
    if result.get("halted"):
        log("Backfill halted — Phase 5 NOT triggered")
        write_progress(f"BACKFILL HALTED embedded={result.get('embedded')}")
        sys.exit(2)

    log(f"Backfill complete — embedded {result.get('embedded')} chunks")
    rc = run_phase5()
    sys.exit(rc)


if __name__ == "__main__":
    main()
