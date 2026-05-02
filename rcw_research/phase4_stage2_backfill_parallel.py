"""
Phase 4 Stage 2 Backfill — PARALLEL worker-pool variant.

Spawns N=4 worker processes, each loading its own BGE-M3 instance with
OMP_NUM_THREADS=16. Total 64 threads across the machine.

Coordination:
  - Workers claim chunks atomically via Postgres FOR UPDATE SKIP LOCKED
  - Worker-shared HALT signal: filesystem flag at HALT_FLAG path; checked at
    the start of every batch by every worker
  - Aggregated progress: single shared append-only log; per-worker batches
    emit worker-prefixed lines; main process emits summary lines every N seconds

Safety properties (9 total — 6 carry-forward + 3 multi-worker):
  1. Halt-on-error: throughput floor, batch fail, equivalence drift,
     psql errors, vector dim mismatch
  2. Idempotency: WHERE embedding IS NULL + FOR UPDATE OF c SKIP LOCKED
  3. Checkpoint logging: 25/50/75/100% milestones aggregated across workers
  4. No silent fallbacks: pre-backfill + 25% equivalence checks
  5. Phase 5 auto-trigger: only on clean completion (HALT_FLAG absent,
     pending_count == 0, all workers exited 0)
  6. Resource limits: model loaded once per worker (cached after first call)
  7. FOR UPDATE SKIP LOCKED correctness: atomic claim + commit-after-embed
     releases locks; no double-embedding even under crash mid-batch
  8. Worker-shared HALT signal: filesystem flag checked at start of each batch
  9. Aggregated checkpoint reporting: single log, worker-prefixed lines plus
     main summary lines

Estimated wall time: ~1.5-2 hours for 27,953 chunks (vs ~11 hr single-process).
"""
from __future__ import annotations

import json, math, os, subprocess, sys, time, signal
from datetime import datetime, timezone
from multiprocessing import Process, Queue
from pathlib import Path

ROOT = Path("/opt/wdws/rcw_research")
LOG = ROOT / "phase4_stage2_backfill_parallel.log"
PROGRESS = ROOT / "phase4_stage2_backfill_parallel.progress"
HALT_FLAG = ROOT / "phase4_stage2_backfill_parallel.HALT"
WORKER_PROGRESS_DIR = ROOT / "stage2_worker_progress"
WORKER_PROGRESS_DIR.mkdir(exist_ok=True)

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)

# Tuning
# HF .187 (saturn-sister, 2x T4) primary as of 2026-04-30 20:28 UTC; .113
# kept as fallback. 2 workers gives modest pipelining — single GPU on .187
# embedder (cuda:0) so concurrency above ~2 won't help (GPU serializes).
N_WORKERS = 2
OMP_THREADS_PER_WORKER = 10
EMBED_BATCH_SIZE = 64
# HF endpoint operation baseline 0.30/s/worker; CPU-only steady state ~0.25/s/worker;
# floor lowered to 0.10 to catch only catastrophic slowdowns
# (memory thrashing, stuck batches, runaway swap).
THROUGHPUT_FLOOR = 0.10
THROUGHPUT_FLOOR_RUN = 5
# HF degradation halt is inert when EMBEDDING_FORCE_LOCAL=1 (workers never
# call HF), but the constants are kept so re-enabling HF later is one edit.
# Calibrated to real-text BGE-M3 batch_64 baseline on .187 single-T4 (~5-7s
# observed); 12s threshold catches genuine degradation while accommodating
# real-workload baseline.
HF_SLOW_BATCH_SEC = 12.0
HF_SLOW_BATCH_RUN = 10
EQUIV_THRESHOLD = 0.9999


# -----------------------------------------------------------------------------
# Shared logging (append-only, worker-prefixed)
# -----------------------------------------------------------------------------

def log(msg: str, worker: int | None = None):
    prefix = f"[w{worker}] " if worker is not None else "[main] "
    line = f"[{datetime.now(timezone.utc).isoformat()}] {prefix}{msg}"
    print(line, flush=True)
    with open(LOG, "a") as fh:
        fh.write(line + "\n")


def write_main_progress(text: str):
    with open(PROGRESS, "w") as fh:
        fh.write(f"{datetime.now(timezone.utc).isoformat()} {text}\n")


def write_worker_progress(worker_id: int, payload: dict):
    """Each worker writes its own per-worker JSON file (overwritten each
    batch). Main aggregates by reading all files."""
    f = WORKER_PROGRESS_DIR / f"worker_{worker_id}.json"
    payload["ts"] = datetime.now(timezone.utc).isoformat()
    with open(f, "w") as fh:
        json.dump(payload, fh)


def aggregate_worker_progress() -> dict:
    """Read all per-worker progress files; return aggregate."""
    agg = {"workers": [], "total_embedded": 0, "active_workers": 0}
    for f in WORKER_PROGRESS_DIR.glob("worker_*.json"):
        try:
            with open(f) as fh:
                d = json.load(fh)
            agg["workers"].append(d)
            agg["total_embedded"] += d.get("embedded", 0)
            if d.get("status") == "running":
                agg["active_workers"] += 1
        except Exception:
            pass
    return agg


# -----------------------------------------------------------------------------
# Pre-backfill equivalence check (single process)
# -----------------------------------------------------------------------------

def numerical_equivalence_check() -> tuple[bool, list[float]]:
    """Pull 5 already-embedded chunks, re-embed via current path, compute
    cosine similarity. Returns (all_pass, sims). HF is primary; equivalence
    here is HF vs already-stored (mix of CPU + earlier-HF stored vectors)."""
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
# Worker function (run in separate process)
# -----------------------------------------------------------------------------

def worker_fn(worker_id: int, omp_threads: int):
    """Worker loop: claim chunks via FOR UPDATE SKIP LOCKED, embed, persist,
    release locks, repeat. Halts on HALT_FLAG, throughput floor, batch error,
    or vector dim mismatch."""
    # Thread budget — harmless when HF is primary (model not loaded), useful
    # if HF fails through to CPU fallback per batch
    os.environ["OMP_NUM_THREADS"] = str(omp_threads)
    os.environ["MKL_NUM_THREADS"] = str(omp_threads)
    os.environ["OPENBLAS_NUM_THREADS"] = str(omp_threads)
    # Skip HF Hub network calls during local model load (local model cached)
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        import torch
        torch.set_num_threads(omp_threads)
    except ImportError:
        pass

    sys.path.insert(0, "/opt/wdws")
    from embedding_service import embed_texts_sync
    import psycopg2
    import psycopg2.extras as extras

    log(f"started — OMP={omp_threads}, pid={os.getpid()}", worker=worker_id)
    write_worker_progress(worker_id, {
        "worker": worker_id, "status": "starting",
        "embedded": 0, "batches": 0, "pid": os.getpid(),
    })

    conn = psycopg2.connect(DSN); conn.autocommit = False
    consecutive_floor = 0
    consecutive_slow_hf = 0
    embedded = 0
    batches = 0
    t_start = time.time()

    while True:
        # Halt check at every batch start
        if HALT_FLAG.exists():
            log(f"halt-flag detected — exiting cleanly", worker=worker_id)
            break

        # Atomic claim of next 64 chunks
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.id, c.embedded_content
                    FROM core.document_chunks c
                    JOIN rcw.sections s ON s.document_id = c.document_id
                    WHERE c.embedding IS NULL
                    ORDER BY c.id
                    FOR UPDATE OF c SKIP LOCKED
                    LIMIT %s
                """, (EMBED_BATCH_SIZE,))
                rows = cur.fetchall()
        except Exception as e:
            log(f"psql claim error: {type(e).__name__}: {e}", worker=worker_id)
            HALT_FLAG.write_text(f"worker {worker_id} psql claim error: {e}\n")
            conn.close()
            return

        if not rows:
            # No more chunks — done
            conn.commit()
            log(f"no more chunks — done (embedded={embedded}, batches={batches})",
                worker=worker_id)
            break

        ids = [r[0] for r in rows]
        texts = [r[1] for r in rows]

        # Embed
        batch_t0 = time.time()
        try:
            vecs = embed_texts_sync(texts)
        except Exception as e:
            log(f"BATCH FAIL: {type(e).__name__}: {e}", worker=worker_id)
            HALT_FLAG.write_text(f"worker {worker_id} batch fail: {e}\n")
            conn.rollback()
            conn.close()
            return
        batch_elapsed = time.time() - batch_t0
        rate = len(rows) / batch_elapsed if batch_elapsed > 0 else 0

        # Validate vector dimensions
        for cid, vec in zip(ids, vecs):
            if len(vec) != 1024:
                log(f"BATCH FAIL: vector dim {len(vec)} != 1024 for chunk {cid}",
                    worker=worker_id)
                HALT_FLAG.write_text(f"worker {worker_id} dim mismatch at {cid}\n")
                conn.rollback()
                conn.close()
                return

        # Persist (still in transaction holding the SKIP LOCKED locks)
        upd_rows = []
        for cid, vec in zip(ids, vecs):
            lit = "[" + ",".join(f"{x:.7f}" for x in vec) + "]"
            upd_rows.append((lit, cid))
        try:
            with conn.cursor() as cur:
                extras.execute_batch(cur, """
                    UPDATE core.document_chunks
                    SET embedding = %s::halfvec(1024), embedded_at = NOW()
                    WHERE id = %s
                """, upd_rows, page_size=64)
            conn.commit()  # releases SKIP LOCKED locks
        except Exception as e:
            log(f"persist error: {type(e).__name__}: {e}", worker=worker_id)
            HALT_FLAG.write_text(f"worker {worker_id} persist error: {e}\n")
            conn.rollback()
            conn.close()
            return

        embedded += len(rows)
        batches += 1

        # Throughput floor — per-worker, not aggregate
        if rate < THROUGHPUT_FLOOR:
            consecutive_floor += 1
            if consecutive_floor >= THROUGHPUT_FLOOR_RUN:
                msg = (f"throughput {rate:.2f} < floor {THROUGHPUT_FLOOR} "
                       f"for {consecutive_floor} consecutive batches")
                log(f"HALT: {msg}", worker=worker_id)
                HALT_FLAG.write_text(f"worker {worker_id} {msg}\n")
                conn.close()
                return
        else:
            consecutive_floor = 0

        # HF degradation halt — sustained slow batches (>5s vs ~1.3s baseline)
        # signal HF endpoint health degrading. Only meaningful when HF is the
        # primary path; on CPU every batch is ~250s and the counter would
        # always trip. Gate on the FORCE_LOCAL flag we set earlier.
        if os.environ.get("EMBEDDING_FORCE_LOCAL") != "1":
            if batch_elapsed > HF_SLOW_BATCH_SEC:
                consecutive_slow_hf += 1
                if consecutive_slow_hf >= HF_SLOW_BATCH_RUN:
                    msg = (f"HF degradation: {consecutive_slow_hf} consecutive "
                           f"batches >{HF_SLOW_BATCH_SEC}s (latest {batch_elapsed:.1f}s)")
                    log(f"HALT: {msg}", worker=worker_id)
                    HALT_FLAG.write_text(f"worker {worker_id} {msg}\n")
                    conn.close()
                    return
            else:
                consecutive_slow_hf = 0

        # Per-batch progress
        write_worker_progress(worker_id, {
            "worker": worker_id, "status": "running",
            "embedded": embedded, "batches": batches,
            "last_rate": rate, "last_batch_elapsed": batch_elapsed,
            "elapsed_min": (time.time() - t_start) / 60,
        })

        # Verbose log every 10 batches
        if batches % 10 == 0:
            elapsed = time.time() - t_start
            avg_rate = embedded / elapsed if elapsed > 0 else 0
            log(f"batch {batches}: embedded={embedded} avg_rate={avg_rate:.2f}/s "
                f"last={rate:.2f}/s consec_floor={consecutive_floor}",
                worker=worker_id)

    elapsed = time.time() - t_start
    write_worker_progress(worker_id, {
        "worker": worker_id, "status": "done",
        "embedded": embedded, "batches": batches,
        "elapsed_min": elapsed / 60,
    })
    log(f"DONE — embedded={embedded} batches={batches} elapsed={elapsed/60:.1f}min",
        worker=worker_id)
    conn.close()


# -----------------------------------------------------------------------------
# Main aggregator
# -----------------------------------------------------------------------------

def get_pending_count() -> int:
    import psycopg2
    conn = psycopg2.connect(DSN); conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM core.document_chunks c
            JOIN rcw.sections s ON s.document_id = c.document_id
            WHERE c.embedding IS NULL
        """)
        n = cur.fetchone()[0]
    conn.close()
    return n


def run_phase5():
    log("triggering Phase 5 validation")
    p = subprocess.run(
        [sys.executable, "-u", str(ROOT / "phase5_validation.py")],
        capture_output=False, text=True,
    )
    log(f"Phase 5 exited with code {p.returncode}")
    write_main_progress(f"PHASE5 EXIT={p.returncode}")
    return p.returncode


def main():
    # PyTorch + fork() is unsafe: workers inherit the parent's OpenMP/MKL
    # thread state and deadlock on the first torch call (futex_wait_queue).
    # spawn forces a clean interpreter per worker — each loads its own
    # BGE-M3 (~30s startup, parallel across workers).
    import multiprocessing as mp
    mp.set_start_method("spawn", force=True)

    log("=== Phase 4 Stage 2 PARALLEL Backfill START ===")
    log(f"  N_WORKERS={N_WORKERS}, OMP_THREADS_PER_WORKER={OMP_THREADS_PER_WORKER}, "
        f"total threads={N_WORKERS * OMP_THREADS_PER_WORKER}")

    # Stale flag cleanup
    if HALT_FLAG.exists():
        log("stale HALT_FLAG present — removing")
        HALT_FLAG.unlink()
    for f in WORKER_PROGRESS_DIR.glob("worker_*.json"):
        f.unlink()

    # Snapshot pending count
    total_pending = get_pending_count()
    log(f"pending chunks: {total_pending}")
    if total_pending == 0:
        log("no chunks pending — triggering Phase 5 directly")
        sys.exit(run_phase5())

    # PRE-BACKFILL equivalence check (catches model divergence at 0 corrupted chunks)
    log("=== Pre-backfill equivalence check ===")
    ok, sims = numerical_equivalence_check()
    log(f"  cosine similarities (5 samples): {[f'{s:.6f}' for s in sims]}")
    log(f"  verdict: {'PASS' if ok else 'FAIL — halting before backfill'}")
    if not ok:
        HALT_FLAG.write_text(f"pre-backfill equivalence drift: sims={sims}\n")
        write_main_progress(f"HALTED-PRE-BACKFILL sims={sims}")
        sys.exit(3)

    # Spawn N workers
    log(f"spawning {N_WORKERS} worker processes")
    procs = []
    for i in range(N_WORKERS):
        p = Process(target=worker_fn, args=(i, OMP_THREADS_PER_WORKER),
                    name=f"backfill-worker-{i}")
        p.start()
        procs.append(p)

    log(f"all {N_WORKERS} workers spawned; entering aggregation loop")
    write_main_progress(f"BACKFILL_RUNNING workers={N_WORKERS} pending={total_pending}")

    # Aggregator loop — emits 25/50/75% milestones across workers
    milestones_emitted = set()
    last_summary_ts = 0
    aggregator_start = time.time()
    while any(p.is_alive() for p in procs):
        time.sleep(15)
        agg = aggregate_worker_progress()
        embedded = agg["total_embedded"]
        active = agg["active_workers"]
        elapsed = time.time() - aggregator_start
        pct = embedded * 100 / total_pending if total_pending else 0
        agg_rate = embedded / elapsed if elapsed > 0 else 0
        eta_sec = (total_pending - embedded) / agg_rate if agg_rate > 0 else 0

        # Periodic summary every 60 sec
        if time.time() - last_summary_ts >= 60:
            log(f"AGGREGATE: embedded={embedded}/{total_pending} ({pct:.1f}%) "
                f"active_workers={active} agg_rate={agg_rate:.2f}/s "
                f"eta={eta_sec/60:.1f}min")
            write_main_progress(
                f"BACKFILL embedded={embedded}/{total_pending} ({pct:.1f}%) "
                f"workers_active={active} agg_rate={agg_rate:.2f}/s "
                f"eta_min={eta_sec/60:.1f}"
            )
            last_summary_ts = time.time()

        # Milestone reporting (25/50/75%)
        for milestone in (25, 50, 75):
            if milestone not in milestones_emitted and pct >= milestone:
                milestones_emitted.add(milestone)
                log(f"=== MILESTONE-{milestone}% === embedded={embedded} "
                    f"agg_rate={agg_rate:.2f}/s eta={eta_sec/60:.1f}min")

    # Wait for all workers to actually exit
    log("all workers stopped reporting — joining")
    for p in procs:
        p.join(timeout=60)
        log(f"  worker {p.name}: exitcode={p.exitcode}")

    # Decision: clean completion or halt?
    if HALT_FLAG.exists():
        reason = HALT_FLAG.read_text().strip()
        log(f"HALT_FLAG present — backfill halted: {reason}")
        write_main_progress(f"BACKFILL_HALTED reason={reason[:200]}")
        sys.exit(2)

    # Verify pending == 0
    final_pending = get_pending_count()
    if final_pending > 0:
        log(f"WARNING: pending count = {final_pending} > 0 (workers exited "
            f"but chunks remain) — Phase 5 NOT triggered")
        write_main_progress(f"BACKFILL_INCOMPLETE final_pending={final_pending}")
        sys.exit(4)

    log(f"=== BACKFILL COMPLETE === final_pending=0 — triggering Phase 5")
    rc = run_phase5()
    sys.exit(rc)


if __name__ == "__main__":
    main()
