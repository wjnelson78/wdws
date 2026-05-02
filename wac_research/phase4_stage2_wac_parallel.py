"""
WAC Phase 4 Stage 2 PARALLEL Backfill — adapted from RCW phase4_stage2_backfill_parallel.py.

Spawns N=2 worker processes, each routed to a different HF embedding endpoint:
  worker 0 → HF_ENDPOINT_URL_PRIMARY  (default: http://172.16.81.187:9098, base URL — embedding_service appends /embed)
  worker 1 → HF_ENDPOINT_URL_SECOND   (default: http://172.16.81.187:9099, base URL)

This routes the two GPUs in parallel against the unembedded chunk queue.
Coordination via Postgres FOR UPDATE SKIP LOCKED — no double-embedding.

Usage:
    python3 phase4_stage2_wac_parallel.py                # backfill ALL unembedded WAC chunks
    python3 phase4_stage2_wac_parallel.py --title 162    # scope to one title

Env:
    HF_ENDPOINT_URL_PRIMARY    worker 0 endpoint (default: .187)
    HF_ENDPOINT_URL_SECOND     worker 1 endpoint (default: .113)
    DATABASE_URL               postgres connection string
"""
from __future__ import annotations

import argparse, json, math, os, subprocess, sys, time
from datetime import datetime, timezone
from multiprocessing import Process
from pathlib import Path

ROOT = Path("/opt/wdws/wac_research")
LOG = ROOT / "phase4_stage2_wac_parallel.log"
PROGRESS = ROOT / "phase4_stage2_wac_parallel.progress"
HALT_FLAG = ROOT / "phase4_stage2_wac_parallel.HALT"
WORKER_PROGRESS_DIR = ROOT / "stage2_wac_worker_progress"
WORKER_PROGRESS_DIR.mkdir(exist_ok=True)

DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://wdws:NEL2233obs@127.0.0.1:5432/wdws",
)

# Per-worker endpoint routing.
# IMPORTANT: HF_ENDPOINT_URL is the BASE URL — embedding_service.py constructs
# ${url}/embed internally. Do NOT include the /embed suffix here.
HF_PRIMARY = os.environ.get("HF_ENDPOINT_URL_PRIMARY", "http://172.16.81.187:9098")
HF_SECOND  = os.environ.get("HF_ENDPOINT_URL_SECOND",  "http://172.16.81.187:9099")
WORKER_ENDPOINTS = [HF_PRIMARY, HF_SECOND]

N_WORKERS = 2
OMP_THREADS_PER_WORKER = 10
EMBED_BATCH_SIZE = 64
THROUGHPUT_FLOOR = 0.10
THROUGHPUT_FLOOR_RUN = 5
HF_SLOW_BATCH_SEC = 12.0
HF_SLOW_BATCH_RUN = 10
EQUIV_THRESHOLD = 0.9999

# Title scope — set via CLI arg, propagated to workers
TITLE_FILTER = ""


# -----------------------------------------------------------------------------
# Shared logging
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
    f = WORKER_PROGRESS_DIR / f"worker_{worker_id}.json"
    payload["ts"] = datetime.now(timezone.utc).isoformat()
    with open(f, "w") as fh:
        json.dump(payload, fh)


def aggregate_worker_progress() -> dict:
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
# Worker function (spawned process)
# -----------------------------------------------------------------------------

def worker_fn(worker_id: int, omp_threads: int, endpoint_url: str, title_filter: str):
    """Worker loop with per-worker HF endpoint routing."""
    os.environ["OMP_NUM_THREADS"] = str(omp_threads)
    os.environ["MKL_NUM_THREADS"] = str(omp_threads)
    os.environ["OPENBLAS_NUM_THREADS"] = str(omp_threads)
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    # Per-worker endpoint routing — set BEFORE importing embedding_service
    os.environ["HF_ENDPOINT_URL"] = endpoint_url
    # Disable fallback (we want true parallel, no cross-worker fallback)
    os.environ.pop("HF_ENDPOINT_URL_FALLBACK", None)

    sys.path.insert(0, "/opt/wdws")
    from embedding_service import embed_texts_sync
    import psycopg2
    import psycopg2.extras as extras

    log(f"started — endpoint={endpoint_url} OMP={omp_threads} pid={os.getpid()}",
        worker=worker_id)
    write_worker_progress(worker_id, {
        "worker": worker_id, "status": "starting",
        "endpoint": endpoint_url,
        "embedded": 0, "batches": 0, "pid": os.getpid(),
    })

    conn = psycopg2.connect(DSN); conn.autocommit = False
    consecutive_floor = 0
    consecutive_slow_hf = 0
    embedded = 0
    batches = 0
    t_start = time.time()

    title_clause = ""
    title_args = []
    if title_filter:
        title_clause = " AND s.title_number = %s"
        title_args = [title_filter]

    while True:
        if HALT_FLAG.exists():
            log(f"halt-flag detected — exiting cleanly", worker=worker_id)
            break

        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT c.id, c.embedded_content
                    FROM core.document_chunks c
                    JOIN wac.sections s ON s.document_id = c.document_id
                    WHERE c.embedding IS NULL
                      {title_clause}
                    ORDER BY c.id
                    FOR UPDATE OF c SKIP LOCKED
                    LIMIT %s
                """, [*title_args, EMBED_BATCH_SIZE])
                rows = cur.fetchall()
        except Exception as e:
            log(f"psql claim error: {type(e).__name__}: {e}", worker=worker_id)
            HALT_FLAG.write_text(f"worker {worker_id} psql claim error: {e}\n")
            conn.close()
            return

        if not rows:
            conn.commit()
            log(f"no more chunks — done (embedded={embedded}, batches={batches})",
                worker=worker_id)
            break

        ids = [r[0] for r in rows]
        texts = [r[1] for r in rows]

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

        for cid, vec in zip(ids, vecs):
            if len(vec) != 1024:
                log(f"BATCH FAIL: vector dim {len(vec)} != 1024 for chunk {cid}",
                    worker=worker_id)
                HALT_FLAG.write_text(f"worker {worker_id} dim mismatch at {cid}\n")
                conn.rollback()
                conn.close()
                return

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
            conn.commit()
        except Exception as e:
            log(f"persist error: {type(e).__name__}: {e}", worker=worker_id)
            HALT_FLAG.write_text(f"worker {worker_id} persist error: {e}\n")
            conn.rollback()
            conn.close()
            return

        embedded += len(rows)
        batches += 1

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

        write_worker_progress(worker_id, {
            "worker": worker_id, "status": "running",
            "endpoint": endpoint_url,
            "embedded": embedded, "batches": batches,
            "last_rate": rate, "last_batch_elapsed": batch_elapsed,
            "elapsed_min": (time.time() - t_start) / 60,
        })

        if batches % 10 == 0:
            elapsed = time.time() - t_start
            avg_rate = embedded / elapsed if elapsed > 0 else 0
            log(f"batch {batches}: embedded={embedded} avg_rate={avg_rate:.2f}/s "
                f"last={rate:.2f}/s consec_floor={consecutive_floor}",
                worker=worker_id)

    elapsed = time.time() - t_start
    write_worker_progress(worker_id, {
        "worker": worker_id, "status": "done",
        "endpoint": endpoint_url,
        "embedded": embedded, "batches": batches,
        "elapsed_min": elapsed / 60,
    })
    log(f"DONE — endpoint={endpoint_url} embedded={embedded} batches={batches} elapsed={elapsed/60:.1f}min",
        worker=worker_id)
    conn.close()


# -----------------------------------------------------------------------------
# Main aggregator
# -----------------------------------------------------------------------------

def get_pending_count(title_filter: str = "") -> int:
    import psycopg2
    conn = psycopg2.connect(DSN); conn.autocommit = True
    sql = """
        SELECT COUNT(*) FROM core.document_chunks c
        JOIN wac.sections s ON s.document_id = c.document_id
        WHERE c.embedding IS NULL
    """
    args = []
    if title_filter:
        sql += " AND s.title_number = %s"
        args = [title_filter]
    with conn.cursor() as cur:
        cur.execute(sql, args)
        n = cur.fetchone()[0]
    conn.close()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", default="", help="Scope to single WAC title (optional)")
    args = ap.parse_args()

    title_filter = args.title

    import multiprocessing as mp
    mp.set_start_method("spawn", force=True)

    log("=== WAC Phase 4 Stage 2 PARALLEL Backfill START ===")
    log(f"  N_WORKERS={N_WORKERS}, endpoints: {WORKER_ENDPOINTS}")
    if title_filter:
        log(f"  scope: title {title_filter}")
    else:
        log(f"  scope: all unembedded WAC chunks")

    if HALT_FLAG.exists():
        log("stale HALT_FLAG present — removing")
        HALT_FLAG.unlink()
    for f in WORKER_PROGRESS_DIR.glob("worker_*.json"):
        f.unlink()

    total_pending = get_pending_count(title_filter)
    log(f"pending chunks: {total_pending}")
    if total_pending == 0:
        log("no chunks pending — exit clean")
        sys.exit(0)

    # Spawn N workers, each with its own endpoint
    log(f"spawning {N_WORKERS} workers")
    procs = []
    for i in range(N_WORKERS):
        p = Process(target=worker_fn,
                    args=(i, OMP_THREADS_PER_WORKER, WORKER_ENDPOINTS[i], title_filter),
                    name=f"wac-stage2-w{i}")
        p.start()
        procs.append(p)

    log(f"all {N_WORKERS} workers spawned; entering aggregation loop")
    write_main_progress(f"BACKFILL_RUNNING workers={N_WORKERS} pending={total_pending}")

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

        if time.time() - last_summary_ts >= 60:
            log(f"AGGREGATE: embedded={embedded}/{total_pending} ({pct:.1f}%) "
                f"active={active} agg_rate={agg_rate:.2f}/s "
                f"eta={eta_sec/60:.1f}min")
            write_main_progress(
                f"BACKFILL embedded={embedded}/{total_pending} ({pct:.1f}%) "
                f"workers_active={active} agg_rate={agg_rate:.2f}/s "
                f"eta_min={eta_sec/60:.1f}"
            )
            last_summary_ts = time.time()

        for milestone in (25, 50, 75):
            if milestone not in milestones_emitted and pct >= milestone:
                milestones_emitted.add(milestone)
                log(f"=== MILESTONE-{milestone}% === embedded={embedded} "
                    f"agg_rate={agg_rate:.2f}/s eta={eta_sec/60:.1f}min")

    log("all workers stopped — joining")
    for p in procs:
        p.join(timeout=60)
        log(f"  worker {p.name}: exitcode={p.exitcode}")

    if HALT_FLAG.exists():
        reason = HALT_FLAG.read_text().strip()
        log(f"HALT_FLAG present — backfill halted: {reason}")
        write_main_progress(f"BACKFILL_HALTED reason={reason[:200]}")
        sys.exit(2)

    final_pending = get_pending_count(title_filter)
    if final_pending > 0:
        log(f"WARNING: pending count = {final_pending} > 0")
        write_main_progress(f"BACKFILL_INCOMPLETE final_pending={final_pending}")
        sys.exit(4)

    log(f"=== BACKFILL COMPLETE === final_pending=0")
    write_main_progress(f"BACKFILL_COMPLETE")


if __name__ == "__main__":
    main()
