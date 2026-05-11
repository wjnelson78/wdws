#!/bin/bash
# Athena Cognitive Engine — PostgreSQL Backup (v3, two-stage concurrent)
#
# Runs daily via cron. Keeps 2 days of backups.
#
# DESIGN
# ──────
# pg_dump uses synchronized snapshots: ALL parallel workers hold their
# AccessShareLock until the LAST worker finishes. With one big table
# (core.document_chunks ≈ 38 GB of halfvec embeddings) that means a
# full backup holds an FK lock on core.documents for the entire run —
# blocking ALTER TABLE for the whole window. v2 (-Fd -j 8) cut wall
# clock from ~10 h → ~3 h but the DDL block was still ~3 h.
#
# v3 splits the dump into two concurrent stages:
#
#   Stage 1: "main" — everything EXCEPT core.document_chunks data
#     • Includes all schema/DDL (so the chunks table + its indexes
#       are still defined in the dump).
#     • -Fd -j 8 -Z zstd:3, ~10 min, locks released as soon as it
#       commits → core.documents is unlocked for DDL after ~10 min.
#
#   Stage 2: "chunks" — core.document_chunks data only
#     • -Fd -j 4 -Z zstd:3 --section=data --table=core.document_chunks
#     • ~2-3 h, but the snapshot only covers chunks. core.documents
#       and every other table are free for ALTER TABLE during this
#       window.
#
# The two stages run concurrently — they each open their own snapshot,
# they don't interfere, total wall time = Stage 2.
#
# RESTORE
# ───────
# Restore order matters; the chunks table must exist before its data
# can be COPYed in. Optionally drop+recreate the HNSW index around the
# COPY for ~10x faster restore:
#
#   # 1. Restore everything except chunks data (creates schema + indexes,
#   #    chunks table created empty)
#   pg_restore -d wdws -j 8 \
#       /opt/wdws/backups/wdws_<TIMESTAMP>.main.dir
#
#   # 2. Optionally drop the HNSW index for fast bulk insert:
#   #    psql wdws -c "DROP INDEX IF EXISTS core.document_chunks_embedding_idx"
#
#   # 3. Restore chunks data
#   pg_restore -d wdws -j 4 --data-only \
#       /opt/wdws/backups/wdws_<TIMESTAMP>.chunks.dir
#
#   # 4. If you dropped the HNSW index in step 2, recreate per migration 061.
#
#   # athena_chat is small — single-stage:
#   pg_restore -d athena_chat -j 4 \
#       /opt/wdws/backups/athena_chat_<TIMESTAMP>.dir

set -euo pipefail

BACKUP_DIR="/opt/wdws/backups"
RETENTION_DAYS=2
DATE=$(date +%Y%m%d_%H%M%S)
LOG="${BACKUP_DIR}/backup_${DATE}.log"

# 64 cores. Stage 1's 8 + Stage 2's 4 + 2 leader pg_dump processes ≈ 14
# concurrent backends. zstd:3 at this parallelism saturates IO before CPU.
MAIN_JOBS=8
CHUNKS_JOBS=4

mkdir -p "$BACKUP_DIR"

# ──────────────────────────────────────────────────────────────────────────────
# helpers

dump_main_wdws() {
    # Stage 1: schema + all data EXCEPT core.document_chunks data.
    local TARGET="${BACKUP_DIR}/wdws_${DATE}.main.dir"
    local START
    START=$(date +%s)
    echo "[$(date)] [stage1/main] start (excluding core.document_chunks data) → $TARGET" >> "$LOG"

    if ! sudo -u postgres pg_dump \
            --format=directory \
            --jobs="$MAIN_JOBS" \
            --compress=zstd:3 \
            --no-acl --no-owner \
            --exclude-table-data='core.document_chunks' \
            --file="$TARGET" \
            wdws 2>>"$LOG"
    then
        echo "[$(date)] [stage1/main] ERROR (see $LOG)" | tee -a "$LOG"
        return 1
    fi
    if ! sudo -u postgres pg_restore --list "$TARGET" >/dev/null 2>>"$LOG"; then
        echo "[$(date)] [stage1/main] verification failed" | tee -a "$LOG"
        return 1
    fi
    local SIZE
    SIZE=$(du -sh "$TARGET" | cut -f1)
    local DUR=$(( $(date +%s) - START ))
    echo "[$(date)] [stage1/main] OK: $SIZE in ${DUR}s ($((DUR/60))m$((DUR%60))s)" | tee -a "$LOG"
}

dump_chunks_wdws() {
    # Stage 2: data-only dump of core.document_chunks (the embeddings table).
    local TARGET="${BACKUP_DIR}/wdws_${DATE}.chunks.dir"
    local START
    START=$(date +%s)
    echo "[$(date)] [stage2/chunks] start → $TARGET" >> "$LOG"

    if ! sudo -u postgres pg_dump \
            --format=directory \
            --jobs="$CHUNKS_JOBS" \
            --compress=zstd:3 \
            --no-acl --no-owner \
            --section=data \
            --table='core.document_chunks' \
            --file="$TARGET" \
            wdws 2>>"$LOG"
    then
        echo "[$(date)] [stage2/chunks] ERROR (see $LOG)" | tee -a "$LOG"
        return 1
    fi
    if ! sudo -u postgres pg_restore --list "$TARGET" >/dev/null 2>>"$LOG"; then
        echo "[$(date)] [stage2/chunks] verification failed" | tee -a "$LOG"
        return 1
    fi
    local SIZE
    SIZE=$(du -sh "$TARGET" | cut -f1)
    local DUR=$(( $(date +%s) - START ))
    echo "[$(date)] [stage2/chunks] OK: $SIZE in ${DUR}s ($((DUR/60))m$((DUR%60))s)" | tee -a "$LOG"
}

dump_athena_chat() {
    # Tiny database — single-stage.
    local TARGET="${BACKUP_DIR}/athena_chat_${DATE}.dir"
    local START
    START=$(date +%s)
    echo "[$(date)] [athena_chat] start → $TARGET" >> "$LOG"

    if ! sudo -u postgres pg_dump \
            --format=directory \
            --jobs=4 \
            --compress=zstd:3 \
            --no-acl --no-owner \
            --file="$TARGET" \
            athena_chat 2>>"$LOG"
    then
        echo "[$(date)] [athena_chat] ERROR (see $LOG)" | tee -a "$LOG"
        return 1
    fi
    if ! sudo -u postgres pg_restore --list "$TARGET" >/dev/null 2>>"$LOG"; then
        echo "[$(date)] [athena_chat] verification failed" | tee -a "$LOG"
        return 1
    fi
    local SIZE
    SIZE=$(du -sh "$TARGET" | cut -f1)
    local DUR=$(( $(date +%s) - START ))
    echo "[$(date)] [athena_chat] OK: $SIZE in ${DUR}s ($((DUR/60))m$((DUR%60))s)" | tee -a "$LOG"
}

# ──────────────────────────────────────────────────────────────────────────────

echo "[$(date)] backup run start (host=$(hostname), date=$DATE, mode=v3 two-stage concurrent)" | tee -a "$LOG"

# Run all three stages concurrently. Stage 1 finishes first (~10m),
# unblocking DDL on core.documents. Stage 2 (chunks) and athena_chat
# run alongside.
dump_main_wdws    & MAIN_PID=$!
dump_chunks_wdws  & CHUNKS_PID=$!
dump_athena_chat  & CHAT_PID=$!

MAIN_RC=0; CHUNKS_RC=0; CHAT_RC=0
wait "$MAIN_PID"   || MAIN_RC=$?
wait "$CHUNKS_PID" || CHUNKS_RC=$?
wait "$CHAT_PID"   || CHAT_RC=$?

# ──────────────────────────────────────────────────────────────────────────────
# prune

echo "[$(date)] pruning backups older than ${RETENTION_DAYS} days" | tee -a "$LOG"
find "$BACKUP_DIR" -maxdepth 1 -mindepth 1 -type d -name "*.dir" \
     -mtime +${RETENTION_DAYS} -exec rm -rf {} + 2>>"$LOG" || true
# Legacy v1/v2 single-file dumps (transition cleanup).
find "$BACKUP_DIR" -maxdepth 1 -type f -name "*.dump" \
     -mtime +${RETENTION_DAYS} -delete 2>>"$LOG" || true
# Keep logs longer than dumps so we can diagnose failures retroactively.
find "$BACKUP_DIR" -maxdepth 1 -type f -name "backup_*.log" \
     -mtime +14 -delete 2>>"$LOG" || true

echo "[$(date)] backup run complete (main_rc=$MAIN_RC chunks_rc=$CHUNKS_RC chat_rc=$CHAT_RC)" | tee -a "$LOG"
echo "[$(date)] artifacts:" | tee -a "$LOG"
ls -ld "$BACKUP_DIR"/*.dir 2>/dev/null | tail -10 | tee -a "$LOG"

[ "$MAIN_RC" -eq 0 ] && [ "$CHUNKS_RC" -eq 0 ] && [ "$CHAT_RC" -eq 0 ] || exit 1
