#!/usr/bin/env bash
# Stage-2 watchdog: monitors the running stage-2 process; if it dies,
# restarts it with progressive backoff on in-flight (4 → 3 → 2 → 1).
# Runs until 06:00 PDT or until federal embedding is complete.
#
# Usage: stage2_watchdog.sh <run_dir>

set -e
RUN_DIR="${1:?usage: stage2_watchdog.sh <run_dir>}"
[ -d "$RUN_DIR" ] || { echo "no such dir: $RUN_DIR"; exit 1; }

cd /opt/wdws
set -a; source /opt/wdws/.env; set +a
PYBIN=/opt/wdws/venv/bin/python3
SCRIPT=/opt/wdws/wa_caselaw_research/phase4_embed/phase4_embed_federal.py

# Stop time: 06:00 PDT today (or tomorrow if past 06:00 already)
STOP_TS=$(date -d 'today 06:00' +%s)
NOW_TS=$(date +%s)
[ $NOW_TS -ge $STOP_TS ] && STOP_TS=$(date -d 'tomorrow 06:00' +%s)
echo "Watchdog: will run until $(date -d @$STOP_TS) ($(((STOP_TS-NOW_TS)/60)) min)"

ATTEMPT=0
declare -a IN_FLIGHT_LADDER=(1 1 1 1 1 1)  # throttled per operator 2026-05-05 ~07:00 PDT
                                            # (was 2,2,2,1,1,1; reduced for daytime
                                            # GPU headroom for interactive Athena)
LAST_PID_FILE="$RUN_DIR/.watchdog_last_pid"

while [ $(date +%s) -lt $STOP_TS ]; do
  # Check if any stage-2 worker is alive
  S2_ALIVE_PID=$(pgrep -f 'tag s2\.' | head -1 || true)
  if [ -n "$S2_ALIVE_PID" ] && ps -p "$S2_ALIVE_PID" >/dev/null 2>&1; then
    sleep 30
    continue
  fi

  # Check if all stage 1 workers are done AND no chunks left to embed
  S1_ALIVE_COUNT=$(pgrep -fc 'tag s1\.' || echo 0)
  PENDING=$(psql "$DATABASE_URL" -P pager=off -X -tA -c "
    SELECT COUNT(*) FROM core.document_chunks ch
    JOIN core.documents d ON d.id=ch.document_id
    WHERE d.document_type='case-opinion'
      AND d.metadata->>'court_id' NOT IN ('wash','washctapp','washterr')
      AND ch.embedding IS NULL;
  " 2>/dev/null || echo "?")
  if [ "$S1_ALIVE_COUNT" = "0" ] && [ "$PENDING" = "0" ]; then
    echo "$(date): all stage 1 done + 0 pending chunks — watchdog exiting"
    exit 0
  fi

  # Restart stage 2 with backoff in-flight value
  IDX=$(( ATTEMPT < ${#IN_FLIGHT_LADDER[@]} ? ATTEMPT : ${#IN_FLIGHT_LADDER[@]}-1 ))
  IF=${IN_FLIGHT_LADDER[$IDX]}
  TS=$(date +%Y%m%d-%H%M%S)
  S2_LOG="$RUN_DIR/stage2.wd${ATTEMPT}.if${IF}.${TS}.log"
  S2_REPORT="$RUN_DIR/stage2.wd${ATTEMPT}.if${IF}.${TS}.json"
  echo "$(date): stage2 down (attempt $ATTEMPT, in-flight=$IF, pending=$PENDING) — restarting"
  nohup "$PYBIN" "$SCRIPT" \
    --stage 2 \
    --in-flight "$IF" \
    --follow \
    --follow-max-idle 1800 \
    --tag "s2.wd${ATTEMPT}.if${IF}" \
    --report-file "$S2_REPORT" \
    > "$S2_LOG" 2>&1 &
  NEW_PID=$!
  echo "$NEW_PID" > "$LAST_PID_FILE"
  echo "  new pid=$NEW_PID log=$S2_LOG"
  ATTEMPT=$((ATTEMPT+1))

  # Wait 60s for warm-up before next check
  sleep 60
done

echo "$(date): watchdog stop time reached"
