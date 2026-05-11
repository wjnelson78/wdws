#!/usr/bin/env bash
# Stage-1 watchdog: detects when a stage-1 shard process dies and restarts
# it with the (now skip-and-log) code. Each shard's resume is automatic via
# WHERE o.document_id IS NULL.
# Runs until 06:00 PDT or until all shards complete naturally.

RUN_DIR="${1:?usage: stage1_watchdog.sh <run_dir>}"
[ -d "$RUN_DIR" ] || { echo "no such dir: $RUN_DIR"; exit 1; }

cd /opt/wdws
set -a; source /opt/wdws/.env; set +a
PYBIN=/opt/wdws/venv/bin/python3
SCRIPT=/opt/wdws/wa_caselaw_research/phase4_embed/phase4_embed_federal.py
N_SHARDS=8

STOP_TS=$(date -d 'today 06:00' +%s)
NOW_TS=$(date +%s)
[ $NOW_TS -ge $STOP_TS ] && STOP_TS=$(date -d 'tomorrow 06:00' +%s)
echo "Stage1 watchdog: until $(date -d @$STOP_TS)"

declare -A RESTART_COUNT
for K in $(seq 0 $((N_SHARDS-1))); do RESTART_COUNT[$K]=0; done

while [ $(date +%s) -lt $STOP_TS ]; do
  # Determine which shards still have work
  REMAINING=$(psql "$DATABASE_URL" -P pager=off -X -tA -c "SELECT COUNT(*) FROM case_law.opinions o JOIN case_law.opinion_clusters c ON c.id=o.cluster_id JOIN case_law.dockets d ON d.id=c.docket_id WHERE d.court_id NOT IN ('wash','washctapp','washterr') AND o.document_id IS NULL;" 2>/dev/null || echo "?")
  if [ "$REMAINING" = "0" ]; then
    echo "$(date '+%H:%M:%S') stage1 watchdog: 0 opinions remaining — exiting"
    exit 0
  fi

  for K in $(seq 0 $((N_SHARDS-1))); do
    # Check if this shard's process is alive
    ALIVE=$(pgrep -fc "tag s1\.${K}of${N_SHARDS}" 2>/dev/null || echo 0)
    if [ "$ALIVE" = "0" ]; then
      # Check if shard has any remaining work
      SHARD_REMAINING=$(psql "$DATABASE_URL" -P pager=off -X -tA -c "SELECT COUNT(*) FROM case_law.opinions o JOIN case_law.opinion_clusters c ON c.id=o.cluster_id JOIN case_law.dockets d ON d.id=c.docket_id WHERE d.court_id NOT IN ('wash','washctapp','washterr') AND o.document_id IS NULL AND (o.id % $N_SHARDS) = $K;" 2>/dev/null || echo "?")
      if [ "$SHARD_REMAINING" = "0" ]; then
        # natural completion — don't restart
        continue
      fi
      # restart
      RESTART_COUNT[$K]=$((${RESTART_COUNT[$K]} + 1))
      ATTEMPT=${RESTART_COUNT[$K]}
      TS=$(date +%Y%m%d-%H%M%S)
      LOGFILE="${RUN_DIR}/stage1.shard${K}.wd${ATTEMPT}.${TS}.log"
      REPORT="${RUN_DIR}/stage1.shard${K}.wd${ATTEMPT}.${TS}.json"
      echo "$(date '+%H:%M:%S') stage1 watchdog: shard ${K} dead (remaining=$SHARD_REMAINING) — restart attempt #${ATTEMPT}"
      nohup "$PYBIN" "$SCRIPT" \
        --stage 1 \
        --shard "${K}:${N_SHARDS}" \
        --tag "s1.${K}of${N_SHARDS}" \
        --report-file "$REPORT" \
        > "$LOGFILE" 2>&1 &
    fi
  done
  sleep 60
done

echo "$(date '+%H:%M:%S') stage1 watchdog: stop time reached"
