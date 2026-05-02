#!/bin/bash
# Tier D Phase D-1 parallel ingest — greedy 2-GPU scheduler.
# 28 titles in descending est_chunks order. T11a EXCLUDED — runs in
# Phase D-2 standalone after this orchestrator exits, to guarantee
# T11 fully completed before T11a starts (concurrency mitigation per
# §K #30: T18+T18a embed_pending_for_title overlap artifact).
# T53 skip-disposed (no source XML; reserved/vacant title).
set -u
LOG_DIR=/opt/wdws/usc_research/logs
ORCH_LOG=$LOG_DIR/tier_d_orchestrator.log
PY=/opt/wdws/venv/bin/python3
GPU0_URL=http://172.16.81.187:9098
GPU1_URL=http://172.16.81.187:9099

# Phase D-1 queue (descending est_chunks; T11a EXCLUDED)
TITLE_LIST=(49 19 43 46 36 8 48 30 47 23 14 11 45 44 54 37 17 35 39 52 24 3 32 13 4 27 1 9)

mkdir -p "$LOG_DIR"
exec >>"$ORCH_LOG" 2>&1
echo "=== Tier D Phase D-1 parallel orchestrator START $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Queue (descending est_chunks; T11a deferred to Phase D-2): ${TITLE_LIST[*]}"

xml_for_title() {
  local t=$1
  if [[ "$t" =~ ^[0-9]+$ ]]; then printf "usc%02d.xml" "$t"
  else printf "usc%s.xml" "$t"; fi
}
title_int() { echo "$1" | grep -oE '^[0-9]+'; }
is_appendix_flag() { if [[ "$1" =~ [a-z] ]]; then echo "--appendix"; else echo ""; fi; }

run_full() {
  local n=$1 url=$2
  local fname; fname=$(xml_for_title "$n")
  local tint; tint=$(title_int "$n")
  local app; app=$(is_appendix_flag "$n")
  local logfile=$LOG_DIR/t${n}_ingest.log
  echo "[orchestrator $(date '+%H:%M:%S')] T$n launching on $url, log=$logfile, appendix='$app'"
  HF_ENDPOINT_URL="$url" "$PY" -u /opt/wdws/usc_research/usc_ingest.py \
    --title "$tint" --xml /opt/wdws/data/usc/extracted/"$fname" $app \
    >"$logfile" 2>&1
  local rc=$?
  echo "[orchestrator $(date '+%H:%M:%S')] T$n exit=$rc on $url"
  return $rc
}

queue=("${TITLE_LIST[@]}")
qi=0

# Launch first 2
n0=${queue[$qi]}; qi=$((qi+1))
n1=${queue[$qi]}; qi=$((qi+1))
run_full "$n0" "$GPU0_URL" &
PID_GPU0=$!; GPU0_TITLE=$n0
echo "[orchestrator $(date '+%H:%M:%S')] launched T$n0 on GPU0 pid=$PID_GPU0"
run_full "$n1" "$GPU1_URL" &
PID_GPU1=$!; GPU1_TITLE=$n1
echo "[orchestrator $(date '+%H:%M:%S')] launched T$n1 on GPU1 pid=$PID_GPU1"

while [ $qi -lt ${#queue[@]} ] || [ -n "${PID_GPU0:-}" ] || [ -n "${PID_GPU1:-}" ]; do
  wait -n 2>/dev/null || true
  if [ -n "${PID_GPU0:-}" ] && ! kill -0 "$PID_GPU0" 2>/dev/null; then
    wait "$PID_GPU0" 2>/dev/null
    echo "[orchestrator $(date '+%H:%M:%S')] GPU0 freed (was T$GPU0_TITLE)"
    PID_GPU0=""; GPU0_TITLE=""
    if [ $qi -lt ${#queue[@]} ]; then
      n=${queue[$qi]}; qi=$((qi+1))
      run_full "$n" "$GPU0_URL" &
      PID_GPU0=$!; GPU0_TITLE=$n
      echo "[orchestrator $(date '+%H:%M:%S')] launched T$n on GPU0 pid=$PID_GPU0"
    fi
  fi
  if [ -n "${PID_GPU1:-}" ] && ! kill -0 "$PID_GPU1" 2>/dev/null; then
    wait "$PID_GPU1" 2>/dev/null
    echo "[orchestrator $(date '+%H:%M:%S')] GPU1 freed (was T$GPU1_TITLE)"
    PID_GPU1=""; GPU1_TITLE=""
    if [ $qi -lt ${#queue[@]} ]; then
      n=${queue[$qi]}; qi=$((qi+1))
      run_full "$n" "$GPU1_URL" &
      PID_GPU1=$!; GPU1_TITLE=$n
      echo "[orchestrator $(date '+%H:%M:%S')] launched T$n on GPU1 pid=$PID_GPU1"
    fi
  fi
done
echo "=== Tier D Phase D-1 orchestrator DONE $(date '+%Y-%m-%d %H:%M:%S') ==="
