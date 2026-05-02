#!/bin/bash
# Tier C parallel ingest — greedy 2-GPU scheduler with T16 Stage 2 special first-launch.
# T16 already had Stage 1 done (run_id c2dd9fa6...); resume Stage 2 on GPU0.
# GPU1 starts on T22; greedy refill from queue: T7 T25 T38 T50 T33 T18 T18a T21 T6 T40 T51 T41
# Per-title HF endpoint pinned via HF_ENDPOINT_URL env override.
set -u
LOG_DIR=/opt/wdws/usc_research/logs
ORCH_LOG=$LOG_DIR/tier_c_orchestrator.log
PY=/opt/wdws/venv/bin/python3
GPU0_URL=http://172.16.81.187:9098
GPU1_URL=http://172.16.81.187:9099

# T16 Stage 2 resume parameters (Stage 1 already completed)
T16_RUN_ID=c2dd9fa6-bbe5-457e-bb65-ca2259ac6a53

# Queue for greedy refill (GPU1 starts on T22; subsequent titles fill freed GPUs)
TITLE_LIST=(22 7 25 38 50 33 18 18a 21 6 40 51 41)

mkdir -p "$LOG_DIR"
exec >>"$ORCH_LOG" 2>&1
echo "=== Tier C parallel orchestrator START $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "T16 Stage 2 resume run_id=$T16_RUN_ID on GPU0"
echo "Queue (GPU1 first, greedy refill): ${TITLE_LIST[*]}"

# title_to_xml: handles numeric titles + appendix variants (e.g. 18a)
xml_for_title() {
  local t=$1
  if [[ "$t" =~ ^[0-9]+$ ]]; then
    printf "usc%02d.xml" "$t"
  else
    # e.g. 18a → usc18a.xml
    printf "usc%s.xml" "$t"
  fi
}

# is_appendix: returns 0 (true) if title has alpha suffix (appendix), 1 otherwise
is_appendix_flag() {
  local t=$1
  if [[ "$t" =~ [a-z] ]]; then echo "--appendix"; else echo ""; fi
}

# title_int: extract integer portion for --title arg (18a → 18)
title_int() {
  local t=$1
  echo "$t" | grep -oE '^[0-9]+'
}

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

run_t16_stage2() {
  local url=$1
  local logfile=$LOG_DIR/t16_stage2.log
  echo "[orchestrator $(date '+%H:%M:%S')] T16 Stage 2 resume on $url, run_id=$T16_RUN_ID, log=$logfile"
  HF_ENDPOINT_URL="$url" "$PY" -u /opt/wdws/usc_research/usc_ingest.py \
    --title 16 --xml /opt/wdws/data/usc/extracted/usc16.xml \
    --skip-stage1 --run-id "$T16_RUN_ID" \
    >"$logfile" 2>&1
  local rc=$?
  echo "[orchestrator $(date '+%H:%M:%S')] T16 Stage 2 exit=$rc on $url"
  return $rc
}

# Launch initial pair: T16 Stage 2 on GPU0, T22 (full ingest) on GPU1
queue=("${TITLE_LIST[@]}")
qi=0

run_t16_stage2 "$GPU0_URL" &
PID_GPU0=$!
GPU0_TITLE="16(s2)"
echo "[orchestrator $(date '+%H:%M:%S')] launched T16-Stage2 on GPU0 pid=$PID_GPU0"

n1=${queue[$qi]}; qi=$((qi+1))
run_full "$n1" "$GPU1_URL" &
PID_GPU1=$!
GPU1_TITLE=$n1
echo "[orchestrator $(date '+%H:%M:%S')] launched T$n1 on GPU1 pid=$PID_GPU1"

# Greedy refill loop
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
echo "=== Tier C parallel orchestrator DONE $(date '+%Y-%m-%d %H:%M:%S') ==="
