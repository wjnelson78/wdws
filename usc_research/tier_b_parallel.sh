#!/bin/bash
# Tier B parallel ingest — greedy 2-GPU scheduler.
# Queue (descending by section count): T10, T15, T20, T12, T2, T34, T31.
# Launches first 2; as each finishes, the next from queue starts on the freed GPU.
# Per-title HF endpoint pinned via HF_ENDPOINT_URL env override.
set -u
LOG_DIR=/opt/wdws/usc_research/logs
ORCH_LOG=$LOG_DIR/tier_b_orchestrator.log
PY=/opt/wdws/venv/bin/python3
TITLE_LIST=(10 15 20 12 2 34 31)
GPU0_URL=http://172.16.81.187:9098
GPU1_URL=http://172.16.81.187:9099

mkdir -p "$LOG_DIR"
exec >>"$ORCH_LOG" 2>&1
echo "=== Tier B parallel orchestrator START $(date '+%Y-%m-%d %H:%M:%S') ==="
echo "Queue: ${TITLE_LIST[*]}"

run_one() {
  local n=$1 url=$2
  local fname
  fname=usc$(printf '%02d' "$n").xml
  local logfile=$LOG_DIR/t${n}_ingest.log
  echo "[orchestrator $(date '+%H:%M:%S')] T$n launching on $url, log=$logfile"
  HF_ENDPOINT_URL="$url" "$PY" -u /opt/wdws/usc_research/usc_ingest.py \
    --title "$n" --xml /opt/wdws/data/usc/extracted/"$fname" \
    >"$logfile" 2>&1
  local rc=$?
  echo "[orchestrator $(date '+%H:%M:%S')] T$n exit=$rc on $url"
  return $rc
}

# Indexed queue cursor
queue=("${TITLE_LIST[@]}")
qi=0

# Launch first 2
n0=${queue[$qi]}; qi=$((qi+1))
n1=${queue[$qi]}; qi=$((qi+1))
run_one "$n0" "$GPU0_URL" &
PID_GPU0=$!
GPU0_TITLE=$n0
echo "[orchestrator $(date '+%H:%M:%S')] launched T$n0 on GPU0 pid=$PID_GPU0"
run_one "$n1" "$GPU1_URL" &
PID_GPU1=$!
GPU1_TITLE=$n1
echo "[orchestrator $(date '+%H:%M:%S')] launched T$n1 on GPU1 pid=$PID_GPU1"

# Greedy refill loop
while [ $qi -lt ${#queue[@]} ] || [ -n "${PID_GPU0:-}" ] || [ -n "${PID_GPU1:-}" ]; do
  # Wait for any background to finish
  wait -n 2>/dev/null || true
  # Check which one(s) exited
  if [ -n "${PID_GPU0:-}" ] && ! kill -0 "$PID_GPU0" 2>/dev/null; then
    wait "$PID_GPU0" 2>/dev/null
    echo "[orchestrator $(date '+%H:%M:%S')] GPU0 freed (was T$GPU0_TITLE)"
    PID_GPU0=""
    GPU0_TITLE=""
    if [ $qi -lt ${#queue[@]} ]; then
      n=${queue[$qi]}; qi=$((qi+1))
      run_one "$n" "$GPU0_URL" &
      PID_GPU0=$!
      GPU0_TITLE=$n
      echo "[orchestrator $(date '+%H:%M:%S')] launched T$n on GPU0 pid=$PID_GPU0"
    fi
  fi
  if [ -n "${PID_GPU1:-}" ] && ! kill -0 "$PID_GPU1" 2>/dev/null; then
    wait "$PID_GPU1" 2>/dev/null
    echo "[orchestrator $(date '+%H:%M:%S')] GPU1 freed (was T$GPU1_TITLE)"
    PID_GPU1=""
    GPU1_TITLE=""
    if [ $qi -lt ${#queue[@]} ]; then
      n=${queue[$qi]}; qi=$((qi+1))
      run_one "$n" "$GPU1_URL" &
      PID_GPU1=$!
      GPU1_TITLE=$n
      echo "[orchestrator $(date '+%H:%M:%S')] launched T$n on GPU1 pid=$PID_GPU1"
    fi
  fi
done
echo "=== Tier B parallel orchestrator DONE $(date '+%Y-%m-%d %H:%M:%S') ==="
