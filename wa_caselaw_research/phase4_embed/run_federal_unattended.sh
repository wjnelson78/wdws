#!/usr/bin/env bash
# Federal embed unattended overnight launcher.
# Spawns 8 stage-1 worker processes (sharded by o.id % 8) + 1 stage-2 worker
# in follow mode (4 in-flight per pool = 16 concurrent HTTP calls).
#
# Operator-authorized 2026-05-04 ~21:35 PDT (Path B; agent note 62).
# All workers log to _federal_run/<TS>/{stage1.shardN.log, stage2.log}.
# Each stage-1 worker also writes a JSON report on completion.

cd /opt/wdws

# shellcheck disable=SC1091
# Note: do not 'set -u' before sourcing .env; some values contain literal '$'
# tokens that bash treats as variable refs (e.g. DASHBOARD_USER_ADMIN=2$NEL...).
set -a
source /opt/wdws/.env
set +a

TS=$(date +%Y%m%d-%H%M%S)
RUN_DIR="/opt/wdws/wa_caselaw_research/phase4_embed/_federal_run/${TS}"
mkdir -p "$RUN_DIR"
echo "Run dir: $RUN_DIR"
echo "Run started: $(date)"

PYBIN=/opt/wdws/venv/bin/python3
SCRIPT=/opt/wdws/wa_caselaw_research/phase4_embed/phase4_embed_federal.py

N_SHARDS=8
IN_FLIGHT=4

# Spawn 8 stage-1 workers
STAGE1_PIDS=()
for K in $(seq 0 $((N_SHARDS-1))); do
  LOGFILE="${RUN_DIR}/stage1.shard${K}.log"
  REPORT="${RUN_DIR}/stage1.shard${K}.json"
  TAG="s1.${K}of${N_SHARDS}"
  nohup "$PYBIN" "$SCRIPT" \
    --stage 1 \
    --shard "${K}:${N_SHARDS}" \
    --tag "$TAG" \
    --report-file "$REPORT" \
    > "$LOGFILE" 2>&1 &
  pid=$!
  STAGE1_PIDS+=("$pid")
  echo "stage1 shard ${K}/${N_SHARDS} pid=$pid log=$LOGFILE"
done

# Spawn 1 stage-2 worker in follow mode
sleep 5  # let stage 1 produce a few chunks first
STAGE2_LOG="${RUN_DIR}/stage2.log"
STAGE2_REPORT="${RUN_DIR}/stage2.json"
nohup "$PYBIN" "$SCRIPT" \
  --stage 2 \
  --in-flight "$IN_FLIGHT" \
  --follow \
  --follow-max-idle 1800 \
  --tag "s2.if${IN_FLIGHT}" \
  --report-file "$STAGE2_REPORT" \
  > "$STAGE2_LOG" 2>&1 &
STAGE2_PID=$!
echo "stage2 in-flight=${IN_FLIGHT} pid=$STAGE2_PID log=$STAGE2_LOG"

# Persist PID + run-dir info
{
  echo "TS=${TS}"
  echo "RUN_DIR=${RUN_DIR}"
  echo "N_SHARDS=${N_SHARDS}"
  echo "IN_FLIGHT=${IN_FLIGHT}"
  echo "STAGE1_PIDS=${STAGE1_PIDS[*]}"
  echo "STAGE2_PID=${STAGE2_PID}"
  echo "STARTED_AT=$(date -Iseconds)"
} > "${RUN_DIR}/run_meta.env"

echo ""
echo "=== Launched ==="
echo "  ${N_SHARDS} stage1 workers: ${STAGE1_PIDS[*]}"
echo "  1 stage2 worker (in-flight=${IN_FLIGHT}): ${STAGE2_PID}"
echo "  Run dir: ${RUN_DIR}"
echo ""
echo "Monitor progress with:"
echo "  tail -f ${RUN_DIR}/stage2.log"
echo "  tail -n 5 ${RUN_DIR}/stage1.shard*.log"
echo ""
echo "Stop all workers with:"
echo "  kill ${STAGE1_PIDS[*]} ${STAGE2_PID}"
