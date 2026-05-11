#!/usr/bin/env bash
# Federal embed run status checker.
# Reports worker liveness, federal corpus progress, throughput, and ETAs.
# Usage: check_federal_status.sh [<run_dir>]
# If no run_dir given, picks the most recent one.

cd /opt/wdws

RUN_DIR="${1:-}"
if [ -z "$RUN_DIR" ]; then
  RUN_DIR=$(ls -1dt /opt/wdws/wa_caselaw_research/phase4_embed/_federal_run/2026* 2>/dev/null | head -1)
fi
if [ -z "$RUN_DIR" ] || [ ! -d "$RUN_DIR" ]; then
  echo "No run dir found"
  exit 1
fi
echo "Run dir: $RUN_DIR"
echo "Now: $(date)"
echo ""

set -a
source /opt/wdws/.env
set +a

if [ -f "$RUN_DIR/run_meta.env" ]; then
  STARTED_AT=$(grep '^STARTED_AT=' "$RUN_DIR/run_meta.env" | cut -d= -f2)
  echo "Started at: $STARTED_AT"
fi
echo ""

echo "=== Worker liveness ==="
shopt -s nullglob
S1_ALIVE=0
S1_DEAD=0
for log in "$RUN_DIR"/stage1.shard*.log; do
  shard=$(basename "$log" .log | sed 's/stage1\.//')
  pid=$(pgrep -f "tag s1.${shard#shard}of" | head -1)
  if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
    cpu=$(ps -p "$pid" -o pcpu= | xargs)
    et=$(ps -p "$pid" -o etime= | xargs)
    echo "  $shard: ALIVE pid=$pid etime=$et cpu=${cpu}%"
    S1_ALIVE=$((S1_ALIVE+1))
  else
    last=$(grep -E 'DONE|HALT|done,' "$log" | tail -1 | head -c 200)
    echo "  $shard: DEAD ($last)"
    S1_DEAD=$((S1_DEAD+1))
  fi
done
S2_PID=$(pgrep -f 'tag s2\.' | head -1)
if [ -n "$S2_PID" ] && ps -p "$S2_PID" >/dev/null 2>&1; then
  echo "  stage2: ALIVE pid=$S2_PID etime=$(ps -p "$S2_PID" -o etime= | xargs)"
else
  echo "  stage2: DEAD"
fi
echo ""

echo "=== Federal corpus progress ==="
psql "$DATABASE_URL" -P pager=off -X -tA -F'|' -c "
SELECT
  (SELECT COUNT(*) FROM core.documents
     WHERE document_type='case-opinion'
       AND metadata->>'court_id' NOT IN ('wash','washctapp','washterr')) AS docs,
  (SELECT COUNT(*) FROM core.document_chunks ch
     JOIN core.documents d ON d.id=ch.document_id
     WHERE d.document_type='case-opinion'
       AND d.metadata->>'court_id' NOT IN ('wash','washctapp','washterr')) AS chunks_total,
  (SELECT COUNT(*) FROM core.document_chunks ch
     JOIN core.documents d ON d.id=ch.document_id
     WHERE d.document_type='case-opinion'
       AND d.metadata->>'court_id' NOT IN ('wash','washctapp','washterr')
       AND ch.embedding IS NOT NULL) AS chunks_embedded,
  (SELECT COUNT(*) FROM case_law.opinions o
     JOIN case_law.opinion_clusters c ON c.id=o.cluster_id
     JOIN case_law.dockets d ON d.id=c.docket_id
     WHERE d.court_id NOT IN ('wash','washctapp','washterr')
       AND o.document_id IS NOT NULL) AS ops_with_doc,
  (SELECT COUNT(*) FROM case_law.opinions o
     JOIN case_law.opinion_clusters c ON c.id=o.cluster_id
     JOIN case_law.dockets d ON d.id=c.docket_id
     WHERE d.court_id NOT IN ('wash','washctapp','washterr')
       AND o.document_id IS NULL) AS ops_remaining;
" | awk -F'|' '{
  printf "  federal docs:                   %s\n", $1;
  printf "  federal chunks (total):         %s\n", $2;
  printf "  federal chunks (embedded):      %s\n", $3;
  printf "  opinions w/ document_id:        %s\n", $4;
  printf "  opinions remaining (NULL):      %s\n", $5;
  pct = ($1 == 0) ? 0 : ($3 / ($2 + 0.001)) * 100;
  printf "  embedding completeness:         %.1f%%\n", pct;
}'
echo ""

echo "=== Recent stage 1 progress (last 1 line per shard) ==="
for log in "$RUN_DIR"/stage1.shard*.log; do
  shard=$(basename "$log" .log)
  last=$(grep -E 'done,|DONE|HALT' "$log" | tail -1)
  echo "  $shard: $last"
done
echo ""

echo "=== Recent stage 2 progress (last 5 lines) ==="
S2LATEST=$(ls -1t "$RUN_DIR"/stage2*.log 2>/dev/null | head -1)
[ -n "$S2LATEST" ] && tail -5 "$S2LATEST"
echo ""

echo "=== Disk free (sdb1) ==="
df -h /var/lib/postgresql | tail -1

echo ""
echo "=== GPU on .187 ==="
ssh -o ConnectTimeout=5 172.16.81.187 'nvidia-smi --query-gpu=index,memory.used,memory.total,utilization.gpu --format=csv 2>&1' 2>/dev/null | head -5
