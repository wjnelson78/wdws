#!/usr/bin/env python3
"""Sprint A Task 4 multi-process phase monitor.

Sized for the post-T4_SAMPLE_REVIEW parallel phase: legal bulk worker +
medical v2 reclassification + (later) mini-validation gate. Polls every
60s and emits events on:

  - RATE_ANOMALY:  total >= 20 AND rejected/total > 3% (de-duplicated)
  - BURST_ANOMALY: >=3 rejected in last 30 classifications
  - RECLASS_COMPLETE: ops.athena_decisions marker row with decision_type
    'sprint_a_t4_v2_reclassification_complete' appears
  - MINI_VALIDATION_GATE_READY: a NEW ops.pending_approvals row with
    decision_type 'sprint_a_t4_sample_review' appears (id > initial_max_id
    captured at monitor startup)
  - Progress heartbeats: every 50-doc milestone across staging (by domain)

Events include first-rejection rich detail (title, document_type, domain,
error_detail) when anomaly fires.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import asyncpg


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


async def emit_rejection_report(conn: asyncpg.Connection, header: str) -> None:
    first = await conn.fetchrow("""
        SELECT s.error_detail, s.domain,
               d.title, d.filename, d.document_type
          FROM core.documents_backfill_staging s
          JOIN core.documents d ON d.id = s.document_id
         WHERE s.status='rejected'
         ORDER BY s.created_at ASC LIMIT 1
    """)
    others = await conn.fetch("""
        SELECT d.title, d.filename, d.document_type, s.error_detail
          FROM core.documents_backfill_staging s
          JOIN core.documents d ON d.id = s.document_id
         WHERE s.status='rejected'
         ORDER BY s.created_at ASC OFFSET 1 LIMIT 8
    """)
    print(header)
    if first:
        title = (first['title'] or first['filename'] or '(none)')[:100]
        err = (first['error_detail'] or '(empty)').replace('\n', ' / ')[:800]
        print(f"  first: {title!r} type={first['document_type']} domain={first['domain']}")
        print(f"    error_detail: {err}")
    if others:
        print("  other rejections:")
        for r in others:
            t = (r['title'] or r['filename'] or '(none)')[:70]
            e = (r['error_detail'] or '(empty)').replace('\n', ' ')[:180]
            print(f"    - {t!r} ({r['document_type']}): {e}")


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]

    # Capture initial state to detect NEW events post-startup
    conn = await asyncpg.connect(dsn)
    try:
        initial_max_sample_review_id = await conn.fetchval("""
            SELECT COALESCE(MAX(id), 0) FROM ops.pending_approvals
             WHERE decision_type='sprint_a_t4_sample_review'
        """)
        initial_reclass_done = await conn.fetchval("""
            SELECT EXISTS (SELECT 1 FROM ops.athena_decisions
                            WHERE decision_type='sprint_a_t4_v2_reclassification_complete')
        """)
    finally:
        await conn.close()

    print(f"[{ts()}] monitor start — tracking for new sample_review id > "
          f"{initial_max_sample_review_id}; reclass_already_done={initial_reclass_done}",
          flush=True)

    last_milestone_total = 0
    last_rate_alert_total = -1
    last_burst_alert_first_id: str | None = None
    reclass_complete_notified = initial_reclass_done
    rate_threshold = 0.03
    burst_threshold = 3

    while True:
        conn = await asyncpg.connect(dsn)
        try:
            # Aggregate state
            agg = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE status='rejected') AS rejected,
                       COUNT(*) FILTER (WHERE status='pending') AS pending,
                       COUNT(*) FILTER (WHERE status='promoted') AS promoted,
                       COUNT(*) FILTER (WHERE status='superseded') AS superseded,
                       COUNT(*) FILTER (WHERE domain='legal' AND status='pending') AS legal_p,
                       COUNT(*) FILTER (WHERE domain='medical' AND status='pending') AS medical_p
                  FROM core.documents_backfill_staging
            """)
            total = agg['total']
            rejected = agg['rejected']

            # Reclass complete
            if not reclass_complete_notified:
                row = await conn.fetchrow("""
                    SELECT id, action_taken FROM ops.athena_decisions
                     WHERE decision_type='sprint_a_t4_v2_reclassification_complete'
                     ORDER BY id DESC LIMIT 1
                """)
                if row:
                    summary = (row['action_taken'] or '{}')
                    print(f"[{ts()}] RECLASS_COMPLETE decision_id={row['id']} "
                          f"summary={summary[:500]}", flush=True)
                    reclass_complete_notified = True

            # New sample_review row (mini-validation gate ready)
            new_sample_id = await conn.fetchval("""
                SELECT id FROM ops.pending_approvals
                 WHERE decision_type='sprint_a_t4_sample_review'
                   AND id > $1
                 ORDER BY id DESC LIMIT 1
            """, initial_max_sample_review_id)
            if new_sample_id is not None:
                print(f"[{ts()}] MINI_VALIDATION_GATE_READY approval_id={new_sample_id} "
                      f"pending={agg['pending']} promoted={agg['promoted']} "
                      f"superseded={agg['superseded']} rejected={rejected}",
                      flush=True)
                return  # exit — gate review is the next stop

            # Rate anomaly
            if total >= 20 and rejected / max(total, 1) > rate_threshold:
                if total != last_rate_alert_total:
                    pct = rejected / total * 100
                    await emit_rejection_report(
                        conn,
                        f"[{ts()}] RATE_ANOMALY rejection_rate={rejected}/{total} "
                        f"({pct:.1f}%) threshold>{rate_threshold*100:.0f}% "
                        f"legal_p={agg['legal_p']} medical_p={agg['medical_p']}",
                    )
                    last_rate_alert_total = total

            # Burst anomaly
            burst = await conn.fetchrow("""
                WITH recent AS (
                  SELECT id, status, created_at FROM core.documents_backfill_staging
                   ORDER BY created_at DESC LIMIT 30
                )
                SELECT COUNT(*) FILTER (WHERE status='rejected') AS n_rejected,
                       (SELECT id::text FROM recent WHERE status='rejected'
                         ORDER BY created_at ASC LIMIT 1) AS first_rej_id
                  FROM recent
            """)
            if burst['n_rejected'] and burst['n_rejected'] >= burst_threshold:
                if burst['first_rej_id'] != last_burst_alert_first_id:
                    await emit_rejection_report(
                        conn,
                        f"[{ts()}] BURST_ANOMALY "
                        f"{burst['n_rejected']}_of_last_30_rejected "
                        f"threshold={burst_threshold}",
                    )
                    last_burst_alert_first_id = burst['first_rej_id']

            # Progress heartbeat every 50 docs of total staging
            milestone = (total // 50) * 50
            if milestone > last_milestone_total and milestone > 0:
                print(f"[{ts()}] progress total={total} legal_pending={agg['legal_p']} "
                      f"medical_pending={agg['medical_p']} promoted={agg['promoted']} "
                      f"superseded={agg['superseded']} rejected={rejected}", flush=True)
                last_milestone_total = milestone
        finally:
            await conn.close()
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
