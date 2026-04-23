#!/usr/bin/env python3
"""Sprint A Task 4 sample_batch monitor.

Polls core.documents_backfill_staging every 60s and emits events on:
  - Rate anomaly: >=3% rejection rate over total (min 20 samples).
  - Burst anomaly: >=3 rejected in the last 30 classifications.
  - Completion: ops.pending_approvals row with
    decision_type='sprint_a_t4_sample_review' created.
  - Progress heartbeat at each 50-doc milestone.

Events on anomaly include first rejection's error_detail, title,
document_type, and a short list of other rejections.

Stdout lines → Monitor notifications. Exit ends the watch.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import asyncpg


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


async def emit_rejection_report(conn: asyncpg.Connection, header: str) -> None:
    """Print rich info about the earliest rejection in this run."""
    first = await conn.fetchrow("""
        SELECT s.error_detail, s.created_at, s.domain,
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
    last_milestone = 0
    last_rate_alert_total = -1
    last_burst_alert_first_id: str | None = None
    rate_threshold = 0.03  # 3%
    burst_threshold = 3    # N rejected in last 30

    while True:
        conn = await asyncpg.connect(dsn)
        try:
            agg = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE status='rejected') AS rejected,
                       COUNT(*) FILTER (WHERE domain='legal' AND status='pending') AS legal_p,
                       COUNT(*) FILTER (WHERE domain='medical' AND status='pending') AS medical_p
                  FROM core.documents_backfill_staging
            """)
            total = agg['total']
            rejected = agg['rejected']
            legal_p = agg['legal_p']
            medical_p = agg['medical_p']

            # Completion check
            sample_id = await conn.fetchval("""
                SELECT id FROM ops.pending_approvals
                 WHERE decision_type='sprint_a_t4_sample_review'
                   AND created_at >= now() - interval '2 hours'
                 ORDER BY id DESC LIMIT 1
            """)
            if sample_id is not None:
                print(f"[{ts()}] T4_SAMPLE_REVIEW_READY approval_id={sample_id} "
                      f"legal_pending={legal_p} medical_pending={medical_p} "
                      f"rejected_total={rejected}", flush=True)
                return

            # Rate anomaly: total >= 20 AND rate > 3%
            if total >= 20 and rejected / max(total, 1) > rate_threshold:
                if total != last_rate_alert_total:
                    pct = rejected / total * 100
                    await emit_rejection_report(
                        conn,
                        f"[{ts()}] RATE_ANOMALY rejection_rate={rejected}/{total} "
                        f"({pct:.1f}%) threshold>{rate_threshold*100:.0f}% "
                        f"legal_p={legal_p} medical_p={medical_p}",
                    )
                    last_rate_alert_total = total

            # Burst anomaly: >=3 rejected in last 30 classifications
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

            # Progress heartbeat every 50 docs
            milestone = (total // 50) * 50
            if milestone > last_milestone and milestone > 0:
                print(f"[{ts()}] progress total={total} legal_pending={legal_p} "
                      f"medical_pending={medical_p} rejected={rejected}", flush=True)
                last_milestone = milestone
        finally:
            await conn.close()
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
