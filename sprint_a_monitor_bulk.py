#!/usr/bin/env python3
"""Sprint A Task 4 bulk-phase monitor.

Post-gate-approval, the parallel legal+medical bulk workers run until the
corpus is classified. Monitor watches for:

  - RATE_ANOMALY: rejected/total > 3% (de-duplicated by total)
  - BURST_ANOMALY: >=3 rejected in last 30 classifications
  - BUCKET_OVERSHOOT: rolling 60min staging count > ceiling + 2
    (2 = tolerance for 2-worker micro-race)
  - Progress heartbeats: every 50 docs of new staging since monitor start

Unlike sprint_a_monitor_v2, this monitor runs indefinitely until killed —
there is no single terminal event in the bulk phase. Poll every 60s.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import asyncpg

CEILING = 100
CEILING_TOLERANCE = 2


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


async def emit_rejection_report(conn: asyncpg.Connection, header: str) -> None:
    first = await conn.fetchrow("""
        SELECT s.error_detail, s.domain, s.classifier_version,
               d.title, d.filename, d.document_type
          FROM core.documents_backfill_staging s
          JOIN core.documents d ON d.id = s.document_id
         WHERE s.status='rejected'
         ORDER BY s.created_at ASC LIMIT 1
    """)
    others = await conn.fetch("""
        SELECT d.title, d.filename, d.document_type, s.error_detail, s.classifier_version
          FROM core.documents_backfill_staging s
          JOIN core.documents d ON d.id = s.document_id
         WHERE s.status='rejected'
         ORDER BY s.created_at ASC OFFSET 1 LIMIT 8
    """)
    print(header)
    if first:
        title = (first['title'] or first['filename'] or '(none)')[:100]
        err = (first['error_detail'] or '(empty)').replace('\n', ' / ')[:800]
        print(f"  first: {title!r} type={first['document_type']} "
              f"domain={first['domain']} ver={first['classifier_version']}")
        print(f"    error_detail: {err}")
    if others:
        print("  other rejections:")
        for r in others:
            t = (r['title'] or r['filename'] or '(none)')[:70]
            e = (r['error_detail'] or '(empty)').replace('\n', ' ')[:180]
            print(f"    - {t!r} ({r['document_type']}, {r['classifier_version']}): {e}")


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]

    conn = await asyncpg.connect(dsn)
    try:
        start_total = await conn.fetchval(
            "SELECT COUNT(*) FROM core.documents_backfill_staging")
    finally:
        await conn.close()

    print(f"[{ts()}] bulk-monitor start — baseline total={start_total}, "
          f"ceiling={CEILING}/hr (tolerance +{CEILING_TOLERANCE} for 2-worker race)",
          flush=True)

    last_milestone_delta = 0
    last_rate_alert_total = -1
    last_burst_alert_first_id: str | None = None
    last_bucket_overshoot_count = -1
    rate_threshold = 0.03
    burst_threshold = 3

    while True:
        conn = await asyncpg.connect(dsn)
        try:
            agg = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE status='rejected') AS rejected,
                       COUNT(*) FILTER (WHERE status='pending') AS pending,
                       COUNT(*) FILTER (WHERE status='promoted') AS promoted,
                       COUNT(*) FILTER (WHERE status='superseded') AS superseded,
                       COUNT(*) FILTER (WHERE domain='legal' AND status='pending'
                                         AND classifier_version='agent_athena_v1') AS legal_p_v1,
                       COUNT(*) FILTER (WHERE domain='medical' AND status='pending'
                                         AND classifier_version='agent_athena_v2') AS medical_p_v2
                  FROM core.documents_backfill_staging
            """)
            total = agg['total']
            rejected = agg['rejected']

            # Bucket overshoot check
            bucket_count = await conn.fetchval("""
                SELECT COUNT(*) FROM core.documents_backfill_staging
                 WHERE created_at >= now() - interval '1 hour'
            """)
            if bucket_count > CEILING + CEILING_TOLERANCE:
                if bucket_count != last_bucket_overshoot_count:
                    print(f"[{ts()}] BUCKET_OVERSHOOT 60min_count={bucket_count} "
                          f"ceiling={CEILING} tolerance=+{CEILING_TOLERANCE} "
                          f"actual_over=+{bucket_count - CEILING}", flush=True)
                    last_bucket_overshoot_count = bucket_count

            # Rate anomaly
            if total >= 20 and rejected / max(total, 1) > rate_threshold:
                if total != last_rate_alert_total:
                    pct = rejected / total * 100
                    await emit_rejection_report(
                        conn,
                        f"[{ts()}] RATE_ANOMALY rejection_rate={rejected}/{total} "
                        f"({pct:.1f}%) threshold>{rate_threshold*100:.0f}% "
                        f"legal_p_v1={agg['legal_p_v1']} medical_p_v2={agg['medical_p_v2']}",
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

            # Progress heartbeat every 50 new docs since monitor start
            delta = total - start_total
            milestone = (delta // 50) * 50
            if milestone > last_milestone_delta and milestone > 0:
                print(f"[{ts()}] progress +{delta} since start "
                      f"(legal_p_v1={agg['legal_p_v1']} medical_p_v2={agg['medical_p_v2']} "
                      f"promoted={agg['promoted']} superseded={agg['superseded']} "
                      f"rejected={rejected} bucket_60min={bucket_count})", flush=True)
                last_milestone_delta = milestone
        finally:
            await conn.close()
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
