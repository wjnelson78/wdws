#!/usr/bin/env python3
"""Minimal diagnostic to find the actual failing query in agent_dba."""
import asyncio
import asyncpg
import os
import sys

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    print(f"Testing with URL: {url}")

    try:
        conn = await asyncpg.connect(url)
        print("✓ Database connected")

        # Try the exact first query that agent_dba runs (line 133-140)
        print("\n1. Testing connection pool query (pg_stat_activity)...")
        try:
            result = await conn.fetch("""
                SELECT state, COUNT(*) as count,
                       MAX(EXTRACT(EPOCH FROM now() - state_change)) as max_age_secs
                FROM pg_stat_activity
                WHERE datname = current_database()
                GROUP BY state
                ORDER BY count DESC
            """)
            print(f"   ✓ Success: {len(result)} rows")
        except Exception as e:
            print(f"   ✗ FAILED: {e}")
            return

        # Try max_connections query
        print("\n2. Testing max_connections query...")
        try:
            result = await conn.fetch("""
                SELECT setting::int as max_connections FROM pg_settings
                WHERE name = 'max_connections'
            """)
            print(f"   ✓ Success: {result}")
        except Exception as e:
            print(f"   ✗ FAILED: {e}")
            return

        # Try the table bloat query (line 235-246)
        print("\n3. Testing bloat query...")
        try:
            result = await conn.fetch("""
                SELECT schemaname || '.' || relname as table_name,
                       n_live_tup, n_dead_tup,
                       CASE WHEN n_live_tup > 0
                            THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 1)
                            ELSE 0 END as dead_pct,
                       last_vacuum, last_autovacuum, last_analyze,
                       pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) as total_size
                FROM pg_stat_user_tables
                WHERE schemaname IN ('core', 'legal', 'medical', 'ops', 'paperless')
                ORDER BY n_dead_tup DESC
                LIMIT 30
            """)
            print(f"   ✓ Success: {len(result)} rows")
        except Exception as e:
            print(f"   ✗ FAILED: {e}")
            return

        # Try the sequential scans query (line 279-292)
        print("\n4. Testing sequential scans query...")
        try:
            result = await conn.fetch("""
                SELECT schemaname || '.' || relname as table_name,
                       seq_scan, idx_scan,
                       CASE WHEN (seq_scan + idx_scan) > 0
                            THEN round(100.0 * seq_scan / (seq_scan + idx_scan), 1)
                            ELSE 0 END as seq_pct,
                       n_live_tup
                FROM pg_stat_user_tables
                WHERE schemaname IN ('core', 'legal', 'medical', 'ops')
                  AND n_live_tup > 1000
                  AND seq_scan > idx_scan
                  AND (seq_scan + idx_scan) > 10
                ORDER BY seq_scan DESC
                LIMIT 10
            """)
            print(f"   ✓ Success: {len(result)} rows")
        except Exception as e:
            print(f"   ✗ FAILED: {e}")
            return

        # Try the error rate query (line 530-542)
        print("\n5. Testing error rate query (ops.agent_runs)...")
        try:
            result = await conn.fetch("""
                SELECT agent_id,
                       COUNT(*) as total_runs,
                       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors,
                       CASE WHEN COUNT(*) > 0
                            THEN round(100.0 * SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*), 1)
                            ELSE 0 END as error_pct
                FROM ops.agent_runs
                WHERE created_at > now() - interval '24 hours'
                GROUP BY agent_id
                HAVING SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) > 0
                ORDER BY error_pct DESC
            """)
            print(f"   ✓ Success: {len(result)} rows")
        except Exception as e:
            print(f"   ✗ FAILED (ops.agent_runs missing?): {e}")
            return

        print("\n✓ All tests passed!")
        await conn.close()

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
