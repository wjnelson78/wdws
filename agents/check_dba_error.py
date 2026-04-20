#!/usr/bin/env python3
"""Diagnose the dba agent failure"""
import asyncio
import asyncpg
import os
import json

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    try:
        conn = await asyncpg.connect(url)

        # Get recent dba runs
        rows = await conn.fetch("""
            SELECT id, agent_id, status, error, created_at
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 10
        """)

        print("Recent DBA agent runs:")
        for r in rows:
            print(f"\n=== Run {r['id']} ({r['created_at']}) ===")
            print(f"Status: {r['status']}")
            if r['error']:
                # Show first 1000 chars of error
                err_preview = r['error'][:1000]
                print(f"Error:\n{err_preview}")
                if len(r['error']) > 1000:
                    print(f"\n... (truncated, total {len(r['error'])} chars)")

        # Count error types
        error_rows = await conn.fetch("""
            SELECT status, COUNT(*) as count
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            GROUP BY status
        """)

        print("\n\nError summary:")
        for row in error_rows:
            print(f"  {row['status']}: {row['count']}")

        await conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
