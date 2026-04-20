#!/usr/bin/env python3
"""Query actual DBA agent run status and errors."""
import asyncio
import asyncpg
import os
import json

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    print(f"Connecting to: {url}")

    try:
        conn = await asyncpg.connect(url)
        print("✓ Connected")

        # Get last 5 DBA runs
        print("\n=== Last 5 DBA agent runs ===")
        rows = await conn.fetch("""
            SELECT id, agent_id, status, error, summary, created_at, started_at, ended_at, duration_ms
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 5
        """)

        for r in rows:
            print(f"\nRun ID: {r['id']}")
            print(f"  Status: {r['status']}")
            print(f"  Duration: {r['duration_ms']}ms")
            print(f"  Created: {r['created_at']}")
            if r['summary']:
                print(f"  Summary: {r['summary'][:100]}")
            if r['error']:
                err_preview = r['error'][:300]
                print(f"  Error: {err_preview}")
                if len(r['error']) > 300:
                    print(f"    ... (total {len(r['error'])} chars)")

        # Check agent registry
        print("\n=== DBA in agent_registry ===")
        reg = await conn.fetchrow("""
            SELECT id, name, is_active, last_status, error_count, run_count
            FROM ops.agent_registry
            WHERE id = 'dba'
        """)
        if reg:
            print(f"  Active: {reg['is_active']}")
            print(f"  Last Status: {reg['last_status']}")
            print(f"  Runs: {reg['run_count']}")
            print(f"  Errors: {reg['error_count']}")
        else:
            print("  ✗ DBA not found in agent_registry")

        await conn.close()
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
