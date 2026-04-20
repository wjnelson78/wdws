#!/usr/bin/env python3
"""Check latest DBA agent errors."""
import asyncio
import os
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
from framework import get_pool

async def check():
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT id, agent_id, status, error_msg, created_at
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 3
        ''')
        print(f"Found {len(rows)} recent runs:\n")
        for i, row in enumerate(rows, 1):
            print(f"Run #{i}: {row[2]} (id={row[0]}, created={row[4]})")
            if row[3]:
                print(f"Error:\n{row[3]}\n")
            else:
                print("No error message\n")

if __name__ == "__main__":
    try:
        asyncio.run(check())
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
