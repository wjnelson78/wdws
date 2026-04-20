#!/usr/bin/env python3
"""Quick diagnostic to get latest DBA agent errors."""
import sys
import os
import asyncio
sys.path.insert(0, '/opt/wdws/agents')

from config import DATABASE_URL
import asyncpg

async def get_latest_dba_error():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch('''
            SELECT id, status, error_msg, created_at, run_output
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 10
        ''')
        for i, row in enumerate(rows):
            print(f"\n{'='*80}")
            print(f"Run {i+1}: {row['status']} at {row['created_at']}")
            print(f"{'='*80}")
            if row['error_msg']:
                print(f"ERROR MESSAGE:\n{row['error_msg']}")
            if row['run_output']:
                print(f"\nOUTPUT:\n{row['run_output']}")
            print()
    finally:
        await conn.close()

asyncio.run(get_latest_dba_error())
