#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, '/opt/wdws/agents')
from config import DATABASE_URL
import asyncio
import asyncpg

async def check_dba_errors():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch('''
            SELECT id, status, error_msg, created_at
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 5
        ''')
        for row in rows:
            print(f"Run {row['id']}: {row['status']} at {row['created_at']}")
            if row['error_msg']:
                print(f"  ERROR: {row['error_msg'][:500]}")
            print()
    finally:
        await conn.close()

asyncio.run(check_dba_errors())
