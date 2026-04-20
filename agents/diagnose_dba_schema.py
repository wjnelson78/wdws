#!/usr/bin/env python3
"""Check what tables actually exist in the ops schema."""
import sys
sys.path.insert(0, '/opt/wdws/agents')
from config import DATABASE_URL
import asyncio
import asyncpg

async def check_schema():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Check what ops tables exist
        print("=== Checking ops schema tables ===")
        rows = await conn.fetch('''
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'ops'
            ORDER BY table_name
        ''')
        for row in rows:
            print(f"  ✓ ops.{row['table_name']}")

        print("\n=== Checking ops.agent_runs columns ===")
        cols = await conn.fetch('''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'agent_runs'
            ORDER BY column_name
        ''')
        for col in cols:
            print(f"  ✓ {col['column_name']} ({col['data_type']})")

    finally:
        await conn.close()

if __name__ == '__main__':
    asyncio.run(check_schema())
