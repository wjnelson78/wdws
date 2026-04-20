#!/usr/bin/env python3
"""Check if required ops.* tables exist."""
import asyncio
import os
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'

from framework import get_pool

async def check_schema():
    pool = get_pool()
    async with pool.acquire() as conn:
        # Check for ops schema and tables
        result = await conn.fetch("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'ops'
            ORDER BY table_name
        """)
        print(f"Tables in 'ops' schema: {len(result)}")
        for row in result:
            print(f"  - {row[0]}.{row[1]}")

        if not result:
            print("\n⚠️  WARNING: 'ops' schema has no tables!")
            # Check if schema exists
            schema_check = await conn.fetch("""
                SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'ops'
            """)
            if not schema_check:
                print("❌ 'ops' schema does NOT exist!")
            else:
                print("⚠️  'ops' schema exists but has no tables")

if __name__ == "__main__":
    try:
        asyncio.run(check_schema())
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
