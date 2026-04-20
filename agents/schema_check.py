#!/usr/bin/env python3
"""Check actual database schema to find column mismatches."""
import sys
import asyncio
import asyncpg
from config import DATABASE_URL

async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)

        # Check ops.agent_runs columns
        print("=" * 80)
        print("OPS.AGENT_RUNS COLUMNS")
        print("=" * 80)

        cols = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'agent_runs'
            ORDER BY ordinal_position
        """)

        for col in cols:
            print(f"  {col['column_name']:30} {col['data_type']:20} nullable={col['is_nullable']}")

        # Check ops.agent_findings columns
        print("\n" + "=" * 80)
        print("OPS.AGENT_FINDINGS COLUMNS")
        print("=" * 80)

        cols = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'agent_findings'
            ORDER BY ordinal_position
        """)

        for col in cols:
            print(f"  {col['column_name']:30} {col['data_type']:20} nullable={col['is_nullable']}")

        await conn.close()

    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
