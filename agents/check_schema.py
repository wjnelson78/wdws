#!/usr/bin/env python3
"""Check actual database schema vs expected"""
import asyncio
import asyncpg
import os

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    try:
        conn = await asyncpg.connect(url)

        # Check if core.document_chunks exists
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'core' AND table_name = 'document_chunks'
            )
        """)
        print(f"core.document_chunks exists: {exists}")

        # Check if ops.agent_runs exists
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'ops' AND table_name = 'agent_runs'
            )
        """)
        print(f"ops.agent_runs exists: {exists}")

        # Get all columns in ops.agent_runs
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'agent_runs'
            ORDER BY ordinal_position
        """)
        print(f"\nops.agent_runs columns:")
        for col in cols:
            print(f"  {col['column_name']}: {col['data_type']}")

        # Check if ops.notification_queue exists
        exists = await conn.fetchval("""
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'ops' AND table_name = 'notification_queue'
            )
        """)
        print(f"\nops.notification_queue exists: {exists}")
        if exists:
            cols = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'ops' AND table_name = 'notification_queue'
                ORDER BY ordinal_position
            """)
            print(f"ops.notification_queue columns:")
            for col in cols:
                print(f"  {col['column_name']}: {col['data_type']}")

        await conn.close()
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
