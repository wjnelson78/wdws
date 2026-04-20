#!/usr/bin/env python3
"""Diagnostic: Test agent_dba schema requirements"""
import asyncio
import asyncpg
import os
import json

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    print(f"Testing with URL: {url}")
    print("="*60)

    try:
        conn = await asyncpg.connect(url)

        # 1. Check if ops.notification_queue exists
        print("\n1. Checking ops.notification_queue...")
        try:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ops' AND table_name = 'notification_queue'
                )
            """)
            print(f"   EXISTS: {exists}")
            if exists:
                cols = await conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'ops' AND table_name = 'notification_queue'
                    ORDER BY ordinal_position
                """)
                print("   Columns:")
                for col in cols:
                    print(f"     - {col['column_name']}: {col['data_type']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # 2. Check if ops.agent_memory exists
        print("\n2. Checking ops.agent_memory...")
        try:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ops' AND table_name = 'agent_memory'
                )
            """)
            print(f"   EXISTS: {exists}")
            if exists:
                cols = await conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'ops' AND table_name = 'agent_memory'
                    ORDER BY ordinal_position
                """)
                print("   Columns:")
                for col in cols:
                    print(f"     - {col['column_name']}: {col['data_type']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # 3. Check if core.document_chunks exists
        print("\n3. Checking core.document_chunks...")
        try:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'core' AND table_name = 'document_chunks'
                )
            """)
            print(f"   EXISTS: {exists}")
            if exists:
                cols = await conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'core' AND table_name = 'document_chunks'
                    ORDER BY ordinal_position
                """)
                print("   Columns:")
                for col in cols:
                    print(f"     - {col['column_name']}: {col['data_type']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # 4. Check if ops.agent_runs exists
        print("\n4. Checking ops.agent_runs...")
        try:
            exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ops' AND table_name = 'agent_runs'
                )
            """)
            print(f"   EXISTS: {exists}")
            if exists:
                cols = await conn.fetch("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'ops' AND table_name = 'agent_runs'
                    ORDER BY ordinal_position
                """)
                print("   Columns:")
                for col in cols:
                    print(f"     - {col['column_name']}: {col['data_type']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # 5. Check all ops.* tables
        print("\n5. All tables in 'ops' schema...")
        try:
            tables = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'ops'
                ORDER BY table_name
            """)
            for tbl in tables:
                print(f"   - {tbl['table_name']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        await conn.close()
        print("\n" + "="*60)
        print("Schema check complete")

    except Exception as e:
        print(f"Connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
