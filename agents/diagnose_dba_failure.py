#!/usr/bin/env python3
"""
Code Doctor Diagnostic: Identify agent_dba 3+ failure root cause.

This script checks:
1. Database connectivity
2. Required tables existence
3. Python syntax
4. Recent agent logs
5. Framework status
"""

import sys
import asyncio
import traceback
from pathlib import Path

async def main():
    print("=" * 80)
    print("CODE DOCTOR DIAGNOSTIC: AGENT DBA 3+ FAILURE ROOT CAUSE")
    print("=" * 80)
    print()

    # Check 1: Python syntax
    print("1️⃣  PYTHON SYNTAX CHECK")
    print("-" * 80)
    try:
        import py_compile
        py_compile.compile("agent_dba.py", doraise=True)
        print("✓ agent_dba.py syntax is valid")
    except py_compile.PyCompileError as e:
        print(f"❌ SYNTAX ERROR in agent_dba.py:\n{e}")
        return 1
    print()

    # Check 2: Import check
    print("2️⃣  IMPORT CHECK")
    print("-" * 80)
    try:
        from framework import BaseAgent, RunContext, get_pool
        print("✓ framework imports successful")
    except Exception as e:
        print(f"❌ Framework import failed: {e}")
        traceback.print_exc()
        return 1

    try:
        from agent_dba import DBAAgent
        print("✓ agent_dba imports successful")
    except Exception as e:
        print(f"❌ agent_dba import failed: {e}")
        traceback.print_exc()
        return 1
    print()

    # Check 3: Database connectivity
    print("3️⃣  DATABASE CONNECTIVITY CHECK")
    print("-" * 80)
    try:
        pool = await get_pool()
        conn = await pool.acquire()
        try:
            # Simple connectivity test
            result = await conn.fetchval("SELECT 1")
            if result == 1:
                print("✓ Database connectivity OK")
        finally:
            await pool.release(conn)
    except Exception as e:
        print(f"❌ Database connectivity FAILED: {e}")
        traceback.print_exc()
        return 1
    print()

    # Check 4: Required tables
    print("4️⃣  SCHEMA CHECK: REQUIRED TABLES")
    print("-" * 80)
    required_tables = [
        ("core", "documents"),
        ("core", "document_chunks"),
        ("ops", "agent_findings"),
        ("ops", "notification_queue"),
        ("ops", "agent_runs"),
    ]

    try:
        pool = await get_pool()
        missing = []
        for schema, table in required_tables:
            try:
                conn = await pool.acquire()
                try:
                    result = await conn.fetchval("""
                        SELECT EXISTS(
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = $1 AND table_name = $2
                        )
                    """, schema, table)
                    if result:
                        print(f"  ✓ {schema}.{table}")
                    else:
                        print(f"  ⚠️  {schema}.{table} — MISSING")
                        missing.append(f"{schema}.{table}")
                finally:
                    await pool.release(conn)
            except Exception as e:
                print(f"  ❌ {schema}.{table} — QUERY FAILED: {str(e)[:80]}")
                missing.append(f"{schema}.{table}")

        if missing:
            print(f"\n⚠️  {len(missing)} required tables are missing!")
            for table in missing:
                print(f"     - {table}")
    except Exception as e:
        print(f"❌ Schema check failed: {e}")
        traceback.print_exc()
        return 1
    print()

    # Check 5: PostgreSQL extensions
    print("5️⃣  POSTGRESQL EXTENSIONS CHECK")
    print("-" * 80)
    try:
        pool = await get_pool()
        conn = await pool.acquire()
        try:
            extensions = await conn.fetch("SELECT extname, extversion FROM pg_extension")
            ext_names = {row['extname']: row['extversion'] for row in extensions}

            if 'vector' in ext_names:
                print(f"  ✓ pgvector {ext_names['vector']}")
            else:
                print(f"  ⚠️  pgvector — NOT INSTALLED")

            if 'pg_stat_statements' in ext_names:
                print(f"  ✓ pg_stat_statements {ext_names['pg_stat_statements']}")
            else:
                print(f"  ⚠️  pg_stat_statements — NOT INSTALLED")
        finally:
            await pool.release(conn)
    except Exception as e:
        print(f"❌ Extension check failed: {e}")
    print()

    # Check 6: Try instantiating DBAAgent
    print("6️⃣  AGENT INSTANTIATION CHECK")
    print("-" * 80)
    try:
        agent = DBAAgent()
        print(f"✓ DBAAgent instantiated successfully")
        print(f"  ID: {agent.agent_id}")
        print(f"  Name: {agent.agent_name}")
        print(f"  Version: {agent.version}")
    except Exception as e:
        print(f"❌ DBAAgent instantiation failed: {e}")
        traceback.print_exc()
        return 1
    print()

    print("=" * 80)
    print("✓ ALL DIAGNOSTICS COMPLETE")
    print("=" * 80)
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
