#!/usr/bin/env python3
"""Check database logs for DBA agent errors."""
import sys
import asyncio
import asyncpg
from config import DATABASE_URL

async def main():
    try:
        conn = await asyncpg.connect(DATABASE_URL)

        # Get recent agent_runs for dba agent with errors
        print("=" * 80)
        print("RECENT DBA AGENT RUNS WITH ERRORS")
        print("=" * 80)

        runs = await conn.fetch("""
            SELECT run_id, agent_id, status, created_at, error_message, metadata
            FROM ops.agent_runs
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 10
        """)

        for run in runs:
            print(f"\nRun ID: {run['run_id']}")
            print(f"Status: {run['status']}")
            print(f"Created: {run['created_at']}")
            if run['error_message']:
                print(f"Error: {run['error_message'][:500]}")
            if run['metadata']:
                print(f"Metadata: {run['metadata']}")

        # Get recent findings for dba agent
        print("\n" + "=" * 80)
        print("RECENT DBA AGENT FINDINGS")
        print("=" * 80)

        findings = await conn.fetch("""
            SELECT finding_id, agent_id, severity, category, title, detail, created_at
            FROM ops.agent_findings
            WHERE agent_id = 'dba'
            ORDER BY created_at DESC
            LIMIT 10
        """)

        for f in findings:
            print(f"\nFinding: {f['category']} ({f['severity']})")
            print(f"Title: {f['title']}")
            if f['detail']:
                print(f"Detail: {f['detail'][:300]}")

        await conn.close()

    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {str(e)}")
        print(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
