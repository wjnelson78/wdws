#!/usr/bin/env python3
"""
Emergency diagnostic for agent_dba cascading failures.
Checks: instantiation, database connectivity, schema issues.
"""
import sys
import asyncio
sys.path.insert(0, '/opt/wdws/agents')

async def diagnose():
    print("=" * 70)
    print("AGENT DBA CRITICAL DIAGNOSTIC")
    print("=" * 70)

    # Step 1: Can we import the agent?
    try:
        from agent_dba import DBAAgent
        print("✓ Step 1: agent_dba module imports successfully")
    except Exception as e:
        print(f"✗ Step 1 FAILED: Cannot import agent_dba")
        print(f"   Error: {e}")
        return

    # Step 2: Can we instantiate it?
    try:
        agent = DBAAgent()
        print(f"✓ Step 2: DBAAgent instantiated successfully")
        print(f"   agent_id={agent.agent_id}, schedule={agent.schedule}")
    except Exception as e:
        print(f"✗ Step 2 FAILED: Cannot instantiate DBAAgent")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 3: Can we get database pool?
    try:
        from framework import get_pool
        pool = await get_pool()
        if pool and not pool._closed:
            print("✓ Step 3: Database pool is healthy")
            # Try a simple query
            result = await pool.fetchval("SELECT 1")
            if result == 1:
                print("✓ Step 3b: Database connectivity verified")
        else:
            print("✗ Step 3 FAILED: Database pool is closed or invalid")
    except Exception as e:
        print(f"✗ Step 3 FAILED: Cannot access database pool")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 4: Does ops.agent_runs table exist?
    try:
        from framework import get_pool
        pool = await get_pool()
        result = await pool.fetchval("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'ops' AND table_name = 'agent_runs'
        """)
        if result > 0:
            print("✓ Step 4: ops.agent_runs table exists")
        else:
            print("✗ Step 4: ops.agent_runs table does NOT exist")
    except Exception as e:
        print(f"✗ Step 4 FAILED: Cannot check schema")
        print(f"   Error: {e}")

    # Step 5: Try a minimal run simulation
    try:
        from framework import RunContext
        ctx = RunContext(agent)
        print("✓ Step 5: RunContext created successfully")

        # Try to get a connection
        db = await ctx.db()
        if db and not db._closed:
            print("✓ Step 5b: ctx.db() returned valid pool")
        else:
            print("✗ Step 5b: ctx.db() returned invalid/closed pool")
    except Exception as e:
        print(f"✗ Step 5 FAILED: Cannot create RunContext")
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        return

    # Step 6: Check BaseAgent.broadcast method
    try:
        if hasattr(agent, 'broadcast'):
            print("✓ Step 6: DBAAgent.broadcast method exists")
            if asyncio.iscoroutinefunction(agent.broadcast):
                print("✓ Step 6b: broadcast is async (correct)")
            else:
                print("✗ Step 6b: broadcast is NOT async (problem!)")
        else:
            print("✗ Step 6 FAILED: DBAAgent does not have broadcast method")
    except Exception as e:
        print(f"✗ Step 6 FAILED: Cannot check broadcast method")
        print(f"   Error: {e}")

    print("\n" + "=" * 70)
    print("DIAGNOSIS COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    asyncio.run(diagnose())
