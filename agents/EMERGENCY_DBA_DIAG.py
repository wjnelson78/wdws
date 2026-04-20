#!/usr/bin/env python3
"""
EMERGENCY DIAGNOSTIC — Capture actual agent_dba error
This script runs agent_dba in isolation to see what the actual error is.
"""
import asyncio
import sys
import os
import traceback
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

async def main():
    print("="*80)
    print("EMERGENCY DBA DIAGNOSTIC")
    print("="*80)

    try:
        print("\n1. Importing agent_dba...")
        from agent_dba import DBAAgent
        print("   ✓ Import successful")

        print("\n2. Instantiating agent...")
        agent = DBAAgent()
        print("   ✓ Instantiation successful")

        print("\n3. Calling execute()...")
        result = await agent.execute(trigger="emergency-diagnostic")
        print(f"   ✓ Execute completed")
        print(f"   Result: {result}")

    except Exception as e:
        print(f"\n   ✗ ERROR: {e}")
        print(f"\n   Full Traceback:")
        print(traceback.format_exc())
        sys.exit(1)

    print("\n" + "="*80)
    print("DIAGNOSTIC COMPLETE - Agent DBA executed successfully")
    print("="*80)

if __name__ == '__main__':
    asyncio.run(main())
