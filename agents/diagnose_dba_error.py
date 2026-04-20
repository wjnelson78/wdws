#!/usr/bin/env python3
"""Diagnose DBA agent error by attempting instantiation and run."""
import sys
import asyncio
import traceback

sys.path.insert(0, '/opt/wdws/agents')
sys.path.insert(0, '/opt/wdws')

async def main():
    try:
        from config import Config
        from agent_dba import DBAAgent
        from framework import RunContext

        print("=" * 80)
        print("ATTEMPTING DBA AGENT INSTANTIATION AND RUN")
        print("=" * 80)

        config = Config()
        print(f"Config loaded: {config}")

        agent = DBAAgent(config)
        print(f"Agent instantiated: {agent}")

        # Create a mock context
        ctx = RunContext(agent=agent, config=config, logger=agent.log)

        print("\nAttempting agent.run()...")
        result = await agent.run(ctx)
        print(f"RUN SUCCESSFUL: {result}")

    except Exception as e:
        print("\n" + "!" * 80)
        print("ERROR ENCOUNTERED:")
        print("!" * 80)
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nFull traceback:")
        print(traceback.format_exc())
        print("!" * 80)
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
