#!/usr/bin/env python3
import asyncio
import asyncpg
import os

async def main():
    url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost/athena')
    try:
        conn = await asyncpg.connect(url)
        rows = await conn.fetch(
            "SELECT id, agent_id, status, error FROM ops.agent_runs WHERE agent_id = 'dba' ORDER BY started_at DESC LIMIT 5"
        )
        for r in rows:
            print(f"\n=== Run {r['id']} ===")
            print(f"Status: {r['status']}")
            if r['error']:
                print(f"Error:\n{r['error'][:500]}")
        await conn.close()
    except Exception as e:
        print(f"Connection error: {e}")

asyncio.run(main())
