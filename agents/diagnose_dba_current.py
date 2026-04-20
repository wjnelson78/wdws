#!/usr/bin/env python3
"""Diagnose current DBA agent failures."""
import sys
sys.path.insert(0, '/opt/wdws/agents')

from framework import get_pool

try:
    pool = get_pool()
    print('✓ Database connection OK')

    # Check ops schema tables
    with pool.cursor() as cur:
        cur.execute('''
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'ops'
            ORDER BY table_name
        ''')
        tables = [row[0] for row in cur.fetchall()]
        print(f'✓ ops.* tables found: {", ".join(tables)}')

        # Check if agent_runs table exists and has data
        if 'agent_runs' in tables:
            cur.execute('''
                SELECT id, agent_id, status, error_msg, created_at
                FROM ops.agent_runs
                WHERE agent_id = %s
                ORDER BY created_at DESC
                LIMIT 5
            ''', ('dba',))
            rows = cur.fetchall()
            print(f'\nRecent DBA runs ({len(rows)}):')
            for row in rows:
                print(f'\n  ID: {row[0]}')
                print(f'  Status: {row[2]}')
                print(f'  Created: {row[4]}')
                if row[3]:
                    print(f'  Error: {row[3][:300]}')
        else:
            print('✗ ops.agent_runs table NOT FOUND')

except Exception as e:
    import traceback
    print(f'✗ Error: {e}')
    traceback.print_exc()
    sys.exit(1)
