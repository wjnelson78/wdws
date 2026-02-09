#!/usr/bin/env python3
"""Quick test of the unified MCP server tools."""
import asyncio, json, sys
sys.path.insert(0, '/opt/wdws/mcp-server')
from mcp_server import (
    search_emails, semantic_search, lookup_case,
    get_database_stats, search_medical_records, close_pool
)

async def test():
    print("=== Test 1: Database Stats ===")
    result = await get_database_stats()
    data = json.loads(result)
    print(f"Total docs: {data['total_documents']}, Chunks: {data['total_chunks']}, Cases: {data['total_cases']}")
    for item in data['by_type']:
        print(f"  {item['domain']}/{item['type']}: {item['count']}")

    print("\n=== Test 2: Search Emails (DesJarlais + emergency motion) ===")
    result = await search_emails(query="emergency motion", sender="DesJarlais", limit=5)
    data = json.loads(result)
    count = data['count']
    print(f"Found {count} emails")
    for r in data['results']:
        d = r['date']
        s = r['subject'][:80] if r['subject'] else 'N/A'
        print(f"  {d} | {s}")

    print("\n=== Test 3: Semantic Search (firefighter union) ===")
    result = await semantic_search(query="firefighter union grievance hearing", domain="legal", limit=3)
    data = json.loads(result)
    print(f"Found {data['count']} results")
    for r in data['results']:
        title = r['title'][:60] if r['title'] else 'N/A'
        print(f"  sim={r['similarity']} | {title} ({r['type']})")

    print("\n=== Test 4: Lookup Case (26-2-00762) ===")
    result = await lookup_case(case_number="26-2-00762")
    data = json.loads(result)
    cn = data.get('case_number', 'N/A')
    ct = data.get('title', 'N/A')
    print(f"Case: {cn} - {ct}")
    print(f"Status: {data.get('status')}, Docs: {data.get('doc_count')}")
    parties = data.get('parties', [])
    for p in parties:
        print(f"  {p['role']}: {p['name']}")

    print("\n=== Test 5: Medical Records Search ===")
    result = await search_medical_records(query="blood pressure", limit=3)
    data = json.loads(result)
    print(f"Found {data['count']} medical results")
    for r in data['results']:
        title = r['title'][:60] if r['title'] else 'N/A'
        print(f"  {r.get('date_of_service', 'N/A')} | {title}")

    await close_pool()
    print("\n✅ All tests passed!")

asyncio.run(test())
