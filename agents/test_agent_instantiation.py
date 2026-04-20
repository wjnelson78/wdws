#!/usr/bin/env python3
"""Test if agent_dba can be instantiated without errors."""
import sys
import asyncio

# Test 1: Can we import the agent?
print("1. Attempting to import agent_dba...")
try:
    from agent_dba import DBAAgent
    print("   ✓ Import successful")
except Exception as e:
    print(f"   ✗ IMPORT FAILED: {e}")
    sys.exit(1)

# Test 2: Can we instantiate it?
print("\n2. Attempting to instantiate DBAAgent...")
try:
    agent = DBAAgent()
    print(f"   ✓ Instantiation successful: {agent}")
    print(f"   - agent_id: {agent.agent_id}")
    print(f"   - agent_name: {agent.agent_name}")
    print(f"   - version: {agent.version}")
except Exception as e:
    print(f"   ✗ INSTANTIATION FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Check if run method exists
print("\n3. Checking if run method exists...")
try:
    if hasattr(agent, 'run') and callable(getattr(agent, 'run')):
        print(f"   ✓ run() method exists and is callable")
    else:
        print(f"   ✗ run() method missing or not callable")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ ERROR: {e}")
    sys.exit(1)

# Test 4: Check if _safe_finding method exists
print("\n4. Checking if _safe_finding method exists...")
try:
    if hasattr(agent, '_safe_finding') and callable(getattr(agent, '_safe_finding')):
        print(f"   ✓ _safe_finding() method exists and is callable")
    else:
        print(f"   ✗ _safe_finding() method missing or not callable")
        sys.exit(1)
except Exception as e:
    print(f"   ✗ ERROR: {e}")
    sys.exit(1)

# Test 5: Check Python syntax of the file
print("\n5. Checking Python syntax of agent_dba.py...")
try:
    import py_compile
    py_compile.compile('/opt/wdws/agents/agent_dba.py', doraise=True)
    print(f"   ✓ Python syntax is valid")
except py_compile.PyCompileError as e:
    print(f"   ✗ SYNTAX ERROR: {e}")
    sys.exit(1)

print("\n✓ All instantiation tests passed!")
