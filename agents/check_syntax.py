#!/usr/bin/env python3
"""Check syntax of agent_dba.py"""
import ast
import sys

try:
    with open('agent_dba.py', 'r') as f:
        code = f.read()
    ast.parse(code)
    print("✓ agent_dba.py syntax is valid")
except SyntaxError as e:
    print(f"✗ SYNTAX ERROR in agent_dba.py:")
    print(f"  Line {e.lineno}: {e.msg}")
    print(f"  {e.text}")
    sys.exit(1)
except Exception as e:
    print(f"✗ ERROR: {e}")
    sys.exit(1)
