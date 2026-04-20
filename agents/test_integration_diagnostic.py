#!/usr/bin/env python3
"""
Integration Test Diagnostic — Verify self-healing agent test suite
Checks each test individually and reports pass/fail status
"""
import asyncio
import sys
import json
from datetime import datetime

# Test imports
try:
    from framework import BaseAgent, RunContext, get_pool, close_pool
    from agent_self_healing import SelfHealingAgent
    print("✓ Framework imports successful")
except ImportError as e:
    print(f"✗ Import failed: {e}")
    sys.exit(1)

async def run_diagnostics():
    """Run diagnostic tests."""
    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "tests": {}
    }

    # Test 1: Can instantiate agent
    try:
        agent = SelfHealingAgent()
        results["tests"]["agent_instantiation"] = {"passed": True}
        print("✓ Agent instantiation")
    except Exception as e:
        results["tests"]["agent_instantiation"] = {"passed": False, "error": str(e)}
        print(f"✗ Agent instantiation: {e}")
        return results

    # Test 2: Can acquire database pool
    try:
        pool = await get_pool()
        results["tests"]["db_pool"] = {"passed": True}
        print("✓ Database pool acquired")
    except Exception as e:
        results["tests"]["db_pool"] = {"passed": False, "error": str(e)}
        print(f"✗ Database pool: {e}")
        return results

    # Test 3: Run the full integration test suite
    try:
        agent = SelfHealingAgent()
        result = await agent.execute()

        # Extract test results
        metrics = result.get("metrics", {})
        summary = result.get("summary", "")

        results["tests"]["full_suite"] = {
            "passed": metrics.get("tests_failed", 0) == 0,
            "summary": summary,
            "metrics": metrics
        }

        print(f"\nFull Integration Test Suite Results:")
        print(f"  Summary: {summary}")
        print(f"  Tests Passed: {metrics.get('tests_passed', 0)}")
        print(f"  Tests Failed: {metrics.get('tests_failed', 0)}")
        print(f"  Repairs: {metrics.get('repairs', 0)}")
        print(f"  Cleaned: {metrics.get('cleaned', 0)}")

    except Exception as e:
        import traceback
        results["tests"]["full_suite"] = {
            "passed": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        print(f"✗ Full integration test failed: {e}")
        traceback.print_exc()
    finally:
        await close_pool()

    return results

if __name__ == "__main__":
    results = asyncio.run(run_diagnostics())
    print("\n" + "="*60)
    print("DIAGNOSTIC REPORT")
    print("="*60)
    print(json.dumps(results, indent=2, default=str))

    # Exit with error code if any test failed
    failed = sum(1 for t in results["tests"].values() if not t.get("passed"))
    sys.exit(0 if failed == 0 else 1)
