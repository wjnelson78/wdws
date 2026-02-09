#!/usr/bin/env python3
"""
Patch mcp_server_v2.py to add query/tool-call logging.

This script:
1. Adds the auth_context import
2. Adds the _log_tool_call helper
3. Wraps every @mcp.tool() function to log calls with client_id, args, results, timing
"""

import re
import sys

def patch(source: str) -> str:
    # ─── 1. Add import for get_access_token ───
    old_import = "from mcp.shared.auth import OAuthClientInformationFull, OAuthToken"
    new_import = (
        "from mcp.shared.auth import OAuthClientInformationFull, OAuthToken\n"
        "from mcp.server.auth.middleware.auth_context import get_access_token as _get_auth_token\n"
        "import functools"
    )
    if "_get_auth_token" not in source:
        source = source.replace(old_import, new_import)

    # ─── 2. Add _log_tool_call helper after _row_dict ───
    log_helper = '''

# ── Tool-call logging ────────────────────────────────────────

async def _log_tool_call(tool_name: str, arguments: dict, result: str,
                         duration_ms: int, error: str = None):
    """Log every MCP tool call to ops.mcp_query_log for analytics."""
    try:
        token_info = _get_auth_token()
        client_id = token_info.client_id if token_info else "anonymous"
    except Exception:
        client_id = "anonymous"

    # Build result summary (truncate large results)
    try:
        parsed = json.loads(result) if isinstance(result, str) else result
        if isinstance(parsed, dict):
            result_count = parsed.get("count", parsed.get("total", 0))
            # Keep summary small
            summary = {k: v for k, v in parsed.items()
                       if k in ("count", "total", "query", "domain", "case_number",
                                "tool", "type", "message", "id", "title")}
        elif isinstance(parsed, list):
            result_count = len(parsed)
            summary = {"count": result_count}
        else:
            result_count = 1 if parsed else 0
            summary = {"preview": str(parsed)[:200]}
    except Exception:
        result_count = 0
        summary = {"preview": str(result)[:500]}

    try:
        p = await get_pool()
        # Get client name
        name_row = await p.fetchrow(
            "SELECT client_name FROM ops.oauth_clients WHERE client_id = $1",
            client_id,
        )
        client_name = name_row["client_name"] if name_row else client_id

        # Truncate arguments for storage (remove huge embedding arrays etc)
        safe_args = {}
        for k, v in arguments.items():
            if isinstance(v, str) and len(v) > 500:
                safe_args[k] = v[:500] + "..."
            else:
                safe_args[k] = v

        await p.execute("""
            INSERT INTO ops.mcp_query_log
                (client_id, client_name, tool_name, arguments, result_summary,
                 result_count, duration_ms, error)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, client_id, client_name, tool_name,
            json.dumps(safe_args), json.dumps(summary),
            result_count, duration_ms, error)

        # Update client stats
        await p.execute("""
            UPDATE ops.oauth_clients
            SET last_used = now(), total_tool_calls = COALESCE(total_tool_calls, 0) + 1
            WHERE client_id = $1
        """, client_id)

        log.debug("Logged tool call: %s by %s (%dms)", tool_name, client_id, duration_ms)
    except Exception as e:
        log.warning("Failed to log tool call: %s", e)


def logged_tool(func):
    """Decorator that wraps an MCP tool function to log its invocation."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        tool_name = func.__name__
        t0 = time.time()
        error = None
        result = ""
        try:
            result = await func(*args, **kwargs)
            return result
        except Exception as e:
            error = str(e)
            raise
        finally:
            duration_ms = int((time.time() - t0) * 1000)
            # kwargs are the tool arguments
            tool_args = {k: v for k, v in kwargs.items() if v is not None}
            asyncio.create_task(
                _log_tool_call(tool_name, tool_args, result or "", duration_ms, error)
            )
    return wrapper

'''

    marker = "# ── Embedding helper ──"
    if "_log_tool_call" not in source:
        source = source.replace(marker, log_helper + marker)

    # ─── 3. Wrap all @mcp.tool() functions ───
    # Change @mcp.tool() \n async def foo(...)  →  @mcp.tool() \n @logged_tool \n async def foo(...)
    if "@logged_tool" not in source:
        source = re.sub(
            r'(@mcp\.tool\(\))\n(async def )',
            r'\1\n@logged_tool\n\2',
            source
        )

    return source


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/opt/wdws/mcp-server/mcp_server_v2.py"
    with open(path, "r") as f:
        original = f.read()

    patched = patch(original)

    if patched == original:
        print("No changes needed — already patched")
        sys.exit(0)

    # Backup
    with open(path + ".pre_logging_backup", "w") as f:
        f.write(original)

    with open(path, "w") as f:
        f.write(patched)

    # Count changes
    tool_count = patched.count("@logged_tool")
    print(f"✓ Patched {path}")
    print(f"  - Added auth_context import")
    print(f"  - Added _log_tool_call helper + logged_tool decorator")
    print(f"  - Wrapped {tool_count} tool functions with @logged_tool")
