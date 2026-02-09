#!/bin/bash
# Start the Paperless-ngx MCP server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables
if [ -f "$SCRIPT_DIR/.env" ]; then
    export $(grep -v '^#' "$SCRIPT_DIR/.env" | xargs)
elif [ -f "$HOME/Library/Application Support/paperless-ngx-mcp/.env" ]; then
    export $(grep -v '^#' "$HOME/Library/Application Support/paperless-ngx-mcp/.env" | xargs)
fi

# Set defaults if not in .env
export MCP_PORT="${MCP_PORT:-9001}"
export MCP_REQUIRE_OAUTH="${MCP_REQUIRE_OAUTH:-true}"
export MCP_OAUTH_AUTO_APPROVE="${MCP_OAUTH_AUTO_APPROVE:-true}"

echo "Starting Paperless-ngx MCP Server..."
echo "  Port: $MCP_PORT"
echo "  OAuth: $MCP_REQUIRE_OAUTH"
echo "  Paperless URL: $PAPERLESS_URL"

exec "$SCRIPT_DIR/venv/bin/python" "$SCRIPT_DIR/mcp_server_http.py"
