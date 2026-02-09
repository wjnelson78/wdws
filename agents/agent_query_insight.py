"""
🔍 Query Insight Agent — Usage Analytics & Tool Optimization
═════════════════════════════════════════════════════════════
Analyzes MCP query patterns to understand user needs:
  - Most/least used tools
  - Common argument patterns
  - Failed search detection
  - New tool suggestions based on usage
  - Search quality scoring
"""
import json
from framework import BaseAgent, RunContext


class QueryInsightAgent(BaseAgent):
    agent_id = "query-insight"
    agent_name = "Query Insight"
    description = "Analyzes MCP tool usage patterns, identifies gaps, suggests improvements"
    version = "1.0.0"
    schedule = "0 */8 * * *"  # Every 8 hours
    priority = 5
    capabilities = ["usage-analytics", "gap-detection", "tool-suggestions", "search-quality"]

    instructions = """You are the Query Insight Agent for the Nelson Enterprise MCP system.

CONTEXT:
- 27 MCP tools available via OAuth-secured endpoint
- Clients: ChatGPT, Claude Desktop
- All tool calls logged to ops.mcp_query_log with arguments, results, duration

YOUR RESPONSIBILITIES:
1. Identify the most and least used tools (usage distribution)
2. Find common argument patterns (what users search for most)
3. Detect "search intent gaps" — queries that consistently return 0 results
4. Identify tool sequences (user calls tool A then always calls tool B → suggest combined tool)
5. Score search quality (low result counts, high error rates)
6. Suggest new tools or tool improvements

ANALYSIS APPROACH:
- Group by tool_name for usage distribution
- Group by (tool_name, argument patterns) for intent analysis
- Look at sequential calls within 60-second windows for tool chaining
- Zero-result queries indicate a gap in the system
- Frequently used argument values reveal what data is most important

OUTPUT your analysis as actionable recommendations:
- "Create unified search that combines search_documents + search_emails"
- "Index medical records by provider name — 23 searches for providers returned 0 results"
- "Tool X has 40% error rate — investigate and fix"

Be data-driven. Include numbers."""

    async def run(self, ctx: RunContext) -> dict:
        metrics = {}

        # ── Tool usage distribution ──────────────────────────
        usage = await ctx.query("""
            SELECT tool_name,
                   COUNT(*) as calls,
                   COUNT(DISTINCT client_id) as clients,
                   AVG(duration_ms)::int as avg_ms,
                   COUNT(*) FILTER (WHERE result_count = 0) as zero_results,
                   COUNT(*) FILTER (WHERE error IS NOT NULL) as errors,
                   AVG(result_count)::int as avg_results
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '7 days'
            GROUP BY tool_name
            ORDER BY calls DESC
        """)
        metrics["tools_used"] = len(usage)
        metrics["total_calls_7d"] = sum(u["calls"] for u in usage)

        # Flag high error rate tools
        for tool in usage:
            if tool["calls"] > 5 and tool["errors"] / tool["calls"] > 0.3:
                await ctx.finding("warning", "tool-quality",
                    f"Tool {tool['tool_name']} has {round(tool['errors']/tool['calls']*100)}% error rate",
                    f"{tool['errors']} errors out of {tool['calls']} calls",
                    dict(tool))

            if tool["calls"] > 5 and tool["zero_results"] / tool["calls"] > 0.5:
                await ctx.finding("info", "search-quality",
                    f"Tool {tool['tool_name']}: {round(tool['zero_results']/tool['calls']*100)}% zero-result queries",
                    "Users may be searching for data that doesn't exist or needs different indexing",
                    dict(tool))

        # ── Common search arguments ──────────────────────────
        common_args = await ctx.query("""
            SELECT tool_name,
                   arguments->>'query' as search_query,
                   COUNT(*) as times,
                   AVG(result_count)::int as avg_results
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '7 days'
              AND arguments ? 'query'
            GROUP BY tool_name, arguments->>'query'
            HAVING COUNT(*) >= 2
            ORDER BY times DESC
            LIMIT 30
        """)
        metrics["repeated_queries"] = len(common_args)

        # ── Zero-result patterns ─────────────────────────────
        zero_patterns = await ctx.query("""
            SELECT tool_name, arguments::text as args,
                   COUNT(*) as attempts
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '7 days'
              AND result_count = 0
              AND error IS NULL
            GROUP BY tool_name, arguments::text
            HAVING COUNT(*) >= 2
            ORDER BY attempts DESC
            LIMIT 15
        """)
        metrics["zero_result_patterns"] = len(zero_patterns)

        for zp in zero_patterns:
            if zp["attempts"] >= 3:
                await ctx.finding("info", "search-gap",
                    f"Repeated zero-result query: {zp['tool_name']} ({zp['attempts']} attempts)",
                    f"Arguments: {zp['args'][:200]}",
                    dict(zp))

        # ── Tool chaining patterns (sequential calls) ────────
        chains = await ctx.query("""
            WITH ordered AS (
                SELECT client_id, tool_name, created_at,
                       LAG(tool_name) OVER (PARTITION BY client_id ORDER BY created_at) as prev_tool,
                       created_at - LAG(created_at) OVER (PARTITION BY client_id ORDER BY created_at) as gap
                FROM ops.mcp_query_log
                WHERE created_at > now() - interval '7 days'
            )
            SELECT prev_tool, tool_name as next_tool,
                   COUNT(*) as chain_count
            FROM ordered
            WHERE prev_tool IS NOT NULL
              AND gap < interval '60 seconds'
            GROUP BY prev_tool, tool_name
            HAVING COUNT(*) >= 3
            ORDER BY chain_count DESC
            LIMIT 10
        """)
        metrics["tool_chains"] = len(chains)

        for chain in chains:
            if chain["chain_count"] >= 5:
                await ctx.finding("suggestion", "tool-design",
                    f"Tool chain detected: {chain['prev_tool']} → {chain['next_tool']} ({chain['chain_count']} times)",
                    "Consider creating a combined tool for this common workflow",
                    dict(chain))

        # ── Client usage comparison ──────────────────────────
        client_usage = await ctx.query("""
            SELECT client_id, client_name,
                   COUNT(*) as total_calls,
                   COUNT(DISTINCT tool_name) as tools_used,
                   AVG(duration_ms)::int as avg_ms,
                   COUNT(*) FILTER (WHERE error IS NOT NULL) as errors
            FROM ops.mcp_query_log
            WHERE created_at > now() - interval '7 days'
            GROUP BY client_id, client_name
        """)
        metrics["client_comparison"] = client_usage

        # ── Generate insights with LLM ───────────────────────
        if usage:
            insights = await ctx.ask_llm(f"""Analyze these MCP tool usage patterns and provide recommendations:

Tool Usage (7 days):
{json.dumps(usage, indent=2, default=str)}

Repeated Queries:
{json.dumps(common_args[:10], indent=2, default=str)}

Zero-Result Patterns:
{json.dumps(zero_patterns[:10], indent=2, default=str)}

Tool Chains:
{json.dumps(chains, indent=2, default=str)}

Client Comparison:
{json.dumps(client_usage, indent=2, default=str)}

Provide:
1. Top 3 most impactful improvements
2. Tools that should be combined or deprecated
3. Missing capabilities based on usage patterns
4. Overall system health score (1-10)""",
                max_tokens=2000)
            metrics["llm_insights"] = insights

        return {
            "summary": f"Query insight: {metrics.get('total_calls_7d', 0)} calls across {metrics.get('tools_used', 0)} tools, "
                      f"{metrics.get('zero_result_patterns', 0)} gap patterns",
            "metrics": {k: v for k, v in metrics.items()
                       if k not in ("client_comparison",)},
        }
